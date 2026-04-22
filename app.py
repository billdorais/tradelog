import json
import logging
import os
import sqlite3
import sys
import threading
from zoneinfo import ZoneInfo
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from flask import Flask, Response, abort, jsonify, make_response, render_template, request, stream_with_context

load_dotenv()

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "change-me")
DATABASE_URL  = os.environ.get("DATABASE_URL")

# ---------------------------------------------------------------------------
# Broker initialisation — connect in background so app starts immediately
# ---------------------------------------------------------------------------

ib_broker         = None   # paper gateway
ib_broker_live    = None   # live gateway (IB_HOST_LIVE env var)
_ib_sync_event      = None
_ib_sync_queue      = None
_ib_task_queue      = None
_ib_live_task_queue = None
_ib_paused          = False   # when True, paper IB background thread skips reconnect
_ib_live_paused     = False   # when True, live IB background thread skips reconnect
eod_close_enabled   = True

# ---------------------------------------------------------------------------
# Risk management state
# MAX_DAILY_LOSS: halt + liquidate when daily P&L drops to this value (e.g. -500.0).
#                 Set to 0 (default) to disable.
# SIGNAL_COOLDOWN_SECS: drop duplicate signals for the same strategy+ticker+action
#                       within this window. Set to 0 to disable.
# ---------------------------------------------------------------------------
MAX_DAILY_LOSS        = float(os.environ.get("MAX_DAILY_LOSS", "0"))
MAX_POSITION_LOSS     = float(os.environ.get("MAX_POSITION_LOSS", "0"))
SIGNAL_COOLDOWN_SECS  = int(os.environ.get("SIGNAL_COOLDOWN_SECS", "10"))

_risk_halted          = False   # True when daily loss limit is breached
_last_signal_ts       = {}      # {(strategy, ticker, action): unix timestamp}
_blocked_strategies   = {}      # {strategy: {reason, symbol, loss, ts, broker}}
_auto_closed_symbols  = set()   # symbols already sent a position-stop close today
_latest_positions     = []      # cached by position monitor for the status endpoint
_risk_lock            = threading.Lock()

if os.environ.get("IB_HOST"):
    import queue as _ib_queue_mod
    from brokers.ib_broker import IBBroker
    ib_broker      = IBBroker()
    _ib_sync_event = threading.Event()
    _ib_sync_queue = _ib_queue_mod.SimpleQueue()
    _ib_task_queue = _ib_queue_mod.Queue()

    def _submit_ib_task(fn, *args, _timeout=30, **kwargs):
        """
        Run fn(*args, **kwargs) on the background IB thread (which owns the event loop).
        Blocks the calling thread until the result is ready (or _timeout seconds elapse).
        Raises RuntimeError on task failure or timeout.
        """
        result_q = _ib_queue_mod.SimpleQueue()
        _ib_task_queue.put({"fn": fn, "args": args, "kwargs": kwargs, "result_queue": result_q})
        try:
            item = result_q.get(timeout=_timeout)
        except _ib_queue_mod.Empty:
            raise RuntimeError("IB task timed out after %ds" % _timeout)
        if "error" in item:
            raise RuntimeError(item["error"])
        return item["result"]

    def _on_fill(_trade, fill):
        """Called by ib_async execDetailsEvent(trade, fill) — persists execution to DB."""
        try:
            contract  = fill.contract
            execution = fill.execution
            exec_id   = execution.execId
            pnl = (round(fill.commissionReport.realizedPNL, 2)
                   if fill.commissionReport
                      and fill.commissionReport.realizedPNL == fill.commissionReport.realizedPNL
                   else None)
            conn = get_db()
            cur  = conn.cursor()
            p    = placeholder()
            cur.execute(
                f"INSERT INTO ib_executions "
                f"(exec_id,ts,symbol,sec_type,side,shares,price,order_id,account,exchange,pnl)"
                f" VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})"
                f" ON CONFLICT (exec_id) DO UPDATE SET pnl=EXCLUDED.pnl",
                (exec_id,
                 str(execution.time),
                 contract.symbol,
                 contract.secType,
                 execution.side,
                 float(execution.shares),
                 float(execution.price),
                 execution.orderId,
                 execution.acctNumber,
                 execution.exchange,
                 pnl),
            ) if DATABASE_URL else cur.execute(
                f"INSERT OR REPLACE INTO ib_executions "
                f"(exec_id,ts,symbol,sec_type,side,shares,price,order_id,account,exchange,pnl)"
                f" VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})",
                (exec_id,
                 str(execution.time),
                 contract.symbol,
                 contract.secType,
                 execution.side,
                 float(execution.shares),
                 float(execution.price),
                 execution.orderId,
                 execution.acctNumber,
                 execution.exchange,
                 pnl),
            )
            conn.commit()
            conn.close()
            log.info("IB fill saved: %s %s %s @ %s (pnl=%s)", execution.side, execution.shares, contract.symbol, execution.price, pnl)
            threading.Thread(target=_store_account_snapshot, daemon=True).start()
        except Exception as e:
            log.error("Error saving IB fill: %s", e)

    def _store_account_snapshot():
        """Take one account snapshot and persist it if net_liq changed."""
        try:
            snap = ib_broker.account_snapshot()
            ts   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            conn = get_db()
            cur  = conn.cursor()
            p    = placeholder()
            # Check last stored value to avoid duplicates
            cur.execute("SELECT net_liq FROM account_snapshots ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            last = row[0] if row else None
            if snap["net_liq"] != last:
                cur.execute(
                    f"INSERT INTO account_snapshots (ts, net_liq, realized_pnl, unrealized_pnl)"
                    f" VALUES ({p},{p},{p},{p})",
                    (ts, snap["net_liq"], snap["realized_pnl"], snap["unrealized_pnl"]),
                )
                conn.commit()
                log.debug("Account snapshot stored after fill: %s", snap)
            conn.close()
        except Exception as e:
            log.warning("Account snapshot (fill-triggered) failed: %s", e)

    def _sync_fills_on_connect():
        """Persist any fills from IB via reqExecutions. Returns count saved."""
        saved = 0
        for fill in ib_broker.executions_from_ib():
            exec_id = fill["exec_id"]
            pnl     = fill.get("pnl")
            conn = get_db()
            cur  = conn.cursor()
            p    = placeholder()
            cur.execute(
                f"INSERT INTO ib_executions "
                f"(exec_id,ts,symbol,sec_type,side,shares,price,order_id,account,exchange,pnl)"
                f" VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})"
                f" ON CONFLICT (exec_id) DO UPDATE SET pnl={p}",
                (exec_id, str(fill["time"]), fill["symbol"], fill["sec_type"],
                 fill["side"], fill["shares"], fill["price"],
                 fill["order_id"], fill["account"], fill["exchange"], pnl, pnl),
            ) if DATABASE_URL else cur.execute(
                f"INSERT OR REPLACE INTO ib_executions "
                f"(exec_id,ts,symbol,sec_type,side,shares,price,order_id,account,exchange,pnl)"
                f" VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})",
                (exec_id, str(fill["time"]), fill["symbol"], fill["sec_type"],
                 fill["side"], fill["shares"], fill["price"],
                 fill["order_id"], fill["account"], fill["exchange"], pnl),
            )
            conn.commit()
            conn.close()
            saved += 1
        log.info("IB fill sync complete — %d fills", saved)
        return saved

    def _connect_ib_background():
        """Try to connect to IB Gateway, retrying every 30s on failure.
        Also handles manual sync requests (_ib_sync_event) and queued IB tasks
        (_ib_task_queue) that must run on this thread (which owns the event loop)."""
        time.sleep(10)  # give IB Gateway time to be ready before first connect attempt
        last_periodic_sync = 0
        while True:
            global _ib_paused
            if _ib_paused:
                time.sleep(2)
                continue
            if not ib_broker.is_connected():
                try:
                    ib_broker.connect()
                    ib_broker.register_fill_callback(_on_fill)
                    _sync_fills_on_connect()
                    log.info("IB Gateway connected (pid=%s)", os.getpid())
                except Exception as e:
                    log.warning("IB connect failed, retrying in 30s: %s", e)
                    time.sleep(30)
                    continue

            # Wait up to 2s, or wake immediately on a manual sync request
            if _ib_sync_event.wait(timeout=2):
                _ib_sync_event.clear()
                try:
                    saved = _sync_fills_on_connect()
                    _ib_sync_queue.put({"synced": saved})
                except Exception as e:
                    _ib_sync_queue.put({"error": str(e)})

            # Periodic fill sync every 60s as a safety net
            now_ts = time.time()
            if now_ts - last_periodic_sync >= 60:
                last_periodic_sync = now_ts
                try:
                    saved = _sync_fills_on_connect()
                    if saved:
                        log.info("Periodic fill sync: saved %d new fills", saved)
                except Exception as e:
                    log.warning("Periodic fill sync error: %s", e)

            # Drain any tasks submitted via _submit_ib_task()
            while True:
                try:
                    task = _ib_task_queue.get_nowait()
                    try:
                        result = task["fn"](*task["args"], **task["kwargs"])
                        task["result_queue"].put({"result": result})
                    except Exception as e:
                        log.error("IB background task error: %s", e)
                        task["result_queue"].put({"error": str(e)})
                except _ib_queue_mod.Empty:
                    break

    threading.Thread(target=_connect_ib_background, daemon=True).start()

    def _poll_account_snapshot():
        """Fallback poll every 60s for unrealized P&L drift between fills."""
        time.sleep(15)  # wait for connection to establish
        while True:
            if ib_broker.is_connected():
                _store_account_snapshot()
            time.sleep(60)

    threading.Thread(target=_poll_account_snapshot, daemon=True).start()

    def _eod_close_scheduler():
        """Close all open IB positions at 3:58 PM ET on weekdays (if enabled).
        Fires any time in the 3:58–4:00 PM ET window so a mid-window restart
        (e.g. from a redeploy) still catches the close."""
        ET = ZoneInfo("America/New_York")
        triggered_date = None
        while True:
            now = datetime.now(ET)
            today = now.date()
            t = (now.hour, now.minute)
            in_window = (15, 58) <= t <= (16, 0)   # 3:58 PM – 4:00 PM ET
            if (eod_close_enabled
                    and now.weekday() < 5            # Mon–Fri
                    and in_window
                    and triggered_date != today):
                triggered_date = today
                log.info("EOD scheduler: closing all positions at %02d:%02d ET", now.hour, now.minute)
                try:
                    if ib_broker and ib_broker.is_connected():
                        result = _submit_ib_task(ib_broker.close_all_positions, _timeout=60)
                        log.info("EOD close result: %s", result)
                    else:
                        log.warning("EOD scheduler: IB not connected, skipping close")
                except Exception as e:
                    log.error("EOD close failed: %s", e)
            time.sleep(30)

    threading.Thread(target=_eod_close_scheduler, daemon=True).start()

# ---------------------------------------------------------------------------
# Live IB broker (optional — set IB_HOST_LIVE to enable)
# ---------------------------------------------------------------------------

if os.environ.get("IB_HOST_LIVE"):
    import queue as _ib_queue_mod_live
    if not _ib_task_queue:            # import IBBroker if paper block didn't run
        from brokers.ib_broker import IBBroker
    ib_broker_live      = IBBroker(
        host=os.environ["IB_HOST_LIVE"],
        port=int(os.environ.get("IB_PORT_LIVE", 4004)),
    )
    _ib_live_task_queue = _ib_queue_mod_live.Queue()

    def _submit_ib_live_task(fn, *args, _timeout=30, **kwargs):
        result_q = _ib_queue_mod_live.SimpleQueue()
        _ib_live_task_queue.put({"fn": fn, "args": args, "kwargs": kwargs, "result_queue": result_q})
        try:
            item = result_q.get(timeout=_timeout)
        except _ib_queue_mod_live.Empty:
            raise RuntimeError("IB live task timed out after %ds" % _timeout)
        if "error" in item:
            raise RuntimeError(item["error"])
        return item["result"]

    def _connect_ib_live_background():
        """Connect and maintain the live IB Gateway connection."""
        time.sleep(12)  # stagger slightly from paper connection
        while True:
            global _ib_live_paused
            if _ib_live_paused:
                time.sleep(2)
                continue
            if not ib_broker_live.is_connected():
                try:
                    ib_broker_live.connect()
                    log.info("IB Live Gateway connected — accounts: %s",
                             ib_broker_live.status().get("accounts"))
                except Exception as e:
                    log.warning("IB Live connect failed, retrying in 30s: %s", e)
                    time.sleep(30)
                    continue
            # Drain any tasks (e.g. options contract selection)
            while True:
                try:
                    task = _ib_live_task_queue.get_nowait()
                    try:
                        result = task["fn"](*task["args"], **task["kwargs"])
                        task["result_queue"].put({"result": result})
                    except Exception as e:
                        log.error("IB live background task error: %s", e)
                        task["result_queue"].put({"error": str(e)})
                except Exception:
                    break
            time.sleep(2)

    threading.Thread(target=_connect_ib_live_background, daemon=True).start()

# ---------------------------------------------------------------------------
# Alpaca broker (optional — set ALPACA_KEY + ALPACA_SECRET to enable)
# ---------------------------------------------------------------------------

alpaca_broker = None
_alpaca_fills_cache = {"data": [], "ts": 0.0}
ALPACA_CACHE_TTL = 120  # seconds — paginated fetch can be slow, cache longer
if os.environ.get("ALPACA_KEY"):
    from brokers.alpaca_broker import AlpacaBroker
    alpaca_broker = AlpacaBroker()
    log.info("Alpaca broker initialised (paper=%s)", os.environ.get("ALPACA_PAPER", "true"))

# ---------------------------------------------------------------------------
# Coinbase broker (optional — set COINBASE_KEY + COINBASE_SECRET to enable)
# ---------------------------------------------------------------------------

coinbase_broker = None
if os.environ.get("COINBASE_KEY"):
    from brokers.coinbase_broker import CoinbaseBroker
    coinbase_broker = CoinbaseBroker()
    log.info("Coinbase broker initialised")

# ---------------------------------------------------------------------------
# Risk monitor — polls P&L every 60s; halts + liquidates on MAX_DAILY_LOSS
# ---------------------------------------------------------------------------

_daily_pnl_cache = {"value": None, "ts": 0.0}


def _compute_daily_pnl():
    """Return today's P&L.
    When Alpaca is configured, uses alpaca_broker.daily_pnl() (equity minus
    last close equity) so the number matches what the Alpaca app shows as
    Daily Change and includes both realized and unrealized moves.
    Falls back to pairing BUY/SELL signals from the trades table when Alpaca
    is not configured.
    Result is cached for 60 s to keep the 15 s UI refresh cheap."""
    now_ts = time.time()
    if now_ts - _daily_pnl_cache["ts"] < 60 and _daily_pnl_cache["value"] is not None:
        return _daily_pnl_cache["value"]

    # Prefer live Alpaca account P&L when broker is available
    if alpaca_broker is not None:
        try:
            result = round(alpaca_broker.daily_pnl(), 2)
            _daily_pnl_cache["value"] = result
            _daily_pnl_cache["ts"]    = now_ts
            return result
        except Exception as _e:
            log.debug("_compute_daily_pnl Alpaca error: %s", _e)
            # fall through to signal-based calculation

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT action, ticker, strategy, received_at, price, quantity, exec_status FROM trades ORDER BY id ASC")
        rows = cur.fetchall()
        conn.close()
        if DATABASE_URL:
            cols       = [d[0] for d in cur.description]
            trade_rows = [dict(zip(cols, r)) for r in rows]
        else:
            trade_rows = [dict(r) for r in rows]

        open_longs  = {}   # (strategy, ticker) → [(price, qty)]
        open_shorts = {}
        today_pnl   = 0.0

        for t in trade_rows:
            action   = (t.get("action") or "").strip().upper()
            ticker   = (t.get("ticker") or "").strip().upper()
            strategy = (t.get("strategy") or "Unknown").strip()
            received = t.get("received_at") or ""
            try:
                price = float(t.get("price") or 0)
                qty   = float(t.get("quantity") or 1)
            except (ValueError, TypeError):
                continue
            if not ticker or price == 0:
                continue
            exec_status = (t.get("exec_status") or "").lower()
            if exec_status in ("blocked", "skipped", "error"):
                continue

            key      = (strategy, ticker)
            is_today = received[:10] == today

            if action in ("BUY", "LONG", "EXIT_SHORT"):
                queue = open_shorts.get(key, [])
                if queue:
                    entry_price, entry_qty = queue.pop(0)
                    pnl = (entry_price - price) * min(qty, entry_qty)
                    if is_today:
                        today_pnl += pnl
                else:
                    open_longs.setdefault(key, []).append((price, qty))
            elif action in ("SELL", "SHORT", "EXIT_LONG"):
                queue = open_longs.get(key, [])
                if queue:
                    entry_price, entry_qty = queue.pop(0)
                    pnl = (price - entry_price) * min(qty, entry_qty)
                    if is_today:
                        today_pnl += pnl
                else:
                    open_shorts.setdefault(key, []).append((price, qty))

        result = round(today_pnl, 2)
        _daily_pnl_cache["value"] = result
        _daily_pnl_cache["ts"]    = now_ts
        return result
    except Exception as _e:
        log.debug("_compute_daily_pnl error: %s", _e)
        _daily_pnl_cache["ts"] = now_ts   # back off on errors too
        return _daily_pnl_cache["value"]


def _risk_monitor_loop():
    """Background thread: poll daily P&L every 60s.
    When P&L hits MAX_DAILY_LOSS, set _risk_halted and close all positions."""
    global _risk_halted
    time.sleep(20)  # wait for broker connections to establish
    while True:
        if MAX_DAILY_LOSS < 0:
            try:
                pnl = _compute_daily_pnl()
                if pnl is not None and pnl <= MAX_DAILY_LOSS:
                    with _risk_lock:
                        already_halted = _risk_halted
                        _risk_halted = True
                    if not already_halted:
                        log.error(
                            "RISK HALT: daily P&L $%.2f reached limit $%.2f — liquidating all positions",
                            pnl, MAX_DAILY_LOSS,
                        )
                        for _broker, _label in [
                            (alpaca_broker,   "Alpaca"),
                            (coinbase_broker, "Coinbase"),
                        ]:
                            if _broker:
                                try:
                                    _broker.close_all_positions()
                                    log.info("Risk liquidation: %s positions closed", _label)
                                except Exception as _e:
                                    log.error("Risk liquidation %s failed: %s", _label, _e)
                        # IB close must run on the background IB thread
                        if _ib_task_queue is not None and ib_broker:
                            try:
                                _submit_ib_task(ib_broker.close_all_positions, _timeout=60)
                                log.info("Risk liquidation: IB positions closed")
                            except Exception as _e:
                                log.error("Risk liquidation IB failed: %s", _e)
                        if _ib_live_task_queue is not None and ib_broker_live:
                            try:
                                _submit_ib_live_task(ib_broker_live.close_all_positions, _timeout=60)
                                log.info("Risk liquidation: IB Live positions closed")
                            except Exception as _e:
                                log.error("Risk liquidation IB Live failed: %s", _e)
            except Exception as _e:
                log.warning("Risk monitor error: %s", _e)
        time.sleep(60)


threading.Thread(target=_risk_monitor_loop, daemon=True).start()


def _find_strategy_for_symbol(symbol):
    """Return the most recent strategy name that traded this symbol, or None."""
    try:
        conn = get_db()
        cur  = conn.cursor()
        p    = placeholder()
        cur.execute(
            f"SELECT strategy FROM trades WHERE ticker={p} AND strategy IS NOT NULL "
            f"ORDER BY id DESC LIMIT 1",
            (symbol,),
        )
        row = cur.fetchone()
        conn.close()
        if row:
            return row[0] if DATABASE_URL else row["strategy"]
    except Exception as _e:
        log.debug("_find_strategy_for_symbol failed: %s", _e)
    return None


def _check_position_stops():
    """Check all open positions against MAX_POSITION_LOSS.
    Closes offending positions and blocks the originating strategy."""
    global _latest_positions
    all_positions = []

    if alpaca_broker:
        try:
            # Bypass the position cache here — risk checks need fresh data.
            alpaca_broker._invalidate_pos_cache()
            for p in alpaca_broker.get_positions():
                p["broker"] = "alpaca"
                all_positions.append(p)
        except Exception as _e:
            log.debug("Position stop: Alpaca get_positions failed: %s", _e)

    if _ib_task_queue is not None and ib_broker:
        try:
            for p in _submit_ib_task(ib_broker.get_positions, _timeout=15):
                p["broker"] = "ib"
                all_positions.append(p)
        except Exception as _e:
            log.debug("Position stop: IB get_positions failed: %s", _e)

    if _ib_live_task_queue is not None and ib_broker_live:
        try:
            for p in _submit_ib_live_task(ib_broker_live.get_positions, _timeout=15):
                p["broker"] = "ib-live"
                all_positions.append(p)
        except Exception as _e:
            log.debug("Position stop: IB Live get_positions failed: %s", _e)

    with _risk_lock:
        _latest_positions = all_positions

    # Clear _auto_closed_symbols for any symbol no longer showing an open position.
    # This allows the monitor to protect new entries in the same symbol later in the session.
    open_symbols = {p["symbol"].upper() for p in all_positions}
    with _risk_lock:
        stale = {s for s in _auto_closed_symbols if s.upper() not in open_symbols}
        _auto_closed_symbols.difference_update(stale)
    if stale:
        log.info("Position stop: cleared auto-close guard for %s (no longer open)", stale)

    for pos in all_positions:
        upnl   = float(pos.get("unrealized_pnl") or 0)
        symbol = pos["symbol"]
        broker = pos["broker"]

        if upnl > MAX_POSITION_LOSS:   # e.g. -150 > -200 → still OK
            continue

        with _risk_lock:
            if symbol in _auto_closed_symbols:
                continue
            # Don't add to _auto_closed_symbols yet — only add after a successful close
            # so that a failed close is retried on the next poll rather than silently dropped.

        strategy = _find_strategy_for_symbol(symbol)

        log.error(
            "POSITION STOP: %s unrealized P&L $%.2f hit limit $%.2f [%s] — "
            "closing position, blocking strategy '%s'",
            symbol, upnl, MAX_POSITION_LOSS, broker, strategy,
        )

        # Close the position — only mark as handled if the order is successfully submitted
        close_ok = False
        try:
            if broker == "alpaca":
                res = alpaca_broker.close_position(symbol)
                close_ok = res.get("success", False)
                if not close_ok:
                    log.error("Position stop close failed for %s: %s", symbol, res.get("error"))
            elif broker == "ib" and _ib_task_queue is not None:
                _submit_ib_task(ib_broker.close_position, symbol, pos.get("qty", 0), _timeout=30)
                close_ok = True
            elif broker == "ib-live" and _ib_live_task_queue is not None:
                _submit_ib_live_task(ib_broker_live.close_position, symbol, pos.get("qty", 0), _timeout=30)
                close_ok = True
        except Exception as _e:
            log.error("Position stop close failed for %s: %s", symbol, _e)

        if close_ok:
            log.info("Position stop: %s close order submitted on %s", symbol, broker)
            with _risk_lock:
                _auto_closed_symbols.add(symbol)
        else:
            # Close failed — leave symbol out of _auto_closed_symbols so it retries next poll
            log.warning("Position stop: close order for %s failed — will retry next poll", symbol)
            continue

        # Block the strategy for the rest of the session
        # Skip pseudo-strategies like "manual-close" that aren't real signal strategies
        if strategy and "manual" not in strategy.lower():
            with _risk_lock:
                _blocked_strategies[strategy] = {
                    "reason":   f"Position stop triggered: {symbol} loss ${upnl:.2f}",
                    "symbol":   symbol,
                    "loss":     round(upnl, 2),
                    "ts":       datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "broker":   broker,
                }
            log.error("Strategy '%s' blocked — position stop on %s (loss=$%.2f)", strategy, symbol, upnl)
        elif strategy and "manual" in strategy.lower():
            log.warning("Position stop on %s (loss=$%.2f) — skipping block of pseudo-strategy '%s'", symbol, upnl, strategy)


def _position_monitor_loop():
    """Background thread: poll positions every 10s, close any that breach MAX_POSITION_LOSS."""
    time.sleep(25)  # stagger from risk monitor
    while True:
        if MAX_POSITION_LOSS < 0:
            try:
                _check_position_stops()
            except Exception as _e:
                log.warning("Position monitor error: %s", _e)
        time.sleep(10)


threading.Thread(target=_position_monitor_loop, daemon=True).start()

# ---------------------------------------------------------------------------
# Database — PostgreSQL on Railway, SQLite locally
# ---------------------------------------------------------------------------

def get_db():
    if DATABASE_URL:
        import psycopg
        return psycopg.connect(DATABASE_URL)
    conn = sqlite3.connect("trades.db")
    conn.row_factory = sqlite3.Row
    return conn


def placeholder():
    return "%s" if DATABASE_URL else "?"


_INTRADAY_TF = {"5m", "15m", "30m", "1h"}

def _filter_rth(df):
    """Filter a DataFrame with naive-UTC DatetimeIndex to RTH bars only (9:30-16:00 ET)."""
    import pandas as _pd
    try:
        idx_et = df.index.tz_localize("UTC").tz_convert("America/New_York")
        rth = ((idx_et.hour > 9) | ((idx_et.hour == 9) & (idx_et.minute >= 30))) & (idx_et.hour < 16)
        df = df[rth.values].copy()
        df.index = df.index.tz_localize(None)
    except Exception as _e:
        log.warning("RTH filter skipped: %s", _e)
    return df


def init_db():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id          SERIAL PRIMARY KEY,
            ticker      TEXT,
            action      TEXT,
            sentiment   TEXT,
            quantity    TEXT,
            price       TEXT,
            tv_time     TEXT,
            interval    TEXT,
            received_at TEXT,
            strategy    TEXT,
            broker      TEXT,
            exec_status TEXT,
            exec_detail TEXT
        )
    """ if DATABASE_URL else """
        CREATE TABLE IF NOT EXISTS trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT,
            action      TEXT,
            sentiment   TEXT,
            quantity    TEXT,
            price       TEXT,
            tv_time     TEXT,
            interval    TEXT,
            received_at TEXT,
            strategy    TEXT,
            broker      TEXT,
            exec_status TEXT,
            exec_detail TEXT
        )
    """)
    conn.commit()

    # IB executions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ib_executions (
            exec_id     TEXT PRIMARY KEY,
            ts          TEXT,
            symbol      TEXT,
            sec_type    TEXT,
            side        TEXT,
            shares      REAL,
            price       REAL,
            order_id    INTEGER,
            account     TEXT,
            exchange    TEXT,
            pnl         REAL
        )
    """)
    conn.commit()

    # Migration: add deleted column to ib_executions if missing
    try:
        cur.execute("ALTER TABLE ib_executions ADD COLUMN deleted INTEGER DEFAULT 0")
        conn.commit()
    except Exception:
        conn.rollback()

    # Account snapshots table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS account_snapshots (
            id             SERIAL PRIMARY KEY,
            ts             TEXT,
            net_liq        REAL,
            realized_pnl   REAL,
            unrealized_pnl REAL
        )
    """ if DATABASE_URL else """
        CREATE TABLE IF NOT EXISTS account_snapshots (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            ts             TEXT,
            net_liq        REAL,
            realized_pnl   REAL,
            unrealized_pnl REAL
        )
    """)
    conn.commit()

    # Routing rules table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS routing_rules (
            id         SERIAL PRIMARY KEY,
            name       TEXT,
            enabled    INTEGER DEFAULT 1,
            nodes      TEXT,
            created_at TEXT
        )
    """ if DATABASE_URL else """
        CREATE TABLE IF NOT EXISTS routing_rules (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT,
            enabled    INTEGER DEFAULT 1,
            nodes      TEXT,
            created_at TEXT
        )
    """)
    conn.commit()

    # Migrations for existing databases
    for col in ("strategy TEXT", "broker TEXT", "exec_status TEXT", "exec_detail TEXT"):
        try:
            cur.execute(f"ALTER TABLE trades ADD COLUMN {col}")
            conn.commit()
        except Exception:
            conn.rollback()

    try:
        cur.execute("ALTER TABLE routing_rules ADD COLUMN sort_order INTEGER")
        conn.commit()
    except Exception:
        conn.rollback()

    # Add pine_code column to user_strategies if not present (migration)
    try:
        cur.execute("ALTER TABLE user_strategies ADD COLUMN pine_code TEXT")
        conn.commit()
    except Exception:
        conn.rollback()

    # App settings table (persists risk limits across restarts)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()

    # User-saved backtesting strategies (converted from Pine Script)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_strategies (
            slug       TEXT PRIMARY KEY,
            name       TEXT NOT NULL,
            code       TEXT NOT NULL,
            params     TEXT NOT NULL,
            created_at TEXT
        )
    """)
    conn.commit()

    # Optimization run results
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bt_results (
            id         SERIAL PRIMARY KEY,
            run_id     TEXT,
            strategy   TEXT,
            ticker     TEXT,
            timeframe  TEXT,
            params     TEXT,
            stats      TEXT,
            maximize   TEXT,
            score      REAL,
            created_at TEXT
        )
    """ if DATABASE_URL else """
        CREATE TABLE IF NOT EXISTS bt_results (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id     TEXT,
            strategy   TEXT,
            ticker     TEXT,
            timeframe  TEXT,
            params     TEXT,
            stats      TEXT,
            maximize   TEXT,
            score      REAL,
            created_at TEXT
        )
    """)
    conn.commit()

    conn.close()


def _load_setting(key, default=None):
    """Read a persisted app setting from the DB."""
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT value FROM app_settings WHERE key = %s" % placeholder(), (key,))
        row = cur.fetchone()
        conn.close()
        return row[0] if row else default
    except Exception:
        return default


def _save_setting(key, value):
    """Upsert an app setting to the DB."""
    p = placeholder()
    try:
        conn = get_db()
        cur  = conn.cursor()
        if DATABASE_URL:
            cur.execute(
                f"INSERT INTO app_settings (key, value) VALUES ({p}, {p}) "
                f"ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, str(value)),
            )
        else:
            cur.execute(
                f"INSERT OR REPLACE INTO app_settings (key, value) VALUES ({p}, {p})",
                (key, str(value)),
            )
        conn.commit()
        conn.close()
    except Exception as _e:
        log.warning("Failed to save setting %s: %s", key, _e)


def _insert_trade(cur, row):
    """Insert a trade row and return the new id."""
    p   = placeholder()
    phs = ",".join([p] * len(row))
    cols = ("ticker,action,sentiment,quantity,price,tv_time,"
            "interval,received_at,strategy,broker")
    if DATABASE_URL:
        cur.execute(
            f"INSERT INTO trades ({cols}) VALUES ({phs}) RETURNING id", row
        )
        return cur.fetchone()[0]
    cur.execute(f"INSERT INTO trades ({cols}) VALUES ({phs})", row)
    return cur.lastrowid


def _update_exec(cur, trade_id, exec_status, exec_detail):
    p = placeholder()
    cur.execute(
        f"UPDATE trades SET exec_status={p}, exec_detail={p} WHERE id={p}",
        (exec_status, exec_detail, trade_id),
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

# /webhook lives in routes/webhook.py — registered here once all module-level
# state (brokers, risk flags, db helpers) is defined above, so the blueprint's
# late `import app` lookups resolve cleanly.
from routes.webhook import webhook_bp
app.register_blueprint(webhook_bp)


@app.route("/api/trades")
def api_trades():
    limit = min(int(request.args.get("limit", 200)), 1000)
    p     = placeholder()
    conn  = get_db()
    cur   = conn.cursor()
    cur.execute(f"SELECT * FROM trades ORDER BY id DESC LIMIT {p}", (limit,))
    rows  = cur.fetchall()
    if DATABASE_URL:
        cols   = [d[0] for d in cur.description]
        result = [dict(zip(cols, r)) for r in rows]
    else:
        result = [dict(r) for r in rows]
    conn.close()
    return jsonify(result)


@app.route("/api/trades/<int:trade_id>", methods=["DELETE"])
def delete_trade(trade_id):
    conn = get_db()
    cur  = conn.cursor()
    p    = placeholder()
    cur.execute(f"DELETE FROM trades WHERE id={p}", (trade_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/trades/clear", methods=["POST"])
def clear_trades():
    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != WEBHOOK_TOKEN:
        abort(401)
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("DELETE FROM trades")
    conn.commit()
    conn.close()
    return jsonify({"status": "cleared"}), 200


@app.route("/api/broker/status")
def broker_status():
    brokers = {}
    brokers["IB"] = ib_broker.status() if ib_broker else {
        "connected": False, "broker": "IB", "note": "IB_HOST not set"
    }
    if ib_broker_live is not None:
        st = ib_broker_live.status()
        st["mode"] = "live"
        brokers["IB_LIVE"] = st
    if alpaca_broker is not None:
        brokers["Alpaca"] = alpaca_broker.status()
    if coinbase_broker is not None:
        brokers["Coinbase"] = coinbase_broker.status()
    return jsonify(brokers)


def _railway_ib_call(mutation_name):
    """Call a Railway GraphQL mutation on the IB Gateway service instance.
    Returns a status string."""
    import urllib.request as _urlreq
    railway_token  = os.environ.get("RAILWAY_API_TOKEN")
    ib_service_id  = os.environ.get("RAILWAY_IB_SERVICE_ID")
    environment_id = os.environ.get("RAILWAY_ENVIRONMENT_ID")
    if not (railway_token and ib_service_id):
        return "skipped — RAILWAY_API_TOKEN or RAILWAY_IB_SERVICE_ID not set"
    try:
        query = f"""
          mutation M($serviceId: String!, $environmentId: String) {{
            {mutation_name}(serviceId: $serviceId, environmentId: $environmentId)
          }}
        """
        payload = json.dumps({
            "query": query,
            "variables": {"serviceId": ib_service_id, "environmentId": environment_id},
        }).encode()
        req = _urlreq.Request(
            "https://backboard.railway.com/graphql/v2",
            data=payload,
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {railway_token}"},
        )
        with _urlreq.urlopen(req, timeout=10) as resp:
            resp_data = json.loads(resp.read())
        if resp_data.get("errors"):
            log.warning("Railway %s error: %s", mutation_name, resp_data["errors"])
            return "error: " + str(resp_data["errors"])
        log.info("Railway %s succeeded", mutation_name)
        return "ok"
    except Exception as e:
        log.warning("Railway API call failed: %s", e)
        return f"error: {e}"


@app.route("/api/broker/reconnect", methods=["POST"])
def broker_reconnect():
    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != WEBHOOK_TOKEN:
        abort(401)
    if ib_broker is None:
        return jsonify({"error": "IB_HOST not configured"}), 400
    global _ib_paused, _ib_live_paused
    # Resume the suspended Railway IB Gateway service (best-effort, non-blocking)
    gw = _railway_ib_call("serviceInstanceResume")
    log.info("serviceInstanceResume result: %s", gw)
    # Re-enable both background reconnect loops
    _ib_paused      = False
    _ib_live_paused = False
    # Return immediately; the JS side polls /api/broker/status until connected
    return jsonify({"started": True, "gateway": gw})


@app.route("/api/broker/disconnect", methods=["POST"])
def broker_disconnect():
    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != WEBHOOK_TOKEN:
        abort(401)
    if ib_broker is None:
        return jsonify({"error": "IB_HOST not configured"}), 400

    global _ib_paused, _ib_live_paused

    result = {"connected": False, "status": "disconnected", "gateway_restart": None}

    # Disconnect must run on the background thread that owns the ib_async event loop.
    # Set the pause flag first so the thread won't immediately reconnect after disconnect.
    if _ib_task_queue is not None:
        try:
            def _do_paper_disconnect():
                global _ib_paused
                _ib_paused = True
                if ib_broker.is_connected():
                    ib_broker.disconnect()
            _submit_ib_task(_do_paper_disconnect, _timeout=10)
        except Exception as e:
            log.warning("IB paper disconnect task error: %s", e)
            _ib_paused = True  # fallback: at least stop reconnect attempts
    else:
        _ib_paused = True

    if ib_broker_live is not None and _ib_live_task_queue is not None:
        try:
            def _do_live_disconnect():
                global _ib_live_paused
                _ib_live_paused = True
                if ib_broker_live.is_connected():
                    ib_broker_live.disconnect()
            _submit_ib_live_task(_do_live_disconnect, _timeout=10)
        except Exception as e:
            log.warning("IB live disconnect task error: %s", e)
            _ib_live_paused = True  # fallback
    else:
        _ib_live_paused = True

    # Suspend IB Gateway service on Railway (immediately kills the process)
    result["gateway_restart"] = _railway_ib_call("serviceInstanceSuspend")
    return jsonify(result)


@app.route("/api/broker/orders")
def broker_orders():
    """Return live open orders and recent fills directly from IB (no DB)."""
    if ib_broker is None:
        return jsonify({"error": "IB_HOST not configured"}), 400
    try:
        orders = []
        for t in ib_broker._ib.openTrades():
            orders.append({
                "order_id": t.order.orderId,
                "symbol":   t.contract.symbol,
                "action":   t.order.action,
                "qty":      t.order.totalQuantity,
                "type":     t.order.orderType,
                "status":   t.orderStatus.status,
                "filled":   t.orderStatus.filled,
                "avg_fill": t.orderStatus.avgFillPrice,
            })
        fills = ib_broker.executions()
        return jsonify({"open_orders": orders, "session_fills": fills})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/broker/eod-close", methods=["GET"])
def eod_close_status():
    return jsonify({"eod_close_enabled": eod_close_enabled})


@app.route("/api/broker/eod-close/toggle", methods=["POST"])
def eod_close_toggle():
    global eod_close_enabled
    eod_close_enabled = not eod_close_enabled
    log.info("EOD close toggled to %s", eod_close_enabled)
    return jsonify({"eod_close_enabled": eod_close_enabled})


@app.route("/api/alpaca/positions")
def alpaca_positions():
    """Return current open Alpaca positions with live unrealized P&L."""
    if alpaca_broker is None:
        log.warning("alpaca_positions: broker is None")
        return jsonify([])
    try:
        alpaca_broker._ensure_client()
        alpaca_broker._invalidate_pos_cache()
        raw = alpaca_broker._trading.get_all_positions()
        raw_count = len(raw) if raw else 0
        positions = alpaca_broker.get_positions()
        return jsonify({
            "positions": positions,
            "_debug": {"paper": alpaca_broker._paper, "raw_count": raw_count},
        })
    except Exception as e:
        log.error("alpaca_positions failed: %s", e, exc_info=True)
        return jsonify({"positions": [], "_debug": {"error": str(e)}})


@app.route("/api/risk/status")
def risk_status():
    pnl = _compute_daily_pnl()
    with _risk_lock:
        halted     = _risk_halted
        blocked    = dict(_blocked_strategies)
        positions  = list(_latest_positions)
    return jsonify({
        "halted":               halted,
        "max_daily_loss":       MAX_DAILY_LOSS if MAX_DAILY_LOSS != 0 else None,
        "current_pnl":          round(pnl, 2) if pnl is not None else None,
        "enabled":              MAX_DAILY_LOSS < 0,
        "max_position_loss":    MAX_POSITION_LOSS if MAX_POSITION_LOSS != 0 else None,
        "position_stop_enabled": MAX_POSITION_LOSS < 0,
        "positions":            positions,
        "blocked_strategies":   blocked,
    })


def _update_env_file(key, value):
    """Update or insert KEY=value in the .env file next to app.py.
    Preserves all other lines. Safe to call even if the file doesn't exist."""
    import re as _re
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    try:
        try:
            with open(env_path, "r") as _f:
                content = _f.read()
        except FileNotFoundError:
            content = ""
        line    = f"{key}={value}"
        pattern = rf"^{_re.escape(key)}=.*$"
        if _re.search(pattern, content, _re.MULTILINE):
            content = _re.sub(pattern, line, content, flags=_re.MULTILINE)
        else:
            content = content.rstrip("\n") + ("\n" if content else "") + line + "\n"
        with open(env_path, "w") as _f:
            _f.write(content)
        log.info("Updated .env: %s=%s", key, value)
    except Exception as _e:
        log.warning("Failed to update .env: %s", _e)


@app.route("/api/risk/limit", methods=["POST"])
def risk_set_limit():
    global MAX_DAILY_LOSS, MAX_POSITION_LOSS
    data = request.get_json(silent=True) or {}
    changed = []
    if "max_daily_loss" in data:
        try:
            MAX_DAILY_LOSS = float(data["max_daily_loss"])
        except (TypeError, ValueError):
            return jsonify({"error": "max_daily_loss must be a number"}), 400
        _update_env_file("MAX_DAILY_LOSS", f"{MAX_DAILY_LOSS:g}")
        _save_setting("MAX_DAILY_LOSS", f"{MAX_DAILY_LOSS:g}")
        log.info("MAX_DAILY_LOSS updated to %g", MAX_DAILY_LOSS)
        changed.append("max_daily_loss")
    if "max_position_loss" in data:
        try:
            MAX_POSITION_LOSS = float(data["max_position_loss"])
        except (TypeError, ValueError):
            return jsonify({"error": "max_position_loss must be a number"}), 400
        _update_env_file("MAX_POSITION_LOSS", f"{MAX_POSITION_LOSS:g}")
        _save_setting("MAX_POSITION_LOSS", f"{MAX_POSITION_LOSS:g}")
        log.info("MAX_POSITION_LOSS updated to %g", MAX_POSITION_LOSS)
        changed.append("max_position_loss")
    return jsonify({
        "max_daily_loss":    MAX_DAILY_LOSS,
        "max_position_loss": MAX_POSITION_LOSS,
        "changed":           changed,
    })


@app.route("/api/risk/reset", methods=["POST"])
def risk_reset():
    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != WEBHOOK_TOKEN:
        abort(401)
    global _risk_halted
    with _risk_lock:
        _risk_halted = False
    log.info("Risk halt manually cleared")
    return jsonify({"halted": False})


@app.route("/api/risk/unblock/<path:strategy_name>", methods=["POST"])
def risk_unblock_strategy(strategy_name):
    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != WEBHOOK_TOKEN:
        abort(401)
    with _risk_lock:
        removed = _blocked_strategies.pop(strategy_name, None)
        _auto_closed_symbols.discard(
            removed["symbol"] if removed else ""
        )
    if removed:
        log.info("Strategy '%s' unblocked manually", strategy_name)
        return jsonify({"unblocked": strategy_name})
    return jsonify({"error": "strategy not found"}), 404


@app.route("/api/routing/rules", methods=["GET"])
def routing_rules_list():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT id, name, enabled, nodes, created_at FROM routing_rules ORDER BY COALESCE(sort_order, id) ASC")
    rows = cur.fetchall()
    conn.close()
    if DATABASE_URL:
        cols = [d[0] for d in cur.description]
        result = [dict(zip(cols, r)) for r in rows]
    else:
        result = [dict(r) for r in rows]
    for r in result:
        if isinstance(r["nodes"], str):
            try:
                r["nodes"] = json.loads(r["nodes"])
            except Exception:
                r["nodes"] = []
    return jsonify(result)


@app.route("/api/routing/rules", methods=["POST"])
def routing_rules_create():
    data = request.get_json(silent=True) or {}
    name  = data.get("name", "New Pipeline")
    nodes = json.dumps(data.get("nodes", []))
    ts    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn  = get_db()
    cur   = conn.cursor()
    p     = placeholder()
    if DATABASE_URL:
        cur.execute(
            f"INSERT INTO routing_rules (name,enabled,nodes,created_at) VALUES ({p},{p},{p},{p}) RETURNING id",
            (name, 1, nodes, ts),
        )
        new_id = cur.fetchone()[0]
    else:
        cur.execute(
            f"INSERT INTO routing_rules (name,enabled,nodes,created_at) VALUES ({p},{p},{p},{p})",
            (name, 1, nodes, ts),
        )
        new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return jsonify({"id": new_id, "name": name, "enabled": 1, "nodes": data.get("nodes", [])})


@app.route("/api/routing/rules/<int:rule_id>", methods=["PUT"])
def routing_rules_update(rule_id):
    data = request.get_json(silent=True) or {}
    conn = get_db()
    cur  = conn.cursor()
    p    = placeholder()
    fields, vals = [], []
    if "name" in data:
        fields.append(f"name={p}"); vals.append(data["name"])
    if "enabled" in data:
        fields.append(f"enabled={p}"); vals.append(int(data["enabled"]))
    if "nodes" in data:
        fields.append(f"nodes={p}"); vals.append(json.dumps(data["nodes"]))
    if not fields:
        conn.close()
        return jsonify({"error": "nothing to update"}), 400
    vals.append(rule_id)
    cur.execute(f"UPDATE routing_rules SET {','.join(fields)} WHERE id={p}", vals)
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/routing/rules/<int:rule_id>", methods=["DELETE"])
def routing_rules_delete(rule_id):
    conn = get_db()
    cur  = conn.cursor()
    p    = placeholder()
    cur.execute(f"DELETE FROM routing_rules WHERE id={p}", (rule_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/routing/rules/<int:rule_id>/duplicate", methods=["POST"])
def routing_rules_duplicate(rule_id):
    conn = get_db()
    cur  = conn.cursor()
    p    = placeholder()
    cur.execute(f"SELECT name, nodes FROM routing_rules WHERE id={p}", (rule_id,))
    row  = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "not found"}), 404
    name  = (row[0] if DATABASE_URL else row["name"]) + " (copy)"
    nodes = row[1] if DATABASE_URL else row["nodes"]
    ts    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if DATABASE_URL:
        cur.execute(
            f"INSERT INTO routing_rules (name,enabled,nodes,created_at) VALUES ({p},{p},{p},{p}) RETURNING id",
            (name, 1, nodes, ts),
        )
        new_id = cur.fetchone()[0]
    else:
        cur.execute(
            f"INSERT INTO routing_rules (name,enabled,nodes,created_at) VALUES ({p},{p},{p},{p})",
            (name, 1, nodes, ts),
        )
        new_id = cur.lastrowid
    conn.commit()
    conn.close()
    nodes_parsed = json.loads(nodes) if isinstance(nodes, str) else nodes
    return jsonify({"id": new_id, "name": name, "enabled": 1, "nodes": nodes_parsed})


@app.route("/api/routing/rules/reorder", methods=["POST"])
def routing_rules_reorder():
    order = request.get_json(silent=True) or []  # [{id, sort_order}, ...]
    conn  = get_db()
    cur   = conn.cursor()
    p     = placeholder()
    for item in order:
        cur.execute(f"UPDATE routing_rules SET sort_order={p} WHERE id={p}",
                    (item["sort_order"], item["id"]))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/crypto/price")
def api_crypto_price():
    symbol  = request.args.get("symbol", "BTC").upper()
    product = f"{symbol}-USD"
    try:
        import urllib.request as _urllib
        import json as _json
        url = f"https://api.coinbase.com/v2/prices/{product}/spot"
        with _urllib.urlopen(url, timeout=5) as resp:
            price = float(_json.loads(resp.read())["data"]["amount"])
        return jsonify({"symbol": symbol, "price": price, "currency": "USD"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/broker/close-all", methods=["POST"])
def broker_close_all():
    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != WEBHOOK_TOKEN:
        abort(401)
    results = {}
    errors  = {}
    if alpaca_broker is not None:
        try:
            results["alpaca"] = alpaca_broker.close_all_positions()
        except Exception as e:
            errors["alpaca"] = str(e)
            log.error("close_all alpaca failed: %s", e)
    if ib_broker is not None:
        try:
            results["ib"] = _submit_ib_task(ib_broker.close_all_positions, _timeout=60)
        except Exception as e:
            errors["ib"] = str(e)
            log.error("close_all ib failed: %s", e)
    if not results and not errors:
        return jsonify({"error": "No brokers configured"}), 400
    closed_count = sum(
        len(v) if isinstance(v, list) else (1 if v else 0)
        for v in results.values()
    )
    return jsonify({"closed": closed_count, "detail": results, "errors": errors})


@app.route("/api/alpaca/close/<symbol>", methods=["POST"])
def alpaca_close_position(symbol):
    """Manually close a single Alpaca position by symbol."""
    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != WEBHOOK_TOKEN:
        abort(401)
    if alpaca_broker is None:
        return jsonify({"success": False, "error": "Alpaca broker not initialised"}), 400
    symbol = symbol.upper()
    result = alpaca_broker.close_position(symbol)
    if result.get("success"):
        log.info("Manual close: %s position closed via UI", symbol)
        global _alpaca_fills_cache
        _alpaca_fills_cache = {"data": [], "ts": 0.0}  # force fresh fetch on next load
        return jsonify(result)
    return jsonify(result), 400


@app.route("/")
def dashboard():
    return render_template("index.html")


@app.route("/routing")
def routing_page():
    return render_template("routing.html", webhook_token=WEBHOOK_TOKEN)


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/strategies")
def strategies():
    return render_template("strategies.html")



@app.route("/optimize")
def optimize_page():
    return render_template("optimize.html")


def _extract_strategy_params(code):
    """Auto-detect class-level numeric parameters from a backtesting.py Strategy class."""
    import re
    params, seen = [], set()
    for m in re.finditer(r'^    (\w+)\s*=\s*(\d+(?:\.\d+)?)(?:\s*#.*)?$', code, re.MULTILINE):
        name, val_str = m.group(1), m.group(2)
        if name in seen or name.startswith('_'):
            continue
        seen.add(name)
        val    = float(val_str)
        is_int = '.' not in val_str
        label  = name.replace('_', ' ').title()
        if is_int:
            iv = int(val)
            params.append({"id": name, "label": label, "type": "int",
                           "default": iv, "min": max(1, iv // 4), "max": max(iv * 4, iv + 10), "step": 1})
        else:
            params.append({"id": name, "label": label, "type": "float",
                           "default": val, "min": round(max(0.1, val / 4), 1),
                           "max": round(val * 4, 1), "step": 0.1})
    return params


_PARAM_PRIORITY = {
    "cam_mult": 1, "camarilla_mult": 1,
    "loss_dollars": 2, "loss_points": 2, "stop_loss": 2,
    "trail_points_dollars": 3, "trail_points": 3, "trail_activation": 3,
    "trail_offset_dollars": 4, "trail_offset": 4,
    "ema_period": 5,
    "tick_size": 99,
}

def _sort_params(params):
    return sorted(params, key=lambda p: _PARAM_PRIORITY.get(p["id"], 50))


@app.route("/api/bt/strategies")
def bt_strategies_list():
    """Return built-in + user-saved strategies with their parameter schemas."""
    from strategies.bt_strategies import STRATEGIES
    result = {name: params for name, (_, params) in STRATEGIES.items()}
    # Append user-saved strategies
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT slug, name, params FROM user_strategies ORDER BY created_at ASC")
        rows = cur.fetchall()
        conn.close()
        for row in rows:
            r = dict(row) if not DATABASE_URL else dict(zip([d[0] for d in cur.description], row))
            result[r["slug"]] = _sort_params(json.loads(r["params"]))
    except Exception as _e:
        log.warning("Failed to load user strategies: %s", _e)
    return jsonify(result)


@app.route("/api/bt/strategies/save", methods=["POST"])
def bt_strategy_save():
    """Save a converted Pine Script strategy to the DB."""
    import re
    body = request.get_json(silent=True) or {}
    name      = (body.get("name")      or "").strip()
    code      = (body.get("code")      or "").strip()
    pine_code = (body.get("pine_code") or "").strip()
    if not name or not code:
        return jsonify({"error": "name and code are required"}), 400
    slug   = re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_') or "strategy"
    params = _extract_strategy_params(code)
    p      = placeholder()
    try:
        conn = get_db()
        cur  = conn.cursor()
        if DATABASE_URL:
            cur.execute(
                f"INSERT INTO user_strategies (slug, name, code, pine_code, params, created_at) "
                f"VALUES ({p},{p},{p},{p},{p},{p}) "
                f"ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name, code=EXCLUDED.code, pine_code=EXCLUDED.pine_code, params=EXCLUDED.params",
                (slug, name, code, pine_code or None, json.dumps(params), time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            )
        else:
            cur.execute(
                f"INSERT OR REPLACE INTO user_strategies (slug, name, code, pine_code, params, created_at) "
                f"VALUES ({p},{p},{p},{p},{p},{p})",
                (slug, name, code, pine_code or None, json.dumps(params), time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            )
        conn.commit()
        conn.close()
        return jsonify({"slug": slug, "name": name, "params": params})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/bt/strategies/<slug>", methods=["DELETE"])
def bt_strategy_delete(slug):
    """Delete a user-saved strategy."""
    p = placeholder()
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(f"DELETE FROM user_strategies WHERE slug = {p}", (slug,))
        conn.commit()
        conn.close()
        return jsonify({"deleted": slug})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/bt/strategies/<slug>/pine-export", methods=["POST"])
def bt_pine_export(slug):
    """
    Return a modified Pine Script with optimized param values substituted in.
    Body: { "params": {"stop_loss": 0.50, "ema_period": 12, ...} }
    """
    import re as _re
    body   = request.get_json(silent=True) or {}
    params = body.get("params", {})

    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute(f"SELECT pine_code, name FROM user_strategies WHERE slug = {placeholder()}", (slug,))
        row = cur.fetchone(); conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not row:
        return jsonify({"error": "Strategy not found"}), 404

    pine_code = (row[0] if DATABASE_URL else row["pine_code"]) or ""
    name      = (row[1] if DATABASE_URL else row["name"])

    if not pine_code:
        return jsonify({"error": "No original Pine Script stored for this strategy. Re-upload it to save the source."}), 400

    # ── Pass 1: substitute existing input() declarations ──────────────────
    modified = pine_code
    input_subs_made = set()
    for param_name, new_val in params.items():
        val_str = str(int(new_val)) if float(new_val) == int(float(new_val)) else str(round(float(new_val), 4))
        pn = _re.escape(param_name)
        before = modified
        modified = _re.sub(
            r'(' + pn + r'\s*=\s*input(?:\.(?:float|int|source))?\s*\(\s*defval\s*=\s*)[^\s,)]+',
            lambda m, v=val_str: m.group(1) + v, modified,
        )
        modified = _re.sub(
            r'(' + pn + r'\s*=\s*input(?:\.(?:float|int|source))?\s*\(\s*)([+-]?[\d.]+)',
            lambda m, v=val_str: m.group(1) + v, modified,
        )
        if modified != before:
            input_subs_made.add(param_name)

    # ── Pass 2: inject input block + replace hardcoded values ─────────────
    # For params that weren't handled via input() declarations above, inject
    # a parameterised block and patch the hardcoded values in the script body.
    remaining = {k: v for k, v in params.items() if k not in input_subs_made and k != 'tick_size'}
    if remaining:
        tick_size = float(params.get('tick_size', 0.01)) or 0.01

        lines = []
        for pname, pval in remaining.items():
            fval = float(pval)
            is_int = (fval == int(fval)) and ('period' in pname or 'len' in pname or pname == 'ema_period')
            if is_int:
                lines.append(f"_opt_{pname} = input.int({int(fval)}, '{pname.replace('_',' ').title()}', minval=1)")
            else:
                lines.append(f"_opt_{pname} = input.float({round(fval,4)}, '{pname.replace('_',' ').title()}', step=0.1)")

        # Dollar-amount params: emit tick conversion helpers (short names)
        dollar_params = {k: v for k, v in remaining.items() if k.endswith('_dollars')}
        for pname in dollar_params:
            short = pname[:-len('_dollars')]  # e.g. trail_points_dollars → trail_points
            lines.append(f"_opt_{short}_ticks = math.round(_opt_{pname} / syminfo.mintick)")

        inject = (
            '\n// ── Optimized parameters (from Python backtester) ──────────────\n'
            + '\n'.join(lines)
            + '\n// ─────────────────────────────────────────────────────────────────\n'
        )

        # Insert block after the closing ) of strategy(...) declaration
        modified = _re.sub(r'(strategy\s*\([^)]*\))', r'\1' + inject, modified, count=1)

        # Now patch hardcoded values in the body
        for pname, pval in remaining.items():
            fval = float(pval)

            if pname == 'cam_mult' or 'cam' in pname:
                # Replace the Camarilla multiplier literal in h4/l4 formulas
                modified = _re.sub(
                    r'(\*\s*)[\d.]+(\s*/\s*2\.0)',
                    lambda m, v=round(fval,4): m.group(1) + str(v) + m.group(2),
                    modified,
                )
            elif pname == 'ema_period':
                # Replace ta.ema(close, N) period
                modified = _re.sub(
                    r'(ta\.ema\s*\(\s*\w+\s*,\s*)\d+',
                    lambda m, v=int(fval): m.group(1) + str(v),
                    modified,
                )
            elif pname.endswith('_dollars'):
                pine_key = pname[:-len('_dollars')]  # trail_points_dollars → trail_points
                tick_var = f'_opt_{pine_key}_ticks'
                # Replace  trail_points  = <num>  /  loss  = <num>  inside strategy.exit()
                modified = _re.sub(
                    r'(' + _re.escape(pine_key) + r'\s*=\s*)\d+',
                    lambda m, v=tick_var: m.group(1) + v,
                    modified,
                )

    resp = make_response(modified)
    safe_name = _re.sub(r'[^a-z0-9_]', '_', name.lower())
    resp.headers['Content-Type'] = 'text/plain; charset=utf-8'
    resp.headers['Content-Disposition'] = f'attachment; filename="{safe_name}_optimized.pine"'
    return resp


@app.route("/api/bt/strategies/meta")
def bt_strategies_meta():
    """Return display names + slugs for all strategies (built-in + user)."""
    from strategies.bt_strategies import STRATEGIES
    result = [{"slug": k, "name": k.replace('_', ' ').title(), "user": False}
              for k in STRATEGIES]
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT slug, name FROM user_strategies ORDER BY created_at ASC")
        rows = cur.fetchall()
        conn.close()
        for row in rows:
            r = dict(row) if not DATABASE_URL else dict(zip([d[0] for d in cur.description], row))
            result.append({"slug": r["slug"], "name": r["name"], "user": True})
    except Exception:
        pass
    return jsonify(result)


@app.route("/api/bt/convert", methods=["POST"])
def bt_convert():
    """Stream a Pine Script â†’ backtesting.py Strategy class via Claude."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 503
    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({"error": "anthropic package not installed"}), 503

    body        = request.get_json(silent=True) or {}
    pine_script = (body.get("pine_script") or "").strip()
    if not pine_script:
        return jsonify({"error": "No Pine Script provided"}), 400

    system = (
        "You are an expert in both TradingView Pine Script and the Python backtesting.py library.\n"
        "Convert the given Pine Script strategy to a complete, runnable backtesting.py Strategy class.\n\n"
        "STRICT RULES:\n"
        "1. Output ONLY valid Python code - no markdown fences, no explanation text\n"
        "2. Start with: from backtesting import Strategy\\nimport numpy as np\\nimport pandas as pd\n"
        "3. Do NOT import ta-lib, pandas_ta, or any library not in the standard library / numpy / pandas\n"
        "4. Implement ALL indicators manually using numpy/pandas rolling/ewm/etc.\n"
        "5. All indicator arrays must be wrapped in self.I() inside init()\n"
        "6. All tuneable parameters must be class-level attributes with sensible defaults\n"
        "7. Entry: self.buy(...) / self.sell(...) with sl= and tp= where applicable\n"
        "8. Exit: self.position.close()\n"
        "9. Always guard against NaN at the start of next() before trading\n"
        "10. The class name must end in 'Strategy'\n"
        "11. If the Pine Script uses intraday sessions or repainting, simplify to daily bar logic\n"
        "12. Include a short docstring describing the strategy\n"
        "13. NEVER use self.position.entry_price - it does not exist. Track entry price manually: "
        "set self._entry = self.data.Close[-1] when entering, then read self._entry in next()\n"
        "14. NEVER use self.position.size to get fill price. Use self._entry as above\n"
        "15. Valid Position attributes: .is_long .is_short .pl .pl_pct .close() only\n"
        "16. Set class attribute _trade_on_close = True so trades execute on bar close like Pine Script\n"
        "17. CRITICAL - Entry crossover detection: match EXACTLY what the Pine Script uses.\n"
        "   (a) If Pine uses ta.crossover(close, L) or ta.crossunder(close, L): use Close[-2]/Close[-1].\n"
        "       long:  (self.data.Close[-2] < L) and (self.data.Close[-1] >= L)\n"
        "       short: (self.data.Close[-2] > L) and (self.data.Close[-1] <= L)\n"
        "   (b) If Pine uses explicit 'close > L and open < L' (same-bar crossover): use Open[-1]/Close[-1].\n"
        "       long:  (self.data.Open[-1] < L) and (self.data.Close[-1] > L)\n"
        "       short: (self.data.Open[-1] > L) and (self.data.Close[-1] < L)\n"
        "   NEVER use just 'close > L' alone — that fires on every bar above the level.\n"
        "18. Only enter on the crossover bar. The crossover check itself prevents re-entry on subsequent bars. "
        "Do NOT add any latch or cooldown — it will block valid re-entries and produce far fewer trades than TradingView.\n"
        "19. Always guard len(self.data.Close) >= 2 (i.e. check self.data.Close[-2] exists) before accessing Close[-2]\n"
        "20. CRITICAL - STOP/TRAIL PARAMETER UNITS: TradingView strategy.exit() trail_points, trail_offset, and stop "
        "parameters are in TICKS (syminfo.mintick units). For US equities mintick=$0.01, so trail_points=40 in Pine = "
        "$0.40 in Python. You MUST convert: multiply all tick-based Pine parameters by 0.01 when setting Python defaults. "
        "Example: Pine trail_points=40 → Python trail_points_dollars = 0.40. "
        "Also add a tick_size class attribute (default 0.01) so users can adjust for other instruments. "
        "In next(), use: trail_activation = trail_points * tick_size, stop_dist = loss_points * tick_size. "
        "If you use these parameters as raw dollar class attributes, set sensible DOLLAR defaults (e.g. 0.40, 0.10, 0.80) "
        "NOT the raw tick integers from Pine Script (40, 10, 80).\n"
        "21. CRITICAL - DAILY LEVELS ON INTRADAY BARS (Camarilla, Pivot Points, Daily VWAP, etc.): "
        "If the Pine Script fetches previous-day H/L/C (e.g. request.security(...,'D',...)) to compute levels, "
        "you MUST resample the intraday bar data to daily, shift by 1 day, then forward-fill back to intraday. "
        "NEVER just do pd.Series(high).shift(1) on raw intraday bars — that gives previous-BAR H/L/C, not previous-DAY. "
        "CORRECT pattern (use in init() BEFORE wrapping in self.I()):\n"
        "    import pandas as pd\n"
        "    idx = pd.DatetimeIndex(self.data.index)\n"
        "    s_high = pd.Series(self.data.High, index=idx)\n"
        "    s_low  = pd.Series(self.data.Low,  index=idx)\n"
        "    s_close= pd.Series(self.data.Close,index=idx)\n"
        "    d_high = s_high.resample('B').max().shift(1).reindex(idx, method='ffill')\n"
        "    d_low  = s_low.resample('B').min().shift(1).reindex(idx, method='ffill')\n"
        "    d_close= s_close.resample('B').last().shift(1).reindex(idx, method='ffill')\n"
        "    h4_vals = (d_close + (d_high - d_low) * cam_mult / 2.0).values  # or whatever formula\n"
        "    l4_vals = (d_close - (d_high - d_low) * cam_mult / 2.0).values\n"
        "    self.h4 = self.I(lambda v: v, h4_vals)\n"
        "    self.l4 = self.I(lambda v: v, l4_vals)\n"
        "This produces one H4/L4 value per trading day that is constant for all intraday bars in that day, "
        "exactly matching TradingView's request.security daily behaviour."
    )

    def generate():
        try:
            client = _anthropic.Anthropic(api_key=api_key)
            with client.messages.stream(
                model="claude-opus-4-7",
                max_tokens=3000,
                system=system,
                messages=[{"role": "user", "content":
                    f"Convert this Pine Script strategy to backtesting.py:\n\n{pine_script}"}],
            ) as stream:
                for text in stream.text_stream:
                    yield f"data: {json.dumps({'type': 'chunk', 'text': text})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'msg': str(e)})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/bt/run", methods=["POST"])
def bt_run():
    import math
    import pandas as _pd
    try:
        from backtesting import Backtest
        from strategies.bt_strategies import STRATEGIES
    except ImportError as e:
        return jsonify({"error": f"Missing dependency: {e}"}), 503

    body           = request.get_json(silent=True) or {}
    ticker         = (body.get("ticker") or "AAPL").strip().upper()
    start_date     = body.get("start_date", "2022-01-01")
    end_date       = body.get("end_date",   "2024-12-31")
    timeframe      = body.get("timeframe",  "1d")
    cash           = float(body.get("cash", 10000))
    commission     = float(body.get("commission", 0.0))
    data_source    = body.get("data_source", "yfinance")
    strategy_type  = body.get("strategy_type", "builtin")   # "builtin" | "converted" | "saved"
    strategy_name  = body.get("strategy_name", "camarilla")
    strategy_code  = body.get("strategy_code", "")
    strategy_params = body.get("params", {})

    # â"€â"€ Resolve strategy class â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
    strategy_cls = None
    if strategy_type == "saved":
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(f"SELECT code FROM user_strategies WHERE slug = {placeholder()}", (strategy_name,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return jsonify({"error": f"Saved strategy not found: {strategy_name}"}), 404
        strategy_code = row[0]
        strategy_type = "converted"  # reuse converted exec path
    if strategy_type == "converted" and strategy_code:
        try:
            from backtesting import Strategy as _Strategy
            import numpy as _np, pandas as _pd
            ns = {"Strategy": _Strategy, "np": _np, "numpy": _np, "pd": _pd, "pandas": _pd}
            exec(strategy_code, ns)
            strategy_cls = next(
                (v for v in ns.values()
                 if isinstance(v, type) and issubclass(v, _Strategy) and v is not _Strategy),
                None,
            )
            if strategy_cls is None:
                return jsonify({"error": "No Strategy subclass found in converted code"}), 400
            strategy_cls.__module__ = "__main__"
            strategy_cls.__qualname__ = strategy_cls.__name__
            # Register on __main__ so pickle can resolve the class if
            # backtesting.py's Pool ever falls back to a real subprocess
            # pool (spawn/fork). Without this, only ThreadPool paths work.
            setattr(sys.modules["__main__"], strategy_cls.__name__, strategy_cls)
        except Exception as e:
            return jsonify({"error": f"Strategy code error: {e}"}), 400
    else:
        entry = STRATEGIES.get(strategy_name)
        if not entry:
            return jsonify({"error": f"Unknown strategy: {strategy_name}"}), 400
        strategy_cls = entry[0]

    # â"€â"€ Fetch data â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
    try:
        import pandas as _pd
        if data_source == "alpaca":
            from strategies.data import fetch_bars_alpaca
            raw_bars = fetch_bars_alpaca(ticker, start_date, end_date, timeframe)
        else:
            from strategies.data import fetch_bars
            raw_bars = fetch_bars(ticker, start_date, end_date, timeframe)
        if len(raw_bars) < 30:
            return jsonify({"error": f"Only {len(raw_bars)} bars - need at least 30 (yfinance caps intraday: 5m=60d, 1h=730d)"}), 400
        df = _pd.DataFrame(raw_bars).set_index("time")
        df.index = _pd.to_datetime(df.index)
        df.columns = [c.title() for c in df.columns]
        df = df[["Open", "High", "Low", "Close"]].dropna()
        df["Volume"] = 0
        if timeframe in _INTRADAY_TF:
            df = _filter_rth(df)
    except Exception as e:
        return jsonify({"error": f"Data fetch failed: {e}"}), 500

    # â"€â"€ Run backtest â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
    try:
        bt    = Backtest(df, strategy_cls, cash=cash, commission=commission, exclusive_orders=True,
                        trade_on_close=getattr(strategy_cls, '_trade_on_close', False))
        # Cast params to correct types
        typed_params = {}
        for k, v in strategy_params.items():
            try:
                default = getattr(strategy_cls, k, v)
                typed_params[k] = type(default)(v)
            except Exception:
                typed_params[k] = v
        stats = bt.run(**typed_params)

        n_trades = int(stats.get("# Trades") or 0)
        n_days   = max(1, len(df) // (78 if timeframe in _INTRADAY_TF else 1))
        if timeframe in _INTRADAY_TF and n_trades > n_days * 20:
            stats_dict_warn = {"_conversion_warning":
                f"WARNING: {n_trades} trades over ~{n_days} days ({n_trades/n_days:.1f}/day) suggests the "
                f"converted strategy is entering on every bar above the level rather than only on crossover bars. "
                f"Re-upload your Pine Script — ensure entry uses open<level AND close>level (crossover), not just close>level."}
        else:
            stats_dict_warn = {}

        def _safe(v):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            try:
                import pandas as _pd
                if isinstance(v, (_pd.Series, _pd.DataFrame)):
                    return None
            except Exception:
                pass
            try:
                json.dumps(v); return v
            except Exception:
                return str(v)

        stats_dict = {k: _safe(v) for k, v in stats.items()
                      if k not in ("_strategy", "_trades", "_equity_curve")}
        stats_dict.update(stats_dict_warn)

        trades_list = []
        trades_df = stats.get("_trades")
        if trades_df is not None and not trades_df.empty:
            for _, row in trades_df.iterrows():
                trades_list.append({
                    "entry_time":  str(row.get("EntryTime",  "")),
                    "exit_time":   str(row.get("ExitTime",   "")),
                    "direction":   "Long" if row.get("Size", 0) > 0 else "Short",
                    "size":        abs(int(row.get("Size", 0))),
                    "entry_price": round(float(row.get("EntryPrice", 0)), 4),
                    "exit_price":  round(float(row.get("ExitPrice",  0)), 4),
                    "pnl":         round(float(row.get("PnL", 0)), 2),
                    "return_pct":  round(float(row.get("ReturnPct", 0)) * 100, 2),
                })

        eq_curve = []
        if "_equity_curve" in stats:
            eq      = stats["_equity_curve"]["Equity"]
            step_eq = max(1, len(eq) // 600)
            for t, v in eq.iloc[::step_eq].items():
                try:    ts_int = int(t.timestamp())
                except Exception: ts_int = int(_pd.Timestamp(t).timestamp())
                eq_curve.append({"time": ts_int, "value": round(float(v), 2)})

        # OHLCV bars for Lightweight Charts (capped at 1500 candles)
        ohlcv_list = []
        step_bars = max(1, len(df) // 1500)
        for ts, row in df.iloc[::step_bars].iterrows():
            try:    t_int = int(ts.timestamp())
            except Exception: t_int = int(_pd.Timestamp(ts).timestamp())
            ohlcv_list.append({
                "time":  t_int,
                "open":  round(float(row["Open"]),  4),
                "high":  round(float(row["High"]),  4),
                "low":   round(float(row["Low"]),   4),
                "close": round(float(row["Close"]), 4),
            })

        return jsonify({"stats": stats_dict, "trades": trades_list,
                        "equity": eq_curve, "ohlcv": ohlcv_list, "ticker": ticker})

    except Exception as e:
        log.exception("bt_run error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/bt/optimize", methods=["POST"])
def bt_optimize():
    """
    Streaming SSE endpoint.  For each ticker Ã— timeframe, runs backtesting.py
    bt.optimize() over the supplied param ranges and streams each result row.
    """
    from flask import Response, stream_with_context
    import json as _json

    body           = request.get_json(silent=True) or {}
    strategy_type  = body.get("strategy_type", "builtin")
    strategy_name  = body.get("strategy_name", "camarilla")
    tickers        = [t.strip().upper() for t in body.get("tickers", ["AAPL"]) if t.strip()]
    timeframes     = body.get("timeframes", ["1d"])
    start_date     = body.get("start_date", "2022-01-01")
    end_date       = body.get("end_date",   "2024-12-31")
    cash           = float(body.get("cash", 10000))
    commission     = float(body.get("commission", 0.0))
    data_source    = body.get("data_source", "yfinance")
    maximize       = body.get("maximize",    "Sharpe Ratio")
    param_ranges   = body.get("param_ranges", {})
    strategy_code  = body.get("strategy_code", "")

    class _NpEnc(_json.JSONEncoder):
        def default(self, o):
            import numpy as _np
            if isinstance(o, _np.integer): return int(o)
            if isinstance(o, _np.floating): return float(o)
            if isinstance(o, _np.ndarray): return o.tolist()
            return super().default(o)

    def _sse(obj):
        return f"data: {_json.dumps(obj, cls=_NpEnc)}\n\n"

    def generate():
        try:
            from backtesting import Backtest, Strategy as _Strategy
            from strategies.bt_strategies import STRATEGIES
            import numpy as _np
            import pandas as _pd
        except ImportError as e:
            yield _sse({"type": "error", "msg": f"Missing dependency: {e}"})
            return

        # â"€â"€ Resolve strategy class â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        strategy_cls = None
        if strategy_type == "saved":
            conn = get_db(); cur = conn.cursor()
            cur.execute(f"SELECT code FROM user_strategies WHERE slug = {placeholder()}", (strategy_name,))
            row = cur.fetchone(); conn.close()
            if not row:
                yield _sse({"type": "error", "msg": f"Strategy not found: {strategy_name}"}); return
            exec_code = row[0]
        elif strategy_type == "converted" and strategy_code:
            exec_code = strategy_code
        else:
            exec_code = None

        if exec_code:
            ns = {"Strategy": _Strategy, "np": _np, "numpy": _np, "pd": _pd, "pandas": _pd}
            try:
                exec(exec_code, ns)
            except Exception as e:
                yield _sse({"type": "error", "msg": f"Strategy code error: {e}"}); return
            strategy_cls = next(
                (v for v in ns.values() if isinstance(v, type) and issubclass(v, _Strategy) and v is not _Strategy), None)
            if strategy_cls is not None:
                strategy_cls.__module__ = "__main__"
                strategy_cls.__qualname__ = strategy_cls.__name__
                setattr(sys.modules["__main__"], strategy_cls.__name__, strategy_cls)
        else:
            entry = STRATEGIES.get(strategy_name)
            if entry:
                strategy_cls = entry[0]

        if strategy_cls is None:
            yield _sse({"type": "error", "msg": "Could not resolve strategy class"}); return

        # â"€â"€ Build param sequences â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        opt_kwargs = {}
        for pname, prange in param_ranges.items():
            pmin  = float(prange.get("min",  1))
            pmax  = float(prange.get("max",  10))
            pstep = float(prange.get("step", 1))
            vals, v = [], pmin
            while v <= pmax + 1e-9:
                is_int = (pstep == int(pstep) and pmin == int(pmin))
                vals.append(int(round(v)) if is_int else round(v, 4))
                v += pstep
            if vals:
                opt_kwargs[pname] = vals

        total  = len(tickers) * len(timeframes)
        done   = 0
        run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

        for ticker in tickers:
            for tf in timeframes:
                done += 1
                pct = int((done - 1) / total * 100)
                yield _sse({"type": "progress", "msg": f"Fetching {ticker} / {tf}  ({done}/{total})", "pct": pct})

                _MAX_TRIES = {"5m": 20, "15m": 25, "30m": 30, "1h": 40, "1d": 50}
                tf_max_tries = _MAX_TRIES.get(tf, 50)
                eff_start = start_date
                eff_end   = end_date

                # â"€â"€ Fetch data â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
                try:
                    if data_source == "alpaca":
                        from strategies.data import fetch_bars_alpaca
                        raw = fetch_bars_alpaca(ticker, eff_start, eff_end, tf)
                    else:
                        from strategies.data import fetch_bars
                        raw = fetch_bars(ticker, eff_start, eff_end, tf)

                    if len(raw) < 30:
                        yield _sse({"type": "warning", "msg": f"{ticker}/{tf}: only {len(raw)} bars - skipped"})
                        continue

                    df = _pd.DataFrame(raw).set_index("time")
                    df.index = _pd.to_datetime(df.index)
                    df.columns = [c.title() for c in df.columns]
                    df = df[["Open", "High", "Low", "Close"]].dropna()
                    df["Volume"] = 0
                    if tf in _INTRADAY_TF:
                        df = _filter_rth(df)

                except Exception as e:
                    yield _sse({"type": "warning", "msg": f"{ticker}/{tf} data error: {e}"})
                    continue

                # â"€â"€ Run optimize â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
                try:
                    import itertools as _it

                    def _f(v, n=2):
                        try:
                            fv = float(v or 0)
                            # NaN / Inf are not valid JSON — return None (serialises as null)
                            if fv != fv or fv == float('inf') or fv == float('-inf'):
                                return None
                            return round(fv, n)
                        except Exception:
                            return 0

                    def _make_stats(s):
                        return {
                            "Return [%]":        _f(s.get("Return [%]")),
                            "Sharpe Ratio":      _f(s.get("Sharpe Ratio"), 3),
                            "Max. Drawdown [%]": _f(s.get("Max. Drawdown [%]")),
                            "Win Rate [%]":      _f(s.get("Win Rate [%]")),
                            "# Trades":          int(s.get("# Trades") or 0),
                            "Profit Factor":     _f(s.get("Profit Factor"), 3),
                            "Calmar Ratio":      _f(s.get("Calmar Ratio"), 3),
                            "Equity Final [$]":  _f(s.get("Equity Final [$]"), 2),
                        }

                    bt = Backtest(df, strategy_cls, cash=cash, commission=commission, exclusive_orders=True,
                                  trade_on_close=getattr(strategy_cls, '_trade_on_close', False))

                    combo_results = []   # list of (params_dict, stats)

                    if opt_kwargs:
                        grid_size = 1
                        for vals in opt_kwargs.values():
                            grid_size *= len(vals)

                        yield _sse({"type": "progress",
                                    "msg": f"Optimizing {ticker}/{tf} - {grid_size} combosâ€¦",
                                    "pct": pct + 1})

                        if grid_size <= 500:
                            # Full grid - every combination, full stats
                            for combo in _it.product(*opt_kwargs.values()):
                                p_ov = dict(zip(opt_kwargs.keys(), combo))
                                combo_results.append((p_ov, bt.run(**p_ov)))
                        else:
                            # Sambo smart search â†’ full stats for each trial
                            try:
                                _, heatmap = bt.optimize(
                                    **opt_kwargs, maximize=maximize,
                                    return_heatmap=True, max_tries=tf_max_tries, method="sambo")
                            except Exception:
                                _, heatmap = bt.optimize(
                                    **opt_kwargs, maximize=maximize,
                                    return_heatmap=True, max_tries=tf_max_tries)
                            for idx in heatmap.index:
                                idx_t = idx if isinstance(idx, tuple) else (idx,)
                                p_ov  = dict(zip(opt_kwargs.keys(), idx_t))
                                combo_results.append((p_ov, bt.run(**p_ov)))
                    else:
                        yield _sse({"type": "progress",
                                    "msg": f"Running {ticker}/{tf} at defaultsâ€¦", "pct": pct + 1})
                        combo_results.append(({}, bt.run()))

                    # Sort descending by maximize metric
                    combo_results.sort(
                        key=lambda x: float(x[1].get(maximize) or 0), reverse=True)

                    p_ph = placeholder()
                    for rank, (p_ov, s) in enumerate(combo_results):
                        sd    = _make_stats(s)
                        score = _f(s.get(maximize), 4)
                        # Attach equity curve for top-3 results (downsampled to ≤200 pts)
                        eq_curve = None
                        if rank < 3:
                            try:
                                ec = s._equity_curve["Equity"]
                                step_ec = max(1, len(ec) // 200)
                                ec_s = ec.iloc[::step_ec]
                                eq_curve = {
                                    "dates":  [str(d)[:10] for d in ec_s.index],
                                    "values": [round(float(v), 2) for v in ec_s.values],
                                }
                            except Exception:
                                pass

                        row   = {
                            "type": "result",
                            "run_id": run_id, "strategy": strategy_name,
                            "ticker": ticker,  "timeframe": tf,
                            "params": p_ov,    "stats": sd,
                            "maximize": maximize, "score": score,
                            "rank": rank + 1,
                            "equity_curve": eq_curve,
                        }
                        if rank < 10:   # persist only top-10 per ticker/tf
                            try:
                                conn = get_db(); cur = conn.cursor()
                                cur.execute(
                                    f"INSERT INTO bt_results (run_id,strategy,ticker,timeframe,params,stats,maximize,score,created_at)"
                                    f" VALUES ({p_ph},{p_ph},{p_ph},{p_ph},{p_ph},{p_ph},{p_ph},{p_ph},{p_ph})",
                                    (run_id, strategy_name, ticker, tf,
                                     _json.dumps(p_ov), _json.dumps(sd),
                                     maximize, score,
                                     time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
                                conn.commit(); conn.close()
                            except Exception as db_e:
                                log.warning(f"bt_results DB error: {db_e}")
                        yield _sse(row)

                except Exception as e:
                    log.exception(f"Optimize error {ticker}/{tf}")
                    yield _sse({"type": "warning", "msg": f"{ticker}/{tf} optimize failed: {e}"})

        yield _sse({"type": "done", "run_id": run_id})

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/bt/results", methods=["GET"])
def bt_results_history():
    """Return last N optimization runs grouped by run_id."""
    try:
        p    = placeholder()
        conn = get_db(); cur = conn.cursor()
        cur.execute(
            f"SELECT run_id,strategy,ticker,timeframe,params,stats,maximize,score,created_at"
            f" FROM bt_results ORDER BY created_at DESC LIMIT {p}", (200,))
        rows = cur.fetchall(); conn.close()
        out = []
        for r in rows:
            out.append({
                "run_id": r[0], "strategy": r[1], "ticker": r[2], "timeframe": r[3],
                "params": json.loads(r[4] or "{}"), "stats": json.loads(r[5] or "{}"),
                "maximize": r[6], "score": r[7], "created_at": r[8],
            })
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/ib/clear", methods=["POST"])
def ib_clear_fills():
    """Delete all IB fill data so a fresh sync can rebuild it cleanly."""
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("DELETE FROM ib_executions")
    conn.commit()
    conn.close()
    return jsonify({"cleared": True})


@app.route("/api/ib/sync", methods=["POST"])
def ib_sync_fills():
    """Manually trigger a fill sync — signals the background IB thread to run reqExecutions."""
    if not ib_broker:
        return jsonify({"error": "IB broker not initialised"}), 400
    if not ib_broker.is_connected():
        st = ib_broker.status()
        detail = st.get("last_error") or f"Cannot reach IB Gateway at {st.get('host')}:{st.get('port')}"
        return jsonify({"error": f"IB not connected — {detail}"}), 400
    # Signal the background thread (which owns the IB event loop) to run the sync
    _ib_sync_event.set()
    try:
        result = _ib_sync_queue.get(timeout=30)
    except Exception:
        return jsonify({"error": "Sync timed out — IB may be unresponsive"}), 500
    if "error" in result:
        return jsonify(result), 500
    return jsonify(result)


@app.route("/api/ib/executions/<exec_id>", methods=["DELETE"])
def ib_execution_delete(exec_id):
    """Soft-delete a single IB execution so sync never re-inserts it."""
    conn = get_db()
    cur  = conn.cursor()
    p    = placeholder()
    try:
        cur.execute(f"ALTER TABLE ib_executions ADD COLUMN deleted INTEGER DEFAULT 0")
        conn.commit()
    except Exception:
        pass  # column already exists
    cur.execute(f"UPDATE ib_executions SET deleted=1 WHERE exec_id={p}", (exec_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/alpaca/portfolio_history")
def alpaca_portfolio_history():
    """Return Alpaca portfolio equity history for equity curve and daily P&L bars."""
    if alpaca_broker is None:
        return jsonify([])
    period    = request.args.get("period",    "3M")
    timeframe = request.args.get("timeframe", "1D")
    try:
        return jsonify(alpaca_broker.get_portfolio_history(period=period, timeframe=timeframe))
    except Exception as e:
        log.error("alpaca_portfolio_history error: %s", e)
        return jsonify([])


@app.route("/api/alpaca/trades")
def alpaca_trades():
    """Return filled Alpaca orders with resolved strategy names, cached for 30s."""
    global _alpaca_fills_cache
    if alpaca_broker is None:
        return jsonify([])
    now = time.time()
    if now - _alpaca_fills_cache["ts"] < ALPACA_CACHE_TTL:
        return jsonify(_alpaca_fills_cache["data"])
    try:
        fills = alpaca_broker.get_fills()
        # Resolve strategy for each fill by matching time+ticker against signals DB
        try:
            from datetime import datetime as _dt
            conn = get_db()
            cur  = conn.cursor()
            cur.execute("SELECT ticker, action, received_at, strategy FROM trades ORDER BY id ASC")
            rows = cur.fetchall()
            conn.close()
            sig_rows = [dict(r) if not DATABASE_URL else dict(zip([d[0] for d in cur.description], r)) for r in rows]
            # Build lookup: (ticker, side) → sorted list of (unix_ts, strategy)
            sig_lookup = {}
            for t in sig_rows:
                ticker   = (t.get("ticker") or "").strip().upper()
                action   = (t.get("action") or "").strip().upper()
                received = t.get("received_at") or ""
                strategy = (t.get("strategy") or "").strip()
                if not ticker or not received or not strategy:
                    continue
                side = "BOT" if action == "BUY" else "SLD" if action == "SELL" else None
                if not side:
                    continue
                try:
                    ts = _dt.fromisoformat(received.replace("Z", "+00:00")).timestamp()
                except Exception:
                    continue
                sig_lookup.setdefault((ticker, side), []).append((ts, strategy))
            # Annotate each fill with the closest matching strategy
            for f in fills:
                sym  = (f.get("symbol") or "").upper().replace("/", "").replace("USD", "")
                side = f.get("side", "")
                fill_time = f.get("time") or ""
                try:
                    fill_ts = _dt.fromisoformat(fill_time.replace("Z", "+00:00")).timestamp()
                except Exception:
                    continue
                # Try exact symbol first, then strip USD suffix for crypto
                candidates = sig_lookup.get((sym, side), []) or sig_lookup.get((f.get("symbol","").upper(), side), [])
                if candidates:
                    best = min(candidates, key=lambda x: abs(x[0] - fill_ts))
                    # Only assign if signal is within 5 minutes of the fill
                    if abs(best[0] - fill_ts) <= 300:
                        f["strategy"] = best[1]
        except Exception as _e:
            log.debug("alpaca_trades strategy resolution error: %s", _e)
        _alpaca_fills_cache = {"data": fills, "ts": now}
        return jsonify(fills)
    except Exception as e:
        log.error("alpaca_trades error: %s", e)
        return jsonify([])


@app.route("/api/ib/trades")
def ib_trades():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM ib_executions WHERE deleted IS NULL OR deleted=0 ORDER BY ts DESC")
    rows = cur.fetchall()
    if DATABASE_URL:
        cols   = [d[0] for d in cur.description]
        result = [dict(zip(cols, r)) for r in rows]
    else:
        result = [dict(r) for r in rows]
    conn.close()
    return jsonify(result)


@app.route("/api/ib/equity")
def ib_equity():
    """Cumulative realized P&L from IB fill data (SLD executions with pnl)."""
    conn = get_db()
    cur  = conn.cursor()
    cur.execute(
        "SELECT ts, pnl FROM ib_executions "
        "WHERE side = 'SLD' AND pnl IS NOT NULL AND (deleted IS NULL OR deleted=0) "
        "ORDER BY ts ASC"
    )
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return jsonify([])
    if DATABASE_URL:
        cols  = [d[0] for d in cur.description]
        fills = [dict(zip(cols, r)) for r in rows]
    else:
        fills = [dict(r) for r in rows]
    cumulative = 0
    result = []
    for f in fills:
        cumulative += f["pnl"]
        result.append({"time": f["ts"], "value": round(cumulative, 2)})
    return jsonify(result)


@app.route("/api/stats")
def api_stats():
    strategy_filter = request.args.get("strategy")
    from_date       = request.args.get("from_date")  # "YYYY-MM-DD" — only emit closes on/after this date
    to_date         = request.args.get("to_date")    # "YYYY-MM-DD" — only emit closes on/before this date
    conn = get_db()
    cur  = conn.cursor()
    p    = placeholder()
    if strategy_filter:
        cur.execute(
            f"SELECT * FROM trades WHERE strategy = {p} ORDER BY id ASC",
            (strategy_filter,),
        )
    else:
        cur.execute("SELECT * FROM trades ORDER BY id ASC")
    rows = cur.fetchall()
    if DATABASE_URL:
        cols   = [d[0] for d in cur.description]
        trades = [dict(zip(cols, r)) for r in rows]
    else:
        trades = [dict(r) for r in rows]
    conn.close()

    open_longs  = {}  # (strategy, ticker) → [(price, qty, time, trade_id)]
    open_shorts = {}  # (strategy, ticker) → [(price, qty, time, trade_id)]
    closed      = []

    for t in trades:
        action   = (t.get("action") or "").strip().upper()
        ticker   = (t.get("ticker") or "").strip().upper()
        strategy = (t.get("strategy") or "").strip()
        received = t.get("received_at") or ""
        trade_id = t.get("id")
        try:
            price = float(t.get("price") or 0)
            qty   = float(t.get("quantity") or 1)
        except (ValueError, TypeError):
            continue
        if not ticker or price == 0:
            continue
        exec_status = (t.get("exec_status") or "").lower()
        if exec_status in ("blocked", "skipped", "error"):
            continue
        key = (strategy, ticker)
        in_window = ((not from_date) or (received[:10] >= from_date)) and \
                    ((not to_date)   or (received[:10] <= to_date))
        if action == "BUY":
            # Closes an open short; otherwise opens a new long
            queue = open_shorts.get(key, [])
            if queue:
                entry_price, entry_qty, _, entry_id = queue.pop(0)
                pnl = (entry_price - price) * min(qty, entry_qty)
                if in_window:
                    closed.append({"pnl": pnl, "time": received, "entry_id": entry_id, "exit_id": trade_id})
            else:
                open_longs.setdefault(key, []).append((price, qty, received, trade_id))
        elif action == "SELL":
            # Closes an open long; otherwise opens a new short
            queue = open_longs.get(key, [])
            if queue:
                entry_price, entry_qty, _, entry_id = queue.pop(0)
                pnl = (price - entry_price) * min(qty, entry_qty)
                if in_window:
                    closed.append({"pnl": pnl, "time": received, "entry_id": entry_id, "exit_id": trade_id})
            else:
                open_shorts.setdefault(key, []).append((price, qty, received, trade_id))

    if not closed:
        return jsonify({
            "completed_trades": 0, "win_rate": 0, "avg_win": 0,
            "avg_loss": 0, "profit_factor": None, "max_drawdown": 0,
            "equity_curve": [],
        })

    wins   = [c["pnl"] for c in closed if c["pnl"] > 0]
    losses = [c["pnl"] for c in closed if c["pnl"] <= 0]

    win_rate      = round(len(wins) / len(closed) * 100, 1)
    avg_win       = round(sum(wins)   / len(wins),   2) if wins   else 0
    avg_loss      = round(sum(losses) / len(losses), 2) if losses else 0
    gross_loss    = abs(sum(losses))
    profit_factor = round(sum(wins) / gross_loss, 2) if gross_loss > 0 else None

    equity_curve = []
    cumulative = peak = max_dd = 0
    for c in closed:
        cumulative += c["pnl"]
        equity_curve.append({
            "time":     c["time"],
            "value":    round(cumulative, 2),
            "entry_id": c.get("entry_id"),
            "exit_id":  c.get("exit_id"),
        })
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    return jsonify({
        "completed_trades": len(closed),
        "win_rate":         win_rate,
        "avg_win":          avg_win,
        "avg_loss":         avg_loss,
        "profit_factor":    profit_factor,
        "max_drawdown":     round(max_dd, 2),
        "equity_curve":     equity_curve,
    })


# ---------------------------------------------------------------------------
# Orphaned trades diagnostic
# ---------------------------------------------------------------------------

@app.route("/api/orphaned_trades")
def api_orphaned_trades():
    """Return trades that have no matching pair (stuck open longs/shorts).
    These are the 'rogue' entries that corrupt the equity curve when a real
    close later pairs against them at the wrong historical price."""
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM trades ORDER BY id ASC")
    rows = cur.fetchall()
    if DATABASE_URL:
        cols   = [d[0] for d in cur.description]
        trades = [dict(zip(cols, r)) for r in rows]
    else:
        trades = [dict(r) for r in rows]
    conn.close()

    # (strategy, ticker) → [(trade_id, price, qty, time)]
    open_longs  = {}
    open_shorts = {}

    for t in trades:
        action   = (t.get("action") or "").strip().upper()
        ticker   = (t.get("ticker") or "").strip().upper()
        strategy = (t.get("strategy") or "").strip()
        received = t.get("received_at") or ""
        trade_id = t.get("id")
        exec_status = (t.get("exec_status") or "").lower()
        if exec_status in ("blocked", "skipped", "error", "cancelled"):
            continue
        try:
            price = float(t.get("price") or 0)
            qty   = float(t.get("quantity") or 1)
        except (ValueError, TypeError):
            continue
        if not ticker or price == 0:
            continue
        key = (strategy, ticker)
        if action == "BUY":
            queue = open_shorts.get(key, [])
            if queue:
                queue.pop(0)
            else:
                open_longs.setdefault(key, []).append((trade_id, price, qty, received))
        elif action == "SELL":
            queue = open_longs.get(key, [])
            if queue:
                queue.pop(0)
            else:
                open_shorts.setdefault(key, []).append((trade_id, price, qty, received))

    result = []
    for (strategy, ticker), entries in open_longs.items():
        for (trade_id, price, qty, received) in entries:
            result.append({
                "id": trade_id, "side": "BUY", "ticker": ticker,
                "strategy": strategy, "price": price, "qty": qty, "date": received,
            })
    for (strategy, ticker), entries in open_shorts.items():
        for (trade_id, price, qty, received) in entries:
            result.append({
                "id": trade_id, "side": "SELL", "ticker": ticker,
                "strategy": strategy, "price": price, "qty": qty, "date": received,
            })

    result.sort(key=lambda x: x["date"])
    return jsonify(result)



# ---------------------------------------------------------------------------
# Analysis page
# ---------------------------------------------------------------------------

@app.route("/analysis")
def analysis_page():
    return render_template("analysis.html")


def _build_analysis_stats():
    """
    Pairs BUY/SELL signals from the trades table into closed round-trips.
    Returns per-strategy stats, per-ticker stats, daily buckets, weekly buckets.
    """
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM trades ORDER BY id ASC")
    rows = cur.fetchall()
    if DATABASE_URL:
        cols   = [d[0] for d in cur.description]
        trades = [dict(zip(cols, r)) for r in rows]
    else:
        trades = [dict(r) for r in rows]
    conn.close()

    # Pair trades into closed round-trips per (strategy, ticker)
    open_longs  = {}  # (strategy, ticker) → [(price, qty, time)]
    open_shorts = {}
    closed      = []  # {"pnl", "strategy", "ticker", "date", "entry_time", "exit_time"}

    for t in trades:
        action   = (t.get("action") or "").strip().upper()
        ticker   = (t.get("ticker") or "").strip().upper()
        strategy = (t.get("strategy") or "Unknown").strip()
        received = t.get("received_at") or ""
        try:
            price = float(t.get("price") or 0)
            qty   = float(t.get("quantity") or 1)
        except (ValueError, TypeError):
            continue
        if not ticker or price == 0:
            continue
        exec_status = (t.get("exec_status") or "").lower()
        if exec_status in ("blocked", "skipped", "error"):
            continue

        key = (strategy, ticker)
        date_str = received[:10] if received else ""

        if action == "BUY":
            queue = open_shorts.get(key, [])
            if queue:
                entry_price, entry_qty, entry_time = queue.pop(0)
                pnl = (entry_price - price) * min(qty, entry_qty)
                closed.append({"pnl": round(pnl, 2), "strategy": strategy, "ticker": ticker,
                                "date": date_str, "entry_time": entry_time, "exit_time": received})
            else:
                open_longs.setdefault(key, []).append((price, qty, received))
        elif action == "SELL":
            queue = open_longs.get(key, [])
            if queue:
                entry_price, entry_qty, entry_time = queue.pop(0)
                pnl = (price - entry_price) * min(qty, entry_qty)
                closed.append({"pnl": round(pnl, 2), "strategy": strategy, "ticker": ticker,
                                "date": date_str, "entry_time": entry_time, "exit_time": received})
            else:
                open_shorts.setdefault(key, []).append((price, qty, received))

    def _stats_from_trades(trade_list):
        if not trade_list:
            return None
        wins   = [t["pnl"] for t in trade_list if t["pnl"] > 0]
        losses = [t["pnl"] for t in trade_list if t["pnl"] <= 0]
        gross_win  = sum(wins)
        gross_loss = abs(sum(losses))
        total_pnl  = round(gross_win - gross_loss, 2)
        pf = round(gross_win / gross_loss, 2) if gross_loss > 0 else None
        return {
            "trades":        len(trade_list),
            "wins":          len(wins),
            "losses":        len(losses),
            "win_rate":      round(len(wins) / len(trade_list) * 100, 1),
            "profit_factor": pf,
            "total_pnl":     total_pnl,
            "avg_win":       round(gross_win  / len(wins),   2) if wins   else 0,
            "avg_loss":      round(-gross_loss / len(losses), 2) if losses else 0,
            "largest_win":   round(max(wins),  2) if wins   else 0,
            "largest_loss":  round(min(losses), 2) if losses else 0,
        }

    # Per-strategy
    strat_map = {}
    for c in closed:
        strat_map.setdefault(c["strategy"], []).append(c)
    per_strategy = {}
    for s, tlist in strat_map.items():
        st = _stats_from_trades(tlist)
        if st:
            per_strategy[s] = st

    # Per-ticker
    ticker_map = {}
    for c in closed:
        ticker_map.setdefault(c["ticker"], []).append(c)
    per_ticker = {}
    for tk, tlist in ticker_map.items():
        st = _stats_from_trades(tlist)
        if st:
            per_ticker[tk] = st

    # Daily buckets
    daily_map = {}
    for c in closed:
        d = c["date"] or "unknown"
        daily_map.setdefault(d, []).append(c["pnl"])
    daily = []
    cumulative = 0
    for d in sorted(daily_map):
        day_pnl = round(sum(daily_map[d]), 2)
        cumulative = round(cumulative + day_pnl, 2)
        daily.append({"date": d, "pnl": day_pnl, "trades": len(daily_map[d]), "cumulative": cumulative})

    # Weekly buckets (ISO week)
    from datetime import datetime as _dt
    weekly_map = {}
    for c in closed:
        try:
            dt = _dt.fromisoformat(c["date"])
            week_key = dt.strftime("%Y-W%W")
            week_label = dt.strftime("Week of %b %d, %Y")
        except Exception:
            week_key = week_label = "unknown"
        weekly_map.setdefault(week_key, {"label": week_label, "pnl": 0, "trades": 0})
        weekly_map[week_key]["pnl"]    = round(weekly_map[week_key]["pnl"] + c["pnl"], 2)
        weekly_map[week_key]["trades"] += 1
    weekly = []
    cumulative = 0
    for wk in sorted(weekly_map):
        w = weekly_map[wk]
        cumulative = round(cumulative + w["pnl"], 2)
        weekly.append({"week": w["label"], "pnl": w["pnl"], "trades": w["trades"], "cumulative": cumulative})

    overall = _stats_from_trades(closed) or {}

    return {
        "overall":      overall,
        "per_strategy": per_strategy,
        "per_ticker":   per_ticker,
        "daily":        daily,
        "weekly":       weekly,
    }


@app.route("/api/analysis")
def api_analysis():
    try:
        return jsonify(_build_analysis_stats())
    except Exception as e:
        log.exception("Analysis error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/alpaca/analysis")
def api_alpaca_analysis():
    """Same analysis as /api/analysis but using Alpaca fills.
    Strategies are resolved by matching each fill to the closest signal
    in the trades DB by ticker + direction + time."""
    try:
        from datetime import datetime as _dt

        if alpaca_broker is None:
            return jsonify({"error": "Alpaca not configured"}), 400

        from_date = request.args.get("from_date", "")
        to_date   = request.args.get("to_date",   "")

        fills = alpaca_broker.get_fills()
        if not fills:
            return jsonify({"overall": {}, "per_strategy": {}, "per_ticker": {}, "daily": [], "weekly": [], "equity_curve": []})

        # Filter by date range if requested
        if from_date or to_date:
            def _fill_date(f):
                t = f.get("time") or ""
                return t[:10] if t else ""
            fills = [f for f in fills if
                     (not from_date or _fill_date(f) >= from_date) and
                     (not to_date   or _fill_date(f) <= to_date)]

        signals_only = request.args.get("signals_only", "0") == "1"

        # Build signal lookup: (ticker, side) → sorted list of (unix_ts, strategy)
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT ticker, action, received_at, strategy, exec_status FROM trades ORDER BY id ASC")
        rows = cur.fetchall()
        trades_db = [dict(r) if not DATABASE_URL else dict(zip([d[0] for d in cur.description], r)) for r in rows]
        conn.close()

        signal_lookup = {}  # (ticker, 'BOT'|'SLD') → [(ts, strategy)]
        for t in trades_db:
            ticker   = (t.get("ticker") or "").strip().upper()
            action   = (t.get("action") or "").strip().upper()
            received = t.get("received_at") or ""
            strategy = (t.get("strategy") or "").strip()
            if not strategy:
                continue
            side = "BOT" if action == "BUY" else "SLD" if action == "SELL" else None
            if not side or not ticker or not received:
                continue
            try:
                ts = _dt.fromisoformat(received.replace("Z", "+00:00")).timestamp()
            except Exception:
                continue
            signal_lookup.setdefault((ticker, side), []).append((ts, strategy))

        def _resolve_strategy(symbol, side, fill_time_str):
            try:
                fill_ts = _dt.fromisoformat(fill_time_str.replace("Z", "+00:00")).timestamp()
            except Exception:
                return "Unknown"
            candidates = signal_lookup.get((symbol.upper(), side), [])
            if not candidates:
                return "Unknown"
            best = min(candidates, key=lambda x: abs(x[0] - fill_ts))
            if abs(best[0] - fill_ts) > 300:
                return "Unknown"
            return best[1]

        # Deduplicate fills
        seen = set()
        deduped = []
        for f in fills:
            key = f"{f['symbol']}|{f['side']}|{f['time']}|{f['shares']}"
            if key not in seen:
                seen.add(key)
                deduped.append(f)

        # Sort by time
        def _parse_ts(f):
            try:
                return _dt.fromisoformat((f["time"] or "").replace("Z", "+00:00")).timestamp()
            except Exception:
                return 0
        deduped.sort(key=_parse_ts)

        # LIFO pairing — matches each sell to the most recent preceding buy for that
        # symbol.  This correctly pairs intraday round-trips from algo signals even
        # when older open positions exist in the queue (FIFO would assign the sell to
        # the oldest buy, inflating or deflating P&L vs what the signal actually did).
        # Partial-fill loop: if a sell exceeds the top long we close it fully and
        # keep consuming longs until the sell is exhausted; any residual opens a
        # short. Without this, oversized fills silently drop shares and stale
        # longs get mispaired with unrelated later sells.
        open_longs  = {}
        open_shorts = {}
        closed = []
        for f in deduped:
            sym      = (f.get("symbol") or "").upper()
            side     = f.get("side", "")
            price    = float(f.get("price") or 0)
            qty      = float(f.get("shares") or 0)
            fill_ts  = f.get("time", "")
            date_str = fill_ts[:10] if fill_ts else ""
            strat    = _resolve_strategy(sym, side, fill_ts)
            if side == "BOT":
                q = open_shorts.setdefault(sym, [])
                while qty > 0 and q:
                    ep, eq, et, es = q.pop(-1)  # LIFO: most recent short
                    m = min(qty, eq)
                    closed.append({"pnl": round((ep - price) * m, 2), "strategy": es,
                                   "ticker": sym, "date": date_str, "entry_time": et, "exit_time": fill_ts})
                    qty -= m
                    if eq > m:
                        q.append((ep, eq - m, et, es))   # remainder stays on top (LIFO)
                if qty > 0:
                    open_longs.setdefault(sym, []).append((price, qty, fill_ts, strat))
            elif side == "SLD":
                q = open_longs.setdefault(sym, [])
                while qty > 0 and q:
                    ep, eq, et, es = q.pop(-1)  # LIFO: most recent long
                    m = min(qty, eq)
                    closed.append({"pnl": round((price - ep) * m, 2), "strategy": es,
                                   "ticker": sym, "date": date_str, "entry_time": et, "exit_time": fill_ts})
                    qty -= m
                    if eq > m:
                        q.append((ep, eq - m, et, es))
                if qty > 0:
                    open_shorts.setdefault(sym, []).append((price, qty, fill_ts, strat))

        # Per-day FIFO pairing — used only for the daily/weekly breakdown so each
        # bar shows intraday round-trips only (consistent with the dashboard chart).
        from collections import defaultdict
        fills_by_date = defaultdict(list)
        for f in deduped:
            fill_ts  = f.get("time", "")
            date_str = fill_ts[:10] if fill_ts else "unknown"
            fills_by_date[date_str].append(f)

        daily_closed = []
        for date_str, day_fills in sorted(fills_by_date.items()):
            day_longs  = {}
            day_shorts = {}
            for f in day_fills:
                sym     = (f.get("symbol") or "").upper()
                side    = f.get("side", "")
                price   = float(f.get("price") or 0)
                qty     = float(f.get("shares") or 0)
                fill_ts = f.get("time", "")
                strat   = _resolve_strategy(sym, side, fill_ts)
                if side == "BOT":
                    q = day_shorts.setdefault(sym, [])
                    while qty > 0 and q:
                        ep, eq, et, es = q.pop(0)  # FIFO: oldest short
                        m = min(qty, eq)
                        daily_closed.append({"pnl": round((ep - price) * m, 2), "strategy": es,
                                             "entry_strategy": es, "exit_strategy": strat,
                                             "ticker": sym, "date": date_str, "entry_time": et, "exit_time": fill_ts})
                        qty -= m
                        if eq > m:
                            q.insert(0, (ep, eq - m, et, es))   # remainder stays at front (FIFO)
                    if qty > 0:
                        day_longs.setdefault(sym, []).append((price, qty, fill_ts, strat))
                elif side == "SLD":
                    q = day_longs.setdefault(sym, [])
                    while qty > 0 and q:
                        ep, eq, et, es = q.pop(0)
                        m = min(qty, eq)
                        daily_closed.append({"pnl": round((price - ep) * m, 2), "strategy": es,
                                             "entry_strategy": es, "exit_strategy": strat,
                                             "ticker": sym, "date": date_str, "entry_time": et, "exit_time": fill_ts})
                        qty -= m
                        if eq > m:
                            q.insert(0, (ep, eq - m, et, es))
                    if qty > 0:
                        day_shorts.setdefault(sym, []).append((price, qty, fill_ts, strat))

        def _stats(tlist):
            if not tlist: return None
            wins   = [t["pnl"] for t in tlist if t["pnl"] > 0]
            losses = [t["pnl"] for t in tlist if t["pnl"] <= 0]
            gw, gl = sum(wins), abs(sum(losses))
            return {
                "trades":        len(tlist),
                "wins":          len(wins),
                "losses":        len(losses),
                "win_rate":      round(len(wins) / len(tlist) * 100, 1),
                "profit_factor": round(gw / gl, 2) if gl > 0 else None,
                "total_pnl":     round(gw - gl, 2),
                "avg_win":       round(gw / len(wins),   2) if wins   else 0,
                "avg_loss":      round(-gl / len(losses), 2) if losses else 0,
                "largest_win":   round(max(wins),  2) if wins   else 0,
                "largest_loss":  round(min(losses), 2) if losses else 0,
            }

        # Apply frontend exclusions (localStorage keys: "exit_time|ticker")
        exclude_param = request.args.get("exclude", "").strip()
        if exclude_param:
            excluded_keys = set(exclude_param.split(","))
            closed = [
                c for c in closed
                if f"{c['exit_time']}|{c['ticker']}" not in excluded_keys
            ]
            daily_closed = [
                c for c in daily_closed
                if f"{c['exit_time']}|{c['ticker']}" not in excluded_keys
            ]

        # per_strategy and per_ticker use the global LIFO pairs so overnight
        # positions (bought one day, sold the next) are captured correctly.
        strat_map = {}
        for c in closed:
            strat_map.setdefault(c["strategy"], []).append(c)
        per_strategy = {s: _stats(tl) for s, tl in strat_map.items() if _stats(tl)}

        ticker_map = {}
        for c in closed:
            ticker_map.setdefault(c["ticker"], []).append(c)
        per_ticker = {tk: _stats(tl) for tk, tl in ticker_map.items() if _stats(tl)}

        daily_map = {}
        for c in daily_closed:
            daily_map.setdefault(c["date"] or "unknown", []).append(c["pnl"])
        daily, cum = [], 0
        for d in sorted(daily_map):
            day_pnl = round(sum(daily_map[d]), 2)
            cum = round(cum + day_pnl, 2)
            daily.append({"date": d, "pnl": day_pnl, "trades": len(daily_map[d]), "cumulative": cum})

        weekly_map = {}
        for c in daily_closed:
            try:
                dt = _dt.fromisoformat(c["date"])
                wk = dt.strftime("%Y-W%W")
                wl = dt.strftime("Week of %b %d, %Y")
            except Exception:
                wk = wl = "unknown"
            weekly_map.setdefault(wk, {"label": wl, "pnl": 0, "trades": 0})
            weekly_map[wk]["pnl"]    = round(weekly_map[wk]["pnl"] + c["pnl"], 2)
            weekly_map[wk]["trades"] += 1
        weekly, cum = [], 0
        for wk in sorted(weekly_map):
            w = weekly_map[wk]
            cum = round(cum + w["pnl"], 2)
            weekly.append({"week": w["label"], "pnl": w["pnl"], "trades": w["trades"], "cumulative": cum})

        # Build per-trade equity curve from day-scoped pairs (daily_closed).
        # Using day-scoped pairing avoids cross-day mismatches: each sell is only
        # ever paired with a buy from the same calendar day, which correctly
        # captures intraday round-trips regardless of open multi-day positions.
        def _is_matched(s):
            return bool(s) and s != "Unknown"

        # ── TV-signal-aligned equity curve (signals_only mode) ──────────────
        # When signals_only is requested, bypass FIFO entirely. Instead, pair
        # each Alpaca fill directly to its closest TV signal (same ticker +
        # direction within ±5 min), then match entry↔exit signal pairs. This
        # gives a 1-to-1 correspondence with the TV trade log using actual
        # Alpaca fill prices — no cross-signal phantom pairs possible.
        if signals_only:
            MATCH_WINDOW = 300  # seconds

            # Build Alpaca fill lookup: (ticker, side) → [fill] sorted by time
            fill_lookup = {}
            for f in deduped:
                sym  = (f.get("symbol") or "").upper()
                side = f.get("side", "")
                try:
                    ts = _dt.fromisoformat((f["time"] or "").replace("Z", "+00:00")).timestamp()
                except Exception:
                    continue
                fill_lookup.setdefault((sym, side), []).append((ts, f))

            # Build TV signal pairs: BUY → SELL per (strategy, ticker)
            # Reuse trades_db but need exec_status already filtered above
            tv_open = {}   # (strategy, ticker) → [(ts, received_at)]
            tv_pairs_aligned = []
            for t in trades_db:
                ticker   = (t.get("ticker") or "").strip().upper()
                action   = (t.get("action") or "").strip().upper()
                received = t.get("received_at") or ""
                strategy = (t.get("strategy") or "Unknown").strip()
                exec_st  = (t.get("exec_status") or "").lower()
                if exec_st in ("blocked", "skipped", "error"):
                    continue
                try:
                    ts = _dt.fromisoformat(received.replace("Z", "+00:00")).timestamp()
                except Exception:
                    continue
                key = (strategy, ticker)
                if action == "BUY":
                    tv_open.setdefault(key, []).append((ts, received))
                elif action == "SELL":
                    queue = tv_open.get(key, [])
                    if queue:
                        entry_ts, entry_time = queue.pop(0)
                        tv_pairs_aligned.append({
                            "ticker":     ticker,
                            "strategy":   strategy,
                            "entry_ts":   entry_ts,
                            "entry_time": entry_time,
                            "exit_ts":    ts,
                            "exit_time":  received,
                        })

            # Match each TV pair to Alpaca fills; mark fills as used
            used_fills = set()
            sv_closed = []

            def _find_fill(ticker, side, target_ts):
                candidates = fill_lookup.get((ticker, side), [])
                best = None
                best_diff = MATCH_WINDOW
                best_idx  = None
                for idx, (fts, f) in enumerate(candidates):
                    uid = f.get("exec_id") or f.get("time") or str(idx)
                    if uid in used_fills:
                        continue
                    diff = abs(fts - target_ts)
                    if diff < best_diff:
                        best_diff = diff
                        best = f
                        best_idx = uid
                return best, best_idx

            for p in tv_pairs_aligned:
                ticker = p["ticker"]
                entry_fill, entry_uid = _find_fill(ticker, "BOT", p["entry_ts"])
                exit_fill,  exit_uid  = _find_fill(ticker, "SLD", p["exit_ts"])
                if not entry_fill or not exit_fill:
                    continue
                used_fills.add(entry_uid)
                used_fills.add(exit_uid)
                ep  = float(entry_fill.get("price") or 0)
                xp  = float(exit_fill.get("price")  or 0)
                qty = float(entry_fill.get("shares") or 0)
                pnl = round((xp - ep) * qty, 2)
                date_str = p["exit_time"][:10]
                sv_closed.append({
                    "pnl":            pnl,
                    "strategy":       p["strategy"],
                    "entry_strategy": p["strategy"],
                    "exit_strategy":  p["strategy"],
                    "ticker":         ticker,
                    "date":           date_str,
                    "entry_time":     entry_fill.get("time", ""),
                    "exit_time":      exit_fill.get("time", ""),
                })

            # Apply exclusions to TV-aligned pairs
            if exclude_param:
                sv_closed = [
                    c for c in sv_closed
                    if f"{c['exit_time']}|{c['ticker']}" not in excluded_keys
                ]

            cum_pnl = 0
            equity_curve = []
            for c in sorted(sv_closed, key=lambda x: x["exit_time"]):
                cum_pnl = round(cum_pnl + c["pnl"], 2)
                equity_curve.append({
                    "time":         c["exit_time"],
                    "value":        cum_pnl,
                    "pnl":          c["pnl"],
                    "ticker":       c["ticker"],
                    "strategy":     c["strategy"],
                    "both_matched": True,
                })

        else:
            cum_pnl = 0
            equity_curve = []
            for c in sorted(daily_closed, key=lambda x: x["exit_time"]):
                cum_pnl = round(cum_pnl + c["pnl"], 2)
                equity_curve.append({
                    "time":          c["exit_time"],
                    "value":         cum_pnl,
                    "pnl":           c["pnl"],
                    "ticker":        c["ticker"],
                    "strategy":      c.get("strategy", "Unknown"),
                    "both_matched":  _is_matched(c.get("entry_strategy")) and _is_matched(c.get("exit_strategy")),
                })

        return jsonify({
            "overall":      _stats(daily_closed) or {},
            "per_strategy": per_strategy,
            "per_ticker":   per_ticker,
            "daily":        daily,
            "weekly":       weekly,
            "equity_curve": equity_curve,
        })
    except Exception as e:
        log.exception("Alpaca analysis error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/analysis/suggest", methods=["POST"])
def api_analysis_suggest():
    """Stream AI suggestions for improving profit factor per strategy."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 503
    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({"error": "anthropic package not installed"}), 503

    data        = request.get_json(silent=True) or {}
    per_strategy = data.get("per_strategy", {})
    overall      = data.get("overall", {})

    if not per_strategy:
        return jsonify({"error": "No strategy data provided"}), 400

    rows = "\n".join(
        f"  {name}: trades={s['trades']} win_rate={s['win_rate']}% "
        f"profit_factor={s['profit_factor']} total_pnl=${s['total_pnl']} "
        f"avg_win=${s['avg_win']} avg_loss=${s['avg_loss']} "
        f"largest_win=${s['largest_win']} largest_loss=${s['largest_loss']}"
        for name, s in per_strategy.items()
    )

    prompt = (
        f"I am a retail trader using TradingView strategy alerts routed to Interactive Brokers.\n\n"
        f"Overall account: trades={overall.get('trades')} win_rate={overall.get('win_rate')}% "
        f"profit_factor={overall.get('profit_factor')} total_pnl=${overall.get('total_pnl')}\n\n"
        f"Per-strategy breakdown:\n{rows}\n\n"
        f"For each strategy that has at least 5 trades, provide:\n"
        f"1. A brief assessment of its current performance\n"
        f"2. 2-3 specific, actionable suggestions to improve its profit factor\n\n"
        f"Focus on practical improvements: entry filters, exit management, position sizing, "
        f"time-of-day filters, or stopping trading low-performing strategies altogether. "
        f"Be concise and direct. Use ## Strategy Name as headings."
    )

    def generate():
        try:
            client = _anthropic.Anthropic(api_key=api_key)
            with client.messages.stream(
                model="claude-opus-4-7",
                max_tokens=1500,
                system=(
                    "You are a quantitative trading analyst reviewing live trading results. "
                    "Be concise and actionable. Use markdown: ## for strategy headings, "
                    "**bold** for key metrics, bullet points for suggestions."
                ),
                messages=[{"role": "user", "content": prompt}],
            ) as stream:
                for text in stream.text_stream:
                    yield f"data: {json.dumps({'text': text})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        yield "data: [DONE]\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

init_db()

# Load persisted risk limits from DB if env vars weren't explicitly set
def _restore_risk_settings():
    global MAX_DAILY_LOSS, MAX_POSITION_LOSS
    if MAX_DAILY_LOSS == 0:
        stored = _load_setting("MAX_DAILY_LOSS")
        if stored is not None:
            try:
                MAX_DAILY_LOSS = float(stored)
                log.info("Restored MAX_DAILY_LOSS=%g from DB", MAX_DAILY_LOSS)
            except (TypeError, ValueError):
                pass
    if MAX_POSITION_LOSS == 0:
        stored = _load_setting("MAX_POSITION_LOSS")
        if stored is not None:
            try:
                MAX_POSITION_LOSS = float(stored)
                log.info("Restored MAX_POSITION_LOSS=%g from DB", MAX_POSITION_LOSS)
            except (TypeError, ValueError):
                pass

_restore_risk_settings()

if __name__ == "__main__":
    app.run(debug=True)
