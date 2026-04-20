import json
import logging
import os
import sqlite3
import threading
from zoneinfo import ZoneInfo
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from flask import Flask, Response, abort, jsonify, render_template, request, stream_with_context

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

@app.route("/webhook", methods=["POST"])
def webhook():
    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != WEBHOOK_TOKEN:
        abort(401)

    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    broker_name = (data.get("broker") or "").strip().lower()

    # Normalise ticker — Pine Script sends "symbol", some strategies send "ticker"
    ticker = (data.get("ticker") or data.get("symbol") or "").strip().upper() or None

    # Normalise action — map direction/exit variants to BUY/SELL
    raw_action = (data.get("action") or "").strip().upper()
    action_map = {
        "EXIT_LONG":  "SELL",
        "EXIT_SHORT": "BUY",
        "LONG":       "BUY",
        "SHORT":      "SELL",
    }
    order_action = action_map.get(raw_action, raw_action)  # BUY/SELL pass through unchanged

    # Apply routing rules — look up a matching enabled pipeline and override settings
    strategy_name    = (data.get("strategy") or "").strip()
    quantity         = data.get("quantity", 1)
    opt_target_prem  = None   # set by options_config node
    opt_expiry_type  = "friday"
    opt_right_ovr    = None
    opt_contracts    = 1
    th_start         = None   # set by trading_hours node (HH:MM string)
    th_end           = None
    th_tz            = "America/New_York"
    sec_type        = data.get("sec_type", "STK")
    currency        = data.get("currency", "USD")
    use_live_broker = False  # True = route to ib_broker_live instead of ib_broker
    broker_targets  = []     # populated by broker nodes; supports multi-broker pipelines

    try:
        rconn = get_db()
        rcur  = rconn.cursor()
        rcur.execute("SELECT nodes FROM routing_rules WHERE enabled=1 ORDER BY COALESCE(sort_order, id) ASC")
        rule_rows = rcur.fetchall()
        rconn.close()
        for rrow in rule_rows:
            nodes_raw = rrow[0] if DATABASE_URL else rrow["nodes"]
            nodes = json.loads(nodes_raw) if isinstance(nodes_raw, str) else (nodes_raw or [])
            # Check if a strategy node in this pipeline matches
            strat_nodes = [n for n in nodes if n.get("type") == "strategy"]
            if strat_nodes:
                matched = False
                for sn in strat_nodes:
                    pattern  = (sn.get("value") or "").strip().upper()
                    incoming = strategy_name.upper()
                    if pattern.endswith("*") and incoming.startswith(pattern[:-1]):
                        matched = True
                    elif pattern.startswith("*") and incoming.endswith(pattern[1:]):
                        matched = True
                    elif pattern == incoming:
                        matched = True
                if not matched:
                    continue
            # Pipeline matched — apply node settings
            for n in nodes:
                ntype = n.get("type")
                if ntype == "broker":
                    # Support combined values (ib-paper, ib-live, alpaca-paper, alpaca-live)
                    # as well as legacy bare values (ib, alpaca, coinbase)
                    raw_bv = (n.get("value") or "ib-paper").lower()
                    broker_targets.append(raw_bv)
                    # Combined values also set use_live_broker for IB
                    if raw_bv == "ib-live":
                        use_live_broker = True
                elif ntype == "mode":
                    use_live_broker = (n.get("value") or "").lower() == "live"
                elif ntype == "quantity":
                    quantity = n.get("amount", quantity)
                elif ntype == "crypto_qty":
                    _dollars = float(n.get("dollars") or 10)
                    try:
                        import urllib.request as _ur
                        import json as _jx
                        _sym = (n.get("symbol") or "BTC").upper()
                        with _ur.urlopen(f"https://api.coinbase.com/v2/prices/{_sym}-USD/spot", timeout=5) as _r:
                            _price = float(_jx.loads(_r.read())["data"]["amount"])
                        quantity = round(_dollars / _price, 8)
                        log.info("crypto_qty: $%.2f / $%.2f = %.8f %s", _dollars, _price, quantity, _sym)
                    except Exception as _e:
                        log.error("crypto_qty price fetch failed: %s", _e)
                elif ntype == "instrument":
                    sec_type = n.get("value") or sec_type
                elif ntype == "ticker":
                    ticker = (n.get("value") or ticker or "").upper() or None
                elif ntype == "options_config":
                    opt_broker_mode = n.get("broker_mode") or "alpaca"
                    if opt_broker_mode == "ib":
                        opt_target_prem = float(n.get("ib_target_premium") or 2.0)
                        _ib_exp         = n.get("ib_expiry_type") or "weekly"
                        opt_right_ovr   = n.get("ib_right_override") or None
                        opt_contracts   = int(n.get("ib_contracts") or 1)
                        sec_type        = "OPT"   # trigger IB options path
                        if _ib_exp == "0dte":
                            from datetime import date as _date
                            opt_expiry_type = "weekly"   # fallback if no 0dte listed
                            # pass today as explicit override so IB uses 0DTE chain
                            import datetime as _dt_mod
                            _today_str = _dt_mod.date.today().strftime("%Y%m%d")
                            # store in data dict so _place_async picks it up
                            data["option_expiry"] = _today_str
                        else:
                            opt_expiry_type = _ib_exp   # "weekly" or "monthly"
                    else:
                        opt_target_prem = float(n.get("target_premium") or 2.0)
                        opt_expiry_type = n.get("expiry_type") or "friday"
                        opt_right_ovr   = n.get("right_override") or None
                        opt_contracts   = int(n.get("contracts") or 1)
                elif ntype == "trading_hours":
                    th_start = n.get("start") or "09:30"
                    th_end   = n.get("end")   or "16:00"
                    th_tz    = n.get("tz")    or "America/New_York"
            if broker_targets:
                broker_name = ",".join(broker_targets)
            log.info("Routing rule matched for strategy '%s' — broker=%s live=%s qty=%s sec=%s",
                     strategy_name, broker_name, use_live_broker, quantity, sec_type)
            break  # first matching pipeline wins
    except Exception as e:
        log.warning("Routing rule lookup failed: %s", e)

    # If no broker nodes fired from pipeline, fall back to the single broker_name from request body
    if not broker_targets and broker_name:
        broker_targets = [broker_name]

    # Enforce trading hours if a trading_hours node was found in the matched pipeline
    if th_start and th_end:
        try:
            from zoneinfo import ZoneInfo
            from datetime import datetime as _dt
            _now = _dt.now(ZoneInfo(th_tz))
            _now_t = _now.strftime("%H:%M")
            if not (th_start <= _now_t < th_end):
                log.info(
                    "Signal for '%s' dropped — outside trading hours (%s–%s %s, now %s)",
                    strategy_name, th_start, th_end, th_tz, _now_t,
                )
                return jsonify({"status": "skipped", "reason": f"outside trading hours ({th_start}–{th_end} {th_tz})"}), 200
        except Exception as e:
            log.warning("Trading hours check failed: %s", e)

    # Duplicate signal filter — drop retries / double-fires within cooldown window
    if SIGNAL_COOLDOWN_SECS > 0 and order_action in ("BUY", "SELL"):
        _sig_key  = (strategy_name or "", ticker or "", order_action)
        _now_ts   = time.time()
        with _risk_lock:
            _last_ts = _last_signal_ts.get(_sig_key, 0)
            _remaining = SIGNAL_COOLDOWN_SECS - (_now_ts - _last_ts)
            if _remaining > 0:
                log.info(
                    "Duplicate signal dropped — %s %s %s (%.1fs cooldown remaining)",
                    order_action, ticker, strategy_name, _remaining,
                )
                return jsonify({"status": "skipped", "reason": "duplicate_signal",
                                "cooldown_remaining": round(_remaining, 1)}), 200
            _last_signal_ts[_sig_key] = _now_ts

    conn = get_db()
    cur  = conn.cursor()

    # 1. Log the signal immediately
    trade_id = _insert_trade(cur, (
        ticker,
        raw_action,
        data.get("sentiment"),
        quantity,
        data.get("price"),
        data.get("time"),
        data.get("interval"),
        received_at,
        data.get("strategy"),
        broker_name or None,
    ))
    conn.commit()

    # Daily max loss circuit breaker — block new orders if halt is active
    if MAX_DAILY_LOSS < 0 and order_action in ("BUY", "SELL"):
        with _risk_lock:
            _halted = _risk_halted
        if _halted:
            log.warning("Risk halt active — order blocked: %s %s %s", order_action, quantity, ticker)
            _update_exec(cur, trade_id, "blocked", "Daily max loss limit reached — orders halted")
            conn.commit()
            conn.close()
            return jsonify({"status": "blocked", "reason": "daily_loss_limit"}), 200

    # Per-strategy block (set by position stop monitor)
    if strategy_name and order_action in ("BUY", "SELL"):
        with _risk_lock:
            _block_info = _blocked_strategies.get(strategy_name)
        if _block_info:
            log.warning("Strategy '%s' is blocked — order rejected: %s %s %s",
                        strategy_name, order_action, quantity, ticker)
            _update_exec(cur, trade_id, "blocked",
                         f"Strategy blocked: {_block_info['reason']}")
            conn.commit()
            conn.close()
            return jsonify({
                "status":   "blocked",
                "reason":   "strategy_blocked",
                "strategy": strategy_name,
                "detail":   _block_info,
            }), 200

    # 2. Route to broker(s) — supports single or multi-broker pipelines
    exec_status = None
    exec_detail = None

    if not broker_targets:
        exec_status = "error"
        exec_detail = f"No routing pipeline matched strategy '{strategy_name}' — signal logged but no order placed. Check your Signal Router for a typo in the strategy name."
        log.warning("Webhook: no broker resolved for strategy '%s' — signal logged only", strategy_name)

    # Normalise targets: ib-paper/ib-live → ib, alpaca-paper/alpaca-live → alpaca
    def _broker_family(t):
        if t in ("ib", "ib-paper", "ib-live"):    return "ib"
        if t in ("alpaca", "alpaca-paper", "alpaca-live"): return "alpaca"
        return t

    # All broker targets are now async — Alpaca/Coinbase fire in a background
    # thread so the webhook returns immediately and TradingView never times out.
    coinbase_targets = [t for t in broker_targets if _broker_family(t) == "coinbase"]
    alpaca_targets   = [t for t in broker_targets if _broker_family(t) == "alpaca"]
    ib_targets       = [t for t in broker_targets if _broker_family(t) == "ib"]

    # --- Coinbase (sync-only; typically sub-second) ---
    for target in coinbase_targets:
        if coinbase_broker is None:
            exec_status = "error"
            exec_detail = "Coinbase broker not initialised — set COINBASE_KEY + COINBASE_SECRET env vars"
            log.warning("Coinbase order skipped: broker not initialised")
        elif order_action not in ("BUY", "SELL"):
            exec_status = "skipped"
            exec_detail = f"No order placed for action '{raw_action}'"
        else:
            try:
                result = coinbase_broker.place_order(
                    ticker   = ticker,
                    action   = order_action,
                    quantity = quantity,
                    price    = data.get("price") if data.get("order_type") == "LMT" else None,
                    sec_type = sec_type,
                    currency = currency,
                )
                exec_status = "ok" if result.get("success") else "error"
                exec_detail = json.dumps(result)
                log.info("Coinbase order %s %s %s: %s", order_action, quantity, ticker, result)
            except Exception as e:
                exec_status = "error"
                exec_detail = str(e)
                log.error("Coinbase order failed for %s %s %s: %s", order_action, quantity, ticker, e)

    # Commit any sync (Coinbase) results before launching async threads
    if conn and exec_status is not None:
        _update_exec(cur, trade_id, exec_status, exec_detail)
        conn.commit()

    # --- Alpaca (async — order placement can take 1–3 s; we return 200 first) ---
    for target in alpaca_targets:
        if alpaca_broker is None:
            if conn:
                _update_exec(cur, trade_id, "error",
                             "Alpaca broker not initialised — set ALPACA_KEY + ALPACA_SECRET env vars")
                conn.commit()
            log.warning("Alpaca order skipped: broker not initialised")
        elif order_action not in ("BUY", "SELL"):
            if conn:
                _update_exec(cur, trade_id, "skipped", f"No order placed for action '{raw_action}'")
                conn.commit()
            log.info("Webhook action '%s' logged but no Alpaca order placed", raw_action)
        else:
            # Close DB before handing off — background thread opens its own connection
            if conn:
                conn.commit()
                conn.close()
                conn = None

            # Capture loop variables for the closure
            _ticker      = ticker
            _action      = order_action
            _raw_action  = raw_action
            _qty         = quantity
            _price       = data.get("price") if data.get("order_type") == "LMT" else None
            _sec_type    = sec_type
            _currency    = currency
            _trade_id    = trade_id
            _opt_prem    = opt_target_prem
            _opt_exp     = opt_expiry_type
            _opt_right   = opt_right_ovr
            _opt_ctrs    = opt_contracts

            def _place_alpaca_async(
                ticker=_ticker, action=_action, qty=_qty, price=_price,
                sec_type=_sec_type, currency=_currency, trade_id=_trade_id,
                opt_prem=_opt_prem, opt_exp=_opt_exp, opt_right=_opt_right, opt_ctrs=_opt_ctrs,
            ):
                _exec_status = _exec_detail = None
                try:
                    if opt_prem is not None:
                        opt_direction = "call" if action == "BUY" else "put"
                        if opt_right:
                            opt_direction = "call" if opt_right == "C" else "put"
                        result = alpaca_broker.place_option_order(
                            underlying     = ticker,
                            direction      = opt_direction,
                            expiry_type    = opt_exp or "friday",
                            contracts      = opt_ctrs,
                            target_premium = float(opt_prem),
                        )
                    else:
                        result = alpaca_broker.place_order(
                            ticker   = ticker,
                            action   = action,
                            quantity = qty,
                            price    = price,
                            sec_type = sec_type,
                            currency = currency,
                        )
                    _exec_status = "ok" if result.get("success") else "error"
                    _exec_detail = json.dumps(result)
                    log.info("Alpaca order %s %s %s: %s", action, qty, ticker, result)
                    # If we cancelled a pending BUY, mark the original BUY trade record
                    # as "cancelled" so it doesn't appear as an orphaned/open trade.
                    if result.get("cancelled_buy") and result.get("cancelled_order_ids"):
                        _ph = placeholder()
                        for cid in result["cancelled_order_ids"]:
                            try:
                                _c2 = get_db()
                                _cur2 = _c2.cursor()
                                _cur2.execute(
                                    f"UPDATE trades SET exec_status={_ph}, exec_detail={_ph}"
                                    f" WHERE exec_detail LIKE {_ph} AND exec_status='ok'",
                                    ("cancelled", f"BUY order {cid} cancelled by SELL signal", f"%{cid}%"),
                                )
                                _c2.commit()
                                _c2.close()
                                log.info("Marked BUY trade with order_id %s as cancelled", cid)
                            except Exception as _me:
                                log.warning("Could not mark BUY trade cancelled for order %s: %s", cid, _me)
                except Exception as e:
                    _exec_status = "error"
                    _exec_detail = str(e)
                    log.error("Alpaca order failed for %s %s %s: %s", action, qty, ticker, e)
                finally:
                    try:
                        _c = get_db()
                        _cur = _c.cursor()
                        _update_exec(_cur, trade_id, _exec_status, _exec_detail)
                        _c.commit()
                        _c.close()
                    except Exception as e:
                        log.error("Failed to update exec status for trade %s: %s", trade_id, e)

            threading.Thread(target=_place_alpaca_async, daemon=True).start()

    for target in ib_targets:
        _live = (target == "ib-live") or (use_live_broker and target != "ib-paper")
        active_broker  = ib_broker_live if (_live and ib_broker_live) else ib_broker
        submit_task    = _submit_ib_live_task if (_live and ib_broker_live) else _submit_ib_task
        mode_label     = "live" if (_live and ib_broker_live) else "paper"

        if active_broker is None:
            if conn:
                _update_exec(cur, trade_id, "error",
                             "IB live broker not initialised — set IB_HOST_LIVE env var"
                             if _live else
                             "IB broker not initialised — check IB_HOST env var")
                conn.commit()
        elif order_action not in ("BUY", "SELL"):
            if conn:
                _update_exec(cur, trade_id, "skipped", f"No order placed for action '{raw_action}'")
                conn.commit()
            log.info("Webhook action '%s' logged but no IB order placed", raw_action)
        else:
            # Close DB before handing off — background thread opens its own connection
            if conn:
                conn.commit()
                conn.close()
                conn = None

            def _place_async():
                _exec_status = _exec_detail = None
                try:
                    if sec_type.upper() == "OPT":
                        current_price = float(data.get("price") or 0)
                        if not current_price:
                            raise ValueError("'price' (underlying price) required for options orders")
                        if opt_target_prem is not None:
                            opt = submit_task(
                                active_broker.select_option_by_premium,
                                ticker, order_action, current_price,
                                _timeout       = 45,
                                target_premium = opt_target_prem,
                                expiry_type    = opt_expiry_type,
                                right_override = opt_right_ovr,
                                option_expiry  = data.get("option_expiry") or None,
                            )
                        else:
                            opt = submit_task(
                                active_broker.select_option,
                                ticker, order_action, current_price,
                                _timeout      = 45,
                                option_expiry = data.get("option_expiry") or None,
                                max_spread    = float(data.get("max_spread", 1.0)),
                                strike_offset = int(data.get("strike_offset", 0)),
                            )
                        result = submit_task(
                            active_broker.place_order,
                            ticker, order_action, quantity, None,
                            _timeout = 20,
                            sec_type = "OPT",
                            currency = currency,
                            expiry   = opt["expiry"],
                            strike   = opt["strike"],
                            right    = opt["right"],
                        )
                        _exec_status = "ok" if result.get("success") else "error"
                        _exec_detail = json.dumps({**result, "option_selected": opt, "mode": mode_label})
                        log.info("IB %s option order %s %s %s %s %s: %s",
                                 mode_label, order_action, ticker,
                                 opt["expiry"], opt["strike"], opt["right"], result)
                    else:
                        result = submit_task(
                            active_broker.place_order,
                            ticker   = ticker,
                            action   = order_action,
                            quantity = quantity,
                            price    = data.get("price") if data.get("order_type") == "LMT" else None,
                            sec_type = sec_type,
                            currency = currency,
                            _timeout = 30,
                        )
                        _exec_status = "ok" if result.get("success") else "error"
                        _exec_detail = json.dumps({**result, "mode": mode_label})
                        log.info("IB %s order %s %s %s: %s",
                                 mode_label, order_action, quantity, ticker, result)
                except Exception as e:
                    _exec_status = "error"
                    _exec_detail = str(e)
                    log.error("IB async order failed for %s %s: %s", order_action, ticker, e)
                finally:
                    try:
                        _c = get_db()
                        _cur = _c.cursor()
                        _update_exec(_cur, trade_id, _exec_status, _exec_detail)
                        _c.commit()
                        _c.close()
                    except Exception as e:
                        log.error("Failed to update exec status for trade %s: %s", trade_id, e)

            threading.Thread(target=_place_async, daemon=True).start()

    if conn:
        conn.commit()
        conn.close()
    return jsonify({"status": "ok", "id": trade_id}), 200


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
                q = open_shorts.get(sym, [])
                if q:
                    ep, eq, et, es = q.pop(-1)  # LIFO: most recent short
                    closed.append({"pnl": round((ep - price) * min(qty, eq), 2), "strategy": es,
                                   "ticker": sym, "date": date_str, "entry_time": et, "exit_time": fill_ts})
                else:
                    open_longs.setdefault(sym, []).append((price, qty, fill_ts, strat))
            elif side == "SLD":
                q = open_longs.get(sym, [])
                if q:
                    ep, eq, et, es = q.pop(-1)  # LIFO: most recent long
                    closed.append({"pnl": round((price - ep) * min(qty, eq), 2), "strategy": es,
                                   "ticker": sym, "date": date_str, "entry_time": et, "exit_time": fill_ts})
                else:
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
                    q = day_shorts.get(sym, [])
                    if q:
                        ep, eq, et, es = q.pop(0)
                        daily_closed.append({"pnl": round((ep - price) * min(qty, eq), 2), "strategy": es,
                                             "entry_strategy": es, "exit_strategy": strat,
                                             "ticker": sym, "date": date_str, "entry_time": et, "exit_time": fill_ts})
                    else:
                        day_longs.setdefault(sym, []).append((price, qty, fill_ts, strat))
                elif side == "SLD":
                    q = day_longs.get(sym, [])
                    if q:
                        ep, eq, et, es = q.pop(0)
                        daily_closed.append({"pnl": round((price - ep) * min(qty, eq), 2), "strategy": es,
                                             "entry_strategy": es, "exit_strategy": strat,
                                             "ticker": sym, "date": date_str, "entry_time": et, "exit_time": fill_ts})
                    else:
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
                model="claude-sonnet-4-6",
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
