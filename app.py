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

    for pos in all_positions:
        upnl   = float(pos.get("unrealized_pnl") or 0)
        symbol = pos["symbol"]
        broker = pos["broker"]

        if upnl > MAX_POSITION_LOSS:   # e.g. -150 > -200 → still OK
            continue

        with _risk_lock:
            if symbol in _auto_closed_symbols:
                continue
            _auto_closed_symbols.add(symbol)

        strategy = _find_strategy_for_symbol(symbol)

        log.error(
            "POSITION STOP: %s unrealized P&L $%.2f hit limit $%.2f [%s] — "
            "closing position, blocking strategy '%s'",
            symbol, upnl, MAX_POSITION_LOSS, broker, strategy,
        )

        # Close the position
        try:
            if broker == "alpaca":
                alpaca_broker.close_position(symbol)
            elif broker == "ib" and _ib_task_queue is not None:
                _submit_ib_task(ib_broker.close_position, symbol, pos.get("qty", 0), _timeout=30)
            elif broker == "ib-live" and _ib_live_task_queue is not None:
                _submit_ib_live_task(ib_broker_live.close_position, symbol, pos.get("qty", 0), _timeout=30)
            log.info("Position stop: %s closed on %s", symbol, broker)
        except Exception as _e:
            log.error("Position stop close failed for %s: %s", symbol, _e)

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
    """Background thread: poll positions every 30s, close any that breach MAX_POSITION_LOSS."""
    time.sleep(25)  # stagger from risk monitor
    while True:
        if MAX_POSITION_LOSS < 0:
            try:
                _check_position_stops()
            except Exception as _e:
                log.warning("Position monitor error: %s", _e)
        time.sleep(30)


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

    # Fire sync brokers first (Alpaca, Coinbase), then IB last (async, closes conn)
    sync_targets = [t for t in broker_targets if _broker_family(t) in ("alpaca", "coinbase")]
    ib_targets   = [t for t in broker_targets if _broker_family(t) == "ib"]

    for target in sync_targets:
        if _broker_family(target) == "alpaca":
            if alpaca_broker is None:
                exec_status = "error"
                exec_detail = "Alpaca broker not initialised — set ALPACA_KEY + ALPACA_SECRET env vars"
                log.warning("Alpaca order skipped: broker not initialised")
            elif order_action not in ("BUY", "SELL"):
                exec_status = "skipped"
                exec_detail = f"No order placed for action '{raw_action}'"
            else:
                try:
                    if opt_target_prem is not None:
                        # Options order — direction derived from signal action
                        opt_direction = "call" if order_action == "BUY" else "put"
                        if opt_right_ovr:
                            opt_direction = "call" if opt_right_ovr == "C" else "put"
                        result = alpaca_broker.place_option_order(
                            underlying     = ticker,
                            direction      = opt_direction,
                            expiry_type    = opt_expiry_type or "friday",
                            contracts      = opt_contracts,
                            target_premium = float(opt_target_prem),
                        )
                    else:
                        result = alpaca_broker.place_order(
                            ticker   = ticker,
                            action   = order_action,
                            quantity = quantity,
                            price    = data.get("price") if data.get("order_type") == "LMT" else None,
                            sec_type = sec_type,
                            currency = currency,
                        )
                    exec_status = "ok" if result.get("success") else "error"
                    exec_detail = json.dumps(result)
                    log.info("Alpaca order %s %s %s: %s", order_action, quantity, ticker, result)
                except Exception as e:
                    exec_status = "error"
                    exec_detail = str(e)
                    log.error("Alpaca order failed for %s %s %s: %s", order_action, quantity, ticker, e)

        elif _broker_family(target) == "coinbase":
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

    # Commit sync results before handing off to IB async thread
    if conn and exec_status is not None:
        _update_exec(cur, trade_id, exec_status, exec_detail)
        conn.commit()

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


@app.route("/backtester")
def backtester():
    return render_template("backtester.html")


@app.route("/backtest-lib")
def backtest_lib():
    return render_template("bt.html")


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
            result[r["slug"]] = json.loads(r["params"])
    except Exception as _e:
        log.warning("Failed to load user strategies: %s", _e)
    return jsonify(result)


@app.route("/api/bt/strategies/save", methods=["POST"])
def bt_strategy_save():
    """Save a converted Pine Script strategy to the DB."""
    import re
    body = request.get_json(silent=True) or {}
    name = (body.get("name") or "").strip()
    code = (body.get("code") or "").strip()
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
                f"INSERT INTO user_strategies (slug, name, code, params, created_at) "
                f"VALUES ({p},{p},{p},{p},{p}) "
                f"ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name, code=EXCLUDED.code, params=EXCLUDED.params",
                (slug, name, code, json.dumps(params), time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            )
        else:
            cur.execute(
                f"INSERT OR REPLACE INTO user_strategies (slug, name, code, params, created_at) "
                f"VALUES ({p},{p},{p},{p},{p})",
                (slug, name, code, json.dumps(params), time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
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
    """Stream a Pine Script → backtesting.py Strategy class via Claude."""
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
        "1. Output ONLY valid Python code — no markdown fences, no explanation text\n"
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
        "12. Include a short docstring describing the strategy"
    )

    def generate():
        try:
            client = _anthropic.Anthropic(api_key=api_key)
            with client.messages.stream(
                model="claude-sonnet-4-6",
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
    cash           = float(body.get("cash", 10000))
    strategy_type  = body.get("strategy_type", "builtin")   # "builtin" | "converted" | "saved"
    strategy_name  = body.get("strategy_name", "camarilla")
    strategy_code  = body.get("strategy_code", "")
    strategy_params = body.get("params", {})

    # ── Resolve strategy class ────────────────────────────────────────────────
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
        except Exception as e:
            return jsonify({"error": f"Strategy code error: {e}"}), 400
    else:
        entry = STRATEGIES.get(strategy_name)
        if not entry:
            return jsonify({"error": f"Unknown strategy: {strategy_name}"}), 400
        strategy_cls = entry[0]

    # ── Fetch data ────────────────────────────────────────────────────────────
    try:
        import yfinance as yf
        raw = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True, progress=False)
        if raw.empty:
            return jsonify({"error": f"No data returned for {ticker}"}), 400
        if hasattr(raw.columns, "levels"):
            raw.columns = [c[0] if isinstance(c, tuple) else c for c in raw.columns]
        df = raw[["Open", "High", "Low", "Close", "Volume"]].dropna()
        if len(df) < 30:
            return jsonify({"error": f"Only {len(df)} bars — need at least 30"}), 400
    except Exception as e:
        return jsonify({"error": f"Data fetch failed: {e}"}), 500

    # ── Run backtest ──────────────────────────────────────────────────────────
    try:
        bt    = Backtest(df, strategy_cls, cash=cash, commission=0.001, exclusive_orders=True,
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
    Streaming SSE endpoint.  For each ticker × timeframe, runs backtesting.py
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

        # ── Resolve strategy class ───────────────────────────────────────────
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
        else:
            entry = STRATEGIES.get(strategy_name)
            if entry:
                strategy_cls = entry[0]

        if strategy_cls is None:
            yield _sse({"type": "error", "msg": "Could not resolve strategy class"}); return

        # ── Build param sequences ─────────────────────────────────────────────
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

                # ── Fetch data ────────────────────────────────────────────────
                try:
                    if data_source == "alpaca":
                        from strategies.data import fetch_bars_alpaca
                        raw = fetch_bars_alpaca(ticker, start_date, end_date, tf)
                    else:
                        from strategies.data import fetch_bars
                        raw = fetch_bars(ticker, start_date, end_date, tf)

                    if len(raw) < 30:
                        yield _sse({"type": "warning", "msg": f"{ticker}/{tf}: only {len(raw)} bars — skipped"})
                        continue

                    df = _pd.DataFrame(raw).set_index("time")
                    df.index = _pd.to_datetime(df.index)
                    df.columns = [c.title() for c in df.columns]
                    df = df[["Open", "High", "Low", "Close"]].dropna()
                    df["Volume"] = 0

                except Exception as e:
                    yield _sse({"type": "warning", "msg": f"{ticker}/{tf} data error: {e}"})
                    continue

                # ── Run optimize ──────────────────────────────────────────────
                try:
                    yield _sse({"type": "progress", "msg": f"Optimizing {ticker} / {tf}…", "pct": pct + 1})
                    bt = Backtest(df, strategy_cls, cash=cash, commission=0.001, exclusive_orders=True)

                    if opt_kwargs:
                        try:
                            best, heatmap = bt.optimize(
                                **opt_kwargs, maximize=maximize,
                                return_heatmap=True, max_tries=50, method="sambo")
                        except Exception:
                            best, heatmap = bt.optimize(
                                **opt_kwargs, maximize=maximize,
                                return_heatmap=True, max_tries=50)
                        best_params = {k: getattr(best._strategy, k, None) for k in opt_kwargs}
                    else:
                        best = bt.run()
                        best_params = {}

                    def _f(v, n=2):
                        try: return round(float(v or 0), n)
                        except Exception: return 0

                    stats_dict = {
                        "Return [%]":        _f(best.get("Return [%]")),
                        "Sharpe Ratio":      _f(best.get("Sharpe Ratio"), 3),
                        "Max. Drawdown [%]": _f(best.get("Max. Drawdown [%]")),
                        "Win Rate [%]":      _f(best.get("Win Rate [%]")),
                        "# Trades":          int(best.get("# Trades") or 0),
                        "Profit Factor":     _f(best.get("Profit Factor"), 3),
                        "Calmar Ratio":      _f(best.get("Calmar Ratio"), 3),
                    }
                    score = _f(best.get(maximize), 4)

                    row_data = {
                        "type": "result",
                        "run_id": run_id, "strategy": strategy_name,
                        "ticker": ticker, "timeframe": tf,
                        "params": best_params, "stats": stats_dict,
                        "maximize": maximize, "score": score,
                    }

                    p = placeholder()
                    try:
                        conn = get_db(); cur = conn.cursor()
                        cur.execute(
                            f"INSERT INTO bt_results (run_id,strategy,ticker,timeframe,params,stats,maximize,score,created_at)"
                            f" VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p})",
                            (run_id, strategy_name, ticker, tf,
                             _json.dumps(best_params), _json.dumps(stats_dict),
                             maximize, score, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
                        conn.commit(); conn.close()
                    except Exception as db_e:
                        log.warning(f"bt_results DB error: {db_e}")

                    yield _sse(row_data)

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



@app.route("/api/backtest/run", methods=["POST"])
def backtest_run():
    from strategies.camarilla import run_backtest, optimise
    from strategies.data import fetch_bars, fetch_bars_ib

    body        = request.get_json(silent=True) or {}
    ticker      = (body.get("ticker") or "").strip().upper()
    start_date  = body.get("start_date", "2016-01-01")
    end_date    = body.get("end_date",   "2026-01-01")
    interval    = body.get("interval",   "1d")
    mode        = body.get("mode",       "single")
    data_source = body.get("data_source", "yfinance")

    if not ticker:
        return jsonify({"error": "ticker is required"}), 400

    # Fetch OHLCV bars
    try:
        if data_source == "ib":
            if ib_broker is None:
                return jsonify({"error": "IB not configured — set IB_HOST env var"}), 400
            if not ib_broker.is_connected():
                return jsonify({"error": "IB Gateway not connected — check the gateway service"}), 400
            bars = _submit_ib_task(
                fetch_bars_ib,
                ib_broker, ticker, start_date, end_date, interval,
                _timeout=120,
            )
        else:
            bars = fetch_bars(ticker, start_date, end_date, interval)
    except Exception as e:
        return jsonify({"error": f"Data fetch failed for {ticker}: {e}"}), 500

    if len(bars) < 10:
        return jsonify({"error": f"Only {len(bars)} bars returned for {ticker} "
                                 f"({start_date} → {end_date}, {interval}). "
                                 f"Try a wider date range or a different interval."}), 400

    if mode == "optimize":
        try:
            opt_params = {
                "long_trail_activation_range":  [
                    float(body.get("lta_min", 20)),
                    float(body.get("lta_max", 60)),
                    float(body.get("lta_step", 10)),
                ],
                "short_trail_activation_range": [
                    float(body.get("sta_min", 5)),
                    float(body.get("sta_max", 20)),
                    float(body.get("sta_step", 5)),
                ],
                "long_hard_stop_range": [
                    float(body.get("lhs_min", 50)),
                    float(body.get("lhs_max", 100)),
                    float(body.get("lhs_step", 10)),
                ],
                "short_hard_stop_range": [
                    float(body.get("shs_min", 10)),
                    float(body.get("shs_max", 30)),
                    float(body.get("shs_step", 5)),
                ],
                "trail_distance": float(body.get("trail_distance", 1)),
            }
            grid = optimise(bars, opt_params)
            return jsonify({"mode": "optimize", "bars_loaded": len(bars), "grid": grid})
        except Exception as e:
            log.exception("Backtest optimize error")
            return jsonify({"error": str(e)}), 500
    else:
        try:
            params = {
                "long_trail_activation":  float(body.get("lta", 40)),
                "short_trail_activation": float(body.get("sta", 10)),
                "long_hard_stop":         float(body.get("lhs", 70)),
                "short_hard_stop":        float(body.get("shs", 20)),
                "trail_distance":         float(body.get("trail_distance", 1)),
            }
            result = run_backtest(bars, params)
            result["mode"] = "single"
            result["bars_loaded"] = len(bars)
            return jsonify(result)
        except Exception as e:
            log.exception("Backtest single error")
            return jsonify({"error": str(e)}), 500


@app.route("/api/backtest/analyse", methods=["POST"])
def backtest_analyse():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured on this server"}), 503

    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({"error": "anthropic package not installed"}), 503

    data     = request.get_json(silent=True) or {}
    analysis_type = data.get("type", "grid")
    ticker        = data.get("ticker", "").strip().upper() or "the ticker"

    if analysis_type == "stats":
        s = data.get("stats")
        if not s:
            return jsonify({"error": "No stats data to analyse"}), 400
        source_label = "TradingView Strategy Tester export" if data.get("source") == "trade_list" else "OHLCV backtest simulation"
        prompt = (
            f"I ran the Camarilla pivot breakout strategy on {ticker} ({source_label}).\n\n"
            f"Strategy: enters long when price breaks above H4 with EMA8 below close; "
            f"enters short when price breaks below L4 with EMA8 above close.\n\n"
            f"Results:\n"
            f"  Total trades:   {s.get('total_trades')}\n"
            f"  Win rate:       {s.get('win_rate')}%\n"
            f"  Profit factor:  {s.get('profit_factor')}\n"
            f"  Total P&L:      {s.get('total_pnl')} pts\n"
            f"  Max drawdown:   {s.get('max_drawdown')} pts\n"
            f"  Avg win:        {s.get('avg_win')} pts\n"
            f"  Avg loss:       {s.get('avg_loss')} pts\n"
            f"  Wins / Losses:  {s.get('wins')} / {s.get('losses')}\n\n"
            f"Please provide:\n"
            f"1. Overall assessment — is this a viable strategy? What stands out?\n"
            f"2. Risk/reward analysis — comment on the avg win vs avg loss ratio and drawdown\n"
            f"3. Any concerns or weaknesses visible in these stats\n"
            f"4. Specific suggestions to improve performance"
        )
    else:
        grid = data.get("grid", [])
        if not grid:
            return jsonify({"error": "No grid data to analyse"}), 400
        top = grid[:20]
        rows = "\n".join(
            f"#{i+1}: long_trail={r['long_trail_activation']} short_trail={r['short_trail_activation']} "
            f"long_stop={r['long_hard_stop']} short_stop={r['short_hard_stop']} | "
            f"PF={r['profit_factor']} win={r['win_rate']}% trades={r['total_trades']} "
            f"pnl={r['total_pnl']} maxDD={r['max_drawdown']}"
            for i, r in enumerate(top)
        )
        prompt = (
            f"I ran a parameter grid search on the Camarilla pivot breakout strategy.\n\n"
            f"Strategy: enters long when price breaks above H4 (Camarilla level) with EMA8 below "
            f"close; enters short when price breaks below L4 with EMA8 above close. "
            f"Tested on {ticker} 5-minute bars.\n\n"
            f"Exit parameters optimised:\n"
            f"  long_trail  — profit points before the long trailing stop activates\n"
            f"  short_trail — profit points before the short trailing stop activates\n"
            f"  long_stop   — hard stop loss in points for longs\n"
            f"  short_stop  — hard stop loss in points for shorts\n\n"
            f"Top {len(top)} combinations by profit factor:\n{rows}\n\n"
            f"Please provide:\n"
            f"1. Key patterns — which parameters consistently appear in top results?\n"
            f"2. Recommended parameter set for live trading and why\n"
            f"3. Any concerns (overfitting, trade count, long/short asymmetry, etc.)\n"
            f"4. Suggested ranges to explore in a follow-up optimisation"
        )

    client = _anthropic.Anthropic(api_key=api_key)

    def generate():
        try:
            with client.messages.stream(
                model="claude-sonnet-4-6",
                max_tokens=1200,
                system=(
                    "You are a quantitative trading analyst. Be concise and actionable. "
                    "Use markdown: ## for section headings, **bold** for key values, "
                    "bullet points for lists."
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


@app.route("/api/backtest/agent/run", methods=["POST"])
def backtest_agent_run():
    """
    Autonomous multi-ticker iterative backtesting agent.
    Streams SSE events as it works through:
      1. Fetch bars for each ticker via yfinance
      2. N iterations of: grid search → Claude refines ranges
      3. Final combined ranking + best params
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 503

    from datetime import date, timedelta

    # Accept both multipart/form-data (CSV upload) and application/json
    if request.content_type and "multipart" in request.content_type:
        body        = request.form
        data_source = body.get("data_source", "csv")
        iterations  = min(int(body.get("iterations", 2)), 5)
        interval    = body.get("interval", "1h")
        # CSV bars are pre-loaded; tickers/dates come from the filename field
        csv_ticker  = (body.get("ticker") or "UPLOADED").strip().upper()
        tickers     = [csv_ticker]
        start_date  = end_date = None
    else:
        body        = request.get_json(silent=True) or {}
        tickers     = [t.strip().upper() for t in body.get("tickers", []) if t.strip()][:5]
        interval    = body.get("interval",    "1h")
        iterations  = min(int(body.get("iterations", 2)), 5)
        data_source = body.get("data_source", "yfinance")
        today       = date.today()
        if data_source == "ib":
            ib_max_days = {"5m": 30, "15m": 60, "30m": 90, "1h": 365, "1d": 365 * 5}
            max_days   = ib_max_days.get(interval, 365)
            earliest   = today - timedelta(days=max_days)
            start_date = max(earliest.isoformat(), body.get("start_date", earliest.isoformat()))
            end_date   = min(today.isoformat(),    body.get("end_date",   today.isoformat()))
        else:
            max_days  = 58 if interval in ("5m", "15m", "30m") else 729
            earliest  = today - timedelta(days=max_days)
            start_date = max(earliest.isoformat(), body.get("start_date", earliest.isoformat()))
            end_date   = min(today.isoformat(),    body.get("end_date",   today.isoformat()))

    # Pre-parse CSV before entering the streaming generator
    csv_bars_by_ticker  = {}
    csv_trades_by_ticker = {}
    csv_is_trade_list   = False
    if data_source == "csv":
        from strategies.camarilla import parse_bars as _parse_bars, parse_trade_list as _parse_trade_list, is_trade_list_csv as _is_trade_list_csv
        f = request.files.get("csv_file")
        if not f:
            return jsonify({"error": "No CSV file uploaded"}), 400
        file_bytes = f.read()
        if _is_trade_list_csv(file_bytes):
            csv_is_trade_list = True
            csv_trades_by_ticker[csv_ticker] = _parse_trade_list(file_bytes)
        else:
            csv_bars_by_ticker[csv_ticker] = _parse_bars(file_bytes)

    if not tickers:
        return jsonify({"error": "No tickers provided"}), 400

    def sse(obj):
        return f"data: {json.dumps(obj)}\n\n"

    def generate():
        try:
            import anthropic as _anthropic
            from strategies.data import fetch_bars, fetch_bars_ib
            from strategies.camarilla import optimise
            from collections import defaultdict

            client = _anthropic.Anthropic(api_key=api_key)

            # ── Step 1: Fetch / load bars ─────────────────────────────────
            # Short-circuit: trade list CSV — skip grid search, analyse directly
            if csv_is_trade_list:
                from strategies.camarilla import _compute_stats
                for tkr, trades in csv_trades_by_ticker.items():
                    stats = _compute_stats(trades)
                    yield sse({"type": "status", "msg": f"Loaded {len(trades)} trades from TV Strategy Tester CSV for {tkr}"})
                    yield sse({"type": "status", "msg": "Claude is analysing the trade list…"})
                    rows = "\n".join(
                        f"#{i+1}: {t['direction']} entry={t['entry_price']} exit={t['exit_price']} "
                        f"pnl={t['pnl']} entry={t['entry_time']} exit={t['exit_time']} reason={t['exit_reason']}"
                        for i, t in enumerate(trades[:50])
                    )
                    analysis_prompt = (
                        f"TradingView Strategy Tester results for {tkr} — {len(trades)} trades.\n\n"
                        f"Summary stats:\n"
                        f"  Total trades: {stats['total_trades']}, Win rate: {stats['win_rate']}%\n"
                        f"  Total P&L: {stats['total_pnl']}, Profit factor: {stats['profit_factor']}\n"
                        f"  Avg win: {stats['avg_win']}, Avg loss: {stats['avg_loss']}\n"
                        f"  Max drawdown: {stats['max_drawdown']}\n\n"
                        f"First {min(50, len(trades))} trades:\n{rows}\n\n"
                        f"Provide:\n"
                        f"1. Overall strategy assessment\n"
                        f"2. Win/loss pattern observations\n"
                        f"3. Key risks and concerns\n"
                        f"4. One-line verdict on whether this strategy is ready to trade"
                    )
                    summary_text = ""
                    try:
                        with client.messages.stream(
                            model="claude-sonnet-4-6",
                            max_tokens=800,
                            system="Quantitative trading analyst. Be concise. Use ## headings and bullet points.",
                            messages=[{"role": "user", "content": analysis_prompt}],
                        ) as stream:
                            for text in stream.text_stream:
                                summary_text += text
                                yield sse({"type": "summary_chunk", "text": text})
                    except Exception as e:
                        yield sse({"type": "warning", "msg": f"Claude analysis failed: {e}"})

                    yield sse({"type": "final", "best": {}, "combined": [],
                               "tickers": [tkr], "iterations": 0})
                yield sse({"type": "done"})
                return

            source_label = {"ib": "IB", "csv": "CSV", "yfinance": "Yahoo Finance"}.get(data_source, "Yahoo Finance")
            all_bars = {}
            for tkr in tickers:
                if data_source == "csv":
                    bars = csv_bars_by_ticker.get(tkr, [])
                    yield sse({"type": "status", "msg": f"Loaded {len(bars)} bars from CSV for {tkr}"})
                else:
                    yield sse({"type": "status", "msg": f"Fetching {tkr} {interval} bars via {source_label} ({start_date} → {end_date})…"})
                try:
                    if data_source == "ib":
                        import queue as _queue, threading as _threading
                        progress_q   = _queue.SimpleQueue()
                        result_hold  = [None]
                        error_hold   = [None]

                        def _on_chunk(cs, ce, n, _tkr=tkr):
                            progress_q.put({"type": "status", "msg": f"  {_tkr}: chunk {cs} → {ce} ({n} bars)"})

                        def _fetch():
                            try:
                                result_hold[0] = fetch_bars_ib(ib_broker, tkr, start_date, end_date,
                                                                interval, on_chunk=_on_chunk)
                            except Exception as exc:
                                error_hold[0] = exc
                            finally:
                                progress_q.put(None)  # sentinel

                        _threading.Thread(target=_fetch, daemon=True).start()
                        while True:
                            msg = progress_q.get()
                            if msg is None:
                                break
                            yield sse(msg)
                        if error_hold[0]:
                            raise error_hold[0]
                        bars = result_hold[0] or []
                    elif data_source != "csv":
                        bars = fetch_bars(tkr, start_date, end_date, interval)
                    if len(bars) < 50:
                        yield sse({"type": "warning", "msg": f"{tkr}: only {len(bars)} bars — skipping"})
                        continue
                    all_bars[tkr] = bars
                    yield sse({"type": "fetch_ok", "ticker": tkr, "bars": len(bars)})
                    # Warn early if bar count is likely to produce too few trades
                    if len(bars) < 500 and data_source == "yfinance" and interval in ("5m", "15m", "30m"):
                        yield sse({"type": "warning", "msg":
                            f"{tkr}: {len(bars)} bars ({start_date} → {end_date}) — "
                            f"Yahoo Finance limits {interval} data to ~58 days. "
                            f"Switch to '1h' for ~2 years of data and far more trades."})
                except Exception as e:
                    yield sse({"type": "warning", "msg": f"{tkr} fetch failed: {e}"})

            if not all_bars:
                yield sse({"type": "error", "msg": "No bars fetched for any ticker. Try a shorter date range or '1h' interval."})
                yield sse({"type": "done"})
                return

            # Starting parameter ranges
            opt_params = {
                "long_trail_activation_range":  [10, 60, 10],
                "short_trail_activation_range": [5,  30, 5],
                "long_hard_stop_range":         [30, 100, 10],
                "short_hard_stop_range":        [10, 50,  10],
                "trail_distance": 1,
            }

            iteration_results = []

            # ── Step 2: Iterative grid + Claude refine ────────────────────
            for iteration in range(1, iterations + 1):
                lta_r  = opt_params["long_trail_activation_range"]
                sta_r  = opt_params["short_trail_activation_range"]
                lhs_r  = opt_params["long_hard_stop_range"]
                shs_r  = opt_params["short_hard_stop_range"]
                combos = (
                    len(range(int(lta_r[0]), int(lta_r[1]) + 1, int(lta_r[2]))) *
                    len(range(int(sta_r[0]), int(sta_r[1]) + 1, int(sta_r[2]))) *
                    len(range(int(lhs_r[0]), int(lhs_r[1]) + 1, int(lhs_r[2]))) *
                    len(range(int(shs_r[0]), int(shs_r[1]) + 1, int(shs_r[2])))
                )
                yield sse({"type": "iter_start", "iteration": iteration, "of": iterations,
                           "combos": combos, "tickers": list(all_bars.keys())})

                # Run grid on each ticker
                ticker_grids = {}
                for tkr, bars in all_bars.items():
                    yield sse({"type": "status", "msg": f"  [{tkr}] Testing {combos} parameter combinations…"})
                    try:
                        grid = optimise(bars, opt_params)
                        ticker_grids[tkr] = grid
                        yield sse({"type": "ticker_done", "ticker": tkr, "top_pf": grid[0]["profit_factor"] if grid else None})
                    except Exception as e:
                        yield sse({"type": "warning", "msg": f"  [{tkr}] grid failed: {e}"})

                if not ticker_grids:
                    yield sse({"type": "warning", "msg": "No grids produced this iteration — check ranges"})
                    continue

                # Combine: for each param combo score across all tickers
                combo_scores = defaultdict(lambda: {"pfs": [], "wins": [], "trades": [], "pnls": [], "dds": []})
                for tkr, grid in ticker_grids.items():
                    for r in grid:
                        k = (r["long_trail_activation"], r["short_trail_activation"],
                             r["long_hard_stop"],        r["short_hard_stop"])
                        combo_scores[k]["pfs"].append(r["profit_factor"] or 0)
                        combo_scores[k]["wins"].append(r["win_rate"])
                        combo_scores[k]["trades"].append(r["total_trades"])
                        combo_scores[k]["pnls"].append(r["total_pnl"])
                        combo_scores[k]["dds"].append(r["max_drawdown"])

                combined = []
                for (lta, sta, lhs, shs), v in combo_scores.items():
                    n = len(v["pfs"])
                    combined.append({
                        "long_trail_activation":  lta,
                        "short_trail_activation": sta,
                        "long_hard_stop":         lhs,
                        "short_hard_stop":        shs,
                        "trail_distance":         opt_params["trail_distance"],
                        "avg_profit_factor":  round(sum(v["pfs"]) / n, 2),
                        "min_profit_factor":  round(min(v["pfs"]), 2),
                        "avg_win_rate":       round(sum(v["wins"]) / n, 1),
                        "avg_trades":         round(sum(v["trades"]) / n),
                        "ticker_count":       n,
                        "tickers_tested":     list(ticker_grids.keys()),
                    })

                # Sort by min PF (robustness across tickers) then avg PF
                combined.sort(key=lambda x: (x["min_profit_factor"], x["avg_profit_factor"]), reverse=True)
                top20 = combined[:20]
                iteration_results.append({"iteration": iteration, "combined": top20, "ticker_grids": {
                    tkr: g[:5] for tkr, g in ticker_grids.items()
                }})

                yield sse({"type": "iter_result", "iteration": iteration, "combined": top20})

                # Ask Claude to refine ranges (skip on last iteration)
                if iteration < iterations:
                    yield sse({"type": "status", "msg": f"  Claude is analysing iteration {iteration} results…"})
                    top5_rows = "\n".join(
                        f"#{i+1}: lta={r['long_trail_activation']} sta={r['short_trail_activation']} "
                        f"lhs={r['long_hard_stop']} shs={r['short_hard_stop']} "
                        f"avgPF={r['avg_profit_factor']} minPF={r['min_profit_factor']} "
                        f"win={r['avg_win_rate']}% trades={r['avg_trades']}"
                        for i, r in enumerate(top20[:10])
                    )
                    refine_prompt = (
                        f"Camarilla pivot strategy grid search iteration {iteration}/{iterations} "
                        f"on {', '.join(all_bars.keys())} ({interval} bars).\n\n"
                        f"Top 10 cross-ticker results (scored by min profit factor for robustness):\n{top5_rows}\n\n"
                        f"Return ONLY a JSON object with tighter search ranges centred on the optimal region. "
                        f"Make the ranges about 30-40% narrower than the current search. "
                        f"Use smaller steps to increase precision:\n"
                        f'{{"lta_min":N,"lta_max":N,"lta_step":N,'
                        f'"sta_min":N,"sta_max":N,"sta_step":N,'
                        f'"lhs_min":N,"lhs_max":N,"lhs_step":N,'
                        f'"shs_min":N,"shs_max":N,"shs_step":N}}'
                    )
                    try:
                        resp = client.messages.create(
                            model="claude-sonnet-4-6",
                            max_tokens=300,
                            system="Return only valid JSON. No markdown. No text outside the JSON object.",
                            messages=[{"role": "user", "content": refine_prompt}],
                        )
                        raw = resp.content[0].text.strip()
                        if raw.startswith("```"):
                            raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
                        p = json.loads(raw)
                        opt_params = {
                            "long_trail_activation_range":  [p["lta_min"], p["lta_max"], p["lta_step"]],
                            "short_trail_activation_range": [p["sta_min"], p["sta_max"], p["sta_step"]],
                            "long_hard_stop_range":         [p["lhs_min"], p["lhs_max"], p["lhs_step"]],
                            "short_hard_stop_range":        [p["shs_min"], p["shs_max"], p["shs_step"]],
                            "trail_distance": opt_params["trail_distance"],
                        }
                        yield sse({"type": "refined", "iteration": iteration, "params": opt_params})
                    except Exception as e:
                        yield sse({"type": "warning", "msg": f"  Claude refinement failed ({e}) — keeping current ranges"})

            # ── Step 3: Final summary ─────────────────────────────────────
            if iteration_results:
                final_combined = iteration_results[-1]["combined"]
                best = final_combined[0] if final_combined else {}

                yield sse({"type": "status", "msg": "Claude is writing the final analysis…"})
                rows = "\n".join(
                    f"#{i+1}: lta={r['long_trail_activation']} sta={r['short_trail_activation']} "
                    f"lhs={r['long_hard_stop']} shs={r['short_hard_stop']} "
                    f"avgPF={r['avg_profit_factor']} minPF={r['min_profit_factor']} "
                    f"win={r['avg_win_rate']}% trades={r['avg_trades']}"
                    for i, r in enumerate(final_combined[:10])
                )
                summary_prompt = (
                    f"I ran {iterations} iterations of Camarilla pivot strategy optimisation "
                    f"on {', '.join(all_bars.keys())} using {interval} bars.\n\n"
                    f"Final top 10 cross-ticker results:\n{rows}\n\n"
                    f"Provide:\n"
                    f"1. Recommended parameters for live trading and why\n"
                    f"2. Robustness assessment — how consistent are results across tickers?\n"
                    f"3. Any key risks or concerns\n"
                    f"4. One-line verdict on whether this strategy is ready to trade"
                )
                summary_text = ""
                try:
                    with client.messages.stream(
                        model="claude-sonnet-4-6",
                        max_tokens=800,
                        system="Quantitative trading analyst. Be concise. Use ## headings and bullet points.",
                        messages=[{"role": "user", "content": summary_prompt}],
                    ) as stream:
                        for text in stream.text_stream:
                            summary_text += text
                            yield sse({"type": "summary_chunk", "text": text})
                except Exception as e:
                    yield sse({"type": "warning", "msg": f"Summary generation failed: {e}"})

                yield sse({"type": "final", "best": best, "combined": final_combined,
                           "tickers": list(all_bars.keys()), "iterations": iterations})

        except Exception as e:
            log.exception("Agent run error")
            yield sse({"type": "error", "msg": str(e)})

        yield sse({"type": "done"})

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/backtest/step2/suggest", methods=["POST"])
def backtest_step2_suggest():
    """Phase 1 of Step 2: ask Claude for refined parameter ranges + best single set."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 503
    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({"error": "anthropic package not installed"}), 503

    body   = request.get_json(silent=True) or {}
    step1  = body.get("step1_analysis", "")
    ticker = body.get("ticker", "").strip().upper() or "the ticker"
    grid   = body.get("grid",  [])
    stats  = body.get("stats", {})

    if grid:
        top  = grid[:15]
        rows = "\n".join(
            f"#{i+1}: lta={r['long_trail_activation']} sta={r['short_trail_activation']} "
            f"lhs={r['long_hard_stop']} shs={r['short_hard_stop']} "
            f"PF={r['profit_factor']} win={r['win_rate']}% trades={r['total_trades']}"
            for i, r in enumerate(top)
        )
        context = f"Top {len(top)} optimization results:\n{rows}"
    else:
        context = (f"Performance: PF={stats.get('profit_factor')}, win={stats.get('win_rate')}%, "
                   f"avg_win={stats.get('avg_win')}, avg_loss={stats.get('avg_loss')}, "
                   f"trades={stats.get('total_trades')}")

    prompt = (
        f"Camarilla strategy analysis on {ticker}:\n{step1}\n\n{context}\n\n"
        f"Return ONLY a JSON object — no markdown, no text outside the braces. "
        f"Include the single best parameter set AND a tighter grid range focused on the optimal region:\n"
        f'{{"long_trail_activation":N,"short_trail_activation":N,'
        f'"long_hard_stop":N,"short_hard_stop":N,"trail_distance":N,'
        f'"lta_min":N,"lta_max":N,"lta_step":N,'
        f'"sta_min":N,"sta_max":N,"sta_step":N,'
        f'"lhs_min":N,"lhs_max":N,"lhs_step":N,'
        f'"shs_min":N,"shs_max":N,"shs_step":N,'
        f'"rationale":"one sentence"}}'
    )

    client = _anthropic.Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=500,
            system="Return only valid JSON. No markdown fences. No text outside the JSON object.",
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:])
            raw = raw.rsplit("```", 1)[0].strip()
        params = json.loads(raw)
        return jsonify({"params": params})
    except Exception as e:
        log.warning(f"Step2 suggest failed ({e}); using fallback")
        if grid:
            b = grid[0]
            params = {
                "long_trail_activation":  b["long_trail_activation"],
                "short_trail_activation": b["short_trail_activation"],
                "long_hard_stop":         b["long_hard_stop"],
                "short_hard_stop":        b["short_hard_stop"],
                "trail_distance":         b.get("trail_distance", 1),
                "lta_min": max(5,  b["long_trail_activation"]  - 15), "lta_max": b["long_trail_activation"]  + 15, "lta_step": 5,
                "sta_min": max(2,  b["short_trail_activation"] - 8),  "sta_max": b["short_trail_activation"] + 8,  "sta_step": 2,
                "lhs_min": max(10, b["long_hard_stop"]         - 20), "lhs_max": b["long_hard_stop"]         + 20, "lhs_step": 5,
                "shs_min": max(5,  b["short_hard_stop"]        - 10), "shs_max": b["short_hard_stop"]        + 10, "shs_step": 2,
                "rationale": "Best parameters from the optimization grid",
            }
        else:
            params = {
                "long_trail_activation": 40, "short_trail_activation": 10,
                "long_hard_stop": 70,        "short_hard_stop": 20, "trail_distance": 1,
                "lta_min": 25, "lta_max": 55, "lta_step": 5,
                "sta_min": 5,  "sta_max": 15, "sta_step": 2,
                "lhs_min": 50, "lhs_max": 90, "lhs_step": 10,
                "shs_min": 10, "shs_max": 30, "shs_step": 5,
                "rationale": "Default parameters — run Optimize with OHLCV data for better suggestions",
            }
        return jsonify({"params": params})


@app.route("/api/backtest/step2/script", methods=["POST"])
def backtest_step2_script():
    """Phase 3 of Step 2: stream a complete Pine Script v5 for the given parameters."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 503
    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({"error": "anthropic package not installed"}), 503

    body   = request.get_json(silent=True) or {}
    p      = body.get("params", {})
    ticker = body.get("ticker", "").strip().upper() or "the ticker"
    lta    = p.get("long_trail_activation",  40)
    sta    = p.get("short_trail_activation", 10)
    lhs    = p.get("long_hard_stop",         70)
    shs    = p.get("short_hard_stop",        20)
    trd    = p.get("trail_distance",          1)

    prompt = (
        f"Write a complete TradingView Pine Script v5 for the Camarilla Pivot Breakout strategy optimised for {ticker}.\n\n"
        f"Use these optimised values as input defaults:\n"
        f"  long_trail_activation  = {lta}  // price-units profit before long trailing stop activates\n"
        f"  short_trail_activation = {sta}\n"
        f"  long_hard_stop         = {lhs}  // hard stop distance in price units (long)\n"
        f"  short_hard_stop        = {shs}\n"
        f"  trail_distance         = {trd}  // trailing stop follows by this many price units\n\n"
        f"Strategy rules:\n"
        f"  H4 = prev_close + 1.1*(prev_high-prev_low)/2\n"
        f"  L4 = prev_close - 1.1*(prev_high-prev_low)/2\n"
        f"  EMA(8) trend filter\n"
        f"  Long:  close > H4 and open < H4 and ema8 < close\n"
        f"  Short: close < L4 and open > L4 and ema8 > close\n"
        f"  Long exit:  stop = avg_price - long_hard_stop; trail activates at avg_price + long_trail_activation, offset = trail_distance\n"
        f"  Short exit: stop = avg_price + short_hard_stop; trail activates at avg_price - short_trail_activation, offset = trail_distance\n\n"
        f"Include:\n"
        f"  //@version=5\n"
        f"  strategy() with overlay=true\n"
        f"  input.float() for all 5 parameters\n"
        f"  request.security() with barmerge.lookahead_on for previous-day OHLC\n"
        f"  strategy.entry() and strategy.exit() (use trail_offset in ticks = price/syminfo.mintick)\n"
        f"  plot() for H4 (green) and L4 (red)\n"
        f"  plotshape() for entry signals\n\n"
        f"Output ONLY the Pine Script code. No markdown fences. No explanation."
    )

    client = _anthropic.Anthropic(api_key=api_key)

    def generate():
        try:
            with client.messages.stream(
                model="claude-sonnet-4-6",
                max_tokens=2500,
                system="You are an expert TradingView Pine Script v5 developer. Output only valid Pine Script v5 code. No markdown. No explanation. No code fences.",
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
        if exec_status in ("blocked", "skipped", "error"):
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
