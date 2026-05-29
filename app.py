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

# Persist logs to ./logs/app.log so users can grab a copy after a stall/crash.
try:
    from logging.handlers import RotatingFileHandler
    _log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(_log_dir, exist_ok=True)
    _fh = RotatingFileHandler(os.path.join(_log_dir, "app.log"),
                              maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    _fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    _fh.setLevel(logging.INFO)
    logging.getLogger().addHandler(_fh)
    log.info("File logging enabled: %s", os.path.join(_log_dir, "app.log"))
except Exception as _e:
    log.warning("Could not attach file log handler: %s", _e)

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
MAX_DAILY_LOSS              = float(os.environ.get("MAX_DAILY_LOSS", "0"))
MAX_POSITION_LOSS           = float(os.environ.get("MAX_POSITION_LOSS", "0"))       # dollar stop — Paper All only
MAX_POSITION_LOSS_PCT       = float(os.environ.get("MAX_POSITION_LOSS_PCT", "0"))   # % stop — Refined only
MAX_POSITION_LOSS_REFINED   = float(os.environ.get("MAX_POSITION_LOSS_REFINED", "0"))  # dollar cap — Refined only, fires alongside %
MAX_TRAILING_GIVEBACK       = float(os.environ.get("MAX_TRAILING_GIVEBACK", "0"))
MORNING_TRAIL_PCT           = float(os.environ.get("MORNING_TRAIL_PCT", "0"))       # overrides trail_offset 9:30–10:30 ET when > 0
AFTERNOON_TRAIL_PCT         = float(os.environ.get("AFTERNOON_TRAIL_PCT", "0"))     # caps trail_offset 12:00–close ET when > 0
SIGNAL_COOLDOWN_SECS  = int(os.environ.get("SIGNAL_COOLDOWN_SECS", "10"))
MIN_BUYING_POWER      = float(os.environ.get("MIN_BUYING_POWER", "0"))  # block new entries below this

_risk_halted          = False   # True when daily loss limit is breached
_last_signal_ts       = {}      # {(strategy, ticker, action): unix timestamp}
_blocked_strategies   = {}      # {strategy: {reason, symbol, loss, ts, broker}}
_auto_closed_symbols  = set()   # {(broker, SYMBOL)} — already auto-closed today; keyed per-account so same ticker on alpaca + alpaca2 trips independently
_position_peaks       = {}      # {SYMBOL: peak_unrealized_pnl}; cleared on close
_latest_positions     = []      # cached by position monitor for the status endpoint
_max_hold_positions   = {}      # {(broker_tag, SYMBOL): {entry_time, max_hold_mins}}
_max_hold_fail_ticks  = {}      # {(broker_tag, SYMBOL): consecutive_fail_count}
_risk_lock            = threading.Lock()

_IB_ENABLED = os.environ.get("IB_ENABLED", "0") == "1"

if _IB_ENABLED and os.environ.get("IB_HOST"):
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


# ---------------------------------------------------------------------------
# Live IB broker (optional — set IB_HOST_LIVE to enable)
# ---------------------------------------------------------------------------

if _IB_ENABLED and os.environ.get("IB_HOST_LIVE"):
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
_alpaca_fills_cache    = {"data": [], "ts": 0.0}
_alpaca_fills_lock     = threading.Lock()
_alpaca_analysis_cache = {}   # key → {"data": ..., "ts": float}
_alpaca2_fills_cache   = {"data": [], "ts": 0.0}
_alpaca2_fills_lock    = threading.Lock()
_alpaca2_analysis_cache = {}
ALPACA_CACHE_TTL    = 120  # seconds — paginated fetch can be slow, cache longer
ALPACA_ANALYSIS_TTL =  60  # seconds — analysis computation (LIFO pairing) is also expensive
_broker_status_cache = {"data": None, "ts": 0.0}
BROKER_STATUS_TTL = 30  # seconds — broker connectivity rarely flips that fast

_alpaca_positions_cache = {"data": None, "ts": 0.0}
ALPACA_POSITIONS_TTL = 15  # seconds — live P&L dashboard polls every 10s; cache prevents thundering herd


def _get_cached_fills():
    """Return Alpaca fills from the shared cache, fetching only when stale.
    Lock prevents concurrent duplicate fetches (thundering herd on startup)."""
    global _alpaca_fills_cache
    now = time.time()
    if now - _alpaca_fills_cache["ts"] < ALPACA_CACHE_TTL:
        return _alpaca_fills_cache["data"]
    with _alpaca_fills_lock:
        # Re-check inside lock — another thread may have populated while we waited
        if time.time() - _alpaca_fills_cache["ts"] < ALPACA_CACHE_TTL:
            return _alpaca_fills_cache["data"]
        fills = alpaca_broker.get_fills()
        _alpaca_fills_cache = {"data": fills, "ts": time.time()}
    return _alpaca_fills_cache["data"]
def _get_cached_fills_2():
    """Return Alpaca Refined (account 2) fills, cached separately from account 1."""
    global _alpaca2_fills_cache
    now = time.time()
    if now - _alpaca2_fills_cache["ts"] < ALPACA_CACHE_TTL:
        return _alpaca2_fills_cache["data"]
    with _alpaca2_fills_lock:
        if time.time() - _alpaca2_fills_cache["ts"] < ALPACA_CACHE_TTL:
            return _alpaca2_fills_cache["data"]
        fills = alpaca_broker2.get_fills() if alpaca_broker2 else []
        _alpaca2_fills_cache = {"data": fills, "ts": time.time()}
    return _alpaca2_fills_cache["data"]

# TODO(multi-account): currently hard-coded to 2 Alpaca accounts (primary + Refined).
# To support N accounts (ALPACA_KEY3, KEY4, ...) without manual edits each time:
#   1. Replace alpaca_broker / alpaca_broker2 globals with alpaca_brokers = {"1": ..., "2": ...}
#      populated by scanning ALPACA_KEY, ALPACA_KEY<N> env vars.
#   2. Centralise the broker-target lookup: routes/webhook.py:_resolve_alpaca_broker should
#      key off the trailing digit in target names (alpaca-paper-3, alpaca-live-3, ...).
#   3. Update every iteration site to loop the registry instead of naming each broker:
#        - _check_position_stops (risk monitor)
#        - _check_exit_params_recovery (partial-fill watchdog)
#        - /api/alpaca/account, /api/alpaca/analysis endpoints (?account=N)
#        - fills cache (_alpaca_fills_cache, _alpaca2_fills_cache → keyed dict)
#   4. UI dropdowns instead of fixed Paper All / Paper Refined tabs (templates/index.html,
#      analysis.html, routing.html).
# Bounded ~half-day refactor; defer until a 3rd account is actually needed.
if os.environ.get("ALPACA_KEY"):
    from brokers.alpaca_broker import AlpacaBroker
    alpaca_broker = AlpacaBroker()
    log.info("Alpaca broker initialised (paper=%s)", os.environ.get("ALPACA_PAPER", "true"))

alpaca_broker2 = None
if os.environ.get("ALPACA_KEY2"):
    from brokers.alpaca_broker import AlpacaBroker as _AB2
    _paper2 = os.environ.get("ALPACA_PAPER2", "true").lower() != "false"
    alpaca_broker2 = _AB2(
        key    = os.environ.get("ALPACA_KEY2"),
        secret = os.environ.get("ALPACA_SECRET2"),
        paper  = _paper2,
    )
    log.info("Alpaca broker 2 initialised (paper=%s)", _paper2)

    def _prewarm_fills():
        """Populate the fills cache in background so the first page load is instant."""
        time.sleep(3)   # let gunicorn finish binding before making API calls
        try:
            _get_cached_fills()
            log.info("Fills cache pre-warmed (%d fills)", len(_alpaca_fills_cache["data"]))
        except Exception as _e:
            log.warning("Fills cache pre-warm failed: %s", _e)

    threading.Thread(target=_prewarm_fills, daemon=True).start()

# ---------------------------------------------------------------------------
# Coinbase broker (optional — set COINBASE_KEY + COINBASE_SECRET to enable)
# ---------------------------------------------------------------------------

coinbase_broker = None
if os.environ.get("COINBASE_KEY"):
    from brokers.coinbase_broker import CoinbaseBroker
    coinbase_broker = CoinbaseBroker()
    log.info("Coinbase broker initialised")

# ---------------------------------------------------------------------------
# EOD close scheduler — closes all broker positions at 3:58 PM ET on weekdays.
# Runs regardless of which brokers are configured (IB, Alpaca, Coinbase).
# Fires any time in the 3:58–4:00 PM window so a mid-window restart still
# catches the close.
# ---------------------------------------------------------------------------

def _eod_close_scheduler():
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
            # Alpaca account 1 (Paper All)
            if alpaca_broker is not None:
                try:
                    result = alpaca_broker.close_all_positions()
                    log.info("EOD close Alpaca: %s", result)
                except Exception as e:
                    log.error("EOD close Alpaca failed: %s", e)
            # Alpaca account 2 (Refined) — separate close call since each account
            # holds its own positions. Missing this leaves Refined shorts open overnight.
            if alpaca_broker2 is not None:
                try:
                    result = alpaca_broker2.close_all_positions()
                    log.info("EOD close Alpaca Refined: %s", result)
                except Exception as e:
                    log.error("EOD close Alpaca Refined failed: %s", e)
            # Coinbase
            if coinbase_broker is not None:
                try:
                    result = coinbase_broker.close_all_positions()
                    log.info("EOD close Coinbase: %s", result)
                except Exception as e:
                    log.error("EOD close Coinbase failed: %s", e)
            # IB (must run on the background IB thread)
            if _ib_task_queue is not None and ib_broker is not None:
                try:
                    result = _submit_ib_task(ib_broker.close_all_positions, _timeout=60)
                    log.info("EOD close IB paper: %s", result)
                except Exception as e:
                    log.error("EOD close IB paper failed: %s", e)
            if _ib_live_task_queue is not None and ib_broker_live is not None:
                try:
                    result = _submit_ib_live_task(ib_broker_live.close_all_positions, _timeout=60)
                    log.info("EOD close IB live: %s", result)
                except Exception as e:
                    log.error("EOD close IB live failed: %s", e)
        time.sleep(30)

threading.Thread(target=_eod_close_scheduler, daemon=True).start()

# ---------------------------------------------------------------------------
# Risk monitor — polls P&L every 60s; halts + liquidates on MAX_DAILY_LOSS
# ---------------------------------------------------------------------------

_daily_pnl_cache = {"value": None, "ts": 0.0}


# ---------------------------------------------------------------------------
# Action normalization
#
# The webhook stores the *raw* action string from TradingView. Older alerts
# only ever sent BUY / SELL (because {{strategy.order.action}} could not
# distinguish entries from exits). Pine scripts that build their own JSON in
# alert_message now emit LONG / SHORT / EXIT_LONG / EXIT_SHORT directly.
#
# For any code that only needs to know "did money go in or out of a long
# position", these collapse to two equivalence classes:
#
#   BUY-side  : BUY  | LONG  | EXIT_SHORT  → opens a long  (or closes a short)
#   SELL-side : SELL | SHORT | EXIT_LONG   → opens a short (or closes a long)
# ---------------------------------------------------------------------------
_BUY_ALIASES  = frozenset({"BUY",  "LONG",  "EXIT_SHORT"})
_SELL_ALIASES = frozenset({"SELL", "SHORT", "EXIT_LONG"})


def _canonical_action(action):
    """Collapse LONG/EXIT_SHORT → BUY and SHORT/EXIT_LONG → SELL.
    Returns the original (uppercased) string for unknown values so callers
    that compare against custom tokens still work."""
    a = (action or "").strip().upper()
    if a in _BUY_ALIASES:
        return "BUY"
    if a in _SELL_ALIASES:
        return "SELL"
    return a


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
            total = alpaca_broker.daily_pnl()
            if alpaca_broker2 is not None:
                try:
                    total += alpaca_broker2.daily_pnl()
                except Exception as _e2:
                    log.debug("_compute_daily_pnl acct2 error: %s", _e2)
            result = round(total, 2)
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
            action   = _canonical_action(t.get("action"))
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
                queue = open_shorts.setdefault(key, [])
                while qty > 0 and queue:
                    entry_price, entry_qty = queue.pop(0)
                    m = min(qty, entry_qty)
                    if is_today:
                        today_pnl += (entry_price - price) * m
                    qty -= m
                    if entry_qty > m:
                        queue.insert(0, (entry_price, entry_qty - m))
                if qty > 0:
                    open_longs.setdefault(key, []).append((price, qty))
            elif action in ("SELL", "SHORT", "EXIT_LONG"):
                queue = open_longs.setdefault(key, [])
                while qty > 0 and queue:
                    entry_price, entry_qty = queue.pop(0)
                    m = min(qty, entry_qty)
                    if is_today:
                        today_pnl += (price - entry_price) * m
                    qty -= m
                    if entry_qty > m:
                        queue.insert(0, (entry_price, entry_qty - m))
                if qty > 0:
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
                            (alpaca_broker2,  "Alpaca Refined"),
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


def _check_position_stops():
    """Check all open positions against MAX_POSITION_LOSS and MAX_TRAILING_GIVEBACK.
    Closes offending positions; the originating strategy stays free to re-enter.
    Trailing stop arms once a position's peak unrealized P&L reaches the giveback
    amount, then closes if the position gives back that amount from peak."""
    global _latest_positions
    all_positions = []
    polled_brokers = set()   # only brokers we fetched cleanly; used to gate stale cleanup

    if alpaca_broker:
        try:
            # Bypass the position cache here — risk checks need fresh data.
            alpaca_broker._invalidate_pos_cache()
            for p in alpaca_broker.get_positions():
                p["broker"] = "alpaca"
                all_positions.append(p)
            polled_brokers.add("alpaca")
        except Exception as _e:
            log.warning("Position stop: Alpaca get_positions failed: %s", _e)

    if alpaca_broker2:
        try:
            alpaca_broker2._invalidate_pos_cache()
            for p in alpaca_broker2.get_positions():
                p["broker"] = "alpaca2"
                all_positions.append(p)
            polled_brokers.add("alpaca2")
        except Exception as _e:
            log.warning("Position stop: Alpaca account 2 get_positions failed: %s", _e)

    if _ib_task_queue is not None and ib_broker:
        try:
            for p in _submit_ib_task(ib_broker.get_positions, _timeout=15):
                p["broker"] = "ib"
                all_positions.append(p)
            polled_brokers.add("ib")
        except Exception as _e:
            log.debug("Position stop: IB get_positions failed: %s", _e)

    if _ib_live_task_queue is not None and ib_broker_live:
        try:
            for p in _submit_ib_live_task(ib_broker_live.get_positions, _timeout=15):
                p["broker"] = "ib-live"
                all_positions.append(p)
            polled_brokers.add("ib-live")
        except Exception as _e:
            log.debug("Position stop: IB Live get_positions failed: %s", _e)

    with _risk_lock:
        _latest_positions = all_positions

    # Clear _auto_closed_symbols + peak tracker for any symbol no longer open.
    # This allows the monitor to protect new entries in the same symbol later in the session.
    # Only clear state for brokers we polled cleanly this tick. A transient
    # get_positions() failure must NOT make open positions look closed — doing
    # so would wipe their max-hold timers (in memory + DB) and they'd never exit.
    open_keys    = {(p["broker"], p["symbol"].upper()) for p in all_positions}
    open_symbols = {sym for _, sym in open_keys}
    with _risk_lock:
        stale = {k for k in _auto_closed_symbols if k[0] in polled_brokers and k not in open_keys}
        _auto_closed_symbols.difference_update(stale)
        stale_peaks = [s for s in _position_peaks if s not in open_symbols]
        for s in stale_peaks:
            _position_peaks.pop(s, None)
        stale_holds = [k for k in _max_hold_positions if k[0] in polled_brokers and k not in open_keys]
        for k in stale_holds:
            _max_hold_positions.pop(k, None)
            _clear_max_hold_db(k[0], k[1])
    if stale:
        log.info("Position stop: cleared auto-close guard for %s (no longer open)", stale)

    for pos in all_positions:
        upnl   = float(pos.get("unrealized_pnl") or 0)
        symbol = pos["symbol"]
        broker = pos["broker"]
        sym_u  = symbol.upper()

        # Update peak tracker (always, so peak persists across polls when trailing is on).
        peak = 0.0
        if MAX_TRAILING_GIVEBACK > 0:
            with _risk_lock:
                prev = _position_peaks.get(sym_u, 0.0)
                if upnl > prev:
                    _position_peaks[sym_u] = upnl
                peak = max(prev, upnl)

        # Decide which (if any) stop to fire.
        # PCT stop applies to Refined (alpaca2) only — Paper All uses dollar stop or TV exits.
        triggered = None
        if broker == "alpaca2":
            mkt_val = abs(float(pos.get("market_value") or 0))
            loss_pct = (upnl / mkt_val * 100) if mkt_val > 0 else 0.0
            if MAX_POSITION_LOSS_PCT < 0 and loss_pct <= MAX_POSITION_LOSS_PCT:
                triggered = ("fixed-loss-pct",
                             f"unrealized {loss_pct:.2f}% (${upnl:.2f}) hit % limit {MAX_POSITION_LOSS_PCT:.2f}%")
            elif MAX_POSITION_LOSS_REFINED < 0 and upnl <= MAX_POSITION_LOSS_REFINED:
                triggered = ("fixed-loss-refined",
                             f"unrealized ${upnl:.2f} hit Refined dollar cap ${MAX_POSITION_LOSS_REFINED:.2f}")
        elif MAX_POSITION_LOSS < 0 and broker == "alpaca" and upnl <= MAX_POSITION_LOSS:
            triggered = ("fixed-loss",
                         f"unrealized P&L ${upnl:.2f} hit fixed limit ${MAX_POSITION_LOSS:.2f}")
        elif MAX_TRAILING_GIVEBACK > 0 \
                and peak >= MAX_TRAILING_GIVEBACK \
                and (peak - upnl) >= MAX_TRAILING_GIVEBACK:
            triggered = ("trailing",
                         f"unrealized P&L ${upnl:.2f} gave back ${peak - upnl:.2f} from peak ${peak:.2f} (trail ${MAX_TRAILING_GIVEBACK:.2f})")

        if not triggered:
            continue

        with _risk_lock:
            if (broker, sym_u) in _auto_closed_symbols:
                continue
            # Don't add to _auto_closed_symbols yet — only add after a successful close
            # so that a failed close is retried on the next poll rather than silently dropped.

        log.error("POSITION STOP (%s): %s [%s] — %s — closing position",
                  triggered[0], symbol, broker, triggered[1])

        # Close the position — only mark as handled if the order is successfully submitted
        close_ok = False
        try:
            if broker == "alpaca":
                res = alpaca_broker.close_position(symbol)
                close_ok = res.get("success", False)
                if not close_ok:
                    log.error("Position stop close failed for %s: %s", symbol, res.get("error"))
            elif broker == "alpaca2":
                res = alpaca_broker2.close_position(symbol)
                close_ok = res.get("success", False)
                if not close_ok:
                    log.error("Position stop close failed for %s (acct2): %s", symbol, res.get("error"))
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
                _auto_closed_symbols.add((broker, sym_u))
        else:
            # Close failed — leave symbol out of _auto_closed_symbols so it retries next poll
            log.warning("Position stop: close order for %s failed — will retry next poll", symbol)


def _check_exit_params_recovery():
    """If an exit_params trailing/hard stop triggered but only partially filled
    (e.g. fast-moving market, thin book at the trigger price), the remainder
    is stranded long/short with no covering exit order. Detect that and flatten."""
    import datetime as _dt
    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
    except Exception:
        return  # SDK unavailable

    accounts = []
    if alpaca_broker:  accounts.append(("alpaca",  alpaca_broker))
    if alpaca_broker2: accounts.append(("alpaca2", alpaca_broker2))

    for broker_tag, broker_inst in accounts:
        try:
            broker_inst._invalidate_pos_cache()
            positions = [p for p in broker_inst.get_positions()
                         if abs(float(p.get("qty") or 0)) > 0]
        except Exception as _e:
            log.debug("Exit recovery: get_positions failed on %s: %s", broker_tag, _e)
            continue
        if not positions:
            continue

        open_symbols = [p["symbol"].upper() for p in positions]
        try:
            broker_inst._ensure_client()
            req = GetOrdersRequest(
                status = QueryOrderStatus.CLOSED,
                after  = _dt.datetime.utcnow() - _dt.timedelta(hours=24),
                limit  = 200,
                symbols= open_symbols,
            )
            orders = broker_inst._trading.get_orders(filter=req)
        except Exception as _e:
            log.debug("Exit recovery: get_orders failed on %s: %s", broker_tag, _e)
            continue

        # Map symbol → True if any kairos-trail/hard order partially filled in last 24h
        partial = {}
        for o in orders:
            coid = str(getattr(o, "client_order_id", "") or "")
            if not (coid.startswith("kairos-trail-") or coid.startswith("kairos-hard-")):
                continue
            try:
                req_qty = float(getattr(o, "qty", 0) or 0)
                filled  = float(getattr(o, "filled_qty", 0) or 0)
            except (TypeError, ValueError):
                continue
            if filled > 0 and filled < req_qty:
                partial[o.symbol.upper()] = True

        for pos in positions:
            sym  = pos["symbol"].upper()
            if not partial.get(sym):
                continue
            with _risk_lock:
                if (broker_tag, sym) in _auto_closed_symbols:
                    continue
            log.error("EXIT-PARAMS RECOVERY: %s on %s — stop fired with partial fill, "
                      "flattening remainder (%s shares)",
                      sym, broker_tag, pos.get("qty"))
            try:
                res = broker_inst.close_position(sym)
                if res.get("success"):
                    with _risk_lock:
                        _auto_closed_symbols.add((broker_tag, sym))
                    log.info("Exit recovery: %s flatten order submitted on %s", sym, broker_tag)
                else:
                    log.error("Exit recovery: close failed for %s on %s: %s",
                              sym, broker_tag, res.get("error"))
            except Exception as _e:
                log.error("Exit recovery: close_position raised for %s on %s: %s",
                          sym, broker_tag, _e)


def _check_max_hold_exits():
    """Close positions that have exceeded their max hold time (set via exit_params node)."""
    with _risk_lock:
        snapshot = dict(_max_hold_positions)
    if not snapshot:
        return

    now_utc = datetime.now(timezone.utc)
    for (broker_tag, symbol), info in snapshot.items():
        elapsed_mins = (now_utc - info["entry_time"]).total_seconds() / 60
        if elapsed_mins < info["max_hold_mins"]:
            continue

        with _risk_lock:
            if (broker_tag, symbol) in _auto_closed_symbols:
                continue

        broker_inst = alpaca_broker2 if broker_tag == "alpaca2" else alpaca_broker
        if broker_inst is None:
            continue

        try:
            broker_inst._invalidate_pos_cache()
            positions = broker_inst.get_positions()
            still_open = any(
                p["symbol"].upper() == symbol and abs(float(p.get("qty") or 0)) > 0
                for p in positions
            )
        except Exception as _e:
            log.warning("Max hold check: get_positions failed for %s [%s]: %s", symbol, broker_tag, _e)
            continue

        if not still_open:
            with _risk_lock:
                _max_hold_positions.pop((broker_tag, symbol), None)
            _clear_max_hold_db(broker_tag, symbol)
            continue

        # Throttle retries: skip every other tick after first failure, then
        # every 5 ticks (15s) after 3 consecutive failures.
        fail_count = _max_hold_fail_ticks.get((broker_tag, symbol), 0)
        if fail_count == 1 or (fail_count >= 3 and fail_count % 5 != 0):
            _max_hold_fail_ticks[(broker_tag, symbol)] = fail_count + 1
            continue

        log.info("MAX HOLD: %s [%s] — %.1f min elapsed (limit %.0f min) — closing",
                 symbol, broker_tag, elapsed_mins, info["max_hold_mins"])
        try:
            res = broker_inst.close_position(symbol)
            if res.get("success"):
                with _risk_lock:
                    _auto_closed_symbols.add((broker_tag, symbol))
                    _max_hold_positions.pop((broker_tag, symbol), None)
                _max_hold_fail_ticks.pop((broker_tag, symbol), None)
                _clear_max_hold_db(broker_tag, symbol)
                log.info("Max hold: close order submitted for %s on %s", symbol, broker_tag)
            else:
                _max_hold_fail_ticks[(broker_tag, symbol)] = fail_count + 1
                log.error("Max hold: close failed for %s [%s]: %s", symbol, broker_tag, res.get("error"))
        except Exception as _e:
            _max_hold_fail_ticks[(broker_tag, symbol)] = fail_count + 1
            log.error("Max hold: close_position raised for %s [%s]: %s", symbol, broker_tag, _e)


_exit_recovery_tick = 0


def _position_monitor_loop():
    """Background thread: poll positions every 3s, fire fixed-loss or trailing-giveback stops.
    Every ~15s, also run the exit-params partial-fill recovery watchdog."""
    global _exit_recovery_tick
    time.sleep(25)  # stagger from risk monitor; also gives brokers time to init before recovery
    try:
        _recover_max_hold_positions()
    except Exception as _e:
        log.warning("Max hold recovery error on startup: %s", _e)
    while True:
        if MAX_POSITION_LOSS < 0 or MAX_POSITION_LOSS_PCT < 0 or MAX_POSITION_LOSS_REFINED < 0 or MAX_TRAILING_GIVEBACK > 0:
            try:
                _check_position_stops()
            except Exception as _e:
                log.warning("Position monitor error: %s", _e)
        if _max_hold_positions:
            try:
                _check_max_hold_exits()
            except Exception as _e:
                log.warning("Max hold monitor error: %s", _e)
        _exit_recovery_tick += 1
        if _exit_recovery_tick >= 5:  # every 5 ticks * 3s = 15s
            _exit_recovery_tick = 0
            try:
                _check_exit_params_recovery()
            except Exception as _e:
                log.warning("Exit-params recovery error: %s", _e)
        time.sleep(3)


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
    """Filter a DataFrame with naive-UTC DatetimeIndex to RTH bars only (9:30-16:00 ET).

    Returns a DataFrame whose index is naive ET local time — this matters because
    downstream Strategy classes (including AI-generated ones) use `idx.hour` and
    `idx.minute` to build their own RTH masks. If the naive index still held UTC
    values the Strategy would filter against 13:30-20:00 UTC, which drops ET
    afternoon bars and halves the trade count.
    """
    import pandas as _pd
    try:
        idx_et = df.index.tz_localize("UTC").tz_convert("America/New_York")
        rth = ((idx_et.hour > 9) | ((idx_et.hour == 9) & (idx_et.minute >= 30))) & (idx_et.hour < 16)
        df = df[rth].copy()
        # Strip tz after converting to ET so hour/minute reads match ET local time.
        df.index = idx_et[rth].tz_localize(None)
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

    try:
        cur.execute("ALTER TABLE routing_rules ADD COLUMN tv_alert_created INTEGER DEFAULT 0")
        conn.commit()
    except Exception:
        conn.rollback()

    # Add pine_code column to user_strategies if not present (migration)
    try:
        cur.execute("ALTER TABLE user_strategies ADD COLUMN pine_code TEXT")
        conn.commit()
    except Exception:
        conn.rollback()

    # Add tags column to journal_entries if not present
    try:
        cur.execute("ALTER TABLE journal_entries ADD COLUMN tags TEXT")
        conn.commit()
    except Exception:
        conn.rollback()

    # Migration: update trading_hours end time from 16:00 → 15:55 in all pipelines
    try:
        p = placeholder()
        cur.execute("SELECT id, nodes FROM routing_rules")
        _rr_rows = cur.fetchall()
        _patched = 0
        for _rr in _rr_rows:
            _rr_id   = _rr[0] if DATABASE_URL else _rr["id"]
            _nodes_r = _rr[1] if DATABASE_URL else _rr["nodes"]
            try:
                _nodes = json.loads(_nodes_r) if isinstance(_nodes_r, str) else (_nodes_r or [])
            except Exception:
                continue
            _changed = False
            for _n in _nodes:
                if _n.get("type") == "trading_hours" and _n.get("end") == "16:00":
                    _n["end"] = "15:55"
                    _changed = True
            if _changed:
                cur.execute(
                    f"UPDATE routing_rules SET nodes={p} WHERE id={p}",
                    (json.dumps(_nodes), _rr_id),
                )
                _patched += 1
        if _patched:
            conn.commit()
            log.info("Migration: patched trading_hours end → 15:55 in %d pipeline(s)", _patched)
    except Exception as _e:
        conn.rollback()
        log.warning("Migration: trading_hours patch failed: %s", _e)

    # App settings table (persists risk limits across restarts)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()

    # Refined snapshot history — one row per (run_at, strategy_name).
    # Used to compute the added/removed breakdown and per-strategy tenure
    # (consecutive runs a strategy has been in the top-N).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS refined_history (
            run_at        TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            PRIMARY KEY (run_at, strategy_name)
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

    # Journal entries table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS journal_entries (
            id           SERIAL PRIMARY KEY,
            week         TEXT UNIQUE,
            generated_at TEXT,
            trade_stats  TEXT,
            market_data  TEXT,
            ai_summary   TEXT,
            user_notes   TEXT
        )
    """ if DATABASE_URL else """
        CREATE TABLE IF NOT EXISTS journal_entries (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            week         TEXT UNIQUE,
            generated_at TEXT,
            trade_stats  TEXT,
            market_data  TEXT,
            ai_summary   TEXT,
            user_notes   TEXT
        )
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS position_timers (
            broker_tag    TEXT NOT NULL,
            symbol        TEXT NOT NULL,
            entry_time    TEXT NOT NULL,
            max_hold_mins REAL NOT NULL,
            PRIMARY KEY (broker_tag, symbol)
        )
    """)
    conn.commit()

    conn.close()


def _persist_max_hold(broker_tag: str, symbol: str, entry_time, max_hold_mins: float):
    """Upsert a max-hold timer to the DB so it survives process restarts."""
    p = placeholder()
    try:
        conn = get_db()
        cur  = conn.cursor()
        entry_iso = entry_time.isoformat()
        if DATABASE_URL:
            cur.execute(
                f"INSERT INTO position_timers (broker_tag, symbol, entry_time, max_hold_mins) "
                f"VALUES ({p},{p},{p},{p}) "
                f"ON CONFLICT (broker_tag, symbol) DO UPDATE SET "
                f"entry_time=EXCLUDED.entry_time, max_hold_mins=EXCLUDED.max_hold_mins",
                (broker_tag, symbol, entry_iso, max_hold_mins),
            )
        else:
            cur.execute(
                f"INSERT OR REPLACE INTO position_timers (broker_tag, symbol, entry_time, max_hold_mins) "
                f"VALUES ({p},{p},{p},{p})",
                (broker_tag, symbol, entry_iso, max_hold_mins),
            )
        conn.commit()
        conn.close()
    except Exception as _e:
        log.warning("_persist_max_hold failed for %s [%s]: %s", symbol, broker_tag, _e)


def _clear_max_hold_db(broker_tag: str, symbol: str):
    """Remove a max-hold timer from the DB."""
    p = placeholder()
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(f"DELETE FROM position_timers WHERE broker_tag={p} AND symbol={p}",
                    (broker_tag, symbol))
        conn.commit()
        conn.close()
    except Exception as _e:
        log.warning("_clear_max_hold_db failed for %s [%s]: %s", symbol, broker_tag, _e)


def _recover_max_hold_positions():
    """Reload max-hold timers on startup.

    Two passes:
    1. Restore from position_timers DB (handles clean restarts).
    2. Scan all open Alpaca positions not yet tracked and cross-reference
       against routing rules + recent trades — catches positions entered
       during a deploy window before the DB write was in place.
    """
    p = placeholder()

    # ── Pass 1: restore from DB ───────────────────────────────────────────
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT broker_tag, symbol, entry_time, max_hold_mins FROM position_timers")
        db_rows = cur.fetchall()
        conn.close()
    except Exception as _e:
        log.warning("Max hold recovery: DB query failed: %s", _e)
        db_rows = []

    # ── Fetch open Alpaca positions ───────────────────────────────────────
    open_positions = {}   # (broker_tag, symbol) -> position dict
    failed_brokers = set()
    for _tag, _inst in [("alpaca", alpaca_broker), ("alpaca2", alpaca_broker2)]:
        if _inst is None:
            continue
        try:
            for _pos in _inst.get_positions():
                if abs(float(_pos.get("qty") or 0)) > 0:
                    open_positions[(_tag, _pos["symbol"].upper())] = _pos
        except Exception as _e:
            log.warning("Max hold recovery: get_positions failed [%s]: %s", _tag, _e)
            failed_brokers.add(_tag)

    open_keys = set(open_positions.keys())
    recovered = 0

    for row in db_rows:
        broker_tag, symbol, entry_iso, max_hold_mins = row[0], row[1], row[2], float(row[3])
        key = (broker_tag, symbol.upper())
        if broker_tag in failed_brokers:
            continue  # can't verify — leave DB row for next startup
        if key not in open_keys:
            _clear_max_hold_db(broker_tag, symbol)
            continue
        try:
            entry_time = datetime.fromisoformat(entry_iso)
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=timezone.utc)
        except Exception:
            _clear_max_hold_db(broker_tag, symbol)
            continue
        with _risk_lock:
            _max_hold_positions[key] = {"entry_time": entry_time, "max_hold_mins": max_hold_mins}
        recovered += 1
        log.info("Max hold recovered (DB): %s [%s] — entry %s, limit %.0f min",
                 symbol, broker_tag, entry_iso, max_hold_mins)

    # ── Pass 2: pick up untracked open positions (deploy-window entries) ──
    already_tracked = set(_max_hold_positions.keys())
    untracked = {k: v for k, v in open_positions.items()
                 if k not in already_tracked and k[0] not in failed_brokers}
    if not untracked:
        if recovered:
            log.info("Max hold recovery: %d position(s) restored", recovered)
        return

    # Load routing rules that have max_hold_mins configured
    rule_max_hold = {}  # strategy_name_upper -> max_hold_mins
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT name, nodes FROM routing_rules WHERE enabled=1")
        for rrow in cur.fetchall():
            rname = (rrow[0] or "").upper()
            try:
                nodes = json.loads(rrow[1] or "[]")
            except Exception:
                continue
            for nd in nodes:
                if nd.get("type") == "exit_params":
                    mhm = nd.get("max_hold_mins")
                    if mhm:
                        rule_max_hold[rname] = float(mhm)
                    break
        conn.close()
    except Exception as _e:
        log.warning("Max hold recovery: routing rules query failed: %s", _e)

    if not rule_max_hold:
        if recovered:
            log.info("Max hold recovery: %d position(s) restored", recovered)
        return

    for (broker_tag, symbol) in untracked:
        # Find the most recent filled entry trade for this symbol today
        try:
            conn = get_db()
            cur  = conn.cursor()
            cur.execute(
                f"SELECT strategy, received_at FROM trades "
                f"WHERE ticker={p} AND exec_status='filled' "
                f"AND action NOT IN ('EXIT_LONG','EXIT_SHORT','EXIT') AND sentiment!='flat' "
                f"ORDER BY id DESC LIMIT 10",
                (symbol,),
            )
            trade_rows = cur.fetchall()
            conn.close()
        except Exception as _e:
            log.warning("Max hold recovery: trades query failed for %s: %s", symbol, _e)
            continue

        for trow in trade_rows:
            strategy   = (trow[0] or "").upper()
            received_at = trow[1] or ""
            # Match strategy against rule_max_hold
            mhm = rule_max_hold.get(strategy)
            if mhm is None:
                sname = strategy.replace(" ", "_")
                for rkey, rval in rule_max_hold.items():
                    if "_CAM_" in rkey:
                        parts   = rkey.split("_CAM_")[1].split("_")
                        pattern = "_".join(parts[:2]) if len(parts) >= 2 else rkey
                    else:
                        pattern = rkey
                    if pattern and pattern in sname:
                        mhm = rval
                        break
            if mhm is None:
                continue
            try:
                entry_time = datetime.fromisoformat(received_at.replace(" ", "T"))
                if entry_time.tzinfo is None:
                    entry_time = entry_time.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            key = (broker_tag, symbol)
            with _risk_lock:
                _max_hold_positions[key] = {"entry_time": entry_time, "max_hold_mins": mhm}
            _persist_max_hold(broker_tag, symbol, entry_time, mhm)
            recovered += 1
            log.info("Max hold recovered (open pos): %s [%s] — entry %s, limit %.0f min",
                     symbol, broker_tag, received_at, mhm)
            break  # found match for this symbol

    if recovered:
        log.info("Max hold recovery: %d position(s) restored total", recovered)


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
    global _broker_status_cache
    now = time.time()
    if _broker_status_cache["data"] is not None and (now - _broker_status_cache["ts"]) < BROKER_STATUS_TTL:
        return jsonify(_broker_status_cache["data"])

    # Run each broker status check in a thread with a hard timeout so a
    # hung broker (e.g. Coinbase SSL stall) never blocks the dashboard.
    import concurrent.futures as _cf
    brokers = {}
    tasks = {}
    with _cf.ThreadPoolExecutor(max_workers=4) as ex:
        if ib_broker:
            tasks["IB"] = ex.submit(ib_broker.status)
        else:
            brokers["IB"] = {
                "connected": False, "broker": "IB",
                "note": "IB disabled (set IB_ENABLED=1 to enable)" if not _IB_ENABLED else "IB_HOST not set",
            }
        if ib_broker_live is not None:
            tasks["IB_LIVE"] = ex.submit(ib_broker_live.status)
        if alpaca_broker is not None:
            tasks["Alpaca"] = ex.submit(alpaca_broker.status)
        if coinbase_broker is not None:
            tasks["Coinbase"] = ex.submit(coinbase_broker.status)

        for name, fut in tasks.items():
            try:
                result = fut.result(timeout=10)
                if name == "IB_LIVE":
                    result["mode"] = "live"
                brokers[name] = result
            except _cf.TimeoutError:
                brokers[name] = {"broker": name, "connected": False, "error": "status check timed out"}
            except Exception as e:
                brokers[name] = {"broker": name, "connected": False, "error": str(e)}

    _broker_status_cache = {"data": brokers, "ts": now}
    return jsonify(brokers)


@app.route("/api/alpaca/account")
def api_alpaca_account():
    """Buying power, equity, and open positions — polled by the dashboard.
    Pass ?account=2 to query the Alpaca Refined (second) account."""
    use_acct2 = request.args.get("account") == "2"
    broker = alpaca_broker2 if use_acct2 else alpaca_broker
    if broker is None:
        return jsonify({"error": "Alpaca account 2 not configured" if use_acct2 else "Alpaca not configured"}), 400
    try:
        broker._ensure_client()
        acct      = broker._trading.get_account()
        positions = broker._get_positions_cached()
        pos_list     = []
        total_mv     = 0.0
        total_upnl   = 0.0
        for p in positions:
            mv   = float(p.market_value or 0)
            upnl = float(p.unrealized_pl or 0) if p.unrealized_pl is not None else 0.0
            total_mv   += abs(mv)
            total_upnl += upnl
            pos_list.append({
                "symbol":          p.symbol,
                "qty":             float(p.qty or 0),
                "market_value":    round(mv, 2),
                "unrealized_pnl":  round(upnl, 2),
                "avg_entry_price": round(float(p.avg_entry_price or 0), 2),
                "current_price":   round(float(p.current_price or 0), 2),
                "side":            "long" if float(p.qty or 0) > 0 else "short",
            })
        pos_list.sort(key=lambda x: abs(x["market_value"]), reverse=True)
        bp       = float(acct.buying_power)
        equity   = float(acct.equity)
        # Account 1 uses the cached _compute_daily_pnl() which also feeds the risk monitor.
        # Account 2 calls daily_pnl() directly — no cache, called only when the analysis page loads.
        if use_acct2:
            try:
                daily_pnl = round(broker.daily_pnl(), 2)
            except Exception as _e:
                log.debug("api_alpaca_account: acct2 daily_pnl failed: %s", _e)
                daily_pnl = None
        else:
            daily_pnl = _compute_daily_pnl()
        return jsonify({
            "buying_power":     round(bp, 2),
            "equity":           round(equity, 2),
            "deployed":         round(total_mv, 2),
            "open_positions":   len(pos_list),
            "unrealized_pnl":   round(total_upnl, 2),
            "daily_pnl":        round(daily_pnl, 2) if daily_pnl is not None else None,
            "positions":        pos_list,
            "min_buying_power": MIN_BUYING_POWER,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
    """Return current open Alpaca positions with live unrealized P&L.
    Pass ?account=2 to query the Refined (account 2) broker."""
    account   = request.args.get("account", "1")
    use_acct2 = (account == "2")
    broker    = alpaca_broker2 if use_acct2 else alpaca_broker
    if broker is None:
        return jsonify({"positions": [], "_debug": {"error": "broker not configured"}})
    if not use_acct2:
        global _alpaca_positions_cache
        now = time.time()
        if _alpaca_positions_cache["data"] is not None and (now - _alpaca_positions_cache["ts"]) < ALPACA_POSITIONS_TTL:
            return jsonify(_alpaca_positions_cache["data"])
    try:
        positions = broker.get_positions()
        # Enrich each position with entry_time from the max-hold timer dict so the
        # dashboard can display how long the trade has been live.
        broker_tag = "alpaca2" if use_acct2 else "alpaca"
        with _risk_lock:
            hold_snapshot = dict(_max_hold_positions)
        for pos in positions:
            sym  = (pos.get("symbol") or "").upper()
            info = hold_snapshot.get((broker_tag, sym))
            pos["entry_time"] = info["entry_time"].isoformat() if info else None
        result = {
            "positions": positions,
            "_debug": {"paper": broker._paper, "raw_count": len(positions)},
        }
        if not use_acct2:
            _alpaca_positions_cache = {"data": result, "ts": time.time()}
        return jsonify(result)
    except Exception as e:
        log.error("alpaca_positions failed: %s", e, exc_info=True)
        return jsonify({"positions": [], "_debug": {"error": str(e)}})


def _resolve_position_strategy(symbol, broker):
    """Best-effort lookup of which strategy owns an open position.
    Walks recent fills (most recent first) for the matching account and returns
    the strategy parsed from the first kairos-{strategy}-{ts} client_order_id
    that matches this symbol. Trailing/hard stop orders are skipped so we get
    the entry that opened the position, not the protective stop on top of it."""
    if not symbol:
        return ""
    sym_u = symbol.upper()
    try:
        if broker == "alpaca2":
            fills = _alpaca2_fills_cache["data"]
        elif broker == "alpaca":
            fills = _alpaca_fills_cache["data"]
        else:
            return ""  # IB doesn't tag client_order_id with strategy
        for f in sorted(fills, key=lambda x: x.get("time", ""), reverse=True):
            if (f.get("symbol") or "").upper() != sym_u:
                continue
            oid = f.get("order_id", "") or ""
            if not oid.startswith("kairos-"):
                continue
            if oid.startswith("kairos-trail-") or oid.startswith("kairos-hard-"):
                continue
            parts = oid.split("-", 2)  # ["kairos", strategy, ts]
            if len(parts) == 3 and parts[1]:
                return parts[1]
    except Exception:
        pass
    return ""


@app.route("/api/risk/status")
def risk_status():
    pnl = _compute_daily_pnl()
    with _risk_lock:
        halted     = _risk_halted
        blocked    = dict(_blocked_strategies)
        positions  = [dict(p) for p in _latest_positions]  # copy for mutation
    for p in positions:
        p["strategy"] = _resolve_position_strategy(p.get("symbol", ""), p.get("broker", ""))
    return jsonify({
        "halted":               halted,
        "max_daily_loss":       MAX_DAILY_LOSS if MAX_DAILY_LOSS != 0 else None,
        "current_pnl":          round(pnl, 2) if pnl is not None else None,
        "enabled":              MAX_DAILY_LOSS < 0,
        "max_position_loss":          MAX_POSITION_LOSS if MAX_POSITION_LOSS != 0 else None,
        "max_position_loss_pct":      MAX_POSITION_LOSS_PCT if MAX_POSITION_LOSS_PCT != 0 else None,
        "max_position_loss_refined":  MAX_POSITION_LOSS_REFINED if MAX_POSITION_LOSS_REFINED != 0 else None,
        "position_stop_enabled":      MAX_POSITION_LOSS_PCT < 0 or MAX_POSITION_LOSS < 0 or MAX_POSITION_LOSS_REFINED < 0,
        "position_stop_mode":         "percent" if MAX_POSITION_LOSS_PCT < 0 else "dollars",
        "max_trailing_giveback":   MAX_TRAILING_GIVEBACK if MAX_TRAILING_GIVEBACK != 0 else None,
        "trailing_stop_enabled":   MAX_TRAILING_GIVEBACK > 0,
        "morning_trail_pct":       MORNING_TRAIL_PCT   if MORNING_TRAIL_PCT   > 0 else None,
        "afternoon_trail_pct":     AFTERNOON_TRAIL_PCT if AFTERNOON_TRAIL_PCT > 0 else None,
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
    global MAX_DAILY_LOSS, MAX_POSITION_LOSS, MAX_POSITION_LOSS_PCT, MAX_POSITION_LOSS_REFINED, MAX_TRAILING_GIVEBACK, MORNING_TRAIL_PCT, AFTERNOON_TRAIL_PCT
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
    if "max_position_loss_pct" in data:
        try:
            MAX_POSITION_LOSS_PCT = float(data["max_position_loss_pct"])
        except (TypeError, ValueError):
            return jsonify({"error": "max_position_loss_pct must be a number"}), 400
        _update_env_file("MAX_POSITION_LOSS_PCT", f"{MAX_POSITION_LOSS_PCT:g}")
        _save_setting("MAX_POSITION_LOSS_PCT", f"{MAX_POSITION_LOSS_PCT:g}")
        log.info("MAX_POSITION_LOSS_PCT updated to %g", MAX_POSITION_LOSS_PCT)
        changed.append("max_position_loss_pct")
    if "max_position_loss_refined" in data:
        try:
            MAX_POSITION_LOSS_REFINED = float(data["max_position_loss_refined"])
        except (TypeError, ValueError):
            return jsonify({"error": "max_position_loss_refined must be a number"}), 400
        _update_env_file("MAX_POSITION_LOSS_REFINED", f"{MAX_POSITION_LOSS_REFINED:g}")
        _save_setting("MAX_POSITION_LOSS_REFINED", f"{MAX_POSITION_LOSS_REFINED:g}")
        log.info("MAX_POSITION_LOSS_REFINED updated to %g", MAX_POSITION_LOSS_REFINED)
        changed.append("max_position_loss_refined")
    if "max_trailing_giveback" in data:
        try:
            MAX_TRAILING_GIVEBACK = float(data["max_trailing_giveback"])
        except (TypeError, ValueError):
            return jsonify({"error": "max_trailing_giveback must be a number"}), 400
        if MAX_TRAILING_GIVEBACK < 0:
            MAX_TRAILING_GIVEBACK = abs(MAX_TRAILING_GIVEBACK)  # accept either sign, store positive
        _update_env_file("MAX_TRAILING_GIVEBACK", f"{MAX_TRAILING_GIVEBACK:g}")
        _save_setting("MAX_TRAILING_GIVEBACK", f"{MAX_TRAILING_GIVEBACK:g}")
        log.info("MAX_TRAILING_GIVEBACK updated to %g", MAX_TRAILING_GIVEBACK)
        if MAX_TRAILING_GIVEBACK == 0:
            with _risk_lock:
                _position_peaks.clear()
        changed.append("max_trailing_giveback")
    if "morning_trail_pct" in data:
        try:
            MORNING_TRAIL_PCT = float(data["morning_trail_pct"])
        except (TypeError, ValueError):
            return jsonify({"error": "morning_trail_pct must be a number"}), 400
        if MORNING_TRAIL_PCT < 0:
            MORNING_TRAIL_PCT = 0.0
        _update_env_file("MORNING_TRAIL_PCT", f"{MORNING_TRAIL_PCT:g}")
        _save_setting("MORNING_TRAIL_PCT", f"{MORNING_TRAIL_PCT:g}")
        log.info("MORNING_TRAIL_PCT updated to %g", MORNING_TRAIL_PCT)
        changed.append("morning_trail_pct")
    if "afternoon_trail_pct" in data:
        try:
            AFTERNOON_TRAIL_PCT = float(data["afternoon_trail_pct"])
        except (TypeError, ValueError):
            return jsonify({"error": "afternoon_trail_pct must be a number"}), 400
        if AFTERNOON_TRAIL_PCT < 0:
            AFTERNOON_TRAIL_PCT = 0.0
        _update_env_file("AFTERNOON_TRAIL_PCT", f"{AFTERNOON_TRAIL_PCT:g}")
        _save_setting("AFTERNOON_TRAIL_PCT", f"{AFTERNOON_TRAIL_PCT:g}")
        log.info("AFTERNOON_TRAIL_PCT updated to %g", AFTERNOON_TRAIL_PCT)
        changed.append("afternoon_trail_pct")
    return jsonify({
        "max_daily_loss":         MAX_DAILY_LOSS,
        "max_position_loss":      MAX_POSITION_LOSS,
        "max_trailing_giveback":  MAX_TRAILING_GIVEBACK,
        "morning_trail_pct":      MORNING_TRAIL_PCT,
        "afternoon_trail_pct":    AFTERNOON_TRAIL_PCT,
        "changed":                changed,
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
        if removed and removed.get("symbol"):
            sym_u = removed["symbol"].upper()
            _auto_closed_symbols.difference_update(
                {k for k in _auto_closed_symbols if k[1] == sym_u}
            )
    if removed:
        log.info("Strategy '%s' unblocked manually", strategy_name)
        return jsonify({"unblocked": strategy_name})
    return jsonify({"error": "strategy not found"}), 404


@app.route("/api/routing/rules", methods=["GET"])
def routing_rules_list():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT id, name, enabled, nodes, created_at, tv_alert_created FROM routing_rules ORDER BY COALESCE(sort_order, id) ASC")
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
    if "name" in data and "nodes" not in data:
        # Renaming the rule: also rewrite any strategy node whose value matches
        # the old rule name. Webhook routing matches on the strategy node value,
        # so without this the rename would silently break alert routing.
        cur.execute(f"SELECT name, nodes FROM routing_rules WHERE id={p}", (rule_id,))
        row = cur.fetchone()
        if row:
            old_name  = row[0] if DATABASE_URL else row["name"]
            nodes_raw = row[1] if DATABASE_URL else row["nodes"]
            try:
                nodes = json.loads(nodes_raw) if isinstance(nodes_raw, str) else (nodes_raw or [])
                changed = False
                for n in nodes:
                    if n.get("type") == "strategy" and (n.get("value") or "").strip() == (old_name or "").strip():
                        n["value"] = data["name"]
                        changed = True
                if changed:
                    fields.append(f"nodes={p}"); vals.append(json.dumps(nodes))
            except (ValueError, TypeError):
                pass
    if "name" in data:
        fields.append(f"name={p}"); vals.append(data["name"])
    if "enabled" in data:
        fields.append(f"enabled={p}"); vals.append(int(data["enabled"]))
    if "nodes" in data:
        fields.append(f"nodes={p}"); vals.append(json.dumps(data["nodes"]))
    if "tv_alert_created" in data:
        fields.append(f"tv_alert_created={p}"); vals.append(int(bool(data["tv_alert_created"])))
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


@app.route("/api/market/prices")
def api_market_prices():
    """Return latest close prices for a comma-separated list of symbols via yfinance."""
    symbols = [s.strip().upper() for s in (request.args.get("symbols") or "").split(",") if s.strip()]
    if not symbols:
        return jsonify({})
    try:
        import yfinance as yf
        raw   = yf.download(symbols, period="5d", progress=False, auto_adjust=True)
        close = raw["Close"] if "Close" in raw.columns else raw
        result = {}
        if len(symbols) == 1:
            prices = close.dropna()
            if not prices.empty:
                result[symbols[0]] = round(float(prices.iloc[-1]), 2)
        else:
            for sym in symbols:
                if sym in close.columns:
                    prices = close[sym].dropna()
                    if not prices.empty:
                        result[sym] = round(float(prices.iloc[-1]), 2)
        return jsonify(result)
    except Exception as _e:
        log.warning("api_market_prices failed: %s", _e)
        return jsonify({})


@app.route("/api/broker/asset/<symbol>")
def broker_asset(symbol):
    """Return Alpaca asset info for a symbol — tradable, marginable, fractionable, etc."""
    if alpaca_broker is None:
        return jsonify({"error": "Alpaca not configured"}), 400
    try:
        alpaca_broker._ensure_client()
        asset = alpaca_broker._trading.get_asset(symbol.upper())
        return jsonify({
            "symbol":       asset.symbol,
            "name":         getattr(asset, "name", None),
            "tradable":     asset.tradable,
            "marginable":   getattr(asset, "marginable", None),
            "fractionable": getattr(asset, "fractionable", None),
            "shortable":    getattr(asset, "shortable", None),
            "easy_to_borrow": getattr(asset, "easy_to_borrow", None),
            "asset_class":  str(asset.asset_class) if hasattr(asset, "asset_class") else None,
            "exchange":     str(asset.exchange) if hasattr(asset, "exchange") else None,
            "status":       str(asset.status) if hasattr(asset, "status") else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


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


@app.route("/journal")
def journal():
    return render_template("journal.html")


@app.route("/research")
def research():
    return render_template("research.html")


_RESEARCH_SYSTEM = """You are an expert quantitative strategy developer helping a systematic trader.
Context: the trader runs intraday 5-minute equity strategies (US stocks, RTH 9:30-16:00 ET),
uses Camarilla R3/S3 and H4/L4 levels for entries, trades 100 shares per position,
$26k account with 4x intraday margin. All existing strategies use backtesting.py.

Your task: given a hypothesis, write a complete, runnable backtesting.py Strategy class.

REQUIREMENTS:
- Class name must be exactly: ResearchStrategy
- Must subclass backtesting.Strategy
- Parameters as class-level attributes with sensible defaults
- _trade_on_close = True  (matches Pine Script process_orders_on_close)
- Use only numpy (as np) and pandas (as pd) — both already imported
- All indicator work in init() using self.I()
- All trade logic in next() using self.buy() / self.sell() — NO size argument
- STOP LOSS / TAKE PROFIT: always pass sl= and tp= directly to buy()/sell():
      self.buy(sl=stop_price, tp=tp_price)
      self.sell(sl=stop_price, tp=tp_price)
  NEVER call self.position.close() in the same next() bar as a buy()/sell().
  The order hasn't filled yet — the close() is a no-op, the position stays
  open forever, and you get 0 completed trades with a phantom "return" from
  the open position at backtest end. This is the most common silent bug.
- SL/TP MUST be relative to self.data.Close[-1], NOT a theoretical entry
  price (e.g. or_high, pivot level, prior close). With _trade_on_close=True
  the actual fill IS the current close. If you compute sl/tp from a level that
  is already above/below the close, backtesting.py will reject with:
      "Long orders require: SL < LIMIT < TP"
      "Short orders require: TP < LIMIT < SL"
  Always do:
      fill = self.data.Close[-1]
      self.buy(sl=fill - atr_val * self.sl_mult, tp=fill + atr_val * self.tp_mult)
      self.sell(sl=fill + atr_val * self.sl_mult, tp=fill - atr_val * self.tp_mult)
  You can still use level-based signals (or_high, r3, etc.) to TRIGGER entry,
  but anchor the sl/tp to the fill price.
- Always include a hard stop loss (dollar or pct based)
- For warmup gates use `len(self.data)` (the bar count seen so far), NEVER
  `len(self)` (Strategy has no __len__) and NEVER `self.<indicator>.shape[0]`
  (that's the FULL backtest length, so the gate would never open until the
  very last bar — silent 0-trade outcome). Use a constant period:
      if len(self.data) < max(self.ema_period, self.atr_period, 20):
          return
- RTH filtering is already done server-side before the data reaches your
  strategy, so do NOT add intraday/time-of-day checks. Do NOT count
  `_bars_in_session` or add an `entry_window` gate — you'll mostly get
  `outside_window` rejections and 0 trades.
- VWAP: np.cumsum() is cumulative across the ENTIRE backtest, not per-day.
  A multi-year VWAP is meaningless. Compute rolling VWAP per day instead:
      def _rolling_vwap(h, l, c, v):
          tp = (h + l + c) / 3
          result = np.full_like(c, np.nan)
          from datetime import date  # index dates available via data.index
          return result  # not practical inside self.I() — instead use a
                         # 20-bar rolling proxy: _sma(tp*v, 20) / _sma(v, 20)
  The simplest correct VWAP proxy in backtesting.py:
      self.vwap = self.I(lambda h,l,c,v: _sma((h+l+c)/3*v, 20) / _sma(v, 20),
                         self.data.High, self.data.Low, self.data.Close, self.data.Volume)
- Volume IS available via self.data.Volume — feel free to use volume-based
  filters (e.g. `self.vol_sma = self.I(_sma, self.data.Volume, 20)`).
- DIAGNOSTICS: at the start of init(), do `self._gates = {}`. At EVERY early
  `return` in next() that rejects a candidate entry (warmup, entry-window,
  range-too-wide, volume-too-low, no-breakout, ema-misaligned, etc.),
  increment a counter immediately before the return:
      self._gates['warmup']             = self._gates.get('warmup', 0) + 1
      self._gates['range_too_wide']     = self._gates.get('range_too_wide', 0) + 1
      self._gates['volume_too_low']     = self._gates.get('volume_too_low', 0) + 1
      self._gates['no_breakout']        = self._gates.get('no_breakout', 0) + 1
      self._gates['ema_misaligned']     = self._gates.get('ema_misaligned', 0) + 1
      self._gates['entered_long']       = self._gates.get('entered_long', 0) + 1
      self._gates['entered_short']      = self._gates.get('entered_short', 0) + 1
  Use whatever names match YOUR strategy's gates. This is REQUIRED — it's the
  only way to diagnose a 0-trade outcome (range filter? volume gate? bad EMA?).
  Also count each successful entry under a descriptive key.
- Do NOT track session/range state in init() as plain instance attributes
  (e.g. `self.range_high = None`) and check it once via `current_bar_idx ==
  self.range_bars`. Backtests span many days, so per-day state must reset
  daily. If you need an opening range, detect a new session by comparing
  `self.data.index[-1].date()` to the previous bar's date and reset on
  change. Strategies that latch state once for the whole backtest will
  trade exactly once.

INDICATORS — DO NOT write your own helpers. Import the proven ones:
    from strategies.bt_strategies import _sma, _ema, _atr, _rsi, _bbands, _macd
Each accepts numpy arrays (as you get from self.data.Close, self.data.High, etc.)
and returns numpy arrays. They are the ONLY way to compute indicators —
calling .ewm() / .rolling() on self.data.Close directly will crash because
backtesting.py wraps it in an _Array, not a pandas Series.

Signatures:
    _sma(close, period)             -> ndarray
    _ema(close, period)             -> ndarray
    _atr(high, low, close, period)  -> ndarray
    _rsi(close, period)             -> ndarray
    _bbands(close, period, std_dev) -> (lower, mid, upper)
    _macd(close, fast, slow, signal)-> (macd_line, signal_line)

Use them inside self.I() like this:
    self.ema = self.I(_ema, self.data.Close, self.ema_period)
    self.atr = self.I(_atr, self.data.High, self.data.Low, self.data.Close, self.atr_period)

DO NOT: use TA-Lib, sklearn, external APIs, more than 6 parameters, or complex ML.

OUTPUT — use exactly this structure. The Strategy Code section MUST follow the
template below — fill in the marked sections, do NOT restructure the skeleton:

## Edge Analysis
[2-3 sentences explaining WHY this edge should exist in liquid equity markets]

## Overfitting Risk
[1-2 sentences on what could make historical results misleading]

## Strategy Code
```python
import numpy as np
import pandas as pd
from backtesting import Strategy
from strategies.bt_strategies import _sma, _ema, _atr, _rsi, _bbands, _macd

class ResearchStrategy(Strategy):
    _trade_on_close = True

    # ── FILL IN: numeric parameters only ──────────────────────────────────
    atr_period  = 14
    sl_mult     = 2.0   # stop  = fill ± atr * sl_mult
    tp_mult     = 3.0   # tp    = fill ∓ atr * tp_mult
    # add up to 4 more strategy-specific params here

    def init(self):
        self._gates = {}
        # ── FILL IN: indicators via self.I() ──────────────────────────────
        # self.ema  = self.I(_ema, self.data.Close, self.ema_period)
        # self.rsi  = self.I(_rsi, self.data.Close, self.rsi_period)
        self.atr  = self.I(_atr, self.data.High, self.data.Low, self.data.Close, self.atr_period)
        # session-level state (reset in next() on date change)
        self._prev_date = None
        # add other per-session vars here initialised to 0.0 / False

    def next(self):
        # ── DO NOT MODIFY: warmup gate ─────────────────────────────────────
        if len(self.data) < 20:
            self._gates['warmup'] = self._gates.get('warmup', 0) + 1
            return

        # ── DO NOT MODIFY: session reset ──────────────────────────────────
        cur_date = self.data.index[-1].date()
        if cur_date != self._prev_date:
            self._prev_date = cur_date
            # reset any per-session vars here (e.g. self._range_set = False)

        # ── DO NOT MODIFY: skip if already in a position ──────────────────
        if self.position:
            return

        # ── FILL IN: compute entry signals ────────────────────────────────
        long_signal  = False   # replace with your condition
        short_signal = False   # replace with your condition

        # ── FILL IN: optional entry filters (add gates before each return) ─
        # if some_filter_fails:
        #     self._gates['filter_name'] = self._gates.get('filter_name', 0) + 1
        #     return

        # ── DO NOT MODIFY: sl/tp anchored to actual fill price ─────────────
        fill    = self.data.Close[-1]
        atr_val = self.atr[-1]
        if np.isnan(atr_val) or atr_val <= 0:
            self._gates['invalid_atr'] = self._gates.get('invalid_atr', 0) + 1
            return

        if long_signal:
            self.buy(sl=fill - atr_val * self.sl_mult,
                     tp=fill + atr_val * self.tp_mult)
            self._gates['entered_long'] = self._gates.get('entered_long', 0) + 1

        elif short_signal:
            self.sell(sl=fill + atr_val * self.sl_mult,
                      tp=fill - atr_val * self.tp_mult)
            self._gates['entered_short'] = self._gates.get('entered_short', 0) + 1
```

## Param Ranges
```json
{"param_name": {"min": 0.1, "max": 1.0, "step": 0.1}}
```
"""


import re as _lint_re
_shape_pat     = _lint_re.compile(r"\b(self\.(?!data\b)\w+)\.shape\[0\]")
_pos_len_pat   = _lint_re.compile(r"len\(\s*self\.position\s*\)\s*>\s*0")
_pos_len_eq_pat= _lint_re.compile(r"len\(\s*self\.position\s*\)\s*==\s*0")

def _apply_lints(code):
    """Auto-fix common Claude-generated strategy bugs. Returns (warnings, fixed_code)."""
    warns = []
    if _shape_pat.search(code):
        code = _shape_pat.sub("20", code)
        warns.append("auto-fix: replaced `<indicator>.shape[0]` with literal 20 "
                     "(was the full backtest length, killed warmup gate)")
    if _pos_len_pat.search(code):
        code = _pos_len_pat.sub("self.position", code)
        warns.append("auto-fix: replaced `len(self.position) > 0` with `self.position` "
                     "(Position has no __len__)")
    if _pos_len_eq_pat.search(code):
        code = _pos_len_eq_pat.sub("not self.position", code)
        warns.append("auto-fix: replaced `len(self.position) == 0` with `not self.position`")
    return warns, code


@app.route("/api/agent/research", methods=["POST"])
def api_agent_research():
    """Stream a full research cycle: Claude writes strategy → backtest → Claude evaluates.
    Pass refine_code + refine_verdict to run an improvement iteration instead."""
    import json as _j
    data          = request.get_json(silent=True) or {}
    hypothesis    = data.get("hypothesis", "").strip()
    tickers       = [t.strip().upper() for t in data.get("tickers", ["TSLA", "QQQ", "AMD"]) if t.strip()]
    timeframe     = data.get("timeframe",  "5m")
    start_date    = data.get("start_date", "2025-01-01")
    end_date      = data.get("end_date",   "") or time.strftime("%Y-%m-%d", time.gmtime())
    cash          = float(data.get("cash", 26000))
    refine_code   = data.get("refine_code",   "").strip()   # existing strategy code to improve
    refine_verdict = data.get("refine_verdict", "").strip() # verdict text with suggestions
    is_refine     = bool(refine_code and refine_verdict)

    if not hypothesis and not is_refine:
        return jsonify({"error": "hypothesis required"}), 400

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 503

    def generate():
        def _sse(obj): return f"data: {_j.dumps(obj)}\n\n"
        try:
            import anthropic as _ant
            client = _ant.Anthropic(api_key=api_key)

            # ── Step 1: Claude writes (or refines) the strategy ──────────
            if is_refine:
                yield _sse({"type": "phase", "phase": "research",
                            "msg": "Implementing suggested improvements…"})
                user_msg = (
                    f"Here is a trading strategy and its backtest verdict.\n\n"
                    f"ORIGINAL STRATEGY CODE:\n```python\n{refine_code}\n```\n\n"
                    f"VERDICT AND SUGGESTED IMPROVEMENTS:\n{refine_verdict}\n\n"
                    f"Implement EXACTLY the specific improvements suggested in the verdict. "
                    f"If the verdict recommends abandoning the approach, pivot to the suggested "
                    f"alternative. Keep the mandatory skeleton structure (warmup gate, session "
                    f"reset, sl/tp anchored to fill=self.data.Close[-1], _trade_on_close=True). "
                    f"Test on: {', '.join(tickers)} at {timeframe} bars, starting {start_date}."
                )
            else:
                yield _sse({"type": "phase", "phase": "research",
                            "msg": "Researching hypothesis and writing strategy code…"})
                user_msg = (
                    f"Hypothesis: {hypothesis}\n\n"
                    f"Test on: {', '.join(tickers)} at {timeframe} bars, "
                    f"starting {start_date}."
                )

            full_response = ""
            with client.messages.stream(
                model="claude-haiku-4-5-20251001",
                max_tokens=4000,
                system=_RESEARCH_SYSTEM,
                messages=[{"role": "user", "content": user_msg}],
            ) as stream:
                for text in stream.text_stream:
                    full_response += text
                    yield _sse({"type": "research_chunk", "text": text})

            yield _sse({"type": "research_done", "full_text": full_response})

            # Extract code block — handle truncated responses where closing ``` never arrived
            import re as _re
            code_match = _re.search(r"```python\s*(.*?)```", full_response, _re.DOTALL)
            if not code_match:
                # Fallback: take everything after ```python even if unclosed
                open_match = _re.search(r"```python\s*(.*)", full_response, _re.DOTALL)
                if open_match:
                    raw_code = open_match.group(1).strip()
                    yield _sse({"type": "warning_msg",
                                "msg": "Response was truncated — code may be incomplete. Proceeding anyway."})
                else:
                    yield _sse({"type": "error",
                                "msg": "Claude did not produce a code block. Try a simpler hypothesis."}); return
            else:
                raw_code = _strip_code_fences(code_match.group(1).strip())

            # Extract param ranges
            params_match = _re.search(r"## Param Ranges\s*```json\s*(.*?)```", full_response, _re.DOTALL)
            param_ranges = {}
            if params_match:
                try: param_ranges = _j.loads(params_match.group(1).strip())
                except Exception: pass

            warns, raw_code = _apply_lints(raw_code)
            for w in warns:
                yield _sse({"type": "warning_msg", "msg": w})

            yield _sse({"type": "code", "code": raw_code, "param_ranges": param_ranges})

            # ── Step 2: Validate the code compiles ───────────────────────
            yield _sse({"type": "phase", "phase": "validate", "msg": "Validating strategy code…"})
            import backtesting as _bktst
            def _compile_strategy(code):
                ns = {}
                exec(compile(code, "<research>", "exec"), ns)
                cls = next(
                    (v for v in ns.values()
                     if isinstance(v, type) and issubclass(v, _bktst.Strategy)
                     and v is not _bktst.Strategy), None)
                return cls

            try:
                strategy_cls = _compile_strategy(raw_code)
            except Exception as _ce:
                yield _sse({"type": "error", "msg": f"Strategy code has a syntax error: {_ce}"}); return
            if not strategy_cls:
                yield _sse({"type": "error", "msg": "No Strategy subclass found — Claude may have renamed it."}); return

            # Static checks for the most common silent bugs before burning a backtest run
            _code_warnings = []
            if "self.buy()" in raw_code or "self.sell()" in raw_code:
                # buy()/sell() with no sl= means no stop — check if position.close() is
                # used as a manual stop in the same method (the no-op pattern)
                import re as _re
                if _re.search(r"self\.(buy|sell)\(\s*\).*\n.*self\.position\.close\(\)", raw_code) or \
                   _re.search(r"self\.(buy|sell)\(\s*\)[^\n]*\n[^\n]*self\.position\.close\(\)", raw_code):
                    _code_warnings.append(
                        "⚠ Detected self.buy()/sell() followed by self.position.close() "
                        "on the next line — the position hasn't filled yet so close() is a "
                        "no-op. Use self.buy(sl=..., tp=...) instead."
                    )
            if "_bars_in_session" in raw_code or "entry_window" in raw_code:
                _code_warnings.append(
                    "⚠ Strategy has an entry_window/bars_in_session gate — RTH data is "
                    "already pre-filtered, so this will likely reject most bars as "
                    "'outside_window' and produce 0 trades."
                )
            if "np.cumsum" in raw_code and "vwap" in raw_code.lower():
                _code_warnings.append(
                    "⚠ np.cumsum used for VWAP — this accumulates across the entire "
                    "backtest, not per day. Use the 20-bar rolling proxy instead: "
                    "_sma(tp*v, 20) / _sma(v, 20)."
                )
            import re as _re2
            if _re2.search(r"entry_price\s*=\s*self\.(or_high|or_low|data\.Open|data\.High|data\.Low)", raw_code) \
               and ("sl=stop_loss" in raw_code or "sl=sl_price" in raw_code):
                _code_warnings.append(
                    "⚠ sl/tp appear to be anchored to a level (or_high/or_low/Open) rather "
                    "than self.data.Close[-1]. With _trade_on_close=True the fill IS the close — "
                    "if the close has already moved past the level the order will be rejected "
                    "with 'Long/Short orders require: SL < LIMIT < TP'. "
                    "Fix: fill=self.data.Close[-1]; sl=fill±atr*mult; tp=fill∓atr*mult."
                )
            if _code_warnings:
                yield _sse({"type": "code_warnings", "warnings": _code_warnings})

            yield _sse({"type": "validate_ok", "class_name": strategy_cls.__name__})

            # Fix-on-error: when a backtest crashes in user code, send the broken
            # strategy + the actual error back to Sonnet for a targeted fix, then
            # retry. Capped to keep cost + latency bounded.
            MAX_FIX_ATTEMPTS = 2
            fix_attempts = [0]
            def _attempt_fix(broken_code, error_full):
                if fix_attempts[0] >= MAX_FIX_ATTEMPTS:
                    return None, None
                fix_attempts[0] += 1
                fix_prompt = (
                    "You are debugging a backtesting.py Strategy. The code below crashed "
                    "during bt.run() with this error:\n\n"
                    f"ERROR: {error_full}\n\n"
                    "Fix ONLY this specific bug. Do not refactor unrelated parts.\n\n"
                    "Rules:\n"
                    "- Class name must remain ResearchStrategy\n"
                    "- Indicators come from `from strategies.bt_strategies import _sma, _ema, _atr, _rsi, _bbands, _macd`\n"
                    "- Volume IS available via self.data.Volume\n"
                    "- Use `self.position` (truthy) for in-position check; never `len(self.position)`\n"
                    "- Use literal warmup periods (e.g. 20), never `<indicator>.shape[0]`\n"
                    "- Initialize all session state attrs in init() to safe defaults (0 or 0.0), "
                    "  not None, so arithmetic never hits NoneType\n"
                    "- Per-day state must reset on date change\n"
                    "- Keep `self._gates` instrumentation\n"
                    "- SL/TP MUST be relative to self.data.Close[-1] (the actual fill price "
                    "  when _trade_on_close=True), NOT a theoretical level like or_high/or_low. "
                    "  If you see 'Long orders require: SL < LIMIT < TP' or 'Short orders require: "
                    "  TP < LIMIT < SL', it means sl/tp were anchored to the wrong price. "
                    "  Fix: fill = self.data.Close[-1]; sl = fill ± atr*mult; tp = fill ∓ atr*mult\n\n"
                    "Return ONLY the corrected code in this exact format and nothing else:\n\n"
                    "```python\n<corrected code>\n```\n\n"
                    "CODE TO FIX:\n```python\n" + broken_code + "\n```\n"
                )
                try:
                    resp = client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=4500,
                        messages=[{"role": "user", "content": fix_prompt}],
                    )
                    fix_text = resp.content[0].text if resp.content else ""
                except Exception as _fe:
                    log.warning("research fix call failed: %s", _fe)
                    return None, None
                m = _re.search(r"```python\s*(.*?)```", fix_text, _re.DOTALL) \
                    or _re.search(r"```python\s*(.*)", fix_text, _re.DOTALL)
                if not m:
                    return None, None
                new_code = _strip_code_fences(m.group(1).strip())
                _, new_code = _apply_lints(new_code)
                try:
                    new_cls = _compile_strategy(new_code)
                except Exception:
                    return None, None
                if not new_cls:
                    return None, None
                return new_code, new_cls

            # ── Step 3: Run backtest on each ticker ───────────────────────
            yield _sse({"type": "phase", "phase": "backtest",
                        "msg": f"Running backtest on {len(tickers)} tickers…"})
            from backtesting import Backtest as _BT
            from strategies.data import fetch_bars_alpaca, fetch_bars
            import pandas as _pd

            results = []
            for i, ticker in enumerate(tickers):
                yield _sse({"type": "bt_progress", "ticker": ticker,
                            "pct": int(i / len(tickers) * 100),
                            "msg": f"Fetching {ticker} data…"})

                # Fetch + prep data — run in thread too so we can heartbeat during fetch.
                try:
                    from concurrent.futures import ThreadPoolExecutor as _TPE2, TimeoutError as _ToutE2
                    def _fetch():
                        try:
                            return fetch_bars_alpaca(ticker, start_date, end_date, timeframe)
                        except Exception:
                            return fetch_bars(ticker, start_date, end_date, timeframe)
                    with _TPE2(max_workers=1) as _ex2:
                        _ff = _ex2.submit(_fetch)
                        _fe = 0
                        while True:
                            try:
                                raw = _ff.result(timeout=5)
                                break
                            except _ToutE2:
                                _fe += 5
                                if _fe >= 60:
                                    raise Exception(f"data fetch timed out after 60s for {ticker}")
                                yield ": heartbeat\n\n"
                except Exception as _fetch_err:
                    results.append({"ticker": ticker, "error": f"fetch failed: {_fetch_err}"[:200]})
                    continue
                if not raw or len(raw) < 50:
                    results.append({"ticker": ticker, "error": "insufficient data"})
                    continue
                df = _pd.DataFrame(raw).set_index("time")
                df.index = _pd.to_datetime(df.index)
                df.columns = [c.title() for c in df.columns]
                keep = [c for c in ("Open","High","Low","Close","Volume") if c in df.columns]
                df = df[keep].dropna()
                if "Volume" not in df.columns: df["Volume"] = 0
                df = _filter_rth(df)

                # Retry loop: on a code-bug crash, ask Sonnet to fix the strategy
                # and rerun. Bounded by MAX_FIX_ATTEMPTS so a stubborn bug doesn't
                # spin forever. Already-passed tickers keep their results; only
                # the failing ticker (and any later ones) use the fixed code.
                # backtesting.py returns NaN for PF/Sharpe/WinRate when 0 trades fire,
                # and NaN serializes as "NaN" which is invalid JSON — the client's
                # JSON.parse rejects it and the whole bt_done message is dropped silently.
                def _num(v, default=0.0):
                    try:
                        f = float(v)
                        return f if f == f else default  # NaN: f != f
                    except (TypeError, ValueError):
                        return default

                yield _sse({"type": "bt_progress", "ticker": ticker,
                            "pct": int(i / len(tickers) * 100),
                            "msg": f"Running backtest on {ticker}…"})

                while True:
                    try:
                        bt = _BT(df, strategy_cls, cash=cash, commission=0.0005,
                                 exclusive_orders=True,
                                 trade_on_close=getattr(strategy_cls, "_trade_on_close", False))
                        # Run with a 90-second hard timeout. Poll every 5 seconds so
                        # we can emit SSE heartbeat comments — this keeps Railway's
                        # reverse proxy from killing the idle connection mid-backtest.
                        from concurrent.futures import ThreadPoolExecutor as _TPE, TimeoutError as _ToutE
                        with _TPE(max_workers=1) as _ex:
                            _fut = _ex.submit(bt.run)
                            _elapsed = 0
                            while True:
                                try:
                                    s = _fut.result(timeout=5)
                                    break
                                except _ToutE:
                                    _elapsed += 5
                                    if _elapsed >= 90:
                                        _fut.cancel()
                                        raise RuntimeError(
                                            f"backtest timed out after 90s on {ticker}"
                                            " — strategy may have an infinite loop or"
                                            " O(n²) operation in next()")
                                    yield ": heartbeat\n\n"  # keeps proxy connection alive
                        # Extract per-gate diagnostic counters that Claude was told to maintain.
                        # Surfacing these makes 0-trade outcomes debuggable — instead of
                        # "the strategy didn't trade", the user sees WHICH filter blocked it.
                        diag = {}
                        try:
                            strat_inst = getattr(s, "_strategy", None)
                            gates = getattr(strat_inst, "_gates", None)
                            if isinstance(gates, dict):
                                diag = {k: int(v) for k, v in gates.items()
                                        if isinstance(v, (int, float))}
                        except Exception:
                            pass
                        results.append({
                            "ticker":       ticker,
                            "pf":           round(_num(s.get("Profit Factor")), 3),
                            "sharpe":       round(_num(s.get("Sharpe Ratio")),  3),
                            "return_pct":   round(_num(s.get("Return [%]")),    2),
                            "win_rate":     round(_num(s.get("Win Rate [%]")),  1),
                            "max_dd":       round(abs(_num(s.get("Max. Drawdown [%]"))), 2),
                            "trades":       int(_num(s.get("# Trades"))),
                            "diag":         diag,
                        })
                        break
                    except Exception as _be:
                        # Capture the deepest user-code frame so we know WHICH line crashed.
                        import traceback as _tb
                        line_hint = ""
                        try:
                            for fr in _tb.extract_tb(_be.__traceback__):
                                if fr.filename == "<research>":
                                    line_hint = f" [line {fr.lineno}: {fr.line}]"
                        except Exception:
                            pass
                        error_full = (str(_be) + line_hint)[:500]

                        # Only attempt a fix when the crash looks like generated-strategy code:
                        # has a <research> frame, or a typical bug signature. Otherwise it's
                        # an environment issue (network, missing data, etc.) — no fix to make.
                        looks_fixable = bool(line_hint) or any(
                            sig in str(_be) for sig in (
                                "operand type", "has no attribute", "has no len()",
                                "Indicator", "index out of range",
                            )
                        )
                        if looks_fixable and fix_attempts[0] < MAX_FIX_ATTEMPTS:
                            yield _sse({"type": "fix_progress",
                                        "msg": f"{ticker} crashed — sending error to Sonnet to fix "
                                               f"(attempt {fix_attempts[0] + 1}/{MAX_FIX_ATTEMPTS})…",
                                        "error": error_full})
                            new_code, new_cls = _attempt_fix(raw_code, error_full)
                            if new_code and new_cls:
                                raw_code     = new_code
                                strategy_cls = new_cls
                                yield _sse({"type": "code_fixed",
                                            "code": raw_code,
                                            "msg": f"Strategy patched. Retrying {ticker}…"})
                                continue  # retry same ticker with fixed code
                            yield _sse({"type": "warning_msg",
                                        "msg": "Fix attempt failed — recording original error and moving on."})

                        results.append({"ticker": ticker, "error": error_full})
                        break

            yield _sse({"type": "bt_done", "results": results, "code": raw_code})

            # ── Step 4: Claude evaluates results ─────────────────────────
            yield _sse({"type": "phase", "phase": "evaluate",
                        "msg": "Evaluating results…"})
            good   = [r for r in results if "pf" in r and r["pf"] >= 1.0]
            strong = [r for r in results if "pf" in r and r["pf"] >= 1.3]
            avg_pf = round(sum(r.get("pf",0) for r in results if "pf" in r) /
                           max(len([r for r in results if "pf" in r]),1), 3)
            result_summary = "\n".join(
                f"  {r['ticker']}: PF={r.get('pf','err')} Sharpe={r.get('sharpe','err')} "
                f"Return={r.get('return_pct','err')}% Trades={r.get('trades','err')}"
                for r in results)

            # Include gate diagnostics in eval so Claude can diagnose 0-trade runs
            gate_summary = "\n".join(
                f"  {r['ticker']} gates: {r.get('gates', {})}" for r in results if r.get('gates'))

            eval_prompt = (
                f"Strategy hypothesis: {hypothesis}\n\n"
                f"Strategy code:\n```python\n{raw_code}\n```\n\n"
                f"Backtest results across {len(tickers)} tickers ({timeframe}, {start_date}–{end_date}):\n"
                f"{result_summary}\n\n"
                + (f"Gate diagnostics (bars rejected per filter):\n{gate_summary}\n\n" if gate_summary else "")
                + f"Summary: {len(strong)}/{len(tickers)} tickers passed PF≥1.3 · "
                f"avg PF {avg_pf} · {len(good)}/{len(tickers)} profitable\n\n"
                f"Provide a structured verdict using EXACTLY these 4 numbered points:\n\n"
                f"1. **Edge assessment**: Is the edge real or overfit? Cite specific numbers (PF, Sharpe, trade count).\n"
                f"2. **Failure diagnosis**: What specific gate, parameter, or logic is causing losses or 0 trades? "
                f"Reference actual gate counts or code lines.\n"
                f"3. **Concrete fix** (this MUST be actionable code-level changes, not vague suggestions):\n"
                f"   - Change parameter X from N to M because ...\n"
                f"   - Replace condition `<code>` with `<code>` because ...\n"
                f"   - Add filter: `if <condition>: return` to reject ... \n"
                f"   List every change needed — this section will be fed directly to an LLM to implement.\n"
                f"4. **Recommendation**: Save and test live / Refine (run iteration) / Abandon."
            )
            verdict = ""
            with client.messages.stream(
                model="claude-haiku-4-5-20251001",
                max_tokens=700,
                messages=[{"role": "user", "content": eval_prompt}],
            ) as stream:
                for text in stream.text_stream:
                    verdict += text
                    yield _sse({"type": "verdict_chunk", "text": text})

            # Final summary
            passed = len(strong)
            overall = "pass" if passed >= len(tickers) * 0.5 and avg_pf >= 1.2 else \
                      "marginal" if passed >= 2 and avg_pf >= 1.0 else "fail"
            yield _sse({
                "type": "done",
                "overall": overall,
                "passes": passed,
                "n_tickers": len(tickers),
                "avg_pf": avg_pf,
                "code": raw_code,
                "verdict": verdict,
            })

        except Exception as _e:
            log.exception("Research agent error")
            yield _sse({"type": "error", "msg": str(_e)})

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ---------------------------------------------------------------------------
# Agent optimization loop
# ---------------------------------------------------------------------------

@app.route("/api/agent/optimize", methods=["POST"])
def api_agent_optimize():
    """SSE: automated single-parameter hill-climbing optimization loop.
    Proposes one parameter change per iteration, accepts if avg Sharpe improves,
    reverts otherwise. Stops after max_failures consecutive non-improvements."""
    import json as _j
    data       = request.get_json(silent=True) or {}
    code       = data.get("code",       "").strip()
    tickers    = [t.strip().upper() for t in data.get("tickers", []) if t.strip()]
    timeframe  = data.get("timeframe",  "5m")
    start_date = data.get("start_date", "2025-01-01")
    end_date   = data.get("end_date",   "") or time.strftime("%Y-%m-%d", time.gmtime())
    val_start  = data.get("val_start",  "")
    val_end    = data.get("val_end",    "")
    cash       = float(data.get("cash", 26000))
    max_iter   = int(data.get("max_iterations", 10))
    max_fail   = int(data.get("max_failures",   5))

    if not code or not tickers:
        return jsonify({"error": "code and tickers required"}), 400
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 503

    def generate():
        def _sse(obj): return f"data: {_j.dumps(obj)}\n\n"
        import re as _re
        import anthropic as _ant
        import pandas as _pd
        from backtesting import Backtest as _BT
        from strategies.data import fetch_bars_alpaca, fetch_bars
        from concurrent.futures import ThreadPoolExecutor as _TPE, TimeoutError as _ToutE
        client = _ant.Anthropic(api_key=api_key)

        def _num(v, default=0.0):
            try:
                f = float(v)
                return f if f == f else default
            except (TypeError, ValueError):
                return default

        def _compile_cls(src):
            import backtesting as _bktst
            ns = {"__builtins__": __builtins__}
            exec(compile(src, "<opt>", "exec"), ns)
            return next((v for v in ns.values()
                         if isinstance(v, type) and issubclass(v, _bktst.Strategy)
                         and v is not _bktst.Strategy), None)

        def _run_ticker_gen(strategy_cls, ticker, s_date, e_date):
            """Generator: yields ('hb',None) heartbeats or ('result', dict)."""
            try:
                def _fetch():
                    try:    return fetch_bars_alpaca(ticker, s_date, e_date, timeframe)
                    except: return fetch_bars(ticker, s_date, e_date, timeframe)
                with _TPE(max_workers=1) as ex:
                    ff = ex.submit(_fetch)
                    elapsed = 0
                    while True:
                        try:
                            raw = ff.result(timeout=5); break
                        except _ToutE:
                            elapsed += 5
                            if elapsed >= 60:
                                raise RuntimeError(f"data fetch timeout for {ticker}")
                            yield ("hb", None)
                if not raw or len(raw) < 50:
                    yield ("result", {"ticker": ticker, "sharpe": 0.0, "pf": 0.0,
                                      "trades": 0, "error": "insufficient data"}); return
                df = _pd.DataFrame(raw).set_index("time")
                df.index = _pd.to_datetime(df.index)
                df.columns = [c.title() for c in df.columns]
                keep = [c for c in ("Open","High","Low","Close","Volume") if c in df.columns]
                df = df[keep].dropna()
                if "Volume" not in df.columns: df["Volume"] = 0
                df = _filter_rth(df)
                bt = _BT(df, strategy_cls, cash=cash, commission=0.0005,
                         exclusive_orders=True,
                         trade_on_close=getattr(strategy_cls, "_trade_on_close", False))
                with _TPE(max_workers=1) as ex2:
                    fut = ex2.submit(bt.run)
                    elapsed = 0
                    while True:
                        try:
                            s = fut.result(timeout=5); break
                        except _ToutE:
                            elapsed += 5
                            if elapsed >= 90:
                                raise RuntimeError(f"backtest timeout for {ticker}")
                            yield ("hb", None)
                yield ("result", {
                    "ticker":   ticker,
                    "sharpe":   round(_num(s.get("Sharpe Ratio")),  3),
                    "pf":       round(_num(s.get("Profit Factor")), 3),
                    "trades":   int(_num(s.get("# Trades"))),
                    "ret_pct":  round(_num(s.get("Return [%]")),    2),
                    "win_rate": round(_num(s.get("Win Rate [%]")),  1),
                    "max_dd":   round(abs(_num(s.get("Max. Drawdown [%]"))), 2),
                })
            except Exception as _e:
                yield ("result", {"ticker": ticker, "sharpe": 0.0, "pf": 0.0,
                                  "trades": 0, "error": str(_e)[:120]})

        def _run_all_gen(strategy_cls, s_date, e_date):
            """Generator: yields ('hb',None) heartbeats, ('ticker_start', info),
            ('ticker_done', result), then ('done', results_list) at the end."""
            results = []
            for i, ticker in enumerate(tickers):
                yield ("ticker_start", {"ticker": ticker, "i": i, "n": len(tickers)})
                for item in _run_ticker_gen(strategy_cls, ticker, s_date, e_date):
                    if item[0] == "hb": yield ("hb", None)
                    else:
                        results.append(item[1])
                        yield ("ticker_done", item[1])
            yield ("done", results)

        def _avg(results, key):
            vals = [r[key] for r in results if key in r and "error" not in r]
            return round(sum(vals) / len(vals), 3) if vals else 0.0

        try:
            # ── Baseline ─────────────────────────────────────────────────
            yield _sse({"type": "opt_phase", "phase": "baseline",
                        "msg": "Running baseline backtest…"})
            base_cls = _compile_cls(code)
            if not base_cls:
                yield _sse({"type": "opt_error", "msg": "Could not compile strategy"}); return
            base_results = []
            for item in _run_all_gen(base_cls, start_date, end_date):
                if item[0] == "hb":
                    yield ": heartbeat\n\n"
                elif item[0] == "ticker_start":
                    yield _sse({"type": "opt_ticker", "phase": "baseline",
                                "ticker": item[1]["ticker"], "i": item[1]["i"],
                                "n": item[1]["n"]})
                elif item[0] == "done":
                    base_results = item[1]
            best_code    = code
            best_cls     = base_cls
            best_results = base_results
            best_sharpe  = _avg(base_results, "sharpe")
            best_pf      = _avg(base_results, "pf")
            yield _sse({"type": "opt_baseline", "results": base_results,
                        "avg_sharpe": best_sharpe, "avg_pf": best_pf})

            history, consec_fail = [], 0

            # ── Optimization loop ────────────────────────────────────────
            for iteration in range(1, max_iter + 1):
                if consec_fail >= max_fail:
                    yield _sse({"type": "opt_stopped", "iteration": iteration,
                                "reason": f"{max_fail} consecutive non-improvements — likely at local maximum"})
                    break

                yield _sse({"type": "opt_iter_start", "iteration": iteration,
                            "msg": f"Iteration {iteration}/{max_iter}: proposing change…"})

                history_str = "\n".join(
                    f"  [{h['iter']}] {h['param']}={h['old']}→{h['new']} "
                    f"Sharpe {h['sharpe_before']}→{h['sharpe_after']} "
                    f"({'✓' if h['accepted'] else '✗'})"
                    for h in history) or "  (none yet — first iteration)"

                result_lines = "\n".join(
                    f"  {r['ticker']}: Sharpe={r.get('sharpe',0)} PF={r.get('pf',0)} "
                    f"Trades={r.get('trades',0)} Ret={r.get('ret_pct',0)}%"
                    + (f" [ERR: {r['error']}]" if "error" in r else "")
                    for r in best_results)

                opt_prompt = (
                    "You are a trading strategy parameter optimizer. Given the current strategy "
                    "and its backtest results, propose ONE parameter change to improve average "
                    "Sharpe Ratio across all tickers.\n\n"
                    "EVALUATION HIERARCHY: 1. Sharpe Ratio  2. Profit Factor\n\n"
                    "RULES:\n"
                    "- Change ONLY ONE numeric class-level parameter value\n"
                    "- Do NOT modify init() or next() code in any way\n"
                    "- Propose a specific number, not a range\n"
                    "- Do not repeat a (param, direction) combination from history\n"
                    "- Low trade count (<5/ticker) → loosen entry threshold or widen ATR mult\n"
                    "- Negative Sharpe + many trades → tighten stops or raise entry threshold\n\n"
                    f"CURRENT BEST: Avg Sharpe={best_sharpe} | Avg PF={best_pf}\n"
                    f"Per ticker:\n{result_lines}\n\n"
                    f"TRIED CHANGES:\n{history_str}\n\n"
                    f"STRATEGY CODE:\n```python\n{best_code}\n```\n\n"
                    "Output EXACTLY this format — nothing else:\n"
                    "## Change\n"
                    "param: <name>\n"
                    "old: <current_value>\n"
                    "new: <new_value>\n"
                    "reasoning: <1-2 sentences>\n\n"
                    "## Code\n"
                    "```python\n<full strategy code with ONLY that one value changed>\n```"
                )

                opt_resp = ""
                with client.messages.stream(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=3000,
                    messages=[{"role": "user", "content": opt_prompt}],
                ) as stream:
                    for txt in stream.text_stream:
                        opt_resp += txt
                        yield _sse({"type": "opt_chunk", "iteration": iteration, "text": txt})

                # Parse the structured response
                chg = _re.search(
                    r"## Change\s*\nparam:\s*(\S+)\s*\nold:\s*(\S+)\s*\nnew:\s*(\S+)\s*\nreasoning:\s*(.+?)(?=\n\n## Code|\Z)",
                    opt_resp, _re.DOTALL)
                cde = _re.search(r"```python\s*(.*?)```", opt_resp, _re.DOTALL)

                if not chg or not cde:
                    yield _sse({"type": "opt_iter_result", "iteration": iteration,
                                "accepted": False, "reason": "parse error — skipping",
                                "sharpe_before": best_sharpe, "sharpe_after": None})
                    consec_fail += 1; continue

                param_name = chg.group(1).strip()
                old_val    = chg.group(2).strip()
                new_val    = chg.group(3).strip()
                reasoning  = chg.group(4).strip()
                new_code   = _strip_code_fences(cde.group(1).strip())
                _, new_code = _apply_lints(new_code)

                try:
                    new_cls = _compile_cls(new_code)
                    if not new_cls: raise RuntimeError("No Strategy subclass found")
                except Exception as ce:
                    yield _sse({"type": "opt_iter_result", "iteration": iteration,
                                "accepted": False, "reason": f"compile error: {ce}",
                                "param": param_name, "old": old_val, "new": new_val,
                                "sharpe_before": best_sharpe, "sharpe_after": None})
                    consec_fail += 1; continue

                yield _sse({"type": "opt_testing", "iteration": iteration,
                            "param": param_name, "old": old_val, "new": new_val})

                new_results = []
                for item in _run_all_gen(new_cls, start_date, end_date):
                    if item[0] == "hb":
                        yield ": heartbeat\n\n"
                    elif item[0] == "ticker_start":
                        yield _sse({"type": "opt_ticker", "phase": "iter",
                                    "iteration": iteration,
                                    "ticker": item[1]["ticker"],
                                    "i": item[1]["i"], "n": item[1]["n"]})
                    elif item[0] == "done":
                        new_results = item[1]

                new_sharpe = _avg(new_results, "sharpe")
                new_pf     = _avg(new_results, "pf")
                accepted   = new_sharpe > best_sharpe

                h = {"iter": iteration, "param": param_name, "old": old_val,
                     "new": new_val, "reasoning": reasoning,
                     "sharpe_before": best_sharpe, "sharpe_after": new_sharpe,
                     "pf_before": best_pf, "pf_after": new_pf,
                     "accepted": accepted, "results": new_results}
                history.append(h)

                if accepted:
                    best_code, best_cls, best_results = new_code, new_cls, new_results
                    best_sharpe, best_pf = new_sharpe, new_pf
                    consec_fail = 0
                else:
                    consec_fail += 1

                yield _sse({**h, "type": "opt_iter_result",
                            "consec_fail": consec_fail,
                            "best_sharpe": best_sharpe})

            # ── Out-of-sample validation ─────────────────────────────────
            val_results = None
            if val_start and val_end and best_code != code:
                yield _sse({"type": "opt_phase", "phase": "validation",
                            "msg": f"Validating best params out-of-sample ({val_start} to {val_end})…"})
                val_results = []
                val_cls = _compile_cls(best_code)
                for item in _run_all_gen(val_cls, val_start, val_end):
                    if item[0] == "hb":
                        yield ": heartbeat\n\n"
                    elif item[0] == "ticker_start":
                        yield _sse({"type": "opt_ticker", "phase": "validation",
                                    "ticker": item[1]["ticker"],
                                    "i": item[1]["i"], "n": item[1]["n"]})
                    elif item[0] == "done":
                        val_results = item[1]

            yield _sse({"type": "opt_done",
                        "best_code":    best_code,
                        "best_sharpe":  best_sharpe,
                        "best_pf":      best_pf,
                        "base_sharpe":  _avg(base_results, "sharpe"),
                        "base_pf":      _avg(base_results, "pf"),
                        "best_results": best_results,
                        "val_results":  val_results,
                        "history":      history,
                        "improved":     best_code != code})
        except Exception as _e:
            log.exception("Optimize agent error")
            yield _sse({"type": "opt_error", "msg": str(_e)})

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/journal/entries")
def api_journal_entries():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM journal_entries ORDER BY week DESC")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if DATABASE_URL else None
    entries = []
    for r in rows:
        e = dict(zip(cols, r)) if DATABASE_URL else dict(r)
        for f in ("trade_stats", "market_data", "tags"):
            try:
                e[f] = json.loads(e[f]) if e[f] else {}
            except Exception:
                e[f] = {}
        entries.append(e)
    conn.close()
    return jsonify(entries)


@app.route("/api/journal/summary", methods=["PUT"])
def api_journal_summary():
    """Persist the AI-generated summary and tags after streaming completes."""
    data    = request.get_json(silent=True) or {}
    week    = data.get("week", "").strip()
    summary = data.get("ai_summary", "")
    tags    = data.get("tags")
    if not week:
        return jsonify({"error": "week required"}), 400
    p = placeholder()
    conn = get_db()
    cur  = conn.cursor()
    cur.execute(
        f"UPDATE journal_entries SET ai_summary={p}, tags={p} WHERE week={p}",
        (summary, json.dumps(tags) if tags is not None else None, week)
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/journal/notes", methods=["PUT"])
def api_journal_notes():
    data  = request.get_json(silent=True) or {}
    week  = data.get("week", "").strip()
    notes = data.get("notes", "")
    if not week:
        return jsonify({"error": "week required"}), 400
    p = placeholder()
    conn = get_db()
    cur  = conn.cursor()
    cur.execute(f"UPDATE journal_entries SET user_notes={p} WHERE week={p}", (notes, week))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/journal/entries/<week>", methods=["DELETE"])
def api_journal_delete(week):
    p = placeholder()
    conn = get_db(); cur = conn.cursor()
    cur.execute(f"DELETE FROM journal_entries WHERE week={p}", (week,))
    conn.commit(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/journal/generate", methods=["POST"])
def api_journal_generate():
    """Generate a weekly journal entry. Streams the AI summary via SSE."""
    import datetime as _dtmod

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 503
    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({"error": "anthropic package not installed"}), 503

    data    = request.get_json(silent=True) or {}
    week    = data.get("week", "").strip()   # "YYYY-WXX"
    # Default to Alpaca Refined (account 2) — that's the production cohort the
    # weekly journal is meant to reflect. Pass account="1" to use Paper All.
    account = str(data.get("account", "2")).strip() or "2"
    use_acct2 = (account == "2")

    # Default to current ISO week (must match <input type="week"> / fromisocalendar;
    # strftime %W is NOT ISO and can be off by one).
    if not week:
        _iso  = _dtmod.date.today().isocalendar()
        week  = f"{_iso[0]}-W{_iso[1]:02d}"

    # Derive week start/end dates from the ISO 8601 week string (matches <input type="week">)
    try:
        year, wnum = week.split("-W")
        week_start = _dtmod.date.fromisocalendar(int(year), int(wnum), 1)  # Monday
        week_end   = week_start + _dtmod.timedelta(days=4)   # Friday
    except Exception:
        today = _dtmod.date.today()
        week_start = today - _dtmod.timedelta(days=today.weekday())
        week_end   = week_start + _dtmod.timedelta(days=4)

    from_date = str(week_start)
    to_date   = str(week_end)

    # ── Trade stats for the week (same LIFO logic as /api/alpaca/analysis) ──
    trade_stats = {}
    try:
        active_broker = alpaca_broker2 if use_acct2 else alpaca_broker
        if active_broker is not None:
            from datetime import datetime as _dt2
            fills = _get_cached_fills_2() if use_acct2 else _get_cached_fills()
            week_fills = [f for f in fills if from_date <= (f.get("time") or "")[:10] <= to_date]

            # Build signal lookup (ticker, side) → [(ts, strategy, sentiment)]
            conn = get_db()
            cur  = conn.cursor()
            cur.execute("SELECT ticker, action, sentiment, received_at, strategy, exec_status FROM trades ORDER BY id ASC")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if DATABASE_URL else None
            sig_rows = [dict(zip(cols, r)) if DATABASE_URL else dict(r) for r in rows]
            conn.close()

            _j_sig_lkp = {}
            for t in sig_rows:
                if (t.get("exec_status") or "").lower() in ("blocked", "skipped", "error"):
                    continue
                tk  = (t.get("ticker") or "").strip().upper()
                act = _canonical_action(t.get("action"))
                rcv = t.get("received_at") or ""
                stg = (t.get("strategy") or "").strip()
                snt = (t.get("sentiment") or "").strip().lower()
                if not stg or not tk or not rcv: continue
                sd = "BOT" if act == "BUY" else "SLD" if act == "SELL" else None
                if not sd: continue
                try:
                    ts = _dt2.fromisoformat(rcv.replace("Z", "+00:00")).timestamp()
                except Exception:
                    continue
                _j_sig_lkp.setdefault((tk, sd), []).append((ts, stg, snt))

            def _j_resolve(sym, side, ftime, order_id=""):
                try:
                    fts = _dt2.fromisoformat(ftime.replace("Z", "+00:00")).timestamp()
                except Exception:
                    return "Unknown", ""
                cands = _j_sig_lkp.get((sym.upper(), side), [])
                if cands:
                    best = min(cands, key=lambda x: abs(x[0] - fts))
                    if abs(best[0] - fts) <= 300:
                        return best[1], best[2]
                if order_id and order_id.startswith("kairos-"):
                    parts = order_id.split("-", 2)
                    if len(parts) == 3 and parts[1]:
                        return parts[1], ""
                return "Unknown", ""

            # Deduplicate + sort
            seen = set()
            deduped = []
            for f in week_fills:
                k = f"{f['symbol']}|{f['side']}|{f['time']}|{f['shares']}"
                if k not in seen:
                    seen.add(k)
                    deduped.append(f)
            deduped.sort(key=lambda f: (f.get("time") or ""))

            # LIFO pairing (mirrors analysis page)
            j_open_longs, j_open_shorts, j_closed = {}, {}, []
            for f in deduped:
                sym      = (f.get("symbol") or "").upper()
                side     = f.get("side", "")
                price    = float(f.get("price") or 0)
                qty      = float(f.get("shares") or 0)
                fill_ts  = f.get("time", "")
                date_str = fill_ts[:10] if fill_ts else ""
                strat, sentiment = _j_resolve(sym, side, fill_ts, f.get("order_id", ""))
                if sentiment == "flat":   intent = "exit"
                elif sentiment == "long": intent = "enter_long"
                elif sentiment == "short":intent = "enter_short"
                else:                     intent = "legacy"
                if side == "BOT":
                    if intent == "enter_long":
                        j_open_longs.setdefault(sym, []).append((price, qty, fill_ts, strat))
                        continue
                    q = j_open_shorts.setdefault(sym, [])
                    while qty > 0 and q:
                        ep, eq, et, es = q.pop(-1)
                        m = min(qty, eq)
                        j_closed.append({"pnl": round((ep - price) * m, 2), "strategy": es, "ticker": sym,
                                         "date": date_str, "entry_time": et, "exit_time": fill_ts, "side": "SHORT",
                                         "entry_price": ep, "exit_price": price, "qty": m})
                        qty -= m
                        if eq > m: q.append((ep, eq - m, et, es))
                    if qty > 0 and intent == "legacy":
                        j_open_longs.setdefault(sym, []).append((price, qty, fill_ts, strat))
                elif side == "SLD":
                    if intent == "enter_short":
                        j_open_shorts.setdefault(sym, []).append((price, qty, fill_ts, strat))
                        continue
                    q = j_open_longs.setdefault(sym, [])
                    while qty > 0 and q:
                        ep, eq, et, es = q.pop(-1)
                        m = min(qty, eq)
                        j_closed.append({"pnl": round((price - ep) * m, 2), "strategy": es, "ticker": sym,
                                         "date": date_str, "entry_time": et, "exit_time": fill_ts, "side": "LONG",
                                         "entry_price": ep, "exit_price": price, "qty": m})
                        qty -= m
                        if eq > m: q.append((ep, eq - m, et, es))
                    if qty > 0 and intent == "legacy":
                        j_open_shorts.setdefault(sym, []).append((price, qty, fill_ts, strat))

            # Classify orphans; per-strategy uses clean pairs only
            j_orphans, j_clean = [], []
            for c in j_closed:
                is_orph, _ = _classify_orphan(c)
                (j_orphans if is_orph else j_clean).append(c)

            # Overall total includes orphans (real executions); per-strategy uses clean
            all_j = j_clean + j_orphans
            wins   = [t for t in all_j if t["pnl"] > 0]
            losses = [t for t in all_j if t["pnl"] <= 0]
            gw, gl = sum(t["pnl"] for t in wins), abs(sum(t["pnl"] for t in losses))
            by_strat  = {}
            by_ticker = {}
            for t in j_clean:
                s = t["strategy"]
                by_strat.setdefault(s, {"pnl": 0, "trades": 0})
                by_strat[s]["pnl"]    = round(by_strat[s]["pnl"] + t["pnl"], 2)
                by_strat[s]["trades"] += 1
                tk = t["ticker"]
                by_ticker.setdefault(tk, {"pnl": 0, "trades": 0, "wins": 0})
                by_ticker[tk]["pnl"]    = round(by_ticker[tk]["pnl"] + t["pnl"], 2)
                by_ticker[tk]["trades"] += 1
                if t["pnl"] > 0: by_ticker[tk]["wins"] += 1
            top = sorted(by_strat.items(), key=lambda x: x[1]["pnl"], reverse=True)
            top_tickers = sorted(by_ticker.items(), key=lambda x: x[1]["pnl"], reverse=True)

            # Day-of-week and time-of-day breakdowns (exit time, ET)
            from zoneinfo import ZoneInfo as _ZI
            _et = _ZI("America/New_York")
            by_day  = {}
            by_hour = {}
            _days   = ["Monday","Tuesday","Wednesday","Thursday","Friday"]
            for t in all_j:
                try:
                    dt_et = _dt2.fromisoformat(
                        (t.get("exit_time") or "").replace("Z", "+00:00")
                    ).astimezone(_et)
                    day = dt_et.strftime("%A")
                    by_day.setdefault(day, {"pnl": 0.0, "trades": 0, "wins": 0})
                    by_day[day]["pnl"]    = round(by_day[day]["pnl"] + t["pnl"], 2)
                    by_day[day]["trades"] += 1
                    if t["pnl"] > 0: by_day[day]["wins"] += 1
                    h, m   = dt_et.hour, 0 if dt_et.minute < 30 else 30
                    bucket = f"{h:02d}:{m:02d}"
                    by_hour.setdefault(bucket, {"pnl": 0.0, "trades": 0, "wins": 0})
                    by_hour[bucket]["pnl"]    = round(by_hour[bucket]["pnl"] + t["pnl"], 2)
                    by_hour[bucket]["trades"] += 1
                    if t["pnl"] > 0: by_hour[bucket]["wins"] += 1
                except Exception:
                    pass

            trade_stats = {
                "trades":        len(all_j),
                "wins":          len(wins),
                "losses":        len(losses),
                "win_rate":      round(len(wins) / len(all_j) * 100, 1) if all_j else 0,
                "total_pnl":     round(gw - gl, 2),
                "profit_factor": round(gw / gl, 2) if gl > 0 else None,
                "best_strategy": top[0][0]  if top else None,
                "best_pnl":      top[0][1]["pnl"] if top else 0,
                "worst_strategy":top[-1][0] if len(top) > 1 else None,
                "worst_pnl":     top[-1][1]["pnl"] if len(top) > 1 else 0,
                "per_strategy":  {k: v for k, v in top},
                "tickers":       sorted(set(t["ticker"] for t in all_j)),
                "per_ticker":    {k: v for k, v in top_tickers},
                "by_day":        {d: by_day[d] for d in _days if d in by_day},
                "by_hour":       dict(sorted(by_hour.items())),
            }
    except Exception as _te:
        log.warning("Journal trade stats error: %s", _te)
        trade_stats = {}

    # ── Market data via yfinance ─────────────────────────────────────────
    market_data = {}
    try:
        import yfinance as yf
        symbols = ["SPY", "QQQ", "^VIX"] + trade_stats.get("tickers", [])
        raw = yf.download(symbols, start=from_date,
                          end=str(week_end + _dtmod.timedelta(days=1)),
                          progress=False, auto_adjust=True)
        close = raw["Close"] if "Close" in raw.columns else raw
        for sym in symbols:
            label = sym.replace("^", "")
            col   = sym if sym in close.columns else None
            if col is None: continue
            prices = close[col].dropna()
            if len(prices) < 2: continue
            weekly_ret = round((prices.iloc[-1] / prices.iloc[0] - 1) * 100, 2)
            market_data[label] = {
                "weekly_return": weekly_ret,
                "open":  round(float(prices.iloc[0]),  2),
                "close": round(float(prices.iloc[-1]), 2),
                "high":  round(float(close[col].max()), 2),
                "low":   round(float(close[col].min()),  2),
            }
        # Regime: SPY vs 20-day MA
        spy_hist = yf.download("SPY", period="30d", progress=False, auto_adjust=True)
        spy_close = spy_hist["Close"].dropna() if "Close" in spy_hist else spy_hist.iloc[:, 0].dropna()
        if len(spy_close) >= 20:
            ma20  = float(spy_close.rolling(20).mean().iloc[-1])
            last  = float(spy_close.iloc[-1])
            vix   = market_data.get("VIX", {}).get("close", 20)
            if last > ma20 and vix < 20:
                regime = "Trending / Low Volatility"
            elif last > ma20 and vix >= 20:
                regime = "Trending / Elevated Volatility"
            elif last <= ma20 and vix >= 25:
                regime = "Risk-Off"
            else:
                regime = "Mixed / Choppy"
            market_data["regime"] = regime
            market_data["spy_vs_ma20"] = round(last - ma20, 2)
    except Exception as _me:
        log.warning("Journal market data error: %s", _me)

    # ── Account equity → system return % ────────────────────────────────
    if trade_stats and trade_stats.get('total_pnl') is not None:
        try:
            _ae_broker = alpaca_broker2 if use_acct2 else alpaca_broker
            if _ae_broker is not None:
                _ae_broker._ensure_client()
                _ae_acct   = _ae_broker._trading.get_account()
                _ae_equity = float(getattr(_ae_acct, 'equity', 0) or 0)
                if _ae_equity > 0:
                    trade_stats['account_equity'] = round(_ae_equity, 2)
                    trade_stats['system_ret_pct'] = round(
                        trade_stats['total_pnl'] / _ae_equity * 100, 3
                    )
        except Exception as _ae:
            log.debug("Journal equity fetch error: %s", _ae)

    # ── Compute grade now (only needs trade_stats, not AI output) ────────
    try:
        _pnl = float((trade_stats or {}).get('total_pnl') or 0)
        _wr  = float((trade_stats or {}).get('win_rate')  or 0)
        _pf  = float((trade_stats or {}).get('profit_factor') or 0)
        if   _pnl > 0 and _wr >= 60 and _pf >= 1.5: _init_grade = 'A'
        elif _pnl > 0 and (_wr >= 50 or _pf >= 1.0): _init_grade = 'B'
        elif _pnl > -100: _init_grade = 'C'
        else:              _init_grade = 'D'
    except Exception:
        _init_grade = 'C'
    _init_tags_json = json.dumps({"grade": _init_grade, "labels": []})

    # ── Persist stats + stream AI summary ───────────────────────────────
    p = placeholder()
    conn = get_db()
    cur  = conn.cursor()
    now_str = _dtmod.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    # Upsert entry so regeneration replaces old content; save grade now so it
    # shows immediately even if the stream's label UPDATE fails later.
    if DATABASE_URL:
        cur.execute(
            f"INSERT INTO journal_entries (week, generated_at, trade_stats, market_data, ai_summary, user_notes, tags) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p}) "
            f"ON CONFLICT (week) DO UPDATE SET generated_at={p}, trade_stats={p}, market_data={p}, ai_summary='', tags={p}",
            (week, now_str, json.dumps(trade_stats), json.dumps(market_data), "", "", _init_tags_json,
             now_str, json.dumps(trade_stats), json.dumps(market_data), _init_tags_json),
        )
    else:
        cur.execute(
            "INSERT OR REPLACE INTO journal_entries (week, generated_at, trade_stats, market_data, ai_summary, user_notes, tags) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p})",
            (week, now_str, json.dumps(trade_stats), json.dumps(market_data), "", "", _init_tags_json),
        )
    conn.commit()
    conn.close()

    # ── Prior week's journal for progress comparison ────────────────────────
    # Look up the entry for the ISO week immediately before this one so the AI
    # can frame this week as progress / regression against the last entry.
    prior_summary = ""
    prior_stats   = {}
    try:
        prev_week_start = week_start - _dtmod.timedelta(days=7)
        # Use ISO week to match the storage format set by <input type="week">.
        _piso           = prev_week_start.isocalendar()
        prev_week_key   = f"{_piso[0]}-W{_piso[1]:02d}"
        _pconn = get_db()
        _pcur  = _pconn.cursor()
        _pcur.execute(
            f"SELECT ai_summary, trade_stats FROM journal_entries WHERE week={placeholder()}",
            (prev_week_key,),
        )
        _prow = _pcur.fetchone()
        _pconn.close()
        if _prow:
            prior_summary = (_prow[0] if DATABASE_URL else _prow["ai_summary"]) or ""
            _prior_raw    = (_prow[1] if DATABASE_URL else _prow["trade_stats"]) or "{}"
            try:
                prior_stats = json.loads(_prior_raw) if isinstance(_prior_raw, str) else (_prior_raw or {})
            except Exception:
                prior_stats = {}
    except Exception as _pe:
        log.warning("Journal prior-week lookup failed: %s", _pe)

    # Build AI prompt
    ts   = trade_stats
    md   = market_data
    spy  = md.get("SPY", {})
    qqq  = md.get("QQQ", {})
    vix  = md.get("VIX", {})

    # Sort strategies by P&L for top/bottom 5
    all_strats = sorted(ts.get("per_strategy", {}).items(), key=lambda x: x[1]["pnl"], reverse=True)
    top5    = all_strats[:5]
    bot5    = all_strats[-5:] if len(all_strats) > 5 else []
    top5_pnl = round(sum(v["pnl"] for _, v in top5), 2)
    bot5_pnl = round(sum(v["pnl"] for _, v in bot5), 2)
    spy_ret  = spy.get("weekly_return", 0) or 0
    qqq_ret  = qqq.get("weekly_return", 0) or 0

    account_label = "Alpaca Refined (paper)" if use_acct2 else "Alpaca Paper All"
    prompt = (
        f"You are a trading coach reviewing a systematic trader's weekly performance journal.\n\n"
        f"Week: {week} ({from_date} to {to_date}) · Account: {account_label}\n\n"
        f"MARKET CONTEXT:\n"
        f"  SPY: {spy_ret:+.2f}% (open ${spy.get('open','?')} → close ${spy.get('close','?')})\n"
        f"  QQQ: {qqq_ret:+.2f}%\n"
        f"  VIX: {vix.get('open','?')} → {vix.get('close','?')}\n"
        f"  Regime: {md.get('regime', 'Unknown')}\n\n"
        f"TICKERS TRADED THIS WEEK:\n"
    )
    for sym in ts.get("tickers", []):
        td = md.get(sym, {})
        prompt += f"  {sym}: {td.get('weekly_return', 'N/A'):+.2f}% (open ${td.get('open','?')} → close ${td.get('close','?')})\n" \
                  if isinstance(td.get('weekly_return'), (int, float)) else f"  {sym}: N/A\n"

    prompt += (
        f"\nOVERALL PERFORMANCE:\n"
        f"  {ts.get('trades', 0)} trades · Win rate {ts.get('win_rate', 0)}% · "
        f"Total P&L ${ts.get('total_pnl', 0):+.2f} · PF {ts.get('profit_factor') or '—'}\n\n"
        f"TOP 5 PERFORMERS (combined P&L ${top5_pnl:+.2f} vs SPY {spy_ret:+.2f}% / QQQ {qqq_ret:+.2f}%):\n"
    )
    for strat, sv in top5:
        ticker = strat.split('_')[1] if '_' in strat else ''
        tk_ret = md.get(ticker, {}).get('weekly_return', 'N/A')
        tk_str = f" | {ticker} stock {tk_ret:+.2f}%" if isinstance(tk_ret, (int, float)) else ""
        prompt += f"  {strat}: {sv['trades']} trades · ${sv['pnl']:+.2f}{tk_str}\n"

    if bot5:
        prompt += f"\nBOTTOM 5 PERFORMERS (combined P&L ${bot5_pnl:+.2f}):\n"
        for strat, sv in reversed(bot5):
            ticker = strat.split('_')[1] if '_' in strat else ''
            tk_ret = md.get(ticker, {}).get('weekly_return', 'N/A')
            tk_str = f" | {ticker} stock {tk_ret:+.2f}%" if isinstance(tk_ret, (int, float)) else ""
            prompt += f"  {strat}: {sv['trades']} trades · ${sv['pnl']:+.2f}{tk_str}\n"

    # Prior-week reference block — gives the AI material to assess week-over-week progression.
    if prior_summary or prior_stats:
        prev_pnl   = prior_stats.get("total_pnl", 0)
        prev_wr    = prior_stats.get("win_rate", 0)
        prev_tr    = prior_stats.get("trades", 0)
        prev_pf    = prior_stats.get("profit_factor")
        prev_best  = prior_stats.get("best_strategy")
        prev_worst = prior_stats.get("worst_strategy")
        prompt += (
            f"\nPRIOR WEEK ({prev_week_key}) FOR COMPARISON:\n"
            f"  {prev_tr} trades · WR {prev_wr}% · P&L ${prev_pnl:+.2f} · PF {prev_pf or '—'}\n"
            f"  Best: {prev_best or '—'} · Worst: {prev_worst or '—'}\n"
        )
        if prior_summary.strip():
            # Trim to keep token usage bounded; the AI mainly needs themes, not full text.
            _trimmed = prior_summary.strip()
            if len(_trimmed) > 1500:
                _trimmed = _trimmed[:1500] + "…"
            prompt += f"  Last week's journal:\n    {_trimmed}\n"

    prompt += (
        f"\nWrite a weekly trading journal entry with these FIVE sections. Use the exact headers shown:\n\n"
        f"**Market Regime & Setup Availability**\n"
        f"Was the regime favorable for Camarilla breakout/reversal strategies on 5-min bars? "
        f"Reference VIX level and SPY/QQQ direction specifically.\n\n"
        f"**Top 5 Analysis**\n"
        f"Combined P&L was ${top5_pnl:+.2f}. Write one line per strategy using this exact format:\n"
        f"**TICKER** (P&L · stock return): one sentence — alpha capture or regime-driven?\n"
        f"State whether each strategy captured alpha beyond what the stock did, or just rode the stock move.\n\n"
        f"**Bottom 5 Analysis**\n"
        f"Combined P&L was ${bot5_pnl:+.2f}. Write one line per strategy using this exact format:\n"
        f"**TICKER** (P&L · stock return): one sentence — regime loss or structural miss?\n"
        f"State whether each loss was the stock moving against the strategy (regime) or the stock was up but strategy still lost (structural miss).\n\n"
        f"**Progress vs Last Week**\n"
        + (
            "Compare this week's numbers (trades, win rate, P&L, PF) to last week's. Did the prior 'Next Week Watchlist' "
            "items play out — strategies you flagged to pause or watch, did they actually improve or stay weak? "
            "Call out one thing that's clearly trending better and one that's clearly trending worse.\n\n"
            if (prior_summary or prior_stats) else
            "No prior-week entry exists yet for direct comparison. Note this as the baseline week and state two metrics "
            "you'll watch next week to gauge progress.\n\n"
        ) +
        f"**Next Week Watchlist**\n"
        f"One or two specific things to monitor. Which of the bottom 5 should be paused vs given another week?\n\n"
        f"Be direct and specific. No fluff. Keep each section to 2-3 sentences. Write in second person.\n\n"
        f"After your four sections add one line in exactly this format:\n"
        f"TAGS: tag1, tag2, tag3\n"
        f"Choose exactly 2-3 tags from this fixed vocabulary only — do not invent new tags:\n"
        f"  Character (pick exactly 1): trending, choppy, ranging\n"
        f"  Volatility (pick exactly 1): high-vol, low-vol\n"
        f"  Context (pick 0 or 1 only if clearly applicable): fed-week, opex, earnings-heavy\n"
        f"Example: TAGS: trending, high-vol, fed-week"
    )

    def _stream():
        client  = _anthropic.Anthropic(api_key=api_key)
        summary = ""
        try:
            with client.messages.stream(
                model="claude-haiku-4-5-20251001",
                max_tokens=1100,
                messages=[{"role": "user", "content": prompt}],
            ) as stream:
                for text in stream.text_stream:
                    summary += text
                    yield f"data: {json.dumps({'text': text})}\n\n"
        except Exception as _ae:
            yield f"data: {json.dumps({'error': str(_ae)})}\n\n"
            return
        # Extract TAGS line, strip from displayed summary, compute grade
        import re as _re
        _tag_match = _re.search(r'TAGS:\s*(.+?)(?:\n|$)', summary, _re.IGNORECASE)
        _labels = []
        _clean_summary = summary
        if _tag_match:
            _labels = [_re.sub(r'[^a-z0-9-]', '', t.strip().lower()) for t in _tag_match.group(1).split(',') if t.strip()]
            _labels = [l for l in _labels if l]  # drop empty after sanitize
            _clean_summary = summary[:_tag_match.start()].rstrip()

        # Grade is pre-computed from trade_stats before the stream
        _grade = _init_grade

        _tags_json = json.dumps({"grade": _grade, "labels": _labels})

        # Send grade, labels, and clean summary in the done payload so the
        # client can POST them back as a reliable separate HTTP request.
        yield f"data: {json.dumps({'done': True, 'grade': _grade, 'labels': _labels, 'summary': _clean_summary})}\n\n"

    return Response(stream_with_context(_stream()), mimetype="text/event-stream",
                    headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"})


@app.route("/api/journal/trade_review", methods=["POST"])
def api_trade_review():
    """Generate AI stop-tightness analysis using post-exit price action.
    Streams SSE. Fetches Refined fills for the week, gets 30-min bars after
    each exit, then asks Claude to identify premature exit patterns and suggest
    parameter changes."""
    import datetime as _dtmod
    import re as _re

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 503
    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({"error": "anthropic package not installed"}), 503

    data = request.get_json(silent=True) or {}
    week = data.get("week", "").strip()
    if not week:
        today = _dtmod.date.today()
        week  = today.strftime("%Y-W%W")
    try:
        year, wnum = week.split("-W")
        week_start = _dtmod.date.fromisocalendar(int(year), int(wnum), 1)
        week_end   = week_start + _dtmod.timedelta(days=4)
    except Exception:
        today = _dtmod.date.today()
        week_start = today - _dtmod.timedelta(days=today.weekday())
        week_end   = week_start + _dtmod.timedelta(days=4)
    from_date = str(week_start)
    to_date   = str(week_end)

    def _fetch_post_exit_bars(ticker, exit_time_str):
        """Return (max_fav_pct, max_adv_pct) in 30 min after exit.
        max_fav = best price in exit direction as % of exit price (positive = price continued profitably)
        max_adv = worst price in exit direction as % (negative = price moved against trade)"""
        try:
            from alpaca.data.historical import StockHistoricalDataClient
            from alpaca.data.requests import StockBarsRequest
            from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
            import datetime as _dt

            _client = StockHistoricalDataClient(
                api_key    = alpaca_broker._key,
                secret_key = alpaca_broker._secret,
            )
            _exit_dt = _dt.datetime.fromisoformat(exit_time_str.replace("Z", "+00:00"))
            _end_dt  = _exit_dt + _dt.timedelta(minutes=30)
            req = StockBarsRequest(
                symbol_or_symbols = ticker.upper(),
                timeframe         = TimeFrame(1, TimeFrameUnit.Minute),
                start             = _exit_dt,
                end               = _end_dt,
            )
            bars_df = _client.get_stock_bars(req)
            bars = list(bars_df[ticker.upper()])
            if not bars:
                return None, None
            highs  = [float(b.high)  for b in bars]
            lows   = [float(b.low)   for b in bars]
            return max(highs), min(lows)
        except Exception as _be:
            log.debug("post_exit_bars %s: %s", ticker, _be)
            return None, None

    def _stream():
        try:
            yield from _stream_inner()
        except Exception as _ex:
            log.exception("trade_review _stream crash: %s", _ex)
            yield f"data: {json.dumps({'error': f'Internal error: {_ex}'})}\n\n"

    def _stream_inner():
        if not alpaca_broker2:
            yield f"data: {json.dumps({'error': 'Refined account not configured'})}\n\n"
            return

        fills = _get_cached_fills_2()
        week_fills = [f for f in fills if from_date <= (f.get("time") or "")[:10] <= to_date]
        if not week_fills:
            yield f"data: {json.dumps({'error': 'No Refined fills found for this week'})}\n\n"
            return

        # Pair into round-trips
        paired = _pair_alpaca_fills_lifo(week_fills)
        trades = paired.get("closed_clean") or []
        if not trades:
            yield f"data: {json.dumps({'error': 'No completed round-trips found'})}\n\n"
            return

        yield f"data: {json.dumps({'status': f'Fetching post-exit data for {len(trades)} trades…'})}\n\n"

        # Build trade records with post-exit price action
        records = []
        for t in trades:
            ticker   = (t.get("ticker") or t.get("symbol") or "").upper()
            side     = t.get("side") or ""
            entry_px = float(t.get("entry_price") or 0)
            exit_px  = float(t.get("exit_price")  or 0)
            pnl      = float(t.get("pnl") or 0)
            exit_t   = t.get("exit_time") or t.get("time") or ""
            strategy = (t.get("strategy") or "Unknown")

            is_long = side.upper() in ("BOT", "LONG", "BUY")

            # Duration in minutes
            try:
                _ent_t = _dtmod.datetime.fromisoformat((t.get("entry_time") or exit_t).replace("Z", "+00:00"))
                _ex_t  = _dtmod.datetime.fromisoformat(exit_t.replace("Z", "+00:00"))
                duration_mins = round((_ex_t - _ent_t).total_seconds() / 60, 1)
                exit_hour_et  = (_ex_t.astimezone(_dtmod.timezone(_dtmod.timedelta(hours=-4)))).strftime("%H:%M")
            except Exception:
                duration_mins = None
                exit_hour_et  = exit_t[11:16] if len(exit_t) > 15 else "?"

            if not ticker or not exit_t or exit_px == 0:
                continue

            max_high, min_low = _fetch_post_exit_bars(ticker, exit_t)

            # Compute post-exit continuation as % of exit price
            if max_high and min_low and exit_px > 0:
                if is_long:
                    # For longs: favorable = price went higher after exit
                    post_fav_pct  = round((max_high - exit_px) / exit_px * 100, 3)
                    post_adv_pct  = round((min_low  - exit_px) / exit_px * 100, 3)
                else:
                    # For shorts: favorable = price went lower after exit
                    post_fav_pct  = round((exit_px - min_low)  / exit_px * 100, 3)
                    post_adv_pct  = round((exit_px - max_high) / exit_px * 100, 3)
                premature = post_fav_pct > 0.15  # price continued >0.15% after exit
            else:
                post_fav_pct = post_adv_pct = None
                premature = None

            records.append({
                "strategy":     strategy,
                "ticker":       ticker,
                "side":         "LONG" if is_long else "SHORT",
                "exit_time_et": exit_hour_et,
                "duration_min": duration_mins,
                "entry_px":     round(entry_px, 2),
                "exit_px":      round(exit_px,  2),
                "pnl":          round(pnl, 2),
                "post_fav_pct": post_fav_pct,
                "post_adv_pct": post_adv_pct,
                "premature":    premature,
            })

        if not records:
            yield f"data: {json.dumps({'error': 'Could not build trade records'})}\n\n"
            return

        premature_count = sum(1 for r in records if r["premature"])
        premature_pct   = round(premature_count / len(records) * 100) if records else 0

        yield f"data: {json.dumps({'status': f'Analysing {len(records)} trades ({premature_pct}% flagged as premature exits)…'})}\n\n"

        # Build compact trade table for the prompt
        def _fmt_pct(v):
            return f"{v:+.2f}%" if v is not None else "n/a"

        def _fmt_dur(v):
            return f"{v}m" if v is not None else "?m"

        trade_rows = "\n".join(
            f"{r['ticker']}|{r['strategy'].replace('_CAM_','_').replace('_V02_5MIN','')}|"
            f"{r['side']}|{r['exit_time_et']}|{_fmt_dur(r['duration_min'])}|"
            f"${r['pnl']:+.2f}|"
            f"{_fmt_pct(r['post_fav_pct'])}|"
            f"{'⚠ EARLY' if r['premature'] else 'ok'}"
            for r in records
        )

        # Extract TYPE_LEVEL substrings that will actually match rule names (e.g. BREAKOUT_R4S4).
        # Rule names follow TICKER_CAM_TYPE_LEVEL_VERSION_TF — we want TYPE_LEVEL so the AI
        # generates patterns that work as name_contains substrings.
        _strat_parts = set()
        for _r in records:
            _parts = _r['strategy'].upper().split('_')
            try:
                _ci = _parts.index('CAM')
                if _ci + 2 < len(_parts):
                    _strat_parts.add(f"{_parts[_ci+1]}_{_parts[_ci+2]}")
                elif _ci + 1 < len(_parts):
                    _strat_parts.add(_parts[_ci+1])
            except ValueError:
                pass
        _strat_hint = ", ".join(sorted(_strat_parts)) if _strat_parts else "BREAKOUT_R4S4, REVERSAL_R3S3"

        # Read current exit_params trail values per pattern from live routing rules
        _rule_trails: dict = {}
        try:
            _rc = get_db()
            _rrows = _rc.execute("SELECT name, nodes FROM routing_rules WHERE enabled=1").fetchall()
            for _rrow in _rrows:
                _rname  = (_rrow['name'] or '').upper()
                _rnodes = json.loads(_rrow['nodes'] or '[]')
                _rparts = _rname.split('_')
                try:
                    _rci = _rparts.index('CAM')
                    _rpat = f"{_rparts[_rci+1]}_{_rparts[_rci+2]}" if _rci + 2 < len(_rparts) else None
                except (ValueError, IndexError):
                    _rpat = None
                if not _rpat:
                    continue
                for _nd in _rnodes:
                    if _nd.get('type') == 'exit_params' and _nd.get('trail_offset'):
                        _rule_trails.setdefault(_rpat, []).append(float(_nd['trail_offset']))
                        break
        except Exception:
            pass
        _trail_context = "; ".join(
            f"{pat}={sum(vals)/len(vals):.3g}% ({len(vals)} rules)"
            for pat, vals in sorted(_rule_trails.items())
        ) if _rule_trails else "not available"

        _morn_now = MORNING_TRAIL_PCT
        _aftn_now = AFTERNOON_TRAIL_PCT
        prompt = (
            f"You are reviewing {len(records)} trades from a Camarilla 5-minute breakout/reversal system "
            f"for the week {from_date} to {to_date}.\n\n"
            f"The system uses Kairos broker-side trailing stops with an optional trigger before the trail activates. "
            f"TV signals are the fallback exit. "
            f"Position sizes: SPY/QQQ ~16 shares, cheaper stocks ~50 shares.\n\n"
            f"CURRENT ROUTING RULE STATE (live values — base your recommendations on these):\n"
            f"  exit_params trail_offset per pattern: {_trail_context}\n"
            f"  morning_trail={_morn_now}% (floor, 9:30-10:30 ET): applied as max(rule_trail, morning_trail). "
            f"Has NO effect unless GREATER than the rule's trail_offset.\n"
            f"  afternoon_trail={_aftn_now}% (ceiling, 12:00-close ET): applied as min(rule_trail, afternoon_trail). "
            f"Has NO effect unless LESS than the rule's trail_offset.\n"
            f"  Constraint: morning_trail must exceed the baseline trail; afternoon_trail must be below it.\n\n"
            f"For each trade, POST_FAV shows how much price continued in the profitable direction "
            f"in the 30 minutes after exit (positive = left money on the table, ⚠ EARLY = >0.15% continuation). "
            f"POST_ADV shows the worst price in 30 min (negative = exit was well-timed).\n\n"
            f"TICKER | STRATEGY | SIDE | EXIT_TIME | DURATION | PNL | POST_FAV_30M | FLAG\n"
            f"{trade_rows}\n\n"
            f"SUMMARY: {len(records)} trades, {premature_count} flagged as early exits ({premature_pct}%).\n\n"
            f"Analyse this data and provide:\n"
            f"1. **Stop Tightness Assessment** — is the trail too tight, too wide, or well-calibrated? "
            f"Quote specific trades as evidence.\n"
            f"2. **Time-of-Day Patterns** — which exit times (9:30-10:30, 10:30-12, afternoon) show the most "
            f"premature exits? Any windows to avoid or widen the trail?\n"
            f"3. **Per-Strategy Findings** — which strategies exit too early most consistently?\n"
            f"4. **Specific Recommendations** — give exact parameter values to try next week. "
            f"For session trails, remember the floor/ceiling constraint: morning must exceed your recommended baseline "
            f"trail to have effect; afternoon must be below it.\n\n"
            f"Be direct and data-driven. 3-4 sentences per section max.\n\n"
            f"After your analysis, output ONE line in exactly this format (no extra text, no markdown):\n"
            f"CHANGES_JSON: {{\"trail_rules\":[{{\"pattern\":\"X\",\"trail\":N}},...],\"morning_trail\":N,\"afternoon_trail\":N,\"clear_triggers\":true}}\n"
            f"Rules: pattern must be a TYPE_LEVEL substring WITH underscore matching routing rule names "
            f"(e.g. BREAKOUT_R4S4, REVERSAL_R3S3 — never strip underscores or they match nothing). "
            f"Available patterns: {_strat_hint}. "
            f"trail is the recommended new baseline float % for that pattern (current values: {_trail_context}). "
            f"morning_trail: set ABOVE your recommended baseline trail to widen morning entries — "
            f"e.g. if recommending baseline=0.30%, morning_trail must be >0.30% to activate. Set 0 if not useful. "
            f"afternoon_trail: set BELOW your recommended baseline trail to tighten afternoon entries — "
            f"e.g. if recommending baseline=0.30%, afternoon_trail must be <0.30% to activate. Set 0 if not useful. "
            f"clear_triggers is true only if you recommend removing all trail triggers. "
            f"Only include trail_rules entries for patterns you are actually recommending a change for."
        )

        summary = ""
        try:
            client = _anthropic.Anthropic(api_key=api_key)
            with client.messages.stream(
                model      = "claude-haiku-4-5-20251001",
                max_tokens = 1500,
                messages   = [{"role": "user", "content": prompt}],
            ) as stream:
                for text in stream.text_stream:
                    summary += text
                    yield f"data: {json.dumps({'text': text})}\n\n"
        except Exception as _ae:
            yield f"data: {json.dumps({'error': str(_ae)})}\n\n"
            return

        # Extract CHANGES_JSON block from summary; strip it from the displayed text
        changes = {}
        _changes_match = _re.search(r'CHANGES_JSON:\s*(\{.+\})', summary)
        if _changes_match:
            try:
                changes = json.loads(_changes_match.group(1))
            except Exception:
                pass
            summary = summary[:_changes_match.start()].rstrip()

        yield f"data: {json.dumps({'done': True, 'changes': changes})}\n\n"

        # Persist to settings keyed by week
        try:
            _save_setting(f"TRADE_REVIEW_{week}", json.dumps({
                "summary":       summary,
                "changes":       changes,
                "week":          week,
                "trade_count":   len(records),
                "premature_pct": premature_pct,
                "generated_at":  _dtmod.datetime.now(_dtmod.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            }))
        except Exception as _pe:
            log.warning("Trade review persist error: %s", _pe)

    return Response(stream_with_context(_stream()), mimetype="text/event-stream",
                    headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"})


@app.route("/api/journal/trade_review/<week>", methods=["GET"])
def api_get_trade_review(week):
    """Return cached trade review for a week."""
    stored = _load_setting(f"TRADE_REVIEW_{week}")
    if stored:
        try:
            return jsonify(json.loads(stored))
        except Exception:
            pass
    return jsonify({"summary": None})


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/simulate")
def simulate():
    return render_template("simulate.html")


def _resolve_strategy_trail(strategy_name: str, overrides: dict, default_trail: float) -> float:
    """Return the per-strategy trail override if one matches, else default_trail."""
    if not overrides or not strategy_name:
        return default_trail
    sname = strategy_name.upper()
    for label, trail in overrides.items():
        key = str(label).upper().replace(' ', '_')
        if key and key in sname:
            return float(trail)
    return default_trail


def _get_tiered_trail(peak_gain_pct: float, tiers: list, base_trail: float) -> float:
    """Return the effective trail % for the current peak gain using sorted tier list.
    Tiers: [(gain_threshold_pct, trail_pct), ...] sorted ascending.
    Each tier activates when peak_gain_pct >= threshold, overriding the previous.
    """
    effective = base_trail
    for threshold, trail in tiers:
        if peak_gain_pct >= threshold:
            effective = trail
        else:
            break
    return effective


def _resolve_strategy_trigger(strategy_name: str, overrides: dict, default_trigger: float) -> float:
    """Return the per-strategy trigger override if one matches, else default_trigger."""
    if not overrides or not strategy_name:
        return default_trigger
    sname = strategy_name.upper()
    for label, trigger in overrides.items():
        key = str(label).upper().replace(' ', '_')
        if key and key in sname:
            return float(trigger)
    return default_trigger


def _apply_session_trail(trail_pct: float, entry_dt) -> float:
    """Apply morning/afternoon session trail overrides to an effective trail %.
    Morning  9:30–10:30 ET: max(trail, MORNING_TRAIL_PCT)   — widens (floor)
    Afternoon 12:00–close ET: min(trail, AFTERNOON_TRAIL_PCT) — tightens (ceiling)
    """
    try:
        import zoneinfo as _zi
        et = entry_dt.astimezone(_zi.ZoneInfo("America/New_York"))
        t  = et.hour * 60 + et.minute  # minutes since midnight ET
        if MORNING_TRAIL_PCT > 0 and 570 <= t < 630:   # 9:30–10:30
            return max(trail_pct, MORNING_TRAIL_PCT)
        if AFTERNOON_TRAIL_PCT > 0 and t >= 720:        # 12:00+
            return min(trail_pct, AFTERNOON_TRAIL_PCT)
    except Exception:
        pass
    return trail_pct


@app.route("/api/simulate_stops", methods=["POST"])
def api_simulate_stops():
    """Replay historical fills with simulated stop parameters.
    Runs two simulations per trade:
      - baseline: current Signal Router rule trail settings + rule qty
      - new:      user-supplied parameters + rule qty
    Body: {from_date, to_date, account, trail_pct, trigger_pct, stop_loss_pct, max_hold_mins}
    """
    import concurrent.futures as _cf
    import datetime as _dt

    body          = request.get_json(force=True) or {}
    from_date     = body.get("from_date", "")
    to_date       = body.get("to_date",   "")
    use_acct2     = str(body.get("account", "2")) == "2"
    trail_pct          = float(body.get("trail_pct",         0.5))
    trigger_pct        = float(body.get("trigger_pct",       0.0))
    stop_loss_pct      = float(body.get("stop_loss_pct",     0.0))
    stop_loss_dollars   = float(body.get("stop_loss_dollars",  0.0))
    take_profit_pct     = float(body.get("take_profit_pct",     0.0))
    take_profit_dollars = float(body.get("take_profit_dollars", 0.0))
    max_hold_mins       = int(body.get("max_hold_mins",       60))
    skip_tv_exits       = bool(body.get("skip_tv_exits",      False))
    strategy_overrides          = body.get("strategy_overrides",         {})
    strategy_trigger_overrides  = body.get("strategy_trigger_overrides", {})
    # Dynamic trail tiers: [{gain: float, trail: float}, ...] sorted ascending by gain
    _tiers_raw  = body.get("trail_tiers", [])
    trail_tiers = None
    if _tiers_raw:
        _parsed = []
        for t in _tiers_raw:
            try:
                _g = float(t.get("gain",  0))
                _t = float(t.get("trail", 0))
                if _t > 0:
                    _parsed.append((_g, _t))
            except (TypeError, ValueError):
                pass
        if _parsed:
            trail_tiers = sorted(_parsed, key=lambda x: x[0])

    broker = alpaca_broker2 if use_acct2 else alpaca_broker
    if broker is None:
        return jsonify({"error": "Alpaca account not configured"}), 400
    if not from_date or not to_date:
        return jsonify({"error": "from_date and to_date are required"}), 400

    # ── Load current Signal Router rule settings ─────────────────────────
    # Maps strategy_name.upper() → {trail_pct, trigger_pct, qty}
    rule_settings = {}
    try:
        _rc   = get_db()
        _p    = placeholder()
        _rows = _rc.execute("SELECT name, nodes FROM routing_rules WHERE enabled=1").fetchall()
        for _row in _rows:
            _rname  = (_row[0] or "").upper()
            _rnodes = json.loads(_row[1] or "[]")
            _trail = _trigger = _qty = _mhm = None
            for _nd in _rnodes:
                if _nd.get("type") == "exit_params":
                    _trail   = float(_nd.get("trail_offset") or 0) or None
                    _trigger = float(_nd.get("trail_trigger") or 0)
                    _mhm_raw = _nd.get("max_hold_mins")
                    _mhm     = int(float(_mhm_raw)) if _mhm_raw else None
                if _nd.get("type") == "quantity":
                    _qty = float(_nd.get("amount") or 0) or None
            if _trail is not None:
                rule_settings[_rname] = {
                    "trail_pct":    _trail,
                    "trigger_pct":  _trigger or 0.0,
                    "qty":          _qty,
                    "max_hold_mins": _mhm,
                }
        _rc.close()
    except Exception as _re:
        log.warning("Rule settings lookup failed: %s", _re)
    log.info("Simulate rule_settings: %d rules loaded: %s",
             len(rule_settings),
             {k: v.get("trail_pct") for k, v in rule_settings.items()})

    fills         = _get_cached_fills_2() if use_acct2 else _get_cached_fills()
    signal_lookup = _build_signal_lookup_for_alpaca()
    paired        = _pair_alpaca_fills_lifo(fills, from_date=from_date, to_date=to_date,
                                            signal_lookup=signal_lookup)
    trades = paired["closed_clean"]
    if not trades:
        return jsonify({"error": "No completed round-trips found for the selected period"}), 404

    # Batch bar fetches by (ticker, date) — one API call per ticker/day
    ticker_dates = set()
    for t in trades:
        ticker = (t.get("ticker") or "").upper()
        date   = (t.get("entry_time") or "")[:10]
        if ticker and date:
            ticker_dates.add((ticker, date))

    day_bars = {}
    with _cf.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_day_bars, tk, dt): (tk, dt) for tk, dt in ticker_dates}
        for f in _cf.as_completed(futures):
            tk, dt = futures[f]
            try:
                day_bars[(tk, dt)] = f.result()
            except Exception:
                day_bars[(tk, dt)] = []

    def _pnl(exit_price, entry_px, qty, side):
        return round((exit_price - entry_px) * qty, 2) if side == "LONG" \
               else round((entry_px - exit_price) * qty, 2)

    results = []
    for t in trades:
        ticker     = (t.get("ticker") or "").upper()
        side       = (t.get("side")   or "").upper()
        entry_px   = float(t.get("entry_price") or 0)
        exit_px    = float(t.get("exit_price")  or 0)
        fill_qty   = float(t.get("qty")         or 1)
        actual_pnl = float(t.get("pnl")         or 0)
        entry_time = t.get("entry_time") or ""
        exit_time  = t.get("exit_time")  or ""
        strategy   = t.get("strategy")   or ""
        date       = entry_time[:10]

        if not ticker or not entry_time or entry_px == 0:
            continue

        try:
            entry_dt = _dt.datetime.fromisoformat(entry_time.replace("Z", "+00:00"))
        except Exception:
            continue

        # Always use actual fill qty — rule qty is for live sizing, not P&L replay
        qty = fill_qty
        # Exact match first; fall back to substring match (same logic as _resolve_strategy_trail)
        rule = rule_settings.get(strategy.upper())
        if rule is None:
            sname = strategy.upper().replace(' ', '_')
            for rkey, rval in rule_settings.items():
                if '_CAM_' in rkey:
                    parts = rkey.split('_CAM_')[1].split('_')
                    pattern = '_'.join(parts[:2])  # e.g. BREAKOUT_R3S3
                else:
                    pattern = rkey
                if pattern and pattern in sname:
                    rule = rval
                    break
        rule         = rule or {}
        r_trail      = rule.get("trail_pct",    trail_pct)
        r_trigger    = rule.get("trigger_pct",  0.0)
        r_max_hold   = rule.get("max_hold_mins") or 0  # locked to Signal Router node; 0 = no limit

        # Parse actual exit datetime for the time cap
        try:
            actual_exit_dt = _dt.datetime.fromisoformat(exit_time.replace("Z", "+00:00")) \
                             if exit_time else None
        except Exception:
            actual_exit_dt = None

        # Per-strategy trail/trigger overrides for new params (fall back to global values)
        new_trail   = _resolve_strategy_trail(strategy, strategy_overrides, trail_pct)
        new_trigger = _resolve_strategy_trigger(strategy, strategy_trigger_overrides, trigger_pct)

        # Session overrides applied to baseline only — baseline must match live-system behaviour;
        # new-params sim uses the exact values the user entered so the comparison is meaningful.
        eff_r_trail = _apply_session_trail(r_trail, entry_dt)

        bars       = day_bars.get((ticker, date), [])
        trade_bars = [b for b in bars if b.timestamp >= entry_dt]

        # When skip_tv_exits=True run stops freely (no time cap); baseline P&L won't match
        # actual but shows what the stop alone would have done over the full session.
        # When False, cap at actual exit time so baseline ≈ actual P&L.
        _cap_dt    = None      if skip_tv_exits else actual_exit_dt
        _cap_price = None      if skip_tv_exits else exit_px

        base_sim = _simulate_exit(trade_bars, entry_px, side,
                                  eff_r_trail, r_trigger, 0.0,
                                  r_max_hold, entry_dt,
                                  cap_dt=_cap_dt, cap_price=_cap_price,
                                  stop_loss_dollars=0.0, qty=qty)
        base_pnl = _pnl(base_sim["exit_price"], entry_px, qty, side) if base_sim else None

        new_sim  = _simulate_exit(trade_bars, entry_px, side,
                                  new_trail, new_trigger, stop_loss_pct,
                                  max_hold_mins, entry_dt,
                                  cap_dt=_cap_dt, cap_price=_cap_price,
                                  stop_loss_dollars=stop_loss_dollars, qty=qty,
                                  trail_tiers=trail_tiers,
                                  take_profit_pct=take_profit_pct,
                                  take_profit_dollars=take_profit_dollars)
        new_pnl  = _pnl(new_sim["exit_price"], entry_px, qty, side) if new_sim else None

        delta = round(new_pnl - base_pnl, 2) if (new_pnl is not None and base_pnl is not None) else None

        results.append({
            "date":               date,
            "ticker":             ticker,
            "side":               side,
            "strategy":           strategy,
            "qty":                qty,
            "rule_trail_pct":     eff_r_trail,
            "new_trail_pct":      new_trail,
            "entry_price":        entry_px,
            "entry_time":         entry_time,
            "actual_exit_price":  exit_px,
            "actual_exit_time":   exit_time,
            "actual_pnl":         actual_pnl,
            "base_exit_price":    base_sim["exit_price"] if base_sim else None,
            "base_exit_time":     base_sim["exit_time"]  if base_sim else None,
            "base_exit_reason":   base_sim["reason"]     if base_sim else None,
            "base_exit_mins":     base_sim["exit_mins"]  if base_sim else None,
            "base_pnl":           base_pnl,
            "new_exit_price":     new_sim["exit_price"]  if new_sim else None,
            "new_exit_time":      new_sim["exit_time"]   if new_sim else None,
            "new_exit_reason":    new_sim["reason"]      if new_sim else None,
            "new_exit_mins":      new_sim["exit_mins"]   if new_sim else None,
            "new_pnl":            new_pnl,
            "pnl_delta":          delta,
        })

    results.sort(key=lambda r: (r["date"], r["entry_time"]))

    base_total   = round(sum(r["base_pnl"]   for r in results if r["base_pnl"]   is not None), 2)
    new_total    = round(sum(r["new_pnl"]    for r in results if r["new_pnl"]    is not None), 2)
    actual_total = round(sum(r["actual_pnl"] for r in results), 2)
    improved     = sum(1 for r in results if (r["pnl_delta"] or 0) >  0.01)
    worse        = sum(1 for r in results if (r["pnl_delta"] or 0) < -0.01)

    return jsonify({
        "trades":  results,
        "summary": {
            "trade_count": len(results),
            "actual_pnl":  actual_total,
            "base_pnl":    base_total,
            "new_pnl":     new_total,
            "total_delta": round(new_total - base_total, 2),
            "improved":    improved,
            "worse":       worse,
            "neutral":     len(results) - improved - worse,
        },
        "params": {
            "trail_pct":          trail_pct,
            "trigger_pct":        trigger_pct,
            "stop_loss_pct":      stop_loss_pct,
            "stop_loss_dollars":  stop_loss_dollars,
            "take_profit_pct":     take_profit_pct,
            "take_profit_dollars": take_profit_dollars,
            "max_hold_mins":      max_hold_mins,
            "from_date":         from_date,
            "to_date":           to_date,
            "morning_trail_pct":   MORNING_TRAIL_PCT   if MORNING_TRAIL_PCT   > 0 else None,
            "afternoon_trail_pct": AFTERNOON_TRAIL_PCT if AFTERNOON_TRAIL_PCT > 0 else None,
            "skip_tv_exits":       skip_tv_exits,
            "sr_max_hold_mins":    next((v["max_hold_mins"] for v in rule_settings.values() if v.get("max_hold_mins")), None),
        },
    })


@app.route("/api/simulate/sweep", methods=["POST"])
def simulate_sweep():
    """Parameter sweep: grid-search trail/trigger combos, return ranked results."""
    import datetime as _dt
    import concurrent.futures as _cf
    from collections import defaultdict
    body         = request.get_json() or {}
    from_date    = body.get("from_date", "")
    to_date      = body.get("to_date",   "")
    use_acct2    = str(body.get("account", "2")) == "2"
    trail_min    = float(body.get("trail_min",  0.05))
    trail_max    = float(body.get("trail_max",  0.40))
    trail_step   = float(body.get("trail_step", 0.05))
    triggers     = [round(float(x), 4) for x in (body.get("triggers") or [0])]
    stop_dollars = float(body.get("stop_loss_dollars", 0))
    max_hold     = int(body.get("max_hold_mins", 0))
    skip_exits   = bool(body.get("skip_tv_exits", True))
    per_strategy = bool(body.get("per_strategy", False))
    if not from_date or not to_date:
        return jsonify({"error": "from_date and to_date are required"}), 400
    broker = alpaca_broker2 if use_acct2 else alpaca_broker
    if broker is None:
        return jsonify({"error": "Alpaca account not configured"}), 400

    # Build trail grid
    trail_values, v = [], trail_min
    while v <= trail_max + 1e-9:
        trail_values.append(round(v, 4))
        v = round(v + trail_step, 4)

    # Rule settings (same pattern as /api/simulate)
    rule_settings = {}
    try:
        _rc = get_db()
        for _row in _rc.execute("SELECT name, nodes FROM routing_rules WHERE enabled=1").fetchall():
            _rname  = (_row[0] or "").upper()
            _rnodes = json.loads(_row[1] or "[]")
            _trail = _trigger = _mhm = None
            for _nd in _rnodes:
                if _nd.get("type") == "exit_params":
                    _trail   = float(_nd.get("trail_offset") or 0) or None
                    _trigger = float(_nd.get("trail_trigger") or 0)
                    _mhm_raw = _nd.get("max_hold_mins")
                    _mhm     = int(float(_mhm_raw)) if _mhm_raw else None
            if _trail is not None:
                rule_settings[_rname] = {"trail_pct": _trail, "trigger_pct": _trigger or 0.0, "max_hold_mins": _mhm}
        _rc.close()
    except Exception as _re:
        log.debug("Sweep rule_settings: %s", _re)

    def _rule_for(strategy):
        rule = rule_settings.get(strategy.upper())
        if rule is None:
            sname = strategy.upper().replace(' ', '_')
            for rkey, rval in rule_settings.items():
                pattern = '_'.join(rkey.split('_CAM_')[1].split('_')[:2]) if '_CAM_' in rkey else rkey
                if pattern and pattern in sname:
                    rule = rval; break
        return rule or {}

    def _type_level(strategy):
        s = (strategy or "").upper()
        idx = s.find("_CAM_")
        if idx >= 0:
            parts = s[idx+5:].split("_")
            if len(parts) >= 2:
                return f"{parts[0]} {parts[1]}"
        return s or "Unknown"

    # Fetch trades + bars
    fills         = _get_cached_fills_2() if use_acct2 else _get_cached_fills()
    signal_lookup = _build_signal_lookup_for_alpaca()
    paired        = _pair_alpaca_fills_lifo(fills, from_date=from_date, to_date=to_date,
                                            signal_lookup=signal_lookup)
    trades = paired["closed_clean"]
    if not trades:
        return jsonify({"error": "No completed round-trips found for the selected period"}), 404

    ticker_dates = {((t.get("ticker") or "").upper(), (t.get("entry_time") or "")[:10])
                    for t in trades if t.get("ticker") and t.get("entry_time")}
    day_bars = {}
    with _cf.ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(_fetch_day_bars, tk, dt): (tk, dt) for tk, dt in ticker_dates}
        for f in _cf.as_completed(futs):
            tk, dt = futs[f]
            try:    day_bars[(tk, dt)] = f.result()
            except: day_bars[(tk, dt)] = []

    def _pnl(exit_price, entry_px, qty, side):
        return round((exit_price - entry_px) * qty, 2) if side == "LONG" \
               else round((entry_px - exit_price) * qty, 2)

    # Pre-process: parse datetimes, slice bars, compute SR baseline once
    prepared = []
    for t in trades:
        ticker     = (t.get("ticker") or "").upper()
        side       = (t.get("side")   or "").upper()
        entry_px   = float(t.get("entry_price") or 0)
        qty        = float(t.get("qty") or 1)
        entry_time = t.get("entry_time") or ""
        exit_time  = t.get("exit_time")  or ""
        strategy   = t.get("strategy")   or ""
        if not ticker or not entry_time or entry_px == 0:
            continue
        try:
            entry_dt = _dt.datetime.fromisoformat(entry_time.replace("Z", "+00:00"))
        except Exception:
            continue
        try:
            exit_dt = _dt.datetime.fromisoformat(exit_time.replace("Z", "+00:00")) if exit_time else None
        except Exception:
            exit_dt = None
        bars       = day_bars.get((ticker, entry_time[:10]), [])
        trade_bars = [b for b in bars if b.timestamp >= entry_dt]
        cap_dt     = None if skip_exits else exit_dt
        cap_price  = None if skip_exits else float(t.get("exit_price") or 0)
        rule       = _rule_for(strategy)
        r_trail    = _apply_session_trail(rule.get("trail_pct", 0.15), entry_dt)
        r_trigger  = rule.get("trigger_pct", 0.0)
        r_mh       = rule.get("max_hold_mins") or 0
        sr_sim     = _simulate_exit(trade_bars, entry_px, side, r_trail, r_trigger, 0.0,
                                    r_mh, entry_dt, cap_dt=cap_dt, cap_price=cap_price,
                                    stop_loss_dollars=0.0, qty=qty)
        prepared.append({
            "ticker":     ticker,
            "side":       side,
            "entry_px":   entry_px,
            "qty":        qty,
            "trade_bars": trade_bars,
            "entry_dt":   entry_dt,
            "cap_dt":     cap_dt,
            "cap_price":  cap_price,
            "sr_pnl":     _pnl(sr_sim["exit_price"], entry_px, qty, side) if sr_sim else 0.0,
            "type_level": _type_level(strategy),
        })

    if not prepared:
        return jsonify({"error": "No trades could be prepared for sweep"}), 404

    sr_total = round(sum(p["sr_pnl"] for p in prepared), 2)

    def _sim_pnl(td, trail, trigger):
        sim = _simulate_exit(td["trade_bars"], td["entry_px"], td["side"],
                             trail, trigger, 0.0, max_hold, td["entry_dt"],
                             cap_dt=td["cap_dt"], cap_price=td["cap_price"],
                             stop_loss_dollars=stop_dollars, qty=td["qty"])
        return _pnl(sim["exit_price"], td["entry_px"], td["qty"], td["side"]) if sim else 0.0

    if not per_strategy:
        sweep_results = []
        for trail in trail_values:
            for trigger in triggers:
                total = imp = worse = 0.0
                for td in prepared:
                    p = _sim_pnl(td, trail, trigger)
                    total += p
                    if p > td["sr_pnl"] + 0.01:   imp   += 1
                    elif p < td["sr_pnl"] - 0.01: worse += 1
                sweep_results.append({
                    "trail": trail, "trigger": trigger,
                    "total_pnl":   round(total, 2),
                    "delta_vs_sr": round(total - sr_total, 2),
                    "improved": int(imp), "worse": int(worse), "trades": len(prepared),
                })
        sweep_results.sort(key=lambda r: r["total_pnl"], reverse=True)
        return jsonify({"mode": "global", "sr_total": sr_total, "results": sweep_results})

    else:
        groups = defaultdict(list)
        for td in prepared:
            groups[td["type_level"]].append(td)

        strategy_results = []
        combined_pnl     = 0.0
        best_overrides   = {}
        for strat, tds in sorted(groups.items()):
            best_pnl = best_trail = best_trigger = None
            grid = []
            for trail in trail_values:
                for trigger in triggers:
                    total = sum(_sim_pnl(td, trail, trigger) for td in tds)
                    grid.append({"trail": trail, "trigger": trigger, "total_pnl": round(total, 2)})
                    if best_pnl is None or total > best_pnl:
                        best_pnl = total; best_trail = trail; best_trigger = trigger
            sr_strat = sum(td["sr_pnl"] for td in tds)
            strategy_results.append({
                "strategy": strat, "trades": len(tds),
                "best_trail": best_trail, "best_trigger": best_trigger,
                "best_pnl":  round(best_pnl, 2),
                "sr_pnl":    round(sr_strat, 2),
                "delta":     round(best_pnl - sr_strat, 2),
                "top5": sorted(grid, key=lambda x: x["total_pnl"], reverse=True)[:5],
            })
            combined_pnl   += best_pnl
            best_overrides[strat] = {"trail": best_trail, "trigger": best_trigger}

        return jsonify({
            "mode": "per_strategy", "sr_total": sr_total,
            "combined_pnl": round(combined_pnl, 2),
            "delta": round(combined_pnl - sr_total, 2),
            "strategies": strategy_results,
            "best_overrides": best_overrides,
        })


@app.route("/strategies")
def strategies():
    return render_template("strategies.html")



@app.route("/optimize")
def optimize_page():
    return render_template("optimize.html")


@app.route("/progress")
def progress_page():
    return render_template("progress.html")


# Build the 12-alert spec for a ticker. Order is chosen to minimize TV
# context-switching: outer = strategy (Pine reload), middle = level (dropdown),
# inner = timeframe (fastest swap).
PROGRESS_STRATEGIES = ["BREAKOUT", "REVERSAL"]
PROGRESS_LEVELS     = ["R3S3", "R4S4"]
PROGRESS_TIMEFRAMES = ["05MIN", "15MIN", "30MIN"]


def _progress_alert_specs(ticker, timeframes=None, version=None):
    ticker = ticker.strip().upper()
    tfs = timeframes or PROGRESS_TIMEFRAMES
    ver = (version or "").strip().upper()  # e.g. "V02" or ""
    specs = []
    for strat in PROGRESS_STRATEGIES:
        for level in PROGRESS_LEVELS:
            for tf in tfs:
                tf_display = tf.lstrip("0") if tf.startswith("0") else tf  # 05MIN → 5MIN
                if ver:
                    name = f"{ticker}_CAM_{strat}_{level}_{ver}_{tf_display}"
                else:
                    name = f"CAM_{ticker}_{strat}_{level}_{tf}"
                tv_interval = {"05MIN": "5", "5MIN": "5", "15MIN": "15", "30MIN": "30"}.get(tf, "5")
                specs.append({
                    "name":       name,
                    "ticker":     ticker,
                    "strategy":   strat,
                    "level":      level,
                    "timeframe":  tf,
                    "tv_settings": {
                        "pine_script": f"CAM_R4S4_{strat}_V01",
                        "level":       "R4" if level == "R4S4" else "R3",
                        "interval":    tv_interval,
                    },
                    # Pine script inputs the user must set on the chart so the
                    # alert_message Pine emits already contains the strategy name,
                    # broker, and qty. The TV alert Message field is then a single
                    # placeholder that forwards Pine's pre-built JSON unchanged —
                    # this avoids the {{strategy.order.action}} ambiguity that
                    # collapses entries and exits onto the same buy/sell.
                    "pine_inputs": {
                        "strategy_id": name,
                        "broker":      "alpaca-paper",
                        "qty":         10,
                    },
                    "alert_message": "{{strategy.order.alert_message}}",
                })
    return specs


def _progress_default_nodes(name):
    return [
        {"type": "strategy",      "value": name},
        {"type": "quantity",      "amount": 10, "unit": "shares"},
        {"type": "instrument",    "value": "STK"},
        {"type": "broker",        "value": "alpaca-paper"},
        {"type": "trading_hours", "start": "09:30", "end": "15:55", "tz": "America/New_York"},
        {"type": "exit_params",   "stop_loss": None, "trail_trigger": None, "trail_offset": 0.15, "mode": "percent"},
    ]


@app.route("/api/progress/add_ticker", methods=["POST"])
def progress_add_ticker():
    """Create routing rules for a ticker (idempotent) and return the alert checklist.

    Two modes:
      - "camarilla" (default): 12 rules = 2 strategies × 2 levels × 3 timeframes
      - "single":              1 rule from `strategy_slug` + `timeframe`, with an
                               optional `rule_name` override. Useful for non-Camarilla
                               strategies (research-generated, opening-range, etc.)
                               that don't fit the BREAKOUT/REVERSAL × R3/R4 grid.
    """
    data   = request.get_json(silent=True) or {}
    ticker = (data.get("ticker") or "").strip().upper()
    if not ticker or not ticker.replace("_", "").isalnum():
        return jsonify({"error": "ticker required (alphanumeric)"}), 400

    mode    = (data.get("mode")    or "camarilla").strip().lower()
    version = (data.get("version") or "").strip().upper()  # e.g. "V02"
    if mode == "cam5min":
        specs = _progress_alert_specs(ticker, timeframes=["05MIN"], version=version)
    elif mode == "camarilla" and version:
        specs = _progress_alert_specs(ticker, version=version)
    elif mode == "single":
        strategy_slug = (data.get("strategy_slug") or "").strip().upper()
        timeframe     = (data.get("timeframe") or "").strip().upper()
        rule_name_in  = (data.get("rule_name") or "").strip()
        if not strategy_slug or not strategy_slug.replace("_", "").isalnum():
            return jsonify({"error": "strategy_slug required (alphanumeric)"}), 400
        if not timeframe:
            return jsonify({"error": "timeframe required (e.g. 5MIN, 1H, 1D)"}), 400
        rule_name = rule_name_in or f"{ticker}_{strategy_slug}_{timeframe}"
        specs = [{
            "name":      rule_name,
            "ticker":    ticker,
            "strategy":  strategy_slug,
            "level":     "",
            "timeframe": timeframe,
            "tv_settings": {
                "pine_script": "(custom)",
                "level":       "",
                "interval":    timeframe.replace("MIN", "").replace("M", "") if "MIN" in timeframe or timeframe.endswith("M") else timeframe,
            },
            "pine_inputs": {
                "strategy_id": rule_name,
                "broker":      "alpaca-paper",
                "qty":         10,
            },
            "alert_message": "{{strategy.order.alert_message}}",
        }]
    else:
        specs = _progress_alert_specs(ticker)

    conn  = get_db()
    cur   = conn.cursor()
    p     = placeholder()

    cur.execute("SELECT name FROM routing_rules")
    existing = {(r[0] if DATABASE_URL else r["name"]) for r in cur.fetchall()}

    created, skipped = [], []
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for spec in specs:
        if spec["name"] in existing:
            skipped.append(spec["name"])
            continue
        nodes_json = json.dumps(_progress_default_nodes(spec["name"]))
        if DATABASE_URL:
            cur.execute(
                f"INSERT INTO routing_rules (name,enabled,nodes,created_at,tv_alert_created) "
                f"VALUES ({p},{p},{p},{p},{p}) RETURNING id",
                (spec["name"], 1, nodes_json, ts, 0),
            )
        else:
            cur.execute(
                f"INSERT INTO routing_rules (name,enabled,nodes,created_at,tv_alert_created) "
                f"VALUES ({p},{p},{p},{p},{p})",
                (spec["name"], 1, nodes_json, ts, 0),
            )
        created.append(spec["name"])
    conn.commit()
    conn.close()

    return jsonify({
        "ticker":   ticker,
        "mode":     mode,
        "created":  created,
        "skipped":  skipped,
        "specs":    specs,
    })


@app.route("/api/progress/fill_stats")
def progress_fill_stats():
    """Per-strategy fill counts and most-recent fill timestamp.
    Drives the per-row "X fills · Y ago" annotation and the
    "delete empty (zero-fill) strategies" bulk action on /progress."""
    out = {}
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(
            "SELECT strategy, COUNT(*) AS n, MAX(received_at) AS last_at "
            "FROM trades WHERE strategy IS NOT NULL AND strategy != '' "
            "GROUP BY strategy"
        )
        rows = cur.fetchall()
        conn.close()
        for r in rows:
            if DATABASE_URL:
                name, n, last_at = r[0], r[1], r[2]
            else:
                name, n, last_at = r["strategy"], r["n"], r["last_at"]
            out[name] = {"count": int(n or 0), "last_at": last_at or ""}
    except Exception as _e:
        log.warning("progress_fill_stats failed: %s", _e)
    return jsonify(out)


@app.route("/api/webhook/blocked")
def api_blocked_signals():
    """Recent signals blocked because no routing rule matched their strategy name."""
    days = int(request.args.get("days", 7))
    try:
        conn = get_db(); cur = conn.cursor(); p = placeholder()
        cur.execute(
            f"SELECT strategy, ticker, action, received_at, exec_detail "
            f"FROM trades WHERE exec_status={p} "
            f"AND received_at >= datetime('now', '-{days} days') "
            f"ORDER BY received_at DESC LIMIT 200",
            ("blocked",),
        )
        rows = cur.fetchall()
        conn.close()
        seen = {}
        for r in rows:
            strategy = (r[0] if DATABASE_URL else r["strategy"]) or ""
            ticker   = (r[1] if DATABASE_URL else r["ticker"])   or ""
            action   = (r[2] if DATABASE_URL else r["action"])   or ""
            ts       = (r[3] if DATABASE_URL else r["received_at"]) or ""
            detail   = (r[4] if DATABASE_URL else r["exec_detail"]) or ""
            key = strategy
            if key not in seen:
                seen[key] = {"strategy": strategy, "ticker": ticker,
                             "last_action": action, "last_seen": ts,
                             "detail": detail, "count": 0}
            seen[key]["count"] += 1
            if ts > seen[key]["last_seen"]:
                seen[key]["last_seen"] = ts
                seen[key]["last_action"] = action
        return jsonify(sorted(seen.values(), key=lambda x: x["last_seen"], reverse=True))
    except Exception as e:
        log.warning("api_blocked_signals failed: %s", e)
        return jsonify([])


_refined_last_run    = None   # UTC timestamp of last scheduled/manual refresh
_refined_last_result = {}    # {updated, removed, not_found, top_strategies}


def _build_signal_lookup_for_alpaca(trades_db=None):
    """Build {(ticker, 'BOT'|'SLD'): [(unix_ts, strategy, sentiment), ...]} from the
    TradingView signals stored in the trades table. Used by both the Refined snapshot
    and /api/alpaca/analysis so fill→strategy attribution stays consistent.

    Pass `trades_db` to reuse an already-loaded query result and avoid a duplicate
    DB hit."""
    from datetime import datetime as _dt
    if trades_db is None:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT ticker, action, sentiment, received_at, strategy, exec_status FROM trades ORDER BY id ASC")
        rows = cur.fetchall()
        trades_db = [dict(r) if not DATABASE_URL else dict(zip([d[0] for d in cur.description], r)) for r in rows]
        conn.close()

    lookup = {}
    for t in trades_db:
        ticker    = (t.get("ticker") or "").strip().upper()
        action    = _canonical_action(t.get("action"))
        sentiment = (t.get("sentiment") or "").strip().lower()
        received  = t.get("received_at") or ""
        strategy  = (t.get("strategy") or "").strip()
        if not strategy:
            continue
        side = "BOT" if action == "BUY" else "SLD" if action == "SELL" else None
        if not side or not ticker or not received:
            continue
        try:
            ts = _dt.fromisoformat(received.replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        lookup.setdefault((ticker, side), []).append((ts, strategy, sentiment))
    return lookup


def _resolve_signal_for_fill(signal_lookup, symbol, side, fill_time_str, order_id=""):
    """Return (strategy, sentiment) for the TV signal closest in time to this fill,
    within a ±5-minute window. Falls back to parsing strategy from the
    client_order_id (kairos-{strategy}-{ts}) when no TV signal matches."""
    from datetime import datetime as _dt
    try:
        fill_ts = _dt.fromisoformat(fill_time_str.replace("Z", "+00:00")).timestamp()
    except Exception:
        fill_ts = None
    candidates = signal_lookup.get((symbol.upper(), side), [])
    if candidates and fill_ts is not None:
        best = min(candidates, key=lambda x: abs(x[0] - fill_ts))
        if abs(best[0] - fill_ts) <= 300:
            return best[1], best[2]
    if order_id and order_id.startswith("kairos-"):
        parts = order_id.split("-", 2)
        if len(parts) == 3 and parts[1]:
            return parts[1], ""
    return "Unknown", ""


def _pair_alpaca_fills_lifo(fills, from_date="", to_date="", signal_lookup=None):
    """Date-filter, dedupe, sort, then run global LIFO pairing with sentiment-aware
    intent. Identical pairing semantics to /api/alpaca/analysis. Returns:
      {
        "deduped":      [...],   # filtered + deduped + sorted fills (for daily pairing)
        "closed":       [...],   # all paired round-trips
        "closed_clean": [...],   # round-trips minus orphans
        "orphans":      [...],   # orphan round-trips (with orphan_reason)
        "signal_lookup": dict,
      }"""
    from datetime import datetime as _dt

    if from_date or to_date:
        def _fill_date(f):
            t = f.get("time") or ""
            return t[:10] if t else ""
        fills = [f for f in fills if
                 (not from_date or _fill_date(f) >= from_date) and
                 (not to_date   or _fill_date(f) <= to_date)]

    seen = set()
    deduped = []
    for f in fills:
        key = f"{f['symbol']}|{f['side']}|{f['time']}|{f['shares']}"
        if key not in seen:
            seen.add(key)
            deduped.append(f)

    def _parse_ts(f):
        try:
            return _dt.fromisoformat((f["time"] or "").replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0
    deduped.sort(key=_parse_ts)

    if signal_lookup is None:
        signal_lookup = _build_signal_lookup_for_alpaca()

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
        strat, sentiment = _resolve_signal_for_fill(signal_lookup, sym, side, fill_ts, f.get("order_id", ""))
        if sentiment == "flat":
            intent = "exit"
        elif sentiment == "long":
            intent = "enter_long"
        elif sentiment == "short":
            intent = "enter_short"
        else:
            intent = "legacy"
        if side == "BOT":
            if intent == "enter_long":
                open_longs.setdefault(sym, []).append((price, qty, fill_ts, strat))
                continue
            q = open_shorts.setdefault(sym, [])
            while qty > 0 and q:
                ep, eq, et, es = q.pop(-1)
                m = min(qty, eq)
                # Round-trip uses entry's strategy. If the entry didn't resolve to
                # any known strategy, fall back to the closing fill's strategy so a
                # known exit doesn't get filed under "Unknown".
                pair_strat = es if (es and es != "Unknown") else strat
                closed.append({"pnl": round((ep - price) * m, 2), "strategy": pair_strat,
                               "ticker": sym, "date": date_str, "side": "SHORT",
                               "entry_price": ep, "exit_price": price, "qty": m,
                               "entry_time": et, "exit_time": fill_ts})
                qty -= m
                if eq > m:
                    q.append((ep, eq - m, et, es))
            if qty > 0 and intent == "legacy":
                open_longs.setdefault(sym, []).append((price, qty, fill_ts, strat))
        elif side == "SLD":
            if intent == "enter_short":
                open_shorts.setdefault(sym, []).append((price, qty, fill_ts, strat))
                continue
            q = open_longs.setdefault(sym, [])
            while qty > 0 and q:
                ep, eq, et, es = q.pop(-1)
                m = min(qty, eq)
                pair_strat = es if (es and es != "Unknown") else strat
                closed.append({"pnl": round((price - ep) * m, 2), "strategy": pair_strat,
                               "ticker": sym, "date": date_str, "side": "LONG",
                               "entry_price": ep, "exit_price": price, "qty": m,
                               "entry_time": et, "exit_time": fill_ts})
                qty -= m
                if eq > m:
                    q.append((ep, eq - m, et, es))
            if qty > 0 and intent == "legacy":
                open_shorts.setdefault(sym, []).append((price, qty, fill_ts, strat))

    orphans, closed_clean = [], []
    for c in closed:
        is_orph, reason = _classify_orphan(c)
        if is_orph:
            orphans.append({**c, "orphan_reason": reason})
        else:
            closed_clean.append(c)

    return {"deduped": deduped, "closed": closed, "closed_clean": closed_clean,
            "orphans": orphans, "signal_lookup": signal_lookup}


def _compute_strategy_stats(days=45, from_date=None):
    """Per-strategy stats from Alpaca account-1 fills, using the same signal-resolution
    + LIFO pairing as /api/alpaca/analysis so the Refined snapshot agrees with the
    leaderboard view.

    Returns {strategy_name: {trades, wins, losses, win_rate, gross_win,
    gross_loss, total_pnl, profit_factor}}. profit_factor is None when there
    are no losing trades.

    If from_date (YYYY-MM-DD) is provided it acts as a fixed anchor — the window
    grows over time and rankings become more stable as data accumulates.
    Otherwise falls back to a rolling `days`-day window."""
    import datetime as _dt

    if not from_date:
        from_date = (
            _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=days)
        ).strftime("%Y-%m-%d")

    try:
        fills = _get_cached_fills()
    except Exception:
        return {}

    paired = _pair_alpaca_fills_lifo(fills, from_date=from_date)
    closed_clean = paired["closed_clean"]

    excluded = _load_excluded_strategies()
    strat_map = {}
    for c in closed_clean:
        if c["strategy"] in excluded:
            continue
        strat_map.setdefault(c["strategy"], []).append(
            (c["pnl"], float(c.get("qty") or 1))
        )

    stats_map = {}
    for strat, trade_pairs in strat_map.items():
        pnls          = [p for p, _ in trade_pairs]
        pnl_per_share = [p / max(q, 0.01) for p, q in trade_pairs]
        # Match the analysis endpoint's _stats() exactly: zero-PnL trades count as losses.
        wins       = [p for p in pnls if p > 0]
        losses     = [p for p in pnls if p <= 0]
        gross_win  = round(sum(wins), 2)
        gross_loss = round(abs(sum(losses)), 2)
        total_pnl  = round(gross_win - gross_loss, 2)
        stats_map[strat] = {
            "trades":        len(pnls),
            "wins":          len(wins),
            "losses":        len(losses),
            "win_rate":      round(len(wins) / len(pnls) * 100, 1) if pnls else 0.0,
            "gross_win":     gross_win,
            "gross_loss":    gross_loss,
            "total_pnl":     total_pnl,
            "profit_factor":  round(gross_win / gross_loss, 2) if gross_loss > 0 else None,
            "sharpe":         _sharpe_from_pnls(pnl_per_share),
            "consec_losses":  sum(1 for _ in __import__('itertools').takewhile(
                lambda p: p <= 0, reversed(pnls))),
        }
    return stats_map


# Composite-score weights for ranking strategies into Alpaca Refined.
# Keep these in sync with the client-side mirror in templates/analysis.html
# (addTopNToRefined) so manual and scheduled refreshes pick the same top N.
_REFINED_SCORE_WEIGHTS = {
    "sharpe":        0.35,   # primary — risk-adjusted consistency of returns
    "profit_factor": 0.30,   # trade quality — $ won per $ lost
    "win_rate":      0.20,   # hit rate
    "trades":        0.15,   # sample-size confidence
}
# Saturation points — beyond these, additional gains stop contributing to score.
_REFINED_SHARPE_SATURATION = 2.0   # Sharpe ≥ 2.0 is "great", no extra credit beyond
_REFINED_PF_SATURATION     = 2.5   # PF >= 2.5 is "great"
_REFINED_TRADES_SATURATION = 10    # 10+ trades counts as a full sample


def _composite_score(stats, max_pnl):
    """Composite ranking score in [0, 1]. Higher = better.

    Weights: Sharpe 35% · PF 30% · Win rate 20% · Trades 15%
      - sharpe:        sharpe / 2.0, capped at 1.0; negative → 0; None → 0
      - profit_factor: pf / 2.5, capped at 1.0; None (no losses) → 1.0
      - win_rate:      win_rate / 100
      - trades:        trades / 10, capped at 1.0
    """
    sh = stats.get("sharpe")
    sh_norm     = 0.0 if sh is None else max(min(sh / _REFINED_SHARPE_SATURATION, 1.0), 0.0)
    pf = stats.get("profit_factor")
    pf_norm     = 1.0 if pf is None else max(min(pf / _REFINED_PF_SATURATION, 1.0), 0.0)
    win_norm    = max(min((stats.get("win_rate") or 0) / 100.0, 1.0), 0.0)
    trades_norm = min((stats.get("trades") or 0) / _REFINED_TRADES_SATURATION, 1.0)

    w = _REFINED_SCORE_WEIGHTS
    return round(
        w["sharpe"]        * sh_norm +
        w["profit_factor"] * pf_norm +
        w["win_rate"]      * win_norm +
        w["trades"]        * trades_norm,
        4,
    )


# Refined sizing: per-strategy dollar target by composite-score band.
# Score is 0..1 from _composite_score; bands are score-percent thresholds.
# First matching band wins (highest score-floor first).
# Sized for ~$27k equity using 4× day-trade margin: worst-case 5 concurrent
# band-A fires = $100k, within the $108k DTBP. Strategies are intraday so no
# overnight 2× exposure. Reduce if concurrency creeps above 5 fires/day.
_REFINED_SIZE_BANDS = [
    (80, 25_000),   # score ≥ 0.80 → $25k per trade
    (65, 12_000),   # score ≥ 0.65 → $12k
    (50,  6_000),   # score ≥ 0.50 → $6k
    ( 0,  2_500),   # else         → $2.5k floor
]

# Price-based share cap applied on top of dollar target.
# Prevents cheap stocks (NFLX $89, HOOD $77) from getting outsized share counts
# while leaving expensive stocks (SPY $743, QQQ $715) unaffected.
# e.g. NFLX $89 + $25k target = 280sh → capped at 50.  SPY $743 = 33sh → no cap.
_REFINED_MAX_SHARES_BY_PRICE = [
    (300, None),   # price ≥ $300 → no cap
    (150,   80),   # price ≥ $150 → max 80 shares
    ( 75,   50),   # price ≥  $75 → max 50 shares
    (  0,   30),   # price <  $75 → max 30 shares
]

# Consecutive live losing trades that trigger auto-demotion from Refined.
# Strategy stays in catch-all but is excluded from the top-N selection until
# it records a winning trade and the consecutive count resets.
_REFINED_CONSEC_LOSS_GATE = 3

# Minimum closed round-trips a strategy needs before being eligible for Refined.
# Filters out 1–2-trade flukes that would otherwise score highly on PF/Win.
# Start lenient while the system is young; raise to 5 (then 10) as the cohort
# accumulates samples.
_REFINED_MIN_TRADES = 3


def _band_target_dollars(score):
    """Return the dollar target for the band that contains this composite score."""
    score_pct = (score or 0) * 100
    for floor_pct, target in _REFINED_SIZE_BANDS:
        if score_pct >= floor_pct:
            return target
    return 0


def _strategy_to_ticker(strategy_name):
    """Pull the ticker from a strategy name. Naming convention is
    {TICKER}_{REST}, e.g. SPY_CAM_BREAKOUT_R4S4_V02_5MIN → SPY."""
    if not strategy_name:
        return ""
    return strategy_name.split("_", 1)[0].upper()


_REFINED_PRICE_CACHE = {}   # {ticker: (price, fetched_at_ts)} — refreshed at each /refresh_refined run
_REFINED_PRICE_TTL   = 60 * 60 * 6   # 6h — covers a trading day after the 4:15 PM ET refresh


def _fetch_alpaca_last_prices(tickers):
    """Return {ticker: last_trade_price} for the given tickers via the Alpaca data API.
    Cached per-ticker for _REFINED_PRICE_TTL so within-refresh duplicates don't re-hit."""
    import os
    import time as _time
    tickers = [t for t in {t.upper() for t in tickers if t} if t]
    if not tickers:
        return {}

    now = _time.time()
    fresh, stale = {}, []
    for t in tickers:
        cached = _REFINED_PRICE_CACHE.get(t)
        if cached and (now - cached[1]) < _REFINED_PRICE_TTL:
            fresh[t] = cached[0]
        else:
            stale.append(t)

    if stale:
        try:
            from alpaca.data.historical import StockHistoricalDataClient
            from alpaca.data.requests import StockLatestTradeRequest
            key    = os.environ.get("ALPACA_KEY",    "")
            secret = os.environ.get("ALPACA_SECRET", "")
            client = StockHistoricalDataClient(api_key=key or None, secret_key=secret or None)
            req    = StockLatestTradeRequest(symbol_or_symbols=stale)
            trades = client.get_stock_latest_trade(req) or {}
            for sym, trade in trades.items():
                price = float(getattr(trade, "price", 0) or 0)
                if price > 0:
                    fresh[sym] = price
                    _REFINED_PRICE_CACHE[sym] = (price, now)
        except Exception as _e:
            log.warning("Alpaca last-price fetch failed for %s: %s", stale, _e)
    return fresh


def _compute_refined_qty(score, last_price):
    """Shares to trade for a strategy with this composite score and ticker price.
    Dollar target from band ÷ price, then capped by price tier so cheap stocks
    don't get outsized share counts. Returns None if price unavailable."""
    target = _band_target_dollars(score)
    if not target or not last_price or last_price <= 0:
        return None
    shares = max(1, round(target / last_price))
    # Apply price-based max-shares cap
    for price_floor, max_shares in _REFINED_MAX_SHARES_BY_PRICE:
        if last_price >= price_floor:
            if max_shares is not None:
                shares = min(shares, max_shares)
            break
    return shares


def _do_refresh_refined(n=20, broker_val="alpaca-paper-2", days=45, from_date=None):
    """Core logic: remove broker_val from all rules, re-add to top N by composite score.

    Ranking uses a weighted score over (profit_factor, win_rate, trades, total_pnl);
    see _composite_score. Net-negative strategies are filtered out before scoring.

    from_date (YYYY-MM-DD) anchors the ranking window; falls back to a rolling
    `days`-day window when not provided."""
    global _refined_last_run, _refined_last_result

    stats_map  = _compute_strategy_stats(days=days, from_date=from_date)
    # Eligibility: net-positive AND at least _REFINED_MIN_TRADES round-trips.
    # The trades floor keeps lucky 1–2-trade strategies (typically PF=None,
    # 100% win) out of the top-N — they need more sample evidence first.
    demoted = [k for k, v in stats_map.items()
               if (v.get("consec_losses") or 0) >= _REFINED_CONSEC_LOSS_GATE]
    if demoted:
        log.info("Refined: demoting %d strategies with %d+ consecutive losses: %s",
                 len(demoted), _REFINED_CONSEC_LOSS_GATE, demoted)

    # Build the set of strategy patterns from ENABLED routing rules. A strategy
    # only qualifies for refinement if at least one enabled rule's strategy node
    # matches its name (same prefix/suffix wildcard rules as the webhook router).
    # Disabling a pipeline → strategy drops out of scoring even when historical
    # fills exist. Failure to load = defensive skip (treat all as eligible).
    _enabled_patterns = []
    _patterns_loaded  = False
    try:
        _rconn = get_db(); _rcur = _rconn.cursor()
        _rcur.execute("SELECT nodes FROM routing_rules WHERE enabled=1")
        for row in _rcur.fetchall():
            nodes_raw = row[0] if DATABASE_URL else row["nodes"]
            _nodes = json.loads(nodes_raw) if isinstance(nodes_raw, str) else (nodes_raw or [])
            for nd in _nodes:
                if nd.get("type") == "strategy":
                    val = (nd.get("value") or "").strip().upper()
                    if val:
                        _enabled_patterns.append(val)
        _rconn.close()
        _patterns_loaded = True
    except Exception as _re:
        log.warning("Refined: failed to load enabled rule strategies, skipping rule filter: %s", _re)

    def _strategy_has_enabled_rule(strat_name):
        if not _patterns_loaded:
            return True
        s = strat_name.upper()
        for pattern in _enabled_patterns:
            if pattern == s:
                return True
            if pattern.endswith("*") and s.startswith(pattern[:-1]):
                return True
            if pattern.startswith("*") and s.endswith(pattern[1:]):
                return True
        return False

    candidates = {
        k: v for k, v in stats_map.items()
        if (v.get("total_pnl") or 0) > 0
        and (v.get("trades") or 0) >= _REFINED_MIN_TRADES
        and (v.get("consec_losses") or 0) < _REFINED_CONSEC_LOSS_GATE
        and _strategy_has_enabled_rule(k)
    }
    if _patterns_loaded:
        _filtered_out = [k for k in stats_map
                         if (stats_map[k].get("total_pnl") or 0) > 0
                         and (stats_map[k].get("trades") or 0) >= _REFINED_MIN_TRADES
                         and (stats_map[k].get("consec_losses") or 0) < _REFINED_CONSEC_LOSS_GATE
                         and not _strategy_has_enabled_rule(k)]
        if _filtered_out:
            log.info("Refined: %d strategies filtered out by routing-rule gate: %s",
                     len(_filtered_out), _filtered_out)
    max_pnl    = max((v["total_pnl"] for v in candidates.values()), default=0)

    scored = sorted(
        ((name, stats, _composite_score(stats, max_pnl))
         for name, stats in candidates.items()),
        key=lambda x: x[2],
        reverse=True,
    )
    top_scored = scored[:n]
    top        = [name for name, _, _ in top_scored]

    # Per-strategy share sizing: dollar target by band ÷ last price.
    # Stored on the broker node as qty_override so Paper sizing is untouched.
    tickers     = {_strategy_to_ticker(name) for name, _, _ in top_scored}
    last_prices = _fetch_alpaca_last_prices(tickers)
    qty_by_strat = {}    # strategy → (shares, target_dollars, price)
    for name, stats, score in top_scored:
        ticker = _strategy_to_ticker(name)
        price  = last_prices.get(ticker)
        target = _band_target_dollars(score)
        shares = _compute_refined_qty(score, price)
        qty_by_strat[name] = (shares, target, price)

    conn = get_db(); cur = conn.cursor(); p = placeholder()
    cur.execute("SELECT id, nodes FROM routing_rules ORDER BY id")
    rows = cur.fetchall()

    rule_map = {}
    for row in rows:
        rid       = row[0] if DATABASE_URL else row["id"]
        nodes_raw = row[1] if DATABASE_URL else row["nodes"]
        nodes = json.loads(nodes_raw) if isinstance(nodes_raw, str) else (nodes_raw or [])
        for n_node in nodes:
            if n_node.get("type") == "strategy":
                rule_map[(n_node.get("value") or "").upper()] = (rid, nodes)

    removed = updated = 0
    not_found = []

    # Step 1 — strip broker_val from every rule
    for row in rows:
        rid       = row[0] if DATABASE_URL else row["id"]
        nodes_raw = row[1] if DATABASE_URL else row["nodes"]
        nodes = json.loads(nodes_raw) if isinstance(nodes_raw, str) else (nodes_raw or [])
        new_nodes = [nd for nd in nodes if not (nd.get("type") == "broker" and nd.get("value") == broker_val)]
        if len(new_nodes) != len(nodes):
            cur.execute(f"UPDATE routing_rules SET nodes={p} WHERE id={p}", (json.dumps(new_nodes), rid))
            # Update rule_map in place
            for nd in new_nodes:
                if nd.get("type") == "strategy":
                    rule_map[(nd.get("value") or "").upper()] = (rid, new_nodes)
            removed += 1

    # Step 2 — add broker_val to top N, with per-strategy qty_override when we have a price
    for strat in top:
        entry = rule_map.get(strat.upper())
        if not entry:
            not_found.append(strat)
            continue
        rid, nodes = entry
        broker_node = {"type": "broker", "value": broker_val}
        shares = qty_by_strat.get(strat, (None, 0, None))[0]
        if shares is not None:
            broker_node["qty_override"] = shares
        nodes.append(broker_node)
        cur.execute(f"UPDATE routing_rules SET nodes={p} WHERE id={p}", (json.dumps(nodes), rid))
        updated += 1

    conn.commit(); conn.close()

    import datetime as _dt
    _refined_last_run = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Refined history: persist this run's top, derive added/removed/tenure vs prior runs.
    # Tenure = calendar days since the strategy was first inducted into the top-N.
    # Using first-induction date rather than consecutive run count so manual refreshes
    # (done to fix issues) don't artificially inflate or reset tenure.
    added_strategies   = []
    removed_strategies = []
    tenure_runs        = {}   # strategy_name -> days since first induction
    try:
        hconn = get_db(); hcur = hconn.cursor(); hp = placeholder()
        hcur.execute("SELECT run_at, strategy_name FROM refined_history ORDER BY run_at DESC")
        hist_rows = hcur.fetchall()
        runs = []   # [(run_at, set_of_strategies)] newest-first, pre-write snapshot
        cur_at = None; cur_set = None
        for r in hist_rows:
            rt = r[0] if DATABASE_URL else r["run_at"]
            nm = r[1] if DATABASE_URL else r["strategy_name"]
            if rt != cur_at:
                if cur_set is not None: runs.append((cur_at, cur_set))
                cur_at = rt; cur_set = set()
            cur_set.add(nm)
        if cur_set is not None: runs.append((cur_at, cur_set))

        prev_top = runs[0][1] if runs else set()
        new_top  = set(top)
        added_strategies   = sorted(new_top - prev_top)
        removed_strategies = sorted(prev_top - new_top)

        for name in top:
            if DATABASE_URL:
                hcur.execute(
                    f"INSERT INTO refined_history (run_at, strategy_name) VALUES ({hp}, {hp}) "
                    "ON CONFLICT DO NOTHING",
                    (_refined_last_run, name),
                )
            else:
                hcur.execute(
                    f"INSERT OR IGNORE INTO refined_history (run_at, strategy_name) VALUES ({hp}, {hp})",
                    (_refined_last_run, name),
                )
        hconn.commit()

        # Compute first-induction date for each strategy in this run's top.
        # Walk ALL history runs (oldest-first) to find the earliest appearance.
        first_seen = {}  # strategy_name -> earliest run_at string
        for run_at, names in reversed(runs):  # reversed = oldest first
            for s in names:
                if s not in first_seen:
                    first_seen[s] = run_at
        # Also credit this run for newly added strategies
        for s in added_strategies:
            if s not in first_seen:
                first_seen[s] = _refined_last_run

        now_date = _dt.datetime.now(_dt.timezone.utc).date()
        for s in new_top:
            first_str = first_seen.get(s, _refined_last_run)
            try:
                # run_at format: "YYYY-MM-DD HH:MM:SS UTC"
                first_date = _dt.datetime.strptime(first_str[:10], "%Y-%m-%d").date()
                tenure_runs[s] = max(1, (now_date - first_date).days + 1)
            except Exception:
                tenure_runs[s] = 1
        hconn.close()
    except Exception as _he:
        log.warning("Refined history tracking failed: %s", _he)

    _refined_last_result = {
        "run_at": _refined_last_run,
        "top_strategies": top,
        "added_strategies":   added_strategies,
        "removed_strategies": removed_strategies,
        "tenure_runs":        tenure_runs,
        "top_scored": [
            {
                "name":          name,
                "score":         score,
                "trades":        stats.get("trades"),
                "win_rate":      stats.get("win_rate"),
                "profit_factor": stats.get("profit_factor"),
                "total_pnl":     stats.get("total_pnl"),
                "sharpe":        stats.get("sharpe"),
                "consec_losses": stats.get("consec_losses", 0),
                "shares":        qty_by_strat.get(name, (None, 0, None))[0],
                "target_dollars": qty_by_strat.get(name, (None, 0, None))[1],
                "last_price":    qty_by_strat.get(name, (None, 0, None))[2],
            }
            for name, stats, score in top_scored
        ],
        "weights": _REFINED_SCORE_WEIGHTS,
        "size_bands": _REFINED_SIZE_BANDS,
        "min_trades": _REFINED_MIN_TRADES,
        "consec_loss_gate": _REFINED_CONSEC_LOSS_GATE,
        "demoted": demoted,
        "updated": updated,
        "removed_from": removed,
        "not_found": not_found,
        "from_date": from_date or None,
        "days": days if not from_date else None,
    }
    log.info("Refined refresh: top=%d updated=%d removed=%d not_found=%d anchor=%s",
             len(top), updated, removed, len(not_found), from_date or f"last {days}d")
    try:
        _save_setting("REFINED_LAST_RESULT", json.dumps(_refined_last_result))
    except Exception as _pe:
        log.warning("Failed to persist refined snapshot: %s", _pe)
    return _refined_last_result


def _refined_scheduler_loop():
    """Daily background thread: refresh Alpaca Refined at 4:15 PM ET on weekdays."""
    import datetime as _dt
    from zoneinfo import ZoneInfo
    _et = ZoneInfo("America/New_York")
    _ran_today = None
    while True:
        time.sleep(60)
        if alpaca_broker is None:
            continue
        now = _dt.datetime.now(_et)
        today = now.date()
        if now.weekday() >= 5:          # skip weekends
            continue
        if now.hour == 16 and now.minute == 15 and _ran_today != today:
            _ran_today = today
            try:
                _do_refresh_refined(days=45)  # rolling 45-day window, no fixed anchor
                log.info("Scheduled Refined refresh complete for %s (anchor=%s)", today, _anchor or "rolling")
            except Exception as _re:
                log.warning("Scheduled Refined refresh failed: %s", _re)


threading.Thread(target=_refined_scheduler_loop, daemon=True).start()


def _load_excluded_strategies():
    raw = _load_setting("EXCLUDED_STRATEGIES", "[]")
    try:
        return set(json.loads(raw))
    except Exception:
        return set()

def _save_excluded_strategies(excl_set):
    _save_setting("EXCLUDED_STRATEGIES", json.dumps(sorted(excl_set)))


@app.route("/api/strategies/excluded", methods=["GET"])
def get_excluded_strategies():
    return jsonify(sorted(_load_excluded_strategies()))


@app.route("/api/strategies/excluded", methods=["POST"])
def add_excluded_strategy():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    excl = _load_excluded_strategies()
    excl.add(name)
    _save_excluded_strategies(excl)
    log.info("Strategy excluded from leaderboard: %s", name)
    return jsonify({"excluded": sorted(excl)})


@app.route("/api/strategies/excluded/<path:name>", methods=["DELETE"])
def remove_excluded_strategy(name):
    excl = _load_excluded_strategies()
    excl.discard(name)
    _save_excluded_strategies(excl)
    log.info("Strategy restored to leaderboard: %s", name)
    return jsonify({"excluded": sorted(excl)})


@app.route("/api/routing/rules/refresh_refined", methods=["GET"])
def get_refined_status():
    anchor = _load_setting("REFINED_FROM_DATE") or ""
    if _refined_last_run:
        return jsonify({**_refined_last_result, "anchor_from_date": anchor})
    return jsonify({"run_at": None, "anchor_from_date": anchor})


@app.route("/api/routing/rules/refresh_refined", methods=["POST"])
def refresh_refined():
    data = request.get_json(silent=True) or {}
    n         = int(data.get("n", 20))
    days      = int(data.get("days", 30))
    if "from_date" in data:
        # Caller is explicitly setting (or clearing) the anchor — persist it
        # so the daily scheduler picks it up on subsequent runs.
        from_date = (data.get("from_date") or "").strip() or None
        _save_setting("REFINED_FROM_DATE", from_date or "")
    else:
        # No anchor in payload — fall back to the saved anchor so manual
        # refreshes match scheduler behaviour. Empty saved value → rolling.
        from_date = (_load_setting("REFINED_FROM_DATE") or "").strip() or None
    result = _do_refresh_refined(n=n, days=days, from_date=from_date)
    return jsonify(result)


@app.route("/api/routing/rules/bulk_update_quantity", methods=["POST"])
def bulk_update_quantity():
    """Update the quantity node on all routing rules from old_amount to new_amount."""
    data       = request.get_json(silent=True) or {}
    new_amount = int(data.get("new_amount", 10))
    old_amount = data.get("old_amount")   # None = update regardless of current value
    conn = get_db(); cur = conn.cursor(); p = placeholder()
    cur.execute("SELECT id, nodes FROM routing_rules ORDER BY id")
    rows = cur.fetchall()
    updated = skipped = 0
    for row in rows:
        rid       = row[0] if DATABASE_URL else row["id"]
        nodes_raw = row[1] if DATABASE_URL else row["nodes"]
        nodes = json.loads(nodes_raw) if isinstance(nodes_raw, str) else (nodes_raw or [])
        changed = False
        for n in nodes:
            if n.get("type") == "quantity" and n.get("unit") in ("shares", None):
                if old_amount is None or n.get("amount") == old_amount:
                    n["amount"] = new_amount
                    changed = True
        if changed:
            cur.execute(f"UPDATE routing_rules SET nodes={p} WHERE id={p}", (json.dumps(nodes), rid))
            updated += 1
        else:
            skipped += 1
    conn.commit(); conn.close()
    log.info("bulk_update_quantity: %d→%d shares, updated=%d skipped=%d", old_amount or 0, new_amount, updated, skipped)
    return jsonify({"updated": updated, "skipped": skipped, "new_amount": new_amount})


@app.route("/api/routing/rules/bulk_update_exit_params", methods=["POST"])
def bulk_update_exit_params():
    """Bulk-set exit_params node mode + trail_offset on every routing rule.

    Defaults to mode=percent, trail_offset=0.15 (i.e. 0.15% trail). Leaves
    stop_loss / hard_stop / trail_trigger untouched on each rule so any
    rule-specific dollar values stay intact — only the trail-offset is
    re-keyed to the new unit.

    If a rule has no exit_params node, one is appended."""
    data                 = request.get_json(silent=True) or {}
    mode                 = (data.get("mode") or "percent").lower()
    trail_offset         = data.get("trail_offset")
    trail_trigger        = data.get("trail_trigger")
    stop_loss            = data.get("stop_loss")
    max_hold_mins        = data.get("max_hold_mins")
    clear_trail          = bool(data.get("clear_trail", False))
    clear_trail_trigger  = bool(data.get("clear_trail_trigger", False))
    clear_max_hold       = bool(data.get("clear_max_hold", False))
    name_contains        = (data.get("name_contains") or "").strip().lower()
    if trail_offset is None and trail_trigger is None and stop_loss is None and max_hold_mins is None and not clear_trail and not clear_trail_trigger and not clear_max_hold:
        trail_offset = 0.15
    try:
        if trail_offset  is not None: trail_offset  = float(trail_offset)
        if trail_trigger is not None: trail_trigger = float(trail_trigger)
        if stop_loss     is not None: stop_loss     = float(stop_loss)
        if max_hold_mins is not None: max_hold_mins = float(max_hold_mins)
    except (TypeError, ValueError):
        return jsonify({"error": "exit param values must be numbers"}), 400
    if mode not in ("percent", "dollars"):
        return jsonify({"error": "mode must be 'percent' or 'dollars'"}), 400

    conn = get_db(); cur = conn.cursor(); p = placeholder()
    cur.execute("SELECT id, name, nodes FROM routing_rules ORDER BY id")
    rows = cur.fetchall()
    updated = added_node = skipped = 0
    for row in rows:
        rid       = row[0] if DATABASE_URL else row["id"]
        rname     = (row[1] if DATABASE_URL else row["name"] or "").lower()
        nodes_raw = row[2] if DATABASE_URL else row["nodes"]
        if name_contains and name_contains not in rname:
            skipped += 1
            continue
        nodes = json.loads(nodes_raw) if isinstance(nodes_raw, str) else (nodes_raw or [])
        ep = next((n for n in nodes if n.get("type") == "exit_params"), None)
        # clear_trail_trigger only: skip rules with no existing exit_params (nothing to clear)
        if ep is None and clear_trail_trigger and trail_offset is None and trail_trigger is None and stop_loss is None and not clear_trail:
            skipped += 1
            continue
        if ep is None:
            ep = {"type": "exit_params"}
            nodes.append(ep)
            added_node += 1
        ep["mode"] = mode
        if trail_offset  is not None: ep["trail_offset"]  = trail_offset
        if trail_trigger is not None: ep["trail_trigger"] = trail_trigger
        if stop_loss     is not None: ep["stop_loss"]     = stop_loss
        if max_hold_mins is not None: ep["max_hold_mins"] = max_hold_mins
        if clear_trail:
            ep.pop("trail_offset",  None)
            ep.pop("trail_trigger", None)
        if clear_trail_trigger:
            ep.pop("trail_trigger", None)
        if clear_max_hold:
            ep.pop("max_hold_mins", None)
        cur.execute(f"UPDATE routing_rules SET nodes={p} WHERE id={p}", (json.dumps(nodes), rid))
        updated += 1
    conn.commit(); conn.close()
    log.info("bulk_update_exit_params: %d rules updated, %d skipped (pattern=%r mode=%s trail_offset=%s trail_trigger=%s added_node=%d)",
             updated, skipped, name_contains or "*", mode, trail_offset, trail_trigger, added_node)
    return jsonify({
        "updated":      updated,
        "skipped":      skipped,
        "added_node":   added_node,
        "mode":         mode,
        "trail_offset": trail_offset,
    })


@app.route("/api/routing/rules/add_broker_node", methods=["POST"])
def add_broker_node_to_strategies():
    """Add a broker node to routing rules whose strategy node matches names in the request list."""
    data       = request.get_json(silent=True) or {}
    strategies = data.get("strategies", [])   # list of strategy name strings
    broker_val = data.get("broker", "alpaca-paper-2")
    if not strategies:
        return jsonify({"error": "strategies list required"}), 400

    conn = get_db(); cur = conn.cursor(); p = placeholder()
    cur.execute("SELECT id, nodes FROM routing_rules ORDER BY id")
    rows = cur.fetchall()

    # Build lookup: strategy_node_value → (rule_id, nodes)
    rule_map = {}
    for row in rows:
        rule_id   = row[0] if DATABASE_URL else row["id"]
        nodes_raw = row[1] if DATABASE_URL else row["nodes"]
        nodes = json.loads(nodes_raw) if isinstance(nodes_raw, str) else (nodes_raw or [])
        for n in nodes:
            if n.get("type") == "strategy":
                rule_map[(n.get("value") or "").upper()] = (rule_id, nodes)

    updated = []; skipped = []; not_found = []
    for strat in strategies:
        entry = rule_map.get(strat.upper())
        if not entry:
            not_found.append(strat)
            continue
        rule_id, nodes = entry
        if any(n.get("type") == "broker" and n.get("value") == broker_val for n in nodes):
            skipped.append(strat)
            continue
        nodes.append({"type": "broker", "value": broker_val})
        cur.execute(f"UPDATE routing_rules SET nodes={p} WHERE id={p}", (json.dumps(nodes), rule_id))
        updated.append(strat)

    conn.commit(); conn.close()
    log.info("add_broker_node %s: updated=%d skipped=%d not_found=%d",
             broker_val, len(updated), len(skipped), len(not_found))
    return jsonify({"updated": updated, "skipped": skipped, "not_found": not_found})


@app.route("/api/routing/rules/bulk_add_exit_params", methods=["POST"])
def bulk_add_exit_params():
    """Add a default exit_params node to every routing rule that doesn't already have one."""
    data      = request.get_json(silent=True) or {}
    stop_loss = float(data.get("stop_loss", 0.50))
    conn  = get_db()
    cur   = conn.cursor()
    p     = placeholder()
    cur.execute("SELECT id, nodes FROM routing_rules ORDER BY id")
    rows = cur.fetchall()
    updated = skipped = 0
    for row in rows:
        rule_id   = row[0] if DATABASE_URL else row["id"]
        nodes_raw = row[1] if DATABASE_URL else row["nodes"]
        nodes = json.loads(nodes_raw) if isinstance(nodes_raw, str) else (nodes_raw or [])
        if any(n.get("type") == "exit_params" for n in nodes):
            skipped += 1
            continue
        nodes.append({"type": "exit_params", "stop_loss": stop_loss,
                      "trail_trigger": None, "trail_offset": None, "mode": "dollars"})
        cur.execute(f"UPDATE routing_rules SET nodes={p} WHERE id={p}",
                    (json.dumps(nodes), rule_id))
        updated += 1
    conn.commit()
    conn.close()
    log.info("bulk_add_exit_params: updated=%d skipped=%d stop_loss=$%.2f", updated, skipped, stop_loss)
    return jsonify({"updated": updated, "skipped": skipped, "stop_loss": stop_loss})


@app.route("/api/routing/rules/bulk_remove_exit_params", methods=["POST"])
def bulk_remove_exit_params():
    """Remove exit_params nodes from every routing rule."""
    conn = get_db(); cur = conn.cursor(); p = placeholder()
    cur.execute("SELECT id, nodes FROM routing_rules ORDER BY id")
    rows = cur.fetchall()
    removed = skipped = 0
    for row in rows:
        rid       = row[0] if DATABASE_URL else row["id"]
        nodes_raw = row[1] if DATABASE_URL else row["nodes"]
        nodes = json.loads(nodes_raw) if isinstance(nodes_raw, str) else (nodes_raw or [])
        new_nodes = [n for n in nodes if n.get("type") != "exit_params"]
        if len(new_nodes) < len(nodes):
            cur.execute(f"UPDATE routing_rules SET nodes={p} WHERE id={p}", (json.dumps(new_nodes), rid))
            removed += 1
        else:
            skipped += 1
    conn.commit(); conn.close()
    log.info("bulk_remove_exit_params: removed from %d rules, %d had none", removed, skipped)
    return jsonify({"removed": removed, "skipped": skipped})


@app.route("/api/progress/fix_strategy_mismatch", methods=["POST"])
def progress_fix_strategy_mismatch():
    """One-shot cleanup: where a rule's name differs from its strategy node value, fix the node."""
    conn = get_db()
    cur  = conn.cursor()
    p    = placeholder()
    cur.execute("SELECT id, name, nodes FROM routing_rules")
    rows = cur.fetchall()
    fixed = []
    for r in rows:
        rid       = r[0] if DATABASE_URL else r["id"]
        name      = r[1] if DATABASE_URL else r["name"]
        nodes_raw = r[2] if DATABASE_URL else r["nodes"]
        try:
            nodes = json.loads(nodes_raw) if isinstance(nodes_raw, str) else (nodes_raw or [])
        except Exception:
            continue
        changed = False
        for n in nodes:
            if n.get("type") == "strategy" and (n.get("value") or "") != name:
                n["value"] = name
                changed = True
        if changed:
            cur.execute(
                f"UPDATE routing_rules SET nodes={p} WHERE id={p}",
                (json.dumps(nodes), rid),
            )
            fixed.append(name)
    conn.commit()
    conn.close()
    return jsonify({"fixed": fixed, "count": len(fixed)})


@app.route("/variants")
def variants_page():
    return render_template("variants.html")


def _resolve_variant_strategy(slug):
    """Return a Strategy class for `slug` — built-in first, then user_strategies DB.
    Returns (cls, None) on success or (None, error_message)."""
    from strategies.bt_strategies import STRATEGIES as _BUILTIN
    entry = _BUILTIN.get(slug)
    if entry:
        return entry[0], None
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(f"SELECT code FROM user_strategies WHERE slug = {placeholder()}", (slug,))
        row = cur.fetchone()
        conn.close()
    except Exception as e:
        return None, f"DB lookup failed: {e}"
    if not row:
        return None, f"unknown strategy '{slug}' (not built-in, not saved)"
    code = _strip_code_fences(row[0])
    try:
        from backtesting import Strategy as _Strategy
        import numpy as _np, pandas as _pd
        ns = {"Strategy": _Strategy, "np": _np, "numpy": _np, "pd": _pd, "pandas": _pd}
        exec(code, ns)
        cls = next(
            (v for v in ns.values()
             if isinstance(v, type) and issubclass(v, _Strategy) and v is not _Strategy),
            None,
        )
        if cls is None:
            return None, f"saved strategy '{slug}' has no Strategy subclass"
        cls.__module__ = "__main__"
        cls.__qualname__ = cls.__name__
        setattr(sys.modules["__main__"], cls.__name__, cls)
        return cls, None
    except Exception as e:
        return None, f"saved strategy '{slug}' exec failed: {e}"


@app.route("/api/variants/run", methods=["POST"])
def api_variants_run():
    """Expand a variants config, backtest each with walk-forward, return pass/fail.
    Blocking call — one real Alpaca backtest per variant. Callers should expect
    multi-second latency for grids of more than a few variants."""
    from tools.provision_variants import (
        build_routing_nodes, evaluate_variant, expand_variants, variant_name,
    )
    cfg = request.get_json(silent=True) or {}
    gate_cfg = cfg.get("gate", {})
    start    = cfg.get("start_date", "2024-01-01")
    end      = cfg.get("end_date",   "2024-12-31")
    n_folds  = int(cfg.get("n_folds", 1))
    try:
        variants = list(expand_variants(cfg))
    except Exception as e:
        return jsonify({"error": f"expand_variants: {e}"}), 400
    results = []
    for v in variants:
        cls, err = _resolve_variant_strategy(v["strategy"])
        if err:
            r = {**v, "status": "run-error", "reason": err}
        else:
            r = evaluate_variant(v, start, end, gate_cfg, n_folds=n_folds, strategy_cls=cls)
        r["name"]  = variant_name(v)
        r["nodes"] = build_routing_nodes(v)
        # Drop per-fold detail — UI only shows mean IS/OOS stats
        r.pop("folds", None)
        results.append(r)
    return jsonify({"results": results})


def _strip_code_fences(code: str) -> str:
    """Remove markdown code fences that Claude sometimes emits despite instructions."""
    code = code.strip()
    if code.startswith('```'):
        first_nl = code.find('\n')
        code = code[first_nl + 1:] if first_nl != -1 else code[3:]
        last_fence = code.rfind('```')
        if last_fence != -1:
            code = code[:last_fence]
    return code.strip()


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
    code      = _strip_code_fences((body.get("code")      or "").strip())
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


@app.route("/api/bt/strategies/<slug>", methods=["PUT"])
def bt_strategy_rename(slug):
    """Rename a saved strategy (display name only — slug stays the same)."""
    body = request.get_json(silent=True) or {}
    new_name = (body.get("name") or "").strip()
    if not new_name:
        return jsonify({"error": "name is required"}), 400
    p = placeholder()
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(f"UPDATE user_strategies SET name={p} WHERE slug={p}", (new_name, slug))
        conn.commit()
        conn.close()
        return jsonify({"slug": slug, "name": new_name})
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


def _extract_strategy_summary(code, pine_code):
    """Heuristic: pull a short description + direction flags from saved code.
    Tries Python docstring first, falls back to leading Pine // comments."""
    import re as _re
    description = ""
    m = _re.search(r'class\s+\w+\s*\([^)]*\)\s*:\s*\n\s*"""(.*?)"""', code, _re.DOTALL)
    if m:
        description = _re.sub(r'\s+', ' ', m.group(1)).strip()
    elif pine_code:
        lines = []
        for line in pine_code.splitlines():
            s = line.strip()
            if not s:
                if lines: break
                continue
            if s.startswith("//"):
                cleaned = s.lstrip("/").strip()
                if cleaned and not cleaned.startswith(("@", "version=")):
                    lines.append(cleaned)
            elif lines:
                break
        description = " ".join(lines[:4]).strip()
    if len(description) > 240:
        description = description[:237].rstrip() + "…"
    has_long  = bool(_re.search(r'self\.buy\s*\(',  code))
    has_short = bool(_re.search(r'self\.sell\s*\(', code))
    return description, has_long, has_short


@app.route("/api/bt/strategies/cards")
def bt_strategies_cards():
    """Return condensed card data for user-saved strategies."""
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT slug, name, code, pine_code, params, created_at "
                    "FROM user_strategies ORDER BY created_at DESC")
        rows = cur.fetchall()
        conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    out = []
    for row in rows:
        r = dict(zip([d[0] for d in cur.description], row)) if DATABASE_URL else dict(row)
        desc, has_long, has_short = _extract_strategy_summary(r.get("code") or "", r.get("pine_code") or "")
        try:
            params = json.loads(r.get("params") or "[]")
        except Exception:
            params = []
        out.append({
            "slug":        r["slug"],
            "name":        r["name"],
            "description": desc,
            "long":        has_long,
            "short":       has_short,
            "params":      params,
            "created_at":  r.get("created_at"),
        })
    return jsonify(out)


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
        "11. SESSION / RTH FILTERING: If the Pine Script gates entries or level calculations "
        "by a session (input.session, time(timeframe.period, session), inRTH/newRTHDay variables, "
        "or checks against '0930-1600' etc.), you MUST preserve that gate in Python. Do NOT drop it. "
        "Extract the session start/end (default 09:30-16:00 ET) and gate entries with a mask built from "
        "self.data.index hour/minute. IMPORTANT: the backtester hands you an ET-local naive DatetimeIndex "
        "(tz stripped after conversion to America/New_York), so `idx.hour` directly reflects ET hour. "
        "Do NOT tz_localize or tz_convert — treat the index as ET already.\n"
        "    idx = pd.DatetimeIndex(self.data.index)\n"
        "    mask = ((idx.hour > 9) | ((idx.hour == 9) & (idx.minute >= 30))) & (idx.hour < 16)\n"
        "    in_rth = pd.Series(mask, index=idx).values  # True inside RTH\n"
        "Store `in_rth` on self in init(); then in next() check `if not in_rth[len(self.data)-1]: return` "
        "BEFORE entry logic. Skipping this causes Python to fire entries on overnight/extended-hours bars "
        "that Pine's session gate would block, producing far more trades than TradingView.\n"
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
        "IMPORTANT: If Pine computes the daily H/L/C by tracking RTH variables (e.g. curRTHHigh updated only "
        "when inRTH is true), you MUST filter to RTH bars BEFORE resampling, otherwise overnight extremes "
        "leak into the level formula. Use the same RTH mask from rule #11.\n"
        "CORRECT pattern when Pine uses RTH-tracked daily levels (use in init() BEFORE self.I()):\n"
        "    import pandas as pd\n"
        "    idx = pd.DatetimeIndex(self.data.index)\n"
        "    mask = ((idx.hour > 9) | ((idx.hour == 9) & (idx.minute >= 30))) & (idx.hour < 16)\n"
        "    s_high = pd.Series(self.data.High[mask], index=idx[mask])\n"
        "    s_low  = pd.Series(self.data.Low[mask],  index=idx[mask])\n"
        "    s_close= pd.Series(self.data.Close[mask],index=idx[mask])\n"
        "    d_high = s_high.resample('D').max().shift(1).reindex(idx, method='ffill')\n"
        "    d_low  = s_low.resample('D').min().shift(1).reindex(idx, method='ffill')\n"
        "    d_close= s_close.resample('D').last().shift(1).reindex(idx, method='ffill')\n"
        "    h4_vals = (d_close + (d_high - d_low) * cam_mult / 2.0).values\n"
        "    l4_vals = (d_close - (d_high - d_low) * cam_mult / 2.0).values\n"
        "    self.h4 = self.I(lambda v: v, h4_vals)\n"
        "    self.l4 = self.I(lambda v: v, l4_vals)\n"
        "If Pine uses plain calendar-day levels (no RTH filter), drop the mask and resample on all bars.\n"
        "This produces one H4/L4 value per trading day that is constant for all intraday bars in that day, "
        "exactly matching TradingView's request.security daily behaviour.\n"
        "22. CRITICAL - STOP/TRAIL EXIT CHECKS USE LOW/HIGH, NOT CLOSE: Pine's strategy.exit() fires on "
        "any tick that touches the stop or trailing stop, not just bar close. In backtesting.py we only "
        "have OHLC, but Low/High are the best proxy for 'did the price touch this level inside the bar'. "
        "This rule applies to BOTH the fixed stop AND the trailing stop — a common bug is to check the "
        "fixed stop against Low/High but then check the trailing stop against Close. Do NOT do that. "
        "For LONG positions: fixed stop AND trailing-stop exits must compare against self.data.Low[-1] "
        "(price went down enough intrabar). For SHORT positions: BOTH must compare against self.data.High[-1]. "
        "NEVER check trail/stop exits against self.data.Close[-1] — that misses exits on bars that wick "
        "down to the stop but close above it, keeping positions open longer than Pine and suppressing "
        "re-entries (producing fewer trades than TradingView).\n"
        "ANTI-PATTERN (DO NOT WRITE):\n"
        "    c = self.data.Close[-1]\n"
        "    if self._trailing_active and c <= trail_stop:     # WRONG — uses Close\n"
        "        self.position.close()\n"
        "    if self._trailing_active and c >= trail_stop:     # WRONG (short) — uses Close\n"
        "        self.position.close()\n"
        "CORRECT pattern:\n"
        "    # LONG trailing stop exit\n"
        "    if self._trailing_active:\n"
        "        trail_stop = self._highest - trail_offset\n"
        "        if self.data.Low[-1] <= trail_stop:     # ← Low, NOT Close[-1]\n"
        "            self.position.close(); return\n"
        "    # LONG fixed stop\n"
        "    if self.data.Low[-1] <= self._entry - stop_dist:\n"
        "        self.position.close(); return\n"
        "    # SHORT trailing stop exit\n"
        "    if self._trailing_active:\n"
        "        trail_stop = self._lowest + trail_offset\n"
        "        if self.data.High[-1] >= trail_stop:    # ← High, NOT Close[-1]\n"
        "            self.position.close(); return\n"
        "    # SHORT fixed stop\n"
        "    if self.data.High[-1] >= self._entry + stop_dist:\n"
        "        self.position.close(); return\n"
    )

    def generate():
        t0 = time.time()
        chunks = 0
        chars = 0
        last_chunk_ts = t0
        last_progress_log = 0
        first_chunk_logged = False
        log.info("bt_convert: stream open, pine=%d chars", len(pine_script))
        try:
            client = _anthropic.Anthropic(api_key=api_key)
            with client.messages.stream(
                model="claude-opus-4-7",
                max_tokens=16000,
                system=system,
                messages=[{"role": "user", "content":
                    f"Convert this Pine Script strategy to backtesting.py:\n\n{pine_script}"}],
            ) as stream:
                for text in stream.text_stream:
                    now = time.time()
                    if not first_chunk_logged:
                        log.info("bt_convert: first chunk after %.1fs", now - t0)
                        first_chunk_logged = True
                    if now - last_chunk_ts > 5:
                        log.warning("bt_convert: %.1fs gap between chunks (%d chars so far)",
                                    now - last_chunk_ts, chars)
                    last_chunk_ts = now
                    chunks += 1
                    chars += len(text)
                    if chars - last_progress_log >= 1000:
                        log.info("bt_convert: progress %d chars / %d chunks at %.1fs",
                                 chars, chunks, now - t0)
                        last_progress_log = chars
                    yield f"data: {json.dumps({'type': 'chunk', 'text': text})}\n\n"
                try:
                    final = stream.get_final_message()
                    log.info("bt_convert: stream complete in %.1fs — %d chunks, %d chars, "
                             "stop_reason=%s, in=%d out=%d tok",
                             time.time() - t0, chunks, chars,
                             getattr(final, "stop_reason", "?"),
                             getattr(getattr(final, "usage", None), "input_tokens", -1),
                             getattr(getattr(final, "usage", None), "output_tokens", -1))
                except Exception as _fe:
                    log.info("bt_convert: stream complete in %.1fs — %d chunks, %d chars (no final msg: %s)",
                             time.time() - t0, chunks, chars, _fe)
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except Exception as e:
            log.exception("bt_convert: stream error after %.1fs / %d chars: %s",
                          time.time() - t0, chars, e)
            yield f"data: {json.dumps({'type': 'error', 'msg': str(e)})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/bt/convert/verify", methods=["POST"])
def bt_convert_verify():
    """Second-pass verification: diff the generated Python against the Pine source
    and return either {ok:true} or {ok:false, issues:[...], revised_code:"..."}.
    Streams back as SSE so the UI can show progress."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 503
    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({"error": "anthropic package not installed"}), 503

    body        = request.get_json(silent=True) or {}
    pine_script = (body.get("pine_script") or "").strip()
    python_code = (body.get("python_code") or "").strip()
    if not pine_script or not python_code:
        return jsonify({"error": "pine_script and python_code required"}), 400

    system = (
        "You are auditing a Pine Script → backtesting.py conversion for fidelity. "
        "Your job is to find ANY behavioral discrepancy between the Pine source and the "
        "Python Strategy class that would cause different trade counts, entry timing, exit timing, "
        "or P&L when run on the same data.\n\n"
        "CHECKLIST (run through every item):\n"
        "1. Session/RTH filters — does Pine gate entries by session? If yes, does Python enforce "
        "   the same time-of-day mask before entries?\n"
        "2. Previous-day H/L/C — if Pine uses RTH-tracked daily OHLC (curRTH* variables), does "
        "   Python filter to RTH bars BEFORE resampling? Or is it doing calendar-day resample that "
        "   includes overnight data?\n"
        "3. Crossover semantics — does Pine use same-bar (close > L and open < L) or ta.crossover "
        "   (close[1] < L and close[0] >= L)? Does Python match EXACTLY?\n"
        "4. Entry guards — do both versions prevent re-entry while in position (pyramiding=0)?\n"
        "5. Tick vs dollar units — are trail_points/loss parameters interpreted in the same unit "
        "   between Pine (ticks) and Python (dollars)?\n"
        "6. Trade fill timing — is _trade_on_close = True set? (Pine has process_orders_on_close=true)\n"
        "7. Stop/trail logic — does Python use the same activation distance and offset as Pine's "
        "   strategy.exit(trail_points, trail_offset, loss)?\n"
        "8. Stop/trail exit check — do trail/fixed-stop exits compare against self.data.Low[-1] "
        "   (long) or self.data.High[-1] (short), NOT self.data.Close[-1]? Close-only checks miss "
        "   intrabar stop hits that Pine's tick-level simulation would catch. Check BOTH the fixed "
        "   stop AND the trailing stop independently — a common bug pattern is correct fixed-stop "
        "   code (uses Low/High) paired with buggy trailing-stop code (uses Close). Grep the Python "
        "   for any `c <= trail_stop`, `c >= trail_stop`, `self.data.Close[-1] <= trail`, or "
        "   `Close[-1] >= trail` pattern — if found, flag as an issue and fix it to use Low[-1] "
        "   (long) or High[-1] (short).\n"
        "9. Timezone assumption — does the Python use idx.hour directly (treating index as ET) "
        "   without tz_localize/tz_convert? The backtester guarantees ET-local naive index; a "
        "   tz_localize('UTC') here would be a bug.\n"
        "10. Indicator math — EMA period, length, source column all match?\n"
        "11. Long-only vs long-short — does Python handle BOTH directions if Pine does?\n\n"
        "OUTPUT FORMAT (strict JSON, no markdown fences):\n"
        "If the conversion is faithful, output exactly:\n"
        '  {\"ok\": true, \"issues\": []}\n'
        "If ANY discrepancy exists, output:\n"
        '  {\"ok\": false, \"issues\": [\"short description of each issue\"], '
        '\"revised_code\": \"<complete fixed Python Strategy class, same rules as original conversion>\"}\n'
        "The revised_code MUST be the FULL replacement class from `from backtesting import Strategy` "
        "through the end — no markdown, no explanation text, no diff format. Just the runnable file."
    )

    def generate():
        t0 = time.time()
        chunks = 0
        chars = 0
        last_chunk_ts = t0
        last_progress_log = 0
        first_chunk_logged = False
        log.info("bt_verify: stream open, pine=%d / python=%d chars",
                 len(pine_script), len(python_code))
        try:
            client = _anthropic.Anthropic(api_key=api_key)
            user_msg = (
                f"PINE SCRIPT:\n```\n{pine_script}\n```\n\n"
                f"GENERATED PYTHON:\n```\n{python_code}\n```\n\n"
                "Audit and return the JSON verdict."
            )
            with client.messages.stream(
                model="claude-opus-4-7",
                max_tokens=16000,
                system=system,
                messages=[{"role": "user", "content": user_msg}],
            ) as stream:
                for text in stream.text_stream:
                    now = time.time()
                    if not first_chunk_logged:
                        log.info("bt_verify: first chunk after %.1fs", now - t0)
                        first_chunk_logged = True
                    if now - last_chunk_ts > 5:
                        log.warning("bt_verify: %.1fs gap between chunks (%d chars so far)",
                                    now - last_chunk_ts, chars)
                    last_chunk_ts = now
                    chunks += 1
                    chars += len(text)
                    if chars - last_progress_log >= 1000:
                        log.info("bt_verify: progress %d chars / %d chunks at %.1fs",
                                 chars, chunks, now - t0)
                        last_progress_log = chars
                    yield f"data: {json.dumps({'type': 'chunk', 'text': text})}\n\n"
                try:
                    final = stream.get_final_message()
                    log.info("bt_verify: stream complete in %.1fs — %d chunks, %d chars, "
                             "stop_reason=%s, in=%d out=%d tok",
                             time.time() - t0, chunks, chars,
                             getattr(final, "stop_reason", "?"),
                             getattr(getattr(final, "usage", None), "input_tokens", -1),
                             getattr(getattr(final, "usage", None), "output_tokens", -1))
                except Exception as _fe:
                    log.info("bt_verify: stream complete in %.1fs — %d chunks, %d chars (no final msg: %s)",
                             time.time() - t0, chunks, chars, _fe)
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except Exception as e:
            log.exception("bt_verify: stream error after %.1fs / %d chars: %s",
                          time.time() - t0, chars, e)
            yield f"data: {json.dumps({'type': 'error', 'msg': str(e)})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/bt/strategy-source", methods=["POST"])
def api_bt_strategy_source():
    """Return a strategy class renamed to ResearchStrategy with best params applied.
    Used by the optimizer page's Agent Loop to kick off parameter fine-tuning."""
    import inspect, re as _re
    data   = request.get_json(silent=True) or {}
    slug   = data.get("slug", "").strip()
    params = data.get("params", {})  # {param_name: value}

    from strategies.bt_strategies import STRATEGIES
    raw = None

    if slug in STRATEGIES:
        # Built-in strategy — extract source via inspect
        cls = STRATEGIES[slug]
        try:
            raw = inspect.getsource(cls)
            # Rename class
            raw = _re.sub(r'\bclass\s+' + _re.escape(cls.__name__) + r'\b',
                          'class ResearchStrategy', raw)
        except Exception as e:
            return jsonify({"error": f"Could not get source: {e}"}), 500
    else:
        # User-uploaded strategy — code is stored in DB
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(f"SELECT code FROM user_strategies WHERE slug = {placeholder()}", (slug,))
        row = cur.fetchone()
        conn.close()
        if not row or not row[0]:
            return jsonify({"error": f"Strategy '{slug}' not found"}), 404
        raw = _strip_code_fences(row[0])
        # Rename whatever class is in there to ResearchStrategy
        raw = _re.sub(r'\bclass\s+\w+\s*\(', 'class ResearchStrategy(', raw, count=1)

    # Apply best params: replace each `param = <value>` class-level default
    for param, value in params.items():
        raw = _re.sub(
            r'(^\s{4}' + _re.escape(param) + r'\s*=\s*)[^\n#]+',
            lambda m, v=value: m.group(1) + repr(v),
            raw, flags=_re.MULTILINE
        )

    # Ensure required imports are present
    preamble = (
        "import numpy as np\nimport pandas as pd\n"
        "from backtesting import Strategy\n"
        "from strategies.bt_strategies import _sma, _ema, _atr, _rsi, _bbands, _macd\n\n"
    )
    # Only prepend if not already present
    if "from backtesting import" not in raw:
        raw = preamble + raw
    return jsonify({"code": raw, "class_name": "ResearchStrategy"})


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
    data_source    = body.get("data_source", "alpaca")
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
            strategy_code = _strip_code_fences(strategy_code)
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
            setattr(sys.modules["__main__"], strategy_cls.__name__, strategy_cls)
        except SyntaxError as e:
            lines = strategy_code.splitlines()
            bad = lines[e.lineno - 1].strip() if e.lineno and e.lineno <= len(lines) else ''
            return jsonify({"error": f"Syntax error (line {e.lineno}): {e.msg}  →  {bad}"}), 400
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
        keep = [c for c in ("Open","High","Low","Close","Volume") if c in df.columns]
        df = df[keep].dropna()
        if "Volume" not in df.columns:
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
    data_source    = body.get("data_source", "alpaca")
    maximize       = body.get("maximize",    "Sharpe Ratio")
    param_ranges   = body.get("param_ranges", {})
    strategy_code  = body.get("strategy_code", "")
    multi_ticker   = bool(body.get("multi_ticker", False))
    agg_metric     = body.get("agg_metric", "avg_sharpe")   # avg_sharpe|avg_pf|pass_count|min_pf
    trade_size     = int(body.get("trade_size", 0) or 0)    # 0 = use cash-based sizing

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
            exec_code = _strip_code_fences(exec_code)
            ns = {"Strategy": _Strategy, "np": _np, "numpy": _np, "pd": _pd, "pandas": _pd}
            try:
                exec(exec_code, ns)
            except SyntaxError as e:
                lines = exec_code.splitlines()
                bad = lines[e.lineno - 1].strip() if e.lineno and e.lineno <= len(lines) else ''
                yield _sse({"type": "error", "msg": f"Strategy syntax error (line {e.lineno}): {e.msg}  →  {bad}"}); return
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

        # Fixed position size wrapper — subclass to inject share count on every order
        if trade_size > 0:
            _sz = trade_size
            class _FixedSize(strategy_cls):
                def buy(self, **kwargs):
                    kwargs.setdefault('size', _sz); return super().buy(**kwargs)
                def sell(self, **kwargs):
                    kwargs.setdefault('size', _sz); return super().sell(**kwargs)
            _FixedSize.__name__ = strategy_cls.__name__
            strategy_cls = _FixedSize

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

        def _snap_eq(s_run, n=200):
            """Snapshot equity curve immediately — bt._equity_curve is overwritten on each run."""
            try:
                ec   = s_run._equity_curve["Equity"]
                step = max(1, len(ec) // n)
                ec_s = ec.iloc[::step]
                return {"dates":  [str(d)[:10] for d in ec_s.index],
                        "values": [round(float(v), 2) for v in ec_s.values]}
            except Exception:
                return None

        # ── Multi-ticker mode: find params that work across all tickers ──────
        if multi_ticker and len(tickers) > 1:
            import itertools as _it, random as _rnd

            def _f(v, n=2):
                try:
                    fv = float(v or 0)
                    if fv != fv or fv == float('inf') or fv == float('-inf'): return None
                    return round(fv, n)
                except Exception: return 0

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

            # Step 1: Load all datasets upfront
            all_pairs = [(t, tf) for t in tickers for tf in timeframes]
            datasets  = {}   # (ticker, tf) → df
            yield _sse({"type": "progress", "msg": f"Loading data for {len(all_pairs)} ticker/TF pairs…", "pct": 1})
            for tk, tf in all_pairs:
                try:
                    if data_source == "alpaca":
                        from strategies.data import fetch_bars_alpaca
                        raw = fetch_bars_alpaca(tk, start_date, end_date, tf)
                    else:
                        from strategies.data import fetch_bars
                        raw = fetch_bars(tk, start_date, end_date, tf)
                    if len(raw) < 30:
                        yield _sse({"type": "warning", "msg": f"{tk}/{tf}: only {len(raw)} bars — skipped"}); continue
                    df = _pd.DataFrame(raw).set_index("time")
                    df.index = _pd.to_datetime(df.index)
                    df.columns = [c.title() for c in df.columns]
                    keep = [c for c in ("Open","High","Low","Close","Volume") if c in df.columns]
                    df = df[keep].dropna()
                    if "Volume" not in df.columns: df["Volume"] = 0
                    if tf in _INTRADAY_TF: df = _filter_rth(df)
                    datasets[(tk, tf)] = df
                except Exception as _de:
                    yield _sse({"type": "warning", "msg": f"{tk}/{tf} data error: {_de}"})
            if not datasets:
                yield _sse({"type": "error", "msg": "No data loaded for any ticker"}); return

            # Step 2: Sample param combos (fewer trials since each runs on all tickers)
            if opt_kwargs:
                grid_size = 1
                for vals in opt_kwargs.values(): grid_size *= len(vals)
                n_multi = min(max(30, 240 // len(datasets)), grid_size)
                yield _sse({"type": "progress", "msg": f"Sampling {n_multi} param combos across {len(datasets)} datasets…", "pct": 5})
                combos, seen = [], set()
                attempts = 0
                while len(combos) < n_multi and attempts < n_multi * 10:
                    attempts += 1
                    p = {k: _rnd.choice(v) for k, v in opt_kwargs.items()}
                    key = tuple(sorted(p.items()))
                    if key not in seen:
                        seen.add(key); combos.append(p)
            else:
                combos = [{}]

            # Step 3: Score each combo across all tickers
            def _agg_score(ticker_stats):
                valid = [s for s in ticker_stats if s and s.get("# Trades", 0) >= 3]
                if not valid: return 0.0
                if agg_metric == "avg_pf":
                    return sum(_f(s.get("Profit Factor"), 4) or 0 for s in valid) / len(valid)
                elif agg_metric == "pass_count":
                    return sum(1 for s in valid
                               if (_f(s.get("Profit Factor"), 4) or 0) >= 1.3
                               and abs(_f(s.get("Max. Drawdown [%]"), 2) or 100) <= 20
                               and s.get("# Trades", 0) >= 10)
                elif agg_metric == "min_pf":
                    pfs = [(_f(s.get("Profit Factor"), 4) or 0) for s in valid]
                    return min(pfs) if pfs else 0
                else:   # avg_sharpe (default)
                    return sum(_f(s.get("Sharpe Ratio"), 4) or 0 for s in valid) / len(valid)

            scored_combos = []
            for ci, p_ov in enumerate(combos):
                pct = 5 + int(ci / len(combos) * 85)
                yield _sse({"type": "progress",
                            "msg": f"Combo {ci+1}/{len(combos)}: {' · '.join(f'{k}={v}' for k,v in p_ov.items()) or 'defaults'}",
                            "pct": pct})
                ticker_stats = {}
                ref_eq = None   # equity curve from first successful ticker (for chart)
                for (tk, tf), df in datasets.items():
                    try:
                        bt = Backtest(df, strategy_cls, cash=cash, commission=commission,
                                      exclusive_orders=True,
                                      trade_on_close=getattr(strategy_cls, "_trade_on_close", False))
                        s = bt.run(**p_ov)
                        ticker_stats[(tk, tf)] = _make_stats(s)
                        if ref_eq is None:
                            ref_eq = _snap_eq(s)   # reuse helper from single-ticker path
                    except Exception:
                        ticker_stats[(tk, tf)] = None

                combined = _agg_score(list(ticker_stats.values()))
                scored_combos.append((p_ov, combined, ticker_stats, ref_eq))

            # Step 4: Yield top 3
            scored_combos.sort(key=lambda x: x[1], reverse=True)
            yield _sse({"type": "progress", "msg": "Ranking results…", "pct": 95})
            for rank, (p_ov, combined, ticker_stats, ref_eq) in enumerate(scored_combos[:3]):
                all_stats = [s for s in ticker_stats.values() if s]
                n_pass = sum(1 for s in all_stats
                             if (s.get("Profit Factor") or 0) >= 1.3
                             and abs(s.get("Max. Drawdown [%]") or 100) <= 20
                             and s.get("# Trades", 0) >= 10)
                n = max(len(all_stats), 1)
                avg_ret    = round(sum(s.get("Return [%]")      or 0 for s in all_stats) / n, 2)
                avg_sharpe = round(sum(s.get("Sharpe Ratio")    or 0 for s in all_stats) / n, 3)
                avg_pf     = round(sum(s.get("Profit Factor")   or 0 for s in all_stats) / n, 3)
                avg_wr     = round(sum(s.get("Win Rate [%]")    or 0 for s in all_stats) / n, 1)
                avg_dd     = round(sum(s.get("Max. Drawdown [%]") or 0 for s in all_stats) / n, 2)
                min_pf     = round(min((s.get("Profit Factor")  or 0) for s in all_stats), 3) if all_stats else 0
                avg_trades = int(sum(s.get("# Trades")          or 0 for s in all_stats) / n)
                avg_equity = round(sum(s.get("Equity Final [$]") or cash for s in all_stats) / n, 2)
                per_ticker_out = {f"{tk}/{tf}": ts for (tk, tf), ts in ticker_stats.items()}
                row = {
                    "type": "result", "run_id": run_id,
                    "strategy": strategy_name,
                    "ticker": "MULTI", "timeframe": timeframes[0] if len(timeframes)==1 else "/".join(timeframes),
                    "multi_ticker": True,
                    "params": p_ov, "rank": rank + 1,
                    "score": round(combined, 4),
                    "equity_curve": ref_eq,
                    "agg_metric": agg_metric,
                    "stats": {
                        "Return [%]":        avg_ret,
                        "Sharpe Ratio":      avg_sharpe,
                        "Profit Factor":     avg_pf,
                        "Win Rate [%]":      avg_wr,
                        "Max. Drawdown [%]": avg_dd,
                        "Min PF":            min_pf,
                        "# Trades":          avg_trades,
                        "Equity Final [$]":  avg_equity,
                        "Passes":            f"{n_pass}/{len(datasets)}",
                    },
                    "per_ticker": per_ticker_out,
                    "n_pass": n_pass, "n_total": len(datasets),
                }
                yield _sse(row)
            yield _sse({"type": "done", "run_id": run_id})
            return   # ← skip the per-ticker loop below

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
                    keep = [c for c in ("Open","High","Low","Close","Volume") if c in df.columns]
                    df = df[keep].dropna()
                    if "Volume" not in df.columns:
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

                    combo_results = []   # list of (params_dict, stats, eq_curve)

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
                                _s = bt.run(**p_ov)
                                combo_results.append((p_ov, _s, _snap_eq(_s)))
                        else:
                            # Random search - bypass bt.optimize() which hangs on large spaces.
                            # Sample combos uniformly; more trials than SAMBO's max_tries.
                            _N_RANDOM = {"5m": 80, "15m": 100, "30m": 120, "1h": 150, "1d": 200}
                            n_trials = min(_N_RANDOM.get(tf, 80), grid_size)
                            yield _sse({"type": "progress",
                                        "msg": f"  Random search: {n_trials} trials from {grid_size:,} combos",
                                        "pct": pct + 1})
                            import random as _rnd
                            seen = set()
                            attempts = 0
                            while len(combo_results) < n_trials and attempts < n_trials * 5:
                                attempts += 1
                                p_ov = {k: _rnd.choice(vals) for k, vals in opt_kwargs.items()}
                                key = tuple(sorted(p_ov.items()))
                                if key in seen:
                                    continue
                                seen.add(key)
                                _s = bt.run(**p_ov)
                                combo_results.append((p_ov, _s, _snap_eq(_s)))
                    else:
                        yield _sse({"type": "progress",
                                    "msg": f"Running {ticker}/{tf} at defaultsâ€¦", "pct": pct + 1})
                        _s = bt.run()
                        combo_results.append(({}, _s, _snap_eq(_s)))

                    # Sort descending by maximize metric
                    combo_results.sort(
                        key=lambda x: float(x[1].get(maximize) or 0), reverse=True)

                    p_ph = placeholder()
                    for rank, (p_ov, s, eq_curve) in enumerate(combo_results):
                        sd    = _make_stats(s)
                        score = _f(s.get(maximize), 4)
                        # eq_curve was captured immediately after bt.run() to avoid
                        # bt._equity_curve being overwritten by subsequent runs

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


@app.route("/api/bt/fanout", methods=["POST"])
def bt_fanout():
    """Stream fan-out results: run fixed params across multiple tickers/timeframes.
    Body: {strategy, params, tickers, timeframes, gates:{min_pf, min_trades, max_dd_pct}}"""
    data       = request.get_json(silent=True) or {}
    slug       = data.get("strategy", "")
    params     = data.get("params", {})
    tickers    = [t.strip().upper() for t in data.get("tickers", []) if t.strip()]
    timeframes = data.get("timeframes", ["5m"])
    gates      = data.get("gates", {})
    min_pf     = float(gates.get("min_pf",     1.3))
    min_trades = int(gates.get("min_trades",   10))
    max_dd     = float(gates.get("max_dd_pct", 20.0))

    if not slug or not tickers:
        return jsonify({"error": "strategy and tickers required"}), 400

    # Capture all variables needed by generate() explicitly
    _slug, _params, _tickers, _timeframes = slug, params, tickers, timeframes
    _min_pf, _min_trades, _max_dd = min_pf, min_trades, max_dd

    def generate():
        import json as _j
        def _sse(obj): return f"data: {_j.dumps(obj)}\n\n"
        try:
            from backtesting import Backtest as _BT
        except ImportError:
            yield _sse({"type": "error", "msg": "backtesting not installed"})
            return

        try:
            conn = get_db(); cur = conn.cursor(); p = placeholder()
            cur.execute(f"SELECT code FROM user_strategies WHERE slug={p}", (_slug,))
            row = cur.fetchone(); conn.close()
        except Exception as _dbe:
            yield _sse({"type": "error", "msg": f"DB error: {_dbe}"})
            return

        if not row:
            yield _sse({"type": "error", "msg": f"Strategy {_slug!r} not found"})
            return

        code = _strip_code_fences(row[0] if DATABASE_URL else row["code"])
        ns   = {}
        try:
            exec(compile(code, "<fanout>", "exec"), ns)
        except Exception as _ce:
            yield _sse({"type": "error", "msg": f"Strategy compile error: {_ce}"})
            return

        import backtesting as _bktst
        strategy_cls = next(
            (v for v in ns.values()
             if isinstance(v, type) and issubclass(v, _bktst.Strategy) and v is not _bktst.Strategy),
            None,
        )
        if not strategy_cls:
            yield _sse({"type": "error", "msg": "No Strategy subclass found in code"})
            return

        total = len(_tickers) * len(_timeframes)
        done  = 0
        for ticker in _tickers:
            for tf in _timeframes:
                done += 1
                pct = round(done / total * 100)
                yield _sse({"type": "progress", "ticker": ticker, "tf": tf, "pct": pct})
                try:
                    from strategies.data import fetch_bars_alpaca, fetch_bars
                    tf_map = {"5m": "5m", "15m": "15m", "30m": "30m",
                              "1h": "1h", "1d": "1d"}
                    interval = tf_map.get(tf, tf)
                    try:
                        df = fetch_bars_alpaca(ticker, start="2025-01-01",
                                               end=None, interval=interval)
                    except Exception:
                        df = fetch_bars(ticker, start="2025-01-01",
                                        end=None, interval=interval)
                    if not df or len(df) < 50:
                        yield _sse({"type": "result", "ticker": ticker, "tf": tf,
                                    "status": "error", "reason": "insufficient data"})
                        continue

                    import pandas as _pd2
                    df = _pd2.DataFrame(df).set_index("time")
                    df.index = _pd2.to_datetime(df.index)
                    df.columns = [c.title() for c in df.columns]
                    keep = [c for c in ("Open","High","Low","Close","Volume") if c in df.columns]
                    df = df[keep].dropna()
                    if "Volume" not in df.columns:
                        df["Volume"] = 0
                    if tf not in ("1d",):
                        df = _filter_rth(df)

                    cash       = float(os.environ.get("BT_CASH",       "10000"))
                    commission = float(os.environ.get("BT_COMMISSION", "0.001"))
                    bt = _BT(df, strategy_cls, cash=cash, commission=commission,
                             exclusive_orders=True,
                             trade_on_close=getattr(strategy_cls, "_trade_on_close", False))
                    s  = bt.run(**_params)

                    pf     = float(s.get("Profit Factor")         or 0)
                    trades = int(s.get("# Trades")                or 0)
                    dd     = abs(float(s.get("Max. Drawdown [%]") or 0))
                    ret    = float(s.get("Return [%]")            or 0)
                    sharpe = float(s.get("Sharpe Ratio")          or 0)
                    wr     = float(s.get("Win Rate [%]")          or 0)

                    if trades < _min_trades:
                        status = "fail"; reason = f"only {trades} trades (need ≥{_min_trades})"
                    elif pf < _min_pf:
                        status = "fail"; reason = f"PF {pf:.2f} < {_min_pf}"
                    elif dd > _max_dd:
                        status = "fail"; reason = f"drawdown {dd:.1f}% > {_max_dd}%"
                    else:
                        status = "pass"; reason = ""

                    tf_label = tf.upper().replace("M", "MIN")
                    name     = f"CAM_{ticker}_{_slug.upper()}_{tf_label}"
                    nodes    = [
                        {"type": "strategy", "value": name},
                        {"type": "quantity",  "amount": 1},
                        {"type": "broker",    "value": "alpaca-paper"},
                    ]
                    yield _sse({"type": "result", "ticker": ticker, "tf": tf,
                                "status": status, "reason": reason,
                                "name": name, "nodes": nodes,
                                "stats": {"pf": round(pf, 2), "trades": trades,
                                          "return_pct": round(ret, 2),
                                          "sharpe": round(sharpe, 2),
                                          "max_dd": round(dd, 2),
                                          "win_rate": round(wr, 1)}})
                except Exception as _e:
                    log.warning("Fan-out %s/%s error: %s", ticker, tf, _e)
                    yield _sse({"type": "result", "ticker": ticker, "tf": tf,
                                "status": "error", "reason": str(_e)[:120]})

        yield _sse({"type": "done"})

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
    """Return filled Alpaca orders with resolved strategy names, cached.
    Pass ?account=2 to query the Alpaca Refined (second) account."""
    use_acct2 = request.args.get("account") == "2"
    broker    = alpaca_broker2 if use_acct2 else alpaca_broker
    if broker is None:
        return jsonify([])
    # Check if we already have strategy-annotated data in the fills cache
    now = time.time()
    if use_acct2:
        if now - _alpaca2_fills_cache["ts"] < ALPACA_CACHE_TTL and _alpaca2_fills_cache["data"]:
            return jsonify(_alpaca2_fills_cache["data"])
    else:
        if now - _alpaca_fills_cache["ts"] < ALPACA_CACHE_TTL and _alpaca_fills_cache["data"]:
            return jsonify(_alpaca_fills_cache["data"])
    try:
        fills = _get_cached_fills_2() if use_acct2 else _get_cached_fills()
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
                action   = _canonical_action(t.get("action"))
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
        # Write annotated data back into the cache (keeps ts from _get_cached_fills).
        # Reference the module-level cache directly so we hit whatever dict the
        # _get_cached_fills_*() call most recently bound.
        if use_acct2:
            _alpaca2_fills_cache["data"] = fills
        else:
            _alpaca_fills_cache["data"] = fills
        return jsonify(fills)
    except Exception as e:
        log.error("alpaca_trades error: %s", e)
        return jsonify([])


@app.route("/api/alpaca/after-hours")
def alpaca_after_hours():
    """Return fills that executed outside regular market hours (9:30–16:00 ET)."""
    if alpaca_broker is None:
        return jsonify([])
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    MARKET_OPEN  = (9, 30)
    MARKET_CLOSE = (16, 0)
    fills = _get_cached_fills()
    after_hours = []
    for f in fills:
        t_str = f.get("time") or ""
        if not t_str:
            continue
        try:
            dt_utc = _dt.fromisoformat(t_str.replace("Z", "+00:00"))
            dt_et  = dt_utc.astimezone(ET)
            hm = (dt_et.hour, dt_et.minute)
            if hm < MARKET_OPEN or hm >= MARKET_CLOSE:
                after_hours.append({
                    "time":     t_str,
                    "time_et":  dt_et.strftime("%Y-%m-%d %H:%M:%S ET"),
                    "symbol":   f.get("symbol"),
                    "side":     f.get("side"),
                    "shares":   f.get("shares"),
                    "price":    f.get("price"),
                    "strategy": f.get("strategy"),
                    "order_id": f.get("order_id"),
                })
        except Exception:
            continue
    after_hours.sort(key=lambda x: x["time"], reverse=True)
    return jsonify(after_hours)


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
        action   = _canonical_action(t.get("action"))
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
            queue = open_shorts.setdefault(key, [])
            while qty > 0 and queue:
                entry_price, entry_qty, entry_time, entry_id = queue.pop(0)
                m = min(qty, entry_qty)
                if in_window:
                    closed.append({"pnl": (entry_price - price) * m, "time": received,
                                   "strategy": strategy, "ticker": ticker,
                                   "entry_id": entry_id, "exit_id": trade_id,
                                   "entry_time": entry_time})
                qty -= m
                if entry_qty > m:
                    queue.insert(0, (entry_price, entry_qty - m, entry_time, entry_id))
            if qty > 0:
                open_longs.setdefault(key, []).append((price, qty, received, trade_id))
        elif action == "SELL":
            # Closes an open long; otherwise opens a new short
            queue = open_longs.setdefault(key, [])
            while qty > 0 and queue:
                entry_price, entry_qty, entry_time, entry_id = queue.pop(0)
                m = min(qty, entry_qty)
                if in_window:
                    closed.append({"pnl": (price - entry_price) * m, "time": received,
                                   "strategy": strategy, "ticker": ticker,
                                   "entry_id": entry_id, "exit_id": trade_id,
                                   "entry_time": entry_time})
                qty -= m
                if entry_qty > m:
                    queue.insert(0, (entry_price, entry_qty - m, entry_time, entry_id))
            if qty > 0:
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
            "time":       c["time"],
            "value":      round(cumulative, 2),
            "pnl":        round(c["pnl"], 2),
            "strategy":   c.get("strategy"),
            "ticker":     c.get("ticker"),
            "entry_id":   c.get("entry_id"),
            "exit_id":    c.get("exit_id"),
            "entry_time": c.get("entry_time"),
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
        action   = _canonical_action(t.get("action"))
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


def _sharpe_from_pnls(pnls):
    """Trade-level Sharpe: mean(pnl) / sample_std(pnl). Returns None if < 2 trades."""
    import math as _math
    n = len(pnls)
    if n < 2: return None
    mean = sum(pnls) / n
    std  = _math.sqrt(sum((p - mean) ** 2 for p in pnls) / (n - 1))
    return round(mean / std, 2) if std > 0 else None


def _consec_losses_from_trades(trade_list):
    """Count consecutive losing individual trades from most recent backward."""
    sorted_pnls = [t["pnl"] for t in sorted(trade_list, key=lambda t: t.get("exit_time", ""))]
    count = 0
    for p in reversed(sorted_pnls):
        if p <= 0:
            count += 1
        else:
            break
    return count


def _consecutive_losing_days(trade_list):
    """Count how many of the most recent consecutive trading days had negative P&L."""
    if not trade_list:
        return 0
    by_date = {}
    for t in trade_list:
        d = t.get("date") or (t.get("exit_time") or "")[:10]
        if d:
            by_date[d] = round(by_date.get(d, 0.0) + t["pnl"], 2)
    if not by_date:
        return 0
    streak = 0
    for d in sorted(by_date, reverse=True):
        if by_date[d] < 0:
            streak += 1
        else:
            break
    return streak


def _consecutive_winning_days(trade_list):
    """Count how many of the most recent consecutive trading days had positive P&L."""
    if not trade_list:
        return 0
    by_date = {}
    for t in trade_list:
        d = t.get("date") or (t.get("exit_time") or "")[:10]
        if d:
            by_date[d] = round(by_date.get(d, 0.0) + t["pnl"], 2)
    if not by_date:
        return 0
    streak = 0
    for d in sorted(by_date, reverse=True):
        if by_date[d] > 0:
            streak += 1
        else:
            break
    return streak


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
        action    = _canonical_action(t.get("action"))
        sentiment = (t.get("sentiment") or "").strip().lower()
        ticker    = (t.get("ticker") or "").strip().upper()
        strategy  = (t.get("strategy") or "Unknown").strip()
        received  = t.get("received_at") or ""
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

        # Classify intent: sentiment is the source of truth when present.
        # TV alert templates emit {{strategy.market_position}} as the position
        # state AFTER the fill — "flat" = exit, "long"/"short" = entry. Using
        # this instead of bare action avoids a race condition when two alerts
        # (exit + reversal entry) fire on the same bar and arrive out of order.
        if sentiment == "flat":
            intent = "exit"
        elif sentiment == "long":
            intent = "enter_long"
        elif sentiment == "short":
            intent = "enter_short"
        else:
            intent = "legacy"  # sentiment missing — fall back to action-order heuristic

        if action == "BUY":
            if intent == "enter_long":
                open_longs.setdefault(key, []).append((price, qty, received))
                continue
            # "exit" or "legacy" — try to close a short
            queue = open_shorts.setdefault(key, [])
            while qty > 0 and queue:
                entry_price, entry_qty, entry_time = queue.pop(0)
                m = min(qty, entry_qty)
                closed.append({"pnl": round((entry_price - price) * m, 2), "strategy": strategy,
                               "ticker": ticker, "date": date_str, "side": "SHORT",
                               "entry_price": entry_price, "exit_price": price, "qty": m,
                               "entry_time": entry_time, "exit_time": received})
                qty -= m
                if entry_qty > m:
                    queue.insert(0, (entry_price, entry_qty - m, entry_time))
            # Only legacy mode opens a new long from unpaired BUY. An explicit
            # sentiment=flat signal never opens a position.
            if qty > 0 and intent == "legacy":
                open_longs.setdefault(key, []).append((price, qty, received))
        elif action == "SELL":
            if intent == "enter_short":
                open_shorts.setdefault(key, []).append((price, qty, received))
                continue
            queue = open_longs.setdefault(key, [])
            while qty > 0 and queue:
                entry_price, entry_qty, entry_time = queue.pop(0)
                m = min(qty, entry_qty)
                closed.append({"pnl": round((price - entry_price) * m, 2), "strategy": strategy,
                               "ticker": ticker, "date": date_str, "side": "LONG",
                               "entry_price": entry_price, "exit_price": price, "qty": m,
                               "entry_time": entry_time, "exit_time": received})
                qty -= m
                if entry_qty > m:
                    queue.insert(0, (entry_price, entry_qty - m, entry_time))
            if qty > 0 and intent == "legacy":
                open_shorts.setdefault(key, []).append((price, qty, received))

    def _avg_hold_mins(trade_list):
        from datetime import datetime as _dth
        durations = []
        for t in trade_list:
            try:
                et = _dth.fromisoformat((t.get("entry_time") or "").replace("Z", "+00:00"))
                xt = _dth.fromisoformat((t.get("exit_time")  or "").replace("Z", "+00:00"))
                diff = (xt - et).total_seconds() / 60
                if 0 < diff < 600:  # ignore implausible durations (> 10 h)
                    durations.append(diff)
            except Exception:
                pass
        return round(sum(durations) / len(durations), 1) if durations else None

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
            "trades":               len(trade_list),
            "wins":                 len(wins),
            "losses":               len(losses),
            "win_rate":             round(len(wins) / len(trade_list) * 100, 1),
            "profit_factor":        pf,
            "total_pnl":            total_pnl,
            "avg_win":              round(gross_win  / len(wins),   2) if wins   else 0,
            "avg_loss":             round(-gross_loss / len(losses), 2) if losses else 0,
            "largest_win":          round(max(wins),  2) if wins   else 0,
            "largest_loss":         round(min(losses), 2) if losses else 0,
            "avg_hold_mins":        _avg_hold_mins(trade_list),
            "consec_losing_days":   _consecutive_losing_days(trade_list),
            "consec_winning_days":  _consecutive_winning_days(trade_list),
            "sharpe":               _sharpe_from_pnls([t["pnl"] / max(float(t.get("qty") or 1), 0.01) for t in trade_list]),
            "consec_losses":        _consec_losses_from_trades(trade_list),
        }

    # Separate orphan pairs (phantom round-trips from stale/mispaired signals
    # such as overnight holds on intraday strategies or direction mismatches).
    # Orphans are returned to the UI but excluded from leaderboard stats.
    orphans, closed_clean = [], []
    for c in closed:
        is_orph, reason = _classify_orphan(c)
        if is_orph:
            orphans.append({**c, "orphan_reason": reason})
        else:
            closed_clean.append(c)
    closed = closed_clean

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
    # Directional split — feeds the "Long Win Rate / Short Win Rate" cards on
    # the analysis page so the user can see which side of the book is performing.
    overall["long"]  = _stats_from_trades([c for c in closed if c.get("side") == "LONG"])  or {}
    overall["short"] = _stats_from_trades([c for c in closed if c.get("side") == "SHORT"]) or {}

    return {
        "overall":      overall,
        "per_strategy": per_strategy,
        "per_ticker":   per_ticker,
        "daily":        daily,
        "weekly":       weekly,
        "closed":       closed,
        "orphans":      orphans,
    }


@app.route("/api/analysis")
def api_analysis():
    try:
        return jsonify(_build_analysis_stats())
    except Exception as e:
        log.exception("Analysis error")
        return jsonify({"error": str(e)}), 500


_INTRADAY_TOKENS = ("_1MIN", "_01MIN", "_3MIN", "_03MIN", "_5MIN", "_05MIN", "_15MIN", "_30MIN")

def _classify_orphan(pair):
    """Return (is_orphan, reason) for a closed round-trip.

    Orphan rules (any match):
    (a) slug contains _LONG_/_SHORT_ and pair side contradicts it
    (b) intraday strategy slug (*_5MIN etc.) but entry/exit on different days
    (c) hold duration > 8 hours regardless of slug
    """
    from datetime import datetime as _dt
    strat = (pair.get("strategy") or "").upper()
    side  = pair.get("side", "")
    if "_LONG_" in strat and side == "SHORT":
        return True, "long-only strategy with short pair"
    if "_SHORT_" in strat and side == "LONG":
        return True, "short-only strategy with long pair"
    et_str = pair.get("entry_time", "") or ""
    xt_str = pair.get("exit_time",  "") or ""
    is_intraday = any(tok in strat for tok in _INTRADAY_TOKENS)
    if is_intraday and et_str[:10] and xt_str[:10] and et_str[:10] != xt_str[:10]:
        return True, "intraday strategy with cross-day hold"
    try:
        et = _dt.fromisoformat(et_str.replace("Z", "+00:00"))
        xt = _dt.fromisoformat(xt_str.replace("Z", "+00:00"))
        if (xt - et).total_seconds() > 8 * 3600:
            return True, "hold > 8h"
    except Exception:
        pass
    return False, ""


def _fetch_post_exit_range(ticker: str, exit_time_str: str):
    """Return (max_high, min_low) from 1-min Alpaca bars for 30 min after exit_time.
    Returns (None, None) on any error or if alpaca_broker is not configured."""
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        import datetime as _pdt
        if alpaca_broker is None:
            return None, None
        _client = StockHistoricalDataClient(
            api_key    = alpaca_broker._key,
            secret_key = alpaca_broker._secret,
        )
        _exit_dt = _pdt.datetime.fromisoformat(exit_time_str.replace("Z", "+00:00"))
        _end_dt  = _exit_dt + _pdt.timedelta(minutes=30)
        req = StockBarsRequest(
            symbol_or_symbols = ticker.upper(),
            timeframe         = TimeFrame(1, TimeFrameUnit.Minute),
            start             = _exit_dt,
            end               = _end_dt,
        )
        bars_df = _client.get_stock_bars(req)
        bars    = list(bars_df[ticker.upper()])
        if not bars:
            return None, None
        return max(float(b.high) for b in bars), min(float(b.low) for b in bars)
    except Exception as _pe:
        log.debug("post_exit_range %s: %s", ticker, _pe)
        return None, None


def _fetch_day_bars(ticker: str, date_str: str):
    """Fetch all 1-min bars for a ticker on a single market day via Alpaca.
    Returns list of bar objects sorted by timestamp, or [] on error."""
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        import datetime as _dt
        if alpaca_broker is None:
            return []
        client = StockHistoricalDataClient(
            api_key    = alpaca_broker._key,
            secret_key = alpaca_broker._secret,
        )
        day   = _dt.date.fromisoformat(date_str)
        start = _dt.datetime(day.year, day.month, day.day, 13,  0, tzinfo=_dt.timezone.utc)  # 8am ET
        end   = _dt.datetime(day.year, day.month, day.day, 21,  0, tzinfo=_dt.timezone.utc)  # 5pm ET
        req   = StockBarsRequest(
            symbol_or_symbols = ticker.upper(),
            timeframe         = TimeFrame(1, TimeFrameUnit.Minute),
            start             = start,
            end               = end,
        )
        bars_df = client.get_stock_bars(req)
        return list(bars_df[ticker.upper()])
    except Exception as _de:
        log.debug("fetch_day_bars %s %s: %s", ticker, date_str, _de)
        return []


def _simulate_exit(bars, entry_price: float, side: str,
                   trail_pct: float, trigger_pct: float,
                   stop_loss_pct: float, max_hold_mins: int,
                   entry_dt, cap_dt=None, cap_price=None,
                   stop_loss_dollars: float = 0.0, qty: float = 1.0,
                   trail_tiers: list = None, take_profit_pct: float = 0.0,
                   take_profit_dollars: float = 0.0):
    """Walk 1-min bars and return the simulated exit.

    cap_dt / cap_price: if supplied, bars past cap_dt are ignored. If no stop
    fires before cap_dt the function returns the cap_price with reason 'actual'.

    stop_loss_dollars: dollar-based hard stop. Converted to a per-share price
    level using qty. When both stop_loss_pct and stop_loss_dollars are set the
    tighter stop (closer to entry) fires first.

    Returns dict {exit_price, exit_time, reason, exit_mins} or None if no bars.
    """
    import datetime as _dt
    if not bars:
        return None
    is_long = side.upper() == "LONG"
    peak    = entry_price

    # Pre-compute dollar stop price level (tighter of % and $ wins)
    if stop_loss_dollars > 0 and qty > 0:
        dollar_sl_per_share = stop_loss_dollars / qty
        if is_long:
            _dollar_sl_px = entry_price - dollar_sl_per_share
        else:
            _dollar_sl_px = entry_price + dollar_sl_per_share
    else:
        _dollar_sl_px = None

    # Pre-compute dollar TP price level; if both % and $ are set, take whichever
    # fires sooner (lower for LONG, higher for SHORT — less movement required).
    if take_profit_dollars > 0 and qty > 0:
        dollar_tp_per_share = take_profit_dollars / qty
        if is_long:
            _dollar_tp_px = entry_price + dollar_tp_per_share
        else:
            _dollar_tp_px = entry_price - dollar_tp_per_share
    else:
        _dollar_tp_px = None

    for bar in bars:
        bar_ts = bar.timestamp
        if not isinstance(bar_ts, _dt.datetime):
            bar_ts = _dt.datetime.fromisoformat(str(bar_ts).replace("Z", "+00:00"))

        # Respect the actual-exit time cap before any stop logic
        if cap_dt is not None and bar_ts > cap_dt:
            hold_mins = (cap_dt - entry_dt).total_seconds() / 60
            return {"exit_price": round(cap_price, 4), "exit_time": str(cap_dt),
                    "reason": "actual", "exit_mins": round(hold_mins, 1)}

        hold_mins = (bar_ts - entry_dt).total_seconds() / 60
        high  = float(bar.high)
        low   = float(bar.low)
        close = float(bar.close)
        mid   = (high + low) / 2.0

        # Intra-bar ordering heuristic: use close position to infer which
        # extreme came first within the minute.
        #   LONG:  close > mid  → bullish bar → low came before high
        #   SHORT: close < mid  → bearish bar → high came before low
        # For the "first" extreme, check stop/trail against the pre-bar peak
        # before updating it, giving a more conservative (realistic) estimate
        # for tight trail percentages.

        def _exit(reason, px):
            return {"exit_price": round(px, 4), "exit_time": str(bar_ts),
                    "reason": reason, "exit_mins": round(hold_mins, 1)}

        def _check_exit(trail_px, sl_px, stop_fires, trail_fires, long):
            if stop_fires and trail_fires:
                better = (trail_px > sl_px) if long else (trail_px < sl_px)
                return _exit("trail", trail_px) if better else _exit("stop_loss", sl_px)
            if trail_fires: return _exit("trail",     trail_px)
            if stop_fires:  return _exit("stop_loss", sl_px)
            return None

        if is_long:
            _pct_tp_px = entry_price * (1 + take_profit_pct / 100) if take_profit_pct > 0 else None
            _candidates = [p for p in [_pct_tp_px, _dollar_tp_px] if p is not None]
            tp_px = min(_candidates) if _candidates else None  # lower fires sooner for LONG
            low_first = close > mid          # bullish bar: low before high
            if not low_first:               # bearish bar: HIGH came first
                # TP (on high) fires before stop (on low) in a bearish bar
                if tp_px is not None and high >= tp_px:
                    return _exit("take_profit", tp_px)
                peak = max(peak, high)        # update peak after TP check
            peak_gain_pct = (peak - entry_price) / entry_price * 100 if peak > entry_price else 0.0
            eff_trail = _get_tiered_trail(peak_gain_pct, trail_tiers, trail_pct) if trail_tiers else trail_pct
            _pct_sl_px = entry_price * (1 - stop_loss_pct / 100) if stop_loss_pct > 0 else None
            _sl_px = max(p for p in [_pct_sl_px, _dollar_sl_px] if p is not None) \
                     if (_pct_sl_px is not None or _dollar_sl_px is not None) else None
            _trail_px = None
            if eff_trail > 0:
                if (trigger_pct == 0) or (peak >= entry_price * (1 + trigger_pct / 100)):
                    _trail_px = peak * (1 - eff_trail / 100)
            result = _check_exit(_trail_px, _sl_px,
                                 _sl_px is not None and low <= _sl_px,
                                 _trail_px is not None and low <= _trail_px, True)
            if result: return result
            if low_first:                   # bullish bar: LOW already checked, HIGH fires now
                # TP (on high) fires after stop check in a bullish bar
                if tp_px is not None and high >= tp_px:
                    return _exit("take_profit", tp_px)
                peak = max(peak, high)        # update peak after TP check
        else:
            _pct_tp_px = entry_price * (1 - take_profit_pct / 100) if take_profit_pct > 0 else None
            _candidates = [p for p in [_pct_tp_px, _dollar_tp_px] if p is not None]
            tp_px = max(_candidates) if _candidates else None  # higher fires sooner for SHORT
            high_first = close < mid         # bearish bar: high before low
            if not high_first:              # bullish bar: LOW came first
                # TP (on low) fires before stop (on high) in a bullish bar
                if tp_px is not None and low <= tp_px:
                    return _exit("take_profit", tp_px)
                peak = min(peak, low)         # update trough after TP check
            peak_gain_pct = (entry_price - peak) / entry_price * 100 if peak < entry_price else 0.0
            eff_trail = _get_tiered_trail(peak_gain_pct, trail_tiers, trail_pct) if trail_tiers else trail_pct
            _pct_sl_px = entry_price * (1 + stop_loss_pct / 100) if stop_loss_pct > 0 else None
            _sl_px = min(p for p in [_pct_sl_px, _dollar_sl_px] if p is not None) \
                     if (_pct_sl_px is not None or _dollar_sl_px is not None) else None
            _trail_px = None
            if eff_trail > 0:
                if (trigger_pct == 0) or (peak <= entry_price * (1 - trigger_pct / 100)):
                    _trail_px = peak * (1 + eff_trail / 100)
            result = _check_exit(_trail_px, _sl_px,
                                 _sl_px is not None and high >= _sl_px,
                                 _trail_px is not None and high >= _trail_px, False)
            if result: return result
            if high_first:                  # bearish bar: HIGH already checked, LOW fires now
                # TP (on low) fires after stop check in a bearish bar
                if tp_px is not None and low <= tp_px:
                    return _exit("take_profit", tp_px)
                peak = min(peak, low)         # update trough after TP check

        if max_hold_mins > 0 and hold_mins >= max_hold_mins:
            return {"exit_price": round(float(bar.close), 4), "exit_time": str(bar_ts),
                    "reason": "max_hold", "exit_mins": round(hold_mins, 1)}

    # Reached end of bars without a stop firing
    if cap_dt is not None and cap_price is not None:
        hold_mins = (cap_dt - entry_dt).total_seconds() / 60
        return {"exit_price": round(cap_price, 4), "exit_time": str(cap_dt),
                "reason": "actual", "exit_mins": round(hold_mins, 1)}
    last    = bars[-1]
    last_ts = last.timestamp
    if not isinstance(last_ts, _dt.datetime):
        last_ts = _dt.datetime.fromisoformat(str(last_ts).replace("Z", "+00:00"))
    hold_mins = (last_ts - entry_dt).total_seconds() / 60
    return {"exit_price": round(float(last.close), 4), "exit_time": str(last_ts),
            "reason": "eod", "exit_mins": round(hold_mins, 1)}


@app.route("/api/alpaca/analysis")
def api_alpaca_analysis():
    """Same analysis as /api/analysis but using Alpaca fills.
    Pass ?account=2 to use the Alpaca Refined (second) account."""
    try:
        from datetime import datetime as _dt

        account = request.args.get("account", "1")
        use_acct2 = (account == "2")

        if use_acct2:
            if alpaca_broker2 is None:
                return jsonify({"error": "Alpaca account 2 not configured — set ALPACA_KEY2 + ALPACA_SECRET2"}), 400
        else:
            if alpaca_broker is None:
                return jsonify({"error": "Alpaca not configured"}), 400

        from_date    = request.args.get("from_date",    "")
        to_date      = request.args.get("to_date",      "")
        signals_only = request.args.get("signals_only", "0")
        exclude      = request.args.get("exclude",      "")

        _cache_key  = f"{from_date}|{to_date}|{signals_only}|{exclude}"
        _acache     = _alpaca2_analysis_cache if use_acct2 else _alpaca_analysis_cache
        _cached     = _acache.get(_cache_key)
        if _cached and (time.time() - _cached["ts"] < ALPACA_ANALYSIS_TTL):
            return jsonify(_cached["data"])

        fills = _get_cached_fills_2() if use_acct2 else _get_cached_fills()
        if not fills:
            return jsonify({"overall": {}, "per_strategy": {}, "per_ticker": {}, "daily": [], "weekly": [], "equity_curve": []})

        signals_only = (signals_only == "1")

        # Load the TV signals table once — used to build the (ticker, side) → signal
        # lookup the pairing helper needs, and reused later by the signals_only branch.
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT ticker, action, sentiment, received_at, strategy, exec_status FROM trades ORDER BY id ASC")
        rows = cur.fetchall()
        trades_db = [dict(r) if not DATABASE_URL else dict(zip([d[0] for d in cur.description], r)) for r in rows]
        conn.close()

        signal_lookup = _build_signal_lookup_for_alpaca(trades_db)

        # Date filter, dedupe, sort, LIFO pair + orphan-classify globally.
        # Pairing happens *within* the date range — flat-at-EOD style, avoids leaking
        # prior-day pairing imperfections (partial fills, manual closes) into today's view.
        paired       = _pair_alpaca_fills_lifo(fills, from_date=from_date, to_date=to_date, signal_lookup=signal_lookup)
        deduped      = paired["deduped"]
        closed       = paired["closed_clean"]
        orphans      = paired["orphans"]

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
                strat, sentiment = _resolve_signal_for_fill(signal_lookup, sym, side, fill_ts, f.get("order_id", ""))
                if sentiment == "flat":
                    intent = "exit"
                elif sentiment == "long":
                    intent = "enter_long"
                elif sentiment == "short":
                    intent = "enter_short"
                else:
                    intent = "legacy"
                if side == "BOT":
                    if intent == "enter_long":
                        day_longs.setdefault(sym, []).append((price, qty, fill_ts, strat))
                        continue
                    q = day_shorts.setdefault(sym, [])
                    while qty > 0 and q:
                        ep, eq, et, es = q.pop(0)  # FIFO: oldest short
                        m = min(qty, eq)
                        pair_strat = es if (es and es != "Unknown") else strat
                        daily_closed.append({"pnl": round((ep - price) * m, 2), "strategy": pair_strat,
                                             "entry_strategy": es, "exit_strategy": strat,
                                             "ticker": sym, "date": date_str, "side": "SHORT",
                                             "entry_price": ep, "exit_price": price, "qty": m,
                                             "entry_time": et, "exit_time": fill_ts})
                        qty -= m
                        if eq > m:
                            q.insert(0, (ep, eq - m, et, es))   # remainder stays at front (FIFO)
                    if qty > 0 and intent == "legacy":
                        day_longs.setdefault(sym, []).append((price, qty, fill_ts, strat))
                elif side == "SLD":
                    if intent == "enter_short":
                        day_shorts.setdefault(sym, []).append((price, qty, fill_ts, strat))
                        continue
                    q = day_longs.setdefault(sym, [])
                    while qty > 0 and q:
                        ep, eq, et, es = q.pop(0)
                        m = min(qty, eq)
                        pair_strat = es if (es and es != "Unknown") else strat
                        daily_closed.append({"pnl": round((price - ep) * m, 2), "strategy": pair_strat,
                                             "entry_strategy": es, "exit_strategy": strat,
                                             "ticker": sym, "date": date_str, "side": "LONG",
                                             "entry_price": ep, "exit_price": price, "qty": m,
                                             "entry_time": et, "exit_time": fill_ts})
                        qty -= m
                        if eq > m:
                            q.insert(0, (ep, eq - m, et, es))
                    if qty > 0 and intent == "legacy":
                        day_shorts.setdefault(sym, []).append((price, qty, fill_ts, strat))

        def _hold_mins(tlist):
            from datetime import datetime as _dth
            durs = []
            for t in tlist:
                try:
                    et = _dth.fromisoformat((t.get("entry_time") or "").replace("Z", "+00:00"))
                    xt = _dth.fromisoformat((t.get("exit_time")  or "").replace("Z", "+00:00"))
                    d  = (xt - et).total_seconds() / 60
                    if 0 < d < 600:
                        durs.append(d)
                except Exception:
                    pass
            return round(sum(durs) / len(durs), 1) if durs else None

        def _stats(tlist):
            if not tlist: return None
            wins   = [t["pnl"] for t in tlist if t["pnl"] > 0]
            losses = [t["pnl"] for t in tlist if t["pnl"] <= 0]
            gw, gl = sum(wins), abs(sum(losses))
            return {
                "trades":             len(tlist),
                "wins":               len(wins),
                "losses":             len(losses),
                "win_rate":           round(len(wins) / len(tlist) * 100, 1),
                "profit_factor":      round(gw / gl, 2) if gl > 0 else None,
                "total_pnl":          round(gw - gl, 2),
                "avg_win":            round(gw / len(wins),   2) if wins   else 0,
                "avg_loss":           round(-gl / len(losses), 2) if losses else 0,
                "largest_win":        round(max(wins),  2) if wins   else 0,
                "largest_loss":       round(min(losses), 2) if losses else 0,
                "avg_hold_mins":      _hold_mins(tlist),
                "consec_losing_days":  _consecutive_losing_days(tlist),
                "consec_winning_days": _consecutive_winning_days(tlist),
                "sharpe":              _sharpe_from_pnls([t["pnl"] / max(float(t.get("qty") or 1), 0.01) for t in tlist]),
                "consec_losses":       _consec_losses_from_trades(tlist),
            }

        # Apply frontend exclusions (localStorage keys: "exit_time|ticker") to the
        # already-orphan-split global pairs from the helper, and to the per-day pairs.
        exclude_param = request.args.get("exclude", "").strip()
        if exclude_param:
            excluded_keys = set(exclude_param.split(","))
            def _kept(c):
                return f"{c['exit_time']}|{c['ticker']}" not in excluded_keys
            closed       = [c for c in closed       if _kept(c)]
            orphans      = [c for c in orphans      if _kept(c)]
            daily_closed = [c for c in daily_closed if _kept(c)]

        # Classify per-day orphans (global orphans were already separated by the helper).
        daily_orphans, daily_clean = [], []
        for c in daily_closed:
            is_orph, reason = _classify_orphan(c)
            if is_orph:
                daily_orphans.append({**c, "orphan_reason": reason})
            else:
                daily_clean.append(c)
        daily_closed = daily_clean

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
                action   = _canonical_action(t.get("action"))
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
                    "side":           "LONG",
                    "entry_price":    ep,
                    "exit_price":     xp,
                    "qty":            qty,
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

        # Overall total uses ALL LIFO pairs (clean + orphan) so it matches
        # Alpaca realized P&L.  Orphans are real executions; excluding them
        # creates a gap vs the Alpaca account equity change.
        all_lifo = closed + orphans
        overall = _stats(all_lifo) or {}
        if orphans:
            overall["orphan_pnl"]   = round(sum(c["pnl"] for c in orphans), 2)
            overall["orphan_count"] = len(orphans)
        # Directional split — feeds the "Long Win Rate / Short Win Rate" cards
        # on the analysis page so the user can see which side performs better.
        overall["long"]  = _stats([c for c in all_lifo if c.get("side") == "LONG"])  or {}
        overall["short"] = _stats([c for c in all_lifo if c.get("side") == "SHORT"]) or {}

        result = {
            "overall":      overall,
            "per_strategy": per_strategy,
            "per_ticker":   per_ticker,
            "daily":        daily,
            "weekly":       weekly,
            "equity_curve": equity_curve,
            "closed":       closed,
            "orphans":      orphans,
        }
        _acache[_cache_key] = {"data": result, "ts": time.time()}
        return jsonify(result)
    except Exception as e:
        log.exception("Alpaca analysis error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/post_exit_moves", methods=["POST"])
def api_post_exit_moves():
    """Return post-exit price action for a list of round-trip trades.
    Body: {"trades": [{"ticker":..., "exit_time":..., "exit_price":..., "side":...}]}
    Returns: {"moves": {"TICKER|exit_time": {"post_fav_pct":..., "post_adv_pct":..., "premature":...}}}
    post_fav_pct = % price moved in the original trade's direction after exit (positive = continued = potential premature exit)
    """
    if alpaca_broker is None:
        return jsonify({"error": "Alpaca not configured"}), 400
    body   = request.get_json(force=True) or {}
    trades = body.get("trades") or []
    if not trades:
        return jsonify({"moves": {}})

    import concurrent.futures as _cf

    def _process(t):
        ticker    = (t.get("ticker") or "").upper()
        exit_time = t.get("exit_time") or ""
        exit_px   = float(t.get("exit_price") or 0)
        side      = (t.get("side") or "").upper()
        if not ticker or not exit_time or exit_px == 0:
            return None, None
        key     = f"{ticker}|{exit_time}"
        is_long = side in ("LONG", "BOT", "BUY")
        max_high, min_low = _fetch_post_exit_range(ticker, exit_time)
        if max_high is None or min_low is None:
            return key, None
        if is_long:
            post_fav_pct = round((max_high - exit_px) / exit_px * 100, 3)
            post_adv_pct = round((min_low  - exit_px) / exit_px * 100, 3)
        else:
            post_fav_pct = round((exit_px - min_low)  / exit_px * 100, 3)
            post_adv_pct = round((exit_px - max_high) / exit_px * 100, 3)
        return key, {
            "post_fav_pct": post_fav_pct,
            "post_adv_pct": post_adv_pct,
            "premature":    post_fav_pct > 0.15,
        }

    result = {}
    with _cf.ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_process, t) for t in trades]
        for f in _cf.as_completed(futures):
            try:
                key, val = f.result()
                if key and val is not None:
                    result[key] = val
            except Exception:
                pass
    return jsonify({"moves": result})


@app.route("/api/debug/reconcile")
def api_debug_reconcile():
    """Side-by-side TV vs Alpaca P&L per strategy for diagnosing chart differences."""
    from datetime import datetime as _dt

    from_date = request.args.get("from_date", "")
    to_date   = request.args.get("to_date",   "")

    # ── TV signals side (FIFO pairing, mirrors /api/stats) ──────────────
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM trades ORDER BY id ASC")
    rows = cur.fetchall()
    if DATABASE_URL:
        cols       = [d[0] for d in cur.description]
        all_trades = [dict(zip(cols, r)) for r in rows]
    else:
        all_trades = [dict(r) for r in rows]
    conn.close()

    tv_open_longs  = {}
    tv_open_shorts = {}
    tv_closed      = []

    for t in all_trades:
        action   = _canonical_action(t.get("action"))
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
        if (t.get("exec_status") or "").lower() in ("blocked", "skipped", "error"):
            continue
        key = (strategy, ticker)
        in_window = ((not from_date) or (received[:10] >= from_date)) and \
                    ((not to_date)   or (received[:10] <= to_date))
        if action == "BUY":
            queue = tv_open_shorts.setdefault(key, [])
            while qty > 0 and queue:
                ep, eq, et, eid = queue.pop(0)
                m = min(qty, eq)
                if in_window:
                    tv_closed.append({"pnl": round((ep - price) * m, 2), "strategy": strategy, "ticker": ticker})
                qty -= m
                if eq > m:
                    queue.insert(0, (ep, eq - m, et, eid))
            if qty > 0:
                tv_open_longs.setdefault(key, []).append((price, qty, received, trade_id))
        elif action == "SELL":
            queue = tv_open_longs.setdefault(key, [])
            while qty > 0 and queue:
                ep, eq, et, eid = queue.pop(0)
                m = min(qty, eq)
                if in_window:
                    tv_closed.append({"pnl": round((price - ep) * m, 2), "strategy": strategy, "ticker": ticker})
                qty -= m
                if eq > m:
                    queue.insert(0, (ep, eq - m, et, eid))
            if qty > 0:
                tv_open_shorts.setdefault(key, []).append((price, qty, received, trade_id))

    # ── Alpaca fills side (LIFO pairing, mirrors /api/alpaca/analysis) ───
    alpaca_closed   = []
    unmatched_fills = 0

    if alpaca_broker is not None:
        fills = _get_cached_fills()
        if from_date or to_date:
            fills = [f for f in fills if
                     (not from_date or (f.get("time") or "")[:10] >= from_date) and
                     (not to_date   or (f.get("time") or "")[:10] <= to_date)]

        sig_lkp = {}
        for t in all_trades:
            tk  = (t.get("ticker") or "").strip().upper()
            act = _canonical_action(t.get("action"))
            rcv = t.get("received_at") or ""
            stg = (t.get("strategy") or "").strip()
            snt = (t.get("sentiment") or "").strip().lower()
            if not stg or not tk or not rcv:
                continue
            sd = "BOT" if act == "BUY" else "SLD" if act == "SELL" else None
            if not sd:
                continue
            try:
                ts = _dt.fromisoformat(rcv.replace("Z", "+00:00")).timestamp()
            except Exception:
                continue
            sig_lkp.setdefault((tk, sd), []).append((ts, stg, snt))

        def _rsig(symbol, side, ftime, order_id=""):
            try:
                fts = _dt.fromisoformat(ftime.replace("Z", "+00:00")).timestamp()
            except Exception:
                fts = None
            cands = sig_lkp.get((symbol.upper(), side), [])
            if cands and fts is not None:
                best = min(cands, key=lambda x: abs(x[0] - fts))
                if abs(best[0] - fts) <= 300:
                    return best[1], best[2]
            if order_id and order_id.startswith("kairos-"):
                parts = order_id.split("-", 2)
                if len(parts) == 3 and parts[1]:
                    return parts[1], ""
            return "Unknown", ""

        seen = set()
        deduped = []
        for f in fills:
            k = f"{f['symbol']}|{f['side']}|{f['time']}|{f['shares']}"
            if k not in seen:
                seen.add(k)
                deduped.append(f)

        def _pts(f):
            try:
                return _dt.fromisoformat((f["time"] or "").replace("Z", "+00:00")).timestamp()
            except Exception:
                return 0
        deduped.sort(key=_pts)

        al_longs = {}
        al_shorts = {}
        for f in deduped:
            sym   = (f.get("symbol") or "").upper()
            side  = f.get("side", "")
            price = float(f.get("price") or 0)
            qty   = float(f.get("shares") or 0)
            ftime = f.get("time", "")
            strat, sentiment = _rsig(sym, side, ftime, f.get("order_id", ""))
            if strat == "Unknown":
                unmatched_fills += 1
            if sentiment == "flat":
                intent = "exit"
            elif sentiment == "long":
                intent = "enter_long"
            elif sentiment == "short":
                intent = "enter_short"
            else:
                intent = "legacy"
            if side == "BOT":
                if intent == "enter_long":
                    al_longs.setdefault(sym, []).append((price, qty, ftime, strat))
                    continue
                q = al_shorts.setdefault(sym, [])
                while qty > 0 and q:
                    ep, eq, et, es = q.pop(-1)
                    m = min(qty, eq)
                    alpaca_closed.append({"pnl": round((ep - price) * m, 2), "strategy": es, "ticker": sym})
                    qty -= m
                    if eq > m:
                        q.append((ep, eq - m, et, es))
                if qty > 0 and intent == "legacy":
                    al_longs.setdefault(sym, []).append((price, qty, ftime, strat))
            elif side == "SLD":
                if intent == "enter_short":
                    al_shorts.setdefault(sym, []).append((price, qty, ftime, strat))
                    continue
                q = al_longs.setdefault(sym, [])
                while qty > 0 and q:
                    ep, eq, et, es = q.pop(-1)
                    m = min(qty, eq)
                    alpaca_closed.append({"pnl": round((price - ep) * m, 2), "strategy": es, "ticker": sym})
                    qty -= m
                    if eq > m:
                        q.append((ep, eq - m, et, es))
                if qty > 0 and intent == "legacy":
                    al_shorts.setdefault(sym, []).append((price, qty, ftime, strat))

    # ── Aggregate per strategy ──────────────────────────────────────────
    def _agg(pairs):
        by_strat = {}
        for p in pairs:
            s = p["strategy"] or "Unknown"
            e = by_strat.setdefault(s, {"trades": 0, "pnl": 0.0, "wins": 0})
            e["trades"] += 1
            e["pnl"]     = round(e["pnl"] + p["pnl"], 2)
            if p["pnl"] > 0:
                e["wins"] += 1
        return by_strat

    tv_map     = _agg(tv_closed)
    alpaca_map = _agg(alpaca_closed)

    all_strats = sorted(set(list(tv_map.keys()) + list(alpaca_map.keys())))
    result_rows = []
    for strat in all_strats:
        tv  = tv_map.get(strat,     {"trades": 0, "pnl": 0.0, "wins": 0})
        alp = alpaca_map.get(strat, {"trades": 0, "pnl": 0.0, "wins": 0})
        result_rows.append({
            "strategy":        strat,
            "tv_trades":       tv["trades"],
            "tv_pnl":          tv["pnl"],
            "tv_win_rate":     round(tv["wins"] / tv["trades"] * 100, 1) if tv["trades"] else 0,
            "alpaca_trades":   alp["trades"],
            "alpaca_pnl":      alp["pnl"],
            "alpaca_win_rate": round(alp["wins"] / alp["trades"] * 100, 1) if alp["trades"] else 0,
            "pnl_delta":       round(alp["pnl"] - tv["pnl"], 2),
            "trade_delta":     alp["trades"] - tv["trades"],
        })

    result_rows.sort(key=lambda r: abs(r["pnl_delta"]), reverse=True)

    return jsonify({
        "rows":             result_rows,
        "unmatched_fills":  unmatched_fills,
        "tv_total_pnl":     round(sum(p["pnl"] for p in tv_closed), 2),
        "alpaca_total_pnl": round(sum(p["pnl"] for p in alpaca_closed), 2),
    })


@app.route("/api/trades/reset", methods=["POST"])
def api_trades_reset():
    """Delete TV signal rows from the trades table.
    Body: {} to wipe all, or {"before_date": "YYYY-MM-DD"} to wipe older rows only."""
    data        = request.get_json(silent=True) or {}
    before_date = (data.get("before_date") or "").strip()
    conn = get_db()
    cur  = conn.cursor()
    p    = placeholder()
    if before_date:
        cur.execute(f"DELETE FROM trades WHERE received_at < {p}", (before_date + "T00:00:00",))
    else:
        cur.execute("DELETE FROM trades")
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    log.info("TV trades reset: deleted=%d before_date=%r", deleted, before_date or "ALL")
    return jsonify({"deleted": deleted})


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

# Load persisted risk limits from DB — DB always wins over env vars so
# changes made via the Signal Router UI survive redeploys.
def _restore_risk_settings():
    global MAX_DAILY_LOSS, MAX_POSITION_LOSS, MAX_POSITION_LOSS_PCT, MAX_POSITION_LOSS_REFINED, MAX_TRAILING_GIVEBACK, MORNING_TRAIL_PCT, AFTERNOON_TRAIL_PCT
    stored = _load_setting("MAX_DAILY_LOSS")
    if stored is not None:
        try:
            MAX_DAILY_LOSS = float(stored)
            log.info("Restored MAX_DAILY_LOSS=%g from DB", MAX_DAILY_LOSS)
        except (TypeError, ValueError):
            pass
    stored = _load_setting("MAX_POSITION_LOSS")
    if stored is not None:
        try:
            MAX_POSITION_LOSS = float(stored)
            log.info("Restored MAX_POSITION_LOSS=%g from DB", MAX_POSITION_LOSS)
        except (TypeError, ValueError):
            pass
    stored = _load_setting("MAX_POSITION_LOSS_PCT")
    if stored is not None:
        try:
            MAX_POSITION_LOSS_PCT = float(stored)
            log.info("Restored MAX_POSITION_LOSS_PCT=%g from DB", MAX_POSITION_LOSS_PCT)
        except (TypeError, ValueError):
            pass
    stored = _load_setting("MAX_POSITION_LOSS_REFINED")
    if stored is not None:
        try:
            MAX_POSITION_LOSS_REFINED = float(stored)
            log.info("Restored MAX_POSITION_LOSS_REFINED=%g from DB", MAX_POSITION_LOSS_REFINED)
        except (TypeError, ValueError):
            pass
    stored = _load_setting("MAX_TRAILING_GIVEBACK")
    if stored is not None:
        try:
            MAX_TRAILING_GIVEBACK = float(stored)
            log.info("Restored MAX_TRAILING_GIVEBACK=%g from DB", MAX_TRAILING_GIVEBACK)
        except (TypeError, ValueError):
            pass
    stored = _load_setting("MORNING_TRAIL_PCT")
    if stored is not None:
        try:
            MORNING_TRAIL_PCT = float(stored)
            log.info("Restored MORNING_TRAIL_PCT=%g from DB", MORNING_TRAIL_PCT)
        except (TypeError, ValueError):
            pass
    stored = _load_setting("AFTERNOON_TRAIL_PCT")
    if stored is not None:
        try:
            AFTERNOON_TRAIL_PCT = float(stored)
            log.info("Restored AFTERNOON_TRAIL_PCT=%g from DB", AFTERNOON_TRAIL_PCT)
        except (TypeError, ValueError):
            pass

_restore_risk_settings()

# Reload persisted refined snapshot so the routing page shows the last
# run immediately after a deploy or server restart.
def _restore_refined_snapshot():
    global _refined_last_run, _refined_last_result
    stored = _load_setting("REFINED_LAST_RESULT")
    if stored:
        try:
            data = json.loads(stored)
            _refined_last_run    = data.get("run_at")
            _refined_last_result = data
            log.info("Restored refined snapshot from DB (run_at=%s)", _refined_last_run)
        except Exception as _e:
            log.warning("Failed to restore refined snapshot: %s", _e)

_restore_refined_snapshot()

if __name__ == "__main__":
    app.run(debug=True)
