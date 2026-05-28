"""TradingView webhook receiver, extracted as a Flask Blueprint.

Shared state (brokers, risk flags, config, helpers) continues to live in
app.py. We access it via `import app` INSIDE the view so the import is
deferred until the request fires — this avoids the circular import that
a top-level `import app` would create (app.py → routes.webhook → app).

Every `app.X` lookup re-reads the current module attribute, which is
important for state like `_risk_halted` that gets reassigned from
background threads.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import queue as _queue
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, jsonify, request


webhook_bp = Blueprint("webhook", __name__)


def _broker_family(target: str) -> str:
    if target in ("ib", "ib-paper", "ib-live"):
        return "ib"
    if target in ("alpaca", "alpaca-paper", "alpaca-live",
                  "alpaca-paper-2", "alpaca-live-2"):
        return "alpaca"
    return target


def _resolve_alpaca_broker(target: str):
    """Return the AlpacaBroker instance for a given target name."""
    import app as _app
    if target in ("alpaca-paper-2", "alpaca-live-2"):
        return _app.alpaca_broker2
    return _app.alpaca_broker


def _alpaca_broker_name(target: str) -> str:
    """Stable identifier for an Alpaca broker instance — shared across paper/live
    suffixes that resolve to the same broker, so a 'lock for MU on alpaca' is
    the same lock regardless of whether the signal came in as alpaca-paper or
    alpaca-live."""
    return "alpaca2" if target in ("alpaca-paper-2", "alpaca-live-2") else "alpaca"


# Per-(broker, ticker) FIFO queue so entry + exit signals for the same symbol
# can't race inside a single gunicorn worker. The worker thread drains the
# queue serially; entry → submit → cancel-on-exit logic now always sees a
# consistent state.
#
# Cross-process: the queue alone doesn't cover the case where MU SHORT lands
# on gunicorn worker 1 and MU EXIT_SHORT on worker 2. For that we wrap each
# task in a Postgres pg_advisory_lock keyed by (broker_name, ticker) — see
# _pg_advisory_lock below. On SQLite (local dev) the advisory lock is a no-op
# since gunicorn is typically a single worker there.
# Per-ticker handler lock — serializes Flask webhook handlers for the same ticker
# so the enqueue-order matches arrival-order. Without this, two webhooks 1 second
# apart (LONG then EXIT_LONG) can have their pre-enqueue routing-rule DB lookups
# overlap across 8 Flask threads, and whichever finishes first enqueues first.
# Result: EXIT runs against a not-yet-submitted entry, sees no position, no-ops.
_handler_locks_meta = threading.Lock()
_handler_locks      = {}   # TICKER -> threading.Lock


def _get_handler_lock(ticker):
    sym = (ticker or "").upper()
    with _handler_locks_meta:
        lk = _handler_locks.get(sym)
        if lk is None:
            lk = threading.Lock()
            _handler_locks[sym] = lk
        return lk


_ALPACA_WORKER_IDLE_TIMEOUT = 300  # seconds before an idle worker exits
_alpaca_queue_lock     = threading.Lock()
_alpaca_ticker_queues  = {}   # (broker_name, TICKER) -> queue.Queue
_alpaca_ticker_workers = {}   # (broker_name, TICKER) -> threading.Thread


@contextlib.contextmanager
def _pg_advisory_lock(key_str: str):
    """Serialize order placement across gunicorn workers using a Postgres
    session-level advisory lock. The lock is held on a dedicated connection
    that's closed when the context exits — if the worker process dies mid-task,
    Postgres releases the lock automatically.

    No-op on SQLite (local single-worker dev). On psycopg import failure or
    connection error we log and yield anyway — never block order placement
    on a lock-infrastructure problem."""
    import app as _app
    if not _app.DATABASE_URL:
        yield
        return
    digest = hashlib.blake2b(key_str.encode("utf-8"), digest_size=8).digest()
    key64  = int.from_bytes(digest, "big", signed=True)
    conn   = None
    locked = False
    try:
        import psycopg
        conn = psycopg.connect(_app.DATABASE_URL)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (key64,))
        locked = True
    except Exception as e:
        _app.log.warning(
            "Postgres advisory lock acquire failed for %s: %s — proceeding without cross-process lock",
            key_str, e,
        )
        if conn is not None:
            try: conn.close()
            except Exception: pass
            conn = None
    try:
        yield
    finally:
        if conn is not None:
            try:
                if locked:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_advisory_unlock(%s)", (key64,))
            except Exception as e:
                _app.log.warning("Postgres advisory unlock failed for %s: %s", key_str, e)
            try: conn.close()
            except Exception: pass


def _alpaca_worker_loop(queue_key):
    import app as _app
    broker_name, ticker_upper = queue_key
    lock_key = f"alpaca:{broker_name}:{ticker_upper}"
    while True:
        with _alpaca_queue_lock:
            q = _alpaca_ticker_queues.get(queue_key)
            if q is None:
                return
        try:
            task = q.get(timeout=_ALPACA_WORKER_IDLE_TIMEOUT)
        except _queue.Empty:
            with _alpaca_queue_lock:
                if q.empty():
                    _alpaca_ticker_queues.pop(queue_key, None)
                    _alpaca_ticker_workers.pop(queue_key, None)
                    return
            continue
        try:
            with _pg_advisory_lock(lock_key):
                task()
        except Exception as e:
            _app.log.error("Alpaca worker task failed for %s: %s", queue_key, e, exc_info=True)


def _enqueue_alpaca_task(broker_name, ticker, task_fn):
    """Serialize Alpaca order placement for the same (broker, ticker).
    Within a gunicorn worker, two webhooks for MU can no longer interleave —
    the second one waits for the first to finish, so the entry's submitted
    order is visible to the exit's get_orders() check. Across workers, the
    worker thread additionally takes a Postgres advisory lock before running
    the task — see _alpaca_worker_loop."""
    key = (broker_name, (ticker or "").upper())
    with _alpaca_queue_lock:
        q = _alpaca_ticker_queues.get(key)
        if q is None:
            q = _queue.Queue()
            _alpaca_ticker_queues[key] = q
        w = _alpaca_ticker_workers.get(key)
        if w is None or not w.is_alive():
            w = threading.Thread(target=_alpaca_worker_loop, args=(key,), daemon=True)
            _alpaca_ticker_workers[key] = w
            w.start()
        q.put(task_fn)


@webhook_bp.route("/webhook", methods=["POST"])
def webhook():
    import app  # late import — see module docstring

    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != app.WEBHOOK_TOKEN:
        abort(401)

    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    broker_name = (data.get("broker") or "").strip().lower()

    # Normalise ticker — Pine Script sends "symbol", some strategies send "ticker"
    ticker = (data.get("ticker") or data.get("symbol") or "").strip().upper() or None

    # Serialize same-ticker webhook processing for the rest of the handler so
    # the broker-task enqueue order matches HTTP arrival order. See _handler_locks
    # comment above for the race this prevents.
    _handler_lock = _get_handler_lock(ticker) if ticker else None
    if _handler_lock is not None:
        _handler_lock.acquire()
    try:
        return _webhook_locked(data, received_at, broker_name, ticker)
    finally:
        if _handler_lock is not None:
            _handler_lock.release()


def _webhook_locked(data, received_at, broker_name, ticker):
    import app

    # Normalise action — map direction/exit variants to BUY/SELL
    raw_action = (data.get("action") or "").strip().upper()
    action_map = {
        "EXIT_LONG":  "SELL",
        "EXIT_SHORT": "BUY",
        "LONG":       "BUY",
        "SHORT":      "SELL",
    }
    order_action = action_map.get(raw_action, raw_action)  # BUY/SELL pass through unchanged

    # Infer sentiment from raw_action when the payload doesn't include it explicitly.
    # This lets pairing logic use intent-based matching (enter_long/enter_short/exit)
    # instead of the legacy heuristic, without requiring Pine Script changes.
    _sentiment_map = {
        "LONG":       "long",
        "BUY":        "long",
        "SHORT":      "short",
        "SELL":       "short",
        "EXIT_LONG":  "flat",
        "EXIT_SHORT": "flat",
    }
    if not (data.get("sentiment") or "").strip():
        data["sentiment"] = _sentiment_map.get(raw_action, "")

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
    ep_stop_loss     = None   # set by exit_params node
    ep_hard_stop     = None   # per-pipeline hard-stop override ($, set by exit_params node)
    ep_trail_trigger = None
    ep_trail_offset  = None
    ep_trail_mode    = "dollars"
    ep_max_hold_mins = None

    matched_rule_id   = None  # set when a routing rule matches; used to flip tv_alert_created
    _routing_rule_count = 0   # total enabled rules; used for whitelist enforcement below
    try:
        rconn = app.get_db()
        rcur  = rconn.cursor()
        rcur.execute("SELECT id, nodes FROM routing_rules WHERE enabled=1 ORDER BY COALESCE(sort_order, id) ASC")
        rule_rows = rcur.fetchall()
        rconn.close()
        _routing_rule_count = len(rule_rows)
        for rrow in rule_rows:
            rule_id   = rrow[0] if app.DATABASE_URL else rrow["id"]
            nodes_raw = rrow[1] if app.DATABASE_URL else rrow["nodes"]
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
                    # as well as legacy bare values (ib, alpaca, coinbase).
                    # Optional qty_override is set per-broker by the Refined refresh,
                    # so the same routing rule can size Paper and Refined differently.
                    raw_bv      = (n.get("value") or "ib-paper").lower()
                    qty_ovr_raw = n.get("qty_override")
                    try:
                        qty_ovr = int(qty_ovr_raw) if qty_ovr_raw not in (None, "") else None
                    except (TypeError, ValueError):
                        qty_ovr = None
                    broker_targets.append((raw_bv, qty_ovr))
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
                        app.log.info("crypto_qty: $%.2f / $%.2f = %.8f %s", _dollars, _price, quantity, _sym)
                    except Exception as _e:
                        app.log.error("crypto_qty price fetch failed: %s", _e)
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
                elif ntype == "exit_params":
                    ep_stop_loss     = n.get("stop_loss")
                    ep_trail_trigger = n.get("trail_trigger")
                    ep_trail_offset  = n.get("trail_offset")
                    ep_trail_mode    = n.get("mode", "dollars")
                    # Per-pipeline override of the global MAX_POSITION_LOSS.
                    # Always in DOLLARS — not affected by exit_params mode.
                    ep_hard_stop     = n.get("hard_stop")
                    _mhm = n.get("max_hold_mins")
                    ep_max_hold_mins = float(_mhm) if _mhm else None
            if broker_targets:
                broker_name = ",".join(t[0] for t in broker_targets)
            app.log.info("Routing rule matched for strategy '%s' — broker=%s live=%s qty=%s sec=%s",
                         strategy_name, broker_name, use_live_broker, quantity, sec_type)
            matched_rule_id = rule_id
            break  # first matching pipeline wins
    except Exception as e:
        app.log.warning("Routing rule lookup failed: %s", e)

    # First webhook for a rule means the TV alert is wired up — flip the progress flag.
    if matched_rule_id is not None:
        try:
            fconn = app.get_db()
            fcur  = fconn.cursor()
            fp    = app.placeholder()
            fcur.execute(
                f"UPDATE routing_rules SET tv_alert_created=1 "
                f"WHERE id={fp} AND (tv_alert_created IS NULL OR tv_alert_created=0)",
                (matched_rule_id,),
            )
            fconn.commit()
            fconn.close()
        except Exception as _fe:
            app.log.debug("tv_alert_created flip failed: %s", _fe)

    # Whitelist enforcement: if routing rules exist but none matched, block the signal.
    # Prevents old/unupdated TV alerts (wrong strategy ID, template placeholder names, etc.)
    # from slipping through and executing against default settings.
    if _routing_rule_count > 0 and matched_rule_id is None:
        app.log.warning(
            "Signal BLOCKED — no enabled routing rule matches strategy '%s' (ticker=%s action=%s). "
            "Update the TradingView alert's strategy ID to match a routing rule name.",
            strategy_name, ticker, raw_action,
        )
        try:
            _bc = app.get_db(); _bcur = _bc.cursor()
            _bid = app._insert_trade(_bcur, (
                ticker, raw_action, data.get("sentiment"), data.get("quantity"),
                data.get("price"), data.get("time"), data.get("interval"),
                received_at, strategy_name, broker_name,
            ))
            app._update_exec(_bcur, _bid, "blocked",
                f"No routing rule matches '{strategy_name}' — update the Routing Strategy ID in TradingView")
            _bc.commit(); _bc.close()
        except Exception as _be:
            app.log.debug("Failed to log blocked signal: %s", _be)
        return jsonify({
            "status": "blocked",
            "reason": f"No routing rule matches strategy '{strategy_name}'. "
                      "Check the Routing Strategy ID in your TradingView Pine script inputs.",
        }), 200

    # If no broker nodes fired from pipeline, fall back to the single broker_name from request body
    if not broker_targets and broker_name:
        broker_targets = [(broker_name, None)]

    # Enforce trading hours if a trading_hours node was found in the matched pipeline
    if th_start and th_end:
        try:
            _now = datetime.now(ZoneInfo(th_tz))
            _now_t = _now.strftime("%H:%M")
            if not (th_start <= _now_t < th_end):
                app.log.info(
                    "Signal for '%s' dropped — outside trading hours (%s–%s %s, now %s)",
                    strategy_name, th_start, th_end, th_tz, _now_t,
                )
                return jsonify({"status": "skipped", "reason": f"outside trading hours ({th_start}–{th_end} {th_tz})"}), 200
        except Exception as e:
            app.log.warning("Trading hours check failed: %s", e)

    # Duplicate signal filter — drop retries / double-fires within cooldown window
    if app.SIGNAL_COOLDOWN_SECS > 0 and order_action in ("BUY", "SELL"):
        _sig_key  = (strategy_name or "", ticker or "", order_action)
        _now_ts   = time.time()
        with app._risk_lock:
            _last_ts = app._last_signal_ts.get(_sig_key, 0)
            _remaining = app.SIGNAL_COOLDOWN_SECS - (_now_ts - _last_ts)
            if _remaining > 0:
                app.log.info(
                    "Duplicate signal dropped — %s %s %s (%.1fs cooldown remaining)",
                    order_action, ticker, strategy_name, _remaining,
                )
                return jsonify({"status": "skipped", "reason": "duplicate_signal",
                                "cooldown_remaining": round(_remaining, 1)}), 200
            app._last_signal_ts[_sig_key] = _now_ts

    conn = app.get_db()
    cur  = conn.cursor()

    # 1. Log the signal immediately
    trade_id = app._insert_trade(cur, (
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

    # Exit signals (EXIT_LONG / EXIT_SHORT / sentiment=flat) always bypass risk
    # halts — blocking exits makes open losses worse, not better.
    _is_exit = (raw_action in ("EXIT_LONG", "EXIT_SHORT")
                or (data.get("sentiment") or "").strip().lower() == "flat")

    # Daily max loss circuit breaker — block NEW entries only, never exits
    if app.MAX_DAILY_LOSS < 0 and order_action in ("BUY", "SELL") and not _is_exit:
        with app._risk_lock:
            _halted = app._risk_halted
        if _halted:
            app.log.warning("Risk halt active — order blocked: %s %s %s", order_action, quantity, ticker)
            app._update_exec(cur, trade_id, "blocked", "Daily max loss limit reached — orders halted")
            conn.commit()
            conn.close()
            return jsonify({"status": "blocked", "reason": "daily_loss_limit"}), 200

    # Per-strategy block (set by position stop monitor) — exits bypass this too
    if strategy_name and order_action in ("BUY", "SELL") and not _is_exit:
        with app._risk_lock:
            _block_info = app._blocked_strategies.get(strategy_name)
        if _block_info:
            app.log.warning("Strategy '%s' is blocked — order rejected: %s %s %s",
                            strategy_name, order_action, quantity, ticker)
            app._update_exec(cur, trade_id, "blocked",
                             f"Strategy blocked: {_block_info['reason']}")
            conn.commit()
            conn.close()
            return jsonify({
                "status":   "blocked",
                "reason":   "strategy_blocked",
                "strategy": strategy_name,
                "detail":   _block_info,
            }), 200

    # If the matched pipeline has an exit_params node the broker-side trailing stop
    # manages the exit — suppress the TV exit signal so it doesn't close the position
    # before the stop has a chance to fire.
    if _is_exit and ep_trail_offset is not None:
        _suppress_msg = (
            f"TV exit suppressed — pipeline has exit_params "
            f"(trail {ep_trail_offset}%), broker-side trailing stop controls exit"
        )
        app.log.info("%s: %s %s", _suppress_msg, strategy_name, ticker)
        if conn:
            app._update_exec(cur, trade_id, "skipped", _suppress_msg)
            conn.commit()
            conn.close()
        return jsonify({"status": "skipped", "reason": "exit_params_controls_exit"}), 200

    # 2. Route to broker(s) — supports single or multi-broker pipelines
    exec_status = None
    exec_detail = None

    if not broker_targets:
        exec_status = "error"
        exec_detail = f"No routing pipeline matched strategy '{strategy_name}' — signal logged but no order placed. Check your Signal Router for a typo in the strategy name."
        app.log.warning("Webhook: no broker resolved for strategy '%s' — signal logged only", strategy_name)

    # All broker targets are now async — Alpaca/Coinbase fire in a background
    # thread so the webhook returns immediately and TradingView never times out.
    # broker_targets is a list of (broker_name, qty_override_or_None) tuples.
    coinbase_targets = [(t, o) for (t, o) in broker_targets if _broker_family(t) == "coinbase"]
    alpaca_targets   = [(t, o) for (t, o) in broker_targets if _broker_family(t) == "alpaca"]
    ib_targets       = [(t, o) for (t, o) in broker_targets if _broker_family(t) == "ib"]

    # --- Coinbase (sync-only; typically sub-second) ---
    for target, qty_override in coinbase_targets:
        _qty = qty_override if qty_override is not None else quantity
        if app.coinbase_broker is None:
            exec_status = "error"
            exec_detail = "Coinbase broker not initialised — set COINBASE_KEY + COINBASE_SECRET env vars"
            app.log.warning("Coinbase order skipped: broker not initialised")
        elif order_action not in ("BUY", "SELL"):
            exec_status = "skipped"
            exec_detail = f"No order placed for action '{raw_action}'"
        else:
            try:
                result = app.coinbase_broker.place_order(
                    ticker   = ticker,
                    action   = order_action,
                    quantity = _qty,
                    price    = data.get("price") if data.get("order_type") == "LMT" else None,
                    sec_type = sec_type,
                    currency = currency,
                )
                exec_status = "ok" if result.get("success") else "error"
                exec_detail = json.dumps(result)
                app.log.info("Coinbase order %s %s %s: %s", order_action, _qty, ticker, result)
            except Exception as e:
                exec_status = "error"
                exec_detail = str(e)
                app.log.error("Coinbase order failed for %s %s %s: %s", order_action, _qty, ticker, e)

    # Commit any sync (Coinbase) results before launching async threads
    if conn and exec_status is not None:
        app._update_exec(cur, trade_id, exec_status, exec_detail)
        conn.commit()

    # --- Alpaca (async — order placement can take 1–3 s; we return 200 first) ---
    for target, qty_override in alpaca_targets:
        _broker_inst = _resolve_alpaca_broker(target)
        if _broker_inst is None:
            if conn:
                app._update_exec(cur, trade_id, "error",
                                 "Alpaca broker not initialised — set ALPACA_KEY + ALPACA_SECRET env vars")
                conn.commit()
            app.log.warning("Alpaca order skipped: broker not initialised")
        elif order_action not in ("BUY", "SELL"):
            if conn:
                app._update_exec(cur, trade_id, "skipped", f"No order placed for action '{raw_action}'")
                conn.commit()
            app.log.info("Webhook action '%s' logged but no Alpaca order placed", raw_action)
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
            # Per-broker qty_override (set by the Refined refresh) wins over the rule's default.
            _qty         = qty_override if qty_override is not None else quantity
            _price       = data.get("price") if data.get("order_type") == "LMT" else None
            _sec_type    = sec_type
            _currency    = currency
            _trade_id    = trade_id
            _opt_prem    = opt_target_prem
            _opt_exp     = opt_expiry_type
            _opt_right   = opt_right_ovr
            _opt_ctrs    = opt_contracts

            _strategy         = strategy_name
            _is_entry         = not _is_exit
            _ep_stop_loss     = ep_stop_loss
            _ep_trail_trigger = ep_trail_trigger
            _ep_trail_offset  = ep_trail_offset
            _ep_trail_mode    = ep_trail_mode
            _ep_hard_stop     = ep_hard_stop
            _ep_max_hold_mins = ep_max_hold_mins
            _broker_captured  = _broker_inst

            # Session-based trail overrides (entry orders with percent-mode trail only)
            if not _is_exit and _ep_trail_offset is not None and _ep_trail_mode == "percent":
                import app as _app_morn
                _now_et     = datetime.now(ZoneInfo("America/New_York"))
                _morn_pct   = getattr(_app_morn, "MORNING_TRAIL_PCT",   0.0)
                _aftern_pct = getattr(_app_morn, "AFTERNOON_TRAIL_PCT", 0.0)
                _morn_start = _now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
                _morn_end   = _now_et.replace(hour=10, minute=30, second=0, microsecond=0)
                _aftn_start = _now_et.replace(hour=12, minute=0,  second=0, microsecond=0)
                if _morn_pct > 0 and _morn_start <= _now_et < _morn_end:
                    # Morning: widen — use whichever is larger
                    _ep_trail_offset = max(_ep_trail_offset, _morn_pct)
                elif _aftern_pct > 0 and _now_et >= _aftn_start:
                    # Afternoon: tighten — use whichever is smaller
                    _ep_trail_offset = min(_ep_trail_offset, _aftern_pct)

            def _place_alpaca_async(
                ticker=_ticker, action=_action, qty=_qty, price=_price,
                sec_type=_sec_type, currency=_currency, trade_id=_trade_id,
                opt_prem=_opt_prem, opt_exp=_opt_exp, opt_right=_opt_right, opt_ctrs=_opt_ctrs,
                strategy=_strategy, is_entry=_is_entry,
                ep_stop_loss=_ep_stop_loss, ep_trail_trigger=_ep_trail_trigger,
                ep_trail_offset=_ep_trail_offset, ep_trail_mode=_ep_trail_mode,
                ep_hard_stop=_ep_hard_stop, ep_max_hold_mins=_ep_max_hold_mins,
                broker=_broker_captured,
            ):
                _exec_status = _exec_detail = None
                try:
                    # Buying power gate — block entries when available capital is too low.
                    if action == "BUY" and is_entry and app.MIN_BUYING_POWER > 0:
                        try:
                            broker._ensure_client()
                            acct = broker._trading.get_account()
                            bp   = float(acct.buying_power)
                            if bp < app.MIN_BUYING_POWER:
                                app.log.warning(
                                    "Buying power gate: BUY %s blocked — $%.2f available, need $%.2f (%s)",
                                    ticker, bp, app.MIN_BUYING_POWER, strategy,
                                )
                                _exec_status = "blocked"
                                _exec_detail = (
                                    f"Buying power gate: ${bp:,.2f} available, "
                                    f"minimum ${app.MIN_BUYING_POWER:,.2f} required"
                                )
                                return
                        except Exception as _bpe:
                            app.log.warning(
                                "Buying power check failed for %s: %s — proceeding with order",
                                ticker, _bpe,
                            )

                    # Position gate — block new entries when Alpaca already holds the ticker
                    # in EITHER direction.  Uses abs(qty) so a short position (negative qty)
                    # also blocks a new long entry and vice versa — prevents simultaneous
                    # long+short on the same ticker.
                    # Exits (sentiment=flat / EXIT_LONG / EXIT_SHORT) always bypass this.
                    if action in ("BUY", "SELL") and is_entry:
                        try:
                            positions = broker._get_positions_cached()
                            existing  = next(
                                (p for p in positions
                                 if p.symbol.upper() == ticker.upper() and abs(float(p.qty or 0)) > 0),
                                None,
                            )
                            if existing:
                                held_qty = float(existing.qty)
                                held_side = "long" if held_qty > 0 else "short"
                                app.log.info(
                                    "Position gate: %s %s skipped — already holding %.0f shares %s (%s)",
                                    action, ticker, abs(held_qty), held_side, strategy,
                                )
                                _exec_status = "skipped"
                                _exec_detail = (
                                    f"Position gate: already holding {abs(held_qty):.0f}"
                                    f" shares {held_side} of {ticker}"
                                )
                                return
                        except Exception as _pe:
                            app.log.warning(
                                "Position gate check failed for %s: %s — proceeding with order",
                                ticker, _pe,
                            )

                    if opt_prem is not None:
                        opt_direction = "call" if action == "BUY" else "put"
                        if opt_right:
                            opt_direction = "call" if opt_right == "C" else "put"
                        result = broker.place_option_order(
                            underlying     = ticker,
                            direction      = opt_direction,
                            expiry_type    = opt_exp or "friday",
                            contracts      = opt_ctrs,
                            target_premium = float(opt_prem),
                        )
                    else:
                        # Per-position-stop → hard broker-side stop on entry.
                        # Resolution order: pipeline exit_params.hard_stop (per-rule override)
                        # → global MAX_POSITION_LOSS (set from the Signal Router risk panel).
                        # Always positive dollars; ref_price comes from the TV alert close.
                        _hard_stop = None
                        _ref_price = None
                        if is_entry:
                            if ep_hard_stop and float(ep_hard_stop) > 0:
                                _hard_stop = abs(float(ep_hard_stop))
                            elif getattr(app, "MAX_POSITION_LOSS", 0) < 0:
                                _hard_stop = abs(float(app.MAX_POSITION_LOSS))
                            if _hard_stop:
                                try:
                                    _ref_price = float(data.get("price") or 0) or None
                                except (TypeError, ValueError):
                                    _ref_price = None
                        result = broker.place_order(
                            ticker            = ticker,
                            action            = action,
                            quantity          = qty,
                            price             = price,
                            sec_type          = sec_type,
                            currency          = currency,
                            strategy          = strategy,
                            is_exit           = not is_entry,
                            stop_loss         = ep_stop_loss     if is_entry else None,
                            trail_trigger     = ep_trail_trigger if is_entry else None,
                            trail_offset      = ep_trail_offset  if is_entry else None,
                            trail_mode        = ep_trail_mode,
                            hard_stop_dollars = _hard_stop,
                            ref_price         = _ref_price,
                        )
                    _exec_status = "ok" if result.get("success") else "error"
                    _exec_detail = json.dumps(result)
                    app.log.info("Alpaca order %s %s %s: %s", action, qty, ticker, result)
                    # Register max-hold timer if this entry has a max_hold_mins constraint
                    if is_entry and result.get("success") and ep_max_hold_mins:
                        _broker_tag = "alpaca2" if broker is app.alpaca_broker2 else "alpaca"
                        _entry_ts   = datetime.now(timezone.utc)
                        with app._risk_lock:
                            app._max_hold_positions[(_broker_tag, ticker.upper())] = {
                                "entry_time":    _entry_ts,
                                "max_hold_mins": ep_max_hold_mins,
                            }
                            # Clear stale auto-closed marker so the new position is
                            # protected. _auto_closed_symbols is only scrubbed inside
                            # _check_position_stops which doesn't run when risk limits
                            # are disabled — without this discard, every second-and-later
                            # trade in the same ticker is silently skipped by max hold.
                            app._auto_closed_symbols.discard((_broker_tag, ticker.upper()))
                        app._persist_max_hold(_broker_tag, ticker.upper(), _entry_ts, ep_max_hold_mins)
                        app.log.info("Max hold registered: %s [%s] — %.0f min", ticker, _broker_tag, ep_max_hold_mins)
                    # If we cancelled a pending BUY, mark the original BUY trade record
                    # as "cancelled" so it doesn't appear as an orphaned/open trade.
                    if result.get("cancelled_buy") and result.get("cancelled_order_ids"):
                        _ph = app.placeholder()
                        for cid in result["cancelled_order_ids"]:
                            try:
                                _c2 = app.get_db()
                                _cur2 = _c2.cursor()
                                _cur2.execute(
                                    f"UPDATE trades SET exec_status={_ph}, exec_detail={_ph}"
                                    f" WHERE exec_detail LIKE {_ph} AND exec_status='ok'",
                                    ("cancelled", f"BUY order {cid} cancelled by SELL signal", f"%{cid}%"),
                                )
                                _c2.commit()
                                _c2.close()
                                app.log.info("Marked BUY trade with order_id %s as cancelled", cid)
                            except Exception as _me:
                                app.log.warning("Could not mark BUY trade cancelled for order %s: %s", cid, _me)
                except Exception as e:
                    _exec_status = "error"
                    _exec_detail = str(e)
                    app.log.error("Alpaca order failed for %s %s %s: %s", action, qty, ticker, e)
                finally:
                    try:
                        _c = app.get_db()
                        _cur = _c.cursor()
                        app._update_exec(_cur, trade_id, _exec_status, _exec_detail)
                        _c.commit()
                        _c.close()
                    except Exception as e:
                        app.log.error("Failed to update exec status for trade %s: %s", trade_id, e)

            _enqueue_alpaca_task(_alpaca_broker_name(target), _ticker, _place_alpaca_async)

    for target, qty_override in ib_targets:
        _live = (target == "ib-live") or (use_live_broker and target != "ib-paper")
        active_broker  = app.ib_broker_live if (_live and app.ib_broker_live) else app.ib_broker
        submit_task    = app._submit_ib_live_task if (_live and app.ib_broker_live) else app._submit_ib_task
        mode_label     = "live" if (_live and app.ib_broker_live) else "paper"
        # Per-broker qty_override (set by the Refined refresh) wins over the rule's default.
        ib_qty         = qty_override if qty_override is not None else quantity

        if active_broker is None:
            if conn:
                app._update_exec(cur, trade_id, "error",
                                 "IB live broker not initialised — set IB_HOST_LIVE env var"
                                 if _live else
                                 "IB broker not initialised — check IB_HOST env var")
                conn.commit()
        elif order_action not in ("BUY", "SELL"):
            if conn:
                app._update_exec(cur, trade_id, "skipped", f"No order placed for action '{raw_action}'")
                conn.commit()
            app.log.info("Webhook action '%s' logged but no IB order placed", raw_action)
        else:
            # Close DB before handing off — background thread opens its own connection
            if conn:
                conn.commit()
                conn.close()
                conn = None

            def _place_async(_qty=ib_qty):
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
                            ticker, order_action, _qty, None,
                            _timeout = 20,
                            sec_type = "OPT",
                            currency = currency,
                            expiry   = opt["expiry"],
                            strike   = opt["strike"],
                            right    = opt["right"],
                        )
                        _exec_status = "ok" if result.get("success") else "error"
                        _exec_detail = json.dumps({**result, "option_selected": opt, "mode": mode_label})
                        app.log.info("IB %s option order %s %s %s %s %s: %s",
                                     mode_label, order_action, ticker,
                                     opt["expiry"], opt["strike"], opt["right"], result)
                    else:
                        result = submit_task(
                            active_broker.place_order,
                            ticker   = ticker,
                            action   = order_action,
                            quantity = _qty,
                            price    = data.get("price") if data.get("order_type") == "LMT" else None,
                            sec_type = sec_type,
                            currency = currency,
                            _timeout = 30,
                        )
                        _exec_status = "ok" if result.get("success") else "error"
                        _exec_detail = json.dumps({**result, "mode": mode_label})
                        app.log.info("IB %s order %s %s %s: %s",
                                     mode_label, order_action, _qty, ticker, result)
                except Exception as e:
                    _exec_status = "error"
                    _exec_detail = str(e)
                    app.log.error("IB async order failed for %s %s: %s", order_action, ticker, e)
                finally:
                    try:
                        _c = app.get_db()
                        _cur = _c.cursor()
                        app._update_exec(_cur, trade_id, _exec_status, _exec_detail)
                        _c.commit()
                        _c.close()
                    except Exception as e:
                        app.log.error("Failed to update exec status for trade %s: %s", trade_id, e)

            threading.Thread(target=_place_async, daemon=True).start()

    if conn:
        conn.commit()
        conn.close()
    return jsonify({"status": "ok", "id": trade_id}), 200
