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
    t = (target or "").lower()
    if t == "alpaca" or t.startswith("alpaca-paper") or t.startswith("alpaca-live"):
        return "alpaca"
    return target


def _resolve_alpaca_broker(target: str):
    """Return the AlpacaBroker instance for a given target name, or None.
    Registry-driven, so any configured account (alpaca-paper-2/-3/-4, ...) resolves
    automatically. An explicit but UNCONFIGURED slot (e.g. alpaca-paper-4 with no
    ALPACA_KEY4) returns None so the caller SKIPS the order rather than silently
    routing it to Paper All. Only bare/legacy targets fall back to account 1."""
    import app as _app
    tag = _app._routing_broker_to_tag(target)
    if tag is None:
        t = (target or "").lower()
        return _app.alpaca_broker if t in ("alpaca", "alpaca-paper", "alpaca-live") else None
    rec = _app.ACCOUNTS_BY_TAG.get(tag)
    return rec["broker"] if rec else None


def _alpaca_broker_name(target: str) -> str:
    """Stable identifier for an Alpaca broker instance — shared across paper/live
    suffixes that resolve to the same broker, so a 'lock for MU on alpaca' is
    the same lock regardless of whether the signal came in as alpaca-paper or
    alpaca-live."""
    import app as _app
    return _app._routing_broker_to_tag(target) or "alpaca"


def _add_tv_pilot_targets(alpaca_targets, broker_targets, matched_no_broker_bundles,
                          is_exit, price=None):
    """Fan a TV ENTRY to the TV_PILOT_ALL account(s) — the TV twin of
    ENGINE_PILOT_ALL. Ensures each configured farm account trades EVERY strategy's
    TV entry regardless of the rule's own broker nodes, so the full-sample audition
    pool + leaderboard source (Paper All) sees the whole book — incl. the Refined
    top-N whose rules route only to alpaca-paper-2.

    Sizing (`tag:10` flat shares OR `tag:$1000` equal-dollar) is AUTHORITATIVE for
    the farm account: if the payload-broker fallback already put it in at the base
    rule's shares, we OVERRIDE that qty so the whole farm is uniformly sized (dollar
    → shares via the alert price, like the engine does ÷price). Non-farm targets
    (e.g. Refined) are untouched.

    A fully kairos-suppressed signal returns a skip upstream, so a surviving
    broker_target on an entry means this IS a real TV entry. Mutates alpaca_targets;
    returns [(tag, shares, "added"|"resized")]."""
    import app as _app
    if is_exit or not broker_targets:
        return []
    pilots = _app._tv_pilot_accounts()          # [(tag, amount, unit)]
    if not pilots:
        return []
    try:    _px = float(price or 0)
    except (TypeError, ValueError): _px = 0.0

    def _shares(amount, unit):
        if unit == "dollars":
            return max(1, round(amount / _px)) if _px > 0 else None   # None → base rule qty
        return amount

    # First alpaca target index per resolved account tag (to override in place).
    idx_by_tag = {}
    for i, bt in enumerate(alpaca_targets):
        idx_by_tag.setdefault(_alpaca_broker_name(bt[0]), i)
    # Bundle to clone when ADDING (top-N case): the base TV rule, else first target.
    src = (matched_no_broker_bundles[0] if matched_no_broker_bundles
           else broker_targets[0][2])
    out = []
    for ptag, amount, unit in pilots:
        rec = _app.ACCOUNTS_BY_TAG.get(ptag)
        if rec is None:
            continue
        qov = _shares(amount, unit)
        if ptag in idx_by_tag:                  # already present → override its sizing
            i = idx_by_tag[ptag]
            _t = alpaca_targets[i]
            rs = dict(_t[2]); rs["entry_source_kairos"] = False
            alpaca_targets[i] = (_t[0], qov, rs)
            out.append((ptag, qov, "resized"))
        else:                                   # top-N case → add the farm target
            rs = dict(src); rs["entry_source_kairos"] = False
            alpaca_targets.append((rec["target_paper"], qov, rs))
            idx_by_tag[ptag] = len(alpaca_targets) - 1
            out.append((ptag, qov, "added"))
    return out


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
    entry_source_kairos = False   # set by entry_source node; suppresses TV entry order

    # Side-gate support: classify this signal so a side_gate node can drop entries on
    # the wrong side. Exits (EXIT_*/flat) are never gated — you must always be able to close.
    _gate_is_exit    = raw_action in ("EXIT_LONG", "EXIT_SHORT") or (data.get("sentiment") == "flat")
    _gate_entry_side = (None if _gate_is_exit else
                        "long"  if order_action == "BUY"  else
                        "short" if order_action == "SELL" else None)

    # Template-alert guard: an unsubstituted TradingView {{ticker}} placeholder
    # leaves the literal "CAM_TICKER" (or braces) in the strategy id. It can never
    # match a routing rule, so flag it clearly instead of the generic "no rule"
    # block — the fix is on the TV alert, not the router.
    _su = strategy_name.upper()
    if "CAM_TICKER" in _su or "{{" in strategy_name or "}}" in strategy_name:
        app.log.info("Ignoring template alert (unsubstituted placeholder): strategy=%r ticker=%s",
                     strategy_name, ticker)
        try:
            _tc = app.get_db(); _tcur = _tc.cursor()
            _tid = app._insert_trade(_tcur, (
                ticker, raw_action, data.get("sentiment"), data.get("quantity"),
                data.get("price"), data.get("time"), data.get("interval"),
                received_at, strategy_name, broker_name,
            ))
            app._update_exec(_tcur, _tid, "skipped",
                "Template alert: unsubstituted {{ticker}} placeholder — fix or delete the TradingView alert")
            _tc.commit(); _tc.close()
        except Exception as _te:
            app.log.debug("Failed to log template alert: %s", _te)
        return jsonify({"status": "ignored",
                        "reason": "Template alert with an unsubstituted placeholder — fix the TradingView alert."}), 200

    # Non-shortable guard: a short ENTRY on a ticker the broker won't let you short
    # (e.g. SPCX) just errors at Alpaca. Skip it cleanly. Exits always pass so an
    # existing position can still be closed.
    if _gate_entry_side == "short" and app._is_non_shortable(ticker):
        app.log.info("Skipping short entry on non-shortable ticker %s (strategy=%s)", ticker, strategy_name)
        try:
            _nc = app.get_db(); _ncur = _nc.cursor()
            _nid = app._insert_trade(_ncur, (
                ticker, raw_action, data.get("sentiment"), data.get("quantity"),
                data.get("price"), data.get("time"), data.get("interval"),
                received_at, strategy_name, broker_name,
            ))
            app._update_exec(_ncur, _nid, "skipped",
                f"{ticker} cannot be sold short at the broker — short entry skipped")
            _nc.commit(); _nc.close()
        except Exception as _ne:
            app.log.debug("Failed to log non-shortable skip: %s", _ne)
        return jsonify({"status": "skipped",
                        "reason": f"{ticker} is not shortable at the broker — short entry skipped."}), 200

    matched_rule_id     = None       # first matching rule (kept for back-compat / logging)
    matched_rule_ids    = []         # ALL matching rules — used to flip tv_alert_created on each
    _side_gated_any     = False      # True if any matched rule was skipped by its long/short gate
    _routing_rule_count = 0          # total enabled rules; used for whitelist enforcement below
    # Rules that matched but contributed NO broker node — their settings still
    # apply (quantity, exit_params, etc.), they just need the body-fallback
    # broker to fire. Without this, a rule like 'SPY_CAM_*: quantity 10' with
    # no broker silently lets TV's payload quantity through to the broker.
    _matched_no_broker_bundles = []
    # Multi-pipeline support: each matching rule produces its own settings bundle.
    # broker_targets becomes a list of (target_name, qty_override, rule_settings_bundle)
    # 3-tuples so the downstream dispatch loops can apply each rule's own quantity,
    # exit_params, trading_hours, and entry_source independently. Without this, a
    # 'first matching rule wins' break stopped Crew-style sibling rules from firing.
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
            # Pipeline matched — apply node settings.
            # Per-rule long/short gate: skip this rule entirely (no targets/settings)
            # when it's gated to the opposite side of an ENTRY. Exits always pass.
            _rule_side_gate = next(((n.get("value") or "").lower()
                                    for n in nodes if n.get("type") == "side_gate"), None)
            if _rule_side_gate in ("long", "short") and _gate_entry_side and _rule_side_gate != _gate_entry_side:
                _side_gated_any = True
                if matched_rule_id is None:
                    matched_rule_id = rule_id   # a rule DID match — it was gated, not absent
                matched_rule_ids.append(rule_id)
                app.log.info("Side gate: %s %s entry skipped on a %s-only rule",
                             _gate_entry_side, strategy_name or ticker, _rule_side_gate)
                continue
            # Per-rule settings bundle — defaults from the body, overridden by each
            # node so siblings can have different quantity/exit_params/hours/etc.
            _rs = {
                "rule_id":             rule_id,
                "quantity":            data.get("quantity", 1),
                "sec_type":            data.get("sec_type", "STK"),
                "currency":            data.get("currency", "USD"),
                "use_live_broker":     False,
                "ep_stop_loss":        None,
                "ep_hard_stop":        None,
                "ep_trail_trigger":    None,
                "ep_trail_offset":     None,
                "ep_trail_mode":       "dollars",
                "ep_max_hold_mins":    None,
                "th_start":            None,
                "th_end":              None,
                "th_tz":               "America/New_York",
                "entry_source_kairos": False,
                "opt_broker_mode":     "alpaca",
                "opt_target_prem":     None,
                "opt_expiry_type":     "friday",
                "opt_right_ovr":       None,
                "opt_contracts":       1,
                "ticker":              ticker,
            }
            _rule_brokers = []   # (raw_bv, qty_override) for this rule's broker nodes
            for n in nodes:
                ntype = n.get("type")
                if ntype == "broker":
                    raw_bv      = (n.get("value") or "ib-paper").lower()
                    qty_ovr_raw = n.get("qty_override")
                    try:
                        qty_ovr = int(qty_ovr_raw) if qty_ovr_raw not in (None, "") else None
                    except (TypeError, ValueError):
                        qty_ovr = None
                    _rule_brokers.append((raw_bv, qty_ovr))
                    if raw_bv == "ib-live":
                        _rs["use_live_broker"] = True
                elif ntype == "mode":
                    _rs["use_live_broker"] = (n.get("value") or "").lower() == "live"
                elif ntype == "quantity":
                    _rs["quantity"] = n.get("amount", _rs["quantity"])
                elif ntype == "crypto_qty":
                    _dollars = float(n.get("dollars") or 10)
                    try:
                        import urllib.request as _ur
                        import json as _jx
                        _sym = (n.get("symbol") or "BTC").upper()
                        with _ur.urlopen(f"https://api.coinbase.com/v2/prices/{_sym}-USD/spot", timeout=5) as _r:
                            _price = float(_jx.loads(_r.read())["data"]["amount"])
                        _rs["quantity"] = round(_dollars / _price, 8)
                        app.log.info("crypto_qty: $%.2f / $%.2f = %.8f %s",
                                     _dollars, _price, _rs["quantity"], _sym)
                    except Exception as _e:
                        app.log.error("crypto_qty price fetch failed: %s", _e)
                elif ntype == "instrument":
                    _rs["sec_type"] = n.get("value") or _rs["sec_type"]
                elif ntype == "ticker":
                    _rs["ticker"] = (n.get("value") or _rs["ticker"] or "").upper() or None
                elif ntype == "options_config":
                    _rs["opt_broker_mode"] = n.get("broker_mode") or "alpaca"
                    if _rs["opt_broker_mode"] == "ib":
                        _rs["opt_target_prem"] = float(n.get("ib_target_premium") or 2.0)
                        _ib_exp                = n.get("ib_expiry_type") or "weekly"
                        _rs["opt_right_ovr"]   = n.get("ib_right_override") or None
                        _rs["opt_contracts"]   = int(n.get("ib_contracts") or 1)
                        _rs["sec_type"]        = "OPT"   # trigger IB options path
                        if _ib_exp == "0dte":
                            _rs["opt_expiry_type"] = "weekly"
                            import datetime as _dt_mod
                            data["option_expiry"]  = _dt_mod.date.today().strftime("%Y%m%d")
                        else:
                            _rs["opt_expiry_type"] = _ib_exp
                    else:
                        _rs["opt_target_prem"] = float(n.get("target_premium") or 2.0)
                        _rs["opt_expiry_type"] = n.get("expiry_type") or "friday"
                        _rs["opt_right_ovr"]   = n.get("right_override") or None
                        _rs["opt_contracts"]   = int(n.get("contracts") or 1)
                elif ntype == "trading_hours":
                    _rs["th_start"] = n.get("start") or "09:30"
                    _rs["th_end"]   = n.get("end")   or "16:00"
                    _rs["th_tz"]    = n.get("tz")    or "America/New_York"
                elif ntype == "exit_params":
                    _rs["ep_stop_loss"]     = n.get("stop_loss")
                    _rs["ep_trail_trigger"] = n.get("trail_trigger")
                    _rs["ep_trail_offset"]  = n.get("trail_offset")
                    _rs["ep_trail_mode"]    = n.get("mode", "dollars")
                    _rs["ep_hard_stop"]     = n.get("hard_stop")
                    _mhm = n.get("max_hold_mins")
                    _rs["ep_max_hold_mins"] = float(_mhm) if _mhm else None
                elif ntype == "entry_source":
                    _rs["entry_source_kairos"] = (n.get("value") or "tv").lower() == "kairos"
            # Each broker node in THIS rule gets the rule's bundle.
            if _rule_brokers:
                for (raw_bv, qty_ovr) in _rule_brokers:
                    broker_targets.append((raw_bv, qty_ovr, _rs))
            else:
                # Rule matched but specified no broker — keep its bundle so the
                # body-fallback below can honor THIS rule's quantity / exit_params
                # instead of TV's payload defaults.
                _matched_no_broker_bundles.append(_rs)
            if matched_rule_id is None:
                matched_rule_id = rule_id
            matched_rule_ids.append(rule_id)
            _broker_csv = ",".join(t[0] for t in _rule_brokers) or "(none)"
            app.log.info("Routing rule matched for strategy '%s' [#%s] — broker=%s live=%s qty=%s sec=%s",
                         strategy_name, rule_id, _broker_csv,
                         _rs["use_live_broker"], _rs["quantity"], _rs["sec_type"])
            # NO BREAK — keep collecting sibling rules so e.g. a Crew Paper rule
            # fires alongside the original Paper All rule on the same TV signal.
    except Exception as e:
        app.log.warning("Routing rule lookup failed: %s", e)

    # First webhook for a rule means the TV alert is wired up — flip the
    # progress flag for EVERY matched rule (Crew/sibling rules each get marked).
    if matched_rule_ids:
        try:
            fconn = app.get_db()
            fcur  = fconn.cursor()
            fp    = app.placeholder()
            for _rid in set(matched_rule_ids):
                fcur.execute(
                    f"UPDATE routing_rules SET tv_alert_created=1 "
                    f"WHERE id={fp} AND (tv_alert_created IS NULL OR tv_alert_created=0)",
                    (_rid,),
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

    # If no broker nodes fired from any pipeline, fall back to the single
    # broker_name from request body — wrap a default settings bundle so the
    # downstream 3-tuple unpacking still works. Prefer the FIRST matched
    # no-broker rule's bundle (so its quantity/exit_params apply) over the
    # raw body defaults; otherwise fall back to TV's payload values.
    if not broker_targets and broker_name:
        if _matched_no_broker_bundles:
            _fallback_rs = _matched_no_broker_bundles[0]
        else:
            _fallback_rs = {
                "rule_id":             None,
                "quantity":            quantity,
                "sec_type":            sec_type,
                "currency":            currency,
                "use_live_broker":     use_live_broker,
                "ep_stop_loss":        ep_stop_loss,
                "ep_hard_stop":        ep_hard_stop,
                "ep_trail_trigger":    ep_trail_trigger,
                "ep_trail_offset":     ep_trail_offset,
                "ep_trail_mode":       ep_trail_mode,
                "ep_max_hold_mins":    ep_max_hold_mins,
                "th_start":            th_start,
                "th_end":              th_end,
                "th_tz":               th_tz,
                "entry_source_kairos": entry_source_kairos,
                "opt_broker_mode":     "alpaca",
                "opt_target_prem":     opt_target_prem,
                "opt_expiry_type":     opt_expiry_type,
                "opt_right_ovr":       opt_right_ovr,
                "opt_contracts":       opt_contracts,
                "ticker":              ticker,
            }
        broker_targets = [(broker_name, None, _fallback_rs)]

    # Per-target trading hours check: filter out targets whose rule's hours
    # don't include now. With multi-rule support, each bundle has its own
    # th_start/th_end — we can't kill the whole signal globally anymore.
    if broker_targets:
        _kept_hours = []
        _dropped_hours = []
        for _bt in broker_targets:
            _rs_bt = _bt[2]
            _ths   = _rs_bt.get("th_start")
            _the   = _rs_bt.get("th_end")
            _thtz  = _rs_bt.get("th_tz") or "America/New_York"
            if not _ths or not _the:
                _kept_hours.append(_bt)
                continue
            try:
                _now_t = datetime.now(ZoneInfo(_thtz)).strftime("%H:%M")
                if _ths <= _now_t < _the:
                    _kept_hours.append(_bt)
                else:
                    _dropped_hours.append((_bt[0], f"{_ths}-{_the} {_thtz}, now {_now_t}"))
            except Exception as e:
                app.log.warning("Trading hours check failed for %s: %s — allowing target", _bt[0], e)
                _kept_hours.append(_bt)
        if _dropped_hours:
            app.log.info("Trading hours gate: dropped %s for '%s'",
                         ", ".join(f"{t} ({why})" for t, why in _dropped_hours), strategy_name)
        broker_targets = _kept_hours

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

    # Daily max loss is now enforced PER ACCOUNT below (per-target, alongside the
    # profit-lock gate), so a halted account is dropped while others keep trading.

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

    # N-strikes-per-level gate — once a ticker+Camarilla-level has taken N losing
    # round-trips today, block new entries on that level (exits always bypass).
    if (getattr(app, "STRIKES_ENABLED", False) and strategy_name and ticker
            and order_action in ("BUY", "SELL") and not _is_exit):
        side  = "LONG" if order_action == "BUY" else "SHORT"
        level = app._trade_level(strategy_name, side)
        if level:
            try:
                counts = app._get_strike_counts()
                accts  = {_alpaca_broker_name(_bt[0]) for _bt in broker_targets
                          if _broker_family(_bt[0]) == "alpaca"}
                # Per-account limit: SHORT levels on curated books use the tighter
                # re-fade cap (STRIKES_PER_LEVEL_SHORT); longs/farms use the normal one.
                hit = [(a, app._strike_limit(level, a)) for a in accts
                       if counts.get((a, ticker.upper(), level), 0) >= app._strike_limit(level, a)]
                if hit:
                    _names = [a for a, _ in hit]; _lim = hit[0][1]
                    app.log.warning(
                        "Strikes gate: %s %s (%s) blocked — %s+ losses at %s today on %s",
                        order_action, ticker, strategy_name, _lim, level, ",".join(_names))
                    app._update_exec(cur, trade_id, "blocked",
                        f"{_lim}-strikes/level: {level} already took {_lim} loss(es) "
                        f"today ({', '.join(_names)})")
                    conn.commit()
                    conn.close()
                    return jsonify({"status": "blocked", "reason": "strikes_limit",
                                    "level": level, "limit": _lim, "accounts": _names}), 200
            except Exception as _se:
                app.log.warning("Strikes gate check failed: %s — allowing order", _se)

    # Per-target suppression:
    #   - Exits: drop targets whose rule has ep_trail_offset set (broker-side
    #     trailing stop owns the exit). Targets without exit_params still fire.
    #   - Entries: drop targets whose rule has entry_source=kairos (the engine
    #     owns entries). Targets with entry_source=tv still fire.
    # Globally returning a skip would block sibling rules that didn't set those.
    if broker_targets:
        _kept_sup = []
        _dropped_exit_kairos = []
        for _bt in broker_targets:
            _rs_bt = _bt[2]
            if _is_exit and _rs_bt.get("ep_trail_offset") is not None:
                _dropped_exit_kairos.append((_bt[0], "exit_params"))
                continue
            if (not _is_exit) and _rs_bt.get("entry_source_kairos"):
                _dropped_exit_kairos.append((_bt[0], "entry_source=kairos"))
                continue
            _kept_sup.append(_bt)
        if _dropped_exit_kairos:
            app.log.info("Suppressed targets for '%s': %s", strategy_name,
                         ", ".join(f"{t} ({why})" for t, why in _dropped_exit_kairos))
        broker_targets = _kept_sup
        # If ALL targets were suppressed by exit_params/kairos, log a skip on the trade
        # row so the trade feed shows why nothing fired.
        if not broker_targets and _dropped_exit_kairos:
            _reason = "exit_params_controls_exit" if _is_exit else "kairos_controls_entry"
            _msg    = ("TV exit suppressed — all matched pipelines own the exit broker-side"
                       if _is_exit else
                       "TV entry suppressed — all matched pipelines use entry_source=kairos")
            if conn:
                app._update_exec(cur, trade_id, "skipped", _msg)
                conn.commit()
                conn.close()
            return jsonify({"status": "skipped", "reason": _reason}), 200

    # 2. Route to broker(s) — supports single or multi-broker pipelines
    exec_status = None
    exec_detail = None

    if not broker_targets and _side_gated_any and not _is_exit:
        exec_status = "skipped"
        exec_detail = f"Side gate: {_gate_entry_side} entry skipped — the matched pipeline(s) are gated to the other side only."
        app.log.info("Webhook: %s entry on '%s' skipped by side gate", _gate_entry_side, strategy_name)
    elif not broker_targets:
        exec_status = "error"
        exec_detail = f"No routing pipeline matched strategy '{strategy_name}' — signal logged but no order placed. Check your Signal Router for a typo in the strategy name."
        app.log.warning("Webhook: no broker resolved for strategy '%s' — signal logged only", strategy_name)

    # All broker targets are now async — Alpaca/Coinbase fire in a background
    # thread so the webhook returns immediately and TradingView never times out.
    # broker_targets is a list of (broker_name, qty_override_or_None, rule_settings_bundle).
    coinbase_targets = [bt for bt in broker_targets if _broker_family(bt[0]) == "coinbase"]
    alpaca_targets   = [bt for bt in broker_targets if _broker_family(bt[0]) == "alpaca"]
    ib_targets       = [bt for bt in broker_targets if _broker_family(bt[0]) == "ib"]

    # TV farm: fan every TV ENTRY to the TV_PILOT_ALL account(s) (the TV twin of
    # ENGINE_PILOT_ALL). See _add_tv_pilot_targets. Entries only; dollar sizing uses
    # the TV alert price.
    for _t, _sh, _act in _add_tv_pilot_targets(alpaca_targets, broker_targets,
                                               _matched_no_broker_bundles, _is_exit,
                                               price=data.get("price")):
        app.log.info("TV farm: %s %s (%s sh) to %s %s", _act, _t, _sh, order_action, ticker)

    # Per-account trading-hours gate (entries only) — drop the Alpaca targets whose
    # account is outside its configured window, while letting in-window accounts
    # through. Lets Paper trade all day while Refined only trades the open, etc.
    if not _is_exit and alpaca_targets:
        _in_hours = [bt for bt in alpaca_targets
                     if app._account_hours_ok(_alpaca_broker_name(bt[0]))]
        if len(_in_hours) != len(alpaca_targets):
            _dropped = {_alpaca_broker_name(bt[0]) for bt in alpaca_targets} \
                       - {_alpaca_broker_name(bt[0]) for bt in _in_hours}
            app.log.info("Account-hours gate: %s %s — skipped %s (outside trading window)",
                         order_action, ticker, ",".join(sorted(_dropped)))
            if not _in_hours and conn:
                app._update_exec(cur, trade_id, "skipped",
                                 f"Outside trading hours for {', '.join(sorted(_dropped))}")
                conn.commit()
        alpaca_targets = _in_hours

    # Day-type gate (entries only): drop Refined (acct2) targets for BREAKOUT entries
    # on non-Outside days. Paper All (acct1) is never gated — it keeps trading
    # everything to accumulate data. Reversals pass through. Fails open.
    if not _is_exit and alpaca_targets:
        try:
            _gate_today = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
        except Exception:
            _gate_today = datetime.now(timezone.utc).date().isoformat()
        _kept = []
        _gate_dropped = []
        for bt in alpaca_targets:
            _blk, _why = app._daytype_gate_block(
                strategy_name, ticker, _gate_today, _alpaca_broker_name(bt[0]))
            if _blk:
                _gate_dropped.append((bt[0], _why))
            else:
                _kept.append(bt)
        if _gate_dropped:
            _reason = _gate_dropped[0][1]
            app.log.info("Day-type gate: %s %s — skipped %s (%s)", order_action, ticker,
                         ",".join(_alpaca_broker_name(t) for (t, _w) in _gate_dropped), _reason)
            if not _kept and conn:
                app._update_exec(cur, trade_id, "skipped", _reason)
                conn.commit()
        alpaca_targets = _kept

    # Per-account reversal policy (entries only): drop targets whose account takes
    # reversals on one side only (e.g. "short") or takes none at all ("off", TV
    # Refined). Accounts with no policy (Kairos, Crew Paper, the farms) pass — the
    # farms keep trading reversals, which is what keeps the evidence accruing.
    # Per-target, so gating TV Refined never touches Kairos. Exits always pass.
    # No _gate_entry_side precondition: under "off" the side is irrelevant, and
    # _reversal_gate_block already passes an unknown side for the one-side policies.
    if not _is_exit and alpaca_targets:
        _rev_kept, _rev_dropped = [], []
        for bt in alpaca_targets:
            _acct_tag = _alpaca_broker_name(bt[0])
            if app._reversal_gate_block(strategy_name, _gate_entry_side or "", _acct_tag):
                _rev_dropped.append(_acct_tag)
            else:
                _rev_kept.append(bt)
        if _rev_dropped:
            _pols = {t: app._REVERSAL_SIDE_BY_TAG.get(t) for t in _rev_dropped}
            _why  = lambda t: ("takes no reversal entries" if _pols[t] == "off"
                               else f"trades {_pols[t]}-side reversals only")
            app.log.info("Reversal gate: %s %s — skipped %s", order_action, ticker,
                         "; ".join(f"{t} ({_pols[t]})" for t in sorted(set(_rev_dropped))))
            if not _rev_kept and conn:
                app._update_exec(cur, trade_id, "skipped",
                    "Reversal gate: " + "; ".join(f"{t} {_why(t)}"
                                                  for t in sorted(set(_rev_dropped))))
                conn.commit()
        alpaca_targets = _rev_kept

    # Profit-lock gate (entries only): drop Alpaca targets whose account has halted
    # after giving back its daily profit floor. Per-account — a halted book (e.g.
    # Refined) is skipped while others (e.g. Kairos) keep trading. Exits always pass.
    if not _is_exit and alpaca_targets and getattr(app, "PROFIT_LOCK_DOLLARS", 0) > 0:
        _pl_kept = [bt for bt in alpaca_targets
                    if not app._profit_lock_halted_for(_alpaca_broker_name(bt[0]))]
        if len(_pl_kept) != len(alpaca_targets):
            _pl_dropped = {_alpaca_broker_name(bt[0]) for bt in alpaca_targets} \
                          - {_alpaca_broker_name(bt[0]) for bt in _pl_kept}
            app.log.info("Profit lock gate: %s %s — skipped %s (gave back below $%g)",
                         order_action, ticker, ",".join(sorted(_pl_dropped)), app.PROFIT_LOCK_DOLLARS)
            if not _pl_kept and conn:
                app._update_exec(cur, trade_id, "blocked",
                                 f"Profit lock: {', '.join(sorted(_pl_dropped))} gave back below "
                                 f"${app.PROFIT_LOCK_DOLLARS:g} — halted for the day")
                conn.commit()
        alpaca_targets = _pl_kept

    # Daily-loss gate (entries only): drop Alpaca targets whose account hit its
    # MAX_DAILY_LOSS today and was halted + liquidated. Per-account — a halted book
    # is skipped while others keep trading. Exits always pass (must be able to close).
    if not _is_exit and alpaca_targets and getattr(app, "MAX_DAILY_LOSS", 0) < 0:
        _dl_kept = [bt for bt in alpaca_targets
                    if not app._daily_loss_halted_for(_alpaca_broker_name(bt[0]))]
        if len(_dl_kept) != len(alpaca_targets):
            _dl_dropped = {_alpaca_broker_name(bt[0]) for bt in alpaca_targets} \
                          - {_alpaca_broker_name(bt[0]) for bt in _dl_kept}
            app.log.warning("Daily-loss gate: %s %s — skipped %s (hit $%g daily loss)",
                            order_action, ticker, ",".join(sorted(_dl_dropped)), app.MAX_DAILY_LOSS)
            if not _dl_kept and conn:
                app._update_exec(cur, trade_id, "blocked",
                                 f"Daily loss limit: {', '.join(sorted(_dl_dropped))} hit "
                                 f"${app.MAX_DAILY_LOSS:g} — halted for the day")
                conn.commit()
        alpaca_targets = _dl_kept

    # --- Coinbase (sync-only; typically sub-second) ---
    for target, qty_override, _rs_cb in coinbase_targets:
        _qty       = qty_override if qty_override is not None else _rs_cb["quantity"]
        _sec_type  = _rs_cb["sec_type"]
        _currency  = _rs_cb["currency"]
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
                    sec_type = _sec_type,
                    currency = _currency,
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
    for target, qty_override, _rs_alp in alpaca_targets:
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

            # Capture loop variables for the closure — from this target's per-rule bundle
            _ticker      = _rs_alp.get("ticker") or ticker
            _action      = order_action
            _raw_action  = raw_action
            # Per-broker qty_override (set by the Refined refresh) wins over the rule's default.
            _qty         = qty_override if qty_override is not None else _rs_alp["quantity"]
            _price       = data.get("price") if data.get("order_type") == "LMT" else None
            _sec_type    = _rs_alp["sec_type"]
            _currency    = _rs_alp["currency"]
            _trade_id    = trade_id
            _opt_prem    = _rs_alp["opt_target_prem"]
            _opt_exp     = _rs_alp["opt_expiry_type"]
            _opt_right   = _rs_alp["opt_right_ovr"]
            _opt_ctrs    = _rs_alp["opt_contracts"]

            _strategy         = strategy_name
            _is_entry         = not _is_exit
            _ep_stop_loss     = _rs_alp["ep_stop_loss"]
            _ep_trail_trigger = _rs_alp["ep_trail_trigger"]
            _ep_trail_offset  = _rs_alp["ep_trail_offset"]
            _ep_trail_mode    = _rs_alp["ep_trail_mode"]
            _ep_hard_stop     = _rs_alp["ep_hard_stop"]
            _ep_max_hold_mins = _rs_alp["ep_max_hold_mins"]
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

            # Stable broker_tag for this target (alpaca / alpaca2 / alpaca3 /
            # alpaca4) — used to register the max-hold timer under the right
            # account. Previously hardcoded to a two-account guess.
            _broker_tag_outer = _alpaca_broker_name(target)
            def _place_alpaca_async(
                ticker=_ticker, action=_action, qty=_qty, price=_price,
                sec_type=_sec_type, currency=_currency, trade_id=_trade_id,
                opt_prem=_opt_prem, opt_exp=_opt_exp, opt_right=_opt_right, opt_ctrs=_opt_ctrs,
                strategy=_strategy, is_entry=_is_entry,
                ep_stop_loss=_ep_stop_loss, ep_trail_trigger=_ep_trail_trigger,
                ep_trail_offset=_ep_trail_offset, ep_trail_mode=_ep_trail_mode,
                ep_hard_stop=_ep_hard_stop, ep_max_hold_mins=_ep_max_hold_mins,
                broker=_broker_captured, broker_tag=_broker_tag_outer,
            ):
                _exec_status = _exec_detail = None
                try:
                    # Capital gates — block new entries on low available buying power
                    # (fixed $) or high buying-power utilization (%). One account
                    # fetch covers both.
                    if (is_entry and action in ("BUY", "SELL")
                            and (app.MIN_BUYING_POWER > 0 or app.BP_PAUSE_PCT > 0)):
                        try:
                            broker._ensure_client()
                            acct = broker._trading.get_account()
                            bp   = float(acct.buying_power or 0)
                            # (a) Minimum available buying power — long entries only.
                            if action == "BUY" and app.MIN_BUYING_POWER > 0 and bp < app.MIN_BUYING_POWER:
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
                            # (b) Utilization pause — any entry once gross exposure
                            #     consumes >= BP_PAUSE_PCT of total capacity.
                            if app.BP_PAUSE_PCT > 0:
                                long_mv  = float(getattr(acct, "long_market_value", 0) or 0)
                                short_mv = abs(float(getattr(acct, "short_market_value", 0) or 0))
                                gross    = long_mv + short_mv
                                denom    = gross + bp
                                util     = (gross / denom * 100) if denom > 0 else 0.0
                                if util >= app.BP_PAUSE_PCT:
                                    app.log.warning(
                                        "Buying-power pause: %s %s blocked — %.0f%% utilized (>= %.0f%%, $%.0f deployed / $%.0f free)",
                                        action, ticker, util, app.BP_PAUSE_PCT, gross, bp,
                                    )
                                    _exec_status = "blocked"
                                    _exec_detail = (
                                        f"Buying-power pause: {util:.0f}% utilized "
                                        f"(limit {app.BP_PAUSE_PCT:.0f}%)"
                                    )
                                    return
                        except Exception as _bpe:
                            app.log.warning(
                                "Capital gate check failed for %s: %s — proceeding with order",
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
                    # Register the max-hold timer: per-rule value if set, else the
                    # global MAX_HOLD_MINS backstop so no entry can run uncapped.
                    _eff_mhm = ep_max_hold_mins or (app.MAX_HOLD_MINS if app.MAX_HOLD_MINS > 0 else None)
                    if is_entry and result.get("success") and _eff_mhm:
                        _entry_ts = datetime.now(timezone.utc)
                        with app._risk_lock:
                            app._max_hold_positions[(broker_tag, ticker.upper())] = {
                                "entry_time":    _entry_ts,
                                "max_hold_mins": _eff_mhm,
                            }
                            # Clear stale auto-closed marker so the new position is
                            # protected. _auto_closed_symbols is only scrubbed inside
                            # _check_position_stops which doesn't run when risk limits
                            # are disabled — without this discard, every second-and-later
                            # trade in the same ticker is silently skipped by max hold.
                            app._auto_closed_symbols.discard((broker_tag, ticker.upper()))
                        app._persist_max_hold(broker_tag, ticker.upper(), _entry_ts, _eff_mhm)
                        app.log.info("Max hold registered: %s [%s] — %.0f min%s", ticker, broker_tag,
                                     _eff_mhm, "" if ep_max_hold_mins else " (global backstop)")
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

    for target, qty_override, _rs_ib in ib_targets:
        _use_live_ib   = _rs_ib["use_live_broker"]
        _live = (target == "ib-live") or (_use_live_ib and target != "ib-paper")
        active_broker  = app.ib_broker_live if (_live and app.ib_broker_live) else app.ib_broker
        submit_task    = app._submit_ib_live_task if (_live and app.ib_broker_live) else app._submit_ib_task
        mode_label     = "live" if (_live and app.ib_broker_live) else "paper"
        # Per-broker qty_override (set by the Refined refresh) wins over the rule's default.
        ib_qty         = qty_override if qty_override is not None else _rs_ib["quantity"]
        _ib_sec_type      = _rs_ib["sec_type"]
        _ib_currency      = _rs_ib["currency"]
        _ib_opt_prem      = _rs_ib["opt_target_prem"]
        _ib_opt_expiry    = _rs_ib["opt_expiry_type"]
        _ib_opt_right_ovr = _rs_ib["opt_right_ovr"]

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

            def _place_async(
                _qty=ib_qty,
                sec_type=_ib_sec_type, currency=_ib_currency,
                opt_target_prem=_ib_opt_prem, opt_expiry_type=_ib_opt_expiry,
                opt_right_ovr=_ib_opt_right_ovr,
            ):
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
