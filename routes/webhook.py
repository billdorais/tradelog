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

import json
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, jsonify, request


webhook_bp = Blueprint("webhook", __name__)


def _broker_family(target: str) -> str:
    if target in ("ib", "ib-paper", "ib-live"):
        return "ib"
    if target in ("alpaca", "alpaca-paper", "alpaca-live"):
        return "alpaca"
    return target


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

    matched_rule_id = None  # set when a routing rule matches; used to flip tv_alert_created
    try:
        rconn = app.get_db()
        rcur  = rconn.cursor()
        rcur.execute("SELECT id, nodes FROM routing_rules WHERE enabled=1 ORDER BY COALESCE(sort_order, id) ASC")
        rule_rows = rcur.fetchall()
        rconn.close()
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
            if broker_targets:
                broker_name = ",".join(broker_targets)
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

    # If no broker nodes fired from pipeline, fall back to the single broker_name from request body
    if not broker_targets and broker_name:
        broker_targets = [broker_name]

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

    # Daily max loss circuit breaker — block new orders if halt is active
    if app.MAX_DAILY_LOSS < 0 and order_action in ("BUY", "SELL"):
        with app._risk_lock:
            _halted = app._risk_halted
        if _halted:
            app.log.warning("Risk halt active — order blocked: %s %s %s", order_action, quantity, ticker)
            app._update_exec(cur, trade_id, "blocked", "Daily max loss limit reached — orders halted")
            conn.commit()
            conn.close()
            return jsonify({"status": "blocked", "reason": "daily_loss_limit"}), 200

    # Per-strategy block (set by position stop monitor)
    if strategy_name and order_action in ("BUY", "SELL"):
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

    # 2. Route to broker(s) — supports single or multi-broker pipelines
    exec_status = None
    exec_detail = None

    if not broker_targets:
        exec_status = "error"
        exec_detail = f"No routing pipeline matched strategy '{strategy_name}' — signal logged but no order placed. Check your Signal Router for a typo in the strategy name."
        app.log.warning("Webhook: no broker resolved for strategy '%s' — signal logged only", strategy_name)

    # All broker targets are now async — Alpaca/Coinbase fire in a background
    # thread so the webhook returns immediately and TradingView never times out.
    coinbase_targets = [t for t in broker_targets if _broker_family(t) == "coinbase"]
    alpaca_targets   = [t for t in broker_targets if _broker_family(t) == "alpaca"]
    ib_targets       = [t for t in broker_targets if _broker_family(t) == "ib"]

    # --- Coinbase (sync-only; typically sub-second) ---
    for target in coinbase_targets:
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
                    quantity = quantity,
                    price    = data.get("price") if data.get("order_type") == "LMT" else None,
                    sec_type = sec_type,
                    currency = currency,
                )
                exec_status = "ok" if result.get("success") else "error"
                exec_detail = json.dumps(result)
                app.log.info("Coinbase order %s %s %s: %s", order_action, quantity, ticker, result)
            except Exception as e:
                exec_status = "error"
                exec_detail = str(e)
                app.log.error("Coinbase order failed for %s %s %s: %s", order_action, quantity, ticker, e)

    # Commit any sync (Coinbase) results before launching async threads
    if conn and exec_status is not None:
        app._update_exec(cur, trade_id, exec_status, exec_detail)
        conn.commit()

    # --- Alpaca (async — order placement can take 1–3 s; we return 200 first) ---
    for target in alpaca_targets:
        if app.alpaca_broker is None:
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
                        result = app.alpaca_broker.place_option_order(
                            underlying     = ticker,
                            direction      = opt_direction,
                            expiry_type    = opt_exp or "friday",
                            contracts      = opt_ctrs,
                            target_premium = float(opt_prem),
                        )
                    else:
                        result = app.alpaca_broker.place_order(
                            ticker   = ticker,
                            action   = action,
                            quantity = qty,
                            price    = price,
                            sec_type = sec_type,
                            currency = currency,
                        )
                    _exec_status = "ok" if result.get("success") else "error"
                    _exec_detail = json.dumps(result)
                    app.log.info("Alpaca order %s %s %s: %s", action, qty, ticker, result)
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

            threading.Thread(target=_place_alpaca_async, daemon=True).start()

    for target in ib_targets:
        _live = (target == "ib-live") or (use_live_broker and target != "ib-paper")
        active_broker  = app.ib_broker_live if (_live and app.ib_broker_live) else app.ib_broker
        submit_task    = app._submit_ib_live_task if (_live and app.ib_broker_live) else app._submit_ib_task
        mode_label     = "live" if (_live and app.ib_broker_live) else "paper"

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
                        app.log.info("IB %s option order %s %s %s %s %s: %s",
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
                        app.log.info("IB %s order %s %s %s: %s",
                                     mode_label, order_action, quantity, ticker, result)
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
