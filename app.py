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

ib_broker = None

if os.environ.get("IB_HOST"):
    from brokers.ib_broker import IBBroker
    ib_broker = IBBroker()

    def _on_fill(_reqId, contract, execution):
        """Called by ib_async execDetailsEvent — persists execution to DB."""
        try:
            exec_id = execution.execId
            conn = get_db()
            cur  = conn.cursor()
            p    = placeholder()
            cur.execute(
                f"INSERT INTO ib_executions "
                f"(exec_id,ts,symbol,sec_type,side,shares,price,order_id,account,exchange,pnl)"
                f" VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})"
                f" ON CONFLICT (exec_id) DO NOTHING",
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
                 None),
            ) if DATABASE_URL else cur.execute(
                f"INSERT OR IGNORE INTO ib_executions "
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
                 None),
            )
            conn.commit()
            conn.close()
            log.info("IB fill saved: %s %s %s @ %s", execution.side, execution.shares, contract.symbol, execution.price)
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
        """Persist any fills already in the current IB session after connecting."""
        try:
            for fill in ib_broker.executions():
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
            log.info("IB fill sync complete")
        except Exception as e:
            log.warning("IB fill sync failed: %s", e)

    def _connect_ib_background():
        """Try to connect to IB Gateway, retrying every 30s on failure."""
        time.sleep(3)  # let gunicorn finish forking before we open a socket
        while True:
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
            time.sleep(10)  # check every 10s and reconnect if dropped

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
        """Close all open IB positions at 3:59 PM ET on weekdays."""
        ET = ZoneInfo("America/New_York")
        triggered_date = None
        while True:
            now = datetime.now(ET)
            today = now.date()
            if (now.weekday() < 5                          # Mon–Fri
                    and now.hour == 15 and now.minute == 59
                    and triggered_date != today):
                triggered_date = today
                log.info("EOD scheduler: closing all positions at 3:59 PM ET")
                try:
                    if ib_broker and ib_broker.is_connected():
                        result = ib_broker.close_all_positions()
                        log.info("EOD close result: %s", result)
                    else:
                        log.warning("EOD scheduler: IB not connected, skipping close")
                except Exception as e:
                    log.error("EOD close failed: %s", e)
            time.sleep(30)

    threading.Thread(target=_eod_close_scheduler, daemon=True).start()

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

    # Migrations for existing databases
    for col in ("strategy TEXT", "broker TEXT", "exec_status TEXT", "exec_detail TEXT"):
        try:
            cur.execute(f"ALTER TABLE trades ADD COLUMN {col}")
            conn.commit()
        except Exception:
            conn.rollback()

    conn.close()


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

    received_at  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    broker_name  = (data.get("broker") or "").strip().lower()

    conn = get_db()
    cur  = conn.cursor()

    # 1. Log the signal immediately
    trade_id = _insert_trade(cur, (
        data.get("ticker"),
        data.get("action"),
        data.get("sentiment"),
        data.get("quantity"),
        data.get("price"),
        data.get("time"),
        data.get("interval"),
        received_at,
        data.get("strategy"),
        broker_name or None,
    ))
    conn.commit()

    # 2. Route to broker
    exec_status = None
    exec_detail = None

    if broker_name == "ib":
        if ib_broker is None:
            exec_status = "error"
            exec_detail = "IB broker not initialised — check IB_HOST env var"
            log.warning("IB order skipped: broker not initialised")
        else:
            result = ib_broker.place_order(
                ticker   = data.get("ticker"),
                action   = data.get("action"),
                quantity = data.get("quantity", 1),
                price    = data.get("price") if data.get("order_type") == "LMT" else None,
                sec_type = data.get("sec_type", "STK"),
                currency = data.get("currency", "USD"),
            )
            exec_status = "ok"    if result.get("success") else "error"
            exec_detail = json.dumps(result)
            log.info("IB order result: %s", result)

    # 3. Write execution result back to the row
    if exec_status:
        _update_exec(cur, trade_id, exec_status, exec_detail)
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
    if ib_broker is not None:
        brokers = {"IB": ib_broker.status()}
    else:
        brokers = {"IB": {"connected": False, "broker": "IB",
                          "note": "IB_HOST not set"}}
    return jsonify(brokers)


@app.route("/api/broker/reconnect", methods=["POST"])
def broker_reconnect():
    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != WEBHOOK_TOKEN:
        abort(401)
    if ib_broker is None:
        return jsonify({"error": "IB_HOST not configured"}), 400
    try:
        if ib_broker.is_connected():
            ib_broker.disconnect()
        ib_broker.connect()
        return jsonify(ib_broker.status())
    except Exception as e:
        return jsonify({"connected": False, "error": str(e)}), 500


@app.route("/api/broker/close-all", methods=["POST"])
def broker_close_all():
    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != WEBHOOK_TOKEN:
        abort(401)
    if ib_broker is None:
        return jsonify({"error": "IB_HOST not configured"}), 400
    try:
        result = ib_broker.close_all_positions()
        return jsonify({"closed": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def dashboard():
    return render_template("index.html")


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/strategies")
def strategies():
    return render_template("strategies.html")


@app.route("/backtester")
def backtester():
    return render_template("backtester.html")


@app.route("/api/backtest/run", methods=["POST"])
def backtest_run():
    from strategies.camarilla import (
        parse_bars, run_backtest, optimise,
        is_trade_list_csv, parse_trade_list, _compute_stats,
    )

    if "csv_file" not in request.files:
        return jsonify({"error": "No CSV file uploaded"}), 400

    file = request.files["csv_file"]
    if not file.filename:
        return jsonify({"error": "Empty filename"}), 400

    file_bytes = file.read()

    # ── Trade-list import (TradingView Strategy Tester export) ────────────
    if is_trade_list_csv(file_bytes):
        try:
            trades = parse_trade_list(file_bytes)
        except Exception as e:
            return jsonify({"error": f"CSV parse error: {e}"}), 400

        if not trades:
            return jsonify({"error": "No valid trades found in CSV"}), 400

        stats = _compute_stats(trades)
        return jsonify({
            "mode":         "single",
            "source":       "trade_list",
            "bars_loaded":  len(trades),
            "trades":       trades[:1000],
            "stats":        stats,
        })

    # ── OHLCV backtest ────────────────────────────────────────────────────
    try:
        bars = parse_bars(file_bytes)
    except Exception as e:
        return jsonify({"error": f"CSV parse error: {e}"}), 400

    if len(bars) < 10:
        return jsonify({"error": "Not enough bars in CSV (need at least 10). "
                                 "Make sure you export OHLCV candle data, not the "
                                 "Strategy Tester trade list — or upload the trade "
                                 "list directly (it is also supported)."}), 400

    mode = request.form.get("mode", "single")

    if mode == "optimize":
        try:
            opt_params = {
                "long_trail_activation_range":  [
                    float(request.form.get("lta_min", 20)),
                    float(request.form.get("lta_max", 60)),
                    float(request.form.get("lta_step", 10)),
                ],
                "short_trail_activation_range": [
                    float(request.form.get("sta_min", 5)),
                    float(request.form.get("sta_max", 20)),
                    float(request.form.get("sta_step", 5)),
                ],
                "long_hard_stop_range": [
                    float(request.form.get("lhs_min", 50)),
                    float(request.form.get("lhs_max", 100)),
                    float(request.form.get("lhs_step", 10)),
                ],
                "short_hard_stop_range": [
                    float(request.form.get("shs_min", 10)),
                    float(request.form.get("shs_max", 30)),
                    float(request.form.get("shs_step", 5)),
                ],
                "trail_distance": float(request.form.get("trail_distance", 1)),
            }
            grid = optimise(bars, opt_params)
            return jsonify({"mode": "optimize", "bars_loaded": len(bars), "grid": grid})
        except Exception as e:
            log.exception("Backtest optimize error")
            return jsonify({"error": str(e)}), 500
    else:
        try:
            params = {
                "long_trail_activation":  float(request.form.get("lta", 40)),
                "short_trail_activation": float(request.form.get("sta", 10)),
                "long_hard_stop":         float(request.form.get("lhs", 70)),
                "short_hard_stop":        float(request.form.get("shs", 20)),
                "trail_distance":         float(request.form.get("trail_distance", 1)),
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


@app.route("/api/ib/trades")
def ib_trades():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM ib_executions ORDER BY ts DESC")
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
        "WHERE side = 'SLD' AND pnl IS NOT NULL "
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

    open_longs  = {}  # ticker → [(price, qty, time)]
    open_shorts = {}  # ticker → [(price, qty, time)]
    closed      = []

    for t in trades:
        action   = (t.get("action") or "").strip().upper()
        ticker   = (t.get("ticker") or "").strip().upper()
        received = t.get("received_at") or ""
        try:
            price = float(t.get("price") or 0)
            qty   = float(t.get("quantity") or 1)
        except (ValueError, TypeError):
            continue
        if not ticker or price == 0:
            continue
        if action == "BUY":
            # Closes an open short; otherwise opens a new long
            queue = open_shorts.get(ticker, [])
            if queue:
                entry_price, entry_qty, _ = queue.pop(0)
                pnl = (entry_price - price) * min(qty, entry_qty)
                closed.append({"pnl": pnl, "time": received})
            else:
                open_longs.setdefault(ticker, []).append((price, qty, received))
        elif action == "SELL":
            # Closes an open long; otherwise opens a new short
            queue = open_longs.get(ticker, [])
            if queue:
                entry_price, entry_qty, _ = queue.pop(0)
                pnl = (price - entry_price) * min(qty, entry_qty)
                closed.append({"pnl": pnl, "time": received})
            else:
                open_shorts.setdefault(ticker, []).append((price, qty, received))

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
        equity_curve.append({"time": c["time"], "value": round(cumulative, 2)})
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
# Entry point
# ---------------------------------------------------------------------------

init_db()

if __name__ == "__main__":
    app.run(debug=True)
