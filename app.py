import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from flask import Flask, abort, jsonify, render_template, request

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

    def _on_fill(_trade, fill):
        """Called by ib_async whenever a fill arrives — persists to DB."""
        try:
            exec_id = fill.execution.execId
            pnl     = None
            if fill.commissionReport and fill.commissionReport.realizedPNL == fill.commissionReport.realizedPNL:
                pnl = round(float(fill.commissionReport.realizedPNL), 2)
            conn = get_db()
            cur  = conn.cursor()
            p    = placeholder()
            cur.execute(
                f"INSERT INTO ib_executions "
                f"(exec_id,ts,symbol,sec_type,side,shares,price,order_id,account,exchange,pnl)"
                f" VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})"
                f" ON CONFLICT (exec_id) DO UPDATE SET pnl={p}",
                (exec_id,
                 str(fill.execution.time),
                 fill.contract.symbol,
                 fill.contract.secType,
                 fill.execution.side,
                 float(fill.execution.shares),
                 float(fill.execution.price),
                 fill.execution.orderId,
                 fill.execution.acctNumber,
                 fill.execution.exchange,
                 pnl,
                 pnl),
            ) if DATABASE_URL else cur.execute(
                f"INSERT OR REPLACE INTO ib_executions "
                f"(exec_id,ts,symbol,sec_type,side,shares,price,order_id,account,exchange,pnl)"
                f" VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})",
                (exec_id,
                 str(fill.execution.time),
                 fill.contract.symbol,
                 fill.contract.secType,
                 fill.execution.side,
                 float(fill.execution.shares),
                 float(fill.execution.price),
                 fill.execution.orderId,
                 fill.execution.acctNumber,
                 fill.execution.exchange,
                 pnl),
            )
            conn.commit()
            conn.close()
            log.info("IB fill saved: %s %s %s @ %s", fill.execution.side, fill.execution.shares, fill.contract.symbol, fill.execution.price)
            # Snapshot account 3s after fill so IB has time to update NAV
            def _delayed_snapshot():
                time.sleep(3)
                _store_account_snapshot()
            threading.Thread(target=_delayed_snapshot, daemon=True).start()
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

    def _connect_ib_background():
        """Try to connect to IB Gateway, retrying every 30s on failure."""
        time.sleep(3)  # let gunicorn finish forking before we open a socket
        while True:
            if not ib_broker.is_connected():
                try:
                    ib_broker.connect()
                    ib_broker.register_fill_callback(_on_fill)
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


@app.route("/")
def dashboard():
    return render_template("index.html")


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/strategies")
def strategies():
    return render_template("strategies.html")


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
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT ts, net_liq FROM account_snapshots ORDER BY id ASC")
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return jsonify([])
    if DATABASE_URL:
        cols = [d[0] for d in cur.description]
        snaps = [dict(zip(cols, r)) for r in rows]
    else:
        snaps = [dict(r) for r in rows]
    baseline = snaps[0]["net_liq"]
    return jsonify([
        {"time": s["ts"], "value": round(s["net_liq"] - baseline, 2)}
        for s in snaps
    ])


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

    open_trades = {}
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
            open_trades.setdefault(ticker, []).append((price, qty, received))
        elif action == "SELL":
            queue = open_trades.get(ticker, [])
            if queue:
                buy_price, buy_qty, _ = queue.pop(0)
                pnl = (price - buy_price) * min(qty, buy_qty)
                closed.append({"pnl": pnl, "time": received})

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
