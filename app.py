import os
import sqlite3
from datetime import datetime
from flask import Flask, request, jsonify, render_template, abort
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "change-me")
DATABASE_URL = os.environ.get("DATABASE_URL")  # Set automatically by Railway Postgres plugin

# ---------------------------------------------------------------------------
# Database — PostgreSQL on Railway, SQLite locally
# ---------------------------------------------------------------------------

def get_db():
    if DATABASE_URL:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    else:
        conn = sqlite3.connect("trades.db")
        conn.row_factory = sqlite3.Row
        return conn


def placeholder():
    """SQL placeholder: %s for Postgres, ? for SQLite."""
    return "%s" if DATABASE_URL else "?"


def init_db():
    conn = get_db()
    cur = conn.cursor()
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
            strategy    TEXT
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
            strategy    TEXT
        )
    """)
    # Migration: add strategy column to existing databases
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN strategy TEXT")
        conn.commit()
    except Exception:
        conn.rollback()
    conn.commit()
    conn.close()


def rows_to_dicts(rows):
    """Normalize rows from either psycopg2 or sqlite3 into plain dicts."""
    if DATABASE_URL:
        # psycopg2 returns tuples; we need column names from the cursor
        # (handled in callers that pass cursor description)
        return rows
    return [dict(r) for r in rows]


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

    received_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    p = placeholder()

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        f"""
        INSERT INTO trades (ticker, action, sentiment, quantity, price, tv_time, interval, received_at, strategy)
        VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p})
        """,
        (
            data.get("ticker"),
            data.get("action"),
            data.get("sentiment"),
            data.get("quantity"),
            data.get("price"),
            data.get("time"),
            data.get("interval"),
            received_at,
            data.get("strategy"),
        ),
    )
    conn.commit()
    conn.close()

    return jsonify({"status": "ok"}), 200


@app.route("/api/trades")
def api_trades():
    limit = min(int(request.args.get("limit", 200)), 1000)
    p = placeholder()
    conn = get_db()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM trades ORDER BY id DESC LIMIT {p}", (limit,))
    rows = cur.fetchall()

    if DATABASE_URL:
        cols = [desc[0] for desc in cur.description]
        result = [dict(zip(cols, row)) for row in rows]
    else:
        result = [dict(r) for r in rows]

    conn.close()
    return jsonify(result)


@app.route("/")
def dashboard():
    return render_template("index.html")


@app.route("/api/stats")
def api_stats():
    """
    Compute performance stats by pairing BUY/SELL signals per ticker (FIFO).
    Returns win rate, avg win/loss, profit factor, max drawdown, equity curve.
    """
    strategy_filter = request.args.get("strategy")
    conn = get_db()
    cur = conn.cursor()
    p = placeholder()
    if strategy_filter:
        cur.execute(f"SELECT * FROM trades WHERE strategy = {p} ORDER BY id ASC", (strategy_filter,))
    else:
        cur.execute("SELECT * FROM trades ORDER BY id ASC")
    rows = cur.fetchall()

    if DATABASE_URL:
        cols = [desc[0] for desc in cur.description]
        trades = [dict(zip(cols, row)) for row in rows]
    else:
        trades = [dict(r) for r in rows]

    conn.close()

    # ------------------------------------------------------------------
    # Pair BUY -> SELL per ticker using FIFO to compute closed trade P&L
    # ------------------------------------------------------------------
    open_trades = {}   # ticker -> list of (price, quantity, time)
    closed = []        # list of {"pnl": float, "time": str}

    for t in trades:
        action = (t.get("action") or "").strip().upper()
        ticker = (t.get("ticker") or "").strip().upper()
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
                fill_qty = min(qty, buy_qty)
                pnl = (price - buy_price) * fill_qty
                closed.append({"pnl": pnl, "time": received})

    # ------------------------------------------------------------------
    # Compute stats from closed list
    # ------------------------------------------------------------------
    if not closed:
        return jsonify({
            "completed_trades": 0,
            "win_rate":         0,
            "avg_win":          0,
            "avg_loss":         0,
            "profit_factor":    None,
            "max_drawdown":     0,
            "equity_curve":     [],
        })

    wins   = [c["pnl"] for c in closed if c["pnl"] > 0]
    losses = [c["pnl"] for c in closed if c["pnl"] <= 0]

    win_rate      = round(len(wins) / len(closed) * 100, 1)
    avg_win       = round(sum(wins) / len(wins), 2) if wins else 0
    avg_loss      = round(sum(losses) / len(losses), 2) if losses else 0
    gross_loss    = abs(sum(losses))
    profit_factor = round(sum(wins) / gross_loss, 2) if gross_loss > 0 else None

    # Equity curve & max drawdown
    equity_curve = []
    cumulative   = 0
    peak         = 0
    max_dd       = 0

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

# Run at startup regardless of how the app is launched (gunicorn or direct)
init_db()

if __name__ == "__main__":
    app.run(debug=True)
