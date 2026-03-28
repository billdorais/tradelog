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
            received_at TEXT
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
            received_at TEXT
        )
    """)
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
        INSERT INTO trades (ticker, action, sentiment, quantity, price, tv_time, interval, received_at)
        VALUES ({p},{p},{p},{p},{p},{p},{p},{p})
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


@app.route("/api/stats")
def api_stats():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT ticker, action, price, quantity, received_at FROM trades ORDER BY received_at ASC"
    )
    rows = cur.fetchall()
    if DATABASE_URL:
        cols = [desc[0] for desc in cur.description]
        trades = [dict(zip(cols, row)) for row in rows]
    else:
        trades = [dict(r) for r in rows]
    conn.close()

    # FIFO pairing of buy → sell per ticker
    open_positions = {}  # ticker -> [(price, qty)]
    completed = []       # [(pnl, received_at)]

    for t in trades:
        ticker = t.get("ticker")
        action = (t.get("action") or "").lower()
        try:
            price = float(t.get("price") or 0)
            qty   = float(t.get("quantity") or 0)
        except (TypeError, ValueError):
            continue
        if price == 0 or qty == 0:
            continue

        if action == "buy":
            open_positions.setdefault(ticker, []).append((price, qty))
        elif action == "sell":
            queue = open_positions.get(ticker, [])
            if queue:
                entry_price, entry_qty = queue.pop(0)
                pnl = (price - entry_price) * min(qty, entry_qty)
                completed.append((pnl, t["received_at"]))

    if not completed:
        return jsonify({
            "completed_trades": 0,
            "win_rate": None,
            "avg_win": None,
            "avg_loss": None,
            "profit_factor": None,
            "max_drawdown": None,
            "equity_curve": [],
        })

    wins   = [p for p, _ in completed if p > 0]
    losses = [p for p, _ in completed if p < 0]

    win_rate      = round(len(wins) / len(completed) * 100, 1)
    avg_win       = round(sum(wins)   / len(wins),   2) if wins   else 0
    avg_loss      = round(sum(losses) / len(losses), 2) if losses else 0
    profit_factor = round(sum(wins) / abs(sum(losses)), 2) if losses else None

    running = 0
    peak    = 0
    max_dd  = 0
    equity_curve = []
    for pnl, ts in completed:
        running += pnl
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd
        equity_curve.append({"time": ts, "value": round(running, 2)})

    return jsonify({
        "completed_trades": len(completed),
        "win_rate":      win_rate,
        "avg_win":       avg_win,
        "avg_loss":      avg_loss,
        "profit_factor": profit_factor,
        "max_drawdown":  round(max_dd, 2),
        "equity_curve":  equity_curve,
    })


@app.route("/")
def dashboard():
    return render_template("index.html")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

# Run at startup regardless of how the app is launched (gunicorn or direct)
init_db()

if __name__ == "__main__":
    app.run(debug=True)
