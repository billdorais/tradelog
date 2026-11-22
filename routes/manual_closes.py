"""Manual-close log: every position closed by hand, so discretionary exits can be
judged against what the strategy's own exits would have done.

- The dashboard's Close buttons (/api/alpaca/close/<symbol>) record a row at the
  click: account, symbol, side, qty, entry, price and open P&L, plus an optional
  reason the user can add right after the close.
- ORB positions closed outside the dashboard (e.g. in Alpaca's app) are picked up
  from the ORB trade log: an exit that was neither the bracket's stop or target nor
  the 15:55 flatten. They show with source "outside dashboard".
- For ORB trades the review adds what the bracket would have made (the backtest's
  exit at the same fill and size), so each manual close has a yardstick.

GET /api/manual_closes is read-only. Recording never blocks or fails a close.
"""
from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, jsonify, request

manual_closes_bp = Blueprint("manual_closes", __name__)
log = logging.getLogger(__name__)

TABLE = "manual_closes"
COLUMNS = ("id", "closed_at", "account", "account_tag", "symbol", "side", "qty", "entry_price",
           "close_price", "unrealized_pnl", "reason", "source")
_REAL = {"qty", "entry_price", "close_price", "unrealized_pnl"}
ET = ZoneInfo("America/New_York")
_ready = {"ok": False}
_ready_lock = threading.Lock()
MATCH_SECS = 180          # a dashboard row and an ORB exit this close together are the same close


def _app():
    import app as _kairos
    return _kairos


def ensure_table():
    with _ready_lock:
        if _ready["ok"]:
            return
        a = _app()
        conn = a.get_db()
        try:
            cols = ", ".join(f"{c} {'REAL' if c in _REAL else 'TEXT'}" for c in COLUMNS if c != "id")
            conn.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} (id TEXT PRIMARY KEY, {cols})")
            conn.commit()
        finally:
            conn.close()
        _ready["ok"] = True


def _f(x):
    try:
        return float(x) if x is not None and x != "" else None
    except (TypeError, ValueError):
        return None


def record(account, tag, symbol, pos=None, reason=None, source="dashboard", now=None):
    """Log one manual close. `pos` is the broker's position dict read just before the
    close (may be None if it couldn't be read). Returns the row id, or None on error."""
    try:
        ensure_table()
        pos = pos or {}
        side = str(pos.get("side") or "").lower().replace("positionside.", "") or None
        row = {"id": uuid.uuid4().hex[:12],
               "closed_at": (now or datetime.now(timezone.utc)).isoformat(),
               "account": str(account), "account_tag": tag, "symbol": symbol.upper(),
               "side": side, "qty": _f(pos.get("qty")),
               "entry_price": _f(pos.get("avg_entry_price")),
               "close_price": _f(pos.get("current_price")),
               "unrealized_pnl": _f(pos.get("unrealized_pnl")),
               "reason": (reason or "").strip()[:500] or None, "source": source}
        a = _app()
        ph = a.placeholder()
        conn = a.get_db()
        try:
            conn.execute(f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES ({', '.join([ph] * len(COLUMNS))})",
                         [row[c] for c in COLUMNS])
            conn.commit()
        finally:
            conn.close()
        return row["id"]
    except Exception as e:
        log.warning("Manual-close log: could not record %s [%s]: %s", symbol, tag, e)
        return None


def set_reason(row_id, reason):
    ensure_table()
    a = _app()
    ph = a.placeholder()
    conn = a.get_db()
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE {TABLE} SET reason={ph} WHERE id={ph}",
                    ((reason or "").strip()[:500] or None, row_id))
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()


def _et_day(iso):
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).astimezone(ET).date().isoformat()
    except (TypeError, ValueError):
        return None


def logged(day_from=None, day_to=None, account=None):
    ensure_table()
    a = _app()
    conn = a.get_db()
    try:
        out = conn.execute(f"SELECT {', '.join(COLUMNS)} FROM {TABLE} ORDER BY closed_at").fetchall()
    finally:
        conn.close()
    rows = [dict(zip(COLUMNS, tuple(r))) for r in out]
    for r in rows:
        r["day"] = _et_day(r["closed_at"])
    return [r for r in rows
            if (not day_from or (r["day"] or "") >= day_from) and (not day_to or (r["day"] or "") <= day_to)
            and (not account or r["account"] == str(account))]


# ── ORB yardstick ────────────────────────────────────────────────────────────

def _orb_books():
    try:
        from routes import orb_live
        return orb_live, orb_live.BOOKS
    except Exception:
        return None, {}


def _orb_manual_exit(t):
    """An ORB trade that exited by something other than its bracket or the 15:55 flatten."""
    if t.get("status") != "exited" or not t.get("exit_time") or t.get("flatten_at"):
        return False
    return (t.get("exit_reason") or "") not in ("stop", "target")


def _pnl(t, px):
    fill, qty = _f(t.get("fill_price")), _f(t.get("qty"))
    if px is None or fill is None or not qty:
        return None
    sd = 1 if str(t.get("side") or "").lower() == "long" else -1
    return round((px - fill) * sd * qty, 2)


def review(day_from=None, day_to=None, account=None):
    """Logged closes plus ORB exits closed outside the dashboard, each ORB one with the
    bracket's would-be result beside the actual one."""
    rows = logged(day_from, day_to, account)
    orb, books = _orb_books()
    for book in books:
        if account and str(account) != str(book):
            continue
        try:
            trades = orb.rows(day_from=day_from, book=book, limit=2000)
        except Exception as e:
            log.debug("Manual-close review: ORB book %s unreadable: %s", book, e)
            continue
        for t in trades:
            if day_to and t["day"] > day_to:
                continue
            match = next((r for r in rows if r["account"] == str(book) and r["symbol"] == t["ticker"]
                          and r["day"] == t["day"]), None)
            if not _orb_manual_exit(t) and match is None:
                continue
            if match is None:
                match = {"id": None, "closed_at": t["exit_time"], "account": str(book),
                         "account_tag": None, "symbol": t["ticker"], "side": t.get("side"),
                         "qty": _f(t.get("qty")), "entry_price": _f(t.get("fill_price")),
                         "close_price": _f(t.get("exit_price")), "unrealized_pnl": None,
                         "reason": None, "source": "outside dashboard", "day": t["day"]}
                rows.append(match)
            actual = _pnl(t, _f(t.get("exit_price")))
            bracket = _pnl(t, _f(t.get("bt_exit")))
            match.update(strategy="ORB", actual_pnl=actual, bracket_pnl=bracket,
                         bracket_exit=t.get("bt_reason"),
                         vs_bracket=round(actual - bracket, 2) if actual is not None and bracket is not None else None)
    rows.sort(key=lambda r: r["closed_at"] or "")
    cmp = [r for r in rows if r.get("vs_bracket") is not None]
    summary = {"closes": len(rows), "compared": len(cmp),
               "actual_pnl": round(sum(r["actual_pnl"] for r in cmp), 2) if cmp else None,
               "bracket_pnl": round(sum(r["bracket_pnl"] for r in cmp), 2) if cmp else None,
               "vs_bracket": round(sum(r["vs_bracket"] for r in cmp), 2) if cmp else None,
               "better": sum(1 for r in cmp if r["vs_bracket"] > 0),
               "worse": sum(1 for r in cmp if r["vs_bracket"] < 0)}
    return {"closes": rows, "summary": summary}


# ── endpoints ────────────────────────────────────────────────────────────────

@manual_closes_bp.route("/api/manual_closes")
def api_manual_closes():
    """?from=YYYY-MM-DD &to= &account=N — read-only."""
    try:
        return jsonify(review(request.args.get("from"), request.args.get("to"), request.args.get("account")))
    except Exception as e:
        log.warning("Manual-close review failed: %s", e)
        return jsonify({"error": str(e)}), 500


@manual_closes_bp.route("/api/manual_closes/<row_id>/reason", methods=["POST"])
def api_manual_close_reason(row_id):
    """{reason} — add or change the reason on a logged close (token required)."""
    a = _app()
    token = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if token != a.WEBHOOK_TOKEN:
        abort(401)
    reason = (request.get_json(silent=True) or {}).get("reason")
    if not set_reason(row_id, reason):
        return jsonify({"error": "no such close"}), 404
    return jsonify({"ok": True})
