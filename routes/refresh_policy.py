"""Refresh-policy test for the Refined books: daily top N vs a weekly refresh
with a rank buffer, replayed on each book's own audition farm.

Every past trading day, the roster each policy would have held is rebuilt with
the live ranking code (_stats_from_round_trips -> _refined_scored, then
_apply_ticker_cap or _select_with_buffer) on the farm's trades up to the previous
close, and scored on the farm's trades that day. Nothing is hindsight: a roster
only ever sees data from before the day it is judged on.

Scored on FARM trades ($1k, ungated, all day), so this measures the selection
rule, not the book's gates or execution. Read-only; the live switch is the
refresh-mode setting (REFINED_REFRESH_MODE / KAIROS_REFINED_REFRESH_MODE), which
this module only reads and writes.
"""
from __future__ import annotations

import json
import threading
import time
from bisect import bisect_left, bisect_right
from datetime import date, timedelta

from flask import Blueprint, jsonify, request

refresh_policy_bp = Blueprint("refresh_policy", __name__)

# book -> (farm account, reversal-policy tag, book label, farm label)
BOOKS = {"tv": ("1", "alpaca2", "TV Refined", "TV Farm"),
         "kairos": ("5", "alpaca3", "Kairos Refined", "Kairos Farm")}

POLICIES = (
    ("daily_top",     "Daily, top N",          False, False),   # today's rule
    ("weekly_top",    "Weekly, top N",         True,  False),
    ("weekly_buffer", "Weekly, rank buffer",   True,  True),    # the proposal
    ("daily_buffer",  "Daily, rank buffer",    False, True),
)
WARMUP_DAYS = 28          # don't judge rosters ranked on less than four weeks of farm
RECENCY_DAYS = 10         # the snapshot's recency-blend window
_CACHE_TTL = 10 * 60
_cache, _cache_lock = {}, threading.Lock()


def _app():
    import app as _kairos
    return _kairos


def _pct(t):
    base = abs(float(t.get("entry_price") or 0)) * abs(float(t.get("qty") or 0))
    return float(t.get("pnl") or 0) / base * 100.0 if base else None


def _week(d):
    x = date.fromisoformat(d)
    return (x - timedelta(days=x.weekday())).isoformat()


def _summary(rows):
    """rows: [(pct, pnl)] -> trades, avg_pct, sum_pct, win_rate, profit_factor, pnl."""
    if not rows:
        return {"trades": 0, "avg_pct": None, "sum_pct": 0.0, "win_rate": None,
                "profit_factor": None, "pnl": 0.0}
    pcts = [p for p, _ in rows if p is not None]
    pnls = [d for _, d in rows]
    wins = [d for d in pnls if d > 0]
    gl = abs(sum(d for d in pnls if d <= 0))
    return {"trades": len(rows),
            "avg_pct": round(sum(pcts) / len(pcts), 4) if pcts else None,
            "sum_pct": round(sum(pcts), 3),
            "win_rate": round(len(wins) / len(rows) * 100, 1),
            "profit_factor": round(sum(wins) / gl, 2) if gl > 0 else None,
            "pnl": round(sum(pnls), 2)}


def simulate(rts, rank, pick_top, pick_buffer, n, window_days, entry_rank, exit_rank,
             warmup_days=WARMUP_DAYS, recency_days=RECENCY_DAYS):
    """Replay every policy in POLICIES over the farm's history.

    rts:  round-trips with strategy, pnl, entry_price, qty, exit_time and an ET
          exit `date`.
    rank(window_rts, recent_rts) -> scored [(name, stats, score)] best first.
    pick_top(scored, n) -> rows;  pick_buffer(scored, n, incumbents, e, x) -> rows.

    The roster in force on trading day D is decided at the close of the previous
    trading day P, ranked on [P - window_days, P] (10-day recency on
    [P - recency_days, P]). Weekly policies re-decide only at the last trading
    day of each week, so a roster holds Monday to Friday."""
    rts = sorted((t for t in rts if t.get("date")), key=lambda t: (t["date"], t.get("exit_time") or ""))
    dates = [t["date"] for t in rts]
    days = sorted(set(dates))
    if len(days) < 2:
        return {"error": "not enough farm history to replay", "policies": []}
    start = (date.fromisoformat(days[0]) + timedelta(days=warmup_days)).isoformat()
    eval_idx = [i for i, d in enumerate(days) if i > 0 and d >= start]
    if not eval_idx:
        return {"error": f"the farm's history ({days[0]} to {days[-1]}) is shorter than the "
                         f"{warmup_days}-day warm-up", "policies": []}

    scored_cache = {}

    def _scored(p):
        if p not in scored_cache:
            lo = (date.fromisoformat(p) - timedelta(days=window_days)).isoformat()
            rlo = (date.fromisoformat(p) - timedelta(days=recency_days)).isoformat()
            hi = bisect_right(dates, p)
            scored_cache[p] = rank(rts[bisect_left(dates, lo):hi], rts[bisect_left(dates, rlo):hi])
        return scored_cache[p]

    state = {k: {"roster": None, "rows": [], "adds": 0, "refreshes": 0, "sizes": [],
                 "by_day": {}, "by_week": {}} for k, *_ in POLICIES}
    farm_rows = []
    for i in eval_idx:
        d, p = days[i], days[i - 1]
        week_end = _week(p) != _week(d)          # P closed out its week
        todays = [t for t in rts[bisect_left(dates, d):bisect_right(dates, d)]]
        farm_rows.extend((_pct(t), float(t.get("pnl") or 0)) for t in todays)
        for key, _label, weekly, buffered in POLICIES:
            st = state[key]
            if st["roster"] is None or not weekly or week_end:
                sc = _scored(p)
                if buffered:
                    rows = pick_buffer(sc, n, st["roster"] or [], entry_rank, exit_rank)
                else:
                    rows = pick_top(sc, n)
                new = [r[0] for r in rows]
                if st["roster"] is not None:
                    st["adds"] += len(set(new) - set(st["roster"]))
                    st["refreshes"] += 1
                st["roster"] = new
            held = set(st["roster"])
            st["sizes"].append(len(held))
            mine = [(_pct(t), float(t.get("pnl") or 0)) for t in todays if t["strategy"] in held]
            st["rows"].extend(mine)
            s = sum(x for x, _ in mine if x is not None)
            st["by_day"][d] = s
            st["by_week"][_week(d)] = st["by_week"].get(_week(d), 0.0) + s

    eval_days = [days[i] for i in eval_idx]
    weeks = sorted({_week(d) for d in eval_days})
    out = []
    for key, label, weekly, buffered in POLICIES:
        st = state[key]
        run, line = 0.0, []
        for d in eval_days:
            run += st["by_day"].get(d, 0.0)
            line.append(round(run, 3))
        out.append({"key": key, "label": label, "weekly": weekly, "buffered": buffered,
                    **_summary(st["rows"]),
                    "refreshes": st["refreshes"], "adds": st["adds"],
                    "avg_roster": round(sum(st["sizes"]) / len(st["sizes"]), 1) if st["sizes"] else 0,
                    "weekly_sum_pct": [round(st["by_week"].get(w, 0.0), 3) for w in weeks],
                    "line": line})
    base = {r["key"]: r for r in out}["daily_top"]["weekly_sum_pct"]
    for r in out:
        r["weeks_beat_daily"] = sum(1 for a, b in zip(r["weekly_sum_pct"], base) if a > b)
    return {"policies": out, "dates": eval_days, "weeks": weeks,
            "first_farm_date": days[0], "eval_from": eval_days[0], "eval_to": eval_days[-1],
            "farm": _summary(farm_rows), "error": None}


# ── live wiring ──────────────────────────────────────────────────────────────

def _rule_matcher(a):
    """The snapshot's enabled-rule whitelist, as a predicate (None = allow all)."""
    pats = []
    try:
        conn = a.get_db(); cur = conn.cursor()
        cur.execute("SELECT nodes FROM routing_rules WHERE enabled=1")
        for row in cur.fetchall():
            raw = row[0] if a.DATABASE_URL else row["nodes"]
            for nd in (json.loads(raw) if isinstance(raw, str) else (raw or [])):
                if nd.get("type") == "strategy" and (nd.get("value") or "").strip():
                    pats.append(nd["value"].strip().upper())
        conn.close()
    except Exception:
        return None

    def ok(name):
        s = name.upper()
        return any(p == s or (p.endswith("*") and s.startswith(p[:-1]))
                   or (p.startswith("*") and s.endswith(p[1:])) for p in pats)
    return ok


def _settings(a, book):
    key = "REFINED_DAYS" if book == "tv" else "KAIROS_REFINED_DAYS"
    try:
        days = int(a._load_setting(key) or a.REFINED_WINDOW_DAYS)
    except (TypeError, ValueError):
        days = a.REFINED_WINDOW_DAYS
    min_trades = a._REFINED_MIN_TRADES if book == "tv" else a._KAIROS_REFINED_MIN_TRADES
    return days, min_trades


def run_test(book, n=20, days=None, min_trades=None):
    a = _app()
    import routes.farms as farms
    num, tag, label, farm_label = BOOKS[book]
    d0, m0 = _settings(a, book)
    days, min_trades = days or d0, min_trades or m0
    rts, err = farms._farm_round_trips(num, "", "", full=True)
    if err:
        return {"error": f"{farm_label}: {err}"}
    rts = [dict(t, date=farms.et_date(t.get("exit_time")) or t.get("date") or "")
           for t in rts if not a._reversal_gate_block(t.get("strategy"), t.get("side"), tag)]
    rule_ok = _rule_matcher(a)
    bands = a._load_excluded_bands()

    def rank(window, recent):
        sm = a._stats_from_round_trips(window, apply_exclusions=False)
        sm10 = a._stats_from_round_trips(recent, apply_exclusions=False)
        return a._refined_scored(sm, sm10, min_trades, rule_ok, bands)[0]

    entry_rank, exit_rank = a._buffer_ranks(n)
    out = simulate(rts, rank,
                   lambda sc, k: a._apply_ticker_cap(sc, k)[0],
                   lambda sc, k, inc, e, x: a._select_with_buffer(sc, k, inc, e, x)[0],
                   n, days, entry_rank, exit_rank)
    out.update({"book": book, "label": label, "farm_label": farm_label, "n": n,
                "window_days": days, "min_trades": min_trades,
                "entry_rank": entry_rank, "exit_rank": exit_rank,
                "anchor_from_date": (a._load_setting("KAIROS_REFINED_FROM_DATE") or "").strip() or None
                                    if book == "kairos" else None})
    return out


@refresh_policy_bp.route("/api/refined/refresh_test")
def api_refresh_test():
    """?book=tv|kairos &n=20 — replay daily vs weekly-with-buffer refreshes on the
    book's farm. Read-only; cached 10 minutes."""
    book = (request.args.get("book") or "kairos").lower()
    if book not in BOOKS:
        return jsonify({"error": "book must be tv or kairos"}), 400
    try:
        n = max(1, min(50, int(request.args.get("n") or 20)))
    except ValueError:
        n = 20
    key = (book, n)
    with _cache_lock:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < _CACHE_TTL and not request.args.get("fresh"):
            return jsonify(hit[1])
    out = run_test(book, n)
    if out.get("error"):
        return jsonify(out), 409
    with _cache_lock:
        _cache[key] = (time.time(), out)
    return jsonify(out)


@refresh_policy_bp.route("/api/refined/refresh_mode", methods=["GET", "POST"])
def api_refresh_mode():
    """GET both books' refresh modes. POST {book, mode: daily|weekly_buffer}.
    Weekly mode takes effect at the next scheduled refresh (a Friday close); the
    roster in force now is kept until then."""
    a = _app()
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        book, mode = (body.get("book") or "").lower(), (body.get("mode") or "").lower()
        if book not in BOOKS or mode not in a.REFINED_MODES:
            return jsonify({"error": "book must be tv|kairos and mode daily|weekly_buffer"}), 400
        a._save_setting(a.REFINED_MODE_KEYS[book], mode)
        a.log.info("Refined refresh mode for %s set to %s", book, mode)
    e, x = a._buffer_ranks(20)
    return jsonify({"modes": {b: a._refined_refresh_mode(b) for b in BOOKS},
                    "entry_rank": e, "exit_rank": x})
