"""Farm Leaders — the two farms' top strategies, period by period, and the button
that wires them into Kairos Select (account 7).

The farms (TV Farm = acct1, Kairos Farm = acct5) trade every pipeline at an equal
$1,000 a trade, so their per-strategy P&L is directly comparable across both and
across time. This page ranks each farm's strategies on NET P&L over a window, shows
how the top N did week by week or month by month, and — the part that keeps it
honest — a FORWARD series: at each period boundary, re-rank on only what was known
then, take that top N, and record what it made in the NEXT period. The hindsight
series always looks good (it was picked for it); the forward series is what a
roster refreshed on this page would actually have earned.

Apply reconciles Kairos Select to the combined roster: TV Farm picks are wired with
entry_source=tv, Kairos Farm picks with entry_source=kairos, mirroring how each
earned its record. It is an explicit human action — nothing here runs on a timer,
so Select is still not auto-sourced.
"""
from __future__ import annotations

import copy
from collections import OrderedDict
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from flask import Blueprint, jsonify, render_template, request

farms_bp = Blueprint("farms", __name__)

_ET = ZoneInfo("America/New_York")

FARMS = (("tv", "1", "TV Farm"), ("kairos", "5", "Kairos Farm"))
ENTRY_BY_FARM = {"tv": "tv", "kairos": "kairos"}

SELECT_TAG      = "alpaca7"
SELECT_BROKERS  = ("alpaca-paper-7", "alpaca-live-7")
SELECT_SUFFIX   = " · Select"
DEFAULT_SIZE    = 15_000      # Kairos Select's flat per-trade notional
DEFAULT_MIN_TRADES = 5        # same floor the Refined leaderboards use
DEFAULT_CAP     = 2           # same max-per-ticker as the Refined leaderboards


# ── Pure helpers (unit-tested) ────────────────────────────────────────────────

def et_date(iso):
    """'2026-10-15T19:58:00Z' → '2026-10-15' in New York time ('' if unparseable).
    UTC dates put a late-session exit on the next day."""
    try:
        d = datetime.fromisoformat((iso or "").replace("Z", "+00:00"))
    except ValueError:
        return ""
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(_ET).strftime("%Y-%m-%d")


def period_key(date, period):
    """Week → that week's Monday (YYYY-MM-DD); month → YYYY-MM."""
    if not date:
        return ""
    if period == "month":
        return date[:7]
    d = datetime.strptime(date, "%Y-%m-%d").date()
    return (d - timedelta(days=d.weekday())).isoformat()


def strategy_stats(trips):
    """{strategy: {trades, wins, win_rate, pnl, avg, profit_factor}}."""
    groups = {}
    for t in trips:
        groups.setdefault(t["strategy"], []).append(float(t["pnl"]))
    out = {}
    for name, pnls in groups.items():
        wins = [p for p in pnls if p > 0]
        gross_w = sum(wins)
        gross_l = abs(sum(p for p in pnls if p <= 0))
        out[name] = {
            "trades": len(pnls), "wins": len(wins),
            "win_rate": round(len(wins) / len(pnls) * 100, 1),
            "pnl": round(sum(pnls), 2),
            "avg": round(sum(pnls) / len(pnls), 2),
            "profit_factor": round(gross_w / gross_l, 2) if gross_l > 0 else None,
        }
    return out


def rank_top(trips, n, min_trades=DEFAULT_MIN_TRADES, cap=DEFAULT_CAP):
    """Top `n` strategies by net P&L, net-positive and at least `min_trades`, at
    most `cap` per ticker (0 = no cap). Returns [(name, stats)] best first.

    The cap mirrors the Refined leaderboards: without it a top 10 can be four of
    one underlying's level pairs, which lose together. Unlike Refined it does NOT
    backfill past the cap — on this page "top 10" should never quietly contain a
    third SPY because the pool ran short."""
    stats = strategy_stats(trips)
    eligible = sorted(((k, v) for k, v in stats.items()
                       if v["pnl"] > 0 and v["trades"] >= min_trades),
                      key=lambda kv: (-kv[1]["pnl"], kv[0]))
    out, per = [], {}
    for name, s in eligible:
        if len(out) >= n:
            break
        tk = name.split("_", 1)[0].upper()
        if cap and per.get(tk, 0) >= cap:
            continue
        per[tk] = per.get(tk, 0) + 1
        out.append((name, s))
    return out


def farm_view(trips, n, period, min_trades=DEFAULT_MIN_TRADES, cap=DEFAULT_CAP):
    """Everything the page shows for one farm.

    trips: [{strategy, pnl, date}] with date in ET.

    periods[i]:
      top_pnl      — the CURRENT top N (ranked on the whole window) in that period.
                     Hindsight: they were chosen because these numbers are good.
      forward_pnl  — top N ranked on periods [0, i) only, traded in period i.
                     None for the first period (nothing to rank on yet).
      farm_pnl / strategies — the whole farm that period, and how many strategies
                     traded; avg_n_pnl = farm_pnl / strategies × N, i.e. what N
                     average strategies made. The bar the forward line has to clear.
    """
    for t in trips:
        t["period"] = period_key(t["date"], period)
    keys = sorted({t["period"] for t in trips if t["period"]})
    top = rank_top(trips, n, min_trades, cap)
    top_names = [name for name, _ in top]
    top_set = set(top_names)

    by_period = {k: [] for k in keys}
    for t in trips:
        if t["period"] in by_period:
            by_period[t["period"]].append(t)

    cells = {name: {} for name in top_names}   # strategy → {period: pnl}
    periods, seen = [], []
    for i, k in enumerate(keys):
        rows = by_period[k]
        for t in rows:
            if t["strategy"] in top_set:
                c = cells[t["strategy"]]
                c[k] = round(c.get(k, 0.0) + float(t["pnl"]), 2)
        farm_pnl = sum(float(t["pnl"]) for t in rows)
        active = len({t["strategy"] for t in rows})

        fwd = fwd_names = None
        if i > 0:
            fwd_names = [name for name, _ in rank_top(seen, n, min_trades, cap)]
            pick = set(fwd_names)
            fwd = round(sum(float(t["pnl"]) for t in rows if t["strategy"] in pick), 2)

        periods.append({
            "key": k,
            "top_pnl": round(sum(float(t["pnl"]) for t in rows if t["strategy"] in top_set), 2),
            "forward_pnl": fwd,
            "forward_picks": len(fwd_names) if fwd_names is not None else None,
            "farm_pnl": round(farm_pnl, 2),
            "strategies": active,
            "avg_n_pnl": round(farm_pnl / active * n, 2) if active else 0.0,
        })
        seen.extend(rows)

    fwd_vals = [p["forward_pnl"] for p in periods if p["forward_pnl"] is not None]
    base_vals = [p["avg_n_pnl"] for p in periods[1:]]
    return {
        "periods": periods,
        "top": [{"strategy": name, **s, "by_period": cells[name]} for name, s in top],
        "totals": {
            "farm_pnl": round(sum(float(t["pnl"]) for t in trips), 2),
            "farm_trades": len(trips),
            "farm_strategies": len({t["strategy"] for t in trips}),
            "top_pnl": round(sum(s["pnl"] for _, s in top), 2),
            "forward_pnl": round(sum(fwd_vals), 2),
            "forward_periods": len(fwd_vals),
            "forward_beat": sum(1 for p in periods[1:] if p["forward_pnl"] is not None
                                and p["forward_pnl"] > p["avg_n_pnl"]),
            "baseline_pnl": round(sum(base_vals), 2),
        },
    }


def _priority(r):
    """Same-ticker priority: profit factor first (None = no losing trades = best),
    then win rate, then net P&L, then name so the order is deterministic."""
    pf = r.get("profit_factor")
    return (float("inf") if pf is None else pf, r.get("win_rate") or 0.0, r["pnl"])


def merge_roster_detail(tops):
    """Combine each farm's top list into one Select roster: one rule per STRATEGY.

    tops: {"tv": [{strategy, pnl, profit_factor, win_rate, ...}], "kairos": [...]}.

    A strategy on both farms' lists gets one rule, from the farm whose version has
    the higher profit factor (win rate, then net P&L, break ties) — two rules would
    double its position. Different strategies on the same ticker (R3S3 and R4S4)
    are both kept, by choice (2026-10-19). They share account 7's one position per
    symbol: a second TV entry is skipped by the webhook's position gate, but engine
    entries aren't gated, so two engine-entered strategies on one ticker can stack
    and either one's exit closes both.

    Returns (roster, dropped). dropped rows name the version that beat them."""
    cands = [{"strategy": r["strategy"], "farm": farm, "entry": ENTRY_BY_FARM[farm],
              "pnl": r["pnl"], "profit_factor": r.get("profit_factor"),
              "win_rate": r.get("win_rate")}
             for farm, rows in tops.items() for r in rows]
    on_lists = {}
    for c in cands:
        on_lists.setdefault(c["strategy"], set()).add(c["farm"])
    best, dropped = {}, []
    for c in sorted(cands, key=lambda c: (_priority(c), c["strategy"]), reverse=True):
        w = best.get(c["strategy"])
        if w is None:
            best[c["strategy"]] = c
        else:
            dropped.append({**c, "lost_to": w["strategy"], "lost_to_farm": w["farm"],
                            "lost_to_pf": w["profit_factor"], "lost_to_wr": w["win_rate"]})
    roster = []
    for c in best.values():
        other = on_lists[c["strategy"]] - {c["farm"]}
        roster.append({**c, "also_in": next(iter(other), None)})
    roster.sort(key=lambda p: (-p["pnl"], p["strategy"]))
    dropped.sort(key=lambda p: (p["strategy"].split("_", 1)[0], -p["pnl"]))
    return roster, dropped


def merge_roster(tops):
    return merge_roster_detail(tops)[0]


def accumulation(trips_by_farm, n, period, min_trades=DEFAULT_MIN_TRADES, cap=DEFAULT_CAP):
    """Running P&L by day for each farm's top N and for the combined Select roster.

    Two modes, same shape:
      top     — today's top N (and today's merged roster) traded over the whole
                window. Hindsight: the curve was picked for being good.
      forward — at each period boundary each farm's top N is re-ranked on earlier
                periods only, the two lists merged the way Apply would, and that
                roster trades the period. None before the second period.

    The combined line counts a strategy from ONE farm only — the one its rule
    would enter on — so a name on both lists is never double-counted.

    Returns {dates, top: {tv, kairos, combined}, forward: {...}, forward_start}."""
    farms = list(trips_by_farm)
    for trips in trips_by_farm.values():
        for t in trips:
            t["period"] = period_key(t["date"], period)
    dates = sorted({t["date"] for ts in trips_by_farm.values() for t in ts if t["date"]})
    keys = sorted({t["period"] for ts in trips_by_farm.values() for t in ts if t["period"]})

    def _tops(trips):
        return [{"strategy": nm, "pnl": s["pnl"], "profit_factor": s["profit_factor"],
                 "win_rate": s["win_rate"]} for nm, s in rank_top(trips, n, min_trades, cap)]

    hind = {f: _tops(trips_by_farm[f]) for f in farms}
    hind_sets = {f: {r["strategy"] for r in hind[f]} for f in farms}
    hind_comb = {r["strategy"]: r["farm"] for r in merge_roster(hind)}

    fwd_sets, fwd_comb = {}, {}          # period → {farm: set} / {strategy: farm}
    for k in keys[1:]:
        tops = {f: _tops([t for t in trips_by_farm[f] if t["period"] < k]) for f in farms}
        fwd_sets[k] = {f: {r["strategy"] for r in tops[f]} for f in farms}
        fwd_comb[k] = {r["strategy"]: r["farm"] for r in merge_roster(tops)}

    daily = {m: {s: {} for s in farms + ["combined"]} for m in ("top", "forward")}

    def _add(mode, series, d, pnl):
        b = daily[mode][series]
        b[d] = b.get(d, 0.0) + pnl

    for f in farms:
        for t in trips_by_farm[f]:
            s, d, p = t["strategy"], t["date"], float(t["pnl"])
            if s in hind_sets[f]:
                _add("top", f, d, p)
            if hind_comb.get(s) == f:
                _add("top", "combined", d, p)
            k = t["period"]
            if k in fwd_sets:
                if s in fwd_sets[k][f]:
                    _add("forward", f, d, p)
                if fwd_comb[k].get(s) == f:
                    _add("forward", "combined", d, p)

    # One running total per top strategy, every line starting at $0 on the first
    # date so they compare on the same axis. A strategy on both farms' lists gets
    # two lines: the TV-alert and engine versions are different executions.
    per_strategy = []
    for f in farms:
        for rank, r in enumerate(hind[f], 1):
            s = r["strategy"]
            by_day = {}
            for t in trips_by_farm[f]:
                if t["strategy"] == s:
                    by_day[t["date"]] = by_day.get(t["date"], 0.0) + float(t["pnl"])
            run, line = 0.0, []
            for d in dates:
                run += by_day.get(d, 0.0)
                line.append(round(run, 2))
            per_strategy.append({"strategy": s, "farm": f, "rank": rank,
                                 "pnl": r["pnl"], "values": line})

    start = keys[1] if len(keys) > 1 else None
    out = {"dates": dates, "forward_start": start, "strategies": per_strategy}
    for mode in ("top", "forward"):
        out[mode] = {}
        for series, by_day in daily[mode].items():
            run, line = 0.0, []
            for d in dates:
                if mode == "forward" and (start is None or period_key(d, period) < start):
                    line.append(None)
                    continue
                run += by_day.get(d, 0.0)
                line.append(round(run, 2))
            out[mode][series] = line
    return out


# Exit settings the Farm Leaders page may write onto Kairos Select rules, mapped
# from the replay's names to exit_params keys.
EXIT_OVERRIDE_KEYS = {"max_hold": "max_hold_mins", "trail": "trail_offset",
                      "trigger": "trail_trigger", "tp": "take_profit_pct"}


def apply_exit_overrides(nodes, overrides):
    """Write roster-wide exit overrides onto one rule's exit_params.

    overrides: {max_hold_mins?, trail_offset?, trail_trigger?, take_profit_pct?}.
    Trail and trigger values are % of price (what the replay tested), so they're
    only written onto rules whose exits are in percent mode; a dollar-mode rule
    keeps its trail and the reason is returned.

    Returns (new_nodes, skipped_reason | None). Never mutates `nodes`."""
    out = copy.deepcopy(nodes or [])
    ep = next((n for n in out if n.get("type") == "exit_params"), None)
    if ep is None:
        ep = {"type": "exit_params", "stop_loss": None, "trail_trigger": None,
              "trail_offset": DEFAULT_TRAIL, "mode": "percent"}
        out.append(ep)
    skipped = None
    trail_keys = {"trail_offset", "trail_trigger"} & set(overrides)
    if trail_keys and (ep.get("mode") or "percent") != "percent":
        skipped = "exits are in dollars, so the % trail/trigger wasn't applied"
    for k, v in overrides.items():
        if k in trail_keys and skipped:
            continue
        ep[k] = v
    return out, skipped


def select_nodes(slug, shares, entry, source_nodes=None):
    """Routing nodes for one Select pick. Clones the strategy's own pipeline (tuned
    exits, hours, instrument, side_gate) when there is one, then pins the Select
    broker, the share count and the entry source. The side_gate is KEPT: the farms
    honour the pipeline's side gate (the webhook skips the gated rule, and the
    engine applies it uniformly to every source), so the record being bought here
    was earned under it."""
    if not source_nodes:
        return [
            {"type": "strategy",     "value": slug},
            {"type": "quantity",     "amount": shares, "unit": "shares"},
            {"type": "instrument",   "value": "STK"},
            {"type": "broker",       "value": SELECT_BROKERS[0]},
            {"type": "entry_source", "value": entry},
            {"type": "exit_params",  "stop_loss": None, "trail_trigger": None,
             "trail_offset": 0.15, "mode": "percent"},
        ]
    out, have = [], set()
    for nd in copy.deepcopy(source_nodes):
        t = nd.get("type")
        if t in ("broker", "quantity", "entry_source"):
            if t in have:
                continue
            have.add(t)
            nd = ({"type": "broker", "value": SELECT_BROKERS[0]} if t == "broker" else
                  {"type": "quantity", "amount": shares, "unit": "shares"} if t == "quantity" else
                  {"type": "entry_source", "value": entry})
        out.append(nd)
    for t, nd in (("quantity", {"type": "quantity", "amount": shares, "unit": "shares"}),
                  ("broker", {"type": "broker", "value": SELECT_BROKERS[0]}),
                  ("entry_source", {"type": "entry_source", "value": entry})):
        if t not in have:
            out.append(nd)
    return out


# ── Data access ───────────────────────────────────────────────────────────────

def _app():
    import app as _kairos
    return _kairos


FULL_HISTORY_DAYS = 730          # "All": far enough back to cover each farm's start
_FULL_TTL = 15 * 60              # a full pagination is slow; hold it for 15 minutes
_full_cache = {}                 # num → (fetched_at, fills)
_full_lock = threading.Lock()


def _full_fills(rec, num):
    """Every fill since the farm began. The shared fills cache stops at 90 days
    (FILLS_LOOKBACK_DAYS) to keep cold starts fast, so "All" pays for its own longer
    walk, cached here so it isn't repeated on every toggle. Raises on failure — a
    partial history must not pass for the whole one."""
    with _full_lock:
        hit = _full_cache.get(num)
        if hit and time.time() - hit[0] < _FULL_TTL:
            return hit[1]
        fills = rec["broker"].get_fills(days=FULL_HISTORY_DAYS, raise_on_error=True)
        _full_cache[num] = (time.time(), fills)
        return fills


def _farm_round_trips(num, from_date, to_date, full=False):
    """Full closed round-trips on one farm (prices, qty, times), strategy names
    upper-cased, honouring the same strategy/ticker/band exclusions as the Refined
    leaderboards. Returns (round_trips, error) — a failed fetch must not render as
    an empty farm."""
    a = _app()
    rec = a.ACCOUNTS_BY_NUM.get(num)
    if not rec:
        return [], "not configured"
    try:
        fills = _full_fills(rec, num) if full else rec["fills_fn"]()
    except Exception as e:
        return [], f"fills unavailable: {e}"
    if not fills:
        return [], ("fills unavailable" if not full and a._fills_error(num) else None)
    paired = a._pair_alpaca_fills_lifo(fills, from_date=from_date, to_date=to_date)
    ex_s = a._load_excluded_strategies()
    ex_t = a._load_excluded_tickers()
    ex_b = a._load_excluded_bands()
    out = []
    for c in paired.get("closed_clean") or []:
        s = (c.get("strategy") or "").strip().upper()
        if not s or s == "UNKNOWN" or s in ex_s:
            continue
        if s.split("_", 1)[0] in ex_t or (ex_b and a._band_of(s) in ex_b):
            continue
        out.append({**c, "strategy": s})
    return out, None


def _farm_trips(num, from_date, to_date, full=False):
    """Closed round-trips on one farm as [{strategy, pnl, date}] (ET exit date)."""
    rts, err = _farm_round_trips(num, from_date, to_date, full)
    return [{"strategy": c["strategy"], "pnl": float(c.get("pnl") or 0),
             "date": et_date(c.get("exit_time")) or c.get("date") or ""} for c in rts], err


def _params():
    src = request.get_json(silent=True) if request.method == "POST" else None
    src = src or request.args

    def _int(k, d, lo, hi):
        try:
            return min(hi, max(lo, int(src.get(k, d))))
        except (TypeError, ValueError):
            return d
    days = _int("days", 90, 0, 365)          # 0 = since each farm started
    if 0 < days < 7:
        days = 7
    today = datetime.now(_ET).date()
    return {
        "n": _int("n", 10, 1, 50),
        "days": days,
        "full": days == 0,
        "from_date": "" if days == 0 else (today - timedelta(days=days)).isoformat(),
        "to_date": today.isoformat(),
        "period": "month" if (src.get("period") or "") == "month" else "week",
        "min_trades": _int("min_trades", DEFAULT_MIN_TRADES, 1, 100),
        "cap": _int("cap", DEFAULT_CAP, 0, 50),
    }


def _build(p):
    farms, errors, trips_by_farm = {}, {}, {}
    for key, num, label in FARMS:
        trips, err = _farm_trips(num, p["from_date"], p["to_date"], full=p["full"])
        if err:
            errors[key] = f"{label}: {err}"
        trips_by_farm[key] = trips
        view = farm_view(trips, p["n"], p["period"], p["min_trades"], p["cap"])
        view["label"] = label
        view["num"] = num
        view["first_date"] = min((t["date"] for t in trips), default=None)
        farms[key] = view
    roster = merge_roster({k: v["top"] for k, v in farms.items()})
    farms_acc = accumulation(trips_by_farm, p["n"], p["period"], p["min_trades"], p["cap"])
    return farms, roster, errors, farms_acc


def _select_rules(cur, a):
    """Inventory routing rules: Select-only rules (prunable), mixed rules carrying a
    Select broker beside others (only that broker node is stripped), and each
    strategy's best non-Select pipeline to clone exits from."""
    cur.execute("SELECT id, name, nodes FROM routing_rules")
    own, mixed, source = {}, [], {}
    for r in cur.fetchall():
        rid = r[0] if a.DATABASE_URL else r["id"]
        raw = r[2] if a.DATABASE_URL else r["nodes"]
        try:
            nodes = json.loads(raw) if isinstance(raw, str) else (raw or [])
        except Exception:
            nodes = []
        brokers = [(n.get("value") or "").lower() for n in nodes if n.get("type") == "broker"]
        strats = [(n.get("value") or "").strip().upper()
                  for n in nodes if n.get("type") == "strategy" and n.get("value")]
        sel = [b for b in brokers if b in SELECT_BROKERS]
        if sel and len(sel) == len(brokers):
            for s in strats:
                own[s] = rid
            continue
        if sel:
            mixed.append((rid, nodes, strats))
        # Crew / Crew Live rules are derived copies; prefer the farm's own pipeline.
        derived = any(b.endswith(("-4", "-6")) for b in brokers)
        prio = 1 if derived else 2
        for s in strats:
            if s not in source or source[s][0] < prio:
                source[s] = (prio, [n for n in nodes])
    return own, mixed, source


# Account-level entry gates a Select pick must share with the farm it was measured
# on. Rule-level gates (hours window, side gate, exits) travel on the cloned rule.
PARITY_GATES = ("hours", "day-type", "rvol", "open-location", "reversal",
                "strikes", "profit-lock", "daily-loss")
# Gates Select is allowed to differ on, by deliberate choice — shown, never blocking
# Apply. Both are day-level risk stops rather than setup filters: the profit lock
# caps give-back once the book is up a step, the daily-loss guard halts and
# liquidates at the day's loss limit (2026-10-20).
PARITY_EXEMPT = ("profit-lock", "daily-loss")
FARM_TAGS = {"tv": "alpaca", "kairos": "alpaca5"}


def gate_parity(state_fn):
    """Compare Kairos Select's EFFECTIVE gates (shared config + per-account
    overrides) against both farms. state_fn(tag) → {gate: (on, setting, source)}.

    Returns {rows: [{gate, tv, kairos, select, ok}], mismatches: [gate], farms_agree}.
    A gate is a mismatch when Select differs from EITHER farm: a TV pick was
    measured under TV Farm's gates and an engine pick under Kairos Farm's, and
    Select runs both kinds through one account."""
    st = {k: state_fn(tag) for k, tag in {**FARM_TAGS, "select": SELECT_TAG}.items()}

    def _cell(k, g):
        on, setting, src = st[k].get(g, (False, "—", "shared"))
        return {"on": bool(on), "setting": setting, "source": src}

    rows, bad, disagree = [], [], False
    for g in PARITY_GATES:
        c = {k: _cell(k, g) for k in st}
        same = lambda x, y: (x["on"], x["setting"]) == (y["on"], y["setting"])
        exempt = g in PARITY_EXEMPT
        ok = exempt or (same(c["select"], c["tv"]) and same(c["select"], c["kairos"]))
        disagree = disagree or not same(c["tv"], c["kairos"])
        rows.append({"gate": g, **c, "ok": ok, "exempt": exempt})
        if not ok:
            bad.append(g)
    return {"rows": rows, "mismatches": bad, "farms_agree": not disagree}


def _parity_or_error(a):
    try:
        return gate_parity(a._gate_state)
    except Exception as e:
        return {"rows": [], "mismatches": [], "farms_agree": False,
                "error": f"could not read gate settings: {e}"}


class _PositionsUnreadable(Exception):
    pass


def _prune_select(cur, a, own, mixed, keep=frozenset()):
    """Take Kairos Select off every strategy not in `keep`.

    Select-only rules are deleted; a shared rule just loses its Select broker node.
    A rule whose ticker has an OPEN Select position is left in place so the trade
    keeps its tuned exit — the next apply or clear removes it. Raises
    _PositionsUnreadable (before writing anything) if positions can't be read,
    because pruning blind could strand an open trade without its exits.

    Returns (deleted, deferred, stripped)."""
    by_rid = {}
    for s, rid in own.items():
        by_rid.setdefault(rid, set()).add(s)
    prune = {rid: ss for rid, ss in by_rid.items() if not (ss & keep)}
    open_tk = set()
    if prune:
        try:
            br = a.ACCOUNTS_BY_TAG[SELECT_TAG]["broker"]
            br._invalidate_pos_cache()
            open_tk = {(x.get("symbol") or "").upper() for x in br.get_positions(raise_on_error=True)}
        except Exception as e:
            raise _PositionsUnreadable(
                f"Could not read Kairos Select positions ({e}), so an open trade might "
                "lose its exits. Nothing was changed.") from e
    ph = a.placeholder()
    stripped = []
    for rid, nodes, strats in mixed:
        rest = [n for n in nodes if not (n.get("type") == "broker"
                                         and (n.get("value") or "").lower() in SELECT_BROKERS)]
        cur.execute(f"UPDATE routing_rules SET nodes={ph} WHERE id={ph}", (json.dumps(rest), rid))
        stripped.extend(strats)
    deleted, deferred = [], []
    for rid, ss in prune.items():
        if any(s.split("_", 1)[0] in open_tk for s in ss):
            deferred.extend(sorted(ss))
            continue
        cur.execute(f"DELETE FROM routing_rules WHERE id={ph}", (rid,))
        deleted.extend(sorted(ss))
    return deleted, deferred, sorted(set(stripped))


# ── Roster replay ─────────────────────────────────────────────────────────────
# Re-run the roster's real farm round-trips on 1-minute bars with different exit
# settings. Each test varies ONE thing against a baseline of the live exits, and
# both are simulated the same way, so the delta is the setting and not the model.
#
# The roster was picked on these same trades, so tuning exits on them fits twice.
# Trades are split at the median date: pick the best setting on the older half,
# then see whether it still beats the baseline on the newer half. That guards the
# EXIT choice; it can't undo the hindsight in which strategies made the roster.

REPLAY_TESTS = ("max_hold", "trail", "take_profit")
REPLAY_MAX_TICKER_DAYS = 2000      # bar fetches per run; past this, shorten the window
DEFAULT_TRAIL = 0.15               # same fallback the Replay sweep uses


def build_variants(test, body):
    """The settings one test tries. Each is {label, max_hold?, trail?, trigger?, tp?};
    keys left out keep the trade's own live value."""
    def _nums(key, default):
        raw = body.get(key)
        vals = raw if isinstance(raw, list) else str(raw if raw is not None else default).split(",")
        out = []
        for v in vals:
            try:
                out.append(round(float(v), 4))
            except (TypeError, ValueError):
                continue
        return sorted(set(out)) or [float(x) for x in str(default).split(",")]

    if test == "max_hold":
        return [{"label": "no max hold" if m == 0 else f"{m:g} min", "max_hold": m}
                for m in _nums("max_holds", "5,10,15,20,30,45,60,0")]
    if test == "take_profit":
        return [{"label": f"TP {t:g}%", "tp": t} for t in _nums("tps", "0.3,0.5,0.75,1,1.5,2") if t > 0]
    return [{"label": f"trail {tr:g}% · trigger {tg:g}%", "trail": tr, "trigger": tg}
            for tr in _nums("trails", "0.1,0.15,0.2,0.25,0.3,0.4,0.5")
            for tg in _nums("triggers", "0,0.1,0.2,0.3")]


def split_date(prepared):
    """Median entry date: older half tunes, newer half checks. None when every
    trade is on one day (nothing to split)."""
    ds = sorted(td["date"] for td in prepared)
    if not ds or ds[0] == ds[-1]:
        return None
    mid = ds[len(ds) // 2]
    return mid if mid > ds[0] else ds[-1]


def score_variants(prepared, variants, sim):
    """Score the baseline and each variant on all trades and on each half.

    prepared: [{date, notional, base: {trail, trigger, max_hold}, ...}]
    sim(td, trail, trigger, max_hold, tp) -> {"pnl", "reason"} or None (no bars).
    Returns {baseline, variants, split_date, best, verdict}."""
    sd = split_date(prepared)

    def _run(v):
        tot = {"all": [], "tune": [], "check": []}
        reasons = {}
        pct = []
        for td in prepared:
            b = td["base"]
            r = sim(td, v.get("trail", b["trail"]), v.get("trigger", b["trigger"]),
                    v.get("max_hold", b["max_hold"]), v.get("tp", 0.0))
            if r is None:
                continue
            tot["all"].append(r["pnl"])
            if sd:
                tot["tune" if td["date"] < sd else "check"].append(r["pnl"])
            reasons[r["reason"]] = reasons.get(r["reason"], 0) + 1
            if td["notional"]:
                pct.append(r["pnl"] / td["notional"] * 100)
        n = len(tot["all"])
        return {
            "label": v["label"], "trades": n,
            "pnl": round(sum(tot["all"]), 2),
            "tune_pnl": round(sum(tot["tune"]), 2) if sd else None,
            "check_pnl": round(sum(tot["check"]), 2) if sd else None,
            "tune_trades": len(tot["tune"]), "check_trades": len(tot["check"]),
            "win_rate": round(sum(1 for p in tot["all"] if p > 0) / n * 100, 1) if n else None,
            "avg_pct": round(sum(pct) / len(pct), 4) if pct else None,
            "exits": reasons,
        }

    base = _run({"label": "live exits (baseline)"})
    rows = []
    for v in variants:
        r = _run(v)
        r.update({k: v[k] for k in ("max_hold", "trail", "trigger", "tp") if k in v})
        r["delta"] = round(r["pnl"] - base["pnl"], 2)
        if sd:
            r["tune_delta"] = round(r["tune_pnl"] - base["tune_pnl"], 2)
            r["check_delta"] = round(r["check_pnl"] - base["check_pnl"], 2)
        rows.append(r)

    best = verdict = None
    if sd and rows:
        best = max(rows, key=lambda r: r["tune_delta"])
        if best["tune_delta"] <= 0:
            verdict = "none"         # nothing beat live exits even on the tuning half
        elif best["check_delta"] > 0:
            verdict = "holds"        # better on the half it was picked on AND the other
        else:
            verdict = "fails"        # only better on the half it was fitted to
    rows.sort(key=lambda r: r["delta"], reverse=True)
    return {"baseline": base, "variants": rows, "split_date": sd,
            "best": best["label"] if best else None, "verdict": verdict}


TEST_LABELS = {"max_hold": "Max hold", "trail": "Trail × trigger", "take_profit": "Take profit"}


def _combo_label(v):
    parts = []
    if "max_hold" in v:
        parts.append("no max hold" if v["max_hold"] == 0 else f"max hold {v['max_hold']:g} min")
    if "trail" in v:
        parts.append(f"trail {v['trail']:g}% · trigger {v['trigger']:g}%")
    if "tp" in v:
        parts.append(f"TP {v['tp']:g}%")
    return " + ".join(parts)


def suggest_exits(prepared, sim, body):
    """Run all three exit tests and turn their winners into one suggestion.

    1. Each test picks its best setting on the OLDER half (score_variants).
    2. Only winners that ALSO beat live exits on the newer half are kept; a test
       whose winner didn't hold (or found nothing) stays at the live setting.
    3. The kept winners are replayed TOGETHER, because exits interact (a take
       profit matters less once the trail is tighter; a longer max hold changes
       what the trail sees), and the combination must hold on the newer half too.

    The suggestion is one roster-wide value per setting, replacing the per-rule
    values the baseline uses.

    Returns {tests: {test: {...}}, combined: {...} | None, recommendation, split_date}."""
    tests, combo = {}, {}
    sd = None
    for test in REPLAY_TESTS:
        r = score_variants(prepared, build_variants(test, body), sim)
        sd = r["split_date"]
        best = next((v for v in r["variants"] if v["label"] == r["best"]), None)
        keep = r["verdict"] == "holds"
        tests[test] = {
            "name": TEST_LABELS[test], "verdict": r["verdict"], "kept": keep,
            "best": None if best is None else {
                k: best.get(k) for k in ("label", "pnl", "delta", "tune_delta", "check_delta",
                                         "avg_pct", "win_rate", "max_hold", "trail", "trigger", "tp")
                if k in best},
        }
        if keep:
            combo.update({k: best[k] for k in ("max_hold", "trail", "trigger", "tp") if k in best})

    if sd is None:
        return {"tests": tests, "combined": None, "split_date": None,
                "recommendation": "no_split",
                "summary": "Every roster trade is on one day, so there's no older/newer half to check a "
                           "setting against. Use a longer window."}
    if not combo:
        return {"tests": tests, "combined": None, "split_date": sd,
                "recommendation": "keep",
                "summary": "No exit change held up on the newer half. Keep your live exits."}

    combo["label"] = _combo_label(combo)
    c = score_variants(prepared, [combo], sim)
    row = c["variants"][0]
    combined = {k: row.get(k) for k in ("label", "pnl", "delta", "tune_delta", "check_delta",
                                        "avg_pct", "win_rate", "exits", "max_hold", "trail", "trigger", "tp")
                if k in row}
    combined["baseline"] = {k: c["baseline"][k] for k in ("pnl", "avg_pct", "win_rate", "exits")}
    kept = [t for t in REPLAY_TESTS if tests[t]["kept"]]
    usd = lambda v: ("+" if v >= 0 else "−") + f"${abs(v):,.2f}"
    if row["tune_delta"] > 0 and row["check_delta"] > 0:
        rec = "change"
        summary = (f"Switch to {combo['label']}. "
                   + ("Replayed together, they" if len(kept) > 1 else "It")
                   + f" beat live exits on both halves (older {usd(row['tune_delta'])}, "
                     f"newer {usd(row['check_delta'])}).")
    elif len(kept) > 1:
        # The settings don't stack. Fall back to the single kept winner that did best
        # on the newer half — it already held on its own.
        lone = max(kept, key=lambda t: tests[t]["best"]["check_delta"])
        rec = "partial"
        summary = (f"The winners don't stack — together they don't hold on the newer half. "
                   f"The strongest single change is {tests[lone]['best']['label']} "
                   f"({TEST_LABELS[lone].lower()}); change only that.")
        combined["fallback"] = lone
    else:
        rec = "keep"
        summary = "The change looked better alone but not once replayed in full. Keep your live exits."
    return {"tests": tests, "combined": combined, "split_date": sd,
            "recommendation": rec, "summary": summary}


class Bar:
    """A 1-minute bar with only what the exit simulator reads. The SDK's bar
    objects carry a dozen fields plus model overhead — kept in a cache across a
    roster's worth of ticker-days they ran to hundreds of MB per worker."""
    __slots__ = ("timestamp", "open", "high", "low", "close")

    def __init__(self, timestamp, open, high, low, close):
        self.timestamp, self.open, self.high, self.low, self.close = timestamp, open, high, low, close


def compact_bars(bars):
    return [Bar(b.timestamp, float(b.open), float(b.high), float(b.low), float(b.close))
            for b in bars or []]


# (ticker, date) -> compact 1-min bars, past days only, least-recently-used out
# first. A replay touches up to REPLAY_MAX_TICKER_DAYS days; 800 covers a typical
# roster re-run while capping the cache at ~50-60 MB per gunicorn worker.
BARS_CACHE_MAX = 800
_bars_cache = OrderedDict()
_bars_lock = threading.Lock()


def _day_bars(a, ticker, day):
    """1-minute bars for one ticker-day, cached once the day is over (they never
    change after the close, and a sweep re-reads the same days every run)."""
    key = (ticker, day)
    with _bars_lock:
        if key in _bars_cache:
            _bars_cache.move_to_end(key)
            return _bars_cache[key]
    bars = compact_bars(a._fetch_day_bars(ticker, day))
    if bars and day < datetime.now(_ET).date().isoformat():
        with _bars_lock:
            _bars_cache[key] = bars
            _bars_cache.move_to_end(key)
            while len(_bars_cache) > BARS_CACHE_MAX:
                _bars_cache.popitem(last=False)
    return bars


def _exit_settings(a):
    """{STRATEGY: {trail, trigger, max_hold}} from the enabled routing rules —
    preferring the Select rule (what account 7 trades), else the farm pipeline."""
    out = {}
    conn = a.get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT nodes FROM routing_rules WHERE enabled=1")
        rows = cur.fetchall()
    finally:
        conn.close()
    for row in rows:
        raw = row[0] if a.DATABASE_URL else row["nodes"]
        try:
            nodes = json.loads(raw) if isinstance(raw, str) else (raw or [])
        except Exception:
            continue
        ep = next((n for n in nodes if n.get("type") == "exit_params"), None)
        if not ep:
            continue
        sel = any((n.get("value") or "").lower() in SELECT_BROKERS
                  for n in nodes if n.get("type") == "broker")
        try:
            trail = float(ep.get("trail_offset") or 0) or DEFAULT_TRAIL
            trigger = float(ep.get("trail_trigger") or 0)
            mh = int(float(ep["max_hold_mins"])) if ep.get("max_hold_mins") else None
        except (TypeError, ValueError):
            continue
        for n in nodes:
            if n.get("type") == "strategy" and n.get("value") and "*" not in n["value"]:
                s = n["value"].strip().upper()
                if sel or s not in out:
                    out[s] = {"trail": trail, "trigger": trigger, "max_hold": mh}
    return out


# ── Routes ────────────────────────────────────────────────────────────────────

@farms_bp.route("/farms")
def farms_page():
    return render_template("farms.html")


@farms_bp.route("/api/farms/leaders")
def api_farm_leaders():
    """?n=10|20 &days=30|60|90 &period=week|month &min_trades=5 &cap=2

    Both farms ranked, bucketed by period, plus the combined Select roster and its
    diff against what account 7 runs now."""
    a = _app()
    p = _params()
    farms, roster, errors, acc = _build(p)

    current = []
    try:
        conn = a.get_db(); cur = conn.cursor()
        own, mixed, _src = _select_rules(cur, a)
        conn.close()
        current = sorted(set(own) | {s for _, _, ss in mixed for s in ss})
    except Exception as e:
        errors["router"] = f"could not read routing rules: {e}"
    new = {r["strategy"] for r in roster}
    return jsonify({
        **p,
        "farms": farms,
        "accumulation": acc,
        "roster": roster,
        # The other farm's version of a strategy on both lists (one rule per strategy).
        "roster_dropped": merge_roster_detail({k: v["top"] for k, v in farms.items()})[1],
        "diff": {"add": sorted(new - set(current)),
                 "keep": sorted(new & set(current)),
                 "remove": sorted(set(current) - new)},
        "select_configured": SELECT_TAG in a.ACCOUNTS_BY_TAG,
        "gate_parity": _parity_or_error(a),
        "last_apply": json.loads(a._load_setting("SELECT_LAST_APPLY") or "null"),
        "exit_overrides": _load_exit_overrides(a),
        "exits_undo": json.loads(a._load_setting("SELECT_EXITS_UNDO") or "null"),
        "backstop_max_hold": (a.MAX_HOLD_MINS if a.MAX_HOLD_MINS > 0 else None),
        "lookback_cap_days": FULL_HISTORY_DAYS if p["full"] else 90,
        "errors": errors,
    })


@farms_bp.route("/api/farms/apply_select", methods=["POST"])
def api_farm_apply_select():
    """Rewire Kairos Select to the combined roster for the posted parameters.

    Upserts a "{strategy} · Select" rule per pick, deletes Select-only rules whose
    strategy left the roster (deferred while that ticker has an open Select
    position), and strips the Select broker from any mixed rule that carried it.
    The roster is recomputed server-side from the same parameters the page showed,
    and must match the `expect` list the page sends — if the fills moved between
    preview and click, nothing is written."""
    a = _app()
    if SELECT_TAG not in a.ACCOUNTS_BY_TAG:
        return jsonify({"error": "Kairos Select (account 7) isn't configured — set "
                                 "ALPACA_KEY7. Nothing was changed."}), 400
    parity = _parity_or_error(a)
    if parity.get("error") or parity["mismatches"]:
        return jsonify({"error": "Kairos Select's gates don't match the farms ("
                                 + (parity.get("error") or ", ".join(parity["mismatches"]))
                                 + "). A pick would trade under different rules than the ones "
                                 "it was measured under. Fix the gate overrides on the Routing "
                                 "page first. Nothing was changed.",
                        "gate_parity": parity}), 409
    data = request.get_json(silent=True) or {}
    p = _params()
    try:
        size = max(100.0, float(data.get("size_dollars") or DEFAULT_SIZE))
    except (TypeError, ValueError):
        size = float(DEFAULT_SIZE)

    farms, roster, errors, _acc = _build(p)
    if errors:
        return jsonify({"error": "Farm data is incomplete (" + "; ".join(errors.values())
                                 + "). Nothing was changed.", "errors": errors}), 409
    if not roster:
        return jsonify({"error": "No strategy qualifies with these settings. "
                                 "Nothing was changed."}), 400
    expect = data.get("expect")
    if expect is not None and sorted(expect) != sorted(r["strategy"] for r in roster):
        return jsonify({"error": "The leaderboard changed since the page loaded. "
                                 "Reload and review it again. Nothing was changed.",
                        "roster": roster}), 409

    prices = a._fetch_alpaca_last_prices([r["strategy"].split("_", 1)[0] for r in roster]) or {}
    unpriced = [r["strategy"] for r in roster if not prices.get(r["strategy"].split("_", 1)[0])]
    if unpriced:
        # Never guess a size: a $15k pick at a fallback share count is a different bet.
        return jsonify({"error": "No live price for " + ", ".join(unpriced)
                                 + " — can't size them. Nothing was changed.",
                        "unpriced": unpriced}), 409

    conn = a.get_db(); cur = conn.cursor(); ph = a.placeholder()
    own, mixed, source = _select_rules(cur, a)
    exit_ovr = _load_exit_overrides(a)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    created, updated, sizes = [], [], {}
    for r in roster:
        slug = r["strategy"]
        shares = max(1, round(size / prices[slug.split("_", 1)[0]]))
        sizes[slug] = shares
        built = select_nodes(slug, shares, r["entry"], (source.get(slug) or (0, None))[1])
        # Re-cloning from the farm pipeline would wipe exit changes made for Select
        # on this page, so the saved overrides are re-applied on every rewire.
        if exit_ovr:
            built, _skip = apply_exit_overrides(built, exit_ovr)
        nodes = json.dumps(built)
        rid = own.get(slug)
        if rid is not None:
            cur.execute(f"UPDATE routing_rules SET nodes={ph}, enabled=1 WHERE id={ph}", (nodes, rid))
            updated.append(slug)
        else:
            cur.execute(f"INSERT INTO routing_rules (name,enabled,nodes,created_at,tv_alert_created) "
                        f"VALUES ({ph},{ph},{ph},{ph},{ph})",
                        (slug + SELECT_SUFFIX, 1, nodes, ts, 0))
            created.append(slug)

    try:
        deleted, deferred, stripped = _prune_select(
            cur, a, own, mixed, keep={r["strategy"] for r in roster})
    except _PositionsUnreadable as e:
        conn.rollback(); conn.close()
        return jsonify({"error": str(e)}), 409
    conn.commit(); conn.close()

    record = {"at": ts, "n": p["n"], "days": p["days"], "from_date": p["from_date"],
              "to_date": p["to_date"], "min_trades": p["min_trades"], "cap": p["cap"],
              "size_dollars": size,
              "picks": [{"strategy": r["strategy"], "farm": r["farm"], "entry": r["entry"],
                         "pnl": r["pnl"], "shares": sizes[r["strategy"]]} for r in roster]}
    try:
        a._save_setting("SELECT_LAST_APPLY", json.dumps(record))
        # Roster history for the month-by-month pack. Deferred names stay wired
        # (they hold an open position), so they are still on the roster.
        _log_select_roster(a, [r["strategy"] for r in roster] + list(deferred), ts)
    except Exception as e:
        a.log.warning("farms: could not persist the Select apply record: %s", e)
    a.log.info("Kairos Select rewired from Farm Leaders: %d picks (+%d ~%d -%d, %d deferred)",
               len(roster), len(created), len(updated), len(deleted), len(deferred))
    return jsonify({"created": created, "updated": updated, "deleted": deleted,
                    "deferred_open_position": deferred, "stripped_from_mixed": stripped,
                    "record": record})


@farms_bp.route("/api/farms/clear_select", methods=["POST"])
def api_farm_clear_select():
    """Take Kairos Select off every strategy: delete its own rules and strip its
    broker from shared ones. Rules for tickers with an open Select position stay
    until that position is closed (clear again then)."""
    a = _app()
    if SELECT_TAG not in a.ACCOUNTS_BY_TAG:
        return jsonify({"error": "Kairos Select (account 7) isn't configured. Nothing was changed."}), 400
    conn = a.get_db(); cur = conn.cursor()
    own, mixed, _src = _select_rules(cur, a)
    try:
        deleted, deferred, stripped = _prune_select(cur, a, own, mixed)
    except _PositionsUnreadable as e:
        conn.rollback(); conn.close()
        return jsonify({"error": str(e)}), 409
    conn.commit(); conn.close()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        a._save_setting("SELECT_LAST_APPLY", json.dumps(
            {"at": ts, "cleared": True, "picks": [], "deferred": deferred}))
        _log_select_roster(a, list(deferred), ts)
    except Exception as e:
        a.log.warning("farms: could not persist the Select clear record: %s", e)
    a.log.info("Kairos Select cleared from Farm Leaders: -%d, %d deferred, %d shared rules stripped",
               len(deleted), len(deferred), len(stripped))
    return jsonify({"deleted": deleted, "deferred_open_position": deferred,
                    "stripped_from_mixed": stripped})


def _prepare_replay():
    """Shared setup for the replay endpoints: build the roster from the page's
    params, gather each pick's farm round-trips, fetch their 1-minute bars and
    attach each trade's live exits.

    Each roster pick replays the round-trips of the farm it comes from (TV picks:
    TV Farm, Kairos picks: Kairos Farm) at the farm's $1,000 sizing. Baseline =
    each trade's live exits (rule trail/trigger with the session trail adjustment,
    rule max hold else the global backstop), simulated the same way as every
    variant. TV exit signals are ignored on both sides of the comparison: the test
    is the broker-side exit settings alone.

    Returns (ctx, None) with ctx = {prepared, sim, meta}, or (None, (json, status))."""
    import concurrent.futures as _cf
    a = _app()
    p = _params()
    _farms, roster, errors, _acc = _build(p)
    if errors:
        return None, (jsonify({"error": "Farm data is incomplete (" + "; ".join(errors.values()) + ")."}), 409)
    if not roster:
        return None, (jsonify({"error": "No strategy qualifies with these settings."}), 400)

    picks = {(r["farm"], r["strategy"]) for r in roster}
    nums = {key: num for key, num, _label in FARMS}
    trades = []
    for farm in {f for f, _s in picks}:
        rts, err = _farm_round_trips(nums[farm], p["from_date"], p["to_date"], full=p["full"])
        if err:
            return None, (jsonify({"error": f"{farm} farm: {err}"}), 409)
        trades += [dict(c, farm=farm) for c in rts if (farm, c["strategy"]) in picks]
    if not trades:
        return None, (jsonify({"error": "The roster has no closed trades in this window."}), 404)

    days = {((c.get("ticker") or "").upper(), (c.get("entry_time") or "")[:10])
            for c in trades if c.get("ticker") and c.get("entry_time")}
    if len(days) > REPLAY_MAX_TICKER_DAYS:
        return None, (jsonify({"error": f"{len(days)} ticker-days of 1-minute bars is more than one run "
                                        f"fetches ({REPLAY_MAX_TICKER_DAYS}). Use a shorter window or Top 10."}), 400)
    bars = {}
    with _cf.ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(_day_bars, a, tk, d): (tk, d) for tk, d in days}
        for f in _cf.as_completed(futs):
            try:
                bars[futs[f]] = f.result() or []
            except Exception:
                bars[futs[f]] = []

    rules = _exit_settings(a)
    backstop = int(a.MAX_HOLD_MINS) if a.MAX_HOLD_MINS > 0 else 0
    prepared, no_bars = [], 0
    for c in trades:
        tk = (c.get("ticker") or "").upper()
        et = c.get("entry_time") or ""
        try:
            entry_dt = datetime.fromisoformat(et.replace("Z", "+00:00"))
            entry_px, qty = float(c["entry_price"]), float(c.get("qty") or 0)
        except (KeyError, TypeError, ValueError):
            continue
        tb = [b for b in bars.get((tk, et[:10]), []) if b.timestamp >= entry_dt]
        if not tb or not entry_px or not qty:
            no_bars += 1
            continue
        rule = rules.get(c["strategy"], {})
        prepared.append({
            "strategy": c["strategy"], "farm": c["farm"], "side": (c.get("side") or "").upper(),
            "entry_dt": entry_dt, "entry_px": entry_px, "qty": qty, "notional": entry_px * qty,
            "bars": tb, "date": et_date(et), "actual_pnl": float(c.get("pnl") or 0),
            "base": {"trail": rule.get("trail", DEFAULT_TRAIL), "trigger": rule.get("trigger", 0.0),
                     "max_hold": rule.get("max_hold") or backstop},
        })
    if not prepared:
        return None, (jsonify({"error": "No roster trade had 1-minute bars to replay."}), 404)

    def sim(td, trail, trigger, max_hold, tp):
        r = a._simulate_exit(td["bars"], td["entry_px"], td["side"],
                             a._apply_session_trail(trail, td["entry_dt"]), trigger, 0.0,
                             int(max_hold or 0), td["entry_dt"], qty=td["qty"],
                             take_profit_pct=tp or 0.0)
        if not r:
            return None
        move = r["exit_price"] - td["entry_px"]
        return {"pnl": round((move if td["side"] == "LONG" else -move) * td["qty"], 2),
                "reason": r["reason"]}

    meta = {
        "roster": len(roster), "trades": len(prepared), "no_bars": no_bars,
        "ticker_days": len(days), "actual_pnl": round(sum(t["actual_pnl"] for t in prepared), 2),
        "backstop_max_hold": backstop or None,
        "from_date": p["from_date"] or min(t["date"] for t in prepared), "to_date": p["to_date"],
    }
    return {"prepared": prepared, "sim": sim, "meta": meta}, None


@farms_bp.route("/api/farms/replay", methods=["POST"])
def api_farm_replay():
    """Replay the current Select roster's real farm trades with different exits.

    Body: the page's leaderboard params (n, days, period, min_trades, cap) so the
    roster matches what's on screen, plus test = max_hold | trail | take_profit and
    its grid (max_holds / trails + triggers / tps, comma lists, % of price)."""
    body = request.get_json(silent=True) or {}
    test = body.get("test") if body.get("test") in REPLAY_TESTS else "max_hold"
    ctx, err = _prepare_replay()
    if err:
        return err
    out = score_variants(ctx["prepared"], build_variants(test, body), ctx["sim"])
    out.update({"test": test, **ctx["meta"]})
    return jsonify(out)


@farms_bp.route("/api/farms/replay_suggest", methods=["POST"])
def api_farm_replay_suggest():
    """Run all three exit tests, keep the winners that held up on the newer half,
    replay them together, and say what to change. Same body as /api/farms/replay
    (the grids for all three tests may be passed at once)."""
    body = request.get_json(silent=True) or {}
    ctx, err = _prepare_replay()
    if err:
        return err
    out = suggest_exits(ctx["prepared"], ctx["sim"], body)
    out.update(ctx["meta"])
    return jsonify(out)


# ── Write exit settings to Kairos Select's rules ──────────────────────────────

def _load_exit_overrides(a):
    try:
        v = json.loads(a._load_setting("SELECT_EXIT_OVERRIDES") or "{}")
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _exit_override_values(settings, backstop):
    """Replay names -> exit_params values. Returns (values, error). A value of None
    removes the key (rule falls back to the default)."""
    vals = {}
    for name, key in EXIT_OVERRIDE_KEYS.items():
        if name not in settings or settings[name] is None:
            continue
        try:
            v = float(settings[name])
        except (TypeError, ValueError):
            return None, f"{name} must be a number"
        if v < 0:
            return None, f"{name} can't be negative"
        if name == "max_hold":
            if v == 0 and backstop:
                return None, (f"A rule can't turn max hold off while the global {backstop:g}-min "
                              "backstop is set: a rule without its own max hold falls back to it.")
            vals[key] = int(v) if v else None
        elif name == "tp":
            vals[key] = v or None
        else:
            vals[key] = v
    if not vals:
        return None, "no exit settings given"
    return vals, None


@farms_bp.route("/api/farms/select_exits", methods=["POST"])
def api_farm_select_exits():
    """Write exit settings onto every Kairos Select rule (rules whose only broker is
    account 7 — farms and other books are never touched), and save them as Select's
    exit overrides so a later roster Apply re-applies them instead of re-cloning
    the farm pipeline's exits. The previous exits are kept for one-step Undo.

    Body: {settings: {max_hold?, trail?, trigger?, tp?}} — the replay's units
    (minutes, % of price)."""
    a = _app()
    settings = (request.get_json(silent=True) or {}).get("settings") or {}
    backstop = a.MAX_HOLD_MINS if a.MAX_HOLD_MINS > 0 else 0
    vals, err = _exit_override_values(settings, backstop)
    if err:
        return jsonify({"error": err + " Nothing was changed."}), 400

    conn = a.get_db(); cur = conn.cursor(); ph = a.placeholder()
    own, _mixed, _src = _select_rules(cur, a)
    names = {}
    for s, rid in own.items():
        names.setdefault(rid, []).append(s)
    prev, updated, skipped = {}, [], []
    for rid, strats in names.items():
        cur.execute(f"SELECT nodes FROM routing_rules WHERE id={ph}", (rid,))
        row = cur.fetchone()
        raw = row[0] if a.DATABASE_URL else row["nodes"]
        nodes = json.loads(raw) if isinstance(raw, str) else (raw or [])
        prev[str(rid)] = next((copy.deepcopy(n) for n in nodes if n.get("type") == "exit_params"), None)
        new, why = apply_exit_overrides(nodes, vals)
        cur.execute(f"UPDATE routing_rules SET nodes={ph} WHERE id={ph}", (json.dumps(new), rid))
        updated.extend(strats)
        if why:
            skipped.append({"strategies": strats, "reason": why})
    conn.commit(); conn.close()

    old = _load_exit_overrides(a)
    merged = {k: v for k, v in {**old, **vals}.items() if v is not None}
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    a._save_setting("SELECT_EXIT_OVERRIDES", json.dumps(merged))
    a._save_setting("SELECT_EXITS_UNDO", json.dumps({"at": ts, "applied": vals,
                                                      "prev_overrides": old, "rules": prev}))
    a.log.info("Kairos Select exits set from Farm Leaders: %s on %d rules (%d skipped)",
               vals, len(names), len(skipped))
    return jsonify({"updated": sorted(updated), "skipped": skipped, "overrides": merged,
                    "applied": vals, "at": ts})


@farms_bp.route("/api/farms/select_exits/undo", methods=["POST"])
def api_farm_select_exits_undo():
    """Put back the exits the last change replaced, and the overrides before it.
    Rules deleted since (roster rewired) are skipped."""
    a = _app()
    try:
        rec = json.loads(a._load_setting("SELECT_EXITS_UNDO") or "null")
    except Exception:
        rec = None
    if not rec:
        return jsonify({"error": "Nothing to undo."}), 400
    conn = a.get_db(); cur = conn.cursor(); ph = a.placeholder()
    restored, gone = 0, 0
    for rid, ep in (rec.get("rules") or {}).items():
        cur.execute(f"SELECT nodes FROM routing_rules WHERE id={ph}", (int(rid),))
        row = cur.fetchone()
        if not row:
            gone += 1
            continue
        raw = row[0] if a.DATABASE_URL else row["nodes"]
        nodes = [n for n in (json.loads(raw) if isinstance(raw, str) else (raw or []))
                 if n.get("type") != "exit_params"]
        if ep:
            nodes.append(ep)
        cur.execute(f"UPDATE routing_rules SET nodes={ph} WHERE id={ph}", (json.dumps(nodes), int(rid)))
        restored += 1
    conn.commit(); conn.close()
    a._save_setting("SELECT_EXIT_OVERRIDES", json.dumps(rec.get("prev_overrides") or {}))
    a._save_setting("SELECT_EXITS_UNDO", "")
    a.log.info("Kairos Select exit change of %s undone (%d rules restored, %d gone)",
               rec.get("at"), restored, gone)
    return jsonify({"restored": restored, "gone": gone, "overrides": rec.get("prev_overrides") or {}})


# ── Select vs its farms: are the same trades being made? ──────────────────────

PAIR_WINDOW_SECS = 15 * 60         # same rule as Crew Live vs Paper (_LVP_PAIR_WINDOW_SECS)


def _rt_pct(t):
    try:
        notional = abs(float(t.get("entry_price") or 0)) * abs(float(t.get("qty") or 0))
        return float(t.get("pnl") or 0) / notional * 100 if notional else None
    except (TypeError, ValueError):
        return None


def _epoch(ts):
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def pair_books(a_rts, b_rts, window=PAIR_WINDOW_SECS):
    """One-to-one pairs on strategy + side + ET entry date, nearest entry first
    within `window` seconds. Returns (pairs, a_only, b_only); pairs are (a, b)."""
    left = list(b_rts)
    pairs, a_only = [], []
    for t in sorted(a_rts, key=lambda x: x.get("entry_time") or ""):
        key = (t["strategy"], (t.get("side") or "").upper(), et_date(t.get("entry_time")))
        te = _epoch(t.get("entry_time"))
        best, lag = None, None
        for u in left:
            if (u["strategy"], (u.get("side") or "").upper(), et_date(u.get("entry_time"))) != key:
                continue
            ue = _epoch(u.get("entry_time"))
            if te is None or ue is None or abs(te - ue) > window:
                continue
            if lag is None or abs(te - ue) < lag:
                best, lag = u, abs(te - ue)
        if best is None:
            a_only.append(t)
        else:
            left.remove(best)
            pairs.append((t, best))
    return pairs, a_only, left


def strategy_starts(select_rts, wired_since=None):
    """{STRATEGY: ET date} from which each strategy counts as on Select's roster: the
    earlier of its first Select trade and the date the last Apply wired it.

    Select's roster changes (it began as a frozen 15-name list; Apply rewires it),
    so a single window start would count farm trades from before a strategy was
    even on Select as trades Select "missed". A strategy with neither date has no
    evidence of ever being on Select and is left out of the farm side."""
    starts = dict(wired_since or {})
    for t in select_rts:
        d = et_date(t.get("entry_time"))
        if d and (t["strategy"] not in starts or d < starts[t["strategy"]]):
            starts[t["strategy"]] = d
    return starts


def compare_select_farms(select_rts, farm_rts, source, wired_since=None):
    """Kairos Select's actual trades against the same strategies on their source farms.

    select_rts:  Select's closed round-trips (strategy upper-cased).
    farm_rts:    {"tv": [...], "kairos": [...]} closed round-trips per farm.
    source:      {STRATEGY: "tv" | "kairos"} — the farm each Select strategy enters
                 like (TV picks: TV Farm, engine picks: Kairos Farm).
    wired_since: {STRATEGY: ET date} the roster was wired (last Apply), optional.

    Each strategy's farm trades count only from the day it joined Select (see
    strategy_starts), and the lines start at Select's first trade, so neither the
    farms' earlier history nor a later roster change runs up a head start. Returns
    cumulative % per trade (size-independent: Select trades ~$15k, the farms $1k)
    by ET exit date, plus the trade-by-trade pairing."""
    # A trade whose entry time can't be read has no date to pair or plot on.
    sel = [t for t in select_rts if et_date(t.get("entry_time"))]
    if not sel:
        return {"dates": [], "select": [], "farm": [], "select_trades": 0, "farm_trades": 0,
                "start": None, "by_source": {}, "unknown_source": [], "strategy_starts": {}}
    start = min(et_date(t["entry_time"]) for t in sel)
    starts = {s: max(d, start) for s, d in strategy_starts(sel, wired_since).items()}
    farm = {f: [t for t in rts if source.get(t["strategy"]) == f and t["strategy"] in starts
                and et_date(t.get("entry_time")) >= starts[t["strategy"]]]
            for f, rts in farm_rts.items()}

    def _daily(rts):
        out = {}
        for t in rts:
            p = _rt_pct(t)
            if p is not None:
                d = et_date(t.get("exit_time")) or et_date(t.get("entry_time"))
                out[d] = out.get(d, 0.0) + p
        return out
    farm_all = [t for rts in farm.values() for t in rts]
    ds, df = _daily(sel), _daily(farm_all)
    dates = sorted(set(ds) | set(df))
    cs = cf = 0.0
    sel_line, farm_line = [], []
    for d in dates:
        cs += ds.get(d, 0.0); cf += df.get(d, 0.0)
        sel_line.append(round(cs, 4)); farm_line.append(round(cf, 4))

    def _brief(t):
        p = _rt_pct(t)
        return {"strategy": t["strategy"], "ticker": t.get("ticker"), "side": t.get("side"),
                "entry_time": t.get("entry_time"), "pct": None if p is None else round(p, 4),
                "pnl": round(float(t.get("pnl") or 0), 2)}

    by_source = {}
    for f in ("tv", "kairos"):
        s_rts = [t for t in sel if source.get(t["strategy"]) == f]
        pairs, s_only, f_only = pair_books(s_rts, farm.get(f, []))
        deltas = [(_rt_pct(a) or 0) - (_rt_pct(b) or 0) for a, b in pairs]
        lags = [round(_epoch(a["entry_time"]) - _epoch(b["entry_time"])) for a, b in pairs]
        by_source[f] = {
            "select_trades": len(s_rts), "farm_trades": len(farm.get(f, [])),
            "paired": len(pairs), "select_only": [_brief(t) for t in s_only],
            "farm_only": [_brief(t) for t in f_only],
            "mean_pct_delta": round(sum(deltas) / len(deltas), 4) if deltas else None,
            "median_entry_lag_secs": sorted(lags)[len(lags) // 2] if lags else None,
        }
    return {
        "dates": dates, "select": sel_line, "farm": farm_line, "start": start,
        "select_trades": len(sel), "farm_trades": len(farm_all), "by_source": by_source,
        "unknown_source": sorted({t["strategy"] for t in sel if t["strategy"] not in source}),
        "strategy_starts": {s: d for s, d in sorted(starts.items()) if s in source},
    }


def _select_wired_since(a):
    """{STRATEGY: ET date} for the strategies the last Apply wired onto Select.
    Only the latest Apply is recorded, so earlier roster changes fall back to each
    strategy's first Select trade (strategy_starts)."""
    try:
        rec = json.loads(a._load_setting("SELECT_LAST_APPLY") or "null") or {}
    except Exception:
        return {}
    at = rec.get("at") or ""
    try:
        d = datetime.strptime(at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).astimezone(_ET).date().isoformat()
    except ValueError:
        return {}
    return {pk["strategy"].upper(): d for pk in rec.get("picks") or [] if pk.get("strategy")}


def _select_sources(a):
    """{STRATEGY: "tv" | "kairos"} for Select: the entry source on each Select rule,
    plus the last Apply's picks for strategies no longer wired (they still traded)."""
    out = {}
    try:
        rec = json.loads(a._load_setting("SELECT_LAST_APPLY") or "null") or {}
        for pk in rec.get("picks") or []:
            out[pk["strategy"].upper()] = "kairos" if pk.get("entry") == "kairos" else "tv"
    except Exception:
        pass
    conn = a.get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT nodes FROM routing_rules")
        rows = cur.fetchall()
    finally:
        conn.close()
    for row in rows:
        raw = row[0] if a.DATABASE_URL else row["nodes"]
        try:
            nodes = json.loads(raw) if isinstance(raw, str) else (raw or [])
        except Exception:
            continue
        if not any((n.get("value") or "").lower() in SELECT_BROKERS
                   for n in nodes if n.get("type") == "broker"):
            continue
        src = next(((n.get("value") or "").lower() for n in nodes if n.get("type") == "entry_source"), "tv")
        for n in nodes:
            if n.get("type") == "strategy" and n.get("value") and "*" not in n["value"]:
                out[n["value"].strip().upper()] = "kairos" if src == "kairos" else "tv"
    return out


@farms_bp.route("/api/farms/select_vs_farms")
def api_select_vs_farms():
    """Kairos Select's actual trades vs the same strategies on their source farms,
    over the page's window (clamped to Select's first trade)."""
    a = _app()
    p = _params()
    rec = a.ACCOUNTS_BY_NUM.get("7")
    if not rec or rec.get("broker") is None:
        return jsonify({"error": "Kairos Select (account 7) isn't configured."}), 400
    try:
        fills = rec["fills_fn"]()
    except Exception as e:
        return jsonify({"error": f"Kairos Select fills unavailable: {e}"}), 409
    if not fills and a._fills_error("7"):
        return jsonify({"error": "Kairos Select fills unavailable."}), 409
    paired = a._pair_alpaca_fills_lifo(fills or [], from_date=p["from_date"], to_date=p["to_date"])
    sel = [{**c, "strategy": (c.get("strategy") or "").strip().upper()}
           for c in paired.get("closed_clean") or []]
    sel = [c for c in sel if c["strategy"] and c["strategy"] != "UNKNOWN"]
    farm = {}
    for key, num, label in FARMS:
        rts, err = _farm_round_trips(num, p["from_date"], p["to_date"], full=p["full"])
        if err:
            return jsonify({"error": f"{label}: {err}"}), 409
        farm[key] = rts
    out = compare_select_farms(sel, farm, _select_sources(a), _select_wired_since(a))
    out.update({"from_date": p["from_date"], "to_date": p["to_date"], "pair_window_mins": PAIR_WINDOW_SECS // 60})
    return jsonify(out)


# ── Month-by-month book pack: Kairos Refined vs Kairos Select on Kairos Farm ──
# The data behind a monthly "how did each roster do" read — computed here, so a
# report agent interprets numbers instead of producing them. Every farm trade is
# judged against the roster that held AT ITS ENTRY, never today's roster: filtering
# the past by today's names is hindsight (they were picked for looking good then).

from bisect import bisect_right as _bisect_right


def _to_epoch(ts):
    """'2026-10-23 20:16:21 UTC' / ISO with Z / naive -> epoch seconds, or None."""
    s = str(ts or "").strip().replace(" UTC", "").replace("Z", "+00:00")
    if not s:
        return None
    if "T" not in s:
        s = s.replace(" ", "T", 1)
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.timestamp()


class RosterTimeline:
    """Which strategies a book held at a moment: sorted (epoch, set) snapshots, each
    in force until the next. Before the first snapshot, falls back to per-strategy
    (first, last) intervals when given — the proxy for history nobody recorded."""

    def __init__(self, snapshots, proxy=None):
        snaps = sorted((t, frozenset(s)) for t, s in snapshots if t is not None)
        self.times = [t for t, _ in snaps]
        self.sets = [s for _, s in snaps]
        self.proxy = proxy or {}

    def holds(self, strategy, epoch):
        if epoch is None:
            return False
        i = _bisect_right(self.times, epoch) - 1
        if i >= 0:
            return strategy in self.sets[i]
        iv = self.proxy.get(strategy)
        return bool(iv) and iv[0] <= epoch <= iv[1]


def _metrics(rows):
    """rows: [(pnl, pct)] -> {trades, win_rate, profit_factor, avg_pct, pnl}."""
    if not rows:
        return {"trades": 0, "win_rate": None, "profit_factor": None, "avg_pct": None, "pnl": 0.0}
    pnls = [p for p, _ in rows]
    pcts = [q for _, q in rows if q is not None]
    wins = [p for p in pnls if p > 0]
    gl = abs(sum(p for p in pnls if p <= 0))
    return {"trades": len(rows),
            "win_rate": round(len(wins) / len(rows) * 100, 1),
            "profit_factor": round(sum(wins) / gl, 2) if gl > 0 else None,
            "avg_pct": round(sum(pcts) / len(pcts), 4) if pcts else None,
            "pnl": round(sum(pnls), 2)}


def book_month_pack(farm_rts, refined, select, actual, months):
    """Month-by-month comparison on one farm.

    farm_rts: the farm's closed round-trips.
    refined / select: RosterTimeline for each book.
    actual:   {"refined": [...], "select": [...]} each book's own round-trips.
    months:   ['YYYY-MM', ...] oldest first.

    Per month: the farm's trades split by which roster held the strategy at entry
    (both / refined only / select only / neither), each roster's farm total, and each
    book's ACTUAL results (its own size, gates and execution). % per trade is the
    comparable number — the farm trades $1k, the books ~$15k."""
    def _row(t):
        return float(t.get("pnl") or 0), _rt_pct(t)

    out = []
    for m in months:
        groups = {k: [] for k in ("both", "refined_only", "select_only", "neither")}
        ref_all, sel_all = [], []
        for t in farm_rts:
            et = t.get("entry_time")
            if (et_date(et) or "")[:7] != m:
                continue
            e, s = _to_epoch(et), t["strategy"]
            r, q = refined.holds(s, e), select.holds(s, e)
            key = "both" if r and q else "refined_only" if r else "select_only" if q else "neither"
            groups[key].append(_row(t))
            if r:
                ref_all.append(_row(t))
            if q:
                sel_all.append(_row(t))
        act = {k: _metrics([_row(t) for t in (actual.get(k) or [])
                            if (et_date(t.get("entry_time")) or "")[:7] == m])
               for k in ("refined", "select")}
        farm = {"refined_roster": _metrics(ref_all), "select_roster": _metrics(sel_all)}
        farm.update({k: _metrics(v) for k, v in groups.items()})
        out.append({"month": m, "farm": farm, "actual": act})
    return out


def _book_rts(a, num, full=True):
    """A book's own closed round-trips (no leaderboard exclusions — these are trades
    it actually made). Returns (rts, error)."""
    rec = a.ACCOUNTS_BY_NUM.get(num)
    if not rec:
        return [], "not configured"
    try:
        fills = _full_fills(rec, num) if full else rec["fills_fn"]()
    except Exception as e:
        return [], f"fills unavailable: {e}"
    paired = a._pair_alpaca_fills_lifo(fills or [])
    return [dict(c, strategy=(c.get("strategy") or "").strip().upper())
            for c in paired.get("closed_clean") or []], None


def _refined_timeline(a):
    """Kairos Refined's roster over time, from every saved snapshot run."""
    conn = a.get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT run_at, strategy_name FROM kairos_refined_history")
        rows = cur.fetchall()
    finally:
        conn.close()
    runs = {}
    for r in rows:
        at = r[0] if a.DATABASE_URL else r["run_at"]
        nm = r[1] if a.DATABASE_URL else r["strategy_name"]
        runs.setdefault(at, set()).add((nm or "").strip().upper())
    return RosterTimeline([(_to_epoch(at), names) for at, names in runs.items()])


def _select_timeline(a, select_rts):
    """Kairos Select's roster over time: every logged Apply (from 2026-10-24) and
    Clear, and before the first logged one, each strategy's span of actual Select
    trades — the only record of the roster before Applies were logged."""
    try:
        hist = json.loads(a._load_setting("SELECT_APPLY_HISTORY") or "[]") or []
    except Exception:
        hist = []
    snaps = [(_to_epoch(h.get("at")), {s.upper() for s in h.get("picks") or []}) for h in hist]
    proxy = {}
    for t in select_rts:
        e = _to_epoch(t.get("entry_time"))
        if e is None:
            continue
        lo, hi = proxy.get(t["strategy"], (e, e))
        proxy[t["strategy"]] = (min(lo, e), max(hi, e))
    return RosterTimeline(snaps, proxy)


def _log_select_roster(a, picks, at):
    """Append one Select roster change to SELECT_APPLY_HISTORY (newest 200 kept) —
    SELECT_LAST_APPLY only ever holds the latest, which can't reconstruct a month."""
    try:
        hist = json.loads(a._load_setting("SELECT_APPLY_HISTORY") or "[]") or []
    except Exception:
        hist = []
    hist.append({"at": at, "picks": sorted({p.upper() for p in picks})})
    a._save_setting("SELECT_APPLY_HISTORY", json.dumps(hist[-200:]))


def _recent_months(n, today=None):
    d = today or datetime.now(_ET).date()
    y, m, out = d.year, d.month, []
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return list(reversed(out))


@farms_bp.route("/api/farms/book_months")
def api_book_months():
    """?months=4 — month-by-month on Kairos Farm: the Kairos Refined roster vs the
    Kairos Select roster (each as it stood at each trade), plus both books' actual
    results. Read-only. The data pack for a monthly report agent."""
    a = _app()
    try:
        n = max(1, min(12, int(request.args.get("months") or 4)))
    except ValueError:
        n = 4
    months = _recent_months(n)
    farm, err = _farm_round_trips("5", "", "", full=True)
    if err:
        return jsonify({"error": f"Kairos Farm: {err}"}), 409
    ref_rts, e1 = _book_rts(a, "3")
    sel_rts, e2 = _book_rts(a, "7")
    pack = book_month_pack(farm, _refined_timeline(a), _select_timeline(a, sel_rts),
                           {"refined": ref_rts, "select": sel_rts}, months)
    errors = {k: v for k, v in (("Kairos Refined", e1), ("Kairos Select", e2)) if v}
    return jsonify({
        "farm": "Kairos Farm", "months": pack, "errors": errors,
        "notes": [
            "Each farm trade is attributed to the roster that held at its entry, not today's roster.",
            "Kairos Refined's roster comes from its saved daily snapshots.",
            "Kairos Select's roster comes from logged Applies (from 2026-10-24); before that, "
            "each strategy counts from its first to its last actual Select trade.",
            "% per trade (avg_pct) is the comparable number: the farm trades $1k, the books ~$15k.",
        ],
    })
