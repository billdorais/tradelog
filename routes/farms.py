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


def merge_roster(tops):
    """Combine each farm's top list into one Select roster.

    tops: {"tv": [{strategy, pnl, ...}], "kairos": [...]}. A strategy in BOTH lists
    gets one rule, sourced from the farm where it made more — the same both-books
    rule the crew uses, since two rules would double its position."""
    best = {}
    for farm, rows in tops.items():
        for r in rows:
            s = r["strategy"]
            if s not in best or r["pnl"] > best[s]["pnl"]:
                best[s] = {"strategy": s, "farm": farm, "entry": ENTRY_BY_FARM[farm],
                           "pnl": r["pnl"], "also_in": best.get(s, {}).get("farm")}
            else:
                best[s]["also_in"] = farm
    return sorted(best.values(), key=lambda p: (-p["pnl"], p["strategy"]))


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
        return [{"strategy": nm, "pnl": s["pnl"]} for nm, s in rank_top(trips, n, min_trades, cap)]

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

    start = keys[1] if len(keys) > 1 else None
    out = {"dates": dates, "forward_start": start}
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


def _farm_trips(num, from_date, to_date, full=False):
    """Closed round-trips on one farm as [{strategy, pnl, date}], honouring the same
    strategy/ticker/band exclusions as the Refined leaderboards. Returns
    (trips, error) — a failed fetch must not render as an empty farm."""
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
    trips = []
    for c in paired.get("closed_clean") or []:
        s = (c.get("strategy") or "").strip().upper()
        if not s or s == "UNKNOWN" or s in ex_s:
            continue
        if s.split("_", 1)[0] in ex_t or (ex_b and a._band_of(s) in ex_b):
            continue
        d = et_date(c.get("exit_time")) or c.get("date") or ""
        trips.append({"strategy": s, "pnl": float(c.get("pnl") or 0), "date": d})
    return trips, None


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
        ok = same(c["select"], c["tv"]) and same(c["select"], c["kairos"])
        disagree = disagree or not same(c["tv"], c["kairos"])
        rows.append({"gate": g, **c, "ok": ok})
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
        "diff": {"add": sorted(new - set(current)),
                 "keep": sorted(new & set(current)),
                 "remove": sorted(set(current) - new)},
        "select_configured": SELECT_TAG in a.ACCOUNTS_BY_TAG,
        "gate_parity": _parity_or_error(a),
        "last_apply": json.loads(a._load_setting("SELECT_LAST_APPLY") or "null"),
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
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    created, updated, sizes = [], [], {}
    for r in roster:
        slug = r["strategy"]
        shares = max(1, round(size / prices[slug.split("_", 1)[0]]))
        sizes[slug] = shares
        nodes = json.dumps(select_nodes(slug, shares, r["entry"],
                                        (source.get(slug) or (0, None))[1]))
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
    except Exception as e:
        a.log.warning("farms: could not persist the Select clear record: %s", e)
    a.log.info("Kairos Select cleared from Farm Leaders: -%d, %d deferred, %d shared rules stripped",
               len(deleted), len(deferred), len(stripped))
    return jsonify({"deleted": deleted, "deferred_open_position": deferred,
                    "stripped_from_mixed": stripped})
