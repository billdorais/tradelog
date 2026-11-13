"""Exit replay: re-run each real trade's exit on tick data, then ask "what if".

Why this exists: the 1-min bar sweep (/api/simulate/sweep) can't judge a 0.1% trail
(real exits fire ~18 s after the fill, inside one bar), it models a trail from entry
while the broker actually places a HARD STOP at the trail distance from the signal
price until the trigger (brokers/alpaca_broker.py), and it fills every exit perfectly.
It overstated farm P&L by $0.27–0.86 per trade.

This module:
  1. reads the account's real orders from Alpaca (entries, the hard stops and
     trailing stops the broker placed for each, and how each trade really exited);
  2. replays those exact orders on SIP trade ticks (the MATCH CHECK: does the replay
     reproduce the real exit time and price?) and measures the extra slippage;
  3. replays each trade under the live policy with other trail / trigger / stop
     settings (the VARIANTS), filling at the quoted spread plus measured slippage.

It only reads (orders, trades, quotes) and writes its own results table. It is
CPU-heavy, so it runs as one background job at a time and refuses to start, or
stops, while the market is open (the same server routes live orders).
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
from flask import Blueprint, jsonify, request

log = logging.getLogger(__name__)
exit_replay_bp = Blueprint("exit_replay", __name__)
_ET = ZoneInfo("America/New_York")

MAX_RUN_DAYS = 31            # one run covers at most a month of trades
MAX_VARIANTS = 30
SLOW_FILL_S = 5.0            # broker waits 5 s for the entry; slower fills take the immediate-trail path
POLL_S = 1.0                 # the broker's trigger thread polls unrealized P&L every second
DEFAULT_PLACE_DELAY_S = 1.0  # entry fill → protective order, when not observed
WINDOW_AFTER_S = 16 * 60     # ticks fetched per trade: fill → 16 min (max hold is 10–15)
EOD_FLAT = (15, 55)          # ET: positions still open are flattened here (a variant with no max hold)
LONG_WINDOW_S = 7 * 3600     # when a variant holds past the rule's max hold, one window per symbol-day
MAX_WINDOW_S = 30 * 60       # a merged tick window never spans more than this (a ticker traded
                             # all day would otherwise pull the whole session into memory)
TICK_CHUNK_S = 5 * 60        # ticks are requested 5 min at a time and packed into arrays as they arrive
HEARTBEAT_STALE_S = 15 * 60  # a 'running' run with no progress for this long died (e.g. a restart)
REF_MAX_GAP = 0.01           # a derived signal price more than 1% from the fill is ignored
MATCH_DT_S = 15.0            # match check: replayed exit within 15 s …
MATCH_DPX_PCT = 0.03         # … and within 0.03% of the real exit price
TRUST_MIN_MATCH = 0.70       # the replay is trusted when ≥70% of stop exits match
TRUST_MAX_GAP = 0.25         # … and as-configured P&L is within 25% of actual per group

# SIP sale conditions that don't update the last sale price (odd lot, average price,
# out of sequence, form T, prior reference, official open/close, derivatively priced …).
NON_LAST_SALE = frozenset({"B", "C", "G", "H", "I", "L", "M", "N", "P", "Q", "R", "T",
                           "U", "V", "W", "Z", "4", "7", "9"})

ENTRY_RE = re.compile(r"^kairos-(?!trail-|hard-)(.+)-(\d{10})$")
TRAIL_RE = re.compile(r"^kairos-trail-(.+)-(\d{10})$")
STRATEGY_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,80}$")
MAX_STRATEGIES = 50


# ── pure helpers: the replay engine ────────────────────────────────────────────

def band_of(strategy):
    s = (strategy or "").upper()
    i = s.find("_CAM_")
    if i < 0:
        return "OTHER"
    p = s[i + 5:].split("_")
    return f"{p[0]} {p[1]}" if len(p) >= 2 else "OTHER"


def _first(mask):
    idx = np.flatnonzero(mask)
    return int(idx[0]) if idx.size else -1


def _price_at(t, p, when):
    """Last trade price at or before `when` (first price if `when` is before all ticks)."""
    i = int(np.searchsorted(t, when, side="right")) - 1
    return float(p[max(i, 0)])


def _fill(px, is_long, half_spread, slip):
    """A sell (closing a long) fills at the bid; a buy at the ask; then slippage."""
    adj = px * slip / 100.0
    return px - half_spread - adj if is_long else px + half_spread + adj


def replay_orders(t, p, is_long, orders, end_t=None):
    """First protective order to trigger on ticks (t, p). Each order is
    {kind: 'trail'|'stop', placed, ended (None = open), trail_pct | stop_price}.
    A trailing stop's high-water mark starts at the last price when it was placed.
    Returns (trigger_time, trigger_price, kind) or None."""
    best = None
    for o in orders:
        i0 = int(np.searchsorted(t, o["placed"], side="left"))
        stop_at = o.get("ended")
        if end_t is not None:
            stop_at = end_t if stop_at is None else min(stop_at, end_t)
        i1 = len(t) if stop_at is None else int(np.searchsorted(t, stop_at, side="right"))
        if i1 <= i0:
            continue
        seg = p[i0:i1]
        if o["kind"] == "trail":
            start = float(p[i0 - 1]) if i0 > 0 else float(seg[0])
            f = o["trail_pct"] / 100.0
            if is_long:
                hwm = np.maximum.accumulate(np.concatenate(([start], seg)))[1:]
                hit = _first(seg <= hwm * (1 - f))
            else:
                lwm = np.minimum.accumulate(np.concatenate(([start], seg)))[1:]
                hit = _first(seg >= lwm * (1 + f))
        else:
            hit = _first(seg <= o["stop_price"]) if is_long else _first(seg >= o["stop_price"])
        if hit >= 0:
            j = i0 + hit
            if best is None or t[j] < best[0]:
                best = (float(t[j]), float(p[j]), o["kind"])
    return best


def replay_policy(t, p, is_long, fill_t, pol):
    """Replay one trade under the live exit policy. `pol`:
      mode        'immediate' (trailing stop from the fill) | 'trigger' (hard stop at
                  stop_px until the price reaches trigger_px at a 1 s poll, then a
                  trailing stop from that price)
      trail_pct, stop_px, trigger_px, place_delay, deadline (max-hold or cap time),
      deadline_kind ('max_hold' | 'cap')
    Returns (exit_time, trigger_price, reason)."""
    if len(t) == 0:
        return None
    deadline = pol["deadline"]
    place_t = fill_t + pol.get("place_delay", DEFAULT_PLACE_DELAY_S)

    def _timed():
        return (deadline, _price_at(t, p, deadline), pol.get("deadline_kind", "max_hold"))

    if pol["mode"] == "immediate" or pol.get("trigger_px") is None:
        hit = replay_orders(t, p, is_long, [{"kind": "trail", "placed": place_t, "ended": deadline,
                                            "trail_pct": pol["trail_pct"]}])
        return hit or _timed()

    # Trigger mode: polls at place_t + k·POLL_S until the deadline.
    polls = place_t + POLL_S * np.arange(1, max(1, int((deadline - place_t) / POLL_S)) + 1)
    pidx = np.clip(np.searchsorted(t, polls, side="right") - 1, 0, len(t) - 1)
    ppx = p[pidx]
    k = _first(ppx >= pol["trigger_px"]) if is_long else _first(ppx <= pol["trigger_px"])
    switch_t = float(polls[k]) if k >= 0 else None
    stop_end = switch_t if switch_t is not None else deadline
    hit = replay_orders(t, p, is_long, [{"kind": "stop", "placed": place_t, "ended": stop_end,
                                        "stop_price": pol["stop_px"]}])
    if hit:
        return hit
    if switch_t is None:
        return _timed()
    hit = replay_orders(t, p, is_long, [{"kind": "trail", "placed": switch_t, "ended": deadline,
                                        "trail_pct": pol["trail_pct"]}])
    return hit or _timed()


def eod_flat_t(fill_t):
    """Epoch seconds of the EOD flatten (EOD_FLAT ET) on the trade's day."""
    d = datetime.fromtimestamp(fill_t, _ET)
    return d.replace(hour=EOD_FLAT[0], minute=EOD_FLAT[1], second=0, microsecond=0).timestamp()


def variant_deadline(tr, max_hold=None):
    """When a variant's position is closed if no stop or trail fires first.
    max_hold None = the rule's own; 0 = no max hold (held to the EOD flatten); N = N minutes.
    A 'cap' deadline (a real exit the replay can't model: a TV exit signal, a flatten,
    the strikes gate) is kept whatever the variant's hold."""
    if max_hold is None or tr["deadline_kind"] != "max_hold":
        return tr["deadline"], tr["deadline_kind"]
    eod = max(eod_flat_t(tr["fill_t"]), tr["fill_t"] + 60)
    if max_hold == 0:
        return eod, "eod"
    return min(tr["fill_t"] + max_hold * 60, eod), "max_hold"


def policy_for(tr, trail_pct, trigger_pct, stop_pct=None, max_hold=None):
    """The live policy for trade `tr` with these settings (mirrors alpaca_broker:
    a trigger needs the entry filled within 5 s and a signal price; the hard stop
    sits at stop_pct — or the trail distance when there's no stop_loss — from it).
    max_hold: see variant_deadline."""
    is_long = tr["is_long"]
    ref = tr["ref"]
    immediate = (not trigger_pct) or tr["slow_fill"] or not ref
    sl = (stop_pct or trail_pct) / 100.0
    deadline, dkind = variant_deadline(tr, max_hold)
    pol = {"mode": "immediate" if immediate else "trigger", "trail_pct": trail_pct,
           "place_delay": tr["place_delay"], "deadline": deadline,
           "deadline_kind": dkind, "trigger_px": None, "stop_px": None}
    if not immediate:
        pol["stop_px"] = ref * (1 - sl) if is_long else ref * (1 + sl)
        g = ref * trigger_pct / 100.0
        pol["trigger_px"] = tr["fill_px"] + g if is_long else tr["fill_px"] - g
    return pol


def variant_policy(tr, v, cfg_trail):
    """A variant's policy for one trade. Every setting the variant leaves out (None)
    is the trade's OWN configured value, so a hold-only variant changes the hold and
    nothing else. The stop used to fall through to policy_for's no-stop default (the
    trail distance), which moved a rule's 0.3% hard stop to 0.54% on a variant that
    only meant to change the hold."""
    r = tr["rule"]
    trail = v.get("trail") if v.get("trail") is not None else cfg_trail
    trig = v.get("trigger") if v.get("trigger") is not None else r["trigger"]
    stop = v.get("stop") if v.get("stop") is not None else r["stop"]
    return policy_for(tr, trail, trig, stop, v.get("max_hold"))


def trade_pnl(tr, exit_px):
    d = exit_px - tr["fill_px"] if tr["is_long"] else tr["fill_px"] - exit_px
    return d * tr["qty"]


# ── pure: rebuild trades from the account's orders ────────────────────────────

def _ts(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, datetime):
        return v.timestamp()
    return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()


def normalize_order(o):
    """Alpaca Order (object or dict) → plain dict with epoch-second times."""
    g = (lambda k: o.get(k)) if isinstance(o, dict) else (lambda k: getattr(o, k, None))
    val = lambda x: getattr(x, "value", x)
    num = lambda x: float(x) if x not in (None, "") else None
    return {
        "id": str(g("id")), "coid": str(g("client_order_id") or ""), "symbol": str(g("symbol") or "").upper(),
        "side": str(val(g("side")) or "").lower(), "type": str(val(g("type")) or val(g("order_type")) or "").lower(),
        "qty": num(g("qty")) or 0.0, "filled_qty": num(g("filled_qty")) or 0.0,
        "filled_avg_price": num(g("filled_avg_price")), "stop_price": num(g("stop_price")),
        "trail_percent": num(g("trail_percent")), "status": str(val(g("status")) or "").lower(),
        "created": _ts(g("created_at")), "submitted": _ts(g("submitted_at")),
        "filled": _ts(g("filled_at")), "canceled": _ts(g("canceled_at")),
        "replaced": _ts(g("replaced_at")),
    }


def build_trades(orders, rules, from_date, to_date, max_hold_default=10):
    """Pair each filled kairos entry with the protective orders the broker placed for
    it and the order that closed it. `rules`: STRATEGY → {trail, trigger, stop,
    max_hold}. Returns (trades, skipped_counts)."""
    by_sym = defaultdict(list)
    for o in orders:
        by_sym[o["symbol"]].append(o)
    for v in by_sym.values():
        v.sort(key=lambda o: o["created"] or 0)
    used, trades, skipped = set(), [], defaultdict(int)
    entries = [o for o in orders if ENTRY_RE.match(o["coid"]) and o["filled_qty"] > 0 and o["filled"]]
    entries.sort(key=lambda o: o["filled"])
    next_entry, last = {}, {}
    for e in reversed(entries):
        k = (e["symbol"], ENTRY_RE.match(e["coid"]).group(1).upper())
        next_entry[e["id"]] = last.get(k)
        last[k] = e["filled"]
    for e in entries:
        day = datetime.fromtimestamp(e["filled"], _ET).date().isoformat()
        if not (from_date <= day <= to_date):
            continue
        m = ENTRY_RE.match(e["coid"])
        strategy = m.group(1).upper()
        is_long = e["side"] == "buy"
        opp = "sell" if is_long else "buy"
        qty = e["filled_qty"]
        f0 = e["filled"]
        rule = rules.get(strategy) or {}
        max_hold = float(rule.get("max_hold") or max_hold_default)
        # This trade's protective orders are placed before max hold runs out and before the
        # same strategy's next entry on this symbol; later ones belong to a later trade.
        nxt = next_entry.get(e["id"])
        horizon = min(f0 + max(max_hold, 15.0) * 60 + 120, nxt if nxt else float("inf"))
        trail = hard = exit_o = None
        for o in by_sym[e["symbol"]]:
            if o["id"] in used or o["side"] != opp or o["created"] is None:
                continue
            if o["created"] < f0 - 5 or o["created"] >= horizon:
                continue
            tm = TRAIL_RE.match(o["coid"])
            if trail is None and tm and tm.group(1).upper() == strategy and o["type"] == "trailing_stop":
                trail = o
            elif (hard is None and o["type"] == "stop" and not o["coid"].startswith("kairos-")
                  and abs(o["qty"] - qty) < 1e-6 and o["created"] <= f0 + 30):
                hard = o
        prot = [o for o in (hard, trail) if o is not None and o["filled_qty"] > 0 and o["filled"]]
        if prot:
            exit_o = min(prot, key=lambda o: o["filled"])
        else:
            # First opposite fill after the entry, same ET day, any order type (a stop the
            # broker placed by another path still closes the trade). Never another trade's
            # entry or another strategy's trail. Without the same-day bound an unrecognized
            # exit made this grab a later trade's exit, days away.
            day_end = datetime.combine(datetime.fromtimestamp(f0, _ET).date(), dtime(23, 59, 59),
                                       tzinfo=_ET).timestamp()
            others = []
            for o in by_sym[e["symbol"]]:
                if (o["id"] in used or o["side"] != opp or not o["filled"] or not (f0 < o["filled"] <= day_end)
                        or o["filled_qty"] <= 0 or abs(o["filled_qty"] - qty) > 1e-6 or ENTRY_RE.match(o["coid"])):
                    continue
                tm = TRAIL_RE.match(o["coid"])
                if tm and tm.group(1).upper() != strategy:
                    continue
                others.append(o)
            exit_o = min(others, key=lambda o: o["filled"]) if others else None
        if exit_o is None or not exit_o.get("filled_avg_price"):
            skipped["no same-day exit found"] += 1
            continue
        for o in (trail, hard, exit_o):
            if o is not None:
                used.add(o["id"])
        r_trail = rule.get("trail") or (trail or {}).get("trail_percent") or 0.1
        r_trigger = rule.get("trigger") or 0.0
        r_stop = rule.get("stop")
        # Signal price, from the hard stop the broker placed at (stop or trail)% from it.
        ref = None
        if hard is not None and hard.get("stop_price"):
            sl = (r_stop or (trail or {}).get("trail_percent") or r_trail) / 100.0
            ref = hard["stop_price"] / (1 - sl) if is_long else hard["stop_price"] / (1 + sl)
            if abs(ref / e["filled_avg_price"] - 1) > REF_MAX_GAP:
                ref = None
        submitted = e["submitted"] or float(m.group(2))
        first_prot = min([o["created"] for o in (hard, trail) if o is not None] or [f0 + DEFAULT_PLACE_DELAY_S])
        kind = ("trail" if exit_o is trail else "stop" if exit_o is hard else "other")
        hold = exit_o["filled"] - f0
        mh_s = max_hold * 60
        if kind == "other" and hold < mh_s - 30:
            deadline, dkind = exit_o["filled"], "cap"          # a TV exit / flatten we can't model
        else:
            deadline, dkind = f0 + mh_s, "max_hold"
        trades.append({
            "symbol": e["symbol"], "strategy": strategy, "band": band_of(strategy), "day": day,
            "is_long": is_long, "qty": qty, "fill_t": f0, "fill_px": e["filled_avg_price"],
            "slow_fill": (f0 - submitted) > SLOW_FILL_S, "ref": ref,
            "place_delay": min(5.0, max(0.0, first_prot - f0)),
            "rule": {"trail": r_trail, "trigger": r_trigger, "stop": r_stop, "max_hold": max_hold},
            "trail_order": None if trail is None else {
                "placed": trail["created"], "ended": trail["canceled"] or trail["replaced"],
                "trail_pct": trail["trail_percent"]},
            "hard_order": None if hard is None else {
                "placed": hard["created"], "ended": hard["canceled"] or hard["replaced"],
                "stop_price": hard["stop_price"]},
            "exit": {"t": exit_o["filled"], "px": exit_o["filled_avg_price"], "kind": kind},
            "deadline": deadline, "deadline_kind": dkind,
        })
    return trades, dict(skipped)


def merge_windows(trades, max_span=MAX_WINDOW_S):
    """Group each symbol's trades into overlapping tick windows so overlapping trades
    share one fetch. A trade's window runs to tr['window_end'] when set (variants that
    hold longer). Returns [(symbol, start, end, [trades])] ordered by symbol, time."""
    by_sym = defaultdict(list)
    for tr in trades:
        by_sym[tr["symbol"]].append(tr)
    out = []
    for sym in sorted(by_sym):
        cur = None
        for tr in sorted(by_sym[sym], key=lambda x: x["fill_t"]):
            s = tr["fill_t"] - 5
            e = max(tr["fill_t"] + WINDOW_AFTER_S, tr["exit"]["t"] + 90, tr.get("window_end") or 0)
            if cur and s <= cur[2] and e - cur[1] <= max_span:
                cur[2] = max(cur[2], e)
                cur[3].append(tr)
            else:
                cur = [sym, s, e, [tr]]
                out.append(cur)
    return [tuple(w) for w in out]


# ── pure: score a run ──────────────────────────────────────────────────────────

def evaluate(trades, ticks_for, variants):
    """ticks_for(trade) → (t, p, half_spread_px) or None. Returns the run summary."""
    rows, resid = [], []
    for tr in trades:
        tk = ticks_for(tr)
        # No ticks in this trade's own slice (a halt, or a window whose prints all
        # belong to a neighbouring trade): skip it like a missing window. replay_policy
        # returns None on an empty slice, and run() below would fail the whole run.
        if tk is None or len(tk[0]) == 0:
            continue
        t, p, hs = tk
        tr["half_spread"] = hs
        # 1. Match check: the real protective orders, replayed.
        real = [dict(kind="trail", **tr["trail_order"])] if tr["trail_order"] else []
        if tr["hard_order"]:
            real.append(dict(kind="stop", **tr["hard_order"]))
        rep = replay_orders(t, p, tr["is_long"], real, end_t=tr["exit"]["t"] + 60) if real else None
        row = {"symbol": tr["symbol"], "strategy": tr["strategy"], "band": tr["band"], "day": tr["day"],
               "fill_t": tr["fill_t"], "actual_kind": tr["exit"]["kind"], "actual_t": tr["exit"]["t"],
               "actual_px": tr["exit"]["px"], "actual_pnl": round(trade_pnl(tr, tr["exit"]["px"]), 4),
               "half_spread": hs}
        if rep and tr["exit"]["kind"] in ("trail", "stop"):
            fill_px = _fill(rep[1], tr["is_long"], hs, 0.0)
            dt = rep[0] - tr["exit"]["t"]
            dpx = (fill_px - tr["exit"]["px"]) / tr["exit"]["px"] * 100
            matched = abs(dt) <= MATCH_DT_S and abs(dpx) <= MATCH_DPX_PCT
            row.update(replay_t=rep[0], replay_px=round(fill_px, 4), dt=round(dt, 2), dpx=round(dpx, 4), matched=matched)
            if abs(dt) <= MATCH_DT_S:
                # adverse slippage beyond the quoted spread, in % of price
                resid.append(dpx if tr["is_long"] else -dpx)
        rows.append((tr, t, p, row))
    slip = float(np.median(resid)) if resid else 0.0
    slip = max(0.0, slip)

    def run(tr, t, p, pol):
        ex = replay_policy(t, p, tr["is_long"], tr["fill_t"], pol)
        s = slip if ex[2] in ("trail", "stop") else 0.0
        return trade_pnl(tr, _fill(ex[1], tr["is_long"], tr["half_spread"], s))

    by_band = defaultdict(lambda: {"trades": 0, "actual": 0.0, "as_configured": 0.0})
    by_strat = defaultdict(lambda: {"trades": 0, "actual": 0.0, "as_configured": 0.0})
    vres = [{"trail": v.get("trail"), "trigger": v.get("trigger"), "stop": v.get("stop"),
             "max_hold": v.get("max_hold"), "trades": 0, "wins": 0,
             "by_band": defaultdict(float), "by_strategy": defaultdict(float), "total": 0.0} for v in variants]
    out_rows = []
    for tr, t, p, row in rows:
        b, s = by_band[tr["band"]], by_strat[tr["strategy"]]
        r = tr["rule"]
        cfg_trail = (tr["trail_order"] or {}).get("trail_pct") or r["trail"]
        pnl_cfg = run(tr, t, p, policy_for(tr, cfg_trail, r["trigger"], r["stop"]))
        for x in (b, s):
            x["trades"] += 1
            x["actual"] += row["actual_pnl"]
            x["as_configured"] += pnl_cfg
        row["as_configured_pnl"] = round(pnl_cfg, 4)
        for v, vr in zip(variants, vres):
            pv = run(tr, t, p, variant_policy(tr, v, cfg_trail))
            vr["trades"] += 1
            vr["wins"] += pv > 0
            vr["by_band"][tr["band"]] += pv
            vr["by_strategy"][tr["strategy"]] += pv
            vr["total"] += pv
        out_rows.append(row)

    checked = [r for r in out_rows if "matched" in r]
    match_share = (sum(r["matched"] for r in checked) / len(checked)) if checked else 0.0
    gaps = {k: (abs(v["as_configured"] - v["actual"]) / abs(v["actual"]) if v["actual"] else None)
            for k, v in by_band.items()}
    big = {k: g for k, g in gaps.items() if g is not None and g > TRUST_MAX_GAP and by_band[k]["trades"] >= 30}
    reasons = []
    if match_share < TRUST_MIN_MATCH:
        reasons.append(f"only {match_share:.0%} of stop exits replayed within {MATCH_DT_S:.0f} s and "
                       f"{MATCH_DPX_PCT}% (need {TRUST_MIN_MATCH:.0%})")
    for k, g in sorted(big.items()):
        reasons.append(f"{k}: as-configured P&L is {g:.0%} off actual (limit {TRUST_MAX_GAP:.0%})")
    rnd = lambda x: round(x, 2)
    return {
        "trades": len(out_rows),
        "match": {"stop_exits_checked": len(checked), "matched_share": round(match_share, 3),
                  "median_dt_s": round(float(np.median([r["dt"] for r in checked])), 2) if checked else None,
                  "median_dpx_pct": round(float(np.median([r["dpx"] for r in checked])), 4) if checked else None,
                  "slippage_pct": round(slip, 4),
                  "median_half_spread_pct": round(float(np.median(
                      [r["half_spread"] / r["actual_px"] * 100 for r in out_rows])), 4) if out_rows else None},
        "trusted": not reasons, "reasons": reasons,
        "by_band": {k: {"trades": v["trades"], "actual": rnd(v["actual"]), "as_configured": rnd(v["as_configured"])}
                    for k, v in sorted(by_band.items())},
        "by_strategy": {k: {"trades": v["trades"], "actual": rnd(v["actual"]), "as_configured": rnd(v["as_configured"])}
                        for k, v in sorted(by_strat.items())},
        # pairing sanity: trades whose paired exit came over 30 min after the entry
        "long_holds": sum(1 for tr, *_ in rows if tr["exit"]["t"] - tr["fill_t"] > 1800),
        "variants": [{"trail": v["trail"], "trigger": v["trigger"], "stop": v["stop"], "max_hold": v["max_hold"],
                      "trades": v["trades"], "wins": v["wins"], "total": rnd(v["total"]),
                      "by_band": {k: rnd(x) for k, x in sorted(v["by_band"].items())},
                      "by_strategy": {k: rnd(x) for k, x in sorted(v["by_strategy"].items())}} for v in vres],
        "rows": out_rows,
    }


# ── data access (Alpaca; runs on the server) ──────────────────────────────────

def _app():
    import app as _a
    return _a


def _rules():
    a = _app()
    out = {}
    conn = a.get_db()
    try:
        for row in conn.execute("SELECT name, nodes FROM routing_rules WHERE enabled=1").fetchall():
            name, nodes = (row[0] or "").upper(), json.loads(row[1] or "[]")
            for nd in nodes:
                if nd.get("type") == "exit_params" and name not in out:
                    num = lambda x: float(x) if x not in (None, "") else None
                    out[name] = {"trail": num(nd.get("trail_offset")), "trigger": num(nd.get("trail_trigger")) or 0.0,
                                 "stop": num(nd.get("stop_loss")), "max_hold": num(nd.get("max_hold_mins"))}
    finally:
        conn.close()
    return out


def _orders(broker, from_date, to_date):
    from alpaca.trading.requests import GetOrdersRequest
    from alpaca.trading.enums import QueryOrderStatus
    from brokers.alpaca_broker import _retry_rate_limited
    after = datetime.fromisoformat(from_date).replace(tzinfo=_ET) - timedelta(days=1)
    until = datetime.fromisoformat(to_date).replace(tzinfo=_ET) + timedelta(days=2)
    out, seen = [], set()
    for _ in range(400):
        req = GetOrdersRequest(status=QueryOrderStatus.ALL, limit=500, after=after, until=until, nested=False)
        page = _retry_rate_limited(lambda: broker._trading.get_orders(filter=req), what="replay orders", attempts=4)
        if not page:
            break
        oldest = None
        for o in page:
            if str(o.id) in seen:
                continue
            seen.add(str(o.id))
            out.append(normalize_order(o))
            c = o.submitted_at or o.created_at
            oldest = c if oldest is None or c < oldest else oldest
        if len(page) < 500 or oldest is None:
            break
        until = oldest
    return out


def _data_client():
    a = _app()
    from alpaca.data.historical import StockHistoricalDataClient
    return StockHistoricalDataClient(api_key=a.alpaca_broker._key, secret_key=a.alpaca_broker._secret, raw_data=True)


def _iso_epochs(stamps):
    arr = np.array([s.rstrip("Z") for s in stamps], dtype="datetime64[ns]")
    return arr.astype("int64") / 1e9


def _ticks(client, symbol, start, end, beat=None):
    """Last-sale-eligible trade ticks in [start, end], fetched TICK_CHUNK_S at a time so
    only one chunk's raw dicts are in memory at once."""
    from alpaca.data.requests import StockTradesRequest
    a = _app()
    ts, ps = [], []
    s = start
    while s < end:
        e = min(end, s + TICK_CHUNK_S)
        req = StockTradesRequest(symbol_or_symbols=symbol, start=datetime.fromtimestamp(s, timezone.utc),
                                 end=datetime.fromtimestamp(e, timezone.utc), feed=a._alpaca_data_feed())
        raw = _retry(lambda: client.get_stock_trades(req))
        rows = raw.get(symbol, []) if isinstance(raw, dict) else []
        rows = [r for r in rows if not (set(r.get("c") or []) & NON_LAST_SALE)]
        if rows:
            ts.append(_iso_epochs([r["t"] for r in rows]))
            ps.append(np.array([float(r["p"]) for r in rows]))
        del raw, rows
        s = e
        if beat:
            beat()
    if not ts:
        return np.array([]), np.array([])
    t, p = np.concatenate(ts), np.concatenate(ps)
    order = np.argsort(t, kind="stable")
    return t[order], p[order]


def _half_spread(client, symbol, at):
    from alpaca.data.requests import StockQuotesRequest
    a = _app()
    try:
        req = StockQuotesRequest(symbol_or_symbols=symbol, start=datetime.fromtimestamp(at - 5, timezone.utc),
                                 end=datetime.fromtimestamp(at + 25, timezone.utc), feed=a._alpaca_data_feed())
        raw = client.get_stock_quotes(req)
        q = raw.get(symbol, []) if isinstance(raw, dict) else []
        sp = [(float(x["ap"]) - float(x["bp"])) / 2 for x in q
              if x.get("ap") and x.get("bp") and float(x["ap"]) >= float(x["bp"]) > 0]
        return float(np.median(sp)) if sp else 0.0
    except Exception as e:
        log.debug("replay quotes %s: %s", symbol, e)
        return 0.0


# ── the job ────────────────────────────────────────────────────────────────────

_job_lock = threading.Lock()
_progress = {}


def _market_open(now=None):
    now = now or datetime.now(_ET)
    return now.weekday() < 5 and (9, 0) <= (now.hour, now.minute) < (16, 30)


def _ensure_table():
    a = _app()
    conn = a.get_db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS exit_replay_runs (
            id TEXT PRIMARY KEY, created_at TEXT, params TEXT, status TEXT,
            progress TEXT, result TEXT)""")
        conn.commit()
    finally:
        conn.close()
    conn = a.get_db()
    try:                                     # heartbeat column, added after the first release
        conn.execute("ALTER TABLE exit_replay_runs ADD COLUMN updated_at TEXT")
        conn.commit()
    except Exception:
        pass                                 # already there
    finally:
        conn.close()


def _save(run_id, **fields):
    a = _app()
    ph = a.placeholder()
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    conn = a.get_db()
    try:
        sets = ", ".join(f"{k}={ph}" for k in fields)
        conn.execute(f"UPDATE exit_replay_runs SET {sets} WHERE id={ph}", (*fields.values(), run_id))
        conn.commit()
    finally:
        conn.close()


def _reap_dead_runs():
    """Mark 'running' runs with no heartbeat for HEARTBEAT_STALE_S as failed (the worker
    that ran them restarted). Returns the ids still genuinely running."""
    a = _app()
    conn = a.get_db()
    try:
        rows = conn.execute("SELECT id, created_at, updated_at FROM exit_replay_runs "
                            "WHERE status='running'").fetchall()
    finally:
        conn.close()
    now, alive = time.time(), []
    for rid, created, updated in rows:
        last = _ts(updated) or _ts(created) or 0
        if now - last < HEARTBEAT_STALE_S:
            alive.append(rid)
        else:
            _save(rid, status="failed", progress="stopped without finishing (no progress for "
                                                 f"{HEARTBEAT_STALE_S // 60} min; the server likely restarted)")
    return alive


def run_replay(run_id, account, from_date, to_date, variants, strategies=None):
    """Background body: fetch, replay, score, save. Stops if the market opens."""
    a = _app()
    try:
        rec = a.ACCOUNTS_BY_NUM.get(str(account))
        if not rec or rec.get("broker") is None:
            raise RuntimeError(f"account {account} not configured")
        _progress[run_id] = "reading orders"
        _save(run_id, progress=_progress[run_id])
        # Pair over every order (so one strategy can't take another's exit), then keep the asked-for ones.
        trades, skipped = build_trades(_orders(rec["broker"], from_date, to_date), _rules(), from_date, to_date)
        if strategies:
            keep = set(strategies)
            trades = [tr for tr in trades if tr["strategy"] in keep]
        client = _data_client()
        # Variants that hold past the rule's max hold need ticks up to their own deadline.
        holds = [v.get("max_hold") for v in variants if v.get("max_hold") is not None]
        for tr in trades:
            ends = [variant_deadline(tr, h)[0] for h in holds]
            tr["window_end"] = max([tr["deadline"], tr["exit"]["t"]] + ends) + 90
        long_mode = any(tr["window_end"] - tr["fill_t"] > WINDOW_AFTER_S + 120 for tr in trades)
        windows = merge_windows(trades, LONG_WINDOW_S if long_mode else MAX_WINDOW_S)
        win_of = {id(tr): w for w in windows for tr in w[3]}
        tick_cache, done, last_beat = {}, [0], [0.0]

        def beat(force=False):
            if force or time.time() - last_beat[0] > 30:
                last_beat[0] = time.time()
                _progress[run_id] = f"replayed {done[0]} of {len(trades)} trades"
                _save(run_id, progress=_progress[run_id])

        def ticks_for(tr):
            sym, s, e, _members = win_of[id(tr)]
            key = (sym, s)
            if key not in tick_cache:
                if _market_open():
                    raise RuntimeError("market opened — replay stopped")
                tick_cache.clear()                # one window in memory at a time
                tick_cache[key] = _ticks(client, sym, s, e, beat=beat)
            t, p = tick_cache[key]
            done[0] += 1
            beat()
            if len(t) == 0:
                return None
            i0 = np.searchsorted(t, tr["fill_t"] - 5)
            i1 = np.searchsorted(t, tr.get("window_end") or (max(tr["deadline"], tr["exit"]["t"]) + 90))
            return t[i0:i1], p[i0:i1], _half_spread(client, sym, tr["fill_t"])

        # Walk trades window by window so each window's ticks are fetched once.
        ordered = [tr for w in windows for tr in w[3]]
        res = evaluate(ordered, ticks_for, variants)
        res["skipped"] = skipped
        res["no_ticks"] = len(trades) - res["trades"]
        res["params"] = {"account": str(account), "from": from_date, "to": to_date,
                         "strategies": sorted(strategies) if strategies else None}
        _save(run_id, status="done", progress="done", result=json.dumps(res))
    except Exception as e:
        log.warning("exit replay %s failed: %s", run_id, e, exc_info=True)
        _save(run_id, status="failed", progress=str(e)[:300])
    finally:
        _progress.pop(run_id, None)
        if _job_lock.locked():
            _job_lock.release()


def _retry(fn, attempts=3):
    from brokers.alpaca_broker import _retry_rate_limited
    return _retry_rate_limited(fn, what="replay ticks", attempts=attempts)


# ── endpoints ──────────────────────────────────────────────────────────────────

def _parse_variants(raw):
    out = []
    num = lambda x: float(x) if x not in (None, "") else None
    for v in raw or []:
        try:
            trail, trigger, stop = num(v.get("trail")), num(v.get("trigger")), num(v.get("stop"))
            mh = v.get("max_hold")
            max_hold = int(mh) if mh not in (None, "") else None
        except (AttributeError, TypeError, ValueError):
            raise ValueError("variant fields trail, trigger, stop and max_hold must be numbers")
        if trail is None and max_hold is None:
            raise ValueError("each variant needs a trail or a max_hold (trail left out = each trade's own)")
        if (trail is not None and not (0 < trail <= 5)) or (trigger is not None and not (0 <= trigger <= 5)) \
                or (stop is not None and not (0 < stop <= 5)):
            raise ValueError("trail, trigger and stop are % of price, between 0 and 5")
        if max_hold is not None and not (0 <= max_hold <= 390):
            raise ValueError("max_hold is minutes, 0 (no max hold: held to the EOD flatten) to 390")
        if trail is not None and trigger is None:
            trigger = 0.0
        out.append({"trail": trail, "trigger": trigger, "stop": stop, "max_hold": max_hold})
    if not out:
        raise ValueError("give at least one variant")
    if len(out) > MAX_VARIANTS:
        raise ValueError(f"at most {MAX_VARIANTS} variants per run")
    return out


@exit_replay_bp.route("/api/exit_replay/run", methods=["POST"])
def api_run():
    body = request.get_json(silent=True) or {}
    account = str(body.get("account") or "").strip()
    try:
        d0 = datetime.fromisoformat(body.get("from") or "").date()
        d1 = datetime.fromisoformat(body.get("to") or "").date()
    except ValueError:
        return jsonify({"error": "from and to must be YYYY-MM-DD"}), 400
    if d1 < d0 or (d1 - d0).days > MAX_RUN_DAYS:
        return jsonify({"error": f"to must be on or after from, at most {MAX_RUN_DAYS} days apart"}), 400
    try:
        variants = _parse_variants(body.get("variants"))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    raw = body.get("strategies") or []
    if not isinstance(raw, list) or len(raw) > MAX_STRATEGIES or not all(
            isinstance(x, str) and STRATEGY_NAME_RE.match(x.strip()) for x in raw):
        return jsonify({"error": f"strategies must be a list of up to {MAX_STRATEGIES} strategy names"}), 400
    strategies = sorted({x.strip().upper() for x in raw}) or None
    a = _app()
    if account not in a.ACCOUNTS_BY_NUM:
        return jsonify({"error": f"unknown account {account}"}), 400
    if _market_open():
        return jsonify({"error": "The replay is heavy and this server routes live orders, so it only "
                                 "runs while the market is closed (before 09:00 or after 16:30 ET, "
                                 "and weekends)."}), 423
    _ensure_table()
    if _reap_dead_runs() or not _job_lock.acquire(blocking=False):
        return jsonify({"error": "a replay is already running"}), 409
    run_id = uuid.uuid4().hex[:12]
    params = {"account": account, "from": d0.isoformat(), "to": d1.isoformat(), "variants": variants,
              "strategies": strategies}
    conn = a.get_db()
    try:
        ph = a.placeholder()
        conn.execute(f"INSERT INTO exit_replay_runs (id, created_at, params, status, progress, result) "
                     f"VALUES ({ph},{ph},{ph},{ph},{ph},{ph})",
                     (run_id, datetime.now(timezone.utc).isoformat(), json.dumps(params), "running", "starting", None))
        conn.commit()
    finally:
        conn.close()
    _save(run_id, progress="starting")           # first heartbeat
    threading.Thread(target=run_replay, args=(run_id, account, d0.isoformat(), d1.isoformat(), variants, strategies),
                     daemon=True, name=f"exit-replay-{run_id}").start()
    return jsonify({"run_id": run_id, "status": "running"})


@exit_replay_bp.route("/api/exit_replay/runs", methods=["GET"])
def api_runs():
    _ensure_table()
    _reap_dead_runs()
    a = _app()
    conn = a.get_db()
    try:
        rows = conn.execute("SELECT id, created_at, params, status, progress, result FROM exit_replay_runs "
                            "ORDER BY created_at DESC LIMIT 30").fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        res = json.loads(r[5]) if r[5] else None
        out.append({"id": r[0], "created_at": r[1], "params": json.loads(r[2] or "{}"), "status": r[3],
                    "progress": r[4], "trusted": res.get("trusted") if res else None,
                    "trades": res.get("trades") if res else None})
    return jsonify({"runs": out})


@exit_replay_bp.route("/api/exit_replay/runs/<run_id>", methods=["GET"])
def api_run_detail(run_id):
    _ensure_table()
    a = _app()
    ph = a.placeholder()
    conn = a.get_db()
    try:
        r = conn.execute(f"SELECT id, created_at, params, status, progress, result FROM exit_replay_runs "
                         f"WHERE id={ph}", (run_id,)).fetchone()
    finally:
        conn.close()
    if not r:
        return jsonify({"error": "no such run"}), 404
    res = json.loads(r[5]) if r[5] else None
    if res and request.args.get("rows") != "1":
        res.pop("rows", None)
    return jsonify({"id": r[0], "created_at": r[1], "params": json.loads(r[2] or "{}"), "status": r[3],
                    "progress": _progress.get(r[0], r[4]), "result": res})
