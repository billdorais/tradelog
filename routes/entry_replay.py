"""Entry replay: the same Camarilla setups entered different ways, on SIP trade ticks.

Why this exists: TV Farm (bar-close entries) and Kairos Farm (engine entries on the
pierce) lose about the same per trade, but on setups both took the engine is ~0.04%
better, while the setups only the engine took lose twice as much (likely pierces that
never closed past the level). The bar-based entry tests (/api/simulate/entry_test,
/reversal_test) can't settle this: what happens inside the pierce bar is exactly what
5-minute bars hide, and their exit simulator is the optimistic one.

So this module rebuilds each strategy's setups from 5-minute bars + Camarilla levels,
finds the entry each timing rule would have taken on ticks, and exits every one of
them with the same live exit policy (routes.exit_replay.replay_policy: hard stop until
the trigger, then a trailing stop, max hold). Only the entry differs between variants.

Breakout variants (long at R, short at S):
  close          TV: the 5-min bar closes past the level (prior close on the other side,
                 close vs EMA8 on that bar); market order at the bar close.
  pierce         the Kairos engine: 10 s polls; enter when the last trade is past
                 level ± $0.05 while the prior completed bar closed on the other side
                 and passed the EMA check.
  pierce_abort   pierce, but if that 5-min bar closes back inside the level, exit at
                 its close (keeps the early price, cuts false breakouts).
  pierce_hold30 / pierce_hold60
                 pierce, but only once price has stayed past the trigger at every poll
                 for 30 / 60 s.
  pierce_pct05 / pierce_pct10
                 pierce with the trigger at level ± 0.05% / 0.10% of price instead of
                 $0.05 (the same $0.05 is 0.025% of a $200 stock and 0.007% of a $700 one).
  two_closes     two consecutive closes past the level; enter at the second close.
  close_retest   after a close past the level, a limit order back at the level for
                 3 bars (fills only if a trade prints through it).
Reversal variants (long bounces off S, short rejects at R):
  reject         live: a completed bar wicks into the level and closes back out, wick
                 ≥ 0.25·ATR(14), close vs EMA8 and EMA slope; enter at the close.
  touch          a limit at the level on the first touch (prior bar on the right side
                 and passing the EMA check).
  reject_retest  a reject, then a limit back at the level within 4 bars.
  double / double_close
                 double bottom (top): a first test of the level that closes back out,
                 a rally of ≥ 0.5·ATR (the neckline), a second test within 12 bars that
                 holds within 0.25·ATR of the first low; enter on the first trade
                 through the neckline (double) or the first close above it
                 (double_close), within 6 bars. No EMA filter: the pattern is the filter.

Every variant: entries 09:35–15:45 ET, one position at a time per setup, a 25-minute
cooldown from each entry (the engine's ENGINE_COOLDOWN_MINS) applied to all variants
so trade counts differ only by entry rule. Market entries fill at the quote (half the
median spread) after a 1 s latency (0.5 s after a pierce poll); limit entries fill at
the level only when a trade prints through it by a cent.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np
from flask import Blueprint, jsonify, request

from routes import exit_replay as er

log = logging.getLogger(__name__)
entry_replay_bp = Blueprint("entry_replay", __name__)
_ET = er._ET

BAR_S = 300
POLL_S = 10.0                # the engine polls the last trade every 10 s
POLL_FILL_S = 0.5            # poll → market order fill
CLOSE_LAT_S = 1.0            # bar close → market order fill
ENGINE_BUFFER = 0.05         # $ past the level (ENGINE_PILOT_BUFFER)
COOLDOWN_MIN = 25            # ENGINE_COOLDOWN_MINS, applied to every variant
ATR_MULT = 0.25              # reject wick ≥ this × ATR(14)
RETEST_BARS_BO = 3
RETEST_BARS_REV = 4
DOUBLE_MAX_GAP = 12          # second test within this many bars of the first
DOUBLE_BREAK_BARS = 6        # neckline break within this many bars of the second test
DOUBLE_RALLY_ATR = 0.5
FIRST_ENTRY_MIN = 9 * 60 + 35
LAST_ENTRY_MIN = 15 * 60 + 45
MAX_RUN_DAYS = 31
MAX_STRATEGIES = 60

BREAKOUT_VARIANTS = ("close", "pierce", "pierce_abort", "pierce_hold30", "pierce_hold60",
                     "pierce_pct05", "pierce_pct10", "two_closes", "close_retest")
REVERSAL_VARIANTS = ("reject", "touch", "reject_retest", "double", "double_close")
_PIERCE = {"pierce": dict(buf="usd", hold=0, abort=False),
           "pierce_abort": dict(buf="usd", hold=0, abort=True),
           "pierce_hold30": dict(buf="usd", hold=30, abort=False),
           "pierce_hold60": dict(buf="usd", hold=60, abort=False),
           "pierce_pct05": dict(buf=0.05, hold=0, abort=False),
           "pierce_pct10": dict(buf=0.10, hold=0, abort=False)}


# ── pure: bars and the long-space mirror ──────────────────────────────────────

class Bars:
    """5-min bars as arrays. `mirror()` flips a short setup into long space (prices
    negated, high/low swapped) so one set of long rules serves both sides."""

    def __init__(self, bt, o, h, l, c, ema, ema2, atr):
        self.bt, self.o, self.h, self.l, self.c = bt, o, h, l, c
        self.ema, self.ema2, self.atr = ema, ema2, atr

    @classmethod
    def from_objs(cls, objs):
        f = lambda k: np.array([np.nan if getattr(b, k, None) is None else float(getattr(b, k)) for b in objs])
        bt = np.array([b.timestamp.timestamp() for b in objs], dtype=float)
        return cls(bt, f("open"), f("high"), f("low"), f("close"), f("ema"), f("ema2"), f("atr"))

    def mirror(self):
        return Bars(self.bt, -self.o, -self.l, -self.h, -self.c, -self.ema, -self.ema2, self.atr)

    def __len__(self):
        return len(self.bt)


def _minute_et(ts):
    d = datetime.fromtimestamp(ts, _ET)
    return d.hour * 60 + d.minute


def _first_tick(t, p, at, cond=None):
    """Index of the first tick at/after `at` (optionally also satisfying cond(p)), or -1."""
    i = int(np.searchsorted(t, at, side="left"))
    if cond is None:
        return i if i < len(t) else -1
    seg = cond(p[i:])
    k = er._first(seg)
    return i + k if k >= 0 else -1


def _mk(t_entry, px, ref, bar_i, how, **flags):
    return {"t": float(t_entry), "px": float(px), "ref": float(ref), "bar": int(bar_i), "how": how, **flags}


def find_breakout(b, t, p, L, variant, start_t, real_level=None):
    """First entry at/after start_t for a LONG breakout of L in long space (b, p already
    mirrored for shorts). Returns an entry dict (prices in long space) or None.
    real_level: |L| in real prices, for the percent trigger."""
    n = len(b)
    lvl_real = abs(real_level if real_level is not None else L)
    for i in range(1, n):
        close_t = b.bt[i] + BAR_S
        if close_t <= start_t - BAR_S:          # bar entirely before start
            continue
        if not (b.c[i - 1] <= L):               # fresh: prior completed bar on the other side
            continue
        if variant in ("close", "two_closes", "close_retest"):
            if not (b.c[i] > L and b.c[i] > b.ema[i]):
                continue
            if variant == "close":
                if close_t < start_t:
                    continue
                j = _first_tick(t, p, close_t + CLOSE_LAT_S)
                if j >= 0:
                    return _mk(t[j], p[j], b.c[i], i, "market", closed_back=False)
                continue
            if variant == "two_closes":
                if i + 1 < n and b.c[i + 1] > L and b.bt[i + 1] + BAR_S >= start_t:
                    j = _first_tick(t, p, b.bt[i + 1] + BAR_S + CLOSE_LAT_S)
                    if j >= 0:
                        return _mk(t[j], p[j], b.c[i + 1], i + 1, "market", closed_back=False)
                continue
            # close_retest: limit at L from the break close, filled when a trade prints through it
            lo = max(close_t, start_t)
            hi = b.bt[min(n - 1, i + RETEST_BARS_BO)] + BAR_S
            j = _first_tick(t, p, lo, lambda s: s <= L - 0.01)
            if j >= 0 and t[j] <= hi:
                return _mk(t[j], L, L, i, "limit", closed_back=False)
            continue
        # pierce family (the engine): EMA check on the prior completed bar
        cfg = _PIERCE[variant]
        if not (b.c[i - 1] > b.ema[i - 1]):
            continue
        trig = L + ENGINE_BUFFER if cfg["buf"] == "usd" else L + lvl_real * cfg["buf"] / 100.0
        polls = b.bt[i] + POLL_S * np.arange(1, int(BAR_S / POLL_S) + 1)
        polls = polls[polls >= start_t]
        if polls.size == 0:
            continue
        pi = np.searchsorted(t, polls, side="right") - 1
        ok = (pi >= 0) & (t[np.clip(pi, 0, None)] >= b.bt[i] - 60)
        px = np.where(ok, p[np.clip(pi, 0, None)], -np.inf)
        k = er._first(px >= trig)
        if k < 0:
            continue
        t_fire = float(polls[k])
        if cfg["hold"]:
            need = t_fire + cfg["hold"]
            more = b.bt[i] + POLL_S * np.arange(1, int((BAR_S + cfg["hold"]) / POLL_S) + 1)
            more = more[(more >= t_fire) & (more <= need)]
            mi = np.clip(np.searchsorted(t, more, side="right") - 1, 0, None)
            if not np.all(p[mi] >= trig):
                continue          # fell back before the hold elapsed; the next bar may re-arm
            t_fire = need
        j = _first_tick(t, p, t_fire + POLL_FILL_S)
        if j < 0:
            continue
        ebar = min(n - 1, int(np.searchsorted(b.bt, t_fire, side="right") - 1))
        closed_back = bool(b.c[ebar] <= L)
        return _mk(t[j], p[j], _price_at_safe(t, p, t_fire), ebar, "market",
                   closed_back=closed_back, abort_t=(b.bt[ebar] + BAR_S + CLOSE_LAT_S) if (cfg["abort"] and closed_back) else None)
    return None


def _price_at_safe(t, p, when):
    return er._price_at(t, p, when) if len(t) else float("nan")


def _ema_rev_ok(b, i):
    return b.c[i] > b.ema[i] and (np.isnan(b.ema2[i]) or b.ema[i] > b.ema2[i])


def find_reversal(b, t, p, L, variant, start_t):
    """First entry at/after start_t for a LONG reversal (bounce off support L) in long
    space. Returns an entry dict or None."""
    n = len(b)
    if variant in ("reject", "reject_retest"):
        for i in range(1, n):
            close_t = b.bt[i] + BAR_S
            if close_t < start_t and variant == "reject":
                continue
            atr = b.atr[i] if not np.isnan(b.atr[i]) else 0.0
            if not (b.l[i] <= L < b.c[i] and (L - b.l[i]) >= atr * ATR_MULT and _ema_rev_ok(b, i)):
                continue
            if variant == "reject":
                j = _first_tick(t, p, close_t + CLOSE_LAT_S)
                if j >= 0:
                    return _mk(t[j], p[j], b.c[i], i, "market")
                continue
            lo = max(close_t, start_t)
            hi = b.bt[min(n - 1, i + RETEST_BARS_REV)] + BAR_S
            j = _first_tick(t, p, lo, lambda s: s <= L - 0.01)
            if j >= 0 and t[j] <= hi:
                return _mk(t[j], L, L, i, "limit")
        return None
    if variant == "touch":
        for i in range(1, n):
            if b.bt[i] + BAR_S < start_t:
                continue
            if not (b.c[i - 1] > L and b.l[i - 1] > L and _ema_rev_ok(b, i - 1)):
                continue
            lo = max(b.bt[i], start_t)
            j = _first_tick(t, p, lo, lambda s: s <= L - 0.01)
            if j >= 0 and t[j] < b.bt[i] + BAR_S:
                return _mk(t[j], L, L, i, "limit")
        return None
    # double bottom
    for a in range(1, n):
        if b.bt[a] + BAR_S < start_t - DOUBLE_MAX_GAP * BAR_S:
            continue
        atr = b.atr[a] if not np.isnan(b.atr[a]) else 0.0
        tol = atr * ATR_MULT
        if not (b.l[a] <= L + tol and b.c[a] > L):
            continue
        low1 = b.l[a]
        for bb in range(a + 2, min(n, a + DOUBLE_MAX_GAP + 1)):
            if np.any(b.c[a + 1:bb] < L - tol):
                break                                   # support gave way: no pattern
            neck = float(np.max(b.h[a + 1:bb]))
            if neck < low1 + DOUBLE_RALLY_ATR * atr:
                continue
            if not (b.l[bb] <= L + tol and b.l[bb] >= low1 - tol and b.c[bb] > L):
                continue
            # pattern complete at bb's close; enter on the neckline break
            t0 = max(b.bt[bb] + BAR_S, start_t)
            last = b.bt[min(n - 1, bb + DOUBLE_BREAK_BARS)] + BAR_S
            if variant == "double":
                j = _first_tick(t, p, t0, lambda s: s > neck)
                if j >= 0 and t[j] <= last:
                    return _mk(t[j], p[j], neck, bb, "market")
            else:
                for k in range(bb + 1, min(n, bb + DOUBLE_BREAK_BARS + 1)):
                    if b.c[k] < b.l[bb] - tol:
                        break
                    if b.c[k] > neck and b.bt[k] + BAR_S >= start_t:
                        j = _first_tick(t, p, b.bt[k] + BAR_S + CLOSE_LAT_S)
                        if j >= 0:
                            return _mk(t[j], p[j], b.c[k], k, "market")
                        break
            break                                       # one pattern per first test
    return None


def simulate_setup(b, t, p, level, side, kind, variant, exit_rule, half_spread,
                   cooldown_min=COOLDOWN_MIN):
    """All entries of one variant for one setup-day, each exited with the live policy.
    level/side real; kind 'breakout' | 'reversal'. Returns rows (pnl in % of entry)."""
    is_long = side == "LONG"
    mb, mp = (b, p) if is_long else (b.mirror(), -p)
    mL = level if is_long else -level
    rows, start_t = [], b.bt[0] + BAR_S
    trail, trigger = exit_rule.get("trail") or 0.1, exit_rule.get("trigger") or 0.0
    stop, max_hold = exit_rule.get("stop"), float(exit_rule.get("max_hold") or 10)
    guard = 0
    while guard < 40:
        guard += 1
        if kind == "breakout":
            e = find_breakout(mb, t, mp, mL, variant, start_t, real_level=level)
        else:
            e = find_reversal(mb, t, mp, mL, variant, start_t)
        if e is None:
            break
        m = _minute_et(e["t"])
        if m > LAST_ENTRY_MIN:
            break
        if m < FIRST_ENTRY_MIN:
            start_t = e["t"] + 1
            continue
        px = e["px"] if is_long else -e["px"]
        ref = e["ref"] if is_long else -e["ref"]
        fill = px + half_spread if (is_long and e["how"] == "market") else \
            px - half_spread if e["how"] == "market" else px
        deadline = e["t"] + max_hold * 60
        dkind = "max_hold"
        if e.get("abort_t") and e["abort_t"] < deadline:
            deadline, dkind = e["abort_t"], "abort"
        tr = {"is_long": is_long, "ref": ref, "fill_px": fill, "slow_fill": False,
              "place_delay": er.DEFAULT_PLACE_DELAY_S, "deadline": deadline, "deadline_kind": dkind, "qty": 1.0}
        i0 = int(np.searchsorted(t, e["t"], side="left"))
        i1 = int(np.searchsorted(t, deadline + 90, side="right"))
        ex = er.replay_policy(t[i0:i1], p[i0:i1], is_long, e["t"], er.policy_for(tr, trail, trigger, stop))
        if ex is None:
            break
        exit_fill = er._fill(ex[1], is_long, half_spread, 0.0)
        pnl = (exit_fill - fill) if is_long else (fill - exit_fill)
        rows.append({"variant": variant, "kind": kind, "side": side, "t": e["t"], "exit_t": ex[0],
                     "exit": ex[2], "level": level, "entry_px": round(fill, 4),
                     "pnl_pct": round(pnl / fill * 100, 4), "pre10": m < 600,
                     "bar": e["bar"], "closed_back": e.get("closed_back")})
        start_t = max(ex[0], e["t"] + cooldown_min * 60) + 1
    return rows


# ── pure: score ───────────────────────────────────────────────────────────────

def _stats(rows):
    if not rows:
        return {"trades": 0}
    x = np.array([r["pnl_pct"] for r in rows])
    days = defaultdict(float)
    for r in rows:
        days[r["day"]] += r["pnl_pct"]
    d = np.array(list(days.values()))
    t = float(d.mean() / (d.std(ddof=1) / np.sqrt(len(d)))) if len(d) > 2 and d.std(ddof=1) > 0 else None
    return {"trades": len(rows), "win_pct": round(float((x > 0).mean() * 100), 1),
            "mean_pct": round(float(x.mean()), 4), "sum_pct": round(float(x.sum()), 2),
            "day_t": round(t, 2) if t is not None else None, "days": len(d)}


def summarize(rows):
    by_var = defaultdict(list)
    for r in rows:
        by_var[r["variant"]].append(r)
    out = {}
    for v, rs in by_var.items():
        bands = defaultdict(list)
        for r in rs:
            bands[r["band"]].append(r)
        out[v] = {"all": _stats(rs),
                  "by_band": {k: _stats(x) for k, x in sorted(bands.items())},
                  "before_10": _stats([r for r in rs if r["pre10"]]),
                  "from_10": _stats([r for r in rs if not r["pre10"]])}
        if v.startswith("pierce"):
            out[v]["closed_back"] = _stats([r for r in rs if r.get("closed_back")])
            out[v]["held"] = _stats([r for r in rs if r.get("closed_back") is False])
    # pierce vs close on the same signal bar: shared setups vs pierce-only
    close_keys = {(r["strategy"], r["day"], r["side"], r["bar"]) for r in by_var.get("close", [])}
    pc = by_var.get("pierce", [])
    shared = [r for r in pc if (r["strategy"], r["day"], r["side"], r["bar"]) in close_keys]
    only = [r for r in pc if (r["strategy"], r["day"], r["side"], r["bar"]) not in close_keys]
    pierce_keys = {(r["strategy"], r["day"], r["side"], r["bar"]) for r in pc}
    cshared = [r for r in by_var.get("close", []) if (r["strategy"], r["day"], r["side"], r["bar"]) in pierce_keys]
    out["_pierce_vs_close"] = {"pierce_shared": _stats(shared), "close_shared": _stats(cshared),
                               "pierce_only": _stats(only)}
    return out


# ── data + job (server side) ──────────────────────────────────────────────────

def _bars(a, tk, day):
    return a._fetch_5m_rth_objs(tk, day)


def _levels(a, tk, day):
    return a._camarilla_levels(tk, day)


def _trading_days(from_date, to_date):
    d0 = datetime.fromisoformat(from_date).date()
    d1 = datetime.fromisoformat(to_date).date()
    out, d = [], d0
    while d <= d1:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _setups_for(strategies):
    """strategy names → {ticker: [(strategy, kind, pair)]} for CAM breakouts/reversals."""
    out = defaultdict(list)
    for s in strategies:
        u = s.upper()
        if "_CAM_" not in u:
            continue
        kind = "reversal" if "REVERSAL" in u else "breakout" if "BREAKOUT" in u else None
        pair = "R4S4" if "R4S4" in u else "R3S3" if "R3S3" in u else None
        if kind and pair:
            out[u.split("_CAM_")[0]].append((u, kind, pair))
    return out


def _windows(b, levels_used, max_hold_min):
    """Tick windows: around every bar that comes within 0.5·ATR of a level in use, long
    enough for the slowest entry rule plus the max hold. Merged; [(start, end)]."""
    iv = []
    span = BAR_S * (DOUBLE_MAX_GAP + DOUBLE_BREAK_BARS + 1) + max_hold_min * 60 + 120
    for i in range(len(b)):
        atr = 0.0 if np.isnan(b.atr[i]) else b.atr[i]
        for L in levels_used:
            if b.l[i] - 0.5 * atr <= L <= b.h[i] + 0.5 * atr:
                iv.append((b.bt[i] - 60, min(b.bt[-1] + BAR_S + max_hold_min * 60, b.bt[i] + span)))
                break
    iv.sort()
    out = []
    for s, e in iv:
        if out and s <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], e))
        else:
            out.append((s, e))
    return out


_progress = {}


def _ensure_table():
    a = er._app()
    conn = a.get_db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS entry_replay_runs (
            id TEXT PRIMARY KEY, created_at TEXT, params TEXT, status TEXT,
            progress TEXT, result TEXT, updated_at TEXT)""")
        conn.commit()
    finally:
        conn.close()


def _save(run_id, **fields):
    a = er._app()
    ph = a.placeholder()
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    conn = a.get_db()
    try:
        sets = ", ".join(f"{k}={ph}" for k in fields)
        conn.execute(f"UPDATE entry_replay_runs SET {sets} WHERE id={ph}", (*fields.values(), run_id))
        conn.commit()
    finally:
        conn.close()


def _reap_dead_runs():
    a = er._app()
    conn = a.get_db()
    try:
        rows = conn.execute("SELECT id, created_at, updated_at FROM entry_replay_runs WHERE status='running'").fetchall()
    finally:
        conn.close()
    now = time.time()
    for rid, created, updated in rows:
        if now - (er._ts(updated) or er._ts(created) or 0) >= er.HEARTBEAT_STALE_S:
            _save(rid, status="failed", progress="stopped without finishing (the server likely restarted)")


def run_entry_replay(run_id, from_date, to_date, strategies, variants):
    a = er._app()
    try:
        rules = er._rules()
        by_tk = _setups_for(strategies)
        days = _trading_days(from_date, to_date)
        client = er._data_client()
        rows, done, total = [], 0, len(by_tk) * len(days)
        last_beat = [0.0]

        def beat(force=False):
            if force or time.time() - last_beat[0] > 30:
                last_beat[0] = time.time()
                _save(run_id, progress=f"{done} of {total} ticker-days, {len(rows)} entries")
        for tk in sorted(by_tk):
            for day in days:
                if er._market_open():
                    raise RuntimeError("market opened — replay stopped")
                done += 1
                objs = _bars(a, tk, day)
                lv = _levels(a, tk, day) if objs else {}
                if not objs or not lv:
                    continue
                b = Bars.from_objs(objs)
                setups = []
                for strat, kind, pair in by_tk[tk]:
                    hi, lo = ("r4", "s4") if pair == "R4S4" else ("r3", "s3")
                    for side in ("LONG", "SHORT"):
                        key = (hi if side == "LONG" else lo) if kind == "breakout" else (lo if side == "LONG" else hi)
                        if lv.get(key):
                            setups.append((strat, kind, side, float(lv[key])))
                rule_mh = max(float((rules.get(s) or {}).get("max_hold") or 10) for s, *_ in setups) if setups else 10
                wins = _windows(b, {L for *_, L in setups}, rule_mh)
                if not wins:
                    continue
                ts, ps = [], []
                for s, e in wins:
                    tt, pp = er._ticks(client, tk, s, e, beat=beat)
                    ts.append(tt)
                    ps.append(pp)
                t = np.concatenate(ts) if ts else np.array([])
                p = np.concatenate(ps) if ps else np.array([])
                if len(t) == 0:
                    continue
                o = np.argsort(t, kind="stable")
                t, p = t[o], p[o]
                hs = er._half_spread(client, tk, wins[0][0] + 120)
                for strat, kind, side, L in setups:
                    rule = rules.get(strat) or {}
                    for v in variants:
                        if (v in BREAKOUT_VARIANTS) != (kind == "breakout"):
                            continue
                        for r in simulate_setup(b, t, p, L, side, kind, v, rule, hs):
                            r.update(strategy=strat, symbol=tk, day=day, band=er.band_of(strat))
                            rows.append(r)
                del t, p, ts, ps
                beat()
        res = {"params": {"from": from_date, "to": to_date, "strategies": sorted(strategies), "variants": variants},
               "entries": len(rows), "summary": summarize(rows), "rows": rows}
        _save(run_id, status="done", progress="done", result=json.dumps(res))
    except Exception as e:
        log.warning("entry replay %s failed: %s", run_id, e, exc_info=True)
        _save(run_id, status="failed", progress=str(e)[:300])
    finally:
        if er._job_lock.locked():
            er._job_lock.release()


@entry_replay_bp.route("/api/entry_replay/run", methods=["POST"])
def api_run():
    body = request.get_json(silent=True) or {}
    try:
        d0 = datetime.fromisoformat(body.get("from") or "").date()
        d1 = datetime.fromisoformat(body.get("to") or "").date()
    except ValueError:
        return jsonify({"error": "from and to must be YYYY-MM-DD"}), 400
    if d1 < d0 or (d1 - d0).days > MAX_RUN_DAYS:
        return jsonify({"error": f"to must be on or after from, at most {MAX_RUN_DAYS} days apart"}), 400
    raw = body.get("strategies") or []
    if not isinstance(raw, list) or not raw or len(raw) > MAX_STRATEGIES or not all(
            isinstance(x, str) and er.STRATEGY_NAME_RE.match(x.strip()) for x in raw):
        return jsonify({"error": f"strategies must be a list of 1 to {MAX_STRATEGIES} strategy names"}), 400
    strategies = sorted({x.strip().upper() for x in raw})
    if not _setups_for(strategies):
        return jsonify({"error": "no CAM breakout or reversal strategies in the list"}), 400
    allv = BREAKOUT_VARIANTS + REVERSAL_VARIANTS
    variants = body.get("variants") or list(allv)
    if not isinstance(variants, list) or any(v not in allv for v in variants):
        return jsonify({"error": f"variants must be from {', '.join(allv)}"}), 400
    if er._market_open():
        return jsonify({"error": "The replay is heavy and this server routes live orders, so it only "
                                 "runs while the market is closed (before 09:00 or after 16:30 ET, "
                                 "and weekends)."}), 423
    _ensure_table()
    er._ensure_table()
    _reap_dead_runs()
    if er._reap_dead_runs() or not er._job_lock.acquire(blocking=False):
        return jsonify({"error": "a replay (entry or exit) is already running"}), 409
    run_id = uuid.uuid4().hex[:12]
    params = {"from": d0.isoformat(), "to": d1.isoformat(), "strategies": strategies, "variants": variants}
    a = er._app()
    conn = a.get_db()
    try:
        ph = a.placeholder()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(f"INSERT INTO entry_replay_runs (id, created_at, params, status, progress, result, updated_at) "
                     f"VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph})",
                     (run_id, now, json.dumps(params), "running", "starting", None, now))
        conn.commit()
    finally:
        conn.close()
    threading.Thread(target=run_entry_replay, args=(run_id, d0.isoformat(), d1.isoformat(), strategies, variants),
                     daemon=True, name=f"entry-replay-{run_id}").start()
    return jsonify({"run_id": run_id, "status": "running"})


@entry_replay_bp.route("/api/entry_replay/runs", methods=["GET"])
def api_runs():
    _ensure_table()
    _reap_dead_runs()
    a = er._app()
    conn = a.get_db()
    try:
        rows = conn.execute("SELECT id, created_at, params, status, progress FROM entry_replay_runs "
                            "ORDER BY created_at DESC LIMIT 20").fetchall()
    finally:
        conn.close()
    return jsonify({"runs": [{"id": r[0], "created_at": r[1], "params": json.loads(r[2] or "{}"),
                              "status": r[3], "progress": r[4]} for r in rows]})


@entry_replay_bp.route("/api/entry_replay/runs/<run_id>", methods=["GET"])
def api_run_detail(run_id):
    _ensure_table()
    a = er._app()
    conn = a.get_db()
    try:
        ph = a.placeholder()
        r = conn.execute(f"SELECT id, created_at, params, status, progress, result FROM entry_replay_runs "
                         f"WHERE id={ph}", (run_id,)).fetchone()
    finally:
        conn.close()
    if not r:
        return jsonify({"error": "no such run"}), 404
    res = json.loads(r[5]) if r[5] else None
    if res and request.args.get("rows") not in ("1", "true"):
        res.pop("rows", None)
    return jsonify({"id": r[0], "created_at": r[1], "params": json.loads(r[2] or "{}"),
                    "status": r[3], "progress": r[4], "result": res})
