"""Nasdaq opening-range breakout (ORB) with a moving-average trend filter — replay only.

Rules (defaults; every one is a parameter, see DEFAULTS):
  - Opening range: the highest high and lowest low of bars starting 09:00–09:29 ET.
    Fixed for the day once 09:30 passes.
  - From 09:30, watch 5-minute candles. A candle that CLOSES above the range high is a
    long breakout, below the range low a short one. Wicks do not count. Only candles
    that close by 10:30 qualify.
  - Trend filter on 15-minute bars: long needs SMA(50) > SMA(200), short SMA(50) <
    SMA(200); equal skips. Only 15-minute bars that had COMPLETED when the breakout
    candle closed are used, so nothing from the future leaks in.
  - The first breakout is the day's only chance: if the filter rejects it the day is
    skipped (retry_after_filter=True keeps scanning instead).
  - Entry: the open of the first bar after the breakout candle closes, plus slippage.
  - Stop: the opposite side of the range. Risk = |entry − stop| from the ACTUAL fill.
  - Target: entry ± target_r × risk. Exit at target, stop, or 15:55 ET, whichever first.
    No overnight positions.

Simulation is on the finest bars supplied (1-minute recommended). When one bar touches
both the stop and the target and the order is unknown, the stop is assumed to come
first (same_bar="stop_first"), which is the conservative reading. A bar that OPENS past
the stop fills at that open (a gap), not at the stop.

R is NET of costs: realized $ / initial risk $. A trade that exits exactly at the 3R
target therefore books slightly under 3R once commission and slippage are paid.

Nothing here places orders. It only reads bars and returns numbers.
"""
from __future__ import annotations

import io
import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone

import numpy as np
import pandas as pd

ET = "America/New_York"

# Point values and tick sizes from the CME contract specs. commission is a per-side,
# per-contract all-in guess (broker + exchange + NFA); override it per run.
CONTRACTS = {
    "NQ":  {"label": "E-mini Nasdaq-100 (NQ)", "tick": 0.25, "point_value": 20.0, "commission": 2.25},
    "MNQ": {"label": "Micro E-mini Nasdaq-100 (MNQ)", "tick": 0.25, "point_value": 2.0, "commission": 0.62},
    "QQQ": {"label": "QQQ shares (stand-in for NQ)", "tick": 0.01, "point_value": 1.0, "commission": 0.0},
}

DEFAULTS = {
    "or_start": "09:00",
    "or_end": "09:30",
    "breakout_tf": 5,            # minutes
    "trend_tf": 15,              # minutes
    "fast_sma": 50,
    "slow_sma": 200,
    "target_r": 3.0,
    "stop": "opposite",          # opposite | midpoint
    "last_entry": "10:30",       # last candle CLOSE that may trigger an entry
    "forced_exit": "15:55",
    "max_trades_per_day": 1,
    "trend_filter": True,
    "retry_after_filter": False,
    "same_bar": "stop_first",    # stop_first (conservative) | target_first
    "contracts": 1,
    "slippage_ticks": 1.0,       # per market fill (entry, stop, time exit); targets are limits
    "commission": None,          # per side per contract; None = the contract's default
}

TARGET_GRID = (2.5, 3.0, 3.5, 4.0)
SKIP_REASONS = ("no_range_data", "flat_range", "no_breakout", "trend_warmup",
                "trend_filter", "trend_equal", "no_entry_bar", "entry_beyond_stop")


def _hm(s) -> int:
    """'09:30' → minutes after midnight."""
    if isinstance(s, (int, float)):
        return int(s)
    h, m = str(s).strip().split(":")
    return int(h) * 60 + int(m)


def _fmt_hm(m: int) -> str:
    return f"{m // 60:02d}:{m % 60:02d}"


def normalize_params(p: dict | None) -> dict:
    """Defaults filled in and every value checked. Raises ValueError on a bad one."""
    out = dict(DEFAULTS)
    for k, v in (p or {}).items():
        if k in out and v is not None and v != "":
            out[k] = v
    try:
        for k in ("or_start", "or_end", "last_entry", "forced_exit"):
            out[k] = _fmt_hm(_hm(out[k]))
        for k in ("breakout_tf", "trend_tf", "fast_sma", "slow_sma", "max_trades_per_day", "contracts"):
            out[k] = int(out[k])
        for k in ("target_r", "slippage_ticks"):
            out[k] = float(out[k])
        if out["commission"] is not None:
            out["commission"] = float(out["commission"])
        for k in ("trend_filter", "retry_after_filter"):
            v = out[k]
            out[k] = v if isinstance(v, bool) else str(v).strip().lower() in ("1", "true", "yes", "on")
    except (TypeError, ValueError) as e:
        raise ValueError(f"bad parameter: {e}")
    if not _hm(out["or_start"]) < _hm(out["or_end"]) < _hm(out["last_entry"]) < _hm(out["forced_exit"]):
        raise ValueError("times must run or_start < or_end < last_entry < forced_exit")
    if out["breakout_tf"] not in (1, 2, 3, 5, 10, 15, 30) or out["trend_tf"] not in (5, 10, 15, 30, 60):
        raise ValueError("breakout_tf must be 1/2/3/5/10/15/30 and trend_tf 5/10/15/30/60 minutes")
    if not 1 <= out["fast_sma"] < out["slow_sma"] <= 1000:
        raise ValueError("need 1 <= fast_sma < slow_sma <= 1000")
    if not 0.25 <= out["target_r"] <= 20:
        raise ValueError("target_r must be between 0.25 and 20")
    if out["stop"] not in ("opposite", "midpoint"):
        raise ValueError("stop must be opposite or midpoint")
    if out["same_bar"] not in ("stop_first", "target_first"):
        raise ValueError("same_bar must be stop_first or target_first")
    if not 1 <= out["max_trades_per_day"] <= 10 or not 1 <= out["contracts"] <= 10_000:
        raise ValueError("max_trades_per_day must be 1-10 and contracts 1-10000")
    if not 0 <= out["slippage_ticks"] <= 100 or (out["commission"] is not None and not 0 <= out["commission"] <= 1000):
        raise ValueError("slippage_ticks must be 0-100 and commission 0-1000")
    return out


# ── data ─────────────────────────────────────────────────────────────────────

def frame_from_bars(bars) -> pd.DataFrame:
    """Kairos bar dicts (time, open, high, low, close[, volume]) → OHLC frame. Naive
    times are taken as UTC, which is what the Alpaca fetcher returns."""
    df = pd.DataFrame(bars)
    if df.empty:
        raise ValueError("no bars")
    idx = pd.DatetimeIndex(pd.to_datetime(df.pop("time")))
    df.index = idx.tz_localize("UTC") if idx.tz is None else idx
    return df


def parse_csv(raw, tz: str = ET, filename: str = "") -> pd.DataFrame:
    """A vendor CSV of OHLC bars → frame. Accepts one datetime column (datetime,
    timestamp, time, date, ts_event, …) or separate date + time columns; epoch seconds,
    milliseconds or nanoseconds; and case-insensitive open/high/low/close[/volume].
    Naive timestamps are read in `tz`; timestamps with an offset keep it.
    Each timestamp must be the bar's START (the usual vendor convention)."""
    import csv as _csv
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    name = (filename or "").lower()
    comp = "gzip" if name.endswith(".gz") or raw[:2] == b"\x1f\x8b" else \
           "zip" if name.endswith(".zip") or raw[:2] == b"PK" else None
    if comp:
        head = pd.read_csv(io.BytesIO(raw), compression=comp, nrows=0, sep=None, engine="python")
        sep = "," if len(head.columns) > 1 else None
    else:
        sample = raw[:4096].decode("utf-8-sig", errors="replace")
        try:
            sep = _csv.Sniffer().sniff(sample.splitlines()[0] if sample else ",", delimiters=",;\t|").delimiter
        except _csv.Error:
            sep = ","
    if sep is None:
        df = pd.read_csv(io.BytesIO(raw), compression=comp, sep=None, engine="python")
    else:
        df = pd.read_csv(io.BytesIO(raw), compression=comp, sep=sep)
    cols = {c.strip().lower().lstrip("<").rstrip(">"): c for c in df.columns}
    need = {k: cols.get(k) or next((cols[c] for c in cols if c.startswith(k)), None)
            for k in ("open", "high", "low", "close")}
    if any(v is None for v in need.values()):
        raise ValueError(f"CSV needs open/high/low/close columns; found {list(df.columns)}")
    dt_col = next((cols[c] for c in ("datetime", "timestamp", "ts_event", "date_time", "time", "date")
                   if c in cols), None)
    if dt_col is None:
        raise ValueError("CSV needs a datetime/timestamp column (or date + time)")
    s = df[dt_col]
    if "date" in cols and "time" in cols and dt_col in (cols["date"], cols["time"]):
        s = df[cols["date"]].astype(str) + " " + df[cols["time"]].astype(str)
    if pd.api.types.is_numeric_dtype(s):
        v = float(pd.Series(s).abs().median())
        unit = "ns" if v > 1e17 else "us" if v > 1e14 else "ms" if v > 1e11 else "s"
        idx = pd.DatetimeIndex(pd.to_datetime(s, unit=unit, utc=True))
    else:
        try:
            idx = pd.DatetimeIndex(pd.to_datetime(s, errors="coerce", format="mixed"))
        except (ValueError, TypeError):   # offsets change at DST (-04:00 / -05:00) → via UTC
            idx = pd.DatetimeIndex(pd.to_datetime(s, utc=True, errors="coerce", format="mixed"))
        if idx.isna().mean() > 0.01:
            raise ValueError("could not read the datetime column")
        if idx.tz is None:
            idx = idx.tz_localize(tz, ambiguous="NaT", nonexistent="NaT")
    out = pd.DataFrame({k: pd.to_numeric(df[c], errors="coerce").to_numpy() for k, c in need.items()}, index=idx)
    vol = next((cols[c] for c in cols if c.startswith("vol")), None)
    if vol is not None:
        out["volume"] = pd.to_numeric(df[vol], errors="coerce").to_numpy()
    out = out[out.index.notna()]
    if out.empty:
        raise ValueError("no usable rows in the CSV")
    return out


@dataclass
class Market:
    """Bars in ET, plus resampled candles cached by size. Build once, simulate many."""
    base: pd.DataFrame
    base_min: int
    days: list = field(default_factory=list)          # trading dates in the data
    _day_rng: dict = field(default_factory=dict)      # date → (i0, i1) into base
    _cache: dict = field(default_factory=dict)

    @classmethod
    def from_frame(cls, df: pd.DataFrame) -> "Market":
        d = df[["open", "high", "low", "close"]].astype(float).copy()
        if d.index.tz is None:
            raise ValueError("bars need a timezone")
        d.index = d.index.tz_convert(ET).as_unit("ns")   # all arithmetic below is in ns
        d = d[~d.index.duplicated(keep="last")].sort_index()
        d = d.dropna()
        d = d[(d["high"] >= d["low"]) & (d["open"] > 0)]
        if len(d) < 10:
            raise ValueError("fewer than 10 usable bars")
        steps = np.diff(d.index.asi8) // 60_000_000_000
        steps = steps[(steps > 0) & (steps <= 60)]
        base_min = int(np.median(steps)) if len(steps) else 1
        if base_min not in (1, 2, 3, 5):
            raise ValueError(f"bars look like {base_min}-minute bars; supply 1, 2, 3 or 5-minute bars")
        m = cls(base=d, base_min=base_min)
        key = (d.index.year * 10000 + d.index.month * 100 + d.index.day).to_numpy()
        cuts = np.concatenate(([0], np.flatnonzero(np.diff(key)) + 1, [len(d)]))
        for a, b in zip(cuts[:-1], cuts[1:]):
            k = int(key[a])
            dd = date(k // 10000, k // 100 % 100, k % 100)
            m._day_rng[dd] = (int(a), int(b))
            m.days.append(dd)
        m._t = d.index.asi8
        m._o, m._h, m._l, m._c = (d[k].to_numpy() for k in ("open", "high", "low", "close"))
        m._mins = (d.index.hour * 60 + d.index.minute).to_numpy()
        return m

    def candles(self, minutes: int) -> dict:
        """Clock-aligned candles of `minutes`, each labelled by its START, with arrays
        start_ns, end_ns, close. A candle exists only if some base bar fell in it."""
        if minutes in self._cache:
            return self._cache[minutes]
        if minutes % self.base_min:
            raise ValueError(f"{minutes}-minute candles can't be built from {self.base_min}-minute bars")
        r = (self.base.resample(f"{minutes}min", label="left", closed="left", origin="epoch")
             .agg({"open": "first", "high": "max", "low": "min", "close": "last"}).dropna())
        r.index = r.index.as_unit("ns")
        st = r.index.asi8
        out = {"start": st, "end": st + minutes * 60_000_000_000, "close": r["close"].to_numpy(),
               "mins": (r.index.hour * 60 + r.index.minute).to_numpy()}
        self._cache[minutes] = out
        return out

    def sma(self, minutes: int, n: int) -> np.ndarray:
        key = ("sma", minutes, n)
        if key not in self._cache:
            c = self.candles(minutes)["close"]
            self._cache[key] = pd.Series(c).rolling(n, min_periods=n).mean().to_numpy()
        return self._cache[key]


# ── simulation ───────────────────────────────────────────────────────────────

def _round_tick(px, tick, up):
    q = px / tick
    return (math.ceil(q - 1e-9) if up else math.floor(q + 1e-9)) * tick


def ready_date(m: Market, params: dict) -> date | None:
    """First trading day on which the slow SMA already exists at the opening-range end,
    so a run never counts days where the trend filter could not have been read."""
    p = normalize_params(params)
    slow = m.sma(p["trend_tf"], p["slow_sma"])
    ok = np.flatnonzero(~np.isnan(slow))
    if not len(ok):
        return None
    ready_ns = int(m.candles(p["trend_tf"])["end"][ok[0]])
    or1 = _hm(p["or_end"])
    for d in m.days:
        i0, i1 = m._day_rng[d]
        j = i0 + int(np.searchsorted(m._mins[i0:i1], or1))
        if j < i1 and int(m._t[j]) >= ready_ns:
            return d
    return None


def simulate(m: Market, params: dict, contract: str = "NQ",
             start: date | None = None, end: date | None = None) -> dict:
    """Run the rules over every trading day in [start, end]. Returns {trades, skipped}
    where skipped counts days by reason (see SKIP_REASONS)."""
    p = normalize_params(params)
    spec = CONTRACTS[contract]
    tick, pv = spec["tick"], spec["point_value"]
    comm = spec["commission"] if p["commission"] is None else p["commission"]
    slip = p["slippage_ticks"] * tick
    qty = p["contracts"]
    or0, or1 = _hm(p["or_start"]), _hm(p["or_end"])
    last_entry, fexit = _hm(p["last_entry"]), _hm(p["forced_exit"])
    bk = m.candles(p["breakout_tf"])
    tr = m.candles(p["trend_tf"])
    fast, slow = m.sma(p["trend_tf"], p["fast_sma"]), m.sma(p["trend_tf"], p["slow_sma"])
    min_or_bars = max(1, math.ceil(0.5 * (or1 - or0) / m.base_min))
    NS = 60_000_000_000
    trades, skipped = [], {k: 0 for k in SKIP_REASONS}
    t, o, h, l, c, mins = m._t, m._o, m._h, m._l, m._c, m._mins

    for d in m.days:
        if (start and d < start) or (end and d > end) or d.weekday() >= 5:
            continue
        i0, i1 = m._day_rng[d]
        dm = mins[i0:i1]
        in_or = np.nonzero((dm >= or0) & (dm < or1))[0]
        if len(in_or) < min_or_bars:
            skipped["no_range_data"] += 1
            continue
        hi, lo = float(h[i0 + in_or].max()), float(l[i0 + in_or].min())
        if hi <= lo:
            skipped["flat_range"] += 1
            continue
        # Breakout candles: start at/after the range end, CLOSE no later than last_entry.
        ca = np.searchsorted(bk["start"], t[i0])
        cb = np.searchsorted(bk["start"], t[i1 - 1], side="right")
        cand = [k for k in range(ca, cb)
                if bk["mins"][k] >= or1 and bk["mins"][k] + p["breakout_tf"] <= last_entry]
        n_today, after_ns, outcome = 0, None, "no_breakout"
        for k in cand:
            if after_ns is not None and bk["start"][k] < after_ns:
                continue
            cl = bk["close"][k]
            side = 1 if cl > hi else -1 if cl < lo else 0
            if not side:
                continue
            decide = int(bk["end"][k])
            # Last trend candle COMPLETED by the decision time (its end <= decide).
            j = np.searchsorted(tr["end"], decide, side="right") - 1
            f_, s_ = (fast[j], slow[j]) if j >= 0 else (np.nan, np.nan)
            trend = None if (np.isnan(f_) or np.isnan(s_)) else (1 if f_ > s_ else -1 if f_ < s_ else 0)
            if p["trend_filter"]:
                if trend is None:
                    outcome = "trend_warmup"
                    break
                if trend != side:
                    outcome = "trend_equal" if trend == 0 else "trend_filter"
                    if p["retry_after_filter"]:
                        continue
                    break
            # Entry: first base bar starting at/after the candle close, before the forced exit.
            e = i0 + int(np.searchsorted(t[i0:i1], decide))
            if e >= i1 or mins[e] >= fexit:
                outcome = "no_entry_bar"
                break
            entry = _round_tick(o[e] + side * slip, tick, up=side > 0)
            if p["stop"] == "opposite":
                stop = lo if side > 0 else hi
            else:
                stop = _round_tick((hi + lo) / 2, tick, up=side < 0)
            risk = (entry - stop) * side
            if risk <= 0:
                outcome = "entry_beyond_stop"
                break
            target = _round_tick(entry + side * p["target_r"] * risk, tick, up=side > 0)
            # Walk forward from the entry bar to the forced exit.
            x_end = i0 + int(np.searchsorted(mins[i0:i1], fexit))
            x_end = max(x_end, e)
            hh, ll, oo = h[e:x_end], l[e:x_end], o[e:x_end]
            if side > 0:
                hit_s, hit_t = ll <= stop, hh >= target
            else:
                hit_s, hit_t = hh >= stop, ll <= target
            any_hit = np.nonzero(hit_s | hit_t)[0]
            if len(any_hit):
                q = int(any_hit[0])
                xi = e + q
                both = bool(hit_s[q] and hit_t[q])
                stop_wins = hit_s[q] and (not hit_t[q] or p["same_bar"] == "stop_first")
                if stop_wins:
                    gapped = q > 0 and (oo[q] - stop) * side <= 0
                    base_px = oo[q] if gapped else stop
                    exit_px = _round_tick(base_px - side * slip, tick, up=side < 0)
                    reason = "stop (gap)" if gapped else ("stop (same bar as target)" if both else "stop")
                else:
                    exit_px = target
                    reason = "target (same bar as stop)" if both else "target"
                exit_ns = int(t[xi])
            else:
                fx = i0 + int(np.searchsorted(mins[i0:i1], fexit))
                if fx < i1 and mins[fx] < fexit + max(5, m.base_min):
                    exit_px = _round_tick(o[fx] - side * slip, tick, up=side < 0)
                    exit_ns, reason = int(t[fx]), "time"
                else:
                    last = x_end - 1
                    exit_px = _round_tick(c[last] - side * slip, tick, up=side < 0)
                    exit_ns, reason = int(t[last]) + m.base_min * NS, "session end"
            pts = (exit_px - entry) * side
            risk_usd = risk * pv * qty
            costs = 2 * comm * qty
            pnl = pts * pv * qty - costs
            trades.append({
                "date": d.isoformat(), "side": "long" if side > 0 else "short",
                "or_high": hi, "or_low": lo,
                "signal_time": _ns_hm(decide - p["breakout_tf"] * NS), "entry_time": _ns_hm(int(t[e])),
                "entry": round(entry, 4), "stop": round(stop, 4), "target": round(target, 4),
                "exit_time": _ns_hm(exit_ns), "exit": round(exit_px, 4), "reason": reason,
                "risk_pts": round(risk, 4), "pts": round(pts, 4),
                "r_gross": round(pts / risk, 4), "r": round(pnl / risk_usd, 4),
                "pnl": round(pnl, 2), "costs": round(costs, 2), "trend": trend,
            })
            n_today += 1
            outcome = "traded"
            if n_today >= p["max_trades_per_day"]:
                break
            after_ns = exit_ns
        if outcome != "traded" and not n_today:
            skipped[outcome] += 1
    return {"trades": trades, "skipped": skipped}


def _ns_hm(ns: int) -> str:
    return pd.Timestamp(ns, tz="UTC").tz_convert(ET).strftime("%H:%M")


# ── statistics ───────────────────────────────────────────────────────────────

def _max_dd(rs) -> float:
    peak = run = dd = 0.0
    for r in rs:
        run += r
        peak = max(peak, run)
        dd = max(dd, peak - run)
    return dd


def _longest_losing(rs) -> int:
    best = cur = 0
    for r in rs:
        cur = cur + 1 if r <= 0 else 0
        best = max(best, cur)
    return best


def summarize(trades: list) -> dict:
    rs = [t["r"] for t in trades]
    n = len(rs)
    if not n:
        return {"trades": 0}
    wins = [r for r in rs if r > 0]
    losses = [r for r in rs if r <= 0]
    mean = sum(rs) / n
    sd = float(np.std(rs, ddof=1)) if n > 1 else 0.0
    gl = -sum(losses)
    return {
        "trades": n,
        "win_rate": round(100 * len(wins) / n, 1),
        "avg_win_r": round(sum(wins) / len(wins), 3) if wins else None,
        "avg_loss_r": round(sum(losses) / len(losses), 3) if losses else None,
        "expectancy_r": round(mean, 4),
        "total_r": round(sum(rs), 2),
        "max_dd_r": round(_max_dd(rs), 2),
        "profit_factor": round(sum(wins) / gl, 3) if gl > 0 else None,
        "t_stat": round(mean / (sd / math.sqrt(n)), 2) if sd > 0 else None,
        "longest_losing": _longest_losing(rs),
        "net_pnl": round(sum(t["pnl"] for t in trades), 2),
        "avg_r_gross": round(sum(t["r_gross"] for t in trades) / n, 4),
    }


def by_side(trades):
    return {s: summarize([t for t in trades if t["side"] == s]) for s in ("long", "short")}


def by_month(trades):
    g = {}
    for t in trades:
        g.setdefault(t["date"][:7], []).append(t)
    return [{"month": k, **summarize(v)} for k, v in sorted(g.items())]


def by_reason(trades):
    g = {}
    for t in trades:
        g.setdefault(t["reason"].split(" ")[0], []).append(t["r"])
    return {k: {"trades": len(v), "total_r": round(sum(v), 2)} for k, v in sorted(g.items())}


def equity_curve(trades):
    run, out = 0.0, []
    for t in trades:
        run += t["r"]
        out.append({"date": t["date"], "r": round(run, 3)})
    return out


# ── validation ───────────────────────────────────────────────────────────────

def _pct(arr, q):
    return round(float(np.percentile(arr, q)), 2) if len(arr) else None


def monte_carlo(rs, n_iter: int = 2000, seed: int = 7) -> dict | None:
    """Two resamplings of the realized R sequence.
    reshuffle — same trades, random order: how deep the drawdown could have been
                (total R never changes under a reshuffle);
    bootstrap — trades drawn with replacement: how much the TOTAL and the per-trade
                expectancy could differ by luck of the draw."""
    arr = np.asarray(rs, dtype=float)
    n = len(arr)
    if n < 5:
        return None
    rng = np.random.default_rng(seed)
    shuf = rng.permuted(np.tile(arr, (n_iter, 1)), axis=1)
    cum = np.cumsum(shuf, axis=1)
    peak = np.maximum(np.maximum.accumulate(cum, axis=1), 0.0)
    dds = (peak - cum).max(axis=1).clip(min=0)
    py = random.Random(seed)
    streaks, seq = [], list(arr)
    for _ in range(min(n_iter, 500)):
        py.shuffle(seq)
        streaks.append(_longest_losing(seq))
    totals = arr[rng.integers(0, n, size=(n_iter, n))].sum(axis=1)
    means = totals / n
    hist_edges = np.linspace(0, max(dds.max(), 1e-9), 13)
    counts, _ = np.histogram(dds, bins=hist_edges)
    return {
        "iterations": n_iter, "trades": n,
        "actual_max_dd_r": round(_max_dd(arr), 2),
        "max_dd_r": {"p50": _pct(dds, 50), "p90": _pct(dds, 90), "p95": _pct(dds, 95),
                     "p99": _pct(dds, 99), "worst": round(float(dds.max()), 2)},
        "dd_histogram": {"edges": [round(float(x), 2) for x in hist_edges], "counts": counts.tolist()},
        "longest_losing": {"p50": _pct(streaks, 50), "p95": _pct(streaks, 95), "worst": int(max(streaks))},
        "total_r": {"p5": _pct(totals, 5), "p50": _pct(totals, 50), "p95": _pct(totals, 95)},
        "expectancy_ci95": [round(float(np.percentile(means, 2.5)), 4), round(float(np.percentile(means, 97.5)), 4)],
        "p_total_le_0": round(float((totals <= 0).mean()), 4),
    }


def in_out_sample(m: Market, params: dict, contract: str, start: date, end: date, frac: float = 0.7) -> dict:
    """Split the trading days at `frac`. Report the chosen parameters on each half, and
    an honest selection test: pick the target R that did best IN-sample, then show what
    that pick did OUT-of-sample next to what the default did."""
    days = [d for d in m.days if start <= d <= end and d.weekday() < 5]
    if len(days) < 20:
        return {"error": "need at least 20 trading days to split"}
    cut = days[int(len(days) * frac)]
    is_end = cut - timedelta(days=1)
    base = simulate(m, params, contract, start, end)["trades"]
    ins = [t for t in base if t["date"] < cut.isoformat()]
    oos = [t for t in base if t["date"] >= cut.isoformat()]
    picks = []
    for tr_r in TARGET_GRID:
        pp = {**params, "target_r": tr_r}
        tt = simulate(m, pp, contract, start, end)["trades"]
        picks.append((tr_r, summarize([t for t in tt if t["date"] < cut.isoformat()]),
                      summarize([t for t in tt if t["date"] >= cut.isoformat()])))
    best = max(picks, key=lambda x: (x[1].get("expectancy_r") or -1e9))
    return {
        "split_date": cut.isoformat(), "in_sample": {"from": start.isoformat(), "to": is_end.isoformat(), **summarize(ins)},
        "out_of_sample": {"from": cut.isoformat(), "to": end.isoformat(), **summarize(oos)},
        "selection": {"grid": [{"target_r": r, "in_sample": a, "out_of_sample": b} for r, a, b in picks],
                      "best_in_sample_target": best[0]},
    }


SENSITIVITY = [
    ("target_r", [2.0, 2.5, 3.0, 3.5, 4.0, 5.0]),
    ("or_end", ["09:15", "09:30", "09:45"]),
    ("last_entry", ["10:00", "10:30", "11:00", "11:30"]),
    ("sma_pair", [(20, 100), (50, 100), (50, 200), (100, 200)]),
    ("trend_tf", [5, 15, 30, 60]),
    ("stop", ["opposite", "midpoint"]),
    ("same_bar", ["stop_first", "target_first"]),
    ("slippage_ticks", [0, 1, 2, 4]),
]


def sensitivity(m: Market, params: dict, contract: str, start: date, end: date) -> list:
    """One parameter at a time around the chosen settings. An edge worth trusting is a
    plateau — similar results for nearby values — not one spike at the chosen value."""
    base = normalize_params(params)
    rows = []
    for name, values in SENSITIVITY:
        for v in values:
            pp = dict(base)
            if name == "sma_pair":
                pp["fast_sma"], pp["slow_sma"] = v
                label, current = f"{v[0]}/{v[1]}", (base["fast_sma"], base["slow_sma"]) == tuple(v)
            else:
                pp[name] = v
                label, current = str(v), normalize_params({**base, name: v})[name] == base[name]
            if name == "or_end" and _hm(v) >= _hm(pp["last_entry"]):
                continue
            try:
                s = summarize(simulate(m, pp, contract, start, end)["trades"])
            except ValueError as e:
                s = {"trades": 0, "error": str(e)}
            rows.append({"param": name, "value": label, "current": current, **s})
    return rows


def compare(m: Market, params: dict, contract: str, start: date, end: date) -> list:
    """Trend filter on vs off, for each target in TARGET_GRID."""
    rows = []
    for flt in (True, False):
        for tr_r in TARGET_GRID:
            s = summarize(simulate(m, {**params, "trend_filter": flt, "target_r": tr_r}, contract, start, end)["trades"])
            rows.append({"trend_filter": flt, "target_r": tr_r, **s})
    return rows


def full_report(m: Market, params: dict, contract: str, start: date, end: date, validate: bool = True) -> dict:
    p = normalize_params(params)
    run = simulate(m, p, contract, start, end)
    tr = run["trades"]
    out = {
        "params": p, "contract": contract, "contract_spec": CONTRACTS[contract],
        "summary": summarize(tr), "by_side": by_side(tr), "by_month": by_month(tr),
        "by_reason": by_reason(tr), "equity": equity_curve(tr), "trades": tr,
        "skipped": run["skipped"], "compare": compare(m, p, contract, start, end),
    }
    if validate:
        out["in_out_sample"] = in_out_sample(m, p, contract, start, end)
        out["monte_carlo"] = monte_carlo([t["r"] for t in tr])
        out["sensitivity"] = sensitivity(m, p, contract, start, end)
    return out


# ── loaders ──────────────────────────────────────────────────────────────────

WARMUP_DAYS = 14   # calendar days fetched before `start` so the slow SMA is ready on day one

YAHOO_SYMBOLS = {"NQ": "NQ=F", "MNQ": "MNQ=F"}
YAHOO_MAX_DAYS = 58


def load_yahoo(symbol: str, start: date, end: date) -> pd.DataFrame:
    """Continuous front-month futures from Yahoo, 5-minute bars, nearly 24 h a day.
    Yahoo keeps only ~60 days of 5-minute history."""
    import yfinance as yf
    start = max(start, datetime.utcnow().date() - timedelta(days=YAHOO_MAX_DAYS))
    df = yf.download(symbol, start=start.isoformat(), end=(end + timedelta(days=1)).isoformat(),
                     interval="5m", prepost=True, progress=False, auto_adjust=False)
    if df is None or df.empty:
        raise ValueError(f"Yahoo returned no 5-minute bars for {symbol}")
    if hasattr(df.columns, "levels"):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df[["open", "high", "low", "close"] + (["volume"] if "volume" in df.columns else [])]


ALPACA_CHUNK_DAYS = 31     # one request per month: a month of 1-minute QQQ is ~20k bars


def _alpaca_feed():
    """The app's market-data feed (SIP with the Algo Trader Plus subscription). Mirrors
    strategies.data.fetch_bars_alpaca: ALPACA_DATA_FEED, IEX if unset."""
    import os
    from alpaca.data.enums import DataFeed
    return DataFeed.SIP if (os.environ.get("ALPACA_DATA_FEED", "iex") or "iex").strip().lower() == "sip" else DataFeed.IEX


def load_alpaca(symbol: str, start: date, end: date, interval: str = "5m", progress=None) -> pd.DataFrame:
    """Stock bars from Alpaca, pre- and post-market included (the 09:00–09:30 range needs
    pre-market). For QQQ as a stand-in for NQ when no futures history is on hand.

    interval '1m' or '5m'. Years of 1-minute bars are ~1M rows, so they are fetched a month
    at a time as raw dicts and packed into arrays per month; building one Bar object per
    minute for the whole range would not fit in the server's memory. progress(msg) is
    called after each month."""
    import os
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    if interval not in ("1m", "5m"):
        raise ValueError("interval must be 1m or 5m")
    tf = TimeFrame(1, TimeFrameUnit.Minute) if interval == "1m" else TimeFrame(5, TimeFrameUnit.Minute)
    client = StockHistoricalDataClient(api_key=os.environ.get("ALPACA_KEY") or None,
                                       secret_key=os.environ.get("ALPACA_SECRET") or None, raw_data=True)
    feed, sym = _alpaca_feed(), symbol.upper()
    parts, d, n = [], start, 0
    months = max(1, math.ceil(((end - start).days + 1) / ALPACA_CHUNK_DAYS))
    while d <= end:
        e = min(end, d + timedelta(days=ALPACA_CHUNK_DAYS - 1))
        req = StockBarsRequest(symbol_or_symbols=sym, timeframe=tf, feed=feed, adjustment="all",
                               start=datetime(d.year, d.month, d.day, tzinfo=timezone.utc),
                               end=datetime(e.year, e.month, e.day, tzinfo=timezone.utc) + timedelta(days=1))
        raw = client.get_stock_bars(req)
        rows = raw.get(sym, []) if isinstance(raw, dict) else []
        if rows:
            parts.append(pd.DataFrame({
                "open": np.fromiter((r["o"] for r in rows), float, len(rows)),
                "high": np.fromiter((r["h"] for r in rows), float, len(rows)),
                "low": np.fromiter((r["l"] for r in rows), float, len(rows)),
                "close": np.fromiter((r["c"] for r in rows), float, len(rows)),
                "volume": np.fromiter((r.get("v") or 0 for r in rows), float, len(rows)),
            }, index=pd.DatetimeIndex(pd.to_datetime([r["t"] for r in rows], utc=True))))
        n += 1
        del raw, rows
        if progress:
            progress(f"loading {sym} {interval} bars: month {n} of {months}")
        d = e + timedelta(days=1)
    if not parts:
        raise ValueError(f"Alpaca returned no {interval} bars for {sym} ({start} → {end})")
    df = pd.concat(parts)
    return df[~df.index.duplicated(keep="last")].sort_index()
