"""Daily journal: one row per trading day of market context, plus every strategy's
result that day, so regimes can be found instead of guessed.

The weekly journal (/api/journal/*) is an AI essay written on demand. This module is
its quantitative counterpart and runs on its own after every close:

  journal_days           one row per trading day: SPY/QQQ/IWM/VIX bar features,
                         rule-based regime labels, each routed ticker's day type,
                         calendar events, and the crew's notes and quick tags.
  journal_strategy_days  one row per (day, account, strategy): trades, wins, P&L and
                         the sums needed for % per trade and its t-stat.
  journal_events         the editable event calendar (FOMC seeded; anything else the
                         crew adds). Earnings and monthly OPEX are worked out on the fly.

Labels come from fixed rules (thresholds below), not from a model, so the same kind of
day always gets the same label and a strategy's record can be split by them honestly.
The volatility label uses the PRIOR close of VIX, which is known before the open, so a
regime found on it could be used as a gate.

Nothing here places, closes or changes orders, rules or settings.
"""
from __future__ import annotations

import json
import logging
import math
import os
import random
import sys
import threading
import time
from collections import defaultdict
from datetime import date as _date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from flask import Blueprint, jsonify, request

journal_daily_bp = Blueprint("journal_daily", __name__)
log = logging.getLogger(__name__)
_ET = ZoneInfo("America/New_York")

MARKET_SYMBOLS = ("SPY", "QQQ", "IWM", "^VIX")

# ── Label rules. Change a threshold here and re-run the backfill to relabel history. ──
TREND_BODY_MIN = 0.60      # |close - open| / (high - low) at or above → trend day
CHOP_BODY_MAX = 0.30       # at or below → chop day; in between → mixed
GAP_MIN_PCT = 0.50         # |open / prior close - 1| in % at or above → gap day
VIX_LOW = 15.0             # prior-close VIX below → vix-low
VIX_HIGH = 20.0            # prior-close VIX at or above → vix-high; else vix-mid
WIDE_RANGE_X = 1.30        # SPY range vs its median of the prior 20 sessions
NARROW_RANGE_X = 0.70
RANGE_LOOKBACK = 20
# Pre-market (04:00–09:30 ET) range of SPY and QQQ vs each one's median pre-market range
# over the prior RANGE_LOOKBACK sessions. The label takes the larger of the two, so a
# tech-led pre-market (QQQ) counts even when SPY is calmer.
PREMARKET_SYMBOLS = ("SPY", "QQQ", "IWM")
PM_LABEL_SYMBOLS = ("SPY", "QQQ")
PM_VOLATILE_X = 1.50
PM_QUIET_X = 0.70

DIMENSIONS = {
    "trend": ("trend-up", "trend-down", "mixed", "chop"),
    "vix": ("vix-low", "vix-mid", "vix-high"),
    "gap": ("gap-up", "gap-down", "no-gap"),
    "range": ("wide-range", "normal-range", "narrow-range"),
    "premarket": ("pm-volatile", "pm-normal", "pm-quiet"),
}
DIMENSION_NAMES = {"trend": "SPY trend vs chop", "vix": "VIX at prior close", "gap": "SPY opening gap",
                   "range": "SPY day range", "premarket": "Pre-market range (SPY/QQQ)", "daytype": "Ticker day type", "event": "Calendar"}

FLIP_DAYS = 20             # recent window, in trading days, for spotting a strategy that flipped
FLIP_MIN_TRADES = 8        # each side of the split needs this many trades to call a flip
FLIP_MIN_T = 1.5           # and the change in mean % per trade needs at least this t-stat
PAIR_LEAD_DAYS = 14        # fills paired from this many days before a snapshot range
                           # (Welch), so a drift from -0.02% to +0.03% isn't called a flip

NOTE_TAGS = ("news", "fed", "earnings", "macro data", "geopolitics", "data glitch",
             "platform change", "holiday / half day")

# FOMC decision days (the second day of each meeting). Seeded once; edit them on the
# Daily tab if the Fed's calendar changes. CPI, jobs and similar dates are not seeded:
# add them on the Daily tab rather than trusting a hard-coded list.
FOMC_SEED = ("2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
             "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09")


# ── Pure helpers (unit-tested) ────────────────────────────────────────────────

def _median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    m = len(xs) // 2
    return xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2


def bar_features(bars):
    """Daily bars [{date, open, high, low, close}] (any order) → {date: features}.

    Features need the prior session, so the first bar gets none. range_x compares the
    day's range with the median of the RANGE_LOOKBACK sessions before it (None until
    there are at least half that many)."""
    bars = sorted((b for b in bars if b.get("close")), key=lambda b: b["date"])
    out, ranges = {}, []
    for prev, b in zip(bars, bars[1:]):
        pc = float(prev["close"])
        o, h, l, c = (float(b[k]) for k in ("open", "high", "low", "close"))
        rng = (h - l) / pc * 100 if pc else 0.0
        base = _median(ranges[-RANGE_LOOKBACK:]) if len(ranges) >= RANGE_LOOKBACK // 2 else None
        out[b["date"]] = {
            "open": round(o, 4), "high": round(h, 4), "low": round(l, 4), "close": round(c, 4),
            "prev_close": round(pc, 4),
            "gap_pct": round((o / pc - 1) * 100, 3) if pc else 0.0,
            "ret_pct": round((c / pc - 1) * 100, 3) if pc else 0.0,
            "range_pct": round(rng, 3),
            "body_ratio": round(abs(c - o) / (h - l), 3) if h > l else 0.0,
            "direction": "up" if c >= o else "down",
            "range_x": round(rng / base, 3) if base else None,
        }
        ranges.append(rng)
    return out


def premarket_features(bars, prev_close):
    """Intraday bars [{time, open, high, low, close, volume}] (time in UTC; naive means
    UTC) and {date: prior regular-session close} → {date: pre-market features}.

    The pre-market is 04:00–09:30 ET. pm_range_pct and pm_move_pct are against the
    prior close; pm_range_x compares the range with the median of the RANGE_LOOKBACK
    sessions before it (None until there are at least half that many)."""
    from datetime import time as _t
    days = defaultdict(list)
    for b in bars:
        t = b["time"]
        t = (t if t.tzinfo else t.replace(tzinfo=timezone.utc)).astimezone(_ET)
        if _t(4, 0) <= t.time() < _t(9, 30):
            days[t.date().isoformat()].append((t, b))
    out, ranges = {}, []
    for d in sorted(days):
        pc = prev_close.get(d)
        if not pc:
            continue
        rows = sorted(days[d], key=lambda x: x[0])
        hi = max(float(b["high"]) for _, b in rows)
        lo = min(float(b["low"]) for _, b in rows)
        last = float(rows[-1][1]["close"])
        rng = (hi - lo) / pc * 100
        base = _median(ranges[-RANGE_LOOKBACK:]) if len(ranges) >= RANGE_LOOKBACK // 2 else None
        out[d] = {"pm_high": round(hi, 4), "pm_low": round(lo, 4), "pm_last": round(last, 4),
                  "pm_range_pct": round(rng, 3), "pm_move_pct": round((last / pc - 1) * 100, 3),
                  "pm_volume": int(sum(float(b.get("volume") or 0) for _, b in rows)),
                  "pm_range_x": round(rng / base, 3) if base else None}
        ranges.append(rng)
    return out


def premarket_label(pm_xs):
    """The day's pre-market label from the pm_range_x values of PM_LABEL_SYMBOLS
    (None entries ignored); None when there's no baseline yet."""
    xs = [x for x in pm_xs if x is not None]
    if not xs:
        return None
    x = max(xs)
    return "pm-volatile" if x >= PM_VOLATILE_X else "pm-quiet" if x <= PM_QUIET_X else "pm-normal"


def day_labels(spy, vix_prev_close, pm_xs=()):
    """SPY's features for a day, VIX's prior close and the pre-market range ratios →
    {dimension: label}. A dimension whose input is missing is left out rather than guessed."""
    lab = {}
    pm = premarket_label(pm_xs)
    if pm:
        lab["premarket"] = pm
    if spy:
        br = spy.get("body_ratio") or 0.0
        if br >= TREND_BODY_MIN:
            lab["trend"] = "trend-up" if spy.get("direction") == "up" else "trend-down"
        elif br <= CHOP_BODY_MAX:
            lab["trend"] = "chop"
        else:
            lab["trend"] = "mixed"
        g = spy.get("gap_pct") or 0.0
        lab["gap"] = "gap-up" if g >= GAP_MIN_PCT else "gap-down" if g <= -GAP_MIN_PCT else "no-gap"
        rx = spy.get("range_x")
        if rx is not None:
            lab["range"] = ("wide-range" if rx >= WIDE_RANGE_X
                            else "narrow-range" if rx <= NARROW_RANGE_X else "normal-range")
    if vix_prev_close is not None:
        v = float(vix_prev_close)
        lab["vix"] = "vix-low" if v < VIX_LOW else "vix-high" if v >= VIX_HIGH else "vix-mid"
    return lab


def third_friday(year, month):
    d = _date(year, month, 15)                   # the third Friday falls on the 15th–21st
    return d + timedelta(days=(4 - d.weekday()) % 7)


def calendar_events(day, manual=(), earnings_by_ticker=None):
    """Events for one ISO day: monthly OPEX, the editable list, and earnings
    announcements for the tickers given. → [{kind, label}] sorted, no duplicates."""
    y, m, d = (int(x) for x in day.split("-"))
    ev = set()
    if _date(y, m, d) == third_friday(y, m):
        ev.add(("opex", "Monthly OPEX"))
    for e in manual:
        if e.get("date") == day:
            ev.add((e.get("kind") or "event", e.get("label") or ""))
    for tk, dates in (earnings_by_ticker or {}).items():
        if day in dates:
            ev.add(("earnings", f"{tk} earnings"))
    return [{"kind": k, "label": l} for k, l in sorted(ev)]


def _ticker_of(strategy):
    s = (strategy or "").upper()
    i = s.find("_CAM_")
    if i > 0:
        return s[:i]
    if s.startswith("CAM_"):                      # older CAM_<TICKER>_... names
        parts = s.split("_")
        return parts[1] if len(parts) > 1 else ""
    return s.split("_", 1)[0]


def et_day(iso):
    try:
        d = datetime.fromisoformat((iso or "").replace("Z", "+00:00"))
    except ValueError:
        return ""
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(_ET).strftime("%Y-%m-%d")


def trade_pct(t):
    """P&L as % of the position's entry notional, or None if the size is unknown."""
    try:
        notional = abs(float(t.get("qty") or 0)) * float(t.get("entry_price") or 0)
    except (TypeError, ValueError):
        return None
    return float(t.get("pnl") or 0) / notional * 100 if notional else None


def aggregate_strategy_days(trips):
    """Closed round trips (split exits already merged) → per (ET entry day, strategy)
    rows with the sums that % per trade and its t-stat are built from."""
    agg = {}
    for t in trips:
        s = (t.get("strategy") or "").strip().upper()
        d = et_day(t.get("entry_time")) or et_day(t.get("exit_time")) or (t.get("date") or "")
        if not s or not d:
            continue
        r = agg.setdefault((d, s), {"date": d, "strategy": s, "ticker": _ticker_of(s), "trades": 0,
                                    "wins": 0, "pnl": 0.0, "pct_n": 0, "pct_sum": 0.0,
                                    "pct_sq_sum": 0.0, "notional": 0.0})
        pnl = float(t.get("pnl") or 0)
        r["trades"] += 1
        r["wins"] += 1 if pnl > 0 else 0
        r["pnl"] += pnl
        p = trade_pct(t)
        if p is not None:
            r["pct_n"] += 1
            r["pct_sum"] += p
            r["pct_sq_sum"] += p * p
            r["notional"] += abs(float(t.get("qty") or 0)) * float(t.get("entry_price") or 0)
    for r in agg.values():
        for k in ("pnl", "pct_sum", "pct_sq_sum", "notional"):
            r[k] = round(r[k], 6)
    return sorted(agg.values(), key=lambda r: (r["date"], r["strategy"]))


def summarize(rows):
    """Strategy-day rows → {trades, wins, win_rate, pnl, mean_pct, t, days}."""
    n = sum(int(r.get("trades") or 0) for r in rows)
    wins = sum(int(r.get("wins") or 0) for r in rows)
    pnl = sum(float(r.get("pnl") or 0) for r in rows)
    pn = sum(int(r.get("pct_n") or 0) for r in rows)
    s1 = sum(float(r.get("pct_sum") or 0) for r in rows)
    s2 = sum(float(r.get("pct_sq_sum") or 0) for r in rows)
    mean = s1 / pn if pn else None
    t = None
    if pn >= 5:                                   # a t-stat on 2–4 trades is noise dressed as precision
        var = (s2 - pn * mean * mean) / (pn - 1)
        if var > 1e-12:
            t = mean / math.sqrt(var / pn)
    return {"trades": n, "wins": wins, "win_rate": round(wins / n, 4) if n else None,
            "pnl": round(pnl, 2), "mean_pct": round(mean, 4) if mean is not None else None,
            "t": round(t, 2) if t is not None else None, "days": len({r.get("date") for r in rows})}


def _row_dims(r, days, daytypes):
    """Every (dimension, value) a strategy-day row falls under."""
    day = days.get(r["date"]) or {}
    out = [(k, v) for k, v in (day.get("labels") or {}).items()]
    dt = (daytypes.get(r["date"]) or {}).get(r.get("ticker") or _ticker_of(r["strategy"]))
    if dt:
        out.append(("daytype", dt))
    ev = day.get("events") or []
    tk = r.get("ticker") or _ticker_of(r["strategy"])
    kinds = {e["kind"] for e in ev if e.get("kind") != "earnings"}
    if any(e.get("kind") == "earnings" and e.get("label", "").split(" ")[0] == tk for e in ev):
        kinds.add("own earnings")
    out.append(("event", ", ".join(sorted(kinds)) if kinds else "no event"))
    return out


def regime_breakdown(rows, days, daytypes):
    """A strategy's rows split by every label → {dimension: [{value, ...summary}]},
    ordered by the dimension's natural order."""
    groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        for dim, val in _row_dims(r, days, daytypes):
            groups[dim][val].append(r)
    out = {}
    for dim in list(DIMENSIONS) + ["daytype", "event"]:
        if dim not in groups:
            continue
        order = DIMENSIONS.get(dim, ("Outside", "Neutral", "Inside") if dim == "daytype" else ())
        vals = sorted(groups[dim], key=lambda v: (order.index(v) if v in order else len(order), v))
        out[dim] = [{"value": v, **summarize(groups[dim][v])} for v in vals]
    return out


def good_vs_bad(rows, days, daytypes):
    """How a strategy's winning days differ from its losing days: for each label, the
    share of winning days and of losing days that carried it, biggest gap first."""
    by_day = defaultdict(list)
    for r in rows:
        by_day[r["date"]].append(r)
    win_d, loss_d = [], []
    for d, rs in by_day.items():
        p = sum(float(x.get("pnl") or 0) for x in rs)
        (win_d if p > 0 else loss_d if p < 0 else []).append(rs)
    if not win_d or not loss_d:
        return {"win_days": len(win_d), "loss_days": len(loss_d), "rows": []}

    def _shares(day_groups):
        c = defaultdict(int)
        for rs in day_groups:
            for dv in {dv for r in rs for dv in _row_dims(r, days, daytypes)}:
                c[dv] += 1
        return {k: v / len(day_groups) for k, v in c.items()}
    ws, ls = _shares(win_d), _shares(loss_d)
    rows_out = [{"dimension": dim, "value": val, "win_share": round(ws.get((dim, val), 0), 3),
                 "loss_share": round(ls.get((dim, val), 0), 3),
                 "diff": round(ws.get((dim, val), 0) - ls.get((dim, val), 0), 3)}
                for dim, val in set(ws) | set(ls)]
    rows_out.sort(key=lambda x: (-abs(x["diff"]), x["dimension"], x["value"]))
    return {"win_days": len(win_d), "loss_days": len(loss_d), "rows": rows_out}


def _mean_var(rows):
    n = sum(int(r.get("pct_n") or 0) for r in rows)
    if n < 2:
        return n, None, None
    s1 = sum(float(r.get("pct_sum") or 0) for r in rows)
    s2 = sum(float(r.get("pct_sq_sum") or 0) for r in rows)
    m = s1 / n
    return n, m, max(0.0, (s2 - n * m * m) / (n - 1))


def detect_flip(rows, trading_dates, recent_days=FLIP_DAYS, min_trades=FLIP_MIN_TRADES, min_t=FLIP_MIN_T):
    """Split a strategy's record at the last `recent_days` trading days and compare.
    A flip is a change of sign in mean % per trade, with at least `min_trades` on each
    side and a change big enough to clear `min_t` (Welch t-stat of the difference)."""
    dates = sorted(set(trading_dates))
    if len(dates) <= recent_days:
        return {"cutoff": None, "earlier": summarize([]), "recent": summarize(rows), "flipped": False,
                "direction": None, "change_t": None}
    cutoff = dates[-recent_days]
    early_rows = [r for r in rows if r["date"] < cutoff]
    recent_rows = [r for r in rows if r["date"] >= cutoff]
    early, recent = summarize(early_rows), summarize(recent_rows)
    n1, m1, v1 = _mean_var(early_rows)
    n2, m2, v2 = _mean_var(recent_rows)
    change_t = None
    if m1 is not None and m2 is not None and (v1 / n1 + v2 / n2) > 1e-12:
        change_t = round((m2 - m1) / math.sqrt(v1 / n1 + v2 / n2), 2)
    flipped, direction = False, None
    if (early["trades"] >= min_trades and recent["trades"] >= min_trades
            and m1 is not None and m2 is not None and (m1 > 0) != (m2 > 0)
            and change_t is not None and abs(change_t) >= min_t):
        flipped = True
        direction = "faded" if m1 > 0 else "recovered"
    return {"cutoff": cutoff, "earlier": early, "recent": recent, "flipped": flipped,
            "direction": direction, "change_t": change_t}


# ── Data access ───────────────────────────────────────────────────────────────

def _app():
    import app as _kairos
    return _kairos


def _rows(cur):
    cols = [d[0] for d in cur.description] if cur.description else []
    return [dict(zip(cols, r)) for r in cur.fetchall()]


_tables_ready = False
_tables_lock = threading.Lock()


def _ensure_tables(a):
    global _tables_ready
    if _tables_ready:
        return
    with _tables_lock:
        if _tables_ready:
            return
        real = "DOUBLE PRECISION" if a.DATABASE_URL else "REAL"
        conn = a.get_db()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS journal_days (
                    date             TEXT PRIMARY KEY,
                    market           TEXT,
                    labels           TEXT,
                    daytypes         TEXT,
                    events           TEXT,
                    notes            TEXT,
                    note_tags        TEXT,
                    notes_updated_at TEXT,
                    snapshot_at      TEXT
                )""")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS journal_strategy_days (
                    date       TEXT NOT NULL,
                    account    TEXT NOT NULL,
                    strategy   TEXT NOT NULL,
                    ticker     TEXT,
                    trades     INTEGER NOT NULL,
                    wins       INTEGER NOT NULL,
                    pnl        {real} NOT NULL,
                    pct_n      INTEGER NOT NULL,
                    pct_sum    {real} NOT NULL,
                    pct_sq_sum {real} NOT NULL,
                    notional   {real} NOT NULL,
                    PRIMARY KEY (date, account, strategy)
                )""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS journal_events (
                    date   TEXT NOT NULL,
                    kind   TEXT NOT NULL,
                    label  TEXT NOT NULL,
                    source TEXT,
                    PRIMARY KEY (date, kind, label)
                )""")
            p = a.placeholder()
            for d in FOMC_SEED:
                cur.execute(f"INSERT INTO journal_events (date, kind, label, source) "
                            f"VALUES ({p},{p},{p},{p}) ON CONFLICT DO NOTHING",
                            (d, "fed", "FOMC decision", "seed"))
            conn.commit()
            _tables_ready = True
        finally:
            conn.close()


def _routed_tickers(a):
    """Tickers of every enabled routing rule (the same set the day-type tape shows)."""
    out = set()
    conn = a.get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM routing_rules WHERE enabled=1")
        for (nm,) in [tuple(r) for r in cur.fetchall()]:
            tk = _ticker_of(nm or "")
            if tk and "_CAM_" in (nm or "").upper():
                out.add(tk)
    except Exception as e:
        log.debug("journal_daily routed tickers: %s", e)
    finally:
        conn.close()
    return out


def _daily_bars(symbol, start, end):
    from strategies.data import fetch_bars
    bars = fetch_bars(symbol, start, end, interval="1d")
    return [{"date": b["time"].strftime("%Y-%m-%d"), "open": b["open"], "high": b["high"],
             "low": b["low"], "close": b["close"]} for b in bars]


def _intraday_bars(symbol, start, end):
    """5-minute bars including the pre-market (Alpaca; times are naive UTC)."""
    from strategies.data import fetch_bars_alpaca
    return fetch_bars_alpaca(symbol, start, end, interval="5m")


def _load_events(a, from_date=None, to_date=None):
    _ensure_tables(a)
    conn = a.get_db()
    try:
        cur = conn.cursor()
        p = a.placeholder()
        if from_date and to_date:
            cur.execute(f"SELECT date, kind, label, source FROM journal_events "
                        f"WHERE date >= {p} AND date <= {p} ORDER BY date", (from_date, to_date))
        else:
            cur.execute("SELECT date, kind, label, source FROM journal_events ORDER BY date")
        return _rows(cur)
    finally:
        conn.close()


def _earnings_by_ticker(tickers):
    try:
        from strategies.earnings import announcement_dates
    except Exception:
        return {}
    out = {}
    for tk in sorted(tickers):
        try:
            out[tk] = {d.isoformat() for d in announcement_dates(tk)}
        except Exception as e:
            log.debug("journal_daily earnings %s: %s", tk, e)
    return out


def _account_meta(a):
    """Configured accounts plus any account that has journal history (so a book that
    is later switched off keeps its record visible)."""
    meta = getattr(a, "ACCOUNT_META", {}) or {}
    nums = set(a.ACCOUNTS_BY_NUM)
    try:
        _ensure_tables(a)
        conn = a.get_db()
        try:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT account FROM journal_strategy_days")
            nums |= {str(r[0]) for r in cur.fetchall()}
        finally:
            conn.close()
    except Exception as e:
        log.debug("journal_daily account list: %s", e)
    return [{"num": n, "label": (meta.get(n) or {}).get("label")
                    or (a.ACCOUNTS_BY_NUM.get(n) or {}).get("label") or f"Account {n}",
             "color": (meta.get(n) or {}).get("color") or "#888"}
            for n in sorted(nums)]


# ── Snapshot ──────────────────────────────────────────────────────────────────

_snap_lock = threading.Lock()
_status = {"running": False, "from": None, "to": None, "started_at": None,
           "finished_at": None, "result": None, "error": None}


def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def snapshot_range(from_date, to_date):
    """Build or rebuild journal rows for every trading day in [from_date, to_date].
    Safe to re-run: market context is upserted (notes are never touched) and each
    account's strategy rows for the range are replaced."""
    from concurrent.futures import ThreadPoolExecutor
    import routes.farms as farms

    a = _app()
    _ensure_tables(a)
    errors = []
    d0 = _date.fromisoformat(from_date)
    d1 = _date.fromisoformat(to_date)

    # 1. Market bars, with enough history before the range for range_x.
    start = (d0 - timedelta(days=45)).isoformat()
    end = (d1 + timedelta(days=1)).isoformat()
    feats = {}
    for sym in MARKET_SYMBOLS:
        try:
            feats[sym.replace("^", "")] = bar_features(_daily_bars(sym, start, end))
        except Exception as e:
            errors.append(f"{sym} bars: {e}")
            feats[sym.replace("^", "")] = {}
    trading = sorted(d for d in feats.get("SPY", {}) if from_date <= d <= to_date)

    # 1b. Pre-market (04:00–09:30 ET) from 5-minute bars, same history for its baseline.
    for sym in PREMARKET_SYMBOLS:
        daily = feats.get(sym) or {}
        try:
            pm = premarket_features(_intraday_bars(sym, start, end),
                                    {d: f["prev_close"] for d, f in daily.items()})
        except Exception as e:
            errors.append(f"{sym} pre-market bars: {e}")
            continue
        for d, f in pm.items():
            if d in daily:
                daily[d].update(f)

    # 2. Strategy rows per account (fills → LIFO round trips → merged split exits).
    # Leaderboard exclusions are off: the journal records what each account actually
    # traded, including tickers/strategies/bands barred from the Refined leaderboards.
    # LIFO pairing depends on where the fill window starts: starting it on from_date
    # can match a day's exits to the wrong entries (the nightly 7-day re-snapshot
    # showed HOOD 3 trades / -$3.47 where the full history has 5 / -$19.98). Pair
    # from PAIR_LEAD_DAYS earlier and keep only rows inside the range.
    pair_from = d0 - timedelta(days=PAIR_LEAD_DAYS)
    full = (_date.today() - pair_from).days > 85
    strat_rows = {}
    for num in sorted(a.ACCOUNTS_BY_NUM):
        try:
            rts, err = farms._farm_round_trips(num, pair_from.isoformat(), to_date, full=full,
                                               exclusions=False)
            if err:
                errors.append(f"account {num}: {err}")
                continue
            strat_rows[num] = [r for r in aggregate_strategy_days(farms.merge_fragments(rts))
                               if from_date <= r["date"] <= to_date]
        except Exception as e:
            errors.append(f"account {num}: {e}")

    # 3. Day types for routed tickers and anything traded (cached per ticker-day by the app).
    tickers = _routed_tickers(a) | {r["ticker"] for rows in strat_rows.values() for r in rows if r["ticker"]}
    jobs = [(tk, d) for d in trading for tk in sorted(tickers)]
    daytypes = defaultdict(dict)
    if jobs and hasattr(a, "_get_day_classification"):
        def _one(job):
            try:
                return job, a._get_day_classification(job[0], job[1]) or {}
            except Exception:
                return job, {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            for (tk, d), cls in pool.map(_one, jobs):
                if cls.get("day_type"):
                    daytypes[d][tk] = {"day_type": cls["day_type"], "ratio": cls.get("ratio")}

    # 4. Events.
    manual = _load_events(a, from_date, to_date)
    earnings = _earnings_by_ticker(tickers)

    # 5. Write.
    conn = a.get_db()
    p = a.placeholder()
    try:
        cur = conn.cursor()
        for d in trading:
            market = {sym: f[d] for sym, f in feats.items() if d in f}
            vix_prev = (feats.get("VIX", {}).get(d) or {}).get("prev_close")
            labels = day_labels(feats["SPY"].get(d), vix_prev,
                                [(feats.get(s, {}).get(d) or {}).get("pm_range_x") for s in PM_LABEL_SYMBOLS])
            events = calendar_events(d, manual, earnings)
            cur.execute(
                f"INSERT INTO journal_days (date, market, labels, daytypes, events, snapshot_at) "
                f"VALUES ({p},{p},{p},{p},{p},{p}) ON CONFLICT (date) DO UPDATE SET "
                f"market=excluded.market, labels=excluded.labels, daytypes=excluded.daytypes, "
                f"events=excluded.events, snapshot_at=excluded.snapshot_at",
                (d, json.dumps(market), json.dumps(labels), json.dumps(daytypes.get(d, {})),
                 json.dumps(events), _now_iso()))
        n_rows = 0
        for num, rows in strat_rows.items():
            cur.execute(f"DELETE FROM journal_strategy_days WHERE account={p} AND date >= {p} AND date <= {p}",
                        (num, from_date, to_date))
            for r in rows:
                cur.execute(
                    f"INSERT INTO journal_strategy_days (date, account, strategy, ticker, trades, wins, pnl, "
                    f"pct_n, pct_sum, pct_sq_sum, notional) VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})",
                    (r["date"], num, r["strategy"], r["ticker"], r["trades"], r["wins"], r["pnl"],
                     r["pct_n"], r["pct_sum"], r["pct_sq_sum"], r["notional"]))
                n_rows += 1
        conn.commit()
    finally:
        conn.close()
    return {"days": len(trading), "strategy_rows": n_rows, "accounts": sorted(strat_rows),
            "tickers": len(tickers), "errors": errors}


def run_snapshot(from_date, to_date):
    """snapshot_range under the module lock, recording status for the page."""
    if not _snap_lock.acquire(blocking=False):
        return False
    try:
        _status.update({"running": True, "from": from_date, "to": to_date, "started_at": _now_iso(),
                        "finished_at": None, "result": None, "error": None})
        try:
            _status["result"] = snapshot_range(from_date, to_date)
        except Exception as e:
            log.exception("journal snapshot %s..%s failed", from_date, to_date)
            _status["error"] = str(e)
        _status.update({"running": False, "finished_at": _now_iso()})
        return True
    finally:
        _snap_lock.release()


def _after_close(now):
    """From 16:20 ET (after the EOD close) to midnight, any day."""
    return (now.hour, now.minute) >= (16, 20)


def _scheduler_loop():
    """Once per ET day, after the close, re-snapshot the last 7 days. That records today
    the same evening and fills any days a redeploy or outage missed. (It used to run at
    the first closed moment of the day, which is just after midnight, so a day only
    appeared the next morning, and a morning restart ran it before the open.)"""
    time.sleep(90 + random.uniform(0, 120))       # let the app warm up; stagger workers
    done_for = None
    while True:
        try:
            now = datetime.now(_ET)
            today = now.date().isoformat()
            if done_for != today and _after_close(now):
                frm = (now.date() - timedelta(days=7)).isoformat()
                if run_snapshot(frm, today):
                    done_for = today
                    log.info("journal daily snapshot %s..%s: %s", frm, today, _status.get("result"))
        except Exception as e:
            log.warning("journal daily scheduler: %s", e)
        time.sleep(300)


_scheduler_started = False


def start_scheduler():
    """Start the daily snapshot thread (not under pytest, and not if
    JOURNAL_DAILY_SCHEDULER=0)."""
    global _scheduler_started
    if _scheduler_started or "pytest" in sys.modules or os.environ.get("JOURNAL_DAILY_SCHEDULER") == "0":
        return
    _scheduler_started = True
    threading.Thread(target=_scheduler_loop, daemon=True, name="journal-daily").start()


# ── Reading ───────────────────────────────────────────────────────────────────

def _range_args(default_days=30):
    today = datetime.now(_ET).date()
    to_date = (request.args.get("to") or today.isoformat())[:10]
    from_date = (request.args.get("from") or (today - timedelta(days=default_days)).isoformat())[:10]
    try:
        _date.fromisoformat(from_date)
        _date.fromisoformat(to_date)
    except ValueError:
        raise ValueError("from and to must be YYYY-MM-DD")
    return from_date, to_date


def _load_days(a, from_date, to_date):
    _ensure_tables(a)
    conn = a.get_db()
    p = a.placeholder()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM journal_days WHERE date >= {p} AND date <= {p} ORDER BY date",
                    (from_date, to_date))
        days = _rows(cur)
    finally:
        conn.close()
    for d in days:
        for k in ("market", "labels", "daytypes", "events", "note_tags"):
            try:
                d[k] = json.loads(d.get(k) or ("[]" if k in ("events", "note_tags") else "{}"))
            except (TypeError, ValueError):
                d[k] = [] if k in ("events", "note_tags") else {}
    return days


def _load_strategy_days(a, from_date, to_date, account=None, strategy=None):
    _ensure_tables(a)
    conn = a.get_db()
    p = a.placeholder()
    q = f"SELECT * FROM journal_strategy_days WHERE date >= {p} AND date <= {p}"
    args = [from_date, to_date]
    if account:
        q += f" AND account = {p}"
        args.append(str(account))
    if strategy:
        q += f" AND strategy = {p}"
        args.append(strategy.upper())
    try:
        cur = conn.cursor()
        cur.execute(q + " ORDER BY date, account, strategy", tuple(args))
        return _rows(cur)
    finally:
        conn.close()


@journal_daily_bp.route("/api/journal/days")
def api_journal_days():
    """Trading days in [from, to] with market context, labels, events, notes and
    each account's totals for the day."""
    a = _app()
    try:
        from_date, to_date = _range_args()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    days = _load_days(a, from_date, to_date)
    totals = defaultdict(lambda: defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0}))
    for r in _load_strategy_days(a, from_date, to_date):
        t = totals[r["date"]][r["account"]]
        t["trades"] += int(r["trades"])
        t["wins"] += int(r["wins"])
        t["pnl"] = round(t["pnl"] + float(r["pnl"]), 2)
    for d in days:
        d["accounts"] = totals.get(d["date"], {})
        counts = defaultdict(int)
        for v in (d.get("daytypes") or {}).values():
            counts[v.get("day_type")] += 1
        d["daytype_counts"] = dict(counts)
    return jsonify({"from": from_date, "to": to_date, "days": days, "accounts": _account_meta(a),
                    "note_tags": list(NOTE_TAGS), "dimensions": DIMENSION_NAMES,
                    "rules": {"trend_body_min": TREND_BODY_MIN, "chop_body_max": CHOP_BODY_MAX,
                              "gap_min_pct": GAP_MIN_PCT, "vix_low": VIX_LOW, "vix_high": VIX_HIGH,
                              "wide_range_x": WIDE_RANGE_X, "narrow_range_x": NARROW_RANGE_X,
                              "pm_volatile_x": PM_VOLATILE_X, "pm_quiet_x": PM_QUIET_X},
                    "snapshot": dict(_status)})


@journal_daily_bp.route("/api/journal/strategy_days")
def api_journal_strategy_days():
    a = _app()
    try:
        from_date, to_date = _range_args()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    rows = _load_strategy_days(a, from_date, to_date, request.args.get("account"), request.args.get("strategy"))
    return jsonify({"from": from_date, "to": to_date, "rows": rows})


def _regime_inputs(a, from_date, to_date, account):
    days = {d["date"]: d for d in _load_days(a, from_date, to_date)}
    daytypes = {d: {tk: v.get("day_type") for tk, v in (row.get("daytypes") or {}).items()}
                for d, row in days.items()}
    rows = _load_strategy_days(a, from_date, to_date, account)
    by_strategy = defaultdict(list)
    for r in rows:
        by_strategy[r["strategy"]].append(r)
    return days, daytypes, by_strategy


@journal_daily_bp.route("/api/journal/regimes")
def api_journal_regimes():
    """One strategy on one account: its record split by every label, how its winning
    days differ from its losing days, and whether it flipped recently. Without a
    strategy, lists the account's strategies with enough trades to look at."""
    a = _app()
    try:
        from_date, to_date = _range_args(default_days=120)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    account = request.args.get("account") or "5"
    strategy = (request.args.get("strategy") or "").strip().upper()
    days, daytypes, by_strategy = _regime_inputs(a, from_date, to_date, account)
    trading = sorted(days)
    if not strategy:
        min_trades = int(request.args.get("min_trades") or 10)
        out = []
        for s, rs in by_strategy.items():
            sm = summarize(rs)
            if sm["trades"] < min_trades:
                continue
            fl = detect_flip(rs, trading)
            out.append({"strategy": s, **sm, "flipped": fl["flipped"], "direction": fl["direction"],
                        "earlier_mean_pct": fl["earlier"]["mean_pct"], "recent_mean_pct": fl["recent"]["mean_pct"],
                        "change_t": fl["change_t"]})
        out.sort(key=lambda r: (not r["flipped"], -r["trades"]))
        return jsonify({"account": account, "from": from_date, "to": to_date, "days": len(trading),
                        "strategies": out, "flip_days": FLIP_DAYS, "flip_min_trades": FLIP_MIN_TRADES,
                        "flip_min_t": FLIP_MIN_T})
    rs = by_strategy.get(strategy, [])
    return jsonify({
        "account": account, "strategy": strategy, "from": from_date, "to": to_date,
        "days": len(trading), "overall": summarize(rs),
        "breakdown": regime_breakdown(rs, days, daytypes), "dimensions": DIMENSION_NAMES,
        "good_vs_bad": good_vs_bad(rs, days, daytypes),
        "flip": detect_flip(rs, trading),
        "daily": [{"date": r["date"], "trades": r["trades"], "pnl": round(float(r["pnl"]), 2),
                   "labels": (days.get(r["date"]) or {}).get("labels") or {},
                   "day_type": (daytypes.get(r["date"]) or {}).get(r.get("ticker"))} for r in rs],
    })


# ── Writing (notes, events, snapshot) ─────────────────────────────────────────

@journal_daily_bp.route("/api/journal/days/<day>/notes", methods=["PUT"])
def api_journal_day_notes(day):
    a = _app()
    try:
        _date.fromisoformat(day)
    except ValueError:
        return jsonify({"error": "date must be YYYY-MM-DD"}), 400
    body = request.get_json(silent=True) or {}
    notes = str(body.get("notes") or "")[:2000]
    tags = [t for t in (body.get("tags") or []) if t in NOTE_TAGS]
    _ensure_tables(a)
    conn = a.get_db()
    p = a.placeholder()
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO journal_days (date, notes, note_tags, notes_updated_at) "
                    f"VALUES ({p},{p},{p},{p}) ON CONFLICT (date) DO UPDATE SET notes=excluded.notes, "
                    f"note_tags=excluded.note_tags, notes_updated_at=excluded.notes_updated_at",
                    (day, notes, json.dumps(tags), _now_iso()))
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True, "date": day, "notes": notes, "tags": tags})


@journal_daily_bp.route("/api/journal/events", methods=["GET", "POST", "DELETE"])
def api_journal_events():
    a = _app()
    if request.method == "GET":
        return jsonify({"events": _load_events(a, request.args.get("from"), request.args.get("to"))})
    body = request.get_json(silent=True) or {}
    day = str(body.get("date") or "")[:10]
    label = str(body.get("label") or "").strip()[:80]
    kind = str(body.get("kind") or "event").strip().lower()[:20] or "event"
    try:
        _date.fromisoformat(day)
    except ValueError:
        return jsonify({"error": "date must be YYYY-MM-DD"}), 400
    if not label:
        return jsonify({"error": "label is required"}), 400
    _ensure_tables(a)
    conn = a.get_db()
    p = a.placeholder()
    try:
        cur = conn.cursor()
        if request.method == "POST":
            cur.execute(f"INSERT INTO journal_events (date, kind, label, source) VALUES ({p},{p},{p},{p}) "
                        f"ON CONFLICT DO NOTHING", (day, kind, label, "crew"))
        else:
            cur.execute(f"DELETE FROM journal_events WHERE date={p} AND kind={p} AND label={p}",
                        (day, kind, label))
        conn.commit()
    finally:
        conn.close()
    # The day's stored events are rebuilt on the next snapshot of that day.
    return jsonify({"ok": True, "event": {"date": day, "kind": kind, "label": label}})


@journal_daily_bp.route("/api/journal/snapshot", methods=["GET", "POST"])
def api_journal_snapshot():
    """GET: the last snapshot's status. POST {from, to}: rebuild that range in the
    background (the backfill). One snapshot runs at a time."""
    if request.method == "GET":
        return jsonify(dict(_status))
    body = request.get_json(silent=True) or {}
    today = datetime.now(_ET).date()
    from_date = str(body.get("from") or (today - timedelta(days=7)).isoformat())[:10]
    to_date = str(body.get("to") or today.isoformat())[:10]
    try:
        d0, d1 = _date.fromisoformat(from_date), _date.fromisoformat(to_date)
    except ValueError:
        return jsonify({"error": "from and to must be YYYY-MM-DD"}), 400
    if d1 < d0 or (d1 - d0).days > 400:
        return jsonify({"error": "pick a range of at most 400 days, from before to"}), 400
    if _status.get("running"):
        return jsonify({"error": "a snapshot is already running", **_status}), 409
    threading.Thread(target=run_snapshot, args=(from_date, to_date), daemon=True,
                     name="journal-backfill").start()
    return jsonify({"ok": True, "from": from_date, "to": to_date}), 202
