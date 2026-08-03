"""
Earnings reaction study — what a stock actually does the morning after earnings.

Inverts the gap-fill backtest: instead of running entry rules over a date window
and hoping earnings days fall inside it, this enumerates each ticker's earnings
ANNOUNCEMENT dates and measures the next session directly. That yields ~24
events per ticker (~6 years) instead of the handful a 59-day intraday window can
reach, and it answers the prior question — "do earnings gaps fill, and which way
do they resolve?" — without depending on any strategy's entries firing.

DAILY bars, deliberately: Yahoo serves only 59 days of 5m/2m history (see
_YF_MAX_DAYS in strategies/data.py), so multi-year work has to be daily. Daily
OHLC still answers the question; what it cannot say is WHEN during the session a
gap filled.

BMO/AMC: yfinance gives no reliable announcement time, so for each announcement
we consider the first session on/after it and the following session, and treat
whichever has the larger |gap| as the reaction session. That is the same
disambiguation the gap-size filter did, made explicit.
"""

import bisect
import logging
from datetime import timedelta

from strategies.earnings import announcement_dates

log = logging.getLogger(__name__)

# Gap-size buckets (absolute overnight gap %). Upper bound exclusive.
GAP_BUCKETS = [(0.0, 1.0), (1.0, 2.0), (2.0, 3.0), (3.0, 5.0), (5.0, 100.0)]


def _bucket_label(lo, hi):
    return f"{lo:g}-{hi:g}%" if hi < 100 else f"{lo:g}%+"


def _reaction_rows(ticker, bars, limit=24, raise_on_error=False):
    """One row per earnings event: the gap and how that session resolved."""
    if len(bars) < 2:
        return []
    sess = [b["time"].date() for b in bars]
    by_i = {i: b for i, b in enumerate(bars)}

    def _measure(i):
        """Stats for session index i, gapping from session i-1's close."""
        if i <= 0 or i >= len(bars):
            return None
        prev_close = by_i[i - 1]["close"]
        b = by_i[i]
        if not prev_close or not b["open"]:
            return None
        gap_pct = (b["open"] - prev_close) / prev_close * 100.0
        up = gap_pct >= 0
        # "Filled" = price traded back through the prior close during the session.
        filled = (b["low"] <= prev_close) if up else (b["high"] >= prev_close)
        # Follow-through vs fade, measured from the open (i.e. from where you
        # could actually have entered), signed in the gap's direction.
        c2o = (b["close"] - b["open"]) / b["open"] * 100.0
        return {
            "date":        b["time"].date().isoformat(),
            "prev_close":  round(prev_close, 4),
            "open":        round(b["open"], 4),
            "high":        round(b["high"], 4),
            "low":         round(b["low"], 4),
            "close":       round(b["close"], 4),
            "gap_pct":     round(gap_pct, 3),
            "abs_gap_pct": round(abs(gap_pct), 3),
            "direction":   "up" if up else "down",
            "filled":      bool(filled),
            "close_open_pct": round(c2o, 3),
            # Positive = continued in the gap's direction, negative = faded.
            "follow_pct":  round(c2o if up else -c2o, 3),
            "range_pct":   round((b["high"] - b["low"]) / b["open"] * 100.0, 3),
        }

    rows, used = [], set()
    for ann in announcement_dates(ticker, limit=limit, raise_on_error=raise_on_error):
        i = bisect.bisect_left(sess, ann)
        cands = [j for j in (i, i + 1) if 0 < j < len(bars)]
        measured = [m for m in (_measure(j) for j in cands) if m]
        if not measured:
            continue
        # Larger |gap| wins — disambiguates before-open vs after-close reporting.
        best = max(measured, key=lambda m: m["abs_gap_pct"])
        if best["date"] in used:
            continue
        used.add(best["date"])
        best["announced"] = ann.isoformat()
        rows.append(best)
    rows.sort(key=lambda r: r["date"])
    return rows


def _agg(rows):
    """Aggregate a set of event rows into readable stats."""
    n = len(rows)
    if not n:
        return {"events": 0, "fill_rate": 0.0, "avg_gap_pct": 0.0,
                "avg_follow_pct": 0.0, "avg_range_pct": 0.0, "faded_rate": 0.0}
    filled = sum(1 for r in rows if r["filled"])
    faded  = sum(1 for r in rows if r["follow_pct"] < 0)
    return {
        "events":         n,
        "fill_rate":      round(filled / n * 100.0, 1),
        "faded_rate":     round(faded / n * 100.0, 1),
        "avg_gap_pct":    round(sum(r["abs_gap_pct"] for r in rows) / n, 3),
        "avg_follow_pct": round(sum(r["follow_pct"] for r in rows) / n, 3),
        "avg_range_pct":  round(sum(r["range_pct"] for r in rows) / n, 3),
    }


def _split(rows):
    """Standard breakdown: overall, by direction, by gap-size bucket."""
    out = {
        "overall": _agg(rows),
        "by_direction": {
            "up":   _agg([r for r in rows if r["direction"] == "up"]),
            "down": _agg([r for r in rows if r["direction"] == "down"]),
        },
        "by_gap_bucket": [],
    }
    for lo, hi in GAP_BUCKETS:
        sel = [r for r in rows if lo <= r["abs_gap_pct"] < hi]
        out["by_gap_bucket"].append({
            "bucket": _bucket_label(lo, hi), "min_gap": lo,
            **_agg(sel),
            "up":   _agg([r for r in sel if r["direction"] == "up"]),
            "down": _agg([r for r in sel if r["direction"] == "down"]),
        })
    return out


def run_study(tickers, limit=24, fetch=None, min_gap_pct=0.0):
    """Earnings-reaction study across `tickers`.

    fetch(ticker, start, end, interval) -> list of daily bar dicts; injected so
    this is testable without network. min_gap_pct drops small non-events.
    Returns per-ticker rows/stats plus a pooled aggregate.
    """
    if fetch is None:
        from strategies.data import fetch_bars as fetch
    per_ticker, all_rows, errors = [], [], []
    for tk in tickers:
        tk = (tk or "").strip().upper()
        if not tk:
            continue
        try:
            anns = announcement_dates(tk, limit=limit, raise_on_error=True)
        except Exception as e:
            errors.append(f"{tk}: earnings calendar unavailable — {str(e)[:120]}")
            continue
        if not anns:
            errors.append(f"{tk}: calendar reached but returned no announcement dates")
            continue
        # Pad so the first event has a prior session to gap from.
        start = (min(anns) - timedelta(days=10)).isoformat()
        end   = (max(anns) + timedelta(days=10)).isoformat()
        try:
            bars = fetch(tk, start, end, "1d")
        except Exception as e:
            errors.append(f"{tk}: daily bars unavailable — {str(e)[:120]}")
            continue
        rows = _reaction_rows(tk, bars, limit=limit)
        if min_gap_pct:
            rows = [r for r in rows if r["abs_gap_pct"] >= min_gap_pct]
        if not rows:
            errors.append(f"{tk}: no earnings sessions could be measured")
            continue
        all_rows.extend(rows)
        per_ticker.append({"ticker": tk, "rows": rows, **_split(rows)})

    per_ticker.sort(key=lambda t: t["overall"]["events"], reverse=True)
    return {
        "tickers":     per_ticker,
        "pooled":      _split(all_rows),
        "total_events": len(all_rows),
        "errors":      errors,
        "min_gap_pct": min_gap_pct,
    }
