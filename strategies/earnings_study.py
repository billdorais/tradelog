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

# Gap measured in ATR units — the better normaliser. A 3% gap is a major event for
# JPM (avg range ~3%) and a rounding error for PLTR (~9%), so raw-% buckets pool
# incomparable events across a mixed basket. The first three tiers match the NQ
# futures gap study's tiers so results are directly comparable; earnings gaps run
# much larger than index gaps, hence the extra tiers on top.
ATR_BUCKETS = [(0.0, 0.3), (0.3, 0.7), (0.7, 1.2), (1.2, 2.0), (2.0, 3.0), (3.0, 1e9)]
ATR_PERIOD = 14


def _bucket_label(lo, hi):
    return f"{lo:g}-{hi:g}%" if hi < 100 else f"{lo:g}%+"


def _atr_label(lo, hi):
    return f"{lo:g}-{hi:g}x" if hi < 1e9 else f"{lo:g}x+"


def _atr_by_date(daily_bars, period=ATR_PERIOD):
    """date -> ATR *as of the close of that session*, from daily bars.

    Wilder true range: max(h-l, |h-prev_close|, |l-prev_close|). Callers must look
    up the session BEFORE the one being measured — using the reaction day's own ATR
    would leak that day's range into the bucket that classifies it.
    """
    bars = sorted(daily_bars, key=lambda b: b["time"])
    out, trs = {}, []
    for i, b in enumerate(bars):
        if i == 0:
            tr = b["high"] - b["low"]
        else:
            pc = bars[i - 1]["close"]
            tr = max(b["high"] - b["low"], abs(b["high"] - pc), abs(b["low"] - pc))
        trs.append(tr)
        if len(trs) >= period:
            out[b["time"].date()] = sum(trs[-period:]) / period
    return out


def _attach_atr(rows, atr_map, sess_dates):
    """Add atr / gap_atr to each event row, using the ATR as of the PRIOR session."""
    if not atr_map or not sess_dates:
        return rows
    ordered = sorted(sess_dates)
    for r in rows:
        try:
            d = _date(r["date"])
        except Exception:
            continue
        i = bisect.bisect_left(ordered, d)
        # Walk back to the most recent session with a defined ATR before this one.
        atr = None
        for j in range(i - 1, max(-1, i - 6), -1):
            if j < 0:
                break
            atr = atr_map.get(ordered[j])
            if atr:
                break
        if not atr:
            continue
        r["atr"] = round(atr, 4)
        r["atr_pct"] = round(atr / r["prev_close"] * 100.0, 3) if r["prev_close"] else None
        gap_abs = abs(r["open"] - r["prev_close"])
        r["gap_atr"] = round(gap_abs / atr, 3)
    return rows


def _date(iso):
    from datetime import date as _d
    y, m, d = (int(x) for x in iso.split("-"))
    return _d(y, m, d)


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
            # Did the session open back inside the PRIOR day's range, or clear of
            # it entirely? Opening inside means the fill target sits within
            # yesterday's already-traded territory.
            "opened_inside": bool(by_i[i - 1]["low"] <= b["open"] <= by_i[i - 1]["high"]),
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


def _to_et_rth(bars):
    """UTC-naive intraday bars -> ET-naive, regular session only (09:30-16:00).

    fetch_bars_alpaca returns naive UTC (many callers depend on that), so the
    conversion happens here rather than changing the shared fetcher.
    """
    from datetime import timezone
    from zoneinfo import ZoneInfo
    et  = ZoneInfo("America/New_York")
    out = []
    for b in bars:
        t = b["time"]
        try:
            t_et = t.replace(tzinfo=timezone.utc).astimezone(et).replace(tzinfo=None)
        except Exception:
            continue
        mins = t_et.hour * 60 + t_et.minute
        if 9 * 60 + 30 <= mins < 16 * 60:
            out.append({**b, "time": t_et})
    out.sort(key=lambda b: b["time"])
    return out


def _sessions_from_intraday(bars_et):
    """Group ET RTH bars into per-session OHLC, keeping the bar list for path work."""
    by_day = {}
    for b in bars_et:
        by_day.setdefault(b["time"].date(), []).append(b)
    sessions = []
    for d in sorted(by_day):
        bs = by_day[d]
        sessions.append({
            "date":  d,
            "open":  bs[0]["open"],
            "close": bs[-1]["close"],
            "high":  max(x["high"] for x in bs),
            "low":   min(x["low"] for x in bs),
            "bars":  bs,
        })
    return sessions


def _measure_session(prev, cur):
    """Same metrics as the daily path, plus intraday fill TIMING — the one thing
    daily bars cannot answer ('it filled' vs 'it filled 10 minutes in')."""
    prev_close, o = prev["close"], cur["open"]
    if not prev_close or not o:
        return None
    gap_pct = (o - prev_close) / prev_close * 100.0
    up = gap_pct >= 0
    filled, mins_to_fill = False, None
    session_open = cur["bars"][0]["time"]
    for b in cur["bars"]:
        if (b["low"] <= prev_close) if up else (b["high"] >= prev_close):
            filled = True
            mins_to_fill = int((b["time"] - session_open).total_seconds() // 60)
            break
    c2o = (cur["close"] - o) / o * 100.0
    # Direction of the first 15 minutes, relative to the FILL. For an up gap the
    # fill lies below the open, so a lower price at +15min is "toward fill". The
    # futures study found this the strongest single conditioner.
    first15 = [b for b in cur["bars"]
               if (b["time"] - session_open).total_seconds() < 15 * 60]
    toward = None
    if first15:
        p15 = first15[-1]["close"]
        if p15 != o:
            toward = (p15 < o) if up else (p15 > o)
    return {
        "date":       cur["date"].isoformat(),
        "prev_close": round(prev_close, 4),
        "open":       round(o, 4),
        "high":       round(cur["high"], 4),
        "low":        round(cur["low"], 4),
        "close":      round(cur["close"], 4),
        "gap_pct":     round(gap_pct, 3),
        "abs_gap_pct": round(abs(gap_pct), 3),
        "direction":   "up" if up else "down",
        "filled":      filled,
        "mins_to_fill": mins_to_fill,
        "close_open_pct": round(c2o, 3),
        "follow_pct":  round(c2o if up else -c2o, 3),
        "range_pct":   round((cur["high"] - cur["low"]) / o * 100.0, 3),
        "opened_inside":  bool(prev["low"] <= o <= prev["high"]),
        "first15_toward": toward,
    }


def _intraday_rows(ticker, anns, fetch, interval="5m", pad_days=6):
    """One row per earnings event, from intraday bars fetched per event window.

    Per-event windows rather than one multi-year pull: 6 years of 5m bars is
    ~120k rows per ticker, most of it irrelevant. A few sessions around each
    announcement is a fraction of that.
    """
    rows, used = [], set()
    for ann in anns:
        start = (ann - timedelta(days=pad_days)).isoformat()
        end   = (ann + timedelta(days=pad_days)).isoformat()
        try:
            raw = fetch(ticker, start, end, interval)
        except Exception as e:
            log.debug("intraday fetch failed %s %s: %s", ticker, ann, e)
            continue
        sess = _sessions_from_intraday(_to_et_rth(raw))
        if len(sess) < 2:
            continue
        # Candidate reaction sessions: first on/after the announcement, and the
        # next one (BMO vs AMC is not knowable from the calendar).
        cands = []
        for i in range(1, len(sess)):
            if sess[i]["date"] >= ann:
                cands = [j for j in (i, i + 1) if j < len(sess)]
                break
        measured = [m for m in (_measure_session(sess[j - 1], sess[j]) for j in cands) if m]
        if not measured:
            continue
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
    out = {
        "events":         n,
        "fill_rate":      round(filled / n * 100.0, 1),
        "faded_rate":     round(faded / n * 100.0, 1),
        "avg_gap_pct":    round(sum(r["abs_gap_pct"] for r in rows) / n, 3),
        "avg_follow_pct": round(sum(r["follow_pct"] for r in rows) / n, 3),
        "avg_range_pct":  round(sum(r["range_pct"] for r in rows) / n, 3),
    }
    # Intraday only: how long fills took, and how many happened in the first
    # 30 min — a fill at minute 5 and one at minute 380 are different trades.
    fills = [r["mins_to_fill"] for r in rows if r.get("mins_to_fill") is not None]
    if fills:
        fills_sorted = sorted(fills)
        out["median_mins_to_fill"] = fills_sorted[len(fills_sorted) // 2]
        out["fill_within_30m_pct"] = round(
            sum(1 for m in fills if m <= 30) / n * 100.0, 1)
    return out


def _split(rows):
    """Standard breakdown: overall, by direction, by gap-size bucket."""
    out = {
        "overall": _agg(rows),
        "by_direction": {
            "up":   _agg([r for r in rows if r["direction"] == "up"]),
            "down": _agg([r for r in rows if r["direction"] == "down"]),
        },
        "by_gap_bucket": [],
        "by_atr_bucket": [],
    }
    for lo, hi in GAP_BUCKETS:
        sel = [r for r in rows if lo <= r["abs_gap_pct"] < hi]
        out["by_gap_bucket"].append({
            "bucket": _bucket_label(lo, hi), "min_gap": lo,
            **_agg(sel),
            "up":   _agg([r for r in sel if r["direction"] == "up"]),
            "down": _agg([r for r in sel if r["direction"] == "down"]),
        })
    # Volatility-normalised view. Only events with a computable ATR take part, so
    # report the coverage rather than letting a partial sample look complete.
    with_atr = [r for r in rows if r.get("gap_atr") is not None]
    out["atr_coverage"] = {"events_with_atr": len(with_atr), "events": len(rows)}
    for lo, hi in ATR_BUCKETS:
        sel = [r for r in with_atr if lo <= r["gap_atr"] < hi]
        out["by_atr_bucket"].append({
            "bucket": _atr_label(lo, hi), "min_atr": lo,
            **_agg(sel),
            "up":   _agg([r for r in sel if r["direction"] == "up"]),
            "down": _agg([r for r in sel if r["direction"] == "down"]),
            # Conditioners WITHIN a size bucket. Both correlate with gap size
            # (a big gap usually clears the prior range by construction), so the
            # marginal splits below overstate them; these controlled cells are
            # what actually says whether a conditioner adds information.
            "inside":  _agg([r for r in sel if r.get("opened_inside") is True]),
            "outside": _agg([r for r in sel if r.get("opened_inside") is False]),
            "f15_toward": _agg([r for r in sel if r.get("first15_toward") is True]),
            "f15_away":   _agg([r for r in sel if r.get("first15_toward") is False]),
        })

    # Marginal (uncontrolled) conditioner splits.
    out["by_open_location"] = {
        "inside":  _agg([r for r in rows if r.get("opened_inside") is True]),
        "outside": _agg([r for r in rows if r.get("opened_inside") is False]),
    }
    f15 = [r for r in rows if r.get("first15_toward") is not None]
    out["by_first15"] = {
        "available": len(f15),
        "toward": _agg([r for r in f15 if r["first15_toward"]]),
        "away":   _agg([r for r in f15 if not r["first15_toward"]]),
    }
    return out


def run_study(tickers, limit=24, fetch=None, min_gap_pct=0.0,
              source="daily", interval="5m", daily_fetch=None):
    """Earnings-reaction study across `tickers`.

    source="daily"    — yfinance daily bars. Free, ~6y, but no intraday path.
    source="intraday" — Alpaca intraday bars (needs ALPACA_KEY; SIP if
                        ALPACA_DATA_FEED=sip). Alpaca's history goes back to 2016,
                        so unlike yfinance's 59-day intraday cap this covers the
                        whole calendar AND yields fill timing.

    fetch(ticker, start, end, interval) -> bar dicts; injected for testing.
    """
    intraday = str(source).lower() == "intraday"
    if fetch is None:
        if intraday:
            from strategies.data import fetch_bars_alpaca as fetch
        else:
            from strategies.data import fetch_bars as fetch
    # Same source for the ATR denominator; both fetchers accept interval="1d".
    daily_fetch = daily_fetch or fetch
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
        # Daily bars serve two purposes: the daily measurement path, and the ATR
        # denominator for BOTH paths. Intraday windows are only a few sessions
        # wide — far too short for a 14-period ATR — so intraday mode fetches one
        # daily series per ticker purely to normalise gap size.
        daily_bars = None
        start = (min(anns) - timedelta(days=90)).isoformat()
        end   = (max(anns) + timedelta(days=10)).isoformat()
        try:
            daily_bars = daily_fetch(tk, start, end, "1d")
        except Exception as e:
            errors.append(f"{tk}: daily bars for ATR unavailable — {str(e)[:100]}")

        if intraday:
            rows = _intraday_rows(tk, anns, fetch, interval=interval)
            if not rows:
                errors.append(f"{tk}: no intraday bars returned for any earnings window "
                              f"(check ALPACA_KEY / entitlement for {interval})")
        else:
            if daily_bars is None:
                continue
            rows = _reaction_rows(tk, daily_bars, limit=limit)

        if daily_bars and rows:
            _attach_atr(rows, _atr_by_date(daily_bars),
                        [b["time"].date() for b in daily_bars])
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
        "source":      "intraday" if intraday else "daily",
        "interval":    interval if intraday else "1d",
    }
