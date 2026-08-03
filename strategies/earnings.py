"""
Earnings calendar — historical + upcoming announcement dates per ticker.

Source: yfinance (already a dependency). We don't get a reliable BMO/AMC time, so
`earnings_gap_days` marks BOTH the first session on/after each announcement AND the
next session as candidate reaction days; the gap-size filter in the strategy then
disambiguates which one actually gapped. Cached in-memory (earnings dates move
slowly) so a 20-ticker backtest hits the network once per ticker per TTL.

Reusable beyond the backtest — this is the calendar layer for any future
earnings-aware logic (gates, live gap-fill, dashboards).
"""

import bisect
import logging
import time as _time

log = logging.getLogger(__name__)

_CACHE = {}                 # TICKER -> (fetched_at, [date, ...])
_TTL   = 6 * 3600           # 6h — earnings dates don't change intraday


def announcement_dates(ticker, limit=24, _force=False, raise_on_error=False):
    """Sorted list of earnings announcement dates (datetime.date) for a ticker,
    historical + upcoming, from yfinance. Cached.

    Returns [] on failure by default. Pass raise_on_error=True when an empty
    calendar would be misread as "this ticker has no earnings" — a broken fetch
    (e.g. the missing-lxml case, which made every earnings backtest report 0
    trades) is otherwise indistinguishable from a genuinely empty result."""
    key = (ticker or "").upper()
    if not key:
        return []
    hit = _CACHE.get(key)
    if hit and not _force and (_time.time() - hit[0]) < _TTL:
        return hit[1]
    dates = []
    try:
        import yfinance as yf
        import pandas as pd
        t  = yf.Ticker(key)
        df = None
        try:
            df = t.get_earnings_dates(limit=limit)
        except Exception:
            df = getattr(t, "earnings_dates", None)
        if df is not None and len(df):
            for ts in pd.DatetimeIndex(df.index):
                dates.append(ts.date())
    except Exception as e:
        log.warning("earnings fetch failed for %s: %s", key, e)
        if raise_on_error:
            raise
    dates = sorted(set(dates))
    _CACHE[key] = (_time.time(), dates)
    return dates


def earnings_gap_days(ticker, session_dates, limit=24, raise_on_error=False):
    """Subset of `session_dates` that are earnings-reaction sessions: for each
    announcement date, the first session on/after it (BMO case) plus the following
    session (AMC case). Combined with the strategy's gap-size filter, this isolates
    the actual earnings-gap day. `session_dates` = the trading dates present in the
    backtest data (used as the trading calendar). Returns a set of datetime.date."""
    sess = sorted(set(session_dates))
    if not sess:
        return set()
    out = set()
    for d in announcement_dates(ticker, limit=limit, raise_on_error=raise_on_error):
        i = bisect.bisect_left(sess, d)
        if i < len(sess):
            out.add(sess[i])                 # announcement session (BMO)
            if i + 1 < len(sess):
                out.add(sess[i + 1])         # next session (AMC)
    return out
