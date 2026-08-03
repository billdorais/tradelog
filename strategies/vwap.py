"""
Session-anchored VWAP helpers for intraday backtests.

The backtester has no native VWAP — the only prior reference was a 20-bar SMA
*proxy* in the AI code-gen prompt, which is not a true session VWAP and has no
concept of the prior day. These helpers compute:

  session_vwap()    — the CURRENT session's running VWAP, reset at each ET day
                      boundary (cumulative typical-price*volume / cumulative volume).
  prior_day_vwap()  — each bar carries the PREVIOUS session's FINAL VWAP, held
                      constant across the day (the "prior-day VWAP" magnet/target).
  gap_pct()         — each bar carries the day's opening gap: (today's first open −
                      prior day's last close) / prior close, in percent.

All operate on plain numpy arrays + a DatetimeIndex whose values are ET-local
(that's what app._filter_rth returns), so day boundaries are `index.date`. Bars
before the first full prior session return NaN (no reference yet).
"""

import numpy as np


def _dates(index):
    """Per-bar date array (datetime.date) from a DatetimeIndex-like object."""
    import pandas as pd
    return pd.DatetimeIndex(index).date


def session_vwap(high, low, close, volume, index):
    """Running VWAP within each ET session, reset at every day boundary.
    Falls back to the bar's typical price when a session has zero cumulative
    volume (so it's never NaN mid-session)."""
    high   = np.asarray(high,   dtype=float)
    low    = np.asarray(low,    dtype=float)
    close  = np.asarray(close,  dtype=float)
    volume = np.asarray(volume, dtype=float)
    tp     = (high + low + close) / 3.0
    dates  = _dates(index)
    n      = len(close)
    out    = np.full(n, np.nan)
    cum_pv = cum_v = 0.0
    cur    = None
    for i in range(n):
        d = dates[i]
        if d != cur:
            cur = d
            cum_pv = cum_v = 0.0
        v = volume[i] if (volume[i] == volume[i] and volume[i] > 0) else 0.0
        cum_pv += tp[i] * v
        cum_v  += v
        out[i] = (cum_pv / cum_v) if cum_v > 0 else tp[i]
    return out


def prior_day_vwap(high, low, close, volume, index):
    """Each bar carries the PREVIOUS session's final session-VWAP value, held flat
    across the current day. NaN on the first session (no prior reference)."""
    sv    = session_vwap(high, low, close, volume, index)
    dates = _dates(index)
    n     = len(close)
    out   = np.full(n, np.nan)
    prev_final = np.nan
    cur        = None
    last_sv    = np.nan
    for i in range(n):
        d = dates[i]
        if cur is None:
            cur = d
        elif d != cur:
            prev_final = last_sv        # finalize the day that just ended
            cur = d
        out[i]  = prev_final
        last_sv = sv[i]
    return out


def gap_pct(open_, close, index):
    """Per-bar opening gap %, constant across the day: (today's first open − prior
    day's last close) / prior close × 100. NaN on the first session."""
    open_ = np.asarray(open_, dtype=float)
    close = np.asarray(close, dtype=float)
    dates = _dates(index)
    n     = len(close)
    out   = np.full(n, np.nan)
    cur        = None
    day_open   = np.nan
    prev_close = np.nan
    last_close = np.nan
    for i in range(n):
        d = dates[i]
        if cur is None:
            cur = d
            day_open = open_[i]
        elif d != cur:
            prev_close = last_close
            cur = d
            day_open = open_[i]
        out[i] = ((day_open - prev_close) / prev_close * 100.0) \
            if (prev_close == prev_close and prev_close != 0) else np.nan
        last_close = close[i]
    return out
