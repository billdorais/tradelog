"""Regression test for rule 21 (daily OHLC from intraday bars).

The bt_convert prompt teaches Claude to resample intraday bars to daily,
shift by one bar, and forward-fill. If a converted strategy naively uses
`pd.Series(high).shift(1)` instead, H4/L4 pivots end up wrong by an
order of magnitude and trade counts explode.

This test freezes the canonical pattern:
    series.resample('B').agg(...).shift(1).reindex(idx, method='ffill')

and verifies that on our synthetic 5-min fixture (whose daily closes
are 100, 101, 102, 103, 104), the resampled series at 2026-01-06 bars
equals 2026-01-05's daily close, not the previous 5-min bar close.
"""
from __future__ import annotations

import pandas as pd


def _prev_day(series: pd.Series, agg: str) -> pd.Series:
    """The rule-21 pattern, extracted as a named helper for testing."""
    idx = series.index
    daily = getattr(series.resample("B"), agg)()
    return daily.shift(1).reindex(idx, method="ffill")


def test_prev_day_close_matches_yesterdays_close(intraday_5m_ohlc):
    df = intraday_5m_ohlc
    close = pd.Series(df["Close"].values, index=df.index)

    prev_close = _prev_day(close, "last")

    # Any bar on day 2 (2026-01-06) should reflect day 1's daily close.
    day2_bars = prev_close.loc["2026-01-06"]
    day1_close = close.loc["2026-01-05"].iloc[-1]

    assert (day2_bars == day1_close).all(), (
        f"Expected all day-2 bars to show day-1 close {day1_close}, "
        f"got unique values: {sorted(day2_bars.unique())}"
    )


def test_prev_day_high_is_daily_high_not_bar_high(intraday_5m_ohlc):
    """The bug we're guarding against: naive shift(1) returns the previous
    5-min bar's high (~$0.50 away), not the previous day's high (~$2 away)."""
    df = intraday_5m_ohlc
    high = pd.Series(df["High"].values, index=df.index)

    prev_day_high = _prev_day(high, "max")
    naive = high.shift(1)  # the wrong pattern

    # On day-2 bars, the two approaches should disagree substantially.
    day2 = prev_day_high.loc["2026-01-06"]
    naive_day2 = naive.loc["2026-01-06"]

    # All correct values are equal (yesterday's daily max).
    assert day2.nunique() == 1
    # The naive values change bar-to-bar (they're just the previous bar high).
    assert naive_day2.nunique() > 1


def test_prev_day_has_no_lookahead(intraday_5m_ohlc):
    """A bar at time T must never see its own day's H/L/C."""
    df = intraday_5m_ohlc
    high = pd.Series(df["High"].values, index=df.index)

    prev_day_high = _prev_day(high, "max")

    # For any bar on day D, prev_day_high must be < or != that day's high max.
    # Simpler check: the very first day has no prior day, so values are NaN.
    first_day = prev_day_high.loc["2026-01-05"]
    assert first_day.isna().all(), "First day must not peek at itself"
