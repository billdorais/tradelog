"""Regression tests for rule 17: entry crossover detection.

Pine Script has two distinct crossover patterns that the converted Python
code must mirror exactly:

    (a) ta.crossover(close, L):
            Close[-2] < L  AND  Close[-1] >= L
        Fires when the CLOSED bar crossed. Safe to evaluate in next().

    (b) explicit 'close > L AND open < L' (same-bar cross):
            Open[-1] < L  AND  Close[-1] > L
        Fires on the bar whose open was below and whose close is above.

If the converted code uses `close > L` alone, the entry fires on every
bar above the level — that's what caused the 18,175-trade explosion.
"""
from __future__ import annotations


# ── Rule 17(a): ta.crossover(close, L) semantics ─────────────────────
def _cross_ta(close_prev: float, close_now: float, level: float) -> bool:
    return bool(close_prev < level and close_now >= level)


def test_ta_crossover_fires_on_cross_bar(crossover_fixture):
    df = crossover_fixture
    # Bar 1: close[-2]=99 (bar0) < 100, close[-1]=101 (bar1) >= 100 → fire
    assert _cross_ta(df.Close.iloc[0], df.Close.iloc[1], 100.0)


def test_ta_crossover_silent_when_already_above(crossover_fixture):
    df = crossover_fixture
    # Bar 2: close[-2]=101 (bar1) ≥ 100 already → no cross
    assert not _cross_ta(df.Close.iloc[1], df.Close.iloc[2], 100.0)


def test_ta_crossover_silent_when_below(crossover_fixture):
    df = crossover_fixture
    # Bar 0 alone: no prior bar with close<L and then close≥L
    assert not _cross_ta(df.Close.iloc[0] - 1, df.Close.iloc[0], 100.0)


# ── Rule 17(b): same-bar 'close > L and open < L' semantics ──────────
def _cross_same_bar(open_now: float, close_now: float, level: float) -> bool:
    return bool(open_now < level and close_now > level)


def test_same_bar_crossover_fires_when_bar_spans_level(crossover_fixture):
    df = crossover_fixture
    # Bar 1: open=99 < 100, close=101 > 100 → fire
    assert _cross_same_bar(df.Open.iloc[1], df.Close.iloc[1], 100.0)


def test_same_bar_crossover_silent_when_gap_opens_above(crossover_fixture):
    df = crossover_fixture
    # Bar 2: open=101 already above level → must not fire
    assert not _cross_same_bar(df.Open.iloc[2], df.Close.iloc[2], 100.0)


def test_close_only_comparison_overfires(crossover_fixture):
    """Documents the bug: plain `close > L` fires on bar-2 AND bar-1,
    whereas both correct patterns fire only on bar-1. This is why rule
    17 forbids `close > L` alone."""
    df = crossover_fixture
    close_only_fires = [c > 100.0 for c in df.Close]
    correct_fires = [_cross_same_bar(df.Open.iloc[i], df.Close.iloc[i], 100.0)
                     for i in range(len(df))]

    assert close_only_fires.count(True) == 2  # over-fires
    assert correct_fires.count(True) == 1     # correct


# ── Rule 20: tick → dollar conversion ────────────────────────────────
def test_tick_to_dollar_conversion_us_equity():
    """Pine tick integers must be multiplied by syminfo.mintick ($0.01 for
    US equities) before being passed as dollar values to backtesting.py."""
    TICK_SIZE = 0.01

    pine_trail_points = 40   # ticks in Pine
    pine_loss = 80           # ticks in Pine

    trail_dollars = pine_trail_points * TICK_SIZE
    loss_dollars = pine_loss * TICK_SIZE

    assert trail_dollars == 0.40
    assert loss_dollars == 0.80
    # The bug: using raw ticks as dollars means a $40 trailing stop on a
    # $100 stock, which never triggers — positions run for days.
    assert trail_dollars < 1.0, "Sanity: trailing stop should be under $1"
