"""Per-underlying cap on a top-N snapshot.

The composite score ranks names in isolation. Nothing in it can see that
SPY R3S3 long, SPY R4S4 long and SPY R4S4 short are three bets on one instrument
— so the book's realised variance is far worse than the per-strategy stats imply.

The 2026-09-09 TV snapshot is the case: SPY x3, IWM x3, GLD x2 filled 8 of 18
slots across three underlyings, several the same level pair in both directions.

The cap changes the MIX, never the size. A diversification preference that
quietly shrinks the book would be a much bigger change than the one intended.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

from collections import Counter

import pytest

import app as a


def _rows(*names):
    """Score-sorted rows, descending, in the shape _apply_ticker_cap consumes."""
    return [(n, {}, 100 - i) for i, n in enumerate(names)]


def _tickers(sel):
    return Counter(n.split("_", 1)[0] for n, _, _ in sel)


# ── the real snapshot ───────────────────────────────────────────────────────

def test_the_2026_09_09_snapshot_loses_its_triples():
    """SPY and IWM held three slots each. With the cap they hold two, and the
    freed slots go to underlyings the book had no exposure to at all."""
    sel, displaced = a._apply_ticker_cap(_rows(
        "GLD_CAM_REVERSAL_R3S3", "HOOD_CAM_REVERSAL_R4S4", "NVDA_CAM_BREAKOUT_R3S3",
        "MU_CAM_REVERSAL_R3S3", "INTC_CAM_BREAKOUT_R4S4", "V_CAM_BREAKOUT_R3S3",
        "GOOG_CAM_BREAKOUT_R4S4", "SPCX_CAM_BREAKOUT_R3S3", "SOXL_CAM_BREAKOUT_R3S3",
        "TSLA_CAM_BREAKOUT_R4S4", "IWM_CAM_BREAKOUT_R3S3", "GLD_CAM_BREAKOUT_R4S4",
        "IWM_CAM_REVERSAL_R3S3", "SPY_CAM_BREAKOUT_R4S4", "SPY_CAM_REVERSAL_R4S4",
        "IWM_CAM_BREAKOUT_R4S4", "SPY_CAM_REVERSAL_R3S3", "NFLX_CAM_REVERSAL_R3S3",
        "AMD_CAM_BREAKOUT_R3S3", "META_CAM_REVERSAL_R4S4"), 18)
    assert len(sel) == 18, "the book must stay full"
    assert max(_tickers(sel).values()) == 2
    assert displaced == ["IWM_CAM_BREAKOUT_R4S4", "SPY_CAM_REVERSAL_R3S3"]
    names = {n for n, _, _ in sel}
    assert {"AMD_CAM_BREAKOUT_R3S3", "META_CAM_REVERSAL_R4S4"} <= names


# ── the cap itself ──────────────────────────────────────────────────────────

def test_no_ticker_exceeds_the_cap():
    sel, _ = a._apply_ticker_cap(_rows(*[f"SPY_S{i}" for i in range(5)],
                                       "QQQ_A", "IWM_A"), 4)
    assert _tickers(sel)["SPY"] <= a._REFINED_MAX_PER_TICKER


def test_the_highest_scoring_names_for_a_ticker_are_the_ones_kept():
    """Order within a ticker must be preserved — the cap drops the WORST duplicates."""
    # Needs slack: with a pool the same size as n, backfill correctly restores
    # everything rather than shrinking the book, and nothing is truly displaced.
    sel, displaced = a._apply_ticker_cap(
        _rows("SPY_BEST", "SPY_MID", "SPY_WORST", "QQQ_A", "IWM_A"), 4)
    kept = [n for n, _, _ in sel]
    assert "SPY_BEST" in kept and "SPY_MID" in kept
    assert "SPY_WORST" in displaced and "SPY_WORST" not in kept


def test_ranking_order_is_otherwise_untouched():
    sel, _ = a._apply_ticker_cap(_rows("A_X", "B_X", "C_X", "D_X"), 4)
    assert [n for n, _, _ in sel] == ["A_X", "B_X", "C_X", "D_X"]


# ── it must never shrink the book ───────────────────────────────────────────

def test_a_pool_of_one_ticker_still_fills_every_slot():
    """Otherwise a thin day silently halves the book — a much bigger change than
    the diversification this is meant to buy."""
    sel, displaced = a._apply_ticker_cap(_rows("SPY_A", "SPY_B", "SPY_C", "SPY_D"), 4)
    assert len(sel) == 4
    assert displaced == [], "nothing was truly displaced — they all got in"


def test_backfill_takes_the_best_of_the_displaced():
    sel, _ = a._apply_ticker_cap(_rows("SPY_A", "SPY_B", "SPY_C", "SPY_D", "QQQ_A"), 4)
    kept = [n for n, _, _ in sel]
    assert "SPY_C" in kept and "SPY_D" not in kept


def test_a_short_pool_is_returned_whole():
    sel, _ = a._apply_ticker_cap(_rows("A_X", "B_X"), 18)
    assert len(sel) == 2


def test_an_empty_pool_is_safe():
    assert a._apply_ticker_cap([], 18) == ([], [])


# ── the escape hatch ────────────────────────────────────────────────────────

def test_cap_zero_disables_it_entirely():
    """REFINED_MAX_PER_TICKER=0 must restore the previous behaviour exactly, so the
    change can be reverted from config without a deploy."""
    rows = _rows("SPY_A", "SPY_B", "SPY_C", "QQQ_A")
    assert a._apply_ticker_cap(rows, 4, cap=0) == (rows[:4], [])


def test_the_cap_is_configurable():
    sel, _ = a._apply_ticker_cap(
        _rows("SPY_A", "SPY_B", "SPY_C", "QQQ_A", "IWM_A", "GLD_A"), 3, cap=1)
    assert _tickers(sel)["SPY"] == 1


def test_the_default_is_two():
    assert a._REFINED_MAX_PER_TICKER == 2


# ── both snapshots use it ───────────────────────────────────────────────────

@pytest.mark.parametrize("fn", ["_do_refresh_refined", "_do_refresh_kairos_refined"])
def test_both_books_apply_the_cap(fn):
    """TV and Kairos must stay comparable — capping one and not the other would
    make the head-to-head measure the cap instead of the entry mechanism."""
    import inspect
    src = inspect.getsource(getattr(a, fn))
    assert "_apply_ticker_cap(scored, n)" in src
    assert "scored[:n]" not in src, "the uncapped slice must be gone"


def test_backfill_only_engages_when_the_pool_cannot_fill_the_book():
    """The cap is a preference, not a quota. With slack it bites; without slack the
    book stays full and the cap yields — production has slack (all eligible
    candidates vs n=20), so the starved case is the exception, not the norm."""
    tight = _rows("SPY_A", "SPY_B", "SPY_C")                 # pool == n
    assert len(a._apply_ticker_cap(tight, 3)[0]) == 3
    assert _tickers(a._apply_ticker_cap(tight, 3)[0])["SPY"] == 3

    slack = _rows("SPY_A", "SPY_B", "SPY_C", "QQQ_A", "IWM_A")
    assert _tickers(a._apply_ticker_cap(slack, 3)[0])["SPY"] == 2
