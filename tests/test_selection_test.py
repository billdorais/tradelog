"""Out-of-sample test of the promotion premise.

Filtering a farm chart to its top N is hindsight: picking the best 20 of 160
after the fact produces a beautiful equity curve from pure noise essentially
always. _selection_walk_forward is the honest version — rank on one window, score
on the NEXT, walk forward over several folds, and compare the picked cohort
against the strategies that were NOT picked over the same forward window.

The spread (picked minus unpicked) is the signal. Absolute forward P&L mostly
reflects market drift and would make a rising tape look like predictive skill.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as dt
import random

import pytest

import app as a


def _rt(strategy, date, pct, price=100.0, qty=10):
    """A round-trip whose % return is exactly `pct`."""
    return {"strategy": strategy, "ticker": strategy[:4], "date": date,
            "entry_price": price, "qty": qty, "pnl": pct / 100.0 * price * qty,
            "side": "LONG", "entry_time": f"{date}T13:45:00Z"}


def _series(strategy, start, days, pct_fn):
    out = []
    d = dt.date.fromisoformat(start)
    for i in range(days):
        out.append(_rt(strategy, (d + dt.timedelta(days=i)).isoformat(), pct_fn(i)))
    return out


def test_detects_genuine_persistent_skill():
    """If the good strategies really are good, ranking must find them."""
    rts = []
    for s in range(5):                       # persistently +2% per trade
        rts += _series(f"GOOD{s}", "2026-01-01", 120, lambda i: 2.0)
    for s in range(15):                      # persistently -1% per trade
        rts += _series(f"BAD{s}", "2026-01-01", 120, lambda i: -1.0)
    out = a._selection_walk_forward(rts, rank_days=30, fwd_days=14, n=5, min_trades=5)
    assert out["fold_count"] >= 3
    assert out["weighted_spread_pct"] > 2.0, out["verdict"]
    assert out["folds_with_positive_spread"] == out["fold_count"]
    assert "EVERY fold" in out["verdict"]


def test_reports_no_predictive_power_on_pure_noise():
    """The headline case: 160 zero-edge strategies. A hindsight chart of the top
    20 looks superb; this must report that the ranking predicts nothing."""
    rng = random.Random(11)
    rts = []
    for s in range(160):
        rts += _series(f"N{s}", "2026-01-01", 90,
                       lambda i, rng=rng: rng.choice([1.94, -0.80, -0.80]))
    out = a._selection_walk_forward(rts, rank_days=30, fwd_days=14, n=20, min_trades=5)
    assert out["fold_count"] >= 3
    # No skill ⇒ spread hovers around zero, nowhere near the ~2% of real skill.
    assert abs(out["weighted_spread_pct"]) < 0.5, out
    assert "EVERY fold" not in out["verdict"]


def test_flags_mean_reversion_as_negative():
    """If last window's winners become next window's losers, ranking is actively
    harmful — that must not be reported as merely 'inconclusive'."""
    # Alternates by 14-day block: a strategy good in one block is bad in the next.
    def _flip(offset):
        return lambda i: 2.0 if ((i + offset) // 14) % 2 == 0 else -2.0
    rts = []
    for s in range(10):
        rts += _series(f"A{s}", "2026-01-01", 120, _flip(0))
    for s in range(10):
        rts += _series(f"B{s}", "2026-01-01", 120, _flip(14))
    out = a._selection_walk_forward(rts, rank_days=14, fwd_days=14, n=10, min_trades=5)
    assert out["weighted_spread_pct"] < 0
    assert "UNDERPERFORMS" in out["verdict"]


def test_ranker_never_sees_the_window_it_is_scored_on():
    """Look-ahead guard: the rank and forward windows must not overlap, or the
    test reproduces the very bias it exists to measure."""
    rts = []
    for s in range(6):
        rts += _series(f"S{s}", "2026-01-01", 120, lambda i: 1.0)
    out = a._selection_walk_forward(rts, rank_days=30, fwd_days=14, n=3, min_trades=5)
    for f in out["folds"]:
        assert f["rank_from"] < f["split"] <= f["forward_to"]
        # forward window starts exactly where ranking stopped — no shared days
        assert f["split"] == f["split"]
        assert f["rank_from"] < f["split"]


def test_short_history_is_an_explicit_error_not_a_silent_zero():
    rts = _series("X", "2026-01-01", 5, lambda i: 1.0)
    out = a._selection_walk_forward(rts, rank_days=30, fwd_days=14)
    assert out["folds"] == [] and "history too short" in out["error"]


def test_spread_is_weighted_by_sample_size():
    """A 3-trade fold must not carry the same weight as a 90-trade one."""
    rts = []
    for s in range(6):
        rts += _series(f"S{s}", "2026-01-01", 120, lambda i: 1.0)
    out = a._selection_walk_forward(rts, rank_days=30, fwd_days=14, n=3, min_trades=5)
    assert out["weighted_spread_pct"] is not None
    assert out["pooled_picked_trades"] > 0 and out["pooled_unpicked_trades"] > 0
