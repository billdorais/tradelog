"""Side/band diagnostics must keep the strategy kind.

The band key dropped everything before "_CAM_", so BREAKOUT_x_CAM_R4_S4 and
REVERSAL_x_CAM_R4_S4 both bucketed as "R4 S4". Breakouts and reversals on the
same level can run opposite, so the merged cell hid which one bled — and the
crew reads these cells to decide what to gate.

The numbers below are TV Refined's real 2026-07-01..17 R4S4 longs, the cohort
that exposed this: breakout longs ~flat (-$8.25 / 7t), reversal longs bleeding
(-$233.57 / 8t). Merged they read as one losing "R4S4 LONG" (-$241.82 / 15t),
which invites gating the level and killing the working breakouts.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


@pytest.mark.parametrize("strategy,expected", [
    ("BREAKOUT_AAPL_CAM_R4_S4", "BREAKOUT R4S4"),
    ("REVERSAL_AMD_CAM_R4_S4",  "REVERSAL R4S4"),
    ("BREAKOUT_TSLA_CAM_R3_S3", "BREAKOUT R3S3"),
    ("REVERSAL_NVDA_CAM_R3_S3", "REVERSAL R3S3"),
    ("breakout_lower_CAM_R3_S3", "BREAKOUT R3S3"),   # case-insensitive
    ("Unknown", "OTHER"),
    ("SOMETHING_CAM_R4", "OTHER"),                   # half a level → not a band
    ("", "OTHER"),
    (None, "OTHER"),
])
def test_kind_band_keeps_the_strategy_kind(strategy, expected):
    assert a._kind_band(strategy) == expected


def test_same_level_different_kind_no_longer_collides():
    assert a._kind_band("BREAKOUT_X_CAM_R4_S4") != a._kind_band("REVERSAL_X_CAM_R4_S4")


def test_take_profit_band_key_stays_level_only():
    """_strategy_band is the per-account take-profit lookup key — it must keep
    matching a level regardless of kind, or account TPs silently stop applying."""
    assert a._strategy_band("BREAKOUT_AAPL_CAM_R4_S4") == "R4_S4"
    assert a._strategy_band("REVERSAL_AMD_CAM_R4_S4") == "R4_S4"


def _bucket(trades, band_fn):
    """Same (band, side) bucketing the diagnostics and journal do."""
    out = {}
    for t in trades:
        key = (band_fn(t["strategy"]), t["side"])
        b = out.setdefault(key, {"trades": 0, "pnl": 0.0})
        b["trades"] += 1
        b["pnl"] = round(b["pnl"] + t["pnl"], 2)
    return out


def test_kind_aware_bucketing_separates_the_bleed():
    # TV Refined R4S4 longs, 2026-07-01..17: flat breakouts, bleeding reversals.
    # Whole-cent legs so the running round() in _bucket stays exact.
    _bo = [-1.18] * 6 + [-1.17]                      # 7 trades → -8.25
    _rv = [-29.20] * 7 + [-29.17]                    # 8 trades → -233.57
    trades = (
        [{"strategy": "BREAKOUT_T_CAM_R4_S4", "side": "LONG", "pnl": p} for p in _bo] +
        [{"strategy": "REVERSAL_T_CAM_R4_S4", "side": "LONG", "pnl": p} for p in _rv]
    )

    # Old behaviour: one merged cell that blames the level as a whole.
    def _legacy_band(strat):
        s = (strat or "").upper(); i = s.find("_CAM_")
        p = s[i + 5:].split("_") if i >= 0 else []
        return f"{p[0]} {p[1]}" if len(p) >= 2 else "OTHER"

    merged = _bucket(trades, _legacy_band)
    assert len(merged) == 1
    assert merged[("R4 S4", "LONG")]["trades"] == 15
    assert merged[("R4 S4", "LONG")]["pnl"] == pytest.approx(-241.82, abs=0.01)

    # Kind-aware: the reversals own ~97% of the loss; breakouts are ~flat.
    split = _bucket(trades, a._kind_band)
    assert set(split) == {("BREAKOUT R4S4", "LONG"), ("REVERSAL R4S4", "LONG")}
    assert split[("BREAKOUT R4S4", "LONG")]["pnl"] == pytest.approx(-8.25, abs=0.01)
    assert split[("REVERSAL R4S4", "LONG")]["pnl"] == pytest.approx(-233.57, abs=0.01)
    # The cell a "worst first" sort surfaces is now the reversals, not the level.
    worst = min(split.items(), key=lambda kv: kv[1]["pnl"])[0]
    assert worst == ("REVERSAL R4S4", "LONG")
