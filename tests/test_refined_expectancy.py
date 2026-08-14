"""Refined scoring: expectancy (magnitude) term.

Before this, the composite score was Sharpe/PF/Win/Trades — all pure RATIOS. Two
strategies with identical PF, win rate, Sharpe and trade count scored identically
even if one earned +0.01%/trade and the other +0.30%/trade. Expectancy adds that
missing magnitude dimension.

It is measured as a % of NOTIONAL, never raw dollars: position size is assigned BY
the score (_REFINED_SIZE_BANDS), so ranking on total $ would be self-reinforcing
(bigger size -> bigger $ -> higher rank -> bigger size).
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a


def _base(**over):
    s = {"sharpe": 1.0, "profit_factor": 1.5, "win_rate": 50.0, "trades": 10}
    s.update(over)
    return s


def test_weights_sum_to_one():
    assert round(sum(a._REFINED_SCORE_WEIGHTS.values()), 6) == 1.0
    assert "expectancy" in a._REFINED_SCORE_WEIGHTS


def test_magnitude_now_separates_identical_ratio_strategies():
    """Same PF/win/Sharpe/trades — only per-trade magnitude differs."""
    grind = _base(expectancy_pct=0.01)     # +0.01%/trade
    meaty = _base(expectancy_pct=0.20)     # +0.20%/trade (past saturation)
    assert a._composite_score(meaty, 0) > a._composite_score(grind, 0)
    # The gap is the full expectancy weight less the grinder's sliver.
    gap = a._composite_score(meaty, 0) - a._composite_score(grind, 0)
    assert 0.10 < gap <= a._REFINED_SCORE_WEIGHTS["expectancy"]


def test_expectancy_saturates_and_clamps():
    at_sat  = _base(expectancy_pct=a._REFINED_EXPECTANCY_SATURATION)
    way_up  = _base(expectancy_pct=a._REFINED_EXPECTANCY_SATURATION * 10)
    assert a._composite_score(at_sat, 0) == a._composite_score(way_up, 0)
    # Negative expectancy contributes 0, never a negative score component.
    neg  = _base(expectancy_pct=-5.0)
    zero = _base(expectancy_pct=0.0)
    assert a._composite_score(neg, 0) == a._composite_score(zero, 0)


def test_missing_expectancy_key_is_safe():
    """_side_stats (side-gated candidates) doesn't carry expectancy_pct — the score
    must treat it as 0 rather than raising."""
    assert a._composite_score(_base(), 0) == a._composite_score(_base(expectancy_pct=0.0), 0)


def test_expectancy_pct_is_notional_based(monkeypatch):
    """Mean % return on notional per trade — independent of share count."""
    def _fills():
        # Long 10 @100 -> 101  => +$10 on $1000 notional = +1.0%
        # Long 10 @100 -> 99.5 => -$5  on $1000 notional = -0.5%   mean = +0.25%
        return [
            {"symbol": "AAA", "side": "BOT", "price": 100.0, "shares": 10,
             "time": "2026-08-13T14:00:00Z", "order_id": ""},
            {"symbol": "AAA", "side": "SLD", "price": 101.0, "shares": 10,
             "time": "2026-08-13T14:05:00Z", "order_id": ""},
            {"symbol": "AAA", "side": "BOT", "price": 100.0, "shares": 10,
             "time": "2026-08-13T15:00:00Z", "order_id": ""},
            {"symbol": "AAA", "side": "SLD", "price": 99.5, "shares": 10,
             "time": "2026-08-13T15:05:00Z", "order_id": ""},
        ]
    stats = a._compute_strategy_stats(days=3650, fills_fn=_fills, gate_as=None)
    assert stats, "expected at least one strategy bucket"
    st = next(iter(stats.values()))
    assert st["trades"] == 2
    assert abs(st["expectancy_pct"] - 0.25) < 1e-6


def test_tv_and_kairos_share_the_same_ranking_window():
    """Both books must rank over the SAME window or TV-vs-Kairos comparisons (and
    the crew's [TV]/[Kairos] tagging, which reads both leaderboards) aren't
    apples-to-apples. Kairos silently sat on 30 days after TV moved to 45."""
    import inspect
    tv  = inspect.signature(a._do_refresh_refined).parameters["days"].default
    kai = inspect.signature(a._do_refresh_kairos_refined).parameters["days"].default
    assert tv == kai == 45


def test_min_trade_floors_keep_their_deliberate_asymmetry():
    """The floors are intentionally NOT equal. When the takeable filter shrank the
    denominator, each book got relief proportional to how starved it was: TV was
    filling 5 of 20 slots (7->5), Kairos 9 of 20 (5->4). Pinned so a future change
    is a conscious decision rather than drift — but Kairos must never exceed TV."""
    assert a._REFINED_MIN_TRADES == 5
    assert a._KAIROS_REFINED_MIN_TRADES == 4
    assert a._KAIROS_REFINED_MIN_TRADES <= a._REFINED_MIN_TRADES
    # On-Deck (display-only) must stay BELOW its routing floor or the watchlist
    # empties out — at parity every qualifying name gets routed.
    assert a._REFINED_ONDECK_MIN_TRADES < a._REFINED_MIN_TRADES
    assert a._KAIROS_REFINED_ONDECK_MIN_TRADES < a._KAIROS_REFINED_MIN_TRADES


def test_expectancy_pct_independent_of_share_count(monkeypatch):
    """10x the shares at the same prices => same expectancy %, 10x the dollars."""
    def _mk(qty):
        return lambda: [
            {"symbol": "BBB", "side": "BOT", "price": 50.0, "shares": qty,
             "time": "2026-08-13T14:00:00Z", "order_id": ""},
            {"symbol": "BBB", "side": "SLD", "price": 50.5, "shares": qty,
             "time": "2026-08-13T14:05:00Z", "order_id": ""},
        ]
    small = next(iter(a._compute_strategy_stats(days=3650, fills_fn=_mk(10), gate_as=None).values()))
    big   = next(iter(a._compute_strategy_stats(days=3650, fills_fn=_mk(100), gate_as=None).values()))
    assert abs(small["expectancy_pct"] - big["expectancy_pct"]) < 1e-6   # size-independent
    assert big["total_pnl"] == small["total_pnl"] * 10                    # dollars are not
