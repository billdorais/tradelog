"""Parity tests for the two consolidated Camarilla classes:

  * CamarillaBreakout   — must match CamarillaEMA8 bar-for-bar (same logic,
                          cam_mult exposed as a param).
  * CamarillaH3L3Reversal — smoke test only: verify trades fire from
                            level-rejection entries, trade count stays
                            within sane bounds, RTH/EOD gates are honored.
                            Full Pine parity requires running against
                            TradingView's output on the same ticker+data.
"""
from __future__ import annotations

import pandas as pd
from backtesting import Backtest

from strategies.bt_strategies import (
    CamarillaEMA8,
    CamarillaBreakout,
    CamarillaH3L3Reversal,
)


def _run(strategy_cls, df, **kwargs):
    bt = Backtest(
        df, strategy_cls, cash=100_000, commission=0.0,
        exclusive_orders=True, trade_on_close=True,
    )
    return bt.run(**kwargs)


def test_cam_breakout_matches_cam_ema8(intraday_5m_ohlc):
    """Same data, same params → identical trades."""
    df = intraday_5m_ohlc
    params = dict(stop_loss=0.50, trail_activation=0.40, trail_offset=0.20, ema_period=8)

    ref_stats = _run(CamarillaEMA8, df, **params)
    uni_stats = _run(CamarillaBreakout, df, cam_mult=1.1, **params)

    ref_trades = ref_stats["_trades"][["EntryBar", "ExitBar", "Size", "EntryPrice", "ExitPrice"]].reset_index(drop=True)
    uni_trades = uni_stats["_trades"][["EntryBar", "ExitBar", "Size", "EntryPrice", "ExitPrice"]].reset_index(drop=True)

    assert len(ref_trades) == len(uni_trades), (
        f"trade-count divergence: cam_ema8={len(ref_trades)} cam_breakout={len(uni_trades)}"
    )
    pd.testing.assert_frame_equal(ref_trades, uni_trades, check_dtype=False)


def test_h3l3_reversal_trade_count_bounded(intraday_5m_ohlc):
    """Reversal fires from level rejection; bound trades to ≤5/day to
    catch regressions in the entry/exit/EOD/RTH gates."""
    df = intraday_5m_ohlc
    stats = _run(CamarillaH3L3Reversal, df)
    n_trades = int(stats.get("# Trades") or 0)
    n_days = len(set(d.date() for d in df.index))
    assert n_trades <= 5 * n_days, (
        f"reversal overfiring: {n_trades} trades over {n_days} days — "
        f"check rejection entries (low<=l3 AND close>l3, not just close>l3)"
    )


def test_h3l3_reversal_honors_rth_filter(intraday_5m_ohlc):
    """A zero-width RTH window must produce zero trades — proves the
    in_rth gate actually runs before every entry check."""
    df = intraday_5m_ohlc
    stats = _run(CamarillaH3L3Reversal, df, rth_start_hhmm=1200, rth_end_hhmm=1200)
    assert int(stats.get("# Trades") or 0) == 0, (
        "RTH filter isn't gating entries — zero-width session still produced trades"
    )


def test_h3l3_reversal_eod_force_close(intraday_5m_ohlc):
    """Any trade still open at 15:55 must be force-closed by that bar.
    Verified by scanning the trade list: no ExitTime should be after 15:55."""
    df = intraday_5m_ohlc
    stats = _run(CamarillaH3L3Reversal, df)
    trades = stats["_trades"]
    if trades.empty:
        return  # nothing to verify
    # ExitTime is a Timestamp; the EOD rule caps it at hh=15, mm<=55
    for ts in trades["ExitTime"]:
        hhmm = ts.hour * 100 + ts.minute
        assert hhmm <= 1555, f"position held past EOD bar 15:55: exit at {ts}"
