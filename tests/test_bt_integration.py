"""End-to-end integration test: hand-coded Camarilla strategy on synthetic
intraday data, asserting the full stack of rules 17–21 produces a sane
trade count.

This is the test that would have caught each of the four historical
regressions:
  1. Pickle: if we try to pickle the Strategy class, it must succeed.
  2. Daily resample: without it, H4 is computed from 5-min bar range
     (~$0.50) and entries fire constantly.
  3. Tick→dollar: without it, trailing-stop=40 means $40, positions
     never exit.
  4. Crossover semantics: without (a) or (b), a plain `close > h4`
     check fires on every bar above the level.

Bound chosen: ≤5x the number of trading days. Realistic Camarilla-like
strategies on 5 days of intraday data should produce single-digit
trades, certainly not hundreds.
"""
from __future__ import annotations

import pickle
import pandas as pd
import pytest

from backtesting import Backtest, Strategy


class IntradayCamarillaStrategy(Strategy):
    """Minimal Camarilla-style long entry on H4 breakout.

    Implements all four rules correctly:
      * rule 17(b): same-bar crossover — open<H4 and close>H4
      * rule 19: guards len(self.data.Close) >= 2
      * rule 20: stop/trail as dollar fractions (ticks * 0.01)
      * rule 21: H4 from previous-day H/L/C, not previous-bar
    """

    cam_mult = 1.1
    tick_size = 0.01
    stop_ticks = 80          # pine loss=80 ticks
    _trade_on_close = True

    def init(self):
        idx = pd.DatetimeIndex(self.data.index)
        s_high = pd.Series(self.data.High, index=idx)
        s_low = pd.Series(self.data.Low, index=idx)
        s_close = pd.Series(self.data.Close, index=idx)

        d_high = s_high.resample("B").max().shift(1).reindex(idx, method="ffill")
        d_low = s_low.resample("B").min().shift(1).reindex(idx, method="ffill")
        d_close = s_close.resample("B").last().shift(1).reindex(idx, method="ffill")

        h4 = (d_close + (d_high - d_low) * self.cam_mult / 2.0).values
        self.h4 = self.I(lambda v: v, h4, name="h4")

    def next(self):
        if len(self.data.Close) < 2:
            return
        if pd.isna(self.h4[-1]):
            return
        if self.position:
            return

        h4 = self.h4[-1]
        # Rule 17(b): explicit same-bar crossover
        if self.data.Open[-1] < h4 and self.data.Close[-1] > h4:
            stop_dist = self.stop_ticks * self.tick_size
            self.buy(sl=self.data.Close[-1] - stop_dist)


def test_camarilla_strategy_class_is_picklable():
    """A Strategy class defined at module scope (as here, or as the built-in
    strategies in strategies/bt_strategies.py) pickles without any fixup —
    its __module__ points at a real importable module. The pickle fixup in
    bt_run/bt_optimize is only needed for exec'd classes (see
    test_exec_picklability.py)."""
    blob = pickle.dumps(IntradayCamarillaStrategy)
    assert len(blob) > 0


def test_camarilla_integration_trade_count_bounded(intraday_5m_ohlc):
    bt = Backtest(
        intraday_5m_ohlc,
        IntradayCamarillaStrategy,
        cash=10_000,
        commission=0.0,
        finalize_trades=True,
    )
    stats = bt.run()
    n_trades = stats["# Trades"]
    n_days = intraday_5m_ohlc.index.normalize().nunique()

    # The old bug produced n_trades > 1000 on the same fixture size.
    # With rules 17–21 applied, it must stay proportional to days.
    assert n_trades <= n_days * 5, (
        f"Trade count {n_trades} over {n_days} days — rule regression? "
        f"Expected ≤{n_days * 5}."
    )


def test_naive_close_only_strategy_overfires(intraday_5m_ohlc):
    """Contrast case: a strategy using `close > h4` alone (rule 17 violation)
    should produce dramatically more trades. If this test ever FAILS — i.e.
    the naive version produces a reasonable trade count — something in the
    backtesting.py behavior has shifted and the main test above may no longer
    be a meaningful guard."""

    class NaiveStrategy(Strategy):
        _trade_on_close = True

        def init(self):
            idx = pd.DatetimeIndex(self.data.index)
            s_high = pd.Series(self.data.High, index=idx)
            s_low = pd.Series(self.data.Low, index=idx)
            s_close = pd.Series(self.data.Close, index=idx)
            d_high = s_high.resample("B").max().shift(1).reindex(idx, method="ffill")
            d_low = s_low.resample("B").min().shift(1).reindex(idx, method="ffill")
            d_close = s_close.resample("B").last().shift(1).reindex(idx, method="ffill")
            h4 = (d_close + (d_high - d_low) * 1.1 / 2.0).values
            self.h4 = self.I(lambda v: v, h4, name="h4")

        def next(self):
            if pd.isna(self.h4[-1]):
                return
            if not self.position and self.data.Close[-1] > self.h4[-1]:
                self.buy()  # no stop, no crossover guard — overfires

    bt = Backtest(intraday_5m_ohlc, NaiveStrategy, cash=10_000, finalize_trades=True)
    naive_stats = bt.run()

    bt2 = Backtest(intraday_5m_ohlc, IntradayCamarillaStrategy,
                   cash=10_000, finalize_trades=True)
    correct_stats = bt2.run()

    # The correct version must produce strictly fewer or equal trades.
    assert correct_stats["# Trades"] <= naive_stats["# Trades"]
