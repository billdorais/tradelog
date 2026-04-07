"""
Camarilla H4/L4 Breakout-Failure strategy for backtesting.py
(https://github.com/kernc/backtesting.py)

Entry logic (daily bars):
  Short: today's high pierces H4 but today's close is back below H4 → failed breakout
  Long:  today's low pierces L4 but today's close is back above L4 → failed breakout

Exits: ATR-based trailing stop and hard stop locked at entry bar.
"""

import numpy as np
import pandas as pd
from backtesting import Strategy


def _atr(high, low, close, period):
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(
        high - low,
        np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)),
    )
    atr = pd.Series(tr).rolling(period, min_periods=1).mean().to_numpy()
    return atr


class CamarillaBreakoutFailure(Strategy):
    # ── Tuneable parameters ──────────────────────────────────────────────────
    atr_period     = 14
    trail_mult     = 1.5   # ATR × this = trail activation distance
    stop_mult      = 2.0   # ATR × this = hard stop distance

    def init(self):
        hi  = self.data.High
        lo  = self.data.Low
        cl  = self.data.Close

        # Previous-day levels
        prev_high  = np.roll(hi,  1); prev_high[0]  = np.nan
        prev_low   = np.roll(lo,  1); prev_low[0]   = np.nan
        prev_close = np.roll(cl,  1); prev_close[0] = np.nan

        self.h4 = self.I(
            lambda: prev_close + (prev_high - prev_low) * 1.1 / 2,
            name="H4", overlay=True, color="#ef5350",
        )
        self.l4 = self.I(
            lambda: prev_close - (prev_high - prev_low) * 1.1 / 2,
            name="L4", overlay=True, color="#26a65b",
        )
        self.atr_vals = self.I(
            lambda: _atr(hi, lo, cl, self.atr_period),
            name="ATR", overlay=False,
        )

    def next(self):
        price = self.data.Close[-1]
        high  = self.data.High[-1]
        low   = self.data.Low[-1]
        h4    = self.h4[-1]
        l4    = self.l4[-1]
        atr   = self.atr_vals[-1]

        if np.isnan(h4) or np.isnan(l4) or np.isnan(atr) or atr == 0:
            return

        trail = atr * self.trail_mult
        stop  = atr * self.stop_mult

        # Failed breakout above H4 → Short
        if high > h4 and price < h4:
            if self.position.is_long:
                self.position.close()
            if not self.position.is_short:
                self.sell(sl=price + stop, tp=price - trail * 2)

        # Failed breakout below L4 → Long
        elif low < l4 and price > l4:
            if self.position.is_short:
                self.position.close()
            if not self.position.is_long:
                self.buy(sl=price - stop, tp=price + trail * 2)
