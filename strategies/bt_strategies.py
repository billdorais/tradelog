"""
Built-in backtesting.py strategies for the Backtest page.
All calculations use only numpy + pandas (no TA-Lib).
"""

import numpy as np
import pandas as pd
from backtesting import Strategy


# ── Shared helpers ────────────────────────────────────────────────────────────

def _sma(series, period):
    return pd.Series(series).rolling(period, min_periods=1).mean().to_numpy()

def _ema(series, period):
    return pd.Series(series).ewm(span=period, adjust=False).mean().to_numpy()

def _atr(high, low, close, period):
    pc = np.roll(close, 1); pc[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - pc), np.abs(low - pc)))
    return pd.Series(tr).rolling(period, min_periods=1).mean().to_numpy()

def _rsi(close, period):
    delta = pd.Series(close).diff()
    gain  = delta.where(delta > 0, 0.0).rolling(period, min_periods=1).mean()
    loss  = (-delta.where(delta < 0, 0.0)).rolling(period, min_periods=1).mean()
    rs    = gain / loss.replace(0, 1e-10)
    return (100 - 100 / (1 + rs)).to_numpy()

def _bbands(close, period, std_dev):
    s    = pd.Series(close)
    mid  = s.rolling(period, min_periods=1).mean()
    std  = s.rolling(period, min_periods=1).std(ddof=0).fillna(0)
    return (mid - std * std_dev).to_numpy(), mid.to_numpy(), (mid + std * std_dev).to_numpy()

def _macd(close, fast, slow, signal):
    ema_fast   = pd.Series(_ema(close, fast))
    ema_slow   = pd.Series(_ema(close, slow))
    macd_line  = (ema_fast - ema_slow).to_numpy()
    signal_line = _ema(macd_line, signal)
    return macd_line, signal_line


# ── Built-in strategies ───────────────────────────────────────────────────────

class CamarillaBreakoutFailure(Strategy):
    """H4/L4 failed-breakout mean-reversion strategy."""
    atr_period = 14
    trail_mult = 1.5
    stop_mult  = 2.0

    def init(self):
        hi, lo, cl = self.data.High, self.data.Low, self.data.Close
        ph = np.roll(hi, 1); ph[0] = np.nan
        pl = np.roll(lo, 1); pl[0] = np.nan
        pc = np.roll(cl, 1); pc[0] = np.nan
        self.h4      = self.I(lambda: pc + (ph - pl) * 1.1 / 2, name="H4", overlay=True,  color="#ef5350")
        self.l4      = self.I(lambda: pc - (ph - pl) * 1.1 / 2, name="L4", overlay=True,  color="#26a65b")
        self.atr_val = self.I(lambda: _atr(hi, lo, cl, self.atr_period), name="ATR", overlay=False)

    def next(self):
        p, h, l = self.data.Close[-1], self.data.High[-1], self.data.Low[-1]
        h4, l4, atr = self.h4[-1], self.l4[-1], self.atr_val[-1]
        if any(np.isnan(x) for x in [h4, l4, atr]) or atr == 0:
            return
        stop, trail = atr * self.stop_mult, atr * self.trail_mult
        if h > h4 and p < h4 and not self.position.is_short:
            self.position.close()
            self.sell(sl=p + stop, tp=p - trail * 2)
        elif l < l4 and p > l4 and not self.position.is_long:
            self.position.close()
            self.buy(sl=p - stop, tp=p + trail * 2)


class SmaCross(Strategy):
    """Golden/death cross using two simple moving averages."""
    fast_period = 50
    slow_period = 200

    def init(self):
        cl = self.data.Close
        self.fast = self.I(lambda: _sma(cl, self.fast_period), name=f"SMA{self.fast_period}", overlay=True, color="#4a90d9")
        self.slow = self.I(lambda: _sma(cl, self.slow_period), name=f"SMA{self.slow_period}", overlay=True, color="#f5a623")

    def next(self):
        if np.isnan(self.fast[-1]) or np.isnan(self.slow[-1]):
            return
        if self.fast[-2] < self.slow[-2] and self.fast[-1] > self.slow[-1]:
            self.position.close(); self.buy()
        elif self.fast[-2] > self.slow[-2] and self.fast[-1] < self.slow[-1]:
            self.position.close(); self.sell()


class EmaCross(Strategy):
    """EMA crossover — faster signal than SMA cross."""
    fast_period = 20
    slow_period = 50

    def init(self):
        cl = self.data.Close
        self.fast = self.I(lambda: _ema(cl, self.fast_period), name=f"EMA{self.fast_period}", overlay=True, color="#4a90d9")
        self.slow = self.I(lambda: _ema(cl, self.slow_period), name=f"EMA{self.slow_period}", overlay=True, color="#f5a623")

    def next(self):
        if np.isnan(self.fast[-1]) or np.isnan(self.slow[-1]):
            return
        if self.fast[-2] < self.slow[-2] and self.fast[-1] > self.slow[-1]:
            self.position.close(); self.buy()
        elif self.fast[-2] > self.slow[-2] and self.fast[-1] < self.slow[-1]:
            self.position.close(); self.sell()


class RsiMeanReversion(Strategy):
    """Buy oversold, sell overbought with RSI."""
    rsi_period  = 14
    oversold    = 30
    overbought  = 70
    atr_period  = 14
    stop_mult   = 2.0

    def init(self):
        cl = self.data.Close
        self.rsi     = self.I(lambda: _rsi(cl, self.rsi_period), name="RSI", overlay=False)
        self.atr_val = self.I(lambda: _atr(self.data.High, self.data.Low, cl, self.atr_period), name="ATR", overlay=False)

    def next(self):
        rsi, atr = self.rsi[-1], self.atr_val[-1]
        if np.isnan(rsi) or np.isnan(atr) or atr == 0:
            return
        p    = self.data.Close[-1]
        stop = atr * self.stop_mult
        if rsi < self.oversold and not self.position.is_long:
            self.position.close(); self.buy(sl=p - stop)
        elif rsi > self.overbought and not self.position.is_short:
            self.position.close(); self.sell(sl=p + stop)


class BollingerBandBreakout(Strategy):
    """Enter on BB squeeze breakout, exit at opposite band."""
    bb_period  = 20
    bb_std     = 2.0
    atr_period = 14
    stop_mult  = 1.5

    def init(self):
        cl = self.data.Close
        lo, mid, hi = _bbands(cl, self.bb_period, self.bb_std)
        self.bb_lo   = self.I(lambda: lo,  name="BB Lower", overlay=True, color="#26a65b")
        self.bb_mid  = self.I(lambda: mid, name="BB Mid",   overlay=True, color="#888")
        self.bb_hi   = self.I(lambda: hi,  name="BB Upper", overlay=True, color="#ef5350")
        self.atr_val = self.I(lambda: _atr(self.data.High, self.data.Low, cl, self.atr_period), name="ATR", overlay=False)

    def next(self):
        p    = self.data.Close[-1]
        prev = self.data.Close[-2]
        atr  = self.atr_val[-1]
        if np.isnan(self.bb_hi[-1]) or atr == 0:
            return
        stop = atr * self.stop_mult
        if prev < self.bb_hi[-2] and p > self.bb_hi[-1] and not self.position.is_long:
            self.position.close(); self.buy(sl=p - stop, tp=p + (self.bb_hi[-1] - self.bb_lo[-1]))
        elif prev > self.bb_lo[-2] and p < self.bb_lo[-1] and not self.position.is_short:
            self.position.close(); self.sell(sl=p + stop, tp=p - (self.bb_hi[-1] - self.bb_lo[-1]))


class MacdStrategy(Strategy):
    """MACD line crosses signal line."""
    fast_period   = 12
    slow_period   = 26
    signal_period = 9
    atr_period    = 14
    stop_mult     = 2.0

    def init(self):
        cl = self.data.Close
        macd, sig = _macd(cl, self.fast_period, self.slow_period, self.signal_period)
        self.macd    = self.I(lambda: macd, name="MACD",   overlay=False, color="#4a90d9")
        self.signal  = self.I(lambda: sig,  name="Signal", overlay=False, color="#f5a623")
        self.atr_val = self.I(lambda: _atr(self.data.High, self.data.Low, cl, self.atr_period), name="ATR", overlay=False)

    def next(self):
        if np.isnan(self.macd[-1]) or np.isnan(self.signal[-1]):
            return
        atr  = self.atr_val[-1]
        p    = self.data.Close[-1]
        stop = atr * self.stop_mult if atr > 0 else p * 0.02
        if self.macd[-2] < self.signal[-2] and self.macd[-1] > self.signal[-1]:
            self.position.close(); self.buy(sl=p - stop)
        elif self.macd[-2] > self.signal[-2] and self.macd[-1] < self.signal[-1]:
            self.position.close(); self.sell(sl=p + stop)


class CamarillaEMA8(Strategy):
    """
    Exact Python translation of cam_v6 Pine Script.

    Entry: bar closes across H4/L4 (open on wrong side, close on right side)
           confirmed by 8-period EMA direction filter.
    Exit:  trail activates after `trail_activation` profit, trails at
           `trail_offset` distance.  Hard stop at `stop_loss`.
    All exit values are in price units (dollars for US stocks).

    NOTE: H4/L4 uses the previous bar's H/L/C.  On daily data this equals
    the previous trading day — matching the Pine Script's RTH logic.
    On intraday data feed the bars in 1D resolution or resample first.
    """
    stop_loss        = 0.50   # hard stop in price units  (Pine: loss=50 pts)
    trail_activation = 0.40   # profit to activate trail  (Pine: trail_points=40)
    trail_offset     = 0.20   # trail distance once active (Pine: trail_offset=20)
    ema_period       = 8

    # Tell bt_run / bt_optimize to use trade_on_close=True (matches Pine Script)
    _trade_on_close  = True

    def init(self):
        cl, hi, lo = self.data.Close, self.data.High, self.data.Low
        self.ema8 = self.I(lambda: _ema(cl, self.ema_period), name="EMA8", overlay=True, color="#ffffff")

        # Detect intraday: if index has time component, resample to daily first
        # so H4/L4 use the previous CALENDAR DAY's H/L/C — matching TradingView's
        # prevRTHHigh/prevRTHLow/prevRTHClose logic.
        idx = self.data.index
        is_intraday = hasattr(idx[0], 'hour') and len(set(t.date() for t in idx)) < len(idx)

        if is_intraday:
            daily = pd.DataFrame({'h': hi, 'l': lo, 'c': cl}, index=idx)
            daily = daily.resample('1D').agg({'h': 'max', 'l': 'min', 'c': 'last'}).dropna()
            # Shift by 1 day to get previous day's values
            prev = daily.shift(1)
            # Reindex back to intraday frequency (forward-fill within each day)
            ph_s = prev['h'].reindex(idx, method='ffill').to_numpy()
            pl_s = prev['l'].reindex(idx, method='ffill').to_numpy()
            pc_s = prev['c'].reindex(idx, method='ffill').to_numpy()
        else:
            ph_s = np.roll(hi, 1); ph_s[0] = np.nan
            pl_s = np.roll(lo, 1); pl_s[0] = np.nan
            pc_s = np.roll(cl, 1); pc_s[0] = np.nan

        self.h4 = self.I(lambda: pc_s + (ph_s - pl_s) * 1.1 / 2.0, name="H4", overlay=True, color="#ef5350")
        self.l4 = self.I(lambda: pc_s - (ph_s - pl_s) * 1.1 / 2.0, name="L4", overlay=True, color="#26a65b")

        self._entry    = np.nan
        self._trail_sl = np.nan

    def next(self):
        c    = self.data.Close[-1]
        o    = self.data.Open[-1]
        h4   = self.h4[-1]
        l4   = self.l4[-1]
        ema8 = self.ema8[-1]

        if np.isnan(h4) or np.isnan(l4) or np.isnan(ema8):
            return

        # ── Manage open long ─────────────────────────────────────
        if self.position.is_long:
            profit = c - self._entry
            if profit <= -self.stop_loss:
                self.position.close()
                self._entry = self._trail_sl = np.nan
                return
            if profit >= self.trail_activation:
                new_sl = c - self.trail_offset
                if np.isnan(self._trail_sl) or new_sl > self._trail_sl:
                    self._trail_sl = new_sl
            if not np.isnan(self._trail_sl) and c <= self._trail_sl:
                self.position.close()
                self._entry = self._trail_sl = np.nan
            return

        # ── Manage open short ────────────────────────────────────
        if self.position.is_short:
            profit = self._entry - c
            if profit <= -self.stop_loss:
                self.position.close()
                self._entry = self._trail_sl = np.nan
                return
            if profit >= self.trail_activation:
                new_sl = c + self.trail_offset
                if np.isnan(self._trail_sl) or new_sl < self._trail_sl:
                    self._trail_sl = new_sl
            if not np.isnan(self._trail_sl) and c >= self._trail_sl:
                self.position.close()
                self._entry = self._trail_sl = np.nan
            return

        # ── No position — check entries ──────────────────────────
        self._entry = self._trail_sl = np.nan
        if c > h4 and o < h4 and ema8 < c:
            self.buy()
            self._entry = c
        elif c < l4 and o > l4 and ema8 > c:
            self.sell()
            self._entry = c


class CamarillaBreakout(Strategy):
    """H4/L4 breakout with EMA-direction filter. Matches CAM_*_v6 Pine
    bar-for-bar (same entry/exit logic as CamarillaEMA8, with cam_mult
    exposed so the level multiplier can be tuned).
    """
    cam_mult         = 1.1
    stop_loss        = 0.50
    trail_activation = 0.40
    trail_offset     = 0.20
    ema_period       = 8
    _trade_on_close  = True

    def init(self):
        cl, hi, lo = self.data.Close, self.data.High, self.data.Low
        self.ema = self.I(lambda: _ema(cl, self.ema_period), name=f"EMA{self.ema_period}", overlay=True, color="#ffffff")

        idx = self.data.index
        is_intraday = hasattr(idx[0], 'hour') and len(set(t.date() for t in idx)) < len(idx)
        if is_intraday:
            daily = pd.DataFrame({'h': hi, 'l': lo, 'c': cl}, index=idx)
            daily = daily.resample('1D').agg({'h': 'max', 'l': 'min', 'c': 'last'}).dropna()
            prev = daily.shift(1)
            ph = prev['h'].reindex(idx, method='ffill').to_numpy()
            pl = prev['l'].reindex(idx, method='ffill').to_numpy()
            pc = prev['c'].reindex(idx, method='ffill').to_numpy()
        else:
            ph = np.roll(hi, 1); ph[0] = np.nan
            pl = np.roll(lo, 1); pl[0] = np.nan
            pc = np.roll(cl, 1); pc[0] = np.nan

        self.h4 = self.I(lambda: pc + (ph - pl) * self.cam_mult / 2.0, name="H4", overlay=True, color="#ef5350")
        self.l4 = self.I(lambda: pc - (ph - pl) * self.cam_mult / 2.0, name="L4", overlay=True, color="#26a65b")

        self._entry    = np.nan
        self._trail_sl = np.nan

    def next(self):
        c, o = self.data.Close[-1], self.data.Open[-1]
        h4, l4, ema = self.h4[-1], self.l4[-1], self.ema[-1]
        if any(np.isnan(x) for x in (h4, l4, ema)):
            return

        if self.position.is_long:
            profit = c - self._entry
            if profit <= -self.stop_loss:
                self.position.close(); self._entry = self._trail_sl = np.nan; return
            if profit >= self.trail_activation:
                new_sl = c - self.trail_offset
                if np.isnan(self._trail_sl) or new_sl > self._trail_sl:
                    self._trail_sl = new_sl
            if not np.isnan(self._trail_sl) and c <= self._trail_sl:
                self.position.close(); self._entry = self._trail_sl = np.nan
            return

        if self.position.is_short:
            profit = self._entry - c
            if profit <= -self.stop_loss:
                self.position.close(); self._entry = self._trail_sl = np.nan; return
            if profit >= self.trail_activation:
                new_sl = c + self.trail_offset
                if np.isnan(self._trail_sl) or new_sl < self._trail_sl:
                    self._trail_sl = new_sl
            if not np.isnan(self._trail_sl) and c >= self._trail_sl:
                self.position.close(); self._entry = self._trail_sl = np.nan
            return

        self._entry = self._trail_sl = np.nan
        if c > h4 and o < h4 and ema < c:
            self.buy();  self._entry = c
        elif c < l4 and o > l4 and ema > c:
            self.sell(); self._entry = c


class CamarillaH3L3Reversal(Strategy):
    """H3/L3 failed-breakout reversal, matching CAM_H3L3_REV_V01 Pine.

    Entry (fade the level):
      Long:  low <= l3 and close > l3 and ema8 < close  (wick pierced L3, closed back above)
      Short: high >= h3 and close < h3 and ema8 > close (wick pierced H3, closed back below)

    Differences from CamarillaBreakout:
      * H3/L3 levels use divisor 4.0 (not 2.0 for H4/L4)
      * Asymmetric long/short exit params — the Pine uses tighter stops on shorts
      * RTH session filter (09:30–16:00) — no entries outside RTH
      * EOD force-close at `eod_close_hhmm` (default 1555, matching the Pine)
    """
    cam_mult               = 1.1
    ema_period             = 8
    # Long exits (Pine: loss=70, trail_points=40, trail_offset=1 → dollars)
    stop_loss_long         = 0.70
    trail_activation_long  = 0.40
    trail_offset_long      = 0.01
    # Short exits (Pine: loss=20, trail_points=10, trail_offset=1 → dollars)
    stop_loss_short        = 0.20
    trail_activation_short = 0.10
    trail_offset_short     = 0.01
    # Session / EOD
    rth_start_hhmm         = 930   # inclusive
    rth_end_hhmm           = 1600  # exclusive (matches Pine's 0930-1600 session)
    eod_close_hhmm         = 1555  # last completed 5-min bar before close
    _trade_on_close        = True

    def init(self):
        cl, hi, lo = self.data.Close, self.data.High, self.data.Low
        self.ema = self.I(lambda: _ema(cl, self.ema_period), name=f"EMA{self.ema_period}", overlay=True, color="#ffffff")

        idx = self.data.index
        is_intraday = hasattr(idx[0], 'hour') and len(set(t.date() for t in idx)) < len(idx)
        if is_intraday:
            # Pine tracks RTH session OHLC explicitly. On data that's already
            # RTH-filtered, a daily resample is equivalent; otherwise restrict
            # the daily aggregate to RTH bars first.
            ts = pd.Series(1, index=idx)
            rth_mask = [(self._hhmm(t) >= self.rth_start_hhmm and self._hhmm(t) < self.rth_end_hhmm) for t in idx]
            rth_mask = pd.Series(rth_mask, index=idx)
            daily = pd.DataFrame({'h': hi, 'l': lo, 'c': cl}, index=idx)[rth_mask]
            daily = daily.resample('1D').agg({'h': 'max', 'l': 'min', 'c': 'last'}).dropna()
            prev = daily.shift(1)
            ph = prev['h'].reindex(idx, method='ffill').to_numpy()
            pl = prev['l'].reindex(idx, method='ffill').to_numpy()
            pc = prev['c'].reindex(idx, method='ffill').to_numpy()
        else:
            ph = np.roll(hi, 1); ph[0] = np.nan
            pl = np.roll(lo, 1); pl[0] = np.nan
            pc = np.roll(cl, 1); pc[0] = np.nan

        # H3/L3 use divisor 4.0 — Pine: prevRTHClose ± (H - L) * 1.1 / 4.0
        self.h3 = self.I(lambda: pc + (ph - pl) * self.cam_mult / 4.0, name="H3", overlay=True, color="#ef5350")
        self.l3 = self.I(lambda: pc - (ph - pl) * self.cam_mult / 4.0, name="L3", overlay=True, color="#26a65b")

        self._entry    = np.nan
        self._trail_sl = np.nan

    @staticmethod
    def _hhmm(ts):
        return ts.hour * 100 + ts.minute

    def next(self):
        c, o, h, l = self.data.Close[-1], self.data.Open[-1], self.data.High[-1], self.data.Low[-1]
        h3, l3, ema = self.h3[-1], self.l3[-1], self.ema[-1]
        idx = self.data.index[-1]
        hhmm = self._hhmm(idx) if hasattr(idx, 'hour') else 0
        in_rth = self.rth_start_hhmm <= hhmm < self.rth_end_hhmm
        is_eod_bar = hhmm == self.eod_close_hhmm

        # EOD flatten (priority over entries, matches Pine ordering)
        if is_eod_bar and self.position:
            self.position.close(); self._entry = self._trail_sl = np.nan
            return

        if np.isnan(h3) or np.isnan(l3) or np.isnan(ema):
            return

        if self.position.is_long:
            profit = c - self._entry
            if profit <= -self.stop_loss_long:
                self.position.close(); self._entry = self._trail_sl = np.nan; return
            if profit >= self.trail_activation_long:
                new_sl = c - self.trail_offset_long
                if np.isnan(self._trail_sl) or new_sl > self._trail_sl:
                    self._trail_sl = new_sl
            if not np.isnan(self._trail_sl) and c <= self._trail_sl:
                self.position.close(); self._entry = self._trail_sl = np.nan
            return

        if self.position.is_short:
            profit = self._entry - c
            if profit <= -self.stop_loss_short:
                self.position.close(); self._entry = self._trail_sl = np.nan; return
            if profit >= self.trail_activation_short:
                new_sl = c + self.trail_offset_short
                if np.isnan(self._trail_sl) or new_sl < self._trail_sl:
                    self._trail_sl = new_sl
            if not np.isnan(self._trail_sl) and c >= self._trail_sl:
                self.position.close(); self._entry = self._trail_sl = np.nan
            return

        if not in_rth or is_eod_bar:
            return

        self._entry = self._trail_sl = np.nan
        if l <= l3 and c > l3 and ema < c:
            self.buy();  self._entry = c
        elif h >= h3 and c < h3 and ema > c:
            self.sell(); self._entry = c


# ── Registry ─────────────────────────────────────────────────────────────────
# Maps strategy_name → (class, [{id, label, type, default, min, max, step}])

STRATEGIES = {
    "cam_breakout": (
        CamarillaBreakout,
        [
            {"id": "cam_mult",         "label": "Cam Multiplier",       "type": "float", "default": 1.1,  "min": 0.5,  "max": 2.5,  "step": 0.05},
            {"id": "stop_loss",        "label": "Stop Loss ($)",        "type": "float", "default": 0.50, "min": 0.20, "max": 1.50, "step": 0.10},
            {"id": "trail_activation", "label": "Trail Activation ($)", "type": "float", "default": 0.40, "min": 0.20, "max": 1.50, "step": 0.10},
            {"id": "trail_offset",     "label": "Trail Offset ($)",     "type": "float", "default": 0.20, "min": 0.10, "max": 0.80, "step": 0.10},
            {"id": "ema_period",       "label": "EMA Period",           "type": "int",   "default": 8,    "min": 5,    "max": 21,   "step": 1},
        ],
    ),
    "cam_h3l3_reversal": (
        CamarillaH3L3Reversal,
        [
            {"id": "cam_mult",               "label": "Cam Multiplier",            "type": "float", "default": 1.1,  "min": 0.5,  "max": 2.5,  "step": 0.05},
            {"id": "ema_period",             "label": "EMA Period",                "type": "int",   "default": 8,    "min": 5,    "max": 21,   "step": 1},
            {"id": "stop_loss_long",         "label": "Long Stop Loss ($)",        "type": "float", "default": 0.70, "min": 0.10, "max": 2.00, "step": 0.10},
            {"id": "trail_activation_long",  "label": "Long Trail Activation ($)", "type": "float", "default": 0.40, "min": 0.05, "max": 2.00, "step": 0.05},
            {"id": "trail_offset_long",      "label": "Long Trail Offset ($)",     "type": "float", "default": 0.01, "min": 0.01, "max": 0.50, "step": 0.01},
            {"id": "stop_loss_short",        "label": "Short Stop Loss ($)",       "type": "float", "default": 0.20, "min": 0.05, "max": 2.00, "step": 0.05},
            {"id": "trail_activation_short", "label": "Short Trail Activation ($)","type": "float", "default": 0.10, "min": 0.05, "max": 2.00, "step": 0.05},
            {"id": "trail_offset_short",     "label": "Short Trail Offset ($)",    "type": "float", "default": 0.01, "min": 0.01, "max": 0.50, "step": 0.01},
        ],
    ),
    "cam_ema8": (
        CamarillaEMA8,
        [
            {"id": "stop_loss",        "label": "Stop Loss ($)",       "type": "float", "default": 0.50, "min": 0.20, "max": 1.50, "step": 0.10},
            {"id": "trail_activation", "label": "Trail Activation ($)", "type": "float", "default": 0.40, "min": 0.20, "max": 1.50, "step": 0.10},
            {"id": "trail_offset",     "label": "Trail Offset ($)",     "type": "float", "default": 0.20, "min": 0.10, "max": 0.80, "step": 0.10},
            {"id": "ema_period",       "label": "EMA Period",           "type": "int",   "default": 8,    "min": 5,    "max": 21,   "step": 1},
        ],
    ),
    "camarilla": (
        CamarillaBreakoutFailure,
        [
            {"id": "atr_period", "label": "ATR Period",       "type": "int",   "default": 14,  "min": 5,   "max": 50,  "step": 1},
            {"id": "trail_mult", "label": "Trail Multiplier", "type": "float", "default": 1.5, "min": 0.5, "max": 5.0, "step": 0.1},
            {"id": "stop_mult",  "label": "Stop Multiplier",  "type": "float", "default": 2.0, "min": 0.5, "max": 10,  "step": 0.1},
        ],
    ),
    "sma_cross": (
        SmaCross,
        [
            {"id": "fast_period", "label": "Fast SMA", "type": "int", "default": 50,  "min": 5,  "max": 200, "step": 5},
            {"id": "slow_period", "label": "Slow SMA", "type": "int", "default": 200, "min": 10, "max": 500, "step": 10},
        ],
    ),
    "ema_cross": (
        EmaCross,
        [
            {"id": "fast_period", "label": "Fast EMA", "type": "int", "default": 20, "min": 5,  "max": 100, "step": 5},
            {"id": "slow_period", "label": "Slow EMA", "type": "int", "default": 50, "min": 10, "max": 200, "step": 5},
        ],
    ),
    "rsi": (
        RsiMeanReversion,
        [
            {"id": "rsi_period",  "label": "RSI Period",  "type": "int",   "default": 14, "min": 5,  "max": 50,  "step": 1},
            {"id": "oversold",    "label": "Oversold",    "type": "int",   "default": 30, "min": 10, "max": 45,  "step": 1},
            {"id": "overbought",  "label": "Overbought",  "type": "int",   "default": 70, "min": 55, "max": 90,  "step": 1},
            {"id": "atr_period",  "label": "ATR Period",  "type": "int",   "default": 14, "min": 5,  "max": 50,  "step": 1},
            {"id": "stop_mult",   "label": "Stop Mult",   "type": "float", "default": 2.0,"min": 0.5,"max": 10,  "step": 0.1},
        ],
    ),
    "bollinger": (
        BollingerBandBreakout,
        [
            {"id": "bb_period",  "label": "BB Period",  "type": "int",   "default": 20,  "min": 5,  "max": 100, "step": 1},
            {"id": "bb_std",     "label": "BB Std Dev", "type": "float", "default": 2.0, "min": 1.0,"max": 4.0, "step": 0.1},
            {"id": "atr_period", "label": "ATR Period", "type": "int",   "default": 14,  "min": 5,  "max": 50,  "step": 1},
            {"id": "stop_mult",  "label": "Stop Mult",  "type": "float", "default": 1.5, "min": 0.5,"max": 10,  "step": 0.1},
        ],
    ),
    "macd": (
        MacdStrategy,
        [
            {"id": "fast_period",   "label": "Fast EMA",   "type": "int",   "default": 12,  "min": 3,  "max": 50,  "step": 1},
            {"id": "slow_period",   "label": "Slow EMA",   "type": "int",   "default": 26,  "min": 5,  "max": 100, "step": 1},
            {"id": "signal_period", "label": "Signal EMA", "type": "int",   "default": 9,   "min": 3,  "max": 30,  "step": 1},
            {"id": "atr_period",    "label": "ATR Period", "type": "int",   "default": 14,  "min": 5,  "max": 50,  "step": 1},
            {"id": "stop_mult",     "label": "Stop Mult",  "type": "float", "default": 2.0, "min": 0.5,"max": 10,  "step": 0.1},
        ],
    ),
}
