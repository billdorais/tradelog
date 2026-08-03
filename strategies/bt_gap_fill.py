"""
Gap-fill → prior-day-VWAP reversion strategy for backtesting.py.

The setup (from the video transcript): a stock gaps up, fades into the morning
chop, then reverts UP to the prior-day VWAP. You buy the morning wash-out with a
stop under the early-session lows and target the prior-day VWAP as the magnet.

Mechanical rules (first pass — deterministic version of Kenny's discretion):
  - Qualify the day: opening gap up >= gap_min_pct (proxy for "earnings gap" —
    we have no earnings calendar, so any gap of that size qualifies).
  - Skip the opening chop: do nothing for the first `warmup_min` minutes, but
    track the session low (the "early lows" = the stop reference).
  - Entry (long, one per day): after warmup, if price is still BELOW the prior-day
    VWAP (so there's room to the target) and the reward:risk to that target clears
    `min_rr`, buy. Stop = the warmup low − stop_buf_pct; target = prior-day VWAP.
  - Force-flat by `eod_close_min` (no overnight hold).

Long-only for now (matches the gap-up example); a gap-down/short twin is a later
add. Run per-ticker via backtest_gap_fill(df, ...); the df must be RTH-only with an
ET-local DatetimeIndex (app._filter_rth output).
"""

import numpy as np
from backtesting import Strategy, Backtest

from .vwap import prior_day_vwap, gap_pct


class GapFillVWAP(Strategy):
    # ── Tuneable parameters ──────────────────────────────────────────────────
    gap_min_pct   = 1.0     # minimum opening gap up (%) to qualify the day
    warmup_min    = 15      # minutes after 09:30 to let the chop print lows
    stop_buf_pct  = 0.05    # stop sits this % below the warmup low
    min_rr        = 2.0     # only enter if (target−entry)/(entry−stop) >= this
    eod_close_min = 385     # minutes after 09:30 to force-flat (385 = 15:55 ET)

    def init(self):
        o = self.data.Open; h = self.data.High
        l = self.data.Low;  c = self.data.Close; v = self.data.Volume
        idx = self.data.index
        self.pdv = self.I(lambda: prior_day_vwap(h, l, c, v, idx),
                          name="PrevDayVWAP", overlay=True, color="#F2A03D")
        self.gap = self.I(lambda: gap_pct(o, c, idx),
                          name="GapPct", overlay=False)
        self._cur_day       = None
        self._session_low   = np.inf
        self._entered_today = False

    def _minutes_since_open(self, ts):
        return (ts.hour - 9) * 60 + ts.minute - 30

    def next(self):
        ts    = self.data.index[-1]
        d     = ts.date()
        mins  = self._minutes_since_open(ts)
        price = self.data.Close[-1]
        low   = self.data.Low[-1]
        pdv   = self.pdv[-1]
        gap   = self.gap[-1]

        # New session — reset per-day state.
        if d != self._cur_day:
            self._cur_day       = d
            self._session_low   = low
            self._entered_today = False
        else:
            self._session_low = min(self._session_low, low)

        # Force-flat near the close; never hold overnight.
        if self.position and mins >= self.eod_close_min:
            self.position.close()
            return

        # Opening chop window: only track the low, don't trade.
        if mins < self.warmup_min:
            return
        # One entry per day, and don't stack.
        if self._entered_today or self.position:
            return

        # Qualify: gapped up enough AND price is below the prior-day VWAP (room up).
        if gap != gap or gap < self.gap_min_pct:      # NaN or too small
            return
        if pdv != pdv or pdv <= price:                # no prior VWAP, or no room
            return

        stop   = self._session_low * (1 - self.stop_buf_pct / 100.0)
        target = pdv
        if not (stop < price < target):
            return
        rr = (target - price) / (price - stop)
        if rr < self.min_rr:
            return

        self.buy(sl=stop, tp=target)
        self._entered_today = True


def _num(v, default=0.0):
    try:
        f = float(v)
        return f if f == f else default          # NaN → default
    except (TypeError, ValueError):
        return default


def backtest_gap_fill(df, cash=100_000, commission=0.0, **params):
    """Run GapFillVWAP on one ticker's RTH DataFrame and return a compact stats dict.
    `df` needs Open/High/Low/Close/Volume columns and an ET-local DatetimeIndex.
    `params` override the class-level tuneables (gap_min_pct, warmup_min, ...)."""
    if df is None or len(df) < 50:
        return {"trades": 0, "error": "insufficient data"}
    bt = Backtest(df, GapFillVWAP, cash=cash, commission=commission,
                  trade_on_close=False, exclusive_orders=True)
    stats = bt.run(**params)
    _pf = stats.get("Profit Factor")
    # backtesting.py returns NaN for PF when there are no losing trades — keep that
    # as None (→ JSON null) so the UI can show ∞ instead of a misleading 0.
    pf = None if (_pf is None or _pf != _pf) else round(float(_pf), 2)
    return {
        "trades":      int(_num(stats.get("# Trades"))),
        "win_rate":    round(_num(stats.get("Win Rate [%]")), 1),
        "profit_factor": pf,
        "return_pct":  round(_num(stats.get("Return [%]")), 2),
        "expectancy_pct": round(_num(stats.get("Expectancy [%]")), 3),
        "avg_trade_pct":  round(_num(stats.get("Avg. Trade [%]")), 3),
        "max_drawdown_pct": round(_num(stats.get("Max. Drawdown [%]")), 2),
    }
