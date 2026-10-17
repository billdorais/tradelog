"""1-hour backtests: bars aligned to the 9:30 open, and flat at the close.

Alpaca's native hourly bars are clock-aligned, so the RTH filter dropped the 9:00
bar and with it the 9:30-10:00 open, and every later bar straddled TradingView's
session bars. The breakout strategies also carried positions overnight, which the
live books never do.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from backtesting import Backtest

from strategies import data as bt_data
from strategies.bt_strategies import (
    CamarillaBreakout, CamarillaBreakoutPct, CamarillaEMA8, CamarillaR3S3BreakoutPct,
    _last_bar_of_day,
)


def _five_min_utc(days=("2026-10-14", "2026-10-15"), start="09:00", end="16:25"):
    """5-minute bars incl. pre/post-market, UTC index, price = minutes since 9:00 ET."""
    frames = []
    for d in days:
        idx = pd.date_range(f"{d} {start}", f"{d} {end}", freq="5min", tz="America/New_York")
        m = np.array([(t.hour - 9) * 60 + t.minute for t in idx], dtype=float)
        frames.append(pd.DataFrame({"open": 100 + m, "high": 100.5 + m, "low": 99.5 + m,
                                    "close": 100.2 + m, "volume": 10.0}, index=idx))
    return pd.concat(frames).tz_convert("UTC")


def test_hourly_bars_start_at_the_open_and_end_with_a_half_bar():
    out = bt_data.session_hourly(_five_min_utc())
    et = out.tz_convert("America/New_York")
    day = et[et.index.date == pd.Timestamp("2026-10-14").date()]
    assert [t.strftime("%H:%M") for t in day.index] == [
        "09:30", "10:30", "11:30", "12:30", "13:30", "14:30", "15:30"]
    first = day.iloc[0]
    assert first["open"] == 130          # the 9:30 bar's open, not pre-market
    assert first["high"] == 100.5 + 85   # max through the 10:25 bar
    assert first["close"] == 100.2 + 85  # 10:25 bar's close
    assert first["volume"] == 120        # twelve 5-minute bars
    assert day.iloc[-1]["volume"] == 60  # 15:30-16:00 is six bars
    assert len(et) == 14                 # nothing from pre/post-market


def _fake_alpaca(monkeypatch, seen):
    """Stand-in alpaca-py modules — the SDK isn't needed to test which bars are asked for."""
    import sys
    import types

    class TimeFrameUnit:
        Minute, Hour, Day = "Min", "Hour", "Day"

    class TimeFrame:
        def __init__(self, amount, unit):
            self.amount, self.unit = amount, unit

        def __repr__(self):
            return f"{self.amount}{self.unit}"
    TimeFrame.Day, TimeFrame.Hour = TimeFrame(1, "Day"), TimeFrame(1, "Hour")

    class StockBarsRequest:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    class _Client:
        def __init__(self, *a, **k):
            pass

        def get_stock_bars(self, req):
            seen["tf"] = repr(req.timeframe)
            frame = _five_min_utc(days=("2026-10-14",))
            frame.index = pd.MultiIndex.from_product([["SPY"], frame.index],
                                                     names=["symbol", "timestamp"])
            return types.SimpleNamespace(df=frame)

    mods = {
        "alpaca": types.ModuleType("alpaca"),
        "alpaca.data": types.ModuleType("alpaca.data"),
        "alpaca.data.historical": types.SimpleNamespace(StockHistoricalDataClient=_Client),
        "alpaca.data.requests": types.SimpleNamespace(StockBarsRequest=StockBarsRequest),
        "alpaca.data.timeframe": types.SimpleNamespace(TimeFrame=TimeFrame, TimeFrameUnit=TimeFrameUnit),
        "alpaca.data.enums": types.SimpleNamespace(DataFeed=types.SimpleNamespace(SIP="sip", IEX="iex")),
    }
    for k, v in mods.items():
        monkeypatch.setitem(sys.modules, k, v)


def test_hourly_request_fetches_five_minute_bars(monkeypatch):
    seen = {}
    _fake_alpaca(monkeypatch, seen)
    bars = bt_data.fetch_bars_alpaca("SPY", "2026-10-14", "2026-10-15", "1h")
    assert seen["tf"] == "5Min"
    assert len(bars) == 7
    # Naive UTC, like every other interval: 9:30 ET on Oct 14 = 13:30 UTC.
    assert bars[0]["time"] == pd.Timestamp("2026-10-14 13:30").to_pydatetime()


def test_last_bar_of_day_flags_each_session_close_and_ignores_daily_data():
    intraday = pd.DatetimeIndex(["2026-10-14 15:25", "2026-10-14 15:30",
                                 "2026-10-15 09:30", "2026-10-15 15:30"])
    assert _last_bar_of_day(intraday).tolist() == [False, True, False, True]
    daily = pd.DatetimeIndex(["2026-10-13", "2026-10-14", "2026-10-15"])
    assert not _last_bar_of_day(daily).any()


def _run(cls, df, **kw):
    # finalize_trades: a trade still open on the last bar is listed too, so a
    # hold-overnight run can't hide its overnight trade by never closing it.
    return Backtest(df, cls, cash=100_000, commission=0.0, exclusive_orders=True,
                    trade_on_close=True, finalize_trades=True).run(**kw)


@pytest.mark.parametrize("cls,params", [
    (CamarillaBreakout, dict(stop_loss=5, trail_activation=5, trail_offset=1)),
    (CamarillaEMA8, dict(stop_loss=5, trail_activation=5, trail_offset=1)),
    (CamarillaBreakoutPct, dict(stop_loss_pct=5, trail_activation_pct=5, trail_offset_pct=1)),
    (CamarillaR3S3BreakoutPct, dict(stop_loss_pct=5, trail_act_pct=5, trail_offset_pct=1)),
])
def test_breakouts_are_flat_overnight(cls, params, intraday_5m_ohlc):
    # Wide exits so trades would otherwise run for days.
    df = intraday_5m_ohlc.copy()
    df.index = df.index.tz_localize(None)
    trades = _run(cls, df, **params)["_trades"]
    assert len(trades), "fixture produced no trades — the test would prove nothing"
    for _, t in trades.iterrows():
        assert pd.Timestamp(t["EntryTime"]).date() == pd.Timestamp(t["ExitTime"]).date()
        assert pd.Timestamp(t["EntryTime"]).strftime("%H:%M") != "15:55"   # no entry on the last bar


def test_eod_flatten_off_can_hold_overnight(intraday_5m_ohlc):
    df = intraday_5m_ohlc.copy()
    df.index = df.index.tz_localize(None)
    held = _run(CamarillaBreakout, df, eod_flatten=0,
                stop_loss=50, trail_activation=50, trail_offset=10)["_trades"]
    assert any(pd.Timestamp(t["EntryTime"]).date() != pd.Timestamp(t["ExitTime"]).date()
               for _, t in held.iterrows())
