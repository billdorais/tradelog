"""RVOL surface data for the Visualizers page — per-minute RVOL grid aligned to a
390-slot RTH minute axis, one row per ticker.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as dt
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import app as a

UTC = dt.timezone.utc
ET = ZoneInfo("America/New_York")


def _bars(n=21, spike_at=20, vol=100, spike=300):
    # UTC 13:30 == 09:30 ET (EDT). n minutes from the open.
    out = []
    for i in range(n):
        h, m = 13 + (30 + i) // 60, (30 + i) % 60
        out.append(SimpleNamespace(timestamp=dt.datetime(2026, 7, 10, h, m, tzinfo=UTC),
                                   volume=float(spike if i == spike_at else vol),
                                   high=1.0, low=1.0, open=1.0, close=1.0))
    return out


def test_row_series_alignment_and_rvol(monkeypatch):
    monkeypatch.setattr(a, "_fetch_day_bars", lambda tk, ds: _bars())
    s = a._rvol_surface_row("AAA", "2026-07-10", lookback=20)
    assert len(s) == a._RTH_MINUTES == 390
    assert s[0] == 0.0                    # first bar: no prior → 0
    assert s[19] == 1.0                   # 100 / mean(prior 19 = 100)
    assert s[20] == 3.0                   # 300 / mean(prior 20 = 100) — the spike
    assert all(x == 0.0 for x in s[21:])  # nothing after the last bar


def test_row_none_when_no_bars(monkeypatch):
    monkeypatch.setattr(a, "_fetch_day_bars", lambda tk, ds: [])
    assert a._rvol_surface_row("AAA", "2026-07-10") is None


def test_surface_endpoint_grid_shape(monkeypatch):
    monkeypatch.setattr(a, "_fetch_day_bars", lambda tk, ds: _bars())
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/viz/rvol_surface?tickers=AAA,BBB&date=2026-07-10").get_json()
    assert d["date"] == "2026-07-10"
    assert d["tickers"] == ["AAA", "BBB"]
    assert d["rows"] == 2 and d["cols"] == 390
    assert len(d["grid"]) == 2 and len(d["grid"][0]) == 390
    assert d["max_rvol"] == 3.0


def test_surface_endpoint_drops_tickers_without_data(monkeypatch):
    # BBB has no bars → dropped from the grid; AAA stays.
    monkeypatch.setattr(a, "_fetch_day_bars",
                        lambda tk, ds: _bars() if tk.upper() == "AAA" else [])
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/viz/rvol_surface?tickers=AAA,BBB&date=2026-07-10").get_json()
    assert d["tickers"] == ["AAA"] and d["rows"] == 1


# ── Price timeline (live 3D price line, pre-market + RTH) ────────────────────

def _pbars(date_str):
    day = dt.date.fromisoformat(date_str)
    def bar(h, m, close):
        return SimpleNamespace(timestamp=dt.datetime(day.year, day.month, day.day, h, m, tzinfo=ET),
                               close=float(close), volume=1.0, high=close, low=close, open=close)
    # two pre-market (08:00, 09:00 ET) + two RTH (09:30, 10:00 ET)
    return [bar(8, 0, 550.0), bar(9, 0, 551.0), bar(9, 30, 552.0), bar(10, 0, 553.5)]


def test_price_timeline_sessions_and_axis(monkeypatch):
    monkeypatch.setattr(a, "_fetch_intraday_ext", lambda tk, ds: _pbars(ds))
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/viz/price_timeline?ticker=SPY&date=2026-07-10").get_json()
    assert d["ticker"] == "SPY" and d["date"] == "2026-07-10"
    # Axis is 04:00→20:00 (pre + rth + after-hours); 09:30 open, 16:00 close.
    assert d["axis_minutes"] == 960 and d["pre_end_t"] == 330 and d["rth_end_t"] == 720
    pts = d["points"]
    assert [p["s"] for p in pts] == ["pre", "pre", "rth", "rth"]
    assert [p["t"] for p in pts] == [240, 300, 330, 360]        # minutes past 04:00
    assert d["last_price"] == 553.5
    assert d["price_min"] == 550.0 and d["price_max"] == 553.5
    assert d["live"] is False                                   # a past date is static


def test_price_lines_groups_and_volume(monkeypatch):
    monkeypatch.setattr(a, "_fetch_intraday_ext", lambda tk, ds: _pbars(ds))
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/viz/price_lines?indexes=SPY,QQQ&refined=AAA&date=2026-07-10").get_json()
    idx = d["groups"]["index"]; ref = d["groups"]["refined"]
    assert [s["ticker"] for s in idx] == ["SPY", "QQQ"]     # index group, in order
    assert [s["ticker"] for s in ref] == ["AAA"]            # refined group, separate
    spy = idx[0]
    assert spy["last_price"] == 553.5                       # price series
    assert spy["vol_max"] == 1 and "last_vol" in spy        # volume series present for the toggle
    assert "rvol" in spy and "rvol_last" in spy             # per-ticker RVOL drives the emphasis
    assert [p["s"] for p in spy["points"]] == ["pre", "pre", "rth", "rth"]


def test_series_rvol_peak_and_last():
    # flat then a 3x spike, then back to baseline: peak=3.0, last≈1.0
    vols = [100] * 20 + [300, 100]
    peak, last = a._series_rvol(vols, lookback=20)
    assert peak == 3.0
    assert last < 1.5


def test_price_timeline_excludes_out_of_window(monkeypatch):
    day = "2026-07-10"
    def _mix(tk, ds):
        base = _pbars(ds)
        d = dt.date.fromisoformat(ds)
        # 03:00 ET (before the 04:00 axis) must be dropped.
        early = SimpleNamespace(timestamp=dt.datetime(d.year, d.month, d.day, 3, 0, tzinfo=ET),
                                close=549.0, volume=1.0, high=549.0, low=549.0, open=549.0)
        return [early] + base
    monkeypatch.setattr(a, "_fetch_intraday_ext", _mix)
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/viz/price_timeline?ticker=SPY&date=%s" % day).get_json()
    assert len(d["points"]) == 4                                # the 03:00 bar is excluded
    assert d["points"][0]["t"] == 240
