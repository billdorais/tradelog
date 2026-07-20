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

import app as a

UTC = dt.timezone.utc


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
