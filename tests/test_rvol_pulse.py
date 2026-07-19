"""RVOL Pulse endpoint — live relative volume for indexes + watchlist, zoned
against the gate band with a live gate-pass preview.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


@pytest.fixture()
def _pulse(monkeypatch):
    monkeypatch.setattr(a, "RVOL_GATE_MIN", 1.5)
    monkeypatch.setattr(a, "RVOL_GATE_SHORT_CAP", 3.0)
    monkeypatch.setattr(a, "RVOL_GATE_ENABLED", True)
    # Deterministic per-ticker RVOL covering every zone.
    vals = {"SPY": 1.6, "QQQ": 2.1, "IWM": 1.4,      # indexes
            "NVDA": 2.4, "TSLA": 0.7, "UNH": 3.3,    # sweet / dead / blow-off
            "AAPL": 1.2, "GLD": None}                # warming / no-data
    monkeypatch.setattr(a, "_live_rvol", lambda t, lookback=20, now_dt=None: vals.get(t.upper()))
    monkeypatch.setattr(a, "_pulse_watchlist",
                        lambda limit=18: ["NVDA", "TSLA", "UNH", "AAPL", "GLD"])
    return vals


def test_zoning_and_gate_preview(_pulse):
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/rvol/pulse").get_json()
    by = {r["ticker"]: r for r in d["watch"]}
    assert by["NVDA"]["zone"] == "sweet"    and by["NVDA"]["pass_long"] is True
    assert by["UNH"]["zone"]  == "blowoff"                                      # >= 3.0 cap
    assert by["UNH"]["pass_long"] is True   and by["UNH"]["pass_short"] is False  # short-capped
    assert by["AAPL"]["zone"] == "warming"  and by["AAPL"]["pass_long"] is False  # < 1.5
    assert by["TSLA"]["zone"] == "dead"     and by["TSLA"]["pass_long"] is False
    assert by["GLD"]["zone"]  == "nodata"   and by["GLD"]["rvol"] is None
    # No-data (fails open in the gate) is NOT reported as a pass here — it's just unknown.
    assert by["GLD"]["pass_long"] is False


def test_market_pulse_aggregate(_pulse):
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/rvol/pulse").get_json()
    # avg(1.6, 2.1, 1.4) = 1.70 → "Active"
    assert d["market_pulse"] == 1.7
    assert d["pulse_label"] == "Active"
    assert [r["ticker"] for r in d["indexes"]] == ["SPY", "QQQ", "IWM"]


def test_watch_sorted_hottest_first_nodata_last(_pulse):
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/rvol/pulse").get_json()
    order = [r["ticker"] for r in d["watch"]]
    assert order[0] == "UNH"          # 3.3 highest
    assert order[-1] == "GLD"         # None sorts last


def test_tickers_param_overrides_watchlist(_pulse):
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/rvol/pulse?tickers=NVDA,AAPL").get_json()
    assert {r["ticker"] for r in d["watch"]} == {"NVDA", "AAPL"}
