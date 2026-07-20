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
                        lambda limit=18, include_indexes=False: ["NVDA", "TSLA", "UNH", "AAPL", "GLD"])
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
    # refined_count reflects the watchlist (fixture has 5, none are indexes here)
    assert d["refined_count"] == 5
    assert d["refined_index_count"] == 0


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


def test_watchlist_is_the_refined_books_tickers(monkeypatch, tmp_path):
    """_pulse_watchlist pulls tickers from enabled rules targeting the refined
    books (alpaca-paper-2/-3), not just any recently-traded name."""
    import json
    import shutil
    import sqlite3
    db = tmp_path / "wl.db"; shutil.copy("trades.db", db)
    conn = sqlite3.connect(db); conn.execute("DELETE FROM routing_rules")
    def _rule(name, strat, broker, enabled=1):
        conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,?,?)",
                     (name, enabled, json.dumps([
                         {"type": "strategy", "value": strat},
                         {"type": "broker",   "value": broker}])))
    # Refined-book rules → should appear.
    _rule("NVDA->TVRef",   "NVDA_CAM_BREAKOUT_R4S4_V02_5MIN", "alpaca-paper-2")
    _rule("HOOD->Kairos",  "HOOD_CAM_BREAKOUT_R4S4_V02_5MIN", "alpaca-paper-3")
    # A farm-only rule (Paper All) → should NOT appear.
    _rule("ZZZZ->Farm",    "ZZZZ_CAM_BREAKOUT_R4S4_V02_5MIN", "alpaca-paper-1")
    # A disabled refined rule → should NOT appear.
    _rule("MSTR->TVRef",   "MSTR_CAM_BREAKOUT_R4S4_V02_5MIN", "alpaca-paper-2", enabled=0)
    conn.commit(); conn.close()

    # A refined rule on an index ticker (SPY) → excluded from tiles, but counted
    # when include_indexes=True (it's shown as a gauge, not a tile).
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                 ("SPY->TVRef", json.dumps([
                     {"type": "strategy", "value": "SPY_CAM_BREAKOUT_R4S4_V02_5MIN"},
                     {"type": "broker",   "value": "alpaca-paper-2"}])))
    conn.commit(); conn.close()

    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c
    monkeypatch.setattr(a, "get_db", _fake_db)
    wl = a._pulse_watchlist(limit=18)
    assert "NVDA" in wl and "HOOD" in wl        # refined-book tickers
    assert "ZZZZ" not in wl                      # farm-only, excluded
    assert "MSTR" not in wl                      # disabled rule, excluded
    assert "SPY" not in wl                       # index → not a tile by default
    # ...but include_indexes counts it (shown as a gauge, part of the honest total).
    assert "SPY" in a._pulse_watchlist(limit=18, include_indexes=True)
