"""The ORB (stocks in play) preset on the Progress page.

The Opening Range Breakout holds to the close and exits on its own Pine alerts, so
its rule must differ from the Camarilla default in two ways that are easy to get
wrong: no broker-side trail (a trail makes the webhook drop the Pine's exit
alerts) and its own full-session max hold (the global backstop would otherwise
close every trade 15 minutes in).
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import shutil
import sqlite3

import pytest


@pytest.fixture()
def db(tmp_path, monkeypatch):
    import app as a
    path = tmp_path / "orb.db"
    shutil.copy("trades.db", path)
    conn = sqlite3.connect(path)
    conn.execute("DELETE FROM routing_rules")
    conn.commit(); conn.close()

    def _db():
        c = sqlite3.connect(path); c.row_factory = sqlite3.Row
        return c
    monkeypatch.setattr(a, "get_db", _db)
    return path


def _rules(path):
    c = sqlite3.connect(path)
    out = {n: json.loads(nodes) for n, nodes in c.execute("SELECT name, nodes FROM routing_rules")}
    c.close()
    return out


def test_orb_preset_creates_one_rule_that_lets_the_pine_exit(db):
    import app as a
    r = a.app.test_client().post("/api/progress/add_ticker", json={"ticker": "spy", "mode": "orb"})
    body = r.get_json()
    assert r.status_code == 200, body
    assert body["created"] == ["SPY_ORB_RVOL_V01_5MIN"]
    nodes = _rules(db)["SPY_ORB_RVOL_V01_5MIN"]
    ep = next(n for n in nodes if n["type"] == "exit_params")
    assert ep["trail_offset"] is None                 # Pine exit alerts must pass the webhook
    assert ep["max_hold_mins"] == 390                 # overrides the 15-min global backstop
    assert {"type": "broker", "value": "alpaca-paper"} in nodes      # TV Farm auditions it
    assert {"type": "strategy", "value": "SPY_ORB_RVOL_V01_5MIN"} in nodes
    assert body["specs"][0]["tv_settings"]["pine_script"] == "ORB_RVOL_V01"


def test_orb_preset_is_idempotent(db):
    import app as a
    c = a.app.test_client()
    c.post("/api/progress/add_ticker", json={"ticker": "QQQ", "mode": "orb"})
    r = c.post("/api/progress/add_ticker", json={"ticker": "QQQ", "mode": "orb"}).get_json()
    assert r["created"] == [] and r["skipped"] == ["QQQ_ORB_RVOL_V01_5MIN"]
    assert len(_rules(db)) == 1


def test_orb_name_stays_out_of_the_camarilla_machinery():
    """ENGINE_PILOT_ALL fires every BREAKOUT/REVERSAL pipeline and parses levels
    from _CAM_ names; an ORB name must match none of them."""
    import app as a
    name = a._orb_spec("SPY")["name"]
    for word in ("BREAKOUT", "REVERSAL", "_CAM_"):
        assert word not in name
    assert a._band_of(name) == ""


def test_camarilla_presets_keep_their_default_nodes(db):
    import app as a
    a.app.test_client().post("/api/progress/add_ticker", json={"ticker": "IWM", "mode": "cam5min"})
    rules = _rules(db)
    assert len(rules) == 4
    for nodes in rules.values():
        ep = next(n for n in nodes if n["type"] == "exit_params")
        assert ep["trail_offset"] == 0.15


def test_orb_on_a_camarilla_ticker_warns(db):
    import app as a
    c = a.app.test_client()
    c.post("/api/progress/add_ticker", json={"ticker": "SPY", "mode": "cam5min", "version": "V02"})
    r = c.post("/api/progress/add_ticker", json={"ticker": "SPY", "mode": "orb"}).get_json()
    assert r["created"] == ["SPY_ORB_RVOL_V01_5MIN"]
    assert r["warning"] and "4 Camarilla" in r["warning"]
    clean = c.post("/api/progress/add_ticker", json={"ticker": "XLE", "mode": "orb"}).get_json()
    assert clean["warning"] is None
