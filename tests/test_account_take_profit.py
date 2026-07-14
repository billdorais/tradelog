"""Per-account take-profit — each paper book carries its own band TP.

The TP sweep's per-band apply writes here scoped to the account it swept, so
Kairos can hold a band TP that Refined doesn't. Enforced by the risk monitor
ahead of the cross-account rule TP and the global TP.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import shutil
import sqlite3

import pytest


@pytest.fixture()
def tp_app(tmp_path):
    import app as a
    db = tmp_path / "tp.db"
    shutil.copy("trades.db", db)

    def _fake_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    saved = (a.get_db, dict(a.ACCOUNTS_BY_NUM), dict(a.ACCOUNTS_BY_TAG))
    a.get_db = _fake_db
    a.ACCOUNTS_BY_NUM = {"3": {"tag": "alpaca3", "label": "Kairos engine"}}
    a.ACCOUNTS_BY_TAG = {"alpaca3": {"tag": "alpaca3", "label": "Kairos engine"}}
    a._route_tp_acct_cache = {}
    a._route_tp_acct_ts = 0.0
    yield a
    a.get_db, a.ACCOUNTS_BY_NUM, a.ACCOUNTS_BY_TAG = saved[0], saved[1], saved[2]
    a._route_tp_acct_cache = {}
    a._route_tp_acct_ts = 0.0


def test_set_lookup_isolation_and_clear(tp_app):
    a = tp_app
    client = a.app.test_client()

    # Set Kairos R4S4-breakout TP; other accounts/bands must be unaffected.
    r = client.post("/api/routing/take_profit_band",
                    json={"account": "3", "band_key": "BREAKOUT_R4S4", "take_profit_pct": 1.25})
    assert r.status_code == 200 and r.get_json()["action"] == "set"
    a._route_tp_acct_ts = 0.0  # force reload

    assert a._get_account_take_profit_pct("AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "alpaca3") == 1.25
    # matches the band via the strategy's TYPE_LEVEL, any ticker
    assert a._get_account_take_profit_pct("NVDA_CAM_BREAKOUT_R4S4_V02_5MIN", "alpaca3") == 1.25
    # per-account isolation: Refined does NOT inherit Kairos's TP
    assert a._get_account_take_profit_pct("AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "alpaca2") is None
    # per-band isolation: a different band on the same account is untouched
    assert a._get_account_take_profit_pct("AAPL_CAM_BREAKOUT_R3S3_V02_5MIN", "alpaca3") is None

    # Exposed in risk status.
    assert client.get("/api/risk/status").get_json()["take_profit_by_account"] == {"alpaca3": {"BREAKOUT_R4S4": 1.25}}

    # Clear removes it (and prunes the now-empty account).
    r = client.post("/api/routing/take_profit_band",
                    json={"account": "3", "band_key": "BREAKOUT_R4S4", "clear": True})
    assert r.get_json()["action"] == "cleared"
    a._route_tp_acct_ts = 0.0
    assert a._get_account_take_profit_pct("AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "alpaca3") is None
    assert a._account_tp_map() == {}


def test_zero_or_negative_clears(tp_app):
    a = tp_app
    client = a.app.test_client()
    client.post("/api/routing/take_profit_band",
                json={"account": "alpaca3", "band_key": "REVERSAL_R4S4", "take_profit_pct": 0.5})
    a._route_tp_acct_ts = 0.0
    assert a._get_account_take_profit_pct("V_CAM_REVERSAL_R4S4_V02_5MIN", "alpaca3") == 0.5
    # tp <= 0 is a clear, not a set to zero.
    r = client.post("/api/routing/take_profit_band",
                    json={"account": "alpaca3", "band_key": "REVERSAL_R4S4", "take_profit_pct": 0})
    assert r.get_json()["action"] == "cleared"
    a._route_tp_acct_ts = 0.0
    assert a._get_account_take_profit_pct("V_CAM_REVERSAL_R4S4_V02_5MIN", "alpaca3") is None
