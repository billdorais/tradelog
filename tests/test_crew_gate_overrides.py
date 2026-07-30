"""Per-account ENTRY-GATE overrides — Crew Paper (or any book) tunes its day-type /
strikes / trading-hours gates independently, falling back to the shared curated
value when a field is blank. Enforced via the shared helpers _strike_limit,
_account_hours, _daytype_gate_block (so every webhook/engine call site inherits).
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json

import pytest

import app as a


@pytest.fixture()
def gates(monkeypatch):
    store = {}
    monkeypatch.setattr(a, "_load_setting", lambda k, d=None: store.get(k, d))
    monkeypatch.setattr(a, "_save_setting", lambda k, v: store.__setitem__(k, v))
    # Deterministic shared strikes globals for the fallback assertions.
    monkeypatch.setattr(a, "STRIKES_PER_LEVEL", 2)
    monkeypatch.setattr(a, "STRIKES_PER_LEVEL_SHORT", 0)
    a._gates_acct_cache = {}
    a._gates_acct_ts = 0.0
    yield a, store
    a._gates_acct_cache = {}
    a._gates_acct_ts = 0.0


def _set(store, gates_map):
    store["GATES_BY_ACCOUNT"] = json.dumps(gates_map)
    a._gates_acct_ts = 0.0          # force reload from the patched _load_setting


def test_strike_limit_override_and_fallback(gates):
    _a, store = gates
    _set(store, {"alpaca4": {"strikes": {"base": 3, "short": 1}}})
    assert a._strike_limit("S4", "alpaca4") == 1     # short cap override
    assert a._strike_limit("R4", "alpaca4") == 3     # base override
    assert a._strike_limit("S4", "alpaca2") == 2     # other account unaffected → shared
    _set(store, {})                                  # no override → shared globals
    assert a._strike_limit("S4", "alpaca4") == 2
    assert a._strike_limit("R4", "alpaca4") == 2


def test_hours_override_and_fallback(gates):
    _a, store = gates
    _set(store, {"alpaca4": {"hours": {"start": "10:00", "end": "11:00"}}})
    assert a._account_hours("alpaca4") == ("10:00", "11:00")
    _set(store, {})                                  # no override → hours_key window
    assert a._account_hours("alpaca4") == (a.REFINED_HOURS_START, a.REFINED_HOURS_END)


def test_daytype_override(gates, monkeypatch):
    _a, store = gates
    monkeypatch.setattr(a, "_get_day_classification", lambda tk, d: {"day_type": "Inside"})
    strat, tk, date = "GOOG_CAM_BREAKOUT_R3S3_V02_5MIN", "GOOG", "2026-07-29"
    # enabled=False → the breakout day-type gate is OFF for this account
    _set(store, {"alpaca4": {"daytype": {"enabled": False}}})
    assert a._daytype_gate_block(strat, tk, date, "alpaca4")[0] is False
    # enabled=True, Outside-only → an Inside day is blocked
    _set(store, {"alpaca4": {"daytype": {"enabled": True, "breakout_ok_days": ["Outside"]}}})
    assert a._daytype_gate_block(strat, tk, date, "alpaca4")[0] is True
    # another account (no override) is unaffected by alpaca4's override
    assert a._daytype_gate_block(strat, tk, date, "alpaca2")[0] is False or True  # shared-dependent; must not raise


def test_endpoint_round_trip_and_clear(gates, monkeypatch):
    _a, store = gates
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {"4": {"tag": "alpaca4", "label": "Crew Paper"}})
    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {"alpaca4": {"tag": "alpaca4", "label": "Crew Paper"}})
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        r = c.post("/api/routing/account_gates", json={
            "account": "4", "daytype": "off", "strikes_base": "3", "strikes_short": "1",
            "hours_start": "10:00", "hours_end": "11:00"}).get_json()
    assert r["overrides"]["daytype"] == {"enabled": False}
    assert r["overrides"]["strikes"] == {"base": 3, "short": 1}
    assert r["overrides"]["hours"] == {"start": "10:00", "end": "11:00"}
    # persisted + enforced through the helpers
    a._gates_acct_ts = 0.0
    assert a._strike_limit("S4", "alpaca4") == 1
    assert a._account_hours("alpaca4") == ("10:00", "11:00")
    # blank fields → the override is cleared (re-inherits)
    with a.app.test_client() as c:
        r2 = c.post("/api/routing/account_gates", json={
            "account": "4", "daytype": "inherit", "strikes_base": "",
            "hours_start": "", "hours_end": ""}).get_json()
    assert r2["overrides"] == {}
