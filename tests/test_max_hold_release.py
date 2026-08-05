"""Per-position max-hold release — let a runner trail out instead of being
flattened at its time limit.

The subtlety is durability. _recover_max_hold_positions re-scans every 2 minutes
and re-arms any OPEN position it finds untracked, so simply deleting the timer
would silently restore it within two minutes. Release therefore NEGATES the
stored limit and keeps the key in _max_hold_positions: the exit checker skips
non-positive limits, the re-scan sees the position as tracked, and the magnitude
survives so the timer can be re-armed with its original value.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as dt

import pytest

import app as a


@pytest.fixture()
def tracked(monkeypatch):
    """One open position with a 15-minute timer that is already 99 minutes old."""
    tag, sym = "alpaca4", "AAPL"
    entry = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=99)
    monkeypatch.setattr(a, "_persist_max_hold", lambda *args, **kw: None)
    monkeypatch.setattr(a, "_clear_max_hold_db", lambda *args, **kw: None)
    with a._risk_lock:
        a._max_hold_positions.clear()
        a._max_hold_positions[(tag, sym)] = {"entry_time": entry, "max_hold_mins": 15.0}
    a.app.config["TESTING"] = True
    yield tag, sym
    with a._risk_lock:
        a._max_hold_positions.clear()


def _release(client, tag, sym, released=True):
    return client.post("/api/risk/max_hold/release",
                       json={"broker": tag, "symbol": sym, "released": released})


def test_release_negates_the_limit_and_keeps_the_key(tracked):
    tag, sym = tracked
    with a.app.test_client() as c:
        d = _release(c, tag, sym).get_json()
    assert d["released"] is True and d["max_hold_mins"] == 15.0
    with a._risk_lock:
        info = a._max_hold_positions[(tag, sym)]
    # Key retained (so the re-scan won't re-arm) and the original limit recoverable.
    assert info["max_hold_mins"] == -15.0


def test_released_position_is_not_force_closed(tracked, monkeypatch):
    """The regression that matters: 99 minutes into a 15-minute timer, a released
    position must not be closed."""
    tag, sym = tracked
    closed = []

    class _Broker:
        def _invalidate_pos_cache(self): pass
        def get_positions(self, raise_on_error=False):
            return [{"symbol": sym, "qty": 10}]
        def close_position(self, s):
            closed.append(s); return {"success": True}

    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {tag: {"broker": _Broker(), "tag": tag}})
    monkeypatch.setattr(a, "MAX_HOLD_ENFORCEMENT", True)

    a._check_max_hold_exits()
    assert closed == [sym], "armed timer should have closed the position"

    closed.clear()
    with a.app.test_client() as c:
        _release(c, tag, sym)
    with a._risk_lock:                     # clear the auto-close guard
        a._auto_closed_symbols.discard((tag, sym))
    a._check_max_hold_exits()
    assert closed == [], "released position was force-closed anyway"


def test_re_arming_restores_the_original_limit(tracked):
    tag, sym = tracked
    with a.app.test_client() as c:
        _release(c, tag, sym)
        d = _release(c, tag, sym, released=False).get_json()
    assert d["released"] is False and d["max_hold_mins"] == 15.0
    with a._risk_lock:
        assert a._max_hold_positions[(tag, sym)]["max_hold_mins"] == 15.0


def test_release_survives_the_recovery_rescan(tracked, monkeypatch):
    """_recover_max_hold_positions re-arms untracked open positions every 2 min.
    A released timer must not be picked up by that pass."""
    tag, sym = tracked
    with a.app.test_client() as c:
        _release(c, tag, sym)

    class _Broker:
        _paper = True
        def _invalidate_pos_cache(self): pass
        def get_positions(self, raise_on_error=False):
            return [{"symbol": sym, "qty": 10}]

    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [{"tag": tag, "broker": _Broker(),
                                                "num": "4", "label": "Crew"}])
    monkeypatch.setattr(a, "MAX_HOLD_MINS", 15)
    a._recover_max_hold_positions()
    with a._risk_lock:
        assert a._max_hold_positions[(tag, sym)]["max_hold_mins"] == -15.0, \
            "the 2-minute re-scan re-armed a released timer"


def test_release_requires_a_tracked_timer(tracked):
    with a.app.test_client() as c:
        r = _release(c, "alpaca4", "NOSUCH")
    assert r.status_code == 404


def test_risk_status_reports_released_with_a_positive_limit(tracked):
    tag, sym = tracked
    with a.app.test_client() as c:
        _release(c, tag, sym)
        timers = c.get("/api/risk/status").get_json()["max_hold_timers"]
    row = next(t for t in timers if t["symbol"] == sym)
    assert row["released"] is True
    assert row["max_hold_mins"] == 15.0, "UI needs the limit it would return to"
