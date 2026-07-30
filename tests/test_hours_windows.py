"""Multi-window trading hours — accounts can trade in several windows per day
(e.g. 09:35-10:00 AND 12:00-15:55). _account_hours_ok is true if now is in ANY
window. Windows come from the shared refined/paper settings or a per-account
override (GATES_BY_ACCOUNT[tag].hours.windows).
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime
import json
from zoneinfo import ZoneInfo

import pytest

import app as a


def _et(h, m=0):
    return datetime.datetime(2026, 7, 30, h, m, tzinfo=ZoneInfo("America/New_York"))


@pytest.fixture()
def hstore(monkeypatch):
    store = {}
    monkeypatch.setattr(a, "_load_setting", lambda k, d=None: store.get(k, d))
    a._gates_acct_cache = {}
    a._gates_acct_ts = 0.0
    yield a, store
    a._gates_acct_cache = {}
    a._gates_acct_ts = 0.0


def test_parse_hours_windows_forms():
    p = a._parse_hours_windows
    assert p("09:35-10:00, 12:00-15:55") == [("09:35", "10:00"), ("12:00", "15:55")]
    assert p([{"start": "09:35", "end": "10:00"}, {"start": "12:00", "end": "15:55"}]) \
        == [("09:35", "10:00"), ("12:00", "15:55")]
    assert p(("09:30", "11:00")) == [("09:30", "11:00")]      # single pair
    assert p("") == [] and p(None) == [] and p([]) == []
    assert p([{"start": "", "end": ""}, {"start": "10:00", "end": ""}]) == []  # blanks dropped


def _set(store, key, windows):
    store[key] = json.dumps([{"start": s, "end": e} for s, e in windows])
    a._gates_acct_ts = 0.0


def test_per_account_two_windows(hstore):
    _a, store = hstore
    _set(store, "GATES_BY_ACCOUNT", [])  # placeholder to keep types happy
    store["GATES_BY_ACCOUNT"] = json.dumps(
        {"alpaca4": {"hours": {"windows": [{"start": "09:35", "end": "10:00"},
                                           {"start": "12:00", "end": "15:55"}]}}})
    a._gates_acct_ts = 0.0
    assert a._account_hours_ok("alpaca4", now_et=_et(9, 45)) is True    # in window 1
    assert a._account_hours_ok("alpaca4", now_et=_et(11, 0)) is False   # the pause gap
    assert a._account_hours_ok("alpaca4", now_et=_et(13, 0)) is True    # in window 2
    assert a._account_hours_ok("alpaca4", now_et=_et(16, 30)) is False  # after close
    # back-compat: _account_hours returns the first window
    assert a._account_hours("alpaca4") == ("09:35", "10:00")


def test_shared_refined_multi_window(hstore):
    _a, store = hstore
    _set(store, "REFINED_HOURS_WINDOWS", [("09:35", "10:00"), ("12:00", "15:55")])
    # alpaca2 (TV Refined) follows the shared refined windows
    assert a._account_hours_ok("alpaca2", now_et=_et(9, 45)) is True
    assert a._account_hours_ok("alpaca2", now_et=_et(11, 0)) is False
    assert a._account_hours_ok("alpaca2", now_et=_et(14, 0)) is True
