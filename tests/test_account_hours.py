"""Per-account trading hours, resolved from the registry.

Hours was the one gate that wasn't registry-driven: _account_hours_ok inferred the
window from the tag ("alpaca2" -> REFINED_HOURS, everything else -> PAPER_HOURS),
and the engine asked it about "alpaca2" ONCE PER TICK regardless of which book the
entry was for. That was inert only because REFINED_HOURS is unset — the moment a
TV Refined window is configured (it's on the going-live checklist), every book the
engine feeds (alpaca3 via the snapshot, alpaca5 via ENGINE_PILOT_ALL, any rule
broker) would have gone silent outside TV Refined's window, with nothing in the
live path saying why.

Windows stay runtime-editable from Settings, so the resolver must read them live.
The farms must stay all-day: their symmetry is what makes farm-vs-farm a
controlled test of the entry mechanism.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as _dt
from zoneinfo import ZoneInfo

import pytest

import app as a

ET = ZoneInfo("America/New_York")


def _et(hh, mm):
    return _dt.datetime(2026, 7, 17, hh, mm, tzinfo=ET)


@pytest.fixture()
def hours(monkeypatch):
    """Set the two Settings-page windows; restore after."""
    def _set(paper=("", ""), refined=("", "")):
        monkeypatch.setattr(a, "PAPER_HOURS_START", paper[0])
        monkeypatch.setattr(a, "PAPER_HOURS_END",   paper[1])
        monkeypatch.setattr(a, "REFINED_HOURS_START", refined[0])
        monkeypatch.setattr(a, "REFINED_HOURS_END",   refined[1])
    return _set


def test_empty_window_allows_everything(hours):
    hours()
    for tag in ("alpaca", "alpaca2", "alpaca3", "alpaca5"):
        assert a._account_hours_ok(tag, now_et=_et(15, 30)) is True


def test_refined_window_applies_only_to_its_own_book(hours):
    """The regression that mattered: a TV Refined window must not mute the others."""
    hours(paper=("", ""), refined=("09:30", "11:00"))
    assert a._account_hours_ok("alpaca2", now_et=_et(10, 0))  is True   # inside
    assert a._account_hours_ok("alpaca2", now_et=_et(14, 0))  is False  # outside
    # Every other book is on PAPER_HOURS (empty) and keeps trading all day.
    for tag in ("alpaca", "alpaca3", "alpaca4", "alpaca5"):
        assert a._account_hours_ok(tag, now_et=_et(14, 0)) is True


def test_hours_key_is_registry_driven(hours, monkeypatch):
    """Pointing a book at the refined window is how you test a time-of-day
    hypothesis (e.g. does Kairos Refined improve on TV Refined's window?)."""
    hours(paper=("", ""), refined=("09:30", "11:00"))
    monkeypatch.setitem(a._HOURS_KEY_BY_TAG, "alpaca3", "refined")
    assert a._account_hours_ok("alpaca3", now_et=_et(10, 0)) is True
    assert a._account_hours_ok("alpaca3", now_et=_et(14, 0)) is False
    # The farms are untouched — their symmetry is the controlled entry test.
    assert a._account_hours_ok("alpaca",  now_et=_et(14, 0)) is True
    assert a._account_hours_ok("alpaca5", now_et=_et(14, 0)) is True


def test_current_config_farms_all_day_refined_on_refined_window():
    """Today's mapping. Update deliberately if a book's window source changes."""
    assert a._HOURS_KEY_BY_TAG.get("alpaca2") == "refined"
    for tag in ("alpaca", "alpaca3", "alpaca4", "alpaca5"):
        assert a._HOURS_KEY_BY_TAG.get(tag) == "paper"


def test_env_override_gives_one_book_its_own_window(hours, monkeypatch):
    hours(paper=("", ""), refined=("", ""))
    monkeypatch.setenv("HOURS_ALPACA3_START", "09:30")
    monkeypatch.setenv("HOURS_ALPACA3_END",   "11:00")
    assert a._account_hours("alpaca3") == ("09:30", "11:00")
    assert a._account_hours_ok("alpaca3", now_et=_et(14, 0)) is False
    assert a._account_hours_ok("alpaca5", now_et=_et(14, 0)) is True   # unaffected


def test_windows_are_read_live_not_cached(hours):
    """Settings edits take effect without a restart, so no caching the window."""
    hours(paper=("", ""), refined=("09:30", "11:00"))
    assert a._account_hours_ok("alpaca2", now_et=_et(14, 0)) is False
    hours(paper=("", ""), refined=("", ""))          # cleared from Settings
    assert a._account_hours_ok("alpaca2", now_et=_et(14, 0)) is True


def test_window_wrapping_past_midnight(hours):
    hours(paper=("22:00", "02:00"))
    assert a._account_hours_ok("alpaca", now_et=_et(23, 0)) is True
    assert a._account_hours_ok("alpaca", now_et=_et(1, 0))  is True
    assert a._account_hours_ok("alpaca", now_et=_et(12, 0)) is False


def test_engine_resolves_hours_per_target_not_a_fixed_account(hours, monkeypatch):
    """The bug this fixes: the engine gated every target on alpaca2's window.

    Simulates the engine's per-target loop decision for a tick at 14:00 ET with a
    TV Refined morning window configured. alpaca3/alpaca5 must still be allowed.
    """
    hours(paper=("", ""), refined=("09:30", "11:00"))
    now = _et(14, 0)
    targets = ["alpaca3", "alpaca5", "alpaca2"]
    allowed = [t for t in targets if a._account_hours_ok(t, now_et=now)]
    assert allowed == ["alpaca3", "alpaca5"]     # not [] — the old behaviour

    # And the tick-level short-circuit must stay open while ANY book can trade.
    accounts = [{"tag": t} for t in targets]
    assert any(a._account_hours_ok(x["tag"], now_et=now) for x in accounts) is True
