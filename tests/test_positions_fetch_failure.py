"""A failed positions fetch must not read as "the account is flat".

AlpacaBroker.get_positions() swallowed every exception and returned [], so a
transient network blip (stale keep-alive socket -> RemoteDisconnected, seen ~2x/day
in the diagnostics ring) was indistinguishable from a genuinely empty account.
Several risk paths had written defensive `except` blocks / failed-broker sets that
could therefore never fire — most damagingly the max-hold checker, which deletes a
position's max-hold timer when it sees no matching position.

get_positions(raise_on_error=True) restores the "failure is loud" contract for the
callers that act on emptiness.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

from brokers.alpaca_broker import AlpacaBroker


class _ConnAborted(Exception):
    """Stand-in for requests.exceptions.ConnectionError (requests isn't a direct
    dependency of the test env; the broker catches bare Exception anyway)."""


class _BoomTrading:
    """Stands in for the Alpaca SDK client, reproducing the observed failure."""
    def get_all_positions(self):
        raise _ConnAborted("('Connection aborted.', RemoteDisconnected('Remote end "
                           "closed connection without response'))")


def _broker_that_fails():
    b = AlpacaBroker.__new__(AlpacaBroker)      # bypass __init__ (needs API keys)
    b._trading       = _BoomTrading()
    b._pos_cache     = None
    b._pos_cache_ts  = 0.0
    b._paper         = True
    b._ensure_client = lambda: None
    return b


def test_default_still_returns_empty_for_display_callers():
    """Dashboard/display callers keep the old forgiving behaviour."""
    assert _broker_that_fails().get_positions() == []


def test_raise_on_error_propagates_instead_of_looking_flat():
    with pytest.raises(_ConnAborted):
        _broker_that_fails().get_positions(raise_on_error=True)


def test_failure_is_not_cached_as_empty():
    """A blip must not poison the broker's 20s position cache with []."""
    b = _broker_that_fails()
    with pytest.raises(Exception):
        b.get_positions(raise_on_error=True)
    assert b._pos_cache is None, "failed fetch left an empty list in the cache"


def test_max_hold_timer_survives_a_failed_fetch(monkeypatch):
    """The regression that mattered: a network blip during the max-hold check
    used to look like 'position closed' and delete the timer."""
    import datetime as _dt

    import app as a

    broker = _broker_that_fails()
    broker._invalidate_pos_cache = lambda: None
    tag, sym = "alpaca2", "GOOG"
    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {tag: {"broker": broker, "tag": tag}})
    monkeypatch.setattr(a, "_clear_max_hold_db", lambda *args, **kw: None)

    entry = _dt.datetime.now(a.ZoneInfo("America/New_York")) - _dt.timedelta(minutes=99)
    with a._risk_lock:
        a._max_hold_positions.clear()
        a._max_hold_positions[(tag, sym)] = {"entry_time": entry, "max_hold_mins": 15}
    try:
        a._check_max_hold_exits()
        with a._risk_lock:
            assert (tag, sym) in a._max_hold_positions, \
                "max-hold timer was dropped because a failed fetch read as 'closed'"
    finally:
        with a._risk_lock:
            a._max_hold_positions.clear()
