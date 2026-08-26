"""A submitted max-hold close is not a closed position.

Observed 2026-08-26: AMD sat at 18.7m against a 15m limit showing "auto-close sent",
with nothing left watching it. The close order had been ACCEPTED, and on acceptance
the old code dropped the tracker, cleared the DB row, and flagged the symbol — so the
position survived every one of the three things that would have caught it.

Acceptance is not closure. An accepted order can be rejected async, partially fill,
or sit unfilled. The only authority is what the broker reports holding.
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

import app as kairos

TAG, SYM = "alpaca6", "AMD"
KEY = (TAG, SYM)


class _Broker:
    """Accepts every close, and only goes flat when told to."""
    def __init__(self, qty=-2.0):
        self.qty = qty
        self.close_calls = 0

    def _invalidate_pos_cache(self):
        pass

    def get_positions(self, raise_on_error=False):
        return [{"symbol": SYM, "qty": self.qty}] if self.qty else []

    def close_position(self, symbol):
        self.close_calls += 1
        return {"success": True}          # accepted — but nothing fills


@pytest.fixture
def env(monkeypatch):
    from datetime import datetime, timedelta, timezone
    broker = _Broker()
    monkeypatch.setattr(kairos, "ACCOUNTS_BY_TAG",
                        {TAG: {"broker": broker, "label": "Crew Live", "tag": TAG}})
    monkeypatch.setattr(kairos, "_clear_max_hold_db", lambda *a, **k: None)
    kairos._max_hold_positions.clear()
    kairos._max_hold_close_sent.clear()
    kairos._max_hold_fail_ticks.clear()
    kairos._auto_closed_symbols.clear()
    # Entered 19 minutes ago against a 15 minute limit.
    kairos._max_hold_positions[KEY] = {
        "entry_time": datetime.now(timezone.utc) - timedelta(minutes=19),
        "max_hold_mins": 15}
    return broker


def _tick(monkeypatch, at=None):
    if at is not None:
        monkeypatch.setattr(kairos.time, "time", lambda: at)
    kairos._check_max_hold_exits()


def test_the_position_stays_tracked_after_the_close_is_accepted(env):
    """The bug in one line: acceptance used to delete the tracker, so nothing was
    left to notice the position was still there."""
    kairos._check_max_hold_exits()
    assert env.close_calls == 1
    assert KEY in kairos._max_hold_positions, "stopped watching a position still open"
    assert KEY in kairos._max_hold_close_sent


def test_it_does_not_resubmit_while_the_fill_is_still_in_flight(env, monkeypatch):
    """A 3s poll loop must not fire a close every tick — that is order spam, and it
    is what fed the 09:35 rate-limit burst."""
    now = 1_000_000.0
    monkeypatch.setattr(kairos.time, "time", lambda: now)
    kairos._check_max_hold_exits()
    assert env.close_calls == 1
    for bump in (3, 9, 30, kairos.MAX_HOLD_CLOSE_VERIFY_SECS - 1):
        _tick(monkeypatch, now + bump)
    assert env.close_calls == 1, "resubmitted before the fill window elapsed"


def test_it_resubmits_once_the_fill_window_passes_and_it_is_still_open(env, monkeypatch):
    now = 1_000_000.0
    monkeypatch.setattr(kairos.time, "time", lambda: now)
    kairos._check_max_hold_exits()
    _tick(monkeypatch, now + kairos.MAX_HOLD_CLOSE_VERIFY_SECS + 1)
    assert env.close_calls == 2
    assert kairos._max_hold_close_sent[KEY]["n"] == 2


def test_retries_are_capped_rather_than_hammering_the_broker(env, monkeypatch):
    now = 1_000_000.0
    monkeypatch.setattr(kairos.time, "time", lambda: now)
    for i in range(12):
        _tick(monkeypatch, now + i * (kairos.MAX_HOLD_CLOSE_VERIFY_SECS + 1))
    assert env.close_calls == kairos.MAX_HOLD_CLOSE_MAX_ATTEMPTS


def test_giving_up_is_logged_loudly_and_only_once(env, monkeypatch, caplog):
    now = 1_000_000.0
    monkeypatch.setattr(kairos.time, "time", lambda: now)
    caplog.set_level("ERROR")
    for i in range(12):
        _tick(monkeypatch, now + i * (kairos.MAX_HOLD_CLOSE_VERIFY_SECS + 1))
    stuck = [r for r in caplog.records if "MAX HOLD STUCK" in r.getMessage()]
    assert len(stuck) == 1, f"expected one give-up ERROR, got {len(stuck)}"
    assert KEY in kairos._max_hold_positions, "a stuck position must stay visible"


def test_observing_the_position_flat_is_what_completes_the_close(env, monkeypatch):
    """The happy path: the broker reporting nothing held is the only thing that
    counts as done."""
    now = 1_000_000.0
    monkeypatch.setattr(kairos.time, "time", lambda: now)
    kairos._check_max_hold_exits()
    env.qty = 0.0                       # the close finally fills
    _tick(monkeypatch, now + kairos.MAX_HOLD_CLOSE_VERIFY_SECS + 1)
    assert KEY not in kairos._max_hold_positions
    assert KEY not in kairos._max_hold_close_sent
    assert KEY not in kairos._auto_closed_symbols
    assert env.close_calls == 1, "sent a second close at a flat position"


def test_a_flat_position_is_cleaned_up_even_before_the_fill_window(env, monkeypatch):
    """Cleanup must not sit behind the retry throttle, or a closed position would
    linger in the tracker for 45s."""
    now = 1_000_000.0
    monkeypatch.setattr(kairos.time, "time", lambda: now)
    kairos._check_max_hold_exits()
    env.qty = 0.0
    _tick(monkeypatch, now + 5)
    assert KEY not in kairos._max_hold_positions


def test_a_close_attempt_never_outlives_its_position(env, monkeypatch):
    """Left behind, the record would make the NEXT position in that symbol look
    already-handled and skip its close entirely."""
    now = 1_000_000.0
    monkeypatch.setattr(kairos.time, "time", lambda: now)
    kairos._check_max_hold_exits()
    assert KEY in kairos._max_hold_close_sent

    from datetime import datetime, timedelta, timezone
    env.qty = 0.0
    _tick(monkeypatch, now + 5)
    assert KEY not in kairos._max_hold_close_sent

    # A fresh position in the same symbol must get its own close.
    env.qty = -3.0
    kairos._max_hold_positions[KEY] = {
        "entry_time": datetime.now(timezone.utc) - timedelta(minutes=19),
        "max_hold_mins": 15}
    _tick(monkeypatch, now + 600)
    assert env.close_calls == 2, "new position inherited the old close record"


def test_a_released_timer_is_still_never_closed(env):
    """A negative limit means the user released this position to its trailing stop.
    The new retry path must not resurrect it."""
    kairos._max_hold_positions[KEY]["max_hold_mins"] = -15
    kairos._check_max_hold_exits()
    assert env.close_calls == 0
