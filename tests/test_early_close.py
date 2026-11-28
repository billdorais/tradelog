"""Early closes (13:00 ET): entries stop 30 minutes before, the EOD flatten and the ORB
exit move up with the close.

2026-11-27 (the day after Thanksgiving) ran as a 16:00 day: the engine entered at
12:54–12:59, every max-hold close (13:04+) was accepted and never filled, and ORB's
15:55 exit never came, so seven positions sat open over the weekend.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

import app as a

ET = ZoneInfo("America/New_York")
EARLY = date(2026, 11, 27)
NORMAL = date(2026, 11, 30)


def _t(d, h, m):
    return datetime(d.year, d.month, d.day, h, m, tzinfo=ET)


def test_session_close_by_day():
    assert a._session_close_min(EARLY) == 13 * 60
    assert a._session_close_min("2026-12-24") == 13 * 60
    assert a._session_close_min(NORMAL) == 16 * 60


def test_alpaca_calendar_adds_closes_the_list_misses_but_never_extends_one(monkeypatch):
    monkeypatch.setattr(a, "_alpaca_session_close", lambda ds: 13 * 60 if ds == "2028-11-24" else 16 * 60)
    a._session_close_cache.clear()
    assert a._session_close_min("2028-11-24") == 13 * 60      # not in the list, Alpaca knows
    assert a._session_close_min(EARLY) == 13 * 60             # Alpaca saying 16:00 can't undo it


_REAL_ALPACA_CLOSE = a._alpaca_session_close      # conftest stubs it per test


def test_reads_the_close_from_alpacas_calendar_model(monkeypatch):
    from types import SimpleNamespace
    from alpaca.trading.models import Calendar
    day = Calendar(date="2026-11-27", open="09:30", close="13:00")
    br = SimpleNamespace(_ensure_client=lambda: None,
                         _trading=SimpleNamespace(get_calendar=lambda req: [day]))
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [{"broker": br}])
    assert _REAL_ALPACA_CLOSE("2026-11-27") == 13 * 60
    assert _REAL_ALPACA_CLOSE("2026-11-30") is None            # the calendar has no row for it


def test_an_unreadable_calendar_falls_back_and_never_raises(monkeypatch):
    def boom(ds):
        raise RuntimeError("alpaca 503")
    monkeypatch.setattr(a, "_alpaca_session_close", boom)
    a._session_close_cache.clear()
    assert a._session_close_min(EARLY) == 13 * 60
    assert a._session_close_min(NORMAL) == 16 * 60


def test_entries_stop_30_minutes_before_an_early_close():
    assert a._early_close_block_reason(_t(EARLY, 12, 29)) is None
    why = a._early_close_block_reason(_t(EARLY, 12, 30))
    assert why and "13:00" in why and "12:30" in why
    assert a._early_close_block_reason(_t(NORMAL, 15, 59)) is None


def test_the_hours_gate_refuses_the_12_59_entry_even_for_an_all_day_ticker(monkeypatch):
    monkeypatch.setattr(a, "_account_hours_windows", lambda acct: [])          # all-day book
    monkeypatch.setattr(a, "_hours_exempt_tickers", lambda acct: {"QQQ"})
    assert a._account_hours_ok("alpaca5", now_et=_t(EARLY, 12, 0), ticker="QQQ")
    assert not a._account_hours_ok("alpaca5", now_et=_t(EARLY, 12, 59), ticker="QQQ")
    assert not a._account_hours_ok("alpaca7", now_et=_t(EARLY, 12, 54))
    assert a._account_hours_ok("alpaca5", now_et=_t(NORMAL, 12, 59), ticker="QQQ")


def test_an_override_stop_time_does_not_beat_the_early_close(monkeypatch):
    monkeypatch.setattr(a, "_strategy_stop_mode", lambda s, acct: "override")
    monkeypatch.setattr(a, "_strategy_start_for", lambda s, acct: "09:35")
    assert not a._account_hours_ok("alpaca7", now_et=_t(EARLY, 12, 45), strategy="QQQ_CAM_BREAKOUT_R4S4_V02_5MIN")
    assert a._account_hours_ok("alpaca7", now_et=_t(NORMAL, 12, 45), strategy="QQQ_CAM_BREAKOUT_R4S4_V02_5MIN")


@pytest.mark.parametrize("d,h,m,due", [
    (EARLY, 12, 57, False), (EARLY, 12, 58, True), (EARLY, 13, 0, True), (EARLY, 15, 58, False),
    (NORMAL, 15, 57, False), (NORMAL, 15, 58, True), (NORMAL, 16, 0, True), (NORMAL, 12, 58, False),
])
def test_eod_flatten_window_follows_the_close(d, h, m, due):
    assert a._eod_flatten_due(_t(d, h, m)) is due
