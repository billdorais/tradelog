"""Earnings-gap day mapping — announcement date → reaction session(s).

announcement_dates() hits yfinance (network), so we monkeypatch it; the logic
under test is earnings_gap_days: each announcement maps to the first session
on/after it (BMO) plus the next session (AMC), intersected with the trading
calendar we pass in.
"""
from __future__ import annotations

import datetime as dt

import strategies.earnings as earn


def _d(s):
    return dt.date.fromisoformat(s)


def test_gap_days_marks_announcement_and_next_session(monkeypatch):
    # Trading calendar (skips the weekend 10th/11th).
    sessions = [_d(x) for x in (
        "2026-01-07", "2026-01-08", "2026-01-09", "2026-01-12", "2026-01-13",
    )]
    # Announcement on the 8th → mark the 8th (BMO) and the 9th (AMC).
    monkeypatch.setattr(earn, "announcement_dates", lambda t, limit=24, raise_on_error=False: [_d("2026-01-08")])
    got = earn.earnings_gap_days("TEST", sessions)
    assert got == {_d("2026-01-08"), _d("2026-01-09")}


def test_gap_days_after_hours_rolls_over_weekend(monkeypatch):
    sessions = [_d(x) for x in (
        "2026-01-08", "2026-01-09", "2026-01-12", "2026-01-13",
    )]
    # Announcement Friday the 9th → mark Fri 9th and the NEXT session (Mon 12th),
    # skipping the weekend since it's not in the calendar.
    monkeypatch.setattr(earn, "announcement_dates", lambda t, limit=24, raise_on_error=False: [_d("2026-01-09")])
    got = earn.earnings_gap_days("TEST", sessions)
    assert got == {_d("2026-01-09"), _d("2026-01-12")}


def test_no_announcements_gives_empty(monkeypatch):
    sessions = [_d("2026-01-08"), _d("2026-01-09")]
    monkeypatch.setattr(earn, "announcement_dates", lambda t, limit=24, raise_on_error=False: [])
    assert earn.earnings_gap_days("TEST", sessions) == set()


def test_announcement_outside_calendar_ignored(monkeypatch):
    sessions = [_d("2026-01-08"), _d("2026-01-09")]
    # Announcement after the calendar ends → nothing to mark.
    monkeypatch.setattr(earn, "announcement_dates", lambda t, limit=24, raise_on_error=False: [_d("2026-03-01")])
    assert earn.earnings_gap_days("TEST", sessions) == set()
