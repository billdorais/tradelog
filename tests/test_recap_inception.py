"""Recap's "From start" window, and the limit that makes it honest.

The window itself is easy: leave the start open, pair everything, take the earliest
round-trip. The part worth testing is what it CANNOT see.

get_fills() reaches back FILLS_LOOKBACK_DAYS and no further. For a book that began
trading before that floor, the oldest trade returned is the WINDOW EDGE, not the
book's inception -- and because the window rolls forward daily, a naive "from start"
would keep reporting a first date that quietly advances, presenting a truncated
record as a complete one. That is the fail-silent shape this repo keeps producing,
so the clipped case has to be detected and labelled, not merely computed.
"""
from __future__ import annotations

import datetime as _dt
import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as kairos
from brokers.alpaca_broker import FILLS_LOOKBACK_DAYS


class _Broker:
    def account_equity(self): return 7500.0


def _rt(day):
    return {"strategy": "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN", "ticker": "NVDA",
            "side": "LONG", "pnl": 100.0, "qty": 10, "date": day.isoformat(),
            "entry_price": 100.0, "exit_price": 110.0,
            "entry_time": f"{day.isoformat()}T13:40:00+00:00",
            "exit_time":  f"{day.isoformat()}T13:55:00+00:00"}


def _today_et():
    return _dt.datetime.now(kairos.ZoneInfo("America/New_York")).date()


def _floor():
    return _today_et() - _dt.timedelta(days=FILLS_LOOKBACK_DAYS)


def _recap(monkeypatch, first_day, n=3, period="inception", frm="", to=""):
    rts = [_rt(first_day + _dt.timedelta(days=i * 5)) for i in range(n)]
    rec = {"broker": _Broker(), "label": "Crew Paper", "tag": "alpaca4",
           "num": "4", "paper": True, "fills_fn": (lambda: rts)}
    monkeypatch.setattr(kairos, "ACCOUNTS_BY_NUM", {"4": rec})
    monkeypatch.setattr(kairos, "ACCOUNTS_BY_TAG", {"alpaca4": rec})
    monkeypatch.setattr(kairos, "ALPACA_ACCOUNTS", [rec])

    def _paired(f, from_date="", to_date="", **kw):
        # Honour the window, or an open start could not be told from a closed one.
        keep = [t for t in f
                if (not from_date or t["date"] >= from_date)
                and (not to_date or t["date"] <= to_date)]
        return {"closed_clean": keep, "closed": keep, "orphans": [],
                "deduped": keep, "signal_lookup": {}}
    monkeypatch.setattr(kairos, "_pair_alpaca_fills_lifo", _paired)
    monkeypatch.setattr(kairos, "_fills_error", lambda a: None)
    return kairos._build_recap(account="4", period=period, frm=frm, to=to) or {}


# ── the window ────────────────────────────────────────────────────────────────

def test_inception_starts_at_the_first_trade_not_the_calendar(monkeypatch):
    start = _floor() + _dt.timedelta(days=40)
    d = _recap(monkeypatch, start)
    assert d["book"]["first_date"] == start.isoformat()
    assert d["book"]["trades"] == 3


def test_inception_reaches_further_back_than_a_month(monkeypatch):
    """The point of the button: a book older than the month windows shows all of it."""
    start = _floor() + _dt.timedelta(days=10)
    everything = _recap(monkeypatch, start, n=8)["book"]
    one_month  = _recap(monkeypatch, start, n=8, period="last_month")["book"]
    assert everything["trades"] > one_month["trades"]


# ── the limit ─────────────────────────────────────────────────────────────────

def test_a_book_older_than_the_fill_window_is_flagged_not_captioned(monkeypatch):
    """The oldest visible trade sits on the fetch floor, so it is the edge of the
    data rather than the book's start. Saying "since first trade" here would be a
    confident falsehood."""
    d = _recap(monkeypatch, _floor())
    assert d["book"]["history_clipped"] is True
    assert "limit of fill history" in d["week_label"]
    assert "Since first trade" not in d["week_label"]


def test_the_flag_has_a_weekend_of_grace(monkeypatch):
    """A book whose oldest trade lands a day or two inside the floor is
    indistinguishable from one clipped by it -- a Friday floor with no weekend
    trading looks identical either way, so it is flagged too."""
    assert _recap(monkeypatch, _floor() + _dt.timedelta(days=2)
                  )["book"]["history_clipped"] is True


def test_a_book_that_started_inside_the_window_is_not_flagged(monkeypatch):
    """The flag must not cry wolf, or it stops being read."""
    d = _recap(monkeypatch, _floor() + _dt.timedelta(days=30))
    assert d["book"]["history_clipped"] is False
    assert "Since first trade" in d["week_label"]


# ── degrading ─────────────────────────────────────────────────────────────────

def test_a_book_with_no_trades_says_so(monkeypatch):
    d = _recap(monkeypatch, _floor() + _dt.timedelta(days=30), n=0)
    assert d["book"]["first_date"] is None
    assert d["book"]["history_clipped"] is False
    assert "no trades" in d["week_label"]


def test_other_windows_are_untouched(monkeypatch):
    """first_date/history_clipped are inception-only; a normal period must not
    start reporting an inception date it never computed."""
    start = _floor() + _dt.timedelta(days=30)
    for period, frm, to in [("last_month", "", ""), ("this_week", "", ""),
                            ("", "2026-08-01", "2026-08-31")]:
        b = _recap(monkeypatch, start, period=period, frm=frm, to=to)["book"]
        assert b["first_date"] is None
        assert b["history_clipped"] is False


def test_return_on_equity_still_computed_over_the_inception_window(monkeypatch):
    """The percentage tile has to follow this window like any other."""
    b = _recap(monkeypatch, _floor() + _dt.timedelta(days=30))["book"]
    assert b["pnl"] == 300.0
    assert b["equity_start_est"] == 7200.0        # 7500 now - 300 earned
