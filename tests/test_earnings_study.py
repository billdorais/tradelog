"""Earnings reaction study — measures the session after each announcement.

Inverts the gap-fill backtest (enumerate earnings dates -> measure the next
session) so the sample is ~24 events/ticker instead of whatever lands inside a
59-day intraday window. Daily bars by necessity: Yahoo serves only 59 days of 5m.
"""
from __future__ import annotations

import datetime as dt
import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import strategies.earnings_study as es


def _bar(d, o, h, l, c, v=1e6):
    return {"time": dt.datetime.fromisoformat(d), "open": o, "high": h,
            "low": l, "close": c, "volume": v}


# Session 0 = the day before earnings (close 100).
# Session 1 = reaction day: gaps UP to 105 (+5%), dips to 99 (fills back through
#             100), closes 103 (faded from the open).
GAP_UP_FILLED = [
    _bar("2026-01-05", 99, 101, 98, 100),
    _bar("2026-01-06", 105, 106, 99, 103),
]


def test_gap_fill_and_fade_are_measured_from_the_open(monkeypatch):
    monkeypatch.setattr(es, "announcement_dates",
                        lambda t, limit=24, raise_on_error=False: [dt.date(2026, 1, 6)])
    rows = es._reaction_rows("AAPL", GAP_UP_FILLED)
    assert len(rows) == 1
    r = rows[0]
    assert r["date"] == "2026-01-06"
    assert r["gap_pct"] == pytest.approx(5.0)      # 100 -> 105
    assert r["direction"] == "up"
    assert r["filled"] is True                     # low 99 <= prior close 100
    # close 103 vs open 105 = -1.905% ; an up-gap that closes below its open faded
    assert r["close_open_pct"] == pytest.approx(-1.905, abs=0.01)
    assert r["follow_pct"] < 0                     # negative = faded


def test_down_gap_fill_uses_the_high_not_the_low(monkeypatch):
    """A down gap fills by trading back UP through the prior close."""
    bars = [
        _bar("2026-01-05", 99, 101, 98, 100),
        _bar("2026-01-06", 95, 101, 94, 97),       # gap -5%, high 101 > 100
    ]
    monkeypatch.setattr(es, "announcement_dates",
                        lambda t, limit=24, raise_on_error=False: [dt.date(2026, 1, 6)])
    r = es._reaction_rows("X", bars)[0]
    assert r["direction"] == "down" and r["gap_pct"] == pytest.approx(-5.0)
    assert r["filled"] is True
    # close 97 vs open 95 = +2.1% up, which for a DOWN gap is a fade (retrace).
    assert r["close_open_pct"] > 0 and r["follow_pct"] < 0


def test_unfilled_gap(monkeypatch):
    bars = [
        _bar("2026-01-05", 99, 101, 98, 100),
        _bar("2026-01-06", 105, 110, 104, 109),    # never trades back to 100
    ]
    monkeypatch.setattr(es, "announcement_dates",
                        lambda t, limit=24, raise_on_error=False: [dt.date(2026, 1, 6)])
    r = es._reaction_rows("X", bars)[0]
    assert r["filled"] is False
    assert r["follow_pct"] > 0                     # continued in the gap direction


def test_amc_announcement_picks_the_bigger_gap_session(monkeypatch):
    """No reliable BMO/AMC flag, so the larger |gap| session is the reaction."""
    bars = [
        _bar("2026-01-05", 99, 101, 98, 100),
        _bar("2026-01-06", 100.2, 101, 99, 100.5),   # announcement day: flat
        _bar("2026-01-07", 108, 109, 107, 108.5),    # next day: the real reaction
    ]
    monkeypatch.setattr(es, "announcement_dates",
                        lambda t, limit=24, raise_on_error=False: [dt.date(2026, 1, 6)])
    rows = es._reaction_rows("X", bars)
    assert len(rows) == 1
    assert rows[0]["date"] == "2026-01-07", "picked the flat session, not the gap"


def test_run_study_aggregates_and_surfaces_calendar_errors(monkeypatch):
    def _fake_fetch(tk, start, end, interval):
        return GAP_UP_FILLED

    def _anns(t, limit=24, raise_on_error=False):
        if t == "BROKEN":
            raise RuntimeError("Import lxml failed")
        return [dt.date(2026, 1, 6)]

    monkeypatch.setattr(es, "announcement_dates", _anns)
    out = es.run_study(["AAPL", "BROKEN"], fetch=_fake_fetch)

    assert out["total_events"] == 1
    assert out["pooled"]["overall"]["fill_rate"] == 100.0
    assert out["pooled"]["overall"]["faded_rate"] == 100.0
    assert out["pooled"]["by_direction"]["up"]["events"] == 1
    # A broken calendar must be reported, never silently reduce the sample.
    assert any("BROKEN" in e and "calendar unavailable" in e for e in out["errors"])
    assert [t["ticker"] for t in out["tickers"]] == ["AAPL"]


def test_gap_buckets_partition_by_absolute_gap(monkeypatch):
    monkeypatch.setattr(es, "announcement_dates",
                        lambda t, limit=24, raise_on_error=False: [dt.date(2026, 1, 6)])
    out = es.run_study(["AAPL"], fetch=lambda *a: GAP_UP_FILLED)
    buckets = {b["bucket"]: b for b in out["pooled"]["by_gap_bucket"]}
    assert buckets["5%+"]["events"] == 1        # the 5.0% gap lands in the top bucket
    assert buckets["0-1%"]["events"] == 0
