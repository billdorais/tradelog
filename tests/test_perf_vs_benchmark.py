"""Performance-vs-benchmark overlay — /api/perf/vs_benchmark.

Every series is rebased to 0% on the first date so SPY (buy & hold) and the
curated books line up on one axis. Books scale by a fixed notional (cumulative
realized P&L / base) or, with base=equity, by the account's true Alpaca equity.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


_SPY = [
    {"date": "2026-01-02", "close": 100.0},
    {"date": "2026-01-05", "close": 101.0},
    {"date": "2026-01-06", "close": 99.0},
    {"date": "2026-01-07", "close": 103.0},
]


@pytest.fixture()
def _closes(monkeypatch):
    monkeypatch.setattr(a, "_fetch_daily_closes", lambda t, s, e: list(_SPY))


def test_benchmark_rebased_to_zero(_closes):
    """SPY alone rebases to 0% at the first close, then tracks % change."""
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/perf/vs_benchmark?accounts=&benchmark=SPY").get_json()
    assert d["dates"] == ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"]
    spy = next(s for s in d["series"] if s["key"] == "SPY")
    assert spy["benchmark"] is True
    assert spy["values"] == [0.0, 1.0, -1.0, 3.0]   # (close/100 - 1) * 100


def test_missing_benchmark_data_is_graceful(monkeypatch):
    """No benchmark bars → an empty-but-valid payload, never a 500."""
    monkeypatch.setattr(a, "_fetch_daily_closes", lambda t, s, e: [])
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/perf/vs_benchmark?accounts=2,3,4").get_json()
    assert d["dates"] == [] and d["series"] == []
    assert "error" in d


def test_equity_mode_rebases_account_equity(_closes, monkeypatch):
    """base=equity rebases the account's true Alpaca equity to the first in-range
    point, forward-filled onto the benchmark's trading days (01-06 is missing)."""
    class _FakeBroker:
        def get_portfolio_history(self, period="3M", timeframe="1D"):
            return [
                {"time": "2026-01-02", "equity": 100000.0, "pnl": 0.0},
                {"time": "2026-01-05", "equity": 100500.0, "pnl": 500.0},
                {"time": "2026-01-07", "equity": 101000.0, "pnl": 1000.0},
            ]
    monkeypatch.setattr(a, "_alpaca_account_ctx",
                        lambda acct: (_FakeBroker(), "alpaca2", "TV Refined", None))
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/perf/vs_benchmark?accounts=2&base=equity").get_json()
    book = next(s for s in d["series"] if s["key"] == "2")
    assert book["label"] == "TV Refined"
    assert book["values"] == [0.0, 0.5, 0.5, 1.0]   # rebased to 100000, ff on 01-06
