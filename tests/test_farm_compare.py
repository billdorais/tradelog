"""Two-account head-to-head compare (/api/engine_pilot/compare).

Defaults to TV Refined (2) vs Kairos engine (3) — the shape the entry-engine page
and the crew reader already consume. ?a=&b= generalizes it, the point being
?a=1&b=5: both farms trade EVERY pipeline, so they differ only in entry mechanism,
making that pair a controlled test of the entry itself.

The window auto-aligns to the later of the two accounts' first fills. Kairos Farm
is days old and TV Farm has months, so a naive 30-day window would hand TV Farm a
month of P&L and Kairos Farm three days of it, then read the gap as an engine
deficit. That failure is silent and confidently wrong, so it's pinned here.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as _dt

import pytest

import app as a


def _fill(day, symbol, side, price, qty=10):
    return {"symbol": symbol, "side": side, "price": price, "shares": qty,
            "time": f"{day}T15:00:00Z", "order_id": ""}


def _round_trip(day, symbol, entry, exit_):
    """One long round-trip: BOT then SLD on the same day."""
    return [_fill(day, symbol, "BOT", entry), _fill(day, symbol, "SLD", exit_)]


@pytest.fixture()
def farms(monkeypatch):
    """Registry with TV Farm (1, long-running) and Kairos Farm (5, brand new)."""
    old = _dt.date.today() - _dt.timedelta(days=40)
    new = _dt.date.today() - _dt.timedelta(days=2)

    # TV Farm: a winner 40 days ago (outside any aligned window), a loser 2 days ago.
    tv_fills = (_round_trip(old.isoformat(), "AAPL", 100.0, 150.0) +
                _round_trip(new.isoformat(), "AAPL", 100.0,  90.0))
    # Kairos Farm: only exists for the last 2 days, and made money there.
    kf_fills = _round_trip(new.isoformat(), "AAPL", 100.0, 110.0)

    reg = dict(a.ACCOUNTS_BY_NUM)
    reg["1"] = {"num": "1", "tag": "alpaca",  "label": "TV Farm",
                "broker": object(), "fills_fn": lambda: tv_fills}
    reg["5"] = {"num": "5", "tag": "alpaca5", "label": "Kairos Farm",
                "broker": object(), "fills_fn": lambda: kf_fills}
    # The curated pair, so the default (2 vs 3) path stays exercisable.
    reg["2"] = {"num": "2", "tag": "alpaca2", "label": "TV Refined",
                "broker": object(), "fills_fn": lambda: []}
    reg["3"] = {"num": "3", "tag": "alpaca3", "label": "Kairos Refined",
                "broker": object(), "fills_fn": lambda: []}
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", reg)
    a.app.config["TESTING"] = True
    return a.app.test_client(), old, new


def test_window_aligns_to_the_younger_farm(farms):
    client, old, new = farms
    d = client.get("/api/engine_pilot/compare?a=1&b=5&days=30").get_json()

    assert d["window_aligned"] is True
    # Starts the day Kairos Farm actually began trading, not 30 days back.
    assert d["from_date"] == new.isoformat()
    assert d["a"]["label"] == "TV Farm"
    assert d["b"]["label"] == "Kairos Farm"
    assert d["b"]["first_fill"] == new.isoformat()

    # TV Farm's 40-day-old +$500 winner is correctly excluded: over the common
    # window it is only the -$100 loser, so Kairos Farm (+$100) is ahead.
    assert d["tv"]["pnl"] == pytest.approx(-100.0)
    assert d["engine"]["pnl"] == pytest.approx(100.0)
    assert d["delta"] == pytest.approx(200.0)


def test_explicit_from_date_disables_alignment(farms):
    client, old, new = farms
    frm = (old - _dt.timedelta(days=1)).isoformat()
    d = client.get(f"/api/engine_pilot/compare?a=1&b=5&from_date={frm}").get_json()

    assert d["window_aligned"] is False
    assert d["from_date"] == frm
    # Unaligned, TV Farm's old winner counts and the read flips — which is exactly
    # the misleading comparison alignment exists to prevent.
    assert d["tv"]["pnl"] == pytest.approx(400.0)


def test_defaults_stay_two_vs_three(farms):
    """The entry-engine page and crew call this with no a/b — must not move."""
    client, _, _ = farms
    d = client.get("/api/engine_pilot/compare?days=30").get_json()
    assert d["a"]["account"] == "2"
    assert d["b"]["account"] == "3"
    # Legacy keys the existing consumers read.
    for k in ("days", "configured", "rows", "tv", "engine"):
        assert k in d


def test_unconfigured_account_is_rejected(farms):
    client, _, _ = farms
    r = client.get("/api/engine_pilot/compare?a=1&b=9")
    assert r.status_code == 400
    assert "not configured" in r.get_json()["error"]


def test_rows_carry_both_books_and_cumulative(farms):
    client, _, new = farms
    d = client.get("/api/engine_pilot/compare?a=1&b=5&days=30").get_json()
    row = next(r for r in d["rows"] if r["date"] == new.isoformat())
    assert row["tv_pnl"] == pytest.approx(-100.0)
    assert row["engine_pnl"] == pytest.approx(100.0)
    assert row["cum_tv"] == pytest.approx(-100.0)
    assert row["cum_engine"] == pytest.approx(100.0)
