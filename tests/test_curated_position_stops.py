"""Per-position stops (% stop + $ cap) apply to ALL curated books, not just TV
Refined; the farms run ungated and the old TV-Farm dollar stop is retired.

The user consolidated the two per-account risk cards into one "Curated Books" card
whose % stop + max-loss cap protect TV Refined (alpaca2), Kairos Refined (alpaca3)
and Crew Paper (alpaca4) with the same values. The farms (alpaca, alpaca5) are the
full-sample audition pools and must stay ungated so a stop can't bias the sample.

These tests drive _check_position_stops with a synthetic open position per broker
and assert which ones get closed.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


def test_curated_tags_are_the_three_books():
    assert a._CURATED_TAGS == {"alpaca2", "alpaca3", "alpaca4"}


def test_curated_hours_share_the_refined_window():
    # All three curated books follow the one "refined" hours window; farms stay "paper".
    for t in ("alpaca2", "alpaca3", "alpaca4"):
        assert a._HOURS_KEY_BY_TAG[t] == "refined"
    for t in ("alpaca", "alpaca5"):
        assert a._HOURS_KEY_BY_TAG[t] == "paper"


@pytest.fixture()
def _stops(monkeypatch):
    """A -$50 unrealized loss on every account, the % / $ caps set, no other stops."""
    monkeypatch.setattr(a, "MAX_POSITION_LOSS_PCT", 0.0)        # % stop off for this test
    monkeypatch.setattr(a, "MAX_POSITION_LOSS_REFINED", -40.0)  # $ cap at -$40
    monkeypatch.setattr(a, "MAX_POSITION_LOSS", -40.0)          # old farm stop (must NOT fire)
    monkeypatch.setattr(a, "MAX_TRAILING_GIVEBACK", 0.0)
    monkeypatch.setattr(a, "TAKE_PROFIT_DOLLARS", 0.0)
    monkeypatch.setattr(a, "TAKE_PROFIT_PCT", 0.0)

    positions = [
        {"symbol": "AAA", "qty": 10, "unrealized_pnl": -50.0, "market_value": 1000.0,
         "avg_entry_price": 100.0, "current_price": 95.0, "broker": tag}
        for tag in ("alpaca", "alpaca2", "alpaca3", "alpaca4", "alpaca5")
    ]

    closed = []

    class _FakeBroker:
        def __init__(self, tag, poss): self._tag, self._poss = tag, poss
        def _invalidate_pos_cache(self): pass
        def get_positions(self, raise_on_error=False):
            return [dict(p) for p in self._poss if p["broker"] == self._tag]

    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [
        {"tag": t, "num": i + 1, "label": t, "broker": _FakeBroker(t, positions)}
        for i, t in enumerate(("alpaca", "alpaca2", "alpaca3", "alpaca4", "alpaca5"))
    ])
    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {}, raising=False)
    # Record close attempts instead of hitting a broker.
    monkeypatch.setattr(a, "_resolve_position_entry", lambda *args, **kw: (None, None))

    def _fake_close(pos, reason):
        closed.append(pos["broker"])
        return True
    # _check_position_stops closes via broker record / _close_position paths; patch the
    # actual close helper the monitor uses.
    return closed, positions, _fake_close


def test_curated_books_get_the_dollar_cap_farms_do_not(monkeypatch, _stops):
    closed, positions, _fake_close = _stops
    # Intercept the close so we can see which brokers would be flattened.
    hit = []

    def _spy_close(*args, **kwargs):
        # first positional arg is the position dict in the monitor's close path
        return True
    # Rather than reach into the close internals, re-implement the decision the way
    # _check_position_stops does and assert on the trigger set. Drive the real function
    # but capture log.error calls (it logs "POSITION STOP (...)" per close).
    import logging
    records = []

    class _H(logging.Handler):
        def emit(self, r): records.append(r.getMessage())

    h = _H()
    a.log.addHandler(h)
    try:
        a._check_position_stops()
    finally:
        a.log.removeHandler(h)

    fired = " ".join(records)
    # Curated books hit the -$40 cap on a -$50 loss.
    for tag in ("alpaca2", "alpaca3", "alpaca4"):
        assert f"[{tag}]" in fired, f"expected {tag} to be stopped\n{fired}"
    # Farms must NOT be stopped — the old MAX_POSITION_LOSS farm stop is retired.
    for tag in ("alpaca", "alpaca5"):
        assert f"[{tag}]" not in fired, f"{tag} should run ungated\n{fired}"
