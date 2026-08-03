"""Dashboard load-path caching.

The dashboard fetches all 5 accounts on every refresh (and re-polls on 10/30s
timers), so anything uncached on that path multiplies by 5. Two costs used to sit
there: /api/alpaca/positions cached ONLY account 1 (4 live Alpaca round-trips per
load), and /api/alpaca/trades rebuilt the strategy lookup from a full `trades`
scan once per account (the same query and dict, 5x).
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


class _FakeBroker:
    """Counts get_positions() calls so we can prove the cache is doing its job."""
    _paper = True

    def __init__(self):
        self.calls = 0

    def get_positions(self, raise_on_error=False):
        self.calls += 1
        return [{"symbol": "GOOG", "qty": 10, "market_value": 1000.0}]


@pytest.fixture()
def fake_accounts(monkeypatch):
    brokers = {n: _FakeBroker() for n in ("1", "2", "3", "4", "5")}
    by_num = {n: {"broker": b, "tag": "alpaca" + ("" if n == "1" else n),
                  "label": "Acct " + n, "fills_fn": lambda: []}
              for n, b in brokers.items()}
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", by_num)
    for n in a._alpaca_positions_caches:                 # start every cache cold
        a._alpaca_positions_caches[n]["data"] = None
        a._alpaca_positions_caches[n]["ts"]   = 0.0
    a.app.config["TESTING"] = True
    yield brokers
    for n in a._alpaca_positions_caches:
        a._alpaca_positions_caches[n]["data"] = None
        a._alpaca_positions_caches[n]["ts"]   = 0.0


def test_positions_cached_for_every_account_not_just_account_1(fake_accounts):
    brokers = fake_accounts
    with a.app.test_client() as c:
        for acct in ("1", "2", "3", "4", "5"):           # first load — all cold
            assert c.get("/api/alpaca/positions?account=" + acct).status_code == 200
        for acct in ("1", "2", "3", "4", "5"):           # dashboard refresh
            c.get("/api/alpaca/positions?account=" + acct)
            c.get("/api/alpaca/positions?account=" + acct)
    # Each account hit Alpaca exactly ONCE despite three requests apiece.
    # Before the fix accounts 2-5 called out on every single request.
    for n, b in brokers.items():
        assert b.calls == 1, f"account {n} made {b.calls} Alpaca calls, expected 1"


def test_positions_caches_are_per_account(fake_accounts):
    """Account 2's cached payload must not be served for account 3."""
    brokers = fake_accounts
    brokers["3"].get_positions = lambda raise_on_error=False: [{"symbol": "TSLA", "qty": 5}]
    with a.app.test_client() as c:
        d2 = c.get("/api/alpaca/positions?account=2").get_json()
        d3 = c.get("/api/alpaca/positions?account=3").get_json()
    assert d2["positions"][0]["symbol"] == "GOOG"
    assert d3["positions"][0]["symbol"] == "TSLA"


def test_signal_lookup_is_cached_across_calls(monkeypatch):
    """The 5 per-account /api/alpaca/trades calls share one lookup build."""
    a._sig_lookup_cache["data"] = None
    a._sig_lookup_cache["ts"]   = 0.0
    calls = {"n": 0}
    real_get_db = a.get_db

    def _counting_get_db(*args, **kw):
        calls["n"] += 1
        return real_get_db(*args, **kw)

    monkeypatch.setattr(a, "get_db", _counting_get_db)
    first = a._signal_lookup()
    for _ in range(4):                       # the other four accounts
        assert a._signal_lookup() is first   # same object, no rebuild
    assert calls["n"] == 1, f"rebuilt the lookup {calls['n']}x, expected 1"
    a._sig_lookup_cache["data"] = None
    a._sig_lookup_cache["ts"]   = 0.0
