"""The daily P&L cards must not wait on a 90-day fill pagination.

_get_today_fills_n is an alias for the shared 90-day cache, so "today's P&L" was
computed by fetching the whole fill history and filtering to today. On a warm
cache that is free. On a stale one the request pays the full paginated fetch, and
the dashboard asks for every account at once.

The cache already had a stale-while-revalidate mode for exactly this; the card
path simply was not using it. The important part is WHO may use it: a glance card
re-polled every 30s can read a few seconds stale, and the risk monitor -- which
decides whether to halt an account -- cannot.
"""
from __future__ import annotations

import os
import threading
import time

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


class _SlowBroker:
    """Stands in for a paginated 90-day fetch."""
    def __init__(self, delay=2.0):
        self.delay, self.calls = delay, 0
    def get_fills(self, raise_on_error=False):
        self.calls += 1
        time.sleep(self.delay)
        return [{"pnl": 1.0, "strategy": "S", "ticker": "T"}]


@pytest.fixture
def stale_cache(monkeypatch):
    """A cache holding data whose timestamp is long past the TTL."""
    broker = _SlowBroker()
    cached = [{"pnl": 5.0, "strategy": "OLD", "ticker": "T"}]
    cache = {"data": cached, "ts": time.time() - (a.ALPACA_CACHE_TTL + 60),
             "error": None, "error_ts": 0}
    monkeypatch.setattr(a, "_alpaca_caches", {"9": cache})
    monkeypatch.setattr(a, "_alpaca_cache_locks", {"9": threading.Lock()})
    monkeypatch.setitem(a.ACCOUNTS_BY_NUM, "9", {"broker": broker, "num": "9"})
    return broker, cache, cached


def test_swr_returns_the_stale_fills_without_waiting(stale_cache):
    """The card path. Must come back in well under the fetch time."""
    broker, _cache, cached = stale_cache
    t0 = time.time()
    out = a._get_today_fills_n("9", swr=True)
    assert time.time() - t0 < 0.5, "swr blocked on the pagination"
    assert out == cached


def test_the_default_still_blocks_for_fresh_data(stale_cache):
    """The risk monitor's path. A halt decision on stale fills is the failure this
    protects against, so the default must NOT have become swr."""
    broker, _cache, _cached = stale_cache
    t0 = time.time()
    out = a._get_today_fills_n("9")
    assert time.time() - t0 >= 1.5, "the default went stale-while-revalidate"
    assert out[0]["strategy"] == "S", "did not get the fresh fills"


def test_the_risk_monitor_does_not_pass_swr():
    """Source-level, because the loop is a thread that cannot easily be driven
    here. If this ever needs changing, the halt path has been made stale."""
    import inspect
    src = inspect.getsource(a._risk_monitor_loop)
    assert "_get_today_fills_n(_n)" in src, "risk monitor stopped using the blocking read"
    assert "swr=True" not in src, "risk monitor would decide a halt on stale fills"


def test_the_glance_endpoint_does_pass_swr():
    import inspect
    src = inspect.getsource(a.api_alpaca_account)
    assert "swr=True" in src, "the card is back on the blocking path"


# ── the warm loop ───────────────────────────────────────────────────────────

def test_the_warm_cycle_finishes_inside_the_ttl_at_any_account_count():
    """The idle used to be a flat 90s, hand-tuned for six accounts. A seventh put
    the cycle at ~135s against a 120s TTL, so every cycle opened a window where a
    request paid the full pagination -- which is what made the cards slow."""
    TTL = a.ALPACA_CACHE_TTL
    for n in (4, 6, 7, 10, 16):
        sweep = (n - 1) * 4 + n * 3          # stagger + ~3s per paginated fetch
        idle  = max(10, TTL * 0.7 - sweep)
        assert sweep + idle < TTL, f"{n} accounts: cycle {sweep + idle:.0f}s >= TTL {TTL}s"


def test_the_warm_idle_is_derived_not_hardcoded():
    import inspect
    src = inspect.getsource(a)
    i = src.index("def _prewarm_fills")
    body = src[i:i + 2600]
    assert "ALPACA_CACHE_TTL" in body, "the warm idle does not reference the TTL"
    assert "time.sleep(90)" not in body, "the flat 90s idle is back"
