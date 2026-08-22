"""A failed fills fetch must not read as "this account has no trades".

get_fills() swallowed every exception and returned [], and the cache then stored
that empty list WITH a fresh timestamp — so one transient Alpaca error became two
full minutes of every book looking flat, on every page, with nothing on screen to
say otherwise. That is what made an outage indistinguishable from a quiet market.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import time

import pytest

import app as a


class _Boom:
    def get_fills(self, days=90, raise_on_error=False):
        if raise_on_error:
            raise RuntimeError("429 rate limited")
        return []


class _Good:
    def __init__(self, fills): self._f = fills
    def get_fills(self, days=90, raise_on_error=False): return list(self._f)


@pytest.fixture()
def cache(monkeypatch):
    c = {"data": [], "ts": 0.0}
    monkeypatch.setattr(a, "_alpaca_caches", {"9": c})
    import threading
    monkeypatch.setattr(a, "_alpaca_cache_locks", {"9": threading.Lock()})
    return c


def _reg(monkeypatch, broker):
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {"9": {"num": "9", "broker": broker}})


def test_a_failed_fetch_is_not_cached_as_success(cache, monkeypatch):
    """The poisoning bug: a failure used to write [] AND a fresh timestamp, so the
    next 120 seconds served 'no trades' without retrying."""
    _reg(monkeypatch, _Boom())
    assert a._get_cached_fills_n("9") == []
    assert cache["ts"] == 0.0, "a failed fetch refreshed the cache timestamp"
    assert cache["error"] and "429" in cache["error"]


def test_previous_good_data_survives_a_failure(cache, monkeypatch):
    """Serving the last known fills beats serving an empty book."""
    _reg(monkeypatch, _Good([{"symbol": "AAPL"}]))
    assert len(a._get_cached_fills_n("9")) == 1
    cache["ts"] = 0.0                       # force the next call to refetch
    _reg(monkeypatch, _Boom())
    assert len(a._get_cached_fills_n("9")) == 1, "dropped good data on a transient error"
    assert a._fills_error("9")


def test_backoff_stops_hammering_a_failing_api(cache, monkeypatch):
    calls = {"n": 0}

    class _Counting:
        def get_fills(self, days=90, raise_on_error=False):
            calls["n"] += 1
            raise RuntimeError("boom")

    _reg(monkeypatch, _Counting())
    a._get_cached_fills_n("9")
    a._get_cached_fills_n("9")
    a._get_cached_fills_n("9")
    assert calls["n"] == 1, f"retried {calls['n']}x inside the backoff window"


def test_recovery_clears_the_error(cache, monkeypatch):
    _reg(monkeypatch, _Boom())
    a._get_cached_fills_n("9")
    assert a._fills_error("9")
    cache["error_ts"] = time.time() - (a.FILLS_ERROR_BACKOFF + 1)
    _reg(monkeypatch, _Good([{"symbol": "MSFT"}]))
    assert len(a._get_cached_fills_n("9")) == 1
    assert a._fills_error("9") is None, "stale error left set after a good fetch"


def test_raise_on_error_propagates(cache, monkeypatch):
    _reg(monkeypatch, _Boom())
    with pytest.raises(Exception):
        a._get_cached_fills_n("9", raise_on_error=True)


def test_prewarm_is_staggered():
    """Six accounts x a paginated 90-day fetch, fired at once across two workers,
    is a plausible route into the rate limit that started all this."""
    import inspect
    src = inspect.getsource(a._prewarm_fills) if hasattr(a, "_prewarm_fills") else ""
    if not src:
        import re
        src = re.search(r"def _prewarm_fills.*?(?=\n\S)", open("app.py", encoding="utf-8").read(), re.S).group(0)
    assert "time.sleep(4)" in src and "enumerate(" in src, "prewarm still fires all at once"


def test_ui_distinguishes_unavailable_from_empty():
    html = open("templates/index.html", encoding="utf-8").read()
    assert "_fillsUnavailable" in html
    assert "Could not load fills" in html
