"""Dashboard load speed: incremental fills refresh, gzip, and 304s.

The dashboard stalled for up to a minute because every account's fills cache
re-walked 90 days of Alpaca orders on each 2-minute expiry, in each gunicorn
worker, while readers waited; and it moved ~10 MB of uncompressed JSON on every
30-second poll.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as dt
import gzip
import json
import time

import pytest

import app as a

NOW = dt.datetime(2026, 10, 22, 18, 0, tzinfo=dt.timezone.utc)


def _f(eid, when, **kw):
    return {"exec_id": eid, "time": when, "symbol": "AAA", "side": "BOT", "shares": 1.0,
            "price": 10.0, **kw}


# ── merge ─────────────────────────────────────────────────────────────────────

def test_merge_replaces_by_order_id_keeps_annotation_and_prunes_old():
    old = [_f("1", "2026-10-21T15:00:00Z", strategy="AAA_X", shares=1.0),
           _f("2", "2026-10-20T15:00:00Z", strategy="AAA_Y"),
           _f("ancient", "2026-06-01T15:00:00Z")]
    recent = [_f("1", "2026-10-21T15:00:00Z", shares=3.0),       # partial completed later
              _f("3", "2026-10-22T14:00:00Z")]                   # new fill
    out = a._merge_fills(old, recent, 90, NOW)
    by = {f["exec_id"]: f for f in out}
    assert set(by) == {"1", "2", "3"}                            # ancient pruned
    assert by["1"]["shares"] == 3.0 and by["1"]["strategy"] == "AAA_X"
    assert by["2"]["strategy"] == "AAA_Y"
    assert [f["exec_id"] for f in out] == ["3", "1", "2"]        # newest first


# ── refresh window ────────────────────────────────────────────────────────────

class _Broker:
    def __init__(self, oldest=None, fills=None, fail=False):
        self.oldest, self.fills, self.fail, self.calls = oldest, fills or [], fail, []

    def oldest_open_order_ts(self):
        return self.oldest

    def get_fills(self, days=90, raise_on_error=False):
        self.calls.append(days)
        if self.fail:
            raise RuntimeError("alpaca down")
        return list(self.fills)


def test_empty_cache_does_a_full_walk():
    b, cache = _Broker(fills=[_f("1", "2026-10-22T14:00:00Z")]), {"data": [], "ts": 0}
    out = a._fetch_fills_for_cache(b, cache)
    assert b.calls == [90] and len(out) == 1 and cache["full_ts"] > 0


def test_warm_cache_fetches_about_two_days():
    b = _Broker(fills=[_f("2", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))])
    cache = {"data": [_f("1", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 3600)))],
             "ts": 0, "full_ts": time.time()}
    out = a._fetch_fills_for_cache(b, cache)
    assert len(b.calls) == 1 and 2.0 <= b.calls[0] < 2.1
    assert {f["exec_id"] for f in out} == {"1", "2"}


def test_an_order_open_at_the_last_refresh_extends_the_window():
    """A GTC exit stop submitted 5 days ago (overnight position) that filled since
    the last refresh must still be re-listed."""
    five_days = time.time() - 5 * 86400
    b = _Broker(oldest=None)                       # it has filled: nothing open now
    cache = {"data": [_f("1", "2026-10-22T14:00:00Z")], "ts": 0,
             "full_ts": time.time(), "open_since": five_days}
    a._fetch_fills_for_cache(b, cache)
    assert b.calls[0] >= 5.0
    assert cache["open_since"] is None             # nothing open any more


def test_an_order_open_now_extends_the_window():
    b = _Broker(oldest=time.time() - 4 * 86400)
    cache = {"data": [_f("1", "2026-10-22T14:00:00Z")], "ts": 0, "full_ts": time.time()}
    a._fetch_fills_for_cache(b, cache)
    assert b.calls[0] >= 4.0


def test_full_walk_every_six_hours():
    b = _Broker()
    cache = {"data": [_f("1", "2026-10-22T14:00:00Z")], "ts": 0,
             "full_ts": time.time() - a.FILLS_FULL_REFRESH_SECS - 1}
    a._fetch_fills_for_cache(b, cache)
    assert b.calls == [90]


def test_a_failed_fetch_keeps_the_previous_open_order_time():
    prev = time.time() - 5 * 86400
    b = _Broker(oldest=None, fail=True)
    cache = {"data": [_f("1", "2026-10-22T14:00:00Z")], "ts": 0,
             "full_ts": time.time(), "open_since": prev}
    with pytest.raises(RuntimeError):
        a._fetch_fills_for_cache(b, cache)
    assert cache["open_since"] == prev


def test_a_broker_without_open_order_support_falls_back_to_full():
    class _Old:
        calls = []
        def get_fills(self, days=90, raise_on_error=False):
            self.calls.append(days); return []
    b = _Old()
    a._fetch_fills_for_cache(b, {"data": [_f("1", "2026-10-22T14:00:00Z")], "ts": 0,
                                 "full_ts": time.time()})
    assert b.calls == [90]


# ── gzip + ETag ───────────────────────────────────────────────────────────────

@pytest.fixture()
def fills_cache(monkeypatch):
    big = [_f(str(i), "2026-10-22T14:00:00Z", strategy="AAA_CAM_BREAKOUT_R4S4_V02_5MIN")
           for i in range(800)]
    cache = a._alpaca_caches["1"]
    monkeypatch.setitem(cache, "data", big)
    monkeypatch.setitem(cache, "ts", time.time())
    monkeypatch.setattr(a, "_alpaca_account_ctx", lambda acct: (object(), "alpaca", "TV Farm", lambda: big))
    return big


def test_large_json_is_gzipped_when_the_browser_accepts_it(fills_cache):
    c = a.app.test_client()
    r = c.get("/api/alpaca/trades", headers={"Accept-Encoding": "gzip, deflate"})
    assert r.headers.get("Content-Encoding") == "gzip"
    assert json.loads(gzip.decompress(r.data)) == fills_cache
    plain = c.get("/api/alpaca/trades")
    assert plain.headers.get("Content-Encoding") is None and json.loads(plain.data) == fills_cache


def test_unchanged_fills_answer_304(fills_cache):
    c = a.app.test_client()
    first = c.get("/api/alpaca/trades")
    tag = first.headers["ETag"]
    assert first.headers["Cache-Control"] == "no-cache"
    again = c.get("/api/alpaca/trades", headers={"If-None-Match": tag})
    assert again.status_code == 304 and again.data == b""
    a._alpaca_caches["1"]["ts"] += 1                       # cache refreshed
    fresh = c.get("/api/alpaca/trades", headers={"If-None-Match": tag})
    assert fresh.status_code == 200 and fresh.headers["ETag"] != tag


def test_small_responses_are_not_compressed():
    r = a.app.test_client().get("/api/broker/eod-close", headers={"Accept-Encoding": "gzip"})
    assert r.headers.get("Content-Encoding") is None


def test_range_change_reuses_loaded_fills():
    html = open("templates/index.html", encoding="utf-8").read()
    i = html.index("async function renderAlpacaStats")
    block = html[i:i + 4000]
    assert "'7': allAlpaca7Execs" in block and "execs = _loaded[String(account)]" in block
