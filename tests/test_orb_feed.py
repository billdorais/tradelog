"""ORB backtests can pick their Alpaca data feed per run (SIP, every exchange, on the paid plan;
IEX, one exchange, the free plan's real-time feed), so a strategy can be checked on IEX before the
paid plan is cancelled. An IEX run is stored with its feed and sits beside the ticker's SIP run in
Compare tickers instead of replacing it."""
from __future__ import annotations

import json
import os

import pandas as pd
import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

from routes import orb_replay as R  # noqa: E402
from strategies import orb  # noqa: E402


def test_feed_name_defaults_to_the_app_feed_and_accepts_an_override(monkeypatch):
    monkeypatch.setenv("ALPACA_DATA_FEED", "sip")
    assert orb.feed_name() == "sip" and orb.feed_name("IEX") == "iex" and orb.feed_name("") == "sip"
    monkeypatch.delenv("ALPACA_DATA_FEED")
    assert orb.feed_name() == "iex"
    with pytest.raises(ValueError):
        orb.feed_name("delayed")
    from alpaca.data.enums import DataFeed
    monkeypatch.setenv("ALPACA_DATA_FEED", "sip")
    assert orb._alpaca_feed() == DataFeed.SIP and orb._alpaca_feed("iex") == DataFeed.IEX


def _body(**kw):
    return {"source": "alpaca", "symbol": "QQQ", "resolution": "1m", "from": "2026-01-02", "to": "2026-01-30",
            "params": {"trend_filter": False}, **kw}


def test_a_run_stores_its_feed(monkeypatch):
    monkeypatch.setenv("ALPACA_DATA_FEED", "sip")
    assert R._validate_run(_body())["feed"] == "sip"
    assert R._validate_run(_body(feed="iex"))["feed"] == "iex"
    with pytest.raises(ValueError):
        R._validate_run(_body(feed="nasdaq"))


def test_the_run_loads_bars_on_its_feed_and_says_so(monkeypatch):
    from tests.test_orb import _day
    seen, saved = [], {}

    def fake_load(sym, d0, d1, res, progress=None, feed=None):
        seen.append(feed)
        df = _day("2026-01-05", [("09:00", "09:30", 100.5, 100.5), ("09:30", "16:00", 101.0, 102.0)],
                  {"09:10": (100.5, 101, 100, 100.5)})
        df.index = df.index.tz_convert("UTC")
        return df
    monkeypatch.setattr(orb, "load_alpaca", fake_load)
    monkeypatch.setattr(R, "_save_run", lambda rid, **kw: saved.update(kw))
    import threading
    monkeypatch.setattr(R, "_lock", threading.Lock())   # _execute releases it when held; here it isn't
    R._alpaca_cache.update(key=None, df=None)
    cfg = R._validate_run(_body(feed="iex", **{"from": "2026-01-05", "to": "2026-01-05"}))
    R._execute("r1", cfg)
    assert seen == ["iex"] and saved.get("status") == "done", saved.get("progress")
    assert "(IEX feed)" in json.loads(saved["result"])["data"]["label"]
    cfg_sip = {**cfg, "feed": "sip"}
    R._execute("r2", cfg_sip)
    assert seen == ["iex", "sip"]                  # same range, other feed: not served from the cache


def test_an_iex_run_sits_beside_the_sip_run_in_compare_tickers():
    p = json.dumps
    base = {"source": "alpaca", "symbol": "QQQ", "from": "2021-01-01", "to": "2026-11-13",
            "params": orb.normalize_params({"trend_filter": False})}
    rows = [("old", "2026-11-13", p(base)),                                  # saved before feeds were recorded
            ("iex", "2026-11-28", p({**base, "feed": "iex"})),
            ("sip", "2026-11-28", p({**base, "feed": "sip", "from": "2025-01-01"}))]
    opts = {o["run_id"]: o for o in R.portfolio_options(rows)}
    assert set(opts) == {"old", "iex"}             # the longer SIP run still wins its slot
    assert opts["old"]["variant"] == "" and opts["iex"]["variant"] == "IEX feed"
    tgt = {**base, "feed": "iex", "params": orb.normalize_params({"trend_filter": False, "target_r": 2})}
    assert R._run_variant(tgt) == "2.0R target, IEX feed"


def test_the_orb_page_offers_the_feed():
    html = open("templates/orb.html", encoding="utf-8").read()
    assert 'id="feed"' in html and "feed: $('feed').value" in html and 'value="iex"' in html
