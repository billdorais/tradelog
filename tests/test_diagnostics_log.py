"""Diagnostics — in-memory ring buffer of recent WARNING+ log records, surfaced on
/diagnostics so bugs that only log a warning are visible/copyable in-app.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import logging

import app as a


def _clear_ring():
    with a._LOG_RING_LOCK:
        a._LOG_RING.clear()


def test_ring_captures_warning_and_error_not_info():
    _clear_ring()
    lg = logging.getLogger("app.test.diag")
    lg.info("this info line should NOT be captured")
    lg.warning("a warning about the thing")
    lg.error("an error happened")
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/diagnostics/errors?level=warning").get_json()
    msgs = [r["msg"] for r in d["records"]]
    assert any("a warning about the thing" in m for m in msgs)
    assert any("an error happened" in m for m in msgs)
    assert not any("should NOT be captured" in m for m in msgs)
    # newest first
    assert "an error happened" in d["records"][0]["msg"]
    assert d["counts"]["WARNING"] >= 1 and d["counts"]["ERROR"] >= 1


def test_level_filter_errors_only():
    _clear_ring()
    lg = logging.getLogger("app.test.diag")
    lg.warning("warn one")
    lg.error("err one")
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/diagnostics/errors?level=error").get_json()
    levels = {r["level"] for r in d["records"]}
    assert levels <= {"ERROR", "CRITICAL"}          # no WARNING rows
    assert any("err one" in r["msg"] for r in d["records"])


def test_exception_traceback_is_captured():
    _clear_ring()
    lg = logging.getLogger("app.test.diag")
    try:
        raise ValueError("boom-xyz")
    except ValueError:
        lg.exception("handling failed")
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/diagnostics/errors?level=error").get_json()
    blob = "\n".join(r["msg"] for r in d["records"])
    assert "handling failed" in blob and "boom-xyz" in blob   # traceback included


def test_window_filters_by_age_without_expiring_the_buffer(monkeypatch):
    """The ring needs no expiry — it's in-memory, capped, and wiped on redeploy.
    `window` is presentation only: it scopes the view so a week of low-volume
    warnings doesn't bury today's, while `buffered` still reports the true size."""
    import time as _t

    _clear_ring()
    now = _t.time()
    with a._LOG_RING_LOCK:
        a._LOG_RING.append({"ts": now - 60,          "level": "ERROR",
                            "logger": "app", "msg": "fresh problem"})
        a._LOG_RING.append({"ts": now - 3 * 86400,   "level": "ERROR",
                            "logger": "app", "msg": "three days stale"})
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        recent = c.get("/api/diagnostics/errors?window=24h").get_json()
        every  = c.get("/api/diagnostics/errors?window=all").get_json()

    msgs = [r["msg"] for r in recent["records"]]
    assert "fresh problem" in msgs
    assert "three days stale" not in msgs
    # Nothing was deleted — the buffer still holds both.
    assert recent["buffered"] == 2 and recent["total"] == 1
    assert len(every["records"]) == 2


def test_default_window_is_24h_not_calendar_today(monkeypatch):
    """An error from an hour ago must not vanish just because midnight passed."""
    import datetime as _dt
    import time as _t

    _clear_ring()
    et = a.ZoneInfo("America/New_York")
    now_et = _dt.datetime.now(et)
    # A record from 1h ago that is nonetheless "yesterday" whenever it is early ET.
    with a._LOG_RING_LOCK:
        a._LOG_RING.append({"ts": _t.time() - 3600, "level": "ERROR",
                            "logger": "app", "msg": "an hour ago"})
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/diagnostics/errors").get_json()      # no window param
    assert d["window"] == "24h"
    assert any("an hour ago" in r["msg"] for r in d["records"]), \
        "the default window dropped a one-hour-old error"
