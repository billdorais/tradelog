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
