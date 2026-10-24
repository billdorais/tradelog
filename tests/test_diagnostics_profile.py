"""Read-only diagnostics for "the server is pegged — by what?"."""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import threading
import time

import app as a


def test_request_accounting_counts_routes_and_4xx():
    c = a.app.test_client()
    c.get("/api/broker/eod-close")
    c.get("/api/broker/eod-close")
    c.post("/webhook", json={})                      # no token -> 401
    d = c.get("/api/diagnostics/requests").get_json()
    by = {r["route"]: r for r in d["routes"]}
    assert by["/api/broker/eod-close"]["count"] >= 2
    assert by["/webhook"]["n4xx"] >= 1
    assert d["clients"] and d["requests"] >= 3


def test_profile_samples_a_busy_thread():
    stop = threading.Event()

    def spin():
        while not stop.is_set():
            sum(i * i for i in range(2000))

    t = threading.Thread(target=spin, name="spinner-under-test", daemon=True)
    t.start()
    try:
        d = a.app.test_client().get("/api/diagnostics/profile?secs=1").get_json()
    finally:
        stop.set(); t.join(2)
    spin_rows = [r for r in d["threads"] if r["thread"] == "spinner-under-test"]
    assert d["samples"] > 10 and spin_rows
    assert any("spin" in f["fn"] for f in spin_rows[0]["top_functions"])
