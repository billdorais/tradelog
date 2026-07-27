"""Dynamic (tiered) trailing-stop sweep endpoint — input validation.

The heavy path (bar replay via _simulate_exit) needs a live broker + bars, so
these cover the guards that run before that: required dates and grid bounds.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a


def _post(client, body):
    return client.post("/api/simulate/trail_tier_sweep", json=body)


def test_requires_date_range():
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        r = _post(c, {"account": "2"})
    assert r.status_code == 400
    assert "from_date" in (r.get_json() or {}).get("error", "")


def test_rejects_oversized_grid():
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        r = _post(c, {"from_date": "2026-07-01", "to_date": "2026-07-26",
                      "gain_min": 0.1, "gain_max": 5.0, "gain_step": 0.1,
                      "trail_min": 0.05, "trail_max": 2.0, "trail_step": 0.05})
    assert r.status_code == 400
    assert "too large" in (r.get_json() or {}).get("error", "").lower()
