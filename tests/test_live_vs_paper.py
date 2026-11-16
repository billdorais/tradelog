"""/api/live_vs_paper is retired with Crew Paper (2026-11-15).

It paired Crew Live's round-trips with Crew Paper's to isolate execution. Crew Paper
no longer exists — the crew wires straight to Crew Live — and slot 4's Alpaca account
now belongs to ORB Paper, so pairing against it would compare Crew Live with the ORB
engine. The endpoint answers 410 with the reason instead of a wrong comparison.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a


def test_the_endpoint_is_retired_with_a_reason():
    r = a.app.test_client().get("/api/live_vs_paper?from=2026-11-02&to=2026-11-13")
    assert r.status_code == 410
    d = r.get_json()
    assert d["retired"] is True and "Crew Paper was retired" in d["error"]


def test_no_pairing_code_is_left_behind():
    assert not any(n.startswith("_lvp_") for n in dir(a))
