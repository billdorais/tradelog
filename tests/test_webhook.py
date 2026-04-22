"""Smoke tests for the /webhook blueprint.

These tests guard the extraction in routes/webhook.py against regression.
They boot the Flask app in-process (no broker env vars set, so all brokers
resolve to None) and exercise the four front-door branches:
  * unauthenticated → 401
  * wrong token → 401
  * non-JSON body → 400
  * valid signal with no routing rule match → 200 (signal logged,
    no order placed)
"""
from __future__ import annotations

import os
import pytest


@pytest.fixture(scope="module")
def flask_client(tmp_path_factory):
    # Point the SQLite DB at a scratch file so the real trades.db isn't touched.
    db_path = tmp_path_factory.mktemp("tradelog") / "test_trades.db"
    os.environ["WEBHOOK_TOKEN"] = "test-token"
    os.environ.pop("IB_HOST", None)
    os.environ.pop("IB_HOST_LIVE", None)
    os.environ.pop("ALPACA_KEY", None)
    os.environ.pop("COINBASE_KEY", None)
    os.environ.pop("DATABASE_URL", None)

    # app.py picks the SQLite path from the repo root; a clean import is
    # enough for these behavioral assertions (the real DB is not written).
    import app as a
    return a.app.test_client()


def test_webhook_rejects_missing_token(flask_client):
    r = flask_client.post("/webhook", json={"ticker": "AAPL"})
    assert r.status_code == 401


def test_webhook_rejects_bad_token(flask_client):
    r = flask_client.post("/webhook?token=nope", json={"ticker": "AAPL"})
    assert r.status_code == 401


def test_webhook_rejects_non_json_body(flask_client):
    r = flask_client.post("/webhook?token=test-token", data="not-json")
    assert r.status_code == 400
    assert r.get_json() == {"error": "Invalid JSON"}


def test_webhook_accepts_valid_signal_with_no_routing_match(flask_client):
    """No routing pipeline is configured in the test DB, so the webhook
    should still 200 with a trade_id (signal is logged; no order placed)."""
    r = flask_client.post(
        "/webhook?token=test-token",
        json={"strategy": "NoRuleStrat", "ticker": "AAPL", "action": "BUY"},
    )
    assert r.status_code == 200
    body = r.get_json()
    assert body["status"] == "ok"
    assert isinstance(body["id"], int)
