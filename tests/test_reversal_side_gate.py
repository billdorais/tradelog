"""Per-account reversal-side gate.

Evidence (Book Breakdown, Jun-Jul '26): Refined's reversals bleed long-side, so
Refined (alpaca2) is gated to short-only reversals; Kairos (alpaca3) and others
have no policy. The gate is enforced by TARGET ACCOUNT, so gating Refined never
touches Kairos. Exits and breakouts always pass.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import shutil
import sqlite3

import pytest


def test_helper_gates_refined_long_only(engine_app):
    a = engine_app
    # Refined (alpaca2) = short-only reversals.
    assert a._reversal_gate_block("AMZN_CAM_REVERSAL_R4S4_V02_5MIN", "long",  "alpaca2") is True
    assert a._reversal_gate_block("AMZN_CAM_REVERSAL_R4S4_V02_5MIN", "short", "alpaca2") is False
    # Kairos (alpaca3) + Paper All (alpaca) = no policy.
    assert a._reversal_gate_block("AMZN_CAM_REVERSAL_R4S4_V02_5MIN", "long",  "alpaca3") is False
    assert a._reversal_gate_block("AMZN_CAM_REVERSAL_R4S4_V02_5MIN", "long",  "alpaca")  is False
    # Breakouts are never reversal-gated.
    assert a._reversal_gate_block("AMZN_CAM_BREAKOUT_R4S4_V02_5MIN", "long",  "alpaca2") is False


@pytest.fixture()
def engine_app():
    import app as a
    return a


@pytest.fixture()
def webhook_client(tmp_path):
    import app as a
    # Registry so alpaca-paper-2 -> alpaca2, alpaca-paper-3 -> alpaca3.
    a.ALPACA_ACCOUNTS = [
        {"tag": "alpaca2", "target_paper": "alpaca-paper-2", "target_live": "alpaca-live-2"},
        {"tag": "alpaca3", "target_paper": "alpaca-paper-3", "target_live": "alpaca-live-3"},
    ]
    db = tmp_path / "rev.db"
    shutil.copy("trades.db", db)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM routing_rules")
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                 ("AMZN rev -> Refined", json.dumps([
                     {"type": "strategy", "value": "AMZN_CAM_REVERSAL_R4S4_V02_5MIN"},
                     {"type": "broker",   "value": "alpaca-paper-2"}])))
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                 ("NVDA rev -> Kairos", json.dumps([
                     {"type": "strategy", "value": "NVDA_CAM_REVERSAL_R4S4_V02_5MIN"},
                     {"type": "broker",   "value": "alpaca-paper-3"}])))
    conn.commit()
    conn.close()

    saved_db = a.get_db

    def _fake_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    a.get_db = _fake_db
    yield a.app.test_client(), db
    a.get_db = saved_db


def _last_exec(db):
    c = sqlite3.connect(db)
    row = c.execute("SELECT exec_status, exec_detail FROM trades ORDER BY id DESC LIMIT 1").fetchone()
    c.close()
    return row


def test_refined_long_reversal_is_skipped(webhook_client):
    client, db = webhook_client
    client.post("/webhook?token=test-token",
                json={"strategy": "AMZN_CAM_REVERSAL_R4S4_V02_5MIN", "ticker": "AMZN", "action": "LONG"})
    status, detail = _last_exec(db)
    assert status == "skipped"
    assert "reversal" in (detail or "").lower()


def test_kairos_long_reversal_not_reversal_gated(webhook_client):
    client, db = webhook_client
    client.post("/webhook?token=test-token",
                json={"strategy": "NVDA_CAM_REVERSAL_R4S4_V02_5MIN", "ticker": "NVDA", "action": "LONG"})
    status, detail = _last_exec(db)
    # Kairos has no policy → the reversal-side gate must NOT skip it.
    assert not (status == "skipped" and "reversal-side" in (detail or "").lower())
