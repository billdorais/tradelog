"""Per-account reversal policy: one side only, "off", or free.

Evidence (Book Breakdown): Kairos (alpaca3) and Crew reversals are profitable
both sides, so they run free. TV Refined (alpaca2) was short-only on a short-side
edge, but 2026-07-01..17 killed that premise — 22 round-trips, ONE winner (4.5%),
-$394, both sides dead — so it takes no reversal entries at all ("off"). TV Farm
keeps trading reversals, so the evidence for a comeback keeps accruing.

The gate is enforced by TARGET ACCOUNT, so gating TV Refined never touches
Kairos. Exits and breakouts always pass.

Mechanism tests below patch the policy map directly, so they keep testing the
gate rather than today's config; the current per-account policy is pinned
separately in test_current_policy_config.
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

REV = "AMZN_CAM_REVERSAL_R4S4_V02_5MIN"
BRK = "AMZN_CAM_BREAKOUT_R4S4_V02_5MIN"


@pytest.fixture()
def engine_app():
    import app as a
    return a


@pytest.fixture()
def policy(engine_app):
    """Patch the resolved policy map so mechanism tests don't depend on config."""
    a = engine_app
    saved = a._REVERSAL_SIDE_BY_TAG.copy()

    def _set(**tags):
        a._REVERSAL_SIDE_BY_TAG.clear()
        a._REVERSAL_SIDE_BY_TAG.update(tags)
        return a

    yield _set
    a._REVERSAL_SIDE_BY_TAG.clear()
    a._REVERSAL_SIDE_BY_TAG.update(saved)


def test_one_side_policy_blocks_only_the_other_side(policy):
    a = policy(x="short")
    assert a._reversal_gate_block(REV, "long",  "x") is True
    assert a._reversal_gate_block(REV, "short", "x") is False


def test_off_policy_blocks_both_sides(policy):
    a = policy(x="off")
    assert a._reversal_gate_block(REV, "long",  "x") is True
    assert a._reversal_gate_block(REV, "short", "x") is True
    # An unparseable side can't make a reversal allowed when the book takes none.
    assert a._reversal_gate_block(REV, "",      "x") is True
    assert a._reversal_gate_block(REV, None,    "x") is True


def test_off_policy_still_lets_breakouts_through(policy):
    """"off" is a reversal policy — it must not touch the breakout book, which is
    where TV Refined's edge actually lives."""
    a = policy(x="off")
    assert a._reversal_gate_block(BRK, "long",  "x") is False
    assert a._reversal_gate_block(BRK, "short", "x") is False


def test_no_policy_passes_everything(policy):
    a = policy(x=None)
    assert a._reversal_gate_block(REV, "long",  "x") is False
    assert a._reversal_gate_block(REV, "short", "x") is False
    # An account absent from the map entirely also passes (fails open).
    assert a._reversal_gate_block(REV, "long",  "unknown-tag") is False


def test_unknown_side_passes_a_one_side_policy(policy):
    """Can't prove a violation without a side — only "off" blocks regardless."""
    a = policy(x="short")
    assert a._reversal_gate_block(REV, "", "x") is False


def test_current_policy_config(engine_app):
    """Today's per-account policy. Update deliberately when a book's policy changes."""
    a = engine_app
    assert a._REVERSAL_SIDE_BY_TAG.get("alpaca2") == "off"    # TV Refined: no reversals
    assert a._REVERSAL_SIDE_BY_TAG.get("alpaca3") is None     # Kairos Refined: free
    assert a._REVERSAL_SIDE_BY_TAG.get("alpaca4") is None     # Crew Paper: free
    assert a._REVERSAL_SIDE_BY_TAG.get("alpaca")  is None     # TV Farm: free (keeps the data)
    assert a._REVERSAL_SIDE_BY_TAG.get("alpaca5") is None     # Kairos Farm: free


def test_env_override_accepts_off(engine_app, monkeypatch):
    a = engine_app
    monkeypatch.setenv("REVERSAL_SIDE_ALPACA9", "off")
    assert a._reversal_side_cfg("alpaca9", {}) == "off"
    monkeypatch.setenv("REVERSAL_SIDE_ALPACA9", "garbage")
    assert a._reversal_side_cfg("alpaca9", {}) is None        # unknown value fails open


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
    saved_hours = a._account_hours_ok

    def _fake_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    a.get_db = _fake_db
    # Neutralize the trading-hours gate so it can't pre-empt the reversal gate
    # (Refined has a 09:30-11:00 ET window; otherwise the test is wall-clock dependent).
    a._account_hours_ok = lambda tag: True
    yield a.app.test_client(), db
    a.get_db = saved_db
    a._account_hours_ok = saved_hours


def _last_exec(db):
    c = sqlite3.connect(db)
    row = c.execute("SELECT exec_status, exec_detail FROM trades ORDER BY id DESC LIMIT 1").fetchone()
    c.close()
    return row


@pytest.mark.parametrize("action", ["LONG", "SHORT"])
def test_refined_reversal_is_skipped_both_sides(webhook_client, action):
    """With "off", the short side is skipped too — it used to be the allowed one."""
    client, db = webhook_client
    client.post("/webhook?token=test-token",
                json={"strategy": REV, "ticker": "AMZN", "action": action})
    status, detail = _last_exec(db)
    assert status == "skipped"
    assert "no reversal entries" in (detail or "").lower()


def test_kairos_long_reversal_not_reversal_gated(webhook_client):
    client, db = webhook_client
    client.post("/webhook?token=test-token",
                json={"strategy": "NVDA_CAM_REVERSAL_R4S4_V02_5MIN", "ticker": "NVDA", "action": "LONG"})
    status, detail = _last_exec(db)
    # Kairos has no policy → the reversal gate must NOT skip it.
    assert not (status == "skipped" and "reversal gate" in (detail or "").lower())
