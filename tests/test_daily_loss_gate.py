"""Per-account daily-loss guard.

MAX_DAILY_LOSS is enforced per account (Refined/Kairos/Crew; Paper All exempt):
when a book hits its own daily limit it halts + liquidates independently, and its
new entries are dropped by target account in the webhook. Exits always pass.
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


@pytest.fixture()
def webhook_client(tmp_path):
    import app as a
    a.ALPACA_ACCOUNTS = [
        {"tag": "alpaca2", "target_paper": "alpaca-paper-2", "target_live": "alpaca-live-2"},
        {"tag": "alpaca3", "target_paper": "alpaca-paper-3", "target_live": "alpaca-live-3"},
    ]
    db = tmp_path / "dl.db"
    shutil.copy("trades.db", db)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM routing_rules")
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                 ("AMZN brk -> Refined", json.dumps([
                     {"type": "strategy", "value": "AMZN_CAM_BREAKOUT_R4S4_V02_5MIN"},
                     {"type": "broker",   "value": "alpaca-paper-2"}])))
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                 ("NVDA brk -> Kairos", json.dumps([
                     {"type": "strategy", "value": "NVDA_CAM_BREAKOUT_R4S4_V02_5MIN"},
                     {"type": "broker",   "value": "alpaca-paper-3"}])))
    conn.commit()
    conn.close()

    saved_db, saved_hours, saved_limit = a.get_db, a._account_hours_ok, a.MAX_DAILY_LOSS

    def _fake_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    a.get_db = _fake_db
    a._account_hours_ok = lambda tag: True
    a.MAX_DAILY_LOSS = -125.0
    a._daily_loss_halted.clear()
    a._daily_loss_halted["alpaca2"] = True   # Refined hit its limit; Kairos did not
    yield a.app.test_client(), db
    a.get_db, a._account_hours_ok, a.MAX_DAILY_LOSS = saved_db, saved_hours, saved_limit
    a._daily_loss_halted.clear()


def _last_exec(db):
    c = sqlite3.connect(db)
    row = c.execute("SELECT exec_status, exec_detail FROM trades ORDER BY id DESC LIMIT 1").fetchone()
    c.close()
    return row


def test_halted_account_entry_blocked(webhook_client):
    client, db = webhook_client
    client.post("/webhook?token=test-token",
                json={"strategy": "AMZN_CAM_BREAKOUT_R4S4_V02_5MIN", "ticker": "AMZN", "action": "LONG"})
    status, detail = _last_exec(db)
    assert status == "blocked"
    assert "daily loss" in (detail or "").lower()


def test_other_account_keeps_trading(webhook_client):
    client, db = webhook_client
    client.post("/webhook?token=test-token",
                json={"strategy": "NVDA_CAM_BREAKOUT_R4S4_V02_5MIN", "ticker": "NVDA", "action": "LONG"})
    status, detail = _last_exec(db)
    # Kairos is not halted → the daily-loss gate must NOT block it.
    assert not (status == "blocked" and "daily loss" in (detail or "").lower())


def test_halted_account_exit_passes(webhook_client):
    client, db = webhook_client
    client.post("/webhook?token=test-token",
                json={"strategy": "AMZN_CAM_BREAKOUT_R4S4_V02_5MIN", "ticker": "AMZN", "action": "EXIT_LONG"})
    status, detail = _last_exec(db)
    # Exit must never be blocked by the daily-loss gate.
    assert not (status == "blocked" and "daily loss" in (detail or "").lower())
