"""Per-account entry-block recording.

The signals table carries ONE signal-level exec_status, written only when every
target was dropped. So a gate that stops Kairos Refined while the farm still
trades left no trace outside the app log — which made "why has this book not
traded?" unanswerable, and biased the blocked-reason chart toward whole-signal
gates (day-type) over account-specific ones (hours, RVOL, side, halts).

blocked_targets records the drop per ACCOUNT. Writes are buffered: the recorders
fire from the webhook mid-transaction and from the engine tick loop just before
an order, so writing inline deadlocked SQLite and added order-path latency.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import sqlite3

import pytest

import app as a


@pytest.fixture()
def blocks_db(monkeypatch, tmp_path):
    db = tmp_path / "blocks.db"
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE blocked_targets (
        ts TEXT NOT NULL, account TEXT NOT NULL, ticker TEXT, strategy TEXT,
        side TEXT, gate TEXT NOT NULL, reason TEXT, source TEXT)""")
    conn.commit(); conn.close()

    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c

    monkeypatch.setattr(a, "get_db", _fake_db)
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS",
                        [{"tag": "alpaca3", "num": "3", "label": "Kairos Refined"},
                         {"tag": "alpaca5", "num": "5", "label": "Kairos Farm"}])
    with a._blocked_q_lock:
        a._blocked_queue.clear()
    a._blocked_seen.clear()
    a._blocked_seen_day = None
    a._blocked_prune_day = None
    a.app.config["TESTING"] = True
    yield db
    with a._blocked_q_lock:
        a._blocked_queue.clear()
    a._blocked_seen.clear()


def _rows(db):
    c = sqlite3.connect(db)
    out = c.execute("SELECT account, ticker, gate, reason, source FROM blocked_targets").fetchall()
    c.close()
    return out


def test_recording_is_buffered_not_written_inline(blocks_db):
    """Nothing may touch the DB on the order path — inline writes deadlocked
    SQLite while the webhook still held its transaction open."""
    a._record_block("alpaca3", "AAPL", "AAPL_CAM_BREAKOUT_R3S3", "long",
                    "hours", "outside the account's trading window")
    assert _rows(blocks_db) == [], "wrote to the DB on the caller's thread"
    assert len(a._blocked_queue) == 1
    a._flush_blocked_targets()
    assert len(_rows(blocks_db)) == 1
    assert len(a._blocked_queue) == 0, "queue not drained"


def test_attributes_the_block_to_one_account_only(blocks_db):
    """The farm kept trading; only the refined book was gated."""
    a._record_block("alpaca3", "GOOG", "GOOG_CAM_BREAKOUT_R4S4", "long",
                    "rvol", "below the 1.50x RVOL floor (RVOL 1.10x)")
    a._flush_blocked_targets()
    rows = _rows(blocks_db)
    assert len(rows) == 1 and rows[0][0] == "alpaca3" and rows[0][2] == "rvol"


def test_engine_dedupes_per_day_but_webhook_does_not(blocks_db):
    """The engine re-evaluates the same setups every tick; without dedupe a single
    all-day block would write thousands of identical rows. The webhook fires once
    per signal and must NOT be deduped."""
    for _ in range(50):
        a._record_block("alpaca3", "TSLA", "TSLA_CAM_BREAKOUT_R3S3", "long",
                        "day-type", "blocked on Neutral day",
                        source="engine", once_per_day=True)
    a._flush_blocked_targets()
    assert len(_rows(blocks_db)) == 1, "engine tick loop spammed the table"

    for _ in range(3):
        a._record_block("alpaca3", "TSLA", "TSLA_CAM_BREAKOUT_R3S3", "long",
                        "day-type", "blocked on Neutral day")     # webhook path
    a._flush_blocked_targets()
    assert len(_rows(blocks_db)) == 4, "webhook signals were wrongly deduped"


def test_recorder_never_raises_when_the_table_is_missing(monkeypatch, tmp_path):
    """A logging failure must never break an order."""
    empty = tmp_path / "no_table.db"
    sqlite3.connect(empty).close()
    monkeypatch.setattr(a, "get_db",
                        lambda: sqlite3.connect(empty))
    with a._blocked_q_lock:
        a._blocked_queue.clear()
    a._record_block("alpaca3", "X", "S", "long", "hours", "why")   # queues fine
    a._flush_blocked_targets()                                     # insert fails, swallowed
    assert True


def test_endpoint_groups_by_account_and_gate(blocks_db):
    for acct, gate in [("alpaca3", "hours"), ("alpaca3", "hours"),
                       ("alpaca3", "rvol"), ("alpaca5", "day-type")]:
        a._record_block(acct, "AAPL", "AAPL_CAM_BREAKOUT_R3S3", "long", gate, "r")
    with a.app.test_client() as c:
        d = c.get("/api/signals/blocked_by_account?days=7").get_json()
    assert d["total"] == 4
    top = d["by_account"][0]
    assert top["account"] == "alpaca3" and top["label"] == "Kairos Refined"
    assert top["total"] == 3
    assert dict((g["gate"], g["count"]) for g in top["gates"]) == {"hours": 2, "rvol": 1}


def test_endpoint_filters_to_one_account(blocks_db):
    a._record_block("alpaca3", "AAPL", "S", "long", "hours", "r")
    a._record_block("alpaca5", "AAPL", "S", "long", "day-type", "r")
    with a.app.test_client() as c:
        d = c.get("/api/signals/blocked_by_account?days=7&account=alpaca5").get_json()
    assert d["total"] == 1 and d["recent"][0]["account"] == "alpaca5"


def test_endpoint_returns_recent_times_in_et(blocks_db):
    a._record_block("alpaca3", "AAPL", "S", "long", "hours", "r")
    with a.app.test_client() as c:
        d = c.get("/api/signals/blocked_by_account?days=7").get_json()
    # Stored UTC; surfaced ET. Same trap the signal-level panel fell into.
    assert d["recent"][0]["ts"] != d["recent"][0].get("ts_utc", object())
    assert len(d["recent"][0]["ts"]) == 19
