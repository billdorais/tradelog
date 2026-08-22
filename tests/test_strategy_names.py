"""Strategy picker name list.

The picker used to derive its options from /api/trades, which is capped at 200
rows. At ~190 signals a day that is roughly ONE day of history, so any strategy
that had not fired yesterday silently disappeared from the picker — the "Indices"
preset would return only whichever index happened to have traded. The row cap is
correct for the table and wrong for a name list, so they are split.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import sqlite3

import pytest

import app as a


@pytest.fixture()
def names_db(monkeypatch, tmp_path):
    db = tmp_path / "names.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE trades (received_at TEXT, strategy TEXT)")
    conn.execute("CREATE TABLE routing_rules (name TEXT, enabled INT, nodes TEXT)")
    conn.commit(); conn.close()

    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c

    monkeypatch.setattr(a, "get_db", _fake_db)
    a.app.config["TESTING"] = True
    return db


def _seed_signals(db, rows):
    c = sqlite3.connect(db)
    c.executemany("INSERT INTO trades (received_at, strategy) VALUES (?,?)", rows)
    c.commit(); c.close()


def test_returns_names_beyond_the_200_row_table_page(names_db):
    """A strategy that fired long ago, far past the table's 200-row page, must
    still be selectable."""
    rows = [("2026-08-14 13:45:00", f"NOISE{i}_CAM_BREAKOUT_R3S3") for i in range(400)]
    rows.append(("2026-01-02 14:00:00", "SPY_CAM_BREAKOUT_R4S4_V02_5MIN"))
    rows.append(("2026-01-02 14:00:00", "QQQ_CAM_REVERSAL_R3S3_V02_5MIN"))
    _seed_signals(names_db, rows)
    with a.app.test_client() as c:
        d = c.get("/api/strategies").get_json()
    assert "SPY_CAM_BREAKOUT_R4S4_V02_5MIN" in d["strategies"]
    assert "QQQ_CAM_REVERSAL_R3S3_V02_5MIN" in d["strategies"]
    assert d["count"] == 402


def test_includes_wired_but_never_fired_strategies(names_db):
    """A strategy routed to an enabled rule should be pickable before its first
    signal — otherwise a freshly wired name is invisible."""
    _seed_signals(names_db, [("2026-08-14 13:45:00", "AAPL_CAM_BREAKOUT_R3S3")])
    c0 = sqlite3.connect(names_db)
    c0.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,?,?)",
               ("new pick", 1, json.dumps([{"type": "strategy",
                                            "value": "NVDA_CAM_BREAKOUT_R4S4"},
                                           {"type": "broker", "value": "alpaca-paper-4"}])))
    c0.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,?,?)",
               ("disabled", 0, json.dumps([{"type": "strategy", "value": "OFF_CAM_X"}])))
    c0.commit(); c0.close()
    with a.app.test_client() as c:
        d = c.get("/api/strategies").get_json()
    assert "NVDA_CAM_BREAKOUT_R4S4" in d["strategies"]
    assert "OFF_CAM_X" not in d["strategies"], "disabled rules should not contribute"


def test_days_scopes_to_recent_signals(names_db):
    # Seeded RELATIVE to today. A hardcoded "recent" date is a time bomb: this was
    # pinned to 2026-08-14 against ?days=7 and started failing on 2026-08-21, when
    # the fixture aged out of its own window.
    import datetime as _dt
    _recent = (_dt.datetime.now() - _dt.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    _seed_signals(names_db, [(_recent, "RECENT_CAM_BREAKOUT_R3S3"),
                             ("2026-01-02 14:00:00", "OLD_CAM_BREAKOUT_R3S3")])
    with a.app.test_client() as c:
        allt = c.get("/api/strategies").get_json()
        recent = c.get("/api/strategies?days=7").get_json()
    assert "OLD_CAM_BREAKOUT_R3S3" in allt["strategies"]
    assert "OLD_CAM_BREAKOUT_R3S3" not in recent["strategies"]
    assert "RECENT_CAM_BREAKOUT_R3S3" in recent["strategies"]


def test_sorted_deduped_and_blank_free(names_db):
    _seed_signals(names_db, [("2026-08-14 13:45:00", "B_CAM_X"),
                             ("2026-08-14 13:46:00", "A_CAM_X"),
                             ("2026-08-14 13:47:00", "A_CAM_X"),
                             ("2026-08-14 13:48:00", "")])
    with a.app.test_client() as c:
        d = c.get("/api/strategies").get_json()
    assert d["strategies"] == ["A_CAM_X", "B_CAM_X"]
