"""The weekly journal freezes a recap snapshot per entry.

A journal is a RECORD. Recomputing recap numbers when an old entry is opened lets
it rewrite its own history — fills get re-paired, gates change, windows move — so
the notes you wrote end up sitting next to figures that no longer say the same
thing. The payload is therefore snapshotted at generation and stored on the row.
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
def jdb(monkeypatch, tmp_path):
    db = tmp_path / "j.db"
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE journal_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT, week TEXT, account TEXT,
        generated_at TEXT, trade_stats TEXT, market_data TEXT, ai_summary TEXT,
        user_notes TEXT, tags TEXT, sweep_results TEXT, recap TEXT,
        UNIQUE(week, account))""")
    conn.commit(); conn.close()

    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c

    monkeypatch.setattr(a, "get_db", _fake_db)
    a.app.config["TESTING"] = True
    return db


def _rt(strategy, pnl, date="2026-08-17"):
    return {"strategy": strategy, "ticker": strategy[:4], "pnl": pnl, "qty": 10,
            "entry_price": 100.0, "exit_price": 100.0 + pnl / 10, "side": "LONG",
            "date": date, "entry_time": f"{date}T13:45:00Z",
            "exit_time": f"{date}T14:05:00Z", "exit_reason": "Trail"}


def test_recap_builds_for_any_account_not_just_crew(monkeypatch):
    """It was hardcoded to acct4; the journal needs one per book."""
    class _B: _paper = True
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "2": {"tag": "alpaca2", "num": "2", "label": "TV Refined",
              "broker": _B(), "fills_fn": lambda: ["x"]}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda *A, **K: {"closed_clean": [_rt("AAPL_CAM_BREAKOUT_R3S3", 40)]})
    out = a._build_recap(account="2", frm="2026-08-17", to="2026-08-21")
    assert not out.get("error")
    assert out["account"] == "2" and out["label"] == "TV Refined"
    assert out["book"]["trades"] == 1


def test_crew_scorecard_only_appears_on_the_crew_book(monkeypatch):
    """The scorecard grades the crew's picks against acct4. On another book it
    would be an unrelated table presented as if it applied."""
    class _B: _paper = True
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "2": {"tag": "alpaca2", "num": "2", "label": "TV Refined",
              "broker": _B(), "fills_fn": lambda: ["x"]},
        "4": {"tag": "alpaca4", "num": "4", "label": "Crew Paper",
              "broker": _B(), "fills_fn": lambda: ["x"]}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda *A, **K: {"closed_clean": [_rt("AAPL_CAM_BREAKOUT_R3S3", 10)]})
    tv = a._build_recap(account="2", frm="2026-08-17", to="2026-08-21")
    assert tv["scorecard"] == {}, "crew scorecard leaked onto a non-crew book"


def test_unconfigured_account_returns_an_error_dict_not_a_response(monkeypatch):
    """_build_recap is called from the journal, not just a route — it must return a
    plain dict so a failure can be logged rather than raising mid-generation."""
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {})
    out = a._build_recap(account="9", frm="2026-08-17", to="2026-08-21")
    assert out["error"] and "9" in out["error"]
    assert not hasattr(out, "status_code")


def test_snapshot_is_stored_and_decoded_on_read(jdb):
    payload = {"account": "2", "label": "TV Refined",
               "book": {"trades": 3, "net_pnl": 42.0}, "winners": [], "losers": []}
    c = sqlite3.connect(jdb)
    c.execute("INSERT INTO journal_entries (week, account, trade_stats, market_data, "
              "tags, sweep_results, recap) VALUES (?,?,?,?,?,?,?)",
              ("2026-W34", "2", "{}", "{}", "{}", "{}", json.dumps(payload)))
    c.commit(); c.close()
    with a.app.test_client() as cl:
        entries = cl.get("/api/journal/entries").get_json()
    e = entries[0]
    assert isinstance(e["recap"], dict), "recap came back as a raw string"
    assert e["recap"]["book"]["net_pnl"] == 42.0


def test_entry_without_a_snapshot_reads_as_empty_not_broken(jdb):
    """Entries written before this feature must still load."""
    c = sqlite3.connect(jdb)
    c.execute("INSERT INTO journal_entries (week, account, trade_stats, market_data, "
              "tags, sweep_results) VALUES (?,?,?,?,?,?)",
              ("2026-W33", "2", "{}", "{}", "{}", "{}"))
    c.commit(); c.close()
    with a.app.test_client() as cl:
        entries = cl.get("/api/journal/entries").get_json()
    assert entries[0]["recap"] == {}


def test_migration_is_idempotent(monkeypatch, tmp_path):
    """init_db runs on every boot; adding the column twice must not error."""
    import shutil
    db = tmp_path / "m.db"
    shutil.copy("trades.db", db)
    c = sqlite3.connect(db)
    try:    c.execute("ALTER TABLE journal_entries DROP COLUMN recap"); c.commit()
    except Exception: pass
    c.close()
    monkeypatch.setattr(a, "get_db",
                        lambda: (lambda x: (x.__setattr__("row_factory", sqlite3.Row), x)[1])
                        (sqlite3.connect(db)))
    a.init_db(); a.init_db()          # twice
    c = sqlite3.connect(db)
    assert "recap" in [r[1] for r in c.execute("PRAGMA table_info(journal_entries)")]
    c.close()
