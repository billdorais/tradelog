"""Per-account weekly journals: (week, account) instead of one entry per week.

The journal was keyed `week UNIQUE` and implicitly covered TV Refined, so the
three curated books (TV Refined / Kairos Refined / Crew) could not each hold a
weekly entry. These tests pin the migration (existing rows are TV Refined and
must survive intact) and the account scoping of the week-keyed endpoints — an
unscoped write or delete would clobber another book's entry.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import sqlite3

import pytest


PRE_MIGRATION_DDL = """
    CREATE TABLE journal_entries (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        week         TEXT UNIQUE,
        generated_at TEXT,
        trade_stats  TEXT,
        market_data  TEXT,
        ai_summary   TEXT,
        user_notes   TEXT
    , tags TEXT, sweep_results TEXT)
"""


@pytest.fixture()
def journal_app(tmp_path):
    """App wired to a fresh DB holding a pre-migration journal_entries table."""
    import app as a

    db = tmp_path / "journal.db"
    conn = sqlite3.connect(db)
    conn.execute(PRE_MIGRATION_DDL)
    # One legacy row: written before per-account journals, so it IS TV Refined.
    conn.execute(
        "INSERT INTO journal_entries (week, generated_at, trade_stats, market_data, "
        "ai_summary, user_notes, tags, sweep_results) VALUES (?,?,?,?,?,?,?,?)",
        ("2026-W27", "2026-07-06T10:00:00Z", json.dumps({"total_pnl": 123.45}),
         json.dumps({"SPY": {"weekly_return": 1.2}}), "legacy summary",
         "legacy notes", json.dumps({"grade": "B"}), json.dumps({"mode": "global"})),
    )
    conn.commit()
    conn.close()

    saved_db = a.get_db

    def _fake_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    a.get_db = _fake_db
    a.init_db()                      # runs the per-account migration
    a.app.config["TESTING"] = True
    yield a, db
    a.get_db = saved_db


def _rows(db, sql, args=()):
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    out = [dict(r) for r in c.execute(sql, args).fetchall()]
    c.close()
    return out


def test_migration_preserves_legacy_row_as_tv_refined(journal_app):
    _, db = journal_app
    rows = _rows(db, "SELECT * FROM journal_entries WHERE week='2026-W27'")
    assert len(rows) == 1
    r = rows[0]
    # Backfilled to TV Refined — that's what pre-account entries actually were.
    assert r["account"] == "2"
    # Content survives the table rebuild.
    assert r["ai_summary"] == "legacy summary"
    assert r["user_notes"] == "legacy notes"
    assert json.loads(r["trade_stats"])["total_pnl"] == 123.45
    assert json.loads(r["tags"])["grade"] == "B"
    assert json.loads(r["sweep_results"])["mode"] == "global"


def test_migration_is_idempotent(journal_app):
    a, db = journal_app
    a.init_db()                      # second boot must not duplicate or wipe
    a.init_db()
    rows = _rows(db, "SELECT * FROM journal_entries WHERE week='2026-W27'")
    assert len(rows) == 1
    assert rows[0]["ai_summary"] == "legacy summary"


def test_three_books_coexist_in_one_week(journal_app):
    _, db = journal_app
    c = sqlite3.connect(db)
    for acct in ("3", "4"):
        c.execute("INSERT INTO journal_entries (week, account, ai_summary) VALUES (?,?,?)",
                  ("2026-W27", acct, f"acct{acct} summary"))
    c.commit()
    # ...but the same book twice in a week is still rejected.
    with pytest.raises(sqlite3.IntegrityError):
        c.execute("INSERT INTO journal_entries (week, account) VALUES ('2026-W27','3')")
    c.close()
    rows = _rows(db, "SELECT account FROM journal_entries WHERE week='2026-W27' ORDER BY account")
    assert [r["account"] for r in rows] == ["2", "3", "4"]


def test_entries_endpoint_filters_by_account(journal_app):
    a, db = journal_app
    c = sqlite3.connect(db)
    c.execute("INSERT INTO journal_entries (week, account, ai_summary) VALUES (?,?,?)",
              ("2026-W27", "3", "kairos summary"))
    c.commit(); c.close()

    with a.app.test_client() as cl:
        only_3 = cl.get("/api/journal/entries?account=3").get_json()
        assert [e["week"] for e in only_3] == ["2026-W27"]
        assert only_3[0]["ai_summary"] == "kairos summary"
        # Label comes from the registry, so it can't drift from the account.
        assert only_3[0]["account_label"] == a.ACCOUNT_META["3"]["label"]

        every = cl.get("/api/journal/entries").get_json()
        assert sorted(e["account"] for e in every) == ["2", "3"]


def test_notes_and_delete_scope_to_one_book(journal_app):
    a, db = journal_app
    c = sqlite3.connect(db)
    c.execute("INSERT INTO journal_entries (week, account, user_notes) VALUES (?,?,?)",
              ("2026-W27", "3", "kairos notes"))
    c.commit(); c.close()

    with a.app.test_client() as cl:
        # Writing Kairos notes must not touch TV Refined's.
        cl.put("/api/journal/notes",
               json={"week": "2026-W27", "account": "3", "notes": "kairos notes v2"})
        rows = {r["account"]: r["user_notes"]
                for r in _rows(db, "SELECT account, user_notes FROM journal_entries")}
        assert rows == {"2": "legacy notes", "3": "kairos notes v2"}

        # Deleting Kairos's entry must leave TV Refined's standing.
        cl.delete("/api/journal/entries/2026-W27?account=3")
        left = _rows(db, "SELECT account FROM journal_entries WHERE week='2026-W27'")
        assert [r["account"] for r in left] == ["2"]


def test_sweep_files_under_the_account_it_ran_on(journal_app):
    a, db = journal_app
    with a.app.test_client() as cl:
        cl.put("/api/journal/sweep",
               json={"week": "2026-W27", "account": "4", "sweep_results": {"mode": "crew"}})
        # Creates Crew's own row rather than overwriting TV Refined's sweep.
        rows = {r["account"]: r["sweep_results"]
                for r in _rows(db, "SELECT account, sweep_results FROM journal_entries "
                                   "WHERE week='2026-W27'")}
        assert json.loads(rows["4"])["mode"] == "crew"
        assert json.loads(rows["2"])["mode"] == "global"   # untouched


def test_generate_rejects_unconfigured_account(journal_app):
    a, _ = journal_app
    # Account validation runs before the AI-service guards, so this holds whether
    # or not an API key is configured — and no API call is made.
    with a.app.test_client() as cl:
        r = cl.post("/api/journal/generate", json={"week": "2026-W27", "account": "9"})
        # Must 400 rather than silently falling back to account 1's fills while
        # labelling the entry account 9.
        assert r.status_code == 400
        assert "not configured" in r.get_json()["error"]
