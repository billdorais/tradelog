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


# ── Rendering: evidence yes, narration no ────────────────────────────────────

def _journal_html():
    return open("templates/journal.html", encoding="utf-8").read()


def test_journal_renders_the_recap_block():
    html = _journal_html()
    assert "function renderRecap(e)" in html
    assert "${renderRecap(e)}" in html, "renderRecap defined but never called"


def test_recap_block_sits_above_the_ai_summary():
    """Evidence, then interpretation, then your notes. The AI summary is doing the
    interpreting, so the facts should be in view before you read its take."""
    html = _journal_html()
    i_recap = html.index("${renderRecap(e)}")
    i_ai    = html.index("Weekly Analysis", i_recap - 4000)
    i_notes = html.index('class="notes-section"')
    assert i_recap < i_ai < i_notes


def test_narration_is_not_rendered_even_though_it_is_stored():
    """script/script_prose are a show outline. They stay in the stored snapshot so
    the record is faithful and the choice is reversible, but the journal shows what
    happened rather than what to say about it."""
    html = _journal_html()
    start = html.index("function renderRecap(e)")
    end   = html.index("function renderEntry(e)")
    block = html[start:end]
    assert "script_prose" not in block
    # `r.script` must not be read either (the word appears in prose/comments only).
    import re
    assert not re.search(r"\br\.script\b", block)


def test_snapshot_still_stores_the_narration(monkeypatch):
    """Not displaying it is an editorial choice, not a data loss — turning it back
    on later must not require regenerating old entries."""
    class _B: _paper = True
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "4": {"tag": "alpaca4", "num": "4", "label": "Crew Paper",
              "broker": _B(), "fills_fn": lambda: ["x"]}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda *A, **K: {"closed_clean": [_rt("AAPL_CAM_BREAKOUT_R3S3", 25)]})
    out = a._build_recap(account="4", frm="2026-08-17", to="2026-08-21")
    assert "script" in out and "script_prose" in out


# ── Entry-mechanism split: TV-wired picks vs Kairos-wired picks ──────────────

def _rules_db(monkeypatch, tmp_path, rules):
    db = tmp_path / "r.db"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE routing_rules (name TEXT, enabled INT, nodes TEXT)")
    for name, enabled, nodes in rules:
        c.execute("INSERT INTO routing_rules VALUES (?,?,?)", (name, enabled, json.dumps(nodes)))
    c.commit(); c.close()
    monkeypatch.setattr(a, "get_db", lambda: sqlite3.connect(db))
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [
        {"tag": "alpaca4", "num": "4", "label": "Crew Paper",
         "target_paper": "alpaca-paper-4", "target_live": "alpaca-live-4"}])
    return db


def test_entry_source_comes_from_rule_wiring_not_the_order_id(monkeypatch, tmp_path):
    """Every order this app places is tagged "kairos-{slug}-{ts}" regardless of
    mechanism — that prefix is the app's name. The rule's entry_source node is what
    actually says TV or engine."""
    _rules_db(monkeypatch, tmp_path, [
        ("A · Crew", 1, [{"type": "strategy", "value": "AAA_CAM_BREAKOUT_R3S3"},
                         {"type": "entry_source", "value": "kairos"},
                         {"type": "broker", "value": "alpaca-paper-4"}]),
        ("B · Crew", 1, [{"type": "strategy", "value": "BBB_CAM_BREAKOUT_R3S3"},
                         {"type": "entry_source", "value": "tv"},
                         {"type": "broker", "value": "alpaca-paper-4"}]),
        # No entry_source node at all -> fires on TV alerts.
        ("C · Crew", 1, [{"type": "strategy", "value": "CCC_CAM_BREAKOUT_R3S3"},
                         {"type": "broker", "value": "alpaca-paper-4"}]),
        ("off", 0, [{"type": "strategy", "value": "OFF_X"},
                    {"type": "entry_source", "value": "kairos"}]),
    ])
    m = a._entry_source_by_strategy("alpaca4")
    assert m["AAA_CAM_BREAKOUT_R3S3"] == "kairos"
    assert m["BBB_CAM_BREAKOUT_R3S3"] == "tv"
    assert m["CCC_CAM_BREAKOUT_R3S3"] == "tv", "no node should default to TV"
    assert "OFF_X" not in m, "disabled rules must not contribute"


def test_split_reports_each_mechanism_separately(monkeypatch, tmp_path):
    _rules_db(monkeypatch, tmp_path, [
        ("K", 1, [{"type": "strategy", "value": "KKK_CAM_BREAKOUT_R3S3"},
                  {"type": "entry_source", "value": "kairos"},
                  {"type": "broker", "value": "alpaca-paper-4"}]),
        ("T", 1, [{"type": "strategy", "value": "TTT_CAM_BREAKOUT_R3S3"},
                  {"type": "entry_source", "value": "tv"},
                  {"type": "broker", "value": "alpaca-paper-4"}]),
    ])
    class _B: _paper = True
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "4": {"tag": "alpaca4", "num": "4", "label": "Crew Paper",
              "broker": _B(), "fills_fn": lambda: ["x"]}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda *A, **K: {"closed_clean": [
        _rt("KKK_CAM_BREAKOUT_R3S3", 60), _rt("KKK_CAM_BREAKOUT_R3S3", -20),
        _rt("TTT_CAM_BREAKOUT_R3S3", 10),
        _rt("ZZZ_NOT_WIRED", 99),                      # no rule -> unknown
    ]})
    es = a._build_recap(account="4", frm="2026-08-17", to="2026-08-21")["entry_split"]
    assert es["kairos"]["trades"] == 2 and es["kairos"]["pnl"] == 40.0
    assert es["kairos"]["win_rate"] == 50.0 and es["kairos"]["profit_factor"] == 3.0
    assert es["tv"]["trades"] == 1 and es["tv"]["pnl"] == 10.0
    # An unwired strategy is its own bucket, not silently folded into TV.
    assert es["unknown"]["trades"] == 1 and es["unknown"]["pnl"] == 99.0
    assert es["kairos"]["strategies"] == 1 and es["tv"]["strategies"] == 1


def test_split_states_that_it_reflects_current_wiring(monkeypatch, tmp_path):
    """A pick rewired mid-window has its earlier trades counted under today's
    source. The payload has to say so rather than imply per-trade provenance."""
    _rules_db(monkeypatch, tmp_path, [])
    class _B: _paper = True
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "4": {"tag": "alpaca4", "num": "4", "label": "Crew Paper",
              "broker": _B(), "fills_fn": lambda: ["x"]}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda *A, **K: {"closed_clean": [_rt("X_CAM_BREAKOUT_R3S3", 5)]})
    es = a._build_recap(account="4", frm="2026-08-17", to="2026-08-21")["entry_split"]
    assert "current rule wiring" in es["basis"]


def test_entry_split_is_rendered_on_both_surfaces():
    """Same question, both places it gets asked."""
    recap = open("templates/recap.html", encoding="utf-8").read()
    assert "renderEntrySplit" in recap and "${renderEntrySplit(d.entry_split)}" in recap
    journal = open("templates/journal.html", encoding="utf-8").read()
    start = journal.index("function renderRecap(e)")
    end   = journal.index("function renderEntry(e)")
    assert "entry_split" in journal[start:end], "journal recap block omits the split"


def test_split_surfaces_expectancy_not_just_total_pnl():
    """TV and Kairos rarely trade the same number of times, so a raw P&L column
    alone would rank by activity. Per-trade expectancy is the honest comparison."""
    for f in ("templates/recap.html", "templates/journal.html"):
        html = open(f, encoding="utf-8").read()
        assert "expectancy" in html, f"{f} shows no per-trade expectancy"


# ── Crew scorecard bucketed by entry mechanism ───────────────────────────────

def test_scorecard_rows_carry_the_reports_entry_tag(monkeypatch):
    """The crew tags each pick [TV] or [Kairos] and the parser already keeps it —
    _pick_scorecard was dropping it on the floor. Grading a report's picks should
    use the tag from THAT report, not current rule wiring, or a pick rewired since
    would be graded under a decision nobody made."""
    import routes.crew as crew
    picks = [{"strategy": "AAA_CAM_BREAKOUT_R3S3", "side": "both", "entry": "kairos"},
             {"strategy": "BBB_CAM_BREAKOUT_R3S3", "side": "long", "entry": "tv"},
             {"strategy": "CCC_CAM_BREAKOUT_R3S3", "side": "both"}]          # untagged
    monkeypatch.setattr(crew, "_parse_picks_block", lambda *A, **K: picks, raising=False)
    src = inspect_source(crew._pick_scorecard)
    # The expression was hoisted into _mech once proxy grading added a third row
    # branch. What matters is that EVERY branch emits an entry, none defaults to
    # blank, and the fallback is the report's own tag rather than live wiring.
    assert '(p.get("entry") or "tv")' in src, "entry tag not carried into rows"
    n_rows  = src.count('rows.append({')
    n_entry = src.count('"entry": _mech') + src.count('"entry": (p.get("entry") or "tv")')
    assert n_rows >= 3, f"expected traded / proxy / ungraded branches, found {n_rows}"
    assert n_entry == n_rows, f"{n_rows} row branches, {n_entry} carry an entry tag"


def inspect_source(fn):
    import inspect
    return inspect.getsource(fn)


def test_scorecard_is_bucketed_with_per_bucket_subtotals():
    html = open("templates/recap.html", encoding="utf-8").read()
    i = html.index("function renderScorecard(sc)")
    j = html.index("</script>", i)
    block = html[i:j]
    assert "_bucket('tv', 'TV entries'" in block
    assert "_bucket('kairos', 'Kairos entries'" in block
    # A bucket without its own net is just a visual grouping, not a comparison.
    assert "net <span" in block and "green" in block


def test_untagged_picks_default_to_tv_not_dropped():
    """An older report without tags must still render every pick — silently
    dropping rows would make the scorecard understate the roster."""
    html = open("templates/recap.html", encoding="utf-8").read()
    i = html.index("function renderScorecard(sc)")
    block = html[i:html.index("</script>", i)]
    assert "(p.entry || 'tv')" in block
