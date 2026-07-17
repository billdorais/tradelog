"""The live Crew Paper book view (_crew_book_scorecard).

The pick scorecard grades one report's card text, so it only ever saw the last
Top-N and missed strategies that accumulated across months. This view is sourced
from the LIVE routing rules — the ground truth of what's wired to acct4 — and
measures each strategy since its own wire date (a rule's created_at survives
upserts). So it covers the whole live book and reflects manual edits.
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


def _slug(tkr):
    return f"{tkr}_CAM_BREAKOUT_R4S4_V02_5MIN"


def _nodes(slug, broker="alpaca-paper-4"):
    return json.dumps([{"type": "strategy", "value": slug},
                       {"type": "broker", "value": broker}])


# What the acct4 analysis endpoint will "return" for the wired strategies.
FAKE_PER_STRAT = {
    _slug("AAA"): {"trades": 10, "total_pnl": 100.0, "win_rate": 60.0},
    _slug("BBB"): {"trades": 8,  "total_pnl": -50.0, "win_rate": 25.0},
    _slug("CCC"): {"trades": 3,  "total_pnl": 20.0,  "win_rate": 33.3},
    _slug("DDD"): {"trades": 5,  "total_pnl": 999.0, "win_rate": 80.0},  # acct2 — must be ignored
}


@pytest.fixture()
def book(tmp_path, monkeypatch):
    import app as a
    import routes.crew as crew

    db = tmp_path / "book.db"
    shutil.copy("trades.db", db)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM routing_rules")
    seed = [
        (_slug("AAA"), 1, "alpaca-paper-4", "2026-07-01 09:00:00"),
        (_slug("BBB"), 1, "alpaca-paper-4", "2026-07-10 09:00:00"),
        (_slug("CCC"), 0, "alpaca-paper-4", "2026-07-05 09:00:00"),   # disabled but wired
        (_slug("DDD"), 1, "alpaca-paper-2", "2026-07-02 09:00:00"),   # NOT crew
    ]
    for slug, en, brk, created in seed:
        conn.execute("INSERT INTO routing_rules (name, enabled, nodes, created_at) VALUES (?,?,?,?)",
                     (f"{slug} · rule", en, _nodes(slug, brk), created))
    conn.commit(); conn.close()

    def _fake_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(a, "get_db", _fake_db)

    # Stub the acct4 analysis endpoint the function calls over the test client.
    from flask import request, jsonify
    captured = {}

    def _fake_analysis():
        captured["account"]   = request.args.get("account")
        captured["from_date"] = request.args.get("from_date")
        return jsonify({"per_strategy": FAKE_PER_STRAT})

    monkeypatch.setitem(a.app.view_functions, "api_alpaca_analysis", _fake_analysis)
    return crew, captured


def test_book_is_sourced_from_wired_rules(book):
    crew, captured = book
    bk = crew._crew_book_scorecard()

    # DDD (acct2) is excluded; only the three acct4 strategies count.
    slugs = {r["strategy"] for r in bk["picks"]}
    assert slugs == {_slug("AAA"), _slug("BBB"), _slug("CCC")}
    assert bk["n_wired"] == 3
    # Analysis was queried for acct4 from the EARLIEST wire date.
    assert captured["account"] == "4"
    assert captured["from_date"] == "2026-07-01"


def test_totals_and_worst_first_order(book):
    crew, _ = book
    bk = crew._crew_book_scorecard()
    assert bk["n_traded"] == 3
    assert bk["n_positive"] == 2                      # AAA, CCC
    assert bk["total_pnl"] == pytest.approx(70.0)     # 100 - 50 + 20
    # Worst first.
    assert [r["strategy"] for r in bk["picks"]] == [_slug("BBB"), _slug("CCC"), _slug("AAA")]


def test_each_strategy_carries_its_own_wire_date(book):
    crew, _ = book
    bk = crew._crew_book_scorecard()
    since = {r["strategy"]: r["since"] for r in bk["picks"]}
    assert since[_slug("AAA")] == "2026-07-01"
    assert since[_slug("BBB")] == "2026-07-10"        # wired later → its own window
    assert since[_slug("CCC")] == "2026-07-05"


def test_disabled_rule_is_flagged_but_still_shown(book):
    crew, _ = book
    bk = crew._crew_book_scorecard()
    by = {r["strategy"]: r for r in bk["picks"]}
    assert by[_slug("CCC")]["enabled"] is False
    assert by[_slug("AAA")]["enabled"] is True


def test_empty_when_nothing_wired(book, monkeypatch):
    crew, _ = book
    import app as a
    conn = a.get_db()
    conn.execute("DELETE FROM routing_rules")
    conn.commit(); conn.close()
    assert crew._crew_book_scorecard() == {}


def test_untraded_wired_strategy_sorts_last_with_null_pnl(book, monkeypatch):
    """A wired strategy with no acct4 fills yet shows no P&L and sorts after earners."""
    crew, _ = book
    import app as a
    conn = a.get_db()
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes, created_at) VALUES (?,?,?,?)",
                 (f"{_slug('EEE')} · rule", 1, _nodes(_slug("EEE")), "2026-07-12 09:00:00"))
    conn.commit(); conn.close()
    bk = crew._crew_book_scorecard()
    eee = next(r for r in bk["picks"] if r["strategy"] == _slug("EEE"))
    assert eee["trades"] == 0 and eee["pnl"] is None
    assert bk["picks"][-1]["strategy"] == _slug("EEE")   # untraded sorts last
    assert bk["n_wired"] == 4 and bk["n_traded"] == 3    # EEE wired but not traded
