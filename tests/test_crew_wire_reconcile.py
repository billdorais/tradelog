"""Wiring crew picks reconciles Crew Live instead of only appending.

The wire button used to add/update but never remove, so every month's picks piled
onto the last. Wiring now deletes crew rules whose strategy dropped out of the
latest report, so the book follows that report.

Since Crew Paper was retired (2026-11-15) the crew's only book is Crew Live
(acct6, REAL MONEY): the wire writes and prunes Crew Live rules directly. A dropped
rule stops NEW entries at once; an open position keeps its broker-side exits, so
there is no open-position deferral. Retired Crew Paper rules are never written,
updated or used as a clone source.

Pick history is unaffected — it lives in crew_reports, which these tests leave
alone.
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


def _slug(tkr, kind="BREAKOUT", band="R4S4"):
    return f"{tkr}_CAM_{kind}_{band}_V02_5MIN"


def _live_rule_nodes(slug):
    return json.dumps([{"type": "strategy", "value": slug},
                       {"type": "broker", "value": "alpaca-live-6"},
                       {"type": "quantity", "amount": 3, "unit": "shares"}])


def _report_picking(slugs):
    """Minimal report whose decision card names these slugs in the 'Top N to run' row."""
    picks = "<br>".join(f"{i+1}. {s} — both [TV]" for i, s in enumerate(slugs))
    return ("| Field | Recommendation |\n"
            "|---|---|\n"
            f"| Top {len(slugs)} to run | {picks} |\n"
            "| Entries | TV Refined |\n"
            "| Sizing | Equal risk |\n")


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    """DB pre-seeded with Crew Live rules for AAA,BBB,XXX,YYY,ZZZ and one stored report."""
    import app as a
    import routes.crew as crew

    db = tmp_path / "wire.db"
    shutil.copy("trades.db", db)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM routing_rules")
    for tkr in ("AAA", "BBB", "XXX", "YYY", "ZZZ"):
        s = _slug(tkr)
        conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                     (f"{s} · Crew Live", _live_rule_nodes(s)))
    conn.execute("DELETE FROM crew_reports")
    conn.commit()
    conn.close()

    def _fake_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(a, "get_db", _fake_db)
    # Crew Live must look configured and sized; prices must not hit the network.
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca6", {"tag": "alpaca6", "broker": None})
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 1000.0, raising=False)
    monkeypatch.setattr(a, "_fetch_alpaca_last_prices",
                        lambda tks: {t: 50.0 for t in tks}, raising=False)
    a.app.config["TESTING"] = True

    def _seed_report(slugs):
        c = sqlite3.connect(db)
        c.execute("INSERT INTO crew_reports (week, created_at, report) VALUES (?,?,?)",
                  ("2026-W30", "2026-07-20 09:00:00", _report_picking(slugs)))
        c.commit(); c.close()

    def _rules(broker):
        c = sqlite3.connect(db)
        rows = c.execute("SELECT nodes FROM routing_rules").fetchall()
        c.close()
        out = set()
        for (raw,) in rows:
            nodes = json.loads(raw)
            if any(n.get("type") == "broker" and n.get("value") == broker for n in nodes):
                out |= {n["value"] for n in nodes if n.get("type") == "strategy"}
        return out

    return a, crew, db, _seed_report, _rules


def test_deselected_rules_are_deleted(wired):
    a, crew, db, seed_report, rules = wired
    keep = [_slug("AAA"), _slug("BBB"), _slug("CCC")]
    seed_report(keep)
    with a.app.test_client() as cl:
        d = cl.post("/api/crew/wire_to_router", json={}).get_json()
    assert rules("alpaca-live-6") == set(keep)
    assert set(d["deleted"]) == {_slug("XXX"), _slug("YYY"), _slug("ZZZ")}
    assert _slug("CCC") in d["created"]
    assert set(d["updated"]) == {_slug("AAA"), _slug("BBB")}
    assert d["deferred_open_position"] == []
    assert d["live_mirror"]["deleted"] == d["deleted"], "the Crew page reads the same lists"


def test_every_rule_is_sized_from_the_live_setting(wired):
    a, crew, db, seed_report, rules = wired
    seed_report([_slug("AAA"), _slug("CCC")])
    with a.app.test_client() as cl:
        d = cl.post("/api/crew/wire_to_router", json={}).get_json()
    assert d["size_dollars"] == 1000.0
    c = sqlite3.connect(db)
    qty = {}
    for (raw,) in c.execute("SELECT nodes FROM routing_rules").fetchall():
        ns = json.loads(raw)
        s = next(n["value"] for n in ns if n["type"] == "strategy")
        qty[s] = next((n["amount"] for n in ns if n["type"] == "quantity"), None)
    c.close()
    assert qty == {_slug("AAA"): 20, _slug("CCC"): 20}            # $1,000 / $50


def test_no_price_means_no_rule_not_a_guessed_size(wired, monkeypatch):
    a, crew, db, seed_report, rules = wired
    monkeypatch.setattr(a, "_fetch_alpaca_last_prices", lambda tks: {"AAA": 50.0}, raising=False)
    seed_report([_slug("AAA"), _slug("CCC")])
    with a.app.test_client() as cl:
        d = cl.post("/api/crew/wire_to_router", json={}).get_json()
    assert d["book"]["skipped_no_price"] == [_slug("CCC")]
    assert rules("alpaca-live-6") == {_slug("AAA")}


def test_the_wire_never_writes_a_crew_paper_rule(wired):
    a, crew, db, seed_report, rules = wired
    seed_report([_slug("AAA"), _slug("NEW")])
    with a.app.test_client() as cl:
        cl.post("/api/crew/wire_to_router", json={})
    assert rules("alpaca-paper-4") == set() and rules("alpaca-live-4") == set()


def test_refuses_without_a_sized_crew_live(wired, monkeypatch):
    a, crew, db, seed_report, rules = wired
    seed_report([_slug("AAA")])
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 0, raising=False)
    with a.app.test_client() as cl:
        r = cl.post("/api/crew/wire_to_router", json={})
    assert r.status_code == 400 and "LIVE_SIZE_DOLLARS" in r.get_json()["error"]
    monkeypatch.delitem(a.ACCOUNTS_BY_TAG, "alpaca6")
    with a.app.test_client() as cl:
        r = cl.post("/api/crew/wire_to_router", json={})
    assert r.status_code == 400 and "Crew Live" in r.get_json()["error"]
    assert len(rules("alpaca-live-6")) == 5, "nothing was changed"


def test_non_crew_rules_are_never_touched(wired):
    """Reconciliation must only ever delete Crew Live rules."""
    a, crew, db, seed_report, rules = wired
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                 (f"{_slug('XXX')} · Refined",
                  json.dumps([{"type": "strategy", "value": _slug("XXX")},
                              {"type": "broker", "value": "alpaca-paper-2"}])))
    conn.commit(); conn.close()

    seed_report([_slug("AAA")])
    with a.app.test_client() as cl:
        cl.post("/api/crew/wire_to_router", json={})

    conn = sqlite3.connect(db)
    names = {r[0] for r in conn.execute("SELECT name FROM routing_rules").fetchall()}
    conn.close()
    assert f"{_slug('XXX')} · Refined" in names               # acct2 rule survives
