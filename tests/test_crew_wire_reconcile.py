"""Wiring crew picks reconciles Crew Paper instead of only appending.

The wire button used to add/update but never remove, so every month's picks piled
onto the last: "filter Crew in the Signal Router" grew without bound and the
report reviewed only a fresh Top 10 while old, un-graded strategies kept trading.
Wiring now deletes Crew rules whose strategy dropped out of the latest report, so
Crew Paper mirrors that report. A prune is deferred for any strategy with a LIVE
Crew Paper position, so an open trade keeps its tuned exit.

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


def _crew_rule_nodes(slug):
    return json.dumps([{"type": "strategy", "value": slug},
                       {"type": "broker", "value": "alpaca-paper-4"}])


def _report_picking(slugs):
    """Minimal report whose decision card names these slugs in the 'Top N to run' row."""
    picks = "<br>".join(f"{i+1}. {s} — both [TV]" for i, s in enumerate(slugs))
    return ("| Field | Recommendation |\n"
            "|---|---|\n"
            f"| Top {len(slugs)} to run | {picks} |\n"
            "| Entries | TV Refined |\n"
            "| Sizing | Equal risk |\n")


class _FakeBroker:
    """Stands in for the acct4 Alpaca broker's open-position read."""
    def __init__(self, open_symbols):
        self._open = list(open_symbols)
    def _invalidate_pos_cache(self):
        pass
    def get_positions(self):
        return [{"symbol": s} for s in self._open]


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    """DB pre-seeded with Crew rules for AAA,BBB,XXX,YYY,ZZZ and one stored report."""
    import app as a
    import routes.crew as crew

    db = tmp_path / "wire.db"
    shutil.copy("trades.db", db)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM routing_rules")
    for tkr in ("AAA", "BBB", "XXX", "YYY", "ZZZ"):
        s = _slug(tkr)
        conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                     (f"{s} · Crew", _crew_rule_nodes(s)))
    conn.execute("DELETE FROM crew_reports")
    conn.commit()
    conn.close()

    def _fake_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(a, "get_db", _fake_db)
    # acct4 must look configured, and price/size lookups must not hit the network.
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca4",
                        {**a.ACCOUNTS_BY_TAG.get("alpaca4", {}), "tag": "alpaca4",
                         "broker": _FakeBroker([])})
    monkeypatch.setattr(a, "_fetch_alpaca_last_prices", lambda tks: {}, raising=False)
    a.app.config["TESTING"] = True

    def _seed_report(slugs):
        c = sqlite3.connect(db)
        c.execute("INSERT INTO crew_reports (week, created_at, report) VALUES (?,?,?)",
                  ("2026-W30", "2026-07-20 09:00:00", _report_picking(slugs)))
        c.commit(); c.close()

    def _crew_slugs():
        c = sqlite3.connect(db)
        rows = c.execute("SELECT nodes FROM routing_rules").fetchall()
        c.close()
        out = set()
        for (raw,) in rows:
            nodes = json.loads(raw)
            if any(n.get("type") == "broker" and n.get("value") == "alpaca-paper-4" for n in nodes):
                out |= {n["value"] for n in nodes if n.get("type") == "strategy"}
        return out

    return a, crew, db, _seed_report, _crew_slugs


def _set_open(a, symbols):
    a.ACCOUNTS_BY_TAG["alpaca4"]["broker"] = _FakeBroker(symbols)


def test_deselected_rules_are_deleted(wired):
    a, crew, db, seed_report, crew_slugs = wired
    # New report keeps AAA, BBB; adds CCC; drops XXX, YYY, ZZZ.
    keep = [_slug("AAA"), _slug("BBB"), _slug("CCC")]
    seed_report(keep)

    with a.app.test_client() as cl:
        d = cl.post("/api/crew/wire_to_router", json={}).get_json()

    assert set(crew_slugs()) == set(keep)                    # exact mirror
    assert set(d["deleted"]) == {_slug("XXX"), _slug("YYY"), _slug("ZZZ")}
    assert _slug("CCC") in d["created"]
    assert set(d["updated"]) == {_slug("AAA"), _slug("BBB")}
    assert d["deferred_open_position"] == []


def test_open_position_defers_the_prune(wired):
    a, crew, db, seed_report, crew_slugs = wired
    # XXX would be pruned, but it has a live Crew Paper position.
    _set_open(a, ["XXX"])
    seed_report([_slug("AAA")])

    with a.app.test_client() as cl:
        d = cl.post("/api/crew/wire_to_router", json={}).get_json()

    assert _slug("XXX") in d["deferred_open_position"]
    assert _slug("XXX") in crew_slugs()                      # spared, still trading
    assert set(d["deleted"]) == {_slug("BBB"), _slug("YYY"), _slug("ZZZ")}
    assert _slug("XXX") not in d["deleted"]


def test_positions_fetch_failure_still_prunes(wired):
    """If the acct4 position read raises, prune anyway — the global position-loss
    guard and per-position max-hold protect any open trade regardless."""
    a, crew, db, seed_report, crew_slugs = wired

    class _Boom:
        def _invalidate_pos_cache(self): pass
        def get_positions(self): raise RuntimeError("alpaca down")
    a.ACCOUNTS_BY_TAG["alpaca4"]["broker"] = _Boom()

    seed_report([_slug("AAA")])
    with a.app.test_client() as cl:
        d = cl.post("/api/crew/wire_to_router", json={}).get_json()

    assert set(crew_slugs()) == {_slug("AAA")}
    assert set(d["deleted"]) == {_slug("BBB"), _slug("XXX"), _slug("YYY"), _slug("ZZZ")}
    assert d["deferred_open_position"] == []


def test_non_crew_rules_are_never_touched(wired):
    """Reconciliation must only ever delete acct4 (Crew) rules."""
    a, crew, db, seed_report, crew_slugs = wired
    # Add a TV Refined rule for a strategy that is NOT in the new picks.
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
