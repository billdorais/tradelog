"""Crew Paper (acct4) retired on 2026-11-15; Crew Live (acct6) is the crew's own book.

_retire_crew_paper runs once at startup:
  1) copies Crew Paper's gate overrides onto Crew Live — which used to inherit them
     through _GATE_MIRROR — so Crew Live gates exactly as before the mirror went;
  2) deletes rules that only target Crew Paper and strips the Crew Paper broker from
     rules that also target another book.
Both parts are flag-guarded so they run once and a later manual change sticks.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "retire.db"
    shutil.copy("trades.db", path)
    c = sqlite3.connect(path)
    c.execute("DELETE FROM routing_rules")
    c.execute("DELETE FROM app_settings WHERE key IN ('GATES_BY_ACCOUNT', 'CREW_PAPER_GATES_MOVED', "
              "'CREW_PAPER_RULES_RETIRED')")
    c.commit()
    c.close()

    def _db():
        k = sqlite3.connect(path)
        k.row_factory = sqlite3.Row
        return k
    monkeypatch.setattr(a, "get_db", _db)
    monkeypatch.setattr(a, "_gates_acct_ts", 0.0)
    return path


def _rule(path, name, brokers, strat="AAA_CAM_BREAKOUT_R4S4_V02_5MIN"):
    nodes = [{"type": "strategy", "value": strat}] + [{"type": "broker", "value": b} for b in brokers]
    c = sqlite3.connect(path)
    c.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)", (name, json.dumps(nodes)))
    c.commit()
    c.close()


def _rules(path):
    c = sqlite3.connect(path)
    out = {n: [x["value"] for x in json.loads(raw) if x["type"] == "broker"]
           for n, raw in c.execute("SELECT name, nodes FROM routing_rules").fetchall()}
    c.close()
    return out


# ── gates ─────────────────────────────────────────────────────────────────────

CREW_GATES = {"daytype": {"enabled": True, "exempt_tickers": ["SPY"]},
              "rvol": {"enabled": True, "min": 1.2},
              "hours": {"windows": [["09:35", "11:00"]]}}


def test_crew_live_keeps_exactly_the_gates_it_had_through_the_mirror(db, monkeypatch):
    a._save_setting("GATES_BY_ACCOUNT", json.dumps({"alpaca4": CREW_GATES}))
    # Before: what the old mirror resolved for Crew Live.
    with monkeypatch.context() as m:
        m.setattr(a, "_GATE_MIRROR", {"alpaca6": "alpaca4"})
        a._gates_acct_ts = 0.0
        before = a._account_gate_overrides("alpaca6")
    assert before == CREW_GATES
    a._retire_crew_paper()
    a._gates_acct_ts = 0.0
    assert a._account_gate_overrides("alpaca6") == before
    assert json.loads(a._load_setting("GATES_BY_ACCOUNT"))["alpaca6"] == CREW_GATES


def test_crew_lives_own_overrides_are_never_overwritten(db):
    own = {"rvol": {"enabled": False}}
    a._save_setting("GATES_BY_ACCOUNT", json.dumps({"alpaca4": CREW_GATES, "alpaca6": own}))
    a._retire_crew_paper()
    assert json.loads(a._load_setting("GATES_BY_ACCOUNT"))["alpaca6"] == own


def test_the_gate_copy_runs_once(db):
    a._save_setting("GATES_BY_ACCOUNT", json.dumps({"alpaca4": CREW_GATES}))
    a._retire_crew_paper()
    later = {"alpaca4": {"rvol": {"enabled": False}}}            # a later manual edit
    a._save_setting("GATES_BY_ACCOUNT", json.dumps(later))
    a._retire_crew_paper()
    assert json.loads(a._load_setting("GATES_BY_ACCOUNT")) == later


# ── rules ─────────────────────────────────────────────────────────────────────

def test_crew_paper_rules_are_removed_and_nothing_else(db):
    _rule(db, "AAA · Crew", ["alpaca-paper-4"])
    _rule(db, "BBB · Crew", ["alpaca-live-4"])
    _rule(db, "AAA · Crew Live", ["alpaca-live-6"])
    _rule(db, "AAA · Refined", ["alpaca-paper-2"])
    _rule(db, "AAA · shared", ["alpaca-paper-2", "alpaca-paper-4"])
    a._retire_crew_paper()
    assert _rules(db) == {"AAA · Crew Live": ["alpaca-live-6"], "AAA · Refined": ["alpaca-paper-2"],
                          "AAA · shared": ["alpaca-paper-2"]}


def test_the_rule_cleanup_runs_once(db):
    a._retire_crew_paper()
    _rule(db, "re-added by hand", ["alpaca-paper-4"])
    a._retire_crew_paper()
    assert "re-added by hand" in _rules(db)


def test_it_does_not_run_on_import_under_pytest():
    import inspect
    src = inspect.getsource(a)
    assert 'if "pytest" not in sys.modules:      # tests call it on their own database\n    _retire_crew_paper()' in src


# ── the crew wire builds Crew Live rules exactly as the mirror did ────────────

def test_a_cloned_crew_live_rule_has_the_source_pipelines_exits(db, monkeypatch):
    slug = "AAA_CAM_BREAKOUT_R4S4_V02_5MIN"
    src = [{"type": "strategy", "value": slug}, {"type": "broker", "value": "alpaca-paper-2"},
           {"type": "quantity", "amount": 7, "unit": "shares"},
           {"type": "exit_params", "trail_offset": 0.42, "trail_trigger": 0.1, "max_hold_mins": 10},
           {"type": "hours", "start": "09:35", "end": "15:30"}, {"type": "entry_source", "value": "tv"}]
    c = sqlite3.connect(db)
    c.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)", ("src", json.dumps(src)))
    c.execute("DELETE FROM crew_reports")
    c.execute("INSERT INTO crew_reports (week, created_at, report) VALUES (?,?,?)",
              ("2026-W46", "2026-11-14 09:00:00",
               f"| Field | Recommendation |\n|---|---|\n| Top 1 to run | 1. {slug} — long [TV] |\n"))
    c.commit()
    c.close()
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca6", {"tag": "alpaca6", "broker": None})
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 1000.0, raising=False)
    monkeypatch.setattr(a, "_fetch_alpaca_last_prices", lambda tks: {"AAA": 50.0}, raising=False)
    a.app.config["TESTING"] = True
    with a.app.test_client() as cl:
        assert cl.post("/api/crew/wire_to_router", json={}).status_code == 200
    c = sqlite3.connect(db)
    live = [json.loads(r[0]) for r in c.execute("SELECT nodes FROM routing_rules WHERE name LIKE '%Crew Live'")]
    c.close()
    assert live == [[{"type": "strategy", "value": slug}, {"type": "broker", "value": "alpaca-live-6"},
                     {"type": "quantity", "amount": 20, "unit": "shares"},
                     {"type": "exit_params", "trail_offset": 0.42, "trail_trigger": 0.1, "max_hold_mins": 10},
                     {"type": "hours", "start": "09:35", "end": "15:30"}, {"type": "entry_source", "value": "tv"},
                     {"type": "side_gate", "value": "long"}]]
