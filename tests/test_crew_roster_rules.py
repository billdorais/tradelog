"""Roster rules the wire enforces, rather than trusting the card.

A report wired 4 auditions on a 2-slot cap (calling two of them "provisional CORE"),
and kept two incumbents at full size after switching their entry from [TV] to
[Kairos] — on a live record earned entirely on TV entries. So:

  - a pick's live record counts only trades on its CURRENT entry tag; a switch
    restarts it, so a switched pick is an audition until it re-earns core;
  - core needs CREW_CORE_MIN_TRADES of those trades and a profit, else audition;
  - auditions beyond CREW_AUDITION_SLOTS are dropped from the bottom of the card;
  - Crew Live (real money) mirrors core picks only;
  - the hours block judges each name on its better farm.
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

import app as a
from routes import crew as C


def _s(t):
    return f"{t}_CAM_BREAKOUT_R3S3_V02_5MIN"


def _card(picks):
    """picks: [(ticker, side, book, tier)] -> a report with a picks block."""
    rows = "\n".join(f"{_s(t)} | {side} | {book} | {tier}" for t, side, book, tier in picks)
    top = "<br>".join(f"{i + 1}. {_s(t)} — {side} [{book}]" for i, (t, side, book, _) in enumerate(picks))
    return (f"| Decision | Recommendation |\n|---|---|\n| Top {len(picks)} to run | {top} |\n\n"
            f"```picks\n{rows}\n```\n")


# ── report history recovers switches made before the record existed ───────────

@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "r.db"
    shutil.copy("trades.db", path)
    c = sqlite3.connect(path)
    c.execute("DELETE FROM routing_rules"); c.execute("DELETE FROM crew_reports")
    c.commit(); c.close()

    def _db():
        k = sqlite3.connect(path); k.row_factory = sqlite3.Row
        return k
    monkeypatch.setattr(a, "get_db", _db)
    monkeypatch.setattr(a, "DATABASE_URL", None)
    store = {}
    monkeypatch.setattr(a, "_load_setting", lambda k: store.get(k))
    monkeypatch.setattr(a, "_save_setting", lambda k, v: store.__setitem__(k, v))
    return path, store


def _report(path, created, picks):
    c = sqlite3.connect(path)
    c.execute("INSERT INTO crew_reports (week, created_at, report) VALUES (?,?,?)",
              ("w", created, _card(picks)))
    c.commit(); c.close()


def _rule(path, ticker, entry, created, broker="alpaca-paper-4"):
    nodes = [{"type": "strategy", "value": _s(ticker)}, {"type": "broker", "value": broker},
             {"type": "entry_source", "value": entry}]
    c = sqlite3.connect(path)
    c.execute("INSERT INTO routing_rules (name, enabled, nodes, created_at) VALUES (?,1,?,?)",
              (f"{_s(ticker)} · Crew", json.dumps(nodes), created))
    c.commit(); c.close()


def test_the_newest_tag_change_is_where_the_current_run_began(db):
    path, _ = db
    _report(path, "2026-09-01T00:00:00Z", [("NVDA", "both", "TV", "core")])
    _report(path, "2026-10-01T00:00:00Z", [("NVDA", "both", "Kairos", "core")])
    _report(path, "2026-11-01T00:00:00Z", [("NVDA", "both", "Kairos", "core"),
                                           ("PLTR", "both", "TV", "core")])
    _report(path, "2026-10-15T00:00:00Z", [("PLTR", "both", "TV", "core")])
    assert C._report_tag_switches(a) == {_s("NVDA"): "2026-10-01"}


def test_entry_since_uses_the_record_then_report_history_then_the_wire_date(db):
    path, store = db
    _rule(path, "NVDA", "kairos", "2026-06-29 09:00:00")
    _rule(path, "PLTR", "kairos", "2026-07-26 09:00:00")
    _rule(path, "MU", "kairos", "2026-07-01 09:00:00")
    _report(path, "2026-10-31T01:00:00Z", [("NVDA", "both", "TV", "core"), ("PLTR", "both", "TV", "core")])
    _report(path, "2026-11-01T01:00:00Z", [("NVDA", "both", "Kairos", "core"), ("PLTR", "both", "Kairos", "core")])
    store[C.CREW_ENTRY_SINCE_KEY] = json.dumps({_s("PLTR"): {"entry": "kairos", "since": "2026-11-02"}})
    m = C._entry_since_map(a)
    assert m[_s("NVDA")]["since"] == "2026-11-01", "recovered from the reports"
    assert m[_s("PLTR")]["since"] == "2026-11-02", "the saved record wins"
    assert m[_s("MU")]["since"] == "2026-07-01", "never switched: its wire date"


# ── finalize: core bar on the current entry, the cap ──────────────────────────

@pytest.fixture
def fin(monkeypatch):
    state = {"rules": {}, "since": {}, "trades": {}}
    monkeypatch.setattr(C, "_crew_rules_acct4", lambda app_obj: state["rules"])
    monkeypatch.setattr(C, "_entry_since_map", lambda app_obj, rules=None: state["since"])
    def _ts(app_obj, acct, since_by, slugs=None, takeable=False):
        if acct != "4":                                   # a farm's forward record
            return state.get("farm", {})
        return state.get("trades_fn", lambda sb: state["trades"])(since_by)
    monkeypatch.setattr(C, "_trades_since", _ts)
    monkeypatch.setattr(C, "CREW_AUDITION_SLOTS", 2)
    return state


def _p(t, entry, tier):
    return {"strategy": _s(t), "side": "both", "entry": entry, "tier": tier}


def test_a_switched_incumbent_is_an_audition(fin):
    fin["rules"] = {_s("NVDA"): {"entry": "tv", "wired": "2026-06-29"}}
    fin["since"] = {_s("NVDA"): {"entry": "tv", "since": "2026-06-29"}}
    seen = {}
    fin["trades_fn"] = lambda sb: (seen.update(sb), {_s("NVDA"): {"trades": 0, "pnl": 0.0}})[1]
    picks, notes, since = C._finalize_report_picks(a, [_p("NVDA", "kairos", "core")], today="2026-11-02")
    assert picks[0]["tier"] == "audition"
    assert notes["switched"] == [{"strategy": _s("NVDA"), "from": "tv", "to": "kairos"}]
    assert seen[_s("NVDA")] == "2026-11-02" and since[_s("NVDA")] == "2026-11-02"


def test_core_that_earned_it_on_its_current_entry_stays_core(fin):
    fin["rules"] = {_s("NVDA"): {"entry": "kairos", "wired": "2026-06-29"}}
    fin["since"] = {_s("NVDA"): {"entry": "kairos", "since": "2026-06-29"}}
    fin["trades"] = {_s("NVDA"): {"trades": 16, "pnl": 39.5}}
    picks, notes, _ = C._finalize_report_picks(a, [_p("NVDA", "kairos", "core")])
    assert picks[0]["tier"] == "core" and notes["demoted"] == []


@pytest.mark.parametrize("trades,pnl", [(9, 50.0), (20, -5.0)])
def test_thin_or_losing_core_is_demoted(fin, trades, pnl):
    fin["rules"] = {_s("NVDA"): {"entry": "tv", "wired": "2026-06-29"}}
    fin["since"] = {_s("NVDA"): {"entry": "tv", "since": "2026-06-29"}}
    fin["trades"] = {_s("NVDA"): {"trades": trades, "pnl": pnl}}
    picks, notes, _ = C._finalize_report_picks(a, [_p("NVDA", "tv", "core")])
    assert picks[0]["tier"] == "audition" and len(notes["demoted"]) == 1


def test_auditions_over_the_cap_drop_from_the_bottom(fin):
    fin["trades"] = {}
    card = [_p("MU", "kairos", "audition"), _p("INTC", "tv", "audition"),
            _p("META", "tv", "audition"), _p("NFLX", "kairos", "core")]
    picks, notes, _ = C._finalize_report_picks(a, card)
    assert [p["strategy"] for p in picks] == [_s("MU"), _s("INTC")]
    assert notes["dropped_over_cap"] == [_s("META"), _s("NFLX")], "NFLX was demoted, then over the cap"


def test_an_unreadable_book_leaves_tiers_as_written(fin):
    fin["trades_fn"] = lambda sb: None
    picks, notes, _ = C._finalize_report_picks(a, [_p("NVDA", "tv", "core")])
    assert picks[0]["tier"] == "core" and notes["unreadable"] is True


# ── the wire: live mirrors every pick at full size; entry-since is saved ──────

def test_crew_live_mirrors_every_pick_at_full_size(db, monkeypatch):
    path, store = db
    _rule(path, "NVDA", "kairos", "2026-06-29 09:00:00")
    _rule(path, "MU", "kairos", "2026-10-31 09:00:00", broker="alpaca-live-6")   # existing live rule
    _rule(path, "AMD", "kairos", "2026-10-31 09:00:00", broker="alpaca-live-6")  # dropped from the card
    _report(path, "2026-11-01T01:00:00Z", [("NVDA", "both", "Kairos", "core"),
                                           ("MU", "both", "Kairos", "audition")])
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca4", {"tag": "alpaca4", "broker": None})
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca6", {"tag": "alpaca6", "broker": None})
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 1000, raising=False)
    monkeypatch.setattr(a, "_fetch_alpaca_last_prices", lambda tks: {t: 100.0 for t in tks}, raising=False)
    monkeypatch.setattr(C, "_trades_since", lambda *a_, **k: {_s("NVDA"): {"trades": 16, "pnl": 39.5}})
    monkeypatch.setattr(C, "CREW_AUDITION_SLOTS", 4)
    a.app.config["TESTING"] = True
    with a.app.test_client() as cl:
        d = cl.post("/api/crew/wire_to_router", json={}).get_json()
    assert d["roster_rules"]["dropped_over_cap"] == []
    c = sqlite3.connect(path)
    live = [json.loads(r[0]) for r in c.execute("SELECT nodes FROM routing_rules").fetchall()]
    c.close()
    live_rules = [ns for ns in live if any(x.get("value") == "alpaca-live-6" for x in ns)]
    live_slugs = {n["value"] for ns in live_rules for n in ns if n.get("type") == "strategy"}
    assert live_slugs == {_s("NVDA"), _s("MU")}, "an audition goes live too"
    assert d["live_mirror"]["deleted"] == [_s("AMD")], "only the dropped name is pruned"
    # Full live size for both tiers: $1000 / $100 = 10 shares, no audition halving.
    qtys = {next(n["value"] for n in ns if n.get("type") == "strategy"):
            next(n["amount"] for n in ns if n.get("type") == "quantity") for ns in live_rules}
    assert qtys == {_s("NVDA"): 10, _s("MU"): 10}
    saved = json.loads(store[C.CREW_ENTRY_SINCE_KEY])
    assert saved[_s("NVDA")]["entry"] == "kairos" and _s("MU") in saved


def test_the_preview_shows_tiers_after_the_rules_and_who_goes_live(db, monkeypatch):
    path, _ = db
    _report(path, "2026-11-01T01:00:00Z", [("NVDA", "both", "Kairos", "core")])
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca6", {"tag": "alpaca6"})
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 1000, raising=False)
    monkeypatch.setattr(C, "_trades_since", lambda *a_, **k: {})
    monkeypatch.setattr(C, "_safe_pick_windows", lambda *a_, **k: {"windows": {}})
    with a.app.test_client() as cl:
        d = cl.get("/api/crew/wire_preview").get_json()
    p = d["picks"][0]
    assert p["tier"] == "audition" and p["live"] is True, "an audition still mirrors to live"
    assert d["roster_rules"]["demoted"][0]["strategy"] == _s("NVDA")


# ── hours block: the better farm, not the current wiring ──────────────────────

def test_the_hours_split_can_be_bound_to_a_preferred_farm(monkeypatch):
    def _rt(slug, hhmm, pnl):
        h, m = hhmm.split(":")
        return {"strategy": slug, "pnl": pnl, "entry_time": f"2026-09-21T{int(h) + 4:02d}:{m}:00Z"}
    fills = {"1": [_rt(_s("INTC"), "10:00", 1.0)] * 8 + [_rt(_s("INTC"), "16:30", 1.0)] * 8,
             "5": [_rt(_s("INTC"), "10:00", -1.0)] * 8 + [_rt(_s("INTC"), "16:30", 2.0)] * 8}
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "1": {"tag": "alpaca", "label": "TV Farm", "broker": object(), "fills_fn": lambda: "1"},
        "5": {"tag": "alpaca5", "label": "Kairos Farm", "broker": object(), "fills_fn": lambda: "5"}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda f, **k: {"closed_clean": fills[f]})
    monkeypatch.setattr(a, "_build_signal_lookup_for_alpaca", lambda: {})
    monkeypatch.setattr(a, "_shared_hours_windows", lambda key: [("09:35", "10:00"), ("12:00", "15:55")])
    monkeypatch.setattr(C, "_entry_source_by_strategy", lambda *a_, **k: {})
    row = lambda d: next(r for r in d["strategies"] if r["strategy"] == _s("INTC"))
    assert row(C._hours_split_data(45))["mechanism"] == "kairos"
    assert row(C._hours_split_data(45, prefer={_s("INTC"): "tv"}))["mechanism"] == "tv"


def test_the_crew_passes_the_better_farm_to_the_hours_block():
    import inspect
    src = inspect.getsource(C._run_kairos_crew)
    assert 'prefer={r["strategy"]: r["better"] for r in _farm_rows}' in src


# ── the farm-forward route to core ────────────────────────────────────────────

def test_core_verdict():
    v = C._core_verdict
    assert v({"trades": 12, "pnl": 30.0}, None, 10)[0] == "crew"
    assert v({"trades": 2, "pnl": 5.0}, {"trades": 11, "pnl": 14.0}, 10)[0] == "farm"
    assert v({"trades": 2, "pnl": 5.0}, {"trades": 9, "pnl": 14.0}, 10)[0] is None, "thin everywhere"
    assert v({"trades": 12, "pnl": 30.0}, {"trades": 15, "pnl": -4.0}, 10)[0] is None, "the farm contradicts"
    assert v({"trades": 12, "pnl": -1.0}, {"trades": 15, "pnl": 40.0}, 10)[0] is None, "the book contradicts"


def test_an_audition_proven_on_its_farm_forward_is_promoted(fin):
    fin["rules"] = {_s("MU"): {"entry": "kairos", "wired": "2026-11-01"}}
    fin["since"] = {_s("MU"): {"entry": "kairos", "since": "2026-11-01"}}
    fin["trades"] = {_s("MU"): {"trades": 3, "pnl": 8.0}}
    fin["farm"] = {_s("MU"): {"trades": 11, "pnl": 14.0}}
    picks, notes, _ = C._finalize_report_picks(a, [_p("MU", "kairos", "audition")])
    assert picks[0]["tier"] == "core" and picks[0]["core_via"] == "farm"
    assert notes["promoted"][0]["via"] == "farm"
    assert "Kairos Farm forward: 11 trades, $+14.00" in notes["promoted"][0]["why"]


def test_a_brand_new_pick_has_no_forward_record_yet(fin):
    seen = {}
    fin["trades_fn"] = lambda sb: (seen.update(sb), {})[1]
    fin["farm"] = {}
    picks, _, _ = C._finalize_report_picks(a, [_p("MU", "kairos", "core")], today="2026-11-02")
    assert picks[0]["tier"] == "audition" and seen[_s("MU")] == "2026-11-02"


def test_a_farm_earned_core_is_not_flagged_thin_after_the_wire():
    class _App:
        def test_client(self):
            class _C:
                def __enter__(s): return s
                def __exit__(s, *x): return False
                def get(s, url):
                    class _R:
                        def get_json(_): return {"per_strategy": {}}
                    return _R()
            return _C()
    picks = [{"strategy": _s("MU"), "tier": "core", "core_via": "farm"},
             {"strategy": _s("AAA"), "tier": "core"}]
    got = [c["strategy"] for c in C._tier_conflicts(_App(), picks)["conflicts"] if c["kind"] == "core_thin"]
    assert got == [_s("AAA")]


def test_the_book_block_shows_the_farm_forward_record():
    import inspect
    src = inspect.getsource(C._run_kairos_crew)
    assert "Farm FORWARD since" in src and "farm_forward" in src
    assert "OR on its same-entry farm since it was wired" in src
