"""Farm Leaders (/farms): rank both farms, show the top N by period with an honest
forward series, and rewire Kairos Select (acct7) to the combined roster."""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import shutil
import sqlite3

import pytest

import routes.farms as farms


def _s(tkr, band="R4S4"):
    return f"{tkr}_CAM_BREAKOUT_{band}_V02_5MIN"


def _trips(spec):
    """spec: [(strategy, pnl, date)]"""
    return [{"strategy": s, "pnl": p, "date": d} for s, p, d in spec]


# ── pure helpers ──────────────────────────────────────────────────────────────

def test_late_session_exit_buckets_on_the_new_york_date():
    assert farms.et_date("2026-10-16T00:30:00Z") == "2026-10-15"
    assert farms.et_date("garbage") == ""


def test_period_keys():
    assert farms.period_key("2026-10-15", "week") == "2026-10-12"   # Thursday → Monday
    assert farms.period_key("2026-10-12", "week") == "2026-10-12"
    assert farms.period_key("2026-10-15", "month") == "2026-10"


def test_rank_needs_profit_and_the_trade_floor_and_respects_the_ticker_cap():
    spec = []
    for s, pnl in ((_s("SPY"), 50), (_s("SPY", "R3S3"), 40), (_s("SPY", "R2S2"), 30),
                   (_s("AAA"), 20), (_s("LOS"), -10)):
        spec += [(s, pnl / 5, "2026-10-01")] * 5
    spec += [(_s("THIN"), 100, "2026-10-01")] * 2        # big but under the floor
    top = [n for n, _ in farms.rank_top(_trips(spec), n=10, min_trades=5, cap=2)]
    assert top == [_s("SPY"), _s("SPY", "R3S3"), _s("AAA")]   # third SPY capped, no backfill
    assert _s("THIN") not in top and _s("LOS") not in top
    nocap = [n for n, _ in farms.rank_top(_trips(spec), n=10, min_trades=5, cap=0)]
    assert _s("SPY", "R2S2") in nocap


def test_forward_series_ranks_only_on_earlier_periods():
    # Week 1: A wins, B loses. Week 2: A loses, B wins big.
    A, B = _s("AAA"), _s("BBB")
    spec = ([(A, 10, "2026-10-05")] * 2 + [(B, -5, "2026-10-05")] * 2
            + [(A, -3, "2026-10-12")] * 2 + [(B, 40, "2026-10-12")] * 2)
    v = farms.farm_view(_trips(spec), n=1, period="week", min_trades=1, cap=0)
    p1, p2 = v["periods"]
    assert p1["forward_pnl"] is None                      # nothing to rank on yet
    # Forward pick for week 2 was A (the week-1 leader), which lost 6 —
    # NOT B, which today's hindsight ranking prefers.
    assert p2["forward_pnl"] == -6
    assert v["top"][0]["strategy"] == B
    assert p2["top_pnl"] == 80
    # Baseline: week 2 farm made 74 across 2 strategies → 1 average strategy = 37.
    assert p2["avg_n_pnl"] == 37
    assert v["totals"]["forward_beat"] == 0
    assert v["top"][0]["by_period"] == {"2026-10-05": -10, "2026-10-12": 80}


def test_merge_gives_a_strategy_on_both_lists_one_rule_from_its_better_farm():
    S = _s("DUP")
    ro = farms.merge_roster({
        "tv":     [{"strategy": S, "pnl": 30}, {"strategy": _s("TVO"), "pnl": 10}],
        "kairos": [{"strategy": S, "pnl": 50}],
    })
    by = {r["strategy"]: r for r in ro}
    assert len(ro) == 2
    assert by[S]["farm"] == "kairos" and by[S]["entry"] == "kairos" and by[S]["also_in"] == "tv"
    assert by[_s("TVO")]["entry"] == "tv"


def test_select_nodes_clone_exits_and_pin_broker_size_and_entry():
    src = [{"type": "strategy", "value": "X"},
           {"type": "quantity", "amount": 10, "unit": "shares"},
           {"type": "broker", "value": "alpaca-paper-2", "qty_override": 99},
           {"type": "broker", "value": "alpaca-paper"},
           {"type": "side_gate", "value": "long"},
           {"type": "exit_params", "trail_offset": 0.4, "mode": "percent"}]
    out = farms.select_nodes("X", 42, "kairos", src)
    types = [n["type"] for n in out]
    assert types.count("broker") == 1
    # The farms honour the pipeline's side gate, so Select must too.
    assert {"type": "side_gate", "value": "long"} in out
    assert {"type": "broker", "value": "alpaca-paper-7"} in out
    assert {"type": "quantity", "amount": 42, "unit": "shares"} in out
    assert {"type": "entry_source", "value": "kairos"} in out
    assert {"type": "exit_params", "trail_offset": 0.4, "mode": "percent"} in out
    assert src[2]["value"] == "alpaca-paper-2"             # source not mutated


# ── apply ─────────────────────────────────────────────────────────────────────

class _FakeBroker:
    def __init__(self, open_symbols):
        self._open = list(open_symbols)
    def _invalidate_pos_cache(self):
        pass
    def get_positions(self, raise_on_error=False):
        return [{"symbol": s} for s in self._open]


@pytest.fixture()
def router(tmp_path, monkeypatch):
    import app as a
    db = tmp_path / "farms.db"
    shutil.copy("trades.db", db)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM routing_rules")
    rows = [
        # existing Select-only rules: OLD leaves, KEEP stays, OPEN leaves but is open
        (_s("OLD") + " · Select",  [{"type": "strategy", "value": _s("OLD")},  {"type": "broker", "value": "alpaca-paper-7"}]),
        (_s("KEEP") + " · Select", [{"type": "strategy", "value": _s("KEEP")}, {"type": "broker", "value": "alpaca-paper-7"}]),
        (_s("OPEN") + " · Select", [{"type": "strategy", "value": _s("OPEN")}, {"type": "broker", "value": "alpaca-paper-7"}]),
        # a farm pipeline that ALSO carried the Select broker (old wiring style)
        (_s("MIX"), [{"type": "strategy", "value": _s("MIX")}, {"type": "broker", "value": "alpaca-paper"},
                     {"type": "broker", "value": "alpaca-paper-7"}]),
        # the farm pipeline for a new pick — its exits should be cloned
        (_s("NEW"), [{"type": "strategy", "value": _s("NEW")}, {"type": "broker", "value": "alpaca-paper"},
                     {"type": "exit_params", "trail_offset": 0.3, "mode": "percent"}]),
    ]
    for name, nodes in rows:
        conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)", (name, json.dumps(nodes)))
    conn.commit(); conn.close()

    def _db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row
        return c
    monkeypatch.setattr(a, "get_db", _db)
    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {**a.ACCOUNTS_BY_TAG,
                                               "alpaca7": {"broker": _FakeBroker(["OPEN"])}})
    monkeypatch.setattr(a, "_fetch_alpaca_last_prices",
                        lambda tks: {t: 100.0 for t in tks})
    roster = [{"strategy": _s("NEW"), "farm": "tv", "entry": "tv", "pnl": 50, "also_in": None},
              {"strategy": _s("KEEP"), "farm": "kairos", "entry": "kairos", "pnl": 30, "also_in": None}]
    monkeypatch.setattr(farms, "_build", lambda p: ({}, roster, {}, {}))
    saved = {}
    monkeypatch.setattr(a, "_save_setting", lambda k, v: saved.__setitem__(k, v))
    return db, roster, saved


def _rules(db):
    c = sqlite3.connect(db)
    out = {name: json.loads(nodes) for name, nodes in c.execute("SELECT name, nodes FROM routing_rules")}
    c.close()
    return out


def test_apply_reconciles_select_to_the_roster(router):
    import app as a
    db, roster, saved = router
    r = a.app.test_client().post("/api/farms/apply_select",
                                 json={"n": 10, "size_dollars": 15000,
                                       "expect": [x["strategy"] for x in roster]})
    body = r.get_json()
    assert r.status_code == 200, body
    assert body["created"] == [_s("NEW")]
    assert body["updated"] == [_s("KEEP")]
    assert body["deleted"] == [_s("OLD")]
    assert body["deferred_open_position"] == [_s("OPEN")]
    assert body["stripped_from_mixed"] == [_s("MIX")]

    rules = _rules(db)
    assert _s("OLD") + " · Select" not in rules
    assert _s("OPEN") + " · Select" in rules
    new = rules[_s("NEW") + " · Select"]
    assert {"type": "broker", "value": "alpaca-paper-7"} in new
    assert {"type": "quantity", "amount": 150, "unit": "shares"} in new      # $15k / $100
    assert {"type": "entry_source", "value": "tv"} in new
    assert {"type": "exit_params", "trail_offset": 0.3, "mode": "percent"} in new
    assert {"type": "entry_source", "value": "kairos"} in rules[_s("KEEP") + " · Select"]
    assert all(n.get("value") != "alpaca-paper-7" for n in rules[_s("MIX")])
    assert {"type": "broker", "value": "alpaca-paper"} in rules[_s("NEW")]   # farm rule untouched
    assert json.loads(saved["SELECT_LAST_APPLY"])["picks"][0]["shares"] == 150


def test_apply_refuses_when_the_leaderboard_moved(router):
    import app as a
    db, _roster, _ = router
    before = _rules(db)
    r = a.app.test_client().post("/api/farms/apply_select", json={"expect": [_s("SOMETHINGELSE")]})
    assert r.status_code == 409
    assert _rules(db) == before


def test_apply_refuses_to_guess_a_size_without_a_price(router, monkeypatch):
    import app as a
    db, roster, _ = router
    monkeypatch.setattr(a, "_fetch_alpaca_last_prices", lambda tks: {})
    before = _rules(db)
    r = a.app.test_client().post("/api/farms/apply_select",
                                 json={"expect": [x["strategy"] for x in roster]})
    assert r.status_code == 409 and "price" in r.get_json()["error"]
    assert _rules(db) == before


# ── accumulation chart ────────────────────────────────────────────────────────

def test_accumulation_runs_per_farm_and_combines_without_double_counting():
    A, B, D = _s("AAA"), _s("BBB"), _s("DUP")
    tv = _trips([(A, 10, "2026-10-05"), (D, 5, "2026-10-06"), (A, 4, "2026-10-12")])
    ka = _trips([(B, 7, "2026-10-05"), (D, 20, "2026-10-06"), (B, -2, "2026-10-13")])
    acc = farms.accumulation({"tv": tv, "kairos": ka}, n=5, period="week", min_trades=1, cap=0)
    assert acc["dates"] == ["2026-10-05", "2026-10-06", "2026-10-12", "2026-10-13"]
    assert acc["top"]["tv"] == [10, 15, 19, 19]
    assert acc["top"]["kairos"] == [7, 27, 27, 25]
    # DUP is on both lists; combined counts it once, from Kairos (where it made more).
    assert acc["top"]["combined"] == [17, 37, 41, 39]


def test_forward_accumulation_starts_in_the_second_period_and_uses_no_hindsight():
    A, B = _s("AAA"), _s("BBB")
    tv = _trips([(A, 10, "2026-10-05"), (B, -5, "2026-10-05"),
                 (A, -3, "2026-10-12"), (B, 40, "2026-10-12")])
    acc = farms.accumulation({"tv": tv, "kairos": []}, n=1, period="week", min_trades=1, cap=0)
    assert acc["forward_start"] == "2026-10-12"
    assert acc["forward"]["tv"] == [None, -3]          # week-1 leader A, not B
    assert acc["forward"]["combined"] == [None, -3]
    assert acc["top"]["tv"] == [-5, 35]                 # today's leader is B


def test_all_window_reads_full_history_and_drops_the_date_floor(monkeypatch):
    import app as a
    seen = {}
    def fake(num, fd, td, full=False):
        seen[num] = (fd, full)
        return [], None
    monkeypatch.setattr(farms, "_farm_trips", fake)
    d = a.app.test_client().get("/api/farms/leaders?days=0").get_json()
    assert d["full"] is True and d["from_date"] == ""
    assert seen == {"1": ("", True), "5": ("", True)}
    a.app.test_client().get("/api/farms/leaders?days=30")
    assert seen["1"][1] is False and seen["1"][0] != ""


# ── clear ─────────────────────────────────────────────────────────────────────

def test_clear_takes_select_off_everything_but_open_positions(router):
    import app as a
    db, _roster, saved = router
    r = a.app.test_client().post("/api/farms/clear_select")
    body = r.get_json()
    assert r.status_code == 200, body
    assert sorted(body["deleted"]) == sorted([_s("OLD"), _s("KEEP")])
    assert body["deferred_open_position"] == [_s("OPEN")]
    assert body["stripped_from_mixed"] == [_s("MIX")]
    rules = _rules(db)
    assert set(rules) == {_s("OPEN") + " · Select", _s("MIX"), _s("NEW")}
    assert rules[_s("MIX")] == [{"type": "strategy", "value": _s("MIX")},
                                {"type": "broker", "value": "alpaca-paper"}]
    assert json.loads(saved["SELECT_LAST_APPLY"])["cleared"] is True


def test_clear_changes_nothing_when_positions_cannot_be_read(router, monkeypatch):
    import app as a
    db, _roster, _ = router

    class _Down(_FakeBroker):
        def get_positions(self, raise_on_error=False):
            raise RuntimeError("alpaca down")
    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {**a.ACCOUNTS_BY_TAG, "alpaca7": {"broker": _Down([])}})
    before = _rules(db)
    r = a.app.test_client().post("/api/farms/clear_select")
    assert r.status_code == 409 and "Nothing was changed" in r.get_json()["error"]
    assert _rules(db) == before


# ── gate parity ───────────────────────────────────────────────────────────────

def _states(select_overrides=None, kairos_overrides=None):
    base = {g: (False, "off", "shared") for g in farms.PARITY_GATES}
    base["strikes"] = (True, "3 losses per level per day", "shared")
    per = {"alpaca": dict(base), "alpaca5": {**base, **(kairos_overrides or {})},
           "alpaca7": {**base, **(select_overrides or {})}}
    return lambda tag: per[tag]


def test_parity_passes_when_select_runs_the_farms_gates():
    r = farms.gate_parity(_states())
    assert r["mismatches"] == [] and r["farms_agree"] is True


def test_parity_flags_an_override_only_select_has():
    r = farms.gate_parity(_states({"day-type": (True, "breakouts only on Outside days", "override")}))
    assert r["mismatches"] == ["day-type"] and r["farms_agree"] is True


def test_parity_flags_when_the_farms_disagree_with_each_other():
    r = farms.gate_parity(_states(kairos_overrides={"strikes": (True, "2 losses per level per day", "override")}))
    assert r["mismatches"] == ["strikes"] and r["farms_agree"] is False


def test_parity_lets_select_carry_the_profit_lock_alone():
    r = farms.gate_parity(_states(select_overrides={"profit-lock": (True, "$100 give-back floor", "shared")}))
    assert r["mismatches"] == []
    row = next(x for x in r["rows"] if x["gate"] == "profit-lock")
    assert row["exempt"] is True and row["select"]["on"] is True


def test_select_meta_flags_match_both_farms():
    import app as a
    # profit_lock excluded: Select carries it deliberately (farms.PARITY_EXEMPT).
    flags = ("daytype_gate", "reversal_gate", "reversal_side",
             "daily_loss_guard", "rvol_gate", "open_loc_gate", "hours_key", "retest")
    for f in flags:
        assert a.ACCOUNT_META["7"].get(f) == a.ACCOUNT_META["1"].get(f) == a.ACCOUNT_META["5"].get(f), f


def test_apply_refuses_when_select_gates_differ(router, monkeypatch):
    import app as a
    db, roster, _ = router
    monkeypatch.setattr(a, "_gate_state", _states({"rvol": (True, "breakouts need >=1.5x", "override")}))
    before = _rules(db)
    r = a.app.test_client().post("/api/farms/apply_select",
                                 json={"expect": [x["strategy"] for x in roster]})
    assert r.status_code == 409 and "rvol" in r.get_json()["error"]
    assert _rules(db) == before


def test_accumulation_carries_one_line_per_top_strategy_per_farm():
    A, D = _s("AAA"), _s("DUP")
    tv = _trips([(A, 10, "2026-10-05"), (D, 5, "2026-10-06")])
    ka = _trips([(D, 20, "2026-10-06"), (D, -4, "2026-10-07")])
    acc = farms.accumulation({"tv": tv, "kairos": ka}, n=5, period="week", min_trades=1, cap=0)
    lines = {(s["farm"], s["strategy"]): s for s in acc["strategies"]}
    assert set(lines) == {("tv", A), ("tv", D), ("kairos", D)}   # DUP once per farm
    assert lines[("tv", A)]["values"] == [10, 10, 10]           # every line spans all dates
    assert lines[("kairos", D)]["values"] == [0, 20, 16]
    assert lines[("tv", A)]["rank"] == 1 and lines[("tv", D)]["rank"] == 2


# ── one rule per strategy; same-ticker strategies both kept ─────────────────

def test_r3_and_r4_on_the_same_ticker_are_both_kept():
    ro, dropped = farms.merge_roster_detail({
        "tv": [{"strategy": _s("NVDA"), "pnl": 50, "profit_factor": 1.4, "win_rate": 60}],
        "kairos": [{"strategy": _s("NVDA", "R3S3"), "pnl": 10, "profit_factor": 2.1, "win_rate": 45}],
    })
    assert {r["strategy"] for r in ro} == {_s("NVDA"), _s("NVDA", "R3S3")}
    assert dropped == []


def test_strategy_on_both_lists_runs_from_the_farm_with_higher_profit_factor():
    S = _s("QQQ")
    ro, dropped = farms.merge_roster_detail({
        "tv":     [{"strategy": S, "pnl": 40, "profit_factor": 1.2, "win_rate": 50}],
        "kairos": [{"strategy": S, "pnl": 10, "profit_factor": 1.9, "win_rate": 50}],
    })
    assert len(ro) == 1 and ro[0]["farm"] == "kairos" and ro[0]["also_in"] == "tv"
    assert dropped[0]["farm"] == "tv" and dropped[0]["lost_to_farm"] == "kairos"


def test_profit_factor_tie_goes_to_win_rate_and_no_losses_is_best():
    S, T = _s("AAPL"), _s("SPY")
    ro, _ = farms.merge_roster_detail({
        "tv":     [{"strategy": S, "pnl": 20, "profit_factor": 1.5, "win_rate": 40},
                   {"strategy": T, "pnl": 5,  "profit_factor": None, "win_rate": 100}],
        "kairos": [{"strategy": S, "pnl": 30, "profit_factor": 1.5, "win_rate": 55},
                   {"strategy": T, "pnl": 90, "profit_factor": 3.0, "win_rate": 70}],
    })
    by = {r["strategy"]: r["farm"] for r in ro}
    assert by[S] == "kairos"     # PF tied, win rate 55 > 40
    assert by[T] == "tv"         # no losing trades beats PF 3.0


# ── roster replay ─────────────────────────────────────────────────────────────

def test_variant_grids():
    mh = farms.build_variants("max_hold", {"max_holds": "15,0,30"})
    assert [v["max_hold"] for v in mh] == [0, 15, 30] and mh[0]["label"] == "no max hold"
    tr = farms.build_variants("trail", {"trails": "0.1,0.2", "triggers": "0,0.1"})
    assert len(tr) == 4 and {(v["trail"], v["trigger"]) for v in tr} == {(0.1, 0), (0.1, 0.1), (0.2, 0), (0.2, 0.1)}
    tp = farms.build_variants("take_profit", {"tps": "0,0.5,1"})
    assert [v["tp"] for v in tp] == [0.5, 1]                       # 0 = no TP = the baseline


def _td(date, base_mh=15):
    return {"date": date, "notional": 1000.0, "base": {"trail": 0.15, "trigger": 0.0, "max_hold": base_mh}}


def test_split_is_at_the_median_date():
    prepared = [_td(d) for d in ("2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04")]
    assert farms.split_date(prepared) == "2026-09-03"
    assert farms.split_date([_td("2026-09-01"), _td("2026-09-01")]) is None


def test_best_on_the_older_half_is_checked_on_the_newer_half():
    prepared = [_td("2026-09-01"), _td("2026-09-02"), _td("2026-09-10"), _td("2026-09-11")]

    # 30-min hold: +$5 a trade on the older half, -$5 on the newer -> fitted, fails.
    # 60-min hold: +$1 everywhere -> smaller on the tune half, but it holds.
    def sim(td, trail, trigger, max_hold, tp):
        old = td["date"] < "2026-09-10"
        if max_hold == 30:
            return {"pnl": 5.0 if old else -5.0, "reason": "max_hold"}
        if max_hold == 60:
            return {"pnl": 1.0, "reason": "trail"}
        return {"pnl": 0.0, "reason": "trail"}          # baseline (15)

    out = farms.score_variants(prepared, farms.build_variants("max_hold", {"max_holds": "30,60"}), sim)
    assert out["split_date"] == "2026-09-10"
    assert out["best"] == "30 min" and out["verdict"] == "fails"
    by = {v["label"]: v for v in out["variants"]}
    assert by["30 min"]["tune_delta"] == 10 and by["30 min"]["check_delta"] == -10
    assert by["60 min"]["check_delta"] == 2 and by["60 min"]["avg_pct"] == 0.1


def test_verdict_none_when_nothing_beats_live_exits():
    prepared = [_td("2026-09-01"), _td("2026-09-10")]
    out = farms.score_variants(prepared, farms.build_variants("take_profit", {"tps": "1"}),
                               lambda td, *a: {"pnl": -1.0 if a[3] else 0.0, "reason": "x"})
    assert out["verdict"] == "none"


def test_replay_endpoint_runs_the_roster_through_the_real_exit_simulator(monkeypatch):
    import datetime as dt
    import types
    import app as a

    S = _s("AAA")
    roster = [{"strategy": S, "farm": "tv", "entry": "tv", "pnl": 5, "also_in": None}]
    monkeypatch.setattr(farms, "_build", lambda p: ({}, roster, {}, {}))

    def _rt(day):
        return {"strategy": S, "ticker": "AAA", "side": "LONG", "entry_price": 100.0, "qty": 10,
                "entry_time": f"{day}T14:00:00Z", "exit_time": f"{day}T14:30:00Z", "pnl": 3.0}
    monkeypatch.setattr(farms, "_farm_round_trips",
                        lambda num, f, t, full=False: ([_rt("2026-09-01"), _rt("2026-09-15")], None))
    monkeypatch.setattr(farms, "_exit_settings", lambda a_: {S: {"trail": 5.0, "trigger": 0.0, "max_hold": 10}})

    def _bars(a_, tk, day):
        t0 = dt.datetime.fromisoformat(f"{day}T14:00:00+00:00")
        # Price climbs $0.10 a minute for an hour: a longer hold makes more.
        return [types.SimpleNamespace(timestamp=t0 + dt.timedelta(minutes=i),
                                      open=100 + i * .1, high=100 + i * .1 + .05,
                                      low=100 + i * .1 - .05, close=100 + i * .1)
                for i in range(61)]
    monkeypatch.setattr(farms, "_day_bars", _bars)
    monkeypatch.setattr(a, "MORNING_TRAIL_PCT", 0, raising=False)
    monkeypatch.setattr(a, "AFTERNOON_TRAIL_PCT", 0, raising=False)

    r = a.app.test_client().post("/api/farms/replay", json={"test": "max_hold", "max_holds": "10,30"})
    body = r.get_json()
    assert r.status_code == 200, body
    assert body["trades"] == 2 and body["roster"] == 1
    assert body["baseline"]["pnl"] == 20.0                  # 10-min hold: +$1.00 x 10 sh x 2 trades
    by = {v["label"]: v for v in body["variants"]}
    assert by["30 min"]["pnl"] == 60.0 and by["30 min"]["delta"] == 40.0
    assert body["best"] == "30 min" and body["verdict"] == "holds"
