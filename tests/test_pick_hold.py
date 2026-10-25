"""Pick and hold: rank once on a lookback, freeze, and score what follows."""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json

import pytest

import routes.farms as farms


def _s(tkr, band="R4S4"):
    return f"{tkr}_CAM_BREAKOUT_{band}_V02_5MIN"


def _t(s, pnl, d):
    return {"strategy": s, "pnl": pnl, "date": d}


A, B, C, D = _s("AAA"), _s("BBB"), _s("CCC"), _s("DDD")

KAIROS = (
    # audition (Sep): A and B win, C loses, D never trades
    [_t(A, 10, f"2026-09-{d:02d}") for d in range(1, 6)]
    + [_t(B, 5, f"2026-09-{d:02d}") for d in range(1, 6)]
    + [_t(C, -4, f"2026-09-{d:02d}") for d in range(1, 6)]
    # an old win for C, outside a 30-day lookback from Sep 30
    + [_t(C, 100, "2026-07-01")]
    # after the pick (Oct): A fades, B holds, C and D win
    + [_t(A, -3, "2026-10-05"), _t(A, -3, "2026-10-13"),
       _t(B, 2, "2026-10-06"), _t(C, 6, "2026-10-07"), _t(D, 9, "2026-10-14")]
)


def test_ranks_only_on_the_lookback_and_scores_only_after_the_pick():
    out = farms.pick_and_hold({"kairos": KAIROS}, "2026-09-30", 30, 2, "kairos", min_trades=5, cap=2)
    assert out["picks"] == {A: "kairos", B: "kairos"}           # C's July win is outside the lookback
    assert out["audition"] == {"trades": 10, "win_rate": 100.0, "profit_factor": None, "pnl": 75.0}
    assert out["test"]["trades"] == 3 and out["test"]["pnl"] == -4.0
    # Baseline: 4 strategies traded after the pick, total +11; 2 picks -> 2/4 of it.
    assert out["baseline_strategies"] == 4 and out["baseline_test_pnl"] == 5.5
    # Two test weeks: Oct 5 (picks -1 vs base 2.5) and Oct 12 (picks -3 vs base 3).
    assert out["weeks"] == [{"week": "2026-10-05", "picks": -1.0, "baseline": 2.5},
                            {"week": "2026-10-12", "picks": -3.0, "baseline": 3.0}]
    assert out["weeks_beaten"] == 0
    rows = {r["strategy"]: r for r in out["rows"]}
    assert rows[A]["audition"]["pnl"] == 50.0 and rows[A]["test"]["pnl"] == -6.0


def test_the_line_is_continuous_and_the_baseline_starts_after_the_pick():
    out = farms.pick_and_hold({"kairos": KAIROS}, "2026-09-30", 30, 2, "kairos", min_trades=5)
    cut = next(i for i, d in enumerate(out["dates"]) if d > "2026-09-30")
    assert out["picks_line"][cut - 1] == 75.0                  # the audition's total
    assert out["picks_line"][-1] == 71.0                       # then -4 after the pick
    assert all(v is None for v in out["baseline_line"][:cut])
    assert out["baseline_line"][-1] == 5.5
    assert "2026-07-01" not in out["dates"]                    # the axis starts at the lookback


def test_a_frozen_list_is_scored_as_saved_not_re_ranked():
    out = farms.pick_and_hold({"kairos": KAIROS}, "2026-09-30", 30, 2, "kairos",
                              picks={C: "kairos", D: "kairos"})
    assert out["picks"] == {C: "kairos", D: "kairos"}
    assert out["test"]["pnl"] == 15.0 and out["weeks_beaten"] == 2


def test_both_farms_count_each_strategy_from_one_farm_only():
    tv = [_t(A, 20, f"2026-09-{d:02d}") for d in range(1, 6)] + [_t(A, 7, "2026-10-05")]
    out = farms.pick_and_hold({"kairos": KAIROS, "tv": tv}, "2026-09-30", 30, 2, "both", min_trades=5)
    assert out["picks"][A] == "tv"                             # TV's version has the better record
    a = next(r for r in out["rows"] if r["strategy"] == A)
    assert a["farm"] == "tv" and a["test"]["pnl"] == 7.0       # Kairos' -6 on A isn't counted


def test_a_pick_with_nothing_after_it_is_all_audition():
    out = farms.pick_and_hold({"kairos": KAIROS}, "2026-10-20", 60, 2, "kairos", min_trades=5)
    assert out["test"]["trades"] == 0 and out["weeks"] == [] and out["test_days"] == 0
    assert all(v is None for v in out["baseline_line"])


def test_params_clamp_a_future_date_and_unknown_farm():
    p = farms._pick_params({"pick_date": "2999-01-01", "farm": "nope", "lookback": "9999"})
    assert p["pick_date"] <= "2999-01-01" and p["pick_date"] != "2999-01-01"
    assert p["farm"] == "kairos" and p["lookback"] == 365


# ── endpoints ─────────────────────────────────────────────────────────────────

@pytest.fixture()
def client(monkeypatch):
    import app as a
    saved = {}
    monkeypatch.setattr(a, "_load_setting", lambda k, d=None: saved.get(k, d))
    monkeypatch.setattr(a, "_save_setting", lambda k, v: saved.__setitem__(k, v))
    monkeypatch.setattr(farms, "_farm_trips",
                        lambda num, f, t, full=False: (list(KAIROS) if num == "5" else [], None))
    a.app.config["TESTING"] = True
    return a.app.test_client(), saved


def test_save_freezes_the_servers_ranking_and_scores_it_later(client):
    c, saved = client
    r = c.post("/api/farms/forward_tests", json={"farm": "kairos", "pick_date": "2026-09-30",
                                                  "lookback": 30, "n": 2, "min_trades": 5,
                                                  "picks": {D: "kairos"}})      # ignored: the server ranks
    rec = r.get_json()["saved"]
    assert rec["picks"] == {A: "kairos", B: "kairos"}
    assert json.loads(saved[farms.FORWARD_TESTS_KEY])[0]["id"] == rec["id"]
    out = c.get("/api/farms/pick_hold?test_id=" + rec["id"]).get_json()
    assert out["saved"]["id"] == rec["id"] and out["test"]["pnl"] == -4.0
    assert c.get("/api/farms/forward_tests").get_json()["tests"][0]["name"].startswith("Kairos Farm top 2")
    c.post(f"/api/farms/forward_tests/{rec['id']}/delete")
    assert c.get("/api/farms/forward_tests").get_json()["tests"] == []
    assert c.get("/api/farms/pick_hold?test_id=" + rec["id"]).status_code == 404


def test_unsaved_pick_runs_read_only(client):
    c, saved = client
    out = c.get("/api/farms/pick_hold?farm=kairos&pick_date=2026-09-30&lookback=30&n=2&min_trades=5").get_json()
    assert out["picks"] == {A: "kairos", B: "kairos"} and not saved


def test_saving_an_empty_pick_is_refused(client):
    c, _ = client
    r = c.post("/api/farms/forward_tests", json={"pick_date": "2026-06-01", "lookback": 30, "n": 2})
    assert r.status_code == 400


def test_page_has_the_panel():
    html = open("templates/farms.html", encoding="utf-8").read()
    assert 'id="phPanel"' in html and "_loadPickHold();" in html and "chart-ph" in html
