"""Crew picks take the Tickers page's suggested trading window.

Once the crew has picked, each pick's suggested stop time (the Tickers page's "when
should you have stopped trading?") is worked out on its own farm — TV Farm for
[TV], Kairos Farm for [Kairos] — and the wire can set it as a strategy window on
the Crew books. Stops that held up in both halves start ticked; a window the trader
set by hand is never touched; crew windows on dropped or unticked picks go away.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

import app as a
from routes import crew as C

TV = "AAA_CAM_BREAKOUT_R3S3_V02_5MIN"
KA = "BBB_CAM_BREAKOUT_R3S3_V02_5MIN"
MINE = "CCC_CAM_BREAKOUT_R3S3_V02_5MIN"


def _t(slug, day, hhmm_et, pnl):
    """A round-trip entered at hhmm ET (EDT, so UTC = ET + 4h)."""
    h, m = map(int, hhmm_et.split(":"))
    return {"strategy": slug, "pnl": pnl, "entry_time": f"{day}T{h + 4:02d}:{m:02d}:00Z"}


def _morning_edge(slug, days=("2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24")):
    """Wins before noon, loses after, on every day — so a 12:00 stop helps both halves."""
    out = []
    for d in days:
        out += [_t(slug, d, "10:05", 5.0), _t(slug, d, "13:10", -3.0)]
    return out


# ── the calculation (a port of tickers.html stopCurve) ─────────────────────────

def test_a_morning_edge_suggests_a_noon_stop_in_both_halves():
    s = C._suggest_stop(_morning_edge(TV))
    assert s["stop"] == "10:30", "the first 30-min step after the last winner is the best stop"
    assert s["lift"] == 12.0 and s["skipped"] == 4 and s["n"] == 8
    assert s["both_halves"] is True
    assert s["older"]["lift"] > 0 and s["newer"]["lift"] > 0


def test_all_day_best_suggests_nothing():
    trades = [_t(TV, "2026-09-21", "10:05", 1.0), _t(TV, "2026-09-21", "15:00", 4.0)]
    s = C._suggest_stop(trades)
    assert s["stop"] is None and "all day" in s["reason"]


def test_one_half_only_is_flagged():
    # The afternoon loses only in the newer half.
    trades = [_t(TV, "2026-09-21", "10:05", 2.0), _t(TV, "2026-09-21", "13:10", 1.0),
              _t(TV, "2026-09-22", "10:05", 2.0), _t(TV, "2026-09-22", "13:10", 1.0),
              _t(TV, "2026-09-23", "10:05", 2.0), _t(TV, "2026-09-23", "13:10", -9.0),
              _t(TV, "2026-09-24", "10:05", 2.0), _t(TV, "2026-09-24", "13:10", -9.0)]
    s = C._suggest_stop(trades)
    assert s["stop"] is not None and s["both_halves"] is False


def test_entries_outside_the_session_are_ignored():
    assert C._suggest_stop([_t(TV, "2026-09-21", "08:00", 5.0)])["n"] == 0


# ── per pick, on its own farm ─────────────────────────────────────────────────

@pytest.fixture
def farms(monkeypatch):
    fills = {"1": _morning_edge(TV) + _morning_edge(MINE), "5": _morning_edge(KA)}
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "1": {"tag": "alpaca", "label": "TV Farm", "fills_fn": lambda: "1"},
        "5": {"tag": "alpaca5", "label": "Kairos Farm", "fills_fn": lambda: "5"}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda f, **k: {"closed_clean": fills[f]})
    store = {"m": {MINE: {"start": "09:35", "stop": "14:00", "mode": "within",
                          "accounts": ["alpaca2", "alpaca4"]}}}
    monkeypatch.setattr(a, "_strategy_stop_times", lambda: store["m"])
    monkeypatch.setattr(a, "_save_strategy_stop_times", lambda m: store.update(m=m))
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 0, raising=False)
    return store


PICKS = [{"strategy": TV, "entry": "tv"}, {"strategy": KA, "entry": "kairos"},
         {"strategy": MINE, "entry": "tv"}]


def test_each_pick_is_measured_on_its_own_farm(farms):
    w = C._pick_windows(a, PICKS)
    assert w[TV]["farm"] == "TV Farm" and w[KA]["farm"] == "Kairos Farm"
    assert w[TV]["suggest"]["stop"] == "10:30" and w[TV]["default"] is True
    assert w[KA]["suggest"]["n"] == 8, "the Kairos pick reads Kairos Farm fills"


def test_a_hand_set_window_is_locked(farms):
    w = C._pick_windows(a, PICKS)[MINE]
    assert w["locked"] is True and w["default"] is False
    assert w["existing"]["stop"] == "14:00"


def test_apply_sets_crew_windows_and_keeps_yours(farms):
    res = C._apply_pick_windows(a, PICKS, {TV: {"start": "09:35", "stop": "10:30"},
                                           MINE: {"start": "09:35", "stop": "10:30"}})
    m = farms["m"]
    assert res["set"] == [f"{TV} 09:35-10:30"] and res["kept_yours"] == [MINE]
    assert m[TV]["source"] == "crew" and m[TV]["accounts"] == ["alpaca4"]
    assert m[TV]["mode"] == "within" and m[TV]["basis"]["book"] == "TV Farm"
    assert m[MINE]["stop"] == "14:00", "the trader's own window is untouched"


def test_crew_live_gets_the_window_when_the_wire_mirrors_to_it(farms, monkeypatch):
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca6", {"tag": "alpaca6"})
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 500, raising=False)
    C._apply_pick_windows(a, PICKS, {TV: {"start": "09:35", "stop": "10:30"}})
    assert farms["m"][TV]["accounts"] == ["alpaca4", "alpaca6"]


def test_unticked_and_dropped_picks_lose_their_crew_window(farms):
    C._apply_pick_windows(a, PICKS, {TV: {"start": "09:35", "stop": "10:30"},
                                     KA: {"start": "09:35", "stop": "10:30"}})
    res = C._apply_pick_windows(a, [{"strategy": KA, "entry": "kairos"}], {})
    assert sorted(res["removed"]) == sorted([TV, KA])
    assert set(farms["m"]) == {MINE}, "only the hand-set window is left"


def test_bad_or_foreign_windows_are_refused(farms):
    res = C._apply_pick_windows(a, PICKS, {TV: {"start": "11:00", "stop": "10:30"},
                                           "ZZZ_CAM_BREAKOUT_R3S3_V02_5MIN": {"stop": "12:00"}})
    assert len(res["refused"]) == 2 and TV not in farms["m"]


# ── the wire endpoint ─────────────────────────────────────────────────────────

@pytest.fixture
def wire(tmp_path, monkeypatch, farms):
    db = tmp_path / "w.db"
    shutil.copy("trades.db", db)
    c = sqlite3.connect(db)
    c.execute("DELETE FROM routing_rules"); c.execute("DELETE FROM crew_reports")
    card = "<br>".join(f"{i + 1}. {s} — both [TV]" for i, s in enumerate([TV, MINE]))
    c.execute("INSERT INTO crew_reports (week, created_at, report) VALUES (?,?,?)",
              ("2026-W44", "2026-10-30 21:00:00",
               f"| Decision | Recommendation |\n|---|---|\n| Top 2 to run | {card} |\n"))
    c.commit(); c.close()

    def _db():
        k = sqlite3.connect(db); k.row_factory = sqlite3.Row
        return k
    monkeypatch.setattr(a, "get_db", _db)
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca4", {"tag": "alpaca4", "broker": None})
    monkeypatch.setattr(a, "_fetch_alpaca_last_prices", lambda tks: {}, raising=False)
    a.app.config["TESTING"] = True
    return farms


def test_preview_shows_each_picks_window(wire):
    with a.app.test_client() as cl:
        d = cl.get("/api/crew/wire_preview").get_json()
    pw = d["pick_windows"]
    assert pw["books"] == ["alpaca4"] and pw["days"] == C.CREW_WINDOW_DAYS
    assert pw["windows"][TV]["default"] is True and pw["windows"][MINE]["locked"] is True


def test_wire_applies_only_when_windows_are_sent(wire):
    with a.app.test_client() as cl:
        d = cl.post("/api/crew/wire_to_router", json={}).get_json()
        assert d["windows"] is None and TV not in wire["m"], "no map sent, nothing changed"
        d = cl.post("/api/crew/wire_to_router",
                    json={"windows": {TV: {"start": "09:35", "stop": "10:30"}}}).get_json()
    assert d["windows"]["set"] == [f"{TV} 09:35-10:30"]
    assert wire["m"][TV]["source"] == "crew"


def test_crew_page_has_the_window_column():
    html = open("templates/crew.html", encoding="utf-8").read()
    assert "_windowCell(p.strategy" in html and "body.windows = _chosenWindows()" in html
    assert "class=\"crewWin\"" in html
