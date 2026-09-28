"""Hours gate: in-window vs out-of-window, priced on the ungated farms.

The curated books refuse to trade outside their windows. The farms don't — they
run all day with no gates at all. So a farm's out-of-window fills are the natural
control for the hours gate: same strategy, same period, the only difference being
the hours the books refuse.

That turns "should this name trade all day?" into an answerable question, and it
feeds the per-ticker exemption directly.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a
from routes import crew as C

WIN = [("09:35", "10:00"), ("12:00", "15:55")]


def _rt(slug, hhmm_et, pnl, day="2026-09-21"):
    h, m = hhmm_et.split(":")
    return {"strategy": slug, "ticker": slug.split("_")[0], "side": "LONG", "date": day,
            "entry_price": 100.0, "qty": 10, "pnl": pnl,
            "entry_time": f"{day}T{int(h) + 4:02d}:{m}:00Z",      # ET -> UTC in Sept
            "exit_time":  f"{day}T{int(h) + 4:02d}:{m}:30Z"}


@pytest.fixture
def farms(monkeypatch):
    state = {"1": [], "5": []}
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "1": {"tag": "alpaca",  "label": "TV Farm",     "broker": object(), "fills_fn": lambda: "1"},
        "5": {"tag": "alpaca5", "label": "Kairos Farm", "broker": object(), "fills_fn": lambda: "5"}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda f, **k: {"closed_clean": state[f]})
    monkeypatch.setattr(a, "_build_signal_lookup_for_alpaca", lambda: {})
    monkeypatch.setattr(a, "_shared_hours_windows", lambda key: WIN)
    monkeypatch.setattr(C, "_entry_source_by_strategy", lambda *a_, **k: {})
    return state


def _split(days=45):
    return C._hours_split_data(days)


def _by(d):
    return {r["strategy"]: r for r in d["strategies"]}


# ── the split itself ────────────────────────────────────────────────────────

def test_entries_are_bucketed_by_ET_not_UTC(farms):
    """09:45 ET is inside the window; the same instant in UTC (13:45) is not. Get
    this wrong and every trade lands in the wrong bucket."""
    farms["1"] = [_rt("X_CAM_BREAKOUT_R3S3", "09:45", 5.0)]
    r = _by(_split())["X_CAM_BREAKOUT_R3S3"]
    assert r["inside"]["trades"] == 1 and r["outside"]["trades"] == 0


def test_the_midday_gap_counts_as_outside(farms):
    """The real config pauses 10:00-12:00. That gap is exactly what an exemption
    would re-open, so it has to be measured."""
    farms["1"] = [_rt("X_CAM_BREAKOUT_R3S3", "11:00", 5.0)]
    assert _by(_split())["X_CAM_BREAKOUT_R3S3"]["outside"]["trades"] == 1


def test_a_name_that_earns_more_outside_is_an_all_day_candidate(farms):
    n = C.HOURS_SPLIT_MIN_SIDE
    farms["1"] = ([_rt("SPY_CAM_BREAKOUT_R3S3", "09:45", 2.0)  for _ in range(n)] +
                  [_rt("SPY_CAM_BREAKOUT_R3S3", "10:45", 20.0) for _ in range(n)])
    d = _split()
    r = _by(d)["SPY_CAM_BREAKOUT_R3S3"]
    assert r["recommend"] == "all_day"
    assert "BETTER hours" in r["verdict"]
    assert d["candidates"] == ["SPY_CAM_BREAKOUT_R3S3"]


def test_a_name_that_bleeds_outside_keeps_its_window(farms):
    n = C.HOURS_SPLIT_MIN_SIDE
    farms["1"] = ([_rt("NVDA_CAM_BREAKOUT_R3S3", "09:45", 15.0) for _ in range(n)] +
                  [_rt("NVDA_CAM_BREAKOUT_R3S3", "11:00", -9.0) for _ in range(n)])
    d = _split()
    r = _by(d)["NVDA_CAM_BREAKOUT_R3S3"]
    assert r["recommend"] == "keep_window"
    assert "earns its keep" in r["verdict"]
    assert d["candidates"] == []


def test_profitable_outside_but_weaker_is_not_a_recommendation(farms):
    """The window also holds position count down, so merely making money outside
    is not a reason to widen it."""
    n = C.HOURS_SPLIT_MIN_SIDE
    farms["1"] = ([_rt("X_CAM_BREAKOUT_R3S3", "09:45", 20.0) for _ in range(n)] +
                  [_rt("X_CAM_BREAKOUT_R3S3", "10:45", 3.0)  for _ in range(n)])
    r = _by(_split())["X_CAM_BREAKOUT_R3S3"]
    assert r["recommend"] == "either" and r["outside"]["per_trade"] > 0


# ── the guard that matters most ─────────────────────────────────────────────

def test_a_thin_side_gets_no_verdict_however_large_the_gap(farms):
    """The failure this prevents: recommending an all-day exemption off three lucky
    afternoon fills. A big delta on a tiny sample is the most tempting bad call."""
    n = C.HOURS_SPLIT_MIN_SIDE
    farms["1"] = ([_rt("GLD_CAM_REVERSAL_R3S3", "12:30", 2.0) for _ in range(n)] +
                  [_rt("GLD_CAM_REVERSAL_R3S3", "10:30", 99.0) for _ in range(2)])
    d = _split()
    r = _by(d)["GLD_CAM_REVERSAL_R3S3"]
    assert r["thin"] is True and r["recommend"] is None
    assert r["delta_per_trade"] > 50, "the gap is huge — and still not actionable"
    assert d["candidates"] == []


# ── mechanism pairing ───────────────────────────────────────────────────────

def test_each_name_is_priced_against_its_own_mechanism_farm(farms, monkeypatch):
    """Comparing a TV pick to engine fills would measure the mechanism, not the
    hours — the same reasoning the gate-cost view already uses."""
    n = C.HOURS_SPLIT_MIN_SIDE
    monkeypatch.setattr(C, "_entry_source_by_strategy",
                        lambda *a_, **k: {"X_CAM_BREAKOUT_R3S3": "kairos"})
    farms["1"] = [_rt("X_CAM_BREAKOUT_R3S3", "09:45", -50.0) for _ in range(n)]
    farms["5"] = ([_rt("X_CAM_BREAKOUT_R3S3", "09:45", 5.0)  for _ in range(n)] +
                  [_rt("X_CAM_BREAKOUT_R3S3", "10:45", 15.0) for _ in range(n)])
    r = _by(_split())["X_CAM_BREAKOUT_R3S3"]
    assert r["mechanism"] == "kairos" and r["farm"] == "Kairos Farm"
    assert r["inside"]["per_trade"] == 5.0, "the TV farm's fills must not leak in"


# ── failure modes ───────────────────────────────────────────────────────────

def test_no_windows_configured_is_an_explicit_refusal(farms, monkeypatch):
    monkeypatch.setattr(a, "_shared_hours_windows", lambda key: [])
    assert "nothing to compare" in _split()["error"]


def test_an_unreadable_farm_does_not_take_the_whole_view_down(farms, monkeypatch):
    def _boom(f, **k):
        if f == "1":
            raise RuntimeError("fills down")
        return {"closed_clean": [_rt("K_CAM_BREAKOUT_R3S3", "09:45", 5.0)]}
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", _boom)
    d = _split()
    assert "error" not in d
    assert "K_CAM_BREAKOUT_R3S3" in _by(d)


def test_the_caveats_name_what_this_evidence_cannot_settle(farms):
    farms["1"] = [_rt("X_CAM_BREAKOUT_R3S3", "09:45", 1.0)]
    cav = " ".join(_split()["caveats"])
    assert "ungated" in cav
    assert "day-type, RVOL or strikes" in cav, "an out-of-window edge is not proof"


# ── the endpoint and the prompt ─────────────────────────────────────────────

def test_the_endpoint_returns_400_when_there_is_nothing_to_compare(farms, monkeypatch):
    monkeypatch.setattr(a, "_shared_hours_windows", lambda key: [])
    a.app.config["TESTING"] = True
    with a.app.test_request_context("/api/crew/hours_split"):
        resp = C.api_crew_hours_split()
    assert resp[1] == 400


def test_the_prompt_asks_for_a_recommendation_and_bounds_it():
    src = open("routes/crew.py", encoding="utf-8").read()
    i = src.index("Trading Hours — All Day or Gated?")
    block = src[i:i + 1600]
    assert "MORE per trade outside" in block, "the bar for recommending an exemption"
    assert "merely profitable outside is not a reason" in block
    assert "ALL-DAY CANDIDATES" in block, "a machine-readable line to act on"
    assert "per TICKER" in block, "exemptions apply per ticker, not per strategy"


def test_the_hours_block_reaches_the_prompt():
    src = open("routes/crew.py", encoding="utf-8").read()
    assert "hours_split_block = _fmt_hours_split()" in src
    assert "{hours_split_block}" in src
