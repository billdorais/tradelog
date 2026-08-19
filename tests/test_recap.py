"""Weekly Crew Paper recap (/api/recap).

Assembles a filmable episode outline: window, book totals, winners/losers, the
trade of the week, which gates are ACTUALLY live, the crew's out-of-sample
scorecard, and talking points with the numbers already substituted.

The gate strip matters most. RVOL is not wired to Crew Paper at all
(RVOL_GATE_ACCOUNTS is {alpaca2, alpaca3}), so toggling it globally changes
nothing here — the strip exists so a script never narrates a gate that is not
running on this book.
"""
from __future__ import annotations

import datetime as dt
import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


def _rt(strat, pnl, date, ticker="NVDA", side="LONG"):
    return {"strategy": strat, "ticker": ticker, "pnl": pnl, "qty": 10,
            "entry_price": 100.0, "exit_price": 100.0 + pnl / 10, "side": side,
            "date": date, "entry_time": f"{date}T14:35:00Z",
            "exit_time": f"{date}T14:50:00Z", "exit_reason": "Trail"}


def _last_week_monday():
    today = dt.date.today()
    return today - dt.timedelta(days=today.weekday() + 7)


@pytest.fixture
def crew(monkeypatch):
    """A configured Crew Paper book. monkeypatch, not assignment, so the registry
    cannot leak into other test modules."""
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "4": {"tag": "alpaca4", "label": "Crew Paper",
              "broker": object(), "fills_fn": lambda: []},
    })


def _client():
    a.app.config["TESTING"] = True
    return a.app.test_client()


def _with(monkeypatch, rts):
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda *A, **K: {"closed_clean": rts})


def test_unconfigured_book_is_refused_not_silently_empty():
    r = _client().get("/api/recap")
    assert r.status_code == 400
    assert "acct4" in r.get_json()["error"]


def test_defaults_to_the_last_completed_week(monkeypatch, crew):
    """You record a recap ABOUT the week that just ended, so that is the default."""
    _with(monkeypatch, [])
    d = _client().get("/api/recap").get_json()
    mon = _last_week_monday()
    assert d["from"] == mon.isoformat()
    assert d["to"] == (mon + dt.timedelta(days=6)).isoformat()
    assert d["week_label"].startswith("Last week")
    assert dt.date.fromisoformat(d["from"]).weekday() == 0      # Monday
    assert dt.date.fromisoformat(d["to"]).weekday() == 6        # Sunday


def test_week_this_selects_the_current_week(monkeypatch, crew):
    _with(monkeypatch, [])
    d = _client().get("/api/recap?week=this").get_json()
    today = dt.date.today()
    assert d["from"] == (today - dt.timedelta(days=today.weekday())).isoformat()
    assert d["week_label"].startswith("This week")


def test_book_winners_losers_and_trade_of_the_week(monkeypatch, crew):
    mon = _last_week_monday()
    d1, d2 = mon.isoformat(), (mon + dt.timedelta(days=2)).isoformat()
    _with(monkeypatch, [
        _rt("NVDA_CAM_BREAKOUT_R3S3_V02_5MIN", 40.0, d1),
        _rt("NVDA_CAM_BREAKOUT_R3S3_V02_5MIN", 12.0, d2),
        _rt("BA_CAM_REVERSAL_R3S3_V02_5MIN", -25.0, d1, ticker="BA", side="SHORT"),
    ])
    d = _client().get("/api/recap").get_json()
    assert d["book"]["pnl"] == 27.0 and d["book"]["trades"] == 3
    assert [w["name"] for w in d["winners"]] == ["NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"]
    assert [l["name"] for l in d["losers"]]  == ["BA_CAM_REVERSAL_R3S3_V02_5MIN"]
    assert d["winners"][0]["pnl"] == 52.0 and d["winners"][0]["ticker"] == "NVDA"
    # Trade of the week is a single round-trip, not the strategy total.
    assert d["best_trade"]["pnl"] == 40.0 and d["best_trade"]["date"] == d1
    assert d["worst_trade"]["pnl"] == -25.0
    # Day rollup drives the best/worst-day tiles: d1 = 40 - 25 = 15, d2 = 12.
    assert d["book"]["best_day"] == [d1, 15.0]
    assert d["book"]["worst_day"] == [d2, 12.0]


def test_gate_strip_reports_rvol_as_not_wired_to_crew(monkeypatch, crew):
    """The reason this panel exists: RVOL only ever gated acct2/acct3, so turning
    it on globally must NOT show as live on Crew Paper."""
    _with(monkeypatch, [])
    monkeypatch.setattr(a, "RVOL_GATE_ENABLED", True)
    monkeypatch.setattr(a, "RVOL_GATE_ACCOUNTS", {"alpaca2", "alpaca3"})
    gates = {g["gate"]: g for g in _client().get("/api/recap").get_json()["gates"]}
    assert gates["RVOL"]["on"] is False
    assert "not wired" in gates["RVOL"]["detail"]


def test_gate_strip_follows_live_config(monkeypatch, crew):
    _with(monkeypatch, [])
    monkeypatch.setattr(a, "DAYTYPE_GATE_ENABLED", True)
    monkeypatch.setattr(a, "DAYTYPE_GATE_ACCOUNTS", {"alpaca4"})
    monkeypatch.setattr(a, "STRIKES_ENABLED", False)
    gates = {g["gate"]: g for g in _client().get("/api/recap").get_json()["gates"]}
    assert gates["Day-type"]["on"] is True
    assert gates["Strikes"]["on"] is False
    # reversal_side "long" on acct4 => the reversal-side gate is live
    assert gates["Reversal side"]["on"] is True


def test_script_lines_are_readable_and_disclose_paper(monkeypatch, crew):
    mon = _last_week_monday().isoformat()
    _with(monkeypatch, [_rt("NVDA_CAM_BREAKOUT_R3S3_V02_5MIN", 40.0, mon)])
    d = _client().get("/api/recap").get_json()
    segs = [x["segment"] for x in d["script"]]
    cold = next(x["line"] for x in d["script"] if x["segment"].startswith("0"))
    assert "on paper" in cold, "the cold open must disclose paper trading"
    assert "$40.00" in cold
    assert any(s.startswith("3") for s in segs)   # the Refusal segment is always present
    assert any(s.startswith("5") for s in segs)   # and the falsifiable claim


def test_empty_week_still_returns_a_usable_shell(monkeypatch, crew):
    """A quiet week should not 500 — you still film an episode."""
    _with(monkeypatch, [])
    d = _client().get("/api/recap").get_json()
    assert d["book"]["pnl"] == 0.0 and d["book"]["trades"] == 0
    assert d["winners"] == [] and d["losers"] == []
    assert d["best_trade"] is None and d["worst_trade"] is None
    assert d["script"] and d["gates"]
