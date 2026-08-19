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


@pytest.mark.parametrize("period,label", [
    ("this_month", "This month"),
    ("last_month", "Last month"),
])
def test_month_periods_span_the_full_calendar_month(monkeypatch, crew, period, label):
    _with(monkeypatch, [])
    d = _client().get(f"/api/recap?period={period}").get_json()
    frm, to = dt.date.fromisoformat(d["from"]), dt.date.fromisoformat(d["to"])
    assert frm.day == 1                                   # starts on the 1st
    assert (to + dt.timedelta(days=1)).day == 1           # ends on month-end
    assert frm.month == to.month
    assert d["week_label"].startswith(label)
    assert d["period"] == period
    today = dt.date.today()
    if period == "this_month":
        assert (frm.year, frm.month) == (today.year, today.month)
    else:
        prev = today.replace(day=1) - dt.timedelta(days=1)
        assert (frm.year, frm.month) == (prev.year, prev.month)


@pytest.mark.parametrize("year,month,last_day", [
    (2026, 1, 31), (2026, 2, 28), (2026, 3, 31), (2026, 4, 30),
    (2024, 2, 29),                                    # leap year
    (2026, 11, 30), (2026, 12, 31),                   # December must roll the YEAR
])
def test_month_end_arithmetic_survives_short_months(year, month, last_day):
    """The endpoint finds month-end with `day=28 + 4 days, back to the 1st, minus a
    day` rather than a lookup table. Verify that formula directly — the endpoint
    test above can only ever exercise whatever month today happens to be."""
    first = dt.date(year, month, 1)
    nxt_first = (first.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    end = nxt_first - dt.timedelta(days=1)
    assert end == dt.date(year, month, last_day)
    assert end.month == month and end.year == year


def test_explicit_from_to_still_wins(monkeypatch, crew):
    _with(monkeypatch, [])
    d = _client().get("/api/recap?period=this_month&from=2026-03-02&to=2026-03-08").get_json()
    assert (d["from"], d["to"]) == ("2026-03-02", "2026-03-08")


def test_week_param_still_works_for_back_compat(monkeypatch, crew):
    _with(monkeypatch, [])
    assert _client().get("/api/recap?week=this").get_json()["period"] == "this_week"
    assert _client().get("/api/recap?week=last").get_json()["period"] == "last_week"


def test_curve_is_cumulative_by_trade_and_reconciles_with_book_pnl(monkeypatch, crew):
    """The equity curve at the top of the page. One point per CLOSED trade ordered
    by exit — the same shape the dashboard draws — so its last point must equal the
    headline P&L or the chart and the tile would disagree on camera."""
    mon = _last_week_monday()
    d0, d1, d2 = [(mon + dt.timedelta(days=i)).isoformat() for i in (0, 1, 2)]
    _with(monkeypatch, [
        _rt("A_CAM_BREAKOUT_R3S3_V02_5MIN", -30.0, d0),
        _rt("B_CAM_REVERSAL_R3S3_V02_5MIN", -40.0, d1, ticker="BA"),
        _rt("A_CAM_BREAKOUT_R3S3_V02_5MIN",  90.0, d2),
    ])
    d = _client().get("/api/recap").get_json()
    curve = d["book"]["curve"]
    assert [p["cum"] for p in curve] == [-30.0, -70.0, 20.0]      # running total
    assert curve[-1]["cum"] == d["book"]["pnl"]                    # chart == headline
    assert curve[0]["ticker"] and curve[0]["strategy"]             # tooltip fields


def test_each_strategy_is_charted_on_its_biggest_move_session(monkeypatch, crew):
    """Six charts = top 3 + bottom 3, one session each. A name that traded several
    days would otherwise bury the page, so each gets the session holding its
    largest-magnitude trade."""
    mon = _last_week_monday()
    d0, d2 = mon.isoformat(), (mon + dt.timedelta(days=2)).isoformat()
    _with(monkeypatch, [
        _rt("A_CAM_BREAKOUT_R3S3_V02_5MIN", -30.0, d0),   # smaller move
        _rt("A_CAM_BREAKOUT_R3S3_V02_5MIN",  90.0, d2),   # the one to chart
        _rt("B_CAM_REVERSAL_R3S3_V02_5MIN", -40.0, d0, ticker="BA"),
    ])
    d = _client().get("/api/recap").get_json()
    win = next(w for w in d["winners"] if w["name"].startswith("A_"))
    assert win["chart_date"] == d2      # biggest magnitude, not the first or last
    loss = next(l for l in d["losers"] if l["name"].startswith("B_"))
    assert loss["chart_date"] == d0


def test_book_carries_profit_factor_drawdown_and_averages(monkeypatch, crew):
    """The three tiles added next to P&L. Conventions match _strategy_breakdown so
    the recap cannot disagree with the Explorer about the same trades."""
    mon = _last_week_monday()
    d = [(mon + dt.timedelta(days=i)).isoformat() for i in range(5)]
    _with(monkeypatch, [
        _rt("A_CAM_BREAKOUT_R3S3_V02_5MIN",  100.0, d[0]),
        _rt("A_CAM_BREAKOUT_R3S3_V02_5MIN",  -60.0, d[1]),
        _rt("A_CAM_BREAKOUT_R3S3_V02_5MIN",   20.0, d[2]),
        _rt("A_CAM_BREAKOUT_R3S3_V02_5MIN",  -50.0, d[3]),
        _rt("A_CAM_BREAKOUT_R3S3_V02_5MIN",   40.0, d[4]),
    ])
    b = _client().get("/api/recap").get_json()["book"]
    assert (b["gross_win"], b["gross_loss"]) == (160.0, 110.0)
    assert b["profit_factor"] == round(160.0 / 110.0, 2)
    assert (b["win_count"], b["loss_count"]) == (3, 2)
    assert b["avg_win"] == round(160.0 / 3, 2)
    assert b["avg_loss"] == -55.0                      # negative, like the raw P&L
    # cum runs 100, 40, 60, 10, 50 -> peak 100, trough 10 -> -90, and it is NEGATIVE
    assert b["max_drawdown"] == -90.0


def test_profit_factor_is_null_not_zero_without_losses(monkeypatch, crew):
    """None means "no losing trades yet", which the tile renders as infinity. Zero
    would read as the exact opposite — a strategy that only loses."""
    mon = _last_week_monday().isoformat()
    _with(monkeypatch, [_rt("A_CAM_BREAKOUT_R3S3_V02_5MIN", 30.0, mon)])
    b = _client().get("/api/recap").get_json()["book"]
    assert b["profit_factor"] is None
    assert b["avg_loss"] is None and b["loss_count"] == 0
    assert b["max_drawdown"] == 0.0


def test_scratch_trade_counts_as_a_loss(monkeypatch, crew):
    """A 0.00 round-trip is a loss everywhere else in the app; keep that here or
    win_rate on the recap would drift from the Explorer's."""
    mon = _last_week_monday()
    d0, d1 = mon.isoformat(), (mon + dt.timedelta(days=1)).isoformat()
    _with(monkeypatch, [
        _rt("A_CAM_BREAKOUT_R3S3_V02_5MIN", 10.0, d0),
        _rt("A_CAM_BREAKOUT_R3S3_V02_5MIN",  0.0, d1),
    ])
    b = _client().get("/api/recap").get_json()["book"]
    assert (b["win_count"], b["loss_count"]) == (1, 1)
    assert b["win_rate"] == 50.0


def test_empty_period_stats_do_not_crash(monkeypatch, crew):
    _with(monkeypatch, [])
    b = _client().get("/api/recap").get_json()["book"]
    assert b["profit_factor"] is None and b["avg_win"] is None and b["avg_loss"] is None
    assert b["max_drawdown"] == 0.0 and b["curve"] == []


@pytest.fixture
def w32_scorecard(monkeypatch):
    """The real 2026-W32 card: 18 picks, 8 traded, net $334.41."""
    import routes.crew as rc
    raw = [("NVDA", 2, 249.38), ("AAPL4", 2, -27.54), ("HOOD", 0, None), ("IONQ", 2, 61.82),
           ("IWMR", 0, None), ("SMHR", 0, None), ("GOOG", 2, -37.96), ("AAPL3", 1, -35.76),
           ("AVGO", 0, None), ("MS", 2, 22.09), ("SPCX", 0, None), ("GLD", 0, None),
           ("MSFT", 0, None), ("AMZN", 1, 15.64), ("PLTR", 2, 86.74), ("IWMB", 0, None),
           ("UBER", 0, None), ("NEM", 0, None)]
    picks = [{"strategy": n, "side": "both", "trades": t, "pnl": p} for n, t, p in raw]
    traded = [p for p in picks if p["pnl"] is not None]
    monkeypatch.setattr(rc, "_pick_scorecard", lambda *A, **K: {
        "report_week": "2026-W32", "since": "2026-08-05", "n_picks": len(picks),
        "n_traded": len(traded), "n_positive": sum(1 for p in traded if p["pnl"] > 0),
        "total_pnl": round(sum(p["pnl"] for p in traded), 2), "picks": picks, "caveat": "x"})


def test_scorecard_summary_cards(monkeypatch, crew, w32_scorecard):
    _with(monkeypatch, [])
    m = _client().get("/api/recap").get_json()["scorecard"]["summary"]
    assert (m["n_picks"], m["n_traded"], m["n_untraded"]) == (18, 8, 10)
    assert (m["n_positive"], m["n_negative"]) == (5, 3)
    assert (m["trades_positive"], m["trades_negative"]) == (9, 5)
    # The two halves must reconcile with the headline net.
    assert round(m["pnl_positive"] + m["pnl_negative"], 2) == 334.41
    assert m["hit_rate"] == 62.5          # 5 of the 8 that fired
    assert m["activation"] == 44.4        # only 8 of 18 ever fired


def test_top_share_exposes_concentration(monkeypatch, crew, w32_scorecard):
    """The number the table hides: one name was three-quarters of the net."""
    _with(monkeypatch, [])
    m = _client().get("/api/recap").get_json()["scorecard"]["summary"]
    assert m["top_pick"] == "NVDA" and m["top_pick_pnl"] == 249.38
    assert m["top_share"] == 74.6          # 249.38 / 334.41


def test_top_share_is_null_when_the_book_is_not_net_positive(monkeypatch, crew):
    """Share-of-net is undefined against a zero or negative denominator — return
    None rather than a nonsense percentage."""
    import routes.crew as rc
    picks = [{"strategy": "A", "side": "both", "trades": 1, "pnl": 10.0},
             {"strategy": "B", "side": "both", "trades": 1, "pnl": -40.0}]
    monkeypatch.setattr(rc, "_pick_scorecard", lambda *A, **K: {
        "report_week": "W", "since": "d", "n_picks": 2, "n_traded": 2, "n_positive": 1,
        "total_pnl": -30.0, "picks": picks, "caveat": ""})
    _with(monkeypatch, [])
    m = _client().get("/api/recap").get_json()["scorecard"]["summary"]
    assert m["top_share"] is None
    assert m["n_untraded"] == 0 and m["hit_rate"] == 50.0


def test_summary_absent_when_there_is_no_scorecard(monkeypatch, crew):
    import routes.crew as rc
    monkeypatch.setattr(rc, "_pick_scorecard", lambda *A, **K: {})
    _with(monkeypatch, [])
    assert _client().get("/api/recap").get_json()["scorecard"] == {}
