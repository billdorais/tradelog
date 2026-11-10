"""Daily journal (routes/journal_daily.py): rule-based day labels, per-strategy day
rows, regime splits, flip detection, and the snapshot → API round trip."""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import sqlite3

import pytest

import routes.journal_daily as jd

S = "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"


def _bars(rows):
    return [{"date": d, "open": o, "high": h, "low": l, "close": c} for d, o, h, l, c in rows]


# ── bar features and labels ──────────────────────────────────────────────────

def test_bar_features_needs_prior_close_and_measures_gap_body_range():
    f = jd.bar_features(_bars([
        ("2026-11-04", 100, 101, 99, 100),
        ("2026-11-05", 101, 104, 100.5, 103.5),   # gap +1%, body 2.5 of range 3.5
    ]))
    assert "2026-11-04" not in f
    d = f["2026-11-05"]
    assert d["gap_pct"] == pytest.approx(1.0)
    assert d["ret_pct"] == pytest.approx(3.5)
    assert d["range_pct"] == pytest.approx(3.5)
    assert d["body_ratio"] == pytest.approx(2.5 / 3.5, abs=1e-3)
    assert d["direction"] == "up"
    assert d["range_x"] is None                     # not enough history for a baseline


def test_bar_features_range_x_against_prior_median():
    rows = [("2026-09-%02d" % (i + 1), 100, 101, 99, 100) for i in range(12)]   # 2% ranges
    rows.append(("2026-09-15", 100, 102, 98, 100))                              # 4% range
    f = jd.bar_features(_bars(rows))
    assert f["2026-09-15"]["range_x"] == pytest.approx(2.0)


@pytest.mark.parametrize("spy,vix,expect", [
    ({"body_ratio": 0.8, "direction": "up", "gap_pct": 0.1, "range_x": 1.0}, 14.0,
     {"trend": "trend-up", "gap": "no-gap", "range": "normal-range", "vix": "vix-low"}),
    ({"body_ratio": 0.7, "direction": "down", "gap_pct": -0.9, "range_x": 1.5}, 22.0,
     {"trend": "trend-down", "gap": "gap-down", "range": "wide-range", "vix": "vix-high"}),
    ({"body_ratio": 0.2, "direction": "up", "gap_pct": 0.6, "range_x": 0.5}, 17.0,
     {"trend": "chop", "gap": "gap-up", "range": "narrow-range", "vix": "vix-mid"}),
    ({"body_ratio": 0.45, "direction": "up", "gap_pct": 0.0, "range_x": None}, None,
     {"trend": "mixed", "gap": "no-gap"}),
])
def test_day_labels_follow_the_fixed_rules(spy, vix, expect):
    assert jd.day_labels(spy, vix) == expect


def test_vix_label_boundaries_are_inclusive_at_high():
    assert jd.day_labels(None, 15.0) == {"vix": "vix-mid"}
    assert jd.day_labels(None, 20.0) == {"vix": "vix-high"}


# ── calendar ──────────────────────────────────────────────────────────────────

def test_third_friday_and_events():
    assert jd.third_friday(2026, 11).isoformat() == "2026-11-20"
    assert jd.third_friday(2026, 10).isoformat() == "2026-10-16"
    ev = jd.calendar_events("2026-10-16", manual=[{"date": "2026-10-16", "kind": "fed", "label": "FOMC decision"},
                                                  {"date": "2026-10-17", "kind": "event", "label": "other day"}],
                            earnings_by_ticker={"NVDA": {"2026-10-16"}, "AAPL": {"2026-10-30"}})
    assert ev == [{"kind": "earnings", "label": "NVDA earnings"},
                  {"kind": "fed", "label": "FOMC decision"},
                  {"kind": "opex", "label": "Monthly OPEX"}]


# ── strategy-day rows ─────────────────────────────────────────────────────────

def test_aggregate_strategy_days_buckets_by_et_entry_day_and_sums_pct():
    trips = [
        {"strategy": S.lower(), "entry_time": "2026-11-05T14:00:00Z", "exit_time": "2026-11-05T14:10:00Z",
         "pnl": 10.0, "qty": 10, "entry_price": 100.0},          # +1%
        {"strategy": S, "entry_time": "2026-11-05T19:55:00Z", "exit_time": "2026-11-06T01:00:00Z",
         "pnl": -5.0, "qty": 10, "entry_price": 100.0},          # -0.5%, still the 5th in ET
        {"strategy": S, "entry_time": "2026-11-06T15:00:00Z", "exit_time": "2026-11-06T15:05:00Z",
         "pnl": 2.0, "qty": 0, "entry_price": 100.0},            # size unknown → no %
    ]
    rows = jd.aggregate_strategy_days(trips)
    assert [(r["date"], r["trades"], r["wins"]) for r in rows] == [("2026-11-05", 2, 1), ("2026-11-06", 1, 1)]
    d5 = rows[0]
    assert d5["ticker"] == "NVDA"
    assert d5["pnl"] == pytest.approx(5.0)
    assert d5["pct_n"] == 2 and d5["pct_sum"] == pytest.approx(0.5)
    assert d5["pct_sq_sum"] == pytest.approx(1.25)
    assert rows[1]["pct_n"] == 0


def test_ticker_of_handles_both_name_styles():
    assert jd._ticker_of(S) == "NVDA"
    assert jd._ticker_of("CAM_PLTR_BREAKOUT_R4S4_05MIN") == "PLTR"


def test_summarize_mean_and_t():
    rows = [{"date": "a", "trades": 4, "wins": 3, "pnl": 3.0, "pct_n": 4, "pct_sum": 0.8, "pct_sq_sum": 0.2},
            {"date": "b", "trades": 1, "wins": 1, "pnl": 1.0, "pct_n": 1, "pct_sum": 0.2, "pct_sq_sum": 0.04}]
    s = jd.summarize(rows)
    assert s["trades"] == 5 and s["wins"] == 4 and s["days"] == 2
    assert s["mean_pct"] == pytest.approx(0.2)
    var = (0.24 - 5 * 0.04) / 4
    assert s["t"] == pytest.approx(round(0.2 / (var / 5) ** 0.5, 2))
    assert jd.summarize([dict(rows[1], trades=4, pct_n=4)])["t"] is None   # under 5 trades: no t-stat
    assert jd.summarize([])["mean_pct"] is None


# ── regimes ───────────────────────────────────────────────────────────────────

def _row(d, pnl, pct, trades=1):
    return {"date": d, "strategy": S, "ticker": "NVDA", "trades": trades, "wins": int(pnl > 0), "pnl": pnl,
            "pct_n": trades, "pct_sum": pct * trades, "pct_sq_sum": pct * pct * trades}


def test_regime_breakdown_and_good_vs_bad():
    days = {"d1": {"labels": {"trend": "trend-up", "vix": "vix-low"}, "events": []},
            "d2": {"labels": {"trend": "chop", "vix": "vix-low"}, "events": [{"kind": "earnings", "label": "NVDA earnings"}]},
            "d3": {"labels": {"trend": "trend-up", "vix": "vix-high"}, "events": [{"kind": "fed", "label": "FOMC decision"}]}}
    daytypes = {"d1": {"NVDA": "Outside"}, "d2": {"NVDA": "Inside"}, "d3": {"NVDA": "Outside"}}
    rows = [_row("d1", 5, 0.5), _row("d2", -4, -0.4), _row("d3", 3, 0.3)]
    b = jd.regime_breakdown(rows, days, daytypes)
    assert [r["value"] for r in b["trend"]] == ["trend-up", "chop"]       # natural order
    assert b["trend"][0]["trades"] == 2 and b["trend"][0]["pnl"] == 8
    assert {r["value"]: r["trades"] for r in b["daytype"]} == {"Outside": 2, "Inside": 1}
    assert {r["value"] for r in b["event"]} == {"no event", "own earnings", "fed"}
    g = jd.good_vs_bad(rows, days, daytypes)
    assert g["win_days"] == 2 and g["loss_days"] == 1
    top = {(r["dimension"], r["value"]): r["diff"] for r in g["rows"]}
    assert top[("trend", "trend-up")] == pytest.approx(1.0)
    assert top[("daytype", "Inside")] == pytest.approx(-1.0)


def test_detect_flip_ignores_a_small_drift_across_zero():
    dates = [f"2026-09-{i:02d}" for i in range(1, 31)]
    noisy = [_row(d, 1, v) for d, v in zip(dates, [0.5, -0.48] * 15)]   # alternating ±0.5%: both halves near zero
    f = jd.detect_flip(noisy, dates, recent_days=20, min_trades=8)
    assert not f["flipped"] and abs(f["change_t"]) < jd.FLIP_MIN_T


def test_detect_flip_needs_enough_trades_each_side():
    dates = [f"2026-09-{i:02d}" for i in range(1, 31)]
    early = [_row(d, 1, v) for d, v in zip(dates[:10], [0.12, 0.08] * 5)]
    late = [_row(d, -1, v) for d, v in zip(dates[10:], [-0.12, -0.08] * 10)]
    f = jd.detect_flip(early + late, dates, recent_days=20, min_trades=8)
    assert f["cutoff"] == "2026-09-11" and f["flipped"] and f["direction"] == "faded"
    assert not jd.detect_flip(early[:5] + late, dates, recent_days=20, min_trades=8)["flipped"]
    short = jd.detect_flip(early, dates[:10], recent_days=20)
    assert not short["flipped"] and short["direction"] is None and short["cutoff"] is None


# ── snapshot → API, against a scratch SQLite DB with the network stubbed ──────

@pytest.fixture()
def journal(tmp_path, monkeypatch):
    import app as a
    import routes.farms as farms
    db = tmp_path / "journal.db"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE routing_rules (id INTEGER PRIMARY KEY, name TEXT, enabled INTEGER, nodes TEXT)")
    c.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,'[]')", (S,))
    c.commit(); c.close()

    def _db():
        conn = sqlite3.connect(db); conn.row_factory = sqlite3.Row
        return conn
    monkeypatch.setattr(a, "get_db", _db)
    monkeypatch.setattr(jd, "_tables_ready", False)
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {"5": {"label": "Kairos Farm"}})
    bars = {
        "SPY": [("2026-11-04", 100, 101, 99, 100), ("2026-11-05", 100.2, 102, 100, 101.8), ("2026-11-06", 101.8, 102, 101, 101.5)],
        "^VIX": [("2026-11-04", 16, 17, 15, 16.5), ("2026-11-05", 16.5, 17, 15, 15.2), ("2026-11-06", 15.2, 16, 14, 14.8)],
    }
    monkeypatch.setattr(jd, "_daily_bars", lambda sym, s, e: _bars(bars.get(sym, bars["SPY"])))
    monkeypatch.setattr(jd, "_intraday_bars", lambda sym, s, e: _pm_bars(sym))
    monkeypatch.setattr(jd, "_earnings_by_ticker", lambda tks: {"NVDA": {"2026-11-06"}})
    monkeypatch.setattr(a, "_get_day_classification", lambda tk, d: {"day_type": "Outside", "ratio": 0.5}, raising=False)
    trips = [{"strategy": S, "side": "LONG", "entry_time": "2026-11-05T15:00:00Z", "exit_time": "2026-11-05T15:05:00Z",
              "pnl": 4.0, "qty": 2, "entry_price": 200.0},
             {"strategy": S, "side": "LONG", "entry_time": "2026-11-06T15:00:00Z", "exit_time": "2026-11-06T15:05:00Z",
              "pnl": -1.0, "qty": 1, "entry_price": 200.0},
             {"strategy": S, "side": "LONG", "entry_time": "2026-11-06T15:00:00Z", "exit_time": "2026-11-06T15:06:00Z",
              "pnl": -1.0, "qty": 1, "entry_price": 200.0}]          # a split exit: one trade
    calls = []

    def _rts(num, f, t, full=False, exclusions=True):
        calls.append((f, exclusions))
        return trips, None
    monkeypatch.setattr(farms, "_farm_round_trips", _rts)
    a._journal_rts_calls = calls
    return a, db


def test_snapshot_ignores_exclusions_and_pairs_with_lead_in(journal):
    a, _ = journal
    res = jd.snapshot_range("2026-11-05", "2026-11-06")
    assert a._journal_rts_calls == [("2026-10-22", False)]     # 14 days of lead-in, no exclusions
    assert res["strategy_rows"] == 2                           # rows still limited to the range


def test_farm_round_trips_exclusions_switch(monkeypatch):
    import app as a
    import routes.farms as farms
    names = ["ASTS_CAM_BREAKOUT_R4S4_V02_5MIN", "HOOD_CAM_REVERSAL_R3S3_V02_5MIN",
             "AAPL_CAM_BREAKOUT_R3S3_V02_5MIN", "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"]
    closed = [{"strategy": n.lower(), "pnl": 1.0} for n in names]
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {"9": {"fills_fn": lambda: [{"id": 1}]}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda fills, from_date=None, to_date=None: {"closed_clean": [dict(c) for c in closed]})
    monkeypatch.setattr(a, "_load_excluded_tickers", lambda: {"ASTS"})
    monkeypatch.setattr(a, "_load_excluded_bands", lambda: {"REVERSAL R3S3"})
    monkeypatch.setattr(a, "_load_excluded_strategies", lambda: {names[2]})
    kept, err = farms._farm_round_trips("9", "", "")
    assert err is None and [c["strategy"] for c in kept] == [names[3]]
    everything, err = farms._farm_round_trips("9", "", "", exclusions=False)
    assert err is None and [c["strategy"] for c in everything] == names


def test_snapshot_writes_days_and_strategy_rows_and_keeps_notes(journal):
    a, db = journal
    client = a.app.test_client()
    r = client.put("/api/journal/days/2026-11-05/notes", json={"notes": "Fed chatter", "tags": ["fed", "bogus"]})
    assert r.status_code == 200 and r.get_json()["tags"] == ["fed"]

    res = jd.snapshot_range("2026-11-05", "2026-11-06")
    assert res["days"] == 2 and res["strategy_rows"] == 2 and not res["errors"]
    jd.snapshot_range("2026-11-05", "2026-11-06")             # re-running is idempotent

    d = client.get("/api/journal/days?from=2026-11-01&to=2026-11-07").get_json()
    days = {x["date"]: x for x in d["days"]}
    assert set(days) == {"2026-11-05", "2026-11-06"}
    assert days["2026-11-05"]["notes"] == "Fed chatter" and days["2026-11-05"]["note_tags"] == ["fed"]
    assert days["2026-11-05"]["labels"]["trend"] == "trend-up"
    assert days["2026-11-05"]["labels"]["vix"] == "vix-mid"       # prior close 16.5
    assert days["2026-11-06"]["labels"]["vix"] == "vix-mid"       # prior close 15.2
    assert {"kind": "earnings", "label": "NVDA earnings"} in days["2026-11-06"]["events"]
    assert days["2026-11-06"]["accounts"]["5"] == {"trades": 1, "wins": 0, "pnl": -2.0}
    assert days["2026-11-05"]["daytype_counts"] == {"Outside": 1}
    assert days["2026-11-06"]["market"]["SPY"]["pm_range_pct"] == pytest.approx(1.0 / 101.8 * 100, abs=1e-3)
    assert "premarket" not in days["2026-11-06"]["labels"]          # no 20-day baseline in the fixture

    c = sqlite3.connect(db)
    assert c.execute("SELECT COUNT(*) FROM journal_strategy_days").fetchone()[0] == 2
    assert c.execute("SELECT COUNT(*) FROM journal_events WHERE kind='fed'").fetchone()[0] == len(jd.FOMC_SEED)
    c.close()

    reg = client.get(f"/api/journal/regimes?account=5&from=2026-11-01&to=2026-11-07&strategy={S}").get_json()
    assert reg["overall"]["trades"] == 2
    assert reg["overall"]["mean_pct"] == pytest.approx((1.0 - 0.5) / 2)   # +4/$400, then -2/$400 merged
    assert {r["value"] for r in reg["breakdown"]["event"]} == {"no event", "own earnings"}
    lst = client.get("/api/journal/regimes?account=5&from=2026-11-01&to=2026-11-07&min_trades=1").get_json()
    assert [s["strategy"] for s in lst["strategies"]] == [S]


def test_events_and_input_validation(journal):
    a, _ = journal
    client = a.app.test_client()
    assert client.post("/api/journal/events", json={"date": "2026-11-12", "label": "CPI release"}).status_code == 200
    ev = client.get("/api/journal/events?from=2026-11-01&to=2026-11-30").get_json()["events"]
    assert {"date": "2026-11-12", "kind": "event", "label": "CPI release", "source": "crew"} in ev
    assert client.delete("/api/journal/events", json={"date": "2026-11-12", "label": "CPI release"}).status_code == 200
    assert client.post("/api/journal/events", json={"date": "nope", "label": "x"}).status_code == 400
    assert client.put("/api/journal/days/2026-13-40/notes", json={}).status_code == 400
    assert client.get("/api/journal/days?from=bad").status_code == 400
    assert client.post("/api/journal/snapshot", json={"from": "2026-11-06", "to": "2026-11-01"}).status_code == 400


def test_scheduler_does_not_start_under_pytest(monkeypatch):
    monkeypatch.setattr(jd, "_scheduler_started", False)
    jd.start_scheduler()
    assert jd._scheduler_started is False


def test_scheduler_runs_after_the_close_not_in_the_morning():
    from datetime import datetime
    et = jd._ET
    assert jd._after_close(datetime(2026, 11, 9, 16, 20, tzinfo=et))
    assert jd._after_close(datetime(2026, 11, 7, 18, 0, tzinfo=et))          # Saturday evening
    assert not jd._after_close(datetime(2026, 11, 9, 6, 42, tzinfo=et))      # before the open (the old bug)
    assert not jd._after_close(datetime(2026, 11, 9, 0, 5, tzinfo=et))       # just after midnight
    assert not jd._after_close(datetime(2026, 11, 9, 11, 0, tzinfo=et))      # mid-session


# ── pre-market ────────────────────────────────────────────────────────────────

def _pm_bars(sym, day="2026-11-06", lo=100.0, hi=101.0):
    """5-min bars around one pre-market, given in ET and returned as naive UTC like Alpaca's."""
    from datetime import datetime as _dt, timezone as _tz
    y, m, d = map(int, day.split("-"))

    def mk(hh, mm, l, h, c):
        t = _dt(y, m, d, hh, mm, tzinfo=jd._ET).astimezone(_tz.utc).replace(tzinfo=None)
        return {"time": t, "open": c, "high": h, "low": l, "close": c, "volume": 100}
    return [mk(3, 55, 90.0, 110.0, 100.5),      # overnight, ignored
            mk(4, 0, lo, min(hi, 100.5), 100.2),
            mk(7, 0, 100.3, hi, 100.4),
            mk(9, 25, lo, min(hi, 100.9), 100.6),   # last pre-market bar
            mk(9, 30, 95.0, 106.0, 105.0)]      # regular session, ignored


def test_premarket_features_window_and_measures():
    f = jd.premarket_features(_pm_bars("SPY"), {"2026-11-06": 100.0})["2026-11-06"]
    assert (f["pm_low"], f["pm_high"], f["pm_last"]) == (100.0, 101.0, 100.6)
    assert f["pm_range_pct"] == pytest.approx(1.0)
    assert f["pm_move_pct"] == pytest.approx(0.6)
    assert f["pm_volume"] == 300 and f["pm_range_x"] is None


def test_premarket_range_x_against_prior_median_and_label():
    import datetime as _d
    bars, pc = [], {}
    day0 = _d.date(2026, 9, 1)
    for i in range(12):                                  # 0.5% pre-market ranges
        d = (day0 + _d.timedelta(days=i)).isoformat()
        bars += _pm_bars("SPY", d, lo=100.0, hi=100.5)
        pc[d] = 100.0
    bars += _pm_bars("SPY", "2026-09-20", lo=99.5, hi=101.0)   # 1.5%: 3× usual
    pc["2026-09-20"] = 100.0
    f = jd.premarket_features(bars, pc)
    assert f["2026-09-12"]["pm_range_x"] == pytest.approx(1.0)
    assert f["2026-09-20"]["pm_range_x"] == pytest.approx(3.0)
    assert jd.premarket_label([0.9, 3.0]) == "pm-volatile"           # QQQ volatile, SPY calm
    assert jd.premarket_label([None, 1.0]) == "pm-normal"
    assert jd.premarket_label([0.5, 0.6]) == "pm-quiet"
    assert jd.premarket_label([None, None]) is None
    assert jd.day_labels(None, None, [1.6, 0.8]) == {"premarket": "pm-volatile"}
