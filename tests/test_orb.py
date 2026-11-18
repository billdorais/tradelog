"""Opening-range breakout replay (strategies/orb.py): rules, costs, look-ahead, stats.

Every scenario is a hand-built day of 1-minute bars whose right answer is known."""
from __future__ import annotations

import gzip
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

from strategies import orb

ET = "America/New_York"
FREE = {"slippage_ticks": 0, "commission": 0, "fast_sma": 2, "slow_sma": 4}


def _day(d, segs, overrides=None):
    """1-minute bars for date d. segs: (from, to_exclusive, p0, p1) with the close moving
    linearly from p0 (first minute) to p1 (last minute). Open = previous close."""
    rows, prev = [], None
    for a, b, p0, p1 in segs:
        t0 = pd.Timestamp(f"{d} {a}", tz=ET)
        n = int((pd.Timestamp(f"{d} {b}", tz=ET) - t0).total_seconds() // 60)
        for i in range(n):
            c = p0 + (p1 - p0) * (i / (n - 1) if n > 1 else 1)
            c = round(c * 4) / 4
            o = c if prev is None else prev
            rows.append((t0 + pd.Timedelta(minutes=i), o, max(o, c), min(o, c), c))
            prev = c
    df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close"]).set_index("t")
    for hm, ohlc in (overrides or {}).items():
        df.loc[pd.Timestamp(f"{d} {hm}", tz=ET), ["open", "high", "low", "close"]] = ohlc
    return df


WARM = ("2026-03-02", [("06:00", "16:00", 100, 118)])          # Monday: a steady uptrend


def _market(*days):
    frames = [_day(d, s, o) for d, s, *rest in days for o in [rest[0] if rest else None]]
    return orb.Market.from_frame(pd.concat(frames).sort_index())


def _range_day(d="2026-03-03", after=None, overrides=None, pre=(118, 120)):
    """Range 120–121 from 09:00–09:29, then `after` segments from 09:30."""
    segs = [("06:00", "09:00", *pre), ("09:00", "09:30", 120.5, 120.5)] + (after or [])
    ov = {"09:10": (120.5, 121, 120, 120.5)}
    ov.update(overrides or {})
    return (d, segs, ov)


def _run(m, **p):
    return orb.simulate(m, {**FREE, **p}, "NQ", date(2026, 3, 3), date(2026, 3, 31))


def test_long_breakout_hits_3r_target():
    m = _market(WARM, _range_day(after=[("09:30", "09:35", 120.75, 121.5), ("09:35", "10:00", 121.5, 126.5),
                                        ("10:00", "17:00", 126.5, 126.5)]))
    r = _run(m)
    assert len(r["trades"]) == 1
    t = r["trades"][0]
    assert (t["side"], t["or_low"], t["or_high"]) == ("long", 120.0, 121.0)
    assert (t["signal_time"], t["entry_time"], t["entry"], t["stop"], t["target"]) == ("09:30", "09:35", 121.5, 120.0, 126.0)
    assert t["reason"] == "target" and t["r"] == pytest.approx(3.0)


def test_a_wick_through_the_range_is_not_a_breakout():
    m = _market(WARM, _range_day(after=[("09:30", "17:00", 120.5, 120.5)], overrides={"09:32": (120.5, 122, 120.5, 120.75)}))
    r = _run(m)
    assert r["trades"] == [] and r["skipped"]["no_breakout"] == 1


def test_trend_filter_skips_a_short_in_an_uptrend_and_the_day_is_over():
    after = [("09:30", "09:35", 120.25, 119.5), ("09:35", "17:00", 119.5, 119.5)]
    m = _market(WARM, _range_day(after=after))
    r = _run(m)
    assert r["trades"] == [] and r["skipped"]["trend_filter"] == 1
    off = _run(m, trend_filter=False)
    assert [t["side"] for t in off["trades"]] == ["short"] and off["trades"][0]["reason"] == "time"


def test_retry_after_filter_takes_a_later_breakout_that_agrees():
    after = [("09:30", "09:35", 120.25, 119.5), ("09:35", "09:45", 119.5, 120.5), ("09:45", "09:50", 120.75, 121.5),
             ("09:50", "17:00", 121.5, 121.5)]
    m = _market(WARM, _range_day(after=after))
    assert _run(m)["trades"] == []
    r = _run(m, retry_after_filter=True)
    assert [(t["side"], t["signal_time"]) for t in r["trades"]] == [("long", "09:45")]


def test_stop_and_target_in_one_bar_is_scored_as_the_stop_by_default():
    after = [("09:30", "09:35", 120.75, 121.5), ("09:35", "09:40", 121.5, 122), ("09:40", "17:00", 122, 122)]
    m = _market(WARM, _range_day(after=after, overrides={"09:38": (122, 127, 119, 122)}))
    t = _run(m)["trades"][0]
    assert t["reason"] == "stop (same bar as target)" and t["r"] == pytest.approx(-1.0)
    t2 = _run(m, same_bar="target_first")["trades"][0]
    assert t2["reason"] == "target (same bar as stop)" and t2["r"] == pytest.approx(3.0)


def test_a_gap_through_the_stop_fills_at_the_open_not_the_stop():
    after = [("09:30", "09:35", 120.75, 121.5), ("09:35", "09:40", 121.5, 121.5), ("09:40", "17:00", 119, 119)]
    m = _market(WARM, _range_day(after=after, overrides={"09:40": (119, 119, 119, 119)}))
    t = _run(m)["trades"][0]
    assert t["reason"] == "stop (gap)" and t["exit"] == 119.0 and t["r"] == pytest.approx((119 - 121.5) / 1.5, abs=1e-4)


def test_no_entry_on_a_candle_that_closes_after_last_entry():
    late = [("09:30", "10:30", 120.5, 120.5), ("10:30", "10:35", 120.75, 121.5), ("10:35", "17:00", 121.5, 121.5)]
    m = _market(WARM, _range_day(after=late))
    r = _run(m)
    assert r["trades"] == [] and r["skipped"]["no_breakout"] == 1
    on_time = [("09:30", "10:25", 120.5, 120.5), ("10:25", "10:30", 120.75, 121.5), ("10:30", "17:00", 121.5, 121.5)]
    r2 = _run(_market(WARM, _range_day(after=on_time)))
    assert [(t["signal_time"], t["entry_time"]) for t in r2["trades"]] == [("10:25", "10:30")]


def test_flat_by_the_forced_exit():
    m = _market(WARM, _range_day(after=[("09:30", "09:35", 120.75, 121.5), ("09:35", "17:00", 121.5, 122)]))
    t = _run(m)["trades"][0]
    assert t["reason"] == "time" and t["exit_time"] == "15:55"


def test_an_early_close_exits_at_the_last_bar_of_the_session():
    m = _market(WARM, _range_day(after=[("09:30", "09:35", 120.75, 121.5), ("09:35", "13:15", 121.5, 122)]))
    t = _run(m)["trades"][0]
    assert t["reason"] == "session end" and t["exit_time"] == "13:15" and t["exit"] == 122.0


def test_one_trade_per_day_by_default():
    after = [("09:30", "09:35", 120.75, 121.5), ("09:35", "09:40", 121.5, 119.5), ("09:40", "09:45", 119.5, 121.5),
             ("09:45", "17:00", 121.75, 121.75)]
    m = _market(WARM, _range_day(after=after))
    assert len(_run(m)["trades"]) == 1
    assert len(_run(m, max_trades_per_day=2, trend_filter=False)["trades"]) == 2


def test_the_trend_filter_only_sees_completed_candles():
    """Fast/slow = 1/2. Completed 15-minute closes at 09:35 are 120.75 then 120.25 (down).
    The 09:30–09:45 candle rips higher, but it has not closed when the 09:30–09:35
    breakout candle does, so it must not turn the trend up."""
    segs = [("06:00", "09:00", 118, 120), ("09:00", "09:15", 120.75, 120.75), ("09:15", "09:30", 120.25, 120.25),
            ("09:30", "09:35", 120.5, 121.5), ("09:35", "09:45", 121.5, 130), ("09:45", "17:00", 130, 130)]
    m = _market(WARM, ("2026-03-03", segs, {"09:10": (120.75, 121, 120, 120.75)}))
    r = _run(m, fast_sma=1, slow_sma=2)
    assert r["trades"] == [] and r["skipped"]["trend_filter"] == 1


def test_trend_warmup_days_are_not_traded():
    m = _market(_range_day(d="2026-03-03", after=[("09:30", "09:35", 120.75, 121.5), ("09:35", "17:00", 121.5, 121.5)]))
    r = _run(m, slow_sma=200)
    assert r["trades"] == [] and r["skipped"]["trend_warmup"] == 1
    assert orb.ready_date(m, {**FREE, "slow_sma": 200}) is None


def test_costs_come_out_of_r():
    m = _market(WARM, _range_day(after=[("09:30", "09:35", 120.75, 121.5), ("09:35", "10:00", 121.5, 126.5),
                                        ("10:00", "17:00", 126.5, 126.5)]))
    t = orb.simulate(m, {**FREE, "slippage_ticks": 1, "commission": 2.25}, "NQ", date(2026, 3, 3), date(2026, 3, 3))["trades"][0]
    assert t["entry"] == 121.75                       # one tick worse than the 121.50 open
    risk = 121.75 - 120.0
    assert t["target"] == pytest.approx(121.75 + 3 * risk)
    assert t["pnl"] == pytest.approx((t["exit"] - 121.75) * 20 - 4.5)
    assert t["r"] == pytest.approx(t["pnl"] / (risk * 20), abs=1e-4) and t["r"] < t["r_gross"]


def test_utc_bars_give_the_same_trades_across_dst():
    """2026-03-08 is the DST change. Bars handed in as UTC must still build the range
    from 09:00–09:30 Eastern on both sides of it."""
    days = [("2026-03-06", WARM[1]),
            _range_day(d="2026-03-09", after=[("09:30", "09:35", 120.75, 121.5), ("09:35", "17:00", 121.5, 122)])]
    et = pd.concat([_day(d, s, o) for d, s, *rest in days for o in [rest[0] if rest else None]]).sort_index()
    a = orb.simulate(orb.Market.from_frame(et), FREE, "NQ", date(2026, 3, 9), date(2026, 3, 9))["trades"]
    utc = et.copy()
    utc.index = utc.index.tz_convert("UTC")
    b = orb.simulate(orb.Market.from_frame(utc), FREE, "NQ", date(2026, 3, 9), date(2026, 3, 9))["trades"]
    assert a == b and len(a) == 1 and a[0]["or_low"] == 120.0 and a[0]["signal_time"] == "09:30"


def test_summary_math():
    trades = [{"r": r, "r_gross": r, "pnl": r * 100, "side": "long", "date": "2026-01-0%d" % (i + 1), "reason": "x"}
              for i, r in enumerate([3.0, -1.0, -1.0, 3.0, -1.0, -1.0, -1.0])]
    s = orb.summarize(trades)
    assert s["trades"] == 7 and s["win_rate"] == pytest.approx(28.6)
    assert s["total_r"] == 1.0 and s["profit_factor"] == pytest.approx(6 / 5)
    assert s["max_dd_r"] == 3.0 and s["longest_losing"] == 3
    assert s["avg_win_r"] == 3.0 and s["avg_loss_r"] == -1.0


def test_monte_carlo_shapes():
    rs = [3.0, -1, -1, 3, -1, -1, -1, 2, -1, -1] * 5
    mc = orb.monte_carlo(rs, n_iter=500, seed=1)
    assert mc["trades"] == 50 and 0 <= mc["p_total_le_0"] <= 1
    assert 0 < mc["max_dd_r"]["p50"] <= mc["max_dd_r"]["p95"] <= mc["max_dd_r"]["worst"]
    assert mc["expectancy_ci95"][0] < np.mean(rs) < mc["expectancy_ci95"][1]
    assert sum(mc["dd_histogram"]["counts"]) == 500
    assert orb.monte_carlo([1, 2]) is None


def test_full_report_runs_end_to_end():
    m = _market(WARM, _range_day(after=[("09:30", "09:35", 120.75, 121.5), ("09:35", "10:00", 121.5, 126.5),
                                        ("10:00", "17:00", 126.5, 126.5)]))
    rep = orb.full_report(m, FREE, "MNQ", date(2026, 3, 3), date(2026, 3, 3))
    assert rep["summary"]["trades"] == 1
    assert len(rep["compare"]) == 2 * len(orb.TARGET_GRID)
    assert {r["param"] for r in rep["sensitivity"]} >= {"target_r", "sma_pair", "same_bar"}
    assert any(r["current"] for r in rep["sensitivity"] if r["param"] == "target_r")


def test_parse_csv_formats():
    et_txt = "Date,Time,Open,High,Low,Close,Volume\n2026-03-03,09:00,1,2,0.5,1.5,10\n2026-03-03,09:01,1.5,2,1,1.75,5\n"
    df = orb.parse_csv(et_txt.encode(), tz=ET)
    assert str(df.index.tz) == ET and df.index[0].hour == 9 and list(df["close"]) == [1.5, 1.75]
    epoch = "ts_event,open,high,low,close\n1772546400000000000,1,2,0.5,1.5\n"       # 2026-03-03 14:00 UTC
    df2 = orb.parse_csv(epoch.encode())
    assert df2.index[0].tz_convert(ET).hour == 9
    df3 = orb.parse_csv(gzip.compress(et_txt.encode()), tz="America/Chicago", filename="x.csv.gz")
    assert df3.index[0].tz_convert(ET).hour == 10
    dst = ("datetime,open,high,low,close\n2026-03-06 09:00:00-05:00,1,2,0.5,1.5\n"
           "2026-03-09 09:00:00-04:00,1,2,0.5,1.5\n")
    df4 = orb.parse_csv(dst.encode())
    assert [t.tz_convert(ET).hour for t in df4.index] == [9, 9]
    with pytest.raises(ValueError):
        orb.parse_csv(b"a,b\n1,2\n")


def test_bad_params_are_rejected():
    for bad in ({"or_end": "08:00"}, {"fast_sma": 300}, {"target_r": 0}, {"stop": "x"}, {"breakout_tf": 7}):
        with pytest.raises(ValueError):
            orb.normalize_params(bad)
    assert orb.normalize_params({"trend_filter": "false"})["trend_filter"] is False


def test_page_and_meta_and_validation():
    import app as kairos
    c = kairos.app.test_client()
    assert c.get("/orb").status_code == 200
    meta = c.get("/api/orb/meta").get_json()
    assert meta["defaults"]["target_r"] == 3.0 and meta["contracts"]["MNQ"]["point_value"] == 2.0
    r = c.post("/api/orb/run", json={"source": "yahoo", "from": "2026-03-03", "to": "2026-03-01"})
    assert r.status_code == 400
    r = c.post("/api/orb/run", json={"source": "csv", "from": "2026-03-01", "to": "2026-03-03", "contract": "NQ"})
    assert r.status_code == 400 and "dataset" in r.get_json()["error"]


# ── Alpaca 1-minute loader (2026-11-15) ───────────────────────────────────────

def test_load_alpaca_fetches_1m_a_month_at_a_time(monkeypatch):
    import alpaca.data.historical as hist
    from datetime import date as _d
    calls = []

    class FakeClient:
        def __init__(self, api_key=None, secret_key=None, raw_data=False):
            assert raw_data is True                      # dicts, not one Bar object per minute
        def get_stock_bars(self, req):
            calls.append((req.start, req.end, str(req.timeframe), str(req.feed)))
            t0 = req.start.replace(hour=14, minute=0)   # 09:00 ET in winter, 10:00 in summer
            return {"QQQ": [{"t": (t0 + timedelta(minutes=k)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                             "o": 100 + k, "h": 101 + k, "l": 99 + k, "c": 100.5 + k, "v": 1000}
                            for k in range(3)]}
    monkeypatch.setattr(hist, "StockHistoricalDataClient", FakeClient)
    monkeypatch.setenv("ALPACA_DATA_FEED", "sip")
    msgs = []
    df = orb.load_alpaca("qqq", _d(2024, 1, 1), _d(2024, 3, 31), "1m", progress=msgs.append)
    assert len(calls) == 3 and all("1Min" in c[2] or "Minute" in c[2] for c in calls)
    assert all("sip" in c[3].lower() for c in calls)
    assert calls[0][0].date() == _d(2024, 1, 1) and calls[-1][1].date() == _d(2024, 4, 1)
    assert len(df) == 9 and list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert df.index.is_monotonic_increasing and str(df.index.tz) == "UTC"
    assert msgs[-1].endswith("month 3 of 3")
    with pytest.raises(ValueError):
        orb.load_alpaca("QQQ", _d(2024, 1, 1), _d(2024, 1, 2), "15m")


def test_alpaca_1m_runs_allow_several_years(client_or_none=None):
    from routes import orb_replay as r
    ok = r._validate_run({"source": "alpaca", "symbol": "QQQ", "resolution": "1m",
                          "from": "2021-01-01", "to": "2026-01-01"})
    assert ok["resolution"] == "1m" and ok["contract"] == "QQQ"
    with pytest.raises(ValueError):
        r._validate_run({"source": "alpaca", "symbol": "QQQ", "resolution": "1m",
                         "from": "2015-01-01", "to": "2026-01-01"})


# ── compare tickers (2026-11-15) ──────────────────────────────────────────────

def _t(d, r, pnl, time="09:40"):
    return {"date": d, "time": time, "r": r, "r_gross": r, "pnl": pnl, "side": "long"}


def test_portfolio_sums_tickers_by_day_inside_the_range():
    from routes.orb_replay import portfolio
    a = {"run_id": "a", "label": "QQQ", "trading_from": "2026-01-01", "trading_to": "2026-12-31",
         "trades": [_t("2026-09-30", 5.0, 500), _t("2026-10-01", 3.0, 300), _t("2026-10-03", -1.0, -100)]}
    b = {"run_id": "b", "label": "GOOG", "trading_from": "2026-10-02", "trading_to": "2026-12-31",
         "trades": [_t("2026-10-02", -1.0, -50), _t("2026-10-03", 2.0, 80), _t("2026-11-02", 9.0, 900)]}
    out = portfolio([a, b], "2026-10-01", "2026-10-31")
    assert out["dates"] == ["2026-10-01", "2026-10-02", "2026-10-03"]       # Sept 30 and Nov 2 left out
    qqq, goog = out["series"]
    assert qqq["cum_r"] == [3.0, 3.0, 2.0] and goog["cum_r"] == [0.0, -1.0, 1.0]
    assert qqq["covers"] is True and goog["covers"] is False                # GOOG's data starts Oct 2
    c = out["combined"]
    assert c["cum_r"] == [3.0, 2.0, 3.0] and c["cum_usd"] == [300.0, 250.0, 230.0]
    assert c["summary"]["trades"] == 4 and c["summary"]["total_r"] == 3.0
    assert (c["days"], c["green_days"], c["best_day_r"], c["worst_day_r"]) == (3, 2, 3.0, -1.0)
    assert (qqq["days"], qqq["green_days"]) == (2, 1)
    # Data through Friday 2026-11-13 covers a range ending Sunday the 15th, but not Monday the 16th.
    c = {"run_id": "c", "label": "SPY", "trading_from": "2021-11-15", "trading_to": "2026-11-13", "trades": []}
    assert portfolio([c], "2026-11-01", "2026-11-15")["series"][0]["covers"] is True
    assert portfolio([c], "2026-11-01", "2026-11-16")["series"][0]["covers"] is False


def _st(d, r, risk_pts, entry, time="09:40"):
    """A trade from a 100-share run: $ per share = R × the stop distance."""
    return {"date": d, "time": time, "r": r, "r_gross": r, "pnl": r * risk_pts * 100, "pps": r * risk_pts,
            "risk_pts": risk_pts, "entry": entry, "side": "long"}


def test_size_trade_whole_shares_under_the_risk_and_the_cap():
    from routes.orb_replay import size_trade
    t = _st("2026-10-01", 1.0, 1.48, 440.0)
    assert size_trade(t, 50) == 33                     # 33 × $1.48 = $48.84 ≤ $50
    assert size_trade(t, 1.47) == 0                    # one share already risks more
    assert size_trade(t, 100, cap=10_000) == 22        # $10k buys 22 shares at $440 (67 by risk alone)
    assert size_trade({**t, "risk_pts": 0.25}, 20) == 80   # exact fit counts despite float error
    assert size_trade({**t, "risk_pts": 0}, 50) == 0


def test_portfolio_sizes_every_trade_to_the_risk():
    from routes.orb_replay import portfolio
    a = {"run_id": "a", "label": "QQQ", "trades": [_st("2026-10-01", 3.0, 1.0, 400.0), _st("2026-10-02", -1.0, 2.0, 400.0),
                                                    _st("2026-10-03", 2.0, 60.0, 400.0)]}
    b = {"run_id": "b", "label": "GOOG", "trades": [_st("2026-10-02", -1.0, 0.5, 100.0)]}
    out = portfolio([a, b], "2026-10-01", "2026-10-31", risk=50)
    qqq, goog = out["series"]
    # QQQ: 50 sh × +$3 = +150; 25 sh × −$2 = −50; a $60 stop can't fit $50 → skipped
    assert qqq["cum_risk"] == [150.0, 100.0, 100.0]
    assert qqq["sizing"] == {"risk": 50, "cap": None, "net": 100.0, "taken": 2, "skipped": 1, "capped": 0,
                             "avg_risk": 50.0, "median_position": 20000.0, "max_position": 20000.0}
    assert goog["cum_risk"] == [0.0, -50.0, -50.0]     # 100 sh × −$0.50
    c = out["combined"]
    assert c["cum_risk"] == [150.0, 50.0, 50.0] and c["sizing"]["net"] == 50.0
    assert (c["sizing"]["best_day"], c["sizing"]["worst_day"], c["sizing"]["max_dd"]) == (150.0, -100.0, 100.0)
    capped = portfolio([a], "2026-10-01", "2026-10-31", risk=50, cap=8_000)["series"][0]["sizing"]
    assert capped["net"] == 20.0 and capped["capped"] == 2                   # 20 sh × $3 − 20 sh × $2
    assert "cum_risk" not in portfolio([a], "2026-10-01", "2026-10-31")["series"][0]


def test_portfolio_with_no_trades_in_range():
    from routes.orb_replay import portfolio
    out = portfolio([{"run_id": "a", "label": "QQQ", "trades": [_t("2026-01-02", 1.0, 10)]}], "2026-10-01", "2026-10-31")
    assert out["dates"] == [] and out["combined"]["summary"] == {"trades": 0} and out["combined"]["best_day_r"] is None


def test_portfolio_options_keep_the_longest_run_per_ticker_filter_and_settings():
    import json
    from routes.orb_replay import portfolio_options
    def cfg(sym, frm, to, **p):
        return json.dumps({"source": "alpaca", "symbol": sym, "from": frm, "to": to,
                           "params": orb.normalize_params({"contracts": 100, **p})})
    rows = [("old5y", "2026-11-14T01:00", cfg("QQQ", "2021-11-15", "2026-11-13", trend_filter=False)),
            ("new60d", "2026-11-15T01:00", cfg("QQQ", "2026-09-01", "2026-11-13", trend_filter=False)),
            ("on", "2026-11-14T02:00", cfg("QQQ", "2021-11-15", "2026-11-13", trend_filter=True)),
            ("t4", "2026-11-14T03:00", cfg("QQQ", "2021-11-15", "2026-11-13", trend_filter=False, target_r=4)),
            ("goog", "2026-11-14T04:00", cfg("GOOG", "2021-11-15", "2026-11-13", trend_filter=False)),
            ("fut", "2026-11-14T05:00", json.dumps({"source": "csv", "contract": "MNQ", "from": "2024-01-01",
                                                    "to": "2024-06-30", "params": orb.normalize_params({})}))]
    opts = {(o["label"], o["trend_filter"], o["variant"]): o["run_id"] for o in portfolio_options(rows)}
    assert opts == {("GOOG", False, ""): "goog", ("MNQ (CSV)", True, ""): "fut", ("QQQ", False, ""): "old5y",
                    ("QQQ", False, "4.0R target"): "t4", ("QQQ", True, ""): "on"}


def test_portfolio_endpoint_reads_stored_runs(monkeypatch, tmp_path):
    import json
    import sqlite3
    import app as kairos
    from routes import orb_replay as r
    db = str(tmp_path / "orb.db")
    monkeypatch.setattr(r, "_db", lambda: sqlite3.connect(db))
    monkeypatch.setattr(r, "_ph", lambda: "?")
    monkeypatch.setattr(r, "_trades_cache", {})
    r._ensure_tables()
    res = {"trades": [{"date": "2026-10-06", "entry_time": "09:35", "r": 3.0, "r_gross": 3.0, "pnl": 150.0, "side": "long",
                       "risk_pts": 0.5, "entry": 400.0},
                      {"date": "2026-10-07", "entry_time": "09:40", "r": -1.0, "r_gross": -1.0, "pnl": -50.0, "side": "short",
                       "risk_pts": 0.5, "entry": 400.0}],
           "data": {"trading_from": "2021-11-15", "trading_to": "2026-11-13"}}
    params = json.dumps({"source": "alpaca", "symbol": "QQQ", "from": "2021-11-15", "to": "2026-11-13",
                         "params": orb.normalize_params({"trend_filter": False, "contracts": 100})})
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO orb_runs VALUES (?,?,?,?,?,?,?)", ("r1", "2026-11-15", params, "done", "done", json.dumps(res), "x"))
    conn.execute("INSERT INTO orb_runs VALUES (?,?,?,?,?,?,?)", ("r2", "2026-11-15", params, "running", "", None, "x"))
    conn.commit()
    conn.close()
    c = kairos.app.test_client()
    opts = c.get("/api/orb/portfolio/options").get_json()["options"]
    assert [o["run_id"] for o in opts] == ["r1"]
    d = c.get("/api/orb/portfolio?runs=r1&from=2026-10-01&to=2026-10-31").get_json()
    assert d["dates"] == ["2026-10-06", "2026-10-07"] and d["series"][0]["cum_r"] == [3.0, 2.0]
    assert d["series"][0]["summary"]["net_pnl"] == 100.0
    s = c.get("/api/orb/portfolio?runs=r1&from=2026-10-01&to=2026-10-31&risk=50").get_json()["series"][0]
    assert s["cum_risk"] == [150.0, 100.0] and s["sizing"]["taken"] == 2      # 100 sh: $50 ÷ $0.50 stop
    assert c.get("/api/orb/portfolio?runs=r1&from=2026-10-01&to=2026-10-31&risk=abc").status_code == 400
    assert c.get("/api/orb/portfolio?runs=r1&from=2026-10-01&to=2026-10-31&risk=0").status_code == 400
    assert c.get("/api/orb/portfolio?runs=r2&from=2026-10-01&to=2026-10-31").status_code == 404
    assert c.get("/api/orb/portfolio?runs=r1&from=2026-10-31&to=2026-10-01").status_code == 400
    assert c.get("/api/orb/portfolio?runs=&from=2026-10-01&to=2026-10-31").status_code == 400
    many = ",".join(f"x{i}" for i in range(8))
    assert c.get(f"/api/orb/portfolio?runs={many}&from=2026-10-01&to=2026-10-31").status_code == 400


# ── breakeven and trailing exits (2026-11-17) ────────────────────────────────
# Entry 121.5, stop 120 → 1R = 1.5 points, 3R target 126.

_BREAK = ("09:30", "09:35", 120.75, 121.5)


def _flat(after):
    return after + [("10:30", "17:00", after[-1][3], after[-1][3])]


def test_breakeven_turns_a_round_trip_into_a_scratch():
    m = _market(WARM, _range_day(after=_flat([_BREAK, ("09:35", "09:45", 121.5, 123.0),
                                              ("09:45", "10:30", 123.0, 119.0)])))
    base = _run(m)["trades"][0]
    be = _run(m, be_at_r=1.0)["trades"][0]
    assert base["reason"] == "stop" and base["r"] == pytest.approx(-1.0)
    assert be["reason"] == "breakeven" and be["exit"] == 121.5 and be["r"] == pytest.approx(0.0)


def test_a_trail_from_breakeven_locks_in_profit():
    m = _market(WARM, _range_day(after=_flat([_BREAK, ("09:35", "09:45", 121.5, 124.5),
                                              ("09:45", "10:30", 124.5, 119.0)])))
    t = _run(m, be_at_r=1.0, trail_r=1.0)["trades"][0]
    assert t["reason"] == "trail" and t["exit"] == 123.0 and t["r"] == pytest.approx(1.0)   # peak 124.5 − 1.5
    assert _run(m)["trades"][0]["r"] == pytest.approx(-1.0)


def test_a_one_minute_spike_cannot_move_the_stop_in_that_minute():
    """1-minute bars hide the order inside the bar: a bar that reached +1R and the
    stop must be read as stopped at the ORIGINAL stop, not saved by breakeven."""
    m = _market(WARM, _range_day(after=_flat([_BREAK, ("09:35", "10:30", 121.5, 121.5)]),
                                 overrides={"09:36": (121.5, 123.25, 119.5, 119.75)}))
    t = _run(m, be_at_r=1.0, trail_r=1.0)["trades"][0]
    assert t["reason"] == "stop" and t["r"] == pytest.approx(-1.0)


def test_no_target_lets_a_trail_run_past_3r():
    m = _market(WARM, _range_day(after=_flat([_BREAK, ("09:35", "10:00", 121.5, 130.0),
                                              ("10:00", "10:30", 130.0, 127.0)])))
    capped = _run(m, be_at_r=1.0, trail_r=1.0)["trades"][0]
    free = _run(m, be_at_r=1.0, trail_r=1.0, target_r=20)["trades"][0]
    assert capped["reason"] == "target" and capped["r"] == pytest.approx(3.0)
    assert free["reason"] == "trail" and free["exit"] == 128.5 and free["r"] == pytest.approx(7 / 1.5, abs=1e-4)


def test_breakeven_never_sits_below_the_entry_for_a_short():
    m = _market(WARM, _range_day(after=_flat([("09:30", "09:35", 120.25, 119.5), ("09:35", "09:45", 119.5, 118.0),
                                              ("09:45", "10:30", 118.0, 122.0)])))
    # WARM is an uptrend, so the filter must be off for a short.
    t = _run(m, be_at_r=1.0, trend_filter=False)["trades"][0]   # short: entry 119.5, stop 121, 1R = 1.5
    assert t["side"] == "short" and t["reason"] == "breakeven" and t["exit"] == 119.5


def test_a_trail_needs_breakeven_and_the_bracket_is_unchanged_by_default():
    with pytest.raises(ValueError):
        orb.normalize_params({"trail_r": 1.0})
    assert orb.normalize_params({})["be_at_r"] == 0.0 and orb.normalize_params({})["trail_r"] == 0.0


def test_exit_variants_rerun_the_same_entries():
    m = _market(WARM, _range_day(after=_flat([_BREAK, ("09:35", "09:45", 121.5, 124.5),
                                              ("09:45", "10:30", 124.5, 119.0)])))
    vs = orb.exit_variants(m, {**FREE}, "NQ", date(2026, 3, 3), date(2026, 3, 31))
    assert len(vs) == len(orb.EXIT_GRID) and vs[0]["be_at_r"] == 0
    assert vs[0]["summary"] == orb.summarize(_run(m)["trades"])
    assert {v["summary"]["trades"] for v in vs} == {1}, "every variant takes the same entry"
    by = {v["label"]: v["summary"]["total_r"] for v in vs}
    assert by["BE at 1R, trail 1R"] == pytest.approx(1.0) and by["Breakeven at 1R"] == pytest.approx(0.0)
