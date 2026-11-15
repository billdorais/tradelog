"""Opening-range breakout replay (strategies/orb.py): rules, costs, look-ahead, stats.

Every scenario is a hand-built day of 1-minute bars whose right answer is known."""
from __future__ import annotations

import gzip
import os
from datetime import date

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
