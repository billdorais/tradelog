"""Per-strategy explorer data.

Sample sizes here are small — 18 crew picks split by time bucket, side and day
type produces hundreds of cells, most with n < 5. So the payload carries the
standard error next to expectancy, and n next to every rate, rather than leaving
the page to render a confident-looking PF on three trades.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


def _rt(strat, pnl, entry_et="09:45", hold_min=20, ticker="AAPL", date="2026-08-12"):
    """entry_et is ET; stored UTC (+4 in August)."""
    hh, mm = (int(x) for x in entry_et.split(":"))
    start = hh * 60 + mm
    end = start + hold_min
    def _utc(mins):
        return f"{date}T{(mins // 60) + 4:02d}:{mins % 60:02d}:00Z"
    return {"strategy": strat, "ticker": ticker, "pnl": pnl, "qty": 10,
            "entry_price": 100.0, "exit_price": 100.0 + pnl / 10, "side": "LONG",
            "date": date, "entry_time": _utc(start), "exit_time": _utc(end),
            "exit_reason": "Trail"}


def test_expectancy_carries_a_standard_error():
    """PF is unstable and unbounded at low n; expectancy +/- SE is the honest
    headline. A single-trade strategy must report SE as null, not 0."""
    out = a._strategy_breakdown([_rt("S", 10), _rt("S", -4), _rt("S", 30)])
    s = out[0]
    assert s["trades"] == 3
    assert s["expectancy"] == pytest.approx(12.0)
    assert s["expectancy_se"] is not None and s["expectancy_se"] > 0
    one = a._strategy_breakdown([_rt("Z", 5)])[0]
    assert one["expectancy_se"] is None, "SE of one sample must be null, not zero"


def test_max_drawdown_is_measured_from_the_running_peak():
    # +100, then -60 => peak 100, trough 40 => drawdown -60. Ends net positive.
    out = a._strategy_breakdown([_rt("S", 100, entry_et="09:35"),
                                 _rt("S", -60, entry_et="10:05")])
    s = out[0]
    assert s["net_pnl"] == 40.0
    assert s["max_drawdown"] == pytest.approx(-60.0)


def test_buckets_use_ENTRY_time_not_exit():
    """Entry is the decision the hours gate acts on. A 09:45 entry held past
    10:00 must bucket at 09:30, not 10:00."""
    out = a._strategy_breakdown([_rt("S", 50, entry_et="09:45", hold_min=40)])
    b = out[0]["by_bucket"]
    assert [x["bucket"] for x in b] == ["09:30"], b


def test_entry_cutoff_curve_accumulates_over_buckets():
    """'Only enter before HH:MM' — each point is the cumulative P&L of every
    bucket up to that time."""
    out = a._strategy_breakdown([_rt("S", 100, entry_et="09:45"),
                                 _rt("S", -40, entry_et="11:15"),
                                 _rt("S", 20,  entry_et="14:05")])
    cuts = out[0]["entry_cutoffs"]
    assert [c["time"] for c in cuts] == ["09:30", "11:00", "14:00"]
    assert [c["cum"] for c in cuts] == [100.0, 60.0, 80.0]


def test_hold_time_split_by_outcome():
    """Winners held shorter than losers is the cut-winners/ride-losers signature."""
    out = a._strategy_breakdown([_rt("S", 50, hold_min=5), _rt("S", 60, hold_min=7),
                                 _rt("S", -20, hold_min=40), _rt("S", -30, hold_min=60)])
    s = out[0]
    assert s["hold_win_median_min"] == 6.0
    assert s["hold_loss_median_min"] == 50.0
    assert s["hold_win_median_min"] < s["hold_loss_median_min"]


def test_curve_is_chronological_but_rows_are_newest_first():
    out = a._strategy_breakdown([_rt("S", 10, entry_et="09:35"),
                                 _rt("S", 20, entry_et="14:05")])
    s = out[0]
    assert [p["cum"] for p in s["curve"]] == [10.0, 30.0]      # oldest → newest
    assert s["rows"][0]["entry_time"].endswith("18:05:00")     # newest first


def test_strategies_sorted_by_net_and_split_correctly():
    out = a._strategy_breakdown([_rt("BIG", 100), _rt("SMALL", 5), _rt("BIG", 50)])
    assert [s["name"] for s in out] == ["BIG", "SMALL"]
    assert out[0]["trades"] == 2 and out[1]["trades"] == 1


def test_page_and_endpoint_are_wired(monkeypatch):
    """The explorer must not collide with /strategies, which is the Pine Script
    uploader — a different page entirely."""
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        assert c.get("/strategy-explorer").status_code == 200
        assert c.get("/strategies").status_code == 200          # still the uploader


def test_endpoint_returns_bands_for_a_configured_account(monkeypatch):
    class _B: _paper = True
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM",
                        {"4": {"tag": "alpaca4", "num": "4", "label": "Crew Paper",
                               "broker": _B(), "fills_fn": lambda: ["x"]}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda fills, **kw: {"closed_clean": [
                            _rt("AAPL_CAM_BREAKOUT_R3S3", 40, entry_et="09:45"),
                            _rt("AAPL_CAM_BREAKOUT_R3S3", -10, entry_et="12:15")]})
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/crew/strategies?account=4&days=30").get_json()
    assert d["label"] == "Crew Paper" and d["strategy_count"] == 1
    s = d["strategies"][0]
    for k in ("curve", "by_bucket", "entry_cutoffs", "rows",
              "expectancy", "expectancy_se", "max_drawdown"):
        assert k in s, f"missing {k}"
    assert s["net_pnl"] == 30.0


def test_unconfigured_account_is_a_clean_error(monkeypatch):
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {})
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        r = c.get("/api/crew/strategies?account=9")
    assert r.status_code == 400 and "not configured" in r.get_json()["error"]


def test_profit_factor_and_its_inputs():
    """PF is reported with the gross figures it comes from, so an 8.29 or an ∞ is
    interpretable rather than just impressive."""
    out = a._strategy_breakdown([_rt("S", 100), _rt("S", 60), _rt("S", -20)])
    s = out[0]
    assert s["gross_win"] == 160.0 and s["gross_loss"] == 20.0
    assert s["profit_factor"] == pytest.approx(8.0)
    assert s["loss_count"] == 1


def test_no_losses_gives_null_profit_factor_not_a_big_number():
    """'No losers yet' and 'a large finite PF' are different claims — the UI shows
    the former as ∞, so it must not be collapsed into a number."""
    s = a._strategy_breakdown([_rt("S", 10), _rt("S", 20)])[0]
    assert s["profit_factor"] is None
    assert s["loss_count"] == 0


def test_zero_pnl_trade_counts_as_a_loss():
    """Matches the analysis endpoint's _stats(), so the two never disagree."""
    s = a._strategy_breakdown([_rt("S", 10), _rt("S", 0)])[0]
    assert s["loss_count"] == 1 and s["win_rate"] == 50.0
    assert s["profit_factor"] is None      # gross loss is 0 → undefined, not 10


def test_curves_are_forward_filled_onto_a_shared_date_axis(monkeypatch):
    """A benchmark is a daily series and trade #5 is a different date for every
    strategy, so the overlay only means anything on a shared DATE axis. Between
    trading days the cumulative carries forward rather than dropping to zero."""
    class _B: _paper = True
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM",
                        {"4": {"tag": "alpaca4", "num": "4", "label": "Crew Paper",
                               "broker": _B(), "fills_fn": lambda: ["x"]}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda fills, **kw: {"closed_clean": [
                            _rt("S", 100, date="2026-08-10"),
                            _rt("S", -40, date="2026-08-12")]})
    monkeypatch.setattr(a, "_fetch_daily_closes", lambda tk, s_, e_: [
        {"date": "2026-08-10", "close": 100.0},
        {"date": "2026-08-11", "close": 101.0},
        {"date": "2026-08-12", "close": 102.0}])
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/crew/strategies?account=4&days=30").get_json()
    assert d["dates"] == ["2026-08-10", "2026-08-11", "2026-08-12"]
    assert d["spy_pct"] == [0.0, 1.0, 2.0]
    s = d["strategies"][0]
    # 100 on the 10th, carried through the 11th (no trades), 60 after the 12th.
    assert s["daily_cum"] == [100.0, 100.0, 60.0]
    assert len(s["daily_cum"]) == len(d["dates"])
    # notional is the base SPY gets scaled to: 100.0 price x 10 qty
    assert s["notional"] == 1000.0


def test_benchmark_failure_leaves_the_curves_usable(monkeypatch):
    """No SPY data must not take the P&L chart down with it."""
    class _B: _paper = True
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM",
                        {"4": {"tag": "alpaca4", "num": "4", "label": "Crew",
                               "broker": _B(), "fills_fn": lambda: ["x"]}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda fills, **kw: {"closed_clean": [_rt("S", 10)]})
    def _boom(tk, s_, e_):
        raise RuntimeError("no bars")
    monkeypatch.setattr(a, "_fetch_daily_closes", _boom)
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/crew/strategies?account=4&days=30").get_json()
    assert d["dates"] == [] and d["spy_pct"] == []
    assert d["strategies"][0]["net_pnl"] == 10.0     # curve still there
