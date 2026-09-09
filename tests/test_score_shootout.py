"""Comparing ranking rules on identical walk-forward folds.

The walk-forward machinery already existed but was hardwired to ONE ranker —
`-sum(pcts)`, summed % return. That matters: the t=0.35 figure quoted around this
codebase was measured on THAT baseline, not on `_composite_score`. The production
scorer had never been walk-forward tested at all.

`ranker` makes it pluggable so a scoring change is judged on whether it predicts
rather than on whether the reasoning sounds right — which is the whole point,
given the signal is weak enough that plausible reasoning is cheap.
"""
from __future__ import annotations

import datetime as dt
import os
import random

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


def _history(seed, n_edge, n_noise, edge_mu, sigma=0.9, days=240, tpd=14):
    """Synthetic fills where the edge names are known by construction."""
    random.seed(seed)
    names = ([(f"E{i}_CAM_BREAKOUT_R3S3", edge_mu) for i in range(n_edge)] +
             [(f"N{i}_CAM_BREAKOUT_R3S3", 0.0) for i in range(n_noise)])
    rts, d0 = [], dt.date(2026, 1, 5)
    for day in range(days):
        d = (d0 + dt.timedelta(days=day)).isoformat()
        for nm, mu in random.sample(names, min(tpd, len(names))):
            pct = random.gauss(mu, sigma)
            rts.append({"strategy": nm, "ticker": nm.split("_")[0], "side": "LONG",
                        "date": d, "entry_price": 100.0, "qty": 10,
                        "pnl": round(pct / 100 * 1000, 4),
                        "entry_time": f"{d}T14:00:00Z", "exit_time": f"{d}T14:30:00Z"})
    return rts


def _wf(rts, ranker, n=15):
    return a._selection_walk_forward(rts, rank_days=45, fwd_days=14, n=n,
                                     min_trades=5, ranker=ranker)


# ── the harness must be able to fail ────────────────────────────────────────

def test_a_strong_edge_is_detected_by_every_ranker():
    """Sanity floor. A harness that cannot see an obvious edge proves nothing about
    a subtle one, and every 'no difference' result below would be vacuous."""
    rts = _history(1, n_edge=12, n_noise=48, edge_mu=0.80)
    for r in ("pnl", "v2", "v3"):
        out = _wf(rts, r)
        assert out["t_stat"] > 5, f"{r} missed an obvious edge: t={out['t_stat']}"
        assert out["folds_with_positive_spread"] == out["fold_count"]


def test_pure_noise_reads_as_no_signal():
    """The other half of the floor: it must not manufacture a signal from nothing."""
    rts = _history(3, n_edge=0, n_noise=60, edge_mu=0.0)
    for r in ("pnl", "v2", "v3"):
        out = _wf(rts, r)
        assert abs(out["t_stat"]) < 2, f"{r} found signal in noise: t={out['t_stat']}"
        assert out["significant"] is False


def test_a_weak_edge_is_visible_but_not_overwhelming():
    rts = _history(2, n_edge=12, n_noise=48, edge_mu=0.12)
    out = _wf(rts, "v2")
    assert 1.0 < out["t_stat"] < 5.0


# ── the ranker actually changes the picks ───────────────────────────────────

def test_different_rankers_pick_different_baskets():
    """If eligible <= n every ranker picks the same set and a comparison is
    meaningless. This is the setup error that made the first shootout run report
    three identical rows."""
    rts = _history(5, n_edge=12, n_noise=48, edge_mu=0.3)
    outs = {r: _wf(rts, r) for r in ("pnl", "v2", "v3")}
    spreads = {r: o["weighted_spread_pct"] for r, o in outs.items()}
    assert len(set(spreads.values())) > 1, f"rankers were indistinguishable: {spreads}"


def test_an_unknown_ranker_falls_back_to_the_baseline():
    rts = _history(6, n_edge=8, n_noise=40, edge_mu=0.4)
    assert _wf(rts, "nonsense")["t_stat"] == _wf(rts, "pnl")["t_stat"]


def test_every_registered_ranker_runs():
    rts = _history(7, n_edge=8, n_noise=40, edge_mu=0.4)
    for r in a._SELECTION_RANKERS:
        assert _wf(rts, r).get("error") is None, r


# ── what the evidence actually said ─────────────────────────────────────────

def test_v3_is_not_wired_into_either_snapshot():
    """v3 exists to be MEASURED, not used. On synthetic history it did not beat v2
    (mean t across 5 seeds: pnl 1.42, v2 1.23, v3 1.31) and the gap between rankers
    was smaller than the seed-to-seed variance. Shipping it on the strength of the
    reasoning is exactly what the shootout exists to prevent."""
    import inspect
    for fn in ("_do_refresh_refined", "_do_refresh_kairos_refined"):
        src = inspect.getsource(getattr(a, fn))
        assert "_composite_score_v3" not in src, f"{fn} must not use the unvalidated v3"


def test_the_baseline_ranker_is_not_the_production_scorer():
    """Guards against re-conflating them. The familiar t=0.35 came from 'pnl'."""
    rts = _history(8, n_edge=10, n_noise=50, edge_mu=0.5)
    assert _wf(rts, "pnl")["t_stat"] != _wf(rts, "v2")["t_stat"]


# ── v3's own mechanics ──────────────────────────────────────────────────────

def test_shrinkage_pulls_a_thin_sample_toward_the_prior():
    thin  = {"trades": 1,  "profit_factor": 2.5, "win_rate": 100.0,
             "expectancy_pct": 0.15, "sharpe_pct": 3.5}
    thick = {**thin, "trades": 200}
    assert a._composite_score_v3(thin) < a._composite_score_v3(thick)


def test_shrinkage_also_lifts_a_thin_disaster_toward_the_prior():
    """It cuts both ways — that is what makes it shrinkage and not a penalty."""
    thin  = {"trades": 1,   "profit_factor": 0.0, "win_rate": 0.0,
             "expectancy_pct": -1.0, "sharpe_pct": -2.0}
    thick = {**thin, "trades": 200}
    assert a._composite_score_v3(thin) > a._composite_score_v3(thick)


def test_v3_has_no_trades_term():
    """Sample size stops being points a strategy earns and becomes how much its
    evidence counts."""
    assert "trades" not in a._V3_SCORE_WEIGHTS
    assert round(sum(a._V3_SCORE_WEIGHTS.values()), 6) == 1.0


def test_v3_prefers_percent_sharpe_but_tolerates_its_absence():
    """v2's Sharpe is dollars-per-share, so it is not scale-free across tickers.
    v3 uses the % series when given one, and must not crash when it is not."""
    pct_based = {"trades": 20, "profit_factor": 2.0, "win_rate": 60.0,
                 "expectancy_pct": 0.1, "sharpe_pct": 2.0, "sharpe": 99.0}
    assert a._composite_score_v3(pct_based) == a._composite_score_v3(
        {**pct_based, "sharpe": 0.0})          # the dollar figure is ignored
    legacy = {k: v for k, v in pct_based.items() if k != "sharpe_pct"}
    assert a._composite_score_v3(legacy) > 0   # falls back rather than failing


def test_stats_from_pct_series_matches_the_scorers_expectations():
    st = a._stats_from_pct_series([1.0, -0.5, 2.0, -1.0, 0.5])
    assert st["trades"] == 5
    assert st["win_rate"] == 60.0
    assert st["profit_factor"] == pytest.approx(3.5 / 1.5, abs=1e-3)
    assert st["expectancy_pct"] == pytest.approx(0.4, abs=1e-3)


def test_a_flawless_series_has_no_profit_factor():
    """Which is what both scorers must then handle without paying full marks."""
    assert a._stats_from_pct_series([1.0, 2.0, 0.5])["profit_factor"] is None


def test_an_empty_series_is_safe():
    assert a._stats_from_pct_series([])["trades"] == 0
    assert a._composite_score_v3({"trades": 0}) >= 0


# ── the endpoint ────────────────────────────────────────────────────────────

def test_the_shootout_endpoint_rejects_an_unconfigured_account():
    a.app.config["TESTING"] = True
    r = a.app.test_client().post("/api/backtest/score_shootout", json={"account": "9"})
    assert r.status_code == 400


def test_the_shootout_endpoint_rejects_unknown_rankers(monkeypatch):
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM",
                        {"1": {"tag": "alpaca", "label": "TV Farm",
                               "broker": object(), "fills_fn": lambda: []}})
    a.app.config["TESTING"] = True
    r = a.app.test_client().post("/api/backtest/score_shootout",
                                 json={"account": "1", "rankers": ["made_up"]})
    assert r.status_code == 400 and "rankers" not in r.get_json()["error"].lower()[:5]


def test_the_shootout_reports_every_ranker_on_identical_folds(monkeypatch):
    rts = _history(9, n_edge=10, n_noise=50, edge_mu=0.5)
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM",
                        {"1": {"tag": "alpaca", "label": "TV Farm",
                               "broker": object(), "fills_fn": lambda: "f"}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda *_a, **_k: {"closed_clean": rts})
    a.app.config["TESTING"] = True
    d = a.app.test_client().post("/api/backtest/score_shootout",
                                 json={"account": "1", "n": 15}).get_json()
    assert set(d["results"]) == set(a._SELECTION_RANKERS)
    assert [r["ranker"] for r in d["leaderboard"]]          # ordered by t_stat
    ts = [r["t_stat"] for r in d["leaderboard"]]
    assert ts == sorted(ts, reverse=True)
    assert any("t=0.35" in c for c in d["caveats"]), "must say what the baseline is"
