"""Opening-location split in /api/alpaca/ls_breakdown (Thor Young test).

A breakout that opens near the CPR has room to travel to its level; one that
opens already at the extreme is exhausted on arrival and tends to revert. The
endpoint tags each BREAKOUT round-trip by where the day opened relative to the
CPR mid and the level it broke, and buckets P&L room→extreme. These tests drive
the full endpoint with mocked fills + day data so the metric, side symmetry,
breakout-only gating, and bucket ordering are all pinned.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a

# CPR mid = 100; long level r4 = 110, short level s4 = 90. Neutral bias by default.
_CLS = {"day_type": "Outside", "mid_cpr": 100.0, "r4": 110.0, "s4": 90.0, "r3": 105.0,
        "s3": 95.0, "bias": "Neutral"}


def _rt(ticker, side, pnl, strat="BREAKOUT"):
    kind = "BREAKOUT" if strat == "BREAKOUT" else "REVERSAL"
    # Real strategy format: level is one token (R4S4), matching production names.
    return {"ticker": ticker, "side": side, "pnl": pnl, "qty": 10,
            "entry_time": "2026-07-06T14:00:00Z", "exit_time": "2026-07-06T14:20:00Z",
            "strategy": f"{ticker}_CAM_{kind}_R4S4_V02_5MIN"}


@pytest.fixture()
def ls(monkeypatch):
    """Wire ls_breakdown to fixed fills, a per-ticker day classification (so bias
    can vary by ticker), and per-ticker opens placed at a chosen spot on the path."""
    # ticker -> that day's open price (chosen to land in a known bucket).
    opens = {
        "NEAR":  100.0,   # opened at mid → near CPR (room)     [long & short]
        "MIDT":  105.0,   # halfway to r4 (long) → mid-travel
        "EXT":   109.0,   # 0.9 to r4 → extended
        "PEAK":  114.0,   # past r4 → at/past extreme
        "SNEAR": 100.0,   # short: at mid → near CPR
        "SPEAK":  86.0,   # short: past s4 → at/past extreme
        "BULL":  100.0, "BEAR": 100.0,   # trend-context tickers (open irrelevant here)
    }
    # ticker -> bias override; default Neutral.
    bias = {"BULL": "Bullish", "BEAR": "Bearish"}

    def _cls(tk, dt):
        return {**_CLS, "bias": bias.get(tk.upper(), "Neutral")}
    monkeypatch.setattr(a, "_get_day_classification", _cls)
    monkeypatch.setattr(a, "_get_day_open", lambda tk, dt: opens.get(tk.upper()))
    monkeypatch.setattr(a, "_build_signal_lookup_for_alpaca", lambda: {})

    trades_holder = {"rows": []}
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda *args, **kw: {"closed_clean": trades_holder["rows"]})
    # Registry: pretend account 2 is configured with an empty fills fn.
    monkeypatch.setitem(a.ACCOUNTS_BY_NUM, "2",
                        {**a.ACCOUNTS_BY_NUM.get("2", {}), "num": "2", "tag": "alpaca2",
                         "label": "TV Refined", "broker": object(), "fills_fn": lambda: [1]})
    a.app.config["TESTING"] = True

    def _run(rows):
        trades_holder["rows"] = rows
        with a.app.test_client() as cl:
            return cl.get("/api/alpaca/ls_breakdown?account=2").get_json()
    return _run


def _cell(rows, side, loc):
    return next((r for r in rows if r["side"] == side and r["open_location"] == loc), None)


def test_long_breakouts_bucket_by_opening_location(ls):
    d = ls([
        _rt("NEAR", "LONG", 50), _rt("MIDT", "LONG", 20),
        _rt("EXT",  "LONG", -30), _rt("PEAK", "LONG", -80),
    ])
    ol = d["by_open_location"]
    assert _cell(ol, "LONG", "near CPR (room)")["pnl"] == 50
    assert _cell(ol, "LONG", "mid-travel")["pnl"] == 20
    assert _cell(ol, "LONG", "extended")["pnl"] == -30
    assert _cell(ol, "LONG", "at/past extreme")["pnl"] == -80


def test_short_side_is_symmetric(ls):
    d = ls([_rt("SNEAR", "SHORT", 40), _rt("SPEAK", "SHORT", -90)])
    ol = d["by_open_location"]
    assert _cell(ol, "SHORT", "near CPR (room)")["pnl"] == 40
    assert _cell(ol, "SHORT", "at/past extreme")["pnl"] == -90


def test_rows_ordered_room_to_extreme_within_side(ls):
    d = ls([
        _rt("PEAK", "LONG", -80), _rt("NEAR", "LONG", 50),
        _rt("EXT", "LONG", -30), _rt("MIDT", "LONG", 20),
    ])
    longs = [r["open_location"] for r in d["by_open_location"] if r["side"] == "LONG"]
    assert longs == ["near CPR (room)", "mid-travel", "extended", "at/past extreme"]


def test_reversals_are_excluded(ls):
    d = ls([_rt("NEAR", "LONG", 50, strat="REVERSAL")])
    assert d["by_open_location"] == []
    assert d["open_location_unresolved"] == 0    # reversals skipped, not counted


def test_missing_open_counts_as_unresolved(ls):
    d = ls([_rt("UNKNOWN", "LONG", 10)])          # not in the opens map → None
    assert d["by_open_location"] == []
    assert d["open_location_unresolved"] == 1


def test_gradient_reveals_the_thor_pattern(ls):
    """The whole point: if losers cluster at the extreme and winners near the CPR,
    the aggregate win rate should fall as opening location extends."""
    d = ls([
        _rt("NEAR", "LONG", 60), _rt("NEAR", "LONG", 40),   # near: 2 wins
        _rt("PEAK", "LONG", -50), _rt("PEAK", "LONG", -70),  # extreme: 2 losses
    ])
    near = _cell(d["by_open_location"], "LONG", "near CPR (room)")
    peak = _cell(d["by_open_location"], "LONG", "at/past extreme")
    assert near["win_rate"] == 100.0 and near["pnl"] == 100
    assert peak["win_rate"] == 0.0 and peak["pnl"] == -120


def _tc(rows, side, ctx):
    return next((r for r in rows if r["side"] == side and r["trend_context"] == ctx), None)


def test_trend_context_classifies_with_and_against(ls):
    d = ls([
        _rt("BULL", "LONG", 50),    # long on a bullish day → with-trend
        _rt("BEAR", "LONG", -40),   # long on a bearish day → against-trend
        _rt("BEAR", "SHORT", 30),   # short on a bearish day → with-trend
        _rt("BULL", "SHORT", -90),  # short on a bullish day → AGAINST-trend (the suspect)
    ])
    tc = d["by_trend_context"]
    assert _tc(tc, "LONG", "with-trend")["pnl"] == 50
    assert _tc(tc, "LONG", "against-trend")["pnl"] == -40
    assert _tc(tc, "SHORT", "with-trend")["pnl"] == 30
    assert _tc(tc, "SHORT", "against-trend")["pnl"] == -90


def test_against_trend_short_is_the_bleed_bucket(ls):
    """The deck's thesis: a SHORT breakout against a bullish 2-day trend
    ('S4 Breakout Against the Trend') is the lower-quality, bleeding cohort."""
    d = ls([
        _rt("BEAR", "SHORT", 40), _rt("BEAR", "SHORT", 20),    # with-trend shorts: green
        _rt("BULL", "SHORT", -60), _rt("BULL", "SHORT", -80),  # against-trend shorts: red
    ])
    tc = d["by_trend_context"]
    assert _tc(tc, "SHORT", "with-trend")["pnl"] == 60 and _tc(tc, "SHORT", "with-trend")["win_rate"] == 100.0
    assert _tc(tc, "SHORT", "against-trend")["pnl"] == -140 and _tc(tc, "SHORT", "against-trend")["win_rate"] == 0.0


def test_neutral_bias_is_its_own_bucket_not_unresolved(ls):
    d = ls([_rt("NEAR", "LONG", 10)])   # NEAR has default Neutral bias
    assert _tc(d["by_trend_context"], "LONG", "neutral")["pnl"] == 10
    assert d["trend_context_unresolved"] == 0


def test_trend_context_ordered_against_first(ls):
    d = ls([
        _rt("BULL", "LONG", 10),    # with-trend
        _rt("BEAR", "LONG", -10),   # against-trend
        _rt("NEAR", "LONG", 5),     # neutral
    ])
    longs = [r["trend_context"] for r in d["by_trend_context"] if r["side"] == "LONG"]
    assert longs == ["against-trend", "neutral", "with-trend"]


def test_trend_context_reversals_excluded(ls):
    d = ls([_rt("BULL", "LONG", 10, strat="REVERSAL")])
    assert d["by_trend_context"] == []
