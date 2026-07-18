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

# CPR mid = 100; long level r4 = 110, short level s4 = 90.
_CLS = {"day_type": "Outside", "mid_cpr": 100.0, "r4": 110.0, "s4": 90.0, "r3": 105.0, "s3": 95.0}


def _rt(ticker, side, pnl, strat="BREAKOUT"):
    kind = "BREAKOUT" if strat == "BREAKOUT" else "REVERSAL"
    # Real strategy format: level is one token (R4S4), matching production names.
    return {"ticker": ticker, "side": side, "pnl": pnl, "qty": 10,
            "entry_time": "2026-07-06T14:00:00Z", "exit_time": "2026-07-06T14:20:00Z",
            "strategy": f"{ticker}_CAM_{kind}_R4S4_V02_5MIN"}


@pytest.fixture()
def ls(monkeypatch):
    """Wire ls_breakdown to fixed fills, a fixed day classification, and per-ticker
    opens that place each ticker's open at a chosen spot on the CPR→level path."""
    # ticker -> that day's open price (chosen to land in a known bucket).
    opens = {
        "NEAR":  100.0,   # opened at mid → near CPR (room)     [long & short]
        "MIDT":  105.0,   # halfway to r4 (long) → mid-travel
        "EXT":   109.0,   # 0.9 to r4 → extended
        "PEAK":  114.0,   # past r4 → at/past extreme
        "SNEAR": 100.0,   # short: at mid → near CPR
        "SPEAK":  86.0,   # short: past s4 → at/past extreme
    }
    monkeypatch.setattr(a, "_get_day_classification", lambda tk, dt: dict(_CLS))
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
