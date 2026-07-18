"""Entry Test split by side (the short-entry tool).

The entry simulator was already side-aware — it enters shorts on a breakdown and
computes short P&L correctly — but the endpoint blended long+short in its totals,
hiding whether a different entry timing rescues the shorts specifically. The
endpoint now aggregates per (rule, side) and returns combined + long/short. These
tests mock the data boundary (fills, bars, levels, the replay) so only the
side-split aggregation is under test.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


def _setup(tk, side):
    return {"ticker": tk, "side": side, "pnl": 1.0, "qty": 1,
            "entry_time": "2026-07-06T14:00:00Z", "exit_time": "2026-07-06T14:20:00Z",
            "strategy": f"{tk}_CAM_BREAKOUT_R4S4_V02_5MIN"}


@pytest.fixture()
def entry(monkeypatch):
    # 2 long setups, 3 short setups (distinct tickers → distinct setups).
    trades = [_setup("AAA", "LONG"), _setup("BBB", "LONG"),
              _setup("CCC", "SHORT"), _setup("DDD", "SHORT"), _setup("EEE", "SHORT")]

    monkeypatch.setattr(a, "alpaca_broker", object())
    monkeypatch.setattr(a, "_get_cached_fills_2", lambda: [1])          # content irrelevant
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda *args, **kw: {"closed_clean": trades})
    monkeypatch.setattr(a, "_fetch_5m_rth_objs", lambda tk, dt: [1])    # truthy bars
    monkeypatch.setattr(a, "_camarilla_levels",
                        lambda tk, dt: {"r4": 110.0, "s4": 90.0, "r3": 105.0, "s3": 95.0})
    monkeypatch.setattr(a, "_trade_level", lambda strat, side: "R4" if side == "LONG" else "S4")
    # Each entry rule yields ONE trade per setup: +10/share long, -5/share short.
    monkeypatch.setattr(a, "_replay_entries",
                        lambda bars, level, side, *ar, **kw: [(10.0, 0.1)] if side == "LONG" else [(-5.0, 0.1)])
    a.app.config["TESTING"] = True

    with a.app.test_client() as cl:
        return cl.get("/api/simulate/entry_test?account=2&buffers=0.05,0.1").get_json()


def _rule(d, name):
    return next(r for r in d["rules"] if r["rule"] == name)


def test_setups_counted_per_side(entry):
    assert entry["n_setups"] == 5
    assert entry["n_setups_long"] == 2
    assert entry["n_setups_short"] == 3


def test_short_side_is_isolated(entry):
    for name in ("confirmed", "immediate", "buffered", "retest"):
        sh = _rule(entry, name)["short"]
        assert sh["trades"] == 3                    # 3 short setups
        assert sh["total_pnl"] == pytest.approx(-15.0)   # 3 × -5
        assert sh["win_rate"] == 0.0                # all shorts lost in the mock


def test_long_side_is_isolated(entry):
    lo = _rule(entry, "immediate")["long"]
    assert lo["trades"] == 2
    assert lo["total_pnl"] == pytest.approx(20.0)   # 2 × +10
    assert lo["win_rate"] == 100.0


def test_combined_equals_long_plus_short(entry):
    row = _rule(entry, "confirmed")
    assert row["trades"] == 5                        # 2 long + 3 short
    assert row["total_pnl"] == pytest.approx(5.0)    # +20 - 15
    assert row["long"]["total_pnl"] + row["short"]["total_pnl"] == pytest.approx(row["total_pnl"])


def test_buffer_sweep_also_side_split(entry):
    for s in entry["sweep"]:
        assert s["short"]["trades"] == 3
        assert s["short"]["total_pnl"] == pytest.approx(-15.0)
        assert s["long"]["total_pnl"] == pytest.approx(20.0)
