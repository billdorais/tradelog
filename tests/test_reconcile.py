"""Sim-vs-actual reconciliation — attribute the model/actual P&L gap to entry vs exit.

The Entry Test showed breakout setups are profitable in simulation while the live
account bled. This endpoint decomposes, per setup:
  total gap = full-model − actual = (full-model − model-exit) + (model-exit − actual)
            =            entry gap +                             exit gap
where model-exit replays the modeled trailing exit from YOUR actual entry (so a
gap there is pure exit), and full-model uses the modeled entry too. These tests
mock the data + replay boundary so only the attribution arithmetic is under test.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as _dt

import pytest

import app as a


class _Bar:
    open = high = low = close = 100.0
    def __init__(self, ts):
        self.timestamp = ts


def _rt(tk, side, entry, exit_, day="2026-07-06"):
    t0 = f"{day}T14:00:00+00:00"
    return {"ticker": tk, "side": side, "strategy": f"{tk}_CAM_BREAKOUT_R4S4_V02_5MIN",
            "date": day, "entry_time": t0, "exit_time": f"{day}T14:20:00+00:00",
            "entry_price": entry, "exit_price": exit_, "qty": 10,
            "pnl": round((entry - exit_) * 10, 2) if side == "SHORT" else round((exit_ - entry) * 10, 2)}


@pytest.fixture()
def recon(monkeypatch):
    t0 = _dt.datetime(2026, 7, 6, 14, 0, tzinfo=_dt.timezone.utc)
    bars = [_Bar(t0 + _dt.timedelta(minutes=5 * i)) for i in range(4)]

    monkeypatch.setattr(a, "alpaca_broker", object())
    monkeypatch.setattr(a, "_alpaca_account_ctx",
                        lambda acct: (object(), "alpaca2", "TV Refined", lambda: [1]))
    monkeypatch.setattr(a, "_fetch_5m_rth_objs", lambda tk, dt: bars)
    monkeypatch.setattr(a, "_camarilla_levels",
                        lambda tk, dt: {"s4": 90.0, "r4": 110.0, "s3": 95.0, "r3": 105.0})
    monkeypatch.setattr(a, "_trade_level", lambda strat, side: "S4" if side == "SHORT" else "R4")
    monkeypatch.setattr(a, "_apply_session_trail", lambda trail, dt: trail)
    # Modeled short entry = 101 (higher = better short than the actual 100).
    monkeypatch.setattr(a, "_find_entry",
                        lambda bars, level, side, rule, buf, ema_filter=True, start=1:
                        (0, 101.0) if side == "SHORT" else None)
    # Modeled exit always 98 (both the model-exit-from-actual and full-model calls).
    monkeypatch.setattr(a, "_simulate_exit",
                        lambda bars, entry, side, *ar, **kw: {"exit_price": 98.0})
    a.app.config["TESTING"] = True

    def _run(trades, qs=""):
        monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                            lambda *args, **kw: {"closed_clean": trades})
        with a.app.test_client() as cl:
            return cl.get("/api/simulate/reconcile?account=2&side=short" + qs).get_json()
    return _run


def test_gap_decomposes_into_entry_plus_exit(recon):
    # SHORT: actual entry 100, exit 105 → actual pnl = 100-105 = -5 (lost, price rose).
    # model-exit from actual entry: 100 → 98 = +2  → exit_gap = 2 - (-5) = +7
    # full-model: entry 101 → 98 = +3            → entry_gap = 3 - 2 = +1
    # total_gap = 3 - (-5) = +8 = 7 + 1
    d = recon([_rt("AAA", "SHORT", 100.0, 105.0)])
    row = d["rows"][0]
    assert row["actual_pnl"] == pytest.approx(-5.0)
    assert row["model_pnl"] == pytest.approx(3.0)
    assert row["exit_gap"] == pytest.approx(7.0)
    assert row["entry_gap"] == pytest.approx(1.0)
    assert row["total_gap"] == pytest.approx(8.0)
    # The identity must hold exactly.
    assert row["entry_gap"] + row["exit_gap"] == pytest.approx(row["total_gap"])


def test_aggregate_totals_and_attribution(recon):
    d = recon([_rt("AAA", "SHORT", 100.0, 105.0), _rt("BBB", "SHORT", 100.0, 105.0)])
    assert d["actual_total"] == pytest.approx(-10.0)      # 2 × -5
    assert d["model_total"] == pytest.approx(6.0)         # 2 × +3
    assert d["total_gap"] == pytest.approx(16.0)          # 2 × +8
    assert d["exit_gap_total"] == pytest.approx(14.0)     # 2 × +7
    assert d["entry_gap_total"] == pytest.approx(2.0)     # 2 × +1
    # Attribution splits the gap; the two halves sum to 100%.
    assert d["exit_pct"] == pytest.approx(87.5)
    assert d["entry_pct"] == pytest.approx(12.5)
    assert d["entry_pct"] + d["exit_pct"] == pytest.approx(100.0)


def test_first_of_day_dedup(recon):
    # Two AAA shorts same day → only the earliest is reconciled (1 setup).
    early = _rt("AAA", "SHORT", 100.0, 105.0)
    late  = {**_rt("AAA", "SHORT", 100.0, 105.0), "entry_time": "2026-07-06T15:00:00+00:00"}
    d = recon([late, early])
    assert d["n_setups"] == 1


def test_unreconciled_when_model_wont_enter(recon, monkeypatch):
    monkeypatch.setattr(a, "_find_entry",
                        lambda *ar, **kw: None)   # model never enters
    d = recon([_rt("AAA", "SHORT", 100.0, 105.0)])
    assert d["n_no_model_entry"] == 1
    assert d["rows"][0]["total_gap"] is None
    assert d["actual_total"] == 0.0               # nothing reconciled


def test_side_filter(recon):
    d = recon([_rt("AAA", "SHORT", 100.0, 105.0), _rt("BBB", "LONG", 100.0, 95.0)])
    assert {r["side"] for r in d["rows"]} == {"SHORT"}   # ?side=short excludes the long
