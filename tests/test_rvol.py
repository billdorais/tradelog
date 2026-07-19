"""Relative volume at entry (trailing-bar RVOL) + the P&L-by-RVOL diagnostic.

_compute_trade_rvol divides the entry 1-min bar's SIP volume by the average of the
prior N RTH bars; /api/simulate/rvol_breakdown buckets real round-trips by RVOL and
profiles each ticker so a low-volume entry gate can be validated before it's built.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as dt
from types import SimpleNamespace

import pytest

import app as a

UTC = dt.timezone.utc


def _bar(hh, mm, vol, px=10.0):
    # ET 9:30–16:00 == UTC 13:30–20:00 (July / EDT).
    return SimpleNamespace(timestamp=dt.datetime(2026, 7, 10, hh, mm, tzinfo=UTC),
                           high=px + 1, low=px - 1, open=px, close=px, volume=float(vol))


def _rth_bars(entry_hh, entry_mm, prior_vol, entry_vol, n_prior=20):
    """n_prior RTH bars at `prior_vol`, then the entry bar at `entry_vol`."""
    bars = [_bar(13, 30 + i, prior_vol) for i in range(n_prior)]     # 9:30.. ET
    bars.append(_bar(entry_hh, entry_mm, entry_vol))
    return bars


def test_rvol_is_entry_over_trailing_average():
    bars = _rth_bars(13, 55, prior_vol=100, entry_vol=300)           # 300 / 100
    trade = {"ticker": "AAA", "entry_time": "2026-07-10T13:55:00+00:00"}
    rv = a._compute_trade_rvol(trade, bars=bars, lookback=20)
    assert rv["rvol"] == 3.0
    assert rv["entry_volume"] == 300.0
    assert rv["baseline_volume"] == 100.0
    assert rv["method"] == "trailing_bars"


def test_rvol_thin_entry_below_one():
    bars = _rth_bars(13, 55, prior_vol=100, entry_vol=40)            # 40 / 100 = 0.4
    trade = {"ticker": "AAA", "entry_time": "2026-07-10T13:55:00+00:00"}
    assert a._compute_trade_rvol(trade, bars=bars, lookback=20)["rvol"] == 0.4


def test_rvol_none_too_close_to_open():
    # Only 3 prior bars before the entry → under min_bars(5) → can't assess.
    bars = _rth_bars(13, 33, prior_vol=100, entry_vol=300, n_prior=3)
    trade = {"ticker": "AAA", "entry_time": "2026-07-10T13:33:00+00:00"}
    assert a._compute_trade_rvol(trade, bars=bars, lookback=20) is None


def test_rvol_none_when_no_bars():
    trade = {"ticker": "AAA", "entry_time": "2026-07-10T13:55:00+00:00"}
    assert a._compute_trade_rvol(trade, bars=[], lookback=20) is None


def test_rvol_bucket_label():
    assert a._rvol_bucket_label(0.3) == "< 0.5×"
    assert a._rvol_bucket_label(0.9) == "0.5–1.0×"
    assert a._rvol_bucket_label(1.2) == "1.0–1.5×"
    assert a._rvol_bucket_label(5.0) == "≥ 3.0×"


@pytest.fixture()
def _synthetic(monkeypatch):
    # Two thin-entry losers (RVOL 0.4) and one thick-entry winner (RVOL 3.0).
    trades = [
        {"ticker": "THN", "side": "SHORT", "pnl": -60.0, "entry_price": 10, "qty": 10,
         "strategy": "THN_CAM_BREAKOUT_R4S4_V02_5MIN",
         "entry_time": "2026-07-10T13:55:00+00:00", "exit_time": "2026-07-10T14:30:00+00:00"},
        {"ticker": "THN", "side": "SHORT", "pnl": -40.0, "entry_price": 10, "qty": 10,
         "strategy": "THN_CAM_BREAKOUT_R4S4_V02_5MIN",
         "entry_time": "2026-07-10T14:00:00+00:00", "exit_time": "2026-07-10T14:40:00+00:00"},
        {"ticker": "FAT", "side": "SHORT", "pnl": 80.0, "entry_price": 10, "qty": 10,
         "strategy": "FAT_CAM_BREAKOUT_R4S4_V02_5MIN",
         "entry_time": "2026-07-10T13:55:00+00:00", "exit_time": "2026-07-10T14:30:00+00:00"},
    ]
    # Bars: THN entries land on a 40-vol bar (thin), FAT on a 300-vol bar (thick).
    thin_bars = _rth_bars(13, 55, 100, 40) + [_bar(14, 0, 40)]
    fat_bars  = _rth_bars(13, 55, 100, 300)
    bars = {"THN": thin_bars, "FAT": fat_bars}
    monkeypatch.setattr(a, "_build_signal_lookup_for_alpaca", lambda *ar, **kw: {})
    monkeypatch.setattr(a, "_alpaca_account_ctx",
                        lambda acct: (object(), "alpaca" + acct, "Book " + acct, lambda: []))
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda *ar, **kw: {"closed_clean": trades})
    monkeypatch.setattr(a, "_fetch_day_bars", lambda tk, ds: bars.get(tk.upper(), []))
    monkeypatch.setattr(a, "_persist_trade_rvol", lambda *ar, **kw: None)   # no DB writes in test
    return trades


def test_endpoint_buckets_and_ticker_profile(_synthetic):
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.post("/api/simulate/rvol_breakdown",
                   json={"accounts": ["2"], "from_date": "2026-07-01",
                         "to_date": "2026-07-17"}).get_json()
    assert d["trade_count"] == 3
    short_buckets = {b["bucket"]: b for b in d["by_bucket"]["short"]}
    # Two thin (0.4×) shorts land in the < 0.5× bucket, both losers.
    assert short_buckets["< 0.5×"]["trades"] == 2
    assert short_buckets["< 0.5×"]["total_pnl"] == -100.0
    # The thick (3.0×) short is the winner in the ≥ 3.0× bucket.
    assert short_buckets["≥ 3.0×"]["trades"] == 1
    assert short_buckets["≥ 3.0×"]["total_pnl"] == 80.0
    # Per-ticker: THN is the chronically-thin offender (avg well under 1×, 100%
    # thin, and net-negative), sorted first; FAT is the thick winner.
    thn = d["by_ticker"][0]
    assert thn["ticker"] == "THN"
    assert thn["avg_rvol"] < 0.5
    assert thn["thin_rate"] == 100
    assert thn["total_pnl"] == -100.0
    assert d["by_ticker"][-1]["ticker"] == "FAT"
