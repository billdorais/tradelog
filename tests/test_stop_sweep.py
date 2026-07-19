"""Max-loss stop test (Maximum Adverse Excursion) on real round-trips.

_trade_mae_dollars walks bars and returns the worst unrealized $ a trade touched;
the /api/simulate/stop_sweep endpoint buckets each trade at each candidate stop
into "recovered" (dipped past the stop then closed higher — the stop's cost) vs
"bled" (closed at/below — the stop's saving) and reports the net P&L delta.
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


def _bar(hh, mm, high, low):
    return SimpleNamespace(timestamp=dt.datetime(2026, 7, 10, hh, mm, tzinfo=UTC),
                           high=float(high), low=float(low),
                           open=float(low), close=float(low))


def test_mae_short_uses_bar_high():
    # SHORT entry 100, qty 10; worst is the highest high (112) → (100-112)*10 = -120.
    entry = dt.datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    exit_ = dt.datetime(2026, 7, 10, 15, 0, tzinfo=UTC)
    bars = [_bar(14, 10, 105, 99), _bar(14, 20, 112, 101), _bar(14, 30, 108, 100)]
    assert a._trade_mae_dollars("SHORT", 100, 10, entry, exit_, bars) == -120.0


def test_mae_long_uses_bar_low():
    # LONG entry 50, qty 10; worst is the lowest low (48) → (48-50)*10 = -20.
    entry = dt.datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    exit_ = dt.datetime(2026, 7, 10, 15, 0, tzinfo=UTC)
    bars = [_bar(14, 10, 52, 49), _bar(14, 20, 51, 48), _bar(14, 30, 53, 50)]
    assert a._trade_mae_dollars("LONG", 50, 10, entry, exit_, bars) == -20.0


def test_mae_never_adverse_is_zero():
    # SHORT that only ever traded below entry → never adverse → seeded 0.
    entry = dt.datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    exit_ = dt.datetime(2026, 7, 10, 15, 0, tzinfo=UTC)
    bars = [_bar(14, 10, 99, 95), _bar(14, 20, 98, 96)]
    assert a._trade_mae_dollars("SHORT", 100, 10, entry, exit_, bars) == 0.0


def test_mae_no_bars_in_window_is_none():
    entry = dt.datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    exit_ = dt.datetime(2026, 7, 10, 15, 0, tzinfo=UTC)
    # All bars are before entry → nothing covers the hold.
    bars = [_bar(13, 0, 112, 100)]
    assert a._trade_mae_dollars("SHORT", 100, 10, entry, exit_, bars) is None


@pytest.fixture()
def _synthetic(monkeypatch):
    """Three trades with known MAE/realized so the classification is deterministic."""
    trades = [
        # A: SHORT, high→112 (MAE -120), closed +30 → recovered above a -100 stop (cost 130)
        {"ticker": "AAA", "side": "SHORT", "entry_price": 100, "qty": 10, "pnl": 30.0,
         "entry_time": "2026-07-10T14:00:00+00:00", "exit_time": "2026-07-10T15:00:00+00:00"},
        # B: SHORT, high→115 (MAE -150), closed -150 → bled through a -100 stop (saved 50)
        {"ticker": "BBB", "side": "SHORT", "entry_price": 100, "qty": 10, "pnl": -150.0,
         "entry_time": "2026-07-10T14:00:00+00:00", "exit_time": "2026-07-10T15:00:00+00:00"},
        # C: LONG, low→48 (MAE -20), closed +40 → never hit a -100 stop
        {"ticker": "CCC", "side": "LONG", "entry_price": 50, "qty": 10, "pnl": 40.0,
         "entry_time": "2026-07-10T14:00:00+00:00", "exit_time": "2026-07-10T15:00:00+00:00"},
    ]
    bars = {
        "AAA": [_bar(14, 20, 112, 100)],
        "BBB": [_bar(14, 20, 115, 100)],
        "CCC": [_bar(14, 20, 52, 48)],
    }
    monkeypatch.setattr(a, "_build_signal_lookup_for_alpaca", lambda *args, **kw: {})
    monkeypatch.setattr(a, "_alpaca_account_ctx",
                        lambda acct: (object(), "alpaca" + acct, "Book " + acct, lambda: []))
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda *args, **kw: {"closed_clean": trades})
    monkeypatch.setattr(a, "_fetch_day_bars", lambda tk, ds: bars.get(tk.upper(), []))
    return trades


def test_endpoint_classifies_recovered_vs_bled(_synthetic):
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.post("/api/simulate/stop_sweep",
                   json={"accounts": ["2"], "from_date": "2026-07-01",
                         "to_date": "2026-07-17", "stops": [100]}).get_json()
    assert d["trade_count"] == 3
    L = d["levels"][0]
    assert L["stop"] == 100

    short = L["short"]
    assert short["hit"] == 2
    assert short["recovered"]["count"] == 1          # trade A came back
    assert short["recovered"]["cost"] == 130.0       # 30 - (-100)
    assert short["bled"]["count"] == 1               # trade B kept bleeding
    assert short["bled"]["saved"] == 50.0            # -100 - (-150)
    assert short["net_delta"] == -80.0               # stop is net-negative on shorts here

    # The long never dipped to -100 → the stop is inert on it.
    assert L["long"]["hit"] == 0
    assert L["long"]["net_delta"] == 0.0
