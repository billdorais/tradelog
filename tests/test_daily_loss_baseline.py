"""Daily-loss guard measures P&L from an equity baseline captured at the ET-midnight
roll — independent of Alpaca's last_equity timing. This fixes the bug where, after a
loss day, the guard kept re-halting through pre-market (because broker.daily_pnl still
showed yesterday's loss until the open) and never auto-cleared.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


class _FakeBroker:
    def __init__(self, eq): self.eq = float(eq); self.closed = 0
    def account_equity(self): return self.eq
    def close_all_positions(self): self.closed += 1


@pytest.fixture()
def _guard(monkeypatch):
    monkeypatch.setattr(a, "MAX_DAILY_LOSS", -125.0)
    monkeypatch.setattr(a, "_persist_risk_day_state", lambda: None)
    br = _FakeBroker(24875.0)   # "down $125 from yesterday" — but that's yesterday's loss
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS",
                        [{"tag": "alpaca2", "num": 2, "label": "TV Refined",
                          "broker": br, "daily_loss_guard": True}])
    with a._risk_lock:
        a._daily_loss_day = None
        a._daily_loss_halted.clear()
        a._daily_loss_baseline.clear()
    return br


def test_new_day_does_not_false_halt(_guard):
    a._daily_loss_guard_tick()
    # Baseline captured at current equity → today's P&L is 0, so no halt even though
    # the account carried yesterday's loss.
    assert not a._daily_loss_halted.get("alpaca2")
    assert a._daily_loss_baseline["alpaca2"] == 24875.0
    assert _guard.closed == 0


def test_halts_when_down_the_limit_from_baseline(_guard):
    a._daily_loss_guard_tick()          # capture baseline 24875
    _guard.eq = 24750.0                  # down exactly $125 from TODAY's baseline
    a._daily_loss_guard_tick()
    assert a._daily_loss_halted.get("alpaca2") is True
    assert _guard.closed == 1
    # Sticky within the day: recovering doesn't un-halt.
    _guard.eq = 25000.0
    a._daily_loss_guard_tick()
    assert a._daily_loss_halted.get("alpaca2") is True


def test_date_roll_clears_and_rebaselines(_guard):
    with a._risk_lock:
        a._daily_loss_day = "2000-01-01"           # stale day → forces a roll this tick
        a._daily_loss_halted["alpaca2"] = True     # yesterday's halt
        a._daily_loss_baseline["alpaca2"] = 25000.0
    a._daily_loss_guard_tick()
    # Rolled: halt cleared, baseline re-captured from current equity, P&L 0 → no halt.
    assert not a._daily_loss_halted.get("alpaca2")
    assert a._daily_loss_baseline["alpaca2"] == 24875.0
    assert _guard.closed == 0


def test_farms_are_exempt(monkeypatch):
    monkeypatch.setattr(a, "MAX_DAILY_LOSS", -125.0)
    monkeypatch.setattr(a, "_persist_risk_day_state", lambda: None)
    farm = _FakeBroker(20000.0)   # way "down" but exempt
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS",
                        [{"tag": "alpaca", "num": 1, "label": "TV Farm",
                          "broker": farm, "daily_loss_guard": False}])
    with a._risk_lock:
        a._daily_loss_day = None; a._daily_loss_halted.clear(); a._daily_loss_baseline.clear()
    a._daily_loss_guard_tick()
    assert "alpaca" not in a._daily_loss_baseline    # never even evaluated
    assert farm.closed == 0
