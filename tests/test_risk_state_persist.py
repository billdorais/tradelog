"""Same-day persistence of per-account risk halt state.

A mid-session restart/redeploy used to clear the in-memory profit-lock and
daily-loss dicts. The profit lock cannot self-heal from current P&L alone —
armed-then-halted looks identical to never-armed once the P&L sits below the
floor — so a redeploy silently resumed a halted book. State is now snapshotted
to app_settings on every transition and restored on boot (same ET day only).
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import shutil
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest


@pytest.fixture()
def risk_app(tmp_path):
    import app as a
    db = tmp_path / "risk.db"
    shutil.copy("trades.db", db)
    saved_db = a.get_db

    def _fake_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    a.get_db = _fake_db
    # Clean slate for the state under test.
    a._profit_lock_day = None
    a._profit_lock_armed = {}
    a._profit_lock_halted = {}
    a._daily_loss_day = None
    a._daily_loss_halted.clear()
    yield a
    a.get_db = saved_db
    a._profit_lock_day = None
    a._profit_lock_armed = {}
    a._profit_lock_halted = {}
    a._daily_loss_day = None
    a._daily_loss_halted.clear()


def _today_et():
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()


def test_same_day_state_survives_restart(risk_app):
    a = risk_app
    today = _today_et()
    # Mid-session: Crew armed+halted on profit lock, Refined halted on daily loss.
    a._profit_lock_day = today
    a._profit_lock_armed = {"alpaca4": True}
    a._profit_lock_halted = {"alpaca4": True}
    a._daily_loss_day = today
    a._daily_loss_halted["alpaca2"] = True
    a._persist_risk_day_state()

    # "Restart": in-memory state gone.
    a._profit_lock_day = None
    a._profit_lock_armed = {}
    a._profit_lock_halted = {}
    a._daily_loss_day = None
    a._daily_loss_halted.clear()

    a._restore_risk_settings()
    assert a._profit_lock_armed == {"alpaca4": True}
    assert a._profit_lock_halted == {"alpaca4": True}
    assert a._profit_lock_day == today
    assert dict(a._daily_loss_halted) == {"alpaca2": True}
    assert a._daily_loss_day == today


def test_stale_previous_day_state_is_ignored(risk_app):
    a = risk_app
    a._save_setting("RISK_DAY_STATE", json.dumps({
        "pl_day": "2020-01-01", "pl_armed": ["alpaca4"], "pl_halted": ["alpaca4"],
        "dl_day": "2020-01-01", "dl_halted": ["alpaca2"],
    }))
    a._restore_risk_settings()
    # Yesterday's halts must NOT carry into a new trading day.
    assert a._profit_lock_armed == {}
    assert a._profit_lock_halted == {}
    assert dict(a._daily_loss_halted) == {}


def test_restore_without_stored_state_is_noop(risk_app):
    a = risk_app
    # Ensure no stored state (fresh install / first boot).
    a._save_setting("RISK_DAY_STATE", "")
    a._restore_risk_settings()
    assert a._profit_lock_armed == {}
    assert dict(a._daily_loss_halted) == {}
