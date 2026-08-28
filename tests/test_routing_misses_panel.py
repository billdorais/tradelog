"""Routing misses: what TradingView sent that matched no pipeline.

The panel used to select exec_status="blocked" under the heading "no matching
routing rule" — but "blocked" is a GATE stopping a strategy that is wired
perfectly. On 2026-08-27 it listed GLD, GOOG, HOOD and friends (all strikes-gate
blocks) and told the user to fix a typo in a strategy name that was never wrong.
A real routing miss records exec_status="error" with a distinct detail.

Clearing marks a timestamp instead of deleting rows: these are the record of what
TV actually sent, and a strategy that keeps missing must reappear.
"""
from __future__ import annotations

import datetime as dt
import os
import shutil
import sqlite3

import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a

MISS = "No routing pipeline matched strategy '%s' — signal logged but no order placed."


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db = tmp_path / "miss.db"
    shutil.copy("trades.db", db)
    now = dt.datetime.now(dt.timezone.utc)

    def _ts(**kw):
        return (now - dt.timedelta(**kw)).strftime("%Y-%m-%d %H:%M:%S")

    rows = [
        ("TYPO_CAM_BREAKOUT_R3S3_V02_5MIN", "TYPO", _ts(minutes=5), "error", MISS % "TYPO"),
        ("TYPO_CAM_BREAKOUT_R3S3_V02_5MIN", "TYPO", _ts(minutes=3), "error", MISS % "TYPO"),
        # wired strategy stopped by a gate — NOT a routing miss
        ("GLD_CAM_BREAKOUT_R3S3_V02_5MIN", "GLD", _ts(minutes=4), "blocked",
         "2-strikes/level: R3 already took 2 loss(es) today (alpaca6)"),
        ("OLD_CAM_BREAKOUT_R3S3_V02_5MIN", "OLD", _ts(days=3), "error", MISS % "OLD"),
        # an error that is not a routing miss
        ("ERR_CAM_BREAKOUT_R3S3_V02_5MIN", "ERR", _ts(minutes=2), "error",
         "Alpaca rejected the order"),
    ]
    c = sqlite3.connect(db)
    c.execute("DELETE FROM trades")
    for r in rows:
        c.execute("INSERT INTO trades (strategy,ticker,action,received_at,exec_status,"
                  "exec_detail) VALUES (?,?,'BUY',?,?,?)", (r[0], r[1], r[2], r[3], r[4]))
    c.commit(); c.close()

    def _fake_db():
        x = sqlite3.connect(db); x.row_factory = sqlite3.Row
        return x

    store = {}
    monkeypatch.setattr(a, "get_db", _fake_db)
    monkeypatch.setattr(a, "_load_setting", lambda k: store.get(k))
    monkeypatch.setattr(a, "_save_setting", lambda k, v: store.__setitem__(k, v))
    return a.app.test_client(), db


def _names(client, qs="?days=7"):
    return sorted(x["strategy"].split("_")[0]
                  for x in (client.get("/api/webhook/blocked" + qs).get_json() or []))


def test_a_gate_block_is_not_a_routing_miss(client):
    """The bug: GLD is wired correctly, and telling the user to fix its strategy ID
    sends them hunting a typo that does not exist."""
    cl, _ = client
    assert "GLD" not in _names(cl)


def test_an_unrelated_error_is_not_a_routing_miss(client):
    cl, _ = client
    assert "ERR" not in _names(cl)


def test_real_misses_are_listed_and_collapsed_by_strategy(client):
    cl, _ = client
    rows = cl.get("/api/webhook/blocked?days=7").get_json()
    typo = next(r for r in rows if r["strategy"].startswith("TYPO"))
    assert typo["count"] == 2, "repeat misses should collapse into one row"
    assert sorted(r["strategy"].split("_")[0] for r in rows) == ["OLD", "TYPO"]


def test_days_zero_means_today_not_this_instant(client):
    """A now-minus-N cutoff made Today return nothing, since every row predates the
    request by seconds."""
    cl, _ = client
    assert _names(cl, "?days=0") == ["TYPO"]


def test_clearing_hides_the_list_without_deleting_anything(client):
    cl, db = client
    before = sqlite3.connect(db).execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    assert cl.post("/api/webhook/blocked/clear").get_json()["ok"] is True
    assert _names(cl) == []
    after = sqlite3.connect(db).execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    assert after == before, "clearing deleted signal history"


def test_a_miss_after_the_clear_comes_back(client):
    """Otherwise Clear would permanently blind the panel to a live problem."""
    cl, db = client
    cl.post("/api/webhook/blocked/clear")
    later = (dt.datetime.now(dt.timezone.utc)
             + dt.timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S")
    c = sqlite3.connect(db)
    c.execute("INSERT INTO trades (strategy,ticker,action,received_at,exec_status,"
              "exec_detail) VALUES (?,?,'BUY',?,?,?)",
              ("NEW_CAM_BREAKOUT_R3S3_V02_5MIN", "NEW", later, "error", MISS % "NEW"))
    c.commit(); c.close()
    assert _names(cl) == ["NEW"]


def test_an_out_of_range_days_value_does_not_error(client):
    cl, _ = client
    for qs in ("?days=abc", "?days=-5", "?days=9999", ""):
        assert isinstance(cl.get("/api/webhook/blocked" + qs).get_json(), list)


# ── UI placement ────────────────────────────────────────────────────────────────

def test_the_panel_moved_off_the_signal_router():
    """It reports what happened; it does not configure anything."""
    routing = open("templates/routing.html", encoding="utf-8").read()
    assert "blockedTbody" not in routing
    assert "loadBlockedSignals" not in routing


def test_the_panel_lives_on_diagnostics_with_a_clear_button():
    diag = open("templates/diagnostics.html", encoding="utf-8").read()
    assert 'id="routeMissTbody"' in diag
    assert "clearRouteMisses(this)" in diag
    assert "/api/webhook/blocked/clear" in diag


def test_clearing_asks_first_and_says_nothing_is_deleted():
    diag = open("templates/diagnostics.html", encoding="utf-8").read()
    i = diag.index("async function clearRouteMisses")
    block = diag[i:i + 900]
    assert "confirm(" in block
    assert "Nothing is deleted" in block


def test_today_is_offered_in_the_range_selector():
    diag = open("templates/diagnostics.html", encoding="utf-8").read()
    assert '<option value="0">Today</option>' in diag


def test_both_panels_share_the_one_range_selector():
    """Two panels on one screen showing different windows would be read as one."""
    diag = open("templates/diagnostics.html", encoding="utf-8").read()
    assert "loadBlockedBreakdown(); loadRouteMisses()" in diag
    i = diag.index("async function loadRouteMisses")
    assert "getElementById('blockedDays')" in diag[i:i + 700]
