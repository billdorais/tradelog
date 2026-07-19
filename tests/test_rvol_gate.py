"""RVOL entry gate — block low relative-volume breakouts (and short blow-offs) on
the gated books (TV Refined, Kairos Refined). Breakouts only; reversals/exits pass;
fails open when RVOL can't be computed. Default OFF.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import shutil
import sqlite3

import pytest

import app as a

BREAKOUT = "RVG_CAM_BREAKOUT_R4S4_V02_5MIN"
REVERSAL = "RVG_CAM_REVERSAL_R4S4_V02_5MIN"


@pytest.fixture()
def gate(monkeypatch):
    monkeypatch.setattr(a, "RVOL_GATE_ENABLED", True)
    monkeypatch.setattr(a, "RVOL_GATE_MIN", 1.5)
    monkeypatch.setattr(a, "RVOL_GATE_SHORT_CAP", 3.0)
    monkeypatch.setattr(a, "RVOL_GATE_ACCOUNTS", {"alpaca2", "alpaca3"})
    return monkeypatch


def _fake_rvol(val):
    return lambda ticker, lookback=20, now_dt=None: val


def test_blocks_thin_breakout(gate, monkeypatch):
    monkeypatch.setattr(a, "_live_rvol", _fake_rvol(1.1))     # below 1.5x floor
    blk, reason, rv = a._rvol_gate_block(BREAKOUT, "long", "RVG", "alpaca3")
    assert blk is True and reason == "rvol_low" and rv == 1.1


def test_allows_in_band_breakout(gate, monkeypatch):
    monkeypatch.setattr(a, "_live_rvol", _fake_rvol(1.8))     # inside 1.5–3.0
    blk, reason, rv = a._rvol_gate_block(BREAKOUT, "long", "RVG", "alpaca3")
    assert blk is False and rv == 1.8


def test_blocks_short_blowoff(gate, monkeypatch):
    monkeypatch.setattr(a, "_live_rvol", _fake_rvol(3.4))     # >= 3.0 cap, short
    blk, reason, rv = a._rvol_gate_block(BREAKOUT, "short", "RVG", "alpaca2")
    assert blk is True and reason == "rvol_blowoff"
    # A LONG blow-off is fine (longs run in a volume surge).
    assert a._rvol_gate_block(BREAKOUT, "long", "RVG", "alpaca2")[0] is False


def test_reversals_pass(gate, monkeypatch):
    monkeypatch.setattr(a, "_live_rvol", _fake_rvol(0.4))     # very thin, but reversal
    assert a._rvol_gate_block(REVERSAL, "long", "RVG", "alpaca3")[0] is False


def test_ungated_account_passes(gate, monkeypatch):
    monkeypatch.setattr(a, "_live_rvol", _fake_rvol(0.4))
    # Crew (alpaca4) and the farms are not in RVOL_GATE_ACCOUNTS.
    assert a._rvol_gate_block(BREAKOUT, "long", "RVG", "alpaca4")[0] is False
    assert a._rvol_gate_block(BREAKOUT, "long", "RVG", "alpaca")[0] is False


def test_fails_open_when_rvol_unavailable(gate, monkeypatch):
    monkeypatch.setattr(a, "_live_rvol", _fake_rvol(None))    # no bars / near open
    assert a._rvol_gate_block(BREAKOUT, "long", "RVG", "alpaca3")[0] is False


def test_disabled_passes(monkeypatch):
    monkeypatch.setattr(a, "RVOL_GATE_ENABLED", False)
    monkeypatch.setattr(a, "RVOL_GATE_ACCOUNTS", {"alpaca2", "alpaca3"})
    monkeypatch.setattr(a, "_live_rvol", _fake_rvol(0.1))
    assert a._rvol_gate_block(BREAKOUT, "long", "RVG", "alpaca3")[0] is False


def test_webhook_drops_gated_target(gate, monkeypatch, tmp_path):
    """A thin breakout routed to Kairos Refined is skipped by the webhook gate."""
    import routes.webhook as w  # noqa: F401
    monkeypatch.setattr(a, "_live_rvol", _fake_rvol(1.0))     # thin → block
    a.ALPACA_ACCOUNTS = [{"tag": "alpaca3", "target_paper": "alpaca-paper-3",
                          "target_live": "alpaca-live-3"}]
    db = tmp_path / "rvg.db"; shutil.copy("trades.db", db)
    conn = sqlite3.connect(db); conn.execute("DELETE FROM routing_rules")
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                 ("RVG -> Kairos", json.dumps([
                     {"type": "strategy", "value": BREAKOUT},
                     {"type": "broker",   "value": "alpaca-paper-3"}])))
    conn.commit(); conn.close()

    saved_db, saved_hours = a.get_db, a._account_hours_ok
    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c
    monkeypatch.setattr(a, "get_db", _fake_db)
    monkeypatch.setattr(a, "_account_hours_ok", lambda *ar, **kw: True)
    # Neutralize the day-type gate so RVOL is the only thing that can drop the target.
    monkeypatch.setattr(a, "_daytype_gate_block", lambda *ar, **kw: (False, None))
    try:
        with a.app.test_client() as cl:
            cl.post("/webhook?token=test-token",
                    json={"strategy": BREAKOUT, "ticker": "RVG", "action": "BUY"})
    finally:
        a.get_db, a._account_hours_ok = saved_db, saved_hours
    # The only target was gated out — the trade row records the RVOL skip (same as
    # the day-type / reversal gates: drop the target, mark the row skipped).
    c2 = sqlite3.connect(db)
    status, detail = c2.execute(
        "SELECT exec_status, exec_detail FROM trades ORDER BY id DESC LIMIT 1").fetchone()
    c2.close()
    assert status == "skipped"
    assert "rvol" in (detail or "").lower()


def test_webhook_allows_in_band_breakout(gate, monkeypatch, tmp_path):
    """An in-band breakout is NOT skipped by the RVOL gate."""
    import routes.webhook as w  # noqa: F401
    monkeypatch.setattr(a, "_live_rvol", _fake_rvol(1.8))     # in band → allow
    a.ALPACA_ACCOUNTS = [{"tag": "alpaca3", "target_paper": "alpaca-paper-3",
                          "target_live": "alpaca-live-3"}]
    db = tmp_path / "rvg2.db"; shutil.copy("trades.db", db)
    conn = sqlite3.connect(db); conn.execute("DELETE FROM routing_rules")
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                 ("RVG2 -> Kairos", json.dumps([
                     {"type": "strategy", "value": BREAKOUT},
                     {"type": "broker",   "value": "alpaca-paper-3"}])))
    conn.commit(); conn.close()

    saved_db, saved_hours = a.get_db, a._account_hours_ok
    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c
    monkeypatch.setattr(a, "get_db", _fake_db)
    monkeypatch.setattr(a, "_account_hours_ok", lambda *ar, **kw: True)
    monkeypatch.setattr(a, "_daytype_gate_block", lambda *ar, **kw: (False, None))
    try:
        with a.app.test_client() as cl:
            cl.post("/webhook?token=test-token",
                    json={"strategy": BREAKOUT, "ticker": "RVG", "action": "BUY"})
    finally:
        a.get_db, a._account_hours_ok = saved_db, saved_hours
    c2 = sqlite3.connect(db)
    status, detail = c2.execute(
        "SELECT exec_status, exec_detail FROM trades ORDER BY id DESC LIMIT 1").fetchone()
    c2.close()
    # Not an RVOL skip (it may fire or fail at the broker in a test env, but the
    # gate must not be what stopped it).
    assert not (status == "skipped" and "rvol" in (detail or "").lower())
