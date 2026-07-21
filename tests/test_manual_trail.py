"""Manual Pull-Stop trail override — the gap a BE/halfway pull creates becomes the
new trailing distance the Kairos trail monitor uses for that position (replacing the
routing rule's trail_pct, bypassing tiers). Cleared when the position closes.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import logging

import pytest

import app as a


def test_manual_trail_roundtrip():
    a._set_manual_trail("alpaca3", "NVDA", 0.12)
    assert a._get_manual_trail("alpaca3", "nvda") == 0.12   # case-insensitive
    assert a._get_manual_trail("alpaca3", "AAPL") is None


class _FakeBroker:
    def __init__(self, positions): self._p = positions
    def _invalidate_pos_cache(self): pass
    def get_positions(self): return [dict(p) for p in self._p]
    def close_position(self, sym): return {"success": True}


@pytest.fixture()
def _monitor(monkeypatch):
    # Only the Kairos trail can fire — silence the other stop layers.
    for name, val in (("MAX_POSITION_LOSS_PCT", 0.0), ("MAX_POSITION_LOSS_REFINED", 0.0),
                      ("MAX_TRAILING_GIVEBACK", 0.0), ("TAKE_PROFIT_DOLLARS", 0.0),
                      ("TAKE_PROFIT_PCT", 0.0)):
        monkeypatch.setattr(a, name, val)
    monkeypatch.setattr(a, "_resolve_position_entry", lambda *ar, **kw: (None, None))
    monkeypatch.setattr(a, "_get_route_trail_pct", lambda s: 0.0)   # NO rule trail
    # OVR: long 10 @100, peak 110 (pnl 100), now 109 — gave back 0.9% from peak.
    # CTRL: same shape but no override → nothing should trail it.
    pos = [
        {"symbol": "OVR",  "qty": 10, "unrealized_pnl": 90.0, "market_value": 1090.0,
         "avg_entry_price": 100.0, "current_price": 109.0},
        {"symbol": "CTRL", "qty": 10, "unrealized_pnl": 90.0, "market_value": 1090.0,
         "avg_entry_price": 100.0, "current_price": 109.0},
    ]
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS",
                        [{"tag": "alpaca3", "num": 3, "label": "Kairos", "broker": _FakeBroker(pos)}])
    with a._risk_lock:
        a._position_peaks[("alpaca3", "OVR")]  = 100.0   # peak_px = 110
        a._position_peaks[("alpaca3", "CTRL")] = 100.0
        a._auto_closed_symbols.clear()
        a._manual_trail_pct.clear()
    return monkeypatch


def _run_capture():
    recs = []
    class _H(logging.Handler):
        def emit(self, r): recs.append(r.getMessage())
    h = _H(); a.log.addHandler(h)
    try:    a._check_position_stops()
    finally: a.log.removeHandler(h)
    return " ".join(recs)


def test_override_drives_the_trail(_monitor):
    # A 0.5% manual trail: stop = 110*(1-0.005)=109.45; current 109 < that → fires.
    a._set_manual_trail("alpaca3", "OVR", 0.5)
    fired = _run_capture()
    assert "kairos_trail" in fired and "[alpaca3] — OVR" not in fired  # sanity on format
    assert "OVR" in fired
    # CTRL has no override and no rule trail → never trailed/closed.
    assert "CTRL" not in fired


def test_no_override_no_trail(_monitor):
    # Without any override and no rule trail, nothing fires for OVR either.
    fired = _run_capture()
    assert "kairos_trail" not in fired


def test_override_cleared_when_position_closes(_monitor):
    a._set_manual_trail("alpaca3", "OVR", 0.5)
    # Broker now reports NO open positions → stale cleanup drops peaks + the override.
    a.ALPACA_ACCOUNTS[0]["broker"] = _FakeBroker([])
    a._check_position_stops()
    assert a._get_manual_trail("alpaca3", "OVR") is None


# ── halfway references the LIVE trail (tightens after a run-up) ──────────────

from types import SimpleNamespace


class _PullBroker:
    """Minimal broker for the pull_stop endpoint: snapshot + replace_stop echo."""
    def __init__(self, entry, cur, qty): self._pos = SimpleNamespace(
        avg_entry_price=entry, current_price=cur, qty=qty)
    def _ensure_client(self): pass
    @property
    def _trading(self):
        outer = self
        class _T:
            def get_open_position(self, sym): return outer._pos
        return _T()
    def replace_stop(self, sym, new_stop):
        return {"success": True, "new_stop_price": round(float(new_stop), 4), "prev_stop_price": None}


def test_halfway_references_live_trail_and_tightens(monkeypatch):
    # Long 10 @100, ran up to 110 (peak_px 110). A prior BE pull left a 1.0% trail.
    br = _PullBroker(100.0, 110.0, 10.0)
    monkeypatch.setattr(a, "_alpaca_account_ctx", lambda acct: (br, "alpaca3", "Kairos", lambda: []))
    with a._risk_lock:
        a._position_peaks[("alpaca3", "OVR")] = 100.0   # peak_px = 110
        a._manual_trail_pct[("alpaca3", "OVR")] = 1.0    # current trail 1.0%
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.post("/api/alpaca/pull_stop/OVR?account=3&token=test-token",
                   json={"mode": "halfway"}).get_json()
    # Live trail level = 110*(1-0.01)=108.9; new stop = midpoint(108.9,110)=109.45;
    # new trail = (110-109.45)/110 = 0.5% → TIGHTER than the 1.0% it referenced.
    assert d["success"] is True
    assert d["new_stop_price"] == 109.45
    assert d["new_trail_pct"] == 0.5
    assert a._get_manual_trail("alpaca3", "OVR") == 0.5   # override tightened, not loosened
