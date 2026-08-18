"""Opening-location entry gate (Thor Young "exhausted on arrival").

Blocks a BREAKOUT whose session OPENED already at/past the level it is trying to
break. Default OFF and LONG-only, matching where the evidence is: LONG at/past
extreme was 19 trades at -$385 (largest bucket, biggest loser) while SHORT
at/past extreme was 7 trades at -$23 — noise. Gating both sides would cost real
sample for no demonstrated benefit.

Shares _open_frac/_open_bucket with the Long/Short diagnostic so the gate cannot
disagree with the numbers the decision was made on.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a

BRK = "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"
REV = "AAPL_CAM_REVERSAL_R4S4_V02_5MIN"
# mid 100, R4 110 => a LONG open at 112 is past the level (frac 1.2)
CLS = {"mid_cpr": 100.0, "r4": 110.0, "r3": 105.0, "s4": 90.0, "s3": 95.0}


@pytest.fixture()
def on(monkeypatch):
    monkeypatch.setattr(a, "OPEN_LOC_GATE_ENABLED", True)
    monkeypatch.setattr(a, "OPEN_LOC_GATE_ACCOUNTS", {"alpaca2", "alpaca3", "alpaca4"})
    monkeypatch.setattr(a, "OPEN_LOC_GATE_BUCKETS", {"at/past extreme"})
    monkeypatch.setattr(a, "OPEN_LOC_GATE_SIDES", {"long"})
    monkeypatch.setattr(a, "_account_gate_overrides", lambda tag=None: {})
    monkeypatch.setattr(a, "_get_day_classification", lambda tk, d: dict(CLS))
    return monkeypatch


def _open_at(monkeypatch, px):
    monkeypatch.setattr(a, "_get_day_open", lambda tk, d: px)


def test_blocks_a_long_breakout_that_opened_past_the_level(on, monkeypatch):
    _open_at(monkeypatch, 112.0)                      # frac 1.2 -> at/past extreme
    blocked, why, bucket = a._open_location_gate_block(BRK, "AAPL", "2026-08-18", "LONG", "alpaca4")
    assert blocked is True and bucket == "at/past extreme"
    assert "opened 'at/past extreme'" in why


def test_allows_a_long_breakout_with_room(on, monkeypatch):
    _open_at(monkeypatch, 102.0)                      # frac 0.2 -> near CPR
    blocked, _why, bucket = a._open_location_gate_block(BRK, "AAPL", "2026-08-18", "LONG", "alpaca4")
    assert blocked is False and bucket == "near CPR (room)"


def test_shorts_pass_by_default(on, monkeypatch):
    """SHORT at/past extreme was 7 trades at -$23. Not evidence — do not gate it."""
    _open_at(monkeypatch, 88.0)                       # short: mid 100, S4 90 -> frac 1.2
    # The classifier does see this as at/past extreme...
    assert a._open_bucket(a._open_frac(BRK, "SHORT", CLS, 88.0)) == "at/past extreme"
    # ...but the gate leaves SHORT alone, and short-circuits before spending a data
    # fetch on a side it was never going to act on (hence bucket is None).
    blocked, _why, bucket = a._open_location_gate_block(BRK, "AAPL", "2026-08-18", "SHORT", "alpaca4")
    assert blocked is False and bucket is None


def test_reversals_are_never_gated(on, monkeypatch):
    _open_at(monkeypatch, 112.0)
    assert a._open_location_gate_block(REV, "AAPL", "2026-08-18", "LONG", "alpaca4")[0] is False


def test_farms_are_exempt_so_the_gate_stays_priceable(on, monkeypatch):
    """The farms are the control group for gate-cost analysis. Gating them would
    leave this gate's blocks with no counterfactual, exactly like day-type was."""
    _open_at(monkeypatch, 112.0)
    for farm in a._FARM_TAGS:
        assert a._open_location_gate_block(BRK, "AAPL", "2026-08-18", "LONG", farm)[0] is False
    assert not (set(a._FARM_TAGS) & set(a.OPEN_LOC_GATE_ACCOUNTS))


def test_off_by_default():
    """Ships disabled — flipped on from Routing once the operator decides."""
    assert a.OPEN_LOC_GATE_ENABLED is False
    assert a.OPEN_LOC_GATE_SIDES == {"long"}


def test_fails_open_on_missing_data(on, monkeypatch):
    """No session open, or an unclassifiable ticker, must ALLOW. A gate that blocks
    on missing data silently stops trading when a feed hiccups."""
    _open_at(monkeypatch, None)
    assert a._open_location_gate_block(BRK, "AAPL", "2026-08-18", "LONG", "alpaca4")[0] is False
    _open_at(monkeypatch, 112.0)
    monkeypatch.setattr(a, "_get_day_classification", lambda tk, d: {})
    assert a._open_location_gate_block(BRK, "AAPL", "2026-08-18", "LONG", "alpaca4")[0] is False
    def _boom(tk, d): raise RuntimeError("no bars")
    monkeypatch.setattr(a, "_get_day_classification", _boom)
    assert a._open_location_gate_block(BRK, "AAPL", "2026-08-18", "LONG", "alpaca4")[0] is False


def test_per_account_override(on, monkeypatch):
    """One book can opt in to both sides, or out entirely, without touching others."""
    _open_at(monkeypatch, 88.0)                       # SHORT at/past extreme
    monkeypatch.setattr(a, "_account_gate_overrides",
                        lambda tag=None: {"open_loc": {"enabled": True,
                                                       "buckets": ["at/past extreme"],
                                                       "sides": ["long", "short"]}}
                        if tag == "alpaca3" else {})
    assert a._open_location_gate_block(BRK, "AAPL", "2026-08-18", "SHORT", "alpaca3")[0] is True
    assert a._open_location_gate_block(BRK, "AAPL", "2026-08-18", "SHORT", "alpaca4")[0] is False


def test_shares_the_diagnostic_classifier():
    """Gate and diagnostic must agree — one implementation, not two."""
    frac = a._open_frac(BRK, "LONG", CLS, 112.0)
    assert frac == pytest.approx(1.2)
    assert a._open_bucket(frac) == "at/past extreme"
    assert a._open_bucket(a._open_frac(BRK, "LONG", CLS, 100.0)) == "near CPR (room)"
    assert a._open_bucket(a._open_frac(BRK, "LONG", CLS, 106.0)) == "mid-travel"
    assert a._open_bucket(a._open_frac(BRK, "LONG", CLS, 109.0)) == "extended"


def test_toggle_round_trips_through_the_risk_endpoint(monkeypatch):
    """Flippable from Routing without a deploy, and reported back in risk status."""
    monkeypatch.setattr(a, "_update_env_file", lambda *args, **kw: None)
    monkeypatch.setattr(a, "_save_setting", lambda *args, **kw: None)
    monkeypatch.setattr(a, "OPEN_LOC_GATE_ENABLED", False)
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        r = c.post("/api/risk/limit", json={"open_loc_gate_enabled": True})
        assert r.status_code == 200
        assert "open_loc_gate_enabled" in (r.get_json().get("changed") or [])
    assert a.OPEN_LOC_GATE_ENABLED is True
    a.OPEN_LOC_GATE_ENABLED = False
