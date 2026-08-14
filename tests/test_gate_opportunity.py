"""Gate opportunity cost — price each blocked entry against the ungated farm.

The farms are deliberately ungated on hours / RVOL / profit-lock / daily-loss, so
when one of those stops a curated book the farm usually still took the setup. That
farm round-trip is the counterfactual. The interesting cases are the ones where
there ISN'T one: day-type is ON for the farms too, so those blocks have no control
group and must be reported as unanswerable rather than as zero.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as dt
import sqlite3

import pytest

import app as a


TODAY = dt.datetime.now(dt.timezone.utc).date().isoformat()


def _rt(ticker, strategy, pnl, entry_price=100.0, qty=10, date=TODAY):
    return {"ticker": ticker, "strategy": strategy, "pnl": pnl, "date": date,
            "entry_price": entry_price, "qty": qty, "side": "LONG",
            "entry_time": f"{date}T14:00:00Z", "exit_time": f"{date}T14:30:00Z"}


@pytest.fixture()
def opp(monkeypatch, tmp_path):
    db = tmp_path / "opp.db"
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE blocked_targets (
        ts TEXT NOT NULL, account TEXT NOT NULL, ticker TEXT, strategy TEXT,
        side TEXT, gate TEXT NOT NULL, reason TEXT, source TEXT)""")
    conn.commit(); conn.close()

    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c

    monkeypatch.setattr(a, "get_db", _fake_db)
    with a._blocked_q_lock:
        a._blocked_queue.clear()
    a._blocked_seen.clear(); a._blocked_seen_day = None

    farm_rts = []
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [
        {"tag": "alpaca", "num": "1", "label": "TV Farm", "fills_fn": lambda: ["x"]},
        {"tag": "alpaca4", "num": "4", "label": "Crew Paper", "fills_fn": lambda: []},
    ])
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda fills, **kw: {"closed_clean": list(farm_rts)})
    a.app.config["TESTING"] = True
    yield farm_rts
    with a._blocked_q_lock:
        a._blocked_queue.clear()


def _block(gate, ticker, strategy="S_CAM_BREAKOUT_R3S3"):
    a._record_block("alpaca4", ticker, strategy, "long", gate, "reason")
    a._flush_blocked_targets()


def _gates(days=14):
    with a.app.test_client() as c:
        d = c.get(f"/api/signals/gate_opportunity?days={days}&account=alpaca4").get_json()
    return d, {g["gate"]: g for g in d["by_gate"]}


def test_gate_that_blocked_a_winner_is_reported_as_costing_money(opp):
    farm_rts = opp
    farm_rts.append(_rt("AAPL", "S_CAM_BREAKOUT_R3S3", pnl=50.0))   # farm took it, +$50
    _block("hours", "AAPL")
    d, g = _gates()
    assert g["hours"]["blocked"] == 1 and g["hours"]["matched"] == 1
    assert g["hours"]["avg_pct"] == pytest.approx(5.0)      # 50 / (100*10) = 5%
    assert g["hours"]["verdict"] == "gate COST money"


def test_gate_that_blocked_a_loser_is_reported_as_saving_money(opp):
    farm_rts = opp
    farm_rts.append(_rt("TSLA", "S_CAM_BREAKOUT_R3S3", pnl=-80.0))
    _block("rvol", "TSLA")
    _, g = _gates()
    assert g["rvol"]["verdict"] == "gate SAVED money"
    assert g["rvol"]["avg_pct"] < 0


def test_day_type_has_no_control_group(opp):
    """The farms are day-type gated too, so those blocks are unanswerable — they
    must NOT be scored as zero, which would read as 'the gate cost nothing'."""
    farm_rts = opp
    farm_rts.append(_rt("UBER", "S_CAM_BREAKOUT_R3S3", pnl=99.0))   # must be ignored
    _block("day-type", "UBER")
    _, g = _gates()
    row = g["day-type"]
    assert row["unanswerable"] is True
    assert row["matched"] == 0 and row["farm_pnl"] == 0.0
    assert "no control group" in row["verdict"]


def test_no_farm_match_is_distinguished_from_a_zero_result(opp):
    """The farm not taking it at all is a different statement from breakeven."""
    _block("hours", "NOSUCH")
    _, g = _gates()
    assert g["hours"]["blocked"] == 1 and g["hours"]["matched"] == 0
    assert g["hours"]["verdict"] == "no farm match"


def test_loose_match_is_flagged_when_the_strategy_differs(opp):
    """Same ticker and day but a different strategy still gives a usable read —
    but it is a weaker claim, so it is counted separately."""
    farm_rts = opp
    farm_rts.append(_rt("GOOG", "OTHER_CAM_REVERSAL_R4S4", pnl=20.0))
    _block("profit-lock", "GOOG", strategy="GOOG_CAM_BREAKOUT_R3S3")
    _, g = _gates()
    assert g["profit-lock"]["matched"] == 1
    assert g["profit-lock"]["loose_matches"] == 1


def test_percent_normalisation_not_raw_dollars(opp):
    """Farms are equal-dollar sized, so raw $ are not transferable to Crew. A big
    farm position and a small one with the same % must score the same."""
    farm_rts = opp
    farm_rts.append(_rt("AAPL", "S_CAM_BREAKOUT_R3S3", pnl=100.0,
                        entry_price=100.0, qty=20))   # 100/2000 = 5%
    _block("hours", "AAPL")
    _, g = _gates()
    assert g["hours"]["avg_pct"] == pytest.approx(5.0)
    assert g["hours"]["farm_pnl"] == 100.0          # dollars still shown for reference
