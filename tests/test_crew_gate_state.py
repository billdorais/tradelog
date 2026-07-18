"""The crew reports LIVE gate state as fact, not from stale knowledge.

A crew report once wrote "the system does not yet have an automated pre-market CPR
filter; implement manually" while the day-type gate was live on every book,
including the Crew Paper account it was analysing. The crew was never fed the gate
configuration, so it confabulated. _gate_state() assembles that config from the app
so the report states what's actually on.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a
import routes.crew as crew


def test_reports_daytype_gate_on_for_crew_paper():
    """The exact confabulation: the gate is ON for Crew Paper (acct4), and the
    block must say so and flag it as a pre-market filter."""
    gs = crew._gate_state()
    db = gs["daytype_breakout"]
    assert db["on"] is True
    assert "Crew Paper" in db["accounts"]
    assert db["ok_days"] == ["Outside"]
    assert "pre-market" in db["note"].lower()          # kills "no CPR filter" claim
    crew_book = next(b for b in gs["books"] if b["label"] == "Crew Paper")
    assert crew_book["breakout_daytype_gated"] is True


def test_reflects_reversal_policy_per_book():
    gs = crew._gate_state()
    by = {b["label"]: b for b in gs["books"]}
    assert by["TV Refined"]["reversal_policy"] == "off"    # set earlier this session
    assert by["Kairos Refined"]["reversal_policy"] == "long"   # reversal shorts paused
    assert by["Crew Paper"]["reversal_policy"] == "free"


def test_tracks_the_global_toggle(monkeypatch):
    """If the day-type gate is switched off in prod, the block must say OFF —
    otherwise it would assert a filter that isn't running."""
    monkeypatch.setattr(a, "DAYTYPE_GATE_ENABLED", False)
    gs = crew._gate_state()
    assert gs["daytype_breakout"]["on"] is False
    assert all(b["breakout_daytype_gated"] is False for b in gs["books"])


def test_covers_the_three_curated_books():
    gs = crew._gate_state()
    assert [b["label"] for b in gs["books"]] == ["TV Refined", "Kairos Refined", "Crew Paper"]
    for b in gs["books"]:
        # Every book reports each gate as a concrete value, never omitted/None.
        assert isinstance(b["profit_lock"], bool)
        assert isinstance(b["daily_loss_guard"], bool)
        assert b["hours"]                                  # non-empty ("all day" or a window)


def test_gate_state_endpoint():
    a.app.config["TESTING"] = True
    with a.app.test_client() as cl:
        d = cl.get("/api/crew/gate_state").get_json()
    assert d["daytype_breakout"]["on"] is True
    assert "books" in d
