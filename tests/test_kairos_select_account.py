"""Kairos Select (account 7) — a frozen 15-name roster, run as an experiment.

The names were chosen because they were the Kairos Farm's winners over
2026-08-01..10-01. That is IN-SAMPLE selection, and the farm's own walk-forward
(/api/backtest/score_shootout) put its ranking at t=0.41 with the picked cohort
still losing. A paper book trading a FROZEN list is the instrument that settles
whether the selection carries forward.

Two properties make it a test rather than a story, and both are asserted here:

  1. The gates MIRROR THE FARM. The names were measured ungated and all-day; a
     gate here would confound a forward shortfall between "the edge was noise"
     and "a gate got in the way".
  2. It is not fed by any auto-source. A snapshot or pilot feed would re-pick the
     roster, which is the curve-fit this is meant to test rather than repeat.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

import app as a


SELECT = "7"
FARM   = "5"          # Kairos Farm — the book the roster was drawn from

# Everything that changes which trades the book is allowed to take.
GATE_FLAGS = ("daytype_gate", "reversal_gate", "reversal_side", "profit_lock",
              "daily_loss_guard", "rvol_gate", "open_loc_gate", "hours_key")


def test_the_account_is_registered():
    meta = a.ACCOUNT_META[SELECT]
    assert meta["tag"] == "alpaca7"
    assert meta["label"] == "Kairos Select"


def test_its_gates_mirror_the_farm_it_was_drawn_from():
    """If this ever diverges, forward results stop being comparable to the farm
    numbers that chose the roster -- the comparison would measure the config
    difference instead of the strategies."""
    sel, farm = a.ACCOUNT_META[SELECT], a.ACCOUNT_META[FARM]
    for f in GATE_FLAGS:
        assert sel.get(f) == farm.get(f), f"{f}: Select {sel.get(f)!r} != Farm {farm.get(f)!r}"


def test_it_trades_all_day_like_the_farm():
    """No hours_key means no curated window. 36% of the measured edge is earned
    outside curated hours, so a window here would silently discard it."""
    assert "hours_key" not in a.ACCOUNT_META[SELECT]


def test_no_auto_source_can_repick_the_roster():
    """The roster is frozen by construction: entries come only from explicitly
    wired rules, never from a snapshot or pilot feed that re-selects winners."""
    assert a.ACCOUNT_META[SELECT]["auto_source"] is False


def test_it_is_a_paper_slot_in_the_ui_order_and_has_a_tab_key():
    assert SELECT in a.UI_ACCOUNT_ORDER
    assert a._TAB_KEY_BY_NUM.get(SELECT) == "select"
    # A slot with no tab key renders a button switchTab cannot handle.
    for n in a.ACCOUNT_META:
        assert n in a._TAB_KEY_BY_NUM, f"account {n} has no tab key"


def test_an_unconfigured_target_resolves_to_nothing_rather_than_a_fallback():
    """The rules name alpaca-paper-7 before the keys exist. Resolution must return
    None (the engine then skips the target) -- a fallback to the default account
    would fire 15 strategies at $15k into the wrong book."""
    configured = {x["tag"] for x in a.ALPACA_ACCOUNTS}
    if "alpaca7" in configured:
        return                                   # keys present; nothing to prove
    assert a._routing_broker_to_tag("alpaca-paper-7") is None
    assert a._routing_broker_to_tag("alpaca-live-7") is None


def test_the_slot_fits_within_the_scan_limit():
    assert a.MAX_ALPACA_ACCOUNTS >= int(SELECT)
