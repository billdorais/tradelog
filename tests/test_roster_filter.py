"""A one-click filter for the strategies a book actually trades.

Paging week by week to compare Kairos Select against the farm it was drawn from
only works if the basket is held FIXED across the windows. Two things make that
true, and both are easy to get wrong:

  - The roster is read from the ROUTING RULES, not kept as a copy in the template.
    A hardcoded list would drift the moment a rule changed, and the comparison
    would quietly be against the wrong basket.
  - The dashboard ADDS newly-appearing strategies to the current selection when
    the window changes (so a manual pick stays useful as names appear). For a
    pinned roster that is exactly wrong -- paging to a week where a new name
    traded would silently widen the basket -- so the roster is re-applied after.
"""
from __future__ import annotations

import json
import os
import re

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


class _Conn:
    def __init__(self, rows): self.rows = rows
    def execute(self, *_a, **_k): return self
    def fetchall(self): return [(json.dumps(n),) for n in self.rows]
    def close(self): pass


def _rules(monkeypatch, rows):
    monkeypatch.setattr(a, "get_db", lambda *_a, **_k: _Conn(rows))
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS",
                        [{"num": "7", "tag": "alpaca7", "label": "Kairos Select",
                          "target_paper": "alpaca-paper-7", "target_live": "alpaca-live-7"}])


def test_the_roster_is_the_strategies_routed_to_that_account(monkeypatch):
    _rules(monkeypatch, [
        [{"type": "strategy", "value": "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"},
         {"type": "broker", "value": "alpaca-paper-7"}],
        [{"type": "strategy", "value": "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"},
         {"type": "broker", "value": "alpaca-paper-7"}],
        [{"type": "strategy", "value": "TSLA_CAM_BREAKOUT_R4S4_V02_5MIN"},
         {"type": "broker", "value": "alpaca-paper-2"}],      # a different book
    ])
    assert a._wired_roster("alpaca7") == ["AAPL_CAM_BREAKOUT_R4S4_V02_5MIN",
                                          "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"]


def test_a_book_with_no_explicit_rules_has_no_roster(monkeypatch):
    """The farms are fed by a blanket pilot fan-out. "Every strategy" is not a
    roster, and a chip offering it would filter to nothing meaningful."""
    _rules(monkeypatch, [[{"type": "strategy", "value": "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"},
                          {"type": "broker", "value": "alpaca-paper-2"}]])
    assert a._wired_roster("alpaca5") == []


def test_pattern_rules_are_not_roster_members(monkeypatch):
    """A wildcard names a family, not a strategy -- it cannot be selected in a
    picker that lists concrete names."""
    _rules(monkeypatch, [[{"type": "strategy", "value": "*_CAM_BREAKOUT_*"},
                          {"type": "broker", "value": "alpaca-paper-7"}]])
    assert a._wired_roster("alpaca7") == []


def test_an_unconfigured_account_yields_nothing(monkeypatch):
    """_routing_broker_to_tag returns None for a slot with no keys, so the rule
    matches no account rather than falling back to a default one."""
    _rules(monkeypatch, [[{"type": "strategy", "value": "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"},
                          {"type": "broker", "value": "alpaca-paper-9"}]])
    assert a._wired_roster("alpaca9") == []


def test_a_broken_rules_table_does_not_take_the_dashboard_down(monkeypatch):
    """The roster is a convenience. It must degrade to "no chip", never to a 500
    on the main page."""
    def _boom(*_a, **_k): raise RuntimeError("db gone")
    monkeypatch.setattr(a, "get_db", _boom)
    assert a._wired_roster("alpaca7") == []


# ── the page ────────────────────────────────────────────────────────────────

def _page(monkeypatch, roster_for):
    monkeypatch.setattr(a, "_ui_accounts", lambda: [
        {"num": "5", "tag": "alpaca5", "label": "Kairos Farm", "color": "#5FC8D4",
         "paper": True, "tab": "farm"},
        {"num": "7", "tag": "alpaca7", "label": "Kairos Select", "color": "#B8A1E3",
         "paper": True, "tab": "select"}])
    monkeypatch.setattr(a, "_wired_roster", roster_for)
    a.app.config["TESTING"] = True
    return a.app.test_client().get("/").get_data(as_text=True)


def test_the_chip_appears_only_for_books_that_have_a_roster(monkeypatch):
    html = _page(monkeypatch, lambda tag: ["AAPL_X"] if tag == "alpaca7" else [])
    assert 'id="rosterChip_select"' in html
    assert 'id="rosterChip_farm"' not in html


def test_the_names_reach_the_page_from_the_rules(monkeypatch):
    html = _page(monkeypatch, lambda tag: ["AAPL_X", "NVDA_Y"] if tag == "alpaca7" else [])
    m = re.search(r"const _ROSTERS = (\{.*?\});", html, re.S)
    assert m, "roster data missing from the dashboard"
    assert json.loads(m.group(1))["select"]["strategies"] == ["AAPL_X", "NVDA_Y"]


def test_the_roster_is_reapplied_after_the_auto_add(monkeypatch):
    """Order matters: the dashboard adds newly-appearing names to the selection
    when the window changes, so the roster has to be restored AFTER that, or
    paging would widen the basket a name at a time."""
    html = _page(monkeypatch, lambda tag: ["AAPL_X"] if tag == "alpaca7" else [])
    i_add = html.index("Add newly appearing strategies as visible")
    i_fix = html.index("_reapplyRoster();")
    i_pop = html.index("_populateStratPicker();", i_add)
    assert i_add < i_fix < i_pop, "roster is re-applied before the auto-add, or after the repaint"


def test_choosing_a_top_n_unpins_the_chip(monkeypatch):
    """A lit chip over a selection that is no longer the roster is worse than no
    chip at all."""
    html = _page(monkeypatch, lambda tag: ["AAPL_X"] if tag == "alpaca7" else [])
    for anchor in ("selectedStrategies = new Set(top);",
                   "selectedStrategies = new Set(refined);"):
        i = html.index(anchor)
        assert "_unpinRoster();" in html[i:i + 120], f"no unpin after {anchor}"
