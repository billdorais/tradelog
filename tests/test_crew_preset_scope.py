"""The "Crew" picker preset must reflect what is wired for the book on screen.

On Kairos Refined it selected all 18 crew picks, but Kairos Refined enters via the
engine — only the [Kairos]-wired half can ever trade there. Selecting all 18
implied a roster the account cannot take, and the filtered chart then looked like
those strategies had simply not fired.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")


def _index():
    return open("templates/index.html", encoding="utf-8").read()


def test_crew_set_is_split_by_entry_source():
    html = _index()
    assert "_crewStrategiesTv" in html and "_crewStrategiesKairos" in html
    # Split where the rule nodes are already in hand — no second fetch.
    i = html.index("const hasCrewBroker")
    block = html[i:i + 900]
    assert "n.type === 'entry_source'" in block
    assert "'tv'" in block, "a rule with no entry_source node must default to TV"


def test_preset_follows_the_selected_book():
    html = _index()
    i = html.index("async function selectCrew")
    block = html[i:i + 1800]
    assert "activeTab === 'kairos'" in block and "_crewStrategiesKairos" in block
    assert "activeTab === 'refined'" in block and "_crewStrategiesTv" in block
    # Crew Paper / Crew Live / the farms still take the whole roster.
    assert ": _crewStrategies;" in block


def test_empty_subset_says_why_rather_than_selecting_everything():
    """Falling back to all 18 when a book has no wired picks would reintroduce the
    exact bug — silently showing a roster the account cannot trade. The guard lives
    in _applyCrewSet, which every crew preset routes through."""
    html = _index()
    i = html.index("async function _applyCrewSet")
    block = html[i:i + 1800]
    assert "if (!_crewSet || !_crewSet.size)" in block
    assert "entry source on each pick" in block
    guard = block[block.index("if (!_crewSet || !_crewSet.size)"):]
    assert "return;" in guard.split("selectedStrategies =")[0],         "must bail out before assigning — an empty Set means show-all"


def test_every_crew_preset_routes_through_the_same_guard():
    """A preset that skipped it would silently select everything on an empty set."""
    html = _index()
    for fn in ("selectCrew", "selectCrewTv", "selectCrewKairos"):
        i = html.index("async function %s(" % fn)
        assert "_applyCrewSet(" in html[i:i + 1100], fn


def test_the_farm_tabs_get_explicit_per_mechanism_presets():
    """Auto-scoping keys off the account being viewed, which is wrong on the farms:
    both farms run EVERY strategy, so the [Kairos]-tagged picks have TV-entry fills
    on TV Farm. That cross view is the head-to-head, not a mistake."""
    html = _index()
    assert "selectCrewTv(event)" in html and "selectCrewKairos(event)" in html
    assert ">Crew TV<" in html and ">Crew Kairos<" in html
