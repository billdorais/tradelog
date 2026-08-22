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
    exact bug — silently showing a roster the account cannot trade."""
    html = _index()
    i = html.index("async function selectCrew")
    block = html[i:i + 1800]
    assert "if (!_crewSet.size)" in block
    assert "entry source on each pick" in block
