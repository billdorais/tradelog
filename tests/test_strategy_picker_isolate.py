"""Isolating a strategy, and narrowing a selection down to its winners/losers.

The picker treats an EMPTY selection as "show everything", so any feature that
*shrinks* a selection has to defend that boundary — narrowing to nothing would
silently display the whole book instead of the nothing the user asked for.
"""
from __future__ import annotations

import os
import re

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")


def _index():
    return open("templates/index.html", encoding="utf-8").read()


def _fn(html, name):
    """Source of one top-level function in the page script."""
    i = html.index("function %s(" % name)
    return html[i:html.index("\n    }", i)]


def test_empty_selection_still_means_show_all():
    """The premise the guards below depend on. If this ever flips, the narrow
    handlers can drop their empty-result check — and not before."""
    html = _index()
    assert "selectedStrategies.size === 0" in html


def test_narrowing_to_nothing_leaves_the_selection_untouched():
    src = _fn(_index(), "narrowByPnl")
    assert "if (!keep.length)" in src
    guard = src[src.index("if (!keep.length)"):]
    assert "return;" in guard.split("selectedStrategies")[0], (
        "must bail out BEFORE assigning; an empty Set inverts the filter to show-all"
    )
    assert "alert(" in guard, "a no-op needs to say why nothing happened"


def test_narrow_starts_from_the_current_selection_not_the_whole_list():
    """"Crew" then "Winners" should give the crew's winners. Reselecting from the
    full list instead would quietly pull in names outside the crew."""
    src = _fn(_index(), "narrowByPnl")
    assert "selectedStrategies.size ? [...selectedStrategies]" in src
    assert "[..._allStrategyNames]" in src, "empty selection means all, so narrow all"


def test_untraded_strategies_are_neither_winners_nor_losers():
    src = _fn(_index(), "narrowByPnl")
    assert "typeof st.pnl !== 'number'" in src and "return false" in src


def test_winners_and_losers_use_strict_sign_so_breakeven_is_excluded():
    src = _fn(_index(), "narrowByPnl")
    assert "st.pnl > 0" in src and "st.pnl < 0" in src
    assert "st.pnl >= 0" not in src, "0.00 is not a win"


def test_isolate_selects_exactly_one_strategy():
    src = _fn(_index(), "isolateStrat")
    assert "new Set([name])" in src
    assert "if (!name) return;" in src, "a blank name would clear the filter to show-all"


def test_isolate_does_not_also_toggle_the_row_it_sits_in():
    """The row itself is a click target (toggleStratVis); without stopPropagation
    isolating would immediately hide the one strategy it just isolated."""
    html = _index()
    assert "e.stopPropagation();" in _fn(html, "isolateStrat")
    row = html[html.index('class="strat-picker-row" onclick'):]
    row = row[:row.index("</div>`")]
    assert "isolateStrat('${esc}', event)" in row, "row must pass the event through"


def test_both_handlers_persist_to_the_tab_and_repaint():
    """Selections are per-tab and the chart caches stats; skipping either leaves
    the screen disagreeing with the filter."""
    html = _index()
    for name in ("narrowByPnl", "isolateStrat"):
        src = _fn(html, name)
        assert "_tabSelections[activeTab] = selectedStrategies;" in src, name
        assert "_cachedFullStats = null;" in src, name
        assert "applyEquityFilter()" in src, name
        assert "_populateStratPicker();" in src, name


def test_winners_and_losers_are_offered_in_the_preset_bar():
    html = _index()
    assert "narrowByPnl(1,event)" in html and "narrowByPnl(-1,event)" in html


def test_solo_control_is_revealed_on_hover_not_always_painted():
    """200+ always-on buttons would compete with the P&L column."""
    html = _index()
    assert re.search(r"\.strat-solo\s*\{[^}]*opacity:\s*0\b", html)
    assert ".strat-picker-row:hover .strat-solo" in html
