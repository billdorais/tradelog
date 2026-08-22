"""The crew report stitches together blocks covering DIFFERENT windows.

The farms are pinned to a fixed 45-day trailing window and the engine compare to a
fixed 30 days, while the Refined books follow the report's range. Nothing used to
state that, so a default run put a one-session book next to a six-week farm and read
them as comparable. These pin the labelling that makes the mismatch visible.
"""
from __future__ import annotations

import inspect
import os
import re

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

import routes.crew as crew


def _crewhtml():
    return open("templates/crew.html", encoding="utf-8").read()


def test_default_range_is_not_a_single_session():
    """'Today' judged the primary book on one day. Whatever the default becomes, it
    must not be that."""
    html = _crewhtml()
    m = re.search(r"let _activeRange = '([a-z_]+)';", html)
    assert m, "could not find the default range"
    assert m.group(1) != "today"


def test_default_range_matches_the_farm_window():
    """Book and audition pool should cover the same ground; 45 is _FARM_WINDOW_DAYS."""
    html = _crewhtml()
    m = re.search(r"let _activeRange = '([a-z_]+)';", html)
    assert m.group(1) == "farm_window"
    i = html.index("_activeRange === 'farm_window'")
    assert "setDate(today.getDate() - 45)" in html[i:i + 300]
    src = inspect.getsource(crew._prep_and_run_kairos)
    assert "_FARM_WINDOW_DAYS = 45" in src, "farm window moved; update the UI default too"


def test_the_new_range_is_offered_as_a_button():
    html = _crewhtml()
    assert "setRange('farm_window')" in html
    assert "Last 45 Days" in html


def test_every_window_is_stated_in_the_prompt():
    src = inspect.getsource(crew._run_kairos_crew)
    assert "WINDOWS COVERED BY EACH BLOCK" in src
    for key in ("analysis", "farm", "engine"):
        assert '_w.get("%s")' % key in src, key


def test_the_prompt_says_the_windows_differ():
    """Listing them is not enough; the crew has to be told not to compare across."""
    src = inspect.getsource(crew._run_kairos_crew)
    assert "These windows DIFFER" in src
    assert "longer window" in src


def test_the_prompt_warns_that_refined_totals_are_tenure_biased():
    """Top-20 rosters rewired daily: total P&L partly measures days-wired."""
    src = inspect.getsource(crew._run_kairos_crew)
    assert "REWIRED DAILY" in src
    assert "per-trade" in src


def test_windows_block_is_omitted_rather_than_faked_when_prep_failed():
    """The prep runs under a bare except; a half-built label is worse than none."""
    src = inspect.getsource(crew._run_kairos_crew)
    assert "_w = windows or {}" in src
    assert 'windows_block = ""' in src
    assert "if _w:" in src


def test_windows_are_labelled_from_the_values_the_queries_used():
    """A hardcoded label drifts the moment the window changes."""
    src = inspect.getsource(crew._prep_and_run_kairos)
    assert "_FARM_WINDOW_DAYS}d trailing window" in src
    assert "windows=_win" in src
    assert '_win         = {}' in src, "must exist even if the prep block raises"


def test_stale_20_day_header_is_gone():
    """acct2's leaderboard spans the report's range, not a fixed 20 days."""
    assert "last ~20 days" not in inspect.getsource(crew)
