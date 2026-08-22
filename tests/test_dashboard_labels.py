"""Dashboard tiles must not report a quiet market as a broken system, and one
failing panel must not take the rest of the tab switch with it.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")


def _index():
    return open("templates/index.html", encoding="utf-8").read()


def _switch_tab_block(html):
    """The switchTab function body — bounded by the next function, not a fixed
    slice. A magic window silently stops covering the code it was written for."""
    i = html.index("async function switchTab")
    end = html.index("    async function ", i + 10)
    return html[i:end]


def test_no_pnl_today_is_not_reported_as_unconfigured():
    """A normal Saturday — market shut, no fills, daily_pnl null — rendered all
    three curated books as "not configured", which reads as an outage."""
    html = _index()
    i = html.index("async function loadGlancePnl")
    block = html[i:i + 2200]
    assert "no closed trades today" in block, "quiet day still reads as unconfigured"
    assert "if (d.error) {" in block
    assert "if (d.daily_pnl == null) {" in block
    assert "d.error || d.daily_pnl == null" not in block, "the two states are still conflated"


def test_exec_list_is_coerced_before_filtering():
    """renderAlpacaStats fetches fills with `.catch(() => [])`, which only fires on
    a network/parse failure. An endpoint answering with a JSON error OBJECT parses
    fine, arrives as a non-array, and `execs.filter` throws — taking out the rest of
    the tab switch including the loadAccount() call below it. loadTrades already
    guards every one of these with Array.isArray; this call site did not."""
    html = _index()
    i = html.index("async function renderAlpacaStats")
    block = html[i:i + 2600]
    assert "if (!Array.isArray(execs)) execs = [];" in block
    assert block.index("Array.isArray(execs)") < block.index("execs.filter("), \
        "the coercion must come before the filter"


def test_account_card_refreshes_even_if_the_chart_render_fails():
    """Crew Live is REAL MONEY and sorts first, so it is what loads on arrival. A
    throw in renderAlpacaStats used to skip loadAccount() and strand Crew Live's
    buying power on screen under whichever book you had selected."""
    block = _switch_tab_block(_index())
    j = block.index("renderAlpacaStats(")
    k = block.index("loadAccount();", j)
    between = block[max(0, j - 400):k]
    assert "try {" in between and "catch" in between, \
        "renderAlpacaStats is not isolated from loadAccount()"
