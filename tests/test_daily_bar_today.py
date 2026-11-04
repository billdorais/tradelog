"""The Daily P&L bar chart's today bar must not be replaced by the account-wide
figure while the chart is filtered. It was: a strategy subset showed a green day
for the selected strategies, then today's bar became the whole farm's red total.
"""
from __future__ import annotations


def _render_block():
    html = open("templates/index.html", encoding="utf-8").read()
    i = html.index("async function renderAlpacaStats")
    end = html.index("    async function ", i + 10)
    return html[i:end]


def test_today_override_only_for_the_whole_account():
    block = _render_block()
    call = block[block.index("drawNetDailyChart("):]
    call = call[:call.index(";")]
    assert "_wholeAccount" in call, "today's bar still takes the account-wide P&L unconditionally"
    guard = block[block.index("const _wholeAccount"):block.index("drawNetDailyChart(")]
    for f in ("selectedStrategies.size", "_sideFilter", "_signalsOnly", "_hoursWindow", "_loadExclusions()"):
        assert f in guard, f"{f} filter does not suppress the account-wide today bar"


def test_today_override_respects_the_date_range():
    block = _render_block()
    assert "_todayInRange" in block[block.index("drawNetDailyChart("):][:200]
