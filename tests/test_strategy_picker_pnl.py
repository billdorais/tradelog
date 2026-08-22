"""The strategy picker shows P&L per strategy for the book and range on screen."""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")


def _index():
    return open("templates/index.html", encoding="utf-8").read()


def test_pnl_comes_from_the_analysis_response_already_fetched():
    """renderAlpacaStats already pulls per_strategy for the chart; the picker reuses
    it rather than issuing a second request, so the dropdown and the chart beside it
    can never disagree."""
    html = _index()
    i = html.index("async function renderAlpacaStats")
    block = html[i:i + 3000]
    assert "_pnlByStrategy = {}" in block
    assert "data.per_strategy" in block
    assert "_populateStratPicker()" in block, "picker not repainted once P&L arrives"


def test_untraded_strategies_show_no_amount_rather_than_zero():
    """"Did not trade" and "traded to breakeven" are different statements. A column
    of $0.00 reads as the second."""
    html = _index()
    i = html.index("function _populateStratPicker")
    block = html[i:i + 2200]
    assert "pnl === null ? '' :" in block


def test_amount_is_coloured_by_sign_and_right_aligned():
    html = _index()
    i = html.index("function _populateStratPicker")
    block = html[i:i + 2200]
    assert "#7FE098" in block and "#ef5350" in block
    assert "margin-left:auto" in block, "amount should sit in its own right-hand column"
    assert "tabular-nums" in block, "digits should align down the column"


def test_trade_count_is_shown_not_just_hinted():
    """Sample size decides whether a number means anything, so it is painted in the
    row rather than hidden behind a tooltip."""
    html = _index()
    i = html.index("function _populateStratPicker")
    block = html[i:i + 3200]
    assert "st.trades" in block
    assert "${n}t" in block, "trade count should be visible in the row"


def test_per_trade_average_is_shown_beside_the_total():
    """Total P&L is tenure-biased on the Refined books — they are top-20 rosters
    rewired daily, so a longer-tenured name wins on total without being better per
    trade. The average is the figure that compares like with like."""
    html = _index()
    i = html.index("function _populateStratPicker")
    block = html[i:i + 3200]
    assert "pnl / n" in block
    assert "avg === null" in block, "no average for a strategy with zero trades"
