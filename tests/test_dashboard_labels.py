"""Dashboard tiles must not report a quiet market as a broken system.

A normal Saturday — market shut, no fills, daily_pnl null — rendered all three
curated books as "not configured", which reads as an outage rather than a weekend.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")


def _index():
    return open("templates/index.html", encoding="utf-8").read()


def test_no_pnl_today_is_not_reported_as_unconfigured():
    html = _index()
    i = html.index("async function loadGlancePnl")
    block = html[i:i + 2200]
    assert "no closed trades today" in block, "quiet day still reads as unconfigured"
    # The two states must be separate branches, not one condition.
    assert "if (d.error) {" in block
    assert "if (d.daily_pnl == null) {" in block
    assert "d.error || d.daily_pnl == null" not in block, "the two states are still conflated"
