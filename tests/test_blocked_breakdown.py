"""Signal block-reason categorization — _categorize_block_reason.

Buckets a signal's stored exec_status/exec_detail into the gate that stopped it,
so the Analysis panel can show why entries didn't trade.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a


def test_categories_match_stored_reason_strings():
    cat = a._categorize_block_reason
    assert cat("ok", None) == "Executed"
    assert cat("error", "broker rejected") == "Error"
    # Exact strings the webhook writes:
    assert cat("skipped", "RVOL gate: entry below the 1.50x RVOL floor (RVOL 0.90x)") == "RVOL gate"
    assert cat("skipped", "day-type gate: breakout blocked on Inside day (Outside only)") == "Day-type gate"
    assert cat("skipped", "reversal policy off for this account") == "Reversal gate"
    assert cat("skipped", "side gate: long-only, short entry dropped") == "Side gate"
    assert cat("blocked", "regime gate: VIX > 25") == "Regime gate"
    assert cat("skipped", "something unmapped") == "Other block"


def test_day_type_precedence_over_reversal_wording():
    # A reversal blocked by the day-type gate mentions BOTH words; it's a day-type block.
    cat = a._categorize_block_reason
    assert cat("skipped", "day-type gate: reversal blocked on Outside day (Inside only)") == "Day-type gate"


def test_endpoint_shape_no_signals(monkeypatch):
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE trades (received_at TEXT, ticker TEXT, strategy TEXT, "
                 "action TEXT, sentiment TEXT, exec_status TEXT, exec_detail TEXT)")
    conn.commit()
    monkeypatch.setattr(a, "get_db", lambda: conn)
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/signals/blocked_breakdown?days=7").get_json()
    assert d["total_entries"] == 0 and d["executed"] == 0 and d["by_reason"] == []
