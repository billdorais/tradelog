"""LIFO fill pairing is POSITION-STATE-FIRST, not sentiment-driven.

Regression: a clean same-day BUY→SELL round-trip on an engine account (acct3)
showed $0 on the card AND chart because the pairing decided open-vs-close from the
signal's resolved sentiment. When an engine BUY resolved to a nearby TV signal's
"exit"/"short" sentiment, it was dropped (or a SELL opened a phantom short instead
of closing the long), so no round-trip formed. Pairing now nets against the actual
position first; sentiment is attribution only. A real fill can never vanish.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a


def _fill(symbol, side, price, qty, t):
    return {"symbol": symbol, "side": side, "price": price, "shares": qty,
            "time": t, "order_id": ""}


def test_same_day_roundtrip_pairs_regardless_of_resolved_sentiment():
    """BUY 27 @597 then SELL 27 @596.39 (the real META trade) must pair to -16.47
    even when BOTH fills resolve to a 'short'/'flat' sentiment (which the old code
    treated as open-short / exit and dropped)."""
    buy  = _fill("META", "BOT", 597.00, 27, "2026-08-05T13:43:09Z")
    sell = _fill("META", "SLD", 596.39, 27, "2026-08-05T13:43:27Z")
    # Force a "wrong" sentiment on both sides within the ±5min resolution window.
    ts_buy  = 1754401389  # ~13:43:09Z
    ts_sell = 1754401407
    lookup = {
        ("META", "BOT"): [(ts_buy,  "META_CAM_BREAKOUT_R4S4_V02_5MIN", "short")],
        ("META", "SLD"): [(ts_sell, "META_CAM_BREAKOUT_R4S4_V02_5MIN", "short")],
    }
    res = a._pair_alpaca_fills_lifo([buy, sell], from_date="2026-08-05",
                                    to_date="2026-08-05", signal_lookup=lookup)
    closed = res["closed"]
    assert len(closed) == 1, closed
    assert closed[0]["side"] == "LONG"
    assert closed[0]["pnl"] == round((596.39 - 597.00) * 27, 2) == -16.47


def test_short_roundtrip_pairs():
    """SELL to open, BUY to cover → one SHORT round-trip."""
    sell = _fill("SPY", "SLD", 500.00, 10, "2026-08-05T14:00:00Z")
    buy  = _fill("SPY", "BOT", 499.00, 10, "2026-08-05T14:05:00Z")
    res = a._pair_alpaca_fills_lifo([sell, buy], from_date="2026-08-05",
                                    to_date="2026-08-05", signal_lookup={})
    assert len(res["closed"]) == 1
    assert res["closed"][0]["side"] == "SHORT"
    assert res["closed"][0]["pnl"] == round((500.00 - 499.00) * 10, 2) == 10.0


def test_scale_in_then_exit_all():
    """Two BUYs (scale-in) then one SELL closing the full size → both legs pair."""
    b1 = _fill("AAPL", "BOT", 200.0, 5, "2026-08-05T14:00:00Z")
    b2 = _fill("AAPL", "BOT", 202.0, 5, "2026-08-05T14:01:00Z")
    s  = _fill("AAPL", "SLD", 205.0, 10, "2026-08-05T14:10:00Z")
    res = a._pair_alpaca_fills_lifo([b1, b2, s], from_date="2026-08-05",
                                    to_date="2026-08-05", signal_lookup={})
    total = round(sum(c["pnl"] for c in res["closed"]), 2)
    # LIFO: 5@205 vs 202 = +15, 5@205 vs 200 = +25 → +40 total.
    assert total == 40.0
    assert all(c["side"] == "LONG" for c in res["closed"])
