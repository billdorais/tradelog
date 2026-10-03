"""A fill's strategy comes from the order that placed it, not from a nearby signal.

Every entry order this app places is tagged kairos-{strategy}-{ts}, TV-triggered
and engine-triggered alike -- it is the app's own record of which strategy it
placed the order for. The TV signal table is a +/-5 minute ticker+side PROXIMITY
GUESS, and it used to take precedence over that record.

The consequence was not cosmetic. Kairos Select placed four orders on 2026-10-02,
all tagged AAPL/AMZN BREAKOUT R4S4 and both on its 15-name roster. Two came back
labelled AMZN_CAM_REVERSAL_R4S4 and AAPL_CAM_BREAKOUT_R3S3 -- strategies that book
does not trade -- because another strategy signalled the same ticker and side
within the window. A roster filter then found two of its four trades, and the
per-strategy P&L of both books was attributing trades to the wrong names.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a

FILL = "2026-10-02T13:59:16Z"
# A TV signal for a DIFFERENT strategy on the same ticker and side, seconds away.
LOOKUP = {("AMZN", "BOT"): [(1790949616.0, "AMZN_CAM_REVERSAL_R4S4_V02_5MIN", "bullish")]}
ENTRY = "kairos-AMZN_CAM_BREAKOUT_R4S4_V02_5MIN-1790949556"


def _resolve(lookup, order_id, symbol="AMZN"):
    return a._resolve_signal_for_fill(lookup, symbol, "BOT", FILL, order_id)


def test_the_order_id_beats_a_nearby_signal_for_another_strategy():
    """The reported bug, in one line."""
    assert _resolve(LOOKUP, ENTRY)[0] == "AMZN_CAM_BREAKOUT_R4S4_V02_5MIN"


def test_a_fill_with_no_tagged_order_still_uses_the_signal():
    """Fills not placed by this app -- a manual close, a broker liquidation -- have
    no tag, and the proximity match is the only thing available."""
    assert _resolve(LOOKUP, "af5478ed-2986-4237-b145-c31bc0a399b5")[0] \
        == "AMZN_CAM_REVERSAL_R4S4_V02_5MIN"
    assert _resolve(LOOKUP, "")[0] == "AMZN_CAM_REVERSAL_R4S4_V02_5MIN"


def test_exit_orders_do_not_hijack_the_strategy():
    """kairos-trail- and kairos-hard- are EXIT orders: the segment after the prefix
    is 'trail'/'hard', not a strategy. Reading it as one would file every trailing
    stop under a strategy named 'trail'."""
    for oid in ("kairos-trail-AMZN-1790949556", "kairos-hard-AMZN-1790949556"):
        assert _resolve(LOOKUP, oid)[0] == "AMZN_CAM_REVERSAL_R4S4_V02_5MIN"


def test_nothing_known_is_reported_as_unknown_not_guessed():
    assert _resolve({}, "")[0] == "Unknown"


def test_a_hyphenated_ticker_is_not_truncated():
    """The timestamp is the LAST segment. Splitting from the left cut the name at
    the first hyphen, so BRK-B would have resolved to 'BRK'."""
    assert _resolve({}, "kairos-BRK-B_CAM_BREAKOUT_R4S4_V02_5MIN-1790949556",
                    symbol="BRK.B")[0] == "BRK-B_CAM_BREAKOUT_R4S4_V02_5MIN"


def test_sentiment_still_comes_from_the_signal():
    """The order id carries a strategy but not a direction, so the TV signal is
    still consulted for sentiment even when it loses on the strategy."""
    strat, sentiment = _resolve(LOOKUP, ENTRY)
    assert strat == "AMZN_CAM_BREAKOUT_R4S4_V02_5MIN"
    assert sentiment == "bullish"


def test_a_signal_outside_the_window_is_not_used():
    far = {("AMZN", "BOT"): [(1790949616.0 - 3600, "AMZN_CAM_REVERSAL_R4S4_V02_5MIN", "bullish")]}
    assert _resolve(far, "")[0] == "Unknown"
