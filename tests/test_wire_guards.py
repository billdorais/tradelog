"""Guards on the crew wire path.

Two failure modes that both ended in a silently wrong roster:

  * the Sizing row naming BOTH schemes, where substring matching picked the one the
    row's own reasoning argued against;
  * a generation cut off before the ```picks fence closed, which makes
    _parse_picks_block match nothing and _parse_next_month_card fall back to the
    prose row it documents as unreliable.

Neither announced itself. The wire just proceeded.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

import routes.crew as crew


def _card(row):
    return "## Next Month\n\n| Decision | Recommendation |\n|---|---|\n" + row + "\n"


# ── Sizing ──────────────────────────────────────────────────────────────────────

def test_a_row_naming_both_schemes_falls_back_to_equal_risk():
    """The real report read "Equal risk ($1.5k-$5k per trade scaled by score band)"
    and wired as scaled — the opposite of what the same cell argued for."""
    row = ("| **Sizing** | Equal risk ($1.5k-$5k per trade scaled by score band) - "
           "score-based scaling is premature; equal risk preserves capital |")
    d = crew._parse_next_month_card(_card(row))
    assert d["sizing"] == "equal"
    assert d["sizing_conflict"], "a conflict must be reported, not just resolved"


def test_an_unambiguous_scaled_row_still_scales():
    d = crew._parse_next_month_card(_card("| Sizing | Scaled by score band, $3k base |"))
    assert d["sizing"] == "scaled" and not d["sizing_conflict"]


def test_an_unambiguous_equal_row_stays_equal():
    d = crew._parse_next_month_card(_card("| Sizing | Equal risk, $2k per trade |"))
    assert d["sizing"] == "equal" and not d["sizing_conflict"]


def test_sizing_defaults_to_equal_when_the_row_is_absent():
    d = crew._parse_next_month_card(_card("| Entries | TV |"))
    assert d["sizing"] == "equal" and not d["sizing_conflict"]


def test_the_dollar_size_is_still_read_from_a_conflicted_row():
    """Resolving the scheme must not throw away the size beside it."""
    row = "| Sizing | Equal risk ($1.5k per trade scaled by score) |"
    assert crew._parse_next_month_card(_card(row))["size_dollars"] == 1500.0


# ── Truncated picks block ───────────────────────────────────────────────────────

CLOSED = "text\n```picks\nAAPL_CAM_BREAKOUT_R4S4_V02_5MIN | both | TV\n```\n"
OPEN   = "text\n```picks\nAAPL_CAM_BREAKOUT_R4S4_V02_5MIN | both | TV\n"


def test_an_unclosed_picks_fence_is_detected():
    assert crew._picks_block_truncated(OPEN) is True
    assert crew._picks_block_truncated(CLOSED) is False
    assert crew._picks_block_truncated("no fence at all") is False
    assert crew._picks_block_truncated("") is False


def test_truncation_is_what_makes_the_parser_downgrade_silently():
    """Pins the reason the guard exists: an unclosed fence yields no block picks, so
    the card parser drops to the prose row without saying so."""
    assert crew._parse_picks_block(CLOSED), "closed fence should parse"
    assert crew._parse_picks_block(OPEN) == [], "unclosed fence should yield nothing"


def test_claimed_count_is_read_from_the_top_n_row():
    assert crew._claimed_pick_count("| **Top 18 to run** | 1. X |") == 18
    assert crew._claimed_pick_count("| Top 9 to run | x |") == 9


def test_an_unreadable_or_absurd_count_does_not_block_a_wire():
    """An unknown target must not become a reason to refuse."""
    assert crew._claimed_pick_count("| Sizing | equal |") is None
    assert crew._claimed_pick_count("| Top 200 to run | x |") is None
    assert crew._claimed_pick_count("") is None
