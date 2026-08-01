"""Crew wire list — the authoritative ```picks fenced block.

The prose "Top 18 to run" row leaks the model's DROP/REPLACE/GUARDRAIL reasoning
and duplicate slot numbers, which the prose parser had to guess through — and it
could wire a name mentioned only in a "REPLACE with X" aside. The report now also
emits a clean machine-readable ```picks block; the wire button prefers it. These
tests lock in that behavior and the prose fallback for older reports.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import routes.crew as crew


def test_block_is_preferred_over_messy_prose():
    """When a ```picks block is present, the wire list comes from it — not the
    prose row — so DROP/REPLACE leakage in the prose can't wire phantom picks."""
    report = (
        "## Next Month\n"
        "| Decision | Recommendation |\n|---|---|\n"
        "| Top 18 to run | 1. NVDA_CAM_BREAKOUT_R3S3_V02_5MIN both [TV]<br>"
        "5. TSLA_CAM_REVERSAL_R4S4_V02_5MIN both [Kairos] "
        "(DROP; REPLACE with C_CAM_BREAKOUT_R3S3_V02_5MIN) |\n"
        "| Sizing | Equal risk ($1.5k/trade flat) |\n"
        "\n### Changes\n"
        "```picks\n"
        "SLUG | side | book\n"
        "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN | both | TV\n"
        "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN | both | Kairos\n"
        "PLTR_CAM_BREAKOUT_R3S3_V02_5MIN | long | TV\n"
        "```\n"
    )
    parsed = crew._parse_next_month_card(report)
    slugs  = [p["strategy"] for p in parsed["picks"]]
    assert slugs == [
        "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN",
        "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN",
        "PLTR_CAM_BREAKOUT_R3S3_V02_5MIN",
    ]
    # The phantom (REPLACE-with) and the dropped bleeder never wire.
    assert "C_CAM_BREAKOUT_R3S3_V02_5MIN" not in slugs
    assert "TSLA_CAM_REVERSAL_R4S4_V02_5MIN" not in slugs


def test_block_parses_side_and_book_tags():
    report = (
        "```picks\n"
        "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN | both | TV\n"
        "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN | both | Kairos\n"
        "PLTR_CAM_BREAKOUT_R3S3_V02_5MIN | long | TV\n"
        "SMH_CAM_REVERSAL_R3S3_V02_5MIN | short | Kairos\n"
        "```\n"
    )
    picks = {p["strategy"]: p for p in crew._parse_next_month_card(report)["picks"]}
    assert picks["NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"]["entry"] == "tv"
    assert picks["AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"]["entry"] == "kairos"
    assert picks["PLTR_CAM_BREAKOUT_R3S3_V02_5MIN"]["side"] == "long"
    assert picks["SMH_CAM_REVERSAL_R3S3_V02_5MIN"]["side"] == "short"
    assert picks["SMH_CAM_REVERSAL_R3S3_V02_5MIN"]["entry"] == "kairos"


def test_block_dedups_and_skips_header():
    report = (
        "```picks\n"
        "SLUG | side | book\n"                       # header — ignored (no valid slug)
        "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN | both | TV\n"
        "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN | both | Kairos\n"  # dup slug — first wins
        "```\n"
    )
    picks = crew._parse_next_month_card(report)["picks"]
    assert [p["strategy"] for p in picks] == ["NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"]
    assert picks[0]["entry"] == "tv"


def test_prose_fallback_when_no_block():
    """Older reports have no block — the prose Top-18 parser still runs."""
    report = (
        "## Next Month\n"
        "| Decision | Recommendation |\n|---|---|\n"
        "| Top 18 to run | 1. NVDA_CAM_BREAKOUT_R3S3_V02_5MIN both [TV]<br>"
        "2. AAPL_CAM_BREAKOUT_R4S4_V02_5MIN both [Kairos] |\n"
    )
    slugs = [p["strategy"] for p in crew._parse_next_month_card(report)["picks"]]
    assert slugs == ["NVDA_CAM_BREAKOUT_R3S3_V02_5MIN", "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"]


def test_untagged_block_pick_inherits_entries_default():
    """A block line with no book column falls back to the card's Entries default."""
    report = (
        "| Decision | Recommendation |\n|---|---|\n"
        "| Entries | Kairos Refined |\n"
        "```picks\n"
        "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN | both\n"
        "```\n"
    )
    picks = crew._parse_next_month_card(report)["picks"]
    assert picks[0]["entry"] == "kairos"
