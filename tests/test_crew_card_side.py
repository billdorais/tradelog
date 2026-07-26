"""Crew card side-tag parsing — the per-pick long/short/both gate.

Regression: the side tag sits right after the slug ("— LONG-only [TV]"), but the
justification often names the OTHER side ("...while SHORT bleeds"). A plain
"SHORT in tail" check flipped LONG-only picks to short, so the wire button gated
them the wrong way. Side is now first-mention-wins, like the entry tag.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import routes.crew as crew


def _card(top_row: str) -> str:
    return (
        "## 📋 Next Month — Crew Paper Account\n"
        "| Decision | Recommendation |\n|---|---|\n"
        f"| Top 18 to run | {top_row} |\n"
    )


def _side_of(report, slug):
    picks = crew._parse_next_month_card(report)["picks"]
    return next(p["side"] for p in picks if p["strategy"] == slug)


def _entry_of(report, slug):
    picks = crew._parse_next_month_card(report)["picks"]
    return next(p["entry"] for p in picks if p["strategy"] == slug)


def test_long_only_with_short_in_reasoning_stays_long():
    row = ("1. TSLA_CAM_BREAKOUT_R3S3_V02_5MIN — LONG-only [TV] "
           "(BREAKOUT R3S3 LONG earns $+692 on TV Refined while SHORT bleeds; "
           "gate to LONG-only)")
    assert _side_of(_card(row), "TSLA_CAM_BREAKOUT_R3S3_V02_5MIN") == "long"


def test_short_only_with_long_in_reasoning_stays_short():
    row = ("1. AAPL_CAM_BREAKOUT_R4S4_V02_5MIN — SHORT-only [Kairos] "
           "(SHORT side carries the edge; the LONG side is a net bleeder)")
    assert _side_of(_card(row), "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN") == "short"


def test_untagged_pick_defaults_to_both():
    row = "1. NVDA_CAM_BREAKOUT_R4S4_V02_5MIN [TV] (positive on both books)"
    assert _side_of(_card(row), "NVDA_CAM_BREAKOUT_R4S4_V02_5MIN") == "both"


def test_explicit_tag_phrase_overrides_stale_leading_tag():
    """The guardrail can flip a Kairos candidate to TV in the reasoning while the
    crew leaves a stale leading [Kairos]. An explicit 'TAG [TV]' phrase wins."""
    row = ("1. NVDA_CAM_BREAKOUT_R4S4_V02_5MIN — both [Kairos] (acct3 $23.30/2t; "
           "Kairos Farm $-1.76/15t/PF 0.57 — guardrail BLOCKS [Kairos]; TV Farm "
           "$4.40/10t positive; TAG [TV] per guardrail)")
    assert _entry_of(_card(row), "NVDA_CAM_BREAKOUT_R4S4_V02_5MIN") == "tv"


def test_leading_tag_used_when_no_explicit_phrase():
    """Without an explicit 'TAG [X]', first-mention (the leading tag) wins."""
    row = ("1. AAPL_CAM_BREAKOUT_R4S4_V02_5MIN — both [Kairos] (acct3 dominant; "
           "acct2 also positive)")
    assert _entry_of(_card(row), "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN") == "kairos"


def test_multiple_picks_each_keep_their_own_side():
    row = ("1. TSLA_CAM_BREAKOUT_R3S3_V02_5MIN — LONG-only [TV] (SHORT bleeds)<br>"
           "2. AAPL_CAM_BREAKOUT_R4S4_V02_5MIN — SHORT-only [Kairos] (LONG bleeds)<br>"
           "3. NVDA_CAM_BREAKOUT_R4S4_V02_5MIN — both [TV] (strong)")
    report = _card(row)
    assert _side_of(report, "TSLA_CAM_BREAKOUT_R3S3_V02_5MIN") == "long"
    assert _side_of(report, "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN") == "short"
    assert _side_of(report, "NVDA_CAM_BREAKOUT_R4S4_V02_5MIN") == "both"
