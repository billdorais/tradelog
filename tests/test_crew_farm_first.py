"""The crew ranks and tags on the farms.

Forward tests (frozen top-10, four pick dates Aug-Oct 2026): a 45-day farm window
picked better than 20 days (the Refined composite's lookback) and 90 days, on both
farms. And on the same names, the engine's entries often did better than TV's — a
report concluded the engine was "demonstrably better" on breakouts and then tagged
every pick [TV]. So the card now ranks on a FARM CANDIDATES block, and each name is
tagged with the mechanism whose farm did better on it.
"""
from __future__ import annotations

import inspect
import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

import routes.crew as crew

A = "AAA_CAM_BREAKOUT_R3S3_V02_5MIN"
B = "BBB_CAM_BREAKOUT_R3S3_V02_5MIN"
C = "CCC_CAM_BREAKOUT_R3S3_V02_5MIN"
W = "WWW_CAM_BREAKOUT_R3S3_V02_5MIN"


def _farm(**strats):
    """name=(all_day_pnl, all_day_n, takeable_pnl, takeable_n)."""
    return {"hours_reach": {"active": True},
            "per_strategy": {k: {"total_pnl": v[0], "trades": v[1],
                                 "in_hours_pnl": v[2], "in_hours_trades": v[3]}
                             for k, v in strats.items()}}


TV = _farm(**{A: (90, 20, 40, 10), B: (10, 12, 12, 6), C: (5, 3, 5, 3), W: (-60, 20, -30, 10)})
KA = _farm(**{A: (20, 20, 10, 10), B: (80, 12, 30, 6), C: (-40, 12, -20, 8)})


def _rows():
    return {r["strategy"]: r for r in crew._farm_side_by_side(TV, KA)}


def test_the_better_mechanism_is_the_higher_takeable_per_trade():
    r = _rows()
    assert r[A]["better"] == "tv"       # $4/t vs $1/t
    assert r[B]["better"] == "kairos"   # $5/t vs $2/t


def test_takeable_hours_not_all_day_decide():
    r = _rows()[A]
    assert r["tv"] == {"pnl": 40.0, "trades": 10, "per_trade": 4.0}


def test_a_farm_under_the_trade_floor_cannot_be_better():
    """C has 3 takeable trades on TV (+) and 8 on Kairos (−): only Kairos counts."""
    r = _rows()[C]
    assert r["better"] == "kairos" and r["both_qualify"] is False


def test_sorted_by_the_better_farms_pnl():
    rows = crew._farm_side_by_side(TV, KA)
    assert [r["strategy"] for r in rows][:2] == [A, B]


def test_the_block_lists_positive_names_and_every_wired_one():
    txt = crew._fmt_farm_candidates(crew._farm_side_by_side(TV, KA), wired=[A, W])
    assert "PRIMARY RANKING" in txt
    assert f"#1 {A} [WIRED NOW]" in txt and "better: TV" in txt
    assert f"#2 {B}" in txt and "better: KAIROS" in txt
    assert C not in txt.split("Wired to Crew Paper now")[0], "negative names are not candidates"
    i = txt.index("Wired to Crew Paper now")
    assert W in txt[i:], "a wired loser still shows its farm record"


def test_a_wired_name_below_the_cutoff_is_not_called_a_loser():
    """B is positive but cut by the limit; W genuinely loses. Only W is 'NOT positive'."""
    txt = crew._fmt_farm_candidates(crew._farm_side_by_side(TV, KA), wired=[B, W], limit=1)
    i = txt.index("Wired to Crew Paper now")
    assert f"{B} (positive, rank #2 of 2)" in txt[i:]
    assert f"{W} (NOT positive on its better farm)" in txt[i:]


def test_an_empty_farm_says_so():
    assert "too thin" in crew._fmt_farm_candidates([])


# ── the prompt ────────────────────────────────────────────────────────────────

def _src():
    return inspect.getsource(crew._run_kairos_crew)


def test_the_card_ranks_on_the_farm_candidates():
    s = _src()
    assert "RANKED PRIMARILY by the FARM CANDIDATES block" in s
    assert "RANKED PRIMARILY by the SNAPSHOT" not in s
    assert "REFINED STAYS PRIMARY" not in s
    assert "{farm_candidates_block}" in s


def test_snapshot_ranks_only_break_ties():
    s = _src()
    assert "ONLY to break ties" in s
    assert "Do NOT invent a different order from raw farm P&L" not in s


def test_tags_follow_the_better_farm_including_incumbents():
    s = _src()
    i = s.index("(d) ENTRY TAG RULE")
    rule = s[i:i + 1200]
    assert "'better'" in rule and "INCUMBENTS" in rule and "switch its tag" in rule
    assert "KAIROS-TAG GUARDRAIL" not in s


def test_core_needs_its_farm_not_to_disagree():
    s = _src()
    i = s.index("CORE:")
    assert "net-NEGATIVE on the farm of the entry you tag it with" in s[i:i + 900]


def test_the_refined_veto_needs_a_real_sample():
    s = _src()
    assert "10+ trades" in s and "negative per trade" in s
