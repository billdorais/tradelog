"""Five changes to how the crew picks the monthly roster.

Audited 2026-09-06. The chain was: farm fills -> composite score -> snapshot rank ->
"RANKED PRIMARILY by the SNAPSHOT LEADERBOARD RANKINGS". Everything downstream
inherited that ranking, which the walk-forward Selection Test measured at
+0.023%/trade, t=0.35 sigma — no forward power. Meanwhile the forward scorecard
returned 0/18 twice running, so no out-of-sample penalty ever landed.

  1. roster 18 -> 10, split into core / audition tiers with different sizing
  2. mechanism quota gains a "none" mode and becomes the default
  3. side asymmetries reach the wire instead of dying in the prose
  4. untraded picks are proxy-graded on their farm, closing the feedback loop
  5. leaderboards the crew reads rank on per-trade, not total
"""
from __future__ import annotations

import inspect
import os

import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as kairos
import routes.crew as crew


def _card_src():
    return inspect.getsource(crew._run_kairos_crew)


class _StubApp:
    """Serves one canned payload to every internal request."""
    def __init__(self, by_url=None, payload=None):
        self.by_url, self.payload = by_url or {}, payload

    def test_client(self):
        outer = self

        class _C:
            def __enter__(s): return s
            def __exit__(s, *a): return False

            def get(s, url):
                data = outer.payload
                if data is None:
                    data = next((v for k, v in outer.by_url.items() if k in url), {})

                class _R:
                    def get_json(_): return data
                return _R()
        return _C()


# ── 1. roster size and tiers ────────────────────────────────────────────────────

def test_the_roster_is_a_constant_not_a_scattered_literal():
    """It was hardcoded as 18 in ~15 places, which is how a size change drifts."""
    assert crew.CREW_ROSTER_SIZE == 10
    src = _card_src()
    assert "Top 18" not in src and "EXACTLY 18" not in src


def test_the_card_demands_two_tiers():
    src = _card_src()
    assert "TIERS" in src and "AUDITION" in src
    assert "CREW_CORE_MIN_TRADES" in src


def test_core_is_defined_by_live_trades_not_by_rank():
    """The whole point: rank is the signal that measured t=0.35 sigma. A core slot
    has to be earned on evidence the score does not contain."""
    src = _card_src()
    i = src.index("CORE:")
    block = src[i:i + 400]
    assert "LIVE round-trips" in block
    assert "PER TRADE" in block
    assert "composite score" in block and "does NOT contain" in block


def test_a_short_card_is_explicitly_allowed():
    """Padding to a fixed count with names that never traded is the failure mode
    this replaces — 8 of the last 18 had zero trades."""
    src = _card_src()
    assert "RUN A SHORTER CARD" in src
    assert "AT MOST {CREW_ROSTER_SIZE} lines" in src


def test_the_tier_column_parses_and_defaults_to_core():
    """Absent on older reports, so the default must not silently halve an existing
    roster's size when this ships."""
    block = """```picks
AAA_CAM_BREAKOUT_R3S3_V02_5MIN | both | TV | core
BBB_CAM_BREAKOUT_R3S3_V02_5MIN | long | TV | audition
CCC_CAM_BREAKOUT_R4S4_V02_5MIN | both | Kairos
```"""
    got = {p["strategy"][:3]: p["tier"] for p in crew._parse_picks_block(block)}
    assert got == {"AAA": "core", "BBB": "audition", "CCC": "core"}


def test_auditions_are_wired_smaller_than_core():
    """Otherwise the tier is a label the wire ignores, and an unproven name takes
    the same risk as one with a live record."""
    src = inspect.getsource(crew.api_crew_wire_to_router)
    assert "_tier_mult" in src
    assert "CREW_AUDITION_SIZE_PCT" in inspect.getsource(crew)
    i = src.index("def _tier_mult")
    assert 'get("tier") == "audition"' in src[i:i + 400]


def test_the_live_mirror_also_respects_the_tier():
    """Crew Live is real money; an audition must not size like a core pick there."""
    src = inspect.getsource(crew.api_crew_wire_to_router)
    i = src.index("def _live_qty")
    assert "_tier_mult(pick)" in src[i:i + 300]


# ── 2. the mechanism quota ──────────────────────────────────────────────────────

def test_a_no_quota_mode_exists_and_is_the_default():
    """Every other mode is a FLOOR with no ceiling: "Refined-led (>=5)" still
    produced 10 of 18 Kairos in a report whose own section 4 measured the engine at
    -$728/30d on breakouts."""
    assert '_kt == "none"' in _card_src()
    assert 'kairos_target") or "none"' in inspect.getsource(crew.api_crew_run)
    html = open("templates/crew.html", encoding="utf-8").read()
    assert '<option value="none" selected>' in html


def test_the_no_quota_mode_defers_to_the_head_to_head():
    src = _card_src()
    i = src.index('_kt == "none"')
    block = src[i:i + 1200]
    assert "NO target count" in block
    assert "ENGINE-vs-TV head-to-head" in block


def test_the_remaining_modes_derive_from_the_roster_constant():
    """They said "of 18" in prose while the roster was 10."""
    src = _card_src()
    i = src.index('_kt == "max"')
    assert "of 18" not in src[i:i + 2000]


# ── 3. side asymmetry reaches the wire ──────────────────────────────────────────

PAYLOAD = {
    "side_gated_candidates": [
        {"strategy": "IWM_CAM_BREAKOUT_R3S3_V02_5MIN", "best_side": "long",
         "best_side_score": 27, "both_sides_score": 25, "trades": 9}],
    "by_band_side": [
        {"band": "R3S3", "side": "LONG",  "trades": 18, "pnl": 364.0},
        {"band": "R3S3", "side": "SHORT", "trades": 22, "pnl": -292.0},
        {"band": "R4S4", "side": "LONG",  "trades": 11, "pnl": 40.0},
        {"band": "R4S4", "side": "SHORT", "trades": 12, "pnl": 30.0}],
}


def _conflicts(picks, payload=None):
    return crew._side_gate_conflicts(_StubApp(payload=payload or PAYLOAD),
                                     picks)["conflicts"]


def test_a_both_sided_pick_is_flagged_when_its_band_bleeds_one_side():
    """The finding that kept dying in prose: R3S3 LONG +$364/18t vs SHORT -$292/22t,
    with the crew writing "tag LONG-only" and the picks block wiring `both`."""
    c = _conflicts([{"strategy": "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN", "side": "both"}])
    assert len(c) == 1 and c[0]["best_side"] == "long" and c[0]["level"] == "band"


def test_a_per_strategy_asymmetry_outranks_the_band_signal():
    """Its own record beats its band's, so it is reported at that level."""
    c = _conflicts([{"strategy": "IWM_CAM_BREAKOUT_R3S3_V02_5MIN", "side": "both"}])
    assert len(c) == 1 and c[0]["level"] == "strategy"


def test_an_already_gated_pick_is_left_alone():
    assert _conflicts([{"strategy": "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN",
                        "side": "long"}]) == []


def test_a_band_positive_on_both_sides_is_not_flagged():
    """There is no bleed to gate away — flagging it would be noise."""
    assert _conflicts([{"strategy": "XOM_CAM_BREAKOUT_R4S4_V02_5MIN",
                        "side": "both"}]) == []


def test_a_thin_band_is_not_called():
    thin = {"side_gated_candidates": [],
            "by_band_side": [{"band": "R3S3", "side": "LONG",  "trades": 3, "pnl": 300.0},
                             {"band": "R3S3", "side": "SHORT", "trades": 2, "pnl": -300.0}]}
    assert _conflicts([{"strategy": "AAA_CAM_BREAKOUT_R3S3_V02_5MIN", "side": "both"}],
                      thin) == []


def test_a_small_spread_is_not_an_asymmetry():
    close = {"side_gated_candidates": [],
             "by_band_side": [{"band": "R3S3", "side": "LONG",  "trades": 20, "pnl": 10.0},
                              {"band": "R3S3", "side": "SHORT", "trades": 20, "pnl": -12.0}]}
    assert _conflicts([{"strategy": "AAA_CAM_BREAKOUT_R3S3_V02_5MIN", "side": "both"}],
                      close) == []


def test_an_unreadable_book_reports_itself(monkeypatch):
    class _Boom:
        def test_client(self): raise RuntimeError("alpaca down")
    out = crew._side_gate_conflicts(_Boom(), [{"strategy": "A_CAM_BREAKOUT_R3S3", "side": "both"}])
    assert out["conflicts"] == [] and "error" in out


def test_the_wire_reports_side_conflicts_without_refusing():
    """Same discipline as the entry-tag check: only corruption blocks a wire."""
    src = inspect.getsource(crew.api_crew_wire_to_router)
    assert '"side_conflicts"' in src
    i = src.index("_side_gate_conflicts")
    assert "409" not in src[i:], "the side check must warn, not block"


def test_the_ui_shows_them():
    html = open("templates/crew.html", encoding="utf-8").read()
    assert "side_conflicts" in html and "one side bleeds" in html


# ── 4. the scorecard grades untraded picks ──────────────────────────────────────

REPORT = """```picks
TRADED_CAM_BREAKOUT_R3S3_V02_5MIN | both | TV | core
PWIN_CAM_BREAKOUT_R3S3_V02_5MIN | both | TV | audition
PLOSS_CAM_BREAKOUT_R4S4_V02_5MIN | both | Kairos | audition
GHOST_CAM_REVERSAL_R3S3_V02_5MIN | both | TV | audition
```"""


@pytest.fixture
def graded(monkeypatch):
    book = {"TRADED_CAM_BREAKOUT_R3S3_V02_5MIN": {"trades": 4, "total_pnl": 40.0,
                                                  "win_rate": 75}}
    tv   = {"PWIN_CAM_BREAKOUT_R3S3_V02_5MIN": {"trades": 9, "total_pnl": 55.0,
                                                "win_rate": 56}}
    kr   = {"PLOSS_CAM_BREAKOUT_R4S4_V02_5MIN": {"trades": 7, "total_pnl": -80.0,
                                                 "win_rate": 29}}
    stub = _StubApp(by_url={"account=4": {"per_strategy": book},
                            "account=1": {"per_strategy": tv},
                            "account=5": {"per_strategy": kr}})
    monkeypatch.setattr(kairos.app, "test_client", stub.test_client)
    return crew._pick_scorecard({"week": "W36", "created_at": "2026-09-01T10:00:00",
                                 "report": REPORT})


def test_an_untraded_pick_is_graded_on_its_own_farm(graded):
    """The book returned 0/N twice running, so the selection method was never
    tested. The farms are ungated and trade the same names."""
    assert graded["n_proxy"] == 2


def test_a_proxy_loser_is_reported_as_a_loser(graded):
    """It used to read as "no trades yet", which is how a failed pick survived."""
    row = next(r for r in graded["picks"] if r["strategy"].startswith("PLOSS"))
    assert row["proxy"] is True and row["proxy_pnl"] == -80.0
    assert graded["n_proxy_positive"] == 1


def test_the_proxy_never_contaminates_the_book_total(graded):
    """Merging it would overstate how much was actually tested."""
    assert graded["total_pnl"] == 40.0
    assert graded["proxy_pnl"] == -25.0


def test_a_pick_with_no_book_and_no_farm_stays_ungraded(graded):
    """Honest: some picks really are untested."""
    assert graded["n_ungraded"] == 1
    ghost = next(r for r in graded["picks"] if r["strategy"].startswith("GHOST"))
    assert not ghost.get("proxy") and ghost["pnl"] is None


def test_each_pick_is_proxied_against_its_own_mechanism(graded):
    """A [Kairos] pick priced on the TV farm would measure the mechanism, not the
    pick — the same error the gate-cost pricing had."""
    row = next(r for r in graded["picks"] if r["strategy"].startswith("PLOSS"))
    assert "kairos" in row["proxy_source"]


def test_the_proxy_is_measured_on_curated_hours():
    """Farms trade all day; a proxy on unreachable hours would flatter every pick."""
    assert "hours=curated" in inspect.getsource(crew._pick_scorecard)


def test_the_prompt_is_told_to_trust_the_proxy_over_the_rank():
    """A proxy is out-of-sample; the rank that chose the name is not."""
    src = _card_src()
    assert "TRUST THE PROXY" in src
    assert "never merge it into the book total" in src


# ── 5. leaderboards rank per trade ──────────────────────────────────────────────

def _leaderboard(per_strategy):
    return crew._fmt_strategies({"overall": {"total_pnl": 0},
                                 "per_strategy": per_strategy}, header="T")


def test_a_sharper_name_outranks_a_longer_tenured_one():
    """A total on a daily-rotating roster partly measures days-wired: $200 over 40
    trades is a worse strategy than $120 over 6."""
    out = _leaderboard({
        "TENURED_CAM_BREAKOUT_R3S3_V02_5MIN": {"trades": 40, "total_pnl": 200.0,
                                               "win_rate": 50, "profit_factor": 1.2},
        "SHARP_CAM_BREAKOUT_R3S3_V02_5MIN":   {"trades": 6, "total_pnl": 120.0,
                                               "win_rate": 66, "profit_factor": 3.0}})
    assert out.index("SHARP_CAM") < out.index("TENURED_CAM")


def test_the_per_trade_figure_is_printed_not_just_used():
    """A ranking you cannot see the key for is one you have to trust."""
    out = _leaderboard({"A_CAM_BREAKOUT_R3S3_V02_5MIN": {"trades": 4, "total_pnl": 40.0,
                                                         "win_rate": 50}})
    assert "PER TRADE $+10.00" in out


def test_trade_count_stays_visible_next_to_it():
    """Per-trade without sample size just moves the trap: a huge figure on 2 trades
    is noise."""
    out = _leaderboard({"A_CAM_BREAKOUT_R3S3_V02_5MIN": {"trades": 2, "total_pnl": 400.0,
                                                         "win_rate": 100}})
    assert "2 trades" in out
    assert "noise, not an edge" in out


def test_a_zero_trade_strategy_does_not_divide_by_zero():
    out = _leaderboard({"A_CAM_BREAKOUT_R3S3_V02_5MIN": {"trades": 0, "total_pnl": 0.0,
                                                         "win_rate": 0}})
    assert "PER TRADE $+0.00" in out


def test_the_ordering_rule_is_stated_in_the_block():
    out = _leaderboard({"A_CAM_BREAKOUT_R3S3_V02_5MIN": {"trades": 5, "total_pnl": 10.0,
                                                         "win_rate": 50}})
    assert "sorted by P&L PER TRADE" in out
