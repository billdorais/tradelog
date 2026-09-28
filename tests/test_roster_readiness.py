"""Roster readiness (/api/crew/roster_readiness).

Roster size is usually argued from a feeling about how many names are too many.
The book already knows: a core slot costs CREW_CORE_MIN_TRADES live round-trips,
so the earned pool is countable. If it is smaller than the core slots a roster
implies, that roster can only be filled by padding with unproven names at full
size — the exact failure the tier split was introduced to replace.

Built while deciding whether to cut the roster from 10 to 5-6, so that the answer
came from the book rather than from the number feeling about right.
"""
from __future__ import annotations

import json
import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a
from routes import crew as C


def _st(trades, pnl, win_rate=50.0, pf=1.2):
    return {"trades": trades, "total_pnl": pnl, "win_rate": win_rate, "profit_factor": pf}


@pytest.fixture
def book(monkeypatch):
    """A readable Crew Paper book whose per_strategy the test controls."""
    state = {"per_strategy": {}, "unavailable": False}

    class _Client:
        def __enter__(self): return self
        def __exit__(self, *x): return False
        def get(self, url):
            class _R:
                def get_json(_s):
                    if state["unavailable"]:
                        return {"fills_unavailable": True}
                    return {"per_strategy": state["per_strategy"]}
            return _R()

    monkeypatch.setattr(a.app, "test_client", lambda *x, **k: _Client())
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM",
                        {"4": {"tag": "alpaca4", "label": "Crew Paper", "broker": object()}})
    monkeypatch.setattr(C, "_selection_picks", lambda r: [])
    monkeypatch.setattr(a, "get_db",
                        lambda: (_ for _ in ()).throw(RuntimeError("no db in test")))
    return state


def _get(roster=None, auditions=None, account="4"):
    q = f"/api/crew/roster_readiness?account={account}"
    if roster    is not None: q += f"&roster={roster}"
    if auditions is not None: q += f"&auditions={auditions}"
    with a.app.test_request_context(q):
        resp = C.api_crew_roster_readiness()
    body = resp[0] if isinstance(resp, tuple) else resp
    return json.loads(body.get_data()), (resp[1] if isinstance(resp, tuple) else 200)


# ── the bar ─────────────────────────────────────────────────────────────────

def test_earned_means_the_same_bar_the_wire_enforces(book):
    """If readiness and _tier_conflicts disagreed, the page would promise a core
    slot the wire then flags as thin."""
    bar = C.CREW_CORE_MIN_TRADES
    book["per_strategy"] = {
        "NVDA_CAM_BREAKOUT_R3S3": _st(bar,     50.0),
        "AAPL_CAM_BREAKOUT_R4S4": _st(bar - 1, 50.0),
    }
    d, _ = _get()
    assert [r["strategy"] for r in d["earned"]] == ["NVDA_CAM_BREAKOUT_R3S3"]
    assert [r["strategy"] for r in d["thin"]]   == ["AAPL_CAM_BREAKOUT_R4S4"]
    assert d["bar"] == bar


def test_ranking_is_per_trade_not_total(book):
    """A total on a rotating roster partly measures how many days a name held a
    slot — the same reason the leaderboards moved to per-trade."""
    book["per_strategy"] = {
        "BIGVOL_CAM_BREAKOUT_R3S3": _st(40, 200.0),   # +5.00/trade
        "SHARP_CAM_BREAKOUT_R4S4":  _st(12, 120.0),   # +10.00/trade
    }
    d, _ = _get()
    assert [r["strategy"] for r in d["earned"]][0] == "SHARP_CAM_BREAKOUT_R4S4"
    assert d["earned"][0]["per_trade"] == 10.0


def test_a_losing_name_can_still_be_earned(book):
    """Earned is a SAMPLE-SIZE test, not a profit test — it says we know enough to
    judge, not that the verdict is good. Conflating them would hide the losers."""
    book["per_strategy"] = {"DUD_CAM_BREAKOUT_R3S3": _st(30, -90.0)}
    d, _ = _get()
    assert len(d["earned"]) == 1
    assert d["earned"][0]["per_trade"] == -3.0
    assert any("not a profit test" in c for c in d["caveats"])


# ── does the book support the roster ────────────────────────────────────────

def test_it_says_so_when_the_earned_pool_is_too_small(book):
    book["per_strategy"] = {f"T{i}_CAM_BREAKOUT_R3S3": _st(12, 10.0 * i) for i in range(3)}
    d, _ = _get(roster=10, auditions=3)          # needs 7 core, has 3
    assert "only 3 name" in " ".join(d["warnings"])
    assert "supports about 3 core" in d["verdict"]
    assert "roster of 6 would be fully earned" in d["verdict"]


def test_a_supportable_roster_reports_the_bench(book):
    book["per_strategy"] = {f"T{i}_CAM_BREAKOUT_R3S3": _st(12, 10.0 * i) for i in range(6)}
    d, _ = _get(roster=6, auditions=2)           # needs 4 core, has 6
    assert "leave 2 on the bench" in d["verdict"]
    assert not any("earned a core slot, but" in w for w in d["warnings"])


def test_an_empty_book_says_the_tier_split_is_idle(book):
    book["per_strategy"] = {}
    d, _ = _get(roster=6, auditions=2)
    assert "No name has" in d["verdict"]
    assert d["earned"] == [] and d["at_proposed"]["core"] == []


# ── concentration, which matters more the smaller the roster ────────────────

def test_a_ticker_holding_too_many_core_slots_is_flagged(book):
    """The crew card has no per-ticker cap, unlike the refined snapshot — so at a
    small roster this is the thing most likely to bite."""
    book["per_strategy"] = {
        "SPY_CAM_BREAKOUT_R3S3": _st(20, 90.0),
        "SPY_CAM_BREAKOUT_R4S4": _st(20, 80.0),
        "SPY_CAM_REVERSAL_R3S3": _st(20, 70.0),
        "QQQ_CAM_BREAKOUT_R3S3": _st(20, 60.0),
    }
    d, _ = _get(roster=4, auditions=0)
    assert any("SPY would hold 3 of 4" in w for w in d["warnings"])


def test_one_premise_across_several_bands_is_still_one_premise(book):
    """The gap the first run exposed: BREAKOUT_R3S3 and BREAKOUT_R4S4 are two
    bands, so a band-only check reads them as diversified. They are one bet on
    breakouts working."""
    book["per_strategy"] = {
        "NVDA_CAM_BREAKOUT_R3S3": _st(20, 90.0),
        "AAPL_CAM_BREAKOUT_R4S4": _st(20, 80.0),
        "MU_CAM_BREAKOUT_R3S3":   _st(20, 70.0),
        "SPY_CAM_BREAKOUT_R4S4":  _st(20, 60.0),
    }
    d, _ = _get(roster=4, auditions=0)
    assert d["at_proposed"]["distinct_bands"] == 2, "band view alone looks spread"
    assert d["at_proposed"]["distinct_setups"] == 1
    assert any("one premise" in w for w in d["warnings"])


def test_a_genuinely_mixed_core_raises_nothing(book):
    book["per_strategy"] = {
        "NVDA_CAM_BREAKOUT_R3S3": _st(20, 90.0),
        "AAPL_CAM_REVERSAL_R4S4": _st(20, 80.0),
        "MU_CAM_BREAKOUT_R4S4":   _st(20, 70.0),
        "GLD_CAM_REVERSAL_R3S3":  _st(20, 60.0),
    }
    d, _ = _get(roster=4, auditions=0)
    assert d["warnings"] == []


def test_too_few_underlyings_is_flagged(book):
    book["per_strategy"] = {
        "SPY_CAM_BREAKOUT_R3S3": _st(20, 90.0),
        "QQQ_CAM_REVERSAL_R4S4": _st(20, 80.0),
    }
    d, _ = _get(roster=2, auditions=0)
    assert any("2 distinct underlyings" in w for w in d["warnings"])


# ── inputs and failure modes ────────────────────────────────────────────────

def test_it_defaults_to_the_configured_roster(book):
    book["per_strategy"] = {"A_CAM_BREAKOUT_R3S3": _st(20, 40.0)}
    d, _ = _get()
    assert d["proposed"]["roster"]    == C.CREW_ROSTER_SIZE
    assert d["proposed"]["auditions"] == C.CREW_AUDITION_SLOTS
    assert d["current"]["roster"]     == C.CREW_ROSTER_SIZE


def test_auditions_cannot_exceed_the_roster(book):
    book["per_strategy"] = {}
    d, _ = _get(roster=4, auditions=9)
    assert d["proposed"]["auditions"] == 4
    assert d["proposed"]["core_slots"] == 0


def test_garbage_inputs_fall_back_rather_than_erroring(book):
    book["per_strategy"] = {}
    with a.app.test_request_context("/api/crew/roster_readiness?roster=abc&auditions=-5"):
        d = json.loads(C.api_crew_roster_readiness().get_data())
    assert d["proposed"]["roster"] == C.CREW_ROSTER_SIZE
    assert d["proposed"]["auditions"] >= 0


def test_an_unreadable_book_refuses_rather_than_understating(book):
    """Every trade count would read low, so every name would look unearned and the
    page would advise cutting a roster that is actually fine."""
    book["unavailable"] = True
    d, code = _get()
    assert code == 503 and "understate" in d["error"]


def test_an_unconfigured_account_is_refused(book):
    d, code = _get(account="9")
    assert code == 400 and "not configured" in d["error"]


# ── the panel ───────────────────────────────────────────────────────────────

def test_the_crew_page_carries_the_panel():
    a.app.config["TESTING"] = True
    html = a.app.test_client().get("/crew").get_data(as_text=True)
    assert 'id="rosterBody"' in html
    assert 'id="rrRoster"' in html and 'id="rrAud"' in html, "must be adjustable"
    assert "loadRosterReadiness()" in html


def test_the_panel_dims_rather_than_hides_names_below_the_line():
    """Seeing who just missed the cut is the point — a roster decision needs the
    names on the bench, not only the ones above it."""
    src = open("templates/crew.html", encoding="utf-8").read()
    # Anchor on the DEFINITION — the first mention is the markup's onchange.
    i = src.index("async function loadRosterReadiness")
    # To the end of the function, not a fixed byte count — the window was outgrown
    # the moment the rollups were added above it.
    block = src[i:src.index("loadRosterReadiness();", i)]
    assert "coreNames" in block and "opacity:0.55" in block
    assert "below the line at this roster" in block


# ── rollups: splits that are invisible name-by-name ─────────────────────────

@pytest.fixture
def entries(monkeypatch):
    def _set(m): monkeypatch.setattr(C, "_entry_source_by_strategy", lambda *a_, **k: m)
    _set({})
    return _set


def test_rollups_total_per_trade_not_per_name(book, entries):
    """Averaging per-name figures would let a 4-trade name weigh as much as a
    20-trade one — the bias the whole readiness view exists to avoid."""
    book["per_strategy"] = {
        "A_CAM_BREAKOUT_R3S3": _st(20, 200.0),      # +10.00/trade
        "B_CAM_BREAKOUT_R3S3": _st(2,  -80.0),      # -40.00/trade, tiny sample
    }
    d, _ = _get()
    band = {r["key"]: r for r in d["rollups"]["band"]}["BREAKOUT_R3S3"]
    assert band["trades"] == 22 and band["total_pnl"] == 120.0
    assert band["per_trade"] == pytest.approx(120.0 / 22, abs=0.01)   # not (10-40)/2


def test_the_band_split_the_book_actually_shows(book, entries):
    """The finding that prompted this: R3S3 carrying the book while R4S4 sits at
    roughly zero. Name-by-name it is invisible."""
    book["per_strategy"] = {
        "NVDA_CAM_BREAKOUT_R3S3": _st(20, 355.75), "PLTR_CAM_BREAKOUT_R3S3": _st(12, 169.28),
        "MSFT_CAM_REVERSAL_R3S3": _st(12, 119.83), "GLD_CAM_BREAKOUT_R3S3":  _st(11, 21.91),
        "IWM_CAM_REVERSAL_R3S3":  _st(14, -10.89),
        "AAPL_CAM_BREAKOUT_R4S4": _st(15, 125.51), "NVDA_CAM_BREAKOUT_R4S4": _st(15, -29.19),
        "AMZN_CAM_REVERSAL_R4S4": _st(19, -88.82),
    }
    d, _ = _get()
    by = {r["key"]: r for r in d["rollups"]["band"]}
    assert by["BREAKOUT_R3S3"]["per_trade"] > by["BREAKOUT_R4S4"]["per_trade"]
    setups = {r["key"]: r for r in d["rollups"]["setup"]}
    assert setups["BREAKOUT"]["trades"] == 73 and setups["REVERSAL"]["trades"] == 45


def test_entry_mechanism_is_rolled_up(book, entries):
    """The head-to-head the system exists to answer: do engine entries beat TV?"""
    entries({"A_CAM_BREAKOUT_R3S3": "kairos", "B_CAM_BREAKOUT_R3S3": "kairos",
             "C_CAM_REVERSAL_R4S4": "tv"})
    book["per_strategy"] = {
        "A_CAM_BREAKOUT_R3S3": _st(20, 200.0), "B_CAM_BREAKOUT_R3S3": _st(10, 100.0),
        "C_CAM_REVERSAL_R4S4": _st(30, -60.0),
    }
    d, _ = _get()
    by = {r["key"]: r for r in d["rollups"]["entry"]}
    assert by["kairos"]["trades"] == 30 and by["kairos"]["per_trade"] == 10.0
    assert by["tv"]["per_trade"] == -2.0


def test_a_strategy_with_no_rule_here_is_labelled_not_guessed(book, entries):
    """Defaulting an unknown mechanism to "tv" would quietly load one side of the
    head-to-head with names that may have been engine-driven."""
    entries({"A_CAM_BREAKOUT_R3S3": "kairos"})
    book["per_strategy"] = {"A_CAM_BREAKOUT_R3S3": _st(20, 100.0),
                            "ORPHAN_CAM_BREAKOUT_R3S3": _st(20, -100.0)}
    d, _ = _get()
    keys = {r["key"] for r in d["rollups"]["entry"]}
    assert keys == {"kairos", "not wired here"}


def test_the_attribution_limit_is_stated(book, entries):
    """Trades carry no record of what triggered them, so this reads CURRENT wiring.
    A reader who assumes it is per-trade history would over-trust it."""
    book["per_strategy"] = {"A_CAM_BREAKOUT_R3S3": _st(20, 100.0)}
    d, _ = _get()
    assert any("CURRENT routing rules" in c and "rewired mid-period" in c
               for c in d["caveats"])


def test_the_panel_renders_all_three_rollups():
    src = open("templates/crew.html", encoding="utf-8").read()
    i = src.index("async function loadRosterReadiness")
    block = src[i:i + 7000]
    assert "By entry mechanism" in block
    assert "By setup" in block and "By band" in block
    assert "CURRENT rules" in block, "the attribution limit must be on screen"
