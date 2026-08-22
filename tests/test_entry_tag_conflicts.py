"""Wire-time check: does a pick's entry tag survive the two books' head-to-head?

A pick's [TV]/[Kairos] tag decides which mechanism enters it, and the report argues
each tag in prose. Prose can contradict the record — one card routed six breakouts
through the Kairos engine in the same document that measured that engine losing $728
over 30 days on breakouts. So the tag is checked against TV Refined (acct2) and
Kairos Refined (acct3), never against the narrative.

It warns rather than blocks: a truncated picks block is corruption, but a contrarian
tag can be a deliberate call.
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

import app as kairos
import routes.crew as crew


class _FakeApp:
    """Stands in for the Flask app: serves canned per-strategy stats per account."""
    def __init__(self, by_account):
        self._by = by_account

    def test_client(self):
        outer = self

        class _C:
            def __enter__(self):  return self
            def __exit__(self, *a): return False

            def get(self, url):
                acct = url.split("account=")[1].split("&")[0]
                payload = outer._by.get(acct, {})

                class _R:
                    def get_json(self_inner):
                        return payload
                return _R()
        return _C()


def _book(**strats):
    return {"per_strategy": {k: {"total_pnl": v[0], "trades": v[1]}
                             for k, v in strats.items()}}


def _pick(slug, entry):
    return {"strategy": slug, "side": "both", "entry": entry}


A = "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"
B = "GOOG_CAM_BREAKOUT_R4S4_V02_5MIN"


def test_a_kairos_tag_is_flagged_when_the_tv_book_is_the_winner():
    app = _FakeApp({"2": _book(**{A: (120.0, 8)}), "3": _book(**{A: (-90.0, 6)})})
    r = crew._entry_tag_conflicts(app, [_pick(A, "kairos")])
    assert len(r["conflicts"]) == 1
    c = r["conflicts"][0]
    assert c["tagged"] == "kairos" and c["better"] == "tv"
    assert c["swing"] == 210.0
    assert "KAIROS book lost $90.00 over 6t" in c["note"]


def test_a_tv_tag_is_flagged_symmetrically():
    """The check must not be one-directional — the engine wins on some names."""
    app = _FakeApp({"2": _book(**{A: (-40.0, 5)}), "3": _book(**{A: (75.0, 9)})})
    c = crew._entry_tag_conflicts(app, [_pick(A, "tv")])["conflicts"]
    assert len(c) == 1 and c[0]["tagged"] == "tv" and c[0]["better"] == "kairos"


def test_a_tag_backed_by_its_own_book_is_not_flagged():
    app = _FakeApp({"2": _book(**{A: (-40.0, 5)}), "3": _book(**{A: (75.0, 9)})})
    assert crew._entry_tag_conflicts(app, [_pick(A, "kairos")])["conflicts"] == []


def test_a_name_losing_on_BOTH_books_is_not_an_entry_conflict():
    """That is a bad strategy, not a bad tag. Flagging it would blame the mechanism
    for something switching mechanisms cannot fix."""
    app = _FakeApp({"2": _book(**{A: (-40.0, 5)}), "3": _book(**{A: (-90.0, 6)})})
    assert crew._entry_tag_conflicts(app, [_pick(A, "kairos")])["conflicts"] == []


def test_a_name_winning_on_both_books_is_not_flagged():
    app = _FakeApp({"2": _book(**{A: (40.0, 5)}), "3": _book(**{A: (90.0, 6)})})
    assert crew._entry_tag_conflicts(app, [_pick(A, "kairos")])["conflicts"] == []


@pytest.mark.parametrize("tagged_trades,other_trades", [(2, 9), (9, 2), (1, 1)])
def test_a_thin_sample_on_either_side_is_not_evidence(tagged_trades, other_trades):
    """Two trades cannot overrule a tag. The threshold applies to BOTH books, so a
    deep record on one side cannot carry a thin one on the other."""
    app = _FakeApp({"2": _book(**{A: (120.0, other_trades)}),
                    "3": _book(**{A: (-90.0, tagged_trades)})})
    assert crew._entry_tag_conflicts(app, [_pick(A, "kairos")])["conflicts"] == []


def test_a_strategy_missing_from_a_book_is_skipped_not_assumed():
    """No fills is not the same as losing fills."""
    app = _FakeApp({"2": _book(**{A: (120.0, 8)}), "3": _book()})
    assert crew._entry_tag_conflicts(app, [_pick(A, "kairos")])["conflicts"] == []


def test_an_unreadable_book_reports_itself_instead_of_passing_silently():
    """An Alpaca outage must not read as "no conflicts" — that is a clean bill of
    health nobody earned."""
    app = _FakeApp({"2": {"fills_unavailable": True}, "3": _book(**{A: (-90.0, 6)})})
    r = crew._entry_tag_conflicts(app, [_pick(A, "kairos")])
    assert r["conflicts"] == [] and r["unreadable"] == ["tv"]


def test_conflicts_are_ordered_by_how_much_the_tag_is_costing():
    app = _FakeApp({
        "2": _book(**{A: (10.0, 5),  B: (300.0, 12)}),
        "3": _book(**{A: (-5.0, 5),  B: (-100.0, 10)})})
    c = crew._entry_tag_conflicts(app, [_pick(A, "kairos"), _pick(B, "kairos")])["conflicts"]
    assert [x["strategy"] for x in c] == [B, A]


def test_an_untagged_pick_defaults_to_tv_rather_than_being_skipped():
    app = _FakeApp({"2": _book(**{A: (-40.0, 5)}), "3": _book(**{A: (75.0, 9)})})
    c = crew._entry_tag_conflicts(app, [{"strategy": A, "side": "both", "entry": None}])
    assert len(c["conflicts"]) == 1 and c["conflicts"][0]["tagged"] == "tv"


def test_the_wire_reports_conflicts_without_refusing():
    """A contrarian tag can be deliberate; only corruption blocks a wire."""
    import inspect
    src = inspect.getsource(crew.api_crew_wire_to_router)
    assert '"entry_conflicts"' in src
    assert "_entry_tag_conflicts" in src
    i = src.index("_entry_tag_conflicts")
    assert "409" not in src[i:], "the entry check must warn, not block"


# ── UI: warnings that survive a successful wire ─────────────────────────────────

def _crewhtml():
    return open("templates/crew.html", encoding="utf-8").read()


def _warn_fn():
    html = _crewhtml()
    i = html.index("function _wireWarnings(d)")
    return html[i:html.index("\n  async function confirmWire", i)]


def test_warnings_render_on_the_success_path_not_only_on_failure():
    """The wire SUCCEEDS with conflicts — the rules are written. If the warning only
    appeared on an error branch it would never be seen."""
    html = _crewhtml()
    i = html.index("open Signal Router")
    assert "_wireWarnings(d)" in html[i:i + 200]


def test_conflicts_are_visually_separate_from_the_green_confirmation():
    """The success line sets colour green for the whole message; an amber warning
    inside it would read as part of the confirmation."""
    src = _warn_fn()
    assert "#F2C76B" in src, "warning colour missing"
    assert "border:1px solid #4a3d1c" in src, "warning needs its own block"


def test_each_conflict_shows_both_sides_of_the_head_to_head():
    """A flag without the numbers behind it is an instruction to trust the tool."""
    src = _warn_fn()
    for field in ("c.tagged_pnl", "c.tagged_trades", "c.other_pnl", "c.other_trades",
                  "c.swing", "c.strategy"):
        assert field in src, field


def test_the_warning_says_the_picks_were_still_wired():
    """Ambiguity here is dangerous: the user must not think the wire was refused."""
    src = _warn_fn()
    assert "These were wired as tagged" in src


def test_an_unreadable_book_is_reported_in_the_ui_too():
    src = _warn_fn()
    assert "entry_check" in src and "unreadable" in src
    assert "Could not read the" in src


def test_a_sizing_conflict_says_which_scheme_won():
    src = _warn_fn()
    assert "sizing_conflict" in src
    assert "equal risk</strong>" in src


def test_the_live_mirror_states_whether_it_can_actually_fire():
    """Crew Live is real money. "Rules written" and "rules that can trade" are
    different facts and the difference is one env var."""
    src = _warn_fn()
    assert "lm.armed" in src
    assert "real money" in src
    assert "LIVE_TRADING_ARMED=1" in src


def test_a_clean_wire_renders_no_warning_block():
    """Warning furniture on a clean wire trains the eye to skip it."""
    src = _warn_fn()
    assert "let html = '';" in src
    assert "return html;" in src
