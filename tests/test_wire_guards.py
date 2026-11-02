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


# ── Short card vs truncation ────────────────────────────────────────────────────

def _report(top_row, block_names):
    block = "\n".join(f"{n} | both | Kairos | audition" for n in block_names)
    return ("| Decision | Recommendation |\n|---|---|\n"
            f"| **Top 6 to run** | {top_row} |\n\n```picks\n{block}\n```\n\n## 0. More\n")


FOUR = ["MU_CAM_BREAKOUT_R3S3_V02_5MIN", "INTC_CAM_BREAKOUT_R3S3_V02_5MIN",
        "PLTR_CAM_BREAKOUT_R3S3_V02_5MIN", "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"]


def test_the_top_row_slugs_are_the_names_the_card_lists():
    row = "<br>".join(f"{i}. {n} — both [Kairos] (AUDITION)" for i, n in enumerate(FOUR, 1))
    row += "<br>4 auditions reached. **RUNNING A SHORT CARD.**"
    assert crew._top_row_slugs(_report(row, FOUR)) == FOUR


def test_a_deliberately_short_card_wires(monkeypatch):
    """2026-11-02: "Top 6 to run" listed 4 names and said it was running a short
    card; the block held the same 4 and four sections followed it. The wire refused
    it as truncated."""
    import app as a
    row = "<br>".join(f"{i}. {n}" for i, n in enumerate(FOUR, 1)) + "<br>SHORT CARD"
    rep = _report(row, FOUR)
    assert crew._claimed_pick_count(rep) == 6
    picks = crew._parse_next_month_card(rep)["picks"]
    named = crew._top_row_slugs(rep)
    assert len(named) <= len(picks), "the card names no more than the block holds"


def test_a_card_naming_more_than_the_block_still_counts_as_cut_off():
    six = FOUR + ["META_CAM_BREAKOUT_R3S3_V02_5MIN", "NFLX_CAM_BREAKOUT_R3S3_V02_5MIN"]
    rep = _report("<br>".join(six), FOUR)
    assert len(crew._top_row_slugs(rep)) > len(crew._parse_next_month_card(rep)["picks"])


# ── through the wire endpoint ───────────────────────────────────────────────────

def _wire_with(tmp_path, monkeypatch, report):
    import shutil, sqlite3
    import app as a
    db = tmp_path / "w.db"
    shutil.copy("trades.db", db)
    c = sqlite3.connect(db)
    c.execute("DELETE FROM routing_rules"); c.execute("DELETE FROM crew_reports")
    c.execute("INSERT INTO crew_reports (week, created_at, report) VALUES (?,?,?)",
              ("2026-W45", "2026-11-02 21:00:00", report))
    c.commit(); c.close()

    def _db():
        k = sqlite3.connect(db); k.row_factory = sqlite3.Row
        return k
    monkeypatch.setattr(a, "get_db", _db)
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca4", {"tag": "alpaca4", "broker": None})
    monkeypatch.setattr(a, "_fetch_alpaca_last_prices", lambda tks: {}, raising=False)
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 0, raising=False)
    a.app.config["TESTING"] = True
    with a.app.test_client() as cl:
        return cl.post("/api/crew/wire_to_router", json={})


def test_the_endpoint_wires_a_short_card(tmp_path, monkeypatch):
    row = "<br>".join(f"{i}. {n} — both [Kairos] (AUDITION)" for i, n in enumerate(FOUR, 1))
    r = _wire_with(tmp_path, monkeypatch, _report(row + "<br>RUNNING A SHORT CARD", FOUR))
    assert r.status_code != 409, r.get_json()


def test_the_endpoint_still_refuses_a_block_shorter_than_the_card(tmp_path, monkeypatch):
    six = FOUR + ["META_CAM_BREAKOUT_R3S3_V02_5MIN", "NFLX_CAM_BREAKOUT_R3S3_V02_5MIN"]
    r = _wire_with(tmp_path, monkeypatch, _report("<br>".join(six), FOUR))
    assert r.status_code == 409 and r.get_json()["found"] == 4
