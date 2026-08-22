"""Replaying a crew report's picks over a past window.

The picks carry an entry source ([TV] / [Kairos]) and are sourced from the matching
FARM, because a freshly promoted pick has no history in the curated book it is being
promoted into.
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

import app as kairos
import routes.crew as crew

REPORT = """
## Next Month - Crew Paper

| Sizing | equal |
| Entries | TV |

```picks
AAPL_CAM_BREAKOUT_R4S4_V02_5MIN | long | TV
MSFT_CAM_BREAKOUT_R3S3_V02_5MIN | both | Kairos
NVDA_CAM_REVERSAL_R3S3_V02_5MIN | short | TV
```
"""


def _fill(strategy, side, pnl, day, entry_hhmm, exit_hhmm):
    """A round-trip as _pair_alpaca_fills_lifo would emit it."""
    return {"strategy": strategy, "side": side, "pnl": pnl, "ticker": strategy.split("_")[0],
            "entry_time": f"2026-08-{day:02d}T{entry_hhmm}:00-04:00",
            "exit_time":  f"2026-08-{day:02d}T{exit_hhmm}:00-04:00", "qty": 10}


@pytest.fixture
def seeded(monkeypatch):
    conn = kairos.get_db(); cur = conn.cursor()
    cur.execute("DELETE FROM crew_reports")
    p = kairos.placeholder()
    cur.execute(f"INSERT INTO crew_reports (week, created_at, report) VALUES ({p},{p},{p})",
                ("2026-W33", "2026-08-15T10:00:00", REPORT))
    conn.commit(); conn.close()

    farm = {
        # acct1 = TV farm, acct5 = Kairos farm
        "1": [_fill("AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "LONG",  100.0, 3, "09:40", "10:05"),
              _fill("AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "SHORT", 999.0, 4, "09:40", "10:05"),
              _fill("NVDA_CAM_REVERSAL_R3S3_V02_5MIN", "SHORT",  50.0, 5, "09:40", "10:05"),
              _fill("AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "LONG",  777.0, 6, "07:10", "07:30"),
              _fill("UNPICKED_CAM_BREAKOUT_R3S3_V02_5MIN", "LONG", 500.0, 7, "09:40", "10:05")],
        "5": [_fill("MSFT_CAM_BREAKOUT_R3S3_V02_5MIN", "LONG", -30.0, 3, "09:40", "10:05"),
              # same name on the TV farm must NOT be picked up for a [Kairos] pick
              _fill("AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "LONG", 400.0, 3, "09:40", "10:05")],
    }
    monkeypatch.setattr(crew, "_SELECTION_CURVE_SOURCES", {"tv": "1", "kairos": "5"})
    monkeypatch.setattr(kairos, "_alpaca_account_ctx",
                        lambda a: (object(), f"alpaca{a}", f"Farm {a}", lambda: farm.get(str(a), [])))
    monkeypatch.setattr(kairos, "_pair_alpaca_fills_lifo",
                        lambda fills, **kw: {"closed_clean": fills})
    monkeypatch.setattr(kairos, "_shared_hours_windows", lambda key: [("09:35", "10:00")])
    kairos.app.config["TESTING"] = True
    return kairos.app.test_client()


def _get(client, qs=""):
    r = client.get("/api/crew/selection_curve" + qs)
    return r.status_code, (r.get_json() or {})


def test_each_pick_is_sourced_from_the_farm_matching_its_entry(seeded):
    """A [Kairos] pick must come from the engine farm, not the TV farm. The same
    strategy name trades on BOTH farms, so crossing them would silently credit a
    pick with the other mechanism's result."""
    _, d = _get(seeded, "?hours=all")
    names = {(t["strategy"], t["entry"]) for t in d["curve"]}
    assert ("MSFT_CAM_BREAKOUT_R3S3_V02_5MIN", "kairos") in names
    assert ("AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "tv") in names
    assert ("AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "kairos") not in names, \
        "AAPL is a [TV] pick; its acct5 fills belong to a different mechanism"


def test_strategies_outside_the_selection_are_excluded(seeded):
    _, d = _get(seeded, "?hours=all")
    assert not any("UNPICKED" in t["strategy"] for t in d["curve"])


def test_a_side_scoped_pick_only_counts_that_side(seeded):
    """AAPL is wired long-only; its short farm fill is a different bet."""
    _, d = _get(seeded, "?hours=all")
    aapl = [t for t in d["curve"] if t["strategy"].startswith("AAPL")]
    assert aapl and all(t["side"] == "long" for t in aapl)
    assert not any(t["pnl"] == 999.0 for t in d["curve"])


def test_curated_hours_is_the_default_because_farms_trade_all_day(seeded):
    """A 07:10 farm fill is unreachable by the book that would trade these picks."""
    _, d = _get(seeded)
    assert d["hours"] == "curated"
    assert not any(t["pnl"] == 777.0 for t in d["curve"]), "pre-window fill leaked in"
    _, d_all = _get(seeded, "?hours=all")
    assert any(t["pnl"] == 777.0 for t in d_all["curve"])


def test_curve_is_cumulative_and_time_ordered(seeded):
    _, d = _get(seeded, "?hours=all")
    times = [t["time"] for t in d["curve"]]
    assert times == sorted(times)
    running = 0.0
    for pt in d["curve"]:
        running = round(running + pt["pnl"], 2)
        assert pt["value"] == running
    assert d["total_pnl"] == d["curve"][-1]["value"]


def test_totals_are_split_by_entry_mechanism(seeded):
    """The whole question is TV entries vs Kairos entries."""
    _, d = _get(seeded, "?hours=all")
    assert d["by_entry"]["tv"]["picks"] == 2
    assert d["by_entry"]["kairos"]["picks"] == 1
    assert d["by_entry"]["kairos"]["pnl"] == -30.0


def test_a_failed_fills_fetch_is_reported_not_drawn_as_flat(seeded, monkeypatch):
    """An empty list from an outage must never render as a flat line."""
    monkeypatch.setattr(kairos, "_alpaca_account_ctx",
                        lambda a: (object(), f"alpaca{a}", f"Farm {a}", lambda: []))
    monkeypatch.setattr(kairos, "_fills_error", lambda a: "connection reset")
    _, d = _get(seeded, "?hours=all")
    assert d["fills_unavailable"], "outage silently produced an empty curve"
    assert d["curve"] == []


def test_response_says_the_replay_is_in_sample(seeded):
    """The picks were chosen on this window; a rising curve is partly built in."""
    _, d = _get(seeded, "?hours=all")
    assert d["in_sample"] is True
    assert "IN-SAMPLE" in d["caveat"]


def test_missing_report_is_a_404_not_an_empty_chart(seeded):
    conn = kairos.get_db(); cur = conn.cursor()
    cur.execute("DELETE FROM crew_reports"); conn.commit(); conn.close()
    code, d = _get(seeded)
    assert code == 404 and "error" in d


# ── UI ──────────────────────────────────────────────────────────────────────────

def _crewhtml():
    return open("templates/crew.html", encoding="utf-8").read()


def test_page_has_a_chart_library():
    """The crew page had none before this section."""
    assert "chart.umd.min.js" in _crewhtml()


def test_replay_defaults_to_curated_hours():
    """The farms trade all day; the book that would run these picks does not."""
    html = _crewhtml()
    assert "let _selHours = 'curated';" in html
    assert 'data-hours="curated"' in html and "sel-hours active" in html


def test_replay_default_window_matches_the_crew_default():
    html = _crewhtml()
    assert "let _selDays  = 45;" in html


def test_both_entry_mechanisms_are_plotted_separately():
    """The question is TV entries vs Kairos entries, so they get their own lines."""
    html = _crewhtml()
    assert "ds('TV entries'" in html and "ds('Kairos entries'" in html


def test_in_sample_caveat_is_rendered_not_just_returned():
    html = _crewhtml()
    assert "In-sample." in html and "d.caveat" in html


def test_unavailable_fills_are_surfaced_in_the_ui():
    html = _crewhtml()
    assert "d.fills_unavailable" in html and "incomplete" in html


def test_per_pick_contribution_is_listed():
    """A curve carried by one name reads the same as a broad one until you break it out."""
    html = _crewhtml()
    assert "Contribution by pick" in html and "d.per_strategy" in html


# ── source=actual: real fills from the crew book ────────────────────────────────

@pytest.fixture
def seeded_actual(monkeypatch, seeded):
    """Crew Paper (acct4) holding real fills for picks of BOTH mechanisms."""
    acct4 = [_fill("AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "LONG",   60.0, 3, "09:40", "10:05"),
             _fill("MSFT_CAM_BREAKOUT_R3S3_V02_5MIN", "LONG",  -25.0, 4, "09:40", "10:05"),
             _fill("NVDA_CAM_REVERSAL_R3S3_V02_5MIN", "SHORT",  15.0, 5, "07:10", "07:30"),
             _fill("UNPICKED_CAM_BREAKOUT_R3S3_V02_5MIN", "LONG", 900.0, 6, "09:40", "10:05")]
    monkeypatch.setattr(kairos, "_alpaca_account_ctx",
                        lambda a: (object(), f"alpaca{a}", f"Book {a}",
                                   lambda: acct4 if str(a) in ("4", "6") else []))
    return seeded


def test_actual_reads_the_crew_book_not_the_farms(seeded_actual):
    _, d = _get(seeded_actual, "?source=actual")
    assert d["source"] == "actual" and d["account"] == "4"
    assert d["by_entry"]["tv"]["account"] == "4"
    assert d["by_entry"]["kairos"]["account"] == "4"


def test_actual_attributes_each_trade_to_its_picks_mechanism(seeded_actual):
    """One book holds both mechanisms, so the split comes from the pick's [TV] /
    [Kairos] tag rather than from which account the fill came off."""
    _, d = _get(seeded_actual, "?source=actual")
    by = {t["strategy"]: t["entry"] for t in d["curve"]}
    assert by["AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"] == "tv"
    assert by["MSFT_CAM_BREAKOUT_R3S3_V02_5MIN"] == "kairos"
    assert d["by_entry"]["kairos"]["pnl"] == -25.0
    assert d["by_entry"]["tv"]["pnl"] == 75.0


def test_actual_still_excludes_strategies_that_are_not_picks(seeded_actual):
    _, d = _get(seeded_actual, "?source=actual")
    assert not any("UNPICKED" in t["strategy"] for t in d["curve"])
    assert d["total_pnl"] == 50.0


def test_actual_does_not_filter_by_curated_hours(seeded_actual):
    """The book already trades inside its gates; filtering its own fills would drop
    nothing and imply a filter that is not doing work."""
    _, d = _get(seeded_actual, "?source=actual")
    assert d["hours"] == "all"
    assert any(t["pnl"] == 15.0 for t in d["curve"]), "07:10 book fill was dropped"


def test_actual_is_not_labelled_in_sample(seeded_actual):
    """These are real forward fills, not a replay of the selection window."""
    _, d = _get(seeded_actual, "?source=actual")
    assert d["in_sample"] is False
    assert "REAL FILLS" in d["caveat"]
    assert "wire date" in d["caveat"], "must explain why a pick can show no trades"


def test_farm_mode_is_unchanged_by_the_new_parameter(seeded_actual):
    _, d = _get(seeded_actual, "?hours=all")
    assert d["source"] == "farm" and d["in_sample"] is True


def test_only_a_real_crew_book_can_be_read(seeded_actual):
    """Pointing this at a farm would quietly relabel simulated fills as real."""
    code, d = _get(seeded_actual, "?source=actual&account=1")
    assert code == 400 and "crew book" in d["error"]


def test_crew_live_is_selectable(seeded_actual):
    _, d = _get(seeded_actual, "?source=actual&account=6")
    assert d["account"] == "6"


def test_both_charts_share_one_window():
    """Two charts on different periods would invite a false comparison."""
    html = _crewhtml()
    i = html.index("function setSelDays")
    block = html[i:i + 400]
    assert "loadSelectionCurve();" in block and "loadBookCurve();" in block


def test_book_chart_is_present_and_defaults_to_crew_paper():
    html = _crewhtml()
    assert 'id="bookPerfSection"' in html
    assert "let _bookAcct  = '4';" in html
    assert "source=actual" in html
