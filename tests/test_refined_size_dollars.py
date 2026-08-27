"""REFINED_SIZE_DOLLARS — one flat size across the curated books.

TV Refined trades $16k-$25k per position off a score ladder, and a single ordinary
0.54% trail stop there costs $86-$135 — enough for one routine losing trade to
exhaust a $125 daily-loss limit. This collapses the ladder to one number so the
curated books can be sized against the risk limit rather than against each other.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

import app as kairos


def test_unset_keeps_the_score_graded_ladder():
    """The default must be exactly what shipped — this is opt-in."""
    assert kairos._build_size_bands(0) == [(60, 25_000), (52, 22_000),
                                           (46, 19_000), (0, 16_000)]
    assert kairos._build_size_bands(None) == kairos._build_size_bands(0)


def test_a_flat_value_collapses_every_band_to_it():
    assert kairos._build_size_bands(15_000) == [(0, 15_000)]


def test_every_score_gets_the_flat_size(monkeypatch):
    """Including scores that would have landed on different rungs."""
    monkeypatch.setattr(kairos, "_REFINED_SIZE_BANDS", kairos._build_size_bands(15_000))
    for score in (0.0, 0.3, 0.45, 0.46, 0.52, 0.60, 0.99, 1.0):
        assert kairos._band_target_dollars(score) == 15_000, score


def test_the_ladder_still_grades_when_unset(monkeypatch):
    monkeypatch.setattr(kairos, "_REFINED_SIZE_BANDS", kairos._build_size_bands(0))
    assert kairos._band_target_dollars(0.65) == 25_000
    assert kairos._band_target_dollars(0.53) == 22_000
    assert kairos._band_target_dollars(0.10) == 16_000


def test_crew_paper_inherits_the_flat_size():
    """The crew wire sizes from _REFINED_SIZE_BANDS[0][1] so Crew Paper matches the
    curated books. Collapsing the ladder must carry through that index."""
    assert kairos._build_size_bands(15_000)[0][1] == 15_000


def test_a_zero_or_negative_value_is_ignored_rather_than_sizing_at_zero():
    """A $0 position is not a smaller trade, it is a broken one."""
    for bad in (0, -1, -15_000):
        assert kairos._build_size_bands(bad) == kairos._build_size_bands(0)


def test_a_non_numeric_env_value_does_not_take_the_app_down():
    """Read at import; raising there would fail the whole deploy over a typo."""
    import inspect
    src = inspect.getsource(kairos)
    i = src.index('REFINED_SIZE_DOLLARS = float(')
    assert "except (TypeError, ValueError)" in src[i - 200:i + 300]


def test_the_flat_size_is_a_whole_number_of_dollars():
    """It divides into a share count; a float target would round unpredictably."""
    bands = kairos._build_size_bands(15_000.7)
    assert bands == [(0, 15_000)] and isinstance(bands[0][1], int)
