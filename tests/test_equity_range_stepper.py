"""The dashboard's date arrows page the window by its own length.

One pair of arrows rather than three: the step is READ OFF the window on screen,
so "Last Week" then back means the week before, and a month then back means the
month before. The alternative -- a separate granularity selector -- puts the user
in charge of keeping two controls in agreement.

The arithmetic is where this breaks, so it is what is tested:

  - A month is a CALENDAR month. Stepping Mar 1-31 back by a fixed day count
    lands in mid-February, and stepping Jan 1-31 forward by "one month" has to
    produce Feb 1-28, not a Feb 31 that JavaScript silently reads as March 3.
  - A single day that falls on the 1st is a DAY, not a month. The month rule also
    matches 1st-to-1st, so without an explicit order "Today" paged a whole month
    on the first of the month.
  - new Date('2026-09-21') is UTC midnight, which in US timezones is the previous
    day locally. Parsing from parts is what keeps every step from drifting a day.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a

pytestmark = pytest.mark.skipif(shutil.which("node") is None,
                                reason="node not on PATH")


@pytest.fixture(scope="module")
def index_js():
    a.app.config["TESTING"] = True
    r = a.app.test_client().get("/")
    assert r.status_code == 200
    js = "\n".join(re.findall(r"<script>(.*?)</script>", r.get_data(as_text=True), re.S))
    for fn in ("_equityParse", "_equityStep", "stepEquityRange"):
        assert "function " + fn + "(" in js, fn + " missing from the dashboard"
    return js


def _grab(js, name):
    """Pull one function out by brace balance -- a fixed slice stops covering the
    code it was written for the moment anything above it moves."""
    i = js.index("function " + name + "(")
    depth, started = 0, False
    for j in range(i, len(js)):
        if js[j] == "{":
            depth += 1; started = True
        elif js[j] == "}":
            depth -= 1
            if started and depth == 0:
                return js[i:j + 1]
    raise AssertionError("unbalanced braces in " + name)


def _run(index_js, tmp_path, cases):
    """Drive the real functions against a two-input stub DOM."""
    src = """
const FROM = { value: '' }, TO = { value: '' };
globalThis.document = { getElementById: id => id === 'equityFromDate' ? FROM
                                      : id === 'equityToDate' ? TO : null };
globalThis._equityIso = d => d.toISOString().slice(0, 10);
globalThis._setEquityRange = (f, t) => { FROM.value = f; TO.value = t; };
globalThis._syncEquityDateButtons = () => {};
""" + "\n".join(_grab(index_js, f) for f in
                ("_equityParse", "_equityStep", "stepEquityRange")) + """
const out = [];
for (const [f, t, dir] of CASES) {
  FROM.value = f; TO.value = t;
  const st = _equityStep();
  if (dir !== 0) stepEquityRange(dir);
  out.push([st && st.label, FROM.value, TO.value]);
}
console.log(JSON.stringify(out));
""".replace("CASES", repr(cases).replace("'", '"'))
    p = tmp_path / "stepper.js"
    p.write_text(src, encoding="utf-8")
    res = subprocess.run(["node", str(p)], capture_output=True, text=True, timeout=60)
    assert res.returncode == 0, res.stderr.strip()
    import json
    return json.loads(res.stdout)


def test_a_single_day_steps_one_day(index_js, tmp_path):
    (lbl, f, t), = _run(index_js, tmp_path, [["2026-10-02", "2026-10-02", -1]])
    assert lbl == "day"
    assert (f, t) == ("2026-10-01", "2026-10-01")


def test_a_single_day_on_the_first_is_still_a_day(index_js, tmp_path):
    """The month rule also matches 1st-to-1st. Without an explicit order this
    paged a whole month, so "Today" on the 1st jumped to November."""
    (lbl, f, t), = _run(index_js, tmp_path, [["2026-10-01", "2026-10-01", 1]])
    assert lbl == "day"
    assert (f, t) == ("2026-10-02", "2026-10-02")


def test_a_weekday_week_steps_a_full_seven_days(index_js, tmp_path):
    """Last Week is Mon-Fri, 5 days. Stepping 5 would land Wed-Sun and drift
    further every click; a week has to move to the previous Monday."""
    (lbl, f, t), = _run(index_js, tmp_path, [["2026-09-21", "2026-09-25", -1]])
    assert lbl == "week"
    assert (f, t) == ("2026-09-14", "2026-09-18")


def test_a_full_month_steps_to_the_whole_previous_month(index_js, tmp_path):
    (lbl, f, t), = _run(index_js, tmp_path, [["2026-09-01", "2026-09-30", -1]])
    assert lbl == "month"
    assert (f, t) == ("2026-08-01", "2026-08-31")


def test_month_length_is_respected_in_both_directions(index_js, tmp_path):
    """Mar 1-31 back is Feb 1-28, and Feb 1-28 forward is Mar 1-31. Carrying the
    day number across would ask for Feb 31, which JavaScript reads as March 3."""
    back, fwd = _run(index_js, tmp_path, [["2026-03-01", "2026-03-31", -1],
                                          ["2026-02-01", "2026-02-28", 1]])
    assert (back[1], back[2]) == ("2026-02-01", "2026-02-28")
    assert (fwd[1], fwd[2]) == ("2026-03-01", "2026-03-31")


def test_stepping_a_month_crosses_the_year(index_js, tmp_path):
    (_, f, t), = _run(index_js, tmp_path, [["2026-01-01", "2026-01-31", -1]])
    assert (f, t) == ("2025-12-01", "2025-12-31")


def test_a_partial_month_keeps_its_shape(index_js, tmp_path):
    """This Month is 1st-to-today. Back gives the same slice of the month before."""
    (lbl, f, t), = _run(index_js, tmp_path, [["2026-10-01", "2026-10-02", -1]])
    assert lbl == "month"
    assert (f, t) == ("2026-09-01", "2026-09-02")


def test_an_arbitrary_span_steps_by_its_own_length(index_js, tmp_path):
    (lbl, f, t), = _run(index_js, tmp_path, [["2026-09-10", "2026-09-24", -1]])
    assert lbl == "15d"
    assert (f, t) == ("2026-08-26", "2026-09-09")


def test_no_range_and_a_reversed_range_page_nothing(index_js, tmp_path):
    """Cleared dates and an inverted range must return null, not a guess -- the
    arrows disable rather than inventing a window."""
    rows = _run(index_js, tmp_path, [["", "", 0], ["2026-10-05", "2026-10-01", 0]])
    assert rows[0][0] is None
    assert rows[1][0] is None


def test_dates_do_not_drift_a_day_when_parsed(index_js, tmp_path):
    """Ten steps back from a Monday is exactly ten Mondays back. new Date() on a
    'YYYY-MM-DD' string is UTC midnight, so a US-local getDate() would shed a day
    per step and this would land on a Sunday."""
    cases = [["2026-09-21", "2026-09-25", -1]]
    cur = ("2026-09-21", "2026-09-25")
    for _ in range(10):
        (_, f, t), = _run(index_js, tmp_path, [[cur[0], cur[1], -1]])
        cur = (f, t)
    assert cur == ("2026-07-13", "2026-07-17")
