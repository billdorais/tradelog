"""Top-N can rank on the period BEFORE the one it charts.

The plain Top-N ranks by P&L over the visible range and then draws that same
range, so every name it picks is positive by construction and the curve cannot
slope down — it grades its own homework. The OOS toggle moves only the RANKING
query to the preceding equal-length window, so the basket is judged on data it
never saw. A selection that only works in-sample goes flat.
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

_HARNESS = os.path.join(os.path.dirname(__file__), "js_load_harness.js")

pytestmark = pytest.mark.skipif(shutil.which("node") is None,
                                reason="node not on PATH")

PRELUDE = """
function ok(cond, msg) { if (!cond) throw new Error('FAILED: ' + msg); }
"""


@pytest.fixture(scope="module")
def index_js():
    a.app.config["TESTING"] = True
    r = a.app.test_client().get("/")
    assert r.status_code == 200
    js = "\n".join(re.findall(r"<script>(.*?)</script>", r.get_data(as_text=True), re.S))
    assert "_priorWindow" in js, "OOS helpers missing from the dashboard"
    return js


def _probe(index_js, tmp_path, name, body):
    p = tmp_path / (name + ".js")
    p.write_text(index_js + "\n" + PRELUDE + "\n" + body, encoding="utf-8")
    return subprocess.run(["node", _HARNESS, str(p)],
                          capture_output=True, text=True, timeout=60)


def _assert_ok(res):
    assert res.returncode == 0, res.stderr.strip() or "probe failed"


def test_prior_window_is_equal_length_and_ends_the_day_before(index_js, tmp_path):
    """8/1-8/22 is 22 days, so the ranking window is 7/10-7/31 — same length, no
    overlap. An overlapping window would leak the answer back into the ranking."""
    _assert_ok(_probe(index_js, tmp_path, "window", """
      const w = _priorWindow('2026-08-01', '2026-08-22');
      ok(w.from === '2026-07-10', 'from was ' + w.from);
      ok(w.to   === '2026-07-31', 'to was '   + w.to);
      ok(w.days === 22, 'length was ' + w.days);
      ok(w.to < '2026-08-01', 'ranking window overlaps the charted one');
    """))


def test_prior_window_is_undefined_for_an_open_ended_range(index_js, tmp_path):
    """"Before all time" is not a window; the caller must refuse rather than guess."""
    _assert_ok(_probe(index_js, tmp_path, "openended", """
      ok(_priorWindow('', '2026-08-22') === null, 'invented a window with no start');
      ok(_priorWindow('2026-08-01', '') === null, 'invented a window with no end');
      ok(_priorWindow('2026-08-22', '2026-08-01') === null, 'accepted a reversed range');
      ok(_priorWindow('nonsense', 'x') === null, 'accepted junk dates');
    """))


def test_a_single_day_range_still_yields_a_window(index_js, tmp_path):
    _assert_ok(_probe(index_js, tmp_path, "oneday", """
      const w = _priorWindow('2026-08-10', '2026-08-10');
      ok(w && w.from === '2026-08-09' && w.to === '2026-08-09', JSON.stringify(w));
    """))


def test_month_boundaries_and_leap_day_are_handled(index_js, tmp_path):
    """Date arithmetic done by hand is where this would quietly go wrong."""
    _assert_ok(_probe(index_js, tmp_path, "boundary", """
      const w = _priorWindow('2026-03-01', '2026-03-03');
      ok(w.from === '2026-02-26' && w.to === '2026-02-28', JSON.stringify(w));
      const l = _priorWindow('2024-03-01', '2024-03-01');
      ok(l.from === '2024-02-29', 'leap day lost: ' + JSON.stringify(l));
    """))


def test_toggle_refuses_to_turn_on_without_a_range(index_js, tmp_path):
    """Turning it on when it cannot work would silently behave as in-sample."""
    _assert_ok(_probe(index_js, tmp_path, "toggle", """
      _topNOutOfSample = false;
      document.getElementById = () => ({ value: '' });
      toggleTopNOOS(null);
      ok(_topNOutOfSample === false, 'enabled OOS with no date range');
    """))


def test_selection_is_never_emptied_by_a_barren_ranking_window(index_js, tmp_path):
    """An empty selection means "show all" in this picker, so clearing it would
    invert the filter and chart the whole book."""
    html = open("templates/index.html", encoding="utf-8").read()
    i = html.index("async function selectTopN")
    block = html[i:i + 4500]
    assert "if (!top.length)" in block
    guard = block[block.index("if (!top.length)"):]
    assert "return;" in guard.split("selectedStrategies =")[0], \
        "must bail out before assigning an empty Set"


def test_only_the_ranking_query_moves_not_the_chart(index_js, tmp_path):
    """The whole point is to chart the visible range with a basket chosen elsewhere.
    Moving the chart too would just show the old period again."""
    html = open("templates/index.html", encoding="utf-8").read()
    i = html.index("async function selectTopN")
    block = html[i:i + 4500]
    assert "fromDate = prior.from" in block and "toDate   = prior.to" in block
    # applyEquityFilter reads the date inputs, which are never written to.
    assert "equityFromDate').value =" not in block
    assert "equityToDate').value =" not in block


def test_the_toggle_is_offered_in_the_preset_bar(index_js, tmp_path):
    html = open("templates/index.html", encoding="utf-8").read()
    assert 'id="topNOosChip"' in html and "toggleTopNOOS(event)" in html
    assert 'id="topNOosHint"' in html, "user should see which window ranked the basket"


def test_all_trades_is_coerced_to_an_array():
    """Same guard as `execs`: a non-array reaching one of the many allTrades.filter
    calls throws inside a render and blanks the dashboard."""
    html = open("templates/index.html", encoding="utf-8").read()
    assert "allTrades = Array.isArray(trades) ? trades : [];" in html


def test_the_probe_harness_can_actually_fail(index_js, tmp_path):
    r = _probe(index_js, tmp_path, "selftest", "ok(false, 'deliberate');")
    assert r.returncode == 1 and "deliberate" in r.stderr
