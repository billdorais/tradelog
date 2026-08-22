"""The "Changes vs the Current Book" table is regrouped before it is rendered.

The crew emits one flat table. The trader reads it to decide a re-wire, so it is
grouped by entry mechanism ([TV] / [Kairos]) and then ordered KEEP -> ADD -> DROP.

Done in the renderer rather than the prompt so it also reorders reports that were
already saved, and so the ordering cannot be half-followed by a model. That makes
the safety property the important one: a report must never come back mangled or
short a row. These probes run the real page JS and throw on violation — the load
harness reports a throw as a failure with the message on stderr.
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

REPORT = """## Next Month

### Changes vs the Current Book

| Strategy | Entry [TV]/[Kairos] | Live P&L (since wire) | Action |
|---|---|---|---|
| NVDA_CAM_BREAKOUT_R3S3_V02_5MIN | [TV] | $492.32 / 14t | **KEEP** - top earner |
| XLK_CAM_BREAKOUT_R3S3_V02_5MIN | [TV] | $70.96 / 1t | **DROP** - 1 trade only |
| XOM_CAM_BREAKOUT_R4S4_V02_5MIN | [TV] | n/a | **ADD** - replaces XLK |
| AAPL_CAM_BREAKOUT_R4S4_V02_5MIN | [Kairos] | $77.93 / 5t | **KEEP** - positive live |
| MS_CAM_REVERSAL_R3S3_V02_5MIN | [Kairos] | -$40.00 / 6t | **DROP** - clear bleeder |
| SPY_CAM_REVERSAL_R4S4_V02_5MIN | [Kairos] | n/a | **ADD** - farm top-ranked |

KEEP 2 - ADD 2 - DROP 2

```picks
NVDA_CAM_BREAKOUT_R3S3_V02_5MIN | both | TV
```
"""

# Assertion helpers injected alongside the page JS.
PRELUDE = """
function ok(cond, msg) { if (!cond) throw new Error('FAILED: ' + msg); }
function idx(hay, needle) { return hay.indexOf(needle); }
"""


@pytest.fixture(scope="module")
def crew_js():
    a.app.config["TESTING"] = True
    r = a.app.test_client().get("/crew")
    assert r.status_code == 200
    js = "\n".join(re.findall(r"<script>(.*?)</script>", r.get_data(as_text=True), re.S))
    assert "_groupChangesTable" in js, "regrouper missing from the crew page"
    return js


def _probe(crew_js, tmp_path, name, body):
    """Run the page JS plus a probe. Any throw fails, with the message on stderr."""
    src = crew_js + "\n" + PRELUDE + "\nconst MD = " + _js_str(REPORT) + ";\n" + body
    p = tmp_path / (name + ".js")
    p.write_text(src, encoding="utf-8")
    return subprocess.run(["node", _HARNESS, str(p)],
                          capture_output=True, text=True, timeout=60)


def _js_str(s):
    import json
    return json.dumps(s)


def _assert_ok(res):
    assert res.returncode == 0, res.stderr.strip() or "probe failed"


def test_tv_group_comes_before_kairos_group(crew_js, tmp_path):
    _assert_ok(_probe(crew_js, tmp_path, "order", """
      const g = _groupChangesTable(MD);
      ok(idx(g, '[TV] entries') > -1, 'no TV group');
      ok(idx(g, '[Kairos] entries') > -1, 'no Kairos group');
      ok(idx(g, '[TV] entries') < idx(g, '[Kairos] entries'), 'Kairos came first');
    """))


def test_rows_are_ordered_keep_then_add_then_drop_within_a_group(crew_js, tmp_path):
    _assert_ok(_probe(crew_js, tmp_path, "actions", """
      const g   = _groupChangesTable(MD);
      const tv  = g.slice(idx(g, '[TV] entries'), idx(g, '[Kairos] entries'));
      ok(idx(tv, 'NVDA') < idx(tv, 'XOM'), 'KEEP should precede ADD');
      ok(idx(tv, 'XOM')  < idx(tv, 'XLK'), 'ADD should precede DROP');
      const ka = g.slice(idx(g, '[Kairos] entries'));
      ok(idx(ka, 'AAPL') < idx(ka, 'SPY'), 'KEEP should precede ADD');
      ok(idx(ka, 'SPY')  < idx(ka, 'MS_'), 'ADD should precede DROP');
    """))


def test_no_row_is_lost(crew_js, tmp_path):
    """The whole point is a re-wire decision; a dropped row is a silent wrong call."""
    _assert_ok(_probe(crew_js, tmp_path, "rowcount", """
      const g = _groupChangesTable(MD);
      const before = (MD.match(/_V02_5MIN/g) || []).length;
      const after  = (g.match(/_V02_5MIN/g) || []).length;
      ok(before === after, 'slug count changed: ' + before + ' -> ' + after);
    """))


def test_each_group_is_counted_in_its_heading(crew_js, tmp_path):
    _assert_ok(_probe(crew_js, tmp_path, "counts", """
      const g = _groupChangesTable(MD);
      ok(/\\[TV\\] entries\\*\\* . KEEP 1 . ADD 1 . DROP 1/.test(g),
         'TV heading missing its counts: ' + g.slice(idx(g,'[TV] entries'), idx(g,'[TV] entries')+60));
    """))


def test_redundant_entry_column_is_dropped_inside_a_group(crew_js, tmp_path):
    _assert_ok(_probe(crew_js, tmp_path, "col", """
      const g  = _groupChangesTable(MD);
      const tv = g.slice(idx(g, '[TV] entries'), idx(g, '[Kairos] entries'));
      ok(idx(tv, '| [TV] |') < 0, 'entry cell still repeated under its own heading');
      ok(idx(tv, 'Strategy') > -1 && idx(tv, 'Action') > -1, 'lost the other columns');
    """))


def test_the_tally_and_the_wire_block_survive(crew_js, tmp_path):
    """The picks fence is parsed literally by the wire button — touching it would
    change what actually gets traded."""
    _assert_ok(_probe(crew_js, tmp_path, "tail", """
      const g = _groupChangesTable(MD);
      ok(/KEEP 2 - ADD 2 - DROP 2/.test(g), 'tally line lost');
      ok(/```picks/.test(g), 'wire block lost');
      ok(/NVDA_CAM_BREAKOUT_R3S3_V02_5MIN \\| both \\| TV/.test(g), 'wire row altered');
    """))


def test_markdown_without_the_section_is_returned_untouched(crew_js, tmp_path):
    _assert_ok(_probe(crew_js, tmp_path, "noop", """
      const plain = '# Hello\\n\\nsome text\\n';
      ok(_groupChangesTable(plain) === plain, 'rewrote unrelated markdown');
      ok(_groupChangesTable('') === '', 'choked on empty input');
    """))


def test_a_section_with_no_parseable_table_is_left_alone(crew_js, tmp_path):
    """Better an ungrouped table than a mangled report."""
    _assert_ok(_probe(crew_js, tmp_path, "unparseable", """
      const md = '### Changes vs the Current Book\\n\\nNo changes this month.\\n';
      ok(_groupChangesTable(md) === md, 'mangled a section with no table');
      const oneRow = '### Changes vs the Current Book\\n\\n| A | Entry |\\n|---|---|\\n| x | [TV] |\\n';
      ok(_groupChangesTable(oneRow) === oneRow, 'regrouped a table too small to need it');
    """))


def test_a_table_without_an_entry_column_is_left_alone(crew_js, tmp_path):
    """Entry is the primary grouping key; without it there is nothing to group by."""
    _assert_ok(_probe(crew_js, tmp_path, "noentry", """
      const md = '### Changes vs the Current Book\\n\\n| Strategy | Action |\\n|---|---|\\n'
               + '| A_CAM | KEEP |\\n| B_CAM | DROP |\\n';
      ok(_groupChangesTable(md) === md, 'grouped a table with no entry column');
    """))


def test_an_unrecognised_entry_keeps_its_row_and_its_column(crew_js, tmp_path):
    """A tag the parser does not know must not silently vanish into a TV group."""
    _assert_ok(_probe(crew_js, tmp_path, "unknown", """
      const md = '### Changes vs the Current Book\\n\\n'
               + '| Strategy | Entry | Action |\\n|---|---|---|\\n'
               + '| A_CAM | [TV] | KEEP |\\n| B_CAM | [???] | DROP |\\n';
      const g = _groupChangesTable(md);
      ok(idx(g, 'B_CAM') > -1, 'unknown-entry row disappeared');
      ok(idx(g, 'Unclassified entry') > -1, 'no bucket for the unknown tag');
      ok(idx(g, '[???]') > -1, 'unknown tag lost its entry cell');
    """))


def test_the_renderer_actually_calls_the_regrouper(crew_js, tmp_path):
    """A transform nothing routes through is dead code that reads as a feature."""
    assert "_groupChangesTable(md || '')" in crew_js


def test_the_probe_harness_can_actually_fail(crew_js, tmp_path):
    """A guard that cannot fail proves nothing."""
    r = _probe(crew_js, tmp_path, "selftest", "ok(false, 'deliberate');")
    assert r.returncode == 1
    assert "deliberate" in r.stderr
