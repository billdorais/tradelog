"""Div balance across the templates.

Twice now a hand-edited panel has shipped with a missing </div>: the browser
silently nests the following siblings inside the unclosed element instead of
erroring, so a recap tile swallowed the two tiles after it and the layout only
looked "mysteriously squeezed". A tag count is crude but catches exactly that,
and it is far cheaper than noticing it in a screenshot.
"""
from __future__ import annotations

import glob
import os
import re

import pytest

TEMPLATES = sorted(glob.glob(os.path.join("templates", "*.html")))
assert TEMPLATES, "no templates found — is the test running from the repo root?"


@pytest.mark.parametrize("path", TEMPLATES, ids=[os.path.basename(p) for p in TEMPLATES])
def test_div_tags_balance(path):
    with open(path, encoding="utf-8") as fh:
        src = fh.read()
    opens  = len(re.findall(r"<div\b", src))
    closes = len(re.findall(r"</div>", src))
    assert opens == closes, (
        f"{os.path.basename(path)}: {opens} <div> vs {closes} </div> "
        f"({opens - closes:+d}). An unclosed div nests the following siblings "
        f"inside it — that is how the recap tiles ended up inside each other."
    )


def test_recap_tiles_are_siblings_not_nested():
    """The specific regression: every tile must be a direct child of .tiles. When
    the profit-factor tile lost its closing tag, max-drawdown and avg-win/loss
    became its children and collapsed to a few pixels wide."""
    with open(os.path.join("templates", "recap.html"), encoding="utf-8") as fh:
        src = fh.read()
    block = src[src.index('<div class="tiles">'):src.index("${gatePills}")]
    depth = 0
    top_level_tiles = 0
    for tok in re.findall(r"<div\b[^>]*>|</div>", block):
        if tok.startswith("</"):
            depth -= 1
        else:
            if depth == 1 and 'class="tile' in tok:
                top_level_tiles += 1
            depth += 1
    assert top_level_tiles == 7, f"expected 7 sibling tiles, found {top_level_tiles}"
    assert depth == 1, f".tiles should still be open before the gate pills, depth={depth}"
