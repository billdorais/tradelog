"""Kairos Select on the dashboard: a roster badge and a daily P&L card.

Both hang off things that were previously conflated, and the point of these tests
is that the two concepts stay separate:

  - The glance row used to mean "carries a profit lock". That coincided with "a
    book whose day I watch" until Kairos Select, which mirrors the FARM's gates on
    purpose (no profit lock) and is still a book whose day matters. Flipping
    profit_lock to get a card would have broken the farm-mirroring the whole
    experiment rests on.
  - The badge is built from the server-side roster, so a strategy leaving the
    account's routing rules stops being badged without a second edit anywhere.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a

SELECT, FARM = "7", "5"


# Records are built through the SAME function production uses, so a flag dropped
# from it breaks the fixture and the app together. Reconstructing the key list
# here is what let `glance` ship with no card: the stub had it, the record did not.
def _record(n):
    meta = a.ACCOUNT_META[n]
    rec = {"num": n, "tag": meta["tag"], "paper": n != "6"}
    rec.update(a._account_meta_fields(n, meta))
    return rec


def _accounts(monkeypatch):
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS",
                        [_record(n) for n in ("1", "2", "3", "4", "5", "6", "7")])

def test_every_meta_flag_the_ui_reads_is_copied_into_the_record():
    """The bug this guards: _ui_accounts reads its flags off the account RECORD,
    and that record is assembled from an EXPLICIT key list rather than the meta
    dict. A meta flag missing from that list silently reads as its default, so the
    dashboard disagrees with ACCOUNT_META and nothing fails. That is exactly how
    `glance` shipped with no card on the live page."""
    import inspect
    import re as _re
    wanted = set(_re.findall(r'a[.]get[(]"(\w+)"', inspect.getsource(a._ui_accounts)))
    src = inspect.getsource(a)
    rec = src[src.index("def _account_meta_fields"):]
    rec = rec[:rec.index(chr(10) + "ALPACA_ACCOUNTS")]
    copied = set(_re.findall(r'"(\w+)":', rec))
    # `paper` is read off the record too but comes from ALPACA_PAPER{N}, not
    # from meta, so it is not the builder's to supply.
    missing = wanted - copied - {"paper"}
    assert not missing, (
        "_ui_accounts reads " + str(sorted(missing))
        + " off the record, but the record never copies it")

# ── the card ────────────────────────────────────────────────────────────────

def test_select_gets_a_card_without_carrying_a_profit_lock(monkeypatch):
    """The whole point of the `glance` flag. If this ever needs profit_lock set to
    get its card, the account has stopped mirroring the farm."""
    _accounts(monkeypatch)
    byname = {x["label"]: x for x in a._ui_accounts()}
    assert byname["Kairos Select"]["curated"] is True
    assert a.ACCOUNT_META[SELECT].get("profit_lock") is False


def test_the_farms_still_have_no_card(monkeypatch):
    """`glance` must not become "every account" -- the row is a short list."""
    _accounts(monkeypatch)
    byname = {x["label"]: x for x in a._ui_accounts()}
    assert byname["Kairos Farm"]["curated"] is False
    assert byname["TV Farm"]["curated"] is False


def test_every_other_book_keeps_the_card_it_had(monkeypatch):
    """`glance` defaults to the old profit_lock predicate, so adding it changed
    nothing for the accounts that already had cards."""
    _accounts(monkeypatch)
    for acct in a._ui_accounts():
        meta = a.ACCOUNT_META[acct["num"]]
        if "glance" not in meta:
            assert acct["curated"] == bool(meta.get("profit_lock", True)), acct["label"]


def test_the_card_renders_in_ui_order(monkeypatch):
    _accounts(monkeypatch)
    monkeypatch.setattr(a, "_wired_roster", lambda t: [])
    a.app.config["TESTING"] = True
    html = a.app.test_client().get("/").get_data(as_text=True)
    assert re.findall(r'id="glance(\d+)"', html) == ["6", "4", "7", "3", "2"]


def test_the_heading_describes_the_row_it_labels(monkeypatch):
    """It read "Curated Accounts" while listing a deliberately ungated book."""
    _accounts(monkeypatch)
    monkeypatch.setattr(a, "_wired_roster", lambda t: [])
    a.app.config["TESTING"] = True
    html = a.app.test_client().get("/").get_data(as_text=True)
    assert "Curated Accounts" not in html


# ── the badge ───────────────────────────────────────────────────────────────

pytestmark_node = pytest.mark.skipif(shutil.which("node") is None,
                                     reason="node not on PATH")


def _rank_emoji(tmp_path, roster, strategy):
    """Run the real _rankEmoji against a stubbed roster."""
    a.app.config["TESTING"] = True
    html = a.app.test_client().get("/").get_data(as_text=True)
    js = "\n".join(re.findall(r"<script>(.*?)</script>", html, re.S))
    i = js.index("function _rankEmoji(")
    depth, started, end = 0, False, None
    for j in range(i, len(js)):
        if js[j] == "{":
            depth += 1; started = True
        elif js[j] == "}":
            depth -= 1
            if started and depth == 0:
                end = j + 1; break
    # Built with chr(10) joins so there are no escape sequences to mangle.
    src = chr(10).join([
        "const _ROSTERS = " + json.dumps(
            {"select": {"label": "Kairos Select", "strategies": roster}}) + ";",
        "const _crewStrategies = new Set();",
        "const _refinedRank = {};",
        js[js.index("const _ROSTER_ICON"):end],
        "console.log(_rankEmoji(" + json.dumps(strategy) + "));",
    ])
    p = tmp_path / "badge.js"
    p.write_text(src, encoding="utf-8")
    # Decode as UTF-8 explicitly: the default is the console codepage, which
    # mangles the emoji the badge is made of.
    r = subprocess.run(["node", str(p)], capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=60)
    assert r.returncode == 0, r.stderr.strip()
    return r.stdout.strip()


@pytestmark_node
def test_a_roster_member_is_badged(tmp_path):
    out = _rank_emoji(tmp_path, ["AAPL_X"], "AAPL_X")
    assert "\U0001F3AF" in out
    assert "Kairos Select roster" in out


@pytestmark_node
def test_a_non_member_is_not_badged(tmp_path):
    assert _rank_emoji(tmp_path, ["AAPL_X"], "TSLA_Y") == ""


@pytestmark_node
def test_a_blank_strategy_is_not_badged_as_a_top_five_name(tmp_path):
    """Open positions can arrive with no strategy attached, and the rank lookup
    read `stratU && _refinedRank[stratU]`, which is '' for a blank name. '' <= 5
    coerces to 0 <= 5, so an UNATTRIBUTED position rendered as a Refined top-5
    GOAT -- a confident claim about a position nothing is known about."""
    assert _rank_emoji(tmp_path, ["AAPL_X"], "") == ""


@pytestmark_node
def test_an_unknown_strategy_is_not_badged(tmp_path):
    """undefined <= 5 is already false, but pin it so the guard above cannot be
    "simplified" back into the coercion bug."""
    assert _rank_emoji(tmp_path, ["AAPL_X"], "NEVER_SEEN_BEFORE") == ""


def test_select_positions_sort_with_the_curated_books():
    """The open-positions sort map never learned about account 7, so a Select
    position fell to the default rank and sorted below the farms -- under a
    heading that groups curated books above them."""
    html = open("templates/index.html", encoding="utf-8").read()
    m = re.search(r"const _acctRank = \{([^}]*)\}", html)
    assert m, "_acctRank missing"
    ranks = dict(re.findall(r"(\w+):\s*(\d+)", m.group(1)))
    assert "alpaca7" in ranks, "account 7 falls to the default rank"
    assert int(ranks["alpaca7"]) < int(ranks["alpaca5"])
    assert int(ranks["alpaca7"]) < int(ranks["alpaca"])
