"""Per-account UI surfaces are registry-driven.

Adding an account used to mean hand-editing tab buttons, feed buttons, switch
branches and label maps across six templates — docs/adding_a_paper_account.md
listed the sites precisely because nothing enforced them. Crew Live is the first
account added since, and these pin that it appears everywhere from one registry.

The load-bearing case is the REAL-MONEY one: a book trading real money must never
be indistinguishable from a paper book in the UI, and a book the server has no
keys for must never render a tab that fetches nothing.
"""
from __future__ import annotations

import os
import re

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a

_ALL = [("1", "alpaca",  "TV Farm",        True),
        ("2", "alpaca2", "TV Refined",     True),
        ("3", "alpaca3", "Kairos Refined", True),
        ("4", "alpaca4", "Crew Paper",     True),
        ("5", "alpaca5", "Kairos Farm",    True),
        ("6", "alpaca6", "Crew Live",      False)]

# Every page that renders a per-account picker, with the markup that PROVES the
# live book is actually selectable there. Matching the label text alone is not
# enough: "Crew Live" also appears in CSS comments and in the dashboard's static
# label table, both of which are present even when the account is unconfigured.
_PAGES = {
    "/":            'id="tab-live"',
    "/analysis":    'id="src-alpaca6"',
    "/diagnostics": '<option value="alpaca6">',
    "/review":      '<option value="6"',
    "/simulate":    "Crew Live",
}
# /routing is deliberately absent. Its broker dropdown lists every POSSIBLE target,
# not the configured ones — a rule may reference a broker this deploy has no keys
# for, and the router still has to display and edit it.


def _registry(monkeypatch, rows):
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS",
                        [{"num": n, "tag": t, "label": l, "color": "#888", "paper": p}
                         for n, t, l, p in rows])


@pytest.fixture
def all_books(monkeypatch):
    _registry(monkeypatch, _ALL)
    a.app.config["TESTING"] = True
    return a.app.test_client()


@pytest.fixture
def no_live(monkeypatch):
    """The same deploy WITHOUT ALPACA_KEY6 — acct6 never enters the registry."""
    _registry(monkeypatch, [r for r in _ALL if r[0] != "6"])
    a.app.config["TESTING"] = True
    return a.app.test_client()


@pytest.mark.parametrize("path,marker", sorted(_PAGES.items()))
def test_every_account_page_renders_crew_live(all_books, path, marker):
    html = all_books.get(path).get_data(as_text=True)
    assert marker in html, f"{path} does not surface the live book"


@pytest.mark.parametrize("path,marker", sorted(_PAGES.items()))
def test_no_page_invents_an_account_the_server_lacks(no_live, path, marker):
    """A control for an unconfigured book would fetch an account the server cannot
    reach, and on the dashboard it would be the DEFAULT tab — an empty page on
    every deploy that forgot the key."""
    r = no_live.get(path)
    assert r.status_code == 200
    assert marker not in r.get_data(as_text=True), f"{path} rendered an absent book"


def test_the_dashboard_defaults_to_the_live_book(all_books):
    html = all_books.get("/").get_data(as_text=True)
    assert re.search(r"activeTab = \(window\.__ACCT6_ON__ \? 'live' : 'crew'\)", html)
    assert '"live"' in re.search(r"_CONFIGURED = new Set\((\[[^\]]*\])\)", html).group(1)


def test_the_dashboard_falls_back_when_the_live_book_is_absent(no_live):
    """__ACCT6_ON__ is false, so the same expression selects Crew Paper."""
    html = no_live.get("/").get_data(as_text=True)
    assert '"live"' not in re.search(r"_CONFIGURED = new Set\((\[[^\]]*\])\)", html).group(1)
    assert 'id="tab-live"' not in html


def test_analysis_defaults_to_the_live_book(all_books):
    html = all_books.get("/analysis").get_data(as_text=True)
    assert "_CONFIGURED_SRC.has('alpaca6') ? 'alpaca6' : 'alpaca4'" in html


def test_the_dashboard_feed_tab_tracks_the_pnl_tab(all_books):
    """Chart tab and feed tab are kept in sync, so the feed needs its own live
    button or switching to Crew Live would leave the feed on another book."""
    assert 'id="feedTab-live"' in all_books.get("/").get_data(as_text=True)


def test_the_live_book_is_visually_marked_as_real_money(all_books):
    """A pink tab is decoration; the ● and the title are the actual signal. Losing
    them would leave a real-money book looking exactly like the five paper ones."""
    html = all_books.get("/").get_data(as_text=True)
    assert 'title="REAL MONEY"' in html
    assert re.search(r'id="tab-live"[^>]*live-tab', html), "live tab keeps its tint when inactive"
    assert "● Crew Live" in html


def test_tab_order_puts_the_live_book_first(all_books):
    html = all_books.get("/").get_data(as_text=True)
    assert re.findall(r'id="tab-([a-z]+)"', html)[0] == "live"


def test_the_router_can_target_the_live_broker(all_books):
    """The crew wire writes alpaca-live-6 rules. Without the <option> the router
    could not display or edit a rule it had itself created."""
    html = all_books.get("/routing").get_data(as_text=True)
    assert 'value="alpaca-live-6"' in html
    assert "REAL MONEY" in html
    assert ".node-broker-alpaca-live-6" in html


def test_the_live_broker_chip_is_the_only_red_one(all_books):
    """Colour is how you spot a real-money rule in a list of ~100 pipelines."""
    html = all_books.get("/routing").get_data(as_text=True)
    live = re.search(r"\.node-broker-alpaca-live-6\s*\{([^}]*)\}", html).group(1)
    assert "#ef5350" in live
    for other in ("alpaca-paper-2", "alpaca-paper-3", "alpaca-paper-4", "alpaca-paper-5"):
        block = re.search(r"\.node-broker-" + other + r"\s*\{([^}]*)\}", html)
        if block:
            assert "#ef5350" not in block.group(1), f"{other} must not look like real money"


def test_chart_review_accepts_any_configured_book(monkeypatch):
    """The review endpoint used to hard-code ?account=2|3, which was the only thing
    keeping Crew Paper and Crew Live off the chart-review page."""
    import inspect
    src = inspect.getsource(a.api_review)
    assert 'account not in ("2", "3")' not in src
    assert "account not in ACCOUNTS_BY_NUM" in src


def test_tab_keys_cover_every_declared_account():
    """A slot with no tab key would render a button switchTab cannot handle."""
    for num in a.ACCOUNT_META:
        assert num in a._TAB_KEY_BY_NUM, f"slot {num} has no UI tab key"


def test_ui_accounts_is_ordered_and_flags_real_money(monkeypatch):
    _registry(monkeypatch, _ALL)
    accts = a._ui_accounts()
    assert [x["num"] for x in accts] == ["6", "4", "3", "2", "5", "1"]
    assert accts[0]["tab"] == "live" and accts[0]["paper"] is False
    assert all(x["paper"] for x in accts[1:]), "only acct6 trades real money"
