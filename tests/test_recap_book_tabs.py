"""The recap page can describe either crew book.

It was hardcoded to Crew Paper in four places — /api/recap, the gate-cost panel,
the day charts and the gate-docs modal — so there was no way to see the same
outline for Crew Live, which is the book trading real money.
"""
from __future__ import annotations

import os
import re

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as kairos

_ALL = [{"num": n, "tag": t, "label": l, "color": c, "paper": p, "curated": True}
        for n, t, l, c, p in [("1", "alpaca",  "TV Farm",    "#888",    True),
                              ("2", "alpaca2", "TV Refined", "#70a8f5", True),
                              ("4", "alpaca4", "Crew Paper", "#7FE098", True),
                              ("6", "alpaca6", "Crew Live",  "#E8A0BF", False)]]


@pytest.fixture
def page(monkeypatch):
    monkeypatch.setattr(kairos, "_ui_accounts", lambda: _ALL)
    kairos.app.config["TESTING"] = True
    return kairos.app.test_client().get("/recap").get_data(as_text=True)


def _html():
    return open("templates/recap.html", encoding="utf-8").read()


def test_both_crew_books_get_a_tab(page):
    assert 'data-acct="4"' in page and 'data-acct="6"' in page
    assert "Crew Live" in page


def test_only_crew_books_are_offered(page):
    """The page is the crew book's outline; a farm tab would be a different report."""
    assert 'id="bk_1"' not in page and 'id="bk_2"' not in page


def test_the_live_book_is_marked_as_real_money(page):
    assert 'title="real money"' in page


def test_tabs_come_from_the_configured_accounts(monkeypatch):
    """A deploy without ALPACA_KEY6 must not render a tab that fetches an account
    the server does not have."""
    monkeypatch.setattr(kairos, "_ui_accounts",
                        lambda: [a for a in _ALL if a["num"] == "4"])
    kairos.app.config["TESTING"] = True
    solo = kairos.app.test_client().get("/recap").get_data(as_text=True)
    assert 'data-acct="6"' not in solo


def test_a_lone_book_renders_no_tab_row(monkeypatch):
    """One tab is furniture, not a choice."""
    monkeypatch.setattr(kairos, "_ui_accounts",
                        lambda: [a for a in _ALL if a["num"] == "4"])
    kairos.app.config["TESTING"] = True
    solo = kairos.app.test_client().get("/recap").get_data(as_text=True)
    assert 'class="btn book-tab' not in solo


def test_no_panel_is_still_pinned_to_crew_paper():
    """Four fetches were hardcoded. One left behind would put two accounts' numbers
    on a single screen under one heading."""
    html = _html()
    assert "alpaca4" not in html
    assert "account=4" not in html


def test_every_account_bearing_fetch_follows_the_tab():
    html = _html()
    fetches = re.findall(r"/api/(recap|signals/gate_opportunity|gates/docs|strategy/day_charts)"
                         r"[^`']*", html)
    assert len(fetches) >= 4, f"expected the four account-scoped fetches, saw {fetches}"
    for name in ("recap", "signals/gate_opportunity", "gates/docs"):
        i = html.index("/api/" + name)
        assert "${acct" in html[i:i + 120], name


def test_switching_books_drops_the_cached_gate_docs():
    """Gate state is per account — _account_gate_overrides means the same gate can
    be ON for one book and OFF for the other. A cached table would describe the
    book you just left."""
    html = _html()
    i = html.index("function setBook")
    assert "GATE_DOCS = null" in html[i:i + 500]


def test_the_choice_survives_a_reload():
    html = _html()
    assert "localStorage.getItem('recap_acct')" in html
    assert "localStorage.setItem('recap_acct'" in html


def test_a_stored_book_that_is_gone_falls_back():
    """Someone who last viewed Crew Live before ALPACA_KEY6 was removed would
    otherwise keep fetching an account that no longer exists."""
    html = _html()
    i = html.index("function _syncBookTabs")
    block = html[i:i + 600]
    assert "some(b => b.dataset.acct === acct)" in block
    assert "tabs[0].dataset.acct" in block


def test_the_page_syncs_tabs_before_its_first_load():
    html = _html()
    i = html.rindex("load();")
    assert "_syncBookTabs()" in html[i - 200:i]
