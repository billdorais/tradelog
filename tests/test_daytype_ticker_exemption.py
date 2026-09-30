"""Per-ticker exemption from the day-type gate.

The gate reads the MARKET's day: breakouts only pay on "Outside" days, so they are
blocked on Inside/Neutral ones. That premise does not hold uniformly — a name with
its own liquidity or catalyst can expand on a day the classification calls Inside.

Same shape as the hours exemption: stored per account, inherited by Crew Live
through _GATE_MIRROR, with a DAYTYPE_EXEMPT_<TAG> env fallback.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a

DAY = "2026-09-30"


@pytest.fixture
def gated(monkeypatch):
    """Day-type gate on for the Crew books; today classifies as Inside."""
    monkeypatch.setattr(a, "DAYTYPE_GATE_ENABLED", True)
    monkeypatch.setattr(a, "DAYTYPE_GATE_ACCOUNTS", {"alpaca4", "alpaca6", "alpaca2"})
    monkeypatch.setattr(a, "_get_day_classification", lambda tk, d: {"day_type": "Inside"})
    monkeypatch.setattr(a, "_gates_acct_cache", {})
    monkeypatch.setattr(a, "_gates_acct_ts", float("inf"))
    monkeypatch.delenv("DAYTYPE_EXEMPT_ALPACA4", raising=False)


def _exempt(monkeypatch, tickers, tag="alpaca4", extra=None):
    dt = {"exempt_tickers": tickers}
    dt.update(extra or {})
    monkeypatch.setattr(a, "_gates_acct_cache", {tag: {"daytype": dt}})
    monkeypatch.setattr(a, "_gates_acct_ts", float("inf"))


def _blocked(tag, ticker, kind="BREAKOUT"):
    return a._daytype_gate_block(f"{ticker}_CAM_{kind}_R4S4", ticker, DAY, tag)[0]


# ── the gate still governs everything else ──────────────────────────────────

def test_without_an_exemption_an_inside_day_blocks_breakouts(gated):
    assert _blocked("alpaca4", "NVDA") is True


def test_an_exempt_ticker_trades_on_a_blocked_day_type(monkeypatch, gated):
    _exempt(monkeypatch, ["AAPL"])
    assert _blocked("alpaca4", "AAPL") is False


def test_every_other_ticker_still_obeys_the_gate(monkeypatch, gated):
    """The carve-out must be surgical — this is the assertion the bug below broke."""
    _exempt(monkeypatch, ["AAPL"])
    assert _blocked("alpaca4", "NVDA") is True


def test_reversals_are_untouched(monkeypatch, gated):
    """Breakout and reversal day-type gates are separate; an exemption written for
    one must not quietly apply to the other's premise."""
    _exempt(monkeypatch, ["AAPL"])
    monkeypatch.setattr(a, "DAYTYPE_REVERSAL_GATE_ENABLED", False)
    assert _blocked("alpaca4", "NVDA", kind="REVERSAL") is False


# ── the bug this feature introduced, and its fix ────────────────────────────

def test_an_exemption_alone_does_not_switch_the_gate_off(monkeypatch, gated):
    """The override branch keyed on the daytype override merely EXISTING, so storing
    {exempt_tickers: [...]} with no `enabled` read as "this book controls the gate
    and has it off" — silently disabling day-type filtering for the entire book the
    moment one ticker was exempted. It must fall through to the shared setting."""
    _exempt(monkeypatch, ["AAPL"])
    assert _blocked("alpaca4", "NVDA") is True, "the shared gate must still apply"
    assert _blocked("alpaca4", "AAPL") is False


def test_an_explicit_off_still_disables_the_book(monkeypatch, gated):
    """The real override must keep working — the fix narrows it, not removes it."""
    _exempt(monkeypatch, [], extra={"enabled": False})
    assert _blocked("alpaca4", "NVDA") is False


def test_an_explicit_on_overrides_a_shared_off(monkeypatch, gated):
    monkeypatch.setattr(a, "DAYTYPE_GATE_ENABLED", False)
    _exempt(monkeypatch, [], extra={"enabled": True})
    assert _blocked("alpaca4", "NVDA") is True


def test_an_exemption_and_an_explicit_on_coexist(monkeypatch, gated):
    _exempt(monkeypatch, ["AAPL"], extra={"enabled": True, "breakout_ok_days": ["Outside"]})
    assert _blocked("alpaca4", "AAPL") is False
    assert _blocked("alpaca4", "NVDA") is True


# ── scope ───────────────────────────────────────────────────────────────────

def test_the_exemption_is_per_book(monkeypatch, gated):
    _exempt(monkeypatch, ["AAPL"], tag="alpaca4")
    assert _blocked("alpaca4", "AAPL") is False
    assert _blocked("alpaca2", "AAPL") is True, "another book must not inherit it"


def test_crew_live_inherits_it(monkeypatch, gated):
    """Same roster on both books — a ticker that trades every day type on paper has
    to do the same live, or the comparison measures the config gap."""
    _exempt(monkeypatch, ["AAPL"], tag="alpaca4")
    assert _blocked("alpaca6", "AAPL") is False
    assert _blocked("alpaca6", "NVDA") is True


def test_matching_is_case_insensitive(monkeypatch, gated):
    _exempt(monkeypatch, [" aapl "])
    assert _blocked("alpaca4", "AAPL") is False


def test_an_env_override_works_without_a_settings_row(monkeypatch, gated):
    monkeypatch.setenv("DAYTYPE_EXEMPT_ALPACA4", "TSLA,AAPL")
    assert _blocked("alpaca4", "TSLA") is False
    assert _blocked("alpaca4", "AMD") is True


def test_no_exemption_configured_is_the_empty_set(gated):
    assert a._daytype_exempt_tickers("alpaca4") == set()


# ── it survives the settings round-trip ─────────────────────────────────────

def test_saving_an_exemption_while_inheriting_the_on_off_decision(monkeypatch, gated):
    """The common case: keep the shared gate, exempt one ticker. Setting the
    dropdown to "inherit" must not wipe the exemption stored beside it."""
    store = {}
    monkeypatch.setattr(a, "_load_setting", lambda k: store.get(k))
    monkeypatch.setattr(a, "_save_setting", lambda k, v: store.__setitem__(k, v))
    monkeypatch.setattr(a, "_update_env_file", lambda *x: None, raising=False)
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {"4": {"tag": "alpaca4", "label": "Crew Paper"}})
    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {"alpaca4": {"tag": "alpaca4", "label": "Crew Paper"}})
    a.app.config["TESTING"] = True

    d = a.app.test_client().post("/api/routing/account_gates", json={
        "account": "4", "daytype": "inherit", "daytype_exempt": "aapl, tsla"}).get_json()
    assert d["overrides"]["daytype"] == {"exempt_tickers": ["AAPL", "TSLA"]}
    assert "enabled" not in d["overrides"]["daytype"], "inherit must not pin on/off"


def test_clearing_the_field_removes_the_exemption(monkeypatch, gated):
    store = {}
    monkeypatch.setattr(a, "_load_setting", lambda k: store.get(k))
    monkeypatch.setattr(a, "_save_setting", lambda k, v: store.__setitem__(k, v))
    monkeypatch.setattr(a, "_update_env_file", lambda *x: None, raising=False)
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {"4": {"tag": "alpaca4", "label": "Crew Paper"}})
    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {"alpaca4": {"tag": "alpaca4", "label": "Crew Paper"}})
    a.app.config["TESTING"] = True
    c = a.app.test_client()
    c.post("/api/routing/account_gates",
           json={"account": "4", "daytype": "inherit", "daytype_exempt": "AAPL"})
    d = c.post("/api/routing/account_gates",
               json={"account": "4", "daytype": "inherit", "daytype_exempt": ""}).get_json()
    assert "daytype" not in d["overrides"]


# ── surfaced, not buried ────────────────────────────────────────────────────

def test_the_gate_reference_shows_the_exemption(monkeypatch, gated):
    _exempt(monkeypatch, ["AAPL"])
    monkeypatch.setattr(a, "_manual_halted_for", lambda tag: False)
    assert "AAPL exempt (every day)" in a._gate_docs("alpaca4")["day-type"]["setting"]


def test_the_rule_text_mentions_the_escape_hatch():
    assert "exempted" in " ".join(a._GATE_RULES["day-type"]["rule"]).lower()


def test_the_gated_book_list_comes_from_the_registry(monkeypatch):
    """The UI hardcoded "TV Farm, TV Refined, Kairos Refined, Kairos Farm" — naming
    both farms, which are NOT day-type gated, and omitting both Crew books, which
    are. A label that can rot is worse than no label."""
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [])
    a.app.config["TESTING"] = True
    books = a.app.test_client().get("/api/risk/status").get_json()["gated_books"]["daytype"]
    assert "TV Farm" not in books and "Kairos Farm" not in books
    assert "Crew Paper" in books and "Crew Live" in books


# ── the dropdown must survive a save ────────────────────────────────────────

def test_the_dropdown_reads_the_enabled_key_not_the_overrides_existence():
    """Setting "Inherit (shared)" flipped back to "Off" on the next 5s poll.

    Saving inherit-with-an-exemption stores {exempt_tickers: [...]} — correctly, no
    on/off decision in it. But the UI tested the OBJECT, and an object with no
    `enabled` read as false, so it rendered "off". Worse than cosmetic: the next
    Save then wrote that off for real, turning the whole book's day-type gate off.

    Same conflation as the gate-logic bug, in the half of the round trip that was
    not fixed with it.
    """
    src = open("templates/routing.html", encoding="utf-8").read()
    assert "cg.daytype ? (cg.daytype.enabled" not in src, "the truthiness test is back"
    assert "'enabled' in o" in src
    i = src.index("const _sel = o =>")
    block = src[i:i + 400]
    assert "_cgSet('crewDaytypeSel', _sel(cg.daytype))" in src
    assert "_cgSet('crewRvolSel',    _sel(cg.rvol))" in src, "same fragility, same fix"


@pytest.mark.parametrize("choice,exempt,stored_keys", [
    ("off",     "AAPL", {"enabled", "exempt_tickers"}),
    ("inherit", "AAPL", {"exempt_tickers"}),
    ("on",      "AAPL", {"enabled", "exempt_tickers"}),
])
def test_what_each_choice_stores(monkeypatch, gated, choice, exempt, stored_keys):
    """Pins the shape the dropdown has to read back. inherit stores NO `enabled` —
    that is the whole point, and what made the UI misread it."""
    store = {}
    monkeypatch.setattr(a, "_load_setting", lambda k: store.get(k))
    monkeypatch.setattr(a, "_save_setting", lambda k, v: store.__setitem__(k, v))
    monkeypatch.setattr(a, "_update_env_file", lambda *x: None, raising=False)
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {"4": {"tag": "alpaca4", "label": "Crew Paper"}})
    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {"alpaca4": {"tag": "alpaca4", "label": "Crew Paper"}})
    a.app.config["TESTING"] = True
    d = a.app.test_client().post("/api/routing/account_gates", json={
        "account": "4", "daytype": choice, "daytype_exempt": exempt}).get_json()
    assert set(d["overrides"]["daytype"]) == stored_keys


def test_inherit_with_an_exemption_leaves_the_shared_gate_in_charge(monkeypatch, gated):
    """The end state the user actually wanted: Crew keeps obeying the shared gate,
    AAPL alone ignores it."""
    _exempt(monkeypatch, ["AAPL"])                      # no `enabled` — i.e. inherit
    assert _blocked("alpaca4", "AAPL") is False
    assert _blocked("alpaca4", "NVDA") is True
    assert _blocked("alpaca6", "NVDA") is True
