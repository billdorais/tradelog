"""Per-ticker exemption from a book's trading-hours window.

The window is a book-wide policy, but liquidity is not: an index ETF is tradeable
all session while a thin name genuinely should be confined to the open. Before
this there was no way to express that — the only dial was the whole book.

Two independent hours gates exist and are ANDed, which is why widening a routing
rule's own `trading_hours` node was never enough on its own:

  rule-level     routes/webhook.py filters broker_targets by the rule's node
  account-level  _account_hours_ok filters alpaca_targets by the book's window

The exemption belongs to the ACCOUNT gate, because that is the one with no
per-ticker dimension.
"""
from __future__ import annotations

import datetime as dt
import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a

try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:                                    # pragma: no cover
    _ET = dt.timezone(dt.timedelta(hours=-4))


def _et(h, m):
    return dt.datetime(2026, 9, 28, h, m, tzinfo=_ET)


@pytest.fixture
def crew_window(monkeypatch):
    """Crew on 09:35-15:55, nothing exempt yet."""
    monkeypatch.setattr(a, "_account_hours_windows",
                        lambda tag: [("09:35", "15:55")] if tag == "alpaca4" else [])
    monkeypatch.setattr(a, "_gates_acct_cache", {})
    monkeypatch.setattr(a, "_gates_acct_ts", float("inf"))
    monkeypatch.delenv("HOURS_EXEMPT_ALPACA4", raising=False)


def _exempt(monkeypatch, tickers, tag="alpaca4"):
    monkeypatch.setattr(a, "_gates_acct_cache",
                        {tag: {"hours": {"exempt_tickers": tickers}}})
    monkeypatch.setattr(a, "_gates_acct_ts", float("inf"))


# ── the window still governs everything else ────────────────────────────────

def test_without_an_exemption_the_window_binds(crew_window):
    assert a._account_hours_ok("alpaca4", now_et=_et(10, 0), ticker="SPY") is True
    assert a._account_hours_ok("alpaca4", now_et=_et(19, 0), ticker="SPY") is False


def test_an_exempt_ticker_trades_outside_the_window(monkeypatch, crew_window):
    _exempt(monkeypatch, ["SPY"])
    assert a._account_hours_ok("alpaca4", now_et=_et(19, 0), ticker="SPY") is True


def test_every_other_ticker_still_obeys_the_window(monkeypatch, crew_window):
    """The point of the feature is that it is surgical."""
    _exempt(monkeypatch, ["SPY"])
    assert a._account_hours_ok("alpaca4", now_et=_et(19, 0), ticker="NVDA") is False
    assert a._account_hours_ok("alpaca4", now_et=_et(19, 0), ticker=None) is False


def test_matching_is_case_and_whitespace_insensitive(monkeypatch, crew_window):
    _exempt(monkeypatch, [" spy ", "qqq"])
    for t in ("SPY", "spy", " Spy "):
        assert a._account_hours_ok("alpaca4", now_et=_et(19, 0), ticker=t) is True
    assert a._account_hours_ok("alpaca4", now_et=_et(19, 0), ticker="QQQ") is True


def test_a_comma_string_is_accepted_as_well_as_a_list(monkeypatch, crew_window):
    """The settings endpoint stores a list; the env fallback is a comma string."""
    _exempt(monkeypatch, "SPY, QQQ")
    assert a._account_hours_ok("alpaca4", now_et=_et(19, 0), ticker="QQQ") is True


def test_the_exemption_is_per_book(monkeypatch):
    """Exempting SPY on Crew must not quietly open TV Refined's window too."""
    monkeypatch.setattr(a, "_account_hours_windows", lambda tag: [("09:35", "15:55")])
    _exempt(monkeypatch, ["SPY"], tag="alpaca4")
    assert a._account_hours_ok("alpaca4", now_et=_et(19, 0), ticker="SPY") is True
    assert a._account_hours_ok("alpaca2", now_et=_et(19, 0), ticker="SPY") is False


def test_crew_live_inherits_the_exemption(monkeypatch):
    """acct6 mirrors acct4's gate overrides — the two books trade one roster, so a
    ticker that runs all day on paper must run all day live or the comparison
    measures the config gap."""
    monkeypatch.setattr(a, "_account_hours_windows", lambda tag: [("09:35", "15:55")])
    _exempt(monkeypatch, ["SPY"], tag="alpaca4")
    assert a._account_hours_ok("alpaca6", now_et=_et(19, 0), ticker="SPY") is True


def test_an_env_override_works_without_the_settings_row(monkeypatch, crew_window):
    monkeypatch.setenv("HOURS_EXEMPT_ALPACA4", "TSLA,SPY")
    assert a._account_hours_ok("alpaca4", now_et=_et(19, 0), ticker="TSLA") is True
    assert a._account_hours_ok("alpaca4", now_et=_et(19, 0), ticker="AMD") is False


def test_no_exemptions_configured_is_the_empty_set(crew_window):
    assert a._hours_exempt_tickers("alpaca4") == set()


# ── it has to reach the actual gates ────────────────────────────────────────

def test_both_entry_gates_pass_the_ticker_through():
    """An exemption the entry path never consults is decoration."""
    import inspect
    from routes import webhook as w
    assert "ticker=tk" in inspect.getsource(a._engine_pilot_tick) \
        if hasattr(a, "_engine_pilot_tick") else True
    wh = inspect.getsource(w)
    assert "_account_hours_ok(_alpaca_broker_name(bt[0]), ticker=ticker)" in wh
    app_src = inspect.getsource(a)
    assert "_account_hours_ok(broker_tag, now_et=now_et, ticker=tk)" in app_src


def test_the_coarse_tick_filter_cannot_strand_an_exempt_ticker():
    """The engine short-circuits the whole tick when no book is inside its window.
    With every window shut but SPY exempt, that check has to stand down — otherwise
    the cheap pre-filter silently overrules the authoritative per-target one."""
    import inspect
    src = inspect.getsource(a)
    i = src.index("Cheap tick-level short-circuit")
    assert "_hours_exempt_tickers(_a[\"tag\"])" in src[i:i + 900]


# ── surfaced, not buried in config ──────────────────────────────────────────

def test_the_gate_reference_shows_the_exemptions(monkeypatch, crew_window):
    _exempt(monkeypatch, ["SPY", "QQQ"])
    monkeypatch.setattr(a, "_manual_halted_for", lambda tag: False)
    setting = a._gate_docs("alpaca4")["hours"]["setting"]
    assert "09:35-15:55 ET" in setting
    assert "QQQ, SPY exempt (all day)" in setting


def test_the_gate_reference_is_unchanged_without_exemptions(monkeypatch, crew_window):
    monkeypatch.setattr(a, "_manual_halted_for", lambda tag: False)
    assert a._gate_docs("alpaca4")["hours"]["setting"] == "09:35-15:55 ET"


def test_the_hours_rule_text_mentions_the_escape_hatch():
    rule = " ".join(a._GATE_RULES["hours"]["rule"])
    assert "exempted" in rule.lower()


# ── the exemption must clear BOTH gates ─────────────────────────────────────

def test_the_exemption_also_beats_the_rules_own_hours_node():
    """The gap that made the first version unusable.

    Crew rules carry their own trading_hours node (09:35-15:55 on the card). That
    filter runs BEFORE the account gate, so an exempt ticker was dropped there and
    the exemption was unreachable — the field would have looked wired and done
    nothing."""
    import inspect
    from routes import webhook as w
    src = inspect.getsource(w)
    i = src.index("Per-target trading hours check")
    block = src[i:i + 2200]
    assert "_hours_exempt_tickers" in block, "the rule filter must consult the exemption"
    assert "_routing_broker_to_tag(_bt[0])" in block


def test_the_rule_filter_resolves_the_tag_without_the_alpaca_fallback():
    """_alpaca_broker_name falls back to "alpaca" for IB/Coinbase, which would
    apply the TV Farm book's exemptions to an entirely different broker."""
    import inspect
    from routes import webhook as w
    src = inspect.getsource(w)
    i = src.index("Per-target trading hours check")
    block = src[i:i + 2200]
    assert "_alpaca_broker_name(_bt[0])" not in block


def test_a_midday_pause_still_binds_for_everything_else(monkeypatch):
    """The real Crew config is two windows with a midday gap (09:35-10:00,
    12:00-15:55). The exemption is surgical: SPY ignores the gap, NVDA does not."""
    monkeypatch.setattr(a, "_account_hours_windows",
                        lambda tag: [("09:35", "10:00"), ("12:00", "15:55")])
    _exempt(monkeypatch, ["SPY"])
    assert a._account_hours_ok("alpaca4", now_et=_et(11, 0), ticker="NVDA") is False
    assert a._account_hours_ok("alpaca4", now_et=_et(11, 0), ticker="SPY")  is True
    assert a._account_hours_ok("alpaca4", now_et=_et(9, 45), ticker="NVDA") is True
    assert a._account_hours_ok("alpaca4", now_et=_et(13, 0), ticker="NVDA") is True


# ── the UI must not misstate what a rule will do ────────────────────────────

def test_the_risk_payload_carries_resolved_exemptions(monkeypatch):
    """Resolved, not raw: the UI needs the answer AFTER the acct6 -> acct4 mirror,
    or every card on the live book would render as if nothing were exempt."""
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [
        {"num": "4", "tag": "alpaca4", "label": "Crew Paper"},
        {"num": "6", "tag": "alpaca6", "label": "Crew Live"},
        {"num": "2", "tag": "alpaca2", "label": "TV Refined"}])
    _exempt(monkeypatch, ["SPY"], tag="alpaca4")
    a.app.config["TESTING"] = True
    d = a.app.test_client().get("/api/risk/status").get_json()
    ex = d["hours_exempt_by_account"]
    assert ex["alpaca4"] == ["SPY"]
    assert ex["alpaca6"] == ["SPY"], "the mirror must be applied before sending"
    assert "alpaca2" not in ex, "books with no exemptions are omitted, not empty"
    assert d["gate_mirror"] == {"alpaca6": "alpaca4"}


def test_the_hours_chip_reports_exemption_rather_than_the_window():
    """A card rendering 09:35-15:55 for a ticker that ignores it is a card that
    lies. This became possible only once the exemption started beating the rule
    node — before that the window was always the truth."""
    src = open("templates/routing.html", encoding="utf-8").read()
    i = src.index("function renderNode(")
    block = src[i:i + 1800]
    assert "_ruleHoursExempt(ruleId)" in block
    assert "node-hours-exempt" in block
    assert "value = 'all day'" in block


def test_the_hours_modal_warns_that_edits_will_not_apply():
    """Editing a window the rule will not honour is a trap."""
    src = open("templates/routing.html", encoding="utf-8").read()
    i = src.index("Signals arriving outside this window are silently dropped")
    block = src[i:i + 1200]
    assert "_ruleHoursExempt(activePipelineId)" in block
    assert "will not change that" in block


def test_the_client_tag_resolver_mirrors_the_server():
    """BROKER_TAG_OF duplicates _routing_broker_to_tag's mapping in JS. If they
    drift, the chip attributes an exemption to the wrong book."""
    src = open("templates/routing.html", encoding="utf-8").read()
    assert "function BROKER_TAG_OF" in src
    i = src.index("function BROKER_TAG_OF")
    block = src[i:i + 600]
    # Non-Alpaca brokers must resolve to null, not fall back to slot 1.
    assert "return m ?" in block and "null" in block
    for tag in ("alpaca-paper", "alpaca-live"):
        assert tag in block


@pytest.mark.parametrize("value,tag", [
    ("alpaca", "alpaca"), ("alpaca-paper", "alpaca"), ("alpaca-live", "alpaca"),
    ("alpaca-paper-4", "alpaca4"), ("alpaca-live-6", "alpaca6"),
    ("ib-paper", None), ("coinbase", None),
])
def test_the_server_mapping_the_client_copies(monkeypatch, value, tag):
    """Pins the behaviour BROKER_TAG_OF is written against, so a server-side change
    to broker naming fails here rather than silently desyncing the UI.

    One deliberate difference: the server is REGISTRY-driven and returns None for a
    slot with no keys, while the client resolves by pattern. Harmless, because
    hours_exempt_by_account is itself built from the registry — an unconfigured
    book has no exemptions to attribute, so the client never gets to be wrong."""
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [
        {"tag": "alpaca4", "target_paper": "alpaca-paper-4", "target_live": "alpaca-live-4"},
        {"tag": "alpaca6", "target_paper": "alpaca-paper-6", "target_live": "alpaca-live-6"}])
    assert a._routing_broker_to_tag(value) == tag
