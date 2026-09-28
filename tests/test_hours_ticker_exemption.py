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
