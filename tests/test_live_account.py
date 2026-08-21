"""Crew Live (acct6) — the first real-money book.

Every Alpaca account before this one was paper, so `paper` sat in the registry
and was never consulted: nothing between a signal and submit_order asked whether
real money was on the other end. These tests pin the guard that now does.

The guard's defining property is that it FAILS CLOSED. Every other gate in this
system fails open on purpose — they protect an edge, and a data glitch costing
one trade beats a gate that mutes the book. This one protects the balance, so an
unreadable equity figure has to mean "no", not "probably fine".
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


class _Acct:
    def __init__(self, equity, buying_power=None, shorting_enabled=True, **extra):
        self.equity = equity
        self.buying_power = equity if buying_power is None else buying_power
        self.shorting_enabled = shorting_enabled
        for k, v in extra.items():
            setattr(self, k, v)


class _Trading:
    def __init__(self, acct):
        self._acct = acct

    def get_account(self):
        if isinstance(self._acct, Exception):
            raise self._acct
        return self._acct


class _Broker:
    """Minimal stand-in. Pass an Exception to simulate a broker outage."""
    def __init__(self, equity=50_000.0, buying_power=None, shorting_enabled=True):
        self._trading = _Trading(
            equity if isinstance(equity, Exception)
            else _Acct(equity, buying_power, shorting_enabled))

    def account_equity(self):
        return float(self._trading.get_account().equity)


@pytest.fixture
def live(monkeypatch):
    """A configured, armed live book at $2,000/position on $20k equity."""
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca6",
                        {"tag": "alpaca6", "label": "Crew Live", "paper": False,
                         "broker": _Broker(20_000.0)})
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca4",
                        {"tag": "alpaca4", "label": "Crew Paper", "paper": True,
                         "broker": _Broker(6_000.0)})
    monkeypatch.setattr(a, "LIVE_TRADING_ARMED", True)
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 2_000.0)
    monkeypatch.setattr(a, "LIVE_MAX_POSITION_PCT", 20.0)
    monkeypatch.setattr(a, "LIVE_MAX_ENTRIES_PER_DAY", 0)
    a._live_entry_counts.clear()
    a._live_snap_cache.clear()      # the snapshot is cached; tests swap brokers freely


# ── the account itself ──────────────────────────────────────────────────────

def test_crew_live_mirrors_crew_papers_policy_exactly():
    """The two books exist to be compared. Any gate difference would make the
    comparison measure the config gap instead of slippage and fills."""
    paper, live = a.ACCOUNT_META["4"], a.ACCOUNT_META["6"]
    for k in ("daytype_gate", "reversal_gate", "retest", "auto_source",
              "profit_lock", "reversal_side", "daily_loss_guard",
              "open_loc_gate", "hours_key"):
        assert paper.get(k) == live.get(k), f"acct6 diverges from acct4 on {k}"


def test_crew_live_is_a_curated_book():
    """It carries a profit lock, so it must land in the curated set that the
    position-stop and gate-cost surfaces iterate."""
    assert "alpaca6" in a._CURATED_TAGS


def test_the_live_book_is_shown_first():
    """It is the only book with real money on it."""
    assert a.UI_ACCOUNT_ORDER[0] == "6"


# ── the guard ───────────────────────────────────────────────────────────────

def test_paper_books_are_untouched(live):
    """The guard must be a no-op on paper, including when live config is absent."""
    assert a._live_entry_allowed("alpaca4") == (True, "")


def test_a_live_book_is_inert_until_armed(monkeypatch, live):
    monkeypatch.setattr(a, "LIVE_TRADING_ARMED", False)
    ok, why = a._live_entry_allowed("alpaca6")
    assert ok is False and "not armed" in why


def test_an_unset_size_refuses_rather_than_guessing(monkeypatch, live):
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 0.0)
    ok, why = a._live_entry_allowed("alpaca6")
    assert ok is False and "LIVE_SIZE_DOLLARS" in why


def test_a_position_over_the_equity_cap_is_refused(monkeypatch, live):
    """$2k on $20k is 10% and fine; $5k is 25% and over the 20% ceiling."""
    assert a._live_entry_allowed("alpaca6", notional=2_000)[0] is True
    ok, why = a._live_entry_allowed("alpaca6", notional=5_000)
    assert ok is False and "exceeds 20%" in why


def test_the_cap_follows_equity_down(monkeypatch, live):
    """The size was chosen once; equity moves. A drawdown has to shrink the cap,
    which is the whole reason the check reads equity at order time."""
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"] = _Broker(8_000.0)
    ok, why = a._live_entry_allowed("alpaca6", notional=2_000)
    assert ok is False and "$8,000 equity" in why


def test_an_unreadable_balance_refuses_the_entry(live):
    """FAILS CLOSED — the property that separates this from every other gate."""
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"] = _Broker(RuntimeError("api down"))
    ok, why = a._live_entry_allowed("alpaca6", notional=100)
    assert ok is False
    assert "could not read the live account" in why and "api down" in why


def test_zero_or_negative_equity_refuses(live):
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"] = _Broker(0.0)
    assert a._live_entry_allowed("alpaca6", notional=1)[0] is False


def test_the_daily_entry_cap_is_a_runaway_guard(monkeypatch, live):
    """Not a PDT limit — the day-trade count stopped mattering on 2026-06-04. This
    exists so an engine fault that fires in a loop cannot empty the account."""
    monkeypatch.setattr(a, "LIVE_MAX_ENTRIES_PER_DAY", 3)
    for _ in range(3):
        assert a._live_entry_allowed("alpaca6", notional=100)[0] is True
        a._note_live_entry("alpaca6")
    ok, why = a._live_entry_allowed("alpaca6", notional=100)
    assert ok is False and "runaway guard" in why


def test_the_cap_does_not_count_paper_entries(monkeypatch, live):
    monkeypatch.setattr(a, "LIVE_MAX_ENTRIES_PER_DAY", 1)
    for _ in range(5):
        a._note_live_entry("alpaca4")
    assert a._live_entry_allowed("alpaca6", notional=100)[0] is True


def test_checks_run_before_any_network_call(monkeypatch, live):
    """A disarmed book must not reach the broker at all — both so it costs nothing
    and so a broker outage can never be the reason a size check was skipped."""
    monkeypatch.setattr(a, "LIVE_TRADING_ARMED", False)
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"] = _Broker(RuntimeError("must not be called"))
    ok, why = a._live_entry_allowed("alpaca6")
    assert ok is False and "not armed" in why      # not the broker error


def test_an_unknown_account_is_treated_as_paper():
    """Defensive: a tag with no registry record is not a live book."""
    assert a._live_entry_allowed("alpaca99") == (True, "")


# ── preflight ───────────────────────────────────────────────────────────────

def test_preflight_names_every_blocker_at_once(monkeypatch, live):
    """One read should tell you everything standing between here and a live fill,
    rather than surfacing them one deploy at a time."""
    monkeypatch.setattr(a, "LIVE_TRADING_ARMED", False)
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 0.0)

    class _Acct:
        equity = 20_000.0; cash = 20_000.0; buying_power = 80_000.0
        daytrading_buying_power = 80_000.0; regt_buying_power = 40_000.0
        last_equity = 20_000.0; pattern_day_trader = False
        trading_blocked = False; account_blocked = False
        transfers_blocked = False; shorting_enabled = True
        daytrade_count = 0; status = "ACTIVE"

    class _T:
        def get_account(self): return _Acct()
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"]._trading = _T()

    d = a._live_account_preflight("alpaca6")
    assert d["will_trade"] is False
    assert any("not armed" in b for b in d["blockers"])
    assert any("LIVE_SIZE_DOLLARS unset" in b for b in d["blockers"])
    assert d["equity"] == 20_000.0


def test_a_stale_pdt_flag_is_never_treated_as_a_blocker(monkeypatch, live):
    """Superseded on purpose. This used to assert the OPPOSITE — that a
    pattern_day_trader flag blocks arming — which held only while the rule existed.
    FINRA retired it effective 2026-06-04 and Alpaca removed the field on
    2026-07-06, so anything still setting it is stale data, not a restriction.
    Blocking on it would ground the book over a field that no longer means anything."""
    class _A:
        equity = 20_000.0; cash = 20_000.0; buying_power = 80_000.0
        daytrading_buying_power = 0.0; regt_buying_power = 0.0; last_equity = 20_000.0
        pattern_day_trader = True; trading_blocked = False; account_blocked = False
        transfers_blocked = False; shorting_enabled = True
        daytrade_count = 7; status = "ACTIVE"

    class _T:
        def get_account(self): return _A()
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"]._trading = _T()

    d = a._live_account_preflight("alpaca6")
    assert not any("pattern_day_trader" in b for b in d["blockers"])
    assert d["_retired_fields"]["pattern_day_trader"] is True   # reported, not acted on
    assert d["margin_extended"] is True                         # 4x BP is what counts


def test_preflight_surfaces_a_broker_side_block(monkeypatch, live):
    class _Acct:
        equity = 20_000.0; cash = 0.0; buying_power = 0.0
        daytrading_buying_power = 0.0; regt_buying_power = 0.0; last_equity = 0.0
        pattern_day_trader = False; trading_blocked = True; account_blocked = False
        transfers_blocked = False; shorting_enabled = True
        daytrade_count = 0; status = "ACTIVE"

    class _T:
        def get_account(self): return _Acct()
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"]._trading = _T()
    assert "trading_blocked" in a._live_account_preflight("alpaca6")["blockers"]


def test_preflight_endpoint_rejects_an_unknown_account():
    a.app.config["TESTING"] = True
    r = a.app.test_client().get("/api/accounts/preflight?account=nope")
    assert r.status_code == 400


# ── the gate reference covers it ────────────────────────────────────────────

def test_the_live_guard_documents_itself(live):
    """It records blocks, so the Refusal table will show it — and the drift guard
    in test_gate_docs requires anything recordable to carry a rule modal."""
    d = a._gate_docs("alpaca6")["live-guard"]
    assert d["on"] is True and "ARMED" in d["setting"]
    assert "2,000" in d["setting"]


def test_a_disarmed_live_book_says_so_in_its_modal(monkeypatch, live):
    monkeypatch.setattr(a, "LIVE_TRADING_ARMED", False)
    assert "NOT ARMED" in a._gate_docs("alpaca6")["live-guard"]["setting"]


def test_the_guard_reads_as_inert_on_paper(live):
    d = a._gate_docs("alpaca4")["live-guard"]
    assert d["on"] is False and "paper book" in d["setting"]


# ── permissions and settled cash (found by the first real preflight) ─────────

def test_a_short_entry_is_refused_when_shorting_is_disabled(live):
    """The funded account came back shorting_enabled=false. Crew picks carry sides,
    so without this every SHORT pick becomes a broker order rejection instead of a
    recorded, explainable block."""
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"] = _Broker(20_000.0, shorting_enabled=False)
    ok, why = a._live_entry_allowed("alpaca6", notional=1_000, side="SHORT")
    assert ok is False and "shorting is not enabled" in why
    # ...and the long side of the same roster still trades.
    assert a._live_entry_allowed("alpaca6", notional=1_000, side="LONG")[0] is True


def test_shorts_pass_when_the_account_permits_them(live):
    assert a._live_entry_allowed("alpaca6", notional=1_000, side="SHORT")[0] is True


def test_an_entry_beyond_available_buying_power_is_refused(live):
    """Equity counts money that may not be spendable — deposits under a hold, or
    margin the broker has not extended — so equity alone would pass orders the
    broker then rejects."""
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"] = _Broker(7_000.0, buying_power=500.0)
    ok, why = a._live_entry_allowed("alpaca6", notional=1_000, side="LONG")
    assert ok is False and "available buying power" in why


def test_the_snapshot_is_cached_but_not_indefinitely(live):
    """Read on every entry, so it is cached — but a stale balance is exactly what
    this guard must not act on."""
    calls = {"n": 0}

    class _Counting(_Trading):
        def get_account(self):
            calls["n"] += 1
            return super().get_account()

    a._live_snap_cache.clear()
    br = _Broker(20_000.0)
    br._trading = _Counting(_Acct(20_000.0))
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"] = br
    for _ in range(5):
        a._live_entry_allowed("alpaca6", notional=100, side="LONG")
    assert calls["n"] == 1, "should hit the broker once inside the TTL"
    a._live_snap_cache.clear()
    a._live_entry_allowed("alpaca6", notional=100, side="LONG")
    assert calls["n"] == 2, "and re-read once the entry expires"


def test_preflight_reports_leverage_not_enabled(live):
    """The funded account came back with buying power at 1x equity and shorting off.

    Read from `buying_power` ALONE. Alpaca removed pattern_day_trader,
    daytrade_count and daytrading_buying_power on 2026-07-06 with the PDT rule, so
    those now read false/None/0 for every account — diagnosing from them produced a
    confident wrong answer once, and this test exists so it cannot again."""
    class _A:
        equity = 7_000.0; cash = 7_000.0; buying_power = 7_000.0
        daytrading_buying_power = 0.0; regt_buying_power = 7_000.0
        last_equity = 1_000.0; pattern_day_trader = False
        trading_blocked = False; account_blocked = False
        transfers_blocked = False; shorting_enabled = False
        daytrade_count = None; status = "ACTIVE"

    class _T:
        def get_account(self): return _A()
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"]._trading = _T()

    d = a._live_account_preflight("alpaca6")
    assert d["margin_extended"] is False
    assert d["buying_power_ratio"] == 1.0
    assert "LEVERAGE-ENABLED" in d["margin_note"]
    assert any("shorting is not enabled" in b for b in d["blockers"])
    # The retired fields must never produce a blocker of their own.
    assert not any("pattern_day_trader" in b for b in d["blockers"])
    assert d["_retired_fields"]["note"].startswith("removed from Alpaca")


def test_a_zero_daytrading_buying_power_is_not_evidence_of_anything(live):
    """DTBP reads 0 for every account since 2026-07-06. A leverage-enabled account
    must still read as leveraged despite it."""
    class _A:
        equity = 7_000.0; cash = 7_000.0; buying_power = 28_000.0
        daytrading_buying_power = 0.0; regt_buying_power = 0.0
        last_equity = 7_000.0; pattern_day_trader = False
        trading_blocked = False; account_blocked = False
        transfers_blocked = False; shorting_enabled = True
        daytrade_count = None; status = "ACTIVE"

    class _T:
        def get_account(self): return _A()
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"]._trading = _T()
    d = a._live_account_preflight("alpaca6")
    assert d["margin_extended"] is True and d["buying_power_ratio"] == 4.0
    assert "margin_note" not in d


def test_preflight_sees_margin_when_it_is_extended(live):
    """4x day-trading buying power is unambiguous."""
    class _A:
        equity = 7_000.0; cash = 7_000.0; buying_power = 28_000.0
        daytrading_buying_power = 28_000.0; regt_buying_power = 14_000.0
        last_equity = 7_000.0; pattern_day_trader = False
        trading_blocked = False; account_blocked = False
        transfers_blocked = False; shorting_enabled = True
        daytrade_count = 0; status = "ACTIVE"

    class _T:
        def get_account(self): return _A()
    a._live_snap_cache.clear()
    a.ACCOUNTS_BY_TAG["alpaca6"]["broker"]._trading = _T()
    d = a._live_account_preflight("alpaca6")
    assert d["margin_extended"] is True
    assert "margin_note" not in d
    assert d["buying_power_fields"], "must surface whatever BP fields Alpaca returns"
