"""Promotion ranks farm strategies on TAKEABLE performance only.

The farms trade ungated on purpose — they are the full-sample audition pool and
the control group for gate-cost analysis. But their raw leaderboard therefore
contains trades the curated book could never have taken: outside its hours, on a
blocked day type, or on a reversal side it refuses. Promoting on that ranks
strategies partly on unreachable P&L.

_takeable_by replays each farm round-trip through the target book's OWN live gate
functions, so the filter cannot drift from real behaviour.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


BRK = "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"
REV = "AMZN_CAM_REVERSAL_R4S4_V02_5MIN"


def _rt(strategy, ticker, pnl=10.0, side="LONG", date="2026-08-12",
        entry_hhmm_et="09:45"):
    # entry_time is stored UTC; 09:45 ET = 13:45 UTC in August (EDT).
    hh, mm = (int(x) for x in entry_hhmm_et.split(":"))
    return {"strategy": strategy, "ticker": ticker, "pnl": pnl, "side": side,
            "date": date, "qty": 10, "entry_price": 100.0,
            "entry_time": f"{date}T{hh + 4:02d}:{mm:02d}:00Z",
            "exit_time": f"{date}T{hh + 5:02d}:{mm:02d}:00Z"}


@pytest.fixture()
def gates(monkeypatch):
    """Outside-day gate on, no hours window, no reversal policy — tests opt in."""
    monkeypatch.setattr(a, "DAYTYPE_GATE_ENABLED", True)
    monkeypatch.setattr(a, "DAYTYPE_GATE_ACCOUNTS", {"alpaca2", "alpaca3"})
    monkeypatch.setattr(a, "DAYTYPE_GATE_BREAKOUT_OK_DAYS", {"Outside"})
    monkeypatch.setattr(a, "_account_gate_overrides", lambda tag=None: {})
    monkeypatch.setattr(a, "_account_hours_windows", lambda tag: [])
    monkeypatch.setattr(a, "_REVERSAL_SIDE_BY_TAG", {})
    return monkeypatch


def test_drops_trades_on_a_blocked_day_type(gates, monkeypatch):
    monkeypatch.setattr(a, "_get_day_classification",
                        lambda tk, d: {"day_type": "Inside" if tk == "BAD" else "Outside"})
    rts = [_rt(BRK, "GOOD"), _rt(BRK, "BAD")]
    kept, dropped = a._takeable_by(rts, "alpaca2")
    assert [c["ticker"] for c in kept] == ["GOOD"]
    assert dropped == {"day-type": 1}


def test_drops_trades_outside_the_books_hours(gates, monkeypatch):
    monkeypatch.setattr(a, "_get_day_classification", lambda tk, d: {"day_type": "Outside"})
    monkeypatch.setattr(a, "_account_hours_windows", lambda tag: [("09:35", "10:00")])
    rts = [_rt(BRK, "IN",  entry_hhmm_et="09:45"),
           _rt(BRK, "OUT", entry_hhmm_et="11:30")]
    kept, dropped = a._takeable_by(rts, "alpaca2")
    assert [c["ticker"] for c in kept] == ["IN"]
    assert dropped == {"hours": 1}


def test_drops_a_reversal_side_the_book_refuses(gates, monkeypatch):
    monkeypatch.setattr(a, "_get_day_classification", lambda tk, d: {"day_type": "Outside"})
    monkeypatch.setattr(a, "_REVERSAL_SIDE_BY_TAG", {"alpaca3": "long"})   # shorts refused
    rts = [_rt(REV, "OKL", side="LONG"), _rt(REV, "NOS", side="SHORT")]
    kept, dropped = a._takeable_by(rts, "alpaca3")
    assert [c["ticker"] for c in kept] == ["OKL"]
    assert dropped == {"reversal": 1}


def test_fails_open_when_a_gate_check_raises(gates, monkeypatch):
    """An unclassifiable ticker keeps the trade — matching how the live gates
    themselves fail open. Silently dropping would understate the strategy."""
    def _boom(tk, d):
        raise RuntimeError("no daily bars")
    monkeypatch.setattr(a, "_get_day_classification", _boom)
    kept, dropped = a._takeable_by([_rt(BRK, "WEIRD")], "alpaca2")
    assert len(kept) == 1 and dropped == {}


def test_untouched_when_no_gate_as_is_given(gates, monkeypatch):
    """Farm/analysis callers that want the FULL sample must be unaffected — the
    farms' whole job is to keep collecting everything."""
    monkeypatch.setattr(a, "_get_day_classification", lambda tk, d: {"day_type": "Inside"})
    rts = [_rt(BRK, "AAA"), _rt(BRK, "BBB")]
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda fills, **kw: {"closed_clean": list(rts)})
    monkeypatch.setattr(a, "_load_excluded_strategies", lambda: set())
    monkeypatch.setattr(a, "_load_excluded_tickers", lambda: set())
    full = a._compute_strategy_stats(days=45, fills_fn=lambda: ["x"])
    assert full[BRK]["trades"] == 2                      # nothing filtered

    gated = a._compute_strategy_stats(days=45, fills_fn=lambda: ["x"], gate_as="alpaca2")
    assert BRK not in gated, "an Inside-day breakout survived the takeable filter"


def test_ranking_changes_when_the_edge_is_unreachable(gates, monkeypatch):
    """The point of the whole thing: a strategy whose P&L comes from trades the
    book cannot take must not out-rank one whose P&L is reachable."""
    monkeypatch.setattr(a, "_get_day_classification", lambda tk, d: {"day_type": "Outside"})
    monkeypatch.setattr(a, "_account_hours_windows", lambda tag: [("09:35", "10:00")])
    MIRAGE, REAL = "MIR_CAM_BREAKOUT_R3S3", "REA_CAM_BREAKOUT_R3S3"
    rts = [
        _rt(MIRAGE, "MIR", pnl=500.0, entry_hhmm_et="14:00"),   # outside hours
        _rt(MIRAGE, "MIR", pnl=5.0,   entry_hhmm_et="09:45"),
        _rt(REAL,   "REA", pnl=60.0,  entry_hhmm_et="09:45"),
        _rt(REAL,   "REA", pnl=40.0,  entry_hhmm_et="09:50"),
    ]
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda fills, **kw: {"closed_clean": list(rts)})
    monkeypatch.setattr(a, "_load_excluded_strategies", lambda: set())
    monkeypatch.setattr(a, "_load_excluded_tickers", lambda: set())

    raw   = a._compute_strategy_stats(days=45, fills_fn=lambda: ["x"])
    gated = a._compute_strategy_stats(days=45, fills_fn=lambda: ["x"], gate_as="alpaca2")

    assert raw[MIRAGE]["total_pnl"] > raw[REAL]["total_pnl"], "raw pool favours the mirage"
    assert gated[REAL]["total_pnl"] > gated[MIRAGE]["total_pnl"], \
        "promotion still ranks on unreachable P&L"
    assert gated[MIRAGE]["trades"] == 1      # only the reachable one survives


# ── Performance: the filter must not hammer the network ───────────────────────
# _classify_day costs an Alpaca fetch per (ticker, date). A 45-day farm window
# across every ticker is four figures of calls — enough to hang a refresh, which
# is exactly what happened when day-type ran before the free gates.

def test_cheap_gates_run_before_the_network_gate(gates, monkeypatch):
    """A trade already excluded by hours must never be day-classified."""
    calls = []
    monkeypatch.setattr(a, "_get_day_classification",
                        lambda tk, d: (calls.append(tk), {"day_type": "Outside"})[1])
    monkeypatch.setattr(a, "_account_hours_windows", lambda tag: [("09:35", "10:00")])
    rts = [_rt(BRK, "OUT1", entry_hhmm_et="11:30"),
           _rt(BRK, "OUT2", entry_hhmm_et="14:00"),
           _rt(BRK, "IN",   entry_hhmm_et="09:45")]
    kept, dropped = a._takeable_by(rts, "alpaca2")
    assert dropped == {"hours": 2}
    assert calls == ["IN"], f"classified out-of-hours trades: {calls}"


def test_classification_budget_keeps_trades_rather_than_stalling(gates, monkeypatch):
    """Past the budget the filter degrades to 'unfiltered', never to 'hung' or
    'silently dropped'."""
    calls = []
    monkeypatch.setattr(a, "_get_day_classification",
                        lambda tk, d: (calls.append(tk), {"day_type": "Inside"})[1])
    a._day_class_cache.clear()
    rts = [_rt(BRK, f"T{i}", date=f"2026-07-{i+1:02d}") for i in range(10)]
    kept, dropped = a._takeable_by(rts, "alpaca2", classify_budget=3)
    assert len(calls) == 3, "budget did not cap the network work"
    # 3 classified (Inside → blocked), the other 7 kept unfiltered.
    assert dropped.get("day-type") == 3 and len(kept) == 7


def test_day_classification_is_persisted_and_reused(monkeypatch, tmp_path):
    """Past dates are immutable, so the fetch must happen once — not once per
    worker, per redeploy, per refresh."""
    import sqlite3
    db = tmp_path / "dc.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE day_classifications (ticker TEXT, date TEXT, "
                 "payload TEXT, PRIMARY KEY (ticker, date))")
    conn.commit(); conn.close()
    monkeypatch.setattr(a, "get_db",
                        lambda: sqlite3.connect(db))
    fetches = []
    monkeypatch.setattr(a, "_classify_day",
                        lambda tk, d: (fetches.append((tk, d)), {"day_type": "Outside"})[1])
    a._day_class_cache.clear()

    assert a._get_day_classification("AAPL", "2026-07-01")["day_type"] == "Outside"
    a._day_class_cache.clear()                      # simulate a fresh worker
    assert a._get_day_classification("AAPL", "2026-07-01")["day_type"] == "Outside"
    assert len(fetches) == 1, f"re-fetched a past date: {fetches}"


def test_todays_classification_is_not_persisted(monkeypatch, tmp_path):
    """Only closed days are written — today could still be computed off a partial
    feed, and a wrong value would be cached forever."""
    import datetime as _dt
    import sqlite3
    db = tmp_path / "dc2.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE day_classifications (ticker TEXT, date TEXT, "
                 "payload TEXT, PRIMARY KEY (ticker, date))")
    conn.commit(); conn.close()
    monkeypatch.setattr(a, "get_db", lambda: sqlite3.connect(db))
    monkeypatch.setattr(a, "_classify_day", lambda tk, d: {"day_type": "Outside"})
    a._day_class_cache.clear()
    today = _dt.datetime.now(a.ZoneInfo("America/New_York")).date().isoformat()
    a._get_day_classification("AAPL", today)
    c = sqlite3.connect(db)
    assert c.execute("SELECT COUNT(*) FROM day_classifications").fetchone()[0] == 0
    c.close()


# ── Eligibility floors ────────────────────────────────────────────────────────

def test_ondeck_floor_stays_below_the_routing_floor():
    """On-Deck is the 'next up' watchlist. At parity with the routing floor it
    empties out — every qualifying name gets routed (20 slots) and there is
    nothing left to watch. Lowering the routing floor has to bring this with it."""
    assert a._REFINED_ONDECK_MIN_TRADES < a._REFINED_MIN_TRADES, \
        "TV Refined On-Deck floor must be strictly below the routing floor"
    assert a._KAIROS_REFINED_ONDECK_MIN_TRADES < a._KAIROS_REFINED_MIN_TRADES, \
        "Kairos On-Deck floor must be strictly below the routing floor"


def test_routing_floor_is_not_below_the_small_sample_guard():
    """The floor exists to keep lucky handfuls out. Takeable trades are better
    evidence than mixed ones, but there is a hard bottom — below it a PF of 8 on
    three trades starts routing real money.

    Kairos sits one lower than TV by long-standing design: the engine account is
    newer and carries fewer fills per strategy. Neither may reach 3.
    """
    assert a._REFINED_MIN_TRADES >= 5
    assert a._KAIROS_REFINED_MIN_TRADES >= 4
    assert a._KAIROS_REFINED_MIN_TRADES <= a._REFINED_MIN_TRADES,         "Kairos floor should never exceed TV's — it is the thinner book"
