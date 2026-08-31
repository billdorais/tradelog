"""Pairing Crew Live against Crew Paper (/api/live_vs_paper).

The two books trade the same roster under the same gates, so any difference
between them is EXECUTION — fill timing, fill price, and the knock-on effect of
one book already holding a ticker the other does not.

This exists because of a real morning: 2026-08-31, MSFT, Live in at 09:35 for
-$5.05 and Paper in at 09:38 for +$29.23. Two charts side by side could not say
whether that was slippage on one setup or two different setups entirely.

The load-bearing decision is that ranking is on RETURN, never dollars. Paper runs
the Refined band and Live runs LIVE_SIZE_DOLLARS, so a dollar comparison would
mostly be measuring position size.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a

DAY = "2026-08-31"


def _rt(strategy, ticker, side, entry_price, exit_price, qty, entry_hhmm,
        exit_hhmm=None, reason="Trail"):
    pnl = (exit_price - entry_price) * qty * (1 if side == "LONG" else -1)
    return {"strategy": strategy, "ticker": ticker, "side": side, "date": DAY,
            "entry_price": entry_price, "exit_price": exit_price, "qty": qty,
            "pnl": round(pnl, 2), "exit_reason": reason,
            "entry_time": f"{DAY}T{entry_hhmm}:00Z",
            "exit_time":  f"{DAY}T{exit_hhmm or entry_hhmm}:30Z"}


@pytest.fixture
def books(monkeypatch):
    """Both Crew books configured; each returns its own round-trips."""
    state = {"live": [], "paper": []}

    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "4": {"tag": "alpaca4", "label": "Crew Paper", "broker": object(),
              "fills_fn": lambda: "paper"},
        "6": {"tag": "alpaca6", "label": "Crew Live", "broker": object(),
              "fills_fn": lambda: "live"},
    })
    monkeypatch.setattr(a, "_build_signal_lookup_for_alpaca", lambda: {})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda fills, **k: {"closed_clean": state[fills]})
    monkeypatch.setattr(a, "_lvp_blocks", lambda *_: {})
    a.app.config["TESTING"] = True
    return state


def _get(**params):
    q = "&".join(f"{k}={v}" for k, v in {"from": DAY, "to": DAY, **params}.items())
    return a.app.test_client().get(f"/api/live_vs_paper?{q}").get_json()


# ── configuration ───────────────────────────────────────────────────────────

def test_refuses_when_the_live_book_is_not_configured():
    r = a.app.test_client().get("/api/live_vs_paper")
    assert r.status_code == 400 and "acct6" in r.get_json()["error"]


# ── pairing ─────────────────────────────────────────────────────────────────

def test_the_same_setup_in_both_books_is_paired(books):
    books["live"]  = [_rt("MSFT_CAM_BREAKOUT_R4S4", "MSFT", "LONG", 500.0, 499.0, 10, "13:35")]
    books["paper"] = [_rt("MSFT_CAM_BREAKOUT_R4S4", "MSFT", "LONG", 499.0, 505.0, 50, "13:38")]
    d = _get()
    assert d["summary"] == {**d["summary"], "paired": 1, "live_only": 0, "paper_only": 0}
    p = d["pairs"][0]
    assert p["ticker"] == "MSFT"
    # The real 08-31 shape: Live in at 09:35, Paper at 09:38. Live moved FIRST, so
    # the lag is negative — and that is the fact worth reading off the row.
    assert p["entry_lag_secs"] == -180
    assert p["live"]["pnl"] < 0 < p["paper"]["pnl"], "opposite outcomes, one setup"


def test_entry_lag_is_signed_from_lives_point_of_view(books):
    """Which book moved first is the first question you ask; an unsigned gap
    cannot answer it."""
    books["live"]  = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.1, 10, "13:35")]
    books["paper"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.1, 50, "13:40")]
    assert _get()["pairs"][0]["entry_lag_secs"] == -300      # Live 5 min earlier

    books["live"]  = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.1, 10, "13:45")]
    assert _get()["pairs"][0]["entry_lag_secs"] == 300       # Live 5 min later


def test_setups_too_far_apart_are_not_paired(books):
    """A morning trade and an afternoon one on the same name are two setups, not
    one trade with a very slow fill."""
    books["live"]  = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.1, 10, "13:35")]
    books["paper"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.1, 50, "18:00")]
    d = _get()
    assert d["summary"]["paired"] == 0
    assert d["summary"]["live_only"] == 1 and d["summary"]["paper_only"] == 1


def test_opposite_sides_are_never_paired(books):
    books["live"]  = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG",  10.0, 10.1, 10, "13:35")]
    books["paper"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "SHORT", 10.0,  9.9, 50, "13:36")]
    assert _get()["summary"]["paired"] == 0


def test_repeated_setups_pair_nearest_first(books):
    """Greedy on smallest lag. Pairing the first Live trade with a much later
    Paper trade would report a huge lag and mis-attribute both."""
    books["live"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.2, 10, "13:35"),
                     _rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 11.0, 11.2, 10, "13:44")]
    books["paper"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.2, 50, "13:36"),
                      _rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 11.0, 11.2, 50, "13:45")]
    d = _get()
    assert d["summary"]["paired"] == 2
    assert all(abs(p["entry_lag_secs"]) <= 60 for p in d["pairs"])


def test_a_paper_trade_is_only_used_once(books):
    books["live"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.2, 10, "13:35"),
                     _rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.2, 10, "13:36")]
    books["paper"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.2, 50, "13:35")]
    d = _get()
    assert d["summary"]["paired"] == 1 and d["summary"]["live_only"] == 1


# ── the comparison must not be a size comparison ────────────────────────────

def test_identical_trades_at_different_sizes_show_no_difference(books):
    """The whole point. Same entry, same exit, 5x the size — dollars differ wildly
    and the RETURN delta is zero, because nothing about execution differed."""
    books["live"]  = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 100.0, 101.0, 10, "13:35")]
    books["paper"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 100.0, 101.0, 50, "13:35")]
    p = _get()["pairs"][0]
    assert p["live"]["pnl"] == 10.0 and p["paper"]["pnl"] == 50.0    # 5x the dollars
    assert p["pct_delta"] == 0.0                                     # identical execution
    assert p["slippage_bps"] == 0.0


def test_return_is_measured_on_the_trades_own_notional(books):
    books["live"]  = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 100.0, 101.0, 10, "13:35")]
    books["paper"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 100.0, 101.0, 50, "13:35")]
    p = _get()["pairs"][0]
    assert p["live"]["pct"] == 1.0 and p["paper"]["pct"] == 1.0


# ── slippage sign ───────────────────────────────────────────────────────────

def test_a_long_paying_more_is_worse_slippage(books):
    books["live"]  = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 100.10, 101.0, 10, "13:35")]
    books["paper"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 100.00, 101.0, 50, "13:35")]
    assert _get()["pairs"][0]["slippage_bps"] == 10.0        # positive = Live worse


def test_a_short_receiving_less_is_also_worse_slippage(books):
    """Sign must follow the side. Unsigned, a mixed book averages its own slippage
    away to nothing and reads as perfect execution."""
    books["live"]  = [_rt("X_CAM_REVERSAL_R3S3", "X", "SHORT", 99.90, 99.0, 10, "13:35")]
    books["paper"] = [_rt("X_CAM_REVERSAL_R3S3", "X", "SHORT", 100.00, 99.0, 50, "13:35")]
    assert _get()["pairs"][0]["slippage_bps"] == 10.0        # sold lower = Live worse


def test_slippage_does_not_cancel_out_across_a_mixed_book(books):
    """A long and a short, each 10bps worse on Live. The median must be 10, not 0."""
    books["live"]  = [_rt("A_CAM_BREAKOUT_R3S3", "A", "LONG",  100.10, 101.0, 10, "13:35"),
                      _rt("B_CAM_REVERSAL_R3S3", "B", "SHORT", 99.90,  99.0, 10, "13:36")]
    books["paper"] = [_rt("A_CAM_BREAKOUT_R3S3", "A", "LONG",  100.00, 101.0, 50, "13:35"),
                      _rt("B_CAM_REVERSAL_R3S3", "B", "SHORT", 100.00, 99.0, 50, "13:36")]
    assert _get()["summary"]["median_slippage_bps"] == 10.0


# ── unmatched trades ────────────────────────────────────────────────────────

def test_a_trade_only_one_book_took_is_reported_as_such(books):
    books["live"]  = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.1, 10, "13:35")]
    books["paper"] = []
    d = _get()
    assert d["summary"]["live_only"] == 1 and d["summary"]["paper_only"] == 0
    assert d["live_only"][0]["trade"]["ticker"] == "X"


def test_an_unmatched_trade_names_the_other_books_gate(monkeypatch, books):
    """The actionable half: 'Paper skipped this on RVOL' beats an unexplained gap."""
    monkeypatch.setattr(a, "_lvp_blocks", lambda *_: {
        ("alpaca4", "X", DAY): [{"gate": "rvol", "reason": "below 1.5x",
                                 "strategy": "X_CAM_BREAKOUT_R3S3"}]})
    books["live"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.1, 10, "13:35")]
    blocked = _get()["live_only"][0]["other_book_blocked"]
    assert blocked["gate"] == "rvol" and "1.5x" in blocked["reason"]


def test_no_recorded_block_reads_as_unknown_not_as_no_reason(books):
    """The position gate is not recorded, so a blank here usually means the other
    book was already holding the ticker — the caveats say so explicitly."""
    books["live"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 10.0, 10.1, 10, "13:35")]
    d = _get()
    assert d["live_only"][0]["other_book_blocked"] is None
    assert any("position gate is not recorded" in c.lower() for c in d["caveats"])


# ── summary ─────────────────────────────────────────────────────────────────

def test_summary_counts_which_book_executed_better(books):
    books["live"]  = [_rt("A_CAM_BREAKOUT_R3S3", "A", "LONG", 100.0, 102.0, 10, "13:35"),
                      _rt("B_CAM_BREAKOUT_R3S3", "B", "LONG", 100.0,  99.0, 10, "13:36")]
    books["paper"] = [_rt("A_CAM_BREAKOUT_R3S3", "A", "LONG", 100.0, 101.0, 50, "13:35"),
                      _rt("B_CAM_BREAKOUT_R3S3", "B", "LONG", 100.0, 100.5, 50, "13:36")]
    s = _get()["summary"]
    assert s["live_better"] == 1 and s["paper_better"] == 1


def test_an_empty_window_returns_a_usable_shell(books):
    d = _get()
    assert d["summary"]["paired"] == 0
    assert d["live"]["trades"] == 0 and d["paper"]["trades"] == 0
    assert d["summary"]["median_slippage_bps"] is None
    assert d["caveats"]


def test_book_totals_carry_both_dollars_and_return(books):
    books["live"]  = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 100.0, 101.0, 10, "13:35")]
    books["paper"] = [_rt("X_CAM_BREAKOUT_R3S3", "X", "LONG", 100.0, 101.0, 50, "13:35")]
    d = _get()
    assert d["live"]["pnl"] == 10.0 and d["paper"]["pnl"] == 50.0
    assert d["live"]["avg_pct"] == d["paper"]["avg_pct"] == 1.0
    assert d["live"]["label"] == "Crew Live"
