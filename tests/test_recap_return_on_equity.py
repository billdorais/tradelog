"""Recap shows the period's P&L as a percentage of the equity that produced it.

Dollars alone are not comparable across books: $50 on Crew Live's balance is a very
different result from $50 on Crew Paper's, and the recap now has a tab for each.

The denominator is the equity at the START of the window. There is no historical
per-book equity to read — account_snapshots is IB-only and has no account column —
so the start is backed out as (current equity - window P&L). Dividing by CURRENT
equity would be the easy version and is wrong in a consistent direction: it
understates every gain and overstates every loss.
"""
from __future__ import annotations

import inspect
import os

import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as kairos


def _rt(pnl, day):
    return {"strategy": "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN", "ticker": "NVDA",
            "side": "LONG", "pnl": pnl, "qty": 10, "date": f"2026-09-{day:02d}",
            "entry_price": 100.0, "exit_price": 100.0 + pnl / 10,
            "entry_time": f"2026-09-{day:02d}T13:40:00+00:00",
            "exit_time":  f"2026-09-{day:02d}T13:55:00+00:00"}


class _Broker:
    def __init__(self, eq): self.eq = eq

    def account_equity(self):
        if self.eq is None:
            raise RuntimeError("connection reset")
        return self.eq


def _book(monkeypatch, equity, pnls, frm="2026-09-01", to="2026-09-30"):
    rts = [_rt(p, 2 + i) for i, p in enumerate(pnls)]
    rec = {"broker": _Broker(equity), "label": "Crew Paper", "tag": "alpaca4",
           "num": "4", "paper": True, "fills_fn": (lambda: rts)}
    monkeypatch.setattr(kairos, "ACCOUNTS_BY_NUM", {"4": rec})
    monkeypatch.setattr(kairos, "ACCOUNTS_BY_TAG", {"alpaca4": rec})
    monkeypatch.setattr(kairos, "ALPACA_ACCOUNTS", [rec])
    monkeypatch.setattr(kairos, "_alpaca_account_ctx",
                        lambda a: (_Broker(equity), "alpaca4", "Crew Paper", lambda: rts))
    def _paired(f, from_date="", to_date="", **kw):
        # Honour the window as the real pairing does, or a narrower period would
        # silently return every trade and the ratio test would prove nothing.
        keep = [t for t in f
                if (not from_date or t["date"] >= from_date)
                and (not to_date or t["date"] <= to_date)]
        return {"closed_clean": keep, "closed": keep, "orphans": [],
                "deduped": keep, "signal_lookup": {}}
    monkeypatch.setattr(kairos, "_pair_alpaca_fills_lifo", _paired)
    monkeypatch.setattr(kairos, "_fills_error", lambda a: None)
    return (kairos._build_recap(account="4", frm=frm, to=to) or {}).get("book") or {}


def test_the_denominator_is_the_start_of_the_window_not_now(monkeypatch):
    """+$500 ending at $7,500 started at $7,000: 7.14%, not the 6.67% you get by
    dividing by current equity."""
    b = _book(monkeypatch, 7500.0, [200.0, 300.0])
    assert b["equity_start_est"] == 7000.0
    assert b["pct_return"] == pytest.approx(7.14, abs=0.01)


def test_a_loss_is_measured_the_same_way(monkeypatch):
    """Backing out a NEGATIVE P&L raises the start, so the loss reads slightly
    larger — which is the honest direction."""
    b = _book(monkeypatch, 6500.0, [-200.0, -300.0])
    assert b["equity_start_est"] == 7000.0
    assert b["pct_return"] == pytest.approx(-7.14, abs=0.01)


def test_the_same_dollars_read_differently_on_two_books(monkeypatch):
    """The reason for the tile: the recap now has a tab per book."""
    small = _book(monkeypatch, 7050.0, [50.0])
    big   = _book(monkeypatch, 100050.0, [50.0])
    assert small["pnl"] == big["pnl"] == 50.0
    assert small["pct_return"] > big["pct_return"] * 10


def test_a_flat_book_is_zero_not_missing(monkeypatch):
    b = _book(monkeypatch, 7000.0, [100.0, -100.0])
    assert b["pct_return"] == 0.0


def test_the_window_moves_both_halves_of_the_ratio(monkeypatch):
    """A narrower window has less P&L AND a start closer to current equity, so the
    percentage has to be recomputed rather than scaled."""
    wide   = _book(monkeypatch, 7500.0, [200.0, 300.0])
    narrow = _book(monkeypatch, 7500.0, [200.0, 300.0], frm="2026-09-02", to="2026-09-02")
    assert narrow["pnl"] < wide["pnl"]
    assert narrow["equity_start_est"] > wide["equity_start_est"]
    assert narrow["pct_return"] != wide["pct_return"]


def test_unreadable_equity_shows_nothing_rather_than_a_wrong_number(monkeypatch):
    """A broker wobble must cost the percentage line, not the whole recap."""
    b = _book(monkeypatch, None, [200.0])
    assert b["pct_return"] is None and b["equity"] is None
    assert b["equity_error"]
    assert b["pnl"] == 200.0, "the rest of the recap still has to work"


def test_a_start_at_or_below_zero_is_refused(monkeypatch):
    """P&L exceeding present equity means money left the account — a funding
    change, not a return, and not a denominator."""
    b = _book(monkeypatch, 100.0, [500.0])
    assert b["pct_return"] is None and b["equity_start_est"] is None


def test_equity_is_read_inside_its_own_guard():
    src = inspect.getsource(kairos._build_recap)
    i = src.index("account_equity()")
    assert "try:" in src[i - 200:i]
    assert "except Exception" in src[i:i + 200]


# ── UI ──────────────────────────────────────────────────────────────────────────

def _html():
    return open("templates/recap.html", encoding="utf-8").read()


def test_the_tile_is_rendered_with_its_basis():
    html = _html()
    assert "Return on equity" in html
    assert "b.pct_return" in html
    assert "at period start" in html, "the denominator should be visible, not implied"


def test_the_tile_degrades_to_a_dash():
    html = _html()
    i = html.index("Return on equity")
    block = html[i - 700:i + 900]
    assert "equity unavailable" in block
    assert "'—'" in block


def test_the_estimate_is_labelled_as_one():
    """A deposit or withdrawal inside the window breaks the back-out, so the number
    must not present itself as exact."""
    html = _html()
    i = html.index("Return on equity")
    assert "estimated as" in html[i - 900:i + 400]


def test_the_band_no_longer_claims_crew_paper_only():
    """The page has a tab per book now; a hardcoded subtitle would mislabel Live."""
    html = _html()
    assert "Crew Paper only" not in html
    assert "Real money." in html
