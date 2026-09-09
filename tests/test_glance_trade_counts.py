"""Today's P&L cards show the record behind the dollars, not just the dollars.

The motivating case: a card reading "+$0.00" is ambiguous. It is produced both by a
book that took no trades at all (sum of an empty list) and by a book that traded to
a flat zero -- opposite situations with identical headlines.

The counts are derived from the SAME LIFO pairing that produces the P&L. Counting
separately would let a card show dollars and a trade count that disagree about what
happened, which is worse than showing no count.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as kairos


def _rts(*pnls):
    return [{"pnl": p, "strategy": "S", "ticker": "T", "side": "LONG", "qty": 1}
            for p in pnls]


def _stats(monkeypatch, pnls, fail=False):
    def _paired(f, from_date="", to_date="", **kw):
        if fail:
            raise RuntimeError("alpaca 502")
        return {"closed_clean": list(f)}
    monkeypatch.setattr(kairos, "_pair_alpaca_fills_lifo", _paired)
    return kairos._realized_daily_stats(lambda: _rts(*pnls))


def test_counts_wins_and_losses(monkeypatch):
    st = _stats(monkeypatch, [120.0, 45.0, -30.0])
    assert (st["trades"], st["wins"], st["losses"]) == (3, 2, 1)
    assert st["pnl"] == 135.0


def test_no_trades_is_distinguishable_from_a_flat_day(monkeypatch):
    """Both headline +$0.00. The count is the only thing that tells them apart, which
    is the whole reason for this change."""
    quiet = _stats(monkeypatch, [])
    flat  = _stats(monkeypatch, [50.0, -50.0])
    assert quiet["pnl"] == flat["pnl"] == 0.0
    assert quiet["trades"] == 0
    assert flat["trades"] == 2


def test_a_scratch_counts_as_a_loss(monkeypatch):
    """Same convention as _strategy_breakdown and the recap. The dashboard must not
    use a kinder rule than the report it is summarising."""
    st = _stats(monkeypatch, [0.0])
    assert (st["wins"], st["losses"]) == (0, 1)


def test_a_failed_fetch_is_none_not_a_zeroed_record(monkeypatch):
    """A zeroed dict would render as a confident "0 trades" for a book whose fills
    merely failed to load -- the fail-silent shape this repo keeps producing."""
    assert _stats(monkeypatch, [120.0], fail=True) is None


def test_the_scalar_helper_agrees_with_the_stats(monkeypatch):
    """_realized_daily_pnl delegates, so the profit-lock loop and the card can never
    disagree about today's dollars."""
    def _paired(f, from_date="", to_date="", **kw):
        return {"closed_clean": list(f)}
    monkeypatch.setattr(kairos, "_pair_alpaca_fills_lifo", _paired)
    fn = lambda: _rts(120.0, -30.0)
    assert kairos._realized_daily_pnl(fn) == kairos._realized_daily_stats(fn)["pnl"]


def test_a_failed_fetch_still_returns_none_from_the_scalar(monkeypatch):
    """The profit-lock loop relies on None to SKIP rather than read a false $0."""
    def _boom(f, from_date="", to_date="", **kw):
        raise RuntimeError("alpaca 502")
    monkeypatch.setattr(kairos, "_pair_alpaca_fills_lifo", _boom)
    assert kairos._realized_daily_pnl(lambda: _rts(120.0)) is None


def test_the_card_renders_the_count(monkeypatch):
    html = kairos.app.test_client().get("/").get_data(as_text=True)
    assert "daily_trades" in html
    assert "no closed trades yet" in html
    # The null branch means the fills did not load; it must not claim there were
    # no trades, which is a statement about the market rather than about the fetch.
    assert "no closed trades today" not in html
