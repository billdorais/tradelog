"""Refined snapshots rank only what the book's reversal policy lets it trade.

TV Refined has reversals OFF, yet 2 of its 7 snapshot slots were reversals it
could never take (0 trades, forever). Kairos Refined is reversal-LONG only, so a
reversal ranked on both sides was partly ranked on trades it refuses.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a

REV = "QQQ_CAM_REVERSAL_R4S4_V02_5MIN"
BRK = "INTC_CAM_BREAKOUT_R3S3_V02_5MIN"


def _rt(strategy, side, pnl):
    return {"strategy": strategy, "side": side, "pnl": pnl, "ticker": strategy.split("_")[0],
            "entry_price": 100.0, "qty": 10, "date": "2026-10-20",
            "entry_time": "2026-10-20T14:00:00Z", "exit_time": "2026-10-20T14:10:00Z"}


def _stats(monkeypatch, reversal_as):
    rts = [_rt(REV, "LONG", 5), _rt(REV, "SHORT", -3), _rt(REV, "SHORT", -2),
           _rt(BRK, "LONG", 4), _rt(BRK, "SHORT", 1)]
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda *x, **k: {"closed_clean": list(rts)})
    monkeypatch.setattr(a, "_load_excluded_strategies", lambda: set())
    monkeypatch.setattr(a, "_load_excluded_tickers", lambda: set())
    return a._compute_strategy_stats(days=60, fills_fn=lambda: [1], reversal_as=reversal_as)


def test_reversals_off_book_ranks_no_reversals(monkeypatch):
    monkeypatch.setitem(a._REVERSAL_SIDE_BY_TAG, "alpaca2", "off")
    s = _stats(monkeypatch, "alpaca2")
    assert REV not in s
    assert s[BRK]["trades"] == 2                       # breakouts untouched


def test_reversal_long_book_ranks_reversal_longs_only(monkeypatch):
    monkeypatch.setitem(a._REVERSAL_SIDE_BY_TAG, "alpaca3", "long")
    s = _stats(monkeypatch, "alpaca3")
    assert s[REV]["trades"] == 1 and s[REV]["total_pnl"] == 5
    assert s[BRK]["trades"] == 2


def test_no_book_means_the_raw_pool(monkeypatch):
    s = _stats(monkeypatch, None)
    assert s[REV]["trades"] == 3 and s[BRK]["trades"] == 2


def test_both_refreshes_pass_their_book():
    import inspect
    tv = inspect.getsource(a._do_refresh_refined)
    ka = inspect.getsource(a._do_refresh_kairos_refined)
    assert tv.count("reversal_as=_REFINED_POLICY_TAG.get(broker_val)") == 2
    assert ka.count('reversal_as="alpaca3"') == 2
    assert a._REFINED_POLICY_TAG["alpaca-paper-2"] == "alpaca2"
