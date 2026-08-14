"""Leaderboard "last trade" vs the strategy modal.

Reported: SPY_CAM_BREAKOUT_R3S3 showed "14d" on the leaderboard while its modal
listed a trade from 3 days ago. Both were right — they measure different things.
`last_trade_at` is built AFTER the takeable filter, so it means "the newest trade
this book could actually have placed"; the modal lists every farm round-trip,
gate-blocked ones included.

The stats now also carry `last_trade_at_all` (pre-filter) so the UI can show both
and the two views stop looking like they contradict each other.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a


def _fills():
    """Two round-trips: an older one (07-31) and a newer one (08-11)."""
    return [
        {"symbol": "SPY", "side": "BOT", "price": 744.28, "shares": 1,
         "time": "2026-07-31T16:25:03Z", "order_id": ""},
        {"symbol": "SPY", "side": "SLD", "price": 744.61, "shares": 1,
         "time": "2026-07-31T16:40:07Z", "order_id": ""},
        {"symbol": "SPY", "side": "SLD", "price": 771.88, "shares": 1,
         "time": "2026-08-11T15:35:03Z", "order_id": ""},
        {"symbol": "SPY", "side": "BOT", "price": 772.05, "shares": 1,
         "time": "2026-08-11T15:50:09Z", "order_id": ""},
    ]


def test_ungated_both_timestamps_agree():
    """With no gate filter there is nothing to diverge — both point at the newest."""
    st = next(iter(a._compute_strategy_stats(days=3650, fills_fn=_fills, gate_as=None).values()))
    assert st["trades"] == 2
    assert st["last_trade_at"][:10] == "2026-08-11"
    assert st["last_trade_at_all"] == st["last_trade_at"]


def test_gate_blocked_newest_trade_splits_the_two(monkeypatch):
    """The reported case: the newest round-trip is NOT takeable, so ranking sees the
    older one — while `_all` still reports what the farm actually did."""
    def _drop_newest(rts, tag):
        keep = [r for r in rts if (r.get("exit_time") or "")[:10] != "2026-08-11"]
        return keep, {"day-type": len(rts) - len(keep)}
    monkeypatch.setattr(a, "_takeable_by", _drop_newest)

    st = next(iter(a._compute_strategy_stats(days=3650, fills_fn=_fills,
                                             gate_as="alpaca2").values()))
    # Ranking counts only the takeable trade...
    assert st["trades"] == 1
    assert st["last_trade_at"][:10] == "2026-07-31"
    # ...but the raw farm activity is still reported, so the UI can show both.
    assert st["last_trade_at_all"][:10] == "2026-08-11"
    assert st["last_trade_at_all"] > st["last_trade_at"]
