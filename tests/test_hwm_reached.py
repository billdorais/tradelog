"""High-water-mark peak-profit-reached buckets — _hwm_summarize.

"How many trades made it to $X profit" = trades whose peak unrealized P&L
(peak_dollars) touched >= $X at any point, split into kept (closed >= $X) vs
gave it back (closed below $X).
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a


def _row(peak_dollars, realized, entry=100.0):
    # peak_price only affects the went-green classification, not the reached buckets;
    # give a small favorable move so ep is non-zero.
    return {"peak_dollars": peak_dollars, "realized_pnl": realized,
            "entry_price": entry, "peak_price": entry * 1.01,
            "giveback_dollars": max(0.0, peak_dollars - realized),
            "strategy": "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"}


def _at(summary, threshold):
    return next(x for x in summary["peak_reached"] if x["threshold"] == threshold)


def test_reached_kept_and_gaveback_counts():
    rows = [
        _row(60, 55),   # reached 25 & 50, kept both
        _row(70, 30),   # reached 25 & 50; kept 25, gave back at 50
        _row(40, 40),   # reached 25 only, kept 25
        _row(10, -20),  # reached nothing
    ]
    s = a._hwm_summarize(rows)
    t50 = _at(s, 50)
    assert t50["reached"] == 2 and t50["kept"] == 1 and t50["gaveback"] == 1
    t25 = _at(s, 25)
    assert t25["reached"] == 3 and t25["kept"] == 3 and t25["gaveback"] == 0
    assert _at(s, 100)["reached"] == 0


def test_tp_sim_pnl_and_delta_vs_actual():
    rows = [
        _row(60, 55),   # peak 60, closed +55
        _row(70, 30),   # peak 70, closed +30 (gave back)
        _row(40, 40),   # peak 40, closed +40
        _row(10, -20),  # never green, closed -20
    ]
    s = a._hwm_summarize(rows)
    assert s["overall"]["realized"] == 105.0            # actual total
    # TP at $50: A,B fire (+50 each), C,D close as-is (40, -20) -> 120, delta +15
    t50 = _at(s, 50)
    assert t50["sim_pnl"] == 120.0 and t50["delta"] == 15.0
    # TP at $25: A,B,C fire (+25 each), D closes -20 -> 55, delta -50 (caps too early)
    t25 = _at(s, 25)
    assert t25["sim_pnl"] == 55.0 and t25["delta"] == -50.0


def test_thresholds_present_even_with_no_rows():
    s = a._hwm_summarize([])
    assert [x["threshold"] for x in s["peak_reached"]] == [25, 50, 100, 150, 200]
    assert all(x["reached"] == 0 and x["kept"] == 0 for x in s["peak_reached"])
