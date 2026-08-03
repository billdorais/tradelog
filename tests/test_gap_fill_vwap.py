"""Session-anchored VWAP helpers + gap-fill strategy smoke test.

The whole point of this strategy is a TRUE prior-day VWAP as the target, so the
VWAP math is checked against hand-computed values on a tiny two-day fixture.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.vwap import session_vwap, prior_day_vwap, gap_pct


def _fixture():
    # Two ET sessions, 3 bars each. Typical price = (H+L+C)/3.
    idx = pd.DatetimeIndex([
        "2026-01-05 09:30", "2026-01-05 09:31", "2026-01-05 09:32",
        "2026-01-06 09:30", "2026-01-06 09:31", "2026-01-06 09:32",
    ])
    o = np.array([100, 101, 102, 110, 111, 112], float)
    h = np.array([101, 102, 103, 111, 112, 113], float)
    l = np.array([ 99, 100, 101, 109, 110, 111], float)
    c = np.array([100, 101, 102, 110, 111, 112], float)
    v = np.array([100, 100, 100, 200, 200, 200], float)
    return o, h, l, c, v, idx


def test_session_vwap_resets_each_day():
    o, h, l, c, v, idx = _fixture()
    sv = session_vwap(h, l, c, v, idx)
    # Typical price = (H+L+C)/3. Day 1 tp: 100, 101, 102; equal vol → running mean.
    assert sv[0] == 100.0                 # tp0 = (101+99+100)/3
    assert sv[1] == 100.5                 # (100+101)/2
    assert sv[2] == 101.0                 # (100+101+102)/3
    # Day 2 RESETS: first bar is its own tp (110), not carrying day 1.
    assert sv[3] == 110.0                 # tp3 = (111+109+110)/3
    assert sv[4] == 110.5                 # (110+111)/2
    assert sv[5] == 111.0                 # (110+111+112)/3


def test_prior_day_vwap_carries_previous_session_final():
    o, h, l, c, v, idx = _fixture()
    pdv = prior_day_vwap(h, l, c, v, idx)
    # Day 1 has no prior session → NaN.
    assert np.isnan(pdv[0]) and np.isnan(pdv[1]) and np.isnan(pdv[2])
    # Day 2 carries day 1's FINAL session VWAP (101.0), flat across the day.
    assert pdv[3] == 101.0 and pdv[4] == 101.0 and pdv[5] == 101.0


def test_gap_pct_uses_open_vs_prior_close():
    o, h, l, c, v, idx = _fixture()
    g = gap_pct(o, c, idx)
    assert np.isnan(g[0])                 # first day: no prior close
    # Day 2 open 110 vs day 1 last close 102 → (110-102)/102*100 ≈ 7.843%
    assert abs(g[3] - (8 / 102 * 100)) < 1e-9
    assert abs(g[5] - (8 / 102 * 100)) < 1e-9   # constant across day 2


def test_strategy_runs_on_synthetic_gap_day():
    """A gap-up day that fades then reverts to prior-day VWAP should produce a
    winning long; mainly a smoke test that the strategy wires up and trades."""
    from strategies.bt_gap_fill import backtest_gap_fill
    # Day 1: flat ~100 so prior-day VWAP ≈ 100. Day 2: gap to 108, fade to ~104,
    # then rally back up through 100... but pdv≈100 < price, so no room. Instead
    # build day 2 to fade BELOW pdv then revert up to it.
    times = []
    for day in ("2026-02-02", "2026-02-03"):
        for m in range(30):               # 30 one-minute bars 09:30–09:59
            times.append(f"{day} 09:{30 + m:02d}")
    idx = pd.DatetimeIndex(times)
    # Day 1: oscillate around 100 → prior-day VWAP ≈ 100.
    d1 = 100 + np.sin(np.linspace(0, 3.14, 30))
    # Day 2: gap up to ~102 open, fade to ~98 (below pdv), then revert up to ~101.
    d2 = np.concatenate([np.linspace(102, 98, 15), np.linspace(98, 101, 15)])
    close = np.concatenate([d1, d2])
    o = close.copy()
    h = close + 0.2
    lo = close - 0.2
    v = np.full(len(close), 1000.0)
    df = pd.DataFrame({"Open": o, "High": h, "Low": lo, "Close": close, "Volume": v},
                      index=idx)
    res = backtest_gap_fill(df, gap_min_pct=0.5, warmup_min=5, min_rr=0.5,
                            eod_close_min=29)
    assert "error" not in res
    assert res["trades"] >= 1             # took the gap-fade-revert long
