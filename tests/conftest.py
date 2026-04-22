"""Shared OHLC fixtures for backtester parity tests.

Rationale: we can't call the live Claude API in tests, but we CAN pin the
structural invariants that rules 17–21 of the bt_convert prompt enforce.
Each fixture is a deterministic OHLC DataFrame with properties we can
assert against.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


@pytest.fixture(scope="session")
def intraday_5m_ohlc() -> pd.DataFrame:
    """Five trading days of 5-minute bars, RTH only (09:30–16:00 ET).

    Each day's synthetic price walks in a known envelope so daily H/L/C
    are predictable:
        Day N close = 100 + N
        Day N high  = close + 2
        Day N low   = close - 2
    """
    frames = []
    base_close = 100.0
    for day_idx, day in enumerate(
        pd.bdate_range("2026-01-05", periods=5, tz="America/New_York")
    ):
        session = pd.date_range(
            day.replace(hour=9, minute=30),
            day.replace(hour=15, minute=55),
            freq="5min",
            tz="America/New_York",
        )
        close_target = base_close + day_idx
        n = len(session)
        t = np.linspace(0, np.pi, n)
        # Sinusoidal walk: opens near prev close, arcs up +2, back to target.
        close = close_target - 1.0 + 2.0 * np.sin(t)
        open_ = np.concatenate([[close_target - 1.0], close[:-1]])
        high = np.maximum(open_, close) + 0.25
        low = np.minimum(open_, close) - 0.25
        vol = np.full(n, 1000.0)
        frames.append(
            pd.DataFrame(
                {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": vol},
                index=session,
            )
        )
    df = pd.concat(frames)
    df.index = df.index.tz_localize(None)
    return df


@pytest.fixture(scope="session")
def crossover_fixture() -> pd.DataFrame:
    """Three bars engineered to exercise both crossover patterns.

    Bar 0: open=99, close=99  (below level 100)
    Bar 1: open=99, close=101 (crosses level 100 — both patterns should fire)
    Bar 2: open=101, close=102 (already above — neither pattern should fire)
    """
    idx = pd.date_range("2026-01-05 09:30", periods=3, freq="5min")
    return pd.DataFrame(
        {
            "Open":   [99.0,  99.0, 101.0],
            "High":   [99.5, 101.5, 102.5],
            "Low":    [98.5,  98.5, 100.5],
            "Close":  [99.0, 101.0, 102.0],
            "Volume": [1e3,   1e3,   1e3],
        },
        index=idx,
    )
