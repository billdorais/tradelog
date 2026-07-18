"""Volume + 20-EMA short filter (/api/simulate/short_filter_test).

Tests whether stacking a 20-EMA + volume-surge confirmation on breakout SHORTS
filters out losers. Each breakout short is bucketed baseline / +EMA / +volume /
+both by the ENTRY BAR's 20-EMA position and volume vs its trailing average. These
tests mock the data + replay boundary so only the bucketing logic is under test.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as _dt

import pytest

import app as a


class _Bar:
    def __init__(self, ts, close, ema20, volume):
        self.timestamp = ts
        self.open = self.high = self.low = self.close = close
        self.ema20 = ema20
        self.volume = volume


def _rt(tk, entry_px, exit_px, day="2026-07-06"):
    return {"ticker": tk, "side": "SHORT", "strategy": f"{tk}_CAM_BREAKOUT_R4S4_V02_5MIN",
            "date": day, "entry_time": f"{day}T14:00:00+00:00", "exit_time": f"{day}T14:20:00+00:00",
            "entry_price": entry_px, "exit_price": exit_px, "qty": 10, "pnl": 0}


@pytest.fixture()
def sf(monkeypatch):
    """Per-ticker control of the entry bar's 20-EMA position, volume, and P&L.

    scenario[ticker] = (close, ema20, entry_vol, trailing_avg_vol, exit_px)
    Entry bar is index = vol_lookback (20) so there are enough prior bars for the
    trailing average; prior bars all carry `trailing_avg_vol` volume.
    """
    scen = {}

    def _bars(tk, dt):
        close, ema20, evol, avgvol, _exit = scen[tk.upper()]
        t0 = _dt.datetime(2026, 7, 6, 13, 0, tzinfo=_dt.timezone.utc)
        prior = [_Bar(t0 + _dt.timedelta(minutes=5 * i), 100.0, 100.0, avgvol) for i in range(20)]
        entry = _Bar(t0 + _dt.timedelta(minutes=5 * 20), close, ema20, evol)
        tail  = [_Bar(t0 + _dt.timedelta(minutes=5 * (21 + i)), close, ema20, avgvol) for i in range(3)]
        return prior + [entry] + tail

    def _find_entry(bars, level, side, rule, buf, ema_filter=True, start=1):
        return (20, bars[20].close)   # entry at the crafted entry bar

    monkeypatch.setattr(a, "alpaca_broker", object())
    monkeypatch.setattr(a, "_alpaca_account_ctx",
                        lambda acct: (object(), "alpaca2", "TV Refined", lambda: [1]))
    monkeypatch.setattr(a, "_fetch_5m_rth_objs", _bars)
    monkeypatch.setattr(a, "_camarilla_levels", lambda tk, dt: {"s4": 90.0, "r4": 110.0})
    monkeypatch.setattr(a, "_trade_level", lambda strat, side: "S4")
    monkeypatch.setattr(a, "_apply_session_trail", lambda trail, dt: trail)
    monkeypatch.setattr(a, "_find_entry", _find_entry)
    monkeypatch.setattr(a, "_simulate_exit",
                        lambda bars, entry, side, *ar, **kw: {"exit_price": scen[_cur["tk"]][4]})
    a.app.config["TESTING"] = True
    _cur = {"tk": None}

    # _simulate_exit needs to know which ticker; stash it via the bars fetch order.
    # Simpler: encode exit in the entry via a per-ticker lookup keyed by entry price.
    def _exit(bars, entry, side, *ar, **kw):
        for tk, v in scen.items():
            if abs(v[0] - entry) < 1e-9:
                return {"exit_price": v[4]}
        return {"exit_price": entry}
    monkeypatch.setattr(a, "_simulate_exit", _exit)

    def _run(scenario, qs=""):
        scen.clear(); scen.update({k.upper(): v for k, v in scenario.items()})
        trades = [_rt(tk, v[0], v[4]) for tk, v in scen.items()]
        monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                            lambda *args, **kw: {"closed_clean": trades})
        with a.app.test_client() as cl:
            return cl.get("/api/simulate/short_filter_test?account=2&vol_mult=2" + qs).get_json()
    return _run


def test_buckets_partition_by_ema_and_volume(sf):
    # close, ema20, entry_vol, avg_vol, exit_px  (SHORT pnl = entry - exit)
    d = sf({
        # below EMA (99<100) + surge (300>=2*100) → both. WIN (exit 95 < entry 99).
        "AAA": (99.0, 100.0, 300, 100, 95.0),
        # below EMA, NO surge (120<200) → ema only. LOSS (exit 102 > 99).
        "BBB": (99.5, 100.0, 120, 100, 102.0),
        # above EMA (101>100) + surge → vol only. WIN.
        "CCC": (101.0, 100.0, 300, 100, 98.0),
        # above EMA, no surge → baseline only. LOSS.
        "DDD": (101.5, 100.0, 100, 100, 105.0),
    })
    assert d["n_setups"] == 4
    assert d["baseline"]["trades"] == 4
    assert d["ema"]["trades"] == 2      # AAA, BBB
    assert d["vol"]["trades"] == 2      # AAA, CCC
    assert d["both"]["trades"] == 1     # AAA only
    # The +both trade (AAA) is the winner.
    assert d["both"]["win_rate"] == 100.0
    assert d["both"]["total_pnl"] == pytest.approx(4.0)   # 99 - 95


def test_both_filter_lifts_win_rate(sf):
    # Distinct entry prices (the exit mock keys on them). Baseline mixes wins and
    # losses; +both keeps only the surge-below-EMA winners.
    d = sf({
        "AAA": (99.0, 100.0, 300, 100, 94.0),   # both, WIN (99-94=+5)
        "BBB": (98.0, 100.0, 300, 100, 95.0),   # both, WIN (98-95=+3)
        "CCC": (99.5, 100.0, 100, 100, 108.0),  # ema only (no surge), LOSS
        "DDD": (101.0, 100.0, 100, 100, 110.0), # baseline only, LOSS
    })
    assert d["baseline"]["win_rate"] == 50.0
    assert d["both"]["trades"] == 2 and d["both"]["win_rate"] == 100.0


def test_volume_sweep_tightens_the_set(sf):
    # AAA surges 3×, BBB surges 2× — a 3× threshold keeps only AAA.
    d = sf({
        "AAA": (99.0, 100.0, 300, 100, 95.0),
        "BBB": (98.0, 100.0, 200, 100, 96.0),
    }, qs="&vol_mults=2,3")
    sweep = {s["vol_mult"]: s for s in d["vol_sweep"]}
    assert sweep[2.0]["trades"] == 2    # both clear 2×
    assert sweep[3.0]["trades"] == 1    # only AAA clears 3×


def test_missing_volume_counted_and_excluded_from_vol_buckets(sf):
    # Zero volume → can't evaluate the surge; counts n_no_volume, stays out of +vol/+both.
    d = sf({"AAA": (99.0, 100.0, 0, 0, 95.0)})
    assert d["n_no_volume"] == 1
    assert d["vol"]["trades"] == 0
    assert d["both"]["trades"] == 0
    assert d["ema"]["trades"] == 1      # EMA check still works without volume
