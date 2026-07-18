"""Volume + 20-EMA short filter (/api/simulate/short_filter_test).

Tests whether stacking a 20-EMA + volume-surge confirmation on SHORTS filters out
losers, for BOTH kinds:
  · breakout short (break S4/S3) wants the entry bar BELOW the 20-EMA (momentum),
  · reversal short (reject R4/R3) wants it ABOVE the 20-EMA (extended / fade).
Each is bucketed baseline / +EMA / +volume / +both. These tests mock the data +
replay boundary so only the bucketing + the inverted EMA condition are under test.
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
        self.ema = close          # 8-EMA unused here (filters mocked off)
        self.ema2 = close
        self.ema20 = ema20
        self.volume = volume


def _rt(tk, entry_px, exit_px, kind="breakout", day="2026-07-06"):
    word = "BREAKOUT" if kind == "breakout" else "REVERSAL"
    return {"ticker": tk, "side": "SHORT", "strategy": f"{tk}_CAM_{word}_R4S4_V02_5MIN",
            "date": day, "entry_time": f"{day}T14:00:00+00:00", "exit_time": f"{day}T14:20:00+00:00",
            "entry_price": entry_px, "exit_price": exit_px, "qty": 10, "pnl": 0}


@pytest.fixture()
def sf(monkeypatch):
    # scenario[ticker] = (close, ema20, entry_vol, avg_vol, exit_px, kind)
    scen = {}

    def _bars(tk, dt):
        close, ema20, evol, avgvol, _exit, _kind = scen[tk.upper()]
        t0 = _dt.datetime(2026, 7, 6, 13, 0, tzinfo=_dt.timezone.utc)
        prior = [_Bar(t0 + _dt.timedelta(minutes=5 * i), 100.0, 100.0, avgvol) for i in range(20)]
        entry = _Bar(t0 + _dt.timedelta(minutes=5 * 20), close, ema20, evol)
        tail  = [_Bar(t0 + _dt.timedelta(minutes=5 * (21 + i)), close, ema20, avgvol) for i in range(3)]
        return prior + [entry] + tail

    def _exit(bars, entry, side, *ar, **kw):
        for tk, v in scen.items():
            if abs(v[0] - entry) < 1e-9:
                return {"exit_price": v[4]}
        return {"exit_price": entry}

    monkeypatch.setattr(a, "alpaca_broker", object())
    monkeypatch.setattr(a, "_alpaca_account_ctx",
                        lambda acct: (object(), "alpaca2", "TV Refined", lambda: [1]))
    monkeypatch.setattr(a, "_fetch_5m_rth_objs", _bars)
    monkeypatch.setattr(a, "_camarilla_levels", lambda tk, dt: {"s4": 90.0, "r4": 110.0})
    monkeypatch.setattr(a, "_apply_session_trail", lambda trail, dt: trail)
    monkeypatch.setattr(a, "_simulate_exit", _exit)
    monkeypatch.setattr(a, "_find_entry",
                        lambda bars, level, side, rule, buf, ema_filter=True, start=1: (20, bars[20].close))
    monkeypatch.setattr(a, "_find_reversal_entry",
                        lambda bars, level, side, rule, ema_filter=True, atr_mult=0.25, start=1, retest_bars=4:
                        (20, bars[20].close))
    a.app.config["TESTING"] = True

    def _run(scenario, qs=""):
        scen.clear(); scen.update({k.upper(): v for k, v in scenario.items()})
        trades = [_rt(tk, v[0], v[4], kind=v[5]) for tk, v in scen.items()]
        monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                            lambda *args, **kw: {"closed_clean": trades})
        with a.app.test_client() as cl:
            return cl.get("/api/simulate/short_filter_test?account=2&vol_mult=2" + qs).get_json()
    return _run


def test_breakout_buckets_partition_by_ema_and_volume(sf):
    d = sf({
        # below EMA (99<100) + surge → both. WIN.
        "AAA": (99.0, 100.0, 300, 100, 95.0, "breakout"),
        # below EMA, no surge → ema only. LOSS.
        "BBB": (99.5, 100.0, 120, 100, 102.0, "breakout"),
        # above EMA (101>100) + surge → vol only. WIN.
        "CCC": (101.0, 100.0, 300, 100, 98.0, "breakout"),
        # above EMA, no surge → baseline only. LOSS.
        "DDD": (101.5, 100.0, 100, 100, 105.0, "breakout"),
    })["breakout"]
    assert d["n_setups"] == 4
    assert d["baseline"]["trades"] == 4
    assert d["ema"]["trades"] == 2      # AAA, BBB (below EMA)
    assert d["vol"]["trades"] == 2      # AAA, CCC (surge)
    assert d["both"]["trades"] == 1     # AAA
    assert d["both"]["win_rate"] == 100.0


def test_reversal_ema_condition_is_inverted(sf):
    # For reversal shorts the EMA filter wants price ABOVE the 20-EMA (extended).
    d = sf({
        # above EMA (101>100) + surge → both. WIN.
        "AAA": (101.0, 100.0, 300, 100, 96.0, "reversal"),
        # BELOW EMA (99<100) → NOT extended → fails the reversal EMA filter.
        "BBB": (99.0, 100.0, 300, 100, 95.0, "reversal"),
    })["reversal"]
    assert d["n_setups"] == 2
    assert d["ema"]["trades"] == 1      # only AAA (above EMA)
    assert d["vol"]["trades"] == 2      # both surged
    assert d["both"]["trades"] == 1     # only AAA (above EMA AND surge)
    assert d["both"]["label"].startswith("+ both")


def test_both_kinds_run_in_one_call(sf):
    d = sf({
        "AAA": (99.0, 100.0, 300, 100, 95.0, "breakout"),
        "ZZZ": (101.0, 100.0, 300, 100, 96.0, "reversal"),
    })
    assert d["breakout"]["n_setups"] == 1
    assert d["reversal"]["n_setups"] == 1
    assert d["breakout"]["ema"]["label"] == "+ below 20-EMA"
    assert d["reversal"]["ema"]["label"] == "+ above 20-EMA (extended)"


def test_volume_sweep_tightens_the_set(sf):
    d = sf({
        "AAA": (99.0, 100.0, 300, 100, 95.0, "breakout"),   # 3× surge
        "BBB": (98.0, 100.0, 200, 100, 96.0, "breakout"),   # 2× surge
    }, qs="&vol_mults=2,3")["breakout"]
    sweep = {s["vol_mult"]: s for s in d["vol_sweep"]}
    assert sweep[2.0]["trades"] == 2
    assert sweep[3.0]["trades"] == 1


def test_missing_volume_counted_and_excluded(sf):
    d = sf({"AAA": (99.0, 100.0, 0, 0, 95.0, "breakout")})["breakout"]
    assert d["n_no_volume"] == 1
    assert d["vol"]["trades"] == 0
    assert d["both"]["trades"] == 0
    assert d["ema"]["trades"] == 1      # EMA check still works without volume


def _reversal_bars(reject_above=True, confirm_below=True, confirm_vol=500):
    """20 prior bars, a reject at idx 20 (above the 20-EMA=100), then bars that
    either close back below the EMA (confirm) or stay above (no confirm)."""
    t0 = _dt.datetime(2026, 7, 6, 13, 0, tzinfo=_dt.timezone.utc)
    b = [_Bar(t0 + _dt.timedelta(minutes=5 * i), 100.0, 100.0, 100) for i in range(20)]
    b.append(_Bar(t0 + _dt.timedelta(minutes=5 * 20), 101.0, 100.0, 100))   # reject (above EMA)
    b.append(_Bar(t0 + _dt.timedelta(minutes=5 * 21), 102.0, 100.0, 100))   # still above
    if confirm_below:
        b.append(_Bar(t0 + _dt.timedelta(minutes=5 * 22), 99.0, 100.0, confirm_vol))  # closes below → confirm
        b.append(_Bar(t0 + _dt.timedelta(minutes=5 * 23), 98.0, 100.0, 100))
    else:
        b.append(_Bar(t0 + _dt.timedelta(minutes=5 * 22), 103.0, 100.0, 100))         # stays above the EMA
        b.append(_Bar(t0 + _dt.timedelta(minutes=5 * 23), 104.0, 100.0, 100))         # ...all of it
    return b


def _wire_reversal(monkeypatch, bars, exit_px):
    monkeypatch.setattr(a, "alpaca_broker", object())
    monkeypatch.setattr(a, "_alpaca_account_ctx",
                        lambda acct: (object(), "alpaca2", "TV Refined", lambda: [1]))
    monkeypatch.setattr(a, "_fetch_5m_rth_objs", lambda tk, dt: bars)
    monkeypatch.setattr(a, "_camarilla_levels", lambda tk, dt: {"r4": 110.0, "s4": 90.0})
    monkeypatch.setattr(a, "_apply_session_trail", lambda trail, dt: trail)
    monkeypatch.setattr(a, "_find_reversal_entry",
                        lambda bars, level, side, rule, ema_filter=True, atr_mult=0.25, start=1, retest_bars=4: (20, 101.0))
    monkeypatch.setattr(a, "_simulate_exit", lambda bars, entry, side, *ar, **kw: {"exit_price": exit_px})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda *ar, **kw: {"closed_clean": [
        {"ticker": "AAA", "side": "SHORT", "strategy": "AAA_CAM_REVERSAL_R4S4_V02_5MIN",
         "date": "2026-07-06", "entry_time": "2026-07-06T14:00:00+00:00",
         "exit_time": "2026-07-06T14:20:00+00:00", "entry_price": 101.0, "exit_price": exit_px,
         "qty": 10, "pnl": 0}]})
    a.app.config["TESTING"] = True


def test_reversal_confirmation_enters_on_the_ema_break(monkeypatch):
    # Reject at idx20 (px 101), price closes below the 20-EMA at idx22 (px 99) →
    # confirmation entry at 99; exit 95 → short pnl = 99-95 = +4 (not fading at 101).
    _wire_reversal(monkeypatch, _reversal_bars(confirm_below=True, confirm_vol=500), exit_px=95.0)
    with a.app.test_client() as cl:
        c = cl.get("/api/simulate/short_filter_test?account=2&vol_mult=2").get_json()["reversal"]["confirm"]
    assert c["n_confirmed"] == 1 and c["n_no_confirm"] == 0 and c["window"] == 8
    assert c["baseline"]["trades"] == 1
    assert c["baseline"]["total_pnl"] == pytest.approx(4.0)     # entered at 99, not 101
    assert c["vol"]["trades"] == 1                              # 500 vs ~100 avg = surge


def test_reversal_that_never_breaks_below_ema_is_not_confirmed(monkeypatch):
    _wire_reversal(monkeypatch, _reversal_bars(confirm_below=False), exit_px=95.0)
    with a.app.test_client() as cl:
        c = cl.get("/api/simulate/short_filter_test?account=2&vol_mult=2").get_json()["reversal"]["confirm"]
    assert c["n_confirmed"] == 0 and c["n_no_confirm"] == 1
    assert c["baseline"]["trades"] == 0


def test_confirmation_window_sweep(monkeypatch):
    # Break happens at idx22 = 2 bars after the reject (idx20). A window < 2 misses
    # it (no confirm); a window >= 2 catches it.
    _wire_reversal(monkeypatch, _reversal_bars(confirm_below=True, confirm_vol=100), exit_px=95.0)
    with a.app.test_client() as cl:
        c = cl.get("/api/simulate/short_filter_test?account=2&confirm_windows=1,2,5").get_json()["reversal"]["confirm"]
    sweep = {row["window"]: row for row in c["window_sweep"]}
    assert set(sweep) == {1, 2, 5}
    assert sweep[1]["n_confirmed"] == 0 and sweep[1]["trades"] == 0   # break is 2 bars out
    assert sweep[2]["n_confirmed"] == 1 and sweep[2]["trades"] == 1   # caught at window 2
    assert sweep[5]["n_confirmed"] == 1                               # still caught
    assert sweep[2]["total_pnl"] == pytest.approx(4.0)                # entered 99, exit 95
