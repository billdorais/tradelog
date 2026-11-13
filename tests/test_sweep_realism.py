"""Trail sweep realism knobs: exit cost and the entry minute (opt-in; defaults unchanged)."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import app as a

T0 = datetime(2026, 10, 6, 14, 5, 0, tzinfo=timezone.utc)   # 10:05 ET


def bar(minute, o, h, l, c):
    return SimpleNamespace(timestamp=T0 + timedelta(minutes=minute), open=o, high=h, low=l, close=c)


# ── _entry_minute_exit ─────────────────────────────────────────────────────────

def test_entry_minute_close_vs_extreme_long():
    b = bar(0, 100.0, 100.05, 99.85, 99.95)        # touched 99.85, closed 99.95
    # 0.1% trail from 100 → 99.90: touched but did not close through
    assert a._entry_minute_exit(b, "close", 100.0, True, 0.1, 0.0, 0, None) is None
    assert a._entry_minute_exit(b, "extreme", 100.0, True, 0.1, 0.0, 0, None) == ("trail", pytest.approx(99.9))
    b2 = bar(0, 100.0, 100.02, 99.80, 99.85)       # closed through
    assert a._entry_minute_exit(b2, "close", 100.0, True, 0.1, 0.0, 0, None) == ("trail", pytest.approx(99.9))


def test_entry_minute_short_and_trigger():
    b = bar(0, 100.0, 100.2, 99.9, 100.15)
    assert a._entry_minute_exit(b, "close", 100.0, False, 0.1, 0.0, 0, None) == ("trail", pytest.approx(100.1))
    # A trail that only arms after +0.1% can't fire in the entry minute; a % stop still can.
    assert a._entry_minute_exit(b, "extreme", 100.0, False, 0.54, 0.1, 0, None) is None
    assert a._entry_minute_exit(b, "extreme", 100.0, False, 0.54, 0.1, 0.15, None) == ("stop_loss", pytest.approx(100.15))


def test_simulate_exit_default_ignores_entry_bar():
    later = [bar(1, 100.0, 100.5, 99.95, 100.4), bar(2, 100.4, 100.45, 100.0, 100.05)]
    eb = bar(0, 100.0, 100.0, 99.5, 99.6)
    base = a._simulate_exit(later, 100.0, "LONG", 0.1, 0.0, 0.0, 0, T0 + timedelta(seconds=20))
    same = a._simulate_exit(later, 100.0, "LONG", 0.1, 0.0, 0.0, 0, T0 + timedelta(seconds=20),
                            entry_bar=eb, entry_bar_mode="skip")
    assert base == same
    hit = a._simulate_exit(later, 100.0, "LONG", 0.1, 0.0, 0.0, 0, T0 + timedelta(seconds=20),
                           entry_bar=eb, entry_bar_mode="close")
    assert hit["reason"] == "trail" and hit["exit_price"] == pytest.approx(99.9)
    assert hit["exit_time"] == str(T0 + timedelta(minutes=1))


# ── /api/simulate/sweep end to end with stubbed fills and bars ────────────────

@pytest.fixture()
def sweep_client(monkeypatch):
    entry = T0 + timedelta(seconds=20)
    trade = {"ticker": "NVDA", "side": "LONG", "entry_price": 100.0, "qty": 10,
             "entry_time": entry.isoformat(), "exit_time": (T0 + timedelta(seconds=40)).isoformat(),
             "exit_price": 99.88, "pnl": -1.2, "strategy": "NVDA_CAM_BREAKOUT_R4S4_V02_5MIN"}
    bars = [bar(0, 100.0, 100.02, 99.80, 99.85),          # entry minute: closes through 99.90
            bar(1, 99.95, 100.6, 99.95, 100.55),          # then rallies without retesting 99.90
            bar(2, 100.55, 100.6, 100.0, 100.05)]
    monkeypatch.setattr(a, "_alpaca_account_ctx", lambda acct: (object(), "t", "Test", lambda: []))
    monkeypatch.setattr(a, "_build_signal_lookup_for_alpaca", lambda: {})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda *x, **k: {"closed_clean": [dict(trade)]})
    monkeypatch.setattr(a, "_fetch_day_bars", lambda tk, dt: bars)

    def _no_db():
        raise RuntimeError("no rules in this test")
    monkeypatch.setattr(a, "get_db", _no_db)
    return a.app.test_client()


def _post(client, **extra):
    body = {"from_date": "2026-10-06", "to_date": "2026-10-06", "account": "5",
            "trail_min": 0.1, "trail_max": 0.1, "trail_step": 0.05, "triggers": [0], "max_hold_mins": 10}
    body.update(extra)
    return client.post("/api/simulate/sweep", json=body)


def test_sweep_defaults_unchanged_and_reports_actual(sweep_client):
    d = _post(sweep_client).get_json()
    assert d["entry_minute"] == "skip" and d["exit_cost_pct"] == 0 and d["actual_total"] == -1.2
    # Skipping the entry minute, the trade rides the rally and the trail exits in profit.
    assert d["results"][0]["total_pnl"] > 0


def test_sweep_entry_minute_and_exit_cost(sweep_client):
    d = _post(sweep_client, entry_minute="close").get_json()
    assert d["results"][0]["total_pnl"] == pytest.approx(-1.0)          # stopped at 99.90 × 10
    c = _post(sweep_client, entry_minute="close", exit_cost_pct=0.02).get_json()
    assert c["results"][0]["total_pnl"] == pytest.approx(-1.2, abs=0.01)  # 99.90 less 0.02% ≈ 99.88
    p = _post(sweep_client, entry_minute="close", per_strategy=True).get_json()
    s = p["strategies"][0]
    assert s["actual_pnl"] == -1.2 and len(s["grid"]) == 1 and p["actual_total"] == -1.2


def test_sweep_rejects_bad_entry_minute(sweep_client):
    assert _post(sweep_client, entry_minute="sometimes").status_code == 400


def test_entry_minute_exit_yields_to_an_earlier_actual_exit():
    """With skip_tv_exits off the baseline anchors on the real exit (cap_dt). The
    entry-minute exit returned a trail exit at the end of the bar even when the trade
    had already exited inside that minute, and exit_cost was charged on it."""
    later = [bar(1, 99.6, 99.7, 99.5, 99.6)]
    eb = bar(0, 100.0, 100.0, 99.5, 99.6)                  # closed through the 0.1% trail
    entry = T0 + timedelta(seconds=20)
    early = a._simulate_exit(later, 100.0, "LONG", 0.1, 0.0, 0.0, 0, entry,
                             cap_dt=T0 + timedelta(seconds=40), cap_price=99.88,
                             entry_bar=eb, entry_bar_mode="close")
    assert early["reason"] == "actual" and early["exit_price"] == pytest.approx(99.88)
    assert early["exit_time"] == str(T0 + timedelta(seconds=40))
    # A real exit AFTER the entry minute leaves the entry-minute exit in place.
    late = a._simulate_exit(later, 100.0, "LONG", 0.1, 0.0, 0.0, 0, entry,
                            cap_dt=T0 + timedelta(seconds=90), cap_price=99.6,
                            entry_bar=eb, entry_bar_mode="close")
    assert late["reason"] == "trail" and late["exit_time"] == str(T0 + timedelta(minutes=1))
