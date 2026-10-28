"""SPY-alignment gate + the what-if that prices it (and a 10:00 start)."""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as dt
from array import array

import pytest

import app as a
import routes.trade_context as tc

OPEN = dt.datetime(2026, 10, 20, 13, 30, tzinfo=dt.timezone.utc)      # 9:30 ET


def _spy(path):
    """Minute closes from 9:30 following path(minute) -> price."""
    ep, cl = array("d"), array("d")
    for m in range(0, 390):
        ep.append((OPEN + dt.timedelta(minutes=m)).timestamp()); cl.append(path(m))
    return ep, cl


def test_last_hour_move_and_the_pre_10_30_fallback():
    ep, cl = _spy(lambda m: 500 + m * 0.05)
    at_11 = (OPEN + dt.timedelta(minutes=90)).timestamp()
    assert round(tc.spy_last_hour_pct(ep, cl, at_11), 4) == round((504.5 / 501.5 - 1) * 100, 4)
    at_10 = (OPEN + dt.timedelta(minutes=30)).timestamp()            # no full hour: from the open
    assert round(tc.spy_last_hour_pct(ep, cl, at_10), 4) == round((501.5 / 500 - 1) * 100, 4)
    assert tc.spy_last_hour_pct(array("d"), array("d"), at_10) is None


@pytest.fixture()
def gate(monkeypatch):
    monkeypatch.setattr(a, "SPY_ALIGN_GATE_ENABLED", True)
    monkeypatch.setattr(a, "SPY_ALIGN_MIN_PCT", 0.15)
    monkeypatch.setattr(a, "_account_gate_overrides", lambda tag=None: {})
    state = {"path": lambda m: 500 + m * 0.02}                       # +0.24%/hour, rising

    def _mins(now):
        return _spy(state["path"])
    monkeypatch.setattr(a, "_live_spy_minutes", _mins)
    return state


def test_gate_blocks_a_long_into_a_flat_spy_and_passes_a_long_into_a_rising_one(gate):
    now = OPEN + dt.timedelta(minutes=120)
    B = "MU_CAM_BREAKOUT_R3S3_V02_5MIN"
    blk, why, mv = a._spy_align_gate_block(B, "long", "alpaca3", now_dt=now)
    assert not blk and mv > 0.15
    blk, why, mv = a._spy_align_gate_block(B, "short", "alpaca3", now_dt=now)  # SPY up: against a short
    assert blk and "SPY-alignment gate" in why and mv < 0
    gate["path"] = lambda m: 500.0                                    # flat
    assert a._spy_align_gate_block(B, "long", "alpaca3", now_dt=now)[0]


def test_gate_only_touches_refined_breakouts_and_fails_open(gate, monkeypatch):
    now = OPEN + dt.timedelta(minutes=120)
    gate["path"] = lambda m: 500.0
    assert not a._spy_align_gate_block("MU_CAM_REVERSAL_R3S3_V02_5MIN", "long", "alpaca3", now_dt=now)[0]
    assert not a._spy_align_gate_block("MU_CAM_BREAKOUT_R3S3_V02_5MIN", "long", "alpaca5", now_dt=now)[0]  # farm
    assert not a._spy_align_gate_block("MU_CAM_BREAKOUT_R3S3_V02_5MIN", "", "alpaca3", now_dt=now)[0]
    monkeypatch.setattr(a, "_live_spy_minutes", lambda now: (array("d"), array("d")))
    assert not a._spy_align_gate_block("MU_CAM_BREAKOUT_R3S3_V02_5MIN", "long", "alpaca3", now_dt=now)[0]
    monkeypatch.setattr(a, "SPY_ALIGN_GATE_ENABLED", False)
    gate["path"] = lambda m: 500.0
    monkeypatch.setattr(a, "_live_spy_minutes", lambda now: _spy(gate["path"]))
    assert not a._spy_align_gate_block("MU_CAM_BREAKOUT_R3S3_V02_5MIN", "long", "alpaca3", now_dt=now)[0]


def test_a_per_book_override_wins(gate, monkeypatch):
    now = OPEN + dt.timedelta(minutes=120)
    monkeypatch.setattr(a, "_account_gate_overrides",
                        lambda tag=None: {"spy_align": {"enabled": True, "min": 0.5}} if tag == "alpaca4" else {})
    blk, why, _mv = a._spy_align_gate_block("MU_CAM_BREAKOUT_R3S3_V02_5MIN", "long", "alpaca4", now_dt=now)
    assert blk and "0.50%" in why                                     # +0.24% < 0.5% on the override
    assert a._gate_state("alpaca4")["spy-align"][2] == "override"


def test_gate_is_wired_to_both_refined_books_only():
    assert a.SPY_ALIGN_GATE_ACCOUNTS == {"alpaca2", "alpaca3"}
    assert "spy-align" in a._GATE_RULES


def test_settings_round_trip(monkeypatch):
    saved = {}
    monkeypatch.setattr(a, "_save_setting", lambda k, v: saved.__setitem__(k, v))
    monkeypatch.setattr(a, "_update_env_file", lambda k, v: None)
    monkeypatch.setattr(a, "SPY_ALIGN_GATE_ENABLED", False)
    monkeypatch.setattr(a, "SPY_ALIGN_MIN_PCT", 0.15)
    c = a.app.test_client()
    r = c.post("/api/risk/limit", json={"spy_align_gate_enabled": True, "spy_align_min_pct": 0.2})
    assert r.status_code == 200, r.get_json()
    assert saved["SPY_ALIGN_GATE_ENABLED"] == "1" and saved["SPY_ALIGN_MIN_PCT"] == "0.2"
    d = c.get("/api/risk/status").get_json()
    assert d["spy_align_gate_enabled"] is True and d["spy_align_min_pct"] == 0.2
    assert c.post("/api/risk/limit", json={"spy_align_min_pct": 5}).status_code == 400


# ── what-if ───────────────────────────────────────────────────────────────────

def _r(pct, d, hhmm, spy_dir, breakout=True):
    return {"pct": pct, "pnl": pct * 10, "date": d, "hhmm": hhmm, "breakout": breakout, "spy_dir": spy_dir}


def test_whatif_blocks_by_the_rule_and_reports_savings_by_half():
    rows = [_r(-0.2, "2026-09-01", 945, 0.0),          # flat SPY + early: both rules block it
            _r(0.3, "2026-09-02", 1030, 0.4),          # SPY with it: kept by everything
            _r(-0.1, "2026-10-01", 1015, -0.2),        # SPY against: SPY rule blocks
            _r(0.1, "2026-10-02", 935, 0.5),           # early but SPY with it: start rule blocks
            _r(-0.5, "2026-10-03", 1040, None),        # no SPY data: kept (fails open)
            _r(-0.4, "2026-10-04", 1045, 0.0, breakout=False)]   # reversal: SPY rule ignores it
    w = tc.whatif_scenarios(rows, 0.15)
    spy15 = next(x for x in w["spy"] if x["threshold"] == 0.15)
    assert spy15["blocked"]["n"] == 2 and spy15["saved_pct"] == 0.3
    assert w["start"]["blocked"]["n"] == 2 and w["start"]["saved_pct"] == 0.1
    assert w["both"]["blocked"]["n"] == 3 and w["both"]["saved_pct"] == 0.2
    assert w["unknown_spy"] == 1 and w["all"]["n"] == 6
    assert w["split_date"] == "2026-10-02"
    assert spy15["saved_older_pct"] == 0.3 and spy15["saved_newer_pct"] == 0.0


def test_whatif_endpoint_on_fake_books(monkeypatch):
    def rt(strategy, entry, pnl, side="LONG"):
        return {"strategy": strategy, "ticker": strategy.split("_")[0], "side": side, "entry_time": entry,
                "exit_time": entry, "entry_price": 100.0, "qty": 10.0, "pnl": pnl}
    book = [rt("MU_CAM_BREAKOUT_R3S3_V02_5MIN", "2026-10-20T13:45:00Z", -5)]
    farm = [rt("MU_CAM_BREAKOUT_R3S3_V02_5MIN", "2026-10-20T14:10:00Z", 3),
            rt("MU_CAM_BREAKOUT_R3S3_V02_5MIN", "2026-10-20T17:30:00Z", 9),      # outside 9:30-11:00
            rt("MU_CAM_REVERSAL_R3S3_V02_5MIN", "2026-10-20T14:10:00Z", 1)]      # not a breakout
    monkeypatch.setattr(tc, "_round_trips", lambda a_, acct, f, t: ((book if acct == "3" else farm), []))
    monkeypatch.setattr(tc, "_day_minutes", lambda tk, d: (*_spy(lambda m: 500.0), 0.0))
    monkeypatch.setattr(a, "_account_hours_windows", lambda tag: [("09:30", "11:00")])
    tc._results.d.clear()
    d = a.app.test_client().get("/api/gates/what_if?book=3").get_json()
    assert d["label"] == "Kairos Refined" and d["hours"] == ["09:30-11:00"]
    assert d["book_trades"]["all"]["n"] == 1 and d["farm_trades"]["all"]["n"] == 1
    spy15 = next(x for x in d["book_trades"]["spy"] if x["threshold"] == 0.15)
    assert spy15["blocked"]["n"] == 1                       # flat SPY blocks the early long
    assert d["book_trades"]["start"]["blocked"]["n"] == 1   # 9:45 ET is before 10:00
    assert a.app.test_client().get("/api/gates/what_if?book=9").status_code == 400


def test_routing_page_has_the_card():
    html = open("templates/routing.html", encoding="utf-8").read()
    assert 'id="spyAlignCard"' in html and "runGateWhatIf('3', this)" in html and "setSpyAlignGate(true)" in html
