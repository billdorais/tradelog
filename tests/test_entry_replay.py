"""Entry replay: entry-timing rules on ticks, each exited with the live policy."""
import sqlite3
from datetime import datetime

import numpy as np
import pytest

import routes.entry_replay as en
import routes.exit_replay as er

ET = er._ET
D0 = datetime(2026, 10, 6, 10, 0, 0, tzinfo=ET).timestamp()   # bar 0 starts 10:00 ET


def bars(rows, ema=None, atr=0.4):
    """rows: [(o, h, l, c)] for consecutive 5-min bars from 10:00 ET."""
    n = len(rows)
    a = np.array(rows, dtype=float)
    e = np.full(n, 99.0) if ema is None else np.array(ema, dtype=float)
    return en.Bars(D0 + 300 * np.arange(n), a[:, 0], a[:, 1], a[:, 2], a[:, 3], e, e - 0.1, np.full(n, atr))


def ticks(pairs):
    t = np.array([D0 + s for s, _ in pairs], dtype=float)
    return t, np.array([p for _, p in pairs], dtype=float)


L = 100.0
RULE = {"trail": 0.2, "trigger": 0.0, "stop": None, "max_hold": 10}


def test_pierce_enters_inside_the_bar_close_waits_for_the_close():
    b = bars([(99.5, 99.9, 99.4, 99.8), (99.8, 100.6, 99.8, 100.5), (100.5, 100.9, 100.4, 100.8)])
    # bar 1 (300–600 s) trades through 100.05 at 340 s and closes at 100.5
    t, p = ticks([(0, 99.6), (305, 99.9), (335, 100.02), (340, 100.10), (400, 100.3), (599, 100.5),
                  (601, 100.52), (700, 100.8), (900, 100.85)])
    pierce = en.find_breakout(b, t, p, L, "pierce", b.bt[0])
    close = en.find_breakout(b, t, p, L, "close", b.bt[0])
    assert pierce["t"] == D0 + 400 and pierce["bar"] == 1        # poll at 350 sees 100.10 → next tick
    assert pierce["closed_back"] is False and pierce["ref"] == pytest.approx(100.10)
    assert close["t"] == D0 + 601 and close["px"] == 100.52     # bar 1 closes at 600, +1 s latency


def test_a_false_pierce_is_flagged_and_the_abort_exits_at_the_bar_close():
    b = bars([(99.5, 99.9, 99.4, 99.8), (99.8, 100.3, 99.5, 99.7), (99.7, 99.8, 99.2, 99.3)])
    t, p = ticks([(0, 99.6), (305, 99.9), (345, 100.2), (360, 100.1), (500, 99.8), (599, 99.7),
                  (601, 99.68), (700, 99.4), (1200, 99.2)])
    e = en.find_breakout(b, t, p, L, "pierce", b.bt[0])
    assert e["closed_back"] is True and e["abort_t"] is None
    assert en.find_breakout(b, t, p, L, "close", b.bt[0]) is None   # never closed above
    e2 = en.find_breakout(b, t, p, L, "pierce_abort", b.bt[0])
    assert e2["abort_t"] == D0 + 601
    wide = {"trail": 2.0, "trigger": 0.0, "stop": None, "max_hold": 10}
    rows = en.simulate_setup(b, t, p, L, "LONG", "breakout", "pierce_abort", wide, 0.0)
    assert rows[0]["exit"] == "abort" and rows[0]["exit_t"] == D0 + 601
    held = en.simulate_setup(b, t, p, L, "LONG", "breakout", "pierce", wide, 0.0)
    assert held[0]["exit"] == "max_hold" and held[0]["pnl_pct"] < rows[0]["pnl_pct"]


def test_hold_requires_price_to_stay_past_the_trigger():
    b = bars([(99.5, 99.9, 99.4, 99.8), (99.8, 100.3, 99.5, 100.2), (100.2, 100.5, 100.1, 100.4)])
    t, p = ticks([(0, 99.6), (305, 99.9), (345, 100.2), (365, 99.98), (380, 100.2), (599, 100.2), (700, 100.4)])
    e = en.find_breakout(b, t, p, L, "pierce_hold30", b.bt[0])
    assert e is None or e["bar"] >= 2            # the dip at 365 fails the hold inside bar 1
    assert en.find_breakout(b, t, p, L, "pierce", b.bt[0])["bar"] == 1


def test_percent_trigger_scales_with_price():
    b = bars([(99.5, 99.9, 99.4, 99.8), (99.8, 100.3, 99.8, 100.2), (100.2, 100.5, 100.1, 100.4)])
    t, p = ticks([(0, 99.6), (305, 99.9), (345, 100.06), (420, 100.12), (599, 100.2), (700, 100.4)])
    usd = en.find_breakout(b, t, p, L, "pierce", b.bt[0])           # $0.05 trigger: fires at the 350 s poll
    pct = en.find_breakout(b, t, p, L, "pierce_pct10", b.bt[0])     # 0.10% = $0.10: waits for 100.12
    assert usd["ref"] == pytest.approx(100.06) and pct["ref"] == pytest.approx(100.12) and pct["t"] > usd["t"]


def test_shorts_mirror_longs():
    # The long case mirrored around 100: a break DOWN through support at 100.
    b = bars([(100.5, 100.6, 100.1, 100.2), (100.2, 100.2, 99.4, 99.5), (99.5, 99.6, 99.1, 99.2)], ema=[101] * 3)
    t, p = ticks([(0, 100.4), (305, 100.1), (335, 99.98), (340, 99.90), (400, 99.7), (599, 99.5),
                  (601, 99.48), (700, 99.2), (900, 99.15)])
    rows = en.simulate_setup(b, t, p, L, "SHORT", "breakout", "close", RULE, 0.0)
    assert rows[0]["t"] == D0 + 601 and rows[0]["entry_px"] == 99.48 and rows[0]["side"] == "SHORT"
    pr = en.simulate_setup(b, t, p, L, "SHORT", "breakout", "pierce", RULE, 0.0)
    assert pr[0]["t"] == D0 + 400 and pr[0]["closed_back"] is False


def test_reversal_reject_and_double_bottom():
    # support at 100: bar1 wicks to 99.8 and closes 100.3 (a reject); rally to 101.0 (the
    # neckline); bar4 retests to 99.95 and closes 100.2; bar5 breaks the neckline.
    rows = [(100.6, 100.8, 100.4, 100.5), (100.5, 100.6, 99.8, 100.3), (100.3, 101.0, 100.2, 100.9),
            (100.9, 100.95, 100.4, 100.5), (100.5, 100.6, 99.95, 100.2), (100.2, 101.3, 100.2, 101.2),
            (101.2, 101.5, 101.0, 101.4)]
    b = bars(rows, ema=[100.0, 100.1, 100.2, 100.25, 100.1, 100.3, 100.5], atr=0.4)
    t, p = ticks([(0, 100.6), (420, 99.8), (599, 100.3), (601, 100.31), (700, 100.9), (1300, 99.95),
                  (1499, 100.2), (1505, 100.4), (1600, 101.01), (1650, 101.2), (1799, 101.2), (1801, 101.21),
                  (2000, 101.4)])
    rej = en.find_reversal(b, t, p, L, "reject", b.bt[0])
    assert rej["bar"] == 1 and rej["t"] == D0 + 601
    dbl = en.find_reversal(b, t, p, L, "double", b.bt[0])
    assert dbl["bar"] == 4 and dbl["t"] == D0 + 1600 and dbl["ref"] == pytest.approx(101.0)
    dbc = en.find_reversal(b, t, p, L, "double_close", b.bt[0])
    assert dbc["bar"] == 5 and dbc["t"] == D0 + 1801
    touch = en.find_reversal(b, t, p, L, "touch", b.bt[0])
    assert touch["px"] == L and touch["t"] == D0 + 420


def test_cooldown_and_one_position_at_a_time():
    b = bars([(99.5, 99.9, 99.4, 99.8)] + [(99.8, 100.6, 99.5, 99.7), (99.7, 99.9, 99.5, 99.6)] * 6)
    pairs = [(0, 99.6)]
    for k in range(6):                       # a false pierce every 10 minutes
        s = 300 + 600 * k
        pairs += [(s + 5, 99.9), (s + 40, 100.2), (s + 200, 99.7), (s + 590, 99.6)]
    t, p = ticks(pairs)
    rows = en.simulate_setup(b, t, p, L, "LONG", "breakout", "pierce", RULE, 0.0)
    gaps = np.diff([r["t"] for r in rows])
    assert len(rows) >= 2 and np.all(gaps >= en.COOLDOWN_MIN * 60)


def test_summarize_splits_pierce_into_shared_and_only():
    def mk(v, bar, pnl, cb=None):
        return {"variant": v, "strategy": "X", "day": "2026-10-06", "side": "LONG", "bar": bar,
                "pnl_pct": pnl, "band": "BREAKOUT R3S3", "pre10": False, "closed_back": cb}
    s = en.summarize([mk("close", 1, 0.1), mk("pierce", 1, 0.2, False), mk("pierce", 5, -0.3, True)])
    assert s["_pierce_vs_close"]["pierce_shared"]["mean_pct"] == 0.2
    assert s["_pierce_vs_close"]["pierce_only"]["mean_pct"] == -0.3
    assert s["pierce"]["closed_back"]["trades"] == 1 and s["pierce"]["held"]["trades"] == 1


def test_windows_cover_bars_near_a_level_and_merge():
    far = bars([(95.0, 95.2, 94.9, 95.1)] * 40)
    assert en._windows(far, {100.0}, 10) == []
    near = bars([(99.6, 99.8, 99.5, 99.7), (99.7, 100.1, 99.6, 100.0)] + [(95.0, 95.2, 94.9, 95.1)] * 40)
    w = en._windows(near, {100.0}, 10)
    assert len(w) == 1 and w[0][0] == D0 - 60


@pytest.fixture()
def client(tmp_path, monkeypatch):
    import app as a
    db = tmp_path / "er.db"
    monkeypatch.setattr(a, "get_db", lambda: sqlite3.connect(db))
    monkeypatch.setattr(a, "placeholder", lambda: "?")
    monkeypatch.setattr(er, "_market_open", lambda now=None: False)
    started = []

    def _fake(run_id, *args):
        en._save(run_id, status="done", progress="done", result='{"entries": 0, "rows": [1], "summary": {}}')
        er._job_lock.release()
        started.append(args)
    monkeypatch.setattr(en, "run_entry_replay", _fake)
    return a.app.test_client(), started


def test_run_endpoint_validates_and_starts(client):
    c, started = client
    body = {"from": "2026-10-01", "to": "2026-10-31", "strategies": ["NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"]}
    assert c.post("/api/entry_replay/run", json={**body, "to": "2026-12-31"}).status_code == 400
    assert c.post("/api/entry_replay/run", json={**body, "strategies": []}).status_code == 400
    assert c.post("/api/entry_replay/run", json={**body, "strategies": ["SPY_ORB"]}).status_code == 400
    assert c.post("/api/entry_replay/run", json={**body, "variants": ["bogus"]}).status_code == 400
    r = c.post("/api/entry_replay/run", json={**body, "variants": ["close", "pierce"]})
    assert r.status_code == 200
    rid = r.get_json()["run_id"]
    import time as _t
    for _ in range(50):
        if started:
            break
        _t.sleep(0.02)
    assert started[0][2] == ["NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"] and started[0][3] == ["close", "pierce"]
    d = c.get(f"/api/entry_replay/runs/{rid}").get_json()
    assert d["status"] == "done" and "rows" not in d["result"]
    assert c.get(f"/api/entry_replay/runs/{rid}?rows=1").get_json()["result"]["rows"] == [1]
    assert c.get("/api/entry_replay/runs").get_json()["runs"][0]["id"] == rid


def test_a_close_with_no_print_until_a_later_window_is_not_a_fill():
    """Bar 1 closes above L but its ticks were never fetched (a gap bar outside every
    window). The next print is two hours on, in another window: that is not where a
    market order at the close would fill, so there is no entry on this bar."""
    b = bars([(99.5, 99.9, 99.4, 99.8), (99.8, 100.6, 99.8, 100.5), (100.5, 100.9, 100.4, 100.8)])
    t, p = ticks([(0, 99.6), (7200, 103.0)])
    assert en.find_breakout(b, t, p, L, "close", b.bt[0]) is None
    # A print within the fill bound is the fill.
    t, p = ticks([(0, 99.6), (605, 100.52), (7200, 103.0)])
    assert en.find_breakout(b, t, p, L, "close", b.bt[0])["t"] == D0 + 605
