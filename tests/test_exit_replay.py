"""Exit replay: tick engine, live-policy model, order pairing, scoring, endpoints."""
import sqlite3
from datetime import datetime

import numpy as np
import pytest

import routes.exit_replay as er

ET = er._ET
T0 = datetime(2026, 10, 6, 10, 5, 0, tzinfo=ET).timestamp()     # a Tuesday, 10:05 ET


def ticks(*pairs):
    t = np.array([T0 + s for s, _ in pairs], dtype=float)
    p = np.array([px for _, px in pairs], dtype=float)
    return t, p


# ── replay_orders ──────────────────────────────────────────────────────────────

def test_trailing_stop_long_tracks_high_water_mark():
    t, p = ticks((0, 100.0), (1, 100.2), (2, 100.3), (3, 100.21), (4, 100.19), (5, 100.5))
    hit = er.replay_orders(t, p, True, [{"kind": "trail", "placed": T0 + 0.5, "ended": None, "trail_pct": 0.1}])
    # HWM 100.3 → trigger at ≤ 100.1997; 100.21 doesn't, 100.19 does.
    assert hit == (T0 + 4, 100.19, "trail")


def test_trailing_stop_short_and_hwm_starts_at_placement_price():
    t, p = ticks((0, 50.0), (1, 49.9), (2, 49.96), (3, 49.8))
    # Placed at t=1.5: low-water mark starts at the last price, 49.9 → trigger ≥ 49.9499.
    hit = er.replay_orders(t, p, False, [{"kind": "trail", "placed": T0 + 1.5, "ended": None, "trail_pct": 0.1}])
    assert hit == (T0 + 2, 49.96, "trail")


def test_stop_order_window_and_earliest_wins():
    t, p = ticks((0, 100.0), (1, 99.8), (2, 99.5))
    orders = [{"kind": "stop", "placed": T0, "ended": T0 + 0.5, "stop_price": 99.85},   # canceled before 99.8
              {"kind": "stop", "placed": T0, "ended": None, "stop_price": 99.6}]
    assert er.replay_orders(t, p, True, orders) == (T0 + 2, 99.5, "stop")
    assert er.replay_orders(t, p, True, orders, end_t=T0 + 1.5) is None


# ── replay_policy / policy_for ─────────────────────────────────────────────────

def trade(**kw):
    tr = {"is_long": True, "fill_t": T0, "fill_px": 100.0, "qty": 10, "ref": 99.95, "slow_fill": False,
          "place_delay": 0.5, "deadline": T0 + 600, "deadline_kind": "max_hold"}
    tr.update(kw)
    return tr


def test_policy_for_mirrors_broker_paths():
    pol = er.policy_for(trade(), 0.1, 0.1)
    assert pol["mode"] == "trigger"
    assert pol["stop_px"] == pytest.approx(99.95 * 0.999)              # stop = trail distance from the signal price
    assert pol["trigger_px"] == pytest.approx(100.0 + 99.95 * 0.001)   # unrealized ≥ ref × trigger%
    assert er.policy_for(trade(), 0.3, 0.1, stop_pct=0.5)["stop_px"] == pytest.approx(99.95 * 0.995)
    assert er.policy_for(trade(slow_fill=True), 0.1, 0.1)["mode"] == "immediate"   # >5 s fill path
    assert er.policy_for(trade(ref=None), 0.1, 0.1)["mode"] == "immediate"
    assert er.policy_for(trade(), 0.1, 0)["mode"] == "immediate"


def test_trigger_mode_stop_before_trigger():
    t, p = ticks((0, 100.0), (1, 99.95), (2, 99.84))
    ex = er.replay_policy(t, p, True, T0, er.policy_for(trade(), 0.1, 0.1))
    assert ex[2] == "stop" and ex[0] == T0 + 2


def test_trigger_mode_switches_to_trail_at_a_poll():
    # Up past the trigger (100.09995) by the 1.5 s poll, then a 0.1% pullback from the new high.
    t, p = ticks((0, 100.0), (1, 100.12), (2, 100.3), (3, 100.25), (4, 100.19))
    ex = er.replay_policy(t, p, True, T0, er.policy_for(trade(), 0.1, 0.1))
    assert ex == (T0 + 4, 100.19, "trail")


def test_time_exit_at_deadline():
    t, p = ticks((0, 100.0), (300, 100.02), (700, 100.04))
    ex = er.replay_policy(t, p, True, T0, er.policy_for(trade(deadline=T0 + 600), 0.5, 0))
    assert ex == (T0 + 600, 100.02, "max_hold")


# ── build_trades ───────────────────────────────────────────────────────────────

def order(**kw):
    o = {"id": kw.get("id", "x"), "coid": "", "symbol": "NVDA", "side": "buy", "type": "market", "qty": 10.0,
         "filled_qty": 0.0, "filled_avg_price": None, "stop_price": None, "trail_percent": None,
         "status": "filled", "created": None, "submitted": None, "filled": None, "canceled": None, "replaced": None}
    o.update(kw)
    return o


S = "NVDA_CAM_BREAKOUT_R4S4_V02_5MIN"
RULES = {S: {"trail": 0.1, "trigger": 0.1, "stop": None, "max_hold": 10.0}}


def test_build_trades_trigger_path_hard_stop_exit():
    orders = [
        order(id="e1", coid=f"kairos-{S}-{int(T0)}", filled_qty=10, filled_avg_price=100.0,
              created=T0 - 1, submitted=T0 - 1, filled=T0),
        order(id="h1", side="sell", type="stop", stop_price=99.85, created=T0 + 0.4,
              filled_qty=10, filled_avg_price=99.84, filled=T0 + 20),
    ]
    trs, skipped = er.build_trades(orders, RULES, "2026-10-06", "2026-10-06")
    assert not skipped and len(trs) == 1
    tr = trs[0]
    assert tr["exit"]["kind"] == "stop" and tr["exit"]["px"] == 99.84
    assert tr["ref"] == pytest.approx(99.85 / 0.999)          # signal price recovered from the hard stop
    assert tr["place_delay"] == pytest.approx(0.4) and not tr["slow_fill"]
    assert tr["deadline_kind"] == "max_hold" and tr["deadline"] == T0 + 600


def test_build_trades_immediate_trail_and_other_exit_cap():
    orders = [
        order(id="e1", coid=f"kairos-{S}-{int(T0)}", filled_qty=10, filled_avg_price=100.0,
              created=T0 - 8, submitted=T0 - 8, filled=T0),                         # slow fill
        order(id="t1", coid=f"kairos-trail-{S}-{int(T0)}", side="sell", type="trailing_stop",
              trail_percent=0.1, created=T0 + 0.2, canceled=T0 + 120),
        order(id="m1", side="sell", type="market", filled_qty=10, filled_avg_price=100.3, created=T0 + 120,
              filled=T0 + 120),                                                      # a TV exit at 2 min
    ]
    tr = er.build_trades(orders, RULES, "2026-10-06", "2026-10-06")[0][0]
    assert tr["slow_fill"] and tr["exit"]["kind"] == "other"
    assert tr["deadline_kind"] == "cap" and tr["deadline"] == T0 + 120
    assert tr["trail_order"] == {"placed": T0 + 0.2, "ended": T0 + 120, "trail_pct": 0.1}


# ── evaluate ───────────────────────────────────────────────────────────────────

def test_evaluate_match_check_slippage_and_variants():
    t, p = ticks((0, 100.0), (1, 99.95), (2, 99.84), (3, 99.9), (600, 100.2))
    tr = trade(symbol="NVDA", strategy=S, band="BREAKOUT R4S4", day="2026-10-06",
               rule={"trail": 0.1, "trigger": 0.1, "stop": None, "max_hold": 10.0},
               trail_order=None,
               hard_order={"placed": T0 + 0.4, "ended": None, "stop_price": 99.85},
               exit={"t": T0 + 2, "px": 99.83, "kind": "stop"})
    res = er.evaluate([tr], lambda _tr: (t, p, 0.005), [{"trail": 0.1, "trigger": 0.1}, {"trail": 0.5, "trigger": 0.1}])
    m = res["match"]
    assert m["stop_exits_checked"] == 1 and m["matched_share"] == 1.0
    # replay fill 99.84 − 0.005 = 99.835 vs real 99.83 → 0.005% extra slippage measured
    assert m["slippage_pct"] == pytest.approx((99.835 - 99.83) / 99.83 * 100, abs=1e-4)
    assert res["by_band"]["BREAKOUT R4S4"]["actual"] == pytest.approx(-1.7)
    tight, wide = res["variants"]
    assert tight["total"] < 0                         # 0.1% stop from the signal price hits at 99.84
    assert wide["total"] > tight["total"]             # a 0.5% stop rides to the 10-min close at 100.2
    assert res["trusted"]


def test_evaluate_flags_untrusted_when_replay_misses():
    t, p = ticks((0, 100.0), (1, 100.01), (2, 100.02))
    tr = trade(symbol="NVDA", strategy=S, band="BREAKOUT R4S4", day="2026-10-06",
               rule={"trail": 0.1, "trigger": 0.1, "stop": None, "max_hold": 10.0}, trail_order=None,
               hard_order={"placed": T0 + 0.4, "ended": None, "stop_price": 99.85},
               exit={"t": T0 + 2, "px": 99.83, "kind": "stop"})
    res = er.evaluate([tr], lambda _tr: (t, p, 0.0), [{"trail": 0.1}])
    assert not res["trusted"] and res["match"]["stop_exits_checked"] == 0
    assert any("replayed" in r for r in res["reasons"])


def test_merge_windows_groups_overlaps_per_symbol():
    a = {"symbol": "NVDA", "fill_t": T0, "exit": {"t": T0 + 30}}
    b = {"symbol": "NVDA", "fill_t": T0 + 60, "exit": {"t": T0 + 90}}
    c = {"symbol": "NVDA", "fill_t": T0 + 7200, "exit": {"t": T0 + 7210}}
    d = {"symbol": "AAPL", "fill_t": T0, "exit": {"t": T0 + 10}}
    w = er.merge_windows([a, b, c, d])
    assert [(x[0], len(x[3])) for x in w] == [("AAPL", 1), ("NVDA", 2), ("NVDA", 1)]
    # A ticker traded every 5 minutes doesn't merge into one all-day window.
    day = [{"symbol": "TSLA", "fill_t": T0 + k * 300, "exit": {"t": T0 + k * 300 + 60}} for k in range(60)]
    spans = [x[2] - x[1] for x in er.merge_windows(day)]
    assert len(spans) > 1 and max(spans) <= er.MAX_WINDOW_S


# ── endpoints ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def client(tmp_path, monkeypatch):
    import app as a
    db = tmp_path / "replay.db"

    def _db():
        return sqlite3.connect(db)
    monkeypatch.setattr(a, "get_db", _db)
    monkeypatch.setattr(a, "placeholder", lambda: "?")
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {"5": {"broker": object()}})
    monkeypatch.setattr(er, "_market_open", lambda now=None: False)
    started = []

    def _fake_run(run_id, *args):
        started.append((run_id, args))
        er._save(run_id, status="done", progress="done", result='{"trades": 0, "rows": [1], "trusted": false}')
        er._job_lock.release()
    monkeypatch.setattr(er, "run_replay", _fake_run)
    return a.app.test_client(), started


def _body(**kw):
    b = {"account": "5", "from": "2026-10-01", "to": "2026-10-31", "variants": [{"trail": 0.1, "trigger": 0.1}]}
    b.update(kw)
    return b


def test_run_endpoint_validates_and_starts(client):
    c, started = client
    assert c.post("/api/exit_replay/run", json=_body(to="2026-12-31")).status_code == 400
    assert c.post("/api/exit_replay/run", json=_body(variants=[])).status_code == 400
    assert c.post("/api/exit_replay/run", json=_body(variants=[{"trail": 9}])).status_code == 400
    assert c.post("/api/exit_replay/run", json=_body(account="99")).status_code == 400
    r = c.post("/api/exit_replay/run", json=_body())
    assert r.status_code == 200
    rid = r.get_json()["run_id"]
    import time as _t
    for _ in range(50):
        if started:
            break
        _t.sleep(0.02)
    assert started and started[0][1][:3] == ("5", "2026-10-01", "2026-10-31")
    runs = c.get("/api/exit_replay/runs").get_json()["runs"]
    assert runs[0]["id"] == rid and runs[0]["status"] == "done"
    detail = c.get(f"/api/exit_replay/runs/{rid}").get_json()
    assert detail["status"] == "done" and "rows" not in detail["result"]
    assert c.get(f"/api/exit_replay/runs/{rid}?rows=1").get_json()["result"]["rows"] == [1]
    assert c.get("/api/exit_replay/runs/nope").status_code == 404


def test_dead_run_is_reaped_and_no_longer_blocks(client, monkeypatch):
    c, started = client
    er._ensure_table()
    conn = er._app().get_db()
    old = "2026-11-08T10:00:00+00:00"
    conn.execute("INSERT INTO exit_replay_runs (id, created_at, params, status, progress, result, updated_at) "
                 "VALUES ('dead1', ?, '{}', 'running', 'replayed 1450 of 2040 trades', NULL, ?)", (old, old))
    conn.commit(); conn.close()
    assert c.post("/api/exit_replay/run", json=_body()).status_code == 200
    runs = {r["id"]: r for r in c.get("/api/exit_replay/runs").get_json()["runs"]}
    assert runs["dead1"]["status"] == "failed" and "restarted" in runs["dead1"]["progress"]


def test_ticks_are_fetched_in_chunks(monkeypatch):
    calls = []

    class FakeClient:
        def get_stock_trades(self, req):
            calls.append((req.start, req.end))
            return {"NVDA": [{"t": req.start.strftime("%Y-%m-%dT%H:%M:%S.000000001Z"), "p": 100.0, "c": ["@"]},
                             {"t": req.start.strftime("%Y-%m-%dT%H:%M:%S.5Z"), "p": 100.1, "c": ["I"]}]}
    import app as a
    monkeypatch.setattr(a, "_alpaca_data_feed", lambda: "sip", raising=False)
    beats = []
    t, p = er._ticks(FakeClient(), "NVDA", T0, T0 + 12 * 60, beat=lambda: beats.append(1))
    assert len(calls) == 3 and len(beats) == 3           # 5 + 5 + 2 minutes
    assert list(p) == [100.0, 100.0, 100.0]              # the odd-lot ('I') ticks are dropped
    assert t[0] == pytest.approx(T0, abs=1e-6)


def test_run_refused_while_market_open(client, monkeypatch):
    c, _ = client
    monkeypatch.setattr(er, "_market_open", lambda now=None: True)
    assert c.post("/api/exit_replay/run", json=_body()).status_code == 423


def test_market_open_window():
    assert er._market_open(datetime(2026, 11, 9, 10, 0, tzinfo=ET))          # Monday 10:00
    assert not er._market_open(datetime(2026, 11, 9, 17, 0, tzinfo=ET))
    assert not er._market_open(datetime(2026, 11, 8, 11, 0, tzinfo=ET))      # Sunday


# ── pairing fixes (2026-11-09) ─────────────────────────────────────────────────

def test_unrecognized_stop_exit_is_used_not_a_later_days_market_sell():
    # The real exit is a stop the pairing doesn't recognize as this trade's hard stop
    # (placed late). It must not skip it and grab a market sell two days later.
    day2 = T0 + 2 * 86400
    orders = [
        order(id="e1", coid=f"kairos-{S}-{int(T0)}", filled_qty=10, filled_avg_price=100.0,
              created=T0 - 1, submitted=T0 - 1, filled=T0),
        order(id="s1", side="sell", type="stop", stop_price=99.9, created=T0 + 40,
              filled_qty=10, filled_avg_price=99.88, filled=T0 + 60),
        order(id="m2", side="sell", type="market", filled_qty=10, filled_avg_price=108.0,
              created=day2, filled=day2),
    ]
    trs, skipped = er.build_trades(orders, RULES, "2026-10-06", "2026-10-08")
    assert len(trs) == 1 and trs[0]["exit"]["px"] == 99.88 and trs[0]["exit"]["t"] == T0 + 60


def test_no_exit_on_a_later_day():
    orders = [
        order(id="e1", coid=f"kairos-{S}-{int(T0)}", filled_qty=10, filled_avg_price=100.0,
              created=T0 - 1, submitted=T0 - 1, filled=T0),
        order(id="m2", side="sell", type="market", filled_qty=10, filled_avg_price=108.0,
              created=T0 + 86400, filled=T0 + 86400),
    ]
    trs, skipped = er.build_trades(orders, RULES, "2026-10-06", "2026-10-08")
    assert trs == [] and skipped == {"no same-day exit found": 1}


def test_a_later_trades_trail_is_not_taken():
    # Trade 1 exits by its hard stop and never gets a trail; trade 2 (same strategy, 20 min
    # later) gets a trail that fills. Trade 1 must not claim trade 2's trail.
    t2 = T0 + 1200
    orders = [
        order(id="e1", coid=f"kairos-{S}-{int(T0)}", filled_qty=10, filled_avg_price=100.0,
              created=T0 - 1, submitted=T0 - 1, filled=T0),
        order(id="h1", side="sell", type="stop", stop_price=99.85, created=T0 + 0.4,
              filled_qty=10, filled_avg_price=99.84, filled=T0 + 20),
        order(id="e2", coid=f"kairos-{S}-{int(t2)}", filled_qty=10, filled_avg_price=101.0,
              created=t2 - 1, submitted=t2 - 1, filled=t2),
        order(id="h2", side="sell", type="stop", stop_price=100.85, created=t2 + 0.4, canceled=t2 + 30),
        order(id="t2", coid=f"kairos-trail-{S}-{int(t2) + 30}", side="sell", type="trailing_stop",
              trail_percent=0.1, created=t2 + 30, filled_qty=10, filled_avg_price=101.2, filled=t2 + 90),
    ]
    trs, _ = er.build_trades(orders, RULES, "2026-10-06", "2026-10-06")
    assert [(tr["exit"]["kind"], tr["exit"]["px"]) for tr in trs] == [("stop", 99.84), ("trail", 101.2)]
    assert trs[0]["trail_order"] is None and trs[1]["trail_order"]["placed"] == t2 + 30


def test_evaluate_reports_per_strategy_results():
    t, p = ticks((0, 100.0), (1, 99.95), (2, 99.84), (3, 99.9), (600, 100.2))
    mk = lambda strat: trade(symbol="NVDA", strategy=strat, band="BREAKOUT R4S4", day="2026-10-06",
                             rule={"trail": 0.1, "trigger": 0.1, "stop": None, "max_hold": 10.0}, trail_order=None,
                             hard_order={"placed": T0 + 0.4, "ended": None, "stop_price": 99.85},
                             exit={"t": T0 + 2, "px": 99.83, "kind": "stop"})
    res = er.evaluate([mk(S), mk("AAPL_X")], lambda _tr: (t, p, 0.0), [{"trail": 0.1, "trigger": 0.1}])
    assert set(res["by_strategy"]) == {S, "AAPL_X"} and res["by_strategy"][S]["trades"] == 1
    v = res["variants"][0]
    assert v["by_strategy"][S] == pytest.approx(v["total"] / 2, abs=0.01)
    assert res["long_holds"] == 0


def test_run_endpoint_takes_a_strategy_list(client):
    c, started = client
    assert c.post("/api/exit_replay/run", json=_body(strategies="MU")).status_code == 400
    assert c.post("/api/exit_replay/run", json=_body(strategies=["bad name;"])).status_code == 400
    assert c.post("/api/exit_replay/run", json=_body(strategies=["x"] * 51)).status_code == 400
    r = c.post("/api/exit_replay/run", json=_body(strategies=[" mu_cam_breakout_r3s3_v02_5min", "NVDA_X"]))
    assert r.status_code == 200
    import time as _t
    for _ in range(50):
        if started:
            break
        _t.sleep(0.02)
    assert started[0][1][4] == ["MU_CAM_BREAKOUT_R3S3_V02_5MIN", "NVDA_X"]


# ── max-hold variants (2026-11-12) ─────────────────────────────────────────────

def test_variant_deadline_rule_minutes_none_and_cap():
    tr = trade(deadline=T0 + 600, deadline_kind="max_hold")
    assert er.variant_deadline(tr, None) == (T0 + 600, "max_hold")              # the rule's own
    assert er.variant_deadline(tr, 30) == (T0 + 1800, "max_hold")
    eod = datetime(2026, 10, 6, 15, 55, tzinfo=ET).timestamp()
    assert er.variant_deadline(tr, 0) == (eod, "eod")                           # no max hold
    assert er.variant_deadline(tr, 390)[0] == eod                               # never past the flatten
    capped = trade(deadline=T0 + 120, deadline_kind="cap")                      # a real exit we can't model
    assert er.variant_deadline(capped, 0) == (T0 + 120, "cap")


def test_a_longer_hold_lets_a_wide_trail_ride_the_trend():
    # Up 0.1% per minute for an hour; a 0.54% trail never fires. The 10-min hold sells at
    # +1%, the 60-min hold near +6%, no hold rides to the 15:55 flatten.
    secs = np.arange(0, 3 * 3600, 5)
    px = 100.0 * (1 + 0.001 * np.minimum(secs, 3600) / 60)
    t, p = T0 + secs.astype(float), px
    tr = trade(symbol="NVDA", strategy=S, band="BREAKOUT R3S3", day="2026-10-06", ref=100.0,
               rule={"trail": 0.54, "trigger": 0.1, "stop": None, "max_hold": 10.0},
               trail_order=None, hard_order={"placed": T0 + 0.4, "ended": None, "stop_price": 99.46},
               exit={"t": T0 + 600, "px": 101.0, "kind": "other"}, deadline=T0 + 600, deadline_kind="max_hold")
    variants = [{"trail": None, "trigger": None, "max_hold": 10}, {"trail": None, "trigger": None, "max_hold": 60},
                {"trail": None, "trigger": None, "max_hold": 0}]
    res = er.evaluate([tr], lambda _tr: (t, p, 0.0), variants)
    ten, sixty, none = (v["total"] for v in res["variants"])
    assert ten == pytest.approx(10 * (101.0 - 100.0), abs=0.2)
    assert sixty == pytest.approx(10 * (106.0 - 100.0), abs=0.2)
    assert none >= sixty - 0.01
    assert [v["max_hold"] for v in res["variants"]] == [10, 60, 0] and res["variants"][0]["trades"] == 1


def test_parse_variants_max_hold():
    assert er._parse_variants([{"max_hold": 0}]) == [{"trail": None, "trigger": None, "stop": None, "max_hold": 0}]
    assert er._parse_variants([{"trail": 0.3}])[0] == {"trail": 0.3, "trigger": 0.0, "stop": None, "max_hold": None}
    for bad in ([{}], [{"max_hold": 500}], [{"max_hold": "x"}], [{"trail": 9}]):
        with pytest.raises(ValueError):
            er._parse_variants(bad)


def test_merge_windows_follow_window_end_and_span():
    a = {"symbol": "NVDA", "fill_t": T0, "exit": {"t": T0 + 30}, "window_end": T0 + 5 * 3600}
    b = {"symbol": "NVDA", "fill_t": T0 + 3600, "exit": {"t": T0 + 3700}, "window_end": T0 + 5 * 3600}
    one = er.merge_windows([a, b], er.LONG_WINDOW_S)
    assert len(one) == 1 and one[0][2] == T0 + 5 * 3600
    assert len(er.merge_windows([a, b])) == 2                 # default 30-min cap keeps them apart
