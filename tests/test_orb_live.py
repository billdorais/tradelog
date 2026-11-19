"""ORB paper engine (routes/orb_live.py): signals on closed bars only, one bracket per ticker
per day across workers, paper-only guards, the 15:55 flatten, and the live-vs-backtest reconcile.

No network: Alpaca bars and the trading client are fakes; the database is a temp sqlite file."""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

import app as a  # noqa: E402
from routes import orb_live as L  # noqa: E402
from strategies import orb  # noqa: E402
from tests.test_orb import _day  # noqa: E402

ET = ZoneInfo("America/New_York")
DAY = date(2026, 11, 16)          # a Monday


def _bars(breakout=True):
    """Range 120–121 (09:00–09:29). With breakout, the 09:40 candle closes at 122 (long)."""
    segs = [("09:00", "09:30", 120.5, 120.5), ("09:30", "09:40", 120.5, 120.5)]
    segs += ([("09:40", "09:45", 120.75, 122.0), ("09:45", "16:00", 122.0, 123.0)] if breakout
             else [("09:40", "16:00", 120.5, 120.75)])
    df = _day(DAY.isoformat(), segs, {"09:10": (120.5, 121, 120, 120.5)})
    df.index = df.index.tz_convert("UTC")
    return df


class FakeTrading:
    def __init__(self):
        self.submitted, self.orders = [], {}

    def submit_order(self, req):
        if any(o.client_order_id == req.client_order_id for o in self.orders.values()):
            raise Exception('{"code":40010001,"message":"client_order_id must be unique"}')
        o = SimpleNamespace(id=f"oid-{len(self.orders) + 1}", client_order_id=req.client_order_id)
        self.orders[o.id] = o
        self.submitted.append(req)
        return o

    def get_order_by_client_id(self, coid):
        return next(o for o in self.orders.values() if o.client_order_id == coid)


class FakeBroker:
    def __init__(self, paper=True):
        self._paper = paper
        self._trading = FakeTrading()
        self.positions, self.closed = [], []

    def _ensure_client(self):
        pass

    def _invalidate_pos_cache(self):
        pass

    def get_positions(self, raise_on_error=False):
        return list(self.positions)

    def close_position(self, sym):
        self.closed.append(sym)
        return {"success": True}


@pytest.fixture
def env(monkeypatch, tmp_path):
    db = str(tmp_path / "orb_live.db")
    monkeypatch.setattr(a, "get_db", lambda: sqlite3.connect(db))
    monkeypatch.setattr(a, "placeholder", lambda: "?")
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    conn.close()
    L.ensure_table()
    L._state.update(day=None, decided=set(), last_scan=0.0, warm={}, reconciled=None, flattened=None,
                    flatten_claim=None, flatten_sent={}, cfg=None, cfg_ts=0.0)
    L._states["6"].update(L._fresh_state())
    L._owned.update(ts=0.0, val=frozenset())
    L._preflight.update(ts=0.0, val=None)
    broker = FakeBroker()
    rec = {"num": "8", "tag": "alpaca8", "label": "ORB Paper", "paper": True, "self_managed": True, "broker": broker}
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {**a.ACCOUNTS_BY_NUM, "8": rec})
    frames = {"QQQ": _bars(), "GOOG": _bars(breakout=False)}

    def fake_fetch(symbols, start, end=None, adjustment="raw"):
        out = {}
        for s in symbols:
            df = frames.get(s)
            if df is not None:
                df = df[df.index >= pd.Timestamp(start).tz_convert("UTC")]
                if end is not None:
                    df = df[df.index < pd.Timestamp(end).tz_convert("UTC")]
            out[s] = df
        return out
    monkeypatch.setattr(L, "fetch_bars", fake_fetch)
    return SimpleNamespace(broker=broker, rec=rec, frames=frames, db=db)


def _cfg(mode="paper", **kw):
    cfg = L.validate_config({"mode": mode, "tickers": [{"symbol": "QQQ", "group": "core"},
                                                       {"symbol": "GOOG", "group": "core"}], **kw})
    L.save_config(cfg)
    return cfg


def _at(h, m, s=0):
    return datetime(DAY.year, DAY.month, DAY.day, h, m, s, tzinfo=ET)


def _row(t):
    return {r["ticker"]: r for r in L.rows(day=DAY.isoformat())}.get(t)


def _new_worker():
    """Another gunicorn worker: same database, empty memory."""
    L._state.update(day=None, decided=set(), last_scan=0.0, warm={}, flattened=None, flatten_claim=None,
                    flatten_sent={})


# ── signals and entries ───────────────────────────────────────────────────────

def test_breakout_sends_one_bracket_with_the_backtest_levels(env):
    cfg = _cfg(risk=250, cap=25000)
    L.scan(_at(9, 45, 5), cfg)
    assert len(env.broker._trading.submitted) == 1
    req = env.broker._trading.submitted[0]
    assert req.symbol == "QQQ" and req.qty == 125                 # $250 ÷ ($122 close − $120 stop)
    assert str(req.side).lower().endswith("buy") and "bracket" in str(req.order_class).lower()
    assert req.stop_loss.stop_price == 120.0 and req.take_profit.limit_price == 128.0   # 122 + 3 × 2
    assert req.client_order_id == "orb-20261116-QQQ"
    r = _row("QQQ")
    assert r["status"] == "entered" and r["side"] == "long" and r["signal_time"] == "09:40"
    assert (r["or_high"], r["or_low"]) == (121.0, 120.0)
    assert _row("GOOG") is None                                     # no breakout yet: still waiting


def test_a_candle_is_not_used_before_it_closes(env):
    cfg = _cfg()
    L.scan(_at(9, 44, 30), cfg)       # the 09:44 bar is still forming: the 09:40 candle isn't closed
    assert env.broker._trading.submitted == [] and _row("QQQ") is None


def test_one_entry_per_ticker_per_day_across_workers(env):
    cfg = _cfg()
    L.scan(_at(9, 45, 5), cfg)
    _new_worker()
    L.scan(_at(9, 45, 15), cfg)                      # second worker: QQQ is decided in the table
    assert len(env.broker._trading.submitted) == 1
    # Both workers racing past that check: Alpaca's unique client_order_id stops the second one.
    m = orb.Market.from_frame(env.frames["QQQ"])
    sig = orb.day_signal(m, DAY, L.engine_params(cfg), asof_ns=pd.Timestamp(_at(9, 45)).value)
    L.enter(_at(9, 45, 6), DAY, "QQQ", sig, L.engine_params(cfg), cfg, {"grp": "core", "mode": "paper"})
    assert len(env.broker._trading.submitted) == 1
    assert _row("QQQ")["status"] == "entered" and "duplicate" in _row("QQQ")["note"]


def test_dry_run_sends_nothing(env):
    cfg = _cfg(mode="dry-run")
    L.scan(_at(9, 45, 5), cfg)
    assert env.broker._trading.submitted == []
    r = _row("QQQ")
    assert r["status"] == "dry-run" and r["note"].startswith("would buy 125") and r["target"] == 128.0


def test_stale_data_waits(env):
    env.frames["QQQ"] = env.frames["QQQ"][env.frames["QQQ"].index < pd.Timestamp(_at(9, 31))]
    cfg = _cfg()
    L.scan(_at(9, 45, 5), cfg)
    assert env.broker._trading.submitted == []
    assert _row("QQQ")["status"] == "waiting" and "stale" in _row("QQQ")["note"]


def test_a_signal_found_late_is_logged_not_traded(env):
    cfg = _cfg()
    L.scan(_at(9, 52, 0), cfg)        # engine was down at 09:45
    assert env.broker._trading.submitted == []
    assert _row("QQQ")["status"] == "missed"


def test_no_breakout_by_the_last_entry_is_a_no_trade(env):
    cfg = _cfg()
    L.scan(_at(10, 31, 5), cfg)
    assert _row("GOOG")["status"] == "no-trade" and _row("GOOG")["note"] == "no breakout"


def test_a_share_that_risks_more_than_the_budget_is_skipped(env):
    cfg = _cfg(risk=1)
    L.scan(_at(9, 45, 5), cfg)
    assert env.broker._trading.submitted == [] and _row("QQQ")["status"] == "skipped"


def test_max_position_caps_the_shares(env):
    cfg = _cfg(risk=250, cap=5000)
    L.scan(_at(9, 45, 5), cfg)
    assert env.broker._trading.submitted[0].qty == 40              # $5,000 ÷ $122


# ── guards ────────────────────────────────────────────────────────────────────

def test_a_key_still_on_another_slot_blocks_the_engine(env, monkeypatch):
    """Slot 8 took over Crew Paper's Alpaca account. If ALPACA_KEY4 was left in place,
    Kairos rules aimed at slot 4 would trade the ORB account, so the engine refuses."""
    env.broker._key = "PKSAMEKEY"
    other = FakeBroker()
    other._key = "PKSAMEKEY"
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {**a.ACCOUNTS_BY_NUM, "4": {"tag": "alpaca4", "broker": other}})
    cfg = _cfg()
    L.scan(_at(9, 45, 5), cfg)
    assert env.broker._trading.submitted == []
    r = _row("QQQ")
    assert r["status"] == "blocked" and "ALPACA_KEY4" in r["note"]
    other._key = "PKOTHER"
    assert L.account_check()[1] is True

@pytest.mark.parametrize("patch, why", [
    ({"paper": False}, "not a paper account"),
    ({"self_managed": False}, "self_managed"),
])
def test_only_a_paper_self_managed_account_is_traded(env, patch, why):
    env.rec.update(patch)
    cfg = _cfg()
    L.scan(_at(9, 45, 5), cfg)
    assert env.broker._trading.submitted == []
    assert _row("QQQ")["status"] == "blocked" and why in _row("QQQ")["note"]


def test_a_broker_on_the_live_endpoint_is_refused_even_if_flagged_paper(env):
    env.broker._paper = False
    cfg = _cfg()
    L.scan(_at(9, 45, 5), cfg)
    assert env.broker._trading.submitted == [] and _row("QQQ")["status"] == "blocked"


def test_missing_account_blocks_paper_mode(env, monkeypatch):
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {k: v for k, v in a.ACCOUNTS_BY_NUM.items() if k != "8"})
    c = a.app.test_client()
    r = c.post("/api/orb/live/config", json={"mode": "paper"})
    assert r.status_code == 400 and "ALPACA_KEY8" in r.get_json()["error"]
    assert c.post("/api/orb/live/config", json={"mode": "dry-run"}).status_code == 200


def test_config_validation():
    with pytest.raises(ValueError):
        L.validate_config({"mode": "live"})
    with pytest.raises(ValueError):
        L.validate_config({"tickers": [{"symbol": "QQQ!"}]})
    with pytest.raises(ValueError):
        L.validate_config({"tickers": []})
    with pytest.raises(ValueError):
        L.validate_config({"risk": 0})
    with pytest.raises(ValueError):
        L.validate_config({"params": {"last_entry": "09:15"}})        # before the range ends
    c = L.validate_config({"tickers": [{"symbol": "qqq"}, {"symbol": "QQQ", "group": "watch"}], "cap": ""})
    assert c["tickers"] == [{"symbol": "QQQ", "group": "core"}] and c["cap"] is None and c["mode"] == "off"


def test_status_and_config_endpoints(env):
    c = a.app.test_client()
    r = c.post("/api/orb/live/config", json={"mode": "dry-run", "risk": 100})
    assert r.status_code == 200 and r.get_json()["config"]["risk"] == 100.0
    s = c.get("/api/orb/live/status").get_json()
    assert s["config"]["mode"] == "dry-run" and s["account"]["ok"] is True
    assert set(s["summary"]) == {"core", "watch", "all"}


# ── flatten and reconcile ─────────────────────────────────────────────────────

def test_flatten_closes_account_8_positions_once_across_workers(env):
    cfg = _cfg()
    env.broker.positions = [{"symbol": "QQQ", "qty": 125}]
    L.flatten(_at(15, 55, 2), cfg)
    assert env.broker.closed == ["QQQ"]
    _new_worker()
    L.flatten(_at(15, 55, 7), cfg)                   # the other worker lost the claim
    assert env.broker.closed == ["QQQ"]


def test_a_failed_close_is_logged_and_retried_until_flat(env):
    """2026-11-16: MU's 3:55 close did not take and the 15:58 EOD backstop closed it,
    while the engine had already marked the day flat. Now the day is flat only when
    the broker says so, a failed close is noted on the row, and it is retried."""
    cfg = _cfg()
    L.scan(_at(9, 45, 5), cfg)                                   # a QQQ row to note on
    env.broker.positions = [{"symbol": "QQQ", "qty": 125}, {"symbol": "MU", "qty": -2}]
    results = {"QQQ": [{"success": True}], "MU": [{"success": False, "error": "held_for_orders"},
                                                  {"success": True}]}
    def close(sym):
        env.broker.closed.append(sym)
        r = results[sym].pop(0) if results[sym] else {"success": True}
        if r["success"]:
            env.broker.positions = [p for p in env.broker.positions if p["symbol"] != sym]
        return r
    env.broker.close_position = close

    L.flatten(_at(15, 55, 2), cfg)
    assert env.broker.closed == ["QQQ", "MU"] and L._state["flattened"] is None
    assert _row("MU")["note"].startswith("3:55 close failed at 15:55:02 ET") and "held_for_orders" in _row("MU")["note"]
    assert _row("QQQ")["flatten_at"]

    L.flatten(_at(15, 55, 7), cfg)                               # retried on the next tick
    assert env.broker.closed == ["QQQ", "MU", "MU"]
    L.flatten(_at(15, 55, 12), cfg)                              # broker reports flat
    assert L._state["flattened"] == DAY and env.broker.closed == ["QQQ", "MU", "MU"]


def test_a_close_in_flight_is_not_sent_twice(env):
    """Paper fills lagged up to a minute at the open; a close that is still filling
    must not be re-sent every 5 seconds."""
    cfg = _cfg()
    env.broker.positions = [{"symbol": "QQQ", "qty": 125}]       # the fake never fills it
    L.flatten(_at(15, 55, 2), cfg)
    L.flatten(_at(15, 55, 7), cfg)
    L.flatten(_at(15, 55, 40), cfg)
    assert env.broker.closed == ["QQQ"]
    L.flatten(_at(15, 55, 50), cfg)                              # 48 s later: send again
    assert env.broker.closed == ["QQQ", "QQQ"]


def test_orb_fills_are_labelled_orb_not_a_nearby_tv_signal():
    """2026-11-16: the QQQ and GOOG ORB entries were labelled with Camarilla strategies
    that happened to signal within 5 minutes."""
    lookup = {("QQQ", "SLD"): [(pd.Timestamp("2026-11-16T14:36:00Z").timestamp(),
                               "QQQ_CAM_REVERSAL_R4S4_V02_5MIN", "bearish")]}
    got = a._resolve_signal_for_fill(lookup, "QQQ", "SLD", "2026-11-16T14:36:07Z", "orb-20261116-QQQ")
    assert got == ("QQQ_ORB", "")
    # Everything else is untouched: a Kairos id still wins, an unknown id still matches.
    assert a._resolve_signal_for_fill(lookup, "QQQ", "SLD", "2026-11-16T14:36:07Z",
                                      "kairos-QQQ_CAM_BREAKOUT_R3S3-1")[0] == "QQQ_CAM_BREAKOUT_R3S3"
    assert a._resolve_signal_for_fill(lookup, "QQQ", "SLD", "2026-11-16T14:36:07Z", "abc")[0] \
        == "QQQ_CAM_REVERSAL_R4S4_V02_5MIN"
    assert a._orb_fill_strategy("orb-20261116-BRK.B", "BRK.B") == "BRK.B_ORB"
    import inspect
    src = inspect.getsource(a)
    # Used by all three resolvers: the journal's (sym), and the main one and the weekly
    # report's (symbol) — plus the helper's own def line.
    assert src.count("_orb_fill_strategy(order_id, sym)") == 1 and src.count("_orb_fill_strategy(order_id, symbol)") == 3


def test_tick_runs_only_in_its_windows(env, monkeypatch):
    calls = []
    monkeypatch.setattr(L, "scan", lambda now, cfg, book="8": calls.append(("scan", now.strftime("%H:%M:%S"))))
    monkeypatch.setattr(L, "flatten", lambda now, cfg, book="8": calls.append(("flatten", now.strftime("%H:%M"))))
    monkeypatch.setattr(L, "reconcile", lambda day, cfg, book="8": calls.append(("reconcile", day.isoformat())))
    _cfg()
    for t in (_at(9, 20), _at(9, 35, 10), _at(9, 38, 0), _at(12, 0), _at(15, 55, 1), _at(16, 6), _at(16, 7)):
        L._state["last_scan"] = 0.0
        L.tick(t)
    assert calls == [("scan", "09:35:10"), ("flatten", "15:55"), ("reconcile", "2026-11-16")]
    calls.clear()
    _cfg(mode="off")
    L._state["cfg"] = None
    L.tick(_at(9, 35, 10))
    assert calls == []


def test_reconcile_reads_the_fill_and_exit_and_the_backtest(env):
    cfg = _cfg()
    L.scan(_at(9, 45, 5), cfg)
    filled = datetime(2026, 11, 16, 14, 45, 6)
    parent = SimpleNamespace(id="oid-1", filled_avg_price="122.05", filled_at=filled, status="filled",
                             legs=[SimpleNamespace(id="leg-t", order_type="OrderType.LIMIT", filled_avg_price=None),
                                   SimpleNamespace(id="leg-s", order_type="OrderType.STOP", filled_avg_price=None)])
    flat = SimpleNamespace(id="flat-1", side="OrderSide.SELL", filled_avg_price="122.95",
                           filled_at=datetime(2026, 11, 16, 20, 55, 3))
    env.broker._trading.get_order_by_id = lambda oid, filter=None: parent
    env.broker._trading.get_orders = lambda filter=None: [flat]
    L.reconcile(DAY, cfg)
    r = _row("QQQ")
    assert r["status"] == "exited" and r["exit_reason"] == "time" and r["fill_price"] == 122.05
    assert r["live_r"] == round(0.9 / 2.05, 4) and r["live_pnl"] == round(0.9 * 125, 2)
    assert r["bt_side"] == "long" and r["bt_entry"] == 122.01 and r["bt_reason"] == "time"
    assert r["slip_vs_bt"] == 0.04 and r["slip_vs_signal"] == 0.05
    g = _row("GOOG")
    assert g["status"] == "no-trade" and g["bt_side"] is None and g["reconciled_at"]


def test_reconcile_flags_a_day_the_backtest_traded_and_the_engine_missed(env):
    cfg = _cfg()
    L.reconcile(DAY, cfg)                             # the engine logged nothing today
    assert _row("QQQ")["status"] == "missed" and _row("QQQ")["bt_side"] == "long"


def test_target_leg_fill_is_scored_as_the_target(env):
    cfg = _cfg()
    L.scan(_at(9, 45, 5), cfg)
    parent = SimpleNamespace(id="oid-1", filled_avg_price="122.00", filled_at=datetime(2026, 11, 16, 14, 45, 6),
                             legs=[SimpleNamespace(id="leg-t", order_type="OrderType.LIMIT", filled_avg_price="128.00",
                                                   filled_at=datetime(2026, 11, 16, 17, 0))])
    env.broker._trading.get_order_by_id = lambda oid, filter=None: parent
    r = L.live_result(env.broker, {**_row("QQQ"), "ticker": "QQQ"})
    assert r["exit_reason"] == "target" and r["live_r"] == 3.0


# ── Kairos leaves the account alone ───────────────────────────────────────────

def test_account_8_is_self_managed_and_skipped_by_max_hold_recovery(monkeypatch, tmp_path):
    assert "alpaca8" in a._SELF_MANAGED_TAGS and a._TAB_KEY_BY_NUM["8"] == "orb"
    assert a.ACCOUNT_META["8"]["profit_lock"] is False and a.ACCOUNT_META["8"]["daily_loss_guard"] is False

    class Boom:
        def get_positions(self, raise_on_error=False):
            raise AssertionError("max-hold recovery must not read the ORB account")

    class Other:
        def get_positions(self, raise_on_error=False):
            return []

    monkeypatch.setattr(a, "get_db", lambda: sqlite3.connect(str(tmp_path / "x.db")))
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [{"tag": "alpaca8", "broker": Boom()}, {"tag": "alpaca5", "broker": Other()}])
    a._recover_max_hold_positions()


def test_position_stops_skip_the_orb_account(monkeypatch):
    class Losing:
        closed = []

        def _invalidate_pos_cache(self):
            pass

        def get_positions(self, raise_on_error=False):
            return [{"symbol": "QQQ", "qty": 100, "unrealized_pnl": -5000.0, "market_value": 10000.0,
                     "avg_entry_price": 150.0, "current_price": 100.0, "side": "long"}]

        def close_position(self, sym):
            Losing.closed.append(sym)
            return {"success": True}

    br = Losing()
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [{"tag": "alpaca8", "num": "8", "label": "ORB Paper", "broker": br}])
    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {"alpaca8": {"broker": br}})
    monkeypatch.setattr(a, "_CURATED_TAGS", {"alpaca8"})            # even if it were curated
    monkeypatch.setattr(a, "MAX_POSITION_LOSS_PCT", -1.0)
    a._check_position_stops()
    assert Losing.closed == []


# ── the shared signal function ────────────────────────────────────────────────

def test_day_signal_minute_by_minute_matches_the_backtest():
    df = _bars()
    m_full = orb.Market.from_frame(df)
    p = orb.normalize_params({"trend_filter": False, "contracts": 1})
    bt = orb.simulate(m_full, p, "QQQ", DAY, DAY)["trades"][0]
    first = None
    for minute in pd.date_range(_at(9, 30), _at(10, 31), freq="1min"):
        part = df[df.index <= pd.Timestamp(minute).tz_convert("UTC") - pd.Timedelta(minutes=1)]
        asof = int(part.index[-1].value) + 60_000_000_000
        sig = orb.day_signal(orb.Market.from_frame(part), DAY, p, asof_ns=asof)
        if sig["status"] != "waiting":
            first = (minute, sig)
            break
    minute, sig = first
    assert minute == pd.Timestamp(_at(9, 45))                         # not one minute before the candle closes
    assert sig["status"] == "signal" and sig["side"] == 1
    assert ("long", sig["lo"]) == (bt["side"], bt["stop"]) and bt["signal_time"] == "09:40"


def test_day_signal_waits_until_the_range_is_complete():
    m = orb.Market.from_frame(_bars())
    p = orb.normalize_params({})
    assert orb.day_signal(m, DAY, p, asof_ns=pd.Timestamp(_at(9, 20)).value)["status"] == "waiting"


# ── the main page's Open Positions table ──────────────────────────────────────

def test_open_positions_show_the_orb_bracket_not_a_trail_estimate(env, monkeypatch):
    """ORB orders are not kairos-* orders, so the trail estimate had nothing to work
    from; the row now shows the bracket the engine logged."""
    from datetime import datetime as _dt
    cfg = _cfg()
    L.scan(_at(9, 45, 5), cfg)                       # logs QQQ long: stop 120, target 128
    L.upsert(DAY.isoformat(), "QQQ", fill_time="2026-11-16T14:45:06+00:00")

    class _Now(_dt):
        @classmethod
        def now(cls, tz=None):
            return _dt(2026, 11, 16, 10, 0, tzinfo=tz)
    monkeypatch.setattr(a, "datetime", _Now)
    monkeypatch.setattr(a, "_latest_positions", [
        {"symbol": "QQQ", "broker": "alpaca8", "qty": 125, "avg_entry_price": 122.05,
         "current_price": 123.0, "unrealized_pnl": 118.75, "market_value": 15375.0}])
    a.app.config["TESTING"] = True
    pos = a.app.test_client().get("/api/risk/status").get_json()["positions"]
    p = next(x for x in pos if x["broker"] == "alpaca8")
    assert (p["strategy"], p["est_stop_price"], p["target_price"], p["stop_kind"]) == ("ORB long", 120.0, 128.0, "bracket")
    assert p["trail_pct"] is None and p["entry_time"] == "2026-11-16T14:45:06+00:00"


def test_the_dashboard_draws_the_orb_bracket_and_tints_its_rows():
    html = open("templates/index.html", encoding="utf-8").read()
    assert "price-meter-tick-tgt" in html and "p.stop_kind === 'bracket'" in html
    assert 'class="orb-row"' in html and "tr.orb-row" in html


def test_the_trades_list_labels_orb_fills_too(monkeypatch):
    """/api/alpaca/trades (the dashboard's fills list) annotates fills in its own
    loop; it kept the Camarilla labels after the resolvers were fixed."""
    import time as _t
    fills = [{"symbol": "QQQ", "side": "SLD", "time": "2026-11-16T14:36:07Z", "order_id": "orb-20261116-QQQ"},
             {"symbol": "QQQ", "side": "BOT", "time": "2026-11-16T14:43:59Z", "order_id": "leg-1"}]
    cache = {"data": [], "ts": _t.time()}
    monkeypatch.setitem(a._alpaca_caches, "8", cache)
    monkeypatch.setattr(a, "_alpaca_account_ctx", lambda acct: (object(), "alpaca8", "ORB Paper", lambda: [dict(f) for f in fills]))
    monkeypatch.setattr(a, "_signal_lookup", lambda: {
        ("QQQ", "SLD"): [(pd.Timestamp("2026-11-16T14:36:00Z").timestamp(), "QQQ_CAM_REVERSAL_R4S4_V02_5MIN", "bearish")]})
    a.app.config["TESTING"] = True
    got = a.app.test_client().get("/api/alpaca/trades?account=8").get_json()
    rows = got if isinstance(got, list) else got.get("trades") or got.get("fills") or []
    by = {f["order_id"]: f.get("strategy") for f in rows}
    assert by["orb-20261116-QQQ"] == "QQQ_ORB"


# ── the live book: Crew Live (account 6), real money ──────────────────────────
# Same engine, second book. The account also trades the crew's Camarilla rules, so the
# book owns only its own tickers there, and every entry passes the live-money guards.

@pytest.fixture
def live(env, monkeypatch):
    br = FakeBroker(paper=False)
    br._trading.get_account = lambda: SimpleNamespace(equity=6764.55, buying_power=27058.0, shorting_enabled=True,
                                                      trading_blocked=False, account_blocked=False)
    rec = {"num": "6", "tag": "alpaca6", "label": "Crew Live", "paper": False, "broker": br}
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {**a.ACCOUNTS_BY_NUM, "6": rec})
    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {**a.ACCOUNTS_BY_TAG, "alpaca6": rec})
    monkeypatch.setattr(a, "LIVE_TRADING_ARMED", True)
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 1000.0)
    monkeypatch.setattr(a, "LIVE_MAX_POSITION_PCT", 120.0)
    monkeypatch.setattr(a, "LIVE_MAX_ENTRIES_PER_DAY", 0)
    monkeypatch.setattr(a, "_live_snap_cache", {})
    for fn in ("_profit_lock_halted_for", "_daily_loss_halted_for", "_manual_halted_for"):
        monkeypatch.setattr(a, fn, lambda tag: False)
    conn = sqlite3.connect(env.db)
    conn.execute("CREATE TABLE routing_rules (id INTEGER PRIMARY KEY, name TEXT, enabled INTEGER, nodes TEXT)")
    conn.commit()
    conn.close()
    return SimpleNamespace(broker=br, rec=rec, db=env.db)


def _crew_rule(db, slug, enabled=1):
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?, ?, ?)",
                 (f"{slug} · Crew Live", enabled, json.dumps([{"type": "strategy", "value": slug},
                                                              {"type": "broker", "value": "alpaca-live-6"}])))
    conn.commit()
    conn.close()


def _cfg6(mode="live", **kw):
    cfg = L.validate_config({"mode": mode, "tickers": [{"symbol": "QQQ", "group": "core"},
                                                       {"symbol": "GOOG", "group": "watch"}], **kw}, book="6")
    L.save_config(cfg, book="6")
    return cfg


def _row6(t):
    return {r["ticker"]: r for r in L.rows(day=DAY.isoformat(), book="6")}.get(t)


def test_live_book_defaults_are_qqq_and_spcx_at_20_risk_7000_cap_and_off():
    c = L.validate_config({}, book="6")
    assert [t["symbol"] for t in c["tickers"]] == ["QQQ", "SPCX"]
    assert (c["mode"], c["risk"], c["cap"], c["params"]["target_r"], c["params"]["forced_exit"]) == \
        ("off", 20.0, 7000.0, 3.0, "15:55")
    with pytest.raises(ValueError):
        L.validate_config({"mode": "paper"}, book="6")          # the live book has no paper mode
    with pytest.raises(ValueError):
        L.validate_config({"mode": "live"})                     # and the paper book no live mode
    with pytest.raises(ValueError):
        L.validate_config({"risk": 501}, book="6")              # real-money ceilings
    with pytest.raises(ValueError):
        L.validate_config({"cap": 60000}, book="6")


def test_live_book_sends_a_real_money_bracket_at_20_risk(live, monkeypatch):
    noted = []
    monkeypatch.setattr(a, "_note_live_entry", lambda tag: noted.append(tag))
    cfg = _cfg6(risk=20, cap=7000)
    L.scan(_at(9, 45, 5), cfg, book="6")
    assert len(live.broker._trading.submitted) == 1
    req = live.broker._trading.submitted[0]
    assert req.symbol == "QQQ" and req.qty == 10                  # $20 / ($122 - $120)
    assert req.stop_loss.stop_price == 120.0 and req.take_profit.limit_price == 128.0
    assert req.client_order_id == "orb-20261116-QQQ" and noted == ["alpaca6"]
    assert _row6("QQQ")["status"] == "entered" and _row6("QQQ")["mode"] == "live"
    assert L.rows(day=DAY.isoformat()) == []                      # ORB Paper's log is untouched


def test_live_book_caps_the_position_at_the_max_position(live):
    L.scan(_at(9, 45, 5), _cfg6(risk=500, cap=7000), book="6")
    assert live.broker._trading.submitted[0].qty == 57            # min($500 / $2, $7,000 / $122)


def test_live_book_sizes_down_to_the_live_position_limit(live, monkeypatch):
    monkeypatch.setattr(a, "LIVE_MAX_POSITION_PCT", 20.0)          # 20% of $6,764.55 = $1,352.91
    L.scan(_at(9, 45, 5), _cfg6(risk=500, cap=7000), book="6")
    assert live.broker._trading.submitted[0].qty == 11            # $1,352.91 / $122
    r = _row6("QQQ")
    assert r["qty"] == 11 and "sized down from 57 to 11" in r["note"]


def test_live_book_refuses_when_not_armed(live, monkeypatch):
    monkeypatch.setattr(a, "LIVE_TRADING_ARMED", False)
    L.scan(_at(9, 45, 5), _cfg6(), book="6")
    assert live.broker._trading.submitted == []
    assert _row6("QQQ")["status"] == "blocked" and "not armed" in _row6("QQQ")["note"]
    r = a.app.test_client().post("/api/orb/live/config", json={"book": "6", "mode": "live"})
    assert r.status_code == 400 and "not armed" in r.get_json()["error"]


def test_live_book_refuses_a_paper_account_6(live):
    live.rec["paper"] = True
    _, ok, why = L.account_check("6")
    assert not ok and "not a live account" in why


def test_live_book_refuses_when_the_live_money_guard_says_no(live, monkeypatch):
    seen = []
    monkeypatch.setattr(a, "_live_entry_allowed",
                        lambda tag, notional=None, side=None: (seen.append((tag, notional, side)) or (False, "nope")))
    L.scan(_at(9, 45, 5), _cfg6(risk=20), book="6")
    assert live.broker._trading.submitted == [] and _row6("QQQ")["note"] == "nope"
    assert seen == [("alpaca6", 1220.0, "LONG")]                  # 10 shares x $122


def test_live_book_never_trades_a_ticker_the_crew_trades(live):
    _crew_rule(live.db, "QQQ_CAM_BREAKOUT_R3S3_V02_5MIN")
    _crew_rule(live.db, "GOOG_CAM_BREAKOUT_R3S3_V02_5MIN", enabled=0)   # a disabled rule doesn't count
    assert L.crew_live_tickers() == {"QQQ": ["QQQ_CAM_BREAKOUT_R3S3_V02_5MIN"]}
    r = a.app.test_client().post("/api/orb/live/config", json={"book": "6", "mode": "live"})
    assert r.status_code == 400 and "already trade QQQ" in r.get_json()["error"]
    L.scan(_at(9, 45, 5), _cfg6(), book="6")                      # saved directly: the entry check still holds
    assert live.broker._trading.submitted == []
    assert _row6("QQQ")["status"] == "blocked" and "through the crew" in _row6("QQQ")["note"]


def test_live_book_never_adds_to_a_position_it_did_not_open(live):
    live.broker.positions = [{"symbol": "QQQ", "qty": 3}]
    L.scan(_at(9, 45, 5), _cfg6(), book="6")
    assert live.broker._trading.submitted == [] and "already holds QQQ" in _row6("QQQ")["note"]


def test_live_book_stops_entering_when_crew_live_is_halted(live, monkeypatch):
    monkeypatch.setattr(a, "_daily_loss_halted_for", lambda tag: tag == "alpaca6")
    L.scan(_at(9, 45, 5), _cfg6(), book="6")
    assert live.broker._trading.submitted == [] and "daily loss guard" in _row6("QQQ")["note"]


def test_live_flatten_closes_only_the_orb_positions(live):
    """At 15:55 the live book closes what it opened; a crew position on the same account stays."""
    cfg = _cfg6()
    L.scan(_at(9, 45, 5), cfg, book="6")
    live.broker.positions = [{"symbol": "QQQ", "qty": 10}, {"symbol": "NVDA", "qty": 5}]

    def close(sym):
        live.broker.closed.append(sym)
        live.broker.positions = [p for p in live.broker.positions if p["symbol"] != sym]
        return {"success": True}
    live.broker.close_position = close
    L.flatten(_at(15, 55, 2), cfg, book="6")
    assert live.broker.closed == ["QQQ"]
    L.flatten(_at(15, 55, 7), cfg, book="6")
    assert live.broker.closed == ["QQQ"] and L._states["6"]["flattened"] == DAY
    assert live.broker.positions == [{"symbol": "NVDA", "qty": 5}]
    assert L._state["flattened"] is None                         # ORB Paper's flatten is its own


def test_live_flatten_still_closes_after_disarming_or_switching_off(live, monkeypatch):
    L.scan(_at(9, 45, 5), _cfg6(), book="6")
    monkeypatch.setattr(a, "LIVE_TRADING_ARMED", False)
    _cfg6(mode="off")
    live.broker.positions = [{"symbol": "QQQ", "qty": 10}]
    L.tick(_at(15, 55, 2))
    assert live.broker.closed == ["QQQ"]


def test_tick_runs_the_live_book_only_when_it_is_on(live, monkeypatch):
    calls = []
    monkeypatch.setattr(L, "scan", lambda now, cfg, book="8": calls.append(("scan", book)))
    _cfg(mode="off")
    _cfg6(mode="off")
    L.tick(_at(9, 35, 10))
    assert calls == []
    _cfg6(mode="live")
    L.tick(_at(9, 35, 10))
    assert calls == [("scan", "6")]


def test_kairos_stops_and_max_holds_skip_the_orb_positions_on_crew_live(live, monkeypatch):
    """Owned: the live book's tickers while it is live, and any it ordered today. Kairos'
    exits skip those and still act on the crew's positions on the same account."""
    _cfg6()
    assert ("alpaca6", "QQQ") in L.owned_positions() and ("alpaca6", "NVDA") not in L.owned_positions()

    live.broker.positions = [
        {"symbol": s, "qty": 100, "unrealized_pnl": -5000.0, "market_value": 10000.0,
         "avg_entry_price": 150.0, "current_price": 100.0, "side": "long"} for s in ("QQQ", "NVDA")]
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [{"tag": "alpaca6", "num": "6", "label": "Crew Live", "broker": live.broker}])
    monkeypatch.setattr(a, "_CURATED_TAGS", {"alpaca6"})
    monkeypatch.setattr(a, "MAX_POSITION_LOSS_PCT", -1.0)
    monkeypatch.setattr(a, "_auto_closed_symbols", set())
    monkeypatch.setattr(a, "_resolve_position_entry", lambda sym, broker: (None, None))
    a._check_position_stops()
    assert live.broker.closed == ["NVDA"]

    live.broker.closed.clear()
    from datetime import timedelta, timezone
    old = datetime.now(timezone.utc) - timedelta(hours=1)
    monkeypatch.setattr(a, "_max_hold_positions", {("alpaca6", "QQQ"): {"entry_time": old, "max_hold_mins": 15.0},
                                                   ("alpaca6", "NVDA"): {"entry_time": old, "max_hold_mins": 15.0}})
    monkeypatch.setattr(a, "_max_hold_close_sent", {})
    monkeypatch.setattr(a, "_max_hold_fail_ticks", {})
    monkeypatch.setattr(a, "MAX_HOLD_ENFORCEMENT", True)
    a._check_max_hold_exits()
    assert live.broker.closed == ["NVDA"]


def test_risk_status_shows_the_bracket_for_orb_rows_on_crew_live(live, monkeypatch):
    from datetime import datetime as _dt
    L.scan(_at(9, 45, 5), _cfg6(), book="6")

    class _Now(_dt):
        @classmethod
        def now(cls, tz=None):
            return _dt(2026, 11, 16, 10, 0, tzinfo=tz)
    monkeypatch.setattr(a, "datetime", _Now)
    monkeypatch.setattr(a, "_latest_positions", [
        {"symbol": "QQQ", "broker": "alpaca6", "qty": 10, "avg_entry_price": 122.05,
         "current_price": 123.0, "unrealized_pnl": 9.5, "market_value": 1230.0}])
    a.app.config["TESTING"] = True
    pos = a.app.test_client().get("/api/risk/status").get_json()["positions"]
    p = next(x for x in pos if x["broker"] == "alpaca6")
    assert (p["strategy"], p["est_stop_price"], p["target_price"], p["stop_kind"]) == ("ORB long", 120.0, 128.0, "bracket")


def test_live_status_reports_the_preflight_and_warns_when_the_limit_is_below_the_cap(live, monkeypatch):
    monkeypatch.setattr(a, "LIVE_MAX_POSITION_PCT", 20.0)
    _cfg6(mode="dry-run")
    s = a.app.test_client().get("/api/orb/live/status?book=6").get_json()
    assert s["book"] == "6" and s["real_money"] is True and s["modes"] == ["off", "dry-run", "live"]
    P = s["preflight"]
    assert P["armed"] is True and P["equity"] == 6764.55 and P["position_limit"] == 1352.91
    assert any("sized down" in w and "LIVE_MAX_POSITION_PCT" in w for w in P["warnings"])
    assert "preflight" not in a.app.test_client().get("/api/orb/live/status").get_json()


def test_preflight_opens_the_alpaca_client_before_reading_the_account(live):
    """Deploy of 2026-11-18: the client is created lazily, and the first status read found
    _trading still None ("'NoneType' object has no attribute 'get_account'")."""
    acct = live.broker._trading
    live.broker._trading = None

    def ensure():
        live.broker._trading = acct
    live.broker._ensure_client = ensure
    _cfg6(mode="dry-run")
    P = a.app.test_client().get("/api/orb/live/status?book=6").get_json()["preflight"]
    assert "error" not in P and P["equity"] == 6764.55


def test_the_orb_page_has_a_real_money_panel():
    html = open("templates/orb.html", encoding="utf-8").read()
    assert 'id="l6Panel"' in html and "REAL MONEY" in html and 'data-m="live"' in html
    assert "/api/orb/live/status?book=" in html and "book: B.book" in html


def test_the_crew_wire_skips_picks_on_the_live_orb_tickers(tmp_path, monkeypatch):
    import shutil
    import routes.crew as crew
    db = tmp_path / "wire.db"
    shutil.copy("trades.db", db)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM routing_rules")
    conn.execute("DELETE FROM crew_reports")
    conn.execute("DELETE FROM app_settings WHERE key LIKE 'ORB_LIVE_CONFIG%'")
    conn.commit()
    conn.close()

    def _fake_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c
    monkeypatch.setattr(a, "get_db", _fake_db)
    monkeypatch.setitem(a.ACCOUNTS_BY_TAG, "alpaca6", {"tag": "alpaca6", "broker": None})
    monkeypatch.setattr(a, "LIVE_SIZE_DOLLARS", 1000.0, raising=False)
    monkeypatch.setattr(a, "_fetch_alpaca_last_prices", lambda tks: {t: 50.0 for t in tks}, raising=False)
    slugs = ["QQQ_CAM_BREAKOUT_R4S4_V02_5MIN", "AAA_CAM_BREAKOUT_R4S4_V02_5MIN"]
    monkeypatch.setattr(crew, "_snapshot_top_picks",
                        lambda app_obj, n=9: ([{"strategy": s, "side": "both", "entry": "tv", "tier": "core"}
                                               for s in slugs], None))
    L._states["6"].update(L._fresh_state())
    try:
        L.save_config(L.validate_config({"mode": "live"}, book="6"), book="6")      # QQQ + SPCX
        a.app.config["TESTING"] = True
        r = a.app.test_client().post("/api/crew/wire_to_router", json={"source": "snapshot"})
        d = r.get_json()
        assert r.status_code == 200, d
        assert d["book"]["skipped_orb"] == [slugs[0]] and d["created"] == [slugs[1]]
    finally:
        L._states["6"].update(L._fresh_state())
