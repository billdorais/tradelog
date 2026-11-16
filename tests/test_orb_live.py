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
                    flatten_claim=None, cfg=None, cfg_ts=0.0)
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
    L._state.update(day=None, decided=set(), last_scan=0.0, warm={}, flattened=None, flatten_claim=None)


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


def test_tick_runs_only_in_its_windows(env, monkeypatch):
    calls = []
    monkeypatch.setattr(L, "scan", lambda now, cfg: calls.append(("scan", now.strftime("%H:%M:%S"))))
    monkeypatch.setattr(L, "flatten", lambda now, cfg: calls.append(("flatten", now.strftime("%H:%M"))))
    monkeypatch.setattr(L, "reconcile", lambda day, cfg: calls.append(("reconcile", day.isoformat())))
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
