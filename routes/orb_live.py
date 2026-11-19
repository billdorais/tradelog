"""ORB engine: the opening-range breakout (strategies/orb.py) traded on two books, with every
trade logged beside what the backtest did on the same day.

  Book "8" — ORB Paper (account 8, paper money). The engine owns the whole account.
  Book "6" — Crew Live (account 6, REAL MONEY). The account also trades the crew's
             Camarilla rules, so the engine owns only its own tickers there.

  - Signals come from orb.day_signal — the same function the backtest uses — fed only
    1-minute bars that have closed, so live and backtest can't drift apart.
  - Entries are Alpaca BRACKET orders: market in, stop at the far side of the range,
    limit target at signal close ± target_r × risk. client_order_id = orb-YYYYMMDD-TICKER,
    which Alpaca keeps unique per account: one entry per ticker per day even with two
    gunicorn workers running this loop, and across restarts.
  - The engine's positions are closed at forced_exit (15:55 ET). The 15:58 EOD close in
    app.py is the backstop. Kairos' own stops and max holds skip them: all of account 8
    (ACCOUNT_META "self_managed"), and on account 6 the pairs owned_positions() returns.
  - After 16:05 ET each trade is reconciled: real fill and exit, live R, and the
    backtest's trade for the same day, so slippage and drift are measured, not guessed.

Modes: off (default) · dry-run (logs the orders it would send) · paper (book 8) / live (book 6).
Hard guards, not configurable:
  - book 8: the account must be configured, paper, self_managed, and its key not shared.
  - book 6: the account must be configured and live, live trading armed, no Crew Live rule
    may trade the ticker, Crew Live must not already hold it, the account must not be halted
    (profit lock, daily loss guard, manual halt), the position must fit LIVE_MAX_POSITION_PCT
    of equity and the buying power (the size shrinks to fit), and app._live_entry_allowed
    must pass. Every check fails closed.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from flask import Blueprint, jsonify, request

from strategies import orb

log = logging.getLogger(__name__)
orb_live_bp = Blueprint("orb_live", __name__)

ET = ZoneInfo("America/New_York")
ACCOUNT = "8"
CONFIG_KEY = "ORB_LIVE_CONFIG"
MODES = ("off", "dry-run", "paper")
GROUPS = ("core", "watch")
MAX_TICKERS = 10
POLL_S = 5
SCAN_EVERY_S = 10          # bar fetches per worker while a candle may just have closed
SCAN_WINDOW_S = 150        # how long after a candle boundary to keep looking for its bars
STALE_DATA_S = 180         # newest bar older than this → don't decide
LATE_SIGNAL_S = 150        # a signal older than this (engine was down) is logged, not traded
FLATTEN_RESEND_S = 45      # a sent close may take this long to fill before it is re-sent
RECONCILE_AFTER = (16, 5)
LIVE_PARAMS = ("trend_filter", "target_r", "last_entry", "forced_exit")
CREW_LIVE_BROKERS = ("alpaca-live-6", "alpaca-paper-6")

_PARAMS = {"trend_filter": False, "target_r": 3.0, "last_entry": "10:30", "forced_exit": "15:55"}
DEFAULT_CONFIG = {
    "mode": "off",
    "tickers": [{"symbol": "QQQ", "group": "core"}, {"symbol": "GOOG", "group": "core"},
                {"symbol": "MU", "group": "watch"}, {"symbol": "NVDA", "group": "watch"},
                {"symbol": "SPCX", "group": "watch"}],
    "risk": 250.0,
    "cap": 25000.0,
    "params": dict(_PARAMS),
}
DEFAULT_CONFIG_6 = {
    "mode": "off",
    "tickers": [{"symbol": "QQQ", "group": "core"}, {"symbol": "SPCX", "group": "watch"}],
    "risk": 20.0,
    "cap": 7000.0,
    "params": dict(_PARAMS),
}

BOOKS = {
    "8": {"num": "8", "tag": "alpaca8", "name": "ORB Paper", "config_key": CONFIG_KEY,
          "table": "orb_live_trades", "modes": MODES, "send": "paper", "claim": "ORB_FLATTEN_",
          "real_money": False, "default": DEFAULT_CONFIG, "max_risk": 100_000, "max_cap": 10_000_000},
    "6": {"num": "6", "tag": "alpaca6", "name": "Crew Live", "config_key": "ORB_LIVE_CONFIG_6",
          "table": "orb_live_trades_a6", "modes": ("off", "dry-run", "live"), "send": "live",
          "claim": "ORB_FLATTEN_A6_", "real_money": True, "default": DEFAULT_CONFIG_6,
          "max_risk": 500, "max_cap": 50_000},
}

COLUMNS = ("grp", "mode", "status", "note", "side", "or_high", "or_low", "signal_time", "signal_close",
           "stop", "target", "qty", "client_order_id", "order_id", "fill_price", "fill_time",
           "exit_price", "exit_time", "exit_reason", "live_r", "live_pnl", "bt_side", "bt_entry",
           "bt_exit", "bt_reason", "bt_r", "slip_vs_bt", "slip_vs_signal", "flatten_at",
           "reconciled_at", "created_at", "updated_at")
ENTERED = ("entered", "filled")


def _fresh_state():
    return {"day": None, "decided": set(), "last_scan": 0.0, "warm": {}, "reconciled": None,
            "flattened": None, "flatten_claim": None, "flatten_sent": {}, "cfg": None, "cfg_ts": 0.0}


_state = _fresh_state()                      # book 8 (the name the tests and older code use)
_states = {"8": _state, "6": _fresh_state()}
_state_lock = threading.Lock()
_started = False
_owned = {"ts": 0.0, "val": frozenset()}
_preflight = {"ts": 0.0, "val": None}


def _app():
    import app as _a
    return _a


def _book(book) -> dict:
    b = BOOKS.get(str(book))
    if not b:
        raise ValueError("book must be 8 (ORB Paper) or 6 (Crew Live)")
    return b


def _st(book) -> dict:
    return _states[str(book)]


# ── config ───────────────────────────────────────────────────────────────────

def validate_config(body: dict, base: dict | None = None, book="8") -> dict:
    """A full, checked config from `body` laid over `base` (the current one). Raises ValueError."""
    B = _book(book)
    cur = json.loads(json.dumps(base or B["default"]))
    out = {**cur, **{k: v for k, v in (body or {}).items() if k in B["default"]}}
    if out["mode"] not in B["modes"]:
        raise ValueError(f"mode must be {', '.join(B['modes'][:-1])} or {B['modes'][-1]}")
    tks, seen = [], set()
    for t in out.get("tickers") or []:
        sym = str((t or {}).get("symbol") or "").strip().upper()
        grp = str((t or {}).get("group") or "core").strip().lower()
        if not sym.isalnum() or len(sym) > 6:
            raise ValueError(f"bad ticker {sym!r}")
        if grp not in GROUPS:
            raise ValueError("group must be core or watch")
        if sym not in seen:
            seen.add(sym)
            tks.append({"symbol": sym, "group": grp})
    if not 1 <= len(tks) <= MAX_TICKERS:
        raise ValueError(f"pick 1 to {MAX_TICKERS} tickers")
    out["tickers"] = tks
    try:
        out["risk"] = float(out["risk"])
        out["cap"] = None if out.get("cap") in (None, "", 0) else float(out["cap"])
    except (TypeError, ValueError):
        raise ValueError("risk and max position must be dollar amounts")
    if not 1 <= out["risk"] <= B["max_risk"]:
        raise ValueError(f"risk per trade must be $1 to ${B['max_risk']:,}")
    if out["cap"] is not None and not 100 <= out["cap"] <= B["max_cap"]:
        raise ValueError(f"max position must be $100 to ${B['max_cap']:,}")
    p = {k: v for k, v in {**_PARAMS, **(out.get("params") or {})}.items() if k in LIVE_PARAMS}
    norm = orb.normalize_params(p)                       # raises ValueError on a bad value
    out["params"] = {k: norm[k] for k in LIVE_PARAMS}
    return out


def load_config(fresh: bool = False, book="8") -> dict:
    B, st = _book(book), _st(book)
    with _state_lock:
        if not fresh and st["cfg"] is not None and time.time() - st["cfg_ts"] < 10:
            return st["cfg"]
    raw = _app()._load_setting(B["config_key"])
    try:
        cfg = validate_config(json.loads(raw), book=book) if raw else validate_config({}, book=book)
    except (ValueError, TypeError):
        log.warning("ORB %s: stored config is invalid; using defaults with mode off", B["name"])
        cfg = validate_config({}, book=book)
    with _state_lock:
        st["cfg"], st["cfg_ts"] = cfg, time.time()
    return cfg


def save_config(cfg: dict, book="8") -> None:
    B, st = _book(book), _st(book)
    _app()._save_setting(B["config_key"], json.dumps(cfg))
    with _state_lock:
        st["cfg"], st["cfg_ts"] = cfg, time.time()
        _owned["ts"] = 0.0


def engine_params(cfg: dict) -> dict:
    """Backtest params for the live config: the shared defaults plus the live overrides, 1 share."""
    return orb.normalize_params({**cfg["params"], "contracts": 1, "slippage_ticks": 1.0})


# ── the accounts (hard guards) ───────────────────────────────────────────────

def account_check(book="8", for_close: bool = False):
    """(record, ok, why) for a book's account. `for_close` drops the arming check: closing a
    real-money position must never wait on the switch that allows opening one."""
    a = _app()
    if str(book) == "6":
        rec = a.ACCOUNTS_BY_NUM.get("6")
        if not rec:
            return None, False, "Crew Live (account 6) isn't configured"
        if rec.get("paper", True) or getattr(rec.get("broker"), "_paper", True):
            return rec, False, "account 6 is not a live account; this book only trades Crew Live's real money"
        if not for_close and not getattr(a, "LIVE_TRADING_ARMED", False):
            return rec, False, "live trading is not armed (LIVE_TRADING_ARMED=1 in Railway)"
        return rec, True, ""
    rec = a.ACCOUNTS_BY_NUM.get(ACCOUNT)
    if not rec:
        return None, False, ("ORB Paper (account 8) isn't configured: create an Alpaca paper account and set "
                             "ALPACA_KEY8, ALPACA_SECRET8 and ALPACA_PAPER8=true")
    if not rec.get("paper") or not getattr(rec.get("broker"), "_paper", False):
        return rec, False, "account 8 is not a paper account; the ORB engine only trades paper"
    if not rec.get("self_managed"):
        return rec, False, "account 8 isn't marked self_managed, so Kairos' exits would interfere"
    # Slot 8 took over Crew Paper's Alpaca account (its keys moved from slot 4). If the old
    # slot still carries the same key, Kairos rules aimed at that slot would trade the
    # ORB account, so refuse until the duplicate is removed.
    key = getattr(rec.get("broker"), "_key", None)
    if key:
        for n, other in (a.ACCOUNTS_BY_NUM or {}).items():
            if n != ACCOUNT and getattr((other or {}).get("broker"), "_key", None) == key:
                return rec, False, (f"account 8 uses the same Alpaca key as account {n}; delete "
                                    f"ALPACA_KEY{n}/SECRET{n}/PAPER{n} so only the ORB engine trades it")
    return rec, True, ""


def crew_live_tickers() -> dict:
    """{TICKER: [strategy, ...]} for every enabled Signal Router rule that routes to Crew Live.
    The ORB must not trade these on account 6: one account holds one position per symbol."""
    a = _app()
    conn = a.get_db()
    try:
        got = conn.execute("SELECT nodes FROM routing_rules WHERE enabled=1").fetchall()
    finally:
        conn.close()
    out = {}
    for r in got:
        raw = r[0]
        try:
            nodes = json.loads(raw) if isinstance(raw, str) else (raw or [])
        except Exception:
            continue
        if not any(n.get("type") == "broker" and (n.get("value") or "").lower() in CREW_LIVE_BROKERS
                   for n in nodes):
            continue
        for n in nodes:
            if n.get("type") == "strategy" and n.get("value"):
                slug = n["value"].strip().upper()
                out.setdefault(slug.split("_", 1)[0], []).append(slug)
    return out


def _live_gate(rec, sym: str, plan: dict):
    """(why, qty, note) for a real-money entry on Crew Live. A non-empty `why` refuses it. The size
    shrinks to fit the live position limit (LIVE_MAX_POSITION_PCT of equity, and buying power)."""
    a, tag = _app(), BOOKS["6"]["tag"]
    for fn, label in (("_profit_lock_halted_for", "profit lock"), ("_daily_loss_halted_for", "daily loss guard"),
                      ("_manual_halted_for", "manual halt")):
        if getattr(a, fn)(tag):
            return f"Crew Live is halted for the day ({label})", 0, ""
    try:
        crew = crew_live_tickers()
    except Exception as e:
        return f"could not read the Signal Router rules ({e})", 0, ""
    if sym in crew:
        return f"Crew Live already trades {sym} through the crew ({', '.join(crew[sym])})", 0, ""
    try:
        rec["broker"]._invalidate_pos_cache()
        held = [p for p in rec["broker"].get_positions(raise_on_error=True)
                if str(p.get("symbol") or "").upper() == sym and abs(float(p.get("qty") or 0)) > 0]
    except Exception as e:
        return f"could not read Crew Live's positions ({e})", 0, ""
    if held:
        return f"Crew Live already holds {sym}; the ORB only trades a ticker it opens itself", 0, ""
    try:
        snap = a._live_account_snapshot(tag)
    except Exception as e:
        return f"could not read the live account ({e})", 0, ""
    room = float(snap.get("equity") or 0) * float(a.LIVE_MAX_POSITION_PCT) / 100.0
    bp = float(snap.get("buying_power") or 0)
    if bp > 0:
        room = min(room, bp)
    qty, close, note = plan["qty"], plan["close"], ""
    if qty * close > room:
        qty = int(room // close)
        if qty < 1:
            return f"one share (${close:,.2f}) is more than the ${room:,.0f} live position limit", 0, ""
        note = (f"sized down from {plan['qty']} to {qty} shares to fit the ${room:,.0f} live position limit "
                f"({float(a.LIVE_MAX_POSITION_PCT):g}% of equity)")
    ok, why = a._live_entry_allowed(tag, notional=qty * close, side="LONG" if plan["side"] > 0 else "SHORT")
    if not ok:
        return why, 0, ""
    return "", qty, note


# ── storage ──────────────────────────────────────────────────────────────────

_REAL = {"or_high", "or_low", "signal_close", "stop", "target", "fill_price", "exit_price", "live_r",
         "live_pnl", "bt_entry", "bt_exit", "bt_r", "slip_vs_bt", "slip_vs_signal"}


def ensure_table(book=None):
    a = _app()
    conn = a.get_db()
    try:
        cols = ", ".join(f"{c} {'REAL' if c in _REAL else 'INTEGER' if c == 'qty' else 'TEXT'}" for c in COLUMNS)
        for b in ([str(book)] if book else BOOKS):
            conn.execute(f"CREATE TABLE IF NOT EXISTS {BOOKS[b]['table']} (day TEXT NOT NULL, ticker TEXT NOT NULL, "
                         f"{cols}, PRIMARY KEY (day, ticker))")
        conn.commit()
    finally:
        conn.close()


def upsert(day: str, ticker: str, book="8", **fields):
    """Insert or update one (day, ticker) row, touching only the given columns."""
    table = _book(book)["table"]
    fields = {k: v for k, v in fields.items() if k in COLUMNS}
    now = datetime.now(timezone.utc).isoformat()
    fields["updated_at"] = now
    a = _app()
    ph = a.placeholder()
    cols = ["day", "ticker", "created_at", *fields]
    vals = [day, ticker, now, *fields.values()]
    sets = ", ".join(f"{k}=excluded.{k}" for k in fields)
    conn = a.get_db()
    try:
        conn.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join([ph] * len(cols))}) "
                     f"ON CONFLICT (day, ticker) DO UPDATE SET {sets}", vals)
        conn.commit()
    finally:
        conn.close()


def claim(key: str) -> bool:
    """True for exactly one caller per key, across gunicorn workers and restarts (an INSERT
    that does nothing when the key exists). Used so only one worker runs the flatten —
    two concurrent close_position calls can race into a double close."""
    a = _app()
    ph = a.placeholder()
    conn = a.get_db()
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO app_settings (key, value) VALUES ({ph}, {ph}) ON CONFLICT (key) DO NOTHING",
                    (key, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()


def rows(day_from: str | None = None, day: str | None = None, limit: int = 500, book="8") -> list:
    table = _book(book)["table"]
    a = _app()
    ph = a.placeholder()
    where, args = [], []
    if day:
        where.append(f"day={ph}")
        args.append(day)
    elif day_from:
        where.append(f"day>={ph}")
        args.append(day_from)
    sql = (f"SELECT day, ticker, {', '.join(COLUMNS)} FROM {table} "
           f"{'WHERE ' + ' AND '.join(where) if where else ''} ORDER BY day DESC, ticker LIMIT {int(limit)}")
    conn = a.get_db()
    try:
        out = conn.execute(sql, args).fetchall()
    finally:
        conn.close()
    return [dict(zip(("day", "ticker", *COLUMNS), tuple(r))) for r in out]


def _ordered_today(day: date, book) -> set:
    """Tickers the book sent an order for on `day` (it owns those positions)."""
    return {r["ticker"] for r in rows(day=day.isoformat(), book=book) if r.get("order_id")}


def owned_positions() -> frozenset:
    """(tag, SYMBOL) pairs on Crew Live that the ORB owns, so Kairos' trailing stops, take-profit
    and max holds leave them alone: the live book's tickers while it is in live mode, plus any
    ticker it sent an order for today. Account 8 is skipped as a whole (self_managed) and is not
    listed. Cached 5 s; on a read error the last answer stands rather than an empty one."""
    now = time.time()
    with _state_lock:
        if now - _owned["ts"] < 5:
            return _owned["val"]
    try:
        tag = BOOKS["6"]["tag"]
        cfg = load_config(book="6")
        syms = {t["symbol"] for t in cfg["tickers"]} if cfg["mode"] == "live" else set()
        syms |= _ordered_today(datetime.now(ET).date(), "6")
        val = frozenset((tag, s) for s in syms)
    except Exception as e:
        log.debug("ORB owned positions: %s", e)
        with _state_lock:
            return _owned["val"]
    with _state_lock:
        _owned.update(ts=now, val=val)
    return val


# ── bars ─────────────────────────────────────────────────────────────────────

def _frame(rows_) -> pd.DataFrame | None:
    if not rows_:
        return None
    return pd.DataFrame({
        "open": np.fromiter((r["o"] for r in rows_), float, len(rows_)),
        "high": np.fromiter((r["h"] for r in rows_), float, len(rows_)),
        "low": np.fromiter((r["l"] for r in rows_), float, len(rows_)),
        "close": np.fromiter((r["c"] for r in rows_), float, len(rows_)),
    }, index=pd.DatetimeIndex(pd.to_datetime([r["t"] for r in rows_], utc=True)))


def fetch_bars(symbols, start: datetime, end: datetime | None = None, adjustment: str = "raw") -> dict:
    """1-minute bars for several symbols in one request → {SYM: frame}. Same client and feed
    as the backtest's orb.load_alpaca. Monkeypatched in tests."""
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    client = StockHistoricalDataClient(api_key=os.environ.get("ALPACA_KEY") or None,
                                       secret_key=os.environ.get("ALPACA_SECRET") or None, raw_data=True)
    req = StockBarsRequest(symbol_or_symbols=list(symbols), timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                           feed=orb._alpaca_feed(), adjustment=adjustment,
                           start=start.astimezone(timezone.utc),
                           end=end.astimezone(timezone.utc) if end else None)
    raw = client.get_stock_bars(req)
    raw = raw if isinstance(raw, dict) else {}
    return {s: _frame(raw.get(s) or []) for s in symbols}


def bars_for(symbols, day: date, p: dict, now: datetime | None = None, book="8") -> dict:
    """Today's closed 1-minute bars from the range start (plus trend warm-up when the filter
    is on, fetched once a day), as {SYM: frame}. A bar still forming is dropped."""
    st = _st(book)
    start = datetime.combine(day, dtime(0, 0), ET) + timedelta(minutes=orb._hm(p["or_start"]))
    today = fetch_bars(symbols, start, now)
    out = {}
    with _state_lock:
        warm = dict(st["warm"]) if st["day"] == day else {}
    need = [s for s in symbols if p["trend_filter"] and s not in warm]
    if need:
        w0 = datetime.combine(day - timedelta(days=orb.WARMUP_DAYS + 7), dtime(4, 0), ET)
        fetched = fetch_bars(need, w0, start, adjustment="all")
        warm.update(fetched)
        with _state_lock:
            if st["day"] == day:
                st["warm"].update(fetched)
    cutoff = pd.Timestamp(now or datetime.now(timezone.utc)).tz_convert("UTC") - pd.Timedelta(minutes=1)
    for s in symbols:
        df = today.get(s)
        if df is not None and now is not None:
            df = df[df.index <= cutoff]                 # bar starting in the last minute is still forming
        parts = [x for x in (warm.get(s) if p["trend_filter"] else None, df) if x is not None and len(x)]
        out[s] = pd.concat(parts).sort_index() if parts else None
    return out


# ── decisions ────────────────────────────────────────────────────────────────

def order_plan(sig: dict, p: dict, risk: float, cap: float | None) -> dict:
    """Bracket legs and size for a signal. Reference price = the breakout candle's close."""
    side, close = sig["side"], sig["close"]
    stop = sig["lo"] if side > 0 else sig["hi"]
    risk_ps = (close - stop) * side
    target = orb._round_tick(close + side * p["target_r"] * risk_ps, 0.01, up=side > 0)
    qty = orb.shares_for_risk(risk_ps, close, risk, cap)
    return {"side": side, "stop": round(stop, 2), "target": round(target, 2), "risk_ps": round(risk_ps, 4),
            "qty": qty, "close": close}


def client_order_id(day: date, ticker: str) -> str:
    return f"orb-{day:%Y%m%d}-{ticker.upper()}"


def submit_bracket(broker, ticker: str, plan: dict, coid: str):
    """Send the bracket. Returns (order_id, note). A duplicate client_order_id means another
    worker (or a restart) already entered — that counts as entered, not as an error."""
    from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
    from alpaca.trading.requests import MarketOrderRequest, StopLossRequest, TakeProfitRequest
    broker._ensure_client()
    req = MarketOrderRequest(symbol=ticker, qty=plan["qty"],
                             side=OrderSide.BUY if plan["side"] > 0 else OrderSide.SELL,
                             time_in_force=TimeInForce.DAY, order_class=OrderClass.BRACKET,
                             client_order_id=coid,
                             take_profit=TakeProfitRequest(limit_price=plan["target"]),
                             stop_loss=StopLossRequest(stop_price=plan["stop"]))
    try:
        o = broker._trading.submit_order(req)
        broker._invalidate_pos_cache()
        return str(o.id), ""
    except Exception as e:
        msg = str(e)
        if "client_order_id" in msg and "unique" in msg.lower():
            o = broker._trading.get_order_by_client_id(coid)
            return str(o.id), "already entered (duplicate id)"
        raise


def _new_day(day: date, book="8"):
    st = _st(book)
    with _state_lock:
        if st["day"] != day:
            st.update(day=day, decided=set(), last_scan=0.0, warm={})


def _decided_today(day: date, book="8") -> set:
    """Tickers already decided today, from this worker's memory and from the table (another worker
    or an earlier process)."""
    with _state_lock:
        mine = set(_st(book)["decided"])
    try:
        done = {r["ticker"] for r in rows(day=day.isoformat(), book=book) if r["status"] not in (None, "waiting")}
    except Exception:
        done = set()
    return mine | done


def scan(now: datetime, cfg: dict, book="8"):
    """Fetch closed bars and decide every ticker still open today."""
    day = now.date()
    p = engine_params(cfg)
    groups = {t["symbol"]: t["group"] for t in cfg["tickers"]}
    todo = [s for s in groups if s not in _decided_today(day, book)]
    if not todo:
        return
    data = bars_for(todo, day, p, now=now, book=book)
    now_ns = pd.Timestamp(now).value
    for sym in todo:
        df = data.get(sym)
        try:
            if df is None or not len(df):
                continue
            last_end = int(df.index[-1].value) + 60_000_000_000
            if now_ns - last_end > STALE_DATA_S * 1e9:
                upsert(day.isoformat(), sym, book=book, grp=groups[sym], mode=cfg["mode"], status="waiting",
                       note=f"stale data: newest bar ended {pd.Timestamp(last_end, tz='UTC').tz_convert(ET):%H:%M}")
                continue
            m = orb.Market.from_frame(df)
            sig = orb.day_signal(m, day, p, asof_ns=last_end)
        except Exception as e:
            log.warning("ORB %s: %s signal failed: %s", _book(book)["name"], sym, e)
            continue
        if sig["status"] == "waiting":
            continue
        with _state_lock:
            _st(book)["decided"].add(sym)
        base = {"grp": groups[sym], "mode": cfg["mode"], "or_high": sig.get("hi"), "or_low": sig.get("lo")}
        if sig["status"] == "skip":
            upsert(day.isoformat(), sym, book=book, status="no-trade", note=sig["reason"].replace("_", " "), **base)
            continue
        enter(now, day, sym, sig, p, cfg, base, book=book)


def enter(now: datetime, day: date, sym: str, sig: dict, p: dict, cfg: dict, base: dict, book="8"):
    B = _book(book)
    plan = order_plan(sig, p, cfg["risk"], cfg["cap"])
    sig_t = pd.Timestamp(sig["candle_start_ns"], tz="UTC").tz_convert(ET).strftime("%H:%M")
    coid = client_order_id(day, sym)
    row = {**base, "side": "long" if plan["side"] > 0 else "short", "signal_time": sig_t,
           "signal_close": plan["close"], "stop": plan["stop"], "target": plan["target"], "qty": plan["qty"],
           "client_order_id": coid}
    d = day.isoformat()
    late = (pd.Timestamp(now).value - sig["decide_ns"]) / 1e9
    if late > LATE_SIGNAL_S:
        upsert(d, sym, book=book, status="missed", note=f"signal from {sig_t} found {late / 60:.0f} min late", **row)
        return
    if plan["qty"] < 1:
        upsert(d, sym, book=book, status="skipped",
               note=f"one share risks ${plan['risk_ps']:.2f}, more than ${cfg['risk']:.0f}", **row)
        return
    if cfg["mode"] == "dry-run":
        upsert(d, sym, book=book, status="dry-run",
               note=f"would {'buy' if plan['side'] > 0 else 'short'} {plan['qty']} (not sent)", **row)
        return
    if cfg["mode"] != B["send"]:
        return
    rec, ok, why = account_check(book)
    if not ok or sym not in {t["symbol"] for t in cfg["tickers"]}:
        upsert(d, sym, book=book, status="blocked", note=why or "ticker not in the config", **row)
        return
    note = ""
    if B["real_money"]:
        why, qty, note = _live_gate(rec, sym, plan)
        if why:
            upsert(d, sym, book=book, status="blocked", note=why[:240], **row)
            log.warning("ORB %s: %s entry blocked: %s", B["name"], sym, why)
            return
        plan = {**plan, "qty": qty}
        row["qty"] = qty
    try:
        oid, dup = submit_bracket(rec["broker"], sym, plan, coid)
        if B["real_money"] and not dup:
            _app()._note_live_entry(B["tag"])
        upsert(d, sym, book=book, status="entered", order_id=oid,
               note="; ".join(x for x in (note, dup) if x) or None, **row)
        with _state_lock:
            _owned["ts"] = 0.0
        log.info("ORB %s: %s %s %d (stop %.2f, target %.2f) order %s", B["name"], row["side"], sym, plan["qty"],
                 plan["stop"], plan["target"], oid)
    except Exception as e:
        upsert(d, sym, book=book, status="rejected", note=str(e)[:240], **row)
        log.warning("ORB %s: %s bracket rejected: %s", B["name"], sym, e)


def flatten(now: datetime, cfg: dict, book="8"):
    """Close the book's positions at the forced exit: every position on account 8; on Crew Live
    only the tickers the ORB ordered today, never a crew position."""
    B, st = _book(book), _st(book)
    day = now.date()
    with _state_lock:
        if st["flattened"] == day:
            return
        mine = st.get("flatten_claim") == day
    rec, ok, why = account_check(book, for_close=True)
    if not ok:
        return
    if not mine:
        if not claim(f"{B['claim']}{day.isoformat()}"):
            with _state_lock:
                st["flattened"] = day                 # the other worker owns today's flatten
            return
        with _state_lock:
            st["flatten_claim"] = day
    br = rec["broker"]
    try:
        own = None if book == "8" else _ordered_today(day, book)
        br._invalidate_pos_cache()
        positions = br.get_positions(raise_on_error=True)
    except Exception as e:
        log.warning("ORB %s: flatten could not read positions or its trades: %s", B["name"], e)
        return
    open_syms = [str(p.get("symbol") or "").upper() for p in positions
                 if abs(float(p.get("qty") or 0)) > 0 and p.get("symbol")]
    if own is not None:
        open_syms = [s for s in open_syms if s in own]
    # Done only when the broker reports the book flat — not when the closes were
    # merely sent. On 2026-11-16 MU's close did not take and the 15:58 EOD backstop
    # closed it; the engine had already marked the day flat.
    if not open_syms:
        with _state_lock:
            st["flattened"] = day
        log.info("ORB %s: flat at %s", B["name"], now.strftime("%H:%M:%S"))
        return
    with _state_lock:
        sent = st.setdefault("flatten_sent", {})
        if sent.get("_day") != day:
            sent.clear()
            sent["_day"] = day
    stamp = now.astimezone(timezone.utc).isoformat()
    for sym in open_syms:
        last = sent.get(sym)
        if last is not None and (now - last).total_seconds() < FLATTEN_RESEND_S:
            continue                                 # a close is in flight (fills can lag)
        try:
            res = br.close_position(sym) or {}
        except Exception as e:
            res = {"success": False, "error": str(e)}
        if res.get("success"):
            if last is None:
                upsert(day.isoformat(), sym, book=book, flatten_at=stamp)
            sent[sym] = now
            log.info("ORB %s: flatten %s at %s: %s", B["name"], sym, now.strftime("%H:%M:%S"), res)
        else:
            err = str(res.get("error") or res)[:200]
            upsert(day.isoformat(), sym, book=book, note=f"3:55 close failed at {now:%H:%M:%S} ET, retrying: {err}")
            log.warning("ORB %s: flatten %s failed at %s (will retry): %s", B["name"], sym,
                        now.strftime("%H:%M:%S"), err)


# ── reconcile ────────────────────────────────────────────────────────────────

def _f(x):
    try:
        return float(x) if x not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _ts(x):
    return x.isoformat() if hasattr(x, "isoformat") else (str(x) if x else None)


def live_result(broker, row: dict) -> dict:
    """Fill, exit and live R for an entered row, read back from Alpaca."""
    from alpaca.trading.enums import QueryOrderStatus
    from alpaca.trading.requests import GetOrderByIdRequest, GetOrdersRequest
    broker._ensure_client()
    o = broker._trading.get_order_by_id(row["order_id"], filter=GetOrderByIdRequest(nested=True))
    fill = _f(getattr(o, "filled_avg_price", None))
    if not fill:
        return {"status": "rejected", "note": f"entry not filled ({getattr(o, 'status', '?')})"}
    side = 1 if row["side"] == "long" else -1
    out = {"status": "filled", "fill_price": fill, "fill_time": _ts(getattr(o, "filled_at", None))}
    exit_px = exit_t = reason = None
    for leg in getattr(o, "legs", None) or []:
        px = _f(getattr(leg, "filled_avg_price", None))
        if px:
            typ = str(getattr(leg, "order_type", "") or getattr(leg, "type", "")).lower()
            exit_px, exit_t, reason = px, _ts(getattr(leg, "filled_at", None)), "target" if "limit" in typ else "stop"
    if exit_px is None:                                  # closed by the 15:55 flatten (or a Kairos backstop)
        closed = broker._trading.get_orders(filter=GetOrdersRequest(
            status=QueryOrderStatus.CLOSED, symbols=[row["ticker"]], after=getattr(o, "filled_at", None), limit=50))
        want = "sell" if side > 0 else "buy"
        leg_ids = {str(getattr(l, "id", "")) for l in (getattr(o, "legs", None) or [])}
        for c in sorted(closed, key=lambda c: str(getattr(c, "filled_at", "") or "")):
            if str(getattr(c, "id", "")) in leg_ids or str(getattr(c, "id", "")) == str(o.id):
                continue
            if want in str(getattr(c, "side", "")).lower() and _f(getattr(c, "filled_avg_price", None)):
                exit_px, exit_t, reason = _f(c.filled_avg_price), _ts(getattr(c, "filled_at", None)), "time"
                break
    if exit_px is None:
        out["note"] = "no exit found yet"
        return out
    risk = (fill - row["stop"]) * side
    qty = int(row.get("qty") or 0)
    out.update(status="exited", exit_price=exit_px, exit_time=exit_t, exit_reason=reason,
               live_pnl=round((exit_px - fill) * side * qty, 2),
               live_r=round((exit_px - fill) * side / risk, 4) if risk > 0 else None)
    return out


def backtest_day(day: date, symbols, p: dict, book="8") -> dict:
    """What the backtest did on `day` for each symbol, on the same bars: {SYM: trade dict or None}."""
    data = bars_for(symbols, day, p, now=None, book=book)
    out = {}
    for s in symbols:
        df = data.get(s)
        try:
            m = orb.Market.from_frame(df) if df is not None and len(df) >= 10 else None
            tr = orb.simulate(m, p, "QQQ", day, day)["trades"] if m else []
        except Exception as e:
            log.warning("ORB live: backtest for %s %s failed: %s", s, day, e)
            tr = []
        out[s] = tr[0] if tr else None
    return out


def reconcile(day: date, cfg: dict, book="8") -> int:
    """Fill in live results and the backtest's trade for every ticker today. Returns rows written."""
    p = engine_params(cfg)
    have = {r["ticker"]: r for r in rows(day=day.isoformat(), book=book)}
    syms = sorted({t["symbol"] for t in cfg["tickers"]} | set(have))
    groups = {t["symbol"]: t["group"] for t in cfg["tickers"]}
    bt = backtest_day(day, syms, p, book=book)
    rec, ok, _ = account_check(book, for_close=True)
    n = 0
    for s in syms:
        row = have.get(s) or {"ticker": s, "status": None}
        f = {}
        t = bt.get(s)
        f.update(bt_side=t["side"] if t else None, bt_entry=t["entry"] if t else None,
                 bt_exit=t["exit"] if t else None, bt_reason=t["reason"] if t else None, bt_r=t["r"] if t else None)
        if row.get("status") in ENTERED and row.get("order_id") and ok:
            try:
                f.update(live_result(rec["broker"], {**row, "ticker": s}))
            except Exception as e:
                f["note"] = f"reconcile failed: {str(e)[:160]}"
        fill = f.get("fill_price") or row.get("fill_price")
        if fill and row.get("side"):
            sd = 1 if row["side"] == "long" else -1
            if t and t["side"] == row["side"]:
                f["slip_vs_bt"] = round((fill - t["entry"]) * sd, 4)
            if row.get("signal_close"):
                f["slip_vs_signal"] = round((fill - row["signal_close"]) * sd, 4)
        if row.get("status") is None:
            f.update(status="no-trade" if not t else "missed", grp=groups.get(s), mode=cfg["mode"],
                     note=None if not t else "the backtest traded; the engine logged nothing")
        f["reconciled_at"] = datetime.now(timezone.utc).isoformat()
        upsert(day.isoformat(), s, book=book, **f)
        n += 1
    return n


# ── loop ─────────────────────────────────────────────────────────────────────

def tick(now: datetime):
    """One pass of the engine at ET time `now`, for each book."""
    for b in BOOKS:
        try:
            _tick_book(now, b)
        except Exception as e:
            log.warning("ORB %s: tick failed: %s", BOOKS[b]["name"], e)


def _tick_book(now: datetime, book: str):
    B, st = BOOKS[book], _st(book)
    cfg = load_config(book=book)
    if now.weekday() >= 5:
        return
    day = now.date()
    p = engine_params(cfg)
    hm = now.hour * 60 + now.minute
    or1, last, fexit = orb._hm(p["or_end"]), orb._hm(p["last_entry"]), orb._hm(p["forced_exit"])
    in_flatten = fexit <= hm < 16 * 60
    if cfg["mode"] != B["send"]:
        # Switched off or to dry run after entering: the positions it opened still close at 15:55.
        if in_flatten and _ordered_today(day, book):
            flatten(now, cfg, book=book)
        if cfg["mode"] == "off":
            return
    _new_day(day, book)
    tf = p["breakout_tf"]
    if or1 + tf <= hm <= last + 2:
        secs = (hm - or1) % tf * 60 + now.second       # seconds since the last candle boundary
        with _state_lock:
            due = secs <= SCAN_WINDOW_S and time.time() - st["last_scan"] >= SCAN_EVERY_S
            if due:
                st["last_scan"] = time.time()
        if due:
            scan(now, cfg, book=book)
    if cfg["mode"] == B["send"] and in_flatten:
        flatten(now, cfg, book=book)
    if (now.hour, now.minute) >= RECONCILE_AFTER:
        with _state_lock:
            done = st["reconciled"] == day
        if not done:
            try:
                reconcile(day, cfg, book=book)
                with _state_lock:
                    st["reconciled"] = day
            except Exception as e:
                log.warning("ORB %s: reconcile failed (will retry): %s", B["name"], e)


def _loop():
    time.sleep(15)
    try:
        ensure_table()
    except Exception as e:
        log.warning("ORB live: table setup failed: %s", e)
    while True:
        try:
            tick(datetime.now(ET))
        except Exception as e:
            log.warning("ORB live loop: %s", e)
        time.sleep(POLL_S)


def start_engine():
    """Start the engine thread (not under pytest, not with ORB_LIVE=0). Idles while both books are off."""
    global _started
    if _started or "pytest" in sys.modules or os.environ.get("ORB_LIVE") == "0":
        return
    _started = True
    threading.Thread(target=_loop, daemon=True, name="orb-live").start()


# ── endpoints ────────────────────────────────────────────────────────────────

def _summary(rs: list) -> dict:
    out = {}
    for g in ("core", "watch", "all"):
        sel = [r for r in rs if g == "all" or r.get("grp") == g]
        live = [r["live_r"] for r in sel if r.get("live_r") is not None]
        btr = [r["bt_r"] for r in sel if r.get("bt_r") is not None]
        slip = [r["slip_vs_bt"] for r in sel if r.get("slip_vs_bt") is not None]
        both = [(r["live_r"], r["bt_r"]) for r in sel if r.get("live_r") is not None and r.get("bt_r") is not None]
        out[g] = {"live_trades": len(live), "live_total_r": round(sum(live), 2) if live else 0.0,
                  "live_avg_r": round(sum(live) / len(live), 3) if live else None,
                  "live_pnl": round(sum(r.get("live_pnl") or 0 for r in sel), 2),
                  "bt_trades": len(btr), "bt_total_r": round(sum(btr), 2) if btr else 0.0,
                  "bt_avg_r": round(sum(btr) / len(btr), 3) if btr else None,
                  "avg_slip_vs_bt": round(sum(slip) / len(slip), 4) if slip else None,
                  "matched": len(both),
                  "avg_gap_r": round(sum(a - b for a, b in both) / len(both), 3) if both else None}
    return out


def live_preflight(cfg: dict) -> dict:
    """What stands between the live book and an order today, read from the broker (cached 30 s)."""
    a = _app()
    now = time.time()
    with _state_lock:
        hit = _preflight["val"] if now - _preflight["ts"] < 30 else None
    if hit is None:
        rec = a.ACCOUNTS_BY_NUM.get("6")
        hit = {"armed": bool(getattr(a, "LIVE_TRADING_ARMED", False)),
               "max_position_pct": float(getattr(a, "LIVE_MAX_POSITION_PCT", 0) or 0),
               "size_dollars": float(getattr(a, "LIVE_SIZE_DOLLARS", 0) or 0)}
        if rec:
            try:
                acct = rec["broker"]._trading.get_account()
                for k in ("equity", "buying_power"):
                    hit[k] = float(getattr(acct, k, 0) or 0)
                hit["shorting_enabled"] = bool(getattr(acct, "shorting_enabled", False))
                hit["trading_blocked"] = bool(getattr(acct, "trading_blocked", False)
                                              or getattr(acct, "account_blocked", False))
            except Exception as e:
                hit["error"] = f"could not read Crew Live: {e}"
        with _state_lock:
            _preflight.update(ts=now, val=hit)
    out = dict(hit)
    eq, bp = out.get("equity") or 0, out.get("buying_power") or 0
    limit = eq * out["max_position_pct"] / 100.0 if eq else None
    if limit is not None and bp > 0:
        limit = min(limit, bp)
    out["position_limit"] = round(limit, 2) if limit is not None else None
    warn = []
    if out.get("error"):
        warn.append(out["error"])
    if not out["armed"]:
        warn.append("Live trading is not armed (LIVE_TRADING_ARMED=1), so nothing will be sent.")
    if out["size_dollars"] <= 0:
        warn.append("LIVE_SIZE_DOLLARS is 0; the live-money guard refuses every entry until it is set.")
    if out.get("trading_blocked"):
        warn.append("Alpaca reports trading blocked on Crew Live.")
    if limit is not None and cfg.get("cap") and limit < cfg["cap"]:
        warn.append(f"Positions are capped at ${limit:,.0f} ({out['max_position_pct']:g}% of ${eq:,.0f} equity"
                    f"{', buying power' if bp and bp < eq * out['max_position_pct'] / 100.0 else ''}), below the "
                    f"${cfg['cap']:,.0f} max position: trades will be sized down to fit, so they risk less than "
                    f"${cfg['risk']:,.0f}. Raise LIVE_MAX_POSITION_PCT in Railway to allow the full size.")
    if out.get("shorting_enabled") is False and "error" not in out:
        warn.append("Shorting is off on Crew Live, so short breakouts will be blocked (longs only).")
    try:
        crew = crew_live_tickers()
        clash = sorted(t["symbol"] for t in cfg["tickers"] if t["symbol"] in crew)
    except Exception:
        clash = []
    if clash:
        warn.append(f"Crew Live's rules already trade {', '.join(clash)}; the ORB will not trade "
                    f"{'it' if len(clash) == 1 else 'them'} on this account.")
    out["crew_overlap"], out["warnings"] = clash, warn
    return out


def _req_book():
    b = str((request.get_json(silent=True) or {}).get("book") or request.args.get("book") or "8")
    _book(b)
    return b


@orb_live_bp.route("/api/orb/live/status")
def api_status():
    try:
        book = _req_book()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    B = BOOKS[book]
    try:
        ensure_table(book)
        cfg = load_config(fresh=True, book=book)
        recent = rows(day_from=(date.today() - timedelta(days=int(request.args.get("days", 120) or 120))).isoformat(),
                      book=book)
    except Exception as e:
        return jsonify({"error": f"status unavailable: {e}"}), 500
    rec, ok, why = account_check(book)
    now = datetime.now(ET)
    today = now.date().isoformat()
    out = {"book": book, "real_money": B["real_money"], "modes": list(B["modes"]), "config": cfg,
           "account": {"num": B["num"], "configured": rec is not None, "label": (rec or {}).get("label") or B["name"],
                       "ok": ok, "why": why},
           "now_et": now.strftime("%Y-%m-%d %H:%M"), "today": [r for r in recent if r["day"] == today],
           "trades": [r for r in recent if r["day"] != today or r.get("reconciled_at")],
           "summary": _summary(recent)}
    if B["real_money"]:
        try:
            out["preflight"] = live_preflight(cfg)
        except Exception as e:
            out["preflight"] = {"warnings": [f"preflight unavailable: {e}"]}
    return jsonify(out)


@orb_live_bp.route("/api/orb/live/config", methods=["POST"])
def api_config():
    body = dict(request.get_json(silent=True) or {})
    try:
        book = _req_book()
        cfg = validate_config({k: v for k, v in body.items() if k != "book"},
                              base=load_config(fresh=True, book=book), book=book)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    B = BOOKS[book]
    if cfg["mode"] == B["send"]:
        _, ok, why = account_check(book)
        if not ok:
            return jsonify({"error": why}), 400
        if B["real_money"]:
            try:
                crew = crew_live_tickers()
            except Exception as e:
                return jsonify({"error": f"could not read the Signal Router rules: {e}"}), 400
            clash = sorted(t["symbol"] for t in cfg["tickers"] if t["symbol"] in crew)
            if clash:
                return jsonify({"error": f"Crew Live's rules already trade {', '.join(clash)}. One account holds one "
                                         f"position per ticker, so remove {'it' if len(clash) == 1 else 'them'} "
                                         f"from the ORB tickers or from the crew's rules first."}), 400
    save_config(cfg, book=book)
    log.info("ORB %s: config saved — mode %s, %d tickers, risk $%.0f, cap %s", B["name"], cfg["mode"],
             len(cfg["tickers"]), cfg["risk"], cfg["cap"])
    return jsonify({"config": cfg, "book": book})
