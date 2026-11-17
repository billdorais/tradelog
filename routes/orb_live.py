"""ORB paper engine: the opening-range breakout (strategies/orb.py) traded on ONE Alpaca
paper account (slot 8, "ORB Paper"), with every trade logged beside what the backtest
did on the same day.

  - Signals come from orb.day_signal — the same function the backtest uses — fed only
    1-minute bars that have closed, so live and backtest can't drift apart.
  - Entries are Alpaca BRACKET orders: market in, stop at the far side of the range,
    limit target at signal close ± target_r × risk. client_order_id = orb-YYYYMMDD-TICKER,
    which Alpaca keeps unique per account: one entry per ticker per day even with two
    gunicorn workers running this loop, and across restarts.
  - Every position on the account is closed at forced_exit (15:55 ET). The 15:58 EOD
    close in app.py is the backstop. Kairos' own stops / max holds skip this account
    (ACCOUNT_META "self_managed").
  - After 16:05 ET each trade is reconciled: real fill and exit, live R, and the
    backtest's trade for the same day, so slippage and drift are measured, not guessed.

Modes: off (default) · dry-run (logs the orders it would send) · paper.
Hard guards, not configurable: the account must be configured, paper and self_managed,
the ticker must be in the config, and nothing here touches any other account.
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
FLATTEN_RESEND_S = 45      # a sent close may take this long to fill on paper before it is re-sent
RECONCILE_AFTER = (16, 5)
LIVE_PARAMS = ("trend_filter", "target_r", "last_entry", "forced_exit")

DEFAULT_CONFIG = {
    "mode": "off",
    "tickers": [{"symbol": "QQQ", "group": "core"}, {"symbol": "GOOG", "group": "core"},
                {"symbol": "MU", "group": "watch"}, {"symbol": "NVDA", "group": "watch"},
                {"symbol": "SPCX", "group": "watch"}],
    "risk": 250.0,
    "cap": 25000.0,
    "params": {"trend_filter": False, "target_r": 3.0, "last_entry": "10:30", "forced_exit": "15:55"},
}

COLUMNS = ("grp", "mode", "status", "note", "side", "or_high", "or_low", "signal_time", "signal_close",
           "stop", "target", "qty", "client_order_id", "order_id", "fill_price", "fill_time",
           "exit_price", "exit_time", "exit_reason", "live_r", "live_pnl", "bt_side", "bt_entry",
           "bt_exit", "bt_reason", "bt_r", "slip_vs_bt", "slip_vs_signal", "flatten_at",
           "reconciled_at", "created_at", "updated_at")
ENTERED = ("entered", "filled")

_state = {"day": None, "decided": set(), "last_scan": 0.0, "warm": {}, "reconciled": None,
          "flattened": None, "flatten_claim": None, "flatten_sent": {}, "cfg": None, "cfg_ts": 0.0}
_state_lock = threading.Lock()
_started = False


def _app():
    import app as _a
    return _a


# ── config ───────────────────────────────────────────────────────────────────

def validate_config(body: dict, base: dict | None = None) -> dict:
    """A full, checked config from `body` laid over `base` (the current one). Raises ValueError."""
    cur = json.loads(json.dumps(base or DEFAULT_CONFIG))
    out = {**cur, **{k: v for k, v in (body or {}).items() if k in DEFAULT_CONFIG}}
    if out["mode"] not in MODES:
        raise ValueError("mode must be off, dry-run or paper")
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
    if not 1 <= out["risk"] <= 100_000:
        raise ValueError("risk per trade must be $1 to $100,000")
    if out["cap"] is not None and not 100 <= out["cap"] <= 10_000_000:
        raise ValueError("max position must be $100 to $10,000,000")
    p = {k: v for k, v in {**DEFAULT_CONFIG["params"], **(out.get("params") or {})}.items() if k in LIVE_PARAMS}
    norm = orb.normalize_params(p)                       # raises ValueError on a bad value
    out["params"] = {k: norm[k] for k in LIVE_PARAMS}
    return out


def load_config(fresh: bool = False) -> dict:
    with _state_lock:
        if not fresh and _state["cfg"] is not None and time.time() - _state["cfg_ts"] < 10:
            return _state["cfg"]
    raw = _app()._load_setting(CONFIG_KEY)
    try:
        cfg = validate_config(json.loads(raw)) if raw else validate_config({})
    except (ValueError, TypeError):
        log.warning("ORB live: stored config is invalid; using defaults with mode off")
        cfg = validate_config({})
    with _state_lock:
        _state["cfg"], _state["cfg_ts"] = cfg, time.time()
    return cfg


def save_config(cfg: dict) -> None:
    _app()._save_setting(CONFIG_KEY, json.dumps(cfg))
    with _state_lock:
        _state["cfg"], _state["cfg_ts"] = cfg, time.time()


def engine_params(cfg: dict) -> dict:
    """Backtest params for the live config: the shared defaults plus the live overrides, 1 share."""
    return orb.normalize_params({**cfg["params"], "contracts": 1, "slippage_ticks": 1.0})


# ── the account (hard guard) ─────────────────────────────────────────────────

def account_check():
    """(record, ok, why). The engine only ever trades this one paper, self-managed account."""
    rec = _app().ACCOUNTS_BY_NUM.get(ACCOUNT)
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
        for n, other in (_app().ACCOUNTS_BY_NUM or {}).items():
            if n != ACCOUNT and getattr((other or {}).get("broker"), "_key", None) == key:
                return rec, False, (f"account 8 uses the same Alpaca key as account {n}; delete "
                                    f"ALPACA_KEY{n}/SECRET{n}/PAPER{n} so only the ORB engine trades it")
    return rec, True, ""


# ── storage ──────────────────────────────────────────────────────────────────

def ensure_table():
    a = _app()
    conn = a.get_db()
    try:
        cols = ", ".join(f"{c} {'REAL' if c in _REAL else 'INTEGER' if c == 'qty' else 'TEXT'}" for c in COLUMNS)
        conn.execute(f"CREATE TABLE IF NOT EXISTS orb_live_trades (day TEXT NOT NULL, ticker TEXT NOT NULL, "
                     f"{cols}, PRIMARY KEY (day, ticker))")
        conn.commit()
    finally:
        conn.close()


_REAL = {"or_high", "or_low", "signal_close", "stop", "target", "fill_price", "exit_price", "live_r",
         "live_pnl", "bt_entry", "bt_exit", "bt_r", "slip_vs_bt", "slip_vs_signal"}


def upsert(day: str, ticker: str, **fields):
    """Insert or update one (day, ticker) row, touching only the given columns."""
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
        conn.execute(f"INSERT INTO orb_live_trades ({', '.join(cols)}) VALUES ({', '.join([ph] * len(cols))}) "
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


def rows(day_from: str | None = None, day: str | None = None, limit: int = 500) -> list:
    a = _app()
    ph = a.placeholder()
    where, args = [], []
    if day:
        where.append(f"day={ph}")
        args.append(day)
    elif day_from:
        where.append(f"day>={ph}")
        args.append(day_from)
    sql = (f"SELECT day, ticker, {', '.join(COLUMNS)} FROM orb_live_trades "
           f"{'WHERE ' + ' AND '.join(where) if where else ''} ORDER BY day DESC, ticker LIMIT {int(limit)}")
    conn = a.get_db()
    try:
        out = conn.execute(sql, args).fetchall()
    finally:
        conn.close()
    return [dict(zip(("day", "ticker", *COLUMNS), tuple(r))) for r in out]


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


def bars_for(symbols, day: date, p: dict, now: datetime | None = None) -> dict:
    """Today's closed 1-minute bars from the range start (plus trend warm-up when the filter
    is on, fetched once a day), as {SYM: frame}. A bar still forming is dropped."""
    start = datetime.combine(day, dtime(0, 0), ET) + timedelta(minutes=orb._hm(p["or_start"]))
    today = fetch_bars(symbols, start, now)
    out = {}
    with _state_lock:
        warm = dict(_state["warm"]) if _state["day"] == day else {}
    need = [s for s in symbols if p["trend_filter"] and s not in warm]
    if need:
        w0 = datetime.combine(day - timedelta(days=orb.WARMUP_DAYS + 7), dtime(4, 0), ET)
        fetched = fetch_bars(need, w0, start, adjustment="all")
        warm.update(fetched)
        with _state_lock:
            if _state["day"] == day:
                _state["warm"].update(fetched)
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


def _new_day(day: date):
    with _state_lock:
        if _state["day"] != day:
            _state.update(day=day, decided=set(), last_scan=0.0, warm={})


def _decided_today(day: date) -> set:
    """Tickers already decided today, from this worker's memory and from the table (another worker
    or an earlier process)."""
    with _state_lock:
        mine = set(_state["decided"])
    try:
        done = {r["ticker"] for r in rows(day=day.isoformat()) if r["status"] not in (None, "waiting")}
    except Exception:
        done = set()
    return mine | done


def scan(now: datetime, cfg: dict):
    """Fetch closed bars and decide every ticker still open today."""
    day = now.date()
    p = engine_params(cfg)
    groups = {t["symbol"]: t["group"] for t in cfg["tickers"]}
    todo = [s for s in groups if s not in _decided_today(day)]
    if not todo:
        return
    data = bars_for(todo, day, p, now=now)
    now_ns = pd.Timestamp(now).value
    for sym in todo:
        df = data.get(sym)
        try:
            if df is None or not len(df):
                continue
            last_end = int(df.index[-1].value) + 60_000_000_000
            if now_ns - last_end > STALE_DATA_S * 1e9:
                upsert(day.isoformat(), sym, grp=groups[sym], mode=cfg["mode"], status="waiting",
                       note=f"stale data: newest bar ended {pd.Timestamp(last_end, tz='UTC').tz_convert(ET):%H:%M}")
                continue
            m = orb.Market.from_frame(df)
            sig = orb.day_signal(m, day, p, asof_ns=last_end)
        except Exception as e:
            log.warning("ORB live: %s signal failed: %s", sym, e)
            continue
        if sig["status"] == "waiting":
            continue
        with _state_lock:
            _state["decided"].add(sym)
        base = {"grp": groups[sym], "mode": cfg["mode"], "or_high": sig.get("hi"), "or_low": sig.get("lo")}
        if sig["status"] == "skip":
            upsert(day.isoformat(), sym, status="no-trade", note=sig["reason"].replace("_", " "), **base)
            continue
        enter(now, day, sym, sig, p, cfg, base)


def enter(now: datetime, day: date, sym: str, sig: dict, p: dict, cfg: dict, base: dict):
    plan = order_plan(sig, p, cfg["risk"], cfg["cap"])
    sig_t = pd.Timestamp(sig["candle_start_ns"], tz="UTC").tz_convert(ET).strftime("%H:%M")
    coid = client_order_id(day, sym)
    row = {**base, "side": "long" if plan["side"] > 0 else "short", "signal_time": sig_t,
           "signal_close": plan["close"], "stop": plan["stop"], "target": plan["target"], "qty": plan["qty"],
           "client_order_id": coid}
    late = (pd.Timestamp(now).value - sig["decide_ns"]) / 1e9
    if late > LATE_SIGNAL_S:
        upsert(day.isoformat(), sym, status="missed", note=f"signal from {sig_t} found {late / 60:.0f} min late", **row)
        return
    if plan["qty"] < 1:
        upsert(day.isoformat(), sym, status="skipped",
               note=f"one share risks ${plan['risk_ps']:.2f}, more than ${cfg['risk']:.0f}", **row)
        return
    if cfg["mode"] == "dry-run":
        upsert(day.isoformat(), sym, status="dry-run",
               note=f"would {'buy' if plan['side'] > 0 else 'short'} {plan['qty']} (not sent)", **row)
        return
    if cfg["mode"] != "paper":
        return
    rec, ok, why = account_check()
    if not ok or sym not in {t["symbol"] for t in cfg["tickers"]}:
        upsert(day.isoformat(), sym, status="blocked", note=why or "ticker not in the config", **row)
        return
    try:
        oid, note = submit_bracket(rec["broker"], sym, plan, coid)
        upsert(day.isoformat(), sym, status="entered", order_id=oid, note=note or None, **row)
        log.info("ORB live: %s %s %d (stop %.2f, target %.2f) order %s", row["side"], sym, plan["qty"],
                 plan["stop"], plan["target"], oid)
    except Exception as e:
        upsert(day.isoformat(), sym, status="rejected", note=str(e)[:240], **row)
        log.warning("ORB live: %s bracket rejected: %s", sym, e)


def flatten(now: datetime, cfg: dict):
    """Close every position on account 8 (and only there) at the forced exit."""
    day = now.date()
    with _state_lock:
        if _state["flattened"] == day:
            return
        mine = _state.get("flatten_claim") == day
    rec, ok, why = account_check()
    if not ok:
        return
    if not mine:
        if not claim(f"ORB_FLATTEN_{day.isoformat()}"):
            with _state_lock:
                _state["flattened"] = day             # the other worker owns today's flatten
            return
        with _state_lock:
            _state["flatten_claim"] = day
    br = rec["broker"]
    try:
        br._invalidate_pos_cache()
        positions = br.get_positions(raise_on_error=True)
    except Exception as e:
        log.warning("ORB live: flatten could not read positions: %s", e)
        return
    open_syms = [str(p.get("symbol") or "").upper() for p in positions
                 if abs(float(p.get("qty") or 0)) > 0 and p.get("symbol")]
    # Done only when the broker reports the account flat — not when the closes were
    # merely sent. On 2026-11-16 MU's close did not take and the 15:58 EOD backstop
    # closed it; the engine had already marked the day flat.
    if not open_syms:
        with _state_lock:
            _state["flattened"] = day
        log.info("ORB live: account 8 flat at %s", now.strftime("%H:%M:%S"))
        return
    with _state_lock:
        sent = _state.setdefault("flatten_sent", {})
        if sent.get("_day") != day:
            sent.clear()
            sent["_day"] = day
    stamp = now.astimezone(timezone.utc).isoformat()
    for sym in open_syms:
        last = sent.get(sym)
        if last is not None and (now - last).total_seconds() < FLATTEN_RESEND_S:
            continue                                 # a close is in flight (paper fills can lag)
        try:
            res = br.close_position(sym) or {}
        except Exception as e:
            res = {"success": False, "error": str(e)}
        if res.get("success"):
            if last is None:
                upsert(day.isoformat(), sym, flatten_at=stamp)
            sent[sym] = now
            log.info("ORB live: flatten %s at %s: %s", sym, now.strftime("%H:%M:%S"), res)
        else:
            err = str(res.get("error") or res)[:200]
            upsert(day.isoformat(), sym, note=f"3:55 close failed at {now:%H:%M:%S} ET, retrying: {err}")
            log.warning("ORB live: flatten %s failed at %s (will retry): %s", sym, now.strftime("%H:%M:%S"), err)


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
    if exit_px is None:                                  # closed by the 15:55 flatten (or the EOD backstop)
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


def backtest_day(day: date, symbols, p: dict) -> dict:
    """What the backtest did on `day` for each symbol, on the same bars: {SYM: trade dict or None}."""
    data = bars_for(symbols, day, p, now=None)
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


def reconcile(day: date, cfg: dict) -> int:
    """Fill in live results and the backtest's trade for every ticker today. Returns rows written."""
    p = engine_params(cfg)
    have = {r["ticker"]: r for r in rows(day=day.isoformat())}
    syms = sorted({t["symbol"] for t in cfg["tickers"]} | set(have))
    groups = {t["symbol"]: t["group"] for t in cfg["tickers"]}
    bt = backtest_day(day, syms, p)
    rec, ok, _ = account_check()
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
        upsert(day.isoformat(), s, **f)
        n += 1
    return n


# ── loop ─────────────────────────────────────────────────────────────────────

def tick(now: datetime):
    """One pass of the engine at ET time `now`."""
    cfg = load_config()
    if cfg["mode"] == "off" or now.weekday() >= 5:
        return
    day = now.date()
    _new_day(day)
    p = engine_params(cfg)
    hm = now.hour * 60 + now.minute
    or1, last, fexit = orb._hm(p["or_end"]), orb._hm(p["last_entry"]), orb._hm(p["forced_exit"])
    tf = p["breakout_tf"]
    if or1 + tf <= hm <= last + 2:
        secs = (hm - or1) % tf * 60 + now.second       # seconds since the last candle boundary
        with _state_lock:
            due = secs <= SCAN_WINDOW_S and time.time() - _state["last_scan"] >= SCAN_EVERY_S
            if due:
                _state["last_scan"] = time.time()
        if due:
            scan(now, cfg)
    if cfg["mode"] == "paper" and fexit <= hm < 16 * 60:
        flatten(now, cfg)
    if (now.hour, now.minute) >= RECONCILE_AFTER:
        with _state_lock:
            done = _state["reconciled"] == day
        if not done:
            try:
                reconcile(day, cfg)
                with _state_lock:
                    _state["reconciled"] = day
            except Exception as e:
                log.warning("ORB live: reconcile failed (will retry): %s", e)


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
    """Start the engine thread (not under pytest, not with ORB_LIVE=0). Idles while mode is off."""
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


@orb_live_bp.route("/api/orb/live/status")
def api_status():
    try:
        ensure_table()
        cfg = load_config(fresh=True)
        recent = rows(day_from=(date.today() - timedelta(days=int(request.args.get("days", 120) or 120))).isoformat())
    except Exception as e:
        return jsonify({"error": f"status unavailable: {e}"}), 500
    rec, ok, why = account_check()
    now = datetime.now(ET)
    today = now.date().isoformat()
    return jsonify({"config": cfg, "account": {"num": ACCOUNT, "configured": rec is not None,
                                               "label": (rec or {}).get("label"), "ok": ok, "why": why},
                    "now_et": now.strftime("%Y-%m-%d %H:%M"), "today": [r for r in recent if r["day"] == today],
                    "trades": [r for r in recent if r["day"] != today or r.get("reconciled_at")],
                    "summary": _summary(recent)})


@orb_live_bp.route("/api/orb/live/config", methods=["POST"])
def api_config():
    try:
        cfg = validate_config(request.get_json(silent=True) or {}, base=load_config(fresh=True))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    if cfg["mode"] == "paper":
        _, ok, why = account_check()
        if not ok:
            return jsonify({"error": why}), 400
    save_config(cfg)
    log.info("ORB live: config saved — mode %s, %d tickers, risk $%.0f", cfg["mode"], len(cfg["tickers"]), cfg["risk"])
    return jsonify({"config": cfg})
