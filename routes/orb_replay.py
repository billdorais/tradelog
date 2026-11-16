"""ORB replay: the Nasdaq opening-range breakout backtest (strategies/orb.py) on the web.

Replay only. Nothing here routes or places an order, and the strategy is not wired to
any account.

Data sources, by how faithful they are to the rules:
  csv     — your own NQ/MNQ futures bars (1-minute best), uploaded once and kept in
            the database. The only way to test years of real futures history.
  yahoo   — continuous front-month NQ=F / MNQ=F, 5-minute bars, nearly 24 h a day,
            but Yahoo keeps only the last ~60 days.
  alpaca  — QQQ (or another ETF) with pre-market included, years of history. A
            STAND-IN: R-multiples are scale-free so they carry over, dollars do not,
            and QQQ's thin pre-market makes a different 09:00–09:30 range than NQ's.

A run happens in a background thread; the result is stored in orb_runs and the page
polls for it. Runs are light (seconds of compute after the data arrives) so, unlike
the tick replays, they are allowed while the market is open.
"""
from __future__ import annotations

import base64
import gzip
import io
import json
import logging
import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from flask import Blueprint, jsonify, render_template, request

from strategies import orb

log = logging.getLogger(__name__)
orb_replay_bp = Blueprint("orb_replay", __name__)

MAX_UPLOAD_BYTES = 150 * 1024 * 1024
MAX_DATASETS = 20
ALPACA_MAX_DAYS = {"5m": 365 * 10, "1m": 365 * 6}   # 1-minute: ~1M bars for 5 years, fetched by month
STALE_S = 20 * 60
_lock = threading.Lock()
# The last Alpaca download, kept so a second run on the same range (another target R, the
# filter off) doesn't refetch years of minute bars. One entry: a few years of QQQ is ~50 MB.
_alpaca_cache = {"key": None, "df": None}


def _app():
    import app as _a
    return _a


def _db():
    return _app().get_db()


def _ph():
    return _app().placeholder()


def _ensure_tables():
    conn = _db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS orb_runs (
            id TEXT PRIMARY KEY, created_at TEXT, params TEXT, status TEXT,
            progress TEXT, result TEXT, updated_at TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS orb_datasets (
            id TEXT PRIMARY KEY, created_at TEXT, name TEXT, symbol TEXT, rows INTEGER,
            first_ts TEXT, last_ts TEXT, base_min INTEGER, data TEXT)""")
        conn.commit()
    finally:
        conn.close()


def _save_run(run_id, **fields):
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    ph = _ph()
    conn = _db()
    try:
        conn.execute(f"UPDATE orb_runs SET {', '.join(f'{k}={ph}' for k in fields)} WHERE id={ph}",
                     (*fields.values(), run_id))
        conn.commit()
    finally:
        conn.close()


def _reap():
    conn = _db()
    try:
        rows = conn.execute("SELECT id, updated_at FROM orb_runs WHERE status='running'").fetchall()
    finally:
        conn.close()
    for rid, upd in rows:
        try:
            age = time.time() - datetime.fromisoformat(upd).timestamp()
        except Exception:
            age = STALE_S
        if age >= STALE_S:
            _save_run(rid, status="failed", progress="stopped without finishing (the server likely restarted)")


# ── datasets (uploaded futures bars) ─────────────────────────────────────────

def _pack(df: pd.DataFrame) -> str:
    """Bars → gzip'd CSV of epoch-ns + OHLC, base64 for a TEXT column."""
    buf = io.StringIO()
    out = pd.DataFrame({"t": df.index.tz_convert("UTC").as_unit("ns").asi8,
                        "o": df["open"].to_numpy(), "h": df["high"].to_numpy(),
                        "l": df["low"].to_numpy(), "c": df["close"].to_numpy()})
    out.to_csv(buf, index=False, float_format="%.10g")
    return base64.b64encode(gzip.compress(buf.getvalue().encode(), 6)).decode()


def _unpack(blob: str) -> pd.DataFrame:
    raw = gzip.decompress(base64.b64decode(blob))
    d = pd.read_csv(io.BytesIO(raw))
    idx = pd.DatetimeIndex(pd.to_datetime(d["t"].to_numpy(), unit="ns", utc=True))
    return pd.DataFrame({"open": d["o"].to_numpy(), "high": d["h"].to_numpy(),
                         "low": d["l"].to_numpy(), "close": d["c"].to_numpy()}, index=idx)


def _load_dataset(ds_id):
    ph = _ph()
    conn = _db()
    try:
        r = conn.execute(f"SELECT data, name, symbol FROM orb_datasets WHERE id={ph}", (ds_id,)).fetchone()
    finally:
        conn.close()
    if not r:
        raise ValueError("no such dataset")
    return _unpack(r[0]), r[1], r[2]


# ── a run ────────────────────────────────────────────────────────────────────

def _validate_run(body: dict) -> dict:
    src = (body.get("source") or "").strip().lower()
    if src not in ("csv", "yahoo", "alpaca"):
        raise ValueError("source must be csv, yahoo or alpaca")
    try:
        d0 = date.fromisoformat(body.get("from") or "")
        d1 = date.fromisoformat(body.get("to") or "")
    except ValueError:
        raise ValueError("from and to must be YYYY-MM-DD")
    if d1 < d0:
        raise ValueError("to must be on or after from")
    params = orb.normalize_params(body.get("params") or {})
    out = {"source": src, "from": d0.isoformat(), "to": d1.isoformat(), "params": params,
           "validate": bool(body.get("validate", True))}
    if src == "alpaca":
        sym = (body.get("symbol") or "QQQ").strip().upper()
        if not sym.isalnum() or len(sym) > 6:
            raise ValueError("symbol must be a stock/ETF ticker such as QQQ")
        res = body.get("resolution") or "5m"
        if res not in ALPACA_MAX_DAYS:
            raise ValueError("resolution must be 5m or 1m")
        if (d1 - d0).days > ALPACA_MAX_DAYS[res]:
            raise ValueError(f"{res} Alpaca runs are capped at {ALPACA_MAX_DAYS[res]} days")
        out.update(symbol=sym, resolution=res, contract="QQQ")
    else:
        con = (body.get("contract") or "NQ").strip().upper()
        if con not in ("NQ", "MNQ"):
            raise ValueError("contract must be NQ or MNQ")
        out["contract"] = con
        if src == "csv":
            ds = (body.get("dataset_id") or "").strip()
            if not ds:
                raise ValueError("pick an uploaded dataset")
            out["dataset_id"] = ds
        else:
            out["symbol"] = orb.YAHOO_SYMBOLS[con]
    return out


def _execute(run_id: str, cfg: dict):
    try:
        d0, d1 = date.fromisoformat(cfg["from"]), date.fromisoformat(cfg["to"])
        fetch0 = d0 - timedelta(days=orb.WARMUP_DAYS)
        notes = []
        _save_run(run_id, progress="loading bars")
        if cfg["source"] == "csv":
            df, name, sym = _load_dataset(cfg["dataset_id"])
            data_label = f"uploaded dataset “{name}” ({sym or cfg['contract']})"
        elif cfg["source"] == "yahoo":
            df = orb.load_yahoo(cfg["symbol"], fetch0, d1)
            data_label = f"Yahoo {cfg['symbol']} 5-minute bars (continuous front month)"
            notes.append("Yahoo keeps only about 60 days of 5-minute bars, so this is a small sample.")
        else:
            key = (cfg["symbol"], cfg["resolution"], fetch0.isoformat(), d1.isoformat())
            if _alpaca_cache["key"] == key:
                df = _alpaca_cache["df"]
                notes.append("Bars reused from the previous run on the same range.")
            else:
                _alpaca_cache.update(key=None, df=None)          # free the old download first
                df = orb.load_alpaca(cfg["symbol"], fetch0, d1, cfg["resolution"],
                                     progress=lambda msg: _save_run(run_id, progress=msg))
                _alpaca_cache.update(key=key, df=df)
            feed = orb._alpaca_feed().value.upper()
            data_label = f"Alpaca {cfg['symbol']} {cfg['resolution']} bars ({feed} feed), pre-market included"
            notes.append(f"{cfg['symbol']} stands in for NQ. R-multiples carry over; dollars are per share "
                         f"and the 09:00–09:30 range is built from thin pre-market trading.")
        m = orb.Market.from_frame(df)
        _save_run(run_id, progress=f"simulating {len(m.days)} days of {m.base_min}-minute bars")
        ready = orb.ready_date(m, cfg["params"])
        start = max(d0, ready) if ready else d0
        if ready and ready > d0:
            notes.append(f"Trading starts {ready.isoformat()}: before that there weren't "
                         f"{cfg['params']['slow_sma']} completed {cfg['params']['trend_tf']}-minute bars "
                         f"for the slow SMA.")
        if m.base_min == 5:
            notes.append("5-minute bars: a stop and target touched inside the same 5 minutes are scored as "
                         "the stop (conservative). 1-minute bars settle more of those.")
        rep = orb.full_report(m, cfg["params"], cfg["contract"], start, d1, validate=cfg["validate"])
        days = [d for d in m.days if start <= d <= d1 and d.weekday() < 5]
        rep["data"] = {"label": data_label, "bars": len(m.base), "base_min": m.base_min,
                       "first_bar": m.base.index[0].isoformat(), "last_bar": m.base.index[-1].isoformat(),
                       "trading_from": start.isoformat(), "trading_to": d1.isoformat(),
                       "weekdays_in_window": len(days), "notes": notes}
        _save_run(run_id, status="done", progress="done", result=json.dumps(rep, default=str))
    except Exception as e:
        log.warning("ORB run %s failed: %s", run_id, e, exc_info=True)
        _save_run(run_id, status="failed", progress=str(e)[:300])
    finally:
        if _lock.locked():
            _lock.release()


# ── endpoints ────────────────────────────────────────────────────────────────

@orb_replay_bp.route("/orb")
def orb_page():
    return render_template("orb.html")


@orb_replay_bp.route("/api/orb/meta")
def api_meta():
    return jsonify({"defaults": orb.DEFAULTS, "contracts": orb.CONTRACTS, "target_grid": orb.TARGET_GRID,
                    "yahoo_max_days": orb.YAHOO_MAX_DAYS, "alpaca_max_days": ALPACA_MAX_DAYS})


@orb_replay_bp.route("/api/orb/datasets", methods=["GET"])
def api_datasets():
    _ensure_tables()
    conn = _db()
    try:
        rows = conn.execute("SELECT id, created_at, name, symbol, rows, first_ts, last_ts, base_min "
                            "FROM orb_datasets ORDER BY created_at DESC").fetchall()
    finally:
        conn.close()
    return jsonify({"datasets": [dict(zip(("id", "created_at", "name", "symbol", "rows", "first", "last", "base_min"), r))
                                 for r in rows]})


@orb_replay_bp.route("/api/orb/datasets", methods=["POST"])
def api_upload():
    f = request.files.get("file")
    if f is None:
        return jsonify({"error": "attach a CSV file"}), 400
    raw = f.read(MAX_UPLOAD_BYTES + 1)
    if len(raw) > MAX_UPLOAD_BYTES:
        return jsonify({"error": "file is over 150 MB; gzip it (.csv.gz) or upload a shorter span"}), 413
    tz = (request.form.get("tz") or orb.ET).strip()
    if tz not in (orb.ET, "America/Chicago", "UTC"):
        return jsonify({"error": "tz must be America/New_York, America/Chicago or UTC"}), 400
    sym = (request.form.get("symbol") or "NQ").strip().upper()[:12]
    name = ((request.form.get("name") or f.filename or "dataset").strip())[:80]
    try:
        df = orb.parse_csv(raw, tz=tz, filename=f.filename or "")
        m = orb.Market.from_frame(df)
    except Exception as e:
        return jsonify({"error": f"could not read the file: {e}"}), 400
    _ensure_tables()
    conn = _db()
    try:
        n = conn.execute("SELECT COUNT(*) FROM orb_datasets").fetchone()[0]
        if n >= MAX_DATASETS:
            return jsonify({"error": f"already {MAX_DATASETS} datasets; delete one first"}), 409
        ph = _ph()
        ds_id = uuid.uuid4().hex[:12]
        conn.execute(f"INSERT INTO orb_datasets (id, created_at, name, symbol, rows, first_ts, last_ts, base_min, data) "
                     f"VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})",
                     (ds_id, datetime.now(timezone.utc).isoformat(), name, sym, len(m.base),
                      m.base.index[0].isoformat(), m.base.index[-1].isoformat(), m.base_min, _pack(m.base)))
        conn.commit()
    finally:
        conn.close()
    return jsonify({"id": ds_id, "name": name, "rows": len(m.base), "base_min": m.base_min,
                    "first": m.base.index[0].isoformat(), "last": m.base.index[-1].isoformat(),
                    "days": len(m.days)})


@orb_replay_bp.route("/api/orb/datasets/<ds_id>", methods=["DELETE"])
def api_delete_dataset(ds_id):
    _ensure_tables()
    ph = _ph()
    conn = _db()
    try:
        conn.execute(f"DELETE FROM orb_datasets WHERE id={ph}", (ds_id,))
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True})


@orb_replay_bp.route("/api/orb/run", methods=["POST"])
def api_run():
    try:
        cfg = _validate_run(request.get_json(silent=True) or {})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    _ensure_tables()
    _reap()
    if not _lock.acquire(blocking=False):
        return jsonify({"error": "an ORB run is already going; wait for it to finish"}), 409
    run_id = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).isoformat()
    ph = _ph()
    conn = _db()
    try:
        conn.execute(f"INSERT INTO orb_runs (id, created_at, params, status, progress, result, updated_at) "
                     f"VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph})",
                     (run_id, now, json.dumps(cfg), "running", "starting", None, now))
        conn.commit()
    except Exception:
        _lock.release()
        raise
    finally:
        conn.close()
    threading.Thread(target=_execute, args=(run_id, cfg), daemon=True, name=f"orb-{run_id}").start()
    return jsonify({"run_id": run_id, "status": "running"})


@orb_replay_bp.route("/api/orb/runs")
def api_runs():
    _ensure_tables()
    _reap()
    conn = _db()
    try:
        rows = conn.execute("SELECT id, created_at, params, status, progress FROM orb_runs "
                            "ORDER BY created_at DESC LIMIT 25").fetchall()
    finally:
        conn.close()
    return jsonify({"runs": [{"id": r[0], "created_at": r[1], "params": json.loads(r[2] or "{}"),
                              "status": r[3], "progress": r[4]} for r in rows]})


@orb_replay_bp.route("/api/orb/runs/<run_id>")
def api_run_detail(run_id):
    _ensure_tables()
    ph = _ph()
    conn = _db()
    try:
        r = conn.execute(f"SELECT id, created_at, params, status, progress, result FROM orb_runs WHERE id={ph}",
                         (run_id,)).fetchone()
    finally:
        conn.close()
    if not r:
        return jsonify({"error": "no such run"}), 404
    return jsonify({"id": r[0], "created_at": r[1], "params": json.loads(r[2] or "{}"), "status": r[3],
                    "progress": r[4], "result": json.loads(r[5]) if r[5] else None})


# ── compare tickers: finished runs side by side over a date range ────────────
#
# Reads the trades already stored with each run; nothing is re-simulated. For each
# ticker (and filter setting, and any non-default settings) the run used is the one
# whose data reaches furthest, so a short re-run doesn't hide a 5-year one.

MAX_PORTFOLIO = 7
_TRADES_CACHE_MAX = 40
_trades_cache: dict = {}          # run_id -> trades + coverage; a finished run never changes
_VARIANT_SKIP = {"trend_filter", "contracts", "commission"}
_VARIANT_NAMES = {"target_r": "{}R target", "or_start": "range from {}", "or_end": "range to {}",
                  "last_entry": "last entry {}", "forced_exit": "exit {}", "stop": "{} stop",
                  "same_bar": "{}", "slippage_ticks": "{} tick slip"}


def _run_label(cfg: dict) -> str:
    if cfg.get("source") == "alpaca":
        return (cfg.get("symbol") or "QQQ").upper()
    con = cfg.get("contract") or "NQ"
    return f"{con} ({'CSV' if cfg.get('source') == 'csv' else 'Yahoo'})"


def _variant(params: dict) -> str:
    """The settings that differ from the defaults, other than the filter and size; '' for a default run."""
    base = orb.normalize_params({})
    try:
        p = orb.normalize_params(params)
    except ValueError:
        p = params
    out = []
    for k in sorted(p):
        if k in _VARIANT_SKIP or k not in base or p[k] == base[k]:
            continue
        out.append(_VARIANT_NAMES.get(k, k.replace("_", " ") + " {}").format(p[k]))
    return ", ".join(out)


def _neg(iso):
    """Sort key that ranks an EARLIER start date higher (the longer run wins a tie on end date)."""
    try:
        return -date.fromisoformat(iso).toordinal()
    except (TypeError, ValueError):
        return 0


def portfolio_options(rows) -> list:
    """rows: (id, created_at, params JSON) of finished runs → one option per ticker / filter / variant."""
    best = {}
    for rid, created, raw in rows:
        try:
            cfg = json.loads(raw or "{}")
        except ValueError:
            continue
        p = cfg.get("params") or {}
        opt = {"run_id": rid, "label": _run_label(cfg), "variant": _variant(p),
               "trend_filter": bool(p.get("trend_filter")), "from": cfg.get("from"), "to": cfg.get("to"),
               "created_at": created, "shares": p.get("contracts"), "source": cfg.get("source")}
        key = (opt["label"], opt["trend_filter"], opt["variant"])
        cur = best.get(key)
        rank = lambda o: (o["to"] or "", _neg(o["from"]), o["created_at"] or "")
        if cur is None or rank(opt) > rank(cur):
            best[key] = opt
    return sorted(best.values(), key=lambda o: (o["label"], o["trend_filter"], o["variant"]))


def _run_trades(run_id: str):
    hit = _trades_cache.get(run_id)
    if hit is not None:
        return hit
    ph = _ph()
    conn = _db()
    try:
        r = conn.execute(f"SELECT params, status, result FROM orb_runs WHERE id={ph}", (run_id,)).fetchone()
    finally:
        conn.close()
    if not r or r[1] != "done" or not r[2]:
        return None
    cfg, res = json.loads(r[0] or "{}"), json.loads(r[2])
    p, data = cfg.get("params") or {}, res.get("data") or {}
    size = max(int(p.get("contracts") or 1), 1)
    entry = {"label": _run_label(cfg), "variant": _variant(p), "trend_filter": bool(p.get("trend_filter")),
             "shares": p.get("contracts"),
             "trading_from": data.get("trading_from") or cfg.get("from"),
             "trading_to": data.get("trading_to") or cfg.get("to"),
             "trades": [{"date": t["date"], "time": t.get("entry_time") or "", "r": float(t["r"]),
                         "r_gross": float(t.get("r_gross", t["r"])), "pnl": float(t.get("pnl") or 0.0),
                         "pps": float(t.get("pnl") or 0.0) / size,          # net $ per share (or contract)
                         "risk_pts": float(t.get("risk_pts") or 0.0), "entry": float(t.get("entry") or 0.0),
                         "side": t.get("side")} for t in (res.get("trades") or [])]}
    if len(_trades_cache) >= _TRADES_CACHE_MAX:
        _trades_cache.pop(next(iter(_trades_cache)))
    _trades_cache[run_id] = entry
    return entry


def size_trade(t: dict, risk: float, cap: float | None = None) -> int:
    """Whole shares for a trade risking at most `risk` dollars (stop distance × shares), and costing at
    most `cap` dollars to open when a cap is given. 0 = one share already risks more than `risk`."""
    return orb.shares_for_risk(t.get("risk_pts") or 0.0, t.get("entry") or 0.0, risk, cap)


def _sizing(rows: list, risk: float, cap: float | None) -> dict:
    """rows: (trade, shares) → what sizing by `risk` did: net $, trades taken/skipped, $ risked, position sizes."""
    taken = [(t, n) for t, n in rows if n > 0]
    pos = sorted(n * t["entry"] for t, n in taken)
    full = sum(1 for t, n in taken if t["risk_pts"] * (n + 1) <= risk + 1e-9)     # held back by the cap
    return {"risk": risk, "cap": cap, "net": round(sum(t["pps"] * n for t, n in taken), 2),
            "taken": len(taken), "skipped": len(rows) - len(taken), "capped": full,
            "avg_risk": round(sum(t["risk_pts"] * n for t, n in taken) / len(taken), 2) if taken else None,
            "median_position": round(pos[len(pos) // 2], 0) if pos else None,
            "max_position": round(pos[-1], 0) if pos else None}


def portfolio(series: list, d0: str, d1: str, risk: float | None = None, cap: float | None = None) -> dict:
    """series: [{"run_id", "label", "trades", ...}] → per-series and combined results over d0..d1 (ISO
    dates), with cumulative R and $ on every date any of them traded. Combined = each trade at 1R of its
    own risk, so a $900 stock and a $40 one count the same in R (not in $).

    risk: also size every trade to risk about that many dollars, in whole shares (cum_risk, sizing);
    cap: and spend at most that many dollars on a position."""
    out, all_trades, dates = [], [], set()
    all_sized = []
    # Coverage is judged on weekdays: a range ending on a Sunday is covered by data through Friday.
    w0, w1 = date.fromisoformat(d0), date.fromisoformat(d1)
    while w0.weekday() >= 5 and w0 < w1:
        w0 += timedelta(days=1)
    while w1.weekday() >= 5 and w1 > w0:
        w1 -= timedelta(days=1)
    w0, w1 = w0.isoformat(), w1.isoformat()
    for s in series:
        tr = sorted((t for t in s["trades"] if d0 <= t["date"] <= d1), key=lambda t: (t["date"], t["time"]))
        sized = [(t, size_trade(t, risk, cap)) for t in tr] if risk else [(t, 0) for t in tr]
        daily = {}
        for t, n in sized:
            a = daily.setdefault(t["date"], [0.0, 0.0, 0.0])      # R, $ at the run's size, $ at `risk`
            a[0] += t["r"]
            a[1] += t["pnl"]
            a[2] += t.get("pps", 0.0) * n
        dates.update(daily)
        all_trades += tr
        all_sized += sized
        out.append({"run_id": s["run_id"], "label": s["label"], "variant": s.get("variant", ""),
                    "trend_filter": s.get("trend_filter"), "shares": s.get("shares"),
                    "trading_from": s.get("trading_from"), "trading_to": s.get("trading_to"),
                    "covers": bool(s.get("trading_from") and s.get("trading_to")
                                   and s["trading_from"] <= w0 and s["trading_to"] >= w1),
                    "_daily": daily, "summary": orb.summarize(tr),
                    **({"sizing": _sizing(sized, risk, cap)} if risk else {})})
    dates = sorted(dates)
    keys = ("cum_r", "cum_usd", "cum_risk") if risk else ("cum_r", "cum_usd")
    nd = (3, 2, 2)

    def _curves(daily):
        run, curves = [0.0, 0.0, 0.0], {k: [] for k in keys}
        for d in dates:
            a = daily.get(d)
            if a:
                for i in range(3):
                    run[i] += a[i]
            for i, k in enumerate(keys):
                curves[k].append(round(run[i], nd[i]))
        return curves

    comb = {}
    for s in out:
        daily = s.pop("_daily")
        for d, a in daily.items():
            c = comb.setdefault(d, [0.0, 0.0, 0.0])
            for i in range(3):
                c[i] += a[i]
        s.update(_curves(daily))
        s["days"] = len(daily)
        s["green_days"] = sum(1 for a in daily.values() if a[0] > 0)
    all_trades.sort(key=lambda t: (t["date"], t["time"]))
    day_r = [a[0] for a in comb.values()]
    combined = {"summary": orb.summarize(all_trades), **_curves(comb), "days": len(comb),
                "green_days": sum(1 for v in day_r if v > 0),
                "best_day_r": round(max(day_r), 3) if day_r else None,
                "worst_day_r": round(min(day_r), 3) if day_r else None}
    if risk:
        combined["sizing"] = _sizing(all_sized, risk, cap)
        day_x = [a[2] for a in comb.values()]
        combined["sizing"].update(best_day=round(max(day_x), 2) if day_x else None,
                                  worst_day=round(min(day_x), 2) if day_x else None,
                                  max_dd=round(_max_dd_usd(sorted(all_sized, key=lambda x: (x[0]["date"], x[0]["time"]))), 2))
    return {"from": d0, "to": d1, "dates": dates, "series": out, "combined": combined, "risk": risk, "cap": cap}


def _max_dd_usd(sized) -> float:
    run = peak = dd = 0.0
    for t, n in sized:
        run += t.get("pps", 0.0) * n
        peak = max(peak, run)
        dd = max(dd, peak - run)
    return dd


@orb_replay_bp.route("/api/orb/portfolio/options")
def api_portfolio_options():
    _ensure_tables()
    conn = _db()
    try:
        rows = conn.execute("SELECT id, created_at, params FROM orb_runs WHERE status='done'").fetchall()
    finally:
        conn.close()
    return jsonify({"options": portfolio_options([tuple(r) for r in rows]), "max": MAX_PORTFOLIO})


@orb_replay_bp.route("/api/orb/portfolio")
def api_portfolio():
    ids = list(dict.fromkeys(x.strip() for x in (request.args.get("runs") or "").split(",") if x.strip()))
    if not ids:
        return jsonify({"error": "pick at least one ticker"}), 400
    if len(ids) > MAX_PORTFOLIO:
        return jsonify({"error": f"up to {MAX_PORTFOLIO} tickers at a time"}), 400
    try:
        d0 = date.fromisoformat(request.args.get("from") or "")
        d1 = date.fromisoformat(request.args.get("to") or "")
    except ValueError:
        return jsonify({"error": "from and to must be YYYY-MM-DD"}), 400
    if d1 < d0:
        return jsonify({"error": "to must be on or after from"}), 400
    try:
        risk = float(request.args["risk"]) if request.args.get("risk") else None
        cap = float(request.args["cap"]) if request.args.get("cap") else None
    except ValueError:
        return jsonify({"error": "risk and cap must be dollar amounts"}), 400
    if risk is not None and not 1 <= risk <= 1_000_000:
        return jsonify({"error": "risk per trade must be between $1 and $1,000,000"}), 400
    if cap is not None and not 1 <= cap <= 100_000_000:
        return jsonify({"error": "max position must be between $1 and $100,000,000"}), 400
    _ensure_tables()
    series = []
    for rid in ids:
        e = _run_trades(rid)
        if e is None:
            return jsonify({"error": f"run {rid} isn't a finished run"}), 404
        series.append({"run_id": rid, **e})
    return jsonify(portfolio(series, d0.isoformat(), d1.isoformat(), risk=risk, cap=cap if risk else None))
