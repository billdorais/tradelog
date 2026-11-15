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
