"""What made it work? — the conditions at entry behind a ticker/strategy's wins.

Every closed trade is tagged with the market and stock conditions at the moment it
entered (SPY's day and last hour, SPY's trend, VIX, the stock's gap, pre-market
volume, open location, day type, RVOL, volatility vs its own norm, trend, the move
into the entry, time of day, weekday, earnings), then winners and losers are split
on each condition.

Built to resist fooling the trader, because with ~15 conditions some bucket will
always look great by chance:
  * every number carries its trade count, and thin buckets are flagged;
  * each bucket is also measured on the OLDER and NEWER half of the trades
    separately; a pattern counts only if it points the same way in both;
  * the same split is shown for the PEERS: every other ticker's trades in the same
    strategy family, so "is this MU, or true of every breakout?" has an answer.
A surviving pattern is a candidate to test as a gate (replay / gate-cost tools),
never a gate by itself.

Side-aware conditions are measured in the trade's direction ("SPY with the trade"
means up for a long, down for a short) so longs and shorts pool meaningfully.
Read-only.
"""
from __future__ import annotations

import math
import threading
import time
from array import array
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from flask import Blueprint, jsonify, request

trade_context_bp = Blueprint("trade_context", __name__)
_ET = ZoneInfo("America/New_York")

MIN_BUCKET = 8          # a bucket smaller than this is shown but never called a finding
MIN_HALF = 4            # each half needs this many trades in the bucket to judge it
MIN_PEER = 20           # peer buckets thinner than this don't count as agreement
LIFT_FLOOR = 0.03       # % per trade a bucket must beat the rest by to be a finding
# How far the bucket's average sits from the rest's, in standard errors (Welch t).
# With ~60 buckets on the page, ~3 would clear 2.0 by pure chance; 2.5 leaves well
# under one, before the both-halves check halves it again.
T_FLOOR = 2.5
PEER_TICKER_CAP = 60
_RESULT_TTL = 15 * 60


# ── pure: conditions → buckets ────────────────────────────────────────────────

def _sgn(side):
    return -1 if (side or "").upper() == "SHORT" else 1


def _b(x, cuts, labels):
    """Bucket x by ascending cut points; labels has len(cuts) + 1 entries."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    for c, lab in zip(cuts, labels):
        if x < c:
            return lab
    return labels[-1]


def bucket_tod(hhmm):
    if hhmm is None:
        return None
    return _b(hhmm, [1000, 1100, 1300, 1500],
              ["9:30–10:00", "10:00–11:00", "11:00–13:00", "13:00–15:00", "15:00–16:00"])


# (key, label, group, peers_ok, bucket order, fn(ctx) -> bucket)
FEATURES = [
    ("side", "Side", "The trade", True, ["LONG", "SHORT"],
     lambda c: (c.get("side") or "").upper() or None),
    ("spy_day", "SPY so far today, in the trade's direction", "Market", True,
     ["against > 0.5%", "against 0–0.5%", "with 0–0.5%", "with > 0.5%"],
     lambda c: _b(c.get("spy_day_dir"), [-0.5, 0, 0.5],
                  ["against > 0.5%", "against 0–0.5%", "with 0–0.5%", "with > 0.5%"])),
    ("spy_hour", "SPY over the last hour (since the open, before 10:30), in the trade's direction", "Market", True,
     ["against > 0.15%", "flat (±0.15%)", "with > 0.15%"],
     lambda c: _b(c.get("spy_hour_dir"), [-0.15, 0.15],
                  ["against > 0.15%", "flat (±0.15%)", "with > 0.15%"])),
    ("spy_gap", "SPY's opening gap", "Market", True,
     ["gap down > 0.3%", "flat (±0.3%)", "gap up > 0.3%"],
     lambda c: _b(c.get("spy_gap"), [-0.3, 0.3], ["gap down > 0.3%", "flat (±0.3%)", "gap up > 0.3%"])),
    ("spy_trend", "SPY's trend (prior close vs 20- and 50-day averages)", "Market", True,
     ["above both", "between", "below both"], lambda c: c.get("spy_trend")),
    ("vix", "VIX (prior close)", "Market", True, ["< 15", "15–20", "20–25", "≥ 25"],
     lambda c: _b(c.get("vix"), [15, 20, 25], ["< 15", "15–20", "20–25", "≥ 25"])),
    ("gap", "The stock's opening gap, in the trade's direction", "Before the open", True,
     ["against > 1%", "against 0.25–1%", "flat (±0.25%)", "with 0.25–1%", "with > 1%"],
     lambda c: _b(c.get("gap_dir"), [-1, -0.25, 0.25, 1],
                  ["against > 1%", "against 0.25–1%", "flat (±0.25%)", "with 0.25–1%", "with > 1%"])),
    ("premarket", "Pre-market volume (8:00–9:30, thirds of this stock's trades)", "Before the open", False,
     ["light", "normal", "heavy"], lambda c: c.get("pm_bucket")),
    ("open_loc", "Where it opened vs the breakout level", "Before the open", False,
     ["near CPR (room)", "mid-travel", "extended", "at/past extreme"], lambda c: c.get("open_loc")),
    ("day_type", "Day type (Camarilla)", "Before the open", False, None, lambda c: c.get("day_type")),
    ("rvol", "RVOL at entry", "At entry", True, ["< 1×", "1–1.5×", "1.5–3×", "≥ 3×"],
     lambda c: _b(c.get("rvol"), [1, 1.5, 3], ["< 1×", "1–1.5×", "1.5–3×", "≥ 3×"])),
    ("atr_rel", "Volatility vs its own norm (14-day ATR ÷ its 60-day median)", "At entry", True,
     ["quieter (< 0.85×)", "normal", "livelier (> 1.15×)"],
     lambda c: _b(c.get("atr_rel"), [0.85, 1.15], ["quieter (< 0.85×)", "normal", "livelier (> 1.15×)"])),
    ("trend", "The stock's trend vs 20- and 50-day averages, in the trade's direction", "At entry", True,
     ["with the trend", "mixed", "against the trend"], lambda c: c.get("trend_dir")),
    ("move", "Move from the open to the entry, in the trade's direction", "At entry", True,
     ["against > 0.5%", "±0.5%", "with 0.5–1.5%", "with > 1.5%"],
     lambda c: _b(c.get("move_dir"), [-0.5, 0.5, 1.5], ["against > 0.5%", "±0.5%", "with 0.5–1.5%", "with > 1.5%"])),
    ("tod", "Time of day (ET)", "Timing", True,
     ["9:30–10:00", "10:00–11:00", "11:00–13:00", "13:00–15:00", "15:00–16:00"],
     lambda c: bucket_tod(c.get("hhmm"))),
    ("weekday", "Day of the week", "Timing", True, ["Mon", "Tue", "Wed", "Thu", "Fri"],
     lambda c: c.get("weekday")),
    ("earnings", "Earnings", "Timing", False,
     ["0–5 days after earnings", "≤ 5 days before earnings", "no earnings within 5 days"],
     lambda c: c.get("earnings")),
]


def _stats(rows):
    """rows: [(pct, pnl)] -> n, win_rate, avg_pct."""
    n = len(rows)
    if not n:
        return {"n": 0, "win_rate": None, "avg_pct": None}
    pcts = [p for p, _ in rows if p is not None]
    return {"n": n, "win_rate": round(sum(1 for _, d in rows if d > 0) / n * 100, 1),
            "avg_pct": round(sum(pcts) / len(pcts), 4) if pcts else None}


def _tstat(inside, outside):
    """Welch t of the bucket's mean % vs the rest's; None when either side is too
    small or flat to estimate."""
    a = [p for p, _ in inside if p is not None]
    b = [p for p, _ in outside if p is not None]
    if len(a) < 3 or len(b) < 3:
        return None
    ma, mb = sum(a) / len(a), sum(b) / len(b)
    va = sum((x - ma) ** 2 for x in a) / (len(a) - 1)
    vb = sum((x - mb) ** 2 for x in b) / (len(b) - 1)
    se = math.sqrt(va / len(a) + vb / len(b))
    return round((ma - mb) / se, 2) if se > 0 else None


def _lift(inside, outside):
    a, b = _stats(inside), _stats(outside)
    if a["avg_pct"] is None or b["avg_pct"] is None:
        return None
    return round(a["avg_pct"] - b["avg_pct"], 4)


# Conditions that are the same for every trade on a given day. Their strength is
# judged on daily averages: 50 trades over 20 days is 20 observations of VIX, not 50,
# and every ticker trading those days shares them.
DAY_LEVEL = {"spy_gap", "spy_trend", "vix", "weekday"}


def _day_means(tagged_rows):
    """[(bucket, row)] -> [(mean pct, mean pnl)] one per (bucket, date)."""
    acc = {}
    for b, t in tagged_rows:
        if t["pct"] is None:
            continue
        a = acc.setdefault((b, t["date"]), [0.0, 0.0, 0])
        a[0] += t["pct"]; a[1] += t["pnl"]; a[2] += 1
    return {k: (v[0] / v[2], v[1] / v[2]) for k, v in acc.items()}


def summarize(target, peers=None, only_peer_features=False):
    """target / peers: [{"pct", "pnl", "date", "ctx"}]. The whole report, pure.
    only_peer_features: skip the conditions peers don't carry (used for the
    family-wide pass, where target and peers are pooled)."""
    peers = peers or []
    if not target:
        return {"n": 0, "features": [], "findings": [], "base": _stats([]), "split_date": None}
    dates = sorted(t["date"] for t in target)
    split = dates[len(dates) // 2] if len(dates) > 1 else dates[0]
    older = [t for t in target if t["date"] < split]
    newer = [t for t in target if t["date"] >= split]
    old_ids = {id(t) for t in older}
    row = lambda t: (t["pct"], t["pnl"])       # noqa: E731
    features, findings = [], []
    for key, label, group, peers_ok, order, fn in FEATURES:
        if only_peer_features and not peers_ok:
            continue
        tagged = [(fn(t["ctx"]), t) for t in target]
        known = [(b, t) for b, t in tagged if b is not None]
        if not known:
            continue
        ptag = [(fn(p["ctx"]), p) for p in peers] if peers_ok else []
        labels = list(order) if order else sorted({b for b, _ in known})
        labels += sorted({b for b, _ in known} - set(labels))
        buckets = []
        day = key in DAY_LEVEL
        dm = _day_means(known) if day else None
        for lab in labels:
            ins = [row(t) for b, t in known if b == lab]
            if not ins:
                continue
            out = [row(t) for b, t in known if b != lab]
            o_in = [row(t) for b, t in known if b == lab and id(t) in old_ids]
            o_out = [row(t) for b, t in known if b != lab and id(t) in old_ids]
            n_in = [row(t) for b, t in known if b == lab and id(t) not in old_ids]
            n_out = [row(t) for b, t in known if b != lab and id(t) not in old_ids]
            p_in = [row(p) for b, p in ptag if b == lab]
            p_out = [row(p) for b, p in ptag if b is not None and b != lab]
            st = _stats(ins)
            lift = _lift(ins, out)
            if day:
                tval = _tstat([v for (b, _d), v in dm.items() if b == lab],
                              [v for (b, _d), v in dm.items() if b != lab])
            else:
                tval = _tstat(ins, out)
            half_ok = len(o_in) >= MIN_HALF and len(n_in) >= MIN_HALF
            lo, ln = _lift(o_in, o_out), _lift(n_in, n_out)
            holds = bool(half_ok and lift and lo is not None and ln is not None
                         and (lo > 0) == (lift > 0) and (ln > 0) == (lift > 0))
            peer = _stats(p_in) if peers_ok else None
            peer_lift = _lift(p_in, p_out) if peers_ok and len(p_in) >= MIN_PEER else None
            n_days = len({t["date"] for bb, t in known if bb == lab})
            thin = st["n"] < MIN_BUCKET or (day and n_days < MIN_HALF)
            strong = tval is not None and abs(tval) >= T_FLOOR and not thin
            b = {"label": lab, **st, "days": n_days, "lift": lift, "t": tval, "strong": strong,
                 "thin": thin,
                 "older": {**_stats(o_in), "lift": lo}, "newer": {**_stats(n_in), "lift": ln},
                 "holds": holds, "peer": peer, "peer_lift": peer_lift}
            buckets.append(b)
            if (holds and strong and not b["thin"] and len(out) >= MIN_BUCKET and lift is not None
                    and abs(lift) >= LIFT_FLOOR):
                findings.append({"feature": key, "feature_label": label, "bucket": lab, "n": st["n"],
                                 "win_rate": st["win_rate"], "avg_pct": st["avg_pct"], "lift": lift,
                                 "older_lift": lo, "newer_lift": ln, "peer_lift": peer_lift, "t": tval,
                                 "peer_agrees": None if peer_lift is None else (peer_lift > 0) == (lift > 0),
                                 "score": abs(tval)})
        features.append({"key": key, "label": label, "group": group, "peers": peers_ok, "day_level": day,
                         "unknown": len(tagged) - len(known), "buckets": buckets})
    findings.sort(key=lambda f: -f["score"])
    return {"n": len(target), "base": _stats([row(t) for t in target]),
            "older_n": len(older), "newer_n": len(newer), "split_date": split,
            "peer_n": len(peers), "peer_base": _stats([row(p) for p in peers]),
            "features": features,
            "helped": [f for f in findings if f["lift"] > 0][:6],
            "hurt": [f for f in findings if f["lift"] < 0][:6]}


def family(strategy):
    """'MU_CAM_BREAKOUT_R3S3_V02_5MIN' -> 'CAM_BREAKOUT_R3S3' (the peer group)."""
    parts = (strategy or "").upper().split("_")
    if len(parts) >= 4 and parts[1] == "CAM":
        return "_".join(parts[1:4])
    return "_".join(parts[1:-2]) if len(parts) > 3 else "_".join(parts[1:])


# ── market data (live), cached ────────────────────────────────────────────────

def _app():
    import app as _kairos
    return _kairos


class _LRU:
    def __init__(self, cap):
        self.cap, self.d, self.lock = cap, OrderedDict(), threading.Lock()

    def get(self, k):
        with self.lock:
            if k in self.d:
                self.d.move_to_end(k)
                return True, self.d[k]
            return False, None

    def put(self, k, v):
        with self.lock:
            self.d[k] = v
            self.d.move_to_end(k)
            while len(self.d) > self.cap:
                self.d.popitem(last=False)


_minute = _LRU(1500)        # (ticker, date) -> (epochs array, closes array, premarket volume)
_daily = _LRU(300)          # ticker -> (fetched_at, start, end, [bars])
_classify = _LRU(6000)      # (ticker, date) -> dict
_vix = {"at": 0.0, "start": None, "data": {}}
_results = _LRU(60)


def _today():
    return datetime.now(_ET).date().isoformat()


def _day_minutes(ticker, d):
    """(epochs, closes, premarket_volume) for one session, regular hours closes only.
    Past sessions are cached; today's never is."""
    hit, v = _minute.get((ticker, d))
    if hit:
        return v
    a = _app()
    bars = a._fetch_day_bars(ticker, d) or []
    ep, cl, pm = array("d"), array("d"), 0.0
    for b in bars:
        ts = b.timestamp if isinstance(b.timestamp, datetime) else datetime.fromisoformat(str(b.timestamp))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        et = ts.astimezone(_ET)
        hm = et.hour * 100 + et.minute
        if 800 <= hm < 930:
            pm += float(getattr(b, "volume", 0) or 0)
        elif 930 <= hm < 1600:
            ep.append(ts.timestamp()); cl.append(float(b.close))
    v = (ep, cl, pm if bars else None)
    if d < _today() and bars:
        _minute.put((ticker, d), v)
    return v


def _daily_bars(ticker, start, end):
    """[{date, open, high, low, close, volume}] oldest first, covering [start, end]."""
    hit, v = _daily.get(ticker)
    if hit and v[1] <= start and v[2] >= end and time.time() - v[0] < 3600:
        return v[3]
    a = _app()
    out = []
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        if a.alpaca_broker is None:
            return []
        client = StockHistoricalDataClient(api_key=a.alpaca_broker._key, secret_key=a.alpaca_broker._secret)
        s, e = date.fromisoformat(start), date.fromisoformat(end)
        req = StockBarsRequest(symbol_or_symbols=ticker.upper(), timeframe=TimeFrame(1, TimeFrameUnit.Day),
                               start=datetime(s.year, s.month, s.day, tzinfo=timezone.utc),
                               end=datetime(e.year, e.month, e.day, 23, 59, tzinfo=timezone.utc),
                               feed=a._alpaca_data_feed())
        for b in list(client.get_stock_bars(req)[ticker.upper()]):
            out.append({"date": str(b.timestamp.astimezone(_ET).date() if hasattr(b.timestamp, "astimezone")
                                    else str(b.timestamp)[:10]),
                        "open": float(b.open), "high": float(b.high), "low": float(b.low),
                        "close": float(b.close), "volume": float(b.volume or 0)})
    except Exception as e:
        a.log.debug("trade_context daily %s: %s", ticker, e)
        return []
    _daily.put(ticker, (time.time(), start, end, out))
    return out


def _vix_closes(start, end):
    """{date: VIX close} via yfinance (6h cache); {} if unavailable."""
    if _vix["start"] and _vix["start"] <= start and time.time() - _vix["at"] < 6 * 3600:
        return _vix["data"]
    try:
        import yfinance as yf
        df = yf.download("^VIX", start=start, end=(date.fromisoformat(end) + timedelta(days=1)).isoformat(),
                         progress=False, auto_adjust=True)
        col = df["Close"]
        if hasattr(col, "columns"):
            col = col.iloc[:, 0]
        data = {str(i.date()): float(v) for i, v in col.items() if v == v}
    except Exception as e:
        _app().log.debug("trade_context vix: %s", e)
        data = {}
    if data:
        _vix.update(at=time.time(), start=start, data=data)
    return data


def _classify_day(ticker, d):
    hit, v = _classify.get((ticker, d))
    if hit:
        return v
    try:
        v = _app()._classify_day(ticker, d) or {}
    except Exception:
        v = {}
    if d < _today():
        _classify.put((ticker, d), v)
    return v


def _earnings(ticker):
    try:
        from strategies.earnings import announcement_dates
        return sorted(announcement_dates(ticker, limit=16))
    except Exception:
        return []


def _rvol_map(symbols):
    """{(SYMBOL, entry minute): rvol} from trade_rvol, using the gate's method."""
    a = _app()
    col = "rvol_tod" if getattr(a, "RVOL_GATE_METHOD", "") == "tod" else "rvol"
    out = {}
    if not symbols:
        return out
    conn = a.get_db()
    try:
        cur = conn.cursor()
        ph = a.placeholder()
        syms = sorted(symbols)
        cur.execute(f"SELECT symbol, entry_time, {col} FROM trade_rvol WHERE symbol IN ({','.join([ph] * len(syms))})",
                    tuple(syms))
        for r in cur.fetchall():
            sym = r[0] if a.DATABASE_URL else r["symbol"]
            et = r[1] if a.DATABASE_URL else r["entry_time"]
            rv = r[2] if a.DATABASE_URL else r[col]
            if rv is not None:
                out[((sym or "").upper(), _minute_key(et))] = float(rv)
    except Exception as e:
        a.log.debug("trade_context rvol: %s", e)
    finally:
        conn.close()
    return out


def _minute_key(iso):
    try:
        d = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    except (TypeError, ValueError):
        return None


# ── per-trade conditions ──────────────────────────────────────────────────────

def _sma(xs, n):
    return sum(xs[-n:]) / n if len(xs) >= n else None


def _daily_ctx(bars, d):
    """Prior-close-based facts for session d from oldest-first daily bars."""
    i = next((k for k in range(len(bars) - 1, -1, -1) if bars[k]["date"] < d), None)
    if i is None:
        return {}
    prior = bars[: i + 1]
    closes = [b["close"] for b in prior]
    today = bars[i + 1] if i + 1 < len(bars) and bars[i + 1]["date"] == d else None
    trs = []
    for k in range(1, len(prior)):
        h, l, pc = prior[k]["high"], prior[k]["low"], prior[k - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr_series = [sum(trs[j - 13: j + 1]) / 14 / prior[j + 1]["close"] for j in range(13, len(trs))]
    atr_rel = None
    if len(atr_series) >= 20:
        hist = sorted(atr_series[-60:])
        med = hist[len(hist) // 2]
        atr_rel = atr_series[-1] / med if med else None
    vols = [b["volume"] for b in prior[-20:]]
    return {"prior_close": closes[-1], "open": today["open"] if today else None,
            "sma20": _sma(closes, 20), "sma50": _sma(closes, 50), "atr_rel": atr_rel,
            "avg_vol": sum(vols) / len(vols) if vols else None}


def _trend_label(close, s20, s50):
    if close is None or s20 is None or s50 is None:
        return None
    above = (close > s20) + (close > s50)
    return {2: "above both", 1: "between", 0: "below both"}[above]


def spy_last_hour_pct(ep, cl, ts, lookback_secs=3600):
    """SPY's % move over the `lookback_secs` before `ts` from regular-session minute
    closes (epochs `ep`, closes `cl`); before a full hour exists, from the first
    bar. Shared by the "What made it work?" panel and the live SPY-alignment gate,
    so the gate enforces exactly the number the evidence was measured on."""
    now = _price_at(ep, cl, ts)
    then = _price_at(ep, cl, ts - lookback_secs) or (cl[0] if cl else None)
    if not now or not then:
        return None
    return (now / then - 1) * 100


def _price_at(ep, cl, ts):
    if not ep:
        return None
    lo, hi = 0, len(ep)
    while lo < hi:                        # last bar at or before ts
        mid = (lo + hi) // 2
        if ep[mid] <= ts:
            lo = mid + 1
        else:
            hi = mid
    return cl[lo - 1] if lo else None


def build_context(t, md, deep):
    """Raw conditions for one round-trip. md: prefetched market data. deep: also the
    per-ticker intraday/pivot/earnings facts (target trades only)."""
    side = (t.get("side") or "").upper()
    sgn = _sgn(side)
    e = datetime.fromisoformat(str(t["entry_time"]).replace("Z", "+00:00"))
    if e.tzinfo is None:
        e = e.replace(tzinfo=timezone.utc)
    et = e.astimezone(_ET)
    d = et.date().isoformat()
    ts = e.timestamp()
    c = {"side": side, "hhmm": et.hour * 100 + et.minute, "weekday": et.strftime("%a")}

    spy_d = _daily_ctx(md["daily"].get("SPY") or [], d)
    ep, cl, _pm = md["spy_min"].get(d) or (array("d"), array("d"), None)
    spy_open = spy_d.get("open") or (cl[0] if cl else None)
    spy_now = _price_at(ep, cl, ts)
    if spy_open and spy_now:
        c["spy_day_dir"] = (spy_now / spy_open - 1) * 100 * sgn
    hr = spy_last_hour_pct(ep, cl, ts)
    if hr is not None:
        c["spy_hour_dir"] = hr * sgn
    if spy_d.get("open") and spy_d.get("prior_close"):
        c["spy_gap"] = (spy_d["open"] / spy_d["prior_close"] - 1) * 100
    c["spy_trend"] = _trend_label(spy_d.get("prior_close"), spy_d.get("sma20"), spy_d.get("sma50"))
    prev_days = [k for k in md["vix"] if k < d]
    if prev_days:
        c["vix"] = md["vix"][max(prev_days)]

    tk = (t.get("ticker") or t["strategy"].split("_", 1)[0]).upper()
    sd = _daily_ctx(md["daily"].get(tk) or [], d)
    if sd.get("open") and sd.get("prior_close"):
        c["gap_dir"] = (sd["open"] / sd["prior_close"] - 1) * 100 * sgn
    c["atr_rel"] = sd.get("atr_rel")
    tr = _trend_label(sd.get("prior_close"), sd.get("sma20"), sd.get("sma50"))
    if tr:
        c["trend_dir"] = ("with the trend" if (tr == "above both") == (sgn > 0) and tr != "between"
                          else "mixed" if tr == "between" else "against the trend")
    try:
        ep_px = float(t.get("entry_price") or 0)
    except (TypeError, ValueError):
        ep_px = 0
    if sd.get("open") and ep_px:
        c["move_dir"] = (ep_px / sd["open"] - 1) * 100 * sgn
    c["rvol"] = md["rvol"].get((tk, _minute_key(t["entry_time"])))

    if deep:
        mins = md["tk_min"].get((tk, d))
        if mins and mins[2] is not None and sd.get("avg_vol"):
            c["pm_frac"] = mins[2] / sd["avg_vol"]
        cls = md["classify"].get((tk, d)) or {}
        c["day_type"] = cls.get("day_type")
        a = md.get("app")
        if a is not None and cls and sd.get("open"):
            try:
                fr = a._open_frac(t["strategy"], side, cls, sd["open"])
                c["open_loc"] = a._open_bucket(fr) if fr is not None else None
            except Exception:
                pass
        ann = md["earnings"].get(tk) or []
        dd = date.fromisoformat(d)
        after = any(0 <= (dd - x).days <= 5 for x in ann)
        before = any(0 < (x - dd).days <= 5 for x in ann)
        if ann:
            c["earnings"] = ("0–5 days after earnings" if after else "≤ 5 days before earnings" if before
                             else "no earnings within 5 days")
    return d, c


def _assign_terciles(rows, key, out_key, labels=("light", "normal", "heavy")):
    vals = sorted(r["ctx"][key] for r in rows if r["ctx"].get(key) is not None)
    if len(vals) < 6:
        return
    lo, hi = vals[len(vals) // 3], vals[2 * len(vals) // 3]
    for r in rows:
        v = r["ctx"].get(key)
        if v is not None:
            r["ctx"][out_key] = labels[0] if v < lo else labels[2] if v >= hi else labels[1]


# ── trades in scope ───────────────────────────────────────────────────────────

def _round_trips(a, only_acct, from_date, to_date):
    import routes.farms as farms
    books = [x for x in (a.ALPACA_ACCOUNTS or []) if not only_acct or x["num"] == only_acct or x["tag"] == only_acct]
    rts, unavailable = [], []
    for acct in books:
        try:
            _b, _tag, label, fills_fn = a._alpaca_account_ctx(acct["num"])
            fills = fills_fn()
        except Exception:
            fills, label = None, acct.get("label")
        if not fills:
            if a._fills_error(acct["num"]):
                unavailable.append(label)
            continue
        paired = a._pair_alpaca_fills_lifo(fills, from_date=from_date, to_date=to_date)
        for c in farms.merge_fragments(paired.get("closed_clean") or []):
            s = (c.get("strategy") or "").strip().upper()
            if s and s != "UNKNOWN" and c.get("entry_time"):
                rts.append({**c, "strategy": s, "ticker": (c.get("ticker") or s.split("_", 1)[0]).upper()})
    if not only_acct:
        # The same signal taken by several books is one observation, not four.
        seen, out = set(), []
        for t in rts:
            k = (t["strategy"], (t.get("side") or "").upper(), _minute_key(t["entry_time"]))
            if k not in seen:
                seen.add(k); out.append(t)
        rts = out
    return rts, unavailable


def _pct(t):
    try:
        n = abs(float(t.get("entry_price") or 0)) * abs(float(t.get("qty") or 0))
        return float(t.get("pnl") or 0) / n * 100 if n else None
    except (TypeError, ValueError):
        return None


def run(ticker, strategy, account, from_date, to_date):
    a = _app()
    rts, unavailable = _round_trips(a, account, from_date, to_date)
    target = [t for t in rts if t["ticker"] == ticker and (not strategy or t["strategy"] == strategy)]
    fams = {family(t["strategy"]) for t in target}
    peer_all = [t for t in rts if t["ticker"] != ticker and family(t["strategy"]) in fams]
    counts = {}
    for t in peer_all:
        counts[t["ticker"]] = counts.get(t["ticker"], 0) + 1
    keep = set(sorted(counts, key=lambda k: -counts[k])[:PEER_TICKER_CAP])
    peers = [t for t in peer_all if t["ticker"] in keep]
    if not target:
        return {"error": None, "n": 0, "features": [], "unavailable": unavailable}

    def _d(t):
        return _minute_key(t["entry_time"])[:10] if _minute_key(t["entry_time"]) else None
    all_dates = sorted({x for x in (_d(t) for t in target + peers) if x})
    start = (date.fromisoformat(all_dates[0]) - timedelta(days=130)).isoformat()
    end = all_dates[-1]
    # Session dates in ET (an entry is always in regular hours, so its UTC date is its ET date).
    t_dates = sorted({_d(t) for t in target})

    md = {"app": a, "daily": {}, "spy_min": {}, "tk_min": {}, "classify": {}, "earnings": {}}
    tickers = sorted({"SPY", ticker} | keep)
    with ThreadPoolExecutor(max_workers=8) as ex:
        f_daily = {tk: ex.submit(_daily_bars, tk, start, end) for tk in tickers}
        f_spy = {d: ex.submit(_day_minutes, "SPY", d) for d in all_dates}
        f_tk = {d: ex.submit(_day_minutes, ticker, d) for d in t_dates}
        f_cls = {d: ex.submit(_classify_day, ticker, d) for d in t_dates}
        f_vix = ex.submit(_vix_closes, start, end)
        f_earn = ex.submit(_earnings, ticker)
        md["daily"] = {tk: f.result() for tk, f in f_daily.items()}
        md["spy_min"] = {d: f.result() for d, f in f_spy.items()}
        md["tk_min"] = {(ticker, d): f.result() for d, f in f_tk.items()}
        md["classify"] = {(ticker, d): f.result() for d, f in f_cls.items()}
        md["vix"] = f_vix.result()
        md["earnings"] = {ticker: f_earn.result()}
    md["rvol"] = _rvol_map({ticker} | keep)

    def _rows(ts, deep):
        out = []
        for t in ts:
            try:
                d, c = build_context(t, md, deep)
            except Exception as e:
                a.log.debug("trade_context ctx %s: %s", t.get("strategy"), e)
                continue
            out.append({"pct": _pct(t), "pnl": float(t.get("pnl") or 0), "date": d, "ctx": c})
        return out
    trows, prows = _rows(target, True), _rows(peers, False)
    for r in trows:
        if r["ctx"].get("pm_frac") is not None:
            r["ctx"]["pm"] = r["ctx"]["pm_frac"]
    _assign_terciles(trows, "pm", "pm_bucket")
    out = summarize(trows, prows)
    # The same question for the whole strategy family (this ticker + its peers),
    # on the conditions every ticker carries: far more trades, so real effects
    # that are too faint on one ticker can clear the bar here.
    fam = summarize(trows + prows, only_peer_features=True) if prows else {"helped": [], "hurt": []}
    out["family_helped"], out["family_hurt"] = fam["helped"], fam["hurt"]
    out["family_n"] = len(trows) + len(prows)
    # A market-wide condition found on one ticker alone is judged on few days; say
    # whether the family (same days, many more trades) backs it up.
    fam_keys = {(f["feature"], f["bucket"]) for f in fam["helped"] + fam["hurt"]}
    for f in out["helped"] + out["hurt"]:
        if f["feature"] in DAY_LEVEL and prows:
            f["family_confirms"] = (f["feature"], f["bucket"]) in fam_keys
    out.update({"ticker": ticker, "strategy": strategy or None, "account": account or None,
                "from_date": from_date, "to_date": to_date, "families": sorted(fams),
                "peer_tickers": len(keep), "unavailable": unavailable,
                "vix_available": bool(md["vix"]), "error": None})
    return out


@trade_context_bp.route("/api/ticker/context")
def api_ticker_context():
    """?ticker=MU &strategy= &account= &from_date= &to_date= — conditions at entry
    split by winners and losers, with older/newer halves and peers. Read-only;
    results cached 15 minutes."""
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "Pick a ticker."}), 400
    strategy = (request.args.get("strategy") or "").strip().upper()
    account = (request.args.get("account") or "").strip()
    f, t = (request.args.get("from_date") or "").strip(), (request.args.get("to_date") or "").strip()
    key = (ticker, strategy, account, f, t)
    hit, v = _results.get(key)
    if hit and time.time() - v[0] < _RESULT_TTL and not request.args.get("fresh"):
        return jsonify(v[1])
    try:
        out = run(ticker, strategy, account, f, t)
    except Exception as e:
        _app().log.warning("trade context %s failed: %s", ticker, e)
        return jsonify({"error": f"Could not build the analysis: {e}"}), 500
    _results.put(key, (time.time(), out))
    return jsonify(out)


# ── What-if: the SPY-alignment gate and a 10:00 start on a Refined book's past ──
# The gate-cost tool prices a gate from the signals it actually blocked, so it can't
# judge a gate that's still off. This replays history instead: every past trade is
# tested against the gate's rule (the same spy_last_hour_pct the live gate uses),
# and the trades it would have blocked are summed. Two samples: the book's own
# trades (already through its other gates and hours) and its farm's breakouts inside
# the book's trading hours (far more trades, but ungated otherwise).

WHATIF_BOOKS = {"2": ("alpaca2", "1", "TV Refined", "TV Farm"),
                "3": ("alpaca3", "5", "Kairos Refined", "Kairos Farm")}
SPY_THRESHOLDS = (0.0, 0.10, 0.15, 0.25)


def _hhmm(s):
    try:
        h, m = str(s).split(":")[:2]
        return int(h) * 100 + int(m)
    except (TypeError, ValueError):
        return None


def whatif_scenarios(rows, min_pct, thresholds=SPY_THRESHOLDS, start_hhmm=1000):
    """rows: [{"pct", "pnl", "date", "hhmm", "breakout", "spy_dir"}] -> scenarios.

    A scenario lists the trades it would have BLOCKED and the ones it keeps. The SPY
    rule only touches breakouts and lets a trade with no SPY reading through (the
    live gate fails open the same way)."""
    def grp(rs):
        n = len(rs)
        pcts = [r["pct"] for r in rs if r["pct"] is not None]
        return {"n": n, "pnl": round(sum(r["pnl"] for r in rs), 2),
                "sum_pct": round(sum(pcts), 3), "avg_pct": round(sum(pcts) / len(pcts), 4) if pcts else None,
                "win_rate": round(sum(1 for r in rs if r["pnl"] > 0) / n * 100, 1) if n else None}

    dates = sorted(r["date"] for r in rows)
    split = dates[len(dates) // 2] if dates else None

    def _avg(rs):
        p = [r["pct"] for r in rs if r["pct"] is not None]
        return sum(p) / len(p) if p else 0.0
    avg_all = _avg(rows)
    avg_old = _avg([r for r in rows if r["date"] < split])
    avg_new = _avg([r for r in rows if r["date"] >= split])

    def scen(label, blocked_fn):
        b = [r for r in rows if blocked_fn(r)]
        k = [r for r in rows if not blocked_fn(r)]
        bo = [r for r in b if r["date"] < split]
        bn = [r for r in b if r["date"] >= split]

        def edge(rs, base):
            # What blocking these saved BEYOND blocking the same number of trades at
            # random. When nearly every trade loses, blocking any big group "saves",
            # so the raw saving can't tell a good gate from a coin flip; this can.
            return round(sum(base - (r["pct"] or 0) for r in rs), 3)
        return {"label": label, "blocked": grp(b), "kept": grp(k),
                # Positive = the blocked trades lost money, so the gate would have saved it.
                "saved_pct": round(-sum(r["pct"] or 0 for r in b), 3),
                "saved_older_pct": round(-sum(r["pct"] or 0 for r in bo), 3),
                "saved_newer_pct": round(-sum(r["pct"] or 0 for r in bn), 3),
                "edge_pct": edge(b, avg_all), "edge_older_pct": edge(bo, avg_old),
                "edge_newer_pct": edge(bn, avg_new)}

    spy = lambda thr: (lambda r: r["breakout"] and r["spy_dir"] is not None and r["spy_dir"] < thr)  # noqa: E731
    early = lambda r: r["hhmm"] is not None and r["hhmm"] < start_hhmm                             # noqa: E731
    out = {"all": grp(rows), "split_date": split, "unknown_spy": sum(1 for r in rows if r["breakout"] and r["spy_dir"] is None),
           "spy": [dict(scen(f"SPY's last hour ≥ {t:g}% the trade's way", spy(t)), threshold=t) for t in thresholds],
           "start": scen(f"Start at {start_hhmm // 100}:{start_hhmm % 100:02d}", early),
           "both": scen(f"Both (SPY ≥ {min_pct:g}% and start at {start_hhmm // 100}:{start_hhmm % 100:02d})",
                        lambda r: spy(min_pct)(r) or early(r))}
    return out


def _whatif_rows(rts, md):
    out = []
    for t in rts:
        try:
            e = datetime.fromisoformat(str(t["entry_time"]).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
        if e.tzinfo is None:
            e = e.replace(tzinfo=timezone.utc)
        et = e.astimezone(_ET)
        d = et.date().isoformat()
        ep, cl, _pm = md.get(d) or (array("d"), array("d"), None)
        mv = spy_last_hour_pct(ep, cl, e.timestamp())
        sgn = _sgn(t.get("side"))
        out.append({"pct": _pct(t), "pnl": float(t.get("pnl") or 0), "date": d,
                    "hhmm": et.hour * 100 + et.minute, "breakout": "BREAKOUT" in t["strategy"],
                    "spy_dir": None if mv is None else mv * sgn})
    return out


def _in_windows(t, windows):
    if not windows:
        return True
    try:
        e = datetime.fromisoformat(str(t["entry_time"]).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    if e.tzinfo is None:
        e = e.replace(tzinfo=timezone.utc)
    hm = e.astimezone(_ET).hour * 100 + e.astimezone(_ET).minute
    for s, en in windows:
        a, b = _hhmm(s), _hhmm(en)
        if a is not None and b is not None and a <= hm < b:
            return True
    return False


def run_whatif(book, days):
    a = _app()
    tag, farm_num, label, farm_label = WHATIF_BOOKS[book]
    today = datetime.now(_ET).date()
    frm = (today - timedelta(days=days)).isoformat()
    book_rts, err1 = _round_trips(a, book, frm, today.isoformat())
    farm_rts, err2 = _round_trips(a, farm_num, frm, today.isoformat())
    windows = a._account_hours_windows(tag) or []
    farm_rts = [t for t in farm_rts if "BREAKOUT" in t["strategy"] and _in_windows(t, windows)]
    dates = sorted({_minute_key(t["entry_time"])[:10] for t in book_rts + farm_rts if _minute_key(t["entry_time"])})
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {d: ex.submit(_day_minutes, "SPY", d) for d in dates}
        md = {d: f.result() for d, f in futs.items()}
    on, mn = a._spy_align_settings(tag)
    return {"book": book, "label": label, "farm_label": farm_label, "days": days,
            "hours": [f"{s}-{e}" for s, e in windows], "spy_gate_on": on, "min_pct": mn,
            "unavailable": (err1 or []) + (err2 or []),
            "book_trades": whatif_scenarios(_whatif_rows(book_rts, md), mn),
            "farm_trades": whatif_scenarios(_whatif_rows(farm_rts, md), mn)}


@trade_context_bp.route("/api/gates/what_if")
def api_gates_what_if():
    """?book=2|3 &days=60 — what the SPY-alignment gate and a 10:00 start would
    have blocked on this Refined book's past trades and on its farm's in-hours
    breakouts. Read-only; cached 15 minutes."""
    book = str(request.args.get("book") or "3")
    if book not in WHATIF_BOOKS:
        return jsonify({"error": "book must be 2 (TV Refined) or 3 (Kairos Refined)"}), 400
    try:
        days = max(14, min(180, int(request.args.get("days") or 60)))
    except ValueError:
        days = 60
    key = ("whatif", book, days)
    hit, v = _results.get(key)
    if hit and time.time() - v[0] < _RESULT_TTL and not request.args.get("fresh"):
        return jsonify(v[1])
    try:
        out = run_whatif(book, days)
    except Exception as e:
        _app().log.warning("gate what-if failed: %s", e)
        return jsonify({"error": f"Could not run the what-if: {e}"}), 500
    _results.put(key, (time.time(), out))
    return jsonify(out)
