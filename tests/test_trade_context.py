"""What made it work? — conditions at entry, split by winners and losers."""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

from array import array
from datetime import date, datetime, timedelta, timezone

import routes.trade_context as tc


def _row(pct, d, **ctx):
    return {"pct": pct, "pnl": pct * 10, "date": d, "ctx": ctx}


# ── buckets ───────────────────────────────────────────────────────────────────

def test_buckets_and_families():
    assert tc.bucket_tod(945) == "9:30–10:00" and tc.bucket_tod(1530) == "15:00–16:00"
    assert tc._b(0.2, [-0.15, 0.15], ["a", "b", "c"]) == "c" and tc._b(None, [1], ["x", "y"]) is None
    assert tc.family("MU_CAM_BREAKOUT_R3S3_V02_5MIN") == "CAM_BREAKOUT_R3S3"
    assert tc.family("SPY_ORB_RVOL_V01_5MIN") == "ORB_RVOL"


# ── the summary: halves, thin buckets, peers ─────────────────────────────────

def test_a_pattern_that_holds_in_both_halves_is_a_finding():
    rows = []
    for i in range(40):
        d = f"2026-0{8 if i < 20 else 9}-{(i % 20) + 1:02d}"
        hot = i % 2 == 0
        wob = 0.05 if i % 4 < 2 else -0.05          # some spread, averages unchanged
        rows.append(_row((0.30 if hot else -0.10) + wob, d, side="LONG", vix=14 if hot else 22))
    peers = [_row(0.1 if i % 2 else -0.1, "2026-08-10", side="LONG", vix=14 if i % 2 else 22) for i in range(60)]
    out = tc.summarize(rows, peers)
    assert out["n"] == 40 and out["split_date"] == "2026-09-01"
    vix = next(f for f in out["features"] if f["key"] == "vix")
    low = next(b for b in vix["buckets"] if b["label"] == "< 15")
    assert low["n"] == 20 and low["avg_pct"] == 0.3 and low["lift"] == 0.4 and low["holds"]
    assert low["strong"] and low["t"] > 2.5
    assert low["older"]["n"] == 10 and low["newer"]["n"] == 10
    assert low["peer"]["n"] == 30 and low["peer_lift"] == 0.2
    top = out["helped"][0]
    assert (top["feature"], top["bucket"], top["peer_agrees"]) == ("vix", "< 15", True)
    assert out["hurt"][0]["bucket"] == "20–25"


def test_a_pattern_that_flips_between_halves_is_not_a_finding():
    rows = []
    for i in range(40):
        older = i < 20
        hot = i % 2 == 0
        good = hot if older else not hot          # the bucket wins early, loses late
        rows.append(_row(0.3 if good else -0.1, f"2026-0{8 if older else 9}-{(i % 20) + 1:02d}",
                         side="LONG", vix=14 if hot else 22))
    out = tc.summarize(rows)
    vix = next(f for f in out["features"] if f["key"] == "vix")
    assert not any(b["holds"] for b in vix["buckets"])
    assert out["helped"] == [] and out["hurt"] == []


def test_thin_buckets_are_flagged_and_unknowns_counted():
    rows = [_row(0.5, "2026-09-01", side="LONG", vix=30)] + \
           [_row(-0.1, f"2026-09-{i:02d}", side="LONG", vix=None) for i in range(2, 12)]
    out = tc.summarize(rows)
    vix = next(f for f in out["features"] if f["key"] == "vix")
    assert vix["unknown"] == 10 and vix["buckets"][0]["thin"]
    assert out["helped"] == []


# ── conditions from market data ───────────────────────────────────────────────

def _daily(start, n, close=100.0, step=0.5, rng=2.0):
    out, d, c = [], date.fromisoformat(start), close
    while len(out) < n:
        if d.weekday() < 5:
            out.append({"date": d.isoformat(), "open": c + 0.2, "high": c + rng, "low": c - rng,
                        "close": c, "volume": 1_000_000})
            c += step
        d += timedelta(days=1)
    return out


def test_build_context_measures_in_the_trades_direction():
    daily = _daily("2026-06-01", 90)
    d = daily[-1]["date"]
    open_ = daily[-1]["open"]
    prior = daily[-2]["close"]
    t0 = datetime.fromisoformat(d + "T13:30:00+00:00").timestamp()       # 9:30 ET (EDT)
    ep, cl = array("d"), array("d")
    for m in range(0, 120):
        ep.append(t0 + m * 60); cl.append(500 + m * 0.05)               # SPY rising all morning
    spy = _daily("2026-06-01", 90, close=455.0)                         # SPY opens ~499.7 that day
    md = {"daily": {"SPY": spy, "MU": daily},
          "spy_min": {d: (ep, cl, 0.0)}, "vix": {daily[-2]["date"]: 17.5}, "rvol": {},
          "tk_min": {("MU", d): (array("d"), array("d"), 50_000.0)},
          "classify": {("MU", d): {"day_type": "Outside"}}, "earnings": {"MU": [date.fromisoformat(d) - timedelta(days=2)]},
          "app": None}
    entry = datetime.fromisoformat(d + "T15:00:00+00:00")                # 11:00 ET
    short = {"strategy": "MU_CAM_BREAKOUT_R3S3_V02_5MIN", "ticker": "MU", "side": "SHORT",
             "entry_time": entry.isoformat().replace("+00:00", "Z"), "entry_price": open_ - 1.0}
    day, c = tc.build_context(short, md, deep=True)
    assert day == d and c["hhmm"] == 1100 and c["side"] == "SHORT"
    assert c["spy_day_dir"] < 0                  # SPY up = against a short
    assert c["spy_hour_dir"] < 0
    assert round(c["gap_dir"], 3) == round(-(open_ / prior - 1) * 100, 3)
    assert c["move_dir"] > 0                     # price fell from the open: with the short
    assert c["trend_dir"] == "against the trend" # steady uptrend vs a short
    assert c["vix"] == 17.5 and c["day_type"] == "Outside"
    assert c["earnings"] == "0–5 days after earnings"
    assert round(c["pm_frac"], 3) == 0.05


# ── the endpoint, end to end on fake data ─────────────────────────────────────

def test_endpoint_runs_on_fake_books(monkeypatch):
    import app as a
    daily = _daily("2026-05-01", 140)
    days = [b["date"] for b in daily[-40:]]

    def rt(tk, d, pnl, side="LONG"):
        e = d + "T14:30:00Z"
        return {"ticker": tk, "strategy": f"{tk}_CAM_BREAKOUT_R3S3_V02_5MIN", "side": side, "entry_time": e,
                "exit_time": d + "T14:45:00Z", "entry_price": 100.0, "exit_price": 100 + pnl / 10,
                "qty": 10.0, "pnl": pnl}
    fills = [rt("MU", d, 5 if i % 2 else -3) for i, d in enumerate(days)] + \
            [rt("NVDA", d, 2 if i % 3 else -2) for i, d in enumerate(days)]
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [{"num": "5", "tag": "alpaca5", "label": "Kairos Farm"}])
    monkeypatch.setattr(a, "_alpaca_account_ctx", lambda n: (object(), "alpaca5", "Kairos Farm", lambda: list(fills)))
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda f, **kw: {"closed_clean": list(f)})
    monkeypatch.setattr(a, "_fills_error", lambda n: None)
    monkeypatch.setattr(tc, "_daily_bars", lambda tk, s, e: daily)
    monkeypatch.setattr(tc, "_day_minutes", lambda tk, d: (array("d"), array("d"), 1000.0))
    monkeypatch.setattr(tc, "_classify_day", lambda tk, d: {"day_type": "Outside"})
    monkeypatch.setattr(tc, "_vix_closes", lambda s, e: {})
    monkeypatch.setattr(tc, "_earnings", lambda tk: [])
    monkeypatch.setattr(tc, "_rvol_map", lambda syms: {})
    tc._results.d.clear()
    out = a.app.test_client().get("/api/ticker/context?ticker=MU&account=5").get_json()
    assert out["error"] is None and out["n"] == 40 and out["peer_n"] == 40
    assert out["families"] == ["CAM_BREAKOUT_R3S3"] and out["vix_available"] is False
    keys = [f["key"] for f in out["features"]]
    assert "weekday" in keys and "tod" in keys and "vix" not in keys     # no VIX data: no VIX feature
    assert a.app.test_client().get("/api/ticker/context").status_code == 400


def test_tickers_page_has_the_panel():
    html = open("templates/tickers.html", encoding="utf-8").read()
    assert 'id="ctxPanel"' in html and "/api/ticker/context" in html


def test_a_weak_difference_that_happens_to_agree_in_both_halves_is_not_a_finding():
    import random
    random.seed(4)
    rows = [_row(random.gauss(0.02 if i % 2 else 0.0, 0.5), f"2026-0{8 if i < 30 else 9}-{(i % 28) + 1:02d}",
                 side="LONG", vix=14 if i % 2 else 22) for i in range(60)]
    out = tc.summarize(rows)
    vix = next(f for f in out["features"] if f["key"] == "vix")
    assert all(not b["strong"] for b in vix["buckets"])
    assert out["helped"] == [] and out["hurt"] == []


def test_context_panel_has_a_copy_button():
    html = open("templates/tickers.html", encoding="utf-8").read()
    assert "copyContext(this)" in html and "function _ctxText(d)" in html
