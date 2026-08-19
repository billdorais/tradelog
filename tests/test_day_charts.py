"""Per-session charts for one strategy (/api/strategy/day_charts).

The explorer's receipts table says WHAT happened; this endpoint says what it
LOOKED like — one intraday chart per session the strategy traded. It is the
Chart Review payload sliced the other way (one strategy x every day, rather than
one day x every ticker), so both pages build markers/levels through the same
shared helpers and cannot drift apart.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


def _rt(strat, pnl, date, side="LONG", ticker="NVDA", entry="14:35", exit_="14:50"):
    return {"strategy": strat, "ticker": ticker, "pnl": pnl, "qty": 10,
            "entry_price": 100.0, "exit_price": 100.0 + pnl / 10, "side": side,
            "date": date, "entry_time": f"{date}T{entry}:03Z",
            "exit_time": f"{date}T{exit_}:07Z", "exit_reason": "Trail"}


# ── shared chart builders ───────────────────────────────────────────────────

def test_bar_epoch_floors_to_the_five_minute_grid():
    """Markers must land on a bar, not between two."""
    ep = a._bar_epoch("2026-08-12T14:37:41Z")
    assert ep % 300 == 0
    assert a._bar_epoch("2026-08-12T14:35:00Z") == ep      # same 5-min bucket
    assert a._bar_epoch("garbage") is None
    assert a._bar_epoch(None) is None


def test_markers_colour_exits_by_pnl_not_side():
    """A losing SHORT and a losing LONG should read the same — red exit."""
    win  = a._chart_markers([_rt("S", 12.0, "2026-08-12", side="LONG")])
    loss = a._chart_markers([_rt("S", -12.0, "2026-08-12", side="SHORT")])
    assert [m["shape"] for m in win]  == ["arrowUp", "arrowDown"]     # buy, then exit
    assert [m["shape"] for m in loss] == ["arrowDown", "arrowUp"]     # short, then cover
    assert win[1]["color"]  == "#26a69a"      # profitable exit
    assert loss[1]["color"] == "#ef5350"      # losing exit
    assert win[0]["position"] == "belowBar" and loss[0]["position"] == "aboveBar"


def test_markers_are_time_sorted():
    rts = [_rt("S", 1.0, "2026-08-12", entry="15:30", exit_="15:45"),
           _rt("S", 1.0, "2026-08-12", entry="14:00", exit_="14:10")]
    times = [m["time"] for m in a._chart_markers(rts)]
    assert times == sorted(times)


def test_levels_show_only_the_pair_that_traded():
    lv = {"dp": 100.0, "r3": 103.0, "s3": 97.0, "r4": 105.0, "s4": 95.0}
    r3 = {t["title"] for t in a._chart_levels(lv, [_rt("X_CAM_BREAKOUT_R3S3_V02_5MIN", 1, "2026-08-12")])}
    assert r3 == {"DP", "R3", "S3"}
    r4 = {t["title"] for t in a._chart_levels(lv, [_rt("X_CAM_BREAKOUT_R4S4_V02_5MIN", 1, "2026-08-12")])}
    assert r4 == {"DP", "R4", "S4"}
    # Unrecognised strategy → show both pairs rather than hiding the context.
    both = {t["title"] for t in a._chart_levels(lv, [_rt("MYSTERY", 1, "2026-08-12")])}
    assert both == {"DP", "R3", "S3", "R4", "S4"}
    assert a._chart_levels({}, [_rt("X", 1, "2026-08-12")]) == []   # no pivots → no lines


# ── endpoint ────────────────────────────────────────────────────────────────

def _client():
    a.app.config["TESTING"] = True
    return a.app.test_client()


@pytest.fixture
def acct(monkeypatch):
    """Stub a configured account. The test env pops ALPACA_KEY, so the registry is
    empty and the endpoint would 400 before reaching any logic. monkeypatch (not a
    bare assignment) so the registry cannot leak into other test modules."""
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "1": {"broker": object(), "label": "TV Farm", "fills_fn": lambda: []},
    })


def test_requires_a_strategy():
    r = _client().get("/api/strategy/day_charts")
    assert r.status_code == 400
    assert "strategy" in r.get_json()["error"]


def test_groups_by_session_newest_first_and_filters_to_one_strategy(monkeypatch, acct):
    """Only the requested strategy's trades, one entry per session, newest first —
    and bar/pivot fetches are stubbed so the test never touches the network."""
    target, other = "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN", "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"
    rts = [
        _rt(target, 5.0,  "2026-08-10"),
        _rt(target, -2.0, "2026-08-12"),
        _rt(target, 3.0,  "2026-08-12", entry="15:00", exit_="15:20"),
        _rt(other,  99.0, "2026-08-12", ticker="AAPL"),
    ]
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda *args, **kw: {"closed_clean": rts})
    monkeypatch.setattr(a, "_fetch_review_bars",
                        lambda tk, d, **kw: [{"time": 1, "open": 1, "high": 2, "low": 0, "close": 1}])
    monkeypatch.setattr(a, "_camarilla_levels",
                        lambda tk, d: {"dp": 100.0, "r3": 103.0, "s3": 97.0, "r4": 105.0, "s4": 95.0})

    d = _client().get(f"/api/strategy/day_charts?strategy={target}&account=1").get_json()
    assert d["strategy"] == target
    assert d["ticker"] == "NVDA"
    assert [s["date"] for s in d["sessions"]] == ["2026-08-12", "2026-08-10"]   # newest first
    aug12 = d["sessions"][0]
    assert aug12["n_trades"] == 2                       # the other strategy is excluded
    assert aug12["total_pnl"] == 1.0                    # -2.0 + 3.0
    assert d["total_pnl"] == 6.0                        # + the 5.0 on 08-10
    assert aug12["markers"] and aug12["levels"] and aug12["bars"]
    assert all(r["strategy"] == target for s in d["sessions"] for r in s["rows"])


def test_limit_caps_sessions_and_reports_what_it_cut(monkeypatch, acct):
    rts = [_rt("S", 1.0, f"2026-08-{d:02d}") for d in range(1, 11)]
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda *args, **kw: {"closed_clean": rts})
    monkeypatch.setattr(a, "_fetch_review_bars", lambda tk, d, **kw: [])
    monkeypatch.setattr(a, "_camarilla_levels", lambda tk, d: {})
    d = _client().get("/api/strategy/day_charts?strategy=S&account=1&limit=3").get_json()
    assert d["session_count"] == 3
    assert d["truncated"] == 7                     # honest about the 7 it dropped
    assert [s["date"] for s in d["sessions"]] == ["2026-08-10", "2026-08-09", "2026-08-08"]


def test_no_trades_returns_empty_not_an_error(monkeypatch, acct):
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda *args, **kw: {"closed_clean": []})
    d = _client().get("/api/strategy/day_charts?strategy=NOPE&account=1").get_json()
    assert d["sessions"] == [] and d["session_count"] == 0


# ── gate_opportunity window ─────────────────────────────────────────────────

def _seed_blocks(rows):
    conn = a.get_db(); cur = conn.cursor(); p = a.placeholder()
    cur.execute("DELETE FROM blocked_targets")
    for r in rows:
        cur.execute(f"INSERT INTO blocked_targets (ts,account,ticker,strategy,side,gate,reason) "
                    f"VALUES ({p},{p},{p},{p},{p},{p},{p})", r)
    conn.commit(); conn.close()


def test_gate_opportunity_honours_an_explicit_window(monkeypatch):
    """The recap prices the week it REPORTS on. A rolling ?days= always ends now, so
    it cannot express "last week" at all — hence explicit from/to."""
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda *A, **K: {"closed_clean": []})
    _seed_blocks([
        ("2026-08-05 14:00:00", "alpaca4", "AAA", "S1", "long",  "hours",    "x"),
        ("2026-08-11 14:00:00", "alpaca4", "BBB", "S2", "long",  "day-type", "x"),
        ("2026-08-12 15:00:00", "alpaca4", "CCC", "S3", "short", "reversal", "x"),
        ("2026-08-19 14:00:00", "alpaca4", "DDD", "S4", "long",  "hours",    "x"),
    ])
    try:
        c = _client()
        wk = c.get("/api/signals/gate_opportunity?account=alpaca4"
                   "&from=2026-08-10&to=2026-08-16").get_json()
        assert wk["from"] == "2026-08-10" and wk["to"] == "2026-08-16"
        assert wk["days"] == 7
        assert wk["total_blocks"] == 2                     # excludes 08-05 and 08-19
        gates = {g["gate"] for g in wk["books"][0]["by_gate"]}
        assert gates == {"day-type", "reversal"}

        # A later window sees only its own block — proving it is not a relabel.
        nxt = c.get("/api/signals/gate_opportunity?account=alpaca4"
                    "&from=2026-08-17&to=2026-08-23").get_json()
        assert nxt["total_blocks"] == 1

        assert c.get("/api/signals/gate_opportunity?account=alpaca4"
                     "&from=nope&to=nope").status_code == 400
    finally:
        _seed_blocks([])


def test_explicit_window_narrows_to_a_single_session(monkeypatch, acct):
    """The recap charts ONE session per strategy, so it passes from == to. Without a
    window the endpoint would fetch bars+pivots for every session in the lookback."""
    ALL = [_rt("S", -30.0, "2026-08-10"), _rt("S", 90.0, "2026-08-12"),
           _rt("S", 5.0, "2026-08-14")]

    def _windowed(fills, from_date="", to_date="", **kw):
        return {"closed_clean": [t for t in ALL
                                 if (not from_date or t["date"] >= from_date)
                                 and (not to_date or t["date"] <= to_date)]}
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", _windowed)
    monkeypatch.setattr(a, "_fetch_review_bars",
                        lambda tk, d, **kw: [{"time": 1, "open": 1, "high": 2, "low": 0, "close": 1}])
    monkeypatch.setattr(a, "_camarilla_levels", lambda tk, d: {})

    c = _client()
    one = c.get("/api/strategy/day_charts?strategy=S&account=1"
                "&from=2026-08-12&to=2026-08-12").get_json()
    assert [s["date"] for s in one["sessions"]] == ["2026-08-12"]

    wk = c.get("/api/strategy/day_charts?strategy=S&account=1"
               "&from=2026-08-10&to=2026-08-16").get_json()
    assert len(wk["sessions"]) == 3

    assert c.get("/api/strategy/day_charts?strategy=S&account=1"
                 "&from=bad&to=bad").status_code == 400
