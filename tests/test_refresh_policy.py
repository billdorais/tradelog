"""Refined refresh policy: daily top N vs weekly refresh with a rank buffer."""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as dt
import json

import pytest

import app as a
import routes.refresh_policy as rp


def _row(name, score):
    return (name, {}, score)


def _s(tkr, band="R3S3"):
    return f"{tkr}_CAM_BREAKOUT_{band}_V02_5MIN"


# ── the buffer rule ───────────────────────────────────────────────────────────

def test_buffer_keeps_incumbents_inside_the_exit_rank_and_admits_only_inside_entry():
    scored = [_row(_s(t), 1 - i / 100) for i, t in enumerate("ABCDEFGHIJ")]   # ranks 1..10
    inc = [_s("H"), _s("J"), _s("Z")]          # ranks 8, 10, and one no longer ranked
    rows, info = a._select_with_buffer(scored, 4, inc, entry_rank=2, exit_rank=8, cap=0)
    names = [r[0] for r in rows]
    assert names == [_s("A"), _s("B"), _s("H")]   # H kept (rank 8), J out (10 > 8), A/B admitted
    assert info["kept"] == [_s("H")] and info["added"] == [_s("A"), _s("B")]
    assert info["dropped"] == sorted([_s("J"), _s("Z")])
    # The 4th slot stays open: C is rank 3, outside the entry rank.
    assert len(rows) == 3


def test_buffer_seeds_like_the_daily_rule_and_respects_the_ticker_cap():
    scored = [_row(_s("SPY", b), 0.9 - i / 10) for i, b in enumerate(["R3S3", "R4S4", "R5S5"])] \
        + [_row(_s("QQQ"), 0.1)]
    rows, info = a._select_with_buffer(scored, 3, [], 2, 6, cap=2)
    assert [r[0] for r in rows] == [_s("SPY", "R3S3"), _s("SPY", "R4S4"), _s("QQQ")]
    rows, _ = a._select_with_buffer(scored, 3, [_s("QQQ")], 4, 6, cap=2)
    assert [r[0] for r in rows] == [_s("SPY", "R3S3"), _s("SPY", "R4S4"), _s("QQQ")]   # 3rd SPY capped


def test_buffer_ranks_for_a_top_20():
    assert a._buffer_ranks(20) == (15, 30)


def test_shared_scorer_filters_and_blends_like_the_snapshot():
    win = {"trades": 25, "total_pnl": 50, "win_rate": 60, "profit_factor": 2.0,
           "expectancy_pct": 0.1, "sharpe": 1.0, "consec_losses": 0}
    sm = {"GOOD": dict(win), "THIN": dict(win, trades=5), "LOSER": dict(win, total_pnl=-1),
          "STREAK": dict(win, consec_losses=3)}
    scored, blend = a._refined_scored(sm, {"GOOD": dict(win, trades=3)}, 20)
    assert [r[0] for r in scored] == ["GOOD"]
    assert blend("GOOD", sm["GOOD"]) == scored[0][2]


# ── scheduling ────────────────────────────────────────────────────────────────

@pytest.fixture()
def settings(monkeypatch):
    saved = {}
    monkeypatch.setattr(a, "_load_setting", lambda k, d=None: saved.get(k, d))
    monkeypatch.setattr(a, "_save_setting", lambda k, v: saved.__setitem__(k, v))
    return saved


def test_daily_mode_refreshes_every_weekday_weekly_only_fridays_or_when_stale(settings):
    mon, fri = dt.date(2026, 10, 19), dt.date(2026, 10, 23)
    assert a._refined_due_today("kairos", mon)                       # default: daily
    settings["KAIROS_REFINED_REFRESH_MODE"] = "weekly_buffer"
    settings["KAIROS_REFINED_LAST_RESULT"] = json.dumps({"run_at": "2026-10-16 20:16:00 UTC"})
    assert not a._refined_due_today("kairos", dt.date(2026, 10, 20))  # Tue, last run 4 days ago
    assert a._refined_due_today("kairos", fri)
    assert a._refined_due_today("kairos", dt.date(2026, 10, 26))      # Mon, 10 days stale
    assert a._refined_due_today("tv", mon)                            # the other book untouched


def test_incumbents_come_from_the_persisted_snapshot(settings):
    settings["REFINED_LAST_RESULT"] = json.dumps({"top_strategies": ["a_X", "B_Y"]})
    assert a._refined_incumbents("tv") == ["a_X", "B_Y"]


def test_mode_endpoint_validates_and_saves(settings):
    c = a.app.test_client()
    assert c.post("/api/refined/refresh_mode", json={"book": "tv", "mode": "hourly"}).status_code == 400
    d = c.post("/api/refined/refresh_mode", json={"book": "tv", "mode": "weekly_buffer"}).get_json()
    assert d["modes"] == {"tv": "weekly_buffer", "kairos": "daily"}
    assert settings["REFINED_REFRESH_MODE"] == "weekly_buffer"


# ── the replay ────────────────────────────────────────────────────────────────

def _rt(s, d, pnl):
    return {"strategy": s, "date": d, "exit_time": d + "T15:00:00Z", "pnl": pnl,
            "entry_price": 100.0, "qty": 10.0}


def _days(start, n):
    out, d = [], dt.date.fromisoformat(start)
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


def _rank_by_window_pnl(window, recent):
    tot = {}
    for t in window:
        tot[t["strategy"]] = tot.get(t["strategy"], 0) + t["pnl"]
    return sorted(((k, {}, v) for k, v in tot.items() if v > 0), key=lambda r: -r[2])


def _top(sc, n):
    return sc[:n]


def _buf(sc, n, inc, e, x):
    return a._select_with_buffer(sc, n, inc, e, x, cap=0)[0]


def test_replay_never_uses_the_day_it_scores_and_weekly_holds_all_week():
    days = _days("2026-09-01", 40)
    # A wins every day until it flips on day 30; B starts winning on day 30.
    rts = []
    for i, d in enumerate(days):
        rts.append(_rt("A", d, 5 if i < 30 else -5))
        rts.append(_rt("B", d, -5 if i < 30 else 5))
    out = rp.simulate(rts, _rank_by_window_pnl, _top, _buf, n=1, window_days=5,
                      entry_rank=1, exit_rank=1, warmup_days=7)
    pol = {p["key"]: p for p in out["policies"]}
    flip = days[30]
    i = out["dates"].index(flip)
    # On the flip day every policy still holds A (ranked on data before it): -5 that day.
    for p in pol.values():
        assert p["line"][i] - p["line"][i - 1] == -5.0 * 100 / 1000
    # Daily drops A after a few losing days; weekly policies keep A until the week ends.
    assert pol["daily_top"]["sum_pct"] > pol["weekly_top"]["sum_pct"]
    assert pol["weekly_top"]["refreshes"] <= len(out["weeks"])
    assert pol["daily_top"]["refreshes"] == len(out["dates"]) - 1


def test_replay_buffer_cuts_churn_on_noise():
    days = _days("2026-09-01", 45)
    rts = []
    for i, d in enumerate(days):
        # Five near-equal strategies whose order shuffles daily, all slightly positive.
        for j, s in enumerate("ABCDE"):
            rts.append(_rt(s, d, 1 + ((i + j) % 5) * 0.2))
    out = rp.simulate(rts, _rank_by_window_pnl, _top, _buf, n=2, window_days=3,
                      entry_rank=1, exit_rank=4, warmup_days=7)
    pol = {p["key"]: p for p in out["policies"]}
    assert pol["daily_buffer"]["adds"] < pol["daily_top"]["adds"]
    assert pol["weekly_buffer"]["adds"] <= pol["weekly_top"]["adds"]


def test_replay_reports_short_history():
    out = rp.simulate([_rt("A", "2026-09-01", 1), _rt("A", "2026-09-02", 1)],
                      _rank_by_window_pnl, _top, _buf, 1, 5, 1, 1, warmup_days=28)
    assert out["policies"] == [] and "warm-up" in out["error"]


def test_endpoint_uses_the_live_ranking(monkeypatch, settings):
    import routes.farms as farms
    days = _days("2026-08-03", 50)
    rts = [_rt(_s(t), d, (3 if t in "AB" else -1) + (i % 3)) for i, d in enumerate(days) for t in "ABCD"]
    for t in rts:
        t["side"] = "long"
    monkeypatch.setattr(farms, "_farm_round_trips", lambda num, f, t, full=False: (list(rts), None))
    monkeypatch.setattr(rp, "_rule_matcher", lambda a_: None)
    rp._cache.clear()
    out = a.app.test_client().get("/api/refined/refresh_test?book=kairos&n=2").get_json()
    assert out["label"] == "Kairos Refined" and out["entry_rank"] == 2 and out["exit_rank"] == 3
    assert [p["key"] for p in out["policies"]] == ["daily_top", "weekly_top", "weekly_buffer", "daily_buffer"]
    assert all(p["trades"] > 0 for p in out["policies"])


def test_analysis_page_has_the_panel():
    html = open("templates/analysis.html", encoding="utf-8").read()
    assert 'id="rpPanel"' in html and "/api/refined/refresh_test" in html and "rpSetMode" in html
