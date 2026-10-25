"""Month-by-month pack: Kairos Refined vs Kairos Select rosters on Kairos Farm.

Every farm trade is judged against the roster that held at its ENTRY. Filtering
the past by today's roster is hindsight — today's names were picked for looking
good back then.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import routes.farms as farms

A, B, C = "AAA_CAM_BREAKOUT_R3S3_V02_5MIN", "BBB_CAM_BREAKOUT_R3S3_V02_5MIN", "CCC_CAM_BREAKOUT_R4S4_V02_5MIN"


def _rt(s, when, pnl, notional=1000.0):
    return {"strategy": s, "entry_time": when, "exit_time": when, "entry_price": 100.0,
            "qty": notional / 100.0, "pnl": pnl}


def _ep(s):
    return farms._to_epoch(s)


def test_roster_timeline_uses_the_snapshot_in_force_and_a_proxy_before_it():
    tl = farms.RosterTimeline(
        [(_ep("2026-10-01 20:16:00 UTC"), {A}), (_ep("2026-10-10 20:16:00 UTC"), {B})],
        proxy={C: (_ep("2026-09-20T14:00:00Z"), _ep("2026-09-25T14:00:00Z"))})
    assert tl.holds(A, _ep("2026-10-05T14:00:00Z"))
    assert not tl.holds(A, _ep("2026-10-12T14:00:00Z"))       # dropped at the Oct 10 run
    assert tl.holds(B, _ep("2026-10-12T14:00:00Z"))
    assert tl.holds(C, _ep("2026-09-22T14:00:00Z"))           # before any snapshot: proxy
    assert not tl.holds(C, _ep("2026-09-27T14:00:00Z"))


def test_pack_splits_farm_trades_by_the_roster_at_entry_and_adds_actuals():
    refined = farms.RosterTimeline([(_ep("2026-10-01 20:16:00 UTC"), {A}),
                                    (_ep("2026-10-10 20:16:00 UTC"), {B})])
    select = farms.RosterTimeline([(_ep("2026-10-08 18:00:00"), {A, C})],
                                  proxy={C: (_ep("2026-09-20T14:00:00Z"), _ep("2026-09-25T14:00:00Z"))})
    farm = [_rt(A, "2026-10-05T14:00:00Z", 2),    # refined only (Select's Apply is Oct 8)
            _rt(A, "2026-10-09T14:00:00Z", 3),    # both
            _rt(A, "2026-10-12T14:00:00Z", -1),   # select only (Refined dropped A Oct 10)
            _rt(B, "2026-10-12T14:00:00Z", 4),    # refined only
            _rt(C, "2026-09-22T14:00:00Z", -2),   # select via the pre-history proxy
            _rt(B, "2026-09-22T14:00:00Z", 1)]    # neither (before Refined's first run)
    actual = {"refined": [_rt(B, "2026-10-12T14:00:00Z", 30, notional=15000)],
              "select": [_rt(A, "2026-10-12T14:00:00Z", -45, notional=15000)]}
    sep, octo = farms.book_month_pack(farm, refined, select, actual, ["2026-09", "2026-10"])
    assert sep["farm"]["select_only"]["trades"] == 1 and sep["farm"]["neither"]["trades"] == 1
    f = octo["farm"]
    assert (f["refined_only"]["trades"], f["both"]["trades"], f["select_only"]["trades"]) == (2, 1, 1)
    assert f["refined_roster"]["trades"] == 3 and f["refined_roster"]["pnl"] == 9
    assert f["select_roster"]["trades"] == 2 and f["select_roster"]["pnl"] == 2
    assert f["refined_roster"]["avg_pct"] == 0.3                 # (0.2 + 0.3 + 0.4) / 3
    assert octo["actual"]["refined"]["avg_pct"] == 0.2           # $30 on $15k
    assert octo["actual"]["select"]["trades"] == 1 and sep["actual"]["select"]["trades"] == 0


def test_recent_months_runs_oldest_first_across_a_year_end():
    import datetime as dt
    assert farms._recent_months(3, dt.date(2027, 1, 15)) == ["2026-11", "2026-12", "2027-01"]


def test_select_applies_and_clears_are_logged(monkeypatch):
    import app as a
    saved = {}
    monkeypatch.setattr(a, "_load_setting", lambda k, d=None: saved.get(k, d))
    monkeypatch.setattr(a, "_save_setting", lambda k, v: saved.__setitem__(k, v))
    farms._log_select_roster(a, ["aaa_x", "BBB_Y"], "2026-10-24 14:00:00")
    farms._log_select_roster(a, [], "2026-10-25 14:00:00")
    import json
    hist = json.loads(saved["SELECT_APPLY_HISTORY"])
    assert hist == [{"at": "2026-10-24 14:00:00", "picks": ["AAA_X", "BBB_Y"]},
                    {"at": "2026-10-25 14:00:00", "picks": []}]
