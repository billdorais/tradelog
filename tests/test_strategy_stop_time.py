"""Per-strategy stop time: set from the Ticker Breakdown, enforced per book."""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

import app as a

ET = ZoneInfo("America/New_York")
S = "MU_CAM_BREAKOUT_R3S3_V02_5MIN"


@pytest.fixture()
def store(monkeypatch):
    db = {}
    monkeypatch.setattr(a, "_load_setting", lambda k, d=None: db.get(k, d))
    monkeypatch.setattr(a, "_save_setting", lambda k, v: db.__setitem__(k, v))
    monkeypatch.setattr(a, "_stop_times_ts", 0.0)
    monkeypatch.setattr(a, "_stop_times_cache", {})
    return db


def _at(h, m):
    return dt.datetime(2026, 10, 29, h, m, tzinfo=ET)


def test_set_enforce_and_clear(store):
    c = a.app.test_client()
    r = c.post("/api/strategy/stop_time", json={"strategy": S.lower(), "stop": "12:00",
                                                "accounts": ["alpaca2", "alpaca4", "alpaca6"],
                                                "basis": {"book": "Kairos Farm", "days": 30, "trades": 47,
                                                          "both_halves": True, "junk": 1}})
    assert r.status_code == 200, r.get_json()
    e = r.get_json()["entry"]
    assert e["stop"] == "12:00" and e["accounts"] == ["alpaca6", "alpaca4", "alpaca2"]   # UI order
    assert "junk" not in e["basis"]

    assert not a._strategy_stop_block(S, "alpaca2", now_et=_at(11, 59))[0]
    blk, why = a._strategy_stop_block(S, "alpaca2", now_et=_at(12, 0))
    assert blk and "12:00" in why
    assert a._strategy_stop_block(S, "alpaca6", now_et=_at(14, 0))[0]
    assert not a._strategy_stop_block(S, "alpaca3", now_et=_at(14, 0))[0]        # not ticked
    assert not a._strategy_stop_block("NVDA_CAM_BREAKOUT_R3S3_V02_5MIN", "alpaca2", now_et=_at(14, 0))[0]

    assert c.get("/api/strategy/stop_time?strategy=" + S).get_json()["entry"]["stop"] == "12:00"
    assert c.get("/api/strategy/stop_times").get_json()["stops"][0]["strategy"] == S
    on, setting, _src = a._gate_state("alpaca2")["stop-time"]
    assert on and S in setting and "12:00" in setting
    assert not a._gate_state("alpaca3")["stop-time"][0]

    r = c.post("/api/strategy/stop_time", json={"strategy": S, "clear": True})
    assert r.get_json()["cleared"] is True
    assert not a._strategy_stop_block(S, "alpaca2", now_et=_at(14, 0))[0]


def test_refuses_farms_bad_times_and_wildcards(store):
    c = a.app.test_client()
    post = lambda **b: c.post("/api/strategy/stop_time", json={"strategy": S, "stop": "12:00",  # noqa: E731
                                                               "accounts": ["alpaca2"], **b})
    assert post(accounts=["alpaca5"]).status_code == 400          # Kairos Farm
    assert post(accounts=["alpaca", "alpaca2"]).status_code == 400  # TV Farm
    assert post(accounts=[]).status_code == 400
    for bad in ("9:30", "09:30", "16:00", "noon", "25:00"):
        assert post(stop=bad).status_code == 400, bad
    assert post(strategy="").status_code == 400
    assert post(strategy="MU_*").status_code == 400
    assert "STRATEGY_STOP_TIMES" not in store


def test_a_farm_never_blocks_even_if_the_setting_names_it(store):
    store["STRATEGY_STOP_TIMES"] = '{"%s": {"stop": "10:00", "accounts": ["alpaca5", "alpaca2"]}}' % S
    assert not a._strategy_stop_block(S, "alpaca5", now_et=_at(15, 0))[0]
    assert a._strategy_stop_block(S, "alpaca2", now_et=_at(15, 0))[0]


def test_stop_books_are_the_non_farm_books():
    tags = a._stop_time_tags()
    assert "alpaca" not in tags and "alpaca5" not in tags
    assert {"alpaca2", "alpaca3", "alpaca4", "alpaca6", "alpaca7"} <= set(tags)


def test_both_entry_paths_check_it():
    hook = open("routes/webhook.py", encoding="utf-8").read()
    assert "app._strategy_stop_block(strategy_name, _acct_tag)" in hook
    src = open("app.py", encoding="utf-8").read()
    i = src.index("if not _account_hours_ok(broker_tag, now_et=now_et, ticker=tk, strategy=strat):")
    assert "_strategy_stop_block(strat, broker_tag, now_et=now_et)" in src[i:i + 600]


def test_pages_have_the_controls():
    tk = open("templates/tickers.html", encoding="utf-8").read()
    assert 'id="stopApplyBtn"' in tk and "confirmStopApply()" in tk and "REAL MONEY" in tk
    rt = open("templates/routing.html", encoding="utf-8").read()
    assert 'id="stopTimesCard"' in rt and "loadStopTimes()" in rt


@pytest.fixture()
def refined_hours(monkeypatch):
    wins = [("09:35", "10:00"), ("12:00", "15:55")]
    monkeypatch.setattr(a, "_account_hours_windows", lambda tag: [] if tag in ("alpaca", "alpaca5", "alpaca7") else wins)
    monkeypatch.setattr(a, "_account_gate_overrides", lambda tag=None: {})
    monkeypatch.setattr(a, "_hours_exempt_tickers", lambda tag: set())


def _set(c, mode, stop="12:00", accounts=("alpaca2",)):
    return c.post("/api/strategy/stop_time", json={"strategy": S, "stop": stop, "mode": mode,
                                                   "accounts": list(accounts)})


def test_within_mode_keeps_the_gap_closed(store, refined_hours):
    c = a.app.test_client()
    assert _set(c, "within", stop="14:00").get_json()["entry"]["mode"] == "within"
    assert not a._account_hours_ok("alpaca2", now_et=_at(10, 30), strategy=S)     # 10-12 still shut
    assert a._account_hours_ok("alpaca2", now_et=_at(12, 30), strategy=S)
    assert a._strategy_stop_block(S, "alpaca2", now_et=_at(14, 0))[0]
    assert not a._book_has_hours_override("alpaca2")


def test_override_mode_trades_from_the_book_open_to_the_stop(store, refined_hours):
    c = a.app.test_client()
    assert _set(c, "override").get_json()["entry"]["mode"] == "override"
    assert not a._account_hours_ok("alpaca2", now_et=_at(9, 32), strategy=S)      # before the 09:35 open
    assert a._account_hours_ok("alpaca2", now_et=_at(10, 30), strategy=S)         # the gap is open for it
    assert a._strategy_stop_block(S, "alpaca2", now_et=_at(12, 0))[0]             # and the stop ends it
    # Only this strategy, only the ticked book.
    assert not a._account_hours_ok("alpaca2", now_et=_at(10, 30), strategy="NVDA_CAM_BREAKOUT_R3S3_V02_5MIN")
    assert not a._account_hours_ok("alpaca2", now_et=_at(10, 30))
    assert not a._account_hours_ok("alpaca3", now_et=_at(10, 30), strategy=S)
    assert a._book_has_hours_override("alpaca2") and not a._book_has_hours_override("alpaca3")
    assert "overrides hours" in a._gate_state("alpaca2")["stop-time"][1]


def test_get_shows_each_books_hours_and_bad_modes_are_refused(store, refined_hours):
    c = a.app.test_client()
    books = {b["tag"]: b for b in c.get("/api/strategy/stop_time?strategy=" + S).get_json()["books"]}
    assert books["alpaca2"]["windows"] == ["09:35-10:00", "12:00-15:55"] and books["alpaca2"]["open"] == "09:35"
    assert books["alpaca7"]["windows"] == [] and books["alpaca7"]["open"] == "09:30"
    assert _set(c, "sideways").status_code == 400


def test_override_reaches_both_entry_paths():
    hook = open("routes/webhook.py", encoding="utf-8").read()
    assert "strategy=strategy_name)]" in hook                                  # account window
    assert 'app._strategy_stop_mode(strategy_name, _bt_tag) == "override"' in hook  # rule window
    src = open("app.py", encoding="utf-8").read()
    assert "ticker=tk, strategy=strat)" in src
    assert '_book_has_hours_override(_a["tag"])' in src                         # coarse engine check


def test_start_defaults_to_0935_and_blocks_earlier_entries(store, refined_hours):
    c = a.app.test_client()
    e = _set(c, "within", accounts=("alpaca7",)).get_json()["entry"]     # Select trades all day
    assert e["start"] == "09:35"
    blk, why = a._strategy_stop_block(S, "alpaca7", now_et=_at(9, 31))
    assert blk and "before 09:35" in why
    assert not a._strategy_stop_block(S, "alpaca7", now_et=_at(9, 35))[0]


def test_a_custom_window_drives_override_mode(store, refined_hours):
    c = a.app.test_client()
    r = c.post("/api/strategy/stop_time", json={"strategy": S, "start": "10:15", "stop": "11:45",
                                                "mode": "override", "accounts": ["alpaca2"]})
    assert r.get_json()["entry"]["start"] == "10:15"
    assert not a._account_hours_ok("alpaca2", now_et=_at(10, 0), strategy=S)
    assert a._account_hours_ok("alpaca2", now_et=_at(10, 15), strategy=S)
    assert not a._strategy_stop_block(S, "alpaca2", now_et=_at(11, 0))[0]
    assert a._strategy_stop_block(S, "alpaca2", now_et=_at(11, 45))[0]
    assert "10:15 to 11:45" in a._gate_state("alpaca2")["stop-time"][1]


def test_bad_starts_are_refused(store):
    c = a.app.test_client()
    post = lambda start: c.post("/api/strategy/stop_time", json={  # noqa: E731
        "strategy": S, "start": start, "stop": "12:00", "accounts": ["alpaca2"]})
    for bad in ("09:29", "12:00", "13:00", "9:35", "x"):
        assert post(bad).status_code == 400, bad
    assert post("09:30").status_code == 200


def test_the_router_pipelines_show_the_window_on_the_hours_node():
    rt = open("templates/routing.html", encoding="utf-8").read()
    assert "function _ruleStratWindow(rule)" in rt and "${windowNote}" in rt
    i = rt.index("async function loadStopTimes")
    assert "_STRAT_WINDOWS = " in rt[i:i + 1200] and "renderAll()" in rt[i:i + 1200]
