"""hours=curated on /api/alpaca/analysis.

The farms are ungated and trade all day, so their headline curve includes trades
the curated books can never take. from_time/to_time cannot express the real gate:
curated hours are MULTIPLE windows (09:35-10:00 + 12:00-15:55) and that pair is one
contiguous span. This filter shares _hhmm_in_windows with the live entry gate so the
chart and the gate cannot drift apart.
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

import app as kairos

WINDOWS = [("09:35", "10:00"), ("12:00", "15:55")]


def _fill(strategy, pnl, day, entry_hhmm, exit_hhmm="15:59"):
    return {"strategy": strategy, "side": "LONG", "pnl": pnl, "ticker": "AAPL",
            "qty": 10,
            "entry_time": f"2026-08-{day:02d}T{entry_hhmm}:00-04:00",
            "exit_time":  f"2026-08-{day:02d}T{exit_hhmm}:00-04:00"}


# entry times chosen to sit either side of each window boundary
TRADES = [
    _fill("EARLY_CAM_BREAKOUT_R3S3_V02_5MIN",  100.0, 3, "07:10"),   # pre-market
    _fill("OPEN_CAM_BREAKOUT_R3S3_V02_5MIN",    50.0, 3, "09:34"),   # 1 min early
    _fill("IN1_CAM_BREAKOUT_R3S3_V02_5MIN",     10.0, 3, "09:35"),   # window start
    _fill("IN1B_CAM_BREAKOUT_R3S3_V02_5MIN",    11.0, 3, "09:59"),   # last minute
    _fill("GAP_CAM_BREAKOUT_R3S3_V02_5MIN",    200.0, 3, "10:00"),   # window END, excl
    _fill("MID_CAM_BREAKOUT_R3S3_V02_5MIN",    300.0, 3, "11:30"),   # between windows
    _fill("IN2_CAM_BREAKOUT_R3S3_V02_5MIN",     20.0, 3, "12:00"),   # 2nd window start
    _fill("LATE_CAM_BREAKOUT_R3S3_V02_5MIN",   400.0, 3, "15:55"),   # 2nd window end
]
IN_WINDOW = {"IN1_CAM_BREAKOUT_R3S3_V02_5MIN", "IN1B_CAM_BREAKOUT_R3S3_V02_5MIN",
             "IN2_CAM_BREAKOUT_R3S3_V02_5MIN"}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(kairos, "_shared_hours_windows", lambda key: WINDOWS)
    monkeypatch.setattr(kairos, "_pair_alpaca_fills_lifo",
                        lambda fills, **kw: {"closed": list(fills),
                                             "closed_clean": list(fills),
                                             "orphans": [], "deduped": list(fills),
                                             "signal_lookup": {}})
    monkeypatch.setattr(kairos, "_alpaca_account_ctx",
                        lambda a: (object(), f"alpaca{a}", f"Book {a}", lambda: list(TRADES)))
    for cache in kairos._alpaca_analysis_caches.values():
        cache.clear()
    kairos.app.config["TESTING"] = True
    return kairos.app.test_client()


def _strats(client, qs):
    d = client.get("/api/alpaca/analysis?account=1" + qs).get_json() or {}
    return d, set(d.get("per_strategy") or {})


def test_unfiltered_keeps_every_trade(client):
    d, names = _strats(client, "")
    assert len(names) == len(TRADES)
    assert d["hours_applied"] == "all"


def test_curated_keeps_only_entries_inside_a_window(client):
    _, names = _strats(client, "&hours=curated")
    assert names == IN_WINDOW


def test_windows_are_half_open_so_the_end_minute_is_excluded(client):
    """[start, end) matches the live gate. An inclusive end would let in a trade the
    book would have refused."""
    _, names = _strats(client, "&hours=curated")
    assert "IN1B_CAM_BREAKOUT_R3S3_V02_5MIN" in names, "09:59 is inside 09:35-10:00"
    assert "GAP_CAM_BREAKOUT_R3S3_V02_5MIN" not in names, "10:00 is the exclusive end"


def test_the_gap_between_windows_is_excluded(client):
    """The reason from_time/to_time cannot do this job."""
    _, names = _strats(client, "&hours=curated")
    assert "MID_CAM_BREAKOUT_R3S3_V02_5MIN" not in names


def test_it_filters_on_ENTRY_time_not_exit(client):
    """Every fixture exits at 15:59, outside both windows. Filtering on exit would
    return nothing; the gate decides at entry."""
    _, names = _strats(client, "&hours=curated")
    assert names, "filtered on exit time — nothing survived"


def test_the_big_out_of_window_winners_are_what_gets_removed(client):
    """The whole point: farm P&L earned at 07:10 or 11:30 is unreachable."""
    d_all, _ = _strats(client, "")
    d_cur, _ = _strats(client, "&hours=curated")
    assert d_all["overall"]["total_pnl"] > d_cur["overall"]["total_pnl"]
    assert d_cur["overall"]["total_pnl"] == pytest.approx(41.0)


def test_the_response_names_the_windows_it_used(client):
    """A filter the user cannot inspect is one they must take on trust."""
    d, _ = _strats(client, "&hours=curated")
    assert d["hours_applied"] == "curated"
    assert d["hours_windows"] == ["09:35-10:00", "12:00-15:55"]


def test_no_configured_windows_filters_nothing_and_says_so(client, monkeypatch):
    """Empty windows mean "all day" to the gate; the chart must agree rather than
    silently blanking."""
    monkeypatch.setattr(kairos, "_shared_hours_windows", lambda key: [])
    for cache in kairos._alpaca_analysis_caches.values():
        cache.clear()
    d, names = _strats(client, "&hours=curated")
    assert len(names) == len(TRADES)
    assert d["hours_applied"] == "all" and d["hours_windows"] == []


def test_filtered_and_unfiltered_results_are_cached_separately(client):
    """Sharing a cache key would serve whichever ran first to both."""
    _, plain = _strats(client, "")
    _, cur   = _strats(client, "&hours=curated")
    _, again = _strats(client, "")
    assert plain == again and cur != plain


def test_the_filter_and_the_live_gate_share_one_window_test():
    """If these ever diverge, the chart starts promising trades the book refuses."""
    import inspect
    src = inspect.getsource(kairos.api_alpaca_analysis)
    assert "_hhmm_in_windows" in src
    assert '_shared_hours_windows("refined")' in src


# ── UI ──────────────────────────────────────────────────────────────────────────

def _index():
    return open("templates/index.html", encoding="utf-8").read()


def test_the_toggle_refetches_rather_than_filtering_points_on_screen():
    """The windows are multi-range and live in settings, so only the server knows
    them. Filtering the drawn points would need the rules duplicated in JS."""
    html = _index()
    i = html.index("function toggleHoursWindow")
    block = html[i:i + 900]
    assert "renderAlpacaStats(" in block
    assert "params.set('hours', 'curated')" in html


def test_the_toggle_survives_a_reload():
    html = _index()
    assert "localStorage.getItem('alpaca_hours_window')" in html
    assert "localStorage.setItem('alpaca_hours_window'" in html


def test_the_button_names_the_real_windows_once_known():
    html = _index()
    i = html.index("function _syncHoursButton")
    block = html[i:i + 1200]
    assert "windows.join(', ')" in block
    assert "hours_windows" in html, "server-reported windows must reach the button"


def test_the_button_says_when_it_is_filtering_nothing():
    """Lit but inert is the one state that would quietly mislead."""
    html = _index()
    i = html.index("function _syncHoursButton")
    block = html[i:i + 1200]
    assert "No curated windows are configured" in block


def test_signals_and_ib_tabs_skip_the_server_filter():
    """Neither is Alpaca fills, so hours=curated does not apply to them."""
    html = _index()
    i = html.index("function toggleHoursWindow")
    block = html[i:i + 900]
    assert "activeTab === 'signals'" in block and "activeTab === 'ib'" in block
