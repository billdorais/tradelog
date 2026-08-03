"""Curated-hours reachability split on the farm leaderboards.

The farms trade all day by design (full-sample audition pools); the curated books
only trade inside the shared "refined" windows. Once those windows are narrow, a
farm strategy can rank into a promotion slot on P&L earned at times the curated
books can never take. /api/alpaca/analysis therefore splits each strategy's
round-trips by whether the ENTRY time-of-day was reachable, and the crew's farm
leaderboards rank on the takeable column.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a


WINS = [("09:35", "10:00"), ("12:00", "15:55")]


def test_hhmm_in_windows_matches_the_live_gate():
    assert a._hhmm_in_windows("09:45", WINS) is True     # inside window 1
    assert a._hhmm_in_windows("09:31", WINS) is False    # before the open window
    assert a._hhmm_in_windows("11:00", WINS) is False    # the midday pause
    assert a._hhmm_in_windows("13:00", WINS) is True     # inside window 2
    assert a._hhmm_in_windows("15:58", WINS) is False    # after the close window
    assert a._hhmm_in_windows("10:00", WINS) is False    # end is exclusive
    assert a._hhmm_in_windows("09:35", WINS) is True     # start is inclusive
    assert a._hhmm_in_windows("03:00", []) is True       # no windows = all day


def test_wrap_past_midnight():
    overnight = [("22:00", "02:00")]
    assert a._hhmm_in_windows("23:30", overnight) is True
    assert a._hhmm_in_windows("01:00", overnight) is True
    assert a._hhmm_in_windows("12:00", overnight) is False


def test_crew_farm_block_ranks_on_takeable_pnl():
    """The regression this guards: a farm name whose edge is entirely outside the
    curated windows must not out-rank a name whose edge is takeable."""
    from routes.crew import _fmt_strategies

    data = {
        "overall": {"trades": 20, "win_rate": 55.0, "total_pnl": 900.0},
        "hours_reach": {
            "active": True,
            "windows": [{"start": s, "end": e} for s, e in WINS],
            "in_trades": 8, "out_trades": 12, "in_pnl": 150.0, "out_pnl": 750.0,
        },
        "per_strategy": {
            # Big headline P&L, but nearly all of it earned outside the windows.
            "MIRAGE_CAM_BREAKOUT_R3S3": {
                "trades": 12, "win_rate": 66.0, "total_pnl": 700.0,
                "in_hours_pnl": 20.0,   "in_hours_trades": 2,
                "out_hours_pnl": 680.0, "out_hours_trades": 10,
            },
            # Smaller headline, but the edge is reachable.
            "REAL_CAM_BREAKOUT_R4S4": {
                "trades": 8, "win_rate": 50.0, "total_pnl": 200.0,
                "in_hours_pnl": 130.0, "in_hours_trades": 6,
                "out_hours_pnl": 70.0, "out_hours_trades": 2,
            },
        },
    }
    body = _fmt_strategies(data, header="TV FARM (account 1)", show_reach=True)
    assert "CURATED-HOURS REACHABILITY" in body
    assert "09:35-10:00, 12:00-15:55" in body
    # The takeable name must be listed FIRST despite the lower headline P&L.
    assert body.index("REAL_CAM_BREAKOUT_R4S4") < body.index("MIRAGE_CAM_BREAKOUT_R3S3"),         "ranked on headline P&L instead of takeable P&L"
    assert "TAKEABLE $+130.00" in body
    assert "outside $+680.00" in body


def test_curated_blocks_are_unchanged_without_show_reach():
    """Refined/Crew leaderboards are reachable by definition — no reach noise, and
    they keep ranking on headline P&L."""
    from routes.crew import _fmt_strategies

    data = {
        "overall": {"trades": 2, "win_rate": 50.0, "total_pnl": 900.0},
        "hours_reach": {"active": True, "windows": [{"start": "09:35", "end": "10:00"}],
                        "in_trades": 1, "out_trades": 1, "in_pnl": 20.0, "out_pnl": 680.0},
        "per_strategy": {
            "BIG":   {"trades": 1, "win_rate": 100.0, "total_pnl": 700.0, "in_hours_pnl": 20.0},
            "SMALL": {"trades": 1, "win_rate": 0.0,   "total_pnl": 200.0, "in_hours_pnl": 130.0},
        },
    }
    body = _fmt_strategies(data, header="TV REFINED (account 2)")
    assert "CURATED-HOURS REACHABILITY" not in body
    assert "TAKEABLE" not in body
    assert body.index("BIG") < body.index("SMALL")


def test_reach_section_hidden_when_no_windows_configured():
    """All-day curated books (windows unset) make the split meaningless."""
    from routes.crew import _fmt_strategies

    data = {
        "overall": {"trades": 1, "total_pnl": 10.0, "win_rate": 100.0},
        "hours_reach": {"active": False, "windows": [], "in_trades": 0,
                        "out_trades": 0, "in_pnl": 0, "out_pnl": 0},
        "per_strategy": {"X": {"trades": 1, "win_rate": 100.0, "total_pnl": 10.0}},
    }
    body = _fmt_strategies(data, header="TV FARM (account 1)", show_reach=True)
    assert "CURATED-HOURS REACHABILITY" not in body
