"""TV farm — fan every TV entry to the TV_PILOT_ALL account (Paper All).

The TV twin of ENGINE_PILOT_ALL. Most rules have no broker node, so Paper All
gets them via the payload-broker fallback; but the Refined top-N rules carry an
alpaca-paper-2 broker node, which suppresses that fallback and routes only to
Refined. This makes Paper All miss exactly the (daily-rotating) top-N. The farm
adds Paper All to every TV entry regardless of the rule's broker nodes.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest


@pytest.fixture()
def farm_app():
    import app as a
    import routes.webhook as wh
    saved = (a.TV_PILOT_ALL, dict(a.ACCOUNTS_BY_TAG), list(a.ALPACA_ACCOUNTS))
    a.TV_PILOT_ALL = "alpaca:10"
    a.ACCOUNTS_BY_TAG = {
        "alpaca":  {"tag": "alpaca",  "target_paper": "alpaca-paper"},
        "alpaca2": {"tag": "alpaca2", "target_paper": "alpaca-paper-2"},
    }
    a.ALPACA_ACCOUNTS = [
        {"tag": "alpaca",  "target_paper": "alpaca-paper",   "target_live": "alpaca-live"},
        {"tag": "alpaca2", "target_paper": "alpaca-paper-2", "target_live": "alpaca-live-2"},
    ]
    yield a, wh
    a.TV_PILOT_ALL, a.ACCOUNTS_BY_TAG, a.ALPACA_ACCOUNTS = saved


def _tags(targets):
    return [t[0] for t in targets]


def test_topn_rule_adds_paper_all(farm_app):
    a, wh = farm_app
    # Rule routes ONLY to Refined (alpaca-paper-2) — the farm must add Paper All.
    rs = {"quantity": 76, "entry_source_kairos": False, "ep_trail_offset": 0.35}
    alpaca_targets = [("alpaca-paper-2", 76, rs)]
    added = wh._add_tv_pilot_targets(alpaca_targets, list(alpaca_targets), [], is_exit=False)
    assert added == [("alpaca", 10)]
    assert _tags(alpaca_targets) == ["alpaca-paper-2", "alpaca-paper"]
    # farm target carries flat shares and never suppresses the TV entry
    assert alpaca_targets[-1][1] == 10
    assert alpaca_targets[-1][2]["entry_source_kairos"] is False


def test_no_double_when_fallback_already_paper_all(farm_app):
    a, wh = farm_app
    # Non-top-N rule: the payload-broker fallback already put Paper All there.
    base = {"quantity": 10, "entry_source_kairos": False}
    alpaca_targets = [("alpaca-paper", None, base)]
    added = wh._add_tv_pilot_targets(alpaca_targets, list(alpaca_targets), [dict(base)], is_exit=False)
    assert added == []
    assert _tags(alpaca_targets) == ["alpaca-paper"]


def test_exit_never_adds_farm(farm_app):
    a, wh = farm_app
    rs = {"quantity": 76, "entry_source_kairos": False}
    alpaca_targets = [("alpaca-paper-2", 76, rs)]
    added = wh._add_tv_pilot_targets(alpaca_targets, list(alpaca_targets), [], is_exit=True)
    assert added == []
    assert _tags(alpaca_targets) == ["alpaca-paper-2"]


def test_disabled_is_noop(farm_app):
    a, wh = farm_app
    a.TV_PILOT_ALL = ""
    rs = {"quantity": 76, "entry_source_kairos": False}
    alpaca_targets = [("alpaca-paper-2", 76, rs)]
    added = wh._add_tv_pilot_targets(alpaca_targets, list(alpaca_targets), [], is_exit=False)
    assert added == []
    assert _tags(alpaca_targets) == ["alpaca-paper-2"]


def test_fully_suppressed_signal_adds_nothing(farm_app):
    a, wh = farm_app
    # No surviving broker_targets (a fully kairos-suppressed entry) → no farm add.
    added = wh._add_tv_pilot_targets([], [], [], is_exit=False)
    assert added == []
