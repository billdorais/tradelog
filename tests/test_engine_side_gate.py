"""Engine-side enforcement of the routing `side_gate` node.

A side_gate node on a rule must apply to EVERY engine source — not just
entry_source=kairos rules (Source 2), but also the ENGINE_PILOT_ALL "all
pipelines" source (Source 3) that drives Paper All. Regression guard for the
gap where Paper All fired both sides of a side-gated reversal.
"""
from __future__ import annotations

import os

# Match the webhook suite's import-time env so importing `app` here first doesn't
# cache a missing WEBHOOK_TOKEN and break order-dependent webhook tests.
os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import shutil
import sqlite3

import pytest


@pytest.fixture()
def engine_env(tmp_path):
    import app as a

    # Temp DB seeded with the routing_rules schema from the repo trades.db.
    tmp = tmp_path / "engine.db"
    shutil.copy("trades.db", tmp)
    conn = sqlite3.connect(tmp)
    conn.execute("DELETE FROM routing_rules")
    conn.execute(
        "INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
        ("AMZN rev R4S4 (short-gated)", json.dumps([
            {"type": "strategy",  "value": "AMZN_CAM_REVERSAL_R4S4_V02_5MIN"},
            {"type": "side_gate", "value": "short"},
        ])),
    )
    conn.execute(
        "INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
        ("GLD rev R4S4 (ungated)", json.dumps([
            {"type": "strategy", "value": "GLD_CAM_REVERSAL_R4S4_V02_5MIN"},
        ])),
    )
    conn.execute(
        "INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
        ("SPCX brk R3S3 (non-shortable)", json.dumps([
            {"type": "strategy", "value": "SPCX_CAM_BREAKOUT_R3S3_V02_5MIN"},
        ])),
    )
    conn.commit()
    conn.close()

    # Save + monkeypatch the module globals the engine reads.
    saved = (a.get_db, a._refined_last_result, a.ACCOUNTS_BY_TAG,
             a.ENGINE_PILOT_ALL, a.ENGINE_PILOT_EXTRA)

    def _fake_db():
        c = sqlite3.connect(tmp)
        c.row_factory = sqlite3.Row
        return c

    a.get_db = _fake_db
    a._refined_last_result = {}
    a.ACCOUNTS_BY_TAG = {"alpaca": {"tag": "alpaca"}}   # let the Source-3 parser accept 'alpaca'
    a.ENGINE_PILOT_ALL = "alpaca:$1000"                  # Paper All fires ALL pipelines (Source 3)
    a.ENGINE_PILOT_EXTRA = ""
    yield a
    (a.get_db, a._refined_last_result, a.ACCOUNTS_BY_TAG,
     a.ENGINE_PILOT_ALL, a.ENGINE_PILOT_EXTRA) = saved


def _sides(setups, strategy):
    return sorted({s["side"] for s in setups if s["strategy"] == strategy})


def test_side_gate_suppresses_gated_side_on_all_pipelines(engine_env):
    setups = engine_env._entry_engine_setups()
    # The short-gated reversal must produce ONLY short setups (Source 3 / Paper All).
    assert _sides(setups, "AMZN_CAM_REVERSAL_R4S4_V02_5MIN") == ["SHORT"]


def test_ungated_strategy_keeps_both_sides(engine_env):
    setups = engine_env._entry_engine_setups()
    assert _sides(setups, "GLD_CAM_REVERSAL_R4S4_V02_5MIN") == ["LONG", "SHORT"]


def test_non_shortable_ticker_is_long_only(engine_env):
    # SPCX can't be sold short at the broker → the engine must not arm its short.
    setups = engine_env._entry_engine_setups()
    assert _sides(setups, "SPCX_CAM_BREAKOUT_R3S3_V02_5MIN") == ["LONG"]
