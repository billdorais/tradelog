"""Short re-fade cap — a tighter strike limit for SHORT levels on curated books.

The reconciliation showed the first short of a setup is ~breakeven while the
same-day re-entries carry the bleed. STRIKES_PER_LEVEL_SHORT makes a SHORT level
(S3/S4) go cold after that many losing shorts on the CURATED books (profit_lock
on: TV Refined / Kairos Refined / Crew Paper), capping the re-fades. Longs
(R-levels) and both farms keep the normal STRIKES_PER_LEVEL. 0 = off.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import shutil
import sqlite3

import pytest

import app as a


@pytest.fixture()
def strikes(monkeypatch):
    monkeypatch.setattr(a, "STRIKES_PER_LEVEL", 3)
    yield


def test_helper_curated_short_uses_tighter_limit(strikes, monkeypatch):
    monkeypatch.setattr(a, "STRIKES_PER_LEVEL_SHORT", 1)
    # Curated books, SHORT level → tight limit.
    for tag in ("alpaca2", "alpaca3", "alpaca4"):
        assert a._strike_limit("S4", tag) == 1
        assert a._strike_limit("S3", tag) == 1
    # Curated LONG level → normal.
    assert a._strike_limit("R4", "alpaca2") == 3
    # Farms (short or long) → normal, so they keep sampling.
    assert a._strike_limit("S4", "alpaca") == 3
    assert a._strike_limit("S4", "alpaca5") == 3


def test_helper_off_by_default(strikes, monkeypatch):
    monkeypatch.setattr(a, "STRIKES_PER_LEVEL_SHORT", 0)
    assert a._strike_limit("S4", "alpaca2") == 3   # 0 = use the normal limit


@pytest.mark.parametrize("short_cap,cur_losses,expect_blocked", [
    (1, 1, True),    # curated short cold after 1 loss
    (1, 0, False),   # first short still allowed
    (0, 1, False),   # cap off → 1 loss is under the normal 3-strike limit
])
def test_webhook_short_cap(monkeypatch, tmp_path, short_cap, cur_losses, expect_blocked):
    import routes.webhook as w  # noqa: F401 (imported for the blueprint)
    monkeypatch.setattr(a, "STRIKES_ENABLED", True)
    monkeypatch.setattr(a, "STRIKES_PER_LEVEL", 3)
    monkeypatch.setattr(a, "STRIKES_PER_LEVEL_SHORT", short_cap)
    # One losing SHORT round-trip today on TV Refined at S4 on RFCAP → strike count.
    monkeypatch.setattr(a, "_get_strike_counts",
                        lambda: {("alpaca2", "RFCAP", "S4"): cur_losses})
    # Route RFCAP shorts to TV Refined (alpaca-paper-2).
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS",
                        [{"tag": "alpaca2", "num": "2", "target_paper": "alpaca-paper-2",
                          "target_live": "alpaca-live-2"}])
    db = tmp_path / "rf.db"; shutil.copy("trades.db", db)
    conn = sqlite3.connect(db); conn.execute("DELETE FROM routing_rules")
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                 ("RFCAP short -> Refined", json.dumps([
                     {"type": "strategy", "value": "RFCAP_CAM_BREAKOUT_R4S4_V02_5MIN"},
                     {"type": "broker",   "value": "alpaca-paper-2"}])))
    conn.commit(); conn.close()

    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c
    monkeypatch.setattr(a, "get_db", _fake_db)
    monkeypatch.setattr(a, "_account_hours_ok", lambda *ar, **kw: True)
    with a.app.test_client() as cl:
        r = cl.post("/webhook?token=test-token",
                    json={"strategy": "RFCAP_CAM_BREAKOUT_R4S4_V02_5MIN",
                          "ticker": "RFCAP", "action": "SELL"})       # SELL = short entry
        body = r.get_json()

    if expect_blocked:
        assert body.get("reason") == "strikes_limit"
        assert body.get("limit") == 1
    else:
        assert body.get("reason") != "strikes_limit"


def test_webhook_long_unaffected_by_short_cap(monkeypatch, tmp_path):
    monkeypatch.setattr(a, "STRIKES_ENABLED", True)
    monkeypatch.setattr(a, "STRIKES_PER_LEVEL", 3)
    monkeypatch.setattr(a, "STRIKES_PER_LEVEL_SHORT", 1)
    # One losing LONG at R4 → under the normal 3-strike limit, so a long entry passes.
    monkeypatch.setattr(a, "_get_strike_counts", lambda: {("alpaca2", "RFCAP", "R4"): 1})
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS",
                        [{"tag": "alpaca2", "num": "2", "target_paper": "alpaca-paper-2",
                          "target_live": "alpaca-live-2"}])
    db = tmp_path / "rf2.db"; shutil.copy("trades.db", db)
    conn = sqlite3.connect(db); conn.execute("DELETE FROM routing_rules")
    conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                 ("RFCAP long -> Refined", json.dumps([
                     {"type": "strategy", "value": "RFCAP_CAM_BREAKOUT_R4S4_V02_5MIN"},
                     {"type": "broker",   "value": "alpaca-paper-2"}])))
    conn.commit(); conn.close()

    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c
    monkeypatch.setattr(a, "get_db", _fake_db)
    monkeypatch.setattr(a, "_account_hours_ok", lambda *ar, **kw: True)
    with a.app.test_client() as cl:
        r = cl.post("/webhook?token=test-token",
                    json={"strategy": "RFCAP_CAM_BREAKOUT_R4S4_V02_5MIN",
                          "ticker": "RFCAP", "action": "BUY"})        # BUY = long entry
        body = r.get_json()
    assert body.get("reason") != "strikes_limit"    # long at R4, 1 loss < 3
