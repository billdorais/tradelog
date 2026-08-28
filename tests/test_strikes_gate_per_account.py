"""The strikes gate counts per account, so it must enforce per account.

Counts and limits were already per-book (_strike_limit takes an account tag), but
enforcement returned outright the moment ANY account was over — blocking every
other book for losses it had not taken. On 2026-08-27 that read in the log as
"blocked — 2+ losses at R3 today on alpaca2", which names only the account that
TRIPPED the gate and hides that the block applied everywhere.

It also silently gated the farms. Their whole job is to be the ungated control
group that makes a curated book's gate block priceable — a farm blocked by another
account's losses is a missing trade with no record of why.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3

import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a

# Alpaca orders are serialized on per-(broker, ticker) worker threads that outlive
# a test, so every test gets its own ticker. Sharing one made a late task from the
# previous test land in this one's results.
_TICK = [0]

placed = []


class _Broker:
    def __init__(self, tag):
        self.tag = tag

    def _get_positions_cached(self):
        return []

    def _invalidate_pos_cache(self):
        pass

    def place_order(self, **kw):
        placed.append((self.tag, kw["action"], kw["ticker"]))
        return {"success": True, "order_id": f"o{len(placed)}", "status": "accepted"}


@pytest.fixture()
def wh(tmp_path, monkeypatch):
    placed.clear()
    a._pending_entries.clear()
    _TICK[0] += 1
    ticker = f"GLDT{_TICK[0]}"
    strat  = f"{ticker}_CAM_BREAKOUT_R3S3_V02_5MIN"

    # All paper: the live-entry guard fires before this gate and would mask it.
    accounts = [
        {"tag": "alpaca2", "num": "2", "target_paper": "alpaca-paper-2",
         "target_live": "alpaca-live-2", "broker": _Broker("alpaca2"),
         "label": "TV Refined", "paper": True},
        {"tag": "alpaca4", "num": "4", "target_paper": "alpaca-paper-4",
         "target_live": "alpaca-live-4", "broker": _Broker("alpaca4"),
         "label": "Crew Paper", "paper": True},
        {"tag": "alpaca", "num": "1", "target_paper": "alpaca",
         "target_live": "alpaca", "broker": _Broker("alpaca"),
         "label": "TV Farm", "paper": True},
    ]
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", accounts)
    monkeypatch.setattr(a, "ACCOUNTS_BY_TAG", {x["tag"]: x for x in accounts})
    monkeypatch.setattr(a, "STRIKES_ENABLED", True)
    monkeypatch.setattr(a, "STRIKES_PER_LEVEL", 2)
    monkeypatch.setattr(a, "_account_hours_ok", lambda tag: True)
    monkeypatch.setattr(a, "_trade_level", lambda strategy, side: "R3")
    monkeypatch.setattr(a, "_strike_limit", lambda level, tag: 2)

    db = tmp_path / "strikes.db"
    shutil.copy("trades.db", db)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM routing_rules")
    for name, target in (("refined", "alpaca-paper-2"), ("crew", "alpaca-paper-4"),
                         ("farm", "alpaca")):
        conn.execute("INSERT INTO routing_rules (name, enabled, nodes) VALUES (?,1,?)",
                     (f"{strat} -> {name}", json.dumps([
                         {"type": "strategy", "value": strat},
                         {"type": "broker",   "value": target}])))
    conn.commit(); conn.close()

    saved = a.get_db

    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row
        return c

    a.get_db = _fake_db
    yield a.app.test_client(), ticker, strat
    a.get_db = saved


def _drain(timeout=6.0):
    """Alpaca orders are handed to per-(broker,ticker) worker threads, so the POST
    returns before any order is placed."""
    import time
    from routes import webhook as _wh
    t0 = time.time()
    while time.time() - t0 < timeout:
        qs = list(_wh._alpaca_ticker_queues.values())
        if all(q.empty() for q in qs):
            time.sleep(0.2)
            if all(q.empty() for q in list(_wh._alpaca_ticker_queues.values())):
                return
        time.sleep(0.05)


def _fire(wh):
    client, ticker, strat = wh
    r = client.post("/webhook?token=test-token",
                    json={"strategy": strat, "ticker": ticker, "action": "BUY",
                          "sentiment": "bullish", "quantity": 2, "price": 100.0})
    _drain()
    return r


def _tags():
    return sorted({t for t, _, _ in placed})


def test_nobody_over_the_limit_routes_everywhere(wh, monkeypatch):
    monkeypatch.setattr(a, "_get_strike_counts", lambda: {})
    _fire(wh)
    assert _tags() == ["alpaca", "alpaca2", "alpaca4"]


def test_one_book_over_its_limit_does_not_block_the_others(wh, monkeypatch):
    """The bug: alpaca2's two losses used to stop Crew Paper and the farm too."""
    monkeypatch.setattr(a, "_get_strike_counts",
                        lambda: {("alpaca2", wh[1], "R3"): 2})
    _fire(wh)
    assert "alpaca2" not in _tags(), "the account over its limit still traded"
    assert _tags() == ["alpaca", "alpaca4"], "other books were blocked by alpaca2's losses"


def test_the_ungated_farm_keeps_trading(wh, monkeypatch):
    """A farm blocked by a curated book's losses is a hole in the control group."""
    monkeypatch.setattr(a, "_get_strike_counts",
                        lambda: {("alpaca2", wh[1], "R3"): 5,
                                 ("alpaca4", wh[1], "R3"): 5})
    _fire(wh)
    assert _tags() == ["alpaca"]


def test_every_book_over_the_limit_still_blocks_the_signal(wh, monkeypatch):
    monkeypatch.setattr(a, "_get_strike_counts",
                        lambda: {("alpaca2", wh[1], "R3"): 2,
                                 ("alpaca4", wh[1], "R3"): 2,
                                 ("alpaca",  wh[1], "R3"): 2})
    r = _fire(wh)
    assert placed == []
    assert (r.get_json() or {}).get("reason") == "strikes_limit"


def test_a_book_under_its_own_limit_is_unaffected_by_a_higher_count_elsewhere(wh, monkeypatch):
    monkeypatch.setattr(a, "_get_strike_counts",
                        lambda: {("alpaca2", wh[1], "R3"): 99,
                                 ("alpaca4", wh[1], "R3"): 1})
    _fire(wh)
    assert "alpaca4" in _tags()


def test_per_account_limits_are_honoured_individually(wh, monkeypatch):
    """alpaca2 tighter (1), alpaca4 looser (3): one count trips only the tighter."""
    monkeypatch.setattr(a, "_strike_limit",
                        lambda level, tag: 1 if tag == "alpaca2" else 3)
    monkeypatch.setattr(a, "_get_strike_counts",
                        lambda: {("alpaca2", wh[1], "R3"): 1,
                                 ("alpaca4", wh[1], "R3"): 1})
    _fire(wh)
    assert "alpaca2" not in _tags() and "alpaca4" in _tags()


def test_each_dropped_book_is_recorded_against_its_own_account(wh, monkeypatch):
    """So a gate that stops one book while another trades stays attributable."""
    seen = []
    monkeypatch.setattr(a, "_record_block",
                        lambda account, ticker, strategy, side, gate, reason, **kw:
                            seen.append((account, gate)))
    monkeypatch.setattr(a, "_get_strike_counts",
                        lambda: {("alpaca2", wh[1], "R3"): 2})
    _fire(wh)
    assert ("alpaca2", "strikes") in seen
    assert not any(acct in ("alpaca4", "alpaca") for acct, _ in seen), \
        "recorded a block against a book that still traded"


def test_the_log_names_who_was_blocked_and_who_kept_trading(wh):
    """The old message named only the tripping account and read as a global block."""
    src = open("routes/webhook.py", encoding="utf-8").read()
    i = src.index("Strikes gate: %s %s (%s) blocked on %s")
    assert "still routing to" in src[i:i + 600]
