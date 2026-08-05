"""Signal block-reason categorization — _categorize_block_reason.

Buckets a signal's stored exec_status/exec_detail into the gate that stopped it,
so the Analysis panel can show why entries didn't trade.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as a


def test_categories_match_stored_reason_strings():
    cat = a._categorize_block_reason
    assert cat("ok", None) == "Executed"
    assert cat("error", "broker rejected") == "Error"
    # Exact strings the webhook writes:
    assert cat("skipped", "RVOL gate: entry below the 1.50x RVOL floor (RVOL 0.90x)") == "RVOL gate"
    assert cat("skipped", "day-type gate: breakout blocked on Inside day (Outside only)") == "Day-type gate"
    assert cat("skipped", "reversal policy off for this account") == "Reversal gate"
    assert cat("skipped", "side gate: long-only, short entry dropped") == "Side gate"
    assert cat("blocked", "regime gate: VIX > 25") == "Regime gate"
    assert cat("skipped", "something unmapped") == "Other block"


def test_day_type_precedence_over_reversal_wording():
    # A reversal blocked by the day-type gate mentions BOTH words; it's a day-type block.
    cat = a._categorize_block_reason
    assert cat("skipped", "day-type gate: reversal blocked on Outside day (Inside only)") == "Day-type gate"


def test_endpoint_shape_no_signals(monkeypatch):
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE trades (received_at TEXT, ticker TEXT, strategy TEXT, "
                 "action TEXT, sentiment TEXT, exec_status TEXT, exec_detail TEXT)")
    conn.commit()
    monkeypatch.setattr(a, "get_db", lambda: conn)
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/signals/blocked_breakdown?days=7").get_json()
    assert d["total_entries"] == 0 and d["executed"] == 0 and d["by_reason"] == []


def test_unmatched_strategies_groups_and_suggests(monkeypatch):
    import json, sqlite3, datetime
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE trades (received_at TEXT, ticker TEXT, strategy TEXT, "
                 "action TEXT, sentiment TEXT, exec_status TEXT, exec_detail TEXT)")
    conn.execute("CREATE TABLE routing_rules (id INTEGER PRIMARY KEY, enabled INTEGER, nodes TEXT)")
    today = datetime.date.today().isoformat()
    NM = "no routing pipeline matched strategy"
    # AAPL R3S3 fired twice with no rule, but a near-name rule exists (R4S4) → typo/drift.
    for _ in range(2):
        conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                     (today, "AAPL", "AAPL_CAM_BREAKOUT_R3S3_V02_5MIN", "buy", "long", "error", NM))
    # ZZZZ fired once, genuinely no rule anywhere.
    conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                 (today, "ZZZZ", "ZZZZ_CAM_BREAKOUT_R3S3_V02_5MIN", "buy", "long", "error", NM))
    # MSFT fired with a V02 alert but the rule is V01 — same ticker+kind+level → real drift.
    conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                 (today, "MSFT", "MSFT_CAM_BREAKOUT_R3S3_V02_5MIN", "buy", "long", "error", NM))
    # GOOG fired and the EXACT rule exists (but has no broker) → rule_exists, not drift.
    conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                 (today, "GOOG", "GOOG_CAM_BREAKOUT_R3S3_V02_5MIN", "buy", "long", "error", NM))
    conn.execute("INSERT INTO routing_rules VALUES (1,1,?)",
                 (json.dumps([{"type": "strategy", "value": "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"}]),))
    conn.execute("INSERT INTO routing_rules VALUES (2,1,?)",
                 (json.dumps([{"type": "strategy", "value": "MSFT_CAM_BREAKOUT_R3S3_V01_5MIN"}]),))
    conn.execute("INSERT INTO routing_rules VALUES (3,1,?)",
                 (json.dumps([{"type": "strategy", "value": "GOOG_CAM_BREAKOUT_R3S3_V02_5MIN"}]),))
    conn.commit()
    monkeypatch.setattr(a, "get_db", lambda: conn)
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/signals/unmatched_strategies?days=7").get_json()
    by = {u["strategy"]: u for u in d["unmatched"]}
    aapl = by["AAPL_CAM_BREAKOUT_R3S3_V02_5MIN"]
    assert aapl["count"] == 2 and aapl["nearest_rule"] == "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"
    assert aapl["likely_typo"] is False          # different level → distinct strategy, not a typo
    assert by["ZZZZ_CAM_BREAKOUT_R3S3_V02_5MIN"]["nearest_rule"] is None
    msft = by["MSFT_CAM_BREAKOUT_R3S3_V02_5MIN"]
    assert msft["likely_typo"] is True           # same ticker+kind+level, only version differs
    goog = by["GOOG_CAM_BREAKOUT_R3S3_V02_5MIN"]
    assert goog["rule_exists"] is True           # exact rule present — a routing (no-broker) issue
    assert goog["likely_typo"] is False and goog["nearest_rule"] is None


def test_wire_to_farm_creates_rule_then_is_idempotent(monkeypatch, tmp_path):
    import json, sqlite3
    db = str(tmp_path / "rules.db")
    _init = sqlite3.connect(db)
    _init.execute("CREATE TABLE routing_rules (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                  "name TEXT, enabled INTEGER, nodes TEXT, created_at TEXT)")
    _init.commit(); _init.close()
    def _open():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c
    monkeypatch.setattr(a, "get_db", _open)
    a.app.config["TESTING"] = True
    strat = "ZZZZ_CAM_BREAKOUT_R3S3_V02_5MIN"
    with a.app.test_client() as c:
        d1 = c.post("/api/signals/wire_to_farm", json={"strategy": strat}).get_json()
        d2 = c.post("/api/signals/wire_to_farm", json={"strategy": strat}).get_json()
    assert d1.get("created") is True and d1.get("target", "").startswith("alpaca-paper-1")
    assert d2.get("already_wired") is True          # second call is a no-op
    q = _open()
    rows = q.execute("SELECT nodes FROM routing_rules").fetchall(); q.close()
    assert len(rows) == 1
    nodes = json.loads(rows[0]["nodes"])
    assert {"type": "strategy", "value": strat} in nodes
    assert {"type": "broker", "value": "alpaca-paper-1"} in nodes


def test_recent_timestamps_are_converted_to_et(monkeypatch, tmp_path):
    """received_at is stored UTC by the webhook, but the panel column says
    "When (ET)". Shipping UTC under an ET heading makes a 15:45 in-session signal
    look like a 19:45 after-hours one — which derails any hours-gate diagnosis."""
    import sqlite3

    import app as a

    db = tmp_path / "bb_tz.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE trades (received_at TEXT, ticker TEXT, strategy TEXT, "
                 "action TEXT, sentiment TEXT, exec_status TEXT, exec_detail TEXT)")
    conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                 ("2026-08-04 19:45:00", "UBER", "UBER_CAM_BREAKOUT_R3S3_V02_5MIN",
                  "BUY", "long", "skipped",
                  "day-type gate: breakout blocked on Neutral day (Outside only)"))
    conn.commit(); conn.close()

    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c

    monkeypatch.setattr(a, "get_db", _fake_db)
    a.app.config["TESTING"] = True
    with a.app.test_client() as c:
        d = c.get("/api/signals/blocked_breakdown?days=60").get_json()
    row = d["recent"][0]
    assert row["received_at"].endswith("15:45:00"), \
        f"UTC leaked into an ET-labelled column: {row['received_at']}"
