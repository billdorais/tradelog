"""Manual-close log: Close buttons record the close; ORB closes are judged against the bracket."""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import sqlite3
from datetime import datetime, timezone

import pytest

import app as a
from routes import manual_closes as M
from routes import orb_live as L


class _Broker:
    def __init__(self, positions):
        self.positions, self.closed = positions, []

    def get_positions(self, raise_on_error=False):
        return list(self.positions)

    def close_position(self, sym):
        self.closed.append(sym)
        return {"success": True, "order_id": "x"}


@pytest.fixture
def db(monkeypatch, tmp_path):
    path = str(tmp_path / "mc.db")
    monkeypatch.setattr(a, "get_db", lambda: sqlite3.connect(path))
    monkeypatch.setattr(a, "placeholder", lambda: "?")
    M._ready["ok"] = False
    L.ensure_table()
    return path


def _pos(sym="GOOG", side="short", qty=28, entry=279.54, px=278.30, upnl=34.71):
    return {"symbol": sym, "side": side, "qty": qty, "avg_entry_price": entry,
            "current_price": px, "unrealized_pnl": upnl}


def test_close_button_logs_the_close_and_the_reason_can_be_added_after(db, monkeypatch):
    br = _Broker([_pos()])
    monkeypatch.setattr(a, "_alpaca_account_ctx", lambda acct: (br, "alpaca8", "ORB Paper", None))
    c = a.app.test_client()
    d = c.post("/api/alpaca/close/goog?account=8&token=test-token").get_json()
    assert d["success"] and br.closed == ["GOOG"] and d["manual_close_id"]
    assert c.post(f"/api/manual_closes/{d['manual_close_id']}/reason",
                  json={"reason": "at R3, expected a reversal"}).status_code == 401
    assert c.post(f"/api/manual_closes/{d['manual_close_id']}/reason?token=test-token",
                  json={"reason": "at R3, expected a reversal"}).get_json() == {"ok": True}
    row = M.logged()[0]
    assert (row["account"], row["symbol"], row["side"], row["qty"], row["close_price"], row["unrealized_pnl"],
            row["reason"], row["source"]) == ("8", "GOOG", "short", 28.0, 278.30, 34.71,
                                              "at R3, expected a reversal", "dashboard")


def test_a_failed_snapshot_or_log_never_blocks_the_close(db, monkeypatch):
    class Flaky(_Broker):
        def get_positions(self, raise_on_error=False):
            raise RuntimeError("broker down")
    br = Flaky([])
    monkeypatch.setattr(a, "_alpaca_account_ctx", lambda acct: (br, "alpaca8", "ORB Paper", None))
    monkeypatch.setattr(a, "get_db", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    d = a.app.test_client().post("/api/alpaca/close/QQQ?account=8&token=test-token").get_json()
    assert d["success"] and br.closed == ["QQQ"] and d["manual_close_id"] is None


def _orb(day, ticker, side, fill, qty, exit_px, reason, bt_exit, bt_reason, flatten_at=None, exit_time=None):
    L.upsert(day, ticker, book="8", status="exited", side=side, fill_price=fill, qty=qty, exit_price=exit_px,
             exit_reason=reason, bt_exit=bt_exit, bt_reason=bt_reason, flatten_at=flatten_at,
             exit_time=exit_time or f"{day}T16:04:14+00:00", order_id="o")


def test_review_finds_orb_closes_outside_the_dashboard_and_scores_them_against_the_bracket(db):
    # Closed in Alpaca's app: a market exit, not the stop/target, no 15:55 flatten.
    _orb("2026-11-18", "QQQ", "short", 659.89, 8, 654.85, "time", 662.27, "stop")
    # Closed with the dashboard button: logged, and matched to its ORB trade.
    M.record("8", "alpaca8", "NVDA", _pos("NVDA", "short", 9, 158.11, 153.57, 40.89),
             reason="level", now=datetime(2026, 11, 18, 14, 51, 45, tzinfo=timezone.utc))
    _orb("2026-11-18", "NVDA", "short", 158.11, 9, 153.57, "time", 151.48, "target")
    # Bracket exits and the 15:55 flatten are not manual.
    _orb("2026-11-18", "SPCX", "long", 143.63, 12, 142.115, "stop", 142.21, "stop")
    _orb("2026-11-16", "MU", "short", 700.0, 1, 690.0, "time", 690.0, "time",
         flatten_at="2026-11-16T20:55:02+00:00")
    out = M.review(day_from="2026-11-16")
    by = {r["symbol"]: r for r in out["closes"]}
    assert set(by) == {"QQQ", "NVDA"}
    assert by["QQQ"]["source"] == "outside dashboard" and by["NVDA"]["source"] == "dashboard"
    assert by["QQQ"]["actual_pnl"] == 40.32 and by["QQQ"]["bracket_pnl"] == -19.04
    assert by["NVDA"]["reason"] == "level" and by["NVDA"]["vs_bracket"] == round(40.86 - 59.67, 2)
    s = out["summary"]
    assert (s["closes"], s["compared"], s["better"], s["worse"]) == (2, 2, 1, 1)


def test_review_filters_by_account_and_date(db):
    M.record("6", "alpaca6", "PLTR", _pos("PLTR", "long", 10, 140, 141, 10),
             now=datetime(2026, 11, 21, 2, 0, tzinfo=timezone.utc))      # 9 PM ET on the 20th
    assert [r["day"] for r in M.review(day_from="2026-11-20", day_to="2026-11-20")["closes"]] == ["2026-11-20"]
    assert M.review(account="8")["closes"] == []
