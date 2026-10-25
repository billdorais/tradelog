"""Monthly book report: Claude interprets the month-by-month pack."""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import json
import shutil
import sqlite3
import threading
import types

import pytest

import routes.book_report as br


# ── the API call ──────────────────────────────────────────────────────────────

class _Stream:
    def __init__(self, msg): self.msg = msg
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def get_final_message(self): return self.msg


def _msg(text="## Summary\n- ok", stop="end_turn"):
    return types.SimpleNamespace(
        content=[types.SimpleNamespace(type="thinking", thinking=""),
                 types.SimpleNamespace(type="text", text=text)],
        stop_reason=stop, model=br.MODEL,
        usage=types.SimpleNamespace(input_tokens=1200, output_tokens=800))


def test_call_uses_opus_with_refusal_fallbacks_and_the_pack():
    seen = {}

    class _Beta:
        def stream(self, **kw):
            seen.update(kw); return _Stream(_msg())

    client = types.SimpleNamespace(beta=types.SimpleNamespace(messages=_Beta()))
    text, usage, stop = br.call_claude({"months": [{"month": "2026-10"}]}, client=client)
    assert text == "## Summary\n- ok" and stop == "end_turn" and usage["output_tokens"] == 800
    assert seen["model"] == "claude-opus-5-5"
    assert seen["fallbacks"] == "default" and seen["betas"] == ["server-side-fallback-2026-07-01"]
    assert seen["output_config"] == {"effort": "high"}
    assert '"2026-10"' in seen["messages"][0]["content"]
    assert "Use only numbers that are in the pack" in seen["system"]


def test_call_falls_back_to_the_plain_api_on_an_older_sdk():
    class _OldBeta:
        def stream(self, **kw):
            if "fallbacks" in kw:
                raise TypeError("unexpected keyword argument 'fallbacks'")
    plain = {}

    class _Plain:
        def stream(self, **kw):
            plain.update(kw); return _Stream(_msg("report"))

    client = types.SimpleNamespace(beta=types.SimpleNamespace(messages=_OldBeta()), messages=_Plain())
    text, _u, _s = br.call_claude({}, client=client)
    assert text == "report" and "fallbacks" not in plain


# ── a run, end to end ─────────────────────────────────────────────────────────

@pytest.fixture()
def db(tmp_path, monkeypatch):
    import app as a
    path = tmp_path / "br.db"
    shutil.copy("trades.db", path)

    def _db():
        c = sqlite3.connect(path); c.row_factory = sqlite3.Row
        return c
    monkeypatch.setattr(a, "get_db", _db)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    import routes.farms as farms
    monkeypatch.setattr(farms, "build_book_months_pack", lambda months, **kw: {"months": [], "n": months})
    return path


def _sync_threads(monkeypatch):
    """Run the background job inline so the test can read its result."""
    class _T:
        def __init__(self, target, args=(), **k): self.t, self.a = target, args
        def start(self): self.t(*self.a)
    monkeypatch.setattr(br.threading, "Thread", _T)


def test_a_run_stores_the_report(db, monkeypatch):
    import app as a
    _sync_threads(monkeypatch)
    monkeypatch.setattr(br, "call_claude", lambda pack: ("## Summary\n- fine", {"output_tokens": 5}, "end_turn"))
    c = a.app.test_client()
    r = c.post("/api/farms/book_report/run", json={"months": 3})
    assert r.status_code == 200
    d = c.get("/api/farms/book_reports").get_json()
    rep = d["reports"][0]
    assert rep["status"] == "done" and rep["report"].startswith("## Summary") and rep["months"] == 3
    assert rep["usage"] == {"output_tokens": 5}


def test_refusal_and_missing_key_are_failures_not_empty_reports(db, monkeypatch):
    import app as a
    _sync_threads(monkeypatch)
    monkeypatch.setattr(br, "call_claude", lambda pack: ("", {}, "refusal"))
    c = a.app.test_client()
    c.post("/api/farms/book_report/run", json={})
    assert "refusal" in c.get("/api/farms/book_reports").get_json()["reports"][0]["error"]
    monkeypatch.delenv("ANTHROPIC_API_KEY")
    c.post("/api/farms/book_report/run", json={})
    rep = c.get("/api/farms/book_reports").get_json()["reports"][0]
    assert rep["status"] == "failed" and "ANTHROPIC_API_KEY" in rep["error"]


def test_only_one_report_runs_at_a_time_and_stale_runs_expire(db, monkeypatch):
    import app as a
    started = []
    monkeypatch.setattr(br.threading, "Thread",
                        lambda target, args=(), **k: types.SimpleNamespace(start=lambda: started.append(args)))
    c = a.app.test_client()
    first = c.post("/api/farms/book_report/run", json={}).get_json()
    again = c.post("/api/farms/book_report/run", json={})
    assert again.status_code == 409 and again.get_json()["id"] == first["id"]
    # A run left "running" by a dead worker frees the slot after STALE_RUN_SECS.
    conn = sqlite3.connect(db)
    conn.execute("UPDATE book_reports SET created_at='2000-01-01 00:00:00' WHERE id=?", (first["id"],))
    conn.commit(); conn.close()
    rep = c.get("/api/farms/book_reports").get_json()["reports"][0]
    assert rep["status"] == "failed" and "stopped" in rep["error"]
    assert c.post("/api/farms/book_report/run", json={}).status_code == 200


def test_reports_carry_their_pack_for_copy_with_data(db, monkeypatch):
    import app as a
    _sync_threads(monkeypatch)
    monkeypatch.setattr(br, "call_claude", lambda pack: ("## Summary\n- x", {}, "end_turn"))
    c = a.app.test_client()
    c.post("/api/farms/book_report/run", json={"months": 2})
    rep = c.get("/api/farms/book_reports").get_json()["reports"][0]
    assert rep["pack"] == {"months": [], "n": 2}
    html = open("templates/farms.html", encoding="utf-8").read()
    assert "copyBookReport(${r.id}, false, this)" in html and "copyBookReport(${r.id}, true, this)" in html
