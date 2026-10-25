"""Monthly book report: Claude reads the month-by-month pack and writes insights.

The pack (routes.farms.build_book_months_pack) computes every number; the model
only interprets it, so it can't invent a figure. One Messages API call — no tools,
no agent loop. Runs in a background thread because a long report can outlast the
web server's request timeout; the page polls for the result.

Reports are stored in their own table, not crew_reports: the crew's wiring reads
the LATEST crew report as its picks, so a report stored there would be misread.
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

book_report_bp = Blueprint("book_report", __name__)

MODEL = "claude-opus-5-5"
STALE_RUN_SECS = 20 * 60          # a "running" row older than this died with its worker

SYSTEM_PROMPT = """You are the analyst for one trader's systematic trading system. You write a short monthly report comparing two of their paper books, Kairos Refined and Kairos Select, against the Kairos Farm they are drawn from.

How the system works:
- The Kairos Farm trades every strategy pipeline at $1,000 per trade, ungated and all day. It is the full audition pool.
- Kairos Refined trades the top strategies from a daily-refreshed ranking of the farm, under entry gates (curated trading hours, day-type, RVOL, open-location, a reversal side policy, strikes), at about $15,000 per trade.
- Kairos Select trades a roster the trader picks by hand from both farms, ungated except for a profit lock and a daily-loss guard, at about $15,000 per trade.

The data pack you receive, per month:
- farm.refined_roster / farm.select_roster: the farm's trades in strategies each book held AT THE TIME of the trade (never today's roster).
- farm.both / refined_only / select_only: the overlap split.
- farm.neither: every farm trade neither book held. This is the baseline a roster has to beat to show its selection adds anything.
- actual.refined / actual.select: each book's own trades at its own size, gates and execution.
- Each group has trades, win_rate (%), profit_factor, avg_pct (average % return per trade) and pnl (dollars).

Rules:
- Use only numbers that are in the pack. Never estimate, extrapolate or invent a figure.
- Compare books on avg_pct (% per trade). Dollar pnl is not comparable between the farm ($1k) and the books (~$15k).
- Say how many trades stand behind every claim. Treat a group with fewer than 20 trades in a month as too thin to conclude from, and say so.
- The gap between a roster's farm result and its book's actual result is gates plus execution. Name it as that, not as skill.
- Be plain and direct. Short sentences, no hype. If the data doesn't support a conclusion, say "not enough data" instead of hedging around it.
- Any suggestion is a suggestion for the trader to test, never a change to make now.

Write markdown with exactly these sections:
## Summary
Three bullets: the most important things this month and across the months shown.
## Refined vs Select
Which roster did better on the farm, month by month, against the rest-of-farm baseline.
## Gates and execution
For each book, actual vs its roster on the farm, and what the gap suggests.
## Worth watching
Up to three specific things to keep an eye on or test next month.
## Caveats
Sample sizes, missing data (errors in the pack), and anything else that limits these conclusions."""


def _app():
    import app as _kairos
    return _kairos


def _ensure_table(a):
    conn = a.get_db()
    try:
        cur = conn.cursor()
        idcol = "SERIAL PRIMARY KEY" if a.DATABASE_URL else "INTEGER PRIMARY KEY AUTOINCREMENT"
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS book_reports (
                id         {idcol},
                created_at TEXT NOT NULL,
                status     TEXT NOT NULL,
                months     INTEGER,
                model      TEXT,
                pack       TEXT,
                report     TEXT,
                error      TEXT,
                usage      TEXT
            )
        """)
        conn.commit()
    finally:
        conn.close()


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _insert(a, months):
    _ensure_table(a)
    conn = a.get_db()
    try:
        cur = conn.cursor()
        p = a.placeholder()
        if a.DATABASE_URL:
            cur.execute(f"INSERT INTO book_reports (created_at, status, months, model) "
                        f"VALUES ({p},{p},{p},{p}) RETURNING id", (_now(), "running", months, MODEL))
            rid = cur.fetchone()[0]
        else:
            cur.execute(f"INSERT INTO book_reports (created_at, status, months, model) "
                        f"VALUES ({p},{p},{p},{p})", (_now(), "running", months, MODEL))
            rid = cur.lastrowid
        conn.commit()
        return rid
    finally:
        conn.close()


def _update(a, rid, **fields):
    conn = a.get_db()
    try:
        cur = conn.cursor()
        p = a.placeholder()
        cols = ", ".join(f"{k}={p}" for k in fields)
        cur.execute(f"UPDATE book_reports SET {cols} WHERE id={p}", (*fields.values(), rid))
        conn.commit()
    finally:
        conn.close()


def call_claude(pack, client=None):
    """Send the pack, return (report_markdown, usage_dict, stop_reason).

    Refusal fallbacks are on: on a safety decline the API re-runs the request on
    a fallback model inside the same call. If the installed SDK predates the
    fallbacks parameter, the plain call is used instead."""
    if client is None:
        import anthropic
        client = anthropic.Anthropic()          # ANTHROPIC_API_KEY from the environment
    user = ("Here is the month-by-month data pack (JSON). Write the report.\n\n"
            + json.dumps(pack, indent=1, sort_keys=True))
    kwargs = dict(model=MODEL, max_tokens=32000, system=SYSTEM_PROMPT,
                  output_config={"effort": "high"},
                  messages=[{"role": "user", "content": user}])
    try:
        with client.beta.messages.stream(betas=["server-side-fallback-2026-07-01"],
                                         fallbacks="default", **kwargs) as stream:
            msg = stream.get_final_message()
    except TypeError:
        with client.messages.stream(**kwargs) as stream:
            msg = stream.get_final_message()
    text = "\n".join(b.text for b in msg.content if getattr(b, "type", None) == "text").strip()
    u = getattr(msg, "usage", None)
    usage = {"input_tokens": getattr(u, "input_tokens", None),
             "output_tokens": getattr(u, "output_tokens", None),
             "model": getattr(msg, "model", MODEL)}
    return text, usage, msg.stop_reason


def _run(rid, months):
    a = _app()
    try:
        from routes.farms import build_book_months_pack
        pack = build_book_months_pack(months)
        _update(a, rid, pack=json.dumps(pack))
        if not os.environ.get("ANTHROPIC_API_KEY"):
            raise RuntimeError("ANTHROPIC_API_KEY is not set on the server")
        text, usage, stop = call_claude(pack)
        if stop == "refusal" or not text:
            _update(a, rid, status="failed", usage=json.dumps(usage),
                    error=f"no report returned (stop_reason={stop})")
            return
        _update(a, rid, status="done", report=text, usage=json.dumps(usage))
        a.log.info("Book report %s done: %s", rid, usage)
    except Exception as e:
        a.log.warning("Book report %s failed: %s", rid, e)
        _update(a, rid, status="failed", error=str(e)[:500])


def _rows(a, limit):
    _ensure_table(a)
    conn = a.get_db()
    try:
        cur = conn.cursor()
        p = a.placeholder()
        cur.execute(f"SELECT id, created_at, status, months, model, error, usage, report, pack "
                    f"FROM book_reports ORDER BY id DESC LIMIT {p}", (limit,))
        rows = cur.fetchall()
        cols = ["id", "created_at", "status", "months", "model", "error", "usage", "report", "pack"]
        return [dict(zip(cols, r)) if a.DATABASE_URL else {c: r[c] for c in cols} for r in rows]
    finally:
        conn.close()


def _mark_stale(row):
    if row["status"] == "running":
        try:
            age = (datetime.now(timezone.utc)
                   - datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc))
            if age.total_seconds() > STALE_RUN_SECS:
                row["status"], row["error"] = "failed", "the run stopped (server restart?) — try again"
        except ValueError:
            pass
    return row


@book_report_bp.route("/api/farms/book_report/run", methods=["POST"])
def api_book_report_run():
    """Start a report in the background. Body: {months: 4}. Returns its id."""
    a = _app()
    try:
        months = max(1, min(12, int((request.get_json(silent=True) or {}).get("months") or 4)))
    except (TypeError, ValueError):
        months = 4
    running = [r for r in map(_mark_stale, _rows(a, 5)) if r["status"] == "running"]
    if running:
        return jsonify({"error": "a report is already being written", "id": running[0]["id"]}), 409
    rid = _insert(a, months)
    threading.Thread(target=_run, args=(rid, months), daemon=True, name=f"book-report-{rid}").start()
    return jsonify({"id": rid, "status": "running"})


@book_report_bp.route("/api/farms/book_reports")
def api_book_reports():
    """Recent reports, newest first (report text included)."""
    a = _app()
    out = []
    for r in map(_mark_stale, _rows(a, 12)):
        r["usage"] = json.loads(r["usage"]) if r.get("usage") else None
        # The pack the report was written from, so "Copy with data" can hand both
        # to a reviewer who checks the report's claims against its numbers.
        r["pack"] = json.loads(r["pack"]) if r.get("pack") else None
        out.append(r)
    return jsonify({"reports": out, "model": MODEL})
