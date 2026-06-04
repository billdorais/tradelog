"""
Kairos Crew — Flask Blueprint
Provides /crew (page) and /api/crew/run (SSE stream).
Runs a two-agent CrewAI crew (Researcher → Summarizer) in a background
thread and streams events to the browser via Server-Sent Events.
"""

import json
import os
import queue
import re
import sys
import threading
from datetime import datetime

from flask import Blueprint, Response, jsonify, render_template, request, stream_with_context

crew_bp = Blueprint("crew", __name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ── Crew runner (executes in a daemon thread) ─────────────────────────────────

def _run_crew(topic: str, q: queue.Queue) -> None:
    """Build and run the crew, pushing SSE-ready dicts into q."""
    _orig = sys.stdout

    # Thread-local stdout capture — routes CrewAI's verbose text to the queue.
    # sys.stdout is process-global, so this is only safe for sequential runs
    # (acceptable for a demo/learning context).
    class _Cap:
        _SKIP = re.compile(
            r"^\s*$|^=+$|^-+$|Token|Usage|openai|litellm|LiteLLM|"
            r"Entering new|AgentExecutor|^#|HTTPSConnect|requests\.",
            re.I,
        )
        _AGENTS = [
            (re.compile(r"Research Analyst|Researcher", re.I), "Research Analyst"),
            (re.compile(r"Content Strategist|Summar", re.I),   "Content Strategist"),
        ]

        def __init__(self):
            self._buf   = ""
            self.agent  = "system"

        def write(self, text: str):
            _orig.write(text)
            self._buf += text
            while "\n" in self._buf:
                line, self._buf = self._buf.split("\n", 1)
                line = line.strip()
                if not line or self._SKIP.match(line):
                    continue
                for pat, name in self._AGENTS:
                    if pat.search(line):
                        self.agent = name
                        break
                q.put({"type": "log", "agent": self.agent, "text": line, "ts": _ts()})

        def flush(self):
            _orig.flush()

        def isatty(self):
            return False

        def fileno(self):
            return _orig.fileno()

    cap = _Cap()
    sys.stdout = cap

    try:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            q.put({"type": "error", "error": "ANTHROPIC_API_KEY is not set.", "ts": _ts()})
            return

        import litellm as _litellm
        _orig_comp = _litellm.completion
        def _safe_comp(*args, **kwargs):
            msgs = kwargs.get("messages") or []
            while msgs and isinstance(msgs[-1], dict) and msgs[-1].get("role") == "assistant":
                msgs = msgs[:-1]
            kwargs["messages"] = msgs
            return _orig_comp(*args, **kwargs)
        _litellm.completion = _safe_comp
        from crewai import Agent, Crew, LLM, Process, Task
        from crewai.tools import tool
        import wikipedia as _wiki

        # ── Tool ─────────────────────────────────────────────────────────────
        @tool("Wikipedia Search")
        def search_wikipedia(query: str) -> str:
            """Search Wikipedia for factual information. Use specific queries."""
            try:
                hits = _wiki.search(query, results=3)
                if not hits:
                    return "No results found."
                page = _wiki.page(hits[0], auto_suggest=False)
                return page.content[:4000]
            except _wiki.DisambiguationError as e:
                try:
                    return _wiki.page(e.options[0], auto_suggest=False).content[:4000]
                except Exception:
                    return f"Ambiguous query — try more specific. Options: {e.options[:4]}"
            except Exception as exc:
                return f"Wikipedia error: {exc}"

        # ── LLM ──────────────────────────────────────────────────────────────
        def _llm(temp=0.3):
            return LLM(model="anthropic/claude-sonnet-4-6", api_key=api_key, temperature=temp)

        # ── Agents ───────────────────────────────────────────────────────────
        researcher = Agent(
            role="Research Analyst",
            goal=f"Gather comprehensive, accurate information about '{topic}'.",
            backstory=(
                "You are a meticulous research analyst who locates reliable facts "
                "and synthesises them into clear structured notes."
            ),
            llm=_llm(0.3),
            tools=[search_wikipedia],
            verbose=True,
            allow_delegation=False,
        )

        summarizer = Agent(
            role="Content Strategist",
            goal="Transform research notes into a crisp 3-paragraph summary.",
            backstory=(
                "You distil complex research into readable narratives anyone "
                "can understand, knowing exactly what to keep and what to cut."
            ),
            llm=_llm(0.5),
            tools=[],
            verbose=True,
            allow_delegation=False,
        )

        # ── Tasks ─────────────────────────────────────────────────────────────
        research_task = Task(
            description=(
                f"Research the topic: **{topic}**\n"
                "1. Use Wikipedia Search with 1-2 focused queries.\n"
                "2. Extract 5 key facts with brief explanations.\n"
                "3. Note 1-2 surprising or counter-intuitive insights.\n"
                "4. Flag any important uncertainties."
            ),
            expected_output=(
                "Structured bullet-point notes: 5 facts, 1-2 insights, any caveats."
            ),
            agent=researcher,
        )

        summary_task = Task(
            description=(
                "Using the research notes, write a 3-paragraph summary for a "
                "general audience with no prior knowledge:\n"
                "Para 1 — What it is and why it matters.\n"
                "Para 2 — Key facts connected into a coherent narrative.\n"
                "Para 3 — Most interesting insight or takeaway."
            ),
            expected_output=(
                "Three polished paragraphs (4-6 sentences each), jargon-free."
            ),
            agent=summarizer,
            context=[research_task],
        )

        # ── Callbacks ─────────────────────────────────────────────────────────
        def on_task(task_output):
            q.put({"type": "task_done", "agent": cap.agent, "ts": _ts()})

        # ── Kickoff ───────────────────────────────────────────────────────────
        crew = Crew(
            agents=[researcher, summarizer],
            tasks=[research_task, summary_task],
            process=Process.sequential,
            verbose=True,
            task_callback=on_task,
        )

        result = crew.kickoff()
        q.put({"type": "done", "result": str(result), "ts": _ts()})

    except Exception as exc:
        q.put({"type": "error", "error": str(exc), "ts": _ts()})
    finally:
        sys.stdout = _orig


# ── Kairos Trading Crew ────────────────────────────────────────────────────────

def _run_kairos_crew(q: queue.Queue, strat_data: dict = None, journal_data: list = None, prev_reports: list = None) -> None:
    """Two-agent Kairos trading crew: Data Analyst + Professional Systematic Trader."""
    _orig = sys.stdout

    class _Cap:
        _SKIP = re.compile(
            r"^\s*$|^=+$|^-+$|Token|Usage|openai|litellm|LiteLLM|"
            r"Entering new|AgentExecutor|^#|HTTPSConnect|requests\.",
            re.I,
        )
        _AGENTS = [
            (re.compile(r"Data Analyst|Kairos", re.I),   "Data Analyst"),
            (re.compile(r"Systematic Trader|Advisor", re.I), "Trading Advisor"),
        ]

        def __init__(self):
            self._buf  = ""
            self.agent = "system"

        def write(self, text: str):
            _orig.write(text)
            self._buf += text
            while "\n" in self._buf:
                line, self._buf = self._buf.split("\n", 1)
                line = line.strip()
                if not line or self._SKIP.match(line):
                    continue
                for pat, name in self._AGENTS:
                    if pat.search(line):
                        self.agent = name
                        break
                q.put({"type": "log", "agent": self.agent, "text": line, "ts": _ts()})

        def flush(self):
            _orig.flush()

        def isatty(self):
            return False

        def fileno(self):
            return _orig.fileno()

    cap = _Cap()
    sys.stdout = cap

    try:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            q.put({"type": "error", "error": "ANTHROPIC_API_KEY is not set.", "ts": _ts()})
            return

        import litellm as _litellm

        # CrewAI sometimes ends the message array with an assistant turn, which Claude
        # rejects ("conversation must end with a user message"). Patch litellm.completion
        # to strip any trailing assistant messages before they reach the API.
        _orig_completion = _litellm.completion
        def _safe_completion(*args, **kwargs):
            msgs = kwargs.get("messages") or []
            while msgs and isinstance(msgs[-1], dict) and msgs[-1].get("role") == "assistant":
                msgs = msgs[:-1]
            kwargs["messages"] = msgs
            return _orig_completion(*args, **kwargs)
        _litellm.completion = _safe_completion

        from crewai import Agent, Crew, LLM, Process, Task

        def _llm(temp=0.2):
            return LLM(model="anthropic/claude-sonnet-4-6", api_key=api_key, temperature=temp, max_tokens=2048)

        # ── Format pre-fetched data ───────────────────────────────────────────
        # Data was fetched in the Flask route handler and passed in directly.

        def _fmt_strategies(data: dict) -> str:
            overall   = (data or {}).get("overall", {})
            per_strat = (data or {}).get("per_strategy", {})
            if not per_strat:
                return "No strategy data available (Alpaca may not be configured)."
            lines = [
                "=== REFINED ACCOUNT — STRATEGY LEADERBOARD (last ~20 days) ===",
                f"Overall: {overall.get('trades',0)} trades | "
                f"Win Rate {overall.get('win_rate',0):.1f}% | "
                f"PF {overall.get('profit_factor') or '—'} | "
                f"Total P&L ${overall.get('total_pnl',0):.2f} | "
                f"Sharpe {overall.get('sharpe') or '—'}",
                "", "Per Strategy (sorted by P&L):",
            ]
            for name, s in sorted(per_strat.items(), key=lambda x: x[1].get("total_pnl", 0), reverse=True):
                pf = f"{s['profit_factor']:.2f}" if s.get("profit_factor") else "—"
                sh = f"{s['sharpe']:.2f}"        if s.get("sharpe")         else "—"
                lines.append(
                    f"  {name}: {s.get('trades',0)} trades | "
                    f"{s.get('win_rate',0):.1f}% WR | PF {pf} | "
                    f"Sharpe {sh} | P&L ${s.get('total_pnl',0):.2f}"
                )
            return "\n".join(lines)

        def _fmt_journal(entries: list) -> str:
            entries = (entries or [])[:4]
            if not entries:
                return "No journal entries found."
            lines = ["=== WEEKLY JOURNAL — LAST 4 ENTRIES ===", ""]
            for e in entries:
                ts   = e.get("trade_stats") or {}
                md   = e.get("market_data")  or {}
                tags = e.get("tags")          or {}
                sw   = e.get("sweep_results") or {}
                vix  = md.get("VIX", {}).get("close") or md.get("VIX", {}).get("weekly_return")
                spy  = md.get("SPY", {}).get("weekly_return")
                lines.append(f"Week {e.get('week')} — Grade {tags.get('grade','')} — {', '.join(tags.get('labels') or [])}")
                lines.append(f"  P&L: ${ts.get('total_pnl',0):.2f} | {ts.get('trades',0)} trades | {ts.get('win_rate',0):.1f}% WR | PF {ts.get('profit_factor') or '—'}")
                if vix or spy is not None:
                    spy_str = f"+{spy:.2f}%" if spy and spy >= 0 else f"{spy:.2f}%" if spy else "—"
                    lines.append(f"  Market: VIX {vix or '—'} | SPY {spy_str} | Regime: {md.get('regime','—')}")
                if sw.get("per_strategy"):
                    lines.append("  Stop Sweep Results:")
                    for s in sw["per_strategy"]:
                        trig = f" trigger {s['best_trigger']}%" if s.get("best_trigger") else ""
                        delta = f" Δ${s['delta']:.2f}" if s.get("delta") is not None else ""
                        lines.append(f"    {s['strategy']}: best trail {s['best_trail']}%{trig}{delta} ({s.get('trades',0)} trades)")
                summary = (e.get("ai_summary") or "")[:400]
                if summary:
                    lines.append(f"  AI Analysis: \"{summary}...\"")
                notes = (e.get("user_notes") or "").strip()
                if notes:
                    lines.append(f"  Your Notes: \"{notes}\"")
                lines.append("")
            return "\n".join(lines)

        strategy_block = _fmt_strategies(strat_data)
        journal_block  = _fmt_journal(journal_data)

        # ── Agents — no tools; data is embedded in task descriptions ─────────
        # Removes all tool-call complexity and token overhead.

        advisor = Agent(
            role="Professional Systematic Trading Advisor",
            goal="Analyse the Kairos trading data and deliver specific, actionable recommendations.",
            backstory=(
                "You are a seasoned systematic trading professional with 20 years of "
                "experience managing algorithmic strategy portfolios on US equities. "
                "You specialise in intraday Camarilla pivot strategies on 5-minute bars. "
                "You are rigorous about sample size — you don't change parameters based on "
                "two trades. You understand regime effects: reversal strategies thrive in "
                "ranging markets, breakouts in trending ones. You know that a 44% win rate "
                "with a 2.18 profit factor is a GOOD system — the edge is in letting winners "
                "run, not in win rate. You give direct, numbered recommendations. You name "
                "specific strategies. You are not afraid to say 'hold steady' when the data "
                "doesn't support a change. The account targets ~$25k live capital."
            ),
            llm=_llm(0.4),
            tools=[],
            verbose=True,
            allow_delegation=False,
        )

        # ── Previous reports block ────────────────────────────────────────────
        prev_block = ""
        if prev_reports:
            lines = ["=== YOUR PREVIOUS ADVISORY REPORTS (most recent first) ===", ""]
            for r in (prev_reports or [])[:3]:
                lines.append(f"--- Report from {r.get('created_at','')[:10]} (Week {r.get('week','')}) ---")
                lines.append(r.get("report", "")[:1200])  # cap each to stay within context
                lines.append("")
            prev_block = "\n".join(lines)

        # ── Single task — all data embedded directly ──────────────────────────

        analysis_task = Task(
            description=(
                f"Here is the current Kairos trading system data:\n\n"
                f"{strategy_block}\n\n"
                f"{journal_block}\n\n"
                + (f"For historical context, here are your previous advisory reports:\n\n{prev_block}\n\n" if prev_block else "")
                + "Based on all of this, deliver a professional advisory report with five sections:\n\n"
                "1. **Portfolio Health** — Is the Refined top-20 earning its keep? "
                "What does the PF and Sharpe say about real edge vs. luck?\n\n"
                "2. **Strategy Calls** — Name 2-3 to promote/add to Refined and 2-3 to pause "
                "or demote. Give specific reasons tied to numbers.\n\n"
                "3. **Stop & Parameter Check** — Given the regime tags and any sweep data in "
                "the journal, are current trailing stops appropriate? Reference specific sweep "
                "results and the trader's own notes if they offer relevant observations.\n\n"
                "4. **Risk Observations** — Concentration, drawdown patterns, or ticker "
                "exposure worth flagging for a ~$25k live account.\n\n"
                "5. **This Week's Focus** — One specific, testable action item. If you gave "
                "advice last week, note whether it played out and whether it should continue.\n\n"
                "Be direct. Cite strategy names and numbers. 'Hold steady' is valid when warranted."
            ),
            expected_output=(
                "A professional 5-section advisory report with specific strategy names, "
                "numbers-backed recommendations, and one concrete next-week action item."
            ),
            agent=advisor,
        )

        # ── Callbacks & Kickoff ───────────────────────────────────────────────

        def on_task(task_output):
            q.put({"type": "task_done", "agent": cap.agent, "ts": _ts()})

        crew = Crew(
            agents=[advisor],
            tasks=[analysis_task],
            process=Process.sequential,
            verbose=True,
            task_callback=on_task,
        )

        result      = crew.kickoff()
        report_text = str(result)
        q.put({"type": "done", "result": report_text, "ts": _ts()})

        # Persist report to DB so future runs have historical context
        try:
            import app as _app
            import datetime as _dt2
            _conn = _app.get_db(); _cur2 = _conn.cursor(); _p2 = _app.placeholder()
            _iso  = _dt2.date.today().isocalendar()
            _week = f"{_iso[0]}-W{_iso[1]:02d}"
            _now  = _dt2.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            _cur2.execute(
                f"INSERT INTO crew_reports (week, created_at, report) VALUES ({_p2},{_p2},{_p2})",
                (_week, _now, report_text),
            )
            _conn.commit(); _conn.close()
        except Exception:
            pass  # non-fatal

    except Exception as exc:
        q.put({"type": "error", "error": str(exc), "ts": _ts()})
    finally:
        sys.stdout = _orig


# ── Routes ────────────────────────────────────────────────────────────────────

@crew_bp.route("/api/crew/chat", methods=["POST"])
def api_crew_chat():
    """Streaming chat with the Systematic Trading Advisor using the crew report as context."""
    data     = request.get_json(silent=True) or {}
    report   = (data.get("report") or "").strip()
    messages = data.get("messages") or []

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not set"}), 503

    system_prompt = (
        "You are a Professional Systematic Trading Advisor specialising in "
        "intraday Camarilla pivot strategies on US equities (5-min bars, Alpaca). "
        "You have just completed a full analysis of the Kairos trading system. "
        "Here is your advisory report:\n\n"
        f"{report}\n\n"
        "Answer follow-up questions directly and specifically. Reference the actual "
        "strategy names and numbers from your report. Be concise — 100-200 words "
        "unless the question warrants more. No filler phrases."
    )

    def generate():
        try:
            import anthropic as _ant
            client = _ant.Anthropic(api_key=api_key)
            full_text = ""
            with client.messages.stream(
                model="claude-sonnet-4-6",
                max_tokens=600,
                system=system_prompt,
                messages=messages,
            ) as stream:
                for text in stream.text_stream:
                    full_text += text
                    yield f"data: {json.dumps({'text': text})}\n\n"
            yield f"data: {json.dumps({'done': True, 'full_text': full_text})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


@crew_bp.route("/crew")
def crew_page():
    return render_template("crew.html")


@crew_bp.route("/api/crew/run", methods=["POST"])
def api_crew_run():
    data       = request.get_json(silent=True) or {}
    crew_type  = (data.get("crew_type") or "research").strip()
    topic      = (data.get("topic") or "").strip()

    q = queue.Queue()

    if crew_type == "kairos":
        # Pre-fetch all data while we have Flask request context.
        from flask import current_app as _ca
        import app as _kairos
        strat_data   = {}
        journal_data = []
        prev_reports = []
        try:
            with _ca.test_client() as _c:
                strat_data   = _c.get("/api/alpaca/analysis?account=2").get_json() or {}
                journal_data = _c.get("/api/journal/entries").get_json()            or []
        except Exception:
            pass
        try:
            _conn = _kairos.get_db(); _cur = _conn.cursor()
            _cur.execute(
                "SELECT week, created_at, report FROM crew_reports "
                "ORDER BY created_at DESC LIMIT 3"
            )
            for _row in _cur.fetchall():
                prev_reports.append({
                    "week":       _row[0] if _kairos.DATABASE_URL else _row["week"],
                    "created_at": _row[1] if _kairos.DATABASE_URL else _row["created_at"],
                    "report":     _row[2] if _kairos.DATABASE_URL else _row["report"],
                })
            _conn.close()
        except Exception:
            pass
        threading.Thread(
            target=_run_kairos_crew,
            args=(q, strat_data, journal_data, prev_reports),
            daemon=True,
        ).start()
    else:
        if not topic:
            return jsonify({"error": "topic required"}), 400
        threading.Thread(target=_run_crew, args=(topic, q), daemon=True).start()

    def generate():
        while True:
            try:
                event = q.get(timeout=180)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in ("done", "error"):
                    return
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )
