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
        _litellm.drop_params = True
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
            return LLM(model="anthropic/claude-sonnet-4-6", api_key=api_key, temperature=temp,
                       drop_params=True, modify_params=True)

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

def _run_kairos_crew(q: queue.Queue, base_url: str) -> None:
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
        _litellm.drop_params = True   # prevents CrewAI appending assistant prefill Claude rejects
        import requests as _req
        from crewai import Agent, Crew, LLM, Process, Task
        from crewai.tools import tool

        def _llm(temp=0.2):
            return LLM(model="anthropic/claude-sonnet-4-6", api_key=api_key, temperature=temp, max_tokens=2048,
                       drop_params=True, modify_params=True)

        # ── Tools ─────────────────────────────────────────────────────────────

        @tool("Get Strategy Performance")
        def get_strategy_performance() -> str:
            """
            Fetch the current Refined account strategy leaderboard from Kairos.
            Returns per-strategy P&L, win rate, profit factor, Sharpe ratio, and
            trade count for the last 20 days. Use this to understand which strategies
            are performing well and which are underperforming.
            """
            try:
                r = _req.get(f"{base_url}/api/alpaca/analysis?account=2", timeout=30)
                data = r.json()
                overall = data.get("overall", {})
                per_strat = data.get("per_strategy", {})
                lines = [
                    "=== REFINED ACCOUNT — STRATEGY LEADERBOARD (last ~20 days) ===",
                    f"Overall: {overall.get('trades',0)} trades | "
                    f"Win Rate {overall.get('win_rate',0):.1f}% | "
                    f"PF {overall.get('profit_factor') or '—'} | "
                    f"Total P&L ${overall.get('total_pnl',0):.2f} | "
                    f"Sharpe {overall.get('sharpe') or '—'}",
                    "",
                    "Per Strategy (sorted by P&L):",
                ]
                sorted_strats = sorted(per_strat.items(), key=lambda x: x[1].get("total_pnl", 0), reverse=True)
                for name, s in sorted_strats:
                    pf = f"{s.get('profit_factor'):.2f}" if s.get("profit_factor") else "—"
                    sh = f"{s.get('sharpe'):.2f}" if s.get("sharpe") else "—"
                    lines.append(
                        f"  {name}: {s.get('trades',0)} trades | "
                        f"{s.get('win_rate',0):.1f}% WR | PF {pf} | "
                        f"Sharpe {sh} | P&L ${s.get('total_pnl',0):.2f}"
                    )
                return "\n".join(lines)
            except Exception as e:
                return f"Error fetching strategy data: {e}"

        @tool("Get Journal History")
        def get_journal_history() -> str:
            """
            Fetch the last 4 weekly journal entries from Kairos. Each entry includes
            weekly P&L, trade stats, market regime (VIX, SPY performance, regime character),
            AI-generated analysis, and any saved sweep snapshots of optimal stop parameters.
            Use this to understand recent trends, regime context, and historical observations.
            """
            try:
                r = _req.get(f"{base_url}/api/journal/entries", timeout=30)
                entries = r.json()[:4]
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
                    grade  = tags.get("grade", "")
                    labels = ", ".join(tags.get("labels") or [])
                    lines.append(f"Week {e.get('week')} — Grade {grade} — {labels}")
                    lines.append(
                        f"  P&L: ${ts.get('total_pnl',0):.2f} | "
                        f"{ts.get('trades',0)} trades | "
                        f"{ts.get('win_rate',0):.1f}% WR | "
                        f"PF {ts.get('profit_factor') or '—'}"
                    )
                    if vix or spy is not None:
                        lines.append(
                            f"  Market: VIX {vix or '—'} | "
                            f"SPY {f'+{spy:.2f}%' if spy and spy >= 0 else f'{spy:.2f}%' if spy else '—'} | "
                            f"Regime: {md.get('regime','—')}"
                        )
                    if sw.get("per_strategy"):
                        sweep_str = " | ".join(
                            f"{s['strategy']}: trail {s['best_trail']}%"
                            for s in sw["per_strategy"][:4]
                        )
                        lines.append(f"  Sweep: {sweep_str}")
                    summary = (e.get("ai_summary") or "")[:300]
                    if summary:
                        lines.append(f"  AI Notes: \"{summary}...\"")
                    lines.append("")
                return "\n".join(lines)
            except Exception as e:
                return f"Error fetching journal: {e}"

        # ── Agents ────────────────────────────────────────────────────────────

        data_agent = Agent(
            role="Kairos Data Analyst",
            goal=(
                "Collect and organize all available trading performance data from the "
                "Kairos system — strategy leaderboard, weekly journal entries, market "
                "regime context, and any saved parameter sweep results."
            ),
            backstory=(
                "You are a quantitative data specialist who extracts and organises "
                "trading system performance data with precision. You present facts cleanly, "
                "highlight patterns without editorialising, and flag anything that looks "
                "anomalous. Your job is to give the advisor everything they need."
            ),
            llm=_llm(0.1),
            tools=[get_strategy_performance, get_journal_history],
            verbose=True,
            allow_delegation=False,
        )

        advisor = Agent(
            role="Professional Systematic Trading Advisor",
            goal=(
                "Analyse the Kairos trading system's performance data and deliver "
                "specific, actionable recommendations to improve returns and manage risk."
            ),
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

        # ── Tasks ─────────────────────────────────────────────────────────────

        collection_task = Task(
            description=(
                "Fetch the Kairos trading system performance data:\n"
                "1. Call 'Get Strategy Performance' for the full strategy leaderboard\n"
                "2. Call 'Get Journal History' for the last 4 weekly entries\n"
                "3. Identify: top 5 by P&L, bottom 5 by P&L, strategies with 0% win rate, "
                "strategies with fewer than 5 trades\n"
                "4. Note the last 2 weeks' regime tags and any sweep snapshots on file\n"
                "5. Flag any strategies that appear in both best and worst lists across weeks"
            ),
            expected_output=(
                "A structured data report with: overall performance summary, "
                "ranked strategy list, 4-week journal summary with regime tags, "
                "and any notable patterns or anomalies."
            ),
            agent=data_agent,
        )

        analysis_task = Task(
            description=(
                "As a professional systematic trading advisor, analyse the data and deliver "
                "a concise advisory report with these five sections:\n\n"
                "1. **Portfolio Health** — Is the Refined top-20 earning its keep? "
                "What does the PF and Sharpe tell you about real edge vs. luck?\n\n"
                "2. **Strategy Calls** — Name 2-3 strategies that deserve promotion "
                "(more capital or addition to Refined) and 2-3 that should be paused or "
                "demoted. Give specific reasons tied to the numbers.\n\n"
                "3. **Stop & Parameter Check** — Given the recent regime (VIX, character "
                "tags), are the current trailing stops appropriate? Reference any sweep "
                "data if available. Should anything be tightened or loosened?\n\n"
                "4. **Risk Observations** — Any concentration risk, drawdown pattern, "
                "or ticker exposure worth flagging for a ~$25k live account?\n\n"
                "5. **This Week's Focus** — One specific, testable thing to monitor or "
                "experiment with next week. Be concrete.\n\n"
                "Be direct. Cite strategy names and numbers. Don't hedge when the data "
                "is clear. 'Hold steady' is a valid call when warranted."
            ),
            expected_output=(
                "A professional 5-section advisory report with specific strategy names, "
                "numbers-backed recommendations, and one concrete next-week action item."
            ),
            agent=advisor,
            context=[collection_task],
        )

        # ── Callbacks & Kickoff ───────────────────────────────────────────────

        def on_task(task_output):
            q.put({"type": "task_done", "agent": cap.agent, "ts": _ts()})

        crew = Crew(
            agents=[data_agent, advisor],
            tasks=[collection_task, analysis_task],
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


# ── Routes ────────────────────────────────────────────────────────────────────

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
        # Use the app's own base URL for internal API calls
        port     = os.environ.get("PORT", "5000")
        base_url = f"http://127.0.0.1:{port}"
        threading.Thread(target=_run_kairos_crew, args=(q, base_url), daemon=True).start()
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
