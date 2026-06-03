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

    cap = _Cap()
    sys.stdout = cap

    try:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            q.put({"type": "error", "error": "ANTHROPIC_API_KEY is not set.", "ts": _ts()})
            return

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


# ── Routes ────────────────────────────────────────────────────────────────────

@crew_bp.route("/crew")
def crew_page():
    return render_template("crew.html")


@crew_bp.route("/api/crew/run", methods=["POST"])
def api_crew_run():
    data  = request.get_json(silent=True) or {}
    topic = (data.get("topic") or "").strip()
    if not topic:
        return jsonify({"error": "topic required"}), 400

    q = queue.Queue()
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
