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
from datetime import datetime, timedelta, timezone

from flask import Blueprint, Response, jsonify, render_template, request, stream_with_context

crew_bp = Blueprint("crew", __name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


# CrewAI's verbose output carries ANSI color codes (e.g. ESC[00m). They render as
# stray "[00m" litter once the text lands in the browser feed — strip them.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

def _strip_ansi(s: str) -> str:
    return _ANSI_RE.sub("", s)


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
                line = _strip_ansi(line).strip()
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

def _run_kairos_crew(q: queue.Queue, strat_data: dict = None, journal_data: list = None, prev_reports: list = None, period: str = "", rules_data: list = None, engine_data: dict = None, engine_strat_data: dict = None, card_data: dict = None, scorecard_data: dict = None, book_data: dict = None, farm_strat_data: dict = None, kairos_farm_strat_data: dict = None, kairos_target: str = "9", tv_snap_rank: dict = None, kairos_snap_rank: dict = None) -> None:
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
                line = _strip_ansi(line).strip()
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
            return LLM(model="anthropic/claude-sonnet-4-6", api_key=api_key, temperature=temp, max_tokens=4096)

        # ── Format pre-fetched data ───────────────────────────────────────────
        # Data was fetched in the Flask route handler and passed in directly.

        def _fmt_strategies(data: dict, header: str = "TV REFINED (account 2) — STRATEGY LEADERBOARD (last ~20 days)",
                            empty_msg: str = "No strategy data available (Alpaca may not be configured).") -> str:
            overall   = (data or {}).get("overall", {})
            per_strat = (data or {}).get("per_strategy", {})
            if not per_strat:
                return empty_msg
            lines = [
                f"=== {header} ===",
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

        def _fmt_stops_comparison(rules: list, journal_entries: list) -> str:
            """Build a current SR stops vs best sweep results comparison table."""
            from collections import defaultdict

            # ── Current Signal Router stops grouped by type_level ────────────
            sr = defaultdict(list)  # type_level → [{trail, trigger, max_hold}]
            for rule in (rules or []):
                if not rule.get("enabled"):
                    continue
                name  = (rule.get("name") or "").upper()
                idx   = name.find("_CAM_")
                if idx < 0:
                    continue
                parts = name[idx + 5:].split("_")
                if len(parts) < 2:
                    continue
                tl = f"{parts[0]} {parts[1]}"  # e.g. "BREAKOUT R4S4"
                for nd in (rule.get("nodes") or []):
                    if nd.get("type") == "exit_params":
                        sr[tl].append({
                            "trail":   nd.get("trail_offset"),
                            "trigger": nd.get("trail_trigger"),
                            "hold":    nd.get("max_hold_mins"),
                        })
                        break

            if not sr:
                return ""

            # ── Most recent sweep results from journal ───────────────────────
            sweep_by_tl = {}   # type_level → {best_trail, best_trigger, delta, date}
            for e in (journal_entries or []):
                sw = e.get("sweep_results") or {}
                if not sw.get("per_strategy"):
                    continue
                date = (e.get("week") or "")
                for s in sw["per_strategy"]:
                    raw  = (s.get("strategy") or "").upper()
                    tidx = raw.find("_CAM_") if "_CAM_" in raw else -1
                    if tidx < 0:
                        tl_sw = raw
                    else:
                        pts   = raw[tidx + 5:].split("_")
                        tl_sw = f"{pts[0]} {pts[1]}" if len(pts) >= 2 else raw
                    # Keep most recent sweep entry per type_level
                    if tl_sw not in sweep_by_tl:
                        sweep_by_tl[tl_sw] = {
                            "trail":   s.get("best_trail"),
                            "trigger": s.get("best_trigger"),
                            "delta":   s.get("delta"),
                            "trades":  s.get("trades"),
                            "date":    date,
                        }

            # ── Format comparison table ──────────────────────────────────────
            lines = ["=== SIGNAL ROUTER STOPS vs SWEEP RESULTS ===", ""]
            lines.append(f"{'Type':<18} {'SR Trail':>9} {'SR Trig':>9} {'SR Hold':>8} | "
                         f"{'Sweep Trail':>11} {'Sweep Trig':>11} {'Δ vs SR':>9} {'Sweep Date'}")
            lines.append("-" * 90)

            for tl in sorted(sr.keys()):
                stops = sr[tl]
                # Representative values (use first non-null; flag if mixed)
                trails   = [s["trail"]   for s in stops if s.get("trail")   is not None]
                triggers = [s["trigger"] for s in stops if s.get("trigger") is not None]
                holds    = [s["hold"]    for s in stops if s.get("hold")    is not None]
                sr_trail   = f"{trails[0]}%"   if trails   else "—"
                sr_trigger = f"{triggers[0]}%" if triggers else "none"
                sr_hold    = f"{holds[0]}m"    if holds    else "—"

                sw = sweep_by_tl.get(tl, {})
                sw_trail   = f"{sw['trail']}%"   if sw.get("trail")   else "—"
                sw_trigger = f"{sw['trigger']}%" if sw.get("trigger") else "—"
                sw_date    = sw.get("date", "—")

                # Gap analysis
                gap = ""
                if sw.get("trail") and trails:
                    diff = float(sw["trail"]) - float(trails[0])
                    if abs(diff) >= 0.01:
                        gap = f"+{diff:.2f}%" if diff > 0 else f"{diff:.2f}%"
                        gap += " (loosen)" if diff > 0 else " (tighten)"
                    else:
                        gap = "≈ aligned"
                sw_delta = f"${sw['delta']:.2f}" if sw.get("delta") is not None else "—"

                lines.append(f"{tl:<18} {sr_trail:>9} {sr_trigger:>9} {sr_hold:>8} | "
                             f"{sw_trail:>11} {sw_trigger:>11} {sw_delta:>9}   {sw_date}  {gap}")

            lines.append("")
            lines.append("Note: Δ vs SR = sweep P&L improvement over current SR settings. "
                         "Gap = difference between sweep-optimal trail and current configured trail.")
            return "\n".join(lines)

        def _fmt_engine(data: dict) -> str:
            """Kairos Refined (acct 3) vs TV Refined (acct 2) head-to-head."""
            data = data or {}
            if not data.get("configured"):
                return ("=== KAIROS ENGINE PILOT (account 3) ===\n"
                        "Not configured yet — the server-side entry pilot has no data to compare. "
                        "Treat the engine experiment as not-yet-started.")
            rows = data.get("rows") or []
            tv   = data.get("tv") or {}
            eng  = data.get("engine") or {}
            if not eng.get("trades") and not tv.get("trades"):
                return ("=== KAIROS ENGINE PILOT (account 3) vs TV REFINED (account 2) ===\n"
                        "The engine account is live but has barely traded — too early to judge. "
                        "Say so plainly; do not over-read a handful of trades.")
            diff = (eng.get("pnl", 0) or 0) - (tv.get("pnl", 0) or 0)
            lines = [
                f"=== KAIROS ENGINE PILOT (acct 3, server-side entries) vs TV REFINED (acct 2) — last {data.get('days', 30)}d ===",
                f"TV Refined:    ${tv.get('pnl', 0):.2f} | {tv.get('trades', 0)} trades | {tv.get('win_rate', 0):.1f}% win",
                f"Kairos Engine: ${eng.get('pnl', 0):.2f} | {eng.get('trades', 0)} trades | {eng.get('win_rate', 0):.1f}% win",
                f"Engine − TV:   ${diff:+.2f}  (this is the experiment's headline number)",
                "", "Daily (most recent first):",
            ]
            for r in rows[:15]:
                lines.append(
                    f"  {r.get('date')}: TV ${r.get('tv_pnl', 0):+.2f} ({r.get('tv_trades', 0)} tr) | "
                    f"Engine ${r.get('engine_pnl', 0):+.2f} ({r.get('engine_trades', 0)} tr)"
                )
            lines.append("")
            lines.append("Context: same Refined strategies, but acct-3 entries are generated server-side "
                         "(fresh-cross buffered breakouts / confirmed reversal rejects) instead of by TV alerts. "
                         "The engine enters a touch earlier on breakouts. Slippage and sim-vs-real caveats apply; "
                         "the matched-trade comparison and a multi-week trend matter more than any single day.")
            return "\n".join(lines)

        strategy_block = _fmt_strategies(strat_data)
        engine_strat_block = _fmt_strategies(
            engine_strat_data,
            header="KAIROS REFINED (account 3, engine entries) — STRATEGY LEADERBOARD (co-equal to TV Refined; fed by the Kairos Farm)",
            empty_msg=("=== KAIROS REFINED (account 3) — STRATEGY LEADERBOARD ===\n"
                       "No per-strategy data yet — Kairos Refined (acct3) has no closed round-trips "
                       "in this window. Treat the Kairos entries as not-yet-evaluable per strategy."),
        )
        # Farm full-sample leaderboards — the audition pools the Refined books are
        # drawn from. Deeper history / more trades per name than the curated Refined
        # subset, so a thin-but-promising Refined name can be corroborated (and clear
        # the sample floor) by its farm record on the SAME entry mechanism.
        farm_strat_block = _fmt_strategies(
            farm_strat_data,
            header=("TV FARM (account 1) — FULL-SAMPLE STRATEGY LEADERBOARD, last ~45d "
                    "(audition pool for TV Refined; TV entries; deeper history than acct2 — "
                    "spans a fixed trailing window, NOT the report's analysis range)"),
            empty_msg=("=== TV FARM (account 1) — FULL-SAMPLE LEADERBOARD ===\n"
                       "No TV Farm data in the trailing 45d window."),
        )
        kairos_farm_strat_block = _fmt_strategies(
            kairos_farm_strat_data,
            header=("KAIROS FARM (account 5) — FULL-SAMPLE STRATEGY LEADERBOARD, last ~45d "
                    "(audition pool for Kairos Refined; server-side engine entries; deeper history than acct3 — "
                    "spans a fixed trailing window, NOT the report's analysis range)"),
            empty_msg=("=== KAIROS FARM (account 5) — FULL-SAMPLE LEADERBOARD ===\n"
                       "No Kairos Farm data in the trailing 45d window."),
        )
        def _fmt_snapshot_rank(snap, header, tag):
            """The Analysis-page snapshot leaderboard in composite-SCORE rank order —
            the ranking the user actually watches and that the Refined book trades.
            The crew should draw its picks for this book in THIS order (top-down),
            not re-rank from raw farm P&L."""
            rows = (snap or {}).get("top_scored") or []
            if not rows:
                return (f"=== {header} ===\nNo snapshot data yet — rank this book's picks by "
                        f"the farm/Refined records above.")
            lines = [f"=== {header} ===",
                     f"Ranked by composite SCORE (the leaderboard order). Prefer {tag} picks in "
                     f"THIS order, top-down, subject to the guardrail + sample floor:"]
            for i, r in enumerate(rows):
                pf = f"{r['profit_factor']:.2f}" if r.get("profit_factor") else "—"
                sc = f"{r['score']:.0f}" if r.get("score") is not None else "—"
                lines.append(
                    f"  #{i+1} {r.get('name')}: score {sc} | "
                    f"{r.get('trades',0)} tr | {r.get('win_rate',0):.0f}% WR | PF {pf} | "
                    f"P&L ${r.get('total_pnl',0):.2f}"
                )
            return "\n".join(lines)

        tv_snap_block = _fmt_snapshot_rank(
            tv_snap_rank,
            "TV REFINED SNAPSHOT — LEADERBOARD RANK (the Analysis-page snapshot; acct2 trades this)",
            "[TV]")
        kairos_snap_block = _fmt_snapshot_rank(
            kairos_snap_rank,
            "KAIROS REFINED SNAPSHOT — LEADERBOARD RANK (the Analysis-page snapshot; acct3 trades this)",
            "[Kairos]")
        journal_block  = _fmt_journal(journal_data)
        stops_block    = _fmt_stops_comparison(rules_data, journal_data)
        engine_block   = _fmt_engine(engine_data)

        def _fmt_card_inputs(cd):
            cd = cd or {}
            lines = ["=== NEXT-MONTH CARD INPUTS (for the leading recommendation card) ==="]
            idx = cd.get("indices_paper_all") or {}
            if idx:
                tot = round(sum(idx.values()), 2)
                lines.append(f"Indices-only P&L on TV Farm (acct1): ${tot:.2f} total — "
                             + ", ".join(f"{k} ${v:.2f}" for k, v in sorted(idx.items(), key=lambda x: -x[1])))
            else:
                lines.append("Indices-only P&L on TV Farm: no index data in window.")
            _BOOKS = (("TV Refined acct2", "refined"), ("Kairos Refined acct3", "kairos"),
                      ("Crew Paper acct4", "crew"))
            for label, sfx in _BOOKS:
                ss = cd.get(f"side_{sfx}") or []
                if ss:
                    lines.append(f"{label} by side: " + " · ".join(
                        f"{r.get('side')} ${r.get('pnl', 0):.2f} ({r.get('trades', 0)}t {r.get('win_rate', 0)}%)"
                        for r in ss))
            # Per-band x side P&L — band-level side edges (more robust than the
            # per-strategy candidates). Only cells with >=5 trades, worst-first, so
            # the advisor can side-gate a band whose one side bleeds. Bands carry the
            # strategy kind (BREAKOUT R4S4 / REVERSAL R4S4) — the same level's
            # breakouts and reversals can run opposite, so they must not be merged.
            for label, sfx in _BOOKS:
                rows = [r for r in (cd.get(f"band_side_{sfx}") or []) if (r.get("trades", 0) or 0) >= 5]
                if rows:
                    rows = sorted(rows, key=lambda r: r.get("pnl", 0))[:8]
                    lines.append(
                        f"BAND x SIDE on {label} (>=5 trades, worst first — a band whose ONE side "
                        f"bleeds is a side-gate candidate; a side that bleeds across ALL bands is regime, not strategy; "
                        f"if one KIND bleeds (e.g. REVERSAL R4S4) while the same level's breakouts hold, "
                        f"cut the kind, not the level): "
                        + " · ".join(
                            f"{r.get('band')} {r.get('side')} ${r.get('pnl', 0):.0f} "
                            f"({r.get('trades', 0)}t {r.get('win_rate', 0)}%)" for r in rows))
            # SIDE x DAY-TYPE — the regime-vs-structural test. A side that loses on
            # EVERY day type is regime (the tape this month); a side that loses only
            # on specific day types is structural, and the day-type gate can address
            # it. This is what decides whether a bleeding side gets a filter or is
            # left alone as one month's direction.
            for label, key in (("TV Refined acct2", "side_daytype_refined"),
                               ("Kairos Refined acct3", "side_daytype_kairos"),
                               ("Crew Paper acct4", "side_daytype_crew")):
                rows = [r for r in (cd.get(key) or []) if (r.get("trades", 0) or 0) >= 3]
                if rows:
                    rows = sorted(rows, key=lambda r: r.get("pnl", 0))[:8]
                    lines.append(
                        f"SIDE x DAY-TYPE on {label} (>=3 trades, worst first — a side that bleeds "
                        f"on EVERY day type is REGIME; a side that bleeds only on specific day types "
                        f"is STRUCTURAL and the day-type gate can target it): "
                        + " · ".join(
                            f"{r.get('side')} on {r.get('day_type')} ${r.get('pnl', 0):.0f} "
                            f"({r.get('trades', 0)}t {r.get('win_rate', 0)}%)" for r in rows))
            for label, key in (("TV Refined acct2", "side_gated_refined"), ("Kairos Refined acct3", "side_gated_kairos"),
                               ("Crew Paper acct4", "side_gated_crew")):
                cands = cd.get(key) or []
                if cands:
                    lines.append(
                        f"SIDE-GATED CANDIDATES on {label} (would score higher gated to one side — "
                        f"score is /100, candidates for a Top-5 slot with a side gate): " + " · ".join(
                            f"{c.get('strategy')} {c.get('best_side')}-only "
                            f"{c.get('best_side_score')} vs both {c.get('both_sides_score')} "
                            f"(${c.get('pnl', 0):.0f}, {c.get('trades', 0)}t {c.get('win_rate', 0)}%)"
                            for c in cands[:6]))
            return "\n".join(lines)

        card_block = _fmt_card_inputs(card_data)

        def _fmt_scorecard(sc):
            """The advisor's out-of-sample feedback loop: last report's picks vs
            what they ACTUALLY did on Crew Paper after wiring."""
            if not sc or not sc.get("picks"):
                return ""
            lines = [
                f"=== PREVIOUS PICKS SCORECARD — your last Top-{sc.get('n_picks')} "
                f"(report {sc.get('report_week')}, wired since {sc.get('since')}) vs ACTUAL Crew Paper results ===",
                f"Traded: {sc.get('n_traded')}/{sc.get('n_picks')} | "
                f"Positive: {sc.get('n_positive')}/{sc.get('n_traded') or 0} | "
                f"Net P&L: ${sc.get('total_pnl', 0):.2f}",
                "",
            ]
            for r in sc.get("picks", []):
                if r.get("trades"):
                    lines.append(f"  {r['strategy']} ({r.get('side')}): {r['trades']} trades | "
                                 f"${r['pnl']:.2f} | {r['win_rate']:.0f}% win")
                else:
                    lines.append(f"  {r['strategy']} ({r.get('side')}): no trades yet")
            lines.append("")
            lines.append("This is the OUT-OF-SAMPLE test of your own selection method — the picks were "
                         "chosen on lookback data, this is what they did afterwards. Grade it honestly.")
            return "\n".join(lines)

        scorecard_block = _fmt_scorecard(scorecard_data)

        def _fmt_book(bk):
            """The LIVE Crew Paper book: every strategy currently wired to acct4 and
            how it's actually doing since it was wired. Unlike the pick scorecard,
            this covers the real wired set (not just the last card), so nothing on
            Crew Paper is invisible when deciding what to keep vs cut."""
            if not bk or not bk.get("picks"):
                return ""
            lines = [
                f"=== CURRENT CREW PAPER BOOK — {bk.get('n_wired')} strategies wired NOW "
                f"(each since its wire date; earliest {bk.get('since')}) ===",
                f"Traded: {bk.get('n_traded')}/{bk.get('n_wired')} | "
                f"Positive: {bk.get('n_positive')}/{bk.get('n_traded') or 0} | "
                f"Net P&L: ${bk.get('total_pnl', 0):.2f}",
                "",
            ]
            for r in bk.get("picks", []):
                off = "" if r.get("enabled", True) else " [DISABLED]"
                if r.get("trades"):
                    lines.append(f"  {r['strategy']}{off}: {r['trades']} trades | "
                                 f"${r['pnl']:.2f} | {r['win_rate']:.0f}% win | since {r.get('since')}")
                else:
                    lines.append(f"  {r['strategy']}{off}: no trades yet | since {r.get('since')}")
            lines.append("")
            lines.append("This is the ACTUAL live book on Crew Paper — worst first. Keep the earners, "
                         "cut the persistent bleeders. A strategy here but NOT in this month's Top picks "
                         "will be removed from Crew Paper on the next wire, so say so if you'd keep it.")
            return "\n".join(lines)

        book_block = _fmt_book(book_data)

        def _fmt_gate_state(gs):
            """Ground-truth system gate config, so the crew reports what's actually
            live instead of guessing (it once claimed there was no pre-market CPR
            filter while the day-type gate was on for every book)."""
            if not gs:
                return ""
            db, dr = gs.get("daytype_breakout", {}), gs.get("daytype_reversal", {})
            lines = ["=== LIVE SYSTEM GATE STATE (ground truth — report this as FACT; do NOT "
                     "claim a gate is missing when it is listed ON here) ==="]
            if db.get("on"):
                lines.append(f"Day-type gate (breakouts): ON — blocks BREAKOUT entries except on "
                             f"{'/'.join(db.get('ok_days') or [])} days, for: {', '.join(db.get('accounts') or [])}. "
                             f"{db.get('note','')}")
            else:
                lines.append("Day-type gate (breakouts): OFF.")
            if dr.get("on"):
                lines.append(f"Day-type gate (reversals): ON — blocks REVERSAL entries except on "
                             f"{'/'.join(dr.get('ok_days') or [])} days, for: {', '.join(dr.get('accounts') or [])}.")
            else:
                lines.append("Day-type gate (reversals): OFF (separate, independently toggled).")
            lines.append("")
            lines.append("Per curated book:")
            for b in gs.get("books", []):
                lines.append(
                    f"  {b['label']}: breakout day-type gate {'ON' if b['breakout_daytype_gated'] else 'off'}"
                    f" · reversals {b['reversal_policy']}"
                    f" · hours {b['hours']}"
                    f" · profit-lock {'on' if b['profit_lock'] else 'off'}"
                    f" · daily-loss guard {'on' if b['daily_loss_guard'] else 'off'}")
            return "\n".join(lines)

        gate_block = _fmt_gate_state(_gate_state())

        # ── Load knowledge base ───────────────────────────────────────────────
        knowledge_block = ""
        try:
            import pathlib
            kb_path = pathlib.Path(__file__).parent.parent / "crew_knowledge.md"
            if kb_path.exists():
                knowledge_block = kb_path.read_text(encoding="utf-8")
        except Exception:
            pass

        # ── Agents — no tools; data is embedded in task descriptions ─────────
        # Removes all tool-call complexity and token overhead.

        KAIROS_SYSTEM_KNOWLEDGE = """
=== KAIROS AUTOMATA — SYSTEM OVERVIEW ===

Kairos is an automated intraday trading system built by Bill Dorais.

SIGNAL FLOW:
TradingView (Pine Script alerts) → POST /webhook → Signal Router → Alpaca broker → fill logged

STRATEGY NAMING CONVENTION:
{TICKER}_CAM_{BREAKOUT|REVERSAL}_{R3S3|R4S4}_V02_5MIN
Example: PLTR_CAM_BREAKOUT_R4S4_V02_5MIN
- R3S3 / R4S4 = Camarilla level sets used for entry triggers
- BREAKOUT = entry on level break with momentum
- REVERSAL = entry on level rejection (mean-reversion)
- V02 = strategy version; 5MIN = bar timeframe

THE ACCOUNTS — a symmetric 2x2 by entry mechanism (each full-sample FARM refines into a
curated top-N account):
1. TV Farm (account 1): ALL strategies run here via TradingView bar-close entries, ~100+
   active pipelines. Full-sample audition pool + selection source for TV Refined.
2. TV Refined (account 2, alpaca-paper-2): TOP-20 only, selected from TV Farm. TV entries.
   THIS IS THE PRIMARY ACCOUNT UNDER REVIEW.
   - Daily 4:15 PM ET refresh selects top-20 by composite score:
     Sharpe 35% + Profit Factor 30% + Win Rate 20% + Trades 15%
   - 20-day rolling lookback with 10-day recency blend (60/40)
   - Min 5 trades to be eligible; 3+ consecutive losses = auto-demoted
   - Will transition to LIVE trading (PDT $25k floor no longer required — Alpaca moved to
     an intraday-margin model; ~$25k is now a comfort choice, not a regulation)
5. Kairos Farm (account 5): the engine-entry twin of TV Farm — ALL strategies via the
   server-side Kairos engine. Full-sample pool + selection source for Kairos Refined.
3. Kairos Refined (account 3): the engine-entry curated book. It trades the top strategies,
   but the Kairos engine generates the entries itself (fresh-cross buffered breakouts;
   confirmed reversal rejects with wick >= 0.25*ATR) instead of relying on TradingView alerts
   — entering a touch earlier on breakouts. TV Refined vs Kairos Refined is the head-to-head:
   do server-side entries beat TV entries? Earlier sim work was a mirage; the real edge is
   modest and per-share, so judge acct3-vs-acct2 on a multi-week trend, not single days.
4. Crew Paper (account 4): trades only the picks wired from this advisor's Next-Month card.

STOP SYSTEM (6 layers, first to fire wins):
1. Alpaca broker trailing stop — placed immediately after entry fill
   - Configured per pipeline in Exit Params node (trail %, trigger %, max hold)
   - REVERSAL strategies: trigger 0.1% (wait for profit before trail activates)
   - BREAKOUT strategies: immediate trail (no trigger)
2. Max Hold Timer — Kairos closes position after N minutes (default 15m) regardless of trail
3. Kairos Trail-Price Backup — catches Alpaca paper stop execution failures (3s poll)
4. Per-Position Hard Stop — MAX_POSITION_LOSS dollar/percent limit
5. TV EXIT Signal — suppressed when broker trail is active
6. Daily Max Loss Halt — closes everything if day P&L hits MAX_DAILY_LOSS

REPLAY / SWEEP:
The Replay page re-simulates exits on real fills using 1-min bars. Sweep mode
grid-searches trail% and trigger% combinations per strategy type to find optimal
parameters. Results are saved to the weekly journal as "sweep snapshots."

SIZING:
Refined score bands: ≥80 → $5k/trade, ≥65 → $3k, ≥50 → $1.5k, else $500
(pre-live target sizing for ~$25k equity)
"""

        advisor = Agent(
            role="Professional Systematic Trading Advisor",
            goal="Analyse the TV Refined account performance and deliver specific, actionable recommendations.",
            backstory=(
                "You are a seasoned systematic trading professional with 20 years of "
                "experience managing algorithmic strategy portfolios on US equities.\n\n"
                f"{KAIROS_SYSTEM_KNOWLEDGE}\n"
                "Your PRIMARY focus is the TV Refined account (account 2), but you ALSO directly "
                "analyse the Kairos Refined book (account 3) — its own per-strategy leaderboard, "
                "not just the aggregate head-to-head — since those server-side entries are the "
                "live experiment. When the Kairos Refined book has enough trades, call out which Kairos "
                "ENTRIES (by strategy) are pulling their weight vs which are bleeding, and how "
                "that squares with the same strategy on the TV Refined book. "
                "You are rigorous about sample size — you don't change parameters based on "
                "two trades. You understand regime effects: reversals thrive in ranging "
                "markets, breakouts in trending ones. A 44% win rate with PF > 2 is a GOOD "
                "system. You give direct, numbered recommendations naming specific strategies. "
                "'Hold steady' is valid when the data doesn't support a change."
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

        # ── Kairos/TV balance directive (user knob) ───────────────────────────
        # Controls how hard to push engine ([Kairos]) entries. The Kairos Farm
        # (acct5) leaderboard is a first-class SOURCING pool here — top Kairos Farm
        # names can win [Kairos] slots on their own, not just corroborate acct3.
        _kt = str(kairos_target or "9").lower()
        if _kt == "max":
            balance_block = (
                "KAIROS BALANCE = MAX: maximise [Kairos] picks. Take EVERY name from the top of the "
                "KAIROS FARM (acct5) leaderboard + Kairos Refined (acct3) that clears the bar (net-positive, "
                "≥5 farm trades, sane PF) as a first-class [Kairos] pick — the Kairos Farm is the audition "
                "pool, so a strong farm name with thin/no acct3 trades STILL qualifies (mark '(farm-backed)'). "
                "Only fill remaining slots with [TV] names the Kairos side can't cover. Ignore the 'farm-only "
                "cap' for Kairos here — tapping the Kairos Farm edge is the whole point.")
        elif _kt == "5":
            balance_block = (
                "KAIROS BALANCE = REFINED-LED: at LEAST 5 of 18 must be [Kairos]; TV may lead the rest. "
                "Source the Kairos picks from the strongest KAIROS FARM (acct5) + Kairos Refined (acct3) names, "
                "farm-backed allowed.")
        else:  # "9" balanced (default)
            balance_block = (
                "KAIROS BALANCE = BALANCED: TARGET ~9 of 18 tagged [Kairos] for a roughly EVEN TV/Kairos split. "
                "This is the priority instruction — draw the ~9 Kairos picks from the TOP of the KAIROS FARM "
                "(acct5) leaderboard AND Kairos Refined (acct3): a strong Kairos Farm name is a FIRST-CLASS "
                "[Kairos] pick in its own right, even with thin/no acct3 trades yet (mark '(farm-backed)'). "
                "Do NOT fall short of ~9 Kairos just because the acct3 sample is thin — that is exactly what the "
                "Kairos Farm full sample is for — EXCEPT where the KAIROS-TAG GUARDRAIL blocks it (a deep net-negative "
                "Kairos Farm sample with no credible positive acct3 record → tag [TV] instead, even if that lands under 9). "
                "Prefer the band/kind (R3S3/R4S4, breakout/reversal) that tops the Kairos Farm leaderboard.")

        # ── Single task — all data embedded directly ──────────────────────────

        analysis_task = Task(
            description=(
                f"Here is the Kairos account data"
                + (f" for: {period}" if period else "") + ":\n\n"
                + (f"{gate_block}\n\n" if gate_block else "")
                + (f"{scorecard_block}\n\n" if scorecard_block else "")
                + (f"{book_block}\n\n" if book_block else "")
                + f"{strategy_block}\n\n"
                f"{engine_strat_block}\n\n"
                + "FARM LEADERBOARDS BELOW are the FULL-SAMPLE audition pools the Refined books are "
                "drawn from — use them to corroborate thin Refined samples, NOT to override Refined "
                "performance. TV Farm (acct1) backs [TV] picks; Kairos Farm (acct5) backs [Kairos] picks.\n\n"
                + f"{farm_strat_block}\n\n"
                f"{kairos_farm_strat_block}\n\n"
                + "SNAPSHOT LEADERBOARD RANKINGS BELOW are the composite-SCORE order the user watches on "
                "the Analysis page — the SAME ranking the Refined books trade. Draw your [TV] picks in the "
                "TV Refined snapshot's rank order and your [Kairos] picks in the Kairos Refined snapshot's "
                "rank order (top-down), THEN apply the guardrail, sample floor, incumbency and balance on "
                "top. Do NOT invent a different order from raw farm P&L when a snapshot rank exists.\n\n"
                + f"{tv_snap_block}\n\n"
                f"{kairos_snap_block}\n\n"
                f"{card_block}\n\n"
                f"{journal_block}\n\n"
                + (f"{stops_block}\n\n" if stops_block else "")
                + (f"{engine_block}\n\n" if engine_block else "")
                + (f"KNOWLEDGE BASE — Camarilla theory and validated trading observations:\n\n{knowledge_block}\n\n" if knowledge_block else "")
                + (f"For historical context, here are your previous advisory reports:\n\n{prev_block}\n\n" if prev_block else "")
                + "Based on all of this, deliver the report. START with this decision card "
                "(Markdown table) as the VERY FIRST thing, before any section — fill every cell "
                "from the data above, never invent; write 'insufficient data' if a cell lacks it:\n\n"
                "## 📋 Next Month — Crew Paper Account\n"
                "| Decision | Recommendation |\n|---|---|\n"
                "| Top 18 to run | eighteen strategy names, RANKED PRIMARILY by the SNAPSHOT LEADERBOARD RANKINGS "
                "(the composite-SCORE order the user watches): take [TV] picks in the TV Refined snapshot's rank "
                "order and [Kairos] picks in the Kairos Refined snapshot's rank order, top-down, then apply the "
                "rules below. Tag each by its BEST side. "
                "SOURCING RULES: (a) names positive on BOTH TV Refined (acct2) and Kairos Refined (acct3) are first-class picks; "
                "(b) a name positive on only ONE book is an ENTRY-SPECIFIC bet — the entry mechanism is part of the "
                "strategy (the two books have shown OPPOSITE edges on the same names) — and MUST carry that book's entry tag. "
                "(c) SAMPLE FLOOR — an entry-specific bet needs ≥8 trades, BUT that sample may be met on the matching "
                "FARM leaderboard (TV Farm acct1 for [TV] picks, Kairos Farm acct5 for [Kairos] picks), since the farm is "
                "the full-sample audition pool the Refined book is a curated subset of. (This floor is deliberately set to 8, "
                "not 15, so the young Kairos Farm audition pool can back [Kairos] picks while it accrues history — 8+ trades "
                "on a positive-PF farm record is enough to qualify.) So a name with a THIN Refined sample "
                "but a deep, positive farm record on the SAME entry mechanism can still qualify — mark it '(farm-backed)' in "
                "its Detail line. REFINED STAYS PRIMARY: never promote a name that is NEGATIVE on its Refined book on the "
                "strength of the farm alone; use the farm only to clear the sample floor, corroborate a thin Refined edge, "
                "and break ties. A [TV] name with NO TV Refined trades is a farm-only audition — at most 2 of the 18, "
                "flagged as unproven on the book. (Kairos farm-only names are NOT capped that way — they are governed by "
                "the KAIROS BALANCE directive below, since sourcing Kairos picks from the Kairos Farm is deliberate.) "
                "(d) KAIROS-TAG GUARDRAIL — a HARD rule that OVERRIDES the balance target below: do NOT tag a pick "
                "[Kairos] when the Kairos side's most CREDIBLE sample is a loser. Concretely — if Kairos Farm (acct5, full "
                "sample) is net-NEGATIVE with ≥5 trades (e.g. PF < 1), a positive Kairos Refined (acct3) reading overrides "
                "it ONLY when acct3 itself has ≥5 trades; a 1–4 trade acct3 blip does NOT justify [Kairos] against a deep "
                "negative farm sample. When the guardrail blocks [Kairos], tag [TV] if the TV side is positive, else drop "
                "the name. TIE-BREAK: when a name could go either book, tag it for the side whose farm/refined record is the "
                "STRONGER, more-credible one (bigger positive sample) — never flip to [Kairos] purely to hit the count. "
                "Falling SHORT of the Kairos balance target with HONEST tags is REQUIRED over forcing a [Kairos] tag the "
                "deeper Kairos data contradicts. "
                f"PER-BOOK QUOTA / BALANCE: {balance_block} (This target yields to the (d) guardrail above.) At least 5 of "
                "the 18 must be earned on EACH book so both entry mechanisms are represented — never let one book dominate all 18. "
                "CHURN GUARD: this is a mostly-stable book. A strategy currently wired to Crew Paper (see the CURRENT CREW "
                "PAPER BOOK block) that is net-positive KEEPS its slot by default; only DROP an incumbent if it's a clear "
                "bleeder, and only ADD a challenger over an incumbent when it's CLEARLY better (not a marginal score edge). "
                "When the current book is healthy, keep the roster mostly intact rather than reshuffling on one month's noise. "
                "Tag each pick's ENTRY as [TV] (earned on TV Refined) or [Kairos] (earned on Kairos Refined) — the tag is REAL: "
                "the wire button sets that rule's entry source per pick. "
                "CRITICAL — THE LEADING TAG IS WHAT GETS WIRED: the [TV]/[Kairos] tag immediately after the strategy name "
                "MUST be your FINAL decision AFTER applying the (d) guardrail. If the guardrail flips a Kairos candidate to "
                "TV, write the leading tag as [TV] — do NOT write '[Kairos]' and then argue '[TV]' in the reasoning. Never "
                "let the leading tag disagree with your conclusion; the parser reads the leading tag (and any explicit "
                "'TAG [X]' phrase), not your prose. "
                "Tag each pick's SIDE long / short / both. A strategy may earn a slot on its single-side record — use "
                "the SIDE-GATED CANDIDATES in the card inputs (best_side score vs both-sides score): if a name scores "
                "clearly higher gated to one side, include it tagged that side. The side tag is a REAL gate "
                "(long = long-only, short = short-only). "
                "FORMAT: put each numbered pick on its OWN line, separated by <br> (one ticker per line) — e.g. "
                "`1. SMH_CAM_... — SHORT-only [Kairos] (...)<br>2. SPY_CAM_... — both [TV] (...)<br>3. ...`. "
                "HARD FORMAT RULES for this row (the wire button reads THIS row literally — obey exactly):\n"
                "  • EXACTLY 18 lines, numbered 1 through 18, each number used ONCE. No 15b, no two '10.'s, no gaps.\n"
                "  • Each line = ONE final strategy: `N. SLUG — <side> [<book>] (brief why)`. One slug per line.\n"
                "  • Each strategy SLUG appears AT MOST ONCE across all 18 — a slug is ONE Crew Paper rule with ONE "
                "entry source, so it CANNOT be listed as both [TV] and [Kairos]. If a name is strong on BOTH books, "
                "pick the SINGLE book with the stronger/more-credible record (bigger positive sample) and use that one "
                "slot; do NOT spend two of the 18 slots on the same slug.\n"
                "  • Do your DROP/REPLACE and guardrail reasoning SILENTLY; this row shows only the WINNERS. "
                "NEVER write a rejected strategy here, and NEVER write the words DROP / REPLACE / 'flip to' / a "
                "second [tag] in a line. If a candidate loses to a replacement, only the REPLACEMENT appears — at "
                "the loser's slot, renumbered so 1–18 stays contiguous.\n"
                "  • The leading [TV]/[Kairos] tag IS the wired entry — it must already be the post-guardrail FINAL "
                "answer. Do not write one tag then argue another.\n"
                "  • SELF-CHECK before you emit this row: count your lines (must be 18), confirm numbers 1–18 each "
                "appear once, confirm no slug repeats, confirm no line contains DROP/REPLACE or two tags. Fix it "
                "BEFORE writing. All KEEP/DROP/ADD narrative belongs ONLY in the '🔄 Changes vs the Current Book' "
                "table below — never in this row. |\n"
                "| Sizing | Equal risk OR Scaled-by-score — one-clause why (equal risk is preferred for a fresh test until the score proves forward edge) |\n"
                "| Day-type gate | Read the LIVE SYSTEM GATE STATE block — report the ACTUAL state (ON/OFF + which books + allowed days). Do NOT claim it is missing or recommend building it if it is listed ON there. Only suggest a CHANGE to its threshold if the side×day-type data supports one. |\n"
                "| Entries | Default for UNTAGGED picks only: TV Refined OR Kairos Refined — per the TV-vs-Kairos read. Per-pick [TV]/[Kairos] tags override this default. |\n"
                "| Best indices | top index tickers · indices-only P&L from TV Farm: $X (from the card inputs) |\n\n"
                "IMMEDIATELY AFTER the card, output a **### 🔄 Changes vs the Current Book** table so the trader can see "
                "exactly what would change before deciding to re-wire. Compare the CURRENT CREW PAPER BOOK (every strategy "
                "wired right now, with its live P&L since its wire date) against your Top 18. Columns: "
                "Strategy | Entry [TV]/[Kairos] | Live P&L now | Action. Mark every currently-wired strategy KEEP or DROP "
                "(DROP only clear bleeders — give the $ reason), and every new pick ADD. Do NOT list unchanged picks as "
                "changes. End with a one-line tally: 'KEEP N · ADD N · DROP N'. If the current book is healthy (most wired "
                "strategies net-positive), bias toward KEEP, keep the change count LOW, and say so in one line — a winning "
                "book should not be churned. If a re-wire isn't worth it this month, say that explicitly.\n\n"
                "AFTER the Changes table, output the AUTHORITATIVE machine-readable wire list — a fenced code block the "
                "wire button parses LITERALLY. The prose card above is for the human; THIS block is what actually gets "
                "wired, so it must be clean. Format EXACTLY (open with a line that is only ```picks and close with a line "
                "that is only ```):\n"
                "```picks\n"
                "SLUG | side | book\n"
                "SLUG | side | book\n"
                "... (exactly 18 lines) ...\n"
                "```\n"
                "where side is one of long|short|both and book is one of TV|Kairos — e.g. "
                "`NVDA_CAM_BREAKOUT_R3S3_V02_5MIN | both | TV`. RULES: EXACTLY 18 data lines; each is a FINAL "
                "post-guardrail pick; one full strategy slug per line; NO numbering, NO reasoning/notes, NO DROP/REPLACE, "
                "NO duplicate slugs (a slug appears AT MOST ONCE — never the same name as both | TV and | Kairos; one "
                "slug = one Crew Paper rule = one entry source). This block MUST match the Top-18 card row's final picks — if they ever disagree, "
                "THIS block is what wires. Emit it every run.\n\n"
                "Then continue with the detailed sections:\n\n"
                "0. **Last Picks — Grade Yourself** — If a PREVIOUS PICKS SCORECARD block is "
                "present above, review it FIRST and let it shape this month's Top 10: state "
                "plainly how the last picks did out-of-sample (X/N traded, Y positive, net $Z), "
                "name the picks that validated vs the ones that flopped, and say whether the "
                "selection method (best-side composite ranking) is showing forward edge or "
                "picking in-sample flukes. Drop or down-weight repeat offenders; keep validated "
                "picks. If there is no scorecard (first run, or picks never wired/traded), say "
                "so in one line and move on. ALSO review the CURRENT CREW PAPER BOOK block if "
                "present — that is every strategy wired to Crew Paper right now and how it is "
                "actually doing since its wire date (the scorecard only covers the last card). "
                "A strategy earning its keep there should stay in this month's Top picks — if you "
                "drop it, it gets removed from Crew Paper on the next wire, so only drop the "
                "bleeders. Explicitly name any live-book earner you are choosing to KEEP and any "
                "bleeder you are cutting.\n\n"
                "1. **Portfolio Health** — Is the Refined top-20 earning its keep? "
                "What does the PF and Sharpe say about real edge vs. luck?\n\n"
                "2. **Strategy Calls** — Name 2-3 to promote/add to Refined and 2-3 to pause "
                "or demote. Give specific reasons tied to numbers. Include a **Side-gated callouts** "
                "line: use BOTH the SIDE-GATED CANDIDATES (per-strategy) AND the BAND x SIDE table "
                "(band-level, more robust) to call out any cohort you'd run one-sided (e.g. "
                "'BREAKOUT R3S3 shorts bleed −$X across the band — tag those picks LONG-only'). "
                "IMPORTANT distinctions. (a) If ONE side of a specific band bleeds, that's a "
                "strategy fix (side-gate it). But if the SAME side bleeds across ALL bands AND "
                "across the books, that is REGIME (e.g. shorts lost because the tape rose this "
                "month), NOT a persistent strategy edge — do NOT hard-gate a whole side on one "
                "month's direction; note it as regime and let the day-type/regime filter handle "
                "it. Use the SIDE x DAY-TYPE data to settle regime-vs-structural: a side that "
                "bleeds on EVERY day type is regime (leave it, or defer to a market-regime "
                "filter); a side that bleeds ONLY on specific day types is structural — the "
                "day-type gate can target exactly those days, so recommend a gate change rather "
                "than a blanket side ban. (b) Bands carry their KIND (BREAKOUT R4S4 vs REVERSAL R4S4). If one kind "
                "bleeds while the same level's other kind holds, cut the KIND, not the level — "
                "recommending 'pause R4S4' when only its reversals bleed would kill working "
                "breakouts. (c) The three books are separate evidence: a pattern in all three is "
                "far stronger than one book's, and books can hold OPPOSITE edges (TV Refined and "
                "Kairos Refined have before) — never carry a finding from one book to another "
                "without its own numbers. Always keep the hindsight caveat (a side's edge can "
                "flip).\n\n"
                "3. **Stop & Parameter Check** — Given the regime tags and any sweep data in "
                "the journal, are current trailing stops appropriate? Reference specific sweep "
                "results and the trader's own notes if they offer relevant observations.\n\n"
                "4. **Kairos Refined vs TV Refined** — Using BOTH the KAIROS REFINED STRATEGY LEADERBOARD "
                "(acct 3 per-strategy, engine entries) and the head-to-head PILOT data, is the Kairos Refined book "
                "(acct 3, server-side engine entries) beating, matching, or lagging the TV Refined account (acct 2)? "
                "Name the specific Kairos ENTRIES (by strategy) that are working vs the ones "
                "dragging the book, and compare each to the same strategy on acct 2 where possible "
                "(breakouts likely lead the edge, reversals are the new/risky part). If there isn't "
                "enough data yet, say so plainly and state exactly what you'd want to see before "
                "judging — do NOT over-read a few trades or a single good/bad day.\n\n"
                "5. **Risk Observations** — Concentration, drawdown patterns, or ticker "
                "exposure worth flagging for a live account (note: leverage up to 4x intraday is "
                "now available from $2k equity, so sizing discipline matters more).\n\n"
                "6. **This Week's Focus** — One specific, testable action item. If you gave "
                "advice last week, note whether it played out and whether it should continue.\n\n"
                "Be direct. Cite strategy names and numbers. 'Hold steady' is valid when warranted."
            ),
            expected_output=(
                "A professional 6-section advisory report with specific strategy names, "
                "numbers-backed recommendations, an honest read on the engine-vs-TV pilot, "
                "and one concrete next-week action item."
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

# ── Pick scorecard — grade the last report's Top-N against reality ─────────────

def _pick_scorecard(prev_report=None):
    """Grade the LAST crew report's Top-N picks against what they actually did on
    Crew Paper (acct4) since the report was written. Picks are chosen on lookback
    data, so their forward Crew Paper record is the only honest, out-of-sample
    test of the selection method — this closes that feedback loop. Returns {}
    when there is no prior report or no parseable picks."""
    import app as _kairos
    if prev_report is None:
        try:
            conn = _kairos.get_db(); cur = conn.cursor()
            cur.execute("SELECT week, created_at, report FROM crew_reports "
                        "ORDER BY created_at DESC LIMIT 1")
            row = cur.fetchone()
            conn.close()
        except Exception:
            row = None
        if not row:
            return {}
        prev_report = {"week":       row[0] if _kairos.DATABASE_URL else row["week"],
                       "created_at": row[1] if _kairos.DATABASE_URL else row["created_at"],
                       "report":     row[2] if _kairos.DATABASE_URL else row["report"]}
    parsed = _parse_next_month_card(prev_report.get("report") or "")
    picks  = parsed.get("picks") or []
    if not picks:
        return {}
    since = (prev_report.get("created_at") or "")[:10]
    per_strat = {}
    try:
        with _kairos.app.test_client() as _c:
            d = _c.get(f"/api/alpaca/analysis?account=4&from_date={since}").get_json() or {}
            per_strat = {k.upper(): v for k, v in (d.get("per_strategy") or {}).items()}
    except Exception:
        per_strat = {}
    rows, traded, positive, total = [], 0, 0, 0.0
    for p in picks:
        s = per_strat.get((p.get("strategy") or "").upper())
        if s and (s.get("trades") or 0) > 0:
            pnl = round(s.get("total_pnl", 0) or 0, 2)
            rows.append({"strategy": p["strategy"], "side": p.get("side", "both"),
                         "trades": s.get("trades", 0), "pnl": pnl,
                         "win_rate": s.get("win_rate", 0)})
            traded   += 1
            positive += 1 if pnl > 0 else 0
            total    += pnl
        else:
            rows.append({"strategy": p["strategy"], "side": p.get("side", "both"),
                         "trades": 0, "pnl": None, "win_rate": None})
    return {"report_week": prev_report.get("week"), "since": since,
            "n_picks": len(picks), "n_traded": traded, "n_positive": positive,
            "total_pnl": round(total, 2), "picks": rows,
            "caveat": "FORWARD / out-of-sample: these picks were chosen on lookback data; "
                      "this is how they actually performed on Crew Paper after wiring."}


@crew_bp.route("/api/crew/scorecard")
def api_crew_scorecard():
    """Standalone view of the pick scorecard (also fed into reports + chat tool)."""
    sc = _pick_scorecard()
    if not sc:
        return jsonify({"error": "No previous crew report with parseable picks"}), 404
    return jsonify(sc)


def _crew_book_scorecard():
    """How the strategies CURRENTLY wired to Crew Paper (acct4) are actually doing,
    each measured since it was wired. Sourced from the LIVE routing rules — the
    ground truth of what's trading — not from a report's text, so it reflects manual
    edits and the wire reconcile and covers every wired strategy (the pick scorecard
    only sees the last report's card, so it missed strategies that accumulated across
    months). Returns {} if nothing is wired to acct4.

    A rule's created_at is its wire date and survives upserts (only `nodes` is
    updated on re-wire), so a carried-forward strategy keeps its full track record.
    A strategy has no acct4 fills before it was wired, so a single from_date at the
    earliest wire date yields each strategy's true since-wiring P&L."""
    import app as _kairos
    wired, enabled_of = {}, {}   # strat -> earliest wire date (YYYY-MM-DD)
    try:
        conn = _kairos.get_db(); cur = conn.cursor()
        cur.execute("SELECT enabled, nodes, created_at FROM routing_rules")
        rows = cur.fetchall()
        conn.close()
    except Exception:
        return {}
    for r in rows:
        enabled = r[0] if _kairos.DATABASE_URL else r["enabled"]
        raw     = r[1] if _kairos.DATABASE_URL else r["nodes"]
        created = r[2] if _kairos.DATABASE_URL else r["created_at"]
        try:    nodes = json.loads(raw) if isinstance(raw, str) else (raw or [])
        except Exception: nodes = []
        if not any(n.get("type") == "broker"
                   and (n.get("value") or "").lower() in ("alpaca-paper-4", "alpaca-live-4")
                   for n in nodes):
            continue
        c = (created or "")[:10]
        for n in nodes:
            if n.get("type") == "strategy" and n.get("value"):
                s = n["value"].strip().upper()
                if s not in wired or (c and c < wired[s]):
                    wired[s] = c
                enabled_of[s] = enabled_of.get(s, False) or bool(enabled)
    if not wired:
        return {}
    since = min([c for c in wired.values() if c], default="") or None
    per_strat = {}
    try:
        with _kairos.app.test_client() as _c:
            qs = "account=4" + (f"&from_date={since}" if since else "")
            d = _c.get(f"/api/alpaca/analysis?{qs}").get_json() or {}
            per_strat = {k.upper(): v for k, v in (d.get("per_strategy") or {}).items()}
    except Exception:
        per_strat = {}
    out, traded, positive, total = [], 0, 0, 0.0
    for s in wired:
        st = per_strat.get(s)
        if st and (st.get("trades") or 0) > 0:
            pnl = round(st.get("total_pnl", 0) or 0, 2)
            out.append({"strategy": s, "since": wired[s], "enabled": enabled_of.get(s, True),
                        "trades": st.get("trades", 0), "pnl": pnl, "win_rate": st.get("win_rate", 0)})
            traded += 1; positive += 1 if pnl > 0 else 0; total += pnl
        else:
            out.append({"strategy": s, "since": wired[s], "enabled": enabled_of.get(s, True),
                        "trades": 0, "pnl": None, "win_rate": None})
    out.sort(key=lambda r: (r["pnl"] is None, r["pnl"] if r["pnl"] is not None else 0))  # worst first, untraded last
    return {"n_wired": len(wired), "n_traded": traded, "n_positive": positive,
            "total_pnl": round(total, 2), "since": since, "picks": out,
            "caveat": "LIVE Crew Paper book — the strategies wired to acct4 right now, each since "
                      "its wire date. This is what is actually trading; keep the earners, cut the "
                      "bleeders. Distinct from the pick scorecard (which grades one report's picks)."}


@crew_bp.route("/api/crew/book")
def api_crew_book():
    """Standalone view of the live Crew Paper book (also fed into reports + chat)."""
    bk = _crew_book_scorecard()
    if not bk:
        return jsonify({"error": "No strategies are wired to Crew Paper (acct4)."}), 404
    return jsonify(bk)


def _gate_state():
    """The live gate configuration, read from the app so the crew reports system
    state as FACT instead of guessing. The crew once wrote 'the system does not yet
    have an automated pre-market CPR filter' while the day-type gate was live on
    every book — this block is the ground truth that stops that confabulation."""
    import app as _kairos
    meta = _kairos.ACCOUNT_META
    tag_label = {m.get("tag"): m.get("label", n) for n, m in meta.items()}

    def _labels(tags):
        return sorted(tag_label.get(t, t) for t in (tags or []))

    dt_on   = bool(getattr(_kairos, "DAYTYPE_GATE_ENABLED", False))
    rdt_on  = bool(getattr(_kairos, "DAYTYPE_REVERSAL_GATE_ENABLED", False))
    dt_acc  = getattr(_kairos, "DAYTYPE_GATE_ACCOUNTS", set())
    rdt_acc = getattr(_kairos, "DAYTYPE_REVERSAL_GATE_ACCOUNTS", set())
    rev_by  = getattr(_kairos, "_REVERSAL_SIDE_BY_TAG", {})

    books = []
    for n in ("2", "3", "4"):          # the curated books the crew picks for
        m = meta.get(n) or {}
        tag = m.get("tag")
        try:    hs, he = _kairos._account_hours(tag)
        except Exception: hs, he = "", ""
        books.append({
            "label": m.get("label", n), "tag": tag,
            "breakout_daytype_gated": dt_on and tag in dt_acc,
            "reversal_daytype_gated": rdt_on and tag in rdt_acc,
            "reversal_policy": rev_by.get(tag) or "free",   # "off" / "long" / "short" / "free"
            "hours": (f"{hs}-{he} ET" if hs and he else "all day"),
            "profit_lock": bool(m.get("profit_lock")),
            "daily_loss_guard": bool(m.get("daily_loss_guard")),
        })

    return {
        "daytype_breakout": {
            "on": dt_on,
            "ok_days": sorted(getattr(_kairos, "DAYTYPE_GATE_BREAKOUT_OK_DAYS", set())),
            "accounts": _labels(dt_acc),
            "note": "This IS an automated pre-market filter: the day type is computed from the "
                    "prior day's Camarilla CPR width (known before the open), and breakout entries "
                    "are blocked except on the allowed day type.",
        },
        "daytype_reversal": {
            "on": rdt_on,
            "ok_days": sorted(getattr(_kairos, "DAYTYPE_REVERSAL_OK_DAYS", set())),
            "accounts": _labels(rdt_acc),
        },
        "books": books,
    }


@crew_bp.route("/api/crew/gate_state")
def api_crew_gate_state():
    """Standalone view of the live gate configuration (also fed into reports)."""
    return jsonify(_gate_state())


# ── Chat tools — let the advisor pull LIVE Kairos data on demand ───────────────

_CREW_TOOLS = [
    {
        "name": "engine_vs_tv",
        "description": "Head-to-head realized P&L: Kairos Refined (account 3, server-side "
                       "entries) vs TV Refined (account 2) over the last N days — daily rows + "
                       "cumulative totals + per-account win rate. Use for 'is the engine beating TV'.",
        "input_schema": {"type": "object", "properties": {
            "days": {"type": "integer", "description": "lookback days (default 30)"}}},
    },
    {
        "name": "day_recap",
        "description": "One specific day's trades comparing TV Refined (acct2) vs Kairos Refined "
                       "(acct3): per-account P&L, win rate, and the round-trip list. Use for "
                       "questions about a particular day ('how did Wednesday go').",
        "input_schema": {"type": "object", "properties": {
            "date": {"type": "string", "description": "YYYY-MM-DD"}}, "required": ["date"]},
    },
    {
        "name": "strategy_stats",
        "description": "Per-strategy leaderboard + overall stats (trades, win rate, PF, P&L) for "
                       "an account over an optional date range.",
        "input_schema": {"type": "object", "properties": {
            "account": {"type": "string", "enum": ["1", "2", "3"],
                        "description": "1=TV Farm, 2=TV Refined, 3=Kairos Refined (default 2)"},
            "from_date": {"type": "string", "description": "YYYY-MM-DD (optional)"},
            "to_date": {"type": "string", "description": "YYYY-MM-DD (optional)"}}},
    },
    {
        "name": "open_positions",
        "description": "Current open positions for an account (symbol, qty, unrealized P&L).",
        "input_schema": {"type": "object", "properties": {
            "account": {"type": "string", "enum": ["1", "2", "3"], "description": "default 2"}}},
    },
    {
        "name": "engine_fills",
        "description": "The Kairos engine's armed entries (fills) log — each with ticker, side, "
                       "kind (breakout/reversal), level, intended order price, ACTUAL fill price, "
                       "slippage (fill vs intended), and trigger reason. Use for slippage questions "
                       "like 'worst slippage on breakouts this week' or 'how clean were the fills'.",
        "input_schema": {"type": "object", "properties": {
            "from_date": {"type": "string", "description": "YYYY-MM-DD (optional)"},
            "to_date": {"type": "string", "description": "YYYY-MM-DD (optional)"},
            "kind": {"type": "string", "enum": ["breakout", "reversal"], "description": "optional filter"}}},
    },
    {
        "name": "rank_compare",
        "description": "Backtest the leaderboard SELECTION METHOD on TV Farm (acct1 — the "
                       "audition pool the Refined leaderboard is built from) over N days: ranks "
                       "strategies by RAW P&L vs by the live COMPOSITE SCORE (Sharpe 35/PF 30/Win "
                       "20/Trades 15), takes the top N of each, and reports each set's combined "
                       "P&L + the overlap + which strategies differ. Use for 'would trading the "
                       "top 20 by P&L have beaten the leaderboard?'. CRITICAL: both rankings are "
                       "computed AND measured on the SAME window, so by-P&L is tautologically >= "
                       "by-score in-sample — that is NOT evidence it's better going forward "
                       "(classic overfit). Read the 'caveat' field and say so.",
        "input_schema": {"type": "object", "properties": {
            "days": {"type": "integer", "description": "lookback days (default 30)"},
            "n": {"type": "integer", "description": "top N (default 20)"}}},
    },
    {
        "name": "side_breakdown",
        "description": "Long vs Short performance for an account over a date range: overall per "
                       "side, band×side, side×day-type, PER-STRATEGY×side, AND side_gated_candidates "
                       "— strategies whose best single side scores higher than trading both sides "
                       "(i.e. would rank better if gated long- or short-only). Use for 'what if I "
                       "only traded shorts' and to find strategies that would make the top 10 if "
                       "side-gated. Hindsight caveat: a side's past edge can flip — 'shorts-only "
                       "would have made $X' is what happened, not a forward guarantee.",
        "input_schema": {"type": "object", "properties": {
            "account": {"type": "string", "enum": ["1", "2", "3"],
                        "description": "1=TV Farm, 2=TV Refined, 3=Kairos Refined (default 3)"},
            "from_date": {"type": "string", "description": "YYYY-MM-DD (optional)"},
            "to_date": {"type": "string", "description": "YYYY-MM-DD (optional)"}}},
    },
    {
        "name": "band_fill_quality",
        "description": "Compare ONE band across TV Refined (acct2, TV entries) vs Kairos Refined (acct3, "
                       "engine entries): trades, win%, P&L on each, PLUS the engine's measured "
                       "slippage on that band's fills. Use to confirm whether the server-side "
                       "engine is DEGRADING a band's edge that TV captures cleanly (acct3 P&L far "
                       "below acct2 + adverse slippage = the engine entry is the culprit). Band "
                       "must be: breakout_r3s3 | breakout_r4s4 | reversal_r3s3 | reversal_r4s4.",
        "input_schema": {"type": "object", "properties": {
            "band": {"type": "string",
                     "description": "breakout_r3s3 | breakout_r4s4 | reversal_r3s3 | reversal_r4s4"}},
            "required": ["band"]},
    },
    {
        "name": "pick_scorecard",
        "description": "Grade the LAST crew report's Top-N picks against their ACTUAL Crew Paper "
                       "(acct4) results since that report was written — the out-of-sample test of "
                       "the selection method. Returns per-pick trades/P&L/win% plus a summary "
                       "(traded X/N, Y positive, net $Z). ALWAYS pull this before making new "
                       "next-month picks, and use it for 'how did my last picks do'.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "cross_account",
        "description": "Cross-validate strategies across TV Refined (acct2, TV entries) and Kairos Refined "
                       "(acct3, engine entries): returns strategies POSITIVE on BOTH books (the "
                       "most robust set — works on TV and the engine), the DIVERGENT ones "
                       "(positive on one, negative on the other), and an INDEX rollup (SPY/QQQ/"
                       "IWM/SMH P&L+win% per account). Use for 'which strategies work in both' and "
                       "'how did the indices do on their own', and to seed a curated set for a new "
                       "account. Optional date range.",
        "input_schema": {"type": "object", "properties": {
            "from_date": {"type": "string", "description": "YYYY-MM-DD (optional)"},
            "to_date": {"type": "string", "description": "YYYY-MM-DD (optional)"}}},
    },
    {
        "name": "entry_test",
        "description": "Compare BREAKOUT entry TIMING over a date range, per-share, holding each "
                       "strategy's live exits fixed so only the entry varies: confirmed (wait for a "
                       "close beyond the level), immediate (fill at the level touch), buffered "
                       "(level +/- buffer), retest (pullback to the level). Plus a buffer sweep. Use "
                       "this to diagnose the immediate-loser problem and RECOMMEND how the Kairos "
                       "ENGINE should enter breakouts (e.g. 'confirmed beats immediate by $X/share — "
                       "make the engine wait for the close' or 'a 0.15 buffer cuts the false breaks'). "
                       "Breakouts only (reversals enter on rejection). NOTE: this is advisory — the "
                       "engine buffer/entry is set via ENGINE_PILOT_BUFFER + routing, not the wire "
                       "button. Uses acct1 (TV Farm, full sample) by default.",
        "input_schema": {"type": "object", "properties": {
            "account": {"type": "string", "enum": ["1", "2"],
                        "description": "setup universe: 1=TV Farm full sample (default), 2=TV Refined"},
            "buffers": {"type": "string", "description": "comma buffer sweep, e.g. '0.05,0.1,0.15,0.2'"},
            "from_date": {"type": "string", "description": "YYYY-MM-DD (optional)"},
            "to_date": {"type": "string", "description": "YYYY-MM-DD (optional)"}}},
    },
]

def _run_crew_tool(name: str, args: dict) -> str:
    """Execute a chat tool against Kairos's own internal endpoints. Returns a
    compact JSON string (bounded) for the model to read."""
    args = args or {}
    try:
        import app as _kairos
        def _get(path):
            with _kairos.app.test_client() as _c:
                return _c.get(path).get_json() or {}
        if name == "engine_vs_tv":
            days = int(args.get("days") or 30)
            d = _get(f"/api/engine_pilot/compare?days={days}")
            return json.dumps({"days": d.get("days"), "configured": d.get("configured"),
                               "tv": d.get("tv"), "engine": d.get("engine"),
                               "rows": (d.get("rows") or [])[:20]})[:6000]
        if name == "day_recap":
            date = (args.get("date") or "").strip()
            if not date:
                return "Error: date (YYYY-MM-DD) required."
            return json.dumps(_get(f"/api/engine_pilot/day_recap?date={date}"))[:6000]
        if name == "strategy_stats":
            acct = str(args.get("account") or "2")
            qs = f"account={acct}"
            if args.get("from_date"): qs += f"&from_date={args['from_date']}"
            if args.get("to_date"):   qs += f"&to_date={args['to_date']}"
            d = _get(f"/api/alpaca/analysis?{qs}")
            ps = d.get("per_strategy") or {}
            top = sorted(ps.items(), key=lambda x: x[1].get("total_pnl", 0), reverse=True)
            summ = [{"name": k, "trades": v.get("trades"), "win_rate": v.get("win_rate"),
                     "pf": v.get("profit_factor"), "pnl": v.get("total_pnl")} for k, v in top[:25]]
            return json.dumps({"account": acct, "overall": d.get("overall"), "per_strategy": summ})[:6000]
        if name == "open_positions":
            acct = str(args.get("account") or "2")
            d = _get(f"/api/alpaca/positions?account={acct}")
            return json.dumps(d.get("positions") or [])[:4000]
        if name == "engine_fills":
            d = _get("/api/engine_pilot/status")
            fd   = (args.get("from_date") or "").strip()
            td   = (args.get("to_date") or "").strip()
            kind = (args.get("kind") or "").strip().lower()
            out = []
            for f in (d.get("fills") or []):
                dt = f.get("date") or ""
                if fd and dt < fd: continue
                if td and dt > td: continue
                if kind and f.get("kind") != kind: continue
                out.append({k: f.get(k) for k in (
                    "ts", "ticker", "side", "kind", "level_name", "level", "order_px",
                    "fill_price", "fill_slip", "slippage", "qty", "reason", "ok")})
            return json.dumps({"count": len(out), "fills": out[:60]})[:6000]
        if name == "rank_compare":
            days = int(args.get("days") or 30)
            n    = int(args.get("n") or 20)
            stats = _kairos._compute_strategy_stats(days=days)   # acct1 (TV Farm) per-strategy
            if not stats:
                return json.dumps({"error": "No TV Farm (acct1) stats available."})
            max_pnl = max((s.get("total_pnl", 0) or 0) for s in stats.values())
            rows = [{
                "name": k, "pnl": round(s.get("total_pnl", 0) or 0, 2),
                "trades": s.get("trades", 0), "win_rate": s.get("win_rate", 0),
                "pf": s.get("profit_factor"),
                "score": round(_kairos._composite_score(s, max_pnl), 4),
            } for k, s in stats.items()]
            # Same eligibility the leaderboard applies: net-positive + min trades.
            elig = [r for r in rows if r["pnl"] > 0 and r["trades"] >= _kairos._REFINED_MIN_TRADES]
            by_pnl   = sorted(elig, key=lambda r: -r["pnl"])[:n]
            by_score = sorted(elig, key=lambda r: -r["score"])[:n]
            sp = {r["name"] for r in by_pnl}; ss = {r["name"] for r in by_score}
            return json.dumps({
                "account": "TV Farm (1)", "days": days, "n": n, "eligible_pool": len(elig),
                "combined_pnl_top_by_pnl":   round(sum(r["pnl"] for r in by_pnl), 2),
                "combined_pnl_top_by_score": round(sum(r["pnl"] for r in by_score), 2),
                "overlap": len(sp & ss),
                "only_in_pnl_rank":   sorted(sp - ss),
                "only_in_score_rank": sorted(ss - sp),
                "caveat": "IN-SAMPLE: ranked AND measured on the same window, so by-P&L is "
                          "tautologically >= by-score here. Not evidence P&L-ranking wins going "
                          "forward (overfit). The composite score trades in-sample P&L for "
                          "out-of-sample stability — fewer flukes, less promote/demote churn.",
                "top_by_pnl": by_pnl, "top_by_score": by_score,
            })[:6500]
        if name == "pick_scorecard":
            sc = _pick_scorecard()
            if not sc:
                return json.dumps({"error": "No previous crew report with parseable picks yet."})
            return json.dumps(sc)[:6000]
        if name == "side_breakdown":
            acct = str(args.get("account") or "3")
            qs = f"account={acct}"
            if args.get("from_date"): qs += f"&from_date={args['from_date']}"
            if args.get("to_date"):   qs += f"&to_date={args['to_date']}"
            d = _get(f"/api/alpaca/ls_breakdown?{qs}")
            if d.get("error"):
                return json.dumps({"error": d["error"]})
            return json.dumps({
                "account": d.get("account"), "trades": d.get("trades"),
                "overall_side": d.get("overall_side"),
                "by_band_side": d.get("by_band_side"),
                "by_side_daytype": d.get("by_side_daytype"),
                "by_strategy_side": d.get("by_strategy_side"),
                "side_gated_candidates": d.get("side_gated_candidates"),
                "caveat": "Hindsight/descriptive: 'shorts-only would have made $X' is what "
                          "happened, not a forward guarantee — a side's edge can flip, and you "
                          "didn't know in advance which side would win.",
            })[:8000]
        if name == "band_fill_quality":
            band = (args.get("band") or "").strip().lower()
            kind = "breakout" if "breakout" in band else "reversal" if "reversal" in band else ""
            pair = "r3s3" if "r3s3" in band else "r4s4" if "r4s4" in band else ""
            if not kind or not pair:
                return "Error: band must be breakout_r3s3 | breakout_r4s4 | reversal_r3s3 | reversal_r4s4."
            def _agg(acct):
                d = _get(f"/api/alpaca/analysis?account={acct}")
                ps = d.get("per_strategy") or {}
                tr = wn = 0; pnl = 0.0; ns = 0
                for nm, s in ps.items():
                    u = nm.upper()
                    if kind.upper() in u and pair.upper() in u:
                        t = s.get("trades", 0) or 0
                        w = s.get("wins")
                        if w is None: w = round((s.get("win_rate", 0) or 0) / 100 * t)
                        tr += t; wn += w; pnl += s.get("total_pnl", 0) or 0; ns += 1
                return {"trades": tr, "win_rate": round(wn / tr * 100, 1) if tr else 0,
                        "pnl": round(pnl, 2), "strategies": ns}
            fills = _get("/api/engine_pilot/status").get("fills") or []
            bf = [f for f in fills if f.get("kind") == kind
                  and pair.upper() in (f.get("strategy") or "").upper()]
            slips = [f["fill_slip"] for f in bf if f.get("fill_slip") is not None]
            avg_slip = round(sum(slips) / len(slips), 4) if slips else None
            return json.dumps({
                "band": f"{kind.upper()} {pair.upper()}",
                "refined_acct2_TV": _agg(2),
                "kairos_acct3_engine": _agg(3),
                "engine_fills_with_slippage": len(slips),
                "engine_avg_slippage_per_share": avg_slip,
                "engine_worst_slippage_per_share": round(max(slips), 4) if slips else None,
                "note": "If acct3 P&L is materially below acct2 on the same band, the engine entry "
                        "is degrading the edge; positive avg slippage (filled worse than the "
                        "intended level) is the prime suspect. Slippage is per-share — multiply by "
                        "size for dollars.",
            })[:6000]
        if name == "entry_test":
            acct = str(args.get("account") or "1")
            qs = f"account={acct}"
            if args.get("from_date"): qs += f"&from={args['from_date']}"
            if args.get("to_date"):   qs += f"&to={args['to_date']}"
            qs += f"&buffers={args.get('buffers') or '0.05,0.1,0.15,0.2'}"
            d = _get(f"/api/simulate/entry_test?{qs}")
            if d.get("error"):
                return json.dumps({"error": d["error"]})
            return json.dumps({
                "account": d.get("account"), "from": d.get("from"), "to": d.get("to"),
                "n_setups": d.get("n_setups"), "n_tickers": d.get("n_tickers"),
                "rules": d.get("rules"), "buffer_sweep": d.get("sweep"),
                "note": "Per-share P&L (qty 1), breakouts only, first entry/day, live exits held "
                        "fixed. Higher total_pnl = better entry timing. If 'confirmed' or a bigger "
                        "'buffered' beats 'immediate', the engine is entering too early on false "
                        "breaks — recommend raising ENGINE_PILOT_BUFFER or waiting for a confirmed "
                        "close. Advisory only: applied via routing/env, not the wire button.",
            })[:6000]
        if name == "cross_account":
            fd = (args.get("from_date") or "").strip()
            td = (args.get("to_date") or "").strip()
            def _ps(acct):
                qs = f"account={acct}"
                if fd: qs += f"&from_date={fd}"
                if td: qs += f"&to_date={td}"
                return _get(f"/api/alpaca/analysis?{qs}").get("per_strategy") or {}
            ref = _ps(2); eng = _ps(3)
            def _row(nm):
                r = ref.get(nm) or {}; e = eng.get(nm) or {}
                return {"name": nm,
                        "refined": {"trades": r.get("trades", 0), "win_rate": r.get("win_rate", 0),
                                    "pnl": round(r.get("total_pnl", 0) or 0, 2)},
                        "kairos":  {"trades": e.get("trades", 0), "win_rate": e.get("win_rate", 0),
                                    "pnl": round(e.get("total_pnl", 0) or 0, 2)}}
            both_pos, divergent = [], []
            for nm in (set(ref) | set(eng)):
                r = ref.get(nm) or {}; e = eng.get(nm) or {}
                rp = r.get("total_pnl", 0) or 0; ep = e.get("total_pnl", 0) or 0
                if (r.get("trades", 0) or 0) and (e.get("trades", 0) or 0):   # traded on both
                    if rp > 0 and ep > 0: both_pos.append(_row(nm))
                    elif (rp > 0) != (ep > 0): divergent.append(_row(nm))
            both_pos.sort(key=lambda x: -(x["refined"]["pnl"] + x["kairos"]["pnl"]))
            divergent.sort(key=lambda x: -(x["refined"]["pnl"] + x["kairos"]["pnl"]))
            IDX = ("SPY", "QQQ", "IWM", "SMH")
            def _idx(ps):
                out = {}
                for nm, s in ps.items():
                    tk = nm.upper().split("_")[0]
                    if tk in IDX:
                        o = out.setdefault(tk, {"trades": 0, "wins": 0, "pnl": 0.0})
                        t = s.get("trades", 0) or 0; w = s.get("wins")
                        if w is None: w = round((s.get("win_rate", 0) or 0) / 100 * t)
                        o["trades"] += t; o["wins"] += w; o["pnl"] += s.get("total_pnl", 0) or 0
                return {k: {"trades": v["trades"],
                            "win_rate": round(v["wins"] / v["trades"] * 100, 1) if v["trades"] else 0,
                            "pnl": round(v["pnl"], 2)} for k, v in out.items()}
            idx_all = _idx(_ps(1))   # TV Farm — the full audition pool, for 'indices-only'
            return json.dumps({
                "window": {"from": fd or "default", "to": td or "now"},
                "works_in_both": both_pos[:20],
                "divergent_one_sided": divergent[:15],
                "indices_paper_all_acct1": idx_all,
                "indices_paper_all_total_pnl": round(sum(v["pnl"] for v in idx_all.values()), 2),
                "indices_refined_acct2": _idx(ref),
                "indices_kairos_acct3": _idx(eng),
                "note": "works_in_both = net-positive on BOTH the TV (acct2) and engine (acct3) "
                        "books — the most cross-validated, robust set. divergent = positive on one, "
                        "negative on the other (usually engine timing/fill difference). "
                        "indices_paper_all = SPY/QQQ/IWM/SMH on TV Farm (acct1, full pool) — use "
                        "for 'what if I just ran the indices'; its total is indices_paper_all_total_pnl. "
                        "Still in-sample; small per-strategy samples regress, so weight by trade count.",
            })[:7000]
        return f"Unknown tool: {name}"
    except Exception as e:
        return f"Tool error ({name}): {e}"


@crew_bp.route("/api/crew/chat", methods=["POST"])
def api_crew_chat():
    """Streaming, tool-using chat with the Systematic Trading Advisor. The advisor
    can pull LIVE Kairos data (engine-vs-TV, day recaps, strategy stats, positions)
    on demand, using the crew report as its standing context."""
    data     = request.get_json(silent=True) or {}
    report   = (data.get("report") or "").strip()
    messages = data.get("messages") or []

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not set"}), 503

    try:
        from zoneinfo import ZoneInfo as _ZI
        _today = datetime.now(_ZI("America/New_York")).strftime("%Y-%m-%d (%A)")
    except Exception:
        _today = datetime.now().strftime("%Y-%m-%d")

    system_prompt = (
        "You are a Professional Systematic Trading Advisor specialising in "
        "intraday Camarilla pivot strategies on US equities (5-min bars, Alpaca).\n\n"
        "═══ ABSOLUTE DATA-INTEGRITY RULE (read first) ═══\n"
        "Every specific figure you state — P&L, win rate, trade count, profit factor, a "
        "per-strategy or per-band number, a ranking — MUST come from either (a) a tool RESULT "
        "you received in THIS conversation, or (b) the standing report snapshot below. You may "
        "NOT invent, estimate, round-from-memory, or 'fill in' numbers. If you need current/live/"
        "fresh data and have not yet received a tool result for it, CALL THE TOOL and WAIT for the "
        "result before writing any numbers. Never write 'let me pull fresh data…' and then produce "
        "numbers in the same message without an actual tool result — emit the tool call and stop. "
        "If a tool fails or returns nothing, say so plainly; do not substitute fabricated figures. "
        "When you cite a number, it must be traceable to a tool result or the snapshot — if you "
        "can't trace it, don't say it.\n"
        "═════════════════════════════════════════════════\n\n"
        "Standing report SNAPSHOT (from the last full analysis — may be STALE; for anything "
        "'current'/'live'/'fresh' you MUST re-pull with a tool, not quote this):\n\n"
        f"{report}\n\n"
        f"Today is {_today} (US/Eastern).\n\n"
        "You have TOOLS to pull live Kairos data when the report doesn't already contain the "
        "answer — engine_vs_tv (acct3 server-side engine vs acct2 TV Refined), day_recap (a "
        "specific day's trades on both accounts), strategy_stats (per-account leaderboard), "
        "open_positions, engine_fills (the engine's entry log with actual fill prices + "
        "slippage, for fill-quality questions), rank_compare (top-N by raw P&L vs by composite "
        "score on TV Farm — for 'would the top-20-by-P&L have beaten the leaderboard'), and "
        "side_breakdown (long vs short by band/day-type — for 'what if I only traded shorts'), and "
        "band_fill_quality (one band: Refined-TV vs Kairos-engine P&L + the engine's slippage — "
        "for 'is the engine degrading R3S3 breakout'), and cross_account (strategies positive on "
        "BOTH books + divergent ones + an index SPY/QQQ/IWM/SMH rollup — for 'which strategies "
        "work in both' and 'how did the indices do'), pick_scorecard (how your LAST report's "
        "Top-N picks actually did on Crew Paper since wiring — the out-of-sample grade of your "
        "own selection method), and entry_test (compare breakout entry timing — confirmed vs "
        "immediate vs buffered vs retest + a buffer sweep — to diagnose immediate losers and "
        "recommend how the Kairos engine should enter; breakouts only, advisory). "
        "USE them whenever the user asks about anything current, specific, or "
        "not covered by the report — don't guess or say you lack data. Account map: 1=TV Farm, "
        "2=TV Refined, 3=Kairos Refined, 5=Kairos Farm (engine). Resolve relative dates ('yesterday', "
        "'Wednesday', 'this week') against today's date above.\n\n"
        "Stay honest about the engine pilot: the real edge is modest and per-share; never over-read "
        "a single day or a few trades. CRITICAL on backward-looking 'would it have been better' "
        "questions (rank_compare, side_breakdown): these are IN-SAMPLE / hindsight. Ranking by raw "
        "P&L always wins on the same window you measure (you literally picked the biggest winners) "
        "— that is OVERFITTING, not a better method. 'Shorts-only made $X' is what happened, not a "
        "forward edge. Always read and relay the tool's 'caveat' field; recommend a method only if "
        "it would plausibly hold OUT of sample, and prefer the risk-adjusted composite score for "
        "selection.\n\n"
        "FORMAT — FORWARD / NEXT-MONTH RECOMMENDATIONS: when the user asks what to run next month "
        "or for a new paper account, FIRST pull the data (pick_scorecard — grade your last picks "
        "before making new ones — then cross_account, side_breakdown, "
        "band_fill_quality, rank_compare), then LEAD your reply with exactly this card (Markdown "
        "table), every value from a tool result — never invented. Use side_breakdown's "
        "side_gated_candidates to rank by each strategy's BEST side — a strong one-sided record can "
        "earn a Top-5 slot, tagged with that side (the tag is a real long/short gate). Source picks "
        "from BOTH books: names positive on BOTH Refined and Kairos are first-class; a name positive "
        "on only ONE book is an entry-specific bet (the books have shown OPPOSITE edges on the same "
        "names) — needs ≥8 trades on that book (or its matching farm audition pool: TV Farm for [TV], "
        "Kairos Farm for [Kairos]) and MUST carry that book's entry tag, [TV] (TV Refined) "
        "or [Kairos] (Kairos Refined); the tag is real — the wire button sets that rule's entry source:\n\n"
        "## 📋 Next Month — Crew Paper Account\n"
        "| Decision | Recommendation |\n|---|---|\n"
        "| Top 10 to run | ten strategy names, each tagged long / short / both AND [TV] / [Kairos] (a name may earn its slot on its best single side or single book). FORMAT: each numbered pick on its OWN line, separated by <br> (one ticker per line). |\n"
        "| Sizing | Equal risk OR Scaled-by-score — one-line why |\n"
        "| Day-type gate | Report the ACTUAL state from the LIVE SYSTEM GATE STATE block (ON/OFF + books + allowed days); never claim it's missing if listed ON |\n"
        "| Entries | Default for untagged picks: TV Refined OR Kairos Refined (per-pick [TV]/[Kairos] tags override) |\n"
        "| Best indices | tickers · indices-only P&L from TV Farm: $X |\n\n"
        "Then a `## Detail` section with the reasoning, samples and caveats. Keep the card to "
        "those five rows; put everything else under Detail.\n\n"
        "Answer directly and specifically, citing strategy names and numbers that trace "
        "to a tool result or the snapshot (per the data-integrity rule — never invented). Be "
        "concise — the card plus a tight Detail; no filler."
    )

    # Anti-fabrication guard: if the user's message is data-seeking, FORCE a tool
    # call on the first round so the advisor can't stream a made-up answer before
    # pulling real numbers. Conceptual questions are left free (auto).
    def _last_user_text(msgs):
        for m in reversed(msgs):
            if isinstance(m, dict) and m.get("role") == "user":
                c = m.get("content")
                if isinstance(c, str): return c
                if isinstance(c, list):
                    return " ".join(b.get("text", "") for b in c if isinstance(b, dict))
        return ""
    _ut = _last_user_text(messages).lower()
    _DATA_KWS = ("pull", "current", "live", "fresh", "today", "yesterday", "week", "month",
                 "30 day", "leaderboard", "account", "acct", "stats", "p&l", "pnl", "win rate",
                 "breakdown", "short", "long", "strateg", "top ", "rank", "fill", "slippage",
                 "position", "engine", "refined", "paper all", "kairos", "day type", "day-type",
                 "recommend", "should i", "which ", "how did", "how much", "drawdown", "compare",
                 "play", "trade")
    _force_tool_round0 = any(k in _ut for k in _DATA_KWS)

    def generate():
        try:
            import anthropic as _ant
            client = _ant.Anthropic(api_key=api_key)
            convo = list(messages)
            full_text = ""
            for _round in range(5):   # cap tool-use rounds
                _stream_kwargs = dict(
                    model="claude-sonnet-4-6",
                    max_tokens=900,
                    system=system_prompt,
                    tools=_CREW_TOOLS,
                    messages=convo,
                )
                # Force at least one real tool call before any prose, for data questions.
                if _round == 0 and _force_tool_round0:
                    _stream_kwargs["tool_choice"] = {"type": "any"}
                with client.messages.stream(**_stream_kwargs) as stream:
                    for text in stream.text_stream:
                        full_text += text
                        yield f"data: {json.dumps({'text': text})}\n\n"
                    final = stream.get_final_message()
                convo.append({"role": "assistant", "content": final.content})
                if final.stop_reason != "tool_use":
                    break
                tool_results = []
                for block in final.content:
                    if getattr(block, "type", None) == "tool_use":
                        yield f"data: {json.dumps({'tool': block.name})}\n\n"
                        out = _run_crew_tool(block.name, dict(block.input or {}))
                        tool_results.append({"type": "tool_result",
                                             "tool_use_id": block.id, "content": out})
                convo.append({"role": "user", "content": tool_results})
            yield f"data: {json.dumps({'done': True, 'full_text': full_text})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


@crew_bp.route("/api/crew/reports", methods=["GET"])
def api_crew_reports():
    """List saved advisory reports (newest first) so past output can be browsed/read."""
    try:
        import app as _kairos
        conn = _kairos.get_db()
        cur  = conn.cursor()
        cur.execute("SELECT id, week, created_at, report FROM crew_reports ORDER BY created_at DESC LIMIT 50")
        rows = cur.fetchall()
        conn.close()
        out = []
        for r in rows:
            if _kairos.DATABASE_URL:
                out.append({"id": r[0], "week": r[1], "created_at": r[2], "report": r[3]})
            else:
                out.append({"id": r["id"], "week": r["week"], "created_at": r["created_at"], "report": r["report"]})
        return jsonify({"reports": out})
    except Exception as e:
        return jsonify({"reports": [], "error": str(e)})


# Canonical per-ticker strategy slug, e.g. AAPL_CAM_BREAKOUT_R4S4_V02_5MIN
_STRAT_SLUG_RE = re.compile(
    r'[A-Z][A-Z0-9]*_CAM_(?:BREAKOUT|REVERSAL)_(?:R3S3|R4S4)_V\d+_5MIN', re.I)


def _parse_picks_block(report):
    """Parse the AUTHORITATIVE machine-readable ```picks fenced block, if present.
    Each data line is `SLUG | side | book` (side: long|short|both, book: TV|Kairos).
    Returns [{strategy, side, entry}] (entry: 'tv'|'kairos'|None), or [] if absent.

    The wire button prefers this over the prose Top-18 row: the prose leaks the
    model's DROP/REPLACE reasoning and duplicate slot numbers, which the prose
    parser then has to guess through (and can wire a name mentioned only in a
    'REPLACE with X' aside). The block is clean by construction — one final slug
    per line, no reasoning."""
    m = re.search(r"```picks\s*\n(.*?)```", report or "", re.DOTALL | re.IGNORECASE)
    if not m:
        return []
    out, seen = [], set()
    for raw in m.group(1).splitlines():
        parts = [c.strip() for c in raw.strip().strip("|").split("|")]
        sm = _STRAT_SLUG_RE.search(parts[0]) if parts and parts[0] else None
        if not sm:
            continue                          # header row / blank / stray line
        slug = sm.group(0).upper()
        if slug in seen:
            continue
        seen.add(slug)
        side = "both"
        if len(parts) >= 2:
            s = parts[1].lower()
            side = "long" if "long" in s else "short" if "short" in s else "both"
        entry = None
        if len(parts) >= 3:
            b = parts[2].lower()
            entry = "kairos" if ("kairos" in b or "engine" in b) else "tv" if "tv" in b else None
        out.append({"strategy": slug, "side": side, "entry": entry})
    return out


def _parse_next_month_card(report):
    """Extract the wire-able picks from a crew report's "Next Month — Crew Paper"
    decision card. Returns {picks:[{strategy, side}], entry_source, sizing,
    size_dollars, daytype}. Picks come from the authoritative ```picks fenced block
    when present (clean, one final slug per line); otherwise they fall back to the
    'Top N to run' prose row (older reports). Sizing/entries/day-type are always read
    from the prose table rows."""
    picks, seen = [], set()
    entry_source, sizing, size_dollars, daytype = "tv", "equal", None, None
    # Authoritative machine-readable block wins over the prose Top-18 row.
    block_picks = _parse_picks_block(report)
    used_block  = bool(block_picks)
    if used_block:
        picks = block_picks
        seen  = {p["strategy"] for p in picks}
    for raw in (report or "").splitlines():
        line = raw.strip()
        # Only parse the decision-card TABLE ROWS (| Label | Recommendation |).
        # Detail-section prose mentions "sizing", "$5k", "engine entries" etc. and
        # would otherwise clobber the card's real values (last write wins).
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) < 2:
            continue
        label = cells[0].replace("*", "").strip().lower()
        value = cells[1]
        low_v = value.lower()
        if label.startswith("top") and "run" in label and not used_block:
            matches = list(_STRAT_SLUG_RE.finditer(value))
            for i, m in enumerate(matches):
                slug = m.group(0).upper()
                if slug in seen:
                    continue
                seen.add(slug)
                # This pick's tail = text up to the NEXT slug, so one pick's
                # annotations can't bleed into the next pick's tags.
                nxt  = matches[i + 1].start() if i + 1 < len(matches) else len(value)
                tail = value[m.end():nxt].upper()
                # The Top-18 row must be FINAL picks only, but the crew sometimes
                # leaves a "DROP — replace with X" line inline (listing both the
                # dropped name and its replacement). Skip a slug the crew marked
                # dropped so a rejected bleeder isn't silently wired.
                if "DROP" in tail and ("REPLACE" in tail or "NEW DROP" in tail):
                    continue
                # First mention wins (mirrors the entry parser below): the side TAG
                # sits right after the slug (e.g. "— LONG-only [TV]"), while the
                # justification often names the OTHER side too ("...while SHORT
                # bleeds"). A plain "SHORT in tail" check flipped LONG-only picks to
                # short. Take whichever of LONG/SHORT appears FIRST.
                i_long  = tail.find("LONG")
                i_short = tail.find("SHORT")
                if i_long < 0 and i_short < 0:
                    side = "both"
                elif i_short < 0 or (i_long >= 0 and i_long < i_short):
                    side = "long"
                else:
                    side = "short"
                # Per-pick entry source ([TV] / [Kairos] tag; [Engine] still accepted) — the entry mechanism
                # is part of the strategy (Refined vs Kairos have shown OPPOSITE
                # edges on the same names), so a Kairos-book pick keeps engine
                # entries even when the card's global Entries row says TV.
                # An explicit final-decision phrase WINS over the leading tag: the
                # crew sometimes writes "both [Kairos] (... guardrail BLOCKS [Kairos];
                # TAG [TV] per guardrail ...)", flipping its own tag in the reasoning
                # while leaving a stale leading [Kairos]. Honor the LAST "TAG [X]".
                i_dec_tv  = tail.rfind("TAG [TV]")
                i_dec_eng = max(tail.rfind("TAG [KAIROS]"), tail.rfind("TAG [ENGINE]"))
                if i_dec_tv >= 0 or i_dec_eng >= 0:
                    entry = "tv" if i_dec_tv > i_dec_eng else "kairos"
                else:
                    # No explicit decision phrase → first mention of the tag wins
                    # (the tag sits right after the slug, before the reasoning).
                    i_tv   = tail.find("TV")
                    _cands = [p for p in (tail.find("KAIROS"), tail.find("ENGINE")) if p >= 0]
                    i_eng  = min(_cands) if _cands else -1
                    entry  = ("kairos" if (i_eng >= 0 and (i_tv < 0 or i_eng < i_tv)) else
                              "tv"     if i_tv >= 0 else None)
                picks.append({"strategy": slug, "side": side, "entry": entry})
        elif label == "entries":
            # First mention wins: whichever of TV / Kairos(engine) the cell names
            # FIRST is the recommendation. Prevents flipping to engine just because
            # the justification mentions the word "engine" (e.g. "TV as primary —
            # the engine is net worse").
            i_tv   = low_v.find("tv")
            _cands = [p for p in (low_v.find("kairos"), low_v.find("engine")) if p >= 0]
            i_eng  = min(_cands) if _cands else -1
            entry_source = "kairos" if (i_eng >= 0 and (i_tv < 0 or i_eng < i_tv)) else "tv"
        elif label == "sizing":
            if "scaled" in low_v:
                sizing = "scaled"
            # First "$N[k]" in the cell → the flat per-trade dollar size (e.g. $1.5k).
            m = re.search(r"\$\s*([\d][\d,.]*)\s*([kK])?", value)
            if m:
                try:
                    val = float(m.group(1).replace(",", ""))
                    if m.group(2):
                        val *= 1000
                    size_dollars = val
                except ValueError:
                    pass
        elif label.startswith("day-type") or label.startswith("day type"):
            if re.search(r"\byes\b", low_v):  daytype = True
            elif re.search(r"\bno\b", low_v): daytype = False
    # Untagged picks inherit the card's global Entries recommendation.
    for p in picks:
        if not p.get("entry"):
            p["entry"] = entry_source
    return {"picks": picks, "entry_source": entry_source, "sizing": sizing,
            "size_dollars": size_dollars, "daytype": daytype}


@crew_bp.route("/api/crew/wire_preview")
def api_crew_wire_preview():
    """Dry-run: parse the latest report's 'Next Month — Crew Paper' picks and return
    them for a pre-wire eyeball. Makes NO DB writes. Flags a parsed count != 18 (the
    report may be truncated, have duplicate slugs, or a malformed block) and a
    missing machine-readable picks block (older report parsed from prose)."""
    import app as _kairos
    conn = _kairos.get_db(); cur = conn.cursor()
    cur.execute("SELECT report FROM crew_reports ORDER BY created_at DESC LIMIT 1")
    row = cur.fetchone(); conn.close()
    if not row:
        return jsonify({"error": "No crew report found — generate a report first."}), 400
    report = row[0] if _kairos.DATABASE_URL else row["report"]
    parsed    = _parse_next_month_card(report)
    picks     = parsed["picks"]
    has_block = bool(_parse_picks_block(report))
    warnings  = []
    if not has_block:
        warnings.append("No machine-readable picks block found — parsed from the prose card. "
                        "Re-run the crew for a clean block before trusting this.")
    # Duplicate slugs in the raw block (same strategy twice — e.g. once | TV and once
    # | Kairos). Only the FIRST tag wires, so the second slot is silently lost.
    if has_block:
        import re as _re2
        from collections import Counter as _Counter
        _bm  = _re2.search(r"```picks\s*\n(.*?)```", report, _re2.DOTALL | _re2.IGNORECASE)
        _raw = [m.group(0).upper() for m in _STRAT_SLUG_RE.finditer(_bm.group(1))] if _bm else []
        _dups = sorted(s for s, c in _Counter(_raw).items() if c > 1)
        if _dups:
            warnings.append("Duplicate strategy in the block (only the first tag wires; the other slot is "
                            "lost): " + ", ".join(_dups) + ". Re-run so each name takes one slot.")
    if len(picks) != 18:
        warnings.append(f"Parsed {len(picks)} picks, expected 18 — the report may be truncated, "
                        f"have duplicate slugs, or a malformed block. Review carefully before wiring.")
    return jsonify({
        "picks": picks, "count": len(picks), "has_block": has_block,
        "entry_source": parsed["entry_source"], "sizing": parsed["sizing"],
        "size_dollars": parsed["size_dollars"], "daytype": parsed["daytype"],
        "warnings": warnings,
    })


@crew_bp.route("/api/crew/wire_to_router", methods=["POST"])
def api_crew_wire_to_router():
    """Sync the latest crew report's "Next Month — Crew Paper" picks to the Signal
    Router as routing rules targeting the Crew Paper account (alpaca-paper-4).

    Reconciling, not append-only: picks are upserted (update in place, else insert),
    AND Crew rules whose strategy dropped out of this report are deleted, so Crew
    Paper mirrors the LATEST report instead of accumulating every strategy ever
    wired. Nothing is lost — every report is kept in crew_reports (the scorecard
    reads from there), so an old set can be recreated by re-wiring its report. A
    prune is deferred for any strategy with a live Crew Paper position. Fails loudly
    if there's no report or no parsable picks."""
    import app as _kairos
    import copy as _copy
    import json as _json
    data = request.get_json(silent=True) or {}
    try:    qty = max(1, int(data.get("qty", 10)))
    except (TypeError, ValueError): qty = 10

    # Crew Paper (acct4) must be a real configured account, otherwise alpaca-paper-4
    # rules would have nowhere to route (the webhook skips unconfigured slots). Refuse
    # rather than create orphan rules. Alpaca caps paper accounts at 3 per login — to
    # enable acct4, point ALPACA_KEY4/SECRET4/PAPER4 at a separate Alpaca login.
    if "alpaca4" not in _kairos.ACCOUNTS_BY_TAG:
        return jsonify({"error": "Crew Paper account (acct4) isn't configured — set "
                        "ALPACA_KEY4/SECRET4/PAPER4 (separate Alpaca login; see "
                        "docs/adding_a_paper_account.md). No rules created."}), 400

    conn = _kairos.get_db()
    cur  = conn.cursor()

    # 1) Latest report
    cur.execute("SELECT week, created_at, report FROM crew_reports ORDER BY created_at DESC LIMIT 1")
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "No crew report found — generate a report first."}), 400
    if _kairos.DATABASE_URL:
        week, created_at, report = row[0], row[1], row[2]
    else:
        week, created_at, report = row["week"], row["created_at"], row["report"]

    parsed = _parse_next_month_card(report)
    picks  = parsed["picks"]
    if not picks:
        conn.close()
        return jsonify({"error": "Could not find any strategy names in the "
                        "'Next Month — Crew Paper' card. Make sure the latest report "
                        "leads with the decision card and names full strategy slugs "
                        "(e.g. AAPL_CAM_BREAKOUT_R4S4_V02_5MIN)."}), 400

    # 2) Inventory existing rules: source pipelines (to clone each pick's tuned
    #    exit_params / hours / instrument) and existing Crew rules (to update in
    #    place on re-run instead of duplicating).
    cur.execute("SELECT id, name, nodes FROM routing_rules")
    source_nodes  = {}   # strat -> (priority, nodes) — best non-acct4 pipeline
    existing_crew = {}   # strat -> rule_id of the acct4 rule
    for r in cur.fetchall():
        rid = r[0] if _kairos.DATABASE_URL else r["id"]
        raw = r[2] if _kairos.DATABASE_URL else r["nodes"]
        try:    nodes = _json.loads(raw) if isinstance(raw, str) else (raw or [])
        except Exception: nodes = []
        brokers   = [(n.get("value") or "").lower() for n in nodes if n.get("type") == "broker"]
        is_acct4  = any(b in ("alpaca-paper-4", "alpaca-live-4") for b in brokers)
        strat_vals = [(n.get("value") or "").strip().upper()
                      for n in nodes if n.get("type") == "strategy" and n.get("value")]
        if is_acct4:
            for s in strat_vals:
                existing_crew[s] = rid
            continue
        # Prefer the Refined (acct2) pipeline as the source of truth for exits/hours.
        prio = 2 if any(b in ("alpaca-paper-2", "alpaca-live-2") for b in brokers) else 1
        for s in strat_vals:
            if s not in source_nodes or source_nodes[s][0] < prio:
                source_nodes[s] = (prio, nodes)

    # 2b) Sizing — Crew Paper trades at the TOP Refined band so its positions match
    # the top band in Refined/Kairos (single source of truth: _REFINED_SIZE_BANDS[0]).
    # Converted to shares per ticker via a live price fetch. The card's parsed size is
    # kept for reference but no longer caps Crew sizing. Request body overrides via
    # size_dollars; falls back to a flat share count (qty) if no price is available.
    try:    size_dollars = float(_kairos._REFINED_SIZE_BANDS[0][1])
    except Exception: size_dollars = parsed.get("size_dollars")
    if data.get("size_dollars") not in (None, ""):
        try:    size_dollars = max(1.0, float(data["size_dollars"]))
        except (TypeError, ValueError): pass
    prices = {}
    if size_dollars:
        try:
            _tks = [pk["strategy"].split("_", 1)[0].upper() for pk in picks]
            prices = _kairos._fetch_alpaca_last_prices(_tks) or {}
        except Exception as _pe:
            _kairos.log.debug("crew wire price fetch failed: %s", _pe)
            prices = {}

    def _rule_qty(slug):
        tk = slug.split("_", 1)[0].upper()
        if size_dollars and prices.get(tk):
            return max(1, round(size_dollars / prices[tk]))
        return qty

    def _build_nodes(slug, q, side, entry):
        """Clone the strategy's top-performer pipeline (tuned exit_params, hours,
        instrument) and swap in the Crew broker, dollar-sized quantity, the PICK's
        entry source ([TV]/[Kairos] tag — falls back to the card's global Entries
        row), and a long/short side_gate per the pick (both = none). Per-pick entry
        matters: a name that earned its slot on the Kairos book keeps engine
        entries even when the card's default is TV. Falls back to a generic
        default if no source rule."""
        gate  = side if (side or "").lower() in ("long", "short") else None
        entry = entry if entry in ("tv", "kairos") else parsed["entry_source"]
        src = source_nodes.get(slug)
        if not src:
            return _kairos._crew_default_nodes(slug, entry_source=entry,
                                               qty=q, side_gate=gate)
        out, have_broker, have_entry = [], False, False
        for n in _copy.deepcopy(src[1]):
            t = n.get("type")
            if t == "broker":
                if have_broker:
                    continue                      # collapse multiple brokers to one
                out.append({"type": "broker", "value": "alpaca-paper-4"})
                have_broker = True
            elif t == "quantity":
                out.append({"type": "quantity", "amount": q, "unit": (n.get("unit") or "shares")})
            elif t == "entry_source":
                out.append({"type": "entry_source", "value": entry})
                have_entry = True
            elif t == "side_gate":
                continue                          # re-applied below from the card's pick
            else:
                out.append(n)                     # strategy, instrument, hours, exit_params, ...
        if not have_broker:
            out.append({"type": "broker", "value": "alpaca-paper-4"})
        if not have_entry:
            out.append({"type": "entry_source", "value": entry})
        if gate:
            out.append({"type": "side_gate", "value": gate})
        return out

    # 3) Upsert: update an existing Crew rule in place, else insert a new one. This
    # makes re-clicking re-sync Crew Paper to the latest card + tuned exits.
    p  = _kairos.placeholder()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    created, updated, cloned = [], [], []
    for pick in picks:
        slug = pick["strategy"]
        nodes_json = _json.dumps(_build_nodes(slug, _rule_qty(slug), pick.get("side"), pick.get("entry")))
        if slug in source_nodes:
            cloned.append(slug)
        rid = existing_crew.get(slug)
        if rid is not None:
            cur.execute(f"UPDATE routing_rules SET nodes={p} WHERE id={p}", (nodes_json, rid))
            updated.append(slug)
        else:
            cur.execute(
                f"INSERT INTO routing_rules (name,enabled,nodes,created_at,tv_alert_created) "
                f"VALUES ({p},{p},{p},{p},{p})",
                (f"{slug} · Crew", 1, nodes_json, ts, 0),
            )
            created.append(slug)

    # 4) Reconcile — delete Crew rules whose strategy dropped out of this report,
    # so Crew Paper mirrors the latest picks rather than piling up every strategy
    # ever wired ("filter Crew" used to grow without bound). A rule is spared if
    # ANY of its strategies is still picked, or if any of its tickers has a LIVE
    # Crew Paper position — that prune is deferred to the next wire so an open trade
    # keeps its tuned exit (global position-loss + max-hold protect it regardless).
    # Pick history is untouched: it lives in crew_reports, not in these rules.
    new_slugs = {pick["strategy"] for pick in picks}
    rid_strats = {}
    for _s, _rid in existing_crew.items():
        rid_strats.setdefault(_rid, set()).add(_s)
    prune_rids = {rid: strs for rid, strs in rid_strats.items() if not (strs & new_slugs)}

    open_tickers = set()
    if prune_rids:
        try:
            _br4 = (_kairos.ACCOUNTS_BY_TAG.get("alpaca4") or {}).get("broker")
            if _br4 is not None:
                _br4._invalidate_pos_cache()
                open_tickers = {(pp.get("symbol") or "").upper() for pp in _br4.get_positions()}
        except Exception as _pe:
            _kairos.log.warning("crew wire: acct4 positions fetch failed; pruning "
                                "without the open-position guard: %s", _pe)

    deleted, deferred = [], []
    for rid, strs in prune_rids.items():
        if any(s.split("_", 1)[0].upper() in open_tickers for s in strs):
            deferred.extend(sorted(strs))
            continue
        cur.execute(f"DELETE FROM routing_rules WHERE id={p}", (rid,))
        deleted.extend(sorted(strs))

    conn.commit()
    conn.close()

    return jsonify({
        "created": created, "updated": updated,
        "deleted": deleted, "deferred_open_position": deferred,
        "cloned_from_source": cloned,
        "entry_source": parsed["entry_source"], "sizing": parsed["sizing"],
        "size_dollars": size_dollars, "daytype_gate": parsed["daytype"], "qty": qty,
        "source_report_week": week, "source_report_at": created_at,
        "sides":   {pk["strategy"]: pk["side"] for pk in picks},
        "entries": {pk["strategy"]: pk.get("entry") for pk in picks},
    })


@crew_bp.route("/api/crew/knowledge", methods=["GET"])
def api_crew_knowledge_get():
    import pathlib
    kb_path = pathlib.Path(__file__).parent.parent / "crew_knowledge.md"
    try:
        return jsonify({"content": kb_path.read_text(encoding="utf-8")})
    except Exception as e:
        return jsonify({"content": "", "error": str(e)})


@crew_bp.route("/api/crew/knowledge", methods=["PUT"])
def api_crew_knowledge_put():
    import pathlib
    data    = request.get_json(silent=True) or {}
    content = data.get("content", "")
    kb_path = pathlib.Path(__file__).parent.parent / "crew_knowledge.md"
    try:
        kb_path.write_text(content, encoding="utf-8")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@crew_bp.route("/crew")
def crew_page():
    return render_template("crew.html")


def _prep_and_run_kairos(q, app_obj, from_date, to_date, range_label, kairos_target):
    """Pre-fetch all Kairos crew inputs, then run the crew. Runs in a BACKGROUND
    thread so the SSE Response returns immediately: the pre-fetch makes ~9 internal
    Alpaca calls and on a wide window (e.g. 'Last Month') can exceed the gunicorn
    worker timeout — when it ran synchronously before the Response, the proxy saw no
    bytes and returned 502 before the stream even started. Uses the passed app object
    for test_client (there's no request context off-thread). Any prep failure is
    pushed to the queue as an error event so the stream surfaces it instead of hanging."""
    import app as _kairos
    strat_data        = {}
    engine_strat_data = {}
    journal_data = []
    prev_reports = []
    rules_data   = []
    engine_data  = {}
    card_data    = {}
    _pa          = {}   # TV Farm (acct1) full-sample leaderboard
    _pa5         = {}   # Kairos Farm (acct5) full-sample leaderboard
    try:
        _dr  = (f"&from_date={from_date}" if from_date else "") + (f"&to_date={to_date}" if to_date else "")
        # Farms are the FULL-SAMPLE audition pools — always a fixed 45d trailing window
        # regardless of the report's analysis range (a narrow window would starve them).
        _FARM_WINDOW_DAYS = 45
        try:    _farm_anchor = datetime.fromisoformat(to_date).date() if to_date else datetime.now(timezone.utc).date()
        except Exception: _farm_anchor = datetime.now(timezone.utc).date()
        _farm_dr = (f"&from_date={(_farm_anchor - timedelta(days=_FARM_WINDOW_DAYS)).isoformat()}"
                    f"&to_date={_farm_anchor.isoformat()}")
        _qs  = "account=2" + _dr
        _qs3 = "account=3" + _dr
        with app_obj.test_client() as _c:
            strat_data        = _c.get(f"/api/alpaca/analysis?{_qs}").get_json()  or {}
            engine_strat_data = _c.get(f"/api/alpaca/analysis?{_qs3}").get_json() or {}
            journal_data = _c.get("/api/journal/entries?account=2").get_json() or []
            rules_data   = _c.get("/api/routing/rules").get_json()           or []
            engine_data  = _c.get("/api/engine_pilot/compare?days=30").get_json() or {}
            _pa  = _c.get(f"/api/alpaca/analysis?account=1{_farm_dr}").get_json() or {}
            _pa5 = _c.get(f"/api/alpaca/analysis?account=5{_farm_dr}").get_json() or {}
            _s2  = _c.get(f"/api/alpaca/ls_breakdown?account=2{_dr}").get_json() or {}
            _s3  = _c.get(f"/api/alpaca/ls_breakdown?account=3{_dr}").get_json() or {}
            _s4  = _c.get(f"/api/alpaca/ls_breakdown?account=4{_dr}").get_json() or {}
        _IDX = ("SPY", "QQQ", "IWM", "SMH")
        def _idx_sum(ps):
            out = {}
            for nm, s in (ps or {}).items():
                tk = nm.upper().split("_")[0]
                if tk in _IDX:
                    out[tk] = round(out.get(tk, 0) + (s.get("total_pnl", 0) or 0), 2)
            return out
        card_data = {
            "indices_paper_all": _idx_sum(_pa.get("per_strategy")),
            "side_refined": (_s2 or {}).get("overall_side"),
            "side_kairos":  (_s3 or {}).get("overall_side"),
            "side_crew":    (_s4 or {}).get("overall_side"),
            "side_gated_refined": (_s2 or {}).get("side_gated_candidates"),
            "side_gated_kairos":  (_s3 or {}).get("side_gated_candidates"),
            "side_gated_crew":    (_s4 or {}).get("side_gated_candidates"),
            "band_side_refined": (_s2 or {}).get("by_band_side"),
            "band_side_kairos":  (_s3 or {}).get("by_band_side"),
            "band_side_crew":    (_s4 or {}).get("by_band_side"),
            "side_daytype_refined": (_s2 or {}).get("by_side_daytype"),
            "side_daytype_kairos":  (_s3 or {}).get("by_side_daytype"),
            "side_daytype_crew":    (_s4 or {}).get("by_side_daytype"),
        }
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
    scorecard_data = {}
    try:
        if prev_reports:
            scorecard_data = _pick_scorecard(prev_reports[0])
    except Exception:
        pass
    book_data = {}
    try:
        book_data = _crew_book_scorecard()
    except Exception:
        pass
    # Snapshot leaderboard rankings (composite SCORE order) — the SAME ranking the
    # user watches on the Analysis page and that acct2/acct3 trade. In-memory first,
    # else the persisted copy.
    def _load_snap(mem_attr, setting_key):
        snap = getattr(_kairos, mem_attr, None) or {}
        if not snap.get("top_scored"):
            try:
                _st = _kairos._load_setting(setting_key)
                if _st:
                    import json as _sj
                    snap = _sj.loads(_st)
            except Exception:
                pass
        return snap
    tv_snap_rank     = _load_snap("_refined_last_result",        "REFINED_LAST_RESULT")
    kairos_snap_rank = _load_snap("_kairos_refined_last_result", "KAIROS_REFINED_LAST_RESULT")
    try:
        _run_kairos_crew(q, strat_data, journal_data, prev_reports, range_label or "custom range",
                         rules_data, engine_data, engine_strat_data, card_data, scorecard_data,
                         book_data, _pa, _pa5, kairos_target, tv_snap_rank, kairos_snap_rank)
    except Exception as _e:
        q.put({"type": "error", "error": f"crew prep/run failed: {_e}", "ts": _ts()})


@crew_bp.route("/api/crew/run", methods=["POST"])
def api_crew_run():
    data       = request.get_json(silent=True) or {}
    crew_type  = (data.get("crew_type") or "research").strip()
    topic      = (data.get("topic") or "").strip()

    q = queue.Queue()

    if crew_type == "kairos":
        # Extract request-bound values here (request context), then hand off ALL the
        # heavy pre-fetch to a background thread so the streaming Response returns
        # immediately (see _prep_and_run_kairos for why — avoids the 502-before-stream).
        from flask import current_app as _ca
        _app = _ca._get_current_object()
        from_date    = (data.get("from") or "").strip()
        to_date      = (data.get("to")   or "").strip()
        range_label  = (data.get("label") or "").strip()
        kairos_target = (str(data.get("kairos_target") or "9")).strip().lower()
        threading.Thread(
            target=_prep_and_run_kairos,
            args=(q, _app, from_date, to_date, range_label, kairos_target),
            daemon=True,
        ).start()
    else:
        if not topic:
            return jsonify({"error": "topic required"}), 400
        threading.Thread(target=_run_crew, args=(topic, q), daemon=True).start()

    def generate():
        # Flush a heartbeat IMMEDIATELY so the proxy gets bytes before the (possibly
        # slow) background pre-fetch finishes — otherwise a wide-window prep could
        # exceed the gateway's time-to-first-byte and 502 before the stream starts.
        yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        # Then heartbeat every 15s during silence. Agent 2 (the systematic trader) is a
        # single long LLM call that emits no queue events while it thinks; without a
        # frequent keep-alive, an idle proxy/load-balancer drops the stream mid-run
        # and the browser reports a bare "network error". 15s stays well under any
        # common idle timeout (Railway edge, nginx, CDNs).
        while True:
            try:
                event = q.get(timeout=15)
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
