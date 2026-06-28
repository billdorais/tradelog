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
from datetime import datetime, timezone

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

def _run_kairos_crew(q: queue.Queue, strat_data: dict = None, journal_data: list = None, prev_reports: list = None, period: str = "", rules_data: list = None, engine_data: dict = None, engine_strat_data: dict = None, card_data: dict = None) -> None:
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
            return LLM(model="anthropic/claude-sonnet-4-6", api_key=api_key, temperature=temp, max_tokens=4096)

        # ── Format pre-fetched data ───────────────────────────────────────────
        # Data was fetched in the Flask route handler and passed in directly.

        def _fmt_strategies(data: dict, header: str = "REFINED ACCOUNT — STRATEGY LEADERBOARD (last ~20 days)",
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
            """Kairos engine pilot (acct 3) vs TV Refined (acct 2) head-to-head."""
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
            header="KAIROS ENGINE (account 3, server-side entries) — STRATEGY LEADERBOARD",
            empty_msg=("=== KAIROS ENGINE (account 3) — STRATEGY LEADERBOARD ===\n"
                       "No per-strategy data yet — the engine book (acct3) has no closed round-trips "
                       "in this window. Treat the Kairos entries as not-yet-evaluable per strategy."),
        )
        journal_block  = _fmt_journal(journal_data)
        stops_block    = _fmt_stops_comparison(rules_data, journal_data)
        engine_block   = _fmt_engine(engine_data)

        def _fmt_card_inputs(cd):
            cd = cd or {}
            lines = ["=== NEXT-MONTH CARD INPUTS (for the leading recommendation card) ==="]
            idx = cd.get("indices_paper_all") or {}
            if idx:
                tot = round(sum(idx.values()), 2)
                lines.append(f"Indices-only P&L on Paper All (acct1): ${tot:.2f} total — "
                             + ", ".join(f"{k} ${v:.2f}" for k, v in sorted(idx.items(), key=lambda x: -x[1])))
            else:
                lines.append("Indices-only P&L on Paper All: no index data in window.")
            for label, key in (("Refined acct2 (TV)", "side_refined"), ("Kairos acct3 (engine)", "side_kairos")):
                ss = cd.get(key) or []
                if ss:
                    lines.append(f"{label} by side: " + " · ".join(
                        f"{r.get('side')} ${r.get('pnl', 0):.2f} ({r.get('trades', 0)}t {r.get('win_rate', 0)}%)"
                        for r in ss))
            return "\n".join(lines)

        card_block = _fmt_card_inputs(card_data)

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

THREE ALPACA ACCOUNTS:
1. Paper All (account 1): ALL strategies run here, ~100+ active pipelines.
2. Refined (account 2, alpaca-paper-2): TOP-20 only. THIS IS THE PRIMARY ACCOUNT UNDER REVIEW.
   - Daily 4:15 PM ET refresh selects top-20 by composite score:
     Sharpe 35% + Profit Factor 30% + Win Rate 20% + Trades 15%
   - 20-day rolling lookback with 10-day recency blend (60/40)
   - Min 5 trades to be eligible; 3+ consecutive losses = auto-demoted
   - Will transition to LIVE trading (PDT $25k floor no longer required — Alpaca moved to
     an intraday-margin model; ~$25k is now a comfort choice, not a regulation)
3. Kairos Engine (account 3): a NEW server-side entry PILOT, running in parallel. It trades the
   SAME Refined strategies, but Kairos generates the entries itself (fresh-cross buffered
   breakouts; confirmed reversal rejects with wick >= 0.25*ATR) instead of relying on TradingView
   alerts — entering a touch earlier on breakouts. Purpose: test whether server-side entries beat
   TV entries, head-to-head, in a separate book. Earlier sim work was a mirage; the real edge is
   modest and per-share, so judge acct3-vs-acct2 on a multi-week trend, not single days.

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
            goal="Analyse the Kairos Refined account performance and deliver specific, actionable recommendations.",
            backstory=(
                "You are a seasoned systematic trading professional with 20 years of "
                "experience managing algorithmic strategy portfolios on US equities.\n\n"
                f"{KAIROS_SYSTEM_KNOWLEDGE}\n"
                "Your PRIMARY focus is the Refined account (account 2), but you ALSO directly "
                "analyse the Kairos engine book (account 3) — its own per-strategy leaderboard, "
                "not just the aggregate head-to-head — since those server-side entries are the "
                "live experiment. When the Kairos book has enough trades, call out which Kairos "
                "ENTRIES (by strategy) are pulling their weight vs which are bleeding, and how "
                "that squares with the same strategy on the TV-driven Refined book. "
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

        # ── Single task — all data embedded directly ──────────────────────────

        analysis_task = Task(
            description=(
                f"Here is the Kairos account data"
                + (f" for: {period}" if period else "") + ":\n\n"
                f"{strategy_block}\n\n"
                f"{engine_strat_block}\n\n"
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
                "| Top 5 to run | five strategy names (your best risk-adjusted picks across acct2/acct3), each tagged long / short / both per the side data |\n"
                "| Sizing | Equal risk OR Scaled-by-score — one-clause why (equal risk is preferred for a fresh test until the score proves forward edge) |\n"
                "| Day-type gate | Yes / No |\n"
                "| Entries | Refined TV OR Kairos engine — per the engine-vs-TV read |\n"
                "| Best indices | top index tickers · indices-only P&L from Paper All: $X (from the card inputs) |\n\n"
                "Then continue with the six detailed sections:\n\n"
                "1. **Portfolio Health** — Is the Refined top-20 earning its keep? "
                "What does the PF and Sharpe say about real edge vs. luck?\n\n"
                "2. **Strategy Calls** — Name 2-3 to promote/add to Refined and 2-3 to pause "
                "or demote. Give specific reasons tied to numbers.\n\n"
                "3. **Stop & Parameter Check** — Given the regime tags and any sweep data in "
                "the journal, are current trailing stops appropriate? Reference specific sweep "
                "results and the trader's own notes if they offer relevant observations.\n\n"
                "4. **Engine vs TV Pilot** — Using BOTH the KAIROS ENGINE STRATEGY LEADERBOARD "
                "(acct 3 per-strategy) and the head-to-head PILOT data, is the server-side engine "
                "(acct 3) beating, matching, or lagging the TV-driven Refined account (acct 2)? "
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

# ── Chat tools — let the advisor pull LIVE Kairos data on demand ───────────────

_CREW_TOOLS = [
    {
        "name": "engine_vs_tv",
        "description": "Head-to-head realized P&L: the Kairos engine (account 3, server-side "
                       "entries) vs TV Refined (account 2) over the last N days — daily rows + "
                       "cumulative totals + per-account win rate. Use for 'is the engine beating TV'.",
        "input_schema": {"type": "object", "properties": {
            "days": {"type": "integer", "description": "lookback days (default 30)"}}},
    },
    {
        "name": "day_recap",
        "description": "One specific day's trades comparing TV Refined (acct2) vs Kairos engine "
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
                        "description": "1=Paper All, 2=Refined, 3=Kairos engine (default 2)"},
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
        "description": "Backtest the leaderboard SELECTION METHOD on Paper All (acct1 — the "
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
                       "side, plus band×side and side×day-type (trades, win%, P&L). Use for 'what "
                       "if I only traded shorts' / 'are my longs the problem'. Hindsight caveat: a "
                       "side's past edge can flip — 'shorts-only would have made $X' is what "
                       "happened, not a forward guarantee.",
        "input_schema": {"type": "object", "properties": {
            "account": {"type": "string", "enum": ["1", "2", "3"],
                        "description": "1=Paper All, 2=Refined, 3=Kairos (default 3)"},
            "from_date": {"type": "string", "description": "YYYY-MM-DD (optional)"},
            "to_date": {"type": "string", "description": "YYYY-MM-DD (optional)"}}},
    },
    {
        "name": "band_fill_quality",
        "description": "Compare ONE band across Refined (acct2, TV entries) vs Kairos (acct3, "
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
        "name": "cross_account",
        "description": "Cross-validate strategies across Refined (acct2, TV entries) and Kairos "
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
            stats = _kairos._compute_strategy_stats(days=days)   # acct1 (Paper All) per-strategy
            if not stats:
                return json.dumps({"error": "No Paper All (acct1) stats available."})
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
                "account": "Paper All (1)", "days": days, "n": n, "eligible_pool": len(elig),
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
                "caveat": "Hindsight/descriptive: 'shorts-only would have made $X' is what "
                          "happened, not a forward guarantee — a side's edge can flip, and you "
                          "didn't know in advance which side would win.",
            })[:6500]
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
            idx_all = _idx(_ps(1))   # Paper All — the full audition pool, for 'indices-only'
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
                        "indices_paper_all = SPY/QQQ/IWM/SMH on Paper All (acct1, full pool) — use "
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
        "score on Paper All — for 'would the top-20-by-P&L have beaten the leaderboard'), and "
        "side_breakdown (long vs short by band/day-type — for 'what if I only traded shorts'), and "
        "band_fill_quality (one band: Refined-TV vs Kairos-engine P&L + the engine's slippage — "
        "for 'is the engine degrading R3S3 breakout'), and cross_account (strategies positive on "
        "BOTH books + divergent ones + an index SPY/QQQ/IWM/SMH rollup — for 'which strategies "
        "work in both' and 'how did the indices do'). "
        "USE them whenever the user asks about anything current, specific, or "
        "not covered by the report — don't guess or say you lack data. Account map: 1=Paper All, "
        "2=Refined (TV), 3=Kairos engine (server-side pilot). Resolve relative dates ('yesterday', "
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
        "or for a new paper account, FIRST pull the data (cross_account, side_breakdown, "
        "band_fill_quality, rank_compare), then LEAD your reply with exactly this card (Markdown "
        "table), every value from a tool result — never invented:\n\n"
        "## 📋 Next Month — Crew Paper Account\n"
        "| Decision | Recommendation |\n|---|---|\n"
        "| Top 5 to run | five strategy names, each tagged long / short / both |\n"
        "| Sizing | Equal risk OR Scaled-by-score — one-line why |\n"
        "| Day-type gate | Yes / No |\n"
        "| Entries | Refined TV OR Kairos engine |\n"
        "| Best indices | tickers · indices-only P&L from Paper All: $X |\n\n"
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


def _parse_next_month_card(report):
    """Extract the wire-able picks from a crew report's "Next Month — Crew Paper"
    decision card. Returns {picks:[{strategy, side}], entry_source, sizing,
    size_dollars, daytype}. Strategy names are pulled ONLY from the 'Top 5 to run'
    row so the Detail section's pause/demote mentions never get wired by mistake."""
    picks, seen = [], set()
    entry_source, sizing, size_dollars, daytype = "tv", "equal", None, None
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
        if label.startswith("top 5"):
            for m in _STRAT_SLUG_RE.findall(value):
                slug = m.upper()
                if slug in seen:
                    continue
                seen.add(slug)
                # Best-effort side tag from the text just after the slug.
                tail = value.upper().split(slug, 1)[1][:24]
                side = ("short" if "SHORT" in tail else
                        "long"  if "LONG"  in tail else "both")
                picks.append({"strategy": slug, "side": side})
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
    return {"picks": picks, "entry_source": entry_source, "sizing": sizing,
            "size_dollars": size_dollars, "daytype": daytype}


@crew_bp.route("/api/crew/wire_to_router", methods=["POST"])
def api_crew_wire_to_router():
    """Append the latest crew report's "Next Month — Crew Paper" picks to the
    Signal Router as routing rules targeting the Crew Paper account (alpaca-paper-4).

    Append-only with dedupe: a pick is skipped if a rule already routes that
    strategy to acct4. Fails loudly if there's no report or no parsable picks."""
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

    # 2b) Sizing — honor the card's flat per-trade dollar size (e.g. "$1.5k/trade
    # flat" = equal risk). Convert to shares per ticker via a live price fetch so
    # every pick risks ~the same dollars. Falls back to a flat share count (qty)
    # when no dollar size is parsed or a price is unavailable. Request body may
    # override via size_dollars / qty.
    size_dollars = parsed.get("size_dollars")
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

    def _build_nodes(slug, q):
        """Clone the strategy's top-performer pipeline (tuned exit_params, hours,
        instrument) and swap in the Crew broker, dollar-sized quantity, and the
        card's entry source. Falls back to a generic default if no source rule."""
        src = source_nodes.get(slug)
        if not src:
            return _kairos._crew_default_nodes(slug, entry_source=parsed["entry_source"], qty=q)
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
                out.append({"type": "entry_source", "value": parsed["entry_source"]})
                have_entry = True
            else:
                out.append(n)                     # strategy, instrument, hours, exit_params, ...
        if not have_broker:
            out.append({"type": "broker", "value": "alpaca-paper-4"})
        if not have_entry:
            out.append({"type": "entry_source", "value": parsed["entry_source"]})
        return out

    # 3) Upsert: update an existing Crew rule in place, else insert a new one. This
    # makes re-clicking re-sync Crew Paper to the latest card + tuned exits.
    p  = _kairos.placeholder()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    created, updated, cloned = [], [], []
    for pick in picks:
        slug = pick["strategy"]
        nodes_json = _json.dumps(_build_nodes(slug, _rule_qty(slug)))
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
    conn.commit()
    conn.close()

    return jsonify({
        "created": created, "updated": updated,
        "cloned_from_source": cloned,
        "entry_source": parsed["entry_source"], "sizing": parsed["sizing"],
        "size_dollars": size_dollars, "daytype_gate": parsed["daytype"], "qty": qty,
        "source_report_week": week, "source_report_at": created_at,
        "sides": {pk["strategy"]: pk["side"] for pk in picks},
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
        from_date    = (data.get("from") or "").strip()
        to_date      = (data.get("to")   or "").strip()
        range_label  = (data.get("label") or "").strip()
        strat_data        = {}
        engine_strat_data = {}
        journal_data = []
        prev_reports = []
        rules_data   = []
        engine_data  = {}
        card_data    = {}
        try:
            _dr  = (f"&from_date={from_date}" if from_date else "") + (f"&to_date={to_date}" if to_date else "")
            _qs  = "account=2" + _dr
            _qs3 = "account=3" + _dr
            with _ca.test_client() as _c:
                strat_data        = _c.get(f"/api/alpaca/analysis?{_qs}").get_json()  or {}
                engine_strat_data = _c.get(f"/api/alpaca/analysis?{_qs3}").get_json() or {}
                journal_data = _c.get("/api/journal/entries").get_json()         or []
                rules_data   = _c.get("/api/routing/rules").get_json()           or []
                engine_data  = _c.get("/api/engine_pilot/compare?days=30").get_json() or {}
                # Inputs for the "Next Month" card baked into the report.
                _pa  = _c.get(f"/api/alpaca/analysis?account=1{_dr}").get_json() or {}
                _s2  = _c.get(f"/api/alpaca/ls_breakdown?account=2{_dr}").get_json() or {}
                _s3  = _c.get(f"/api/alpaca/ls_breakdown?account=3{_dr}").get_json() or {}
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
        threading.Thread(
            target=_run_kairos_crew,
            args=(q, strat_data, journal_data, prev_reports, range_label or "custom range", rules_data, engine_data, engine_strat_data, card_data),
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
