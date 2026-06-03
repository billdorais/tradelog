"""
Kairos Crew — Streamlit Dashboard
----------------------------------
Visualises a two-agent CrewAI crew running in a background thread.
Events (steps, task completions, final output) flow into a queue and
are rendered as "starlit notes" cards as they arrive.

Run:
    streamlit run app.py
"""

import queue
import re
import sys
import threading
import time
from datetime import datetime
from io import StringIO

import streamlit as st
from crew import build_crew

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Kairos Crew",
    page_icon="✦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS — starlit notes aesthetic ─────────────────────────────────────────────
st.markdown("""
<style>
  /* ── Base ── */
  [data-testid="stAppViewContainer"] { background: #0b0d14; }
  [data-testid="stHeader"]           { background: transparent; }
  section[data-testid="stSidebar"]   { background: #0f1117; }

  /* ── Typography ── */
  h1  { font-size: 1.7rem !important; font-weight: 700; color: #e8e8f0; letter-spacing: -0.02em; }
  h3  { font-size: 0.95rem !important; font-weight: 600; color: #c4c4d4; }

  /* ── Agent card ── */
  .agent-card {
    background: #12141f;
    border: 1px solid #1e2130;
    border-radius: 12px;
    padding: 16px 18px;
    margin-bottom: 8px;
    transition: border-color 0.3s, box-shadow 0.3s;
  }
  .agent-card.active {
    border-color: #7FE098;
    box-shadow: 0 0 18px rgba(127,224,152,0.12);
  }
  .agent-card.done {
    border-color: #3a4a3e;
  }
  .agent-role {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #666;
    margin-bottom: 4px;
  }
  .agent-name {
    font-size: 1rem;
    font-weight: 700;
    color: #e0e0e0;
    margin-bottom: 8px;
  }
  .agent-status {
    display: inline-block;
    font-size: 0.72rem;
    padding: 2px 10px;
    border-radius: 20px;
    font-weight: 600;
  }
  .status-waiting  { background: rgba(100,100,120,0.2); color: #888; }
  .status-thinking { background: rgba(127,224,152,0.15); color: #7FE098; }
  .status-done     { background: rgba(100,180,100,0.1);  color: #5a9e6a; }

  /* ── Event log note ── */
  .note {
    background: #13161f;
    border-left: 3px solid #2a2d3e;
    border-radius: 0 8px 8px 0;
    padding: 10px 14px;
    margin-bottom: 8px;
    font-size: 0.82rem;
    line-height: 1.55;
    color: #b0b0c0;
    animation: fadein 0.3s ease;
  }
  .note.researcher { border-left-color: #5a8fa8; }
  .note.summarizer { border-left-color: #c4b5fd; }
  .note.system     { border-left-color: #7FE098; }
  .note.tool       { border-left-color: #F2E7AC; background: #14150e; }
  .note-header {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    font-weight: 700;
    margin-bottom: 4px;
  }
  .note.researcher .note-header { color: #5a8fa8; }
  .note.summarizer .note-header { color: #c4b5fd; }
  .note.system     .note-header { color: #7FE098; }
  .note.tool       .note-header { color: #F2E7AC; }

  /* ── Final output ── */
  .final-box {
    background: #0f1a14;
    border: 1px solid #2a4a35;
    border-radius: 12px;
    padding: 22px 26px;
    margin-top: 8px;
    font-size: 0.9rem;
    line-height: 1.75;
    color: #c8dcc8;
    white-space: pre-wrap;
  }

  /* ── Divider ── */
  .section-label {
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #444;
    margin: 18px 0 8px;
  }

  @keyframes fadein {
    from { opacity: 0; transform: translateY(4px); }
    to   { opacity: 1; transform: translateY(0); }
  }
</style>
""", unsafe_allow_html=True)


# ── Session state ─────────────────────────────────────────────────────────────
def _init_state():
    defaults = {
        "running":      False,
        "done":         False,
        "events":       [],       # list of dicts: {kind, agent, text, ts}
        "result":       None,
        "active_agent": None,
        "q":            queue.Queue(),
        "thread":       None,
        "task_done":    [],       # task names that completed
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()


# ── Helpers ───────────────────────────────────────────────────────────────────
def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _push(kind: str, agent: str, text: str):
    st.session_state.q.put({"kind": kind, "agent": agent, "text": text, "ts": _ts()})


def _drain_queue():
    """Move all pending queue items into st.session_state.events."""
    changed = False
    while True:
        try:
            item = st.session_state.q.get_nowait()
            if item.get("kind") == "result":
                st.session_state.result   = item["text"]
                st.session_state.running  = False
                st.session_state.done     = True
                st.session_state.active_agent = None
            else:
                st.session_state.events.append(item)
                if item.get("agent"):
                    st.session_state.active_agent = item["agent"]
            changed = True
        except queue.Empty:
            break
    return changed


def _note_html(evt: dict) -> str:
    cls = evt.get("agent", "system").lower().replace(" ", "")
    if "research" in cls:
        cls = "researcher"
    elif "summar" in cls or "content" in cls or "strategist" in cls:
        cls = "summarizer"
    elif "tool" in evt.get("kind", ""):
        cls = "tool"
    else:
        cls = "system"

    label_map = {
        "researcher": "🔍 Research Analyst",
        "summarizer": "✍️  Content Strategist",
        "tool":       "🛠  Tool Call",
        "system":     "✦ System",
    }
    label = label_map.get(cls, cls)
    ts    = evt.get("ts", "")
    text  = evt.get("text", "").replace("<", "&lt;").replace(">", "&gt;")
    return (
        f'<div class="note {cls}">'
        f'  <div class="note-header">{label} &nbsp;·&nbsp; {ts}</div>'
        f'  {text}'
        f'</div>'
    )


def _agent_card(name: str, role: str, icon: str, active: bool, done: bool) -> str:
    state_cls = "done" if done else ("active" if active else "")
    if done:
        badge = '<span class="agent-status status-done">✓ Done</span>'
    elif active:
        badge = '<span class="agent-status status-thinking">⟳ Thinking…</span>'
    else:
        badge = '<span class="agent-status status-waiting">Waiting</span>'
    return (
        f'<div class="agent-card {state_cls}">'
        f'  <div class="agent-role">{icon} {role}</div>'
        f'  <div class="agent-name">{name}</div>'
        f'  {badge}'
        f'</div>'
    )


# ── Stdout capture → queue ────────────────────────────────────────────────────
class _QueueWriter:
    """Intercepts CrewAI's verbose stdout and routes lines to the event queue."""

    AGENT_PATTERNS = [
        (re.compile(r"Research Analyst|Researcher", re.I), "Research Analyst"),
        (re.compile(r"Content Strategist|Summarizer", re.I), "Content Strategist"),
    ]
    SKIP = re.compile(
        r"^\s*$|^=+$|Entering new|AgentExecutor|^#|Token|Usage|openai|litellm",
        re.I,
    )
    TOOL_RE = re.compile(r"Action Input|Observation|Action:", re.I)

    def __init__(self, original, q: queue.Queue):
        self._orig   = original
        self._q      = q
        self._buf    = ""
        self._agent  = "system"

    def _detect_agent(self, line: str):
        for pat, name in self.AGENT_PATTERNS:
            if pat.search(line):
                self._agent = name
                return
        if "Tool" in line or "Action" in line:
            self._agent = self._agent  # keep current

    def write(self, text: str):
        self._orig.write(text)
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            line = line.strip()
            if not line or self.SKIP.match(line):
                continue
            self._detect_agent(line)
            kind = "tool" if self.TOOL_RE.search(line) else "step"
            self._q.put({"kind": kind, "agent": self._agent,
                         "text": line, "ts": _ts()})

    def flush(self):
        self._orig.flush()


# ── Crew thread ───────────────────────────────────────────────────────────────
def _run_crew(topic: str, q: queue.Queue):
    writer = _QueueWriter(sys.stdout, q)
    sys.stdout = writer
    try:
        def on_step(step_output):
            text = str(step_output).strip()
            if text:
                q.put({"kind": "step", "agent": writer._agent, "text": text, "ts": _ts()})

        def on_task(task_output):
            q.put({
                "kind":  "task_done",
                "agent": str(getattr(task_output, "agent", "system")),
                "text":  f"Task complete — {str(task_output.description or '')[:80]}…",
                "ts":    _ts(),
            })

        crew   = build_crew(topic, step_callback=on_step, task_callback=on_task)
        result = crew.kickoff()
        q.put({"kind": "result", "agent": "", "text": str(result), "ts": _ts()})
    except Exception as exc:
        q.put({"kind": "result", "agent": "", "text": f"Error: {exc}", "ts": _ts()})
    finally:
        sys.stdout = writer._orig


# ── UI ────────────────────────────────────────────────────────────────────────
st.markdown("# ✦ Kairos Research Crew")
st.markdown(
    "<p style='color:#555;font-size:0.85rem;margin-top:-10px'>"
    "CrewAI · Claude · Streamlit — two agents, one topic, one summary</p>",
    unsafe_allow_html=True,
)

# ── Input row ─────────────────────────────────────────────────────────────────
col_in, col_btn = st.columns([5, 1])
with col_in:
    topic = st.text_input(
        "Research topic",
        value="Camarilla pivot points in trading",
        label_visibility="collapsed",
        placeholder="Enter any topic…",
    )
with col_btn:
    run_clicked = st.button(
        "Run Crew →",
        use_container_width=True,
        disabled=st.session_state.running,
        type="primary",
    )

if run_clicked and topic.strip() and not st.session_state.running:
    # Reset state for a fresh run
    st.session_state.events      = []
    st.session_state.result      = None
    st.session_state.running     = True
    st.session_state.done        = False
    st.session_state.active_agent = None
    st.session_state.task_done   = []
    st.session_state.q           = queue.Queue()

    t = threading.Thread(
        target=_run_crew,
        args=(topic.strip(), st.session_state.q),
        daemon=True,
    )
    t.start()
    st.session_state.thread = t

st.markdown("---")

# ── Agent cards ───────────────────────────────────────────────────────────────
left, right = st.columns(2)
active = st.session_state.active_agent or ""
r_done = "Content Strategist" in st.session_state.task_done or st.session_state.done
rs_done = r_done and not st.session_state.running  # summarizer done = whole crew done

with left:
    r_active = "Research" in active or "Researcher" in active
    r_task_done = any("research" in e.get("kind","").lower() or
                      ("task_done" in e.get("kind","") and "Research" in e.get("agent",""))
                      for e in st.session_state.events)
    st.markdown(
        _agent_card(
            "Research Analyst", "Agent 1", "🔍",
            active=r_active and st.session_state.running,
            done=r_task_done or (not st.session_state.running and st.session_state.done),
        ),
        unsafe_allow_html=True,
    )

with right:
    s_active = "Content" in active or "Summar" in active or "Strategist" in active
    s_done   = not st.session_state.running and st.session_state.done
    st.markdown(
        _agent_card(
            "Content Strategist", "Agent 2", "✍️",
            active=s_active and st.session_state.running,
            done=s_done,
        ),
        unsafe_allow_html=True,
    )

# ── Live event feed ───────────────────────────────────────────────────────────
feed_placeholder  = st.empty()
final_placeholder = st.empty()

# ── Poll loop (reruns Streamlit while crew is running) ────────────────────────
if st.session_state.running:
    _drain_queue()
    # Render current events
    with feed_placeholder.container():
        st.markdown('<div class="section-label">Activity Feed</div>', unsafe_allow_html=True)
        for evt in st.session_state.events[-40:]:   # show last 40 events
            st.markdown(_note_html(evt), unsafe_allow_html=True)
    time.sleep(0.5)
    st.rerun()

elif st.session_state.events or st.session_state.result:
    _drain_queue()
    with feed_placeholder.container():
        st.markdown('<div class="section-label">Activity Feed</div>', unsafe_allow_html=True)
        for evt in st.session_state.events[-60:]:
            st.markdown(_note_html(evt), unsafe_allow_html=True)

    if st.session_state.result:
        with final_placeholder.container():
            st.markdown('<div class="section-label">Final Summary</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="final-box">{st.session_state.result}</div>',
                unsafe_allow_html=True,
            )
            st.download_button(
                "⬇ Download Summary",
                data=str(st.session_state.result),
                file_name=f"summary_{topic[:30].replace(' ','_')}.txt",
                mime="text/plain",
            )

elif not st.session_state.running and not st.session_state.done:
    st.markdown(
        '<p style="color:#444;font-size:0.85rem;margin-top:24px">'
        'Enter a topic above and click <strong style="color:#7FE098">Run Crew →</strong> '
        'to watch the agents work.</p>',
        unsafe_allow_html=True,
    )
