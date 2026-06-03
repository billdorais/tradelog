"""
Kairos Research Crew
--------------------
Two-agent CrewAI crew: Researcher → Summarizer.

Core concepts demonstrated:
  Agent  — role, goal, backstory, llm, tools, verbose
  Task   — description, expected_output, agent, context (dependency)
  Tool   — @tool decorator wrapping a plain Python function
  Crew   — process (sequential), callbacks, kickoff()
  LLM    — Claude via LiteLLM ("anthropic/claude-sonnet-4-6")
"""

import os
import wikipedia as _wiki
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process, LLM
from crewai.tools import tool

load_dotenv()


# ── Tool ─────────────────────────────────────────────────────────────────────
# The @tool decorator turns a plain function into something an agent can call.
# The docstring becomes the tool description the LLM reads when deciding
# whether to use it.

@tool("Wikipedia Search")
def search_wikipedia(query: str) -> str:
    """
    Search Wikipedia for factual information on a topic.
    Returns the first ~4 000 characters of the best matching article.
    Use specific, focused queries for better results.
    """
    try:
        hits = _wiki.search(query, results=3)
        if not hits:
            return "No Wikipedia results found for that query."
        page = _wiki.page(hits[0], auto_suggest=False)
        return page.content[:4000]
    except _wiki.DisambiguationError as e:
        # Pick the first non-ambiguous option
        try:
            page = _wiki.page(e.options[0], auto_suggest=False)
            return page.content[:4000]
        except Exception:
            return f"Ambiguous topic — try a more specific query. Options: {e.options[:5]}"
    except Exception as exc:
        return f"Wikipedia search failed: {exc}"


# ── LLM ──────────────────────────────────────────────────────────────────────
# CrewAI routes through LiteLLM so any supported provider works.
# The model string format is "provider/model-id".

def make_llm(temperature: float = 0.3) -> LLM:
    return LLM(
        model="anthropic/claude-sonnet-4-6",
        api_key=os.environ.get("ANTHROPIC_API_KEY"),
        temperature=temperature,
        max_tokens=2048,
    )


# ── Crew builder ─────────────────────────────────────────────────────────────
# Accepts optional callbacks so the Streamlit app can hook into agent activity.

def build_crew(
    topic: str,
    step_callback=None,
    task_callback=None,
) -> Crew:
    """
    Build a two-agent research + summarise crew for the given topic.

    Parameters
    ----------
    topic          : The subject to research.
    step_callback  : Called after every agent thought/action step.
                     Receives the raw step output string.
    task_callback  : Called after every completed task.
                     Receives the TaskOutput object.
    """
    llm = make_llm()

    # ── Agent 1: Researcher ───────────────────────────────────────────────
    # role     — job title the LLM plays
    # goal     — what success looks like for this agent
    # backstory — context that shapes tone and approach
    # tools    — list of callables the agent may invoke
    # verbose  — prints chain-of-thought to stdout (great for learning)

    researcher = Agent(
        role="Research Analyst",
        goal=(
            f"Gather comprehensive, accurate information about '{topic}' "
            "and return structured research notes."
        ),
        backstory=(
            "You are a meticulous research analyst who excels at locating "
            "reliable facts and synthesising them into clear notes. "
            "You always cite your source and flag anything uncertain."
        ),
        llm=llm,
        tools=[search_wikipedia],
        verbose=True,
        allow_delegation=False,
    )

    # ── Agent 2: Summarizer ───────────────────────────────────────────────
    # No tools — this agent only reasons over the researcher's output.

    summarizer = Agent(
        role="Content Strategist",
        goal=(
            "Turn raw research notes into a concise, engaging 3-paragraph "
            "summary that any reader can understand."
        ),
        backstory=(
            "You are a gifted communicator who distils complex material into "
            "crisp, readable narratives. You know what to keep, what to cut, "
            "and how to make technical content feel effortless to read."
        ),
        llm=make_llm(temperature=0.5),
        tools=[],
        verbose=True,
        allow_delegation=False,
    )

    # ── Task 1: Research ──────────────────────────────────────────────────
    # description     — instructions the agent reads at runtime
    # expected_output — tells the LLM what a good response looks like
    # agent           — which agent executes this task

    research_task = Task(
        description=(
            f"Research the topic: **{topic}**\n\n"
            "Steps:\n"
            "1. Use the Wikipedia Search tool with 1-2 focused queries.\n"
            "2. Extract the 5 most important facts or concepts.\n"
            "3. Note 1-2 surprising or counter-intuitive insights.\n"
            "4. Flag any significant uncertainties.\n\n"
            "Return your findings as clearly labelled bullet-point notes."
        ),
        expected_output=(
            "Structured research notes containing:\n"
            "• 5 key facts (each with a one-sentence explanation)\n"
            "• 1-2 surprising insights\n"
            "• Any important caveats or open questions"
        ),
        agent=researcher,
    )

    # ── Task 2: Summarise ─────────────────────────────────────────────────
    # context — list of tasks whose output is injected as context.
    #           This is how agents hand off work to each other.

    summary_task = Task(
        description=(
            "Using the research notes provided, write a 3-paragraph summary "
            "for a general audience with no prior knowledge of the topic.\n\n"
            "Paragraph 1 — What it is and why it matters.\n"
            "Paragraph 2 — The key facts, connected into a coherent narrative.\n"
            "Paragraph 3 — The most interesting insight or takeaway."
        ),
        expected_output=(
            "Three well-written paragraphs (4-6 sentences each), "
            "accurate to the research notes, free of jargon."
        ),
        agent=summarizer,
        context=[research_task],   # <── dependency: waits for research output
    )

    # ── Crew ──────────────────────────────────────────────────────────────
    # process=sequential — tasks run in order, each agent waits for the previous.
    # Callbacks let external code (e.g. Streamlit) observe execution in real time.

    crew = Crew(
        agents=[researcher, summarizer],
        tasks=[research_task, summary_task],
        process=Process.sequential,
        verbose=True,
        step_callback=step_callback,
        task_callback=task_callback,
    )

    return crew


# ── CLI entry point ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    topic = " ".join(sys.argv[1:]) or "Camarilla pivot points in trading"
    print(f"\n🔍 Researching: {topic}\n{'─' * 50}")
    crew = build_crew(topic)
    result = crew.kickoff()
    print(f"\n{'─' * 50}\n✅ FINAL SUMMARY\n{'─' * 50}\n{result}\n")
