"""Gate reference (/api/gates/docs) — what each gate does, next to its live setting.

The Refusal table and the recap gate strip both NAME gates without explaining
them. These docs back the modal behind every gate name on the page.

The load-bearing part is that state is resolved per account through
GATES_BY_ACCOUNT overrides, not from the shared globals. A book can opt IN to a
gate through an override without ever joining that gate's shared account set, so
reading `RVOL_GATE_ENABLED and tag in RVOL_GATE_ACCOUNTS` reports a gate as off
while it is actively blocking that book's entries.
"""
from __future__ import annotations

import os
import re

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a


@pytest.fixture(autouse=True)
def _no_db(monkeypatch):
    """Manual-halt state is the one gate that reads the DB; pin it so these tests
    describe config, not whatever a local sqlite file happens to hold."""
    monkeypatch.setattr(a, "_manual_halted_for", lambda tag: False)


def _client():
    a.app.config["TESTING"] = True
    return a.app.test_client()


def _override(monkeypatch, tag, cfg):
    monkeypatch.setattr(a, "_account_gate_overrides",
                        lambda t=None: (cfg if t == tag else {}) if t is not None else {tag: cfg})


# ── the docs themselves ─────────────────────────────────────────────────────

def test_every_documented_gate_is_complete():
    for k, r in a._GATE_RULES.items():
        assert r.get("title") and r.get("what") and r.get("why"), k
        assert isinstance(r.get("rule"), list) and r["rule"], k


def test_every_gate_the_router_can_record_is_documented():
    """Drift guard. A new gate that starts recording blocks shows up in the Refusal
    table immediately — and would open an empty modal — unless it is documented
    here too. Scans the actual _record_block call sites rather than a hand list."""
    src   = open("app.py", encoding="utf-8").read()
    for f in ("routes/webhook.py", "routes/crew.py"):
        try:    src += open(f, encoding="utf-8").read()
        except FileNotFoundError: pass
    recorded = set(re.findall(
        r'_record_block\(\s*[^,]+,\s*[^,]+,\s*[^,]+,\s*[^,]+,\s*"([a-z-]+)"', src))
    assert recorded, "expected to find _record_block call sites"
    missing = recorded - set(a._GATE_RULES)
    assert not missing, f"gates recorded but undocumented: {sorted(missing)}"


@pytest.mark.parametrize("label,key", [
    ("Reversal side", "reversal"), ("Opening location", "open-location"),
    ("Profit lock", "profit-lock"), ("Daily loss", "daily-loss"),
    ("Day-type", "day-type"), ("RVOL", "rvol"), ("Trading hours", "hours"),
    ("open-location", "open-location"), ("hours", "hours"),
])
def test_display_labels_and_block_keys_both_resolve(label, key):
    """The strip says "Reversal side", the blocks table says "reversal" — one modal
    has to answer to both spellings."""
    assert a._gate_key(label) == key


# ── live state, resolved per book ───────────────────────────────────────────

def test_rvol_override_makes_the_gate_live_on_an_unwired_book(monkeypatch):
    """THE regression. Crew Paper carries no rvol_gate flag, so it is not in
    RVOL_GATE_ACCOUNTS — yet an override switches the gate on for it. Reading the
    globals reported "not wired" for a week in which it blocked 11 entries."""
    monkeypatch.setattr(a, "RVOL_GATE_ENABLED", False)
    monkeypatch.setattr(a, "RVOL_GATE_ACCOUNTS", {"alpaca2", "alpaca3"})
    _override(monkeypatch, "alpaca4", {"rvol": {"enabled": True, "min": 1.2, "short_cap": 2.5}})

    g = a._gate_docs("alpaca4")["rvol"]
    assert g["on"] is True, "an override must win over the shared account list"
    assert g["source"] == "override"
    assert "1.2x" in g["setting"] and "2.5x" in g["setting"]


def test_recap_gate_strip_agrees_with_the_override(monkeypatch):
    """The strip feeds the read-aloud script, so it must not narrate a gate as off
    while that gate is blocking trades."""
    monkeypatch.setattr(a, "ACCOUNTS_BY_NUM", {
        "4": {"tag": "alpaca4", "label": "Crew Paper",
              "broker": object(), "fills_fn": lambda: []}})
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo", lambda *A, **K: {"closed_clean": []})
    monkeypatch.setattr(a, "RVOL_GATE_ENABLED", False)
    monkeypatch.setattr(a, "RVOL_GATE_ACCOUNTS", {"alpaca2", "alpaca3"})
    _override(monkeypatch, "alpaca4", {"rvol": {"enabled": True}})

    gates = {g["gate"]: g for g in _client().get("/api/recap").get_json()["gates"]}
    assert gates["RVOL"]["on"] is True
    assert "not wired" not in gates["RVOL"]["detail"]
    assert all("key" in g for g in gates.values()), "the strip must carry modal keys"


def test_an_override_can_also_switch_a_wired_gate_off(monkeypatch):
    """The reverse direction: membership alone must not report a gate as live."""
    monkeypatch.setattr(a, "RVOL_GATE_ENABLED", True)
    monkeypatch.setattr(a, "RVOL_GATE_ACCOUNTS", {"alpaca2", "alpaca3"})
    _override(monkeypatch, "alpaca2", {"rvol": {"enabled": False}})
    g = a._gate_docs("alpaca2")["rvol"]
    assert g["on"] is False
    assert "switched off for this book" in g["setting"]


def test_unwired_and_unoverridden_still_reads_as_not_wired(monkeypatch):
    monkeypatch.setattr(a, "RVOL_GATE_ENABLED", True)
    monkeypatch.setattr(a, "RVOL_GATE_ACCOUNTS", {"alpaca2", "alpaca3"})
    monkeypatch.setattr(a, "_account_gate_overrides", lambda t=None: {})
    g = a._gate_docs("alpaca4")["rvol"]
    assert g["on"] is False and "not wired" in g["setting"]


def test_daytype_and_open_location_overrides_are_honoured(monkeypatch):
    monkeypatch.setattr(a, "DAYTYPE_GATE_ENABLED", False)
    monkeypatch.setattr(a, "OPEN_LOC_GATE_ENABLED", False)
    _override(monkeypatch, "alpaca4", {
        "daytype":  {"enabled": True, "breakout_ok_days": ["Outside", "Trend"]},
        "open_loc": {"enabled": True, "buckets": ["at/past extreme"], "sides": ["long"]}})
    docs = a._gate_docs("alpaca4")
    assert docs["day-type"]["on"] is True
    assert "Outside" in docs["day-type"]["setting"] and "Trend" in docs["day-type"]["setting"]
    assert docs["open-location"]["on"] is True
    assert "long" in docs["open-location"]["setting"]


def test_hours_setting_reports_the_books_own_window(monkeypatch):
    monkeypatch.setattr(a, "_account_hours_windows",
                        lambda tag: [("09:30", "15:55")] if tag == "alpaca4" else [])
    assert a._gate_docs("alpaca4")["hours"]["setting"] == "09:30-15:55 ET"
    assert a._gate_docs("alpaca4")["hours"]["on"] is True
    d = a._gate_docs("alpaca5")["hours"]
    assert d["on"] is False and "all day" in d["setting"]


def test_gates_without_a_control_group_say_so(monkeypatch):
    """A row with an em-dash in Est. $ needs a reason, not a shrug."""
    monkeypatch.setattr(a, "_gates_without_control", lambda: {"day-type"})
    docs = a._gate_docs("alpaca4")
    assert docs["day-type"]["answerable"] is False
    assert "no un-gated control" in docs["day-type"]["no_control_note"]
    assert docs["hours"]["answerable"] is True and docs["hours"]["no_control_note"] == ""


# ── endpoint ────────────────────────────────────────────────────────────────

def test_endpoint_returns_docs_for_a_known_book():
    d = _client().get("/api/gates/docs?account=alpaca4").get_json()
    assert d["account"] == "alpaca4" and d["label"] == "Crew Paper"
    assert set(d["gates"]) == set(a._GATE_RULES)
    assert d["gates"]["rvol"]["title"] == "RVOL (relative volume)"


def test_endpoint_rejects_an_unknown_book():
    r = _client().get("/api/gates/docs?account=alpaca99")
    assert r.status_code == 400 and "unknown account" in r.get_json()["error"]


def test_the_same_gate_reads_differently_on_different_books(monkeypatch):
    """Why the endpoint is account-scoped: reversal policy alone differs three ways."""
    settings = {t: a._gate_docs(t)["reversal"]["setting"]
                for t in ("alpaca", "alpaca2", "alpaca3")}
    assert settings["alpaca"]  == "both sides allowed"     # farm — the control
    assert settings["alpaca2"] == "reversals off"
    assert settings["alpaca3"] == "reversals long-only"
