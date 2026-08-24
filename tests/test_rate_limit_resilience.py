"""Surviving the 09:35 burst.

Every curated book opens its window at the same instant, so 09:35 fires a wave of
entries plus the fill-polls and exit-stops each one spawns. On 2026-08-24 Alpaca
answered with 429s and nothing handled them:

  - SELL 2 AMD and SELL 14 RKLB were rejected and simply lost
  - ~20 filled positions ended up with NO broker-side stop, because their
    fill-polls were rate-limited until the delayed-stop thread gave up
  - the position gate failed open ("proceeding with order") on MS and MSTR

These pin the handling. The distinction that matters throughout: a 429 means the
request was REJECTED, so retrying cannot duplicate anything. That is what makes
retrying safe here and unsafe for an ambiguous failure like a timeout.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a
from brokers import alpaca_broker as ab


class _RateLimited(Exception):
    def __str__(self):
        return '{"code":42910000,"message":"rate limit exceeded"}'


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    """No real sleeping — backoff timing is not what these assert."""
    monkeypatch.setattr(ab.time, "sleep", lambda *_: None)


# ── what counts as a rate limit ─────────────────────────────────────────────

def test_recognises_alpacas_rate_limit_shapes():
    assert ab._is_rate_limited(_RateLimited())
    assert ab._is_rate_limited(Exception("rate limit exceeded"))
    assert ab._is_rate_limited(Exception('{"code":42910000}'))


def test_does_not_mistake_other_failures_for_rate_limits():
    """Retrying the wrong error is how you place an order twice."""
    for other in ('{"code":42210000,"message":"asset \\"SOXL\\" cannot be sold short"}',
                  '{"code":40310000,"message":"insufficient buying power"}',
                  "Read timed out", "Connection reset by peer"):
        assert not ab._is_rate_limited(Exception(other)), other


# ── the retry itself ────────────────────────────────────────────────────────

def test_a_rate_limited_call_is_retried_and_succeeds():
    calls = {"n": 0}

    def _flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise _RateLimited()
        return "ok"

    assert ab._retry_rate_limited(_flaky, what="test") == "ok"
    assert calls["n"] == 3


def test_other_errors_propagate_immediately_without_retrying():
    calls = {"n": 0}

    def _boom():
        calls["n"] += 1
        raise ValueError("not a rate limit")

    with pytest.raises(ValueError):
        ab._retry_rate_limited(_boom, what="test")
    assert calls["n"] == 1, "a non-rate-limit error must not be retried"


def test_it_gives_up_rather_than_retrying_forever():
    calls = {"n": 0}

    def _always():
        calls["n"] += 1
        raise _RateLimited()

    with pytest.raises(Exception):
        ab._retry_rate_limited(_always, what="test", attempts=4)
    assert calls["n"] == 4


def test_the_order_path_is_wrapped():
    """AMD and RKLB were lost here specifically."""
    import inspect
    src = inspect.getsource(ab.AlpacaBroker.place_order)
    assert "_retry_rate_limited" in src
    assert "submit_order(req)" in src.replace("lambda: self._trading.", "")


def test_the_positions_fetch_is_wrapped():
    """It feeds the position-stop monitor and the position gate — the gate failing
    open is how a duplicate position gets through."""
    import inspect
    assert "_retry_rate_limited" in inspect.getsource(ab.AlpacaBroker._get_positions_cached)


# ── never abandon a live position ───────────────────────────────────────────

def test_a_rate_limited_poll_is_not_evidence_the_order_is_unfilled():
    """The bug: a 429 on the fill-poll means 'could not look', but it was treated as
    'not filled'. After 30s of that the thread gave up and the position kept no stop."""
    import inspect
    src = inspect.getsource(ab.AlpacaBroker._delayed_attach_exits) \
        if hasattr(ab.AlpacaBroker, "_delayed_attach_exits") else ""
    if not src:                      # name differs — find the give-up text instead
        src = inspect.getsource(ab)
    assert "_is_rate_limited(_ge)" in src, "poll errors must distinguish rate limits"
    assert "get_open_position" in src, "must check for a real position before giving up"
    assert "ARE held" in src, "an abandoned live position must log as an ERROR"


# ── learned non-shortables ──────────────────────────────────────────────────

def test_a_short_rejection_is_remembered(monkeypatch):
    """SOXL was not in the static list, so its short errored out and would have
    errored again every day. One rejection is enough to learn from."""
    monkeypatch.setattr(a, "_non_shortable_learned", set())
    assert a._is_non_shortable("SOXL") is False
    a._note_non_shortable("SOXL")
    assert a._is_non_shortable("SOXL") is True
    assert a._is_non_shortable("soxl") is True, "case must not matter"


def test_the_static_list_still_applies(monkeypatch):
    monkeypatch.setattr(a, "_non_shortable_learned", set())
    assert a._is_non_shortable("SPCX") is True


def test_learning_is_idempotent_and_ignores_blanks(monkeypatch):
    monkeypatch.setattr(a, "_non_shortable_learned", set())
    for _ in range(3):
        a._note_non_shortable("SOXL")
    a._note_non_shortable("")
    a._note_non_shortable(None)
    assert a._non_shortable_learned == {"SOXL"}


def test_the_broker_reports_short_rejections_back(monkeypatch):
    import inspect
    src = inspect.getsource(ab.AlpacaBroker.place_order)
    assert "cannot be sold short" in src and "_note_non_shortable" in src


# ── the Postgres-only failure ───────────────────────────────────────────────

def test_blocked_signals_does_not_use_sqlite_only_sql():
    """`datetime('now', '-N days')` raises on Postgres, so this endpoint returned
    nothing but an error in production while working fine locally on sqlite."""
    import inspect
    src = inspect.getsource(a.api_blocked_signals)
    # Comment lines are excluded — the fix documents the old SQL by quoting it.
    code = " ".join(l for l in src.splitlines() if not l.strip().startswith("#"))
    assert "datetime('now'" not in code
    assert "timedelta(days=days)" in code


def test_no_sqlite_only_date_sql_remains_anywhere():
    for f in ("app.py", "routes/webhook.py", "routes/crew.py"):
        try:    src = open(f, encoding="utf-8").read()
        except FileNotFoundError: continue
        for line in src.splitlines():
            if "datetime('now'" in line and not line.strip().startswith("#"):
                pytest.fail(f"{f}: SQLite-only date SQL: {line.strip()}")
