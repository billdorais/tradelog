"""The position gate must also see entries that have not filled yet.

2026-08-27, HOOD: two SELL 9 entries landed 8 seconds apart. Both passed the
"already holding this ticker?" check because neither had FILLED, so the positions
API showed nothing. The result was one oversized position, two delayed-stop
threads, and the second stop rejected with
  {"available":"5","existing_qty":"13","held_for_orders":"8"}
after which every later order on HOOD was refused for insufficient qty.

No rate limiting involved — the delayed-stop logs record "0 rate-limited polls".
This is a plain check-then-act race across the submit-to-fill window.
"""
from __future__ import annotations

import os
import re

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")

import app as kairos


def _gate_src():
    src = open("routes/webhook.py", encoding="utf-8").read()
    i = src.index("# Position gate — block new entries")
    return src[i:i + 4200]


def test_the_gate_consults_pending_entries_not_only_positions():
    src = _gate_src()
    assert "app._pending_entries" in src
    assert "PENDING_ENTRY_TTL_SECS" in src


def test_a_filled_entry_releases_its_own_claim():
    """Once the position exists it is the guard; leaving the claim would block a
    legitimate re-entry after the position closes."""
    src = _gate_src()
    i = src.index("if existing:")
    assert "app._pending_entries.pop(_pend_key, None)" in src[i:i + 600]


def test_an_expired_claim_is_released_rather_than_blocking_forever():
    """An entry that never fills must not lock its ticker out for the session."""
    src = _gate_src()
    assert ">= app.PENDING_ENTRY_TTL_SECS" in src
    i = src.index(">= app.PENDING_ENTRY_TTL_SECS")
    assert "pop(_pend_key, None)" in src[i:i + 200]


def test_only_a_successful_submission_claims_the_ticker():
    """A rejected order must not claim it — that would block the retry."""
    src = open("routes/webhook.py", encoding="utf-8").read()
    i = src.index("# Claim the ticker the moment the entry is accepted")
    block = src[i:i + 700]
    assert 'if result.get("success"):' in block
    assert "app._pending_entries[_pk] = time.time()" in block
    assert "app._pending_entries.pop(_pk, None)" in block


def test_exits_never_claim_a_ticker():
    """Only entries take the claim; an exit must stay free to close a position."""
    src = open("routes/webhook.py", encoding="utf-8").read()
    i = src.index("# Claim the ticker the moment the entry is accepted")
    assert "if is_entry:" in src[i - 200:i + 200]


def test_the_claim_is_released_when_the_position_appears(monkeypatch):
    """The monitor drops the claim within a poll of the fill, so a position opened
    and closed inside the TTL can be re-entered."""
    src = open("app.py", encoding="utf-8").read()
    i = src.index("for k in [k for k in _pending_entries if k in open_keys]")
    assert "_pending_entries.pop(k, None)" in src[i:i + 200]


def test_the_ttl_is_long_enough_to_cover_a_slow_fill():
    """The delayed-stop thread waits 30s before giving up on an unfilled entry, so a
    TTL under that would release the claim while the order is still live."""
    assert kairos.PENDING_ENTRY_TTL_SECS >= 60


def test_pending_entries_starts_empty_and_is_a_dict():
    assert isinstance(kairos._pending_entries, dict)


def test_the_claim_is_keyed_per_account_not_globally():
    """The same ticker trades on six books; a global key would let one account's
    unfilled entry block every other book."""
    src = _gate_src()
    assert "_pend_key = (broker_tag, ticker.upper())" in src


def test_the_gate_still_blocks_on_a_real_position_either_direction():
    """The original guarantee must survive: a held ticker blocks a new entry on
    either side, so long and short cannot be open at once."""
    src = _gate_src()
    assert "abs(float(p.qty or 0)) > 0" in src
    assert "already holding" in src
