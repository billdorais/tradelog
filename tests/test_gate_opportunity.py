"""Gate opportunity cost — price each blocked entry against the ungated farm.

The farms are deliberately ungated on hours / RVOL / profit-lock / daily-loss, so
when one of those stops a curated book the farm usually still took the setup. That
farm round-trip is the counterfactual. The interesting cases are the ones where
there ISN'T one: day-type is ON for the farms too, so those blocks have no control
group and must be reported as unanswerable rather than as zero.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import datetime as dt
import sqlite3

import pytest

import app as a


TODAY = dt.datetime.now(dt.timezone.utc).date().isoformat()


def _rt(ticker, strategy, pnl, entry_price=100.0, qty=10, date=TODAY):
    return {"ticker": ticker, "strategy": strategy, "pnl": pnl, "date": date,
            "entry_price": entry_price, "qty": qty, "side": "LONG",
            "entry_time": f"{date}T14:00:00Z", "exit_time": f"{date}T14:30:00Z"}


@pytest.fixture()
def opp(monkeypatch, tmp_path):
    db = tmp_path / "opp.db"
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE blocked_targets (
        ts TEXT NOT NULL, account TEXT NOT NULL, ticker TEXT, strategy TEXT,
        side TEXT, gate TEXT NOT NULL, reason TEXT, source TEXT)""")
    conn.commit(); conn.close()

    def _fake_db():
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row; return c

    monkeypatch.setattr(a, "get_db", _fake_db)
    with a._blocked_q_lock:
        a._blocked_queue.clear()
    a._blocked_seen.clear(); a._blocked_seen_day = None

    farm_rts = []
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [
        {"tag": "alpaca", "num": "1", "label": "TV Farm", "fills_fn": lambda: ["x"]},
        {"tag": "alpaca4", "num": "4", "label": "Crew Paper", "fills_fn": lambda: []},
    ])
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda fills, **kw: {"closed_clean": list(farm_rts)})
    a.app.config["TESTING"] = True
    yield farm_rts
    with a._blocked_q_lock:
        a._blocked_queue.clear()


def _block(gate, ticker, strategy="S_CAM_BREAKOUT_R3S3"):
    a._record_block("alpaca4", ticker, strategy, "long", gate, "reason")
    a._flush_blocked_targets()


def _gates(days=14, account="alpaca4"):
    with a.app.test_client() as c:
        d = c.get(f"/api/signals/gate_opportunity?days={days}&account={account}").get_json()
    book = next(b for b in d["books"] if b["account"] == account)
    return d, {g["gate"]: g for g in book["by_gate"]}


def test_gate_that_blocked_a_winner_is_reported_as_costing_money(opp):
    farm_rts = opp
    farm_rts.append(_rt("AAPL", "S_CAM_BREAKOUT_R3S3", pnl=50.0))   # farm took it, +$50
    _block("hours", "AAPL")
    d, g = _gates()
    assert g["hours"]["blocked"] == 1 and g["hours"]["matched"] == 1
    assert g["hours"]["avg_pct"] == pytest.approx(5.0)      # 50 / (100*10) = 5%
    assert g["hours"]["verdict"] == "gate COST money"


def test_gate_that_blocked_a_loser_is_reported_as_saving_money(opp):
    farm_rts = opp
    farm_rts.append(_rt("TSLA", "S_CAM_BREAKOUT_R3S3", pnl=-80.0))
    _block("rvol", "TSLA")
    _, g = _gates()
    assert g["rvol"]["verdict"] == "gate SAVED money"
    assert g["rvol"]["avg_pct"] < 0


def test_a_gate_the_farms_share_is_unanswerable(opp, monkeypatch):
    """A gate the farms are ALSO subject to has no counterfactual, and must not be
    scored as zero — zero reads as 'the gate cost nothing', a different claim.
    Farms are ungated on day-type as of 2026-08-13, so re-gate one to exercise it."""
    monkeypatch.setattr(a, "DAYTYPE_GATE_ACCOUNTS",
                        set(a.DAYTYPE_GATE_ACCOUNTS) | {"alpaca"})
    farm_rts = opp
    farm_rts.append(_rt("UBER", "S_CAM_BREAKOUT_R3S3", pnl=99.0))   # must be ignored
    _block("day-type", "UBER")
    _, g = _gates()
    row = g["day-type"]
    assert row["unanswerable"] is True
    assert row["matched"] == 0 and row["farm_pnl"] == 0.0
    assert "no control group" in row["verdict"]


def test_day_type_is_priceable_now_that_the_farms_are_ungated(opp):
    """With the farms ungated, day-type blocks finally get a counterfactual —
    the reason for turning those gates off on the farms."""
    farm_rts = opp
    farm_rts.append(_rt("UBER", "S_CAM_BREAKOUT_R3S3", pnl=99.0))
    _block("day-type", "UBER")
    _, g = _gates()
    assert g["day-type"]["unanswerable"] is False
    assert g["day-type"]["matched"] == 1


def test_no_farm_match_is_distinguished_from_a_zero_result(opp):
    """The farm not taking it at all is a different statement from breakeven."""
    _block("hours", "NOSUCH")
    _, g = _gates()
    assert g["hours"]["blocked"] == 1 and g["hours"]["matched"] == 0
    assert g["hours"]["verdict"] == "no farm match"


def test_loose_match_is_flagged_when_the_strategy_differs(opp):
    """Same ticker and day but a different strategy still gives a usable read —
    but it is a weaker claim, so it is counted separately."""
    farm_rts = opp
    farm_rts.append(_rt("GOOG", "OTHER_CAM_REVERSAL_R4S4", pnl=20.0))
    _block("profit-lock", "GOOG", strategy="GOOG_CAM_BREAKOUT_R3S3")
    _, g = _gates()
    assert g["profit-lock"]["matched"] == 1
    assert g["profit-lock"]["loose_matches"] == 1


def test_percent_normalisation_not_raw_dollars(opp):
    """Farms are equal-dollar sized, so raw $ are not transferable to Crew. A big
    farm position and a small one with the same % must score the same."""
    farm_rts = opp
    farm_rts.append(_rt("AAPL", "S_CAM_BREAKOUT_R3S3", pnl=100.0,
                        entry_price=100.0, qty=20))   # 100/2000 = 5%
    _block("hours", "AAPL")
    _, g = _gates()
    assert g["hours"]["avg_pct"] == pytest.approx(5.0)
    assert g["hours"]["farm_pnl"] == 100.0          # dollars still shown for reference


def test_prices_every_curated_book_in_one_call(opp):
    """All three curated books side by side — the farm index is built once and
    shared, so adding books costs no extra fill fetches."""
    farm_rts = opp
    farm_rts.append(_rt("AAPL", "S_CAM_BREAKOUT_R3S3", pnl=40.0))    # +4%
    farm_rts.append(_rt("TSLA", "S_CAM_BREAKOUT_R3S3", pnl=-60.0))   # -6%
    # Same gate, opposite outcomes on two books.
    a._record_block("alpaca2", "AAPL", "S_CAM_BREAKOUT_R3S3", "long", "hours", "r")
    a._record_block("alpaca3", "TSLA", "S_CAM_BREAKOUT_R3S3", "long", "hours", "r")
    a._record_block("alpaca4", "AAPL", "S_CAM_BREAKOUT_R3S3", "long", "rvol", "r")
    a._flush_blocked_targets()

    with a.app.test_client() as c:
        d = c.get("/api/signals/gate_opportunity?days=14").get_json()

    books = {b["account"]: b for b in d["books"]}
    assert set(books) == {"alpaca2", "alpaca3", "alpaca4", "alpaca6"}, "defaulted to the curated books"
    # The union of gates drives the matrix rows, most-blocked first.
    assert set(d["gates"]) == {"hours", "rvol"}
    assert d["gates"][0] == "hours"

    g2 = {g["gate"]: g for g in books["alpaca2"]["by_gate"]}
    g3 = {g["gate"]: g for g in books["alpaca3"]["by_gate"]}
    # Same gate, opposite verdicts per book — the whole point of splitting them.
    assert g2["hours"]["verdict"] == "gate COST money"
    assert g3["hours"]["verdict"] == "gate SAVED money"
    assert books["alpaca4"]["total_blocks"] == 1


def test_explicit_accounts_list_is_honoured(opp):
    a._record_block("alpaca2", "AAPL", "S", "long", "hours", "r")
    a._record_block("alpaca4", "AAPL", "S", "long", "hours", "r")
    a._flush_blocked_targets()
    with a.app.test_client() as c:
        d = c.get("/api/signals/gate_opportunity?days=14&accounts=alpaca4").get_json()
    assert [b["account"] for b in d["books"]] == ["alpaca4"]
    assert d["total_blocks"] == 1, "queried outside the requested accounts"


def test_books_are_ordered_like_every_other_surface(opp):
    """Columns follow UI_ACCOUNT_ORDER — Crew, Kairos Refined, TV Refined — the
    same order as the dashboard tabs, not the arbitrary iteration order of a set."""
    import app as _a
    a.ALPACA_ACCOUNTS.extend([
        {"tag": "alpaca2", "num": "2", "label": "TV Refined", "fills_fn": lambda: []},
        {"tag": "alpaca3", "num": "3", "label": "Kairos Refined", "fills_fn": lambda: []},
    ])
    for acct in ("alpaca4", "alpaca3", "alpaca2"):        # inserted out of order
        _a._record_block(acct, "AAPL", "S", "long", "hours", "r")
    _a._flush_blocked_targets()
    with a.app.test_client() as c:
        d = c.get("/api/signals/gate_opportunity?days=14").get_json()
    assert [b["account"] for b in d["books"]] == ["alpaca6", "alpaca4", "alpaca3", "alpaca2"]


# ── Dollar estimate at the book's own position size ───────────────────────────

def test_estimates_dollars_at_the_books_own_position_size(opp, monkeypatch):
    """A farm % is only meaningful once re-priced at the size THIS book trades."""
    farm_rts = opp
    farm_rts.append(_rt("AAPL", "S", pnl=50.0, entry_price=100.0, qty=10))   # +5%
    # Crew's own recent positions are ~$2,000 notional → 5% of 2000 = $100.
    monkeypatch.setattr(a, "_book_position_notional",
                        lambda tag, f, t, fills_fn=None: (2000.0, 6))
    _block("hours", "AAPL")
    d, g = _gates()
    book = next(b for b in d["books"] if b["account"] == "alpaca4")
    assert book["position_size"] == 2000.0
    assert "median of 6" in book["position_size_source"]
    assert g["hours"]["est_dollars"] == pytest.approx(100.0)
    assert book["est_dollars_total"] == pytest.approx(100.0)


def test_size_override_is_honoured(opp, monkeypatch):
    farm_rts = opp
    farm_rts.append(_rt("AAPL", "S", pnl=50.0, entry_price=100.0, qty=10))   # +5%
    _block("hours", "AAPL")
    with a.app.test_client() as c:
        d = c.get("/api/signals/gate_opportunity?days=14&account=alpaca4&size=1000").get_json()
    book = d["books"][0]
    assert book["position_size"] == 1000.0
    assert book["position_size_source"] == "override"
    assert book["by_gate"][0]["est_dollars"] == pytest.approx(50.0)   # 5% of 1000


def test_estimate_is_null_not_zero_when_size_is_unknown(opp, monkeypatch):
    """A book with no recent trades has no basis for a dollar figure. Reporting 0
    would read as 'this gate cost nothing', which is a different claim."""
    farm_rts = opp
    farm_rts.append(_rt("AAPL", "S", pnl=50.0))
    monkeypatch.setattr(a, "_book_position_notional",
                        lambda tag, f, t, fills_fn=None: (None, 0))
    _block("hours", "AAPL")
    d, g = _gates()
    book = next(b for b in d["books"] if b["account"] == "alpaca4")
    assert g["hours"]["est_dollars"] is None
    assert book["est_dollars_total"] is None
    assert "no recent trades" in book["position_size_source"]


def test_notional_uses_median_not_mean(opp, monkeypatch):
    """One oversized position must not drag the assumed size upward."""
    rts = [_rt("A", "S", 1.0, entry_price=100.0, qty=10),      # 1,000
           _rt("B", "S", 1.0, entry_price=100.0, qty=10),      # 1,000
           _rt("C", "S", 1.0, entry_price=100.0, qty=500)]     # 50,000 outlier
    monkeypatch.setattr(a, "_pair_alpaca_fills_lifo",
                        lambda fills, **kw: {"closed_clean": rts})
    size, n = a._book_position_notional("alpaca4", "2026-01-01", "2026-12-31",
                                        fills_fn=lambda: ["x"])
    assert size == 1000.0 and n == 3      # mean would be ~17,333


# ── Which gates have a control group is CONFIG-derived, not hardcoded ─────────

def test_farms_are_exempt_from_the_curated_gates():
    """The farms are the control group; sharing a gate with the curated books
    makes that gate's blocks unpriceable. Guards against a farm being re-gated
    without anyone noticing the analysis went blind."""
    for farm in a._FARM_TAGS:
        assert farm not in a.DAYTYPE_GATE_ACCOUNTS, f"{farm} re-gated on day-type"
        assert farm not in a.DAYTYPE_REVERSAL_GATE_ACCOUNTS, f"{farm} re-gated on reversal day-type"
        assert not a._REVERSAL_SIDE_BY_TAG.get(farm), f"{farm} has a reversal-side policy"
    assert a._gates_without_control() == set(), "some gate lost its control group"


def test_no_control_follows_config_if_a_farm_is_re_gated(monkeypatch):
    """If a farm ever shares the day-type gate again, that gate must go back to
    reporting 'no control group' rather than a bogus number."""
    monkeypatch.setattr(a, "DAYTYPE_GATE_ACCOUNTS", set(a.DAYTYPE_GATE_ACCOUNTS) | {"alpaca"})
    assert "day-type" in a._gates_without_control()


def test_reversal_blocks_are_priceable(opp):
    """Regression: 'reversal' was hardcoded as unanswerable, but the farms carry no
    reversal-side policy, so those blocks always had a counterfactual. That
    mislabel hid roughly a third of the priceable sample."""
    farm_rts = opp
    farm_rts.append(_rt("AMZN", "S_CAM_REVERSAL_R4S4", pnl=30.0))
    a._record_block("alpaca4", "AMZN", "S_CAM_REVERSAL_R4S4", "short", "reversal", "policy")
    a._flush_blocked_targets()
    _, g = _gates()
    assert g["reversal"]["unanswerable"] is False
    assert g["reversal"]["matched"] == 1
    assert g["reversal"]["verdict"] == "gate COST money"
