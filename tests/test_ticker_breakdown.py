"""Per-ticker breakdown — one symbol, everything that traded it.

The other views slice a different way: recap is period-first, /strategy-explorer
is a strategy leaderboard, the dashboard is account-first. None answers "how is
AAPL doing, and which of its strategies and books are responsible" — and a ticker
routinely carries several strategies across six books whose results disagree.
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as kairos


def _t(ticker, strategy, side, pnl, day, qty=10, reason="trail"):
    return {"ticker": ticker, "strategy": strategy, "side": side.upper(), "pnl": pnl,
            "qty": qty, "entry_price": 100.0, "exit_price": 100.0 + pnl / qty,
            "exit_reason": reason,
            "entry_time": f"2026-08-{day:02d}T13:40:00+00:00",
            "exit_time":  f"2026-08-{day:02d}T13:55:00+00:00"}


BOOKS = {
    "2": [_t("AAPL", "AAPL_CAM_BREAKOUT_R3S3_V02_5MIN", "long",   60.0, 3),
          _t("AAPL", "AAPL_CAM_BREAKOUT_R3S3_V02_5MIN", "long",  -20.0, 4),
          _t("AAPL", "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "short", -50.0, 5, reason="max_hold"),
          _t("MSFT", "MSFT_CAM_REVERSAL_R3S3_V02_5MIN", "long",   10.0, 3)],
    "4": [_t("AAPL", "AAPL_CAM_BREAKOUT_R3S3_V02_5MIN", "long",   15.0, 6)],
}


@pytest.fixture
def client(monkeypatch):
    accounts = [{"num": "2", "tag": "alpaca2", "label": "TV Refined", "paper": True},
                {"num": "4", "tag": "alpaca4", "label": "Crew Paper", "paper": True}]
    monkeypatch.setattr(kairos, "ALPACA_ACCOUNTS", accounts)
    monkeypatch.setattr(kairos, "_alpaca_account_ctx",
                        lambda n: (object(), f"alpaca{n}",
                                   {"2": "TV Refined", "4": "Crew Paper"}[str(n)],
                                   lambda: list(BOOKS.get(str(n), []))))
    monkeypatch.setattr(kairos, "_pair_alpaca_fills_lifo",
                        lambda fills, **kw: {"closed_clean": list(fills)})
    monkeypatch.setattr(kairos, "_fills_error", lambda n: None)
    kairos.app.config["TESTING"] = True
    return kairos.app.test_client()


def _get(client, qs=""):
    return client.get("/api/ticker/breakdown" + qs).get_json() or {}


def test_it_spans_every_book_by_default(client):
    """A ticker's story is split across books; defaulting to one would hide half."""
    d = _get(client, "?ticker=AAPL")
    assert d["overall"]["trades"] == 4
    assert set(d["by_book"]) == {"TV Refined", "Crew Paper"}


def test_other_tickers_are_excluded(client):
    d = _get(client, "?ticker=AAPL")
    assert all(t["strategy"].startswith("AAPL") for t in d["trades"])


def test_it_splits_by_strategy_because_they_can_disagree(client):
    """AAPL's R3S3 makes money and its R4S4 loses — an aggregate hides both."""
    d = _get(client, "?ticker=AAPL")
    by = d["by_strategy"]
    assert by["AAPL_CAM_BREAKOUT_R3S3_V02_5MIN"]["pnl"] == 55.0
    assert by["AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"]["pnl"] == -50.0


def test_narrowing_to_one_strategy_narrows_everything(client):
    d = _get(client, "?ticker=AAPL&strategy=AAPL_CAM_BREAKOUT_R4S4_V02_5MIN")
    assert d["overall"]["trades"] == 1
    assert list(d["by_strategy"]) == ["AAPL_CAM_BREAKOUT_R4S4_V02_5MIN"]
    assert d["overall"]["pnl"] == -50.0


def test_narrowing_to_one_book_narrows_everything(client):
    d = _get(client, "?ticker=AAPL&account=4")
    assert d["overall"]["trades"] == 1 and list(d["by_book"]) == ["Crew Paper"]


def test_side_and_exit_reason_are_broken_out(client):
    d = _get(client, "?ticker=AAPL")
    assert d["by_side"]["long"]["trades"] == 3
    assert d["by_side"]["short"]["pnl"] == -50.0
    assert d["by_exit_reason"]["max_hold"]["trades"] == 1


def test_the_curve_is_cumulative_in_exit_order(client):
    d = _get(client, "?ticker=AAPL")
    times = [p["time"] for p in d["curve"]]
    assert times == sorted(times)
    run = 0.0
    for p in d["curve"]:
        run = round(run + p["pnl"], 2)
        assert p["value"] == run
    assert d["curve"][-1]["value"] == d["overall"]["pnl"]


def test_drawdown_is_peak_to_trough_not_the_worst_trade(client):
    """AAPL runs +60, +40, -10, +5. The worst single trade is -50, but the drawdown
    from the +60 peak is -70."""
    d = _get(client, "?ticker=AAPL")
    assert d["overall"]["max_drawdown"] == -70.0
    assert d["overall"]["worst"] == -50.0


def test_an_all_winning_group_reports_no_drawdown(client):
    d = _get(client, "?ticker=AAPL&account=4")
    assert d["overall"]["max_drawdown"] == 0.0
    assert d["overall"]["profit_factor"] is None, "no losses means PF is undefined, not 0"


def test_omitting_the_ticker_returns_the_pickable_list(client):
    """So the picker only offers symbols that actually traded in the range."""
    d = _get(client)
    assert d["tickers"] == ["AAPL", "MSFT"]
    assert d["overall"] is None


def test_an_unknown_ticker_reports_no_trades_rather_than_zeroes(client):
    """"Did not trade" and "traded to zero" are different answers."""
    d = _get(client, "?ticker=NOSUCH")
    assert d["overall"] is None and d["trades"] == []


def test_an_unreadable_book_is_named_not_silently_dropped(client, monkeypatch):
    """Otherwise an outage renders as a confident number for half the books."""
    monkeypatch.setattr(kairos, "_alpaca_account_ctx",
                        lambda n: (object(), f"alpaca{n}", f"Book {n}", lambda: []))
    monkeypatch.setattr(kairos, "_fills_error", lambda n: "connection reset")
    d = _get(client, "?ticker=AAPL")
    assert d["fills_unavailable"], "an outage produced an empty breakdown with no warning"


def test_an_unknown_account_is_rejected(client):
    r = client.get("/api/ticker/breakdown?ticker=AAPL&account=99")
    assert r.status_code == 400


def test_the_page_renders_and_lists_only_configured_books(client):
    """From _ui_accounts(), so it cannot drift. /strategy-explorer hardcodes its
    list and is already missing Crew Live."""
    html = client.get("/tickers").get_data(as_text=True)
    assert html.count('data-acct="2"') == 1
    assert html.count('data-acct="4"') == 1
    assert 'data-acct="6"' not in html, "listed a book that is not configured"
    assert "Crew Paper" in html and "TV Refined" in html


def test_the_page_offers_a_strategy_filter():
    html = open("templates/tickers.html", encoding="utf-8").read()
    assert 'id="stratSel"' in html
    assert "pickStrategy(" in html, "clicking a strategy row should narrow the page"


def test_thin_samples_are_not_coloured():
    """Colour is a claim. Same MIN_N rule as the Strategy Explorer."""
    html = open("templates/tickers.html", encoding="utf-8").read()
    assert "const MIN_N" in html
    assert "n < MIN_N" in html and "thin" in html


# ── Gate cost, scoped to one ticker ─────────────────────────────────────────────

@pytest.fixture
def blocks(tmp_path, monkeypatch):
    import datetime as _dt
    import shutil as _sh
    import sqlite3 as _sq
    db = tmp_path / "gc.db"
    _sh.copy("trades.db", db)
    ts = (_dt.datetime.now(_dt.timezone.utc)
          - _dt.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    c = _sq.connect(db)
    c.execute("DELETE FROM blocked_targets")
    for acct, tk, strat, side, gate in [
        ("alpaca2", "AAPL", "AAPL_CAM_BREAKOUT_R3S3_V02_5MIN", "long",  "strikes"),
        ("alpaca2", "AAPL", "AAPL_CAM_BREAKOUT_R4S4_V02_5MIN", "short", "daytype"),
        ("alpaca3", "AAPL", "AAPL_CAM_BREAKOUT_R3S3_V02_5MIN", "long",  "hours"),
        ("alpaca2", "MSFT", "MSFT_CAM_REVERSAL_R3S3_V02_5MIN", "long",  "strikes"),
    ]:
        c.execute("INSERT INTO blocked_targets (ts,account,ticker,strategy,side,gate,"
                  "reason) VALUES (?,?,?,?,?,?,?)", (ts, acct, tk, strat, side, gate, "t"))
    c.commit(); c.close()

    def _fake_db():
        x = _sq.connect(db); x.row_factory = _sq.Row
        return x

    monkeypatch.setattr(kairos, "get_db", _fake_db)
    monkeypatch.setattr(kairos, "_flush_blocked_targets", lambda: None)
    kairos.app.config["TESTING"] = True
    return kairos.app.test_client()


def _gc(client, qs=""):
    return client.get("/api/signals/gate_opportunity?days=7" + qs).get_json() or {}


def test_gate_cost_is_unfiltered_by_default(blocks):
    assert _gc(blocks)["total_blocks"] == 4


def test_gate_cost_can_be_scoped_to_one_ticker(blocks):
    """So the Ticker Breakdown page can price the gates for just that symbol."""
    d = _gc(blocks, "&ticker=AAPL")
    assert d["total_blocks"] == 3 and d["ticker"] == "AAPL"


def test_the_ticker_filter_is_case_insensitive(blocks):
    assert _gc(blocks, "&ticker=aapl")["total_blocks"] == 3


def test_a_ticker_with_no_blocks_reports_zero_not_everything(blocks):
    """A filter that silently falls back to unfiltered would read as "this ticker
    was blocked constantly"."""
    d = _gc(blocks, "&ticker=NOSUCH")
    assert d["total_blocks"] == 0 and not d["gates"]


def test_the_panel_asks_for_the_selected_ticker():
    html = open("templates/tickers.html", encoding="utf-8").read()
    i = html.index("async function loadGateCost")
    assert "gate_opportunity" in html[i:i + 900]
    assert "&ticker=' + encodeURIComponent(t)" in html[i:i + 900]


def test_a_gate_with_no_control_group_is_not_reported_as_free():
    """day-type runs on the farms too, so those blocks have no counterfactual.
    Rendering them as $0 would read as "the gate cost nothing". The wording lives
    on the server (see the verdict test below); what the page must guarantee is
    that no NUMBER is shown for an unanswerable gate."""
    import inspect
    assert "no control group" in inspect.getsource(kairos.api_gate_opportunity)
    html = open("templates/tickers.html", encoding="utf-8").read()
    i = html.index("async function loadGateCost")
    block = html[i:i + 4200]
    assert "r.unanswerable" in block
    # both the $ and % cells fall back to an em-dash rather than a figure
    assert block.count("noCtl ? '") >= 2, "the $ and % cells must both blank out"


# ── Trail sweep for the selected strategy ───────────────────────────────────────

def _sweep_src():
    html = open("templates/tickers.html", encoding="utf-8").read()
    i = html.index("async function runSweep")
    return html[i:html.index("function _renderCurve", i)]


def test_the_sweep_requires_one_strategy():
    """Sweeping a whole ticker would average across strategies that want different
    trails — R3S3 and R4S4 do not share an answer."""
    html = open("templates/tickers.html", encoding="utf-8").read()
    assert "_syncSweepAvailability" in html
    i = html.index("function _syncSweepAvailability")
    assert "btn.disabled = !strat" in html[i:i + 500]


def test_the_sweep_reuses_the_replay_engine():
    """/api/strategy/sweep replays the strategy's own Alpaca fills; a second
    implementation would drift from what Replay reports."""
    assert "/api/strategy/sweep" in _sweep_src()


def test_the_sweep_compares_against_the_currently_wired_trail():
    """A ranking without it names the best value but not whether it beats what is
    already running."""
    src = _sweep_src()
    assert "sr_trail" in src and "delta_vs_sr" in src
    assert "Currently wired" in src


def test_the_sweep_says_the_winner_is_in_sample():
    """It picks the best value on the very trades it scores. Without saying so, a
    curve fit reads as a finding."""
    src = _sweep_src()
    assert "not a forecast" in src
    assert "noise" in src


# ── Control group: each book vs the farm sharing its entry mechanism ────────────

def test_each_curated_book_is_paired_with_its_own_farm():
    """TV Refined enters on TV alerts and Kairos Refined via the engine. Pricing a
    Kairos block against the TV farm measures the MECHANISM difference, not the
    gate — and that difference ran -$728 over 30 days."""
    import inspect
    src = inspect.getsource(kairos.api_gate_opportunity)
    assert '"alpaca2": "alpaca"' in src
    assert '"alpaca3": "alpaca5"' in src


def test_the_crew_books_are_deliberately_unpaired():
    """Their picks carry per-pick entry tags, so neither farm is right for the whole
    book and any single choice would be wrong half the time."""
    import inspect
    src = inspect.getsource(kairos.api_gate_opportunity)
    i = src.index("_CONTROL_FARM_BY_BOOK = {")
    mapping = src[i:src.index("}", i)]
    assert "alpaca4" not in mapping and "alpaca6" not in mapping


def test_falling_back_to_the_other_farm_is_counted_not_hidden():
    """A cross-farm match is weaker evidence; it has to be visible as such."""
    import inspect
    src = inspect.getsource(kairos.api_gate_opportunity)
    assert "cross_farm" in src
    html = open("templates/tickers.html", encoding="utf-8").read()
    assert "priced against the other farm" in html


def test_the_panel_reads_the_by_gate_rows_not_the_name_list():
    """Top-level `gates` is a list of gate NAME strings for row order. Reading it as
    objects rendered blank rows of zeros."""
    html = open("templates/tickers.html", encoding="utf-8").read()
    i = html.index("async function loadGateCost")
    block = html[i:i + 3500]
    assert "b.by_gate" in block
    assert "d.gates" not in block, "the name-only list is not the data"


def test_the_panel_shows_which_farm_each_book_was_priced_against():
    html = open("templates/tickers.html", encoding="utf-8").read()
    assert "control_farm_label" in html


def test_the_verdict_comes_from_the_server_not_recomputed():
    """The sign convention is the non-obvious one — a farm WIN on a blocked setup
    means the gate COST you. Recomputing it client-side inverted it once already."""
    html = open("templates/tickers.html", encoding="utf-8").read()
    i = html.index("async function loadGateCost")
    block = html[i:i + 3500]
    assert "r.verdict" in block
    assert "'saved money'" not in block and "'cost money'" not in block


def test_the_subtitle_states_the_sign_convention_correctly():
    html = open("templates/tickers.html", encoding="utf-8").read()
    i = html.index("What did the gates cost on this ticker?")
    block = html[i:i + 600]
    assert "the gate cost you" in block
    assert "A positive number means the gate SAVED money" not in block


# ── Book-first control layout ───────────────────────────────────────────────────

def _tk_html():
    return open("templates/tickers.html", encoding="utf-8").read()


def test_books_are_buttons_not_a_dropdown(client):
    html = client.get("/tickers").get_data(as_text=True)
    assert 'class="btn acct' in html
    assert 'id="acctSel"' not in html, "the dropdown should be gone"
    assert "setAccount('2')" in html and "setAccount('4')" in html


def test_all_books_is_the_default_and_is_offered(client):
    html = client.get("/tickers").get_data(as_text=True)
    assert 'data-acct="" onclick="setAccount(\'\')"' in html
    assert "let _acct = ''" in html


def test_the_book_row_comes_before_the_ticker_picker(client):
    """The book scopes everything below it, so the page should read top-down."""
    html = client.get("/tickers").get_data(as_text=True)
    assert html.index('class="btn acct') < html.index('id="tickerSel"')


def test_a_live_book_is_marked_as_real_money(client, monkeypatch):
    monkeypatch.setattr(kairos, "_ui_accounts", lambda: [
        {"num": "6", "tag": "alpaca6", "label": "Crew Live", "color": "#E8A0BF",
         "paper": False, "curated": True}])
    html = client.get("/tickers").get_data(as_text=True)
    assert 'title="real money"' in html


def test_switching_books_rebuilds_the_ticker_list_and_totals():
    """Ranking one book's tickers by another book's P&L is exactly the mistake this
    page exists to avoid, so the picker is rebuilt rather than refiltered."""
    html = _tk_html()
    i = html.index("async function setAccount")
    assert "refreshTickerList()" in html[i:i + 800]
    j = html.index("async function refreshTickerList")
    block = html[j:j + 1200]
    assert "'&account=' + encodeURIComponent(_acct)" in block
    assert "_tickerTotals = d.ticker_totals" in block


def test_the_selected_symbol_survives_a_book_switch_when_it_traded_there():
    """So two books can be compared on one name without re-finding it each time."""
    html = _tk_html()
    i = html.index("async function refreshTickerList")
    assert "list.includes(keep)" in html[i:i + 1200]


def test_the_strategy_filter_resets_on_a_book_switch():
    """Strategies are per book AND per ticker; a stale one would silently filter to
    nothing."""
    html = _tk_html()
    i = html.index("async function refreshTickerList")
    assert "stratSel').value = ''" in html[i:i + 1200]


def test_changing_the_range_also_rebuilds_the_picker():
    """Totals are range-dependent, so the sort order is too."""
    html = _tk_html()
    i = html.index("function setDays")
    assert "refreshTickerList()" in html[i:i + 400]


def test_the_book_choice_persists_across_a_reload():
    html = _tk_html()
    assert "localStorage.setItem('ticker_acct'" in html
    assert "localStorage.getItem('ticker_acct')" in html


def test_a_book_with_no_trades_clears_the_gate_and_sweep_panels():
    """Otherwise an empty book shows the previous book's numbers under its name."""
    html = _tk_html()
    i = html.index("if (!o) {")
    block = html[i:i + 600]
    assert "'gateCost'" in block and "'sweepOut'" in block


def test_the_sweep_falls_back_to_one_book_on_all_books():
    """/api/strategy/sweep replays a single account's fills; "all" is not a book."""
    html = _tk_html()
    i = html.index("async function runSweep")
    assert "_acct || '2'" in html[i:i + 1500]


# ── Gate drill-down ─────────────────────────────────────────────────────────────

def test_every_block_produces_a_sample_not_just_the_matched_ones(blocks, monkeypatch):
    """A drill-down showing 15 rows under a "22 blocks" heading looks like a
    miscount, and hides exactly the cases worth understanding."""
    monkeypatch.setattr(kairos, "_gates_without_control", lambda: {"daytype"})
    d = _gc(blocks, "&ticker=AAPL")
    assert len(d["samples"]) == d["total_blocks"] == 3


def test_a_sample_says_why_it_could_not_be_priced(blocks, monkeypatch):
    monkeypatch.setattr(kairos, "_gates_without_control", lambda: {"daytype"})
    d = _gc(blocks, "&ticker=AAPL")
    by = {s["gate"]: s["status"] for s in d["samples"]}
    assert by["daytype"] == "unanswerable"
    assert by["strikes"] in ("no_farm_match", "matched")


def test_samples_carry_what_the_modal_needs(blocks):
    d = _gc(blocks, "&ticker=AAPL")
    s = d["samples"][0]
    for k in ("ts", "account", "gate", "ticker", "side", "strategy", "status"):
        assert k in s, k


def test_truncation_is_reported_rather_than_silent(blocks):
    """A silently short list reads as "that is all of them"."""
    d = _gc(blocks, "&ticker=AAPL")
    assert "samples_capped" in d and d["samples_capped"] is False


def test_the_sample_cap_is_larger_when_scoped_to_one_ticker():
    """Per-ticker is where the individual trades are the point."""
    import inspect
    src = inspect.getsource(kairos.api_gate_opportunity)
    assert "_sample_cap = 400 if only_ticker else 60" in src


def test_clicking_a_gate_row_opens_the_detail():
    html = _tk_html()
    assert "openGateDetail(" in html and 'id="gateModal"' in html
    i = html.index("const _open =")
    assert "b.account" in html[i:i + 300], "detail must be scoped to the book AND gate"


def test_the_modal_filters_to_one_book_and_gate():
    """The samples array spans every book; showing all of them under one book's
    row would overstate that gate."""
    html = _tk_html()
    i = html.index("function openGateDetail")
    block = html[i:i + 600]
    assert "s.account === account" in block and "s.gate === gate" in block


def test_the_modal_keeps_the_pages_sign_convention():
    """A farm WIN on a blocked setup is money the gate cost you — the same
    convention as the summary table, or the two would contradict each other."""
    html = _tk_html()
    i = html.index("function openGateDetail")
    block = html[i:i + 3000]
    assert "r.pct > 0 ? 'neg' : 'pos'" in block


def test_the_modal_can_be_dismissed_three_ways():
    html = _tk_html()
    assert "closeGateDetail()" in html
    assert "event.target === this" in html, "clicking the backdrop should close it"
    assert "e.key === 'Escape'" in html


# ── Modal timestamps ────────────────────────────────────────────────────────────

def _et_samples(client, rows):
    """Seed blocked_targets with raw UTC timestamps and read back what the API shows."""
    import shutil as _sh, sqlite3 as _sq, tempfile as _tf, os as _os
    d = _tf.mkdtemp(); db = _os.path.join(d, "ts.db")
    _sh.copy("trades.db", db)
    c = _sq.connect(db); c.execute("DELETE FROM blocked_targets")
    for ts in rows:
        c.execute("INSERT INTO blocked_targets (ts,account,ticker,strategy,side,gate,"
                  "reason) VALUES (?,?,?,?,?,?,?)",
                  (ts, "alpaca2", "AAPL", "AAPL_CAM_BREAKOUT_R3S3_V02_5MIN",
                   "long", "hours", "t"))
    c.commit(); c.close()
    def _fake():
        x = _sq.connect(db); x.row_factory = _sq.Row
        return x
    kairos.get_db = _fake
    kairos._flush_blocked_targets = lambda: None
    kairos.app.config["TESTING"] = True
    r = kairos.app.test_client().get(
        "/api/signals/gate_opportunity?days=2&ticker=AAPL&accounts=alpaca2").get_json()
    return [s["ts"] for s in (r.get("samples") or [])]


def test_modal_timestamps_are_market_time_not_utc(monkeypatch):
    """Everything else this app shows a trader is ET — the gates, the windows and
    the day boundaries are all defined in market time."""
    import datetime as _d
    day = _d.datetime.now(_d.timezone.utc).strftime("%Y-%m-%d")
    got = _et_samples(None, [f"{day} 14:25:00"])
    assert got == [f"{day} 10:25"], got     # 14:25 UTC = 10:25 EDT


def test_a_utc_timestamp_can_roll_back_a_day_in_et(monkeypatch):
    """03:10 UTC is the PREVIOUS evening in ET. Showing the UTC date would file the
    block under a trading day it did not belong to."""
    import datetime as _d
    now = _d.datetime.now(_d.timezone.utc)
    day = now.strftime("%Y-%m-%d")
    prev = (now - _d.timedelta(days=1)).strftime("%Y-%m-%d")
    got = _et_samples(None, [f"{day} 03:10:00"])
    assert got == [f"{prev} 23:10"], got


def test_conversion_happens_on_the_server_not_the_browser():
    """toLocaleString would use the VIEWER's zone, which is not market time."""
    import inspect
    src = inspect.getsource(kairos.api_gate_opportunity)
    assert "_ts_et" in src and 'ZoneInfo("America/New_York")' in src


def test_an_unreadable_timestamp_is_shown_raw_not_blanked():
    """A blank cell in the modal reads as missing data."""
    import inspect
    src = inspect.getsource(kairos.api_gate_opportunity)
    i = src.index("def _ts_et")
    assert 'return (raw or "")[:16]' in src[i:i + 500]


def test_the_column_is_labelled_et():
    html = _tk_html()
    assert "When (ET)" in html and "When (UTC)" not in html
