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
    """A hardcoded account list drifts the moment a book is added or removed."""
    html = client.get("/tickers").get_data(as_text=True)
    assert html.count("<option value=\"2\">") == 1
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
