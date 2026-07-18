"""Env-gated Alpaca market-data feed (_alpaca_data_feed).

All historical bar fetches route through this helper so the IEX→SIP switch is one
config flip (ALPACA_DATA_FEED). Default is IEX (free tier, safe); 'sip' selects the
consolidated feed (Algo Trader Plus). Anything else falls back to IEX.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a

pytest.importorskip("alpaca")   # helper resolves the DataFeed enum from the SDK
from alpaca.data.enums import DataFeed


def test_default_is_iex(monkeypatch):
    monkeypatch.delenv("ALPACA_DATA_FEED", raising=False)
    assert a._alpaca_data_feed() == DataFeed.IEX


@pytest.mark.parametrize("val", ["sip", "SIP", " Sip "])
def test_sip_selects_consolidated(monkeypatch, val):
    monkeypatch.setenv("ALPACA_DATA_FEED", val)
    assert a._alpaca_data_feed() == DataFeed.SIP


@pytest.mark.parametrize("val", ["iex", "garbage", ""])
def test_non_sip_falls_back_to_iex(monkeypatch, val):
    monkeypatch.setenv("ALPACA_DATA_FEED", val)
    assert a._alpaca_data_feed() == DataFeed.IEX
