"""
Market data fetchers.
Returns bars in the same format as parse_bars() — list of dicts with
time (datetime), open, high, low, close.

Sources:
  fetch_bars()    — Yahoo Finance (yfinance), free, limited history
  fetch_bars_ib() — Interactive Brokers, requires connected IBBroker, 10+ years
"""

from datetime import datetime
import logging

log = logging.getLogger(__name__)


# Yahoo Finance maximum lookback per interval
_YF_MAX_DAYS = {
    "1d":  365 * 20,   # daily: 20 years available
    "1wk": 365 * 20,
    "1mo": 365 * 20,
    "1h":  729,         # hourly: ~2 years
    "30m": 59,
    "15m": 59,
    "5m":  59,
    "2m":  59,
}


def fetch_bars(ticker, start, end, interval="1d"):
    try:
        import yfinance as yf
    except ImportError:
        raise RuntimeError("yfinance is not installed")

    from datetime import datetime, timedelta

    # Enforce Yahoo Finance's per-interval lookback limits
    max_days = _YF_MAX_DAYS.get(interval)
    if max_days:
        earliest = (datetime.utcnow() - timedelta(days=max_days)).strftime("%Y-%m-%d")
        if start < earliest:
            log.warning(
                "fetch_bars: %s %s requested start %s, but Yahoo Finance only "
                "keeps %d days for this interval — capping to %s",
                ticker, interval, start, max_days, earliest,
            )
            start = earliest

    log.info(f"yfinance download: {ticker} {interval} {start}→{end}")

    try:
        df = yf.download(
            ticker,
            start=start,
            end=end,
            interval=interval,
            auto_adjust=True,
            progress=False,
        )
    except Exception as e:
        raise ValueError(f"yfinance download error for {ticker}: {e}")

    log.info(f"{ticker}: df shape={getattr(df, 'shape', '?')} columns={list(getattr(df, 'columns', []))}")

    if df is None or df.empty:
        raise ValueError(f"No data returned for {ticker} ({start} → {end}, {interval})")

    # Flatten MultiIndex columns if present
    if hasattr(df.columns, "levels"):
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

    # Normalise column names to title case
    df.columns = [str(c).strip().title() for c in df.columns]
    log.info(f"{ticker}: normalised columns={list(df.columns)}")

    bars = []
    for ts, row in df.iterrows():
        try:
            dt = ts.to_pydatetime().replace(tzinfo=None)
        except Exception:
            try:
                dt = datetime.utcfromtimestamp(float(ts) / 1e9)
            except Exception:
                continue

        try:
            o = float(row.get("Open",  0) or 0)
            h = float(row.get("High",  0) or 0)
            l = float(row.get("Low",   0) or 0)
            c = float(row.get("Close", 0) or 0)
        except (TypeError, ValueError):
            continue

        if not (o and c):
            continue

        try:
            v = float(row.get("Volume", 0) or 0)
        except (TypeError, ValueError):
            v = 0.0
        bars.append({"time": dt, "open": o, "high": h, "low": l, "close": c, "volume": v})

    log.info(f"{ticker}: parsed {len(bars)} bars")
    bars.sort(key=lambda b: b["time"])
    return bars


def session_hourly(df):
    """Regular-session hourly bars from 5-minute bars, aligned to the 9:30 open:
    9:30-10:30, 10:30-11:30, ... 14:30-15:30, then a half bar 15:30-16:00.

    Alpaca's native 1-hour bars are clock-aligned (9:00, 10:00, ...). The 9:00 bar
    mixes pre-market with the opening half hour, so an RTH filter drops it and the
    open, where most Camarilla breakouts fire, vanishes from the backtest; every
    later bar also straddles TradingView's 9:30-aligned session bars. Building from
    5-minute RTH bars gives the same bars TradingView draws.

    df: tz-aware (UTC) DatetimeIndex with open/high/low/close/volume columns.
    Returns the same shape, UTC index, labelled by each bar's START."""
    import pandas as pd
    et = df.tz_convert("America/New_York") if df.index.tz is not None \
        else df.tz_localize("UTC").tz_convert("America/New_York")
    mins = et.index.hour * 60 + et.index.minute
    et = et[(mins >= 9 * 60 + 30) & (mins < 16 * 60)]
    if et.empty:
        return df.iloc[0:0]
    agg = {"open": "first", "high": "max", "low": "min", "close": "last"}
    if "volume" in et.columns:
        agg["volume"] = "sum"
    out = (et[list(agg)].resample("60min", origin="start_day", offset="30min",
                                  label="left", closed="left")
             .agg(agg).dropna(subset=["open"]))
    return out.tz_convert("UTC")


def fetch_bars_alpaca(ticker, start, end, interval="1d"):
    """
    Fetch OHLCV bars from Alpaca Data API.
    interval: '1d', '1h', '30m', '15m', '5m'
    '1h' is built from 5-minute bars aligned to the 9:30 open (see session_hourly).
    Requires ALPACA_KEY + ALPACA_SECRET env vars.
    """
    import os
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    from datetime import datetime as _dt

    _TF = {
        "1d":  TimeFrame.Day,
        "1h":  TimeFrame.Hour,
        "30m": TimeFrame(30, TimeFrameUnit.Minute),
        "15m": TimeFrame(15, TimeFrameUnit.Minute),
        "5m":  TimeFrame(5,  TimeFrameUnit.Minute),
    }
    hourly = interval == "1h"
    tf = _TF["5m"] if hourly else _TF.get(interval, TimeFrame.Day)

    key    = os.environ.get("ALPACA_KEY",    "")
    secret = os.environ.get("ALPACA_SECRET", "")
    client = StockHistoricalDataClient(api_key=key or None, secret_key=secret or None)

    # Honour ALPACA_DATA_FEED. Omitting it silently served IEX (~a few % of volume)
    # even on an Algo Trader Plus subscription, which skews volume and can miss
    # prints entirely on thin names. Mirrors app._alpaca_data_feed(); duplicated
    # rather than imported because app imports this module.
    from alpaca.data.enums import DataFeed
    _feed = (DataFeed.SIP
             if (os.environ.get("ALPACA_DATA_FEED", "iex") or "iex").strip().lower() == "sip"
             else DataFeed.IEX)

    req = StockBarsRequest(
        symbol_or_symbols=ticker,
        timeframe=tf,
        start=_dt.fromisoformat(start),
        end=_dt.fromisoformat(end),
        adjustment="all",
        feed=_feed,
    )
    df = client.get_stock_bars(req).df

    if df is None or df.empty:
        raise ValueError(f"No Alpaca data returned for {ticker} ({start}→{end}, {interval})")

    # Drop symbol level from MultiIndex if present
    if hasattr(df.index, "levels"):
        df = df.xs(ticker, level="symbol")
    if hourly:
        df = session_hourly(df)

    bars = []
    for ts, row in df.iterrows():
        dt = ts.to_pydatetime().replace(tzinfo=None) if hasattr(ts, "to_pydatetime") else ts
        o = float(row.get("open",  0) or 0)
        c = float(row.get("close", 0) or 0)
        if not (o and c):
            continue
        bars.append({
            "time":   dt,
            "open":   o,
            "high":   float(row.get("high",   0) or 0),
            "low":    float(row.get("low",    0) or 0),
            "close":  c,
            "volume": float(row.get("volume", 0) or 0),
        })

    log.info(f"Alpaca {ticker}: {len(bars)} bars ({interval})")
    bars.sort(key=lambda b: b["time"])
    return bars


def fetch_bars_ib(ib_broker, ticker, start, end, interval="1h", on_chunk=None):
    """
    Fetch historical bars via Interactive Brokers.
    ib_broker: IBBroker instance (from app.ib_broker). Will reconnect if needed.
    on_chunk: optional callback(chunk_start, chunk_end, n_bars) called after each chunk.
    """
    if ib_broker is None:
        raise RuntimeError("IB broker is not initialised — set IB_HOST env var")

    log.info(f"IB fetch: {ticker} {interval} {start}→{end}")
    bars = ib_broker.fetch_historical_bars(ticker, start, end, interval, on_chunk=on_chunk)
    log.info(f"IB {ticker}: {len(bars)} bars returned")

    if not bars:
        raise ValueError(f"No bars returned from IB for {ticker} ({start}→{end}, {interval})")
    return bars
