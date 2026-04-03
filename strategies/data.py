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


def fetch_bars(ticker, start, end, interval="1h"):
    try:
        import yfinance as yf
    except ImportError:
        raise RuntimeError("yfinance is not installed")

    log.info(f"yfinance download: {ticker} {interval} {start}→{end}")

    try:
        tkr_obj = yf.Ticker(ticker)
        df = tkr_obj.history(
            start=start,
            end=end,
            interval=interval,
            auto_adjust=True,
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

        bars.append({"time": dt, "open": o, "high": h, "low": l, "close": c})

    log.info(f"{ticker}: parsed {len(bars)} bars")
    bars.sort(key=lambda b: b["time"])
    return bars


def fetch_bars_ib(ib_broker, ticker, start, end, interval="1h"):
    """
    Fetch historical bars via Interactive Brokers.
    ib_broker: connected IBBroker instance (from app.ib_broker)
    """
    if ib_broker is None:
        raise RuntimeError("IB broker is not initialised (IB_HOST env var not set)")
    if not ib_broker.is_connected():
        raise RuntimeError("IB is not connected")

    log.info(f"IB fetch: {ticker} {interval} {start}→{end}")
    bars = ib_broker.fetch_historical_bars(ticker, start, end, interval)
    log.info(f"IB {ticker}: {len(bars)} bars returned")

    if not bars:
        raise ValueError(f"No bars returned from IB for {ticker} ({start}→{end}, {interval})")
    return bars
