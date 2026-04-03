"""
Market data fetcher using yfinance.
Returns bars in the same format as parse_bars() — list of dicts with
time (datetime), open, high, low, close.

Supported intervals:
  "1h"  — up to ~730 days history
  "5m"  — up to ~60 days history (recent only)
"""

from datetime import datetime


def fetch_bars(ticker, start, end, interval="1h"):
    """
    Fetch OHLCV bars from Yahoo Finance.
    Returns list of bar dicts or raises ValueError on failure.
    Compatible with yfinance >= 0.2.x (handles multi-level column index).
    """
    try:
        import yfinance as yf
    except ImportError:
        raise RuntimeError("yfinance is not installed")

    df = yf.download(
        ticker,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=True,
        progress=False,
    )

    if df is None or df.empty:
        raise ValueError(f"No data returned for {ticker} ({start} → {end}, {interval})")

    # yfinance >= 0.2.38 returns a MultiIndex column when downloading a single
    # ticker too — flatten it so we always get plain column names.
    if hasattr(df.columns, "levels"):
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

    bars = []
    for ts, row in df.iterrows():
        try:
            dt = ts.to_pydatetime().replace(tzinfo=None)
        except Exception:
            dt = datetime.utcfromtimestamp(float(ts.value) / 1e9)

        try:
            o = float(row["Open"])
            h = float(row["High"])
            l = float(row["Low"])
            c = float(row["Close"])
        except (KeyError, TypeError, ValueError):
            continue

        if not (o and h and l and c):
            continue

        bars.append({"time": dt, "open": o, "high": h, "low": l, "close": c})

    bars.sort(key=lambda b: b["time"])
    return bars
