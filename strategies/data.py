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
        multi_level_index=False,
    )

    if df is None or df.empty:
        raise ValueError(f"No data returned for {ticker} ({start} → {end}, {interval})")

    bars = []
    for ts, row in df.iterrows():
        try:
            dt = ts.to_pydatetime().replace(tzinfo=None)
        except Exception:
            dt = datetime.utcfromtimestamp(float(ts.value) / 1e9)

        o = float(row.get("Open",  row.get("open",  0)) or 0)
        h = float(row.get("High",  row.get("high",  0)) or 0)
        l = float(row.get("Low",   row.get("low",   0)) or 0)
        c = float(row.get("Close", row.get("close", 0)) or 0)

        if o == 0 or c == 0:
            continue

        bars.append({"time": dt, "open": o, "high": h, "low": l, "close": c})

    bars.sort(key=lambda b: b["time"])
    return bars
