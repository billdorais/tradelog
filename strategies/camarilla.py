"""
Camarilla pivot strategy backtester.
No external dependencies — pure Python only.
"""

import csv
import io
import itertools
from datetime import datetime, date

# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------

def is_trade_list_csv(file_bytes):
    """
    Return True if the CSV looks like a TradingView Strategy Tester trade list
    (has 'Trade #' or 'Net P&L' in the header row).
    """
    if isinstance(file_bytes, bytes):
        header_line = file_bytes.decode("utf-8", errors="replace").split("\n")[0]
    else:
        header_line = file_bytes.split("\n")[0]
    return "Trade #" in header_line or "Net P&L" in header_line


def parse_trade_list(file_bytes):
    """
    Parse a TradingView Strategy Tester trade list CSV.

    Expected columns:
      Trade #, Type, Date and time, Signal, Price USD,
      Net P&L USD, Cumulative P&L USD

    Returns list of trade dicts compatible with _compute_stats().
    """
    if isinstance(file_bytes, bytes):
        text = file_bytes.decode("utf-8", errors="replace")
    else:
        text = file_bytes

    reader = csv.DictReader(io.StringIO(text))

    entries = {}  # trade_num -> entry info
    trades  = []

    for row in reader:
        trade_num = row.get("Trade #", "").strip()
        row_type  = (row.get("Type", "") or "").strip().lower()
        dt_str    = (row.get("Date and time", "") or "").strip()
        price_str = (row.get("Price USD", "") or "").strip()
        pnl_str   = (row.get("Net P&L USD", "") or "").strip()

        if not row_type or not dt_str:
            continue

        dt = _parse_time(dt_str)
        if dt is None:
            continue

        try:
            price = float(price_str.replace(",", "")) if price_str else 0.0
        except ValueError:
            price = 0.0

        if "entry" in row_type:
            direction = "LONG" if "long" in row_type else "SHORT"
            entries[trade_num] = {
                "direction":   direction,
                "entry_time":  dt.strftime("%Y-%m-%d %H:%M"),
                "entry_price": price,
            }

        elif "exit" in row_type:
            try:
                pnl = float(pnl_str.replace(",", "")) if pnl_str else 0.0
            except ValueError:
                pnl = 0.0

            direction   = "LONG" if "long" in row_type else "SHORT"
            entry_info  = entries.pop(trade_num, {})

            trades.append({
                "direction":   direction,
                "entry_price": entry_info.get("entry_price", 0.0),
                "exit_price":  price,
                "entry_time":  entry_info.get("entry_time", dt.strftime("%Y-%m-%d %H:%M")),
                "exit_time":   dt.strftime("%Y-%m-%d %H:%M"),
                "pnl":         round(pnl, 4),
                "exit_reason": "tv_export",
                "exit_bar":    0,
            })

    return trades


def parse_bars(file_bytes):
    """
    Parse a TradingView OHLCV CSV export.
    Accepts bytes or string. Returns list of bar dicts.
    Supports both Unix timestamps and ISO datetime strings.
    """
    if isinstance(file_bytes, bytes):
        text = file_bytes.decode("utf-8", errors="replace")
    else:
        text = file_bytes

    reader = csv.DictReader(io.StringIO(text))
    bars = []
    for row in reader:
        # Normalise column names (TradingView may use 'time' or 'Date')
        time_val = (row.get("time") or row.get("Time") or row.get("date") or
                    row.get("Date") or "").strip()
        try:
            o = float(row.get("open") or row.get("Open") or 0)
            h = float(row.get("high") or row.get("High") or 0)
            l = float(row.get("low")  or row.get("Low")  or 0)
            c = float(row.get("close") or row.get("Close") or 0)
        except (ValueError, TypeError):
            continue
        if not time_val or o == 0:
            continue
        dt = _parse_time(time_val)
        if dt is None:
            continue
        bars.append({"time": dt, "open": o, "high": h, "low": l, "close": c})

    bars.sort(key=lambda b: b["time"])
    return bars


def _parse_time(s):
    """Try several common TradingView time formats. Return datetime or None."""
    # Unix timestamp (integer seconds)
    try:
        ts = int(s)
        return datetime.utcfromtimestamp(ts)
    except (ValueError, TypeError):
        pass
    # ISO / space-separated
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Indicators
# ---------------------------------------------------------------------------

def _compute_ema(closes, period):
    """Returns list of EMA values aligned with closes list."""
    k = 2.0 / (period + 1)
    ema = closes[0]
    result = [ema]
    for p in closes[1:]:
        ema = p * k + ema * (1 - k)
        result.append(ema)
    return result


def _compute_daily_pivots(bars):
    """
    Build Camarilla H4/L4 levels for each date using the previous trading day's
    High, Low, Close.

    Returns dict: date → {"h4": float, "l4": float}
    """
    daily = {}  # date → {"high", "low", "close"}
    for bar in bars:
        d = bar["time"].date()
        if d not in daily:
            daily[d] = {"high": bar["high"], "low": bar["low"], "close": bar["close"]}
        else:
            daily[d]["high"]  = max(daily[d]["high"], bar["high"])
            daily[d]["low"]   = min(daily[d]["low"],  bar["low"])
            daily[d]["close"] = bar["close"]  # last close of the day

    dates   = sorted(daily)
    pivots  = {}
    for i in range(1, len(dates)):
        prev = dates[i - 1]
        curr = dates[i]
        h, l, c = daily[prev]["high"], daily[prev]["low"], daily[prev]["close"]
        rng = h - l
        pivots[curr] = {
            "h4": c + 1.1 * rng / 2,
            "l4": c - 1.1 * rng / 2,
        }
    return pivots


# ---------------------------------------------------------------------------
# Trade simulation helpers
# ---------------------------------------------------------------------------

def _simulate_trade(bars, entry_idx, direction, entry_price,
                    trail_activation, trail_dist, hard_stop):
    """
    Simulate one trade from entry_idx onward.
    Returns a dict with trade details and the index of the exit bar.

    direction: "LONG" or "SHORT"
    """
    best_price = entry_price   # highest high for LONG, lowest low for SHORT
    trailing_active = False
    entry_time = bars[entry_idx]["time"]

    for j in range(entry_idx, len(bars)):
        bar = bars[j]

        if direction == "LONG":
            best_price = max(best_price, bar["high"])
            profit = best_price - entry_price

            if not trailing_active and profit >= trail_activation:
                trailing_active = True

            hard_stop_level = entry_price - hard_stop

            # Hard stop check (worst-case fill at stop level)
            if bar["low"] <= hard_stop_level:
                return _make_trade(direction, entry_price, hard_stop_level,
                                   entry_time, bar["time"], j, "hard_stop")

            # Trailing stop check
            if trailing_active:
                trail_level = best_price - trail_dist
                if bar["low"] <= trail_level:
                    return _make_trade(direction, entry_price, trail_level,
                                       entry_time, bar["time"], j, "trail_stop")

        else:  # SHORT
            best_price = min(best_price, bar["low"])
            profit = entry_price - best_price

            if not trailing_active and profit >= trail_activation:
                trailing_active = True

            hard_stop_level = entry_price + hard_stop

            if bar["high"] >= hard_stop_level:
                return _make_trade(direction, entry_price, hard_stop_level,
                                   entry_time, bar["time"], j, "hard_stop")

            if trailing_active:
                trail_level = best_price + trail_dist
                if bar["high"] >= trail_level:
                    return _make_trade(direction, entry_price, trail_level,
                                       entry_time, bar["time"], j, "trail_stop")

    # End of data — close at last bar
    exit_price = bars[-1]["close"]
    return _make_trade(direction, entry_price, exit_price,
                       entry_time, bars[-1]["time"], len(bars) - 1, "eod")


def _make_trade(direction, entry, exit_price, entry_time, exit_time, exit_bar, reason):
    pnl = (exit_price - entry) if direction == "LONG" else (entry - exit_price)
    return {
        "direction":   direction,
        "entry_price": round(entry, 4),
        "exit_price":  round(exit_price, 4),
        "entry_time":  entry_time.strftime("%Y-%m-%d %H:%M"),
        "exit_time":   exit_time.strftime("%Y-%m-%d %H:%M"),
        "pnl":         round(pnl, 4),
        "exit_reason": reason,
        "exit_bar":    exit_bar,
    }


# ---------------------------------------------------------------------------
# Core backtest
# ---------------------------------------------------------------------------

def run_backtest(bars, params):
    """
    Run a single Camarilla strategy backtest.

    params keys:
      long_trail_activation  — profit pts before long trailing stop activates
      short_trail_activation — profit pts before short trailing stop activates
      long_hard_stop         — pts hard stop loss for longs
      short_hard_stop        — pts hard stop loss for shorts
      trail_distance         — how many pts the trailing stop trails behind best price
      eod_close              — bool: close any open trade at last bar of the day (default True)

    Returns dict with trades list and stats.
    """
    lta  = params.get("long_trail_activation",  40)
    sta  = params.get("short_trail_activation", 10)
    lhs  = params.get("long_hard_stop",         70)
    shs  = params.get("short_hard_stop",        20)
    trd  = params.get("trail_distance",          1)

    pivots = _compute_daily_pivots(bars)
    closes = [b["close"] for b in bars]
    emas   = _compute_ema(closes, 8)

    trades = []
    i = 0
    while i < len(bars) - 1:
        bar   = bars[i]
        ema8  = emas[i]
        d     = bar["time"].date()

        if d not in pivots:
            i += 1
            continue

        h4 = pivots[d]["h4"]
        l4 = pivots[d]["l4"]

        # Long entry: breakout above H4, price above EMA8
        if bar["close"] > h4 and bar["open"] < h4 and ema8 < bar["close"]:
            entry_price = bars[i + 1]["open"]
            trade = _simulate_trade(bars, i + 1, "LONG", entry_price,
                                    lta, trd, lhs)
            trades.append(trade)
            i = trade["exit_bar"] + 1
            continue

        # Short entry: breakdown below L4, price below EMA8
        if bar["close"] < l4 and bar["open"] > l4 and ema8 > bar["close"]:
            entry_price = bars[i + 1]["open"]
            trade = _simulate_trade(bars, i + 1, "SHORT", entry_price,
                                    sta, trd, shs)
            trades.append(trade)
            i = trade["exit_bar"] + 1
            continue

        i += 1

    return {"trades": trades, "stats": _compute_stats(trades)}


def _compute_stats(trades):
    if not trades:
        return {
            "total_trades": 0, "wins": 0, "losses": 0,
            "win_rate": 0, "total_pnl": 0,
            "avg_win": 0, "avg_loss": 0,
            "profit_factor": None, "max_drawdown": 0,
            "equity_curve": [],
        }

    wins   = [t["pnl"] for t in trades if t["pnl"] > 0]
    losses = [t["pnl"] for t in trades if t["pnl"] <= 0]

    win_rate      = round(len(wins) / len(trades) * 100, 1)
    avg_win       = round(sum(wins)   / len(wins),   2) if wins   else 0
    avg_loss      = round(sum(losses) / len(losses), 2) if losses else 0
    gross_loss    = abs(sum(losses))
    profit_factor = round(sum(wins) / gross_loss, 2) if gross_loss > 0 else None
    total_pnl     = round(sum(t["pnl"] for t in trades), 2)

    cumulative = peak = max_dd = 0
    equity_curve = []
    for t in trades:
        cumulative += t["pnl"]
        equity_curve.append({"time": t["exit_time"], "value": round(cumulative, 2)})
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    return {
        "total_trades": len(trades),
        "wins":         len(wins),
        "losses":       len(losses),
        "win_rate":     win_rate,
        "total_pnl":    total_pnl,
        "avg_win":      avg_win,
        "avg_loss":     avg_loss,
        "profit_factor": profit_factor,
        "max_drawdown": round(max_dd, 2),
        "equity_curve": equity_curve,
    }


# ---------------------------------------------------------------------------
# Parameter optimisation
# ---------------------------------------------------------------------------

def optimise(bars, opt_params):
    """
    Grid-search over parameter ranges.

    opt_params:
      long_trail_activation_range:  [min, max, step]
      short_trail_activation_range: [min, max, step]
      long_hard_stop_range:         [min, max, step]
      short_hard_stop_range:        [min, max, step]
      trail_distance:               single value (default 1)

    Returns list of result dicts sorted by profit_factor desc.
    """
    def _range(spec):
        lo, hi, step = spec
        vals = []
        v = lo
        while v <= hi + 1e-9:
            vals.append(round(v, 4))
            v += step
        return vals

    lta_vals = _range(opt_params.get("long_trail_activation_range",  [20, 60, 10]))
    sta_vals = _range(opt_params.get("short_trail_activation_range", [5,  20,  5]))
    lhs_vals = _range(opt_params.get("long_hard_stop_range",         [50, 100, 10]))
    shs_vals = _range(opt_params.get("short_hard_stop_range",        [10,  30,  5]))
    trd      = opt_params.get("trail_distance", 1)

    results = []
    for lta, sta, lhs, shs in itertools.product(lta_vals, sta_vals, lhs_vals, shs_vals):
        params = {
            "long_trail_activation":  lta,
            "short_trail_activation": sta,
            "long_hard_stop":         lhs,
            "short_hard_stop":        shs,
            "trail_distance":         trd,
        }
        bt = run_backtest(bars, params)
        s  = bt["stats"]
        results.append({
            "long_trail_activation":  lta,
            "short_trail_activation": sta,
            "long_hard_stop":         lhs,
            "short_hard_stop":        shs,
            "trail_distance":         trd,
            "total_trades":           s["total_trades"],
            "win_rate":               s["win_rate"],
            "profit_factor":          s["profit_factor"],
            "total_pnl":              s["total_pnl"],
            "max_drawdown":           s["max_drawdown"],
        })

    # Sort by profit_factor desc (None = 0)
    results.sort(key=lambda r: r["profit_factor"] or 0, reverse=True)
    return results[:50]  # top 50
