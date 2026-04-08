import asyncio
import os
import random
import threading
import logging
from ib_async import IB, Stock, Forex, Option, MarketOrder, LimitOrder

log = logging.getLogger(__name__)

IB_HOST = os.environ.get("IB_HOST", "127.0.0.1")
IB_PORT = int(os.environ.get("IB_PORT", 4002))


class IBBroker:
    def __init__(self):
        self._ib         = IB()
        self._lock       = threading.Lock()
        self._last_error = None  # last connection error, surfaced via status()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self):
        self._ensure_event_loop()
        # Pick a unique client ID each attempt to avoid "already in use" errors
        # across gunicorn workers and restarts. Use env override if set, else random.
        client_id = random.randint(10, 999)
        log.info("IB connecting to %s:%s with clientId %s", IB_HOST, IB_PORT, client_id)
        try:
            self._ib.connect(IB_HOST, IB_PORT, clientId=client_id, timeout=15, readonly=False)
            self._last_error = None
            log.info("IB connected — accounts: %s", self._ib.managedAccounts())
        except Exception as e:
            self._last_error = str(e)
            raise

    def disconnect(self):
        self._ib.disconnect()

    def is_connected(self):
        return self._ib.isConnected()

    def _ensure_event_loop(self):
        """Ensure the calling thread has an asyncio event loop (ib_async requires one)."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("closed")
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    def _ensure_connected(self):
        self._ensure_event_loop()
        if not self._ib.isConnected():
            self.connect()

    # ------------------------------------------------------------------
    # Status (used by /api/broker/status)
    # ------------------------------------------------------------------

    def status(self):
        if not self._ib.isConnected():
            return {"connected": False, "broker": "IB",
                    "last_error": self._last_error,
                    "host": IB_HOST, "port": IB_PORT}
        return {
            "connected": True,
            "broker":    "IB",
            "accounts":  list(self._ib.managedAccounts()),
            "server_version": self._ib.client.serverVersion(),
        }

    # ------------------------------------------------------------------
    # Account snapshot (for equity curve polling)
    # ------------------------------------------------------------------

    def account_snapshot(self):
        """
        Return current account values as a dict.
        Fetches NetLiquidation, RealizedPnL, UnrealizedPnL.
        """
        with self._lock:
            self._ensure_connected()
            vals = {v.tag: v.value for v in self._ib.accountValues()
                    if v.currency in ("USD", "")}
            return {
                "net_liq":       float(vals.get("NetLiquidation", 0) or 0),
                "realized_pnl":  float(vals.get("RealizedPnL",    0) or 0),
                "unrealized_pnl": float(vals.get("UnrealizedPnL",  0) or 0),
            }

    # ------------------------------------------------------------------
    # Fill event subscription
    # ------------------------------------------------------------------

    def register_fill_callback(self, callback):
        """Register a callback fired on every new fill/execution."""
        self._ib.execDetailsEvent += callback

    # ------------------------------------------------------------------
    # Executions
    # ------------------------------------------------------------------

    def executions(self):
        """
        Return fills already fetched in this IB session.
        Uses self._ib.fills() (cached, no network call) so it is safe to call
        from any thread, including Flask request threads.
        Prior-session fills are synced via reqExecutions() on startup in the
        background connection thread.
        """
        result = []
        for f in self._ib.fills():
            result.append({
                "exec_id":  f.execution.execId,
                "time":     f.execution.time,
                "symbol":   f.contract.symbol,
                "sec_type": f.contract.secType,
                "side":     f.execution.side,
                "shares":   f.execution.shares,
                "price":    f.execution.price,
                "order_id": f.execution.orderId,
                "account":  f.execution.acctNumber,
                "exchange": f.execution.exchange,
                "pnl":      round(f.commissionReport.realizedPNL, 2)
                            if f.commissionReport
                               and f.commissionReport.realizedPNL == f.commissionReport.realizedPNL
                            else None,
            })
        return result

    def executions_from_ib(self, days_back=7):
        """
        Actively request recent fills from IB via reqExecutions().
        Must be called from the thread that owns the IB event loop (background thread).
        days_back: how many calendar days back to fetch (default 7 to cover weekends/holidays).
        """
        from ib_async import ExecutionFilter
        from datetime import datetime, timedelta, timezone
        since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y%m%d %H:%M:%S")
        with self._lock:
            self._ensure_connected()
            fills = self._ib.reqExecutions(ExecutionFilter(time=since))
            result = []
            for f in fills:
                result.append({
                    "exec_id":  f.execution.execId,
                    "time":     f.execution.time,
                    "symbol":   f.contract.symbol,
                    "sec_type": f.contract.secType,
                    "side":     f.execution.side,
                    "shares":   f.execution.shares,
                    "price":    f.execution.price,
                    "order_id": f.execution.orderId,
                    "account":  f.execution.acctNumber,
                    "exchange": f.execution.exchange,
                    "pnl":      round(f.commissionReport.realizedPNL, 2)
                                if f.commissionReport
                                   and f.commissionReport.realizedPNL == f.commissionReport.realizedPNL
                                else None,
                })
            return result

    # ------------------------------------------------------------------
    # EOD — close all open positions
    # ------------------------------------------------------------------

    def close_all_positions(self):
        """Place market orders to flatten all open positions. Returns list of actions taken."""
        with self._lock:
            self._ensure_connected()
            positions = self._ib.positions()
            closed = []
            for pos in positions:
                qty = pos.position
                if qty == 0:
                    continue
                action   = "SELL" if qty > 0 else "BUY"
                abs_qty  = abs(qty)
                contract = pos.contract
                order    = MarketOrder(action, abs_qty)
                trade    = self._ib.placeOrder(contract, order)
                self._ib.sleep(2)
                log.info("EOD close: %s %s %s — %s", action, abs_qty, contract.symbol, trade.orderStatus.status)
                closed.append({
                    "symbol":   contract.symbol,
                    "action":   action,
                    "qty":      abs_qty,
                    "status":   trade.orderStatus.status,
                })
            return closed

    # ------------------------------------------------------------------
    # Historical data
    # ------------------------------------------------------------------

    def fetch_historical_bars(self, ticker, start, end, interval="1h",
                               what_to_show="TRADES", use_rth=True,
                               on_chunk=None):
        """
        Fetch historical OHLCV bars from IB for a US stock.

        interval: "5m", "15m", "30m", "1h", "1d"
        start/end: "YYYY-MM-DD" strings
        Returns list of bar dicts: {time, open, high, low, close}
        """
        from datetime import datetime, timedelta
        from ib_async import Stock

        bar_size_map = {
            "5m":  "5 mins",
            "15m": "15 mins",
            "30m": "30 mins",
            "1h":  "1 hour",
            "1d":  "1 day",
        }
        bar_size = bar_size_map.get(interval, "1 hour")

        # Chunk sizes per request — IB allows larger durations for longer bar sizes
        chunk_days_map = {
            "5m":  30,    # "1 M" per request
            "15m": 60,
            "30m": 90,
            "1h":  365,
            "1d":  365 * 5,
        }
        chunk_days = chunk_days_map.get(interval, 365)

        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt   = datetime.strptime(end,   "%Y-%m-%d")

        with self._lock:
            self._ensure_connected()
            contract = Stock(ticker, "SMART", "USD")
            self._ib.qualifyContracts(contract)

            all_bars = []
            cursor   = end_dt

            while cursor > start_dt:
                if not self._ib.isConnected():
                    raise RuntimeError("IB disconnected during historical data fetch")

                chunk_start   = max(start_dt, cursor - timedelta(days=chunk_days))
                duration_days = (cursor - chunk_start).days or 1
                end_str       = cursor.strftime("%Y%m%d %H:%M:%S")

                try:
                    bars = self._ib.reqHistoricalData(
                        contract,
                        endDateTime    = end_str,
                        durationStr    = f"{duration_days} D",
                        barSizeSetting = bar_size,
                        whatToShow     = what_to_show,
                        useRTH         = use_rth,
                        formatDate     = 1,
                        keepUpToDate   = False,
                        timeout        = 30,   # don't hang forever on a pacing/connection issue
                    )
                except Exception as e:
                    log.warning("IB reqHistoricalData error for %s: %s", ticker, e)
                    break

                if not bars:
                    log.warning("IB: empty chunk for %s ending %s — stopping", ticker, end_str)
                    break

                if on_chunk:
                    on_chunk(chunk_start.strftime("%Y-%m-%d"), cursor.strftime("%Y-%m-%d"), len(bars))

                for b in bars:
                    try:
                        if hasattr(b.date, "strftime"):
                            dt = b.date.replace(tzinfo=None)
                        else:
                            dt = datetime.strptime(str(b.date)[:19], "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        continue
                    if dt < start_dt or dt > end_dt:
                        continue
                    all_bars.append({
                        "time":  dt,
                        "open":  float(b.open),
                        "high":  float(b.high),
                        "low":   float(b.low),
                        "close": float(b.close),
                    })

                # Move cursor back past the earliest bar in this chunk
                cursor = chunk_start - timedelta(days=1)
                self._ib.sleep(1)  # IB pacing — 1s between requests

        all_bars.sort(key=lambda b: b["time"])
        # Deduplicate by timestamp
        seen = set()
        unique = []
        for b in all_bars:
            if b["time"] not in seen:
                seen.add(b["time"])
                unique.append(b)
        return unique

    # ------------------------------------------------------------------
    # Options helpers (must run on background IB thread)
    # ------------------------------------------------------------------

    def get_option_chain(self, ticker):
        """
        Fetch option chain metadata via reqSecDefOptParams.
        Returns (expirations, strikes) — both sorted lists.
        Must be called from the background IB thread.
        """
        contract = Stock(ticker, "SMART", "USD")
        self._ib.qualifyContracts(contract)
        chains = self._ib.reqSecDefOptParams(
            contract.symbol, "", contract.secType, contract.conId
        )
        if not chains:
            raise RuntimeError(f"No option chain found for {ticker}")
        # Prefer SMART exchange; fall back to first result
        chain = next((c for c in chains if c.exchange == "SMART"), chains[0])
        return sorted(chain.expirations), sorted(chain.strikes)

    @staticmethod
    def _next_weekly_expiry(expirations):
        """
        Return the nearest Friday expiry >= today from a list of 'YYYYMMDD' strings.
        Returns None if none found.
        """
        from datetime import date
        today = date.today()
        candidates = []
        for exp in expirations:
            try:
                d = date(int(exp[:4]), int(exp[4:6]), int(exp[6:8]))
                if d >= today and d.weekday() == 4:  # 4 = Friday
                    candidates.append(d)
            except ValueError:
                continue
        if not candidates:
            return None
        return min(candidates).strftime("%Y%m%d")

    def get_option_quote(self, ticker, expiry, strike, right):
        """
        Return (bid, ask) for a single option contract via reqTickers snapshot.
        Must be called from the background IB thread.
        """
        contract = Option(ticker, expiry, float(strike), right, "SMART")
        self._ib.qualifyContracts(contract)
        tickers = self._ib.reqTickers(contract)
        if not tickers:
            return None, None
        t   = tickers[0]
        bid = float(t.bid) if t.bid and t.bid > 0 else None
        ask = float(t.ask) if t.ask and t.ask > 0 else None
        return bid, ask

    def select_option(self, ticker, action, current_price,
                      option_expiry=None, max_spread=1.0, strike_offset=0):
        """
        Select the nearest liquid option contract for a directional signal.

        - action BUY  → call option
        - action SELL → put option
        - option_expiry: 'YYYYMMDD' override; None = next weekly Friday
        - max_spread:    max bid/ask spread in dollars (default $1.00)
        - strike_offset: strikes OTM from ATM (0 = ATM, 1 = 1 strike OTM, …)

        Returns dict: {expiry, strike, right, bid, ask, spread}
        Raises RuntimeError if no qualifying contract found.
        Must be called from the background IB thread.
        """
        expirations, strikes = self.get_option_chain(ticker)

        expiry = option_expiry or self._next_weekly_expiry(expirations)
        if not expiry:
            raise RuntimeError(f"No weekly Friday expiry found for {ticker}")
        if expiry not in expirations:
            # Snap to closest available date
            closest = min(expirations, key=lambda e: abs(int(e) - int(expiry)))
            log.warning("Expiry %s not in chain for %s — using %s", expiry, ticker, closest)
            expiry = closest

        right = "C" if action.upper() == "BUY" else "P"

        # Build candidate strikes starting at ATM + offset, expanding outward
        if right == "C":
            # Calls: sort ascending; OTM = higher strikes
            ordered = sorted(strikes)
        else:
            # Puts: sort descending; OTM = lower strikes
            ordered = sorted(strikes, reverse=True)

        atm_idx = min(range(len(ordered)), key=lambda i: abs(ordered[i] - current_price))
        start   = min(atm_idx + strike_offset, len(ordered) - 1)
        # Try up to 10 strikes around the target
        candidates = ordered[start: start + 10]

        for strike in candidates:
            try:
                bid, ask = self.get_option_quote(ticker, expiry, strike, right)
                if bid is None or ask is None:
                    continue
                spread = round(ask - bid, 2)
                if spread <= max_spread:
                    log.info(
                        "Option selected: %s %s %s %s bid=%.2f ask=%.2f spread=%.2f",
                        ticker, expiry, strike, right, bid, ask, spread,
                    )
                    return {
                        "expiry": expiry,
                        "strike": float(strike),
                        "right":  right,
                        "bid":    bid,
                        "ask":    ask,
                        "spread": spread,
                    }
            except Exception as e:
                log.warning("Quote failed %s %s %s %s: %s", ticker, expiry, strike, right, e)

        raise RuntimeError(
            f"No {right} option for {ticker} {expiry} with spread ≤ ${max_spread}"
        )

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    def place_order(self, ticker, action, quantity, price=None,
                    sec_type="STK", currency="USD",
                    expiry=None, strike=None, right=None):
        """
        Place a market or limit order.

        sec_type: "STK" (default), "CASH" for forex, or "OPT" for options.
        For OPT: expiry (YYYYMMDD), strike (float), right ("C" / "P") are required.
        Returns a dict with success/error details.
        """
        with self._lock:
            try:
                self._ensure_connected()

                if sec_type == "CASH":
                    contract = Forex(ticker)
                elif sec_type == "OPT":
                    contract = Option(ticker, expiry, float(strike), right, "SMART", currency)
                    self._ib.qualifyContracts(contract)
                else:
                    contract = Stock(ticker, "SMART", currency)

                order = (
                    LimitOrder(action.upper(), float(quantity), float(price))
                    if price
                    else MarketOrder(action.upper(), float(quantity))
                )

                trade = self._ib.placeOrder(contract, order)
                self._ib.sleep(2)  # let event loop process the acknowledgment

                result = {
                    "success":        True,
                    "order_id":       trade.order.orderId,
                    "status":         trade.orderStatus.status,
                    "filled":         trade.orderStatus.filled,
                    "avg_fill_price": trade.orderStatus.avgFillPrice,
                }
                if sec_type == "OPT":
                    result.update({"expiry": expiry, "strike": strike, "right": right})
                return result

            except Exception as e:
                log.error("IB place_order error: %s", e)
                return {"success": False, "error": str(e)}
