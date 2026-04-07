import asyncio
import os
import random
import threading
import logging
from ib_async import IB, Stock, Forex, MarketOrder, LimitOrder

log = logging.getLogger(__name__)

IB_HOST = os.environ.get("IB_HOST", "127.0.0.1")
IB_PORT = int(os.environ.get("IB_PORT", 4002))
_IB_CLIENT_ID_BASE = int(os.environ.get("IB_CLIENT_ID", 0))


class IBBroker:
    def __init__(self):
        self._ib   = IB()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self):
        self._ensure_event_loop()
        # Pick a unique client ID each attempt to avoid "already in use" errors
        # across gunicorn workers and restarts. Use env override if set, else random.
        client_id = _IB_CLIENT_ID_BASE if _IB_CLIENT_ID_BASE else random.randint(10, 999)
        log.info("IB connecting with clientId %s", client_id)
        self._ib.connect(IB_HOST, IB_PORT, clientId=client_id, timeout=15, readonly=False)
        log.info("IB connected — accounts: %s", self._ib.managedAccounts())

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
            return {"connected": False, "broker": "IB"}
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
        Return list of filled executions from IB.
        Uses reqExecutions() to actively request today's fills from IB
        (unlike fills() which only returns what was seen in the current session).
        """
        from ib_async import ExecutionFilter
        with self._lock:
            self._ensure_connected()
            # reqExecutions with an empty filter returns all today's executions
            fills = self._ib.reqExecutions(ExecutionFilter())
            result = []
            for f in fills:
                result.append({
                    "exec_id":   f.execution.execId,
                    "time":      f.execution.time,
                    "symbol":    f.contract.symbol,
                    "sec_type":  f.contract.secType,
                    "side":      f.execution.side,       # BOT / SLD
                    "shares":    f.execution.shares,
                    "price":     f.execution.price,
                    "order_id":  f.execution.orderId,
                    "account":   f.execution.acctNumber,
                    "exchange":  f.execution.exchange,
                    "pnl":       round(f.commissionReport.realizedPNL, 2)
                                 if f.commissionReport and f.commissionReport.realizedPNL == f.commissionReport.realizedPNL
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

        # Chunk sizes per request to stay within IB limits
        chunk_days_map = {
            "5m":  5,
            "15m": 10,
            "30m": 20,
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
    # Order placement
    # ------------------------------------------------------------------

    def place_order(self, ticker, action, quantity, price=None,
                    sec_type="STK", currency="USD"):
        """
        Place a market or limit order.

        sec_type: "STK" (default) or "CASH" for forex pairs.
        Returns a dict with success/error details.
        """
        with self._lock:
            try:
                self._ensure_connected()

                if sec_type == "CASH":
                    contract = Forex(ticker)
                else:
                    contract = Stock(ticker, "SMART", currency)

                order = (
                    LimitOrder(action.upper(), float(quantity), float(price))
                    if price
                    else MarketOrder(action.upper(), float(quantity))
                )

                trade = self._ib.placeOrder(contract, order)
                self._ib.sleep(2)  # let event loop process the acknowledgment

                return {
                    "success":        True,
                    "order_id":       trade.order.orderId,
                    "status":         trade.orderStatus.status,
                    "filled":         trade.orderStatus.filled,
                    "avg_fill_price": trade.orderStatus.avgFillPrice,
                }

            except Exception as e:
                log.error("IB place_order error: %s", e)
                return {"success": False, "error": str(e)}
