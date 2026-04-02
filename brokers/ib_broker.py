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
        # ib_insync uses asyncio internally — ensure the calling thread has an event loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("closed")
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

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

    def _ensure_connected(self):
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
        """Return list of filled executions from IB for the current session."""
        with self._lock:
            self._ensure_connected()
            fills = self._ib.fills()
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
