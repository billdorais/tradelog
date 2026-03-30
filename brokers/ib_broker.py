import asyncio
import os
import threading
import logging
from ib_async import IB, Stock, Forex, MarketOrder, LimitOrder

log = logging.getLogger(__name__)

IB_HOST = os.environ.get("IB_HOST", "127.0.0.1")
IB_PORT = int(os.environ.get("IB_PORT", 4002))
# Use PID-based client ID so gunicorn master + worker don't collide
IB_CLIENT_ID = int(os.environ.get("IB_CLIENT_ID", 1)) + (os.getpid() % 10)


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

        self._ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=15, readonly=False)
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
