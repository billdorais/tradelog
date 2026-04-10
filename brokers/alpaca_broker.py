import logging
import os
from datetime import datetime, timezone

log = logging.getLogger(__name__)

ALPACA_KEY     = os.environ.get("ALPACA_KEY", "")
ALPACA_SECRET  = os.environ.get("ALPACA_SECRET", "")
ALPACA_PAPER   = os.environ.get("ALPACA_PAPER", "true").lower() != "false"


class AlpacaBroker:
    """
    Alpaca broker using alpaca-py SDK.
    Paper trading:  set ALPACA_PAPER=true  (default) — uses paper-api.alpaca.markets
    Live trading:   set ALPACA_PAPER=false — uses api.alpaca.markets
    """

    def __init__(self, key=None, secret=None, paper=None):
        self._key    = key    or ALPACA_KEY
        self._secret = secret or ALPACA_SECRET
        self._paper  = paper  if paper is not None else ALPACA_PAPER
        self._client = None
        self._trading = None

    def _ensure_client(self):
        if self._trading is not None:
            return
        from alpaca.trading.client import TradingClient
        self._trading = TradingClient(
            api_key    = self._key,
            secret_key = self._secret,
            paper      = self._paper,
        )
        log.info("Alpaca TradingClient initialised (paper=%s)", self._paper)

    def status(self):
        try:
            self._ensure_client()
            acct = self._trading.get_account()
            return {
                "broker":    "Alpaca",
                "connected": True,
                "paper":     self._paper,
                "account":   acct.id,
                "buying_power": float(acct.buying_power),
                "equity":    float(acct.equity),
            }
        except Exception as e:
            return {"broker": "Alpaca", "connected": False, "error": str(e)}

    def place_order(self, ticker, action, quantity, price=None, sec_type="STK", currency="USD"):
        """
        Place a market or limit order.
        action: BUY or SELL
        price:  None = market order, float = limit order
        """
        from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce

        self._ensure_client()

        side = OrderSide.BUY if action.upper() == "BUY" else OrderSide.SELL
        qty  = int(quantity) if quantity else 1

        try:
            if price:
                req = LimitOrderRequest(
                    symbol       = ticker,
                    qty          = qty,
                    side         = side,
                    time_in_force= TimeInForce.DAY,
                    limit_price  = round(float(price), 2),
                )
            else:
                req = MarketOrderRequest(
                    symbol       = ticker,
                    qty          = qty,
                    side         = side,
                    time_in_force= TimeInForce.DAY,
                )
            order = self._trading.submit_order(req)
            log.info("Alpaca order submitted: %s %s %s → id=%s status=%s",
                     action, qty, ticker, order.id, order.status)
            return {
                "success":  True,
                "order_id": str(order.id),
                "status":   str(order.status),
                "symbol":   ticker,
                "qty":      qty,
                "side":     action,
                "paper":    self._paper,
            }
        except Exception as e:
            log.error("Alpaca order failed for %s %s %s: %s", action, qty, ticker, e)
            return {"success": False, "error": str(e)}

    def get_positions(self):
        self._ensure_client()
        try:
            positions = self._trading.get_all_positions()
            return [
                {
                    "symbol": p.symbol,
                    "qty":    float(p.qty),
                    "side":   p.side,
                    "market_value": float(p.market_value),
                    "unrealized_pnl": float(p.unrealized_pl),
                }
                for p in positions
            ]
        except Exception as e:
            log.error("Alpaca get_positions failed: %s", e)
            return []

    def close_all_positions(self):
        self._ensure_client()
        try:
            self._trading.close_all_positions(cancel_orders=True)
            log.info("Alpaca: all positions closed")
            return {"success": True}
        except Exception as e:
            log.error("Alpaca close_all_positions failed: %s", e)
            return {"success": False, "error": str(e)}

    def get_fills(self):
        """Return today's filled orders."""
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        self._ensure_client()
        try:
            req    = GetOrdersRequest(status=QueryOrderStatus.CLOSED, limit=100)
            orders = self._trading.get_orders(filter=req)
            result = []
            for o in orders:
                if str(o.status) != "filled":
                    continue
                result.append({
                    "exec_id":  str(o.id),
                    "time":     o.filled_at.strftime("%Y%m%d  %H:%M:%S") if o.filled_at else "",
                    "symbol":   o.symbol,
                    "sec_type": "STK",
                    "side":     "BOT" if str(o.side) == "buy" else "SLD",
                    "shares":   float(o.filled_qty or 0),
                    "price":    float(o.filled_avg_price or 0),
                    "order_id": str(o.client_order_id or o.id),
                    "account":  "",
                    "exchange": "",
                    "pnl":      None,
                })
            return result
        except Exception as e:
            log.error("Alpaca get_fills failed: %s", e)
            return []
