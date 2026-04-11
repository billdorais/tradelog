import logging
import os
import uuid

log = logging.getLogger(__name__)

COINBASE_KEY    = os.environ.get("COINBASE_KEY", "")
COINBASE_SECRET = os.environ.get("COINBASE_SECRET", "")


class CoinbaseBroker:
    """
    Coinbase Advanced Trade broker using coinbase-advanced-py SDK.
    No paper mode — Coinbase only has a live API.
    Use small quantities for testing.
    """

    def __init__(self, key=None, secret=None):
        self._key    = key    or COINBASE_KEY
        self._secret = secret or COINBASE_SECRET
        self._client = None

    def _ensure_client(self):
        if self._client is not None:
            return
        from coinbase.rest import RESTClient
        # Railway stores multi-line secrets as literal \n — convert to real newlines
        secret = self._secret.replace("\\n", "\n") if self._secret else self._secret
        self._client = RESTClient(api_key=self._key, api_secret=secret)
        log.info("Coinbase RESTClient initialised")

    def status(self):
        try:
            self._ensure_client()
            resp = self._client.get_accounts()
            # SDK returns a typed object; accounts list is an attribute
            accts = getattr(resp, "accounts", None) or []
            ids = []
            for a in accts[:3]:
                uid = getattr(a, "uuid", None) or (a.get("uuid") if isinstance(a, dict) else None)
                if uid:
                    ids.append(uid)
            return {
                "broker":    "Coinbase",
                "connected": True,
                "accounts":  ids,
            }
        except Exception as e:
            return {"broker": "Coinbase", "connected": False, "error": str(e)}

    def place_order(self, ticker, action, quantity, price=None, sec_type="CRYPTO", currency="USD"):
        """
        Place a market or limit order on Coinbase.
        ticker:   e.g. "BTC" — will be converted to "BTC-USD" product_id
        action:   BUY or SELL
        quantity: base currency amount (e.g. 0.001 BTC)
        price:    None = market order, float = limit order
        """
        self._ensure_client()

        # Build product_id: BTC → BTC-USD
        product_id   = f"{ticker.upper()}-{currency.upper()}"
        client_oid   = str(uuid.uuid4())
        side         = action.upper()

        try:
            if price:
                order_config = {
                    "limit_limit_gtc": {
                        "base_size":   str(quantity),
                        "limit_price": str(round(float(price), 2)),
                        "post_only":   False,
                    }
                }
            else:
                if side == "BUY":
                    order_config = {
                        "market_market_ioc": {
                            "quote_size": str(quantity),  # dollar amount for market buys
                        }
                    }
                else:
                    order_config = {
                        "market_market_ioc": {
                            "base_size": str(quantity),   # coin amount for market sells
                        }
                    }

            result = self._client.create_order(
                client_order_id = client_oid,
                product_id      = product_id,
                side            = side,
                order_configuration = order_config,
            )
            success = result.get("success", False)
            order   = result.get("order_id") or result.get("success_response", {}).get("order_id")
            log.info("Coinbase order %s %s %s → success=%s order_id=%s",
                     side, quantity, product_id, success, order)
            return {
                "success":    success,
                "order_id":   order,
                "product_id": product_id,
                "side":       side,
                "qty":        quantity,
            }
        except Exception as e:
            log.error("Coinbase order failed for %s %s %s: %s", side, quantity, product_id, e)
            return {"success": False, "error": str(e)}

    def get_positions(self):
        """Return non-zero account balances as positions."""
        self._ensure_client()
        try:
            resp   = self._client.get_accounts()
            accts  = getattr(resp, "accounts", None) or []
            result = []
            for acct in accts:
                # Support both typed objects and dicts
                if isinstance(acct, dict):
                    bal = float((acct.get("available_balance") or {}).get("value", 0))
                    sym = acct.get("currency", "")
                else:
                    ab  = getattr(acct, "available_balance", None)
                    bal = float(getattr(ab, "value", 0) or 0)
                    sym = getattr(acct, "currency", "")
                if bal > 0:
                    result.append({
                        "symbol":         sym,
                        "qty":            bal,
                        "side":           "long",
                        "market_value":   None,
                        "unrealized_pnl": None,
                    })
            return result
        except Exception as e:
            log.error("Coinbase get_positions failed: %s", e)
            return []

    def close_all_positions(self):
        """Sell all non-USD balances at market."""
        self._ensure_client()
        results = []
        try:
            for pos in self.get_positions():
                if pos["symbol"] in ("USD", "USDC", "USDT"):
                    continue
                r = self.place_order(pos["symbol"], "SELL", pos["qty"])
                results.append(r)
            return results
        except Exception as e:
            log.error("Coinbase close_all_positions failed: %s", e)
            return [{"success": False, "error": str(e)}]
