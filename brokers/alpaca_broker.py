import logging
import os
from datetime import datetime, timezone, date, timedelta

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
        # Positions cache: avoids a blocking API call on every SELL signal.
        # Invalidated after any order is placed so the next check is fresh.
        self._pos_cache     = None   # list of positions or None
        self._pos_cache_ts  = 0.0
        _POS_CACHE_TTL      = 20     # seconds

    _POS_CACHE_TTL = 20  # seconds

    def _get_positions_cached(self):
        """Return open positions, using a 20-second cache to avoid blocking the
        webhook handler on every signal.  Invalidated after any order is placed."""
        now = time.time()
        if self._pos_cache is not None and (now - self._pos_cache_ts) < self._POS_CACHE_TTL:
            return self._pos_cache
        self._pos_cache    = self._trading.get_all_positions()
        self._pos_cache_ts = now
        return self._pos_cache

    def _invalidate_pos_cache(self):
        self._pos_cache    = None
        self._pos_cache_ts = 0.0

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

    def daily_pnl(self):
        """Return today's P&L as equity minus previous trading day's closing equity."""
        self._ensure_client()
        acct = self._trading.get_account()
        return float(acct.equity) - float(acct.last_equity)

    def place_order(self, ticker, action, quantity, price=None, sec_type="STK", currency="USD"):
        """
        Place a market or limit order.
        action: BUY or SELL
        price:  None = market order, float = limit order
        """
        from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce

        self._ensure_client()

        # Normalise crypto tickers: ETHUSD → ETH/USD, COINBASE:BTCUSD → BTC/USD
        import re as _re
        t = ticker.upper()
        if ":" in t:
            t = t.split(":")[-1]
        if "/" not in t:
            m = _re.match(r'^([A-Z]{2,5})(USD[T]?|BTC|ETH)$', t)
            if m:
                t = f"{m.group(1)}/{m.group(2)}"
        ticker = t

        side = OrderSide.BUY if action.upper() == "BUY" else OrderSide.SELL
        # Crypto quantities are fractional; stocks use whole shares
        raw_qty = float(quantity) if quantity else 1
        qty = raw_qty if (raw_qty != int(raw_qty)) else int(raw_qty)
        # Crypto requires GTC; stocks use DAY
        is_crypto = "/" in ticker
        tif = TimeInForce.GTC if is_crypto else TimeInForce.DAY

        # For stock SELL: first cancel any pending BUY orders for this ticker.
        # This handles the race where an exit signal arrives before the entry fill:
        # rather than dropping the exit (leaving the position open), we cancel the
        # pending entry so we never enter at all.
        if not is_crypto and side == OrderSide.SELL:
            try:
                from alpaca.trading.requests import GetOrdersRequest
                from alpaca.trading.enums import QueryOrderStatus, OrderSide as _OS
                open_req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[ticker.upper()])
                open_orders = self._trading.get_orders(filter=open_req)
                pending_buys = [
                    o for o in (open_orders or [])
                    if (o.side.value if hasattr(o.side, 'value') else str(o.side)) == "buy"
                ]
                if pending_buys:
                    cancelled_ids = []
                    for o in pending_buys:
                        try:
                            self._trading.cancel_order_by_id(o.id)
                            cancelled_ids.append(str(o.id))
                            log.warning(
                                "Alpaca SELL %s: cancelled pending BUY order %s "
                                "(exit signal arrived before entry filled — not entering trade).",
                                ticker, o.id,
                            )
                        except Exception as _ce:
                            log.warning("Alpaca cancel order %s failed: %s", o.id, _ce)
                    self._invalidate_pos_cache()
                    return {
                        "success":          False,
                        "skipped":          True,
                        "cancelled_buy":    True,
                        "cancelled_order_ids": cancelled_ids,
                        "error":            f"Pending BUY for {ticker} cancelled — exit signal arrived before fill.",
                    }
            except Exception as _oe:
                log.warning("Alpaca open-order check for %s SELL failed: %s — continuing", ticker, _oe)

        # For stock SELL: verify a long position exists to avoid unintended shorts.
        # If no long is held, skip the order and return an error so the caller can
        # log it rather than Alpaca silently opening a short position.
        if not is_crypto and side == OrderSide.SELL:
            try:
                positions = self._get_positions_cached()
                pos = next((p for p in positions if p.symbol.upper() == ticker.upper()), None)
                held_qty = float(pos.qty) if pos else 0.0
                if held_qty <= 0:
                    log.warning(
                        "Alpaca SELL %s skipped — no long position held (qty=%.4f). "
                        "Order would open an unintended short; ignoring.",
                        ticker, held_qty,
                    )
                    return {
                        "success": False,
                        "skipped": True,
                        "error":   f"No long position in {ticker} to sell (held qty={held_qty}). "
                                   "Sell skipped to prevent unintended short.",
                    }
                # Clamp sell qty to what is actually held
                if int(qty) > int(held_qty):
                    log.warning(
                        "Alpaca SELL %s: requested %s shares but only %s held; clamping.",
                        ticker, qty, int(held_qty),
                    )
                    qty = int(held_qty)
            except Exception as _pe:
                log.warning("Alpaca position check failed for %s SELL: %s — proceeding anyway", ticker, _pe)

        # For crypto SELL: close the position via Alpaca's close_position API
        # (Alpaca doesn't support shorting crypto — this closes whatever is held)
        if is_crypto and side == OrderSide.SELL:
            try:
                # Scan all positions — Alpaca may store symbol as ETHUSD or ETH/USD
                base = ticker.split("/")[0].upper()  # "ETH" from "ETH/USD"
                positions = self._get_positions_cached()
                pos = next((p for p in positions if base in p.symbol.upper()), None)
                if pos is None:
                    all_syms = [p.symbol for p in positions]
                    log.warning("No %s position found. Open positions: %s", base, all_syms)
                    return {"success": False, "error": f"No {base} position to close (open: {all_syms})"}
                order = self._trading.close_position(pos.symbol)
                log.info("Alpaca close_position %s → id=%s status=%s", pos.symbol, order.id, order.status)
                return {
                    "success":  True,
                    "order_id": str(order.id),
                    "status":   str(order.status),
                    "symbol":   pos.symbol,
                    "side":     "sell",
                    "paper":    self._paper,
                }
            except Exception as e:
                log.error("Alpaca close_position failed for %s: %s", ticker, e)
                return {"success": False, "error": str(e)}

        try:
            if price:
                req = LimitOrderRequest(
                    symbol       = ticker,
                    qty          = qty,
                    side         = side,
                    time_in_force= tif,
                    limit_price  = round(float(price), 2),
                )
            else:
                req = MarketOrderRequest(
                    symbol       = ticker,
                    qty          = qty,
                    side         = side,
                    time_in_force= tif,
                )
            order = self._trading.submit_order(req)
            self._invalidate_pos_cache()  # position state changed
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

    def place_option_order(self, underlying, direction, expiry_type="friday", contracts=1, target_premium=2.0):
        """
        Buy a call or put option on Alpaca.
        underlying:    e.g. "TSLA"
        direction:     "call"/"put" or "BUY"/"SELL" or "LONG"/"SHORT"
        expiry_type:   "0dte" = today, "friday" = nearest Friday
        contracts:     number of contracts (each = 100 shares)
        target_premium: target mid-price; selects the strike whose (bid+ask)/2
                        is closest to this value; limit placed at ask for best fill
        """
        from alpaca.trading.requests import GetOptionContractsRequest, LimitOrderRequest
        from alpaca.trading.enums import ContractType, OrderSide, TimeInForce
        self._ensure_client()

        # Resolve direction → call/put
        d = direction.upper()
        opt_type = ContractType.CALL if d in ("CALL", "BUY", "LONG") else ContractType.PUT

        # Resolve expiry date
        today = date.today()
        if expiry_type == "0dte":
            expiry = today
        else:  # friday
            days_ahead = (4 - today.weekday()) % 7  # 4 = Friday
            expiry = today + timedelta(days=days_ahead if days_ahead > 0 else 7)

        log.info("Options order: %s %s %s expiry=%s target=$%.2f contracts=%s",
                 opt_type.value, underlying, expiry_type, expiry, target_premium, contracts)

        # 1. Get option contracts for this underlying / expiry / type
        try:
            contract_req = GetOptionContractsRequest(
                underlying_symbols=[underlying.upper()],
                expiration_date=expiry,
                type=opt_type,
                limit=100,
            )
            resp = self._trading.get_option_contracts(contract_req)
            contract_list = getattr(resp, "option_contracts", None) or []
        except Exception as e:
            log.error("get_option_contracts failed: %s", e)
            return {"success": False, "error": f"Could not fetch option contracts: {e}"}

        if not contract_list:
            return {"success": False, "error": f"No {opt_type.value} contracts found for {underlying} expiring {expiry}"}

        # 2. Get quotes (bid/ask) via the data client to find best strike
        try:
            from alpaca.data.historical.option import OptionHistoricalDataClient
            from alpaca.data.requests import OptionSnapshotRequest
            data_client = OptionHistoricalDataClient(
                api_key=self._key, secret_key=self._secret
            )
            symbols = [c.symbol for c in contract_list[:60]]
            snap_req = OptionSnapshotRequest(symbol_or_symbols=symbols, feed="indicative")
            snapshots = data_client.get_option_snapshot(snap_req)
        except Exception as e:
            log.error("option snapshots failed: %s", e)
            snapshots = {}

        # 3. Pick the contract whose midpoint is closest to target_premium
        best_symbol = None
        best_ask    = None
        best_mid    = None
        best_diff   = float("inf")

        for sym, snap in (snapshots or {}).items():
            try:
                q   = getattr(snap, "latest_quote", None)
                bid = float(getattr(q, "bid_price", 0) or 0)
                ask = float(getattr(q, "ask_price", 0) or 0)
                if bid <= 0 or ask <= 0:
                    continue
                mid  = (bid + ask) / 2
                diff = abs(mid - target_premium)
                if diff < best_diff:
                    best_diff   = diff
                    best_symbol = sym
                    best_ask    = ask
                    best_mid    = mid
            except Exception:
                continue

        # Fallback: if no quotes, pick ATM strike by strike price proximity
        if not best_symbol:
            log.warning("No option quotes returned — falling back to ATM strike selection")
            try:
                # Get current stock price via Coinbase public API as proxy
                import urllib.request as _ur, json as _jx
                with _ur.urlopen(
                    f"https://data.alpaca.markets/v2/stocks/{underlying}/quotes/latest",
                    timeout=5
                ) as _r:
                    _d = _jx.loads(_r.read())
                    stock_price = float((_d.get("quote") or {}).get("ap") or 0)
            except Exception:
                stock_price = 0

            best_contract = min(
                contract_list,
                key=lambda c: abs(float(getattr(c, "strike_price", 0) or 0) - stock_price)
            ) if stock_price else contract_list[len(contract_list) // 2]
            best_symbol = best_contract.symbol
            best_ask    = target_premium * 1.10  # estimate: 10% above target
            best_mid    = target_premium

        # 4. Place limit order at the ask (maximises fill probability)
        limit_price = round(best_ask, 2)
        log.info("Selected option %s mid=$%.2f ask=$%.2f (target=$%.2f diff=$%.2f)",
                 best_symbol, best_mid or 0, best_ask, target_premium, best_diff)

        try:
            order_req = LimitOrderRequest(
                symbol        = best_symbol,
                qty           = int(contracts),
                side          = OrderSide.BUY,
                time_in_force = TimeInForce.DAY,
                limit_price   = limit_price,
            )
            order = self._trading.submit_order(order_req)
            log.info("Alpaca option order submitted: %s x%s @ $%.2f → id=%s status=%s",
                     best_symbol, contracts, limit_price, order.id, order.status)
            return {
                "success":    True,
                "order_id":   str(order.id),
                "status":     str(order.status),
                "symbol":     best_symbol,
                "contracts":  contracts,
                "limit_price": limit_price,
                "mid_price":  round(best_mid, 2) if best_mid else None,
                "paper":      self._paper,
            }
        except Exception as e:
            log.error("Alpaca option order failed for %s: %s", best_symbol, e)
            return {"success": False, "error": str(e)}

    def get_positions(self):
        self._ensure_client()
        try:
            positions = self._get_positions_cached()
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

    def close_position(self, symbol):
        """Close a single open position by symbol."""
        self._ensure_client()
        try:
            order = self._trading.close_position(symbol)
            self._invalidate_pos_cache()
            log.info("Alpaca close_position %s → id=%s status=%s", symbol, order.id, order.status)
            return {
                "success":  True,
                "order_id": str(order.id),
                "status":   str(order.status),
                "symbol":   symbol,
            }
        except Exception as e:
            log.error("Alpaca close_position failed for %s: %s", symbol, e)
            return {"success": False, "error": str(e)}

    def close_all_positions(self):
        self._ensure_client()
        try:
            self._trading.close_all_positions(cancel_orders=True)
            self._invalidate_pos_cache()
            log.info("Alpaca: all positions closed")
            return {"success": True}
        except Exception as e:
            log.error("Alpaca close_all_positions failed: %s", e)
            return {"success": False, "error": str(e)}

    def get_portfolio_history(self, period="1M", timeframe="1D"):
        """Return portfolio equity history from Alpaca.
        period:    '1D' | '1W' | '1M' | '3M' | '6M' | '1A'
        timeframe: '1Min' | '5Min' | '15Min' | '1H' | '1D'
        Returns list of {time, equity, pnl} dicts, one per bar.
        """
        self._ensure_client()
        try:
            from alpaca.trading.requests import GetPortfolioHistoryRequest
            req = GetPortfolioHistoryRequest(period=period, timeframe=timeframe)
            hist = self._trading.get_portfolio_history(history_filter=req)
            timestamps = getattr(hist, "timestamp", []) or []
            equities   = getattr(hist, "equity",    []) or []
            pnls       = getattr(hist, "profit_loss",[]) or []
            result = []
            for ts, eq, pl in zip(timestamps, equities, pnls):
                if eq is None or float(eq) == 0:
                    continue
                # Convert Unix int timestamps → "YYYY-MM-DD" ISO strings so the
                # frontend date filter (slice(0,10) >= fromDate) works correctly.
                if isinstance(ts, int):
                    from datetime import datetime, timezone
                    ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                elif isinstance(ts, str):
                    ts_str = ts
                else:
                    ts_str = str(ts)
                result.append({
                    "time":   ts_str,
                    "equity": float(eq),
                    "pnl":    float(pl) if pl is not None else 0.0,
                })
            log.info("Alpaca portfolio history: %d points (period=%s timeframe=%s)", len(result), period, timeframe)
            return result
        except Exception as e:
            log.error("Alpaca get_portfolio_history failed: %s", e, exc_info=True)
            return []

    def get_fills(self):
        """Return all filled orders, paginating newest-first using until= cursor."""
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        self._ensure_client()
        try:
            result   = []
            seen_ids = set()
            # Use 'after' to cast a wide net back 2 years, then page backwards
            # using 'until' when there are more than 500 results.
            after_ts  = datetime.now(timezone.utc) - timedelta(days=730)
            until_ts  = None
            while True:
                kwargs = dict(status=QueryOrderStatus.CLOSED, limit=500, after=after_ts)
                if until_ts:
                    kwargs["until"] = until_ts
                req    = GetOrdersRequest(**kwargs)
                orders = self._trading.get_orders(filter=req)
                if not orders:
                    break
                oldest_sub = None
                for o in orders:
                    oid = str(o.id)
                    if oid in seen_ids:
                        continue
                    seen_ids.add(oid)
                    status_str = o.status.value if hasattr(o.status, 'value') else str(o.status)
                    side_raw   = o.side.value if hasattr(o.side, 'value') else str(o.side)
                    sub = getattr(o, 'submitted_at', None) or getattr(o, 'created_at', None)
                    if sub and (oldest_sub is None or sub < oldest_sub):
                        oldest_sub = sub
                    if status_str != "filled":
                        continue
                    filled_at = o.filled_at.strftime("%Y-%m-%dT%H:%M:%SZ") if o.filled_at else ""
                    result.append({
                        "exec_id":  oid,
                        "time":     filled_at,
                        "symbol":   o.symbol,
                        "sec_type": "STK",
                        "side":     "BOT" if side_raw == "buy" else "SLD",
                        "shares":   float(o.filled_qty or 0),
                        "price":    float(o.filled_avg_price or 0),
                        "order_id": str(o.client_order_id or o.id),
                        "account":  "",
                        "exchange": "",
                        "pnl":      None,
                    })
                if len(orders) < 500 or oldest_sub is None:
                    break
                until_ts = oldest_sub
            log.info("Alpaca get_fills: returned %d filled orders total", len(result))
            return result
        except Exception as e:
            log.error("Alpaca get_fills failed: %s", e)
            return []
