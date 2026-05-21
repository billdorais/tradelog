import logging
import os
import time
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

    def _wait_for_order_filled(self, order_id, max_wait_secs=5):
        """Poll an order until it reaches 'filled' (return True) or a terminal
        non-fill state (return False). Used to gate broker-side stop/trail order
        submission so we don't hit Alpaca's wash-trade rule:
            "potential wash trade detected ... opposite side market/stop order exists"
        which fires when the entry BUY is still PENDING_NEW.

        Returns False on timeout (5s default) so callers can degrade gracefully —
        the entry order itself remains submitted, just without the broker-side
        stop. Kairos' polling stop will still cover the position."""
        deadline = time.time() + max_wait_secs
        while time.time() < deadline:
            try:
                o = self._trading.get_order_by_id(order_id)
                status = (o.status.value if hasattr(o.status, 'value') else str(o.status)).lower()
                # 'filled' = fully done; safe to attach SELL stop without wash-trade reject.
                if status == 'filled':
                    return True
                # Terminal non-fill — entry won't materialize, no stop needed.
                if status in ('canceled', 'expired', 'rejected', 'done_for_day'):
                    return False
            except Exception as _ge:
                log.debug("get_order_by_id polling failed for %s: %s", order_id, _ge)
            time.sleep(0.25)
        return False

    def _close_position_with_retry(self, ticker, max_retries=3, poll_secs=3):
        """Submit close_position, then poll for residual and re-submit if non-zero.

        Handles partial fills on thin-volume names — a single market DAY order
        may only execute against the visible liquidity, with the rest of the
        order canceled at session end, leaving an orphaned residual position
        even though Alpaca returned a successful order_id.

        Returns the most-recently-submitted order object. Safe inside the
        per-ticker FIFO worker since the blocking sleeps don't conflict with
        new signals for the same ticker (they wait behind us in the queue)."""
        sym = ticker.upper()
        last_order = self._trading.close_position(sym)
        self._invalidate_pos_cache()
        for attempt in range(1, max_retries + 1):
            time.sleep(poll_secs)
            try:
                positions = self._trading.get_all_positions()
                self._pos_cache    = positions
                self._pos_cache_ts = time.time()
            except Exception as _pe:
                log.warning("Position recheck failed for %s during close-retry: %s", sym, _pe)
                break
            residual = next(
                (p for p in positions
                 if p.symbol.upper() == sym and abs(float(p.qty or 0)) > 0),
                None,
            )
            if not residual:
                if attempt > 1:
                    log.info("Close-retry: %s fully flat after %d attempt(s)", sym, attempt)
                return last_order
            log.warning(
                "Partial fill detected for %s — %s shares remaining after attempt %d/%d, re-submitting close",
                sym, abs(float(residual.qty)), attempt, max_retries,
            )
            try:
                last_order = self._trading.close_position(sym)
                self._invalidate_pos_cache()
            except Exception as _ce:
                # If Alpaca rejects with "no position" between our check and the close,
                # that's the desired state — treat as success.
                log.info("Close-retry %d for %s ended early: %s", attempt, sym, _ce)
                return last_order
        return last_order

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

    def _activate_trail_on_trigger(self, ticker, qty, action, trail_trigger, trail_offset,
                                    trail_mode, hard_stop_id, ref_price, strat_slug):
        """Background thread: polls unrealized P&L and swaps the hard stop for a
        trailing stop once the position moves trail_trigger in the trader's favour.
        Mirrors TradingView's trail_points concept which delays trail activation."""
        import time as _t
        import threading as _th
        is_long = action.upper() == "BUY"
        _ref    = float(ref_price or 0)
        _qty    = float(qty)
        if trail_mode == "percent":
            trigger_dollars = _ref * (float(trail_trigger) / 100) * _qty
        else:
            trigger_dollars = float(trail_trigger) * _qty
        log.info("Trail activation thread started: %s trigger=$%.2f (%.4g%s, ref=$%.2f)",
                 ticker, trigger_dollars, trail_trigger,
                 "%" if trail_mode == "percent" else "$", _ref)
        deadline = _t.time() + 6 * 3600  # cover full extended hours + buffer
        while _t.time() < deadline:
            _t.sleep(1.0)
            try:
                positions = self._trading.get_all_positions()
                pos = next((p for p in positions
                            if p.symbol.upper() == ticker.upper()), None)
                if not pos:
                    log.info("Trail activation: %s position closed before trigger", ticker)
                    return
                unrealized = float(pos.unrealized_pl or 0)
                if unrealized >= trigger_dollars:
                    log.info("Trail activation: %s unrealized=$%.2f >= trigger=$%.2f "
                             "— cancelling hard stop, submitting trail",
                             ticker, unrealized, trigger_dollars)
                    try:
                        self._trading.cancel_order_by_id(hard_stop_id)
                        log.info("Trail activation: hard stop %s cancelled", hard_stop_id)
                    except Exception as _ce:
                        log.debug("Trail activation: cancel hard stop %s: %s", hard_stop_id, _ce)
                    from alpaca.trading.requests import TrailingStopOrderRequest
                    from alpaca.trading.enums import OrderSide, TimeInForce
                    _trail_side = OrderSide.SELL if is_long else OrderSide.BUY
                    _trail_val  = round(float(trail_offset), 4)
                    _trail_oid  = f"kairos-trail-{strat_slug}-{int(_t.time())}"
                    if trail_mode == "percent":
                        _req = TrailingStopOrderRequest(
                            symbol=ticker, qty=qty, side=_trail_side,
                            time_in_force=TimeInForce.GTC,
                            trail_percent=_trail_val, client_order_id=_trail_oid,
                        )
                        log.info("Trail activated: %s %s trail=%.2f%%",
                                 _trail_side.value, ticker, _trail_val)
                    else:
                        _req = TrailingStopOrderRequest(
                            symbol=ticker, qty=qty, side=_trail_side,
                            time_in_force=TimeInForce.GTC,
                            trail_price=_trail_val, client_order_id=_trail_oid,
                        )
                        log.info("Trail activated: %s %s trail=$%.2f",
                                 _trail_side.value, ticker, _trail_val)
                    try:
                        self._trading.submit_order(_req)
                    except Exception as _te:
                        log.warning("Trail submit failed for %s after activation: %s", ticker, _te)
                    return
            except Exception as _pe:
                log.debug("Trail activation poll error %s: %s", ticker, _pe)
        log.info("Trail activation thread for %s timed out", ticker)

    def place_order(self, ticker, action, quantity, price=None, sec_type="STK", currency="USD", strategy="", is_exit=False, stop_loss=None, trail_trigger=None, trail_offset=None, trail_mode="dollars", hard_stop_dollars=None, ref_price=None):
        """
        Place a market or limit order.
        action: BUY or SELL
        price:  None = market order, float = limit order
        """
        import time as _time
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

        # For stock SELL exit: cancel any pending BUY orders, then use close_position.
        # If entry and exit fire simultaneously the BUY may still be pending — we cancel
        # it, but ALSO check whether an existing position still needs to be closed.
        if not is_crypto and side == OrderSide.SELL and is_exit:
            try:
                from alpaca.trading.requests import GetOrdersRequest
                from alpaca.trading.enums import QueryOrderStatus
                open_req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[ticker.upper()])
                open_orders = self._trading.get_orders(filter=open_req)
                # Pending BUY entries — if present, TV's exit raced an unfilled entry;
                # the cancel-and-reconcile branch below handles that case explicitly.
                pending_buys = [
                    o for o in (open_orders or [])
                    if (o.side.value if hasattr(o.side, 'value') else str(o.side)) == "buy"
                ]
                # NEW: If a broker-side trailing stop is already in place and the position
                # is open, defer to it — it executes natively at Alpaca with no webhook
                # latency, so it fills at a tighter price than close_position market would.
                # Skip this when there's a pending BUY (entry race — handled below).
                if not pending_buys:
                    pending_trails = [
                        o for o in (open_orders or [])
                        if (o.side.value if hasattr(o.side, 'value') else str(o.side)) == "sell"
                        and (o.type.value if hasattr(o.type, 'value') else str(o.type)).lower() == "trailing_stop"
                    ]
                    if pending_trails:
                        positions = self._get_positions_cached()
                        if any(p.symbol.upper() == ticker.upper() and float(p.qty or 0) > 0
                               for p in positions):
                            log.info("EXIT_LONG %s: broker trail %s is active — ignoring TV exit signal "
                                     "(trail will fire natively at Alpaca for a tighter fill)",
                                     ticker, pending_trails[0].id)
                            return {
                                "success":       True,
                                "skipped":       True,
                                "reason":        "broker_trail_active",
                                "trail_order_id": str(pending_trails[0].id),
                                "symbol":        ticker,
                            }
                # No active trail (or entry race) — cancel any pending SELL stops
                # (hard stops, expired trails, etc.) so they don't fire after
                # close_position takes the position to zero.
                pending_sells = [
                    o for o in (open_orders or [])
                    if (o.side.value if hasattr(o.side, 'value') else str(o.side)) == "sell"
                ]
                for o in pending_sells:
                    try:
                        self._trading.cancel_order_by_id(o.id)
                        log.info("Alpaca EXIT_LONG %s: cancelled pending SELL stop %s before close", ticker, o.id)
                    except Exception as _ce:
                        log.warning("Alpaca cancel pending SELL %s failed: %s", o.id, _ce)
                if pending_buys:
                    cancelled_ids = []
                    for o in pending_buys:
                        try:
                            self._trading.cancel_order_by_id(o.id)
                            cancelled_ids.append(str(o.id))
                            log.warning(
                                "Alpaca EXIT_LONG %s: cancelled pending BUY order %s",
                                ticker, o.id,
                            )
                        except Exception as _ce:
                            log.warning("Alpaca cancel order %s failed: %s", o.id, _ce)
                    self._invalidate_pos_cache()
                    # After cancelling the pending entry, check whether an existing
                    # position still needs to be closed. Poll a few times because
                    # Alpaca's positions endpoint can lag a partial fill by 1-2s —
                    # a single immediate check often shows 0 even when the cancel
                    # raced with a partial fill that will appear shortly.
                    has_position = False
                    for _i in range(3):
                        positions = self._trading.get_all_positions()
                        self._pos_cache    = positions
                        self._pos_cache_ts = time.time()
                        if any(p.symbol.upper() == ticker.upper() and float(p.qty or 0) > 0
                               for p in positions):
                            has_position = True
                            break
                        time.sleep(1)
                    if not has_position:
                        return {
                            "success":             False,
                            "skipped":             True,
                            "cancelled_buy":       True,
                            "cancelled_order_ids": cancelled_ids,
                            "error":               f"Pending BUY for {ticker} cancelled — no open position to close.",
                        }
                    log.info("EXIT_LONG %s: pending BUY cancelled but partial-fill position detected — closing", ticker)
            except Exception as _oe:
                log.warning("Alpaca open-order check for %s SELL-exit failed: %s — continuing", ticker, _oe)

            # Use close_position for reliability — Alpaca determines exact qty/direction.
            # Check position exists FIRST: if the user manually flattened the position
            # in Kairos, TV's natural EXIT_LONG would otherwise fall through to a
            # directional SELL below and open a NEW short. Mirrors the EXIT_SHORT path.
            try:
                positions = self._get_positions_cached()
                pos = next((p for p in positions if p.symbol.upper() == ticker.upper()), None)
                if pos is None:
                    log.warning("EXIT_LONG close_position %s: no position found — skipping", ticker)
                    return {"success": False, "error": f"No {ticker} position to close"}
                order = self._close_position_with_retry(ticker)
                log.info("Alpaca close_position (EXIT_LONG) %s → id=%s status=%s",
                         ticker, order.id, order.status)
                return {
                    "success":  True,
                    "order_id": str(order.id),
                    "status":   str(order.status),
                    "symbol":   ticker,
                    "side":     "sell",
                    "paper":    self._paper,
                }
            except Exception as e:
                log.error("Alpaca close_position (EXIT_LONG) failed for %s: %s", ticker, e)
                return {"success": False, "error": str(e)}

        # For non-exit stock SELL: cancel any pending BUY orders (entry race guard).
        if not is_crypto and side == OrderSide.SELL and not is_exit:
            try:
                from alpaca.trading.requests import GetOrdersRequest
                from alpaca.trading.enums import QueryOrderStatus
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

        # For stock BUY that is an exit (EXIT_SHORT): cancel any pending SELL orders
        # first, then use close_position.  Mirrors the existing SELL exit logic above.
        # This handles the race where the short entry is still pending when the exit arrives.
        if not is_crypto and side == OrderSide.BUY and is_exit:
            try:
                from alpaca.trading.requests import GetOrdersRequest
                from alpaca.trading.enums import QueryOrderStatus
                open_req   = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[ticker.upper()])
                open_orders = self._trading.get_orders(filter=open_req)
                # Pending SELL entries — entry race case (see EXIT_LONG mirror above)
                pending_sells = [
                    o for o in (open_orders or [])
                    if (o.side.value if hasattr(o.side, 'value') else str(o.side)) == "sell"
                ]
                # NEW: defer to a live broker-side trailing stop when one exists.
                # The trail closes the short natively at Alpaca with no webhook latency.
                if not pending_sells:
                    pending_trails = [
                        o for o in (open_orders or [])
                        if (o.side.value if hasattr(o.side, 'value') else str(o.side)) == "buy"
                        and (o.type.value if hasattr(o.type, 'value') else str(o.type)).lower() == "trailing_stop"
                    ]
                    if pending_trails:
                        positions = self._get_positions_cached()
                        if any(p.symbol.upper() == ticker.upper() and float(p.qty or 0) < 0
                               for p in positions):
                            log.info("EXIT_SHORT %s: broker trail %s is active — ignoring TV exit signal "
                                     "(trail will fire natively at Alpaca for a tighter fill)",
                                     ticker, pending_trails[0].id)
                            return {
                                "success":       True,
                                "skipped":       True,
                                "reason":        "broker_trail_active",
                                "trail_order_id": str(pending_trails[0].id),
                                "symbol":        ticker,
                            }
                # No active trail (or entry race) — cancel pending BUY stops
                # (hard stops, expired trails, etc.) so they don't fire after close.
                pending_buys_to_cancel = [
                    o for o in (open_orders or [])
                    if (o.side.value if hasattr(o.side, 'value') else str(o.side)) == "buy"
                ]
                for o in pending_buys_to_cancel:
                    try:
                        self._trading.cancel_order_by_id(o.id)
                        log.info("Alpaca EXIT_SHORT %s: cancelled pending BUY stop %s before close", ticker, o.id)
                    except Exception as _ce:
                        log.warning("Alpaca cancel pending BUY %s failed: %s", o.id, _ce)
                if pending_sells:
                    for o in pending_sells:
                        try:
                            self._trading.cancel_order_by_id(o.id)
                            log.warning(
                                "Alpaca EXIT_SHORT %s: cancelled pending SELL order %s "
                                "(exit arrived before entry filled — not entering trade).",
                                ticker, o.id,
                            )
                        except Exception as _ce:
                            log.warning("Alpaca cancel order %s failed: %s", o.id, _ce)
                    self._invalidate_pos_cache()
                    # Poll positions — Alpaca's positions endpoint can lag a partial
                    # fill by 1-2s. If the cancel raced with a partial fill we'd see 0
                    # immediately but a short position shortly. Don't return skipped
                    # until we've waited long enough to be sure nothing's incoming.
                    has_position = False
                    for _i in range(3):
                        positions = self._trading.get_all_positions()
                        self._pos_cache    = positions
                        self._pos_cache_ts = time.time()
                        if any(p.symbol.upper() == ticker.upper() and float(p.qty or 0) != 0
                               for p in positions):
                            has_position = True
                            break
                        time.sleep(1)
                    if not has_position:
                        return {
                            "success":      False,
                            "skipped":      True,
                            "cancelled_sell": True,
                            "error":        f"Pending SELL for {ticker} cancelled — exit arrived before fill.",
                        }
                    log.info("EXIT_SHORT %s: pending SELL cancelled but partial-fill position detected — closing", ticker)
            except Exception as _oe:
                log.warning("Alpaca open-order check for %s BUY-exit failed: %s — continuing", ticker, _oe)

            # Use close_position so Alpaca handles the direction automatically.
            # This is more reliable than a directional BUY when the short may not
            # be settled yet in the positions API.
            try:
                positions = self._get_positions_cached()
                pos = next((p for p in positions if p.symbol.upper() == ticker.upper()), None)
                if pos is None:
                    log.warning("EXIT_SHORT close_position %s: no position found — skipping", ticker)
                    return {"success": False, "error": f"No {ticker} position to close"}
                order = self._close_position_with_retry(ticker)
                log.info("Alpaca close_position (EXIT_SHORT) %s → id=%s status=%s",
                         ticker, order.id, order.status)
                return {
                    "success":  True,
                    "order_id": str(order.id),
                    "status":   str(order.status),
                    "symbol":   ticker,
                    "side":     "buy",
                    "paper":    self._paper,
                }
            except Exception as e:
                log.error("Alpaca close_position (EXIT_SHORT) failed for %s: %s", ticker, e)
                return {"success": False, "error": str(e)}

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
            # Embed strategy name so fills can be identified even if TV DB is reset.
            # Format: kairos-{strategy[:36]}-{unix_ts}  (max 48 chars, all alphanumeric/dash/underscore)
            _strat_slug  = (strategy or "unknown")[:36].replace(" ", "_")
            _client_oid  = f"kairos-{_strat_slug}-{int(_time.time())}"
            if price:
                req = LimitOrderRequest(
                    symbol           = ticker,
                    qty              = qty,
                    side             = side,
                    time_in_force    = tif,
                    limit_price      = round(float(price), 2),
                    client_order_id  = _client_oid,
                )
            else:
                req = MarketOrderRequest(
                    symbol           = ticker,
                    qty              = qty,
                    side             = side,
                    time_in_force    = tif,
                    client_order_id  = _client_oid,
                )
            order = self._trading.submit_order(req)
            self._invalidate_pos_cache()
            log.info("Alpaca order submitted: %s %s %s → id=%s status=%s",
                     action, qty, ticker, order.id, order.status)
            result = {
                "success":  True,
                "order_id": str(order.id),
                "status":   str(order.status),
                "symbol":   ticker,
                "qty":      qty,
                "side":     action,
                "paper":    self._paper,
            }
            # Gate stop-order submission on the entry actually filling. Alpaca rejects
            # opposite-side stops while the parent BUY/SELL is PENDING_NEW with a
            # wash-trade error: "potential wash trade detected. use complex orders".
            # Wait up to 5s; if entry hasn't filled, skip the stops and rely on the
            # Kairos polling stop. Only wait when stops are actually configured.
            _exit_trail   = trail_offset  # stop_loss alone → hard stop, not trailing stop
            _has_stops    = bool(_exit_trail) or bool(stop_loss) or bool(trail_trigger) or bool(hard_stop_dollars and float(hard_stop_dollars) > 0)
            _entry_filled = False
            if not is_exit and not is_crypto and _has_stops:
                _entry_filled = self._wait_for_order_filled(str(order.id), max_wait_secs=5)
                if not _entry_filled:
                    log.warning(
                        "Entry %s %s %s not filled within 5s — skipping broker-side stops "
                        "(falls back to Kairos polling stop). order_id=%s",
                        action, qty, ticker, order.id,
                    )
                    result["stops_skipped"] = "entry_not_filled_in_time"

            # Option A — delayed trail activation: submit hard stop immediately, then swap
            # to a trailing stop once the position moves trail_trigger in our favour.
            # Mirrors TradingView's trail_points parameter.
            _trail_activated = False
            if not is_exit and trail_trigger and trail_offset and not is_crypto and _entry_filled:
                try:
                    from alpaca.trading.requests import StopOrderRequest
                    _ref = float(ref_price or 0) or (float(price) if price else 0.0)
                    _sl  = float(stop_loss or trail_offset)  # fall back to trail_offset if no explicit SL
                    if _ref > 0:
                        if trail_mode == "percent":
                            _stop_px = round(_ref * (1 - _sl / 100), 2) if action.upper() == "BUY" \
                                       else round(_ref * (1 + _sl / 100), 2)
                        else:
                            _stop_px = round(_ref - _sl, 2) if action.upper() == "BUY" \
                                       else round(_ref + _sl, 2)
                        _stop_side = OrderSide.SELL if action.upper() == "BUY" else OrderSide.BUY
                        _hs_req    = StopOrderRequest(
                            symbol=ticker, qty=qty, side=_stop_side,
                            time_in_force=TimeInForce.GTC, stop_price=_stop_px,
                        )
                        _hs_order = self._trading.submit_order(_hs_req)
                        result["hard_stop_order_id"] = str(_hs_order.id)
                        result["hard_stop_price"]    = _stop_px
                        log.info("Hard stop submitted %s %s @ %.2f (trail activation pending at %s%s)",
                                 _stop_side.value, ticker, _stop_px, trail_trigger,
                                 "%" if trail_mode == "percent" else "$")
                        import threading as _thr
                        _thr.Thread(
                            target=self._activate_trail_on_trigger,
                            args=(ticker, qty, action, trail_trigger, trail_offset,
                                  trail_mode, str(_hs_order.id), ref_price or price, _strat_slug),
                            daemon=True,
                        ).start()
                        _trail_activated = True
                    else:
                        log.warning("Trail activation setup skipped for %s: no ref_price — "
                                    "falling back to immediate trail", ticker)
                except Exception as _ha:
                    log.warning("Hard stop for trail activation failed for %s: %s — "
                                "falling back to immediate trail", ticker, _ha)

            # Immediate trail — used when no trail_trigger is configured or activation setup failed.
            if not is_exit and _exit_trail and not is_crypto and _entry_filled and not _trail_activated:
                try:
                    from alpaca.trading.requests import TrailingStopOrderRequest
                    _trail_side = OrderSide.SELL if action.upper() == "BUY" else OrderSide.BUY
                    _trail_val  = round(float(_exit_trail), 4)
                    _trail_oid = f"kairos-trail-{_strat_slug}-{int(_time.time())}"
                    if trail_mode == "percent":
                        _trail_req = TrailingStopOrderRequest(
                            symbol           = ticker,
                            qty              = qty,
                            side             = _trail_side,
                            time_in_force    = TimeInForce.GTC,
                            trail_percent    = _trail_val,
                            client_order_id  = _trail_oid,
                        )
                        log.info("Alpaca trailing stop: %s %s trail=%.2f%% → submitting",
                                 _trail_side.value, ticker, _trail_val)
                    else:
                        _trail_req = TrailingStopOrderRequest(
                            symbol           = ticker,
                            qty              = qty,
                            side             = _trail_side,
                            time_in_force    = TimeInForce.GTC,
                            trail_price      = _trail_val,
                            client_order_id  = _trail_oid,
                        )
                        log.info("Alpaca trailing stop: %s %s trail=$%.2f → submitting",
                                 _trail_side.value, ticker, _trail_val)
                    _trail_order = self._trading.submit_order(_trail_req)
                    result["trail_order_id"] = str(_trail_order.id)
                    result["trail_value"]    = _trail_val
                    result["trail_mode"]     = trail_mode
                except Exception as _te:
                    log.warning("Trailing stop failed for %s: %s — entry order still placed", ticker, _te)
            # stop_loss without trail_offset — submit a fixed hard stop so TV EXIT
            # signals remain the primary exit. Trail would block TV exits; hard stop won't.
            if (not is_exit and stop_loss and not trail_offset and not trail_trigger
                    and not is_crypto and _entry_filled and not _trail_activated
                    and not result.get("trail_order_id")):
                try:
                    from alpaca.trading.requests import StopOrderRequest
                    _ref = float(ref_price or 0) or (float(price) if price else 0.0)
                    if _ref > 0:
                        _sl = float(stop_loss)
                        if trail_mode == "percent":
                            _stop_px = round(_ref * (1 - _sl / 100), 2) if action.upper() == "BUY" \
                                       else round(_ref * (1 + _sl / 100), 2)
                        else:
                            _stop_px = round(_ref - _sl, 2) if action.upper() == "BUY" \
                                       else round(_ref + _sl, 2)
                        _stop_side = OrderSide.SELL if action.upper() == "BUY" else OrderSide.BUY
                        _sl_req = StopOrderRequest(
                            symbol=ticker, qty=qty, side=_stop_side,
                            time_in_force=TimeInForce.GTC, stop_price=_stop_px,
                        )
                        _sl_order = self._trading.submit_order(_sl_req)
                        result["hard_stop_order_id"] = str(_sl_order.id)
                        result["hard_stop_price"]    = _stop_px
                        log.info("Hard stop (safety net) submitted: %s %s @ %.2f — TV exits remain primary",
                                 _stop_side.value, ticker, _stop_px)
                    else:
                        log.warning("Hard stop skipped for %s: no ref_price", ticker)
                except Exception as _se:
                    log.warning("Hard stop (safety net) failed for %s: %s", ticker, _se)

            # Attach a HARD broker-side stop loss for the per-position-stop limit.
            # Sits at Alpaca — fires in milliseconds when triggered, vs the soft
            # Kairos polling stop which slips on fast-moving stocks.
            # Skip when a trailing stop is already attached: Alpaca only allows one
            # opposing-side stop on a position ("held_for_orders" claims all the qty),
            # so the hard stop would fail with "insufficient qty available". The trail
            # is the tighter exit anyway and Kairos' polling stop covers the rest.
            if (not is_exit and hard_stop_dollars and float(hard_stop_dollars) > 0
                    and not is_crypto and _entry_filled
                    and not result.get("trail_order_id")):
                log.info("Hard stop attempt: %s %s qty=%s dollars=%s ref_price=%s price=%s",
                         action, ticker, qty, hard_stop_dollars, ref_price, price)
                try:
                    from alpaca.trading.requests import StopOrderRequest
                    _ref = float(ref_price) if ref_price else (float(price) if price else 0.0)
                    if _ref <= 0:
                        log.warning("Hard stop SKIPPED for %s: no reference price available "
                                    "(ref_price=%s, price=%s) — entry placed without hard stop. "
                                    "Falls back to Kairos polling stop.",
                                    ticker, ref_price, price)
                        result["hard_stop_skipped"] = "no_ref_price"
                    else:
                        _per_share = float(hard_stop_dollars) / float(qty or 1)
                        if action.upper() == "BUY":
                            _stop_px   = round(_ref - _per_share, 2)
                            _stop_side = OrderSide.SELL
                        else:
                            _stop_px   = round(_ref + _per_share, 2)
                            _stop_side = OrderSide.BUY
                        _stop_req = StopOrderRequest(
                            symbol        = ticker,
                            qty           = qty,
                            side          = _stop_side,
                            time_in_force = TimeInForce.GTC,
                            stop_price    = _stop_px,
                        )
                        _stop_order = self._trading.submit_order(_stop_req)
                        result["hard_stop_order_id"] = str(_stop_order.id)
                        result["hard_stop_price"]    = _stop_px
                        log.info("Hard stop SUBMITTED: %s %s @ %.2f (ref=$%.2f, $%.2f/share) "
                                 "→ order_id=%s",
                                 _stop_side.value, ticker, _stop_px, _ref, _per_share,
                                 _stop_order.id)
                except Exception as _hs:
                    log.error("Hard stop FAILED for %s (ref_price=%s, dollars=%s): %s "
                              "— entry order still placed, falls back to polling stop",
                              ticker, ref_price, hard_stop_dollars, _hs, exc_info=True)
                    result["hard_stop_error"] = str(_hs)[:200]
            return result
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
            result = []
            for p in positions:
                try:
                    result.append({
                        "symbol":        p.symbol,
                        "qty":           float(p.qty or 0),
                        "side":          p.side.value if hasattr(p.side, "value") else str(p.side),
                        "market_value":  float(p.market_value or 0),
                        "unrealized_pnl": float(p.unrealized_pl) if p.unrealized_pl is not None else 0.0,
                        "current_price": float(p.current_price or 0),
                        "avg_entry_price": float(p.avg_entry_price) if getattr(p, "avg_entry_price", None) is not None else None,
                    })
                except Exception as _pe:
                    log.warning("Alpaca get_positions: skipping %s due to field error: %s", getattr(p, 'symbol', '?'), _pe)
            return result
        except Exception as e:
            log.error("Alpaca get_positions failed: %s", e, exc_info=True)
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

    def get_fills(self, days=90):
        """Return filled and partially-filled orders for the last `days` days.

        Returns one row per order with the order's actual filled_qty / filled_avg_price.
        Orders that are status=canceled/replaced/partially_filled but have filled_qty>0
        are included — those represent real fills that the activities page shows but
        that the previous "status == filled only" filter was silently dropping (e.g.
        a trailing stop that partially filled, then was canceled by a follow-up exit).

        Paginating 2 years of history was causing 60s+ cold-start delays so we cap
        the lookback at `days`."""
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        self._ensure_client()
        try:
            result   = []
            seen_ids = set()
            dropped_no_fill = 0
            after_ts  = datetime.now(timezone.utc) - timedelta(days=days)
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
                    side_raw   = o.side.value if hasattr(o.side, 'value') else str(o.side)
                    sub = getattr(o, 'submitted_at', None) or getattr(o, 'created_at', None)
                    if sub and (oldest_sub is None or sub < oldest_sub):
                        oldest_sub = sub
                    # Accept any order that actually moved shares, regardless of terminal
                    # status. Canceled / replaced / partially_filled with filled_qty>0
                    # are real executions that the activities page reports.
                    try:
                        filled_qty = float(o.filled_qty or 0)
                    except (TypeError, ValueError):
                        filled_qty = 0.0
                    if filled_qty <= 0:
                        dropped_no_fill += 1
                        continue
                    filled_at = o.filled_at.strftime("%Y-%m-%dT%H:%M:%SZ") if o.filled_at else ""
                    result.append({
                        "exec_id":  oid,
                        "time":     filled_at,
                        "symbol":   o.symbol,
                        "sec_type": "STK",
                        "side":     "BOT" if side_raw == "buy" else "SLD",
                        "shares":   filled_qty,
                        "price":    float(o.filled_avg_price or 0),
                        "order_id": str(o.client_order_id or o.id),
                        "account":  "",
                        "exchange": "",
                        "pnl":      None,
                    })
                if len(orders) < 500 or oldest_sub is None:
                    break
                until_ts = oldest_sub
            log.info("Alpaca get_fills: returned %d orders with fills (skipped %d with no fills)",
                     len(result), dropped_no_fill)
            return result
        except Exception as e:
            log.error("Alpaca get_fills failed: %s", e)
            return []
