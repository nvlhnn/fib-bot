"""
Binance Futures API client.

Wraps ccxt for async exchange operations: fetching candles,
placing orders, managing positions, and querying account state.
"""

from __future__ import annotations

import asyncio
import re
import time
from typing import Any

import ccxt.async_support as ccxt
from loguru import logger

from src.core.config import Config
from src.data.models import Candle


class BinanceClient:
    """Async Binance Futures (USDT-M) client via ccxt."""

    def __init__(self, config: Config) -> None:
        self._cfg = config
        self._exchange: ccxt.binanceusdm | None = None
        self._exchange_info: dict | None = None  # Cached
        self._raw_to_unified: dict[str, str] = {}  # Raw symbol → unified symbol map
        self._banned_until_ms = 0
        self._ban_logged_until_ms = 0

    # ── Rate-limit / ban backoff ──────────────────────────

    @property
    def banned_until_ms(self) -> int:
        return self._banned_until_ms

    def is_rate_limited(self) -> bool:
        return int(time.time() * 1000) < self._banned_until_ms

    def rate_limit_sleep_seconds(self, buffer_seconds: int = 5) -> float:
        remaining = (self._banned_until_ms - int(time.time() * 1000)) / 1000
        return max(0.0, remaining + buffer_seconds)

    def _rate_limit_message(self) -> str:
        return f"Binance API temporarily banned until {self._banned_until_ms}"

    def _remember_rate_limit(self, error: Exception) -> None:
        """Record Binance 418 ban windows so loops stop hammering testnet."""
        text = str(error)
        match = re.search(r"banned until (\d+)", text)
        if not match and "418" not in text and "-1003" not in text:
            return

        # Binance usually sends a millisecond epoch. If it does not, use a
        # conservative 60s cool-off so we still stop extending the ban.
        until_ms = int(match.group(1)) if match else int(time.time() * 1000) + 60_000
        self._banned_until_ms = max(self._banned_until_ms, until_ms)

        if self._ban_logged_until_ms != self._banned_until_ms:
            self._ban_logged_until_ms = self._banned_until_ms
            wait_s = self.rate_limit_sleep_seconds(buffer_seconds=0)
            logger.warning(
                "Binance rate-limit ban detected; pausing API calls for {:.0f}s until {}",
                wait_s, self._banned_until_ms,
            )

    def _raise_if_rate_limited(self) -> None:
        if self.is_rate_limited():
            raise RuntimeError(self._rate_limit_message())

    # ── Connection ─────────────────────────────────────────

    async def connect(self) -> None:
        """Initialize exchange connection."""
        opts: dict[str, Any] = {
            "apiKey": self._cfg.binance_api_key,
            "secret": self._cfg.binance_api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "future",
                "adjustForTimeDifference": True,
                # Futures testnet/demo keys cannot call Binance SAPI wallet
                # endpoints; avoid CCXT's authenticated currency preload.
                "fetchCurrencies": False,
            },
        }

        self._exchange = ccxt.binanceusdm(opts)

        if self._cfg.is_testnet:
            # CCXT deprecated Binance futures sandbox mode; route USDT-M futures
            # endpoints directly to Binance Futures testnet instead.
            self._exchange.urls["api"].update({
                "fapiPublic": "https://testnet.binancefuture.com/fapi/v1",
                "fapiPublicV2": "https://testnet.binancefuture.com/fapi/v2",
                "fapiPublicV3": "https://testnet.binancefuture.com/fapi/v3",
                "fapiPrivate": "https://testnet.binancefuture.com/fapi/v1",
                "fapiPrivateV2": "https://testnet.binancefuture.com/fapi/v2",
                "fapiPrivateV3": "https://testnet.binancefuture.com/fapi/v3",
                "fapiData": "https://testnet.binancefuture.com/futures/data",
            })

        # Test connectivity
        await self._exchange.load_markets()
        try:
            balance = await self.get_balance()
        except Exception as e:
            self._remember_rate_limit(e)
            logger.warning("Balance check skipped during connect: {}", e)
            balance = 0.0
        mode = "TESTNET" if self._cfg.is_testnet else "LIVE"
        logger.info("Binance {} connected — balance: ${:.2f}", mode, balance)

        # Build raw → unified symbol map for screener use
        self._build_symbol_map()

    def _build_symbol_map(self) -> None:
        """Build mapping from raw Binance symbol (e.g. BTCUSDT) to ccxt unified (e.g. BTC/USDT:USDT)."""
        if not self._exchange:
            return
        self._raw_to_unified = {}
        for unified, market in self._exchange.markets.items():
            raw_id = market.get("id", "")
            if raw_id:
                self._raw_to_unified[raw_id] = unified

    def raw_to_unified(self, raw_symbol: str) -> str | None:
        """Convert a raw Binance symbol to ccxt unified symbol."""
        return self._raw_to_unified.get(raw_symbol)

    async def close(self) -> None:
        """Close exchange connection."""
        if self._exchange:
            await self._exchange.close()
            self._exchange = None

    # ── Market Data ────────────────────────────────────────

    async def fetch_candles(
        self,
        symbol: str,
        interval: str = "5m",
        limit: int = 200,
    ) -> list[Candle]:
        """Fetch OHLCV candles for a symbol."""
        assert self._exchange is not None
        if self.is_rate_limited():
            return []
        try:
            ohlcv = await self._exchange.fetch_ohlcv(
                symbol, timeframe=interval, limit=limit,
            )
            return [
                Candle(
                    timestamp=int(bar[0]),
                    open=float(bar[1]),
                    high=float(bar[2]),
                    low=float(bar[3]),
                    close=float(bar[4]),
                    volume=float(bar[5]),
                    symbol=symbol,
                    interval=interval,
                )
                for bar in ohlcv
            ]
        except Exception as e:
            self._remember_rate_limit(e)
            logger.error("Failed to fetch candles for {}: {}", symbol, e)
            return []

    async def fetch_exchange_info(self) -> dict:
        """
        Get exchange info (all symbols, contract types, etc.).
        Cached after first call — rarely changes.
        """
        if self._exchange_info is not None:
            return self._exchange_info

        assert self._exchange is not None
        self._raise_if_rate_limited()
        self._exchange_info = await self._exchange.fapiPublicGetExchangeInfo()
        return self._exchange_info

    async def fetch_all_tickers(self) -> dict[str, dict]:
        """
        Fetch 24hr ticker data for ALL symbols in one call.
        Returns dict keyed by symbol.
        """
        assert self._exchange is not None
        self._raise_if_rate_limited()
        try:
            tickers = await self._exchange.fetch_tickers()
            return tickers
        except Exception as e:
            self._remember_rate_limit(e)
            raise

    async def fetch_ticker(self, symbol: str) -> dict:
        """Fetch single symbol ticker."""
        assert self._exchange is not None
        self._raise_if_rate_limited()
        try:
            return await self._exchange.fetch_ticker(symbol)
        except Exception as e:
            self._remember_rate_limit(e)
            raise

    # ── Account ────────────────────────────────────────────

    async def get_balance(self) -> float:
        """Get available USDT balance."""
        assert self._exchange is not None
        self._raise_if_rate_limited()
        try:
            balance = await self._exchange.fetch_balance()
            return float(balance.get("USDT", {}).get("free", 0))
        except Exception as e:
            self._remember_rate_limit(e)
            raise

    async def get_total_balance(self) -> float:
        """Get total USDT balance (including margin)."""
        assert self._exchange is not None
        self._raise_if_rate_limited()
        try:
            balance = await self._exchange.fetch_balance()
            return float(balance.get("USDT", {}).get("total", 0))
        except Exception as e:
            self._remember_rate_limit(e)
            raise

    async def get_positions(self) -> list[dict]:
        """Get all open positions."""
        assert self._exchange is not None
        self._raise_if_rate_limited()
        try:
            positions = await self._exchange.fetch_positions()
            return [p for p in positions if float(p.get("contracts", 0)) > 0]
        except Exception as e:
            self._remember_rate_limit(e)
            raise

    async def get_position(self, symbol: str) -> dict | None:
        """Get the open position for a symbol, if any."""
        for position in await self.get_positions():
            if position.get("symbol") == symbol:
                return position
        return None

    async def get_my_trades(self, symbol: str, limit: int = 100) -> list[dict]:
        """Fetch recent account trades for a symbol."""
        assert self._exchange is not None
        self._raise_if_rate_limited()
        try:
            return await self._exchange.fetch_my_trades(symbol, limit=limit)
        except Exception as e:
            self._remember_rate_limit(e)
            raise

    async def get_open_orders(self, symbol: str | None = None) -> list[dict]:
        """Get open regular + conditional algo orders.

        Binance USDT-M STOP_MARKET / TAKE_PROFIT_MARKET orders are exposed by
        the futures algo endpoints on this testnet. CCXT's normal
        fetch_open_orders() can miss them, which breaks recovery and cleanup.
        """
        assert self._exchange is not None
        self._raise_if_rate_limited()
        orders: list[dict] = []

        try:
            if symbol:
                orders.extend(await self._exchange.fetch_open_orders(symbol))
            else:
                self._exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
                orders.extend(await self._exchange.fetch_open_orders())
        except Exception as e:
            self._remember_rate_limit(e)
            raise

        try:
            params = {}
            if symbol:
                params["symbol"] = self._exchange.market(symbol)["id"]
            algo_orders = await self._exchange.fapiPrivateGetOpenAlgoOrders(params)
            for order in algo_orders:
                raw_symbol = order.get("symbol")
                unified_symbol = (
                    symbol
                    if symbol
                    else self._exchange.safe_symbol(raw_symbol, None, None, "swap")
                )
                orders.append({
                    "id": str(order.get("algoId", "")),
                    "symbol": unified_symbol,
                    "type": str(order.get("orderType", "")).lower(),
                    "side": str(order.get("side", "")).lower(),
                    "amount": float(order.get("quantity") or 0),
                    "price": float(order.get("price") or 0),
                    "stopPrice": float(order.get("triggerPrice") or 0),
                    "status": str(order.get("algoStatus", "")).lower(),
                    "reduceOnly": bool(order.get("reduceOnly")),
                    "info": order,
                })
        except Exception as e:
            self._remember_rate_limit(e)
            logger.debug("Could not fetch open algo orders for {}: {}", symbol or "all", e)

        return orders

    async def get_max_notional_for_leverage(self, symbol: str, leverage: int) -> float | None:
        """Return Binance max notional allowed for a symbol at leverage."""
        assert self._exchange is not None
        try:
            tiers_by_symbol = await self._exchange.fetch_leverage_tiers([symbol])
            tiers = tiers_by_symbol.get(symbol, [])
            eligible = [
                float(t.get("maxNotional") or 0)
                for t in tiers
                if float(t.get("maxLeverage") or 0) >= leverage
            ]
            eligible = [value for value in eligible if value > 0]
            return max(eligible) if eligible else None
        except Exception as e:
            logger.debug("Could not fetch leverage tiers for {}: {}", symbol, e)
            return None

    # ── Trading ────────────────────────────────────────────

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for a symbol."""
        assert self._exchange is not None
        try:
            await self._exchange.set_leverage(leverage, symbol)
            logger.debug("Leverage set: {} = {}x", symbol, leverage)
            return True
        except Exception as e:
            logger.error("Failed to set leverage for {}: {}", symbol, e)
            return False

    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> bool:
        """Set margin type (ISOLATED or CROSSED)."""
        assert self._exchange is not None
        try:
            await self._exchange.set_margin_mode(margin_type.lower(), symbol)
            return True
        except Exception as e:
            # Binance returns error if already set — that's OK
            if "No need to change" in str(e):
                return True
            logger.error("Failed to set margin type for {}: {}", symbol, e)
            return False

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
    ) -> str:
        """Place a limit order. Returns order ID."""
        assert self._exchange is not None
        order = await self._exchange.create_order(
            symbol=symbol,
            type="limit",
            side=side.lower(),
            amount=amount,
            price=price,
        )
        logger.info(
            "Limit order placed: {} {} {} @ {} — id={}",
            side, amount, symbol, price, order["id"],
        )
        return str(order["id"])

    async def place_market_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        reduce_only: bool = False,
    ) -> str:
        """Place a market order. Returns order ID."""
        assert self._exchange is not None
        params = {"reduceOnly": True} if reduce_only else {}
        order = await self._exchange.create_order(
            symbol=symbol,
            type="market",
            side=side.lower(),
            amount=amount,
            params=params,
        )
        logger.info(
            "Market order placed: {} {} {}{} — id={}",
            side, amount, symbol, " (reduceOnly)" if reduce_only else "", order["id"],
        )
        return str(order["id"])

    async def close_position_market(
        self,
        symbol: str,
        side: str,
        amount: float,
    ) -> str:
        """Close an existing futures position with reduce-only market order(s)."""
        order_ids: list[str] = []
        for chunk in self.split_market_amount(symbol, amount):
            order_ids.append(await self.place_market_order(symbol, side, chunk, reduce_only=True))
            # Avoid bursting several market orders into Binance at the same ms.
            await asyncio.sleep(0.2)
        return ",".join(order_ids)

    async def place_stop_loss(
        self,
        symbol: str,
        side: str,
        amount: float,
        stop_price: float,
    ) -> str:
        """Place server-side stop-loss order(s) with reduceOnly. Returns order ID(s)."""
        assert self._exchange is not None
        order_ids: list[str] = []
        for chunk in self.split_market_amount(symbol, amount):
            order = await self._exchange.create_order(
                symbol=symbol,
                type="stop_market",
                side=side.lower(),
                amount=chunk,
                params={"stopPrice": stop_price, "reduceOnly": True},
            )
            logger.info(
                "Stop loss placed: {} {} {} @ {} — id={} (reduceOnly)",
                side, chunk, symbol, stop_price, order["id"],
            )
            order_ids.append(str(order["id"]))
            await asyncio.sleep(0.2)
        return ",".join(order_ids)

    async def place_take_profit(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
    ) -> str:
        """Place take-profit order(s) with reduceOnly. Returns order ID(s)."""
        assert self._exchange is not None
        order_ids: list[str] = []
        for chunk in self.split_market_amount(symbol, amount):
            order = await self._exchange.create_order(
                symbol=symbol,
                type="take_profit_market",
                side=side.lower(),
                amount=chunk,
                params={"stopPrice": price, "reduceOnly": True},
            )
            logger.info(
                "Take profit placed: {} {} {} @ {} — id={} (reduceOnly)",
                side, chunk, symbol, price, order["id"],
            )
            order_ids.append(str(order["id"]))
            await asyncio.sleep(0.2)
        return ",".join(order_ids)

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel an open order."""
        assert self._exchange is not None
        if "," in str(order_id):
            results = [await self.cancel_order(symbol, oid.strip()) for oid in str(order_id).split(",") if oid.strip()]
            return all(results)
        try:
            await self._exchange.cancel_order(order_id, symbol)
            logger.info("Order cancelled: {} — {}", symbol, order_id)
            return True
        except Exception as e:
            try:
                await self._exchange.fapiPrivateDeleteAlgoOrder({"algoId": order_id})
                logger.info("Algo order cancelled: {} — {}", symbol, order_id)
                return True
            except Exception as algo_error:
                logger.error("Failed to cancel order {}: {}; algo: {}", order_id, e, algo_error)
                return False

    async def get_order(self, symbol: str, order_id: str) -> dict:
        """Get order details."""
        assert self._exchange is not None
        if "," in str(order_id):
            orders = []
            for oid in str(order_id).split(","):
                oid = oid.strip()
                if oid:
                    orders.append(await self.get_order(symbol, oid))
            closed = [o for o in orders if o.get("status", "").lower() == "closed"]
            return closed[0] if closed else orders[0]
        return await self._exchange.fetch_order(order_id, symbol)

    # ── Symbol Info ────────────────────────────────────────

    async def get_symbol_info(self, symbol: str) -> dict:
        """Get symbol trading rules (tick size, lot size, etc.)."""
        assert self._exchange is not None
        markets = self._exchange.markets
        return markets.get(symbol, {})

    def get_min_amount(self, symbol: str) -> float:
        """Get minimum order quantity for a symbol."""
        assert self._exchange is not None
        market = self._exchange.markets.get(symbol, {})
        limits = market.get("limits", {}).get("amount", {})
        return float(limits.get("min", 0.001))

    def get_max_amount(self, symbol: str, *, market_order: bool = False) -> float | None:
        """Get maximum order quantity for a symbol, preferring market limits when needed."""
        assert self._exchange is not None
        market = self._exchange.markets.get(symbol, {})
        limit_group = "market" if market_order else "amount"
        value = market.get("limits", {}).get(limit_group, {}).get("max")
        if value is None and market_order:
            value = market.get("limits", {}).get("amount", {}).get("max")
        return float(value) if value else None

    def get_price_precision(self, symbol: str) -> int:
        """Get price decimal precision for a symbol."""
        assert self._exchange is not None
        market = self._exchange.markets.get(symbol, {})
        return int(market.get("precision", {}).get("price", 2))

    def get_amount_precision(self, symbol: str) -> int:
        """Get amount decimal precision for a symbol."""
        assert self._exchange is not None
        market = self._exchange.markets.get(symbol, {})
        return int(market.get("precision", {}).get("amount", 3))

    def format_price(self, symbol: str, price: float) -> float:
        """Format price using ccxt's built-in precision handling."""
        assert self._exchange is not None
        return float(self._exchange.price_to_precision(symbol, price))

    def format_amount(self, symbol: str, amount: float) -> float:
        """Format amount using ccxt's built-in precision handling."""
        assert self._exchange is not None
        return float(self._exchange.amount_to_precision(symbol, amount))

    def split_market_amount(self, symbol: str, amount: float) -> list[float]:
        """Split amount into market-order-safe chunks for closes and STOP_MARKET/TP_MARKET."""
        max_amount = self.get_max_amount(symbol, market_order=True)
        if not max_amount or amount <= max_amount:
            return [self.format_amount(symbol, amount)]

        # Leave a small buffer under Binance's exact cap to avoid precision edge cases.
        chunk_size = self.format_amount(symbol, max_amount * 0.95)
        chunks: list[float] = []
        remaining = float(amount)
        while remaining > 0:
            chunk = min(remaining, chunk_size)
            formatted = self.format_amount(symbol, chunk)
            if formatted <= 0:
                break
            chunks.append(formatted)
            remaining -= formatted
        return chunks
