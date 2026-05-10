"""
Binance Futures websocket candle store.

Keeps local kline history for active symbols/timeframes so the strategy can
move away from REST candle polling. This module is intentionally standalone in
Phase 2; later phases wire it into Bot's signal loop.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict, deque
from contextlib import suppress
from typing import Any

import websockets
from loguru import logger

from src.data.models import Candle
from src.exchange.binance_client import BinanceClient


class WebSocketCandleStore:
    """Maintain Binance kline candles from websocket streams."""

    MAINNET_BASE_URL = "wss://fstream.binance.com/stream?streams="
    TESTNET_BASE_URL = "wss://stream.binancefuture.com/stream?streams="

    def __init__(
        self,
        client: BinanceClient,
        *,
        base_url: str | None = None,
        max_history: dict[str, int] | None = None,
        max_streams_per_connection: int = 50,
        reconnect_base_delay_seconds: float = 5.0,
        reconnect_max_delay_seconds: float = 120.0,
    ) -> None:
        self._client = client
        self._base_url = base_url or (
            self.TESTNET_BASE_URL if client.config.is_testnet else self.MAINNET_BASE_URL
        )
        self._max_history = max_history or {"5m": 200, "15m": 100, "1h": 200}
        self._max_streams_per_connection = max(1, max_streams_per_connection)
        self._reconnect_base_delay = reconnect_base_delay_seconds
        self._reconnect_max_delay = reconnect_max_delay_seconds

        self._candles: dict[str, dict[str, deque[Candle]]] = defaultdict(dict)
        self._prices: dict[str, float] = {}
        self._symbols: list[str] = []
        self._timeframes: list[str] = []
        self._include_mark_price = False
        self._mark_price_interval = "1s"
        self._raw_to_unified: dict[str, str] = {}
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._lock = asyncio.Lock()
        self._last_event_ts = 0.0

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def last_event_ts(self) -> float:
        return self._last_event_ts

    async def start(
        self,
        symbols: list[str],
        timeframes: list[str],
        *,
        include_mark_price: bool = False,
        mark_price_interval: str = "1s",
    ) -> None:
        """Start websocket workers for symbols/timeframes."""
        await self.stop()
        self._symbols = list(dict.fromkeys(symbols))
        self._timeframes = list(dict.fromkeys(timeframes))
        self._include_mark_price = include_mark_price
        self._mark_price_interval = mark_price_interval
        self._raw_to_unified = self._build_raw_map(self._symbols)
        streams = self._build_streams(
            self._symbols,
            self._timeframes,
            include_mark_price=include_mark_price,
            mark_price_interval=mark_price_interval,
        )
        if not streams:
            logger.warning("WebSocketCandleStore: no streams to start")
            return

        self._running = True
        for chunk in self._chunks(streams, self._max_streams_per_connection):
            self._tasks.append(asyncio.create_task(self._run_connection(chunk)))
        logger.info(
            "WebSocketCandleStore started: symbols={} timeframes={} mark_price={} streams={} connections={}",
            len(self._symbols), self._timeframes, include_mark_price, len(streams), len(self._tasks),
        )

    async def stop(self) -> None:
        """Stop websocket workers."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()

    async def update_symbols(self, symbols: list[str]) -> None:
        """Restart streams if the active symbol list changed."""
        unique = list(dict.fromkeys(symbols))
        if unique == self._symbols:
            return
        await self.start(
            unique,
            self._timeframes,
            include_mark_price=self._include_mark_price,
            mark_price_interval=self._mark_price_interval,
        )

    async def seed(self, symbol: str, timeframe: str, candles: list[Candle]) -> None:
        """Seed history from REST bootstrap before websocket updates arrive."""
        max_len = self._max_history.get(timeframe, 200)
        async with self._lock:
            self._candles[symbol][timeframe] = deque(candles[-max_len:], maxlen=max_len)

    async def bootstrap_history(
        self,
        symbols: list[str],
        timeframes: list[str],
        *,
        batch_size: int = 2,
        delay_seconds: float = 1.5,
        stop_on_rate_limit: bool = True,
    ) -> dict[str, dict[str, int]]:
        """Seed candle history via slow REST bootstrap.

        This is the only planned REST candle use in websocket mode. It runs
        sequentially/small-batch and stops as soon as Binance backoff is active
        so startup does not extend testnet IP bans.

        Returns: {symbol: {timeframe: candle_count}}
        """
        results: dict[str, dict[str, int]] = {}
        jobs = [(symbol, tf) for symbol in symbols for tf in timeframes]
        batch_size = max(1, batch_size)

        for i in range(0, len(jobs), batch_size):
            if stop_on_rate_limit and self._client.is_rate_limited():
                logger.warning("WebSocketCandleStore bootstrap paused by Binance rate-limit backoff")
                break

            batch = jobs[i:i + batch_size]
            fetched = await asyncio.gather(
                *(self._bootstrap_one(symbol, timeframe) for symbol, timeframe in batch),
                return_exceptions=True,
            )

            for (symbol, timeframe), count in zip(batch, fetched):
                if isinstance(count, Exception):
                    logger.warning("Bootstrap failed for {} {}: {}", symbol, timeframe, count)
                    count = 0
                results.setdefault(symbol, {})[timeframe] = int(count or 0)

            if stop_on_rate_limit and self._client.is_rate_limited():
                logger.warning("WebSocketCandleStore bootstrap stopped after rate-limit detection")
                break

            if i + batch_size < len(jobs):
                await asyncio.sleep(delay_seconds)

        ready = len(self.ready_symbols(timeframes))
        logger.info(
            "WebSocketCandleStore bootstrap complete: ready_symbols={}/{}",
            ready, len(symbols),
        )
        return results

    async def _bootstrap_one(self, symbol: str, timeframe: str) -> int:
        limit = self._max_history.get(timeframe, 200)
        candles = await self._client.fetch_candles(symbol, timeframe, limit=limit)
        if candles:
            await self.seed(symbol, timeframe, candles)
        return len(candles)

    def get(self, symbol: str) -> dict[str, list[Candle]]:
        """Return a copy of candle history for one symbol."""
        data = self._candles.get(symbol, {})
        return {tf: list(candles) for tf, candles in data.items()}

    def get_price(self, symbol: str) -> float | None:
        """Return latest websocket mark price for symbol, if available."""
        return self._prices.get(symbol)

    def snapshot(self, symbols: list[str] | None = None) -> dict[str, dict[str, list[Candle]]]:
        """Return a copy of candle history for selected/all symbols."""
        selected = symbols or list(self._candles.keys())
        return {symbol: self.get(symbol) for symbol in selected}

    def ready_symbols(self, required_timeframes: list[str] | None = None) -> set[str]:
        """Symbols with non-empty candle history for every required timeframe."""
        required = required_timeframes or self._timeframes
        ready: set[str] = set()
        for symbol, by_tf in self._candles.items():
            if all(by_tf.get(tf) for tf in required):
                ready.add(symbol)
        return ready

    def _build_raw_map(self, symbols: list[str]) -> dict[str, str]:
        raw_to_unified: dict[str, str] = {}
        for unified in symbols:
            raw = self._client.unified_to_raw(unified)
            if raw:
                raw_to_unified[raw.upper()] = unified
        return raw_to_unified

    def _build_streams(
        self,
        symbols: list[str],
        timeframes: list[str],
        *,
        include_mark_price: bool = False,
        mark_price_interval: str = "1s",
    ) -> list[str]:
        streams: list[str] = []
        for unified in symbols:
            raw = self._client.unified_to_raw(unified)
            if not raw:
                logger.warning("WebSocketCandleStore: cannot map symbol {} to raw Binance id", unified)
                continue
            for tf in timeframes:
                streams.append(f"{raw.lower()}@kline_{tf}")
            if include_mark_price:
                suffix = "@1s" if mark_price_interval == "1s" else ""
                streams.append(f"{raw.lower()}@markPrice{suffix}")
        return streams

    @staticmethod
    def _chunks(items: list[str], size: int) -> list[list[str]]:
        return [items[i:i + size] for i in range(0, len(items), size)]

    async def _run_connection(self, streams: list[str]) -> None:
        url = self._base_url + "/".join(streams)
        delay = self._reconnect_base_delay
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    logger.info("WebSocketCandleStore connected: {} streams", len(streams))
                    delay = self._reconnect_base_delay
                    async for message in ws:
                        if not self._running:
                            break
                        await self._handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._running:
                    logger.warning(
                        "WebSocketCandleStore disconnected ({} streams): {}; reconnecting in {:.0f}s",
                        len(streams), e, delay,
                    )
                    await asyncio.sleep(delay)
                    delay = min(self._reconnect_max_delay, delay * 2)

    async def _handle_message(self, message: str | bytes) -> None:
        try:
            payload: dict[str, Any] = json.loads(message)
            data = payload.get("data", payload)
            event_type = data.get("e")
            if event_type == "markPriceUpdate":
                raw_symbol = str(data.get("s", "")).upper()
                symbol = self._raw_to_unified.get(raw_symbol)
                if symbol:
                    self._prices[symbol] = float(data.get("p") or 0)
                    self._last_event_ts = time.time()
                return

            kline = data.get("k", {})
            if not kline:
                return

            raw_symbol = str(kline.get("s", "")).upper()
            symbol = self._raw_to_unified.get(raw_symbol)
            if not symbol:
                return

            timeframe = str(kline.get("i", ""))
            candle = Candle(
                timestamp=int(kline.get("t")),
                open=float(kline.get("o")),
                high=float(kline.get("h")),
                low=float(kline.get("l")),
                close=float(kline.get("c")),
                volume=float(kline.get("v")),
                symbol=symbol,
                interval=timeframe,
            )
        except Exception as e:
            logger.debug("WebSocketCandleStore ignored malformed message: {}", e)
            return

        max_len = self._max_history.get(timeframe, 200)
        async with self._lock:
            by_tf = self._candles[symbol]
            if timeframe not in by_tf:
                by_tf[timeframe] = deque(maxlen=max_len)
            candles = by_tf[timeframe]
            if candles and candles[-1].timestamp == candle.timestamp:
                candles[-1] = candle
            else:
                candles.append(candle)
            self._last_event_ts = time.time()
