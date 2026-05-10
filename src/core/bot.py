"""
Main bot orchestrator — runs the 3-tier async event system.

Tier 1: Coin Scanner     — every 4 hours
Tier 2: Signal Checker   — every 5-minute candle close
Tier 3: Position Monitor — every 30 seconds (when positions open)
"""

from __future__ import annotations

import asyncio
import copy
import json
import time
from datetime import datetime, timezone

from loguru import logger

from src.core.config import Config
from src.data.candle_cache import CandleCache
from src.data.models import PositionState, Signal, Trade
from src.data.ws_candle_store import WebSocketCandleStore
from src.database.db import Database
from src.exchange.binance_client import BinanceClient
from src.notifications.telegram import TelegramNotifier
from src.risk.risk_manager import RiskManager
from src.strategy.fibonacci import FibonacciScorer
from src.strategy.engine import IndicatorEngine
from src.strategy.screener import CoinScanner


class Bot:
    """
    FIB Bot — Fibonacci Scalper.

    Coordinates all subsystems and runs the 3-tier async event loop.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.is_running = False

        # Subsystems
        self.client = BinanceClient(config)
        self.db = Database(config)
        self.notifier = TelegramNotifier(config)
        self.risk_manager = RiskManager(config, self.db)
        self.screener = CoinScanner(config, self.client)
        self.indicator_engine = IndicatorEngine(config)
        self.strategy = FibonacciScorer(config)
        self.candle_cache = CandleCache()
        self.ws_candle_store: WebSocketCandleStore | None = None

        # Position tracking
        self.open_positions: list[PositionState] = []
        self.pending_entries: list[Trade] = []  # Unfilled limit entries

        # Timing
        self._last_heartbeat = 0.0
        self._heartbeat_interval = 3600  # 1 hour
        self._last_position_reconcile = 0.0
        self._last_ws_bootstrap_retry = 0.0

    # ── Lifecycle ──────────────────────────────────────────

    async def start(self) -> None:
        """Initialize all systems and start the event loop."""
        self.is_running = True
        logger.info("=" * 60)
        logger.info("FIB Bot — Fibonacci Scalper")
        logger.info("=" * 60)

        # Connect subsystems
        self.db.connect()
        await self.client.connect()
        await self.notifier.initialize()

        # Initialize risk manager. If testnet is currently 418-banned, do not
        # crash/restart-loop and make the ban worse; use a temporary DB/zero
        # balance and let the normal backoff pause API work until it expires.
        try:
            balance = await self.client.get_balance()
        except Exception as e:
            logger.warning("Risk balance check skipped during rate-limit/backoff: {}", e)
            balance = 0.0
        self.risk_manager.initialize(balance)

        # Recover open positions from exchange
        await self._recover_positions()

        # Initial coin scan
        logger.info("Running initial coin scan...")
        if self.client.is_rate_limited():
            if self.config.market_data_mode == "websocket":
                logger.warning("Initial REST scan skipped during Binance backoff; starting websocket path")
            else:
                sleep_s = self.client.rate_limit_sleep_seconds()
                logger.warning("Initial coin scan paused for Binance rate-limit backoff ({:.0f}s)", sleep_s)
                await self._sleep_with_shutdown(sleep_s)
                if balance <= 0:
                    try:
                        balance = await self.client.get_balance()
                        self.risk_manager.initialize(balance)
                        logger.info("Risk manager balance refreshed after backoff: ${:.2f}", balance)
                    except Exception as e:
                        logger.warning("Balance refresh after backoff failed: {}", e)
                if not self.open_positions:
                    await self._recover_positions()
        active_coins: list[str] = []
        while self.is_running and not active_coins:
            active_coins = await self.screener.scan()
            if active_coins:
                break
            if (
                self.config.market_data_mode == "websocket"
                and self.client.is_rate_limited()
                and self.open_positions
            ):
                logger.warning(
                    "Initial scan blocked by Binance backoff; starting websocket market data for open positions"
                )
                break
            sleep_s = (
                self.client.rate_limit_sleep_seconds()
                if self.client.is_rate_limited()
                else 60.0
            )
            logger.warning("Initial coin scan returned no coins; retrying in {:.0f}s", sleep_s)
            await self._sleep_with_shutdown(sleep_s)
        logger.info("Active coins ({}): {}", len(active_coins), active_coins)

        await self._start_market_data(active_coins)

        # Start trading loops immediately. Startup scan logging / Telegram must
        # never block position monitoring or DCA protection.

        async def _startup_side_effects() -> None:
            try:
                scores = self.screener.get_scores()
                self.db.log_scan(
                    selected_coins=active_coins,
                    scores={s: {"score": c.score, "atr": c.atr_pct, "vol": c.volume_24h}
                            for s, c in scores.items()},
                    total_scanned=0,
                    passed_filter=len(active_coins),
                )
            except Exception as e:
                logger.warning("Initial scan log skipped: {}", e)
            try:
                await asyncio.wait_for(
                    self.notifier.bot_started(
                        mode=self.config.bot_mode,
                        balance=balance,
                        coins=len(active_coins),
                    ),
                    timeout=10,
                )
            except Exception as e:
                logger.warning("Bot-start notification skipped/timed out: {}", e)

        asyncio.create_task(_startup_side_effects())

        # Run the 3-tier system
        try:
            await asyncio.gather(
                self._tier1_coin_scanner(),
                self._tier2_signal_checker(),
                self._tier3_position_monitor(),
                self._heartbeat_loop(),
            )
        except asyncio.CancelledError:
            logger.info("Bot shutting down...")
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """Graceful shutdown."""
        self.is_running = False
        logger.info("Shutting down...")
        if self.ws_candle_store:
            await self.ws_candle_store.stop()
        await self.client.close()
        self.db.close()
        logger.info("Bot stopped.")

    async def _start_market_data(self, active_coins: list[str]) -> None:
        """Start optional websocket candle infrastructure.

        Phase 3 only bootstraps and starts the store behind config. The signal
        loop still reads REST CandleCache until Phase 4 swaps it over.
        """
        md_cfg = self.config.market_data_config
        ws_cfg = md_cfg.get("websocket", {}) or {}
        if self.config.market_data_mode != "websocket" or not ws_cfg.get("enabled", False):
            return
        symbols = self._market_data_symbols(active_coins)
        if not symbols:
            logger.warning("Websocket market data requested but active coin list is empty")
            return

        timeframes = list(ws_cfg.get("timeframes") or ["5m", "15m", "1h"])
        self.ws_candle_store = WebSocketCandleStore(
            self.client,
            max_streams_per_connection=int(ws_cfg.get("max_streams_per_connection", 50)),
            reconnect_base_delay_seconds=float(ws_cfg.get("reconnect_base_delay_seconds", 5)),
            reconnect_max_delay_seconds=float(ws_cfg.get("reconnect_max_delay_seconds", 120)),
        )

        await self.ws_candle_store.start(
            symbols,
            timeframes,
            include_mark_price=bool(ws_cfg.get("mark_price_enabled", True)),
            mark_price_interval=str(ws_cfg.get("mark_price_interval", "1s")),
        )
        if not self.is_running:
            return

        bootstrap_cfg = md_cfg.get("rest_bootstrap", {}) or {}
        if bootstrap_cfg.get("enabled", False):
            await self.ws_candle_store.bootstrap_history(
                symbols,
                timeframes,
                batch_size=int(bootstrap_cfg.get("batch_size", 2)),
                delay_seconds=float(bootstrap_cfg.get("delay_seconds", 1.5)),
            )
            if self.client.is_rate_limited():
                logger.warning("Websocket bootstrap hit Binance backoff; live streams remain active")
        else:
            logger.info("REST candle bootstrap disabled; using websocket live candles only")

    def _market_data_symbols(self, active_coins: list[str]) -> list[str]:
        """Active scan symbols plus any recovered/open position symbols."""
        symbols = list(active_coins)
        for pos in self.open_positions:
            if pos.trade.signal:
                symbols.append(pos.trade.signal.symbol)
        for trade in self.pending_entries:
            if trade.signal:
                symbols.append(trade.signal.symbol)
        return list(dict.fromkeys(symbols))

    async def _sleep_with_shutdown(self, seconds: float, step: float = 1.0) -> None:
        """Sleep in short chunks so systemd stop/SIGTERM can exit quickly."""
        deadline = time.time() + max(0.0, seconds)
        while self.is_running and time.time() < deadline:
            await asyncio.sleep(min(step, max(0.0, deadline - time.time())))

    async def _sleep_for_rate_limit(self, label: str) -> None:
        """Pause until the local Binance cooldown ends, but remain stoppable."""
        sleep_s = self.client.rate_limit_sleep_seconds()
        logger.warning("{} paused for Binance rate-limit backoff ({:.0f}s)", label, sleep_s)
        await self._sleep_with_shutdown(sleep_s)

    # ── Tier 1: Coin Scanner ──────────────────────────────

    async def _tier1_coin_scanner(self) -> None:
        """Scan all futures pairs every 4 hours."""
        interval = self.config.rescreen_interval_hours * 3600

        # Wait before first re-scan (initial scan already done in start())
        await self._sleep_with_shutdown(interval)

        while self.is_running:
            try:
                if self.client.is_rate_limited():
                    await self._sleep_for_rate_limit("Tier 1 scanner")
                    continue

                logger.info("━━━ TIER 1: Coin Scanner ━━━")
                old_coins = set(self.screener.active_coins)

                new_coins = await self.screener.scan()
                new_set = set(new_coins)

                added = new_set - old_coins
                removed = old_coins - new_set

                if added or removed:
                    await self.notifier.coin_rotation(added, removed, new_coins)

                if self.ws_candle_store and new_coins:
                    ws_cfg = self.config.market_data_config.get("websocket", {}) or {}
                    timeframes = list(ws_cfg.get("timeframes") or ["5m", "15m", "1h"])
                    symbols = self._market_data_symbols(new_coins)
                    await self.ws_candle_store.update_symbols(symbols)
                    bootstrap_cfg = self.config.market_data_config.get("rest_bootstrap", {}) or {}
                    if bootstrap_cfg.get("enabled", False) and not self.client.is_rate_limited():
                        await self.ws_candle_store.bootstrap_history(
                            symbols,
                            timeframes,
                            batch_size=int(bootstrap_cfg.get("batch_size", 2)),
                            delay_seconds=float(bootstrap_cfg.get("delay_seconds", 1.5)),
                        )

                # Log scan
                scores = self.screener.get_scores()
                self.db.log_scan(
                    selected_coins=new_coins,
                    scores={s: {"score": c.score} for s, c in scores.items()},
                    added=added,
                    removed=removed,
                )

            except Exception as e:
                logger.error("Tier 1 error: {}", e)
                await self.notifier.error_alert(f"Scanner error: {e}")

            await self._sleep_with_shutdown(interval)

    # ── Tier 2: Signal Checker ────────────────────────────

    async def _tier2_signal_checker(self) -> None:
        """Check for signals on every 5-minute candle close."""
        while self.is_running:
            try:
                # Wait for next candle close
                await self._wait_for_candle_close()

                if self.client.is_rate_limited() and not self.ws_candle_store:
                    await self._sleep_for_rate_limit("Signal checker")
                    continue

                active_coins = self.screener.active_coins
                if not active_coins:
                    logger.debug("No active coins — skipping cycle")
                    continue

                logger.info(
                    "━━━ TIER 2: Signal Check ({} coins) ━━━",
                    len(active_coins),
                )

                candle_data = await self._get_signal_candle_data(active_coins)

                # Process each coin
                all_signals: list[Signal] = []
                checked_count = 0
                skipped_count = 0
                for symbol in active_coins:
                    data = candle_data.get(symbol, {})
                    candles_5m = data.get("5m", [])
                    candles_15m = data.get("15m", [])
                    candles_1h = data.get("1h", [])

                    if not candles_5m or not candles_15m or not candles_1h:
                        skipped_count += 1
                        continue

                    checked_count += 1

                    # Calculate indicators
                    ind_set, rsi_hist, price_hist = self.indicator_engine.calculate(
                        symbol, candles_5m, candles_15m, candles_1h,
                    )

                    # Run Fibonacci strategy
                    signal = self.strategy.evaluate(ind_set, rsi_hist, price_hist, candles_5m, candles_15m, candles_1h)

                    if signal is not None:
                        signal.timestamp = int(time.time() * 1000)
                        all_signals.append(signal)
                        logger.info(
                            "  {} {} — FIB score {} ({})",
                            signal.direction, signal.symbol,
                            signal.confluence_score, signal.quality,
                        )

                if not all_signals:
                    logger.info(
                        "Signal cycle complete — checked={} skipped={} signals=0",
                        checked_count, skipped_count,
                    )
                    continue

                # Sort by score (best first)
                all_signals.sort(
                    key=lambda s: s.confluence_score, reverse=True,
                )

                # Validate and execute
                taken_count = 0
                rejected_count = 0
                failed_count = 0
                for signal in all_signals:
                    cooldown_reason = self._zone_cooldown_reason(signal)
                    if cooldown_reason:
                        rejected_count += 1
                        logger.info(
                            "  {} {} rejected: {}",
                            signal.direction, signal.symbol, cooldown_reason,
                        )
                        self.db.log_signal(signal, taken=False, reason=cooldown_reason)
                        continue

                    approved, reason = self.risk_manager.validate(signal)

                    if approved:
                        trade = await self._execute_signal(signal)
                        if trade:
                            taken_count += 1
                            self.db.log_signal(signal, taken=True)
                        else:
                            failed_count += 1
                            self.db.log_signal(signal, taken=False, reason="Execution failed")
                    else:
                        rejected_count += 1
                        logger.debug(
                            "  {} {} rejected: {}",
                            signal.direction, signal.symbol, reason,
                        )
                        self.db.log_signal(signal, taken=False, reason=reason)

                logger.info(
                    "Signal cycle complete — checked={} skipped={} signals={} taken={} rejected={} failed={}",
                    checked_count, skipped_count, len(all_signals),
                    taken_count, rejected_count, failed_count,
                )

            except Exception as e:
                logger.error("Tier 2 error: {}", e)
                await self._sleep_with_shutdown(10)

    async def _get_signal_candle_data(self, active_coins: list[str]) -> dict[str, dict[str, list]]:
        """Return candle data for signal evaluation.

        In websocket mode, this is a local in-memory snapshot and makes no REST
        candle calls. REST mode keeps the existing CandleCache behavior.
        """
        if self.config.market_data_mode == "websocket" and self.ws_candle_store:
            required_timeframes = ["5m", "15m", "1h"]
            ready = self.ws_candle_store.ready_symbols(required_timeframes)
            if not ready:
                await self._retry_ws_bootstrap_if_due(active_coins, required_timeframes)
                ready = self.ws_candle_store.ready_symbols(required_timeframes)
            if not ready:
                logger.warning("Websocket candle store has no ready symbols yet")
                return {}
            usable = [symbol for symbol in active_coins if symbol in ready]
            skipped = len(active_coins) - len(usable)
            if skipped:
                logger.info("Websocket candle data not ready for {} symbols", skipped)
            return self.ws_candle_store.snapshot(usable)

        # REST fallback/default.
        return await self.candle_cache.update(active_coins, self.client)

    async def _retry_ws_bootstrap_if_due(self, active_coins: list[str], timeframes: list[str]) -> None:
        """Retry REST history bootstrap after Binance backoff clears.

        If startup hits a 418, websocket streams can connect but indicators have
        no history. This retries conservatively from the signal loop instead of
        waiting hours for the next scanner rotation.
        """
        if not self.ws_candle_store or self.client.is_rate_limited():
            return

        md_cfg = self.config.market_data_config
        bootstrap_cfg = md_cfg.get("rest_bootstrap", {}) or {}
        if not bootstrap_cfg.get("enabled", False):
            return
        retry_interval = float(bootstrap_cfg.get("retry_interval_seconds", 300))
        now = time.time()
        if now - self._last_ws_bootstrap_retry < max(60.0, retry_interval):
            return

        symbols = self._market_data_symbols(active_coins)
        if not symbols:
            return

        self._last_ws_bootstrap_retry = now
        logger.info("Retrying websocket historical bootstrap for {} symbols", len(symbols))
        await self.ws_candle_store.bootstrap_history(
            symbols,
            timeframes,
            batch_size=int(bootstrap_cfg.get("batch_size", 2)),
            delay_seconds=float(bootstrap_cfg.get("delay_seconds", 1.5)),
        )

    def _zone_cooldown_reason(self, signal: Signal) -> str:
        """Prevent repeated entries on the same Fib zone after it closes.

        - Losing/SL-like closes block the same symbol + level + swing until the
          swing changes.
        - Winning closes only cool the same symbol + level + swing briefly.
        This keeps broad scanner frequency high without re-buying the same
        broken pullback every cycle.
        """
        meta = signal.metadata or {}
        zone = meta.get("fib_zone", {}) or {}
        swing = meta.get("swing", {}) or {}
        level = zone.get("level")
        if level is None or not swing:
            return ""

        cfg = self.config.get("strategy", "fibonacci", default={})
        win_cooldown_ms = int(cfg.get("zone_cooldown_minutes", 30)) * 60_000
        loss_blocks = cfg.get("block_zone_after_loss", True)
        now_ms = int(time.time() * 1000)

        try:
            recent = self.db.get_recent_trades(signal.symbol, limit=30)
        except Exception as e:
            logger.warning("Could not read zone cooldown history for {}: {}", signal.symbol, e)
            return ""

        for trade in recent:
            if trade.get("status") != "CLOSED":
                continue
            try:
                old_meta = json.loads(trade.get("metadata") or "{}")
            except Exception:
                continue
            old_zone = old_meta.get("fib_zone", {}) or {}
            old_swing = old_meta.get("swing", {}) or {}
            if round(float(old_zone.get("level", -1)), 3) != round(float(level), 3):
                continue
            if not self._same_fib_swing(swing, old_swing):
                continue

            net = float(trade.get("net_pnl") or 0)
            if net < 0 and loss_blocks:
                return f"Zone blocked after loss: {signal.symbol} {zone.get('name')} same swing"

            closed_at = int(trade.get("closed_at") or 0)
            if closed_at and now_ms - closed_at < win_cooldown_ms:
                remaining = int((win_cooldown_ms - (now_ms - closed_at)) / 60_000) + 1
                return f"Zone cooldown: {signal.symbol} {zone.get('name')} {remaining}m remaining"

        return ""

    def _same_fib_swing(self, current: dict, previous: dict) -> bool:
        try:
            return (
                current.get("direction") == previous.get("direction")
                and abs(float(current.get("start", 0)) - float(previous.get("start", 0))) < 1e-9
                and abs(float(current.get("end", 0)) - float(previous.get("end", 0))) < 1e-9
            )
        except Exception:
            return False

    # ── Tier 3: Position Monitor ──────────────────────────

    async def _tier3_position_monitor(self) -> None:
        """Monitor pending entries and open positions every 30 seconds."""
        while self.is_running:
            try:
                if self.client.is_rate_limited() and not self.ws_candle_store:
                    await self._sleep_for_rate_limit("Position monitor")
                    continue

                # Reconcile exchange state periodically instead of every 30s.
                # Order/position truth remains REST, but websocket mark prices
                # handle normal PnL/exit monitoring between reconciliations.
                if self._should_reconcile_positions() and not self.client.is_rate_limited():
                    # First reconcile parent positions. If a 0.5 parent has already
                    # closed by TP/SL/manual, cancel its still-resting 0.618 DCA
                    # before checking pending entries, so the DCA cannot become a
                    # standalone trade after the idea is finished.
                    await self._sync_open_positions_with_exchange()
                    await self._cancel_orphan_dca_entries()
                    self._last_position_reconcile = time.time()

                # Then check pending entry fills.
                if not self.client.is_rate_limited():
                    await self._check_pending_entries()

                if not self.open_positions:
                    await self._sleep_with_shutdown(5)
                    continue

                for pos in list(self.open_positions):
                    trade = pos.trade
                    signal = trade.signal
                    if not signal:
                        continue

                    current_price = await self._get_monitor_price(signal.symbol)
                    if current_price <= 0:
                        continue

                    pos.current_price = current_price
                    pos.bars_held += 1

                    # Calculate unrealized P&L
                    if signal.direction == "LONG":
                        pos.unrealized_pnl = (
                            (current_price - trade.entry_fill_price)
                            / trade.entry_fill_price * trade.position_size
                        )
                    else:
                        pos.unrealized_pnl = (
                            (trade.entry_fill_price - current_price)
                            / trade.entry_fill_price * trade.position_size
                        )

                    # R:R ratio
                    stop_dist = abs(trade.entry_fill_price - signal.stop_loss)
                    if stop_dist > 0:
                        if signal.direction == "LONG":
                            pos.unrealized_rr = (
                                (current_price - trade.entry_fill_price) / stop_dist
                            )
                        else:
                            pos.unrealized_rr = (
                                (trade.entry_fill_price - current_price) / stop_dist
                            )

                    # ── DCA from original Fib swing, then exit management ──
                    await self._maybe_dca_from_monitor(pos)
                    await self._manage_exit(pos)

                await self._check_portfolio_take_profit()

                await self._sleep_with_shutdown(30)

            except Exception as e:
                logger.error("Tier 3 error: {}", e)
                await self._sleep_with_shutdown(5)

    def _should_reconcile_positions(self) -> bool:
        if self.pending_entries:
            return True
        interval = float(
            self.config.market_data_config
            .get("position_monitor", {})
            .get("reconcile_interval_seconds", 300)
        )
        return time.time() - self._last_position_reconcile >= max(30.0, interval)

    async def _get_monitor_price(self, symbol: str) -> float:
        if self.config.market_data_mode == "websocket" and self.ws_candle_store:
            price = self.ws_candle_store.get_price(symbol)
            if price and price > 0:
                return float(price)

        if self.client.is_rate_limited():
            return 0.0
        try:
            ticker = await self.client.fetch_ticker(symbol)
            return float(ticker.get("last", 0) or 0)
        except Exception:
            return 0.0

    def _portfolio_take_profit_config(self) -> dict:
        """Portfolio-level TP config for closing all managed positions."""
        return self.config.execution_config.get("portfolio_take_profit", {}) or {}

    def _portfolio_net_unrealized_pnl(self) -> float:
        """Estimated net unrealized PnL across managed open positions."""
        cfg = self._portfolio_take_profit_config()
        fee_rate = float(cfg.get("estimated_round_trip_fee_rate", 0.0008) or 0.0)
        mode = str(cfg.get("mode", "net") or "net").lower()
        gross = sum(float(pos.unrealized_pnl or 0.0) for pos in self.open_positions)
        if mode == "gross":
            return gross
        estimated_fees = sum(float(pos.trade.position_size or 0.0) * fee_rate for pos in self.open_positions)
        return gross - estimated_fees

    async def _check_portfolio_take_profit(self) -> None:
        """Close all managed positions when combined net uPNL reaches target."""
        cfg = self._portfolio_take_profit_config()
        if not cfg.get("enabled", False):
            return
        if not self.open_positions:
            return

        target = float(cfg.get("target_usdt", 0) or 0)
        if target <= 0:
            return

        combined_pnl = self._portfolio_net_unrealized_pnl()
        if combined_pnl < target:
            return

        logger.info(
            "Portfolio TP triggered: combined {} uPNL ${:.2f} >= ${:.2f}; closing {} positions",
            str(cfg.get("mode", "net") or "net").upper(), combined_pnl, target, len(self.open_positions),
        )
        # Close sequentially to avoid Binance/order-race surprises.
        for pos in list(self.open_positions):
            await self._close_position(pos, "PORTFOLIO_TP")

    async def _sync_open_positions_with_exchange(self) -> None:
        """Reconcile all tracked positions before processing pending entries."""
        for pos in list(self.open_positions):
            await self._sync_position_with_exchange(pos)

    def _is_dca_trade(self, trade: Trade) -> bool:
        """Return True only for 0.618 orders that were explicitly created as DCA.

        Plain scanner entries can also be at the configured 0.618 Fib level.
        Those are normal first-leg trades when no 0.5 parent exists; treating
        every 0.618 pending order as DCA caused fresh entries to be cancelled as
        "orphan DCA" and sometimes flattened after a partial fill.
        """
        signal = trade.signal
        if not signal:
            return False
        cfg = self.config.get("strategy", "fibonacci", default={})
        meta = signal.metadata or {}
        dca_source = meta.get("dca_source")
        if dca_source not in {"position_monitor", "scanner_parent"} and not meta.get("dca_parent_trade_id"):
            return False
        zone = meta.get("fib_zone", {}) or {}
        try:
            return round(float(zone.get("level", -1)), 3) == round(float(cfg.get("dca_to_level", 0.618)), 3)
        except Exception:
            return False

    async def _cancel_orphan_dca_entries(self) -> None:
        """Cancel pending 0.618 DCA orders whose 0.5 parent is no longer open."""
        for trade in list(self.pending_entries):
            signal = trade.signal
            if not signal or not self._is_dca_trade(trade):
                continue
            if self._find_dca_parent(signal) is not None:
                continue

            managed_same_symbol = next(
                (
                    pos for pos in self.open_positions
                    if pos.trade.signal
                    and pos.trade.signal.symbol == signal.symbol
                    and pos.trade.signal.direction == signal.direction
                ),
                None,
            )

            status = "unknown"
            filled_amount = 0.0
            try:
                order = await self.client.get_order(signal.symbol, trade.entry_order_id)
                status = order.get("status", "").lower()
                filled_amount = float(
                    order.get("filled")
                    or order.get("info", {}).get("executedQty")
                    or 0
                )
            except Exception as e:
                logger.warning("Could not inspect orphan DCA {}: {}", trade.entry_order_id, e)

            if managed_same_symbol is not None:
                # Parent may already have been merged into a 0.618/2-leg trade,
                # so _find_dca_parent() intentionally returns None. Never
                # flatten a managed same-symbol position here; at most cancel a
                # duplicate resting DCA order.
                if status not in ("closed", "canceled", "cancelled", "expired", "rejected"):
                    try:
                        await self.client.cancel_order(signal.symbol, trade.entry_order_id)
                        logger.warning(
                            "Cancelled duplicate DCA while managed position exists: {} {} order={}",
                            signal.direction, signal.symbol, trade.entry_order_id,
                        )
                    except Exception as e:
                        logger.error("Failed to cancel duplicate DCA {}: {}", trade.entry_order_id, e)
                elif status == "closed" or filled_amount > 0:
                    logger.critical(
                        "Duplicate DCA filled while managed position exists; leaving position open for normal protection: {} {} order={}",
                        signal.direction, signal.symbol, trade.entry_order_id,
                    )
                    await self.notifier.error_alert(
                        f"Duplicate DCA filled while managed position exists: {signal.symbol}. Check combined protection."
                    )
                if trade in self.pending_entries:
                    self.pending_entries.remove(trade)
                trade.status = "CANCELLED" if status != "closed" and filled_amount <= 0 else "CLOSED"
                trade.close_reason = "DCA_DUPLICATE_AFTER_MERGE"
                trade.closed_at = int(time.time() * 1000)
                self.db.save_trade(trade)
                continue

            if status not in ("closed", "canceled", "cancelled", "expired", "rejected"):
                try:
                    await self.client.cancel_order(signal.symbol, trade.entry_order_id)
                    logger.info(
                        "Cancelled orphan DCA after parent close: {} {} order={}",
                        signal.direction, signal.symbol, trade.entry_order_id,
                    )
                except Exception as e:
                    logger.error("Failed to cancel orphan DCA {}: {}", trade.entry_order_id, e)

            # If it somehow filled after the parent was gone, flatten it rather
            # than managing it as a new standalone 0.618 entry.
            if status == "closed" or filled_amount > 0:
                try:
                    expected_side = "long" if signal.direction == "LONG" else "short"
                    close_side = "sell" if signal.direction == "LONG" else "buy"
                    exchange_pos = await self.client.get_position(signal.symbol)
                    if exchange_pos and (exchange_pos.get("side") or "").lower() == expected_side:
                        amount = abs(float(exchange_pos.get("contracts") or filled_amount))
                        if amount > 0:
                            await self.client.close_position_market(signal.symbol, close_side, amount)
                            logger.warning(
                                "Flattened orphan DCA fill after parent close: {} {} amount={}",
                                signal.direction, signal.symbol, amount,
                            )
                except Exception as e:
                    logger.critical("Failed to flatten orphan DCA {}: {}", signal.symbol, e)
                    await self.notifier.error_alert(
                        f"Orphan DCA filled after parent close and flatten failed: {signal.symbol} — {e}"
                    )

            if trade in self.pending_entries:
                self.pending_entries.remove(trade)
            trade.status = "CANCELLED" if status != "closed" and filled_amount <= 0 else "CLOSED"
            trade.close_reason = "DCA_PARENT_CLOSED"
            trade.closed_at = int(time.time() * 1000)
            self.db.save_trade(trade)

    async def _maybe_dca_from_monitor(self, pos: PositionState) -> None:
        """Trigger Fib DCA from the open trade's original swing/level.

        Scanner-based DCA can miss the level when the swing recalculates. This
        monitor uses the stored 0.5 entry swing and watches the stored 0.618
        retracement directly.
        """
        trade = pos.trade
        signal = trade.signal
        if not signal:
            return

        cfg = self.config.get("strategy", "fibonacci", default={})
        if not cfg.get("dca_enabled", False):
            return

        meta = signal.metadata or {}
        zone = meta.get("fib_zone", {}) or {}
        swing = meta.get("swing", {}) or {}
        try:
            current_level = round(float(zone.get("level", -1)), 3)
            from_level = round(float(cfg.get("dca_from_level", 0.5)), 3)
            to_level = float(cfg.get("dca_to_level", 0.618))
            legs = int(meta.get("dca_legs", 1) or 1)
            max_legs = int(cfg.get("dca_max_legs", 2) or 2)
            start = float(swing.get("start", 0))
            end = float(swing.get("end", 0))
        except Exception:
            return

        # On restart, Binance gives us the net position but stale DB metadata can
        # still say this is a single 0.5 leg. If live notional is already larger
        # than one planned leg, infer that DCA has happened and do not add more.
        pos_cfg = self.config.risk_config.get("position", {})
        planned_leg_notional = float(pos_cfg.get("fixed_margin_usdt", 0) or 0) * int(
            pos_cfg.get("fixed_leverage", self.config.base_leverage)
        )
        if planned_leg_notional > 0 and trade.position_size >= planned_leg_notional * 1.5:
            inferred_legs = max(2, round(trade.position_size / planned_leg_notional))
            if inferred_legs >= max_legs:
                if legs < inferred_legs:
                    meta["dca_legs"] = inferred_legs
                    meta["dca_inferred_from_notional"] = True
                    signal.metadata = meta
                    self.db.save_trade(trade)
                return

        if current_level != from_level or legs >= max_legs or start <= 0 or end <= 0:
            return
        if meta.get("dca_triggered_at"):
            return

        size = abs(end - start)
        if size <= 0:
            return
        if swing.get("direction") == "UP":
            dca_price = end - size * to_level
            touched = pos.current_price <= dca_price
        else:
            dca_price = end + size * to_level
            touched = pos.current_price >= dca_price
        if not touched:
            await self._ensure_monitor_dca_limit(pos, dca_price)
            return

        # Mark before order placement so a transient error doesn't loop several
        # market DCA attempts every 30s without human review.
        meta["dca_triggered_at"] = int(time.time() * 1000)
        meta["dca_trigger_price"] = pos.current_price
        signal.metadata = meta
        self.db.save_trade(trade)

        try:
            await self._execute_monitor_dca(pos, dca_price)
        except Exception as e:
            logger.error("Monitor DCA failed for {}: {}", signal.symbol, e)
            await self.notifier.error_alert(f"Monitor DCA failed: {signal.symbol} — {e}")

    async def _ensure_monitor_dca_limit(self, pos: PositionState, dca_price: float) -> None:
        """Keep one resting DCA limit order at the stored 0.618 level."""
        trade = pos.trade
        signal = trade.signal
        if not signal:
            return

        dca_signal = self._build_monitor_dca_signal(signal, dca_price, dca_price)
        if self._has_duplicate_pending_signal(dca_signal):
            return

        cfg = self.config.get("strategy", "fibonacci", default={})
        pos_cfg = self.config.risk_config.get("position", {})
        leverage = int(pos_cfg.get("fixed_leverage", self.config.base_leverage))
        notional = float(pos_cfg.get("fixed_margin_usdt", 5.0)) * leverage
        if notional <= 0 or dca_price <= 0:
            return

        entry_side = "buy" if signal.direction == "LONG" else "sell"
        amount = self.client.format_amount(signal.symbol, notional / dca_price)
        min_amount = self.client.get_min_amount(signal.symbol)
        if amount < min_amount:
            logger.warning("DCA limit amount too small for {}: {} < {}", signal.symbol, amount, min_amount)
            return

        price = self.client.format_price(signal.symbol, dca_price)
        order_id = await self.client.place_limit_order(signal.symbol, entry_side, amount, price)
        dca_trade = Trade(
            signal=dca_signal,
            entry_order_id=order_id,
            status="PENDING",
            entry_fill_price=0.0,
            position_size=notional,
            margin_used=notional / leverage,
            leverage=leverage,
            opened_at=int(time.time() * 1000),
        )
        self.pending_entries.append(dca_trade)
        self.db.save_trade(dca_trade)
        logger.info(
            "📝 DCA limit armed: {} {} ret_{} @ ${:.4f} size=${:.2f}",
            signal.direction, signal.symbol, cfg.get("dca_to_level", 0.618), price, notional,
        )

    async def _execute_monitor_dca(self, pos: PositionState, dca_price: float) -> None:
        trade = pos.trade
        signal = trade.signal
        if not signal:
            return

        cfg = self.config.get("strategy", "fibonacci", default={})
        pos_cfg = self.config.risk_config.get("position", {})
        leverage = int(pos_cfg.get("fixed_leverage", self.config.base_leverage))
        notional = float(pos_cfg.get("fixed_margin_usdt", 5.0)) * leverage
        if notional <= 0 or pos.current_price <= 0:
            return

        entry_side = "buy" if signal.direction == "LONG" else "sell"
        amount = self.client.format_amount(signal.symbol, notional / pos.current_price)
        min_amount = self.client.get_min_amount(signal.symbol)
        if amount < min_amount:
            logger.warning("DCA amount too small for {}: {} < {}", signal.symbol, amount, min_amount)
            return

        order_id = await self.client.place_market_order(signal.symbol, entry_side, amount)
        try:
            order = await self.client.get_order(signal.symbol, order_id)
        except Exception:
            order = {"id": order_id, "average": pos.current_price, "price": pos.current_price, "filled": amount, "amount": amount}

        fill_price = float(order.get("average") or order.get("price") or pos.current_price)
        filled_amount = float(
            order.get("filled")
            or order.get("amount")
            or order.get("info", {}).get("executedQty")
            or amount
        )

        dca_signal = self._build_monitor_dca_signal(signal, fill_price, dca_price)

        dca_trade = Trade(
            signal=dca_signal,
            entry_order_id=order_id,
            status="OPEN",
            entry_fill_price=fill_price,
            position_size=notional,
            margin_used=notional / leverage,
            leverage=leverage,
            opened_at=int(time.time() * 1000),
        )

        exchange_pos = await self.client.get_position(signal.symbol)
        if not exchange_pos:
            raise RuntimeError("DCA order filled but exchange position not found")
        await self._merge_dca_fill(pos, dca_trade, order, exchange_pos, "MONITOR_DCA")

    def _build_monitor_dca_signal(self, signal: Signal, entry_price: float, dca_price: float) -> Signal:
        cfg = self.config.get("strategy", "fibonacci", default={})
        dca_signal = copy.deepcopy(signal)
        dca_signal.entry_price = entry_price
        dca_signal.metadata = copy.deepcopy(signal.metadata or {})
        dca_signal.metadata["fib_zone"] = {
            "name": f"ret_{cfg.get('dca_to_level', 0.618)}",
            "low": dca_price,
            "high": dca_price,
            "level": float(cfg.get("dca_to_level", 0.618)),
        }
        dca_signal.metadata["dca_source"] = "position_monitor"
        return dca_signal

    async def _manage_exit(self, pos: PositionState) -> None:
        """Manage trailing stop and time-based exit for a position."""
        trade = pos.trade
        signal = trade.signal
        if not signal:
            return

        exchange_pos = await self._sync_position_with_exchange(pos)
        if exchange_pos is None:
            return

        position_amount = abs(float(exchange_pos.get("contracts") or 0))
        if position_amount <= 0:
            return

        exec_cfg = self.config.execution_config
        trail_cfg = exec_cfg.get("trailing", {})
        if not trail_cfg.get("enabled", True):
            return

        # ── Breakeven ──
        be_rr = trail_cfg.get("breakeven_at_rr", 1.5)
        if pos.unrealized_rr >= be_rr and pos.trailing_stop == 0:
            buffer = trail_cfg.get("breakeven_buffer_pct", 0.05) / 100.0
            if signal.direction == "LONG":
                new_stop = trade.entry_fill_price * (1 + buffer)
            else:
                new_stop = trade.entry_fill_price * (1 - buffer)

            pos.trailing_stop = new_stop
            # Update stop on exchange
            try:
                # Cancel old stop
                if trade.stop_order_id:
                    await self.client.cancel_order(signal.symbol, trade.stop_order_id)

                # Place new stop
                sl_side = "sell" if signal.direction == "LONG" else "buy"
                amount = self.client.format_amount(signal.symbol, position_amount)
                new_id = await self.client.place_stop_loss(
                    signal.symbol, sl_side, amount, new_stop,
                )
                trade.stop_order_id = new_id

                logger.info(
                    "  {} SL → breakeven ${:.4f}",
                    signal.symbol, new_stop,
                )
                await self.notifier.stop_updated(signal.symbol, new_stop, "BREAKEVEN")
            except Exception as e:
                logger.error("Failed to update SL: {}", e)

        # ── Trailing stop ──
        trail_rr = trail_cfg.get("activate_at_rr", 2.0)
        if pos.unrealized_rr >= trail_rr and pos.trailing_stop > 0:
            atr_val = signal.metadata.get("atr", 0)
            if atr_val > 0:
                new_stop = None
                if signal.direction == "LONG":
                    candidate = pos.current_price - atr_val
                    if candidate > pos.trailing_stop:
                        new_stop = candidate
                else:
                    candidate = pos.current_price + atr_val
                    if candidate < pos.trailing_stop:
                        new_stop = candidate

                if new_stop is not None:
                    try:
                        if trade.stop_order_id:
                            await self.client.cancel_order(signal.symbol, trade.stop_order_id)
                        sl_side = "sell" if signal.direction == "LONG" else "buy"
                        amount = self.client.format_amount(signal.symbol, position_amount)
                        new_id = await self.client.place_stop_loss(
                            signal.symbol, sl_side, amount, new_stop,
                        )
                        trade.stop_order_id = new_id
                        pos.trailing_stop = new_stop
                        logger.info("  {} trailing SL → ${:.4f}", signal.symbol, new_stop)
                        await self.notifier.stop_updated(signal.symbol, new_stop, "TRAILING")
                    except Exception as e:
                        logger.error("Failed to update trailing SL: {}", e)

        # ── Time-based exit ──
        time_cfg = exec_cfg.get("time_stop", {})
        if time_cfg.get("enabled", True):
            max_bars = time_cfg.get("max_bars", 15)
            min_move = time_cfg.get("min_move_pct", 0.3) / 100.0

            if pos.bars_held >= max_bars * 6:  # bars_held counts every 30s
                price_move = abs(
                    pos.current_price - trade.entry_fill_price
                ) / trade.entry_fill_price
                if price_move < min_move:
                    logger.info(
                        "  {} time exit — {}bars, only {:.2f}% move",
                        signal.symbol, pos.bars_held, price_move * 100,
                    )
                    await self._close_position(pos, "TIME")

    async def _sync_position_with_exchange(self, pos: PositionState) -> dict | None:
        """Reconcile in-memory position with Binance before managing exits.

        If SL/TP/manual action already closed the exchange position, mark the
        local trade closed and cancel any leftover protective order. This avoids
        sending a later time-exit order that can accidentally flip the position.
        """
        trade = pos.trade
        signal = trade.signal
        if not signal:
            return None

        try:
            exchange_pos = await self.client.get_position(signal.symbol)
        except Exception as e:
            logger.error("Failed to fetch exchange position for {}: {}", signal.symbol, e)
            return None

        expected_side = "long" if signal.direction == "LONG" else "short"
        actual_side = (exchange_pos or {}).get("side", "").lower()

        if exchange_pos and actual_side == expected_side:
            return exchange_pos

        # No matching live position. Binance likely closed via SL/TP or manual.
        exit_order = None
        close_reason = "EXCHANGE"
        for order_id, reason in (
            (trade.stop_order_id, "SL"),
            (trade.tp_order_id, "TP"),
        ):
            if not order_id:
                continue
            try:
                order = await self.client.get_order(signal.symbol, order_id)
                if order.get("status", "").lower() == "closed":
                    exit_order = order
                    close_reason = reason
                    break
            except Exception as e:
                logger.warning("Could not inspect protective order {}: {}", order_id, e)

        # Cancel any remaining protective orders after the exchange position is gone.
        for order_id in (trade.stop_order_id, trade.tp_order_id):
            if not order_id or (exit_order and str(exit_order.get("id")) == order_id):
                continue
            await self.client.cancel_order(signal.symbol, order_id)

        exit_price = pos.current_price
        actual_fees = None
        if exit_order:
            exit_price = float(
                exit_order.get("average")
                or exit_order.get("price")
                or exit_order.get("stopPrice")
                or exit_order.get("info", {}).get("avgPrice")
                or exit_order.get("info", {}).get("stopPrice")
                or exit_price
            )
        else:
            trade_fill = await self._recent_exit_fill(signal.symbol, signal.direction, trade.opened_at)
            if trade_fill:
                exit_price, actual_fees = trade_fill

        trade.status = "CLOSED"
        trade.exit_fill_price = exit_price
        if trade.entry_fill_price and trade.position_size:
            if signal.direction == "LONG":
                trade.pnl = (exit_price - trade.entry_fill_price) / trade.entry_fill_price * trade.position_size
            else:
                trade.pnl = (trade.entry_fill_price - exit_price) / trade.entry_fill_price * trade.position_size
            trade.fees = actual_fees if actual_fees is not None else trade.position_size * 0.0004 * 2
            trade.net_pnl = trade.pnl - trade.fees
        trade.closed_at = int(time.time() * 1000)
        trade.close_reason = close_reason

        self.db.save_trade(trade)
        self.risk_manager.remove_open_position(trade.id)
        self.risk_manager.record_trade_result(trade)
        if pos in self.open_positions:
            self.open_positions.remove(pos)

        await self.notifier.position_closed(trade, close_reason)
        logger.warning(
            "Position reconciled from exchange: {} {} closed by {} @ ${:.6f} — local P&L ${:.2f}",
            signal.direction, signal.symbol, close_reason, exit_price, trade.net_pnl,
        )
        return None

    async def _recent_exit_fill(
        self,
        symbol: str,
        direction: str,
        opened_at: int,
    ) -> tuple[float, float] | None:
        """Infer actual exit fill from account trades when algo order lookup fails.

        Binance testnet sometimes returns -2013 for filled STOP_MARKET /
        TAKE_PROFIT_MARKET algo IDs. In that case, use recent opposite-side
        account trades after the entry opened time instead of the stale mark
        price from the last position snapshot.
        """
        try:
            fills = await self.client.get_my_trades(symbol, limit=100)
        except Exception as e:
            logger.warning("Could not fetch fills for {} reconciliation: {}", symbol, e)
            return None

        exit_side = "sell" if direction == "LONG" else "buy"
        matched = []
        for fill in fills:
            if fill.get("timestamp", 0) <= opened_at:
                continue
            if str(fill.get("side", "")).lower() != exit_side:
                continue
            info = fill.get("info", {}) or {}
            # Entry fills have zero realizedPnl; closing fills report realizedPnl.
            if float(info.get("realizedPnl", 0) or 0) == 0:
                continue
            matched.append(fill)

        amount = sum(float(f.get("amount", 0) or 0) for f in matched)
        if amount <= 0:
            return None
        cost = sum(float(f.get("cost", 0) or 0) for f in matched)
        fees = sum(float((f.get("fee") or {}).get("cost", 0) or 0) for f in matched)
        return cost / amount, fees

    # ── Trade Execution ────────────────────────────────────

    async def _execute_signal(self, signal: Signal) -> Trade | None:
        """Execute a signal: set leverage, place entry order only.

        SL/TP are placed after entry fill confirmation (see _check_pending_entries).
        """
        try:
            if await self._has_unmanaged_exchange_positions():
                logger.warning("Skipping new entry: unmanaged exchange position exists")
                return None

            dca_parent = self._find_dca_parent(signal)
            if dca_parent is not None:
                signal = copy.deepcopy(signal)
                signal.metadata = copy.deepcopy(signal.metadata or {})
                signal.metadata["dca_source"] = "scanner_parent"
                signal.metadata["dca_parent_trade_id"] = dca_parent.trade.id

            if self._has_duplicate_pending_signal(signal):
                logger.info("Skipping duplicate pending Fib entry: {} {}", signal.symbol, signal.metadata.get("fib_zone", {}).get("name"))
                return None

            balance = await self.client.get_balance()

            # Check combined pending + open against limits
            max_pos = self.config.max_open_positions
            if max_pos > 0 and len(self.open_positions) + len(self.pending_entries) >= max_pos:
                logger.warning("Position limit reached (open={}, pending={})",
                               len(self.open_positions), len(self.pending_entries))
                return None

            # Calculate position size
            sizing = self.risk_manager.calculate_position_size(
                balance, signal, signal.metadata.get("atr", 0),
            )

            if sizing["position_size"] < self.config.risk_config.get(
                "position", {}
            ).get("min_order_value", 5.0):
                logger.warning("Position size too small: ${:.2f}", sizing["position_size"])
                return None

            # Set leverage and margin type
            await self.client.set_margin_type(signal.symbol, self.config.margin_type)
            if not await self.client.set_leverage(signal.symbol, sizing["leverage"]):
                logger.warning(
                    "Skipping {}: could not set leverage to {}x",
                    signal.symbol, sizing["leverage"],
                )
                return None

            # Binance enforces symbol-specific notional caps per leverage tier.
            # Cap before formatting amount so weak/large signals are downsized
            # instead of failing with -2027 at order placement.
            max_notional = await self.client.get_max_notional_for_leverage(
                signal.symbol, sizing["leverage"],
            )
            if max_notional and sizing["position_size"] > max_notional * 0.95:
                capped_size = max_notional * 0.95
                sizing["position_size"] = round(capped_size, 2)
                sizing["margin_required"] = round(capped_size / sizing["leverage"], 2)
                logger.info(
                    "Capped {} size to ${:.2f} for {}x leverage tier",
                    signal.symbol, sizing["position_size"], sizing["leverage"],
                )

            # STOP_MARKET / TAKE_PROFIT_MARKET protection uses Binance's market
            # quantity cap, which can be much lower than the regular limit-order
            # cap on tiny-price contracts. Keep entries inside that cap when
            # possible; the client also chunks protection/close orders as a
            # fallback for recovered or already-filled oversized positions.
            max_market_amount = self.client.get_max_amount(signal.symbol, market_order=True)
            if max_market_amount:
                max_protectable_size = max_market_amount * signal.entry_price * 0.95
                if sizing["position_size"] > max_protectable_size:
                    sizing["position_size"] = round(max_protectable_size, 2)
                    sizing["margin_required"] = round(
                        sizing["position_size"] / sizing["leverage"], 2,
                    )
                    logger.info(
                        "Capped {} size to ${:.2f} for market/protection qty limit",
                        signal.symbol, sizing["position_size"],
                    )

            # Use ccxt precision methods
            amount = self.client.format_amount(
                signal.symbol, sizing["position_size"] / signal.entry_price,
            )
            min_amount = self.client.get_min_amount(signal.symbol)
            if amount < min_amount:
                logger.warning(
                    "{} amount {:.6f} < min {:.6f}",
                    signal.symbol, amount, min_amount,
                )
                return None

            entry_price = self.client.format_price(signal.symbol, signal.entry_price)

            # ── Place entry order only — SL/TP after fill ──
            entry_side = "buy" if signal.direction == "LONG" else "sell"

            entry_id = await self.client.place_limit_order(
                signal.symbol, entry_side, amount, entry_price,
            )

            # Create trade as PENDING (no SL/TP yet)
            trade = Trade(
                signal=signal,
                entry_order_id=entry_id,
                status="PENDING",
                entry_fill_price=0.0,  # Set on fill
                position_size=sizing["position_size"],
                margin_used=sizing["margin_required"],
                leverage=sizing["leverage"],
                opened_at=int(time.time() * 1000),
            )

            # Track as pending — NOT in open_positions or risk_manager yet
            self.pending_entries.append(trade)
            self.db.save_trade(trade)

            logger.info(
                "📝 {} {} — entry limit ${:.4f} size=${:.2f} lev={}x (pending fill)",
                signal.direction, signal.symbol,
                entry_price, sizing["position_size"], sizing["leverage"],
            )

            return trade

        except Exception as e:
            logger.error("Execution failed for {}: {}", signal.symbol, e)
            await self.notifier.error_alert(f"Execution failed: {signal.symbol} — {e}")
            return None

    def _has_duplicate_pending_signal(self, signal: Signal) -> bool:
        """Avoid stacking identical pending orders for the same Fib zone/swing."""
        meta = signal.metadata or {}
        zone = meta.get("fib_zone", {}) or {}
        swing = meta.get("swing", {}) or {}
        level = zone.get("level")
        if level is None:
            return any(t.signal and t.signal.symbol == signal.symbol for t in self.pending_entries)

        for trade in self.pending_entries:
            old = trade.signal
            if not old or old.symbol != signal.symbol or old.direction != signal.direction:
                continue
            old_meta = old.metadata or {}
            old_zone = old_meta.get("fib_zone", {}) or {}
            old_swing = old_meta.get("swing", {}) or {}
            try:
                if round(float(old_zone.get("level", -1)), 3) != round(float(level), 3):
                    continue
            except Exception:
                continue
            if self._same_fib_swing(swing, old_swing):
                return True
        return False

    async def _has_unmanaged_exchange_positions(self) -> bool:
        """Return True when Binance has positions this bot is not managing."""
        managed_symbols = {
            pos.trade.signal.symbol
            for pos in self.open_positions
            if pos.trade.signal is not None
        }
        try:
            positions = await self.client.get_positions()
        except Exception as e:
            logger.error("Could not check exchange positions before entry: {}", e)
            return True

        unmanaged = [
            p.get("symbol")
            for p in positions
            if p.get("symbol") not in managed_symbols
        ]
        if unmanaged:
            logger.warning("Unmanaged exchange positions detected: {}", unmanaged)
            return True
        return False

    async def _check_pending_entries(self) -> None:
        """Check if pending entry orders have been filled, cancelled, or timed out."""
        entry_timeout = self.config.execution_config.get("entry_timeout_seconds", 300)

        for trade in list(self.pending_entries):
            signal = trade.signal
            if not signal:
                self.pending_entries.remove(trade)
                continue

            try:
                order = await self.client.get_order(signal.symbol, trade.entry_order_id)
                status = order.get("status", "").lower()
                filled_amount = float(
                    order.get("filled")
                    or order.get("info", {}).get("executedQty")
                    or 0
                )

                if status == "closed":  # Filled
                    await self._promote_filled_entry(trade, order, "FILLED")

                elif status in ("canceled", "cancelled", "expired", "rejected"):
                    if filled_amount > 0:
                        await self._promote_filled_entry(trade, order, f"PARTIAL_{status.upper()}")
                        continue

                    self.pending_entries.remove(trade)
                    trade.status = "CANCELLED"
                    trade.close_reason = status.upper()
                    self.db.save_trade(trade)
                    logger.info("Entry {} for {} {}", status, signal.direction, signal.symbol)

                else:
                    if self._is_dca_trade(trade):
                        # DCA limits are intentionally standing orders at the
                        # stored 0.618 level. They should live until filled,
                        # cancelled/rejected by exchange, or removed by the
                        # orphan-DCA guard when the parent position closes.
                        continue

                    # Check timeout
                    age_s = (time.time() * 1000 - trade.opened_at) / 1000
                    if age_s > entry_timeout:
                        await self.client.cancel_order(signal.symbol, trade.entry_order_id)
                        cancelled_order = await self.client.get_order(signal.symbol, trade.entry_order_id)
                        cancelled_filled = float(
                            cancelled_order.get("filled")
                            or cancelled_order.get("info", {}).get("executedQty")
                            or 0
                        )
                        if cancelled_filled > 0:
                            logger.warning(
                                "Entry timed out after partial fill ({:.0f}s): {} {} filled={}",
                                age_s, signal.direction, signal.symbol, cancelled_filled,
                            )
                            await self._promote_filled_entry(trade, cancelled_order, "PARTIAL_TIMEOUT")
                            continue

                        self.pending_entries.remove(trade)
                        trade.status = "CANCELLED"
                        trade.close_reason = "TIMEOUT"
                        self.db.save_trade(trade)
                        logger.info(
                            "Entry timed out ({:.0f}s): {} {}",
                            age_s, signal.direction, signal.symbol,
                        )

            except Exception as e:
                logger.error("Error checking pending entry {}: {}", trade.entry_order_id, e)

    def _find_dca_parent(self, signal: Signal) -> PositionState | None:
        """Return the existing 0.5 Fib position that this 0.618 signal should DCA."""
        cfg = self.config.get("strategy", "fibonacci", default={})
        if not cfg.get("dca_enabled", False):
            return None

        new_meta = signal.metadata or {}
        new_zone = new_meta.get("fib_zone", {}) or {}
        new_swing = new_meta.get("swing", {}) or {}
        try:
            new_level = round(float(new_zone.get("level", -1)), 3)
            to_level = round(float(cfg.get("dca_to_level", 0.618)), 3)
            from_level = round(float(cfg.get("dca_from_level", 0.5)), 3)
            max_legs = int(cfg.get("dca_max_legs", 2) or 2)
        except Exception:
            return None
        if new_level != to_level:
            return None

        for pos in self.open_positions:
            old = pos.trade.signal
            if not old or old.symbol != signal.symbol or old.direction != signal.direction:
                continue
            old_meta = old.metadata or {}
            old_zone = old_meta.get("fib_zone", {}) or {}
            old_swing = old_meta.get("swing", {}) or {}
            try:
                old_level = round(float(old_zone.get("level", -1)), 3)
                old_legs = int(old_meta.get("dca_legs", 1) or 1)
            except Exception:
                continue
            if old_level != from_level or old_legs >= max_legs:
                continue
            if self._same_fib_swing(new_swing, old_swing):
                return pos
        return None

    async def _merge_dca_fill(
        self,
        parent: PositionState,
        dca_trade: Trade,
        order: dict,
        exchange_pos: dict,
        reason: str,
    ) -> None:
        """Merge a filled 0.618 DCA leg into the existing exchange position.

        Binance runs this account as a net position. After DCA fills, replace the
        old protection with one combined SL/TP sized to the total position.
        """
        parent_trade = parent.trade
        signal = dca_trade.signal
        if not signal or not parent_trade.signal:
            return

        sl_side = "sell" if signal.direction == "LONG" else "buy"
        position_amount = abs(float(exchange_pos.get("contracts") or 0))
        avg_entry = float(
            exchange_pos.get("entryPrice")
            or exchange_pos.get("entry_price")
            or exchange_pos.get("info", {}).get("entryPrice")
            or dca_trade.entry_fill_price
            or order.get("average", 0)
            or order.get("price", 0)
        )
        if position_amount <= 0 or avg_entry <= 0:
            raise RuntimeError(f"Invalid DCA merge details for {signal.symbol}: amount={position_amount}, avg={avg_entry}")

        # Remove old reduce-only protection before placing combined protection.
        for order_id in (parent_trade.stop_order_id, parent_trade.tp_order_id):
            if order_id:
                await self.client.cancel_order(signal.symbol, order_id)

        old_meta = parent_trade.signal.metadata or {}
        old_legs = int(old_meta.get("dca_legs", 1) or 1)
        new_legs = old_legs + 1
        total_notional = position_amount * avg_entry
        stop, tp = self._combined_fixed_pnl_exits(avg_entry, total_notional, signal.direction, new_legs)
        signal.stop_loss = stop
        signal.take_profit = tp
        signal.metadata["dca_legs"] = new_legs
        signal.metadata["dca_parent_trade_id"] = parent_trade.id
        signal.metadata["dca_merged_trade_id"] = dca_trade.id

        amount = self.client.format_amount(signal.symbol, position_amount)
        sl_price = self.client.format_price(signal.symbol, stop)
        tp_price = self.client.format_price(signal.symbol, tp)
        sl_id = await self.client.place_stop_loss(signal.symbol, sl_side, amount, sl_price)
        tp_id = await self.client.place_take_profit(signal.symbol, sl_side, amount, tp_price)

        parent_trade.signal = signal
        parent_trade.entry_order_id = ",".join(x for x in [parent_trade.entry_order_id, dca_trade.entry_order_id] if x)
        parent_trade.entry_fill_price = avg_entry
        parent_trade.position_size = round(total_notional, 2)
        parent_trade.margin_used = round(total_notional / max(parent_trade.leverage, 1), 2)
        parent_trade.stop_order_id = sl_id
        parent_trade.tp_order_id = tp_id
        parent.trailing_stop = 0.0

        if dca_trade in self.pending_entries:
            self.pending_entries.remove(dca_trade)
        dca_trade.status = "CLOSED"
        dca_trade.close_reason = "DCA_MERGED"
        dca_trade.closed_at = int(time.time() * 1000)

        self.db.save_trade(dca_trade)
        self.db.save_trade(parent_trade)
        logger.info(
            "✅ DCA merged: {} {} legs={} avg=${:.4f} size=${:.2f} SL=${:.4f} TP=${:.4f}",
            signal.direction, signal.symbol, new_legs, avg_entry, total_notional, sl_price, tp_price,
        )

    def _combined_fixed_pnl_exits(
        self,
        avg_entry: float,
        total_notional: float,
        direction: str,
        legs: int,
    ) -> tuple[float, float]:
        """Return combined SL/TP preserving fixed PnL per DCA leg."""
        cfg = self.config.get("strategy", "fibonacci", default={})
        # DCA should cap total loss for the merged net position, not multiply
        # risk by legs. Without explicit DCA caps, keep the old per-leg behavior.
        if cfg.get("dca_max_total_loss_usdt") is not None:
            risk_usdt = float(cfg.get("dca_max_total_loss_usdt", 10.0))
            reward_usdt = float(cfg.get("dca_reward_usdt", risk_usdt * 1.5))
        else:
            risk_usdt = float(cfg.get("stop_loss_usdt", 5.0)) * legs
            reward_usdt = float(cfg.get("take_profit_usdt", risk_usdt)) * legs
        if total_notional <= 0:
            return avg_entry, avg_entry
        stop_pct = risk_usdt / total_notional
        tp_pct = reward_usdt / total_notional
        if direction == "LONG":
            return avg_entry * (1 - stop_pct), avg_entry * (1 + tp_pct)
        return avg_entry * (1 + stop_pct), avg_entry * (1 - tp_pct)

    def _fixed_pnl_exits_from_fill(
        self,
        fill_price: float,
        total_notional: float,
        direction: str,
    ) -> tuple[float, float]:
        """Return SL/TP from actual fill so fixed-PnL risk is exact.

        Signal generation estimates fixed-PnL exits from the intended limit
        entry. Limit fills can improve/slip versus that intended price, so
        placing protection from the signal price can make realized max loss
        smaller/larger than configured. Recalculate after fill using the real
        exchange position notional.
        """
        cfg = self.config.get("strategy", "fibonacci", default={})
        risk_usdt = float(cfg.get("stop_loss_usdt", 5.0))
        reward_usdt = float(cfg.get("take_profit_usdt", risk_usdt))
        if fill_price <= 0 or total_notional <= 0:
            return fill_price, fill_price
        stop_pct = risk_usdt / total_notional
        tp_pct = reward_usdt / total_notional
        if direction == "LONG":
            return fill_price * (1 - stop_pct), fill_price * (1 + tp_pct)
        return fill_price * (1 + stop_pct), fill_price * (1 - tp_pct)

    async def _promote_filled_entry(self, trade: Trade, order: dict, reason: str) -> None:
        """Promote a filled/partially-filled entry to OPEN and place protection.

        Binance can partially fill a limit entry and later leave it open until
        timeout. Cancelling the remainder does not remove the already-filled
        position, so any positive fill must become a managed protected trade.
        """
        signal = trade.signal
        if not signal:
            if trade in self.pending_entries:
                self.pending_entries.remove(trade)
            return

        fill_price = float(order.get("average", 0) or order.get("price", 0))
        filled_amount = float(
            order.get("filled")
            or order.get("amount")
            or order.get("info", {}).get("executedQty")
            or 0
        )
        if fill_price <= 0 or filled_amount <= 0:
            raise RuntimeError(
                f"Invalid fill details for {signal.symbol}: price={fill_price}, amount={filled_amount}"
            )

        trade.entry_fill_price = fill_price
        trade.status = "OPEN"
        sl_side = "sell" if signal.direction == "LONG" else "buy"
        expected_side = "long" if signal.direction == "LONG" else "short"

        exchange_pos = await self.client.get_position(signal.symbol)
        if not exchange_pos or (exchange_pos.get("side") or "").lower() != expected_side:
            logger.critical(
                "Entry {} but no matching exchange position for {}. Emergency closing reduce-only.",
                reason, signal.symbol,
            )
            try:
                await self.client.close_position_market(signal.symbol, sl_side, filled_amount)
            except Exception as close_error:
                logger.error("Emergency close failed for {}: {}", signal.symbol, close_error)
            if trade in self.pending_entries:
                self.pending_entries.remove(trade)
            trade.status = "CLOSED"
            trade.exit_fill_price = fill_price
            trade.closed_at = int(time.time() * 1000)
            trade.close_reason = "POSITION_VERIFY_FAILED"
            self.db.save_trade(trade)
            await self.notifier.error_alert(
                f"Entry {reason} but position verify failed for {signal.symbol}; emergency close attempted"
            )
            return

        position_amount = abs(float(exchange_pos.get("contracts") or filled_amount))
        amount = self.client.format_amount(signal.symbol, position_amount)

        dca_parent = self._find_dca_parent(signal)
        if dca_parent is not None:
            await self._merge_dca_fill(dca_parent, trade, order, exchange_pos, reason)
            return

        actual_notional = abs(float(exchange_pos.get("notional") or 0))
        if actual_notional <= 0:
            actual_notional = position_amount * fill_price
        if self.config.get("strategy", "fibonacci", "exit_mode", default="fixed_pnl") == "fixed_pnl":
            stop_loss, take_profit = self._fixed_pnl_exits_from_fill(
                fill_price, actual_notional, signal.direction,
            )
            signal.stop_loss = stop_loss
            signal.take_profit = take_profit
            trade.stop_loss = stop_loss
            trade.take_profit = take_profit

        sl_price = self.client.format_price(signal.symbol, signal.stop_loss)
        tp_price = self.client.format_price(signal.symbol, signal.take_profit)

        sl_id = ""
        tp_id = ""
        try:
            sl_id = await self.client.place_stop_loss(signal.symbol, sl_side, amount, sl_price)
            tp_id = await self.client.place_take_profit(signal.symbol, sl_side, amount, tp_price)
        except Exception as protective_error:
            logger.critical(
                "Protective order failure for {} after entry {}: {}. Emergency closing.",
                signal.symbol, reason, protective_error,
            )
            if sl_id:
                await self.client.cancel_order(signal.symbol, sl_id)
            if tp_id:
                await self.client.cancel_order(signal.symbol, tp_id)

            latest_pos = await self.client.get_position(signal.symbol)
            if latest_pos and (latest_pos.get("side") or "").lower() == expected_side:
                close_amount = abs(float(latest_pos.get("contracts") or position_amount))
                await self.client.close_position_market(signal.symbol, sl_side, close_amount)

            if trade in self.pending_entries:
                self.pending_entries.remove(trade)
            trade.status = "CLOSED"
            trade.exit_fill_price = fill_price
            trade.closed_at = int(time.time() * 1000)
            trade.close_reason = "PROTECTIVE_ORDER_FAILED"
            self.db.save_trade(trade)
            await self.notifier.error_alert(
                f"Emergency closed {signal.symbol}: protective order failed — {protective_error}"
            )
            return

        trade.stop_order_id = sl_id
        trade.tp_order_id = tp_id

        if trade in self.pending_entries:
            self.pending_entries.remove(trade)
        self.risk_manager.add_open_position(trade)
        self.open_positions.append(PositionState(trade=trade))
        self.db.save_trade(trade)

        await self.notifier.position_opened(trade)
        if reason.startswith("PARTIAL"):
            await self.notifier.error_alert(
                f"Partial entry managed: {signal.symbol} filled {position_amount}; cancelled remainder and placed SL/TP"
            )
        logger.info(
            "✅ {} {} {} @ ${:.4f} amount={} — SL/TP placed",
            signal.direction, signal.symbol, reason, fill_price, position_amount,
        )

    async def _close_position(self, pos: PositionState, reason: str) -> None:
        """Close a position at market."""
        trade = pos.trade
        signal = trade.signal
        if not signal:
            return

        try:
            # Verify and close the live position first. Protective orders are
            # cancelled after the reduce-only market close, so a failed/manual
            # close attempt does not leave the position naked.
            close_side = "sell" if signal.direction == "LONG" else "buy"
            exchange_pos = await self.client.get_position(signal.symbol)
            if not exchange_pos:
                logger.warning("No exchange position left to close for {}", signal.symbol)
                return
            actual_side = (exchange_pos.get("side") or "").lower()
            expected_side = "long" if signal.direction == "LONG" else "short"
            if actual_side != expected_side:
                logger.error(
                    "Refusing to close {}: expected {} position, exchange has {}",
                    signal.symbol, expected_side, actual_side,
                )
                return
            amount = abs(float(exchange_pos.get("contracts") or 0))
            await self.client.close_position_market(signal.symbol, close_side, amount)

            # Cancel leftover SL/TP after successful market close.
            if trade.stop_order_id:
                await self.client.cancel_order(signal.symbol, trade.stop_order_id)
            if trade.tp_order_id:
                await self.client.cancel_order(signal.symbol, trade.tp_order_id)

            # Update trade
            trade.status = "CLOSED"
            trade.exit_fill_price = pos.current_price
            trade.pnl = pos.unrealized_pnl
            trade.fees = trade.position_size * 0.0004 * 2  # Estimate
            trade.net_pnl = trade.pnl - trade.fees
            trade.closed_at = int(time.time() * 1000)
            trade.close_reason = reason

            self.db.save_trade(trade)
            self.risk_manager.remove_open_position(trade.id)
            self.risk_manager.record_trade_result(trade)
            self.open_positions.remove(pos)

            await self.notifier.position_closed(trade, reason)

            logger.info(
                "Position closed: {} {} — P&L: ${:.2f} ({})",
                signal.direction, signal.symbol, trade.net_pnl, reason,
            )

        except Exception as e:
            logger.error("Failed to close {}: {}", signal.symbol, e)

    # ── Recovery ───────────────────────────────────────────

    async def _recover_positions(self) -> None:
        """Recover open positions after restart — reconstruct full state."""
        try:
            positions = await self.client.get_positions()
            await self._cancel_orphan_entry_orders()
            if not positions:
                logger.info("No open positions to recover")
                return

            logger.info("Recovering {} open positions...", len(positions))
            db_open = {}
            try:
                for row in self.db.get_open_trades():
                    if row.get("status") != "OPEN":
                        continue
                    key = (row.get("symbol"), row.get("direction"))
                    if key not in db_open or int(row.get("opened_at") or 0) > int(db_open[key].get("opened_at") or 0):
                        db_open[key] = row
            except Exception as e:
                logger.warning("Could not load DB metadata for recovery: {}", e)

            for p in positions:
                try:
                    symbol = p.get("symbol") or ""
                    side = (p.get("side") or "").lower()
                    contracts = abs(float(p.get("contracts") or 0))
                    entry_price = float(p.get("entryPrice") or p.get("entry_price") or 0)
                    notional = abs(float(p.get("notional") or 0))
                    leverage = int(float(p.get("leverage") or self.config.base_leverage))
                except (TypeError, ValueError) as e:
                    logger.warning("Skipping malformed recovered position: {} ({})", p, e)
                    continue

                if contracts <= 0 or not symbol or entry_price <= 0:
                    continue

                direction = "LONG" if side == "long" else "SHORT"
                db_trade = db_open.get((symbol, direction), {})
                try:
                    recovered_meta = json.loads(db_trade.get("metadata") or "{}") if db_trade else {}
                except Exception:
                    recovered_meta = {}

                # Build minimal signal for recovered position
                signal = Signal(
                    symbol=symbol,
                    direction=direction,
                    entry_price=entry_price,
                    stop_loss=0.0,
                    take_profit=0.0,
                    confluence_score=0,
                    quality="RECOVERED",
                    regime="UNKNOWN",
                    timestamp=int(time.time() * 1000),
                    metadata=recovered_meta,
                )

                trade = Trade(
                    signal=signal,
                    status="OPEN",
                    entry_fill_price=entry_price,
                    position_size=notional if notional > 0 else contracts * entry_price,
                    leverage=leverage,
                    opened_at=int(db_trade.get("opened_at") or int(time.time() * 1000)) if db_trade else int(time.time() * 1000),
                )

                # Try to find existing SL/TP orders on exchange
                try:
                    open_orders = await self.client.get_open_orders(symbol)
                    for order in open_orders:
                        otype = order.get("type", "").lower()
                        if "stop" in otype and "profit" not in otype:
                            trade.stop_order_id = str(order.get("id", ""))
                            signal.stop_loss = float(order.get("stopPrice", 0) or 0)
                        elif "profit" in otype:
                            trade.tp_order_id = str(order.get("id", ""))
                            signal.take_profit = float(order.get("stopPrice", 0) or 0)
                except Exception as e:
                    logger.warning("Could not fetch open orders for {}: {}", symbol, e)
                    if db_trade:
                        trade.stop_order_id = str(db_trade.get("stop_order_id") or "")
                        trade.tp_order_id = str(db_trade.get("tp_order_id") or "")
                        signal.stop_loss = float(db_trade.get("stop_loss") or 0)
                        signal.take_profit = float(db_trade.get("take_profit") or 0)
                        if trade.stop_order_id or trade.tp_order_id:
                            logger.warning(
                                "Recovered {} {} protection from DB while exchange orders are unavailable",
                                direction, symbol,
                            )

                if not trade.stop_order_id and not trade.tp_order_id:
                    logger.warning(
                        "Skipping unmanaged exchange position: {} {} has no bot SL/TP orders",
                        direction, symbol,
                    )
                    continue

                pos = PositionState(trade=trade)
                self.open_positions.append(pos)
                self.risk_manager.add_open_position(trade)

                logger.info(
                    "  Recovered: {} {} — ${:.2f} @ ${:.4f} SL={} TP={}",
                    direction, symbol, trade.position_size, entry_price,
                    f"${signal.stop_loss:.4f}" if signal.stop_loss else "none",
                    f"${signal.take_profit:.4f}" if signal.take_profit else "none",
                )

            logger.info("Recovery complete: {} positions restored", len(self.open_positions))

        except Exception as e:
            logger.warning("Position recovery failed: {}", e)
            if self.config.market_data_mode == "websocket" and self.client.is_rate_limited():
                self._recover_positions_from_db_fallback()

    def _recover_positions_from_db_fallback(self) -> None:
        """Recover DB-known open trades when Binance REST is temporarily banned.

        This is a temporary safety state for websocket monitoring/backoff periods.
        Exchange reconciliation will correct it after REST becomes available.
        """
        if self.open_positions:
            return
        recovered = 0
        try:
            rows = [row for row in self.db.get_open_trades() if row.get("status") == "OPEN"]
            rows.sort(key=lambda r: int(r.get("opened_at") or 0), reverse=True)
            if len(rows) > 1:
                logger.warning(
                    "DB fallback found {} OPEN trades; recovering latest only until exchange confirms state",
                    len(rows),
                )

            for row in rows[:1]:
                if row.get("status") != "OPEN":
                    continue
                try:
                    metadata = json.loads(row.get("metadata") or "{}")
                except Exception:
                    metadata = {}

                signal = Signal(
                    symbol=str(row.get("symbol") or ""),
                    direction=str(row.get("direction") or "LONG"),
                    entry_price=float(row.get("entry_price") or 0),
                    stop_loss=float(row.get("stop_loss") or 0),
                    take_profit=float(row.get("take_profit") or 0),
                    confluence_score=int(row.get("confluence_score") or 0),
                    quality=str(row.get("quality") or "RECOVERED"),
                    regime=str(row.get("regime") or "UNKNOWN"),
                    timestamp=int(time.time() * 1000),
                    metadata=metadata,
                )
                if not signal.symbol or signal.entry_price <= 0:
                    continue

                trade = Trade(
                    id=str(row.get("id") or ""),
                    signal=signal,
                    status="OPEN",
                    entry_order_id=str(row.get("entry_order_id") or ""),
                    stop_order_id=str(row.get("stop_order_id") or ""),
                    tp_order_id=str(row.get("tp_order_id") or ""),
                    entry_fill_price=signal.entry_price,
                    position_size=float(row.get("position_size") or 0),
                    margin_used=float(row.get("margin_used") or 0),
                    leverage=int(row.get("leverage") or self.config.base_leverage),
                    opened_at=int(row.get("opened_at") or int(time.time() * 1000)),
                )
                self.open_positions.append(PositionState(trade=trade))
                self.risk_manager.add_open_position(trade)
                recovered += 1
        except Exception as db_e:
            logger.warning("DB fallback recovery failed: {}", db_e)
            return

        if recovered:
            logger.warning("Recovered {} DB open trades during Binance backoff", recovered)

    async def _cancel_orphan_entry_orders(self) -> None:
        """Cancel regular non-reduce-only entry orders left across restarts.

        Open positions can be recovered because they have SL/TP context. A naked
        unfilled entry order after restart is unsafe: if it fills later, the new
        process does not know to place protection. Cancel these on startup.
        """
        try:
            open_orders = await self.client.get_open_orders()
        except Exception as e:
            logger.warning("Could not inspect open orders during recovery: {}", e)
            return

        for order in open_orders:
            otype = str(order.get("type", "")).lower()
            reduce_only = bool(order.get("reduceOnly") or order.get("info", {}).get("reduceOnly"))
            if reduce_only or otype not in ("limit", "market"):
                continue
            symbol = order.get("symbol") or ""
            order_id = str(order.get("id") or "")
            if not symbol or not order_id:
                continue
            try:
                await self.client.cancel_order(symbol, order_id)
                logger.warning("Cancelled orphan entry order on startup: {} {}", symbol, order_id)
            except Exception as e:
                logger.error("Failed to cancel orphan entry order {} {}: {}", symbol, order_id, e)

    # ── Timing ─────────────────────────────────────────────

    async def _wait_for_candle_close(self, interval_minutes: int = 5) -> None:
        """
        Sleep until next 5-minute candle close.

        Adds a 2-second buffer for API data availability.
        """
        now = datetime.now(timezone.utc)
        minutes_past = now.minute % interval_minutes
        seconds_to_close = (
            (interval_minutes - minutes_past) * 60
            - now.second
            - now.microsecond / 1_000_000
        )

        if seconds_to_close <= 0:
            seconds_to_close += interval_minutes * 60

        wait_time = seconds_to_close + 2.0
        next_close = now.strftime("%H:%M:%S")
        logger.debug("Waiting {:.0f}s for next candle close...", wait_time)

        await self._sleep_with_shutdown(wait_time)

    # ── Heartbeat ──────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """Send hourly heartbeat without touching Binance during backoff."""
        while self.is_running:
            try:
                now = time.time()
                if now - self._last_heartbeat >= self._heartbeat_interval:
                    if self.client.is_rate_limited():
                        logger.warning("Heartbeat skipped during Binance rate-limit backoff")
                    else:
                        balance = await self.client.get_balance()
                        await self.notifier.heartbeat(
                            balance, len(self.open_positions),
                        )
                        self._last_heartbeat = now

                        # Update balance tracking
                        self.risk_manager.update_balance(balance)
            except Exception as e:
                logger.error("Heartbeat error: {}", e)

            await self._sleep_with_shutdown(60)  # Check every minute
