"""
Telegram notification system.

Sends trade alerts, daily summaries, circuit breaker warnings,
and heartbeat messages to the configured Telegram chat.
"""

from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Any

from loguru import logger

from src.core.config import Config
from src.data.models import PositionState, Signal, Trade


class TelegramNotifier:
    """Sends formatted messages to Telegram."""

    def __init__(self, config: Config) -> None:
        self._cfg = config
        self._enabled = config.telegram_enabled
        self._bot = None

    async def initialize(self) -> None:
        """Initialize Telegram bot if enabled."""
        if not self._enabled:
            logger.info("Telegram notifications disabled")
            return

        token = self._cfg.telegram_bot_token
        if not token:
            logger.warning("Telegram token not set — disabling notifications")
            self._enabled = False
            return

        try:
            from telegram import Bot
            self._bot = Bot(token=token)
            me = await self._bot.get_me()
            logger.info("Telegram bot connected: @{}", me.username)
        except Exception as e:
            logger.error("Failed to initialize Telegram: {}", e)
            self._enabled = False

    async def _send(self, text: str) -> None:
        """Send a message to the configured chat."""
        if not self._enabled or not self._bot:
            return
        try:
            chat_id = self._cfg.telegram_chat_id
            await self._bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error("Telegram send failed: {}", e)

    # ── Signal Messages ────────────────────────────────────

    def _fib_level_info(self, signal: Signal) -> str:
        """Build Fibonacci entry-level details for notifications."""
        if signal.metadata.get("strategy") != "FIB":
            return ""

        zone = signal.metadata.get("fib_zone") or {}
        swing = signal.metadata.get("swing") or {}
        if not zone:
            return ""

        level = float(zone.get("level") or 0)
        level_pct = level * 100
        zone_low = float(zone.get("low") or 0)
        zone_high = float(zone.get("high") or 0)
        swing_start = float(swing.get("start") or 0)
        swing_end = float(swing.get("end") or 0)
        swing_low = min(swing_start, swing_end) if swing_start and swing_end else 0
        swing_high = max(swing_start, swing_end) if swing_start and swing_end else 0
        impulse_pct = float(swing.get("impulse_pct") or 0)
        impulse_atr = float(swing.get("impulse_atr") or 0)

        lines = [
            "🧮 Fib Entry:",
            f"  Mode:  {signal.metadata.get('mode', 'n/a')}",
            f"  Level: {zone.get('name', 'n/a')} ({level_pct:.1f}%)",
            f"  Zone:  ${zone_low:,.6f} - ${zone_high:,.6f}",
        ]
        if swing_low and swing_high:
            lines.extend([
                f"  High:  ${swing_high:,.6f}",
                f"  Low:   ${swing_low:,.6f}",
                f"  Swing: ${swing_start:,.6f} → ${swing_end:,.6f} "
                f"({impulse_pct:.2f}%, {impulse_atr:.1f} ATR)",
            ])
        return "\n".join(lines) + "\n\n"

    def _score_indicator_info(self, signal: Signal) -> str:
        """Build full score + indicator details for signal notifications."""
        scores = signal.metadata.get("layer_scores", {}) or {}
        indicators = signal.metadata.get("indicators", {}) or {}
        if not indicators:
            indicators = {
                "rsi": signal.metadata.get("rsi", 0),
                "adx": signal.metadata.get("adx", 0),
                "atr": signal.metadata.get("atr", 0),
                "volume_ratio": signal.metadata.get("volume_ratio", 0),
            }

        rating = escape(str(signal.quality or "n/a"))
        score_pct = (signal.confluence_score / 13 * 100) if signal.confluence_score else 0
        strategy_name = escape(str(signal.metadata.get("strategy", "n/a")))
        strategy_mode = escape(str(signal.metadata.get("mode", "n/a")))
        score_lines = [
            "📊 Score / Rating:",
            f"  Strategy:    {strategy_name} / {strategy_mode}",
            f"  Score:       {signal.confluence_score}/13 ({score_pct:.0f}%)",
            f"  Rating:      {rating}",
            "",
            "🧩 Strategy Scores:",
            f"  {strategy_name:<12} {signal.confluence_score}/13 — {rating}",
        ]
        preferred = [
            "regime", "trend", "fib", "level", "early_touch",
            "rejection", "divergence", "volume", "candle",
        ]
        max_scores = {
            "regime": 2,
            "trend": 2,
            "fib": 3,
            "level": 3,
            "early_touch": 1,
            "rejection": 2,
            "divergence": 3,
            "volume": 2,
            "candle": 1,
        }
        seen = set()
        for key in preferred:
            if key in scores:
                label = key.replace("_", " ").title()
                value = scores.get(key)
                max_value = max_scores.get(key)
                suffix = f"/{max_value}" if max_value is not None else ""
                score_lines.append(f"  {label:<12} {value}{suffix}")
                seen.add(key)
        for key, value in scores.items():
            if key not in seen:
                label = str(key).replace("_", " ").title()
                max_value = max_scores.get(str(key))
                suffix = f"/{max_value}" if max_value is not None else ""
                score_lines.append(f"  {label:<12} {value}{suffix}")

        def num(name: str, default: float = 0.0) -> float:
            try:
                return float(indicators.get(name, default) or 0)
            except Exception:
                return default

        near_levels = indicators.get("near_levels") or []
        if isinstance(near_levels, list):
            near = ", ".join(
                escape(str(level.get("name", level))) if isinstance(level, dict) else escape(str(level))
                for level in near_levels[:4]
            ) or "none"
        else:
            near = escape(str(near_levels))

        indicator_lines = [
            "📈 Indicators:",
            f"  Price:       ${num('current_price'):,.6f}",
            f"  RSI:         {num('rsi'):.1f}",
            f"  ADX:         {num('adx'):.1f}",
            f"  ATR:         {num('atr'):,.6f} ({num('atr_pct'):.2f}%)",
            f"  BB Width %:  {num('bb_width_percentile'):.1f}",
            f"  EMA50 5m:    ${num('ema_50_5m'):,.6f}",
            f"  EMA50 15m:   ${num('ema_50_15m'):,.6f}",
            f"  EMA200 1h:   ${num('ema_200_1h'):,.6f}",
            f"  VWAP:        ${num('vwap'):,.6f}",
            f"  Trend Bias:  {escape(str(indicators.get('trend_bias', 'n/a')))}",
            f"  Volume:      {num('current_volume'):,.0f} / SMA20 {num('volume_sma_20'):,.0f} ({num('volume_ratio'):.2f}x)",
            f"  Divergence:  {escape(str(indicators.get('divergence_type', 'NONE')))} ({num('divergence_strength'):.2f})",
            f"  Candle:      {escape(str(indicators.get('candle_pattern', 'NONE')))}",
            f"  Prev H/L:    ${num('prev_session_high'):,.6f} / ${num('prev_session_low'):,.6f}",
            f"  Near Levels: {near}",
        ]
        return "\n".join(score_lines + [""] + indicator_lines) + "\n"

    async def signal_detected(self, signal: Signal) -> None:
        """Notify about a new signal."""
        fib_info = self._fib_level_info(signal)
        details = self._score_indicator_info(signal)
        msg = (
            f"🔍 <b>Signal Detected</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>{signal.direction}</b> {signal.symbol}\n"
            f"Score/Rating: <b>{signal.confluence_score}/13 — {escape(str(signal.quality))}</b>\n"
            f"Regime: {signal.regime}\n"
            f"\n"
            f"{fib_info}"
            f"{details}"
        )
        await self._send(msg)

    async def position_opened(self, trade: Trade) -> None:
        """Notify about a new position."""
        signal = trade.signal
        if not signal:
            return

        direction_emoji = "📈" if signal.direction == "LONG" else "📉"
        fib_info = self._fib_level_info(signal)
        details = self._score_indicator_info(signal)
        msg = (
            f"{direction_emoji} <b>Position Opened</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>{signal.direction}</b> {signal.symbol}\n"
            f"Score/Rating: <b>{signal.confluence_score}/13 — {escape(str(signal.quality))}</b>\n"
            f"Entry: ${trade.entry_fill_price:,.4f}\n"
            f"SL:    ${signal.stop_loss:,.4f}\n"
            f"TP:    ${signal.take_profit:,.4f}\n"
            f"\n"
            f"{fib_info}"
            f"{details}\n"
            f"Size:     ${trade.position_size:,.2f}\n"
            f"Margin:   ${trade.margin_used:,.2f}\n"
            f"Leverage: {trade.leverage}x"
        )
        await self._send(msg)

    async def position_closed(self, trade: Trade, reason: str = "") -> None:
        """Notify about a closed position."""
        signal = trade.signal
        if not signal:
            return

        if trade.net_pnl >= 0:
            emoji = "✅"
            pnl_str = f"+${trade.net_pnl:.2f}"
        else:
            emoji = "❌"
            pnl_str = f"-${abs(trade.net_pnl):.2f}"

        close_reason = trade.close_reason or reason
        msg = (
            f"{emoji} <b>Position Closed</b> — {close_reason}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{signal.direction} {signal.symbol}\n"
            f"Entry:  ${trade.entry_fill_price:,.4f}\n"
            f"Exit:   ${trade.exit_fill_price:,.4f}\n"
            f"P&L:    <b>{pnl_str}</b> (fees: ${trade.fees:.4f})\n"
            f"Reason: {close_reason}"
        )
        await self._send(msg)

    async def stop_updated(self, symbol: str, new_stop: float, reason: str) -> None:
        """Notify about stop loss update."""
        emoji = "🔄" if reason == "BREAKEVEN" else "📊"
        msg = (
            f"{emoji} <b>Stop Updated</b> — {reason}\n"
            f"{symbol} → SL: ${new_stop:,.4f}"
        )
        await self._send(msg)

    # ── Scanner Messages ───────────────────────────────────

    async def coin_rotation(
        self,
        added: set[str],
        removed: set[str],
        active: list[str],
    ) -> None:
        """Notify about coin lineup change."""
        msg = (
            f"🔄 <b>Coin Rotation</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
        )
        if added:
            msg += f"➕ Added: {', '.join(added)}\n"
        if removed:
            msg += f"➖ Removed: {', '.join(removed)}\n"
        msg += f"\n📋 Active ({len(active)}): {', '.join(active[:10])}"
        if len(active) > 10:
            msg += f" +{len(active) - 10} more"
        await self._send(msg)

    # ── Circuit Breaker Messages ───────────────────────────

    async def circuit_breaker(self, reason: str) -> None:
        """Notify about circuit breaker trigger."""
        msg = (
            f"🛑 <b>CIRCUIT BREAKER</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"Trading PAUSED\n"
            f"Reason: {reason}\n"
            f"\n"
            f"⚠️ Manual review required before resuming."
        )
        await self._send(msg)

    # ── Daily Summary ──────────────────────────────────────

    async def daily_summary(self, stats: dict[str, Any]) -> None:
        """Send end-of-day summary."""
        pnl = stats.get("net_pnl", 0)
        pnl_emoji = "🟢" if pnl >= 0 else "🔴"
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"

        msg = (
            f"📋 <b>Daily Summary</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{pnl_emoji} Net P&L: <b>{pnl_str}</b>\n"
            f"Trades:   {stats.get('trades', 0)}\n"
            f"Wins:     {stats.get('wins', 0)}\n"
            f"Losses:   {stats.get('losses', 0)}\n"
            f"Win Rate: {stats.get('win_rate', 0):.0f}%\n"
            f"Fees:     ${stats.get('fees', 0):.4f}\n"
            f"\n"
            f"💰 Balance: ${stats.get('balance', 0):.2f}\n"
            f"📈 Peak:    ${stats.get('peak', 0):.2f}"
        )
        await self._send(msg)

    # ── System Messages ────────────────────────────────────

    async def heartbeat(self, balance: float, open_positions: int) -> None:
        """Hourly heartbeat status."""
        msg = (
            f"🔧 <b>Heartbeat</b> — "
            f"{datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
            f"Balance: ${balance:.2f} | "
            f"Open: {open_positions} positions"
        )
        await self._send(msg)

    async def bot_started(self, mode: str, balance: float, coins: int) -> None:
        """Bot startup notification."""
        msg = (
            f"🚀 <b>FIB Bot Started</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"Mode:    {mode.upper()}\n"
            f"Balance: ${balance:.2f}\n"
            f"Coins:   {coins} active\n"
            f"\n"
            f"Strategy: FIB (Fibonacci Reversal/Pullback)"
        )
        await self._send(msg)

    async def error_alert(self, error: str) -> None:
        """Send error notification."""
        msg = (
            f"⚠️ <b>Error</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<code>{error[:500]}</code>"
        )
        await self._send(msg)
