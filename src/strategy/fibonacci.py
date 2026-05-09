"""
Fibonacci strategy scorer.

Initial live mode is ``reversal``: fade exhaustion at Fibonacci extension
zones only after confirmation. The config also defines trend-pullback and
confluence modes so the bot can be switched without changing runtime code.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from src.core.config import Config
from src.data.models import Candle, IndicatorSet, Signal
from src.strategy.regime import classify_regime


@dataclass(frozen=True)
class Swing:
    direction: str  # UP or DOWN impulse
    start_price: float
    end_price: float
    start_idx: int
    end_idx: int
    atr_multiple: float
    pct: float
    start_ts: int = 0
    end_ts: int = 0

    @property
    def size(self) -> float:
        return abs(self.end_price - self.start_price)


@dataclass(frozen=True)
class FibZone:
    name: str
    low: float
    high: float
    level: float


class FibonacciScorer:
    """Generate Fibonacci reversal / pullback / confluence signals."""

    def __init__(self, config: Config) -> None:
        self._cfg = config

    def evaluate(
        self,
        ind: IndicatorSet,
        rsi_history: np.ndarray,
        price_history: np.ndarray,
        candles_5m: list[Candle],
        candles_15m: list[Candle],
        candles_1h: list[Candle],
    ) -> Signal | None:
        cfg = self._cfg.get("strategy", "fibonacci", default={})
        mode = cfg.get("mode", "reversal")

        regime_name, regime_score = classify_regime(
            ind.adx,
            ind.bb_width_percentile,
            self._cfg.regime_config.get("thresholds", {}),
        )
        if regime_name == "DEAD" and cfg.get("avoid_dead_regime", True):
            return None

        if mode == "trend_pullback":
            return self._evaluate_trend_pullback(
                ind, rsi_history, candles_5m, candles_15m, regime_name, regime_score, cfg,
            )
        if mode == "confluence":
            return self._evaluate_confluence(
                ind, rsi_history, candles_5m, candles_15m, regime_name, regime_score, cfg,
            )
        return self._evaluate_reversal(
            ind, rsi_history, candles_5m, candles_15m, regime_name, regime_score, cfg,
        )

    # ── Modes ──────────────────────────────────────────────

    def _evaluate_reversal(
        self,
        ind: IndicatorSet,
        rsi_history: np.ndarray,
        candles_5m: list[Candle],
        candles_15m: list[Candle],
        regime_name: str,
        regime_score: int,
        cfg: dict,
    ) -> Signal | None:
        swing = self._find_impulse_swing(candles_15m, ind.atr, cfg)
        if not swing or swing.size <= 0:
            return None

        current = ind.current_price
        direction = "SHORT" if swing.direction == "UP" else "LONG"
        zones = self._extension_zones(swing, cfg)
        zone = self._matching_zone(current, zones)
        if zone is None:
            return None

        rejection = self._rejection_score(candles_5m[-1], direction)
        divergence = self._rsi_divergence_score(rsi_history, candles_5m, direction, cfg)
        volume = 1 if ind.volume_ratio >= cfg.get("volume_ratio_min", 1.2) else 0
        extension_score = 3 if zone.level >= 1.618 else 2

        if cfg.get("require_confirmation", True) and rejection == 0 and divergence == 0:
            return None
        if cfg.get("require_volume", False) and volume == 0:
            return None

        total = extension_score + rejection + divergence + volume + regime_score
        if total < self._cfg.scoring_config.get("min_score", 5):
            return None

        entry, stop, tp = self._reversal_levels(current, swing, zone, direction, ind.atr, cfg)
        if entry <= 0 or stop <= 0 or tp <= 0:
            return None

        quality, size_mult = self._quality_tier(total)
        return self._signal(
            ind=ind,
            direction=direction,
            entry=entry,
            stop=stop,
            tp=tp,
            total=total,
            quality=quality,
            size_mult=size_mult,
            regime=regime_name,
            mode="reversal",
            swing=swing,
            zone=zone,
            scores={
                "fib": extension_score,
                "rejection": rejection,
                "divergence": divergence,
                "volume": volume,
                "regime": regime_score,
            },
        )

    def _evaluate_trend_pullback(
        self,
        ind: IndicatorSet,
        rsi_history: np.ndarray,
        candles_5m: list[Candle],
        candles_15m: list[Candle],
        regime_name: str,
        regime_score: int,
        cfg: dict,
    ) -> Signal | None:
        swing = self._find_impulse_swing(candles_15m, ind.atr, cfg)
        if not swing or swing.size <= 0:
            return None

        direction = "LONG" if swing.direction == "UP" else "SHORT"
        zones = self._retracement_zones(swing, cfg)
        zone = self._matching_zone(ind.current_price, zones)
        touch_count = 0
        entry_price = ind.current_price
        early_touch = False

        if zone is None and cfg.get("enter_on_zone_touch", True):
            zone = self._matching_zone_touch(candles_5m[-1], zones)
            if zone is not None:
                touch_count = self._zone_touch_count(candles_5m, zone, swing.end_ts)
                max_touches = int(cfg.get("max_entry_zone_touches", 2))
                if touch_count <= 0 or touch_count > max_touches:
                    return None
                early_touch = True
                entry_price = zone.high if direction == "LONG" else zone.low
        elif zone is not None:
            touch_count = self._zone_touch_count(candles_5m, zone, swing.end_ts)
            max_touches = int(cfg.get("max_entry_zone_touches", 2))
            early_touch = 0 < touch_count <= max_touches

        if zone is None:
            return None

        if cfg.get("trend_filter", True):
            strict_trend_ok = (
                ind.current_price >= ind.ema_50_15m
                if direction == "LONG"
                else ind.current_price <= ind.ema_50_15m
            )
            deep_trend_ok = False
            if zone.level >= cfg.get("deep_zone_trend_bypass_level", 0.618):
                bypass_pct = cfg.get("deep_zone_trend_bypass_pct", 0.6) / 100.0
                deep_trend_ok = (
                    ind.current_price >= ind.ema_50_15m * (1 - bypass_pct)
                    if direction == "LONG"
                    else ind.current_price <= ind.ema_50_15m * (1 + bypass_pct)
                )
            if not strict_trend_ok and not deep_trend_ok:
                return None

        rejection = self._rejection_score(candles_5m[-1], direction)
        volume = 1 if ind.volume_ratio >= cfg.get("volume_ratio_min", 1.2) else 0
        if cfg.get("require_confirmation", True) and rejection == 0 and volume == 0:
            return None

        fib_score = 3 if 0.5 <= zone.level <= 0.618 else 2
        early_touch_bonus = int(cfg.get("early_zone_touch_bonus", 1)) if early_touch else 0
        total = fib_score + rejection + volume + regime_score + early_touch_bonus
        q_filters = cfg.get("quality_filters", {}) or {}
        min_confirmation_score = int(q_filters.get("require_confirmation_below_score", 0) or 0)
        if min_confirmation_score > 0 and total < min_confirmation_score and rejection == 0 and volume == 0:
            return None
        if not self._passes_trend_pullback_quality_filters(ind, regime_name, early_touch, cfg):
            return None
        if total < self._cfg.scoring_config.get("min_score", 5):
            return None

        entry, stop, tp = self._pullback_levels(
            entry_price, swing, zone, direction, ind.atr, cfg,
        )
        if entry <= 0 or stop <= 0 or tp <= 0:
            return None
        quality, size_mult = self._quality_tier(total)
        return self._signal(
            ind, direction, entry, stop, tp, total, quality, size_mult, regime_name,
            "trend_pullback", swing, zone,
            {
                "fib": fib_score,
                "early_touch": early_touch_bonus,
                "rejection": rejection,
                "volume": volume,
                "regime": regime_score,
            },
        )

    def _evaluate_confluence(self, *args, **kwargs) -> Signal | None:
        # First version: confluence mode is trend-pullback plus the existing
        # confirmation gates. More layers can be added without touching runtime.
        return self._evaluate_trend_pullback(*args, **kwargs)

    # ── Fibonacci geometry ─────────────────────────────────

    def _find_impulse_swing(self, candles: list[Candle], atr: float, cfg: dict) -> Swing | None:
        lookback = min(len(candles), cfg.get("swing_lookback", 96))
        if lookback < 12:
            return None

        window = candles[-lookback:]
        lows = np.array([c.low for c in window])
        highs = np.array([c.high for c in window])

        low_idx = int(np.argmin(lows))
        high_idx = int(np.argmax(highs))
        low = float(lows[low_idx])
        high = float(highs[high_idx])
        if low <= 0 or high <= 0 or high == low:
            return None

        if low_idx < high_idx:
            direction = "UP"
            start, end = low, high
            start_idx, end_idx = low_idx, high_idx
        else:
            direction = "DOWN"
            start, end = high, low
            start_idx, end_idx = high_idx, low_idx

        size = abs(end - start)
        atr_multiple = size / atr if atr > 0 else 0.0
        pct = size / start * 100 if start > 0 else 0.0
        if atr_multiple < cfg.get("min_impulse_atr", 2.0):
            return None
        if pct < cfg.get("min_impulse_pct", 0.4):
            return None
        if end_idx < lookback - cfg.get("max_swing_age", 48):
            return None

        return Swing(
            direction, start, end, start_idx, end_idx, atr_multiple, pct,
            int(window[start_idx].timestamp), int(window[end_idx].timestamp),
        )

    def _extension_zones(self, swing: Swing, cfg: dict) -> list[FibZone]:
        tolerance = cfg.get("zone_tolerance_pct", 0.12) / 100.0
        zones = []
        for level in cfg.get("reversal_extensions", [1.272, 1.618]):
            price = self._fib_price(swing, level)
            zones.append(FibZone(f"ext_{level}", price * (1 - tolerance), price * (1 + tolerance), level))
        return zones

    def _retracement_zones(self, swing: Swing, cfg: dict) -> list[FibZone]:
        """Return pullback zones measured from impulse end back toward origin.

        This matches the common chart workflow: draw Fib from swing high to
        swing low for a bearish impulse and short the bounce at 0.382/0.5/0.618;
        inverse for bullish pullbacks.
        """
        tolerance = cfg.get("zone_tolerance_pct", 0.12) / 100.0
        zones = []
        for level in cfg.get("pullback_retracements", [0.382, 0.5, 0.618]):
            price = self._retracement_price(swing, level)
            zones.append(FibZone(f"ret_{level}", price * (1 - tolerance), price * (1 + tolerance), level))
        return zones

    def _retracement_price(self, swing: Swing, level: float) -> float:
        if swing.direction == "UP":
            return swing.end_price - swing.size * level
        return swing.end_price + swing.size * level

    def _fib_price(self, swing: Swing, level: float) -> float:
        if swing.direction == "UP":
            return swing.start_price + swing.size * level
        return swing.start_price - swing.size * level

    def _matching_zone(self, price: float, zones: list[FibZone]) -> FibZone | None:
        matches = [z for z in zones if z.low <= price <= z.high]
        if not matches:
            return None
        return sorted(matches, key=lambda z: abs(price - (z.low + z.high) / 2))[0]

    def _matching_zone_touch(self, candle: Candle, zones: list[FibZone]) -> FibZone | None:
        matches = [z for z in zones if candle.low <= z.high and candle.high >= z.low]
        if not matches:
            return None
        return sorted(matches, key=lambda z: abs(candle.close - (z.low + z.high) / 2))[0]

    def _zone_touch_count(self, candles: list[Candle], zone: FibZone, after_ts: int) -> int:
        touches = 0
        in_touch = False
        for candle in candles:
            if after_ts and candle.timestamp <= after_ts:
                continue
            touching = candle.low <= zone.high and candle.high >= zone.low
            if touching and not in_touch:
                touches += 1
            in_touch = touching
        return touches

    def _passes_trend_pullback_quality_filters(
        self,
        ind: IndicatorSet,
        regime_name: str,
        early_touch: bool,
        cfg: dict,
    ) -> bool:
        """Extra data-driven guardrails from live trade history analysis.

        Fib winners clustered around VOLATILE/SQUEEZE regimes, or otherwise
        decent trend strength with RSI in a healthier 40-70 band. Stale/retest
        entries underperformed, so require first/early zone touch when enabled.
        """
        filters = cfg.get("quality_filters", {}) or {}
        if not filters.get("enabled", False):
            return True

        if filters.get("require_early_touch", False) and not early_touch:
            return False

        strong_regimes = set(filters.get("strong_regimes", ["VOLATILE", "SQUEEZE"]))
        if regime_name in strong_regimes:
            return True

        min_adx = float(filters.get("min_adx", 20.0))
        rsi_min = float(filters.get("rsi_min", 40.0))
        rsi_max = float(filters.get("rsi_max", 70.0))
        return ind.adx >= min_adx and rsi_min <= ind.rsi <= rsi_max

    # ── Confirmation / levels ──────────────────────────────

    def _rejection_score(self, candle: Candle, direction: str) -> int:
        rng = candle.high - candle.low
        if rng <= 0:
            return 0
        body = abs(candle.close - candle.open)
        upper = candle.high - max(candle.open, candle.close)
        lower = min(candle.open, candle.close) - candle.low
        if direction == "LONG" and lower >= body * 1.5 and candle.close > candle.open:
            return 2
        if direction == "SHORT" and upper >= body * 1.5 and candle.close < candle.open:
            return 2
        return 0

    def _rsi_divergence_score(
        self,
        rsi: np.ndarray,
        candles: list[Candle],
        direction: str,
        cfg: dict,
    ) -> int:
        lookback = min(len(candles), len(rsi), cfg.get("divergence_lookback", 24))
        if lookback < 8:
            return 0
        closes = np.array([c.close for c in candles[-lookback:]])
        rsis = rsi[-lookback:]
        if np.any(np.isnan(rsis)):
            rsis = np.nan_to_num(rsis, nan=50.0)
        half = lookback // 2
        if direction == "LONG":
            return 2 if closes[-1] < np.min(closes[:half]) and rsis[-1] > np.min(rsis[:half]) else 0
        return 2 if closes[-1] > np.max(closes[:half]) and rsis[-1] < np.max(rsis[:half]) else 0

    def _reversal_levels(
        self,
        current: float,
        swing: Swing,
        zone: FibZone,
        direction: str,
        atr: float,
        cfg: dict,
    ) -> tuple[float, float, float]:
        buffer = max(atr * cfg.get("stop_atr_buffer", 0.35), current * cfg.get("min_stop_pct", 0.2) / 100.0)
        if direction == "SHORT":
            stop = max(zone.high, current) + buffer
            tp = self._fib_price(swing, cfg.get("reversal_take_profit_retracement", 0.618))
        else:
            stop = min(zone.low, current) - buffer
            tp = self._fib_price(swing, cfg.get("reversal_take_profit_retracement", 0.618))
        if direction == "SHORT" and tp >= current:
            tp = current - (stop - current) * cfg.get("fallback_rr", 1.5)
        if direction == "LONG" and tp <= current:
            tp = current + (current - stop) * cfg.get("fallback_rr", 1.5)
        return current, stop, tp

    def _pullback_levels(
        self,
        current: float,
        swing: Swing,
        zone: FibZone,
        direction: str,
        atr: float,
        cfg: dict,
    ) -> tuple[float, float, float]:
        """Return exits for Fib pullbacks.

        Default live mode is fixed PnL: with fixed margin + leverage, set SL/TP
        at equal dollar distance (e.g. -$5 / +$5).
        """
        if cfg.get("exit_mode", "fixed_pnl") == "fixed_pnl":
            margin = cfg.get("fixed_margin_usdt", 5.0)
            leverage = cfg.get("fixed_leverage", 50)
            notional = margin * leverage
            risk_usdt = cfg.get("stop_loss_usdt", 5.0)
            reward_usdt = cfg.get("take_profit_usdt", risk_usdt)
            if notional <= 0:
                return current, 0.0, 0.0
            stop_pct = risk_usdt / notional
            tp_pct = reward_usdt / notional
            if direction == "LONG":
                return current, current * (1 - stop_pct), current * (1 + tp_pct)
            return current, current * (1 + stop_pct), current * (1 - tp_pct)

        buffer = max(
            atr * cfg.get("stop_atr_buffer", 0.35),
            current * cfg.get("min_stop_pct", 0.2) / 100.0,
        )
        target_level = cfg.get("pullback_take_profit_retracement", 0.236)

        if direction == "LONG":
            stop_base = self._long_stop_base(swing, zone.level, cfg)
            stop = stop_base - buffer
            tp = self._retracement_price(swing, target_level)
        else:
            stop_base = self._short_stop_base(swing, zone.level, cfg)
            stop = stop_base + buffer
            tp = self._retracement_price(swing, target_level)
        return current, stop, tp

    def _long_stop_base(self, swing: Swing, entry_level: float, cfg: dict) -> float:
        if entry_level <= 0.382:
            return self._retracement_price(swing, cfg.get("shallow_entry_stop_level", 0.618))
        return self._retracement_price(swing, cfg.get("deep_entry_stop_level", 0.786))

    def _short_stop_base(self, swing: Swing, entry_level: float, cfg: dict) -> float:
        if entry_level <= 0.382:
            return self._retracement_price(swing, cfg.get("shallow_entry_stop_level", 0.618))
        return self._retracement_price(swing, cfg.get("deep_entry_stop_level", 0.786))

    def _risk_reward(self, entry: float, stop: float, tp: float, direction: str) -> float:
        if direction == "LONG":
            risk = entry - stop
            reward = tp - entry
        else:
            risk = stop - entry
            reward = entry - tp
        if risk <= 0 or reward <= 0:
            return 0.0
        return reward / risk

    def _quality_tier(self, score: int) -> tuple[str, float]:
        scoring = self._cfg.scoring_config
        if score >= scoring.get("a_plus_threshold", 11):
            return "A_PLUS", 1.0
        if score >= scoring.get("a_threshold", 9):
            return "A", 1.0
        if score >= scoring.get("b_threshold", 5):
            return "B", scoring.get("b_size_multiplier", 0.75)
        return "REJECTED", 0.0

    def _signal(
        self,
        ind: IndicatorSet,
        direction: str,
        entry: float,
        stop: float,
        tp: float,
        total: int,
        quality: str,
        size_mult: float,
        regime: str,
        mode: str,
        swing: Swing,
        zone: FibZone,
        scores: dict,
    ) -> Signal:
        return Signal(
            symbol=ind.symbol,
            direction=direction,
            entry_price=entry,
            stop_loss=stop,
            take_profit=tp,
            confluence_score=total,
            quality=quality,
            regime=regime,
            timestamp=0,
            size_multiplier=size_mult,
            metadata={
                "strategy": "FIB",
                "mode": mode,
                "swing": {
                    "direction": swing.direction,
                    "start": swing.start_price,
                    "end": swing.end_price,
                    "impulse_atr": swing.atr_multiple,
                    "impulse_pct": swing.pct,
                },
                "fib_zone": {"name": zone.name, "low": zone.low, "high": zone.high, "level": zone.level},
                "layer_scores": scores,
                "indicators": {
                    "current_price": ind.current_price,
                    "rsi": ind.rsi,
                    "adx": ind.adx,
                    "atr": ind.atr,
                    "atr_pct": (ind.atr / ind.current_price * 100) if ind.current_price else 0,
                    "bb_width": ind.bb_width,
                    "bb_width_percentile": ind.bb_width_percentile,
                    "ema_50_5m": ind.ema_50_5m,
                    "ema_50_15m": ind.ema_50_15m,
                    "ema_200_1h": ind.ema_200_1h,
                    "trend_bias": ind.trend_bias,
                    "vwap": ind.vwap,
                    "prev_session_high": ind.prev_session_high,
                    "prev_session_low": ind.prev_session_low,
                    "divergence_type": ind.divergence_type,
                    "divergence_strength": ind.divergence_strength,
                    "current_volume": ind.current_volume,
                    "volume_sma_20": ind.volume_sma_20,
                    "volume_ratio": ind.volume_ratio,
                    "candle_pattern": ind.candle_pattern,
                    "near_levels": ind.near_levels,
                },
                # Keep legacy top-level keys for older notification/recovery code.
                "volume_ratio": ind.volume_ratio,
                "rsi": ind.rsi,
                "adx": ind.adx,
                "atr": ind.atr,
            },
        )
