# Fibonacci Strategy

## Reversal mode (first active mode)

1. Detect a strong 15m impulse swing.
2. Project Fib extensions from the impulse.
3. Watch extension zones such as `1.272` and `1.618`.
4. Enter counter-trend only after confirmation:
   - rejection candle, or
   - RSI divergence
5. Optional volume gate can be enabled with `require_volume: true`.
6. Stop sits beyond the extension zone with ATR/min-percent buffer.
7. Take profit defaults toward the `0.618` retracement, with RR fallback.

## Trend pullback mode

1. Detect a strong 15m impulse.
2. Trade with the impulse direction.
3. Enter on pullback into configured retracement levels.
4. Require rejection or volume confirmation.
5. Stop beyond swing origin; TP near swing end or RR fallback.

## Config knobs

All primary knobs live under `strategy.fibonacci` in `config/settings.yaml`.
