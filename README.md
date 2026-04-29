# FIB Scalper

Separate Binance USDT-M futures bot using the hardened confluence runtime with a configurable Fibonacci strategy.

Initial active mode: `reversal`.

Modes available in `config/settings.yaml`:

- `reversal` — fade Fib extension exhaustion zones (`1.272`, `1.618`) after confirmation.
- `trend_pullback` — trade with the impulse trend on Fib retracements (`0.382`, `0.5`, `0.618`, `0.786`).
- `confluence` — currently trend-pullback with stricter confirmation hooks; reserved for extra layers.

Default timeframe model:

- `1h` trend/context
- `15m` swing/impulse detection
- `5m` entry confirmation

The bot is scaffolded for testnet first. Keep `.env` out of git.
