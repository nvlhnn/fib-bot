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

## Closed Trade Dashboard

Fibbo includes a lightweight read-only web dashboard for closed trade history.
It uses only Python stdlib + the existing SQLite DB, so no extra frontend stack is needed.

```bash
python dashboard.py
```

Default URL:

```text
http://127.0.0.1:8090
```

Options:

```bash
python dashboard.py --host 0.0.0.0 --port 8090 --db data/fib.db
```

The dashboard reads `data/fib.db`, filters only `status = CLOSED` trades, and shows:

- total net PnL, win rate, profit factor, average ROI, fees
- equity curve and daily net PnL bars
- long vs short performance
- top symbols, best/worst trades
- searchable closed-trade table
