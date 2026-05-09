# Fib Bot Revamp Plan

Goal: rebuild the bot around a safer, lower-REST architecture so it can trade reliably without repeatedly triggering Binance Futures testnet `418` IP bans.

## Current Problems

1. **REST-heavy candle fetching**
   - Signal loop fetches candles for every active coin every 5m.
   - Startup also runs ATR scans via REST candles.
   - Even with reduced active coins, Binance testnet keeps banning the server IP.

2. **Backoff is reactive, not architectural**
   - Current patch detects `418` and pauses, but the next REST call after cooldown can trigger another ban.
   - This prevents reliable signal generation and order execution.

3. **Position recovery/history issues**
   - Recovered trades can show `confluence_score=0` / `RECOVERED`, which makes trade history analysis misleading.

4. **Shutdown/backoff loops are too slow**
   - `systemctl stop fib-bot.service` can hang until systemd kills it.

5. **Safety gating blocks entries during API errors**
   - When position checks fail due API ban, entry execution is correctly blocked, but this means good signals are missed.

## Target Architecture

```text
Binance WebSocket klines
        ↓
MarketDataStore / CandleStore
        ↓
IndicatorEngine + FibonacciScorer
        ↓
RiskManager + ExecutionEngine
        ↓
REST only for orders, balances, positions, recovery
```

REST should be used for:
- startup market metadata
- initial historical candle bootstrap
- order placement/cancel
- balance/position/open-order reconciliation

WebSocket should be used for:
- live `5m`, `15m`, `1h` candle updates
- ticker/mark price updates where possible

## Phase 1 — Stabilize Current Bot Before Bigger Changes

### 1. Stop bot before refactor

```bash
systemctl stop fib-bot.service
```

Verify no process remains:

```bash
pgrep -af 'fib-scalper|main.py' || true
```

### 2. Patch shutdown behavior

- Replace long `asyncio.sleep(...)` calls during backoff with cancellable helper:

```python
async def sleep_with_shutdown(self, seconds: float, step: float = 1.0):
    end = time.time() + seconds
    while self.is_running and time.time() < end:
        await asyncio.sleep(min(step, end - time.time()))
```

- Use it in:
  - signal checker backoff
  - position monitor backoff
  - scanner backoff
  - startup scan retry

### 3. Patch heartbeat to respect Binance backoff

Heartbeat must not call Binance during a known ban window.

Expected behavior:
- If `client.is_rate_limited()`, skip Binance API heartbeat checks.
- Log one concise warning max per ban window.

### 4. Make 418 cooldown conservative

Current: ban expiry + small buffer.

Change to:

```text
cooldown_until = banned_until + 10 minutes
```

If bans continue, use exponential cooloff:

```text
1st ban: +10m
2nd consecutive ban: +20m
3rd consecutive ban: +40m
max: +2h
```

## Phase 2 — WebSocket Candle Store

### New module

Create:

```text
src/data/ws_candle_store.py
```

Responsibilities:
- maintain candles per symbol/timeframe
- expose ready state
- update closed candles from websocket events
- keep max history length similar to current cache:
  - `5m`: 200 candles
  - `15m`: 100 candles
  - `1h`: 200 candles

Suggested interface:

```python
class WebSocketCandleStore:
    async def start(self, symbols: list[str], timeframes: list[str]) -> None: ...
    async def stop(self) -> None: ...
    def get(self, symbol: str) -> dict[str, list[Candle]]: ...
    def ready_symbols(self) -> set[str]: ...
    async def update_symbols(self, symbols: list[str]) -> None: ...
```

### Binance stream format

USDT-M Futures websocket base:

```text
wss://fstream.binance.com/stream?streams=
```

Combined stream examples:

```text
btcusdt@kline_5m/ethusdt@kline_5m
```

For each active symbol, subscribe to:

```text
<raw_lower>@kline_5m
<raw_lower>@kline_15m
<raw_lower>@kline_1h
```

For 30 coins × 3 timeframes = 90 streams. If needed, split into chunks of 40–50 streams per connection.

### Candle event handling

Only evaluate/store when kline is closed:

```json
{
  "k": {
    "t": 123,
    "T": 456,
    "s": "BTCUSDT",
    "i": "5m",
    "o": "...",
    "h": "...",
    "l": "...",
    "c": "...",
    "v": "...",
    "x": true
  }
}
```

Map raw symbol back to unified symbol using existing client map.

## Phase 3 — Bootstrap Historical Candles Once

Websocket alone starts empty. We still need initial history.

On startup:
1. Run active coin scan.
2. For selected coins, bootstrap historical candles with REST.
3. Use strict REST budget:
   - sequential or tiny batch size
   - delay between requests
   - stop immediately on `418`
4. Start websocket streams.
5. Only run signal checks for symbols with all required histories ready.

If bootstrap hits `418`, pause for conservative cooldown and resume, instead of continuing partial hammering.

## Phase 4 — Replace Signal Loop Candle Fetching

Current signal loop calls:

```python
candle_data = await self.candle_cache.update(active_coins, self.client)
```

Replace with local store read:

```python
candle_data = self.ws_candles.snapshot(active_coins)
```

Signal checker should:
- wake on 5m candle close, or still use clock-based 5m cycle initially
- read local candle cache only
- never call REST for candles during normal operation

Expected result:
- normal signal cycles use **0 REST candle calls**

## Phase 5 — Position/Ticker Monitoring via WebSocket

To reduce REST further:

1. Add mark price/ticker websocket stream for active/open symbols.
2. Position monitor uses local ticker price for unrealized PnL.
3. REST position reconciliation runs less often:
   - on startup
   - after order placement
   - every 5–15 minutes when not banned
   - on suspected SL/TP close

Important: order/position truth still comes from REST, but not every 30 seconds.

## Phase 6 — Execution Safety Improvements

### 1. Preserve recovered trade metadata

When recovering an open position from DB, copy:
- `confluence_score`
- `quality`
- `regime`
- original `entry_order_id`
- metadata

Recovered rows should not become score `0` unless there is genuinely no DB record.

### 2. Do not execute entries if API degraded

Before order placement:

```python
if client.is_rate_limited():
    reject signal with reason="API backoff active"
```

Do not mark as generic `Execution failed`; use explicit reason.

### 3. Recovery state warning

If exchange position exists but open orders cannot be verified due API ban:
- do not mark unmanaged permanently
- set state as `UNKNOWN_PROTECTION_DURING_API_BACKOFF`
- retry after cooldown
- notify user only if still unverifiable after cooldown

## Phase 7 — Config Changes

Add config section:

```yaml
market_data:
  mode: "websocket"  # websocket | rest
  websocket:
    enabled: true
    timeframes: ["5m", "15m", "1h"]
    max_streams_per_connection: 50
    reconnect_base_delay_seconds: 5
    reconnect_max_delay_seconds: 120
  rest_bootstrap:
    batch_size: 2
    delay_seconds: 1.5
    cooldown_after_418_minutes: 10
```

Keep fallback:

```yaml
market_data:
  mode: "rest"
```

So we can roll back quickly.

## Phase 8 — Tests / Verification

Minimum checks before restart:

```bash
venv/bin/python -m py_compile $(find src -name '*.py') main.py dashboard.py scripts/full_reset_exchange.py
```

Manual verification:

1. Start bot with REST mode off / WS mode on.
2. Confirm startup scan succeeds or pauses cleanly.
3. Confirm websocket connects.
4. Confirm candle store fills.
5. Confirm signal checker cycles without `fetch_candles` REST logs.
6. Confirm no `418` after 30–60 minutes.
7. Confirm service stops within 5–10 seconds.

## Phase 9 — Deployment Plan

1. Keep `fib-bot.service` stopped.
2. Implement Phase 1 shutdown/backoff fixes.
3. Implement websocket candle store behind config flag.
4. Test in dry-run / no-entry mode if available; otherwise use testnet with very small active coin count.
5. Start with:
   - `max_active_coins: 10`
   - `atr_scan_limit: 20`
6. Observe 30 minutes.
7. Increase to:
   - `max_active_coins: 30`
   - `atr_scan_limit: 40`

## Recommended First Coding Order

1. `sleep_with_shutdown()` and heartbeat backoff fix.
2. Conservative `418` cooldown.
3. `WebSocketCandleStore` skeleton.
4. Historical bootstrap method.
5. Swap signal checker to read from local store.
6. Preserve recovered score metadata.
7. Commit and push.

## Success Criteria

- Bot can run 1 hour without Binance `418`.
- Signal loop produces valid checks with zero REST candle polling.
- Startup/restart does not create API request bursts.
- `systemctl stop fib-bot.service` exits quickly.
- Trade history preserves original signal scores after recovery.
