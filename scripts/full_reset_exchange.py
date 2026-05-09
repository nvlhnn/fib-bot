#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from src.core.config import Config
from src.exchange.binance_client import BinanceClient


async def main() -> None:
    cfg = Config(config_path=Path("config/settings.yaml"))
    client = BinanceClient(cfg)
    await client.connect()
    summary = {"cancelled_orders": [], "cancel_errors": [], "closed_positions": [], "close_errors": []}
    try:
        # Cancel all open regular and algo orders first so protection/DCA cannot fire during reset.
        orders = await client.get_open_orders()
        seen: set[tuple[str, str]] = set()
        for order in orders:
            symbol = order.get("symbol") or ""
            order_id = str(order.get("id") or "")
            if not symbol or not order_id or (symbol, order_id) in seen:
                continue
            seen.add((symbol, order_id))
            try:
                ok = await client.cancel_order(symbol, order_id)
                summary["cancelled_orders"].append({"symbol": symbol, "id": order_id, "ok": ok})
            except Exception as e:
                summary["cancel_errors"].append({"symbol": symbol, "id": order_id, "error": str(e)})

        # Close all remaining futures positions at market reduce-only.
        positions = await client.get_positions()
        for pos in positions:
            symbol = pos.get("symbol") or ""
            side = (pos.get("side") or "").lower()
            amount = abs(float(pos.get("contracts") or 0))
            if not symbol or amount <= 0 or side not in {"long", "short"}:
                continue
            close_side = "sell" if side == "long" else "buy"
            try:
                order_id = await client.close_position_market(symbol, close_side, amount)
                summary["closed_positions"].append({"symbol": symbol, "side": side, "amount": amount, "order_id": order_id})
            except Exception as e:
                summary["close_errors"].append({"symbol": symbol, "side": side, "amount": amount, "error": str(e)})

        await asyncio.sleep(1)
        summary["remaining_orders"] = [
            {"symbol": o.get("symbol"), "id": o.get("id"), "type": o.get("type"), "side": o.get("side"), "amount": o.get("amount"), "reduceOnly": o.get("reduceOnly")}
            for o in await client.get_open_orders()
        ]
        summary["remaining_positions"] = [
            {"symbol": p.get("symbol"), "side": p.get("side"), "contracts": p.get("contracts"), "notional": p.get("notional")}
            for p in await client.get_positions()
        ]
        print(json.dumps(summary, indent=2, default=str))
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
