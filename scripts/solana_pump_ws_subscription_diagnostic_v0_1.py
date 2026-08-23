from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

import httpx
import websockets


HTTP_RPC = "https://api.mainnet.solana.com"
WS_RPC = "wss://api.mainnet.solana.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


async def rpc_http(method: str, params=None):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": [] if params is None else params,
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(HTTP_RPC, json=payload)
        r.raise_for_status()
        return r.json()


async def wait_for_response(ws, request_id: int, timeout_s: float):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        msg = json.loads(raw)
        if msg.get("id") == request_id:
            return msg
    raise TimeoutError(f"No response for request id={request_id}")


async def wait_for_notification(ws, method: str, timeout_s: float):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        msg = json.loads(raw)
        if msg.get("method") == method:
            return msg
    raise TimeoutError(f"No {method} within {timeout_s}s")


async def test_slot_subscription():
    print()
    print("-" * 100)
    print("TEST B — WebSocket slotSubscribe")
    print("-" * 100)

    async with websockets.connect(
        WS_RPC,
        ping_interval=None,
        open_timeout=10,
        close_timeout=5,
        max_size=4_000_000,
    ) as ws:
        req = {
            "jsonrpc": "2.0",
            "id": 101,
            "method": "slotSubscribe",
        }
        await ws.send(json.dumps(req))
        ack = await wait_for_response(ws, 101, 10.0)
        print("Subscription ACK :", ack)

        if "error" in ack:
            return False, f"slotSubscribe RPC error: {ack['error']}"

        note = await wait_for_notification(ws, "slotNotification", 15.0)
        result = note.get("params", {}).get("result", {})
        print("Slot notification:", result)

        sub_id = ack.get("result")
        if sub_id is not None:
            await ws.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 102,
                        "method": "slotUnsubscribe",
                        "params": [sub_id],
                    }
                )
            )

        return True, None


async def test_pump_logs_subscription():
    print()
    print("-" * 100)
    print("TEST C — Pump logsSubscribe")
    print("-" * 100)
    print("Pump program     :", PUMP_PROGRAM_ID)

    async with websockets.connect(
        WS_RPC,
        ping_interval=None,
        open_timeout=10,
        close_timeout=5,
        max_size=8_000_000,
    ) as ws:
        req = {
            "jsonrpc": "2.0",
            "id": 201,
            "method": "logsSubscribe",
            "params": [
                {"mentions": [PUMP_PROGRAM_ID]},
                {"commitment": "processed"},
            ],
        }
        await ws.send(json.dumps(req))
        ack = await wait_for_response(ws, 201, 10.0)
        print("Subscription ACK :", ack)

        if "error" in ack:
            return False, f"logsSubscribe RPC error: {ack['error']}"

        note = await wait_for_notification(ws, "logsNotification", 30.0)
        params = note.get("params", {})
        result = params.get("result", {})
        value = result.get("value", {}) if isinstance(result, dict) else {}
        context = result.get("context", {}) if isinstance(result, dict) else {}

        print("Notification slot:", context.get("slot"))
        print("Signature        :", value.get("signature"))
        logs = value.get("logs") or []
        print("Log lines        :", len(logs))
        for line in logs[:8]:
            print("  ", line)

        sub_id = ack.get("result")
        if sub_id is not None:
            await ws.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 202,
                        "method": "logsUnsubscribe",
                        "params": [sub_id],
                    }
                )
            )

        return True, None


async def main_async() -> int:
    print("=" * 100)
    print("SOLANA / PUMP WEBSOCKET SUBSCRIPTION DIAGNOSTIC v0.1")
    print("=" * 100)
    print("HTTP RPC         :", HTTP_RPC)
    print("WebSocket RPC    :", WS_RPC)
    print("Pump program     :", PUMP_PROGRAM_ID)
    print("Production DB    : NOT OPENED")
    print("Collector        : NOT STARTED")
    print("Wallet/orders    : NONE")
    print()

    http_ok = False
    slot_ok = False
    pump_ok = False
    errors = []

    print("-" * 100)
    print("TEST A — HTTP RPC")
    print("-" * 100)
    try:
        version = await rpc_http("getVersion")
        slot = await rpc_http("getSlot", [{"commitment": "processed"}])
        print("getVersion:", version)
        print("getSlot   :", slot)
        http_ok = "result" in version and "result" in slot
        if not http_ok:
            errors.append("HTTP RPC returned no result")
    except Exception as exc:
        errors.append(f"HTTP RPC: {type(exc).__name__}: {exc}")
        print("HTTP ERROR:", errors[-1])

    try:
        slot_ok, err = await test_slot_subscription()
        if err:
            errors.append(err)
    except Exception as exc:
        errors.append(f"slotSubscribe: {type(exc).__name__}: {exc}")
        print("SLOT WS ERROR:", errors[-1])

    try:
        pump_ok, err = await test_pump_logs_subscription()
        if err:
            errors.append(err)
    except Exception as exc:
        errors.append(f"Pump logsSubscribe: {type(exc).__name__}: {exc}")
        print("PUMP WS ERROR:", errors[-1])

    print()
    print("-" * 100)
    print("SUMMARY")
    print("-" * 100)
    print(f"HTTP RPC reachable          : {'PASS' if http_ok else 'FAIL'}")
    print(f"WebSocket slotSubscribe     : {'PASS' if slot_ok else 'FAIL'}")
    print(f"Pump logsSubscribe          : {'PASS' if pump_ok else 'FAIL'}")
    print("Production DB touched       : NO")
    print("Collector started           : NO")
    print("Wallet/live orders          : NO")
    if errors:
        print("Errors:")
        for err in errors:
            print(" -", err)

    result = "PASS" if (http_ok and slot_ok and pump_ok) else "CHECK"
    print()
    print("RESULT:", result)

    if http_ok and slot_ok and not pump_ok:
        print(
            "INTERPRETATION: Solana RPC/WebSocket is alive, but the Pump-specific "
            "logs subscription did not deliver. Diagnose the Pump subscription/filter/program path next."
        )
    elif http_ok and not slot_ok:
        print(
            "INTERPRETATION: HTTP works but WebSocket does not. Diagnose the public RPC "
            "WebSocket endpoint/network path before touching collector/Phase 4."
        )
    elif not http_ok:
        print(
            "INTERPRETATION: Base Solana RPC connectivity is unhealthy. Resolve RPC/network "
            "connectivity before any further live smoke."
        )
    elif http_ok and slot_ok and pump_ok:
        print(
            "INTERPRETATION: Direct Solana + Pump subscriptions work. The fault is inside "
            "the collector's live subscription/receive path rather than the RPC service itself."
        )

    return 0 if result == "PASS" else 2


def main() -> int:
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
