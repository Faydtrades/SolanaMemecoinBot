"""Phase 1 / Step 2: verify live WebSocket connectivity to Solana mainnet.

This test is READ ONLY. It subscribes to Solana slot notifications, prints a
small number of live events, unsubscribes cleanly, and exits.

No wallet, private key, seed phrase, or API key is required.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

from solders.rpc.responses import SlotNotification, SubscriptionResult
from solana.rpc.websocket_api import connect

DEFAULT_RPC_WS = "wss://api.mainnet.solana.com"
EVENT_TARGET = 10
EVENT_TIMEOUT_SECONDS = 20.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def main() -> None:
    ws_url = os.environ.get("SOLANA_RPC_WS", DEFAULT_RPC_WS)

    print("=" * 68)
    print("SOLANA MEMECOIN BOT - PHASE 1 WEBSOCKET TEST")
    print("=" * 68)
    print(f"UTC time : {utc_now()}")
    print(f"WebSocket: {ws_url}")
    print("Mode     : READ ONLY (slot subscription; no wallet / no trading)")
    print(f"Target   : {EVENT_TARGET} live slot notifications")
    print()

    subscription_id: int | None = None

    try:
        async with connect(ws_url) as websocket:
            print("[OK] WebSocket connection established")

            await websocket.slot_subscribe()
            subscription_response = await asyncio.wait_for(
                websocket.recv(), timeout=EVENT_TIMEOUT_SECONDS
            )

            first_message = subscription_response[0]
            if not isinstance(first_message, SubscriptionResult):
                raise RuntimeError(
                    "Unexpected response while creating slot subscription: "
                    f"{type(first_message).__name__}"
                )

            subscription_id = first_message.result
            print(f"[OK] Slot subscription active (id={subscription_id})")
            print()

            received = 0
            previous_slot: int | None = None

            while received < EVENT_TARGET:
                response = await asyncio.wait_for(
                    websocket.recv(), timeout=EVENT_TIMEOUT_SECONDS
                )

                for message in response:
                    if not isinstance(message, SlotNotification):
                        continue

                    received += 1
                    slot = message.result.slot
                    parent = message.result.parent
                    root = message.result.root
                    gap = "-" if previous_slot is None else str(slot - previous_slot)
                    previous_slot = slot

                    print(
                        f"[LIVE {received:02d}/{EVENT_TARGET}] "
                        f"{utc_now()} | slot={slot} | parent={parent} | "
                        f"root={root} | slot_delta={gap}"
                    )

                    if received >= EVENT_TARGET:
                        break

            await websocket.slot_unsubscribe(subscription_id)
            print()
            print("[OK] Unsubscribed cleanly")

        print("[OK] Received live Solana WebSocket events")
        print()
        print("RESULT: PASS")

    except asyncio.TimeoutError as exc:
        print()
        print(
            f"[ERROR] No expected WebSocket message arrived within "
            f"{EVENT_TIMEOUT_SECONDS:.0f} seconds."
        )
        print("RESULT: FAIL")
        raise SystemExit(1) from exc
    except Exception as exc:
        print()
        print(f"[ERROR] {type(exc).__name__}: {exc}")
        print("RESULT: FAIL")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    asyncio.run(main())
