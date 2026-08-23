import asyncio
import json
from datetime import datetime, timezone

from websockets.asyncio.client import connect

WS_URL = "wss://api.mainnet.solana.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
TARGET_EVENTS = 10
TIMEOUT_SECONDS = 60


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def find_instruction_hint(logs: list[str]) -> str:
    for line in logs:
        lower = line.lower()
        if "instruction:" in lower:
            return line.strip()
    return "(no instruction label in logs)"


async def main() -> None:
    print("=" * 66)
    print("SOLANA MEMECOIN BOT - PHASE 1 PUMP PROGRAM LOG TEST")
    print("=" * 66)
    print(f"UTC time     : {utc_now()}")
    print(f"WebSocket    : {WS_URL}")
    print(f"Pump program : {PUMP_PROGRAM_ID}")
    print("Mode         : READ ONLY (no wallet / no trading)")
    print(f"Target       : {TARGET_EVENTS} Pump program transactions")
    print()

    subscribe_request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [
            {"mentions": [PUMP_PROGRAM_ID]},
            {"commitment": "processed"},
        ],
    }

    unsubscribe_id = 2
    subscription_id = None
    received = 0

    try:
        async with connect(
            WS_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_size=4 * 1024 * 1024,
        ) as websocket:
            print("[OK] WebSocket connection established")

            await websocket.send(json.dumps(subscribe_request))
            raw = await asyncio.wait_for(websocket.recv(), timeout=15)
            response = json.loads(raw)

            if "error" in response:
                raise RuntimeError(f"Subscription error: {response['error']}")

            subscription_id = response.get("result")
            if subscription_id is None:
                raise RuntimeError(f"Unexpected subscribe response: {response}")

            print(f"[OK] Pump log subscription active (id={subscription_id})")
            print()

            deadline = asyncio.get_running_loop().time() + TIMEOUT_SECONDS

            while received < TARGET_EVENTS:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Timed out after {TIMEOUT_SECONDS}s with "
                        f"{received}/{TARGET_EVENTS} Pump events received."
                    )

                raw = await asyncio.wait_for(websocket.recv(), timeout=remaining)
                message = json.loads(raw)

                if message.get("method") != "logsNotification":
                    continue

                params = message.get("params", {})
                result = params.get("result", {})
                context = result.get("context", {})
                value = result.get("value", {})

                signature = value.get("signature", "?")
                err = value.get("err")
                logs = value.get("logs") or []
                slot = context.get("slot", "?")

                received += 1
                status = "OK" if err is None else "ERR"
                hint = find_instruction_hint(logs)

                print(
                    f"[PUMP {received:02d}/{TARGET_EVENTS}] "
                    f"{utc_now()} | slot={slot} | status={status}"
                )
                print(f"  signature : {signature}")
                print(f"  hint      : {hint}")

            unsubscribe_request = {
                "jsonrpc": "2.0",
                "id": unsubscribe_id,
                "method": "logsUnsubscribe",
                "params": [subscription_id],
            }
            await websocket.send(json.dumps(unsubscribe_request))

            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=10)
                response = json.loads(raw)
                if response.get("id") == unsubscribe_id and response.get("result") is True:
                    print()
                    print("[OK] Unsubscribed cleanly")
                else:
                    print()
                    print(f"[WARN] Unexpected unsubscribe response: {response}")
            except asyncio.TimeoutError:
                print()
                print("[WARN] Unsubscribe acknowledgement timed out")

        print("[OK] Received live Pump.fun program transactions")
        print()
        print("RESULT: PASS")

    except KeyboardInterrupt:
        print()
        print("RESULT: STOPPED BY USER")
    except Exception as exc:
        print()
        print(f"[ERROR] {type(exc).__name__}: {exc}")
        print()
        print("RESULT: FAIL")
        raise


if __name__ == "__main__":
    asyncio.run(main())
