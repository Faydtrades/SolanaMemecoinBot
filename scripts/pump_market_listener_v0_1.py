import argparse
import asyncio
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from websockets.asyncio.client import connect


WS_URL = "wss://api.mainnet.solana.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
SCHEMA_VERSION = "pump_raw_event_v0.1"

# Project root = parent of /scripts
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

INSTRUCTION_PATTERN = re.compile(r"Instruction:\s*([A-Za-z0-9_ -]+)", re.IGNORECASE)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_instruction(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def extract_instruction_names(logs: list[str]) -> list[str]:
    names: list[str] = []
    for line in logs:
        match = INSTRUCTION_PATTERN.search(line)
        if match:
            normalized = normalize_instruction(match.group(1))
            if normalized and normalized not in names:
                names.append(normalized)
    return names


def classify_instructions(instructions: list[str]) -> list[str]:
    categories: list[str] = []

    for instruction in instructions:
        category = "OTHER"

        if instruction in {"create", "create_v2"} or instruction.startswith("create_"):
            category = "LAUNCH"

        elif (
            instruction in {
                "buy",
                "buy_v2",
                "buy_exact_sol_in",
                "buy_exact_quote_in",
                "buy_exact_quote_in_v2",
            }
            or instruction.startswith("buy_")
        ):
            category = "BUY"

        elif instruction in {"sell", "sell_v2"} or instruction.startswith("sell_"):
            category = "SELL"

        elif "migrate" in instruction:
            category = "MIGRATION"

        if category not in categories:
            categories.append(category)

    if not categories:
        categories.append("UNKNOWN")

    return categories


def primary_classification(categories: list[str]) -> str:
    # Preserve all categories in the record. This is only a console-friendly primary label.
    priority = ["LAUNCH", "BUY", "SELL", "MIGRATION", "OTHER", "UNKNOWN"]
    for item in priority:
        if item in categories:
            return item
    return "UNKNOWN"


def raw_file_path() -> Path:
    date_part = datetime.now(timezone.utc).strftime("%Y%m%d")
    return RAW_DIR / f"pump_program_events_{date_part}.jsonl"


def append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()


async def subscribe(websocket) -> int:
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [
            {"mentions": [PUMP_PROGRAM_ID]},
            {"commitment": "processed"},
        ],
    }
    await websocket.send(json.dumps(request))

    while True:
        raw = await asyncio.wait_for(websocket.recv(), timeout=15)
        response = json.loads(raw)

        if response.get("id") != 1:
            continue

        if "error" in response:
            raise RuntimeError(f"Subscription error: {response['error']}")

        subscription_id = response.get("result")
        if subscription_id is None:
            raise RuntimeError(f"Unexpected subscribe response: {response}")

        return int(subscription_id)


async def unsubscribe(websocket, subscription_id: int) -> None:
    request = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "logsUnsubscribe",
        "params": [subscription_id],
    }
    await websocket.send(json.dumps(request))

    try:
        while True:
            raw = await asyncio.wait_for(websocket.recv(), timeout=5)
            response = json.loads(raw)
            if response.get("id") == 2:
                return
    except asyncio.TimeoutError:
        return


async def run_listener(max_events: int | None) -> None:
    output_path = raw_file_path()
    seen_signatures: set[str] = set()
    written = 0
    reconnect_attempt = 0

    print("=" * 76)
    print("SOLANA MEMECOIN BOT - PUMP MARKET LISTENER v0.1")
    print("=" * 76)
    print(f"UTC start      : {utc_now()}")
    print(f"WebSocket      : {WS_URL}")
    print(f"Pump program   : {PUMP_PROGRAM_ID}")
    print("Commitment     : processed")
    print("Mode           : READ ONLY / RAW RECORDER")
    print(f"Output         : {output_path}")
    print(f"Max events     : {max_events if max_events is not None else 'unlimited (Ctrl+C to stop)'}")
    print()

    while True:
        subscription_id = None

        try:
            async with connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=4 * 1024 * 1024,
            ) as websocket:
                reconnect_attempt = 0
                print(f"[{utc_now()}] [OK] WebSocket connected")

                subscription_id = await subscribe(websocket)
                print(
                    f"[{utc_now()}] [OK] Pump subscription active "
                    f"(id={subscription_id})"
                )

                async for raw in websocket:
                    message = json.loads(raw)

                    if message.get("method") != "logsNotification":
                        continue

                    params = message.get("params", {})
                    result = params.get("result", {})
                    context = result.get("context", {})
                    value = result.get("value", {})

                    signature = value.get("signature")
                    if not signature:
                        continue

                    # Deterministic transport-level de-duplication within this run.
                    if signature in seen_signatures:
                        continue
                    seen_signatures.add(signature)

                    logs = value.get("logs") or []
                    instructions = extract_instruction_names(logs)
                    categories = classify_instructions(instructions)
                    primary = primary_classification(categories)
                    tx_error = value.get("err")

                    record = {
                        "schema_version": SCHEMA_VERSION,
                        "received_at_utc": utc_now(),
                        "source": "solana_logs_subscribe",
                        "commitment": "processed",
                        "program_id": PUMP_PROGRAM_ID,
                        "slot": context.get("slot"),
                        "signature": signature,
                        "transaction_status": "OK" if tx_error is None else "ERROR",
                        "transaction_error": tx_error,
                        "instruction_names": instructions,
                        "classifications": categories,
                        "primary_classification": primary,
                        "raw_logs": logs,
                    }

                    append_jsonl(output_path, record)
                    written += 1

                    instruction_display = ",".join(instructions) if instructions else "-"
                    print(
                        f"[EVENT {written:04d}] "
                        f"{record['received_at_utc']} | "
                        f"{primary:<9} | "
                        f"slot={record['slot']} | "
                        f"status={record['transaction_status']} | "
                        f"instructions={instruction_display}"
                    )
                    print(f"             sig={signature}")

                    if max_events is not None and written >= max_events:
                        await unsubscribe(websocket, subscription_id)
                        print()
                        print(f"[OK] Test target reached: {written} events written")
                        print(f"[OK] Raw JSONL file: {output_path}")
                        print("RESULT: PASS")
                        return

        except asyncio.CancelledError:
            raise
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            reconnect_attempt += 1
            delay = min(2 ** min(reconnect_attempt, 5), 30)
            print(
                f"[{utc_now()}] [WARN] Connection/listener error: "
                f"{type(exc).__name__}: {exc}"
            )
            print(f"[{utc_now()}] [INFO] Reconnecting in {delay}s...")
            await asyncio.sleep(delay)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only Pump.fun program log listener + append-only JSONL recorder."
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after N unique Pump transactions. Omit to run until Ctrl+C.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        asyncio.run(run_listener(args.max_events))
    except KeyboardInterrupt:
        print()
        print("[STOP] Listener stopped by user (Ctrl+C).")
        print("RESULT: STOPPED CLEANLY")


if __name__ == "__main__":
    main()
