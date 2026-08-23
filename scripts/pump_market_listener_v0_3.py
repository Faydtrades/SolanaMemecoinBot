import argparse
import asyncio
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from websockets.asyncio.client import connect


WS_URL = "wss://api.mainnet.solana.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
SCHEMA_VERSION = "pump_scoped_event_v0.3"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

INVOKE_RE = re.compile(r"^Program\s+([1-9A-HJ-NP-Za-km-z]+)\s+invoke\s+\[(\d+)\]")
EXIT_RE = re.compile(r"^Program\s+([1-9A-HJ-NP-Za-km-z]+)\s+(success|failed:.*)$")
INSTRUCTION_RE = re.compile(r"Instruction:\s*([A-Za-z0-9_ -]+)", re.IGNORECASE)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_instruction(name: str) -> str:
    # Pump logs may use snake_case, CamelCase or PascalCase.
    # Examples:
    #   SellV2 -> sell_v2
    #   BuyExactQuoteInV2 -> buy_exact_quote_in_v2
    #   buy_exact_sol_in -> buy_exact_sol_in
    value = name.strip()
    value = re.sub(r"([a-z0-9])([A-Z])", r"\\1_\\2", value)
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\\1_\\2", value)
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    return value.lower().strip("_")


def raw_file_path() -> Path:
    date_part = datetime.now(timezone.utc).strftime("%Y%m%d")
    return RAW_DIR / f"pump_scoped_events_v0_3_{date_part}.jsonl"


def append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()


def extract_all_instruction_names(logs: list[str]) -> list[str]:
    names: list[str] = []
    for line in logs:
        match = INSTRUCTION_RE.search(line)
        if match:
            name = normalize_instruction(match.group(1))
            if name and name not in names:
                names.append(name)
    return names


def extract_program_scoped_instructions(
    logs: list[str], target_program: str
) -> list[str]:
    """
    Track Solana's nested program invocation stack and only accept
    'Instruction:' log lines while Pump is the currently executing program.
    """
    stack: list[str] = []
    names: list[str] = []

    for line in logs:
        invoke_match = INVOKE_RE.match(line)
        if invoke_match:
            stack.append(invoke_match.group(1))
            continue

        exit_match = EXIT_RE.match(line)
        if exit_match:
            program = exit_match.group(1)

            # Usually the exiting program is the top of stack. If logs are
            # unusual, unwind to the matching program rather than guessing.
            if stack:
                if stack[-1] == program:
                    stack.pop()
                elif program in stack:
                    while stack:
                        popped = stack.pop()
                        if popped == program:
                            break
            continue

        instruction_match = INSTRUCTION_RE.search(line)
        if instruction_match and stack and stack[-1] == target_program:
            name = normalize_instruction(instruction_match.group(1))
            if name and name not in names:
                names.append(name)

    return names


def classify_pump_instructions(instructions: list[str]) -> list[str]:
    categories: list[str] = []

    for instruction in instructions:
        category = "OTHER"

        if instruction == "create" or instruction.startswith("create_"):
            category = "LAUNCH"
        elif instruction == "buy" or instruction.startswith("buy_"):
            category = "BUY"
        elif instruction == "sell" or instruction.startswith("sell_"):
            category = "SELL"
        elif "migrate" in instruction:
            category = "MIGRATION"

        if category not in categories:
            categories.append(category)

    if not categories:
        categories.append("UNKNOWN")

    return categories


def primary_classification(categories: list[str]) -> str:
    priority = ["LAUNCH", "BUY", "SELL", "MIGRATION", "OTHER", "UNKNOWN"]
    for category in priority:
        if category in categories:
            return category
    return "UNKNOWN"


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

    class_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()

    print("=" * 82)
    print("SOLANA MEMECOIN BOT - PUMP MARKET LISTENER v0.3 (PROGRAM-SCOPED)")
    print("=" * 82)
    print(f"UTC start      : {utc_now()}")
    print(f"WebSocket      : {WS_URL}")
    print(f"Pump program   : {PUMP_PROGRAM_ID}")
    print("Commitment     : processed")
    print("Mode           : READ ONLY / RAW RECORDER")
    print(f"Output         : {output_path}")
    print(
        f"Max events     : "
        f"{max_events if max_events is not None else 'unlimited (Ctrl+C to stop)'}"
    )
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
                    if not signature or signature in seen_signatures:
                        continue
                    seen_signatures.add(signature)

                    logs = value.get("logs") or []
                    tx_error = value.get("err")
                    executed = tx_error is None

                    all_instructions = extract_all_instruction_names(logs)
                    pump_instructions = extract_program_scoped_instructions(
                        logs, PUMP_PROGRAM_ID
                    )
                    categories = classify_pump_instructions(pump_instructions)
                    primary = primary_classification(categories)

                    record = {
                        "schema_version": SCHEMA_VERSION,
                        "received_at_utc": utc_now(),
                        "source": "solana_logs_subscribe",
                        "commitment": "processed",
                        "program_id": PUMP_PROGRAM_ID,
                        "slot": context.get("slot"),
                        "signature": signature,
                        "executed": executed,
                        "transaction_status": "OK" if executed else "ERROR",
                        "transaction_error": tx_error,
                        "pump_instruction_names": pump_instructions,
                        "all_instruction_names": all_instructions,
                        "classifications": categories,
                        "primary_classification": primary,
                        "raw_logs": logs,
                    }

                    append_jsonl(output_path, record)
                    written += 1

                    class_counts[primary] += 1
                    status_counts[record["transaction_status"]] += 1

                    pump_display = ",".join(pump_instructions) if pump_instructions else "-"
                    print(
                        f"[EVENT {written:04d}] "
                        f"{record['received_at_utc']} | "
                        f"{primary:<9} | "
                        f"executed={'YES' if executed else 'NO ':<3} | "
                        f"slot={record['slot']} | "
                        f"pump_ix={pump_display}"
                    )
                    print(f"             sig={signature}")

                    if max_events is not None and written >= max_events:
                        await unsubscribe(websocket, subscription_id)
                        print()
                        print("-" * 82)
                        print("SUMMARY")
                        print("-" * 82)
                        print(f"Events written : {written}")
                        print(
                            "Status         : "
                            + ", ".join(
                                f"{key}={value}"
                                for key, value in sorted(status_counts.items())
                            )
                        )
                        print(
                            "Classes        : "
                            + ", ".join(
                                f"{key}={value}"
                                for key, value in sorted(class_counts.items())
                            )
                        )
                        print(f"Raw JSONL      : {output_path}")
                        print()
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
        description=(
            "Read-only Pump.fun program-scoped log listener + append-only recorder."
        )
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after N unique Pump-related transactions. Omit for Ctrl+C mode.",
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
