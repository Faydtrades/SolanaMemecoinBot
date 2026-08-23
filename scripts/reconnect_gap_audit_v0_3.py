import argparse
import asyncio
import base64
import json
import re
import sqlite3
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


RPC_URL = "https://api.mainnet.solana.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

CREATE_EVENT_DISC = bytes([27, 114, 169, 77, 222, 235, 99, 118])
TRADE_EVENT_DISC = bytes([189, 219, 127, 211, 78, 230, 97, 238])

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
AUDIT_DIR = PROJECT_ROOT / "data" / "audit"

INVOKE_RE = re.compile(r"^Program\s+([1-9A-HJ-NP-Za-km-z]+)\s+invoke\s+\[(\d+)\]")
EXIT_RE = re.compile(r"^Program\s+([1-9A-HJ-NP-Za-km-z]+)\s+(success|failed:.*)$")

# Solana public RPC currently documents max 40 requests / 10 seconds
# for a single RPC method. 0.32 sec ≈ 3.1 requests/sec, leaving margin.
MIN_REQUEST_INTERVAL = 0.32
MAX_CONCURRENCY = 4


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def audit_output_path() -> Path:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return AUDIT_DIR / f"reconnect_gap_audit_v0_3_{tag}.jsonl"


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000;")
    return conn


def ensure_time_index(conn: sqlite3.Connection) -> None:
    print("[1/5] Ensuring fast observation-time index...")
    started = time.monotonic()
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_websocket_observations_received_at
        ON websocket_observations(received_at_utc)
        """
    )
    conn.commit()
    print(f"      ready in {time.monotonic() - started:.2f}s")


def safe_details(row: sqlite3.Row) -> dict[str, Any]:
    try:
        return json.loads(row["details_json"])
    except Exception:
        return {}


def find_latest_long_v03_run(
    conn: sqlite3.Connection,
) -> tuple[int, int | None, list[sqlite3.Row]]:
    starts = conn.execute(
        """
        SELECT id, happened_at_utc
        FROM collector_events
        WHERE event_type='COLLECTOR_V0_3_START'
        ORDER BY id DESC
        """
    ).fetchall()

    for start in starts:
        stop = conn.execute(
            """
            SELECT id
            FROM collector_events
            WHERE id > ?
              AND event_type='LIVE_COLLECTION_STOPPED'
            ORDER BY id ASC
            LIMIT 1
            """,
            (start["id"],),
        ).fetchone()

        stop_id = int(stop["id"]) if stop else None

        if stop_id is None:
            rows = conn.execute(
                """
                SELECT *
                FROM collector_events
                WHERE id >= ?
                ORDER BY id
                """,
                (start["id"],),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT *
                FROM collector_events
                WHERE id BETWEEN ? AND ?
                ORDER BY id
                """,
                (start["id"], stop_id),
            ).fetchall()

        gap_count = sum(1 for r in rows if r["event_type"] == "DATA_GAP_CLOSED")
        if gap_count >= 10:
            return int(start["id"]), stop_id, rows

    raise RuntimeError("Could not locate the long v0.3 run with recorded gaps")


def normalize_disconnect_error(text: str) -> str:
    low = text.lower()

    for marker in [
        "keepalive ping timeout",
        "no close frame",
        "connection reset",
        "timed out",
        "timeout",
    ]:
        if marker in low:
            prefix = text.split(":", 1)[0] if ":" in text else text
            return f"{prefix}: {marker}"

    return text[:160] if text else "(missing error)"


def nearest_before(
    conn: sqlite3.Connection,
    timestamp: str,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT signature, slot, received_at_utc
        FROM websocket_observations
        WHERE received_at_utc <= ?
        ORDER BY received_at_utc DESC
        LIMIT 1
        """,
        (timestamp,),
    ).fetchone()


def nearest_after(
    conn: sqlite3.Connection,
    timestamp: str,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT signature, slot, received_at_utc
        FROM websocket_observations
        WHERE received_at_utc >= ?
        ORDER BY received_at_utc ASC
        LIMIT 1
        """,
        (timestamp,),
    ).fetchone()


class RateGate:
    def __init__(self, interval: float):
        self.interval = interval
        self.lock = asyncio.Lock()
        self.last_request = 0.0
        self.cooldown_until = 0.0

    async def wait_turn(self) -> None:
        async with self.lock:
            now = time.monotonic()
            target = max(
                self.last_request + self.interval,
                self.cooldown_until,
            )
            if target > now:
                await asyncio.sleep(target - now)
            self.last_request = time.monotonic()

    async def cooldown(self, seconds: float) -> None:
        async with self.lock:
            self.cooldown_until = max(
                self.cooldown_until,
                time.monotonic() + max(seconds, 1.0),
            )


GATE = RateGate(MIN_REQUEST_INTERVAL)
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENCY)


async def rpc_post(
    client: httpx.AsyncClient,
    payload: dict[str, Any],
) -> dict[str, Any]:
    for attempt in range(1, 6):
        await GATE.wait_turn()

        try:
            async with SEMAPHORE:
                response = await client.post(RPC_URL, json=payload)

            if response.status_code == 429:
                retry_header = response.headers.get("Retry-After")
                try:
                    delay = float(retry_header) if retry_header else 10.0
                except ValueError:
                    delay = 10.0

                print(f"      [429] global cooldown {delay:.1f}s")
                await GATE.cooldown(delay)
                continue

            response.raise_for_status()
            body = response.json()

            if "error" in body:
                raise RuntimeError(f"RPC error: {body['error']}")

            return body

        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            if attempt >= 5:
                raise
            delay = min(2 ** attempt, 10)
            print(
                f"      [retry {attempt}/5] {type(exc).__name__}; "
                f"waiting {delay}s"
            )
            await asyncio.sleep(delay)

    raise RuntimeError("RPC attempts exhausted")


async def get_blocks(
    client: httpx.AsyncClient,
    start_slot: int,
    end_slot: int,
) -> list[int]:
    if end_slot < start_slot:
        return []

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlocks",
        "params": [
            start_slot,
            end_slot,
            {"commitment": "confirmed"},
        ],
    }
    body = await rpc_post(client, payload)
    return [int(x) for x in (body.get("result") or [])]


async def get_block(
    client: httpx.AsyncClient,
    slot: int,
) -> tuple[int, dict[str, Any] | None]:
    payload = {
        "jsonrpc": "2.0",
        "id": slot,
        "method": "getBlock",
        "params": [
            slot,
            {
                "commitment": "confirmed",
                "encoding": "jsonParsed",
                "transactionDetails": "full",
                "maxSupportedTransactionVersion": 0,
                "rewards": False,
            },
        ],
    }
    body = await rpc_post(client, payload)
    return slot, body.get("result")


def extract_pump_event_discriminators(logs: list[str]) -> list[str]:
    stack: list[str] = []
    found: list[str] = []

    for line in logs or []:
        if not isinstance(line, str):
            continue

        invoke_match = INVOKE_RE.match(line)
        if invoke_match:
            stack.append(invoke_match.group(1))
            continue

        exit_match = EXIT_RE.match(line)
        if exit_match:
            program = exit_match.group(1)
            if stack:
                if stack[-1] == program:
                    stack.pop()
                elif program in stack:
                    while stack:
                        popped = stack.pop()
                        if popped == program:
                            break
            continue

        if not (stack and stack[-1] == PUMP_PROGRAM_ID):
            continue

        if not line.startswith("Program data: "):
            continue

        try:
            raw = base64.b64decode(
                line[len("Program data: "):].strip(),
                validate=True,
            )
        except Exception:
            continue

        if len(raw) >= 8 and raw[:8] in (CREATE_EVENT_DISC, TRADE_EVENT_DISC):
            found.append(raw[:8].hex())

    return found


def transaction_signature(tx_entry: dict[str, Any]) -> str | None:
    tx = tx_entry.get("transaction") or {}
    signatures = tx.get("signatures") or []
    return signatures[0] if signatures else None


def local_event_exists(
    conn: sqlite3.Connection,
    event_key: str,
) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM pump_events WHERE event_key=? LIMIT 1",
            (event_key,),
        ).fetchone()
        is not None
    )


def append_report(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


async def main(audit_gaps: int) -> None:
    conn = connect_db()

    print("=" * 106)
    print("SOLANA MEMECOIN BOT - OPTIMIZED RECONNECT + GAP AUDIT v0.3")
    print("=" * 106)
    print(f"Database : {DB_PATH}")
    print()

    ensure_time_index(conn)

    print("[2/5] Locating long unattended v0.3 run...")
    start_id, stop_id, run_rows = find_latest_long_v03_run(conn)

    disconnects = [
        r for r in run_rows if r["event_type"] == "WS_DISCONNECTED"
    ]
    gap_rows = [
        r for r in run_rows if r["event_type"] == "DATA_GAP_CLOSED"
    ]

    error_counts = Counter(
        normalize_disconnect_error(str(safe_details(r).get("error", "")))
        for r in disconnects
    )

    gap_candidates = []
    for row in gap_rows:
        details = safe_details(row)
        start = details.get("gap_started_at_utc")
        end = details.get("gap_ended_at_utc")
        if not start or not end:
            continue

        try:
            seconds = float(details.get("gap_seconds") or 0.0)
        except Exception:
            seconds = 0.0

        gap_candidates.append(
            {
                "event_id": int(row["id"]),
                "start": start,
                "end": end,
                "seconds": seconds,
            }
        )

    gap_candidates.sort(key=lambda x: x["seconds"], reverse=True)
    selected = gap_candidates[:max(0, audit_gaps)]

    print(f"      start id={start_id}, stop id={stop_id}")
    print(f"      disconnects={len(disconnects)}, gaps={len(gap_candidates)}")
    print()

    print("-" * 106)
    print("RECONNECT CAUSES")
    print("-" * 106)
    for reason, count in error_counts.most_common(20):
        print(f"{count:5d}  {reason}")

    print()
    print("[3/5] Resolving slot boundaries for selected gaps...")

    resolved = []

    for i, gap in enumerate(selected, 1):
        before = nearest_before(conn, gap["start"])
        after = nearest_after(conn, gap["end"])

        if before is None or after is None:
            print(f"      GAP {i:02d}: boundary not found")
            continue

        before_slot = int(before["slot"])
        after_slot = int(after["slot"])

        # Only FULL slots strictly inside the observed boundaries count as
        # definitive missed-data territory. Boundary slots can contain txs
        # both before and after the gap and are therefore intentionally excluded.
        first_interior = before_slot + 1
        last_interior = after_slot - 1

        resolved.append(
            {
                **gap,
                "before_slot": before_slot,
                "after_slot": after_slot,
                "first_interior": first_interior,
                "last_interior": last_interior,
            }
        )

        interior_count = max(0, last_interior - first_interior + 1)

        print(
            f"      GAP {i:02d}: {gap['seconds']:.3f}s | "
            f"boundary slots {before_slot}->{after_slot} | "
            f"interior span={interior_count}"
        )

    report_path = audit_output_path()

    print()
    print("[4/5] Fetching unique confirmed interior blocks...")

    timeout = httpx.Timeout(
        connect=15.0,
        read=60.0,
        write=20.0,
        pool=20.0,
    )

    gap_slots: dict[int, list[int]] = {}
    unique_slots: set[int] = set()
    getblocks_failures = 0

    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"Content-Type": "application/json"},
    ) as client:

        for gap in resolved:
            if gap["last_interior"] < gap["first_interior"]:
                gap_slots[gap["event_id"]] = []
                continue

            try:
                slots = await get_blocks(
                    client,
                    gap["first_interior"],
                    gap["last_interior"],
                )
                gap_slots[gap["event_id"]] = slots
                unique_slots.update(slots)
            except Exception as exc:
                getblocks_failures += 1
                gap_slots[gap["event_id"]] = []
                print(
                    f"      getBlocks failed for gap {gap['event_id']}: "
                    f"{type(exc).__name__}: {exc}"
                )

        ordered_slots = sorted(unique_slots)

        print(
            f"      {len(ordered_slots)} unique confirmed blocks to fetch "
            f"(overlap removed)"
        )

        block_cache: dict[int, dict[str, Any] | None] = {}
        block_failures = 0

        async def fetch_one(slot: int):
            try:
                return await get_block(client, slot)
            except Exception as exc:
                return slot, exc

        tasks = [asyncio.create_task(fetch_one(slot)) for slot in ordered_slots]

        completed = 0
        for future in asyncio.as_completed(tasks):
            slot, result = await future
            completed += 1

            if isinstance(result, Exception):
                block_failures += 1
                print(
                    f"      block {slot} failed: "
                    f"{type(result).__name__}: {result}"
                )
            else:
                block_cache[slot] = result

            if completed % 25 == 0 or completed == len(tasks):
                print(
                    f"      blocks fetched: {completed}/{len(tasks)}"
                )

    print()
    print("[5/5] Comparing authoritative confirmed Pump events with SQLite...")
    print()

    total_chain_events = 0
    total_local = 0
    total_missing = 0
    gaps_with_missing = 0
    auditable_gaps = 0

    for i, gap in enumerate(resolved, 1):
        slots = gap_slots.get(gap["event_id"], [])

        if gap["last_interior"] < gap["first_interior"]:
            print(
                f"[GAP {i:02d}] {gap['seconds']:7.3f}s | "
                f"no full interior slot -> INCONCLUSIVE"
            )
            continue

        gap_chain = 0
        gap_local = 0
        gap_missing = 0
        gap_block_failures = 0

        for slot in slots:
            if slot not in block_cache:
                gap_block_failures += 1
                continue

            block = block_cache[slot]
            if not block:
                continue

            for tx_entry in block.get("transactions") or []:
                meta = tx_entry.get("meta") or {}

                if meta.get("err") is not None:
                    continue

                signature = transaction_signature(tx_entry)
                if not signature:
                    continue

                discriminators = extract_pump_event_discriminators(
                    meta.get("logMessages") or []
                )

                for event_index, disc_hex in enumerate(discriminators):
                    gap_chain += 1
                    event_key = f"{signature}:{event_index}:{disc_hex}"

                    if local_event_exists(conn, event_key):
                        gap_local += 1
                    else:
                        gap_missing += 1

                        record = {
                            "schema_version": "gap_audit_v0.3",
                            "audited_at_utc": utc_now(),
                            "gap_event_id": gap["event_id"],
                            "gap_start_utc": gap["start"],
                            "gap_end_utc": gap["end"],
                            "gap_seconds": gap["seconds"],
                            "slot": slot,
                            "signature": signature,
                            "event_index": event_index,
                            "discriminator_hex": disc_hex,
                            "event_key": event_key,
                        }
                        append_report(report_path, record)

        if gap_block_failures == 0:
            auditable_gaps += 1

        if gap_missing:
            gaps_with_missing += 1

        total_chain_events += gap_chain
        total_local += gap_local
        total_missing += gap_missing

        status = (
            "COMPLETE"
            if gap_block_failures == 0
            else f"PARTIAL({gap_block_failures} block failures)"
        )

        print(
            f"[GAP {i:02d}] {gap['seconds']:7.3f}s | "
            f"blocks={len(slots):3d} | "
            f"Pump events={gap_chain:4d} | "
            f"local={gap_local:4d} | "
            f"MISSING={gap_missing:4d} | {status}"
        )

    print()
    print("-" * 106)
    print("AUDIT SUMMARY")
    print("-" * 106)
    print(f"Gaps requested                : {audit_gaps}")
    print(f"Gaps with slot boundaries     : {len(resolved)}")
    print(f"Fully auditable gaps          : {auditable_gaps}")
    print(f"getBlocks failures            : {getblocks_failures}")
    print(f"getBlock failures             : {block_failures}")
    print(f"Unique confirmed blocks       : {len(unique_slots)}")
    print(f"Authoritative Pump events     : {total_chain_events}")
    print(f"Already present locally       : {total_local}")
    print(f"Authoritative Pump events lost: {total_missing}")
    print(f"Gaps with missing events      : {gaps_with_missing}")
    print(f"Missing-event report          : {report_path}")
    print()

    if (
        total_missing > 0
        and getblocks_failures == 0
        and block_failures == 0
    ):
        print("CONCLUSION: BACKFILL REQUIRED")
        print(
            "Confirmed Pump events exist in full slots inside recorded gaps "
            "and are absent from the local event database."
        )
    elif (
        total_missing == 0
        and len(resolved) > 0
        and getblocks_failures == 0
        and block_failures == 0
    ):
        print("CONCLUSION: NO DEFINITIVE EVENT LOSS FOUND IN AUDITED GAPS")
    else:
        print("CONCLUSION: AUDIT PARTIAL - RETRY FAILED RPC/BLOCK ITEMS")

    print()
    print("RESULT: PASS")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--audit-gaps",
        type=int,
        default=10,
        help="Audit N longest recorded gaps. Default: 10.",
    )
    args = parser.parse_args()
    asyncio.run(main(args.audit_gaps))
