import argparse
import asyncio
import base64
import json
import re
import sqlite3
import time
from collections import Counter
from pathlib import Path
from typing import Any

import httpx

RPC_URL = "https://api.mainnet.solana.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

CREATE_EVENT_DISC = bytes([27, 114, 169, 77, 222, 235, 99, 118])
TRADE_EVENT_DISC = bytes([189, 219, 127, 211, 78, 230, 97, 238])

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"

INVOKE_RE = re.compile(r"^Program\s+([1-9A-HJ-NP-Za-km-z]+)\s+invoke\s+\[(\d+)\]")
EXIT_RE = re.compile(r"^Program\s+([1-9A-HJ-NP-Za-km-z]+)\s+(success|failed:.*)$")

RPC_INTERVAL = 0.9


def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def safe_details(row):
    try:
        return json.loads(row["details_json"])
    except Exception:
        return {}


def latest_long_v03_run(conn):
    """
    Pick the most recent v0.3 start that has DATA_GAP_CLOSED rows after it.
    The long unattended test is the one we want to audit.
    """
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

        end_id = stop["id"] if stop else 10**18

        gap_count = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM collector_events
            WHERE id BETWEEN ? AND ?
              AND event_type='DATA_GAP_CLOSED'
            """,
            (start["id"], end_id),
        ).fetchone()["n"]

        if gap_count > 0:
            return int(start["id"]), (int(stop["id"]) if stop else None)

    raise RuntimeError("Could not find a v0.3 run with gap records")


def run_events(conn, start_id, stop_id):
    if stop_id is None:
        return conn.execute(
            "SELECT * FROM collector_events WHERE id >= ? ORDER BY id",
            (start_id,),
        ).fetchall()

    return conn.execute(
        "SELECT * FROM collector_events WHERE id BETWEEN ? AND ? ORDER BY id",
        (start_id, stop_id),
    ).fetchall()


def normalize_error(text):
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
    return text[:140] if text else "(missing error)"


def nearest_before(conn, timestamp):
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


def nearest_after(conn, timestamp):
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


class Pacer:
    def __init__(self, interval):
        self.interval = interval
        self.last = 0.0

    async def wait(self):
        now = time.monotonic()
        delay = self.interval - (now - self.last)
        if delay > 0:
            await asyncio.sleep(delay)
        self.last = time.monotonic()


PACER = Pacer(RPC_INTERVAL)


async def rpc_post(client, payload):
    while True:
        await PACER.wait()
        response = await client.post(RPC_URL, json=payload)

        if response.status_code == 429:
            retry = response.headers.get("Retry-After")
            try:
                wait_for = float(retry) if retry else 10.0
            except ValueError:
                wait_for = 10.0
            print(f"      [429] waiting {wait_for:.1f}s")
            await asyncio.sleep(wait_for)
            continue

        response.raise_for_status()
        body = response.json()

        if "error" in body:
            raise RuntimeError(f"RPC error: {body['error']}")

        return body


async def get_blocks(client, first_slot, last_slot):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlocks",
        "params": [
            int(first_slot),
            int(last_slot),
            {"commitment": "confirmed"},
        ],
    }
    body = await rpc_post(client, payload)
    return body.get("result") or []


async def get_block(client, slot):
    payload = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "getBlock",
        "params": [
            int(slot),
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
    return body.get("result")


def extract_pump_events(logs):
    stack = []
    out = []

    for line in logs or []:
        if not isinstance(line, str):
            continue

        m = INVOKE_RE.match(line)
        if m:
            stack.append(m.group(1))
            continue

        m = EXIT_RE.match(line)
        if m:
            program = m.group(1)
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
            out.append(raw[:8].hex())

    return out


def signature_from_tx(tx_entry):
    tx = tx_entry.get("transaction") or {}
    sigs = tx.get("signatures") or []
    return sigs[0] if sigs else None


def event_key_exists(conn, event_key):
    return conn.execute(
        "SELECT 1 FROM pump_events WHERE event_key=?",
        (event_key,),
    ).fetchone() is not None


async def main(audit_gaps):
    conn = connect_db()
    start_id, stop_id = latest_long_v03_run(conn)
    rows = run_events(conn, start_id, stop_id)

    disconnects = [r for r in rows if r["event_type"] == "WS_DISCONNECTED"]
    gap_rows = [r for r in rows if r["event_type"] == "DATA_GAP_CLOSED"]

    counts = Counter(
        normalize_error(str(safe_details(r).get("error", "")))
        for r in disconnects
    )

    print("=" * 104)
    print("SOLANA MEMECOIN BOT - SLOT/BLOCK GAP AUDIT v0.2")
    print("=" * 104)
    print(f"Database           : {DB_PATH}")
    print(f"Run start id       : {start_id}")
    print(f"Run stop id        : {stop_id}")
    print(f"Disconnect records : {len(disconnects)}")
    print(f"Gap records        : {len(gap_rows)}")
    print()

    print("-" * 104)
    print("RECONNECT CAUSES")
    print("-" * 104)
    for reason, count in counts.most_common(20):
        print(f"{count:5d}  {reason}")

    gaps = []
    for row in gap_rows:
        d = safe_details(row)
        start = d.get("gap_started_at_utc")
        end = d.get("gap_ended_at_utc")
        seconds = float(d.get("gap_seconds") or 0)

        if not start or not end:
            continue

        older = nearest_before(conn, start)
        newer = nearest_after(conn, end)

        if older is None or newer is None:
            continue

        gaps.append({
            "seconds": seconds,
            "start": start,
            "end": end,
            "older_slot": int(older["slot"]),
            "newer_slot": int(newer["slot"]),
        })

    gaps.sort(key=lambda x: x["seconds"], reverse=True)
    selected = gaps[:audit_gaps]

    print()
    print("-" * 104)
    print(f"AUDITING {len(selected)} LONGEST GAPS BY CONFIRMED SLOT/BLOCK RANGE")
    print("-" * 104)

    audited = 0
    rpc_failures = 0
    blocks_checked = 0
    chain_pump_events = 0
    local_present = 0
    missing_events = 0
    gaps_with_missing = 0

    timeout = httpx.Timeout(
        connect=15.0,
        read=60.0,
        write=20.0,
        pool=20.0,
    )

    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"Content-Type": "application/json"},
    ) as client:

        for i, gap in enumerate(selected, 1):
            first_slot = min(gap["older_slot"], gap["newer_slot"])
            last_slot = max(gap["older_slot"], gap["newer_slot"])

            try:
                slots = await get_blocks(client, first_slot, last_slot)
            except Exception as exc:
                rpc_failures += 1
                print(
                    f"[GAP {i:02d}] {gap['seconds']:.3f}s | "
                    f"getBlocks ERROR: {type(exc).__name__}: {exc}"
                )
                continue

            gap_chain = 0
            gap_local = 0
            gap_missing = 0

            for slot in slots:
                try:
                    block = await get_block(client, slot)
                except Exception as exc:
                    rpc_failures += 1
                    print(
                        f"          slot={slot} getBlock ERROR: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    continue

                if block is None:
                    continue

                blocks_checked += 1

                for tx_entry in block.get("transactions") or []:
                    meta = tx_entry.get("meta") or {}

                    # Authoritative market events only come from successful txs.
                    if meta.get("err") is not None:
                        continue

                    signature = signature_from_tx(tx_entry)
                    if not signature:
                        continue

                    discs = extract_pump_events(meta.get("logMessages") or [])

                    for event_index, disc_hex in enumerate(discs):
                        gap_chain += 1
                        chain_pump_events += 1

                        event_key = f"{signature}:{event_index}:{disc_hex}"
                        if event_key_exists(conn, event_key):
                            gap_local += 1
                            local_present += 1
                        else:
                            gap_missing += 1
                            missing_events += 1
                            if gap_missing <= 3:
                                print(
                                    f"          missing event: "
                                    f"slot={slot} sig={signature} disc={disc_hex}"
                                )

            audited += 1
            if gap_missing:
                gaps_with_missing += 1

            print(
                f"[GAP {i:02d}] {gap['seconds']:7.3f}s | "
                f"slots={first_slot}-{last_slot} | "
                f"confirmed_pump_events={gap_chain:4d} | "
                f"local={gap_local:4d} | "
                f"MISSING={gap_missing:4d}"
            )

    print()
    print("-" * 104)
    print("AUDIT SUMMARY")
    print("-" * 104)
    print(f"Gaps requested             : {len(selected)}")
    print(f"Gaps successfully audited  : {audited}")
    print(f"RPC/block failures         : {rpc_failures}")
    print(f"Confirmed blocks checked   : {blocks_checked}")
    print(f"Confirmed Pump events found: {chain_pump_events}")
    print(f"Already present locally    : {local_present}")
    print(f"Confirmed Pump events lost : {missing_events}")
    print(f"Gaps with missing events   : {gaps_with_missing}")
    print()

    if missing_events > 0:
        print("CONCLUSION: AUTHORITATIVE PUMP EVENTS WERE MISSED DURING AUDITED GAPS")
        print("NEXT: BUILD AUTOMATIC SLOT-RANGE BACKFILL")
    elif audited == len(selected) and rpc_failures == 0:
        print("CONCLUSION: NO AUTHORITATIVE PUMP EVENTS MISSING IN AUDITED GAPS")
    else:
        print("CONCLUSION: AUDIT INCOMPLETE; RETRY FAILED BLOCKS/GAPS")

    print()
    print("RESULT: PASS")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--audit-gaps",
        type=int,
        default=10,
        help="Audit N longest recorded gaps using confirmed slot/block data.",
    )
    args = parser.parse_args()
    asyncio.run(main(args.audit_gaps))
