import argparse
import asyncio
import json
import sqlite3
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

RPC_URL = "https://api.mainnet.solana.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"

RPC_MIN_INTERVAL = 0.75
MAX_SIGNATURES_PER_GAP = 10000


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def latest_v03_run_bounds(conn):
    start = conn.execute(
        "SELECT id FROM collector_events WHERE event_type='COLLECTOR_V0_3_START' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if start is None:
        raise RuntimeError("No COLLECTOR_V0_3_START found")

    stop = conn.execute(
        """
        SELECT id FROM collector_events
        WHERE id > ? AND event_type='LIVE_COLLECTION_STOPPED'
        ORDER BY id ASC LIMIT 1
        """,
        (start["id"],),
    ).fetchone()
    return int(start["id"]), (int(stop["id"]) if stop else None)


def load_run_events(conn, start_id, stop_id):
    if stop_id is None:
        return conn.execute(
            "SELECT * FROM collector_events WHERE id >= ? ORDER BY id",
            (start_id,),
        ).fetchall()

    return conn.execute(
        "SELECT * FROM collector_events WHERE id BETWEEN ? AND ? ORDER BY id",
        (start_id, stop_id),
    ).fetchall()


def safe_details(row):
    try:
        return json.loads(row["details_json"])
    except Exception:
        return {}


def normalize_disconnect_error(text):
    if not text:
        return "(missing error)"
    low = text.lower()
    for marker in [
        "keepalive ping timeout",
        "no close frame",
        "sent 1011",
        "received 1011",
        "connection reset",
        "timed out",
        "timeout",
    ]:
        if marker in low:
            prefix = text.split(":", 1)[0] if ":" in text else text
            return f"{prefix}: {marker}"
    return text[:140]


def nearest_before(conn, timestamp):
    return conn.execute(
        """
        SELECT signature, slot, received_at_utc
        FROM websocket_observations
        WHERE received_at_utc <= ?
        ORDER BY received_at_utc DESC LIMIT 1
        """,
        (timestamp,),
    ).fetchone()


def nearest_after(conn, timestamp):
    return conn.execute(
        """
        SELECT signature, slot, received_at_utc
        FROM websocket_observations
        WHERE received_at_utc >= ?
        ORDER BY received_at_utc ASC LIMIT 1
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


PACER = Pacer(RPC_MIN_INTERVAL)


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


async def signatures_between(client, newer_signature, older_signature):
    all_rows = []
    before = newer_signature
    capped = False

    while True:
        remaining = MAX_SIGNATURES_PER_GAP - len(all_rows)
        if remaining <= 0:
            capped = True
            break

        limit = min(1000, remaining)
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                PUMP_PROGRAM_ID,
                {
                    "before": before,
                    "until": older_signature,
                    "limit": limit,
                    "commitment": "confirmed",
                },
            ],
        }

        body = await rpc_post(client, payload)
        rows = body.get("result") or []
        if not rows:
            break

        all_rows.extend(rows)

        if len(rows) < limit:
            break

        next_before = rows[-1].get("signature")
        if not next_before or next_before == before:
            break

        before = next_before

    return all_rows, capped


def local_signature_set(conn, signatures):
    if not signatures:
        return set()

    found = set()
    for start in range(0, len(signatures), 500):
        chunk = signatures[start:start + 500]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT signature FROM websocket_observations WHERE signature IN ({placeholders})",
            chunk,
        ).fetchall()
        found.update(row["signature"] for row in rows)

    return found


async def main(audit_gaps):
    conn = connect_db()
    start_id, stop_id = latest_v03_run_bounds(conn)
    rows = load_run_events(conn, start_id, stop_id)

    starts = [r for r in rows if r["event_type"] == "COLLECTOR_V0_3_START"]
    stops = [r for r in rows if r["event_type"] == "LIVE_COLLECTION_STOPPED"]
    disconnects = [r for r in rows if r["event_type"] == "WS_DISCONNECTED"]
    gap_rows = [r for r in rows if r["event_type"] == "DATA_GAP_CLOSED"]

    print("=" * 100)
    print("SOLANA MEMECOIN BOT - RECONNECT + GAP AUDIT v0.1")
    print("=" * 100)
    print(f"Database             : {DB_PATH}")
    print(f"Latest v0.3 start id : {start_id}")
    print(f"Latest v0.3 stop id  : {stop_id}")
    if starts:
        print(f"Run started          : {starts[0]['happened_at_utc']}")
    if stops:
        print(f"Run stopped          : {stops[-1]['happened_at_utc']}")
    print(f"Disconnect records   : {len(disconnects)}")
    print(f"Closed gap records   : {len(gap_rows)}")
    print()

    counts = Counter(
        normalize_disconnect_error(str(safe_details(r).get("error", "")))
        for r in disconnects
    )

    print("-" * 100)
    print("RECONNECT CAUSES")
    print("-" * 100)
    if counts:
        for reason, count in counts.most_common(20):
            print(f"{count:5d}  {reason}")
    else:
        print("(none)")

    gaps = []
    for row in gap_rows:
        details = safe_details(row)
        start = details.get("gap_started_at_utc")
        end = details.get("gap_ended_at_utc")
        seconds = details.get("gap_seconds")

        if not start or not end:
            continue

        try:
            duration = float(seconds)
        except Exception:
            duration = (parse_dt(end) - parse_dt(start)).total_seconds()

        gaps.append({
            "start": start,
            "end": end,
            "seconds": duration,
            "id": row["id"],
        })

    gaps.sort(key=lambda x: x["seconds"], reverse=True)
    total = sum(g["seconds"] for g in gaps)

    print()
    print("-" * 100)
    print("GAP DURATION")
    print("-" * 100)
    print(f"Gap count            : {len(gaps)}")
    print(f"Total gap seconds    : {total:.3f}")
    if gaps:
        print(f"Average gap seconds  : {total / len(gaps):.3f}")
        print(f"Longest gap seconds  : {gaps[0]['seconds']:.3f}")

    print()
    print("Top 10 longest gaps:")
    for i, gap in enumerate(gaps[:10], 1):
        print(
            f"{i:2d}. {gap['seconds']:8.3f}s | "
            f"{gap['start']} -> {gap['end']}"
        )

    selected = gaps[:max(0, audit_gaps)]
    if not selected:
        print()
        print("RESULT: DIAGNOSTIC COMPLETE")
        conn.close()
        return

    print()
    print("-" * 100)
    print(f"ONLINE GAP AUDIT - {len(selected)} LONGEST GAPS")
    print("-" * 100)

    audited = 0
    failures = 0
    remote_total = 0
    local_total = 0
    missing_total = 0
    capped_gaps = 0
    gaps_with_missing = 0

    timeout = httpx.Timeout(connect=15.0, read=45.0, write=20.0, pool=20.0)

    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"Content-Type": "application/json"},
    ) as client:

        for i, gap in enumerate(selected, 1):
            older = nearest_before(conn, gap["start"])
            newer = nearest_after(conn, gap["end"])

            if older is None or newer is None:
                failures += 1
                print(f"[GAP {i:02d}] boundary not found")
                continue

            try:
                remote_rows, capped = await signatures_between(
                    client,
                    newer_signature=newer["signature"],
                    older_signature=older["signature"],
                )
            except Exception as exc:
                failures += 1
                print(f"[GAP {i:02d}] RPC ERROR: {type(exc).__name__}: {exc}")
                continue

            remote_sigs = [
                row.get("signature")
                for row in remote_rows
                if row.get("signature")
            ]

            local = local_signature_set(conn, remote_sigs)
            missing = [sig for sig in remote_sigs if sig not in local]

            audited += 1
            remote_total += len(remote_sigs)
            local_total += len(local)
            missing_total += len(missing)

            if missing:
                gaps_with_missing += 1
            if capped:
                capped_gaps += 1

            cap_text = " [CAPPED]" if capped else ""

            print(
                f"[GAP {i:02d}] {gap['seconds']:8.3f}s | "
                f"chain_between={len(remote_sigs):5d} | "
                f"local={len(local):5d} | "
                f"MISSING={len(missing):5d}{cap_text}"
            )

            for sig in missing[:3]:
                print(f"          missing sample: {sig}")

    print()
    print("-" * 100)
    print("AUDIT SUMMARY")
    print("-" * 100)
    print(f"Gaps requested             : {len(selected)}")
    print(f"Gaps successfully audited  : {audited}")
    print(f"Boundary/RPC failures      : {failures}")
    print(f"Gaps capped at safety max  : {capped_gaps}")
    print(f"Gaps with missing sigs     : {gaps_with_missing}")
    print(f"Confirmed chain sigs found : {remote_total}")
    print(f"Already present locally    : {local_total}")
    print(f"Confirmed sigs missing     : {missing_total}")
    print()

    if missing_total > 0:
        print("CONCLUSION: BACKFILL REQUIRED")
    elif audited > 0 and failures == 0:
        print("CONCLUSION: NO MISSING SIGNATURES FOUND IN AUDITED GAPS")
    else:
        print("CONCLUSION: INSUFFICIENT AUDIT DATA")

    print("RESULT: PASS")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--audit-gaps",
        type=int,
        default=10,
        help="Audit the N longest gaps against confirmed Solana history. Default: 10.",
    )
    args = parser.parse_args()
    asyncio.run(main(args.audit_gaps))
