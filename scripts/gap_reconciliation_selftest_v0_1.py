import asyncio
import base64
import json
import sqlite3
import struct
import sys
from datetime import datetime, timezone
from pathlib import Path

# When this file lives in scripts\, Python will find the collector beside it.
import live_pump_collector_v0_3_3 as collector


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SELFTEST_DIR = PROJECT_ROOT / "data" / "selftest"
TEST_DB = SELFTEST_DIR / "gap_reconciliation_selftest.sqlite3"

TEST_SLOT = 123456789
BEFORE_SLOT = TEST_SLOT - 1
AFTER_SLOT = TEST_SLOT + 1
TEST_SIGNATURE = "SELFTEST_SIG_GAP_RECOVERY_001"

PUMP_PROGRAM_ID = collector.PUMP_PROGRAM_ID
TRADE_DISC = collector.TRADE_EVENT_DISC


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_trade_payload() -> bytes:
    """
    Build the stable/core portion of Pump TradeEvent.

    decode_trade_event() accepts this because the optional newer tail is
    intentionally decoded opportunistically and safely stops when absent.
    """
    mint = bytes(range(1, 33))
    user = bytes(range(33, 65))

    sol_lamports = 250_000_000          # 0.25 SOL
    token_amount_raw = 123_456_789
    is_buy = 1
    timestamp = 1_750_000_000
    virtual_sol_reserves = 30_000_000_000
    virtual_token_reserves = 1_000_000_000_000

    return b"".join(
        [
            TRADE_DISC,
            mint,
            struct.pack("<Q", sol_lamports),
            struct.pack("<Q", token_amount_raw),
            bytes([is_buy]),
            user,
            struct.pack("<q", timestamp),
            struct.pack("<Q", virtual_sol_reserves),
            struct.pack("<Q", virtual_token_reserves),
        ]
    )


def make_fake_confirmed_block() -> dict:
    payload = make_trade_payload()
    encoded = base64.b64encode(payload).decode("ascii")

    logs = [
        f"Program {PUMP_PROGRAM_ID} invoke [1]",
        f"Program data: {encoded}",
        f"Program {PUMP_PROGRAM_ID} success",
    ]

    return {
        "blockTime": 1_750_000_001,
        "transactions": [
            {
                "transaction": {
                    "signatures": [TEST_SIGNATURE],
                },
                "meta": {
                    "err": None,
                    "logMessages": logs,
                },
            }
        ],
    }


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(TEST_DB)
    conn.row_factory = sqlite3.Row
    return conn


def reset_test_db() -> None:
    SELFTEST_DIR.mkdir(parents=True, exist_ok=True)

    for suffix in ("", "-wal", "-shm"):
        path = Path(str(TEST_DB) + suffix)
        if path.exists():
            path.unlink()

    # Redirect ALL collector DB work to the isolated self-test DB.
    collector.DB_PATH = TEST_DB

    conn = collector.connect_db()
    collector.ensure_schema(conn)
    conn.close()


async def fake_get_confirmed_blocks(client, start_slot: int, end_slot: int):
    assert start_slot == TEST_SLOT
    assert end_slot == TEST_SLOT
    return [TEST_SLOT]


async def fake_get_confirmed_block(client, slot: int):
    assert slot == TEST_SLOT
    return make_fake_confirmed_block()


async def wait_until_done(gap_key: str, timeout_seconds: float = 8.0) -> sqlite3.Row:
    deadline = asyncio.get_running_loop().time() + timeout_seconds

    while asyncio.get_running_loop().time() < deadline:
        conn = db_connect()
        row = conn.execute(
            "SELECT * FROM gap_jobs WHERE gap_key=?",
            (gap_key,),
        ).fetchone()
        conn.close()

        if row is not None and str(row["status"]).startswith("DONE"):
            return row

        await asyncio.sleep(0.05)

    raise TimeoutError(f"Gap job did not finish within {timeout_seconds}s: {gap_key}")


async def run_worker_until_gap_done(gap_key: str, stats: dict) -> sqlite3.Row:
    workers_stop = asyncio.Event()

    task = asyncio.create_task(
        collector.gap_reconciliation_worker(workers_stop, stats)
    )

    try:
        row = await wait_until_done(gap_key)
    finally:
        workers_stop.set()
        await asyncio.wait_for(task, timeout=5.0)

    return row


def fresh_stats() -> dict:
    return {
        "gap_jobs_oversize": 0,
        "gap_events_recovered": 0,
        "gap_jobs_completed": 0,
        "gap_blocks_checked": 0,
        "gap_reconciliation_errors": 0,
        "rpc_429_count": 0,
    }


def event_count() -> int:
    conn = db_connect()
    count = conn.execute("SELECT COUNT(*) FROM pump_events").fetchone()[0]
    conn.close()
    return int(count)


def event_snapshot() -> sqlite3.Row | None:
    conn = db_connect()
    row = conn.execute(
        """
        SELECT event_key, signature, event_type, slot,
               sol_amount, source_decoded_file, confirmation_status
        FROM pump_events
        ORDER BY rowid
        LIMIT 1
        """
    ).fetchone()
    conn.close()
    return row


async def main() -> None:
    print("=" * 92)
    print("SOLANA MEMECOIN BOT - DETERMINISTIC GAP RECOVERY SELF-TEST v0.1")
    print("=" * 92)
    print(f"Test DB : {TEST_DB}")
    print("Network : DISABLED / SYNTHETIC CONFIRMED BLOCK")
    print("Prod DB : NOT TOUCHED")
    print()

    reset_test_db()

    # Monkeypatch only the two block-fetch functions.
    collector.get_confirmed_blocks = fake_get_confirmed_blocks
    collector.get_confirmed_block = fake_get_confirmed_block

    conn = collector.connect_db()

    gap_key = collector.enqueue_gap_job(
        conn,
        gap_started_at_utc="2026-08-18T20:00:00+00:00",
        gap_ended_at_utc="2026-08-18T20:00:01+00:00",
        before_signature="SELFTEST_BEFORE",
        after_signature="SELFTEST_AFTER",
        before_slot=BEFORE_SLOT,
        after_slot=AFTER_SLOT,
    )
    conn.close()

    print("[TEST 1] Synthetic gap recovery")
    stats1 = fresh_stats()
    row1 = await run_worker_until_gap_done(gap_key, stats1)

    snap = event_snapshot()
    count1 = event_count()

    test1_ok = (
        row1["status"] == "DONE"
        and int(row1["confirmed_blocks_checked"]) == 1
        and int(row1["events_recovered"]) == 1
        and count1 == 1
        and snap is not None
        and snap["signature"] == TEST_SIGNATURE
        and snap["event_type"] == "BUY"
        and int(snap["slot"]) == TEST_SLOT
        and snap["source_decoded_file"] == "GAP_RECONCILIATION_V0_3_3"
        and snap["confirmation_status"] == "confirmed"
    )

    print(f"         gap status          : {row1['status']}")
    print(f"         blocks checked      : {row1['confirmed_blocks_checked']}")
    print(f"         events recovered    : {row1['events_recovered']}")
    print(f"         DB Pump events      : {count1}")
    print(f"         decoded event type  : {snap['event_type'] if snap else None}")
    print(f"         decoded SOL         : {snap['sol_amount'] if snap else None}")
    print(f"         RESULT              : {'PASS' if test1_ok else 'FAIL'}")
    print()

    # TEST 2: force the same job back into PENDING. The block is identical.
    # insert_event() must reject the same event_key, keeping pump_events at 1.
    print("[TEST 2] Persistent event dedup on gap replay")
    conn = db_connect()
    conn.execute(
        """
        UPDATE gap_jobs
        SET status='PENDING',
            attempts=0,
            next_retry_at_utc=?,
            last_error=NULL,
            confirmed_blocks_checked=0,
            events_recovered=0,
            completed_at_utc=NULL,
            updated_at_utc=?
        WHERE gap_key=?
        """,
        (utc_now(), utc_now(), gap_key),
    )
    conn.commit()
    conn.close()

    stats2 = fresh_stats()
    row2 = await run_worker_until_gap_done(gap_key, stats2)
    count2 = event_count()

    test2_ok = (
        row2["status"] == "DONE"
        and int(row2["confirmed_blocks_checked"]) == 1
        and int(row2["events_recovered"]) == 0
        and count2 == 1
    )

    print(f"         gap status          : {row2['status']}")
    print(f"         replay recovered    : {row2['events_recovered']}")
    print(f"         DB Pump events      : {count2}")
    print(f"         RESULT              : {'PASS' if test2_ok else 'FAIL'}")
    print()

    # TEST 3: simulate process death while job is IN_PROGRESS.
    # recover_gap_jobs() must convert it to RETRY, after which the worker
    # can safely complete it again without creating duplicates.
    print("[TEST 3] Restart-safe IN_PROGRESS recovery")
    conn = db_connect()
    conn.execute(
        """
        UPDATE gap_jobs
        SET status='IN_PROGRESS',
            attempts=1,
            next_retry_at_utc=NULL,
            last_error=NULL,
            completed_at_utc=NULL,
            updated_at_utc=?
        WHERE gap_key=?
        """,
        (utc_now(), gap_key),
    )
    conn.commit()
    conn.close()

    # Simulate startup recovery.
    conn = collector.connect_db()
    recovered_jobs = collector.recover_gap_jobs(conn)
    recovered_row = conn.execute(
        "SELECT status FROM gap_jobs WHERE gap_key=?",
        (gap_key,),
    ).fetchone()
    conn.close()

    recovered_to_retry = (
        recovered_jobs >= 1
        and recovered_row is not None
        and recovered_row["status"] == "RETRY"
    )

    stats3 = fresh_stats()
    row3 = await run_worker_until_gap_done(gap_key, stats3)
    count3 = event_count()

    test3_ok = (
        recovered_to_retry
        and row3["status"] == "DONE"
        and count3 == 1
    )

    print(f"         startup jobs reset  : {recovered_jobs}")
    print(f"         recovered status    : {recovered_row['status'] if recovered_row else None}")
    print(f"         final gap status    : {row3['status']}")
    print(f"         DB Pump events      : {count3}")
    print(f"         RESULT              : {'PASS' if test3_ok else 'FAIL'}")
    print()

    all_ok = test1_ok and test2_ok and test3_ok

    print("-" * 92)
    print("SUMMARY")
    print("-" * 92)
    print(f"Synthetic event recovered   : {'PASS' if test1_ok else 'FAIL'}")
    print(f"Replay dedup                 : {'PASS' if test2_ok else 'FAIL'}")
    print(f"Restart-safe gap queue       : {'PASS' if test3_ok else 'FAIL'}")
    print(f"Production DB touched        : NO")
    print(f"Final self-test Pump events  : {count3}")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'FAIL'}")

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
