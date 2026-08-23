import argparse
import asyncio
import json
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


HTTP_RPC_URL = "https://api.mainnet.solana.com"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DB_DIR = DATA_DIR / "db"
BACKUP_DIR = DB_DIR / "backups"
DB_PATH = DB_DIR / "tradingbot.sqlite3"

CONFIRM_BATCH_SIZE = 8
MAX_CONFIRM_ROUNDS = 5
MAX_DEEP_ATTEMPTS = 5
MIN_HTTP_INTERVAL = 1.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def timestamp_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (name,),
    ).fetchone()
    return row is not None


def ensure_housekeeping_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS housekeeping_job_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_table TEXT NOT NULL,
            signature TEXT NOT NULL,
            original_status TEXT,
            reason TEXT NOT NULL,
            archived_at_utc TEXT NOT NULL,
            snapshot_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_housekeeping_archive_signature
            ON housekeeping_job_archive(signature);

        CREATE TABLE IF NOT EXISTS housekeeping_runs (
            run_id TEXT PRIMARY KEY,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT,
            result TEXT,
            summary_json TEXT
        );
        """
    )
    conn.commit()


def backup_database(db_path: Path) -> Path:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backup_path = BACKUP_DIR / f"tradingbot_before_housekeeping_{timestamp_tag()}.sqlite3"

    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(backup_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()

    return backup_path


def start_run(conn: sqlite3.Connection, run_id: str) -> None:
    conn.execute(
        """
        INSERT INTO housekeeping_runs(
            run_id, started_at_utc, finished_at_utc, result, summary_json
        )
        VALUES (?, ?, NULL, NULL, NULL)
        """,
        (run_id, utc_now()),
    )
    conn.commit()


def finish_run(
    conn: sqlite3.Connection,
    run_id: str,
    result: str,
    summary: dict[str, Any],
) -> None:
    conn.execute(
        """
        UPDATE housekeeping_runs
        SET finished_at_utc=?,
            result=?,
            summary_json=?
        WHERE run_id=?
        """,
        (
            utc_now(),
            result,
            json.dumps(summary, ensure_ascii=False, separators=(",", ":")),
            run_id,
        ),
    )
    conn.commit()


def archive_row(
    conn: sqlite3.Connection,
    source_table: str,
    row: sqlite3.Row,
    reason: str,
) -> None:
    snapshot = {key: row[key] for key in row.keys()}
    conn.execute(
        """
        INSERT INTO housekeeping_job_archive(
            source_table,
            signature,
            original_status,
            reason,
            archived_at_utc,
            snapshot_json
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            source_table,
            row["signature"],
            row["status"] if "status" in row.keys() else None,
            reason,
            utc_now(),
            json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")),
        ),
    )


def archive_legacy_v02_jobs(conn: sqlite3.Connection) -> int:
    if not table_exists(conn, "enrichment_jobs"):
        return 0

    rows = conn.execute(
        """
        SELECT *
        FROM enrichment_jobs
        WHERE status != 'DONE'
        ORDER BY created_at_utc, signature
        """
    ).fetchall()

    for row in rows:
        archive_row(
            conn,
            "enrichment_jobs",
            row,
            "Legacy v0.2 queue retired after v0.3 confirmation architecture replaced it.",
        )
        conn.execute(
            """
            UPDATE enrichment_jobs
            SET status='ARCHIVED_LEGACY_V0_2',
                last_error='Archived by housekeeping v0.3.1; replaced by v0.3 queue architecture.',
                updated_at_utc=?
            WHERE signature=?
            """,
            (utc_now(), row["signature"]),
        )

    conn.commit()
    return len(rows)


def active_confirmation_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    if not table_exists(conn, "confirmation_jobs"):
        return []

    return conn.execute(
        """
        SELECT *
        FROM confirmation_jobs
        WHERE status != 'DONE'
          AND status NOT LIKE 'ARCHIVED%'
        ORDER BY created_at_utc, attempts, signature
        """
    ).fetchall()


def active_deep_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    if not table_exists(conn, "deep_enrichment_jobs"):
        return []

    return conn.execute(
        """
        SELECT *
        FROM deep_enrichment_jobs
        WHERE status != 'DONE'
          AND status NOT LIKE 'ARCHIVED%'
        ORDER BY created_at_utc, attempts, signature
        """
    ).fetchall()


class RpcPacer:
    def __init__(self, interval: float):
        self.interval = interval
        self.last_request = 0.0

    async def wait(self) -> None:
        now = time.monotonic()
        wait_for = self.interval - (now - self.last_request)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        self.last_request = time.monotonic()


PACER = RpcPacer(MIN_HTTP_INTERVAL)


async def rpc_post(
    client: httpx.AsyncClient,
    payload: dict[str, Any],
) -> dict[str, Any]:
    await PACER.wait()
    response = await client.post(HTTP_RPC_URL, json=payload)

    if response.status_code == 429:
        retry_header = response.headers.get("Retry-After")
        try:
            retry_after = float(retry_header) if retry_header else 10.0
        except ValueError:
            retry_after = 10.0
        raise RuntimeError(f"HTTP_429:{retry_after}")

    response.raise_for_status()
    body = response.json()

    if "error" in body:
        raise RuntimeError(f"RPC_ERROR:{body['error']}")

    return body


async def get_signature_statuses(
    client: httpx.AsyncClient,
    signatures: list[str],
) -> list[Any]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignatureStatuses",
        "params": [
            signatures,
            {"searchTransactionHistory": True},
        ],
    }
    body = await rpc_post(client, payload)
    result = body.get("result") or {}
    values = result.get("value") or []

    if len(values) != len(signatures):
        raise RuntimeError(
            f"Status length mismatch: got {len(values)}, expected {len(signatures)}"
        )

    return values


async def get_transaction(
    client: httpx.AsyncClient,
    signature: str,
) -> dict[str, Any] | None:
    payload = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "commitment": "confirmed",
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
            },
        ],
    }
    body = await rpc_post(client, payload)
    return body.get("result")


def mark_confirmation_done(
    conn: sqlite3.Connection,
    signature: str,
    status: str,
    slot: int | None,
) -> None:
    now = utc_now()

    conn.execute(
        """
        UPDATE confirmation_jobs
        SET status='DONE',
            next_poll_at_utc=NULL,
            last_error=NULL,
            updated_at_utc=?
        WHERE signature=?
        """,
        (now, signature),
    )

    conn.execute(
        """
        UPDATE transactions
        SET confirmation_status=?,
            confirmed_at_utc=?,
            slot=COALESCE(?, slot)
        WHERE signature=?
        """,
        (status, now, slot, signature),
    )

    conn.execute(
        """
        UPDATE pump_events
        SET confirmation_status=?,
            confirmed_at_utc=?,
            slot=COALESCE(?, slot)
        WHERE signature=?
        """,
        (status, now, slot, signature),
    )

    conn.commit()


def mark_confirmation_failed(
    conn: sqlite3.Connection,
    signature: str,
    slot: int | None,
    error: Any,
) -> None:
    now = utc_now()
    error_json = json.dumps(error, ensure_ascii=False, separators=(",", ":"))

    conn.execute(
        """
        UPDATE confirmation_jobs
        SET status='DONE',
            next_poll_at_utc=NULL,
            last_error=?,
            updated_at_utc=?
        WHERE signature=?
        """,
        (error_json, now, signature),
    )

    conn.execute(
        """
        UPDATE transactions
        SET confirmation_status='failed',
            transaction_error_json=?,
            confirmed_at_utc=?,
            slot=COALESCE(?, slot)
        WHERE signature=?
        """,
        (error_json, now, slot, signature),
    )

    conn.execute(
        """
        UPDATE pump_events
        SET confirmation_status='failed',
            confirmed_at_utc=?,
            slot=COALESCE(?, slot)
        WHERE signature=?
        """,
        (now, slot, signature),
    )

    conn.commit()


def archive_confirmation_unresolved(
    conn: sqlite3.Connection,
    signature: str,
    reason: str,
) -> None:
    row = conn.execute(
        "SELECT * FROM confirmation_jobs WHERE signature=?",
        (signature,),
    ).fetchone()

    if row is None:
        return

    archive_row(conn, "confirmation_jobs", row, reason)

    conn.execute(
        """
        UPDATE confirmation_jobs
        SET status='ARCHIVED_UNRESOLVED',
            next_poll_at_utc=NULL,
            last_error=?,
            updated_at_utc=?
        WHERE signature=?
        """,
        (reason[:1000], utc_now(), signature),
    )

    conn.commit()


async def clean_confirmation_jobs(
    conn: sqlite3.Connection,
    client: httpx.AsyncClient,
    stats: dict[str, int],
) -> None:
    for round_number in range(1, MAX_CONFIRM_ROUNDS + 1):
        rows = active_confirmation_rows(conn)

        if not rows:
            return

        print(
            f"[CONFIRM ROUND {round_number}/{MAX_CONFIRM_ROUNDS}] "
            f"active={len(rows)}"
        )

        any_pending = False

        for start in range(0, len(rows), CONFIRM_BATCH_SIZE):
            batch_rows = rows[start:start + CONFIRM_BATCH_SIZE]
            signatures = [row["signature"] for row in batch_rows]

            try:
                statuses = await get_signature_statuses(client, signatures)
                stats["status_rpc_calls"] += 1
                stats["status_signatures_checked"] += len(signatures)

            except Exception as exc:
                stats["status_rpc_errors"] += 1
                text = f"{type(exc).__name__}: {exc}"
                print(f"  [WARN] status batch failed: {text}")

                if str(exc).startswith("HTTP_429:"):
                    stats["rpc_429"] += 1
                    try:
                        wait_for = float(str(exc).split(":", 1)[1])
                    except Exception:
                        wait_for = 10.0
                    print(f"  [INFO] RPC cooldown {wait_for:.1f}s")
                    await asyncio.sleep(wait_for)
                else:
                    await asyncio.sleep(2.0)

                any_pending = True
                continue

            for signature, status in zip(signatures, statuses):
                if status is None:
                    stats["status_null"] += 1
                    any_pending = True
                    continue

                error = status.get("err")
                slot = status.get("slot")
                confirmation_status = status.get("confirmationStatus")

                if error is not None:
                    mark_confirmation_failed(
                        conn,
                        signature,
                        slot,
                        error,
                    )
                    stats["confirmation_failed"] += 1
                    continue

                if confirmation_status in ("confirmed", "finalized"):
                    mark_confirmation_done(
                        conn,
                        signature,
                        confirmation_status,
                        slot,
                    )
                    stats["confirmation_done"] += 1
                    continue

                any_pending = True

        if not any_pending:
            return

        if round_number < MAX_CONFIRM_ROUNDS:
            await asyncio.sleep(2.0)

    # Anything still active is preserved in the archive rather than deleted.
    unresolved = active_confirmation_rows(conn)

    for row in unresolved:
        archive_confirmation_unresolved(
            conn,
            row["signature"],
            (
                "Housekeeping v0.3.1 could not obtain confirmed/finalized status "
                f"after {MAX_CONFIRM_ROUNDS} controlled rounds."
            ),
        )
        stats["confirmation_archived"] += 1


def account_pubkey(entry: Any) -> str | None:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        value = entry.get("pubkey")
        return str(value) if value is not None else None
    return None


def save_deep_transaction(
    conn: sqlite3.Connection,
    signature: str,
    result: dict[str, Any],
) -> None:
    meta = result.get("meta") or {}
    tx = result.get("transaction") or {}
    message = tx.get("message") or {}
    keys = message.get("accountKeys") or []
    fee_payer = account_pubkey(keys[0]) if keys else None

    conn.execute(
        """
        UPDATE transactions
        SET slot=?,
            block_time=?,
            fee_payer=?,
            transaction_error_json=?,
            fee_lamports=?,
            enriched_at_utc=?,
            raw_get_transaction_json=?
        WHERE signature=?
        """,
        (
            result.get("slot"),
            result.get("blockTime"),
            fee_payer,
            None
            if meta.get("err") is None
            else json.dumps(meta.get("err"), separators=(",", ":")),
            meta.get("fee"),
            utc_now(),
            json.dumps(result, ensure_ascii=False, separators=(",", ":")),
            signature,
        ),
    )

    conn.execute(
        """
        UPDATE pump_events
        SET block_time=?,
            fee_payer=COALESCE(fee_payer, ?)
        WHERE signature=?
        """,
        (
            result.get("blockTime"),
            fee_payer,
            signature,
        ),
    )

    conn.commit()


def mark_deep_done(
    conn: sqlite3.Connection,
    signature: str,
) -> None:
    conn.execute(
        """
        UPDATE deep_enrichment_jobs
        SET status='DONE',
            next_retry_at_utc=NULL,
            last_error=NULL,
            updated_at_utc=?
        WHERE signature=?
        """,
        (utc_now(), signature),
    )
    conn.commit()


def archive_deep_unresolved(
    conn: sqlite3.Connection,
    signature: str,
    reason: str,
) -> None:
    row = conn.execute(
        "SELECT * FROM deep_enrichment_jobs WHERE signature=?",
        (signature,),
    ).fetchone()

    if row is None:
        return

    archive_row(conn, "deep_enrichment_jobs", row, reason)

    conn.execute(
        """
        UPDATE deep_enrichment_jobs
        SET status='ARCHIVED_UNRESOLVED',
            next_retry_at_utc=NULL,
            last_error=?,
            updated_at_utc=?
        WHERE signature=?
        """,
        (reason[:1000], utc_now(), signature),
    )

    conn.commit()


async def clean_deep_jobs(
    conn: sqlite3.Connection,
    client: httpx.AsyncClient,
    stats: dict[str, int],
) -> None:
    rows = active_deep_rows(conn)

    for index, row in enumerate(rows, start=1):
        signature = row["signature"]
        print(
            f"[DEEP {index}/{len(rows)}] "
            f"signature={signature}"
        )

        success = False

        for attempt in range(1, MAX_DEEP_ATTEMPTS + 1):
            try:
                result = await get_transaction(client, signature)
                stats["deep_rpc_calls"] += 1

                if result is None:
                    stats["deep_null"] += 1
                    print(
                        f"  [WAIT] getTransaction null "
                        f"(attempt {attempt}/{MAX_DEEP_ATTEMPTS})"
                    )
                    await asyncio.sleep(2.0)
                    continue

                save_deep_transaction(conn, signature, result)
                mark_deep_done(conn, signature)
                stats["deep_done"] += 1
                success = True
                print("  [OK] deep enrichment stored")
                break

            except Exception as exc:
                stats["deep_rpc_errors"] += 1
                text = f"{type(exc).__name__}: {exc}"
                print(
                    f"  [WARN] deep attempt "
                    f"{attempt}/{MAX_DEEP_ATTEMPTS}: {text}"
                )

                if str(exc).startswith("HTTP_429:"):
                    stats["rpc_429"] += 1
                    try:
                        wait_for = float(str(exc).split(":", 1)[1])
                    except Exception:
                        wait_for = 10.0
                    await asyncio.sleep(wait_for)
                else:
                    await asyncio.sleep(2.0)

        if not success:
            archive_deep_unresolved(
                conn,
                signature,
                (
                    "Housekeeping v0.3.1 could not complete deep enrichment "
                    f"after {MAX_DEEP_ATTEMPTS} controlled attempts."
                ),
            )
            stats["deep_archived"] += 1


def active_counts(conn: sqlite3.Connection) -> tuple[int, int, int]:
    confirmation = len(active_confirmation_rows(conn))
    deep = len(active_deep_rows(conn))

    legacy = 0
    if table_exists(conn, "enrichment_jobs"):
        legacy = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM enrichment_jobs
            WHERE status != 'DONE'
              AND status NOT LIKE 'ARCHIVED%'
            """
        ).fetchone()["n"]

    return confirmation, deep, int(legacy)


def integrity_checks(conn: sqlite3.Connection) -> tuple[bool, list[str]]:
    issues: list[str] = []

    quick = conn.execute("PRAGMA quick_check;").fetchone()[0]
    if quick != "ok":
        issues.append(f"quick_check={quick}")

    fk = conn.execute("PRAGMA foreign_key_check;").fetchall()
    if fk:
        issues.append(f"foreign_key_check={len(fk)} issue(s)")

    return len(issues) == 0, issues


async def run() -> None:
    backup_path = backup_database(DB_PATH)
    conn = connect_db(DB_PATH)
    ensure_housekeeping_schema(conn)

    run_id = f"housekeeping_v0_3_1_{timestamp_tag()}"
    start_run(conn, run_id)

    stats = {
        "legacy_archived": 0,
        "status_rpc_calls": 0,
        "status_signatures_checked": 0,
        "status_rpc_errors": 0,
        "status_null": 0,
        "confirmation_done": 0,
        "confirmation_failed": 0,
        "confirmation_archived": 0,
        "deep_rpc_calls": 0,
        "deep_rpc_errors": 0,
        "deep_null": 0,
        "deep_done": 0,
        "deep_archived": 0,
        "rpc_429": 0,
    }

    initial_confirm, initial_deep, initial_legacy = active_counts(conn)

    print("=" * 100)
    print("SOLANA MEMECOIN BOT - HOUSEKEEPING v0.3.1")
    print("=" * 100)
    print(f"UTC start                  : {utc_now()}")
    print(f"Database                   : {DB_PATH}")
    print(f"Safety backup              : {backup_path}")
    print(f"Initial confirmation jobs  : {initial_confirm}")
    print(f"Initial deep jobs          : {initial_deep}")
    print(f"Initial legacy v0.2 jobs   : {initial_legacy}")
    print("Mode                       : CONTROLLED QUEUE CLEANUP / NO RAW DATA DELETION")
    print()

    stats["legacy_archived"] = archive_legacy_v02_jobs(conn)

    timeout = httpx.Timeout(
        connect=15.0,
        read=45.0,
        write=20.0,
        pool=20.0,
    )

    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"Content-Type": "application/json"},
    ) as client:
        await clean_confirmation_jobs(conn, client, stats)
        await clean_deep_jobs(conn, client, stats)

    final_confirm, final_deep, final_legacy = active_counts(conn)
    integrity_ok, integrity_issues = integrity_checks(conn)

    summary = {
        "backup_path": str(backup_path),
        "initial_confirmation_jobs": initial_confirm,
        "initial_deep_jobs": initial_deep,
        "initial_legacy_jobs": initial_legacy,
        "final_confirmation_jobs": final_confirm,
        "final_deep_jobs": final_deep,
        "final_legacy_jobs": final_legacy,
        **stats,
        "integrity_ok": integrity_ok,
        "integrity_issues": integrity_issues,
    }

    clean = (
        final_confirm == 0
        and final_deep == 0
        and final_legacy == 0
        and integrity_ok
    )

    result = "PASS" if clean else "CHECK"
    finish_run(conn, run_id, result, summary)

    print()
    print("-" * 100)
    print("SUMMARY")
    print("-" * 100)
    print(f"Legacy v0.2 jobs archived : {stats['legacy_archived']}")
    print(f"Status batch RPC calls     : {stats['status_rpc_calls']}")
    print(f"Signatures status-checked  : {stats['status_signatures_checked']}")
    print(f"Confirmation jobs resolved : {stats['confirmation_done']}")
    print(f"Confirmed failed sigs      : {stats['confirmation_failed']}")
    print(f"Confirmation jobs archived : {stats['confirmation_archived']}")
    print(f"Status null responses      : {stats['status_null']}")
    print(f"Status RPC errors          : {stats['status_rpc_errors']}")
    print(f"Deep getTransaction calls  : {stats['deep_rpc_calls']}")
    print(f"Deep jobs completed        : {stats['deep_done']}")
    print(f"Deep jobs archived         : {stats['deep_archived']}")
    print(f"Deep null responses        : {stats['deep_null']}")
    print(f"Deep RPC errors            : {stats['deep_rpc_errors']}")
    print(f"HTTP 429 count             : {stats['rpc_429']}")
    print()
    print(f"Active confirmation jobs   : {final_confirm}")
    print(f"Active deep jobs           : {final_deep}")
    print(f"Active legacy v0.2 jobs    : {final_legacy}")
    print(f"SQLite quick/FK integrity  : {'PASS' if integrity_ok else 'CHECK'}")
    print(f"Safety backup              : {backup_path}")
    print()

    if integrity_issues:
        for issue in integrity_issues:
            print(f"[INTEGRITY] {issue}")

    print(f"RESULT: {result}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean historical test queues before unattended collector test."
    )
    _ = parser.parse_args()
    asyncio.run(run())
