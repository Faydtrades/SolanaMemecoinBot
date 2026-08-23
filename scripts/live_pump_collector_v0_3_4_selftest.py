from __future__ import annotations

import ast
import hashlib
import importlib.util
import inspect
import sqlite3
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE = PROJECT_ROOT / "scripts" / "live_pump_collector_v0_3_3.py"
TARGET = PROJECT_ROOT / "scripts" / "live_pump_collector_v0_3_4.py"
SELFTEST_DB = PROJECT_ROOT / "data" / "selftest" / "collector_v0_3_4_queue_cutover_selftest.sqlite3"

EXPECTED_SOURCE_SHA256 = "2a0d9c7889cab31a33aefb3369c0d9c80b46a1030ee9abf15f5b53d336faff7b"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load_target():
    spec = importlib.util.spec_from_file_location("collector_v034_selftest_module", TARGET)
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load generated v0.3.4 collector")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def create_legacy_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS confirmation_jobs (
            signature TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_poll_at_utc TEXT,
            last_error TEXT,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS deep_enrichment_jobs (
            signature TEXT PRIMARY KEY,
            reason TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_retry_at_utc TEXT,
            last_error TEXT,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS gap_jobs (
            gap_key TEXT PRIMARY KEY,
            gap_started_at_utc TEXT NOT NULL,
            gap_ended_at_utc TEXT NOT NULL,
            before_signature TEXT,
            after_signature TEXT,
            before_slot INTEGER,
            after_slot INTEGER,
            first_interior_slot INTEGER,
            last_interior_slot INTEGER,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_retry_at_utc TEXT,
            last_error TEXT,
            confirmed_blocks_checked INTEGER NOT NULL DEFAULT 0,
            events_recovered INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            completed_at_utc TEXT
        );
        """
    )
    now = "2026-08-23T00:00:00+00:00"
    conn.execute(
        """
        INSERT OR REPLACE INTO confirmation_jobs(
            signature,status,attempts,next_poll_at_utc,last_error,created_at_utc,updated_at_utc
        ) VALUES ('LEGACY_CONFIRM','IN_PROGRESS',9,?,NULL,?,?)
        """,
        (now, now, now),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO deep_enrichment_jobs(
            signature,reason,status,attempts,next_retry_at_utc,last_error,created_at_utc,updated_at_utc
        ) VALUES ('LEGACY_DEEP','LAUNCH','PENDING',3,?,NULL,?,?)
        """,
        (now, now, now),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO gap_jobs(
            gap_key,gap_started_at_utc,gap_ended_at_utc,before_signature,after_signature,
            before_slot,after_slot,first_interior_slot,last_interior_slot,status,attempts,
            next_retry_at_utc,last_error,confirmed_blocks_checked,events_recovered,
            created_at_utc,updated_at_utc,completed_at_utc
        ) VALUES ('LEGACY_GAP',?,?,NULL,NULL,1,3,2,2,'PENDING',1,?,NULL,0,0,?,?,NULL)
        """,
        (now, now, now, now, now),
    )
    conn.commit()


def status_of(conn: sqlite3.Connection, table: str, key_col: str, key: str) -> str:
    row = conn.execute(
        f"SELECT status FROM {table} WHERE {key_col}=?",
        (key,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"Missing row {table}:{key}")
    return row[0]


def main() -> int:
    print("=" * 108)
    print("live_pump_collector_v0_3_4 — QUEUE CUTOVER SELF-TEST v0.1")
    print("=" * 108)
    print(f"Project root          : {PROJECT_ROOT}")
    print(f"v0.3.3 source         : {SOURCE}")
    print(f"v0.3.4 target         : {TARGET}")
    print(f"Isolated self-test DB : {SELFTEST_DB}")
    print("Production DB touched : NO")
    print("Network/RPC           : NO")
    print("Wallet/orders         : NO")
    print()

    checks = {
        "v033_exists": SOURCE.exists(),
        "v034_exists": TARGET.exists(),
    }

    if not all(checks.values()):
        for label, ok in checks.items():
            print(f"{label:<48}: {'PASS' if ok else 'FAIL'}")
        print("RESULT: CHECK")
        return 2

    checks["v033_hash_unchanged"] = sha256_file(SOURCE) == EXPECTED_SOURCE_SHA256

    target_text = TARGET.read_text(encoding="utf-8")
    ast.parse(target_text)
    checks["v034_syntax"] = True
    checks["isolated_confirmation_table"] = "confirmation_jobs_v034" in target_text
    checks["isolated_deep_table"] = "deep_enrichment_jobs_v034" in target_text
    checks["isolated_gap_table"] = "gap_jobs_v034" in target_text

    mod = load_target()
    recover_src = inspect.getsource(mod.recover_jobs)
    checks["recover_no_pump_events_scan"] = "pump_events" not in recover_src
    checks["recover_no_transactions_scan"] = "transactions" not in recover_src

    SELFTEST_DB.parent.mkdir(parents=True, exist_ok=True)
    if SELFTEST_DB.exists():
        SELFTEST_DB.unlink()

    conn = sqlite3.connect(SELFTEST_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    recovered_confirm = recovered_deep = recovered_gap = None

    try:
        mod.ensure_schema(conn)
        create_legacy_tables(conn)

        now = mod.utc_now()

        conn.execute(
            """
            INSERT INTO confirmation_jobs_v034(
                signature,status,attempts,next_poll_at_utc,last_error,created_at_utc,updated_at_utc
            ) VALUES ('LIVE_CONFIRM','IN_PROGRESS',1,?,NULL,?,?)
            """,
            (now, now, now),
        )
        conn.execute(
            """
            INSERT INTO deep_enrichment_jobs_v034(
                signature,reason,status,attempts,next_retry_at_utc,last_error,created_at_utc,updated_at_utc
            ) VALUES ('LIVE_DEEP','LAUNCH','IN_PROGRESS',1,?,NULL,?,?)
            """,
            (now, now, now),
        )
        conn.execute(
            """
            INSERT INTO gap_jobs_v034(
                gap_key,gap_started_at_utc,gap_ended_at_utc,before_signature,after_signature,
                before_slot,after_slot,first_interior_slot,last_interior_slot,status,attempts,
                next_retry_at_utc,last_error,confirmed_blocks_checked,events_recovered,
                created_at_utc,updated_at_utc,completed_at_utc
            ) VALUES ('LIVE_GAP',?,?,NULL,NULL,10,12,11,11,'IN_PROGRESS',1,?,NULL,0,0,?,?,NULL)
            """,
            (now, now, now, now, now),
        )
        conn.commit()
        conn.close()

        conn = sqlite3.connect(SELFTEST_DB)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")

        recovered_confirm, recovered_deep, _ = mod.recover_jobs(conn)
        recovered_gap = mod.recover_gap_jobs(conn)

        checks["live_confirm_restart_recovered"] = (
            status_of(conn, "confirmation_jobs_v034", "signature", "LIVE_CONFIRM") == "PENDING"
        )
        checks["live_deep_restart_recovered"] = (
            status_of(conn, "deep_enrichment_jobs_v034", "signature", "LIVE_DEEP") == "PENDING"
        )
        checks["live_gap_restart_recovered"] = (
            status_of(conn, "gap_jobs_v034", "gap_key", "LIVE_GAP") == "RETRY"
        )

        checks["legacy_confirm_untouched"] = (
            status_of(conn, "confirmation_jobs", "signature", "LEGACY_CONFIRM") == "IN_PROGRESS"
        )
        checks["legacy_deep_untouched"] = (
            status_of(conn, "deep_enrichment_jobs", "signature", "LEGACY_DEEP") == "PENDING"
        )
        checks["legacy_gap_untouched"] = (
            status_of(conn, "gap_jobs", "gap_key", "LEGACY_GAP") == "PENDING"
        )

        mod.enqueue_confirmation(conn, "NEW_LIVE_CONFIRM")
        mod.enqueue_deep_launch_enrichment(conn, "NEW_LIVE_DEEP")
        new_gap_key = mod.enqueue_gap_job(
            conn,
            now,
            mod.utc_now(),
            "BEFORE",
            "AFTER",
            100,
            103,
        )

        checks["new_confirm_in_v034"] = conn.execute(
            "SELECT 1 FROM confirmation_jobs_v034 WHERE signature='NEW_LIVE_CONFIRM'"
        ).fetchone() is not None
        checks["new_confirm_not_legacy"] = conn.execute(
            "SELECT 1 FROM confirmation_jobs WHERE signature='NEW_LIVE_CONFIRM'"
        ).fetchone() is None
        checks["new_deep_in_v034"] = conn.execute(
            "SELECT 1 FROM deep_enrichment_jobs_v034 WHERE signature='NEW_LIVE_DEEP'"
        ).fetchone() is not None
        checks["new_gap_in_v034"] = conn.execute(
            "SELECT 1 FROM gap_jobs_v034 WHERE gap_key=?",
            (new_gap_key,),
        ).fetchone() is not None

        batch = mod.confirmation_batch(conn, 64)
        checks["confirmation_batch_active_generation"] = any(
            row["signature"] in {"LIVE_CONFIRM", "NEW_LIVE_CONFIRM"}
            for row in batch
        )

        deep = mod.next_deep_job(conn)
        checks["deep_selector_active_generation"] = (
            deep is not None
            and deep["signature"] in {"LIVE_DEEP", "NEW_LIVE_DEEP"}
        )

        gap = mod.next_gap_job(conn)
        checks["gap_selector_active_generation"] = (
            gap is not None
            and gap["gap_key"] in {"LIVE_GAP", new_gap_key}
        )

        checks["sqlite_quick_check"] = conn.execute(
            "PRAGMA quick_check"
        ).fetchone()[0] == "ok"

    finally:
        try:
            conn.close()
        except Exception:
            pass

    print("-" * 108)
    print("VALIDATION")
    print("-" * 108)
    for label, ok in checks.items():
        print(f"{label:<48}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values())

    print()
    print(f"Recovered v0.3.4 confirmation jobs : {recovered_confirm}")
    print(f"Recovered v0.3.4 deep jobs         : {recovered_deep}")
    print(f"Recovered v0.3.4 gap jobs          : {recovered_gap}")
    print("Historical fullscan at startup     : NO")
    print("Legacy queue rows deleted          : NO")
    print("Production DB touched              : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
