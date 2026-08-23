from __future__ import annotations

import ast
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE = PROJECT_ROOT / "scripts" / "live_pump_collector_v0_3_3.py"
TARGET = PROJECT_ROOT / "scripts" / "live_pump_collector_v0_3_4.py"
MANIFEST = PROJECT_ROOT / "data" / "paper" / "smoke" / "collector_v0_3_4_build_manifest.json"

EXPECTED_SOURCE_SHA256 = "2a0d9c7889cab31a33aefb3369c0d9c80b46a1030ee9abf15f5b53d336faff7b"

TABLE_RENAMES = {
    "confirmation_jobs": "confirmation_jobs_v034",
    "deep_enrichment_jobs": "deep_enrichment_jobs_v034",
    "gap_jobs": "gap_jobs_v034",
}

INDEX_RENAMES = {
    "idx_confirmation_jobs_status": "idx_confirmation_jobs_v034_status",
    "idx_deep_jobs_status": "idx_deep_jobs_v034_status",
    "idx_gap_jobs_status": "idx_gap_jobs_v034_status",
}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def replace_function(source: str, function_name: str, replacement: str) -> str:
    tree = ast.parse(source)
    lines = source.splitlines(keepends=True)

    target = None
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name:
            target = node
            break

    if target is None:
        raise RuntimeError(f"Function not found: {function_name}")

    start = target.lineno - 1
    end = target.end_lineno
    return "".join(
        lines[:start]
        + [replacement.rstrip() + "\n\n"]
        + lines[end:]
    )


RECOVER_JOBS_V034 = """
def recover_jobs(conn: sqlite3.Connection) -> tuple[int, int, int]:
    # v0.3.4 fast-start recovery:
    # resume only genuinely interrupted jobs from the isolated v0.3.4
    # active queue generation. Historical pump_events are NOT rescanned.
    now = utc_now()

    confirm_cursor = conn.execute(
        \"""
        UPDATE confirmation_jobs_v034
        SET status='PENDING',
            next_poll_at_utc=?,
            updated_at_utc=?
        WHERE status='IN_PROGRESS'
        \""",
        (now, now),
    )

    deep_cursor = conn.execute(
        \"""
        UPDATE deep_enrichment_jobs_v034
        SET status='PENDING',
            next_retry_at_utc=?,
            updated_at_utc=?
        WHERE status='IN_PROGRESS'
        \""",
        (now, now),
    )

    conn.commit()

    recovered_confirm = (
        confirm_cursor.rowcount if confirm_cursor.rowcount != -1 else 0
    )
    recovered_deep = (
        deep_cursor.rowcount if deep_cursor.rowcount != -1 else 0
    )

    # Exact pre-v0.3.4 backlog counting is intentionally omitted from
    # live startup; old queue tables remain preserved in SQLite.
    legacy_backlog_deferred = 0

    return (
        int(recovered_confirm),
        int(recovered_deep),
        legacy_backlog_deferred,
    )
"""


def main() -> int:
    print("=" * 108)
    print("BUILD live_pump_collector_v0_3_4.py — QUEUE GENERATION CUTOVER")
    print("=" * 108)
    print(f"Project root       : {PROJECT_ROOT}")
    print(f"Source             : {SOURCE}")
    print(f"Target             : {TARGET}")
    print("Source overwritten : NO")
    print("Production DB      : NOT OPENED")
    print("Network/RPC        : NO")
    print("Wallet/orders      : NO")
    print()

    if not SOURCE.exists():
        print("RESULT: CHECK")
        print(f"Missing source baseline: {SOURCE}")
        return 2

    source_bytes = SOURCE.read_bytes()
    source_hash = sha256_bytes(source_bytes)
    print(f"Source SHA256      : {source_hash}")

    if source_hash != EXPECTED_SOURCE_SHA256:
        print("RESULT: CHECK")
        print("Source baseline hash does not match the exact v0.3.3 inspected in Phase 4.")
        print(f"Expected           : {EXPECTED_SOURCE_SHA256}")
        return 2

    source = source_bytes.decode("utf-8")

    patched = source.replace("V0_3_3", "V0_3_4")
    patched = patched.replace("v0_3_3", "v0_3_4")
    patched = patched.replace("v0.3.3", "v0.3.4")

    # Exact SQL table-token cutover. Word boundaries prevent changing stats names.
    for old, new in TABLE_RENAMES.items():
        patched = re.sub(rf"\b{re.escape(old)}\b", new, patched)

    # Index identifiers contain underscores, so rename them explicitly.
    for old, new in INDEX_RENAMES.items():
        patched = patched.replace(old, new)

    patched = replace_function(patched, "recover_jobs", RECOVER_JOBS_V034)

    banner_marker = '    print("Gap recovery       : persistent slot-range reconciliation worker")\n'
    if banner_marker not in patched:
        raise RuntimeError("Expected run() banner marker not found")
    patched = patched.replace(
        banner_marker,
        banner_marker
        + '    print("Queue generation   : v0.3.4 active queues; older backlog deferred")\n'
        + '    print("Startup recovery   : interrupted v0.3.4 jobs only; NO historical fullscan")\n',
        1,
    )

    patched = patched.replace(
        '"legacy_v0_2_jobs_ignored"',
        '"legacy_backlog_deferred"',
    )
    patched = patched.replace(
        "legacy_v0_2_jobs_ignored=",
        "legacy_backlog_deferred=",
    )
    patched = patched.replace(
        "Legacy v0.2 jobs ignored",
        "Legacy backlog deferred",
    )

    # Syntax + static safety contract.
    ast.parse(patched)

    tree = ast.parse(patched)
    recover_node = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "recover_jobs"
    )
    lines = patched.splitlines()
    recover_text = "\n".join(lines[recover_node.lineno - 1: recover_node.end_lineno])

    forbidden = [
        "FROM pump_events",
        "FROM transactions",
        "INSERT OR IGNORE INTO confirmation_jobs_v034",
        "INSERT OR IGNORE INTO deep_enrichment_jobs_v034",
    ]
    bad = [token for token in forbidden if token in recover_text]
    if bad:
        raise RuntimeError(f"Historical startup backfill survived patch: {bad}")

    required = [
        "confirmation_jobs_v034",
        "deep_enrichment_jobs_v034",
        "gap_jobs_v034",
        "LIVE_WEBSOCKET_EVENT_V0_3_4",
        "GAP_RECONCILIATION_V0_3_4",
    ]
    missing = [token for token in required if token not in patched]
    if missing:
        raise RuntimeError(f"Required v0.3.4 tokens missing: {missing}")

    target_bytes = patched.encode("utf-8")
    target_hash = sha256_bytes(target_bytes)
    TARGET.write_bytes(target_bytes)

    source_hash_after = sha256_bytes(SOURCE.read_bytes())
    if source_hash_after != source_hash:
        raise RuntimeError("v0.3.3 source changed during build")

    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": "collector_v0_3_4_build_manifest_v0.1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": str(SOURCE),
        "target": str(TARGET),
        "source_sha256_before": source_hash,
        "source_sha256_after": source_hash_after,
        "target_sha256": target_hash,
        "source_unchanged": source_hash == source_hash_after,
        "active_queue_tables": list(TABLE_RENAMES.values()),
        "legacy_queue_tables_preserved": list(TABLE_RENAMES.keys()),
        "historical_startup_backfill": False,
        "production_db_opened_by_builder": False,
    }
    MANIFEST.write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print()
    print(f"Target SHA256      : {target_hash}")
    print(f"Manifest           : {MANIFEST}")
    print("v0.3.3 unchanged   : PASS")
    print("Historical backfill: REMOVED FROM LIVE STARTUP")
    print("Legacy queues      : PRESERVED / DEFERRED")
    print("v0.3.4 queues      : ISOLATED ACTIVE GENERATION")
    print()
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
