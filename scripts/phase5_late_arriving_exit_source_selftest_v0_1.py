from __future__ import annotations

import ast
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import phase5_shadow_lifecycle_bridge_selftest_v0_1 as fx  # noqa: E402
from phase5.shadow_domain_v0_1 import (  # noqa: E402
    ShadowState,
    canonical_json,
    deterministic_id,
)
from phase5.shadow_lifecycle_bridge_v0_1 import (  # noqa: E402
    EXIT_INTENT_CLASSIFICATION,
    LEGACY_V02_EXIT_EVIDENCE_SCHEMA_VERSION,
    LEGACY_V02_MODEL_FINGERPRINT,
    LEGACY_V02_MODEL_ID,
    LEGACY_V02_SCHEMA_VERSION,
    MODEL_FINGERPRINT,
    MODEL_ID,
    SCHEMA_VERSION,
    SourceIdentityConflict,
    _EXIT_SQL,
    open_shadow_lifecycle_bridge,
)


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def expect(error: type[BaseException], callback: Callable[[], Any]) -> bool:
    try:
        callback()
    except error:
        return True
    return False


def add_exit(path: Path, signal_key: str, item: dict[str, Any], cursor: int) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "INSERT INTO paper_exit_track_states VALUES(?,?,?,?,?,?,?,?,?)",
            (
                item["paper_position_id"],
                item["paper_order_id"],
                signal_key,
                fx.MINT,
                item["track_id"],
                item["exit_variant"],
                cursor,
                "EXIT_INTENT",
                item["exit_intent_id"],
            ),
        )
        conn.execute(
            "INSERT INTO paper_exit_intents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                item["exit_intent_id"],
                item["paper_position_id"],
                item["paper_order_id"],
                signal_key,
                fx.MINT,
                item["track_id"],
                item["exit_variant"],
                item["reason"],
                item["requested_at"],
                item["trigger_observed_at"],
                item["trigger_ingest_seq"],
                item["trigger_source_event_key"],
                item["trigger_price_numerator_raw"],
                item["trigger_price_denominator_raw"],
                item["rule_return_bps"],
                item["trail_peak_return_bps"],
                item["last_fresh_observed_at"],
                item["last_fresh_ingest_seq"],
                item["last_fresh_return_bps"],
            ),
        )
        conn.commit()
    finally:
        conn.close()


def prepare_completed_parent(paper: Path, shadow: Path) -> str:
    parent_id = fx.bridge_entries(paper, shadow)[0]
    parent, quote, result = fx.complete_entry_chain(shadow, parent_id)
    with open_shadow_lifecycle_bridge(paper, shadow) as bridge:
        assert bridge.materialize_expected_inventory(
            parent, quote, result, evidence_at_us=fx.BASE_US + 30
        ) is not None
    return parent_id


def convert_one_row_database_to_legacy_v02(shadow: Path) -> tuple[str, str]:
    conn = sqlite3.connect(shadow)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM shadow_t004b_exit_source_evidence"
        ).fetchone()
        assert row is not None
        payload = json.loads(str(row["evidence_json"]))
        payload["schema_version"] = LEGACY_V02_EXIT_EVIDENCE_SCHEMA_VERSION
        payload["model_id"] = LEGACY_V02_MODEL_ID
        payload["model_fingerprint"] = LEGACY_V02_MODEL_FINGERPRINT
        del payload["phase4_exit"]["source_rowid"]
        legacy_json = canonical_json(payload)
        legacy_id = deterministic_id(
            "P5XEV",
            LEGACY_V02_EXIT_EVIDENCE_SCHEMA_VERSION,
            str(row["source_id"]),
            str(row["candidate_run_id"]),
            str(row["requested_at"]),
            str(row["exit_intent_id"]),
        )
        legacy_fingerprint = hashlib.sha256(legacy_json.encode("utf-8")).hexdigest()
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.executescript(
            """
            DROP TRIGGER shadow_t004b_exit_progress_no_delete;
            DROP TRIGGER shadow_t004b_exit_evidence_no_update;
            DROP TRIGGER shadow_t004b_exit_cursor_no_delete;
            DELETE FROM shadow_t004b_exit_source_progress;
            """
        )
        conn.execute(
            "UPDATE shadow_t004b_exit_source_evidence SET evidence_id=?,"
            "evidence_json=?,content_fingerprint=?",
            (legacy_id, legacy_json, legacy_fingerprint),
        )
        conn.execute(
            "UPDATE shadow_t004b_exit_cursors SET last_evidence_id=?",
            (legacy_id,),
        )
        conn.execute(
            "UPDATE shadow_t004b_schema_meta SET model_id=?,schema_version=?,"
            "model_fingerprint=?,exit_evidence_schema_version=?",
            (
                LEGACY_V02_MODEL_ID,
                LEGACY_V02_SCHEMA_VERSION,
                LEGACY_V02_MODEL_FINGERPRINT,
                LEGACY_V02_EXIT_EVIDENCE_SCHEMA_VERSION,
            ),
        )
        conn.commit()
        conn.execute("DROP TABLE shadow_t004b_exit_source_progress")
        conn.execute(
            "ALTER TABLE shadow_t004b_exit_cursors DROP COLUMN last_source_rowid"
        )
        conn.execute(
            "ALTER TABLE shadow_t004b_schema_meta "
            "DROP COLUMN exit_progress_schema_version"
        )
        conn.commit()
        return legacy_id, legacy_json
    finally:
        conn.close()


def main() -> int:
    checks: dict[str, bool] = {}
    shadow_root = PROJECT_ROOT / "data" / "shadow"
    shadow_root.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="phase5_t004d1_", dir=shadow_root))
    t004a_path = PROJECT_ROOT / "src" / "phase5" / "shadow_continuous_source_bridge_v0_1.py"
    t004a_before = hashlib.sha256(t004a_path.read_bytes()).hexdigest()
    try:
        # Physical append order, not requested_at or ID order, drives discovery.
        paper = root / "late.paper.sqlite3"
        shadow = root / "late.shadow.sqlite3"
        larger = fx.exit_row("exit-z", "FINAL-A", "FALLBACK", 2_000, triggered=False)
        fx.create_paper_database(
            paper,
            [{"signal_key": "late", "source_cursor": 800, "exits": [larger]}],
        )
        prepare_completed_parent(paper, shadow)
        with open_shadow_lifecycle_bridge(paper, shadow) as bridge:
            first = bridge.poll_exit_once()
            check("A01_FIRST_PHYSICAL_ROW_CONSUMED", first.last_source_rowid == 1, checks)

        smaller = fx.exit_row("exit-a", "FINAL-B", "FALLBACK", 2_000, triggered=False)
        add_exit(paper, "late", smaller, 800)
        with open_shadow_lifecycle_bridge(paper, shadow) as bridge:
            second = bridge.poll_exit_once()
            check(
                "A02_SAME_TIME_SMALLER_ID_LATE_ROW_CONSUMED_ONCE",
                second.processed_rows == 1
                and second.last_source_rowid == 2
                and second.last_exit_intent_id == "exit-a",
                checks,
            )

        older = fx.exit_row("exit-m", "SENS-C", "FALLBACK", 1_000, triggered=False)
        add_exit(paper, "late", older, 800)
        with open_shadow_lifecycle_bridge(paper, shadow) as bridge:
            third = bridge.poll_exit_once()
            counts = bridge._conn.execute(
                "SELECT COUNT(*),COUNT(DISTINCT evidence_id),"
                "COUNT(DISTINCT shadow_exit_intent_id) "
                "FROM shadow_t004b_exit_source_evidence"
            ).fetchone()
            lanes = {
                json.loads(str(row[0]))["exit_track_id"]
                for row in bridge._conn.execute(
                    "SELECT intent_json FROM shadow_execution_intents WHERE role='EXIT'"
                )
            }
            check(
                "A03_OLDER_CAUSAL_TIME_LATE_ROW_CONSUMED_ONCE",
                third.processed_rows == 1
                and third.last_source_rowid == 3
                and third.last_requested_at == older["requested_at"],
                checks,
            )
            check(
                "A04_RESTART_REPLAY_IDEMPOTENT_AND_TRACKS_INDEPENDENT",
                bridge.poll_exit_once().processed_rows == 0
                and tuple(counts) == (3, 3, 3)
                and lanes == {"FINAL-A", "FINAL-B", "SENS-C"},
                checks,
            )
            check(
                "A05_APPEND_PROGRESS_EXACT",
                [
                    tuple(row)
                    for row in bridge._conn.execute(
                        "SELECT source_rowid,exit_intent_id "
                        "FROM shadow_t004b_exit_source_progress ORDER BY source_rowid"
                    ).fetchall()
                ]
                == [(1, "exit-z"), (2, "exit-a"), (3, "exit-m")],
                checks,
            )
            check(
                "A06_PHASE4_CONNECTION_QUERY_ONLY",
                bridge._paper_conn.execute("PRAGMA query_only").fetchone()[0] == 1,
                checks,
            )

        # A real v0.2-shaped cursor is reset to rowid zero and reconciled.
        migration_paper = root / "migration.paper.sqlite3"
        migration_shadow = root / "migration.shadow.sqlite3"
        old_a = fx.exit_row("legacy-z", "FINAL-A", "FALLBACK", 4_000, triggered=False)
        fx.create_paper_database(
            migration_paper,
            [{"signal_key": "migration", "source_cursor": 900, "exits": [old_a]}],
        )
        prepare_completed_parent(migration_paper, migration_shadow)
        with open_shadow_lifecycle_bridge(migration_paper, migration_shadow) as bridge:
            assert bridge.poll_exit_once().processed_rows == 1
        legacy_id, legacy_json = convert_one_row_database_to_legacy_v02(migration_shadow)
        hole = fx.exit_row("legacy-a", "FINAL-B", "FALLBACK", 4_000, triggered=False)
        add_exit(migration_paper, "migration", hole, 900)
        with open_shadow_lifecycle_bridge(migration_paper, migration_shadow) as bridge:
            migrated_meta = bridge._conn.execute(
                "SELECT model_id,schema_version,model_fingerprint "
                "FROM shadow_t004b_schema_meta"
            ).fetchone()
            check(
                "B01_ACCEPTED_V02_SCHEMA_MIGRATES_TO_V03",
                tuple(migrated_meta) == (MODEL_ID, SCHEMA_VERSION, MODEL_FINGERPRINT)
                and bridge.cursor()[0] == 0,
                checks,
            )
            first_reconciled = bridge.poll_exit_once(limit=1)
            legacy_after = bridge._conn.execute(
                "SELECT evidence_json FROM shadow_t004b_exit_source_evidence "
                "WHERE evidence_id=?",
                (legacy_id,),
            ).fetchone()
            recovered = bridge.poll_exit_once(limit=1)
            check(
                "B02_EXISTING_EVIDENCE_RECONCILED_UNCHANGED",
                first_reconciled.processed_rows == 1
                and str(legacy_after[0]) == legacy_json
                and bridge._conn.execute(
                    "SELECT COUNT(*) FROM shadow_t004b_exit_source_evidence "
                    "WHERE evidence_id=?",
                    (legacy_id,),
                ).fetchone()[0]
                == 1,
                checks,
            )
            check(
                "B03_LEGACY_HOLE_RECOVERED_EXACTLY_ONCE",
                recovered.processed_rows == 1
                and recovered.last_exit_intent_id == "legacy-a"
                and bridge._conn.execute(
                    "SELECT COUNT(*) FROM shadow_t004b_exit_source_evidence"
                ).fetchone()[0]
                == 2
                and bridge._conn.execute(
                    "SELECT COUNT(*) FROM shadow_execution_intents WHERE role='EXIT'"
                ).fetchone()[0]
                == 2,
                checks,
            )
            check(
                "B04_POST_MIGRATION_RESTART_IDEMPOTENT",
                bridge.poll_exit_once().processed_rows == 0
                and bridge.cursor()[0] == 2,
                checks,
            )

        bad_meta_shadow = root / "inexact-legacy-meta.shadow.sqlite3"
        shutil.copy2(migration_shadow, bad_meta_shadow)
        conn = sqlite3.connect(bad_meta_shadow)
        conn.execute(
            "UPDATE shadow_t004b_schema_meta SET model_id='wrong-model',"
            "schema_version=?,model_fingerprint=?,exit_evidence_schema_version=?",
            (
                LEGACY_V02_SCHEMA_VERSION,
                LEGACY_V02_MODEL_FINGERPRINT,
                LEGACY_V02_EXIT_EVIDENCE_SCHEMA_VERSION,
            ),
        )
        conn.commit()
        conn.close()
        check(
            "B05_INEXACT_LEGACY_METADATA_FAILS_CLOSED",
            expect(
                SourceIdentityConflict,
                lambda: open_shadow_lifecycle_bridge(migration_paper, bad_meta_shadow),
            ),
            checks,
        )

        # Cursor/source-scope mismatch remains fail closed.
        conn = sqlite3.connect(migration_shadow)
        conn.execute(
            "UPDATE shadow_t004b_exit_cursors SET source_scope_version='wrong-scope'"
        )
        conn.commit()
        conn.close()
        check(
            "C01_CURSOR_SOURCE_SCOPE_MISMATCH_FAILS_CLOSED",
            expect(
                SourceIdentityConflict,
                lambda: open_shadow_lifecycle_bridge(migration_paper, migration_shadow),
            ),
            checks,
        )

        # Processed physical source identity is audited on every poll.
        conn = sqlite3.connect(paper)
        conn.execute(
            "UPDATE paper_exit_intents SET exit_intent_id='source-rewritten' WHERE rowid=1"
        )
        conn.commit()
        conn.close()
        with open_shadow_lifecycle_bridge(paper, shadow) as bridge:
            check(
                "C02_PROCESSED_SOURCE_ROW_REWRITE_FAILS_CLOSED",
                expect(SourceIdentityConflict, bridge.poll_exit_once),
                checks,
            )

        # Existing lifecycle tests cover nonterminal/negative/completed parent outcomes.
        production = PROJECT_ROOT / "src" / "phase5" / "shadow_lifecycle_bridge_v0_1.py"
        tree = ast.parse(production.read_text(encoding="utf-8"))
        imports = {
            alias.name.split(".")[0].lower()
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            str(node.module).split(".")[0].lower()
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        }
        flattened = production.read_text(encoding="utf-8").lower().replace("_", "")
        prohibited = (
            "keypair",
            "privatekey",
            "sendtransaction",
            "sendrawtransaction",
            "broadcast",
        )
        check(
            "D01_NO_SIGNER_SEND_BROADCAST_CAPABILITY",
            not ({"requests", "httpx", "websockets", "solana"} & imports)
            and not any(token in flattened for token in prohibited),
            checks,
        )
        check(
            "D02_T004A_IMPLEMENTATION_UNCHANGED",
            hashlib.sha256(t004a_path.read_bytes()).hexdigest() == t004a_before,
            checks,
        )
        normalized_sql = " ".join(_EXIT_SQL.lower().split())
        check(
            "D03_DISCOVERY_SQL_USES_ONLY_PHYSICAL_APPEND_CURSOR",
            "where e.rowid > ? order by e.rowid" in normalized_sql
            and "requested_at >" not in normalized_sql
            and "exit_intent_id >" not in normalized_sql,
            checks,
        )
    finally:
        shutil.rmtree(root)

    passed = sum(checks.values())
    print("=" * 104)
    print("PHASE-5 LATE-ARRIVING EXIT SOURCE COVERAGE CORRECTION v0.1 SELF-TEST")
    print("=" * 104)
    for name, ok in checks.items():
        print(f"{name}: {'PASS' if ok else 'FAIL'}")
    print(f"MODEL_ID: {MODEL_ID}")
    print(f"MODEL_FINGERPRINT: {MODEL_FINGERPRINT}")
    print(f"CHECKS: {passed}/{len(checks)}")
    print(f"RESULT: {'PASS' if passed == len(checks) else 'FAIL'}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
