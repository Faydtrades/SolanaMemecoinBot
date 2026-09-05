from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase5.shadow_continuous_source_bridge_v0_1 import (  # noqa: E402
    CONTRACT_SPEC,
    MODEL_FINGERPRINT,
    MODEL_ID,
    SourceBridgeError,
    SourceIdentityConflict,
    SourceRowConflict,
    open_continuous_source_bridge,
)
from phase5.shadow_domain_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT as T001_FINGERPRINT,
    IntentRole,
    IntentSide,
    ShadowDeterminismConflict,
)


RUN_ID = "phase4-continuous-run-0001"
BASE_TIMESTAMP = "2026-08-24T13:53:52.299461+00:00"
EXPECTED_DECISION_US = 1_787_579_632_299_461
EXPECTED_T001_FINGERPRINT = (
    "506312a81b6d8acc724cb27d6d450086bc3fc4986dcdea4831b998a3a8ec91e3"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def expect(error: type[BaseException], callback: Callable[[], Any]) -> bool:
    try:
        callback()
    except error:
        return True
    return False


def row(
    cursor: int,
    signal_key: str,
    *,
    route_id: str | None = None,
    candidate_id: str | None = None,
    state: str = "FILLED",
    reason: str = "SIMULATED_ENTRY_FILLED",
    context_mint: str | None = None,
    route_mint: str | None = None,
    context_strategy: str = "FIRSTPULLBACK-v0.1",
    route_strategy: str | None = None,
    context_parameters: str = "EXP-0012:FINAL",
    route_parameters: str | None = None,
    timestamp: str = BASE_TIMESTAMP,
    size: int = 100_000_000,
) -> dict[str, Any]:
    mint = context_mint or f"mint-{signal_key}"
    return {
        "signal_key": signal_key,
        "route_id": route_id or f"route-{signal_key}",
        "source_evaluation_id": f"evaluation-{signal_key}",
        "run_id": RUN_ID,
        "context_mint": mint,
        "route_mint": route_mint or mint,
        "context_strategy": context_strategy,
        "route_strategy": route_strategy or context_strategy,
        "context_parameters": context_parameters,
        "route_parameters": route_parameters or context_parameters,
        "source_cursor": cursor,
        "source_event_key": f"pump_events:{cursor}:{signal_key}",
        "candidate_id": candidate_id or f"candidate-{signal_key}",
        "signal_observed_at": timestamp,
        "signal_ingest_seq": cursor,
        "requested_size_lamports": size,
        "state": state,
        "state_reason": reason,
    }


def create_paper_database(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    paper_tracks: int = 0,
) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE paper_continuous_signal_contexts_v0_1 (
                signal_key TEXT PRIMARY KEY,
                route_id TEXT NOT NULL UNIQUE,
                source_evaluation_id TEXT NOT NULL,
                decision_key TEXT NOT NULL UNIQUE,
                run_id TEXT NOT NULL,
                role TEXT NOT NULL,
                mint TEXT NOT NULL,
                strategy_version TEXT NOT NULL,
                parameter_set_id TEXT NOT NULL,
                source_cursor INTEGER NOT NULL,
                source_event_key TEXT NOT NULL,
                content_fingerprint TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE paper_entry_routes (
                route_id TEXT PRIMARY KEY,
                candidate_id TEXT NOT NULL,
                mint TEXT NOT NULL,
                strategy_version TEXT NOT NULL,
                parameter_set_id TEXT NOT NULL,
                signal_observed_at TEXT NOT NULL,
                signal_ingest_seq INTEGER NOT NULL,
                requested_size_lamports INTEGER NOT NULL,
                state TEXT NOT NULL,
                state_reason TEXT NOT NULL
            );
            CREATE TABLE paper_orders (
                paper_order_id TEXT PRIMARY KEY,
                signal_key TEXT NOT NULL,
                track_id TEXT NOT NULL
            );
            """
        )
        for item in rows:
            conn.execute(
                "INSERT INTO paper_continuous_signal_contexts_v0_1 "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    item["signal_key"],
                    item["route_id"],
                    item["source_evaluation_id"],
                    f"decision-{item['signal_key']}",
                    item["run_id"],
                    "CONTROL",
                    item["context_mint"],
                    item["context_strategy"],
                    item["context_parameters"],
                    item["source_cursor"],
                    item["source_event_key"],
                    f"context-fingerprint-{item['signal_key']}",
                    BASE_TIMESTAMP,
                    BASE_TIMESTAMP,
                ),
            )
            conn.execute(
                "INSERT INTO paper_entry_routes VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    item["route_id"],
                    item["candidate_id"],
                    item["route_mint"],
                    item["route_strategy"],
                    item["route_parameters"],
                    item["signal_observed_at"],
                    item["signal_ingest_seq"],
                    item["requested_size_lamports"],
                    item["state"],
                    item["state_reason"],
                ),
            )
            for track_index in range(paper_tracks):
                conn.execute(
                    "INSERT INTO paper_orders VALUES(?,?,?)",
                    (
                        f"order-{item['signal_key']}-{track_index}",
                        item["signal_key"],
                        ("FINAL-A", "FINAL-B", "SENS-C")[track_index],
                    ),
                )
        conn.commit()
    finally:
        conn.close()


def scalar(database: Path, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
    conn = sqlite3.connect(database)
    try:
        return conn.execute(sql, parameters).fetchone()[0]
    finally:
        conn.close()


def crash_once(target_phase: str) -> Callable[[str, Any], None]:
    fired = False

    def hook(phase: str, _source: Any) -> None:
        nonlocal fired
        if phase == target_phase and not fired:
            fired = True
            raise RuntimeError(f"injected crash: {phase}")

    return hook


def main() -> int:
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}
    shadow_root = PROJECT_ROOT / "data" / "shadow"
    shadow_root.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="phase5_t004a_", dir=shadow_root))
    protected = [
        PROJECT_ROOT / "src" / "phase5" / "shadow_domain_v0_1.py",
        PROJECT_ROOT / "src" / "phase5" / "shadow_repository_v0_1.py",
        PROJECT_ROOT / "src" / "phase5" / "shadow_venue_route_quote_v0_1.py",
        PROJECT_ROOT / "src" / "phase5" / "shadow_venue_repository_v0_1.py",
        PROJECT_ROOT / "src" / "phase5" / "shadow_unsigned_plan_simulation_v0_1.py",
        PROJECT_ROOT / "src" / "phase5" / "shadow_simulation_repository_v0_1.py",
    ]
    protected_before = {str(path): sha256(path) for path in protected}
    try:
        # A. Exact mapping, one source, repeat, reopen, read-only source, and digest.
        paper_a = root / "paper_a.sqlite3"
        shadow_a = root / "shadow_a.sqlite3"
        create_paper_database(paper_a, [row(10, "signal-a")])
        paper_hash = sha256(paper_a)
        with open_continuous_source_bridge(
            paper_a, shadow_a, candidate_run_id=RUN_ID
        ) as bridge:
            first = bridge.poll_once()
            first_digest = bridge.canonical_digest()
            replay = bridge.poll_once()
            intent_row = bridge._conn.execute(
                "SELECT intent_json FROM shadow_execution_intents"
            ).fetchone()
            intent = json.loads(str(intent_row[0]))
            lineage = json.loads(
                str(
                    bridge._conn.execute(
                        "SELECT lineage_json FROM shadow_t004a_entry_lineage"
                    ).fetchone()[0]
                )
            )
            check("A01_ONE_SOURCE_ONE_ENTRY", first.processed_rows == 1 and bridge.intent_count() == 1, checks)
            check("A02_REPEAT_POLL_NO_DUPLICATE", replay.processed_rows == 0 and bridge.lineage_count() == 1, checks)
            check("A03_EXACT_SOURCE_TO_INTENT_MAPPING", all((
                intent["candidate_signal_id"] == "candidate-signal-a",
                intent["candidate_run_id"] == RUN_ID,
                intent["strategy_evaluation_id"] == "evaluation-signal-a",
                intent["strategy_version"] == "FIRSTPULLBACK-v0.1",
                intent["parameter_set_id"] == "EXP-0012:FINAL",
                intent["source_run_id"] is None,
                intent["source_event_key"] == "pump_events:10:signal-a",
                intent["source_ingest_seq"] == 10,
                intent["decision_at_us"] == EXPECTED_DECISION_US,
                intent["mint"] == "mint-signal-a",
                intent["role"] == IntentRole.ENTRY.value,
                intent["side"] == IntentSide.BUY.value,
                intent["input_asset"] == "SOL_LAMPORTS",
                intent["input_amount_base_units"] == 100_000_000,
                all(intent[name] is None for name in (
                    "position_id", "parent_entry_intent_id", "exit_decision_id",
                    "exit_track_id", "exit_lifecycle_id",
                )),
            )), checks)
            check("A04_PAPER_OUTCOME_LINEAGE_ONLY", lineage["phase4_source"]["paper_route_state"] == "FILLED" and lineage["paper_outcome_is_shadow_outcome"] is False, checks)
            check("A05_PHASE4_CONNECTION_QUERY_ONLY", int(bridge._paper_conn.execute("PRAGMA query_only").fetchone()[0]) == 1 and expect(sqlite3.OperationalError, lambda: bridge._paper_conn.execute("DELETE FROM paper_entry_routes")), checks)
            check("A06_SHADOW_QUICK_CHECK", bridge.quick_check() == "ok", checks)
        check("A07_PHASE4_DB_BYTE_IDENTICAL", sha256(paper_a) == paper_hash, checks)
        with open_continuous_source_bridge(paper_a, shadow_a, candidate_run_id=RUN_ID) as reopened:
            check("A08_RESTART_REOPEN_NO_DUPLICATE", reopened.poll_once().processed_rows == 0 and reopened.intent_count() == 1 and reopened.lineage_count() == 1, checks)
            check("A09_RESTART_DIGEST_DETERMINISTIC", reopened.canonical_digest() == first_digest, checks)

        # B. Equal-cursor ordering and paper-track non-aggregation.
        paper_b = root / "paper_b.sqlite3"
        shadow_b = root / "shadow_b.sqlite3"
        create_paper_database(
            paper_b,
            [row(20, "signal-z"), row(20, "signal-a")],
            paper_tracks=3,
        )
        with open_continuous_source_bridge(paper_b, shadow_b, candidate_run_id=RUN_ID) as bridge:
            first_equal = bridge.poll_once(limit=1)
            second_equal = bridge.poll_once(limit=1)
            check("B01_EQUAL_CURSOR_SIGNAL_KEY_ORDER", first_equal.last_signal_key == "signal-a" and second_equal.last_signal_key == "signal-z", checks)
            check("B02_THREE_PAPER_TRACKS_NO_DUPLICATE_BUYS", bridge.intent_count() == 2 and bridge.lineage_count() == 2 and scalar(paper_b, "SELECT COUNT(*) FROM paper_orders") == 6, checks)

        # C. Both required crash windows replay safely.
        for label, phase, expected_lineage in (
            ("C01_CRASH_AFTER_INTENT_SAFE", "AFTER_INTENT_REGISTRATION", 0),
            ("C02_CRASH_AFTER_LINEAGE_SAFE", "AFTER_LINEAGE_PERSISTENCE", 1),
        ):
            paper = root / f"{label}.paper.sqlite3"
            shadow = root / f"{label}.shadow.sqlite3"
            create_paper_database(paper, [row(30, label.lower())])
            bridge = open_continuous_source_bridge(
                paper,
                shadow,
                candidate_run_id=RUN_ID,
                fault_hook=crash_once(phase),
            )
            crashed = expect(RuntimeError, bridge.poll_once)
            cursor_after_crash = bridge.cursor()
            intent_after_crash = bridge.intent_count()
            lineage_after_crash = bridge.lineage_count()
            bridge.close()
            with open_continuous_source_bridge(paper, shadow, candidate_run_id=RUN_ID) as recovered:
                recovery = recovered.poll_once()
                check(label, crashed and cursor_after_crash[:2] == (-1, "") and intent_after_crash == 1 and lineage_after_crash == expected_lineage and recovery.processed_rows == 1 and recovered.intent_count() == 1 and recovered.lineage_count() == 1, checks)

        paper_state_drift = root / "paper_state_drift.sqlite3"
        shadow_state_drift = root / "shadow_state_drift.sqlite3"
        create_paper_database(
            paper_state_drift,
            [
                row(
                    35,
                    "state-drift",
                    state="ENTRY_PENDING",
                    reason="AWAITING_EXECUTION_OBSERVATION",
                )
            ],
        )
        interrupted = open_continuous_source_bridge(
            paper_state_drift,
            shadow_state_drift,
            candidate_run_id=RUN_ID,
            fault_hook=crash_once("AFTER_LINEAGE_PERSISTENCE"),
        )
        expect(RuntimeError, interrupted.poll_once)
        interrupted.close()
        writer = sqlite3.connect(paper_state_drift)
        writer.execute(
            "UPDATE paper_entry_routes SET state='FILLED',state_reason='FILLED_LATER'"
        )
        writer.commit()
        writer.close()
        with open_continuous_source_bridge(
            paper_state_drift,
            shadow_state_drift,
            candidate_run_id=RUN_ID,
        ) as recovered:
            recovered_result = recovered.poll_once()
            observed = recovered._conn.execute(
                "SELECT paper_route_state,paper_route_reason "
                "FROM shadow_t004a_entry_lineage"
            ).fetchone()
            check(
                "C03_ROUTE_OUTCOME_ADVANCE_AFTER_LINEAGE_SAFE",
                recovered_result.processed_rows == 1
                and tuple(observed)
                == ("ENTRY_PENDING", "AWAITING_EXECUTION_OBSERVATION")
                and recovered.cursor()[:2] == (35, "state-drift"),
                checks,
            )

        # D. Failed row blocks the cursor and conflicting replay fails closed.
        paper_d = root / "paper_d.sqlite3"
        shadow_d = root / "shadow_d.sqlite3"
        create_paper_database(
            paper_d,
            [row(39, "good"), row(40, "bad", size=0), row(41, "later")],
        )
        with open_continuous_source_bridge(paper_d, shadow_d, candidate_run_id=RUN_ID) as bridge:
            failed = expect(SourceRowConflict, bridge.poll_once)
            check("D01_CURSOR_NEVER_PASSES_FAILED_ROW", failed and bridge.cursor()[:2] == (39, "good") and bridge.intent_count() == 1 and bridge.lineage_count() == 1, checks)

        paper_conflict = root / "paper_conflict.sqlite3"
        shadow_conflict = root / "shadow_conflict.sqlite3"
        create_paper_database(paper_conflict, [row(50, "conflict")])
        crashing = open_continuous_source_bridge(
            paper_conflict,
            shadow_conflict,
            candidate_run_id=RUN_ID,
            fault_hook=crash_once("AFTER_INTENT_REGISTRATION"),
        )
        expect(RuntimeError, crashing.poll_once)
        crashing.close()
        writer = sqlite3.connect(paper_conflict)
        writer.execute(
            "UPDATE paper_entry_routes SET requested_size_lamports=? WHERE route_id=?",
            (200_000_000, "route-conflict"),
        )
        writer.commit()
        writer.close()
        with open_continuous_source_bridge(
            paper_conflict, shadow_conflict, candidate_run_id=RUN_ID
        ) as bridge:
            check("D02_CONFLICTING_REPLAY_FAILS_CLOSED", expect(ShadowDeterminismConflict, bridge.poll_once) and bridge.cursor()[:2] == (-1, "") and bridge.lineage_count() == 0, checks)

        # E. Source validation matrix; no invalid row may create state or advance.
        invalid_cases = (
            ("E01_MINT_MISMATCH", {"context_mint": "mint-a", "route_mint": "mint-b"}),
            ("E02_STRATEGY_MISMATCH", {"route_strategy": "OTHER"}),
            ("E03_PARAMETER_SET_MISMATCH", {"route_parameters": "OTHER"}),
            ("E04_INVALID_UTC_TIMESTAMP", {"timestamp": "2026-08-24T13:53:52.299461+02:00"}),
            ("E05_NON_POSITIVE_SIZE", {"size": 0}),
            ("E06_NEGATIVE_SOURCE_CURSOR", {"cursor": -1}),
        )
        for index, (label, overrides) in enumerate(invalid_cases):
            paper = root / f"invalid_{index}.paper.sqlite3"
            shadow = root / f"invalid_{index}.shadow.sqlite3"
            cursor = int(overrides.pop("cursor", 60 + index))
            create_paper_database(paper, [row(cursor, f"invalid-{index}", **overrides)])
            with open_continuous_source_bridge(paper, shadow, candidate_run_id=RUN_ID) as bridge:
                check(label, expect(SourceRowConflict, bridge.poll_once) and bridge.cursor()[:2] == (-1, "") and bridge.intent_count() == 0 and bridge.lineage_count() == 0, checks)

        # F. FILLED and REJECTED are both physical candidates, not Shadow outcomes.
        paper_f = root / "paper_f.sqlite3"
        shadow_f = root / "shadow_f.sqlite3"
        create_paper_database(
            paper_f,
            [
                row(70, "filled", state="FILLED", reason="FILLED_REASON"),
                row(71, "rejected", state="REJECTED", reason="REJECTED_REASON"),
            ],
        )
        with open_continuous_source_bridge(paper_f, shadow_f, candidate_run_id=RUN_ID) as bridge:
            result = bridge.poll_once()
            states = {
                str(item[0])
                for item in bridge._conn.execute(
                    "SELECT paper_route_state FROM shadow_t004a_entry_lineage"
                )
            }
            created_states = {
                str(item[0])
                for item in bridge._conn.execute(
                    "SELECT current_state FROM shadow_state_machines"
                )
            }
            check("F01_FILLED_AND_REJECTED_BOTH_CREATE_SHARED_ENTRY", result.processed_rows == 2 and bridge.intent_count() == 2 and states == {"FILLED", "REJECTED"} and created_states == {"CREATED"}, checks)
            check("F02_LINEAGE_IMMUTABLE", expect(sqlite3.IntegrityError, lambda: bridge._conn.execute("UPDATE shadow_t004a_entry_lineage SET paper_route_state=paper_route_state")), checks)

        # G. Replacing the source file cannot silently continue the old cursor.
        paper_g = root / "paper_g.sqlite3"
        shadow_g = root / "shadow_g.sqlite3"
        create_paper_database(paper_g, [row(80, "identity")])
        with open_continuous_source_bridge(paper_g, shadow_g, candidate_run_id=RUN_ID) as bridge:
            bridge.poll_once()
        old_paper = root / "paper_g_old.sqlite3"
        os.replace(paper_g, old_paper)
        create_paper_database(paper_g, [row(81, "replacement")])
        check("G01_REPLACED_SOURCE_IDENTITY_FAILS_CLOSED", expect(SourceIdentityConflict, lambda: open_continuous_source_bridge(paper_g, shadow_g, candidate_run_id=RUN_ID)), checks)

        # H. Runner surface and structural capability/read-only checks.
        paper_h = root / "paper_h.sqlite3"
        shadow_h = root / "shadow_h.sqlite3"
        create_paper_database(paper_h, [row(90, "runner")])
        env = dict(os.environ)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        completed = subprocess.run(
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "phase5_shadow_continuous_source_bridge_v0_1.py"),
                "--paper-db",
                str(paper_h),
                "--shadow-db",
                str(shadow_h),
                "--candidate-run-id",
                RUN_ID,
                "--once",
                "--poll-ms",
                "1",
            ],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        runner_output = json.loads(completed.stdout) if completed.returncode == 0 else {}
        check("H01_RUNNER_ONCE_SURFACE", completed.returncode == 0 and runner_output.get("processed_rows") == 1 and runner_output.get("quick_check") == "ok", checks)
        production_files = [
            PROJECT_ROOT / "src" / "phase5" / "shadow_continuous_source_bridge_v0_1.py",
            PROJECT_ROOT / "scripts" / "phase5_shadow_continuous_source_bridge_v0_1.py",
        ]
        trees = [ast.parse(path.read_text(encoding="utf-8")) for path in production_files]
        call_names = {
            node.func.attr.lower()
            for tree in trees
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        } | {
            node.func.id.lower()
            for tree in trees
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        source_sql = (PROJECT_ROOT / "src" / "phase5" / "shadow_continuous_source_bridge_v0_1.py").read_text(encoding="utf-8")
        check("H02_NO_NETWORK_SIGNER_BROADCAST_CAPABILITY", not ({"requests", "post", "send_transaction", "sendrawtransaction", "keypair", "sign"} & call_names), checks)
        check("H03_PHASE4_SQL_SELECT_ONLY", all(token not in source_sql.upper().split("_SOURCE_SQL =", 1)[1].split('"""', 2)[1] for token in ("INSERT ", "UPDATE ", "DELETE ", "CREATE ", "DROP ", "ALTER ")), checks)
        check("H04_MODEL_ID_AND_FINGERPRINT", MODEL_ID == "P5-SHADOW-CONTINUOUS-SOURCE-BRIDGE-0001" and len(MODEL_FINGERPRINT) == 64 and CONTRACT_SPEC.get("entry_input_asset_unit") == "SOL_LAMPORTS" and T001_FINGERPRINT == EXPECTED_T001_FINGERPRINT, checks)
        check("H05_SOURCE_SHADOW_SAME_FILE_REJECTED", expect(SourceBridgeError, lambda: open_continuous_source_bridge(paper_h, paper_h, candidate_run_id=RUN_ID)), checks)
        check("H06_SHADOW_OUTSIDE_DATA_SHADOW_REJECTED", expect(SourceBridgeError, lambda: open_continuous_source_bridge(paper_h, PROJECT_ROOT / "forbidden_t004a.sqlite3", candidate_run_id=RUN_ID)), checks)

        protected_after = {str(path): sha256(path) for path in protected}
        check("H07_ACCEPTED_PHASE5_MODULES_UNCHANGED", protected_after == protected_before, checks)
        evidence.update(
            {
                "model_id": MODEL_ID,
                "model_fingerprint": MODEL_FINGERPRINT,
                "checks_total": len(checks),
                "checks_passed": sum(checks.values()),
                "restart_digest": first_digest,
                "phase4_source_sha256_before_after": [paper_hash, sha256(paper_a)],
            }
        )
    finally:
        shutil.rmtree(root)

    passed = sum(checks.values())
    print("=" * 104)
    print("PHASE-5 CONTINUOUS PHASE4 TO SHADOW SOURCE BRIDGE v0.1 SELF-TEST")
    print("=" * 104)
    for name, ok in checks.items():
        print(f"{name}: {'PASS' if ok else 'FAIL'}")
    print(f"MODEL_ID: {MODEL_ID}")
    print(f"MODEL_FINGERPRINT: {MODEL_FINGERPRINT}")
    print(f"RESTART_DIGEST: {evidence.get('restart_digest', 'UNAVAILABLE')}")
    print(f"CHECKS: {passed}/{len(checks)}")
    print(f"RESULT: {'PASS' if passed == len(checks) else 'FAIL'}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
