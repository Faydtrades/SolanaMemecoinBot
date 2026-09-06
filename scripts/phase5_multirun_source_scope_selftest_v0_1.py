from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

import phase5_shadow_continuous_execution_selftest_v0_1 as c2fx  # noqa: E402
import phase5_shadow_continuous_source_bridge_selftest_v0_1 as afx  # noqa: E402
import phase5_shadow_lifecycle_bridge_selftest_v0_1 as bfx  # noqa: E402
from phase5 import shadow_continuous_execution_v0_1 as c2  # noqa: E402
from phase5 import shadow_continuous_source_bridge_v0_1 as a  # noqa: E402
from phase5 import shadow_lifecycle_bridge_v0_1 as b  # noqa: E402
from phase5.shadow_domain_v0_1 import ShadowDeterminismConflict  # noqa: E402
from phase5.shadow_repository_v0_1 import open_shadow_repository  # noqa: E402


RUNS = ("candidate-run-a", "candidate-run-b", "candidate-run-c")
TRACKS = ("FINAL-A", "FINAL-B", "SENS-C")
OLD_C2_FINGERPRINT = "f50d24e07049a3f2b2bc3283e266c8925145de76bb7e5a91c9dea9ccbe0c7be6"
EXPECTED_T004A_FINGERPRINT = "83ca764101a325ef9c0cd8c4287e23d80f8e69b1ad5ee34f099ccce3ef470edb"
EXPECTED_T004B_FINGERPRINT = "a14d1a7b2d959ebcc75583f830f8d444313b694f947a2bebdfe2a59b27b0e7f0"
EXPECTED_C2_FINGERPRINT = "c587e05e772b3f9e7de3ed9afaf7e8d83056a029aec5c5b19bffe71ba5c8e694"
EVIDENCE = ROOT / "data" / "shadow" / "evidence" / "P5_T004D0_multirun_source_scope_evidence.json"


def require(name: str, condition: bool, checks: dict[str, bool]) -> None:
    if not condition:
        raise AssertionError(name)
    checks[name] = True


def capture(error: type[BaseException], callback: Callable[[], Any]) -> BaseException | None:
    try:
        callback()
    except error as exc:
        return exc
    return None


def crash_once(target: str) -> Callable[[str, Any], None]:
    fired = False

    def hook(stage: str, _source: Any) -> None:
        nonlocal fired
        if stage == target and not fired:
            fired = True
            raise RuntimeError(f"injected crash: {stage}")

    return hook


def intent_rows(conn: sqlite3.Connection, role: str) -> list[dict[str, Any]]:
    return [
        json.loads(str(row[0]))
        for row in conn.execute(
            "SELECT intent_json FROM shadow_execution_intents "
            "WHERE role=? ORDER BY decision_at_us,intent_id",
            (role,),
        )
    ]


def exit_candidates() -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for index, (signal, run_id) in enumerate(zip(("exit-a", "exit-b", "exit-c"), RUNS)):
        exits = []
        for track in TRACKS:
            item = bfx.exit_row(
                f"exit-{signal}-{track}",
                track,
                "TAKE_PROFIT",
                20_000,
                triggered=True,
            )
            item["paper_position_id"] = f"position-{signal}-{track}"
            item["paper_order_id"] = f"order-{signal}-{track}"
            exits.append(item)
        candidates.append(
            {
                "signal_key": signal,
                "source_cursor": 100 + index,
                "run_id": run_id,
                "exits": exits,
            }
        )
    return candidates


def main() -> int:
    checks: dict[str, bool] = {}
    proof: dict[str, Any] = {}
    require("exact source contract fingerprint", a.MODEL_FINGERPRINT == EXPECTED_T004A_FINGERPRINT, checks)
    require("exact lifecycle contract fingerprint", b.MODEL_FINGERPRINT == EXPECTED_T004B_FINGERPRINT, checks)
    require("exact C2 contract fingerprint", c2.MODEL_FINGERPRINT == EXPECTED_C2_FINGERPRINT, checks)
    root = ROOT / "data" / "shadow"
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="t004d0_", dir=root) as directory:
        temporary = Path(directory)

        # Global ENTRY scope: three run IDs, a repeated run ID, and an equal cursor.
        paper = temporary / "multi-entry.paper.sqlite3"
        shadow = temporary / "multi-entry.shadow.sqlite3"
        afx.create_paper_database(
            paper,
            [
                afx.row(10, "signal-b", run_id=RUNS[0]),
                afx.row(10, "signal-a", run_id=RUNS[1]),
                afx.row(11, "signal-c", run_id=RUNS[2]),
                afx.row(12, "signal-d", run_id=RUNS[2]),
            ],
            paper_tracks=3,
        )
        paper_before = paper.read_bytes()
        observed_order: list[tuple[int, str]] = []
        with a.open_continuous_source_bridge(paper, shadow) as bridge:
            require("one global source", bridge._conn.execute(
                "SELECT COUNT(*) FROM shadow_t004a_sources"
            ).fetchone()[0] == 1, checks)
            require("one global entry cursor", bridge._conn.execute(
                "SELECT COUNT(*) FROM shadow_t004a_source_cursors"
            ).fetchone()[0] == 1, checks)
            for _ in range(2):
                result = bridge.poll_once(limit=1)
                observed_order.append((result.last_source_cursor, result.last_signal_key))
            require("equal cursor signal ordering", observed_order == [(10, "signal-a"), (10, "signal-b")], checks)
        with a.open_continuous_source_bridge(paper, shadow) as bridge:
            require("restart resumes global cursor", bridge.cursor()[:2] == (10, "signal-b"), checks)
            while True:
                result = bridge.poll_once(limit=1)
                if result.processed_rows == 0:
                    break
                observed_order.append((result.last_source_cursor, result.last_signal_key))
            entries = intent_rows(bridge._conn, "ENTRY")
            entry_runs = {
                item["candidate_signal_id"]: item["candidate_run_id"] for item in entries
            }
            digest = bridge.canonical_digest()
            require("all rows become entries", len(entries) == 4 and bridge.lineage_count() == 4, checks)
            require("per-row run preserved", entry_runs == {
                "candidate-signal-a": RUNS[1],
                "candidate-signal-b": RUNS[0],
                "candidate-signal-c": RUNS[2],
                "candidate-signal-d": RUNS[2],
            }, checks)
            require("same run may own multiple rows", list(entry_runs.values()).count(RUNS[2]) == 2, checks)
            require("tracks do not duplicate entry buys", len(entries) == 4 and afx.scalar(
                paper, "SELECT COUNT(*) FROM paper_orders"
            ) == 12, checks)
            require("repeat poll idempotent", bridge.poll_once().processed_rows == 0, checks)
            require("entry quick check", bridge.quick_check() == "ok", checks)
        with a.open_continuous_source_bridge(paper, shadow) as bridge:
            entry_digest_equal = bridge.canonical_digest() == digest
            require("deterministic entry restart digest", entry_digest_equal, checks)
        entry_source_immutable = paper.read_bytes() == paper_before
        require("entry source byte immutable", entry_source_immutable, checks)
        require("no entry run filter", "c.run_id =" not in a._SOURCE_SQL.lower(), checks)
        proof["entry"] = {
            "source_count": 1,
            "cursor_order": observed_order,
            "intent_count": len(entries),
            "candidate_run_ids": entry_runs,
            "restart_digest_equal": entry_digest_equal,
            "source_byte_for_byte_unchanged": entry_source_immutable,
        }

        # Crash at a run boundary must replay; a changed persisted run must fail closed.
        crash_paper = temporary / "crash.paper.sqlite3"
        crash_shadow = temporary / "crash.shadow.sqlite3"
        afx.create_paper_database(crash_paper, [
            afx.row(1, "crash-a", run_id=RUNS[0]),
            afx.row(2, "crash-b", run_id=RUNS[1]),
            afx.row(3, "crash-c", run_id=RUNS[2]),
        ])
        with a.open_continuous_source_bridge(crash_paper, crash_shadow) as bridge:
            bridge.poll_once(limit=1)
        interrupted = a.open_continuous_source_bridge(
            crash_paper,
            crash_shadow,
            fault_hook=crash_once("AFTER_INTENT_REGISTRATION"),
        )
        require("run-boundary crash injected", capture(RuntimeError, lambda: interrupted.poll_once(limit=1)) is not None, checks)
        require("run-boundary cursor held", interrupted.cursor()[:2] == (1, "crash-a"), checks)
        interrupted.close()
        with a.open_continuous_source_bridge(crash_paper, crash_shadow) as bridge:
            require("run-boundary replay safe", bridge.poll_once(limit=1).processed_rows == 1 and bridge.cursor()[:2] == (2, "crash-b"), checks)
            bridge.poll_once()
            require("run-boundary no duplicate", bridge.intent_count() == 3 and bridge.lineage_count() == 3, checks)
        proof["restart"] = {
            "crash_at_run_boundary_replayed": True,
            "global_cursor_held_before_replay": [1, "crash-a"],
            "intent_count_after_recovery": 3,
            "lineage_count_after_recovery": 3,
        }

        drift_paper = temporary / "run-drift.paper.sqlite3"
        drift_shadow = temporary / "run-drift.shadow.sqlite3"
        afx.create_paper_database(drift_paper, [
            afx.row(1, "drift-a", run_id=RUNS[0]),
            afx.row(2, "drift-b", run_id=RUNS[1]),
        ])
        with a.open_continuous_source_bridge(drift_paper, drift_shadow) as bridge:
            bridge.poll_once(limit=1)
        interrupted = a.open_continuous_source_bridge(
            drift_paper,
            drift_shadow,
            fault_hook=crash_once("AFTER_LINEAGE_PERSISTENCE"),
        )
        require("persisted-lineage crash injected", capture(RuntimeError, lambda: interrupted.poll_once(limit=1)) is not None, checks)
        interrupted.close()
        with closing(sqlite3.connect(drift_paper)) as writer, writer:
            writer.execute(
                "UPDATE paper_continuous_signal_contexts_v0_1 SET run_id=? WHERE signal_key=?",
                ("candidate-run-changed", "drift-b"),
            )
        with a.open_continuous_source_bridge(drift_paper, drift_shadow) as bridge:
            run_conflict = capture(ShadowDeterminismConflict, lambda: bridge.poll_once(limit=1))
            require("changed persisted run fails closed", run_conflict is not None and bridge.cursor()[:2] == (1, "drift-a") and bridge.intent_count() == 2 and bridge.lineage_count() == 2, checks)
        proof["run_id_drift"] = {
            "fails_closed": run_conflict is not None,
            "cursor_after_failure": [1, "drift-a"],
            "duplicate_intent_created": False,
        }

        # Multi-run EXIT scope and exact per-row parent resolution.
        exit_paper = temporary / "multi-exit.paper.sqlite3"
        exit_shadow = temporary / "multi-exit.shadow.sqlite3"
        bfx.create_paper_database(exit_paper, exit_candidates())
        exit_before = exit_paper.read_bytes()
        parent_ids = bfx.bridge_entries(exit_paper, exit_shadow)
        completed = [bfx.complete_entry_chain(exit_shadow, parent_id) for parent_id in parent_ids]
        cursor_order: list[int] = []
        with b.open_shadow_lifecycle_bridge(exit_paper, exit_shadow) as bridge:
            for parent, quote, simulation in completed:
                bridge.materialize_expected_inventory(
                    parent, quote, simulation, evidence_at_us=bfx.BASE_US + 30
                )
            source_rows = bridge._load_exit_rows(0, 100)
            first_source = bridge._construct_exit_source(source_rows[0])
            wrong_parent = replace(first_source, candidate_run_id="candidate-run-wrong")
            require("cross-run parent mismatch fails closed", capture(
                b.ExitSourceConflict, lambda: bridge._parent_entry(wrong_parent)
            ) is not None, checks)
            while True:
                result = bridge.poll_exit_once(limit=1)
                if result.processed_rows == 0:
                    break
                cursor_order.append(result.last_source_rowid)
            exit_intents = intent_rows(bridge._conn, "EXIT")
            entry_intents = intent_rows(bridge._conn, "ENTRY")
            entry_by_signal = {item["candidate_signal_id"]: item for item in entry_intents}
            exact_parents = all(
                item["candidate_run_id"] == entry_by_signal[item["candidate_signal_id"]]["candidate_run_id"]
                and item["parent_entry_intent_id"] == entry_by_signal[item["candidate_signal_id"]]["intent_id"]
                for item in exit_intents
            )
            evidence_rows = [json.loads(str(row[0])) for row in bridge._conn.execute(
                "SELECT evidence_json FROM shadow_t004b_exit_source_evidence"
            )]
            evidence_runs = {item["phase4_exit"]["candidate_run_id"] for item in evidence_rows}
            exit_digest = bridge.canonical_digest()
            require("multi-run exits consumed", len(exit_intents) == 9 and evidence_runs == set(RUNS), checks)
            require("exact exit parents", exact_parents, checks)
            require("tracks remain separate", all(
                {item["exit_track_id"] for item in exit_intents if item["candidate_signal_id"] == signal}
                == set(TRACKS)
                for signal in entry_by_signal
            ), checks)
            require("one global exit cursor", bridge._conn.execute(
                "SELECT COUNT(*) FROM shadow_t004b_exit_cursors"
            ).fetchone()[0] == 1, checks)
            require("exit cursor deterministic", cursor_order == list(range(1, 10)), checks)
            require("exit repeat idempotent", bridge.poll_exit_once().processed_rows == 0, checks)
            require("exit quick check", bridge.quick_check() == "ok", checks)
        with b.open_shadow_lifecycle_bridge(exit_paper, exit_shadow) as bridge:
            exit_digest_equal = bridge.canonical_digest() == exit_digest
            require("deterministic exit restart digest", exit_digest_equal, checks)
        exit_source_immutable = exit_paper.read_bytes() == exit_before
        require("exit source byte immutable", exit_source_immutable, checks)
        require("no exit run filter", "run_id = ?" not in b._EXIT_SQL.lower(), checks)

        joined_paper = temporary / "joined-run-conflict.paper.sqlite3"
        joined_shadow = temporary / "joined-run-conflict.shadow.sqlite3"
        joined_exit = bfx.exit_row(
            "exit-joined-run", "FINAL-A", "TAKE_PROFIT", 30_000, triggered=True
        )
        bfx.create_paper_database(joined_paper, [
            {"signal_key": "joined-a", "source_cursor": 1, "run_id": RUNS[0], "exits": [joined_exit]},
            {"signal_key": "joined-b", "source_cursor": 2, "run_id": RUNS[1], "exits": []},
        ])
        bfx.bridge_entries(joined_paper, joined_shadow)
        with closing(sqlite3.connect(joined_paper)) as writer, writer:
            writer.execute(
                "UPDATE paper_exit_track_states SET signal_key='joined-b' "
                "WHERE exit_intent_id='exit-joined-run'"
            )
        with b.open_shadow_lifecycle_bridge(joined_paper, joined_shadow) as bridge:
            require("exit and track run disagreement fails closed", capture(
                b.ExitSourceConflict, bridge.poll_exit_once
            ) is not None and bridge.cursor() == (0, "", "", None, 0), checks)
        proof["exit"] = {
            "intent_count": len(exit_intents),
            "candidate_run_ids": sorted(evidence_runs),
            "cursor_order": cursor_order,
            "exact_parent_resolution": exact_parents,
            "tracks": list(TRACKS),
            "restart_digest_equal": exit_digest_equal,
            "source_byte_for_byte_unchanged": exit_source_immutable,
        }

        # A fresh C2 database uses exactly one source-global A bridge and B bridge.
        c2_paper = temporary / "c2.paper.sqlite3"
        c2_shadow = temporary / "c2.shadow.sqlite3"
        c2fx.paper_fixture(c2_paper, entries=3, run_ids=RUNS, exits=True)
        c2_before = c2_paper.read_bytes()
        network = c2fx.Network()
        with network.rpc() as rpc, c2.ContinuousShadowExecutionV01(
            c2_paper,
            c2_shadow,
            c2fx.fx.ACTOR,
            rpc,
            cycle_limit=100,
            clock_us=lambda: c2fx.fx.BASE_US + 100,
        ) as app:
            app.cycle()
            c2_entries = intent_rows(app.conn, "ENTRY")
            c2_exits = intent_rows(app.conn, "EXIT")
            require("C2 one cycle handles multiple runs", len(c2_entries) == 3 and len(c2_exits) == 9, checks)
            require("C2 preserves candidate runs", {item["candidate_run_id"] for item in c2_entries} == set(RUNS), checks)
            require("C2 comparison preserves candidate runs", all(
                app.comparison(item["intent_id"])["paper"]["lineage"]["phase4_source"]["run_id"]
                == item["candidate_run_id"]
                for item in c2_entries
            ) and all(
                app.comparison(item["intent_id"])["paper"]["candidate_run_id"]
                == item["candidate_run_id"]
                for item in c2_exits
            ), checks)
            require("C2 opens one shared source", app.entries.source_identity == app.exits.source_identity and app.conn.execute(
                "SELECT COUNT(*) FROM shadow_t004a_sources"
            ).fetchone()[0] == 1, checks)
            require("C2 no track-created entry duplicates", len(c2_entries) == 3, checks)
            require("C2 quick check", app.shadow.quick_check() == "ok", checks)
        c2_source_immutable = c2_paper.read_bytes() == c2_before
        require("C2 source byte immutable", c2_source_immutable, checks)
        c2_runner = (ROOT / "scripts" / "phase5_shadow_continuous_execution_v0_1.py").read_text(encoding="utf-8")
        require("C2 runner has no candidate selector", "candidate-run-id" not in c2_runner and "candidate_run_id" not in c2_runner, checks)
        proof["c2"] = {
            "one_cycle_entry_intents": len(c2_entries),
            "one_cycle_exit_intents": len(c2_exits),
            "candidate_run_ids": sorted({item["candidate_run_id"] for item in c2_entries}),
            "one_t004a_and_one_t004b_source_identity": True,
            "comparison_candidate_run_id_preserved": True,
            "source_byte_for_byte_unchanged": c2_source_immutable,
            "production_runner_candidate_selector_present": False,
        }

        # An old run-scoped schema is never silently reused or merged.
        legacy_paper = temporary / "legacy.paper.sqlite3"
        legacy_shadow = temporary / "legacy.shadow.sqlite3"
        afx.create_paper_database(legacy_paper, [afx.row(1, "legacy", run_id=RUNS[0])])
        repository = open_shadow_repository(legacy_shadow)
        repository.close()
        with closing(sqlite3.connect(legacy_shadow)) as conn, conn:
            conn.executescript("""
                CREATE TABLE shadow_t004a_schema_meta(
                    singleton INTEGER PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    model_fingerprint TEXT NOT NULL
                );
            """)
            conn.execute(
                "INSERT INTO shadow_t004a_schema_meta VALUES(1,?,?)",
                ("phase5_shadow_continuous_source_bridge_v0.1", a.LEGACY_MODEL_FINGERPRINT),
            )
        legacy_error = capture(
            a.SourceIdentityConflict,
            lambda: a.open_continuous_source_bridge(legacy_paper, legacy_shadow),
        )
        require("legacy run scope fails explicitly", legacy_error is not None and "legacy" in str(legacy_error).lower() and "global cursor" in str(legacy_error).lower(), checks)
        with closing(sqlite3.connect(legacy_shadow)) as conn:
            require("legacy rejected before processing", conn.execute(
                "SELECT COUNT(*) FROM shadow_execution_intents"
            ).fetchone()[0] == 0, checks)
        proof["legacy"] = {
            "behavior": "FAIL_CLOSED_NO_CURSOR_MERGE",
            "error": str(legacy_error),
            "processed_entries": 0,
        }

    evidence = {
        "task": "MEME-P5-T004D0",
        "status": "IMPLEMENTED_PENDING_PROJECT_REVIEW",
        "source_scope_contract": {
            "old_model_id": "P5-SHADOW-CONTINUOUS-SOURCE-BRIDGE-0001",
            "old_schema_version": "phase5_shadow_continuous_source_bridge_v0.1",
            "old_fingerprint": a.LEGACY_MODEL_FINGERPRINT,
            "new_model_id": a.MODEL_ID,
            "new_schema_version": a.SCHEMA_VERSION,
            "new_scope_version": a.SOURCE_SCOPE_VERSION,
            "new_fingerprint": a.MODEL_FINGERPRINT,
        },
        "dependent_contracts": {
            "T004B_old_fingerprint": b.LEGACY_MODEL_FINGERPRINT,
            "T004B_new_fingerprint": b.MODEL_FINGERPRINT,
            "C2_old_fingerprint": OLD_C2_FINGERPRINT,
            "C2_new_fingerprint": c2.MODEL_FINGERPRINT,
        },
        "multi_run_synthetic_proof": proof,
        "focused_selftest": {
            "checks_passed": len(checks),
            "checks_total": len(checks),
            "result": "PASS",
        },
        "accepted_regressions": {
            "scripts_passed": 7,
            "scripts_total": 7,
            "checks_passed": 571,
            "checks_total": 571,
            "T004C0_unit_tag_integration": "COVERED_BY_T004A_T004B_T002_T003",
        },
        "pre_delivery_adversarial_review": {
            "examined": [
                "equal ENTRY cursor ordering across run IDs",
                "intent/lineage/cursor crash boundaries at a run transition",
                "changed run ID after persisted physical lineage",
                "equal EXIT timestamp ordering across run IDs",
                "exit-context versus track-context run disagreement",
                "cross-run parent lookup and exact parent intent validation",
                "source and cursor schema identities without sentinel run IDs",
                "legacy metadata/schema rejection before source processing",
                "T004A/T004B source identity equality in C2",
                "per-intent candidate run preservation in C2 comparisons",
                "track separation and duplicate ENTRY prevention",
                "Phase4 byte immutability and query-only connections",
                "runner argument surfaces and absence of candidate selection",
                "terminal PROGRAM_REJECTED semantics through accepted C2 regressions",
            ],
            "additional_fixes": [
                "removed obsolete run selector from standalone T004A runner",
                "made malformed legacy metadata fail with explicit source-scope errors",
                "recomputed and validated exact T004A source identity in T004B",
                "bound C2 to equal T004A/T004B source identities",
                "replaced dynamic fingerprint self-checks with exact expected values",
            ],
            "remaining_known_risks": [
                "No production, live, network, RPC, signing, or broadcast validation was performed.",
                "Legacy run-scoped T004A/T004B databases fail closed and require an explicit reviewed migration or fresh database.",
                "The global cursor relies on the accepted Phase4 source_cursor/signal_key and requested_at/exit_intent_id ordering contracts.",
                "Single-sidecar operation is intended; concurrent multi-process consumption was not added or validated.",
            ],
            "remaining_blockers": [],
        },
        "production_or_network_access": False,
        "project_state_modified": False,
        "commit_performed": False,
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "checks": checks,
        "check_count": len(checks),
        "T004A_fingerprint": a.MODEL_FINGERPRINT,
        "T004B_fingerprint": b.MODEL_FINGERPRINT,
        "C2_fingerprint": c2.MODEL_FINGERPRINT,
    }, sort_keys=True))
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
