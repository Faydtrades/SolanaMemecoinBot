from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_external_observability_run_v0_1 as integration  # noqa: E402
import phase4_external_live_observability_selftest_v0_1 as t017  # noqa: E402
import phase4_timer_fence_throughput_selftest_v0_1 as fixtures  # noqa: E402
from phase4.paper_external_live_observability_v0_1 import (  # noqa: E402
    LiveRunPublisherV01,
    ObservabilityPublicationError,
    ObserverStatus,
    ReadOnlyLiveObservabilityV01,
    RunState,
)


WORKLOAD_ROWS = 40


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def expect_error(error_type: type[BaseException], callback) -> bool:
    try:
        callback()
    except error_type:
        return True
    return False


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def run_wrapper_case(
    root: Path,
    *,
    run_id: str,
    ttl_expired: bool = False,
    heartbeat_failure: bool = False,
    terminal_failure: bool = False,
    json_replace_failure: bool = False,
    base_code: int = 0,
    result_class: str = "PASS_MULTI_HOUR",
    interrupt: bool = False,
    final_cursor_delta: int = 0,
    final_watermark_delta: int = 0,
) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    source_path = root / "source.sqlite3"
    fixtures.create_source(source_path)
    fixtures.append_rows(source_path, 1, WORKLOAD_ROWS)
    baseline_digest, baseline = t017.semantic_fixture_digest(
        source_path,
        root / "baseline.sqlite3",
    )
    registry = root / "registry.sqlite3"
    artifact = root / "run-summary.json"
    clock = t017.FakeClock()
    lower = integration.accepted_v05.accepted_v04.accepted_v03
    original_run = lower.run_multihour
    original_heartbeat = integration.LiveRunPublisherV01.publish_heartbeat
    original_finalize = integration.LiveRunPublisherV01.__dict__["finalize_existing"]
    original_replace = integration.os.replace
    captured: dict[str, Any] = {}

    def fake_run(*, duration_seconds: int, launch_collector: bool):
        del duration_seconds, launch_collector
        source = lower.ContinuousMarketSourceV02(
            source_path,
            start_after_p1_rowid=0,
            database_identity="T017:SEMANTIC-EQUIVALENCE",
        )
        conn = integration.accepted_v05.accepted.connect_entry_router_db(
            root / "observable.sqlite3"
        )
        try:
            binding = lower.ContinuousFirstPullbackBindingV03(
                conn,
                source,
                wall_clock=lambda: t017.BASE,
            )
            clock.advance(1.1)
            source.latest_p1_rowid()
            batch = binding.process_next_batch(batch_size=WORKLOAD_ROWS)
            timer = binding.prepare_clock_tick(
                t017.BASE + timedelta(seconds=1),
                timer_id="T017:EQUIVALENCE",
            )
            timer_result = binding.drain_and_execute_prepared_timer(
                timer,
                batch_size=WORKLOAD_ROWS,
            )
            captured.update(
                digest=binding.canonical_digest(),
                durable_cursor=binding.durable_p1_rowid,
                raw_rows=batch.raw_rows_fetched,
                normalized_rows=batch.normalized_records,
                timer_watermark=timer_result.requested_watermark_p1_rowid,
                pending_outbox=binding.pending_outbox_count(),
            )
        finally:
            conn.close()
            captured["binding_closed"] = expect_error(
                sqlite3.ProgrammingError,
                lambda: conn.execute("SELECT 1"),
            )
        if ttl_expired:
            clock.advance(10.0)
        summary = {
            "result_class": result_class,
            "duration_bound_reached": True,
            "production_source": {
                "final_durable_p1_rowid": (
                    WORKLOAD_ROWS + final_cursor_delta
                ),
                "end_watermark_p1_rowid": (
                    WORKLOAD_ROWS + final_watermark_delta
                ),
            },
            "errors": [] if base_code == 0 else ["INJECTED_BASE_FAILURE"],
            "artifact_paths": {"json": str(artifact.resolve())},
            "sentinel": {
                "preserve": ["all", "existing", "V0.5", "fields"],
                "value": 21,
            },
        }
        captured["base_summary"] = json.loads(json.dumps(summary))
        artifact.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        captured["base_artifact_sha256"] = file_sha256(artifact)
        if interrupt:
            raise KeyboardInterrupt("INJECTED_ABORT_AFTER_RUNTIME_TEARDOWN")
        return base_code, summary

    lower.run_multihour = fake_run
    if heartbeat_failure:
        def fail_heartbeat(_self, **_kwargs):
            raise RuntimeError("INJECTED_HEARTBEAT_PUBLICATION_FAILURE")

        integration.LiveRunPublisherV01.publish_heartbeat = fail_heartbeat
    if terminal_failure:
        def fail_terminal(_cls, *_args, **_kwargs):
            raise RuntimeError("INJECTED_INDEPENDENT_TERMINAL_FAILURE")

        integration.LiveRunPublisherV01.finalize_existing = classmethod(
            fail_terminal
        )
    if json_replace_failure:
        def fail_replace(_source, _destination):
            raise OSError("INJECTED_ATOMIC_REPLACE_FAILURE")

        integration.os.replace = fail_replace

    returned_code: int | None = None
    returned_summary: dict[str, Any] | None = None
    interrupted = False
    started = time.perf_counter()
    try:
        try:
            returned_code, returned_summary = integration.run_multihour(
                duration_seconds=14_400,
                launch_collector=False,
                registry_path=registry,
                wall_clock=clock.wall,
                monotonic_clock=clock.monotonic,
                run_id_factory=lambda: run_id,
            )
        except KeyboardInterrupt as exc:
            interrupted = "INJECTED_ABORT" in str(exc)
    finally:
        elapsed = time.perf_counter() - started
        lower.run_multihour = original_run
        integration.LiveRunPublisherV01.publish_heartbeat = original_heartbeat
        integration.LiveRunPublisherV01.finalize_existing = original_finalize
        integration.os.replace = original_replace

    with ReadOnlyLiveObservabilityV01(registry) as reader:
        records = reader.records()
        stale_view = reader.view(clock.now + timedelta(seconds=6.0))
        query_only = reader.connection_is_query_only
    record = records[0] if records else None
    persisted = (
        json.loads(artifact.read_text(encoding="utf-8"))
        if artifact.exists()
        else None
    )
    persisted_core = None
    persisted_external = None
    if isinstance(persisted, dict):
        persisted_core = dict(persisted)
        persisted_external = persisted_core.pop("external_observability", None)
    return {
        "code": returned_code,
        "summary": returned_summary,
        "interrupted": interrupted,
        "record": record,
        "record_count": len(records),
        "stale_status": stale_view.status,
        "query_only": query_only,
        "persisted_core": persisted_core,
        "persisted_external": persisted_external,
        "artifact_sha256": file_sha256(artifact) if artifact.exists() else None,
        "temporary_files": tuple(root.glob("*.t021.tmp")),
        "baseline_digest": baseline_digest,
        "baseline": baseline,
        "captured": captured,
        "elapsed_seconds": elapsed,
        "clock": clock,
        "registry": registry,
        "artifact": artifact,
    }


def make_direct_registry(
    root: Path,
    *,
    run_id: str,
    anchor: int = 0,
    cursor: int = 0,
    watermark: int = 3,
) -> tuple[Path, Path, Path, t017.FakeClock, LiveRunPublisherV01]:
    root.mkdir(parents=True, exist_ok=True)
    source = root / "source.sqlite3"
    paper = root / "paper.sqlite3"
    registry = root / "registry.sqlite3"
    t017.create_raw_source(source, marker="T021", rows=max(3, watermark))
    t017.create_paper(paper)
    clock = t017.FakeClock()
    publisher = t017.start_publisher(
        registry,
        source,
        paper,
        clock,
        run_id=run_id,
        anchor=anchor,
        cursor=cursor,
        watermark=watermark,
    )
    return registry, source, paper, clock, publisher


def main() -> int:
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}
    root = Path(tempfile.mkdtemp(prefix="phase4_t021_"))
    try:
        integration._validate_locked_contracts()

        normal = run_wrapper_case(
            root / "normal",
            run_id=t017.run_uuid(2101),
        )
        normal_record = normal["record"]
        normal_external = normal["summary"]["external_observability"]
        check(
            "1_EXACT_WRAPPER_LIFECYCLE_TERMINALIZES_AFTER_BINDING_CLOSE",
            normal["code"] == 0
            and normal["captured"]["binding_closed"]
            and normal_record.runtime_state == RunState.COMPLETED.value
            and normal_record.ended_at_utc is not None
            and normal_record.durable_source_cursor_p1_rowid == WORKLOAD_ROWS
            and normal_record.source_watermark_p1_rowid == WORKLOAD_ROWS
            and normal_record.source_watermark_kind == "FROZEN"
            and normal["stale_status"] is ObserverStatus.NO_ACTIVE_RUN
            and normal_external["terminal_publication_outcome"] == "SUCCEEDED",
            checks,
        )

        expired = run_wrapper_case(
            root / "ttl-expired",
            run_id=t017.run_uuid(2102),
            ttl_expired=True,
        )
        check(
            "2_TTL_EXPIRED_RECORD_CAN_STILL_TERMINALIZE",
            expired["record"].runtime_state == RunState.COMPLETED.value
            and expired["record"].ended_at_utc is not None
            and expired["summary"]["external_observability"][
                "terminal_publication_outcome"
            ]
            == "SUCCEEDED",
            checks,
        )

        heartbeat_failed = run_wrapper_case(
            root / "heartbeat-failed",
            run_id=t017.run_uuid(2103),
            heartbeat_failure=True,
        )
        heartbeat_external = heartbeat_failed["summary"]["external_observability"]
        check(
            "3_CLOSED_FAILED_HEARTBEAT_PUBLISHER_CANNOT_BLOCK_TERMINAL",
            heartbeat_failed["code"] == 0
            and heartbeat_failed["record"].runtime_state
            == RunState.COMPLETED.value
            and heartbeat_external["publisher_disabled"]
            and heartbeat_external["terminal_publication_outcome"] == "SUCCEEDED"
            and any(
                "INJECTED_HEARTBEAT_PUBLICATION_FAILURE" in error
                for error in heartbeat_external["publication_errors"]
            ),
            checks,
        )

        terminal_failed = run_wrapper_case(
            root / "terminal-failed",
            run_id=t017.run_uuid(2104),
            terminal_failure=True,
            ttl_expired=True,
        )
        terminal_external = terminal_failed["summary"]["external_observability"]
        check(
            "4_TERMINAL_FAILURE_PERSISTS_EXACT_FORENSIC_ERROR_WITH_PASS",
            terminal_failed["code"] == 0
            and terminal_failed["record"].runtime_state == RunState.RUNNING.value
            and terminal_failed["stale_status"] is ObserverStatus.NO_ACTIVE_RUN
            and terminal_external == terminal_failed["persisted_external"]
            and terminal_external["terminal_publication_outcome"] == "FAILED"
            and terminal_external["forensic_json_outcome"] == "SUCCEEDED"
            and any(
                "INJECTED_INDEPENDENT_TERMINAL_FAILURE" in error
                for error in terminal_external["publication_errors"]
            ),
            checks,
        )

        failed_base = run_wrapper_case(
            root / "failed-base",
            run_id=t017.run_uuid(2105),
            base_code=1,
            result_class="FAIL",
        )
        check(
            "5_FAILED_BASE_RUNTIME_TERMINALIZES_FAILED_UNCHANGED",
            failed_base["code"] == 1
            and failed_base["summary"]["result_class"] == "FAIL"
            and failed_base["record"].runtime_state == RunState.FAILED.value
            and failed_base["summary"]["external_observability"][
                "actual_terminal_state"
            ]
            == RunState.FAILED.value,
            checks,
        )

        aborted = run_wrapper_case(
            root / "aborted",
            run_id=t017.run_uuid(2106),
            interrupt=True,
        )
        check(
            "6_ABORTED_PATH_TERMINALIZES_AND_RERAISES_INTERRUPT",
            aborted["interrupted"]
            and aborted["record"].runtime_state == RunState.ABORTED.value
            and aborted["record"].ended_at_utc is not None,
            checks,
        )

        check(
            "7_TERMINAL_RECORD_CANNOT_TRANSITION_OR_RESURRECT",
            expect_error(
                ObservabilityPublicationError,
                lambda: LiveRunPublisherV01.finalize_existing(
                    normal["registry"],
                    run_id=t017.run_uuid(2101),
                    state=RunState.FAILED,
                    durable_source_cursor_p1_rowid=WORKLOAD_ROWS,
                    source_watermark_p1_rowid=WORKLOAD_ROWS,
                    source_watermark_kind="FROZEN",
                    ended_at_utc=normal["clock"].wall(),
                ),
            ),
            checks,
        )

        exact_root = root / "exact-run-id"
        registry, source, paper, clock, first = make_direct_registry(
            exact_root,
            run_id=t017.run_uuid(2110),
        )
        second = t017.start_publisher(
            registry,
            source,
            paper,
            clock,
            run_id=t017.run_uuid(2111),
        )
        first.close()
        second.close()
        clock.advance(20.0)
        missing_rejected = expect_error(
            ObservabilityPublicationError,
            lambda: LiveRunPublisherV01.finalize_existing(
                registry,
                run_id=t017.run_uuid(2199),
                state=RunState.COMPLETED,
                durable_source_cursor_p1_rowid=0,
                source_watermark_p1_rowid=3,
                source_watermark_kind="CURRENT",
                ended_at_utc=clock.wall(),
            ),
        )
        LiveRunPublisherV01.finalize_existing(
            registry,
            run_id=t017.run_uuid(2110),
            state=RunState.COMPLETED,
            durable_source_cursor_p1_rowid=0,
            source_watermark_p1_rowid=3,
            source_watermark_kind="CURRENT",
            ended_at_utc=clock.wall(),
        )
        with ReadOnlyLiveObservabilityV01(registry) as reader:
            exact_states = {
                record.run_id: record.runtime_state for record in reader.records()
            }
        check(
            "8_EXACT_RUN_ID_ONLY_MISSING_NEVER_SELECTS_LATEST",
            missing_rejected
            and exact_states[t017.run_uuid(2110)] == RunState.COMPLETED.value
            and exact_states[t017.run_uuid(2111)] == RunState.RUNNING.value,
            checks,
        )

        invalid_cursor = run_wrapper_case(
            root / "invalid-cursor",
            run_id=t017.run_uuid(2120),
            final_cursor_delta=-1,
            final_watermark_delta=0,
        )
        invalid_watermark = run_wrapper_case(
            root / "invalid-watermark",
            run_id=t017.run_uuid(2121),
            final_cursor_delta=0,
            final_watermark_delta=-1,
        )
        time_registry, _, _, time_clock, time_publisher = make_direct_registry(
            root / "backward-terminal-time",
            run_id=t017.run_uuid(2122),
        )
        time_publisher.close()
        backward_time_rejected = expect_error(
            ObservabilityPublicationError,
            lambda: LiveRunPublisherV01.finalize_existing(
                time_registry,
                run_id=t017.run_uuid(2122),
                state=RunState.COMPLETED,
                durable_source_cursor_p1_rowid=0,
                source_watermark_p1_rowid=3,
                source_watermark_kind="CURRENT",
                ended_at_utc=time_clock.now - timedelta(seconds=1),
            ),
        )
        with ReadOnlyLiveObservabilityV01(time_registry) as time_reader:
            time_record = time_reader.records()[0]
        check(
            "9_FINAL_CURSOR_AND_WATERMARK_VALIDATION_ISOLATED_FROM_TRADING",
            invalid_cursor["code"] == 0
            and invalid_watermark["code"] == 0
            and invalid_cursor["record"].runtime_state == RunState.RUNNING.value
            and invalid_watermark["record"].runtime_state
            == RunState.RUNNING.value
            and invalid_cursor["summary"]["external_observability"][
                "terminal_publication_outcome"
            ]
            == "FAILED"
            and invalid_watermark["summary"]["external_observability"][
                "terminal_publication_outcome"
            ]
            == "FAILED"
            and backward_time_rejected
            and time_record.runtime_state == RunState.RUNNING.value
            and time_record.ended_at_utc is None,
            checks,
        )

        json_failed = run_wrapper_case(
            root / "json-failed",
            run_id=t017.run_uuid(2130),
            json_replace_failure=True,
        )
        malformed = root / "malformed.json"
        malformed.write_text('{"partial":', encoding="utf-8")
        malformed_before = file_sha256(malformed)
        malformed_summary = {"artifact_paths": {"json": str(malformed)}}
        malformed_rejected = expect_error(
            RuntimeError,
            lambda: integration._persist_external_observability_json(
                malformed_summary,
                {"never": "written"},
            ),
        )
        check(
            "10_ATOMIC_JSON_PRESERVES_CORE_AND_REJECTS_PARTIAL_REPLACEMENT",
            normal["persisted_core"] == normal["captured"]["base_summary"]
            and normal["persisted_external"] == normal_external
            and json_failed["record"].runtime_state == RunState.COMPLETED.value
            and json_failed["artifact_sha256"]
            == json_failed["captured"]["base_artifact_sha256"]
            and json_failed["persisted_external"] is None
            and json_failed["summary"]["external_observability"][
                "forensic_json_outcome"
            ]
            == "FAILED"
            and any(
                "INJECTED_ATOMIC_REPLACE_FAILURE" in error
                for error in json_failed["summary"]["external_observability"][
                    "publication_errors"
                ]
            )
            and not json_failed["temporary_files"]
            and malformed_rejected
            and file_sha256(malformed) == malformed_before,
            checks,
        )

        check(
            "11_REOPEN_READ_RETAINS_EXACT_TERMINAL_POSITION",
            normal_record.runtime_state == RunState.COMPLETED.value
            and normal_record.durable_source_cursor_p1_rowid == WORKLOAD_ROWS
            and normal_record.source_watermark_p1_rowid == WORKLOAD_ROWS,
            checks,
        )
        check(
            "12_READ_ONLY_CONSUMER_REMAINS_QUERY_ONLY_NO_CONTROL_PATH",
            normal["query_only"]
            and not any(
                token in name.lower()
                for name in dir(ReadOnlyLiveObservabilityV01)
                for token in ("publish", "finalize", "control", "execute")
            ),
            checks,
        )
        check(
            "13_SEMANTIC_DIGEST_AND_RUNTIME_RESULT_NON_INTERFERENCE",
            normal["baseline_digest"] == normal["captured"]["digest"]
            and heartbeat_failed["baseline_digest"]
            == heartbeat_failed["captured"]["digest"]
            and terminal_failed["baseline_digest"]
            == terminal_failed["captured"]["digest"]
            and normal["baseline"]
            == {
                "raw_rows": normal["captured"]["raw_rows"],
                "normalized_rows": normal["captured"]["normalized_rows"],
                "durable_cursor": normal["captured"]["durable_cursor"],
                "timer_status": "COMMITTED",
                "timer_watermark": normal["captured"]["timer_watermark"],
                "pending_outbox": normal["captured"]["pending_outbox"],
            },
            checks,
        )

        busy_root = root / "busy-retry"
        busy_registry, _, _, busy_clock, busy_publisher = make_direct_registry(
            busy_root,
            run_id=t017.run_uuid(2140),
        )
        busy_publisher.close()
        blocker = sqlite3.connect(busy_registry, check_same_thread=False)
        blocker.execute("BEGIN IMMEDIATE")

        def release_lock() -> None:
            time.sleep(0.05)
            blocker.commit()
            blocker.close()

        releaser = threading.Thread(target=release_lock)
        releaser.start()
        busy_started = time.perf_counter()
        LiveRunPublisherV01.finalize_existing(
            busy_registry,
            run_id=t017.run_uuid(2140),
            state=RunState.COMPLETED,
            durable_source_cursor_p1_rowid=0,
            source_watermark_p1_rowid=3,
            source_watermark_kind="CURRENT",
            ended_at_utc=busy_clock.wall(),
        )
        busy_elapsed = time.perf_counter() - busy_started
        releaser.join()
        check(
            "15_BOUNDED_FINALIZER_AND_SQLITE_BUSY_RETRY",
            normal["elapsed_seconds"] < 5.0
            and busy_elapsed < 5.0,
            checks,
        )

        evidence = {
            "integration_model_id": integration.MODEL_ID,
            "integration_fingerprint": integration.MODEL_FINGERPRINT,
            "contract_fingerprint": integration.OBSERVABILITY_FINGERPRINT,
            "semantic_digest": normal["baseline_digest"],
            "normal_finalizer_seconds": normal["elapsed_seconds"],
            "busy_retry_finalizer_seconds": busy_elapsed,
            "normal_external_observability": normal_external,
            "terminal_failure_external_observability": terminal_external,
            "json_failure_external_observability": json_failed["summary"][
                "external_observability"
            ],
        }
    finally:
        shutil.rmtree(root, ignore_errors=True)

    print("=" * 112)
    print("EXTERNAL OBSERVABILITY TERMINAL FINALIZATION CORRECTION v0.1 SELF-TEST")
    print("=" * 112)
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    print(
        "EVIDENCE: "
        f"semantic_digest={evidence.get('semantic_digest')} "
        f"normal_seconds={evidence.get('normal_finalizer_seconds', 0.0):.6f} "
        f"busy_retry_seconds={evidence.get('busy_retry_finalizer_seconds', 0.0):.6f}"
    )
    print(f"INTEGRATION_FINGERPRINT: {evidence.get('integration_fingerprint')}")
    passed = bool(checks) and all(checks.values())
    print(f"CHECKS: {sum(checks.values())}/{len(checks)}")
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
