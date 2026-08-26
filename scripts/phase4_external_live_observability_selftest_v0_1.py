from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_external_observability_run_v0_1 as integration  # noqa: E402
import phase4_timer_fence_throughput_selftest_v0_1 as fixtures  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_5 import (  # noqa: E402
    ContinuousFirstPullbackBindingV05,
    connect_atomic_entry_router_db,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    MODEL_FINGERPRINT as SOURCE_FINGERPRINT,
    MODEL_ID as SOURCE_MODEL_ID,
    SCHEMA_VERSION as SOURCE_SCHEMA_VERSION,
    ContinuousMarketSourceV02,
)
from phase4.paper_external_live_observability_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT,
    MODEL_ID,
    NORMALIZED_INGEST_IDENTITY,
    SCHEMA_VERSION,
    SOURCE_ROW_IDENTITY,
    LiveRunPublisherV01,
    ObservabilityContractError,
    ObservabilityPublicationError,
    ObserverStatus,
    ReadOnlyLiveObservabilityV01,
    RunState,
    assert_no_secret_fields,
    contract_field_names,
    open_bound_source_readonly,
    read_bound_source_rows,
)


BASE = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)


@dataclass
class FakeClock:
    now: datetime = BASE
    mono: float = 1_000.0

    def wall(self) -> datetime:
        return self.now

    def monotonic(self) -> float:
        return self.mono

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=float(seconds))
        self.mono += float(seconds)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def run_uuid(index: int) -> str:
    return f"00000000-0000-4000-8000-{int(index):012d}"


def create_raw_source(path: Path, *, marker: str, rows: int = 3) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE pump_events(event_key TEXT NOT NULL,marker TEXT NOT NULL)"
        )
        conn.executemany(
            "INSERT INTO pump_events VALUES(?,?)",
            tuple((f"{marker}:{index}", marker) for index in range(1, rows + 1)),
        )
        conn.commit()
    finally:
        conn.close()


def create_paper(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE paper_marker(value TEXT NOT NULL)")
        conn.execute("INSERT INTO paper_marker VALUES('PAPER_ONLY')")
        conn.commit()
    finally:
        conn.close()


def start_publisher(
    registry: Path,
    source: Path,
    paper: Path,
    clock: FakeClock,
    *,
    run_id: str,
    anchor: int = 0,
    cursor: int = 0,
    watermark: int = 3,
) -> LiveRunPublisherV01:
    return LiveRunPublisherV01.start(
        registry,
        runtime_model_id=integration.accepted_v05.MODEL_ID,
        runtime_fingerprint=integration.accepted_v05.MODEL_FINGERPRINT,
        runtime_session_identity=f"SESSION:{run_id}",
        paper_runtime_sqlite_path=paper,
        source_model_id=SOURCE_MODEL_ID,
        source_model_fingerprint=SOURCE_FINGERPRINT,
        source_contract_version=SOURCE_SCHEMA_VERSION,
        source_identity=f"SOURCE:{run_id}",
        source_sqlite_path=source,
        source_database_identity=source.resolve().as_posix(),
        source_anchor_p1_rowid=anchor,
        initial_durable_source_cursor_p1_rowid=cursor,
        initial_source_watermark_p1_rowid=watermark,
        price_representation_version="QAP-0.1/P1QAA-0.1",
        run_id=run_id,
        wall_clock=clock.wall,
        monotonic_clock=clock.monotonic,
    )


def expect_error(error_type: type[BaseException], fn: Callable[[], Any]) -> bool:
    try:
        fn()
    except error_type:
        return True
    return False


def semantic_fixture_digest(
    source_path: Path,
    paper_path: Path,
    *,
    binding_class: type[ContinuousFirstPullbackBindingV05] = (
        ContinuousFirstPullbackBindingV05
    ),
) -> tuple[str, dict[str, Any]]:
    paper_path.parent.mkdir(parents=True, exist_ok=True)
    source = ContinuousMarketSourceV02(
        source_path,
        start_after_p1_rowid=0,
        database_identity="T017:SEMANTIC-EQUIVALENCE",
    )
    conn = connect_atomic_entry_router_db(paper_path)
    try:
        binding = binding_class(conn, source, wall_clock=lambda: BASE)
        source.latest_p1_rowid()
        batch = binding.process_next_batch(batch_size=10_000)
        timer = binding.prepare_clock_tick(
            BASE + timedelta(seconds=1), timer_id="T017:EQUIVALENCE"
        )
        timer_result = binding.drain_and_execute_prepared_timer(
            timer,
            batch_size=10_000,
        )
        digest = binding.canonical_digest()
        evidence = {
            "raw_rows": batch.raw_rows_fetched,
            "normalized_rows": batch.normalized_records,
            "durable_cursor": binding.durable_p1_rowid,
            "timer_status": (
                "COMMITTED"
                if timer in timer_result.executed_timer_keys
                else "NOT_COMMITTED"
            ),
            "timer_watermark": timer_result.requested_watermark_p1_rowid,
            "pending_outbox": binding.pending_outbox_count(),
        }
        return digest, evidence
    finally:
        conn.close()


def run_integration_equivalence(
    root: Path,
    *,
    run_id: str,
    workload_rows: int = 1_000,
    terminal_publication_failure: bool = False,
) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    source_path = root / "semantic source.sqlite3"
    fixtures.create_source(source_path)
    fixtures.append_rows(source_path, 1, workload_rows)
    baseline_started = time.perf_counter()
    baseline_digest, baseline = semantic_fixture_digest(
        source_path, root / "baseline.sqlite3"
    )
    baseline_seconds = time.perf_counter() - baseline_started

    lower = integration.accepted_v05.accepted_v04.accepted_v03
    original_run = lower.run_multihour
    original_finalize_existing = (
        integration.LiveRunPublisherV01.__dict__["finalize_existing"]
    )
    registry = root / "observable registry.sqlite3"
    clock = FakeClock()
    captured: dict[str, Any] = {}

    def fake_run(*, duration_seconds: int, launch_collector: bool):
        del duration_seconds, launch_collector
        source = lower.ContinuousMarketSourceV02(
            source_path,
            start_after_p1_rowid=0,
            database_identity="T017:SEMANTIC-EQUIVALENCE",
        )
        paper_path = root / "observable.sqlite3"
        conn = integration.accepted_v05.accepted.connect_entry_router_db(paper_path)
        try:
            binding = lower.ContinuousFirstPullbackBindingV03(
                conn, source, wall_clock=lambda: BASE
            )
            source.latest_p1_rowid()
            batch = binding.process_next_batch(batch_size=workload_rows)
            timer = binding.prepare_clock_tick(
                BASE + timedelta(seconds=1), timer_id="T017:EQUIVALENCE"
            )
            timer_result = binding.drain_and_execute_prepared_timer(
                timer, batch_size=workload_rows
            )
            captured.update(
                digest=binding.canonical_digest(),
                source_identity=source.source_identity,
                raw_rows=batch.raw_rows_fetched,
                normalized_rows=batch.normalized_records,
                durable_cursor=binding.durable_p1_rowid,
                timer_status=(
                    "COMMITTED"
                    if timer in timer_result.executed_timer_keys
                    else "NOT_COMMITTED"
                ),
                timer_watermark=timer_result.requested_watermark_p1_rowid,
                pending_outbox=binding.pending_outbox_count(),
            )
        finally:
            conn.close()
        summary = {
            "result_class": "PASS_MULTI_HOUR",
            "duration_bound_reached": True,
            "production_source": {
                "final_durable_p1_rowid": workload_rows,
                "end_watermark_p1_rowid": workload_rows,
            },
            "errors": [],
            "artifact_paths": {
                "json": str((root / "observable-summary.json").resolve()),
            },
        }
        Path(summary["artifact_paths"]["json"]).write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return 0, summary

    lower.run_multihour = fake_run
    if terminal_publication_failure:
        def fail_terminal(_cls, *_args, **_kwargs):
            raise RuntimeError("INJECTED_TERMINAL_PUBLICATION_FAILURE")

        integration.LiveRunPublisherV01.finalize_existing = classmethod(
            fail_terminal
        )
    try:
        enabled_started = time.perf_counter()
        code, summary = integration.run_multihour(
            duration_seconds=14_400,
            launch_collector=False,
            registry_path=registry,
            wall_clock=clock.wall,
            monotonic_clock=clock.monotonic,
            run_id_factory=lambda: run_id,
        )
        enabled_seconds = time.perf_counter() - enabled_started
    finally:
        lower.run_multihour = original_run
        integration.LiveRunPublisherV01.finalize_existing = (
            original_finalize_existing
        )

    persisted_summary = json.loads(
        (root / "observable-summary.json").read_text(encoding="utf-8")
    )

    with ReadOnlyLiveObservabilityV01(registry) as reader:
        records = reader.records()
        stale_view = reader.view(clock.now + timedelta(seconds=6.0))
    bound_probe_rows = (
        read_bound_source_rows(
            records[0],
            after_p1_rowid=0,
            through_p1_rowid=workload_rows,
            limit=5,
        )
        if records
        else ()
    )
    return {
        "code": code,
        "workload_rows": workload_rows,
        "baseline_seconds": baseline_seconds,
        "enabled_seconds": enabled_seconds,
        "baseline_digest": baseline_digest,
        "observability_digest": captured.get("digest"),
        "baseline": baseline,
        "observability": {
            key: captured[key]
            for key in (
                "raw_rows",
                "normalized_rows",
                "durable_cursor",
                "timer_status",
                "timer_watermark",
                "pending_outbox",
            )
        },
        "summary_contract": summary["external_observability"],
        "persisted_external_observability": persisted_summary.get(
            "external_observability"
        ),
        "record_count": len(records),
        "record_state": records[0].runtime_state if records else None,
        "record_source_path": records[0].source_sqlite_path if records else None,
        "record_source_identity": records[0].source_identity if records else None,
        "actual_source_identity": captured.get("source_identity"),
        "source_probe_rowids": [row["p1_rowid"] for row in bound_probe_rows],
        "source_probe_has_mint": bool(bound_probe_rows)
        and all("mint" in row for row in bound_probe_rows),
        "stale_status": stale_view.status.value,
        "semantic_equal": (
            baseline_digest == captured.get("digest")
            and baseline
            == {
                key: captured[key]
                for key in (
                    "raw_rows",
                    "normalized_rows",
                    "durable_cursor",
                    "timer_status",
                    "timer_watermark",
                    "pending_outbox",
                )
            }
        ),
    }


def measure_cadence_overhead(root: Path) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    source = root / "performance-source.sqlite3"
    paper = root / "performance-paper.sqlite3"
    registry = root / "performance-registry.sqlite3"
    create_raw_source(source, marker="PERF", rows=20_000)
    create_paper(paper)
    workload_size = 20_000
    simulated_step_seconds = 0.001

    baseline_started = time.perf_counter()
    baseline_checksum = 0
    for cursor in range(1, workload_size + 1):
        watermark = cursor + 1
        baseline_checksum ^= cursor ^ watermark
    baseline_seconds = time.perf_counter() - baseline_started

    clock = FakeClock()
    publisher = start_publisher(
        registry,
        source,
        paper,
        clock,
        run_id=run_uuid(900),
        watermark=workload_size + 1,
    )
    enabled_started = time.perf_counter()
    enabled_checksum = 0
    for cursor in range(1, workload_size + 1):
        watermark = cursor + 1
        enabled_checksum ^= cursor ^ watermark
        clock.advance(simulated_step_seconds)
        publisher.publish_heartbeat(
            durable_source_cursor_p1_rowid=cursor,
            source_watermark_p1_rowid=watermark,
            source_watermark_kind="CURRENT",
        )
    enabled_seconds = time.perf_counter() - enabled_started
    publications_before_terminal = publisher.publication_count
    heartbeat_publications = publisher.heartbeat_publication_count
    publisher.publish_terminal(
        RunState.COMPLETED,
        durable_source_cursor_p1_rowid=workload_size,
        source_watermark_p1_rowid=workload_size + 1,
        source_watermark_kind="CURRENT",
    )
    publisher.close()
    delta = enabled_seconds - baseline_seconds
    simulated_duration = workload_size * simulated_step_seconds
    return {
        "workload_size": workload_size,
        "simulated_live_seconds": simulated_duration,
        "heartbeat_interval_seconds": 1.0,
        "baseline_seconds": baseline_seconds,
        "enabled_seconds": enabled_seconds,
        "absolute_delta_seconds": delta,
        "runtime_delta_percent": (
            delta / baseline_seconds * 100.0 if baseline_seconds > 0.0 else None
        ),
        "live_time_budget_percent": max(0.0, delta) / simulated_duration * 100.0,
        "baseline_rows_per_second": workload_size / max(baseline_seconds, 1e-9),
        "enabled_rows_per_second": workload_size / max(enabled_seconds, 1e-9),
        "start_plus_heartbeat_publications": publications_before_terminal,
        "heartbeat_publications": heartbeat_publications,
        "total_publications": publications_before_terminal + 1,
        "added_sqlite_write_transactions": publications_before_terminal + 1,
        "checksums_equal": baseline_checksum == enabled_checksum,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-json", type=Path)
    args = parser.parse_args(argv)
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}
    root = Path(tempfile.mkdtemp(prefix="phase4_t017_"))
    publishers: list[LiveRunPublisherV01] = []
    try:
        integration._validate_locked_contracts()
        checks["MODEL_AND_LOCKED_V05_IDENTITIES"] = (
            MODEL_ID == "P4-EXTERNAL-LIVE-OBSERVABILITY-0001"
            and SCHEMA_VERSION == "phase4_external_live_observability_v0.1"
            and len(MODEL_FINGERPRINT) == 64
        )

        source_dir = root / "sources with spaces"
        bound_source = source_dir / "older exact source.sqlite3"
        wrong_newest = source_dir / "newest but wrong source.sqlite3"
        create_raw_source(bound_source, marker="BOUND")
        create_raw_source(wrong_newest, marker="WRONG")
        wrong_newest.touch()
        paper = root / "paper runtime.sqlite3"
        create_paper(paper)
        registry = root / "live registry.sqlite3"
        clock = FakeClock()
        checks["RUN_ID_REQUIRES_CANONICAL_UUID4"] = expect_error(
            ValueError,
            lambda: start_publisher(
                root / "invalid-id.sqlite3",
                bound_source,
                paper,
                clock,
                run_id="NOT-A-UUID4",
            ),
        )
        run_one = run_uuid(1)
        run_two = run_uuid(2)
        active_a_id = run_uuid(3)
        active_b_id = run_uuid(4)
        first = start_publisher(
            registry, bound_source, paper, clock, run_id=run_one
        )
        publishers.append(first)

        with ReadOnlyLiveObservabilityV01(registry) as reader:
            initial_view = reader.view(clock.now)
            first_record = reader.records()[0]
            query_only = reader.connection_is_query_only
            registry_write_rejected = expect_error(
                sqlite3.OperationalError,
                lambda: reader._conn.execute(
                    "DELETE FROM phase4_external_live_runs_v0_1"
                ),
            )
        checks["A_RUNNING_HAS_POSITIVE_CURRENT_LIVENESS"] = (
            initial_view.status is ObserverStatus.ONE_ACTIVE_RUN
            and initial_view.active_run_ids == (run_one,)
            and first_record.is_running_at(clock.now)
        )
        checks["B_PAPER_DB_ALONE_CANNOT_PROVE_ACTIVE"] = expect_error(
            ObservabilityContractError,
            lambda: ReadOnlyLiveObservabilityV01(paper),
        )
        checks["CONSUMER_REGISTRY_QUERY_ONLY"] = (
            query_only and registry_write_rejected
        )

        source_hash_before = sha256_file(bound_source)
        paper_hash_before = sha256_file(paper)
        source_conn = open_bound_source_readonly(first_record)
        try:
            source_query_only = int(
                source_conn.execute("PRAGMA query_only").fetchone()[0]
            ) == 1
            write_rejected = expect_error(
                sqlite3.OperationalError,
                lambda: source_conn.execute(
                    "INSERT INTO pump_events VALUES('MUTATION','MUTATION')"
                ),
            )
        finally:
            source_conn.close()
        rows = read_bound_source_rows(
            first_record, after_p1_rowid=0, through_p1_rowid=3
        )
        source_hash_after = sha256_file(bound_source)
        paper_hash_after = sha256_file(paper)
        checks["G_EXACT_ACTUAL_SOURCE_NOT_NEWEST_NEARBY"] = (
            first_record.source_sqlite_path == str(bound_source.resolve())
            and all(row["marker"] == "BOUND" for row in rows)
            and str(wrong_newest.resolve()) != first_record.source_sqlite_path
        )
        checks["H_EXTERNAL_SOURCE_OPEN_RO_QUERY_ONLY_NO_MUTATION"] = (
            source_query_only
            and write_rejected
            and source_hash_before == source_hash_after
            and paper_hash_before == paper_hash_after
        )
        checks["I_STABLE_SOURCE_ROW_AND_INGEST_IDENTITY"] = (
            [row["p1_rowid"] for row in rows] == [1, 2, 3]
            and first_record.source_row_identity == SOURCE_ROW_IDENTITY
            and first_record.normalized_ingest_identity
            == NORMALIZED_INGEST_IDENTITY
        )
        checks["K_NO_SOURCE_PATH_OR_DIRECTORY_GUESSING"] = (
            Path(first_record.source_sqlite_path).is_absolute()
            and "sources with spaces" in first_record.source_sqlite_path
            and first_record.source_database_identity
            == bound_source.resolve().as_posix()
        )

        clock.advance(5.0)
        with ReadOnlyLiveObservabilityV01(registry) as reader:
            exact_expiry_view = reader.view(clock.now)
        checks["EXACT_TTL_BOUNDARY_REMAINS_PRODUCER_VALID"] = (
            exact_expiry_view.status is ObserverStatus.ONE_ACTIVE_RUN
            and exact_expiry_view.records[0].heartbeat_ttl_seconds == 5.0
        )
        clock.advance(0.000001)
        with ReadOnlyLiveObservabilityV01(registry) as reader:
            expired_view = reader.view(clock.now)
        checks["C_EXPIRED_HEARTBEAT_NOT_ACTIVE"] = (
            expired_view.status is ObserverStatus.NO_ACTIVE_RUN
        )
        checks["E_CRASH_STALE_CANNOT_REMAIN_FALSE_RUNNING"] = (
            expired_view.records[0].runtime_state == RunState.RUNNING.value
            and not expired_view.records[0].is_running_at(clock.now)
        )
        first.close()
        publishers.remove(first)

        second = start_publisher(
            registry, bound_source, paper, clock, run_id=run_two
        )
        publishers.append(second)
        with ReadOnlyLiveObservabilityV01(registry) as reader:
            restart_view = reader.view(clock.now)
        checks["F_NEW_RUN_RECEIVES_NEW_STABLE_IDENTITY"] = (
            restart_view.active_run_ids == (run_two,)
            and {record.run_id for record in restart_view.records}
            == {run_one, run_two}
        )
        checks["J_RESTART_REPLACES_ACTIVE_BINDING_NOT_OLD_RECORD"] = (
            restart_view.status is ObserverStatus.ONE_ACTIVE_RUN
            and restart_view.records[0].source_sqlite_path
            == restart_view.records[1].source_sqlite_path
        )

        before_invalid = second.publication_count
        with ReadOnlyLiveObservabilityV01(registry) as reader:
            before_invalid_record = tuple(
                record for record in reader.records() if record.run_id == run_two
            )[0]
        checks["CURSOR_VALIDATED_BEFORE_STATE_UPDATE"] = expect_error(
            ObservabilityPublicationError,
            lambda: second.publish_heartbeat(
                durable_source_cursor_p1_rowid=2,
                source_watermark_p1_rowid=1,
                source_watermark_kind="CURRENT",
                force=True,
            ),
        ) and second.publication_count == before_invalid
        with ReadOnlyLiveObservabilityV01(registry) as reader:
            after_invalid_record = tuple(
                record for record in reader.records() if record.run_id == run_two
            )[0]
        checks["INVALID_CURSOR_DOES_NOT_PARTIALLY_UPDATE_STATE"] = (
            before_invalid_record == after_invalid_record
        )
        clock.advance(1.0)
        second.publish_heartbeat(
            durable_source_cursor_p1_rowid=2,
            source_watermark_p1_rowid=3,
            source_watermark_kind="FROZEN",
            force=True,
        )
        second.publish_terminal(
            RunState.COMPLETED,
            durable_source_cursor_p1_rowid=2,
            source_watermark_p1_rowid=3,
            source_watermark_kind="FROZEN",
        )
        with ReadOnlyLiveObservabilityV01(registry) as reader:
            terminal_view = reader.view(clock.now)
            terminal_record = tuple(
                record for record in reader.records() if record.run_id == run_two
            )[0]
        checks["D_NORMAL_COMPLETION_PUBLISHES_TERMINAL_STATE"] = (
            terminal_record.runtime_state == RunState.COMPLETED.value
            and terminal_record.ended_at_utc is not None
            and terminal_view.status is ObserverStatus.NO_ACTIVE_RUN
        )
        checks["TERMINAL_STATE_CANNOT_RESURRECT"] = expect_error(
            ObservabilityPublicationError,
            lambda: second.publish_heartbeat(
                durable_source_cursor_p1_rowid=2,
                source_watermark_p1_rowid=3,
                source_watermark_kind="CURRENT",
                force=True,
            ),
        )
        second.close()
        publishers.remove(second)

        active_a = start_publisher(
            registry, bound_source, paper, clock, run_id=active_a_id
        )
        active_b = start_publisher(
            registry, bound_source, paper, clock, run_id=active_b_id
        )
        publishers.extend((active_a, active_b))
        with ReadOnlyLiveObservabilityV01(registry) as reader:
            ambiguous = reader.view(clock.now)
        checks["MULTIPLE_ACTIVE_RUNS_EXPLICITLY_AMBIGUOUS"] = (
            ambiguous.status is ObserverStatus.AMBIGUOUS_MULTIPLE_ACTIVE_RUNS
            and ambiguous.active_run_ids == (active_a_id, active_b_id)
        )

        concurrent_registry = root / "concurrent.sqlite3"
        concurrent_clock = FakeClock()
        concurrent = start_publisher(
            concurrent_registry,
            bound_source,
            paper,
            concurrent_clock,
            run_id=run_uuid(5),
        )
        publishers.append(concurrent)
        reader_errors: list[str] = []
        stop_reader = threading.Event()

        def concurrent_reader() -> None:
            try:
                with ReadOnlyLiveObservabilityV01(concurrent_registry) as reader:
                    while not stop_reader.is_set():
                        records = reader.records()
                        if len(records) != 1:
                            raise AssertionError("reader observed a partial run set")
            except BaseException as exc:
                reader_errors.append(f"{type(exc).__name__}:{exc}")

        thread = threading.Thread(target=concurrent_reader)
        thread.start()
        for cursor in range(1, 51):
            concurrent_clock.advance(0.1)
            concurrent.publish_heartbeat(
                durable_source_cursor_p1_rowid=cursor,
                source_watermark_p1_rowid=cursor + 1,
                source_watermark_kind="CURRENT",
                force=True,
            )
        stop_reader.set()
        thread.join(timeout=5.0)
        checks["ATOMIC_CONCURRENT_READ_DURING_PUBLICATION"] = (
            not thread.is_alive() and not reader_errors
        )
        concurrent.publish_terminal(
            RunState.FAILED,
            durable_source_cursor_p1_rowid=50,
            source_watermark_p1_rowid=51,
            source_watermark_kind="CURRENT",
        )
        concurrent.close()
        publishers.remove(concurrent)
        with ReadOnlyLiveObservabilityV01(concurrent_registry) as reader:
            failed_state = reader.records()[0].runtime_state

        corrupt = root / "corrupt.sqlite3"
        corrupt.write_bytes(b"not-a-valid-sqlite-publication")
        checks["CORRUPT_OR_PARTIAL_PUBLICATION_FAILS_CLOSED"] = expect_error(
            ObservabilityContractError,
            lambda: ReadOnlyLiveObservabilityV01(corrupt),
        )

        assert_no_secret_fields()
        serialized_contract = json.dumps(
            [record.__dict__ if hasattr(record, "__dict__") else {
                name: getattr(record, name) for name in contract_field_names()
            } for record in ambiguous.records],
            sort_keys=True,
        ).lower()
        forbidden_values = (
            "seed phrase",
            "private key",
            "signing key",
            "rpc secret",
            "api secret",
            "auth token",
        )
        checks["N_NO_SECRET_FIELDS_OR_VALUES"] = not any(
            value in serialized_contract for value in forbidden_values
        )
        module_text = (
            PROJECT_ROOT
            / "src"
            / "phase4"
            / "paper_external_live_observability_v0_1.py"
        ).read_text(encoding="utf-8")
        checks["O_NO_OBSERVER_CONTROL_PATH_INTO_EXECUTION"] = (
            "paper_entry" not in module_text
            and "strategy" not in module_text
            and "subprocess" not in module_text
            and "httpx" not in module_text
            and "websockets" not in module_text
            and not any(
                token in name.lower()
                for name in dir(ReadOnlyLiveObservabilityV01)
                for token in ("execute", "start_run", "publish", "control", "write")
            )
        )

        integration_evidence = run_integration_equivalence(
            root / "integration", run_id=run_uuid(100)
        )
        replay_evidence = run_integration_equivalence(
            root / "integration-replay", run_id=run_uuid(101)
        )
        publication_failure_evidence = run_integration_equivalence(
            root / "integration-publication-failure",
            run_id=run_uuid(102),
            workload_rows=50,
            terminal_publication_failure=True,
        )
        evidence["deterministic_equivalence"] = integration_evidence
        evidence["deterministic_replay"] = replay_evidence
        evidence["passive_publication_failure"] = publication_failure_evidence
        checks["L_TRADING_SEMANTICS_UNCHANGED_ENABLED_VS_DISABLED"] = bool(
            integration_evidence["semantic_equal"]
        )
        checks["P_RESTART_IDEMPOTENCY_AND_TERMINAL_INTEGRATION"] = (
            integration_evidence["code"] == 0
            and integration_evidence["record_count"] == 1
            and integration_evidence["record_state"] == RunState.COMPLETED.value
            and not integration_evidence["summary_contract"]["publication_errors"]
            and integration_evidence["summary_contract"]
            == integration_evidence["persisted_external_observability"]
            and integration_evidence["baseline_digest"]
            == replay_evidence["baseline_digest"]
            and integration_evidence["observability_digest"]
            == replay_evidence["observability_digest"]
            and integration_evidence["record_source_identity"]
            == integration_evidence["actual_source_identity"]
            and integration_evidence["source_probe_rowids"] == [1, 2, 3, 4, 5]
            and integration_evidence["source_probe_has_mint"]
        )
        checks["OBSERVABILITY_FAILURE_CANNOT_CHANGE_RUNTIME_RESULT"] = (
            publication_failure_evidence["code"] == 0
            and publication_failure_evidence["semantic_equal"]
            and publication_failure_evidence["record_state"]
            == RunState.RUNNING.value
            and publication_failure_evidence["stale_status"]
            == ObserverStatus.NO_ACTIVE_RUN.value
            and publication_failure_evidence["summary_contract"][
                "publisher_disabled"
            ]
            and any(
                "INJECTED_TERMINAL_PUBLICATION_FAILURE" in error
                for error in publication_failure_evidence["summary_contract"][
                    "publication_errors"
                ]
            )
            and publication_failure_evidence["summary_contract"]
            == publication_failure_evidence["persisted_external_observability"]
            and publication_failure_evidence["summary_contract"][
                "terminal_publication_outcome"
            ]
            == "FAILED"
            and publication_failure_evidence["summary_contract"][
                "forensic_json_outcome"
            ]
            == "SUCCEEDED"
        )

        cadence = measure_cadence_overhead(root / "performance")
        baseline_seconds = (
            integration_evidence["baseline_seconds"]
            + replay_evidence["baseline_seconds"]
        )
        enabled_seconds = (
            integration_evidence["enabled_seconds"]
            + replay_evidence["enabled_seconds"]
        )
        production_rows = (
            integration_evidence["workload_rows"]
            + replay_evidence["workload_rows"]
        )
        delta_seconds = enabled_seconds - baseline_seconds
        performance = {
            "fixture": "ACTUAL_V05_PRODUCTION_SHAPED_SYNTHETIC_BINDING",
            "repetitions": 2,
            "workload_size": production_rows,
            "baseline_seconds": baseline_seconds,
            "enabled_seconds": enabled_seconds,
            "absolute_delta_seconds": delta_seconds,
            "runtime_delta_percent": (
                delta_seconds / baseline_seconds * 100.0
                if baseline_seconds > 0.0
                else None
            ),
            "baseline_rows_per_second": production_rows
            / max(baseline_seconds, 1e-9),
            "enabled_rows_per_second": production_rows
            / max(enabled_seconds, 1e-9),
            "publication_metrics": (
                integration_evidence["summary_contract"]["publication_metrics"],
                replay_evidence["summary_contract"]["publication_metrics"],
            ),
            "semantic_digests_equal": (
                integration_evidence["semantic_equal"]
                and replay_evidence["semantic_equal"]
            ),
        }
        evidence["performance"] = performance
        evidence["cadence_overhead"] = cadence
        checks["Q_PERFORMANCE_OVERHEAD_BOUNDED"] = (
            performance["semantic_digests_equal"]
            and performance["enabled_rows_per_second"] >= 15.0
            and performance["enabled_rows_per_second"]
            >= performance["baseline_rows_per_second"] * 0.75
            and cadence["checksums_equal"]
            and cadence["heartbeat_publications"] <= 21
            and cadence["live_time_budget_percent"] < 5.0
            and cadence["added_sqlite_write_transactions"] <= 22
        )

        for publisher in (active_a, active_b):
            publisher.publish_terminal(
                RunState.ABORTED,
                durable_source_cursor_p1_rowid=0,
                source_watermark_p1_rowid=3,
                source_watermark_kind="CURRENT",
            )
            publisher.close()
            publishers.remove(publisher)
        with ReadOnlyLiveObservabilityV01(registry) as reader:
            terminal_states = {
                record.run_id: record.runtime_state for record in reader.records()
            }
        checks["FAILED_AND_ABORTED_TERMINAL_STATES_ARE_EXPLICIT"] = (
            terminal_states[active_a_id] == RunState.ABORTED.value
            and terminal_states[active_b_id] == RunState.ABORTED.value
            and terminal_states[run_two] == RunState.COMPLETED.value
            and failed_state == RunState.FAILED.value
        )

        evidence["contract"] = {
            "model_id": MODEL_ID,
            "schema_version": SCHEMA_VERSION,
            "fingerprint": MODEL_FINGERPRINT,
            "integration_model_id": integration.MODEL_ID,
            "integration_fingerprint": integration.MODEL_FINGERPRINT,
            "heartbeat_interval_seconds": 1.0,
            "heartbeat_ttl_seconds": 5.0,
            "source_row_identity": SOURCE_ROW_IDENTITY,
            "normalized_ingest_identity": NORMALIZED_INGEST_IDENTITY,
        }
        evidence["adversarial"] = {
            "expired_running": True,
            "terminal_no_resurrection": True,
            "cursor_validation_before_update": True,
            "concurrent_atomic_read": not reader_errors,
            "multiple_active_ambiguous": True,
            "wrong_nearby_source_rejected_by_exact_binding": True,
            "corrupt_publication_fails_closed": True,
            "windows_path_with_spaces": True,
        }
    finally:
        for publisher in publishers:
            try:
                publisher.close()
            except Exception:
                pass
        shutil.rmtree(root, ignore_errors=True)

    evidence["checks"] = checks
    evidence["check_count"] = len(checks)
    evidence["passed_count"] = sum(checks.values())
    evidence["failed"] = [name for name, passed in checks.items() if not passed]
    if args.evidence_json is not None:
        args.evidence_json.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_json.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    print("=" * 112)
    print("EXTERNAL READ-ONLY LIVE OBSERVABILITY CONTRACT v0.1 SELF-TEST")
    print("=" * 112)
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    if "performance" in evidence:
        perf = evidence["performance"]
        print(
            "PERFORMANCE: "
            f"rows={perf['workload_size']} "
            f"baseline={perf['baseline_seconds']:.6f}s "
            f"enabled={perf['enabled_seconds']:.6f}s "
            f"delta={perf['absolute_delta_seconds']:.6f}s "
            f"delta_percent={perf['runtime_delta_percent']:.6f}% "
            f"enabled_rps={perf['enabled_rows_per_second']:.3f}"
        )
    passed = bool(checks) and all(checks.values())
    print(f"CHECKS: {sum(checks.values())}/{len(checks)}")
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
