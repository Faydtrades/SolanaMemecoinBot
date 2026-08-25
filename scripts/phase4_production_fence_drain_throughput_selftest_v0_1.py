from __future__ import annotations

import json
import hashlib
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_timer_fence_throughput_selftest_v0_1 as fixtures  # noqa: E402
from phase4 import paper_entry_router_v0_1 as entry_router_v01  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_4 import (  # noqa: E402
    MODEL_FINGERPRINT as V04_FINGERPRINT,
    ContinuousFirstPullbackBindingV04,
)
from phase4.paper_continuous_firstpullback_binding_v0_5 import (  # noqa: E402
    MODEL_FINGERPRINT,
    MODEL_ID,
    AtomicSourceBatchConnection,
    ContinuousFirstPullbackBindingV05,
    connect_atomic_entry_router_db,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    ContinuousMarketSourceV02,
)


BASE = fixtures.BASE
EXPECTED_V04_FINGERPRINT = (
    "e2c35ce63fe419ac4a934913dd26249d72411884cb115e0701f601b276b0f7d6"
)
EXPECTED_V05_FINGERPRINT = (
    "f66acb5926f3dd77d68c086d74e1bc07b43fad4307071c1ec8c3b7d1111660b7"
)
THROUGHPUT_MINIMUM_RPS = 15.0
PRODUCTION_SHAPED_ROWS = 250
COMMIT_LATENCY_SECONDS = 0.025


class SlowFullSyncConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.delay_enabled = False
        self.physical_commit_boundaries = 0

    def _observe_commit(self, had_transaction: bool) -> None:
        if self.delay_enabled and had_transaction:
            self.physical_commit_boundaries += 1
            time.sleep(COMMIT_LATENCY_SECONDS)

    def commit(self) -> None:
        had_transaction = self.in_transaction
        sqlite3.Connection.commit(self)
        self._observe_commit(had_transaction)

    def __exit__(self, exc_type, exc_value, traceback):
        had_transaction = self.in_transaction
        result = sqlite3.Connection.__exit__(
            self, exc_type, exc_value, traceback
        )
        self._observe_commit(had_transaction and exc_type is None)
        return result


class SlowAtomicSourceBatchConnection(AtomicSourceBatchConnection):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.delay_enabled = False
        self.physical_commit_boundaries = 0

    def _observe_commit(self, had_transaction: bool) -> None:
        if self.delay_enabled and had_transaction:
            self.physical_commit_boundaries += 1
            time.sleep(COMMIT_LATENCY_SECONDS)

    def commit(self) -> None:
        if self._source_batch_active:
            super().commit()
            return
        had_transaction = self.in_transaction
        super().commit()
        self._observe_commit(had_transaction)

    def __exit__(self, exc_type, exc_value, traceback):
        if self._source_batch_active:
            return super().__exit__(exc_type, exc_value, traceback)
        had_transaction = self.in_transaction
        result = super().__exit__(exc_type, exc_value, traceback)
        self._observe_commit(had_transaction and exc_type is None)
        return result

    def _physical_batch_commit(self) -> None:
        had_transaction = self.in_transaction
        sqlite3.Connection.commit(self)
        self._observe_commit(had_transaction)


class FailBeforePhysicalBatchCommitConnection(AtomicSourceBatchConnection):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.fail_next_batch_commit = False

    def _physical_batch_commit(self) -> None:
        if self.fail_next_batch_commit:
            self.fail_next_batch_commit = False
            raise sqlite3.OperationalError("INJECTED_BATCH_COMMIT_FAILURE")
        super()._physical_batch_commit()


class FailOnce:
    def __init__(self, input_key: str, phase: str) -> None:
        self.input_key = input_key
        self.phase = phase
        self.triggered = False

    def __call__(self, input_key: str, phase: str) -> None:
        if (
            not self.triggered
            and input_key == self.input_key
            and phase == self.phase
        ):
            self.triggered = True
            raise RuntimeError(f"INJECTED:{input_key}:{phase}")


def connect_slow_v04(path: Path) -> SlowFullSyncConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, factory=SlowFullSyncConnection)
    assert isinstance(conn, SlowFullSyncConnection)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    entry_router_v01._create_schema(conn)
    return conn


def source(path: Path, identity: str) -> ContinuousMarketSourceV02:
    return ContinuousMarketSourceV02(
        path,
        start_after_p1_rowid=0,
        database_identity=identity,
    )


def stable_rows(conn: sqlite3.Connection, table: str, order: str) -> tuple:
    excluded = {
        "created_at",
        "updated_at",
        "binding_model_id",
        "binding_fingerprint",
        "runner_source_identity",
    }
    rows = []
    for row in conn.execute(f"SELECT * FROM {table} ORDER BY {order}"):
        rows.append(tuple((key, row[key]) for key in row.keys() if key not in excluded))
    return tuple(rows)


def semantic_snapshot(conn: sqlite3.Connection) -> dict[str, tuple]:
    return {
        "inputs": stable_rows(conn, "paper_fp_binding_inputs_v0_1", "input_key"),
        "features": stable_rows(
            conn,
            "paper_fp_binding_feature_events_v0_1",
            "production_p1_rowid",
        ),
        "runs": stable_rows(
            conn, "paper_fp_binding_strategy_runs_v0_1", "mint,role"
        ),
        "audits": fixtures.stable_audit_rows(conn),
        "evaluations": fixtures.stable_evaluation_rows(conn),
        "outbox": stable_rows(
            conn, "paper_fp_binding_outbox_v0_1", "event_sequence"
        ),
    }


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}
    root = Path(tempfile.mkdtemp(prefix="phase4_t016_throughput_"))
    try:
        check(
            "MODEL_IDENTITIES",
            MODEL_ID == "P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0005"
            and MODEL_FINGERPRINT == EXPECTED_V05_FINGERPRINT
            and V04_FINGERPRINT == EXPECTED_V04_FINGERPRINT,
            checks,
        )

        source_path = root / "production_shaped_source.sqlite3"
        fixtures.create_source(source_path)
        fixtures.append_rows(source_path, 1, PRODUCTION_SHAPED_ROWS)
        identity = "SELFTEST:T016:PRODUCTION-SHAPED"
        v04_source = source(source_path, identity)
        v05_source = source(source_path, identity)

        v04_conn = connect_slow_v04(root / "v04.sqlite3")
        v04 = ContinuousFirstPullbackBindingV04(
            v04_conn, v04_source, wall_clock=lambda: BASE
        )
        v04_timer = v04.prepare_clock_tick(BASE, timer_id="T016-PROFILE")

        v05_conn = connect_atomic_entry_router_db(
            root / "v05.sqlite3",
            connection_factory=SlowAtomicSourceBatchConnection,
        )
        assert isinstance(v05_conn, SlowAtomicSourceBatchConnection)
        v05 = ContinuousFirstPullbackBindingV05(
            v05_conn, v05_source, wall_clock=lambda: BASE
        )
        v05_timer = v05.prepare_clock_tick(BASE, timer_id="T016-PROFILE")

        fixtures.append_rows(
            source_path,
            PRODUCTION_SHAPED_ROWS + 1,
            PRODUCTION_SHAPED_ROWS + 10,
        )

        v04_conn.delay_enabled = True
        before_started = time.perf_counter()
        before = v04.drain_and_execute_prepared_timer(
            v04_timer, batch_size=PRODUCTION_SHAPED_ROWS
        )
        before_elapsed = time.perf_counter() - before_started
        before_rps = before.fence_drain_raw_rows / before_elapsed
        before_commits = v04_conn.physical_commit_boundaries
        before_snapshot = semantic_snapshot(v04_conn)

        v05_conn.delay_enabled = True
        after_started = time.perf_counter()
        after = v05.drain_and_execute_prepared_timer(
            v05_timer, batch_size=PRODUCTION_SHAPED_ROWS
        )
        after_elapsed = time.perf_counter() - after_started
        after_rps = after.fence_drain_raw_rows / after_elapsed
        after_commits = v05_conn.physical_commit_boundaries
        after_snapshot = semantic_snapshot(v05_conn)
        after_metrics = v05.performance_metrics()

        evidence["production_shaped_before_after"] = {
            "workload_raw_rows": PRODUCTION_SHAPED_ROWS,
            "commit_latency_seconds": COMMIT_LATENCY_SECONDS,
            "v04": {
                "elapsed_seconds": before_elapsed,
                "raw_rows_per_second": before_rps,
                "physical_commit_boundaries": before_commits,
                "durable_cursor": before.final_durable_p1_rowid,
            },
            "v05": {
                "elapsed_seconds": after_elapsed,
                "raw_rows_per_second": after_rps,
                "physical_commit_boundaries": after_commits,
                "durable_cursor": after.final_durable_p1_rowid,
                "performance": after_metrics,
            },
        }
        check(
            "PRODUCTION_SHAPED_PATH_REPRODUCES_COMMIT_AMPLIFICATION",
            before.fence_drain_raw_rows == PRODUCTION_SHAPED_ROWS
            and before_commits > PRODUCTION_SHAPED_ROWS * 2
            and before_rps < THROUGHPUT_MINIMUM_RPS,
            checks,
        )
        check(
            "ATOMIC_BATCH_MINIMAL_COMMIT_SHAPE",
            after_metrics["source_batch_commits"] == 1
            and after_metrics["source_batch_rollbacks"] == 0
            and after_metrics["suppressed_commit_boundaries"] > 0
            and after_commits < before_commits // 10,
            checks,
        )
        check(
            "LOCKED_15_RAW_ROWS_PER_SECOND",
            after_rps >= THROUGHPUT_MINIMUM_RPS,
            checks,
        )
        check(
            "SEMANTIC_ROWS_EQUIVALENT_TO_ACCEPTED_V04",
            before_snapshot == after_snapshot,
            checks,
        )
        check(
            "FROZEN_WATERMARK_AND_CURSOR_EXACT",
            after.requested_watermark_p1_rowid == PRODUCTION_SHAPED_ROWS
            and after.final_durable_p1_rowid == PRODUCTION_SHAPED_ROWS
            and v05.durable_p1_rowid == PRODUCTION_SHAPED_ROWS,
            checks,
        )
        admitted = tuple(
            int(row[0])
            for row in v05_conn.execute(
                "SELECT production_p1_rowid FROM paper_fp_binding_inputs_v0_1 "
                "WHERE production_p1_rowid IS NOT NULL ORDER BY production_p1_rowid"
            )
        )
        check(
            "NO_LOSS_DUPLICATION_REORDER_OR_W_PLUS_1",
            admitted == tuple(range(1, PRODUCTION_SHAPED_ROWS + 1))
            and v05_conn.execute(
                "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1 "
                "WHERE production_p1_rowid>?",
                (PRODUCTION_SHAPED_ROWS,),
            ).fetchone()[0]
            == 0,
            checks,
        )
        resumed = v05.process_next_batch(batch_size=10)
        check(
            "W_PLUS_1_RELEASED_ONLY_AFTER_TIMER",
            resumed.raw_rows_fetched == 10
            and resumed.durable_p1_rowid == PRODUCTION_SHAPED_ROWS + 10,
            checks,
        )
        digest = v05.canonical_digest()
        check(
            "SQLITE_QUICK_CHECK",
            v05_conn.execute("PRAGMA quick_check").fetchone()[0] == "ok",
            checks,
        )
        v04_conn.close()
        v05_conn.close()

        reopen_sha_before = sha256_file(root / "v05.sqlite3")
        reopened_conn = connect_atomic_entry_router_db(root / "v05.sqlite3")
        reopened = ContinuousFirstPullbackBindingV05(
            reopened_conn,
            source(source_path, identity),
            wall_clock=lambda: BASE,
        )
        check(
            "REOPEN_DIGEST_EXACT",
            reopened.durable_p1_rowid == PRODUCTION_SHAPED_ROWS + 10
            and reopened.canonical_digest() == digest,
            checks,
        )
        reopened_conn.close()
        reopen_sha_after = sha256_file(root / "v05.sqlite3")
        check(
            "EXACT_STATE_REOPEN_LEAVES_DATABASE_BYTES",
            reopen_sha_before == reopen_sha_after,
            checks,
        )

        crash_source_path = root / "crash_source.sqlite3"
        fixtures.create_source(crash_source_path)
        fixtures.append_rows(crash_source_path, 1, 40)
        crash_source = source(crash_source_path, "SELFTEST:T016:CRASH")
        crash_conn = connect_atomic_entry_router_db(root / "crash.sqlite3")
        fail = FailOnce(
            "P1:1", "after_runner_items_before_production_cursor"
        )
        crash_binding = ContinuousFirstPullbackBindingV05(
            crash_conn,
            crash_source,
            wall_clock=lambda: BASE,
            failure_injector=fail,
        )
        crash_timer = crash_binding.prepare_clock_tick(
            BASE, timer_id="T016-CRASH"
        )
        injected = False
        try:
            crash_binding.drain_and_execute_prepared_timer(
                crash_timer, batch_size=40
            )
        except RuntimeError as exc:
            injected = str(exc).startswith("INJECTED:")
        terminal_reuse_rejected = False
        try:
            crash_binding.process_next_batch(batch_size=40)
        except Exception as exc:
            terminal_reuse_rejected = "terminal after a rolled-back" in str(exc)
        crash_metrics = crash_binding.performance_metrics()
        check(
            "BATCH_FAILURE_ROLLS_BACK_CURSOR_AND_ALL_SEMANTICS",
            injected
            and crash_binding.durable_p1_rowid == 0
            and crash_conn.execute(
                "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1"
            ).fetchone()[0]
            == 0
            and crash_metrics["source_batch_rollbacks"] == 1,
            checks,
        )
        check(
            "ROLLED_BACK_INSTANCE_FAILS_CLOSED",
            terminal_reuse_rejected,
            checks,
        )
        crash_conn.close()

        fixtures.append_rows(crash_source_path, 41, 41)
        recovery_conn = connect_atomic_entry_router_db(root / "crash.sqlite3")
        recovery = ContinuousFirstPullbackBindingV05(
            recovery_conn,
            source(crash_source_path, "SELFTEST:T016:CRASH"),
            wall_clock=lambda: BASE,
        )
        recovered = recovery.drain_and_execute_prepared_timer(
            crash_timer, batch_size=40
        )
        row_41_before_release = recovery_conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1 "
            "WHERE production_p1_rowid=41"
        ).fetchone()[0]
        recovery_digest = recovery.canonical_digest()
        duplicates = recovery_conn.execute(
            "SELECT COUNT(*) FROM (SELECT production_p1_rowid FROM "
            "paper_fp_binding_inputs_v0_1 WHERE production_p1_rowid IS NOT NULL "
            "GROUP BY production_p1_rowid HAVING COUNT(*)>1)"
        ).fetchone()[0]
        check(
            "ROLLBACK_RESTART_REPLAYS_TO_EXACT_FENCE",
            recovered.final_durable_p1_rowid == 40
            and row_41_before_release == 0
            and duplicates == 0
            and recovery.pending_outbox_count() == 0,
            checks,
        )
        recovery_conn.close()

        replay_conn = connect_atomic_entry_router_db(root / "crash.sqlite3")
        replay = ContinuousFirstPullbackBindingV05(
            replay_conn,
            source(crash_source_path, "SELFTEST:T016:CRASH"),
            wall_clock=lambda: BASE,
        )
        replay_result = replay.drain_and_execute_prepared_timer(
            crash_timer, batch_size=40
        )
        check(
            "TIMER_REPLAY_IDEMPOTENT_AND_DETERMINISTIC",
            replay_result.executed_timer_keys == ()
            and replay.canonical_digest() == recovery_digest
            and replay_conn.execute("PRAGMA quick_check").fetchone()[0] == "ok",
            checks,
        )
        replay_conn.close()

        commit_source_path = root / "commit_failure_source.sqlite3"
        fixtures.create_source(commit_source_path)
        fixtures.append_rows(commit_source_path, 1, 40)
        commit_conn = connect_atomic_entry_router_db(
            root / "commit_failure.sqlite3",
            connection_factory=FailBeforePhysicalBatchCommitConnection,
        )
        assert isinstance(
            commit_conn, FailBeforePhysicalBatchCommitConnection
        )
        commit_binding = ContinuousFirstPullbackBindingV05(
            commit_conn,
            source(commit_source_path, "SELFTEST:T016:COMMIT-FAILURE"),
            wall_clock=lambda: BASE,
        )
        commit_conn.fail_next_batch_commit = True
        commit_failed = False
        try:
            commit_binding.process_next_batch(batch_size=40)
        except sqlite3.OperationalError as exc:
            commit_failed = str(exc) == "INJECTED_BATCH_COMMIT_FAILURE"
        check(
            "PHYSICAL_COMMIT_FAILURE_CANNOT_ADVANCE_DURABLE_CURSOR",
            commit_failed
            and commit_binding.durable_p1_rowid == 0
            and commit_conn.execute(
                "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1"
            ).fetchone()[0]
            == 0
            and commit_binding.performance_metrics()[
                "source_batch_rollbacks"
            ]
            == 1,
            checks,
        )
        commit_conn.close()

        commit_recovery_conn = connect_atomic_entry_router_db(
            root / "commit_failure.sqlite3"
        )
        commit_recovery = ContinuousFirstPullbackBindingV05(
            commit_recovery_conn,
            source(commit_source_path, "SELFTEST:T016:COMMIT-FAILURE"),
            wall_clock=lambda: BASE,
        )
        commit_recovered = commit_recovery.process_next_batch(batch_size=40)
        check(
            "PHYSICAL_COMMIT_FAILURE_RESTART_REPLAYS_EXACTLY",
            commit_recovered.durable_p1_rowid == 40
            and commit_recovery_conn.execute(
                "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1"
            ).fetchone()[0]
            == 40
            and commit_recovery_conn.execute("PRAGMA quick_check").fetchone()[0]
            == "ok",
            checks,
        )
        commit_recovery_conn.close()

        partial_source_path = root / "partial_failure_source.sqlite3"
        fixtures.create_source(partial_source_path)
        fixtures.append_rows(partial_source_path, 1, 80)
        partial_conn = connect_atomic_entry_router_db(
            root / "partial_failure.sqlite3"
        )
        partial_fail = FailOnce(
            "P1:41", "after_runner_items_before_production_cursor"
        )
        partial_binding = ContinuousFirstPullbackBindingV05(
            partial_conn,
            source(partial_source_path, "SELFTEST:T016:PARTIAL-FAILURE"),
            wall_clock=lambda: BASE,
            failure_injector=partial_fail,
        )
        partial_timer = partial_binding.prepare_clock_tick(
            BASE, timer_id="T016-PARTIAL-FAILURE"
        )
        partial_injected = False
        try:
            partial_binding.drain_and_execute_prepared_timer(
                partial_timer, batch_size=40
            )
        except RuntimeError as exc:
            partial_injected = str(exc).startswith("INJECTED:")
        check(
            "LATER_BATCH_FAILURE_PRESERVES_PRIOR_DURABLE_BATCH_ONLY",
            partial_injected
            and partial_binding.durable_p1_rowid == 40
            and partial_conn.execute(
                "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1"
            ).fetchone()[0]
            == 40
            and partial_conn.execute(
                "SELECT status FROM paper_fp_binding_prepared_timers_v0_1 "
                "WHERE timer_key=?",
                (partial_timer,),
            ).fetchone()[0]
            == "PREPARED",
            checks,
        )
        partial_conn.close()

        partial_recovery_conn = connect_atomic_entry_router_db(
            root / "partial_failure.sqlite3"
        )
        partial_recovery = ContinuousFirstPullbackBindingV05(
            partial_recovery_conn,
            source(partial_source_path, "SELFTEST:T016:PARTIAL-FAILURE"),
            wall_clock=lambda: BASE,
        )
        partial_recovered = partial_recovery.drain_and_execute_prepared_timer(
            partial_timer, batch_size=40
        )
        check(
            "LATER_BATCH_FAILURE_RESTART_CONVERGES_WITHOUT_DUPLICATES",
            partial_recovered.final_durable_p1_rowid == 80
            and partial_recovery_conn.execute(
                "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1"
            ).fetchone()[0]
            == 81
            and partial_recovery_conn.execute(
                "SELECT COUNT(*) FROM (SELECT production_p1_rowid FROM "
                "paper_fp_binding_inputs_v0_1 WHERE production_p1_rowid IS NOT NULL "
                "GROUP BY production_p1_rowid HAVING COUNT(*)>1)"
            ).fetchone()[0]
            == 0
            and partial_recovery_conn.execute("PRAGMA quick_check").fetchone()[0]
            == "ok",
            checks,
        )
        partial_recovery_conn.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)

    print("=" * 108)
    print("PRODUCTION FENCE-DRAIN THROUGHPUT SELF-TEST V0.1")
    print("=" * 108)
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    print("BEFORE_AFTER_EVIDENCE=" + json.dumps(evidence, sort_keys=True))
    passed = bool(checks) and all(checks.values())
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
