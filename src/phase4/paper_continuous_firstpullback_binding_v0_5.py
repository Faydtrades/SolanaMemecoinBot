from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from . import paper_continuous_firstpullback_binding_v0_1 as binding_v01
from . import paper_entry_router_v0_1 as entry_router_v01
from .paper_continuous_firstpullback_binding_v0_1 import (
    BindingBatchResultV01,
    BindingConflict,
)
from .paper_continuous_firstpullback_binding_v0_4 import (
    MODEL_FINGERPRINT as BINDING_V04_FINGERPRINT,
    ContinuousFirstPullbackBindingV04,
)
from .paper_continuous_market_source_v0_2 import (
    MODEL_FINGERPRINT as MARKET_SOURCE_FINGERPRINT,
    MODEL_ID as MARKET_SOURCE_MODEL_ID,
    ContinuousMarketSourceV02,
)
from .paper_continuous_runner_v0_2 import (
    MODEL_ID as RUNNER_MODEL_ID,
    RUNNER_SPEC_FINGERPRINT,
    ContinuousPaperRunnerV02,
)


MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0005"
SCHEMA_VERSION = "phase4_continuous_firstpullback_binding_v0.5"
SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "accepted_binding_v0_4_fingerprint": BINDING_V04_FINGERPRINT,
    "market_source_model_id": MARKET_SOURCE_MODEL_ID,
    "market_source_fingerprint": MARKET_SOURCE_FINGERPRINT,
    "continuous_runner_fingerprint": RUNNER_SPEC_FINGERPRINT,
    "source_batch_persistence": (
        "ONE_ATOMIC_SQLITE_TRANSACTION_PER_FETCHED_SOURCE_BATCH"
    ),
    "nested_commit_policy": "DEFER_UNTIL_SOURCE_BATCH_COMMIT",
    "failure_policy": (
        "ROLLBACK_WHOLE_BATCH_KEEP_PREVIOUS_DURABLE_CURSOR_REPLAY_EXACT"
    ),
    "sqlite_journal_mode": "WAL",
    "sqlite_synchronous": "FULL",
    "timer_fence_semantics": "ACCEPTED_V04_UNCHANGED",
    "paper_only": True,
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


class AtomicSourceBatchConnection(sqlite3.Connection):
    """SQLite connection that defers nested commits inside one source batch.

    Existing Phase-4 components keep their accepted local transaction scopes.
    During a source batch those scopes become save-until-batch boundaries.  A
    successful batch performs one FULL-synchronous WAL commit; any exception
    rolls every semantic write and the durable cursor back together.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._source_batch_active = False
        self._source_batch_rollback_only = False
        self._suppressed_commit_boundaries = 0
        self._source_batch_commits = 0
        self._source_batch_rollbacks = 0

    def __enter__(self):
        if self._source_batch_active:
            return self
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        if self._source_batch_active:
            self._suppressed_commit_boundaries += 1
            if exc_type is not None:
                self._source_batch_rollback_only = True
            return False
        return super().__exit__(exc_type, exc_value, traceback)

    def commit(self) -> None:
        if self._source_batch_active:
            self._suppressed_commit_boundaries += 1
            return
        sqlite3.Connection.commit(self)

    def rollback(self) -> None:
        if self._source_batch_active:
            self._source_batch_rollback_only = True
        sqlite3.Connection.rollback(self)

    def _physical_batch_commit(self) -> None:
        sqlite3.Connection.commit(self)

    @contextmanager
    def source_batch_transaction(self) -> Iterator[None]:
        if self._source_batch_active:
            raise BindingConflict("nested atomic source batch is not allowed")
        if self.in_transaction:
            raise BindingConflict(
                "atomic source batch requires a clean SQLite transaction boundary"
            )
        sqlite3.Connection.execute(self, "BEGIN")
        self._source_batch_active = True
        self._source_batch_rollback_only = False
        try:
            yield
            if self._source_batch_rollback_only:
                raise BindingConflict("atomic source batch became rollback-only")
            self._source_batch_active = False
            self._physical_batch_commit()
            self._source_batch_commits += 1
        except BaseException:
            self._source_batch_active = False
            sqlite3.Connection.rollback(self)
            self._source_batch_rollbacks += 1
            raise
        finally:
            self._source_batch_active = False
            self._source_batch_rollback_only = False

    def source_batch_metrics(self) -> dict[str, int]:
        return {
            "source_batch_commits": self._source_batch_commits,
            "source_batch_rollbacks": self._source_batch_rollbacks,
            "suppressed_commit_boundaries": self._suppressed_commit_boundaries,
        }


def connect_atomic_entry_router_db(
    path: Path,
    *,
    connection_factory: type[AtomicSourceBatchConnection] = (
        AtomicSourceBatchConnection
    ),
) -> AtomicSourceBatchConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, factory=connection_factory)
    if not isinstance(conn, AtomicSourceBatchConnection):
        conn.close()
        raise TypeError("atomic paper connection factory returned an invalid type")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    entry_router_v01._create_schema(conn)
    return conn


class ContinuousFirstPullbackBindingV05(ContinuousFirstPullbackBindingV04):
    """Accepted V04 semantics with atomic source-batch persistence."""

    model_id = MODEL_ID
    model_fingerprint = MODEL_FINGERPRINT
    schema_version = SCHEMA_VERSION

    def __init__(
        self,
        conn: AtomicSourceBatchConnection,
        market_source: ContinuousMarketSourceV02,
        *,
        wall_clock: Callable[[], datetime] | None = None,
        failure_injector=None,
        runner_failure_injector=None,
        fence_failure_injector: Callable[[str], None] | None = None,
    ) -> None:
        if not isinstance(conn, AtomicSourceBatchConnection):
            raise BindingConflict(
                "binding v0.5 requires an atomic source-batch connection"
            )
        if not isinstance(market_source, ContinuousMarketSourceV02):
            raise BindingConflict("binding v0.5 requires market source v0.2")
        if market_source.model_fingerprint != MARKET_SOURCE_FINGERPRINT:
            raise BindingConflict("market source v0.2 fingerprint drift")
        if market_source.start_after_p1_rowid < 0:
            raise ValueError("market-source anchor must be non-negative")
        self._fence_drain_active = False
        self._fence_failure_injector = fence_failure_injector
        self._performance = {
            "normal_source_fetch_count": 0,
            "normal_source_fetch_seconds": 0.0,
            "fence_source_fetch_count": 0,
            "fence_source_fetch_seconds": 0.0,
            "fence_drain_count": 0,
            "fence_drain_batches": 0,
            "fence_drain_raw_rows": 0,
            "fence_drain_normalized_rows": 0,
            "fence_drain_seconds": 0.0,
            "passive_fence_wait_seconds": 0.0,
            "source_unavailable_fence_wait_seconds": 0.0,
            "hydration_seconds": 0.0,
            "strategy_evaluation_seconds": 0.0,
            "semantic_outbox_seconds": 0.0,
            "cursor_commit_seconds": 0.0,
            "timer_prepare_seconds": 0.0,
            "timer_execution_seconds": 0.0,
            "sqlite_transaction_seconds": 0.0,
            "maximum_fence_cursor_distance_rowids": 0,
            "maximum_fence_drain_seconds": 0.0,
            "evaluation_delivery_transactions": 0,
            "source_batch_transaction_count": 0,
            "source_batch_transaction_seconds": 0.0,
            "source_batch_transaction_raw_rows": 0,
            "source_batch_transaction_normalized_rows": 0,
        }
        self._source_batch_terminal_failure: str | None = None
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        journal_mode = str(self.conn.execute("PRAGMA journal_mode").fetchone()[0])
        synchronous = int(self.conn.execute("PRAGMA synchronous").fetchone()[0])
        if journal_mode.lower() != "wal" or synchronous != 2:
            raise BindingConflict(
                "binding v0.5 requires WAL with synchronous=FULL"
            )
        self.market_source = market_source
        self.wall_clock = wall_clock or (lambda: datetime.now(timezone.utc))
        self.failure_injector = failure_injector
        self.params_by_role = binding_v01.locked_parameter_sets()
        self.reconstruction_queries = 0
        binding_v01._create_schema(self.conn)
        self._binding_source_identity = binding_v01._fingerprint({
            "binding_model_id": MODEL_ID,
            "binding_fingerprint": MODEL_FINGERPRINT,
            "market_source_identity": market_source.source_identity,
            "start_after_p1_rowid": market_source.start_after_p1_rowid,
        })
        self._bind_runtime_v05()
        self._verify_prepared_timers()
        self.conn.commit()
        self.runner = ContinuousPaperRunnerV02(
            conn,
            source_identity=self._binding_source_identity,
            wall_clock=self.wall_clock,
            failure_injector=runner_failure_injector,
        )
        self.conn.commit()
        self._engine = self._rebuild_engine()

    def process_next_batch(
        self, *, batch_size: int = 1_000
    ) -> BindingBatchResultV01:
        if self._source_batch_terminal_failure is not None:
            raise BindingConflict(
                "binding v0.5 is terminal after a rolled-back source batch: "
                + self._source_batch_terminal_failure
            )
        started = time.perf_counter()
        try:
            with self.conn.source_batch_transaction():
                result = super().process_next_batch(batch_size=batch_size)
        except BaseException as exc:
            self._source_batch_terminal_failure = f"{type(exc).__name__}:{exc}"
            try:
                self._engine = self._rebuild_engine()
            except BaseException as rebuild_exc:
                self._source_batch_terminal_failure += (
                    f";ENGINE_REBUILD_FAILED:{type(rebuild_exc).__name__}:{rebuild_exc}"
                )
            raise
        elapsed = time.perf_counter() - started
        self._performance["source_batch_transaction_count"] += 1
        self._performance["source_batch_transaction_seconds"] += elapsed
        self._performance["source_batch_transaction_raw_rows"] += (
            result.raw_rows_fetched
        )
        self._performance["source_batch_transaction_normalized_rows"] += (
            result.normalized_records
        )
        return result

    def performance_metrics(self) -> dict[str, int | float]:
        metrics = super().performance_metrics()
        metrics.update(self.conn.source_batch_metrics())
        return metrics

    def canonical_digest(self) -> str:
        tables = (
            "paper_fp_binding_runtime_v0_1",
            "paper_fp_binding_inputs_v0_1",
            "paper_fp_binding_feature_events_v0_1",
            "paper_fp_binding_strategy_runs_v0_1",
            "paper_fp_binding_evaluation_audit_v0_1",
            "paper_fp_binding_outbox_v0_1",
            "paper_fp_binding_prepared_timers_v0_1",
        )
        payload: dict[str, Any] = {
            "model_id": MODEL_ID,
            "fingerprint": MODEL_FINGERPRINT,
            "runner_digest": self.runner.canonical_digest(),
        }
        for table in tables:
            payload[table] = [
                dict(row)
                for row in self.conn.execute(
                    f"SELECT * FROM {table} ORDER BY rowid"
                ).fetchall()
            ]
        return binding_v01._fingerprint(payload)

    def _bind_runtime_v05(self) -> None:
        row = self.conn.execute(
            "SELECT * FROM paper_fp_binding_runtime_v0_1 WHERE singleton=1"
        ).fetchone()
        expected = (
            MODEL_ID,
            MODEL_FINGERPRINT,
            self.market_source.source_identity,
            self.market_source.start_after_p1_rowid,
            RUNNER_MODEL_ID,
            RUNNER_SPEC_FINGERPRINT,
            self._binding_source_identity,
        )
        if row is None:
            now = binding_v01._dt_text(binding_v01._utc(self.wall_clock()))
            with self.conn:
                self.conn.execute(
                    "INSERT INTO paper_fp_binding_runtime_v0_1("
                    "singleton,binding_model_id,binding_fingerprint,"
                    "market_source_identity,start_after_p1_rowid,"
                    "last_durable_p1_rowid,runner_model_id,runner_fingerprint,"
                    "runner_source_identity,next_event_sequence,created_at,updated_at) "
                    "VALUES(1,?,?,?,?,?,?,?,?,0,?,?)",
                    (
                        MODEL_ID,
                        MODEL_FINGERPRINT,
                        self.market_source.source_identity,
                        self.market_source.start_after_p1_rowid,
                        self.market_source.start_after_p1_rowid,
                        RUNNER_MODEL_ID,
                        RUNNER_SPEC_FINGERPRINT,
                        self._binding_source_identity,
                        now,
                        now,
                    ),
                )
            return
        actual = (
            str(row["binding_model_id"]),
            str(row["binding_fingerprint"]),
            str(row["market_source_identity"]),
            int(row["start_after_p1_rowid"]),
            str(row["runner_model_id"]),
            str(row["runner_fingerprint"]),
            str(row["runner_source_identity"]),
        )
        if actual != expected:
            raise BindingConflict(
                "persisted v0.5 binding identity conflict: "
                f"actual={actual!r} expected={expected!r}"
            )
