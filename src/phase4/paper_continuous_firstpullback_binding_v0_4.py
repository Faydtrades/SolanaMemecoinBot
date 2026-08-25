from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from . import paper_continuous_firstpullback_binding_v0_1 as binding_v01
from . import paper_runtime_observability_v0_1 as observability_v01
from .paper_continuous_firstpullback_binding_v0_1 import (
    BindingBatchResultV01,
    BindingConflict,
    BindingTimerResultV01,
)
from .paper_continuous_firstpullback_binding_v0_3 import (
    MODEL_FINGERPRINT as BINDING_V03_FINGERPRINT,
    ContinuousFirstPullbackBindingV03,
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


MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0004"
SCHEMA_VERSION = "phase4_continuous_firstpullback_binding_v0.4"
SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "accepted_binding_v0_3_fingerprint": BINDING_V03_FINGERPRINT,
    "market_source_model_id": MARKET_SOURCE_MODEL_ID,
    "market_source_fingerprint": MARKET_SOURCE_FINGERPRINT,
    "continuous_runner_fingerprint": RUNNER_SPEC_FINGERPRINT,
    "timer_fence_drain": "ACTIVE_FIXED_WATERMARK_NO_READY_BACKLOG_SLEEP",
    "same_watermark_timers": "COMPLETE_IN_TIMER_KEY_ORDER_BEFORE_W_PLUS_1",
    "later_watermark_timers": "CANNOT_OVERTAKE_MINIMUM_ACTIVE_WATERMARK",
    "source_unavailable_wait": "EXPLICIT_AND_SEPARATE_FROM_ACTIVE_DRAIN",
    "health_probe": "BEFORE_AND_AFTER_EACH_FENCE_DRAIN_BATCH",
    "evaluation_delivery": (
        "SAME_IDENTITIES_AND_CONTENT_ONE_SQLITE_TRANSACTION_PER_SOURCE_INPUT"
    ),
    "paper_only": True,
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


@dataclass(frozen=True, slots=True)
class ActiveFenceDrainResultV04:
    requested_timer_key: str
    requested_watermark_p1_rowid: int
    executed_timer_keys: tuple[str, ...]
    fence_drain_batches: int
    fence_drain_raw_rows: int
    fence_drain_normalized_rows: int
    final_durable_p1_rowid: int


class ContinuousFirstPullbackBindingV04(ContinuousFirstPullbackBindingV03):
    """V03 semantics with observable fixed-watermark active draining."""

    model_id = MODEL_ID
    model_fingerprint = MODEL_FINGERPRINT
    schema_version = SCHEMA_VERSION

    def __init__(
        self,
        conn: sqlite3.Connection,
        market_source: ContinuousMarketSourceV02,
        *,
        wall_clock: Callable[[], datetime] | None = None,
        failure_injector=None,
        runner_failure_injector=None,
        fence_failure_injector: Callable[[str], None] | None = None,
    ) -> None:
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
        }
        if not isinstance(market_source, ContinuousMarketSourceV02):
            raise BindingConflict("binding v0.4 requires market source v0.2")
        if market_source.model_fingerprint != MARKET_SOURCE_FINGERPRINT:
            raise BindingConflict("market source v0.2 fingerprint drift")
        if market_source.start_after_p1_rowid < 0:
            raise ValueError("market-source anchor must be non-negative")
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
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
        self._bind_runtime_v03()
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

    def performance_metrics(self) -> dict[str, int | float]:
        return dict(self._performance)

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

    def process_next_batch(
        self, *, batch_size: int = 1_000
    ) -> BindingBatchResultV01:
        after = self.durable_p1_rowid
        fence = self.active_timer_fence_p1_rowid
        if fence is not None and after == fence:
            return BindingBatchResultV01(
                requested_after_p1_rowid=after,
                highest_fetched_p1_rowid=after,
                raw_rows_fetched=0,
                normalized_records=0,
                deterministic_skips=0,
                runner_items_committed=0,
                durable_p1_rowid=after,
            )
        effective_batch_size = int(batch_size)
        if fence is not None:
            effective_batch_size = min(effective_batch_size, fence - after)
        fetch_started = time.perf_counter()
        batch = self.market_source.fetch_batch(
            after_p1_rowid=after,
            batch_size=effective_batch_size,
        )
        fetch_seconds = time.perf_counter() - fetch_started
        prefix = "fence" if self._fence_drain_active else "normal"
        self._performance[f"{prefix}_source_fetch_count"] += 1
        self._performance[f"{prefix}_source_fetch_seconds"] += fetch_seconds
        self._fence_inject("after_source_fetch")
        self._fence_inject("after_source_normalization")

        records = {item.production_p1_rowid: item for item in batch.records}
        skips = {item.production_p1_rowid: item for item in batch.skips}
        fetched_rowids = tuple(sorted(set(records) | set(skips)))
        if len(fetched_rowids) != batch.raw_rows_fetched:
            raise BindingConflict("market source fetched-row accounting is incomplete")
        admitted_rowids = tuple(
            rowid for rowid in fetched_rowids if fence is None or rowid <= fence
        )
        committed = 0
        for rowid in admitted_rowids:
            if rowid <= after:
                raise BindingConflict("market source returned a non-ascending rowid")
            if rowid in records:
                committed += self._handle_record(records[rowid])
            else:
                self._handle_skip(rowid, binding_v01._fingerprint(skips[rowid]))
        return BindingBatchResultV01(
            requested_after_p1_rowid=after,
            highest_fetched_p1_rowid=(
                after if not admitted_rowids else admitted_rowids[-1]
            ),
            raw_rows_fetched=len(admitted_rowids),
            normalized_records=sum(rowid in records for rowid in admitted_rowids),
            deterministic_skips=sum(rowid in skips for rowid in admitted_rowids),
            runner_items_committed=committed,
            durable_p1_rowid=self.durable_p1_rowid,
        )

    def prepare_clock_tick(self, *args, **kwargs) -> str:
        started = time.perf_counter()
        try:
            return super().prepare_clock_tick(*args, **kwargs)
        finally:
            self._performance["timer_prepare_seconds"] += (
                time.perf_counter() - started
            )

    def execute_prepared_timer(self, timer_key: str) -> BindingTimerResultV01:
        started = time.perf_counter()
        try:
            return super().execute_prepared_timer(timer_key)
        finally:
            self._performance["timer_execution_seconds"] += (
                time.perf_counter() - started
            )

    def drain_and_execute_prepared_timer(
        self,
        timer_key: str,
        *,
        batch_size: int,
        progress_probe: Callable[[int], None] | None = None,
        source_unavailable_wait: Callable[[], None] | None = None,
        batch_observer: Callable[[BindingBatchResultV01, float], None] | None = None,
    ) -> ActiveFenceDrainResultV04:
        requested = self._prepared_timer(timer_key)
        requested_watermark = int(requested["source_watermark_p1_rowid"])
        executed: list[str] = []
        batch_count = 0
        raw_rows = 0
        normalized_rows = 0
        drain_started = time.perf_counter()
        initial_cursor = self.durable_p1_rowid
        self._performance["fence_drain_count"] += 1
        self._performance["maximum_fence_cursor_distance_rowids"] = max(
            self._performance["maximum_fence_cursor_distance_rowids"],
            max(0, requested_watermark - initial_cursor),
        )
        self._fence_drain_active = True
        try:
            while True:
                requested_status = str(self._prepared_timer(timer_key)["status"])
                if requested_status == "COMPLETE":
                    break
                active_watermark = self.active_timer_fence_p1_rowid
                if active_watermark is None:
                    raise BindingConflict(
                        "requested timer is incomplete without an active fence"
                    )
                while self.durable_p1_rowid < active_watermark:
                    latest = self.market_source.latest_p1_rowid()
                    if latest < active_watermark:
                        if source_unavailable_wait is None:
                            raise BindingConflict(
                                "captured timer watermark is temporarily unavailable"
                            )
                        wait_started = time.perf_counter()
                        source_unavailable_wait()
                        waited = time.perf_counter() - wait_started
                        self._performance[
                            "source_unavailable_fence_wait_seconds"
                        ] += waited
                        if waited < 0.001:
                            raise BindingConflict(
                                "source-unavailable callback returned without a bounded wait"
                            )
                        continue
                    if progress_probe is not None:
                        progress_probe(latest)
                    before = self.durable_p1_rowid
                    batch_started = time.perf_counter()
                    result = self.process_next_batch(batch_size=batch_size)
                    batch_seconds = time.perf_counter() - batch_started
                    if result.durable_p1_rowid <= before:
                        raise BindingConflict(
                            "active timer drain made no durable cursor progress"
                        )
                    batch_count += 1
                    raw_rows += result.raw_rows_fetched
                    normalized_rows += result.normalized_records
                    self._performance["fence_drain_batches"] += 1
                    self._performance["fence_drain_raw_rows"] += (
                        result.raw_rows_fetched
                    )
                    self._performance["fence_drain_normalized_rows"] += (
                        result.normalized_records
                    )
                    if batch_observer is not None:
                        batch_observer(result, batch_seconds)
                    self._fence_inject("after_fence_batch")
                    if progress_probe is not None:
                        progress_probe(self.market_source.latest_p1_rowid())

                self._fence_inject("after_final_cursor_before_timer")
                rows = self.conn.execute(
                    "SELECT timer_key FROM paper_fp_binding_prepared_timers_v0_1 "
                    "WHERE source_watermark_p1_rowid=? AND status<>'COMPLETE' "
                    "ORDER BY timer_key",
                    (active_watermark,),
                ).fetchall()
                if not rows:
                    raise BindingConflict("active timer fence has no incomplete timers")
                for row in rows:
                    active_key = str(row["timer_key"])
                    timer_result = self.execute_prepared_timer(active_key)
                    if timer_result.status != "COMMITTED":
                        raise BindingConflict(
                            f"active timer did not commit: {timer_result!r}"
                        )
                    executed.append(active_key)
                    self._fence_inject("after_timer_execution")
        finally:
            self._fence_drain_active = False
            elapsed = time.perf_counter() - drain_started
            self._performance["fence_drain_seconds"] += elapsed
            self._performance["maximum_fence_drain_seconds"] = max(
                self._performance["maximum_fence_drain_seconds"], elapsed
            )
        return ActiveFenceDrainResultV04(
            requested_timer_key=timer_key,
            requested_watermark_p1_rowid=requested_watermark,
            executed_timer_keys=tuple(executed),
            fence_drain_batches=batch_count,
            fence_drain_raw_rows=raw_rows,
            fence_drain_normalized_rows=normalized_rows,
            final_durable_p1_rowid=self.durable_p1_rowid,
        )

    def _hydrate(self, record):
        started = time.perf_counter()
        try:
            return super()._hydrate(record)
        finally:
            self._performance["hydration_seconds"] += time.perf_counter() - started

    def _plan_record(self, *args, **kwargs) -> None:
        started = time.perf_counter()
        try:
            return super()._plan_record(*args, **kwargs)
        finally:
            self._performance["strategy_evaluation_seconds"] += (
                time.perf_counter() - started
            )

    def _drain_evaluations(self, input_key: str) -> int:
        started = time.perf_counter()
        transaction_started = started
        count = 0
        rows = self.conn.execute(
            "SELECT * FROM paper_fp_binding_evaluation_audit_v0_1 "
            "WHERE input_key=? ORDER BY ordinal",
            (input_key,),
        ).fetchall()
        try:
            with self.conn:
                for row in rows:
                    payload = json.loads(str(row["evaluation_json"]))
                    if binding_v01._fingerprint(payload) != str(
                        row["content_fingerprint"]
                    ):
                        raise BindingConflict(
                            "evaluation audit ledger content conflict"
                        )
                    evaluation = binding_v01._audit_from_dict(payload)
                    evaluation_id = self._record_strategy_evaluation_no_commit(
                        evaluation
                    )
                    if int(row["delivered"]) == 0:
                        self._inject(
                            input_key,
                            "after_evaluation_persistence_before_audit_delivery",
                        )
                        self.conn.execute(
                            "UPDATE paper_fp_binding_evaluation_audit_v0_1 "
                            "SET accepted_evaluation_id=?,delivered=1 "
                            "WHERE input_key=? AND ordinal=?",
                            (evaluation_id, input_key, int(row["ordinal"])),
                        )
                        count += 1
                    elif evaluation_id != str(row["accepted_evaluation_id"]):
                        raise BindingConflict("accepted evaluation identity conflict")
            self._performance["evaluation_delivery_transactions"] += bool(rows)
            return count
        finally:
            elapsed = time.perf_counter() - started
            self._performance["semantic_outbox_seconds"] += elapsed
            self._performance["sqlite_transaction_seconds"] += (
                time.perf_counter() - transaction_started
            )

    def _drain_input(self, input_key: str) -> int:
        started = time.perf_counter()
        try:
            return super()._drain_input(input_key)
        finally:
            self._performance["semantic_outbox_seconds"] += (
                time.perf_counter() - started
            )

    def _complete_production_input(self, input_key: str, rowid: int) -> None:
        started = time.perf_counter()
        try:
            return super()._complete_production_input(input_key, rowid)
        finally:
            elapsed = time.perf_counter() - started
            self._performance["cursor_commit_seconds"] += elapsed
            self._performance["sqlite_transaction_seconds"] += elapsed

    def _record_strategy_evaluation_no_commit(self, item) -> str:
        if not item.run_id or not item.role or not item.mint:
            raise ValueError("run_id/role/mint required")
        if item.ingest_seq < 0 or item.evaluated_at_us < 0:
            raise ValueError("ingest_seq/evaluated_at_us must be >= 0")
        payload = {
            "run_id": item.run_id,
            "role": item.role,
            "mint": item.mint,
            "strategy_version": item.strategy_version,
            "parameter_set_id": item.parameter_set_id,
            "ingest_seq": int(item.ingest_seq),
            "evaluated_at_us": int(item.evaluated_at_us),
            "current_state": item.current_state,
            "transition_occurred": bool(item.transition_occurred),
            "reason_code": item.reason_code,
            "filters_passed": list(item.filters_passed),
            "filters_failed": list(item.filters_failed),
            "signal_type": item.signal_type,
            "candidate_signal_id": item.candidate_signal_id,
            "audit_snapshot": item.audit_snapshot,
        }
        fingerprint = observability_v01._fingerprint(payload)
        evaluation_id = observability_v01._hash_id(
            "PSE",
            observability_v01.SCHEMA_VERSION,
            item.run_id,
            item.role,
            item.ingest_seq,
        )
        existing = self.conn.execute(
            "SELECT content_fingerprint FROM paper_strategy_evaluations "
            "WHERE evaluation_id=?",
            (evaluation_id,),
        ).fetchone()
        if existing is not None:
            if str(existing["content_fingerprint"]) != fingerprint:
                raise observability_v01.ObservabilityDeterminismConflict(
                    f"strategy evaluation replay conflict: {evaluation_id}"
                )
            return evaluation_id
        self.conn.execute(
            "INSERT INTO paper_strategy_evaluations("
            "evaluation_id,run_id,role,mint,strategy_version,parameter_set_id,"
            "ingest_seq,evaluated_at_us,current_state,transition_occurred,"
            "reason_code,filters_passed_json,filters_failed_json,signal_type,"
            "candidate_signal_id,audit_snapshot_json,content_fingerprint,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                evaluation_id,
                item.run_id,
                item.role,
                item.mint,
                item.strategy_version,
                item.parameter_set_id,
                int(item.ingest_seq),
                int(item.evaluated_at_us),
                item.current_state,
                1 if item.transition_occurred else 0,
                item.reason_code,
                observability_v01._canonical_json(list(item.filters_passed)),
                observability_v01._canonical_json(list(item.filters_failed)),
                item.signal_type,
                item.candidate_signal_id,
                observability_v01._canonical_json(item.audit_snapshot),
                fingerprint,
                datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            ),
        )
        return evaluation_id

    def _bind_runtime_v03(self) -> None:
        row = self.conn.execute(
            "SELECT * FROM paper_fp_binding_runtime_v0_1 WHERE singleton=1"
        ).fetchone()
        # V03 invokes this hook before constructing self.runner.
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
                "persisted v0.4 binding identity conflict: "
                f"actual={actual!r} expected={expected!r}"
            )

    def _fence_inject(self, phase: str) -> None:
        if self._fence_drain_active and self._fence_failure_injector is not None:
            self._fence_failure_injector(phase)
