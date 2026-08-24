from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from phase2.denomination_aware_flow_v0_1 import (
    DenominationAwareFeatureEngineV01,
)
from phase2.models_v0_1 import (
    CandidateSignal,
    ConfirmationState,
    EventType,
    FirstPullbackParameterSet,
    FirstPullbackState,
    IngestionSource,
    MarketPoint,
    NormalizedMarketEvent,
    StrategyRunState,
)
from phase2.quote_aware_price_v0_1 import (
    Phase1QuoteAwareAdapterV01,
    QuoteAwareNormalizedEvent,
    QuoteAwarePriceEngineV01,
)
from phase2.strategy_clock_v0_2 import (
    FirstPullbackStrategyClockV02,
    StrategyDeadlineV02,
)
from phase2.strategy_first_pullback_v0_2 import FirstPullbackStrategyV02

from .paper_continuous_market_source_v0_1 import (
    MODEL_FINGERPRINT as MARKET_SOURCE_FINGERPRINT,
    MODEL_ID as MARKET_SOURCE_MODEL_ID,
    ContinuousMarketSourceRecordV01,
    ContinuousMarketSourceV01,
)
from .paper_continuous_runner_v0_1 import (
    MODEL_ID as RUNNER_MODEL_ID,
    RUNNER_SPEC_FINGERPRINT,
    CandidateEvaluationEvent,
    ClockTickEvent,
    ContinuousPaperRunnerV01,
    ContinuousSourceItem,
    MarketObservationEvent,
    TerminalSkipEvaluationEvent,
)
from .paper_entry_router_v0_1 import CandidateSignalEnvelope
from .paper_runtime_observability_v0_1 import StrategyEvaluationAuditInput
from .phase1_gap_source_compat_v0_1 import (
    MODEL_FINGERPRINT as GAP_COMPAT_FINGERPRINT,
    MODEL_ID as GAP_COMPAT_MODEL_ID,
    Phase1GapSourceCompatibilityV01,
)


MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0001"
SCHEMA_VERSION = "phase4_continuous_firstpullback_binding_v0.1"
LOCKED_ROLE_ORDER = ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3")
LOCKED_SELECTION_SHA256 = (
    "408657c1d6dc39435b61e01b368dac69796481864b7462efd150a2d4e1b0daa1"
)

_PARAMETER_ROWS: dict[str, dict[str, int | str]] = {
    "CONTROL": {
        "parameter_set_id": "FP1-EXP0005-D-005-CONTROL-R500-T2-U1-PD1000-9000-RB200-BU1-FL0-RC200-RW10000",
        "min_return_bps": 500, "min_trades_since_t0": 2,
        "min_unique_buyers_since_t0": 1, "min_depth_bps": 1000,
        "max_depth_bps": 9000, "min_rebound_bps": 200,
        "min_buys": 1, "min_net_flow_reserve_ppm": 0,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 10000,
    },
    "ROBUST_1": {
        "parameter_set_id": "FP1-EXP0005-D-026-ROBUST_1-R1500-T5-U9-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
        "min_return_bps": 1500, "min_trades_since_t0": 5,
        "min_unique_buyers_since_t0": 9, "min_depth_bps": 2500,
        "max_depth_bps": 6500, "min_rebound_bps": 2300,
        "min_buys": 2, "min_net_flow_reserve_ppm": 6000,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 2500,
    },
    "ROBUST_2": {
        "parameter_set_id": "FP1-EXP0005-D-050-ROBUST_2-R2000-T3-U5-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
        "min_return_bps": 2000, "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 5, "min_depth_bps": 2500,
        "max_depth_bps": 6500, "min_rebound_bps": 2300,
        "min_buys": 2, "min_net_flow_reserve_ppm": 6000,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 2500,
    },
    "ROBUST_3": {
        "parameter_set_id": "FP1-EXP0005-D-074-ROBUST_3-R5000-T3-U2-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
        "min_return_bps": 5000, "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 2, "min_depth_bps": 2500,
        "max_depth_bps": 6500, "min_rebound_bps": 2300,
        "min_buys": 2, "min_net_flow_reserve_ppm": 6000,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 2500,
    },
}

LOCKED_PARAMETER_SET_BY_ROLE = {
    role: str(row["parameter_set_id"]) for role, row in _PARAMETER_ROWS.items()
}

_SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "feature_engine": "DenominationAwareFeatureEngineV01",
    "feature_engine_schema": DenominationAwareFeatureEngineV01.schema_version,
    "quote_adapter_schema": Phase1QuoteAwareAdapterV01.schema_version,
    "quote_price_engine_schema": QuoteAwarePriceEngineV01.schema_version,
    "strategy_implementation": "FirstPullbackStrategyV02",
    "strategy_name": FirstPullbackStrategyV02.strategy_name,
    "strategy_version": FirstPullbackStrategyV02.strategy_version,
    "strategy_run_schema": FirstPullbackStrategyV02.run_schema_version,
    "strategy_clock": "FirstPullbackStrategyClockV02",
    "strategy_clock_schema": FirstPullbackStrategyClockV02.schema_version,
    "strategy_clock_deadline_schema": FirstPullbackStrategyClockV02.deadline_schema_version,
    "selection_sha256": LOCKED_SELECTION_SHA256,
    "role_order": LOCKED_ROLE_ORDER,
    "parameters": _PARAMETER_ROWS,
    "arbitration": "FIRST_SOL_NATIVE_CANDIDATE_IN_LOCKED_ROLE_ORDER_PER_MINT_LIFECYCLE",
    "same_row_order": "STRATEGY_TERMINAL_ACTIONS_THEN_MARKET_OBSERVATION",
    "evaluation_audit": (
        "EVERY_ACTUAL_MARKET_OR_STRATEGY_CLOCK_EVALUATION_DURABLE_EXACTLY_ONCE_"
        "IN_ACCEPTED_PAPER_STRATEGY_EVALUATIONS_VIA_REPLAY_SAFE_AUDIT_LEDGER"
    ),
    "market_source_model_id": MARKET_SOURCE_MODEL_ID,
    "market_source_fingerprint": MARKET_SOURCE_FINGERPRINT,
    "gap_compat_model_id": GAP_COMPAT_MODEL_ID,
    "gap_compat_fingerprint": GAP_COMPAT_FINGERPRINT,
    "runner_model_id": RUNNER_MODEL_ID,
    "runner_fingerprint": RUNNER_SPEC_FINGERPRINT,
    "source_hydration": "EXACT_ROWID_READONLY_ACCEPTED_GAP_COMPAT_WITH_PROJECTION_ASSERTION",
    "cursor": "DURABLE_FETCHED_PREFIX_AFTER_ALL_OUTBOX_ITEMS_RUNNER_COMMITTED",
    "outbox": "MONOTONIC_SQLITE_SEQUENCE_IMMUTABLE_CONTENT_FINGERPRINT",
    "timer_preparation": (
        "BINDING_CAPTURED_IMMUTABLE_PRODUCTION_P1_WATERMARK_PREPARE_THEN_DRAIN_"
        "THEN_EXECUTE_RESTART_STABLE"
    ),
    "timer_production_fence": (
        "PREPARED_TIMER_ESTABLISHES_PRODUCTION_CONSUMPTION_FENCE_UNTIL_ALL_"
        "TIMERS_AT_EARLIEST_WATERMARK_COMPLETE"
    ),
    "strategy_timer": "EXPLICIT_AWARE_TIMESTAMP_DEADLINE_AT_T0_PLUS_300000MS_PLUS_1US",
    "exit_timer": "SEPARATE_EXPLICIT_RUNNER_CLOCK_ITEM_AFTER_STRATEGY_TIMER_ITEMS",
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(_SPEC, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
).hexdigest()

UTC = timezone.utc
FailureInjector = Callable[[str, str], None]


class BindingConflict(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class BindingBatchResultV01:
    requested_after_p1_rowid: int
    highest_fetched_p1_rowid: int
    raw_rows_fetched: int
    normalized_records: int
    deterministic_skips: int
    runner_items_committed: int
    durable_p1_rowid: int


@dataclass(frozen=True, slots=True)
class BindingTimerResultV01:
    timer_key: str
    status: str
    source_watermark_p1_rowid: int
    strategy_terminal_items: int
    exit_clock_items: int
    runner_items_committed: int


def locked_parameter_sets() -> dict[str, FirstPullbackParameterSet]:
    result: dict[str, FirstPullbackParameterSet] = {}
    for role in LOCKED_ROLE_ORDER:
        row = _PARAMETER_ROWS[role]
        params = FirstPullbackParameterSet(
            parameter_set_id=str(row["parameter_set_id"]),
            strategy_version="v1.1",
            max_entry_age_ms=300_000,
            impulse_parameters={
                "min_return_bps": int(row["min_return_bps"]),
                "min_trades_since_t0": int(row["min_trades_since_t0"]),
                "min_unique_buyers_since_t0": int(row["min_unique_buyers_since_t0"]),
            },
            pullback_parameters={
                "min_depth_bps": int(row["min_depth_bps"]),
                "max_depth_bps": int(row["max_depth_bps"]),
            },
            buyer_response_parameters={
                "window_id": "3s",
                "min_rebound_bps": int(row["min_rebound_bps"]),
                "min_buys": int(row["min_buys"]),
                "min_net_flow_reserve_ppm": int(row["min_net_flow_reserve_ppm"]),
            },
            reclaim_parameters={
                "min_extension_from_response_bps": int(row["min_extension_from_response_bps"]),
            },
            runaway_entry_parameters={
                "max_extension_from_response_bps": int(row["max_extension_from_response_bps"]),
            },
        )
        FirstPullbackStrategyV02.validate_parameters(params)
        result[role] = params
    return result


class ContinuousFirstPullbackBindingV01:
    model_id = MODEL_ID
    model_fingerprint = MODEL_FINGERPRINT
    schema_version = SCHEMA_VERSION

    def __init__(
        self,
        conn: sqlite3.Connection,
        market_source: ContinuousMarketSourceV01,
        *,
        wall_clock: Callable[[], datetime] | None = None,
        failure_injector: FailureInjector | None = None,
        runner_failure_injector: Callable[[ContinuousSourceItem, str], None] | None = None,
    ) -> None:
        if market_source.start_after_p1_rowid < 0:
            raise ValueError("market-source anchor must be non-negative")
        if MARKET_SOURCE_FINGERPRINT != "242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb":
            raise BindingConflict("accepted market-source fingerprint drift")
        if GAP_COMPAT_FINGERPRINT != "aabb765f81a29b2dd119607cdea92a2949dd3acec10aa204d246e6aa7a28a78c":
            raise BindingConflict("accepted gap-compatibility fingerprint drift")
        if RUNNER_SPEC_FINGERPRINT != "8dcab9d17f2ec71f4920a199b5745dae92dc3464a466860593231424e0a24dea":
            raise BindingConflict("accepted continuous-runner fingerprint drift")
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.market_source = market_source
        self.wall_clock = wall_clock or (lambda: datetime.now(UTC))
        self.failure_injector = failure_injector
        self.params_by_role = locked_parameter_sets()
        self.reconstruction_queries = 0
        _create_schema(self.conn)
        self._binding_source_identity = _fingerprint({
            "binding_model_id": MODEL_ID,
            "binding_fingerprint": MODEL_FINGERPRINT,
            "market_source_identity": market_source.source_identity,
            "start_after_p1_rowid": market_source.start_after_p1_rowid,
        })
        self._bind_runtime()
        self._verify_prepared_timers()
        self.conn.commit()
        self.runner = ContinuousPaperRunnerV01(
            conn,
            source_identity=self._binding_source_identity,
            wall_clock=self.wall_clock,
            failure_injector=runner_failure_injector,
        )
        self.conn.commit()
        self._engine = self._rebuild_engine()

    @property
    def source_identity(self) -> str:
        return self._binding_source_identity

    @property
    def durable_p1_rowid(self) -> int:
        return int(self._runtime()["last_durable_p1_rowid"])

    @property
    def next_event_sequence(self) -> int:
        return int(self._runtime()["next_event_sequence"])

    @property
    def active_timer_fence_p1_rowid(self) -> int | None:
        return self._active_timer_fence()

    def process_next_batch(self, *, batch_size: int = 1_000) -> BindingBatchResultV01:
        after = self.durable_p1_rowid
        fence = self._active_timer_fence()
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
        effective_batch_size = batch_size
        if fence is not None:
            # Rowids may be sparse, so this bound minimizes read-ahead but the
            # post-fetch filter below remains the authoritative semantic fence.
            effective_batch_size = min(batch_size, fence - after)
        batch = self.market_source.fetch_batch(
            after_p1_rowid=after, batch_size=effective_batch_size
        )
        records = {r.production_p1_rowid: r for r in batch.records}
        skips = {s.production_p1_rowid: s for s in batch.skips}
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
            elif rowid in skips:
                self._handle_skip(rowid, _fingerprint(skips[rowid]))
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

    def prepare_clock_tick(
        self,
        now: datetime,
        *,
        drive_exit_clock: bool = True,
        timer_id: str | None = None,
    ) -> str:
        instant = _utc(now)
        timer_key = timer_id or ("CLOCK:" + _dt_text(instant))
        return self._prepare_timer(
            timer_key, "STRATEGY_CLOCK", instant, drive_exit_clock
        )

    def prepare_exit_clock_tick(
        self, now: datetime, *, timer_id: str | None = None
    ) -> str:
        instant = _utc(now)
        timer_key = timer_id or ("EXIT-CLOCK:" + _dt_text(instant))
        return self._prepare_timer(timer_key, "EXIT_CLOCK", instant, True)

    def submit_clock_tick(
        self, now: datetime, *, drive_exit_clock: bool = True
    ) -> BindingTimerResultV01:
        key = self.prepare_clock_tick(now, drive_exit_clock=drive_exit_clock)
        return self.execute_prepared_timer(key)

    def submit_exit_clock_tick(self, now: datetime) -> BindingTimerResultV01:
        key = self.prepare_exit_clock_tick(now)
        return self.execute_prepared_timer(key)

    def execute_prepared_timer(self, timer_key: str) -> BindingTimerResultV01:
        self._active_timer_fence()
        timer = self._prepared_timer(timer_key)
        watermark = int(timer["source_watermark_p1_rowid"])
        if self.durable_p1_rowid < watermark:
            return BindingTimerResultV01(timer_key, "PENDING", watermark, 0, 0, 0)

        status = str(timer["status"])
        input_key = str(timer["input_key"] or ("TIMER-INPUT:" + timer_key))
        if status == "PREPARED":
            events: list[tuple[str, Any]] = []
            evaluations: list[StrategyEvaluationAuditInput] = []
            strategy_items = 0
            exit_items = 0
            clock_type = str(timer["clock_type"])
            instant = _dt_parse(str(timer["clock_timestamp"]))
            if clock_type == "STRATEGY_CLOCK":
                runs = self.conn.execute(
                    "SELECT r.* FROM paper_fp_binding_strategy_runs_v0_1 r "
                    "WHERE r.candidate_signal_id IS NULL AND NOT EXISTS ("
                    "SELECT 1 FROM paper_fp_binding_strategy_runs_v0_1 winner "
                    "WHERE winner.mint=r.mint AND winner.candidate_signal_id IS NOT NULL) "
                    "ORDER BY r.mint, r.role_rank"
                ).fetchall()
                for row in runs:
                    run = _run_from_json(str(row["run_json"]))
                    deadline = _deadline_from_json(row["deadline_json"])
                    if deadline is None or run.finished or run.signal_emitted:
                        continue
                    role = str(row["role"])
                    params = self.params_by_role[role]
                    evaluation = FirstPullbackStrategyClockV02.fire_if_due(
                        deadline, run, params, now_us=_datetime_to_us(instant)
                    )
                    if evaluation is None:
                        continue
                    audit = _audit_input(
                        evaluation, run, role, params,
                        _clock_audit_ingest_seq(timer_key, watermark),
                    )
                    self._persist_run(
                        str(row["mint"]), role, run, deadline,
                        int(row["last_ingest_seq"]), None,
                    )
                    evaluations.append(audit)
                    events.append((
                        f"{timer_key}:STRATEGY:{row['mint']}:{role}",
                        TerminalSkipEvaluationEvent(audit),
                    ))
                    strategy_items += 1
            if clock_type == "EXIT_CLOCK" or bool(timer["drive_exit_clock"]):
                events.append((f"{timer_key}:EXIT", ClockTickEvent(instant)))
                exit_items = 1
            content_fp = str(timer["content_fingerprint"])
            with self.conn:
                self._persist_input_and_events(
                    input_key, "PREPARED_TIMER", None, content_fp, events,
                    evaluations=evaluations,
                )
                self.conn.execute(
                    "UPDATE paper_fp_binding_prepared_timers_v0_1 "
                    "SET status='PLANNED',input_key=?,updated_at=? WHERE timer_key=?",
                    (input_key, _dt_text(_utc(self.wall_clock())), timer_key),
                )
        else:
            strategy_items = int(self.conn.execute(
                "SELECT COUNT(*) FROM paper_fp_binding_evaluation_audit_v0_1 "
                "WHERE input_key=?", (input_key,)
            ).fetchone()[0])
            exit_items = int(self.conn.execute(
                "SELECT COUNT(*) FROM paper_fp_binding_outbox_v0_1 "
                "WHERE input_key=? AND event_type='ClockTickEvent'", (input_key,)
            ).fetchone()[0])

        self._drain_evaluations(input_key)
        committed = self._drain_input(input_key)
        self._mark_input_complete(input_key)
        with self.conn:
            self.conn.execute(
                "UPDATE paper_fp_binding_prepared_timers_v0_1 "
                "SET status='COMPLETE',updated_at=? WHERE timer_key=?",
                (_dt_text(_utc(self.wall_clock())), timer_key),
            )
        return BindingTimerResultV01(
            timer_key, "COMMITTED", watermark, strategy_items, exit_items, committed
        )

    def _prepare_timer(
        self, timer_key: str, clock_type: str, instant: datetime,
        drive_exit_clock: bool,
    ) -> str:
        if not timer_key:
            raise ValueError("timer identity must not be empty")
        existing = self.conn.execute(
            "SELECT * FROM paper_fp_binding_prepared_timers_v0_1 WHERE timer_key=?",
            (timer_key,),
        ).fetchone()
        if existing is not None:
            self._validate_timer_row(existing)
            intended = (
                clock_type, _dt_text(instant), 1 if drive_exit_clock else 0,
            )
            actual = (
                str(existing["clock_type"]), str(existing["clock_timestamp"]),
                int(existing["drive_exit_clock"]),
            )
            if actual != intended:
                raise BindingConflict("prepared timer identity replayed with different meaning")
            return timer_key
        watermark = max(
            self.market_source.start_after_p1_rowid,
            self.market_source.latest_p1_rowid(),
        )
        if watermark < self.durable_p1_rowid:
            raise BindingConflict(
                "captured timer watermark is behind the durable production cursor"
            )
        payload = _timer_payload(
            timer_key, clock_type, instant, drive_exit_clock, watermark
        )
        fp = _fingerprint(payload)
        now = _dt_text(_utc(self.wall_clock()))
        with self.conn:
            self.conn.execute(
                "INSERT INTO paper_fp_binding_prepared_timers_v0_1("
                "timer_key,clock_type,clock_timestamp,drive_exit_clock,"
                "source_watermark_p1_rowid,content_fingerprint,status,input_key,"
                "created_at,updated_at) VALUES(?,?,?,?,?,?,'PREPARED',NULL,?,?)",
                (timer_key, clock_type, _dt_text(instant),
                 1 if drive_exit_clock else 0, watermark, fp, now, now),
            )
        return timer_key

    def pending_outbox_count(self) -> int:
        return int(self.conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_outbox_v0_1 WHERE delivered=0"
        ).fetchone()[0])

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
            rows = [dict(r) for r in self.conn.execute(
                f"SELECT * FROM {table} ORDER BY rowid"
            ).fetchall()]
            payload[table] = rows
        return _fingerprint(payload)

    def _handle_record(self, record: ContinuousMarketSourceRecordV01) -> int:
        input_key = f"P1:{record.production_p1_rowid}"
        content_fp = record.content_fingerprint
        existing = self.conn.execute(
            "SELECT content_fingerprint FROM paper_fp_binding_inputs_v0_1 WHERE input_key=?",
            (input_key,),
        ).fetchone()
        if existing is None:
            qevent = self._hydrate(record)
            try:
                self._plan_record(input_key, record, qevent, content_fp)
            except Exception:
                self._engine = self._rebuild_engine()
                raise
            self._inject(input_key, "after_strategy_state_persistence_before_runner")
        elif str(existing["content_fingerprint"]) != content_fp:
            raise BindingConflict("production row replayed with different source content")
        self._drain_evaluations(input_key)
        committed = self._drain_input(input_key)
        self._inject(input_key, "after_runner_items_before_production_cursor")
        self._complete_production_input(input_key, record.production_p1_rowid)
        return committed

    def _handle_skip(self, rowid: int, content_fp: str) -> None:
        input_key = f"P1:{rowid}"
        existing = self.conn.execute(
            "SELECT content_fingerprint FROM paper_fp_binding_inputs_v0_1 WHERE input_key=?",
            (input_key,),
        ).fetchone()
        if existing is None:
            with self.conn:
                self._persist_input_and_events(input_key, "SKIP", rowid, content_fp, [])
        elif str(existing["content_fingerprint"]) != content_fp:
            raise BindingConflict("deterministic skip replay conflict")
        self._inject(input_key, "after_skip_semantics_before_production_cursor")
        self._complete_production_input(input_key, rowid)

    def _hydrate(self, record: ContinuousMarketSourceRecordV01) -> QuoteAwareNormalizedEvent:
        conn = self.market_source.open_readonly(self.market_source.db_path)
        try:
            self.reconstruction_queries += 1
            row = conn.execute(
                "SELECT rowid AS p1_rowid, * FROM pump_events WHERE rowid=?",
                (record.production_p1_rowid,),
            ).fetchone()
            if row is None:
                raise BindingConflict("accepted market-source row disappeared")
            compat = Phase1GapSourceCompatibilityV01.row_to_event(row)
        finally:
            conn.close()
        qevent = compat.event
        if qevent is None:
            raise BindingConflict("accepted market-source record no longer normalizes")
        base = qevent.base
        point = QuoteAwarePriceEngineV01.point(qevent)
        projection = (
            int(base.ingest_seq), str(base.event_key), str(base.mint),
            str(base.event_type.value), _us_to_datetime(int(base.event_at_us)),
            _us_to_datetime(int(base.observed_at_us)), str(point.identity.label),
            point.numerator_reserve_raw, point.token_reserve_raw,
            base.source == IngestionSource.GAP_RECOVERY,
            str(base.source.value), compat.raw_source_decoded_file or "",
            compat.compatibility_applied,
        )
        expected = (
            record.ingest_seq, record.event_key, record.mint, record.event_type,
            record.event_at, record.observed_at, record.price_identity,
            record.price_numerator_raw, record.price_denominator_raw,
            record.is_gap_recovery, record.ingestion_source,
            record.raw_source_decoded_file, record.source_compatibility_applied,
        )
        if projection != expected:
            raise BindingConflict("hydrated Phase-2 event differs from accepted source projection")
        return qevent

    def _plan_record(
        self, input_key: str, record: ContinuousMarketSourceRecordV01,
        qevent: QuoteAwareNormalizedEvent, content_fp: str,
    ) -> None:
        state = self._engine.process(qevent)
        events: list[tuple[str, Any]] = []
        evaluations: list[StrategyEvaluationAuditInput] = []
        mint_lock = self.conn.execute(
            "SELECT candidate_signal_id FROM paper_fp_binding_strategy_runs_v0_1 "
            "WHERE mint=? AND candidate_signal_id IS NOT NULL LIMIT 1",
            (record.mint,),
        ).fetchone()
        if mint_lock is None:
            for rank, role in enumerate(LOCKED_ROLE_ORDER):
                params = self.params_by_role[role]
                persisted = self.conn.execute(
                    "SELECT * FROM paper_fp_binding_strategy_runs_v0_1 WHERE mint=? AND role=?",
                    (record.mint, role),
                ).fetchone()
                if persisted is None:
                    run = FirstPullbackStrategyV02.start_run(state, params)
                    deadline = None
                else:
                    run = _run_from_json(str(persisted["run_json"]))
                    deadline = _deadline_from_json(persisted["deadline_json"])
                if deadline is None:
                    deadline = FirstPullbackStrategyClockV02.schedule_entry_expiry(
                        state, run, params
                    )
                evaluation = None
                if (
                    deadline is not None
                    and state.as_of_observed_at_us >= deadline.expire_at_us
                    and not run.finished
                ):
                    evaluation = FirstPullbackStrategyClockV02.fire_if_due(
                        deadline, run, params, now_us=state.as_of_observed_at_us
                    )
                elif not run.finished and not run.signal_emitted:
                    evaluation = FirstPullbackStrategyV02.evaluate(state, run, params)
                candidate_id = None
                if evaluation is not None and evaluation.candidate_signal is not None:
                    candidate_id = str(evaluation.candidate_signal.signal_id)
                winning_candidate_id = (
                    candidate_id
                    if candidate_id is not None
                    and record.price_identity == "SOL_NATIVE"
                    and not record.is_gap_recovery
                    else None
                )
                self._persist_run(
                    record.mint, role, run, deadline, record.ingest_seq,
                    winning_candidate_id,
                    role_rank=rank,
                )
                if evaluation is None:
                    continue
                audit = _audit_input(evaluation, run, role, params, record.ingest_seq)
                evaluations.append(audit)
                candidate = evaluation.candidate_signal
                if candidate is not None:
                    envelope = CandidateSignalEnvelope(
                        candidate_id=str(candidate.signal_id), mint=record.mint,
                        strategy_version=str(candidate.strategy_version),
                        parameter_set_id=str(candidate.parameter_set_id),
                        signal_observed_at=_us_to_datetime(int(candidate.generated_at_us)),
                        signal_ingest_seq=record.ingest_seq,
                        price_identity=record.price_identity,
                        price_numerator_raw=record.price_numerator_raw,
                        price_denominator_raw=record.price_denominator_raw,
                    )
                    events.append((
                        f"{record.event_key}:CANDIDATE:{role}",
                        CandidateEvaluationEvent(audit, envelope),
                    ))
                    if record.price_identity == "SOL_NATIVE" and not record.is_gap_recovery:
                        break
                elif run.finished:
                    events.append((
                        f"{record.event_key}:TERMINAL:{role}",
                        TerminalSkipEvaluationEvent(audit),
                    ))
        events.append((
            f"{record.event_key}:MARKET",
            MarketObservationEvent(
                mint=record.mint, observed_at=record.observed_at,
                ingest_seq=record.ingest_seq, price_identity=record.price_identity,
                price_numerator_raw=record.price_numerator_raw,
                price_denominator_raw=record.price_denominator_raw,
                current_virtual_token_reserve_raw=record.current_virtual_token_reserve_raw,
                is_gap_recovery=record.is_gap_recovery,
            ),
        ))
        with self.conn:
            self.conn.execute(
                "INSERT INTO paper_fp_binding_feature_events_v0_1("
                "production_p1_rowid,mint,event_json) VALUES(?,?,?)",
                (record.production_p1_rowid, record.mint, _json(qevent)),
            )
            self._persist_input_and_events(
                input_key, "MARKET", record.production_p1_rowid, content_fp, events,
                evaluations=evaluations,
            )

    def _persist_run(
        self, mint: str, role: str, run: StrategyRunState,
        deadline: StrategyDeadlineV02 | None, last_ingest_seq: int,
        candidate_signal_id: str | None, *, role_rank: int | None = None,
    ) -> None:
        rank = LOCKED_ROLE_ORDER.index(role) if role_rank is None else role_rank
        existing = self.conn.execute(
            "SELECT candidate_signal_id FROM paper_fp_binding_strategy_runs_v0_1 "
            "WHERE mint=? AND role=?", (mint, role)
        ).fetchone()
        locked_candidate = (
            str(existing["candidate_signal_id"])
            if existing is not None and existing["candidate_signal_id"] is not None
            else candidate_signal_id
        )
        self.conn.execute(
            "INSERT INTO paper_fp_binding_strategy_runs_v0_1("
            "mint,role,role_rank,parameter_set_id,run_json,deadline_json,last_ingest_seq,"
            "candidate_signal_id) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(mint,role) DO UPDATE SET run_json=excluded.run_json,"
            "deadline_json=excluded.deadline_json,last_ingest_seq=excluded.last_ingest_seq,"
            "candidate_signal_id=COALESCE(paper_fp_binding_strategy_runs_v0_1.candidate_signal_id,"
            "excluded.candidate_signal_id)",
            (mint, role, rank, self.params_by_role[role].parameter_set_id,
             _json(run), None if deadline is None else _json(deadline),
             last_ingest_seq, locked_candidate),
        )

    def _persist_input_and_events(
        self, input_key: str, kind: str, p1_rowid: int | None,
        content_fp: str, events: list[tuple[str, Any]], *,
        evaluations: list[StrategyEvaluationAuditInput] | None = None,
    ) -> None:
        runtime = self._runtime()
        seq = int(runtime["next_event_sequence"])
        created = _dt_text(_utc(self.wall_clock()))
        self.conn.execute(
            "INSERT INTO paper_fp_binding_inputs_v0_1("
            "input_key,input_kind,production_p1_rowid,content_fingerprint,complete,created_at,updated_at) "
            "VALUES(?,?,?,?,0,?,?)",
            (input_key, kind, p1_rowid, content_fp, created, created),
        )
        for ordinal, evaluation in enumerate(evaluations or []):
            payload = _primitive(evaluation)
            self.conn.execute(
                "INSERT INTO paper_fp_binding_evaluation_audit_v0_1("
                "input_key,ordinal,evaluation_json,content_fingerprint,"
                "accepted_evaluation_id,delivered) VALUES(?,?,?,?,NULL,0)",
                (input_key, ordinal, _json(payload), _fingerprint(payload)),
            )
        for ordinal, (event_key, event) in enumerate(events):
            payload = _event_payload(event)
            item_fp = _fingerprint({
                "cursor": seq, "event_key": event_key,
                "event_type": type(event).__name__, "event": payload,
            })
            self.conn.execute(
                "INSERT INTO paper_fp_binding_outbox_v0_1("
                "event_sequence,input_key,ordinal,event_key,event_type,event_json,"
                "content_fingerprint,delivered) VALUES(?,?,?,?,?,?,?,0)",
                (seq, input_key, ordinal, event_key, type(event).__name__,
                 _json(payload), item_fp),
            )
            seq += 1
        self.conn.execute(
            "UPDATE paper_fp_binding_runtime_v0_1 SET next_event_sequence=?,updated_at=? "
            "WHERE singleton=1", (seq, created)
        )

    def _drain_evaluations(self, input_key: str) -> int:
        count = 0
        rows = self.conn.execute(
            "SELECT * FROM paper_fp_binding_evaluation_audit_v0_1 "
            "WHERE input_key=? ORDER BY ordinal", (input_key,)
        ).fetchall()
        for row in rows:
            payload = json.loads(str(row["evaluation_json"]))
            if _fingerprint(payload) != str(row["content_fingerprint"]):
                raise BindingConflict("evaluation audit ledger content conflict")
            evaluation = _audit_from_dict(payload)
            if int(row["delivered"]) == 0:
                evaluation_id = (
                    self.runner.observability.store.record_strategy_evaluation(
                        evaluation
                    )
                )
                self._inject(
                    input_key,
                    "after_evaluation_persistence_before_audit_delivery",
                )
                with self.conn:
                    self.conn.execute(
                        "UPDATE paper_fp_binding_evaluation_audit_v0_1 "
                        "SET accepted_evaluation_id=?,delivered=1 "
                        "WHERE input_key=? AND ordinal=?",
                        (evaluation_id, input_key, int(row["ordinal"])),
                    )
                count += 1
            else:
                expected_id = self.runner.observability.store.record_strategy_evaluation(
                    evaluation
                )
                if expected_id != str(row["accepted_evaluation_id"]):
                    raise BindingConflict("accepted evaluation identity conflict")
        return count

    def _drain_input(self, input_key: str) -> int:
        count = 0
        rows = self.conn.execute(
            "SELECT * FROM paper_fp_binding_outbox_v0_1 WHERE input_key=? "
            "ORDER BY event_sequence", (input_key,)
        ).fetchall()
        for row in rows:
            event = _event_from_payload(
                str(row["event_type"]), json.loads(str(row["event_json"]))
            )
            item = ContinuousSourceItem(
                int(row["event_sequence"]), str(row["event_key"]), event
            )
            if item.content_fingerprint != str(row["content_fingerprint"]):
                raise BindingConflict("outbox item content fingerprint conflict")
            if int(row["delivered"]) == 0:
                self._inject(input_key, "before_runner_item")
                self.runner.process_source_item(item)
                self._inject(input_key, "after_runner_accept_before_outbox_delivery")
                with self.conn:
                    self.conn.execute(
                        "UPDATE paper_fp_binding_outbox_v0_1 SET delivered=1 WHERE event_sequence=?",
                        (item.cursor,),
                    )
                count += 1
        return count

    def _complete_production_input(self, input_key: str, rowid: int) -> None:
        expected = self.durable_p1_rowid + 1
        if rowid < expected:
            self._mark_input_complete(input_key)
            return
        pending = int(self.conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_outbox_v0_1 "
            "WHERE input_key=? AND delivered=0", (input_key,)
        ).fetchone()[0])
        if pending:
            raise BindingConflict("cannot advance production cursor with pending outbox")
        pending_audit = int(self.conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_evaluation_audit_v0_1 "
            "WHERE input_key=? AND delivered=0", (input_key,)
        ).fetchone()[0])
        if pending_audit:
            raise BindingConflict("cannot advance production cursor with pending evaluation audit")
        now = _dt_text(_utc(self.wall_clock()))
        with self.conn:
            self.conn.execute(
                "UPDATE paper_fp_binding_inputs_v0_1 SET complete=1,updated_at=? WHERE input_key=?",
                (now, input_key),
            )
            self.conn.execute(
                "UPDATE paper_fp_binding_runtime_v0_1 SET last_durable_p1_rowid=?,updated_at=? "
                "WHERE singleton=1", (rowid, now)
            )

    def _mark_input_complete(self, input_key: str) -> None:
        pending = int(self.conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_outbox_v0_1 "
            "WHERE input_key=? AND delivered=0", (input_key,)
        ).fetchone()[0])
        if pending:
            raise BindingConflict("cannot complete input with pending outbox")
        pending_audit = int(self.conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_evaluation_audit_v0_1 "
            "WHERE input_key=? AND delivered=0", (input_key,)
        ).fetchone()[0])
        if pending_audit:
            raise BindingConflict("cannot complete input with pending evaluation audit")
        with self.conn:
            self.conn.execute(
                "UPDATE paper_fp_binding_inputs_v0_1 SET complete=1,updated_at=? WHERE input_key=?",
                (_dt_text(_utc(self.wall_clock())), input_key),
            )

    def _bind_runtime(self) -> None:
        row = self.conn.execute(
            "SELECT * FROM paper_fp_binding_runtime_v0_1 WHERE singleton=1"
        ).fetchone()
        expected = (
            MODEL_ID, MODEL_FINGERPRINT, self.market_source.source_identity,
            self.market_source.start_after_p1_rowid, RUNNER_MODEL_ID,
            RUNNER_SPEC_FINGERPRINT, self._binding_source_identity,
        )
        if row is None:
            now = _dt_text(_utc(self.wall_clock()))
            with self.conn:
                self.conn.execute(
                    "INSERT INTO paper_fp_binding_runtime_v0_1("
                    "singleton,binding_model_id,binding_fingerprint,market_source_identity,"
                    "start_after_p1_rowid,last_durable_p1_rowid,runner_model_id,runner_fingerprint,"
                    "runner_source_identity,next_event_sequence,created_at,updated_at) "
                    "VALUES(1,?,?,?,?,?,?,?,?,0,?,?)",
                    (MODEL_ID, MODEL_FINGERPRINT, self.market_source.source_identity,
                     self.market_source.start_after_p1_rowid,
                     self.market_source.start_after_p1_rowid, RUNNER_MODEL_ID,
                     RUNNER_SPEC_FINGERPRINT, self._binding_source_identity, now, now),
                )
            return
        actual = (
            str(row["binding_model_id"]), str(row["binding_fingerprint"]),
            str(row["market_source_identity"]), int(row["start_after_p1_rowid"]),
            str(row["runner_model_id"]), str(row["runner_fingerprint"]),
            str(row["runner_source_identity"]),
        )
        if actual != expected:
            raise BindingConflict(
                f"persisted binding identity conflict: actual={actual!r} expected={expected!r}"
            )

    def _runtime(self) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM paper_fp_binding_runtime_v0_1 WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise BindingConflict("binding runtime row is missing")
        return row

    def _prepared_timer(self, timer_key: str) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM paper_fp_binding_prepared_timers_v0_1 WHERE timer_key=?",
            (timer_key,),
        ).fetchone()
        if row is None:
            raise BindingConflict(f"prepared timer is missing: {timer_key}")
        self._validate_timer_row(row)
        return row

    def _verify_prepared_timers(self) -> None:
        for row in self.conn.execute(
            "SELECT * FROM paper_fp_binding_prepared_timers_v0_1 ORDER BY timer_key"
        ).fetchall():
            self._validate_timer_row(row)
        self._active_timer_fence()

    def _active_timer_fence(self) -> int | None:
        row = self.conn.execute(
            "SELECT MIN(source_watermark_p1_rowid) AS fence "
            "FROM paper_fp_binding_prepared_timers_v0_1 "
            "WHERE status<>'COMPLETE'"
        ).fetchone()
        fence = None if row is None or row["fence"] is None else int(row["fence"])
        if fence is not None and fence < self.durable_p1_rowid:
            raise BindingConflict(
                "incomplete prepared timer watermark is behind the durable production cursor"
            )
        return fence

    @staticmethod
    def _validate_timer_row(row: sqlite3.Row) -> None:
        clock_type = str(row["clock_type"])
        if clock_type not in {"STRATEGY_CLOCK", "EXIT_CLOCK"}:
            raise BindingConflict("prepared timer has unsupported clock type")
        drive = bool(row["drive_exit_clock"])
        if clock_type == "EXIT_CLOCK" and not drive:
            raise BindingConflict("exit-only timer must drive the runner exit clock")
        payload = _timer_payload(
            str(row["timer_key"]), clock_type,
            _dt_parse(str(row["clock_timestamp"])), drive,
            int(row["source_watermark_p1_rowid"]),
        )
        if _fingerprint(payload) != str(row["content_fingerprint"]):
            raise BindingConflict("prepared timer immutable content conflict")
        if str(row["status"]) not in {"PREPARED", "PLANNED", "COMPLETE"}:
            raise BindingConflict("prepared timer status is invalid")

    def _rebuild_engine(self) -> DenominationAwareFeatureEngineV01:
        engine = DenominationAwareFeatureEngineV01()
        rows = self.conn.execute(
            "SELECT event_json FROM paper_fp_binding_feature_events_v0_1 "
            "ORDER BY production_p1_rowid"
        ).fetchall()
        for row in rows:
            engine.process(_quote_event_from_json(str(row["event_json"])))
        return engine

    def _inject(self, input_key: str, phase: str) -> None:
        if self.failure_injector is not None:
            self.failure_injector(input_key, phase)


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS paper_fp_binding_runtime_v0_1 (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            binding_model_id TEXT NOT NULL,
            binding_fingerprint TEXT NOT NULL,
            market_source_identity TEXT NOT NULL,
            start_after_p1_rowid INTEGER NOT NULL,
            last_durable_p1_rowid INTEGER NOT NULL,
            runner_model_id TEXT NOT NULL,
            runner_fingerprint TEXT NOT NULL,
            runner_source_identity TEXT NOT NULL,
            next_event_sequence INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS paper_fp_binding_inputs_v0_1 (
            input_key TEXT PRIMARY KEY,
            input_kind TEXT NOT NULL,
            production_p1_rowid INTEGER UNIQUE,
            content_fingerprint TEXT NOT NULL,
            complete INTEGER NOT NULL CHECK(complete IN (0,1)),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS paper_fp_binding_feature_events_v0_1 (
            production_p1_rowid INTEGER PRIMARY KEY,
            mint TEXT NOT NULL,
            event_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS paper_fp_binding_strategy_runs_v0_1 (
            mint TEXT NOT NULL,
            role TEXT NOT NULL,
            role_rank INTEGER NOT NULL,
            parameter_set_id TEXT NOT NULL,
            run_json TEXT NOT NULL,
            deadline_json TEXT,
            last_ingest_seq INTEGER NOT NULL,
            candidate_signal_id TEXT,
            PRIMARY KEY(mint, role)
        );
        CREATE TABLE IF NOT EXISTS paper_fp_binding_outbox_v0_1 (
            event_sequence INTEGER PRIMARY KEY,
            input_key TEXT NOT NULL REFERENCES paper_fp_binding_inputs_v0_1(input_key),
            ordinal INTEGER NOT NULL,
            event_key TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL,
            event_json TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            delivered INTEGER NOT NULL CHECK(delivered IN (0,1)),
            UNIQUE(input_key, ordinal)
        );
        CREATE TABLE IF NOT EXISTS paper_fp_binding_evaluation_audit_v0_1 (
            input_key TEXT NOT NULL REFERENCES paper_fp_binding_inputs_v0_1(input_key),
            ordinal INTEGER NOT NULL,
            evaluation_json TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            accepted_evaluation_id TEXT,
            delivered INTEGER NOT NULL CHECK(delivered IN (0,1)),
            PRIMARY KEY(input_key, ordinal)
        );
        CREATE TABLE IF NOT EXISTS paper_fp_binding_prepared_timers_v0_1 (
            timer_key TEXT PRIMARY KEY,
            clock_type TEXT NOT NULL CHECK(clock_type IN ('STRATEGY_CLOCK','EXIT_CLOCK')),
            clock_timestamp TEXT NOT NULL,
            drive_exit_clock INTEGER NOT NULL CHECK(drive_exit_clock IN (0,1)),
            source_watermark_p1_rowid INTEGER NOT NULL,
            content_fingerprint TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('PREPARED','PLANNED','COMPLETE')),
            input_key TEXT UNIQUE REFERENCES paper_fp_binding_inputs_v0_1(input_key),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )


def _audit_input(evaluation: Any, run: StrategyRunState, role: str,
                 params: FirstPullbackParameterSet, ingest_seq: int) -> StrategyEvaluationAuditInput:
    candidate = evaluation.candidate_signal
    return StrategyEvaluationAuditInput(
        run_id=run.run_id, role=role, mint=run.mint,
        strategy_version=params.strategy_version,
        parameter_set_id=params.parameter_set_id, ingest_seq=int(ingest_seq),
        evaluated_at_us=int(evaluation.evaluated_at_us),
        current_state=str(evaluation.current_state.value),
        transition_occurred=bool(evaluation.transition_occurred),
        reason_code=str(evaluation.reason_code),
        filters_passed=tuple(str(x) for x in evaluation.filters_passed),
        filters_failed=tuple(str(x) for x in evaluation.filters_failed),
        signal_type="FIRST_PULLBACK" if candidate is not None else None,
        candidate_signal_id=None if candidate is None else str(candidate.signal_id),
        audit_snapshot=_primitive(evaluation.audit_snapshot),
    )


def _timer_payload(
    timer_key: str, clock_type: str, instant: datetime,
    drive_exit_clock: bool, source_watermark_p1_rowid: int,
) -> dict[str, Any]:
    return {
        "timer_key": timer_key,
        "clock_type": clock_type,
        "clock_timestamp": _dt_text(instant),
        "drive_exit_clock": bool(drive_exit_clock),
        "source_watermark_p1_rowid": int(source_watermark_p1_rowid),
        "binding_model_id": MODEL_ID,
        "binding_fingerprint": MODEL_FINGERPRINT,
        "market_source_identity_contract": "PERSISTED_BINDING_RUNTIME",
    }


def _clock_audit_ingest_seq(timer_key: str, watermark: int) -> int:
    # The accepted observability identity is (run, role, ingest_seq). A clock
    # evaluation can follow a market evaluation without a new production row,
    # so it needs a deterministic non-production namespace to remain distinct.
    digest = hashlib.sha256(f"{timer_key}|{watermark}".encode()).hexdigest()
    return 4_000_000_000_000_000_000 + (int(digest[:15], 16) % 1_000_000_000_000_000)


def _event_payload(event: Any) -> dict[str, Any]:
    return _primitive(event)


def _event_from_payload(event_type: str, p: dict[str, Any]) -> Any:
    if event_type == "MarketObservationEvent":
        return MarketObservationEvent(
            mint=p["mint"], observed_at=_dt_parse(p["observed_at"]),
            ingest_seq=int(p["ingest_seq"]), price_identity=p["price_identity"],
            price_numerator_raw=int(p["price_numerator_raw"]),
            price_denominator_raw=int(p["price_denominator_raw"]),
            current_virtual_token_reserve_raw=int(p["current_virtual_token_reserve_raw"]),
            is_gap_recovery=bool(p["is_gap_recovery"]),
        )
    if event_type == "ClockTickEvent":
        return ClockTickEvent(_dt_parse(p["now"]))
    evaluation = _audit_from_dict(p["evaluation"])
    if event_type == "TerminalSkipEvaluationEvent":
        return TerminalSkipEvaluationEvent(evaluation)
    if event_type == "CandidateEvaluationEvent":
        c = p["candidate"]
        candidate = CandidateSignalEnvelope(
            candidate_id=c["candidate_id"], mint=c["mint"],
            strategy_version=c["strategy_version"], parameter_set_id=c["parameter_set_id"],
            signal_observed_at=_dt_parse(c["signal_observed_at"]),
            signal_ingest_seq=int(c["signal_ingest_seq"]),
            price_identity=c["price_identity"],
            price_numerator_raw=int(c["price_numerator_raw"]),
            price_denominator_raw=int(c["price_denominator_raw"]),
        )
        return CandidateEvaluationEvent(evaluation, candidate)
    raise BindingConflict(f"unknown persisted outbox event type: {event_type}")


def _audit_from_dict(p: dict[str, Any]) -> StrategyEvaluationAuditInput:
    return StrategyEvaluationAuditInput(
        run_id=p["run_id"], role=p["role"], mint=p["mint"],
        strategy_version=p["strategy_version"], parameter_set_id=p["parameter_set_id"],
        ingest_seq=int(p["ingest_seq"]), evaluated_at_us=int(p["evaluated_at_us"]),
        current_state=p["current_state"], transition_occurred=bool(p["transition_occurred"]),
        reason_code=p["reason_code"], filters_passed=tuple(p["filters_passed"]),
        filters_failed=tuple(p["filters_failed"]), signal_type=p["signal_type"],
        candidate_signal_id=p["candidate_signal_id"], audit_snapshot=p["audit_snapshot"],
    )


def _run_from_json(value: str) -> StrategyRunState:
    p = json.loads(value)
    for key in ("impulse_start_ref", "impulse_high_ref", "pullback_low_ref", "response_ref", "reclaim_ref"):
        ref = p.get(key)
        if ref is not None:
            p[key] = MarketPoint(
                event_key=ref["event_key"], observed_at_us=int(ref["observed_at_us"]),
                ingest_seq=int(ref["ingest_seq"]), price_proxy=Decimal(ref["price_proxy"]),
            )
    p["current_state"] = FirstPullbackState(p["current_state"])
    if p.get("final_outcome") is not None:
        p["final_outcome"] = FirstPullbackState(p["final_outcome"])
    return StrategyRunState(**p)


def _deadline_from_json(value: str | None) -> StrategyDeadlineV02 | None:
    if value is None:
        return None
    p = json.loads(str(value))
    return StrategyDeadlineV02(**p)


def _quote_event_from_json(value: str) -> QuoteAwareNormalizedEvent:
    p = json.loads(value)
    b = p["base"]
    b["event_type"] = EventType(b["event_type"])
    b["source"] = IngestionSource(b["source"])
    if b.get("confirmation_state") is not None:
        b["confirmation_state"] = ConfirmationState(b["confirmation_state"])
    return QuoteAwareNormalizedEvent(base=NormalizedMarketEvent(**b),
                                     quote_mint=p.get("quote_mint"),
                                     quote_amount_raw=p.get("quote_amount_raw"),
                                     virtual_quote_reserve_raw=p.get("virtual_quote_reserve_raw"),
                                     real_quote_reserve_raw=p.get("real_quote_reserve_raw"))


def _primitive(value: Any) -> Any:
    if is_dataclass(value):
        return _primitive(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite Decimal is prohibited")
        return str(value)
    if isinstance(value, datetime):
        return _dt_text(value)
    if isinstance(value, dict):
        return {str(k): _primitive(v) for k, v in value.items()}
    if isinstance(value, (tuple, list, set)):
        return [_primitive(v) for v in value]
    return value


def _json(value: Any) -> str:
    return json.dumps(_primitive(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _dt_parse(value: str) -> datetime:
    return _utc(datetime.fromisoformat(value))


def _datetime_to_us(value: datetime) -> int:
    return int((_utc(value) - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds() * 1_000_000)


def _us_to_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000, tz=UTC)
