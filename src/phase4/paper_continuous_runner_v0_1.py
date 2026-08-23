from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Iterator, Protocol, TypeAlias

from .paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001
from .paper_cost_model_v0_2 import PaperCostModelV02
from .paper_entry_router_v0_1 import (
    CandidateSignalEnvelope,
    EntryMarketObservation,
    EntryRoute,
    EntryRouteState,
    PaperEntryRouterV01,
    canonical_entry_router_digest,
)
from .paper_exit_fill_executor_v0_1 import (
    ExitExecutionMarketObservation,
    PaperExitFillExecutorV01,
)
from .paper_exit_lifecycle_bridge_v0_2 import PaperExitLifecycleBridgeV02
from .paper_exit_orchestrator_v0_1 import (
    LOCKED_EXIT_SPEC_FINGERPRINT,
    LOCKED_TRACK_ORDER,
    ExitMarketObservation,
)
from .paper_lifecycle_v0_1 import PositionState
from .paper_live_observability_binding_v0_2 import (
    MODEL_ID as LIVE_OBSERVABILITY_BINDING_ID,
    PaperLiveObservabilityBindingV01,
)
from .paper_runtime_observability_v0_1 import (
    MODEL_FINGERPRINT as OBSERVABILITY_FINGERPRINT,
    MODEL_ID as OBSERVABILITY_MODEL_ID,
    PositionMarkObservation,
    StrategyEvaluationAuditInput,
    TerminalDecisionAuditInput,
    TradeOrSkip,
    build_report,
    canonical_report_digest,
)
from .paper_trade_accounting_v0_1 import load_completed_trades, validate_source_schema
from .runtime_exit_price_impact_v0_1 import (
    MODEL_FINGERPRINT as EXIT_IMPACT_FINGERPRINT,
    MODEL_ID as EXIT_IMPACT_MODEL_ID,
)
from .runtime_price_impact_v0_1 import (
    MODEL_FINGERPRINT as ENTRY_IMPACT_FINGERPRINT,
    MODEL_ID as ENTRY_IMPACT_MODEL_ID,
    RuntimePriceImpactV01,
)


SCHEMA_VERSION = "phase4_continuous_paper_runner_v0.1"
ENGINE_VERSION = "ContinuousPaperRunnerV01"
MODEL_ID = "P4-CONTINUOUS-PAPER-RUNNER-0001"

LOCKED_STRATEGY_BINDING_ID = "FIRST-PULLBACK-V1.1-EXP-0005-D-LOCKED-ROLES"
LOCKED_STRATEGY_SELECTION_SHA256 = (
    "408657c1d6dc39435b61e01b368dac69796481864b7462efd150a2d4e1b0daa1"
)
LOCKED_ROLE_ORDER = ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3")
LOCKED_PARAMETER_SET_BY_ROLE = {
    "CONTROL": "FP1-EXP0005-D-005-CONTROL-R500-T2-U1-PD1000-9000-RB200-BU1-FL0-RC200-RW10000",
    "ROBUST_1": "FP1-EXP0005-D-026-ROBUST_1-R1500-T5-U9-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
    "ROBUST_2": "FP1-EXP0005-D-050-ROBUST_2-R2000-T3-U5-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
    "ROBUST_3": "FP1-EXP0005-D-074-ROBUST_3-R5000-T3-U2-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
}
LOCKED_TRACKS = ("FINAL-A", "FINAL-B", "SENS-C")

_SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "strategy_binding_id": LOCKED_STRATEGY_BINDING_ID,
    "strategy_selection_sha256": LOCKED_STRATEGY_SELECTION_SHA256,
    "strategy_role_order": LOCKED_ROLE_ORDER,
    "strategy_parameter_sets": LOCKED_PARAMETER_SET_BY_ROLE,
    "cost_baseline_id": "P4-COST-BASELINE-0001",
    "cost_fingerprint": "9652aeffc792eb48d62348360c6ae804f660e43329195d1f169219b80ccab67c",
    "entry_impact_model_id": "P4-RUNTIME-PRICE-IMPACT-0001",
    "entry_impact_fingerprint": "30d8880aa58d56f70f3baf235f4973a80c94b59724a5ee286b67f60be924d9e1",
    "exit_spec_fingerprint": "0bde5378f4ff2b63c6a96ad64140840ef8b2770c3be772a7aa71f8bdeb070129",
    "exit_impact_model_id": "P4-RUNTIME-EXIT-PRICE-IMPACT-0001",
    "exit_impact_fingerprint": "e1f1fd1c786cc679cf54f45c16b5d9a0c6aff37ed4132ac63c8abc5506aea59d",
    "observability_model_id": "P4-RUNTIME-OBSERVABILITY-0001",
    "observability_fingerprint": "0c16799f2587957ea8b73aaa919d5213f15426a306cd44d58b10d4e08637ee43",
    "live_observability_binding_id": "P4-LIVE-OBSERVABILITY-BINDING-0002",
    "locked_tracks": LOCKED_TRACKS,
    "cross_track_aggregation": "PROHIBITED",
    "source_delivery": (
        "AT_LEAST_ONCE_MONOTONIC_SPARSE_CURSOR_AFTER_DURABLE_PROCESSING_"
        "FAIL_CLOSED_LOWER_CURSOR"
    ),
    "clock_tick_semantics": "EXPLICIT_TIMEZONE_AWARE_CLOCK_TICKS_REPLAY_STABLE",
}
RUNNER_SPEC_FINGERPRINT = hashlib.sha256(
    json.dumps(_SPEC, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "utf-8"
    )
).hexdigest()


class RuntimeBindingConflict(RuntimeError):
    pass


class SourceCursorConflict(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CandidateEvaluationEvent:
    evaluation: StrategyEvaluationAuditInput
    candidate: CandidateSignalEnvelope

    def __post_init__(self) -> None:
        ev = self.evaluation
        candidate = self.candidate
        if ev.candidate_signal_id != candidate.candidate_id:
            raise ValueError("evaluation/candidate identity mismatch")
        if ev.mint != candidate.mint:
            raise ValueError("evaluation/candidate mint mismatch")
        if ev.strategy_version != candidate.strategy_version:
            raise ValueError("evaluation/candidate strategy version mismatch")
        if ev.parameter_set_id != candidate.parameter_set_id:
            raise ValueError("evaluation/candidate parameter set mismatch")
        if ev.role not in LOCKED_ROLE_ORDER:
            raise ValueError(f"unsupported locked strategy role: {ev.role}")
        if ev.strategy_version != "v1.1":
            raise ValueError("only locked FirstPullback strategy version v1.1 is allowed")
        if ev.parameter_set_id != LOCKED_PARAMETER_SET_BY_ROLE[ev.role]:
            raise ValueError("strategy role/parameter-set binding mismatch")
        if ev.signal_type != "FIRST_PULLBACK":
            raise ValueError("candidate must use the locked FIRST_PULLBACK signal type")


@dataclass(frozen=True, slots=True)
class TerminalSkipEvaluationEvent:
    evaluation: StrategyEvaluationAuditInput

    def __post_init__(self) -> None:
        if self.evaluation.candidate_signal_id is not None:
            raise ValueError("terminal skip evaluation cannot contain a candidate")
        if self.evaluation.role not in LOCKED_ROLE_ORDER:
            raise ValueError(
                f"unsupported locked strategy role: {self.evaluation.role}"
            )
        if self.evaluation.strategy_version != "v1.1":
            raise ValueError("only locked FirstPullback strategy version v1.1 is allowed")
        if (
            self.evaluation.parameter_set_id
            != LOCKED_PARAMETER_SET_BY_ROLE[self.evaluation.role]
        ):
            raise ValueError("strategy role/parameter-set binding mismatch")


@dataclass(frozen=True, slots=True)
class MarketObservationEvent:
    mint: str
    observed_at: datetime
    ingest_seq: int
    price_identity: str
    price_numerator_raw: int
    price_denominator_raw: int
    current_virtual_token_reserve_raw: int
    is_gap_recovery: bool = False

    def __post_init__(self) -> None:
        if not self.mint or not self.price_identity:
            raise ValueError("mint and price_identity are required")
        _require_aware(self.observed_at)
        if self.ingest_seq < 0:
            raise ValueError("ingest_seq must be >= 0")
        if self.price_numerator_raw <= 0 or self.price_denominator_raw <= 0:
            raise ValueError("market price numerator/denominator must be > 0")
        if self.current_virtual_token_reserve_raw <= 0:
            raise ValueError("current_virtual_token_reserve_raw must be > 0")


@dataclass(frozen=True, slots=True)
class ClockTickEvent:
    now: datetime

    def __post_init__(self) -> None:
        _require_aware(self.now)


RunnerEvent: TypeAlias = (
    CandidateEvaluationEvent
    | TerminalSkipEvaluationEvent
    | MarketObservationEvent
    | ClockTickEvent
)


@dataclass(frozen=True, slots=True)
class ContinuousSourceItem:
    cursor: int
    event_key: str
    event: RunnerEvent

    def __post_init__(self) -> None:
        if self.cursor < 0:
            raise ValueError("source cursor must be >= 0")
        if not self.event_key:
            raise ValueError("source event_key is required")

    @property
    def content_fingerprint(self) -> str:
        return _fingerprint(
            {
                "cursor": self.cursor,
                "event_key": self.event_key,
                "event_type": type(self.event).__name__,
                "event": _primitive(self.event),
            }
        )


class ContinuousEventSource(Protocol):
    source_identity: str

    def __iter__(self) -> Iterator[ContinuousSourceItem]: ...


@dataclass(frozen=True, slots=True)
class ProcessResult:
    cursor: int
    event_key: str
    status: str


FailureInjector: TypeAlias = Callable[[ContinuousSourceItem, str], None]
WallClock: TypeAlias = Callable[[], datetime]


class ContinuousPaperRunnerV01:
    """Restart-safe paper-only orchestration over the accepted Phase-4 stack.

    The injected source owns normalization and the already accepted
    FirstPullback role-order arbitration. It may emit the first candidate from
    CONTROL / ROBUST_1 / ROBUST_2 / ROBUST_3 for a source observation, exactly
    as the Phase-4.5C runner did. This core does not select a new winner.

    Lifecycle authority is always reconstructed from SQLite. No in-memory map
    is used to decide whether an entry, position, intent, or fill already exists.
    """

    model_id = MODEL_ID
    spec_fingerprint = RUNNER_SPEC_FINGERPRINT
    schema_version = SCHEMA_VERSION

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        source_identity: str,
        wall_clock: WallClock | None = None,
        failure_injector: FailureInjector | None = None,
    ) -> None:
        if not source_identity:
            raise ValueError("source_identity is required")
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.source_identity = source_identity
        self.wall_clock = wall_clock or (lambda: datetime.now(timezone.utc))
        self.failure_injector = failure_injector

        _verify_locked_contracts()
        self.cost_model = PaperCostModelV02(P4_COST_BASELINE_0001)
        self.router = PaperEntryRouterV01(conn, self.cost_model)
        self.exit_bridge = PaperExitLifecycleBridgeV02(
            conn, lifecycle=self.router.lifecycle
        )
        self.exit_executor = PaperExitFillExecutorV01(
            conn,
            self.cost_model,
            lifecycle=self.router.lifecycle,
            exit_orchestrator=self.exit_bridge.exit_orchestrator,
        )
        self.observability = PaperLiveObservabilityBindingV01(
            conn, self.cost_model
        )
        validate_source_schema(conn)
        _create_schema(conn)
        self._bind_runtime_state()
        self._rebind_persisted_positions()

    @property
    def last_committed_source_cursor(self) -> int:
        row = self._runtime_row()
        return int(row["last_committed_source_cursor"])

    def run(self, source: ContinuousEventSource) -> Iterator[ProcessResult]:
        if source.source_identity != self.source_identity:
            raise RuntimeBindingConflict(
                "event source identity differs from persisted runtime identity"
            )

        def results() -> Iterator[ProcessResult]:
            for item in source:
                yield self.process_source_item(item)

        return results()

    def process_source_item(self, item: ContinuousSourceItem) -> ProcessResult:
        # Validate and bind source content exactly once before any durable
        # dispatch. This makes malformed/non-finite source content side-effect
        # free and commits the same fingerprint that guarded processing.
        content_fingerprint = item.content_fingerprint
        state = self._runtime_row()
        committed = int(state["last_committed_source_cursor"])
        if item.cursor < committed:
            raise SourceCursorConflict(
                "source cursor regressed below the last committed cursor; "
                "historical processing cannot be proven without a durable ledger"
            )
        if item.cursor == committed:
            persisted = state["last_source_item_fingerprint"]
            if persisted != content_fingerprint:
                raise SourceCursorConflict(
                    "committed source cursor replayed with different content"
                )
            return ProcessResult(item.cursor, item.event_key, "ALREADY_COMMITTED")

        self._dispatch(item, content_fingerprint)
        if self.failure_injector is not None:
            self.failure_injector(
                item, "after_event_processing_before_cursor_commit"
            )
        now = _dt_text(self._now())
        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_continuous_runtime_state_v0_1
                SET last_committed_source_cursor=?, last_source_event_key=?,
                    last_source_item_fingerprint=?, updated_at=?
                WHERE singleton=1
                """,
                (item.cursor, item.event_key, content_fingerprint, now),
            )
        return ProcessResult(item.cursor, item.event_key, "COMMITTED")

    def canonical_digest(self) -> str:
        runtime = [dict(r) for r in self.conn.execute(
            "SELECT * FROM paper_continuous_runtime_state_v0_1 ORDER BY singleton"
        ).fetchall()]
        signals = [dict(r) for r in self.conn.execute(
            "SELECT * FROM paper_continuous_signal_contexts_v0_1 ORDER BY signal_key"
        ).fetchall()]
        trades = [_primitive(t) for t in load_completed_trades(self.conn)]
        report = build_report(self.conn)
        payload = {
            "model_id": MODEL_ID,
            "spec_fingerprint": RUNNER_SPEC_FINGERPRINT,
            "runtime": runtime,
            "signal_contexts": signals,
            "entry_digest": canonical_entry_router_digest(self.conn),
            "exit_bridge_digest": self.exit_bridge.canonical_digest(),
            "exit_executor_digest": self.exit_executor.canonical_digest(),
            "observability_digest": canonical_report_digest(report),
            "completed_trades": trades,
        }
        return _fingerprint(payload)

    def _dispatch(
        self, item: ContinuousSourceItem, content_fingerprint: str
    ) -> None:
        event = item.event
        if isinstance(event, CandidateEvaluationEvent):
            self._process_candidate(item, event, content_fingerprint)
        elif isinstance(event, TerminalSkipEvaluationEvent):
            self._process_terminal_skip(event)
        elif isinstance(event, MarketObservationEvent):
            self._process_market(item, event)
        elif isinstance(event, ClockTickEvent):
            self._process_clock(event)
        else:  # pragma: no cover - fail closed for future unbound event types.
            raise TypeError(f"unsupported continuous runner event: {type(event)!r}")

    def _process_candidate(
        self,
        item: ContinuousSourceItem,
        event: CandidateEvaluationEvent,
        content_fingerprint: str,
    ) -> None:
        normalized_evaluation = _normalize_strategy_evaluation(event.evaluation)
        evaluation_id = self.observability.store.record_strategy_evaluation(
            normalized_evaluation
        )
        candidate = event.candidate
        if candidate.price_identity != "SOL_NATIVE":
            self.observability.store.record_terminal_decision(
                _decision_from_evaluation(
                    normalized_evaluation,
                    source_evaluation_id=evaluation_id,
                    trade_or_skip=TradeOrSkip.SKIP,
                    decision_reason="QUOTE_INPUT_CONVERSION_UNAVAILABLE",
                    candidate_signal_id=candidate.candidate_id,
                    paper_entry_route_id=None,
                    decision_at_us=normalized_evaluation.evaluated_at_us,
                )
            )
            return

        routed = self.router.route_candidate(candidate)
        self._persist_signal_context(
            item=item,
            event=event,
            evaluation_id=evaluation_id,
            route_id=routed.route.route_id,
            content_fingerprint=content_fingerprint,
        )

    def _process_terminal_skip(
        self, event: TerminalSkipEvaluationEvent
    ) -> None:
        normalized_evaluation = _normalize_strategy_evaluation(event.evaluation)
        evaluation_id = self.observability.store.record_strategy_evaluation(
            normalized_evaluation
        )
        self.observability.store.record_terminal_decision(
            _decision_from_evaluation(
                normalized_evaluation,
                source_evaluation_id=evaluation_id,
                trade_or_skip=TradeOrSkip.SKIP,
                decision_reason=normalized_evaluation.reason_code,
                candidate_signal_id=None,
                paper_entry_route_id=None,
                decision_at_us=normalized_evaluation.evaluated_at_us,
            )
        )

    def _process_market(
        self,
        item: ContinuousSourceItem,
        event: MarketObservationEvent,
    ) -> None:
        entry_observation = EntryMarketObservation(
            mint=event.mint,
            observed_at=event.observed_at,
            ingest_seq=event.ingest_seq,
            price_identity=event.price_identity,
            price_numerator_raw=event.price_numerator_raw,
            price_denominator_raw=event.price_denominator_raw,
            is_gap_recovery=event.is_gap_recovery,
            source_event_key=item.event_key,
        )

        pending = [
            route
            for route in self.router.list_routes()
            if route.mint == event.mint
            and route.state is EntryRouteState.ENTRY_PENDING
        ]
        for route in pending:
            if event.ingest_seq <= route.signal_ingest_seq:
                continue
            candidate = _candidate_from_route(route)
            impact = RuntimePriceImpactV01.for_entry(
                price_identity=candidate.price_identity,
                requested_size_lamports=route.requested_size_lamports,
                virtual_sol_reserve_lamports=event.price_numerator_raw,
            )
            if not impact.available or impact.router_price_impact_bps is None:
                raise RuntimeError(
                    f"entry impact unavailable for {route.route_id}: {impact.reason_code}"
                )
            self.router.on_market_observation(
                candidate=candidate,
                observation=entry_observation,
                price_impact_bps=int(impact.router_price_impact_bps),
                observed_priority_fee_lamports=None,
                venue_fee_bps=None,
            )

        # Heal the durable route -> audit boundary after any crash between those
        # existing idempotent writes and the runner cursor commit.
        for route in self.router.list_routes():
            if route.mint != event.mint:
                continue
            if route.state not in (EntryRouteState.FILLED, EntryRouteState.REJECTED):
                continue
            if (
                route.selected_market_ingest_seq == event.ingest_seq
                and route.selected_source_event_key == item.event_key
            ):
                self._ensure_entry_terminal_decision(route)

        signal_keys = self._active_signal_keys(event.mint)
        if not signal_keys:
            return

        observable = not event.is_gap_recovery and event.price_identity == "SOL_NATIVE"
        if observable:
            self.observability.record_position_marks_for_market(
                PositionMarkObservation(
                    mint=event.mint,
                    observed_at=event.observed_at,
                    ingest_seq=event.ingest_seq,
                    source_event_key=item.event_key,
                    price_identity=event.price_identity,
                    price_numerator_raw=event.price_numerator_raw,
                    price_denominator_raw=event.price_denominator_raw,
                    current_virtual_token_reserve_raw=(
                        event.current_virtual_token_reserve_raw
                    ),
                    is_gap_recovery=False,
                )
            )

        exit_observation = ExitMarketObservation(
            mint=event.mint,
            observed_at=event.observed_at,
            ingest_seq=event.ingest_seq,
            price_identity=event.price_identity,
            price_numerator_raw=event.price_numerator_raw,
            price_denominator_raw=event.price_denominator_raw,
            is_gap_recovery=event.is_gap_recovery,
            source_event_key=item.event_key,
        )
        execution_observation = ExitExecutionMarketObservation(
            mint=event.mint,
            observed_at=event.observed_at,
            ingest_seq=event.ingest_seq,
            price_identity=event.price_identity,
            price_numerator_raw=event.price_numerator_raw,
            price_denominator_raw=event.price_denominator_raw,
            current_virtual_token_reserve_raw=(
                event.current_virtual_token_reserve_raw
            ),
            is_gap_recovery=event.is_gap_recovery,
            source_event_key=item.event_key,
        )
        for signal_key in signal_keys:
            result = self.exit_bridge.on_market_observation(
                signal_key, exit_observation
            )
            for position in result.positions:
                if position.state in (PositionState.EXIT_PENDING, PositionState.CLOSED):
                    self.exit_executor.bind_position(position.paper_position_id)
                if position.state is PositionState.EXIT_PENDING:
                    self.exit_executor.on_market_observation(
                        paper_position_id=position.paper_position_id,
                        observation=execution_observation,
                        observed_priority_fee_lamports=None,
                        venue_fee_bps=None,
                    )

        if observable:
            self.observability.record_track_equity_for_market(
                observed_at=event.observed_at,
                ingest_seq=event.ingest_seq,
                source_event_key=item.event_key,
            )

    def _process_clock(self, event: ClockTickEvent) -> None:
        now = _utc(event.now)
        for signal_key in self._active_signal_keys():
            result = self.exit_bridge.on_clock(signal_key, now)
            for position in result.positions:
                if position.state in (PositionState.EXIT_PENDING, PositionState.CLOSED):
                    self.exit_executor.bind_position(position.paper_position_id)

    def _ensure_entry_terminal_decision(self, route: EntryRoute) -> None:
        context = self.conn.execute(
            "SELECT * FROM paper_continuous_signal_contexts_v0_1 WHERE signal_key=?",
            (route.candidate_id,),
        ).fetchone()
        if context is None:
            raise RuntimeBindingConflict(
                f"terminal route {route.route_id} has no continuous signal context"
            )
        if route.selected_market_observed_at is None:
            raise RuntimeBindingConflict("terminal entry route has no selected timestamp")
        trade_or_skip = (
            TradeOrSkip.TRADE
            if route.state is EntryRouteState.FILLED
            else TradeOrSkip.SKIP
        )
        reason = (
            "ENTRY_FILLED"
            if trade_or_skip is TradeOrSkip.TRADE
            else "ENTRY_REJECTED_SLIPPAGE"
        )
        self.observability.store.record_terminal_decision(
            TerminalDecisionAuditInput(
                decision_key=str(context["decision_key"]),
                decision_scope="ENTRY_ROLE",
                run_id=str(context["run_id"]),
                role=str(context["role"]),
                mint=str(context["mint"]),
                strategy_version=str(context["strategy_version"]),
                parameter_set_id=str(context["parameter_set_id"]),
                decision_at_us=_datetime_to_us(route.selected_market_observed_at),
                trade_or_skip=trade_or_skip,
                decision_reason=reason,
                candidate_signal_id=route.candidate_id,
                paper_entry_route_id=route.route_id,
                source_evaluation_id=str(context["source_evaluation_id"]),
            )
        )
        if route.state is EntryRouteState.FILLED:
            self.exit_bridge.bind_signal(route.candidate_id)

    def _persist_signal_context(
        self,
        *,
        item: ContinuousSourceItem,
        event: CandidateEvaluationEvent,
        evaluation_id: str,
        route_id: str,
        content_fingerprint: str,
    ) -> None:
        ev = event.evaluation
        now = _dt_text(event.candidate.signal_observed_at)
        values = (
            event.candidate.candidate_id,
            route_id,
            evaluation_id,
            f"ENTRY-RUN:{ev.run_id}:{ev.role}",
            ev.run_id,
            ev.role,
            ev.mint,
            ev.strategy_version,
            ev.parameter_set_id,
            item.cursor,
            item.event_key,
            content_fingerprint,
            now,
            now,
        )
        existing = self.conn.execute(
            "SELECT * FROM paper_continuous_signal_contexts_v0_1 WHERE signal_key=?",
            (event.candidate.candidate_id,),
        ).fetchone()
        if existing is None:
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO paper_continuous_signal_contexts_v0_1(
                        signal_key, route_id, source_evaluation_id, decision_key,
                        run_id, role, mint, strategy_version, parameter_set_id,
                        source_cursor, source_event_key, content_fingerprint,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )
            return
        actual = tuple(existing[key] for key in (
            "signal_key", "route_id", "source_evaluation_id", "decision_key",
            "run_id", "role", "mint", "strategy_version", "parameter_set_id",
            "source_cursor", "source_event_key", "content_fingerprint",
            "created_at", "updated_at",
        ))
        if actual != values:
            raise RuntimeBindingConflict(
                "replayed candidate differs from persisted continuous signal context"
            )

    def _active_signal_keys(self, mint: str | None = None) -> tuple[str, ...]:
        sql = (
            "SELECT DISTINCT signal_key FROM paper_positions "
            "WHERE state IN ('OPEN','EXIT_PENDING')"
        )
        params: tuple[object, ...] = ()
        if mint is not None:
            sql += " AND mint=?"
            params = (mint,)
        sql += " ORDER BY signal_key"
        return tuple(
            str(row["signal_key"])
            for row in self.conn.execute(sql, params).fetchall()
        )

    def _rebind_persisted_positions(self) -> None:
        signal_keys = tuple(
            str(row["signal_key"])
            for row in self.conn.execute(
                "SELECT DISTINCT signal_key FROM paper_positions ORDER BY signal_key"
            ).fetchall()
        )
        for signal_key in signal_keys:
            refs = self.exit_bridge.bind_signal(signal_key)
            for ref in refs:
                position = self.router.lifecycle.get_position(ref.paper_position_id)
                if position is None:
                    raise RuntimeBindingConflict(
                        f"persisted exit ref has no position: {ref.paper_position_id}"
                    )
                if position.state in (PositionState.EXIT_PENDING, PositionState.CLOSED):
                    self.exit_executor.bind_position(position.paper_position_id)

    def _bind_runtime_state(self) -> None:
        row = self.conn.execute(
            "SELECT * FROM paper_continuous_runtime_state_v0_1 WHERE singleton=1"
        ).fetchone()
        if row is None:
            now = _dt_text(self._now())
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO paper_continuous_runtime_state_v0_1(
                        singleton, runner_model_id, runner_spec_fingerprint,
                        source_identity, last_committed_source_cursor,
                        last_source_event_key, last_source_item_fingerprint,
                        created_at, updated_at
                    ) VALUES (1, ?, ?, ?, -1, NULL, NULL, ?, ?)
                    """,
                    (MODEL_ID, RUNNER_SPEC_FINGERPRINT, self.source_identity, now, now),
                )
            return
        actual = (
            str(row["runner_model_id"]),
            str(row["runner_spec_fingerprint"]),
            str(row["source_identity"]),
        )
        expected = (MODEL_ID, RUNNER_SPEC_FINGERPRINT, self.source_identity)
        if actual != expected:
            raise RuntimeBindingConflict(
                f"persisted runner identity conflict: actual={actual!r} expected={expected!r}"
            )

    def _runtime_row(self) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM paper_continuous_runtime_state_v0_1 WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise RuntimeBindingConflict("continuous runtime state is missing")
        return row

    def _now(self) -> datetime:
        return _utc(self.wall_clock())


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_continuous_runtime_state_v0_1 (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            runner_model_id TEXT NOT NULL,
            runner_spec_fingerprint TEXT NOT NULL,
            source_identity TEXT NOT NULL,
            last_committed_source_cursor INTEGER NOT NULL,
            last_source_event_key TEXT,
            last_source_item_fingerprint TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_continuous_signal_contexts_v0_1 (
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
        )
        """
    )
    conn.commit()


def _verify_locked_contracts() -> None:
    baseline = P4_COST_BASELINE_0001
    checks = {
        "cost_baseline_id": baseline.assumption_set_id == _SPEC["cost_baseline_id"],
        "cost_fingerprint": baseline.fingerprint == _SPEC["cost_fingerprint"],
        "reference_size": baseline.reference_position_lamports == 100_000_000,
        "entry_latency": baseline.entry_latency_ms == 500,
        "exit_latency": baseline.exit_latency_ms == 500,
        "entry_slippage_cap": baseline.entry_slippage_cap_bps == 1500,
        "exit_slippage_cap": baseline.exit_slippage_cap_bps == 2000,
        "entry_impact_id": ENTRY_IMPACT_MODEL_ID == _SPEC["entry_impact_model_id"],
        "entry_impact_fingerprint": ENTRY_IMPACT_FINGERPRINT == _SPEC["entry_impact_fingerprint"],
        "exit_spec_fingerprint": LOCKED_EXIT_SPEC_FINGERPRINT == _SPEC["exit_spec_fingerprint"],
        "exit_impact_id": EXIT_IMPACT_MODEL_ID == _SPEC["exit_impact_model_id"],
        "exit_impact_fingerprint": EXIT_IMPACT_FINGERPRINT == _SPEC["exit_impact_fingerprint"],
        "observability_id": OBSERVABILITY_MODEL_ID == _SPEC["observability_model_id"],
        "observability_fingerprint": OBSERVABILITY_FINGERPRINT == _SPEC["observability_fingerprint"],
        "live_observability_binding": LIVE_OBSERVABILITY_BINDING_ID == _SPEC["live_observability_binding_id"],
        "track_order": tuple(LOCKED_TRACK_ORDER) == LOCKED_TRACKS,
    }
    failed = tuple(key for key, ok in checks.items() if not ok)
    if failed:
        raise RuntimeBindingConflict(f"locked Phase-4 contract drift: {failed}")


def _candidate_from_route(route: EntryRoute) -> CandidateSignalEnvelope:
    return CandidateSignalEnvelope(
        candidate_id=route.candidate_id,
        mint=route.mint,
        strategy_version=route.strategy_version,
        parameter_set_id=route.parameter_set_id,
        signal_observed_at=route.signal_observed_at,
        signal_ingest_seq=route.signal_ingest_seq,
        price_identity=route.reference_price_identity,
        price_numerator_raw=route.reference_price_numerator_raw,
        price_denominator_raw=route.reference_price_denominator_raw,
    )


def _decision_from_evaluation(
    evaluation: StrategyEvaluationAuditInput,
    *,
    source_evaluation_id: str,
    trade_or_skip: TradeOrSkip,
    decision_reason: str,
    candidate_signal_id: str | None,
    paper_entry_route_id: str | None,
    decision_at_us: int,
) -> TerminalDecisionAuditInput:
    return TerminalDecisionAuditInput(
        decision_key=f"ENTRY-RUN:{evaluation.run_id}:{evaluation.role}",
        decision_scope="ENTRY_ROLE",
        run_id=evaluation.run_id,
        role=evaluation.role,
        mint=evaluation.mint,
        strategy_version=evaluation.strategy_version,
        parameter_set_id=evaluation.parameter_set_id,
        decision_at_us=decision_at_us,
        trade_or_skip=trade_or_skip,
        decision_reason=decision_reason,
        candidate_signal_id=candidate_signal_id,
        paper_entry_route_id=paper_entry_route_id,
        source_evaluation_id=source_evaluation_id,
    )


def _normalize_strategy_evaluation(
    evaluation: StrategyEvaluationAuditInput,
) -> StrategyEvaluationAuditInput:
    snapshot = _primitive(evaluation.audit_snapshot)
    if not isinstance(snapshot, dict):
        raise ValueError("strategy evaluation audit_snapshot must normalize to a dict")
    return replace(evaluation, audit_snapshot=snapshot)


def _primitive(value: Any) -> Any:
    if is_dataclass(value):
        return _primitive(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Decimal source values must be finite")
        return str(value)
    if isinstance(value, datetime):
        return _dt_text(value)
    if isinstance(value, dict):
        return {str(k): _primitive(v) for k, v in value.items()}
    if isinstance(value, (tuple, list, set)):
        return [_primitive(v) for v in value]
    return value


def _fingerprint(value: Any) -> str:
    body = json.dumps(
        _primitive(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")


def _utc(value: datetime) -> datetime:
    _require_aware(value)
    return value.astimezone(timezone.utc)


def _dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _datetime_to_us(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = _utc(value) - epoch
    return (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )
