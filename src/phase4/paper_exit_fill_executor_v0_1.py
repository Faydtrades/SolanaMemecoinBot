from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from fractions import Fraction
from typing import Any, Iterable

from .paper_cost_model_v0_2 import (
    CostSide,
    ExplicitCostBreakdown,
    FillDecision,
    FillEvaluation,
    PaperCostModelV02,
    RationalPrice,
)
from .paper_exit_orchestrator_v0_1 import (
    ExitIntent,
    ExitPositionRef,
    ExitReason,
    PaperExitOrchestratorV01,
)
from .paper_lifecycle_v0_1 import (
    DeterminismConflict as LifecycleDeterminismConflict,
    PaperLifecycleStore,
    PaperPosition,
    PositionState,
)
from .runtime_exit_price_impact_v0_1 import (
    MODEL_FINGERPRINT as EXIT_IMPACT_FINGERPRINT,
    MODEL_ID as EXIT_IMPACT_MODEL_ID,
    RuntimeExitPriceImpact,
    RuntimeExitPriceImpactV01,
)


SCHEMA_VERSION = "phase4_paper_exit_fill_executor_v0.1"
ENGINE_VERSION = "PaperExitFillExecutorV01"


class ExitExecutionState(StrEnum):
    WAITING = "WAITING"
    FILL_SELECTED = "FILL_SELECTED"
    FILLED = "FILLED"


class ExitReferenceSource(StrEnum):
    TRIGGER_MARKET = "TRIGGER_MARKET"
    LAST_FRESH_MARKET = "LAST_FRESH_MARKET"
    RULE_REFERENCE_FALLBACK = "RULE_REFERENCE_FALLBACK"


@dataclass(frozen=True, slots=True)
class ExitExecutionMarketObservation:
    mint: str
    observed_at: datetime
    ingest_seq: int
    price_identity: str
    price_numerator_raw: int
    price_denominator_raw: int
    current_virtual_token_reserve_raw: int
    is_gap_recovery: bool = False
    source_event_key: str | None = None

    def __post_init__(self) -> None:
        if not self.mint:
            raise ValueError("mint is required")
        _require_aware(self.observed_at)
        if self.ingest_seq < 0:
            raise ValueError("ingest_seq must be >= 0")
        if not self.price_identity:
            raise ValueError("price_identity is required")
        if self.price_numerator_raw <= 0 or self.price_denominator_raw <= 0:
            raise ValueError("price numerator/denominator must be > 0")
        if self.current_virtual_token_reserve_raw <= 0:
            raise ValueError("current_virtual_token_reserve_raw must be > 0")

    @property
    def price(self) -> RationalPrice:
        return RationalPrice(
            self.price_identity,
            self.price_numerator_raw,
            self.price_denominator_raw,
        )


@dataclass(frozen=True, slots=True)
class ExitExecutionRoute:
    route_id: str
    exit_intent_id: str
    paper_position_id: str
    paper_order_id: str
    signal_key: str
    mint: str
    track_id: str
    exit_variant: str
    exit_reason: str
    price_identity: str
    assumption_set_id: str
    cost_model_fingerprint: str
    exit_impact_model_id: str
    exit_impact_model_fingerprint: str
    requested_at: datetime
    execution_ready_at: datetime
    reference_source: ExitReferenceSource
    reference_observed_at: datetime
    reference_ingest_seq: int
    reference_price_numerator_raw: int
    reference_price_denominator_raw: int
    state: ExitExecutionState
    state_reason: str
    created_at: datetime
    updated_at: datetime
    selected_attempt_id: str | None
    selected_market_observed_at: datetime | None
    selected_market_ingest_seq: int | None
    selected_source_event_key: str | None
    derived_token_input_raw: int | None
    price_impact_bps: int | None
    signed_market_move_bps: int | None
    adverse_slippage_bps: int | None
    slippage_cap_bps: int | None
    simulated_exit_price_numerator_raw: int | None
    simulated_exit_price_denominator_raw: int | None
    gross_exit_proceeds_lamports: int | None
    total_exit_explicit_cost_lamports: int | None


@dataclass(frozen=True, slots=True)
class ExitExecutionAttempt:
    attempt_id: str
    route_id: str
    paper_position_id: str
    market_observed_at: datetime
    market_ingest_seq: int
    source_event_key: str | None
    observation_fingerprint: str
    market_price_numerator_raw: int
    market_price_denominator_raw: int
    current_virtual_token_reserve_raw: int
    derived_token_input_raw: int
    price_impact_bps: int
    signed_market_move_bps: int
    adverse_slippage_bps: int
    slippage_cap_bps: int
    decision: FillDecision
    rejection_reason: str | None
    simulated_exit_price_numerator_raw: int
    simulated_exit_price_denominator_raw: int
    gross_exit_proceeds_lamports: int | None
    total_exit_explicit_cost_lamports: int | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ExitExecutionResult:
    route: ExitExecutionRoute
    position: PaperPosition
    attempt: ExitExecutionAttempt | None
    fill_evaluation: FillEvaluation | None
    price_impact: RuntimeExitPriceImpact | None
    explicit_costs: ExplicitCostBreakdown | None
    ignored_reason: str | None


class ExitExecutionBindingError(RuntimeError):
    pass


class ExitExecutionDeterminismConflict(RuntimeError):
    pass


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")


def _utc(value: datetime) -> datetime:
    _require_aware(value)
    return value.astimezone(timezone.utc)


def _dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _dt_parse(value: str | None) -> datetime | None:
    if value is None:
        return None
    return _utc(datetime.fromisoformat(value))


def _hash_id(prefix: str, *parts: object) -> str:
    body = "\x1f".join(str(p) for p in parts)
    return f"{prefix}-{hashlib.sha256(body.encode('utf-8')).hexdigest()[:32]}"


def _obs_key(observed_at: datetime, ingest_seq: int) -> tuple[datetime, int]:
    return (_utc(observed_at), int(ingest_seq))


def _observation_fingerprint(obs: ExitExecutionMarketObservation) -> str:
    payload = {
        "mint": obs.mint,
        "observed_at": _dt_text(obs.observed_at),
        "ingest_seq": obs.ingest_seq,
        "price_identity": obs.price_identity,
        "price_numerator_raw": obs.price_numerator_raw,
        "price_denominator_raw": obs.price_denominator_raw,
        "current_virtual_token_reserve_raw": obs.current_virtual_token_reserve_raw,
        "is_gap_recovery": obs.is_gap_recovery,
        "source_event_key": obs.source_event_key,
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _floor_fraction(value: Fraction) -> int:
    if value < 0:
        raise ValueError("value must be >= 0")
    return value.numerator // value.denominator


class PaperExitFillExecutorV01:
    """Persistent causal EXIT_PENDING -> CLOSED simulated exit executor.

    Boundary owned by this layer:

        immutable ExitIntent + lifecycle EXIT_PENDING position
            -> locked 500ms EXIT latency
            -> first qualifying post-ready market observation
            -> runtime SOL_NATIVE exit curve impact
            -> PaperCostModelV02 EXIT slippage decision
            -> rejected attempt (position remains EXIT_PENDING), or
            -> selected fill + lifecycle CLOSED

    Important semantics:
    - no wallet, signing, RPC or live order path exists here;
    - a slippage rejection is audited and does NOT cancel the exit intent;
      later causal observations may retry the still-EXIT_PENDING position;
    - accepted fills are staged as FILL_SELECTED before lifecycle close so a
      restart can recover the narrow cross-table commit boundary;
    - no PnL/expectancy/PF/drawdown calculation is performed here.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        cost_model: PaperCostModelV02,
        *,
        lifecycle: PaperLifecycleStore | None = None,
        exit_orchestrator: PaperExitOrchestratorV01 | None = None,
        impact_provider: type[RuntimeExitPriceImpactV01] = RuntimeExitPriceImpactV01,
    ) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.cost_model = cost_model
        self.lifecycle = lifecycle or PaperLifecycleStore(conn)
        self.exit_orchestrator = exit_orchestrator or PaperExitOrchestratorV01(conn)
        self.impact_provider = impact_provider
        _create_schema(conn)
        self._recover_selected_fills()

    def bind_position(self, paper_position_id: str) -> ExitExecutionRoute:
        position = self.lifecycle.get_position(paper_position_id)
        if position is None:
            raise ExitExecutionBindingError(f"unknown paper_position_id: {paper_position_id}")
        if position.state not in (PositionState.EXIT_PENDING, PositionState.CLOSED):
            raise ExitExecutionBindingError(
                f"position must be EXIT_PENDING/CLOSED before exit execution: {position.state}"
            )

        intent = self.exit_orchestrator.get_intent_for_position(paper_position_id)
        if intent is None:
            raise ExitExecutionBindingError("EXIT_PENDING/CLOSED position has no persisted ExitIntent")

        track = self.exit_orchestrator.get_track(paper_position_id)
        if track is None:
            raise ExitExecutionBindingError("position has no persisted exit track")

        self._assert_lineage(position, intent, track)
        reference_source, ref_at, ref_seq, ref_num, ref_den = self._resolve_reference(
            position=position,
            intent=intent,
            track=track,
        )

        baseline = self.cost_model.baseline
        ready_at = self.cost_model.execution_ready_at(intent.requested_at, CostSide.EXIT)
        route_id = _hash_id(
            "PXR",
            SCHEMA_VERSION,
            intent.exit_intent_id,
            baseline.assumption_set_id,
            baseline.fingerprint,
            EXIT_IMPACT_MODEL_ID,
            EXIT_IMPACT_FINGERPRINT,
        )

        existing = self.get_route(route_id)
        if existing is None:
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO paper_exit_execution_routes(
                        route_id, exit_intent_id, paper_position_id, paper_order_id,
                        signal_key, mint, track_id, exit_variant, exit_reason,
                        price_identity, assumption_set_id, cost_model_fingerprint,
                        exit_impact_model_id, exit_impact_model_fingerprint,
                        requested_at, execution_ready_at,
                        reference_source, reference_observed_at, reference_ingest_seq,
                        reference_price_numerator_raw, reference_price_denominator_raw,
                        state, state_reason, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        route_id,
                        intent.exit_intent_id,
                        position.paper_position_id,
                        position.paper_order_id,
                        position.signal_key,
                        position.mint,
                        position.track_id,
                        position.exit_variant,
                        intent.reason.value,
                        position.price_identity,
                        baseline.assumption_set_id,
                        baseline.fingerprint,
                        EXIT_IMPACT_MODEL_ID,
                        EXIT_IMPACT_FINGERPRINT,
                        _dt_text(intent.requested_at),
                        _dt_text(ready_at),
                        reference_source.value,
                        _dt_text(ref_at),
                        int(ref_seq),
                        str(ref_num),
                        str(ref_den),
                        ExitExecutionState.WAITING.value,
                        "WAITING_FOR_CAUSAL_EXIT_FILL",
                        _dt_text(intent.requested_at),
                        _dt_text(intent.requested_at),
                    ),
                )
        else:
            expected = (
                intent.exit_intent_id,
                position.paper_position_id,
                position.paper_order_id,
                position.signal_key,
                position.mint,
                position.track_id,
                position.exit_variant,
                intent.reason.value,
                position.price_identity,
                baseline.assumption_set_id,
                baseline.fingerprint,
                EXIT_IMPACT_MODEL_ID,
                EXIT_IMPACT_FINGERPRINT,
                _dt_text(intent.requested_at),
                _dt_text(ready_at),
                reference_source.value,
                _dt_text(ref_at),
                int(ref_seq),
                int(ref_num),
                int(ref_den),
            )
            actual = (
                existing.exit_intent_id,
                existing.paper_position_id,
                existing.paper_order_id,
                existing.signal_key,
                existing.mint,
                existing.track_id,
                existing.exit_variant,
                existing.exit_reason,
                existing.price_identity,
                existing.assumption_set_id,
                existing.cost_model_fingerprint,
                existing.exit_impact_model_id,
                existing.exit_impact_model_fingerprint,
                _dt_text(existing.requested_at),
                _dt_text(existing.execution_ready_at),
                existing.reference_source.value,
                _dt_text(existing.reference_observed_at),
                existing.reference_ingest_seq,
                existing.reference_price_numerator_raw,
                existing.reference_price_denominator_raw,
            )
            if actual != expected:
                raise ExitExecutionDeterminismConflict("replayed exit binding differs from persisted route")

        route = self.get_route(route_id)
        assert route is not None
        if route.state is ExitExecutionState.FILL_SELECTED:
            self._finalize_selected_route(route.route_id)
            route = self.get_route(route.route_id)
            assert route is not None
        return route

    def on_market_observation(
        self,
        *,
        paper_position_id: str,
        observation: ExitExecutionMarketObservation,
        observed_priority_fee_lamports: int | None = None,
        venue_fee_bps: int | None = None,
    ) -> ExitExecutionResult:
        route = self.bind_position(paper_position_id)
        position = self.lifecycle.get_position(paper_position_id)
        assert position is not None

        if route.state is ExitExecutionState.FILLED:
            return ExitExecutionResult(route, position, None, None, None, None, "TERMINAL_REPLAY")

        if observation.mint != route.mint:
            return ExitExecutionResult(route, position, None, None, None, None, "CROSS_MINT")
        if observation.is_gap_recovery:
            return ExitExecutionResult(route, position, None, None, None, None, "GAP_RECOVERY")
        if observation.price_identity != route.price_identity:
            return ExitExecutionResult(route, position, None, None, None, None, "PRICE_IDENTITY_MISMATCH")
        if _utc(observation.observed_at) < route.execution_ready_at:
            return ExitExecutionResult(route, position, None, None, None, None, "BEFORE_EXECUTION_READY")
        if _obs_key(observation.observed_at, observation.ingest_seq) <= _obs_key(
            route.reference_observed_at,
            route.reference_ingest_seq,
        ):
            return ExitExecutionResult(route, position, None, None, None, None, "NOT_CAUSALLY_AFTER_REFERENCE")

        fingerprint = _observation_fingerprint(observation)
        replay_attempt = self._attempt_for_key(
            route.route_id,
            observation.observed_at,
            observation.ingest_seq,
        )
        if replay_attempt is not None:
            if replay_attempt.observation_fingerprint != fingerprint:
                raise ExitExecutionDeterminismConflict(
                    "same exit observation key replayed with different content"
                )
            route_now = self.get_route(route.route_id)
            assert route_now is not None
            pos_now = self.lifecycle.get_position(paper_position_id)
            assert pos_now is not None
            return ExitExecutionResult(
                route_now,
                pos_now,
                replay_attempt,
                self._fill_eval_from_attempt(route_now, replay_attempt),
                self._impact_from_attempt(route_now, replay_attempt),
                self._costs_from_attempt(replay_attempt),
                "ATTEMPT_REPLAY",
            )

        last = self._last_attempt(route.route_id)
        if last is not None and _obs_key(observation.observed_at, observation.ingest_seq) < _obs_key(
            last.market_observed_at,
            last.market_ingest_seq,
        ):
            return ExitExecutionResult(route, position, None, None, None, None, "OUT_OF_ORDER_REPLAY")

        impact = self.impact_provider.for_position(
            price_identity=route.price_identity,
            filled_size_lamports=position.filled_size_lamports,
            entry_price_numerator_raw=position.entry_price_numerator_raw,
            entry_price_denominator_raw=position.entry_price_denominator_raw,
            current_virtual_token_reserve_raw=observation.current_virtual_token_reserve_raw,
        )
        if not impact.available or impact.router_price_impact_bps is None or impact.derived_token_input_raw is None:
            return ExitExecutionResult(
                route,
                position,
                None,
                None,
                impact,
                None,
                f"PRICE_IMPACT_UNAVAILABLE:{impact.reason_code}",
            )

        fill_eval = self.cost_model.evaluate_fill(
            side=CostSide.EXIT,
            signal_reference_price=RationalPrice(
                route.price_identity,
                route.reference_price_numerator_raw,
                route.reference_price_denominator_raw,
            ),
            market_price_at_ready=observation.price,
            price_impact_bps=int(impact.router_price_impact_bps),
            signal_observed_at=route.requested_at,
            market_observed_at=observation.observed_at,
        )

        if fill_eval.decision is FillDecision.REJECTED_SLIPPAGE:
            attempt = self._persist_attempt(
                route=route,
                observation=observation,
                fingerprint=fingerprint,
                impact=impact,
                fill_eval=fill_eval,
                gross_exit_proceeds_lamports=None,
                costs=None,
            )
            return ExitExecutionResult(
                self.get_route(route.route_id) or route,
                self.lifecycle.get_position(paper_position_id) or position,
                attempt,
                fill_eval,
                impact,
                None,
                None,
            )

        gross_fraction = Fraction(
            int(impact.derived_token_input_raw) * fill_eval.simulated_execution_price.numerator_raw,
            fill_eval.simulated_execution_price.denominator_raw,
        )
        gross_proceeds = _floor_fraction(gross_fraction)
        if gross_proceeds <= 0:
            raise RuntimeError("simulated exit gross proceeds must be > 0")

        costs = self.cost_model.explicit_costs(
            side=CostSide.EXIT,
            notional_lamports=gross_proceeds,
            observed_priority_fee_lamports=observed_priority_fee_lamports,
            venue_fee_bps=venue_fee_bps,
        )

        attempt = self._persist_attempt(
            route=route,
            observation=observation,
            fingerprint=fingerprint,
            impact=impact,
            fill_eval=fill_eval,
            gross_exit_proceeds_lamports=gross_proceeds,
            costs=costs,
        )
        self._stage_selected_fill(route.route_id, attempt, impact, fill_eval, gross_proceeds, costs)
        self._finalize_selected_route(route.route_id)

        final_route = self.get_route(route.route_id)
        final_position = self.lifecycle.get_position(paper_position_id)
        assert final_route is not None and final_position is not None
        return ExitExecutionResult(
            final_route,
            final_position,
            attempt,
            fill_eval,
            impact,
            costs,
            None,
        )

    def process_observations(
        self,
        *,
        paper_position_id: str,
        observations: Iterable[ExitExecutionMarketObservation],
        observed_priority_fee_lamports: int | None = None,
        venue_fee_bps: int | None = None,
    ) -> ExitExecutionResult:
        route = self.bind_position(paper_position_id)
        position = self.lifecycle.get_position(paper_position_id)
        assert position is not None
        result = ExitExecutionResult(route, position, None, None, None, None, None)
        for obs in sorted(observations, key=lambda x: _obs_key(x.observed_at, x.ingest_seq)):
            result = self.on_market_observation(
                paper_position_id=paper_position_id,
                observation=obs,
                observed_priority_fee_lamports=observed_priority_fee_lamports,
                venue_fee_bps=venue_fee_bps,
            )
            if result.route.state is ExitExecutionState.FILLED:
                break
        return result

    def get_route(self, route_id: str) -> ExitExecutionRoute | None:
        row = self.conn.execute(
            "SELECT * FROM paper_exit_execution_routes WHERE route_id=?",
            (route_id,),
        ).fetchone()
        return None if row is None else _route_from_row(row)

    def get_route_for_position(self, paper_position_id: str) -> ExitExecutionRoute | None:
        row = self.conn.execute(
            "SELECT * FROM paper_exit_execution_routes WHERE paper_position_id=?",
            (paper_position_id,),
        ).fetchone()
        return None if row is None else _route_from_row(row)

    def list_attempts(self, route_id: str) -> tuple[ExitExecutionAttempt, ...]:
        rows = self.conn.execute(
            """
            SELECT * FROM paper_exit_execution_attempts
            WHERE route_id=?
            ORDER BY market_observed_at, market_ingest_seq, attempt_id
            """,
            (route_id,),
        ).fetchall()
        return tuple(_attempt_from_row(r) for r in rows)

    def canonical_digest(self) -> str:
        routes = [dict(r) for r in self.conn.execute(
            "SELECT * FROM paper_exit_execution_routes ORDER BY route_id"
        ).fetchall()]
        attempts = [dict(r) for r in self.conn.execute(
            "SELECT * FROM paper_exit_execution_attempts ORDER BY attempt_id"
        ).fetchall()]
        positions = [dict(r) for r in self.conn.execute(
            "SELECT * FROM paper_positions ORDER BY paper_position_id"
        ).fetchall()]
        events = [dict(r) for r in self.conn.execute(
            "SELECT * FROM paper_lifecycle_events WHERE entity_type='POSITION' ORDER BY event_id"
        ).fetchall()]
        payload = {
            "schema_version": SCHEMA_VERSION,
            "engine_version": ENGINE_VERSION,
            "cost_baseline": {
                "assumption_set_id": self.cost_model.baseline.assumption_set_id,
                "fingerprint": self.cost_model.baseline.fingerprint,
            },
            "exit_impact_model": {
                "model_id": EXIT_IMPACT_MODEL_ID,
                "fingerprint": EXIT_IMPACT_FINGERPRINT,
            },
            "routes": routes,
            "attempts": attempts,
            "positions": positions,
            "position_events": events,
        }
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    def _resolve_reference(
        self,
        *,
        position: PaperPosition,
        intent: ExitIntent,
        track: Any,
    ) -> tuple[ExitReferenceSource, datetime, int, int, int]:
        if intent.reason in (ExitReason.TAKE_PROFIT, ExitReason.TRAIL):
            fields = (
                intent.trigger_observed_at,
                intent.trigger_ingest_seq,
                intent.trigger_price_numerator_raw,
                intent.trigger_price_denominator_raw,
            )
            if any(v is None for v in fields):
                raise ExitExecutionBindingError("market-triggered ExitIntent is missing trigger price lineage")
            return (
                ExitReferenceSource.TRIGGER_MARKET,
                intent.trigger_observed_at,  # type: ignore[arg-type]
                int(intent.trigger_ingest_seq),
                int(intent.trigger_price_numerator_raw),
                int(intent.trigger_price_denominator_raw),
            )

        if intent.reason is ExitReason.FALLBACK:
            if (
                track.last_fresh_at is not None
                and track.last_fresh_ingest_seq is not None
                and track.last_fresh_price_numerator_raw is not None
                and track.last_fresh_price_denominator_raw is not None
            ):
                return (
                    ExitReferenceSource.LAST_FRESH_MARKET,
                    track.last_fresh_at,
                    int(track.last_fresh_ingest_seq),
                    int(track.last_fresh_price_numerator_raw),
                    int(track.last_fresh_price_denominator_raw),
                )

            # Fallback is a timer and deliberately has no synthetic trigger price.
            # If no post-entry fresh mark exists, use the real CandidateSignal
            # rule reference already persisted by Phase 3/entry routing. This is
            # a causal observed price, not a fabricated fallback fill price.
            return (
                ExitReferenceSource.RULE_REFERENCE_FALLBACK,
                track.signal_observed_at,
                int(track.signal_ingest_seq),
                int(track.rule_reference_price_numerator_raw),
                int(track.rule_reference_price_denominator_raw),
            )

        raise ExitExecutionBindingError(f"unsupported exit reason: {intent.reason}")

    def _assert_lineage(self, position: PaperPosition, intent: ExitIntent, track: Any) -> None:
        if (
            intent.paper_position_id != position.paper_position_id
            or intent.paper_order_id != position.paper_order_id
            or intent.signal_key != position.signal_key
            or intent.mint != position.mint
            or intent.track_id != position.track_id
            or intent.exit_variant != position.exit_variant
        ):
            raise ExitExecutionBindingError("ExitIntent/lifecycle position lineage mismatch")
        if (
            track.paper_position_id != position.paper_position_id
            or track.paper_order_id != position.paper_order_id
            or track.signal_key != position.signal_key
            or track.mint != position.mint
            or track.track_id != position.track_id
            or track.exit_variant != position.exit_variant
            or track.price_identity != position.price_identity
        ):
            raise ExitExecutionBindingError("exit track/lifecycle position lineage mismatch")

    def _persist_attempt(
        self,
        *,
        route: ExitExecutionRoute,
        observation: ExitExecutionMarketObservation,
        fingerprint: str,
        impact: RuntimeExitPriceImpact,
        fill_eval: FillEvaluation,
        gross_exit_proceeds_lamports: int | None,
        costs: ExplicitCostBreakdown | None,
    ) -> ExitExecutionAttempt:
        attempt_id = _hash_id(
            "PXA",
            SCHEMA_VERSION,
            route.route_id,
            _dt_text(observation.observed_at),
            observation.ingest_seq,
        )
        existing = self._attempt_for_key(route.route_id, observation.observed_at, observation.ingest_seq)
        if existing is not None:
            if existing.observation_fingerprint != fingerprint:
                raise ExitExecutionDeterminismConflict("attempt key already exists with different observation")
            return existing

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO paper_exit_execution_attempts(
                    attempt_id, route_id, paper_position_id,
                    market_observed_at, market_ingest_seq, source_event_key,
                    observation_fingerprint, market_price_numerator_raw,
                    market_price_denominator_raw, current_virtual_token_reserve_raw,
                    derived_token_input_raw, price_impact_bps,
                    signed_market_move_bps, adverse_slippage_bps, slippage_cap_bps,
                    decision, rejection_reason,
                    simulated_exit_price_numerator_raw,
                    simulated_exit_price_denominator_raw,
                    gross_exit_proceeds_lamports,
                    venue_fee_lamports, interface_fee_lamports,
                    base_network_fee_lamports, priority_fee_lamports,
                    builder_tip_lamports, total_exit_explicit_cost_lamports,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    route.route_id,
                    route.paper_position_id,
                    _dt_text(observation.observed_at),
                    observation.ingest_seq,
                    observation.source_event_key,
                    fingerprint,
                    str(observation.price_numerator_raw),
                    str(observation.price_denominator_raw),
                    str(observation.current_virtual_token_reserve_raw),
                    int(impact.derived_token_input_raw or 0),
                    int(impact.router_price_impact_bps or 0),
                    fill_eval.signed_market_move_bps,
                    fill_eval.adverse_slippage_bps,
                    fill_eval.slippage_cap_bps,
                    fill_eval.decision.value,
                    fill_eval.rejection_reason,
                    str(fill_eval.simulated_execution_price.numerator_raw),
                    str(fill_eval.simulated_execution_price.denominator_raw),
                    gross_exit_proceeds_lamports,
                    None if costs is None else costs.venue_fee_lamports,
                    None if costs is None else costs.interface_fee_lamports,
                    None if costs is None else costs.base_network_fee_lamports,
                    None if costs is None else costs.priority_fee_lamports,
                    None if costs is None else costs.builder_tip_lamports,
                    None if costs is None else costs.total_explicit_cost_lamports,
                    _dt_text(observation.observed_at),
                ),
            )
        return self._require_attempt(attempt_id)

    def _stage_selected_fill(
        self,
        route_id: str,
        attempt: ExitExecutionAttempt,
        impact: RuntimeExitPriceImpact,
        fill_eval: FillEvaluation,
        gross_proceeds: int,
        costs: ExplicitCostBreakdown,
    ) -> None:
        route = self.get_route(route_id)
        if route is None:
            raise KeyError(route_id)
        if route.state is ExitExecutionState.FILLED:
            return
        if route.state is ExitExecutionState.FILL_SELECTED:
            if route.selected_attempt_id != attempt.attempt_id:
                raise ExitExecutionDeterminismConflict("route already has a different selected fill attempt")
            return

        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_exit_execution_routes
                SET state=?, state_reason=?, updated_at=?,
                    selected_attempt_id=?, selected_market_observed_at=?,
                    selected_market_ingest_seq=?, selected_source_event_key=?,
                    derived_token_input_raw=?, price_impact_bps=?,
                    signed_market_move_bps=?, adverse_slippage_bps=?, slippage_cap_bps=?,
                    simulated_exit_price_numerator_raw=?, simulated_exit_price_denominator_raw=?,
                    gross_exit_proceeds_lamports=?, total_exit_explicit_cost_lamports=?
                WHERE route_id=?
                """,
                (
                    ExitExecutionState.FILL_SELECTED.value,
                    "CAUSAL_EXIT_FILL_SELECTED",
                    _dt_text(attempt.market_observed_at),
                    attempt.attempt_id,
                    _dt_text(attempt.market_observed_at),
                    attempt.market_ingest_seq,
                    attempt.source_event_key,
                    int(impact.derived_token_input_raw or 0),
                    int(impact.router_price_impact_bps or 0),
                    fill_eval.signed_market_move_bps,
                    fill_eval.adverse_slippage_bps,
                    fill_eval.slippage_cap_bps,
                    str(fill_eval.simulated_execution_price.numerator_raw),
                    str(fill_eval.simulated_execution_price.denominator_raw),
                    gross_proceeds,
                    costs.total_explicit_cost_lamports,
                    route_id,
                ),
            )

    def _finalize_selected_route(self, route_id: str) -> None:
        route = self.get_route(route_id)
        if route is None:
            raise KeyError(route_id)
        if route.state is ExitExecutionState.FILLED:
            return
        if route.state is not ExitExecutionState.FILL_SELECTED:
            return
        if not route.selected_attempt_id or route.selected_market_observed_at is None:
            raise ExitExecutionDeterminismConflict("FILL_SELECTED route missing selected attempt lineage")
        if route.simulated_exit_price_numerator_raw is None or route.simulated_exit_price_denominator_raw is None:
            raise ExitExecutionDeterminismConflict("FILL_SELECTED route missing simulated exit price")

        attempt = self._require_attempt(route.selected_attempt_id)
        position = self.lifecycle.get_position(route.paper_position_id)
        if position is None:
            raise ExitExecutionBindingError("selected exit route missing lifecycle position")

        close_reason = f"PAPER_EXIT_FILLED:{route.exit_reason}"
        try:
            closed = self.lifecycle.close_position(
                route.paper_position_id,
                closed_at=route.selected_market_observed_at,
                exit_price_numerator_raw=route.simulated_exit_price_numerator_raw,
                exit_price_denominator_raw=route.simulated_exit_price_denominator_raw,
                reason=close_reason,
            )
        except LifecycleDeterminismConflict as exc:
            raise ExitExecutionDeterminismConflict(str(exc)) from exc

        if closed.state is not PositionState.CLOSED:
            raise ExitExecutionDeterminismConflict("selected exit did not close lifecycle position")

        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_exit_execution_routes
                SET state=?, state_reason=?, updated_at=?
                WHERE route_id=?
                """,
                (
                    ExitExecutionState.FILLED.value,
                    close_reason,
                    _dt_text(attempt.market_observed_at),
                    route_id,
                ),
            )

    def _recover_selected_fills(self) -> None:
        rows = self.conn.execute(
            "SELECT route_id FROM paper_exit_execution_routes WHERE state='FILL_SELECTED' ORDER BY route_id"
        ).fetchall()
        for row in rows:
            self._finalize_selected_route(str(row["route_id"]))

    def _attempt_for_key(
        self,
        route_id: str,
        observed_at: datetime,
        ingest_seq: int,
    ) -> ExitExecutionAttempt | None:
        row = self.conn.execute(
            """
            SELECT * FROM paper_exit_execution_attempts
            WHERE route_id=? AND market_observed_at=? AND market_ingest_seq=?
            """,
            (route_id, _dt_text(observed_at), int(ingest_seq)),
        ).fetchone()
        return None if row is None else _attempt_from_row(row)

    def _last_attempt(self, route_id: str) -> ExitExecutionAttempt | None:
        row = self.conn.execute(
            """
            SELECT * FROM paper_exit_execution_attempts
            WHERE route_id=?
            ORDER BY market_observed_at DESC, market_ingest_seq DESC, attempt_id DESC
            LIMIT 1
            """,
            (route_id,),
        ).fetchone()
        return None if row is None else _attempt_from_row(row)

    def _require_attempt(self, attempt_id: str) -> ExitExecutionAttempt:
        row = self.conn.execute(
            "SELECT * FROM paper_exit_execution_attempts WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise KeyError(attempt_id)
        return _attempt_from_row(row)

    def _fill_eval_from_attempt(
        self,
        route: ExitExecutionRoute,
        attempt: ExitExecutionAttempt,
    ) -> FillEvaluation:
        return self.cost_model.evaluate_fill(
            side=CostSide.EXIT,
            signal_reference_price=RationalPrice(
                route.price_identity,
                route.reference_price_numerator_raw,
                route.reference_price_denominator_raw,
            ),
            market_price_at_ready=RationalPrice(
                route.price_identity,
                attempt.market_price_numerator_raw,
                attempt.market_price_denominator_raw,
            ),
            price_impact_bps=attempt.price_impact_bps,
            signal_observed_at=route.requested_at,
            market_observed_at=attempt.market_observed_at,
        )

    def _impact_from_attempt(
        self,
        route: ExitExecutionRoute,
        attempt: ExitExecutionAttempt,
    ) -> RuntimeExitPriceImpact:
        position = self.lifecycle.get_position(route.paper_position_id)
        if position is None:
            raise ExitExecutionBindingError("attempt replay missing position")
        return self.impact_provider.for_position(
            price_identity=route.price_identity,
            filled_size_lamports=position.filled_size_lamports,
            entry_price_numerator_raw=position.entry_price_numerator_raw,
            entry_price_denominator_raw=position.entry_price_denominator_raw,
            current_virtual_token_reserve_raw=attempt.current_virtual_token_reserve_raw,
        )

    def _costs_from_attempt(self, attempt: ExitExecutionAttempt) -> ExplicitCostBreakdown | None:
        if attempt.total_exit_explicit_cost_lamports is None or attempt.gross_exit_proceeds_lamports is None:
            return None
        # Reconstruct through the locked model. The persisted total is verified
        # against the reconstruction to fail closed on baseline drift.
        costs = self.cost_model.explicit_costs(
            side=CostSide.EXIT,
            notional_lamports=attempt.gross_exit_proceeds_lamports,
            observed_priority_fee_lamports=None,
            venue_fee_bps=None,
        )
        if costs.total_explicit_cost_lamports != attempt.total_exit_explicit_cost_lamports:
            raise ExitExecutionDeterminismConflict("persisted exit explicit cost differs from locked model replay")
        return costs


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS paper_exit_execution_schema_meta (
            schema_version TEXT PRIMARY KEY,
            engine_version TEXT NOT NULL,
            exit_impact_model_id TEXT NOT NULL,
            exit_impact_model_fingerprint TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS paper_exit_execution_routes (
            route_id TEXT PRIMARY KEY,
            exit_intent_id TEXT NOT NULL UNIQUE,
            paper_position_id TEXT NOT NULL UNIQUE,
            paper_order_id TEXT NOT NULL,
            signal_key TEXT NOT NULL,
            mint TEXT NOT NULL,
            track_id TEXT NOT NULL,
            exit_variant TEXT NOT NULL,
            exit_reason TEXT NOT NULL,
            price_identity TEXT NOT NULL,
            assumption_set_id TEXT NOT NULL,
            cost_model_fingerprint TEXT NOT NULL,
            exit_impact_model_id TEXT NOT NULL,
            exit_impact_model_fingerprint TEXT NOT NULL,
            requested_at TEXT NOT NULL,
            execution_ready_at TEXT NOT NULL,
            reference_source TEXT NOT NULL CHECK(reference_source IN ('TRIGGER_MARKET','LAST_FRESH_MARKET','RULE_REFERENCE_FALLBACK')),
            reference_observed_at TEXT NOT NULL,
            reference_ingest_seq INTEGER NOT NULL,
            reference_price_numerator_raw TEXT NOT NULL,
            reference_price_denominator_raw TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('WAITING','FILL_SELECTED','FILLED')),
            state_reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            selected_attempt_id TEXT,
            selected_market_observed_at TEXT,
            selected_market_ingest_seq INTEGER,
            selected_source_event_key TEXT,
            derived_token_input_raw TEXT,
            price_impact_bps INTEGER,
            signed_market_move_bps INTEGER,
            adverse_slippage_bps INTEGER,
            slippage_cap_bps INTEGER,
            simulated_exit_price_numerator_raw TEXT,
            simulated_exit_price_denominator_raw TEXT,
            gross_exit_proceeds_lamports INTEGER,
            total_exit_explicit_cost_lamports INTEGER
        );

        CREATE TABLE IF NOT EXISTS paper_exit_execution_attempts (
            attempt_id TEXT PRIMARY KEY,
            route_id TEXT NOT NULL,
            paper_position_id TEXT NOT NULL,
            market_observed_at TEXT NOT NULL,
            market_ingest_seq INTEGER NOT NULL,
            source_event_key TEXT,
            observation_fingerprint TEXT NOT NULL,
            market_price_numerator_raw TEXT NOT NULL,
            market_price_denominator_raw TEXT NOT NULL,
            current_virtual_token_reserve_raw TEXT NOT NULL,
            derived_token_input_raw TEXT NOT NULL,
            price_impact_bps INTEGER NOT NULL,
            signed_market_move_bps INTEGER NOT NULL,
            adverse_slippage_bps INTEGER NOT NULL,
            slippage_cap_bps INTEGER NOT NULL,
            decision TEXT NOT NULL CHECK(decision IN ('FILLED','REJECTED_SLIPPAGE')),
            rejection_reason TEXT,
            simulated_exit_price_numerator_raw TEXT NOT NULL,
            simulated_exit_price_denominator_raw TEXT NOT NULL,
            gross_exit_proceeds_lamports INTEGER,
            venue_fee_lamports INTEGER,
            interface_fee_lamports INTEGER,
            base_network_fee_lamports INTEGER,
            priority_fee_lamports INTEGER,
            builder_tip_lamports INTEGER,
            total_exit_explicit_cost_lamports INTEGER,
            created_at TEXT NOT NULL,
            UNIQUE(route_id, market_observed_at, market_ingest_seq)
        );

        CREATE INDEX IF NOT EXISTS idx_exit_exec_routes_state
            ON paper_exit_execution_routes(state, track_id);
        CREATE INDEX IF NOT EXISTS idx_exit_exec_attempts_route
            ON paper_exit_execution_attempts(route_id, market_observed_at, market_ingest_seq);
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO paper_exit_execution_schema_meta(
            schema_version, engine_version, exit_impact_model_id, exit_impact_model_fingerprint
        ) VALUES (?, ?, ?, ?)
        """,
        (SCHEMA_VERSION, ENGINE_VERSION, EXIT_IMPACT_MODEL_ID, EXIT_IMPACT_FINGERPRINT),
    )
    row = conn.execute(
        "SELECT * FROM paper_exit_execution_schema_meta WHERE schema_version=?",
        (SCHEMA_VERSION,),
    ).fetchone()
    if row is None:
        raise RuntimeError("exit execution schema metadata missing")
    if (
        row["engine_version"] != ENGINE_VERSION
        or row["exit_impact_model_id"] != EXIT_IMPACT_MODEL_ID
        or row["exit_impact_model_fingerprint"] != EXIT_IMPACT_FINGERPRINT
    ):
        raise ExitExecutionDeterminismConflict("exit execution schema metadata drift")
    conn.commit()


def _route_from_row(row: sqlite3.Row) -> ExitExecutionRoute:
    return ExitExecutionRoute(
        route_id=str(row["route_id"]),
        exit_intent_id=str(row["exit_intent_id"]),
        paper_position_id=str(row["paper_position_id"]),
        paper_order_id=str(row["paper_order_id"]),
        signal_key=str(row["signal_key"]),
        mint=str(row["mint"]),
        track_id=str(row["track_id"]),
        exit_variant=str(row["exit_variant"]),
        exit_reason=str(row["exit_reason"]),
        price_identity=str(row["price_identity"]),
        assumption_set_id=str(row["assumption_set_id"]),
        cost_model_fingerprint=str(row["cost_model_fingerprint"]),
        exit_impact_model_id=str(row["exit_impact_model_id"]),
        exit_impact_model_fingerprint=str(row["exit_impact_model_fingerprint"]),
        requested_at=_dt_parse(row["requested_at"]),  # type: ignore[arg-type]
        execution_ready_at=_dt_parse(row["execution_ready_at"]),  # type: ignore[arg-type]
        reference_source=ExitReferenceSource(str(row["reference_source"])),
        reference_observed_at=_dt_parse(row["reference_observed_at"]),  # type: ignore[arg-type]
        reference_ingest_seq=int(row["reference_ingest_seq"]),
        reference_price_numerator_raw=int(row["reference_price_numerator_raw"]),
        reference_price_denominator_raw=int(row["reference_price_denominator_raw"]),
        state=ExitExecutionState(str(row["state"])),
        state_reason=str(row["state_reason"]),
        created_at=_dt_parse(row["created_at"]),  # type: ignore[arg-type]
        updated_at=_dt_parse(row["updated_at"]),  # type: ignore[arg-type]
        selected_attempt_id=row["selected_attempt_id"],
        selected_market_observed_at=_dt_parse(row["selected_market_observed_at"]),
        selected_market_ingest_seq=(
            None if row["selected_market_ingest_seq"] is None else int(row["selected_market_ingest_seq"])
        ),
        selected_source_event_key=row["selected_source_event_key"],
        derived_token_input_raw=(None if row["derived_token_input_raw"] is None else int(row["derived_token_input_raw"])),
        price_impact_bps=(None if row["price_impact_bps"] is None else int(row["price_impact_bps"])),
        signed_market_move_bps=(None if row["signed_market_move_bps"] is None else int(row["signed_market_move_bps"])),
        adverse_slippage_bps=(None if row["adverse_slippage_bps"] is None else int(row["adverse_slippage_bps"])),
        slippage_cap_bps=(None if row["slippage_cap_bps"] is None else int(row["slippage_cap_bps"])),
        simulated_exit_price_numerator_raw=(None if row["simulated_exit_price_numerator_raw"] is None else int(row["simulated_exit_price_numerator_raw"])),
        simulated_exit_price_denominator_raw=(None if row["simulated_exit_price_denominator_raw"] is None else int(row["simulated_exit_price_denominator_raw"])),
        gross_exit_proceeds_lamports=(None if row["gross_exit_proceeds_lamports"] is None else int(row["gross_exit_proceeds_lamports"])),
        total_exit_explicit_cost_lamports=(None if row["total_exit_explicit_cost_lamports"] is None else int(row["total_exit_explicit_cost_lamports"])),
    )


def _attempt_from_row(row: sqlite3.Row) -> ExitExecutionAttempt:
    return ExitExecutionAttempt(
        attempt_id=str(row["attempt_id"]),
        route_id=str(row["route_id"]),
        paper_position_id=str(row["paper_position_id"]),
        market_observed_at=_dt_parse(row["market_observed_at"]),  # type: ignore[arg-type]
        market_ingest_seq=int(row["market_ingest_seq"]),
        source_event_key=row["source_event_key"],
        observation_fingerprint=str(row["observation_fingerprint"]),
        market_price_numerator_raw=int(row["market_price_numerator_raw"]),
        market_price_denominator_raw=int(row["market_price_denominator_raw"]),
        current_virtual_token_reserve_raw=int(row["current_virtual_token_reserve_raw"]),
        derived_token_input_raw=int(row["derived_token_input_raw"]),
        price_impact_bps=int(row["price_impact_bps"]),
        signed_market_move_bps=int(row["signed_market_move_bps"]),
        adverse_slippage_bps=int(row["adverse_slippage_bps"]),
        slippage_cap_bps=int(row["slippage_cap_bps"]),
        decision=FillDecision(str(row["decision"])),
        rejection_reason=row["rejection_reason"],
        simulated_exit_price_numerator_raw=int(row["simulated_exit_price_numerator_raw"]),
        simulated_exit_price_denominator_raw=int(row["simulated_exit_price_denominator_raw"]),
        gross_exit_proceeds_lamports=(None if row["gross_exit_proceeds_lamports"] is None else int(row["gross_exit_proceeds_lamports"])),
        total_exit_explicit_cost_lamports=(None if row["total_exit_explicit_cost_lamports"] is None else int(row["total_exit_explicit_cost_lamports"])),
        created_at=_dt_parse(row["created_at"]),  # type: ignore[arg-type]
    )
