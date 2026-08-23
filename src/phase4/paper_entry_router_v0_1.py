from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Iterable

from .paper_cost_model_v0_2 import (
    CostSide,
    ExplicitCostBreakdown,
    FillDecision,
    FillEvaluation,
    PaperCostModelV02,
    RationalPrice,
)
from .paper_lifecycle_v0_1 import (
    LOCKED_PHASE4_TRACKS,
    OrderState,
    PaperCandidateRef,
    PaperLifecycleStore,
    PaperOrder,
)


SCHEMA_VERSION = "phase4_paper_entry_router_v0.1"
ENGINE_VERSION = "PaperEntryRouterV01"


class EntryRouteState(StrEnum):
    ROUTED = "ROUTED"
    ENTRY_PENDING = "ENTRY_PENDING"
    FILLED = "FILLED"
    REJECTED = "REJECTED"


@dataclass(frozen=True, slots=True)
class CandidateSignalEnvelope:
    """Stable Phase-4 view of a strategy CandidateSignal.

    This is deliberately an adapter boundary. CandidateSignal remains owned by
    the strategy layer and does not gain order/fill/wallet state.
    """

    candidate_id: str
    mint: str
    strategy_version: str
    parameter_set_id: str
    signal_observed_at: datetime
    signal_ingest_seq: int
    price_identity: str
    price_numerator_raw: int
    price_denominator_raw: int

    def __post_init__(self) -> None:
        if not self.candidate_id:
            raise ValueError("candidate_id is required")
        if not self.mint:
            raise ValueError("mint is required")
        if not self.strategy_version:
            raise ValueError("strategy_version is required")
        if not self.parameter_set_id:
            raise ValueError("parameter_set_id is required")
        _require_aware(self.signal_observed_at)
        if self.signal_ingest_seq < 0:
            raise ValueError("signal_ingest_seq must be >= 0")
        if not self.price_identity:
            raise ValueError("price_identity is required")
        if self.price_numerator_raw <= 0 or self.price_denominator_raw <= 0:
            raise ValueError("candidate price numerator/denominator must be > 0")

    def to_paper_candidate(self) -> PaperCandidateRef:
        return PaperCandidateRef(
            signal_key=self.candidate_id,
            mint=self.mint,
            strategy_version=self.strategy_version,
            parameter_set_id=self.parameter_set_id,
            signal_observed_at=self.signal_observed_at,
            signal_ingest_seq=self.signal_ingest_seq,
            reference_price_identity=self.price_identity,
            reference_price_numerator_raw=self.price_numerator_raw,
            reference_price_denominator_raw=self.price_denominator_raw,
        )


@dataclass(frozen=True, slots=True)
class EntryMarketObservation:
    mint: str
    observed_at: datetime
    ingest_seq: int
    price_identity: str
    price_numerator_raw: int
    price_denominator_raw: int
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
            raise ValueError("market price numerator/denominator must be > 0")

    @property
    def price(self) -> RationalPrice:
        return RationalPrice(
            self.price_identity,
            self.price_numerator_raw,
            self.price_denominator_raw,
        )


@dataclass(frozen=True, slots=True)
class EntryRoute:
    route_id: str
    candidate_id: str
    mint: str
    strategy_version: str
    parameter_set_id: str
    assumption_set_id: str
    model_fingerprint: str
    signal_observed_at: datetime
    signal_ingest_seq: int
    reference_price_identity: str
    reference_price_numerator_raw: int
    reference_price_denominator_raw: int
    requested_size_lamports: int
    execution_ready_at: datetime
    state: EntryRouteState
    state_reason: str
    created_at: datetime
    updated_at: datetime
    selected_market_observed_at: datetime | None
    selected_market_ingest_seq: int | None
    selected_source_event_key: str | None
    price_impact_bps: int | None
    signed_market_move_bps: int | None
    adverse_slippage_bps: int | None
    slippage_cap_bps: int | None
    simulated_entry_price_numerator_raw: int | None
    simulated_entry_price_denominator_raw: int | None
    total_entry_explicit_cost_lamports: int | None


@dataclass(frozen=True, slots=True)
class RouteResult:
    route: EntryRoute
    orders: tuple[PaperOrder, ...]
    fill_evaluation: FillEvaluation | None
    explicit_costs: ExplicitCostBreakdown | None


class RouteDeterminismConflict(RuntimeError):
    pass


class PaperEntryRouterV01:
    """Deterministic CandidateSignal -> simulated-entry router.

    v0.1 intentionally does NOT implement the live strategy runner itself. It
    implements the Phase-4 routing contract that a live CandidateSignal source
    will call in the next integration sub-step.

    Important behavior:
    - one CandidateSignal fans out to the locked FINAL-A / FINAL-B / SENS-C
      paper tracks using one shared entry decision;
    - entry latency is causal: no market observation before execution_ready_at
      can fill the route;
    - GAP_RECOVERY is never eligible as a fresh fill observation;
    - cross-mint and price-identity mismatches cannot contaminate a fill;
    - replay of the same candidate/observation is idempotent;
    - one slippage rejection is applied consistently to all three entry orders;
    - no wallet, signing, RPC, or live order path exists here.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        cost_model: PaperCostModelV02,
        lifecycle_store: PaperLifecycleStore | None = None,
    ) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cost_model = cost_model
        self.lifecycle = lifecycle_store or PaperLifecycleStore(conn)
        _create_schema(conn)

    def route_candidate(self, candidate: CandidateSignalEnvelope) -> RouteResult:
        paper_candidate = candidate.to_paper_candidate()
        baseline = self.cost_model.baseline
        size = baseline.reference_position_lamports
        route_id = deterministic_route_id(
            candidate.candidate_id,
            baseline.assumption_set_id,
            baseline.fingerprint,
        )
        ready_at = self.cost_model.execution_ready_at(
            candidate.signal_observed_at,
            CostSide.ENTRY,
        )

        existing = self.get_route(route_id)
        if existing is None:
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO paper_entry_routes(
                        route_id, candidate_id, mint, strategy_version,
                        parameter_set_id, assumption_set_id, model_fingerprint,
                        signal_observed_at, signal_ingest_seq,
                        reference_price_identity,
                        reference_price_numerator_raw,
                        reference_price_denominator_raw,
                        requested_size_lamports, execution_ready_at,
                        state, state_reason, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        route_id,
                        candidate.candidate_id,
                        candidate.mint,
                        candidate.strategy_version,
                        candidate.parameter_set_id,
                        baseline.assumption_set_id,
                        baseline.fingerprint,
                        _dt_text(candidate.signal_observed_at),
                        candidate.signal_ingest_seq,
                        candidate.price_identity,
                        str(candidate.price_numerator_raw),
                        str(candidate.price_denominator_raw),
                        size,
                        _dt_text(ready_at),
                        EntryRouteState.ROUTED.value,
                        "CANDIDATE_SIGNAL_ROUTED",
                        _dt_text(candidate.signal_observed_at),
                        _dt_text(candidate.signal_observed_at),
                    ),
                )
                _insert_route_event(
                    self.conn,
                    route_id=route_id,
                    from_state=None,
                    to_state=EntryRouteState.ROUTED,
                    effective_at=candidate.signal_observed_at,
                    reason="CANDIDATE_SIGNAL_ROUTED",
                )
        else:
            self._assert_route_matches_candidate(existing, candidate)

        orders = tuple(
            self.lifecycle.create_order(
                paper_candidate,
                track_id=track_id,
                requested_size_lamports=size,
                reason="CANDIDATE_ROUTED_TO_PAPER",
            )
            for track_id in LOCKED_PHASE4_TRACKS
        )

        # Move all newly-created orders to ENTRY_PENDING. Replay is naturally
        # idempotent because lifecycle transitions return persisted state.
        pending_orders: list[PaperOrder] = []
        for order in orders:
            if order.state is OrderState.CREATED:
                pending_orders.append(
                    self.lifecycle.mark_entry_pending(
                        order.paper_order_id,
                        effective_at=candidate.signal_observed_at,
                        reason="WAITING_FOR_CAUSAL_ENTRY_FILL",
                    )
                )
            else:
                pending_orders.append(order)

        route = self.get_route(route_id)
        assert route is not None
        if route.state is EntryRouteState.ROUTED:
            route = self._transition_route(
                route,
                EntryRouteState.ENTRY_PENDING,
                effective_at=candidate.signal_observed_at,
                reason="WAITING_FOR_CAUSAL_ENTRY_FILL",
            )

        return RouteResult(
            route=route,
            orders=tuple(pending_orders),
            fill_evaluation=None,
            explicit_costs=None,
        )

    def on_market_observation(
        self,
        *,
        candidate: CandidateSignalEnvelope,
        observation: EntryMarketObservation,
        price_impact_bps: int,
        observed_priority_fee_lamports: int | None = None,
        venue_fee_bps: int | None = None,
    ) -> RouteResult:
        routed = self.route_candidate(candidate)
        route = routed.route

        if route.state in (EntryRouteState.FILLED, EntryRouteState.REJECTED):
            return self._replay_terminal(
                route=route,
                candidate=candidate,
                observation=observation,
                price_impact_bps=price_impact_bps,
                observed_priority_fee_lamports=observed_priority_fee_lamports,
                venue_fee_bps=venue_fee_bps,
            )

        if observation.mint != candidate.mint:
            return routed
        if observation.is_gap_recovery:
            return routed
        if observation.price_identity != candidate.price_identity:
            return routed
        if _utc(observation.observed_at) < route.execution_ready_at:
            return routed

        fill_eval = self.cost_model.evaluate_fill(
            side=CostSide.ENTRY,
            signal_reference_price=RationalPrice(
                candidate.price_identity,
                candidate.price_numerator_raw,
                candidate.price_denominator_raw,
            ),
            market_price_at_ready=observation.price,
            price_impact_bps=price_impact_bps,
            signal_observed_at=candidate.signal_observed_at,
            market_observed_at=observation.observed_at,
        )

        if fill_eval.decision is FillDecision.REJECTED_SLIPPAGE:
            rejected_orders: list[PaperOrder] = []
            for order in routed.orders:
                if order.state in (OrderState.CREATED, OrderState.ENTRY_PENDING):
                    rejected_orders.append(
                        self.lifecycle.reject_order(
                            order.paper_order_id,
                            effective_at=observation.observed_at,
                            reason="ENTRY_SLIPPAGE_CAP_EXCEEDED",
                        )
                    )
                else:
                    rejected_orders.append(order)

            route = self._complete_route(
                route=route,
                target=EntryRouteState.REJECTED,
                observation=observation,
                fill_eval=fill_eval,
                explicit_costs=None,
                reason="ENTRY_SLIPPAGE_CAP_EXCEEDED",
            )
            return RouteResult(route, tuple(rejected_orders), fill_eval, None)

        costs = self.cost_model.explicit_costs(
            side=CostSide.ENTRY,
            notional_lamports=route.requested_size_lamports,
            observed_priority_fee_lamports=observed_priority_fee_lamports,
            venue_fee_bps=venue_fee_bps,
        )

        filled_orders: list[PaperOrder] = []
        for order in routed.orders:
            filled, _position = self.lifecycle.fill_order(
                order.paper_order_id,
                fill_at=observation.observed_at,
                filled_size_lamports=route.requested_size_lamports,
                simulated_entry_price_numerator_raw=(
                    fill_eval.simulated_execution_price.numerator_raw
                ),
                simulated_entry_price_denominator_raw=(
                    fill_eval.simulated_execution_price.denominator_raw
                ),
                reason="PAPER_ENTRY_FILLED_FROM_SHARED_ROUTE",
            )
            filled_orders.append(filled)

        route = self._complete_route(
            route=route,
            target=EntryRouteState.FILLED,
            observation=observation,
            fill_eval=fill_eval,
            explicit_costs=costs,
            reason="PAPER_ENTRY_FILLED_FROM_SHARED_ROUTE",
        )
        return RouteResult(route, tuple(filled_orders), fill_eval, costs)

    def process_observations(
        self,
        *,
        candidate: CandidateSignalEnvelope,
        observations: Iterable[EntryMarketObservation],
        price_impact_bps: int,
        observed_priority_fee_lamports: int | None = None,
        venue_fee_bps: int | None = None,
    ) -> RouteResult:
        """Deterministic batch helper for local replay/self-test.

        Live callers should feed observations as they become available. The
        helper sorts by BOT_TRUTH (observed_at, ingest_seq) and stops on first
        terminal entry decision.
        """

        result = self.route_candidate(candidate)
        for observation in sorted(
            observations,
            key=lambda row: (_utc(row.observed_at), row.ingest_seq),
        ):
            result = self.on_market_observation(
                candidate=candidate,
                observation=observation,
                price_impact_bps=price_impact_bps,
                observed_priority_fee_lamports=observed_priority_fee_lamports,
                venue_fee_bps=venue_fee_bps,
            )
            if result.route.state in (
                EntryRouteState.FILLED,
                EntryRouteState.REJECTED,
            ):
                break
        return result

    def get_route(self, route_id: str) -> EntryRoute | None:
        row = self.conn.execute(
            "SELECT * FROM paper_entry_routes WHERE route_id=?",
            (route_id,),
        ).fetchone()
        return None if row is None else _route_from_row(row)

    def list_routes(self) -> list[EntryRoute]:
        rows = self.conn.execute(
            "SELECT * FROM paper_entry_routes ORDER BY created_at, route_id"
        ).fetchall()
        return [_route_from_row(row) for row in rows]

    def _transition_route(
        self,
        route: EntryRoute,
        target: EntryRouteState,
        *,
        effective_at: datetime,
        reason: str,
    ) -> EntryRoute:
        if target is not EntryRouteState.ENTRY_PENDING:
            raise ValueError("_transition_route only handles ENTRY_PENDING")
        if route.state is target:
            return route
        if route.state is not EntryRouteState.ROUTED:
            raise RouteDeterminismConflict(
                f"route {route.route_id} cannot transition {route.state} -> {target}"
            )
        if _utc(effective_at) < route.updated_at:
            raise RouteDeterminismConflict("route transition time regressed")
        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_entry_routes
                SET state=?, state_reason=?, updated_at=?
                WHERE route_id=?
                """,
                (
                    target.value,
                    reason,
                    _dt_text(effective_at),
                    route.route_id,
                ),
            )
            _insert_route_event(
                self.conn,
                route_id=route.route_id,
                from_state=route.state,
                to_state=target,
                effective_at=effective_at,
                reason=reason,
            )
        updated = self.get_route(route.route_id)
        assert updated is not None
        return updated

    def _complete_route(
        self,
        *,
        route: EntryRoute,
        target: EntryRouteState,
        observation: EntryMarketObservation,
        fill_eval: FillEvaluation,
        explicit_costs: ExplicitCostBreakdown | None,
        reason: str,
    ) -> EntryRoute:
        if target not in (EntryRouteState.FILLED, EntryRouteState.REJECTED):
            raise ValueError("terminal route target required")
        if route.state not in (EntryRouteState.ROUTED, EntryRouteState.ENTRY_PENDING):
            raise RouteDeterminismConflict(
                f"cannot complete terminal route from state {route.state}"
            )
        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_entry_routes
                SET state=?, state_reason=?, updated_at=?,
                    selected_market_observed_at=?, selected_market_ingest_seq=?,
                    selected_source_event_key=?, price_impact_bps=?,
                    signed_market_move_bps=?, adverse_slippage_bps=?,
                    slippage_cap_bps=?, simulated_entry_price_numerator_raw=?,
                    simulated_entry_price_denominator_raw=?,
                    total_entry_explicit_cost_lamports=?
                WHERE route_id=?
                """,
                (
                    target.value,
                    reason,
                    _dt_text(observation.observed_at),
                    _dt_text(observation.observed_at),
                    observation.ingest_seq,
                    observation.source_event_key,
                    fill_eval.price_impact_bps,
                    fill_eval.signed_market_move_bps,
                    fill_eval.adverse_slippage_bps,
                    fill_eval.slippage_cap_bps,
                    str(fill_eval.simulated_execution_price.numerator_raw),
                    str(fill_eval.simulated_execution_price.denominator_raw),
                    None
                    if explicit_costs is None
                    else explicit_costs.total_explicit_cost_lamports,
                    route.route_id,
                ),
            )
            _insert_route_event(
                self.conn,
                route_id=route.route_id,
                from_state=route.state,
                to_state=target,
                effective_at=observation.observed_at,
                reason=reason,
            )
        updated = self.get_route(route.route_id)
        assert updated is not None
        return updated

    def _replay_terminal(
        self,
        *,
        route: EntryRoute,
        candidate: CandidateSignalEnvelope,
        observation: EntryMarketObservation,
        price_impact_bps: int,
        observed_priority_fee_lamports: int | None,
        venue_fee_bps: int | None,
    ) -> RouteResult:
        if route.selected_market_observed_at is None:
            raise RouteDeterminismConflict("terminal route missing selected market observation")

        # Non-candidate observations after terminal completion are irrelevant.
        selected = (
            _dt_text(observation.observed_at) == _dt_text(route.selected_market_observed_at)
            and observation.ingest_seq == route.selected_market_ingest_seq
            and observation.source_event_key == route.selected_source_event_key
        )
        if not selected:
            orders = tuple(
                self.lifecycle.get_order(
                    _order_id(candidate.candidate_id, track_id)
                )
                for track_id in LOCKED_PHASE4_TRACKS
            )
            if any(order is None for order in orders):
                raise RouteDeterminismConflict("terminal route missing lifecycle order")
            return RouteResult(route, tuple(order for order in orders if order is not None), None, None)

        fill_eval = self.cost_model.evaluate_fill(
            side=CostSide.ENTRY,
            signal_reference_price=RationalPrice(
                candidate.price_identity,
                candidate.price_numerator_raw,
                candidate.price_denominator_raw,
            ),
            market_price_at_ready=observation.price,
            price_impact_bps=price_impact_bps,
            signal_observed_at=candidate.signal_observed_at,
            market_observed_at=observation.observed_at,
        )
        expected_state = (
            EntryRouteState.FILLED
            if fill_eval.decision is FillDecision.FILLED
            else EntryRouteState.REJECTED
        )
        if expected_state is not route.state:
            raise RouteDeterminismConflict("terminal replay changes fill decision")
        if fill_eval.signed_market_move_bps != route.signed_market_move_bps:
            raise RouteDeterminismConflict("terminal replay changes market move")
        if fill_eval.adverse_slippage_bps != route.adverse_slippage_bps:
            raise RouteDeterminismConflict("terminal replay changes adverse slippage")
        if fill_eval.price_impact_bps != route.price_impact_bps:
            raise RouteDeterminismConflict("terminal replay changes price impact")

        costs = None
        if route.state is EntryRouteState.FILLED:
            costs = self.cost_model.explicit_costs(
                side=CostSide.ENTRY,
                notional_lamports=route.requested_size_lamports,
                observed_priority_fee_lamports=observed_priority_fee_lamports,
                venue_fee_bps=venue_fee_bps,
            )
            if costs.total_explicit_cost_lamports != route.total_entry_explicit_cost_lamports:
                raise RouteDeterminismConflict("terminal replay changes explicit entry costs")

        orders = tuple(
            self.lifecycle.get_order(_order_id(candidate.candidate_id, track_id))
            for track_id in LOCKED_PHASE4_TRACKS
        )
        if any(order is None for order in orders):
            raise RouteDeterminismConflict("terminal route missing lifecycle order")
        return RouteResult(
            route,
            tuple(order for order in orders if order is not None),
            fill_eval,
            costs,
        )

    def _assert_route_matches_candidate(
        self,
        route: EntryRoute,
        candidate: CandidateSignalEnvelope,
    ) -> None:
        baseline = self.cost_model.baseline
        expected = (
            candidate.candidate_id,
            candidate.mint,
            candidate.strategy_version,
            candidate.parameter_set_id,
            baseline.assumption_set_id,
            baseline.fingerprint,
            _dt_text(candidate.signal_observed_at),
            candidate.signal_ingest_seq,
            candidate.price_identity,
            candidate.price_numerator_raw,
            candidate.price_denominator_raw,
            baseline.reference_position_lamports,
        )
        actual = (
            route.candidate_id,
            route.mint,
            route.strategy_version,
            route.parameter_set_id,
            route.assumption_set_id,
            route.model_fingerprint,
            _dt_text(route.signal_observed_at),
            route.signal_ingest_seq,
            route.reference_price_identity,
            route.reference_price_numerator_raw,
            route.reference_price_denominator_raw,
            route.requested_size_lamports,
        )
        if actual != expected:
            raise RouteDeterminismConflict(
                "candidate replay differs from persisted paper-entry route"
            )


def deterministic_route_id(
    candidate_id: str,
    assumption_set_id: str,
    model_fingerprint: str,
) -> str:
    body = "\x1f".join(
        (SCHEMA_VERSION, candidate_id, assumption_set_id, model_fingerprint)
    )
    return "PR-" + hashlib.sha256(body.encode("utf-8")).hexdigest()[:32]


def connect_entry_router_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    _create_schema(conn)
    return conn


def canonical_entry_router_digest(conn: sqlite3.Connection) -> str:
    route_rows = [dict(row) for row in conn.execute(
        "SELECT * FROM paper_entry_routes ORDER BY route_id"
    ).fetchall()]
    event_rows = [dict(row) for row in conn.execute(
        "SELECT * FROM paper_entry_route_events ORDER BY event_id"
    ).fetchall()]
    order_rows = [dict(row) for row in conn.execute(
        "SELECT * FROM paper_orders ORDER BY paper_order_id"
    ).fetchall()]
    position_rows = [dict(row) for row in conn.execute(
        "SELECT * FROM paper_positions ORDER BY paper_position_id"
    ).fetchall()]
    payload = {
        "schema": SCHEMA_VERSION,
        "routes": route_rows,
        "route_events": event_rows,
        "orders": order_rows,
        "positions": position_rows,
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _create_schema(conn: sqlite3.Connection) -> None:
    # PaperLifecycleStore creates lifecycle tables. This function only owns
    # Phase-4.3 entry-route persistence.
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS paper_entry_routes (
            route_id TEXT PRIMARY KEY,
            candidate_id TEXT NOT NULL,
            mint TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            parameter_set_id TEXT NOT NULL,
            assumption_set_id TEXT NOT NULL,
            model_fingerprint TEXT NOT NULL,
            signal_observed_at TEXT NOT NULL,
            signal_ingest_seq INTEGER NOT NULL,
            reference_price_identity TEXT NOT NULL,
            reference_price_numerator_raw TEXT NOT NULL,
            reference_price_denominator_raw TEXT NOT NULL,
            requested_size_lamports INTEGER NOT NULL CHECK(requested_size_lamports > 0),
            execution_ready_at TEXT NOT NULL,
            state TEXT NOT NULL,
            state_reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            selected_market_observed_at TEXT,
            selected_market_ingest_seq INTEGER,
            selected_source_event_key TEXT,
            price_impact_bps INTEGER,
            signed_market_move_bps INTEGER,
            adverse_slippage_bps INTEGER,
            slippage_cap_bps INTEGER,
            simulated_entry_price_numerator_raw TEXT,
            simulated_entry_price_denominator_raw TEXT,
            total_entry_explicit_cost_lamports INTEGER,
            UNIQUE(candidate_id, assumption_set_id, model_fingerprint),
            CHECK(state IN ('ROUTED','ENTRY_PENDING','FILLED','REJECTED'))
        );

        CREATE TABLE IF NOT EXISTS paper_entry_route_events (
            event_id TEXT PRIMARY KEY,
            route_id TEXT NOT NULL REFERENCES paper_entry_routes(route_id),
            from_state TEXT,
            to_state TEXT NOT NULL,
            effective_at TEXT NOT NULL,
            reason TEXT NOT NULL
        );
        """
    )
    conn.commit()


def _insert_route_event(
    conn: sqlite3.Connection,
    *,
    route_id: str,
    from_state: EntryRouteState | None,
    to_state: EntryRouteState,
    effective_at: datetime,
    reason: str,
) -> None:
    body = "\x1f".join(
        (
            SCHEMA_VERSION,
            route_id,
            "<NONE>" if from_state is None else from_state.value,
            to_state.value,
            _dt_text(effective_at),
            reason,
        )
    )
    event_id = "PRE-" + hashlib.sha256(body.encode("utf-8")).hexdigest()[:32]
    conn.execute(
        """
        INSERT OR IGNORE INTO paper_entry_route_events(
            event_id, route_id, from_state, to_state, effective_at, reason
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            route_id,
            None if from_state is None else from_state.value,
            to_state.value,
            _dt_text(effective_at),
            reason,
        ),
    )


def _route_from_row(row: sqlite3.Row) -> EntryRoute:
    return EntryRoute(
        route_id=row["route_id"],
        candidate_id=row["candidate_id"],
        mint=row["mint"],
        strategy_version=row["strategy_version"],
        parameter_set_id=row["parameter_set_id"],
        assumption_set_id=row["assumption_set_id"],
        model_fingerprint=row["model_fingerprint"],
        signal_observed_at=_dt_parse(row["signal_observed_at"]),
        signal_ingest_seq=int(row["signal_ingest_seq"]),
        reference_price_identity=row["reference_price_identity"],
        reference_price_numerator_raw=int(row["reference_price_numerator_raw"]),
        reference_price_denominator_raw=int(row["reference_price_denominator_raw"]),
        requested_size_lamports=int(row["requested_size_lamports"]),
        execution_ready_at=_dt_parse(row["execution_ready_at"]),
        state=EntryRouteState(row["state"]),
        state_reason=row["state_reason"],
        created_at=_dt_parse(row["created_at"]),
        updated_at=_dt_parse(row["updated_at"]),
        selected_market_observed_at=(
            None
            if row["selected_market_observed_at"] is None
            else _dt_parse(row["selected_market_observed_at"])
        ),
        selected_market_ingest_seq=(
            None
            if row["selected_market_ingest_seq"] is None
            else int(row["selected_market_ingest_seq"])
        ),
        selected_source_event_key=row["selected_source_event_key"],
        price_impact_bps=(
            None if row["price_impact_bps"] is None else int(row["price_impact_bps"])
        ),
        signed_market_move_bps=(
            None
            if row["signed_market_move_bps"] is None
            else int(row["signed_market_move_bps"])
        ),
        adverse_slippage_bps=(
            None
            if row["adverse_slippage_bps"] is None
            else int(row["adverse_slippage_bps"])
        ),
        slippage_cap_bps=(
            None if row["slippage_cap_bps"] is None else int(row["slippage_cap_bps"])
        ),
        simulated_entry_price_numerator_raw=(
            None
            if row["simulated_entry_price_numerator_raw"] is None
            else int(row["simulated_entry_price_numerator_raw"])
        ),
        simulated_entry_price_denominator_raw=(
            None
            if row["simulated_entry_price_denominator_raw"] is None
            else int(row["simulated_entry_price_denominator_raw"])
        ),
        total_entry_explicit_cost_lamports=(
            None
            if row["total_entry_explicit_cost_lamports"] is None
            else int(row["total_entry_explicit_cost_lamports"])
        ),
    )


def _order_id(candidate_id: str, track_id: str) -> str:
    # Mirror lifecycle deterministic identity without importing its private hash.
    from .paper_lifecycle_v0_1 import deterministic_order_id

    return deterministic_order_id(candidate_id, track_id, LOCKED_PHASE4_TRACKS[track_id])


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")


def _utc(value: datetime) -> datetime:
    _require_aware(value)
    return value.astimezone(timezone.utc)


def _dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _dt_parse(value: str) -> datetime:
    return _utc(datetime.fromisoformat(value))
