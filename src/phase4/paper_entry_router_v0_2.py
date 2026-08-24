from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from .paper_cost_model_v0_2 import (
    CostSide,
    FillDecision,
    FillEvaluation,
    PaperCostModelV02,
    RationalPrice,
)
from .paper_entry_execution_deadline_v0_1 import (
    MODEL_FINGERPRINT as DEADLINE_POLICY_FINGERPRINT,
    MODEL_ID as DEADLINE_POLICY_MODEL_ID,
    TRACK_ORDER,
    deadline_for,
)
from .paper_entry_router_v0_1 import (
    CandidateSignalEnvelope,
    EntryMarketObservation,
    EntryRoute,
    EntryRouteState,
    PaperEntryRouterV01,
    RouteDeterminismConflict,
    RouteResult,
    _insert_route_event,
    _order_id,
)
from .paper_lifecycle_v0_1 import OrderState, PaperLifecycleStore, PaperOrder


SCHEMA_VERSION = "phase4_paper_entry_router_v0.2"
ENGINE_VERSION = "PaperEntryRouterV02"
ENTRY_EXECUTION_DEADLINE_EXPIRED = "ENTRY_EXECUTION_DEADLINE_EXPIRED"
ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED = (
    "ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED"
)


@dataclass(frozen=True, slots=True)
class TrackEntryDecision:
    route_id: str
    paper_order_id: str
    candidate_id: str
    track_id: str
    signal_observed_at: datetime
    eligible_through_at: datetime
    expire_at: datetime
    state: str
    reason: str | None
    terminal_at: datetime | None
    terminal_market_observed_at: datetime | None
    terminal_market_ingest_seq: int | None
    terminal_source_event_key: str | None


class PaperEntryRouterV02(PaperEntryRouterV01):
    """Track-safe entry router with signal-anchored execution deadlines.

    The accepted v0.1 route and lifecycle tables remain authoritative.  This
    version adds a narrow per-track decision ledger so that expired tracks are
    rejected independently while still allowing eligible siblings to fill.
    """

    model_id = DEADLINE_POLICY_MODEL_ID
    model_fingerprint = DEADLINE_POLICY_FINGERPRINT
    schema_version = SCHEMA_VERSION

    def __init__(
        self,
        conn: sqlite3.Connection,
        cost_model: PaperCostModelV02,
        lifecycle_store: PaperLifecycleStore | None = None,
    ) -> None:
        super().__init__(conn, cost_model, lifecycle_store)
        _create_schema(conn)
        self._heal_incomplete_routes()

    def route_candidate(self, candidate: CandidateSignalEnvelope) -> RouteResult:
        result = super().route_candidate(candidate)
        now = _dt_text(candidate.signal_observed_at)
        orders_by_track = {order.track_id: order for order in result.orders}
        if set(orders_by_track) != set(TRACK_ORDER):
            raise RouteDeterminismConflict("entry route does not contain the locked track set")
        for track_id in TRACK_ORDER:
            order = orders_by_track[track_id]
            deadline = deadline_for(track_id, candidate.signal_observed_at)
            expected = (
                result.route.route_id,
                order.paper_order_id,
                candidate.candidate_id,
                track_id,
                _dt_text(candidate.signal_observed_at),
                _dt_text(deadline.eligible_through_at),
                _dt_text(deadline.expire_at),
                "PENDING",
                now,
                now,
            )
            existing = self.conn.execute(
                "SELECT * FROM paper_entry_execution_deadlines_v0_1 "
                "WHERE route_id=? AND track_id=?",
                (result.route.route_id, track_id),
            ).fetchone()
            if existing is None:
                with self.conn:
                    self.conn.execute(
                        """
                        INSERT INTO paper_entry_execution_deadlines_v0_1(
                            route_id,paper_order_id,candidate_id,track_id,
                            signal_observed_at,eligible_through_at,expire_at,
                            state,reason,terminal_at,
                            terminal_market_observed_at,terminal_market_ingest_seq,
                            terminal_source_event_key,price_impact_bps,
                            signed_market_move_bps,adverse_slippage_bps,
                            slippage_cap_bps,simulated_entry_price_numerator_raw,
                            simulated_entry_price_denominator_raw,created_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,'PENDING',NULL,NULL,NULL,NULL,NULL,
                                 NULL,NULL,NULL,NULL,NULL,NULL,?,?)
                        """,
                        expected[:7] + expected[8:],
                    )
            else:
                actual = (
                    str(existing["route_id"]),
                    str(existing["paper_order_id"]),
                    str(existing["candidate_id"]),
                    str(existing["track_id"]),
                    str(existing["signal_observed_at"]),
                    str(existing["eligible_through_at"]),
                    str(existing["expire_at"]),
                )
                if actual != expected[:7]:
                    raise RouteDeterminismConflict(
                        "entry execution deadline replay changed persisted lineage"
                    )
        return RouteResult(
            self._require_route(result.route.route_id),
            self._orders(candidate.candidate_id),
            result.fill_evaluation,
            result.explicit_costs,
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
            return self._replay_v02_terminal(
                route, candidate, observation, price_impact_bps,
                observed_priority_fee_lamports, venue_fee_bps,
            )
        if observation.mint != candidate.mint:
            return routed

        # Time is semantic even when a same-mint observation is otherwise not
        # price-eligible.  Expiry occurs first at canonical deadline + 1us.
        self._expire_due(route.route_id, observation.observed_at)
        route = self._require_route(route.route_id)
        if route.state is EntryRouteState.REJECTED:
            return RouteResult(route, self._orders(candidate.candidate_id), None, None)
        if observation.is_gap_recovery:
            return RouteResult(route, self._orders(candidate.candidate_id), None, None)
        if observation.price_identity != candidate.price_identity:
            return RouteResult(route, self._orders(candidate.candidate_id), None, None)
        if _utc(observation.observed_at) < route.execution_ready_at:
            return RouteResult(route, self._orders(candidate.candidate_id), None, None)

        eligible = tuple(
            row for row in self._deadline_rows(route.route_id)
            if str(row["state"]) == "PENDING"
            and _utc(observation.observed_at) <= _dt_parse(str(row["eligible_through_at"]))
        )
        if not eligible:
            return RouteResult(route, self._orders(candidate.candidate_id), None, None)

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
            for row in eligible:
                order = self.lifecycle.reject_order(
                    str(row["paper_order_id"]),
                    effective_at=observation.observed_at,
                    reason="ENTRY_SLIPPAGE_CAP_EXCEEDED",
                )
                self._record_market_terminal(
                    order, observation, fill_eval,
                    state="REJECTED", reason="ENTRY_SLIPPAGE_CAP_EXCEEDED",
                    route_total_entry_explicit_cost_lamports=None,
                )
            self._finish_terminal_route(route.route_id)
            route = self._require_route(route.route_id)
            return RouteResult(route, self._orders(candidate.candidate_id), fill_eval, None)

        costs = self.cost_model.explicit_costs(
            side=CostSide.ENTRY,
            notional_lamports=route.requested_size_lamports,
            observed_priority_fee_lamports=observed_priority_fee_lamports,
            venue_fee_bps=venue_fee_bps,
        )
        for row in eligible:
            order, _position = self.lifecycle.fill_order(
                str(row["paper_order_id"]),
                fill_at=observation.observed_at,
                filled_size_lamports=route.requested_size_lamports,
                simulated_entry_price_numerator_raw=(
                    fill_eval.simulated_execution_price.numerator_raw
                ),
                simulated_entry_price_denominator_raw=(
                    fill_eval.simulated_execution_price.denominator_raw
                ),
                reason="PAPER_ENTRY_FILLED_WITH_TRACK_DEADLINE",
            )
            self._record_market_terminal(
                order, observation, fill_eval,
                state="FILLED", reason="PAPER_ENTRY_FILLED_WITH_TRACK_DEADLINE",
                route_total_entry_explicit_cost_lamports=(
                    costs.total_explicit_cost_lamports
                ),
            )
        self._finish_terminal_route(route.route_id)
        route = self._require_route(route.route_id)
        return RouteResult(route, self._orders(candidate.candidate_id), fill_eval, costs)

    def on_clock(self, now: datetime) -> tuple[EntryRoute, ...]:
        instant = _utc(now)
        route_ids = tuple(
            str(row[0]) for row in self.conn.execute(
                "SELECT DISTINCT route_id FROM paper_entry_execution_deadlines_v0_1 "
                "WHERE state='PENDING' AND expire_at<=? ORDER BY route_id",
                (_dt_text(instant),),
            ).fetchall()
        )
        changed: list[EntryRoute] = []
        for route_id in route_ids:
            before = self._require_route(route_id)
            self._expire_due(route_id, instant)
            after = self._require_route(route_id)
            if after != before:
                changed.append(after)
        return tuple(changed)

    def list_track_decisions(self, route_id: str | None = None) -> tuple[TrackEntryDecision, ...]:
        sql = "SELECT * FROM paper_entry_execution_deadlines_v0_1"
        params: tuple[object, ...] = ()
        if route_id is not None:
            sql += " WHERE route_id=?"
            params = (route_id,)
        sql += " ORDER BY route_id, CASE track_id WHEN 'FINAL-A' THEN 0 WHEN 'FINAL-B' THEN 1 ELSE 2 END"
        return tuple(_decision_from_row(row) for row in self.conn.execute(sql, params).fetchall())

    def diagnostics(self) -> dict[str, object]:
        counts = {
            str(row[0]): int(row[1])
            for row in self.conn.execute(
                "SELECT state,COUNT(*) FROM paper_entry_execution_deadlines_v0_1 "
                "GROUP BY state ORDER BY state"
            ).fetchall()
        }
        expired = int(self.conn.execute(
            "SELECT COUNT(*) FROM paper_entry_execution_deadlines_v0_1 "
            "WHERE reason=?", (ENTRY_EXECUTION_DEADLINE_EXPIRED,),
        ).fetchone()[0])
        expired_per_track = {
            str(row[0]): int(row[1])
            for row in self.conn.execute(
                "SELECT track_id,COUNT(*) FROM paper_entry_execution_deadlines_v0_1 "
                "WHERE reason=? GROUP BY track_id ORDER BY track_id",
                (ENTRY_EXECUTION_DEADLINE_EXPIRED,),
            ).fetchall()
        }
        partial = int(self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT route_id FROM paper_entry_execution_deadlines_v0_1 "
            "GROUP BY route_id HAVING SUM(state='FILLED')>0 AND SUM(state='REJECTED')>0)"
        ).fetchone()[0])
        all_expired = int(self.conn.execute(
            "SELECT COUNT(*) FROM paper_entry_routes WHERE state='REJECTED' AND state_reason=?",
            (ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED,),
        ).fetchone()[0])
        per_track = {
            str(row[0]): int(row[1])
            for row in self.conn.execute(
                "SELECT track_id,COUNT(*) FROM paper_entry_execution_deadlines_v0_1 "
                "WHERE state='FILLED' GROUP BY track_id ORDER BY track_id"
            ).fetchall()
        }
        return {
            "expiry_events": expired,
            "execution_expired_by_track": expired_per_track,
            "partial_track_candidates": partial,
            "all_expired_candidates": all_expired,
            "pending_deadlines_at_end": counts.get("PENDING", 0),
            "filled_tracks": per_track,
            "track_state_counts": counts,
        }

    def canonical_digest(self) -> str:
        return canonical_entry_router_v02_digest(self.conn)

    def _expire_due(self, route_id: str, now: datetime) -> None:
        instant = _utc(now)
        for row in self._deadline_rows(route_id):
            if str(row["state"]) != "PENDING":
                continue
            expire_at = _dt_parse(str(row["expire_at"]))
            if instant < expire_at:
                continue
            order = self.lifecycle.reject_order(
                str(row["paper_order_id"]),
                effective_at=expire_at,
                reason=ENTRY_EXECUTION_DEADLINE_EXPIRED,
            )
            with self.conn:
                self.conn.execute(
                    "UPDATE paper_entry_execution_deadlines_v0_1 SET "
                    "state='REJECTED',reason=?,terminal_at=?,updated_at=? "
                    "WHERE route_id=? AND track_id=? AND state='PENDING'",
                    (
                        ENTRY_EXECUTION_DEADLINE_EXPIRED,
                        _dt_text(expire_at),
                        _dt_text(expire_at),
                        route_id,
                        order.track_id,
                    ),
                )
        self._finish_terminal_route(route_id)

    def _finish_terminal_route(self, route_id: str) -> None:
        route = self._require_route(route_id)
        rows = self._deadline_rows(route_id)
        if not rows:
            return
        pending = any(str(row["state"]) == "PENDING" for row in rows)
        route_terminal = route.state in (EntryRouteState.FILLED, EntryRouteState.REJECTED)
        if pending:
            if route_terminal:
                raise RouteDeterminismConflict(
                    "terminal route recovery contains a pending track"
                )
            return
        self._validate_terminal_track_lifecycle(rows)
        filled = tuple(row for row in rows if str(row["state"]) == "FILLED")
        market_terminal = tuple(
            row for row in rows if row["terminal_market_observed_at"] is not None
        )
        if filled:
            if len(filled) != len(market_terminal):
                raise RouteDeterminismConflict(
                    "filled route recovery contains a non-fill market terminal"
                )
            target = EntryRouteState.FILLED
            reason = "PAPER_ENTRY_FILLED_WITH_TRACK_DEADLINE"
        elif market_terminal:
            if any(
                str(row["reason"]) != "ENTRY_SLIPPAGE_CAP_EXCEEDED"
                for row in market_terminal
            ):
                raise RouteDeterminismConflict(
                    "rejected route recovery contains mixed market terminal reasons"
                )
            target = EntryRouteState.REJECTED
            reason = "ENTRY_SLIPPAGE_CAP_EXCEEDED"
        else:
            if any(str(row["reason"]) != ENTRY_EXECUTION_DEADLINE_EXPIRED for row in rows):
                raise RouteDeterminismConflict(
                    "all-expired route recovery contains a non-expiry reason"
                )
            target = EntryRouteState.REJECTED
            reason = ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED
            effective_at = max(_dt_parse(str(row["terminal_at"])) for row in rows)
            market_row = None

        if market_terminal:
            provenance_fields = (
                "terminal_market_observed_at",
                "terminal_market_ingest_seq",
                "terminal_source_event_key",
                "price_impact_bps",
                "signed_market_move_bps",
                "adverse_slippage_bps",
                "slippage_cap_bps",
                "simulated_entry_price_numerator_raw",
                "simulated_entry_price_denominator_raw",
                "route_total_entry_explicit_cost_lamports",
            )
            provenance = {
                tuple(row[field] for field in provenance_fields)
                for row in market_terminal
            }
            if len(provenance) != 1:
                raise RouteDeterminismConflict(
                    "route recovery contains inconsistent terminal market provenance"
                )
            market_row = market_terminal[0]
            if target is EntryRouteState.FILLED:
                if market_row["route_total_entry_explicit_cost_lamports"] is None:
                    raise RouteDeterminismConflict("filled route recovery is missing entry cost")
            elif market_row["route_total_entry_explicit_cost_lamports"] is not None:
                raise RouteDeterminismConflict("rejected route recovery contains entry cost")
            effective_at = _dt_parse(str(market_row["terminal_market_observed_at"]))

        if route_terminal:
            self._validate_terminal_route(
                route=route,
                target=target,
                reason=reason,
                effective_at=effective_at,
                market_row=market_row,
            )
            return
        self._write_terminal_route(
            route=route,
            target=target,
            reason=reason,
            effective_at=effective_at,
            market_row=market_row,
        )

    def _validate_terminal_track_lifecycle(
        self, rows: tuple[sqlite3.Row, ...]
    ) -> None:
        for row in rows:
            order = self.lifecycle.get_order(str(row["paper_order_id"]))
            state = str(row["state"])
            expected_order_state = (
                OrderState.FILLED if state == "FILLED" else OrderState.REJECTED
            )
            if order is None or order.state is not expected_order_state:
                raise RouteDeterminismConflict(
                    "terminal deadline state conflicts with lifecycle order"
                )
            position_count = int(self.conn.execute(
                "SELECT COUNT(*) FROM paper_positions WHERE paper_order_id=?",
                (str(row["paper_order_id"]),),
            ).fetchone()[0])
            if (state == "FILLED" and position_count != 1) or (
                state == "REJECTED" and position_count != 0
            ):
                raise RouteDeterminismConflict(
                    "terminal deadline state conflicts with lifecycle position"
                )

    def _validate_terminal_route(
        self,
        *,
        route: EntryRoute,
        target: EntryRouteState,
        reason: str,
        effective_at: datetime,
        market_row: sqlite3.Row | None,
    ) -> None:
        if route.state is not target or route.state_reason != reason:
            raise RouteDeterminismConflict(
                "terminal route outcome conflicts with execution deadline ledger"
            )
        if route.updated_at != _utc(effective_at):
            raise RouteDeterminismConflict(
                "terminal route timestamp conflicts with execution deadline ledger"
            )
        if market_row is None:
            actual = (
                route.selected_market_observed_at,
                route.selected_market_ingest_seq,
                route.selected_source_event_key,
                route.price_impact_bps,
                route.signed_market_move_bps,
                route.adverse_slippage_bps,
                route.slippage_cap_bps,
                route.simulated_entry_price_numerator_raw,
                route.simulated_entry_price_denominator_raw,
                route.total_entry_explicit_cost_lamports,
            )
            if any(value is not None for value in actual):
                raise RouteDeterminismConflict(
                    "all-expired terminal route contains fabricated market provenance"
                )
            return
        expected = (
            _dt_parse(str(market_row["terminal_market_observed_at"])),
            int(market_row["terminal_market_ingest_seq"]),
            market_row["terminal_source_event_key"],
            int(market_row["price_impact_bps"]),
            int(market_row["signed_market_move_bps"]),
            int(market_row["adverse_slippage_bps"]),
            int(market_row["slippage_cap_bps"]),
            int(market_row["simulated_entry_price_numerator_raw"]),
            int(market_row["simulated_entry_price_denominator_raw"]),
            market_row["route_total_entry_explicit_cost_lamports"],
        )
        actual = (
            route.selected_market_observed_at,
            route.selected_market_ingest_seq,
            route.selected_source_event_key,
            route.price_impact_bps,
            route.signed_market_move_bps,
            route.adverse_slippage_bps,
            route.slippage_cap_bps,
            route.simulated_entry_price_numerator_raw,
            route.simulated_entry_price_denominator_raw,
            route.total_entry_explicit_cost_lamports,
        )
        if actual != expected:
            raise RouteDeterminismConflict(
                "terminal route provenance conflicts with execution deadline ledger"
            )

    def _write_terminal_route(
        self,
        *,
        route: EntryRoute,
        target: EntryRouteState,
        reason: str,
        effective_at: datetime,
        market_row: sqlite3.Row | None,
    ) -> None:
        with self.conn:
            if market_row is None:
                self.conn.execute(
                    "UPDATE paper_entry_routes SET state=?,state_reason=?,updated_at=? "
                    "WHERE route_id=?",
                    (target.value, reason, _dt_text(effective_at), route.route_id),
                )
            else:
                self.conn.execute(
                    """
                    UPDATE paper_entry_routes
                    SET state=?,state_reason=?,updated_at=?,
                        selected_market_observed_at=?,selected_market_ingest_seq=?,
                        selected_source_event_key=?,price_impact_bps=?,
                        signed_market_move_bps=?,adverse_slippage_bps=?,
                        slippage_cap_bps=?,simulated_entry_price_numerator_raw=?,
                        simulated_entry_price_denominator_raw=?,
                        total_entry_explicit_cost_lamports=?
                    WHERE route_id=?
                    """,
                    (
                        target.value,
                        reason,
                        _dt_text(effective_at),
                        str(market_row["terminal_market_observed_at"]),
                        int(market_row["terminal_market_ingest_seq"]),
                        market_row["terminal_source_event_key"],
                        int(market_row["price_impact_bps"]),
                        int(market_row["signed_market_move_bps"]),
                        int(market_row["adverse_slippage_bps"]),
                        int(market_row["slippage_cap_bps"]),
                        str(market_row["simulated_entry_price_numerator_raw"]),
                        str(market_row["simulated_entry_price_denominator_raw"]),
                        market_row["route_total_entry_explicit_cost_lamports"],
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

    def _heal_incomplete_routes(self) -> None:
        route_ids = tuple(
            str(row[0])
            for row in self.conn.execute(
                "SELECT DISTINCT route_id FROM paper_entry_execution_deadlines_v0_1 "
                "ORDER BY route_id"
            ).fetchall()
        )
        for route_id in route_ids:
            self._finish_terminal_route(route_id)

    def _record_market_terminal(
        self,
        order: PaperOrder,
        observation: EntryMarketObservation,
        fill_eval: FillEvaluation,
        *,
        state: str,
        reason: str,
        route_total_entry_explicit_cost_lamports: int | None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_entry_execution_deadlines_v0_1
                SET state=?,reason=?,terminal_at=?,terminal_market_observed_at=?,
                    terminal_market_ingest_seq=?,terminal_source_event_key=?,
                    price_impact_bps=?,signed_market_move_bps=?,adverse_slippage_bps=?,
                    slippage_cap_bps=?,simulated_entry_price_numerator_raw=?,
                    simulated_entry_price_denominator_raw=?,
                    route_total_entry_explicit_cost_lamports=?,updated_at=?
                WHERE paper_order_id=? AND state='PENDING'
                """,
                (
                    state,
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
                    route_total_entry_explicit_cost_lamports,
                    _dt_text(observation.observed_at),
                    order.paper_order_id,
                ),
            )

    def _replay_v02_terminal(
        self,
        route: EntryRoute,
        candidate: CandidateSignalEnvelope,
        observation: EntryMarketObservation,
        price_impact_bps: int,
        observed_priority_fee_lamports: int | None,
        venue_fee_bps: int | None,
    ) -> RouteResult:
        if route.selected_market_observed_at is None:
            if route.state is not EntryRouteState.REJECTED or route.state_reason != ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED:
                raise RouteDeterminismConflict("terminal route missing market provenance")
            return RouteResult(route, self._orders(candidate.candidate_id), None, None)
        return super()._replay_terminal(
            route=route,
            candidate=candidate,
            observation=observation,
            price_impact_bps=price_impact_bps,
            observed_priority_fee_lamports=observed_priority_fee_lamports,
            venue_fee_bps=venue_fee_bps,
        )

    def _deadline_rows(self, route_id: str) -> tuple[sqlite3.Row, ...]:
        return tuple(self.conn.execute(
            "SELECT * FROM paper_entry_execution_deadlines_v0_1 WHERE route_id=? "
            "ORDER BY CASE track_id WHEN 'FINAL-A' THEN 0 WHEN 'FINAL-B' THEN 1 ELSE 2 END",
            (route_id,),
        ).fetchall())

    def _orders(self, candidate_id: str) -> tuple[PaperOrder, ...]:
        orders = tuple(
            self.lifecycle.get_order(_order_id(candidate_id, track_id))
            for track_id in TRACK_ORDER
        )
        if any(order is None for order in orders):
            raise RouteDeterminismConflict("entry route is missing a lifecycle order")
        return tuple(order for order in orders if order is not None)

    def _require_route(self, route_id: str) -> EntryRoute:
        route = self.get_route(route_id)
        if route is None:
            raise RouteDeterminismConflict(f"entry route is missing: {route_id}")
        return route


def canonical_entry_router_v02_digest(conn: sqlite3.Connection) -> str:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "engine_version": ENGINE_VERSION,
        "policy_model_id": DEADLINE_POLICY_MODEL_ID,
        "policy_fingerprint": DEADLINE_POLICY_FINGERPRINT,
        "meta": [dict(row) for row in conn.execute(
            "SELECT * FROM paper_entry_execution_deadline_meta_v0_1 ORDER BY singleton"
        ).fetchall()],
        "decisions": [dict(row) for row in conn.execute(
            "SELECT * FROM paper_entry_execution_deadlines_v0_1 ORDER BY route_id,track_id"
        ).fetchall()],
        "routes": [dict(row) for row in conn.execute(
            "SELECT * FROM paper_entry_routes ORDER BY route_id"
        ).fetchall()],
        "orders": [dict(row) for row in conn.execute(
            "SELECT * FROM paper_orders ORDER BY paper_order_id"
        ).fetchall()],
        "positions": [dict(row) for row in conn.execute(
            "SELECT * FROM paper_positions ORDER BY paper_position_id"
        ).fetchall()],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS paper_entry_execution_deadline_meta_v0_1(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            policy_model_id TEXT NOT NULL,
            policy_fingerprint TEXT NOT NULL,
            router_schema_version TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS paper_entry_execution_deadlines_v0_1(
            route_id TEXT NOT NULL REFERENCES paper_entry_routes(route_id),
            paper_order_id TEXT NOT NULL UNIQUE REFERENCES paper_orders(paper_order_id),
            candidate_id TEXT NOT NULL,
            track_id TEXT NOT NULL CHECK(track_id IN ('FINAL-A','FINAL-B','SENS-C')),
            signal_observed_at TEXT NOT NULL,
            eligible_through_at TEXT NOT NULL,
            expire_at TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('PENDING','FILLED','REJECTED')),
            reason TEXT,
            terminal_at TEXT,
            terminal_market_observed_at TEXT,
            terminal_market_ingest_seq INTEGER,
            terminal_source_event_key TEXT,
            price_impact_bps INTEGER,
            signed_market_move_bps INTEGER,
            adverse_slippage_bps INTEGER,
            slippage_cap_bps INTEGER,
            simulated_entry_price_numerator_raw TEXT,
            simulated_entry_price_denominator_raw TEXT,
            route_total_entry_explicit_cost_lamports INTEGER
                CHECK(route_total_entry_explicit_cost_lamports IS NULL OR
                      route_total_entry_explicit_cost_lamports>=0),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(route_id,track_id),
            CHECK((state='PENDING' AND reason IS NULL AND terminal_at IS NULL) OR
                  (state<>'PENDING' AND reason IS NOT NULL AND terminal_at IS NOT NULL))
        );
        """
    )
    row = conn.execute(
        "SELECT * FROM paper_entry_execution_deadline_meta_v0_1 WHERE singleton=1"
    ).fetchone()
    expected = (DEADLINE_POLICY_MODEL_ID, DEADLINE_POLICY_FINGERPRINT, SCHEMA_VERSION)
    if row is None:
        conn.execute(
            "INSERT INTO paper_entry_execution_deadline_meta_v0_1 VALUES(1,?,?,?)",
            expected,
        )
    else:
        actual = (
            str(row["policy_model_id"]),
            str(row["policy_fingerprint"]),
            str(row["router_schema_version"]),
        )
        if actual != expected:
            raise RouteDeterminismConflict(
                f"entry execution deadline metadata conflict: {actual!r}"
            )
    conn.commit()


def _decision_from_row(row: sqlite3.Row) -> TrackEntryDecision:
    return TrackEntryDecision(
        route_id=str(row["route_id"]),
        paper_order_id=str(row["paper_order_id"]),
        candidate_id=str(row["candidate_id"]),
        track_id=str(row["track_id"]),
        signal_observed_at=_dt_parse(str(row["signal_observed_at"])),
        eligible_through_at=_dt_parse(str(row["eligible_through_at"])),
        expire_at=_dt_parse(str(row["expire_at"])),
        state=str(row["state"]),
        reason=None if row["reason"] is None else str(row["reason"]),
        terminal_at=None if row["terminal_at"] is None else _dt_parse(str(row["terminal_at"])),
        terminal_market_observed_at=(
            None if row["terminal_market_observed_at"] is None
            else _dt_parse(str(row["terminal_market_observed_at"]))
        ),
        terminal_market_ingest_seq=(
            None if row["terminal_market_ingest_seq"] is None
            else int(row["terminal_market_ingest_seq"])
        ),
        terminal_source_event_key=(
            None if row["terminal_source_event_key"] is None
            else str(row["terminal_source_event_key"])
        ),
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


def _dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _dt_parse(value: str) -> datetime:
    return _utc(datetime.fromisoformat(value))
