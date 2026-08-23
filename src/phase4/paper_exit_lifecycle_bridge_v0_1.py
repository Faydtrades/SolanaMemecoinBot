from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from .paper_exit_orchestrator_v0_1 import (
    LOCKED_EXIT_SPEC_FINGERPRINT,
    LOCKED_TRACK_ORDER,
    ExitEvaluation,
    ExitMarketObservation,
    ExitPositionRef,
    PaperExitOrchestratorV01,
    PersistedExitTrack,
)
from .paper_lifecycle_v0_1 import (
    OrderState,
    PaperLifecycleStore,
    PaperPosition,
    PositionState,
)


SCHEMA_VERSION = "phase4_paper_exit_lifecycle_bridge_v0.1"
ENGINE_VERSION = "PaperExitLifecycleBridgeV01"


class ExitLifecycleBindingError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ExitBridgeResult:
    refs: tuple[ExitPositionRef, ...]
    evaluations: tuple[ExitEvaluation, ...]
    positions: tuple[PaperPosition, ...]


class PaperExitLifecycleBridgeV01:
    """Bind persisted FILLED entry state to locked Phase-4 exit intent state.

    This bridge owns exactly one boundary:

        persisted OPEN paper positions
            + persisted FILLED shared entry route
            + PaperExitOrchestratorV01 ExitIntent
        -> PaperLifecycleStore EXIT_PENDING

    It does NOT close a position, invent an exit fill, calculate PnL, access RPC,
    use a wallet, sign/broadcast, or tune/reselect exits.

    Position list ordering is explicitly non-semantic. The locked semantic order
    is FINAL-A, FINAL-B, SENS-C.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        lifecycle: PaperLifecycleStore | None = None,
        exit_orchestrator: PaperExitOrchestratorV01 | None = None,
    ) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.lifecycle = lifecycle or PaperLifecycleStore(conn)
        self.exit_orchestrator = exit_orchestrator or PaperExitOrchestratorV01(conn)

    def bind_signal(self, signal_key: str) -> tuple[ExitPositionRef, ...]:
        if not signal_key:
            raise ValueError("signal_key is required")

        route = self._filled_route(signal_key)
        positions = [
            p for p in self.lifecycle.list_open_positions()
            if p.signal_key == signal_key
        ]
        by_track = {p.track_id: p for p in positions}

        if len(positions) != len(LOCKED_TRACK_ORDER) or set(by_track) != set(LOCKED_TRACK_ORDER):
            raise ExitLifecycleBindingError(
                f"signal {signal_key} must have exactly {LOCKED_TRACK_ORDER} OPEN/EXIT_PENDING positions; "
                f"got {tuple(sorted(by_track))}"
            )

        refs: list[ExitPositionRef] = []
        for track_id in LOCKED_TRACK_ORDER:
            position = by_track[track_id]
            order = self.lifecycle.get_order(position.paper_order_id)
            if order is None:
                raise ExitLifecycleBindingError(
                    f"position {position.paper_position_id} missing paper order"
                )
            if order.state is not OrderState.FILLED:
                raise ExitLifecycleBindingError(
                    f"order {order.paper_order_id} is not FILLED: {order.state}"
                )

            self._assert_lineage(route, order, position)

            refs.append(
                ExitPositionRef(
                    paper_position_id=position.paper_position_id,
                    paper_order_id=position.paper_order_id,
                    signal_key=position.signal_key,
                    mint=position.mint,
                    track_id=position.track_id,
                    exit_variant=position.exit_variant,
                    price_identity=position.price_identity,
                    signal_observed_at=order.signal_observed_at,
                    signal_ingest_seq=order.signal_ingest_seq,
                    rule_reference_price_numerator_raw=order.reference_price_numerator_raw,
                    rule_reference_price_denominator_raw=order.reference_price_denominator_raw,
                    entry_market_observed_at=self._dt_parse(route["selected_market_observed_at"]),
                    entry_market_ingest_seq=int(route["selected_market_ingest_seq"]),
                    open_at=position.open_at,
                    entry_price_numerator_raw=position.entry_price_numerator_raw,
                    entry_price_denominator_raw=position.entry_price_denominator_raw,
                )
            )

        bound = self.exit_orchestrator.bind_parallel_positions(tuple(refs))
        if tuple(track.track_id for track in bound) != LOCKED_TRACK_ORDER:
            raise ExitLifecycleBindingError("exit orchestrator did not return semantic locked order")
        return tuple(refs)

    def on_market_observation(
        self,
        signal_key: str,
        observation: ExitMarketObservation,
    ) -> ExitBridgeResult:
        refs = self.bind_signal(signal_key)
        evaluations = self.exit_orchestrator.on_market_observation_parallel(
            refs,
            observation,
        )
        positions = self.apply_evaluations(evaluations)
        return ExitBridgeResult(refs, evaluations, positions)

    def on_clock(self, signal_key: str, now: datetime) -> ExitBridgeResult:
        refs = self.bind_signal(signal_key)
        evaluations = self.exit_orchestrator.on_clock_parallel(refs, now)
        positions = self.apply_evaluations(evaluations)
        return ExitBridgeResult(refs, evaluations, positions)

    def apply_evaluations(
        self,
        evaluations: Sequence[ExitEvaluation],
    ) -> tuple[PaperPosition, ...]:
        by_track: dict[str, PaperPosition] = {}

        for evaluation in evaluations:
            track = evaluation.track
            intent = evaluation.intent
            position = self.lifecycle.get_position(track.paper_position_id)
            if position is None:
                raise ExitLifecycleBindingError(
                    f"exit track {track.paper_position_id} missing lifecycle position"
                )
            if position.track_id != track.track_id:
                raise ExitLifecycleBindingError("exit track / lifecycle track mismatch")

            if intent is not None:
                if intent.paper_position_id != position.paper_position_id:
                    raise ExitLifecycleBindingError("exit intent / position mismatch")
                if position.state is PositionState.CLOSED:
                    raise ExitLifecycleBindingError(
                        "cannot apply an exit intent to a CLOSED position"
                    )
                reason = f"LOCKED_EXIT_INTENT:{intent.reason.value}"
                position = self.lifecycle.request_exit(
                    position.paper_position_id,
                    effective_at=intent.requested_at,
                    reason=reason,
                )

            by_track[position.track_id] = position

        # Evaluations are normally all three tracks. Fail closed if a caller
        # gives an incomplete parallel set rather than returning ambiguous state.
        if set(by_track) != set(LOCKED_TRACK_ORDER):
            raise ExitLifecycleBindingError(
                f"parallel evaluation must cover exactly {LOCKED_TRACK_ORDER}; got {tuple(sorted(by_track))}"
            )
        return tuple(by_track[t] for t in LOCKED_TRACK_ORDER)

    def canonical_digest(self) -> str:
        lifecycle_rows = [dict(row) for row in self.conn.execute(
            "SELECT * FROM paper_positions ORDER BY paper_position_id"
        ).fetchall()]
        lifecycle_events = [dict(row) for row in self.conn.execute(
            "SELECT * FROM paper_lifecycle_events WHERE entity_type='POSITION' ORDER BY event_id"
        ).fetchall()]
        payload = {
            "schema_version": SCHEMA_VERSION,
            "engine_version": ENGINE_VERSION,
            "locked_exit_spec_fingerprint": LOCKED_EXIT_SPEC_FINGERPRINT,
            "exit_digest": self.exit_orchestrator.canonical_digest(),
            "positions": lifecycle_rows,
            "position_events": lifecycle_events,
        }
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    def _filled_route(self, signal_key: str) -> sqlite3.Row:
        rows = self.conn.execute(
            """
            SELECT * FROM paper_entry_routes
            WHERE candidate_id=?
            """,
            (signal_key,),
        ).fetchall()
        if len(rows) != 1:
            raise ExitLifecycleBindingError(
                f"expected exactly one entry route for {signal_key}; found {len(rows)}"
            )
        route = rows[0]
        if route["state"] != "FILLED":
            raise ExitLifecycleBindingError(
                f"entry route for {signal_key} is not FILLED: {route['state']}"
            )
        required = (
            "selected_market_observed_at",
            "selected_market_ingest_seq",
            "simulated_entry_price_numerator_raw",
            "simulated_entry_price_denominator_raw",
        )
        missing = [name for name in required if route[name] is None]
        if missing:
            raise ExitLifecycleBindingError(
                f"FILLED entry route missing causal fill fields: {missing}"
            )
        return route

    def _assert_lineage(self, route: sqlite3.Row, order, position: PaperPosition) -> None:
        if order.signal_key != route["candidate_id"] or position.signal_key != route["candidate_id"]:
            raise ExitLifecycleBindingError("candidate/order/position signal lineage mismatch")
        if order.mint != route["mint"] or position.mint != route["mint"]:
            raise ExitLifecycleBindingError("candidate/order/position mint mismatch")
        if order.track_id != position.track_id or order.exit_variant != position.exit_variant:
            raise ExitLifecycleBindingError("order/position track binding mismatch")
        if order.reference_price_identity != route["reference_price_identity"]:
            raise ExitLifecycleBindingError("order/route price identity mismatch")
        if position.price_identity != route["reference_price_identity"]:
            raise ExitLifecycleBindingError("position/route price identity mismatch")
        if order.reference_price_numerator_raw != int(route["reference_price_numerator_raw"]):
            raise ExitLifecycleBindingError("reference price numerator mismatch")
        if order.reference_price_denominator_raw != int(route["reference_price_denominator_raw"]):
            raise ExitLifecycleBindingError("reference price denominator mismatch")
        if position.entry_price_numerator_raw != int(route["simulated_entry_price_numerator_raw"]):
            raise ExitLifecycleBindingError("position/route simulated entry numerator mismatch")
        if position.entry_price_denominator_raw != int(route["simulated_entry_price_denominator_raw"]):
            raise ExitLifecycleBindingError("position/route simulated entry denominator mismatch")
        if self._dt_text(order.signal_observed_at) != route["signal_observed_at"]:
            raise ExitLifecycleBindingError("signal_observed_at mismatch")
        if order.signal_ingest_seq != int(route["signal_ingest_seq"]):
            raise ExitLifecycleBindingError("signal_ingest_seq mismatch")
        if self._dt_text(position.open_at) != route["selected_market_observed_at"]:
            raise ExitLifecycleBindingError("position open_at must equal selected entry market observation")

    @staticmethod
    def _dt_parse(value: str) -> datetime:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ExitLifecycleBindingError("persisted timestamp must be timezone-aware")
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _dt_text(value: datetime) -> str:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ExitLifecycleBindingError("timestamp must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
