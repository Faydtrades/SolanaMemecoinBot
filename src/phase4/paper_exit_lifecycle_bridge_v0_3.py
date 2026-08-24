from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from typing import Sequence

from .paper_exit_lifecycle_bridge_v0_2 import (
    ExitBridgeResult,
    ExitLifecycleBindingError,
    PaperExitLifecycleBridgeV02,
)
from .paper_exit_orchestrator_v0_1 import (
    LOCKED_EXIT_SPEC_FINGERPRINT,
    LOCKED_TRACK_ORDER,
    ExitEvaluation,
    ExitMarketObservation,
    ExitPositionRef,
    PaperExitOrchestratorV01,
)
from .paper_lifecycle_v0_1 import (
    OrderState,
    PaperLifecycleStore,
    PaperPosition,
    PositionState,
)


SCHEMA_VERSION = "phase4_paper_exit_lifecycle_bridge_v0.3"
ENGINE_VERSION = "PaperExitLifecycleBridgeV03"


class PaperExitLifecycleBridgeV03(PaperExitLifecycleBridgeV02):
    """Exit binding for the non-empty subset of tracks that actually opened."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        lifecycle: PaperLifecycleStore | None = None,
        exit_orchestrator: PaperExitOrchestratorV01 | None = None,
    ) -> None:
        super().__init__(
            conn,
            lifecycle=lifecycle,
            exit_orchestrator=exit_orchestrator,
        )

    def bind_signal(self, signal_key: str) -> tuple[ExitPositionRef, ...]:
        if not signal_key:
            raise ValueError("signal_key is required")
        route = self._filled_route(signal_key)
        positions: list[PaperPosition] = []
        for row in self.conn.execute(
            "SELECT paper_position_id FROM paper_positions WHERE signal_key=?",
            (signal_key,),
        ).fetchall():
            position = self.lifecycle.get_position(str(row["paper_position_id"]))
            if position is None:
                raise ExitLifecycleBindingError(
                    f"signal {signal_key} references a missing lifecycle position"
                )
            positions.append(position)
        by_track = {position.track_id: position for position in positions}
        if not by_track or len(by_track) != len(positions):
            raise ExitLifecycleBindingError(
                f"signal {signal_key} must have a non-empty unique opened-track set"
            )
        if not set(by_track).issubset(LOCKED_TRACK_ORDER):
            raise ExitLifecycleBindingError("signal contains an unsupported exit track")

        refs: list[ExitPositionRef] = []
        for track_id in LOCKED_TRACK_ORDER:
            position = by_track.get(track_id)
            if position is None:
                continue
            order = self.lifecycle.get_order(position.paper_order_id)
            if order is None or order.state is not OrderState.FILLED:
                raise ExitLifecycleBindingError(
                    f"opened track {track_id} does not have a FILLED entry order"
                )
            self._assert_lineage(route, order, position)
            ref = ExitPositionRef(
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
                entry_market_observed_at=self._dt_parse(
                    str(route["selected_market_observed_at"])
                ),
                entry_market_ingest_seq=int(route["selected_market_ingest_seq"]),
                open_at=position.open_at,
                entry_price_numerator_raw=position.entry_price_numerator_raw,
                entry_price_denominator_raw=position.entry_price_denominator_raw,
            )
            self.exit_orchestrator.bind_position(ref)
            refs.append(ref)
        return tuple(refs)

    def on_market_observation(
        self,
        signal_key: str,
        observation: ExitMarketObservation,
    ) -> ExitBridgeResult:
        refs = self.bind_signal(signal_key)
        evaluations = tuple(
            self.exit_orchestrator.on_market_observation(ref, observation)
            for ref in refs
        )
        return ExitBridgeResult(refs, evaluations, self.apply_evaluations(evaluations))

    def on_clock(self, signal_key: str, now: datetime) -> ExitBridgeResult:
        refs = self.bind_signal(signal_key)
        evaluations = tuple(
            self.exit_orchestrator.on_clock(ref, now) for ref in refs
        )
        return ExitBridgeResult(refs, evaluations, self.apply_evaluations(evaluations))

    def apply_evaluations(
        self,
        evaluations: Sequence[ExitEvaluation],
    ) -> tuple[PaperPosition, ...]:
        if not evaluations:
            raise ExitLifecycleBindingError("exit evaluation subset must not be empty")
        signal_keys = {evaluation.track.signal_key for evaluation in evaluations}
        if len(signal_keys) != 1:
            raise ExitLifecycleBindingError("exit evaluation subset crosses signal lineages")
        signal_key = next(iter(signal_keys))
        expected_tracks = {
            str(row["track_id"])
            for row in self.conn.execute(
                "SELECT track_id FROM paper_positions WHERE signal_key=?",
                (signal_key,),
            ).fetchall()
        }
        supplied_tracks = {evaluation.track.track_id for evaluation in evaluations}
        if supplied_tracks != expected_tracks:
            raise ExitLifecycleBindingError(
                "exit evaluation subset does not cover every opened track"
            )
        by_track: dict[str, PaperPosition] = {}
        for evaluation in evaluations:
            track = evaluation.track
            if track.track_id not in LOCKED_TRACK_ORDER or track.track_id in by_track:
                raise ExitLifecycleBindingError("invalid or duplicate exit evaluation track")
            position = self.lifecycle.get_position(track.paper_position_id)
            if position is None or position.track_id != track.track_id:
                raise ExitLifecycleBindingError("exit track / lifecycle position mismatch")
            intent = evaluation.intent
            if intent is not None:
                if intent.paper_position_id != position.paper_position_id:
                    raise ExitLifecycleBindingError("exit intent / position mismatch")
                if position.state is PositionState.CLOSED:
                    if not position.state_reason.startswith("PAPER_EXIT_FILLED:"):
                        raise ExitLifecycleBindingError(
                            "CLOSED position lacks an exit-fill terminal reason"
                        )
                else:
                    position = self.lifecycle.request_exit(
                        position.paper_position_id,
                        effective_at=intent.requested_at,
                        reason=f"LOCKED_EXIT_INTENT:{intent.reason.value}",
                    )
            by_track[track.track_id] = position
        return tuple(by_track[t] for t in LOCKED_TRACK_ORDER if t in by_track)

    def canonical_digest(self) -> str:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "engine_version": ENGINE_VERSION,
            "locked_exit_spec_fingerprint": LOCKED_EXIT_SPEC_FINGERPRINT,
            "exit_digest": self.exit_orchestrator.canonical_digest(),
            "positions": [dict(row) for row in self.conn.execute(
                "SELECT * FROM paper_positions ORDER BY paper_position_id"
            ).fetchall()],
            "position_events": [dict(row) for row in self.conn.execute(
                "SELECT * FROM paper_lifecycle_events WHERE entity_type='POSITION' "
                "ORDER BY event_id"
            ).fetchall()],
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
