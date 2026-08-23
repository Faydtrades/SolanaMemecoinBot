from __future__ import annotations

import sqlite3
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .paper_cost_model_v0_2 import PaperCostModelV02
from .paper_runtime_observability_v0_1 import (
    MODEL_FINGERPRINT as OBSERVABILITY_FINGERPRINT,
    MODEL_ID as OBSERVABILITY_MODEL_ID,
    PaperRuntimeObservabilityV01,
    PositionMarkObservation,
    StrategyEvaluationAuditInput,
    TerminalDecisionAuditInput,
    TradeOrSkip,
)
from .paper_trade_accounting_v0_1 import load_completed_trades


SCHEMA_VERSION = "phase4_live_observability_binding_v0.1"
ENGINE_VERSION = "PaperLiveObservabilityBindingV01"
MODEL_ID = "P4-LIVE-OBSERVABILITY-BINDING-0001"
LOCKED_TRACKS = ("FINAL-A", "FINAL-B", "SENS-C")


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def _primitive(value: Any) -> Any:
    if is_dataclass(value):
        return _primitive(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {str(k): _primitive(v) for k, v in value.items()}
    if isinstance(value, (tuple, list, set)):
        return [_primitive(v) for v in value]
    return value


def _audit_snapshot_dict(value: Any) -> dict[str, Any]:
    primitive = _primitive(value)
    if isinstance(primitive, dict):
        return primitive
    return {"value": primitive}


class PaperLiveObservabilityBindingV01:
    """
    Narrow runtime binding between the live Phase-2 strategy loop, the
    Phase-4 paper execution database, and P4-RUNTIME-OBSERVABILITY-0001.

    This class does not select trades, change parameters, place orders, or
    calculate cross-track portfolio PnL. It only persists already-existing
    strategy/execution facts and causal market marks.
    """

    schema_version = SCHEMA_VERSION
    engine_version = ENGINE_VERSION
    model_id = MODEL_ID
    observability_model_id = OBSERVABILITY_MODEL_ID
    observability_fingerprint = OBSERVABILITY_FINGERPRINT

    def __init__(self, conn: sqlite3.Connection, cost_model: PaperCostModelV02) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.store = PaperRuntimeObservabilityV01(conn, cost_model)

    def record_strategy_evaluation(
        self,
        *,
        run: Any,
        role: str,
        params: Any,
        evaluation: Any,
        ingest_seq: int,
    ) -> str:
        candidate = getattr(evaluation, "candidate_signal", None)
        signal_type = None
        candidate_id = None
        if candidate is not None:
            signal_type = _enum_value(getattr(candidate, "signal_type", "FIRST_PULLBACK"))
            candidate_id = str(getattr(candidate, "signal_id"))

        run_id = str(getattr(run, "run_id"))
        mint = str(getattr(run, "mint"))
        strategy_version = str(getattr(params, "strategy_version"))
        parameter_set_id = str(getattr(params, "parameter_set_id"))
        evaluated_at_us = int(getattr(evaluation, "evaluated_at_us"))
        current_state = _enum_value(getattr(evaluation, "current_state"))
        transition = bool(getattr(evaluation, "transition_occurred"))
        reason = str(getattr(evaluation, "reason_code"))
        passed = tuple(str(x) for x in getattr(evaluation, "filters_passed", ()))
        failed = tuple(str(x) for x in getattr(evaluation, "filters_failed", ()))
        snapshot = _audit_snapshot_dict(getattr(evaluation, "audit_snapshot", {}))

        return self.store.record_strategy_evaluation(
            StrategyEvaluationAuditInput(
                run_id=run_id,
                role=str(role),
                mint=mint,
                strategy_version=strategy_version,
                parameter_set_id=parameter_set_id,
                ingest_seq=int(ingest_seq),
                evaluated_at_us=evaluated_at_us,
                current_state=current_state,
                transition_occurred=transition,
                reason_code=reason,
                filters_passed=passed,
                filters_failed=failed,
                signal_type=signal_type,
                candidate_signal_id=candidate_id,
                audit_snapshot=snapshot,
            )
        )

    def maybe_record_terminal_skip(
        self,
        *,
        run: Any,
        role: str,
        params: Any,
        evaluation: Any,
        source_evaluation_id: str,
    ) -> str | None:
        if not bool(getattr(run, "finished", False)):
            return None
        if getattr(evaluation, "candidate_signal", None) is not None:
            return None

        reason = str(getattr(evaluation, "reason_code"))
        run_id = str(getattr(run, "run_id"))
        return self.store.record_terminal_decision(
            TerminalDecisionAuditInput(
                decision_key=f"ENTRY-RUN:{run_id}:{role}",
                decision_scope="ENTRY_ROLE",
                run_id=run_id,
                role=str(role),
                mint=str(getattr(run, "mint")),
                strategy_version=str(getattr(params, "strategy_version")),
                parameter_set_id=str(getattr(params, "parameter_set_id")),
                decision_at_us=int(getattr(evaluation, "evaluated_at_us")),
                trade_or_skip=TradeOrSkip.SKIP,
                decision_reason=reason,
                candidate_signal_id=None,
                paper_entry_route_id=None,
                source_evaluation_id=source_evaluation_id,
            )
        )

    def record_trade_fill(
        self,
        *,
        run: Any,
        role: str,
        params: Any,
        candidate: Any,
        route_id: str,
        decision_at_us: int,
        source_evaluation_id: str | None,
    ) -> str:
        run_id = str(getattr(run, "run_id"))
        return self.store.record_terminal_decision(
            TerminalDecisionAuditInput(
                decision_key=f"ENTRY-RUN:{run_id}:{role}",
                decision_scope="ENTRY_ROLE",
                run_id=run_id,
                role=str(role),
                mint=str(getattr(run, "mint")),
                strategy_version=str(getattr(params, "strategy_version")),
                parameter_set_id=str(getattr(params, "parameter_set_id")),
                decision_at_us=int(decision_at_us),
                trade_or_skip=TradeOrSkip.TRADE,
                decision_reason="ENTRY_FILLED",
                candidate_signal_id=str(getattr(candidate, "signal_id")),
                paper_entry_route_id=str(route_id),
                source_evaluation_id=source_evaluation_id,
            )
        )

    def record_execution_skip(
        self,
        *,
        run: Any,
        role: str,
        params: Any,
        candidate: Any,
        decision_at_us: int,
        decision_reason: str,
        route_id: str | None,
        source_evaluation_id: str | None,
    ) -> str:
        run_id = str(getattr(run, "run_id"))
        return self.store.record_terminal_decision(
            TerminalDecisionAuditInput(
                decision_key=f"ENTRY-RUN:{run_id}:{role}",
                decision_scope="ENTRY_ROLE",
                run_id=run_id,
                role=str(role),
                mint=str(getattr(run, "mint")),
                strategy_version=str(getattr(params, "strategy_version")),
                parameter_set_id=str(getattr(params, "parameter_set_id")),
                decision_at_us=int(decision_at_us),
                trade_or_skip=TradeOrSkip.SKIP,
                decision_reason=str(decision_reason),
                candidate_signal_id=str(getattr(candidate, "signal_id")),
                paper_entry_route_id=None if route_id is None else str(route_id),
                source_evaluation_id=source_evaluation_id,
            )
        )

    def record_position_marks_for_market(
        self,
        observation: PositionMarkObservation,
    ) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT paper_position_id, track_id, state
            FROM paper_positions
            WHERE mint=? AND price_identity=?
              AND state IN ('OPEN','EXIT_PENDING')
            ORDER BY track_id, paper_position_id
            """,
            (observation.mint, observation.price_identity),
        ).fetchall()
        marks: dict[str, Any] = {}
        for row in rows:
            mark = self.store.record_position_mark(
                str(row["paper_position_id"]),
                observation,
            )
            if mark is not None:
                marks[str(row["track_id"])] = mark
        return marks

    def record_track_equity_for_market(
        self,
        *,
        observed_at: datetime,
        ingest_seq: int,
        source_event_key: str,
    ) -> dict[str, Any]:
        trades = load_completed_trades(self.conn)
        realized_by_track = {
            track: sum(t.net_pnl_lamports for t in trades if t.track_id == track)
            for track in LOCKED_TRACKS
        }

        out: dict[str, Any] = {}
        for track in LOCKED_TRACKS:
            open_rows = self.conn.execute(
                """
                SELECT paper_position_id
                FROM paper_positions
                WHERE track_id=? AND state IN ('OPEN','EXIT_PENDING')
                ORDER BY paper_position_id
                """,
                (track,),
            ).fetchall()

            details: list[tuple[str, int]] = []
            complete = True
            for row in open_rows:
                pid = str(row["paper_position_id"])
                latest = self.conn.execute(
                    """
                    SELECT net_mtm_pnl_lamports
                    FROM paper_position_mtm_marks
                    WHERE paper_position_id=?
                    ORDER BY ingest_seq DESC, observed_at DESC
                    LIMIT 1
                    """,
                    (pid,),
                ).fetchone()
                if latest is None:
                    complete = False
                    break
                details.append((pid, int(latest["net_mtm_pnl_lamports"])))

            if not complete:
                continue

            out[track] = self.store.record_track_equity_mark(
                track_id=track,
                observed_at=observed_at,
                ingest_seq=int(ingest_seq),
                source_event_key=str(source_event_key),
                realized_closed_pnl_lamports=int(realized_by_track[track]),
                open_position_net_mtm=tuple(details),
            )
        return out
