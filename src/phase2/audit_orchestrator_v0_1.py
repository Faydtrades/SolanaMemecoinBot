from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, is_dataclass
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Optional

from .feature_engine_v0_1 import FeatureEngineV01
from .models_v0_1 import (
    FirstPullbackParameterSet,
    FirstPullbackState,
    MarketState,
    NormalizedMarketEvent,
    StrategyEvaluation,
    StrategyRunState,
)
from .strategy_first_pullback_v0_1 import FirstPullbackStrategyV01


def _primitive(value: Any) -> Any:
    if is_dataclass(value):
        return _primitive(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {
            str(k): _primitive(v)
            for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_primitive(v) for v in value]
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(_primitive(value), sort_keys=True, separators=(",", ":"))


def _stable_id(prefix: str, *parts: object) -> str:
    payload = "|".join(str(p) for p in parts)
    digest = sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}-{digest}"


@dataclass(frozen=True, slots=True)
class OrchestrationResult:
    market_state: MarketState
    strategy_run: StrategyRunState
    evaluation: StrategyEvaluation


class Phase2AuditStoreV01:
    """Small persistence adapter for Phase-2 strategy audit data.

    It is deliberately separate from strategy logic. The store can point at an isolated
    self-test SQLite database, an in-memory database, or a future Phase-2 research DB.
    The production Phase-1 database is not opened by this module unless a caller
    explicitly passes that path (the self-test never does).
    """

    schema_version = "AUDIT-0.1"

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_schema()

    def _create_schema(self) -> None:
        self.conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS strategy_runs (
                run_id TEXT PRIMARY KEY,
                mint TEXT NOT NULL,
                strategy_name TEXT NOT NULL,
                strategy_version TEXT NOT NULL,
                parameter_set_id TEXT NOT NULL,
                started_at_us INTEGER NOT NULL,
                last_evaluated_at_us INTEGER NOT NULL,
                current_state TEXT NOT NULL,
                finished INTEGER NOT NULL CHECK (finished IN (0, 1)),
                final_outcome TEXT,
                audit_schema_version TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS state_transitions (
                transition_id TEXT PRIMARY KEY,
                evaluation_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                mint TEXT NOT NULL,
                event_key TEXT NOT NULL,
                event_at_us INTEGER NOT NULL,
                observed_at_us INTEGER NOT NULL,
                from_state TEXT NOT NULL,
                to_state TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                market_state_version TEXT NOT NULL,
                feature_version TEXT NOT NULL,
                feature_snapshot_json TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS decision_records (
                decision_id TEXT PRIMARY KEY,
                evaluation_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                mint TEXT NOT NULL,
                event_key TEXT NOT NULL,
                event_at_us INTEGER NOT NULL,
                observed_at_us INTEGER NOT NULL,
                strategy_version TEXT NOT NULL,
                parameter_set_id TEXT NOT NULL,
                state TEXT NOT NULL,
                signal TEXT NOT NULL,
                signal_reason TEXT NOT NULL,
                filters_passed_json TEXT NOT NULL,
                filters_failed_json TEXT NOT NULL,
                candidate_score TEXT,
                risk_decision TEXT NOT NULL,
                trade_or_skip TEXT NOT NULL,
                skip_reason TEXT,
                market_state_version TEXT NOT NULL,
                feature_version TEXT NOT NULL,
                feature_snapshot_json TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_strategy_runs_mint
                ON strategy_runs(mint);
            CREATE INDEX IF NOT EXISTS idx_state_transitions_run
                ON state_transitions(run_id, observed_at_us);
            CREATE INDEX IF NOT EXISTS idx_decision_records_run
                ON decision_records(run_id, observed_at_us);
            """
        )
        self.conn.commit()

    def upsert_run(self, run: StrategyRunState, evaluated_at_us: int) -> None:
        final_outcome = run.final_outcome.value if run.final_outcome else None
        self.conn.execute(
            """
            INSERT INTO strategy_runs (
                run_id, mint, strategy_name, strategy_version, parameter_set_id,
                started_at_us, last_evaluated_at_us, current_state, finished,
                final_outcome, audit_schema_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                last_evaluated_at_us=excluded.last_evaluated_at_us,
                current_state=excluded.current_state,
                finished=excluded.finished,
                final_outcome=excluded.final_outcome,
                audit_schema_version=excluded.audit_schema_version
            """,
            (
                run.run_id,
                run.mint,
                run.strategy_name,
                run.strategy_version,
                run.parameter_set_id,
                run.started_at_us,
                evaluated_at_us,
                run.current_state.value,
                1 if run.finished else 0,
                final_outcome,
                self.schema_version,
            ),
        )

    def record_transition(
        self,
        event: NormalizedMarketEvent,
        state: MarketState,
        evaluation: StrategyEvaluation,
        *,
        feature_version: str,
    ) -> None:
        if not evaluation.transition_occurred:
            return
        transition_id = _stable_id("p2trans", evaluation.evaluation_id)
        self.conn.execute(
            """
            INSERT OR IGNORE INTO state_transitions (
                transition_id, evaluation_id, run_id, mint, event_key,
                event_at_us, observed_at_us, from_state, to_state, reason_code,
                market_state_version, feature_version, feature_snapshot_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transition_id,
                evaluation.evaluation_id,
                evaluation.run_id,
                evaluation.mint,
                event.event_key,
                event.event_at_us,
                evaluation.evaluated_at_us,
                evaluation.previous_state.value,
                evaluation.current_state.value,
                evaluation.reason_code,
                state.schema_version,
                feature_version,
                _canonical_json(evaluation.audit_snapshot),
            ),
        )

    def record_decision(
        self,
        event: NormalizedMarketEvent,
        state: MarketState,
        run: StrategyRunState,
        evaluation: StrategyEvaluation,
        *,
        feature_version: str,
    ) -> bool:
        """Persist signal/skip decision points only, never every market event."""
        terminal_skip_states = {
            FirstPullbackState.REJECT,
            FirstPullbackState.INVALIDATED,
            FirstPullbackState.EXPIRED,
        }

        if evaluation.candidate_signal is not None:
            trade_or_skip = "PENDING_RISK"
            risk_decision = "NOT_EVALUATED_PHASE2"
            skip_reason: Optional[str] = None
            signal = evaluation.candidate_signal.signal_type
        elif evaluation.current_state in terminal_skip_states:
            trade_or_skip = "SKIP"
            risk_decision = "NOT_APPLICABLE"
            skip_reason = evaluation.reason_code
            signal = "NONE"
        else:
            return False

        decision_id = _stable_id(
            "p2decision",
            evaluation.evaluation_id,
            trade_or_skip,
            evaluation.reason_code,
        )
        self.conn.execute(
            """
            INSERT OR IGNORE INTO decision_records (
                decision_id, evaluation_id, run_id, mint, event_key,
                event_at_us, observed_at_us, strategy_version, parameter_set_id,
                state, signal, signal_reason, filters_passed_json,
                filters_failed_json, candidate_score, risk_decision,
                trade_or_skip, skip_reason, market_state_version,
                feature_version, feature_snapshot_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_id,
                evaluation.evaluation_id,
                evaluation.run_id,
                evaluation.mint,
                event.event_key,
                event.event_at_us,
                evaluation.evaluated_at_us,
                run.strategy_version,
                run.parameter_set_id,
                evaluation.current_state.value,
                signal,
                evaluation.reason_code,
                _canonical_json(evaluation.filters_passed),
                _canonical_json(evaluation.filters_failed),
                None,  # Candidate scoring is intentionally not implemented in v0.1.
                risk_decision,
                trade_or_skip,
                skip_reason,
                state.schema_version,
                feature_version,
                _canonical_json(evaluation.audit_snapshot),
            ),
        )
        return True

    def commit(self) -> None:
        self.conn.commit()

    def count(self, table: str) -> int:
        if table not in {"strategy_runs", "state_transitions", "decision_records"}:
            raise ValueError(f"Unsupported table: {table}")
        row = self.conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        return int(row["n"])

    def fetch_one(self, query: str, args: Iterable[object] = ()) -> Optional[sqlite3.Row]:
        return self.conn.execute(query, tuple(args)).fetchone()

    def export_table(self, table: str) -> list[dict[str, Any]]:
        if table not in {"strategy_runs", "state_transitions", "decision_records"}:
            raise ValueError(f"Unsupported table: {table}")
        pk = {
            "strategy_runs": "run_id",
            "state_transitions": "transition_id",
            "decision_records": "decision_id",
        }[table]
        rows = self.conn.execute(f"SELECT * FROM {table} ORDER BY {pk}").fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()


class Phase2AuditOrchestratorV01:
    """Wires event -> Feature Engine -> MarketState -> Strategy -> Audit Store.

    The strategy remains persistence-agnostic. This class owns the side-effect boundary.
    """

    feature_version = "FE-0.1"
    orchestrator_version = "ORCH-0.1"

    def __init__(
        self,
        params: FirstPullbackParameterSet,
        audit_store: Phase2AuditStoreV01,
    ) -> None:
        FirstPullbackStrategyV01.validate_parameters(params)
        self.params = params
        self.audit_store = audit_store
        self.feature_engine = FeatureEngineV01()
        self.strategy = FirstPullbackStrategyV01()
        self.runs: dict[str, StrategyRunState] = {}

    def process(self, event: NormalizedMarketEvent) -> OrchestrationResult:
        state = self.feature_engine.process(event)
        run = self.runs.get(event.mint)
        if run is None:
            run = self.strategy.start_run(state, self.params)
            self.runs[event.mint] = run

        evaluation = self.strategy.evaluate(state, run, self.params)

        # Parent row first so foreign-key children can be persisted safely.
        self.audit_store.upsert_run(run, evaluation.evaluated_at_us)
        self.audit_store.record_transition(
            event,
            state,
            evaluation,
            feature_version=self.feature_version,
        )
        self.audit_store.record_decision(
            event,
            state,
            run,
            evaluation,
            feature_version=self.feature_version,
        )
        self.audit_store.commit()

        return OrchestrationResult(
            market_state=state,
            strategy_run=run,
            evaluation=evaluation,
        )
