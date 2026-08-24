from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .paper_continuous_runner_v0_1 import (
    RUNNER_SPEC_FINGERPRINT as V01_RUNNER_SPEC_FINGERPRINT,
    ContinuousPaperRunnerV01,
    RuntimeBindingConflict,
)
from .paper_entry_execution_deadline_v0_1 import (
    MODEL_FINGERPRINT as DEADLINE_POLICY_FINGERPRINT,
    MODEL_ID as DEADLINE_POLICY_MODEL_ID,
)
from .paper_entry_router_v0_1 import EntryRoute, EntryRouteState
from .paper_entry_router_v0_2 import (
    ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED,
    PaperEntryRouterV02,
)
from .paper_exit_fill_executor_v0_1 import PaperExitFillExecutorV01
from .paper_exit_lifecycle_bridge_v0_3 import PaperExitLifecycleBridgeV03
from .paper_exit_orchestrator_v0_1 import LOCKED_EXIT_SPEC_FINGERPRINT
from .paper_lifecycle_v0_1 import PositionState
from .paper_runtime_observability_v0_1 import (
    TerminalDecisionAuditInput,
    TradeOrSkip,
    build_report,
    canonical_report_digest,
)
from .paper_trade_accounting_v0_1 import load_completed_trades


MODEL_ID = "P4-CONTINUOUS-PAPER-RUNNER-0002"
SCHEMA_VERSION = "phase4_continuous_paper_runner_v0.2"
ENGINE_VERSION = "ContinuousPaperRunnerV02"
RUNNER_SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "accepted_runner_v0_1_fingerprint": V01_RUNNER_SPEC_FINGERPRINT,
    "entry_execution_deadline_model_id": DEADLINE_POLICY_MODEL_ID,
    "entry_execution_deadline_fingerprint": DEADLINE_POLICY_FINGERPRINT,
    "exit_spec_fingerprint": LOCKED_EXIT_SPEC_FINGERPRINT,
    "partial_track_entry": True,
    "entry_clock_order": "SOURCE_WATERMARK_DRAIN_THEN_DEADLINE_CLOCK_THEN_EXIT_CLOCK",
    "bounded_end": "NO_FAST_FORWARD_BEYOND_FROZEN_BOUNDARY",
    "paper_only": True,
}
RUNNER_SPEC_FINGERPRINT = hashlib.sha256(
    json.dumps(RUNNER_SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


class ContinuousPaperRunnerV02(ContinuousPaperRunnerV01):
    model_id = MODEL_ID
    spec_fingerprint = RUNNER_SPEC_FINGERPRINT
    schema_version = SCHEMA_VERSION

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        source_identity: str,
        wall_clock=None,
        failure_injector=None,
    ) -> None:
        # The base constructor's restart rebinding assumes three filled tracks.
        # Suppress only that dynamic hook, then install the versioned components
        # and perform the subset-safe durable rebind below.
        self._initializing_v02 = True
        super().__init__(
            conn,
            source_identity=source_identity,
            wall_clock=wall_clock,
            failure_injector=failure_injector,
        )
        self.router = PaperEntryRouterV02(conn, self.cost_model)
        self.exit_bridge = PaperExitLifecycleBridgeV03(
            conn, lifecycle=self.router.lifecycle
        )
        self.exit_executor = PaperExitFillExecutorV01(
            conn,
            self.cost_model,
            lifecycle=self.router.lifecycle,
            exit_orchestrator=self.exit_bridge.exit_orchestrator,
        )
        _create_schema(conn)
        self._bind_v02_runtime()
        self._initializing_v02 = False
        self._rebind_persisted_positions()

    def canonical_digest(self) -> str:
        payload = {
            "model_id": MODEL_ID,
            "spec_fingerprint": RUNNER_SPEC_FINGERPRINT,
            "runtime_v02": [dict(row) for row in self.conn.execute(
                "SELECT * FROM paper_continuous_runtime_state_v0_2 ORDER BY singleton"
            ).fetchall()],
            "runtime_v01_transport": [dict(row) for row in self.conn.execute(
                "SELECT * FROM paper_continuous_runtime_state_v0_1 ORDER BY singleton"
            ).fetchall()],
            "signal_contexts": [dict(row) for row in self.conn.execute(
                "SELECT * FROM paper_continuous_signal_contexts_v0_1 ORDER BY signal_key"
            ).fetchall()],
            "entry_digest": self.router.canonical_digest(),
            "exit_bridge_digest": self.exit_bridge.canonical_digest(),
            "exit_executor_digest": self.exit_executor.canonical_digest(),
            "observability_digest": canonical_report_digest(build_report(self.conn)),
            "completed_trades": [_primitive(item) for item in load_completed_trades(self.conn)],
        }
        return _fingerprint(payload)

    def _process_clock(self, event) -> None:
        now = _utc(event.now)
        self.router.on_clock(now)
        # Heal the expiry -> terminal-audit boundary on exact replay after any
        # interruption, including when no deadline changes on the replay.
        for route in self.router.list_routes():
            if (
                route.state is EntryRouteState.REJECTED
                and route.state_reason == ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED
            ):
                self._ensure_entry_terminal_decision(route)
        super()._process_clock(event)

    def _process_market(self, item, event) -> None:
        super()._process_market(item, event)
        # A late same-mint observation can itself advance all pending entry
        # deadlines.  Such a route has no selected fill observation, so the
        # accepted v0.1 market-healing predicate cannot identify it.  Heal the
        # deadline terminal audit explicitly and idempotently.
        for route in self.router.list_routes():
            if (
                route.mint == event.mint
                and route.state is EntryRouteState.REJECTED
                and route.state_reason == ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED
            ):
                self._ensure_entry_terminal_decision(route)

    def _ensure_entry_terminal_decision(self, route: EntryRoute) -> None:
        if route.state_reason != ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED:
            super()._ensure_entry_terminal_decision(route)
            return
        context = self.conn.execute(
            "SELECT * FROM paper_continuous_signal_contexts_v0_1 WHERE signal_key=?",
            (route.candidate_id,),
        ).fetchone()
        if context is None:
            raise RuntimeBindingConflict(
                f"terminal route {route.route_id} has no continuous signal context"
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
                decision_at_us=_datetime_to_us(route.updated_at),
                trade_or_skip=TradeOrSkip.SKIP,
                decision_reason=ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED,
                candidate_signal_id=route.candidate_id,
                paper_entry_route_id=route.route_id,
                source_evaluation_id=str(context["source_evaluation_id"]),
            )
        )

    def _rebind_persisted_positions(self) -> None:
        if getattr(self, "_initializing_v02", False):
            return
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

    def _bind_v02_runtime(self) -> None:
        row = self.conn.execute(
            "SELECT * FROM paper_continuous_runtime_state_v0_2 WHERE singleton=1"
        ).fetchone()
        expected = (
            MODEL_ID,
            RUNNER_SPEC_FINGERPRINT,
            DEADLINE_POLICY_FINGERPRINT,
            self.source_identity,
        )
        if row is None:
            now = _dt_text(self._now())
            with self.conn:
                self.conn.execute(
                    "INSERT INTO paper_continuous_runtime_state_v0_2 VALUES(1,?,?,?,?,?,?)",
                    expected + (now, now),
                )
            return
        actual = (
            str(row["runner_model_id"]),
            str(row["runner_spec_fingerprint"]),
            str(row["entry_deadline_fingerprint"]),
            str(row["source_identity"]),
        )
        if actual != expected:
            raise RuntimeBindingConflict(
                f"persisted v0.2 runner identity conflict: {actual!r}"
            )


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_continuous_runtime_state_v0_2(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            runner_model_id TEXT NOT NULL,
            runner_spec_fingerprint TEXT NOT NULL,
            entry_deadline_fingerprint TEXT NOT NULL,
            source_identity TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _primitive(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return {key: _primitive(getattr(value, key)) for key in value.__dataclass_fields__}
    if isinstance(value, datetime):
        return _dt_text(value)
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, (tuple, list)):
        return [_primitive(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _primitive(item) for key, item in value.items()}
    return value


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


def _dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _datetime_to_us(value: datetime) -> int:
    return int(_utc(value).timestamp() * 1_000_000)
