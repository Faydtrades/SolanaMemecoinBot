from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Callable

from . import paper_continuous_firstpullback_binding_v0_1 as binding_v01
from .paper_continuous_firstpullback_binding_v0_1 import (
    GAP_COMPAT_FINGERPRINT,
    LOCKED_SELECTION_SHA256,
    MARKET_SOURCE_FINGERPRINT,
    BindingConflict,
    ContinuousFirstPullbackBindingV01,
)
from .paper_continuous_market_source_v0_1 import ContinuousMarketSourceV01
from .paper_continuous_runner_v0_2 import (
    MODEL_ID as RUNNER_MODEL_ID,
    RUNNER_SPEC_FINGERPRINT,
    ContinuousPaperRunnerV02,
)
from .paper_entry_execution_deadline_v0_1 import (
    MODEL_FINGERPRINT as DEADLINE_POLICY_FINGERPRINT,
)


MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0002"
SCHEMA_VERSION = "phase4_continuous_firstpullback_binding_v0.2"
SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "accepted_binding_v0_1_fingerprint": binding_v01.MODEL_FINGERPRINT,
    "market_source_fingerprint": MARKET_SOURCE_FINGERPRINT,
    "gap_compatibility_fingerprint": GAP_COMPAT_FINGERPRINT,
    "selection_sha256": LOCKED_SELECTION_SHA256,
    "runner_model_id": RUNNER_MODEL_ID,
    "runner_fingerprint": RUNNER_SPEC_FINGERPRINT,
    "entry_execution_deadline_fingerprint": DEADLINE_POLICY_FINGERPRINT,
    "prepared_timer_fence": "DRAIN_CAPTURED_SOURCE_WATERMARK_BEFORE_CLOCK",
    "paper_only": True,
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


class ContinuousFirstPullbackBindingV02(ContinuousFirstPullbackBindingV01):
    model_id = MODEL_ID
    model_fingerprint = MODEL_FINGERPRINT
    schema_version = SCHEMA_VERSION

    def __init__(
        self,
        conn: sqlite3.Connection,
        market_source: ContinuousMarketSourceV01,
        *,
        wall_clock: Callable[[], datetime] | None = None,
        failure_injector=None,
        runner_failure_injector=None,
    ) -> None:
        if market_source.start_after_p1_rowid < 0:
            raise ValueError("market-source anchor must be non-negative")
        if binding_v01.MODEL_FINGERPRINT != (
            "318e15b104c691821f5715b090c934e5b1cb3d9a8357fe0930d8825ef66d0daa"
        ):
            raise BindingConflict("accepted FirstPullback binding v0.1 fingerprint drift")
        if MARKET_SOURCE_FINGERPRINT != (
            "242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb"
        ):
            raise BindingConflict("accepted market-source fingerprint drift")
        if GAP_COMPAT_FINGERPRINT != (
            "aabb765f81a29b2dd119607cdea92a2949dd3acec10aa204d246e6aa7a28a78c"
        ):
            raise BindingConflict("accepted gap-compatibility fingerprint drift")
        if LOCKED_SELECTION_SHA256 != (
            "408657c1d6dc39435b61e01b368dac69796481864b7462efd150a2d4e1b0daa1"
        ):
            raise BindingConflict("locked strategy selection fingerprint drift")
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.market_source = market_source
        self.wall_clock = wall_clock or (lambda: datetime.now(timezone.utc))
        self.failure_injector = failure_injector
        self.params_by_role = binding_v01.locked_parameter_sets()
        self.reconstruction_queries = 0
        binding_v01._create_schema(self.conn)
        self._binding_source_identity = binding_v01._fingerprint({
            "binding_model_id": MODEL_ID,
            "binding_fingerprint": MODEL_FINGERPRINT,
            "market_source_identity": market_source.source_identity,
            "start_after_p1_rowid": market_source.start_after_p1_rowid,
        })
        self._bind_runtime()
        self._verify_prepared_timers()
        self.conn.commit()
        self.runner = ContinuousPaperRunnerV02(
            conn,
            source_identity=self._binding_source_identity,
            wall_clock=self.wall_clock,
            failure_injector=runner_failure_injector,
        )
        self.conn.commit()
        self._engine = self._rebuild_engine()

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
        payload = {
            "model_id": MODEL_ID,
            "fingerprint": MODEL_FINGERPRINT,
            "runner_digest": self.runner.canonical_digest(),
        }
        for table in tables:
            payload[table] = [dict(row) for row in self.conn.execute(
                f"SELECT * FROM {table} ORDER BY rowid"
            ).fetchall()]
        return binding_v01._fingerprint(payload)

    def _bind_runtime(self) -> None:
        row = self.conn.execute(
            "SELECT * FROM paper_fp_binding_runtime_v0_1 WHERE singleton=1"
        ).fetchone()
        expected = (
            MODEL_ID,
            MODEL_FINGERPRINT,
            self.market_source.source_identity,
            self.market_source.start_after_p1_rowid,
            RUNNER_MODEL_ID,
            RUNNER_SPEC_FINGERPRINT,
            self._binding_source_identity,
        )
        if row is None:
            now = binding_v01._dt_text(binding_v01._utc(self.wall_clock()))
            with self.conn:
                self.conn.execute(
                    "INSERT INTO paper_fp_binding_runtime_v0_1("
                    "singleton,binding_model_id,binding_fingerprint,market_source_identity,"
                    "start_after_p1_rowid,last_durable_p1_rowid,runner_model_id,runner_fingerprint,"
                    "runner_source_identity,next_event_sequence,created_at,updated_at) "
                    "VALUES(1,?,?,?,?,?,?,?,?,0,?,?)",
                    (
                        MODEL_ID,
                        MODEL_FINGERPRINT,
                        self.market_source.source_identity,
                        self.market_source.start_after_p1_rowid,
                        self.market_source.start_after_p1_rowid,
                        RUNNER_MODEL_ID,
                        RUNNER_SPEC_FINGERPRINT,
                        self._binding_source_identity,
                        now,
                        now,
                    ),
                )
            return
        actual = (
            str(row["binding_model_id"]),
            str(row["binding_fingerprint"]),
            str(row["market_source_identity"]),
            int(row["start_after_p1_rowid"]),
            str(row["runner_model_id"]),
            str(row["runner_fingerprint"]),
            str(row["runner_source_identity"]),
        )
        if actual != expected:
            raise BindingConflict(
                f"persisted v0.2 binding identity conflict: actual={actual!r} expected={expected!r}"
            )
