from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Mapping

from .shadow_continuous_source_bridge_v0_1 import (
    SHADOW_DATA_ROOT,
    Phase4SourceIdentityV01,
    SourceBridgeError,
    open_phase4_read_only,
    utc_text_to_epoch_us,
)
from .shadow_domain_v0_1 import (
    TERMINAL_STATES,
    ExecutionIntentV01,
    IntentRole,
    IntentSide,
    ShadowDeterminismConflict,
    ShadowState,
    canonical_json,
    content_fingerprint,
    deterministic_id,
)
from .shadow_repository_v0_1 import ShadowRepositoryV01, open_shadow_repository
from .shadow_simulation_repository_v0_1 import open_simulation_repository
from .shadow_unsigned_plan_simulation_v0_1 import (
    ShadowSimulationResultV01,
    SimulationOutcome,
)
from .shadow_venue_repository_v0_1 import open_venue_repository
from .shadow_venue_route_quote_v0_1 import ExecutableQuoteV01


MODEL_ID = "P5-SHADOW-LIFECYCLE-BRIDGE-0001"
SCHEMA_VERSION = "phase5_shadow_lifecycle_bridge_v0.1"
INVENTORY_SCHEMA_VERSION = "phase5_expected_inventory_v0.1"
EXIT_EVIDENCE_SCHEMA_VERSION = "phase5_shadow_exit_source_evidence_v0.1"
POSITION_SCHEMA_VERSION = "phase5_expected_track_position_v0.1"
EXPECTED_CLASSIFICATION = "EXPECTED_SIMULATED_ONLY"
NO_POSITION_CLASSIFICATION = "NO_SHADOW_ENTRY_POSITION"
EXIT_INTENT_CLASSIFICATION = "SHADOW_EXIT_INTENT"
LOCKED_TRACKS = frozenset({"FINAL-A", "FINAL-B", "SENS-C"})
LOCKED_PAPER_REASONS = frozenset({"TAKE_PROFIT", "TRAIL", "FALLBACK"})
NEGATIVE_ENTRY_STATES = frozenset(
    {ShadowState.REJECTED, ShadowState.EXPIRED, ShadowState.FAILED}
)

CONTRACT_SPEC: Mapping[str, Any] = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "entry_cardinality": "ONE_SHARED_ENTRY_PER_PHYSICAL_CANDIDATE",
    "inventory": {
        "classification": EXPECTED_CLASSIFICATION,
        "amount_source": "ACCEPTED_BUY_QUOTE_EXPECTED_BASE_AMOUNT",
        "identity_source": "PARENT_ENTRY_INTENT_ID",
        "custody_claim": False,
        "wallet_balance_claim": False,
        "actual_fill_claim": False,
    },
    "exit_source": (
        "paper_exit_intents JOIN paper_exit_track_states "
        "JOIN paper_continuous_signal_contexts_v0_1"
    ),
    "exit_order": ["requested_at", "exit_intent_id"],
    "exit_lanes": ["FINAL-A", "FINAL-B", "SENS-C"],
    "exit_input_asset_unit": "MEME_BASE_UNITS",
    "exit_amount": "FULL_EXPECTED_INVENTORY_PER_HYPOTHETICAL_TRACK",
    "no_position_classification": NO_POSITION_CLASSIFICATION,
    "shadow_database": "EXISTING_T004A_BOUND_DATABASE_ONLY",
    "source_access": "SQLITE_URI_MODE_RO_AND_QUERY_ONLY",
    "capabilities": ["READ_PHASE4", "WRITE_SHADOW_EVIDENCE"],
}
MODEL_FINGERPRINT = content_fingerprint(CONTRACT_SPEC)

_EXIT_SQL = """
SELECT
    e.exit_intent_id AS exit_intent_id,
    e.paper_position_id AS exit_paper_position_id,
    e.paper_order_id AS exit_paper_order_id,
    e.signal_key AS exit_signal_key,
    e.mint AS exit_mint,
    e.track_id AS exit_track_id,
    e.exit_variant AS exit_variant,
    e.reason AS exit_reason,
    e.requested_at AS requested_at,
    e.trigger_observed_at AS trigger_observed_at,
    e.trigger_ingest_seq AS trigger_ingest_seq,
    e.trigger_source_event_key AS trigger_source_event_key,
    e.trigger_price_numerator_raw AS trigger_price_numerator_raw,
    e.trigger_price_denominator_raw AS trigger_price_denominator_raw,
    e.rule_return_bps AS rule_return_bps,
    e.trail_peak_return_bps AS trail_peak_return_bps,
    e.last_fresh_observed_at AS last_fresh_observed_at,
    e.last_fresh_ingest_seq AS last_fresh_ingest_seq,
    e.last_fresh_return_bps AS last_fresh_return_bps,
    t.paper_position_id AS track_paper_position_id,
    t.paper_order_id AS track_paper_order_id,
    t.signal_key AS track_signal_key,
    t.mint AS track_mint,
    t.track_id AS track_id,
    t.exit_variant AS track_exit_variant,
    t.signal_ingest_seq AS track_signal_ingest_seq,
    t.state AS track_state,
    t.exit_intent_id AS track_exit_intent_id,
    ce.signal_key AS exit_context_signal_key,
    ce.run_id AS exit_context_run_id,
    ce.mint AS exit_context_mint,
    ct.signal_key AS track_context_signal_key,
    ct.run_id AS track_context_run_id,
    ct.mint AS track_context_mint
FROM paper_exit_intents AS e
LEFT JOIN paper_exit_track_states AS t
    ON t.paper_position_id = e.paper_position_id
LEFT JOIN paper_continuous_signal_contexts_v0_1 AS ce
    ON ce.signal_key = e.signal_key
LEFT JOIN paper_continuous_signal_contexts_v0_1 AS ct
    ON ct.signal_key = t.signal_key
WHERE (ce.run_id = ? OR ct.run_id = ?)
  AND (
      e.requested_at > ?
      OR (e.requested_at = ? AND e.exit_intent_id > ?)
  )
ORDER BY e.requested_at, e.exit_intent_id
LIMIT ?
"""

_REQUIRED_EXIT_COLUMNS = frozenset(
    {
        "exit_intent_id",
        "paper_position_id",
        "paper_order_id",
        "signal_key",
        "mint",
        "track_id",
        "exit_variant",
        "reason",
        "requested_at",
        "trigger_observed_at",
        "trigger_ingest_seq",
        "trigger_source_event_key",
        "trigger_price_numerator_raw",
        "trigger_price_denominator_raw",
        "rule_return_bps",
        "trail_peak_return_bps",
        "last_fresh_observed_at",
        "last_fresh_ingest_seq",
        "last_fresh_return_bps",
    }
)
_REQUIRED_TRACK_COLUMNS = frozenset(
    {
        "paper_position_id",
        "paper_order_id",
        "signal_key",
        "mint",
        "track_id",
        "exit_variant",
        "signal_ingest_seq",
        "state",
        "exit_intent_id",
    }
)


class LifecycleBridgeError(RuntimeError):
    pass


class InventoryConflict(LifecycleBridgeError):
    pass


class ExitSourceConflict(LifecycleBridgeError):
    pass


class ExitPollStatus(StrEnum):
    IDLE = "IDLE"
    PROCESSED = "PROCESSED"
    WAITING_FOR_ENTRY_TERMINAL = "WAITING_FOR_ENTRY_TERMINAL"


@dataclass(frozen=True, slots=True)
class ExpectedInventoryV01:
    inventory_id: str
    parent_entry_intent_id: str
    mint: str
    expected_base_amount: int
    quote_id: str
    quote_fingerprint: str
    simulation_result_id: str
    simulation_result_fingerprint: str
    evidence_at_us: int
    classification: str
    fingerprint: str

    @classmethod
    def create(
        cls,
        *,
        entry_intent: ExecutionIntentV01,
        quote: ExecutableQuoteV01,
        simulation_result: ShadowSimulationResultV01,
        evidence_at_us: int,
    ) -> ExpectedInventoryV01:
        amount = _strict_int(
            "expected_base_amount", quote.expected_base_amount, positive=True
        )
        evidence_time = _strict_int("evidence_at_us", evidence_at_us)
        payload = {
            "schema_version": INVENTORY_SCHEMA_VERSION,
            "parent_entry_intent_id": entry_intent.intent_id,
            "mint": entry_intent.mint,
            "expected_base_amount": amount,
            "quote_id": quote.quote_id,
            "quote_fingerprint": quote.fingerprint,
            "simulation_result_id": simulation_result.result_id,
            "simulation_result_fingerprint": simulation_result.fingerprint,
            "evidence_at_us": evidence_time,
            "classification": EXPECTED_CLASSIFICATION,
        }
        return cls(
            inventory_id=deterministic_id(
                "P5INV", INVENTORY_SCHEMA_VERSION, entry_intent.intent_id
            ),
            parent_entry_intent_id=entry_intent.intent_id,
            mint=entry_intent.mint,
            expected_base_amount=amount,
            quote_id=quote.quote_id,
            quote_fingerprint=quote.fingerprint,
            simulation_result_id=simulation_result.result_id,
            simulation_result_fingerprint=simulation_result.fingerprint,
            evidence_at_us=evidence_time,
            classification=EXPECTED_CLASSIFICATION,
            fingerprint=content_fingerprint(payload),
        )

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": INVENTORY_SCHEMA_VERSION,
            "parent_entry_intent_id": self.parent_entry_intent_id,
            "mint": self.mint,
            "expected_base_amount": self.expected_base_amount,
            "quote_id": self.quote_id,
            "quote_fingerprint": self.quote_fingerprint,
            "simulation_result_id": self.simulation_result_id,
            "simulation_result_fingerprint": self.simulation_result_fingerprint,
            "evidence_at_us": self.evidence_at_us,
            "classification": self.classification,
        }


@dataclass(frozen=True, slots=True)
class Phase4ExitSourceV01:
    exit_intent_id: str
    paper_position_id: str
    paper_order_id: str
    signal_key: str
    mint: str
    track_id: str
    exit_variant: str
    reason: str
    requested_at: str
    requested_at_us: int
    trigger_observed_at: str | None
    trigger_ingest_seq: int | None
    trigger_source_event_key: str | None
    trigger_price_numerator_raw: str | None
    trigger_price_denominator_raw: str | None
    rule_return_bps: int | None
    trail_peak_return_bps: int | None
    last_fresh_observed_at: str | None
    last_fresh_ingest_seq: int | None
    last_fresh_return_bps: int | None
    track_signal_ingest_seq: int

    @property
    def ordering_key(self) -> tuple[str, str]:
        return (self.requested_at, self.exit_intent_id)


@dataclass(frozen=True, slots=True)
class ExitPollResultV01:
    status: ExitPollStatus
    processed_rows: int
    exit_intents: int
    no_position_rows: int
    last_requested_at: str
    last_exit_intent_id: str


FaultHook = Callable[[str, Phase4ExitSourceV01], None]


def _required_text(label: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ExitSourceConflict(f"{label} is required")
    return value


def _optional_text(label: str, value: Any) -> str | None:
    if value is None:
        return None
    return _required_text(label, value)


def _strict_int(label: str, value: Any, *, positive: bool = False) -> int:
    if type(value) is not int:
        raise LifecycleBridgeError(f"{label} must be an integer")
    if positive and value <= 0:
        raise LifecycleBridgeError(f"{label} must be > 0")
    if not positive and value < 0:
        raise LifecycleBridgeError(f"{label} must be >= 0")
    return value


def _optional_int(label: str, value: Any) -> int | None:
    if value is None:
        return None
    return _strict_int(label, value)


def deterministic_track_position_id(
    parent_entry_intent_id: str,
    track_id: str,
) -> str:
    return deterministic_id(
        "P5POS", POSITION_SCHEMA_VERSION, parent_entry_intent_id, track_id
    )


def _table_columns(conn: sqlite3.Connection, table: str) -> frozenset[str]:
    return frozenset(str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})"))


def _verify_exit_schema(conn: sqlite3.Connection) -> None:
    if not _REQUIRED_EXIT_COLUMNS.issubset(
        _table_columns(conn, "paper_exit_intents")
    ):
        raise ExitSourceConflict("Phase4 exit-intent schema is incompatible")
    if not _REQUIRED_TRACK_COLUMNS.issubset(
        _table_columns(conn, "paper_exit_track_states")
    ):
        raise ExitSourceConflict("Phase4 exit-track schema is incompatible")


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS shadow_t004b_schema_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            model_id TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            model_fingerprint TEXT NOT NULL,
            inventory_schema_version TEXT NOT NULL,
            exit_evidence_schema_version TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS shadow_t004b_expected_inventory (
            inventory_id TEXT PRIMARY KEY,
            parent_entry_intent_id TEXT NOT NULL UNIQUE
                REFERENCES shadow_execution_intents(intent_id),
            mint TEXT NOT NULL,
            expected_base_amount TEXT NOT NULL CHECK(
                expected_base_amount <> ''
                AND expected_base_amount <> '0'
                AND expected_base_amount NOT GLOB '*[^0-9]*'
            ),
            quote_id TEXT NOT NULL UNIQUE REFERENCES shadow_t002_quotes(quote_id),
            quote_fingerprint TEXT NOT NULL,
            simulation_result_id TEXT NOT NULL UNIQUE
                REFERENCES shadow_t003_results(result_id),
            simulation_result_fingerprint TEXT NOT NULL,
            evidence_at_us INTEGER NOT NULL CHECK(evidence_at_us >= 0),
            classification TEXT NOT NULL
                CHECK(classification='EXPECTED_SIMULATED_ONLY'),
            inventory_json TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS shadow_t004b_exit_source_evidence (
            evidence_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL REFERENCES shadow_t004a_sources(source_id),
            candidate_run_id TEXT NOT NULL,
            requested_at TEXT NOT NULL,
            exit_intent_id TEXT NOT NULL,
            paper_position_id TEXT NOT NULL,
            signal_key TEXT NOT NULL,
            track_id TEXT NOT NULL,
            parent_entry_intent_id TEXT NOT NULL
                REFERENCES shadow_execution_intents(intent_id),
            classification TEXT NOT NULL CHECK(classification IN (
                'SHADOW_EXIT_INTENT','NO_SHADOW_ENTRY_POSITION'
            )),
            shadow_exit_intent_id TEXT REFERENCES shadow_execution_intents(intent_id),
            terminal_parent_state TEXT,
            evidence_json TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            UNIQUE(source_id,candidate_run_id,exit_intent_id),
            UNIQUE(shadow_exit_intent_id),
            CHECK(
                (classification='SHADOW_EXIT_INTENT'
                    AND shadow_exit_intent_id IS NOT NULL
                    AND terminal_parent_state IS NULL)
                OR
                (classification='NO_SHADOW_ENTRY_POSITION'
                    AND shadow_exit_intent_id IS NULL
                    AND terminal_parent_state IN ('REJECTED','EXPIRED','FAILED'))
            )
        );

        CREATE TABLE IF NOT EXISTS shadow_t004b_exit_cursors (
            source_id TEXT PRIMARY KEY REFERENCES shadow_t004a_sources(source_id),
            candidate_run_id TEXT NOT NULL,
            last_requested_at TEXT NOT NULL,
            last_exit_intent_id TEXT NOT NULL,
            last_evidence_id TEXT
                REFERENCES shadow_t004b_exit_source_evidence(evidence_id),
            revision INTEGER NOT NULL CHECK(revision >= 0),
            CHECK(
                (revision=0 AND last_requested_at='' AND last_exit_intent_id=''
                    AND last_evidence_id IS NULL)
                OR
                (revision>0 AND last_requested_at<>'' AND last_exit_intent_id<>''
                    AND last_evidence_id IS NOT NULL)
            )
        );

        CREATE TRIGGER IF NOT EXISTS shadow_t004b_inventory_no_update
        BEFORE UPDATE ON shadow_t004b_expected_inventory
        BEGIN SELECT RAISE(ABORT, 'T004B expected inventory is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS shadow_t004b_inventory_no_delete
        BEFORE DELETE ON shadow_t004b_expected_inventory
        BEGIN SELECT RAISE(ABORT, 'T004B expected inventory is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS shadow_t004b_exit_evidence_no_update
        BEFORE UPDATE ON shadow_t004b_exit_source_evidence
        BEGIN SELECT RAISE(ABORT, 'T004B exit evidence is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS shadow_t004b_exit_evidence_no_delete
        BEFORE DELETE ON shadow_t004b_exit_source_evidence
        BEGIN SELECT RAISE(ABORT, 'T004B exit evidence is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS shadow_t004b_exit_cursor_no_delete
        BEFORE DELETE ON shadow_t004b_exit_cursors
        BEGIN SELECT RAISE(ABORT, 'T004B exit cursor cannot be deleted'); END;
        """
    )
    expected = (
        MODEL_ID,
        SCHEMA_VERSION,
        MODEL_FINGERPRINT,
        INVENTORY_SCHEMA_VERSION,
        EXIT_EVIDENCE_SCHEMA_VERSION,
    )
    row = conn.execute(
        "SELECT model_id,schema_version,model_fingerprint,"
        "inventory_schema_version,exit_evidence_schema_version "
        "FROM shadow_t004b_schema_meta WHERE singleton=1"
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO shadow_t004b_schema_meta VALUES(1,?,?,?,?,?)", expected
        )
    elif tuple(row) != expected:
        raise ShadowDeterminismConflict("T004B lifecycle contract mismatch")
    conn.commit()


class ShadowLifecycleBridgeV01:
    def __init__(
        self,
        paper_conn: sqlite3.Connection,
        shadow_repository: ShadowRepositoryV01,
        source_identity: Phase4SourceIdentityV01,
        *,
        fault_hook: FaultHook | None = None,
    ) -> None:
        self._paper_conn = paper_conn
        self._shadow = shadow_repository
        self._conn = shadow_repository._conn
        self.source_identity = source_identity
        self._fault_hook = fault_hook
        self._closed = False
        _verify_exit_schema(paper_conn)
        _create_schema(self._conn)
        self._register_cursor()

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("lifecycle bridge is closed")

    def _register_cursor(self) -> None:
        identity = self.source_identity
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT candidate_run_id FROM shadow_t004b_exit_cursors "
                "WHERE source_id=?",
                (identity.source_id,),
            ).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO shadow_t004b_exit_cursors VALUES(?,?,'','',NULL,0)",
                    (identity.source_id, identity.candidate_run_id),
                )
            elif str(row[0]) != identity.candidate_run_id:
                raise SourceIdentityConflict("T004B exit cursor run identity mismatch")

    def _verify_source_still_bound(self) -> None:
        identity = self.source_identity
        stat = Path(identity.resolved_database_path).stat()
        if (str(int(stat.st_dev)), str(int(stat.st_ino))) != (
            identity.filesystem_device,
            identity.filesystem_inode,
        ):
            raise SourceIdentityConflict("Phase4 source file identity changed")
        if int(self._paper_conn.execute("PRAGMA query_only").fetchone()[0]) != 1:
            raise SourceIdentityConflict("Phase4 connection lost query_only mode")

    def cursor(self) -> tuple[str, str, str | None, int]:
        self._require_open()
        row = self._conn.execute(
            "SELECT last_requested_at,last_exit_intent_id,last_evidence_id,revision "
            "FROM shadow_t004b_exit_cursors WHERE source_id=?",
            (self.source_identity.source_id,),
        ).fetchone()
        if row is None:
            raise SourceIdentityConflict("T004B exit cursor is missing")
        return (str(row[0]), str(row[1]), row[2], int(row[3]))

    def materialize_expected_inventory(
        self,
        entry_intent: ExecutionIntentV01,
        quote: ExecutableQuoteV01 | None,
        simulation_result: ShadowSimulationResultV01 | None,
        *,
        evidence_at_us: int,
    ) -> ExpectedInventoryV01 | None:
        self._require_open()
        if not isinstance(entry_intent, ExecutionIntentV01):
            raise TypeError("entry_intent must be ExecutionIntentV01")
        persisted = self._shadow.get_intent(entry_intent.intent_id)
        if persisted is None or persisted.serialize() != entry_intent.serialize():
            raise InventoryConflict("expected inventory requires exact persisted ENTRY")
        if entry_intent.role is not IntentRole.ENTRY or entry_intent.side is not IntentSide.BUY:
            raise InventoryConflict("expected inventory requires ENTRY/BUY intent")
        state = self._shadow.current_state(entry_intent.intent_id)
        if state in NEGATIVE_ENTRY_STATES:
            if self._inventory_row(entry_intent.intent_id) is not None:
                raise InventoryConflict("negative terminal ENTRY has expected inventory")
            return None
        if state is not ShadowState.COMPLETED:
            raise InventoryConflict("expected inventory requires terminal COMPLETED ENTRY")
        if not isinstance(quote, ExecutableQuoteV01):
            raise TypeError("COMPLETED inventory requires ExecutableQuoteV01")
        if not isinstance(simulation_result, ShadowSimulationResultV01):
            raise TypeError("COMPLETED inventory requires ShadowSimulationResultV01")
        self._validate_success_chain(entry_intent, quote, simulation_result)
        evidence_time = _strict_int("evidence_at_us", evidence_at_us)
        if evidence_time < simulation_result.observed_at_us:
            raise InventoryConflict("inventory evidence timestamp precedes simulation result")
        inventory = ExpectedInventoryV01.create(
            entry_intent=entry_intent,
            quote=quote,
            simulation_result=simulation_result,
            evidence_at_us=evidence_time,
        )
        payload = canonical_json(inventory.payload())
        expected = (
            inventory.inventory_id,
            inventory.parent_entry_intent_id,
            inventory.mint,
            str(inventory.expected_base_amount),
            inventory.quote_id,
            inventory.quote_fingerprint,
            inventory.simulation_result_id,
            inventory.simulation_result_fingerprint,
            inventory.evidence_at_us,
            inventory.classification,
            payload,
            inventory.fingerprint,
        )
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            existing = self._conn.execute(
                "SELECT * FROM shadow_t004b_expected_inventory WHERE inventory_id=?",
                (inventory.inventory_id,),
            ).fetchone()
            if existing is not None:
                if tuple(existing) != expected:
                    raise InventoryConflict("expected-inventory replay conflict")
                return inventory
            collision = self._conn.execute(
                "SELECT inventory_id FROM shadow_t004b_expected_inventory "
                "WHERE parent_entry_intent_id=? OR quote_id=? "
                "OR simulation_result_id=?",
                (
                    inventory.parent_entry_intent_id,
                    inventory.quote_id,
                    inventory.simulation_result_id,
                ),
            ).fetchone()
            if collision is not None:
                raise InventoryConflict("expected-inventory uniqueness conflict")
            self._conn.execute(
                "INSERT INTO shadow_t004b_expected_inventory "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                expected,
            )
        return inventory

    def _validate_success_chain(
        self,
        entry: ExecutionIntentV01,
        quote: ExecutableQuoteV01,
        result: ShadowSimulationResultV01,
    ) -> None:
        if (
            quote.intent_id != entry.intent_id
            or quote.intent_fingerprint != entry.fingerprint
            or quote.side is not IntentSide.BUY
            or quote.expected_base_amount is None
            or quote.expected_base_amount <= 0
        ):
            raise InventoryConflict("quote conflicts with exact ENTRY inventory lineage")
        quote_row = self._conn.execute(
            "SELECT content_fingerprint,quote_json FROM shadow_t002_quotes "
            "WHERE quote_id=?",
            (quote.quote_id,),
        ).fetchone()
        if quote_row is None or tuple(str(value) for value in quote_row) != (
            quote.fingerprint,
            canonical_json(quote.payload()),
        ):
            raise InventoryConflict("quote is not exact persisted T002 evidence")
        result_row = self._conn.execute(
            "SELECT content_fingerprint,result_json FROM shadow_t003_results "
            "WHERE result_id=?",
            (result.result_id,),
        ).fetchone()
        if result_row is None or tuple(str(value) for value in result_row) != (
            result.fingerprint,
            canonical_json(result.payload()),
        ):
            raise InventoryConflict("simulation result is not exact persisted T003 evidence")
        chain = self._conn.execute(
            "SELECT p.plan_id,p.intent_id,p.quote_id,a.attempt_index "
            "FROM shadow_t003_results AS r "
            "JOIN shadow_t003_attempts AS a ON a.attempt_id=r.attempt_id "
            "JOIN shadow_t003_plans AS p ON p.plan_id=a.plan_id "
            "WHERE r.result_id=?",
            (result.result_id,),
        ).fetchone()
        maximum = self._conn.execute(
            "SELECT MAX(a.attempt_index) FROM shadow_t003_attempts AS a "
            "WHERE a.plan_id=?",
            (None if chain is None else str(chain[0]),),
        ).fetchone()
        if (
            chain is None
            or maximum is None
            or maximum[0] is None
            or str(chain[1]) != entry.intent_id
            or str(chain[2]) != quote.quote_id
            or int(chain[3]) != int(maximum[0])
            or result.outcome is not SimulationOutcome.SIMULATION_SUCCESS
        ):
            raise InventoryConflict("simulation result is not the successful final attempt")
        final = self._conn.execute(
            "SELECT to_state,reason_code,evidence_json "
            "FROM shadow_transition_history WHERE intent_id=? "
            "ORDER BY sequence DESC LIMIT 1",
            (entry.intent_id,),
        ).fetchone()
        if final is None:
            raise InventoryConflict("ENTRY lacks terminal transition evidence")
        try:
            final_evidence = json.loads(str(final[2]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise InventoryConflict("ENTRY terminal evidence is malformed") from exc
        if (
            str(final[0]) != ShadowState.COMPLETED.value
            or str(final[1]) != "SHADOW_SIMULATION_SUCCESS"
            or final_evidence.get("result_id") != result.result_id
            or final_evidence.get("outcome") != SimulationOutcome.SIMULATION_SUCCESS.value
        ):
            raise InventoryConflict("ENTRY completion is not bound to this simulation result")
        venue = None
        simulation = None
        try:
            venue = open_venue_repository(self._shadow.database_path)
            simulation = open_simulation_repository(self._shadow.database_path)
            venue.audit()
            simulation.audit()
        finally:
            if simulation is not None:
                simulation.close()
            if venue is not None:
                venue.close()

    def _inventory_row(self, parent_entry_intent_id: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM shadow_t004b_expected_inventory "
            "WHERE parent_entry_intent_id=?",
            (parent_entry_intent_id,),
        ).fetchone()

    def get_inventory(
        self,
        parent_entry_intent_id: str,
    ) -> ExpectedInventoryV01 | None:
        self._require_open()
        row = self._inventory_row(parent_entry_intent_id)
        if row is None:
            return None
        value = json.loads(str(row["inventory_json"]))
        if not isinstance(value, dict) or canonical_json(value) != str(row["inventory_json"]):
            raise InventoryConflict("persisted expected inventory JSON is invalid")
        inventory = ExpectedInventoryV01(
            inventory_id=str(row["inventory_id"]),
            parent_entry_intent_id=str(row["parent_entry_intent_id"]),
            mint=str(row["mint"]),
            expected_base_amount=int(row["expected_base_amount"]),
            quote_id=str(row["quote_id"]),
            quote_fingerprint=str(row["quote_fingerprint"]),
            simulation_result_id=str(row["simulation_result_id"]),
            simulation_result_fingerprint=str(row["simulation_result_fingerprint"]),
            evidence_at_us=int(row["evidence_at_us"]),
            classification=str(row["classification"]),
            fingerprint=str(row["content_fingerprint"]),
        )
        if (
            inventory.classification != EXPECTED_CLASSIFICATION
            or value != inventory.payload()
            or str(inventory.expected_base_amount)
            != str(row["expected_base_amount"])
            or content_fingerprint(inventory.payload()) != inventory.fingerprint
            or deterministic_id(
                "P5INV", INVENTORY_SCHEMA_VERSION, inventory.parent_entry_intent_id
            )
            != inventory.inventory_id
        ):
            raise InventoryConflict("persisted expected inventory conflicts")
        return inventory

    def _load_exit_rows(
        self,
        last_requested_at: str,
        last_exit_intent_id: str,
        limit: int,
    ) -> tuple[sqlite3.Row, ...]:
        return tuple(
            self._paper_conn.execute(
                _EXIT_SQL,
                (
                    self.source_identity.candidate_run_id,
                    self.source_identity.candidate_run_id,
                    last_requested_at,
                    last_requested_at,
                    last_exit_intent_id,
                    limit,
                ),
            ).fetchall()
        )

    @staticmethod
    def _construct_exit_source(row: sqlite3.Row) -> Phase4ExitSourceV01:
        exit_intent_id = _required_text("exit_intent_id", row["exit_intent_id"])
        paper_position_id = _required_text(
            "paper_position_id", row["exit_paper_position_id"]
        )
        paper_order_id = _required_text("paper_order_id", row["exit_paper_order_id"])
        signal_key = _required_text("signal_key", row["exit_signal_key"])
        mint = _required_text("mint", row["exit_mint"])
        track_id = _required_text("track_id", row["exit_track_id"])
        exit_variant = _required_text("exit_variant", row["exit_variant"])
        reason = _required_text("reason", row["exit_reason"])
        agreements = (
            (paper_position_id, row["track_paper_position_id"], "paper_position_id"),
            (paper_order_id, row["track_paper_order_id"], "paper_order_id"),
            (signal_key, row["track_signal_key"], "track signal_key"),
            (signal_key, row["exit_context_signal_key"], "exit context signal_key"),
            (signal_key, row["track_context_signal_key"], "track context signal_key"),
            (mint, row["track_mint"], "track mint"),
            (mint, row["exit_context_mint"], "exit context mint"),
            (mint, row["track_context_mint"], "track context mint"),
            (track_id, row["track_id"], "track_id"),
            (exit_variant, row["track_exit_variant"], "exit_variant"),
            (exit_intent_id, row["track_exit_intent_id"], "track exit_intent_id"),
        )
        for expected, actual, label in agreements:
            if expected != actual:
                raise ExitSourceConflict(f"joined {label} mismatch")
        if str(row["track_state"]) != "EXIT_INTENT":
            raise ExitSourceConflict("joined exit track is not in EXIT_INTENT state")
        if track_id not in LOCKED_TRACKS:
            raise ExitSourceConflict("paper exit uses an unknown track")
        if reason not in LOCKED_PAPER_REASONS:
            raise ExitSourceConflict("paper exit uses an unknown reason")
        requested_at = _required_text("requested_at", row["requested_at"])
        try:
            requested_at_us = utc_text_to_epoch_us(requested_at)
        except SourceBridgeError as exc:
            raise ExitSourceConflict("paper exit requested_at is invalid UTC") from exc
        return Phase4ExitSourceV01(
            exit_intent_id=exit_intent_id,
            paper_position_id=paper_position_id,
            paper_order_id=paper_order_id,
            signal_key=signal_key,
            mint=mint,
            track_id=track_id,
            exit_variant=exit_variant,
            reason=reason,
            requested_at=requested_at,
            requested_at_us=requested_at_us,
            trigger_observed_at=_optional_text(
                "trigger_observed_at", row["trigger_observed_at"]
            ),
            trigger_ingest_seq=_optional_int(
                "trigger_ingest_seq", row["trigger_ingest_seq"]
            ),
            trigger_source_event_key=_optional_text(
                "trigger_source_event_key", row["trigger_source_event_key"]
            ),
            trigger_price_numerator_raw=(
                None
                if row["trigger_price_numerator_raw"] is None
                else str(row["trigger_price_numerator_raw"])
            ),
            trigger_price_denominator_raw=(
                None
                if row["trigger_price_denominator_raw"] is None
                else str(row["trigger_price_denominator_raw"])
            ),
            rule_return_bps=(
                None if row["rule_return_bps"] is None else int(row["rule_return_bps"])
            ),
            trail_peak_return_bps=(
                None
                if row["trail_peak_return_bps"] is None
                else int(row["trail_peak_return_bps"])
            ),
            last_fresh_observed_at=_optional_text(
                "last_fresh_observed_at", row["last_fresh_observed_at"]
            ),
            last_fresh_ingest_seq=_optional_int(
                "last_fresh_ingest_seq", row["last_fresh_ingest_seq"]
            ),
            last_fresh_return_bps=(
                None
                if row["last_fresh_return_bps"] is None
                else int(row["last_fresh_return_bps"])
            ),
            track_signal_ingest_seq=_strict_int(
                "track signal_ingest_seq", row["track_signal_ingest_seq"]
            ),
        )

    def _parent_entry(self, source: Phase4ExitSourceV01) -> ExecutionIntentV01:
        rows = self._conn.execute(
            "SELECT intent_id,lineage_json,content_fingerprint "
            "FROM shadow_t004a_entry_lineage "
            "WHERE source_id=? AND candidate_run_id=? AND signal_key=?",
            (
                self.source_identity.source_id,
                self.source_identity.candidate_run_id,
                source.signal_key,
            ),
        ).fetchall()
        if len(rows) != 1:
            raise ExitSourceConflict("paper exit lacks exact T004A parent ENTRY lineage")
        row = rows[0]
        raw = str(row["lineage_json"])
        try:
            value = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ExitSourceConflict("T004A parent lineage JSON is malformed") from exc
        if (
            not isinstance(value, dict)
            or canonical_json(value) != raw
            or hashlib.sha256(raw.encode("utf-8")).hexdigest()
            != str(row["content_fingerprint"])
        ):
            raise ExitSourceConflict("T004A parent lineage fingerprint conflicts")
        phase4 = value.get("phase4_source")
        shadow = value.get("shadow_intent")
        bound_source = value.get("source_identity")
        if (
            not isinstance(phase4, dict)
            or not isinstance(shadow, dict)
            or not isinstance(bound_source, dict)
            or bound_source.get("source_id") != self.source_identity.source_id
            or bound_source.get("candidate_run_id")
            != self.source_identity.candidate_run_id
            or phase4.get("signal_key") != source.signal_key
            or phase4.get("run_id") != self.source_identity.candidate_run_id
            or phase4.get("mint") != source.mint
            or shadow.get("intent_id") != str(row["intent_id"])
        ):
            raise ExitSourceConflict("paper exit conflicts with T004A parent lineage")
        parent = self._shadow.get_intent(str(row["intent_id"]))
        if (
            parent is None
            or parent.role is not IntentRole.ENTRY
            or parent.side is not IntentSide.BUY
            or parent.candidate_run_id != self.source_identity.candidate_run_id
            or parent.mint != source.mint
            or parent.fingerprint != shadow.get("intent_fingerprint")
        ):
            raise ExitSourceConflict("resolved T004A parent ENTRY conflicts")
        return parent

    @staticmethod
    def _exit_intent(
        source: Phase4ExitSourceV01,
        parent: ExecutionIntentV01,
        inventory: ExpectedInventoryV01,
    ) -> ExecutionIntentV01:
        source_event_key = (
            source.trigger_source_event_key
            if source.trigger_source_event_key is not None
            else f"PAPER_EXIT_INTENT:{source.exit_intent_id}"
        )
        source_ingest_seq = (
            source.trigger_ingest_seq
            if source.trigger_ingest_seq is not None
            else source.track_signal_ingest_seq
        )
        return ExecutionIntentV01(
            candidate_signal_id=parent.candidate_signal_id,
            candidate_run_id=parent.candidate_run_id,
            strategy_evaluation_id=parent.strategy_evaluation_id,
            strategy_version=parent.strategy_version,
            parameter_set_id=parent.parameter_set_id,
            source_run_id=parent.source_run_id,
            source_event_key=source_event_key,
            source_ingest_seq=source_ingest_seq,
            decision_at_us=source.requested_at_us,
            mint=source.mint,
            role=IntentRole.EXIT,
            side=IntentSide.SELL,
            input_asset="MEME_BASE_UNITS",
            input_amount_base_units=inventory.expected_base_amount,
            position_id=deterministic_track_position_id(
                parent.intent_id, source.track_id
            ),
            parent_entry_intent_id=parent.intent_id,
            exit_decision_id=source.exit_intent_id,
            exit_track_id=source.track_id,
            exit_lifecycle_id=source.paper_position_id,
        )

    def _evidence_record(
        self,
        source: Phase4ExitSourceV01,
        parent: ExecutionIntentV01,
        *,
        classification: str,
        exit_intent: ExecutionIntentV01 | None,
        terminal_parent_state: ShadowState | None,
        inventory: ExpectedInventoryV01 | None,
    ) -> tuple[str, str, str]:
        evidence_id = deterministic_id(
            "P5XEV",
            EXIT_EVIDENCE_SCHEMA_VERSION,
            self.source_identity.source_id,
            self.source_identity.candidate_run_id,
            source.requested_at,
            source.exit_intent_id,
        )
        payload = {
            "schema_version": EXIT_EVIDENCE_SCHEMA_VERSION,
            "model_id": MODEL_ID,
            "model_fingerprint": MODEL_FINGERPRINT,
            "source_identity": {
                "source_id": self.source_identity.source_id,
                "candidate_run_id": self.source_identity.candidate_run_id,
            },
            "phase4_exit": {
                "exit_intent_id": source.exit_intent_id,
                "paper_position_id": source.paper_position_id,
                "paper_order_id": source.paper_order_id,
                "signal_key": source.signal_key,
                "mint": source.mint,
                "track_id": source.track_id,
                "exit_variant": source.exit_variant,
                "reason": source.reason,
                "requested_at": source.requested_at,
                "requested_at_us": source.requested_at_us,
                "trigger_observed_at": source.trigger_observed_at,
                "trigger_ingest_seq": source.trigger_ingest_seq,
                "trigger_source_event_key": source.trigger_source_event_key,
                "trigger_price_numerator_raw": source.trigger_price_numerator_raw,
                "trigger_price_denominator_raw": source.trigger_price_denominator_raw,
                "rule_return_bps": source.rule_return_bps,
                "trail_peak_return_bps": source.trail_peak_return_bps,
                "last_fresh_observed_at": source.last_fresh_observed_at,
                "last_fresh_ingest_seq": source.last_fresh_ingest_seq,
                "last_fresh_return_bps": source.last_fresh_return_bps,
            },
            "parent_entry_intent_id": parent.intent_id,
            "classification": classification,
            "expected_inventory_id": None if inventory is None else inventory.inventory_id,
            "shadow_exit_intent": (
                None
                if exit_intent is None
                else {
                    "intent_id": exit_intent.intent_id,
                    "intent_fingerprint": exit_intent.fingerprint,
                }
            ),
            "terminal_parent_state": (
                None if terminal_parent_state is None else terminal_parent_state.value
            ),
            "paper_outcome_is_shadow_outcome": False,
        }
        evidence_json = canonical_json(payload)
        return (
            evidence_id,
            evidence_json,
            hashlib.sha256(evidence_json.encode("utf-8")).hexdigest(),
        )

    def _persist_evidence(
        self,
        source: Phase4ExitSourceV01,
        parent: ExecutionIntentV01,
        *,
        classification: str,
        exit_intent: ExecutionIntentV01 | None,
        terminal_parent_state: ShadowState | None,
        inventory: ExpectedInventoryV01 | None,
    ) -> str:
        evidence_id, evidence_json, fingerprint = self._evidence_record(
            source,
            parent,
            classification=classification,
            exit_intent=exit_intent,
            terminal_parent_state=terminal_parent_state,
            inventory=inventory,
        )
        expected = (
            evidence_id,
            self.source_identity.source_id,
            self.source_identity.candidate_run_id,
            source.requested_at,
            source.exit_intent_id,
            source.paper_position_id,
            source.signal_key,
            source.track_id,
            parent.intent_id,
            classification,
            None if exit_intent is None else exit_intent.intent_id,
            None if terminal_parent_state is None else terminal_parent_state.value,
            evidence_json,
            fingerprint,
        )
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            existing = self._conn.execute(
                "SELECT * FROM shadow_t004b_exit_source_evidence WHERE evidence_id=?",
                (evidence_id,),
            ).fetchone()
            if existing is not None:
                if tuple(existing) != expected:
                    raise ShadowDeterminismConflict("T004B exit-evidence replay conflict")
                return evidence_id
            collision = self._conn.execute(
                "SELECT evidence_id FROM shadow_t004b_exit_source_evidence "
                "WHERE source_id=? AND candidate_run_id=? AND exit_intent_id=?",
                (
                    self.source_identity.source_id,
                    self.source_identity.candidate_run_id,
                    source.exit_intent_id,
                ),
            ).fetchone()
            if collision is not None:
                raise ShadowDeterminismConflict("T004B exit-evidence uniqueness conflict")
            self._conn.execute(
                "INSERT INTO shadow_t004b_exit_source_evidence "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                expected,
            )
        return evidence_id

    def _advance_cursor(
        self,
        previous: tuple[str, str, str | None, int],
        source: Phase4ExitSourceV01,
        evidence_id: str,
    ) -> tuple[str, str, str | None, int]:
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            current = self.cursor()
            if current[:2] == source.ordering_key:
                if current[2] != evidence_id:
                    raise ShadowDeterminismConflict("T004B cursor evidence conflict")
                return current
            if current[:2] > source.ordering_key:
                raise ShadowDeterminismConflict("T004B stale cursor advancement")
            if current != previous:
                raise ShadowDeterminismConflict("T004B concurrent cursor advancement")
            exists = self._conn.execute(
                "SELECT 1 FROM shadow_t004b_exit_source_evidence "
                "WHERE evidence_id=? AND source_id=? AND requested_at=? "
                "AND exit_intent_id=?",
                (
                    evidence_id,
                    self.source_identity.source_id,
                    source.requested_at,
                    source.exit_intent_id,
                ),
            ).fetchone()
            if exists is None:
                raise ShadowDeterminismConflict(
                    "T004B cursor cannot advance without exact evidence"
                )
            updated = self._conn.execute(
                "UPDATE shadow_t004b_exit_cursors SET last_requested_at=?,"
                "last_exit_intent_id=?,last_evidence_id=?,revision=revision+1 "
                "WHERE source_id=? AND revision=? AND last_requested_at=? "
                "AND last_exit_intent_id=?",
                (
                    source.requested_at,
                    source.exit_intent_id,
                    evidence_id,
                    self.source_identity.source_id,
                    previous[3],
                    previous[0],
                    previous[1],
                ),
            )
            if updated.rowcount != 1:
                raise ShadowDeterminismConflict("T004B atomic cursor update failed")
        return (source.requested_at, source.exit_intent_id, evidence_id, previous[3] + 1)

    def poll_exit_once(self, *, limit: int = 100) -> ExitPollResultV01:
        self._require_open()
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be a positive integer")
        self._verify_source_still_bound()
        cursor = self.cursor()
        rows = self._load_exit_rows(cursor[0], cursor[1], limit)
        processed = exits = no_positions = 0
        status = ExitPollStatus.IDLE
        for row in rows:
            source = self._construct_exit_source(row)
            if (
                str(row["exit_context_run_id"])
                != self.source_identity.candidate_run_id
                or str(row["track_context_run_id"])
                != self.source_identity.candidate_run_id
            ):
                raise ExitSourceConflict("paper exit candidate run mismatch")
            if source.ordering_key <= cursor[:2]:
                raise ExitSourceConflict("exit source query returned non-advancing row")
            parent = self._parent_entry(source)
            parent_state = self._shadow.current_state(parent.intent_id)
            if parent_state not in TERMINAL_STATES:
                status = ExitPollStatus.WAITING_FOR_ENTRY_TERMINAL
                break
            if parent_state in NEGATIVE_ENTRY_STATES:
                if self.get_inventory(parent.intent_id) is not None:
                    raise InventoryConflict("negative terminal ENTRY has expected inventory")
                evidence_id = self._persist_evidence(
                    source,
                    parent,
                    classification=NO_POSITION_CLASSIFICATION,
                    exit_intent=None,
                    terminal_parent_state=parent_state,
                    inventory=None,
                )
                if self._fault_hook is not None:
                    self._fault_hook("AFTER_NO_POSITION_EVIDENCE_PERSISTENCE", source)
                no_positions += 1
            elif parent_state is ShadowState.COMPLETED:
                inventory = self.get_inventory(parent.intent_id)
                if inventory is None:
                    raise InventoryConflict(
                        "COMPLETED parent ENTRY lacks expected inventory"
                    )
                exit_intent = self._exit_intent(source, parent, inventory)
                persisted = self._shadow.register_intent(exit_intent)
                if self._fault_hook is not None:
                    self._fault_hook("AFTER_EXIT_INTENT_REGISTRATION", source)
                evidence_id = self._persist_evidence(
                    source,
                    parent,
                    classification=EXIT_INTENT_CLASSIFICATION,
                    exit_intent=persisted,
                    terminal_parent_state=None,
                    inventory=inventory,
                )
                if self._fault_hook is not None:
                    self._fault_hook("AFTER_EXIT_EVIDENCE_PERSISTENCE", source)
                exits += 1
            else:
                raise ShadowDeterminismConflict("unknown terminal parent ENTRY state")
            cursor = self._advance_cursor(cursor, source, evidence_id)
            processed += 1
            status = ExitPollStatus.PROCESSED
        return ExitPollResultV01(
            status=status,
            processed_rows=processed,
            exit_intents=exits,
            no_position_rows=no_positions,
            last_requested_at=cursor[0],
            last_exit_intent_id=cursor[1],
        )

    def quick_check(self) -> str:
        self._require_open()
        return self._shadow.quick_check()

    def canonical_digest(self) -> str:
        self._require_open()
        payload = {
            "model_id": MODEL_ID,
            "model_fingerprint": MODEL_FINGERPRINT,
            "shadow_repository_digest": self._shadow.canonical_digest(),
            "inventory": [
                dict(row)
                for row in self._conn.execute(
                    "SELECT * FROM shadow_t004b_expected_inventory ORDER BY inventory_id"
                )
            ],
            "exit_evidence": [
                dict(row)
                for row in self._conn.execute(
                    "SELECT * FROM shadow_t004b_exit_source_evidence ORDER BY evidence_id"
                )
            ],
            "cursors": [
                dict(row)
                for row in self._conn.execute(
                    "SELECT * FROM shadow_t004b_exit_cursors ORDER BY source_id"
                )
            ],
        }
        return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()

    def close(self) -> None:
        if not self._closed:
            self._paper_conn.close()
            self._shadow.close()
            self._closed = True

    def __enter__(self) -> ShadowLifecycleBridgeV01:
        self._require_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class SourceIdentityConflict(LifecycleBridgeError):
    pass


def _resolve_t004a_source(
    conn: sqlite3.Connection,
    *,
    resolved_paper_path: Path,
    filesystem_device: str,
    filesystem_inode: str,
    candidate_run_id: str | None,
) -> Phase4SourceIdentityV01:
    parameters: list[Any] = [str(resolved_paper_path), filesystem_device, filesystem_inode]
    sql = (
        "SELECT source_id,resolved_paper_db_path,filesystem_device,"
        "filesystem_inode,candidate_run_id FROM shadow_t004a_sources "
        "WHERE resolved_paper_db_path=? AND filesystem_device=? "
        "AND filesystem_inode=?"
    )
    if candidate_run_id is not None:
        if not isinstance(candidate_run_id, str) or not candidate_run_id.strip():
            raise SourceIdentityConflict("candidate_run_id is required")
        sql += " AND candidate_run_id=?"
        parameters.append(candidate_run_id)
    sql += " ORDER BY candidate_run_id LIMIT 2"
    try:
        rows = conn.execute(sql, tuple(parameters)).fetchall()
    except sqlite3.OperationalError as exc:
        raise SourceIdentityConflict("accepted T004A source binding is missing") from exc
    if len(rows) != 1:
        raise SourceIdentityConflict(
            "exactly one accepted T004A source/run binding is required"
        )
    row = rows[0]
    return Phase4SourceIdentityV01(
        source_id=str(row[0]),
        resolved_database_path=str(row[1]),
        filesystem_device=str(row[2]),
        filesystem_inode=str(row[3]),
        candidate_run_id=str(row[4]),
    )


def open_shadow_lifecycle_bridge(
    paper_database_path: str | Path,
    shadow_database_path: str | Path,
    *,
    candidate_run_id: str | None = None,
    fault_hook: FaultHook | None = None,
) -> ShadowLifecycleBridgeV01:
    paper_conn, resolved, device, inode = open_phase4_read_only(paper_database_path)
    shadow: ShadowRepositoryV01 | None = None
    try:
        shadow_path = Path(shadow_database_path).resolve()
        if shadow_path != SHADOW_DATA_ROOT and SHADOW_DATA_ROOT not in shadow_path.parents:
            raise SourceIdentityConflict("T004B Shadow database must remain under data/shadow")
        if shadow_path == resolved or (
            shadow_path.exists() and os.path.samefile(resolved, shadow_path)
        ):
            raise SourceIdentityConflict("Phase4 source and Shadow database must differ")
        if not shadow_path.is_file():
            raise SourceIdentityConflict(
                "T004B requires the existing T004A-bound Shadow database"
            )
        shadow = open_shadow_repository(shadow_path)
        identity = _resolve_t004a_source(
            shadow._conn,
            resolved_paper_path=resolved,
            filesystem_device=device,
            filesystem_inode=inode,
            candidate_run_id=candidate_run_id,
        )
        return ShadowLifecycleBridgeV01(
            paper_conn,
            shadow,
            identity,
            fault_hook=fault_hook,
        )
    except BaseException:
        paper_conn.close()
        if shadow is not None:
            shadow.close()
        raise
