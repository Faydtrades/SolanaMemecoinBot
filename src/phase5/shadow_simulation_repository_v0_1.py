from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from .shadow_domain_v0_1 import (
    ShadowDeterminismConflict,
    ShadowState,
    canonical_json,
    content_fingerprint,
    deterministic_id,
)
from .shadow_repository_v0_1 import (
    DEFAULT_SHADOW_DATABASE_PATH,
    PROJECT_ROOT,
    ShadowRepositoryV01,
)
from .shadow_unsigned_plan_simulation_v0_1 import (
    ATTEMPT_SCHEMA_VERSION,
    ENVELOPE_SCHEMA_VERSION,
    LEASE_SCHEMA_VERSION,
    MODEL_FINGERPRINT,
    MODEL_ID,
    PLAN_SCHEMA_VERSION,
    RESULT_SCHEMA_VERSION,
    SCHEMA_VERSION,
    BlockhashLeaseV01,
    BlockhashValidityEvidenceV01,
    ShadowSimulationResultV01,
    SimulationAttemptV01,
    SimulationEnvelopeV01,
    SimulationOutcome,
    SimulationRunEvidenceV01,
    UnsignedTransactionPlanV01,
)


_OPEN_TOKEN = object()

# Persistence has its own contract: the accepted economic/model identities above
# and the state transition helpers below are deliberately unchanged.
LEGACY_PERSISTENCE_SCHEMA_VERSION = "phase5_shadow_simulation_persistence_v0.1"
PERSISTENCE_SCHEMA_VERSION = "phase5_shadow_simulation_persistence_v0.2"
LEGACY_PERSISTENCE_CONTRACT = {
    "schema_version": LEGACY_PERSISTENCE_SCHEMA_VERSION,
    "instruction_occurrence_uniqueness": [["plan_id", "sequence"], ["instruction_id"]],
    "instruction_identity": "P5IX_CONTENT_FINGERPRINT_UNCHANGED",
    "evidence": "APPEND_ONLY_EXACT_REPLAY",
}
LEGACY_PERSISTENCE_FINGERPRINT = content_fingerprint(LEGACY_PERSISTENCE_CONTRACT)
PERSISTENCE_CONTRACT = {
    **LEGACY_PERSISTENCE_CONTRACT,
    "schema_version": PERSISTENCE_SCHEMA_VERSION,
    "instruction_occurrence_uniqueness": [["plan_id", "sequence"], ["plan_id", "instruction_id"]],
    "legacy_migration": "ATOMIC_EXACT_ROWS_AUDIT_AND_FOREIGN_KEYS_OR_ROLLBACK",
}
PERSISTENCE_FINGERPRINT = content_fingerprint(PERSISTENCE_CONTRACT)


def _execute_schema_sql(conn: sqlite3.Connection, script: str) -> None:
    """Unlike executescript(), never implicitly commits the initialization txn."""
    statement = ""
    for line in script.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            conn.execute(statement)
            statement = ""
    if statement.strip():
        raise ShadowDeterminismConflict("incomplete repository schema statement")


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _validate_database_path(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    shadow_root = (PROJECT_ROOT / "data" / "shadow").resolve()
    if not _is_within(resolved, shadow_root):
        raise ValueError("T003 repository must remain under data/shadow")
    lowered = {part.lower() for part in resolved.parts}
    if "paper" in lowered or "db" in lowered and "shadow" not in lowered:
        raise ValueError("T003 repository cannot use Phase-4/source ownership")
    return resolved


def _append_only_triggers(table: str) -> str:
    return f"""
        CREATE TRIGGER IF NOT EXISTS {table}_immutable_update
        BEFORE UPDATE ON {table}
        BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS {table}_immutable_delete
        BEFORE DELETE ON {table}
        BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END;
    """


def _create_schema(conn: sqlite3.Connection) -> None:
    _execute_schema_sql(conn,
        """
        CREATE TABLE IF NOT EXISTS shadow_t003_contract (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            model_id TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            model_fingerprint TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shadow_t003_plans (
            plan_id TEXT PRIMARY KEY,
            intent_id TEXT NOT NULL REFERENCES shadow_execution_intents(intent_id),
            quote_id TEXT NOT NULL REFERENCES shadow_t002_quotes(quote_id),
            state_id TEXT NOT NULL REFERENCES shadow_t002_venue_states(state_id),
            route_id TEXT NOT NULL REFERENCES shadow_t002_routes(route_id),
            content_fingerprint TEXT NOT NULL,
            plan_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shadow_t003_plan_instructions (
            plan_id TEXT NOT NULL REFERENCES shadow_t003_plans(plan_id),
            sequence INTEGER NOT NULL,
            instruction_id TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            instruction_json TEXT NOT NULL,
            PRIMARY KEY(plan_id,sequence),
            UNIQUE(plan_id,instruction_id)
        );
        CREATE TABLE IF NOT EXISTS shadow_t003_blockhash_leases (
            lease_id TEXT PRIMARY KEY,
            plan_id TEXT NOT NULL REFERENCES shadow_t003_plans(plan_id),
            context_slot INTEGER NOT NULL,
            last_valid_block_height INTEGER NOT NULL,
            content_fingerprint TEXT NOT NULL,
            lease_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shadow_t003_blockhash_validity (
            content_fingerprint TEXT PRIMARY KEY,
            lease_id TEXT NOT NULL REFERENCES shadow_t003_blockhash_leases(lease_id),
            validity_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shadow_t003_envelopes (
            envelope_id TEXT PRIMARY KEY,
            plan_id TEXT NOT NULL REFERENCES shadow_t003_plans(plan_id),
            lease_id TEXT NOT NULL REFERENCES shadow_t003_blockhash_leases(lease_id),
            request_config_fingerprint TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            envelope_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shadow_t003_attempts (
            attempt_id TEXT PRIMARY KEY,
            plan_id TEXT NOT NULL REFERENCES shadow_t003_plans(plan_id),
            lease_id TEXT NOT NULL REFERENCES shadow_t003_blockhash_leases(lease_id),
            envelope_id TEXT NOT NULL REFERENCES shadow_t003_envelopes(envelope_id),
            validity_fingerprint TEXT NOT NULL REFERENCES shadow_t003_blockhash_validity(content_fingerprint),
            attempt_index INTEGER NOT NULL CHECK(attempt_index IN (0,1)),
            content_fingerprint TEXT NOT NULL,
            attempt_json TEXT NOT NULL,
            UNIQUE(plan_id,attempt_index)
        );
        CREATE TABLE IF NOT EXISTS shadow_t003_results (
            result_id TEXT PRIMARY KEY,
            attempt_id TEXT NOT NULL UNIQUE REFERENCES shadow_t003_attempts(attempt_id),
            outcome TEXT NOT NULL CHECK(outcome IN (
                'SIMULATION_SUCCESS','PROGRAM_REJECTED','BLOCKHASH_EXPIRED',
                'RPC_FAILURE','MALFORMED_RESPONSE','CONTEXT_REGRESSION'
            )),
            content_fingerprint TEXT NOT NULL,
            result_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_t003_plan_intent ON shadow_t003_plans(intent_id);
        CREATE INDEX IF NOT EXISTS idx_t003_lease_plan ON shadow_t003_blockhash_leases(plan_id);
        CREATE INDEX IF NOT EXISTS idx_t003_attempt_plan ON shadow_t003_attempts(plan_id,attempt_index);
        """
    )
    for table in (
        "shadow_t003_contract",
        "shadow_t003_plans",
        "shadow_t003_plan_instructions",
        "shadow_t003_blockhash_leases",
        "shadow_t003_blockhash_validity",
        "shadow_t003_envelopes",
        "shadow_t003_attempts",
        "shadow_t003_results",
    ):
        _execute_schema_sql(conn, _append_only_triggers(table))
    row = conn.execute(
        "SELECT model_id,schema_version,model_fingerprint FROM shadow_t003_contract WHERE singleton=1"
    ).fetchone()
    expected = (MODEL_ID, SCHEMA_VERSION, MODEL_FINGERPRINT)
    if row is None:
        conn.execute(
            "INSERT INTO shadow_t003_contract VALUES(1,?,?,?)", expected
        )
    elif tuple(str(value) for value in row) != expected:
        raise ShadowDeterminismConflict("T003 repository contract mismatch")


def _instruction_layout(conn: sqlite3.Connection) -> str:
    table = "shadow_t003_plan_instructions"
    columns = [(r[1], r[2], r[3], r[4], r[5]) for r in conn.execute(f"PRAGMA table_info({table})")]
    if columns != [("plan_id", "TEXT", 1, None, 1), ("sequence", "INTEGER", 1, None, 2),
                   ("instruction_id", "TEXT", 1, None, 0), ("content_fingerprint", "TEXT", 1, None, 0),
                   ("instruction_json", "TEXT", 1, None, 0)]:
        raise ShadowDeterminismConflict("unsupported instruction relation columns")
    keys = set()
    for row in conn.execute(f"PRAGMA index_list({table})"):
        if not row[2] or row[4] or row[3] not in ("pk", "u"):
            raise ShadowDeterminismConflict("unsupported instruction relation index")
        name = str(row[1]).replace('"', '""')
        keys.add(tuple(r[2] for r in conn.execute(f'PRAGMA index_info("{name}")')))
    foreign_keys = [tuple(row)[2:] for row in conn.execute(f"PRAGMA foreign_key_list({table})")]
    if foreign_keys != [("shadow_t003_plans", "plan_id", "plan_id", "NO ACTION", "NO ACTION", "NONE")]:
        raise ShadowDeterminismConflict("unsupported instruction relation foreign key")
    if keys == {("plan_id", "sequence"), ("instruction_id",)}:
        return "legacy"
    if keys == {("plan_id", "sequence"), ("plan_id", "instruction_id")}:
        return "corrected"
    raise ShadowDeterminismConflict("unsupported instruction relation uniqueness")


def _initialize_persistence(repository: ShadowSimulationRepositoryV01) -> None:
    conn = repository._conn
    layout = _instruction_layout(conn)
    if layout == "legacy" and conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='shadow_t003_persistence_contract'").fetchone():
        raise ShadowDeterminismConflict("legacy relation conflicts with corrected persistence marker")
    # Audit the legacy evidence BEFORE any row/table replacement. The caller's
    # single transaction includes creation, copying, validation and contract bind.
    repository.audit()
    if layout == "legacy":
        table = "shadow_t003_plan_instructions"
        triggers = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (table,))}
        if triggers != {table + "_immutable_update", table + "_immutable_delete"}:
            raise ShadowDeterminismConflict("unsupported legacy instruction triggers")
        before = [tuple(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY plan_id,sequence")]
        conn.execute("""CREATE TABLE shadow_t003_plan_instructions_migrating (
            plan_id TEXT NOT NULL REFERENCES shadow_t003_plans(plan_id),
            sequence INTEGER NOT NULL, instruction_id TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL, instruction_json TEXT NOT NULL,
            PRIMARY KEY(plan_id,sequence), UNIQUE(plan_id,instruction_id))""")
        conn.execute(f"INSERT INTO shadow_t003_plan_instructions_migrating SELECT * FROM {table}")
        after = [tuple(row) for row in conn.execute(
            "SELECT * FROM shadow_t003_plan_instructions_migrating ORDER BY plan_id,sequence")]
        if len(before) != len(after) or before != after:
            raise ShadowDeterminismConflict("legacy instruction migration changed evidence")
        conn.execute(f"DROP TABLE {table}")
        conn.execute(f"ALTER TABLE shadow_t003_plan_instructions_migrating RENAME TO {table}")
        _execute_schema_sql(conn, _append_only_triggers(table))
        if _instruction_layout(conn) != "corrected":
            raise ShadowDeterminismConflict("instruction migration did not establish corrected relation")
    conn.execute("""CREATE TABLE IF NOT EXISTS shadow_t003_persistence_contract (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), schema_version TEXT NOT NULL,
        persistence_fingerprint TEXT NOT NULL, contract_json TEXT NOT NULL)""")
    expected = (PERSISTENCE_SCHEMA_VERSION, PERSISTENCE_FINGERPRINT, canonical_json(PERSISTENCE_CONTRACT))
    row = conn.execute("SELECT schema_version,persistence_fingerprint,contract_json FROM shadow_t003_persistence_contract WHERE singleton=1").fetchone()
    if row is not None and tuple(row) != expected:
        raise ShadowDeterminismConflict("T003 persistence contract mismatch")
    conn.execute("INSERT OR IGNORE INTO shadow_t003_persistence_contract VALUES(1,?,?,?)", expected)
    _execute_schema_sql(conn, _append_only_triggers("shadow_t003_persistence_contract"))
    repository.audit()


def open_simulation_repository(
    path: str | Path = DEFAULT_SHADOW_DATABASE_PATH,
) -> ShadowSimulationRepositoryV01:
    resolved = _validate_database_path(Path(path))
    if not resolved.exists():
        raise FileNotFoundError("open and initialize the accepted T001/T002 repository first")
    conn = sqlite3.connect(resolved, timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        mode = str(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
        conn.execute("PRAGMA synchronous=FULL")
        if (
            mode != "wal"
            or int(conn.execute("PRAGMA synchronous").fetchone()[0]) != 2
            or int(conn.execute("PRAGMA foreign_keys").fetchone()[0]) != 1
        ):
            raise RuntimeError("T003 repository requires WAL/FULL/foreign keys")
        conn.execute("BEGIN IMMEDIATE")
        _create_schema(conn)
        repository = ShadowSimulationRepositoryV01(conn, resolved, _open_token=_OPEN_TOKEN)
        _initialize_persistence(repository)
        conn.commit()
        return repository
    except BaseException:
        conn.rollback()
        conn.close()
        raise


class ShadowSimulationRepositoryV01:
    def __init__(
        self,
        conn: sqlite3.Connection,
        database_path: Path,
        *,
        _open_token: object,
    ) -> None:
        if _open_token is not _OPEN_TOKEN:
            raise TypeError("use open_simulation_repository()")
        resolved = _validate_database_path(database_path)
        main = [row for row in conn.execute("PRAGMA database_list") if str(row[1]) == "main"]
        if len(main) != 1 or Path(str(main[0][2])).resolve() != resolved:
            raise ValueError("T003 repository connection/path mismatch")
        self._conn = conn
        self.database_path = resolved
        self._closed = False

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("T003 repository is closed")

    def _state(self, intent_id: str) -> ShadowState:
        row = self._conn.execute(
            "SELECT current_state FROM shadow_state_machines WHERE intent_id=?", (intent_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown execution intent: {intent_id}")
        return ShadowState(str(row[0]))

    def persist_plan(self, plan: UnsignedTransactionPlanV01) -> str:
        self._require_open()
        if not isinstance(plan, UnsignedTransactionPlanV01):
            raise TypeError("plan must be UnsignedTransactionPlanV01")
        payload = canonical_json(plan.payload())
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            existing = self._conn.execute(
                "SELECT content_fingerprint,plan_json FROM shadow_t003_plans WHERE plan_id=?",
                (plan.plan_id,),
            ).fetchone()
            if existing is not None:
                if (str(existing[0]), str(existing[1])) != (plan.fingerprint, payload):
                    raise ShadowDeterminismConflict("unsigned-plan replay conflict")
                self._verify_instruction_rows(plan)
                return plan.plan_id
            if self._state(plan.intent_id) is not ShadowState.QUOTE_BOUND:
                raise ShadowDeterminismConflict("new plan requires QUOTE_BOUND prerequisite")
            intent = self._conn.execute(
                "SELECT content_fingerprint,intent_json FROM shadow_execution_intents WHERE intent_id=?",
                (plan.intent_id,),
            ).fetchone()
            quote = self._conn.execute(
                "SELECT content_fingerprint,quote_json,state_id,route_id FROM shadow_t002_quotes WHERE quote_id=?",
                (plan.quote_id,),
            ).fetchone()
            state = self._conn.execute(
                "SELECT content_fingerprint,state_json FROM shadow_t002_venue_states WHERE state_id=?",
                (plan.venue_state_id,),
            ).fetchone()
            route = self._conn.execute(
                "SELECT content_fingerprint,route_json FROM shadow_t002_routes WHERE route_id=?",
                (plan.route_id,),
            ).fetchone()
            if intent is None or quote is None or state is None or route is None:
                raise ShadowDeterminismConflict("plan lacks persisted T001/T002 lineage")
            intent_json = json.loads(str(intent[1]))
            quote_json = json.loads(str(quote[1]))
            state_json = json.loads(str(state[1]))
            route_json = json.loads(str(route[1]))
            if (
                str(intent[0]) != plan.intent_fingerprint
                or intent_json.get("fingerprint") != plan.intent_fingerprint
                or str(quote[0]) != plan.quote_fingerprint
                or str(quote[2]) != plan.venue_state_id
                or str(quote[3]) != plan.route_id
                or str(state[0]) != plan.venue_state_fingerprint
                or str(route[0]) != plan.route_fingerprint
                or quote_json.get("intent_fingerprint") != plan.intent_fingerprint
                or quote_json.get("venue_state_fingerprint") != plan.venue_state_fingerprint
                or quote_json.get("route_fingerprint") != plan.route_fingerprint
                or quote_json.get("venue") != plan.venue.value
                or quote_json.get("side") != plan.side.value
                or state_json.get("venue") != plan.venue.value
                or route_json.get("selected_state_id") != plan.venue_state_id
                or plan.prerequisite_slot < int(quote_json.get("slot_max", -1))
            ):
                raise ShadowDeterminismConflict("plan conflicts with exact persisted quote lineage")
            self._conn.execute(
                "INSERT INTO shadow_t003_plans VALUES(?,?,?,?,?,?,?)",
                (
                    plan.plan_id, plan.intent_id, plan.quote_id, plan.venue_state_id,
                    plan.route_id, plan.fingerprint, payload,
                ),
            )
            for item in plan.instructions:
                shared = self._conn.execute(
                    "SELECT content_fingerprint,instruction_json FROM shadow_t003_plan_instructions WHERE instruction_id=? LIMIT 1",
                    (item.instruction_id,),
                ).fetchone()
                if shared is not None and tuple(shared) != (item.fingerprint, canonical_json(item.payload())):
                    raise ShadowDeterminismConflict("shared instruction identity has conflicting evidence")
                self._conn.execute(
                    "INSERT INTO shadow_t003_plan_instructions VALUES(?,?,?,?,?)",
                    (
                        plan.plan_id, item.sequence, item.instruction_id,
                        item.fingerprint, canonical_json(item.payload()),
                    ),
                )
        return plan.plan_id

    def _verify_instruction_rows(self, plan: UnsignedTransactionPlanV01) -> None:
        rows = self._conn.execute(
            "SELECT sequence,instruction_id,content_fingerprint,instruction_json "
            "FROM shadow_t003_plan_instructions WHERE plan_id=? ORDER BY sequence",
            (plan.plan_id,),
        ).fetchall()
        expected = [
            (
                item.sequence, item.instruction_id, item.fingerprint,
                canonical_json(item.payload()),
            )
            for item in plan.instructions
        ]
        actual = [tuple(row) for row in rows]
        if actual != expected:
            raise ShadowDeterminismConflict("persisted plan instruction sequence conflicts")

    def persist_attempt_evidence(
        self,
        lease: BlockhashLeaseV01,
        validity: BlockhashValidityEvidenceV01,
        envelope: SimulationEnvelopeV01,
        attempt: SimulationAttemptV01,
    ) -> str:
        self._require_open()
        if not all((
            isinstance(lease, BlockhashLeaseV01),
            isinstance(validity, BlockhashValidityEvidenceV01),
            isinstance(envelope, SimulationEnvelopeV01),
            isinstance(attempt, SimulationAttemptV01),
        )):
            raise TypeError("attempt evidence types are invalid")
        if not (
            lease.plan_id == envelope.plan_id == attempt.plan_id
            and lease.lease_id == envelope.lease_id == attempt.lease_id
            and envelope.envelope_id == attempt.envelope_id
            and validity.lease_id == lease.lease_id
            and validity.fingerprint == attempt.validity_fingerprint
            and envelope.request_config_fingerprint == attempt.request_config_fingerprint
        ):
            raise ShadowDeterminismConflict("simulation attempt lineage mismatch")
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            existing = self._conn.execute(
                "SELECT content_fingerprint,attempt_json FROM shadow_t003_attempts WHERE attempt_id=?",
                (attempt.attempt_id,),
            ).fetchone()
            if existing is not None:
                if (str(existing[0]), str(existing[1])) != (
                    attempt.fingerprint, canonical_json(attempt.payload())
                ):
                    raise ShadowDeterminismConflict("simulation-attempt replay conflict")
                self._verify_attempt_components(lease, validity, envelope)
                return attempt.attempt_id
            plan = self._conn.execute(
                "SELECT intent_id,content_fingerprint,plan_json FROM shadow_t003_plans WHERE plan_id=?",
                (attempt.plan_id,),
            ).fetchone()
            if plan is None:
                raise ShadowDeterminismConflict("simulation attempt lacks persisted plan")
            if self._state(str(plan[0])) is not ShadowState.PLAN_BUILT:
                raise ShadowDeterminismConflict("new simulation attempt requires PLAN_BUILT")
            plan_json = json.loads(str(plan[2]))
            try:
                config_json = json.loads(envelope.request_config_json)
                exact_minimum = max(int(plan_json["prerequisite_slot"]), lease.context_slot)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ShadowDeterminismConflict("simulation causal config is malformed") from exc
            if not (
                envelope.plan_fingerprint == str(plan[1])
                and envelope.lease_fingerprint == lease.fingerprint
                and config_json.get("minContextSlot") == exact_minimum
                and validity.required_min_context_slot == exact_minimum
                and validity.validity_context_slot >= exact_minimum
            ):
                raise ShadowDeterminismConflict("simulation causal lineage mismatch")
            indexed = self._conn.execute(
                "SELECT attempt_id,content_fingerprint,attempt_json FROM shadow_t003_attempts "
                "WHERE plan_id=? AND attempt_index=?",
                (attempt.plan_id, attempt.attempt_index),
            ).fetchone()
            if indexed is not None and tuple(str(value) for value in indexed) != (
                attempt.attempt_id, attempt.fingerprint, canonical_json(attempt.payload())
            ):
                raise ShadowDeterminismConflict("plan attempt-index replay conflict")
            self._insert_or_exact(
                "shadow_t003_blockhash_leases", "lease_id", lease.lease_id,
                lease.fingerprint, canonical_json(lease.payload()),
                "INSERT INTO shadow_t003_blockhash_leases VALUES(?,?,?,?,?,?)",
                (
                    lease.lease_id, lease.plan_id, lease.context_slot,
                    lease.last_valid_block_height, lease.fingerprint,
                    canonical_json(lease.payload()),
                ),
                "content_fingerprint", "lease_json",
            )
            self._insert_or_exact(
                "shadow_t003_blockhash_validity", "content_fingerprint", validity.fingerprint,
                validity.fingerprint, canonical_json(validity.payload()),
                "INSERT INTO shadow_t003_blockhash_validity VALUES(?,?,?)",
                (validity.fingerprint, validity.lease_id, canonical_json(validity.payload())),
                "content_fingerprint", "validity_json",
            )
            self._insert_or_exact(
                "shadow_t003_envelopes", "envelope_id", envelope.envelope_id,
                envelope.fingerprint, canonical_json(envelope.payload()),
                "INSERT INTO shadow_t003_envelopes VALUES(?,?,?,?,?,?)",
                (
                    envelope.envelope_id, envelope.plan_id, envelope.lease_id,
                    envelope.request_config_fingerprint, envelope.fingerprint,
                    canonical_json(envelope.payload()),
                ),
                "content_fingerprint", "envelope_json",
            )
            self._conn.execute(
                "INSERT INTO shadow_t003_attempts VALUES(?,?,?,?,?,?,?,?)",
                (
                    attempt.attempt_id, attempt.plan_id, attempt.lease_id,
                    attempt.envelope_id, attempt.validity_fingerprint,
                    attempt.attempt_index, attempt.fingerprint,
                    canonical_json(attempt.payload()),
                ),
            )
        return attempt.attempt_id

    def _insert_or_exact(
        self,
        table: str,
        identity_column: str,
        identity: str,
        fingerprint: str,
        payload: str,
        insert_sql: str,
        insert_values: tuple[Any, ...],
        fingerprint_column: str,
        json_column: str,
    ) -> None:
        row = self._conn.execute(
            f"SELECT {fingerprint_column},{json_column} FROM {table} WHERE {identity_column}=?",
            (identity,),
        ).fetchone()
        if row is None:
            self._conn.execute(insert_sql, insert_values)
        elif (str(row[0]), str(row[1])) != (fingerprint, payload):
            raise ShadowDeterminismConflict(f"{table} replay conflict")

    def _verify_attempt_components(
        self,
        lease: BlockhashLeaseV01,
        validity: BlockhashValidityEvidenceV01,
        envelope: SimulationEnvelopeV01,
    ) -> None:
        specifications = (
            ("shadow_t003_blockhash_leases", "lease_id", lease.lease_id, lease.fingerprint, canonical_json(lease.payload()), "lease_json"),
            ("shadow_t003_blockhash_validity", "content_fingerprint", validity.fingerprint, validity.fingerprint, canonical_json(validity.payload()), "validity_json"),
            ("shadow_t003_envelopes", "envelope_id", envelope.envelope_id, envelope.fingerprint, canonical_json(envelope.payload()), "envelope_json"),
        )
        for table, key, identity, fingerprint, payload, json_column in specifications:
            row = self._conn.execute(
                f"SELECT content_fingerprint,{json_column} FROM {table} WHERE {key}=?", (identity,)
            ).fetchone()
            if row is None or (str(row[0]), str(row[1])) != (fingerprint, payload):
                raise ShadowDeterminismConflict("persisted attempt component conflict")

    def persist_result(self, result: ShadowSimulationResultV01) -> str:
        self._require_open()
        if not isinstance(result, ShadowSimulationResultV01):
            raise TypeError("result must be ShadowSimulationResultV01")
        payload = canonical_json(result.payload())
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            existing = self._conn.execute(
                "SELECT content_fingerprint,result_json FROM shadow_t003_results WHERE result_id=?",
                (result.result_id,),
            ).fetchone()
            if existing is not None:
                if (str(existing[0]), str(existing[1])) != (result.fingerprint, payload):
                    raise ShadowDeterminismConflict("simulation-result replay conflict")
                return result.result_id
            attempt = self._conn.execute(
                "SELECT plan_id,lease_id,envelope_id,attempt_json "
                "FROM shadow_t003_attempts WHERE attempt_id=?",
                (result.attempt_id,),
            ).fetchone()
            if attempt is None:
                raise ShadowDeterminismConflict("result lacks exact attempt lineage")
            try:
                persisted_attempt = json.loads(str(attempt[3]))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ShadowDeterminismConflict("persisted attempt JSON is malformed") from exc
            if tuple(str(value) for value in attempt[:3]) != (
                result.plan_id, result.lease_id, result.envelope_id,
            ) or persisted_attempt.get("request_config_fingerprint") != result.request_config_fingerprint:
                raise ShadowDeterminismConflict("result lacks exact attempt lineage")
            intent = self._conn.execute(
                "SELECT intent_id FROM shadow_t003_plans WHERE plan_id=?", (result.plan_id,)
            ).fetchone()
            if intent is None or self._state(str(intent[0])) is not ShadowState.PLAN_BUILT:
                raise ShadowDeterminismConflict("new result requires PLAN_BUILT")
            attempted = self._conn.execute(
                "SELECT result_id,content_fingerprint,result_json FROM shadow_t003_results "
                "WHERE attempt_id=?",
                (result.attempt_id,),
            ).fetchone()
            if attempted is not None and tuple(str(value) for value in attempted) != (
                result.result_id, result.fingerprint, payload
            ):
                raise ShadowDeterminismConflict("simulation-attempt result replay conflict")
            self._conn.execute(
                "INSERT INTO shadow_t003_results VALUES(?,?,?,?,?)",
                (
                    result.result_id, result.attempt_id, result.outcome.value,
                    result.fingerprint, payload,
                ),
            )
        return result.result_id

    def persist_run(self, run: SimulationRunEvidenceV01) -> tuple[str, ...]:
        self._require_open()
        if not isinstance(run, SimulationRunEvidenceV01):
            raise TypeError("run must be SimulationRunEvidenceV01")
        if not (
            len(run.leases) == len(run.validity) == len(run.envelopes)
            == len(run.attempts) == len(run.results)
        ):
            raise ShadowDeterminismConflict("simulation run evidence lengths disagree")
        identities = []
        for lease, validity, envelope, attempt, result in zip(
            run.leases, run.validity, run.envelopes, run.attempts, run.results, strict=True
        ):
            self.persist_attempt_evidence(lease, validity, envelope, attempt)
            identities.append(self.persist_result(result))
        return tuple(identities)

    def get_json(self, table: str, identity: str) -> dict[str, Any] | None:
        self._require_open()
        allowed = {
            "plan": ("shadow_t003_plans", "plan_id", "plan_json"),
            "lease": ("shadow_t003_blockhash_leases", "lease_id", "lease_json"),
            "validity": ("shadow_t003_blockhash_validity", "content_fingerprint", "validity_json"),
            "envelope": ("shadow_t003_envelopes", "envelope_id", "envelope_json"),
            "attempt": ("shadow_t003_attempts", "attempt_id", "attempt_json"),
            "result": ("shadow_t003_results", "result_id", "result_json"),
        }
        if table not in allowed:
            raise ValueError("unknown T003 evidence table")
        table_name, key, column = allowed[table]
        row = self._conn.execute(
            f"SELECT {column} FROM {table_name} WHERE {key}=?", (identity,)
        ).fetchone()
        if row is None:
            return None
        value = json.loads(str(row[0]))
        if not isinstance(value, dict) or canonical_json(value) != str(row[0]):
            raise ShadowDeterminismConflict("persisted T003 JSON is noncanonical")
        return value

    def quick_check(self) -> str:
        self._require_open()
        return str(self._conn.execute("PRAGMA quick_check").fetchone()[0])

    @property
    def foreign_keys_enabled(self) -> bool:
        self._require_open()
        return bool(self._conn.execute("PRAGMA foreign_keys").fetchone()[0])

    def audit(self) -> bool:
        self._require_open()
        _instruction_layout(self._conn)
        if self.quick_check() != "ok" or self._conn.execute("PRAGMA foreign_key_check").fetchall():
            raise ShadowDeterminismConflict("T003 SQLite integrity check failed")
        if self._conn.execute("""SELECT instruction_id FROM shadow_t003_plan_instructions
            GROUP BY instruction_id HAVING COUNT(DISTINCT content_fingerprint)>1
            OR COUNT(DISTINCT instruction_json)>1 LIMIT 1""").fetchone():
            raise ShadowDeterminismConflict("shared instruction identity conflicts across plans")
        specifications = (
            ("shadow_t003_plans", "plan_id", "plan_json", "content_fingerprint", "P5PL", PLAN_SCHEMA_VERSION),
            ("shadow_t003_plan_instructions", "instruction_id", "instruction_json", "content_fingerprint", "P5IX", "v0.1"),
            ("shadow_t003_blockhash_leases", "lease_id", "lease_json", "content_fingerprint", "P5BH", LEASE_SCHEMA_VERSION),
            ("shadow_t003_envelopes", "envelope_id", "envelope_json", "content_fingerprint", "P5SE", ENVELOPE_SCHEMA_VERSION),
            ("shadow_t003_attempts", "attempt_id", "attempt_json", "content_fingerprint", "P5SA", ATTEMPT_SCHEMA_VERSION),
            ("shadow_t003_results", "result_id", "result_json", "content_fingerprint", "P5SR", RESULT_SCHEMA_VERSION),
        )
        for table, identity_column, json_column, fingerprint_column, prefix, version in specifications:
            for row in self._conn.execute(
                f"SELECT {identity_column},{json_column},{fingerprint_column} FROM {table}"
            ):
                value = json.loads(str(row[json_column]))
                if not isinstance(value, dict) or canonical_json(value) != str(row[json_column]):
                    raise ShadowDeterminismConflict("persisted T003 evidence is noncanonical")
                fingerprint = content_fingerprint(value)
                if (
                    fingerprint != str(row[fingerprint_column])
                    or deterministic_id(prefix, version, fingerprint) != str(row[identity_column])
                ):
                    raise ShadowDeterminismConflict("persisted T003 identity/fingerprint conflict")
        for row in self._conn.execute(
            "SELECT content_fingerprint,validity_json FROM shadow_t003_blockhash_validity"
        ):
            value = json.loads(str(row[1]))
            if canonical_json(value) != str(row[1]) or content_fingerprint(value) != str(row[0]):
                raise ShadowDeterminismConflict("persisted validity evidence conflict")
        for row in self._conn.execute("SELECT plan_id,plan_json FROM shadow_t003_plans"):
            plan = json.loads(str(row[1]))
            rows = self._conn.execute(
                    "SELECT sequence,instruction_id,content_fingerprint,instruction_json FROM shadow_t003_plan_instructions "
                    "WHERE plan_id=? ORDER BY sequence", (str(row[0]),)
                ).fetchall()
            instructions = plan.get("instructions")
            if not isinstance(instructions, list):
                raise ShadowDeterminismConflict("plan instructions malformed")
            expected = [(index, deterministic_id("P5IX", "v0.1", content_fingerprint(item)),
                         content_fingerprint(item), canonical_json(item)) for index, item in enumerate(instructions)]
            if ([tuple(item) for item in rows] != expected
                    or any(item.get("sequence") != index for index, item in enumerate(instructions))):
                raise ShadowDeterminismConflict("plan instruction table diverges from plan")
        return True

    def canonical_digest(self) -> str:
        self._require_open()
        self.audit()
        tables = (
            "shadow_t003_plans", "shadow_t003_plan_instructions",
            "shadow_t003_blockhash_leases", "shadow_t003_blockhash_validity",
            "shadow_t003_envelopes", "shadow_t003_attempts", "shadow_t003_results",
        )
        payload: dict[str, Any] = {
            "model_id": MODEL_ID,
            "model_fingerprint": MODEL_FINGERPRINT,
        }
        for table in tables:
            payload[table] = [dict(row) for row in self._conn.execute(
                f"SELECT * FROM {table} ORDER BY 1,2"
            )]
        return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()

    def close(self) -> None:
        if not self._closed:
            self._conn.close()
            self._closed = True

    def __enter__(self) -> ShadowSimulationRepositoryV01:
        self._require_open()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def record_plan_state(
    shadow: ShadowRepositoryV01,
    repository: ShadowSimulationRepositoryV01,
    plan: UnsignedTransactionPlanV01,
    *,
    effective_at_us: int,
) -> str:
    identity = repository.persist_plan(plan)
    shadow.transition(
        plan.intent_id,
        ShadowState.PLAN_BUILT,
        idempotency_key=f"T003_PLAN:{plan.plan_id}",
        effective_at_us=effective_at_us,
        reason_code="IMMUTABLE_UNSIGNED_PLAN_PERSISTED",
        evidence={"plan_id": plan.plan_id, "plan_fingerprint": plan.fingerprint},
    )
    return identity


def record_simulation_state(
    shadow: ShadowRepositoryV01,
    repository: ShadowSimulationRepositoryV01,
    plan: UnsignedTransactionPlanV01,
    run: SimulationRunEvidenceV01,
    *,
    effective_at_us: int,
) -> ShadowState:
    repository.persist_run(run)
    result = run.final_result
    returned_evidence = result.context_slot is not None and result.outcome in (
        SimulationOutcome.SIMULATION_SUCCESS,
        SimulationOutcome.PROGRAM_REJECTED,
        SimulationOutcome.MALFORMED_RESPONSE,
        SimulationOutcome.CONTEXT_REGRESSION,
    )
    if returned_evidence:
        shadow.transition(
            plan.intent_id,
            ShadowState.SIMULATED,
            idempotency_key=f"T003_SIMULATED:{result.attempt_id}",
            effective_at_us=effective_at_us,
            reason_code="SIMULATION_RPC_EVIDENCE_PERSISTED",
            evidence={
                "attempt_id": result.attempt_id,
                "result_id": result.result_id,
                "outcome": result.outcome.value,
            },
        )
        target = ShadowState.COMPLETED if result.outcome is SimulationOutcome.SIMULATION_SUCCESS else ShadowState.FAILED
        shadow.transition(
            plan.intent_id,
            target,
            idempotency_key=f"T003_FINAL:{result.result_id}",
            effective_at_us=effective_at_us + 1,
            reason_code=(
                "SHADOW_SIMULATION_SUCCESS" if target is ShadowState.COMPLETED
                else "SHADOW_SIMULATION_FAIL_CLOSED"
            ),
            evidence={"result_id": result.result_id, "outcome": result.outcome.value},
        )
        return target
    target = ShadowState.EXPIRED if result.outcome is SimulationOutcome.BLOCKHASH_EXPIRED else ShadowState.FAILED
    shadow.transition(
        plan.intent_id,
        target,
        idempotency_key=f"T003_FINAL:{result.result_id}",
        effective_at_us=effective_at_us,
        reason_code=(
            "BLOCKHASH_REFRESH_EXHAUSTED" if target is ShadowState.EXPIRED
            else "SHADOW_SIMULATION_OPERATIONAL_FAILURE"
        ),
        evidence={"result_id": result.result_id, "outcome": result.outcome.value},
    )
    return target
