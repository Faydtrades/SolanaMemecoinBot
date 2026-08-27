from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping

from .shadow_domain_v0_1 import (
    INTENT_SCHEMA_VERSION,
    MODEL_FINGERPRINT,
    MODEL_ID,
    SCHEMA_VERSION,
    STATE_MACHINE_VERSION,
    ExecutionIntentV01,
    IntentRole,
    InvalidStateTimestamp,
    ShadowDeterminismConflict,
    ShadowState,
    ShadowStateMachineV01,
    ShadowTransitionV01,
    canonical_json,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SHADOW_DATABASE_PATH = (
    PROJECT_ROOT / "data" / "shadow" / "phase5_shadow_execution_v0_1.sqlite3"
)
_REPOSITORY_OPEN_TOKEN = object()


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _validate_database_path(path: Path) -> Path:
    resolved = path.resolve()
    protected_roots = (
        (PROJECT_ROOT / "data" / "paper").resolve(),
        (PROJECT_ROOT / "data" / "db").resolve(),
    )
    if any(_is_within(resolved, root) for root in protected_roots):
        raise ValueError("shadow database cannot use a Phase-4/source data path")
    return resolved


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS shadow_schema_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            model_id TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            model_fingerprint TEXT NOT NULL,
            intent_schema_version TEXT NOT NULL,
            state_machine_version TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS shadow_execution_intents (
            intent_id TEXT PRIMARY KEY,
            candidate_signal_id TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('ENTRY','EXIT')),
            parent_entry_intent_id TEXT,
            exit_decision_id TEXT,
            decision_at_us INTEGER NOT NULL CHECK(decision_at_us >= 0),
            content_fingerprint TEXT NOT NULL,
            intent_json TEXT NOT NULL,
            FOREIGN KEY(parent_entry_intent_id)
                REFERENCES shadow_execution_intents(intent_id),
            CHECK(
                (role = 'ENTRY' AND parent_entry_intent_id IS NULL
                    AND exit_decision_id IS NULL)
                OR
                (role = 'EXIT' AND parent_entry_intent_id IS NOT NULL
                    AND exit_decision_id IS NOT NULL)
            )
        );

        CREATE UNIQUE INDEX IF NOT EXISTS uq_shadow_shared_entry_candidate
            ON shadow_execution_intents(candidate_signal_id)
            WHERE role = 'ENTRY';

        CREATE UNIQUE INDEX IF NOT EXISTS uq_shadow_exit_decision
            ON shadow_execution_intents(exit_decision_id)
            WHERE role = 'EXIT';

        CREATE TABLE IF NOT EXISTS shadow_state_machines (
            intent_id TEXT PRIMARY KEY,
            current_state TEXT NOT NULL CHECK(current_state IN (
                'CREATED','ELIGIBILITY_CHECKED','ROUTE_BOUND','QUOTE_BOUND',
                'PLAN_BUILT','SIMULATED','COMPLETED','REJECTED','EXPIRED','FAILED'
            )),
            last_transition_at_us INTEGER NOT NULL,
            revision INTEGER NOT NULL CHECK(revision >= 0),
            FOREIGN KEY(intent_id)
                REFERENCES shadow_execution_intents(intent_id)
        );

        CREATE TABLE IF NOT EXISTS shadow_transition_history (
            transition_id TEXT PRIMARY KEY,
            intent_id TEXT NOT NULL,
            sequence INTEGER NOT NULL CHECK(sequence >= 0),
            idempotency_key TEXT NOT NULL,
            from_state TEXT CHECK(from_state IS NULL OR from_state IN (
                'CREATED','ELIGIBILITY_CHECKED','ROUTE_BOUND','QUOTE_BOUND',
                'PLAN_BUILT','SIMULATED','COMPLETED','REJECTED','EXPIRED','FAILED'
            )),
            to_state TEXT NOT NULL CHECK(to_state IN (
                'CREATED','ELIGIBILITY_CHECKED','ROUTE_BOUND','QUOTE_BOUND',
                'PLAN_BUILT','SIMULATED','COMPLETED','REJECTED','EXPIRED','FAILED'
            )),
            effective_at_us INTEGER NOT NULL CHECK(effective_at_us >= 0),
            reason_code TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            FOREIGN KEY(intent_id)
                REFERENCES shadow_execution_intents(intent_id),
            UNIQUE(intent_id, sequence),
            UNIQUE(intent_id, idempotency_key)
        );

        CREATE INDEX IF NOT EXISTS idx_shadow_transition_intent
            ON shadow_transition_history(intent_id, sequence);

        CREATE TRIGGER IF NOT EXISTS shadow_intents_no_update
        BEFORE UPDATE ON shadow_execution_intents
        BEGIN
            SELECT RAISE(ABORT, 'shadow execution intents are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS shadow_intents_no_delete
        BEFORE DELETE ON shadow_execution_intents
        BEGIN
            SELECT RAISE(ABORT, 'shadow execution intents are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS shadow_history_no_update
        BEFORE UPDATE ON shadow_transition_history
        BEGIN
            SELECT RAISE(ABORT, 'shadow transition history is append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS shadow_history_no_delete
        BEFORE DELETE ON shadow_transition_history
        BEGIN
            SELECT RAISE(ABORT, 'shadow transition history is append-only');
        END;
        """
    )
    expected = (
        MODEL_ID,
        SCHEMA_VERSION,
        MODEL_FINGERPRINT,
        INTENT_SCHEMA_VERSION,
        STATE_MACHINE_VERSION,
    )
    row = conn.execute(
        "SELECT model_id,schema_version,model_fingerprint,"
        "intent_schema_version,state_machine_version "
        "FROM shadow_schema_meta WHERE singleton=1"
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO shadow_schema_meta VALUES(1,?,?,?,?,?)",
            expected,
        )
    elif tuple(row) != expected:
        raise ShadowDeterminismConflict("shadow repository contract mismatch")
    conn.commit()


def open_shadow_repository(
    path: str | Path = DEFAULT_SHADOW_DATABASE_PATH,
) -> ShadowRepositoryV01:
    resolved = _validate_database_path(Path(path))
    resolved.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(resolved, timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        mode = str(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
        conn.execute("PRAGMA synchronous=FULL")
        foreign_keys = int(conn.execute("PRAGMA foreign_keys").fetchone()[0])
        synchronous = int(conn.execute("PRAGMA synchronous").fetchone()[0])
        if mode != "wal" or synchronous != 2 or foreign_keys != 1:
            raise RuntimeError(
                "shadow repository requires WAL, synchronous=FULL, and foreign keys"
            )
        _create_schema(conn)
        return ShadowRepositoryV01(
            conn,
            resolved,
            _open_token=_REPOSITORY_OPEN_TOKEN,
        )
    except BaseException:
        conn.close()
        raise


class ShadowRepositoryV01:
    def __init__(
        self,
        conn: sqlite3.Connection,
        database_path: Path,
        *,
        _open_token: object,
    ) -> None:
        if _open_token is not _REPOSITORY_OPEN_TOKEN:
            raise TypeError("use open_shadow_repository()")
        resolved = _validate_database_path(database_path)
        main_rows = [
            row
            for row in conn.execute("PRAGMA database_list").fetchall()
            if str(row[1]) == "main"
        ]
        if len(main_rows) != 1 or Path(str(main_rows[0][2])).resolve() != resolved:
            raise ValueError("shadow repository connection/path mismatch")
        self._conn = conn
        self.database_path = resolved
        self._closed = False

    @property
    def foreign_keys_enabled(self) -> bool:
        self._require_open()
        return bool(self._conn.execute("PRAGMA foreign_keys").fetchone()[0])

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("shadow repository is closed")

    def register_intent(self, intent: ExecutionIntentV01) -> ExecutionIntentV01:
        self._require_open()
        if not isinstance(intent, ExecutionIntentV01):
            raise TypeError("intent must be ExecutionIntentV01")
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            existing = self._intent_row(intent.intent_id)
            if existing is not None:
                persisted = self._intent_from_row(existing)
                if persisted.serialize() != intent.serialize():
                    raise ShadowDeterminismConflict(
                        f"execution intent replay conflict: {intent.intent_id}"
                    )
                return persisted

            if intent.role is IntentRole.ENTRY:
                same_candidate = self._conn.execute(
                    "SELECT * FROM shadow_execution_intents "
                    "WHERE role='ENTRY' AND candidate_signal_id=?",
                    (intent.candidate_signal_id,),
                ).fetchone()
                if same_candidate is not None:
                    raise ShadowDeterminismConflict(
                        "canonical candidate already has a different shared ENTRY intent"
                    )
            else:
                parent_row = self._intent_row(str(intent.parent_entry_intent_id))
                if parent_row is None:
                    raise ShadowDeterminismConflict(
                        "EXIT intent references an unknown parent ENTRY intent"
                    )
                parent = self._intent_from_row(parent_row)
                if (
                    parent.role is not IntentRole.ENTRY
                    or parent.candidate_signal_id != intent.candidate_signal_id
                    or parent.mint != intent.mint
                ):
                    raise ShadowDeterminismConflict(
                        "EXIT intent conflicts with parent ENTRY lineage"
                    )
                same_exit = self._conn.execute(
                    "SELECT * FROM shadow_execution_intents "
                    "WHERE role='EXIT' AND exit_decision_id=?",
                    (intent.exit_decision_id,),
                ).fetchone()
                if same_exit is not None:
                    raise ShadowDeterminismConflict(
                        "canonical exit decision already has a different intent"
                    )

            self._conn.execute(
                "INSERT INTO shadow_execution_intents("
                "intent_id,candidate_signal_id,role,parent_entry_intent_id,"
                "exit_decision_id,decision_at_us,content_fingerprint,intent_json) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (
                    intent.intent_id,
                    intent.candidate_signal_id,
                    intent.role.value,
                    intent.parent_entry_intent_id,
                    intent.exit_decision_id,
                    intent.decision_at_us,
                    intent.fingerprint,
                    intent.serialize(),
                ),
            )
            creation = ShadowTransitionV01.create(
                intent_id=intent.intent_id,
                sequence=0,
                idempotency_key="INTENT_CREATED",
                from_state=None,
                to_state=ShadowState.CREATED,
                effective_at_us=intent.decision_at_us,
                reason_code="IMMUTABLE_INTENT_PERSISTED",
                evidence={"intent_fingerprint": intent.fingerprint},
            )
            self._insert_transition(creation)
            self._conn.execute(
                "INSERT INTO shadow_state_machines("
                "intent_id,current_state,last_transition_at_us,revision) "
                "VALUES(?,?,?,0)",
                (
                    intent.intent_id,
                    ShadowState.CREATED.value,
                    intent.decision_at_us,
                ),
            )
        return intent

    def transition(
        self,
        intent_id: str,
        target: ShadowState,
        *,
        idempotency_key: str,
        effective_at_us: int,
        reason_code: str,
        evidence: Mapping[str, Any],
    ) -> ShadowTransitionV01:
        self._require_open()
        if not isinstance(intent_id, str) or not intent_id:
            raise ValueError("intent_id is required")
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise ValueError("idempotency_key is required")
        if not isinstance(reason_code, str) or not reason_code:
            raise ValueError("reason_code is required")
        if type(effective_at_us) is not int or effective_at_us < 0:
            raise ValueError("effective_at_us must be a non-negative integer")
        destination = ShadowState(target)
        evidence_copy = dict(evidence)

        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            machine = self._conn.execute(
                "SELECT * FROM shadow_state_machines WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            if machine is None:
                raise KeyError(f"unknown execution intent: {intent_id}")
            existing = self._conn.execute(
                "SELECT * FROM shadow_transition_history "
                "WHERE intent_id=? AND idempotency_key=?",
                (intent_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                persisted = self._transition_from_row(existing)
                replay = ShadowTransitionV01.create(
                    intent_id=intent_id,
                    sequence=persisted.sequence,
                    idempotency_key=idempotency_key,
                    from_state=persisted.from_state,
                    to_state=destination,
                    effective_at_us=effective_at_us,
                    reason_code=reason_code,
                    evidence=evidence_copy,
                )
                if replay.fingerprint != persisted.fingerprint:
                    raise ShadowDeterminismConflict(
                        f"transition idempotency conflict: {idempotency_key}"
                    )
                return persisted

            current = ShadowState(str(machine["current_state"]))
            ShadowStateMachineV01.validate(current, destination)
            if effective_at_us < int(machine["last_transition_at_us"]):
                raise InvalidStateTimestamp(
                    "transition timestamp precedes current state transition"
                )
            transition = ShadowTransitionV01.create(
                intent_id=intent_id,
                sequence=int(machine["revision"]) + 1,
                idempotency_key=idempotency_key,
                from_state=current,
                to_state=destination,
                effective_at_us=effective_at_us,
                reason_code=reason_code,
                evidence=evidence_copy,
            )
            self._insert_transition(transition)
            updated = self._conn.execute(
                "UPDATE shadow_state_machines SET current_state=?,"
                "last_transition_at_us=?,revision=? "
                "WHERE intent_id=? AND revision=? AND current_state=?",
                (
                    destination.value,
                    effective_at_us,
                    transition.sequence,
                    intent_id,
                    int(machine["revision"]),
                    current.value,
                ),
            )
            if updated.rowcount != 1:
                raise ShadowDeterminismConflict("atomic shadow state update failed")
        return transition

    def get_intent(self, intent_id: str) -> ExecutionIntentV01 | None:
        self._require_open()
        row = self._intent_row(intent_id)
        return None if row is None else self._intent_from_row(row)

    def current_state(self, intent_id: str) -> ShadowState:
        self._require_open()
        row = self._conn.execute(
            "SELECT current_state FROM shadow_state_machines WHERE intent_id=?",
            (intent_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown execution intent: {intent_id}")
        return ShadowState(str(row["current_state"]))

    def history(self, intent_id: str) -> tuple[ShadowTransitionV01, ...]:
        self._require_open()
        rows = self._conn.execute(
            "SELECT * FROM shadow_transition_history WHERE intent_id=? "
            "ORDER BY sequence",
            (intent_id,),
        ).fetchall()
        return tuple(self._transition_from_row(row) for row in rows)

    def reconstruct_state(self, intent_id: str) -> ShadowState:
        history = self.history(intent_id)
        if not history:
            raise ShadowDeterminismConflict("intent has no transition history")
        first = history[0]
        if (
            first.sequence != 0
            or first.from_state is not None
            or first.to_state is not ShadowState.CREATED
        ):
            raise ShadowDeterminismConflict("invalid initial CREATED transition")
        current = first.to_state
        previous_at = first.effective_at_us
        for expected_sequence, item in enumerate(history[1:], start=1):
            if item.sequence != expected_sequence:
                raise ShadowDeterminismConflict("transition history sequence gap")
            if item.from_state is not current:
                raise ShadowDeterminismConflict("transition history state discontinuity")
            if item.effective_at_us < previous_at:
                raise ShadowDeterminismConflict("transition history time regressed")
            ShadowStateMachineV01.validate(current, item.to_state)
            current = item.to_state
            previous_at = item.effective_at_us
        return current

    def audit_intent(self, intent_id: str) -> bool:
        reconstructed = self.reconstruct_state(intent_id)
        row = self._conn.execute(
            "SELECT current_state,revision FROM shadow_state_machines "
            "WHERE intent_id=?",
            (intent_id,),
        ).fetchone()
        if row is None:
            raise ShadowDeterminismConflict("intent lacks state machine")
        history = self.history(intent_id)
        if ShadowState(str(row["current_state"])) is not reconstructed:
            raise ShadowDeterminismConflict("cached state differs from history")
        if int(row["revision"]) != len(history) - 1:
            raise ShadowDeterminismConflict("cached revision differs from history")
        return True

    def quick_check(self) -> str:
        self._require_open()
        return str(self._conn.execute("PRAGMA quick_check").fetchone()[0])

    def foreign_key_violations(self) -> tuple[tuple[Any, ...], ...]:
        self._require_open()
        return tuple(
            tuple(row) for row in self._conn.execute("PRAGMA foreign_key_check")
        )

    def canonical_digest(self) -> str:
        self._require_open()
        payload = {
            "model_id": MODEL_ID,
            "model_fingerprint": MODEL_FINGERPRINT,
            "intents": [
                dict(row)
                for row in self._conn.execute(
                    "SELECT * FROM shadow_execution_intents ORDER BY intent_id"
                )
            ],
            "machines": [
                dict(row)
                for row in self._conn.execute(
                    "SELECT * FROM shadow_state_machines ORDER BY intent_id"
                )
            ],
            "history": [
                dict(row)
                for row in self._conn.execute(
                    "SELECT * FROM shadow_transition_history "
                    "ORDER BY intent_id,sequence"
                )
            ],
        }
        return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()

    def _intent_row(self, intent_id: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM shadow_execution_intents WHERE intent_id=?",
            (intent_id,),
        ).fetchone()

    @staticmethod
    def _intent_from_row(row: sqlite3.Row) -> ExecutionIntentV01:
        record = json.loads(str(row["intent_json"]))
        if not isinstance(record, dict):
            raise ShadowDeterminismConflict("persisted intent is not an object")
        intent = ExecutionIntentV01.from_record(record)
        if (
            intent.serialize() != canonical_json(record)
            or
            intent.intent_id != str(row["intent_id"])
            or intent.candidate_signal_id != str(row["candidate_signal_id"])
            or intent.role.value != str(row["role"])
            or intent.parent_entry_intent_id != row["parent_entry_intent_id"]
            or intent.exit_decision_id != row["exit_decision_id"]
            or intent.decision_at_us != int(row["decision_at_us"])
            or intent.fingerprint != str(row["content_fingerprint"])
        ):
            raise ShadowDeterminismConflict("persisted intent columns conflict")
        return intent

    def _insert_transition(self, transition: ShadowTransitionV01) -> None:
        self._conn.execute(
            "INSERT INTO shadow_transition_history("
            "transition_id,intent_id,sequence,idempotency_key,from_state,"
            "to_state,effective_at_us,reason_code,evidence_json,"
            "content_fingerprint) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                transition.transition_id,
                transition.intent_id,
                transition.sequence,
                transition.idempotency_key,
                None
                if transition.from_state is None
                else transition.from_state.value,
                transition.to_state.value,
                transition.effective_at_us,
                transition.reason_code,
                transition.evidence_json,
                transition.fingerprint,
            ),
        )

    @staticmethod
    def _transition_from_row(row: sqlite3.Row) -> ShadowTransitionV01:
        evidence = json.loads(str(row["evidence_json"]))
        if not isinstance(evidence, dict):
            raise ShadowDeterminismConflict("transition evidence is not an object")
        transition = ShadowTransitionV01.create(
            intent_id=str(row["intent_id"]),
            sequence=int(row["sequence"]),
            idempotency_key=str(row["idempotency_key"]),
            from_state=(
                None
                if row["from_state"] is None
                else ShadowState(str(row["from_state"]))
            ),
            to_state=ShadowState(str(row["to_state"])),
            effective_at_us=int(row["effective_at_us"]),
            reason_code=str(row["reason_code"]),
            evidence=evidence,
        )
        if (
            transition.transition_id != str(row["transition_id"])
            or transition.fingerprint != str(row["content_fingerprint"])
            or transition.evidence_json != str(row["evidence_json"])
        ):
            raise ShadowDeterminismConflict("transition fingerprint conflict")
        return transition

    def close(self) -> None:
        if not self._closed:
            self._conn.close()
            self._closed = True

    def __enter__(self) -> ShadowRepositoryV01:
        self._require_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
