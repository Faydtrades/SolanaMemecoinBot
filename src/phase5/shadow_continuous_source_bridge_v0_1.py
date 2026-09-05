from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from .shadow_domain_v0_1 import (
    ExecutionIntentV01,
    IntentRole,
    IntentSide,
    ShadowDeterminismConflict,
    canonical_json,
    content_fingerprint,
    deterministic_id,
)
from .shadow_repository_v0_1 import ShadowRepositoryV01, open_shadow_repository


MODEL_ID = "P5-SHADOW-CONTINUOUS-SOURCE-BRIDGE-0001"
SCHEMA_VERSION = "phase5_shadow_continuous_source_bridge_v0.1"

CONTRACT_SPEC: Mapping[str, Any] = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "direction": "PHASE4_PERSISTED_PAPER_TO_PHASE5_SHADOW_ONLY",
    "source": (
        "paper_continuous_signal_contexts_v0_1 JOIN paper_entry_routes "
        "ON route_id"
    ),
    "source_access": "SQLITE_URI_MODE_RO_AND_QUERY_ONLY",
    "ordering": ["source_cursor", "signal_key"],
    "cardinality": "ONE_SHARED_SHADOW_ENTRY_PER_PHASE4_SIGNAL_ROUTE",
    "write_order": ["execution_intent", "immutable_lineage", "cursor"],
    "source_identity": [
        "resolved_database_path",
        "filesystem_device",
        "filesystem_inode",
        "candidate_run_id",
    ],
    "paper_outcome_semantics": "LINEAGE_EVIDENCE_ONLY",
    "shadow_persistence_root": "data/shadow",
    "source_shadow_file_alias": "REJECT",
    "capabilities": ["READ_PHASE4", "WRITE_SHADOW_EVIDENCE"],
}
MODEL_FINGERPRINT = content_fingerprint(CONTRACT_SPEC)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SHADOW_DATA_ROOT = (PROJECT_ROOT / "data" / "shadow").resolve()
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_SOURCE_SQL = """
SELECT
    c.signal_key AS signal_key,
    c.route_id AS context_route_id,
    c.source_evaluation_id AS source_evaluation_id,
    c.run_id AS run_id,
    c.mint AS context_mint,
    c.strategy_version AS context_strategy_version,
    c.parameter_set_id AS context_parameter_set_id,
    c.source_cursor AS source_cursor,
    c.source_event_key AS source_event_key,
    r.route_id AS route_id,
    r.candidate_id AS candidate_id,
    r.mint AS route_mint,
    r.strategy_version AS route_strategy_version,
    r.parameter_set_id AS route_parameter_set_id,
    r.signal_observed_at AS signal_observed_at,
    r.signal_ingest_seq AS signal_ingest_seq,
    r.requested_size_lamports AS requested_size_lamports,
    r.state AS paper_route_state,
    r.state_reason AS paper_route_reason
FROM paper_continuous_signal_contexts_v0_1 AS c
JOIN paper_entry_routes AS r ON r.route_id = c.route_id
WHERE c.run_id = ?
  AND (
      c.source_cursor > ?
      OR (c.source_cursor = ? AND c.signal_key > ?)
  )
ORDER BY c.source_cursor, c.signal_key
LIMIT ?
"""

_REQUIRED_SOURCE_COLUMNS = frozenset(
    {
        "signal_key",
        "route_id",
        "source_evaluation_id",
        "run_id",
        "mint",
        "strategy_version",
        "parameter_set_id",
        "source_cursor",
        "source_event_key",
    }
)
_REQUIRED_ROUTE_COLUMNS = frozenset(
    {
        "route_id",
        "candidate_id",
        "mint",
        "strategy_version",
        "parameter_set_id",
        "signal_observed_at",
        "signal_ingest_seq",
        "requested_size_lamports",
        "state",
        "state_reason",
    }
)


class SourceBridgeError(RuntimeError):
    pass


class SourceIdentityConflict(SourceBridgeError):
    pass


class SourceRowConflict(SourceBridgeError):
    pass


@dataclass(frozen=True, slots=True)
class Phase4SourceIdentityV01:
    source_id: str
    resolved_database_path: str
    filesystem_device: str
    filesystem_inode: str
    candidate_run_id: str


@dataclass(frozen=True, slots=True)
class Phase4EntrySourceV01:
    signal_key: str
    route_id: str
    source_evaluation_id: str
    run_id: str
    mint: str
    strategy_version: str
    parameter_set_id: str
    source_cursor: int
    source_event_key: str
    candidate_id: str
    signal_observed_at: str
    signal_ingest_seq: int
    requested_size_lamports: int
    paper_route_state: str
    paper_route_reason: str
    decision_at_us: int

    @property
    def ordering_key(self) -> tuple[int, str]:
        return (self.source_cursor, self.signal_key)

    def to_intent(self) -> ExecutionIntentV01:
        return ExecutionIntentV01(
            candidate_signal_id=self.candidate_id,
            candidate_run_id=self.run_id,
            strategy_evaluation_id=self.source_evaluation_id,
            strategy_version=self.strategy_version,
            parameter_set_id=self.parameter_set_id,
            source_run_id=None,
            source_event_key=self.source_event_key,
            source_ingest_seq=self.source_cursor,
            decision_at_us=self.decision_at_us,
            mint=self.mint,
            role=IntentRole.ENTRY,
            side=IntentSide.BUY,
            input_asset="SOL",
            input_amount_base_units=self.requested_size_lamports,
            position_id=None,
            parent_entry_intent_id=None,
            exit_decision_id=None,
            exit_track_id=None,
            exit_lifecycle_id=None,
        )


@dataclass(frozen=True, slots=True)
class SourceBridgePollResultV01:
    processed_rows: int
    created_or_matched_intent_ids: tuple[str, ...]
    last_source_cursor: int
    last_signal_key: str


FaultHook = Callable[[str, Phase4EntrySourceV01], None]


def _required_text(label: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SourceRowConflict(f"{label} is required")
    return value


def _exact_integer(label: str, value: Any, *, positive: bool = False) -> int:
    if type(value) is not int:
        raise SourceRowConflict(f"{label} must be an integer")
    if positive and value <= 0:
        raise SourceRowConflict(f"{label} must be > 0")
    if not positive and value < 0:
        raise SourceRowConflict(f"{label} must be >= 0")
    return value


def utc_text_to_epoch_us(value: Any) -> int:
    text = _required_text("signal_observed_at", value)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise SourceRowConflict("signal_observed_at is not a valid timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise SourceRowConflict("signal_observed_at must be explicitly UTC")
    normalized = parsed.astimezone(timezone.utc)
    delta = normalized - _EPOCH
    result = (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )
    if result < 0:
        raise SourceRowConflict("signal_observed_at precedes the Unix epoch")
    return result


def _table_columns(conn: sqlite3.Connection, table: str) -> frozenset[str]:
    return frozenset(str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})"))


def _verify_phase4_schema(conn: sqlite3.Connection) -> None:
    contexts = _table_columns(conn, "paper_continuous_signal_contexts_v0_1")
    routes = _table_columns(conn, "paper_entry_routes")
    if not _REQUIRED_SOURCE_COLUMNS.issubset(contexts):
        raise SourceBridgeError("Phase4 signal-context schema is incompatible")
    if not _REQUIRED_ROUTE_COLUMNS.issubset(routes):
        raise SourceBridgeError("Phase4 entry-route schema is incompatible")


def _stat_identity(path: Path) -> tuple[str, str]:
    stat = path.stat()
    device = int(stat.st_dev)
    inode = int(stat.st_ino)
    if device < 0 or inode <= 0:
        raise SourceIdentityConflict("stable filesystem file identity is unavailable")
    return str(device), str(inode)


def open_phase4_read_only(
    path: str | Path,
) -> tuple[sqlite3.Connection, Path, str, str]:
    resolved = Path(path).resolve(strict=True)
    if not resolved.is_file():
        raise SourceBridgeError("Phase4 source must be a regular SQLite file")
    before = _stat_identity(resolved)
    conn = sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        if int(conn.execute("PRAGMA query_only").fetchone()[0]) != 1:
            raise SourceBridgeError("Phase4 SQLite query_only mode is not active")
        main = [row for row in conn.execute("PRAGMA database_list") if row[1] == "main"]
        if len(main) != 1 or Path(str(main[0][2])).resolve() != resolved:
            raise SourceIdentityConflict("Phase4 connection/path identity mismatch")
        after = _stat_identity(resolved)
        if after != before:
            raise SourceIdentityConflict("Phase4 database changed while opening")
        _verify_phase4_schema(conn)
        return conn, resolved, before[0], before[1]
    except BaseException:
        conn.close()
        raise


def _resolve_candidate_run_id(
    conn: sqlite3.Connection,
    candidate_run_id: str | None,
) -> str:
    if candidate_run_id is not None:
        return _required_text("candidate_run_id", candidate_run_id)
    rows = conn.execute(
        "SELECT DISTINCT c.run_id "
        "FROM paper_continuous_signal_contexts_v0_1 AS c "
        "JOIN paper_entry_routes AS r ON r.route_id=c.route_id "
        "ORDER BY c.run_id LIMIT 2"
    ).fetchall()
    if len(rows) != 1:
        raise SourceIdentityConflict(
            "candidate_run_id must be explicit unless exactly one joined run exists"
        )
    return _required_text("candidate_run_id", rows[0][0])


def _source_identity(
    resolved: Path,
    device: str,
    inode: str,
    candidate_run_id: str,
) -> Phase4SourceIdentityV01:
    path_text = str(resolved)
    return Phase4SourceIdentityV01(
        source_id=deterministic_id(
            "P5SBSRC",
            SCHEMA_VERSION,
            path_text,
            device,
            inode,
            candidate_run_id,
        ),
        resolved_database_path=path_text,
        filesystem_device=device,
        filesystem_inode=inode,
        candidate_run_id=candidate_run_id,
    )


def _validate_shadow_database_path(
    paper_database_path: Path,
    shadow_database_path: str | Path,
) -> Path:
    shadow = Path(shadow_database_path).resolve()
    if shadow != SHADOW_DATA_ROOT and SHADOW_DATA_ROOT not in shadow.parents:
        raise SourceBridgeError("T004A Shadow persistence must remain under data/shadow")
    if shadow == paper_database_path:
        raise SourceBridgeError("Phase4 source and Shadow database must be distinct")
    if shadow.exists() and os.path.samefile(paper_database_path, shadow):
        raise SourceBridgeError("Phase4 source and Shadow database alias the same file")
    return shadow


def _create_bridge_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS shadow_t004a_schema_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            model_id TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            model_fingerprint TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS shadow_t004a_sources (
            source_id TEXT PRIMARY KEY,
            resolved_paper_db_path TEXT NOT NULL,
            candidate_run_id TEXT NOT NULL,
            filesystem_device TEXT NOT NULL CHECK(filesystem_device <> ''),
            filesystem_inode TEXT NOT NULL CHECK(filesystem_inode <> ''),
            UNIQUE(resolved_paper_db_path, candidate_run_id)
        );

        CREATE TABLE IF NOT EXISTS shadow_t004a_entry_lineage (
            lineage_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL REFERENCES shadow_t004a_sources(source_id),
            candidate_run_id TEXT NOT NULL,
            source_cursor INTEGER NOT NULL CHECK(source_cursor >= 0),
            signal_key TEXT NOT NULL,
            route_id TEXT NOT NULL,
            intent_id TEXT NOT NULL REFERENCES shadow_execution_intents(intent_id),
            paper_route_state TEXT NOT NULL,
            paper_route_reason TEXT NOT NULL,
            lineage_json TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            UNIQUE(source_id, candidate_run_id, source_cursor, signal_key),
            UNIQUE(source_id, candidate_run_id, route_id),
            UNIQUE(intent_id)
        );

        CREATE TABLE IF NOT EXISTS shadow_t004a_source_cursors (
            source_id TEXT PRIMARY KEY REFERENCES shadow_t004a_sources(source_id),
            candidate_run_id TEXT NOT NULL,
            last_source_cursor INTEGER NOT NULL CHECK(last_source_cursor >= -1),
            last_signal_key TEXT NOT NULL,
            last_lineage_id TEXT REFERENCES shadow_t004a_entry_lineage(lineage_id),
            revision INTEGER NOT NULL CHECK(revision >= 0),
            CHECK(
                (last_source_cursor = -1 AND last_signal_key = ''
                    AND last_lineage_id IS NULL AND revision = 0)
                OR
                (last_source_cursor >= 0 AND last_signal_key <> ''
                    AND last_lineage_id IS NOT NULL AND revision > 0)
            )
        );

        CREATE TRIGGER IF NOT EXISTS shadow_t004a_sources_no_update
        BEFORE UPDATE ON shadow_t004a_sources
        BEGIN
            SELECT RAISE(ABORT, 'T004A source identities are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS shadow_t004a_sources_no_delete
        BEFORE DELETE ON shadow_t004a_sources
        BEGIN
            SELECT RAISE(ABORT, 'T004A source identities are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS shadow_t004a_lineage_no_update
        BEFORE UPDATE ON shadow_t004a_entry_lineage
        BEGIN
            SELECT RAISE(ABORT, 'T004A source lineage is immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS shadow_t004a_lineage_no_delete
        BEFORE DELETE ON shadow_t004a_entry_lineage
        BEGIN
            SELECT RAISE(ABORT, 'T004A source lineage is immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS shadow_t004a_cursor_no_delete
        BEFORE DELETE ON shadow_t004a_source_cursors
        BEGIN
            SELECT RAISE(ABORT, 'T004A source cursor cannot be deleted');
        END;
        """
    )
    expected = (MODEL_ID, SCHEMA_VERSION, MODEL_FINGERPRINT)
    row = conn.execute(
        "SELECT model_id,schema_version,model_fingerprint "
        "FROM shadow_t004a_schema_meta WHERE singleton=1"
    ).fetchone()
    if row is None:
        conn.execute("INSERT INTO shadow_t004a_schema_meta VALUES(1,?,?,?)", expected)
    elif tuple(row) != expected:
        raise ShadowDeterminismConflict("T004A bridge contract mismatch")
    conn.commit()


class ShadowContinuousSourceBridgeV01:
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
        _create_bridge_schema(self._conn)
        self._register_source()

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("source bridge is closed")

    def _register_source(self) -> None:
        identity = self.source_identity
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            existing = self._conn.execute(
                "SELECT * FROM shadow_t004a_sources "
                "WHERE resolved_paper_db_path=? AND candidate_run_id=?",
                (identity.resolved_database_path, identity.candidate_run_id),
            ).fetchone()
            expected = (
                identity.source_id,
                identity.resolved_database_path,
                identity.candidate_run_id,
                identity.filesystem_device,
                identity.filesystem_inode,
            )
            if existing is None:
                self._conn.execute(
                    "INSERT INTO shadow_t004a_sources VALUES(?,?,?,?,?)",
                    expected,
                )
            elif tuple(existing) != expected:
                raise SourceIdentityConflict(
                    "resolved Phase4 database/run is already bound to another file"
                )
            by_id = self._conn.execute(
                "SELECT * FROM shadow_t004a_sources WHERE source_id=?",
                (identity.source_id,),
            ).fetchone()
            if by_id is None or tuple(by_id) != expected:
                raise SourceIdentityConflict("T004A source identity collision")
            cursor = self._conn.execute(
                "SELECT candidate_run_id FROM shadow_t004a_source_cursors "
                "WHERE source_id=?",
                (identity.source_id,),
            ).fetchone()
            if cursor is None:
                self._conn.execute(
                    "INSERT INTO shadow_t004a_source_cursors "
                    "VALUES(?,?,-1,'',NULL,0)",
                    (identity.source_id, identity.candidate_run_id),
                )
            elif str(cursor[0]) != identity.candidate_run_id:
                raise SourceIdentityConflict("T004A cursor run identity mismatch")

    def _verify_source_still_bound(self) -> None:
        identity = self.source_identity
        current = _stat_identity(Path(identity.resolved_database_path))
        if current != (identity.filesystem_device, identity.filesystem_inode):
            raise SourceIdentityConflict("Phase4 source file identity changed")
        if int(self._paper_conn.execute("PRAGMA query_only").fetchone()[0]) != 1:
            raise SourceIdentityConflict("Phase4 connection lost query_only mode")

    def cursor(self) -> tuple[int, str, str | None, int]:
        self._require_open()
        row = self._conn.execute(
            "SELECT last_source_cursor,last_signal_key,last_lineage_id,revision "
            "FROM shadow_t004a_source_cursors WHERE source_id=?",
            (self.source_identity.source_id,),
        ).fetchone()
        if row is None:
            raise SourceIdentityConflict("T004A source cursor is missing")
        return (int(row[0]), str(row[1]), row[2], int(row[3]))

    def _load_rows(
        self,
        after_cursor: int,
        after_signal_key: str,
        limit: int,
    ) -> tuple[sqlite3.Row, ...]:
        return tuple(
            self._paper_conn.execute(
                _SOURCE_SQL,
                (
                    self.source_identity.candidate_run_id,
                    after_cursor,
                    after_cursor,
                    after_signal_key,
                    limit,
                ),
            ).fetchall()
        )

    @staticmethod
    def _construct_source(row: sqlite3.Row) -> Phase4EntrySourceV01:
        signal_key = _required_text("signal_key", row["signal_key"])
        context_route_id = _required_text("context_route_id", row["context_route_id"])
        route_id = _required_text("route_id", row["route_id"])
        if context_route_id != route_id:
            raise SourceRowConflict("joined route identity mismatch")
        context_mint = _required_text("context mint", row["context_mint"])
        route_mint = _required_text("route mint", row["route_mint"])
        if context_mint != route_mint:
            raise SourceRowConflict("joined mint mismatch")
        context_strategy = _required_text(
            "context strategy_version", row["context_strategy_version"]
        )
        route_strategy = _required_text(
            "route strategy_version", row["route_strategy_version"]
        )
        if context_strategy != route_strategy:
            raise SourceRowConflict("joined strategy_version mismatch")
        context_parameters = _required_text(
            "context parameter_set_id", row["context_parameter_set_id"]
        )
        route_parameters = _required_text(
            "route parameter_set_id", row["route_parameter_set_id"]
        )
        if context_parameters != route_parameters:
            raise SourceRowConflict("joined parameter_set_id mismatch")
        observed = _required_text("signal_observed_at", row["signal_observed_at"])
        return Phase4EntrySourceV01(
            signal_key=signal_key,
            route_id=route_id,
            source_evaluation_id=_required_text(
                "source_evaluation_id", row["source_evaluation_id"]
            ),
            run_id=_required_text("run_id", row["run_id"]),
            mint=context_mint,
            strategy_version=context_strategy,
            parameter_set_id=context_parameters,
            source_cursor=_exact_integer("source_cursor", row["source_cursor"]),
            source_event_key=_required_text("source_event_key", row["source_event_key"]),
            candidate_id=_required_text("candidate_id", row["candidate_id"]),
            signal_observed_at=observed,
            signal_ingest_seq=_exact_integer(
                "signal_ingest_seq", row["signal_ingest_seq"]
            ),
            requested_size_lamports=_exact_integer(
                "requested_size_lamports",
                row["requested_size_lamports"],
                positive=True,
            ),
            paper_route_state=_required_text(
                "paper_route_state", row["paper_route_state"]
            ),
            paper_route_reason=_required_text(
                "paper_route_reason", row["paper_route_reason"]
            ),
            decision_at_us=utc_text_to_epoch_us(observed),
        )

    def _lineage_record(
        self,
        source: Phase4EntrySourceV01,
        intent: ExecutionIntentV01,
        *,
        observed_paper_route_state: str | None = None,
        observed_paper_route_reason: str | None = None,
    ) -> tuple[str, str, str]:
        identity = self.source_identity
        paper_route_state = (
            source.paper_route_state
            if observed_paper_route_state is None
            else observed_paper_route_state
        )
        paper_route_reason = (
            source.paper_route_reason
            if observed_paper_route_reason is None
            else observed_paper_route_reason
        )
        payload = {
            "schema_version": SCHEMA_VERSION,
            "model_id": MODEL_ID,
            "model_fingerprint": MODEL_FINGERPRINT,
            "source_identity": {
                "source_id": identity.source_id,
                "resolved_paper_db_path": identity.resolved_database_path,
                "filesystem_device": identity.filesystem_device,
                "filesystem_inode": identity.filesystem_inode,
                "candidate_run_id": identity.candidate_run_id,
            },
            "phase4_source": {
                "signal_key": source.signal_key,
                "route_id": source.route_id,
                "source_evaluation_id": source.source_evaluation_id,
                "run_id": source.run_id,
                "mint": source.mint,
                "strategy_version": source.strategy_version,
                "parameter_set_id": source.parameter_set_id,
                "source_cursor": source.source_cursor,
                "source_event_key": source.source_event_key,
                "candidate_id": source.candidate_id,
                "signal_observed_at": source.signal_observed_at,
                "signal_ingest_seq": source.signal_ingest_seq,
                "requested_size_lamports": source.requested_size_lamports,
                "paper_route_state": paper_route_state,
                "paper_route_reason": paper_route_reason,
            },
            "shadow_intent": {
                "intent_id": intent.intent_id,
                "intent_fingerprint": intent.fingerprint,
            },
            "paper_outcome_is_shadow_outcome": False,
        }
        lineage_json = canonical_json(payload)
        fingerprint = hashlib.sha256(lineage_json.encode("utf-8")).hexdigest()
        lineage_id = deterministic_id(
            "P5SBL",
            SCHEMA_VERSION,
            identity.source_id,
            source.run_id,
            source.source_cursor,
            source.signal_key,
            source.route_id,
        )
        return lineage_id, lineage_json, fingerprint

    def _persist_lineage(
        self,
        source: Phase4EntrySourceV01,
        intent: ExecutionIntentV01,
    ) -> str:
        lineage_id, lineage_json, fingerprint = self._lineage_record(source, intent)
        identity = self.source_identity
        expected = (
            lineage_id,
            identity.source_id,
            source.run_id,
            source.source_cursor,
            source.signal_key,
            source.route_id,
            intent.intent_id,
            source.paper_route_state,
            source.paper_route_reason,
            lineage_json,
            fingerprint,
        )
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            existing = self._conn.execute(
                "SELECT * FROM shadow_t004a_entry_lineage WHERE lineage_id=?",
                (lineage_id,),
            ).fetchone()
            if existing is not None:
                replay_id, replay_json, replay_fingerprint = self._lineage_record(
                    source,
                    intent,
                    observed_paper_route_state=str(existing["paper_route_state"]),
                    observed_paper_route_reason=str(existing["paper_route_reason"]),
                )
                replay_expected = (
                    replay_id,
                    identity.source_id,
                    source.run_id,
                    source.source_cursor,
                    source.signal_key,
                    source.route_id,
                    intent.intent_id,
                    str(existing["paper_route_state"]),
                    str(existing["paper_route_reason"]),
                    replay_json,
                    replay_fingerprint,
                )
                if tuple(existing) != replay_expected:
                    raise ShadowDeterminismConflict("T004A lineage replay conflict")
                return lineage_id
            collision = self._conn.execute(
                "SELECT lineage_id FROM shadow_t004a_entry_lineage "
                "WHERE (source_id=? AND candidate_run_id=? AND source_cursor=? "
                "AND signal_key=?) OR (source_id=? AND candidate_run_id=? "
                "AND route_id=?) OR intent_id=?",
                (
                    identity.source_id,
                    source.run_id,
                    source.source_cursor,
                    source.signal_key,
                    identity.source_id,
                    source.run_id,
                    source.route_id,
                    intent.intent_id,
                ),
            ).fetchone()
            if collision is not None:
                raise ShadowDeterminismConflict("T004A lineage uniqueness conflict")
            self._conn.execute(
                "INSERT INTO shadow_t004a_entry_lineage VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                expected,
            )
        return lineage_id

    def _advance_cursor(
        self,
        previous: tuple[int, str, str | None, int],
        source: Phase4EntrySourceV01,
        lineage_id: str,
    ) -> tuple[int, str, str | None, int]:
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            current = self.cursor()
            if current[:2] == source.ordering_key:
                if current[2] != lineage_id:
                    raise ShadowDeterminismConflict("T004A cursor lineage conflict")
                return current
            if current[:2] > source.ordering_key:
                raise ShadowDeterminismConflict("T004A stale cursor advancement")
            if current != previous:
                raise ShadowDeterminismConflict("T004A concurrent cursor advancement")
            lineage = self._conn.execute(
                "SELECT 1 FROM shadow_t004a_entry_lineage "
                "WHERE lineage_id=? AND source_id=? AND source_cursor=? "
                "AND signal_key=?",
                (
                    lineage_id,
                    self.source_identity.source_id,
                    source.source_cursor,
                    source.signal_key,
                ),
            ).fetchone()
            if lineage is None:
                raise ShadowDeterminismConflict(
                    "T004A cursor cannot advance without exact lineage"
                )
            updated = self._conn.execute(
                "UPDATE shadow_t004a_source_cursors "
                "SET last_source_cursor=?,last_signal_key=?,last_lineage_id=?,"
                "revision=revision+1 WHERE source_id=? AND revision=? "
                "AND last_source_cursor=? AND last_signal_key=?",
                (
                    source.source_cursor,
                    source.signal_key,
                    lineage_id,
                    self.source_identity.source_id,
                    previous[3],
                    previous[0],
                    previous[1],
                ),
            )
            if updated.rowcount != 1:
                raise ShadowDeterminismConflict("T004A atomic cursor update failed")
        return (source.source_cursor, source.signal_key, lineage_id, previous[3] + 1)

    def poll_once(self, *, limit: int = 100) -> SourceBridgePollResultV01:
        self._require_open()
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be a positive integer")
        self._verify_source_still_bound()
        cursor = self.cursor()
        rows = self._load_rows(cursor[0], cursor[1], limit)
        intent_ids: list[str] = []
        for row in rows:
            source = self._construct_source(row)
            if source.run_id != self.source_identity.candidate_run_id:
                raise SourceRowConflict("source row candidate run mismatch")
            if source.ordering_key <= cursor[:2]:
                raise SourceRowConflict("source query returned non-advancing row")
            intent = source.to_intent()
            persisted = self._shadow.register_intent(intent)
            if self._fault_hook is not None:
                self._fault_hook("AFTER_INTENT_REGISTRATION", source)
            lineage_id = self._persist_lineage(source, persisted)
            if self._fault_hook is not None:
                self._fault_hook("AFTER_LINEAGE_PERSISTENCE", source)
            cursor = self._advance_cursor(cursor, source, lineage_id)
            intent_ids.append(persisted.intent_id)
        return SourceBridgePollResultV01(
            processed_rows=len(intent_ids),
            created_or_matched_intent_ids=tuple(intent_ids),
            last_source_cursor=cursor[0],
            last_signal_key=cursor[1],
        )

    def lineage_count(self) -> int:
        self._require_open()
        return int(
            self._conn.execute(
                "SELECT COUNT(*) FROM shadow_t004a_entry_lineage WHERE source_id=?",
                (self.source_identity.source_id,),
            ).fetchone()[0]
        )

    def intent_count(self) -> int:
        self._require_open()
        return int(
            self._conn.execute(
                "SELECT COUNT(*) FROM shadow_execution_intents WHERE role='ENTRY'"
            ).fetchone()[0]
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
            "sources": [
                dict(row)
                for row in self._conn.execute(
                    "SELECT * FROM shadow_t004a_sources ORDER BY source_id"
                )
            ],
            "lineage": [
                dict(row)
                for row in self._conn.execute(
                    "SELECT * FROM shadow_t004a_entry_lineage ORDER BY lineage_id"
                )
            ],
            "cursors": [
                dict(row)
                for row in self._conn.execute(
                    "SELECT * FROM shadow_t004a_source_cursors ORDER BY source_id"
                )
            ],
        }
        return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()

    def close(self) -> None:
        if not self._closed:
            self._paper_conn.close()
            self._shadow.close()
            self._closed = True

    def __enter__(self) -> ShadowContinuousSourceBridgeV01:
        self._require_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def open_continuous_source_bridge(
    paper_database_path: str | Path,
    shadow_database_path: str | Path,
    *,
    candidate_run_id: str | None = None,
    fault_hook: FaultHook | None = None,
) -> ShadowContinuousSourceBridgeV01:
    paper_conn, resolved, device, inode = open_phase4_read_only(paper_database_path)
    shadow: ShadowRepositoryV01 | None = None
    try:
        run_id = _resolve_candidate_run_id(paper_conn, candidate_run_id)
        identity = _source_identity(resolved, device, inode, run_id)
        shadow_path = _validate_shadow_database_path(resolved, shadow_database_path)
        shadow = open_shadow_repository(shadow_path)
        return ShadowContinuousSourceBridgeV01(
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
