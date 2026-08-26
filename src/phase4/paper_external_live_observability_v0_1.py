from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Iterable


MODEL_ID = "P4-EXTERNAL-LIVE-OBSERVABILITY-0001"
SCHEMA_VERSION = "phase4_external_live_observability_v0.1"
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 1.0
DEFAULT_HEARTBEAT_TTL_SECONDS = 5.0
RUNS_TABLE = "phase4_external_live_runs_v0_1"
META_TABLE = "phase4_external_live_meta_v0_1"
SOURCE_ROW_IDENTITY = "pump_events.rowid"
NORMALIZED_INGEST_IDENTITY = "phase2.ingest_seq==pump_events.rowid"

SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "representation": "DEDICATED_SQLITE_RUN_REGISTRY",
    "publication_atomicity": "ONE_TRANSACTION_PER_START_HEARTBEAT_OR_TERMINAL",
    "journal_mode": "WAL",
    "synchronous": "FULL",
    "states": ("RUNNING", "COMPLETED", "FAILED", "ABORTED"),
    "running_validity": (
        "STATE_RUNNING_AND_HEARTBEAT_AT_LE_OBSERVED_AT_LE_VALID_UNTIL"
    ),
    "producer_liveness": {
        "heartbeat_interval_seconds": DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        "heartbeat_ttl_seconds": DEFAULT_HEARTBEAT_TTL_SECONDS,
        "consumer_stale_threshold": "PROHIBITED",
    },
    "run_identity": "UUID4_UNIQUE_PER_PRODUCER_RUN",
    "source_locator": "ABSOLUTE_ACTUAL_SQLITE_PATH",
    "source_access": "SQLITE_URI_MODE_RO_AND_QUERY_ONLY",
    "source_row_identity": SOURCE_ROW_IDENTITY,
    "normalized_ingest_identity": NORMALIZED_INGEST_IDENTITY,
    "multiple_active_runs": "REPORT_AMBIGUOUS_NO_LEADER_ELECTION",
    "observer_control_path": False,
    "paper_only": True,
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


class RunState(StrEnum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


class ObserverStatus(StrEnum):
    NO_ACTIVE_RUN = "NO_ACTIVE_RUN"
    ONE_ACTIVE_RUN = "ONE_ACTIVE_RUN"
    AMBIGUOUS_MULTIPLE_ACTIVE_RUNS = "AMBIGUOUS_MULTIPLE_ACTIVE_RUNS"


class ObservabilityContractError(RuntimeError):
    pass


class ObservabilityPublicationError(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observability timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _dt_parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return _utc(parsed)


def _positive_seconds(value: float, name: str) -> float:
    parsed = float(value)
    if not parsed > 0.0:
        raise ValueError(f"{name} must be positive")
    return parsed


def canonical_run_id(value: str) -> str:
    raw = str(value)
    try:
        parsed = uuid.UUID(raw)
    except (ValueError, AttributeError) as exc:
        raise ValueError("run_id must be a canonical UUID4") from exc
    canonical = str(parsed)
    if (
        raw.lower() != canonical
        or parsed.version != 4
        or parsed.variant != uuid.RFC_4122
    ):
        raise ValueError("run_id must be a canonical UUID4")
    return canonical


def _resolved_file(path: str | Path, name: str) -> Path:
    resolved = Path(path).resolve()
    if not resolved.exists() or not resolved.is_file():
        raise FileNotFoundError(f"{name} does not exist: {resolved}")
    return resolved


def _create_producer_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {META_TABLE}(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            model_id TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            model_fingerprint TEXT NOT NULL,
            created_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS {RUNS_TABLE}(
            run_id TEXT PRIMARY KEY,
            contract_model_id TEXT NOT NULL,
            contract_version TEXT NOT NULL,
            contract_fingerprint TEXT NOT NULL,
            runtime_model_id TEXT NOT NULL,
            runtime_fingerprint TEXT NOT NULL,
            runtime_session_identity TEXT NOT NULL,
            paper_runtime_sqlite_path TEXT NOT NULL,
            producer_pid INTEGER NOT NULL,
            started_at_utc TEXT NOT NULL,
            runtime_state TEXT NOT NULL CHECK(
                runtime_state IN ('RUNNING','COMPLETED','FAILED','ABORTED')
            ),
            heartbeat_sequence INTEGER NOT NULL CHECK(heartbeat_sequence>=1),
            heartbeat_at_utc TEXT NOT NULL,
            heartbeat_ttl_seconds REAL NOT NULL CHECK(heartbeat_ttl_seconds>0),
            heartbeat_valid_until_utc TEXT NOT NULL,
            ended_at_utc TEXT,
            source_model_id TEXT NOT NULL,
            source_model_fingerprint TEXT NOT NULL,
            source_contract_version TEXT NOT NULL,
            source_identity TEXT NOT NULL,
            source_sqlite_path TEXT NOT NULL,
            source_database_identity TEXT NOT NULL,
            source_anchor_p1_rowid INTEGER NOT NULL CHECK(source_anchor_p1_rowid>=0),
            durable_source_cursor_p1_rowid INTEGER NOT NULL,
            source_watermark_p1_rowid INTEGER NOT NULL,
            source_watermark_kind TEXT NOT NULL CHECK(
                source_watermark_kind IN ('CURRENT','FROZEN')
            ),
            source_row_identity TEXT NOT NULL,
            normalized_ingest_identity TEXT NOT NULL,
            price_representation_version TEXT NOT NULL,
            published_at_utc TEXT NOT NULL,
            CHECK(durable_source_cursor_p1_rowid>=source_anchor_p1_rowid),
            CHECK(source_watermark_p1_rowid>=durable_source_cursor_p1_rowid),
            CHECK(
                (runtime_state='RUNNING' AND ended_at_utc IS NULL)
                OR
                (runtime_state<>'RUNNING' AND ended_at_utc IS NOT NULL)
            )
        );

        CREATE INDEX IF NOT EXISTS phase4_external_live_state_v0_1
        ON {RUNS_TABLE}(runtime_state,heartbeat_valid_until_utc);
        """
    )


def _connect_producer(path: Path, now: datetime) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    mode = str(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
    conn.execute("PRAGMA synchronous=FULL")
    if mode != "wal" or int(conn.execute("PRAGMA synchronous").fetchone()[0]) != 2:
        conn.close()
        raise ObservabilityPublicationError(
            "observability registry requires WAL with synchronous=FULL"
        )
    _create_producer_schema(conn)
    conn.execute(
        f"INSERT OR IGNORE INTO {META_TABLE} VALUES(1,?,?,?,?)",
        (MODEL_ID, SCHEMA_VERSION, MODEL_FINGERPRINT, _dt_text(now)),
    )
    meta = conn.execute(f"SELECT * FROM {META_TABLE} WHERE singleton=1").fetchone()
    expected = (MODEL_ID, SCHEMA_VERSION, MODEL_FINGERPRINT)
    actual = None if meta is None else (
        str(meta["model_id"]),
        str(meta["schema_version"]),
        str(meta["model_fingerprint"]),
    )
    if actual != expected:
        conn.close()
        raise ObservabilityPublicationError(
            f"observability registry identity conflict: {actual!r} != {expected!r}"
        )
    conn.commit()
    return conn


@dataclass(frozen=True, slots=True)
class LiveRunRecordV01:
    run_id: str
    contract_model_id: str
    contract_version: str
    contract_fingerprint: str
    runtime_model_id: str
    runtime_fingerprint: str
    runtime_session_identity: str
    paper_runtime_sqlite_path: str
    producer_pid: int
    started_at_utc: str
    runtime_state: str
    heartbeat_sequence: int
    heartbeat_at_utc: str
    heartbeat_ttl_seconds: float
    heartbeat_valid_until_utc: str
    ended_at_utc: str | None
    source_model_id: str
    source_model_fingerprint: str
    source_contract_version: str
    source_identity: str
    source_sqlite_path: str
    source_database_identity: str
    source_anchor_p1_rowid: int
    durable_source_cursor_p1_rowid: int
    source_watermark_p1_rowid: int
    source_watermark_kind: str
    source_row_identity: str
    normalized_ingest_identity: str
    price_representation_version: str
    published_at_utc: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "LiveRunRecordV01":
        return cls(**dict(row))

    def validate(self) -> None:
        if (
            self.contract_model_id != MODEL_ID
            or self.contract_version != SCHEMA_VERSION
            or self.contract_fingerprint != MODEL_FINGERPRINT
        ):
            raise ObservabilityContractError("run record contract identity mismatch")
        try:
            state = RunState(self.runtime_state)
        except ValueError as exc:
            raise ObservabilityContractError("invalid runtime state") from exc
        started = _dt_parse(self.started_at_utc)
        heartbeat = _dt_parse(self.heartbeat_at_utc)
        valid_until = _dt_parse(self.heartbeat_valid_until_utc)
        published = _dt_parse(self.published_at_utc)
        ttl = _positive_seconds(
            self.heartbeat_ttl_seconds, "heartbeat_ttl_seconds"
        )
        if heartbeat < started:
            raise ObservabilityContractError("heartbeat predates run start")
        if published < heartbeat:
            raise ObservabilityContractError("publication predates heartbeat")
        if state is RunState.RUNNING:
            if valid_until != heartbeat + timedelta(seconds=ttl):
                raise ObservabilityContractError("invalid producer liveness interval")
            if self.ended_at_utc is not None:
                raise ObservabilityContractError("RUNNING record has ended_at_utc")
        else:
            if valid_until != heartbeat:
                raise ObservabilityContractError(
                    "terminal liveness interval must be closed"
                )
            if self.ended_at_utc is None:
                raise ObservabilityContractError("terminal record lacks ended_at_utc")
            ended = _dt_parse(self.ended_at_utc)
            if ended < started:
                raise ObservabilityContractError("termination predates run start")
        try:
            if self.run_id != canonical_run_id(self.run_id):
                raise ValueError("non-canonical run ID")
        except ValueError as exc:
            raise ObservabilityContractError("run_id is not a canonical UUID4") from exc
        required_text = (
            self.runtime_model_id,
            self.runtime_fingerprint,
            self.runtime_session_identity,
            self.source_model_id,
            self.source_model_fingerprint,
            self.source_contract_version,
            self.source_identity,
            self.source_database_identity,
            self.price_representation_version,
        )
        if any(not str(value).strip() for value in required_text):
            raise ObservabilityContractError("required identity metadata is empty")
        if self.producer_pid <= 0 or self.heartbeat_sequence < 1:
            raise ObservabilityContractError("invalid producer/heartbeat identity")
        if self.source_row_identity != SOURCE_ROW_IDENTITY:
            raise ObservabilityContractError("unexpected source row identity")
        if self.normalized_ingest_identity != NORMALIZED_INGEST_IDENTITY:
            raise ObservabilityContractError("unexpected normalized ingest identity")
        source_path = Path(self.source_sqlite_path)
        paper_path = Path(self.paper_runtime_sqlite_path)
        if not source_path.is_absolute() or not paper_path.is_absolute():
            raise ObservabilityContractError("runtime/source locators must be absolute")
        if self.source_anchor_p1_rowid < 0:
            raise ObservabilityContractError("negative source anchor")
        if self.durable_source_cursor_p1_rowid < self.source_anchor_p1_rowid:
            raise ObservabilityContractError("durable cursor precedes source anchor")
        if self.source_watermark_p1_rowid < self.durable_source_cursor_p1_rowid:
            raise ObservabilityContractError("source watermark precedes durable cursor")
        if self.source_watermark_kind not in {"CURRENT", "FROZEN"}:
            raise ObservabilityContractError("invalid source watermark kind")

    def is_running_at(self, observed_at: datetime) -> bool:
        self.validate()
        observed = _utc(observed_at)
        return (
            self.runtime_state == RunState.RUNNING.value
            and _dt_parse(self.heartbeat_at_utc)
            <= observed
            <= _dt_parse(self.heartbeat_valid_until_utc)
        )


@dataclass(frozen=True, slots=True)
class ObserverViewV01:
    status: ObserverStatus
    active_run_ids: tuple[str, ...]
    records: tuple[LiveRunRecordV01, ...]


class LiveRunPublisherV01:
    """Producer-only writer for the passive external observability registry."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        registry_path: Path,
        run_id: str,
        heartbeat_interval_seconds: float,
        heartbeat_ttl_seconds: float,
        wall_clock: Callable[[], datetime],
        monotonic_clock: Callable[[], float],
        initial_cursor: int,
    ) -> None:
        self._conn = conn
        self.registry_path = registry_path
        self.run_id = run_id
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.heartbeat_ttl_seconds = heartbeat_ttl_seconds
        self._wall_clock = wall_clock
        self._monotonic_clock = monotonic_clock
        self._last_write_monotonic = monotonic_clock()
        self._last_cursor = int(initial_cursor)
        self._closed = False
        self.publication_count = 1
        self.heartbeat_publication_count = 0
        self.terminal_publication_count = 0

    @classmethod
    def start(
        cls,
        registry_path: str | Path,
        *,
        runtime_model_id: str,
        runtime_fingerprint: str,
        runtime_session_identity: str,
        paper_runtime_sqlite_path: str | Path,
        source_model_id: str,
        source_model_fingerprint: str,
        source_contract_version: str,
        source_identity: str,
        source_sqlite_path: str | Path,
        source_database_identity: str,
        source_anchor_p1_rowid: int,
        initial_durable_source_cursor_p1_rowid: int,
        initial_source_watermark_p1_rowid: int,
        price_representation_version: str,
        run_id: str | None = None,
        heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        heartbeat_ttl_seconds: float = DEFAULT_HEARTBEAT_TTL_SECONDS,
        started_at_utc: datetime | None = None,
        wall_clock: Callable[[], datetime] = utc_now,
        monotonic_clock: Callable[[], float] = time.monotonic,
        producer_pid: int | None = None,
    ) -> "LiveRunPublisherV01":
        interval = _positive_seconds(
            heartbeat_interval_seconds, "heartbeat_interval_seconds"
        )
        ttl = _positive_seconds(heartbeat_ttl_seconds, "heartbeat_ttl_seconds")
        if interval >= ttl:
            raise ValueError("heartbeat interval must be shorter than heartbeat TTL")
        if (
            interval != DEFAULT_HEARTBEAT_INTERVAL_SECONDS
            or ttl != DEFAULT_HEARTBEAT_TTL_SECONDS
        ):
            raise ValueError("v0.1 heartbeat interval/TTL are contract-locked")
        source_path = _resolved_file(source_sqlite_path, "source SQLite")
        paper_path = _resolved_file(paper_runtime_sqlite_path, "paper runtime SQLite")
        anchor = int(source_anchor_p1_rowid)
        cursor = int(initial_durable_source_cursor_p1_rowid)
        watermark = int(initial_source_watermark_p1_rowid)
        if anchor < 0 or cursor < anchor or watermark < cursor:
            raise ValueError("invalid initial source anchor/cursor/watermark")
        identity = canonical_run_id(str(run_id or uuid.uuid4()))
        started = _utc(started_at_utc or wall_clock())
        heartbeat = _utc(wall_clock())
        if heartbeat < started:
            raise ValueError("initial heartbeat predates run start")
        valid_until = heartbeat + timedelta(seconds=ttl)
        resolved_registry = Path(registry_path).resolve()
        conn = _connect_producer(resolved_registry, started)
        values = (
            identity,
            MODEL_ID,
            SCHEMA_VERSION,
            MODEL_FINGERPRINT,
            str(runtime_model_id),
            str(runtime_fingerprint),
            str(runtime_session_identity),
            str(paper_path),
            int(os.getpid() if producer_pid is None else producer_pid),
            _dt_text(started),
            RunState.RUNNING.value,
            1,
            _dt_text(heartbeat),
            ttl,
            _dt_text(valid_until),
            None,
            str(source_model_id),
            str(source_model_fingerprint),
            str(source_contract_version),
            str(source_identity),
            str(source_path),
            str(source_database_identity),
            anchor,
            cursor,
            watermark,
            "CURRENT",
            SOURCE_ROW_IDENTITY,
            NORMALIZED_INGEST_IDENTITY,
            str(price_representation_version),
            _dt_text(heartbeat),
        )
        try:
            with conn:
                conn.execute(
                    f"INSERT INTO {RUNS_TABLE} VALUES("
                    + ",".join("?" for _ in values)
                    + ")",
                    values,
                )
        except BaseException:
            conn.close()
            raise
        return cls(
            conn,
            registry_path=resolved_registry,
            run_id=identity,
            heartbeat_interval_seconds=interval,
            heartbeat_ttl_seconds=ttl,
            wall_clock=wall_clock,
            monotonic_clock=monotonic_clock,
            initial_cursor=cursor,
        )

    def _require_open(self) -> None:
        if self._closed:
            raise ObservabilityPublicationError("publisher is closed")

    def publish_heartbeat(
        self,
        *,
        durable_source_cursor_p1_rowid: int,
        source_watermark_p1_rowid: int,
        source_watermark_kind: str,
        force: bool = False,
    ) -> bool:
        self._require_open()
        cursor = int(durable_source_cursor_p1_rowid)
        watermark = int(source_watermark_p1_rowid)
        if cursor < self._last_cursor:
            raise ObservabilityPublicationError("durable source cursor regressed")
        if watermark < cursor:
            raise ObservabilityPublicationError("source watermark precedes durable cursor")
        if source_watermark_kind not in {"CURRENT", "FROZEN"}:
            raise ObservabilityPublicationError("invalid source watermark kind")
        now_mono = self._monotonic_clock()
        if not force and now_mono - self._last_write_monotonic < (
            self.heartbeat_interval_seconds
        ):
            return False
        now = _utc(self._wall_clock())
        valid_until = now + timedelta(seconds=self.heartbeat_ttl_seconds)
        with self._conn:
            row = self._conn.execute(
                f"SELECT runtime_state,heartbeat_sequence,heartbeat_at_utc,"
                f"durable_source_cursor_p1_rowid FROM {RUNS_TABLE} WHERE run_id=?",
                (self.run_id,),
            ).fetchone()
            if row is None:
                raise ObservabilityPublicationError("run record disappeared")
            if str(row["runtime_state"]) != RunState.RUNNING.value:
                raise ObservabilityPublicationError("terminal run cannot be resurrected")
            if cursor < int(row["durable_source_cursor_p1_rowid"]):
                raise ObservabilityPublicationError("persisted durable cursor regressed")
            if now < _dt_parse(str(row["heartbeat_at_utc"])):
                raise ObservabilityPublicationError("producer wall clock regressed")
            updated = self._conn.execute(
                f"UPDATE {RUNS_TABLE} SET heartbeat_sequence=?,heartbeat_at_utc=?,"
                "heartbeat_valid_until_utc=?,durable_source_cursor_p1_rowid=?,"
                "source_watermark_p1_rowid=?,source_watermark_kind=?,"
                "published_at_utc=? WHERE run_id=? AND runtime_state='RUNNING'",
                (
                    int(row["heartbeat_sequence"]) + 1,
                    _dt_text(now),
                    _dt_text(valid_until),
                    cursor,
                    watermark,
                    source_watermark_kind,
                    _dt_text(now),
                    self.run_id,
                ),
            )
            if updated.rowcount != 1:
                raise ObservabilityPublicationError("atomic heartbeat update failed")
        self._last_cursor = cursor
        self._last_write_monotonic = now_mono
        self.publication_count += 1
        self.heartbeat_publication_count += 1
        return True

    def publish_terminal(
        self,
        state: RunState,
        *,
        durable_source_cursor_p1_rowid: int,
        source_watermark_p1_rowid: int,
        source_watermark_kind: str,
        ended_at_utc: datetime | None = None,
    ) -> None:
        self._require_open()
        if state is RunState.RUNNING:
            raise ValueError("terminal publication requires a terminal state")
        cursor = int(durable_source_cursor_p1_rowid)
        watermark = int(source_watermark_p1_rowid)
        if cursor < self._last_cursor or watermark < cursor:
            raise ObservabilityPublicationError("invalid terminal cursor/watermark")
        if source_watermark_kind not in {"CURRENT", "FROZEN"}:
            raise ObservabilityPublicationError("invalid source watermark kind")
        ended = _utc(ended_at_utc or self._wall_clock())
        with self._conn:
            row = self._conn.execute(
                f"SELECT runtime_state,heartbeat_sequence,started_at_utc,"
                f"heartbeat_at_utc,durable_source_cursor_p1_rowid "
                f"FROM {RUNS_TABLE} WHERE run_id=?",
                (self.run_id,),
            ).fetchone()
            if row is None:
                raise ObservabilityPublicationError("run record disappeared")
            if str(row["runtime_state"]) != RunState.RUNNING.value:
                raise ObservabilityPublicationError("terminal run cannot transition again")
            if ended < _dt_parse(str(row["started_at_utc"])):
                raise ObservabilityPublicationError("termination predates run start")
            if ended < _dt_parse(str(row["heartbeat_at_utc"])):
                raise ObservabilityPublicationError("termination predates heartbeat")
            if cursor < int(row["durable_source_cursor_p1_rowid"]):
                raise ObservabilityPublicationError("terminal durable cursor regressed")
            updated = self._conn.execute(
                f"UPDATE {RUNS_TABLE} SET runtime_state=?,heartbeat_sequence=?,"
                "heartbeat_at_utc=?,heartbeat_valid_until_utc=?,ended_at_utc=?,"
                "durable_source_cursor_p1_rowid=?,source_watermark_p1_rowid=?,"
                "source_watermark_kind=?,published_at_utc=? "
                "WHERE run_id=? AND runtime_state='RUNNING'",
                (
                    state.value,
                    int(row["heartbeat_sequence"]) + 1,
                    _dt_text(ended),
                    _dt_text(ended),
                    _dt_text(ended),
                    cursor,
                    watermark,
                    source_watermark_kind,
                    _dt_text(ended),
                    self.run_id,
                ),
            )
            if updated.rowcount != 1:
                raise ObservabilityPublicationError("atomic terminal update failed")
        self._last_cursor = cursor
        self.publication_count += 1
        self.terminal_publication_count += 1

    def close(self) -> None:
        if not self._closed:
            self._conn.close()
            self._closed = True


class ReadOnlyLiveObservabilityV01:
    """Read-only/query-only consumer; it never creates or migrates schema."""

    def __init__(self, registry_path: str | Path) -> None:
        path = _resolved_file(registry_path, "observability registry")
        self.registry_path = path
        self._conn = sqlite3.connect(
            path.as_uri() + "?mode=ro",
            uri=True,
            timeout=2.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA query_only=ON")
        self._conn.execute("PRAGMA busy_timeout=1000")
        if int(self._conn.execute("PRAGMA query_only").fetchone()[0]) != 1:
            self.close()
            raise ObservabilityContractError("registry connection is not query-only")
        try:
            meta = self._conn.execute(
                f"SELECT model_id,schema_version,model_fingerprint "
                f"FROM {META_TABLE} WHERE singleton=1"
            ).fetchone()
            columns = {
                str(row["name"])
                for row in self._conn.execute(
                    f"PRAGMA table_info({RUNS_TABLE})"
                ).fetchall()
            }
        except sqlite3.DatabaseError as exc:
            self.close()
            raise ObservabilityContractError("invalid observability registry") from exc
        expected_meta = (MODEL_ID, SCHEMA_VERSION, MODEL_FINGERPRINT)
        actual_meta = None if meta is None else tuple(meta)
        expected_columns = set(LiveRunRecordV01.__dataclass_fields__)
        if actual_meta != expected_meta or columns != expected_columns:
            self.close()
            raise ObservabilityContractError("observability registry schema mismatch")

    @property
    def connection_is_query_only(self) -> bool:
        return int(self._conn.execute("PRAGMA query_only").fetchone()[0]) == 1

    def records(self) -> tuple[LiveRunRecordV01, ...]:
        try:
            rows = self._conn.execute(
                f"SELECT * FROM {RUNS_TABLE} ORDER BY started_at_utc,run_id"
            ).fetchall()
        except sqlite3.DatabaseError as exc:
            raise ObservabilityContractError("could not read observability registry") from exc
        records = tuple(LiveRunRecordV01.from_row(row) for row in rows)
        for record in records:
            record.validate()
        return records

    def view(self, observed_at: datetime) -> ObserverViewV01:
        records = self.records()
        active = tuple(
            record.run_id for record in records if record.is_running_at(observed_at)
        )
        if not active:
            status = ObserverStatus.NO_ACTIVE_RUN
        elif len(active) == 1:
            status = ObserverStatus.ONE_ACTIVE_RUN
        else:
            status = ObserverStatus.AMBIGUOUS_MULTIPLE_ACTIVE_RUNS
        return ObserverViewV01(status=status, active_run_ids=active, records=records)

    def close(self) -> None:
        if getattr(self, "_conn", None) is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "ReadOnlyLiveObservabilityV01":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


def open_bound_source_readonly(record: LiveRunRecordV01) -> sqlite3.Connection:
    record.validate()
    path = _resolved_file(record.source_sqlite_path, "bound source SQLite")
    conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=1000")
    if int(conn.execute("PRAGMA query_only").fetchone()[0]) != 1:
        conn.close()
        raise ObservabilityContractError("bound source connection is not query-only")
    return conn


def read_bound_source_rows(
    record: LiveRunRecordV01,
    *,
    after_p1_rowid: int,
    through_p1_rowid: int,
    limit: int = 1_000,
) -> tuple[dict[str, Any], ...]:
    record.validate()
    after = int(after_p1_rowid)
    through = int(through_p1_rowid)
    bounded_limit = int(limit)
    if after < record.source_anchor_p1_rowid:
        raise ValueError("source probe cannot precede the bound run anchor")
    if through < after or through > record.source_watermark_p1_rowid:
        raise ValueError("source probe exceeds the bound run watermark")
    if not 1 <= bounded_limit <= 10_000:
        raise ValueError("source probe limit must be in 1..10000")
    conn = open_bound_source_readonly(record)
    try:
        rows = conn.execute(
            "SELECT rowid AS p1_rowid,* FROM pump_events "
            "WHERE rowid>? AND rowid<=? ORDER BY rowid ASC LIMIT ?",
            (after, through, bounded_limit),
        ).fetchall()
        return tuple(dict(row) for row in rows)
    except sqlite3.DatabaseError as exc:
        raise ObservabilityContractError("bound source row probe failed") from exc
    finally:
        conn.close()


def contract_field_names() -> tuple[str, ...]:
    return tuple(LiveRunRecordV01.__dataclass_fields__)


def assert_no_secret_fields(extra_names: Iterable[str] = ()) -> None:
    forbidden = (
        "seed",
        "private_key",
        "signing_key",
        "wallet_secret",
        "rpc_secret",
        "api_secret",
        "auth_token",
        "credential",
    )
    names = tuple(contract_field_names()) + tuple(str(name) for name in extra_names)
    offenders = tuple(
        name for name in names if any(token in name.lower() for token in forbidden)
    )
    if offenders:
        raise ObservabilityContractError(
            f"secret-bearing fields are prohibited: {offenders!r}"
        )


assert_no_secret_fields()
