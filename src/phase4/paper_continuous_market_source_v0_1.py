from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from phase2.models_v0_1 import EventType, IngestionSource
from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from phase2.quote_aware_price_v0_1 import (
    Phase1QuoteAwareAdapterV01,
    QuoteAwarePriceEngineV01,
)


MODEL_ID = "P4-CONTINUOUS-MARKET-SOURCE-0001"
SCHEMA_VERSION = "phase4_continuous_market_source_v0.1"
DEFAULT_BATCH_SIZE = 1_000
MAX_BATCH_SIZE = 10_000
DEFAULT_PRODUCTION_DB = (
    Path(__file__).resolve().parents[2] / "data" / "db" / "tradingbot.sqlite3"
)

_SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "phase1_adapter": Phase1ReadOnlyAdapterV01.schema_version,
    "phase1_quote_adapter": Phase1QuoteAwareAdapterV01.schema_version,
    "quote_price_engine": QuoteAwarePriceEngineV01.schema_version,
    "connection": "SQLITE_URI_MODE_RO_AND_QUERY_ONLY",
    "polling": "ROWID_GT_CURSOR_ASCENDING_BOUNDED_NO_INTERNAL_COMMIT",
    "session": "EXPLICIT_START_AFTER_ROWID_POST_ANCHOR_LAUNCH_ONLY",
    "launch_eligibility": (
        "POST_ANCHOR_PHASE2_NORMALIZED_NON_GAP_LAUNCH_ONLY"
    ),
    "normalization": "ACCEPTED_PHASE2_QUOTE_AWARE_POINT_NO_FLOATS",
    "gap_semantics": "GAP_RECOVERY_EXPLICIT_NOT_FRESH_TRADABLE_FLOW",
    "unavailable_price": "DETERMINISTIC_SKIP_NO_FABRICATION",
    "default_batch_size": DEFAULT_BATCH_SIZE,
    "max_batch_size": MAX_BATCH_SIZE,
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(_SPEC, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "utf-8"
    )
).hexdigest()

UTC = timezone.utc
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)


class SourceSchemaError(ValueError):
    """The production source cannot satisfy the locked read contract."""


@dataclass(frozen=True, slots=True)
class MarketSourceSkipV01:
    production_p1_rowid: int
    event_key: str | None
    mint: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class ContinuousMarketSourceRecordV01:
    schema_version: str
    source_model_id: str
    source_model_fingerprint: str
    source_identity: str
    start_after_p1_rowid: int
    production_p1_rowid: int
    session_launch_p1_rowid: int
    event_key: str
    mint: str
    event_type: str
    event_at: datetime
    observed_at: datetime
    ingest_seq: int
    price_identity: str
    price_numerator_raw: int
    price_denominator_raw: int
    current_virtual_token_reserve_raw: int
    is_gap_recovery: bool
    ingestion_source: str

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError("market-source record schema version mismatch")
        if self.source_model_id != MODEL_ID:
            raise ValueError("market-source record model ID mismatch")
        if self.source_model_fingerprint != MODEL_FINGERPRINT:
            raise ValueError("market-source record model fingerprint mismatch")
        if self.start_after_p1_rowid < 0:
            raise ValueError("start_after_p1_rowid must be >= 0")
        if self.production_p1_rowid <= self.start_after_p1_rowid:
            raise ValueError("record rowid must be strictly after the session anchor")
        if not (
            self.start_after_p1_rowid
            < self.session_launch_p1_rowid
            <= self.production_p1_rowid
        ):
            raise ValueError("record has invalid post-anchor launch provenance")
        if self.ingest_seq != self.production_p1_rowid:
            raise ValueError("Phase-2 ingest_seq must equal production p1_rowid")
        if self.price_numerator_raw <= 0 or self.price_denominator_raw <= 0:
            raise ValueError("market-source reserve identities must be positive")
        if self.current_virtual_token_reserve_raw != self.price_denominator_raw:
            raise ValueError("current virtual token reserve must bind price denominator")
        object.__setattr__(self, "event_at", _utc(self.event_at))
        object.__setattr__(self, "observed_at", _utc(self.observed_at))

    @property
    def content_fingerprint(self) -> str:
        return _fingerprint(self)


@dataclass(frozen=True, slots=True)
class MarketSourceBatchV01:
    source_identity: str
    start_after_p1_rowid: int
    requested_after_p1_rowid: int
    batch_limit: int
    raw_rows_fetched: int
    highest_fetched_p1_rowid: int
    records: tuple[ContinuousMarketSourceRecordV01, ...]
    skips: tuple[MarketSourceSkipV01, ...]
    skipped_by_reason: tuple[tuple[str, int], ...]

    @property
    def canonical_digest(self) -> str:
        return _fingerprint(self)


class ContinuousMarketSourceV01:
    """Bounded read-only Pump.fun market source with explicit session anchoring.

    The object has no mutable consumption cursor. Each fetch takes the caller's
    last durably accepted production rowid and returns provenance for the rows it
    merely read. A later binding layer owns any durable cursor commit.
    """

    def __init__(
        self,
        db_path: str | Path = DEFAULT_PRODUCTION_DB,
        *,
        start_after_p1_rowid: int,
        database_identity: str | None = None,
    ) -> None:
        anchor = int(start_after_p1_rowid)
        if anchor < 0:
            raise ValueError("start_after_p1_rowid must be >= 0")
        path = Path(db_path).resolve()
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"production market database not found: {path}")
        identity = path.as_posix() if database_identity is None else str(database_identity)
        if not identity.strip():
            raise ValueError("database_identity must not be empty")
        self._db_path = path
        self._database_identity = identity
        self._start_after_p1_rowid = anchor
        self._source_identity = _fingerprint(
            {
                "model_id": MODEL_ID,
                "model_fingerprint": MODEL_FINGERPRINT,
                "database_identity": identity,
                "start_after_p1_rowid": anchor,
            }
        )

    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def start_after_p1_rowid(self) -> int:
        return self._start_after_p1_rowid

    @property
    def database_identity(self) -> str:
        return self._database_identity

    @property
    def source_identity(self) -> str:
        return self._source_identity

    @staticmethod
    def open_readonly(db_path: str | Path) -> sqlite3.Connection:
        path = Path(db_path).resolve()
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"production market database not found: {path}")
        conn = sqlite3.connect(
            path.as_uri() + "?mode=ro",
            uri=True,
            timeout=2.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA busy_timeout=1000")
        if not ContinuousMarketSourceV01.connection_is_query_only(conn):
            conn.close()
            raise RuntimeError("SQLite source connection did not enter query-only mode")
        return conn

    @staticmethod
    def connection_is_query_only(conn: sqlite3.Connection) -> bool:
        row = conn.execute("PRAGMA query_only").fetchone()
        return row is not None and int(row[0]) == 1

    @staticmethod
    def validate_schema(conn: sqlite3.Connection) -> None:
        try:
            Phase1QuoteAwareAdapterV01.validate_schema(conn)
            conn.execute(
                "SELECT rowid AS p1_rowid FROM pump_events ORDER BY rowid LIMIT 0"
            ).fetchall()
        except (sqlite3.DatabaseError, ValueError) as exc:
            raise SourceSchemaError(str(exc)) from exc

    def latest_p1_rowid(self) -> int:
        conn = self.open_readonly(self.db_path)
        try:
            self.validate_schema(conn)
            row = conn.execute(
                "SELECT rowid AS p1_rowid FROM pump_events "
                "ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            return 0 if row is None else int(row["p1_rowid"])
        finally:
            conn.close()

    def fetch_batch(
        self,
        *,
        after_p1_rowid: int,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> MarketSourceBatchV01:
        after = int(after_p1_rowid)
        limit = int(batch_size)
        if after < self.start_after_p1_rowid:
            raise ValueError(
                "after_p1_rowid cannot precede the explicit session anchor"
            )
        if limit <= 0 or limit > MAX_BATCH_SIZE:
            raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")

        conn = self.open_readonly(self.db_path)
        try:
            self.validate_schema(conn)
            launch_by_mint = self._eligible_launches_through(conn, after)
            rows = conn.execute(
                """
                SELECT rowid AS p1_rowid, *
                FROM pump_events
                WHERE rowid > ?
                ORDER BY rowid ASC
                LIMIT ?
                """,
                (after, limit),
            ).fetchall()
            return self._normalize_batch(
                rows,
                requested_after=after,
                batch_limit=limit,
                launch_by_mint=launch_by_mint,
            )
        finally:
            conn.close()

    def _eligible_launches_through(
        self,
        conn: sqlite3.Connection,
        through_p1_rowid: int,
    ) -> dict[str, int]:
        if through_p1_rowid <= self.start_after_p1_rowid:
            return {}
        rows = conn.execute(
            """
            SELECT rowid AS p1_rowid, *
            FROM pump_events
            WHERE rowid > ? AND rowid <= ?
              AND event_type = 'LAUNCH'
              AND mint IS NOT NULL
            ORDER BY rowid ASC
            """,
            (self.start_after_p1_rowid, through_p1_rowid),
        ).fetchall()
        launches: dict[str, int] = {}
        for row in rows:
            quote_event, _reason = Phase1QuoteAwareAdapterV01.row_to_event(row)
            if quote_event is not None and self._is_qualifying_launch(quote_event):
                launches.setdefault(
                    str(quote_event.base.mint), int(row["p1_rowid"])
                )
        return launches

    def _normalize_batch(
        self,
        rows: list[sqlite3.Row],
        *,
        requested_after: int,
        batch_limit: int,
        launch_by_mint: dict[str, int],
    ) -> MarketSourceBatchV01:
        records: list[ContinuousMarketSourceRecordV01] = []
        skips: list[MarketSourceSkipV01] = []
        counts: dict[str, int] = {}
        previous_rowid = requested_after

        for row in rows:
            rowid = int(row["p1_rowid"])
            if rowid <= previous_rowid:
                raise RuntimeError("production rows were not strictly ascending")
            previous_rowid = rowid
            raw_mint = row["mint"]
            mint = str(raw_mint) if raw_mint else None
            raw_event_key = row["event_key"]
            event_key = str(raw_event_key) if raw_event_key is not None else None
            if mint is None:
                self._add_skip(
                    skips, counts, rowid, event_key, None, "MISSING_MINT"
                )
                continue

            quote_event, reason = Phase1QuoteAwareAdapterV01.row_to_event(row)
            if quote_event is None:
                self._add_skip(
                    skips,
                    counts,
                    rowid,
                    event_key,
                    mint,
                    reason or "UNKNOWN_NORMALIZATION_SKIP",
                )
                continue

            base = quote_event.base
            normalized_mint = str(base.mint)
            if self._is_qualifying_launch(quote_event):
                launch_by_mint.setdefault(normalized_mint, rowid)
            launch_rowid = launch_by_mint.get(normalized_mint)
            if launch_rowid is None:
                eligibility_reason = "MINT_NOT_SESSION_ELIGIBLE"
                if (
                    base.event_type == EventType.LAUNCH
                    and base.source == IngestionSource.GAP_RECOVERY
                ):
                    eligibility_reason = (
                        "GAP_RECOVERY_LAUNCH_NOT_SESSION_ELIGIBLE"
                    )
                self._add_skip(
                    skips,
                    counts,
                    rowid,
                    str(base.event_key),
                    normalized_mint,
                    eligibility_reason,
                )
                continue

            point = QuoteAwarePriceEngineV01.point(quote_event)
            if (
                point.price_proxy is None
                or point.numerator_reserve_raw is None
                or point.token_reserve_raw is None
                or int(point.numerator_reserve_raw) <= 0
                or int(point.token_reserve_raw) <= 0
            ):
                self._add_skip(
                    skips,
                    counts,
                    rowid,
                    event_key,
                    mint,
                    "PRICE_POINT_UNAVAILABLE:" + str(point.identity.label),
                )
                continue

            if int(base.ingest_seq) != rowid:
                raise RuntimeError("accepted Phase-2 ingest sequence lost rowid identity")
            records.append(
                ContinuousMarketSourceRecordV01(
                    schema_version=SCHEMA_VERSION,
                    source_model_id=MODEL_ID,
                    source_model_fingerprint=MODEL_FINGERPRINT,
                    source_identity=self.source_identity,
                    start_after_p1_rowid=self.start_after_p1_rowid,
                    production_p1_rowid=rowid,
                    session_launch_p1_rowid=launch_rowid,
                    event_key=str(base.event_key),
                    mint=str(base.mint),
                    event_type=str(base.event_type.value),
                    event_at=_us_to_datetime(int(base.event_at_us)),
                    observed_at=_us_to_datetime(int(base.observed_at_us)),
                    ingest_seq=int(base.ingest_seq),
                    price_identity=str(point.identity.label),
                    price_numerator_raw=int(point.numerator_reserve_raw),
                    price_denominator_raw=int(point.token_reserve_raw),
                    current_virtual_token_reserve_raw=int(point.token_reserve_raw),
                    is_gap_recovery=base.source == IngestionSource.GAP_RECOVERY,
                    ingestion_source=str(base.source.value),
                )
            )

        highest = requested_after if not rows else int(rows[-1]["p1_rowid"])
        return MarketSourceBatchV01(
            source_identity=self.source_identity,
            start_after_p1_rowid=self.start_after_p1_rowid,
            requested_after_p1_rowid=requested_after,
            batch_limit=batch_limit,
            raw_rows_fetched=len(rows),
            highest_fetched_p1_rowid=highest,
            records=tuple(records),
            skips=tuple(skips),
            skipped_by_reason=tuple(sorted(counts.items())),
        )

    @staticmethod
    def _add_skip(
        skips: list[MarketSourceSkipV01],
        counts: dict[str, int],
        rowid: int,
        event_key: str | None,
        mint: str | None,
        reason: str,
    ) -> None:
        skips.append(MarketSourceSkipV01(rowid, event_key, mint, reason))
        counts[reason] = counts.get(reason, 0) + 1

    @staticmethod
    def _is_qualifying_launch(quote_event: Any) -> bool:
        base = quote_event.base
        return (
            base.event_type == EventType.LAUNCH
            and base.source != IngestionSource.GAP_RECOVERY
        )


def _primitive(value: Any) -> Any:
    if is_dataclass(value):
        return _primitive(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Decimal values must be finite")
        return str(value)
    if isinstance(value, datetime):
        return _dt_text(value)
    if isinstance(value, dict):
        return {str(k): _primitive(v) for k, v in value.items()}
    if isinstance(value, (tuple, list, set)):
        return [_primitive(v) for v in value]
    return value


def _fingerprint(value: Any) -> str:
    payload = json.dumps(
        _primitive(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _us_to_datetime(value: int) -> datetime:
    return EPOCH_UTC + timedelta(microseconds=int(value))
