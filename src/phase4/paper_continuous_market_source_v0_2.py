from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .paper_continuous_market_source_v0_1 import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_PRODUCTION_DB,
    MAX_BATCH_SIZE,
    MODEL_FINGERPRINT as V01_MODEL_FINGERPRINT,
    ContinuousMarketSourceRecordV01,
    ContinuousMarketSourceV01,
    MarketSourceSkipV01,
    SourceSchemaError,
    _dt_text,
    _fingerprint,
    _utc,
)
from .phase1_gap_source_compat_v0_1 import (
    MODEL_FINGERPRINT as GAP_COMPAT_FINGERPRINT,
    Phase1GapSourceCompatibilityV01,
    V034_GAP_SOURCE,
)


MODEL_ID = "P4-CONTINUOUS-MARKET-SOURCE-0002"
SCHEMA_VERSION = "phase4_continuous_market_source_v0.2"
SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "accepted_market_source_v0_1_fingerprint": V01_MODEL_FINGERPRINT,
    "gap_compatibility_fingerprint": GAP_COMPAT_FINGERPRINT,
    "connection": "FRESH_SQLITE_URI_MODE_RO_AND_QUERY_ONLY_PER_OPERATION",
    "launch_cache": "NON_AUTHORITATIVE_IN_MEMORY_REBUILDABLE",
    "initialization": "ONE_FULL_REBUILD_ANCHOR_THROUGH_REQUESTED_CURSOR",
    "monotonic_fetch": "NO_PREFIX_REBUILD_INCREMENTAL_BATCH_UPDATE_AFTER_SUCCESS",
    "forward_jump": "CATCH_UP_MISSING_INTERVAL_ONLY",
    "rewind": "REBUILD_ANCHOR_THROUGH_REQUESTED_CURSOR",
    "failure_atomicity": "CACHE_AUTHORITY_ADVANCES_AFTER_NORMALIZATION_SUCCESS_ONLY",
    "continuity_probe": "ROWID_GT_LAST_HEALTH_CURSOR_BOUNDED_INSERTED_AT_UTC",
    "durable_cache": False,
    "default_batch_size": DEFAULT_BATCH_SIZE,
    "max_batch_size": MAX_BATCH_SIZE,
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


@dataclass(frozen=True, slots=True)
class ContinuousMarketSourceRecordV02:
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
    raw_source_decoded_file: str
    source_compatibility_applied: bool

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError("market-source v0.2 record schema version mismatch")
        if self.source_model_id != MODEL_ID:
            raise ValueError("market-source v0.2 record model ID mismatch")
        if self.source_model_fingerprint != MODEL_FINGERPRINT:
            raise ValueError("market-source v0.2 record fingerprint mismatch")
        if self.start_after_p1_rowid < 0:
            raise ValueError("start_after_p1_rowid must be >= 0")
        if not (
            self.start_after_p1_rowid
            < self.session_launch_p1_rowid
            <= self.production_p1_rowid
        ):
            raise ValueError("record has invalid post-anchor launch provenance")
        if self.ingest_seq != self.production_p1_rowid:
            raise ValueError("ingest_seq must equal production p1_rowid")
        if self.price_numerator_raw <= 0 or self.price_denominator_raw <= 0:
            raise ValueError("market-source reserve identities must be positive")
        if self.current_virtual_token_reserve_raw != self.price_denominator_raw:
            raise ValueError("token reserve must bind the price denominator")
        if self.source_compatibility_applied:
            if self.raw_source_decoded_file != V034_GAP_SOURCE:
                raise ValueError("gap compatibility provenance label mismatch")
            if not self.is_gap_recovery:
                raise ValueError("gap compatibility cannot produce fresh flow")
        object.__setattr__(self, "event_at", _utc(self.event_at))
        object.__setattr__(self, "observed_at", _utc(self.observed_at))

    @property
    def content_fingerprint(self) -> str:
        return _fingerprint(self)


@dataclass(frozen=True, slots=True)
class MarketSourceBatchV02:
    source_identity: str
    start_after_p1_rowid: int
    requested_after_p1_rowid: int
    batch_limit: int
    raw_rows_fetched: int
    highest_fetched_p1_rowid: int
    records: tuple[ContinuousMarketSourceRecordV02, ...]
    skips: tuple[MarketSourceSkipV01, ...]
    skipped_by_reason: tuple[tuple[str, int], ...]

    @property
    def canonical_digest(self) -> str:
        return _fingerprint(self)


class ContinuousMarketSourceV02(ContinuousMarketSourceV01):
    """V01-equivalent source with a non-authoritative incremental launch cache."""

    model_id = MODEL_ID
    model_fingerprint = MODEL_FINGERPRINT
    schema_version = SCHEMA_VERSION

    def __init__(
        self,
        db_path: str | Path = DEFAULT_PRODUCTION_DB,
        *,
        start_after_p1_rowid: int,
        database_identity: str | None = None,
    ) -> None:
        super().__init__(
            db_path,
            start_after_p1_rowid=start_after_p1_rowid,
            database_identity=database_identity,
        )
        self._source_identity = _fingerprint({
            "model_id": MODEL_ID,
            "model_fingerprint": MODEL_FINGERPRINT,
            "database_identity": self.database_identity,
            "start_after_p1_rowid": self.start_after_p1_rowid,
        })
        self._launch_state_through_p1_rowid: int | None = None
        self._launch_by_mint: dict[str, int] = {}
        self._metrics = {
            "full_launch_rebuild_queries": 0,
            "incremental_launch_catchup_queries": 0,
            "normal_monotonic_fetches_without_rebuild": 0,
            "rewind_rebuild_queries": 0,
            "source_health_probe_count": 0,
            "source_health_rows_scanned": 0,
        }

    @property
    def launch_state_through_p1_rowid(self) -> int | None:
        return self._launch_state_through_p1_rowid

    def cache_metrics(self) -> dict[str, int]:
        return dict(self._metrics)

    def fetch_batch(
        self,
        *,
        after_p1_rowid: int,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> MarketSourceBatchV02:
        after = int(after_p1_rowid)
        limit = int(batch_size)
        if after < self.start_after_p1_rowid:
            raise ValueError("after_p1_rowid cannot precede the session anchor")
        if limit <= 0 or limit > MAX_BATCH_SIZE:
            raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")

        conn = self.open_readonly(self.db_path)
        try:
            self.validate_schema(conn)
            cached_through = self._launch_state_through_p1_rowid
            if cached_through is None:
                working_launches: dict[str, int] = {}
                self._metrics["full_launch_rebuild_queries"] += 1
                self._update_launches_in_interval(
                    conn,
                    lower_exclusive=self.start_after_p1_rowid,
                    upper_inclusive=after,
                    launches=working_launches,
                )
            elif after == cached_through:
                working_launches = dict(self._launch_by_mint)
            elif after > cached_through:
                working_launches = dict(self._launch_by_mint)
                self._metrics["incremental_launch_catchup_queries"] += 1
                self._update_launches_in_interval(
                    conn,
                    lower_exclusive=cached_through,
                    upper_inclusive=after,
                    launches=working_launches,
                )
            else:
                working_launches = {}
                self._metrics["rewind_rebuild_queries"] += 1
                self._update_launches_in_interval(
                    conn,
                    lower_exclusive=self.start_after_p1_rowid,
                    upper_inclusive=after,
                    launches=working_launches,
                )

            rows = conn.execute(
                "SELECT rowid AS p1_rowid,* FROM pump_events "
                "WHERE rowid>? ORDER BY rowid ASC LIMIT ?",
                (after, limit),
            ).fetchall()
            batch = self._normalize_rows(
                rows,
                requested_after=after,
                batch_limit=limit,
                launch_by_mint=working_launches,
            )
        finally:
            conn.close()

        self._launch_by_mint = working_launches
        self._launch_state_through_p1_rowid = batch.highest_fetched_p1_rowid
        if cached_through is not None and after == cached_through:
            self._metrics["normal_monotonic_fetches_without_rebuild"] += 1
        return batch

    def continuity_rows_after(
        self,
        after_p1_rowid: int,
        *,
        through_p1_rowid: int,
    ) -> tuple[tuple[int, datetime], ...]:
        after = int(after_p1_rowid)
        through = int(through_p1_rowid)
        if after < self.start_after_p1_rowid or through < after:
            raise ValueError("invalid continuity probe cursor interval")
        self._metrics["source_health_probe_count"] += 1
        conn = self.open_readonly(self.db_path)
        try:
            self.validate_schema(conn)
            try:
                rows = conn.execute(
                    "SELECT rowid AS p1_rowid,inserted_at_utc FROM pump_events "
                    "WHERE rowid>? AND rowid<=? ORDER BY rowid ASC",
                    (after, through),
                ).fetchall()
            except sqlite3.DatabaseError as exc:
                raise SourceSchemaError(
                    "pump_events.inserted_at_utc is required for source continuity"
                ) from exc
        finally:
            conn.close()
        result: list[tuple[int, datetime]] = []
        for row in rows:
            raw = row["inserted_at_utc"]
            if raw is None:
                raise SourceSchemaError("source continuity timestamp is NULL")
            try:
                observed = datetime.fromisoformat(
                    str(raw).replace("Z", "+00:00")
                )
                result.append((int(row["p1_rowid"]), _utc(observed)))
            except ValueError as exc:
                raise SourceSchemaError(
                    f"invalid source continuity timestamp at p1 rowid {row['p1_rowid']}"
                ) from exc
        self._metrics["source_health_rows_scanned"] += len(result)
        return tuple(result)

    def _update_launches_in_interval(
        self,
        conn: sqlite3.Connection,
        *,
        lower_exclusive: int,
        upper_inclusive: int,
        launches: dict[str, int],
    ) -> None:
        if upper_inclusive <= lower_exclusive:
            return
        rows = conn.execute(
            "SELECT rowid AS p1_rowid,* FROM pump_events "
            "WHERE rowid>? AND rowid<=? AND event_type='LAUNCH' "
            "AND mint IS NOT NULL ORDER BY rowid ASC",
            (lower_exclusive, upper_inclusive),
        ).fetchall()
        for row in rows:
            compat = Phase1GapSourceCompatibilityV01.row_to_event(row)
            quote_event = compat.event
            if quote_event is not None and self._is_qualifying_launch(quote_event):
                launches.setdefault(str(quote_event.base.mint), int(row["p1_rowid"]))

    def _normalize_rows(
        self,
        rows: list[sqlite3.Row],
        *,
        requested_after: int,
        batch_limit: int,
        launch_by_mint: dict[str, int],
    ) -> MarketSourceBatchV02:
        v01_batch = ContinuousMarketSourceV01._normalize_batch(
            self,
            rows,
            requested_after=requested_after,
            batch_limit=batch_limit,
            launch_by_mint=launch_by_mint,
        )
        records = tuple(self._record_v02(record) for record in v01_batch.records)
        return MarketSourceBatchV02(
            source_identity=self.source_identity,
            start_after_p1_rowid=self.start_after_p1_rowid,
            requested_after_p1_rowid=requested_after,
            batch_limit=batch_limit,
            raw_rows_fetched=v01_batch.raw_rows_fetched,
            highest_fetched_p1_rowid=v01_batch.highest_fetched_p1_rowid,
            records=records,
            skips=v01_batch.skips,
            skipped_by_reason=v01_batch.skipped_by_reason,
        )

    def _record_v02(
        self, record: ContinuousMarketSourceRecordV01
    ) -> ContinuousMarketSourceRecordV02:
        values = asdict(record)
        values.update({
            "schema_version": SCHEMA_VERSION,
            "source_model_id": MODEL_ID,
            "source_model_fingerprint": MODEL_FINGERPRINT,
            "source_identity": self.source_identity,
        })
        return ContinuousMarketSourceRecordV02(**values)


def semantic_record(
    record: ContinuousMarketSourceRecordV01 | ContinuousMarketSourceRecordV02,
) -> dict[str, Any]:
    values = asdict(record)
    for key in (
        "schema_version",
        "source_model_id",
        "source_model_fingerprint",
        "source_identity",
    ):
        values.pop(key, None)
    values["event_at"] = _dt_text(values["event_at"])
    values["observed_at"] = _dt_text(values["observed_at"])
    return values
