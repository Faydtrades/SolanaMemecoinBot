from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from .models_v0_1 import (
    ConfirmationState,
    EventType,
    IngestionSource,
    NormalizedMarketEvent,
)


# Sources written by the validated v0.3.x live collectors preserve the actual
# observation/insertion time in decoded_at_utc. The v0.3.3 gap worker also writes
# recovery time there. Older offline import files are intentionally excluded from
# BOT_TRUTH v0.1 because their decoded_at_utc is not guaranteed to equal the time a
# live bot first knew the event.
BOT_TRUTH_SOURCE_PREFIXES: tuple[str, ...] = (
    "LIVE_WEBSOCKET_EVENT_V0_3",
    "GAP_RECONCILIATION_V0_3_3",
)

REQUIRED_PUMP_EVENT_COLUMNS: frozenset[str] = frozenset(
    {
        "event_key",
        "signature",
        "event_type",
        "slot",
        "pump_timestamp",
        "mint",
        "user_wallet",
        "creator_wallet",
        "sol_amount_lamports",
        "token_amount_raw",
        "virtual_sol_reserves",
        "virtual_token_reserves",
        "real_sol_reserves",
        "real_token_reserves",
        "source_decoded_file",
        "decoded_at_utc",
    }
)


@dataclass(frozen=True, slots=True)
class AdapterRowResult:
    event: NormalizedMarketEvent | None
    skip_reason: str | None


@dataclass(frozen=True, slots=True)
class CohortSelection:
    mints: tuple[str, ...]
    requested_min_trades: int
    effective_min_trades: int


class Phase1ReadOnlyAdapterV01:
    """Read-only adapter from the Phase-1 pump_events table to NME-0.1.

    Important semantics:
    * SQLite rowid is used as deterministic ingest_seq because Phase 1 inserts
      pump_events persistently in observation/recovery order.
    * pump_timestamp is market/event time (seconds -> microseconds).
    * decoded_at_utc is BOT_TRUTH observed time for supported v0.3.x live sources.
    * gap-recovered rows retain GAP_RECOVERY provenance and are therefore not treated
      as if they had been known at their older market timestamp.
    * This class never creates or mutates schema in the source database.
    """

    schema_version = "P1ROA-0.1"

    @staticmethod
    def open_readonly(db_path: str | Path) -> sqlite3.Connection:
        path = Path(db_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Phase-1 database not found: {path}")
        uri = f"{path.as_uri()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        return conn

    @staticmethod
    def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
        return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")}

    @classmethod
    def validate_schema(cls, conn: sqlite3.Connection) -> None:
        table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='pump_events'"
        ).fetchone()
        if table is None:
            raise ValueError("Phase-1 database has no pump_events table")
        columns = cls.table_columns(conn, "pump_events")
        missing = sorted(REQUIRED_PUMP_EVENT_COLUMNS - columns)
        if missing:
            raise ValueError(f"pump_events is missing required columns: {missing}")

    @staticmethod
    def is_bot_truth_source(source: str | None) -> bool:
        if not source:
            return False
        return any(source.startswith(prefix) for prefix in BOT_TRUTH_SOURCE_PREFIXES)

    @staticmethod
    def _iso_to_us(value: str) -> int:
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            raise ValueError(f"UTC timestamp lacks timezone: {value!r}")
        dt = dt.astimezone(timezone.utc)
        return int(round(dt.timestamp() * 1_000_000))

    @staticmethod
    def _optional_int(value: object) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        return int(str(value))

    @staticmethod
    def _event_index(event_key: str, signature: str) -> int:
        prefix = signature + ":"
        if event_key.startswith(prefix):
            rest = event_key[len(prefix):]
            first = rest.split(":", 1)[0]
            try:
                return int(first)
            except ValueError:
                pass
        # Historical rows should still be usable; event_key remains authoritative ID.
        return 0

    @staticmethod
    def _source(source_decoded_file: str) -> IngestionSource:
        if source_decoded_file.startswith("GAP_RECONCILIATION_"):
            return IngestionSource.GAP_RECOVERY
        return IngestionSource.LIVE_WS

    @staticmethod
    def _confirmation_available_at_observation(
        source_decoded_file: str,
    ) -> ConfirmationState:
        # Live WS events enter pump_events as 'processed' and are confirmed later by
        # a background worker. The current confirmation_status column therefore must
        # NOT be projected backwards into the original observation time.
        if source_decoded_file.startswith("GAP_RECONCILIATION_"):
            return ConfirmationState.CONFIRMED
        return ConfirmationState.PROCESSED

    @classmethod
    def row_to_event(cls, row: sqlite3.Row) -> AdapterRowResult:
        source_file = row["source_decoded_file"]
        if not cls.is_bot_truth_source(source_file):
            return AdapterRowResult(None, "UNSUPPORTED_NON_BOT_TRUTH_SOURCE")

        event_type_raw = row["event_type"]
        try:
            event_type = EventType(str(event_type_raw))
        except Exception:
            return AdapterRowResult(None, "UNSUPPORTED_EVENT_TYPE")

        required_values = {
            "event_key": row["event_key"],
            "signature": row["signature"],
            "slot": row["slot"],
            "pump_timestamp": row["pump_timestamp"],
            "mint": row["mint"],
            "decoded_at_utc": row["decoded_at_utc"],
        }
        missing = [name for name, value in required_values.items() if value is None]
        if missing:
            return AdapterRowResult(None, "MISSING_REQUIRED_VALUE:" + ",".join(missing))

        try:
            event_key = str(row["event_key"])
            signature = str(row["signature"])
            event = NormalizedMarketEvent(
                schema_version="NME-0.1",
                event_key=event_key,
                ingest_seq=int(row["p1_rowid"]),
                mint=str(row["mint"]),
                event_type=event_type,
                event_at_us=int(row["pump_timestamp"]) * 1_000_000,
                observed_at_us=cls._iso_to_us(str(row["decoded_at_utc"])),
                slot=int(row["slot"]),
                signature=signature,
                event_index=cls._event_index(event_key, signature),
                user=str(row["user_wallet"]) if row["user_wallet"] is not None else None,
                creator=(
                    str(row["creator_wallet"])
                    if row["creator_wallet"] is not None
                    else None
                ),
                sol_amount_lamports=cls._optional_int(row["sol_amount_lamports"]),
                token_amount_raw=cls._optional_int(row["token_amount_raw"]),
                virtual_sol_reserve_lamports=cls._optional_int(
                    row["virtual_sol_reserves"]
                ),
                virtual_token_reserve_raw=cls._optional_int(
                    row["virtual_token_reserves"]
                ),
                real_sol_reserve_lamports=cls._optional_int(row["real_sol_reserves"]),
                real_token_reserve_raw=cls._optional_int(row["real_token_reserves"]),
                source=cls._source(str(source_file)),
                confirmation_state=cls._confirmation_available_at_observation(
                    str(source_file)
                ),
            )
        except Exception as exc:
            return AdapterRowResult(None, f"NORMALIZATION_ERROR:{type(exc).__name__}:{exc}")

        return AdapterRowResult(event, None)

    @classmethod
    def source_summary(cls, conn: sqlite3.Connection) -> list[tuple[str, int]]:
        rows = conn.execute(
            """
            SELECT source_decoded_file, COUNT(*) AS n
            FROM pump_events
            GROUP BY source_decoded_file
            ORDER BY n DESC, source_decoded_file
            """
        ).fetchall()
        return [(str(row["source_decoded_file"]), int(row["n"])) for row in rows]

    @classmethod
    def select_cohort(
        cls,
        conn: sqlite3.Connection,
        *,
        token_limit: int = 12,
        min_trades: int = 3,
    ) -> CohortSelection:
        if token_limit <= 0:
            raise ValueError("token_limit must be > 0")
        if min_trades <= 0:
            raise ValueError("min_trades must be > 0")

        # Prefix matching keeps the SQL compatible with v0.3, v0.3.2 and v0.3.3
        # while excluding older offline-imported datasets from BOT_TRUTH.
        where_source = "(" + " OR ".join(
            "source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES
        ) + ")"
        source_args = tuple(prefix + "%" for prefix in BOT_TRUTH_SOURCE_PREFIXES)

        def query(threshold: int) -> tuple[str, ...]:
            rows = conn.execute(
                f"""
                SELECT mint, COUNT(*) AS trade_count, MAX(rowid) AS newest_rowid
                FROM pump_events
                WHERE mint IS NOT NULL
                  AND event_type IN ('BUY', 'SELL')
                  AND pump_timestamp IS NOT NULL
                  AND decoded_at_utc IS NOT NULL
                  AND slot IS NOT NULL
                  AND {where_source}
                GROUP BY mint
                HAVING COUNT(*) >= ?
                ORDER BY newest_rowid DESC
                LIMIT ?
                """,
                (*source_args, threshold, token_limit),
            ).fetchall()
            return tuple(str(row["mint"]) for row in rows)

        mints = query(min_trades)
        effective = min_trades
        if not mints and min_trades > 1:
            mints = query(1)
            effective = 1
        return CohortSelection(mints, min_trades, effective)

    @classmethod
    def fetch_rows_for_mints(
        cls,
        conn: sqlite3.Connection,
        mints: Sequence[str],
        *,
        max_events_per_token: int = 120,
    ) -> list[sqlite3.Row]:
        if max_events_per_token <= 0:
            raise ValueError("max_events_per_token must be > 0")
        if not mints:
            return []

        where_source = "(" + " OR ".join(
            "source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES
        ) + ")"
        source_args = tuple(prefix + "%" for prefix in BOT_TRUTH_SOURCE_PREFIXES)

        all_rows: list[sqlite3.Row] = []
        for mint in mints:
            rows = conn.execute(
                f"""
                SELECT rowid AS p1_rowid, *
                FROM pump_events
                WHERE mint = ?
                  AND event_type IN ('LAUNCH', 'BUY', 'SELL')
                  AND pump_timestamp IS NOT NULL
                  AND decoded_at_utc IS NOT NULL
                  AND slot IS NOT NULL
                  AND {where_source}
                ORDER BY rowid ASC
                LIMIT ?
                """,
                (mint, *source_args, max_events_per_token),
            ).fetchall()
            all_rows.extend(rows)

        # True replay processing order is observation time, with Phase-1 rowid as
        # deterministic tie-breaker when multiple Pump events share the same callback.
        all_rows.sort(
            key=lambda row: (
                cls._iso_to_us(str(row["decoded_at_utc"])),
                int(row["p1_rowid"]),
            )
        )
        return all_rows

    @classmethod
    def normalize_rows(
        cls, rows: Iterable[sqlite3.Row]
    ) -> tuple[list[NormalizedMarketEvent], dict[str, int]]:
        events: list[NormalizedMarketEvent] = []
        skipped: dict[str, int] = {}
        for row in rows:
            result = cls.row_to_event(row)
            if result.event is not None:
                events.append(result.event)
            else:
                reason = result.skip_reason or "UNKNOWN"
                skipped[reason] = skipped.get(reason, 0) + 1
        return events, skipped
