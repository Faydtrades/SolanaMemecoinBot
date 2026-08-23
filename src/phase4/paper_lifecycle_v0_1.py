from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Iterable


SCHEMA_VERSION = "phase4_paper_lifecycle_v0.1"
ENGINE_VERSION = "PaperLifecycleV01"

LOCKED_PHASE4_TRACKS: dict[str, str] = {
    "FINAL-A": "E3_TP10_T15",
    "FINAL-B": "E4_ACT10_GB03_T15",
    "SENS-C": "SENS_TP20_T5",
}


class OrderState(StrEnum):
    CREATED = "CREATED"
    ENTRY_PENDING = "ENTRY_PENDING"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


class PositionState(StrEnum):
    OPEN = "OPEN"
    EXIT_PENDING = "EXIT_PENDING"
    CLOSED = "CLOSED"


ORDER_TRANSITIONS: dict[OrderState, frozenset[OrderState]] = {
    OrderState.CREATED: frozenset(
        {OrderState.ENTRY_PENDING, OrderState.REJECTED, OrderState.CANCELLED}
    ),
    OrderState.ENTRY_PENDING: frozenset(
        {OrderState.FILLED, OrderState.REJECTED, OrderState.CANCELLED}
    ),
    OrderState.FILLED: frozenset(),
    OrderState.REJECTED: frozenset(),
    OrderState.CANCELLED: frozenset(),
}

POSITION_TRANSITIONS: dict[PositionState, frozenset[PositionState]] = {
    PositionState.OPEN: frozenset({PositionState.EXIT_PENDING}),
    PositionState.EXIT_PENDING: frozenset({PositionState.CLOSED}),
    PositionState.CLOSED: frozenset(),
}


@dataclass(frozen=True, slots=True)
class PaperCandidateRef:
    """Execution-layer reference to a CandidateSignal.

    CandidateSignal remains a strategy object.  Phase 4 receives only the
    stable identity/provenance needed to create a paper order; it does not
    mutate or add execution state to CandidateSignal.
    """

    signal_key: str
    mint: str
    strategy_version: str
    parameter_set_id: str
    signal_observed_at: datetime
    signal_ingest_seq: int
    reference_price_identity: str
    reference_price_numerator_raw: int
    reference_price_denominator_raw: int

    def __post_init__(self) -> None:
        if not self.signal_key:
            raise ValueError("signal_key is required")
        if not self.mint:
            raise ValueError("mint is required")
        if not self.strategy_version:
            raise ValueError("strategy_version is required")
        if not self.parameter_set_id:
            raise ValueError("parameter_set_id is required")
        _require_aware(self.signal_observed_at)
        if self.signal_ingest_seq < 0:
            raise ValueError("signal_ingest_seq must be >= 0")
        if not self.reference_price_identity:
            raise ValueError("reference_price_identity is required")
        if self.reference_price_numerator_raw <= 0:
            raise ValueError("reference price numerator must be > 0")
        if self.reference_price_denominator_raw <= 0:
            raise ValueError("reference price denominator must be > 0")


@dataclass(frozen=True, slots=True)
class PaperOrder:
    paper_order_id: str
    signal_key: str
    mint: str
    track_id: str
    exit_variant: str
    strategy_version: str
    parameter_set_id: str
    signal_observed_at: datetime
    signal_ingest_seq: int
    reference_price_identity: str
    reference_price_numerator_raw: int
    reference_price_denominator_raw: int
    requested_size_lamports: int
    state: OrderState
    state_reason: str
    created_at: datetime
    updated_at: datetime
    fill_at: datetime | None
    filled_size_lamports: int | None
    simulated_entry_price_numerator_raw: int | None
    simulated_entry_price_denominator_raw: int | None


@dataclass(frozen=True, slots=True)
class PaperPosition:
    paper_position_id: str
    paper_order_id: str
    signal_key: str
    mint: str
    track_id: str
    exit_variant: str
    price_identity: str
    state: PositionState
    state_reason: str
    filled_size_lamports: int
    open_at: datetime
    updated_at: datetime
    entry_price_numerator_raw: int
    entry_price_denominator_raw: int
    exit_requested_at: datetime | None
    closed_at: datetime | None
    exit_price_numerator_raw: int | None
    exit_price_denominator_raw: int | None


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    event_id: str
    entity_type: str
    entity_id: str
    from_state: str | None
    to_state: str
    effective_at: datetime
    reason: str


class InvalidTransition(RuntimeError):
    pass


class DeterminismConflict(RuntimeError):
    pass


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")


def _utc(value: datetime) -> datetime:
    _require_aware(value)
    return value.astimezone(timezone.utc)


def _dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _dt_parse(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    return _utc(parsed)


def _hash_id(prefix: str, *parts: object) -> str:
    body = "\x1f".join(str(p) for p in parts)
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()[:32]
    return f"{prefix}-{digest}"


def deterministic_order_id(signal_key: str, track_id: str, exit_variant: str) -> str:
    return _hash_id("PO", SCHEMA_VERSION, signal_key, track_id, exit_variant)


def deterministic_position_id(paper_order_id: str) -> str:
    return _hash_id("PP", SCHEMA_VERSION, paper_order_id)


def deterministic_event_id(
    entity_type: str,
    entity_id: str,
    from_state: str | None,
    to_state: str,
    effective_at: datetime,
    reason: str,
) -> str:
    return _hash_id(
        "PE",
        SCHEMA_VERSION,
        entity_type,
        entity_id,
        from_state or "<NONE>",
        to_state,
        _dt_text(effective_at),
        reason,
    )


def connect_paper_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    create_schema(conn)
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS paper_schema_meta (
            schema_version TEXT PRIMARY KEY,
            engine_version TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS paper_orders (
            paper_order_id TEXT PRIMARY KEY,
            signal_key TEXT NOT NULL,
            mint TEXT NOT NULL,
            track_id TEXT NOT NULL,
            exit_variant TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            parameter_set_id TEXT NOT NULL,
            signal_observed_at TEXT NOT NULL,
            signal_ingest_seq INTEGER NOT NULL,
            reference_price_identity TEXT NOT NULL,
            reference_price_numerator_raw TEXT NOT NULL,
            reference_price_denominator_raw TEXT NOT NULL,
            requested_size_lamports INTEGER NOT NULL CHECK(requested_size_lamports > 0),
            state TEXT NOT NULL,
            state_reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            fill_at TEXT,
            filled_size_lamports INTEGER,
            simulated_entry_price_numerator_raw TEXT,
            simulated_entry_price_denominator_raw TEXT,
            UNIQUE(signal_key, track_id),
            CHECK(state IN ('CREATED','ENTRY_PENDING','FILLED','REJECTED','CANCELLED'))
        );

        CREATE TABLE IF NOT EXISTS paper_positions (
            paper_position_id TEXT PRIMARY KEY,
            paper_order_id TEXT NOT NULL UNIQUE,
            signal_key TEXT NOT NULL,
            mint TEXT NOT NULL,
            track_id TEXT NOT NULL,
            exit_variant TEXT NOT NULL,
            price_identity TEXT NOT NULL,
            state TEXT NOT NULL,
            state_reason TEXT NOT NULL,
            filled_size_lamports INTEGER NOT NULL CHECK(filled_size_lamports > 0),
            open_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            entry_price_numerator_raw TEXT NOT NULL,
            entry_price_denominator_raw TEXT NOT NULL,
            exit_requested_at TEXT,
            closed_at TEXT,
            exit_price_numerator_raw TEXT,
            exit_price_denominator_raw TEXT,
            FOREIGN KEY(paper_order_id) REFERENCES paper_orders(paper_order_id),
            CHECK(state IN ('OPEN','EXIT_PENDING','CLOSED'))
        );

        CREATE TABLE IF NOT EXISTS paper_lifecycle_events (
            event_id TEXT PRIMARY KEY,
            entity_type TEXT NOT NULL CHECK(entity_type IN ('ORDER','POSITION')),
            entity_id TEXT NOT NULL,
            from_state TEXT,
            to_state TEXT NOT NULL,
            effective_at TEXT NOT NULL,
            reason TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_paper_orders_signal
            ON paper_orders(signal_key, track_id);
        CREATE INDEX IF NOT EXISTS idx_paper_orders_state
            ON paper_orders(state);
        CREATE INDEX IF NOT EXISTS idx_paper_positions_state
            ON paper_positions(state);
        CREATE INDEX IF NOT EXISTS idx_paper_positions_mint
            ON paper_positions(mint, state);
        CREATE INDEX IF NOT EXISTS idx_paper_events_entity
            ON paper_lifecycle_events(entity_type, entity_id, effective_at, event_id);
        """
    )
    conn.execute(
        "INSERT OR IGNORE INTO paper_schema_meta(schema_version, engine_version) VALUES (?, ?)",
        (SCHEMA_VERSION, ENGINE_VERSION),
    )
    conn.commit()


class PaperLifecycleStore:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        create_schema(self.conn)

    def create_order(
        self,
        candidate: PaperCandidateRef,
        *,
        track_id: str,
        requested_size_lamports: int,
        reason: str = "CANDIDATE_ROUTED_TO_PAPER",
    ) -> PaperOrder:
        if track_id not in LOCKED_PHASE4_TRACKS:
            raise ValueError(f"unknown Phase-4 track_id: {track_id}")
        if requested_size_lamports <= 0:
            raise ValueError("requested_size_lamports must be > 0")
        if not reason:
            raise ValueError("reason is required")

        exit_variant = LOCKED_PHASE4_TRACKS[track_id]
        order_id = deterministic_order_id(candidate.signal_key, track_id, exit_variant)
        effective_at = _utc(candidate.signal_observed_at)
        existing = self.get_order(order_id)
        if existing is not None:
            self._assert_order_creation_match(
                existing, candidate, track_id, exit_variant, requested_size_lamports, reason
            )
            return existing

        with self.conn:
            try:
                self.conn.execute(
                    """
                    INSERT INTO paper_orders(
                        paper_order_id, signal_key, mint, track_id, exit_variant,
                        strategy_version, parameter_set_id,
                        signal_observed_at, signal_ingest_seq,
                        reference_price_identity,
                        reference_price_numerator_raw,
                        reference_price_denominator_raw,
                        requested_size_lamports,
                        state, state_reason, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        order_id,
                        candidate.signal_key,
                        candidate.mint,
                        track_id,
                        exit_variant,
                        candidate.strategy_version,
                        candidate.parameter_set_id,
                        _dt_text(candidate.signal_observed_at),
                        candidate.signal_ingest_seq,
                        candidate.reference_price_identity,
                        str(candidate.reference_price_numerator_raw),
                        str(candidate.reference_price_denominator_raw),
                        requested_size_lamports,
                        OrderState.CREATED.value,
                        reason,
                        _dt_text(effective_at),
                        _dt_text(effective_at),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                conflict = self.conn.execute(
                    "SELECT paper_order_id FROM paper_orders WHERE signal_key=? AND track_id=?",
                    (candidate.signal_key, track_id),
                ).fetchone()
                if conflict is not None:
                    raise DeterminismConflict(
                        "signal_key + track_id already exists with a different deterministic order identity"
                    ) from exc
                raise
            self._insert_event(
                entity_type="ORDER",
                entity_id=order_id,
                from_state=None,
                to_state=OrderState.CREATED.value,
                effective_at=effective_at,
                reason=reason,
            )
        created = self.get_order(order_id)
        assert created is not None
        return created

    def mark_entry_pending(
        self,
        paper_order_id: str,
        *,
        effective_at: datetime,
        reason: str = "PAPER_ENTRY_PENDING",
    ) -> PaperOrder:
        return self._transition_order(
            paper_order_id,
            OrderState.ENTRY_PENDING,
            effective_at=effective_at,
            reason=reason,
        )

    def reject_order(
        self,
        paper_order_id: str,
        *,
        effective_at: datetime,
        reason: str,
    ) -> PaperOrder:
        return self._transition_order(
            paper_order_id,
            OrderState.REJECTED,
            effective_at=effective_at,
            reason=reason,
        )

    def cancel_order(
        self,
        paper_order_id: str,
        *,
        effective_at: datetime,
        reason: str,
    ) -> PaperOrder:
        return self._transition_order(
            paper_order_id,
            OrderState.CANCELLED,
            effective_at=effective_at,
            reason=reason,
        )

    def fill_order(
        self,
        paper_order_id: str,
        *,
        fill_at: datetime,
        filled_size_lamports: int,
        simulated_entry_price_numerator_raw: int,
        simulated_entry_price_denominator_raw: int,
        reason: str = "PAPER_ENTRY_FILLED",
    ) -> tuple[PaperOrder, PaperPosition]:
        _require_aware(fill_at)
        if filled_size_lamports <= 0:
            raise ValueError("filled_size_lamports must be > 0")
        if simulated_entry_price_numerator_raw <= 0:
            raise ValueError("entry price numerator must be > 0")
        if simulated_entry_price_denominator_raw <= 0:
            raise ValueError("entry price denominator must be > 0")
        if not reason:
            raise ValueError("reason is required")

        order = self._require_order(paper_order_id)
        position_id = deterministic_position_id(paper_order_id)

        if order.state == OrderState.FILLED:
            position = self._require_position_by_order(paper_order_id)
            expected = (
                _dt_text(fill_at),
                filled_size_lamports,
                simulated_entry_price_numerator_raw,
                simulated_entry_price_denominator_raw,
            )
            actual = (
                _dt_text(order.fill_at) if order.fill_at else None,
                order.filled_size_lamports,
                order.simulated_entry_price_numerator_raw,
                order.simulated_entry_price_denominator_raw,
            )
            if actual != expected:
                raise DeterminismConflict("replayed fill differs from persisted fill")
            return order, position

        if OrderState.FILLED not in ORDER_TRANSITIONS[order.state]:
            raise InvalidTransition(f"order {order.state} -> FILLED is not allowed")
        if _utc(fill_at) < order.updated_at:
            raise InvalidTransition("fill_at precedes prior order transition")
        if filled_size_lamports > order.requested_size_lamports:
            raise ValueError("filled_size_lamports cannot exceed requested_size_lamports in v0.1")

        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_orders
                SET state=?, state_reason=?, updated_at=?, fill_at=?,
                    filled_size_lamports=?,
                    simulated_entry_price_numerator_raw=?,
                    simulated_entry_price_denominator_raw=?
                WHERE paper_order_id=?
                """,
                (
                    OrderState.FILLED.value,
                    reason,
                    _dt_text(fill_at),
                    _dt_text(fill_at),
                    filled_size_lamports,
                    str(simulated_entry_price_numerator_raw),
                    str(simulated_entry_price_denominator_raw),
                    paper_order_id,
                ),
            )
            self._insert_event(
                entity_type="ORDER",
                entity_id=paper_order_id,
                from_state=order.state.value,
                to_state=OrderState.FILLED.value,
                effective_at=fill_at,
                reason=reason,
            )
            self.conn.execute(
                """
                INSERT INTO paper_positions(
                    paper_position_id, paper_order_id, signal_key, mint,
                    track_id, exit_variant, price_identity, state,
                    state_reason, filled_size_lamports, open_at, updated_at,
                    entry_price_numerator_raw, entry_price_denominator_raw
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position_id,
                    paper_order_id,
                    order.signal_key,
                    order.mint,
                    order.track_id,
                    order.exit_variant,
                    order.reference_price_identity,
                    PositionState.OPEN.value,
                    reason,
                    filled_size_lamports,
                    _dt_text(fill_at),
                    _dt_text(fill_at),
                    str(simulated_entry_price_numerator_raw),
                    str(simulated_entry_price_denominator_raw),
                ),
            )
            self._insert_event(
                entity_type="POSITION",
                entity_id=position_id,
                from_state=None,
                to_state=PositionState.OPEN.value,
                effective_at=fill_at,
                reason=reason,
            )

        return self._require_order(paper_order_id), self._require_position(position_id)

    def request_exit(
        self,
        paper_position_id: str,
        *,
        effective_at: datetime,
        reason: str,
    ) -> PaperPosition:
        return self._transition_position(
            paper_position_id,
            PositionState.EXIT_PENDING,
            effective_at=effective_at,
            reason=reason,
        )

    def close_position(
        self,
        paper_position_id: str,
        *,
        closed_at: datetime,
        exit_price_numerator_raw: int,
        exit_price_denominator_raw: int,
        reason: str,
    ) -> PaperPosition:
        _require_aware(closed_at)
        if exit_price_numerator_raw <= 0:
            raise ValueError("exit price numerator must be > 0")
        if exit_price_denominator_raw <= 0:
            raise ValueError("exit price denominator must be > 0")
        if not reason:
            raise ValueError("reason is required")

        position = self._require_position(paper_position_id)
        if position.state == PositionState.CLOSED:
            expected = (
                _dt_text(closed_at),
                exit_price_numerator_raw,
                exit_price_denominator_raw,
                reason,
            )
            actual = (
                _dt_text(position.closed_at) if position.closed_at else None,
                position.exit_price_numerator_raw,
                position.exit_price_denominator_raw,
                position.state_reason,
            )
            if actual != expected:
                raise DeterminismConflict("replayed close differs from persisted close")
            return position
        if PositionState.CLOSED not in POSITION_TRANSITIONS[position.state]:
            raise InvalidTransition(f"position {position.state} -> CLOSED is not allowed")
        if _utc(closed_at) < position.updated_at:
            raise InvalidTransition("closed_at precedes prior position transition")

        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_positions
                SET state=?, state_reason=?, updated_at=?, closed_at=?,
                    exit_price_numerator_raw=?, exit_price_denominator_raw=?
                WHERE paper_position_id=?
                """,
                (
                    PositionState.CLOSED.value,
                    reason,
                    _dt_text(closed_at),
                    _dt_text(closed_at),
                    str(exit_price_numerator_raw),
                    str(exit_price_denominator_raw),
                    paper_position_id,
                ),
            )
            self._insert_event(
                entity_type="POSITION",
                entity_id=paper_position_id,
                from_state=position.state.value,
                to_state=PositionState.CLOSED.value,
                effective_at=closed_at,
                reason=reason,
            )
        return self._require_position(paper_position_id)

    def get_order(self, paper_order_id: str) -> PaperOrder | None:
        row = self.conn.execute(
            "SELECT * FROM paper_orders WHERE paper_order_id=?", (paper_order_id,)
        ).fetchone()
        return None if row is None else _order_from_row(row)

    def get_position(self, paper_position_id: str) -> PaperPosition | None:
        row = self.conn.execute(
            "SELECT * FROM paper_positions WHERE paper_position_id=?",
            (paper_position_id,),
        ).fetchone()
        return None if row is None else _position_from_row(row)

    def list_open_positions(self) -> list[PaperPosition]:
        rows = self.conn.execute(
            """
            SELECT * FROM paper_positions
            WHERE state IN ('OPEN','EXIT_PENDING')
            ORDER BY open_at, paper_position_id
            """
        ).fetchall()
        return [_position_from_row(row) for row in rows]

    def list_events(self, *, entity_type: str, entity_id: str) -> list[LifecycleEvent]:
        rows = self.conn.execute(
            """
            SELECT * FROM paper_lifecycle_events
            WHERE entity_type=? AND entity_id=?
            ORDER BY effective_at, event_id
            """,
            (entity_type, entity_id),
        ).fetchall()
        return [
            LifecycleEvent(
                event_id=row["event_id"],
                entity_type=row["entity_type"],
                entity_id=row["entity_id"],
                from_state=row["from_state"],
                to_state=row["to_state"],
                effective_at=_dt_parse(row["effective_at"]),  # type: ignore[arg-type]
                reason=row["reason"],
            )
            for row in rows
        ]

    def _transition_order(
        self,
        paper_order_id: str,
        target: OrderState,
        *,
        effective_at: datetime,
        reason: str,
    ) -> PaperOrder:
        _require_aware(effective_at)
        if not reason:
            raise ValueError("reason is required")
        order = self._require_order(paper_order_id)
        if order.state == target:
            self._assert_existing_event(
                "ORDER", paper_order_id, None, target.value, effective_at, reason
            )
            return order
        if target not in ORDER_TRANSITIONS[order.state]:
            raise InvalidTransition(f"order {order.state} -> {target} is not allowed")
        if _utc(effective_at) < order.updated_at:
            raise InvalidTransition("effective_at precedes prior order transition")
        with self.conn:
            self.conn.execute(
                "UPDATE paper_orders SET state=?, state_reason=?, updated_at=? WHERE paper_order_id=?",
                (target.value, reason, _dt_text(effective_at), paper_order_id),
            )
            self._insert_event(
                entity_type="ORDER",
                entity_id=paper_order_id,
                from_state=order.state.value,
                to_state=target.value,
                effective_at=effective_at,
                reason=reason,
            )
        return self._require_order(paper_order_id)

    def _transition_position(
        self,
        paper_position_id: str,
        target: PositionState,
        *,
        effective_at: datetime,
        reason: str,
    ) -> PaperPosition:
        _require_aware(effective_at)
        if not reason:
            raise ValueError("reason is required")
        position = self._require_position(paper_position_id)
        if position.state == target:
            self._assert_existing_event(
                "POSITION", paper_position_id, None, target.value, effective_at, reason
            )
            return position
        if target not in POSITION_TRANSITIONS[position.state]:
            raise InvalidTransition(
                f"position {position.state} -> {target} is not allowed"
            )
        if _utc(effective_at) < position.updated_at:
            raise InvalidTransition("effective_at precedes prior position transition")
        with self.conn:
            fields = "state=?, state_reason=?, updated_at=?"
            values: list[object] = [target.value, reason, _dt_text(effective_at)]
            if target == PositionState.EXIT_PENDING:
                fields += ", exit_requested_at=?"
                values.append(_dt_text(effective_at))
            values.append(paper_position_id)
            self.conn.execute(
                f"UPDATE paper_positions SET {fields} WHERE paper_position_id=?",
                values,
            )
            self._insert_event(
                entity_type="POSITION",
                entity_id=paper_position_id,
                from_state=position.state.value,
                to_state=target.value,
                effective_at=effective_at,
                reason=reason,
            )
        return self._require_position(paper_position_id)

    def _insert_event(
        self,
        *,
        entity_type: str,
        entity_id: str,
        from_state: str | None,
        to_state: str,
        effective_at: datetime,
        reason: str,
    ) -> str:
        event_id = deterministic_event_id(
            entity_type, entity_id, from_state, to_state, effective_at, reason
        )
        self.conn.execute(
            """
            INSERT OR IGNORE INTO paper_lifecycle_events(
                event_id, entity_type, entity_id, from_state, to_state, effective_at, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                entity_type,
                entity_id,
                from_state,
                to_state,
                _dt_text(effective_at),
                reason,
            ),
        )
        return event_id

    def _assert_existing_event(
        self,
        entity_type: str,
        entity_id: str,
        from_state: str | None,
        to_state: str,
        effective_at: datetime,
        reason: str,
    ) -> None:
        # Idempotent retry is accepted when an event with the same target,
        # timestamp and reason already exists, regardless of the caller no
        # longer knowing the original from_state after restart.
        row = self.conn.execute(
            """
            SELECT 1 FROM paper_lifecycle_events
            WHERE entity_type=? AND entity_id=? AND to_state=?
              AND effective_at=? AND reason=?
            LIMIT 1
            """,
            (entity_type, entity_id, to_state, _dt_text(effective_at), reason),
        ).fetchone()
        if row is None:
            raise DeterminismConflict(
                f"entity already in {to_state}, but replay transition does not match persisted event"
            )

    def _assert_order_creation_match(
        self,
        order: PaperOrder,
        candidate: PaperCandidateRef,
        track_id: str,
        exit_variant: str,
        requested_size_lamports: int,
        reason: str,
    ) -> None:
        expected = (
            candidate.signal_key,
            candidate.mint,
            track_id,
            exit_variant,
            candidate.strategy_version,
            candidate.parameter_set_id,
            _dt_text(candidate.signal_observed_at),
            candidate.signal_ingest_seq,
            candidate.reference_price_identity,
            candidate.reference_price_numerator_raw,
            candidate.reference_price_denominator_raw,
            requested_size_lamports,
        )
        actual = (
            order.signal_key,
            order.mint,
            order.track_id,
            order.exit_variant,
            order.strategy_version,
            order.parameter_set_id,
            _dt_text(order.signal_observed_at),
            order.signal_ingest_seq,
            order.reference_price_identity,
            order.reference_price_numerator_raw,
            order.reference_price_denominator_raw,
            order.requested_size_lamports,
        )
        if expected != actual:
            raise DeterminismConflict("replayed order creation differs from persisted order")
        creation = self.conn.execute(
            """
            SELECT 1 FROM paper_lifecycle_events
            WHERE entity_type='ORDER' AND entity_id=? AND from_state IS NULL
              AND to_state='CREATED' AND effective_at=? AND reason=?
            """,
            (order.paper_order_id, _dt_text(candidate.signal_observed_at), reason),
        ).fetchone()
        if creation is None:
            raise DeterminismConflict("persisted order is missing matching creation event")

    def _require_order(self, paper_order_id: str) -> PaperOrder:
        order = self.get_order(paper_order_id)
        if order is None:
            raise KeyError(f"paper order not found: {paper_order_id}")
        return order

    def _require_position(self, paper_position_id: str) -> PaperPosition:
        position = self.get_position(paper_position_id)
        if position is None:
            raise KeyError(f"paper position not found: {paper_position_id}")
        return position

    def _require_position_by_order(self, paper_order_id: str) -> PaperPosition:
        row = self.conn.execute(
            "SELECT * FROM paper_positions WHERE paper_order_id=?", (paper_order_id,)
        ).fetchone()
        if row is None:
            raise DeterminismConflict("FILLED order is missing its paper position")
        return _position_from_row(row)


def _order_from_row(row: sqlite3.Row) -> PaperOrder:
    return PaperOrder(
        paper_order_id=row["paper_order_id"],
        signal_key=row["signal_key"],
        mint=row["mint"],
        track_id=row["track_id"],
        exit_variant=row["exit_variant"],
        strategy_version=row["strategy_version"],
        parameter_set_id=row["parameter_set_id"],
        signal_observed_at=_dt_parse(row["signal_observed_at"]),  # type: ignore[arg-type]
        signal_ingest_seq=int(row["signal_ingest_seq"]),
        reference_price_identity=row["reference_price_identity"],
        reference_price_numerator_raw=int(row["reference_price_numerator_raw"]),
        reference_price_denominator_raw=int(row["reference_price_denominator_raw"]),
        requested_size_lamports=int(row["requested_size_lamports"]),
        state=OrderState(row["state"]),
        state_reason=row["state_reason"],
        created_at=_dt_parse(row["created_at"]),  # type: ignore[arg-type]
        updated_at=_dt_parse(row["updated_at"]),  # type: ignore[arg-type]
        fill_at=_dt_parse(row["fill_at"]),
        filled_size_lamports=None
        if row["filled_size_lamports"] is None
        else int(row["filled_size_lamports"]),
        simulated_entry_price_numerator_raw=None
        if row["simulated_entry_price_numerator_raw"] is None
        else int(row["simulated_entry_price_numerator_raw"]),
        simulated_entry_price_denominator_raw=None
        if row["simulated_entry_price_denominator_raw"] is None
        else int(row["simulated_entry_price_denominator_raw"]),
    )


def _position_from_row(row: sqlite3.Row) -> PaperPosition:
    return PaperPosition(
        paper_position_id=row["paper_position_id"],
        paper_order_id=row["paper_order_id"],
        signal_key=row["signal_key"],
        mint=row["mint"],
        track_id=row["track_id"],
        exit_variant=row["exit_variant"],
        price_identity=row["price_identity"],
        state=PositionState(row["state"]),
        state_reason=row["state_reason"],
        filled_size_lamports=int(row["filled_size_lamports"]),
        open_at=_dt_parse(row["open_at"]),  # type: ignore[arg-type]
        updated_at=_dt_parse(row["updated_at"]),  # type: ignore[arg-type]
        entry_price_numerator_raw=int(row["entry_price_numerator_raw"]),
        entry_price_denominator_raw=int(row["entry_price_denominator_raw"]),
        exit_requested_at=_dt_parse(row["exit_requested_at"]),
        closed_at=_dt_parse(row["closed_at"]),
        exit_price_numerator_raw=None
        if row["exit_price_numerator_raw"] is None
        else int(row["exit_price_numerator_raw"]),
        exit_price_denominator_raw=None
        if row["exit_price_denominator_raw"] is None
        else int(row["exit_price_denominator_raw"]),
    )


def canonical_db_digest(conn: sqlite3.Connection) -> str:
    """Hash logical rows, excluding SQLite page/layout details."""
    chunks: list[str] = []
    for table in (
        "paper_schema_meta",
        "paper_orders",
        "paper_positions",
        "paper_lifecycle_events",
    ):
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
        for row in rows:
            chunks.append(table + "\x1e" + "\x1f".join("<NULL>" if v is None else str(v) for v in row))
    return hashlib.sha256("\x1d".join(chunks).encode("utf-8")).hexdigest()


def run_integrity_checks(conn: sqlite3.Connection) -> tuple[bool, list[str]]:
    issues: list[str] = []
    quick = conn.execute("PRAGMA quick_check").fetchone()[0]
    if quick != "ok":
        issues.append(f"quick_check={quick}")
    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk:
        issues.append(f"foreign_key_check rows={len(fk)}")
    return not issues, issues
