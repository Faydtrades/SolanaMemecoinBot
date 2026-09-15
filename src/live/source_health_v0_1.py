"""G001 receipt-bounded collector evidence under an explicit trust profile.

This proves an observed operational prefix, never chain-wide completeness or
producer checkpoint completeness. Collector input is read-only. Evidence has no
position, reservation, obligation, signer or execution capability.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import asdict, dataclass, fields, is_dataclass, replace
from datetime import datetime, timezone
from pathlib import Path


SCHEMA = "live_source_evidence_v0.1"
MODEL_ID = "LIVE-SOURCE-HEALTH-0001"
P4_MODEL_ID = "P4-CONTINUOUS-MARKET-SOURCE-0002"
P4_FINGERPRINT = "9cb094f52bf4b4fe28dc4828b1d9a52a3350cde664c7da5a489fa84e9a4085a9"
PROFILE_ID = "COLLECTOR-V034-RECEIPT-BOUNDED-PREFIX-0001"
TABLES = ("pump_events", "collector_events", "websocket_observations", "gap_jobs_v034")
ZERO_DIGEST = "0" * 64
SOURCE_MARKERS = ("LIVE_WEBSOCKET_EVENT_V0_3_4", "GAP_RECONCILIATION_V0_3_4")
INTEGRITY_REASONS = frozenset({
    "SOURCE_WITNESS_CHANGED", "SOURCE_WITNESS_MISSING", "SOURCE_CURSOR_REGRESSION",
    "SOURCE_IDENTITY_CHANGED", "SOURCE_PROFILE_CHANGED", "PREVIOUS_VERDICT_MISMATCH",
    "SOURCE_ROW_ORDER_INVALID", "SOURCE_CURSOR_WITNESS_MISMATCH",
})


def canonical_json(value: object) -> str:
    # Source records contain immutable dataclasses, tuples and JSON scalars.
    # Let the JSON encoder visit them directly rather than deepcopy every scalar
    # of every historical gap through asdict on each digest calculation.
    def record(item):
        if is_dataclass(item) and not isinstance(item, type):
            return {field.name:getattr(item, field.name) for field in fields(item)}
        raise TypeError("Object of type "+type(item).__name__+" is not JSON serializable")
    return json.dumps(value, default=record if is_dataclass(value) else None,
                      sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False)


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def utc(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("UTC text required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("explicit UTC offset required")
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _seconds(later: str, earlier: str) -> float:
    return (datetime.fromisoformat(later) - datetime.fromisoformat(earlier)).total_seconds()


def _token(value: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9_:.-]{1,256}", value) is None:
        raise ValueError("invalid public evidence identifier")


def _hash(value: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("SHA256 required")


def _integer(value: int, minimum: int = 0) -> None:
    if type(value) is not int or value < minimum:
        raise ValueError("invalid integer evidence field")


def _tuple(value: tuple, member: type) -> None:
    if type(value) is not tuple or any(type(item) is not member for item in value):
        raise ValueError("immutable typed tuple required")


@dataclass(frozen=True, slots=True)
class CursorWitness:
    table: str
    rowid: int
    sha256: str

    def __post_init__(self) -> None:
        if self.table not in TABLES:
            raise ValueError("unsupported source table")
        _integer(self.rowid)
        _hash(self.sha256)
        if (self.rowid == 0) != (self.sha256 == ZERO_DIGEST):
            raise ValueError("zero cursor must have zero digest")


@dataclass(frozen=True, slots=True)
class SourceBinding:
    lineage: str
    coverage_start_utc: str
    anchors: tuple[CursorWitness, ...]
    source_model_id: str = P4_MODEL_ID
    source_model_fingerprint: str = P4_FINGERPRINT

    def __post_init__(self) -> None:
        _token(self.lineage)
        object.__setattr__(self, "coverage_start_utc", utc(self.coverage_start_utc))
        _tuple(self.anchors, CursorWitness)
        if tuple(w.table for w in self.anchors) != TABLES[:3] or any(w.rowid == 0 for w in self.anchors):
            raise ValueError("explicit pump, collector-start and receipt anchors required")
        if (self.source_model_id, self.source_model_fingerprint) != (P4_MODEL_ID, P4_FINGERPRINT):
            raise ValueError("accepted P4 source contract required")

    @property
    def source_identity(self) -> str:
        return digest(self)


@dataclass(frozen=True, slots=True)
class SourceProfile:
    max_rows_per_table: int = 10000
    freshness_seconds: int = 30
    profile_id: str = PROFILE_ID

    def __post_init__(self) -> None:
        _integer(self.max_rows_per_table, 1)
        _integer(self.freshness_seconds, 1)
        if self.max_rows_per_table > 100000 or self.freshness_seconds > 3600:
            raise ValueError("source profile exceeds bounded supported limits")
        if self.profile_id != PROFILE_ID:
            raise ValueError("unsupported source trust profile")

    @property
    def fingerprint(self) -> str:
        return digest(self)


@dataclass(frozen=True, slots=True)
class PumpFact:
    rowid: int
    event_key: str
    signature: str
    slot: int | None
    source: str
    inserted_at_utc: str

    def __post_init__(self) -> None:
        _integer(self.rowid, 1)
        for field in (self.event_key, self.signature, self.source):
            _token(field)
        if self.source not in SOURCE_MARKERS:
            raise ValueError("unsupported collector source marker")
        if self.slot is not None:
            _integer(self.slot)
        object.__setattr__(self, "inserted_at_utc", utc(self.inserted_at_utc))


@dataclass(frozen=True, slots=True)
class ControlFact:
    rowid: int
    event_type: str
    at_utc: str
    gap_start_utc: str = ""
    gap_end_utc: str = ""

    def __post_init__(self) -> None:
        _integer(self.rowid, 1)
        _token(self.event_type)
        object.__setattr__(self, "at_utc", utc(self.at_utc))
        for field in ("gap_start_utc", "gap_end_utc"):
            if getattr(self, field):
                object.__setattr__(self, field, utc(getattr(self, field)))
        if self.event_type == "DATA_GAP_CLOSED":
            if not self.gap_start_utc or not self.gap_end_utc or self.gap_start_utc > self.gap_end_utc:
                raise ValueError("explicit gap control lacks valid interval")


@dataclass(frozen=True, slots=True)
class ReceiptFact:
    rowid: int
    signature: str
    slot: int | None
    at_utc: str
    executed: int

    def __post_init__(self) -> None:
        _integer(self.rowid, 1)
        _token(self.signature)
        if self.slot is not None:
            _integer(self.slot)
        if type(self.executed) is not int or self.executed not in (0, 1):
            raise ValueError("invalid receipt execution fact")
        object.__setattr__(self, "at_utc", utc(self.at_utc))


@dataclass(frozen=True, slots=True)
class GapFact:
    rowid: int
    gap_key: str
    start_utc: str
    end_utc: str
    status: str
    before_slot: int | None
    after_slot: int | None
    confirmed_blocks_checked: int
    events_recovered: int

    def __post_init__(self) -> None:
        _integer(self.rowid, 1)
        # Collector gap keys contain public signatures and an offset timestamp.
        if not isinstance(self.gap_key, str) or len(self.gap_key) > 256:
            raise ValueError("invalid gap key")
        _token(self.status)
        for field in ("start_utc", "end_utc"):
            object.__setattr__(self, field, utc(getattr(self, field)))
        if self.start_utc > self.end_utc:
            raise ValueError("reversed gap interval")
        for value in (self.before_slot, self.after_slot):
            if value is not None:
                _integer(value)
        _integer(self.confirmed_blocks_checked)
        _integer(self.events_recovered)


def witness(table: str, fact: PumpFact | ControlFact | ReceiptFact | GapFact) -> CursorWitness:
    if table == "gap_jobs_v034":
        # Job status/counters are mutable; the original gap identity is not.
        value = (fact.rowid, fact.gap_key, fact.start_utc, fact.end_utc,
                 fact.before_slot, fact.after_slot)
    else:
        value = fact
    return CursorWitness(table, fact.rowid, digest(value))


@dataclass(frozen=True, slots=True)
class BoundaryFact:
    at_utc: str
    reason: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "at_utc", utc(self.at_utc))
        _token(self.reason)


@dataclass(frozen=True, slots=True)
class SourceProgress:
    cursors: tuple[CursorWitness, ...] = ()
    connection_state: str = "UNOBSERVED"
    collector_start_utc: str = ""
    active_since_utc: str = ""
    last_control_utc: str = ""
    last_live_receipt_utc: str = ""
    maximum_pump_activity_utc: str = ""
    boundaries: tuple[BoundaryFact, ...] = ()
    gaps: tuple[GapFact, ...] = ()
    control_gaps: tuple[ControlFact, ...] = ()
    integrity_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field, member in ((self.cursors, CursorWitness), (self.boundaries, BoundaryFact),
                              (self.gaps, GapFact), (self.control_gaps, ControlFact),
                              (self.integrity_reasons, str)):
            _tuple(field, member)
        if any(reason not in INTEGRITY_REASONS for reason in self.integrity_reasons):
            raise ValueError("unsupported source integrity reason")
        if self.cursors and tuple(w.table for w in self.cursors) != TABLES:
            raise ValueError("complete ordered source cursors required")
        if self.connection_state not in ("UNOBSERVED", "STARTED", "CONNECTED", "ACTIVE", "LOST", "STOPPED"):
            raise ValueError("invalid connection state")
        for field in ("collector_start_utc", "active_since_utc", "last_control_utc",
                      "last_live_receipt_utc", "maximum_pump_activity_utc"):
            if getattr(self, field):
                object.__setattr__(self, field, utc(getattr(self, field)))


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    source_identity: str
    profile_fingerprint: str
    observed_at_utc: str
    requested_cut_utc: str
    previous_verdict_digest: str
    pump_rows: tuple[PumpFact, ...] = ()
    controls: tuple[ControlFact, ...] = ()
    receipts: tuple[ReceiptFact, ...] = ()
    gaps: tuple[GapFact, ...] = ()
    cursors: tuple[CursorWitness, ...] = ()
    problems: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for value in (self.source_identity, self.profile_fingerprint, self.previous_verdict_digest):
            _hash(value)
        for field in ("observed_at_utc", "requested_cut_utc"):
            object.__setattr__(self, field, utc(getattr(self, field)))
        for field, member in ((self.pump_rows, PumpFact), (self.controls, ControlFact),
                              (self.receipts, ReceiptFact), (self.gaps, GapFact),
                              (self.cursors, CursorWitness), (self.problems, str)):
            _tuple(field, member)
        for reason in self.problems:
            _token(reason)


@dataclass(frozen=True, slots=True)
class SourceVerdict:
    binding: SourceBinding
    profile: SourceProfile
    snapshot: SourceSnapshot
    progress: SourceProgress
    disposition: str
    reasons: tuple[str, ...]
    covered_from_utc: str = ""
    covered_through_utc: str = ""
    schema: str = SCHEMA
    producer: str = MODEL_ID

    def __post_init__(self) -> None:
        for value, cls in ((self.binding, SourceBinding), (self.profile, SourceProfile),
                           (self.snapshot, SourceSnapshot), (self.progress, SourceProgress)):
            if type(value) is not cls:
                raise ValueError("immutable source evidence required")
        _tuple(self.reasons, str)
        for reason in self.reasons:
            _token(reason)
        if self.disposition not in ("HEALTHY", "UNKNOWN", "GAP", "STALE", "LOST", "REGRESSION"):
            raise ValueError("invalid source disposition")
        if self.schema != SCHEMA or self.producer != MODEL_ID:
            raise ValueError("unsupported source evidence schema/producer")
        if self.snapshot.source_identity != self.binding.source_identity or self.snapshot.profile_fingerprint != self.profile.fingerprint:
            raise ValueError("source verdict domain mismatch")
        for field in ("covered_from_utc", "covered_through_utc"):
            if getattr(self, field):
                object.__setattr__(self, field, utc(getattr(self, field)))
        if self.disposition == "HEALTHY" and (self.reasons or not self.covered_from_utc or not self.covered_through_utc):
            raise ValueError("healthy evidence requires explicit coverage")
        if self.disposition == "HEALTHY" and not _healthy_consistent(self):
            raise ValueError("healthy evidence conflicts with observed coverage")

    @property
    def content_digest(self) -> str:
        return digest(self)


_SELECT = {
    "pump_events": "rowid,event_key,signature,slot,source_decoded_file,inserted_at_utc",
    "collector_events": "id,event_type,happened_at_utc,details_json",
    "websocket_observations": "rowid,signature,slot,received_at_utc,executed",
    "gap_jobs_v034": "rowid,gap_key,gap_started_at_utc,gap_ended_at_utc,status,before_slot,after_slot,confirmed_blocks_checked,events_recovered",
}


def _fact(table: str, row: tuple) -> PumpFact | ControlFact | ReceiptFact | GapFact:
    if table == "pump_events":
        if row[4] not in SOURCE_MARKERS:
            raise SourceReadError("SOURCE_MARKER_UNSUPPORTED")
        return PumpFact(*row)
    if table == "websocket_observations":
        return ReceiptFact(*row)
    if table == "gap_jobs_v034":
        # Do not retain a free-text key; retain its content identity instead.
        return GapFact(row[0], hashlib.sha256(str(row[1]).encode("utf-8")).hexdigest(), *row[2:])
    rowid, event_type, at, details = row
    if event_type != "DATA_GAP_CLOSED":
        return ControlFact(rowid, event_type, at)
    parsed = json.loads(details)
    if type(parsed) is not dict:
        raise ValueError("invalid gap details")
    return ControlFact(rowid, event_type, at, parsed.get("gap_started_at_utc", ""),
                       parsed.get("gap_ended_at_utc", ""))


class SourceReadError(ValueError):
    """Sanitized failure code; never includes SQL, paths or source bodies."""


class CollectorSourceAdapter:
    """One SQLite snapshot, fixed allowlisted columns, bounded incremental rows.

    Bindings are provisioned from reviewed anchors, never inferred from file
    metadata or automatically replaced after loss. The initial control anchor
    must be a collector start; subsequent calls verify anchors and every saved
    cursor before reading a tail. Every gap is inspected from rowid zero on the
    initial call. Growing source files do not change identity.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path).resolve()

    def _open(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path.as_uri() + "?mode=ro", uri=True, timeout=1.0)
        conn.execute("PRAGMA query_only=ON")
        if conn.execute("PRAGMA query_only").fetchone()[0] != 1:
            conn.close()
            raise SourceReadError("READ_ONLY_UNAVAILABLE")
        return conn

    def capture(self, binding: SourceBinding, profile: SourceProfile, *,
                observed_at_utc: str, requested_cut_utc: str,
                previous: SourceVerdict | None = None, max_raw_bytes: int | None = None,
                reserve_rows=None) -> SourceSnapshot:
        base = SourceSnapshot(binding.source_identity, profile.fingerprint,
                              observed_at_utc, requested_cut_utc,
                              ZERO_DIGEST if previous is None else previous.content_digest)
        if previous is not None and previous.binding != binding:
            return replace(base, problems=("SOURCE_IDENTITY_CHANGED",))
        if previous is not None and previous.profile != profile:
            return replace(base, problems=("SOURCE_PROFILE_CHANGED",))
        prior_cursors = () if previous is None else previous.progress.cursors
        conn = None
        try:
            conn = self._open()
            conn.execute("BEGIN")
            # Validate the source schema even when a table currently has no rows.
            for table in TABLES:
                conn.execute(f"SELECT {_SELECT[table]} FROM {table} LIMIT 0")
            if max_raw_bytes is not None or reserve_rows is not None:
                if type(max_raw_bytes) is not int or max_raw_bytes < 1 or not callable(reserve_rows):
                    raise SourceReadError("SOURCE_CAPTURE_RESOURCE_CONTRACT_INVALID")
                # Inspect only lengths/counts in this same read snapshot before
                # materializing any raw public field. Both repeated witnesses
                # and all four complete tails contribute to the capture bound.
                raw_bytes = 0
                counts = {}
                def sizes(table, clause, params):
                    terms = "+".join("COALESCE(length(CAST("+column+" AS BLOB)),0)"
                        for column in _SELECT[table].split(","))
                    return conn.execute("SELECT COUNT(*),COALESCE(SUM(n),0) FROM (SELECT "+terms+
                        " AS n FROM "+table+" "+clause+")", params).fetchone()
                for expected in (*binding.anchors, *prior_cursors):
                    if expected.rowid:
                        _, size = sizes(expected.table, "WHERE rowid=?", (expected.rowid,))
                        raw_bytes += size
                for index, table in enumerate(TABLES):
                    lower = prior_cursors[index].rowid if prior_cursors else (
                        binding.anchors[index].rowid-1 if index < 3 else 0)
                    count, size = sizes(table, "WHERE rowid>? ORDER BY rowid LIMIT ?",
                        (lower, profile.max_rows_per_table+1))
                    if count > profile.max_rows_per_table:
                        raise SourceReadError("SOURCE_PAGE_BUDGET_EXHAUSTED")
                    counts[table] = count
                    raw_bytes += size
                if raw_bytes > max_raw_bytes:
                    raise SourceReadError("SOURCE_CAPTURE_RAW_BYTE_BUDGET_EXHAUSTED")
                if not reserve_rows(counts):
                    raise SourceReadError("SOURCE_CAPTURE_CUMULATIVE_ROW_BUDGET_EXHAUSTED")
            for expected in prior_cursors:
                maximum = conn.execute(f"SELECT COALESCE(MAX(rowid),0) FROM {expected.table}").fetchone()[0]
                if maximum < expected.rowid:
                    raise SourceReadError("SOURCE_CURSOR_REGRESSION")
            for expected in (*binding.anchors, *prior_cursors):
                if expected.rowid == 0:
                    continue
                row = conn.execute(f"SELECT {_SELECT[expected.table]} FROM {expected.table} WHERE rowid=?",
                                   (expected.rowid,)).fetchone()
                if row is None:
                    raise SourceReadError("SOURCE_WITNESS_MISSING")
                fact = _fact(expected.table, row)
                if witness(expected.table, fact) != expected:
                    raise SourceReadError("SOURCE_WITNESS_CHANGED")
            anchor_control = conn.execute(
                "SELECT event_type FROM collector_events WHERE id=?", (binding.anchors[1].rowid,)).fetchone()
            if anchor_control[0] != "COLLECTOR_V0_3_START":
                raise SourceReadError("COLLECTOR_START_ANCHOR_REQUIRED")
            facts = []
            cursors = []
            for index, table in enumerate(TABLES):
                previous_cursor = prior_cursors[index] if prior_cursors else None
                # Include initial anchors as evidence; all later fetches are exclusive.
                lower = previous_cursor.rowid if previous_cursor else (
                    binding.anchors[index].rowid - 1 if index < 3 else 0)
                maximum = conn.execute(f"SELECT COALESCE(MAX(rowid),0) FROM {table}").fetchone()[0]
                if maximum < lower:
                    raise SourceReadError("SOURCE_CURSOR_REGRESSION")
                rows = conn.execute(f"SELECT {_SELECT[table]} FROM {table} WHERE rowid>? ORDER BY rowid LIMIT ?",
                                    (lower, profile.max_rows_per_table + 1)).fetchall()
                if len(rows) > profile.max_rows_per_table:
                    raise SourceReadError("SOURCE_PAGE_BUDGET_EXHAUSTED")
                parsed = tuple(_fact(table, row) for row in rows)
                if maximum > lower and (not parsed or parsed[-1].rowid != maximum):
                    raise SourceReadError("SOURCE_TAIL_INCOMPLETE")
                facts.append(parsed)
                cursors.append(witness(table, parsed[-1]) if parsed else (
                    previous_cursor or CursorWitness(table, 0, ZERO_DIGEST)))
            return replace(base, pump_rows=facts[0], controls=facts[1], receipts=facts[2],
                           gaps=facts[3], cursors=tuple(cursors))
        except SourceReadError as exc:
            return replace(base, problems=(str(exc),))
        except sqlite3.OperationalError:
            return replace(base, problems=("SOURCE_UNAVAILABLE_OR_SCHEMA_MISSING",))
        except (sqlite3.DatabaseError, OSError):
            return replace(base, problems=("SOURCE_READ_FAILED",))
        except (ValueError, TypeError, OverflowError):
            return replace(base, problems=("SOURCE_FACT_INVALID",))
        finally:
            if conn is not None:
                conn.close()

    def observe(self, binding: SourceBinding, profile: SourceProfile, *,
                observed_at_utc: str, requested_cut_utc: str,
                previous: SourceVerdict | None = None, max_raw_bytes: int | None = None,
                reserve_rows=None) -> SourceVerdict:
        snapshot = self.capture(binding, profile, observed_at_utc=observed_at_utc,
                                requested_cut_utc=requested_cut_utc, previous=previous,
                                max_raw_bytes=max_raw_bytes, reserve_rows=reserve_rows)
        return evaluate_source(binding, profile, snapshot, previous=previous)


def _advance(progress: SourceProgress, snapshot: SourceSnapshot) -> SourceProgress:
    state = progress.connection_state
    started, active, last_control = progress.collector_start_utc, progress.active_since_utc, progress.last_control_utc
    receipt = progress.last_live_receipt_utc
    boundaries = list(progress.boundaries)
    control_gaps = list(progress.control_gaps)
    for control in snapshot.controls:
        at, kind = control.at_utc, control.event_type
        if last_control and at < last_control:
            raise SourceReadError("CONTROL_CLOCK_REGRESSION")
        last_control = at
        if kind == "COLLECTOR_V0_3_START":
            if started:
                boundaries.append(BoundaryFact(at, "UNCERTAIN_SESSION_BOUNDARY"))
            started, active, receipt, state = at, "", "", "STARTED"
        elif kind == "WS_CONNECTED":
            if state not in ("STARTED", "LOST"):
                boundaries.append(BoundaryFact(at, "UNCERTAIN_CONNECTION_BOUNDARY"))
                state = "LOST"
            else:
                state = "CONNECTED"
        elif kind == "PUMP_SUBSCRIPTION_ACTIVE":
            if not started or state != "CONNECTED":
                boundaries.append(BoundaryFact(at, "UNCERTAIN_SUBSCRIPTION_BOUNDARY"))
                active, receipt, state = "", "", "LOST"
            else:
                active, receipt, state = at, "", "ACTIVE"
        elif kind in ("WS_STALE", "WS_DISCONNECTED", "LIVE_COLLECTION_STOPPED", "TEST_TARGET_REACHED"):
            boundaries.append(BoundaryFact(at, kind))
            state = "STOPPED" if kind in ("LIVE_COLLECTION_STOPPED", "TEST_TARGET_REACHED") else "LOST"
        elif kind == "DATA_GAP_CLOSED":
            control_gaps.append(control)
        elif kind != "GAP_RECONCILED":
            boundaries.append(BoundaryFact(at, "DECODE_ERROR" if kind == "DECODE_ERROR" else "UNKNOWN_CONTROL_EVENT"))
    if state == "ACTIVE":
        for item in snapshot.receipts:
            if item.at_utc >= active:
                receipt = max(receipt, item.at_utc)
    activity = progress.maximum_pump_activity_utc
    for item in snapshot.pump_rows:
        # Accepted V05: rowid orders processing; inserted_at may decrease.
        activity = max(activity, item.inserted_at_utc)
    return SourceProgress(snapshot.cursors, state, started, active, last_control,
                          receipt, activity, tuple(boundaries), progress.gaps + snapshot.gaps,
                          tuple(control_gaps), progress.integrity_reasons)


def evaluate_source(binding: SourceBinding, profile: SourceProfile, snapshot: SourceSnapshot, *,
                    previous: SourceVerdict | None = None) -> SourceVerdict:
    return _evaluate_source(binding, profile, snapshot, previous=previous,
        previous_digest=ZERO_DIGEST if previous is None else previous.content_digest)


def _evaluate_source(binding: SourceBinding, profile: SourceProfile, snapshot: SourceSnapshot, *,
                     previous: SourceVerdict | None, previous_digest: str) -> SourceVerdict:
    # Internal replay seam: the original store computed and checked this exact
    # immutable predecessor's digest on its immediately preceding row. Public
    # callers continue through evaluate_source and compute it from the object.
    progress = SourceProgress() if previous is None else previous.progress
    reasons = list(snapshot.problems) + list(progress.integrity_reasons)
    expected_previous = previous_digest
    if snapshot.previous_verdict_digest != expected_previous:
        reasons.append("PREVIOUS_VERDICT_MISMATCH")
    if snapshot.source_identity != binding.source_identity or (previous is not None and previous.binding != binding):
        reasons.append("SOURCE_IDENTITY_CHANGED")
    if snapshot.profile_fingerprint != profile.fingerprint or (previous is not None and previous.profile != profile):
        reasons.append("SOURCE_PROFILE_CHANGED")
    now, cut, origin = snapshot.observed_at_utc, snapshot.requested_cut_utc, binding.coverage_start_utc
    if now < cut or cut < origin:
        reasons.append("INVALID_REQUESTED_CUT")
    if previous is not None and (now < previous.snapshot.observed_at_utc or cut < previous.snapshot.requested_cut_utc):
        reasons.append("EVIDENCE_CLOCK_OR_CUT_REGRESSION")
    timestamps = ([row.inserted_at_utc for row in snapshot.pump_rows]
                  + [row.at_utc for row in (*snapshot.controls, *snapshot.receipts)]
                  + [row.end_utc for row in snapshot.gaps]
                  + [row.gap_end_utc for row in snapshot.controls if row.gap_end_utc])
    if any(at > now for at in timestamps):
        reasons.append("FUTURE_SOURCE_UTC")
    if not reasons:
        if tuple(w.table for w in snapshot.cursors) != TABLES:
            reasons.append("SOURCE_CURSORS_INCOMPLETE")
        else:
            for index, rows in enumerate((snapshot.pump_rows, snapshot.controls, snapshot.receipts, snapshot.gaps)):
                lower = progress.cursors[index].rowid if progress.cursors else (
                    binding.anchors[index].rowid - 1 if index < 3 else 0)
                if any(row.rowid <= prior for prior, row in zip((lower, *(r.rowid for r in rows)), rows)):
                    reasons.append("SOURCE_ROW_ORDER_INVALID")
                expected = witness(TABLES[index], rows[-1]) if rows else (
                    progress.cursors[index] if progress.cursors else CursorWitness(TABLES[index], 0, ZERO_DIGEST))
                if snapshot.cursors[index] != expected:
                    reasons.append("SOURCE_CURSOR_WITNESS_MISMATCH")
    if not reasons:
        try:
            progress = _advance(progress, snapshot)
        except SourceReadError as exc:
            reasons.append(str(exc))
    integrity = tuple(sorted(set(progress.integrity_reasons) | (set(reasons) & INTEGRITY_REASONS)))
    if integrity != progress.integrity_reasons:
        progress = replace(progress, integrity_reasons=integrity)
    # Current source health includes all known uncertainty since the original
    # coverage origin, even beyond an older requested prefix. Mutable recovery
    # status, a newer receipt or selecting an older cut cannot remove it.
    if any(g.end_utc >= origin for g in progress.gaps) or any(
            g.gap_end_utc >= origin for g in progress.control_gaps):
        reasons.append("UNRESOLVED_RECORDED_GAP")
    if any(b.at_utc >= origin for b in progress.boundaries):
        reasons.append("UNCERTAIN_OR_INTERRUPTED_PREFIX")
    if not progress.collector_start_utc or not progress.active_since_utc:
        reasons.append("SUBSCRIPTION_COVERAGE_UNOBSERVED")
    elif progress.active_since_utc > origin:
        reasons.append("COVERAGE_START_NOT_IN_CURRENT_SUBSCRIPTION")
    if progress.connection_state in ("LOST", "STOPPED"):
        reasons.append("SUBSCRIPTION_LOST")
    elif progress.connection_state != "ACTIVE":
        reasons.append("SUBSCRIPTION_NOT_ACTIVE")
    if not progress.last_live_receipt_utc or cut > progress.last_live_receipt_utc:
        reasons.append("UNOBSERVED_RECEIPT_TAIL")
    if progress.last_live_receipt_utc and _seconds(now, progress.last_live_receipt_utc) >= profile.freshness_seconds:
        reasons.append("LIVE_RECEIPT_STALE")
    reasons = tuple(sorted(set(reasons)))
    disposition = "HEALTHY"
    if reasons:
        disposition = "UNKNOWN"
        if "LIVE_RECEIPT_STALE" in reasons:
            disposition = "STALE"
        if "SUBSCRIPTION_LOST" in reasons or "SOURCE_WITNESS_MISSING" in reasons:
            disposition = "LOST"
        if any("REGRESSION" in reason or reason in ("SOURCE_WITNESS_CHANGED", "SOURCE_IDENTITY_CHANGED") for reason in reasons):
            disposition = "REGRESSION"
        if "UNRESOLVED_RECORDED_GAP" in reasons:
            disposition = "GAP"
    return SourceVerdict(binding, profile, snapshot, progress, disposition, reasons,
                         origin if not reasons else "", cut if not reasons else "")


def _healthy_consistent(verdict: SourceVerdict) -> bool:
    """Local checks required even when a consumer receives an in-memory object."""
    p, s = verdict.progress, verdict.snapshot
    origin, cut, now = verdict.binding.coverage_start_utc, s.requested_cut_utc, s.observed_at_utc
    if s.previous_verdict_digest == ZERO_DIGEST:
        try:
            if _advance(SourceProgress(), s) != p:
                return False
        except SourceReadError:
            return False
    if any(row.inserted_at_utc > now for row in s.pump_rows) or any(
            row.at_utc > now for row in (*s.controls, *s.receipts)):
        return False
    return bool(
        not s.problems and not p.integrity_reasons and not verdict.reasons
        and verdict.covered_from_utc == origin and verdict.covered_through_utc == cut
        and now >= cut >= origin
        and p.connection_state == "ACTIVE" and p.collector_start_utc
        and p.active_since_utc and p.collector_start_utc <= p.active_since_utc <= origin
        and p.last_live_receipt_utc and cut <= p.last_live_receipt_utc <= now
        and _seconds(now, p.last_live_receipt_utc) < verdict.profile.freshness_seconds
        and p.cursors == s.cursors and len(p.cursors) == len(TABLES)
        and not any(b.at_utc >= origin for b in p.boundaries)
        and not any(g.end_utc >= origin for g in p.gaps)
        and not any(g.gap_end_utc >= origin for g in p.control_gaps)
    )


@dataclass(frozen=True, slots=True)
class SourceConsumerEvidence:
    evidence_digest: str
    source_identity: str
    cut_utc: str
    observed_prefix_supported: bool
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        _hash(self.evidence_digest)
        _hash(self.source_identity)
        object.__setattr__(self, "cut_utc", utc(self.cut_utc))
        if type(self.observed_prefix_supported) is not bool:
            raise ValueError("boolean evidence disposition required")
        _tuple(self.reasons, str)


def source_consumer_evidence(verdict: SourceVerdict, *, expected_source_identity: str,
                             required_cut_utc: str, now_utc: str) -> SourceConsumerEvidence:
    """Immutable evidence port for future Authority/Runtime; no economic mutation."""
    reasons = list(verdict.reasons)
    if verdict.disposition == "HEALTHY" and not _healthy_consistent(verdict):
        reasons.append("CONSUMER_EVIDENCE_INCONSISTENT")
    now, cut = utc(now_utc), utc(required_cut_utc)
    if expected_source_identity != verdict.binding.source_identity:
        reasons.append("CONSUMER_SOURCE_IDENTITY_MISMATCH")
    if cut != verdict.snapshot.requested_cut_utc:
        reasons.append("CONSUMER_CUT_MISMATCH")
    if now < verdict.snapshot.observed_at_utc:
        reasons.append("CONSUMER_CLOCK_REGRESSION")
    if verdict.progress.last_live_receipt_utc and _seconds(now, verdict.progress.last_live_receipt_utc) >= verdict.profile.freshness_seconds:
        reasons.append("LIVE_RECEIPT_STALE")
    return SourceConsumerEvidence(verdict.content_digest, verdict.binding.source_identity, cut,
                                  verdict.disposition == "HEALTHY" and not reasons, tuple(sorted(set(reasons))))


def verdict_from_json(payload: str) -> SourceVerdict:
    """Strict immutable reconstruction used by the append-only evidence store."""
    return _decode_verdict(json.loads(payload))


def _decode_verdict(raw, arrays=None):
    data = dict(raw)
    binding = data.pop("binding")
    binding = dict(binding, anchors=tuple(CursorWitness(**w) for w in binding["anchors"]))
    snapshot = dict(data.pop("snapshot"))
    for field, cls in (("pump_rows", PumpFact), ("controls", ControlFact), ("receipts", ReceiptFact),
                       ("gaps", GapFact), ("cursors", CursorWitness)):
        snapshot[field] = (tuple(cls(**item) for item in snapshot[field]) if arrays is None
            else arrays("snapshot",field,cls,snapshot[field]))
    snapshot["problems"] = tuple(snapshot["problems"])
    progress = dict(data.pop("progress"))
    for field, cls in (("cursors", CursorWitness), ("boundaries", BoundaryFact),
                       ("gaps", GapFact), ("control_gaps", ControlFact)):
        progress[field] = (tuple(cls(**item) for item in progress[field]) if arrays is None
            else arrays("progress",field,cls,progress[field]))
    progress["integrity_reasons"] = tuple(progress["integrity_reasons"])
    data["reasons"] = tuple(data["reasons"])
    return SourceVerdict(SourceBinding(**binding), SourceProfile(**data.pop("profile")),
                         SourceSnapshot(**snapshot), SourceProgress(**progress), **data)


def _same_source_raw(left, right):
    """JSON equality must distinguish bool/float from original integer facts."""
    if type(left) is not type(right):
        return False
    if type(left) in (tuple,list):
        return len(left) == len(right) and all(_same_source_raw(a,b) for a,b in zip(left,right))
    if type(left) is dict:
        return left.keys() == right.keys() and all(_same_source_raw(value,right[key]) for key,value in left.items())
    return left == right


class _SourceReplayDecoder:
    """One cold scan's immediately preceding immutable fact arrays, no history cache.

    Every payload is parsed. Only exactly equal raw arrays reuse their previously
    validated immutable facts; changed content takes the original constructors.
    Each verdict, its complete canonical hash, and its transition are still
    validated. The cached field records are exact original asdict representations,
    not hashes substituted for original evidence or a persisted checkpoint.
    """
    def __init__(self, *, allow_append_prefix=False, canonical_fragments=False):
        if type(allow_append_prefix) is not bool:
            raise ValueError("explicit source replay prefix scope required")
        if type(canonical_fragments) is not bool:
            raise ValueError("explicit canonical fragment scope required")
        self.allow_append_prefix = allow_append_prefix
        self.canonical_fragments = canonical_fragments
        self.parts = {}
        self.comparison_keys = {}
        self.fragments = {}

    def canonical_record(self, value):
        """Original JSON bytes for one cold read, with verified array fragments.

        Only immutable tuples reconstructed by this decoder are fragment keys.
        Every enclosing field is serialized again, in original sorted-key order.
        This private method does not change the public or write-side codec.
        """
        cached = self.fragments.get(id(value))
        if cached is not None and cached[0] is value:
            return cached[1]
        if is_dataclass(value) and not isinstance(value, type):
            return "{"+",".join(json.dumps(field.name)+":"+self.canonical_record(getattr(value,field.name))
                for field in sorted(fields(value),key=lambda field:field.name))+"}"
        if type(value) in (tuple,list):
            return "["+",".join(self.canonical_record(item) for item in value)+"]"
        if type(value) is dict and all(type(key) is str for key in value):
            return "{"+",".join(json.dumps(key,ensure_ascii=True)+":"+self.canonical_record(value[key])
                for key in sorted(value))+"}"
        return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False)

    def decode(self, payload):
        parts = {}
        fragments = {}
        comparison_keys = {}
        def comparison_key(raw):
            # Comparison only, on trees returned by json.loads. No loads,
            # persisted pickle, or change to the original receipt JSON codec.
            import pickle
            try:
                return pickle.dumps(raw, protocol=4)
            except (TypeError, ValueError, OverflowError, RecursionError, pickle.PickleError):
                return None
        def arrays(section, field, cls, raw):
            key = section,field
            prior = self.parts.get(key)
            raw_key = comparison_key(raw) if self.allow_append_prefix else None
            if self.allow_append_prefix:
                prior_key = self.comparison_keys.get(key)
                same = prior is not None and raw_key is not None and raw_key == prior_key
                prefix = (prior is not None and type(raw) is list and len(raw) > len(prior[0])
                    and prior_key is not None and comparison_key(raw[:len(prior[0])]) == prior_key)
                comparison_keys[key] = raw_key
            else:
                same = prior is not None and _same_source_raw(prior[0], raw)
                prefix = False
            if same:
                value, encoded = prior[1:]
            elif prefix:
                # Source-store-only append replay: every old raw field must be
                # exactly equal, not merely the row IDs or tuple length. Only
                # immutable facts already validated in this scan are reused.
                tail = tuple(cls(**item) for item in raw[len(prior[0]):])
                value = prior[1]+tail
                encoded = prior[2]+tuple(asdict(item) for item in tail)
            else:
                value = tuple(cls(**item) for item in raw)
                encoded = tuple(asdict(item) for item in value)
            parts[key] = (raw,value,encoded)
            if self.canonical_fragments:
                cached = self.fragments.get(id(value))
                fragment = (cached[1] if cached is not None and cached[0] is value else
                    json.dumps(encoded,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False))
                fragments[id(value)] = (value,fragment)
            else:
                fragments.update((id(item),(item,record)) for item,record in zip(value,encoded))
            return value
        value = _decode_verdict(json.loads(payload), arrays)
        def encode(item):
            cached = fragments.get(id(item))
            if cached is not None and cached[0] is item:
                return cached[1]
            if is_dataclass(item) and not isinstance(item,type):
                return {field.name:getattr(item,field.name) for field in fields(item)}
            raise TypeError("unsupported source record")
        if self.canonical_fragments:
            previous_fragments = self.fragments
            self.fragments = fragments
            try:
                encoded = self.canonical_record(value)
            except BaseException:
                self.fragments = previous_fragments
                raise
        else:
            encoded = json.dumps(value,default=encode,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False)
        current_digest = hashlib.sha256(encoded.encode()).hexdigest()
        self.parts = parts
        self.comparison_keys = comparison_keys
        return value,current_digest
