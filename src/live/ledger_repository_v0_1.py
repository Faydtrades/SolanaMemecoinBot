"""Dedicated, exclusively written LIVE opening-Evidence journal.

Initialize and reopen are deliberately different operations. The retained OS
lock excludes other cooperating Ledger writers; durable generations and revision
CAS fence stale callers. This is storage ownership, not Operations arming or a
heartbeat lease. No economic records are imported from another database.
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from .ledger_domain_v0_1 import (
    LedgerContractError, LedgerDomain, OpeningBaselineDecision, SCHEMA_VERSION,
    ZERO_DIGEST, adjudicate_opening_baseline, digest_value, ledger_utc,
)
from .ledger_evidence_codec_v0_1 import (
    LedgerEvidenceCodecError, strict_json_object, wallet_observation_from_json, wallet_observation_to_json,
)
from .public_rpc_v0_1 import public_label, u64
from .wallet_evidence_v0_1 import WalletObservation
from .ledger_actions_v0_1 import (
    CandidateInboxInput, PendingAction, AttemptPreparation, AttemptStageInput, StoredAttempt,
    PENDING_ADMISSION, NON_ACCEPTANCE, TERMINAL_INBOX, candidate_from_json, action_from_json,
    preparation_from_json, stage_from_record, position_identity, public_reference,
    decode_message, validate_stage_transition, utc_microseconds,
)

APPLICATION_ID = 0x4C454447
STORAGE_VERSION = 2
RECEIPT_VERSION = "live_ledger_wallet_receipt_v0.1"
GENERATION_VERSION = "live_ledger_writer_generation_v0.1"
COMMIT_VERSION = "live_ledger_concrete_commit_v0.1"
_COMMIT_KINDS = frozenset(("WALLET", "INBOX", "NON_ACCEPTANCE", "ACTION", "ATTEMPT", "ATTEMPT_STAGE"))
_OPEN_TOKEN = object()
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PROTECTED_RUNTIME = Path("D:/Tradingbot/solana_memecoin_bot_phase1_v0_1")


class LedgerJournalError(LedgerContractError):
    pass


class LedgerConflict(LedgerJournalError):
    pass


_DDL = {
    "ledger_domain": """CREATE TABLE ledger_domain (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        economic_domain_id TEXT NOT NULL UNIQUE,
        schema_version TEXT NOT NULL,
        binding_digest TEXT NOT NULL,
        payload_json TEXT NOT NULL)""",
    "ledger_writer_generations": """CREATE TABLE ledger_writer_generations (
        generation INTEGER PRIMARY KEY CHECK(generation>=1),
        previous_digest TEXT NOT NULL,
        content_digest TEXT NOT NULL UNIQUE,
        payload_json TEXT NOT NULL)""",
    "ledger_wallet_observations": """CREATE TABLE ledger_wallet_observations (
        evidence_digest TEXT PRIMARY KEY NOT NULL,
        payload_json TEXT NOT NULL)""",
    "ledger_baseline_journal": """CREATE TABLE ledger_baseline_journal (
        seq INTEGER PRIMARY KEY CHECK(seq>=1),
        ingestion_key TEXT NOT NULL UNIQUE,
        evidence_digest TEXT NOT NULL UNIQUE REFERENCES ledger_wallet_observations(evidence_digest),
        generation INTEGER NOT NULL REFERENCES ledger_writer_generations(generation),
        previous_digest TEXT NOT NULL,
        receipt_digest TEXT NOT NULL UNIQUE,
        payload_json TEXT NOT NULL,
        baseline_domain_id TEXT UNIQUE REFERENCES ledger_domain(economic_domain_id))""",
    "ledger_head": """CREATE TABLE ledger_head (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        generation INTEGER NOT NULL CHECK(generation>=0),
        revision INTEGER NOT NULL CHECK(revision>=0),
        last_receipt_digest TEXT NOT NULL)""",
    "ledger_commits": """CREATE TABLE ledger_commits (
        seq INTEGER PRIMARY KEY CHECK(seq>=1), kind TEXT NOT NULL,
        record_key TEXT NOT NULL, record_digest TEXT NOT NULL,
        generation INTEGER NOT NULL REFERENCES ledger_writer_generations(generation),
        previous_digest TEXT NOT NULL, content_digest TEXT NOT NULL UNIQUE,
        payload_json TEXT NOT NULL, UNIQUE(kind,record_key))""",
    "ledger_candidate_inbox": """CREATE TABLE ledger_candidate_inbox (
        candidate_key TEXT PRIMARY KEY NOT NULL, root_id TEXT NOT NULL UNIQUE,
        mint TEXT NOT NULL, content_digest TEXT NOT NULL, payload_json TEXT NOT NULL,
        commit_seq INTEGER NOT NULL UNIQUE REFERENCES ledger_commits(seq) DEFERRABLE INITIALLY DEFERRED)""",
    "ledger_inbox_dispositions": """CREATE TABLE ledger_inbox_dispositions (
        root_id TEXT NOT NULL REFERENCES ledger_candidate_inbox(root_id), ordinal INTEGER NOT NULL CHECK(ordinal>=1),
        idempotency_key TEXT NOT NULL, disposition TEXT NOT NULL, content_digest TEXT NOT NULL, payload_json TEXT NOT NULL,
        commit_seq INTEGER NOT NULL UNIQUE REFERENCES ledger_commits(seq) DEFERRABLE INITIALLY DEFERRED,
        PRIMARY KEY(root_id,ordinal), UNIQUE(root_id,idempotency_key))""",
    "ledger_pending_actions": """CREATE TABLE ledger_pending_actions (
        action_id TEXT PRIMARY KEY NOT NULL, root_id TEXT NOT NULL REFERENCES ledger_candidate_inbox(root_id),
        side TEXT NOT NULL, position_id TEXT NOT NULL, obligation_id TEXT, ordinal INTEGER NOT NULL CHECK(ordinal>=1),
        content_digest TEXT NOT NULL, payload_json TEXT NOT NULL,
        commit_seq INTEGER NOT NULL UNIQUE REFERENCES ledger_commits(seq) DEFERRABLE INITIALLY DEFERRED)""",
    "ledger_unique_buy_root": "CREATE UNIQUE INDEX ledger_unique_buy_root ON ledger_pending_actions(root_id) WHERE side='BUY'",
    "ledger_unique_sell_ordinal": "CREATE UNIQUE INDEX ledger_unique_sell_ordinal ON ledger_pending_actions(position_id,obligation_id,ordinal) WHERE side='SELL'",
    "ledger_attempts": """CREATE TABLE ledger_attempts (
        attempt_id TEXT PRIMARY KEY NOT NULL, action_id TEXT NOT NULL REFERENCES ledger_pending_actions(action_id),
        ordinal INTEGER NOT NULL CHECK(ordinal>=1), message_sha256 TEXT NOT NULL UNIQUE,
        content_digest TEXT NOT NULL, payload_json TEXT NOT NULL,
        commit_seq INTEGER NOT NULL UNIQUE REFERENCES ledger_commits(seq) DEFERRABLE INITIALLY DEFERRED,
        UNIQUE(action_id,ordinal))""",
    "ledger_attempt_stages": """CREATE TABLE ledger_attempt_stages (
        attempt_id TEXT NOT NULL REFERENCES ledger_attempts(attempt_id), revision INTEGER NOT NULL CHECK(revision>=1),
        idempotency_key TEXT NOT NULL, content_digest TEXT NOT NULL, payload_json TEXT NOT NULL,
        commit_seq INTEGER NOT NULL UNIQUE REFERENCES ledger_commits(seq) DEFERRABLE INITIALLY DEFERRED,
        PRIMARY KEY(attempt_id,revision), UNIQUE(attempt_id,idempotency_key))""",
    "ledger_signature_bindings": """CREATE TABLE ledger_signature_bindings (
        signature TEXT PRIMARY KEY NOT NULL, attempt_id TEXT NOT NULL UNIQUE REFERENCES ledger_attempts(attempt_id),
        wire_digest TEXT NOT NULL)""",
    "ledger_mutation_lane": """CREATE TABLE ledger_mutation_lane (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        active_attempt_id TEXT REFERENCES ledger_attempts(attempt_id), revision INTEGER NOT NULL CHECK(revision>=0))""",
}
for _table in ("ledger_domain", "ledger_writer_generations", "ledger_wallet_observations", "ledger_baseline_journal",
               "ledger_commits", "ledger_candidate_inbox", "ledger_inbox_dispositions", "ledger_pending_actions",
               "ledger_attempts", "ledger_attempt_stages", "ledger_signature_bindings"):
    for _operation in ("UPDATE", "DELETE"):
        _name = f"{_table}_no_{_operation.lower()}"
        _DDL[_name] = (f"CREATE TRIGGER {_name} BEFORE {_operation} ON {_table} BEGIN "
                       "SELECT RAISE(ABORT,'immutable LIVE ledger history'); END")


def _compact_sql(value: str) -> str:
    return " ".join(value.split())


def _journal_path(value: str | Path) -> Path:
    path = Path(value).absolute()
    # Reject aliases that could give one inode different companion lock files.
    for component in (path, *path.parents):
        if component.is_symlink() or getattr(component, "is_junction", lambda: False)():
            raise LedgerJournalError("LEDGER_PATH_ALIAS_FORBIDDEN")
    resolved = path.resolve()
    for root in ((_PROJECT_ROOT / "data").resolve(), _PROTECTED_RUNTIME.resolve()):
        if resolved == root or root in resolved.parents:
            raise LedgerJournalError("PROTECTED_RUNTIME_OR_DATA_PATH")
    if not resolved.parent.is_dir() or resolved.is_dir():
        raise LedgerJournalError("DEDICATED_LEDGER_FILE_REQUIRED")
    if resolved.exists() and resolved.stat().st_nlink != 1:
        raise LedgerJournalError("LEDGER_HARDLINK_FORBIDDEN")
    return resolved


class _ExclusiveWriter:
    def __init__(self, path: Path) -> None:
        self.fd = None
        lock_path = path.with_name(path.name + ".writer.lock")
        if lock_path.is_symlink() or (lock_path.exists() and lock_path.stat().st_nlink != 1):
            raise LedgerJournalError("LEDGER_WRITER_LOCK_ALIAS_FORBIDDEN")
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            if os.fstat(fd).st_nlink != 1:
                raise OSError
            if os.name == "nt":
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.fd = fd
            if os.fstat(fd).st_size == 0:
                os.write(fd, b"L")
                os.fsync(fd)
        except BaseException:
            os.close(fd)
            self.fd = None
            raise LedgerConflict("LEDGER_WRITER_ALREADY_OWNED_OR_UNAVAILABLE") from None

    def close(self) -> None:
        if self.fd is not None:
            fd, self.fd = self.fd, None
            # Closing the descriptor releases the kernel lock. Never unlink the
            # lock file: unlink/recreate would allow two separately locked inodes.
            os.close(fd)


@dataclass(frozen=True, slots=True)
class LedgerWriteFence:
    economic_domain_id: str
    generation: int
    generation_digest: str
    revision: int
    last_receipt_digest: str

    def __post_init__(self) -> None:
        for value in (self.economic_domain_id, self.generation_digest, self.last_receipt_digest):
            digest_value(value)
        if (type(self.generation) is not int or self.generation < 1
                or type(self.revision) is not int or self.revision < 0):
            raise LedgerContractError("LEDGER_WRITE_FENCE_INTEGER_INVALID")


@dataclass(frozen=True, slots=True)
class LedgerWalletReceipt:
    sequence: int
    ingestion_key: str
    previous_digest: str
    generation: int
    generation_digest: str
    observation: WalletObservation
    decision: OpeningBaselineDecision

    def to_record(self) -> dict:
        return {"version": RECEIPT_VERSION, "sequence": self.sequence, "ingestion_key": self.ingestion_key,
                "previous_digest": self.previous_digest, "generation": self.generation,
                "generation_digest": self.generation_digest, "evidence_digest": self.observation.content_digest,
                "decision": self.decision.to_record()}

    @property
    def content_digest(self) -> str:
        return content_fingerprint(self.to_record())


def _generation_record(domain: LedgerDomain, generation: int, previous: str, revision: int, last: str) -> dict:
    return {"version": GENERATION_VERSION, "economic_domain_id": domain.economic_domain_id,
            "binding_digest": domain.binding_digest, "generation": generation, "previous_digest": previous,
            "revision_at_acquisition": revision, "last_receipt_digest": last}


def _schema_and_domain(conn: sqlite3.Connection, domain: LedgerDomain) -> None:
    if (conn.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
            or conn.execute("PRAGMA user_version").fetchone()[0] != STORAGE_VERSION):
        raise LedgerJournalError("LEDGER_STORAGE_VERSION_MISMATCH")
    objects = conn.execute("SELECT name,sql FROM sqlite_master WHERE sql IS NOT NULL").fetchall()
    if {name: _compact_sql(sql) for name, sql in objects} != {name: _compact_sql(sql) for name, sql in _DDL.items()}:
        raise LedgerJournalError("DEDICATED_LEDGER_SCHEMA_REQUIRED")
    if conn.execute("SELECT * FROM ledger_domain").fetchall() != [
            (1, domain.economic_domain_id, SCHEMA_VERSION, domain.binding_digest, domain.serialize())]:
        raise LedgerConflict("LEDGER_IMMUTABLE_DOMAIN_OR_CONFIG_MISMATCH")
    if conn.execute("PRAGMA integrity_check").fetchall() != [("ok",)] or conn.execute("PRAGMA foreign_key_check").fetchall():
        raise LedgerJournalError("LEDGER_SQLITE_INTEGRITY_FAILURE")


class LedgerRepository:
    @classmethod
    def initialize(cls, path: str | Path, domain: LedgerDomain) -> LedgerRepository:
        """Explicit first creation only; an existing file is never reset/adopted."""
        return cls(path, domain, _create=True, _token=_OPEN_TOKEN)

    @classmethod
    def reopen(cls, path: str | Path, domain: LedgerDomain) -> LedgerRepository:
        """Expected existing journal; absence/uninitialized state fails closed."""
        return cls(path, domain, _create=False, _token=_OPEN_TOKEN)

    def __init__(self, path: str | Path, domain: LedgerDomain, *, _create: bool, _token: object) -> None:
        self._conn = None
        self._guard = None
        self._generation = 0
        self._generation_digest = ZERO_DIGEST
        if _token is not _OPEN_TOKEN or type(domain) is not LedgerDomain:
            raise LedgerJournalError("EXPLICIT_LEDGER_INITIALIZE_OR_REOPEN_REQUIRED")
        self.domain = domain
        try:
            self.database_path = _journal_path(path)
            self._guard = _ExclusiveWriter(self.database_path)
            if _create:
                if self.database_path.exists():
                    raise LedgerConflict("LEDGER_JOURNAL_ALREADY_EXISTS")
                fd = os.open(self.database_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
                os.close(fd)
            else:
                if not self.database_path.is_file() or self.database_path.stat().st_size == 0:
                    raise LedgerJournalError("EXPECTED_LEDGER_JOURNAL_MISSING_OR_UNINITIALIZED")
                # No DDL, application-id writes, or journal-mode normalization of
                # a supplied existing file before its dedicated identity is proven.
                preflight = sqlite3.connect(self.database_path.as_uri() + "?mode=ro", uri=True,
                                            timeout=1.0, isolation_level=None)
                try:
                    _schema_and_domain(preflight, domain)
                finally:
                    preflight.close()
            self._conn = sqlite3.connect(self.database_path.as_uri() + "?mode=rw", uri=True,
                                         timeout=1.0, isolation_level=None)
            if not _create:
                _schema_and_domain(self._conn, domain)
            self._conn.execute("PRAGMA busy_timeout=1000")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._check_durability()
            if _create:
                self._conn.execute("BEGIN IMMEDIATE")
                for sql in _DDL.values():
                    self._conn.execute(sql)
                self._conn.execute(f"PRAGMA application_id={APPLICATION_ID}")
                self._conn.execute(f"PRAGMA user_version={STORAGE_VERSION}")
                self._conn.execute("INSERT INTO ledger_domain VALUES(1,?,?,?,?)",
                    (domain.economic_domain_id, SCHEMA_VERSION, domain.binding_digest, domain.serialize()))
                self._conn.execute("INSERT INTO ledger_head VALUES(1,0,0,?)", (ZERO_DIGEST,))
                self._conn.execute("INSERT INTO ledger_mutation_lane VALUES(1,NULL,0)")
                self._acquire_generation()
                self._commit()
            else:
                self._conn.execute("BEGIN IMMEDIATE")
                self._verify_history()
                self._acquire_generation()
                self._commit()
            self._verify_history()
        except BaseException as exc:
            self.close()
            if isinstance(exc, (LedgerContractError, KeyboardInterrupt, SystemExit)):
                raise
            raise LedgerJournalError("LEDGER_OPEN_FAILED") from None

    def _require_open(self) -> None:
        if self._conn is None or self._guard is None or self._guard.fd is None:
            raise LedgerJournalError("LEDGER_WRITER_CLOSED")

    def _check_durability(self) -> None:
        if (str(self._conn.execute("PRAGMA journal_mode").fetchone()[0]).lower() != "wal"
                or self._conn.execute("PRAGMA synchronous").fetchone()[0] != 2
                or self._conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1):
            raise LedgerJournalError("LEDGER_REQUIRES_WAL_FULL_FOREIGN_KEYS")

    def _head(self) -> tuple[int, int, str]:
        rows = self._conn.execute("SELECT generation,revision,last_receipt_digest FROM ledger_head WHERE singleton=1").fetchall()
        if len(rows) != 1:
            raise LedgerJournalError("LEDGER_HEAD_MISSING")
        return rows[0]

    def _commit(self) -> None:
        # Capture trusted state while BEGIN IMMEDIATE still excludes external
        # commits. Sampling after COMMIT could silently bless a racing writer.
        verified_head = self._head()
        verified_data_version = self._conn.execute("PRAGMA data_version").fetchone()[0]
        verified_schema_cookie = self._conn.execute("PRAGMA schema_version").fetchone()[0]
        self._conn.execute("COMMIT")
        self._verified_head = verified_head
        self._verified_data_version = verified_data_version
        self._verified_schema_cookie = verified_schema_cookie

    def _append_commit(self, kind: str, key: str, record_digest: str, current: LedgerWriteFence) -> int:
        """Closed concrete commit envelope; caller owns one existing transaction."""
        if not self._conn.in_transaction or kind not in _COMMIT_KINDS:
            raise LedgerJournalError("LEDGER_CONCRETE_TRANSACTION_REQUIRED")
        record = {"version": COMMIT_VERSION, "economic_domain_id": self.domain.economic_domain_id,
                  "sequence": current.revision+1, "kind": kind, "record_key": key, "record_digest": record_digest,
                  "generation": current.generation, "generation_digest": current.generation_digest,
                  "previous_digest": current.last_receipt_digest}
        digest = content_fingerprint(record)
        self._conn.execute("INSERT INTO ledger_commits VALUES(?,?,?,?,?,?,?,?)",
            (current.revision+1, kind, key, record_digest, current.generation, current.last_receipt_digest, digest, canonical_json(record)))
        updated = self._conn.execute("UPDATE ledger_head SET revision=?,last_receipt_digest=? WHERE singleton=1 "
            "AND generation=? AND revision=? AND last_receipt_digest=?",
            (current.revision+1, digest, current.generation, current.revision, current.last_receipt_digest))
        if updated.rowcount != 1:
            raise LedgerConflict("LEDGER_ATOMIC_HEAD_CAS_FAILED")
        return current.revision+1

    def _begin_economic_write(self, fence: LedgerWriteFence) -> LedgerWriteFence:
        self._require_open()
        if type(fence) is not LedgerWriteFence:
            raise LedgerConflict("LEDGER_WRITE_FENCE_REQUIRED")
        self._conn.execute("BEGIN IMMEDIATE")
        self._check_durability()
        if (self._head() != self._verified_head
                or self._conn.execute("PRAGMA data_version").fetchone()[0] != self._verified_data_version
                or self._conn.execute("PRAGMA schema_version").fetchone()[0] != self._verified_schema_cookie):
            raise LedgerConflict("LEDGER_UNVERIFIED_EXTERNAL_CHANGE_OR_FENCE")
        current = self.write_fence()
        if (fence.economic_domain_id, fence.generation, fence.generation_digest) != (
                current.economic_domain_id, current.generation, current.generation_digest):
            raise LedgerConflict("LEDGER_WRITER_GENERATION_FENCED")
        return current

    def _new_revision(self, fence: LedgerWriteFence, current: LedgerWriteFence) -> None:
        if fence != current:
            raise LedgerConflict("LEDGER_REVISION_CAS_FAILED")

    def _rollback_economic(self, exc: BaseException) -> None:
        if self._conn is not None and self._conn.in_transaction:
            self._conn.execute("ROLLBACK")
        if isinstance(exc, (LedgerContractError, LedgerEvidenceCodecError, KeyboardInterrupt, SystemExit)):
            raise exc
        raise LedgerJournalError("LEDGER_ECONOMIC_STORAGE_FAILED") from None

    def _acquire_generation(self) -> None:
        old_generation, revision, last = self._head()
        previous_row = self._conn.execute(
            "SELECT content_digest FROM ledger_writer_generations ORDER BY generation DESC LIMIT 1").fetchone()
        previous = ZERO_DIGEST if previous_row is None else previous_row[0]
        generation = old_generation + 1
        record = _generation_record(self.domain, generation, previous, revision, last)
        digest = content_fingerprint(record)
        self._conn.execute("INSERT INTO ledger_writer_generations VALUES(?,?,?,?)",
                           (generation, previous, digest, canonical_json(record)))
        changed = self._conn.execute("UPDATE ledger_head SET generation=? WHERE singleton=1 "
            "AND generation=? AND revision=? AND last_receipt_digest=?", (generation, old_generation, revision, last))
        if changed.rowcount != 1:
            raise LedgerConflict("LEDGER_WRITER_GENERATION_CAS_FAILED")
        self._generation, self._generation_digest = generation, digest

    def write_fence(self) -> LedgerWriteFence:
        self._require_open()
        generation, revision, digest = self._head()
        if generation != self._generation:
            raise LedgerConflict("LEDGER_WRITER_FENCED")
        return LedgerWriteFence(self.domain.economic_domain_id, generation, self._generation_digest, revision, digest)

    def _receipt_from_row(self, row: tuple) -> LedgerWalletReceipt:
        try:
            seq, key, evidence, generation, previous, digest, payload, baseline_domain, observation_payload = row
            record = strict_json_object(payload)
            observation = wallet_observation_from_json(observation_payload)
            decision = adjudicate_opening_baseline(self.domain, observation,
                evaluated_at_utc=record["decision"]["evaluated_at_utc"],
                required_min_context_slot=record["decision"]["required_min_context_slot"])
            generation_row = self._conn.execute(
                "SELECT content_digest FROM ledger_writer_generations WHERE generation=?", (generation,)).fetchone()
            public_label(key)
            digest_value(previous)
            if generation_row is None:
                raise ValueError
            receipt = LedgerWalletReceipt(seq, key, previous, generation, generation_row[0], observation, decision)
            if (canonical_json(receipt.to_record()) != payload or receipt.content_digest != digest
                    or observation.content_digest != evidence
                    or baseline_domain != (self.domain.economic_domain_id if decision.disposition == "ESTABLISHED" else None)):
                raise ValueError
            return receipt
        except Exception:
            raise LedgerJournalError("LEDGER_RECEIPT_REPLAY_CONFLICT") from None

    def _rows(self, clause: str = "", params: tuple = ()) -> list[tuple]:
        # Call sites supply only fixed local SQL clauses, never public query text.
        return self._conn.execute("SELECT j.seq,j.ingestion_key,j.evidence_digest,j.generation,j.previous_digest,"
            "j.receipt_digest,j.payload_json,j.baseline_domain_id,o.payload_json FROM ledger_baseline_journal j "
            "JOIN ledger_wallet_observations o ON o.evidence_digest=j.evidence_digest " + clause, params).fetchall()

    def candidate(self, root_id: str) -> CandidateInboxInput | None:
        self._require_open()
        digest_value(root_id)
        row = self._conn.execute("SELECT candidate_key,content_digest,payload_json,mint FROM ledger_candidate_inbox WHERE root_id=?",
                                 (root_id,)).fetchone()
        if row is None:
            return None
        value = candidate_from_json(row[2])
        if (value.trade_root(self.domain), value.canonical_candidate_key, value.content_digest, value.mint) != (root_id, row[0], row[1], row[3]):
            raise LedgerConflict("LEDGER_INBOX_IDENTITY_OR_CONTENT_CONFLICT")
        return value

    def inbox_disposition(self, root_id: str) -> str:
        if self.candidate(root_id) is None:
            raise LedgerConflict("LEDGER_CANDIDATE_NOT_FOUND")
        row = self._conn.execute("SELECT disposition FROM ledger_inbox_dispositions WHERE root_id=? ORDER BY ordinal DESC LIMIT 1",
                                 (root_id,)).fetchone()
        return "RECEIVED" if row is None else row[0]

    def receive_candidate(self, candidate: CandidateInboxInput, *, fence: LedgerWriteFence) -> str:
        """At-least-once inbox receipt only; no admission, amount or reservation."""
        try:
            if type(candidate) is not CandidateInboxInput:
                raise LedgerConflict("LEDGER_CANDIDATE_INPUT_REQUIRED")
            candidate = candidate_from_json(canonical_json(candidate.to_record()))
            current = self._begin_economic_write(fence)
            root = candidate.trade_root(self.domain)
            existing = self.candidate(root)
            if existing is not None:
                if existing != candidate:
                    raise LedgerConflict("LEDGER_CANONICAL_CANDIDATE_CONTENT_CONFLICT")
                self._commit()
                return root
            self._new_revision(fence, current)
            self._conn.execute("INSERT INTO ledger_candidate_inbox VALUES(?,?,?,?,?,?)",
                (candidate.canonical_candidate_key, root, candidate.mint, candidate.content_digest,
                 canonical_json(candidate.to_record()), current.revision+1))
            self._append_commit("INBOX", root, candidate.content_digest, current)
            self._commit()
            return root
        except BaseException as exc:
            self._rollback_economic(exc)

    def action(self, action_id: str) -> PendingAction | None:
        self._require_open()
        digest_value(action_id)
        row = self._conn.execute("SELECT content_digest,payload_json FROM ledger_pending_actions WHERE action_id=?", (action_id,)).fetchone()
        if row is None:
            return None
        value = action_from_json(row[1])
        if value.action_id != action_id or value.content_digest != row[0]:
            raise LedgerConflict("LEDGER_ACTION_IDENTITY_OR_CONTENT_CONFLICT")
        return value

    def _validate_action_parent(self, action: PendingAction, *, allow_terminal: bool = False) -> CandidateInboxInput:
        candidate = self.candidate(action.root_id)
        if (candidate is None or candidate.content_digest != action.candidate_digest or candidate.mint != action.mint
                or candidate.winner_candidate_id != candidate.candidate_signal_id
                or action.position_id != position_identity(self.domain, action.root_id, action.mint)
                or action.side == "BUY" and action.claimed_entry_deadline_us < candidate.generated_at_us):
            raise LedgerConflict("LEDGER_ACTION_CANDIDATE_WINNER_OR_POSITION_MISMATCH")
        if not allow_terminal and self.inbox_disposition(action.root_id) in TERMINAL_INBOX:
            raise LedgerConflict("LEDGER_CANDIDATE_HAS_TERMINAL_TOMBSTONE")
        if self._conn.execute("SELECT 1 FROM ledger_baseline_journal WHERE baseline_domain_id=?", (self.domain.economic_domain_id,)).fetchone() is None:
            raise LedgerConflict("LEDGER_OPENING_BASELINE_REQUIRED")
        return candidate

    def _insert_pending_action(self, action: PendingAction, commit_seq: int) -> None:
        """No commit and no grant. L6 can insert this with real admission facts.

        A future atomic admission port need not call the public staging method
        first: terms, reservation and inbox disposition can share its transaction.
        """
        if not self._conn.in_transaction:
            raise LedgerJournalError("LEDGER_ACTION_REQUIRES_OWNED_TRANSACTION")
        self._validate_action_parent(action)
        if action.side == "SELL":
            conflict = self._conn.execute("SELECT action_id FROM ledger_pending_actions WHERE side='SELL' "
                "AND position_id=? AND obligation_id=? AND ordinal=?", (action.position_id, action.obligation_id, action.ordinal)).fetchone()
            if conflict is not None:
                raise LedgerConflict("LEDGER_SELL_ORDINAL_QUANTITY_CONFLICT")
        self._conn.execute("INSERT INTO ledger_pending_actions VALUES(?,?,?,?,?,?,?,?,?)",
            (action.action_id, action.root_id, action.side, action.position_id, action.obligation_id, action.ordinal,
             action.content_digest, canonical_json(action.to_record()), commit_seq))

    def stage_action(self, action: PendingAction, *, fence: LedgerWriteFence) -> PendingAction:
        """Persist pending external terms. This never changes inbox to ACCEPTED."""
        try:
            if type(action) is not PendingAction:
                raise LedgerConflict("LEDGER_PENDING_ACTION_INPUT_REQUIRED")
            action = action_from_json(canonical_json(action.to_record()))
            current = self._begin_economic_write(fence)
            existing = self.action(action.action_id)
            if existing is not None:
                if existing != action:
                    raise LedgerConflict("LEDGER_ACTION_CONTENT_CONFLICT")
                self._commit()
                return existing
            self._new_revision(fence, current)
            self._insert_pending_action(action, current.revision+1)
            self._append_commit("ACTION", action.action_id, action.content_digest, current)
            self._commit()
            return action
        except BaseException as exc:
            self._rollback_economic(exc)

    def _lane(self) -> tuple[str | None, int]:
        rows = self._conn.execute("SELECT active_attempt_id,revision FROM ledger_mutation_lane WHERE singleton=1").fetchall()
        if len(rows) != 1:
            raise LedgerConflict("LEDGER_MUTATION_LANE_MISSING")
        return rows[0]

    def mutation_lane(self) -> dict:
        self._require_open()
        active, revision = self._lane()
        return {"active_attempt_id": active, "revision": revision, "held": active is not None,
                "admission_status": PENDING_ADMISSION, "has_real_authority_grant": False, "may_send": False}

    def _change_lane(self, old: tuple[str | None, int], active: str | None, revision: int) -> None:
        updated = self._conn.execute("UPDATE ledger_mutation_lane SET active_attempt_id=?,revision=? "
            "WHERE singleton=1 AND active_attempt_id IS ? AND revision=?", (active, revision, old[0], old[1]))
        if updated.rowcount != 1:
            raise LedgerConflict("LEDGER_MUTATION_LANE_CAS_FAILED")

    def _prepared_row(self, attempt_id: str):
        return self._conn.execute("SELECT a.payload_json,a.content_digest,a.commit_seq,c.generation FROM ledger_attempts a "
            "JOIN ledger_commits c ON c.seq=a.commit_seq WHERE a.attempt_id=?", (attempt_id,)).fetchone()

    def _replay_stage(self, previous: StoredAttempt, record: dict, generation: int) -> StoredAttempt:
        prep = previous.preparation
        if (set(record) != {"version", "attempt_id", "revision", "idempotency_key", "from_stage", "to_stage",
                           "recorded_at_utc", "writer_generation", "stage_input", "local_cancel"}
                or record["version"] != "live_ledger_attempt_stage_receipt_v0.1"
                or record["attempt_id"] != prep.attempt_id or type(record["revision"]) is not int
                or record["revision"] != previous.revision+1 or record["from_stage"] != previous.recorded_stage
                or record["writer_generation"] != generation or type(record["writer_generation"]) is not int):
            raise LedgerConflict("LEDGER_ATTEMPT_STAGE_HISTORY_INVALID")
        public_reference(record["idempotency_key"])
        at = ledger_utc(record["recorded_at_utc"])
        if at != record["recorded_at_utc"] or at < previous.last_recorded_at_utc:
            raise LedgerConflict("LEDGER_ATTEMPT_TIME_REGRESSION")
        signature, wire_digest = previous.primary_signature, previous.signed_wire_digest
        if record["local_cancel"]:
            if record["local_cancel"] is not True or record["stage_input"] is not None or previous.recorded_stage == "CANCELLED_UNSIGNED":
                raise LedgerConflict("LEDGER_LOCAL_CANCEL_PROOF_INVALID")
            positive = (previous.recorded_stage in ("PREPARED", "EXACT_SIMULATED", "AUTHORIZED")
                        and previous.prepared_generation == generation and signature is None)
            target = "CANCELLED_UNSIGNED" if positive else "UNKNOWN"
            if record["to_stage"] != target:
                raise LedgerConflict("LEDGER_SIGNED_OR_PREDECESSOR_UNCERTAINTY_CANNOT_RELEASE")
        else:
            if record["local_cancel"] is not False:
                raise LedgerConflict("LEDGER_LOCAL_CANCEL_FLAG_INVALID")
            stage = stage_from_record(record["stage_input"])
            validate_stage_transition(prep, previous.recorded_stage, stage, previous.last_recorded_at_utc)
            target = stage.target_stage
            if record["to_stage"] != target or at != stage.recorded_at_utc:
                raise LedgerConflict("LEDGER_EXTERNAL_STAGE_RECORD_MISMATCH")
            if stage.signed_wire_base64 is not None:
                tx = stage.signed_public_transaction()
                signature = str(tx.signatures[0])
                wire_digest = hashlib.sha256(bytes(tx)).hexdigest()
        return StoredAttempt(prep, target, previous.revision+1, previous.prepared_generation, at,
                             signature, wire_digest, target != "CANCELLED_UNSIGNED")

    def attempt(self, attempt_id: str) -> StoredAttempt | None:
        self._require_open()
        digest_value(attempt_id)
        row = self._prepared_row(attempt_id)
        if row is None:
            return None
        prep = preparation_from_json(row[0])
        if prep.attempt_id != attempt_id or prep.content_digest != row[1]:
            raise LedgerConflict("LEDGER_ATTEMPT_IDENTITY_OR_CONTENT_CONFLICT")
        value = StoredAttempt(prep, "PREPARED", 0, row[3], prep.prepared_at_utc, None, None, True)
        for revision, key, digest, payload, generation in self._conn.execute(
                "SELECT s.revision,s.idempotency_key,s.content_digest,s.payload_json,c.generation FROM ledger_attempt_stages s "
                "JOIN ledger_commits c ON c.seq=s.commit_seq WHERE attempt_id=? ORDER BY revision", (attempt_id,)):
            record = strict_json_object(payload)
            if canonical_json(record) != payload or content_fingerprint(record) != digest or (record["revision"], record["idempotency_key"]) != (revision, key):
                raise LedgerConflict("LEDGER_ATTEMPT_STAGE_CONTENT_CONFLICT")
            value = self._replay_stage(value, record, generation)
        signature_row = self._conn.execute("SELECT signature,wire_digest FROM ledger_signature_bindings WHERE attempt_id=?", (attempt_id,)).fetchone()
        if signature_row != (None if value.primary_signature is None else (value.primary_signature, value.signed_wire_digest)):
            raise LedgerConflict("LEDGER_ATTEMPT_SIGNATURE_LINEAGE_CONFLICT")
        return value

    def _validate_preparation(self, prep: AttemptPreparation) -> None:
        action = self.action(prep.action_id)
        if action is None or action.content_digest != prep.action_content_digest:
            raise LedgerConflict("LEDGER_ATTEMPT_ACTION_CONTENT_MISMATCH")
        candidate = self._validate_action_parent(action)
        baseline_payload = self._conn.execute("SELECT payload_json FROM ledger_baseline_journal WHERE baseline_domain_id=?",
                                               (self.domain.economic_domain_id,)).fetchone()
        baseline_slot = strict_json_object(baseline_payload[0])["decision"]["context_slot"]
        message = decode_message(prep.message_hex)
        if (str(message.account_keys[0]) != self.domain.wallet
                or prep.finalized_lower_anchor.provider_fingerprint != self.domain.expected_profile_fingerprint
                or prep.finalized_lower_anchor.slot < max(self.domain.minimum_context_slot, baseline_slot)
                or utc_microseconds(prep.prepared_at_utc) < candidate.generated_at_us
                or action.side == "BUY" and utc_microseconds(prep.prepared_at_utc) > action.claimed_entry_deadline_us):
            raise LedgerConflict("LEDGER_ATTEMPT_WALLET_ORIGINAL_ANCHOR_OR_DEADLINE_MISMATCH")

    def _insert_prepared_attempt(self, prep: AttemptPreparation, commit_seq: int) -> None:
        """No commit: a future concrete consumer port may share this transaction."""
        if not self._conn.in_transaction:
            raise LedgerJournalError("LEDGER_ATTEMPT_REQUIRES_OWNED_TRANSACTION")
        self._validate_preparation(prep)
        lane = self._lane()
        if lane[0] is not None:
            raise LedgerConflict("LEDGER_WALLET_MUTATION_LANE_HELD")
        previous = self._conn.execute("SELECT attempt_id,ordinal FROM ledger_attempts WHERE action_id=? ORDER BY ordinal DESC LIMIT 1",
                                       (prep.action_id,)).fetchone()
        ordinal = 1 if previous is None else previous[1]+1
        old_attempt = None if previous is None else self.attempt(previous[0])
        if (prep.ordinal != ordinal or old_attempt is not None and (old_attempt.recorded_stage != "CANCELLED_UNSIGNED"
                or prep.prepared_at_utc < old_attempt.last_recorded_at_utc)):
            raise LedgerConflict("LEDGER_PREVIOUS_ATTEMPT_UNRESOLVED_OR_ORDINAL_CONFLICT")
        if self._conn.execute("SELECT 1 FROM ledger_attempts WHERE message_sha256=?", (prep.message_sha256,)).fetchone():
            raise LedgerConflict("LEDGER_SAME_MESSAGE_MUST_RETAIN_ATTEMPT_IDENTITY")
        self._conn.execute("INSERT INTO ledger_attempts VALUES(?,?,?,?,?,?,?)", (prep.attempt_id, prep.action_id, prep.ordinal,
            prep.message_sha256, prep.content_digest, canonical_json(prep.to_record()), commit_seq))
        self._change_lane(lane, prep.attempt_id, commit_seq)

    def prepare_attempt(self, prep: AttemptPreparation, *, fence: LedgerWriteFence) -> StoredAttempt:
        try:
            if type(prep) is not AttemptPreparation:
                raise LedgerConflict("LEDGER_ATTEMPT_PREPARATION_REQUIRED")
            prep = preparation_from_json(canonical_json(prep.to_record()))
            current = self._begin_economic_write(fence)
            existing = self.attempt(prep.attempt_id)
            if existing is not None:
                if existing.preparation != prep:
                    raise LedgerConflict("LEDGER_ATTEMPT_PREPARATION_CONTENT_CONFLICT")
                self._commit()
                return existing
            self._new_revision(fence, current)
            self._insert_prepared_attempt(prep, current.revision+1)
            self._append_commit("ATTEMPT", prep.attempt_id, prep.content_digest, current)
            self._commit()
            return self.attempt(prep.attempt_id)
        except BaseException as exc:
            self._rollback_economic(exc)

    def _append_attempt_stage(self, previous: StoredAttempt, record: dict, current: LedgerWriteFence) -> StoredAttempt:
        if not self._conn.in_transaction:
            raise LedgerJournalError("LEDGER_STAGE_REQUIRES_OWNED_TRANSACTION")
        self._validate_action_parent(self.action(previous.preparation.action_id))
        lane = self._lane()
        if lane[0] != previous.preparation.attempt_id:
            raise LedgerConflict("LEDGER_ATTEMPT_DOES_NOT_HOLD_MUTATION_LANE")
        value = self._replay_stage(previous, record, current.generation)
        digest = content_fingerprint(record)
        self._conn.execute("INSERT INTO ledger_attempt_stages VALUES(?,?,?,?,?,?)",
            (record["attempt_id"], record["revision"], record["idempotency_key"], digest, canonical_json(record), current.revision+1))
        if value.primary_signature is not None and previous.primary_signature is None:
            if self._conn.execute("SELECT 1 FROM ledger_signature_bindings WHERE signature=?", (value.primary_signature,)).fetchone():
                raise LedgerConflict("LEDGER_SIGNATURE_ALREADY_BOUND_TO_ATTEMPT")
            self._conn.execute("INSERT INTO ledger_signature_bindings VALUES(?,?,?)",
                               (value.primary_signature, record["attempt_id"], value.signed_wire_digest))
        self._change_lane(lane, record["attempt_id"] if value.lane_held else None, current.revision+1)
        self._append_commit("ATTEMPT_STAGE", f"{record['attempt_id']}:{record['revision']}", digest, current)
        return value

    def _stage_command(self, attempt_id: str, stage: AttemptStageInput | None, at: str, idempotency_key: str,
                       expected_attempt_revision: int, fence: LedgerWriteFence) -> StoredAttempt:
        try:
            at = ledger_utc(at)
            public_reference(idempotency_key)
            if type(expected_attempt_revision) is not int or expected_attempt_revision < 0:
                raise LedgerConflict("LEDGER_ATTEMPT_REVISION_REQUIRED")
            current = self._begin_economic_write(fence)
            previous = self.attempt(attempt_id)
            if previous is None:
                raise LedgerConflict("LEDGER_ATTEMPT_NOT_FOUND")
            duplicate = self._conn.execute("SELECT payload_json FROM ledger_attempt_stages WHERE attempt_id=? AND idempotency_key=?",
                                            (attempt_id, idempotency_key)).fetchone()
            if duplicate is not None:
                original = strict_json_object(duplicate[0])
                if (original["recorded_at_utc"] != at or original["local_cancel"] != (stage is None)
                        or canonical_json(original["stage_input"]) != canonical_json(None if stage is None else stage.to_record())
                        or original["revision"] != expected_attempt_revision+1):
                    raise LedgerConflict("LEDGER_STAGE_IDEMPOTENCY_CONTENT_CONFLICT")
                self._commit()
                return previous
            self._new_revision(fence, current)
            if previous.revision != expected_attempt_revision:
                raise LedgerConflict("LEDGER_ATTEMPT_REVISION_CAS_FAILED")
            target = stage.target_stage if stage is not None else (
                "CANCELLED_UNSIGNED" if previous.recorded_stage in ("PREPARED", "EXACT_SIMULATED", "AUTHORIZED")
                and previous.prepared_generation == current.generation and previous.primary_signature is None else "UNKNOWN")
            record = {"version": "live_ledger_attempt_stage_receipt_v0.1", "attempt_id": attempt_id,
                "revision": previous.revision+1, "idempotency_key": idempotency_key, "from_stage": previous.recorded_stage,
                "to_stage": target, "recorded_at_utc": at, "writer_generation": current.generation,
                "stage_input": None if stage is None else stage.to_record(), "local_cancel": stage is None}
            value = self._append_attempt_stage(previous, record, current)
            self._commit()
            return value
        except BaseException as exc:
            self._rollback_economic(exc)

    def record_external_attempt_stage(self, stage: AttemptStageInput, *, idempotency_key: str,
                                      expected_attempt_revision: int, fence: LedgerWriteFence) -> StoredAttempt:
        """An external stage CLAIM. AUTHORIZED is never a real Authority grant."""
        if type(stage) is not AttemptStageInput:
            raise LedgerConflict("LEDGER_EXTERNAL_STAGE_INPUT_REQUIRED")
        stage = stage_from_record(stage.to_record())
        return self._stage_command(stage.attempt_id, stage, stage.recorded_at_utc, idempotency_key,
                                   expected_attempt_revision, fence)

    def cancel_attempt_locally(self, attempt_id: str, *, recorded_at_utc: str, idempotency_key: str,
                               expected_attempt_revision: int, fence: LedgerWriteFence) -> StoredAttempt:
        """Release only unsigned facts from this intact exclusive generation.

        Signed or predecessor-process uncertainty becomes UNKNOWN and keeps the
        lane; absence of a send claim is never proof that signed bytes stayed local.
        """
        return self._stage_command(attempt_id, None, recorded_at_utc, idempotency_key, expected_attempt_revision, fence)

    def record_nonacceptance(self, root_id: str, disposition: str, *, external_reference: str,
                             external_record_digest: str, recorded_at_utc: str, idempotency_key: str,
                             fence: LedgerWriteFence) -> str:
        try:
            if disposition not in NON_ACCEPTANCE:
                raise LedgerConflict("LEDGER_CANNOT_ISSUE_ADMISSION_ACCEPTANCE")
            for value in (external_reference, idempotency_key):
                public_reference(value)
            digest_value(external_record_digest)
            at = ledger_utc(recorded_at_utc)
            current = self._begin_economic_write(fence)
            old = self.inbox_disposition(root_id)
            if utc_microseconds(at) < self.candidate(root_id).generated_at_us:
                raise LedgerConflict("LEDGER_NONACCEPTANCE_PRECEDES_ORIGINAL_CANDIDATE")
            rows = self._conn.execute("SELECT ordinal,payload_json FROM ledger_inbox_dispositions WHERE root_id=? ORDER BY ordinal", (root_id,)).fetchall()
            ordinal = len(rows)+1
            record = {"version": "live_ledger_nonacceptance_v0.1", "root_id": root_id, "ordinal": ordinal,
                "idempotency_key": idempotency_key, "from_disposition": old, "disposition": disposition,
                "external_reference": external_reference, "external_record_digest": external_record_digest,
                "recorded_at_utc": at, "has_real_authority_grant": False}
            for number, payload in rows:
                previous = strict_json_object(payload)
                if previous["idempotency_key"] == idempotency_key:
                    replay = {**record, "ordinal": number, "from_disposition": previous["from_disposition"]}
                    if canonical_json(replay) != payload:
                        raise LedgerConflict("LEDGER_INBOX_DISPOSITION_CONTENT_CONFLICT")
                    self._commit()
                    return old
            self._new_revision(fence, current)
            if old in TERMINAL_INBOX or rows and at < strict_json_object(rows[-1][1])["recorded_at_utc"]:
                raise LedgerConflict("LEDGER_TERMINAL_INBOX_OR_TIME_REGRESSION")
            for (attempt_id,) in self._conn.execute("SELECT t.attempt_id FROM ledger_attempts t JOIN ledger_pending_actions a "
                                                    "ON a.action_id=t.action_id WHERE a.root_id=?", (root_id,)):
                if self.attempt(attempt_id).recorded_stage != "CANCELLED_UNSIGNED":
                    raise LedgerConflict("LEDGER_UNRESOLVED_ATTEMPT_PREVENTS_NONACQUISITION_TOMBSTONE")
            digest = content_fingerprint(record)
            self._conn.execute("INSERT INTO ledger_inbox_dispositions VALUES(?,?,?,?,?,?,?)",
                (root_id, ordinal, idempotency_key, disposition, digest, canonical_json(record), current.revision+1))
            self._append_commit("NON_ACCEPTANCE", f"{root_id}:{ordinal}", digest, current)
            self._commit()
            return disposition
        except BaseException as exc:
            self._rollback_economic(exc)

    def _verify_history(self) -> None:
        _schema_and_domain(self._conn, self.domain)
        receipts = [self._receipt_from_row(row) for row in self._rows("ORDER BY j.seq")]
        if (self._conn.execute("SELECT COUNT(*) FROM ledger_wallet_observations").fetchone()[0] != len(receipts)
                or self._conn.execute("SELECT COUNT(*) FROM ledger_baseline_journal").fetchone()[0] != len(receipts)):
            raise LedgerJournalError("LEDGER_ORPHAN_OR_MISSING_ORIGINAL_EVIDENCE")
        generations_raw = self._conn.execute("SELECT * FROM ledger_writer_generations ORDER BY generation").fetchall()
        generation_digests = {row[0]: row[2] for row in generations_raw}
        commits = {}
        last = ZERO_DIGEST
        for seq, kind, key, record_digest, generation, previous, digest, payload in self._conn.execute(
                "SELECT * FROM ledger_commits ORDER BY seq"):
            expected = {"version": COMMIT_VERSION, "economic_domain_id": self.domain.economic_domain_id,
                "sequence": len(commits)+1, "kind": kind, "record_key": key, "record_digest": record_digest,
                "generation": generation, "generation_digest": generation_digests.get(generation), "previous_digest": last}
            digest_value(record_digest)
            if (seq != len(commits)+1 or previous != last or kind not in _COMMIT_KINDS or generation not in generation_digests
                    or canonical_json(expected) != payload or content_fingerprint(expected) != digest):
                raise LedgerJournalError("LEDGER_COMMON_COMMIT_CHAIN_INVALID")
            commits[seq] = {**expected, "content_digest": digest}
            last = digest
        previous = ZERO_DIGEST
        prior_revision = 0
        generations = []
        for generation, prev, digest, payload in generations_raw:
            row = strict_json_object(payload)
            revision = row.get("revision_at_acquisition")
            if type(revision) is not int or not prior_revision <= revision <= len(commits):
                raise LedgerJournalError("LEDGER_GENERATION_REVISION_INVALID")
            cut = ZERO_DIGEST if revision == 0 else commits[revision]["content_digest"]
            expected = _generation_record(self.domain, len(generations)+1, previous, revision, cut)
            if (generation != len(generations)+1 or prev != previous or content_fingerprint(expected) != digest
                    or canonical_json(expected) != payload):
                raise LedgerJournalError("LEDGER_GENERATION_HISTORY_INVALID")
            generations.append((generation, revision))
            previous, prior_revision = digest, revision
        if not generations or self._head() != (len(generations), len(commits), last):
            raise LedgerJournalError("LEDGER_HEAD_RECONSTRUCTION_CONFLICT")
        for seq, commit in commits.items():
            applicable = [generation for generation, revision in generations if revision < seq]
            if not applicable or commit["generation"] != max(applicable):
                raise LedgerJournalError("LEDGER_COMMIT_WRITER_FENCE_INVALID")
        if sum(receipt.decision.disposition == "ESTABLISHED" for receipt in receipts) > 1:
            raise LedgerJournalError("LEDGER_MULTIPLE_OPENING_BASELINES")
        baseline_seq = next((receipt.sequence for receipt in receipts if receipt.decision.disposition == "ESTABLISHED"), None)
        baseline_slot = next((receipt.decision.context_slot for receipt in receipts if receipt.decision.disposition == "ESTABLISHED"), None)
        records = self._audit_economic_history(commits, baseline_seq, baseline_slot)
        for receipt in receipts:
            commit = commits.get(receipt.sequence)
            if (commit is None or receipt.sequence in records or receipt.previous_digest != commit["previous_digest"]
                    or receipt.generation != commit["generation"]):
                raise LedgerJournalError("LEDGER_ORIGINAL_WALLET_COMMIT_BINDING_INVALID")
            records[receipt.sequence] = ("WALLET", receipt.ingestion_key, receipt.content_digest)
        if set(records) != set(commits) or any(records[seq] != (item["kind"], item["record_key"], item["record_digest"])
                                               for seq, item in commits.items()):
            raise LedgerJournalError("LEDGER_ORPHAN_OR_CONFLICTING_CONCRETE_COMMIT")

    def _audit_economic_history(self, commits: dict, baseline_seq: int | None, baseline_slot: int | None) -> dict:
        """Full finite replay on open/audit; ordinary appends validate touched facts."""
        records, events, candidates, actions, attempts = {}, {}, {}, {}, {}

        def remember(seq, kind, key, digest, event):
            if seq in records or seq not in commits:
                raise LedgerJournalError("LEDGER_ECONOMIC_COMMIT_BINDING_INVALID")
            records[seq] = (kind, key, digest)
            events[seq] = (kind, event)

        for candidate_key, root, mint, digest, payload, seq in self._conn.execute("SELECT * FROM ledger_candidate_inbox"):
            item = candidate_from_json(payload)
            if (item.canonical_candidate_key, item.trade_root(self.domain), item.mint, item.content_digest) != (candidate_key, root, mint, digest):
                raise LedgerJournalError("LEDGER_INBOX_REPLAY_CONFLICT")
            candidates[root] = item
            remember(seq, "INBOX", root, digest, (root, item))
        for action_id, root, side, position, obligation, ordinal, digest, payload, seq in self._conn.execute("SELECT * FROM ledger_pending_actions"):
            item = action_from_json(payload)
            candidate = candidates.get(root)
            if ((item.action_id, item.root_id, item.side, item.position_id, item.obligation_id, item.ordinal, item.content_digest)
                    != (action_id, root, side, position, obligation, ordinal, digest) or candidate is None
                    or item.candidate_digest != candidate.content_digest or item.mint != candidate.mint
                    or candidate.winner_candidate_id != candidate.candidate_signal_id
                    or item.position_id != position_identity(self.domain, root, item.mint)
                    or item.side == "BUY" and item.claimed_entry_deadline_us < candidate.generated_at_us):
                raise LedgerJournalError("LEDGER_PENDING_ACTION_REPLAY_CONFLICT")
            actions[action_id] = item
            remember(seq, "ACTION", action_id, digest, item)
        for attempt_id, action_id, ordinal, message_digest, digest, payload, seq in self._conn.execute("SELECT * FROM ledger_attempts"):
            item = preparation_from_json(payload)
            action = actions.get(action_id)
            if ((item.attempt_id, item.action_id, item.ordinal, item.message_sha256, item.content_digest)
                    != (attempt_id, action_id, ordinal, message_digest, digest) or action is None
                    or item.action_content_digest != action.content_digest
                    or str(decode_message(item.message_hex).account_keys[0]) != self.domain.wallet
                    or item.finalized_lower_anchor.provider_fingerprint != self.domain.expected_profile_fingerprint
                    or baseline_slot is None or item.finalized_lower_anchor.slot < max(self.domain.minimum_context_slot, baseline_slot)
                    or utc_microseconds(item.prepared_at_utc) < candidates[action.root_id].generated_at_us
                    or action.side == "BUY" and utc_microseconds(item.prepared_at_utc) > action.claimed_entry_deadline_us):
                raise LedgerJournalError("LEDGER_ATTEMPT_PREPARATION_REPLAY_CONFLICT")
            attempts[attempt_id] = item
            remember(seq, "ATTEMPT", attempt_id, digest, item)
        for root, ordinal, key, disposition, digest, payload, seq in self._conn.execute("SELECT * FROM ledger_inbox_dispositions"):
            item = strict_json_object(payload)
            if (set(item) != {"version", "root_id", "ordinal", "idempotency_key", "from_disposition", "disposition",
                             "external_reference", "external_record_digest", "recorded_at_utc", "has_real_authority_grant"}
                    or item["version"] != "live_ledger_nonacceptance_v0.1" or item["has_real_authority_grant"] is not False
                    or (item["root_id"], item["ordinal"], item["idempotency_key"], item["disposition"]) != (root, ordinal, key, disposition)
                    or type(item["ordinal"]) is not int or disposition not in NON_ACCEPTANCE
                    or canonical_json(item) != payload or content_fingerprint(item) != digest):
                raise LedgerJournalError("LEDGER_INBOX_DISPOSITION_REPLAY_CONFLICT")
            public_reference(key)
            public_reference(item["external_reference"])
            digest_value(item["external_record_digest"])
            if ledger_utc(item["recorded_at_utc"]) != item["recorded_at_utc"]:
                raise LedgerJournalError("LEDGER_NONACCEPTANCE_TIME_INVALID")
            remember(seq, "NON_ACCEPTANCE", f"{root}:{ordinal}", digest, item)
        for attempt_id, revision, key, digest, payload, seq in self._conn.execute("SELECT * FROM ledger_attempt_stages"):
            item = strict_json_object(payload)
            if ((item["attempt_id"], item["revision"], item["idempotency_key"]) != (attempt_id, revision, key)
                    or canonical_json(item) != payload or content_fingerprint(item) != digest):
                raise LedgerJournalError("LEDGER_ATTEMPT_STAGE_REPLAY_CONFLICT")
            remember(seq, "ATTEMPT_STAGE", f"{attempt_id}:{revision}", digest, item)

        inbox, live_actions, live_attempts, per_action = {}, {}, {}, {}
        signatures = {}
        lane, lane_revision = None, 0
        for seq in sorted(events):
            kind, item = events[seq]
            if kind == "INBOX":
                root, _candidate = item
                inbox[root] = ("RECEIVED", 0, "")
            elif kind == "ACTION":
                if item.root_id not in inbox or inbox[item.root_id][0] in TERMINAL_INBOX or baseline_seq is None or seq <= baseline_seq:
                    raise LedgerJournalError("LEDGER_ACTION_BEFORE_BASELINE_OR_AFTER_TOMBSTONE")
                live_actions[item.action_id] = item
            elif kind == "NON_ACCEPTANCE":
                root = item["root_id"]
                state = inbox.get(root)
                if (state is None or state[0] in TERMINAL_INBOX or item["from_disposition"] != state[0]
                        or item["ordinal"] != state[1]+1 or item["recorded_at_utc"] < state[2]
                        or utc_microseconds(item["recorded_at_utc"]) < candidates[root].generated_at_us
                        or any(live_actions[value.preparation.action_id].root_id == root and value.recorded_stage != "CANCELLED_UNSIGNED"
                               for value in live_attempts.values())):
                    raise LedgerJournalError("LEDGER_NONACCEPTANCE_DESTROYS_UNRESOLVED_ATTEMPT_OR_TOMBSTONE")
                inbox[root] = (item["disposition"], item["ordinal"], item["recorded_at_utc"])
            elif kind == "ATTEMPT":
                action = live_actions.get(item.action_id)
                prior_ids = per_action.get(item.action_id, [])
                previous = None if not prior_ids else live_attempts[prior_ids[-1]]
                if (action is None or inbox[action.root_id][0] in TERMINAL_INBOX or lane is not None
                        or item.ordinal != len(prior_ids)+1
                        or previous is not None and (previous.recorded_stage != "CANCELLED_UNSIGNED"
                                                    or item.prepared_at_utc < previous.last_recorded_at_utc)):
                    raise LedgerJournalError("LEDGER_ATTEMPT_LANE_OR_ORDINAL_REPLAY_CONFLICT")
                live_attempts[item.attempt_id] = StoredAttempt(item, "PREPARED", 0, commits[seq]["generation"], item.prepared_at_utc, None, None, True)
                per_action.setdefault(item.action_id, []).append(item.attempt_id)
                lane, lane_revision = item.attempt_id, seq
            else:
                previous = live_attempts.get(item["attempt_id"])
                if (previous is None or lane != item["attempt_id"]
                        or inbox[live_actions[previous.preparation.action_id].root_id][0] in TERMINAL_INBOX):
                    raise LedgerJournalError("LEDGER_STAGE_AFTER_TOMBSTONE_OR_WITHOUT_LANE")
                value = self._replay_stage(previous, item, commits[seq]["generation"])
                if value.primary_signature is not None and previous.primary_signature is None:
                    if value.primary_signature in signatures:
                        raise LedgerJournalError("LEDGER_SIGNATURE_REUSED_BY_ANOTHER_ATTEMPT")
                    signatures[value.primary_signature] = (value.preparation.attempt_id, value.signed_wire_digest)
                live_attempts[item["attempt_id"]] = value
                lane, lane_revision = (item["attempt_id"] if value.lane_held else None), seq
        if self._lane() != (lane, lane_revision):
            raise LedgerJournalError("LEDGER_MUTATION_LANE_RECONSTRUCTION_CONFLICT")
        if {signature: (attempt_id, wire) for signature, attempt_id, wire in self._conn.execute("SELECT * FROM ledger_signature_bindings")} != signatures:
            raise LedgerJournalError("LEDGER_SIGNATURE_BINDINGS_RECONSTRUCTION_CONFLICT")
        return records

    def audit(self) -> dict:
        self._require_open()
        self._check_durability()
        self._verify_history()
        generation, revision, digest = self._head()
        return {"economic_domain_id": self.domain.economic_domain_id, "binding_digest": self.domain.binding_digest,
                "generation": generation, "revision": revision, "last_receipt_digest": digest,
                "sqlite_integrity": "ok", "foreign_key_violations": 0,
                "replayed_receipts": self._conn.execute("SELECT COUNT(*) FROM ledger_baseline_journal").fetchone()[0],
                "candidate_count": self._conn.execute("SELECT COUNT(*) FROM ledger_candidate_inbox").fetchone()[0],
                "action_count": self._conn.execute("SELECT COUNT(*) FROM ledger_pending_actions").fetchone()[0],
                "attempt_count": self._conn.execute("SELECT COUNT(*) FROM ledger_attempts").fetchone()[0]}

    def receipt(self, ingestion_key: str) -> LedgerWalletReceipt | None:
        self._require_open()
        public_label(ingestion_key)
        rows = self._rows("WHERE j.ingestion_key=?", (ingestion_key,))
        return None if not rows else self._receipt_from_row(rows[0])

    def baseline(self) -> LedgerWalletReceipt | None:
        self._require_open()
        rows = self._rows("WHERE j.baseline_domain_id=?", (self.domain.economic_domain_id,))
        return None if not rows else self._receipt_from_row(rows[0])

    def ingest_wallet_observation(self, observation: WalletObservation, *, ingestion_key: str,
                                 evaluated_at_utc: str, required_min_context_slot: int,
                                 fence: LedgerWriteFence) -> LedgerWalletReceipt:
        self._require_open()
        try:
            public_label(ingestion_key)
            evaluated_at_utc = ledger_utc(evaluated_at_utc)
            u64(required_min_context_slot)
            if type(fence) is not LedgerWriteFence:
                raise LedgerConflict("LEDGER_WRITE_FENCE_REQUIRED")
            original = wallet_observation_to_json(observation)
            restored = wallet_observation_from_json(original)
            if restored != observation:
                raise LedgerConflict("LEDGER_ORIGINAL_EVIDENCE_ROUNDTRIP_CONFLICT")
            current = self._begin_economic_write(fence)
            self._verify_history()
            rows = self._rows("WHERE j.ingestion_key=? OR j.evidence_digest=?", (ingestion_key, observation.content_digest))
            if rows:
                if len(rows) != 1:
                    raise LedgerConflict("LEDGER_INGESTION_IDENTITY_CONFLICT")
                existing = self._receipt_from_row(rows[0])
                if (existing.ingestion_key != ingestion_key or existing.observation != observation
                        or existing.decision.evaluated_at_utc != evaluated_at_utc
                        or existing.decision.required_min_context_slot != required_min_context_slot):
                    raise LedgerConflict("LEDGER_INGESTION_CONTENT_CONFLICT")
                # Exact retries are no-ops even when their original revision has
                # advanced. A superseded writer generation is never accepted.
                self._commit()
                return existing
            if fence != current:
                raise LedgerConflict("LEDGER_REVISION_CAS_FAILED")
            decision = adjudicate_opening_baseline(self.domain, restored,
                evaluated_at_utc=evaluated_at_utc, required_min_context_slot=required_min_context_slot)
            if decision.disposition == "ESTABLISHED" and self.baseline() is not None:
                raise LedgerConflict("LEDGER_OPENING_BASELINE_ALREADY_ESTABLISHED")
            receipt = LedgerWalletReceipt(current.revision+1, ingestion_key, current.last_receipt_digest,
                                           current.generation, current.generation_digest, restored, decision)
            self._conn.execute("INSERT INTO ledger_wallet_observations VALUES(?,?)", (restored.content_digest, original))
            self._conn.execute("INSERT INTO ledger_baseline_journal VALUES(?,?,?,?,?,?,?,?)",
                (receipt.sequence, ingestion_key, restored.content_digest, current.generation, current.last_receipt_digest,
                 receipt.content_digest, canonical_json(receipt.to_record()),
                 self.domain.economic_domain_id if decision.disposition == "ESTABLISHED" else None))
            self._append_commit("WALLET", ingestion_key, receipt.content_digest, current)
            self._commit()
            return receipt
        except BaseException as exc:
            if self._conn is not None and self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            if isinstance(exc, (LedgerContractError, LedgerEvidenceCodecError, KeyboardInterrupt, SystemExit)):
                raise
            raise LedgerJournalError("LEDGER_INGESTION_FAILED") from None

    def close(self) -> None:
        try:
            if self._conn is not None:
                conn, self._conn = self._conn, None
                conn.close()
        finally:
            if self._guard is not None:
                guard, self._guard = self._guard, None
                guard.close()

    def __enter__(self) -> LedgerRepository:
        self._require_open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
