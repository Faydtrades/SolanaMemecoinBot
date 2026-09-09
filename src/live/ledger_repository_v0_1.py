"""Dedicated, exclusively written LIVE opening-Evidence journal.

Initialize and reopen are deliberately different operations. The retained OS
lock excludes other cooperating Ledger writers; durable generations and revision
CAS fence stale callers. This is storage ownership, not Operations arming or a
heartbeat lease. No economic records are imported from another database.
"""
from __future__ import annotations

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

APPLICATION_ID = 0x4C454447
STORAGE_VERSION = 1
RECEIPT_VERSION = "live_ledger_wallet_receipt_v0.1"
GENERATION_VERSION = "live_ledger_writer_generation_v0.1"
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
}
for _table in ("ledger_domain", "ledger_writer_generations", "ledger_wallet_observations", "ledger_baseline_journal"):
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
        self._conn.execute("COMMIT")

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

    def _verify_history(self) -> None:
        _schema_and_domain(self._conn, self.domain)
        receipts = [self._receipt_from_row(row) for row in self._rows("ORDER BY j.seq")]
        if (self._conn.execute("SELECT COUNT(*) FROM ledger_wallet_observations").fetchone()[0] != len(receipts)
                or self._conn.execute("SELECT COUNT(*) FROM ledger_baseline_journal").fetchone()[0] != len(receipts)):
            raise LedgerJournalError("LEDGER_ORPHAN_OR_MISSING_ORIGINAL_EVIDENCE")
        last = ZERO_DIGEST
        for seq, receipt in enumerate(receipts, 1):
            if receipt.sequence != seq or receipt.previous_digest != last:
                raise LedgerJournalError("LEDGER_RECEIPT_CHAIN_INVALID")
            last = receipt.content_digest
        previous = ZERO_DIGEST
        prior_revision = 0
        generations = []
        for generation, prev, digest, payload in self._conn.execute(
                "SELECT * FROM ledger_writer_generations ORDER BY generation"):
            row = strict_json_object(payload)
            revision = row.get("revision_at_acquisition")
            if type(revision) is not int or not prior_revision <= revision <= len(receipts):
                raise LedgerJournalError("LEDGER_GENERATION_REVISION_INVALID")
            cut = ZERO_DIGEST if revision == 0 else receipts[revision-1].content_digest
            expected = _generation_record(self.domain, len(generations)+1, previous, revision, cut)
            if (generation != len(generations)+1 or prev != previous or content_fingerprint(expected) != digest
                    or canonical_json(expected) != payload):
                raise LedgerJournalError("LEDGER_GENERATION_HISTORY_INVALID")
            generations.append((generation, revision))
            previous, prior_revision = digest, revision
        if not generations or self._head() != (len(generations), len(receipts), last):
            raise LedgerJournalError("LEDGER_HEAD_RECONSTRUCTION_CONFLICT")
        for receipt in receipts:
            applicable = [generation for generation, revision in generations if revision < receipt.sequence]
            if not applicable or receipt.generation != max(applicable):
                raise LedgerJournalError("LEDGER_RECEIPT_WRITER_FENCE_INVALID")
        if sum(receipt.decision.disposition == "ESTABLISHED" for receipt in receipts) > 1:
            raise LedgerJournalError("LEDGER_MULTIPLE_OPENING_BASELINES")

    def audit(self) -> dict:
        self._require_open()
        self._check_durability()
        self._verify_history()
        generation, revision, digest = self._head()
        return {"economic_domain_id": self.domain.economic_domain_id, "binding_digest": self.domain.binding_digest,
                "generation": generation, "revision": revision, "last_receipt_digest": digest,
                "sqlite_integrity": "ok", "foreign_key_violations": 0, "replayed_receipts": revision}

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
            self._check_durability()
            self._conn.execute("BEGIN IMMEDIATE")
            self._verify_history()
            current = self.write_fence()
            if (fence.economic_domain_id != current.economic_domain_id or fence.generation != current.generation
                    or fence.generation_digest != current.generation_digest):
                raise LedgerConflict("LEDGER_WRITER_GENERATION_FENCED")
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
            updated = self._conn.execute("UPDATE ledger_head SET revision=?,last_receipt_digest=? WHERE singleton=1 "
                "AND generation=? AND revision=? AND last_receipt_digest=?",
                (receipt.sequence, receipt.content_digest, current.generation, current.revision, current.last_receipt_digest))
            if updated.rowcount != 1:
                raise LedgerConflict("LEDGER_ATOMIC_HEAD_CAS_FAILED")
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
