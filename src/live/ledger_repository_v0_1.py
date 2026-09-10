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
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
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
from .ledger_chain_codec_v0_1 import chain_observation_to_json, chain_observation_from_json
from .ledger_finality_v0_1 import (
    AttemptFinality, RetainedChainFacts, LedgerChainReceipt, chain_receipt_from_record, adjudicate_chain_observation,
)
from .ledger_actions_v0_1 import (
    CandidateInboxInput, PendingAction, AttemptPreparation, AttemptStageInput, StoredAttempt,
    PENDING_ADMISSION, NON_ACCEPTANCE, TERMINAL_INBOX, candidate_from_json, action_from_json,
    preparation_from_json, stage_from_record, position_identity, public_reference,
    decode_message, validate_stage_transition, utc_microseconds,
)
from .ledger_settlement_v0_1 import WalletSupportInput
from .ledger_custody_v0_1 import (
    CustodyState, ApplicationDecision, WalletComparison, baseline_custody, adjudicate_application,
    compare_wallet, retain_chain_custody, REPLACEABLE,
    FundingView, PositionView, ComparisonView,
)
from .transaction_evidence_v0_1 import _known_finalized_anchors_consistent
from .ledger_ports_v0_1 import (
    require_admitted_action, ConsumerCut, AdmissionInput, ProtectiveHandoffInput, RetirementInput, DryTerminalInput, ConsumerPortReceipt,
    GROUP_KINDS, admission_from_record, handoff_from_record, retirement_from_record, dry_terminal_from_record,
    port_receipt_from_record, require_common_cut, admit_reservation, activate_protection, retire_reservation, terminate_dry,
    reservation_identity,
)

from .authority_controls_v0_1 import (
    AuthorityState, AuthorityReceipt, ControlCommand, TrustedClockSample, EligibilityInput, EntryBinding,
    apply_control, evaluate_entry, authority_receipt_from_record, command_from_record, clock_from_record,
)
from .evidence_store_v0_1 import SourceEvidenceStore
from .authority_admission_v0_1 import (
    EntryRequest, AuthorityAdmissionReceipt, admission_receipt_from_record, assess_entry_risk, admission_terms,
)

APPLICATION_ID = 0x4C454447
STORAGE_VERSION = 7
RECEIPT_VERSION = "live_ledger_wallet_receipt_v0.1"
GENERATION_VERSION = "live_ledger_writer_generation_v0.1"
COMMIT_VERSION = "live_ledger_concrete_commit_v0.1"
_COMMIT_KINDS = frozenset(("WALLET", "INBOX", "NON_ACCEPTANCE", "ACTION", "ATTEMPT", "ATTEMPT_STAGE", "CHAIN_EVIDENCE",
                 "SETTLEMENT_APPLICATION", "WALLET_COMPARISON", "AUTHORITY_CONTROL", "AUTHORITY_ELIGIBILITY", "AUTHORITY_ADMISSION", *GROUP_KINDS))
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
    "ledger_chain_observations": """CREATE TABLE ledger_chain_observations (
        evidence_digest TEXT PRIMARY KEY NOT NULL, payload_json TEXT NOT NULL)""",
    "ledger_chain_receipts": """CREATE TABLE ledger_chain_receipts (
        commit_seq INTEGER PRIMARY KEY REFERENCES ledger_commits(seq) DEFERRABLE INITIALLY DEFERRED,
        ingestion_key TEXT NOT NULL UNIQUE, attempt_id TEXT NOT NULL REFERENCES ledger_attempts(attempt_id),
        evidence_revision INTEGER NOT NULL CHECK(evidence_revision>=1),
        evidence_digest TEXT NOT NULL UNIQUE REFERENCES ledger_chain_observations(evidence_digest),
        receipt_digest TEXT NOT NULL UNIQUE, payload_json TEXT NOT NULL,
        UNIQUE(attempt_id,evidence_revision))""",
    "ledger_custody_inputs": """CREATE TABLE ledger_custody_inputs (
        input_digest TEXT PRIMARY KEY NOT NULL, payload_json TEXT NOT NULL)""",
    "ledger_application_receipts": """CREATE TABLE ledger_application_receipts (
        commit_seq INTEGER PRIMARY KEY REFERENCES ledger_commits(seq) DEFERRABLE INITIALLY DEFERRED,
        ingestion_key TEXT NOT NULL UNIQUE, attempt_id TEXT NOT NULL REFERENCES ledger_attempts(attempt_id),
        chain_receipt_key TEXT NOT NULL REFERENCES ledger_chain_receipts(ingestion_key),
        input_digest TEXT REFERENCES ledger_custody_inputs(input_digest), receipt_digest TEXT NOT NULL UNIQUE,
        payload_json TEXT NOT NULL)""",
    "ledger_wallet_comparisons": """CREATE TABLE ledger_wallet_comparisons (
        commit_seq INTEGER PRIMARY KEY REFERENCES ledger_commits(seq) DEFERRABLE INITIALLY DEFERRED,
        ingestion_key TEXT NOT NULL UNIQUE, input_digest TEXT NOT NULL REFERENCES ledger_custody_inputs(input_digest),
        receipt_digest TEXT NOT NULL UNIQUE, payload_json TEXT NOT NULL)""",
    "ledger_economic_postings": """CREATE TABLE ledger_economic_postings (
        component_id TEXT PRIMARY KEY NOT NULL, application_seq INTEGER NOT NULL REFERENCES ledger_application_receipts(commit_seq),
        content_digest TEXT NOT NULL, payload_json TEXT NOT NULL)""",
    "ledger_funding_projection": """CREATE TABLE ledger_funding_projection (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), content_digest TEXT NOT NULL, payload_json TEXT NOT NULL)""",
    "ledger_account_projection": """CREATE TABLE ledger_account_projection (
        pubkey TEXT PRIMARY KEY NOT NULL, content_digest TEXT NOT NULL, payload_json TEXT NOT NULL)""",
    "ledger_position_projection": """CREATE TABLE ledger_position_projection (
        position_id TEXT PRIMARY KEY NOT NULL, content_digest TEXT NOT NULL, payload_json TEXT NOT NULL)""",
    "ledger_resolution_projection": """CREATE TABLE ledger_resolution_projection (
        attempt_id TEXT PRIMARY KEY NOT NULL REFERENCES ledger_attempts(attempt_id), signature TEXT NOT NULL UNIQUE,
        content_digest TEXT NOT NULL, payload_json TEXT NOT NULL)""",
    "ledger_authority_records": """CREATE TABLE ledger_authority_records (
        commit_seq INTEGER PRIMARY KEY REFERENCES ledger_commits(seq) DEFERRABLE INITIALLY DEFERRED,
        command_id TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, policy_id TEXT UNIQUE, grant_id TEXT UNIQUE,
        receipt_digest TEXT NOT NULL UNIQUE, payload_json TEXT NOT NULL)""",
    "ledger_authority_bindings": """CREATE TABLE ledger_authority_bindings (
        root_id TEXT PRIMARY KEY NOT NULL REFERENCES ledger_candidate_inbox(root_id),
        first_sequence INTEGER NOT NULL UNIQUE REFERENCES ledger_authority_records(commit_seq),
        content_digest TEXT NOT NULL, payload_json TEXT NOT NULL)""",
    "ledger_authority_admissions": """CREATE TABLE ledger_authority_admissions (
        commit_seq INTEGER PRIMARY KEY REFERENCES ledger_commits(seq) DEFERRABLE INITIALLY DEFERRED,
        command_id TEXT NOT NULL UNIQUE, root_id TEXT NOT NULL REFERENCES ledger_candidate_inbox(root_id),
        accepted_root TEXT UNIQUE REFERENCES ledger_candidate_inbox(root_id), accepted_mint TEXT UNIQUE,
        consumed_grant_id TEXT UNIQUE REFERENCES ledger_authority_records(grant_id),
        receipt_digest TEXT NOT NULL UNIQUE, payload_json TEXT NOT NULL)""",
    "ledger_authority_projection": """CREATE TABLE ledger_authority_projection (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), content_digest TEXT NOT NULL, payload_json TEXT NOT NULL)""",
    "ledger_consumer_groups": """CREATE TABLE ledger_consumer_groups (
        commit_seq INTEGER PRIMARY KEY REFERENCES ledger_commits(seq) DEFERRABLE INITIALLY DEFERRED,
        ingestion_key TEXT NOT NULL UNIQUE, kind TEXT NOT NULL,
        admission_root TEXT UNIQUE REFERENCES ledger_candidate_inbox(root_id),
        protection_position TEXT UNIQUE, retired_reservation TEXT UNIQUE, dry_action TEXT UNIQUE REFERENCES ledger_pending_actions(action_id),
        receipt_digest TEXT NOT NULL UNIQUE, payload_json TEXT NOT NULL)""",
}
for _table in ("ledger_domain", "ledger_writer_generations", "ledger_wallet_observations", "ledger_baseline_journal",
               "ledger_commits", "ledger_candidate_inbox", "ledger_inbox_dispositions", "ledger_pending_actions",
               "ledger_attempts", "ledger_attempt_stages", "ledger_signature_bindings",
               "ledger_chain_observations", "ledger_chain_receipts", "ledger_custody_inputs", "ledger_application_receipts",
               "ledger_wallet_comparisons", "ledger_economic_postings", "ledger_consumer_groups",
               "ledger_authority_records", "ledger_authority_bindings", "ledger_authority_admissions"):
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


@dataclass(frozen=True, slots=True)
class LedgerApplicationReceipt:
    sequence: int
    ingestion_key: str
    attempt_id: str
    chain_receipt_key: str
    chain_receipt_digest: str
    support: WalletSupportInput | None
    recorded_at_utc: str
    common_revision: int
    common_digest: str
    decision: ApplicationDecision

    def to_record(self):
        return {"version": "live_ledger_application_receipt_v0.1", "sequence": self.sequence,
            "ingestion_key": self.ingestion_key, "attempt_id": self.attempt_id, "chain_receipt_key": self.chain_receipt_key,
            "chain_receipt_digest": self.chain_receipt_digest, "input_digest": None if self.support is None else self.support.digest,
            "recorded_at_utc": self.recorded_at_utc, "common_revision": self.common_revision, "common_digest": self.common_digest,
            "decision": self.decision.to_record()}

    @property
    def content_digest(self):
        return content_fingerprint(self.to_record())


@dataclass(frozen=True, slots=True)
class LedgerComparisonReceipt:
    sequence: int
    ingestion_key: str
    support: WalletSupportInput
    comparison: WalletComparison
    prior_custody_digest: str
    resulting_custody_digest: str

    def to_record(self):
        return {"version": "live_ledger_comparison_receipt_v0.1", "sequence": self.sequence,
            "ingestion_key": self.ingestion_key, "input_digest": self.support.digest, "comparison": asdict(self.comparison),
            "prior_custody_digest": self.prior_custody_digest, "resulting_custody_digest": self.resulting_custody_digest}

    @property
    def content_digest(self):
        return content_fingerprint(self.to_record())


def _support_json(support):
    if type(support) is not WalletSupportInput:
        raise LedgerConflict("LEDGER_ORIGINAL_WALLET_SUPPORT_REQUIRED")
    return canonical_json({"version": "live_ledger_custody_wallet_input_v0.1", "input_digest": support.digest,
        "evaluated_at_utc": support.evaluated_at_utc, "required_min_context_slot": support.required_min_context_slot,
        "observation": strict_json_object(wallet_observation_to_json(support.observation))})


def _support_from_json(payload):
    record = strict_json_object(payload)
    if set(record) != {"version", "input_digest", "evaluated_at_utc", "required_min_context_slot", "observation"}:
        raise LedgerConflict("LEDGER_WALLET_SUPPORT_CODEC_SHAPE")
    value = WalletSupportInput(wallet_observation_from_json(canonical_json(record["observation"])),
                               record["evaluated_at_utc"], record["required_min_context_slot"])
    if _support_json(value) != payload:
        raise LedgerConflict("LEDGER_WALLET_SUPPORT_CODEC_CONFLICT")
    return value


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
        self._custody = CustodyState(domain.economic_domain_id)
        self._applications = {}
        self._comparisons = {}
        self._authority = AuthorityState(domain.economic_domain_id)
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
                self._store_custody_projection(self._custody)
                self._store_authority_projection(self._authority)
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
        # Publish derived chain facts before blessing the new trusted head.
        # A lost acknowledgement must never leave a trusted head with old facts.
        pending_chain = getattr(self, "_pending_chain_facts", None)
        if pending_chain is not None:
            self._chain_facts = pending_chain
            self._pending_chain_facts = None
        pending_custody = getattr(self, "_pending_custody_bundle", None)
        if pending_custody is not None:
            self._custody, self._applications, self._comparisons = pending_custody
            self._pending_custody_bundle = None
        pending_authority = getattr(self, "_pending_authority", None)
        if pending_authority is not None:
            self._authority = pending_authority
            self._pending_authority = None
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
        self._pending_custody_bundle = None
        self._pending_authority = None
        if self._conn is not None and self._conn.in_transaction:
            self._conn.execute("ROLLBACK")
        if isinstance(exc, (LedgerContractError, LedgerEvidenceCodecError, KeyboardInterrupt, SystemExit)):
            raise exc
        raise LedgerJournalError("LEDGER_ECONOMIC_STORAGE_FAILED") from None

    @contextmanager
    def _trusted_read(self):
        """One finite, coherent consumer cut; outside commits need explicit reopen."""
        self._require_open()
        own = not self._conn.in_transaction
        if own:
            self._conn.execute("BEGIN")
        try:
            if (self._head() != self._verified_head
                    or self._conn.execute("PRAGMA data_version").fetchone()[0] != self._verified_data_version
                    or self._conn.execute("PRAGMA schema_version").fetchone()[0] != self._verified_schema_cookie):
                raise LedgerConflict("LEDGER_CONSUMER_REQUIRES_VERIFIED_COMMON_CUT")
            yield
        finally:
            if own and self._conn.in_transaction:
                self._conn.execute("ROLLBACK")

    def consumer_snapshot(self, *, max_positions=1024, max_accounts=2048, max_reservations=2048):
        with self._trusted_read():
            # L6 may mark lawful retirement; historical positions/resolutions
            # remain addressable and never consume a lifetime consumer limit.
            positions = tuple(item for item in self._custody.positions if item.status != "RETIRED")
            needed = {item.account for item in positions}
            accounts = tuple(item for item in self._custody.accounts if item.presence == "PRESENT" or item.pubkey in needed
                             or item.mint == "So11111111111111111111111111111111111111112")
            reservations = tuple(item for item in self._custody.reservations if item.status != "RETIRED")
            if (type(max_positions) is not int or type(max_accounts) is not int or not 1 <= max_positions <= 4096
                    or not 1 <= max_accounts <= 8192 or len(positions) > max_positions or len(accounts) > max_accounts
                    or type(max_reservations) is not int or not 1 <= max_reservations <= 8192 or len(reservations) > max_reservations):
                raise LedgerConflict("LEDGER_CONSUMER_SNAPSHOT_BOUND_EXCEEDED")
            state = self._custody
            value = state.latest_comparison
            comparison = None if value is None else ComparisonView(value.input_digest, value.evidence_digest, value.evaluated_at_utc,
                value.required_min_context_slot, value.context_slot, value.profile_fingerprint, value.anchor, value.common_revision,
                value.common_digest, value.pending_attempts, value.disposition, value.reasons, len(value.differences),
                value.target_custody_digest, value.effects_through_sequence, value.recognized_wallet_context_floor)
            fence = self.write_fence()
            return {"fence": fence, "custody_digest": state.content_digest, "mode": self.domain.mode,
                    "consumer_cut": ConsumerCut(self.domain.economic_domain_id, fence.revision, fence.last_receipt_digest, state.content_digest),
                    "funding": FundingView(state.economic_domain_id, state.baseline_receipt_digest, state.known_initial_native_lamports,
                        state.native_lamports, state.network_fees_paid_lamports, state.venue_fees_paid_sol_lamports,
                        state.venue_fees_paid_wsol_units, state.external_setup_paid_lamports, state.anchor, state.last_effect_sequence,
                        state.quarantine_reasons),
                    "positions": tuple(PositionView(item.position_id, item.root_id, item.mint, item.token_program, item.account,
                        item.acquired_units, item.sold_units, item.remaining_units, item.acquisition_signature, item.status,
                        item.usable and not state.quarantined, item.has_protective_handoff, item.protective_binding_id) for item in positions),
                    "reservations": reservations,
                    "protections": tuple(item for item in state.protections if any(position.position_id == item.handoff.position_id for position in positions)),
                    "accounts": accounts, "pending_attempts": self._pending_attempts(), "latest_comparison": comparison,
                    "required_wallet_context_slot": max(state.latest_usable_wallet_context_slot, state.latest_qualified_wallet_anchor_slot, 0 if state.anchor is None else state.anchor.slot),
                    "comparison_is_historical": True, "authority_current": False,
                    "requires_current_external_consumer_validation": True}

    def position_history(self, position_id):
        with self._trusted_read():
            digest_value(position_id)
            return next((item for item in self._custody.positions if item.position_id == position_id), None)

    def _pending_attempts(self):
        active = self._lane()[0]
        return () if active is None else (active,)

    @staticmethod
    def _projection_rows(state):
        funding = state.to_record()
        accounts, positions, resolutions = funding.pop("accounts"), funding.pop("positions"), funding.pop("resolutions")
        def row(value):
            return content_fingerprint(value), canonical_json(value)
        return ((1, *row(funding)), [(item["pubkey"], *row(item)) for item in accounts],
                [(item["position_id"], *row(item)) for item in positions],
                [(item["attempt_id"], item["signature"], *row(item)) for item in resolutions])

    def _store_authority_projection(self, state):
        self._conn.execute("INSERT INTO ledger_authority_projection VALUES(1,?,?) ON CONFLICT(singleton) "
            "DO UPDATE SET content_digest=excluded.content_digest,payload_json=excluded.payload_json",
            (state.content_digest, canonical_json(asdict(state))))

    def _authority_receipt_row(self, row):
        seq, key, kind, policy_id, grant_id, digest, payload = row
        try:
            item = authority_receipt_from_record(strict_json_object(payload))
            command = item.original if kind == "AUTHORITY_CONTROL" else None
            expected_policy = None if command is None or command.policy is None else command.policy.policy_id
            expected_grant = None if command is None or command.grant is None else command.grant.grant_id
            if (item.sequence, item.command_id, item.kind, expected_policy, expected_grant, item.content_digest,
                    canonical_json(asdict(item))) != (seq, key, kind, policy_id, grant_id, digest, payload):
                raise ValueError
            public_reference(key)
            return item
        except Exception:
            raise LedgerConflict("AUTHORITY_ORIGINAL_RECORD_INVALID") from None

    def authority_receipt(self, command_id):
        with self._trusted_read():
            public_reference(command_id)
            row = self._conn.execute("SELECT * FROM ledger_authority_records WHERE command_id=?", (command_id,)).fetchone()
            return None if row is None else self._authority_receipt_row(row)

    def authority_policy(self, policy_id):
        with self._trusted_read():
            public_reference(policy_id)
            row = self._conn.execute("SELECT * FROM ledger_authority_records WHERE policy_id=?", (policy_id,)).fetchone()
            return None if row is None else self._authority_receipt_row(row).original.policy

    def authority_grant(self, grant_id):
        with self._trusted_read():
            public_reference(grant_id)
            row = self._conn.execute("SELECT * FROM ledger_authority_records WHERE grant_id=?", (grant_id,)).fetchone()
            return None if row is None else self._authority_receipt_row(row).original.grant

    def authority_entry_binding(self, root_id):
        with self._trusted_read():
            digest_value(root_id)
            row = self._conn.execute("SELECT payload_json FROM ledger_authority_bindings WHERE root_id=?", (root_id,)).fetchone()
            return None if row is None else EntryBinding(**strict_json_object(row[0]))

    def authority_snapshot(self):
        with self._trusted_read():
            # Control state is immutable; a snapshot is never current message
            # permission. Protection is independently bound by Ledger positions.
            state = self._authority
            return {"fence": self.write_fence(), "state_digest": state.content_digest,
                "policy": state.policy, "armed_entry_grant": state.grant,
                "entry_stop_command": state.entry_stop_command, "hard_stop_command": state.hard_stop_command,
                "last_qualified_clock": state.last_qualified_clock,
                "clock_requires_reconciliation": state.clock_requires_reconciliation,
                "current_source_checkpoint": next((item for item in state.source_checkpoints if state.policy is not None
                    and (item.source_identity, item.profile_fingerprint) == (state.policy.source_identity, state.policy.source_profile_fingerprint)), None),
                "authority_current": False, "grants_message_permission": False,
                "protection_requires_position_obligation_and_current_resource_validation": True}

    def authority_candidate_economics(self, root_id):
        """One guarded concrete cut for A2/A3; historical admission stays exact."""
        with self._trusted_read():
            candidate = self.candidate(root_id)
            if candidate is None:
                raise LedgerConflict("AUTHORITY_CANONICAL_INBOX_REQUIRED")
            row = self._conn.execute("SELECT action_id FROM ledger_pending_actions WHERE root_id=? AND side='BUY'", (root_id,)).fetchone()
            action = None if row is None else self.action(row[0])
            reservation = self.reservation(root_id)
            admission = None if reservation is None else self._port_at_sequence(reservation.sequence)
            if admission is not None and (admission.admission is None or admission.admission.action != action):
                raise LedgerConflict("AUTHORITY_ORIGINAL_ADMITTED_TERMS_CONFLICT")
            fence = self.write_fence()
            return {"domain": self.domain, "candidate": candidate, "inbox_disposition": self.inbox_disposition(root_id),
                "entry_binding": self.authority_entry_binding(root_id), "policy": self._authority.policy,
                "authority_state_digest": self._authority.content_digest, "staged_action": action,
                "admitted_action": None if admission is None else action, "admission_receipt": admission,
                "fence": fence, "cut": ConsumerCut(self.domain.economic_domain_id, fence.revision,
                    fence.last_receipt_digest, self._custody.content_digest), "grants_message_permission": False}

    def _insert_authority_receipt(self, receipt, updated):
        """One closed Authority child; caller owns its common SQL transaction."""
        if not self._conn.in_transaction:
            raise LedgerConflict("AUTHORITY_RECORD_REQUIRES_OWNED_LEDGER_TRANSACTION")
        # Enforce the same bounded finite original codec BEFORE any insertion.
        # An oversized source cut denies persistence; it must never truncate or
        # commit a receipt which the original-history reader cannot reconstruct.
        payload = canonical_json(asdict(receipt))
        if authority_receipt_from_record(strict_json_object(payload)) != receipt:
            raise LedgerConflict("AUTHORITY_ORIGINAL_RECEIPT_ROUNDTRIP_CONFLICT")
        command = receipt.original if receipt.kind == "AUTHORITY_CONTROL" else None
        self._conn.execute("INSERT INTO ledger_authority_records VALUES(?,?,?,?,?,?,?)", (
            receipt.sequence, receipt.command_id, receipt.kind,
            None if command is None or command.policy is None else command.policy.policy_id,
            None if command is None or command.grant is None else command.grant.grant_id,
            receipt.content_digest, payload))
        if receipt.decision is not None and self.authority_entry_binding(receipt.original.root_id) is None:
            binding = receipt.decision.binding
            self._conn.execute("INSERT INTO ledger_authority_bindings VALUES(?,?,?,?)", (binding.root_id,
                receipt.sequence, content_fingerprint(asdict(binding)), canonical_json(asdict(binding))))
        self._store_authority_projection(updated)
        self._pending_authority = updated

    def record_authority_control(self, command, *, fence):
        try:
            if type(command) is not ControlCommand:
                raise LedgerConflict("AUTHORITY_EXACT_CONTROL_REQUIRED")
            command = command_from_record(strict_json_object(canonical_json(asdict(command))))
            current = self._begin_economic_write(fence)
            existing = self.authority_receipt(command.command_id)
            if existing is not None:
                if existing.kind != "AUTHORITY_CONTROL" or existing.original != command:
                    raise LedgerConflict("AUTHORITY_COMMAND_ID_CONTENT_CONFLICT")
                self._commit()
                return existing
            self._new_revision(fence, current)
            if command.grant is not None and command.grant.root_id is not None and self.candidate(command.grant.root_id) is None:
                raise LedgerConflict("AUTHORITY_GRANT_UNKNOWN_ROOT")
            updated = apply_control(self.domain, self._authority, command,
                known_policy=None if command.policy is None else self.authority_policy(command.policy.policy_id),
                known_grant=None if command.grant is None and command.target_id is None else
                    self.authority_grant(command.target_id if command.grant is None else command.grant.grant_id))
            receipt = AuthorityReceipt(current.revision+1, command.command_id, "AUTHORITY_CONTROL", command, None,
                self._authority.content_digest, updated.content_digest, current.last_receipt_digest)
            self._insert_authority_receipt(receipt, updated)
            self._append_commit(receipt.kind, receipt.command_id, receipt.content_digest, current)
            self._commit()
            return receipt
        except BaseException as exc:
            self._rollback_economic(exc)

    def evaluate_authority_entry(self, root_id, track, clock, source_store, *, command_id, fence):
        """Capture latest source knowledge, never accept a caller-selected verdict.

        Exact retries return historical receipts. Every NEW evaluation selects
        latest_record again; later authority stages must do the same.
        """
        try:
            public_reference(command_id)
            if type(clock) is not TrustedClockSample or type(source_store) is not SourceEvidenceStore:
                raise LedgerConflict("AUTHORITY_CLOCK_AND_ACTUAL_SOURCE_STORE_REQUIRED")
            clock = clock_from_record(strict_json_object(canonical_json(asdict(clock))))
            current = self._begin_economic_write(fence)
            existing = self.authority_receipt(command_id)
            if existing is not None:
                if existing.kind != "AUTHORITY_ELIGIBILITY" or (existing.original.root_id, existing.original.track,
                        existing.original.clock) != (root_id, track, clock):
                    raise LedgerConflict("AUTHORITY_COMMAND_ID_CONTENT_CONFLICT")
                self._commit()
                return existing
            self._new_revision(fence, current)
            candidate = self.candidate(root_id)
            if candidate is None:
                raise LedgerConflict("AUTHORITY_CANONICAL_INBOX_REQUIRED")
            current_source = source_store.latest_record()
            old = next((item for item in self._authority.source_checkpoints if
                (item.source_identity, item.profile_fingerprint) == (source_store.binding.source_identity, source_store.profile.fingerprint)), None)
            prior_record = None if old is None else source_store.read_record(old.sequence)
            supplied = EligibilityInput(root_id, candidate.content_digest, track, clock,
                None if current_source is None else current_source[0], None if current_source is None else current_source[1],
                None if prior_record is None else prior_record[1].content_digest)
            decision, updated = evaluate_entry(self.domain, self._authority, candidate, self.inbox_disposition(root_id), supplied,
                self.authority_entry_binding(root_id))
            receipt = AuthorityReceipt(current.revision+1, command_id, "AUTHORITY_ELIGIBILITY", supplied, decision,
                self._authority.content_digest, updated.content_digest, current.last_receipt_digest)
            self._insert_authority_receipt(receipt, updated)
            self._append_commit(receipt.kind, command_id, receipt.content_digest, current)
            self._commit()
            return receipt
        except BaseException as exc:
            self._rollback_economic(exc)

    @staticmethod
    def _authority_admission_row(receipt):
        return (receipt.sequence, receipt.command_id, receipt.request.root_id,
            receipt.request.root_id if receipt.accepted else None, receipt.mint if receipt.accepted else None,
            receipt.consumed_grant_id, receipt.content_digest, canonical_json(asdict(receipt)))

    def _read_authority_admission_row(self, row):
        value = admission_receipt_from_record(strict_json_object(row[-1]))
        if self._authority_admission_row(value) != row:
            raise LedgerConflict("AUTHORITY_ADMISSION_RECORD_LINK_CONFLICT")
        return value

    def authority_admission_receipt(self, command_id):
        """Historical exact receipt, never current message permission."""
        with self._trusted_read():
            public_reference(command_id)
            row = self._conn.execute("SELECT * FROM ledger_authority_admissions WHERE command_id=?", (command_id,)).fetchone()
            return None if row is None else self._read_authority_admission_row(row)

    def authority_acceptance(self, root_id):
        with self._trusted_read():
            digest_value(root_id)
            row = self._conn.execute("SELECT * FROM ledger_authority_admissions WHERE accepted_root=?", (root_id,)).fetchone()
            return None if row is None else self._read_authority_admission_row(row)

    def authority_grant_status(self, grant_id):
        with self._trusted_read():
            issuance = self.authority_grant(grant_id)
            spent = self._conn.execute("SELECT command_id,accepted_root FROM ledger_authority_admissions WHERE consumed_grant_id=?", (grant_id,)).fetchone()
            revoked = any(self._authority_receipt_row(row).original.operation == "REVOKE_GRANT"
                and self._authority_receipt_row(row).original.target_id == grant_id for row in self._conn.execute(
                    "SELECT * FROM ledger_authority_records WHERE kind='AUTHORITY_CONTROL'"))
            return {"issuance": issuance, "consumed_by_command": None if spent is None else spent[0],
                "consumed_root": None if spent is None else spent[1], "revoked": revoked,
                "currently_selected": self._authority.grant is not None and self._authority.grant.grant_id == grant_id,
                "grants_message_permission": False}

    def _authority_risk_history(self, request, candidate):
        # Read original accepted records only for current roots or the exact
        # candidate/mint tombstone. Lifetime source Evidence is not decoded for
        # every new economic append.
        roots = {item.root_id for item in self._custody.reservations if item.retired_sequence is None} | {request.root_id}
        accepted = {}
        for root in sorted(roots):
            value = self.authority_acceptance(root)
            if value is not None:
                accepted[root] = value
        row = self._conn.execute("SELECT * FROM ledger_authority_admissions WHERE accepted_mint=?", (candidate.mint,)).fetchone()
        if row is not None:
            value = self._read_authority_admission_row(row)
            accepted[value.request.root_id] = value
        policy_digests = {value.eligibility.decision.policy_digest for value in accepted.values()}
        policies = {}
        for digest in policy_digests:
            reservation = next((item for item in self._custody.reservations if item.root_id in accepted
                and item.admission.action.policy_digest == digest), None)
            if reservation is not None:
                policy = self.authority_policy(reservation.admission.action.policy_ref)
                if policy is None or policy.content_digest != digest:
                    raise LedgerConflict("AUTHORITY_ACCEPTED_POLICY_HISTORY_MISSING")
                policies[digest] = policy
        actions = {app.decision.proposal.action_id: self.action(app.decision.proposal.action_id)
                   for app in self._applications.values() if app.decision.proposal is not None}
        spent = set()
        grant = self._authority.grant
        if grant is not None and self._conn.execute("SELECT 1 FROM ledger_authority_admissions WHERE consumed_grant_id=?", (grant.grant_id,)).fetchone():
            spent.add(grant.grant_id)
        return accepted, policies, actions, spent

    def _authority_nonacceptance(self, root, decision, risk, *, command_id, current):
        # Unknown time is not positive expiry/nonacceptance proof. A root with
        # an encumbrance or unresolved attempt uses its lawful later disposition.
        at = decision.original.clock.utc_lower_utc
        old = self.inbox_disposition(root)
        if (decision.decision.clock_reasons or utc_microseconds(at) < self.candidate(root).generated_at_us
                or self.reservation(root) is not None or old in TERMINAL_INBOX
                or any(item.recorded_stage != "CANCELLED_UNSIGNED" for item in self._root_attempts(root))):
            return None
        prior = self._conn.execute("SELECT ordinal,payload_json FROM ledger_inbox_dispositions WHERE root_id=? ORDER BY ordinal DESC LIMIT 1", (root,)).fetchone()
        if prior is not None and at < strict_json_object(prior[1])["recorded_at_utc"]:
            return None
        ordinal = 1 if prior is None else prior[0]+1
        record = {"version": "live_ledger_nonacceptance_v0.1", "root_id": root, "ordinal": ordinal,
            "idempotency_key": command_id, "from_disposition": old,
            "disposition": "EXPIRED" if decision.decision.disposition == "EXPIRED" else "DENIED_RETRYABLE",
            "external_reference": command_id, "external_record_digest": risk.content_digest,
            "recorded_at_utc": at, "has_real_authority_grant": False}
        digest = content_fingerprint(record)
        self._conn.execute("INSERT INTO ledger_inbox_dispositions VALUES(?,?,?,?,?,?,?)",
            (root, ordinal, command_id, record["disposition"], digest, canonical_json(record), current.revision+1))
        return digest

    def admit_authority_entry(self, request, clock, source_store, support, *, command_id, fence):
        """Fresh original facts -> risk + terms/reservation/consumption, ONE commit.

        A repeated command returns historical original facts. A new command
        reselects current source and current controls/custody under the common
        writer transaction. Neither return value grants an Execution message.
        """
        try:
            public_reference(command_id)
            if type(request) is not EntryRequest or type(clock) is not TrustedClockSample or type(source_store) is not SourceEvidenceStore:
                raise LedgerConflict("AUTHORITY_CONCRETE_REQUEST_CLOCK_AND_SOURCE_STORE_REQUIRED")
            support = _support_from_json(_support_json(support))
            current = self._begin_economic_write(fence)
            existing = self.authority_admission_receipt(command_id)
            if existing is not None:
                if (existing.request, existing.eligibility.original.clock, existing.wallet_support_digest) != (request, clock, support.digest):
                    raise LedgerConflict("AUTHORITY_ADMISSION_COMMAND_CONTENT_CONFLICT")
                self._commit()
                return existing
            self._new_revision(fence, current)
            candidate = self.candidate(request.root_id)
            if candidate is None:
                raise LedgerConflict("AUTHORITY_CANONICAL_INBOX_REQUIRED")
            selected = source_store.latest_record()
            old = next((item for item in self._authority.source_checkpoints if (item.source_identity, item.profile_fingerprint) ==
                (source_store.binding.source_identity, source_store.profile.fingerprint)), None)
            prior_source = None if old is None else source_store.read_record(old.sequence)
            supplied = EligibilityInput(request.root_id, candidate.content_digest, request.track, clock,
                None if selected is None else selected[0], None if selected is None else selected[1],
                None if prior_source is None else prior_source[1].content_digest)
            decision, authority = evaluate_entry(self.domain, self._authority, candidate, self.inbox_disposition(request.root_id),
                supplied, self.authority_entry_binding(request.root_id))
            eligibility = AuthorityReceipt(current.revision+1, command_id, "AUTHORITY_ELIGIBILITY", supplied, decision,
                self._authority.content_digest, authority.content_digest, current.last_receipt_digest)
            comparison, compared = self._insert_wallet_comparison(support, ingestion_key=command_id, current=current)
            accepted, policies, actions, spent = self._authority_risk_history(request, candidate)
            action_row = self._conn.execute("SELECT action_id FROM ledger_pending_actions WHERE root_id=? AND side='BUY'", (request.root_id,)).fetchone()
            staged = None if action_row is None else self.action(action_row[0])
            risk = assess_entry_risk(self.domain, self._custody, candidate, request, eligibility, self._authority.policy,
                support, comparison.comparison, accepted=accepted, policies=policies, applications=self._applications,
                pending_attempts=self._pending_attempts(), staged_action=staged, spent_grants=spent, actions=actions)
            admission = consumed = denial_digest = None
            updated = compared
            if risk.disposition == "ADMISSIBLE_STORAGE_ONLY":
                cut = ConsumerCut(self.domain.economic_domain_id, current.revision, current.last_receipt_digest, self._custody.content_digest)
                terms = admission_terms(self.domain, candidate, request, eligibility, risk, self._authority.policy, cut, command_id, staged)
                admission = self._insert_admission(terms, ingestion_key=command_id, current=current, compared_custody=compared)
                updated = self._pending_custody_bundle[0]
                consumed = self._authority.grant.grant_id if self._authority.grant.scope == "ENTRY_ONCE" else None
            else:
                denial_digest = self._authority_nonacceptance(request.root_id, eligibility, risk, command_id=command_id, current=current)
            receipt = AuthorityAdmissionReceipt(current.revision+1, command_id, request, candidate.mint, eligibility, support.digest,
                comparison.content_digest, risk, None if admission is None else admission.content_digest, consumed, denial_digest,
                self._custody.content_digest, updated.content_digest, current.last_receipt_digest)
            if admission_receipt_from_record(strict_json_object(canonical_json(asdict(receipt)))) != receipt:
                raise LedgerConflict("AUTHORITY_ADMISSION_FINITE_CODEC_CONFLICT")
            self._insert_authority_receipt(eligibility, authority)
            self._conn.execute("INSERT INTO ledger_authority_admissions VALUES(?,?,?,?,?,?,?,?)", self._authority_admission_row(receipt))
            # Preserve both child caches before _commit publishes the trusted head.
            self._store_custody_projection(updated)
            self._pending_custody_bundle = (updated, self._applications, {**self._comparisons, command_id: comparison})
            if receipt.accepted and source_store.latest_record() != selected:
                raise LedgerConflict("AUTHORITY_CURRENT_SOURCE_CHANGED_DURING_ADMISSION")
            self._append_commit("AUTHORITY_ADMISSION", command_id, receipt.content_digest, current)
            self._commit()
            return receipt
        except BaseException as exc:
            self._rollback_economic(exc)

    def _store_custody_projection(self, state):
        if not self._conn.in_transaction:
            raise LedgerConflict("CUSTODY_PROJECTION_REQUIRES_OWNED_TRANSACTION")
        funding, accounts, positions, resolutions = self._projection_rows(state)
        self._conn.execute("INSERT INTO ledger_funding_projection VALUES(?,?,?) ON CONFLICT(singleton) DO UPDATE SET "
                           "content_digest=excluded.content_digest,payload_json=excluded.payload_json", funding)
        self._conn.executemany("INSERT INTO ledger_account_projection VALUES(?,?,?) ON CONFLICT(pubkey) DO UPDATE SET "
                              "content_digest=excluded.content_digest,payload_json=excluded.payload_json", accounts)
        self._conn.executemany("INSERT INTO ledger_position_projection VALUES(?,?,?) ON CONFLICT(position_id) DO UPDATE SET "
                              "content_digest=excluded.content_digest,payload_json=excluded.payload_json", positions)
        self._conn.executemany("INSERT INTO ledger_resolution_projection VALUES(?,?,?,?) ON CONFLICT(attempt_id) DO UPDATE SET "
                              "signature=excluded.signature,content_digest=excluded.content_digest,payload_json=excluded.payload_json", resolutions)

    def _verify_custody_projection(self, state):
        funding, accounts, positions, resolutions = self._projection_rows(state)
        if (self._conn.execute("SELECT * FROM ledger_funding_projection").fetchall() != [funding]
                or sorted(self._conn.execute("SELECT * FROM ledger_account_projection").fetchall()) != sorted(accounts)
                or sorted(self._conn.execute("SELECT * FROM ledger_position_projection").fetchall()) != sorted(positions)
                or sorted(self._conn.execute("SELECT * FROM ledger_resolution_projection").fetchall()) != sorted(resolutions)):
            raise LedgerConflict("LEDGER_CUSTODY_PROJECTION_RECONSTRUCTION_CONFLICT")

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
        reservation = next((item for item in self._custody.reservations if item.root_id == root_id), None)
        if reservation is not None:
            return reservation.terminal_disposition if reservation.status == "RETIRED" else "ACCEPTED"
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
        if not allow_terminal:
            require_admitted_action(self._custody, action)
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
        with self._trusted_read():
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
        if self.domain.mode == "DRY" and (record.get("to_stage") not in ("EXACT_SIMULATED", "UNKNOWN", "CANCELLED_UNSIGNED")
                or record.get("to_stage") == "CANCELLED_UNSIGNED" and record.get("local_cancel") is not True):
            raise LedgerConflict("LEDGER_DRY_SIGNED_SEND_OR_LIVE_CANCEL_GRAPH_FORBIDDEN")
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
                             signature, wire_digest, target != "CANCELLED_UNSIGNED", previous.chain_finality, previous.chain_quarantined)

    def attempt(self, attempt_id: str) -> StoredAttempt | None:
        with self._trusted_read():
            return self._attempt(attempt_id)

    def _attempt(self, attempt_id: str) -> StoredAttempt | None:
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
        finality = self.attempt_finality(attempt_id)
        resolution = self._custody.resolution(attempt_id)
        economic = "UNAPPLIED" if resolution is None else resolution.disposition
        dry = next((item for item in self._custody.dry_dispositions if item.input.attempt_id == attempt_id), None)
        action = self.action(prep.action_id)
        reservation = self.reservation(action.root_id)
        return replace(value, chain_finality=finality.disposition, chain_quarantined=finality.quarantined,
            lane_held=self._lane()[0] == attempt_id, economic_disposition=economic,
            economically_applied=resolution is not None and resolution.disposition in ("FINALIZED_SUCCESS_APPLIED", "FINALIZED_FAILURE_APPLIED"),
            custody_quarantined=self._custody.quarantined, current_inbox_disposition=self.inbox_disposition(action.root_id),
            reservation_id=None if reservation is None else reservation.reservation_id,
            current_disposition="NON_SUBMITTED" if dry is not None else "CUSTODY_QUARANTINED" if self._custody.quarantined else finality.disposition if resolution is None else economic)

    def attempt_finality(self, attempt_id: str) -> AttemptFinality:
        with self._trusted_read():
            digest_value(attempt_id)
            facts = getattr(self, "_chain_facts", {}).get(attempt_id)
            return AttemptFinality(attempt_id) if facts is None else facts.state

    def _signed_lineage(self, attempt_id: str) -> tuple[bytes, str]:
        for payload, in self._conn.execute("SELECT payload_json FROM ledger_attempt_stages WHERE attempt_id=? ORDER BY revision", (attempt_id,)):
            record = strict_json_object(payload)
            if record["to_stage"] == "SIGNED_DURABLE":
                stage = stage_from_record(record["stage_input"])
                return bytes(stage.signed_public_transaction()), stage.recorded_at_utc
        raise LedgerConflict("LEDGER_DURABLE_SIGNED_LINEAGE_REQUIRED")

    def _chain_receipt_row(self, row: tuple) -> LedgerChainReceipt:
        seq, key, attempt_id, revision, evidence_digest, digest, payload, original = row
        observation = chain_observation_from_json(original)
        receipt = chain_receipt_from_record(strict_json_object(payload), observation)
        if ((receipt.sequence, receipt.ingestion_key, receipt.attempt_id, receipt.decision.resulting_state.evidence_revision,
             observation.content_digest, receipt.content_digest) != (seq, key, attempt_id, revision, evidence_digest, digest)
                or canonical_json(receipt.to_record()) != payload):
            raise LedgerJournalError("LEDGER_ORIGINAL_CHAIN_RECEIPT_CONTENT_CONFLICT")
        return receipt

    def _chain_rows(self, clause: str = "", params: tuple = ()) -> list[tuple]:
        return self._conn.execute("SELECT r.*,o.payload_json FROM ledger_chain_receipts r JOIN ledger_chain_observations o "
                                  "ON o.evidence_digest=r.evidence_digest " + clause, params).fetchall()

    def chain_receipt(self, ingestion_key: str) -> LedgerChainReceipt | None:
        self._require_open()
        public_reference(ingestion_key)
        rows = self._chain_rows("WHERE r.ingestion_key=?", (ingestion_key,))
        return None if not rows else self._chain_receipt_row(rows[0])

    def ingest_chain_observation(self, attempt_id: str, observation, *, ingestion_key: str,
                                 evaluated_at_utc: str, fence: LedgerWriteFence) -> LedgerChainReceipt:
        """Commit original Evidence and its re-derived decision together, no lane release."""
        try:
            if self.domain.mode != "LIVE":
                raise LedgerConflict("LEDGER_DRY_HAS_NO_SIGNED_CHAIN_EVIDENCE_GRAPH")
            public_reference(ingestion_key)
            evaluated_at_utc = ledger_utc(evaluated_at_utc)
            original = chain_observation_to_json(observation)
            restored = chain_observation_from_json(original)
            if restored != observation:
                raise LedgerConflict("LEDGER_ORIGINAL_CHAIN_EVIDENCE_ROUNDTRIP_CONFLICT")
            current = self._begin_economic_write(fence)
            attempt = self.attempt(attempt_id)
            if attempt is None:
                raise LedgerConflict("LEDGER_ATTEMPT_NOT_FOUND")
            rows = self._chain_rows("WHERE r.ingestion_key=? OR r.evidence_digest=?", (ingestion_key, restored.content_digest))
            if rows:
                if len(rows) != 1:
                    raise LedgerConflict("LEDGER_CHAIN_INGESTION_IDENTITY_CONFLICT")
                receipt = self._chain_receipt_row(rows[0])
                if (receipt.attempt_id != attempt_id or receipt.ingestion_key != ingestion_key or receipt.observation != restored
                        or receipt.decision.evaluated_at_utc != evaluated_at_utc):
                    raise LedgerConflict("LEDGER_CHAIN_INGESTION_CONTENT_CONFLICT")
                self._commit()
                return receipt
            self._new_revision(fence, current)
            if self._custody.resolution(attempt_id) is None and (not attempt.lane_held or self._lane()[0] != attempt_id):
                raise LedgerConflict("LEDGER_CHAIN_EVIDENCE_WITHOUT_HELD_ATTEMPT")
            signed_wire, signed_at = self._signed_lineage(attempt_id)
            baseline = self.baseline()
            if baseline is None:
                raise LedgerConflict("LEDGER_CHAIN_EVIDENCE_REQUIRES_OPENING_BASELINE")
            decision, retained = adjudicate_chain_observation(self.domain, attempt, signed_wire, signed_at,
                baseline.observation.anchor, restored, evaluated_at_utc=evaluated_at_utc,
                retained=self._chain_facts.get(attempt_id))
            receipt = LedgerChainReceipt(current.revision+1, ingestion_key, attempt_id, attempt.revision,
                attempt.preparation.content_digest, attempt.signed_wire_digest, baseline.content_digest,
                current.last_receipt_digest, current.generation, current.generation_digest, restored, decision)
            self._conn.execute("INSERT INTO ledger_chain_observations VALUES(?,?)", (restored.content_digest, original))
            self._conn.execute("INSERT INTO ledger_chain_receipts VALUES(?,?,?,?,?,?,?)",
                (receipt.sequence, ingestion_key, attempt_id, retained.state.evidence_revision, restored.content_digest,
                 receipt.content_digest, canonical_json(receipt.to_record())))
            self._append_commit("CHAIN_EVIDENCE", ingestion_key, receipt.content_digest, current)
            self._pending_chain_facts = {**self._chain_facts, attempt_id: retained}
            updated = retain_chain_custody(self._custody, retained)
            if updated != self._custody:
                self._store_custody_projection(updated)
                self._pending_custody_bundle = (updated, self._applications, self._comparisons)
            self._commit()
            return receipt
        except BaseException as exc:
            self._pending_chain_facts = None
            self._rollback_economic(exc)

    def _retain_custody_input(self, support):
        if support is None:
            return
        payload = _support_json(support)
        existing = self._conn.execute("SELECT payload_json FROM ledger_custody_inputs WHERE input_digest=?", (support.digest,)).fetchone()
        if existing is None:
            self._conn.execute("INSERT INTO ledger_custody_inputs VALUES(?,?)", (support.digest, payload))
        elif existing[0] != payload:
            raise LedgerConflict("LEDGER_CUSTODY_INPUT_CONTENT_CONFLICT")

    def _posting_rows(self, receipt):
        proposal = receipt.decision.proposal
        if proposal is None:
            return []
        rows = []
        for component in proposal.components:
            record = {"version": "live_ledger_actual_posting_v0.1", "economic_domain_id": self.domain.economic_domain_id,
                "application_sequence": receipt.sequence, "action_id": proposal.action_id, "attempt_id": proposal.attempt_id,
                "root_id": proposal.root_id, "signature": proposal.signature, "proposal_digest": proposal.content_digest,
                "component": asdict(component)}
            rows.append((component.component_id, receipt.sequence, content_fingerprint(record), canonical_json(record)))
        return rows

    def application_receipt(self, ingestion_key):
        with self._trusted_read():
            public_reference(ingestion_key)
            return self._applications.get(ingestion_key)

    def comparison_receipt(self, ingestion_key):
        with self._trusted_read():
            public_reference(ingestion_key)
            return self._comparisons.get(ingestion_key)

    def wallet_support(self, input_digest):
        with self._trusted_read():
            digest_value(input_digest)
            row = self._conn.execute("SELECT payload_json FROM ledger_custody_inputs WHERE input_digest=?", (input_digest,)).fetchone()
            value = None if row is None else _support_from_json(row[0])
            if value is not None and value.digest != input_digest:
                raise LedgerConflict("LEDGER_CUSTODY_INPUT_LOOKUP_CONFLICT")
            return value

    def reservation(self, root_id):
        with self._trusted_read():
            digest_value(root_id)
            return next((item for item in self._custody.reservations if item.root_id == root_id), None)

    def protection(self, position_id):
        with self._trusted_read():
            digest_value(position_id)
            return next((item for item in self._custody.protections if item.handoff.position_id == position_id), None)

    def _port_row(self, receipt):
        retired = (receipt.retirement.reservation_id if receipt.retirement_disposition == "RETIRED" else
                   reservation_identity(self.domain.economic_domain_id, receipt.dry_terminal.root_id) if receipt.dry_terminal is not None else None)
        return (receipt.sequence, receipt.ingestion_key, receipt.kind,
            None if receipt.admission is None else receipt.admission.action.root_id,
            None if receipt.handoff is None else receipt.handoff.position_id, retired,
            None if receipt.dry_terminal is None else receipt.dry_terminal.action_id,
            receipt.content_digest, canonical_json(receipt.to_record()))

    def _port_receipt_row(self, row):
        receipt = port_receipt_from_record(strict_json_object(row[-1]))
        if self._port_row(receipt) != row:
            raise LedgerConflict("LEDGER_CONSUMER_GROUP_CONTENT_OR_LINK_CONFLICT")
        return receipt

    def port_receipt(self, ingestion_key):
        with self._trusted_read():
            public_reference(ingestion_key)
            row = self._conn.execute("SELECT * FROM ledger_consumer_groups WHERE ingestion_key=?", (ingestion_key,)).fetchone()
            return None if row is None else self._port_receipt_row(row)

    def _port_at_sequence(self, sequence):
        row = self._conn.execute("SELECT * FROM ledger_consumer_groups WHERE commit_seq=?", (sequence,)).fetchone()
        if row is None:
            raise LedgerConflict("LEDGER_CONSUMER_GROUP_REFERENCE_MISSING")
        return self._port_receipt_row(row)

    def _root_attempts(self, root_id):
        return tuple(self._attempt(row[0]) for row in self._conn.execute(
            "SELECT t.attempt_id FROM ledger_attempts t JOIN ledger_pending_actions a ON a.action_id=t.action_id "
            "WHERE a.root_id=? ORDER BY t.commit_seq", (root_id,)))

    def _simulation_input_digest(self, attempt_id):
        values = []
        for payload, in self._conn.execute("SELECT payload_json FROM ledger_attempt_stages WHERE attempt_id=? ORDER BY revision", (attempt_id,)):
            row = strict_json_object(payload)
            if row["to_stage"] == "EXACT_SIMULATED":
                values.append(content_fingerprint(stage_from_record(row["stage_input"]).to_record()))
        if len(values) > 1:
            raise LedgerConflict("LEDGER_ORIGINAL_SIMULATION_LINEAGE_CONFLICT")
        return None if not values else values[0]

    def _finish_port_group(self, receipt, updated, applications=None):
        """Fixed group insertion only; caller appends the common commit."""
        if not self._conn.in_transaction:
            raise LedgerConflict("LEDGER_CONSUMER_GROUP_REQUIRES_OWNED_TRANSACTION")
        self._conn.execute("INSERT INTO ledger_consumer_groups VALUES(?,?,?,?,?,?,?,?,?)", self._port_row(receipt))
        self._store_custody_projection(updated)
        self._pending_custody_bundle = (updated, self._applications if applications is None else applications, self._comparisons)

    def _insert_admission(self, supplied, *, ingestion_key, current, compared_custody=None):
        """Fixed no-commit insertion for the immediate Authority admission consumer.

        Its caller must group any one-time consumption with this exact receipt
        under the same common commit; this helper never grants permission.
        """
        if not self._conn.in_transaction:
            raise LedgerConflict("LEDGER_ADMISSION_REQUIRES_OWNED_TRANSACTION")
        require_common_cut(self.domain, self._custody, supplied.cut, current.revision, current.last_receipt_digest)
        candidate = self._validate_action_parent(supplied.action)
        state = self._custody if compared_custody is None else compared_custody
        updated = admit_reservation(self.domain, state, candidate, self.inbox_disposition(supplied.action.root_id),
            self._root_attempts(supplied.action.root_id), supplied, sequence=current.revision+1)
        previous = self.action(supplied.action.action_id)
        created = None
        if previous is None:
            self._insert_pending_action(supplied.action, current.revision+1)
            created = supplied.action.content_digest
        elif previous != supplied.action:
            raise LedgerConflict("LEDGER_ADMISSION_STAGED_TERMS_CONFLICT")
        receipt = ConsumerPortReceipt(current.revision+1, ingestion_key, "ADMISSION", supplied, None, None, None,
            created, None, None, None, None, (), state.content_digest, updated.content_digest)
        self._finish_port_group(receipt, updated)
        return receipt

    def admit(self, supplied, *, ingestion_key, fence):
        """External exact terms + reservation + ACCEPTED, one common commit."""
        try:
            if type(supplied) is not AdmissionInput:
                raise LedgerConflict("LEDGER_ADMISSION_INPUT_REQUIRED")
            supplied = admission_from_record(strict_json_object(canonical_json(asdict(supplied))))
            current = self._begin_economic_write(fence)
            existing = self.port_receipt(ingestion_key)
            reservation = self.reservation(supplied.action.root_id)
            if existing is not None or reservation is not None:
                original = existing if existing is not None else self._port_at_sequence(reservation.sequence)
                if original.admission != supplied:
                    raise LedgerConflict("LEDGER_ADMISSION_IMMUTABLE_CONTENT_CONFLICT")
                self._commit()
                return original
            self._new_revision(fence, current)
            receipt = self._insert_admission(supplied, ingestion_key=ingestion_key, current=current)
            self._append_commit(receipt.kind, ingestion_key, receipt.content_digest, current)
            self._commit()
            return receipt
        except BaseException as exc:
            self._rollback_economic(exc)

    def _protection_state(self, state, supplied, applications, *, sequence):
        resolution = next((item for item in state.resolutions if item.signature == supplied.acquisition_signature), None)
        acquisition = None if resolution is None else next((item for item in applications.values() if item.sequence == resolution.application_sequence), None)
        chain = None if acquisition is None else self.chain_receipt(acquisition.chain_receipt_key)
        return activate_protection(self.domain, state, supplied, acquisition, chain, sequence=sequence)

    def record_protective_handoff(self, supplied, *, ingestion_key, fence):
        try:
            if type(supplied) is not ProtectiveHandoffInput:
                raise LedgerConflict("LEDGER_PROTECTIVE_HANDOFF_INPUT_REQUIRED")
            supplied = handoff_from_record(strict_json_object(canonical_json(asdict(supplied))))
            current = self._begin_economic_write(fence)
            existing, protection = self.port_receipt(ingestion_key), self.protection(supplied.position_id)
            if existing is not None or protection is not None:
                original = existing if existing is not None else self._port_at_sequence(protection.sequence)
                if original.handoff != supplied:
                    raise LedgerConflict("LEDGER_PROTECTION_BINDING_REPLACEMENT_FORBIDDEN")
                self._commit()
                return original
            self._new_revision(fence, current)
            require_common_cut(self.domain, self._custody, supplied.cut, current.revision, current.last_receipt_digest)
            updated = self._protection_state(self._custody, supplied, self._applications, sequence=current.revision+1)
            receipt = ConsumerPortReceipt(current.revision+1, ingestion_key, "PROTECTION", None, supplied, None, None,
                None, None, None, None, None, (), self._custody.content_digest, updated.content_digest)
            self._finish_port_group(receipt, updated)
            self._append_commit(receipt.kind, ingestion_key, receipt.content_digest, current)
            self._commit()
            return receipt
        except BaseException as exc:
            self._rollback_economic(exc)

    def _retirement_state(self, state, supplied, support, applications, *, current):
        if type(support) is not WalletSupportInput or support.digest != supplied.wallet_support_digest or support.evaluated_at_utc != supplied.recorded_at_utc:
            raise LedgerConflict("LEDGER_RETIREMENT_ORIGINAL_COMPARISON_INPUT_REQUIRED")
        if support.required_min_context_slot < state.anchor.slot:
            raise LedgerConflict("LEDGER_RETIREMENT_COMPARISON_BEHIND_RESULTING_SETTLEMENT_CUT")
        comparison, updated = compare_wallet(self.domain, state, support, common_revision=current.revision,
            common_digest=current.last_receipt_digest, pending_attempts=self._pending_attempts(),
            chain_anchors=(anchor for facts in self._chain_facts.values() for anchor in facts.anchors))
        resolution = state.resolution(supplied.terminal_attempt_id)
        application = None if resolution is None else next((item for item in applications.values() if item.sequence == resolution.application_sequence), None)
        updated, disposition, reasons = retire_reservation(self.domain, updated, supplied, self._root_attempts(supplied.root_id),
            application, comparison, sequence=current.revision+1)
        self._retain_custody_input(support)
        return updated, comparison, disposition, reasons

    def retire(self, supplied, support, *, ingestion_key, fence):
        try:
            if type(supplied) is not RetirementInput:
                raise LedgerConflict("LEDGER_RETIREMENT_INPUT_REQUIRED")
            supplied = retirement_from_record(strict_json_object(canonical_json(asdict(supplied))))
            support = _support_from_json(_support_json(support))
            current = self._begin_economic_write(fence)
            existing = self.port_receipt(ingestion_key)
            reservation = self.reservation(supplied.root_id)
            if existing is None and reservation is not None and reservation.retired_sequence is not None:
                existing = self._port_at_sequence(reservation.retired_sequence)
            if existing is not None:
                if existing.retirement != supplied or supplied.wallet_support_digest != support.digest:
                    raise LedgerConflict("LEDGER_RETIREMENT_IDEMPOTENCY_CONTENT_CONFLICT")
                self._commit()
                return existing
            self._new_revision(fence, current)
            require_common_cut(self.domain, self._custody, supplied.cut, current.revision, current.last_receipt_digest)
            updated, comparison, disposition, reasons = self._retirement_state(self._custody, supplied, support, self._applications, current=current)
            receipt = ConsumerPortReceipt(current.revision+1, ingestion_key, "RETIREMENT", None, None, supplied, None,
                None, None, None, content_fingerprint(asdict(comparison)), disposition, reasons, self._custody.content_digest, updated.content_digest)
            self._finish_port_group(receipt, updated)
            self._append_commit(receipt.kind, ingestion_key, receipt.content_digest, current)
            self._commit()
            return receipt
        except BaseException as exc:
            self._rollback_economic(exc)

    def finish_dry(self, supplied, *, ingestion_key, fence):
        try:
            if type(supplied) is not DryTerminalInput:
                raise LedgerConflict("LEDGER_DRY_TERMINAL_INPUT_REQUIRED")
            supplied = dry_terminal_from_record(strict_json_object(canonical_json(asdict(supplied))))
            current = self._begin_economic_write(fence)
            existing = self.port_receipt(ingestion_key)
            previous = next((item for item in self._custody.dry_dispositions if item.input.root_id == supplied.root_id), None)
            if existing is not None or previous is not None:
                original = existing if existing is not None else self._port_at_sequence(previous.sequence)
                if original.dry_terminal != supplied:
                    raise LedgerConflict("LEDGER_DRY_TERMINAL_CONTENT_CONFLICT")
                self._commit()
                return original
            self._new_revision(fence, current)
            require_common_cut(self.domain, self._custody, supplied.cut, current.revision, current.last_receipt_digest)
            attempts = self._root_attempts(supplied.root_id)
            simulation = None if supplied.attempt_id is None else self._simulation_input_digest(supplied.attempt_id)
            updated = terminate_dry(self.domain, self._custody, supplied, attempts, simulation, sequence=current.revision+1)
            lane = self._lane()
            if any(item.recorded_stage != "CANCELLED_UNSIGNED" and lane[0] != item.preparation.attempt_id for item in attempts):
                raise LedgerConflict("LEDGER_DRY_UNRESOLVED_LINEAGE_REQUIRES_OWN_LANE")
            if supplied.attempt_id is not None and lane[0] == supplied.attempt_id:
                self._change_lane(lane, None, current.revision+1)
            receipt = ConsumerPortReceipt(current.revision+1, ingestion_key, "DRY_NON_SUBMITTED", None, None, None, supplied,
                None, None, None, None, None, (), self._custody.content_digest, updated.content_digest)
            self._finish_port_group(receipt, updated)
            self._append_commit(receipt.kind, ingestion_key, receipt.content_digest, current)
            self._commit()
            return receipt
        except BaseException as exc:
            self._rollback_economic(exc)

    def _insert_settlement_application(self, attempt, chain, support, *, ingestion_key, recorded_at_utc, current):
        """No commit: L6 may add its concrete handoff facts in this transaction.

        The common commit envelope remains the outer port's responsibility.
        No caller-provided proposal or generic callback enters this seam.
        """
        if not self._conn.in_transaction:
            raise LedgerConflict("LEDGER_APPLICATION_REQUIRES_OWNED_TRANSACTION")
        action = self.action(attempt.preparation.action_id)
        if chain is None or chain.attempt_id != attempt.preparation.attempt_id:
            raise LedgerConflict("LEDGER_APPLICATION_CHAIN_RECEIPT_BINDING_CONFLICT")
        if (recorded_at_utc < chain.decision.evaluated_at_utc or support is not None and recorded_at_utc < support.evaluated_at_utc):
            raise LedgerConflict("LEDGER_APPLICATION_RECORDED_TIME_PRECEDES_INPUT")
        if self._custody.resolution(attempt.preparation.attempt_id) is None and self._lane()[0] != attempt.preparation.attempt_id:
            raise LedgerConflict("LEDGER_UNRESOLVED_APPLICATION_REQUIRES_OWN_LANE")
        decision, updated = adjudicate_application(self.domain, self._custody, action, attempt, chain, support,
            sequence=current.revision+1, recorded_at_utc=recorded_at_utc,
            chain_anchors=(anchor for facts in self._chain_facts.values() for anchor in facts.anchors))
        receipt = LedgerApplicationReceipt(current.revision+1, ingestion_key, attempt.preparation.attempt_id,
            chain.ingestion_key, chain.content_digest, support, recorded_at_utc, current.revision, current.last_receipt_digest, decision)
        self._retain_custody_input(support)
        self._conn.execute("INSERT INTO ledger_application_receipts VALUES(?,?,?,?,?,?,?)",
            (receipt.sequence, ingestion_key, receipt.attempt_id, chain.ingestion_key, None if support is None else support.digest,
             receipt.content_digest, canonical_json(receipt.to_record())))
        self._conn.executemany("INSERT INTO ledger_economic_postings VALUES(?,?,?,?)", self._posting_rows(receipt))
        if decision.releases_own_lane:
            lane = self._lane()
            if lane[0] != receipt.attempt_id:
                raise LedgerConflict("LEDGER_APPLICATION_CANNOT_RELEASE_ANOTHER_LANE")
            self._change_lane(lane, None, receipt.sequence)
        self._store_custody_projection(updated)
        self._pending_custody_bundle = (updated, {**self._applications, ingestion_key: receipt}, self._comparisons)
        return receipt

    def apply_settlement(self, attempt_id, *, chain_receipt_key, support, ingestion_key, recorded_at_utc, fence):
        """Original Evidence -> whole proposal -> one atomic economic application.

        support=None is the explicit nonlanding-resolution path, with no postings
        or retry grant. Caller proposals and balance overrides are not accepted.
        """
        try:
            if self.domain.mode != "LIVE":
                raise LedgerConflict("LEDGER_DRY_HAS_NO_ECONOMIC_APPLICATION_GRAPH")
            public_reference(ingestion_key)
            public_reference(chain_receipt_key)
            recorded_at_utc = ledger_utc(recorded_at_utc)
            if support is not None:
                support = _support_from_json(_support_json(support))
            current = self._begin_economic_write(fence)
            existing = self._applications.get(ingestion_key)
            if existing is not None:
                if (existing.attempt_id != attempt_id or existing.chain_receipt_key != chain_receipt_key
                        or existing.support != support or existing.recorded_at_utc != recorded_at_utc):
                    raise LedgerConflict("LEDGER_APPLICATION_IDEMPOTENCY_CONTENT_CONFLICT")
                self._commit()
                return existing
            self._new_revision(fence, current)
            attempt = self.attempt(attempt_id)
            if attempt is None:
                raise LedgerConflict("LEDGER_APPLICATION_ATTEMPT_NOT_FOUND")
            chain = self.chain_receipt(chain_receipt_key)
            receipt = self._insert_settlement_application(attempt, chain, support, ingestion_key=ingestion_key,
                recorded_at_utc=recorded_at_utc, current=current)
            self._append_commit("SETTLEMENT_APPLICATION", ingestion_key, receipt.content_digest, current)
            self._commit()
            return receipt
        except BaseException as exc:
            self._rollback_economic(exc)

    def apply_settlement_with_ports(self, attempt_id, *, chain_receipt_key, support, ingestion_key,
            recorded_at_utc, fence, handoff=None, retirement=None, retirement_support=None):
        """One closed settlement + protection OR retirement group, with no callbacks."""
        try:
            if self.domain.mode != "LIVE" or (handoff is None) == (retirement is None):
                raise LedgerConflict("LEDGER_LIVE_CLOSED_SETTLEMENT_PORT_GROUP_REQUIRED")
            if handoff is not None:
                handoff = handoff_from_record(strict_json_object(canonical_json(asdict(handoff))))
                if retirement_support is not None:
                    raise LedgerConflict("LEDGER_UNEXPECTED_RETIREMENT_SUPPORT")
            else:
                retirement = retirement_from_record(strict_json_object(canonical_json(asdict(retirement))))
                retirement_support = _support_from_json(_support_json(retirement_support))
            supplied = handoff if handoff is not None else retirement
            recorded_at_utc = ledger_utc(recorded_at_utc)
            if supplied.recorded_at_utc != recorded_at_utc:
                raise LedgerConflict("LEDGER_GROUP_RECORDED_TIME_CONFLICT")
            if support is not None:
                support = _support_from_json(_support_json(support))
            public_reference(ingestion_key)
            child_key = ingestion_key+":application"
            public_reference(child_key)
            current = self._begin_economic_write(fence)
            existing = self.port_receipt(ingestion_key)
            if existing is not None:
                child = self._applications.get(child_key)
                if (existing.handoff != handoff or existing.retirement != retirement or child is None
                        or (child.attempt_id, child.chain_receipt_key, child.support, child.recorded_at_utc) !=
                           (attempt_id, chain_receipt_key, support, recorded_at_utc)
                        or retirement is not None and retirement.wallet_support_digest != retirement_support.digest):
                    raise LedgerConflict("LEDGER_SETTLEMENT_GROUP_IDEMPOTENCY_CONTENT_CONFLICT")
                self._commit()
                return existing
            self._new_revision(fence, current)
            require_common_cut(self.domain, self._custody, supplied.cut, current.revision, current.last_receipt_digest)
            attempt = self.attempt(attempt_id)
            if attempt is None:
                raise LedgerConflict("LEDGER_APPLICATION_ATTEMPT_NOT_FOUND")
            action = self.action(attempt.preparation.action_id)
            if ((supplied.root_id, supplied.position_id) != (action.root_id, action.position_id)
                    or handoff is not None and supplied.acquisition_signature != attempt.primary_signature
                    or retirement is not None and supplied.terminal_attempt_id != attempt_id):
                raise LedgerConflict("LEDGER_SETTLEMENT_GROUP_ROOT_POSITION_ATTEMPT_CONFLICT")
            original_digest = self._custody.content_digest
            application = self._insert_settlement_application(attempt, self.chain_receipt(chain_receipt_key), support,
                ingestion_key=child_key, recorded_at_utc=recorded_at_utc, current=current)
            state, applications, _ = self._pending_custody_bundle
            comparison_digest, disposition, reasons = None, None, ()
            if handoff is not None:
                state = self._protection_state(state, handoff, applications, sequence=current.revision+1)
            else:
                state, comparison, disposition, reasons = self._retirement_state(state, retirement, retirement_support, applications, current=current)
                comparison_digest = content_fingerprint(asdict(comparison))
            receipt = ConsumerPortReceipt(current.revision+1, ingestion_key, "SETTLEMENT_PORTS", None, handoff, retirement, None,
                None, child_key, application.content_digest, comparison_digest, disposition, reasons,
                original_digest, state.content_digest, application.decision.disposition)
            self._finish_port_group(receipt, state, applications)
            self._append_commit(receipt.kind, ingestion_key, receipt.content_digest, current)
            self._commit()
            return receipt
        except BaseException as exc:
            self._rollback_economic(exc)

    def _insert_wallet_comparison(self, support, *, ingestion_key, current):
        if not self._conn.in_transaction:
            raise LedgerConflict("LEDGER_COMPARISON_REQUIRES_OWNED_TRANSACTION")
        comparison, updated = compare_wallet(self.domain, self._custody, support, common_revision=current.revision,
            common_digest=current.last_receipt_digest, pending_attempts=self._pending_attempts(),
            chain_anchors=(anchor for facts in self._chain_facts.values() for anchor in facts.anchors))
        receipt = LedgerComparisonReceipt(current.revision+1, ingestion_key, support, comparison,
            self._custody.content_digest, updated.content_digest)
        self._retain_custody_input(support)
        self._conn.execute("INSERT INTO ledger_wallet_comparisons VALUES(?,?,?,?,?)",
            (receipt.sequence, ingestion_key, support.digest, receipt.content_digest, canonical_json(receipt.to_record())))
        self._store_custody_projection(updated)
        self._pending_custody_bundle = (updated, self._applications, {**self._comparisons, ingestion_key: receipt})
        return receipt, updated

    def compare_wallet_observation(self, support, *, ingestion_key, fence):
        """Original ongoing Wallet comparison, separate from opening ingestion."""
        try:
            public_reference(ingestion_key)
            support = _support_from_json(_support_json(support))
            current = self._begin_economic_write(fence)
            existing = self._comparisons.get(ingestion_key)
            if existing is not None:
                if existing.support != support:
                    raise LedgerConflict("LEDGER_COMPARISON_IDEMPOTENCY_CONTENT_CONFLICT")
                self._commit()
                return existing
            self._new_revision(fence, current)
            receipt, updated = self._insert_wallet_comparison(support, ingestion_key=ingestion_key, current=current)
            self._append_commit("WALLET_COMPARISON", ingestion_key, receipt.content_digest, current)
            self._commit()
            return receipt
        except BaseException as exc:
            self._rollback_economic(exc)

    def _validate_preparation(self, prep: AttemptPreparation) -> None:
        action = self.action(prep.action_id)
        if action is None or action.content_digest != prep.action_content_digest:
            raise LedgerConflict("LEDGER_ATTEMPT_ACTION_CONTENT_MISMATCH")
        candidate = self._validate_action_parent(action)
        baseline_payload = self._conn.execute("SELECT payload_json FROM ledger_baseline_journal WHERE baseline_domain_id=?",
                                               (self.domain.economic_domain_id,)).fetchone()
        baseline_slot = strict_json_object(baseline_payload[0])["decision"]["context_slot"]
        custody_slot = 0 if self._custody.anchor is None else self._custody.anchor.slot
        message = decode_message(prep.message_hex)
        if (str(message.account_keys[0]) != self.domain.wallet
                or prep.finalized_lower_anchor.provider_fingerprint != self.domain.expected_profile_fingerprint
                or prep.finalized_lower_anchor.slot < max(self.domain.minimum_context_slot, baseline_slot, custody_slot)
                or self._custody.anchor is not None and not _known_finalized_anchors_consistent(self._custody.anchor, prep.finalized_lower_anchor)
                or any(not _known_finalized_anchors_consistent(anchor, prep.finalized_lower_anchor) for anchor in self._custody.qualified_wallet_anchors)
                or any(not _known_finalized_anchors_consistent(anchor, prep.finalized_lower_anchor)
                       for facts in self._chain_facts.values() for anchor in facts.anchors)
                or utc_microseconds(prep.prepared_at_utc) < candidate.generated_at_us
                or action.side == "BUY" and utc_microseconds(prep.prepared_at_utc) > action.claimed_entry_deadline_us):
            raise LedgerConflict("LEDGER_ATTEMPT_WALLET_ORIGINAL_ANCHOR_OR_DEADLINE_MISMATCH")

    def _insert_prepared_attempt(self, prep: AttemptPreparation, commit_seq: int) -> None:
        """No commit: a future concrete consumer port may share this transaction."""
        if not self._conn.in_transaction:
            raise LedgerJournalError("LEDGER_ATTEMPT_REQUIRES_OWNED_TRANSACTION")
        self._validate_preparation(prep)
        if self._custody.quarantined:
            raise LedgerConflict("LEDGER_CURRENT_CUSTODY_QUARANTINED")
        lane = self._lane()
        if lane[0] is not None:
            raise LedgerConflict("LEDGER_WALLET_MUTATION_LANE_HELD")
        previous = self._conn.execute("SELECT attempt_id,ordinal FROM ledger_attempts WHERE action_id=? ORDER BY ordinal DESC LIMIT 1",
                                       (prep.action_id,)).fetchone()
        ordinal = 1 if previous is None else previous[1]+1
        old_attempt = None if previous is None else self.attempt(previous[0])
        resolution = None if old_attempt is None else self._custody.resolution(old_attempt.preparation.attempt_id)
        if (prep.ordinal != ordinal or old_attempt is not None and (
                old_attempt.recorded_stage != "CANCELLED_UNSIGNED" and (resolution is None or resolution.disposition not in REPLACEABLE)
                or prep.prepared_at_utc < (old_attempt.last_recorded_at_utc if resolution is None else resolution.recorded_at_utc))):
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
            chain = self.attempt_finality(attempt_id)
            if chain.positive_finality is not None or chain.quarantined:
                raise LedgerConflict("LEDGER_CHAIN_FINALITY_OWNS_ATTEMPT")
            target = stage.target_stage if stage is not None else (
                "CANCELLED_UNSIGNED" if previous.recorded_stage in ("PREPARED", "EXACT_SIMULATED", "AUTHORIZED")
                and previous.prepared_generation == current.generation and previous.primary_signature is None else "UNKNOWN")
            record = {"version": "live_ledger_attempt_stage_receipt_v0.1", "attempt_id": attempt_id,
                "revision": previous.revision+1, "idempotency_key": idempotency_key, "from_stage": previous.recorded_stage,
                "to_stage": target, "recorded_at_utc": at, "writer_generation": current.generation,
                "stage_input": None if stage is None else stage.to_record(), "local_cancel": stage is None}
            value = self._append_attempt_stage(previous, record, current)
            self._commit()
            return self.attempt(attempt_id)
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
            if self.reservation(root_id) is not None:
                raise LedgerConflict("LEDGER_ADMITTED_ROOT_REQUIRES_ATOMIC_RETIREMENT_OR_DRY_RELEASE")
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
        baseline_receipt = next((receipt for receipt in receipts if receipt.decision.disposition == "ESTABLISHED"), None)
        records, chain_facts, custody, applications, comparisons, authority = self._audit_economic_history(commits, baseline_seq, baseline_slot, baseline_receipt)
        for receipt in receipts:
            commit = commits.get(receipt.sequence)
            if (commit is None or receipt.sequence in records or receipt.previous_digest != commit["previous_digest"]
                    or receipt.generation != commit["generation"]):
                raise LedgerJournalError("LEDGER_ORIGINAL_WALLET_COMMIT_BINDING_INVALID")
            records[receipt.sequence] = ("WALLET", receipt.ingestion_key, receipt.content_digest)
        if set(records) != set(commits) or any(records[seq] != (item["kind"], item["record_key"], item["record_digest"])
                                               for seq, item in commits.items()):
            raise LedgerJournalError("LEDGER_ORPHAN_OR_CONFLICTING_CONCRETE_COMMIT")
        self._chain_facts = chain_facts
        self._custody, self._applications, self._comparisons = custody, applications, comparisons
        self._authority = authority

    def _audit_economic_history(self, commits: dict, baseline_seq: int | None, baseline_slot: int | None,
                                baseline_receipt: LedgerWalletReceipt | None) -> tuple[dict, dict]:
        """Full finite replay on open/audit; ordinary appends validate touched facts."""
        records, events, candidates, actions, attempts = {}, {}, {}, {}, {}
        groups = {item.sequence: item for item in (self._port_receipt_row(row) for row in self._conn.execute("SELECT * FROM ledger_consumer_groups"))}
        children = {seq: {} for seq in groups}
        authority_groups = {item.sequence: item for item in (self._read_authority_admission_row(row)
            for row in self._conn.execute("SELECT * FROM ledger_authority_admissions"))}
        authority_children = {seq: {} for seq in authority_groups}

        def remember(seq, kind, key, digest, event):
            if seq in authority_groups and kind != "ACTION":
                if kind not in ("AUTHORITY_ELIGIBILITY", "WALLET_COMPARISON", "NON_ACCEPTANCE", "ADMISSION") or kind in authority_children[seq] or seq not in commits:
                    raise LedgerConflict("AUTHORITY_UNEXPECTED_OR_DUPLICATE_GROUP_CHILD")
                authority_children[seq][kind] = (key, digest, event)
                return
            if seq in groups:
                if kind not in ("ACTION", "SETTLEMENT_APPLICATION") or kind in children[seq] or seq not in commits:
                    raise LedgerConflict("LEDGER_UNEXPECTED_OR_DUPLICATE_GROUP_CHILD")
                children[seq][kind] = (key, digest, event)
                return
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

        chain_rows = self._chain_rows()
        if (len(chain_rows) != self._conn.execute("SELECT COUNT(*) FROM ledger_chain_observations").fetchone()[0]
                or len(chain_rows) != self._conn.execute("SELECT COUNT(*) FROM ledger_chain_receipts").fetchone()[0]):
            raise LedgerJournalError("LEDGER_ORPHAN_OR_MISSING_CHAIN_EVIDENCE")
        for row in chain_rows:
            receipt = self._chain_receipt_row(row)
            remember(receipt.sequence, "CHAIN_EVIDENCE", receipt.ingestion_key, receipt.content_digest, receipt)

        inputs = {}
        for digest, payload in self._conn.execute("SELECT * FROM ledger_custody_inputs"):
            support = _support_from_json(payload)
            if support.digest != digest:
                raise LedgerConflict("LEDGER_CUSTODY_ORIGINAL_INPUT_DIGEST_CONFLICT")
            inputs[digest] = support
        used_inputs = set()
        for seq, key, attempt_id, chain_key, input_digest, digest, payload in self._conn.execute("SELECT * FROM ledger_application_receipts"):
            record = strict_json_object(payload)
            support = None if input_digest is None else inputs.get(input_digest)
            if (canonical_json(record) != payload or content_fingerprint(record) != digest
                    or (record.get("sequence"), record.get("ingestion_key"), record.get("attempt_id"), record.get("chain_receipt_key"), record.get("input_digest"))
                    != (seq, key, attempt_id, chain_key, input_digest) or input_digest is not None and support is None):
                raise LedgerConflict("LEDGER_APPLICATION_RECEIPT_CONTENT_CONFLICT")
            if input_digest is not None:
                used_inputs.add(input_digest)
            remember(seq, "SETTLEMENT_APPLICATION", key, digest, (record, support))
        for seq, key, input_digest, digest, payload in self._conn.execute("SELECT * FROM ledger_wallet_comparisons"):
            record = strict_json_object(payload)
            if (canonical_json(record) != payload or content_fingerprint(record) != digest or input_digest not in inputs
                    or (record.get("sequence"), record.get("ingestion_key"), record.get("input_digest")) != (seq, key, input_digest)):
                raise LedgerConflict("LEDGER_COMPARISON_RECEIPT_CONTENT_CONFLICT")
            used_inputs.add(input_digest)
            remember(seq, "WALLET_COMPARISON", key, digest, (record, inputs[input_digest]))
        for seq, group in groups.items():
            expected_children = ({"ACTION"} if group.created_action_digest is not None else set()) | ({"SETTLEMENT_APPLICATION"} if group.application_key is not None else set())
            if children[seq].keys() != expected_children or seq not in commits or seq in records:
                raise LedgerConflict("LEDGER_GROUP_CHILD_SET_CONFLICT")
            if "ACTION" in children[seq] and children[seq]["ACTION"][:2] != (group.admission.action.action_id, group.created_action_digest):
                raise LedgerConflict("LEDGER_GROUP_ACTION_LINK_CONFLICT")
            if "SETTLEMENT_APPLICATION" in children[seq] and children[seq]["SETTLEMENT_APPLICATION"][:2] != (group.application_key, group.application_digest):
                raise LedgerConflict("LEDGER_GROUP_APPLICATION_LINK_CONFLICT")
            if group.retirement is not None:
                if group.retirement.wallet_support_digest not in inputs:
                    raise LedgerConflict("LEDGER_GROUP_RETIREMENT_ORIGINAL_INPUT_MISSING")
                used_inputs.add(group.retirement.wallet_support_digest)
            if seq in authority_groups:
                remember(seq, group.kind, group.ingestion_key, group.content_digest, group)
            else:
                records[seq] = (group.kind, group.ingestion_key, group.content_digest)
                events[seq] = (group.kind, group)
        if used_inputs != inputs.keys():
            raise LedgerConflict("LEDGER_ORPHAN_CUSTODY_ORIGINAL_INPUT")

        for row in self._conn.execute("SELECT * FROM ledger_authority_records"):
            receipt = self._authority_receipt_row(row)
            remember(receipt.sequence, receipt.kind, receipt.command_id, receipt.content_digest, receipt)
        for seq, group in authority_groups.items():
            expected = {"AUTHORITY_ELIGIBILITY", "WALLET_COMPARISON"} | ({"ADMISSION"} if group.accepted else set()) | ({"NON_ACCEPTANCE"} if group.inbox_disposition_digest is not None else set())
            actual = authority_children[seq]
            if (actual.keys() != expected or seq not in commits or seq in records
                    or actual["AUTHORITY_ELIGIBILITY"][:2] != (group.command_id, group.eligibility.content_digest)
                    or actual["AUTHORITY_ELIGIBILITY"][2] != group.eligibility
                    or actual["WALLET_COMPARISON"][:2] != (group.command_id, group.comparison_digest)
                    or group.accepted and actual["ADMISSION"][:2] != (group.command_id, group.admission_digest)
                    or group.inbox_disposition_digest is not None and actual["NON_ACCEPTANCE"][1] != group.inbox_disposition_digest):
                raise LedgerConflict("AUTHORITY_ADMISSION_CLOSED_GROUP_LINK_CONFLICT")
            records[seq] = ("AUTHORITY_ADMISSION", group.command_id, group.content_digest)
            events[seq] = ("AUTHORITY_ADMISSION", group)
        accepted_authority, spent_authority = {}, set()
        authority = AuthorityState(self.domain.economic_domain_id)
        authority_policies, authority_grants, authority_bindings = {}, {}, {}
        inbox, live_actions, live_attempts, per_action = {}, {}, {}, {}
        chain_facts, signed_inputs = {}, {}
        custody = baseline_custody(self.domain, baseline_receipt)
        applications, comparisons, chain_seen, postings = {}, {}, {}, []
        signatures = {}
        lane, lane_revision = None, 0
        simulations = {}

        def replay_application(seq, item):
            nonlocal custody, lane, lane_revision
            record, support = item
            attempt_id = record["attempt_id"]
            previous, chain = live_attempts.get(attempt_id), chain_seen.get(record["chain_receipt_key"])
            if (previous is None or chain is None or chain.attempt_id != attempt_id
                    or custody.resolution(attempt_id) is None and lane != attempt_id):
                raise LedgerConflict("LEDGER_APPLICATION_REPLAY_ORIGINAL_LINEAGE_CONFLICT")
            finality = chain_facts[attempt_id].state
            view = replace(previous, chain_finality=finality.disposition, chain_quarantined=finality.quarantined,
                           lane_held=lane == attempt_id)
            at = ledger_utc(record["recorded_at_utc"])
            if at < chain.decision.evaluated_at_utc or support is not None and at < support.evaluated_at_utc:
                raise LedgerConflict("LEDGER_APPLICATION_REPLAY_TIME_CONFLICT")
            decision, updated = adjudicate_application(self.domain, custody, live_actions[previous.preparation.action_id],
                view, chain, support, sequence=seq, recorded_at_utc=at,
                chain_anchors=(anchor for facts in chain_facts.values() for anchor in facts.anchors))
            expected = LedgerApplicationReceipt(seq, record["ingestion_key"], attempt_id, chain.ingestion_key, chain.content_digest,
                support, at, seq-1, commits[seq]["previous_digest"], decision)
            if canonical_json(expected.to_record()) != canonical_json(record):
                raise LedgerConflict("LEDGER_ORIGINAL_APPLICATION_DECISION_REPLAY_CONFLICT")
            if decision.releases_own_lane:
                if lane != attempt_id:
                    raise LedgerConflict("LEDGER_APPLICATION_RELEASE_REPLAY_CONFLICT")
                lane, lane_revision = None, seq
            custody = updated
            applications[expected.ingestion_key] = expected
            postings.extend(self._posting_rows(expected))

            return expected

        for seq in sorted(events):
            kind, item = events[seq]
            if kind == "AUTHORITY_CONTROL":
                command = item.original
                if command.grant is not None and command.grant.root_id is not None and command.grant.root_id not in inbox:
                    raise LedgerConflict("AUTHORITY_GRANT_UNKNOWN_ROOT")
                previous_digest = authority.content_digest
                authority = apply_control(self.domain, authority, command,
                    known_policy=None if command.policy is None else authority_policies.get(command.policy.policy_id),
                    known_grant=authority_grants.get(command.target_id if command.grant is None else command.grant.grant_id))
                if command.policy is not None:
                    authority_policies[command.policy.policy_id] = command.policy
                if command.grant is not None:
                    authority_grants[command.grant.grant_id] = command.grant
                expected = AuthorityReceipt(seq, command.command_id, kind, command, None, previous_digest,
                    authority.content_digest, commits[seq]["previous_digest"])
                if expected != item:
                    raise LedgerConflict("AUTHORITY_ORIGINAL_CONTROL_REPLAY_CONFLICT")
            elif kind == "AUTHORITY_ELIGIBILITY":
                supplied = item.original
                if supplied.root_id not in inbox:
                    raise LedgerConflict("AUTHORITY_ELIGIBILITY_BEFORE_INBOX")
                previous_digest = authority.content_digest
                decision, authority = evaluate_entry(self.domain, authority, candidates[supplied.root_id],
                    inbox[supplied.root_id][0], supplied, authority_bindings.get(supplied.root_id, (None,))[0])
                authority_bindings.setdefault(supplied.root_id, (decision.binding, seq))
                expected = AuthorityReceipt(seq, item.command_id, kind, supplied, decision, previous_digest,
                    authority.content_digest, commits[seq]["previous_digest"])
                if expected != item:
                    raise LedgerConflict("AUTHORITY_ORIGINAL_ELIGIBILITY_REPLAY_CONFLICT")
            elif kind == "AUTHORITY_ADMISSION":
                group = item
                root, request = group.request.root_id, group.request
                if root not in inbox:
                    raise LedgerConflict("AUTHORITY_ADMISSION_BEFORE_INBOX")
                original = custody
                supplied = group.eligibility.original
                decision, updated_authority = evaluate_entry(self.domain, authority, candidates[root], inbox[root][0], supplied,
                    authority_bindings.get(root, (None,))[0])
                eligibility = AuthorityReceipt(seq, group.command_id, "AUTHORITY_ELIGIBILITY", supplied, decision,
                    authority.content_digest, updated_authority.content_digest, commits[seq]["previous_digest"])
                if eligibility != group.eligibility or request.track != supplied.track:
                    raise LedgerConflict("AUTHORITY_ADMISSION_ORIGINAL_ELIGIBILITY_CONFLICT")
                support = inputs[group.wallet_support_digest]
                comparison, compared = compare_wallet(self.domain, custody, support, common_revision=seq-1,
                    common_digest=commits[seq]["previous_digest"], pending_attempts=() if lane is None else (lane,),
                    chain_anchors=(anchor for facts in chain_facts.values() for anchor in facts.anchors))
                comparison_receipt = LedgerComparisonReceipt(seq, group.command_id, support, comparison, custody.content_digest, compared.content_digest)
                child_comparison = authority_children[seq]["WALLET_COMPARISON"][2][0]
                if canonical_json(comparison_receipt.to_record()) != canonical_json(child_comparison):
                    raise LedgerConflict("AUTHORITY_ADMISSION_ORIGINAL_COMPARISON_CONFLICT")
                staged = next((value for value in live_actions.values() if value.root_id == root and value.side == "BUY"), None)
                risk = assess_entry_risk(self.domain, custody, candidates[root], request, eligibility, authority.policy,
                    support, comparison, accepted=accepted_authority,
                    policies={value.content_digest: value for value in authority_policies.values()}, applications=applications,
                    pending_attempts=() if lane is None else (lane,), staged_action=staged, spent_grants=spent_authority, actions=live_actions)
                admission = consumed = denial_digest = None
                custody = compared
                if risk.disposition == "ADMISSIBLE_STORAGE_ONLY":
                    cut = ConsumerCut(self.domain.economic_domain_id, seq-1, commits[seq]["previous_digest"], original.content_digest)
                    terms = admission_terms(self.domain, candidates[root], request, eligibility, risk, authority.policy, cut, group.command_id, staged)
                    root_attempts = tuple(value for value in live_attempts.values() if live_actions[value.preparation.action_id].root_id == root)
                    prior = custody.content_digest
                    custody = admit_reservation(self.domain, custody, candidates[root], inbox[root][0], root_attempts, terms, sequence=seq)
                    created = terms.action.content_digest if staged is None else None
                    admission = ConsumerPortReceipt(seq, group.command_id, "ADMISSION", terms, None, None, None,
                        created, None, None, None, None, (), prior, custody.content_digest)
                    if admission != authority_children[seq]["ADMISSION"][2]:
                        raise LedgerConflict("AUTHORITY_ADMISSION_ORIGINAL_TERMS_CONFLICT")
                    live_actions[terms.action.action_id] = terms.action
                    inbox[root] = ("ACCEPTED", inbox[root][1], terms.recorded_at_utc)
                    consumed = authority.grant.grant_id if authority.grant.scope == "ENTRY_ONCE" else None
                else:
                    at = eligibility.original.clock.utc_lower_utc
                    eligible_denial = (not decision.clock_reasons and utc_microseconds(at) >= candidates[root].generated_at_us
                        and not any(value.root_id == root for value in custody.reservations) and inbox[root][0] not in TERMINAL_INBOX
                        and not any(live_actions[value.preparation.action_id].root_id == root and value.recorded_stage != "CANCELLED_UNSIGNED" for value in live_attempts.values())
                        and (not inbox[root][2] or at >= inbox[root][2]))
                    if eligible_denial:
                        record = {"version": "live_ledger_nonacceptance_v0.1", "root_id": root, "ordinal": inbox[root][1]+1,
                            "idempotency_key": group.command_id, "from_disposition": inbox[root][0],
                            "disposition": "EXPIRED" if decision.disposition == "EXPIRED" else "DENIED_RETRYABLE",
                            "external_reference": group.command_id, "external_record_digest": risk.content_digest,
                            "recorded_at_utc": at, "has_real_authority_grant": False}
                        denial_digest = content_fingerprint(record)
                        if authority_children[seq].get("NON_ACCEPTANCE", (None, None, None))[2] != record:
                            raise LedgerConflict("AUTHORITY_ORIGINAL_NONACCEPTANCE_CONFLICT")
                        inbox[root] = (record["disposition"], record["ordinal"], at)
                expected = AuthorityAdmissionReceipt(seq, group.command_id, request, candidates[root].mint, eligibility, support.digest,
                    comparison_receipt.content_digest, risk, None if admission is None else admission.content_digest, consumed,
                    denial_digest, original.content_digest, custody.content_digest, commits[seq]["previous_digest"])
                if expected != group:
                    raise LedgerConflict("AUTHORITY_ADMISSION_ORIGINAL_RISK_OR_CONSUMPTION_CONFLICT")
                if group.accepted:
                    if root in accepted_authority or any(value.mint == group.mint for value in accepted_authority.values()) or consumed is not None and consumed in spent_authority:
                        raise LedgerConflict("AUTHORITY_PERMANENT_ACCEPTANCE_OR_GRANT_CONFLICT")
                    accepted_authority[root] = group
                    if consumed is not None:
                        spent_authority.add(consumed)
                authority = updated_authority
                authority_bindings.setdefault(root, (decision.binding, seq))
                comparisons[group.command_id] = comparison_receipt
            elif kind == "INBOX":
                root, _candidate = item
                inbox[root] = ("RECEIVED", 0, "")
            elif kind == "ACTION":
                if item.root_id not in inbox or inbox[item.root_id][0] in TERMINAL_INBOX or baseline_seq is None or seq <= baseline_seq:
                    raise LedgerJournalError("LEDGER_ACTION_BEFORE_BASELINE_OR_AFTER_TOMBSTONE")
                require_admitted_action(custody, item)
                live_actions[item.action_id] = item
            elif kind == "NON_ACCEPTANCE":
                root = item["root_id"]
                state = inbox.get(root)
                if (any(value.root_id == root for value in custody.reservations) or state is None or state[0] in TERMINAL_INBOX or item["from_disposition"] != state[0]
                        or item["ordinal"] != state[1]+1 or item["recorded_at_utc"] < state[2]
                        or utc_microseconds(item["recorded_at_utc"]) < candidates[root].generated_at_us
                        or any(live_actions[value.preparation.action_id].root_id == root and value.recorded_stage != "CANCELLED_UNSIGNED"
                               for value in live_attempts.values())):
                    raise LedgerJournalError("LEDGER_NONACCEPTANCE_DESTROYS_UNRESOLVED_ATTEMPT_OR_TOMBSTONE")
                inbox[root] = (item["disposition"], item["ordinal"], item["recorded_at_utc"])
            elif kind == "ATTEMPT":
                action = live_actions.get(item.action_id)
                if action is not None:
                    require_admitted_action(custody, action)
                prior_ids = per_action.get(item.action_id, [])
                previous = None if not prior_ids else live_attempts[prior_ids[-1]]
                resolution = None if previous is None else custody.resolution(previous.preparation.attempt_id)
                if (action is None or inbox[action.root_id][0] in TERMINAL_INBOX or lane is not None
                        or custody.quarantined or custody.anchor is not None and (item.finalized_lower_anchor.slot < custody.anchor.slot
                            or not _known_finalized_anchors_consistent(custody.anchor, item.finalized_lower_anchor))
                        or any(not _known_finalized_anchors_consistent(anchor, item.finalized_lower_anchor) for anchor in custody.qualified_wallet_anchors)
                        or any(not _known_finalized_anchors_consistent(anchor, item.finalized_lower_anchor)
                               for facts in chain_facts.values() for anchor in facts.anchors)
                        or item.ordinal != len(prior_ids)+1
                        or previous is not None and (previous.recorded_stage != "CANCELLED_UNSIGNED" and
                            (resolution is None or resolution.disposition not in REPLACEABLE)
                            or item.prepared_at_utc < (previous.last_recorded_at_utc if resolution is None else resolution.recorded_at_utc))):
                    raise LedgerJournalError("LEDGER_ATTEMPT_LANE_OR_ORDINAL_REPLAY_CONFLICT")
                live_attempts[item.attempt_id] = StoredAttempt(item, "PREPARED", 0, commits[seq]["generation"], item.prepared_at_utc, None, None, True)
                per_action.setdefault(item.action_id, []).append(item.attempt_id)
                lane, lane_revision = item.attempt_id, seq
            elif kind == "CHAIN_EVIDENCE":
                previous = live_attempts.get(item.attempt_id)
                if (previous is None or item.attempt_id not in signed_inputs or (lane != item.attempt_id and custody.resolution(item.attempt_id) is None)
                        or baseline_receipt is None or seq <= baseline_receipt.sequence):
                    raise LedgerJournalError("LEDGER_CHAIN_RECEIPT_BEFORE_DURABLE_LINEAGE")
                decision, retained = adjudicate_chain_observation(self.domain, previous, *signed_inputs[item.attempt_id],
                    baseline_receipt.observation.anchor, item.observation, evaluated_at_utc=item.decision.evaluated_at_utc,
                    retained=chain_facts.get(item.attempt_id))
                expected = LedgerChainReceipt(seq, item.ingestion_key, item.attempt_id, previous.revision,
                    previous.preparation.content_digest, previous.signed_wire_digest, baseline_receipt.content_digest,
                    commits[seq]["previous_digest"], commits[seq]["generation"], commits[seq]["generation_digest"], item.observation, decision)
                if canonical_json(expected.to_record()) != canonical_json(item.to_record()):
                    raise LedgerJournalError("LEDGER_ORIGINAL_CHAIN_DECISION_REPLAY_CONFLICT")
                chain_facts[item.attempt_id] = retained
                chain_seen[item.ingestion_key] = item
                custody = retain_chain_custody(custody, retained)
            elif kind == "SETTLEMENT_APPLICATION":
                replay_application(seq, item)
            elif kind == "WALLET_COMPARISON":
                record, support = item
                comparison, updated = compare_wallet(self.domain, custody, support, common_revision=seq-1,
                    common_digest=commits[seq]["previous_digest"], pending_attempts=() if lane is None else (lane,),
                    chain_anchors=(anchor for facts in chain_facts.values() for anchor in facts.anchors))
                expected = LedgerComparisonReceipt(seq, record["ingestion_key"], support, comparison, custody.content_digest, updated.content_digest)
                if canonical_json(expected.to_record()) != canonical_json(record):
                    raise LedgerConflict("LEDGER_ORIGINAL_WALLET_COMPARISON_REPLAY_CONFLICT")
                comparisons[expected.ingestion_key] = expected
                custody = updated
            elif kind == "ATTEMPT_STAGE":
                previous = live_attempts.get(item["attempt_id"])
                if (previous is None or lane != item["attempt_id"]
                        or inbox[live_actions[previous.preparation.action_id].root_id][0] in TERMINAL_INBOX):
                    raise LedgerJournalError("LEDGER_STAGE_AFTER_TOMBSTONE_OR_WITHOUT_LANE")
                chain = chain_facts.get(item["attempt_id"])
                if chain is not None and (chain.state.positive_finality is not None or chain.state.quarantined):
                    raise LedgerJournalError("LEDGER_EXTERNAL_STAGE_AFTER_CHAIN_FINALITY")
                require_admitted_action(custody, live_actions[previous.preparation.action_id])
                value = self._replay_stage(previous, item, commits[seq]["generation"])
                if item["to_stage"] == "EXACT_SIMULATED":
                    if item["attempt_id"] in simulations:
                        raise LedgerConflict("LEDGER_DUPLICATE_SIMULATION_REPLAY")
                    simulations[item["attempt_id"]] = content_fingerprint(stage_from_record(item["stage_input"]).to_record())
                if value.primary_signature is not None and previous.primary_signature is None:
                    if value.primary_signature in signatures:
                        raise LedgerJournalError("LEDGER_SIGNATURE_REUSED_BY_ANOTHER_ATTEMPT")
                    signatures[value.primary_signature] = (value.preparation.attempt_id, value.signed_wire_digest)
                    stage = stage_from_record(item["stage_input"])
                    signed_inputs[item["attempt_id"]] = (bytes(stage.signed_public_transaction()), stage.recorded_at_utc)
                live_attempts[item["attempt_id"]] = value
                lane, lane_revision = (item["attempt_id"] if value.lane_held else None), seq
            elif kind in GROUP_KINDS:
                group = item
                supplied = next(value for value in (group.admission, group.handoff, group.retirement, group.dry_terminal) if value is not None)
                require_common_cut(self.domain, custody, supplied.cut, seq-1, commits[seq]["previous_digest"])
                original_digest = custody.content_digest
                application = None
                if group.application_key is not None:
                    child_record = children[seq]["SETTLEMENT_APPLICATION"][2][0]
                    child_attempt = live_attempts.get(child_record["attempt_id"])
                    child_action = None if child_attempt is None else live_actions.get(child_attempt.preparation.action_id)
                    if (child_action is None or (supplied.root_id, supplied.position_id) != (child_action.root_id, child_action.position_id)
                            or group.handoff is not None and supplied.acquisition_signature != child_attempt.primary_signature
                            or group.retirement is not None and supplied.terminal_attempt_id != child_record["attempt_id"]):
                        raise LedgerConflict("LEDGER_GROUP_SETTLEMENT_LINEAGE_REPLAY_CONFLICT")
                    application = replay_application(seq, children[seq]["SETTLEMENT_APPLICATION"][2])
                    if application.recorded_at_utc != supplied.recorded_at_utc:
                        raise LedgerConflict("LEDGER_GROUP_TIME_REPLAY_CONFLICT")
                root = supplied.action.root_id if group.admission is not None else supplied.root_id
                root_attempts = tuple(value for value in live_attempts.values() if live_actions[value.preparation.action_id].root_id == root)
                comparison_digest, disposition, reasons = None, None, ()
                if group.admission is not None:
                    if root not in inbox or root not in candidates or baseline_seq is None or seq <= baseline_seq:
                        raise LedgerConflict("LEDGER_ADMISSION_REPLAY_PARENT_MISSING")
                    action = supplied.action
                    previous = live_actions.get(action.action_id)
                    if group.created_action_digest is not None:
                        if previous is not None or children[seq]["ACTION"][2] != action:
                            raise LedgerConflict("LEDGER_ADMISSION_GROUP_ACTION_REPLAY_CONFLICT")
                        live_actions[action.action_id] = action
                    elif previous != action:
                        raise LedgerConflict("LEDGER_ADMISSION_STAGED_ACTION_REPLAY_CONFLICT")
                    custody = admit_reservation(self.domain, custody, candidates[root], inbox[root][0], root_attempts, supplied, sequence=seq)
                    inbox[root] = ("ACCEPTED", inbox[root][1], supplied.recorded_at_utc)
                elif group.handoff is not None:
                    resolution = next((value for value in custody.resolutions if value.signature == supplied.acquisition_signature), None)
                    acquisition = None if resolution is None else next((value for value in applications.values() if value.sequence == resolution.application_sequence), None)
                    chain = None if acquisition is None else chain_seen.get(acquisition.chain_receipt_key)
                    custody = activate_protection(self.domain, custody, supplied, acquisition, chain, sequence=seq)
                elif group.retirement is not None:
                    support = inputs[supplied.wallet_support_digest]
                    if support.evaluated_at_utc != supplied.recorded_at_utc or support.required_min_context_slot < custody.anchor.slot:
                        raise LedgerConflict("LEDGER_RETIREMENT_RESULTING_CUT_REPLAY_CONFLICT")
                    comparison, custody = compare_wallet(self.domain, custody, support, common_revision=seq-1,
                        common_digest=commits[seq]["previous_digest"], pending_attempts=() if lane is None else (lane,),
                        chain_anchors=(anchor for facts in chain_facts.values() for anchor in facts.anchors))
                    resolution = custody.resolution(supplied.terminal_attempt_id)
                    terminal_application = None if resolution is None else next((value for value in applications.values() if value.sequence == resolution.application_sequence), None)
                    custody, disposition, reasons = retire_reservation(self.domain, custody, supplied, root_attempts,
                        terminal_application, comparison, sequence=seq)
                    comparison_digest = content_fingerprint(asdict(comparison))
                    if disposition == "RETIRED":
                        reservation = next(value for value in custody.reservations if value.reservation_id == supplied.reservation_id)
                        inbox[root] = (reservation.terminal_disposition, inbox[root][1], supplied.recorded_at_utc)
                else:
                    custody = terminate_dry(self.domain, custody, supplied, root_attempts,
                        simulations.get(supplied.attempt_id), sequence=seq)
                    if any(value.recorded_stage != "CANCELLED_UNSIGNED" and lane != value.preparation.attempt_id for value in root_attempts):
                        raise LedgerConflict("LEDGER_DRY_LINEAGE_LANE_REPLAY_CONFLICT")
                    if supplied.attempt_id is not None and lane == supplied.attempt_id:
                        lane, lane_revision = None, seq
                    inbox[root] = ("NON_SUBMITTED", inbox[root][1], supplied.recorded_at_utc)
                expected = ConsumerPortReceipt(seq, group.ingestion_key, kind, group.admission, group.handoff, group.retirement, group.dry_terminal,
                    group.created_action_digest, None if application is None else application.ingestion_key,
                    None if application is None else application.content_digest, comparison_digest, disposition, reasons,
                    original_digest, custody.content_digest, None if application is None else application.decision.disposition)
                if canonical_json(expected.to_record()) != canonical_json(group.to_record()):
                    raise LedgerConflict("LEDGER_ORIGINAL_CONSUMER_GROUP_REPLAY_CONFLICT")
            else:
                raise LedgerConflict("LEDGER_UNKNOWN_ECONOMIC_HISTORY_KIND")
        if self._lane() != (lane, lane_revision):
            raise LedgerJournalError("LEDGER_MUTATION_LANE_RECONSTRUCTION_CONFLICT")
        if {signature: (attempt_id, wire) for signature, attempt_id, wire in self._conn.execute("SELECT * FROM ledger_signature_bindings")} != signatures:
            raise LedgerJournalError("LEDGER_SIGNATURE_BINDINGS_RECONSTRUCTION_CONFLICT")
        if sorted(self._conn.execute("SELECT * FROM ledger_economic_postings").fetchall()) != sorted(postings):
            raise LedgerConflict("LEDGER_ECONOMIC_POSTING_RECONSTRUCTION_CONFLICT")
        self._verify_custody_projection(custody)
        expected_bindings = sorted((root, seq, content_fingerprint(asdict(binding)), canonical_json(asdict(binding)))
            for root, (binding, seq) in authority_bindings.items())
        if sorted(self._conn.execute("SELECT * FROM ledger_authority_bindings").fetchall()) != expected_bindings:
            raise LedgerConflict("AUTHORITY_ORIGINAL_ENTRY_BINDING_REPLAY_CONFLICT")
        if self._conn.execute("SELECT * FROM ledger_authority_projection").fetchall() != [
                (1, authority.content_digest, canonical_json(asdict(authority)))]:
            raise LedgerConflict("AUTHORITY_CONTROL_PROJECTION_REPLAY_CONFLICT")
        return records, chain_facts, custody, applications, comparisons, authority

    def audit(self) -> dict:
        with self._trusted_read():
            return self._audit()

    def _audit(self) -> dict:
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
                "attempt_count": self._conn.execute("SELECT COUNT(*) FROM ledger_attempts").fetchone()[0],
                "chain_receipt_count": self._conn.execute("SELECT COUNT(*) FROM ledger_chain_receipts").fetchone()[0],
                "application_receipt_count": len(self._applications), "comparison_receipt_count": len(self._comparisons),
                "posting_count": self._conn.execute("SELECT COUNT(*) FROM ledger_economic_postings").fetchone()[0],
                "custody_digest": self._custody.content_digest, "authority_digest": self._authority.content_digest,
                "authority_receipt_count": self._conn.execute("SELECT COUNT(*) FROM ledger_authority_records").fetchone()[0]}

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
            if decision.disposition == "ESTABLISHED":
                updated = baseline_custody(self.domain, receipt)
                self._store_custody_projection(updated)
                self._pending_custody_bundle = (updated, self._applications, self._comparisons)
            self._commit()
            return receipt
        except BaseException as exc:
            self._pending_custody_bundle = None
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
