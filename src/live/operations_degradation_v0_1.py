"""Durable, sanitized Operations conditions; never an economic permission.

The trusted Operations consumer submits current owner evidence, not exceptions.
Subject digests bind the original source lineage, attempt, custody, action or
control identity. Changing subjects cannot recover an old subject. Recovery
codes describe evidence the consumer must actually verify at those owners;
this store cannot establish chain truth, reset an owner or erase an obligation.

Readiness/supervisor projections below only discover active conditions. Their
absence is never positive recovery evidence. Runtime integration is separate.
"""
from __future__ import annotations

import os
import sqlite3
from time import monotonic
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, field

from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from .ledger_domain_v0_1 import LedgerDomain, digest_value
from .ledger_repository_v0_1 import _journal_path
from .operations_ownership_v0_1 import OperationsStore

VERSION = "live_operations_degradation_v0.1"
# condition -> affected scope, positive recovery code, delayed alert
CONDITIONS = {
    "SOURCE_TRUTH_UNAVAILABLE": ("ENTRY_AND_PRICE", "SAME_LINEAGE_CONTINUITY_VERIFIED", False),
    "SOURCE_IDENTITY_OR_HISTORY_BROKEN": ("ENTRY_AND_PRICE", "SAME_LINEAGE_CONTINUITY_VERIFIED", False),
    "PRODUCER_INTEGRITY_UNAVAILABLE": ("ENTRY", "EXACT_CHECKPOINT_AND_TAIL_VERIFIED", False),
    "PROFILE_UNRESOLVED": ("ENTRY", "REVIEWED_PROFILE_AND_METRICS_VERIFIED", False),
    "RESOURCE_EXCEEDED": ("ENTRY", "SAME_PROFILE_RESOURCES_WITHIN_LIMITS", False),
    "OWNER_FENCE_UNPROVEN": ("ALL_MUTATION", "EXCLUSIVE_OWNER_AND_BARRIERS_VERIFIED", False),
    "OPERATOR_STOPPED": ("ALL_MUTATION", "AUTHORIZED_RELEASE_AND_BARRIERS_VERIFIED", False),
    "RESTART_EXHAUSTED": ("ALL_MUTATION", "AUTHORIZED_BUDGET_RESET_AND_OWNER_VERIFIED", False),
    "SUPERVISOR_HELD": ("ALL_MUTATION", "EXPLICIT_HOST_RECOVERY_AND_OWNER_VERIFIED", False),
    "UNSENT_ATTEMPT_RECOVERY_REQUIRED": ("WALLET_MUTATION", "ORIGINAL_ATTEMPT_AND_CAPACITY_ADJUDICATION_VERIFIED", False),
    "SEND_UNKNOWN": ("WALLET_MUTATION", "EXACT_FINAL_OUTCOME_OR_NONLANDING_VERIFIED", True),
    "FINALITY_UNRESOLVED": ("WALLET_MUTATION", "CONSISTENT_EXACT_FINALITY_VERIFIED", True),
    "SETTLEMENT_UNKNOWN": ("CUSTODY_AND_FUNDING", "EXACT_BALANCED_SETTLEMENT_VERIFIED", True),
    "CUSTODY_TRUTH_UNAVAILABLE": ("CUSTODY_AND_FUNDING", "RECONCILED_CLASSIFIED_CUSTODY_VERIFIED", False),
    "CLOCK_UNPROVEN": ("TIME_SENSITIVE_MUTATION", "TRUSTED_BOUNDS_AND_CONTINUITY_VERIFIED", False),
    "PROTECTIVE_ACTION_UNAVAILABLE": ("ACTION", "CURRENT_ORIGINAL_ACTION_OR_SATISFACTION_VERIFIED", False),
    "ECONOMIC_INTEGRITY_UNAVAILABLE": ("ALL_MUTATION", "INTACT_ECONOMIC_REPLAY_AND_POLICY_VERIFIED", False),
}
REQUIRED_METRICS = frozenset(("HOST_RSS_BYTES", "HOST_DISK_RESERVE_BYTES", "STARTUP_US",
    "PROTECTIVE_STEP_US", "AGGREGATE_FEATURE_BYTES", "HOTTEST_FEATURE_BYTES",
    "UNFINISHED_MINTS", "NO_T0_MINTS", "IDENTITY_ROWS", "TOMBSTONE_ROWS",
    "PRODUCER_PENDING_ROWS", "LEDGER_TAIL_ROWS", "LEDGER_TAIL_PAYLOAD_BYTES",
    "OLDEST_UNCONSUMED_AGE_US"))
# Additive deployed-envelope dimensions. Original policies still require exactly
# the original coverage; these dimensions are enforced only when configured.
METRICS = REQUIRED_METRICS | frozenset(("RETAINED_EVENTS", "HOTTEST_EVENTS",
    "RETAINED_SERIALIZED_BYTES", "PRODUCER_PENDING_BYTES", "HISTORY_BYTES", "CHECKPOINT_BYTES"))
_DDLS = (
    "CREATE TABLE metadata (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version TEXT NOT NULL, domain_digest TEXT NOT NULL, binding_digest TEXT NOT NULL, policy_digest TEXT NOT NULL, last_us INTEGER NOT NULL)",
    "CREATE TABLE conditions (condition TEXT NOT NULL, subject_digest TEXT NOT NULL, episode INTEGER NOT NULL, state TEXT NOT NULL, first_us INTEGER NOT NULL, last_us INTEGER NOT NULL, observed_us INTEGER NOT NULL, evidence_digest TEXT NOT NULL, active_digest TEXT NOT NULL, alerted INTEGER NOT NULL, alerted_us INTEGER, recovered_us INTEGER, reason TEXT NOT NULL, PRIMARY KEY(condition,subject_digest))",
    "CREATE TABLE receipts (digest TEXT PRIMARY KEY NOT NULL)",
)


def require(value, code="OPERATIONS_DEGRADATION_CONTRACT_INVALID"):
    if not value:
        raise ValueError(code)


def digest(value):
    try:
        digest_value(value)
    except (ValueError, TypeError):
        raise ValueError("OPERATIONS_DEGRADATION_DIGEST_REQUIRED") from None


@dataclass(frozen=True, slots=True)
class ResourceLimit:
    metric: str
    limit: int

    def __post_init__(self):
        require(self.metric in METRICS and type(self.limit) is int and 0 < self.limit < 2**63)


@dataclass(frozen=True, slots=True)
class DegradationPolicy:
    reviewed_configuration_digest: str
    persistent_unknown_alert_us: int
    recovery_evidence_max_age_us: int
    resource_limits: tuple[ResourceLimit, ...]

    def __post_init__(self):
        digest(self.reviewed_configuration_digest)
        for value in (self.persistent_unknown_alert_us, self.recovery_evidence_max_age_us):
            require(type(value) is int and 0 < value <= 86400000000)
        require(type(self.resource_limits) is tuple
            and all(type(item) is ResourceLimit for item in self.resource_limits))
        require(len({item.metric for item in self.resource_limits}) == len(self.resource_limits))

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


def resource_condition(policy, metric, value, *, configuration_digest):
    """No defaults or C1 maxima. Missing/unsupported/config-mismatched is held.

    None means only this particular measured metric is within its reviewed
    threshold; it is never a recovery instruction or whole-profile approval.
    """
    require(type(policy) is DegradationPolicy)
    limit = next((item.limit for item in policy.resource_limits if item.metric == metric), None)
    if (configuration_digest != policy.reviewed_configuration_digest or limit is None
            or type(value) is not int or value < 0 or value >= 2**63):
        return "PROFILE_UNRESOLVED"
    exceeded = value < limit if metric == "HOST_DISK_RESERVE_BYTES" else value > limit
    return "RESOURCE_EXCEEDED" if exceeded else None


@dataclass(frozen=True, slots=True)
class ConditionEvidence:
    condition: str
    subject_digest: str
    observed_us: int
    evidence_digest: str
    state: str = "ACTIVE"
    reason: str = "OWNER_CONDITION_OBSERVED"
    expected_active_digest: str | None = None

    def __post_init__(self):
        require(type(self.condition) is str and self.condition in CONDITIONS)
        digest(self.subject_digest)
        digest(self.evidence_digest)
        OperationsStore._time(self.observed_us)
        require(self.state in ("ACTIVE", "RECOVERED"))
        if self.state == "ACTIVE":
            require(self.reason == "OWNER_CONDITION_OBSERVED" and self.expected_active_digest is None)
        else:
            require(self.reason == CONDITIONS[self.condition][1])
            digest(self.expected_active_digest)

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class ConditionState:
    condition: str
    subject_digest: str
    episode: int
    state: str
    first_us: int
    last_us: int
    observed_us: int
    evidence_digest: str
    active_digest: str
    alerted: int
    alerted_us: int | None
    recovered_us: int | None
    reason: str

    @property
    def scope(self):
        return CONDITIONS[self.condition][0]

    @property
    def action(self):
        if self.state == "RECOVERED":
            return "REVALIDATE_ORIGINAL_BARRIERS"
        return {
            "UNSENT_ATTEMPT_RECOVERY_REQUIRED": "REVIEW_ORIGINAL_UNSENT_ATTEMPT_NO_AUTOMATIC_RETRY",
            "SOURCE_IDENTITY_OR_HISTORY_BROKEN": "INVESTIGATE_ORIGINAL_LINEAGE_AND_HISTORY",
            "PRODUCER_INTEGRITY_UNAVAILABLE": "INVESTIGATE_CHECKPOINT_AND_TAIL",
            "PROFILE_UNRESOLVED": "SUPPLY_REVIEWED_PROFILE_AND_SUPPORTED_METRICS",
            "RESOURCE_EXCEEDED": "RESTORE_REVIEWED_RESOURCE_ENVELOPE",
            "OWNER_FENCE_UNPROVEN": "PROVE_EXCLUSIVE_OWNER_AND_REVALIDATE",
            "OPERATOR_STOPPED": "OBTAIN_AUTHORIZED_RELEASE_AND_REVALIDATE",
            "RESTART_EXHAUSTED": "INVESTIGATE_THEN_AUTHORIZED_BUDGET_RESET",
            "SUPERVISOR_HELD": "INVESTIGATE_HOST_AND_EXACT_CHILD",
            "CUSTODY_TRUTH_UNAVAILABLE": "INVESTIGATE_AND_CLASSIFY_CUSTODY",
            "ECONOMIC_INTEGRITY_UNAVAILABLE": "INVESTIGATE_ORIGINAL_ECONOMIC_EVIDENCE",
        }.get(self.condition, "RETAIN_HOLD_AND_OBTAIN_OWNER_EVIDENCE")


@dataclass(frozen=True, slots=True)
class DegradationFacts:
    conditions: tuple[ConditionState, ...]
    entry_held: bool
    affected_scopes: tuple[str, ...]
    grants_permission: bool = field(init=False, default=False)
    may_sign: bool = field(init=False, default=False)
    may_send: bool = field(init=False, default=False)


def _facts(rows):
    active = tuple(row for row in rows if row.state == "ACTIVE")
    return DegradationFacts(tuple(rows), bool(active), tuple(sorted({row.scope for row in active})))


class DegradationStore:
    """Independent incident journal. No acknowledgement/reset/release API.

    record() consumes explicitly verified evidence. advance() only escalates
    alerts. snapshot() never advances clocks or changes holds. The consumer
    must fail closed if this store cannot be read, and must combine entry_held
    with original current readiness; false is never permission. Call advance
    with a trusted time even when no fresh owner observation can be obtained.
    """
    def __init__(self, path, domain, policy):
        require(type(domain) is LedgerDomain and domain.mode in ("LIVE", "DRY"))
        require(type(policy) is DegradationPolicy)
        self._busy_deadline = None
        self.path, self.policy = _journal_path(path), policy
        self.domain_digest, self.binding_digest = domain.economic_domain_id, domain.binding_digest
        stat = self.path.stat()
        self._identity = stat.st_dev, stat.st_ino
        with self._connection() as conn:
            conn.execute("BEGIN")
            require(conn.execute("PRAGMA quick_check").fetchone()[0] == "ok",
                "OPERATIONS_DEGRADATION_INTEGRITY_REQUIRED")
            self._read(conn)

    @classmethod
    def initialize(cls, path, domain, policy, *, now_us):
        require(type(domain) is LedgerDomain and domain.mode in ("LIVE", "DRY") and type(policy) is DegradationPolicy)
        OperationsStore._time(now_us)
        path = _journal_path(path)
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        os.close(fd)
        with closing(sqlite3.connect(path)) as conn:
            conn.execute("PRAGMA synchronous=FULL")
            for ddl in _DDLS:
                conn.execute(ddl)
            conn.execute("INSERT INTO metadata VALUES (1,?,?,?,?,?)", (VERSION,
                domain.economic_domain_id, domain.binding_digest, policy.content_digest, now_us))
            conn.commit()
        return cls(path, domain, policy)

    @contextmanager
    def contention_window(self):
        """One finite SQLite contention allowance for a composed observation/step."""
        previous = self._busy_deadline
        if previous is None:
            self._busy_deadline = monotonic()+1.0
        try:
            yield
        finally:
            self._busy_deadline = previous

    def _busy_seconds(self):
        return 1.0 if self._busy_deadline is None else max(0.0, self._busy_deadline-monotonic())

    def _commit(self, conn):
        conn.execute("PRAGMA busy_timeout="+str(int(1000*self._busy_seconds())))
        conn.commit()

    @contextmanager
    def _connection(self):
        try:
            require(_journal_path(self.path) == self.path and self.path.is_file(), "OPERATIONS_DEGRADATION_STORE_REQUIRED")
            stat = self.path.stat()
            require((stat.st_dev, stat.st_ino) == self._identity, "OPERATIONS_DEGRADATION_STORE_REPLACED")
            with closing(sqlite3.connect(self.path.as_uri()+"?mode=rw", uri=True,
                    timeout=self._busy_seconds(), isolation_level=None)) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA synchronous=FULL")
                yield conn
        except (sqlite3.DatabaseError, OSError):
            raise ValueError("OPERATIONS_DEGRADATION_STORE_UNAVAILABLE") from None

    def _read(self, conn):
        objects = conn.execute("SELECT sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").fetchall()
        require(sorted(" ".join(row[0].split()) for row in objects)
            == sorted(" ".join(ddl.split()) for ddl in _DDLS), "OPERATIONS_DEGRADATION_SCHEMA_CONFLICT")
        meta = conn.execute("SELECT * FROM metadata").fetchall()
        require(len(meta) == 1 and tuple(meta[0])[:5] == (1, VERSION, self.domain_digest,
            self.binding_digest, self.policy.content_digest), "OPERATIONS_DEGRADATION_IDENTITY_CONFLICT")
        last_us = meta[0]["last_us"]
        OperationsStore._time(last_us)
        rows = tuple(ConditionState(**dict(row)) for row in conn.execute(
            "SELECT * FROM conditions ORDER BY condition,subject_digest"))
        for row in rows:
            evidence = ConditionEvidence(row.condition, row.subject_digest, row.observed_us, row.evidence_digest,
                row.state, row.reason, row.active_digest if row.state == "RECOVERED" else None)
            digest(row.active_digest)
            require((row.state != "ACTIVE" or evidence.content_digest == row.active_digest)
                and conn.execute("SELECT 1 FROM receipts WHERE digest=?", (row.active_digest,)).fetchone()
                and conn.execute("SELECT 1 FROM receipts WHERE digest=?", (evidence.content_digest,)).fetchone(),
                "OPERATIONS_DEGRADATION_RECEIPT_CONFLICT")
            require(type(row.episode) is int and row.episode > 0 and row.alerted in (0, 1))
            for value in (row.first_us, row.last_us):
                OperationsStore._time(value)
            require(row.first_us <= row.last_us <= last_us and row.observed_us <= row.last_us)
            require((row.state == "ACTIVE" and row.recovered_us is None) or
                (row.state == "RECOVERED" and row.recovered_us == row.last_us))
            require((row.alerted == 0 and row.alerted_us is None) or
                (row.alerted == 1 and type(row.alerted_us) is int and row.first_us <= row.alerted_us <= last_us))
            require(row.alerted == 1 or CONDITIONS[row.condition][2])
        for receipt in conn.execute("SELECT digest FROM receipts"):
            digest(receipt[0])
        return last_us, rows

    def snapshot(self):
        with self._connection() as conn:
            conn.execute("BEGIN")
            return _facts(self._read(conn)[1])

    def _escalate(self, conn, now_us):
        for code, (_, _, delayed) in CONDITIONS.items():
            if delayed:
                conn.execute("UPDATE conditions SET alerted=1,alerted_us=coalesce(alerted_us,?) WHERE condition=? AND state='ACTIVE' AND ?-first_us>=?",
                    (now_us, code, now_us, self.policy.persistent_unknown_alert_us))
        conn.execute("UPDATE metadata SET last_us=?", (now_us,))

    def advance(self, *, now_us):
        return self._update(None, now_us)

    def record(self, evidence, *, now_us, coalesce_active=False):
        require(type(evidence) is ConditionEvidence and type(coalesce_active) is bool)
        return self._update(evidence, now_us, coalesce_active=coalesce_active)

    def _update(self, evidence, now_us, *, coalesce_active=False):
        OperationsStore._time(now_us)
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            last_us, rows = self._read(conn)
            if evidence is not None and conn.execute("SELECT 1 FROM receipts WHERE digest=?",
                    (evidence.content_digest,)).fetchone():
                return _facts(rows)  # Old replay cannot resurrect a recovered episode.
            # Independent parent/child samples can arrive out of capture order.
            # Only the ingestion watermark advances monotonically; never turn
            # that watermark into observation time or fresh recovery evidence.
            watermark_us = max(now_us, last_us)
            if evidence is not None:
                require(evidence.observed_us <= now_us, "OPERATIONS_DEGRADATION_FUTURE_EVIDENCE")
                old = next((row for row in rows if (row.condition, row.subject_digest)
                    == (evidence.condition, evidence.subject_digest)), None)
                require(old is None or evidence.observed_us > old.last_us,
                    "OPERATIONS_DEGRADATION_STALE_EVIDENCE")
                recovering = evidence.state == "RECOVERED"
                if recovering:
                    require(old is not None and old.state == "ACTIVE"
                        and evidence.expected_active_digest == old.active_digest
                        and evidence.evidence_digest != old.evidence_digest
                        and watermark_us-evidence.observed_us <= self.policy.recovery_evidence_max_age_us,
                        "OPERATIONS_DEGRADATION_POSITIVE_RECOVERY_REQUIRED")
                continuing = old is not None and old.state == "ACTIVE"
                if continuing and not recovering and coalesce_active:
                    # Retain latest real active sample for stale-recovery fencing,
                    # but preserve the episode receipt and its original witness.
                    conn.execute("UPDATE conditions SET last_us=? WHERE condition=? AND subject_digest=?",
                        (now_us, evidence.condition, evidence.subject_digest))
                    self._escalate(conn, watermark_us)
                    result = _facts(self._read(conn)[1])
                    self._commit(conn)
                    return result
                episode = old.episode if continuing else (1 if old is None else old.episode+1)
                first_us = old.first_us if continuing else now_us
                alerted = old.alerted if continuing else int(not CONDITIONS[evidence.condition][2])
                row = ConditionState(evidence.condition, evidence.subject_digest, episode, evidence.state,
                    first_us, now_us, evidence.observed_us, evidence.evidence_digest,
                    old.active_digest if recovering else evidence.content_digest, alerted,
                    old.alerted_us if continuing else (now_us if alerted else None),
                    now_us if recovering else None, evidence.reason)
                conn.execute("INSERT OR REPLACE INTO conditions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", tuple(asdict(row).values()))
                conn.execute("INSERT INTO receipts VALUES (?)", (evidence.content_digest,))
            self._escalate(conn, watermark_us)
            result = _facts(self._read(conn)[1])
            self._commit(conn)
            return result


def readiness_conditions(facts):
    """Fixed active codes from actual ReadinessFacts; no automatic recovery.

    Caller binds each result to its actual source/control/action identity and
    common read-cut digest when constructing ConditionEvidence. Pending attempt
    alone is ordinary work: inspect original Ledger attempt/finality instead.
    """
    from .operations_readiness_v0_1 import ReadinessFacts
    require(type(facts) is ReadinessFacts)
    reasons = set(facts.entry.reasons) | set(facts.protective.reasons)
    found = set()
    mapping = {
        "CURRENT_OWNER_OR_EVIDENCE_UNAVAILABLE": "OWNER_FENCE_UNPROVEN",
        "GLOBAL_HARD_STOP_LATCHED": "OPERATOR_STOPPED",
        "CURRENT_RECONSTRUCTED_SOURCE_REQUIRED": "SOURCE_TRUTH_UNAVAILABLE",
        "CURRENT_PRODUCER_CHECKPOINT_UNAVAILABLE": "PRODUCER_INTEGRITY_UNAVAILABLE",
    }
    # An unselected original policy is an ENTRY prerequisite, not corrupt economic evidence.
    # The original readiness barrier retains CURRENT_AUTHORITY_POLICY_REQUIRED.
    found.update(code for reason, code in mapping.items() if reason in reasons)
    clock_codes = {"CLOCK_POLICY_MISSING", "CLOCK_PROVIDER_UNSUPPORTED", "CLOCK_UNKNOWN",
        "CLOCK_UNCERTAINTY_EXCEEDED", "CLOCK_CHECKPOINT_DIGEST_CONFLICT", "CLOCK_UNEXPECTED_RECONCILIATION",
        "CLOCK_BACKWARD_BOUNDS", "CLOCK_CONTINUITY_RECONCILIATION_REQUIRED", "CLOCK_MONOTONIC_REGRESSION",
        "CLOCK_CHECKPOINT_TOO_OLD", "CLOCK_UTC_MONOTONIC_DIVERGENCE"}
    if reasons & clock_codes:
        found.add("CLOCK_UNPROVEN")
    if facts.protective.due is True and facts.protective.state == "HELD":
        found.add("PROTECTIVE_ACTION_UNAVAILABLE")
    return tuple(sorted(found))


def supervision_conditions(facts):
    from .operations_supervisor_v0_1 import SupervisionFacts
    require(type(facts) is SupervisionFacts)
    states = {
        "OPERATOR_STOPPED": "OPERATOR_STOPPED", "RESTART_EXHAUSTED": "RESTART_EXHAUSTED",
        "HELD_EXISTING_OWNER": "OWNER_FENCE_UNPROVEN", "HELD_OWNER_CHANGED": "OWNER_FENCE_UNPROVEN", "HELD_CHILD_IDENTITY_CONFLICT": "OWNER_FENCE_UNPROVEN",
    }
    held = {"HELD_LAUNCH_UNPROVEN", "HELD_MONOTONIC_REGRESSION", "HELD_CONTROL_UNREADABLE",
        "HELD_CONTROL_CLOCK_REGRESSION", "HELD_EXIT_WITHOUT_EXACT_ACQUISITION", "HELD_NORMAL_EXIT",
        "HELD_TERMINATION_UNCONFIRMED", "HELD_CHILD_PROTOCOL_CONFLICT", "CHILD_CHANNEL_LOST"}
    if facts.state in states:
        return (states[facts.state],)
    if facts.state in held:
        return ("SUPERVISOR_HELD",)
    require(facts.state in {"STARTING", "RUNNING", "TERMINATING", "RESTART_BACKOFF"},
        "OPERATIONS_DEGRADATION_SUPERVISION_STATE_UNSUPPORTED")
    return ()
