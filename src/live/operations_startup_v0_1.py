"""Finite owned LIVE startup audit; original durable owners retain all truth.

The trusted host supplies a previously reviewed identity, including exact
producer/Evidence schema fingerprints. It must not discover/approve that identity
from possibly damaged stores during startup. No installation or readiness gate.
"""
from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from dataclasses import asdict, dataclass, field
from pathlib import Path

from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02
from phase5.shadow_domain_v0_1 import content_fingerprint
from . import authority_controls_v0_1 as authority
from . import candidate_handoff_v0_1 as handoff
from . import continuous_producer_v0_2 as producer
from . import ledger_domain_v0_1 as ledger_domain
from . import ledger_repository_v0_1 as repository
from . import operations_ownership_v0_1 as ownership
from . import runtime_composition_v0_1 as composition
from . import runtime_reconstruction_v0_1 as cold
from . import source_health_v0_1 as source
from . import execution_signer_v0_1 as signer
from . import execution_send_v0_1 as sender
from .ledger_actions_v0_1 import StoredAttempt

VERSION = "live_operations_startup_v0.1"
CAPABILITY = "LIVE_SINGLE_POSITION_OWNED"


@dataclass(frozen=True, slots=True)
class StartupIdentity:
    runtime_type: str
    runtime_version: str
    runtime_code_digest: str
    capability: str
    restart_profile: ownership.RestartProfile
    contracts: tuple[tuple[str, str], ...]
    configuration_digest: str
    producer_schema_digest: str
    evidence_schema_digest: str


def _schema_digest(conn):
    return content_fingerprint(tuple(conn.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").fetchall()))


def schema_fingerprint(path):
    """Review/install-time metadata helper, never automatic startup approval."""
    with closing(sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro", uri=True)) as conn:
        return _schema_digest(conn)


def configured_identity(domain, *, operations_path, ledger_path, producer_path,
                        market_source, producer_profile, source_path, source_binding,
                        source_profile, database_identity, producer_schema_digest,
                        evidence_schema_digest, restart_profile, batch_rows=32, page_rows=32, queued_roots=64):
    """Describe concrete loaded interfaces/config for comparison to reviewed input.

    Schema digests must come from the reviewed installation/configuration. This
    function neither opens stores nor validates durable history or grants access.
    """
    authority.require(type(domain) is ledger_domain.LedgerDomain and domain.mode == "LIVE"
        and type(market_source) is ContinuousMarketSourceV02
        and type(producer_profile) is producer.ContinuationProfileV02
        and type(restart_profile) is ownership.RestartProfile
        and type(source_binding) is source.SourceBinding and type(source_profile) is source.SourceProfile,
        "OPERATIONS_STARTUP_CONCRETE_LIVE_CONFIGURATION_REQUIRED")
    authority.require(database_identity == market_source.database_identity,
        "OPERATIONS_STARTUP_DATABASE_IDENTITY_CONFLICT")
    for digest in (producer_schema_digest, evidence_schema_digest):
        ledger_domain.digest_value(digest)
    contracts = (("operations", ownership.VERSION), ("startup", VERSION),
        ("ledger_storage", str(repository.STORAGE_VERSION)), ("ledger_schema", ledger_domain.SCHEMA_VERSION),
        ("ledger_domain", ledger_domain.DOMAIN_VERSION), ("authority", authority.VERSION),
        ("evidence", source.SCHEMA), ("producer_schema", producer.SCHEMA_VERSION),
        ("producer_storage", producer.STORAGE_VERSION), ("handoff", handoff.VERSION))
    config = {"domain": domain.to_record(), "producer_profile": asdict(producer_profile),
        "source_binding": asdict(source_binding), "source_profile": asdict(source_profile),
        "market_identity": market_source.source_identity, "database_identity": database_identity,
        "market_anchor": market_source.start_after_p1_rowid,
        "paths": tuple(str(Path(p).resolve()) for p in (operations_path, ledger_path,
            producer_path, source_path, market_source.db_path)),
        "dispatch": (batch_rows, page_rows, queued_roots),
        "producer_model": producer.MODEL_FINGERPRINT, "producer_storage": producer.STORAGE_FINGERPRINT}
    # Fixed consumed boundary only, not a dependency graph. Accepted interface
    # version labels can stay unchanged across concrete implementation changes.
    code_digest = content_fingerprint(tuple((module.__name__, hashlib.sha256(
        Path(module.__file__).read_bytes()).hexdigest()) for module in (cold, composition, ownership, signer, sender)))
    return StartupIdentity(cold.ColdRuntimeV01.__module__+"."+cold.ColdRuntimeV01.__qualname__,
        composition.VERSION, code_digest, CAPABILITY, restart_profile, contracts, content_fingerprint(config),
        producer_schema_digest, evidence_schema_digest)


@dataclass(frozen=True, slots=True)
class RetainedGrantFacts:
    issuance: authority.ArmingGrant | None
    consumed_by_command: str | None
    consumed_root: str | None
    revoked: bool
    currently_selected: bool
    grants_message_permission: bool


@dataclass(frozen=True, slots=True)
class StartupAuditFacts:
    identity: StartupIdentity
    owner_fence: ownership.OwnerFence
    operations_control: tuple[tuple[str, object], ...]
    reconstruction: cold.ReconstructionFacts
    authority: authority.AuthorityState
    relevant_grants: tuple[RetainedGrantFacts, ...]
    pending_attempt: StoredAttempt | None
    source_audit: str
    unresolved: tuple[str, ...]
    grants_permission: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class StartedRuntime:
    runtime: cold.ColdRuntimeV01
    audit: StartupAuditFacts

    def close(self):
        self.runtime._ownership.close()
        self.runtime.close()


def _source_preflight(expected, producer_path, source_path, binding, profile):
    # Read-only SQLite/schema/config audits only. Original factories own their
    # checkpoint/history/domain reconstruction. No migration/DDL or raw writes.
    for path, digest in ((producer_path, expected.producer_schema_digest),
                         (source_path, expected.evidence_schema_digest)):
        with closing(sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro", uri=True)) as conn:
            authority.require(conn.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
                and not conn.execute("PRAGMA foreign_key_check").fetchall(),
                "OPERATIONS_SOURCE_SQLITE_INTEGRITY_FAILURE")
            authority.require(_schema_digest(conn) == digest, "OPERATIONS_SOURCE_SCHEMA_CONFLICT")
    with closing(sqlite3.connect(Path(source_path).resolve().as_uri()+"?mode=ro", uri=True)) as conn:
        authority.require(conn.execute("SELECT binding_json,profile_json FROM source_domain").fetchall()
            == [(source.canonical_json(binding), source.canonical_json(profile))],
            "OPERATIONS_EVIDENCE_DOMAIN_PROFILE_CONFLICT")


def start_live(operations_path, ledger_path, domain, *, process_identity, now_us,
               expected_identity, producer_path, market_source, producer_profile,
               source_path, source_binding, source_profile, database_identity,
               replace_generation=None, batch_rows=32, page_rows=32, queued_roots=64, degradation_config=None):
    """Acquire -> audit -> original cold reconstruction -> historical typed facts.

    Failed startup consumes the acquired restart attempt; it never resets or
    releases the durable stop/budget. Reattempt/replacement is explicit A1 CAS.
    Source failure retains intact economic/protective Runtime with source held.
    Audit facts are a historical common cut, never current readiness/permission.
    """
    store = ownership.OperationsStore(operations_path, domain)
    owner = store.acquire(process_identity, now_us=now_us, replace_generation=replace_generation)
    runtime = None
    try:
        # Serializes the finite composition against stop/takeover. No Runtime
        # step/sign/send is called here (which would recursively take this lock).
        with owner.mutation_guard(domain):
            with closing(sqlite3.connect(store.path.as_uri()+"?mode=ro", uri=True)) as conn:
                authority.require(conn.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
                    and not conn.execute("PRAGMA foreign_key_check").fetchall(),
                    "OPERATIONS_CONTROL_SQLITE_INTEGRITY_FAILURE")
            authority.require(type(expected_identity) is StartupIdentity,
                "OPERATIONS_REVIEWED_STARTUP_IDENTITY_REQUIRED")
            control = store.snapshot()
            actual = configured_identity(domain, operations_path=operations_path, ledger_path=ledger_path,
                producer_path=producer_path, market_source=market_source, producer_profile=producer_profile,
                source_path=source_path, source_binding=source_binding, source_profile=source_profile,
                database_identity=database_identity,
                producer_schema_digest=expected_identity.producer_schema_digest,
                evidence_schema_digest=expected_identity.evidence_schema_digest,
                restart_profile=ownership.RestartProfile(*(control[key] for key in
                    ("max_attempts", "window_us", "backoff_us"))),
                batch_rows=batch_rows, page_rows=page_rows, queued_roots=queued_roots)
            authority.require(actual == expected_identity, "OPERATIONS_STARTUP_IDENTITY_CONFLICT")
            with closing(sqlite3.connect(Path(ledger_path).resolve().as_uri()+"?mode=ro", uri=True)) as conn:
                repository._schema_and_domain(conn, domain)
            source_audit = "SOURCE_PREFLIGHT_NOT_REACHED"

            def audit_sources():
                nonlocal source_audit
                source_audit = "SOURCE_PREFLIGHT_FAILED"
                _source_preflight(expected_identity, producer_path, source_path, source_binding, source_profile)
                source_audit = "SOURCE_PREFLIGHT_VERIFIED"

            runtime = cold.reopen_live(ledger_path, domain, producer_path=producer_path,
                market_source=market_source, producer_profile=producer_profile, source_path=source_path,
                source_binding=source_binding, source_profile=source_profile, database_identity=database_identity,
                batch_rows=batch_rows, page_rows=page_rows, queued_roots=queued_roots, source_preflight=audit_sources)
            authority.require(type(runtime) is cold.ColdRuntimeV01 and runtime.ledger.domain == domain,
                "OPERATIONS_ACTUAL_COLD_RUNTIME_REQUIRED")
            # C4 bypasses __init__: bind the actual root explicitly, including a
            # degraded source root. Never inherit its engineering None default.
            runtime._ownership = owner
            with runtime.ledger._trusted_read():
                facts = runtime.reconstruction_facts()
                state = runtime.ledger._authority  # Original immutable, verified Authority state.
                authority.require(state.economic_domain_id == domain.economic_domain_id,
                    "OPERATIONS_AUTHORITY_DOMAIN_CONFLICT")
                pending = None if facts.pending_attempt_id is None else runtime.ledger.attempt(facts.pending_attempt_id)
                grant_ids = set(() if state.grant is None else (state.grant.grant_id,))
                snapshot = runtime.ledger.consumer_snapshot(max_positions=1, max_reservations=1)
                for reservation in snapshot["reservations"]:
                    receipt = runtime.ledger.authority_acceptance(reservation.admission.action.root_id)
                    grant_ids.add(receipt.eligibility.decision.grant_id)
                grants = tuple(RetainedGrantFacts(**runtime.ledger.authority_grant_status(key))
                    for key in sorted(grant_ids))
                # Explicit original unresolved dispositions, no timeout/balance inference.
                unresolved = tuple(value for value in (
                    None if pending is None else "ATTEMPT:"+pending.current_disposition,
                    None if pending is None else "FINALITY:"+pending.chain_finality,
                    None if facts.protective_state is None else "PROTECTION:"+facts.protective_state,
                    "ADMITTED_ACTION" if facts.entry_action_id is not None else None,
                    "SOURCE_UNAVAILABLE" if facts.source_state != "SOURCE_RECONSTRUCTED" else None)
                    if value is not None)
                authority.require(runtime.ledger.consumer_snapshot()["consumer_cut"] == facts.consumer_cut
                    == snapshot["consumer_cut"], "OPERATIONS_STARTUP_COMMON_CUT_CHANGED")
                control = store.snapshot()
                authority.require(all(control[key] == value for key, value in asdict(owner.fence).items())
                    and not control["stopped"] and not control["exhausted"], "OPERATIONS_STARTUP_OWNER_CHANGED")
                audit = StartupAuditFacts(actual, owner.fence, tuple(sorted(control.items())), facts,
                    state, grants, pending, source_audit, unresolved)
        # Ownership may change after lock release; every eventual Runtime and
        # Execution action still checks A1. Historical audit never defeats that.
        started = StartedRuntime(runtime, audit)
        from .operations_degradation_monitor_v0_1 import OperationsMonitor
        runtime._operations_degradation = OperationsMonitor(started, degradation_config,
            source_binding=source_binding, source_profile=source_profile, producer_profile=producer_profile)
        return started
    except BaseException:
        owner.close()
        if runtime is not None:
            runtime.close()
        raise
