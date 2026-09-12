"""A3 current Operations classifications over the actual A2-owned Runtime.

Results describe one read cut, never a durable readiness latch or SIGN/SEND
capability. Reevaluate with a fresh trusted clock and original external evidence;
Runtime, Authority and Execution still independently select/approve every action.
"""
from __future__ import annotations

import sqlite3
import copy
from dataclasses import dataclass, field, replace

from . import authority_controls_v0_1 as authority
from .authority_admission_v0_1 import EntryRequest, assess_entry_risk
from .ledger_custody_v0_1 import compare_wallet
from .operations_startup_v0_1 import StartedRuntime
from .operations_ownership_v0_1 import OperationsOwnership, OwnerFence
from .runtime_composition_v0_1 import EntryFacts
from .runtime_reconstruction_v0_1 import ColdRuntimeV01, ReconstructionFacts
from .protective_outcome_v0_1 import protective_outcome
from .position_controller_v0_1 import bind_actual_position
from .authority_message_evidence_v0_1 import (
    ExternalMessageEvidence, MessageValidationInput, capture_message_context,
)
from .authority_message_control_v0_1 import assess_current_action
from .ledger_actions_v0_1 import utc_microseconds

VERSION = "live_operations_readiness_v0.1"


@dataclass(frozen=True, slots=True)
class ReadinessBarrier:
    name: str
    state: str
    ready: bool
    reasons: tuple[str, ...]
    root_id: str | None = None
    candidate_digest: str | None = None
    deadline_us: int | None = None
    action_id: str | None = None
    due: bool | None = None
    grants_permission: bool = field(init=False, default=False)
    may_sign: bool = field(init=False, default=False)
    may_send: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class ReadinessFacts:
    clock: authority.TrustedClockSample
    owner_fence: OwnerFence | None
    reconstruction: ReconstructionFacts | None
    protective: ReadinessBarrier
    entry: ReadinessBarrier
    safe_work: tuple[str, ...]
    authority_digest: str | None
    source_digest: str | None
    historical_only: bool = field(init=False, default=True)
    grants_permission: bool = field(init=False, default=False)


def _barrier(name, state, reasons=(), **bindings):
    reasons = tuple(sorted(set(reasons)))
    return ReadinessBarrier(name, state, state == "READY" and not reasons, reasons, **bindings)


def _comparison(repo, support):
    fence = repo.write_fence()
    return compare_wallet(repo.domain, repo._custody, support, common_revision=fence.revision,
        common_digest=fence.last_receipt_digest, pending_attempts=repo._pending_attempts(),
        chain_anchors=(anchor for facts in repo._chain_facts.values() for anchor in facts.anchors))[0]


def _entry(runtime, sample, entry, blockers):
    repo, source = runtime.ledger, runtime.source
    roots = runtime.queued_roots
    if not roots:
        return _barrier("ENTRY_READY", "HELD", (*blockers, "CURRENT_CANONICAL_CANDIDATE_REQUIRED")), None
    root = roots[0]
    candidate = repo.candidate(root)
    authority.require(candidate is not None and candidate.trade_root(repo.domain) == root,
        "OPERATIONS_ORIGINAL_QUEUED_CANDIDATE_REQUIRED")
    if entry is None or source is None or repo._authority.policy is None:
        missing = tuple(reason for absent, reason in (
            (entry is None, "CURRENT_ENTRY_WALLET_ROUTE_FACTS_REQUIRED"),
            (source is None, "CURRENT_RECONSTRUCTED_SOURCE_REQUIRED"),
            (repo._authority.policy is None, "CURRENT_AUTHORITY_POLICY_REQUIRED")) if absent)
        return _barrier("ENTRY_READY", "HELD", (*blockers, *missing), root_id=root,
            candidate_digest=candidate.content_digest), None
    authority.require(type(entry) is EntryFacts, "OPERATIONS_ORIGINAL_ENTRY_FACTS_REQUIRED")
    request = EntryRequest(root, entry.venue, entry.token_program, repo._authority.policy.selected_track)
    selected = source.latest_record()
    old = next((item for item in repo._authority.source_checkpoints if
        (item.source_identity, item.profile_fingerprint) == (source.binding.source_identity, source.profile.fingerprint)), None)
    previous = None if old is None else source.read_record(old.sequence)
    original = authority.EligibilityInput(root, candidate.content_digest, request.track, sample,
        None if selected is None else selected[0], None if selected is None else selected[1],
        None if previous is None else previous[1].content_digest)
    decision, updated = authority.evaluate_entry(repo.domain, repo._authority, candidate,
        repo.inbox_disposition(root), original, repo.authority_entry_binding(root))
    fence = repo.write_fence()
    # Pure Authority adapter only: no receipt is persisted, no acceptance is
    # fabricated and no arming grant is issued/consumed by this classification.
    eligibility = authority.AuthorityReceipt(fence.revision+1, "operations-readiness-preview",
        "AUTHORITY_ELIGIBILITY", original, decision, repo._authority.content_digest,
        updated.content_digest, fence.last_receipt_digest)
    accepted, policies, actions, spent = repo._authority_risk_history(request, candidate)
    risk = assess_entry_risk(repo.domain, repo._custody, candidate, request, eligibility,
        repo._authority.policy, entry.wallet, _comparison(repo, entry.wallet), accepted=accepted,
        policies=policies, applications=repo._applications, pending_attempts=repo._pending_attempts(),
        staged_action=repo.authority_candidate_economics(root)["staged_action"], spent_grants=spent, actions=actions)
    reasons = (*blockers, *risk.reasons)
    if source.latest_record() != selected:
        reasons += ("CURRENT_SOURCE_CHANGED_DURING_READINESS",)
    return _barrier("ENTRY_READY", "HELD" if reasons else "READY", reasons, root_id=root,
        candidate_digest=candidate.content_digest, deadline_us=decision.binding.deadline_us), \
        None if selected is None else selected[1].content_digest


def evaluate(started, *, clock, entry=None, reduction=None, operations_resources=None):
    """Sample original current owners/evidence, without any economic mutation.

    A2 audit proves the startup path only. Its historical controls, funding,
    source/Authority receipts and original reconstruction are never used as
    current permission. Safe audit/reconciliation work is separate from readiness.
    """
    authority.require(type(started) is StartedRuntime and type(started.runtime) is ColdRuntimeV01
        and callable(clock), "OPERATIONS_ACTUAL_A2_START_AND_CLOCK_REQUIRED")
    runtime, sample = started.runtime, clock()
    authority.require(type(sample) is authority.TrustedClockSample, "OPERATIONS_ORIGINAL_CLOCK_REQUIRED")
    owner, repo = runtime._ownership, runtime.ledger
    try:
        authority.require(type(owner) is OperationsOwnership and owner.fence == started.audit.owner_fence,
            "OPERATIONS_ACTUAL_STARTUP_OWNER_REQUIRED")
        with owner.mutation_guard(repo.domain), repo._trusted_read():
            result = _current(runtime, sample, entry, reduction)
        monitor = runtime._operations_degradation
        if monitor is not None:
            alerts = monitor.observe(sample, entry=entry, reduction=reduction, resources=operations_resources,
                readiness=result)
            if alerts.entry_held:
                result = replace(result, entry=replace(result.entry, state="HELD", ready=False,
                    reasons=tuple(sorted(set((*result.entry.reasons, "CURRENT_DEGRADATION_ENTRY_HOLD"))))))
        return result
    except (ValueError, RuntimeError, sqlite3.DatabaseError, OSError, TypeError):
        # Neither a lost owner nor a failed current read can resurrect A2 facts.
        return ReadinessFacts(sample, None, None,
            _barrier("PROTECTIVE_READY", "HELD", ("CURRENT_OWNER_OR_EVIDENCE_UNAVAILABLE",)),
            _barrier("ENTRY_READY", "HELD", ("CURRENT_OWNER_OR_EVIDENCE_UNAVAILABLE",)),
            ("SAFE_READ_AUDIT_ONLY",), None, None)


def _current(runtime, sample, entry, reduction):
    repo = runtime.ledger
    facts = runtime.reconstruction_facts()
    snapshot = repo.consumer_snapshot(max_positions=1, max_reservations=1)
    state = repo._authority
    blockers = []
    if state.hard_stop_command is not None:
        blockers.append("GLOBAL_HARD_STOP_LATCHED")
    if facts.position_id is not None:
        blockers.append("EXISTING_EXPOSURE_REQUIRES_PROTECTION_AND_RETIREMENT_FIRST")
    if snapshot["reservations"] or snapshot["pending_attempts"] or snapshot["funding"].quarantine_reasons:
        blockers.append("CURRENT_CAPACITY_OR_MUTATION_LANE_HELD")
    if runtime.producer is None or runtime.source is None or facts.source_state != "SOURCE_RECONSTRUCTED":
        blockers.append("CURRENT_RECONSTRUCTED_SOURCE_REQUIRED")
    else:
        # ReconstructionFacts retains the startup producer checkpoint. Validate
        # the actual producer now rather than trusting that historical pointer.
        try:
            producer = runtime.producer
            authority.require(not producer._poisoned and not producer.conn.in_transaction,
                "OPERATIONS_CURRENT_PRODUCER_READ_BOUNDARY_REQUIRED")
            producer.conn.execute("BEGIN")
            try:
                observer = copy.copy(producer)
                observer.metrics = dict(producer.metrics)
                manifest = observer._validate_checkpoint()
                authority.require(manifest["generation"] == producer._generation,
                    "OPERATIONS_CURRENT_PRODUCER_GENERATION_REQUIRED")
            finally:
                producer.conn.execute("ROLLBACK")
        except (ValueError, RuntimeError, sqlite3.DatabaseError, OSError):
            blockers.append("CURRENT_PRODUCER_CHECKPOINT_UNAVAILABLE")
    protective, safe = _protection(runtime, facts, sample, reduction)
    try:
        entry_result, source_digest = _entry(runtime, sample, entry, blockers)
    except (ValueError, RuntimeError, sqlite3.DatabaseError, OSError, TypeError):
        entry_result, source_digest = _barrier("ENTRY_READY", "HELD",
            (*blockers, "CURRENT_ENTRY_EVIDENCE_UNAVAILABLE")), None
    authority.require(repo.consumer_snapshot()["consumer_cut"] == facts.consumer_cut == snapshot["consumer_cut"],
        "OPERATIONS_CURRENT_COMMON_CUT_CHANGED")
    return ReadinessFacts(sample, runtime._ownership.fence, facts, protective, entry_result,
        safe, state.content_digest, source_digest)


def _protection(runtime, facts, sample, reduction):
    repo, binding = runtime.ledger, runtime.position_binding
    safe = ("SAFE_READ_AUDIT_ONLY",)
    if runtime.capability == "NO_BROADCAST":
        authority.require(facts.position_id is None and facts.binding_id is None
            and facts.chain_receipt_key is None, "OPERATIONS_DRY_NO_LIVE_ECONOMICS_REQUIRED")
        return _barrier("PROTECTIVE_READY", "NOT_REQUIRED"), safe + (("ORIGINAL_DRY_NON_SUBMITTED_RECOVERY",)
            if facts.entry_action_id is not None else ())
    if facts.pending_attempt_id is not None:
        safe += ("ORIGINAL_RECONCILIATION_OR_APPLICATION_REQUIRED",)
    if facts.position_id is None:
        return _barrier("PROTECTIVE_READY", "NOT_REQUIRED"), safe
    authority.require(binding is not None, "OPERATIONS_ORIGINAL_POSITION_CONTROLLER_REQUIRED")
    controller = bind_actual_position(repo, position_id=binding.position_id, root_id=binding.root_id,
        mint=binding.mint, selected_track=binding.spec.track_id, policy_digest=binding.policy_digest,
        acquisition_application_key=binding.acquisition_application_key, previous=binding)
    safe += ("ORIGINAL_PROTECTION_RETAINED",)
    outcome = None if facts.protective_state is None else protective_outcome(repo, binding)
    due = (outcome is not None and outcome.obligation.state == "DUE"
        or utc_microseconds(sample.utc_upper_utc) >= binding.fallback_fire_boundary_us)
    original_policy = repo.authority_policy(binding.policy_ref)
    reasons = list(authority.clock_reasons(replace(repo._authority, policy=original_policy), sample))
    if repo._authority.hard_stop_command is not None:
        reasons.append("GLOBAL_HARD_STOP_LATCHED")
    if controller.remaining_units == 0:
        return _barrier("PROTECTIVE_READY", "SATISFIED_AWAITING_RETIREMENT",
            (*reasons, "ORIGINAL_RETIREMENT_REQUIRES_CURRENT_TRUTH"), root_id=binding.root_id, due=due), \
            safe+("ORIGINAL_RETIREMENT_REQUIRED",)
    if facts.pending_attempt_id is not None:
        return _barrier("PROTECTIVE_READY", "TRUTH_REQUIRED",
            (*reasons, "ORIGINAL_PENDING_ATTEMPT_REQUIRES_TRUTH"), root_id=binding.root_id, due=due), safe
    if outcome is None or outcome.state in ("MONITORING", "STAGE_ACTION"):
        # This barrier does not gate safe original monitoring or action staging.
        # No synthetic action/attempt can be created to manufacture readiness.
        return _barrier("PROTECTIVE_READY", "DUE_STAGING_REQUIRED" if due else "MONITORING",
            (*reasons, "ACTUAL_STAGED_REDUCTION_AND_CURRENT_RESOURCES_REQUIRED"),
            root_id=binding.root_id, due=due), safe+("ORIGINAL_RUNTIME_PROTECTIVE_STEP_REQUIRED",)
    if outcome.state not in ("PREPARE_ATTEMPT", "REPLACE_ATTEMPT") or not outcome.mutation_eligible:
        reasons.append("ORIGINAL_PROTECTIVE_DISPOSITION_NOT_MUTATION_ELIGIBLE")
    if reduction is None:
        reasons.append("CURRENT_ORIGINAL_REDUCTION_EVIDENCE_REQUIRED")
    elif outcome.action is not None:
        try:
            authority.require(type(reduction) is ExternalMessageEvidence,
                "OPERATIONS_ORIGINAL_EXTERNAL_REDUCTION_EVIDENCE_REQUIRED")
            context = capture_message_context(repo, outcome.action.action_id)
            authority.require(context.action == outcome.action and context.action.side == "SELL",
                "OPERATIONS_ACTUAL_STORED_REDUCTION_REQUIRED")
            selection = repo.authority_message_profile(context.original_policy.content_digest)
            authority.require(selection is not None, "OPERATIONS_CURRENT_MESSAGE_PROFILE_REQUIRED")
            validation = MessageValidationInput(context, selection.command.profile, sample, reduction)
            accepted, policies, actions, _ = repo._authority_risk_history(context.acceptance.request, context.candidate)
            grant = repo.authority_grant_status(context.acceptance.eligibility.decision.grant_id)
            _, current_reasons, _, _ = assess_current_action(validation, repo._authority, repo._custody,
                grant=grant["issuance"], grant_revoked=grant["revoked"], consumed_grant_root=grant["consumed_root"],
                accepted=accepted, policies=policies, applications=repo._applications, actions=actions,
                inbox_disposition=repo.inbox_disposition(context.action.root_id))
            reasons.extend(current_reasons)
            comparison = _comparison(repo, reduction.wallet)
            if (comparison.disposition != "MATCHED_AT_ORIGINAL_CUT" or comparison.reasons
                    or comparison.differences or comparison.pending_attempts
                    or comparison.target_custody_digest != repo._custody.content_digest):
                reasons.append("CURRENT_COMPLETE_PROTECTIVE_WALLET_COMPARISON_REQUIRED")
        except (ValueError, RuntimeError, sqlite3.DatabaseError, OSError, TypeError):
            reasons.append("CURRENT_REDUCTION_EVIDENCE_UNAVAILABLE")
    else:
        reasons.append("ACTUAL_STORED_REDUCTION_REQUIRED")
    return _barrier("PROTECTIVE_READY", "HELD" if reasons else "READY", reasons,
        root_id=binding.root_id, action_id=None if outcome.action is None else outcome.action.action_id,
        due=due), safe
