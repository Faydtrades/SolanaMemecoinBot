"""Current, one-use Authority stage facts in the existing economic journal.

No message construction, signer, sender, timer or retry controller lives here.
The only reusable receipts are historical. Execution must use the newly committed
delivery at its last durable claim/call boundary, under the same writer ownership.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from threading import Lock

from phase5.shadow_domain_v0_1 import content_fingerprint
from .authority_controls_v0_1 import (
    OperatorProvenance, EligibilityInput, SourceCheckpoint, clock_reasons, require,
)
from .authority_admission_v0_1 import root_encumbrances
from .authority_message_evidence_v0_1 import (
    MessageValidationProfile, MessageValidationInput, validate_message_evidence,
)
from .ledger_actions_v0_1 import public_reference, utc_microseconds
from .ledger_domain_v0_1 import digest_value
from .ledger_evidence_codec_v0_1 import strict_json_object
from .public_rpc_v0_1 import u64, primary_signature
from .source_health_v0_1 import source_consumer_evidence

VERSION = "live_authority_message_control_v0.1"
STAGES = ("SIGN", "SEND", "REBROADCAST")


@dataclass(frozen=True, slots=True)
class MessageProfileCommand:
    command_id: str
    operation: str
    profile: MessageValidationProfile
    approval: OperatorProvenance
    version: str = field(init=False, default=VERSION)

    def __post_init__(self):
        public_reference(self.command_id)
        require(self.operation in ("INSTALL_AND_SELECT", "SELECT_EXISTING")
            and type(self.profile) is MessageValidationProfile and type(self.approval) is OperatorProvenance,
            "AUTHORITY_MESSAGE_PROFILE_COMMAND_INVALID")
        if self.operation == "INSTALL_AND_SELECT":
            require(self.approval == self.profile.approval, "AUTHORITY_MESSAGE_PROFILE_APPROVAL_CONFLICT")

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class MessageProfileReceipt:
    sequence: int
    command: MessageProfileCommand
    previous_selection_digest: str | None
    previous_authority_digest: str
    resulting_authority_digest: str
    previous_common_digest: str
    version: str = field(init=False, default=VERSION)
    grants_message_permission: bool = field(init=False, default=False)

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


def select_profile(domain, state, command, policy, known_profile, previous_selection):
    profile = command.profile
    require(domain.mode == "LIVE" and profile.economic_domain_id == domain.economic_domain_id
        and policy is not None and policy.content_digest == profile.policy_digest,
        "AUTHORITY_MESSAGE_PROFILE_DOMAIN_POLICY_UNKNOWN")
    require(state.last_operator_at_utc is None or command.approval.recorded_at_utc >= state.last_operator_at_utc,
        "AUTHORITY_MESSAGE_PROFILE_OPERATOR_TIME_REGRESSION")
    require(command.approval.recorded_at_utc >= profile.approval.recorded_at_utc
        and command.approval.recorded_at_utc >= policy.approval.recorded_at_utc,
        "AUTHORITY_MESSAGE_PROFILE_ORIGINAL_APPROVAL_UNPROVEN")
    if command.operation == "INSTALL_AND_SELECT":
        require(known_profile is None, "AUTHORITY_MESSAGE_PROFILE_ID_ALREADY_INSTALLED")
    else:
        require(known_profile == profile, "AUTHORITY_MESSAGE_PROFILE_ID_CONTENT_CONFLICT")
    if previous_selection is not None:
        require(previous_selection.command.profile.policy_digest == profile.policy_digest,
            "AUTHORITY_MESSAGE_SELECTION_POLICY_CONFLICT")
    # The selection receipt itself is the epoch. Selecting an earlier value
    # again never revives the former selection's consumed stage identity.
    return replace(state, last_operator_at_utc=command.approval.recorded_at_utc)


@dataclass(frozen=True, slots=True)
class MessageStageRequest:
    command_id: str
    action_id: str
    attempt_id: str
    expected_attempt_revision: int
    stage: str
    rebroadcast_ordinal: int
    primary_signature: str | None
    version: str = field(init=False, default=VERSION)

    def __post_init__(self):
        public_reference(self.command_id)
        digest_value(self.action_id)
        digest_value(self.attempt_id)
        u64(self.expected_attempt_revision)
        u64(self.rebroadcast_ordinal)
        require(self.stage in STAGES and (self.rebroadcast_ordinal > 0) == (self.stage == "REBROADCAST"),
            "AUTHORITY_MESSAGE_STAGE_ORDINAL_INVALID")
        require((self.primary_signature is None) == (self.stage == "SIGN"), "AUTHORITY_MESSAGE_STAGE_SIGNATURE_REQUIRED")
        if self.primary_signature is not None:
            primary_signature(self.primary_signature)

    @property
    def consumption_key(self):
        return f"{self.attempt_id}:{self.stage}:{self.rebroadcast_ordinal}"


@dataclass(frozen=True, slots=True)
class MessageStageOriginal:
    request: MessageStageRequest
    validation: MessageValidationInput
    selection_digest: str
    source: EligibilityInput | None

    def __post_init__(self):
        require(type(self.request) is MessageStageRequest and type(self.validation) is MessageValidationInput
            and (self.source is None or type(self.source) is EligibilityInput), "AUTHORITY_MESSAGE_STAGE_ORIGINAL_INVALID")
        digest_value(self.selection_digest)

    @property
    def content_digest(self):
        from .authority_message_control_codec_v0_1 import original_to_json
        import hashlib
        return hashlib.sha256(original_to_json(self).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class CurrentMessageRisk:
    native_lamports: int | None
    observed_wsol_units: int
    observed_locked_lamports: int
    historical_network_paid_lamports: int
    other_root_encumbrances_lamports: int
    current_gross_upfront_lamports: int | None
    failure_allowance_lamports: int | None
    future_protection_lamports: int | None
    required_native_lamports: int | None
    remaining_position_units: int | None
    failed_attempt_count: int | None
    remaining_failed_attempt_count: int | None
    funds_adopted_from_snapshot: bool = field(init=False, default=False)
    releases_reservation: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class MessageStageDecision:
    disposition: str
    reasons: tuple[str, ...]
    clock_reasons: tuple[str, ...]
    validation_digest: str
    validation_disposition: str
    risk: CurrentMessageRisk
    prior_sign_digest: str | None
    prior_send_digest: str | None
    previous_rebroadcast_digest: str | None
    version: str = field(init=False, default=VERSION)
    grants_message_permission: bool = field(init=False, default=False)
    may_sign: bool = field(init=False, default=False)
    may_send: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class MessageStageReceipt:
    sequence: int
    writer_generation: int
    original: MessageStageOriginal
    decision: MessageStageDecision
    comparison_digest: str
    previous_authority_digest: str
    resulting_authority_digest: str
    previous_custody_digest: str
    resulting_custody_digest: str
    previous_common_digest: str
    version: str = field(init=False, default=VERSION)
    historical_only: bool = field(init=False, default=True)
    grants_message_permission: bool = field(init=False, default=False)
    may_sign: bool = field(init=False, default=False)
    may_send: bool = field(init=False, default=False)

    @property
    def content_digest(self):
        from .authority_message_control_codec_v0_1 import receipt_to_json
        import hashlib
        return hashlib.sha256(receipt_to_json(self).encode()).hexdigest()

    @property
    def consumed(self):
        return self.decision.disposition == "CONSUMED_AT_ORIGINAL_CURRENT_CUT"


_FRESH_DELIVERY = object()


class FreshStageConsumption:
    """Ephemeral return from a NEW committed consume, never a persisted bearer.

    There is deliberately no codec or lookup that constructs this type. A lost
    acknowledgement spends the slot and yields only historical receipts later.
    Execution still owns the final claim/call and intact continuation checks,
    including current source and clock. A Ledger fence cannot freeze the
    separate source store or turn this original clock sample into a clock API.
    """
    __slots__ = ("_receipt", "_generation", "_common_digest", "_execution_spent", "_execution_lock", "_source_store")

    def __init__(self, receipt, generation, common_digest, *, _token, _source_store=None):
        require(_token is _FRESH_DELIVERY and type(receipt) is MessageStageReceipt and receipt.consumed,
            "AUTHORITY_FRESH_COMMIT_DELIVERY_REQUIRED")
        object.__setattr__(self, "_receipt", receipt)
        object.__setattr__(self, "_generation", generation)
        object.__setattr__(self, "_common_digest", common_digest)
        object.__setattr__(self, "_execution_spent", False)
        object.__setattr__(self, "_execution_lock", Lock())
        object.__setattr__(self, "_source_store", _source_store)

    def __setattr__(self, name, value):
        raise AttributeError("immutable Authority call delivery")

    def _claim_execution(self):
        """Spend before a consumer callback/key call, including failed calls.

        This changes no Authority decision or durable fact. The original SIGN
        slot is already consumed; Execution cannot retry a possibly used key.
        """
        with self._execution_lock:
            require(not self._execution_spent, "AUTHORITY_FRESH_DELIVERY_ALREADY_SPENT")
            object.__setattr__(self, "_execution_spent", True)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    def __reduce_ex__(self, protocol):
        raise TypeError("ephemeral Authority delivery cannot be serialized")

    def _uses_source_store(self, source_store):
        # A copied journal with the same historical row cannot substitute for
        # the actual source store sampled by the original current consumption.
        return source_store is self._source_store

    @property
    def receipt(self):
        return self._receipt

    @property
    def writer_generation(self):
        return self._generation

    @property
    def consumed_common_digest(self):
        return self._common_digest

    @property
    def stage(self):
        return self._receipt.original.request.stage


def _current_source(state, candidate, policy, supplied):
    reasons, checkpoints = [], state.source_checkpoints
    if supplied is None or supplied.source is None:
        return ("CURRENT_SOURCE_MISSING",), checkpoints
    source, sample = supplied.source, supplied.clock
    require((supplied.candidate_digest, supplied.track) == (candidate.content_digest, policy.selected_track),
        "AUTHORITY_MESSAGE_SOURCE_ORIGINAL_CONFLICT")
    old = next((item for item in checkpoints if (item.source_identity, item.profile_fingerprint) ==
        (source.binding.source_identity, source.profile.fingerprint)), None)
    continuous = (old is None and supplied.previous_source_record_digest is None or old is not None
        and supplied.previous_source_record_digest == old.record_digest and supplied.source_sequence >= old.sequence
        and (supplied.source_sequence != old.sequence or source.content_digest == old.record_digest))
    if not continuous:
        reasons.append("SOURCE_SEQUENCE_OR_RETAINED_RECORD_CONFLICT")
    elif source.binding.source_identity == candidate.source_binding.source_identity:
        checkpoints = tuple(item for item in checkpoints if item != old) + (SourceCheckpoint(
            source.binding.source_identity, source.profile.fingerprint, supplied.source_sequence, source.content_digest),)
    port = source_consumer_evidence(source, expected_source_identity=candidate.source_binding.source_identity,
        required_cut_utc=source.snapshot.requested_cut_utc, now_utc=sample.utc_upper_utc)
    reasons.extend(port.reasons)
    if not port.observed_prefix_supported:
        reasons.append("CURRENT_SOURCE_NOT_USABLE")
    if source.profile.fingerprint != policy.source_profile_fingerprint:
        reasons.append("SOURCE_POLICY_PROFILE_CONFLICT")
    if (utc_microseconds(sample.utc_lower_utc) < utc_microseconds(source.snapshot.observed_at_utc)
            or utc_microseconds(source.snapshot.requested_cut_utc) < candidate.generated_at_us):
        reasons.append("SOURCE_KNOWLEDGE_CUT_UNPROVEN")
    if (not source.covered_from_utc or not source.covered_through_utc
            or not utc_microseconds(source.covered_from_utc) <= candidate.generated_at_us <= utc_microseconds(source.covered_through_utc)
            or candidate.generated_at_us < utc_microseconds(source.binding.coverage_start_utc)):
        reasons.append("ORIGINAL_SIGNAL_SOURCE_COVERAGE_UNPROVEN")
    return tuple(reasons), checkpoints


def assess_message_stage(original, state, custody, attempt, selection, comparison, *, generation, lane,
                         grant, grant_revoked, consumed_grant_root, accepted, policies, applications, actions,
                         stage_history, inbox_disposition):
    """Only original inputs + chronological Ledger facts; no current DB reads."""
    request, validation = original.request, original.validation
    context, clock = validation.context, validation.clock
    policy, action = context.original_policy, context.action
    require(context.domain.mode == "LIVE" and request.action_id == action.action_id
        and attempt is not None and request.attempt_id == attempt.preparation.attempt_id,
        "AUTHORITY_MESSAGE_ACTUAL_ATTEMPT_REQUIRED")
    require(original.selection_digest == selection.content_digest and validation.profile == selection.command.profile,
        "AUTHORITY_MESSAGE_SELECTED_PROFILE_CONFLICT")
    checked = validate_message_evidence(validation)
    reasons = list(checked.reasons)
    if checked.disposition != "SUPPORTED_CONTEXT_ONLY":
        reasons.append("EXACT_MESSAGE_CONTEXT_UNPROVEN")
    clock_denials = clock_reasons(replace(state, policy=policy), clock)
    reasons.extend(clock_denials)
    if state.last_operator_at_utc is not None and utc_microseconds(clock.utc_lower_utc) < utc_microseconds(state.last_operator_at_utc):
        reasons.append("CONTROL_KNOWLEDGE_CLOCK_UNPROVEN")
    if state.hard_stop_command is not None:
        reasons.append("GLOBAL_HARD_STOP_LATCHED")
    original_grant = context.acceptance.eligibility.decision.grant_id
    if grant is None or grant.grant_id != original_grant or grant.policy_digest != policy.content_digest or grant_revoked:
        reasons.append("ORIGINAL_SCOPE_GRANT_MISSING_OR_HARD_REVOKED")
    if grant is not None and grant.scope == "ENTRY_ONCE" and consumed_grant_root != action.root_id:
        reasons.append("ORIGINAL_ONE_TIME_ROOT_CONSUMPTION_REQUIRED")
    checkpoints = state.source_checkpoints
    if action.side == "BUY":
        if state.policy is None or state.policy.content_digest != policy.content_digest:
            reasons.append("CURRENT_ENTRY_POLICY_REPLACED")
        if state.entry_stop_command is not None:
            reasons.append("ENTRY_STOP_LATCHED")
        if state.grant is None or state.grant.grant_id != original_grant:
            reasons.append("ORIGINAL_ENTRY_GRANT_NOT_CURRENTLY_SELECTED")
        if (clock.utc_lower_utc < policy.entry_valid_from_utc or clock.utc_upper_utc > policy.entry_valid_through_utc
                or grant is not None and (clock.utc_lower_utc < grant.entry_valid_from_utc or clock.utc_upper_utc > grant.entry_valid_through_utc)):
            reasons.append("CURRENT_ENTRY_POLICY_OR_GRANT_INTERVAL_UNPROVEN")
        deadline = context.acceptance.eligibility.decision.binding.deadline_us
        if not clock_denials:
            if utc_microseconds(clock.utc_lower_utc) > deadline:
                reasons.append("ORIGINAL_CANDIDATE_EXPIRED")
            elif utc_microseconds(clock.utc_upper_utc) > deadline:
                reasons.append("ORIGINAL_DEADLINE_AMBIGUOUS")
        require(original.source is not None and original.source.root_id == action.root_id and original.source.clock == clock,
            "AUTHORITY_CURRENT_ENTRY_SOURCE_ORIGINAL_REQUIRED")
        source_reasons, checkpoints = _current_source(state, context.candidate, policy, original.source)
        reasons.extend(source_reasons)
    else:
        require(original.source is None, "AUTHORITY_REDUCTION_CANNOT_REUSE_ENTRY_SOURCE_DECISION")
    if custody.quarantined or inbox_disposition != "ACCEPTED":
        reasons.append("CURRENT_CUSTODY_OR_INBOX_NOT_MUTABLE")
    reservation = next((item for item in custody.reservations if item.root_id == action.root_id), None)
    if reservation is None or reservation.retired_sequence is not None:
        reasons.append("ORIGINAL_HELD_RESERVATION_REQUIRED")
    if (comparison.disposition not in ("MATCHED_AT_ORIGINAL_CUT", "PENDING_EFFECTS_UNKNOWN")
            or comparison.differences or comparison.reasons
            or comparison.target_custody_digest != custody.content_digest
            or comparison.pending_attempts != (request.attempt_id,)):
        reasons.append("CURRENT_COMPLETE_UNCHANGED_CUSTODY_COMPARISON_REQUIRED")
    if lane != request.attempt_id or not attempt.lane_held:
        reasons.append("EXACT_OWN_MUTATION_LANE_REQUIRED")
    if request.expected_attempt_revision != attempt.revision:
        reasons.append("CURRENT_ATTEMPT_REVISION_CONFLICT")
    prep, simulation = attempt.preparation, validation.evidence.simulation
    if utc_microseconds(clock.utc_lower_utc) < utc_microseconds(attempt.last_recorded_at_utc):
        reasons.append("CURRENT_ATTEMPT_KNOWLEDGE_CLOCK_UNPROVEN")
    plan_record = strict_json_object(validation.evidence.supplied_plan_json)
    # The plan payload contains its content fingerprint separately from plan_id.
    if (prep.action_id != action.action_id or prep.action_content_digest != action.content_digest
            or prep.message_sha256 != checked.message_sha256 or prep.message_hex != simulation.envelopes[-1].message_hex
            or prep.lease != simulation.leases[-1] or prep.plan_digest != content_fingerprint(plan_record)
            or prep.message_policy_digest != validation.profile.content_digest or prep.validity_profile != "RECENT_BLOCKHASH"):
        reasons.append("STORED_PREPARATION_EXACT_MESSAGE_PLAN_PROFILE_LEASE_CONFLICT")
    if simulation.validity[-1].block_height < prep.finalized_lower_anchor.block_height:
        reasons.append("CURRENT_VALIDITY_BEHIND_ORIGINAL_FINALIZED_ANCHOR")
    if attempt.economically_applied or attempt.chain_quarantined or attempt.chain_finality in ("FINALIZED_SUCCESS", "FINALIZED_FAILURE", "PROVEN_NON_LANDED"):
        reasons.append("ATTEMPT_ALREADY_RESOLVED_OR_QUARANTINED")
    history = tuple(item for item in stage_history if item.consumed and item.original.request.attempt_id == request.attempt_id)
    sign = next((item for item in history if item.original.request.stage == "SIGN"), None)
    send = next((item for item in history if item.original.request.stage == "SEND"), None)
    broadcasts = tuple(item for item in history if item.original.request.stage == "REBROADCAST")
    previous_broadcast = max(broadcasts, key=lambda item: item.original.request.rebroadcast_ordinal, default=None)
    if any(item.original.request.consumption_key == request.consumption_key for item in history):
        reasons.append("AUTHORITY_STAGE_ALREADY_CONSUMED")
    if request.stage == "SIGN":
        if (attempt.recorded_stage not in ("PREPARED", "EXACT_SIMULATED", "AUTHORIZED")
                or attempt.prepared_generation != generation or attempt.primary_signature is not None):
            reasons.append("INTACT_UNSIGNED_ORIGINAL_GENERATION_REQUIRED")
    else:
        if sign is None or sign.original.selection_digest != original.selection_digest:
            reasons.append("PRIOR_REAL_SIGN_CURRENT_SELECTION_REQUIRED")
        if (attempt.primary_signature != request.primary_signature or attempt.signed_wire_digest is None):
            reasons.append("EXACT_DURABLE_PUBLIC_SIGNATURE_REQUIRED")
        if request.stage == "SEND":
            if attempt.recorded_stage != "SIGNED_DURABLE":
                reasons.append("CURRENT_SIGNED_DURABLE_STAGE_REQUIRED")
        else:
            if (send is None or send.original.selection_digest != original.selection_digest
                    or send.original.request.primary_signature != request.primary_signature):
                reasons.append("PRIOR_REAL_SEND_SAME_SIGNATURE_CURRENT_SELECTION_REQUIRED")
            if attempt.recorded_stage not in ("SEND_CLAIMED", "OBSERVING", "UNKNOWN"):
                reasons.append("SAME_MESSAGE_REBROADCAST_STAGE_UNPROVEN")
            if request.rebroadcast_ordinal != (1 if previous_broadcast is None else previous_broadcast.original.request.rebroadcast_ordinal+1):
                reasons.append("REBROADCAST_ORDINAL_NOT_NEXT")
    remaining = None if context.position is None else context.position.remaining_units
    if action.side == "BUY":
        if context.position is not None and context.position.acquired_units:
            reasons.append("SUCCESSFUL_ROOT_CANNOT_REACQUIRE")
    elif (context.position is None or context.protection is None or not context.position.usable
            or not 0 < action.input_units <= context.position.remaining_units
            or context.protection.obligation_state not in ("DUE", "MONITORING")):
        reasons.append("ACTUAL_PROTECTED_REMAINING_REDUCTION_SCOPE_REQUIRED")
    rows = root_encumbrances(custody, accepted, policies, applications, actions)
    if action.side == "BUY":
        if any(row.root_id != action.root_id for row in rows) or any(position.root_id != action.root_id and position.status != "RETIRED" for position in custody.positions):
            reasons.append("V1_OTHER_ECONOMIC_ROOT_OCCUPIED")
        if sum(row.exposure_quote_cap_lamports for row in rows) > policy.size.max_global_exposure_lamports:
            reasons.append("CURRENT_GLOBAL_ORIGINAL_CAP_EXPOSURE_EXCEEDED")
    own = next((item for item in rows if item.root_id == action.root_id), None)
    other = sum(item.outstanding_lamports for item in rows if item.root_id != action.root_id)
    future = failure = required = None
    if own is None or own.failure_budget_remaining_lamports is None or own.remaining_failed_attempt_count is None:
        reasons.append("ORIGINAL_ROOT_COST_HISTORY_UNPROVEN")
    else:
        failure = own.failure_budget_remaining_lamports
        if own.remaining_failed_attempt_count == 0:
            reasons.append("ACTUAL_FAILURE_ATTEMPT_BUDGET_EXHAUSTED")
        costs = policy.costs
        future = (costs.protective_network_fee_lamports+costs.protective_setup_lamports+costs.protective_refundable_lock_lamports
            if action.side == "BUY" or remaining is not None and action.input_units < remaining else 0)
        if checked.costs is not None:
            if checked.costs.network_total_fee_lamports > failure:
                reasons.append("CURRENT_EXACT_FEE_EXCEEDS_REMAINING_FAILURE_ALLOWANCE")
            required = other+checked.costs.gross_upfront_native_lamports+failure+future
            if required > (1<<64)-1 or other > (1<<64)-1 or future > (1<<64)-1:
                reasons.append("CURRENT_RESOURCE_AGGREGATE_U64_OVERFLOW")
            elif custody.native_lamports is None or custody.native_lamports < required:
                reasons.append("CURRENT_GROSS_COST_FAILURE_AND_RESIDUAL_EXIT_HEADROOM_INSUFFICIENT")
    risk = CurrentMessageRisk(custody.native_lamports, sum(item.units for item in custody.accounts if item.mint == "So11111111111111111111111111111111111111112"),
        sum(item.observed_locked_lamports for item in custody.accounts), custody.network_fees_paid_lamports, other,
        None if checked.costs is None else checked.costs.gross_upfront_native_lamports, failure, future, required, remaining,
        None if own is None else own.failed_attempt_count, None if own is None else own.remaining_failed_attempt_count)
    reasons = tuple(sorted(set(reasons)))
    decision = MessageStageDecision("CURRENT_STAGE_DENIED_OR_UNRESOLVED" if reasons else "CONSUMED_AT_ORIGINAL_CURRENT_CUT",
        reasons, clock_denials, checked.content_digest, checked.disposition, risk,
        None if sign is None else sign.content_digest, None if send is None else send.content_digest,
        None if previous_broadcast is None else previous_broadcast.content_digest)
    updated = replace(state, last_qualified_clock=state.last_qualified_clock if clock_denials else clock,
        clock_requires_reconciliation=bool(clock_denials), source_checkpoints=checkpoints)
    return decision, updated
