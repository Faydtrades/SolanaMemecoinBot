"""Finite public Authority controls and original-age eligibility; no sizing/grant consumption.

Inputs name HUMAN_EXTERNAL approvals and trusted clock provenance. This module
neither authenticates operators nor produces an Execution message permission.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone

from phase4.paper_entry_execution_deadline_v0_1 import deadline_for
from phase4.paper_continuous_firstpullback_binding_v0_1 import LOCKED_SELECTION_SHA256, LOCKED_ROLE_ORDER, LOCKED_PARAMETER_SET_BY_ROLE
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from .ledger_actions_v0_1 import CandidateInboxInput, public_reference, utc_microseconds
from .ledger_domain_v0_1 import LedgerContractError, digest_value, ledger_utc, ZERO_DIGEST
from .source_health_v0_1 import SourceVerdict, source_consumer_evidence, verdict_from_json
from .public_rpc_v0_1 import u64

VERSION = "live_authority_controls_v0.1"
SCOPES = ("ENTRY_NORMAL", "ENTRY_ONCE", "DRY")
TRACKS = ("FINAL-A", "FINAL-B", "SENS-C")


def require(condition, reason):
    if not condition:
        raise LedgerContractError(reason)


def utc_from_us(value):
    u64(value)
    return (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=value)).isoformat(timespec="microseconds")


@dataclass(frozen=True, slots=True)
class OperatorProvenance:
    operator_id: str
    approval_reference: str
    approval_record_digest: str
    recorded_at_utc: str
    origin: str

    def __post_init__(self):
        public_reference(self.operator_id)
        public_reference(self.approval_reference)
        digest_value(self.approval_record_digest)
        object.__setattr__(self, "recorded_at_utc", ledger_utc(self.recorded_at_utc))
        require(self.origin == "HUMAN_EXTERNAL", "AUTHORITY_HUMAN_EXTERNAL_APPROVAL_REQUIRED")


@dataclass(frozen=True, slots=True)
class EntrySizeLimits:
    fixed_quote_lamports: int
    minimum_quote_lamports: int
    ceiling_quote_lamports: int
    max_trade_notional_lamports: int
    max_global_exposure_lamports: int
    max_mint_exposure_lamports: int
    max_open_positions: int

    def __post_init__(self):
        for value in asdict(self).values():
            u64(value)
        # Zero is a valid denied policy; no raising a supplied size to a minimum.
        require(self.minimum_quote_lamports <= self.ceiling_quote_lamports, "AUTHORITY_SIZE_RANGE_INVALID")


@dataclass(frozen=True, slots=True)
class CostLimits:
    # Venue fee is a bound INCLUDED in the full BUY quote cap, never an extra
    # principal reservation. Network/setup/protection are separate costs.
    venue_fee_within_quote_cap_lamports: int
    network_total_fee_lamports: int
    priority_fee_within_total_lamports: int
    setup_outflow_lamports: int
    refundable_account_lock_lamports: int
    # Required NEXT-exit headroom persists while a root is unretired. Paid
    # partial-reduction fees do not consume that future headroom.
    protective_network_fee_lamports: int
    protective_setup_lamports: int
    protective_refundable_lock_lamports: int
    # Shared per-root failed BUY/SELL allowance, never reset by partial exit.
    failed_attempt_count: int
    failed_attempt_network_budget_lamports: int
    maximum_impact_bps: int
    maximum_slippage_bps: int

    def __post_init__(self):
        for value in asdict(self).values():
            u64(value)
        require(self.priority_fee_within_total_lamports <= self.network_total_fee_lamports
                and self.maximum_impact_bps <= 10000 and self.maximum_slippage_bps <= 10000,
                "AUTHORITY_COST_LIMITS_INVALID")


@dataclass(frozen=True, slots=True)
class ClockPolicy:
    provider_id: str
    provider_fingerprint: str
    maximum_uncertainty_us: int
    maximum_drift_ppm: int
    maximum_checkpoint_elapsed_us: int

    def __post_init__(self):
        public_reference(self.provider_id)
        digest_value(self.provider_fingerprint)
        for value in (self.maximum_uncertainty_us, self.maximum_drift_ppm, self.maximum_checkpoint_elapsed_us):
            u64(value)
        require(self.maximum_drift_ppm <= 1000000 and self.maximum_checkpoint_elapsed_us > 0,
                "AUTHORITY_CLOCK_POLICY_INVALID")


@dataclass(frozen=True, slots=True)
class AuthorityPolicy:
    economic_domain_id: str
    policy_id: str
    approval: OperatorProvenance
    entry_valid_from_utc: str
    entry_valid_through_utc: str
    allowed_venues: tuple[str, ...]
    selected_track: str
    strategy_version: str
    parameter_set_id: str
    parameter_fingerprint: str
    model_fingerprint: str
    winner_binding_digest: str
    source_identity: str
    source_profile_fingerprint: str
    wallet_profile_fingerprint: str
    account_profile: str
    size: EntrySizeLimits
    costs: CostLimits
    clock: ClockPolicy
    version: str = VERSION

    def __post_init__(self):
        for value in (self.economic_domain_id, self.parameter_fingerprint, self.model_fingerprint,
                      self.winner_binding_digest, self.source_identity, self.source_profile_fingerprint, self.wallet_profile_fingerprint):
            digest_value(value)
        for value in (self.policy_id, self.strategy_version, self.parameter_set_id):
            public_reference(value)
        for name in ("entry_valid_from_utc", "entry_valid_through_utc"):
            object.__setattr__(self, name, ledger_utc(getattr(self, name)))
        require(type(self.approval) is OperatorProvenance and type(self.size) is EntrySizeLimits
                and type(self.costs) is CostLimits and type(self.clock) is ClockPolicy and self.version == VERSION,
                "AUTHORITY_EXPLICIT_POLICY_TYPES_REQUIRED")
        require(type(self.allowed_venues) is tuple and len(set(self.allowed_venues)) == len(self.allowed_venues)
                and all(item in ("PUMP", "PUMPSWAP") for item in self.allowed_venues)
                and self.selected_track in TRACKS and self.account_profile == "LEDGER_SETTLEMENT_SUPPORTED_V0_1"
                and self.entry_valid_from_utc <= self.entry_valid_through_utc, "AUTHORITY_POLICY_PROFILE_INVALID")

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


# V02 pins the static accepted selection manifest and original role; the old
# v0.1 policy still pins its exact candidate winner digest on historical replay.
# No candidate bytes, identity, locked selection or research facts are changed.
@dataclass(frozen=True, slots=True)
class AuthorityPolicyV02:
    economic_domain_id: str
    policy_id: str
    approval: OperatorProvenance
    entry_valid_from_utc: str
    entry_valid_through_utc: str
    allowed_venues: tuple[str, ...]
    selected_track: str
    strategy_version: str
    parameter_set_id: str
    parameter_fingerprint: str
    model_fingerprint: str
    selection_manifest_digest: str
    winner_role: str
    source_identity: str
    source_profile_fingerprint: str
    wallet_profile_fingerprint: str
    account_profile: str
    size: EntrySizeLimits
    costs: CostLimits
    clock: ClockPolicy
    version: str = "live_authority_policy_v0.2"

    def __post_init__(self):
        for value in (self.economic_domain_id, self.parameter_fingerprint, self.model_fingerprint,
                      self.selection_manifest_digest, self.source_identity, self.source_profile_fingerprint, self.wallet_profile_fingerprint):
            digest_value(value)
        require(self.selection_manifest_digest == LOCKED_SELECTION_SHA256 and self.winner_role in LOCKED_ROLE_ORDER
                and self.parameter_set_id == LOCKED_PARAMETER_SET_BY_ROLE[self.winner_role],
                "AUTHORITY_LOCKED_SELECTION_PROFILE_REQUIRED")
        for value in (self.policy_id, self.strategy_version, self.parameter_set_id):
            public_reference(value)
        for name in ("entry_valid_from_utc", "entry_valid_through_utc"):
            object.__setattr__(self, name, ledger_utc(getattr(self, name)))
        require(type(self.approval) is OperatorProvenance and type(self.size) is EntrySizeLimits
                and type(self.costs) is CostLimits and type(self.clock) is ClockPolicy and self.version == "live_authority_policy_v0.2",
                "AUTHORITY_EXPLICIT_POLICY_TYPES_REQUIRED")
        require(type(self.allowed_venues) is tuple and len(set(self.allowed_venues)) == len(self.allowed_venues)
                and all(item in ("PUMP", "PUMPSWAP") for item in self.allowed_venues)
                and self.selected_track in TRACKS and self.account_profile == "LEDGER_SETTLEMENT_SUPPORTED_V0_1"
                and self.entry_valid_from_utc <= self.entry_valid_through_utc, "AUTHORITY_POLICY_PROFILE_INVALID")

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class ArmingGrant:
    grant_id: str
    policy_digest: str
    scope: str
    root_id: str | None
    entry_valid_from_utc: str
    entry_valid_through_utc: str
    approval: OperatorProvenance

    def __post_init__(self):
        public_reference(self.grant_id)
        digest_value(self.policy_digest)
        require(self.scope in SCOPES and type(self.approval) is OperatorProvenance, "AUTHORITY_GRANT_SCOPE_INVALID")
        if self.root_id is not None:
            digest_value(self.root_id)
        require((self.scope == "ENTRY_ONCE") == (self.root_id is not None), "AUTHORITY_ONE_TIME_EXACT_ROOT_REQUIRED")
        for name in ("entry_valid_from_utc", "entry_valid_through_utc"):
            object.__setattr__(self, name, ledger_utc(getattr(self, name)))
        require(self.entry_valid_from_utc <= self.entry_valid_through_utc, "AUTHORITY_GRANT_INTERVAL_INVALID")


@dataclass(frozen=True, slots=True)
class ControlCommand:
    command_id: str
    economic_domain_id: str
    operation: str
    approval: OperatorProvenance
    policy: AuthorityPolicy | None = None
    grant: ArmingGrant | None = None
    target_id: str | None = None
    barrier_audit_digest: str | None = None
    version: str = VERSION

    def __post_init__(self):
        public_reference(self.command_id)
        digest_value(self.economic_domain_id)
        require(type(self.approval) is OperatorProvenance and self.version == VERSION, "AUTHORITY_COMMAND_PROVENANCE_REQUIRED")
        shapes = {"INSTALL_POLICY": (True, False, False, False), "ARM": (False, True, False, False),
                  "DISARM_ENTRY": (False, False, False, False), "STOP_ENTRY": (False, False, False, False),
                  "HARD_STOP": (False, False, False, False), "RELEASE_STOP": (False, False, True, True),
                  "REVOKE_GRANT": (False, False, True, False)}
        require(self.operation in shapes and tuple(value is not None for value in
                (self.policy, self.grant, self.target_id, self.barrier_audit_digest)) == shapes[self.operation],
                "AUTHORITY_CLOSED_CONTROL_SHAPE_REQUIRED")
        if self.policy is not None:
            require(type(self.policy) in (AuthorityPolicy, AuthorityPolicyV02) and self.policy.approval == self.approval,
                    "AUTHORITY_POLICY_APPROVAL_BINDING_CONFLICT")
        if self.grant is not None:
            require(type(self.grant) is ArmingGrant and self.grant.approval == self.approval,
                    "AUTHORITY_GRANT_APPROVAL_BINDING_CONFLICT")
        if self.target_id is not None:
            public_reference(self.target_id)
        if self.barrier_audit_digest is not None:
            digest_value(self.barrier_audit_digest)

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class ClockReconciliation:
    previous_sample_digest: str
    previous_epoch_id: str
    previous_monotonic_ns: int
    checkpoint_record_digest: str

    def __post_init__(self):
        digest_value(self.previous_sample_digest)
        digest_value(self.checkpoint_record_digest)
        public_reference(self.previous_epoch_id)
        u64(self.previous_monotonic_ns)


@dataclass(frozen=True, slots=True)
class TrustedClockSample:
    provider_id: str
    provider_fingerprint: str
    epoch_id: str
    monotonic_ns: int
    utc_lower_utc: str
    utc_upper_utc: str
    observation_record_digest: str
    previous_sample_digest: str
    status: str
    reconciliation: ClockReconciliation | None = None

    def __post_init__(self):
        for value in (self.provider_id, self.epoch_id):
            public_reference(value)
        for value in (self.provider_fingerprint, self.observation_record_digest, self.previous_sample_digest):
            digest_value(value)
        u64(self.monotonic_ns)
        for name in ("utc_lower_utc", "utc_upper_utc"):
            object.__setattr__(self, name, ledger_utc(getattr(self, name)))
        require(self.utc_lower_utc <= self.utc_upper_utc and self.status in ("QUALIFIED", "UNKNOWN")
                and (self.reconciliation is None or type(self.reconciliation) is ClockReconciliation),
                "AUTHORITY_CLOCK_INPUT_INVALID")

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class SourceCheckpoint:
    source_identity: str
    profile_fingerprint: str
    sequence: int
    record_digest: str


@dataclass(frozen=True, slots=True)
class AuthorityState:
    economic_domain_id: str
    policy: AuthorityPolicy | None = None
    grant: ArmingGrant | None = None
    entry_stop_command: str | None = None
    hard_stop_command: str | None = None
    last_operator_at_utc: str | None = None
    last_qualified_clock: TrustedClockSample | None = None
    clock_requires_reconciliation: bool = False
    source_checkpoints: tuple[SourceCheckpoint, ...] = ()
    version: str = VERSION

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


def apply_control(domain, state, command, *, known_policy=None, known_grant=None):
    require(command.economic_domain_id == domain.economic_domain_id == state.economic_domain_id,
            "AUTHORITY_CONTROL_DOMAIN_CONFLICT")
    at = command.approval.recorded_at_utc
    require(state.last_operator_at_utc is None or at >= state.last_operator_at_utc, "AUTHORITY_OPERATOR_TIME_REGRESSION")
    updated = replace(state, last_operator_at_utc=at)
    if command.operation == "INSTALL_POLICY":
        policy = command.policy
        require(policy.economic_domain_id == domain.economic_domain_id
                and policy.wallet_profile_fingerprint == domain.expected_profile_fingerprint,
                "AUTHORITY_POLICY_DOMAIN_PROFILE_CONFLICT")
        require(known_policy is None or known_policy == policy, "AUTHORITY_POLICY_ID_CONTENT_CONFLICT")
        require(known_policy is None, "AUTHORITY_POLICY_VERSION_ALREADY_RECORDED")
        # Installing a version is never arming. Stops are independently latched.
        updated = replace(updated, policy=policy, grant=None)
    elif command.operation == "ARM":
        grant, policy = command.grant, state.policy
        require(policy is not None and grant.policy_digest == policy.content_digest, "AUTHORITY_CURRENT_POLICY_REQUIRED")
        require(known_grant is None, "AUTHORITY_GRANT_ID_ALREADY_ISSUED")
        require((domain.mode == "DRY") == (grant.scope == "DRY"), "AUTHORITY_GRANT_MODE_CONFLICT")
        require(policy.entry_valid_from_utc <= grant.entry_valid_from_utc <= grant.entry_valid_through_utc <= policy.entry_valid_through_utc,
                "AUTHORITY_GRANT_EXCEEDS_POLICY_INTERVAL")
        require(state.entry_stop_command is None and state.hard_stop_command is None, "AUTHORITY_STOP_LATCHED")
        updated = replace(updated, grant=grant)
    elif command.operation == "DISARM_ENTRY":
        updated = replace(updated, grant=None)
    elif command.operation in ("STOP_ENTRY", "HARD_STOP"):
        field = "entry_stop_command" if command.operation == "STOP_ENTRY" else "hard_stop_command"
        require(getattr(state, field) is None, "AUTHORITY_STOP_ALREADY_LATCHED")
        updated = replace(updated, **{field: command.command_id}, grant=None)
    elif command.operation == "RELEASE_STOP":
        require(command.target_id in (state.entry_stop_command, state.hard_stop_command), "AUTHORITY_EXACT_LATCH_REQUIRED")
        updated = replace(updated, grant=None,
            entry_stop_command=None if command.target_id == state.entry_stop_command else state.entry_stop_command,
            hard_stop_command=None if command.target_id == state.hard_stop_command else state.hard_stop_command)
    elif command.operation == "REVOKE_GRANT":
        require(known_grant is not None, "AUTHORITY_KNOWN_GRANT_REQUIRED")
        updated = replace(updated, grant=None if state.grant is not None and state.grant.grant_id == command.target_id else state.grant)
    return updated


def clock_reasons(state, sample):
    reasons = []
    policy = None if state.policy is None else state.policy.clock
    if policy is None:
        return ("CLOCK_POLICY_MISSING",)
    if (sample.provider_id, sample.provider_fingerprint) != (policy.provider_id, policy.provider_fingerprint):
        reasons.append("CLOCK_PROVIDER_UNSUPPORTED")
    if sample.status != "QUALIFIED":
        reasons.append("CLOCK_UNKNOWN")
    lower, upper = utc_microseconds(sample.utc_lower_utc), utc_microseconds(sample.utc_upper_utc)
    if upper-lower > policy.maximum_uncertainty_us:
        reasons.append("CLOCK_UNCERTAINTY_EXCEEDED")
    previous = state.last_qualified_clock
    if sample.previous_sample_digest != (ZERO_DIGEST if previous is None else previous.content_digest):
        reasons.append("CLOCK_CHECKPOINT_DIGEST_CONFLICT")
    if previous is None:
        if sample.reconciliation is not None:
            reasons.append("CLOCK_UNEXPECTED_RECONCILIATION")
    else:
        old_lower, old_upper = utc_microseconds(previous.utc_lower_utc), utc_microseconds(previous.utc_upper_utc)
        if lower < old_lower or upper < old_upper:
            reasons.append("CLOCK_BACKWARD_BOUNDS")
        needed = (sample.epoch_id != previous.epoch_id or state.clock_requires_reconciliation
                  or (sample.provider_id, sample.provider_fingerprint) != (previous.provider_id, previous.provider_fingerprint))
        if needed:
            expected = (previous.content_digest, previous.epoch_id, previous.monotonic_ns)
            proof = sample.reconciliation
            if proof is None or (proof.previous_sample_digest, proof.previous_epoch_id, proof.previous_monotonic_ns) != expected:
                reasons.append("CLOCK_CONTINUITY_RECONCILIATION_REQUIRED")
        elif sample.reconciliation is not None:
            reasons.append("CLOCK_UNEXPECTED_RECONCILIATION")
        if sample.epoch_id == previous.epoch_id:
            elapsed_ns = sample.monotonic_ns-previous.monotonic_ns
            if elapsed_ns < 0:
                reasons.append("CLOCK_MONOTONIC_REGRESSION")
            else:
                if elapsed_ns > policy.maximum_checkpoint_elapsed_us*1000:
                    reasons.append("CLOCK_CHECKPOINT_TOO_OLD")
                # Exact integer interval intersection; no float rounding.
                low_ns, high_ns = (lower-old_upper)*1000, (upper-old_lower)*1000
                if high_ns*1000000 < elapsed_ns*(1000000-policy.maximum_drift_ppm) or low_ns*1000000 > elapsed_ns*(1000000+policy.maximum_drift_ppm):
                    reasons.append("CLOCK_UTC_MONOTONIC_DIVERGENCE")
    return tuple(sorted(set(reasons)))


@dataclass(frozen=True, slots=True)
class EntryBinding:
    root_id: str
    candidate_digest: str
    track: str
    signal_at_us: int
    deadline_us: int


def bind_entry(domain, candidate, track, previous=None):
    require(type(candidate) is CandidateInboxInput and track in TRACKS, "AUTHORITY_ORIGINAL_CANDIDATE_TRACK_REQUIRED")
    deadline = deadline_for(track, datetime.fromisoformat(utc_from_us(candidate.generated_at_us)))
    value = EntryBinding(candidate.trade_root(domain), candidate.content_digest, track, candidate.generated_at_us,
                         utc_microseconds(deadline.eligible_through_at.isoformat()))
    require(previous is None or previous == value, "AUTHORITY_ORIGINAL_DEADLINE_BINDING_CONFLICT")
    return value


@dataclass(frozen=True, slots=True)
class EligibilityInput:
    root_id: str
    candidate_digest: str
    track: str
    clock: TrustedClockSample
    source_sequence: int | None
    source: SourceVerdict | None
    previous_source_record_digest: str | None

    def __post_init__(self):
        digest_value(self.root_id)
        digest_value(self.candidate_digest)
        require(self.track in TRACKS and type(self.clock) is TrustedClockSample
                and ((self.source is None) == (self.source_sequence is None)), "AUTHORITY_ELIGIBILITY_INPUT_INVALID")
        if self.previous_source_record_digest is not None:
            digest_value(self.previous_source_record_digest)
        if self.source is not None:
            require(type(self.source) is SourceVerdict and type(self.source_sequence) is int and self.source_sequence > 0,
                    "AUTHORITY_ORIGINAL_SOURCE_RECORD_REQUIRED")


@dataclass(frozen=True, slots=True)
class EligibilityDecision:
    binding: EntryBinding
    disposition: str
    reasons: tuple[str, ...]
    clock_reasons: tuple[str, ...]
    policy_digest: str | None
    grant_id: str | None
    source_digest: str | None
    source_sequence: int | None
    source_identity: str | None
    source_profile_fingerprint: str | None
    source_cut_utc: str | None
    grants_message_permission: bool = False
    consumes_grant: bool = False


def evaluate_entry(domain, state, candidate, inbox_disposition, supplied, previous_binding=None):
    binding = bind_entry(domain, candidate, supplied.track, previous_binding)
    require((supplied.root_id, supplied.candidate_digest) == (binding.root_id, binding.candidate_digest),
            "AUTHORITY_ELIGIBILITY_CANDIDATE_CONFLICT")
    clock_denials = clock_reasons(state, supplied.clock)
    reasons = list(clock_denials)
    sample, policy, grant, source = supplied.clock, state.policy, state.grant, supplied.source
    if inbox_disposition not in ("RECEIVED", "DENIED_RETRYABLE"):
        reasons.append("INBOX_NOT_AVAILABLE_FOR_NEW_ADMISSION")
    if candidate.winner_candidate_id != candidate.candidate_signal_id:
        reasons.append("CANONICAL_WINNER_REQUIRED")
    if state.entry_stop_command is not None:
        reasons.append("ENTRY_STOP_LATCHED")
    if state.hard_stop_command is not None:
        reasons.append("GLOBAL_HARD_STOP_LATCHED")
    if policy is None:
        reasons.append("POLICY_MISSING")
    else:
        selection_matches = (policy.winner_binding_digest == candidate.winner_binding_digest if type(policy) is AuthorityPolicy
            else policy.selection_manifest_digest == LOCKED_SELECTION_SHA256 and policy.winner_role == candidate.winner_role)
        if not selection_matches or (policy.selected_track, policy.strategy_version, policy.parameter_set_id, policy.parameter_fingerprint,
                policy.model_fingerprint, policy.source_identity) != (
                supplied.track, candidate.strategy_version, candidate.parameter_set_id, candidate.parameter_fingerprint,
                candidate.model_fingerprint, candidate.source_binding.source_identity):
            reasons.append("POLICY_ORIGINAL_CANDIDATE_PROFILE_CONFLICT")
        if not policy.allowed_venues or not policy.size.fixed_quote_lamports or not policy.size.max_open_positions:
            reasons.append("EXPLICIT_ZERO_ENTRY_POLICY")
        if sample.utc_lower_utc < policy.entry_valid_from_utc or sample.utc_upper_utc > policy.entry_valid_through_utc:
            reasons.append("POLICY_ENTRY_INTERVAL_UNPROVEN")
    if grant is None:
        reasons.append("ENTRY_UNARMED")
    else:
        if policy is None or grant.policy_digest != policy.content_digest or grant.scope == "ENTRY_ONCE" and grant.root_id != binding.root_id:
            reasons.append("GRANT_POLICY_OR_ROOT_CONFLICT")
        if sample.utc_lower_utc < grant.entry_valid_from_utc or sample.utc_upper_utc > grant.entry_valid_through_utc:
            reasons.append("GRANT_ENTRY_INTERVAL_UNPROVEN")
    if state.last_operator_at_utc is not None and sample.utc_lower_utc < state.last_operator_at_utc:
        reasons.append("CONTROL_KNOWLEDGE_CLOCK_UNPROVEN")
    expired = False
    if not clock_denials:
        lower, upper = utc_microseconds(sample.utc_lower_utc), utc_microseconds(sample.utc_upper_utc)
        expired = lower > binding.deadline_us
        if expired:
            reasons.append("ORIGINAL_CANDIDATE_EXPIRED")
        elif upper > binding.deadline_us:
            reasons.append("ORIGINAL_DEADLINE_AMBIGUOUS")
        if lower < binding.signal_at_us:
            reasons.append("ORIGINAL_SIGNAL_NOT_YET_KNOWN")
    checkpoints = state.source_checkpoints
    if source is None:
        reasons.append("CURRENT_SOURCE_MISSING")
    else:
        old = next((item for item in checkpoints if (item.source_identity, item.profile_fingerprint) ==
            (source.binding.source_identity, source.profile.fingerprint)), None)
        continuity = (old is None and supplied.previous_source_record_digest is None or old is not None
            and supplied.previous_source_record_digest == old.record_digest and supplied.source_sequence >= old.sequence
            and (supplied.source_sequence != old.sequence or source.content_digest == old.record_digest))
        if not continuity:
            reasons.append("SOURCE_SEQUENCE_OR_RETAINED_RECORD_CONFLICT")
        elif source.binding.source_identity == candidate.source_binding.source_identity:
            checkpoint = SourceCheckpoint(source.binding.source_identity, source.profile.fingerprint, supplied.source_sequence, source.content_digest)
            checkpoints = tuple(item for item in checkpoints if item != old) + (checkpoint,)
        port = source_consumer_evidence(source, expected_source_identity=candidate.source_binding.source_identity,
            required_cut_utc=source.snapshot.requested_cut_utc, now_utc=sample.utc_upper_utc)
        reasons.extend(port.reasons)
        if not port.observed_prefix_supported:
            reasons.append("CURRENT_SOURCE_NOT_USABLE")
        if policy is None or source.profile.fingerprint != policy.source_profile_fingerprint:
            reasons.append("SOURCE_POLICY_PROFILE_CONFLICT")
        if utc_microseconds(sample.utc_lower_utc) < utc_microseconds(source.snapshot.observed_at_utc) or utc_microseconds(source.snapshot.requested_cut_utc) < candidate.generated_at_us:
            reasons.append("SOURCE_KNOWLEDGE_CUT_UNPROVEN")
        if (not source.covered_from_utc or not source.covered_through_utc
                or not utc_microseconds(source.covered_from_utc) <= candidate.generated_at_us <= utc_microseconds(source.covered_through_utc)
                or candidate.generated_at_us < utc_microseconds(source.binding.coverage_start_utc)):
            reasons.append("ORIGINAL_SIGNAL_SOURCE_COVERAGE_UNPROVEN")
    reasons = tuple(sorted(set(reasons)))
    decision = EligibilityDecision(binding, "EXPIRED" if expired else "ELIGIBLE_CONTEXT_ONLY" if not reasons else "DENIED_UNPROVEN",
        reasons, clock_denials, None if policy is None else policy.content_digest, None if grant is None else grant.grant_id,
        None if source is None else source.content_digest, supplied.source_sequence,
        None if source is None else source.binding.source_identity, None if source is None else source.profile.fingerprint,
        None if source is None else source.snapshot.requested_cut_utc)
    updated = replace(state, last_qualified_clock=state.last_qualified_clock if clock_denials else sample,
                      clock_requires_reconciliation=bool(clock_denials), source_checkpoints=checkpoints)
    return decision, updated


# Explicit finite reconstruction. No registry, imports by name or arbitrary codec.
def policy_from_record(record):
    value = dict(record)
    value["approval"] = OperatorProvenance(**value["approval"])
    value["allowed_venues"] = tuple(value["allowed_venues"])
    value["size"] = EntrySizeLimits(**value["size"])
    value["costs"] = CostLimits(**value["costs"])
    value["clock"] = ClockPolicy(**value["clock"])
    return (AuthorityPolicyV02 if value.get("version") == "live_authority_policy_v0.2" else AuthorityPolicy)(**value)


def grant_from_record(record):
    return ArmingGrant(**{**record, "approval": OperatorProvenance(**record["approval"])})


def command_from_record(record):
    value = dict(record)
    value["approval"] = OperatorProvenance(**value["approval"])
    value["policy"] = None if value["policy"] is None else policy_from_record(value["policy"])
    value["grant"] = None if value["grant"] is None else grant_from_record(value["grant"])
    return ControlCommand(**value)


def clock_from_record(record):
    value = dict(record)
    value["reconciliation"] = None if value["reconciliation"] is None else ClockReconciliation(**value["reconciliation"])
    return TrustedClockSample(**value)


def eligibility_from_record(record):
    return EligibilityInput(record["root_id"], record["candidate_digest"], record["track"], clock_from_record(record["clock"]),
        record["source_sequence"], None if record["source"] is None else verdict_from_json(canonical_json(record["source"])),
        record["previous_source_record_digest"])


@dataclass(frozen=True, slots=True)
class AuthorityReceipt:
    sequence: int
    command_id: str
    kind: str
    original: ControlCommand | EligibilityInput
    decision: EligibilityDecision | None
    previous_state_digest: str
    resulting_state_digest: str
    common_previous_digest: str
    version: str = VERSION

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


def authority_receipt_from_record(record):
    value = dict(record)
    require(value.get("version") == VERSION and value.get("kind") in ("AUTHORITY_CONTROL", "AUTHORITY_ELIGIBILITY"),
            "AUTHORITY_RECEIPT_VERSION_OR_KIND_INVALID")
    value["original"] = command_from_record(value["original"]) if value["kind"] == "AUTHORITY_CONTROL" else eligibility_from_record(value["original"])
    if value["decision"] is not None:
        decision = dict(value["decision"])
        decision["binding"] = EntryBinding(**decision["binding"])
        decision["reasons"], decision["clock_reasons"] = tuple(decision["reasons"]), tuple(decision["clock_reasons"])
        value["decision"] = EligibilityDecision(**decision)
    return AuthorityReceipt(**value)
