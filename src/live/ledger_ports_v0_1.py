"""Finite future-consumer storage inputs. No Authority or Runtime policies.

References identify externally produced decisions; recording them is not a
real operating grant. Original candidate terms and public Evidence stay exact.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace

from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from .ledger_actions_v0_1 import CandidateInboxInput, PendingAction, public_reference, utc_microseconds, candidate_from_json, action_from_json
from .ledger_domain_v0_1 import LedgerContractError, digest_value, ledger_utc
from .public_rpc_v0_1 import u64, primary_signature as public_signature

PORT_VERSION = "live_ledger_consumer_ports_v0.1"
TRACKS = frozenset(("FINAL-A", "FINAL-B", "SENS-C"))
GROUP_KINDS = frozenset(("ADMISSION", "PROTECTION", "RETIREMENT", "DRY_NON_SUBMITTED", "SETTLEMENT_PORTS"))


def reservation_identity(economic_domain_id, root_id):
    return content_fingerprint({"identity": "LEDGER_ROOT_RESERVATION", "domain": economic_domain_id, "root_id": root_id})


@dataclass(frozen=True, slots=True)
class ConsumerCut:
    economic_domain_id: str
    revision: int
    commit_digest: str
    custody_digest: str

    def __post_init__(self):
        for value in (self.economic_domain_id, self.commit_digest, self.custody_digest):
            digest_value(value)
        u64(self.revision)


@dataclass(frozen=True, slots=True)
class NativeReservationVector:
    """External exact encumbrance for the supported native-entry profile.

    Ledger neither allocates this vector nor computes available capacity from
    it. Amounts remain separate from actual debits and paid transaction fees.
    """
    principal_lamports: int
    network_fee_lamports: int
    venue_fee_lamports: int
    account_setup_lamports: int
    protective_network_lamports: int
    protective_setup_lamports: int

    def __post_init__(self):
        for value in asdict(self).values():
            u64(value)


@dataclass(frozen=True, slots=True)
class AdmissionInput:
    cut: ConsumerCut
    action: PendingAction
    reservation: NativeReservationVector
    authority_decision_ref: str
    authority_decision_digest: str
    recorded_at_utc: str

    def __post_init__(self):
        if type(self.cut) is not ConsumerCut or type(self.action) is not PendingAction or type(self.reservation) is not NativeReservationVector:
            raise LedgerContractError("LEDGER_ADMISSION_CONCRETE_INPUT_REQUIRED")
        public_reference(self.authority_decision_ref)
        digest_value(self.authority_decision_digest)
        object.__setattr__(self, "recorded_at_utc", ledger_utc(self.recorded_at_utc))
        if (self.action.side != "BUY" or self.action.selected_exit_track not in TRACKS
                or self.reservation.principal_lamports != self.action.input_units):
            raise LedgerContractError("LEDGER_ADMISSION_FIXED_NATIVE_TERMS_REQUIRED")

    @property
    def reservation_id(self):
        return reservation_identity(self.cut.economic_domain_id, self.action.root_id)

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class ProtectiveHandoffInput:
    cut: ConsumerCut
    root_id: str
    position_id: str
    candidate: CandidateInboxInput
    selected_track: str
    selected_policy_digest: str
    acquisition_signature: str
    acquisition_proposal_digest: str
    acquired_units: int
    fallback_anchor_us: int
    fallback_deadline_us: int
    fallback_fire_boundary_us: int
    fallback_rule_ref: str
    fallback_rule_digest: str
    runtime_binding_ref: str
    runtime_binding_digest: str
    obligation_state: str
    trigger_kind: str
    trigger_at_us: int | None
    trigger_evidence_digest: str | None
    knowledge_chain_receipt_digest: str
    knowledge_at_utc: str
    obligation_decision_ref: str
    obligation_decision_digest: str
    recorded_at_utc: str

    def __post_init__(self):
        if type(self.cut) is not ConsumerCut or type(self.candidate) is not CandidateInboxInput:
            raise LedgerContractError("LEDGER_HANDOFF_CONCRETE_INPUT_REQUIRED")
        for value in (self.root_id, self.position_id, self.selected_policy_digest, self.acquisition_proposal_digest,
                      self.fallback_rule_digest, self.runtime_binding_digest, self.knowledge_chain_receipt_digest,
                      self.obligation_decision_digest):
            digest_value(value)
        public_signature(self.acquisition_signature)
        public_reference(self.fallback_rule_ref)
        public_reference(self.runtime_binding_ref)
        public_reference(self.obligation_decision_ref)
        for value in (self.acquired_units, self.fallback_anchor_us, self.fallback_deadline_us, self.fallback_fire_boundary_us):
            u64(value)
        object.__setattr__(self, "knowledge_at_utc", ledger_utc(self.knowledge_at_utc))
        object.__setattr__(self, "recorded_at_utc", ledger_utc(self.recorded_at_utc))
        if (self.selected_track not in TRACKS or self.acquired_units == 0
                or self.fallback_anchor_us != self.candidate.generated_at_us or self.fallback_deadline_us < self.fallback_anchor_us
                or self.fallback_fire_boundary_us != self.fallback_deadline_us+1
                or self.knowledge_at_utc > self.recorded_at_utc):
            raise LedgerContractError("LEDGER_HANDOFF_ORIGINAL_TRACK_OR_FALLBACK_REQUIRED")
        # Exact deadline remains market-eligible; the fixed externally supplied
        # timer boundary is deadline+1us (accepted Phase-4 orchestrator 301-305).
        # This validates a supplied Runtime decision; it does not run a timer.
        if self.obligation_state == "MONITORING":
            if (self.trigger_kind != "NONE" or self.trigger_at_us is not None or self.trigger_evidence_digest is not None
                    or utc_microseconds(self.recorded_at_utc) >= self.fallback_fire_boundary_us):
                raise LedgerContractError("LEDGER_ALREADY_DUE_OR_UNBOUND_MONITORING_INPUT")
        elif self.obligation_state == "DUE":
            u64(self.trigger_at_us)
            digest_value(self.trigger_evidence_digest)
            if (self.trigger_kind not in ("FALLBACK", "MARKET") or self.trigger_at_us > utc_microseconds(self.knowledge_at_utc)
                    or self.trigger_kind == "FALLBACK" and self.trigger_at_us != self.fallback_fire_boundary_us
                    or self.trigger_kind == "MARKET" and self.trigger_at_us > self.fallback_deadline_us):
                raise LedgerContractError("LEDGER_DUE_TRIGGER_ORIGINAL_BOUNDARY_CONFLICT")
        else:
            raise LedgerContractError("LEDGER_CONCRETE_RUNTIME_OBLIGATION_REQUIRED")

    @property
    def binding_id(self):
        return content_fingerprint({"identity": "LEDGER_POSITION_PROTECTION", "domain": self.cut.economic_domain_id,
            "position_id": self.position_id, "track": self.selected_track, "policy_digest": self.selected_policy_digest})

    @property
    def obligation_id(self):
        return content_fingerprint({"identity": "LEDGER_FULL_REDUCTION_OBLIGATION", "binding_id": self.binding_id})

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class RetirementInput:
    cut: ConsumerCut
    root_id: str
    reservation_id: str
    position_id: str
    terminal_attempt_id: str
    reason: str
    wallet_support_digest: str
    authority_intent_ref: str
    authority_intent_digest: str
    recorded_at_utc: str

    def __post_init__(self):
        if type(self.cut) is not ConsumerCut:
            raise LedgerContractError("LEDGER_RETIREMENT_CONCRETE_CUT_REQUIRED")
        for value in (self.root_id, self.reservation_id, self.position_id, self.terminal_attempt_id,
                      self.wallet_support_digest, self.authority_intent_digest):
            digest_value(value)
        public_reference(self.authority_intent_ref)
        object.__setattr__(self, "recorded_at_utc", ledger_utc(self.recorded_at_utc))
        if self.reason not in ("FULLY_REDUCED", "FAILED_NO_ACQUISITION", "PROVEN_NON_LANDED", "POSITIVE_UNSIGNED_CANCEL"):
            raise LedgerContractError("LEDGER_RETIREMENT_REASON_UNSUPPORTED")

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class DryTerminalInput:
    cut: ConsumerCut
    root_id: str
    action_id: str
    attempt_id: str | None
    preparation_digest: str | None
    message_sha256: str | None
    simulation_input_digest: str | None
    reason: str
    runtime_disposition_ref: str
    runtime_disposition_digest: str
    recorded_at_utc: str

    def __post_init__(self):
        if type(self.cut) is not ConsumerCut:
            raise LedgerContractError("LEDGER_DRY_CONCRETE_CUT_REQUIRED")
        for value in (self.root_id, self.action_id, self.runtime_disposition_digest):
            digest_value(value)
        if self.attempt_id is not None:
            for value in (self.attempt_id, self.preparation_digest, self.message_sha256):
                digest_value(value)
        elif self.preparation_digest is not None or self.message_sha256 is not None or self.simulation_input_digest is not None:
            raise LedgerContractError("LEDGER_DRY_ABSENT_PREPARATION_CANNOT_INVENT_LINEAGE")
        if self.simulation_input_digest is not None:
            digest_value(self.simulation_input_digest)
        public_reference(self.runtime_disposition_ref)
        object.__setattr__(self, "recorded_at_utc", ledger_utc(self.recorded_at_utc))
        if self.reason not in ("DRY_CAPABILITY", "DRY_INTERRUPTED_NO_SIGNED_GRAPH", "DRY_INTERRUPTED_BEFORE_PREPARATION"):
            raise LedgerContractError("LEDGER_DRY_TERMINAL_REASON_REQUIRED")
        if (self.attempt_id is None) != (self.reason == "DRY_INTERRUPTED_BEFORE_PREPARATION"):
            raise LedgerContractError("LEDGER_DRY_EXACT_PREPARATION_PRESENCE_REQUIRED")

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class ReservationFact:
    admission: AdmissionInput
    sequence: int
    status: str
    retired_sequence: int | None = None
    terminal_disposition: str | None = None
    has_real_authority_grant: bool = field(init=False, default=False)

    @property
    def reservation_id(self):
        return self.admission.reservation_id

    @property
    def root_id(self):
        return self.admission.action.root_id


@dataclass(frozen=True, slots=True)
class ProtectionFact:
    handoff: ProtectiveHandoffInput
    sequence: int
    obligation_state: str
    implements_runtime_controller: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class DryDispositionFact:
    input: DryTerminalInput
    sequence: int
    action_id: str
    reservation_id: str
    disposition: str = "NON_SUBMITTED"


def admission_from_record(record):
    row = dict(record)
    row["cut"] = ConsumerCut(**row["cut"])
    row["action"] = action_from_json(canonical_json(row["action"]))
    row["reservation"] = NativeReservationVector(**row["reservation"])
    value = AdmissionInput(**row)
    if canonical_json(asdict(value)) != canonical_json(record):
        raise LedgerContractError("LEDGER_ADMISSION_CODEC_CONFLICT")
    return value


def handoff_from_record(record):
    row = dict(record)
    row["cut"] = ConsumerCut(**row["cut"])
    row["candidate"] = candidate_from_json(canonical_json(row["candidate"]))
    value = ProtectiveHandoffInput(**row)
    if canonical_json(asdict(value)) != canonical_json(record):
        raise LedgerContractError("LEDGER_HANDOFF_CODEC_CONFLICT")
    return value


def retirement_from_record(record):
    row = dict(record)
    row["cut"] = ConsumerCut(**row["cut"])
    value = RetirementInput(**row)
    if canonical_json(asdict(value)) != canonical_json(record):
        raise LedgerContractError("LEDGER_RETIREMENT_CODEC_CONFLICT")
    return value


def dry_terminal_from_record(record):
    row = dict(record)
    row["cut"] = ConsumerCut(**row["cut"])
    value = DryTerminalInput(**row)
    if canonical_json(asdict(value)) != canonical_json(record):
        raise LedgerContractError("LEDGER_DRY_CODEC_CONFLICT")
    return value


def require_common_cut(domain, state, cut, revision, commit_digest):
    if cut != ConsumerCut(domain.economic_domain_id, revision, commit_digest, state.content_digest):
        raise LedgerContractError("LEDGER_CONSUMER_COMMON_CUT_CONFLICT")


def admit_reservation(domain, state, candidate, inbox_disposition, root_attempts, supplied, *, sequence):
    """Store external admission facts without computing a capital allocation."""
    if (state.baseline_receipt_digest is None or state.quarantined or root_attempts
            or inbox_disposition not in ("RECEIVED", "DENIED_RETRYABLE")
            or supplied.action.root_id != candidate.trade_root(domain)
            or supplied.action.candidate_digest != candidate.content_digest
            or not candidate.generated_at_us <= utc_microseconds(supplied.recorded_at_utc) <= supplied.action.claimed_entry_deadline_us
            or any(item.root_id == supplied.action.root_id for item in state.reservations)):
        raise LedgerContractError("LEDGER_ADMISSION_BASELINE_ROOT_CUT_OR_HISTORY_CONFLICT")
    fact = ReservationFact(supplied, sequence, "TENTATIVE_DRY" if domain.mode == "DRY" else "RESERVED")
    return replace(state, reservations=(*state.reservations, fact))


def activate_protection(domain, state, supplied, acquisition_receipt, chain_receipt, *, sequence):
    """Persist an actual Runtime decision and inventory usability together."""
    position = next((item for item in state.positions if item.position_id == supplied.position_id), None)
    reservation = next((item for item in state.reservations if item.root_id == supplied.root_id), None)
    proposal = None if acquisition_receipt is None else acquisition_receipt.decision.proposal
    if (domain.mode != "LIVE" or state.quarantined or position is None or position.remaining_units <= 0
            or reservation is None or reservation.status != "RESERVED" or proposal is None or proposal.base_units_delta <= 0
            or any(item.handoff.position_id == supplied.position_id for item in state.protections)
            or (position.root_id, position.acquisition_signature, position.acquired_units) !=
               (supplied.root_id, supplied.acquisition_signature, supplied.acquired_units)
            or proposal.content_digest != supplied.acquisition_proposal_digest or proposal.action_id != reservation.admission.action.action_id
            or supplied.selected_track != reservation.admission.action.selected_exit_track
            or supplied.selected_policy_digest != reservation.admission.action.policy_digest
            or supplied.candidate.content_digest != reservation.admission.action.candidate_digest
            or supplied.candidate.trade_root(domain) != supplied.root_id
            or chain_receipt is None or chain_receipt.content_digest != supplied.knowledge_chain_receipt_digest
            or chain_receipt.content_digest != acquisition_receipt.chain_receipt_digest
            or supplied.knowledge_at_utc < chain_receipt.decision.evaluated_at_utc
            or supplied.recorded_at_utc < acquisition_receipt.recorded_at_utc):
        raise LedgerContractError("LEDGER_PROTECTION_ACTUAL_ACQUISITION_ORIGINAL_BINDING_CONFLICT")
    position = replace(position, usable=True, has_protective_handoff=True, protective_binding_id=supplied.binding_id, status="OWNED_PROTECTED")
    return replace(state, positions=tuple(position if item.position_id == position.position_id else item for item in state.positions),
        protections=(*state.protections, ProtectionFact(supplied, sequence, supplied.obligation_state)))


def retire_reservation(domain, state, supplied, attempts, terminal_application, comparison, *, sequence):
    reservation = next((item for item in state.reservations if item.reservation_id == supplied.reservation_id), None)
    position = next((item for item in state.positions if item.position_id == supplied.position_id), None)
    terminal = next((item for item in attempts if item.preparation.attempt_id == supplied.terminal_attempt_id), None)
    resolution = state.resolution(supplied.terminal_attempt_id)
    proposal = None if terminal_application is None else terminal_application.decision.proposal
    if (domain.mode != "LIVE" or reservation is None or reservation.status != "RESERVED"
            or reservation.root_id != supplied.root_id or reservation.admission.action.position_id != supplied.position_id
            or terminal is None or comparison.input_digest != supplied.wallet_support_digest
            or comparison.evaluated_at_utc != supplied.recorded_at_utc):
        raise LedgerContractError("LEDGER_RETIREMENT_ROOT_INPUT_BINDING_CONFLICT")
    reasons = set()
    if state.quarantined or comparison.disposition != "MATCHED_AT_ORIGINAL_CUT":
        reasons.add("RETIREMENT_CURRENT_CUSTODY_UNRECONCILED")
    if position is not None and position.remaining_units != 0:
        reasons.add("RETIREMENT_ACTUAL_RESIDUAL_REMAINS")
    if any(state.resolution(item.preparation.attempt_id) is None and item.recorded_stage != "CANCELLED_UNSIGNED" for item in attempts):
        reasons.add("RETIREMENT_PENDING_ATTEMPT_OR_UNAPPLIED_COSTS")
    if supplied.reason == "FULLY_REDUCED":
        valid = position is not None and resolution is not None and resolution.disposition == "FINALIZED_SUCCESS_APPLIED" and proposal is not None and proposal.base_units_delta < 0
    elif supplied.reason == "FAILED_NO_ACQUISITION":
        valid = position is None and resolution is not None and resolution.disposition == "FINALIZED_FAILURE_APPLIED" and proposal is not None and proposal.base_units_delta == 0
    elif supplied.reason == "PROVEN_NON_LANDED":
        valid = position is None and resolution is not None and resolution.disposition == "PROVEN_NON_LANDED_RESOLVED"
    else:
        valid = position is None and terminal.recorded_stage == "CANCELLED_UNSIGNED" and terminal.primary_signature is None
    if supplied.recorded_at_utc < (terminal.last_recorded_at_utc if resolution is None else resolution.recorded_at_utc):
        raise LedgerContractError("LEDGER_RETIREMENT_TIME_PRECEDES_ORIGINAL_PROOF")
    if not valid:
        reasons.add("RETIREMENT_POSITIVE_TERMINAL_PROOF_REQUIRED")
    if reasons:
        return state, "WITHHELD", tuple(sorted(reasons))
    terminal_disposition = "RETIRED" if position is not None else "CLOSED_NO_ACQUISITION"
    reservation = replace(reservation, status="RETIRED", retired_sequence=sequence, terminal_disposition=terminal_disposition)
    updated = replace(state, reservations=tuple(reservation if item.reservation_id == reservation.reservation_id else item for item in state.reservations),
        positions=tuple(replace(item, status="RETIRED", usable=False) if item.position_id == supplied.position_id else item for item in state.positions),
        protections=tuple(replace(item, obligation_state="RETIRED_BY_FLAT_PROOF") if item.handoff.position_id == supplied.position_id else item for item in state.protections))
    return updated, "RETIRED", ()


def terminate_dry(domain, state, supplied, attempts, simulation_digest, *, sequence):
    reservation = next((item for item in state.reservations if item.root_id == supplied.root_id), None)
    if (domain.mode != "DRY" or reservation is None or reservation.status != "TENTATIVE_DRY"
            or reservation.admission.action.action_id != supplied.action_id or state.positions or state.resolutions
            or any(item.primary_signature is not None or item.signed_wire_digest is not None for item in attempts)
            or supplied.recorded_at_utc < reservation.admission.recorded_at_utc):
        raise LedgerContractError("LEDGER_DRY_FIXED_MODE_OR_TENTATIVE_RESERVATION_REQUIRED")
    if supplied.attempt_id is None:
        if attempts:
            raise LedgerContractError("LEDGER_DRY_PREPARATION_CANNOT_BE_OMITTED")
    else:
        if not attempts or any(item.preparation.action_id != supplied.action_id for item in attempts):
            raise LedgerContractError("LEDGER_DRY_EXACT_ACTION_LINEAGE_REQUIRED")
        ordered = sorted(attempts, key=lambda item: item.preparation.ordinal)
        if any(item.recorded_stage != "CANCELLED_UNSIGNED" for item in ordered[:-1]):
            raise LedgerContractError("LEDGER_DRY_PREDECESSOR_LINEAGE_UNRESOLVED")
        attempt = ordered[-1]
        if (attempt.preparation.attempt_id != supplied.attempt_id or attempt.preparation.content_digest != supplied.preparation_digest
                or attempt.preparation.message_sha256 != supplied.message_sha256
                or supplied.simulation_input_digest != simulation_digest or supplied.recorded_at_utc < attempt.last_recorded_at_utc
                or supplied.reason == "DRY_CAPABILITY" and simulation_digest is None):
            raise LedgerContractError("LEDGER_DRY_ORIGINAL_PREPARATION_SIMULATION_CONFLICT")
    reservation = replace(reservation, status="RETIRED", retired_sequence=sequence, terminal_disposition="NON_SUBMITTED")
    return replace(state, reservations=tuple(reservation if item.reservation_id == reservation.reservation_id else item for item in state.reservations),
        dry_dispositions=(*state.dry_dispositions, DryDispositionFact(supplied, sequence, supplied.action_id, reservation.reservation_id)))


@dataclass(frozen=True, slots=True)
class ConsumerPortReceipt:
    sequence: int
    ingestion_key: str
    kind: str
    admission: AdmissionInput | None
    handoff: ProtectiveHandoffInput | None
    retirement: RetirementInput | None
    dry_terminal: DryTerminalInput | None
    created_action_digest: str | None
    application_key: str | None
    application_digest: str | None
    retirement_comparison_digest: str | None
    retirement_disposition: str | None
    retirement_reasons: tuple[str, ...]
    prior_custody_digest: str
    resulting_custody_digest: str
    application_disposition: str | None = None
    version: str = field(init=False, default=PORT_VERSION)
    has_real_authority_grant: bool = field(init=False, default=False)

    def __post_init__(self):
        u64(self.sequence)
        public_reference(self.ingestion_key)
        for value in (self.prior_custody_digest, self.resulting_custody_digest):
            digest_value(value)
        for value in (self.created_action_digest, self.application_digest, self.retirement_comparison_digest):
            if value is not None:
                digest_value(value)
        if self.application_key is not None:
            public_reference(self.application_key)
        supplied = tuple(value is not None for value in (self.admission, self.handoff, self.retirement, self.dry_terminal))
        expected = {"ADMISSION": (True, False, False, False), "PROTECTION": (False, True, False, False),
                    "RETIREMENT": (False, False, True, False), "DRY_NON_SUBMITTED": (False, False, False, True)}
        if (self.kind not in GROUP_KINDS or self.kind != "SETTLEMENT_PORTS" and supplied != expected[self.kind]
                or self.kind == "SETTLEMENT_PORTS" and supplied not in ((False, True, False, False), (False, False, True, False))
                or (self.application_key is not None) != (self.kind == "SETTLEMENT_PORTS")
                or (self.application_digest is not None) != (self.application_key is not None)
                or (self.application_disposition is not None) != (self.application_key is not None)
                or self.application_disposition is not None and self.application_disposition not in ("FINALIZED_SUCCESS_APPLIED", "FINALIZED_FAILURE_APPLIED", "PROVEN_NON_LANDED_RESOLVED", "ALREADY_RESOLVED", "UNAPPLIED", "QUARANTINED")
                or self.created_action_digest is not None and self.kind != "ADMISSION"
                or (self.retirement_comparison_digest is not None) != (self.retirement is not None)
                or (self.retirement_disposition in ("RETIRED", "WITHHELD")) != (self.retirement is not None)
                or self.retirement is None and self.retirement_disposition is not None
                or type(self.retirement_reasons) is not tuple or any(type(value) is not str for value in self.retirement_reasons)
                or self.retirement_disposition != "WITHHELD" and self.retirement_reasons):
            raise LedgerContractError("LEDGER_CLOSED_CONSUMER_GROUP_SHAPE_CONFLICT")

    def to_record(self):
        return asdict(self)

    @property
    def content_digest(self):
        return content_fingerprint(self.to_record())


def port_receipt_from_record(record):
    row = dict(record)
    row.pop("version")
    row.pop("has_real_authority_grant")
    row["admission"] = None if row["admission"] is None else admission_from_record(row["admission"])
    row["handoff"] = None if row["handoff"] is None else handoff_from_record(row["handoff"])
    row["retirement"] = None if row["retirement"] is None else retirement_from_record(row["retirement"])
    row["dry_terminal"] = None if row["dry_terminal"] is None else dry_terminal_from_record(row["dry_terminal"])
    row["retirement_reasons"] = tuple(row["retirement_reasons"])
    value = ConsumerPortReceipt(**row)
    if canonical_json(value.to_record()) != canonical_json(record):
        raise LedgerContractError("LEDGER_CONSUMER_GROUP_CODEC_CONFLICT")
    return value


def require_admitted_action(state, action):
    reservation = next((item for item in state.reservations if item.root_id == action.root_id), None)
    if reservation is None:
        return  # Original pending L2 terms remain explicitly unadmitted facts.
    if reservation.status not in ("RESERVED", "TENTATIVE_DRY"):
        raise LedgerContractError("LEDGER_ADMITTED_ROOT_TERMINAL")
    original = reservation.admission.action
    if action.side == "BUY":
        if action != original:
            raise LedgerContractError("LEDGER_ADMITTED_BUY_TERMS_IMMUTABLE")
        return
    protection = next((item for item in state.protections if item.handoff.position_id == action.position_id), None)
    position = next((item for item in state.positions if item.position_id == action.position_id), None)
    if (protection is None or position is None or position.remaining_units <= 0
            or protection.handoff.obligation_id != action.obligation_id
            or (action.selected_exit_track, action.policy_digest, action.candidate_digest, action.mint, action.position_id) !=
               (original.selected_exit_track, original.policy_digest, original.candidate_digest, original.mint, original.position_id)):
        raise LedgerContractError("LEDGER_REDUCTION_ORIGINAL_PROTECTION_BINDING_REQUIRED")
