"""Fixed native quote economics, explicitly UNADMITTED until Authority A3.

No proposal table, quote builder, risk allocator or grant consumption. The quote
cap includes venue fees; it is not a modeled fill, token amount or reservation.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace

from phase5.shadow_domain_v0_1 import content_fingerprint
from .authority_controls_v0_1 import EntrySizeLimits, TrustedClockSample, require
from .ledger_actions_v0_1 import CandidateInboxInput, PendingAction, TERMINAL_INBOX, public_reference, position_identity
from .ledger_domain_v0_1 import LedgerContractError, digest_value
from .ledger_ports_v0_1 import ConsumerCut
from .public_rpc_v0_1 import u64, public_key

VERSION = "live_authority_unadmitted_economics_v0.1"


@dataclass(frozen=True, slots=True)
class UnadmittedEconomics:
    economic_domain_id: str
    mode: str
    root_id: str
    candidate_digest: str
    mint: str
    position_id: str
    selected_track: str
    original_signal_at_us: int
    original_deadline_us: int
    original_deadline_binding_digest: str
    native_quote_cap_lamports: int
    policy_id: str
    policy_digest: str
    eligibility_command_id: str
    eligibility_receipt_digest: str
    authority_state_digest: str
    cut: ConsumerCut
    staged_action: PendingAction | None
    disposition: str = field(init=False, default="UNADMITTED_ECONOMICS_PROPOSAL")
    asset_kind: str = field(init=False, default="NATIVE_SOL_FULL_QUOTE_CAP")
    quote_cap_includes_venue_fees: bool = field(init=False, default=True)
    admitted: bool = field(init=False, default=False)
    reserved: bool = field(init=False, default=False)
    grants_message_permission: bool = field(init=False, default=False)
    requires_current_A3_revalidation: bool = field(init=False, default=True)
    version: str = field(init=False, default=VERSION)

    def __post_init__(self):
        for value in (self.economic_domain_id, self.root_id, self.candidate_digest, self.position_id, self.policy_digest,
                      self.eligibility_receipt_digest, self.authority_state_digest, self.original_deadline_binding_digest):
            digest_value(value)
        public_key(self.mint)
        public_reference(self.policy_id)
        public_reference(self.eligibility_command_id)
        for value in (self.original_signal_at_us, self.original_deadline_us, self.native_quote_cap_lamports):
            u64(value)
        require(self.native_quote_cap_lamports > 0 and self.original_signal_at_us <= self.original_deadline_us
                and self.mode in ("LIVE", "DRY") and self.selected_track in ("FINAL-A", "FINAL-B", "SENS-C")
                and type(self.cut) is ConsumerCut and self.cut.economic_domain_id == self.economic_domain_id
                and (self.staged_action is None or type(self.staged_action) is PendingAction),
                "AUTHORITY_UNADMITTED_ECONOMICS_INVALID")

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class EconomicsResult:
    economic_domain_id: str
    root_id: str
    disposition: str
    reasons: tuple[str, ...]
    proposal: UnadmittedEconomics | None
    original_admitted_action: PendingAction | None
    admission_receipt_digest: str | None
    eligibility_receipt_digest: str | None
    inbox_disposition: str
    grants_message_permission: bool = field(init=False, default=False)
    version: str = field(init=False, default=VERSION)

    def __post_init__(self):
        digest_value(self.economic_domain_id)
        digest_value(self.root_id)
        require(self.disposition in ("UNADMITTED_ECONOMICS_PROPOSAL", "ORIGINAL_LEDGER_ADMITTED_TERMS", "TERMINAL_INBOX",
                "DENIED_UNPROVEN", "HISTORICAL_REQUEST_REQUIRES_REEVALUATION", "EXPIRED", "DENIED_FIXED_SIZE", "STAGED_TERMS_CONFLICT")
                and type(self.reasons) is tuple, "AUTHORITY_ECONOMICS_RESULT_SHAPE_INVALID")
        for reason in self.reasons:
            public_reference(reason)
        if self.proposal is not None:
            require(type(self.proposal) is UnadmittedEconomics and self.disposition == "UNADMITTED_ECONOMICS_PROPOSAL"
                    and (self.proposal.economic_domain_id, self.proposal.root_id) == (self.economic_domain_id, self.root_id)
                    and self.original_admitted_action is None, "AUTHORITY_PROPOSAL_RESULT_BINDING_CONFLICT")
        else:
            require(self.disposition != "UNADMITTED_ECONOMICS_PROPOSAL", "AUTHORITY_POSITIVE_PROPOSAL_MISSING")
        require((self.disposition == "ORIGINAL_LEDGER_ADMITTED_TERMS") == (self.original_admitted_action is not None),
                "AUTHORITY_HISTORICAL_ADMISSION_RESULT_CONFLICT")
        if self.original_admitted_action is not None:
            require(type(self.original_admitted_action) is PendingAction and self.original_admitted_action.root_id == self.root_id
                    and self.admission_receipt_digest is not None, "AUTHORITY_HISTORICAL_ACTION_BINDING_CONFLICT")
        for value in (self.admission_receipt_digest, self.eligibility_receipt_digest):
            if value is not None:
                digest_value(value)

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


def fixed_size_reasons(size):
    require(type(size) is EntrySizeLimits, "AUTHORITY_EXACT_FIXED_SIZE_LIMITS_REQUIRED")
    try:
        for value in asdict(size).values():
            u64(value)
    except ValueError:
        raise LedgerContractError("AUTHORITY_FIXED_SIZE_INTEGER_INVALID") from None
    reasons = []
    amount = size.fixed_quote_lamports
    if amount == 0:
        reasons.append("FIXED_SIZE_ZERO")
    if amount < size.minimum_quote_lamports:
        reasons.append("FIXED_SIZE_BELOW_MINIMUM")
    if amount > size.ceiling_quote_lamports:
        reasons.append("FIXED_SIZE_ABOVE_CEILING")
    if amount > size.max_trade_notional_lamports:
        reasons.append("FIXED_SIZE_ABOVE_TRADE_CAP")
    return tuple(reasons)


def _result(context, disposition, reasons=(), *, proposal=None, receipt=None):
    admitted = context["admitted_action"]
    return EconomicsResult(context["cut"].economic_domain_id, context["candidate"].trade_root(context["domain"]),
        disposition, tuple(reasons), proposal, admitted,
        None if context["admission_receipt"] is None else context["admission_receipt"].content_digest,
        None if receipt is None else receipt.content_digest, context["inbox_disposition"])


def _unchanged_context(context, receipt):
    return (context["cut"].revision == receipt.sequence
            and context["authority_state_digest"] == receipt.resulting_state_digest
            and context["policy"] is not None and context["policy"].content_digest == receipt.decision.policy_digest)


def propose_fixed_entry(repository, candidate, clock, source_store, *, request_id, fence):
    """Select a NEW original eligibility cut, then return exact unadmitted terms.

    Repeating a completed eligibility request is historical lookup, never fresh
    proposal authority. Use a new request ID and a new qualified clock input for
    reevaluation. A positive expiry proof can finish its inbox write after a crash.
    Every A3 admission still needs current common-cut, source, control and funding
    revalidation in its one atomic admission transaction.
    """
    require(type(candidate) is CandidateInboxInput and type(clock) is TrustedClockSample,
            "AUTHORITY_CANONICAL_CANDIDATE_AND_CLOCK_REQUIRED")
    public_reference(request_id)
    eligibility_key = "a2-eligibility:" + content_fingerprint({"request_id": request_id})
    disposition_key = "a2-disposition:" + content_fingerprint({"request_id": request_id})
    root = repository.receive_candidate(candidate, fence=fence)
    context = repository.authority_candidate_economics(root)
    if context["admitted_action"] is not None:
        return _result(context, "ORIGINAL_LEDGER_ADMITTED_TERMS")
    if context["inbox_disposition"] in TERMINAL_INBOX:
        return _result(context, "TERMINAL_INBOX", (context["inbox_disposition"],))
    if context["policy"] is None:
        return _result(context, "DENIED_UNPROVEN", ("POLICY_MISSING",))
    binding = context["entry_binding"]
    track = context["policy"].selected_track if binding is None else binding.track
    original = repository.authority_receipt(eligibility_key)
    if original is not None:
        require(original.kind == "AUTHORITY_ELIGIBILITY" and (original.original.root_id, original.original.candidate_digest,
            original.original.track, original.original.clock) == (root, candidate.content_digest, track, clock),
            "AUTHORITY_ECONOMICS_REQUEST_CONTENT_CONFLICT")
        if original.decision.disposition != "EXPIRED":
            return _result(context, "HISTORICAL_REQUEST_REQUIRES_REEVALUATION", ("NEW_CURRENT_ELIGIBILITY_REQUIRED",), receipt=original)
        receipt = original
    else:
        receipt = repository.evaluate_authority_entry(root, track, clock, source_store,
            command_id=eligibility_key, fence=context["fence"])
        context = repository.authority_candidate_economics(root)
    if context["admitted_action"] is not None:
        return _result(context, "ORIGINAL_LEDGER_ADMITTED_TERMS", receipt=receipt)
    if context["inbox_disposition"] in TERMINAL_INBOX:
        return _result(context, "TERMINAL_INBOX", (context["inbox_disposition"],), receipt=receipt)
    decision = receipt.decision
    if decision.disposition == "EXPIRED":
        # Lower UTC is qualified by original A1 continuity. Policy/source denial
        # cannot unprove that historical positive expiry; ambiguity never gets here.
        repository.record_nonacceptance(root, "EXPIRED", external_reference="AUTHORITY_A2_ORIGINAL_EXPIRY",
            external_record_digest=receipt.content_digest, recorded_at_utc=receipt.original.clock.utc_lower_utc,
            idempotency_key=disposition_key, fence=context["fence"])
        context = repository.authority_candidate_economics(root)
        return _result(context, "EXPIRED", decision.reasons, receipt=receipt)
    if decision.disposition != "ELIGIBLE_CONTEXT_ONLY":
        # Original A1 inputs/denial are already durable. No invented timestamp or
        # extra inbox state is required for an unknown/backward clock.
        return _result(context, "DENIED_UNPROVEN", decision.reasons, receipt=receipt)
    if not _unchanged_context(context, receipt):
        return _result(context, "DENIED_UNPROVEN", ("COMMON_ELIGIBILITY_CUT_CHANGED",), receipt=receipt)
    current_source = source_store.latest_record()
    if current_source != (receipt.original.source_sequence, receipt.original.source):
        return _result(context, "DENIED_UNPROVEN", ("CURRENT_SOURCE_SELECTION_CHANGED",), receipt=receipt)
    policy = context["policy"]
    reasons = fixed_size_reasons(policy.size)
    if reasons:
        result = replace(_result(context, "DENIED_FIXED_SIZE", reasons, receipt=receipt), inbox_disposition="DENIED_RETRYABLE")
        repository.record_nonacceptance(root, "DENIED_RETRYABLE", external_reference="AUTHORITY_A2_FIXED_SIZE_DENIAL",
            external_record_digest=result.content_digest, recorded_at_utc=receipt.original.clock.utc_lower_utc,
            idempotency_key=disposition_key, fence=context["fence"])
        return result
    staged = context["staged_action"]
    if staged is not None and (staged.input_units, staged.policy_ref, staged.policy_digest, staged.selected_exit_track,
            staged.claimed_entry_deadline_us, staged.deadline_binding_digest) != (policy.size.fixed_quote_lamports, policy.policy_id, policy.content_digest,
            decision.binding.track, decision.binding.deadline_us, content_fingerprint(asdict(decision.binding))):
        return _result(context, "STAGED_TERMS_CONFLICT", ("ORIGINAL_STAGED_TERMS_CANNOT_BE_RESIZED_OR_REBOUND",), receipt=receipt)
    proposal = UnadmittedEconomics(repository.domain.economic_domain_id, repository.domain.mode, root,
        candidate.content_digest, candidate.mint, position_identity(repository.domain, root, candidate.mint),
        decision.binding.track, decision.binding.signal_at_us, decision.binding.deadline_us, content_fingerprint(asdict(decision.binding)),
        policy.size.fixed_quote_lamports, policy.policy_id, policy.content_digest, receipt.command_id, receipt.content_digest,
        context["authority_state_digest"], context["cut"], staged)
    return _result(context, "UNADMITTED_ECONOMICS_PROPOSAL", proposal=proposal, receipt=receipt)
