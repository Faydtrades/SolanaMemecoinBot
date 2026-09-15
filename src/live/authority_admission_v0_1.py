"""Finite Authority entry risk and original common-journal admission facts.

No quote builder, signer, message permission, mark-to-market or adaptive sizing.
Public policy caps are owner inputs. The full BUY cap already includes venue
fees; only outstanding costs are subtracted from actual native custody.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

from phase5.shadow_domain_v0_1 import content_fingerprint
from phase5.shadow_unsigned_plan_simulation_v0_1 import derive_associated_token_address
from phase5.shadow_venue_route_quote_v0_1 import TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID, WSOL_MINT
from .authority_controls_v0_1 import AuthorityReceipt, authority_receipt_from_record, require
from .authority_economics_v0_1 import fixed_size_reasons
from .ledger_actions_v0_1 import PendingAction, position_identity, utc_microseconds
from .ledger_domain_v0_1 import digest_value
from .ledger_ports_v0_1 import NativeReservationVector, AdmissionInput
from .public_rpc_v0_1 import TOKEN_PROGRAMS
from .wallet_evidence_v0_1 import ledger_account_evidence

VERSION = "live_authority_admission_v0.1"


@dataclass(frozen=True, slots=True)
class EntryRequest:
    root_id: str
    venue: str
    token_program: str
    track: str

    def __post_init__(self):
        digest_value(self.root_id)
        require(self.venue in ("PUMP", "PUMPSWAP") and self.token_program in TOKEN_PROGRAMS and self.track in ("FINAL-A", "FINAL-B", "SENS-C"),
                "AUTHORITY_ENTRY_ROUTE_PROFILE_INVALID")


@dataclass(frozen=True, slots=True)
class RootEncumbrance:
    root_id: str
    exposure_quote_cap_lamports: int
    principal_lamports: int
    network_lamports: int
    setup_lamports: int
    protective_lamports: int
    paid_failed_network_lamports: int
    paid_reduction_network_lamports: int
    failed_attempt_count: int
    remaining_failed_attempt_count: int | None
    failure_budget_remaining_lamports: int | None
    basis: str

    @property
    def outstanding_lamports(self):
        return self.principal_lamports+self.network_lamports+self.setup_lamports+self.protective_lamports


@dataclass(frozen=True, slots=True)
class EntryRiskDecision:
    disposition: str
    reasons: tuple[str, ...]
    native_lamports: int | None
    known_wsol_units: int
    observed_locked_lamports: int
    historical_network_paid_lamports: int
    historical_venue_sol_paid_lamports: int
    historical_setup_paid_lamports: int
    occupied_roots: tuple[str, ...]
    root_encumbrances: tuple[RootEncumbrance, ...]
    outstanding_native_lamports: int
    available_native_lamports: int | None
    existing_global_exposure_lamports: int
    existing_mint_exposure_lamports: int
    quote_cap_lamports: int | None
    venue_fee_included_bound_lamports: int | None
    required_native_lamports: int | None
    reservation: NativeReservationVector | None
    required_wallet_context_floor: int
    current_wallet_comparison_digest: str
    exposure_basis: str = field(init=False, default="FULL_ORIGINAL_QUOTE_CAP_UNTIL_LAWFUL_RETIREMENT")
    grants_message_permission: bool = field(init=False, default=False)

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


def root_encumbrances(custody, accepted, policies, applications, actions):
    rows = []
    for item in custody.reservations:
        if item.retired_sequence is not None:
            continue
        root, vector = item.root_id, item.admission.reservation
        original = accepted.get(root)
        policy = None if original is None else policies.get(original.eligibility.decision.policy_digest)
        actual = [entry for entry in applications.values()
                  if entry.decision.proposal is not None and entry.decision.disposition in
                  ("FINALIZED_SUCCESS_APPLIED", "FINALIZED_FAILURE_APPLIED") and entry.decision.proposal.root_id == root]
        acquired = any(position.root_id == root and position.acquired_units > 0 for position in custody.positions)
        if policy is None:
            rows.append(RootEncumbrance(root, vector.principal_lamports, vector.principal_lamports,
                vector.network_fee_lamports+vector.venue_fee_lamports, vector.account_setup_lamports,
                vector.protective_network_lamports+vector.protective_setup_lamports, 0, 0, 0, None, None, "EXTERNAL_TERMS_WORST_CASE"))
            continue
        # Paid amounts already changed native_lamports. The shared failure pool
        # shrinks, and principal/setup cease to encumber after
        # actual acquisition. Partial/full reductions keep the rest until lawful
        # retirement; there is no synthetic release based on a quote or timeout.
        failed = sum(p.decision.proposal.transaction_fee_lamports for p in actual
            if p.decision.disposition == "FINALIZED_FAILURE_APPLIED")
        reduction_paid = sum(p.decision.proposal.transaction_fee_lamports for p in actual if actions[p.decision.proposal.action_id].side == "SELL")
        failure_count = sum(p.decision.disposition == "FINALIZED_FAILURE_APPLIED" for p in actual)
        costs = policy.costs
        failure_left = max(0, costs.failed_attempt_network_budget_lamports-failed)
        protection_left = costs.protective_network_fee_lamports  # NEXT-exit headroom survives every partial reduction
        rows.append(RootEncumbrance(root, vector.principal_lamports, 0 if acquired else vector.principal_lamports,
            failure_left+(0 if acquired else costs.network_total_fee_lamports),
            0 if acquired else vector.account_setup_lamports,
            protection_left+vector.protective_setup_lamports, failed, reduction_paid, failure_count,
            max(0, costs.failed_attempt_count-failure_count), failure_left, "ACTUAL_APPLIED_COSTS_AND_UNSPENT_CAPS"))
    return tuple(sorted(rows, key=lambda row: row.root_id))


def assess_entry_risk(domain, custody, candidate, request, eligibility, policy, support, comparison,
                      *, accepted, policies, applications, pending_attempts, staged_action, spent_grants, actions):
    reasons = set(eligibility.decision.reasons)
    if eligibility.decision.disposition != "ELIGIBLE_CONTEXT_ONLY":
        reasons.add("ENTRY_ELIGIBILITY_NOT_PROVEN")
    if custody.quarantined:
        reasons.add("CURRENT_CUSTODY_QUARANTINED")
    if comparison.disposition != "MATCHED_AT_ORIGINAL_CUT" or comparison.target_custody_digest != custody.content_digest:
        reasons.add("CURRENT_COMPLETE_CUSTODY_COMPARISON_REQUIRED")
    floor = max(domain.minimum_context_slot, custody.latest_usable_wallet_context_slot,
        custody.latest_qualified_wallet_anchor_slot, 0 if custody.anchor is None else custody.anchor.slot)
    port = ledger_account_evidence(support.observation, expected_wallet=domain.wallet, expected_genesis=domain.genesis_hash,
        expected_profile_fingerprint=domain.expected_profile_fingerprint, required_min_context_slot=support.required_min_context_slot,
        now_utc=eligibility.original.clock.utc_upper_utc)
    if not port.account_facts_usable or support.required_min_context_slot < floor:
        reasons.add("FRESH_FINALIZED_WALLET_ACCOUNT_MINT_FACTS_REQUIRED")
    if (support.evaluated_at_utc != eligibility.original.clock.utc_upper_utc
            or utc_microseconds(support.observation.observed_at_utc) > utc_microseconds(eligibility.original.clock.utc_lower_utc)):
        reasons.add("WALLET_ORIGINAL_EVALUATION_KNOWLEDGE_CONFLICT")
    base = derive_associated_token_address(domain.wallet, candidate.mint, request.token_program)
    quote = derive_associated_token_address(domain.wallet, WSOL_MINT, TOKEN_PROGRAM_ID)
    expected = {(item.pubkey, item.mint, item.program) for item in support.observation.request.expected_accounts}
    if not {(base, candidate.mint, request.token_program), (quote, WSOL_MINT, TOKEN_PROGRAM_ID)} <= expected:
        reasons.add("EXPLICIT_ENTRY_BASE_AND_WSOL_ACCOUNT_COVERAGE_REQUIRED")
    explicit = {} if support.observation.explicit_read is None else dict(zip(
        support.observation.explicit_read.requested_keys, support.observation.explicit_read.accounts))
    mints = {(item.pubkey, item.program): item for item in port.assessment.mints}
    if (candidate.mint, request.token_program) not in mints or (WSOL_MINT, TOKEN_PROGRAM_ID) not in mints or candidate.mint == WSOL_MINT:
        reasons.add("EXACT_ENTRY_MINT_PROGRAM_SUPPORT_REQUIRED")
    if any(token.mint == WSOL_MINT for token in port.assessment.tokens) or explicit.get(quote) is not None:
        reasons.add("ENTRY_REQUIRES_ABSENT_WSOL_ACCOUNT")
    if request.token_program == TOKEN_2022_PROGRAM_ID and explicit.get(base) is None:
        from .ledger_settlement_v0_1 import fresh_token2022_mint_supported
        if not fresh_token2022_mint_supported(support.observation, candidate.mint):
            reasons.add("FRESH_TOKEN2022_ATA_CREATION_OUTSIDE_SETTLEMENT_PROFILE")
    if pending_attempts:
        reasons.add("COMPETING_POSSIBLY_LANDING_MUTATION_LANE")
    if any(record.request.root_id == request.root_id or record.mint == candidate.mint for record in accepted.values()):
        reasons.add("PERMANENT_ACCEPTED_ROOT_OR_WINNING_MINT_TOMBSTONE")
    # Include external admissions too; an externally staged action is not a
    # true Authority acceptance, but a real Ledger reservation still encumbers.
    if any(reservation.admission.action.mint == candidate.mint for reservation in custody.reservations):
        reasons.add("PERMANENT_LEDGER_WINNING_MINT_TOMBSTONE")
    if eligibility.decision.grant_id in spent_grants:
        reasons.add("ONE_TIME_ENTRY_GRANT_ALREADY_CONSUMED")
    roots = {item.root_id for item in custody.reservations if item.retired_sequence is None}
    roots.update(item.root_id for item in custody.positions if item.status != "RETIRED")
    encumbrances = root_encumbrances(custody, accepted, policies, applications, actions)
    outstanding = sum(row.outstanding_lamports for row in encumbrances)
    global_exposure = sum(row.exposure_quote_cap_lamports for row in encumbrances)
    mint_roots = {row.root_id for row in custody.reservations if row.admission.action.mint == candidate.mint}
    mint_exposure = sum(row.exposure_quote_cap_lamports for row in encumbrances if row.root_id in mint_roots)
    amount = venue_bound = required = vector = None
    if policy is None:
        reasons.add("EXPLICIT_POLICY_REQUIRED")
    else:
        reasons.update(fixed_size_reasons(policy.size))
        amount, costs = policy.size.fixed_quote_lamports, policy.costs
        venue_bound = costs.venue_fee_within_quote_cap_lamports
        if request.venue not in policy.allowed_venues:
            reasons.add("VENUE_NOT_ALLOWED")
        if policy.size.max_open_positions != 1:
            reasons.add("V1_REQUIRES_EXPLICIT_MAX_ONE_POSITION_POLICY")
        if len(roots) >= policy.size.max_open_positions:
            reasons.add("V1_OCCUPIED_POSITION_OR_UNRESOLVED_ADMISSION")
        if global_exposure+amount > policy.size.max_global_exposure_lamports:
            reasons.add("GLOBAL_ORIGINAL_CAP_EXPOSURE_EXCEEDED")
        if mint_exposure+amount > policy.size.max_mint_exposure_lamports:
            reasons.add("MINT_ORIGINAL_CAP_EXPOSURE_EXCEEDED")
        failure_required = costs.failed_attempt_count*max(costs.network_total_fee_lamports, costs.protective_network_fee_lamports)
        if (venue_bound > amount or costs.network_total_fee_lamports == 0 or costs.protective_network_fee_lamports == 0
                or costs.failed_attempt_network_budget_lamports < failure_required):
            reasons.add("EXPLICIT_COST_OR_PROTECTIVE_FAILURE_BUDGET_UNPROVEN")
        values = (amount, costs.network_total_fee_lamports+costs.failed_attempt_network_budget_lamports, 0,
                  costs.setup_outflow_lamports+costs.refundable_account_lock_lamports,
                  costs.protective_network_fee_lamports, costs.protective_setup_lamports+costs.protective_refundable_lock_lamports)
        required = sum(values)
        if any(value > (1 << 64)-1 for value in (*values, failure_required, required, outstanding, global_exposure+amount, mint_exposure+amount)):
            reasons.add("EXACT_U64_BUDGET_AGGREGATE_OVERFLOW")
        else:
            vector = NativeReservationVector(*values)
        if custody.native_lamports is None or custody.native_lamports-outstanding < required:
            reasons.add("INSUFFICIENT_UNENCUMBERED_NATIVE_SOL")
        if staged_action is not None:
            binding = eligibility.decision.binding
            if (staged_action.input_units, staged_action.token_program, staged_action.policy_ref, staged_action.policy_digest, staged_action.selected_exit_track,
                    staged_action.claimed_entry_deadline_us, staged_action.deadline_binding_digest) != (
                    amount, request.token_program, policy.policy_id, policy.content_digest, binding.track, binding.deadline_us, content_fingerprint(asdict(binding))):
                reasons.add("STAGED_IMMUTABLE_TERMS_CONFLICT")
    return EntryRiskDecision("ADMISSIBLE_STORAGE_ONLY" if not reasons else "DENIED", tuple(sorted(reasons)), custody.native_lamports,
        sum(item.units for item in custody.accounts if item.mint == WSOL_MINT), sum(item.observed_locked_lamports for item in custody.accounts),
        custody.network_fees_paid_lamports, custody.venue_fees_paid_sol_lamports, custody.external_setup_paid_lamports,
        tuple(sorted(roots)), encumbrances, outstanding, None if custody.native_lamports is None else custody.native_lamports-outstanding,
        global_exposure, mint_exposure, amount, venue_bound, required, vector, floor, content_fingerprint(asdict(comparison)))


def admission_terms(domain, candidate, request, eligibility, risk, policy, cut, command_id, staged_action=None):
    require(risk.disposition == "ADMISSIBLE_STORAGE_ONLY", "AUTHORITY_POSITIVE_CURRENT_RISK_REQUIRED")
    binding = eligibility.decision.binding
    decision_digest = content_fingerprint({"eligibility": eligibility.content_digest, "risk": risk.content_digest, "request": asdict(request)})
    action = staged_action or PendingAction(request.root_id, candidate.content_digest, "BUY", candidate.mint, request.token_program,
        position_identity(domain, request.root_id, candidate.mint), risk.quote_cap_lamports, command_id, decision_digest,
        policy.policy_id, policy.content_digest, binding.track, None, 1, binding.deadline_us, content_fingerprint(asdict(binding)))
    return AdmissionInput(cut, action, risk.reservation, command_id, decision_digest, eligibility.original.clock.utc_lower_utc)


@dataclass(frozen=True, slots=True)
class AuthorityAdmissionReceipt:
    sequence: int
    command_id: str
    request: EntryRequest
    mint: str
    eligibility: AuthorityReceipt
    wallet_support_digest: str
    comparison_digest: str
    risk: EntryRiskDecision
    admission_digest: str | None
    consumed_grant_id: str | None
    inbox_disposition_digest: str | None
    prior_custody_digest: str
    resulting_custody_digest: str
    common_previous_digest: str
    version: str = VERSION
    grants_message_permission: bool = field(init=False, default=False)
    historical_only: bool = field(init=False, default=True)

    @property
    def accepted(self):
        return self.admission_digest is not None

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


def admission_receipt_from_record(record, *, _source_decoder=None):
    value = dict(record)
    require(value.pop("grants_message_permission") is False and value.pop("historical_only") is True
            and value["version"] == VERSION, "AUTHORITY_ADMISSION_RECORD_PROFILE_INVALID")
    value["request"] = EntryRequest(**value["request"])
    value["eligibility"] = authority_receipt_from_record(value["eligibility"], _source_decoder=_source_decoder)
    risk = dict(value["risk"])
    require(risk.pop("grants_message_permission") is False and risk.pop("exposure_basis") == "FULL_ORIGINAL_QUOTE_CAP_UNTIL_LAWFUL_RETIREMENT",
            "AUTHORITY_RISK_RECORD_PROFILE_INVALID")
    for name in ("reasons", "occupied_roots"):
        risk[name] = tuple(risk[name])
    risk["root_encumbrances"] = tuple(RootEncumbrance(**item) for item in risk["root_encumbrances"])
    risk["reservation"] = None if risk["reservation"] is None else NativeReservationVector(**risk["reservation"])
    value["risk"] = EntryRiskDecision(**risk)
    return AuthorityAdmissionReceipt(**value)
