"""Original external message evidence, context only; no mutation permission.

The accepted Phase-5 pure decoders/factories are replayed from public bytes.
No provider, message materializer, signer, custody model or Authority grant is
implemented here. A4b must select the approved profile and current common cut.
"""
from __future__ import annotations

import base64
import hashlib
from dataclasses import asdict, dataclass, field

from solders.message import Message
from solders.compute_budget import ID as COMPUTE_BUDGET_ID
from phase5 import shadow_unsigned_plan_simulation_v0_1 as plans
from phase5 import shadow_venue_route_quote_v0_1 as venue
from phase5.shadow_domain_v0_1 import ExecutionIntentV01, IntentRole, IntentSide, canonical_json, content_fingerprint
from .authority_controls_v0_1 import AuthorityPolicy, AuthorityPolicyV02, OperatorProvenance, TrustedClockSample, require, utc_from_us
from .authority_admission_v0_1 import AuthorityAdmissionReceipt
from .ledger_actions_v0_1 import CandidateInboxInput, PendingAction, decode_message, public_reference, utc_microseconds
from .ledger_domain_v0_1 import LedgerDomain, LedgerContractError, digest_value
from .ledger_ports_v0_1 import ConsumerCut, ProtectionFact, ConsumerPortReceipt
from .ledger_custody_v0_1 import PositionView
from .ledger_settlement_v0_1 import WalletSupportInput, _roles, _validate_outer_shapes, _validate_failed_inner
from .ledger_evidence_codec_v0_1 import strict_json_object
from .public_rpc_v0_1 import PublicAccount, public_key, block_hash, u64
from .transaction_evidence_v0_1 import InstructionFact, _b58data
from .wallet_evidence_v0_1 import ledger_account_evidence, _token_shape

VERSION = "live_authority_message_evidence_v0.1"
COMPUTE_PROGRAM = str(COMPUTE_BUDGET_ID)


@dataclass(frozen=True, slots=True)
class MessageValidationProfile:
    profile_id: str
    economic_domain_id: str
    policy_digest: str
    rpc_profile_fingerprint: str
    approval: OperatorProvenance
    maximum_venue_age_us: int
    maximum_wallet_age_us: int
    maximum_lease_age_us: int
    maximum_validity_age_us: int
    maximum_simulation_age_us: int
    maximum_fee_age_us: int
    maximum_setup_age_us: int
    maximum_context_span: int
    maximum_compute_units: int
    maximum_compute_unit_price_micro_lamports: int
    version: str = field(init=False, default=VERSION)

    def __post_init__(self):
        public_reference(self.profile_id)
        for value in (self.economic_domain_id, self.policy_digest, self.rpc_profile_fingerprint):
            digest_value(value)
        require(type(self.approval) is OperatorProvenance, "MESSAGE_PROFILE_EXTERNAL_APPROVAL_REQUIRED")
        for name in self.__dataclass_fields__:
            if name.startswith("maximum_"):
                u64(getattr(self, name))
        require(self.maximum_compute_units <= (1<<32)-1, "MESSAGE_COMPUTE_PROFILE_UNSUPPORTED")

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class PublicReadCut:
    genesis_hash: str
    profile_fingerprint: str
    context_slot: int
    observed_at_us: int
    commitment: str

    def __post_init__(self):
        block_hash(self.genesis_hash)
        digest_value(self.profile_fingerprint)
        u64(self.context_slot)
        u64(self.observed_at_us)
        require(self.commitment in ("confirmed", "finalized"), "MESSAGE_PUBLIC_READ_COMMITMENT_UNSUPPORTED")


@dataclass(frozen=True, slots=True)
class OriginalAccountBatch:
    """Exact public getMultipleAccounts request order, response and read cut."""
    cut: PublicReadCut
    requested_keys: tuple[str, ...]
    accounts: tuple[PublicAccount | None, ...]

    def __post_init__(self):
        require(type(self.cut) is PublicReadCut and type(self.requested_keys) is tuple and type(self.accounts) is tuple
            and 1 <= len(self.requested_keys) <= 100 and len(set(self.requested_keys)) == len(self.requested_keys)
            and len(self.requested_keys) == len(self.accounts), "MESSAGE_ORIGINAL_ACCOUNT_BATCH_INVALID")
        for key, value in zip(self.requested_keys, self.accounts):
            public_key(key)
            require(value is None or type(value) is PublicAccount and value.account.pubkey == key,
                    "MESSAGE_ORIGINAL_ACCOUNT_IDENTITY_CONFLICT")


@dataclass(frozen=True, slots=True)
class OriginalVenueRead:
    primary: OriginalAccountBatch
    dependent: OriginalAccountBatch
    verification: OriginalAccountBatch

    def __post_init__(self):
        require(all(type(value) is OriginalAccountBatch for value in (self.primary, self.dependent, self.verification)),
                "MESSAGE_ORIGINAL_VENUE_READ_REQUIRED")


@dataclass(frozen=True, slots=True)
class FeeForMessageRead:
    """Consumer contract for one original read; this module supplies no RPC.

    NULL/MISSING/PRUNED/ERROR preserve unavailability. A positive value is the
    total getFeeForMessage result, including priority, not a second fee pool.
    """
    cut: PublicReadCut
    request_message_base64: str
    required_min_context_slot: int
    outcome: str
    fee_lamports: int | None

    def __post_init__(self):
        require(type(self.cut) is PublicReadCut and self.cut.commitment == "confirmed", "MESSAGE_FEE_READ_CUT_REQUIRED")
        u64(self.required_min_context_slot)
        require(type(self.request_message_base64) is str and len(self.request_message_base64) <= 1644,
                "MESSAGE_FEE_REQUEST_INVALID")
        raw = base64.b64decode(self.request_message_base64, validate=True)
        require(base64.b64encode(raw).decode("ascii") == self.request_message_base64, "MESSAGE_FEE_REQUEST_INVALID")
        decode_message(raw.hex())
        require(self.outcome in ("OBSERVED", "NULL", "MISSING", "PRUNED", "ERROR")
                and (self.fee_lamports is not None) == (self.outcome == "OBSERVED"), "MESSAGE_FEE_RESPONSE_SHAPE_INVALID")
        if self.fee_lamports is not None:
            u64(self.fee_lamports)


@dataclass(frozen=True, slots=True)
class SimulationProvenance:
    genesis_hash: str
    profile_fingerprint: str
    producer_reference: str
    original_read_record_digest: str
    run_digest: str
    started_at_us: int
    observed_at_us: int

    def __post_init__(self):
        block_hash(self.genesis_hash)
        for value in (self.profile_fingerprint, self.original_read_record_digest, self.run_digest):
            digest_value(value)
        public_reference(self.producer_reference)
        u64(self.started_at_us)
        u64(self.observed_at_us)
        require(self.started_at_us <= self.observed_at_us, "MESSAGE_SIMULATION_PROVENANCE_TIME_INVALID")


@dataclass(frozen=True, slots=True)
class ExternalMessageEvidence:
    intent: ExecutionIntentV01
    venue_read: OriginalVenueRead
    wallet: WalletSupportInput
    quote_policy: venue.QuotePolicyV01
    plan_policy: plans.TransactionPlanPolicyV01
    supplied_state_json: str
    supplied_route_json: str
    supplied_quote_json: str
    supplied_plan_json: str
    simulation: plans.SimulationRunEvidenceV01
    simulation_provenance: SimulationProvenance
    fee: FeeForMessageRead | None
    setup_accounts: OriginalAccountBatch | None
    version: str = field(init=False, default=VERSION)

    def __post_init__(self):
        require(type(self.intent) is ExecutionIntentV01 and type(self.venue_read) is OriginalVenueRead
            and type(self.wallet) is WalletSupportInput and type(self.quote_policy) is venue.QuotePolicyV01
            and type(self.plan_policy) is plans.TransactionPlanPolicyV01 and type(self.simulation) is plans.SimulationRunEvidenceV01
            and type(self.simulation_provenance) is SimulationProvenance
            and (self.fee is None or type(self.fee) is FeeForMessageRead)
            and (self.setup_accounts is None or type(self.setup_accounts) is OriginalAccountBatch), "MESSAGE_FINITE_ORIGINAL_INPUT_REQUIRED")
        for value in (self.supplied_state_json, self.supplied_route_json, self.supplied_quote_json, self.supplied_plan_json):
            require(canonical_json(strict_json_object(value)) == value, "MESSAGE_CANONICAL_PUBLIC_PAYLOAD_REQUIRED")


@dataclass(frozen=True, slots=True)
class MessageContext:
    domain: LedgerDomain
    candidate: CandidateInboxInput
    acceptance: AuthorityAdmissionReceipt
    ledger_admission: ConsumerPortReceipt
    admitted_buy: PendingAction
    action: PendingAction
    original_policy: AuthorityPolicy | AuthorityPolicyV02
    entry_policy: AuthorityPolicy | AuthorityPolicyV02 | None
    cut: ConsumerCut
    required_wallet_context_slot: int
    position: PositionView | None
    protection: ProtectionFact | None

    def __post_init__(self):
        require(type(self.domain) is LedgerDomain and type(self.candidate) is CandidateInboxInput
            and type(self.acceptance) is AuthorityAdmissionReceipt and type(self.ledger_admission) is ConsumerPortReceipt
            and type(self.admitted_buy) is PendingAction and type(self.action) is PendingAction
            and type(self.original_policy) in (AuthorityPolicy,AuthorityPolicyV02)
            and (self.entry_policy is None or type(self.entry_policy) in (AuthorityPolicy,AuthorityPolicyV02))
            and type(self.cut) is ConsumerCut and (self.position is None or type(self.position) is PositionView)
            and (self.protection is None or type(self.protection) is ProtectionFact), "MESSAGE_FINITE_STORED_CONTEXT_REQUIRED")
        u64(self.required_wallet_context_slot)
        if self.position is not None:
            for value in (self.position.acquired_units,self.position.sold_units,self.position.remaining_units):
                u64(value)
            require(self.position.acquired_units == self.position.sold_units+self.position.remaining_units,
                    "MESSAGE_ACTUAL_POSITION_QUANTITY_CONFLICT")


@dataclass(frozen=True, slots=True)
class MessageValidationInput:
    context: MessageContext
    profile: MessageValidationProfile
    clock: TrustedClockSample
    evidence: ExternalMessageEvidence

    def __post_init__(self):
        require(type(self.context) is MessageContext and type(self.profile) is MessageValidationProfile
            and type(self.clock) is TrustedClockSample and type(self.evidence) is ExternalMessageEvidence,
            "MESSAGE_FINITE_VALIDATION_INPUT_REQUIRED")

    @property
    def content_digest(self):
        from .authority_message_codec_v0_1 import encode_validation_input
        return hashlib.sha256(encode_validation_input(self).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class SimulatedCostFacts:
    network_total_fee_lamports: int
    priority_fee_within_total_lamports: int
    quote_cap_including_venue_fees: int
    venue_quote_asset: str
    quoted_venue_fee_bound_units: int
    gross_wsol_quote_outflow_units: int
    gross_venue_native_outflow_lamports: int
    program_or_external_setup_lamports: int
    gross_wallet_account_funding_lamports: int
    gross_upfront_native_lamports: int
    created_accounts: tuple[tuple[str, int, int, str], ...]
    simulated_refunds_not_available_upfront: bool = field(init=False, default=True)
    actual_economic_effect: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class MessageValidationResult:
    input_digest: str
    disposition: str
    reasons: tuple[str, ...]
    action_id: str
    profile_digest: str
    message_sha256: str
    costs: SimulatedCostFacts | None
    context_only: bool = field(init=False, default=True)
    grants_message_permission: bool = field(init=False, default=False)
    may_sign: bool = field(init=False, default=False)
    may_send: bool = field(init=False, default=False)

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class SimulationInstructionView:
    """Only decoded message/CPI shape. No transaction outcome or balance truth."""
    header: tuple[int, int, int]
    static_accounts: tuple[str, ...]
    outer_instructions: tuple[InstructionFact, ...]
    inner_instructions: tuple[InstructionFact, ...]
    inner_recorded_outer_indexes: tuple[int, ...]
    loaded_writable: tuple[str, ...] = field(init=False, default=())

    @property
    def account_keys(self):
        return self.static_accounts


class _Unknown(ValueError):
    pass


def _known(condition, reason):
    if not condition:
        raise _Unknown(reason)


def capture_message_context(repository, action_id):
    """Read one verified common cut; no controls are consumed or changed."""
    with repository._trusted_read():
        action = repository.action(action_id)
        require(action is not None, "MESSAGE_STORED_ACTION_REQUIRED")
        entry = repository.authority_candidate_economics(action.root_id)
        acceptance = repository.authority_acceptance(action.root_id)
        snapshot = repository.consumer_snapshot()
        buy = entry["admitted_action"]
        require(acceptance is not None and acceptance.accepted and buy is not None, "MESSAGE_ACTUAL_AUTHORITY_ACCEPTANCE_REQUIRED")
        position = next((item for item in snapshot["positions"] if item.position_id == action.position_id), None)
        protection = next((item for item in snapshot["protections"] if item.handoff.position_id == action.position_id), None)
        return MessageContext(repository.domain, entry["candidate"], acceptance, entry["admission_receipt"], buy, action,
            repository.authority_policy(buy.policy_ref), entry["policy"], snapshot["consumer_cut"],
            snapshot["required_wallet_context_slot"], position, protection)


def validate_message_evidence(original: MessageValidationInput):
    """Replay at the original clock bounds. Result is never current permission."""
    from .authority_message_codec_v0_1 import decode_validation_input, encode_validation_input
    try:
        original = decode_validation_input(encode_validation_input(original))
    except Exception:
        raise LedgerContractError("MESSAGE_ORIGINAL_INPUT_CODEC_INVALID") from None
    evidence, context = original.evidence, original.context
    digest = original.content_digest
    try:
        costs = _validate(original)
        disposition, reasons = "SUPPORTED_CONTEXT_ONLY", ()
    except _Unknown as exc:
        disposition, reasons, costs = "UNKNOWN", (str(exc),), None
    except Exception:
        # Public raw metadata/logs/provider text never become error records.
        disposition, reasons, costs = "DENIED_CONTEXT", ("MESSAGE_LINEAGE_OR_SUPPORTED_SHAPE_CONFLICT",), None
    return MessageValidationResult(digest, disposition, reasons, context.action.action_id, original.profile.content_digest,
        evidence.simulation.envelopes[-1].message_sha256, costs)


def _fresh(at, maximum, original, reason):
    u64(at)
    lower, upper = utc_microseconds(original.clock.utc_lower_utc), utc_microseconds(original.clock.utc_upper_utc)
    _known(at <= lower <= upper and upper-at <= maximum, reason)


def _read_cut(cut, original, age, reason):
    _known(cut.genesis_hash == original.context.domain.genesis_hash
        and cut.profile_fingerprint == original.profile.rpc_profile_fingerprint, "MESSAGE_PUBLIC_READ_DOMAIN_PROFILE_UNKNOWN")
    _fresh(cut.observed_at_us, age, original, reason)


def _validate(original):
    c, p, e, clock = original.context, original.profile, original.evidence, original.clock
    policy, action, item, intent = c.original_policy, c.action, c.candidate, e.intent
    require(c.domain.mode == "LIVE" and c.cut.economic_domain_id == c.domain.economic_domain_id
        == p.economic_domain_id == policy.economic_domain_id,
        "MESSAGE_PROFILE_POLICY_DOMAIN_CONFLICT")
    _known(p.policy_digest == policy.content_digest, "MESSAGE_PROFILE_POLICY_BINDING_UNKNOWN")
    _known(p.rpc_profile_fingerprint == c.domain.expected_profile_fingerprint == policy.wallet_profile_fingerprint,
           "MESSAGE_PROFILE_PROVIDER_UNKNOWN")
    _known(clock.status == "QUALIFIED" and (clock.provider_id, clock.provider_fingerprint) ==
        (policy.clock.provider_id, policy.clock.provider_fingerprint), "MESSAGE_TRUSTED_CLOCK_UNKNOWN")
    lower, upper = utc_microseconds(clock.utc_lower_utc), utc_microseconds(clock.utc_upper_utc)
    _known(lower <= upper and upper-lower <= policy.clock.maximum_uncertainty_us, "MESSAGE_CLOCK_BOUNDS_UNKNOWN")
    require(utc_microseconds(p.approval.recorded_at_utc) <= lower, "MESSAGE_PROFILE_APPROVAL_FROM_FUTURE")
    require(c.acceptance.accepted and c.acceptance.admission_digest == c.ledger_admission.content_digest
        and c.ledger_admission.kind == "ADMISSION" and c.ledger_admission.admission.action == c.admitted_buy and c.acceptance.risk.disposition == "ADMISSIBLE_STORAGE_ONLY"
        and c.acceptance.request.root_id == action.root_id == c.admitted_buy.root_id
        == item.trade_root(c.domain) and action.candidate_digest == item.content_digest
        and c.acceptance.mint == action.mint == item.mint and c.admitted_buy.side == "BUY"
        and c.admitted_buy.policy_digest == policy.content_digest
        and action.policy_ref == policy.policy_id and action.policy_digest == policy.content_digest
        and c.acceptance.eligibility.decision.policy_digest == policy.content_digest
        and c.acceptance.risk.quote_cap_lamports == c.admitted_buy.input_units, "MESSAGE_ORIGINAL_ADMISSION_CONFLICT")
    require((intent.candidate_signal_id, intent.candidate_run_id, intent.strategy_evaluation_id, intent.strategy_version,
        intent.parameter_set_id, intent.source_run_id, intent.source_event_key, intent.source_ingest_seq, intent.mint,
        intent.side.value, intent.input_amount_base_units) == (item.candidate_signal_id, item.candidate_run_id,
        item.strategy_evaluation_id, item.strategy_version, item.parameter_set_id, item.producer_run_id,
        item.source_event_key, item.signal_ingest_seq, item.mint, action.side, action.input_units)
        and item.generated_at_us <= intent.decision_at_us <= lower, "MESSAGE_ORIGINAL_INTENT_CONFLICT")
    require(action.selected_exit_track == c.admitted_buy.selected_exit_track == c.acceptance.request.track
        and c.admitted_buy.claimed_entry_deadline_us == c.acceptance.eligibility.decision.binding.deadline_us
        and c.admitted_buy.deadline_binding_digest == content_fingerprint(asdict(c.acceptance.eligibility.decision.binding)),
        "MESSAGE_ORIGINAL_TRACK_DEADLINE_CONFLICT")
    if action.side == "BUY":
        require(action == c.admitted_buy and c.entry_policy is not None and c.entry_policy.content_digest == policy.content_digest
            and intent == _entry_intent(c), "MESSAGE_ENTRY_ORIGINAL_TERMS_CONFLICT")
        _known(upper <= action.claimed_entry_deadline_us, "MESSAGE_ENTRY_ORIGINAL_WINDOW_UNRESOLVED_OR_ELAPSED")
    else:
        position, protection = c.position, c.protection
        require(position is not None and protection is not None and intent.role is IntentRole.EXIT
            and intent.input_asset == "MEME_BASE_UNITS" and position.root_id == action.root_id
            and position.mint == action.mint and position.token_program == action.token_program
            and position.position_id == action.position_id == intent.position_id == protection.handoff.position_id
            and action.obligation_id == protection.handoff.obligation_id and action.selected_exit_track == intent.exit_track_id
            and protection.handoff.candidate == item and protection.handoff.selected_policy_digest == policy.content_digest
            and intent.exit_decision_id == action.external_decision_ref
            and intent.exit_lifecycle_id == protection.handoff.binding_id,
            "MESSAGE_ORIGINAL_REDUCTION_SCOPE_CONFLICT")
        # Original parent entry intent is reconstructed from the same accepted
        # immutable root terms, not today's ENTRY policy or a PAPER identifier.
        require(intent.parent_entry_intent_id == _entry_intent(c).intent_id, "MESSAGE_REDUCTION_PARENT_INTENT_CONFLICT")
    state, route, quote, plan = _rebuild(original)
    require(quote.price_impact_ppm <= policy.costs.maximum_impact_bps*100
        and e.quote_policy.slippage_bps <= policy.costs.maximum_slippage_bps
        and quote.fees.total_fee <= policy.costs.venue_fee_within_quote_cap_lamports, "MESSAGE_QUOTE_LIMIT_CONFLICT")
    message, view, units, price = _exact_simulation(original, plan)
    return _costs(original, plan, quote, view, units, price)


def _entry_intent(c):
    item = c.candidate
    return ExecutionIntentV01(item.candidate_signal_id, item.candidate_run_id, item.strategy_evaluation_id,
        item.strategy_version, item.parameter_set_id, item.producer_run_id, item.source_event_key, item.signal_ingest_seq,
        utc_microseconds(c.acceptance.eligibility.original.clock.utc_upper_utc), item.mint, IntentRole.ENTRY, IntentSide.BUY,
        "SOL_LAMPORTS", c.admitted_buy.input_units)


def _rebuild(original):
    c, p, e = original.context, original.profile, original.evidence
    first, dependent, last = e.venue_read.primary, e.venue_read.dependent, e.venue_read.verification
    for batch in (first, dependent, last):
        _read_cut(batch.cut, original, p.maximum_venue_age_us, "MESSAGE_VENUE_READ_STALE_OR_FUTURE")
    primary_keys = (venue.derive_bonding_curve_pda(c.action.mint), venue.derive_pumpswap_pool_pda(c.action.mint))
    require(first.requested_keys == last.requested_keys == primary_keys
        and first.cut.context_slot <= dependent.cut.context_slot <= last.cut.context_slot
        and first.cut.observed_at_us <= dependent.cut.observed_at_us <= last.cut.observed_at_us
        and last.cut.context_slot-first.cut.context_slot <= p.maximum_context_span
        and first.cut.commitment == dependent.cut.commitment == last.cut.commitment,
        "MESSAGE_COHERENT_ROUTE_READ_CONFLICT")
    # Accepted coherent_dependent_read compares raw primary identity/data across
    # its surrounding reads; retain lamports as well for the setup consumer.
    require(first.accounts == last.accounts, "MESSAGE_PRIMARY_BYTES_CHANGED_DURING_READ")
    by_key = dict(zip(first.requested_keys, first.accounts))
    require(not set(first.requested_keys) & set(dependent.requested_keys), "MESSAGE_DUPLICATE_READ_ROLE")
    by_key.update(zip(dependent.requested_keys, dependent.accounts))
    slots = {key: first.cut.context_slot for key in first.requested_keys}
    slots.update((key, dependent.cut.context_slot) for key in dependent.requested_keys)
    def raw(key):
        _known(key in by_key and by_key[key] is not None and not by_key[key].executable, "MESSAGE_REQUIRED_VENUE_BYTES_MISSING")
        return by_key[key].account
    mint = c.action.mint
    curve_keys = (primary_keys[0], mint, venue.derive_pump_global_pda(), venue.derive_pump_fee_config_pda())
    curve_slots = tuple(slots[key] for key in curve_keys)
    curve = venue.build_pump_state(e.intent, *(raw(key) for key in curve_keys), slot_min=min(curve_slots),
        slot_max=max(curve_slots), observed_at_us=last.cut.observed_at_us, account_slots=curve_slots)
    pool = None
    needed = set(curve_keys[1:])
    if first.accounts[1] is not None:
        decoded = venue.decode_pumpswap_pool(raw(primary_keys[1]), mint)
        pool_keys = (primary_keys[1], decoded.pool_base_token_account, decoded.pool_quote_token_account,
                    mint, venue.derive_pumpswap_global_pda(), venue.derive_pumpswap_fee_config_pda())
        pool_slots = tuple(slots[key] for key in pool_keys)
        pool = venue.build_pumpswap_state(e.intent, *(raw(key) for key in pool_keys), slot_min=min(pool_slots),
            slot_max=max(pool_slots), observed_at_us=last.cut.observed_at_us, account_slots=pool_slots)
        needed.update(pool_keys[1:])
    require(set(dependent.requested_keys) == needed, "MESSAGE_EXACT_DEPENDENT_ACCOUNT_SET_REQUIRED")
    route = venue.decide_route(e.intent, curve, pool)
    state = curve if route.selected_state_id == curve.state_id else pool
    _known(state is not None and route.selected_state_id == state.state_id, "MESSAGE_SUPPORTED_ROUTE_UNAVAILABLE")
    actual_venue = "PUMP" if isinstance(state, venue.PumpBondingCurveStateV01) else "PUMPSWAP"
    # Admission's venue was an original read hint, not immutable economics.
    # The same root/amount can follow an evidenced curve-to-pool migration.
    require(state.base_token_program == c.action.token_program and actual_venue in c.original_policy.allowed_venues,
            "MESSAGE_APPROVED_VENUE_PROGRAM_CONFLICT")
    if pool is not None:
        _known(not pool.decoded.is_cashback_coin, "MESSAGE_CASHBACK_LIFECYCLE_UNSUPPORTED")
    wallet = e.wallet.observation
    _known(utc_microseconds(e.wallet.evaluated_at_utc) <= utc_microseconds(original.clock.utc_lower_utc),
           "MESSAGE_ORIGINAL_WALLET_EVALUATION_FROM_FUTURE")
    _fresh(utc_microseconds(wallet.observed_at_utc), p.maximum_wallet_age_us, original, "MESSAGE_WALLET_READ_STALE_OR_FUTURE")
    port = ledger_account_evidence(wallet, expected_wallet=c.domain.wallet, expected_genesis=c.domain.genesis_hash,
        expected_profile_fingerprint=c.domain.expected_profile_fingerprint,
        required_min_context_slot=max(e.wallet.required_min_context_slot, c.required_wallet_context_slot, last.cut.context_slot),
        now_utc=original.clock.utc_upper_utc)
    _known(port.account_facts_usable, "MESSAGE_ORIGINAL_WALLET_SUPPORT_UNAVAILABLE")
    mint_reads = {key:value for read in wallet.mint_reads for key,value in zip(read.requested_keys,read.accounts)}
    _known(mint in mint_reads and mint_reads[mint] is not None and mint_reads[mint].account == raw(mint),
           "MESSAGE_ORIGINAL_MINT_READ_CONTRADICTION")
    base = plans.derive_associated_token_address(c.domain.wallet, mint, c.action.token_program)
    quote_account = plans.derive_associated_token_address(c.domain.wallet, venue.WSOL_MINT, venue.TOKEN_PROGRAM_ID)
    explicit = dict(zip(wallet.explicit_read.requested_keys, wallet.explicit_read.accounts))
    _known({base, quote_account, c.domain.wallet} <= explicit.keys(), "MESSAGE_EXPLICIT_ACTOR_ACCOUNT_COVERAGE_MISSING")
    _known(c.action.side == "SELL" or explicit[quote_account] is None, "MESSAGE_PREEXISTING_WSOL_ENTRY_PROFILE_UNSUPPORTED")
    _known(not (explicit[base] is None and c.action.token_program == venue.TOKEN_2022_PROGRAM_ID),
           "MESSAGE_FRESH_TOKEN2022_ATA_LIFECYCLE_UNSUPPORTED")
    quote = venue.create_executable_quote(e.intent, state, route, e.quote_policy)
    actor = plans.PublicShadowActorV01(c.domain.wallet)
    accounts = plans.build_actor_account_snapshot(state, actor,
        base_account=None if explicit[base] is None else explicit[base].account,
        quote_account=None if explicit[quote_account] is None else explicit[quote_account].account,
        context_slot=port.assessment.context_slot, observed_at_us=utc_microseconds(wallet.observed_at_utc))
    global_key = venue.derive_pump_global_pda() if actual_venue == "PUMP" else venue.derive_pumpswap_global_pda()
    recipients = plans.decode_verified_recipient_evidence(state, raw(global_key))
    plan = plans.build_unsigned_transaction_plan(e.intent, state, route, quote, actor, e.plan_policy, recipients, accounts)
    require(tuple(canonical_json(value.payload()) for value in (state, route, quote, plan)) ==
        (e.supplied_state_json, e.supplied_route_json, e.supplied_quote_json, e.supplied_plan_json),
        "MESSAGE_REBUILT_PUBLIC_LINEAGE_CONFLICT")
    return state, route, quote, plan


def _exact_simulation(original, plan):
    e, p, c = original.evidence, original.profile, original.context
    run = e.simulation
    provenance = e.simulation_provenance
    from .authority_message_codec_v0_1 import simulation_run_digest
    _known(provenance.genesis_hash == c.domain.genesis_hash and provenance.profile_fingerprint == p.rpc_profile_fingerprint
        and provenance.run_digest == simulation_run_digest(run), "MESSAGE_ORIGINAL_SIMULATION_DOMAIN_PROFILE_UNKNOWN")
    require(provenance.started_at_us <= run.leases[0].observed_at_us
        and run.results[-1].observed_at_us <= provenance.observed_at_us, "MESSAGE_SIMULATION_ORIGINAL_CAPTURE_CONFLICT")
    _fresh(provenance.observed_at_us, p.maximum_simulation_age_us, original, "MESSAGE_SIMULATION_CAPTURE_STALE_OR_FUTURE")
    # Retain the accepted bounded history if the producer performed its one
    # read-only refresh. This does not grant a LIVE replacement attempt.
    for lease, validity, envelope, attempt, result in zip(run.leases, run.validity, run.envelopes, run.attempts, run.results):
        require(lease.plan_id == envelope.plan_id == attempt.plan_id == result.plan_id == plan.plan_id
            and envelope.plan_fingerprint == plan.fingerprint and envelope.lease_fingerprint == lease.fingerprint,
            "MESSAGE_SIMULATION_PLAN_LINEAGE_CONFLICT")
        require(lease.context_slot >= plan.prerequisite_slot and validity.required_min_context_slot ==
            max(plan.prerequisite_slot, lease.context_slot), "MESSAGE_ORIGINAL_LEASE_FLOOR_CONFLICT")
        require(lease.observed_at_us <= validity.observed_at_us <= attempt.observed_at_us <= result.observed_at_us,
                "MESSAGE_SIMULATION_TIME_ORDER_CONFLICT")
        require(lease.observed_at_us >= max(e.venue_read.verification.cut.observed_at_us,
            utc_microseconds(e.wallet.observation.observed_at_utc)), "MESSAGE_LEASE_PRECEDES_ORIGINAL_PLAN_READS")
    lease, validity, envelope, attempt, result = (values[-1] for values in
        (run.leases, run.validity, run.envelopes, run.attempts, run.results))
    for at, maximum, reason in ((lease.observed_at_us, p.maximum_lease_age_us, "MESSAGE_LEASE_STALE_OR_FUTURE"),
        (validity.observed_at_us, p.maximum_validity_age_us, "MESSAGE_VALIDITY_STALE_OR_FUTURE"),
        (result.observed_at_us, p.maximum_simulation_age_us, "MESSAGE_SIMULATION_STALE_OR_FUTURE")):
        _fresh(at, maximum, original, reason)
    _known(validity.rpc_valid and validity.block_height <= lease.last_valid_block_height, "MESSAGE_RECENT_BLOCKHASH_UNAVAILABLE_OR_EXPIRED")
    config = plans.SimulationRequestConfigV01(min_context_slot=max(plan.prerequisite_slot, lease.context_slot))
    require(envelope.request_config_json == canonical_json(config.payload()) and config.fingerprint == attempt.request_config_fingerprint,
            "MESSAGE_EXACT_SIMULATION_CONFIG_CONFLICT")
    _known(result.outcome is plans.SimulationOutcome.SIMULATION_SUCCESS and result.error_json == "null"
        and result.reason_code == "SIMULATION_RETURNED_SUCCESS", "MESSAGE_EXACT_SIMULATION_NOT_SUPPORTED_SUCCESS")
    _known(result.context_slot is not None and result.context_slot >= config.min_context_slot
        and result.units_consumed is not None and result.loaded_accounts_data_size is not None,
        "MESSAGE_SIMULATION_CONTEXT_OR_METRICS_MISSING")
    _known(max(result.context_slot,validity.validity_context_slot)-plan.prerequisite_slot <= p.maximum_context_span,
           "MESSAGE_SIMULATION_CONTEXT_SPAN_UNKNOWN")
    message = decode_message(envelope.message_hex)
    wire = base64.b64decode(envelope.transaction_base64, validate=True)
    require(len(wire) <= 1232 and wire[:1] == b"\x01" and wire[1:65] == bytes(64)
        and wire[65:] == bytes.fromhex(envelope.message_hex), "MESSAGE_CANONICAL_ZERO_PLACEHOLDER_WIRE_BOUND_CONFLICT")
    require(type(message) is Message and str(message.recent_blockhash) == lease.blockhash
        and envelope.required_authority_count == envelope.zero_placeholder_count == 1
        and message.header.num_required_signatures == 1 and message.header.num_readonly_signed_accounts == 0
        and str(message.account_keys[0]) == c.domain.wallet, "MESSAGE_EXACT_WALLET_LEGACY_LEASE_CONFLICT")
    keys = tuple(map(str, message.account_keys))
    seen, extras = {}, []
    instructions = tuple(message.instructions)
    offset = 0
    for ix in instructions:
        if keys[ix.program_id_index] != COMPUTE_PROGRAM:
            break
        data = bytes(ix.data)
        require(not ix.accounts and data and data[0] in (2, 3) and data[0] not in seen
            and len(data) == (5 if data[0] == 2 else 9), "MESSAGE_COMPUTE_SHAPE_CONFLICT")
        seen[data[0]] = int.from_bytes(data[1:], "little")
        extras.append(COMPUTE_PROGRAM)
        offset += 1
    # Both values must be explicit, including explicit zero priority price.
    _known(set(seen) == {2, 3}, "MESSAGE_EXPLICIT_COMPUTE_ENVELOPE_REQUIRED")
    require(0 < seen[2] <= p.maximum_compute_units and seen[3] <= p.maximum_compute_unit_price_micro_lamports
        and 0 < result.units_consumed <= seen[2], "MESSAGE_COMPUTE_LIMIT_OR_METRICS_CONFLICT")
    expected_flags = {c.domain.wallet: (True, True), COMPUTE_PROGRAM: (False, False)}
    require(len(instructions)-offset == len(plan.instructions), "MESSAGE_EXTRA_OR_MISSING_INSTRUCTION")
    for actual, expected in zip(instructions[offset:], plan.instructions):
        require(keys[actual.program_id_index] == expected.program_id and bytes(actual.data).hex() == expected.data_hex
            and tuple(keys[i] for i in actual.accounts) == tuple(a.pubkey for a in expected.accounts),
            "MESSAGE_ORDERED_INSTRUCTION_CONFLICT")
        expected_flags.setdefault(expected.program_id, (False, False))
        for meta in expected.accounts:
            prior = expected_flags.get(meta.pubkey, (False, False))
            expected_flags[meta.pubkey] = (prior[0] or meta.authority_required, prior[1] or meta.writable)
    header = (message.header.num_required_signatures, message.header.num_readonly_signed_accounts,
              message.header.num_readonly_unsigned_accounts)
    require(set(keys) == set(expected_flags), "MESSAGE_UNREFERENCED_OR_EXTRA_ACCOUNT")
    for i, key in enumerate(keys):
        require(expected_flags[key] == (i < header[0], i < header[0]-header[1] or header[0] <= i < len(keys)-header[2]),
                "MESSAGE_EFFECTIVE_SIGNER_WRITABLE_CONFLICT")
    outer = tuple(InstructionFact(i, None, ix.program_id_index, tuple(ix.accounts), bytes(ix.data), None)
                  for i, ix in enumerate(instructions))
    inner, groups = _simulation_inner(result.inner_instructions_json, len(keys), len(outer))
    view = SimulationInstructionView(header, keys, outer, inner, groups)
    return message, view, seen[2], seen[3]


def _simulation_inner(payload, key_count, outer_count):
    value = strict_json_object('{"rows":'+payload+'}')["rows"]
    _known(type(value) is list, "MESSAGE_COMPLETE_CPI_TRACE_MISSING")
    rows, groups = [], []
    require(len(value) <= outer_count, "MESSAGE_INNER_GROUP_BOUND_INVALID")
    for group in value:
        require(type(group) is dict and set(group) == {"index", "instructions"} and type(group["index"]) is int
            and 0 <= group["index"] < outer_count and type(group["instructions"]) is list
            and group["index"] not in groups, "MESSAGE_INNER_GROUP_SHAPE_CONFLICT")
        groups.append(group["index"])
        for i, raw in enumerate(group["instructions"]):
            require(type(raw) is dict and set(raw) == {"programIdIndex", "accounts", "data", "stackHeight"}
                and type(raw["programIdIndex"]) is int and 0 <= raw["programIdIndex"] < key_count
                and type(raw["accounts"]) is list and all(type(j) is int and 0 <= j < key_count for j in raw["accounts"])
                and type(raw["data"]) is str and len(raw["data"]) <= 90000
                and type(raw["stackHeight"]) is int and raw["stackHeight"] in (2, 3), "MESSAGE_COMPILED_CPI_SHAPE_CONFLICT")
            data = _b58data(raw["data"], 65536)
            rows.append(InstructionFact(group["index"], i, raw["programIdIndex"], tuple(raw["accounts"]), data, raw["stackHeight"]))
    require(groups == sorted(groups) and len(rows) <= 4096, "MESSAGE_CPI_ORDER_OR_BOUND_CONFLICT")
    return tuple(rows), tuple(groups)


def _costs(original, plan, quote, view, compute_units, compute_price):
    e, p, c = original.evidence, original.profile, original.context
    fee, setup = e.fee, e.setup_accounts
    _known(fee is not None and setup is not None, "MESSAGE_EXACT_FEE_OR_SETUP_READ_MISSING")
    _read_cut(fee.cut, original, p.maximum_fee_age_us, "MESSAGE_EXACT_FEE_STALE_OR_FUTURE")
    _read_cut(setup.cut, original, p.maximum_setup_age_us, "MESSAGE_SETUP_READ_STALE_OR_FUTURE")
    lease, result, envelope = e.simulation.leases[-1], e.simulation.results[-1], e.simulation.envelopes[-1]
    floor = max(plan.prerequisite_slot, lease.context_slot)
    _known(fee.outcome == "OBSERVED" and fee.cut.context_slot >= fee.required_min_context_slot >= floor
        and fee.cut.context_slot-plan.prerequisite_slot <= p.maximum_context_span
        and base64.b64decode(fee.request_message_base64).hex() == envelope.message_hex,
        "MESSAGE_EXACT_FEE_UNAVAILABLE_OR_UNBOUND")
    _known(setup.requested_keys == view.account_keys and setup.cut.context_slot >= plan.prerequisite_slot
        and setup.cut.context_slot <= result.context_slot
        and result.context_slot-setup.cut.context_slot <= p.maximum_context_span
        and setup.cut.observed_at_us <= e.simulation.attempts[-1].observed_at_us,
        "MESSAGE_EXACT_SETUP_COVERAGE_OR_CUT_UNKNOWN")
    # Primary protocol: solana.com/docs/rpc/http/getfeeformessage and
    # solana.com/docs/core/fees. Priority is included in total, ceiling division.
    priority = (compute_units*compute_price+999999)//1000000
    costs = c.original_policy.costs
    network_cap = costs.network_total_fee_lamports if c.action.side == "BUY" else costs.protective_network_fee_lamports
    _known(priority <= fee.fee_lamports, "MESSAGE_FEE_AND_PRIORITY_CONTRADICTION")
    require(fee.fee_lamports <= network_cap and priority <= costs.priority_fee_within_total_lamports,
            "MESSAGE_EXPLICIT_NETWORK_FEE_LIMIT_CONFLICT")
    keys, wallet = view.account_keys, c.domain.wallet
    venue_ix, program, roles, token_roles, base, wsol, pool, pool_base, pool_quote, fee_native, fee_tokens, minimum = _roles(view, c.action, wallet)
    _validate_outer_shapes(view, venue_ix, token_roles, wsol, wallet)
    try:
        _validate_failed_inner(view, venue_ix, program, roles, token_roles, fee_native, fee_tokens, wallet)
    except ValueError:
        raise _Unknown("MESSAGE_CPI_LIFECYCLE_OR_PROGRAM_UNSUPPORTED") from None
    groups = set(view.inner_recorded_outer_indexes)
    ata_outer = {ix.outer_index: keys[ix.account_indexes[1]] for ix in view.outer_instructions
                 if keys[ix.program_id_index] == plans.ASSOCIATED_TOKEN_PROGRAM_ID}
    _known({venue_ix.outer_index, *ata_outer} <= groups, "MESSAGE_COMPLETE_REQUIRED_CPI_GROUPS_MISSING")
    _known(all(ix.outer_index in {venue_ix.outer_index, *ata_outer} for ix in view.inner_instructions),
           "MESSAGE_UNEXPECTED_CPI_GROUP_UNSUPPORTED")
    pre = dict(zip(keys, setup.accounts))
    original_accounts = {key:value for batch in (e.venue_read.primary,e.venue_read.dependent)
                         for key,value in zip(batch.requested_keys,batch.accounts)}
    _known(all(pre[key] is not None and value is not None and pre[key].account == value.account
        for key,value in original_accounts.items() if key in pre), "MESSAGE_VENUE_SETUP_ORIGINAL_BYTES_CONTRADICTION")
    used_tokens = {keys[i] for ix in view.inner_instructions for i in ix.account_indexes if keys[i] in token_roles}
    for key, (mint, token_program, owner) in token_roles.items():
        if pre[key] is not None:
            # Preserve the original finite Wallet shape semantics on replay.
            shape = _token_shape(pre[key], setup.cut.context_slot, owner, e.wallet.observation.schema)
            _known(not shape.reasons and (shape.mint, shape.program, shape.authority) == (mint, token_program, owner),
                   "MESSAGE_PRESENT_TOKEN_ROLE_SHAPE_UNSUPPORTED")
    explicit = dict(zip(e.wallet.observation.explicit_read.requested_keys, e.wallet.observation.explicit_read.accounts))
    _known(all(pre[key] == explicit[key] for key in (wallet, base, wsol)), "MESSAGE_WALLET_SETUP_READ_CONTRADICTION")
    _known(pre[wallet] is not None and pre[wallet].account.owner == plans.SYSTEM_PROGRAM_ID
        and not pre[wallet].executable and pre[wallet].account.data == b"", "MESSAGE_NATIVE_WALLET_SETUP_UNKNOWN")
    created, initialized, queried, immutable = {}, set(), set(), set()
    invoked_atas = set(ata_outer.values())
    nested = None
    last_outer = None
    native_venue = wallet_funding = external_funding = base_in = base_out = wsol_out = 0
    for ix in view.inner_instructions:
        pid, accounts = keys[ix.program_id_index], tuple(keys[i] for i in ix.account_indexes)
        if last_outer != ix.outer_index or ix.stack_height == 2:
            nested = None
        last_outer = ix.outer_index
        if pid == plans.ASSOCIATED_TOKEN_PROGRAM_ID:
            nested = accounts[1]
            invoked_atas.add(nested)
            continue
        ata = ata_outer.get(ix.outer_index, nested)
        if pid == plans.SYSTEM_PROGRAM_ID:
            if ix.data[:4] == bytes(4):
                amount = int.from_bytes(ix.data[4:12], "little")
                space = int.from_bytes(ix.data[12:20], "little")
                owner = str(plans.Pubkey.from_bytes(ix.data[20:52]))
                target = accounts[1]
                _known(accounts[0] == wallet and pre[target] is None and target not in created and amount > 0,
                       "MESSAGE_SETUP_CREATE_PRECONDITION_UNKNOWN")
                if ata is None:
                    _known(c.action.side == "BUY" and target == roles.get("user_volume_accumulator")
                        and owner == program and 0 < space <= 4096, "MESSAGE_PROGRAM_SETUP_UNSUPPORTED")
                    external_funding += amount
                else:
                    _known(target == ata and space == 165 and owner == venue.TOKEN_PROGRAM_ID
                        and target in token_roles and ata in queried, "MESSAGE_TOKEN_ACCOUNT_CREATION_UNSUPPORTED")
                    if token_roles[target][2] == wallet:
                        wallet_funding += amount
                    else:
                        external_funding += amount
                created[target] = (amount, space, owner)
            else:
                _known(c.action.side == "BUY" and program == venue.PUMP_PROGRAM_ID and accounts[0] == wallet
                    and accounts[1] in (pool, *fee_native), "MESSAGE_NATIVE_SETUP_OR_TRANSFER_AMBIGUOUS")
                native_venue += int.from_bytes(ix.data[4:12], "little")
        elif pid in (venue.TOKEN_PROGRAM_ID, venue.TOKEN_2022_PROGRAM_ID):
            if ata is not None:
                if ix.data in (b"\x15", b"\x15\x07\x00"):
                    _known(ata not in queried and pre[ata] is None, "MESSAGE_DUPLICATE_ACCOUNT_SIZE_QUERY")
                    queried.add(ata)
                elif ix.data == b"\x16":
                    _known(ata in created and ata not in immutable, "MESSAGE_ACCOUNT_IMMUTABLE_INIT_ORDER_UNKNOWN")
                    immutable.add(ata)
                elif ix.data[:1] == b"\x12":
                    _known(ata in created and ata in queried and ata in immutable and ata not in initialized,
                           "MESSAGE_ACCOUNT_INITIALIZATION_INCOMPLETE")
                    initialized.add(ata)
            elif ix.data[:1] in (b"\x03", b"\x0c"):
                source, target = accounts[0], accounts[2] if ix.data[0] == 12 else accounts[1]
                _known(all(pre[key] is not None or key in initialized for key in (source,target)),
                       "MESSAGE_TOKEN_TRANSFER_BEFORE_INITIALIZATION")
                amount = int.from_bytes(ix.data[1:9], "little")
                if target == base:
                    base_in += amount
                if source == base:
                    base_out += amount
                if source == wsol:
                    wsol_out += amount
    _known(all(key in initialized or key == roles.get("user_volume_accumulator") for key in created),
           "MESSAGE_CREATED_ACCOUNT_LIFECYCLE_INCOMPLETE")
    _known(all(pre[key] is not None or key in created for key in invoked_atas), "MESSAGE_ATA_CREATE_TRACE_MISSING")
    _known(all(pre[key] is not None or key in created for key in used_tokens), "MESSAGE_USED_TOKEN_ROLE_PRECONDITION_UNKNOWN")
    if c.action.side == "BUY":
        volume = roles["user_volume_accumulator"]
        _known(volume in created or pre[volume] is not None and pre[volume].account.owner == program
            and not pre[volume].executable and 0 < len(pre[volume].account.data) <= 4096,
            "MESSAGE_REQUIRED_VOLUME_SETUP_TRACE_MISSING")
    if c.action.side == "BUY":
        _known(base_in >= minimum > 0 and base_out == 0, "MESSAGE_SIMULATED_BASE_DELIVERY_UNKNOWN")
        if program == venue.PUMP_PROGRAM_ID:
            _known(0 < native_venue <= c.action.input_units and wsol_out == 0, "MESSAGE_GROSS_NATIVE_VENUE_OUTFLOW_UNKNOWN")
        else:
            _known(0 < wsol_out <= c.action.input_units and native_venue == 0, "MESSAGE_WRAPPED_QUOTE_OUTFLOW_UNKNOWN")
    else:
        _known(base_out == c.action.input_units and base_in == 0 and native_venue == 0 and wsol_out == 0,
               "MESSAGE_SIMULATED_REDUCTION_QUANTITY_UNKNOWN")
    setup_cap = costs.setup_outflow_lamports if c.action.side == "BUY" else costs.protective_setup_lamports
    lock_cap = costs.refundable_account_lock_lamports if c.action.side == "BUY" else costs.protective_refundable_lock_lamports
    require(external_funding <= setup_cap and wallet_funding <= lock_cap, "MESSAGE_EXPLICIT_SETUP_FUNDING_LIMIT_CONFLICT")
    # Reserve/count the full immutable quote cap before any refunds. Simulated
    # token/native proceeds never replenish upfront native requirements.
    quote_cap = c.action.input_units if c.action.side == "BUY" else 0
    upfront = fee.fee_lamports+quote_cap+external_funding+wallet_funding
    for value in (native_venue, external_funding, wallet_funding, upfront):
        u64(value)
    _known(pre[wallet].lamports >= upfront, "MESSAGE_GROSS_UPFRONT_NATIVE_UNPROVEN")
    _known(result.loaded_accounts_data_size >= sum(len(value.account.data) for value in pre.values() if value is not None),
           "MESSAGE_LOADED_ACCOUNT_METRIC_CONTRADICTION")
    return SimulatedCostFacts(fee.fee_lamports, priority, quote_cap, "SOL" if program == venue.PUMP_PROGRAM_ID else "WSOL",
        quote.fees.total_fee, wsol_out, native_venue, external_funding, wallet_funding, upfront,
        tuple((key, *created[key]) for key in sorted(created)))
