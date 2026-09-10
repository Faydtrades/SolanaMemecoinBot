"""Concrete actual-custody reducers. No admission, protection or retry policy.

All public amounts reconstruct from the opening baseline and whole original
settlements. Wallet comparison never creates capital or revises a past fill.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace

from phase5.shadow_domain_v0_1 import content_fingerprint
from .ledger_domain_v0_1 import LedgerContractError, ledger_utc
from .ledger_settlement_v0_1 import WalletSupportInput, SettlementProposal, attribute_settlement, WSOL_MINT, TOKEN_PROGRAM_ID, derive_associated_token_address
from .public_rpc_v0_1 import u64
from .wallet_evidence_v0_1 import ledger_account_evidence, _epoch
from .transaction_evidence_v0_1 import _known_finalized_anchors_consistent

VERSION = "live_ledger_actual_custody_v0.1"
RESOLVED = frozenset(("FINALIZED_SUCCESS_APPLIED", "FINALIZED_FAILURE_APPLIED", "PROVEN_NON_LANDED_RESOLVED"))
REPLACEABLE = frozenset(("FINALIZED_FAILURE_APPLIED", "PROVEN_NON_LANDED_RESOLVED"))


@dataclass(frozen=True, slots=True)
class CustodyAnchor:
    slot: int
    blockhash: str
    previous_blockhash: str
    parent_slot: int
    block_height: int
    block_time: int | None
    provider_fingerprint: str
    commitment: str = "finalized"

    @classmethod
    def retain(cls, anchor):
        return cls(**{key: getattr(anchor, key) for key in cls.__dataclass_fields__})


@dataclass(frozen=True, slots=True)
class AccountCustody:
    pubkey: str
    mint: str
    program: str
    presence: str
    units: int
    observed_lamports: int
    native_rent_reserve: int | None = None

    def __post_init__(self):
        u64(self.units)
        u64(self.observed_lamports)
        if self.native_rent_reserve is not None:
            u64(self.native_rent_reserve)
        if self.presence not in ("PRESENT", "ABSENT") or self.presence == "ABSENT" and (
                self.units != 0 or self.observed_lamports != 0 or self.native_rent_reserve is not None):
            raise LedgerContractError("CUSTODY_ABSENCE_CANNOT_OWN_UNITS_OR_LAMPORTS")

    @property
    def observed_locked_lamports(self):
        return self.observed_lamports-(self.units if self.mint == WSOL_MINT else 0)


@dataclass(frozen=True, slots=True)
class PositionCustody:
    position_id: str
    root_id: str
    mint: str
    token_program: str
    account: str
    acquired_units: int
    sold_units: int
    remaining_units: int
    acquisition_signature: str
    reduction_signatures: tuple[str, ...]
    posting_ids: tuple[str, ...]
    status: str
    usable: bool = field(init=False, default=False)
    has_protective_handoff: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class AttemptResolution:
    attempt_id: str
    action_id: str
    signature: str
    disposition: str
    application_sequence: int
    recorded_at_utc: str
    proposal_digest: str | None
    may_retry: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class WalletDifference:
    account: str
    asset: str
    kind: str
    expected: int | None
    observed: int | None
    delta: int | None


@dataclass(frozen=True, slots=True)
class WalletComparison:
    input_digest: str
    evidence_digest: str
    evaluated_at_utc: str
    required_min_context_slot: int
    context_slot: int | None
    profile_fingerprint: str
    anchor: CustodyAnchor | None
    common_revision: int
    common_digest: str
    pending_attempts: tuple[str, ...]
    disposition: str
    reasons: tuple[str, ...]
    differences: tuple[WalletDifference, ...]
    proves_external_transfer: bool = field(init=False, default=False)
    authority_current: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class CustodyState:
    economic_domain_id: str
    baseline_receipt_digest: str | None = None
    known_initial_native_lamports: int | None = None
    native_lamports: int | None = None
    network_fees_paid_lamports: int = 0
    venue_fees_paid_sol_lamports: int = 0
    venue_fees_paid_wsol_units: int = 0
    external_setup_paid_lamports: int = 0
    accounts: tuple[AccountCustody, ...] = ()
    positions: tuple[PositionCustody, ...] = ()
    resolutions: tuple[AttemptResolution, ...] = ()
    anchor: CustodyAnchor | None = None
    last_effect_sequence: int = 0
    quarantine_reasons: tuple[str, ...] = ()
    latest_comparison: WalletComparison | None = None
    qualified_wallet_anchors: tuple[CustodyAnchor, ...] = ()
    version: str = field(init=False, default=VERSION)
    authority_current: bool = field(init=False, default=False)

    def to_record(self):
        return asdict(self)

    @property
    def content_digest(self):
        return content_fingerprint(self.to_record())

    @property
    def quarantined(self):
        return bool(self.quarantine_reasons)

    def resolution(self, attempt_id):
        return next((item for item in self.resolutions if item.attempt_id == attempt_id), None)


@dataclass(frozen=True, slots=True)
class ApplicationDecision:
    disposition: str
    reasons: tuple[str, ...]
    proposal: SettlementProposal | None
    prior_custody_digest: str
    resulting_custody_digest: str
    releases_own_lane: bool
    has_real_authority_grant: bool = field(init=False, default=False)
    may_retry: bool = field(init=False, default=False)

    def to_record(self):
        return asdict(self)


@dataclass(frozen=True, slots=True)
class FundingView:
    economic_domain_id: str
    baseline_receipt_digest: str | None
    known_initial_native_lamports: int | None
    native_lamports: int | None
    network_fees_paid_lamports: int
    venue_fees_paid_sol_lamports: int
    venue_fees_paid_wsol_units: int
    external_setup_paid_lamports: int
    anchor: CustodyAnchor | None
    last_effect_sequence: int
    quarantine_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PositionView:
    position_id: str
    root_id: str
    mint: str
    token_program: str
    account: str
    acquired_units: int
    sold_units: int
    remaining_units: int
    acquisition_signature: str
    status: str
    usable: bool = field(init=False, default=False)
    has_protective_handoff: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class ComparisonView:
    input_digest: str
    evidence_digest: str
    evaluated_at_utc: str
    required_min_context_slot: int
    context_slot: int | None
    profile_fingerprint: str
    anchor: CustodyAnchor | None
    common_revision: int
    common_digest: str
    pending_attempts: tuple[str, ...]
    disposition: str
    reasons: tuple[str, ...]
    difference_count: int
    authority_current: bool = field(init=False, default=False)


def baseline_custody(domain, receipt):
    if receipt is None:
        return CustodyState(domain.economic_domain_id)
    decision = receipt.decision
    if decision.disposition != "ESTABLISHED":
        raise LedgerContractError("ESTABLISHED_BASELINE_CUSTODY_REQUIRED")
    accounts = tuple(AccountCustody(item.pubkey, item.mint, item.program, item.presence, 0,
                                   item.observed_lamports or 0) for item in decision.known_token_accounts)
    return CustodyState(domain.economic_domain_id, receipt.content_digest, decision.owned_native_lamports,
        decision.owned_native_lamports, accounts=tuple(sorted(accounts, key=lambda row: row.pubkey)),
        anchor=CustodyAnchor.retain(receipt.observation.anchor), last_effect_sequence=receipt.sequence,
        qualified_wallet_anchors=(CustodyAnchor.retain(receipt.observation.anchor),))


def quarantine_custody(state, reason):
    return replace(state, quarantine_reasons=tuple(sorted(set((*state.quarantine_reasons, reason)))))


def retain_chain_custody(state, retained):
    """L3-qualified facts must agree with every retained qualified Wallet cut."""
    if retained.state.quarantined:
        state = quarantine_custody(state, "CHAIN_FACTS_CONTRADICTION")
    if any(not _known_finalized_anchors_consistent(wallet, anchor)
           for wallet in state.qualified_wallet_anchors for anchor in retained.anchors):
        state = quarantine_custody(state, "CHAIN_ANCHOR_AND_CURRENT_CUSTODY_CONTRADICTION")
    return state


def retain_wallet_custody(state, anchor, chain_anchors):
    """Keep one port-qualified Wallet anchor even when its balances are pending."""
    conflict = any(not _known_finalized_anchors_consistent(anchor, previous) for previous in state.qualified_wallet_anchors)
    conflict = any(not _known_finalized_anchors_consistent(anchor, previous) for previous in chain_anchors) or conflict
    state = replace(state, qualified_wallet_anchors=tuple(dict.fromkeys((*state.qualified_wallet_anchors, anchor))))
    if conflict:
        state = quarantine_custody(state, "WALLET_AND_RETAINED_CANONICAL_ANCHOR_CONTRADICTION")
    return state, conflict


def qualified_wallet_anchor(domain, support):
    """Qualify only the canonical anchor, independently of account coverage.

    Mirror the accepted Wallet adapter's original genesis/slot/read binding.
    Missing token enumeration never erases an independently read finalized
    block. Stale balances remain unusable; a historical canonical claim does
    not cease to exist as its original observation ages.
    """
    obs = support.observation
    anchor, initial, upper = obs.anchor, obs.initial_finalized_slot, obs.finalized_upper_slot
    slots = [item.context.slot for item in (*obs.inventories, *obs.mint_reads)]
    if obs.explicit_read is not None:
        slots.append(obs.explicit_read.context.slot)
    if (obs.request.wallet != domain.wallet or obs.request.genesis_hash != domain.genesis_hash
            or obs.observed_genesis != domain.genesis_hash or obs.observed_genesis_end != domain.genesis_hash
            or obs.profile.fingerprint != domain.expected_profile_fingerprint
            or anchor is None or anchor.provider_fingerprint != domain.expected_profile_fingerprint
            or initial is None or upper is None or initial < obs.request.min_context_slot or upper < initial
            or not slots or anchor.slot != max(slots) or anchor.slot < support.required_min_context_slot
            or any(slot < initial or slot > upper for slot in slots)
            or any(item.wallet != domain.wallet for item in obs.inventories)):
        return None
    started, observed, evaluated = map(_epoch, (obs.started_at_utc, obs.observed_at_utc, support.evaluated_at_utc))
    if not (0 <= observed-started < obs.profile.observation_timeout_seconds and anchor.block_time <= observed <= evaluated):
        return None
    return CustodyAnchor.retain(anchor)


def _preconditions(domain, state, action, tx):
    reasons = set()
    if state.native_lamports is None:
        return ("CUSTODY_OPENING_BASELINE_REQUIRED",)
    keys = tx.account_keys
    index = {key: i for i, key in enumerate(keys)}
    if tx.pre_lamports[index[domain.wallet]] != state.native_lamports:
        reasons.add("CUSTODY_NATIVE_PREBALANCE_DIVERGENCE")
    accounts = {item.pubkey: item for item in state.accounts}
    pre = {keys[item.account_index]: item for item in tx.pre_tokens if item.owner == domain.wallet}
    post = {keys[item.account_index]: item for item in tx.post_tokens if item.owner == domain.wallet}
    identities = {derive_associated_token_address(domain.wallet, action.mint, action.token_program),
                  derive_associated_token_address(domain.wallet, WSOL_MINT, TOKEN_PROGRAM_ID)}
    for key in set(pre) | set(post) | (accounts.keys() & index.keys()) | identities:
        prior = accounts.get(key)
        fact = pre.get(key)
        lamports = tx.pre_lamports[index[key]]
        if prior is None or prior.presence == "ABSENT":
            if fact is not None or lamports != 0:
                reasons.add("CUSTODY_UNATTRIBUTED_PREEXISTING_ACCOUNT")
        elif (fact is None or (fact.mint, fact.program, fact.amount, lamports)
              != (prior.mint, prior.program, prior.units, prior.observed_lamports)):
            reasons.add("CUSTODY_TOKEN_OR_LOCKED_PREBALANCE_DIVERGENCE")
    position = next((item for item in state.positions if item.position_id == action.position_id), None)
    if tx.outcome.succeeded and action.side == "BUY" and position is not None:
        reasons.add("CUSTODY_BUY_ROOT_ALREADY_ACQUIRED")
    if tx.outcome.succeeded and action.side == "SELL" and (position is None or position.remaining_units < action.input_units
            or (position.root_id, position.mint, position.token_program) != (action.root_id, action.mint, action.token_program)):
        reasons.add("CUSTODY_POSITION_MISSING_OR_OVERSELL")
    return tuple(sorted(reasons))


def adjudicate_application(domain, state, action, attempt, receipt, support, *, sequence, recorded_at_utc, chain_anchors=()):
    """Recompute original L4, then apply everything or nothing to owned custody."""
    recorded_at_utc = ledger_utc(recorded_at_utc)
    prior = state.content_digest
    if support is not None:
        qualified = qualified_wallet_anchor(domain, support)
        if qualified is not None:
            state, _ = retain_wallet_custody(state, qualified, chain_anchors)
    resolved = state.resolution(attempt.preparation.attempt_id)
    if resolved is not None:
        return ApplicationDecision("ALREADY_RESOLVED", (), None, prior, state.content_digest, False), state
    if state.quarantined or attempt.chain_quarantined:
        return ApplicationDecision("QUARANTINED", ("CURRENT_CUSTODY_QUARANTINED",), None, prior, state.content_digest, False), state
    if receipt is None or receipt.attempt_id != attempt.preparation.attempt_id:
        raise LedgerContractError("APPLICATION_ORIGINAL_CHAIN_RECEIPT_REQUIRED")
    if support is None:
        if (receipt.decision.resulting_state.positive_finality != "PROVEN_NON_LANDED"
                or attempt.chain_finality != "PROVEN_NON_LANDED"):
            return ApplicationDecision("UNAPPLIED", ("SUPPORTED_NONLANDING_PROOF_REQUIRED",), None, prior, prior, False), state
        resolution = AttemptResolution(attempt.preparation.attempt_id, action.action_id, attempt.primary_signature,
            "PROVEN_NON_LANDED_RESOLVED", sequence, recorded_at_utc, None)
        updated = replace(state, resolutions=(*state.resolutions, resolution))
        return ApplicationDecision(resolution.disposition, (), None, prior, updated.content_digest, True), updated
    result = attribute_settlement(domain, action, attempt, receipt, support)
    if result.proposal is None:
        updated = quarantine_custody(state, "SETTLEMENT_ATTRIBUTION_CONTRADICTION") if result.disposition == "QUARANTINED" else state
        return ApplicationDecision(result.disposition, result.reasons, None, prior, updated.content_digest, False), updated
    tx = receipt.observation.transaction
    anchor = receipt.observation.membership_block
    if (state.anchor is None or tx.slot <= state.anchor.slot or
            not _known_finalized_anchors_consistent(state.anchor, anchor)):
        reasons = ("CUSTODY_TRANSACTION_CUT_OR_ANCHOR_CONFLICT",)
    else:
        reasons = _preconditions(domain, state, action, tx)
    if reasons:
        updated = quarantine_custody(state, "SETTLEMENT_CUSTODY_PRECONDITION_CONTRADICTION")
        return ApplicationDecision("QUARANTINED", reasons, None, prior, updated.content_digest, False), updated
    proposal = result.proposal
    accounts = {item.pubkey: item for item in state.accounts}
    post = {tx.account_keys[item.account_index]: item for item in tx.post_tokens if item.owner == domain.wallet}
    pre = {tx.account_keys[item.account_index]: item for item in tx.pre_tokens if item.owner == domain.wallet}
    port = ledger_account_evidence(support.observation, expected_wallet=domain.wallet, expected_genesis=domain.genesis_hash,
        expected_profile_fingerprint=domain.expected_profile_fingerprint, required_min_context_slot=support.required_min_context_slot,
        now_utc=support.evaluated_at_utc)
    shapes = {item.pubkey: item for item in port.assessment.tokens}
    identities = {derive_associated_token_address(domain.wallet, action.mint, action.token_program): (action.mint, action.token_program),
                  derive_associated_token_address(domain.wallet, WSOL_MINT, TOKEN_PROGRAM_ID): (WSOL_MINT, TOKEN_PROGRAM_ID)}
    for key in set(pre) | set(post) | identities.keys():
        fact = post.get(key) or pre.get(key)
        mint, program = identities[key] if fact is None else (fact.mint, fact.program)
        i = tx.account_keys.index(key)
        reserve = None if key not in post or mint != WSOL_MINT else shapes[key].native_rent_reserve
        value = AccountCustody(key, mint, program, "PRESENT" if key in post else "ABSENT",
            0 if key not in post else post[key].amount, tx.post_lamports[i], reserve)
        if value.observed_locked_lamports < 0 or reserve is not None and value.observed_locked_lamports < reserve:
            updated = quarantine_custody(state, "WSOL_RESERVE_OR_BACKING_CONTRADICTION")
            return ApplicationDecision("QUARANTINED", ("WSOL_RESERVE_OR_BACKING_CONTRADICTION",), None, prior, updated.content_digest, False), updated
        accounts[key] = value
    positions = {item.position_id: item for item in state.positions}
    posting_ids = tuple(item.component_id for item in proposal.components)
    if proposal.base_units_delta:
        base_account = next(item.account for item in proposal.components if item.kind in ("BASE_ACQUIRED", "BASE_SOLD"))
        if action.side == "BUY":
            positions[action.position_id] = PositionCustody(action.position_id, action.root_id, action.mint, action.token_program,
                base_account, proposal.base_units_delta, 0, proposal.base_units_delta, proposal.signature, (), posting_ids, "OWNED_PROTECTION_PENDING")
        else:
            value = positions[action.position_id]
            remaining = value.remaining_units+proposal.base_units_delta
            if remaining < 0 or base_account != value.account:
                raise LedgerContractError("WHOLE_SETTLEMENT_POSITION_INVARIANT_FAILED")
            positions[action.position_id] = replace(value, sold_units=value.sold_units-proposal.base_units_delta,
                remaining_units=remaining, reduction_signatures=(*value.reduction_signatures, proposal.signature),
                posting_ids=(*value.posting_ids, *posting_ids), status="FLAT_PENDING_RECONCILIATION" if remaining == 0 else "OWNED_PROTECTION_PENDING")
    for account in accounts.values():
        if account.mint != WSOL_MINT and sum(item.remaining_units for item in positions.values() if item.account == account.pubkey) != account.units:
            raise LedgerContractError("POSITION_AND_ACCOUNT_AGGREGATE_CONFLICT")
    resolution = AttemptResolution(attempt.preparation.attempt_id, action.action_id, attempt.primary_signature,
        "FINALIZED_SUCCESS_APPLIED" if tx.outcome.succeeded else "FINALIZED_FAILURE_APPLIED", sequence, recorded_at_utc, proposal.content_digest)
    updated = replace(state, native_lamports=state.native_lamports+proposal.native_wallet_delta,
        network_fees_paid_lamports=state.network_fees_paid_lamports+proposal.transaction_fee_lamports,
        venue_fees_paid_sol_lamports=state.venue_fees_paid_sol_lamports+sum(-c.units for c in proposal.components if c.kind == "VENUE_FEE" and c.asset == "SOL"),
        venue_fees_paid_wsol_units=state.venue_fees_paid_wsol_units+sum(-c.units for c in proposal.components if c.kind == "VENUE_FEE" and c.asset == "WSOL"),
        external_setup_paid_lamports=state.external_setup_paid_lamports+sum(-c.units for c in proposal.components if c.kind in ("VENUE_ACCOUNT_SETUP", "EXTERNAL_ACCOUNT_FUNDING")),
        accounts=tuple(sorted(accounts.values(), key=lambda row: row.pubkey)), positions=tuple(sorted(positions.values(), key=lambda row: row.position_id)),
        resolutions=(*state.resolutions, resolution), anchor=CustodyAnchor.retain(anchor), last_effect_sequence=sequence)
    u64(updated.native_lamports)
    return ApplicationDecision(resolution.disposition, (), proposal, prior, updated.content_digest, True), updated


def compare_wallet(domain, state, support, *, common_revision, common_digest, pending_attempts, chain_anchors=()):
    """Retain differences against one exact Ledger cut, never adopt them."""
    if type(support) is not WalletSupportInput:
        raise LedgerContractError("ORIGINAL_WALLET_COMPARISON_INPUT_REQUIRED")
    port = ledger_account_evidence(support.observation, expected_wallet=domain.wallet, expected_genesis=domain.genesis_hash,
        expected_profile_fingerprint=domain.expected_profile_fingerprint, required_min_context_slot=support.required_min_context_slot,
        now_utc=support.evaluated_at_utc)
    reasons = set((*port.assessment.reasons, *port.consumer_reasons))
    obs = support.observation
    anchor = None if obs.anchor is None else CustodyAnchor.retain(obs.anchor)
    qualified = qualified_wallet_anchor(domain, support)
    if qualified is not None:
        state, conflict = retain_wallet_custody(state, qualified, chain_anchors)
        if conflict:
            reasons.add("COMPARISON_RETAINED_CANONICAL_ANCHOR_CONFLICT")
    if state.anchor is None or state.native_lamports is None:
        reasons.add("COMPARISON_OPENING_BASELINE_REQUIRED")
    elif support.required_min_context_slot < state.anchor.slot or anchor is None or anchor.slot < state.anchor.slot:
        reasons.add("COMPARISON_BEHIND_RECOGNIZED_CUSTODY_CUT")
    elif qualified is not None and not _known_finalized_anchors_consistent(state.anchor, anchor):
        reasons.add("COMPARISON_ANCHOR_CONFLICT")
    differences = []
    explicit = {} if obs.explicit_read is None else dict(zip(obs.explicit_read.requested_keys, obs.explicit_read.accounts))
    if port.account_facts_usable and not reasons:
        native = explicit.get(domain.wallet)
        observed = None if native is None else native.lamports
        if observed != state.native_lamports:
            delta = None if observed is None else observed-state.native_lamports
            differences.append(WalletDifference(domain.wallet, "SOL", "UNKNOWN_NATIVE_PRESENCE" if delta is None else
                "POSITIVE_NATIVE_DIFFERENCE" if delta > 0 else "NEGATIVE_NATIVE_DIFFERENCE", state.native_lamports, observed, delta))
        accounts = {item.pubkey: item for item in state.accounts}
        observed_tokens = {item.pubkey: item for item in port.assessment.tokens}
        # Inventory entries are public keyed account facts; explicit reads remain
        # mandatory for known custody so absence cannot become an inferred zero.
        requested = {item.pubkey: (item.mint, item.program) for item in obs.request.expected_accounts}
        for key in sorted(accounts.keys() | observed_tokens.keys()):
            expected, observed = accounts.get(key), observed_tokens.get(key)
            if expected is not None and (requested.get(key) != (expected.mint, expected.program) or key not in explicit):
                reasons.add("COMPARISON_EXPLICIT_KNOWN_ACCOUNT_COVERAGE_MISSING")
                continue
            if expected is None:
                differences.append(WalletDifference(key, observed.mint or "UNKNOWN", "UNATTRIBUTED_TOKEN_ACCOUNT", None, observed.amount, None))
                continue
            if observed is None:
                if expected.presence != "ABSENT":
                    differences.append(WalletDifference(key, expected.mint, "MISSING_KNOWN_TOKEN_ACCOUNT", expected.units, None, None))
                continue
            if (observed.mint, observed.program, observed.authority) != (expected.mint, expected.program, domain.wallet):
                differences.append(WalletDifference(key, expected.mint, "TOKEN_IDENTITY_CONFLICT", expected.units, observed.amount, None))
                continue
            if expected.presence != "PRESENT" or observed.amount != expected.units:
                differences.append(WalletDifference(key, expected.mint, "TOKEN_UNITS_DIFFERENCE", expected.units, observed.amount,
                    None if observed.amount is None else observed.amount-expected.units))
            raw = explicit.get(key)
            if raw is None or raw.lamports != expected.observed_lamports:
                differences.append(WalletDifference(key, "ACCOUNT_LAMPORTS", "LOCKED_LAMPORTS_DIFFERENCE", expected.observed_lamports,
                    None if raw is None else raw.lamports, None if raw is None else raw.lamports-expected.observed_lamports))
            if observed.native_rent_reserve != expected.native_rent_reserve:
                differences.append(WalletDifference(key, "WSOL_RESERVE", "NATIVE_RESERVE_DIFFERENCE", expected.native_rent_reserve, observed.native_rent_reserve, None))
    disposition = ("INCOMPLETE_OR_UNSUPPORTED" if not port.account_facts_usable or reasons else
                   "PENDING_EFFECTS_UNKNOWN" if pending_attempts else "CUSTODY_DIFFERENCES_QUARANTINED" if differences else "MATCHED_AT_ORIGINAL_CUT")
    comparison = WalletComparison(support.digest, obs.content_digest, support.evaluated_at_utc, support.required_min_context_slot,
        port.assessment.context_slot, obs.profile.fingerprint, anchor, common_revision, common_digest, tuple(pending_attempts),
        disposition, tuple(sorted(reasons)), tuple(differences))
    updated = replace(state, latest_comparison=comparison)
    if disposition == "CUSTODY_DIFFERENCES_QUARANTINED" or "COMPARISON_ANCHOR_CONFLICT" in reasons:
        updated = quarantine_custody(updated, "ORIGINAL_WALLET_CUSTODY_CONTRADICTION")
    elif disposition == "MATCHED_AT_ORIGINAL_CUT":
        updated = replace(updated, anchor=anchor)
    return comparison, updated
