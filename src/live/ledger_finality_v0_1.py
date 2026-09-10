"""Ledger-owned chain decisions from original public Evidence, without economics.

The retained positive proof and quarantine are separate facts. Neither finality
nor nonlanding releases a reservation, authorizes a retry, or applies balances.
"""
from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field, replace
from itertools import chain, combinations

from solders.transaction import VersionedTransaction
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from .ledger_actions_v0_1 import StoredAttempt
from .ledger_domain_v0_1 import LedgerContractError, LedgerDomain, ledger_utc
from .public_rpc_v0_1 import FinalizedBlockAnchor, evidence_fingerprint
from .transaction_evidence_v0_1 import (
    TransactionObservation, TransactionRequest, CanonicalSignatureBlock, TransactionOutcome, ExactTransactionFact,
    ledger_transaction_evidence, observation_domain_reasons, _known_finalized_anchors_consistent,
)
from .transaction_coverage_v0_1 import CanonicalCoverageObservation, CoverageRequest, ledger_canonical_coverage

RECEIPT_VERSION = "live_ledger_chain_receipt_v0.1"
POSITIVE_FINALITY = frozenset(("FINALIZED_SUCCESS", "FINALIZED_FAILURE", "PROVEN_NON_LANDED"))


@dataclass(frozen=True, slots=True)
class AttemptFinality:
    attempt_id: str
    disposition: str = "UNOBSERVED"
    positive_finality: str | None = None
    proof_evidence_digest: str | None = None
    exact_transaction_digest: str | None = None
    latest_compatible_transaction_evidence_digest: str | None = None
    quarantined: bool = False
    provisional_seen: bool = False
    canonical_occurrence_seen: bool = False
    evidence_revision: int = 0
    may_release_economic_lane: bool = field(init=False, default=False)
    economically_applied: bool = field(init=False, default=False)
    may_retry: bool = field(init=False, default=False)
    has_real_authority_grant: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class RetainedChainFacts:
    """Concrete replay accumulator, rebuilt from the journal under writer lock."""
    state: AttemptFinality
    anchors: tuple[FinalizedBlockAnchor | CanonicalSignatureBlock, ...] = ()
    finalized_status_claims: tuple[tuple[int, TransactionOutcome], ...] = ()
    exact_transaction_claims: tuple[ExactTransactionFact, ...] = ()
    selected_transaction: ExactTransactionFact | None = None


@dataclass(frozen=True, slots=True)
class ChainDecision:
    observation_disposition: str
    evidence_port_disposition: str
    reasons: tuple[str, ...]
    evaluated_at_utc: str
    required_min_finalized_root_slot: int
    expected_request: TransactionRequest | CoverageRequest
    resulting_state: AttemptFinality

    def to_record(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class LedgerChainReceipt:
    sequence: int
    ingestion_key: str
    attempt_id: str
    attempt_revision: int
    preparation_digest: str
    signed_wire_digest: str
    baseline_receipt_digest: str
    previous_digest: str
    generation: int
    generation_digest: str
    observation: TransactionObservation | CanonicalCoverageObservation
    decision: ChainDecision

    def to_record(self) -> dict:
        return {"version": RECEIPT_VERSION, "sequence": self.sequence, "ingestion_key": self.ingestion_key,
            "attempt_id": self.attempt_id, "attempt_revision": self.attempt_revision,
            "preparation_digest": self.preparation_digest, "signed_wire_digest": self.signed_wire_digest,
            "baseline_receipt_digest": self.baseline_receipt_digest, "previous_digest": self.previous_digest,
            "generation": self.generation, "generation_digest": self.generation_digest,
            "evidence_digest": self.observation.content_digest,
            "kind": "TRANSACTION" if type(self.observation) is TransactionObservation else "COVERAGE",
            "decision": self.decision.to_record()}

    @property
    def content_digest(self) -> str:
        return content_fingerprint(self.to_record())


def chain_receipt_from_record(record: dict, observation) -> LedgerChainReceipt:
    """Decode a concrete receipt; repository replay independently re-derives it."""
    try:
        row = dict(record)
        for name in ("version", "kind", "evidence_digest"):
            row.pop(name)
        decision = dict(row.pop("decision"))
        state = dict(decision["resulting_state"])
        for name in ("may_release_economic_lane", "economically_applied", "may_retry", "has_real_authority_grant"):
            state.pop(name)
        decision["resulting_state"] = AttemptFinality(**state)
        request = dict(decision["expected_request"])
        if type(observation) is TransactionObservation:
            decision["expected_request"] = TransactionRequest(**request)
        else:
            request["original_lower_anchor"] = FinalizedBlockAnchor(**request["original_lower_anchor"])
            decision["expected_request"] = CoverageRequest(**request)
        decision["reasons"] = tuple(decision["reasons"])
        value = LedgerChainReceipt(**row, observation=observation, decision=ChainDecision(**decision))
        if canonical_json(value.to_record()) != canonical_json(record):
            raise ValueError
        return value
    except Exception:
        raise LedgerContractError("LEDGER_CHAIN_RECEIPT_INVALID") from None


def _exact_metadata_compatible(left: ExactTransactionFact, right: ExactTransactionFact) -> bool:
    """Only accepted nullable time/stack facts may enrich an exact transaction."""
    def required(tx):
        return replace(tx, block_time=None,
            inner_instructions=tuple(replace(ix, stack_height=None) for ix in tx.inner_instructions))
    if required(left) != required(right):
        return False
    if left.block_time is not None and right.block_time is not None and left.block_time != right.block_time:
        return False
    return all(a.stack_height is None or b.stack_height is None or a.stack_height == b.stack_height
               for a, b in zip(left.inner_instructions, right.inner_instructions))


def adjudicate_chain_observation(domain: LedgerDomain, attempt: StoredAttempt, signed_wire: bytes,
        signed_at_utc: str, baseline_anchor: FinalizedBlockAnchor,
        observation: TransactionObservation | CanonicalCoverageObservation, *, evaluated_at_utc: str,
        retained: RetainedChainFacts | None = None) -> tuple[ChainDecision, RetainedChainFacts]:
    """No caller disposition enters this reducer. Historical UTC is explicit."""
    if domain.mode != "LIVE":
        raise LedgerContractError("LEDGER_DRY_HAS_NO_CHAIN_FINALITY_GRAPH")
    prep = attempt.preparation
    if (attempt.primary_signature is None or attempt.signed_wire_digest is None
            or hashlib.sha256(signed_wire).hexdigest() != attempt.signed_wire_digest):
        raise LedgerContractError("LEDGER_DURABLE_SIGNED_LINEAGE_REQUIRED")
    tx_wire = VersionedTransaction.from_bytes(signed_wire)
    if str(tx_wire.signatures[0]) != attempt.primary_signature:
        raise LedgerContractError("LEDGER_DURABLE_SIGNATURE_LINEAGE_CONFLICT")
    evaluated_at_utc = ledger_utc(evaluated_at_utc)
    if evaluated_at_utc < signed_at_utc:
        raise LedgerContractError("LEDGER_CHAIN_EVALUATION_BEFORE_SIGNED_FACT")
    retained = retained or RetainedChainFacts(AttemptFinality(prep.attempt_id),
                                            (baseline_anchor, prep.finalized_lower_anchor))
    prior = retained.state
    if prior.attempt_id != prep.attempt_id:
        raise LedgerContractError("LEDGER_CHAIN_REPLAY_ATTEMPT_CONFLICT")
    floor = max(domain.minimum_context_slot, baseline_anchor.slot, prep.finalized_lower_anchor.slot)
    domain_reasons = observation_domain_reasons(profile=observation.profile,
        expected_profile=domain.expected_profile_fingerprint, genesis_start=observation.genesis_start,
        genesis_end=observation.genesis_end, expected_genesis=domain.genesis_hash, root=observation.root,
        started_at=observation.started_at_utc, observed_at=observation.observed_at_utc, now=evaluated_at_utc)
    bound_domain = (observation.profile.fingerprint == domain.expected_profile_fingerprint
                    and observation.genesis_start == observation.genesis_end == domain.genesis_hash)
    reasons = list(domain_reasons)
    conflict = False
    if not _known_finalized_anchors_consistent(baseline_anchor, prep.finalized_lower_anchor):
        reasons.append("ORIGINAL_BASELINE_AND_PREPARATION_ANCHOR_CONFLICT")
        conflict = True
    observed = "UNKNOWN"
    positive = None
    exact_digest = None
    occurrence = False
    incoming = []
    status_claims = retained.finalized_status_claims
    transaction_claims = retained.exact_transaction_claims
    if observation.root is not None:
        incoming.append(observation.root)
        if observation.root.slot < floor:
            reasons.append("FINALIZED_ROOT_BELOW_ORIGINAL_ATTEMPT_FLOOR")
            conflict = True
    if type(observation) is TransactionObservation:
        expected = TransactionRequest(domain.genesis_hash, attempt.primary_signature, floor)
        port = ledger_transaction_evidence(observation, expected_genesis=domain.genesis_hash,
            expected_signature=attempt.primary_signature, expected_profile_fingerprint=domain.expected_profile_fingerprint,
            expected_message_sha256=prep.message_sha256, now_utc=evaluated_at_utc)
        reasons.extend(port.reasons)
        conflict = conflict or port.disposition == "CONTRADICTORY"
        bound_request = observation.request == expected
        tx, block, status = observation.transaction, observation.membership_block, observation.status
        if block is not None:
            incoming.append(block)
        if tx is not None and (tx.wire_bytes != signed_wire or tx.recent_blockhash != prep.lease.blockhash):
            reasons.append("STORED_FULL_WIRE_OR_RECENT_HASH_CONFLICT")
            conflict = True
        if bound_domain and bound_request:
            if block is not None and block.provider_fingerprint == domain.expected_profile_fingerprint:
                occurrence = attempt.primary_signature in block.signatures
            if status is not None and status.signature != attempt.primary_signature:
                reasons.append("STORED_SIGNATURE_STATUS_CONFLICT")
                conflict = True
            if tx is not None and tx.slot <= floor:
                reasons.append("TRANSACTION_NOT_AFTER_ORIGINAL_CUSTODY_CUT")
                conflict = True
            if (tx is not None and block is not None and block.slot == tx.slot
                    and prep.validity_profile == "RECENT_BLOCKHASH" and not tx_wire.uses_durable_nonce()
                    and block.block_height > prep.lease.last_valid_block_height):
                reasons.append("FINALIZED_TRANSACTION_AFTER_ORIGINAL_RECENT_HASH_VALIDITY")
                conflict = True
            if status is not None and status.slot is not None and status.slot <= floor:
                reasons.append("STATUS_NOT_AFTER_ORIGINAL_CUSTODY_CUT")
                conflict = True
            if status is not None and status.signature == attempt.primary_signature and status.confirmation_status == "finalized":
                status_claims = tuple(dict.fromkeys(status_claims + ((status.slot, status.outcome),)))
                occurrence = True
                if observation.root is not None and status.slot > observation.root.slot:
                    reasons.append("FINALIZED_STATUS_BEYOND_LATER_FINALIZED_ROOT")
                    conflict = True
            if (tx is not None and tx.primary_signature == attempt.primary_signature and tx.wire_bytes == signed_wire
                    and tx.provider_fingerprint == domain.expected_profile_fingerprint):
                transaction_claims = tuple(dict.fromkeys(transaction_claims + (tx,)))
                occurrence = True
            if not domain_reasons and status is not None and status.slot is not None:
                if status.confirmation_status == "confirmed":
                    observed = "CONFIRMED_PROVISIONAL"
                if status.confirmation_status == "finalized":
                    occurrence = True
            if port.disposition == "SUPPORTED_FINALIZED_OBSERVATION":
                positive = "FINALIZED_SUCCESS" if tx.outcome.succeeded else "FINALIZED_FAILURE"
                exact_digest = evidence_fingerprint(tx)
                observed = positive
                occurrence = True
    elif type(observation) is CanonicalCoverageObservation:
        expected = CoverageRequest(domain.genesis_hash, attempt.primary_signature, prep.lease.blockhash,
            prep.lease.last_valid_block_height, prep.finalized_lower_anchor, observation.request.upper_slot)
        port = ledger_canonical_coverage(observation, expected_request=expected,
            expected_profile_fingerprint=domain.expected_profile_fingerprint, now_utc=evaluated_at_utc)
        reasons.extend(port.reasons)
        conflict = conflict or port.disposition == "CONTRADICTORY"
        bound_request = observation.request == expected
        incoming.extend(observation.blocks_descending)
        if bound_domain and bound_request:
            occurrence = any(attempt.primary_signature in block.signatures
                             for block in observation.blocks_descending
                             if block.provider_fingerprint == domain.expected_profile_fingerprint)
            if prep.validity_profile != "RECENT_BLOCKHASH" or tx_wire.uses_durable_nonce():
                reasons.append("EXPIRY_REQUIRES_SUPPORTED_RECENT_BLOCKHASH_PROFILE")
            elif port.disposition == "COMPLETE_REQUESTED_INTERVAL":
                if port.chosen_upper_block_height < prep.lease.last_valid_block_height:
                    reasons.append("CANONICAL_COVERAGE_MISSING_VALIDITY_TAIL")
                elif port.fresh_root_block_height <= prep.lease.last_valid_block_height:
                    reasons.append("FRESH_ROOT_NOT_STRICTLY_AFTER_EXPIRY")
                elif occurrence or prior.canonical_occurrence_seen:
                    reasons.append("POSITIVE_SIGNATURE_OCCURRENCE_REQUIRES_EXACT_OUTCOME")
                else:
                    positive = observed = "PROVEN_NON_LANDED"
    else:
        raise LedgerContractError("LEDGER_CONCRETE_CHAIN_OBSERVATION_REQUIRED")
    if not bound_request:
        reasons.append("ORIGINAL_ATTEMPT_REQUEST_BINDING_CONFLICT")
        conflict = True
    if not bound_domain:
        reasons.append("ORIGINAL_ATTEMPT_CHAIN_DOMAIN_UNAVAILABLE_OR_CONFLICTING")
        # Missing genesis is unresolved; a stated foreign identity is quarantined.
        conflict = conflict or observation.profile.fingerprint != domain.expected_profile_fingerprint or any(
            value is not None and value != domain.genesis_hash for value in (observation.genesis_start, observation.genesis_end))
    if observation.started_at_utc < signed_at_utc:
        reasons.append("OBSERVATION_PRECEDES_DURABLE_SIGNED_FACT")
        positive = None
        observed = "UNKNOWN"
    anchors = retained.anchors
    if bound_domain and bound_request:
        if any(anchor.provider_fingerprint != domain.expected_profile_fingerprint for anchor in incoming):
            reasons.append("CANONICAL_ANCHOR_PROFILE_CONFLICT")
            conflict = True
        incoming = [anchor for anchor in incoming if anchor.provider_fingerprint == domain.expected_profile_fingerprint]
        combined = anchors + tuple(incoming)
        # Previously compared pairs are already retained; compare only this
        # observation's new facts against retained facts and one another.
        pairs = ((left, right) for left in anchors for right in incoming)
        for left, right in chain(pairs, combinations(incoming, 2)):
            if not _known_finalized_anchors_consistent(left, right):
                reasons.append("RETAINED_FINALIZED_ANCHOR_CONFLICT")
                conflict = True
            if (type(left) is CanonicalSignatureBlock and type(right) is CanonicalSignatureBlock
                    and left.slot == right.slot and left.signatures != right.signatures):
                reasons.append("RETAINED_CANONICAL_SIGNATURE_VECTOR_CONFLICT")
                conflict = True
        anchors = tuple(dict.fromkeys(combined))
    if len(status_claims) > 1:
        reasons.append("RETAINED_FINALIZED_STATUS_SLOT_OR_OUTCOME_CONFLICT")
        conflict = True
    if any(not _exact_metadata_compatible(left, right) for left, right in combinations(transaction_claims, 2)):
        reasons.append("RETAINED_EXACT_TRANSACTION_METADATA_CONFLICT")
        conflict = True
    if any((tx.slot, tx.outcome) != claim for tx in transaction_claims for claim in status_claims):
        reasons.append("RETAINED_FINALIZED_STATUS_AND_TRANSACTION_CONFLICT")
        conflict = True
    if positive is not None and prior.positive_finality is not None and positive != prior.positive_finality:
        reasons.append("RETAINED_POSITIVE_FINALITY_CONFLICT")
        conflict = True
    if prior.positive_finality == "PROVEN_NON_LANDED" and occurrence:
        reasons.append("CANONICAL_OCCURRENCE_CONTRADICTS_NONLANDING")
        conflict = True
    if domain_reasons or not bound_domain or not bound_request:
        positive = None
        observed = "UNKNOWN"
    if conflict:
        observed = "QUARANTINED"
        positive = None
    retained_positive = prior.positive_finality or positive
    retained_digest = prior.exact_transaction_digest or (exact_digest if positive is not None else None)
    proof_digest = prior.proof_evidence_digest or (observation.content_digest if positive is not None else None)
    quarantine = prior.quarantined or conflict
    provisional = prior.provisional_seen or observed == "CONFIRMED_PROVISIONAL"
    disposition = ("QUARANTINED" if quarantine else retained_positive + "_UNAPPLIED"
                   if retained_positive in ("FINALIZED_SUCCESS", "FINALIZED_FAILURE") else retained_positive
                   or ("CONFIRMED_PROVISIONAL" if observed == "CONFIRMED_PROVISIONAL" else "UNKNOWN"))
    latest = prior.latest_compatible_transaction_evidence_digest
    selected = retained.selected_transaction
    if positive in ("FINALIZED_SUCCESS", "FINALIZED_FAILURE") and not quarantine:
        incoming_tx = observation.transaction
        # Keep an original observation suitable for a later settlement consumer;
        # a later compatible observation must not discard already-known facts.
        if (selected is None or (selected.block_time is None or incoming_tx.block_time is not None)
                and all(left.stack_height is None or right.stack_height is not None
                        for left, right in zip(selected.inner_instructions, incoming_tx.inner_instructions))):
            latest, selected = observation.content_digest, incoming_tx
    state = AttemptFinality(attempt_id=prep.attempt_id, disposition=disposition, positive_finality=retained_positive,
        proof_evidence_digest=proof_digest, exact_transaction_digest=retained_digest,
        latest_compatible_transaction_evidence_digest=latest, quarantined=quarantine, provisional_seen=provisional,
        canonical_occurrence_seen=prior.canonical_occurrence_seen or occurrence, evidence_revision=prior.evidence_revision+1)
    decision = ChainDecision(observed, port.disposition, tuple(sorted(set(reasons))), evaluated_at_utc, floor, expected, state)
    return decision, RetainedChainFacts(state, anchors, status_claims, transaction_claims, selected)
