"""One bounded canonical interval; no indexer or non-landing adjudication."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from .public_rpc_v0_1 import (
    FinalizedBlockAnchor, PublicReadOnlyRpc, PublicRpcError, PublicRpcProfile,
    block_hash, evidence_fingerprint, immutable_tuple, primary_signature, u64,
)
from .transaction_evidence_v0_1 import (
    CanonicalSignatureBlock, TransactionReadFailure, _known_finalized_anchors_consistent,
    observation_domain_reasons, utc,
)


SCOPE = "EXACT_LOWER_THROUGH_CHOSEN_UPPER_NOT_WHOLE_VALIDITY"


@dataclass(frozen=True, slots=True)
class CoverageLimits:
    max_blocks: int = 256
    max_slot_span: int = 2048

    def __post_init__(self) -> None:
        if type(self.max_blocks) is not int or not 1 <= self.max_blocks <= 500:
            raise PublicRpcError("INVALID_COVERAGE_BLOCK_BOUND")
        if type(self.max_slot_span) is not int or not 1 <= self.max_slot_span <= 8192:
            raise PublicRpcError("INVALID_COVERAGE_SLOT_BOUND")


@dataclass(frozen=True, slots=True)
class CoverageRequest:
    genesis_hash: str
    signature: str
    recent_blockhash: str
    last_valid_block_height: int
    original_lower_anchor: FinalizedBlockAnchor
    upper_slot: int | None = None

    def __post_init__(self) -> None:
        block_hash(self.genesis_hash)
        block_hash(self.recent_blockhash)
        primary_signature(self.signature)
        u64(self.last_valid_block_height)
        if type(self.original_lower_anchor) is not FinalizedBlockAnchor:
            raise PublicRpcError("ORIGINAL_FINALIZED_LOWER_ANCHOR_REQUIRED")
        if self.original_lower_anchor.block_height > self.last_valid_block_height:
            raise PublicRpcError("LOWER_ANCHOR_AFTER_VALIDITY_BOUND")
        if self.upper_slot is not None:
            u64(self.upper_slot)
            if self.upper_slot < self.original_lower_anchor.slot:
                raise PublicRpcError("UPPER_PRECEDES_ORIGINAL_LOWER_ANCHOR")


def _anchor_core(block: CanonicalSignatureBlock | FinalizedBlockAnchor) -> tuple:
    return block.slot, block.blockhash, block.previous_blockhash, block.parent_slot, block.block_height


def _linked(child: CanonicalSignatureBlock, parent: CanonicalSignatureBlock) -> bool:
    return (child.parent_slot == parent.slot and child.previous_blockhash == parent.blockhash
            and child.slot > parent.slot and child.block_height == parent.block_height + 1)


@dataclass(frozen=True, slots=True)
class CanonicalCoverageObservation:
    request: CoverageRequest
    profile: PublicRpcProfile
    limits: CoverageLimits
    started_at_utc: str
    observed_at_utc: str
    genesis_start: str | None
    genesis_end: str | None
    initial_finalized_root_slot: int | None
    root: FinalizedBlockAnchor | None
    chosen_upper_slot: int | None
    blocks_descending: tuple[CanonicalSignatureBlock, ...]
    failures: tuple[TransactionReadFailure, ...]
    scope: str = SCOPE

    def __post_init__(self) -> None:
        if type(self.request) is not CoverageRequest or type(self.profile) is not PublicRpcProfile or type(self.limits) is not CoverageLimits:
            raise PublicRpcError("IMMUTABLE_CANONICAL_COVERAGE_DOMAIN_REQUIRED")
        for field in ("started_at_utc", "observed_at_utc"):
            object.__setattr__(self, field, utc(getattr(self, field)))
        for genesis in (self.genesis_start, self.genesis_end):
            if genesis is not None:
                block_hash(genesis)
        for slot in (self.initial_finalized_root_slot, self.chosen_upper_slot):
            if slot is not None:
                u64(slot)
        if self.root is not None and type(self.root) is not FinalizedBlockAnchor:
            raise PublicRpcError("IMMUTABLE_FINALIZED_ROOT_REQUIRED")
        immutable_tuple(self.blocks_descending, CanonicalSignatureBlock)
        immutable_tuple(self.failures, TransactionReadFailure)
        if self.scope != SCOPE:
            raise PublicRpcError("CANONICAL_COVERAGE_SCOPE_INVALID")

    @property
    def content_digest(self) -> str:
        return evidence_fingerprint(self)


class CanonicalCoverageAdapter:
    """Follow one requested parent chain, bounded before every remote read."""

    def __init__(self, rpc: PublicReadOnlyRpc, request: CoverageRequest, *,
                 limits: CoverageLimits = CoverageLimits(), clock: Callable[[], str] | None = None) -> None:
        if type(rpc) is not PublicReadOnlyRpc or type(request) is not CoverageRequest or type(limits) is not CoverageLimits:
            raise PublicRpcError("BOUND_CANONICAL_COVERAGE_ADAPTER_REQUIRED")
        self._rpc, self._request, self._limits, self._used = rpc, request, limits, False
        self._clock = clock or (lambda: datetime.now(timezone.utc).isoformat())

    def observe(self) -> CanonicalCoverageObservation:
        if self._used:
            raise PublicRpcError("BOUNDED_OBSERVATION_ALREADY_USED")
        self._used = True
        started = utc(self._clock())
        failures, blocks = [], []
        genesis_start = genesis_end = initial = root = chosen = None
        lower = self._request.original_lower_anchor

        def read(operation: str, call):
            try:
                return call()
            except PublicRpcError as exc:
                failures.append(TransactionReadFailure(operation, str(exc)))
                return None
        genesis_start = read("GENESIS_START", self._rpc.get_genesis_hash)
        if genesis_start == self._request.genesis_hash:
            initial = read("INITIAL_FINALIZED_ROOT", lambda: self._rpc.get_finalized_slot(min_context_slot=lower.slot))
            if initial is not None:
                chosen = initial if self._request.upper_slot is None else self._request.upper_slot
                if lower.provider_fingerprint != self._rpc.profile.fingerprint:
                    failures.append(TransactionReadFailure("COVERAGE_BOUND", "LOWER_ANCHOR_PROVIDER_MISMATCH"))
                elif chosen > initial:
                    failures.append(TransactionReadFailure("COVERAGE_BOUND", "UPPER_SLOT_NOT_OBSERVED_FINALIZED"))
                elif chosen - lower.slot > self._limits.max_slot_span:
                    failures.append(TransactionReadFailure("COVERAGE_BOUND", "COVERAGE_SLOT_BUDGET_EXHAUSTED"))
                else:
                    slot, reached = chosen, False
                    for _ in range(self._limits.max_blocks):
                        block = read("CANONICAL_BLOCK", lambda slot=slot: self._rpc.get_finalized_signature_block(slot))
                        if block is None:
                            break
                        blocks.append(block)
                        if len(blocks) > 1 and not _linked(blocks[-2], block):
                            failures.append(TransactionReadFailure("CANONICAL_LINK", "CANONICAL_PARENT_HASH_OR_HEIGHT_CONTRADICTION"))
                            break
                        if block.slot == lower.slot:
                            if _anchor_core(block) != _anchor_core(lower):
                                failures.append(TransactionReadFailure("LOWER_ANCHOR", "ORIGINAL_LOWER_ANCHOR_CONTRADICTION"))
                            else:
                                reached = True
                            break
                        if block.parent_slot < lower.slot:
                            failures.append(TransactionReadFailure("LOWER_ANCHOR", "ORIGINAL_LOWER_ANCHOR_NOT_ON_PARENT_CHAIN"))
                            break
                        slot = block.parent_slot
                    if not reached and not failures:
                        failures.append(TransactionReadFailure("COVERAGE_BOUND", "COVERAGE_BLOCK_BUDGET_EXHAUSTED"))
                current = read("FINALIZED_ROOT_SLOT", lambda: self._rpc.get_finalized_slot(min_context_slot=initial))
                if current is not None:
                    root = read("FINALIZED_ROOT", lambda: self._rpc.get_finalized_block_anchor(current))
            genesis_end = read("GENESIS_END", self._rpc.get_genesis_hash)
        return CanonicalCoverageObservation(self._request, self._rpc.profile, self._limits, started, utc(self._clock()),
                                             genesis_start, genesis_end, initial, root, chosen, tuple(blocks), tuple(failures))


@dataclass(frozen=True, slots=True)
class SignatureOccurrence:
    slot: int
    transaction_index: int

    def __post_init__(self) -> None:
        u64(self.slot)
        u64(self.transaction_index)


@dataclass(frozen=True, slots=True)
class LedgerCanonicalCoverageEvidence:
    observation: CanonicalCoverageObservation
    disposition: str
    chosen_upper_block_height: int | None
    fresh_root_block_height: int | None
    signature_occurrences: tuple[SignatureOccurrence, ...]
    reasons: tuple[str, ...]
    scope: str = SCOPE

    def __post_init__(self) -> None:
        if type(self.observation) is not CanonicalCoverageObservation or self.disposition not in (
                "COMPLETE_REQUESTED_INTERVAL", "INSUFFICIENT_COVERAGE", "CONTRADICTORY"):
            raise PublicRpcError("INVALID_LEDGER_CANONICAL_COVERAGE")
        for value in (self.chosen_upper_block_height, self.fresh_root_block_height):
            if value is not None:
                u64(value)
        immutable_tuple(self.signature_occurrences, SignatureOccurrence)
        immutable_tuple(self.reasons, str)
        if self.scope != SCOPE or self.disposition == "COMPLETE_REQUESTED_INTERVAL" and self.reasons:
            raise PublicRpcError("COVERAGE_SCOPE_OR_DISPOSITION_INVALID")


def ledger_canonical_coverage(observation: CanonicalCoverageObservation, *, expected_request: CoverageRequest,
                              expected_profile_fingerprint: str, now_utc: str) -> LedgerCanonicalCoverageEvidence:
    """Facts for future Ledger verification of its ORIGINAL persisted send cut.

    Completeness means the requested interval only. The chosen upper height and
    current root height deliberately remain separate from last-valid height.
    """
    reasons = observation_domain_reasons(profile=observation.profile, expected_profile=expected_profile_fingerprint,
        genesis_start=observation.genesis_start, genesis_end=observation.genesis_end,
        expected_genesis=expected_request.genesis_hash, root=observation.root,
        started_at=observation.started_at_utc, observed_at=observation.observed_at_utc, now=now_utc)
    contradictory = False
    if observation.request != expected_request:
        reasons.append("CONSUMER_ORIGINAL_COVERAGE_BINDING_MISMATCH")
    reasons.extend(f"{item.operation}:{item.code}" for item in observation.failures)
    if any("CONTRADICTION" in item.code or "NOT_ON_PARENT_CHAIN" in item.code or "DUPLICATE" in item.code for item in observation.failures):
        contradictory = True
    lower, chosen = observation.request.original_lower_anchor, observation.chosen_upper_slot
    initial, root, blocks = observation.initial_finalized_root_slot, observation.root, observation.blocks_descending
    if lower.provider_fingerprint != observation.profile.fingerprint:
        reasons.append("ORIGINAL_LOWER_PROVIDER_MISMATCH")
    if chosen is None or initial is None or root is None:
        reasons.append("COVERAGE_FINALIZED_BOUNDS_UNAVAILABLE")
    elif (chosen > initial or initial > root.slot or chosen < lower.slot
          or chosen != (initial if observation.request.upper_slot is None else observation.request.upper_slot)):
        reasons.append("COVERAGE_FINALIZED_BOUNDS_CONTRADICTION")
        contradictory = True
    if chosen is not None and chosen - lower.slot > observation.limits.max_slot_span or len(blocks) > observation.limits.max_blocks:
        reasons.append("COVERAGE_DECLARED_BUDGET_EXHAUSTED")
    if not blocks or blocks[0].slot != chosen or _anchor_core(blocks[-1]) != _anchor_core(lower):
        reasons.append("EXACT_REQUESTED_INTERVAL_NOT_COVERED")
    seen, occurrences = set(), []
    for index, block in enumerate(blocks):
        if block.provider_fingerprint != observation.profile.fingerprint or block.slot < lower.slot:
            reasons.append("CANONICAL_BLOCK_DOMAIN_OR_INTERVAL_CONTRADICTION")
            contradictory = True
        if index and not _linked(blocks[index-1], block):
            reasons.append("CANONICAL_PARENT_HASH_OR_HEIGHT_CONTRADICTION")
            contradictory = True
        if len(block.signatures) > observation.profile.max_block_signatures:
            reasons.append("BLOCK_SIGNATURE_BUDGET_EXHAUSTED")
        for position, signature in enumerate(block.signatures):
            if signature in seen:
                reasons.append("DUPLICATE_PRIMARY_SIGNATURE_ACROSS_CANONICAL_BLOCKS")
                contradictory = True
            seen.add(signature)
            if signature == observation.request.signature:
                occurrences.append(SignatureOccurrence(block.slot, position))
    upper_height = blocks[0].block_height if blocks and blocks[0].slot == chosen else None
    if root is not None and blocks:
        if any(not _known_finalized_anchors_consistent(root, block) for block in blocks):
            reasons.append("UPPER_AND_CURRENT_ROOT_CONTRADICTION")
            contradictory = True
    disposition = "CONTRADICTORY" if contradictory else "INSUFFICIENT_COVERAGE" if reasons else "COMPLETE_REQUESTED_INTERVAL"
    return LedgerCanonicalCoverageEvidence(observation, disposition, upper_height,
        None if root is None else root.block_height, tuple(occurrences), tuple(sorted(set(reasons))))
