"""Public wallet facts for Ledger; no baseline, spendability or attribution."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from solders.pubkey import Pubkey

from phase5.shadow_venue_route_quote_v0_1 import (
    SYSTEM_PROGRAM_ID, TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID, WSOL_MINT, decode_mint_account,
    decode_token_account, VenueStateError,
)
from .public_rpc_v0_1 import (
    TOKEN_PROGRAMS, FinalizedBlockAnchor, PublicAccount, PublicAccountRead,
    PublicReadOnlyRpc, PublicRpcError, PublicRpcProfile, TokenInventoryRead,
    block_hash, evidence_fingerprint, immutable_tuple, public_key, public_label, u64,
)


LEGACY_SCHEMA = "live_wallet_account_evidence_v0.1"
IMMUTABLE_OWNER_SCHEMA = "live_wallet_account_evidence_v0.2"
SCHEMA = "live_wallet_account_evidence_v0.3"
# One explicit new account shape, not a general extension parser. Official
# https://github.com/solana-program/token-2022/tree/
# aa84ca89f26127a8f881c46484974b534fefb6f6/interface/src/extension
# mod.rs SHA256 502b8309d3243f81d3bb7b2ff5f9e412c48d4d68f354b8994792389fc904defd
# immutable_owner.rs SHA256 8346ba8c6b309d75280550585a1bd7b6a43f0059db9fda0fc4d43d5ac408647d
# 165-byte Account + AccountType::Account (u8=2) + ImmutableOwner (LEu16=7)
# + zero value length (LEu16=0). No padding or additional TLV is accepted.
_IMMUTABLE_OWNER_TAIL = b"\x02\x07\x00\x00\x00"


def _utc(value: str) -> str:
    if type(value) is not str:
        raise ValueError("UTC_TEXT_REQUIRED")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")
    except ValueError:
        raise ValueError("EXPLICIT_UTC_REQUIRED") from None


def _epoch(value: str) -> float:
    return datetime.fromisoformat(value).timestamp()


@dataclass(frozen=True, slots=True)
class ExpectedTokenAccount:
    pubkey: str
    mint: str
    program: str

    def __post_init__(self) -> None:
        public_key(self.pubkey)
        public_key(self.mint)
        if self.program not in TOKEN_PROGRAMS:
            raise PublicRpcError("EXPECTED_TOKEN_PROGRAM_UNSUPPORTED")


@dataclass(frozen=True, slots=True)
class WalletEvidenceRequest:
    wallet: str
    genesis_hash: str
    min_context_slot: int
    expected_accounts: tuple[ExpectedTokenAccount, ...] = ()

    def __post_init__(self) -> None:
        public_key(self.wallet)
        block_hash(self.genesis_hash)
        u64(self.min_context_slot)
        immutable_tuple(self.expected_accounts, ExpectedTokenAccount)
        keys = tuple(account.pubkey for account in self.expected_accounts)
        if len(keys) > 32 or len(set(keys)) != len(keys) or self.wallet in keys:
            raise PublicRpcError("EXPECTED_ACCOUNT_KEY_CONTRACT_INVALID")


@dataclass(frozen=True, slots=True)
class PublicReadFailure:
    operation: str
    code: str

    def __post_init__(self) -> None:
        if self.operation not in ("GENESIS", "END_GENESIS", "START_SLOT", "SPL_INVENTORY", "TOKEN2022_INVENTORY",
                                  "EXPLICIT_ACCOUNTS", "MINT_ACCOUNTS", "END_SLOT", "BLOCK_ANCHOR"):
            raise PublicRpcError("INVALID_PUBLIC_READ_OPERATION")
        public_label(self.code)


@dataclass(frozen=True, slots=True)
class WalletObservation:
    request: WalletEvidenceRequest
    profile: PublicRpcProfile
    started_at_utc: str
    observed_at_utc: str
    observed_genesis: str | None
    observed_genesis_end: str | None
    initial_finalized_slot: int | None
    finalized_upper_slot: int | None
    anchor: FinalizedBlockAnchor | None
    inventories: tuple[TokenInventoryRead, ...]
    explicit_read: PublicAccountRead | None
    mint_reads: tuple[PublicAccountRead, ...]
    failures: tuple[PublicReadFailure, ...]
    schema: str = SCHEMA

    def __post_init__(self) -> None:
        if type(self.request) is not WalletEvidenceRequest or type(self.profile) is not PublicRpcProfile or self.schema not in (LEGACY_SCHEMA, IMMUTABLE_OWNER_SCHEMA, SCHEMA):
            raise PublicRpcError("IMMUTABLE_WALLET_DOMAIN_REQUIRED")
        for field in ("started_at_utc", "observed_at_utc"):
            object.__setattr__(self, field, _utc(getattr(self, field)))
        for genesis in (self.observed_genesis, self.observed_genesis_end):
            if genesis is not None:
                block_hash(genesis)
        for slot in (self.initial_finalized_slot, self.finalized_upper_slot):
            if slot is not None:
                u64(slot)
        if self.anchor is not None and type(self.anchor) is not FinalizedBlockAnchor:
            raise PublicRpcError("IMMUTABLE_BLOCK_ANCHOR_REQUIRED")
        if self.explicit_read is not None and type(self.explicit_read) is not PublicAccountRead:
            raise PublicRpcError("IMMUTABLE_ACCOUNT_READ_REQUIRED")
        immutable_tuple(self.inventories, TokenInventoryRead)
        immutable_tuple(self.mint_reads, PublicAccountRead)
        immutable_tuple(self.failures, PublicReadFailure)

    @property
    def content_digest(self) -> str:
        return evidence_fingerprint(self)


@dataclass(frozen=True, slots=True)
class TokenShapeEvidence:
    pubkey: str
    program: str
    context_slot: int
    mint: str | None
    authority: str | None
    amount: int | None
    state: int | None
    delegate: str | None
    delegated_amount: int | None
    native_rent_reserve: int | None
    close_authority: str | None
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        public_key(self.pubkey)
        public_key(self.program)
        u64(self.context_slot)
        for key in (self.mint, self.authority, self.delegate, self.close_authority):
            if key is not None:
                public_key(key)
        for value in (self.amount, self.state, self.delegated_amount, self.native_rent_reserve):
            if value is not None:
                u64(value)
        immutable_tuple(self.reasons, str)


@dataclass(frozen=True, slots=True)
class MintShapeEvidence:
    pubkey: str
    program: str
    context_slot: int
    supply: int | None
    decimals: int | None
    mint_authority: str | None
    freeze_authority: str | None
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        public_key(self.pubkey)
        public_key(self.program)
        u64(self.context_slot)
        for key in (self.mint_authority, self.freeze_authority):
            if key is not None:
                public_key(key)
        for value in (self.supply, self.decimals):
            if value is not None:
                u64(value)
        immutable_tuple(self.reasons, str)


def _key(raw: bytes) -> str:
    return str(Pubkey.from_bytes(raw))


def _option(raw: bytes, offset: int, *, key: bool) -> str | int | None:
    tag = int.from_bytes(raw[offset:offset+4], "little")
    if tag not in (0, 1):
        raise ValueError("INVALID_COPTION_TAG")
    if tag == 0:
        return None
    data = raw[offset+4:offset+(36 if key else 12)]
    return _key(data) if key else int.from_bytes(data, "little")


def _token_shape(value: PublicAccount, slot: int, wallet: str, schema: str) -> TokenShapeEvidence:
    account, data = value.account, value.account.data
    reasons = []
    mint = _key(data[:32]) if len(data) >= 32 else None
    authority = _key(data[32:64]) if len(data) >= 64 else None
    amount = state = delegated_amount = delegate = reserve = close = None
    immutable_owner = (schema in (IMMUTABLE_OWNER_SCHEMA, SCHEMA) and account.owner == TOKEN_2022_PROGRAM_ID
                       and len(data) == 170 and data[165:] == _IMMUTABLE_OWNER_TAIL)
    # Historical v0.1 receipts must keep their original unsupported verdict.
    if len(data) != 165 and not immutable_owner:
        reasons.append("UNSUPPORTED_TOKEN_EXTENSIONS_OR_LENGTH")
    if len(data) >= 165:
        amount, state = int.from_bytes(data[64:72], "little"), data[108]
        delegated_amount = int.from_bytes(data[121:129], "little")
        try:
            # Accepted base decoder is reused; exact length and options are
            # checked here because its minimum-length check permits extensions.
            decode_token_account(account, mint)
            delegate = _option(data, 72, key=True)
            reserve = _option(data, 109, key=False)
            close = _option(data, 129, key=True)
        except (ValueError, VenueStateError):
            reasons.append("INVALID_TOKEN_BASE_LAYOUT")
        if state == 2:
            reasons.append("FROZEN_TOKEN_ACCOUNT_UNSUPPORTED")
        if delegate is not None or delegated_amount:
            reasons.append("DELEGATED_TOKEN_ACCOUNT_UNSUPPORTED")
        if close is not None and close != wallet:
            reasons.append("EXTERNAL_CLOSE_AUTHORITY_UNSUPPORTED")
        if reserve is not None:
            if mint != WSOL_MINT or account.owner != TOKEN_PROGRAM_ID:
                reasons.append("UNSUPPORTED_NATIVE_TOKEN_SHAPE")
            elif reserve + amount > value.lamports:
                reasons.append("NATIVE_TOKEN_LAMPORTS_INCONSISTENT")
        elif mint == WSOL_MINT:
            reasons.append("NATIVE_TOKEN_RESERVE_MISSING")
    if authority != wallet:
        reasons.append("TOKEN_AUTHORITY_MISMATCH")
    if account.owner not in TOKEN_PROGRAMS or value.executable:
        reasons.append("TOKEN_PROGRAM_OR_EXECUTABLE_INVALID")
    return TokenShapeEvidence(account.pubkey, account.owner, slot, mint, authority, amount, state,
                              delegate, delegated_amount, reserve, close, tuple(sorted(set(reasons))))


def _mint_shape(value: PublicAccount, slot: int, schema: str = LEGACY_SCHEMA) -> MintShapeEvidence:
    account, data = value.account, value.account.data
    supply = decimals = authority = freeze = None
    reasons = []
    if len(data) != 82:
        if schema == SCHEMA and account.owner == TOKEN_2022_PROGRAM_ID:
            from .pump_token2022_profile_v0_1 import parse_pump_token2022_mint
            try:
                parse_pump_token2022_mint(account)
            except (ValueError, VenueStateError):
                reasons.append("UNSUPPORTED_MINT_EXTENSIONS_OR_LENGTH")
        else:
            # Version is in the original observation digest: old receipts
            # retain their old verdict even after this compatibility update.
            reasons.append("UNSUPPORTED_MINT_EXTENSIONS_OR_LENGTH")
    if len(data) >= 82:
        supply, decimals = int.from_bytes(data[36:44], "little"), data[44]
        try:
            decode_mint_account(account, account.pubkey)
            authority = _option(data, 0, key=True)
            freeze = _option(data, 46, key=True)
        except (ValueError, VenueStateError):
            reasons.append("INVALID_MINT_BASE_LAYOUT")
    if account.owner not in TOKEN_PROGRAMS or value.executable:
        reasons.append("MINT_PROGRAM_OR_EXECUTABLE_INVALID")
    return MintShapeEvidence(account.pubkey, account.owner, slot, supply, decimals, authority, freeze,
                             tuple(sorted(set(reasons))))


def _needed_mints(request: WalletEvidenceRequest, inventories: tuple[TokenInventoryRead, ...],
                  explicit: PublicAccountRead | None) -> tuple[str, ...]:
    keys = {item.mint for item in request.expected_accounts}
    accounts = [value for read in inventories for value in read.accounts]
    if explicit is not None:
        accounts.extend(value for key, value in zip(explicit.requested_keys, explicit.accounts, strict=True)
                        if key != request.wallet and value is not None)
    keys.update(_key(value.account.data[:32]) for value in accounts if len(value.account.data) >= 32)
    return tuple(sorted(keys))


class WalletEvidenceAdapter:
    """One configured wallet, one bounded observation, typed public RPC only."""

    def __init__(self, rpc: PublicReadOnlyRpc, request: WalletEvidenceRequest, *,
                 clock: Callable[[], str] | None = None, concurrent_cohort: bool = False,
                 retry_min_context: bool = False) -> None:
        if type(rpc) is not PublicReadOnlyRpc or type(request) is not WalletEvidenceRequest:
            raise PublicRpcError("BOUND_PUBLIC_WALLET_ADAPTER_REQUIRED")
        self._rpc, self._request = rpc, request
        self._clock = clock or (lambda: datetime.now(timezone.utc).isoformat())
        self._used = False
        if type(concurrent_cohort) is not bool:
            raise PublicRpcError("EXPLICIT_WALLET_COHORT_MODE_REQUIRED")
        self._concurrent_cohort = concurrent_cohort
        if type(retry_min_context) is not bool or retry_min_context and not concurrent_cohort:
            raise PublicRpcError("EXPLICIT_COHORT_CONTEXT_RETRY_REQUIRED")
        self._retry_min_context = retry_min_context

    def observe(self) -> WalletObservation:
        if self._used:
            raise PublicRpcError("BOUNDED_OBSERVATION_ALREADY_USED")
        self._used = True
        started = _utc(self._clock())
        failures = []
        inventories, mint_reads = [], []
        explicit = genesis = genesis_end = initial = upper = anchor = None

        def read(operation: str, call):
            try:
                return call()
            except PublicRpcError as exc:
                failures.append(PublicReadFailure(operation, str(exc)))
                return None

        genesis = read("GENESIS", self._rpc.get_genesis_hash)
        if genesis == self._request.genesis_hash:
            initial = read("START_SLOT", lambda: self._rpc.get_finalized_slot(min_context_slot=self._request.min_context_slot))
            if initial is not None:
                keys = (self._request.wallet, *(item.pubkey for item in self._request.expected_accounts))
                calls = [(operation, lambda program=program: self._rpc.get_token_accounts_by_owner(
                    self._request.wallet, program, min_context_slot=initial))
                    for operation, program in zip(("SPL_INVENTORY", "TOKEN2022_INVENTORY"), TOKEN_PROGRAMS, strict=True)]
                known_mints = tuple(sorted({item.mint for item in self._request.expected_accounts}))
                dependent_floor = initial
                if self._concurrent_cohort:
                    # One observation, two dependent pairs. Optional retries
                    # repeat only the same minimum slot within original budgets.
                    # Inventory
                    # context is an observed lower bound for account/mint reads,
                    # not an assertion that the provider returned that exact
                    # slot. Final assessment still requires exact equality.
                    from concurrent.futures import ThreadPoolExecutor
                    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="wallet-public") as pool:
                        pending = [(operation, pool.submit(call)) for operation, call in calls]
                        results = [read(operation, future.result) for operation, future in pending]
                        inventories.extend(result for result in results if result is not None)
                        dependent_floor = max((initial, *(item.context.slot for item in inventories)))
                        dependent = [("EXPLICIT_ACCOUNTS", lambda: self._rpc.get_multiple_accounts(keys,
                            min_context_slot=dependent_floor, retry_min_context=self._retry_min_context))]
                        if known_mints:
                            dependent.append(("MINT_ACCOUNTS", lambda: self._rpc.get_multiple_accounts(known_mints,
                                min_context_slot=dependent_floor, retry_min_context=self._retry_min_context)))
                        pending = [(operation, pool.submit(call)) for operation, call in dependent]
                        results = [read(operation, future.result) for operation, future in pending]
                    explicit = results[0]
                    if known_mints and results[1] is not None:
                        mint_reads.append(results[1])
                else:
                    calls.append(("EXPLICIT_ACCOUNTS", lambda: self._rpc.get_multiple_accounts(keys, min_context_slot=initial)))
                    results = [read(operation, call) for operation, call in calls]
                    inventories.extend(result for result in results[:2] if result is not None)
                    explicit = results[2]
                needed = _needed_mints(self._request, tuple(inventories), explicit)
                if self._concurrent_cohort:
                    # An exhausted/other failed known-mint read stays failed.
                    needed = tuple(key for key in needed if key not in known_mints)
                for start in range(0, len(needed), 100):
                    result = read("MINT_ACCOUNTS", lambda start=start: self._rpc.get_multiple_accounts(needed[start:start+100],
                        min_context_slot=dependent_floor, retry_min_context=self._retry_min_context))
                    if result is not None:
                        mint_reads.append(result)
                slots = [item.context.slot for item in (*inventories, *mint_reads)]
                if explicit is not None:
                    slots.append(explicit.context.slot)
                upper_floor = max((initial, *slots)) if self._concurrent_cohort else initial
                upper = read("END_SLOT", lambda: self._rpc.get_finalized_slot(min_context_slot=upper_floor,
                    retry_min_context=self._retry_min_context))
                if slots and upper is not None and max(slots) <= upper:
                    anchor = read("BLOCK_ANCHOR", lambda: self._rpc.get_finalized_block_anchor(max(slots)))
            genesis_end = read("END_GENESIS", self._rpc.get_genesis_hash)
        return WalletObservation(self._request, self._rpc.profile, started, _utc(self._clock()), genesis,
                                 genesis_end, initial, upper, anchor, tuple(inventories), explicit, tuple(mint_reads), tuple(failures))


@dataclass(frozen=True, slots=True)
class WalletAccountAssessment:
    inventory_coverage: str
    coherent_context: str
    supported_shape: str
    native_account_presence: str
    context_slot: int | None
    tokens: tuple[TokenShapeEvidence, ...]
    mints: tuple[MintShapeEvidence, ...]
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if (self.inventory_coverage not in ("COMPLETE", "INCOMPLETE", "CONTRADICTORY")
                or self.coherent_context not in ("COHERENT", "INCOHERENT", "UNKNOWN")
                or self.supported_shape not in ("SUPPORTED", "UNSUPPORTED", "UNKNOWN")
                or self.native_account_presence not in ("PRESENT", "ABSENT", "UNKNOWN")):
            raise PublicRpcError("INVALID_WALLET_ASSESSMENT")
        if self.context_slot is not None:
            u64(self.context_slot)
        immutable_tuple(self.tokens, TokenShapeEvidence)
        immutable_tuple(self.mints, MintShapeEvidence)
        immutable_tuple(self.reasons, str)


def assess_wallet_accounts(observation: WalletObservation) -> WalletAccountAssessment:
    request, profile = observation.request, observation.profile
    reasons = {f"{failure.operation}:{failure.code}" for failure in observation.failures}
    coverage = "COMPLETE"
    support = "SUPPORTED"
    presence = "UNKNOWN"
    unknown = False
    if observation.observed_genesis != request.genesis_hash:
        reasons.add("GENESIS_UNAVAILABLE_OR_MISMATCH")
        unknown = True
    if observation.observed_genesis_end != request.genesis_hash:
        reasons.add("END_GENESIS_UNAVAILABLE_OR_MISMATCH")
        unknown = True
    programs = tuple(read.program for read in observation.inventories)
    if programs != TOKEN_PROGRAMS:
        coverage = "INCOMPLETE"
        reasons.add("TOKEN_PROGRAM_ENUMERATION_INCOMPLETE")
        unknown = True
    if any(read.wallet != request.wallet for read in observation.inventories):
        coverage = "CONTRADICTORY"
        reasons.add("INVENTORY_WALLET_MISMATCH")
    records: list[tuple[PublicAccount, int]] = []
    by_key: dict[str, list[tuple[PublicAccount, int]]] = {}
    slots = []
    for inventory in observation.inventories:
        slots.append(inventory.context.slot)
        if len(inventory.accounts) > profile.max_inventory_accounts_per_program:
            reasons.add("INVENTORY_ACCOUNT_BUDGET_EXHAUSTED")
            coverage = "INCOMPLETE"
        for value in inventory.accounts:
            records.append((value, inventory.context.slot))
            by_key.setdefault(value.account.pubkey, []).append((value, inventory.context.slot))
    explicit = observation.explicit_read
    expected_keys = (request.wallet, *(item.pubkey for item in request.expected_accounts))
    if explicit is None or explicit.requested_keys != expected_keys:
        reasons.add("EXPLICIT_ACCOUNT_READ_INCOMPLETE_OR_MISBOUND")
        unknown = True
    else:
        slots.append(explicit.context.slot)
        native = explicit.accounts[0]
        presence = "ABSENT" if native is None else "PRESENT"
        if native is None:
            reasons.add("NATIVE_WALLET_ACCOUNT_ABSENT")
            unknown = True
        elif native.account.owner != SYSTEM_PROGRAM_ID or native.executable or native.account.data:
            reasons.add("NATIVE_WALLET_SHAPE_UNSUPPORTED")
            support = "UNSUPPORTED"
        for expected, value in zip(request.expected_accounts, explicit.accounts[1:], strict=True):
            enumerated = by_key.get(expected.pubkey, [])
            same_context = [item for item, slot in enumerated if slot == explicit.context.slot]
            if value is None:
                if same_context:
                    coverage = "CONTRADICTORY"
                    reasons.add("EXPECTED_ABSENCE_CONTRADICTS_ENUMERATION")
                continue
            records.append((value, explicit.context.slot))
            mint = _key(value.account.data[:32]) if len(value.account.data) >= 32 else None
            authority = _key(value.account.data[32:64]) if len(value.account.data) >= 64 else None
            if value.account.owner != expected.program or mint != expected.mint:
                reasons.add("EXPECTED_ACCOUNT_MINT_OR_PROGRAM_MISMATCH")
                support = "UNSUPPORTED"
            relevant_read = next((read for read in observation.inventories if read.program == expected.program), None)
            if relevant_read is not None and relevant_read.context.slot == explicit.context.slot:
                if authority == request.wallet and not same_context:
                    coverage = "CONTRADICTORY"
                    reasons.add("EXPECTED_ACCOUNT_OMITTED_FROM_ENUMERATION")
                elif same_context and any(item != value for item in same_context):
                    coverage = "CONTRADICTORY"
                    reasons.add("SAME_CONTEXT_ACCOUNT_CONFLICT")
    unique_records = tuple(dict.fromkeys(records))
    tokens = tuple(_token_shape(value, slot, request.wallet, observation.schema) for value, slot in unique_records)
    for shape in tokens:
        reasons.update(shape.reasons)
        if shape.reasons:
            support = "UNSUPPORTED"
        if shape.authority is not None and shape.authority != request.wallet and shape.pubkey in by_key:
            coverage = "CONTRADICTORY"
            reasons.add("ENUMERATION_AUTHORITY_CONTRADICTION")
    for key, values in by_key.items():
        if len(values) > 1:
            coverage = "CONTRADICTORY"
            reasons.add("DUPLICATE_OR_CONFLICTING_PROGRAM_ACCOUNT")
    needed = set(_needed_mints(request, observation.inventories, explicit))
    mint_values = {}
    mints = []
    for read in observation.mint_reads:
        slots.append(read.context.slot)
        for key, value in zip(read.requested_keys, read.accounts, strict=True):
            if key in mint_values:
                reasons.add("DUPLICATE_MINT_READ")
                unknown = True
            mint_values[key] = value
            if value is None:
                reasons.add("REQUIRED_MINT_ACCOUNT_ABSENT")
                unknown = True
            else:
                shape = _mint_shape(value, read.context.slot, observation.schema)
                mints.append(shape)
                reasons.update(shape.reasons)
                if shape.reasons:
                    support = "UNSUPPORTED"
    if set(mint_values) != needed:
        reasons.add("REQUIRED_MINT_READS_INCOMPLETE_OR_MISBOUND")
        unknown = True
    for mint, program in ([(token.mint, token.program) for token in tokens if token.mint]
                          + [(item.mint, item.program) for item in request.expected_accounts]):
        value = mint_values.get(mint)
        if value is not None and value.account.owner != program:
            reasons.add("TOKEN_MINT_PROGRAM_MISMATCH")
            support = "UNSUPPORTED"
    contexts = set(slots)
    coherence = "INCOHERENT" if len(contexts) > 1 else "UNKNOWN"
    if len(contexts) > 1:
        reasons.add("ACCOUNT_CONTEXTS_DIFFER")
    anchor, initial, upper = observation.anchor, observation.initial_finalized_slot, observation.finalized_upper_slot
    if initial is not None and upper is not None and (initial < request.min_context_slot or upper < initial
            or any(slot < initial or slot > upper for slot in slots)):
        reasons.add("FINALIZED_CONTEXT_BOUNDS_OR_PROVIDER_MISMATCH")
        unknown = True
    if initial is None or upper is None or anchor is None:
        reasons.add("FINALIZED_CONTEXT_ANCHOR_UNAVAILABLE")
        unknown = True
    else:
        if (initial < request.min_context_slot or upper < initial or any(slot < initial or slot > upper for slot in slots)
                or anchor.slot not in contexts or anchor.provider_fingerprint != profile.fingerprint):
            reasons.add("FINALIZED_CONTEXT_BOUNDS_OR_PROVIDER_MISMATCH")
            unknown = True
        if len(contexts) == 1 and not unknown and coverage == "COMPLETE":
            coherence = "COHERENT"
        block_age = _epoch(observation.observed_at_utc) - anchor.block_time
        if block_age < 0:
            reasons.add("FINALIZED_BLOCK_TIME_IN_FUTURE")
        elif block_age >= profile.finalized_block_freshness_seconds:
            reasons.add("FINALIZED_BLOCK_STALE")
    elapsed = _epoch(observation.observed_at_utc) - _epoch(observation.started_at_utc)
    if elapsed < 0 or elapsed >= profile.observation_timeout_seconds:
        reasons.add("OBSERVATION_CLOCK_OR_DURATION_INVALID")
    if observation.failures:
        unknown = True
    if unknown and support == "SUPPORTED":
        support = "UNKNOWN"
    if any(len(value.account.data) > profile.max_account_bytes for value, _ in records):
        reasons.add("ACCOUNT_BYTES_EXCEED_PROFILE")
        support = "UNKNOWN"
    return WalletAccountAssessment(coverage, coherence, support, presence,
                                   next(iter(contexts)) if coherence == "COHERENT" else None,
                                   tokens, tuple(mints), tuple(sorted(reasons)))


@dataclass(frozen=True, slots=True)
class LedgerAccountEvidence:
    observation: WalletObservation
    assessment: WalletAccountAssessment
    account_facts_usable: bool
    consumer_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.observation) is not WalletObservation or type(self.assessment) is not WalletAccountAssessment:
            raise PublicRpcError("IMMUTABLE_LEDGER_ACCOUNT_EVIDENCE_REQUIRED")
        immutable_tuple(self.consumer_reasons, str)
        if type(self.account_facts_usable) is not bool:
            raise PublicRpcError("INVALID_CONSUMER_DISPOSITION")
        if self.assessment != assess_wallet_accounts(self.observation):
            raise PublicRpcError("ACCOUNT_ASSESSMENT_NOT_DERIVED_FROM_OBSERVATION")
        if self.account_facts_usable and (self.consumer_reasons or self.assessment.reasons
                or self.assessment.inventory_coverage != "COMPLETE"
                or self.assessment.coherent_context != "COHERENT" or self.assessment.supported_shape != "SUPPORTED"):
            raise PublicRpcError("UNSUPPORTED_ACCOUNT_FACTS_CANNOT_BE_USABLE")


def ledger_account_evidence(observation: WalletObservation, *, expected_wallet: str,
                            expected_genesis: str, expected_profile_fingerprint: str,
                            required_min_context_slot: int, now_utc: str) -> LedgerAccountEvidence:
    """Recheck consumer domain/floor/freshness; expose facts, never balances to spend."""
    public_key(expected_wallet)
    block_hash(expected_genesis)
    floor = u64(required_min_context_slot)
    now = _epoch(_utc(now_utc))
    assessment = assess_wallet_accounts(observation)
    reasons = []
    if observation.request.wallet != expected_wallet:
        reasons.append("CONSUMER_WALLET_MISMATCH")
    if (observation.request.genesis_hash != expected_genesis or observation.observed_genesis != expected_genesis
            or observation.observed_genesis_end != expected_genesis):
        reasons.append("CONSUMER_GENESIS_MISMATCH")
    if observation.profile.fingerprint != expected_profile_fingerprint:
        reasons.append("CONSUMER_PROVIDER_PROFILE_MISMATCH")
    if assessment.context_slot is None or assessment.context_slot < floor:
        reasons.append("CONSUMER_CONTEXT_FLOOR_UNSATISFIED")
    age = now - _epoch(observation.observed_at_utc)
    if age < 0:
        reasons.append("CONSUMER_CLOCK_IN_FUTURE")
    elif age >= observation.profile.observation_freshness_seconds:
        reasons.append("CONSUMER_OBSERVATION_STALE")
    if observation.anchor is None or now < observation.anchor.block_time:
        reasons.append("CONSUMER_CHAIN_TIME_UNKNOWN_OR_FUTURE")
    elif now - observation.anchor.block_time >= observation.profile.finalized_block_freshness_seconds:
        reasons.append("CONSUMER_FINALIZED_BLOCK_STALE")
    usable = (not reasons and not assessment.reasons and assessment.inventory_coverage == "COMPLETE"
              and assessment.coherent_context == "COHERENT" and assessment.supported_shape == "SUPPORTED")
    return LedgerAccountEvidence(observation, assessment, usable, tuple(reasons))
