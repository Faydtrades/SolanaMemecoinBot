from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Callable, Mapping, Protocol, Sequence

from solders.pubkey import Pubkey

from .shadow_domain_v0_1 import (
    ExecutionIntentV01,
    IntentSide,
    canonical_json,
    content_fingerprint,
    deterministic_id,
)


MODEL_ID = "P5-SHADOW-VENUE-ROUTE-QUOTE-0001"
SCHEMA_VERSION = "phase5_shadow_venue_route_quote_v0.1"
STATE_SCHEMA_VERSION = "phase5_immutable_venue_state_v0.1"
ROUTE_SCHEMA_VERSION = "phase5_immutable_route_v0.1"
QUOTE_SCHEMA_VERSION = "phase5_immutable_executable_quote_v0.1"
POLICY_SCHEMA_VERSION = "phase5_quote_policy_v0.1"

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPSWAP_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
PUMP_FEES_PROGRAM_ID = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ"
WSOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"

PUMP_CURVE_DISCRIMINATOR = bytes((23, 183, 248, 55, 96, 216, 172, 96))
PUMPSWAP_POOL_DISCRIMINATOR = bytes((241, 154, 109, 4, 17, 177, 109, 188))
PUMP_GLOBAL_DISCRIMINATOR = bytes((167, 232, 232, 177, 200, 108, 114, 127))
PUMPSWAP_GLOBAL_DISCRIMINATOR = bytes((149, 8, 156, 202, 160, 252, 176, 217))
FEE_CONFIG_DISCRIMINATOR = bytes((143, 52, 146, 187, 219, 123, 76, 155))
CANONICAL_POOL_INDEX = 0
ONE_BILLION_TOKEN_BASE_UNITS = 1_000_000_000_000_000
MAX_U64 = (1 << 64) - 1
MAX_U128 = (1 << 128) - 1
BPS_DENOMINATOR = 10_000
IMPACT_PPM_DENOMINATOR = 1_000_000


class VenueKind(StrEnum):
    PUMP_BONDING_CURVE = "PUMP_BONDING_CURVE"
    PUMPSWAP_CANONICAL = "PUMPSWAP_CANONICAL"


class RouteOutcome(StrEnum):
    ROUTE_PUMP_BONDING_CURVE = "ROUTE_PUMP_BONDING_CURVE"
    ROUTE_PUMPSWAP_CANONICAL = "ROUTE_PUMPSWAP_CANONICAL"
    NO_ROUTE_MIGRATION_PENDING = "NO_ROUTE_MIGRATION_PENDING"
    NO_ROUTE_UNSUPPORTED = "NO_ROUTE_UNSUPPORTED"
    NO_ROUTE_INVALID_STATE = "NO_ROUTE_INVALID_STATE"
    NO_ROUTE_AMBIGUOUS = "NO_ROUTE_AMBIGUOUS"


class VenueStateError(ValueError):
    pass


class SnapshotCoherenceError(VenueStateError):
    pass


class QuoteError(ValueError):
    pass


def _strict_int(label: str, value: int, *, minimum: int = 0, maximum: int = MAX_U128) -> int:
    if type(value) is not int:
        raise TypeError(f"{label} must be an integer")
    if value < minimum or value > maximum:
        raise ValueError(f"{label} is outside [{minimum}, {maximum}]")
    return value


def _required_text(label: str, value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} is required")
    return value


def _pubkey(value: str) -> Pubkey:
    try:
        return Pubkey.from_string(_required_text("pubkey", value))
    except Exception as exc:
        raise VenueStateError(f"invalid public key: {value}") from exc


def _pubkey_text(raw: bytes) -> str:
    if len(raw) != 32:
        raise VenueStateError("public-key field must be 32 bytes")
    return str(Pubkey.from_bytes(raw))


def _derive_with_bump(program: str, seeds: Sequence[bytes]) -> tuple[str, int]:
    address, bump = Pubkey.find_program_address(list(seeds), _pubkey(program))
    return str(address), bump


def _derive(program: str, seeds: Sequence[bytes]) -> str:
    return _derive_with_bump(program, seeds)[0]


def derive_bonding_curve_pda(mint: str) -> str:
    return _derive(PUMP_PROGRAM_ID, (b"bonding-curve", bytes(_pubkey(mint))))


def derive_pump_pool_authority(mint: str) -> str:
    return _derive(PUMP_PROGRAM_ID, (b"pool-authority", bytes(_pubkey(mint))))


def derive_pumpswap_pool_pda(
    mint: str,
    *,
    creator: str | None = None,
    quote_mint: str = WSOL_MINT,
    index: int = CANONICAL_POOL_INDEX,
) -> str:
    _strict_int("index", index, maximum=65535)
    owner = creator or derive_pump_pool_authority(mint)
    return _derive(
        PUMPSWAP_PROGRAM_ID,
        (
            b"pool",
            index.to_bytes(2, "little"),
            bytes(_pubkey(owner)),
            bytes(_pubkey(mint)),
            bytes(_pubkey(quote_mint)),
        ),
    )


def derive_pumpswap_pool_pda_with_bump(
    mint: str,
    *,
    creator: str | None = None,
    quote_mint: str = WSOL_MINT,
    index: int = CANONICAL_POOL_INDEX,
) -> tuple[str, int]:
    _strict_int("index", index, maximum=65535)
    owner = creator or derive_pump_pool_authority(mint)
    return _derive_with_bump(
        PUMPSWAP_PROGRAM_ID,
        (
            b"pool",
            index.to_bytes(2, "little"),
            bytes(_pubkey(owner)),
            bytes(_pubkey(mint)),
            bytes(_pubkey(quote_mint)),
        ),
    )


def derive_pump_fee_config_pda() -> str:
    return _derive(
        PUMP_FEES_PROGRAM_ID,
        (b"fee_config", bytes(_pubkey(PUMP_PROGRAM_ID))),
    )


def derive_pumpswap_fee_config_pda() -> str:
    return _derive(
        PUMP_FEES_PROGRAM_ID,
        (b"fee_config", bytes(_pubkey(PUMPSWAP_PROGRAM_ID))),
    )


def derive_pump_global_pda() -> str:
    return _derive(PUMP_PROGRAM_ID, (b"global",))


def derive_pumpswap_global_pda() -> str:
    return _derive(PUMPSWAP_PROGRAM_ID, (b"global_config",))


def account_data_fingerprint(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True, slots=True)
class AccountEvidenceV01:
    pubkey: str
    owner: str
    data_sha256: str
    data_length: int
    context_slot: int

    def __post_init__(self) -> None:
        _pubkey(self.pubkey)
        _pubkey(self.owner)
        if len(self.data_sha256) != 64:
            raise ValueError("data_sha256 must be a SHA-256 hex digest")
        int(self.data_sha256, 16)
        _strict_int("data_length", self.data_length, maximum=MAX_U64)
        _strict_int("context_slot", self.context_slot, maximum=MAX_U64)

    def payload(self) -> dict[str, Any]:
        return {
            "pubkey": self.pubkey,
            "owner": self.owner,
            "data_sha256": self.data_sha256,
            "data_length": self.data_length,
            "context_slot": self.context_slot,
        }


@dataclass(frozen=True, slots=True)
class RpcAccountV01:
    pubkey: str
    owner: str
    data: bytes

    def __post_init__(self) -> None:
        _pubkey(self.pubkey)
        _pubkey(self.owner)
        if not isinstance(self.data, bytes):
            raise TypeError("RPC account data must be bytes")

    def evidence(self, slot: int) -> AccountEvidenceV01:
        return AccountEvidenceV01(
            pubkey=self.pubkey,
            owner=self.owner,
            data_sha256=account_data_fingerprint(self.data),
            data_length=len(self.data),
            context_slot=slot,
        )


@dataclass(frozen=True, slots=True)
class RpcAccountBatchV01:
    context_slot: int
    accounts: tuple[RpcAccountV01 | None, ...]

    def __post_init__(self) -> None:
        _strict_int("context_slot", self.context_slot, maximum=MAX_U64)
        if not isinstance(self.accounts, tuple):
            raise TypeError("accounts must be a tuple")


class ReadOnlyRpcV01(Protocol):
    def get_multiple_accounts(
        self,
        pubkeys: tuple[str, ...],
        *,
        min_context_slot: int | None = None,
    ) -> RpcAccountBatchV01: ...


@dataclass(frozen=True, slots=True)
class CoherentReadV01:
    primary: RpcAccountBatchV01
    dependent: RpcAccountBatchV01
    verification: RpcAccountBatchV01
    attempts: int

    @property
    def slot_min(self) -> int:
        return min(
            self.primary.context_slot,
            self.dependent.context_slot,
            self.verification.context_slot,
        )

    @property
    def slot_max(self) -> int:
        return max(
            self.primary.context_slot,
            self.dependent.context_slot,
            self.verification.context_slot,
        )


def coherent_dependent_read(
    rpc: ReadOnlyRpcV01,
    primary_pubkeys: tuple[str, ...],
    dependent_pubkeys: Callable[[RpcAccountBatchV01], tuple[str, ...]],
    *,
    max_attempts: int = 3,
) -> CoherentReadV01:
    _strict_int("max_attempts", max_attempts, minimum=1, maximum=10)
    previous_slot = -1
    for attempt in range(1, max_attempts + 1):
        first = rpc.get_multiple_accounts(
            primary_pubkeys,
            min_context_slot=None if previous_slot < 0 else previous_slot,
        )
        if first.context_slot < previous_slot:
            raise SnapshotCoherenceError("primary context slot regressed")
        dependencies = dependent_pubkeys(first)
        second = rpc.get_multiple_accounts(
            dependencies,
            min_context_slot=first.context_slot,
        )
        if second.context_slot < first.context_slot:
            raise SnapshotCoherenceError("dependent context slot regressed")
        third = rpc.get_multiple_accounts(
            primary_pubkeys,
            min_context_slot=second.context_slot,
        )
        if third.context_slot < second.context_slot:
            raise SnapshotCoherenceError("verification context slot regressed")
        first_fingerprints = tuple(
            None if item is None else (item.pubkey, item.owner, account_data_fingerprint(item.data))
            for item in first.accounts
        )
        third_fingerprints = tuple(
            None if item is None else (item.pubkey, item.owner, account_data_fingerprint(item.data))
            for item in third.accounts
        )
        if first_fingerprints == third_fingerprints:
            return CoherentReadV01(first, second, third, attempt)
        previous_slot = third.context_slot
    raise SnapshotCoherenceError("route-defining state changed during bounded read")


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def take(self, size: int) -> bytes:
        end = self.offset + size
        if end > len(self.data):
            raise VenueStateError("account data is truncated")
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def u8(self) -> int:
        return self.take(1)[0]

    def boolean(self) -> bool:
        value = self.u8()
        if value not in (0, 1):
            raise VenueStateError("invalid Borsh boolean")
        return bool(value)

    def u16(self) -> int:
        return int.from_bytes(self.take(2), "little")

    def u32(self) -> int:
        return int.from_bytes(self.take(4), "little")

    def u64(self) -> int:
        return int.from_bytes(self.take(8), "little")

    def u128(self) -> int:
        return int.from_bytes(self.take(16), "little")

    def i128(self) -> int:
        return int.from_bytes(self.take(16), "little", signed=True)

    def pubkey(self) -> str:
        return _pubkey_text(self.take(32))

    def discriminator(self, expected: bytes) -> None:
        if self.take(8) != expected:
            raise VenueStateError("account discriminator mismatch")


@dataclass(frozen=True, slots=True)
class FeesV01:
    lp_fee_bps: int
    protocol_fee_bps: int
    creator_fee_bps: int

    def __post_init__(self) -> None:
        for label, value in (
            ("lp_fee_bps", self.lp_fee_bps),
            ("protocol_fee_bps", self.protocol_fee_bps),
            ("creator_fee_bps", self.creator_fee_bps),
        ):
            _strict_int(label, value, maximum=BPS_DENOMINATOR)
        if self.total_bps >= BPS_DENOMINATOR:
            raise VenueStateError("total fee bps must be below 10000")

    @property
    def total_bps(self) -> int:
        return self.lp_fee_bps + self.protocol_fee_bps + self.creator_fee_bps

    def payload(self) -> dict[str, int]:
        return {
            "lp_fee_bps": self.lp_fee_bps,
            "protocol_fee_bps": self.protocol_fee_bps,
            "creator_fee_bps": self.creator_fee_bps,
        }


@dataclass(frozen=True, slots=True)
class FeeTierV01:
    market_cap_threshold: int
    fees: FeesV01

    def __post_init__(self) -> None:
        _strict_int("market_cap_threshold", self.market_cap_threshold)

    def payload(self) -> dict[str, Any]:
        return {"market_cap_threshold": self.market_cap_threshold, "fees": self.fees.payload()}


@dataclass(frozen=True, slots=True)
class FeeConfigV01:
    pubkey: str
    admin: str
    flat_fees: FeesV01
    fee_tiers: tuple[FeeTierV01, ...]
    stable_fee_tiers: tuple[FeeTierV01, ...]
    evidence: AccountEvidenceV01
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        _pubkey(self.pubkey)
        _pubkey(self.admin)
        if not self.fee_tiers:
            raise VenueStateError("dynamic fee config requires at least one fee tier")
        thresholds = tuple(tier.market_cap_threshold for tier in self.fee_tiers)
        if thresholds != tuple(sorted(set(thresholds))):
            raise VenueStateError("fee tiers must have unique ascending thresholds")
        object.__setattr__(self, "fingerprint", content_fingerprint(self.payload()))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": "pump_fee_config_v0.1",
            "pubkey": self.pubkey,
            "admin": self.admin,
            "flat_fees": self.flat_fees.payload(),
            "fee_tiers": [tier.payload() for tier in self.fee_tiers],
            "stable_fee_tiers": [tier.payload() for tier in self.stable_fee_tiers],
            "evidence": self.evidence.payload(),
        }

    def select(self, market_cap: int) -> tuple[int, FeeTierV01]:
        _strict_int("market_cap", market_cap)
        selected_index = 0
        for index, tier in enumerate(self.fee_tiers):
            if market_cap >= tier.market_cap_threshold:
                selected_index = index
            else:
                break
        return selected_index, self.fee_tiers[selected_index]


def _read_fees(reader: _Reader) -> FeesV01:
    return FeesV01(reader.u64(), reader.u64(), reader.u64())


def _read_fee_tiers(reader: _Reader) -> tuple[FeeTierV01, ...]:
    count = reader.u32()
    if count > 128:
        raise VenueStateError("fee tier vector is unreasonably large")
    return tuple(FeeTierV01(reader.u128(), _read_fees(reader)) for _ in range(count))


def decode_fee_config(account: RpcAccountV01, slot: int, expected_pubkey: str) -> FeeConfigV01:
    if account.pubkey != expected_pubkey or account.owner != PUMP_FEES_PROGRAM_ID:
        raise VenueStateError("fee config identity or owner mismatch")
    reader = _Reader(account.data)
    reader.discriminator(FEE_CONFIG_DISCRIMINATOR)
    bump = reader.u8()
    expected_program = (
        PUMP_PROGRAM_ID
        if expected_pubkey == derive_pump_fee_config_pda()
        else PUMPSWAP_PROGRAM_ID
        if expected_pubkey == derive_pumpswap_fee_config_pda()
        else None
    )
    if expected_program is None:
        raise VenueStateError("unknown fee-config PDA domain")
    _, expected_bump = _derive_with_bump(
        PUMP_FEES_PROGRAM_ID,
        (b"fee_config", bytes(_pubkey(expected_program))),
    )
    if bump != expected_bump:
        raise VenueStateError("fee-config PDA bump mismatch")
    admin = reader.pubkey()
    flat = _read_fees(reader)
    tiers = _read_fee_tiers(reader)
    stable = _read_fee_tiers(reader)
    return FeeConfigV01(
        pubkey=account.pubkey,
        admin=admin,
        flat_fees=flat,
        fee_tiers=tiers,
        stable_fee_tiers=stable,
        evidence=account.evidence(slot),
    )


@dataclass(frozen=True, slots=True)
class PumpGlobalV01:
    pubkey: str
    fee_basis_points: int
    creator_fee_basis_points: int
    evidence: AccountEvidenceV01


def decode_pump_global(account: RpcAccountV01, slot: int) -> PumpGlobalV01:
    if account.pubkey != derive_pump_global_pda() or account.owner != PUMP_PROGRAM_ID:
        raise VenueStateError("Pump global identity or owner mismatch")
    reader = _Reader(account.data)
    reader.discriminator(PUMP_GLOBAL_DISCRIMINATOR)
    reader.boolean()
    reader.pubkey()
    reader.pubkey()
    for _ in range(4):
        reader.u64()
    fee_bps = reader.u64()
    reader.pubkey()
    reader.boolean()
    reader.u64()
    creator_bps = reader.u64()
    return PumpGlobalV01(account.pubkey, fee_bps, creator_bps, account.evidence(slot))


@dataclass(frozen=True, slots=True)
class PumpSwapGlobalV01:
    pubkey: str
    lp_fee_basis_points: int
    protocol_fee_basis_points: int
    coin_creator_fee_basis_points: int
    disable_flags: int
    evidence: AccountEvidenceV01


def decode_pumpswap_global(account: RpcAccountV01, slot: int) -> PumpSwapGlobalV01:
    if account.pubkey != derive_pumpswap_global_pda() or account.owner != PUMPSWAP_PROGRAM_ID:
        raise VenueStateError("PumpSwap global identity or owner mismatch")
    reader = _Reader(account.data)
    reader.discriminator(PUMPSWAP_GLOBAL_DISCRIMINATOR)
    reader.pubkey()
    lp = reader.u64()
    protocol = reader.u64()
    flags = reader.u8()
    for _ in range(8):
        reader.pubkey()
    creator = reader.u64()
    FeesV01(lp, protocol, creator)
    return PumpSwapGlobalV01(account.pubkey, lp, protocol, creator, flags, account.evidence(slot))


@dataclass(frozen=True, slots=True)
class PumpCurveDecodedV01:
    virtual_token_reserves: int
    virtual_quote_reserves: int
    real_token_reserves: int
    real_quote_reserves: int
    token_total_supply: int
    complete: bool
    creator: str
    is_mayhem_mode: bool
    is_cashback_coin: bool
    quote_mint: str


def decode_pump_curve(account: RpcAccountV01, mint: str) -> PumpCurveDecodedV01:
    if account.pubkey != derive_bonding_curve_pda(mint) or account.owner != PUMP_PROGRAM_ID:
        raise VenueStateError("Pump curve PDA or owner mismatch")
    reader = _Reader(account.data)
    reader.discriminator(PUMP_CURVE_DISCRIMINATOR)
    decoded = PumpCurveDecodedV01(
        reader.u64(), reader.u64(), reader.u64(), reader.u64(), reader.u64(),
        reader.boolean(), reader.pubkey(), reader.boolean(), reader.boolean(), reader.pubkey(),
    )
    if decoded.complete != (decoded.real_token_reserves == 0):
        raise VenueStateError("Pump completion flag conflicts with real token reserves")
    if decoded.quote_mint not in (WSOL_MINT, SYSTEM_PROGRAM_ID):
        raise VenueStateError("Pump v0.1 supports only SOL-paired curves")
    if not decoded.complete and (
        decoded.virtual_token_reserves == 0
        or decoded.virtual_quote_reserves == 0
        or decoded.real_token_reserves == 0
    ):
        raise VenueStateError("active Pump curve has impossible reserves")
    return decoded


@dataclass(frozen=True, slots=True)
class TokenAccountDecodedV01:
    mint: str
    authority: str
    amount: int


def decode_token_account(account: RpcAccountV01, expected_mint: str) -> TokenAccountDecodedV01:
    if account.owner not in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
        raise VenueStateError("token account owner program is unsupported")
    if len(account.data) < 165:
        raise VenueStateError("token account is truncated")
    mint = _pubkey_text(account.data[0:32])
    authority = _pubkey_text(account.data[32:64])
    amount = int.from_bytes(account.data[64:72], "little")
    if mint != expected_mint or account.data[108] not in (1, 2):
        raise VenueStateError("token account mint or state mismatch")
    return TokenAccountDecodedV01(mint, authority, amount)


@dataclass(frozen=True, slots=True)
class MintDecodedV01:
    supply: int
    decimals: int
    token_program: str


def decode_mint_account(account: RpcAccountV01, expected_mint: str) -> MintDecodedV01:
    if account.pubkey != expected_mint or account.owner not in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
        raise VenueStateError("mint identity or token program mismatch")
    if len(account.data) < 82 or account.data[45] != 1:
        raise VenueStateError("mint account is truncated or uninitialized")
    return MintDecodedV01(
        int.from_bytes(account.data[36:44], "little"), account.data[44], account.owner
    )


@dataclass(frozen=True, slots=True)
class PumpSwapPoolDecodedV01:
    pool_bump: int
    index: int
    creator: str
    base_mint: str
    quote_mint: str
    lp_mint: str
    pool_base_token_account: str
    pool_quote_token_account: str
    lp_supply: int
    coin_creator: str
    is_mayhem_mode: bool
    is_cashback_coin: bool
    virtual_quote_reserves: int


def decode_pumpswap_pool(account: RpcAccountV01, mint: str) -> PumpSwapPoolDecodedV01:
    if account.owner != PUMPSWAP_PROGRAM_ID:
        raise VenueStateError("PumpSwap pool owner mismatch")
    reader = _Reader(account.data)
    reader.discriminator(PUMPSWAP_POOL_DISCRIMINATOR)
    decoded = PumpSwapPoolDecodedV01(
        reader.u8(), reader.u16(), reader.pubkey(), reader.pubkey(), reader.pubkey(),
        reader.pubkey(), reader.pubkey(), reader.pubkey(), reader.u64(), reader.pubkey(),
        reader.boolean(), reader.boolean(), reader.i128(),
    )
    canonical_creator = derive_pump_pool_authority(mint)
    expected_pool, expected_bump = derive_pumpswap_pool_pda_with_bump(mint)
    if (
        decoded.pool_bump != expected_bump
        or decoded.index != CANONICAL_POOL_INDEX
        or decoded.creator != canonical_creator
        or decoded.base_mint != mint
        or decoded.quote_mint != WSOL_MINT
        or account.pubkey != expected_pool
    ):
        raise VenueStateError("PumpSwap pool is not the canonical migrated pool")
    return decoded


@dataclass(frozen=True, slots=True)
class PumpBondingCurveStateV01:
    intent_id: str
    mint: str
    bonding_curve: str
    decoded: PumpCurveDecodedV01
    base_token_program: str
    global_state: PumpGlobalV01
    fee_config: FeeConfigV01
    observed_at_us: int
    slot_min: int
    slot_max: int
    accounts: tuple[AccountEvidenceV01, ...]
    schema_version: str = field(init=False, default=STATE_SCHEMA_VERSION)
    venue: VenueKind = field(init=False, default=VenueKind.PUMP_BONDING_CURVE)
    fingerprint: str = field(init=False)
    state_id: str = field(init=False)

    def __post_init__(self) -> None:
        _required_text("intent_id", self.intent_id)
        _pubkey(self.mint)
        if self.bonding_curve != derive_bonding_curve_pda(self.mint):
            raise VenueStateError("bonding curve relationship mismatch")
        if self.base_token_program not in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
            raise VenueStateError("unsupported base token program")
        _strict_int("observed_at_us", self.observed_at_us, maximum=MAX_U64)
        _strict_int("slot_min", self.slot_min, maximum=MAX_U64)
        _strict_int("slot_max", self.slot_max, maximum=MAX_U64)
        if self.slot_max < self.slot_min or not self.accounts:
            raise VenueStateError("invalid slot span or empty evidence")
        account_map = {item.pubkey: item for item in self.accounts}
        required = {
            self.bonding_curve,
            self.mint,
            self.global_state.pubkey,
            self.fee_config.pubkey,
        }
        if len(account_map) != len(self.accounts) or set(account_map) != required:
            raise VenueStateError("Pump evidence account set is incomplete or duplicated")
        evidence_slots = tuple(item.context_slot for item in self.accounts)
        if min(evidence_slots) != self.slot_min or max(evidence_slots) != self.slot_max:
            raise VenueStateError("Pump evidence slots conflict with declared span")
        if (
            account_map[self.global_state.pubkey] != self.global_state.evidence
            or account_map[self.fee_config.pubkey] != self.fee_config.evidence
        ):
            raise VenueStateError("Pump global or fee evidence is inconsistent")
        fp = content_fingerprint(self.payload())
        object.__setattr__(self, "fingerprint", fp)
        object.__setattr__(self, "state_id", deterministic_id("P5VS", STATE_SCHEMA_VERSION, fp))

    @property
    def fee_mint_supply(self) -> int:
        """Historical fee basis; explicitly versioned LIVE states may override."""
        return self.decoded.token_total_supply

    @property
    def executable(self) -> bool:
        return not self.decoded.complete and self.decoded.real_token_reserves > 0

    @property
    def completed(self) -> bool:
        return self.decoded.complete and self.decoded.real_token_reserves == 0

    def payload(self) -> dict[str, Any]:
        d = self.decoded
        return {
            "schema_version": self.schema_version,
            "intent_id": self.intent_id,
            "venue": self.venue.value,
            "mint": self.mint,
            "bonding_curve": self.bonding_curve,
            "virtual_token_reserves": d.virtual_token_reserves,
            "virtual_quote_reserves": d.virtual_quote_reserves,
            "real_token_reserves": d.real_token_reserves,
            "real_quote_reserves": d.real_quote_reserves,
            "token_total_supply": d.token_total_supply,
            "complete": d.complete,
            "creator": d.creator,
            "is_mayhem_mode": d.is_mayhem_mode,
            "is_cashback_coin": d.is_cashback_coin,
            "quote_mint": d.quote_mint,
            "base_token_program": self.base_token_program,
            "global": {
                "pubkey": self.global_state.pubkey,
                "fee_basis_points": self.global_state.fee_basis_points,
                "creator_fee_basis_points": self.global_state.creator_fee_basis_points,
                "evidence": self.global_state.evidence.payload(),
            },
            "fee_config": self.fee_config.payload(),
            "observed_at_us": self.observed_at_us,
            "slot_min": self.slot_min,
            "slot_max": self.slot_max,
            "accounts": [item.payload() for item in self.accounts],
        }


@dataclass(frozen=True, slots=True)
class PumpSwapStateV01:
    intent_id: str
    mint: str
    pool: str
    decoded: PumpSwapPoolDecodedV01
    raw_base_reserve: int
    raw_quote_reserve: int
    effective_quote_reserve: int
    base_mint_supply: int
    base_token_program: str
    quote_token_program: str
    global_state: PumpSwapGlobalV01
    fee_config: FeeConfigV01
    observed_at_us: int
    slot_min: int
    slot_max: int
    accounts: tuple[AccountEvidenceV01, ...]
    schema_version: str = field(init=False, default=STATE_SCHEMA_VERSION)
    venue: VenueKind = field(init=False, default=VenueKind.PUMPSWAP_CANONICAL)
    fingerprint: str = field(init=False)
    state_id: str = field(init=False)

    def __post_init__(self) -> None:
        _required_text("intent_id", self.intent_id)
        if self.pool != derive_pumpswap_pool_pda(self.mint):
            raise VenueStateError("canonical pool PDA relationship mismatch")
        for label, value in (
            ("raw_base_reserve", self.raw_base_reserve),
            ("raw_quote_reserve", self.raw_quote_reserve),
            ("base_mint_supply", self.base_mint_supply),
        ):
            _strict_int(label, value, maximum=MAX_U64)
        if self.raw_base_reserve == 0 or self.raw_quote_reserve == 0:
            raise VenueStateError("executable PumpSwap reserves must be positive")
        if self.effective_quote_reserve != self.raw_quote_reserve + self.decoded.virtual_quote_reserves:
            raise VenueStateError("effective quote reserve relationship mismatch")
        if self.effective_quote_reserve <= 0 or self.effective_quote_reserve > MAX_U128:
            raise VenueStateError("effective quote reserves must be positive u128")
        if self.base_token_program not in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
            raise VenueStateError("unsupported base token program")
        if self.quote_token_program != TOKEN_PROGRAM_ID:
            raise VenueStateError("WSOL quote must use the legacy token program")
        if self.global_state.disable_flags & ((1 << 3) | (1 << 4)):
            raise VenueStateError("PumpSwap buy or sell is disabled")
        _strict_int("observed_at_us", self.observed_at_us, maximum=MAX_U64)
        if self.slot_max < self.slot_min or not self.accounts:
            raise VenueStateError("invalid slot span or empty evidence")
        if (
            self.decoded.index != CANONICAL_POOL_INDEX
            or self.decoded.creator != derive_pump_pool_authority(self.mint)
            or self.decoded.base_mint != self.mint
            or self.decoded.quote_mint != WSOL_MINT
        ):
            raise VenueStateError("decoded pool identity conflicts with canonical state")
        account_map = {item.pubkey: item for item in self.accounts}
        required = {
            self.pool,
            self.decoded.pool_base_token_account,
            self.decoded.pool_quote_token_account,
            self.mint,
            self.global_state.pubkey,
            self.fee_config.pubkey,
        }
        if len(account_map) != len(self.accounts) or set(account_map) != required:
            raise VenueStateError("PumpSwap evidence account set is incomplete or duplicated")
        evidence_slots = tuple(item.context_slot for item in self.accounts)
        if min(evidence_slots) != self.slot_min or max(evidence_slots) != self.slot_max:
            raise VenueStateError("PumpSwap evidence slots conflict with declared span")
        if (
            account_map[self.global_state.pubkey] != self.global_state.evidence
            or account_map[self.fee_config.pubkey] != self.fee_config.evidence
        ):
            raise VenueStateError("PumpSwap global or fee evidence is inconsistent")
        fp = content_fingerprint(self.payload())
        object.__setattr__(self, "fingerprint", fp)
        object.__setattr__(self, "state_id", deterministic_id("P5VS", STATE_SCHEMA_VERSION, fp))

    def payload(self) -> dict[str, Any]:
        d = self.decoded
        return {
            "schema_version": self.schema_version,
            "intent_id": self.intent_id,
            "venue": self.venue.value,
            "mint": self.mint,
            "pool": self.pool,
            "index": d.index,
            "creator": d.creator,
            "base_mint": d.base_mint,
            "quote_mint": d.quote_mint,
            "lp_mint": d.lp_mint,
            "pool_base_token_account": d.pool_base_token_account,
            "pool_quote_token_account": d.pool_quote_token_account,
            "lp_supply": d.lp_supply,
            "coin_creator": d.coin_creator,
            "is_mayhem_mode": d.is_mayhem_mode,
            "is_cashback_coin": d.is_cashback_coin,
            "raw_base_reserve": self.raw_base_reserve,
            "raw_quote_reserve": self.raw_quote_reserve,
            "virtual_quote_reserves": d.virtual_quote_reserves,
            "effective_quote_reserve": self.effective_quote_reserve,
            "base_mint_supply": self.base_mint_supply,
            "base_token_program": self.base_token_program,
            "quote_token_program": self.quote_token_program,
            "global": {
                "pubkey": self.global_state.pubkey,
                "lp_fee_basis_points": self.global_state.lp_fee_basis_points,
                "protocol_fee_basis_points": self.global_state.protocol_fee_basis_points,
                "coin_creator_fee_basis_points": self.global_state.coin_creator_fee_basis_points,
                "disable_flags": self.global_state.disable_flags,
                "evidence": self.global_state.evidence.payload(),
            },
            "fee_config": self.fee_config.payload(),
            "observed_at_us": self.observed_at_us,
            "slot_min": self.slot_min,
            "slot_max": self.slot_max,
            "accounts": [item.payload() for item in self.accounts],
        }


VenueStateV01 = PumpBondingCurveStateV01 | PumpSwapStateV01


def build_pump_state(
    intent: ExecutionIntentV01,
    curve_account: RpcAccountV01,
    mint_account: RpcAccountV01,
    global_account: RpcAccountV01,
    fee_account: RpcAccountV01,
    *,
    slot_min: int,
    slot_max: int,
    observed_at_us: int,
    account_slots: tuple[int, int, int, int] | None = None,
) -> PumpBondingCurveStateV01:
    if intent.mint != curve_account.pubkey and curve_account.pubkey != derive_bonding_curve_pda(intent.mint):
        raise VenueStateError("intent mint and curve mismatch")
    curve = decode_pump_curve(curve_account, intent.mint)
    mint = decode_mint_account(mint_account, intent.mint)
    if curve.token_total_supply != mint.supply:
        raise VenueStateError("curve total supply conflicts with mint supply")
    if account_slots is None:
        if slot_min != slot_max:
            raise SnapshotCoherenceError("multi-slot evidence requires per-account slots")
        account_slots = (slot_max,) * 4
    if min(account_slots) != slot_min or max(account_slots) != slot_max:
        raise SnapshotCoherenceError("account slots do not match declared slot span")
    global_state = decode_pump_global(global_account, account_slots[2])
    fee_config = decode_fee_config(fee_account, account_slots[3], derive_pump_fee_config_pda())
    accounts = tuple(
        item.evidence(item_slot)
        for item, item_slot in zip(
            (curve_account, mint_account, global_account, fee_account),
            account_slots,
            strict=True,
        )
    )
    return PumpBondingCurveStateV01(
        intent.intent_id, intent.mint, curve_account.pubkey, curve, mint.token_program,
        global_state, fee_config, observed_at_us, slot_min, slot_max, accounts,
    )


def build_pumpswap_state(
    intent: ExecutionIntentV01,
    pool_account: RpcAccountV01,
    base_vault_account: RpcAccountV01,
    quote_vault_account: RpcAccountV01,
    base_mint_account: RpcAccountV01,
    global_account: RpcAccountV01,
    fee_account: RpcAccountV01,
    *,
    slot_min: int,
    slot_max: int,
    observed_at_us: int,
    account_slots: tuple[int, int, int, int, int, int] | None = None,
) -> PumpSwapStateV01:
    pool = decode_pumpswap_pool(pool_account, intent.mint)
    if base_vault_account.pubkey != pool.pool_base_token_account or quote_vault_account.pubkey != pool.pool_quote_token_account:
        raise VenueStateError("pool vault identity mismatch")
    base_vault = decode_token_account(base_vault_account, intent.mint)
    quote_vault = decode_token_account(quote_vault_account, WSOL_MINT)
    if base_vault.authority != pool_account.pubkey or quote_vault.authority != pool_account.pubkey:
        raise VenueStateError("pool vault authority relationship mismatch")
    mint = decode_mint_account(base_mint_account, intent.mint)
    if account_slots is None:
        if slot_min != slot_max:
            raise SnapshotCoherenceError("multi-slot evidence requires per-account slots")
        account_slots = (slot_max,) * 6
    if min(account_slots) != slot_min or max(account_slots) != slot_max:
        raise SnapshotCoherenceError("account slots do not match declared slot span")
    global_state = decode_pumpswap_global(global_account, account_slots[4])
    fee_config = decode_fee_config(fee_account, account_slots[5], derive_pumpswap_fee_config_pda())
    effective = quote_vault.amount + pool.virtual_quote_reserves
    accounts = tuple(
        item.evidence(item_slot)
        for item, item_slot in zip(
            (
                pool_account, base_vault_account, quote_vault_account, base_mint_account,
                global_account, fee_account,
            ),
            account_slots,
            strict=True,
        )
    )
    return PumpSwapStateV01(
        intent.intent_id, intent.mint, pool_account.pubkey, pool, base_vault.amount,
        quote_vault.amount, effective, mint.supply, mint.token_program,
        quote_vault_account.owner, global_state, fee_config, observed_at_us,
        slot_min, slot_max, accounts,
    )


@dataclass(frozen=True, slots=True)
class RouteDecisionV01:
    intent_id: str
    outcome: RouteOutcome
    state_fingerprints: tuple[str, ...]
    selected_state_id: str | None
    reason_code: str
    schema_version: str = field(init=False, default=ROUTE_SCHEMA_VERSION)
    fingerprint: str = field(init=False)
    route_id: str = field(init=False)

    def __post_init__(self) -> None:
        _required_text("intent_id", self.intent_id)
        _required_text("reason_code", self.reason_code)
        if self.outcome in (
            RouteOutcome.ROUTE_PUMP_BONDING_CURVE,
            RouteOutcome.ROUTE_PUMPSWAP_CANONICAL,
        ) and self.selected_state_id is None:
            raise VenueStateError("executable route requires selected state")
        if not self.executable and self.selected_state_id is not None:
            raise VenueStateError("non-executable route cannot select a venue state")
        if len(set(self.state_fingerprints)) != len(self.state_fingerprints):
            raise VenueStateError("route state fingerprints must be unique")
        fp = content_fingerprint(self.payload())
        object.__setattr__(self, "fingerprint", fp)
        object.__setattr__(self, "route_id", deterministic_id("P5RT", ROUTE_SCHEMA_VERSION, fp))

    @property
    def executable(self) -> bool:
        return self.outcome in (
            RouteOutcome.ROUTE_PUMP_BONDING_CURVE,
            RouteOutcome.ROUTE_PUMPSWAP_CANONICAL,
        )

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "intent_id": self.intent_id,
            "outcome": self.outcome.value,
            "state_fingerprints": list(self.state_fingerprints),
            "selected_state_id": self.selected_state_id,
            "reason_code": self.reason_code,
        }


def decide_route(
    intent: ExecutionIntentV01,
    curve: PumpBondingCurveStateV01 | None,
    pool: PumpSwapStateV01 | None,
) -> RouteDecisionV01:
    states = tuple(item for item in (curve, pool) if item is not None)
    if any(item.intent_id != intent.intent_id or item.mint != intent.mint for item in states):
        outcome, selected, reason = RouteOutcome.NO_ROUTE_INVALID_STATE, None, "INTENT_STATE_LINEAGE_MISMATCH"
    elif curve is None:
        outcome, selected, reason = RouteOutcome.NO_ROUTE_UNSUPPORTED, None, "VERIFIED_PUMP_CURVE_REQUIRED"
    elif curve.executable and pool is not None:
        outcome, selected, reason = RouteOutcome.NO_ROUTE_AMBIGUOUS, None, "ACTIVE_CURVE_AND_CANONICAL_POOL_CONFLICT"
    elif curve.executable:
        outcome, selected, reason = RouteOutcome.ROUTE_PUMP_BONDING_CURVE, curve.state_id, "ACTIVE_VERIFIED_PUMP_CURVE"
    elif curve.completed and pool is None:
        outcome, selected, reason = RouteOutcome.NO_ROUTE_MIGRATION_PENDING, None, "COMPLETED_CURVE_CANONICAL_POOL_ABSENT"
    elif curve.completed and pool is not None:
        outcome, selected, reason = RouteOutcome.ROUTE_PUMPSWAP_CANONICAL, pool.state_id, "COMPLETED_CURVE_VERIFIED_CANONICAL_POOL"
    else:
        outcome, selected, reason = RouteOutcome.NO_ROUTE_INVALID_STATE, None, "UNPROVEN_VENUE_STATE"
    return RouteDecisionV01(
        intent.intent_id, outcome, tuple(item.fingerprint for item in states), selected, reason
    )


@dataclass(frozen=True, slots=True)
class QuotePolicyV01:
    slippage_bps: int
    schema_version: str = field(init=False, default=POLICY_SCHEMA_VERSION)
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        _strict_int("slippage_bps", self.slippage_bps, maximum=5_000)
        object.__setattr__(self, "fingerprint", content_fingerprint(self.payload()))

    def payload(self) -> dict[str, Any]:
        return {"schema_version": self.schema_version, "slippage_bps": self.slippage_bps}


@dataclass(frozen=True, slots=True)
class FeeBreakdownV01:
    tier_index: int
    market_cap: int
    lp_fee_bps: int
    protocol_fee_bps: int
    creator_fee_bps: int
    lp_fee: int
    protocol_fee: int
    creator_fee: int
    fee_config_fingerprint: str

    def __post_init__(self) -> None:
        _strict_int("tier_index", self.tier_index, maximum=127)
        _strict_int("market_cap", self.market_cap)
        for label, value in (
            ("lp_fee_bps", self.lp_fee_bps),
            ("protocol_fee_bps", self.protocol_fee_bps),
            ("creator_fee_bps", self.creator_fee_bps),
        ):
            _strict_int(label, value, maximum=BPS_DENOMINATOR)
        if self.lp_fee_bps + self.protocol_fee_bps + self.creator_fee_bps >= BPS_DENOMINATOR:
            raise QuoteError("quote fee bps must be below 10000")
        for label, value in (
            ("lp_fee", self.lp_fee),
            ("protocol_fee", self.protocol_fee),
            ("creator_fee", self.creator_fee),
        ):
            _strict_int(label, value, maximum=MAX_U64)
        if len(self.fee_config_fingerprint) != 64:
            raise QuoteError("fee config fingerprint must be SHA-256")
        int(self.fee_config_fingerprint, 16)

    @property
    def total_fee(self) -> int:
        return self.lp_fee + self.protocol_fee + self.creator_fee

    def payload(self) -> dict[str, Any]:
        return {
            "tier_index": self.tier_index,
            "market_cap": self.market_cap,
            "lp_fee_bps": self.lp_fee_bps,
            "protocol_fee_bps": self.protocol_fee_bps,
            "creator_fee_bps": self.creator_fee_bps,
            "lp_fee": self.lp_fee,
            "protocol_fee": self.protocol_fee,
            "creator_fee": self.creator_fee,
            "total_fee": self.total_fee,
            "fee_config_fingerprint": self.fee_config_fingerprint,
        }


def _ceil_div(a: int, b: int) -> int:
    if a < 0 or b <= 0:
        raise QuoteError("ceil division requires non-negative numerator and positive denominator")
    return (a + b - 1) // b


def _fee(amount: int, bps: int) -> int:
    return _ceil_div(amount * bps, BPS_DENOMINATOR)


def _market_cap(quote_reserve: int, supply: int, base_reserve: int) -> int:
    if base_reserve <= 0:
        raise QuoteError("market-cap denominator must be positive")
    return quote_reserve * supply // base_reserve


def _selected_fees(
    state: VenueStateV01,
    *,
    amount: int,
) -> tuple[int, int, FeesV01]:
    if isinstance(state, PumpBondingCurveStateV01):
        market_cap = _market_cap(
            state.decoded.virtual_quote_reserves,
            state.fee_mint_supply,
            state.decoded.virtual_token_reserves,
        )
    else:
        market_cap = _market_cap(
            state.effective_quote_reserve, state.base_mint_supply, state.raw_base_reserve
        )
    index, tier = state.fee_config.select(market_cap)
    return index, market_cap, tier.fees


@dataclass(frozen=True, slots=True)
class ExecutableQuoteV01:
    intent_id: str
    intent_fingerprint: str
    venue_state_id: str
    venue_state_fingerprint: str
    route_id: str
    route_fingerprint: str
    policy_fingerprint: str
    venue: VenueKind
    side: IntentSide
    input_amount: int
    spendable_quote_input: int | None
    expected_base_amount: int | None
    expected_quote_amount: int | None
    net_quote_proceeds: int | None
    total_quote_debit: int | None
    minimum_output: int
    reserve_base_delta: int
    reserve_quote_delta: int
    price_impact_ppm: int
    fees: FeeBreakdownV01
    slot_min: int
    slot_max: int
    schema_version: str = field(init=False, default=QUOTE_SCHEMA_VERSION)
    fingerprint: str = field(init=False)
    quote_id: str = field(init=False)

    def __post_init__(self) -> None:
        _strict_int("input_amount", self.input_amount, minimum=1, maximum=MAX_U64)
        _strict_int("minimum_output", self.minimum_output, maximum=MAX_U64)
        _strict_int("price_impact_ppm", self.price_impact_ppm, maximum=IMPACT_PPM_DENOMINATOR)
        if self.slot_max < self.slot_min:
            raise QuoteError("quote slot span is invalid")
        if self.side is IntentSide.BUY:
            if (
                self.spendable_quote_input is None
                or self.expected_base_amount is None
                or self.total_quote_debit is None
                or self.expected_quote_amount is not None
                or self.net_quote_proceeds is not None
                or self.reserve_base_delta >= 0
                or self.reserve_quote_delta <= 0
            ):
                raise QuoteError("BUY quote economic shape is invalid")
        else:
            if (
                self.spendable_quote_input is not None
                or self.expected_base_amount is not None
                or self.total_quote_debit is not None
                or self.expected_quote_amount is None
                or self.net_quote_proceeds is None
                or self.reserve_base_delta <= 0
                or self.reserve_quote_delta >= 0
            ):
                raise QuoteError("SELL quote economic shape is invalid")
        fp = content_fingerprint(self.payload())
        object.__setattr__(self, "fingerprint", fp)
        object.__setattr__(self, "quote_id", deterministic_id("P5QT", QUOTE_SCHEMA_VERSION, fp))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "intent_id": self.intent_id,
            "intent_fingerprint": self.intent_fingerprint,
            "venue_state_id": self.venue_state_id,
            "venue_state_fingerprint": self.venue_state_fingerprint,
            "route_id": self.route_id,
            "route_fingerprint": self.route_fingerprint,
            "policy_fingerprint": self.policy_fingerprint,
            "venue": self.venue.value,
            "side": self.side.value,
            "input_amount": self.input_amount,
            "spendable_quote_input": self.spendable_quote_input,
            "expected_base_amount": self.expected_base_amount,
            "expected_quote_amount": self.expected_quote_amount,
            "net_quote_proceeds": self.net_quote_proceeds,
            "total_quote_debit": self.total_quote_debit,
            "minimum_output": self.minimum_output,
            "reserve_base_delta": self.reserve_base_delta,
            "reserve_quote_delta": self.reserve_quote_delta,
            "price_impact_ppm": self.price_impact_ppm,
            "fees": self.fees.payload(),
            "slot_min": self.slot_min,
            "slot_max": self.slot_max,
        }


def _impact_ppm(actual: int, spot: int) -> int:
    if actual <= 0 or spot <= 0:
        raise QuoteError("price-impact operands must be positive")
    if actual >= spot:
        return 0
    return min(IMPACT_PPM_DENOMINATOR, (spot - actual) * IMPACT_PPM_DENOMINATOR // spot)


def create_executable_quote(
    intent: ExecutionIntentV01,
    state: VenueStateV01,
    route: RouteDecisionV01,
    policy: QuotePolicyV01,
) -> ExecutableQuoteV01:
    if intent.intent_id != state.intent_id or intent.intent_id != route.intent_id:
        raise QuoteError("intent, state, and route lineage mismatch")
    if route.selected_state_id != state.state_id or state.fingerprint not in route.state_fingerprints:
        raise QuoteError("route is not bound to the exact venue state")
    expected_outcome = (
        RouteOutcome.ROUTE_PUMP_BONDING_CURVE
        if isinstance(state, PumpBondingCurveStateV01)
        else RouteOutcome.ROUTE_PUMPSWAP_CANONICAL
    )
    if route.outcome is not expected_outcome:
        raise QuoteError("route outcome and venue state disagree")
    if intent.side is IntentSide.BUY and intent.input_asset != "SOL_LAMPORTS":
        raise QuoteError("SOL-paired BUY intent input must be SOL_LAMPORTS")
    if intent.side is IntentSide.SELL and intent.input_asset != "MEME_BASE_UNITS":
        raise QuoteError("SELL intent input must be MEME_BASE_UNITS")
    amount = _strict_int("intent input amount", intent.input_amount_base_units, minimum=1, maximum=MAX_U64)
    tier_index, market_cap, rates = _selected_fees(state, amount=amount)
    creator_present = (
        state.decoded.creator != SYSTEM_PROGRAM_ID
        if isinstance(state, PumpBondingCurveStateV01)
        else state.decoded.coin_creator != SYSTEM_PROGRAM_ID
    )
    creator_bps = rates.creator_fee_bps if creator_present else 0
    lp_bps = 0 if isinstance(state, PumpBondingCurveStateV01) else rates.lp_fee_bps
    total_bps = lp_bps + rates.protocol_fee_bps + creator_bps

    if intent.side is IntentSide.BUY:
        if amount <= 1:
            raise QuoteError("quote input is too small")
        spendable = (amount - 1) * BPS_DENOMINATOR // (BPS_DENOMINATOR + total_bps)
        if spendable <= 0:
            raise QuoteError("no spendable quote remains after fees")
        lp_fee = _fee(spendable, lp_bps)
        protocol_fee = _fee(spendable, rates.protocol_fee_bps)
        creator_fee = _fee(spendable, creator_bps)
        over = spendable + lp_fee + protocol_fee + creator_fee - amount
        if over > 0:
            spendable -= over
        if spendable <= 0:
            raise QuoteError("fee reconciliation exhausted quote input")
        if isinstance(state, PumpBondingCurveStateV01):
            v_base = state.decoded.virtual_token_reserves
            v_quote = state.decoded.virtual_quote_reserves
            base_out = spendable * v_base // (v_quote + spendable)
            if base_out <= 0 or base_out > state.decoded.real_token_reserves:
                raise QuoteError("Pump buy would exhaust real token reserves")
            reserve_quote_delta = spendable
            venue = VenueKind.PUMP_BONDING_CURVE
        else:
            input_for_curve = spendable - 1
            if input_for_curve <= 0:
                raise QuoteError("PumpSwap curve input is too small")
            v_base = state.raw_base_reserve
            v_quote = state.effective_quote_reserve
            base_out = v_base * input_for_curve // (v_quote + input_for_curve)
            if base_out <= 0 or base_out >= state.raw_base_reserve:
                raise QuoteError("PumpSwap buy has insufficient liquidity")
            reserve_quote_delta = spendable + lp_fee
            venue = VenueKind.PUMPSWAP_CANONICAL
        total_debit = spendable + lp_fee + protocol_fee + creator_fee
        spot = spendable * v_base // v_quote
        minimum = base_out * (BPS_DENOMINATOR - policy.slippage_bps) // BPS_DENOMINATOR
        expected_quote = None
        net_quote = None
        reserve_base_delta = -base_out
        impact = _impact_ppm(base_out, spot)
        spendable_value: int | None = spendable
        expected_base: int | None = base_out
        total_debit_value: int | None = total_debit
    else:
        if isinstance(state, PumpBondingCurveStateV01):
            v_base = state.decoded.virtual_token_reserves
            v_quote = state.decoded.virtual_quote_reserves
            raw_quote = amount * v_quote // (v_base + amount)
            if raw_quote <= 0 or raw_quote > state.decoded.real_quote_reserves:
                raise QuoteError("Pump sell exceeds real quote reserves")
            venue = VenueKind.PUMP_BONDING_CURVE
        else:
            v_base = state.raw_base_reserve
            v_quote = state.effective_quote_reserve
            raw_quote = v_quote * amount // (v_base + amount)
            if raw_quote <= 0:
                raise QuoteError("PumpSwap sell output is zero")
            venue = VenueKind.PUMPSWAP_CANONICAL
        lp_fee = _fee(raw_quote, lp_bps)
        protocol_fee = _fee(raw_quote, rates.protocol_fee_bps)
        creator_fee = _fee(raw_quote, creator_bps)
        net = raw_quote - lp_fee - protocol_fee - creator_fee
        if net <= 0:
            raise QuoteError("fees consume sell output")
        if isinstance(state, PumpSwapStateV01):
            if raw_quote - lp_fee > state.raw_quote_reserve:
                raise QuoteError("PumpSwap real quote vault cannot cover sell")
            reserve_quote_delta = -(raw_quote - lp_fee)
        else:
            reserve_quote_delta = -raw_quote
        spot = amount * v_quote // v_base
        minimum = net * (BPS_DENOMINATOR - policy.slippage_bps) // BPS_DENOMINATOR
        expected_quote = raw_quote
        net_quote = net
        reserve_base_delta = amount
        impact = _impact_ppm(raw_quote, spot)
        spendable_value = None
        expected_base = None
        total_debit_value = None
    fees = FeeBreakdownV01(
        tier_index, market_cap, lp_bps, rates.protocol_fee_bps, creator_bps,
        lp_fee, protocol_fee, creator_fee, state.fee_config.fingerprint,
    )
    return ExecutableQuoteV01(
        intent.intent_id, intent.fingerprint, state.state_id, state.fingerprint,
        route.route_id, route.fingerprint, policy.fingerprint, venue, intent.side,
        amount, spendable_value, expected_base, expected_quote, net_quote,
        total_debit_value, minimum, reserve_base_delta, reserve_quote_delta,
        impact, fees, state.slot_min, state.slot_max,
    )


CONTRACT_SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "supported_venues": [kind.value for kind in VenueKind],
    "route_outcomes": [outcome.value for outcome in RouteOutcome],
    "chain_order": "SOLANA_CONTEXT_SLOT_WITH_HONEST_SPAN",
    "dynamic_fees": "PUMP_FEES_ACCOUNT_TIERED_INTEGER_CEIL",
    "pump_math": "OFFICIAL_PUMP_SDK_1_36_0_INTEGER_ROUNDING",
    "pumpswap_math": "OFFICIAL_PUMP_SWAP_SDK_1_19_0_EFFECTIVE_QUOTE_RESERVE",
    "canonical_pool": "INDEX_0_POOL_AUTHORITY_PDA_AND_STATE_PROOF",
    "economic_quantities": "INTEGER_ONLY",
    "quote_mutability": "IMMUTABLE_EXACT_STATE_BINDING",
    "capabilities": ["READ_PUBLIC_ACCOUNTS", "PERSIST_SHADOW_EVIDENCE"],
    "handoff": "T003_UNSIGNED_PLAN_SIMULATION_ONLY_LATER",
}
MODEL_FINGERPRINT = content_fingerprint(CONTRACT_SPEC)
