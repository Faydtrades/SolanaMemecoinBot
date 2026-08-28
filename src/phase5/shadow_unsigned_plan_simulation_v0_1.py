from __future__ import annotations

import base64
import hashlib
import json
import struct
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, Protocol, Sequence

from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.message import Message
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import Transaction

from .shadow_domain_v0_1 import (
    ExecutionIntentV01,
    IntentSide,
    canonical_json,
    content_fingerprint,
    deterministic_id,
)
from .shadow_venue_route_quote_v0_1 import (
    MAX_U64,
    PUMP_FEES_PROGRAM_ID,
    PUMP_PROGRAM_ID,
    PUMPSWAP_PROGRAM_ID,
    SYSTEM_PROGRAM_ID,
    TOKEN_2022_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    WSOL_MINT,
    AccountEvidenceV01,
    ExecutableQuoteV01,
    PumpBondingCurveStateV01,
    PumpSwapStateV01,
    RouteDecisionV01,
    RouteOutcome,
    RpcAccountV01,
    VenueKind,
    VenueStateV01,
    account_data_fingerprint,
    decode_token_account,
    derive_bonding_curve_pda,
    derive_pump_fee_config_pda,
    derive_pump_global_pda,
    derive_pumpswap_fee_config_pda,
    derive_pumpswap_global_pda,
)


MODEL_ID = "P5-SHADOW-UNSIGNED-PLAN-SIMULATION-0001"
SCHEMA_VERSION = "phase5_shadow_unsigned_plan_simulation_v0.1"
PLAN_SCHEMA_VERSION = "phase5_unsigned_transaction_plan_v0.1"
POLICY_SCHEMA_VERSION = "phase5_transaction_plan_policy_v0.1"
ACTOR_SCHEMA_VERSION = "phase5_public_shadow_actor_v0.1"
RECIPIENT_SCHEMA_VERSION = "phase5_verified_recipient_evidence_v0.1"
ACTOR_ACCOUNTS_SCHEMA_VERSION = "phase5_actor_account_snapshot_v0.1"
LEASE_SCHEMA_VERSION = "phase5_blockhash_lease_v0.1"
VALIDITY_SCHEMA_VERSION = "phase5_blockhash_validity_evidence_v0.1"
CONFIG_SCHEMA_VERSION = "phase5_simulation_request_config_v0.1"
ENVELOPE_SCHEMA_VERSION = "phase5_simulation_envelope_v0.1"
ATTEMPT_SCHEMA_VERSION = "phase5_simulation_attempt_v0.1"
RESULT_SCHEMA_VERSION = "phase5_shadow_simulation_result_v0.1"

ASSOCIATED_TOKEN_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
PUMP_BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR = bytes((194, 171, 28, 70, 104, 77, 91, 47))
PUMP_SELL_V2_DISCRIMINATOR = bytes((93, 246, 130, 60, 231, 233, 64, 178))
PUMPSWAP_BUY_EXACT_QUOTE_IN_DISCRIMINATOR = bytes((198, 46, 21, 82, 180, 217, 232, 112))
PUMPSWAP_SELL_DISCRIMINATOR = bytes((51, 230, 133, 164, 1, 127, 131, 173))
ZERO_HASH_HEX = "00" * 32
ZERO_PLACEHOLDER_BYTES = b"\x00" * 64
_FACTORY_TOKEN = object()


class PlanError(ValueError):
    pass


class SimulationEvidenceError(ValueError):
    pass


class SimulationOutcome(StrEnum):
    SIMULATION_SUCCESS = "SIMULATION_SUCCESS"
    PROGRAM_REJECTED = "PROGRAM_REJECTED"
    BLOCKHASH_EXPIRED = "BLOCKHASH_EXPIRED"
    RPC_FAILURE = "RPC_FAILURE"
    MALFORMED_RESPONSE = "MALFORMED_RESPONSE"
    CONTEXT_REGRESSION = "CONTEXT_REGRESSION"


def _required_text(label: str, value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} is required")
    return value


def _strict_int(label: str, value: int, *, minimum: int = 0, maximum: int = MAX_U64) -> int:
    if type(value) is not int:
        raise TypeError(f"{label} must be an integer")
    if value < minimum or value > maximum:
        raise ValueError(f"{label} is outside [{minimum}, {maximum}]")
    return value


def _pubkey(value: str) -> Pubkey:
    try:
        return Pubkey.from_string(_required_text("public key", value))
    except Exception as exc:
        raise PlanError(f"invalid public key: {value}") from exc


def _hash(value: str) -> Hash:
    try:
        return Hash.from_string(_required_text("blockhash", value))
    except Exception as exc:
        raise SimulationEvidenceError("invalid blockhash") from exc


def _sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _validate_sha256(label: str, value: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or value != value.lower():
        raise ValueError(f"{label} must be a SHA-256 hex digest")
    try:
        int(value, 16)
    except ValueError as exc:
        raise ValueError(f"{label} must be a SHA-256 hex digest") from exc
    return value


def _validated_payload_json(label: str, value: str) -> dict[str, Any]:
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise PlanError(f"{label} must be canonical JSON") from exc
    if not isinstance(decoded, dict) or canonical_json(decoded) != value:
        raise PlanError(f"{label} must be a canonical JSON object")
    return decoded


def _pda(program: str, seeds: Sequence[bytes]) -> str:
    return str(Pubkey.find_program_address(list(seeds), _pubkey(program))[0])


def derive_associated_token_address(owner: str, mint: str, token_program: str) -> str:
    if token_program not in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
        raise PlanError("unsupported token program for associated token account")
    return _pda(
        ASSOCIATED_TOKEN_PROGRAM_ID,
        (bytes(_pubkey(owner)), bytes(_pubkey(token_program)), bytes(_pubkey(mint))),
    )


def derive_event_authority(program: str) -> str:
    return _pda(program, (b"__event_authority",))


def derive_pump_creator_vault(creator: str) -> str:
    return _pda(PUMP_PROGRAM_ID, (b"creator-vault", bytes(_pubkey(creator))))


def derive_sharing_config(mint: str) -> str:
    return _pda(PUMP_FEES_PROGRAM_ID, (b"sharing-config", bytes(_pubkey(mint))))


def derive_global_volume_accumulator(program: str) -> str:
    if program not in (PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID):
        raise PlanError("unsupported volume-accumulator program")
    return _pda(program, (b"global_volume_accumulator",))


def derive_user_volume_accumulator(program: str, actor: str) -> str:
    if program not in (PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID):
        raise PlanError("unsupported volume-accumulator program")
    return _pda(program, (b"user_volume_accumulator", bytes(_pubkey(actor))))


def derive_pumpswap_creator_vault_authority(coin_creator: str) -> str:
    return _pda(PUMPSWAP_PROGRAM_ID, (b"creator_vault", bytes(_pubkey(coin_creator))))


def derive_pumpswap_pool_v2(mint: str) -> str:
    return _pda(PUMPSWAP_PROGRAM_ID, (b"pool-v2", bytes(_pubkey(mint))))


@dataclass(frozen=True, slots=True)
class PublicShadowActorV01:
    public_key: str
    schema_version: str = field(init=False, default=ACTOR_SCHEMA_VERSION)
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        _pubkey(self.public_key)
        object.__setattr__(self, "fingerprint", content_fingerprint(self.payload()))

    def payload(self) -> dict[str, Any]:
        return {"schema_version": self.schema_version, "public_key": self.public_key}


@dataclass(frozen=True, slots=True)
class TransactionPlanPolicyV01:
    message_format: str = "LEGACY"
    setup_policy: str = "SDK_COMPAT_IDEMPOTENT_V01"
    recipient_selection: str = "HASH_BOUND_INDEX_V01"
    volume_tracking: bool = True
    commitment: str = "confirmed"
    schema_version: str = field(init=False, default=POLICY_SCHEMA_VERSION)
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        if self.message_format != "LEGACY":
            raise PlanError("T003 v0.1 supports only legacy messages")
        if self.setup_policy != "SDK_COMPAT_IDEMPOTENT_V01":
            raise PlanError("unsupported account-setup policy")
        if self.recipient_selection != "HASH_BOUND_INDEX_V01":
            raise PlanError("unsupported recipient-selection policy")
        if type(self.volume_tracking) is not bool:
            raise TypeError("volume_tracking must be bool")
        if self.commitment != "confirmed":
            raise PlanError("T003 v0.1 commitment must be confirmed")
        object.__setattr__(self, "fingerprint", content_fingerprint(self.payload()))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "message_format": self.message_format,
            "setup_policy": self.setup_policy,
            "recipient_selection": self.recipient_selection,
            "volume_tracking": self.volume_tracking,
            "commitment": self.commitment,
        }


@dataclass(frozen=True, slots=True)
class PlanAccountMetaV01:
    pubkey: str
    writable: bool
    authority_required: bool

    def __post_init__(self) -> None:
        _pubkey(self.pubkey)
        if type(self.writable) is not bool or type(self.authority_required) is not bool:
            raise TypeError("account flags must be bool")

    def payload(self) -> dict[str, Any]:
        return {
            "pubkey": self.pubkey,
            "writable": self.writable,
            "authority_required": self.authority_required,
        }

    def materialize(self) -> AccountMeta:
        return AccountMeta(_pubkey(self.pubkey), self.authority_required, self.writable)


@dataclass(frozen=True, slots=True)
class PlannedInstructionV01:
    sequence: int
    name: str
    program_id: str
    accounts: tuple[PlanAccountMetaV01, ...]
    data_hex: str
    contract_fingerprint: str
    fingerprint: str = field(init=False)
    instruction_id: str = field(init=False)

    def __post_init__(self) -> None:
        _strict_int("instruction sequence", self.sequence, maximum=255)
        _required_text("instruction name", self.name)
        _pubkey(self.program_id)
        if not self.accounts:
            raise PlanError("instruction accounts cannot be empty")
        if not isinstance(self.accounts, tuple) or not all(
            isinstance(item, PlanAccountMetaV01) for item in self.accounts
        ):
            raise TypeError("instruction accounts must be immutable account metadata")
        try:
            decoded_data = bytes.fromhex(self.data_hex)
        except ValueError as exc:
            raise PlanError("instruction data must be canonical hex") from exc
        if decoded_data.hex() != self.data_hex:
            raise PlanError("instruction data must be lowercase contiguous hex")
        _validate_sha256("contract_fingerprint", self.contract_fingerprint)
        fp = content_fingerprint(self.payload())
        object.__setattr__(self, "fingerprint", fp)
        object.__setattr__(self, "instruction_id", deterministic_id("P5IX", "v0.1", fp))

    def payload(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "name": self.name,
            "program_id": self.program_id,
            "accounts": [item.payload() for item in self.accounts],
            "data_hex": self.data_hex,
            "contract_fingerprint": self.contract_fingerprint,
        }

    def materialize(self) -> Instruction:
        return Instruction(
            _pubkey(self.program_id),
            bytes.fromhex(self.data_hex),
            [item.materialize() for item in self.accounts],
        )


def _account_schema(names: Sequence[tuple[str, bool, bool]]) -> list[dict[str, Any]]:
    return [
        {"name": name, "writable": writable, "authority_required": authority}
        for name, writable, authority in names
    ]


PUMP_BUY_ACCOUNTS = (
    ("global", False, False), ("base_mint", False, False),
    ("quote_mint", False, False), ("base_token_program", False, False),
    ("quote_token_program", False, False), ("associated_token_program", False, False),
    ("fee_recipient", True, False), ("associated_quote_fee_recipient", True, False),
    ("buyback_fee_recipient", True, False),
    ("associated_quote_buyback_fee_recipient", True, False),
    ("bonding_curve", True, False), ("associated_base_bonding_curve", True, False),
    ("associated_quote_bonding_curve", True, False), ("user", True, True),
    ("associated_base_user", True, False), ("associated_quote_user", True, False),
    ("creator_vault", True, False), ("associated_creator_vault", True, False),
    ("sharing_config", False, False), ("global_volume_accumulator", False, False),
    ("user_volume_accumulator", True, False),
    ("associated_user_volume_accumulator", True, False),
    ("fee_config", False, False), ("fee_program", False, False),
    ("system_program", False, False), ("event_authority", False, False),
    ("program", False, False),
)
PUMP_SELL_ACCOUNTS = PUMP_BUY_ACCOUNTS[:19] + PUMP_BUY_ACCOUNTS[20:]
PUMPSWAP_BUY_ACCOUNTS = (
    ("pool", True, False), ("user", True, True), ("global_config", False, False),
    ("base_mint", False, False), ("quote_mint", False, False),
    ("user_base_token_account", True, False), ("user_quote_token_account", True, False),
    ("pool_base_token_account", True, False), ("pool_quote_token_account", True, False),
    ("protocol_fee_recipient", False, False),
    ("protocol_fee_recipient_token_account", True, False),
    ("base_token_program", False, False), ("quote_token_program", False, False),
    ("system_program", False, False), ("associated_token_program", False, False),
    ("event_authority", False, False), ("program", False, False),
    ("coin_creator_vault_ata", True, False),
    ("coin_creator_vault_authority", False, False),
    ("global_volume_accumulator", False, False),
    ("user_volume_accumulator", True, False), ("fee_config", False, False),
    ("fee_program", False, False),
)
PUMPSWAP_SELL_ACCOUNTS = PUMPSWAP_BUY_ACCOUNTS[:19] + PUMPSWAP_BUY_ACCOUNTS[21:]

PUMPSWAP_POOL_V2_REMAINING_ACCOUNT = {
    "name": "pool_v2",
    "pda": {
        "program_id": PUMPSWAP_PROGRAM_ID,
        "seeds": [
            {"type": "utf8", "value": "pool-v2"},
            {"type": "account", "value": "base_mint"},
        ],
    },
    "presence": "REQUIRED_INDEPENDENT_OF_COIN_CREATOR",
    "writable": False,
    "authority_required": False,
}
PUMPSWAP_BUY_REMAINING_ACCOUNT_CONTRACT = {
    "non_cashback_order": [
        "pool_v2", "buyback_fee_recipient", "buyback_fee_recipient_quote_ata",
    ],
    "cashback_order": [
        "user_volume_accumulator_quote_ata", "pool_v2",
        "buyback_fee_recipient", "buyback_fee_recipient_quote_ata",
    ],
    "pool_v2": PUMPSWAP_POOL_V2_REMAINING_ACCOUNT,
}
PUMPSWAP_SELL_REMAINING_ACCOUNT_CONTRACT = {
    "non_cashback_order": [
        "pool_v2", "buyback_fee_recipient", "buyback_fee_recipient_quote_ata",
    ],
    "cashback_order": [
        "user_volume_accumulator_quote_ata", "user_volume_accumulator", "pool_v2",
        "buyback_fee_recipient", "buyback_fee_recipient_quote_ata",
    ],
    "pool_v2": PUMPSWAP_POOL_V2_REMAINING_ACCOUNT,
}


def _instruction_contract(
    program_id: str,
    instruction_name: str,
    discriminator: bytes,
    accounts: Sequence[tuple[str, bool, bool]],
    args: Sequence[tuple[str, str]],
    remaining_accounts: Mapping[str, Any] | None = None,
) -> str:
    contract = {
        "program_id": program_id,
        "instruction_name": instruction_name,
        "discriminator_hex": discriminator.hex(),
        "ordered_accounts": _account_schema(accounts),
        "args": [{"name": name, "type": kind} for name, kind in args],
    }
    if remaining_accounts is not None:
        contract["remaining_accounts"] = remaining_accounts
    return content_fingerprint(contract)


PUMP_BUY_CONTRACT_FINGERPRINT = _instruction_contract(
    PUMP_PROGRAM_ID, "buy_exact_quote_in_v2", PUMP_BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR,
    PUMP_BUY_ACCOUNTS, (("spendable_quote_in", "u64"), ("min_tokens_out", "u64")),
)
PUMP_SELL_CONTRACT_FINGERPRINT = _instruction_contract(
    PUMP_PROGRAM_ID, "sell_v2", PUMP_SELL_V2_DISCRIMINATOR,
    PUMP_SELL_ACCOUNTS, (("amount", "u64"), ("min_sol_output", "u64")),
)
PUMPSWAP_BUY_CONTRACT_FINGERPRINT = _instruction_contract(
    PUMPSWAP_PROGRAM_ID, "buy_exact_quote_in", PUMPSWAP_BUY_EXACT_QUOTE_IN_DISCRIMINATOR,
    PUMPSWAP_BUY_ACCOUNTS,
    (("spendable_quote_in", "u64"), ("min_base_amount_out", "u64"), ("track_volume", "OptionBool")),
    PUMPSWAP_BUY_REMAINING_ACCOUNT_CONTRACT,
)
PUMPSWAP_SELL_CONTRACT_FINGERPRINT = _instruction_contract(
    PUMPSWAP_PROGRAM_ID, "sell", PUMPSWAP_SELL_DISCRIMINATOR,
    PUMPSWAP_SELL_ACCOUNTS,
    (("base_amount_in", "u64"), ("min_quote_amount_out", "u64")),
    PUMPSWAP_SELL_REMAINING_ACCOUNT_CONTRACT,
)


class _Cursor:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def take(self, count: int) -> bytes:
        if self.offset + count > len(self.data):
            raise PlanError("global account is truncated")
        value = self.data[self.offset:self.offset + count]
        self.offset += count
        return value

    def u8(self) -> int:
        return self.take(1)[0]

    def u64(self) -> int:
        return int.from_bytes(self.take(8), "little")

    def key(self) -> str:
        return str(Pubkey.from_bytes(self.take(32)))


@dataclass(frozen=True, slots=True)
class VerifiedRecipientEvidenceV01:
    venue_state_fingerprint: str
    global_account: AccountEvidenceV01
    normal_fee_recipients: tuple[str, ...]
    reserved_fee_recipients: tuple[str, ...]
    buyback_fee_recipients: tuple[str, ...]
    _factory_token: object = field(repr=False, compare=False)
    schema_version: str = field(init=False, default=RECIPIENT_SCHEMA_VERSION)
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        if self._factory_token is not _FACTORY_TOKEN:
            raise PlanError("recipient evidence must be decoded from verified T002 bytes")
        _validate_sha256("venue_state_fingerprint", self.venue_state_fingerprint)
        for label, values in (
            ("normal fee recipients", self.normal_fee_recipients),
            ("reserved fee recipients", self.reserved_fee_recipients),
            ("buyback fee recipients", self.buyback_fee_recipients),
        ):
            if not isinstance(values, tuple) or len(values) != 8 or len(set(values)) != 8:
                raise PlanError(f"{label} must contain eight unique keys")
            for value in values:
                _pubkey(value)
                if value == SYSTEM_PROGRAM_ID:
                    raise PlanError(f"{label} contains the default key")
        object.__setattr__(self, "fingerprint", content_fingerprint(self.payload()))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "venue_state_fingerprint": self.venue_state_fingerprint,
            "global_account": self.global_account.payload(),
            "normal_fee_recipients": list(self.normal_fee_recipients),
            "reserved_fee_recipients": list(self.reserved_fee_recipients),
            "buyback_fee_recipients": list(self.buyback_fee_recipients),
        }


def decode_verified_recipient_evidence(
    state: VenueStateV01,
    global_account: RpcAccountV01,
) -> VerifiedRecipientEvidenceV01:
    expected_key = derive_pump_global_pda() if isinstance(
        state, PumpBondingCurveStateV01
    ) else derive_pumpswap_global_pda()
    expected_owner = PUMP_PROGRAM_ID if isinstance(
        state, PumpBondingCurveStateV01
    ) else PUMPSWAP_PROGRAM_ID
    evidence_map = {item.pubkey: item for item in state.accounts}
    expected_evidence = evidence_map.get(expected_key)
    if (
        global_account.pubkey != expected_key
        or global_account.owner != expected_owner
        or expected_evidence is None
        or account_data_fingerprint(global_account.data) != expected_evidence.data_sha256
        or len(global_account.data) != expected_evidence.data_length
    ):
        raise PlanError("global recipient bytes do not match T002 evidence")
    cursor = _Cursor(global_account.data)
    cursor.take(8)
    if isinstance(state, PumpBondingCurveStateV01):
        cursor.u8()
        cursor.key()
        first_normal = cursor.key()
        for _ in range(5):
            cursor.u64()
        cursor.key()
        cursor.u8()
        cursor.u64()
        cursor.u64()
        normal = (first_normal,) + tuple(cursor.key() for _ in range(7))
        cursor.key()
        cursor.key()
        cursor.u8()
        cursor.key()
        first_reserved = cursor.key()
        cursor.u8()
        reserved = (first_reserved,) + tuple(cursor.key() for _ in range(7))
        cursor.u8()
        buyback = tuple(cursor.key() for _ in range(8))
    else:
        cursor.key()
        cursor.u64()
        cursor.u64()
        cursor.u8()
        normal = tuple(cursor.key() for _ in range(8))
        cursor.u64()
        cursor.key()
        cursor.key()
        first_reserved = cursor.key()
        cursor.u8()
        reserved = (first_reserved,) + tuple(cursor.key() for _ in range(7))
        cursor.u8()
        buyback = tuple(cursor.key() for _ in range(8))
    return VerifiedRecipientEvidenceV01(
        state.fingerprint, expected_evidence, normal, reserved, buyback, _FACTORY_TOKEN
    )


@dataclass(frozen=True, slots=True)
class ActorAccountSnapshotV01:
    actor_public_key: str
    mint: str
    venue_state_fingerprint: str
    context_slot: int
    observed_at_us: int
    base_token_program: str
    quote_token_program: str
    base_account: AccountEvidenceV01 | None
    quote_account: AccountEvidenceV01 | None
    _factory_token: object = field(repr=False, compare=False)
    schema_version: str = field(init=False, default=ACTOR_ACCOUNTS_SCHEMA_VERSION)
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        if self._factory_token is not _FACTORY_TOKEN:
            raise PlanError("actor account snapshot must be built from verified account bytes")
        _pubkey(self.actor_public_key)
        _pubkey(self.mint)
        _validate_sha256("venue_state_fingerprint", self.venue_state_fingerprint)
        _strict_int("context_slot", self.context_slot)
        _strict_int("observed_at_us", self.observed_at_us)
        if self.base_token_program not in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
            raise PlanError("actor snapshot base token program is unsupported")
        if self.quote_token_program != TOKEN_PROGRAM_ID:
            raise PlanError("actor snapshot quote token program must be legacy SPL Token")
        expected_base = derive_associated_token_address(
            self.actor_public_key, self.mint, self.base_token_program
        )
        expected_quote = derive_associated_token_address(
            self.actor_public_key, WSOL_MINT, self.quote_token_program
        )
        for label, evidence, expected_key, expected_owner in (
            ("base", self.base_account, expected_base, self.base_token_program),
            ("quote", self.quote_account, expected_quote, self.quote_token_program),
        ):
            if evidence is not None and (
                evidence.pubkey != expected_key
                or evidence.owner != expected_owner
                or evidence.context_slot != self.context_slot
            ):
                raise PlanError(f"actor {label} account evidence is inconsistent")
        object.__setattr__(self, "fingerprint", content_fingerprint(self.payload()))

    @property
    def base_exists(self) -> bool:
        return self.base_account is not None

    @property
    def quote_exists(self) -> bool:
        return self.quote_account is not None

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "actor_public_key": self.actor_public_key,
            "mint": self.mint,
            "venue_state_fingerprint": self.venue_state_fingerprint,
            "context_slot": self.context_slot,
            "observed_at_us": self.observed_at_us,
            "base_token_program": self.base_token_program,
            "quote_token_program": self.quote_token_program,
            "base_account": None if self.base_account is None else self.base_account.payload(),
            "quote_account": None if self.quote_account is None else self.quote_account.payload(),
        }


def build_actor_account_snapshot(
    state: VenueStateV01,
    actor: PublicShadowActorV01,
    *,
    base_account: RpcAccountV01 | None,
    quote_account: RpcAccountV01 | None,
    context_slot: int,
    observed_at_us: int,
) -> ActorAccountSnapshotV01:
    _strict_int("context_slot", context_slot)
    if context_slot < state.slot_max:
        raise PlanError("actor account snapshot regresses below venue state")
    expected_base = derive_associated_token_address(
        actor.public_key, state.mint, state.base_token_program
    )
    quote_program = TOKEN_PROGRAM_ID if isinstance(
        state, PumpBondingCurveStateV01
    ) else state.quote_token_program
    expected_quote = derive_associated_token_address(actor.public_key, WSOL_MINT, quote_program)

    def verified(
        item: RpcAccountV01 | None,
        expected_key: str,
        expected_mint: str,
        expected_program: str,
    ) -> AccountEvidenceV01 | None:
        if item is None:
            return None
        if item.pubkey != expected_key or item.owner != expected_program:
            raise PlanError("actor token-account identity mismatch")
        decoded = decode_token_account(item, expected_mint)
        if decoded.authority != actor.public_key:
            raise PlanError("actor token-account authority mismatch")
        return item.evidence(context_slot)

    return ActorAccountSnapshotV01(
        actor.public_key,
        state.mint,
        state.fingerprint,
        context_slot,
        observed_at_us,
        state.base_token_program,
        quote_program,
        verified(base_account, expected_base, state.mint, state.base_token_program),
        verified(quote_account, expected_quote, WSOL_MINT, quote_program),
        _FACTORY_TOKEN,
    )


def _select(values: tuple[str, ...], *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).digest()
    return values[int.from_bytes(digest[:8], "big") % len(values)]


def _meta(pubkey: str, writable: bool = False, authority: bool = False) -> PlanAccountMetaV01:
    return PlanAccountMetaV01(pubkey, writable, authority)


def _expected_pumpswap_remaining_accounts(
    side: IntentSide,
    cashback: bool,
    actor_public_key: str,
    mint: str,
    buyback_fee_recipient: str,
) -> tuple[PlanAccountMetaV01, ...]:
    if not isinstance(side, IntentSide):
        raise TypeError("PumpSwap remaining-account side must be IntentSide")
    if type(cashback) is not bool:
        raise TypeError("PumpSwap cashback flag must be bool")
    user_volume = derive_user_volume_accumulator(PUMPSWAP_PROGRAM_ID, actor_public_key)
    expected: list[PlanAccountMetaV01] = []
    if cashback:
        expected.append(_meta(
            derive_associated_token_address(user_volume, WSOL_MINT, TOKEN_PROGRAM_ID), True
        ))
        if side is IntentSide.SELL:
            expected.append(_meta(user_volume, True))
    expected.append(_meta(derive_pumpswap_pool_v2(mint)))
    expected.extend((
        _meta(buyback_fee_recipient),
        _meta(derive_associated_token_address(
            buyback_fee_recipient, WSOL_MINT, TOKEN_PROGRAM_ID
        ), True),
    ))
    return tuple(expected)


def validate_pumpswap_remaining_account_layout(
    *,
    side: IntentSide,
    cashback: bool,
    actor_public_key: str,
    mint: str,
    buyback_fee_recipient: str,
    accounts: Sequence[PlanAccountMetaV01],
) -> None:
    """Fail closed unless the exact upgraded PumpSwap trailing layout is present."""
    fixed_count = len(
        PUMPSWAP_BUY_ACCOUNTS if side is IntentSide.BUY else PUMPSWAP_SELL_ACCOUNTS
    )
    expected = _expected_pumpswap_remaining_accounts(
        side, cashback, actor_public_key, mint, buyback_fee_recipient
    )
    if not isinstance(accounts, (list, tuple)) or not all(
        isinstance(item, PlanAccountMetaV01) for item in accounts
    ):
        raise TypeError("PumpSwap accounts must be ordered account metadata")
    if len(accounts) != fixed_count + len(expected) or tuple(accounts[fixed_count:]) != expected:
        raise PlanError("PumpSwap remaining-account layout is noncanonical")
    pool_v2 = derive_pumpswap_pool_v2(mint)
    matches = [item for item in accounts if item.pubkey == pool_v2]
    if len(matches) != 1 or matches[0].writable or matches[0].authority_required:
        raise PlanError("PumpSwap pool_v2 must appear exactly once as readonly non-authority")


def _instruction(
    sequence: int,
    name: str,
    program: str,
    accounts: Sequence[PlanAccountMetaV01],
    data: bytes,
    contract_fingerprint: str,
) -> PlannedInstructionV01:
    return PlannedInstructionV01(
        sequence, name, program, tuple(accounts), data.hex(), contract_fingerprint
    )


def _ata_create(sequence: int, actor: str, ata: str, owner: str, mint: str, token_program: str) -> PlannedInstructionV01:
    contract = content_fingerprint({
        "program_id": ASSOCIATED_TOKEN_PROGRAM_ID,
        "instruction": "create_associated_token_account_idempotent",
        "accounts": _account_schema((
            ("payer", True, True), ("ata", True, False), ("owner", False, False),
            ("mint", False, False), ("system_program", False, False),
            ("token_program", False, False),
        )),
        "data_hex": "01",
    })
    return _instruction(sequence, "create_associated_token_account_idempotent", ASSOCIATED_TOKEN_PROGRAM_ID, (
        _meta(actor, True, True), _meta(ata, True), _meta(owner), _meta(mint),
        _meta(SYSTEM_PROGRAM_ID), _meta(token_program),
    ), b"\x01", contract)


def _system_transfer(sequence: int, actor: str, destination: str, amount: int) -> PlannedInstructionV01:
    _strict_int("native transfer amount", amount, minimum=1)
    data = struct.pack("<IQ", 2, amount)
    contract = content_fingerprint({
        "program_id": SYSTEM_PROGRAM_ID,
        "instruction": "transfer",
        "accounts": _account_schema((("from", True, True), ("to", True, False))),
        "args": [{"name": "lamports", "type": "u64"}],
    })
    return _instruction(sequence, "system_transfer", SYSTEM_PROGRAM_ID, (
        _meta(actor, True, True), _meta(destination, True),
    ), data, contract)


def _sync_native(sequence: int, ata: str) -> PlannedInstructionV01:
    contract = content_fingerprint({
        "program_id": TOKEN_PROGRAM_ID,
        "instruction": "sync_native",
        "accounts": _account_schema((("account", True, False),)),
        "data_hex": "11",
    })
    return _instruction(sequence, "sync_native", TOKEN_PROGRAM_ID, (_meta(ata, True),), b"\x11", contract)


def _close_native(sequence: int, ata: str, actor: str) -> PlannedInstructionV01:
    contract = content_fingerprint({
        "program_id": TOKEN_PROGRAM_ID,
        "instruction": "close_account",
        "accounts": _account_schema((
            ("account", True, False), ("destination", True, False), ("owner", False, True),
        )),
        "data_hex": "09",
    })
    return _instruction(sequence, "close_native_account", TOKEN_PROGRAM_ID, (
        _meta(ata, True), _meta(actor, True), _meta(actor, False, True),
    ), b"\x09", contract)


@dataclass(frozen=True, slots=True)
class UnsignedTransactionPlanV01:
    intent_id: str
    intent_fingerprint: str
    quote_id: str
    quote_fingerprint: str
    venue_state_id: str
    venue_state_fingerprint: str
    route_id: str
    route_fingerprint: str
    actor_public_key: str
    actor_fingerprint: str
    venue: VenueKind
    side: IntentSide
    policy_fingerprint: str
    account_snapshot_fingerprint: str
    recipient_evidence_fingerprint: str
    instruction_contract_fingerprint: str
    base_token_program: str
    quote_token_program: str
    prerequisite_slot: int
    instructions: tuple[PlannedInstructionV01, ...]
    actor_payload_json: str
    policy_payload_json: str
    account_snapshot_payload_json: str
    recipient_evidence_payload_json: str
    _factory_token: object = field(repr=False, compare=False)
    schema_version: str = field(init=False, default=PLAN_SCHEMA_VERSION)
    fingerprint: str = field(init=False)
    plan_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self._factory_token is not _FACTORY_TOKEN:
            raise PlanError("unsigned plans must be built by the verified plan builder")
        for label, value in (
            ("intent_id", self.intent_id), ("quote_id", self.quote_id),
            ("venue_state_id", self.venue_state_id), ("route_id", self.route_id),
        ):
            _required_text(label, value)
        for label, value in (
            ("intent fingerprint", self.intent_fingerprint),
            ("quote fingerprint", self.quote_fingerprint),
            ("venue state fingerprint", self.venue_state_fingerprint),
            ("route fingerprint", self.route_fingerprint),
            ("actor fingerprint", self.actor_fingerprint),
            ("policy fingerprint", self.policy_fingerprint),
            ("account snapshot fingerprint", self.account_snapshot_fingerprint),
            ("recipient evidence fingerprint", self.recipient_evidence_fingerprint),
            ("instruction contract fingerprint", self.instruction_contract_fingerprint),
        ):
            _validate_sha256(label, value)
        _pubkey(self.actor_public_key)
        if self.base_token_program not in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
            raise PlanError("plan base token program is unsupported")
        if self.quote_token_program != TOKEN_PROGRAM_ID:
            raise PlanError("T003 v0.1 quote token program must be legacy SPL Token")
        _strict_int("prerequisite_slot", self.prerequisite_slot)
        if not isinstance(self.instructions, tuple) or not self.instructions:
            raise PlanError("plan requires an immutable instruction sequence")
        if tuple(item.sequence for item in self.instructions) != tuple(range(len(self.instructions))):
            raise PlanError("instruction sequence is noncanonical")
        actor_payload = _validated_payload_json("actor payload", self.actor_payload_json)
        policy_payload = _validated_payload_json("policy payload", self.policy_payload_json)
        account_payload = _validated_payload_json(
            "actor account snapshot payload", self.account_snapshot_payload_json
        )
        recipient_payload = _validated_payload_json(
            "recipient evidence payload", self.recipient_evidence_payload_json
        )
        if (
            actor_payload.get("public_key") != self.actor_public_key
            or content_fingerprint(actor_payload) != self.actor_fingerprint
            or content_fingerprint(policy_payload) != self.policy_fingerprint
            or content_fingerprint(account_payload) != self.account_snapshot_fingerprint
            or content_fingerprint(recipient_payload) != self.recipient_evidence_fingerprint
        ):
            raise PlanError("embedded plan evidence conflicts with its fingerprint")
        fp = content_fingerprint(self.payload())
        object.__setattr__(self, "fingerprint", fp)
        object.__setattr__(self, "plan_id", deterministic_id("P5PL", PLAN_SCHEMA_VERSION, fp))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "intent_id": self.intent_id,
            "intent_fingerprint": self.intent_fingerprint,
            "quote_id": self.quote_id,
            "quote_fingerprint": self.quote_fingerprint,
            "venue_state_id": self.venue_state_id,
            "venue_state_fingerprint": self.venue_state_fingerprint,
            "route_id": self.route_id,
            "route_fingerprint": self.route_fingerprint,
            "actor_public_key": self.actor_public_key,
            "actor_fingerprint": self.actor_fingerprint,
            "venue": self.venue.value,
            "side": self.side.value,
            "policy_fingerprint": self.policy_fingerprint,
            "account_snapshot_fingerprint": self.account_snapshot_fingerprint,
            "recipient_evidence_fingerprint": self.recipient_evidence_fingerprint,
            "instruction_contract_fingerprint": self.instruction_contract_fingerprint,
            "base_token_program": self.base_token_program,
            "quote_token_program": self.quote_token_program,
            "prerequisite_slot": self.prerequisite_slot,
            "actor": json.loads(self.actor_payload_json),
            "policy": json.loads(self.policy_payload_json),
            "actor_account_snapshot": json.loads(self.account_snapshot_payload_json),
            "recipient_evidence": json.loads(self.recipient_evidence_payload_json),
            "instructions": [item.payload() for item in self.instructions],
        }


def _verify_lineage(
    intent: ExecutionIntentV01,
    state: VenueStateV01,
    route: RouteDecisionV01,
    quote: ExecutableQuoteV01,
    actor: PublicShadowActorV01,
    policy: TransactionPlanPolicyV01,
    recipients: VerifiedRecipientEvidenceV01,
    actor_accounts: ActorAccountSnapshotV01,
) -> None:
    if not all((
        intent.intent_id == state.intent_id == route.intent_id == quote.intent_id,
        intent.fingerprint == quote.intent_fingerprint,
        state.state_id == quote.venue_state_id == route.selected_state_id,
        state.fingerprint == quote.venue_state_fingerprint,
        route.route_id == quote.route_id,
        route.fingerprint == quote.route_fingerprint,
        state.fingerprint in route.state_fingerprints,
        actor.public_key == actor_accounts.actor_public_key,
        state.mint == actor_accounts.mint,
        state.fingerprint == actor_accounts.venue_state_fingerprint,
        state.fingerprint == recipients.venue_state_fingerprint,
        actor_accounts.context_slot >= state.slot_max,
    )):
        raise PlanError("intent/quote/route/state/actor lineage mismatch")
    expected_outcome = RouteOutcome.ROUTE_PUMP_BONDING_CURVE if isinstance(
        state, PumpBondingCurveStateV01
    ) else RouteOutcome.ROUTE_PUMPSWAP_CANONICAL
    if route.outcome is not expected_outcome or quote.venue is not state.venue:
        raise PlanError("route or quote venue conflicts with exact state")
    if quote.side is not intent.side:
        raise PlanError("quote side conflicts with intent")
    if quote.input_amount != intent.input_amount_base_units:
        raise PlanError("quote input conflicts with immutable intent budget")
    if quote.side is IntentSide.BUY:
        if not all(value is not None for value in (
            quote.spendable_quote_input, quote.expected_base_amount,
            quote.total_quote_debit,
        )):
            raise PlanError("BUY quote is missing required economic evidence")
        spendable = int(quote.spendable_quote_input)
        expected_base = int(quote.expected_base_amount)
        total_debit = int(quote.total_quote_debit)
        if not (
            spendable + quote.fees.total_fee == total_debit
            and 0 <= quote.input_amount - total_debit <= 1
            and 0 < quote.minimum_output <= expected_base
            and quote.reserve_base_delta == -expected_base
        ):
            raise PlanError("BUY quote economic evidence is internally inconsistent")
    else:
        if quote.expected_quote_amount is None or quote.net_quote_proceeds is None:
            raise PlanError("SELL quote is missing required economic evidence")
        if not (
            quote.net_quote_proceeds + quote.fees.total_fee == quote.expected_quote_amount
            and 0 < quote.minimum_output <= quote.net_quote_proceeds
            and quote.reserve_base_delta == quote.input_amount
        ):
            raise PlanError("SELL quote economic evidence is internally inconsistent")
    if policy.commitment != "confirmed":
        raise PlanError("plan policy commitment mismatch")


def build_unsigned_transaction_plan(
    intent: ExecutionIntentV01,
    state: VenueStateV01,
    route: RouteDecisionV01,
    quote: ExecutableQuoteV01,
    actor: PublicShadowActorV01,
    policy: TransactionPlanPolicyV01,
    recipients: VerifiedRecipientEvidenceV01,
    actor_accounts: ActorAccountSnapshotV01,
) -> UnsignedTransactionPlanV01:
    _verify_lineage(intent, state, route, quote, actor, policy, recipients, actor_accounts)
    recipient_set = recipients.reserved_fee_recipients if state.decoded.is_mayhem_mode else recipients.normal_fee_recipients
    fee_recipient = _select(
        recipient_set, quote.fingerprint, actor.fingerprint, policy.fingerprint, "protocol"
    )
    buyback_recipient = _select(
        recipients.buyback_fee_recipients,
        quote.fingerprint,
        actor.fingerprint,
        policy.fingerprint,
        "buyback",
    )
    actor_key = actor.public_key
    base_ata = derive_associated_token_address(actor_key, state.mint, state.base_token_program)
    quote_ata = derive_associated_token_address(actor_key, WSOL_MINT, TOKEN_PROGRAM_ID)
    instructions: list[PlannedInstructionV01] = []

    if isinstance(state, PumpBondingCurveStateV01):
        if intent.side is IntentSide.BUY and not actor_accounts.base_exists:
            instructions.append(_ata_create(len(instructions), actor_key, base_ata, actor_key, state.mint, state.base_token_program))
        if intent.side is IntentSide.SELL and not actor_accounts.base_exists:
            raise PlanError("Pump sell requires a verified actor base-token account")
        curve = derive_bonding_curve_pda(state.mint)
        creator_vault = derive_pump_creator_vault(state.decoded.creator)
        user_volume = derive_user_volume_accumulator(PUMP_PROGRAM_ID, actor_key)
        values = {
            "global": derive_pump_global_pda(),
            "base_mint": state.mint,
            "quote_mint": WSOL_MINT,
            "base_token_program": state.base_token_program,
            "quote_token_program": TOKEN_PROGRAM_ID,
            "associated_token_program": ASSOCIATED_TOKEN_PROGRAM_ID,
            "fee_recipient": fee_recipient,
            "associated_quote_fee_recipient": derive_associated_token_address(fee_recipient, WSOL_MINT, TOKEN_PROGRAM_ID),
            "buyback_fee_recipient": buyback_recipient,
            "associated_quote_buyback_fee_recipient": derive_associated_token_address(buyback_recipient, WSOL_MINT, TOKEN_PROGRAM_ID),
            "bonding_curve": curve,
            "associated_base_bonding_curve": derive_associated_token_address(curve, state.mint, state.base_token_program),
            "associated_quote_bonding_curve": derive_associated_token_address(curve, WSOL_MINT, TOKEN_PROGRAM_ID),
            "user": actor_key,
            "associated_base_user": base_ata,
            "associated_quote_user": quote_ata,
            "creator_vault": creator_vault,
            "associated_creator_vault": derive_associated_token_address(creator_vault, WSOL_MINT, TOKEN_PROGRAM_ID),
            "sharing_config": derive_sharing_config(state.mint),
            "global_volume_accumulator": derive_global_volume_accumulator(PUMP_PROGRAM_ID),
            "user_volume_accumulator": user_volume,
            "associated_user_volume_accumulator": derive_associated_token_address(user_volume, WSOL_MINT, TOKEN_PROGRAM_ID),
            "fee_config": derive_pump_fee_config_pda(),
            "fee_program": PUMP_FEES_PROGRAM_ID,
            "system_program": SYSTEM_PROGRAM_ID,
            "event_authority": derive_event_authority(PUMP_PROGRAM_ID),
            "program": PUMP_PROGRAM_ID,
        }
        if intent.side is IntentSide.BUY:
            schema = PUMP_BUY_ACCOUNTS
            data = PUMP_BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR + struct.pack(
                "<QQ", quote.input_amount, quote.minimum_output
            )
            contract = PUMP_BUY_CONTRACT_FINGERPRINT
            name = "pump_buy_exact_quote_in_v2"
        else:
            schema = PUMP_SELL_ACCOUNTS
            data = PUMP_SELL_V2_DISCRIMINATOR + struct.pack(
                "<QQ", quote.input_amount, quote.minimum_output
            )
            contract = PUMP_SELL_CONTRACT_FINGERPRINT
            name = "pump_sell_v2"
        accounts = tuple(_meta(values[key], writable, authority) for key, writable, authority in schema)
        instructions.append(_instruction(len(instructions), name, PUMP_PROGRAM_ID, accounts, data, contract))
    else:
        if intent.side is IntentSide.SELL and not actor_accounts.base_exists:
            raise PlanError("PumpSwap sell requires a verified actor base-token account")
        if intent.side is IntentSide.BUY and not actor_accounts.base_exists:
            instructions.append(_ata_create(len(instructions), actor_key, base_ata, actor_key, state.mint, state.base_token_program))
        quote_account_created = not actor_accounts.quote_exists
        if quote_account_created:
            instructions.append(_ata_create(len(instructions), actor_key, quote_ata, actor_key, WSOL_MINT, TOKEN_PROGRAM_ID))
        if intent.side is IntentSide.BUY:
            instructions.append(_system_transfer(len(instructions), actor_key, quote_ata, quote.input_amount))
            instructions.append(_sync_native(len(instructions), quote_ata))
        coin_creator_vault = derive_pumpswap_creator_vault_authority(state.decoded.coin_creator)
        user_volume = derive_user_volume_accumulator(PUMPSWAP_PROGRAM_ID, actor_key)
        values = {
            "pool": state.pool,
            "user": actor_key,
            "global_config": derive_pumpswap_global_pda(),
            "base_mint": state.mint,
            "quote_mint": WSOL_MINT,
            "user_base_token_account": base_ata,
            "user_quote_token_account": quote_ata,
            "pool_base_token_account": state.decoded.pool_base_token_account,
            "pool_quote_token_account": state.decoded.pool_quote_token_account,
            "protocol_fee_recipient": fee_recipient,
            "protocol_fee_recipient_token_account": derive_associated_token_address(fee_recipient, WSOL_MINT, TOKEN_PROGRAM_ID),
            "base_token_program": state.base_token_program,
            "quote_token_program": TOKEN_PROGRAM_ID,
            "system_program": SYSTEM_PROGRAM_ID,
            "associated_token_program": ASSOCIATED_TOKEN_PROGRAM_ID,
            "event_authority": derive_event_authority(PUMPSWAP_PROGRAM_ID),
            "program": PUMPSWAP_PROGRAM_ID,
            "coin_creator_vault_ata": derive_associated_token_address(coin_creator_vault, WSOL_MINT, TOKEN_PROGRAM_ID),
            "coin_creator_vault_authority": coin_creator_vault,
            "global_volume_accumulator": derive_global_volume_accumulator(PUMPSWAP_PROGRAM_ID),
            "user_volume_accumulator": user_volume,
            "fee_config": derive_pumpswap_fee_config_pda(),
            "fee_program": PUMP_FEES_PROGRAM_ID,
        }
        schema = PUMPSWAP_BUY_ACCOUNTS if intent.side is IntentSide.BUY else PUMPSWAP_SELL_ACCOUNTS
        accounts = [_meta(values[key], writable, authority) for key, writable, authority in schema]
        if state.decoded.is_cashback_coin:
            accounts.append(_meta(derive_associated_token_address(user_volume, WSOL_MINT, TOKEN_PROGRAM_ID), True))
            if intent.side is IntentSide.SELL:
                accounts.append(_meta(user_volume, True))
        accounts.append(_meta(derive_pumpswap_pool_v2(state.mint)))
        accounts.extend((
            _meta(buyback_recipient),
            _meta(derive_associated_token_address(buyback_recipient, WSOL_MINT, TOKEN_PROGRAM_ID), True),
        ))
        validate_pumpswap_remaining_account_layout(
            side=intent.side,
            cashback=state.decoded.is_cashback_coin,
            actor_public_key=actor_key,
            mint=state.mint,
            buyback_fee_recipient=buyback_recipient,
            accounts=accounts,
        )
        if intent.side is IntentSide.BUY:
            data = PUMPSWAP_BUY_EXACT_QUOTE_IN_DISCRIMINATOR + struct.pack(
                "<QQ", quote.input_amount, quote.minimum_output
            ) + bytes((int(policy.volume_tracking),))
            contract = PUMPSWAP_BUY_CONTRACT_FINGERPRINT
            name = "pumpswap_buy_exact_quote_in"
        else:
            data = PUMPSWAP_SELL_DISCRIMINATOR + struct.pack(
                "<QQ", quote.input_amount, quote.minimum_output
            )
            contract = PUMPSWAP_SELL_CONTRACT_FINGERPRINT
            name = "pumpswap_sell"
        instructions.append(_instruction(len(instructions), name, PUMPSWAP_PROGRAM_ID, accounts, data, contract))
        if quote_account_created:
            instructions.append(_close_native(len(instructions), quote_ata, actor_key))

    venue_contract = (
        PUMP_BUY_CONTRACT_FINGERPRINT if isinstance(state, PumpBondingCurveStateV01) and intent.side is IntentSide.BUY
        else PUMP_SELL_CONTRACT_FINGERPRINT if isinstance(state, PumpBondingCurveStateV01)
        else PUMPSWAP_BUY_CONTRACT_FINGERPRINT if intent.side is IntentSide.BUY
        else PUMPSWAP_SELL_CONTRACT_FINGERPRINT
    )
    return UnsignedTransactionPlanV01(
        intent_id=intent.intent_id,
        intent_fingerprint=intent.fingerprint,
        quote_id=quote.quote_id,
        quote_fingerprint=quote.fingerprint,
        venue_state_id=state.state_id,
        venue_state_fingerprint=state.fingerprint,
        route_id=route.route_id,
        route_fingerprint=route.fingerprint,
        actor_public_key=actor_key,
        actor_fingerprint=actor.fingerprint,
        venue=state.venue,
        side=intent.side,
        policy_fingerprint=policy.fingerprint,
        account_snapshot_fingerprint=actor_accounts.fingerprint,
        recipient_evidence_fingerprint=recipients.fingerprint,
        instruction_contract_fingerprint=venue_contract,
        base_token_program=state.base_token_program,
        quote_token_program=TOKEN_PROGRAM_ID,
        prerequisite_slot=max(
            state.slot_max, actor_accounts.context_slot, recipients.global_account.context_slot
        ),
        instructions=tuple(instructions),
        actor_payload_json=canonical_json(actor.payload()),
        policy_payload_json=canonical_json(policy.payload()),
        account_snapshot_payload_json=canonical_json(actor_accounts.payload()),
        recipient_evidence_payload_json=canonical_json(recipients.payload()),
        _factory_token=_FACTORY_TOKEN,
    )


@dataclass(frozen=True, slots=True)
class LatestBlockhashResponseV01:
    context_slot: int
    blockhash: str
    last_valid_block_height: int

    def __post_init__(self) -> None:
        _strict_int("blockhash context slot", self.context_slot)
        _hash(self.blockhash)
        _strict_int("last valid block height", self.last_valid_block_height)


@dataclass(frozen=True, slots=True)
class BlockHeightResponseV01:
    block_height: int

    def __post_init__(self) -> None:
        _strict_int("block height", self.block_height)


@dataclass(frozen=True, slots=True)
class BlockhashValidityResponseV01:
    context_slot: int
    valid: bool

    def __post_init__(self) -> None:
        _strict_int("validity context slot", self.context_slot)
        if type(self.valid) is not bool:
            raise TypeError("valid must be bool")


@dataclass(frozen=True, slots=True)
class SimulationRpcResponseV01:
    context_slot: int
    value: Mapping[str, Any]

    def __post_init__(self) -> None:
        _strict_int("simulation context slot", self.context_slot)
        if not isinstance(self.value, Mapping):
            raise TypeError("simulation value must be a mapping")


class ReadOnlySimulationRpcV01(Protocol):
    def get_latest_blockhash(
        self, *, commitment: str, min_context_slot: int
    ) -> LatestBlockhashResponseV01: ...

    def get_block_height(
        self, *, commitment: str, min_context_slot: int
    ) -> BlockHeightResponseV01: ...

    def is_blockhash_valid(
        self, blockhash: str, *, commitment: str, min_context_slot: int
    ) -> BlockhashValidityResponseV01: ...

    def simulate_transaction(
        self, transaction_base64: str, *, config: Mapping[str, Any]
    ) -> SimulationRpcResponseV01: ...


@dataclass(frozen=True, slots=True)
class BlockhashLeaseV01:
    plan_id: str
    blockhash: str
    context_slot: int
    last_valid_block_height: int
    commitment: str
    observed_at_us: int
    schema_version: str = field(init=False, default=LEASE_SCHEMA_VERSION)
    fingerprint: str = field(init=False)
    lease_id: str = field(init=False)

    def __post_init__(self) -> None:
        _required_text("plan_id", self.plan_id)
        _hash(self.blockhash)
        _strict_int("context_slot", self.context_slot)
        _strict_int("last_valid_block_height", self.last_valid_block_height)
        if self.commitment != "confirmed":
            raise SimulationEvidenceError("unsupported blockhash commitment")
        _strict_int("observed_at_us", self.observed_at_us)
        fp = content_fingerprint(self.payload())
        object.__setattr__(self, "fingerprint", fp)
        object.__setattr__(self, "lease_id", deterministic_id("P5BH", LEASE_SCHEMA_VERSION, fp))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "blockhash": self.blockhash,
            "context_slot": self.context_slot,
            "last_valid_block_height": self.last_valid_block_height,
            "commitment": self.commitment,
            "observed_at_us": self.observed_at_us,
        }


def acquire_blockhash_lease(
    rpc: ReadOnlySimulationRpcV01,
    plan: UnsignedTransactionPlanV01,
    *,
    observed_at_us: int,
) -> BlockhashLeaseV01:
    response = rpc.get_latest_blockhash(
        commitment="confirmed", min_context_slot=plan.prerequisite_slot
    )
    if not isinstance(response, LatestBlockhashResponseV01):
        raise SimulationEvidenceError("malformed latest-blockhash response")
    if response.context_slot < plan.prerequisite_slot:
        raise SimulationEvidenceError("blockhash context regresses below plan")
    return BlockhashLeaseV01(
        plan.plan_id,
        response.blockhash,
        response.context_slot,
        response.last_valid_block_height,
        "confirmed",
        observed_at_us,
    )


@dataclass(frozen=True, slots=True)
class BlockhashValidityEvidenceV01:
    lease_id: str
    block_height: int
    validity_context_slot: int
    rpc_valid: bool
    required_min_context_slot: int
    observed_at_us: int
    schema_version: str = field(init=False, default=VALIDITY_SCHEMA_VERSION)
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        _required_text("lease_id", self.lease_id)
        _strict_int("block_height", self.block_height)
        _strict_int("validity_context_slot", self.validity_context_slot)
        if type(self.rpc_valid) is not bool:
            raise TypeError("rpc_valid must be bool")
        _strict_int("required_min_context_slot", self.required_min_context_slot)
        if self.validity_context_slot < self.required_min_context_slot:
            raise SimulationEvidenceError("blockhash-validity context regressed")
        _strict_int("observed_at_us", self.observed_at_us)
        object.__setattr__(self, "fingerprint", content_fingerprint(self.payload()))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "lease_id": self.lease_id,
            "block_height": self.block_height,
            "validity_context_slot": self.validity_context_slot,
            "rpc_valid": self.rpc_valid,
            "required_min_context_slot": self.required_min_context_slot,
            "observed_at_us": self.observed_at_us,
        }


def verify_blockhash_lease(
    rpc: ReadOnlySimulationRpcV01,
    plan: UnsignedTransactionPlanV01,
    lease: BlockhashLeaseV01,
    *,
    observed_at_us: int,
) -> BlockhashValidityEvidenceV01:
    if lease.plan_id != plan.plan_id or lease.context_slot < plan.prerequisite_slot:
        raise SimulationEvidenceError("blockhash lease does not belong to plan")
    required = max(plan.prerequisite_slot, lease.context_slot)
    height = rpc.get_block_height(commitment=lease.commitment, min_context_slot=required)
    validity = rpc.is_blockhash_valid(
        lease.blockhash, commitment=lease.commitment, min_context_slot=required
    )
    if not isinstance(height, BlockHeightResponseV01) or not isinstance(
        validity, BlockhashValidityResponseV01
    ):
        raise SimulationEvidenceError("malformed blockhash-validity response")
    return BlockhashValidityEvidenceV01(
        lease.lease_id,
        height.block_height,
        validity.context_slot,
        validity.valid,
        required,
        observed_at_us,
    )


@dataclass(frozen=True, slots=True)
class SimulationRequestConfigV01:
    min_context_slot: int
    encoding: str = "base64"
    sig_verify: bool = False
    replace_recent_blockhash: bool = False
    commitment: str = "confirmed"
    inner_instructions: bool = True
    schema_version: str = field(init=False, default=CONFIG_SCHEMA_VERSION)
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        _strict_int("min_context_slot", self.min_context_slot)
        if (
            self.encoding != "base64"
            or self.sig_verify is not False
            or self.replace_recent_blockhash is not False
            or self.commitment != "confirmed"
            or self.inner_instructions is not True
        ):
            raise SimulationEvidenceError("simulation request contract mismatch")
        object.__setattr__(self, "fingerprint", content_fingerprint(self.payload()))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "encoding": self.encoding,
            "sigVerify": self.sig_verify,
            "replaceRecentBlockhash": self.replace_recent_blockhash,
            "commitment": self.commitment,
            "minContextSlot": self.min_context_slot,
            "innerInstructions": self.inner_instructions,
        }

    def rpc_payload(self) -> dict[str, Any]:
        return {
            "encoding": self.encoding,
            "sigVerify": self.sig_verify,
            "replaceRecentBlockhash": self.replace_recent_blockhash,
            "commitment": self.commitment,
            "minContextSlot": self.min_context_slot,
            "innerInstructions": self.inner_instructions,
        }


@dataclass(frozen=True, slots=True)
class SimulationEnvelopeV01:
    plan_id: str
    plan_fingerprint: str
    lease_id: str
    lease_fingerprint: str
    request_config_fingerprint: str
    request_config_json: str
    message_hex: str
    transaction_base64: str
    required_authority_count: int
    zero_placeholder_count: int
    message_sha256: str
    wire_sha256: str
    schema_version: str = field(init=False, default=ENVELOPE_SCHEMA_VERSION)
    fingerprint: str = field(init=False)
    envelope_id: str = field(init=False)

    def __post_init__(self) -> None:
        for label, value in (
            ("plan_id", self.plan_id), ("lease_id", self.lease_id),
        ):
            _required_text(label, value)
        for label, value in (
            ("plan fingerprint", self.plan_fingerprint),
            ("lease fingerprint", self.lease_fingerprint),
            ("request config fingerprint", self.request_config_fingerprint),
            ("message sha256", self.message_sha256),
            ("wire sha256", self.wire_sha256),
        ):
            _validate_sha256(label, value)
        _strict_int("required_authority_count", self.required_authority_count, minimum=1, maximum=255)
        if self.zero_placeholder_count != self.required_authority_count:
            raise SimulationEvidenceError("every authority slot must have one zero placeholder")
        config_payload = _validated_payload_json(
            "simulation request config", self.request_config_json
        )
        if content_fingerprint(config_payload) != self.request_config_fingerprint:
            raise SimulationEvidenceError("simulation request config fingerprint mismatch")
        expected_fixed = {
            "encoding": "base64",
            "sigVerify": False,
            "replaceRecentBlockhash": False,
            "commitment": "confirmed",
            "innerInstructions": True,
        }
        if any(config_payload.get(key) != value for key, value in expected_fixed.items()):
            raise SimulationEvidenceError("embedded simulation request contract mismatch")
        try:
            message_bytes = bytes.fromhex(self.message_hex)
            wire = base64.b64decode(self.transaction_base64, validate=True)
        except Exception as exc:
            raise SimulationEvidenceError("invalid envelope encoding") from exc
        if self.message_hex != message_bytes.hex() or self.transaction_base64 != base64.b64encode(wire).decode("ascii"):
            raise SimulationEvidenceError("envelope encoding is noncanonical")
        if _sha256_hex(message_bytes) != self.message_sha256 or _sha256_hex(wire) != self.wire_sha256:
            raise SimulationEvidenceError("envelope byte digest mismatch")
        parsed = Transaction.from_bytes(wire)
        if bytes(parsed.message) != message_bytes:
            raise SimulationEvidenceError("transaction wire message conflicts with envelope message")
        if len(parsed.signatures) != self.required_authority_count or any(
            bytes(value) != ZERO_PLACEHOLDER_BYTES for value in parsed.signatures
        ):
            raise SimulationEvidenceError("simulation transaction contains a nonzero placeholder")
        fp = content_fingerprint(self.payload())
        object.__setattr__(self, "fingerprint", fp)
        object.__setattr__(self, "envelope_id", deterministic_id("P5SE", ENVELOPE_SCHEMA_VERSION, fp))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "plan_fingerprint": self.plan_fingerprint,
            "lease_id": self.lease_id,
            "lease_fingerprint": self.lease_fingerprint,
            "request_config_fingerprint": self.request_config_fingerprint,
            "request_config": json.loads(self.request_config_json),
            "message_hex": self.message_hex,
            "transaction_base64": self.transaction_base64,
            "required_authority_count": self.required_authority_count,
            "zero_placeholder_count": self.zero_placeholder_count,
            "message_sha256": self.message_sha256,
            "wire_sha256": self.wire_sha256,
        }


def materialize_simulation_envelope(
    plan: UnsignedTransactionPlanV01,
    lease: BlockhashLeaseV01,
    config: SimulationRequestConfigV01,
) -> SimulationEnvelopeV01:
    if lease.plan_id != plan.plan_id or lease.context_slot < plan.prerequisite_slot:
        raise SimulationEvidenceError("lease does not satisfy plan prerequisite")
    required_min = max(plan.prerequisite_slot, lease.context_slot)
    if config.min_context_slot != required_min:
        raise SimulationEvidenceError("simulation minContextSlot is not exact")
    materialized = [item.materialize() for item in plan.instructions]
    message = Message.new_with_blockhash(materialized, _pubkey(plan.actor_public_key), _hash(lease.blockhash))
    required = int(message.header.num_required_signatures)
    if required < 1:
        raise SimulationEvidenceError("simulation message has no required authority")
    placeholders = [Signature.default() for _ in range(required)]
    if any(bytes(value) != ZERO_PLACEHOLDER_BYTES for value in placeholders):
        raise SimulationEvidenceError("default placeholder is nonzero")
    transaction = Transaction.populate(message, placeholders)
    message_bytes = bytes(message)
    wire = bytes(transaction)
    return SimulationEnvelopeV01(
        plan.plan_id,
        plan.fingerprint,
        lease.lease_id,
        lease.fingerprint,
        config.fingerprint,
        canonical_json(config.payload()),
        message_bytes.hex(),
        base64.b64encode(wire).decode("ascii"),
        required,
        len(placeholders),
        _sha256_hex(message_bytes),
        _sha256_hex(wire),
    )


@dataclass(frozen=True, slots=True)
class SimulationAttemptV01:
    plan_id: str
    lease_id: str
    envelope_id: str
    request_config_fingerprint: str
    validity_fingerprint: str
    attempt_index: int
    observed_at_us: int
    schema_version: str = field(init=False, default=ATTEMPT_SCHEMA_VERSION)
    fingerprint: str = field(init=False)
    attempt_id: str = field(init=False)

    def __post_init__(self) -> None:
        for label, value in (
            ("plan_id", self.plan_id), ("lease_id", self.lease_id),
            ("envelope_id", self.envelope_id),
        ):
            _required_text(label, value)
        _strict_int("attempt_index", self.attempt_index, maximum=1)
        for label, value in (
            ("request config fingerprint", self.request_config_fingerprint),
            ("validity fingerprint", self.validity_fingerprint),
        ):
            _validate_sha256(label, value)
        _strict_int("observed_at_us", self.observed_at_us)
        fp = content_fingerprint(self.payload())
        object.__setattr__(self, "fingerprint", fp)
        object.__setattr__(self, "attempt_id", deterministic_id("P5SA", ATTEMPT_SCHEMA_VERSION, fp))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "lease_id": self.lease_id,
            "envelope_id": self.envelope_id,
            "request_config_fingerprint": self.request_config_fingerprint,
            "validity_fingerprint": self.validity_fingerprint,
            "attempt_index": self.attempt_index,
            "observed_at_us": self.observed_at_us,
        }


@dataclass(frozen=True, slots=True)
class ShadowSimulationResultV01:
    plan_id: str
    attempt_id: str
    lease_id: str
    envelope_id: str
    request_config_fingerprint: str
    outcome: SimulationOutcome
    context_slot: int | None
    error_json: str
    logs: tuple[str, ...]
    units_consumed: int | None
    return_data_json: str
    inner_instructions_json: str
    loaded_accounts_data_size: int | None
    returned_accounts_json: str
    reason_code: str
    observed_at_us: int
    schema_version: str = field(init=False, default=RESULT_SCHEMA_VERSION)
    fingerprint: str = field(init=False)
    result_id: str = field(init=False)

    def __post_init__(self) -> None:
        for label, value in (
            ("plan_id", self.plan_id), ("attempt_id", self.attempt_id),
            ("lease_id", self.lease_id), ("envelope_id", self.envelope_id),
        ):
            _required_text(label, value)
        _validate_sha256("request config fingerprint", self.request_config_fingerprint)
        SimulationOutcome(self.outcome)
        if self.context_slot is not None:
            _strict_int("context_slot", self.context_slot)
        if not isinstance(self.logs, tuple) or not all(isinstance(item, str) for item in self.logs):
            raise TypeError("logs must be an immutable string sequence")
        for label, value in (
            ("units_consumed", self.units_consumed),
            ("loaded_accounts_data_size", self.loaded_accounts_data_size),
        ):
            if value is not None:
                _strict_int(label, value)
        for label, value in (
            ("error_json", self.error_json),
            ("return_data_json", self.return_data_json),
            ("inner_instructions_json", self.inner_instructions_json),
            ("returned_accounts_json", self.returned_accounts_json),
        ):
            try:
                parsed = json.loads(value)
            except (TypeError, json.JSONDecodeError) as exc:
                raise SimulationEvidenceError(f"{label} is invalid JSON") from exc
            if canonical_json(parsed) != value:
                raise SimulationEvidenceError(f"{label} is noncanonical")
        _required_text("reason_code", self.reason_code)
        _strict_int("observed_at_us", self.observed_at_us)
        if self.outcome in (SimulationOutcome.SIMULATION_SUCCESS, SimulationOutcome.PROGRAM_REJECTED) and self.context_slot is None:
            raise SimulationEvidenceError("returned simulation outcome requires a context slot")
        fp = content_fingerprint(self.payload())
        object.__setattr__(self, "fingerprint", fp)
        object.__setattr__(self, "result_id", deterministic_id("P5SR", RESULT_SCHEMA_VERSION, fp))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "attempt_id": self.attempt_id,
            "lease_id": self.lease_id,
            "envelope_id": self.envelope_id,
            "request_config_fingerprint": self.request_config_fingerprint,
            "outcome": self.outcome.value,
            "context_slot": self.context_slot,
            "error": json.loads(self.error_json),
            "logs": list(self.logs),
            "units_consumed": self.units_consumed,
            "return_data": json.loads(self.return_data_json),
            "inner_instructions": json.loads(self.inner_instructions_json),
            "loaded_accounts_data_size": self.loaded_accounts_data_size,
            "returned_accounts": json.loads(self.returned_accounts_json),
            "reason_code": self.reason_code,
            "observed_at_us": self.observed_at_us,
        }


def _json_value(value: Any) -> str:
    return canonical_json(value)


def classify_simulation_response(
    plan: UnsignedTransactionPlanV01,
    attempt: SimulationAttemptV01,
    lease: BlockhashLeaseV01,
    envelope: SimulationEnvelopeV01,
    config: SimulationRequestConfigV01,
    response: SimulationRpcResponseV01,
    *,
    observed_at_us: int,
) -> ShadowSimulationResultV01:
    if not isinstance(response, SimulationRpcResponseV01):
        raise SimulationEvidenceError("malformed simulation response")
    required = config.min_context_slot
    if response.context_slot < required:
        return ShadowSimulationResultV01(
            plan.plan_id, attempt.attempt_id, lease.lease_id, envelope.envelope_id,
            config.fingerprint, SimulationOutcome.CONTEXT_REGRESSION,
            response.context_slot, _json_value(None), (), None, _json_value(None),
            _json_value(None), None, _json_value(None), "SIMULATION_CONTEXT_REGRESSION",
            observed_at_us,
        )
    value = dict(response.value)
    if "err" not in value or "logs" not in value:
        return ShadowSimulationResultV01(
            plan.plan_id, attempt.attempt_id, lease.lease_id, envelope.envelope_id,
            config.fingerprint, SimulationOutcome.MALFORMED_RESPONSE,
            response.context_slot, _json_value(value.get("err")), (), None,
            _json_value(value.get("returnData")), _json_value(value.get("innerInstructions")),
            None, _json_value(value.get("accounts")), "SIMULATION_RESPONSE_MALFORMED",
            observed_at_us,
        )
    logs_value = value.get("logs")
    if logs_value is None:
        logs: tuple[str, ...] = ()
    elif isinstance(logs_value, list) and all(isinstance(item, str) for item in logs_value):
        logs = tuple(logs_value)
    else:
        return ShadowSimulationResultV01(
            plan.plan_id, attempt.attempt_id, lease.lease_id, envelope.envelope_id,
            config.fingerprint, SimulationOutcome.MALFORMED_RESPONSE,
            response.context_slot, _json_value(value.get("err")), (), None,
            _json_value(value.get("returnData")), _json_value(value.get("innerInstructions")),
            None, _json_value(value.get("accounts")), "SIMULATION_LOGS_MALFORMED",
            observed_at_us,
        )
    units = value.get("unitsConsumed")
    loaded = value.get("loadedAccountsDataSize")
    if (units is not None and (type(units) is not int or units < 0)) or (
        loaded is not None and (type(loaded) is not int or loaded < 0)
    ):
        outcome = SimulationOutcome.MALFORMED_RESPONSE
        reason = "SIMULATION_METRICS_MALFORMED"
    elif value["err"] is None:
        outcome = SimulationOutcome.SIMULATION_SUCCESS
        reason = "SIMULATION_RETURNED_SUCCESS"
    else:
        outcome = SimulationOutcome.PROGRAM_REJECTED
        reason = "SIMULATION_PROGRAM_REJECTED"
    return ShadowSimulationResultV01(
        plan.plan_id,
        attempt.attempt_id,
        lease.lease_id,
        envelope.envelope_id,
        config.fingerprint,
        outcome,
        response.context_slot,
        _json_value(value.get("err")),
        logs,
        units if type(units) is int and units >= 0 else None,
        _json_value(value.get("returnData")),
        _json_value(value.get("innerInstructions")),
        loaded if type(loaded) is int and loaded >= 0 else None,
        _json_value(value.get("accounts")),
        reason,
        observed_at_us,
    )


@dataclass(frozen=True, slots=True)
class SimulationRunEvidenceV01:
    leases: tuple[BlockhashLeaseV01, ...]
    validity: tuple[BlockhashValidityEvidenceV01, ...]
    envelopes: tuple[SimulationEnvelopeV01, ...]
    attempts: tuple[SimulationAttemptV01, ...]
    results: tuple[ShadowSimulationResultV01, ...]

    def __post_init__(self) -> None:
        lengths = tuple(len(value) for value in (
            self.leases, self.validity, self.envelopes, self.attempts, self.results
        ))
        if len(set(lengths)) != 1 or not 1 <= lengths[0] <= 2:
            raise SimulationEvidenceError("simulation run requires one or two complete attempts")
        if tuple(item.attempt_index for item in self.attempts) != tuple(range(lengths[0])):
            raise SimulationEvidenceError("simulation attempt ordering is noncanonical")
        for lease, validity, envelope, attempt, result in zip(
            self.leases, self.validity, self.envelopes, self.attempts, self.results, strict=True
        ):
            if not (
                lease.lease_id == validity.lease_id == envelope.lease_id == attempt.lease_id == result.lease_id
                and lease.plan_id == envelope.plan_id == attempt.plan_id == result.plan_id
                and envelope.envelope_id == attempt.envelope_id == result.envelope_id
                and attempt.attempt_id == result.attempt_id
                and envelope.request_config_fingerprint
                == attempt.request_config_fingerprint
                == result.request_config_fingerprint
                and validity.fingerprint == attempt.validity_fingerprint
            ):
                raise SimulationEvidenceError("simulation run evidence lineage mismatch")
        if lengths[0] == 2 and self.results[0].outcome is not SimulationOutcome.BLOCKHASH_EXPIRED:
            raise SimulationEvidenceError("a refresh requires preserved first-attempt expiry")

    @property
    def final_result(self) -> ShadowSimulationResultV01:
        if not self.results:
            raise SimulationEvidenceError("simulation run has no result")
        return self.results[-1]


def _terminal_result(
    plan: UnsignedTransactionPlanV01,
    attempt: SimulationAttemptV01,
    lease: BlockhashLeaseV01,
    envelope: SimulationEnvelopeV01,
    config: SimulationRequestConfigV01,
    outcome: SimulationOutcome,
    reason: str,
    observed_at_us: int,
    error: Any = None,
    context_slot: int | None = None,
) -> ShadowSimulationResultV01:
    return ShadowSimulationResultV01(
        plan.plan_id, attempt.attempt_id, lease.lease_id, envelope.envelope_id,
        config.fingerprint, outcome, context_slot, _json_value(error), (), None,
        _json_value(None), _json_value(None), None, _json_value(None), reason,
        observed_at_us,
    )


def run_bounded_simulation(
    rpc: ReadOnlySimulationRpcV01,
    plan: UnsignedTransactionPlanV01,
    *,
    observed_at_us: int,
    maximum_refreshes: int = 1,
) -> SimulationRunEvidenceV01:
    if maximum_refreshes != 1:
        raise SimulationEvidenceError("T003 v0.1 requires exactly one allowed refresh")
    leases: list[BlockhashLeaseV01] = []
    validity_rows: list[BlockhashValidityEvidenceV01] = []
    envelopes: list[SimulationEnvelopeV01] = []
    attempts: list[SimulationAttemptV01] = []
    results: list[ShadowSimulationResultV01] = []
    for index in range(maximum_refreshes + 1):
        try:
            lease = acquire_blockhash_lease(rpc, plan, observed_at_us=observed_at_us + index * 10)
            config = SimulationRequestConfigV01(max(plan.prerequisite_slot, lease.context_slot))
            envelope = materialize_simulation_envelope(plan, lease, config)
            validity = verify_blockhash_lease(
                rpc, plan, lease, observed_at_us=observed_at_us + index * 10 + 1
            )
        except SimulationEvidenceError:
            raise
        leases.append(lease)
        validity_rows.append(validity)
        envelopes.append(envelope)
        attempt = SimulationAttemptV01(
            plan.plan_id, lease.lease_id, envelope.envelope_id, config.fingerprint,
            validity.fingerprint, index, observed_at_us + index * 10 + 2,
        )
        attempts.append(attempt)
        expired = (
            validity.block_height > lease.last_valid_block_height
            or not validity.rpc_valid
        )
        if expired:
            result = _terminal_result(
                plan, attempt, lease, envelope, config,
                SimulationOutcome.BLOCKHASH_EXPIRED, "BLOCKHASH_EXPIRED_BEFORE_SIMULATION",
                observed_at_us + index * 10 + 3,
            )
            results.append(result)
            if index < maximum_refreshes:
                continue
            break
        try:
            response = rpc.simulate_transaction(
                envelope.transaction_base64, config=config.rpc_payload()
            )
        except Exception as exc:
            results.append(_terminal_result(
                plan, attempt, lease, envelope, config,
                SimulationOutcome.RPC_FAILURE, "SIMULATION_RPC_FAILURE",
                observed_at_us + index * 10 + 3,
                {"exception_type": type(exc).__name__},
            ))
            break
        try:
            result = classify_simulation_response(
                plan, attempt, lease, envelope, config, response,
                observed_at_us=observed_at_us + index * 10 + 3,
            )
        except Exception as exc:
            result = _terminal_result(
                plan, attempt, lease, envelope, config,
                SimulationOutcome.MALFORMED_RESPONSE, "SIMULATION_RESPONSE_MALFORMED",
                observed_at_us + index * 10 + 3,
                {"exception_type": type(exc).__name__},
                response.context_slot if isinstance(response, SimulationRpcResponseV01) else None,
            )
        results.append(result)
        break
    if not results:
        raise SimulationEvidenceError("simulation run produced no durable result")
    return SimulationRunEvidenceV01(
        tuple(leases), tuple(validity_rows), tuple(envelopes), tuple(attempts), tuple(results)
    )


CONTRACT_SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "plan_schema": PLAN_SCHEMA_VERSION,
    "supported_paths": [
        "PUMP_BUY_EXACT_QUOTE_IN_V2", "PUMP_SELL_V2",
        "PUMPSWAP_BUY_EXACT_QUOTE_IN", "PUMPSWAP_SELL"
    ],
    "instruction_contract_fingerprints": {
        "pump_buy": PUMP_BUY_CONTRACT_FINGERPRINT,
        "pump_sell": PUMP_SELL_CONTRACT_FINGERPRINT,
        "pumpswap_buy_exact_quote_in": PUMPSWAP_BUY_CONTRACT_FINGERPRINT,
        "pumpswap_sell": PUMPSWAP_SELL_CONTRACT_FINGERPRINT,
    },
    "simulation_request": {
        "encoding": "base64",
        "sigVerify": False,
        "replaceRecentBlockhash": False,
        "commitment": "confirmed",
        "innerInstructions": True,
    },
    "maximum_blockhash_refreshes": 1,
    "outcomes": [item.value for item in SimulationOutcome],
    "plan_embeds": [
        "public_actor", "structural_policy", "actor_account_snapshot", "verified_recipient_evidence"
    ],
    "construction_guards": ["recipient_evidence", "actor_account_snapshot", "unsigned_plan"],
    "stable_plan_excludes": ["recent_blockhash", "last_valid_block_height", "wire_bytes", "simulation_result"],
}
MODEL_FINGERPRINT = content_fingerprint(CONTRACT_SPEC)
