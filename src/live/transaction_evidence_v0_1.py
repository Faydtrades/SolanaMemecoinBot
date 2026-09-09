"""Exact public transaction observations and canonical transaction order.

All finalization here is a provider-reported observation. Ledger adjudication,
settlement, retry permission and Runtime event eligibility remain separate.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from solders.message import MessageV0, to_bytes_versioned
from solders.transaction import VersionedTransaction

from .public_rpc_v0_1 import (
    TOKEN_PROGRAMS, FinalizedBlockAnchor, PublicReadOnlyRpc, PublicRpcError, PublicRpcProfile,
    block_hash, evidence_fingerprint, immutable_tuple, primary_signature, public_key, public_label, u64,
)


SCHEMA = "live_exact_transaction_evidence_v0.1"
_BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def utc(value: str) -> str:
    try:
        if type(value) is not str:
            raise ValueError
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")
    except ValueError:
        raise PublicRpcError("EXPLICIT_UTC_REQUIRED") from None


def epoch(value: str) -> float:
    return datetime.fromisoformat(value).timestamp()


def _sha(value: str) -> None:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise PublicRpcError("INVALID_PUBLIC_DIGEST")


@dataclass(frozen=True, slots=True)
class TransactionOutcome:
    succeeded: bool
    error_fingerprint: str | None

    def __post_init__(self) -> None:
        if type(self.succeeded) is not bool or self.succeeded != (self.error_fingerprint is None):
            raise PublicRpcError("INVALID_TRANSACTION_OUTCOME")
        if self.error_fingerprint is not None:
            _sha(self.error_fingerprint)


def _outcome(error: object) -> TransactionOutcome:
    if error is None:
        return TransactionOutcome(True, None)
    if type(error) not in (str, dict) or not error:
        raise PublicRpcError("TRANSACTION_ERROR_SHAPE_INVALID")

    def bounded(value: object, depth: int = 0) -> None:
        if depth > 6:
            raise PublicRpcError("TRANSACTION_ERROR_SHAPE_INVALID")
        if type(value) is str:
            if len(value) > 256:
                raise PublicRpcError("TRANSACTION_ERROR_SHAPE_INVALID")
        elif type(value) is int:
            u64(value)
        elif value is None:
            return
        elif type(value) is list and len(value) <= 8:
            for item in value:
                bounded(item, depth + 1)
        elif type(value) is dict and len(value) <= 4:
            for key, item in value.items():
                bounded(key, depth + 1)
                bounded(item, depth + 1)
        else:
            raise PublicRpcError("TRANSACTION_ERROR_SHAPE_INVALID")
    bounded(error)
    # Exact bounded error shape is compared by digest; no enum/free-text body is
    # persisted. In particular logs, return strings and RPC errors never enter it.
    return TransactionOutcome(False, hashlib.sha256(json.dumps(error, sort_keys=True,
                              separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest())


@dataclass(frozen=True, slots=True)
class SignatureStatusFact:
    signature: str
    context_slot: int
    api_version: str
    slot: int | None
    confirmation_status: str | None
    confirmations: int | None
    outcome: TransactionOutcome | None
    search_transaction_history: bool = True
    context_semantics: str = "RPC_STATUS_CONTEXT_NOT_FINALIZED_ROOT"

    def __post_init__(self) -> None:
        primary_signature(self.signature)
        u64(self.context_slot)
        public_label(self.api_version)
        if self.slot is not None:
            u64(self.slot)
            if self.slot > self.context_slot or self.confirmation_status not in ("processed", "confirmed", "finalized") or type(self.outcome) is not TransactionOutcome:
                raise PublicRpcError("SIGNATURE_STATUS_INCONSISTENT")
        elif any(value is not None for value in (self.confirmation_status, self.confirmations, self.outcome)):
            raise PublicRpcError("NULL_SIGNATURE_STATUS_HAS_NO_OUTCOME")
        if self.confirmations is not None:
            u64(self.confirmations)
        if self.slot is not None and (self.confirmation_status == "finalized") != (self.confirmations is None):
            raise PublicRpcError("SIGNATURE_STATUS_FINALIZATION_CONTRADICTION")
        if self.search_transaction_history is not True or self.context_semantics != "RPC_STATUS_CONTEXT_NOT_FINALIZED_ROOT":
            raise PublicRpcError("SIGNATURE_STATUS_SCOPE_INVALID")


def decode_signature_status(signature: str, result: object) -> SignatureStatusFact:
    if type(result) is not dict or set(result) != {"context", "value"}:
        raise PublicRpcError("SIGNATURE_STATUS_RESULT_INVALID")
    context, values = result["context"], result["value"]
    if type(context) is not dict or "slot" not in context or set(context) - {"slot", "apiVersion"}:
        raise PublicRpcError("SIGNATURE_STATUS_CONTEXT_INVALID")
    if type(values) is not list or len(values) != 1:
        raise PublicRpcError("SIGNATURE_STATUS_COUNT_INVALID")
    context_slot = u64(context["slot"])
    version = public_label(context.get("apiVersion", "UNREPORTED"))
    value = values[0]
    if value is None:
        return SignatureStatusFact(signature, context_slot, version, None, None, None, None)
    if type(value) is not dict or not {"slot", "confirmations", "confirmationStatus", "err"} <= set(value):
        raise PublicRpcError("SIGNATURE_STATUS_FIELDS_MISSING")
    outcome = _outcome(value["err"])
    if "status" in value:
        expected = {"Ok": None} if value["err"] is None else {"Err": value["err"]}
        if value["status"] != expected:
            raise PublicRpcError("SIGNATURE_STATUS_OUTCOME_CONTRADICTION")
    return SignatureStatusFact(signature, context_slot, version, value["slot"], value["confirmationStatus"],
                               value["confirmations"], outcome)


@dataclass(frozen=True, slots=True)
class InstructionFact:
    outer_index: int
    inner_index: int | None
    program_id_index: int
    account_indexes: tuple[int, ...]
    data: bytes
    stack_height: int | None

    def __post_init__(self) -> None:
        for value in (self.outer_index, self.program_id_index):
            u64(value)
        for value in (self.inner_index, self.stack_height):
            if value is not None:
                u64(value)
        immutable_tuple(self.account_indexes, int)
        for value in self.account_indexes:
            u64(value)
        if type(self.data) is not bytes:
            raise PublicRpcError("IMMUTABLE_INSTRUCTION_BYTES_REQUIRED")


@dataclass(frozen=True, slots=True)
class LookupFact:
    table: str
    writable_indexes: tuple[int, ...]
    readonly_indexes: tuple[int, ...]

    def __post_init__(self) -> None:
        public_key(self.table)
        for indexes in (self.writable_indexes, self.readonly_indexes):
            immutable_tuple(indexes, int)
            if any(not 0 <= item <= 255 for item in indexes):
                raise PublicRpcError("LOOKUP_INDEX_INVALID")


@dataclass(frozen=True, slots=True)
class TokenBalanceFact:
    account_index: int
    mint: str
    owner: str
    program: str
    amount: int
    decimals: int

    def __post_init__(self) -> None:
        u64(self.account_index)
        public_key(self.mint)
        public_key(self.owner)
        if self.program not in TOKEN_PROGRAMS:
            raise PublicRpcError("TOKEN_BALANCE_PROGRAM_UNSUPPORTED")
        u64(self.amount)
        if type(self.decimals) is not int or not 0 <= self.decimals <= 255:
            raise PublicRpcError("TOKEN_BALANCE_DECIMALS_INVALID")


@dataclass(frozen=True, slots=True)
class ExactTransactionFact:
    primary_signature: str
    signatures: tuple[str, ...]
    slot: int
    block_time: int | None
    version: str
    wire_bytes: bytes
    wire_sha256: str
    message_bytes: bytes
    message_sha256: str
    recent_blockhash: str
    header: tuple[int, int, int]
    static_accounts: tuple[str, ...]
    lookups: tuple[LookupFact, ...]
    loaded_writable: tuple[str, ...]
    loaded_readonly: tuple[str, ...]
    outer_instructions: tuple[InstructionFact, ...]
    inner_instructions: tuple[InstructionFact, ...]
    inner_recorded_outer_indexes: tuple[int, ...]
    pre_lamports: tuple[int, ...]
    post_lamports: tuple[int, ...]
    pre_tokens: tuple[TokenBalanceFact, ...]
    post_tokens: tuple[TokenBalanceFact, ...]
    fee_lamports: int
    outcome: TransactionOutcome
    provider_fingerprint: str
    commitment: str = "finalized"

    def __post_init__(self) -> None:
        primary_signature(self.primary_signature)
        u64(self.slot)
        if self.block_time is not None:
            u64(self.block_time)
        if self.version not in ("legacy", "v0") or self.commitment != "finalized":
            raise PublicRpcError("TRANSACTION_VERSION_OR_COMMITMENT_UNSUPPORTED")
        for value, sha in ((self.wire_bytes, self.wire_sha256), (self.message_bytes, self.message_sha256)):
            if type(value) is not bytes or hashlib.sha256(value).hexdigest() != sha:
                raise PublicRpcError("TRANSACTION_BYTES_DIGEST_MISMATCH")
        for values, kind in ((self.signatures, str), (self.header, int), (self.static_accounts, str),
                             (self.lookups, LookupFact), (self.loaded_writable, str), (self.loaded_readonly, str),
                             (self.outer_instructions, InstructionFact), (self.inner_instructions, InstructionFact),
                             (self.inner_recorded_outer_indexes, int), (self.pre_lamports, int), (self.post_lamports, int),
                             (self.pre_tokens, TokenBalanceFact), (self.post_tokens, TokenBalanceFact)):
            immutable_tuple(values, kind)
        if not self.signatures or self.signatures[0] != self.primary_signature or len(self.header) != 3:
            raise PublicRpcError("TRANSACTION_PRIMARY_SIGNATURE_MISMATCH")
        for value in self.signatures:
            primary_signature(value)
        for value in self.account_keys:
            public_key(value)
        if len(self.pre_lamports) != len(self.account_keys) or len(self.post_lamports) != len(self.account_keys):
            raise PublicRpcError("TRANSACTION_BALANCE_ACCOUNT_COUNT_MISMATCH")
        for value in (*self.header, *self.pre_lamports, *self.post_lamports, self.fee_lamports):
            u64(value)
        block_hash(self.recent_blockhash)
        _sha(self.provider_fingerprint)
        if type(self.outcome) is not TransactionOutcome:
            raise PublicRpcError("TRANSACTION_OUTCOME_REQUIRED")
        _validate_wire_fact(self)

    @property
    def account_keys(self) -> tuple[str, ...]:
        return self.static_accounts + self.loaded_writable + self.loaded_readonly


def _index(value: object, count: int) -> int:
    value = u64(value)
    if value >= count:
        raise PublicRpcError("TRANSACTION_ACCOUNT_OR_INSTRUCTION_INDEX_INVALID")
    return value


def _validate_wire_fact(fact: ExactTransactionFact) -> None:
    """A constructed immutable fact cannot contradict its own public wire."""
    try:
        tx = VersionedTransaction.from_bytes(fact.wire_bytes)
        tx.sanitize()
        msg = tx.message
        is_v0 = type(msg) is MessageV0
        lookups = tuple(LookupFact(str(item.account_key), tuple(item.writable_indexes), tuple(item.readonly_indexes))
                        for item in msg.address_table_lookups) if is_v0 else ()
        if (bytes(tx) != fact.wire_bytes or to_bytes_versioned(msg) != fact.message_bytes
                or tuple(str(value) for value in tx.signatures) != fact.signatures
                or ("v0" if is_v0 else "legacy") != fact.version
                or tuple(str(value) for value in msg.account_keys) != fact.static_accounts
                or (msg.header.num_required_signatures, msg.header.num_readonly_signed_accounts,
                    msg.header.num_readonly_unsigned_accounts) != fact.header
                or str(msg.recent_blockhash) != fact.recent_blockhash or lookups != fact.lookups):
            raise ValueError
        if (len(fact.loaded_writable) != sum(len(item.writable_indexes) for item in lookups)
                or len(fact.loaded_readonly) != sum(len(item.readonly_indexes) for item in lookups)
                or len(fact.account_keys) > 256 or len(set(fact.account_keys)) != len(fact.account_keys)):
            raise ValueError
        outer = tuple(InstructionFact(index, None, _index(item.program_id_index, len(fact.account_keys)),
                      tuple(_index(value, len(fact.account_keys)) for value in item.accounts), bytes(item.data), None)
                      for index, item in enumerate(msg.instructions))
        if outer != fact.outer_instructions:
            raise ValueError
        if tuple(sorted(set(fact.inner_recorded_outer_indexes))) != fact.inner_recorded_outer_indexes:
            raise ValueError
        for index in fact.inner_recorded_outer_indexes:
            _index(index, len(outer))
        prior_by_group = {}
        for item in fact.inner_instructions:
            if item.outer_index not in fact.inner_recorded_outer_indexes or item.inner_index != prior_by_group.get(item.outer_index, 0):
                raise ValueError
            prior_by_group[item.outer_index] = item.inner_index + 1
            _index(item.program_id_index, len(fact.account_keys))
            for value in item.account_indexes:
                _index(value, len(fact.account_keys))
        for values in (fact.pre_tokens, fact.post_tokens):
            if len({item.account_index for item in values}) != len(values):
                raise ValueError
            for item in values:
                _index(item.account_index, len(fact.account_keys))
    except Exception:
        raise PublicRpcError("TRANSACTION_TYPED_FACT_WIRE_OR_INDEX_MISMATCH") from None


def _b58data(value: object, maximum: int) -> bytes:
    if type(value) is not str or len(value) > maximum * 2:
        raise PublicRpcError("INSTRUCTION_DATA_BUDGET_EXHAUSTED")
    number = 0
    for char in value:
        index = _BASE58.find(char)
        if index < 0:
            raise PublicRpcError("INSTRUCTION_BASE58_INVALID")
        number = number * 58 + index
    zeros = len(value) - len(value.lstrip("1"))
    data = bytes(zeros) + number.to_bytes((number.bit_length()+7)//8, "big")
    if len(data) > maximum:
        raise PublicRpcError("INSTRUCTION_DATA_BUDGET_EXHAUSTED")
    return data


def _token_balances(values: object, count: int) -> tuple[TokenBalanceFact, ...]:
    if type(values) is not list or len(values) > count:
        raise PublicRpcError("TOKEN_BALANCE_METADATA_INCOMPLETE")
    result = []
    seen = set()
    for row in values:
        if type(row) is not dict or not {"accountIndex", "mint", "owner", "programId", "uiTokenAmount"} <= set(row):
            raise PublicRpcError("TOKEN_BALANCE_METADATA_INCOMPLETE")
        index = _index(row["accountIndex"], count)
        raw = row["uiTokenAmount"]
        if type(raw) is not dict or not {"amount", "decimals"} <= set(raw):
            raise PublicRpcError("TOKEN_BALANCE_METADATA_INCOMPLETE")
        text = raw["amount"]
        if type(text) is not str or re.fullmatch(r"0|[1-9][0-9]{0,19}", text) is None:
            raise PublicRpcError("TOKEN_BALANCE_RAW_AMOUNT_INVALID")
        if index in seen:
            raise PublicRpcError("TOKEN_BALANCE_DUPLICATE_ACCOUNT")
        seen.add(index)
        result.append(TokenBalanceFact(index, row["mint"], row["owner"], row["programId"], u64(int(text)), raw["decimals"]))
    return tuple(result)


def decode_transaction_result(signature: str, result: object, profile: PublicRpcProfile) -> ExactTransactionFact:
    if type(result) is not dict or not {"transaction", "meta", "slot", "version"} <= set(result):
        raise PublicRpcError("TRANSACTION_RESULT_INCOMPLETE")
    version = result["version"]
    if not (version == "legacy" or type(version) is int and version == 0):
        raise PublicRpcError("TRANSACTION_VERSION_UNSUPPORTED")
    encoded = result["transaction"]
    if type(encoded) is not list or len(encoded) != 2 or type(encoded[0]) is not str or encoded[1] != "base64":
        raise PublicRpcError("TRANSACTION_ENCODING_INVALID")
    if len(encoded[0]) > ((profile.max_transaction_wire_bytes + 2)//3)*4:
        raise PublicRpcError("TRANSACTION_WIRE_BUDGET_EXHAUSTED")
    try:
        wire = base64.b64decode(encoded[0], validate=True)
        if base64.b64encode(wire).decode("ascii") != encoded[0] or len(wire) > profile.max_transaction_wire_bytes:
            raise ValueError
        decoded = VersionedTransaction.from_bytes(wire)
        if bytes(decoded) != wire:
            raise ValueError
        decoded.sanitize()
        message = decoded.message
        message_bytes = to_bytes_versioned(message)
    except Exception:
        raise PublicRpcError("TRANSACTION_WIRE_NONCANONICAL_OR_INVALID") from None
    is_v0 = type(message) is MessageV0
    if (version == 0 and type(version) is int) != is_v0:
        raise PublicRpcError("TRANSACTION_WIRE_VERSION_MISMATCH")
    signatures = tuple(str(value) for value in decoded.signatures)
    if not signatures or signatures[0] != signature:
        raise PublicRpcError("TRANSACTION_PRIMARY_SIGNATURE_MISMATCH")
    header = (message.header.num_required_signatures, message.header.num_readonly_signed_accounts,
              message.header.num_readonly_unsigned_accounts)
    static = tuple(str(key) for key in message.account_keys)
    if len(signatures) != header[0] or header[0] < 1:
        raise PublicRpcError("TRANSACTION_SIGNATURE_HEADER_MISMATCH")
    lookups = tuple(LookupFact(str(item.account_key), tuple(item.writable_indexes), tuple(item.readonly_indexes))
                    for item in message.address_table_lookups) if is_v0 else ()
    meta = result["meta"]
    required = {"err", "fee", "preBalances", "postBalances", "preTokenBalances", "postTokenBalances", "loadedAddresses", "innerInstructions"}
    if type(meta) is not dict or not required <= set(meta):
        raise PublicRpcError("TRANSACTION_METADATA_INCOMPLETE")
    if "status" in meta and meta["status"] != ({"Ok": None} if meta["err"] is None else {"Err": meta["err"]}):
        raise PublicRpcError("TRANSACTION_METADATA_STATUS_OUTCOME_CONTRADICTION")
    loaded = meta["loadedAddresses"]
    if type(loaded) is not dict or set(loaded) != {"writable", "readonly"} or any(type(loaded[key]) is not list for key in loaded):
        raise PublicRpcError("LOADED_ADDRESS_METADATA_INCOMPLETE")
    writable = tuple(public_key(value) for value in loaded["writable"])
    readonly = tuple(public_key(value) for value in loaded["readonly"])
    if len(writable) != sum(len(item.writable_indexes) for item in lookups) or len(readonly) != sum(len(item.readonly_indexes) for item in lookups):
        raise PublicRpcError("LOADED_ADDRESS_LOOKUP_COUNT_MISMATCH")
    keys = static + writable + readonly
    if not 1 <= len(keys) <= 256 or len(set(keys)) != len(keys):
        raise PublicRpcError("TRANSACTION_ACCOUNT_KEYS_DUPLICATE_OR_OVERSIZE")
    balances = []
    for name in ("preBalances", "postBalances"):
        value = meta[name]
        if type(value) is not list or len(value) != len(keys):
            raise PublicRpcError("TRANSACTION_BALANCE_ACCOUNT_COUNT_MISMATCH")
        balances.append(tuple(u64(amount) for amount in value))
    outer = tuple(InstructionFact(index, None, _index(item.program_id_index, len(keys)),
                                  tuple(_index(account, len(keys)) for account in item.accounts), bytes(item.data), None)
                  for index, item in enumerate(message.instructions))
    inner, groups = [], []
    raw_inner = meta["innerInstructions"]
    if type(raw_inner) is not list or len(raw_inner) > len(outer):
        raise PublicRpcError("INNER_INSTRUCTION_RECORDING_INCOMPLETE")
    for group in raw_inner:
        if type(group) is not dict or set(group) != {"index", "instructions"} or type(group["instructions"]) is not list:
            raise PublicRpcError("INNER_INSTRUCTION_RECORDING_INCOMPLETE")
        index = _index(group["index"], len(outer))
        if groups and index <= groups[-1]:
            raise PublicRpcError("INNER_INSTRUCTION_GROUP_ORDER_INVALID")
        groups.append(index)
        if len(inner) + len(group["instructions"]) > profile.max_inner_instructions:
            raise PublicRpcError("INNER_INSTRUCTION_BUDGET_EXHAUSTED")
        for inner_index, item in enumerate(group["instructions"]):
            if type(item) is not dict or not {"programIdIndex", "accounts", "data"} <= set(item) or type(item["accounts"]) is not list:
                raise PublicRpcError("INNER_INSTRUCTION_FIELDS_INCOMPLETE")
            if len(item["accounts"]) > 256:
                raise PublicRpcError("INNER_INSTRUCTION_ACCOUNT_COUNT_INVALID")
            inner.append(InstructionFact(index, inner_index, _index(item["programIdIndex"], len(keys)),
                         tuple(_index(account, len(keys)) for account in item["accounts"]),
                         _b58data(item["data"], profile.max_instruction_data_bytes), item.get("stackHeight")))
    if any(len(item.data) > profile.max_instruction_data_bytes for item in outer):
        raise PublicRpcError("INSTRUCTION_DATA_BUDGET_EXHAUSTED")
    return ExactTransactionFact(signature, signatures, u64(result["slot"]), result.get("blockTime"),
        "v0" if is_v0 else "legacy", wire, hashlib.sha256(wire).hexdigest(), message_bytes,
        hashlib.sha256(message_bytes).hexdigest(), str(message.recent_blockhash), header, static, lookups,
        writable, readonly, outer, tuple(inner), tuple(groups), balances[0], balances[1],
        _token_balances(meta["preTokenBalances"], len(keys)), _token_balances(meta["postTokenBalances"], len(keys)),
        u64(meta["fee"]), _outcome(meta["err"]), profile.fingerprint)


@dataclass(frozen=True, slots=True)
class CanonicalSignatureBlock:
    slot: int
    blockhash: str
    previous_blockhash: str
    parent_slot: int
    block_height: int
    block_time: int | None
    signatures: tuple[str, ...]
    provider_fingerprint: str
    commitment: str = "finalized"

    def __post_init__(self) -> None:
        for value in (self.slot, self.parent_slot, self.block_height):
            u64(value)
        if self.block_time is not None:
            u64(self.block_time)
        block_hash(self.blockhash)
        block_hash(self.previous_blockhash)
        _sha(self.provider_fingerprint)
        immutable_tuple(self.signatures, str)
        for signature in self.signatures:
            primary_signature(signature)
        if len(set(self.signatures)) != len(self.signatures):
            raise PublicRpcError("BLOCK_DUPLICATE_PRIMARY_SIGNATURE")
        if self.block_height > self.slot or self.commitment != "finalized" or self.slot > 0 and self.parent_slot >= self.slot:
            raise PublicRpcError("BLOCK_PARENT_OR_COMMITMENT_INVALID")


def decode_signature_block(slot: int, result: object, profile: PublicRpcProfile) -> CanonicalSignatureBlock:
    if result is None:
        raise PublicRpcError("FINALIZED_BLOCK_UNAVAILABLE")
    required = {"blockhash", "previousBlockhash", "parentSlot", "blockHeight", "signatures"}
    allowed = required | {"blockTime", "rewards", "numRewardPartitions"}
    if type(result) is not dict or not required <= set(result) or set(result) - allowed:
        raise PublicRpcError("BLOCK_SIGNATURE_LIST_OR_METADATA_INCOMPLETE")
    signatures = result["signatures"]
    if type(signatures) is not list:
        raise PublicRpcError("BLOCK_SIGNATURE_LIST_OR_METADATA_INCOMPLETE")
    if len(signatures) > profile.max_block_signatures:
        raise PublicRpcError("BLOCK_SIGNATURE_BUDGET_EXHAUSTED")
    return CanonicalSignatureBlock(slot, result["blockhash"], result["previousBlockhash"], result["parentSlot"],
                                    result["blockHeight"], result.get("blockTime"), tuple(signatures), profile.fingerprint)


@dataclass(frozen=True, slots=True)
class TransactionRequest:
    genesis_hash: str
    signature: str
    min_finalized_root_slot: int

    def __post_init__(self) -> None:
        block_hash(self.genesis_hash)
        primary_signature(self.signature)
        u64(self.min_finalized_root_slot)


@dataclass(frozen=True, slots=True)
class TransactionReadFailure:
    operation: str
    code: str

    def __post_init__(self) -> None:
        public_label(self.operation)
        public_label(self.code)


@dataclass(frozen=True, slots=True)
class TransactionObservation:
    request: TransactionRequest
    profile: PublicRpcProfile
    started_at_utc: str
    observed_at_utc: str
    genesis_start: str | None
    genesis_end: str | None
    root: FinalizedBlockAnchor | None
    status: SignatureStatusFact | None
    transaction: ExactTransactionFact | None
    membership_block: CanonicalSignatureBlock | None
    failures: tuple[TransactionReadFailure, ...]
    schema: str = SCHEMA

    def __post_init__(self) -> None:
        if type(self.request) is not TransactionRequest or type(self.profile) is not PublicRpcProfile or self.schema != SCHEMA:
            raise PublicRpcError("TRANSACTION_OBSERVATION_DOMAIN_INVALID")
        for field in ("started_at_utc", "observed_at_utc"):
            object.__setattr__(self, field, utc(getattr(self, field)))
        for genesis in (self.genesis_start, self.genesis_end):
            if genesis is not None:
                block_hash(genesis)
        for value, kind in ((self.root, FinalizedBlockAnchor), (self.status, SignatureStatusFact),
                            (self.transaction, ExactTransactionFact), (self.membership_block, CanonicalSignatureBlock)):
            if value is not None and type(value) is not kind:
                raise PublicRpcError("IMMUTABLE_TRANSACTION_FACT_REQUIRED")
        immutable_tuple(self.failures, TransactionReadFailure)

    @property
    def content_digest(self) -> str:
        return evidence_fingerprint(self)


class TransactionEvidenceAdapter:
    def __init__(self, rpc: PublicReadOnlyRpc, request: TransactionRequest, *, clock: Callable[[], str] | None = None) -> None:
        if type(rpc) is not PublicReadOnlyRpc or type(request) is not TransactionRequest:
            raise PublicRpcError("BOUND_TRANSACTION_ADAPTER_REQUIRED")
        self._rpc, self._request, self._used = rpc, request, False
        self._clock = clock or (lambda: datetime.now(timezone.utc).isoformat())

    def observe(self) -> TransactionObservation:
        if self._used:
            raise PublicRpcError("BOUNDED_OBSERVATION_ALREADY_USED")
        self._used = True
        started = utc(self._clock())
        failures = []
        genesis_start = genesis_end = root = status = transaction = membership = None

        def read(operation: str, call):
            try:
                return call()
            except PublicRpcError as exc:
                failures.append(TransactionReadFailure(operation, str(exc)))
                return None
        genesis_start = read("GENESIS_START", self._rpc.get_genesis_hash)
        if genesis_start == self._request.genesis_hash:
            status = read("SIGNATURE_STATUS", lambda: self._rpc.get_signature_status(self._request.signature))
            transaction = read("EXACT_TRANSACTION", lambda: self._rpc.get_finalized_transaction(self._request.signature))
            if transaction is not None:
                membership = read("TRANSACTION_BLOCK", lambda: self._rpc.get_finalized_signature_block(transaction.slot))
            slot = read("FINALIZED_ROOT_SLOT", lambda: self._rpc.get_finalized_slot(min_context_slot=self._request.min_finalized_root_slot))
            if slot is not None:
                root = read("FINALIZED_ROOT", lambda: self._rpc.get_finalized_block_anchor(slot))
            genesis_end = read("GENESIS_END", self._rpc.get_genesis_hash)
        return TransactionObservation(self._request, self._rpc.profile, started, utc(self._clock()),
                                       genesis_start, genesis_end, root, status, transaction, membership, tuple(failures))


def observation_domain_reasons(*, profile: PublicRpcProfile, expected_profile: str, genesis_start: str | None,
                               genesis_end: str | None, expected_genesis: str, root: FinalizedBlockAnchor | None,
                               started_at: str, observed_at: str, now: str) -> list[str]:
    reasons = []
    if profile.fingerprint != expected_profile:
        reasons.append("CONSUMER_PROVIDER_PROFILE_MISMATCH")
    if genesis_start != expected_genesis or genesis_end != expected_genesis:
        reasons.append("GENESIS_UNAVAILABLE_OR_CHANGED")
    current, observed, started = epoch(utc(now)), epoch(observed_at), epoch(started_at)
    if observed < started or observed - started >= profile.observation_timeout_seconds:
        reasons.append("OBSERVATION_CLOCK_OR_DURATION_INVALID")
    if current < observed or current - observed >= profile.observation_freshness_seconds:
        reasons.append("OBSERVATION_STALE_OR_FUTURE")
    if root is None:
        reasons.append("FINALIZED_ROOT_UNAVAILABLE")
    elif root.provider_fingerprint != profile.fingerprint:
        reasons.append("FINALIZED_ROOT_PROVIDER_MISMATCH")
    elif root.block_time > observed or current - root.block_time >= profile.finalized_block_freshness_seconds:
        reasons.append("FINALIZED_ROOT_STALE_OR_FUTURE")
    return reasons


@dataclass(frozen=True, slots=True)
class LedgerTransactionEvidence:
    observation: TransactionObservation
    disposition: str
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.observation) is not TransactionObservation or self.disposition not in ("SUPPORTED_FINALIZED_OBSERVATION", "UNKNOWN", "CONTRADICTORY"):
            raise PublicRpcError("INVALID_LEDGER_TRANSACTION_EVIDENCE")
        immutable_tuple(self.reasons, str)
        if self.disposition == "SUPPORTED_FINALIZED_OBSERVATION" and self.reasons:
            raise PublicRpcError("UNRESOLVED_TRANSACTION_CANNOT_BE_SUPPORTED")


def ledger_transaction_evidence(observation: TransactionObservation, *, expected_genesis: str,
                                expected_signature: str, expected_profile_fingerprint: str, now_utc: str,
                                expected_message_sha256: str | None = None) -> LedgerTransactionEvidence:
    primary_signature(expected_signature)
    block_hash(expected_genesis)
    if expected_message_sha256 is not None:
        _sha(expected_message_sha256)
    reasons = observation_domain_reasons(profile=observation.profile, expected_profile=expected_profile_fingerprint,
        genesis_start=observation.genesis_start, genesis_end=observation.genesis_end, expected_genesis=expected_genesis,
        root=observation.root, started_at=observation.started_at_utc, observed_at=observation.observed_at_utc, now=now_utc)
    contradictory = False
    if observation.request.signature != expected_signature or observation.request.genesis_hash != expected_genesis:
        reasons.append("CONSUMER_TRANSACTION_BINDING_MISMATCH")
    reasons.extend(f"{item.operation}:{item.code}" for item in observation.failures)
    if any("MISMATCH" in item.code or "CONTRADICTION" in item.code or "DUPLICATE" in item.code for item in observation.failures):
        contradictory = True
    transaction, status, block, root = observation.transaction, observation.status, observation.membership_block, observation.root
    if transaction is None:
        reasons.append("EXACT_FINALIZED_METADATA_UNAVAILABLE")
    else:
        if transaction.primary_signature != expected_signature or transaction.provider_fingerprint != observation.profile.fingerprint:
            reasons.append("EXACT_TRANSACTION_IDENTITY_MISMATCH")
            contradictory = True
        if expected_message_sha256 is not None and transaction.message_sha256 != expected_message_sha256:
            reasons.append("EXPECTED_MESSAGE_DIGEST_MISMATCH")
            contradictory = True
        if transaction.block_time is not None and transaction.block_time > epoch(observation.observed_at_utc):
            reasons.append("TRANSACTION_TIME_IN_FUTURE")
        if root is not None and (transaction.slot > root.slot or root.slot < observation.request.min_finalized_root_slot):
            reasons.append("TRANSACTION_FINALIZED_ROOT_BOUNDS_MISMATCH")
            contradictory = True
        if status is not None:
            if status.signature != expected_signature:
                reasons.append("SIGNATURE_STATUS_IDENTITY_MISMATCH")
                contradictory = True
            if status.slot is not None and (status.slot != transaction.slot or status.outcome != transaction.outcome):
                reasons.append("STATUS_TRANSACTION_SLOT_OR_OUTCOME_CONTRADICTION")
                contradictory = True
        if block is None:
            reasons.append("TRANSACTION_CANONICAL_MEMBERSHIP_UNAVAILABLE")
        elif (block.slot != transaction.slot or block.provider_fingerprint != observation.profile.fingerprint
              or expected_signature not in block.signatures
              or block.block_time is not None and transaction.block_time is not None and block.block_time != transaction.block_time):
            reasons.append("TRANSACTION_CANONICAL_MEMBERSHIP_CONTRADICTION")
            contradictory = True
        if block is not None and root is not None:
            block_core = (block.blockhash, block.previous_blockhash, block.parent_slot, block.block_height)
            root_core = (root.blockhash, root.previous_blockhash, root.parent_slot, root.block_height)
            if (block.slot == root.slot and block_core != root_core
                    or block.slot < root.slot and block.block_height >= root.block_height
                    or block.slot < root.slot and root.block_height - block.block_height > root.slot - block.slot):
                reasons.append("TRANSACTION_BLOCK_AND_FINALIZED_ROOT_CONTRADICTION")
                contradictory = True
    disposition = "CONTRADICTORY" if contradictory else "UNKNOWN" if reasons else "SUPPORTED_FINALIZED_OBSERVATION"
    return LedgerTransactionEvidence(observation, disposition, tuple(sorted(set(reasons))))


@dataclass(frozen=True, slots=True)
class RuntimeTransactionOrder:
    left_evidence_digest: str
    right_evidence_digest: str
    relation: str
    reasons: tuple[str, ...]
    scope: str = "CANONICAL_TRANSACTION_ORDER_ONLY"

    def __post_init__(self) -> None:
        _sha(self.left_evidence_digest)
        _sha(self.right_evidence_digest)
        immutable_tuple(self.reasons, str)
        if self.relation not in ("BEFORE", "AFTER", "UNKNOWN") or self.scope != "CANONICAL_TRANSACTION_ORDER_ONLY":
            raise PublicRpcError("INVALID_RUNTIME_TRANSACTION_ORDER")


def runtime_transaction_order(left: TransactionObservation, right: TransactionObservation, *,
                              expected_genesis: str, expected_profile_fingerprint: str, now_utc: str) -> RuntimeTransactionOrder:
    reasons = []
    for observation in (left, right):
        result = ledger_transaction_evidence(observation, expected_genesis=expected_genesis,
            expected_signature=observation.request.signature, expected_profile_fingerprint=expected_profile_fingerprint, now_utc=now_utc)
        if result.disposition != "SUPPORTED_FINALIZED_OBSERVATION":
            reasons.append("TRANSACTION_ORDER_EVIDENCE_UNRESOLVED")
    if not reasons:
        # Compare only anchors already retained in the two observations. This
        # catches direct contradictions without inventing unseen ancestry.
        anchors = (left.root, left.membership_block, right.root, right.membership_block)
        for index, a in enumerate(anchors):
            for b in anchors[index+1:]:
                if a.slot == b.slot:
                    if ((a.blockhash, a.previous_blockhash, a.parent_slot, a.block_height)
                            != (b.blockhash, b.previous_blockhash, b.parent_slot, b.block_height)
                            or a.block_time is not None and b.block_time is not None and a.block_time != b.block_time):
                        reasons.append("KNOWN_FINALIZED_ANCHOR_CONTRADICTION")
                else:
                    child, parent = (a, b) if a.slot > b.slot else (b, a)
                    if (not 0 < child.block_height - parent.block_height <= child.slot - parent.slot
                            or child.parent_slot == parent.slot and (child.previous_blockhash != parent.blockhash
                                or child.block_height != parent.block_height + 1)):
                        reasons.append("KNOWN_FINALIZED_ANCHOR_CONTRADICTION")
    relation = "UNKNOWN"
    if not reasons:
        if left.request.signature == right.request.signature:
            reasons.append("EVENT_ORDER_REQUIRED_WITHIN_SAME_TRANSACTION")
        elif left.transaction.slot == right.transaction.slot:
            if left.membership_block != right.membership_block:
                reasons.append("SAME_SLOT_BLOCK_EVIDENCE_CONTRADICTION")
            else:
                a = left.membership_block.signatures.index(left.request.signature)
                b = right.membership_block.signatures.index(right.request.signature)
                relation = "BEFORE" if a < b else "AFTER"
        else:
            relation = "BEFORE" if left.transaction.slot < right.transaction.slot else "AFTER"
    return RuntimeTransactionOrder(left.content_digest, right.content_digest, relation, tuple(sorted(set(reasons))))
