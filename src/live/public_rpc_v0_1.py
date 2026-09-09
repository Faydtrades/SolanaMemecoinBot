"""Bounded LIVE public Solana RPC; no arbitrary method or mutation surface."""
from __future__ import annotations

import base64
import binascii
import json
import math
import re
import time
from dataclasses import asdict, dataclass, is_dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

import httpx
from solders.hash import Hash
from solders.pubkey import Pubkey
from solders.signature import Signature

from phase5.shadow_domain_v0_1 import content_fingerprint
from phase5.shadow_venue_route_quote_v0_1 import (
    RpcAccountV01, TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID,
)


if TYPE_CHECKING:
    from .transaction_evidence_v0_1 import CanonicalSignatureBlock, ExactTransactionFact, SignatureStatusFact


MODEL_ID = "LIVE-PUBLIC-SOLANA-RPC-0002"
TOKEN_PROGRAMS = (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID)
MAX_U64 = (1 << 64) - 1


class PublicRpcError(ValueError):
    """Only a fixed local code is exposed; transport bodies are discarded."""


def u64(value: object) -> int:
    if type(value) is not int or not 0 <= value <= MAX_U64:
        raise PublicRpcError("INVALID_U64")
    return value


def public_key(value: object) -> str:
    try:
        if type(value) is not str or str(Pubkey.from_string(value)) != value:
            raise ValueError
        return value
    except Exception:
        raise PublicRpcError("INVALID_PUBLIC_KEY") from None


def block_hash(value: object) -> str:
    try:
        if type(value) is not str or str(Hash.from_string(value)) != value:
            raise ValueError
        return value
    except Exception:
        raise PublicRpcError("INVALID_BLOCK_HASH") from None


def primary_signature(value: object) -> str:
    try:
        if type(value) is not str or str(Signature.from_string(value)) != value:
            raise ValueError
        return value
    except Exception:
        raise PublicRpcError("INVALID_PRIMARY_SIGNATURE") from None


def immutable_tuple(value: tuple, kind: type) -> None:
    if type(value) is not tuple or any(type(item) is not kind for item in value):
        raise PublicRpcError("IMMUTABLE_TYPED_TUPLE_REQUIRED")


def public_label(value: str) -> str:
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,96}", value) is None:
        raise PublicRpcError("INVALID_PUBLIC_PROFILE_LABEL")
    return value


def _primitive(value: object) -> object:
    if is_dataclass(value):
        return _primitive(asdict(value))
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {key: _primitive(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_primitive(item) for item in value]
    return value


def evidence_fingerprint(value: object) -> str:
    return content_fingerprint({"model": MODEL_ID, "evidence": _primitive(value)})


@dataclass(frozen=True, slots=True)
class PublicRpcProfile:
    provider_id: str
    provider_version: str
    max_response_bytes: int = 1048576
    max_account_bytes: int = 4096
    max_inventory_accounts_per_program: int = 64
    max_requests: int = 320
    max_total_response_bytes: int = 67108864
    request_timeout_seconds: int = 10
    observation_timeout_seconds: int = 180
    observation_freshness_seconds: int = 30
    finalized_block_freshness_seconds: int = 120
    max_block_signatures: int = 8192
    max_transaction_wire_bytes: int = 1232
    max_inner_instructions: int = 1024
    max_instruction_data_bytes: int = 16384
    trust_profile: str = "HONEST-COMPLETE-STANDARD-SOLANA-RPC-0001"

    def __post_init__(self) -> None:
        public_label(self.provider_id)
        public_label(self.provider_version)
        for name, maximum in (("max_response_bytes", 4194304), ("max_account_bytes", 65536),
                              ("max_inventory_accounts_per_program", 256), ("max_requests", 512),
                              ("max_total_response_bytes", 134217728), ("request_timeout_seconds", 30),
                              ("observation_timeout_seconds", 300), ("observation_freshness_seconds", 3600),
                              ("finalized_block_freshness_seconds", 3600), ("max_block_signatures", 32768),
                              ("max_transaction_wire_bytes", 1232), ("max_inner_instructions", 4096),
                              ("max_instruction_data_bytes", 65536)):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise PublicRpcError("INVALID_RPC_PROFILE_BOUND")
        if self.trust_profile != "HONEST-COMPLETE-STANDARD-SOLANA-RPC-0001":
            raise PublicRpcError("UNSUPPORTED_RPC_TRUST_PROFILE")

    @property
    def fingerprint(self) -> str:
        return evidence_fingerprint(self)


@dataclass(frozen=True, slots=True)
class RpcContext:
    slot: int
    api_version: str
    commitment: str = "finalized"

    def __post_init__(self) -> None:
        u64(self.slot)
        public_label(self.api_version)
        if self.commitment != "finalized":
            raise PublicRpcError("FINALIZED_CONTEXT_REQUIRED")


@dataclass(frozen=True, slots=True)
class PublicAccount:
    account: RpcAccountV01
    lamports: int
    executable: bool
    rent_epoch: int

    def __post_init__(self) -> None:
        if type(self.account) is not RpcAccountV01 or type(self.account.data) is not bytes:
            raise PublicRpcError("IMMUTABLE_PUBLIC_ACCOUNT_REQUIRED")
        u64(self.lamports)
        u64(self.rent_epoch)
        if type(self.executable) is not bool:
            raise PublicRpcError("INVALID_EXECUTABLE_FLAG")


@dataclass(frozen=True, slots=True)
class PublicAccountRead:
    context: RpcContext
    requested_keys: tuple[str, ...]
    accounts: tuple[PublicAccount | None, ...]

    def __post_init__(self) -> None:
        if type(self.context) is not RpcContext:
            raise PublicRpcError("IMMUTABLE_CONTEXT_REQUIRED")
        immutable_tuple(self.requested_keys, str)
        if not 1 <= len(self.requested_keys) <= 100 or len(set(self.requested_keys)) != len(self.requested_keys):
            raise PublicRpcError("INVALID_ACCOUNT_KEY_COUNT")
        for key in self.requested_keys:
            public_key(key)
        if type(self.accounts) is not tuple or len(self.accounts) != len(self.requested_keys):
            raise PublicRpcError("ACCOUNT_COUNT_MISMATCH")
        for key, value in zip(self.requested_keys, self.accounts, strict=True):
            if value is not None and (type(value) is not PublicAccount or value.account.pubkey != key):
                raise PublicRpcError("ACCOUNT_KEY_MISMATCH")


@dataclass(frozen=True, slots=True)
class TokenInventoryRead:
    wallet: str
    program: str
    context: RpcContext
    accounts: tuple[PublicAccount, ...]

    def __post_init__(self) -> None:
        public_key(self.wallet)
        if self.program not in TOKEN_PROGRAMS or type(self.context) is not RpcContext:
            raise PublicRpcError("INVALID_TOKEN_INVENTORY_DOMAIN")
        immutable_tuple(self.accounts, PublicAccount)
        keys = tuple(value.account.pubkey for value in self.accounts)
        if len(keys) != len(set(keys)):
            raise PublicRpcError("DUPLICATE_INVENTORY_ACCOUNT")
        if any(value.account.owner != self.program for value in self.accounts):
            raise PublicRpcError("INVENTORY_PROGRAM_MISMATCH")


@dataclass(frozen=True, slots=True)
class FinalizedBlockAnchor:
    slot: int
    blockhash: str
    previous_blockhash: str
    parent_slot: int
    block_height: int
    block_time: int
    provider_fingerprint: str
    commitment: str = "finalized"

    def __post_init__(self) -> None:
        for item in (self.slot, self.parent_slot, self.block_height, self.block_time):
            u64(item)
        block_hash(self.blockhash)
        block_hash(self.previous_blockhash)
        if self.block_height > self.slot or (self.slot > 0 and self.parent_slot >= self.slot) or self.commitment != "finalized":
            raise PublicRpcError("INVALID_FINALIZED_BLOCK_ANCHOR")
        if re.fullmatch(r"[0-9a-f]{64}", self.provider_fingerprint) is None:
            raise PublicRpcError("INVALID_PROVIDER_FINGERPRINT")


class _ReadMethod(StrEnum):
    GENESIS = "getGenesisHash"
    SLOT = "getSlot"
    BLOCK = "getBlock"
    ACCOUNTS = "getMultipleAccounts"
    TOKEN_ACCOUNTS = "getTokenAccountsByOwner"
    SIGNATURE_STATUS = "getSignatureStatuses"
    TRANSACTION = "getTransaction"


READ_ONLY_METHODS = tuple(method.value for method in _ReadMethod)


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _reject_number(_text: str) -> object:
    raise ValueError


def _finite_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):
        raise ValueError
    return value


def _json(content: bytes) -> dict:
    try:
        result = json.loads(content.decode("utf-8"), object_pairs_hook=_unique_object,
                            parse_constant=_reject_number, parse_float=_finite_float)
        if type(result) is not dict:
            raise ValueError
        return result
    except (ValueError, UnicodeError, RecursionError):
        raise PublicRpcError("INVALID_STRICT_JSON") from None


class PublicReadOnlyRpc:
    """Fixed methods and explicit budgets for one bounded public observation.

    No retry/reset, arbitrary-call, simulation or mutation method.
    The endpoint stays private transport configuration and is never evidence.
    """

    def __init__(self, endpoint: str, profile: PublicRpcProfile, *,
                 transport: httpx.BaseTransport | None = None) -> None:
        if type(profile) is not PublicRpcProfile:
            raise PublicRpcError("PUBLIC_RPC_PROFILE_REQUIRED")
        try:
            url = httpx.URL(endpoint)
            if url.scheme not in ("http", "https") or not url.host or url.fragment:
                raise ValueError
        except Exception:
            raise PublicRpcError("INVALID_RPC_ENDPOINT") from None
        self.profile = profile
        self._endpoint = url
        self._requests = 0
        self._response_bytes = 0
        self._started = time.monotonic()
        self._client = httpx.Client(transport=transport, trust_env=False, follow_redirects=False,
                                   headers={"Content-Type": "application/json", "Accept-Encoding": "identity"},
                                   timeout=httpx.Timeout(profile.request_timeout_seconds))

    def __enter__(self) -> PublicReadOnlyRpc:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def _post(self, method: _ReadMethod, params: list) -> object:
        if type(method) is not _ReadMethod:
            raise PublicRpcError("RPC_METHOD_NOT_ALLOWED")
        if self._requests >= self.profile.max_requests:
            raise PublicRpcError("RPC_REQUEST_BUDGET_EXHAUSTED")
        if time.monotonic() - self._started >= self.profile.observation_timeout_seconds:
            raise PublicRpcError("RPC_OBSERVATION_TIMEOUT")
        self._requests += 1
        request_id = self._requests
        request = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method.value, "params": params},
                             separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(request) > 16384:
            raise PublicRpcError("RPC_REQUEST_TOO_LARGE")
        try:
            with self._client.stream("POST", self._endpoint, content=request) as response:
                if response.status_code != 200:
                    raise PublicRpcError("RPC_HTTP_FAILURE")
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise PublicRpcError("RPC_CONTENT_ENCODING_UNSUPPORTED")
                if response.headers.get("content-type", "").split(";", 1)[0].lower() != "application/json":
                    raise PublicRpcError("RPC_CONTENT_TYPE_INVALID")
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=8192):
                    self._response_bytes += len(chunk)
                    if len(content) + len(chunk) > self.profile.max_response_bytes or self._response_bytes > self.profile.max_total_response_bytes:
                        raise PublicRpcError("RPC_RESPONSE_BUDGET_EXHAUSTED")
                    if time.monotonic() - self._started >= self.profile.observation_timeout_seconds:
                        raise PublicRpcError("RPC_OBSERVATION_TIMEOUT")
                    content.extend(chunk)
        except (httpx.HTTPError, OSError):
            raise PublicRpcError("RPC_TRANSPORT_FAILURE") from None
        envelope = _json(bytes(content))
        if set(envelope) != {"jsonrpc", "id", "result"}:
            raise PublicRpcError("RPC_ERROR_OR_INVALID_ENVELOPE")
        if envelope["jsonrpc"] != "2.0" or type(envelope["id"]) is not int or envelope["id"] != request_id:
            raise PublicRpcError("RPC_ENVELOPE_ID_OR_VERSION_MISMATCH")
        return envelope["result"]

    @staticmethod
    def _context(result: object, floor: int) -> tuple[RpcContext, object]:
        if type(result) is not dict or set(result) != {"context", "value"}:
            raise PublicRpcError("RPC_CONTEXT_RESULT_INVALID")
        context = result["context"]
        if type(context) is not dict or "slot" not in context or set(context) - {"slot", "apiVersion"}:
            raise PublicRpcError("RPC_CONTEXT_INVALID")
        parsed = RpcContext(context["slot"], context.get("apiVersion", "UNREPORTED"))
        if parsed.slot < floor:
            raise PublicRpcError("RPC_CONTEXT_BELOW_FLOOR")
        return parsed, result["value"]

    def _account(self, key: str, value: object) -> PublicAccount:
        if type(value) is not dict or not {"owner", "lamports", "executable", "rentEpoch", "data"} <= set(value):
            raise PublicRpcError("RPC_ACCOUNT_FIELDS_MISSING")
        data = value["data"]
        if type(data) is not list or len(data) != 2 or type(data[0]) is not str or data[1] != "base64":
            raise PublicRpcError("RPC_ACCOUNT_ENCODING_INVALID")
        if len(data[0]) > ((self.profile.max_account_bytes + 2) // 3) * 4:
            raise PublicRpcError("RPC_ACCOUNT_BYTES_EXHAUSTED")
        try:
            raw = base64.b64decode(data[0], validate=True)
            if base64.b64encode(raw).decode("ascii") != data[0]:
                raise ValueError
        except (ValueError, binascii.Error):
            raise PublicRpcError("RPC_ACCOUNT_BASE64_INVALID") from None
        if len(raw) > self.profile.max_account_bytes:
            raise PublicRpcError("RPC_ACCOUNT_BYTES_EXHAUSTED")
        if "space" in value and u64(value["space"]) != len(raw):
            raise PublicRpcError("RPC_ACCOUNT_SPACE_MISMATCH")
        return PublicAccount(RpcAccountV01(public_key(key), public_key(value["owner"]), raw),
                             u64(value["lamports"]), value["executable"], u64(value["rentEpoch"]))

    def get_genesis_hash(self) -> str:
        return block_hash(self._post(_ReadMethod.GENESIS, []))

    def get_finalized_slot(self, *, min_context_slot: int) -> int:
        floor = u64(min_context_slot)
        slot = u64(self._post(_ReadMethod.SLOT, [{"commitment": "finalized", "minContextSlot": floor}]))
        if slot < floor:
            raise PublicRpcError("RPC_CONTEXT_BELOW_FLOOR")
        return slot

    def get_finalized_block_anchor(self, slot: int) -> FinalizedBlockAnchor:
        slot = u64(slot)
        result = self._post(_ReadMethod.BLOCK, [slot, {"commitment": "finalized",
                            "transactionDetails": "none", "rewards": False, "maxSupportedTransactionVersion": 0}])
        if result is None:
            raise PublicRpcError("FINALIZED_BLOCK_UNAVAILABLE")
        if type(result) is not dict or not {"blockhash", "previousBlockhash", "parentSlot", "blockHeight", "blockTime"} <= set(result):
            raise PublicRpcError("FINALIZED_BLOCK_METADATA_MISSING")
        if "transactions" in result or "signatures" in result:
            raise PublicRpcError("FINALIZED_BLOCK_DETAILS_UNREQUESTED")
        return FinalizedBlockAnchor(slot, result["blockhash"], result["previousBlockhash"],
                                    result["parentSlot"], result["blockHeight"], result["blockTime"],
                                    self.profile.fingerprint)

    def get_multiple_accounts(self, keys: tuple[str, ...], *, min_context_slot: int) -> PublicAccountRead:
        immutable_tuple(keys, str)
        if not 1 <= len(keys) <= 100 or len(set(keys)) != len(keys):
            raise PublicRpcError("INVALID_ACCOUNT_KEY_COUNT")
        for key in keys:
            public_key(key)
        floor = u64(min_context_slot)
        context, values = self._context(self._post(_ReadMethod.ACCOUNTS,
            [list(keys), {"commitment": "finalized", "encoding": "base64", "minContextSlot": floor}]), floor)
        if type(values) is not list or len(values) != len(keys):
            raise PublicRpcError("ACCOUNT_COUNT_MISMATCH")
        return PublicAccountRead(context, keys, tuple(None if value is None else self._account(key, value)
                                                     for key, value in zip(keys, values, strict=True)))

    def get_token_accounts_by_owner(self, wallet: str, program: str, *, min_context_slot: int) -> TokenInventoryRead:
        public_key(wallet)
        if program not in TOKEN_PROGRAMS:
            raise PublicRpcError("TOKEN_PROGRAM_NOT_ALLOWED")
        floor = u64(min_context_slot)
        context, values = self._context(self._post(_ReadMethod.TOKEN_ACCOUNTS,
            [wallet, {"programId": program}, {"commitment": "finalized", "encoding": "base64", "minContextSlot": floor}]), floor)
        if type(values) is not list or len(values) > self.profile.max_inventory_accounts_per_program:
            raise PublicRpcError("INVENTORY_ACCOUNT_BUDGET_EXHAUSTED")
        accounts = []
        for row in values:
            if type(row) is not dict or set(row) != {"pubkey", "account"} or row["account"] is None:
                raise PublicRpcError("INVENTORY_ENTRY_INVALID")
            accounts.append(self._account(public_key(row["pubkey"]), row["account"]))
        return TokenInventoryRead(wallet, program, context, tuple(accounts))

    def get_signature_status(self, signature: str) -> SignatureStatusFact:
        from .transaction_evidence_v0_1 import decode_signature_status
        primary_signature(signature)
        result = self._post(_ReadMethod.SIGNATURE_STATUS, [[signature], {"searchTransactionHistory": True}])
        return decode_signature_status(signature, result)

    def get_finalized_transaction(self, signature: str) -> ExactTransactionFact | None:
        from .transaction_evidence_v0_1 import decode_transaction_result
        primary_signature(signature)
        result = self._post(_ReadMethod.TRANSACTION, [signature, {"commitment": "finalized",
                            "encoding": "base64", "maxSupportedTransactionVersion": 0}])
        return None if result is None else decode_transaction_result(signature, result, self.profile)

    def get_finalized_signature_block(self, slot: int) -> CanonicalSignatureBlock:
        from .transaction_evidence_v0_1 import decode_signature_block
        slot = u64(slot)
        result = self._post(_ReadMethod.BLOCK, [slot, {"commitment": "finalized", "transactionDetails": "signatures",
                            "rewards": False, "maxSupportedTransactionVersion": 0}])
        return decode_signature_block(slot, result, self.profile)
