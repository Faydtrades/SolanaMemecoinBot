from __future__ import annotations

import base64
import binascii
import json
from enum import StrEnum
from typing import Any, Mapping

import httpx
from solders.hash import Hash
from solders.pubkey import Pubkey

from .shadow_domain_v0_1 import content_fingerprint
from .shadow_unsigned_plan_simulation_v0_1 import (
    BlockhashValidityResponseV01,
    BlockHeightResponseV01,
    LatestBlockhashResponseV01,
    SimulationRpcResponseV01,
)
from .shadow_venue_route_quote_v0_1 import RpcAccountBatchV01, RpcAccountV01


MODEL_ID = "P5-STRICT-READONLY-SOLANA-JSON-RPC-0001"
SCHEMA_VERSION = "phase5_strict_readonly_solana_json_rpc_v0.1"
MAX_U64 = (1 << 64) - 1
DEFAULT_READ_TIMEOUT_SECONDS = 20.0
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0

CONTRACT_SPEC: Mapping[str, Any] = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "transport": "SYNCHRONOUS_HTTP_JSON_RPC_2_0_POST",
    "content_type": "application/json",
    "timeouts_seconds": {"connect": 10, "read": 20, "write": 20, "pool": 20},
    "methods": [
        "getMultipleAccounts",
        "getLatestBlockhash",
        "getBlockHeight",
        "isBlockhashValid",
        "simulateTransaction",
    ],
    "request_ids": "DETERMINISTIC_INSTANCE_LOCAL_MONOTONIC_INTEGERS",
    "automatic_retries": 0,
    "account_encoding": "base64",
    "account_commitment": "confirmed",
    "simulation_config": "CALLER_SUPPLIED_ACCEPTED_T003_RPC_PAYLOAD_EXACT",
    "response_policy": "STRICT_ENVELOPE_FAIL_CLOSED_TYPED_ERRORS",
}
MODEL_FINGERPRINT = content_fingerprint(CONTRACT_SPEC)


class ShadowRpcError(RuntimeError):
    pass


class ShadowRpcTransportError(ShadowRpcError):
    pass


class ShadowRpcProtocolError(ShadowRpcError):
    pass


class ShadowRpcResponseError(ShadowRpcError):
    pass


class _RpcMethod(StrEnum):
    GET_MULTIPLE_ACCOUNTS = "getMultipleAccounts"
    GET_LATEST_BLOCKHASH = "getLatestBlockhash"
    GET_BLOCK_HEIGHT = "getBlockHeight"
    IS_BLOCKHASH_VALID = "isBlockhashValid"
    SIMULATE_TRANSACTION = "simulateTransaction"


def _strict_u64(label: str, value: Any) -> int:
    if type(value) is not int:
        raise ShadowRpcResponseError(f"{label} must be an integer")
    if value < 0 or value > MAX_U64:
        raise ShadowRpcResponseError(f"{label} is outside the unsigned 64-bit range")
    return value


def _required_text(label: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ShadowRpcResponseError(f"{label} must be a non-empty string")
    return value


def _request_u64(label: str, value: Any) -> int:
    try:
        return _strict_u64(label, value)
    except ShadowRpcResponseError as exc:
        raise ShadowRpcProtocolError(str(exc)) from None


def _request_text(label: str, value: Any) -> str:
    try:
        return _required_text(label, value)
    except ShadowRpcResponseError as exc:
        raise ShadowRpcProtocolError(str(exc)) from None


def _context_result(result: Any, *, label: str) -> tuple[int, Any]:
    if not isinstance(result, dict):
        raise ShadowRpcResponseError(f"{label} result must be an object")
    context = result.get("context")
    if not isinstance(context, dict) or "slot" not in context:
        raise ShadowRpcResponseError(f"{label} context must contain slot")
    if "value" not in result:
        raise ShadowRpcResponseError(f"{label} result is missing value")
    return _strict_u64(f"{label} context slot", context["slot"]), result["value"]


def _reject_json_constant(_value: str) -> Any:
    raise ValueError("non-standard JSON constant")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _strict_json_loads(content: bytes) -> Any:
    try:
        text = content.decode("utf-8")
        return json.loads(
            text,
            parse_constant=_reject_json_constant,
            object_pairs_hook=_unique_json_object,
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise ShadowRpcResponseError("RPC response is not strict UTF-8 JSON") from None


class StrictReadOnlySolanaRpcV01:
    def __init__(
        self,
        rpc_http_url: str,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not isinstance(rpc_http_url, str) or not rpc_http_url:
            raise ShadowRpcProtocolError("RPC HTTP URL is required")
        try:
            parsed = httpx.URL(rpc_http_url)
        except Exception:
            raise ShadowRpcProtocolError("RPC HTTP URL is invalid") from None
        if parsed.scheme not in ("http", "https") or not parsed.host:
            raise ShadowRpcProtocolError("RPC HTTP URL must use HTTP or HTTPS")
        self._rpc_http_url = rpc_http_url
        self._next_request_id = 1
        self._client = httpx.Client(
            headers={"Content-Type": "application/json"},
            timeout=httpx.Timeout(
                DEFAULT_READ_TIMEOUT_SECONDS,
                connect=DEFAULT_CONNECT_TIMEOUT_SECONDS,
            ),
            transport=transport,
        )

    def __enter__(self) -> StrictReadOnlySolanaRpcV01:
        return self

    def __exit__(self, *_args: object) -> None:
        self._client.close()

    def _post(self, operation: _RpcMethod, params: list[Any]) -> Any:
        if not isinstance(operation, _RpcMethod):
            raise ShadowRpcProtocolError("RPC method is not part of the closed adapter surface")
        request_id = self._next_request_id
        self._next_request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": operation.value,
            "params": params,
        }
        try:
            response = self._client.post(self._rpc_http_url, json=payload)
        except (httpx.TimeoutException, httpx.TransportError):
            raise ShadowRpcTransportError("RPC transport request failed") from None
        if response.status_code < 200 or response.status_code >= 300:
            raise ShadowRpcTransportError(
                f"RPC HTTP response status was {response.status_code}"
            )
        envelope = _strict_json_loads(response.content)
        if not isinstance(envelope, dict):
            raise ShadowRpcResponseError("RPC response envelope must be an object")
        if envelope.get("jsonrpc") != "2.0":
            raise ShadowRpcResponseError("RPC response JSON-RPC version mismatch")
        response_id = envelope.get("id")
        if type(response_id) is not int or response_id != request_id:
            raise ShadowRpcResponseError("RPC response ID mismatch")
        if "error" in envelope:
            raise ShadowRpcResponseError("RPC response contained an error")
        if "result" not in envelope:
            raise ShadowRpcResponseError("RPC response is missing result")
        return envelope["result"]

    def get_multiple_accounts(
        self,
        pubkeys: tuple[str, ...],
        *,
        min_context_slot: int | None = None,
    ) -> RpcAccountBatchV01:
        if not isinstance(pubkeys, tuple) or not 1 <= len(pubkeys) <= 100:
            raise ShadowRpcProtocolError("pubkeys must be a tuple containing 1 to 100 keys")
        for pubkey in pubkeys:
            try:
                Pubkey.from_string(_request_text("pubkey", pubkey))
            except ShadowRpcProtocolError:
                raise
            except Exception:
                raise ShadowRpcProtocolError("pubkey is invalid") from None
        config: dict[str, Any] = {"encoding": "base64", "commitment": "confirmed"}
        if min_context_slot is not None:
            config["minContextSlot"] = _request_u64(
                "minimum context slot", min_context_slot
            )
        result = self._post(
            _RpcMethod.GET_MULTIPLE_ACCOUNTS,
            [list(pubkeys), config],
        )
        context_slot, values = _context_result(result, label="account batch")
        if not isinstance(values, list) or len(values) != len(pubkeys):
            raise ShadowRpcResponseError("RPC account count does not match request")
        accounts: list[RpcAccountV01 | None] = []
        for pubkey, value in zip(pubkeys, values, strict=True):
            if value is None:
                accounts.append(None)
                continue
            if not isinstance(value, dict):
                raise ShadowRpcResponseError("RPC account must be an object or null")
            owner = value.get("owner")
            data_field = value.get("data")
            if not isinstance(owner, str) or not owner:
                raise ShadowRpcResponseError("RPC account owner is invalid")
            if (
                not isinstance(data_field, list)
                or len(data_field) != 2
                or not isinstance(data_field[0], str)
                or data_field[1] != "base64"
            ):
                raise ShadowRpcResponseError("RPC account data must use base64 encoding")
            try:
                account_data = base64.b64decode(data_field[0], validate=True)
                accounts.append(RpcAccountV01(pubkey, owner, account_data))
            except (binascii.Error, ValueError, TypeError):
                raise ShadowRpcResponseError("RPC account data or identity is invalid") from None
            except Exception:
                raise ShadowRpcResponseError("RPC account identity is invalid") from None
        try:
            return RpcAccountBatchV01(context_slot, tuple(accounts))
        except Exception:
            raise ShadowRpcResponseError("RPC account batch is invalid") from None

    def get_latest_blockhash(
        self,
        *,
        commitment: str,
        min_context_slot: int,
    ) -> LatestBlockhashResponseV01:
        config = {
            "commitment": _request_text("commitment", commitment),
            "minContextSlot": _request_u64("minimum context slot", min_context_slot),
        }
        result = self._post(_RpcMethod.GET_LATEST_BLOCKHASH, [config])
        context_slot, value = _context_result(result, label="latest blockhash")
        if not isinstance(value, dict):
            raise ShadowRpcResponseError("latest blockhash value must be an object")
        blockhash = _required_text("blockhash", value.get("blockhash"))
        height = _strict_u64("last valid block height", value.get("lastValidBlockHeight"))
        try:
            return LatestBlockhashResponseV01(context_slot, blockhash, height)
        except Exception:
            raise ShadowRpcResponseError("latest blockhash response is invalid") from None

    def get_block_height(
        self,
        *,
        commitment: str,
        min_context_slot: int,
    ) -> BlockHeightResponseV01:
        config = {
            "commitment": _request_text("commitment", commitment),
            "minContextSlot": _request_u64("minimum context slot", min_context_slot),
        }
        height = _strict_u64(
            "block height",
            self._post(_RpcMethod.GET_BLOCK_HEIGHT, [config]),
        )
        try:
            return BlockHeightResponseV01(height)
        except Exception:
            raise ShadowRpcResponseError("block-height response is invalid") from None

    def is_blockhash_valid(
        self,
        blockhash: str,
        *,
        commitment: str,
        min_context_slot: int,
    ) -> BlockhashValidityResponseV01:
        blockhash = _request_text("blockhash", blockhash)
        try:
            Hash.from_string(blockhash)
        except Exception:
            raise ShadowRpcProtocolError("blockhash is invalid") from None
        config = {
            "commitment": _request_text("commitment", commitment),
            "minContextSlot": _request_u64("minimum context slot", min_context_slot),
        }
        result = self._post(_RpcMethod.IS_BLOCKHASH_VALID, [blockhash, config])
        context_slot, value = _context_result(result, label="blockhash validity")
        if type(value) is not bool:
            raise ShadowRpcResponseError("blockhash validity value must be boolean")
        try:
            return BlockhashValidityResponseV01(context_slot, value)
        except Exception:
            raise ShadowRpcResponseError("blockhash-validity response is invalid") from None

    def simulate_transaction(
        self,
        transaction_base64: str,
        *,
        config: Mapping[str, Any],
    ) -> SimulationRpcResponseV01:
        transaction_base64 = _request_text("transaction_base64", transaction_base64)
        try:
            base64.b64decode(transaction_base64, validate=True)
        except (binascii.Error, ValueError):
            raise ShadowRpcProtocolError("transaction_base64 is invalid") from None
        if not isinstance(config, Mapping):
            raise ShadowRpcProtocolError("simulation config must be a mapping")
        try:
            supplied = dict(config)
        except Exception:
            raise ShadowRpcProtocolError("simulation config is invalid") from None
        required_keys = {
            "encoding",
            "sigVerify",
            "replaceRecentBlockhash",
            "commitment",
            "minContextSlot",
            "innerInstructions",
        }
        if set(supplied) != required_keys:
            raise ShadowRpcProtocolError("simulation config shape is invalid")
        if (
            supplied["encoding"] != "base64"
            or supplied["sigVerify"] is not False
            or supplied["replaceRecentBlockhash"] is not False
            or supplied["commitment"] != "confirmed"
            or supplied["innerInstructions"] is not True
        ):
            raise ShadowRpcProtocolError("simulation config contract mismatch")
        _request_u64("simulation minimum context slot", supplied["minContextSlot"])
        result = self._post(
            _RpcMethod.SIMULATE_TRANSACTION,
            [transaction_base64, supplied],
        )
        context_slot, value = _context_result(result, label="simulation")
        if not isinstance(value, dict):
            raise ShadowRpcResponseError("simulation value must be an object")
        try:
            return SimulationRpcResponseV01(context_slot, dict(value))
        except Exception:
            raise ShadowRpcResponseError("simulation response is invalid") from None


__all__ = [
    "CONTRACT_SPEC",
    "MODEL_FINGERPRINT",
    "MODEL_ID",
    "SCHEMA_VERSION",
    "ShadowRpcError",
    "ShadowRpcProtocolError",
    "ShadowRpcResponseError",
    "ShadowRpcTransportError",
    "StrictReadOnlySolanaRpcV01",
]
