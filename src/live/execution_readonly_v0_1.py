"""Bounded Execution reads only. No signing, send, retries or arbitrary RPC.

Account parsing and budgets reuse the LIVE Evidence adapter. The additional
closed methods return accepted Phase-5 simulation facts. Original public read
records exclude private endpoint/headers and are immutable canonical JSON.
"""
from __future__ import annotations
import base64
import json
import time
from enum import StrEnum
import httpx
from phase5 import shadow_unsigned_plan_simulation_v0_1 as plans
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from .public_rpc_v0_1 import PublicReadOnlyRpc, PublicRpcError, _ReadMethod, _json, u64, block_hash
from .authority_controls_v0_1 import require
from .authority_message_evidence_v0_1 import OriginalAccountBatch, PublicReadCut, FeeForMessageRead
from .ledger_actions_v0_1 import decode_message

class _ExecutionMethod(StrEnum):
    LEASE = "getLatestBlockhash"
    HEIGHT = "getBlockHeight"
    VALIDITY = "isBlockhashValid"
    SIMULATION = "simulateTransaction"
    FEE = "getFeeForMessage"

class ExecutionReadOnlyRpc(PublicReadOnlyRpc):
    def __init__(self, endpoint, profile, *, now_us, transport=None):
        super().__init__(endpoint, profile, transport=transport)
        self._now_us = now_us
        self._records = []
        self._genesis = None

    @property
    def records(self):
        return tuple(self._records)

    @property
    def last_observed_at_us(self):
        require(bool(self._records), "EXECUTION_ORIGINAL_READ_REQUIRED")
        return json.loads(self._records[-1])["observed_at_us"]

    @property
    def record_digest(self):
        return content_fingerprint({"version": "live_execution_reads_v0.1", "reads": self.records})

    def bind_genesis(self, expected):
        value = self.get_genesis_hash()
        require(value == block_hash(expected), "EXECUTION_READ_GENESIS_CONFLICT")
        self._genesis = value

    def _cut(self, slot, commitment):
        require(self._genesis is not None, "EXECUTION_READ_GENESIS_REQUIRED")
        return PublicReadCut(self._genesis, self.profile.fingerprint, slot,
            json.loads(self._records[-1])["observed_at_us"], commitment)

    def account_batch(self, keys, *, min_context_slot):
        read = self.get_multiple_accounts(keys, min_context_slot=min_context_slot)
        return OriginalAccountBatch(self._cut(read.context.slot, "finalized"), read.requested_keys, read.accounts)

    @staticmethod
    def _config(commitment, min_context_slot):
        require(commitment == "confirmed", "EXECUTION_READ_COMMITMENT_UNSUPPORTED")
        return {"commitment": commitment, "minContextSlot": u64(min_context_slot)}

    def get_latest_blockhash(self, *, commitment, min_context_slot):
        config = self._config(commitment, min_context_slot)
        cut, value = self._context(self._post(_ExecutionMethod.LEASE, [config]), min_context_slot)
        require(type(value) is dict and set(value) == {"blockhash", "lastValidBlockHeight"}, "EXECUTION_LEASE_SHAPE_INVALID")
        return plans.LatestBlockhashResponseV01(cut.slot, value["blockhash"], value["lastValidBlockHeight"])

    def get_block_height(self, *, commitment, min_context_slot):
        return plans.BlockHeightResponseV01(self._post(_ExecutionMethod.HEIGHT, [self._config(commitment, min_context_slot)]))

    def is_blockhash_valid(self, blockhash, *, commitment, min_context_slot):
        config = self._config(commitment, min_context_slot)
        cut, value = self._context(self._post(_ExecutionMethod.VALIDITY, [block_hash(blockhash), config]), min_context_slot)
        return plans.BlockhashValidityResponseV01(cut.slot, value)

    def simulate_transaction(self, transaction_base64, *, config):
        require(type(config) is dict, "EXECUTION_EXACT_SIMULATION_CONFIG_REQUIRED")
        expected = plans.SimulationRequestConfigV01(config.get("minContextSlot")).rpc_payload()
        require(type(config) is dict and canonical_json(config) == canonical_json(expected), "EXECUTION_EXACT_SIMULATION_CONFIG_REQUIRED")
        wire = base64.b64decode(transaction_base64, validate=True)
        require(base64.b64encode(wire).decode() == transaction_base64 and len(wire) <= 1232
            and wire[:65] == b"\x01"+bytes(64), "EXECUTION_ZERO_SIGNATURE_WIRE_REQUIRED")
        message = decode_message(wire[65:].hex())
        require(message.header.num_required_signatures == 1, "EXECUTION_SINGLE_WALLET_REQUIRED")
        cut, value = self._context(self._post(_ExecutionMethod.SIMULATION, [transaction_base64, dict(config)]), expected["minContextSlot"])
        return plans.SimulationRpcResponseV01(cut.slot, value)

    def fee_for_message(self, message_hex, *, min_context_slot):
        decode_message(message_hex)
        message = base64.b64encode(bytes.fromhex(message_hex)).decode()
        config = self._config("confirmed", min_context_slot)
        cut, value = self._context(self._post(_ExecutionMethod.FEE, [message, config]), min_context_slot)
        return FeeForMessageRead(self._cut(cut.slot, "confirmed"), message, min_context_slot,
            "NULL" if value is None else "OBSERVED", None if value is None else u64(value))

    def _post(self, method: _ReadMethod, params: list) -> object:
        if not (type(method) is _ExecutionMethod or type(method) is _ReadMethod
                and method in (_ReadMethod.GENESIS, _ReadMethod.ACCOUNTS)):
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
        result = envelope["result"]
        # Preserve public protocol facts, never endpoint, headers or error bodies.
        self._records.append(canonical_json({"method": method.value, "params": params,
            "result": result, "observed_at_us": u64(self._now_us())}))
        return result
