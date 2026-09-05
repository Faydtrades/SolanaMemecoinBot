from __future__ import annotations

import ast
import base64
import inspect
import json
import sys
from pathlib import Path
from typing import Any, Callable

import httpx


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase5.shadow_readonly_rpc_v0_1 import (  # noqa: E402
    CONTRACT_SPEC,
    MODEL_FINGERPRINT,
    MODEL_ID,
    ShadowRpcError,
    ShadowRpcProtocolError,
    ShadowRpcResponseError,
    ShadowRpcTransportError,
    StrictReadOnlySolanaRpcV01,
)
from phase5.shadow_unsigned_plan_simulation_v0_1 import (  # noqa: E402
    BlockhashValidityResponseV01,
    BlockHeightResponseV01,
    LatestBlockhashResponseV01,
    SimulationRpcResponseV01,
)
from phase5.shadow_venue_route_quote_v0_1 import (  # noqa: E402
    RpcAccountBatchV01,
    RpcAccountV01,
)


RPC_URL = "https://credential.example.invalid/rpc?token=never-log-this"
PUBKEY_A = "11111111111111111111111111111111"
PUBKEY_B = "SysvarRent111111111111111111111111111111111"
BLOCKHASH = "11111111111111111111111111111111"
ACCOUNT_BYTES = b"strict-read-only-account"
ACCOUNT_B64 = base64.b64encode(ACCOUNT_BYTES).decode("ascii")
TRANSACTION_B64 = base64.b64encode(b"unsigned-transaction-envelope").decode("ascii")
SIMULATION_CONFIG = {
    "encoding": "base64",
    "sigVerify": False,
    "replaceRecentBlockhash": False,
    "commitment": "confirmed",
    "minContextSlot": 120,
    "innerInstructions": True,
}


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def expect(error: type[BaseException], callback: Callable[[], Any]) -> bool:
    try:
        callback()
    except error:
        return True
    return False


def account(owner: str = PUBKEY_A, data: Any = None) -> dict[str, Any]:
    return {
        "lamports": 1,
        "owner": owner,
        "data": [ACCOUNT_B64, "base64"] if data is None else data,
        "executable": False,
        "rentEpoch": 0,
        "space": len(ACCOUNT_BYTES),
    }


def context_result(value: Any, slot: Any = 123) -> dict[str, Any]:
    return {"context": {"slot": slot}, "value": value}


class Harness:
    def __init__(
        self,
        response_factory: Callable[[dict[str, Any]], httpx.Response],
    ) -> None:
        self.requests: list[dict[str, Any]] = []
        self.headers: list[httpx.Headers] = []

        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            self.requests.append(payload)
            self.headers.append(request.headers)
            return response_factory(payload)

        self.rpc = StrictReadOnlySolanaRpcV01(
            RPC_URL,
            transport=httpx.MockTransport(handler),
        )

    def close(self) -> None:
        self.rpc.__exit__()


def ok(result: Any) -> Callable[[dict[str, Any]], httpx.Response]:
    def factory(request: dict[str, Any]) -> httpx.Response:
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": request["id"], "result": result},
        )

    return factory


def call(
    factory: Callable[[dict[str, Any]], httpx.Response],
    callback: Callable[[StrictReadOnlySolanaRpcV01], Any],
) -> tuple[Any, list[dict[str, Any]], list[httpx.Headers]]:
    harness = Harness(factory)
    try:
        result = callback(harness.rpc)
        return result, harness.requests, harness.headers
    finally:
        harness.close()


def rejected(
    factory: Callable[[dict[str, Any]], httpx.Response],
    callback: Callable[[StrictReadOnlySolanaRpcV01], Any],
    error: type[BaseException] = ShadowRpcError,
) -> bool:
    harness = Harness(factory)
    try:
        return expect(error, lambda: callback(harness.rpc))
    finally:
        harness.close()


def main() -> int:
    checks: dict[str, bool] = {}

    # A. Strict account reads and exact request mapping.
    account_result = context_result([account(), None])
    batch, requests, headers = call(
        ok(account_result),
        lambda rpc: rpc.get_multiple_accounts(
            (PUBKEY_A, PUBKEY_B), min_context_slot=100
        ),
    )
    check("A01_VALID_ACCOUNT_BATCH_TYPE", isinstance(batch, RpcAccountBatchV01), checks)
    check("A02_VALID_ACCOUNT_BYTES", isinstance(batch.accounts[0], RpcAccountV01) and batch.accounts[0].data == ACCOUNT_BYTES, checks)
    check("A03_NULL_ACCOUNT_PRESERVED", batch.accounts[1] is None, checks)
    check("A04_PUBKEY_ORDER_PRESERVED", batch.accounts[0].pubkey == PUBKEY_A and len(batch.accounts) == 2, checks)
    check("A05_MIN_CONTEXT_SLOT_EMITTED", requests[0]["params"] == [[PUBKEY_A, PUBKEY_B], {"encoding": "base64", "commitment": "confirmed", "minContextSlot": 100}], checks)
    check("A06_JSON_RPC_METHOD_HARDCODED", requests[0]["method"] == "getMultipleAccounts", checks)
    check("A07_CONTENT_TYPE_JSON", headers[0].get("content-type") == "application/json", checks)
    _, no_min_requests, _ = call(
        ok(context_result([None])),
        lambda rpc: rpc.get_multiple_accounts((PUBKEY_A,)),
    )
    check("A08_OPTIONAL_MIN_CONTEXT_SLOT_OMITTED", "minContextSlot" not in no_min_requests[0]["params"][1], checks)
    check("A09_ACCOUNT_COUNT_MISMATCH_REJECTED", rejected(ok(context_result([])), lambda rpc: rpc.get_multiple_accounts((PUBKEY_A,))), checks)
    check("A10_MALFORMED_BASE64_REJECTED", rejected(ok(context_result([account(data=["%%%", "base64"])])), lambda rpc: rpc.get_multiple_accounts((PUBKEY_A,))), checks)
    check("A11_WRONG_ACCOUNT_ENCODING_REJECTED", rejected(ok(context_result([account(data=[ACCOUNT_B64, "base58"])])), lambda rpc: rpc.get_multiple_accounts((PUBKEY_A,))), checks)
    check("A12_MISSING_ACCOUNT_OWNER_REJECTED", rejected(ok(context_result([{"data": [ACCOUNT_B64, "base64"]}])), lambda rpc: rpc.get_multiple_accounts((PUBKEY_A,))), checks)
    check("A13_INVALID_ACCOUNT_OWNER_REJECTED", rejected(ok(context_result([account(owner="invalid")])), lambda rpc: rpc.get_multiple_accounts((PUBKEY_A,))), checks)
    check("A14_BOOL_CONTEXT_SLOT_REJECTED", rejected(ok(context_result([None], slot=True)), lambda rpc: rpc.get_multiple_accounts((PUBKEY_A,))), checks)
    check("A15_INVALID_REQUEST_PUBKEY_REJECTED_BEFORE_HTTP", expect(ShadowRpcProtocolError, lambda: call(ok(context_result([None])), lambda rpc: rpc.get_multiple_accounts(("invalid",)))), checks)
    api_context, _, _ = call(
        ok({"context": {"slot": 123, "apiVersion": "2.0.0"}, "value": [None]}),
        lambda rpc: rpc.get_multiple_accounts((PUBKEY_A,)),
    )
    check("A16_STANDARD_API_VERSION_CONTEXT_ACCEPTED", api_context.context_slot == 123, checks)
    check("A17_BOOL_REQUEST_MIN_CONTEXT_REJECTED", expect(ShadowRpcProtocolError, lambda: call(ok(context_result([None])), lambda rpc: rpc.get_multiple_accounts((PUBKEY_A,), min_context_slot=True))), checks)

    # B. Blockhash lease evidence.
    latest_value = context_result(
        {"blockhash": BLOCKHASH, "lastValidBlockHeight": 999},
        slot=500,
    )
    latest, latest_requests, _ = call(
        ok(latest_value),
        lambda rpc: rpc.get_latest_blockhash(
            commitment="confirmed", min_context_slot=450
        ),
    )
    check("B01_LATEST_BLOCKHASH_TYPE", isinstance(latest, LatestBlockhashResponseV01), checks)
    check("B02_LATEST_BLOCKHASH_VALUES", (latest.context_slot, latest.blockhash, latest.last_valid_block_height) == (500, BLOCKHASH, 999), checks)
    check("B03_LATEST_BLOCKHASH_REQUEST_EXACT", latest_requests[0]["method"] == "getLatestBlockhash" and latest_requests[0]["params"] == [{"commitment": "confirmed", "minContextSlot": 450}], checks)
    check("B04_MALFORMED_BLOCKHASH_REJECTED", rejected(ok(context_result({"blockhash": "invalid", "lastValidBlockHeight": 999})), lambda rpc: rpc.get_latest_blockhash(commitment="confirmed", min_context_slot=1)), checks)
    check("B05_BOOL_LAST_VALID_HEIGHT_REJECTED", rejected(ok(context_result({"blockhash": BLOCKHASH, "lastValidBlockHeight": True})), lambda rpc: rpc.get_latest_blockhash(commitment="confirmed", min_context_slot=1)), checks)
    check("B06_MALFORMED_BLOCKHASH_VALUE_REJECTED", rejected(ok(context_result([])), lambda rpc: rpc.get_latest_blockhash(commitment="confirmed", min_context_slot=1)), checks)

    # C. Block height.
    height, height_requests, _ = call(
        ok(777),
        lambda rpc: rpc.get_block_height(
            commitment="confirmed", min_context_slot=700
        ),
    )
    check("C01_BLOCK_HEIGHT_TYPE_AND_VALUE", isinstance(height, BlockHeightResponseV01) and height.block_height == 777, checks)
    check("C02_BLOCK_HEIGHT_REQUEST_EXACT", height_requests[0]["method"] == "getBlockHeight" and height_requests[0]["params"] == [{"commitment": "confirmed", "minContextSlot": 700}], checks)
    check("C03_BOOL_AS_INT_HEIGHT_REJECTED", rejected(ok(True), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1)), checks)
    check("C04_NEGATIVE_HEIGHT_REJECTED", rejected(ok(-1), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1)), checks)

    # D. Blockhash validity.
    valid, valid_requests, _ = call(
        ok(context_result(True, slot=800)),
        lambda rpc: rpc.is_blockhash_valid(
            BLOCKHASH, commitment="confirmed", min_context_slot=750
        ),
    )
    invalid, _, _ = call(
        ok(context_result(False, slot=801)),
        lambda rpc: rpc.is_blockhash_valid(
            BLOCKHASH, commitment="confirmed", min_context_slot=750
        ),
    )
    check("D01_BLOCKHASH_VALID_TRUE", isinstance(valid, BlockhashValidityResponseV01) and valid.valid is True, checks)
    check("D02_BLOCKHASH_VALID_FALSE", invalid.valid is False, checks)
    check("D03_BLOCKHASH_VALIDITY_REQUEST_EXACT", valid_requests[0]["method"] == "isBlockhashValid" and valid_requests[0]["params"] == [BLOCKHASH, {"commitment": "confirmed", "minContextSlot": 750}], checks)
    check("D04_MALFORMED_VALIDITY_REJECTED", rejected(ok(context_result(1)), lambda rpc: rpc.is_blockhash_valid(BLOCKHASH, commitment="confirmed", min_context_slot=1)), checks)
    check("D05_INVALID_REQUEST_BLOCKHASH_REJECTED", expect(ShadowRpcProtocolError, lambda: call(ok(context_result(True)), lambda rpc: rpc.is_blockhash_valid("invalid", commitment="confirmed", min_context_slot=1))), checks)

    # E. Simulation preserves the accepted downstream classification payload.
    simulation_value = {
        "err": None,
        "logs": ["Program log: accepted"],
        "unitsConsumed": 321,
        "returnData": None,
        "innerInstructions": [],
        "accounts": None,
    }
    simulation, simulation_requests, _ = call(
        ok(context_result(simulation_value, slot=900)),
        lambda rpc: rpc.simulate_transaction(
            TRANSACTION_B64, config=SIMULATION_CONFIG
        ),
    )
    check("E01_SIMULATION_RESPONSE_TYPE", isinstance(simulation, SimulationRpcResponseV01), checks)
    check("E02_SIMULATION_VALUE_PRESERVED", dict(simulation.value) == simulation_value and simulation.context_slot == 900, checks)
    check("E03_SIMULATION_REQUEST_EXACT", simulation_requests[0]["method"] == "simulateTransaction" and simulation_requests[0]["params"] == [TRANSACTION_B64, SIMULATION_CONFIG], checks)
    rejected_value = {"err": {"InstructionError": [0, "Custom"]}, "logs": []}
    downstream, _, _ = call(
        ok(context_result(rejected_value, slot=901)),
        lambda rpc: rpc.simulate_transaction(
            TRANSACTION_B64, config=SIMULATION_CONFIG
        ),
    )
    check("E04_DOWNSTREAM_FAILURE_VALUE_REPRESENTABLE", dict(downstream.value) == rejected_value, checks)
    check("E05_MALFORMED_SIMULATION_VALUE_REJECTED", rejected(ok(context_result(None)), lambda rpc: rpc.simulate_transaction(TRANSACTION_B64, config=SIMULATION_CONFIG)), checks)
    relaxed = dict(SIMULATION_CONFIG, sigVerify=True)
    check("E06_RELAXED_SIMULATION_CONFIG_REJECTED", expect(ShadowRpcProtocolError, lambda: call(ok(context_result(simulation_value)), lambda rpc: rpc.simulate_transaction(TRANSACTION_B64, config=relaxed))), checks)
    check("E07_INVALID_TRANSACTION_BASE64_REJECTED", expect(ShadowRpcProtocolError, lambda: call(ok(context_result(simulation_value)), lambda rpc: rpc.simulate_transaction("%%%", config=SIMULATION_CONFIG))), checks)

    # F. Transport and JSON-RPC envelope failures.
    check("F01_JSON_RPC_ERROR_REJECTED", rejected(lambda request: httpx.Response(200, json={"jsonrpc": "2.0", "id": request["id"], "error": {"code": -1}}), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1), ShadowRpcResponseError), checks)
    check("F02_MISMATCHED_ID_REJECTED", rejected(lambda _request: httpx.Response(200, json={"jsonrpc": "2.0", "id": 99, "result": 1}), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1), ShadowRpcResponseError), checks)
    check("F03_MISSING_RESULT_REJECTED", rejected(lambda request: httpx.Response(200, json={"jsonrpc": "2.0", "id": request["id"]}), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1), ShadowRpcResponseError), checks)
    check("F04_HTTP_ERROR_REJECTED", rejected(lambda _request: httpx.Response(503), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1), ShadowRpcTransportError), checks)
    check("F05_TIMEOUT_REJECTED", rejected(lambda _request: (_ for _ in ()).throw(httpx.ReadTimeout("timeout")), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1), ShadowRpcTransportError), checks)
    check("F06_NETWORK_FAILURE_REJECTED", rejected(lambda _request: (_ for _ in ()).throw(httpx.ConnectError("network")), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1), ShadowRpcTransportError), checks)
    check("F07_MALFORMED_JSON_REJECTED", rejected(lambda _request: httpx.Response(200, content=b"not-json"), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1), ShadowRpcResponseError), checks)
    check("F08_WRONG_JSON_RPC_VERSION_REJECTED", rejected(lambda request: httpx.Response(200, json={"jsonrpc": "1.0", "id": request["id"], "result": 1}), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1), ShadowRpcResponseError), checks)
    check("F09_BOOL_RESPONSE_ID_REJECTED", rejected(lambda _request: httpx.Response(200, json={"jsonrpc": "2.0", "id": True, "result": 1}), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1), ShadowRpcResponseError), checks)
    sequence = Harness(ok(10))
    try:
        sequence.rpc.get_block_height(commitment="confirmed", min_context_slot=1)
        sequence.rpc.get_block_height(commitment="confirmed", min_context_slot=1)
        check("F10_DETERMINISTIC_REQUEST_SEQUENCE", [item["id"] for item in sequence.requests] == [1, 2], checks)
    finally:
        sequence.close()
    check("F11_DUPLICATE_JSON_KEY_REJECTED", rejected(lambda _request: httpx.Response(200, content=b'{"jsonrpc":"2.0","id":1,"result":1,"result":2}'), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1), ShadowRpcResponseError), checks)
    check("F12_NONSTANDARD_JSON_CONSTANT_REJECTED", rejected(lambda _request: httpx.Response(200, content=b'{"jsonrpc":"2.0","id":1,"result":NaN}'), lambda rpc: rpc.get_block_height(commitment="confirmed", min_context_slot=1), ShadowRpcResponseError), checks)

    # G. Closed capability surface and immutable accepted modules.
    public_methods = {
        name
        for name, member in inspect.getmembers(
            StrictReadOnlySolanaRpcV01, predicate=callable
        )
        if not name.startswith("_")
    }
    expected_methods = {
        "get_multiple_accounts",
        "get_latest_blockhash",
        "get_block_height",
        "is_blockhash_valid",
        "simulate_transaction",
    }
    check("G01_PUBLIC_RPC_SURFACE_EXACT", public_methods == expected_methods, checks)
    check("G02_CONTRACT_METHODS_EXACT", CONTRACT_SPEC["methods"] == ["getMultipleAccounts", "getLatestBlockhash", "getBlockHeight", "isBlockhashValid", "simulateTransaction"], checks)
    source_path = PROJECT_ROOT / "src" / "phase5" / "shadow_readonly_rpc_v0_1.py"
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        str(node.module).split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    check("G03_HTTPX_ONLY_NETWORK_DEPENDENCY", "httpx" in imports and not ({"requests", "websockets", "solana"} & imports), checks)
    check("G04_NO_GENERIC_CALLER_METHOD_PARAMETER", all("method" not in inspect.signature(getattr(StrictReadOnlySolanaRpcV01, name)).parameters for name in expected_methods), checks)
    check("G05_NO_SECRET_URL_IN_TYPED_FAILURE", RPC_URL not in str(expect_failure_text()), checks)
    check("G06_MODEL_AND_FINGERPRINT", MODEL_ID == "P5-STRICT-READONLY-SOLANA-JSON-RPC-0001" and len(MODEL_FINGERPRINT) == 64, checks)
    timeout_harness = Harness(ok(1))
    try:
        timeout = timeout_harness.rpc._client.timeout
        check("G07_BOUNDED_TIMEOUTS_EXACT", timeout.connect == 10.0 and timeout.read == 20.0 and timeout.write == 20.0 and timeout.pool == 20.0, checks)
    finally:
        timeout_harness.close()
    check("G08_HTTPX_VERSION_EXACT", httpx.__version__ == "0.28.1", checks)

    passed = sum(checks.values())
    print("=" * 112)
    print("PHASE-5 STRICT READ-ONLY SOLANA JSON-RPC ADAPTER v0.1 SELF-TEST")
    print("=" * 112)
    for name, passed_check in checks.items():
        print(f"{name}: {'PASS' if passed_check else 'FAIL'}")
    print(f"MODEL_ID: {MODEL_ID}")
    print(f"MODEL_FINGERPRINT: {MODEL_FINGERPRINT}")
    print(f"CHECKS: {passed}/{len(checks)}")
    print(f"RESULT: {'PASS' if passed == len(checks) else 'FAIL'}")
    return 0 if passed == len(checks) else 1


def expect_failure_text() -> str:
    harness = Harness(
        lambda _request: (_ for _ in ()).throw(httpx.ConnectError(RPC_URL))
    )
    try:
        try:
            harness.rpc.get_block_height(commitment="confirmed", min_context_slot=1)
        except ShadowRpcError as exc:
            return str(exc)
        return "NO_ERROR"
    finally:
        harness.close()


if __name__ == "__main__":
    raise SystemExit(main())
