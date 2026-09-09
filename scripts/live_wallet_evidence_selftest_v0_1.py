"""Deterministic public-account RPC fixtures; no real endpoint or wallet access."""
from __future__ import annotations

import base64
import json
import sys
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from pathlib import Path

import httpx
from solders.hash import Hash
from solders.pubkey import Pubkey

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from phase5.shadow_venue_route_quote_v0_1 import SYSTEM_PROGRAM_ID, TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID, WSOL_MINT
from live.public_rpc_v0_1 import PublicReadOnlyRpc, PublicRpcError, PublicRpcProfile, READ_ONLY_METHODS, TOKEN_PROGRAMS
from live.wallet_evidence_v0_1 import (
    ExpectedTokenAccount, LedgerAccountEvidence, WalletEvidenceAdapter, WalletEvidenceRequest,
    assess_wallet_accounts, ledger_account_evidence,
)


def key(n: int) -> str:
    return str(Pubkey.from_bytes(bytes([n]) * 32))


WALLET, MINT, TOKEN, MINT22, TOKEN22, OTHER = (key(i) for i in range(1, 7))
GENESIS = str(Hash.from_bytes(bytes([20]) * 32))
BLOCK = str(Hash.from_bytes(bytes([21]) * 32))
PARENT = str(Hash.from_bytes(bytes([22]) * 32))
NOW = 1788912120
ENDPOINT = "https://fixture.invalid/private/DO_NOT_RETAIN?api-key=DO_NOT_RETAIN"
PROFILE = PublicRpcProfile("SYNTHETIC-PROVIDER", "1")
CHECKS = {}


def utc(seconds=NOW):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def raises(error, call):
    try:
        call()
    except error:
        return True
    return False


def option(raw, offset, value, *, integer=False):
    if value is not None:
        raw[offset:offset+4] = (1).to_bytes(4, "little")
        data = value.to_bytes(8, "little") if integer else bytes(Pubkey.from_string(value))
        raw[offset+4:offset+4+len(data)] = data


def token_data(mint=MINT, owner=WALLET, amount=700, *, frozen=False, delegate=None, delegated=0,
               reserve=None, close=None, tail=b""):
    raw = bytearray(165)
    raw[:32], raw[32:64] = bytes(Pubkey.from_string(mint)), bytes(Pubkey.from_string(owner))
    raw[64:72] = amount.to_bytes(8, "little")
    option(raw, 72, delegate)
    raw[108] = 2 if frozen else 1
    option(raw, 109, reserve, integer=True)
    raw[121:129] = delegated.to_bytes(8, "little")
    option(raw, 129, close)
    return bytes(raw) + tail


def mint_data(decimals=6, tail=b""):
    raw = bytearray(82)
    option(raw, 0, OTHER)
    raw[36:44] = (1000000).to_bytes(8, "little")
    raw[44], raw[45] = decimals, 1
    option(raw, 46, OTHER)
    return bytes(raw) + tail


def account(owner, data=b"", lamports=2039280, **extra):
    return {"owner": owner, "data": [base64.b64encode(data).decode(), "base64"],
            "lamports": lamports, "executable": False, "rentEpoch": (1 << 64)-1,
            "space": len(data), "ignoredDiagnostic": "DO_NOT_RETAIN", **extra}


class Scenario:
    def __init__(self):
        self.accounts = {WALLET: account(SYSTEM_PROGRAM_ID, lamports=5000000000)}
        self.inventory = {program: [] for program in TOKEN_PROGRAMS}
        self.context = 101
        self.program_context = {}
        self.multiple_context = 101
        self.initial_slot, self.upper_slot = 100, 102
        self.block_time = NOW - 20
        self.genesis = GENESIS
        self.end_genesis = None
        self.genesis_calls = 0
        self.calls = []
        self.slot_calls = 0
        self.transform = None
        self.program_failure = None
        self.explicit_override = {}
        self.omit_api_version = False

    def add_token(self, *, token=TOKEN, mint=MINT, program=TOKEN_PROGRAM_ID, data=None, mint_bytes=None, lamports=2039280):
        self.accounts[token] = account(program, token_data(mint) if data is None else data, lamports)
        self.accounts[mint] = account(program, mint_data() if mint_bytes is None else mint_bytes)
        self.inventory[program].append(token)
        return self

    def context_result(self, slot, value):
        context = {"slot": slot}
        if not self.omit_api_version:
            context["apiVersion"] = "3.1.0"
        return {"context": context, "value": value}

    def handle(self, request):
        payload = json.loads(request.content)
        self.calls.append((payload, request.extensions.get("timeout")))
        method, params = payload["method"], payload["params"]
        if method == "getGenesisHash":
            result = self.genesis if self.genesis_calls == 0 or self.end_genesis is None else self.end_genesis
            self.genesis_calls += 1
        elif method == "getSlot":
            result = self.initial_slot if self.slot_calls == 0 else self.upper_slot
            self.slot_calls += 1
        elif method == "getTokenAccountsByOwner":
            program = params[1]["programId"]
            if program == self.program_failure:
                return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "error": {"code": -1, "message": "DO_NOT_RETAIN"}})
            result = self.context_result(self.program_context.get(program, self.context),
                [{"pubkey": pubkey, "account": self.accounts[pubkey]} for pubkey in self.inventory[program]])
        elif method == "getMultipleAccounts":
            values = [self.explicit_override.get(pubkey, self.accounts.get(pubkey)) for pubkey in params[0]]
            result = self.context_result(self.multiple_context, values)
        elif method == "getBlock":
            result = {"blockhash": BLOCK, "previousBlockhash": PARENT, "parentSlot": params[0]-1,
                      "blockHeight": 90, "blockTime": self.block_time, "ignoredDiagnostic": "DO_NOT_RETAIN"}
        else:
            raise AssertionError("unallowed RPC reached fixture")
        envelope = {"jsonrpc": "2.0", "id": payload["id"], "result": result}
        if self.transform:
            modified = self.transform(payload, envelope)
            if isinstance(modified, httpx.Response):
                return modified
            envelope = modified
        return httpx.Response(200, json=envelope)


def run(scenario=None, *, request=None, profile=PROFILE):
    scenario = scenario or Scenario()
    request = request or WalletEvidenceRequest(WALLET, GENESIS, 100)
    with PublicReadOnlyRpc(ENDPOINT, profile, transport=httpx.MockTransport(scenario.handle)) as rpc:
        adapter = WalletEvidenceAdapter(rpc, request, clock=lambda: utc())
        observed = adapter.observe()
        check("one_observation_has_no_silent_budget_reset", raises(PublicRpcError, adapter.observe))
    port = ledger_account_evidence(observed, expected_wallet=WALLET, expected_genesis=GENESIS,
                                   expected_profile_fingerprint=profile.fingerprint,
                                   required_min_context_slot=request.min_context_slot, now_utc=utc())
    return observed, port


def protocol_error(name, transform, *, method="genesis", profile=PROFILE):
    scenario = Scenario()
    scenario.transform = transform
    try:
        with PublicReadOnlyRpc(ENDPOINT, profile, transport=httpx.MockTransport(scenario.handle)) as rpc:
            if method == "genesis":
                rpc.get_genesis_hash()
            else:
                rpc.get_multiple_accounts((WALLET,), min_context_slot=100)
    except PublicRpcError as exc:
        check(name, "DO_NOT_RETAIN" not in str(exc))
        return
    raise AssertionError(name)


def main():
    scenario = Scenario()
    empty, positive = run(scenario)
    check("funded_empty_token_wallet_actual_rpc_to_ledger_port", positive.account_facts_usable
          and positive.assessment.inventory_coverage == "COMPLETE"
          and positive.assessment.coherent_context == "COHERENT"
          and positive.assessment.supported_shape == "SUPPORTED"
          and empty.explicit_read.accounts[0].lamports == 5000000000)
    check("advancing_heads_bracket_matching_account_cut", empty.initial_finalized_slot == 100
          and positive.assessment.context_slot == 101 and empty.finalized_upper_slot == 102 and empty.anchor.slot == 101)
    switched = Scenario()
    switched.end_genesis = BLOCK
    switched_obs, switched_port = run(switched)
    check("end_genesis_switch_denies_composed_observation", not switched_port.account_facts_usable
          and switched_obs.observed_genesis == GENESIS and switched_obs.observed_genesis_end == BLOCK)
    check("strict_read_only_methods_and_finalized_configs", set(call[0]["method"] for call in scenario.calls) <= set(READ_ONLY_METHODS)
          and all(call[0]["params"][-1].get("commitment") == "finalized"
                  for call in scenario.calls if call[0]["method"] != "getGenesisHash")
          and all(call[1]["read"] == PROFILE.request_timeout_seconds for call in scenario.calls))
    check("endpoint_and_ignored_rpc_metadata_not_persisted", "DO_NOT_RETAIN" not in repr(empty)
          and "fixture.invalid" not in repr(empty) and empty.anchor.provider_fingerprint == PROFILE.fingerprint)
    check("nested_evidence_immutable", raises(FrozenInstanceError, lambda: setattr(empty.explicit_read.accounts[0], "lamports", 1))
          and raises(PublicRpcError, lambda: replace(empty, inventories=list(empty.inventories))))
    versions = Scenario()
    versions.omit_api_version = True
    unreported, unreported_port = run(versions)
    check("optional_node_version_retained_as_unreported", unreported_port.account_facts_usable
          and all(item.context.api_version == "UNREPORTED" for item in unreported.inventories))

    normal = Scenario().add_token().add_token(token=TOKEN22, mint=MINT22, program=TOKEN_2022_PROGRAM_ID)
    obs, normal_port = run(normal)
    check("normal_SPL_and_base_Token2022_with_mint_truth", normal_port.account_facts_usable
          and len(normal_port.assessment.tokens) == 2 and len(normal_port.assessment.mints) == 2
          and {token.amount for token in normal_port.assessment.tokens} == {700}
          and all(mint.mint_authority == OTHER and mint.freeze_authority == OTHER for mint in normal_port.assessment.mints))
    reserve, amount = 2039280, 500000000
    native = Scenario().add_token(mint=WSOL_MINT, data=token_data(WSOL_MINT, amount=amount, reserve=reserve),
                                  mint_bytes=mint_data(9), lamports=reserve+amount)
    _, native_port = run(native)
    check("WSOL_native_reserve_and_units_retained_without_spendability", native_port.account_facts_usable
          and native_port.assessment.tokens[0].native_rent_reserve == reserve
          and native_port.assessment.tokens[0].amount == amount)
    absent = Scenario()
    absent.accounts[MINT] = account(TOKEN_PROGRAM_ID, mint_data())
    expected = WalletEvidenceRequest(WALLET, GENESIS, 100, (ExpectedTokenAccount(TOKEN, MINT, TOKEN_PROGRAM_ID),))
    absent_obs, absent_port = run(absent, request=expected)
    check("expected_absent_account_is_explicit_null_at_context", absent_port.account_facts_usable
          and absent_obs.explicit_read.requested_keys == (WALLET, TOKEN) and absent_obs.explicit_read.accounts[1] is None)
    zero = Scenario().add_token(data=token_data(amount=0))
    zero.accounts[WALLET]["lamports"] = 0
    zero_obs, zero_port = run(zero, request=expected)
    check("present_zero_distinct_from_absent_and_no_funding_decision", zero_port.account_facts_usable
          and zero_obs.explicit_read.accounts[0].lamports == 0 and zero_obs.explicit_read.accounts[1] is not None
          and zero_port.assessment.tokens[0].amount == 0)
    absent_native = Scenario()
    absent_native.accounts[WALLET] = None
    _, absent_native_port = run(absent_native)
    check("absent_native_wallet_is_unresolved_not_zero", not absent_native_port.account_facts_usable
          and absent_native_port.assessment.native_account_presence == "ABSENT")

    for name, data in (("frozen", token_data(frozen=True)), ("delegate", token_data(delegate=OTHER, delegated=2)),
                       ("external_close", token_data(close=OTHER)), ("extension_account", token_data(tail=b"\x02"))):
        case = Scenario().add_token(data=data)
        _, port = run(case)
        check(f"{name}_unsupported_despite_complete_enumeration", port.assessment.inventory_coverage == "COMPLETE"
              and port.assessment.supported_shape == "UNSUPPORTED" and not port.account_facts_usable)
    own_close = Scenario().add_token(data=token_data(close=WALLET))
    _, own_close_port = run(own_close)
    check("wallet_close_authority_retained", own_close_port.account_facts_usable
          and own_close_port.assessment.tokens[0].close_authority == WALLET)
    for name, program, token, mint in (("SPL", TOKEN_PROGRAM_ID, TOKEN, MINT), ("Token2022", TOKEN_2022_PROGRAM_ID, TOKEN22, MINT22)):
        extended_mint = Scenario().add_token(program=program, token=token, mint=mint, mint_bytes=mint_data(tail=b"\x00" * 84))
        _, port = run(extended_mint)
        check(f"{name}_extended_mint_unsupported", port.assessment.inventory_coverage == "COMPLETE"
              and port.assessment.supported_shape == "UNSUPPORTED" and not port.account_facts_usable)
    extended22 = Scenario().add_token(program=TOKEN_2022_PROGRAM_ID, token=TOKEN22, mint=MINT22,
                                      data=token_data(MINT22, tail=b"\x02\x01\x00"))
    _, extended22_port = run(extended22)
    check("Token2022_account_TLV_shape_not_guessed", extended22_port.assessment.supported_shape == "UNSUPPORTED")
    invalid_option = bytearray(token_data())
    invalid_option[72:76] = (2).to_bytes(4, "little")
    _, invalid_option_port = run(Scenario().add_token(data=bytes(invalid_option)))
    check("invalid_COption_tag_denies", "INVALID_TOKEN_BASE_LAYOUT" in invalid_option_port.assessment.reasons)
    for name, setup in (
        ("wrong_wallet_authority", lambda s: s.add_token(data=token_data(owner=OTHER))),
        ("wrong_mint_program", lambda s: (s.add_token(), s.accounts[MINT].update(owner=TOKEN_2022_PROGRAM_ID))),
        ("missing_mint", lambda s: (s.add_token(), s.accounts.pop(MINT))),
        ("missing_token_program", lambda s: setattr(s, "program_failure", TOKEN_2022_PROGRAM_ID)),
        ("unknown_genesis", lambda s: setattr(s, "genesis", str(Hash.from_bytes(bytes([25]) * 32)))),
        ("native_data_unsupported", lambda s: s.accounts.update({WALLET: account(SYSTEM_PROGRAM_ID, b"x")})),
        ("native_executable_unsupported", lambda s: s.accounts[WALLET].update(executable=True)),
    ):
        case = Scenario()
        setup(case)
        _, port = run(case)
        check(name, not port.account_facts_usable)
    omission = Scenario().add_token()
    omission.inventory[TOKEN_PROGRAM_ID] = []
    _, omission_port = run(omission, request=expected)
    check("known_account_omitted_from_same_context_enumeration_contradicts", omission_port.assessment.inventory_coverage == "CONTRADICTORY")
    conflict = Scenario().add_token()
    conflict.explicit_override[TOKEN] = account(TOKEN_PROGRAM_ID, token_data(amount=999))
    _, conflict_port = run(conflict, request=expected)
    check("same_context_conflicting_account_retained_and_denied", conflict_port.assessment.inventory_coverage == "CONTRADICTORY"
          and {shape.amount for shape in conflict_port.assessment.tokens} == {700, 999})
    wrong_expected = WalletEvidenceRequest(WALLET, GENESIS, 100, (ExpectedTokenAccount(TOKEN, MINT22, TOKEN_PROGRAM_ID),))
    wrong_case = Scenario().add_token()
    wrong_case.accounts[MINT22] = account(TOKEN_PROGRAM_ID, mint_data())
    _, wrong_port = run(wrong_case, request=wrong_expected)
    check("expected_mint_binding_checked", "EXPECTED_ACCOUNT_MINT_OR_PROGRAM_MISMATCH" in wrong_port.assessment.reasons)

    for name, program_slot, want in (("differing", 102, "ACCOUNT_CONTEXTS_DIFFER"),
                                    ("future", 999, "FINALIZED_CONTEXT_BOUNDS_OR_PROVIDER_MISMATCH")):
        different = Scenario()
        different.program_context[TOKEN_2022_PROGRAM_ID] = program_slot
        _, port = run(different)
        check(f"{name}_context_denied", not port.account_facts_usable and want in port.assessment.reasons)
    below = Scenario()
    below.program_context[TOKEN_PROGRAM_ID] = 99
    _, below_port = run(below)
    check("min_context_floor_enforced_by_actual_rpc", not below_port.account_facts_usable
          and any("RPC_CONTEXT_BELOW_FLOOR" in reason for reason in below_port.assessment.reasons))
    for name, block_time in (("stale", NOW-120), ("future", NOW+1), ("unknown", None)):
        timing = Scenario()
        timing.block_time = block_time
        _, port = run(timing)
        check(f"{name}_finalized_time_denied", not port.account_facts_usable)
    null_block = Scenario()
    null_block.transform = lambda p, e: {**e, "result": None} if p["method"] == "getBlock" else e
    _, null_block_port = run(null_block)
    check("null_or_pruned_anchor_is_unknown", not null_block_port.account_facts_usable
          and null_block_port.assessment.coherent_context == "UNKNOWN")

    for name, override in (
        ("wrong_wallet", {"expected_wallet": OTHER}), ("wrong_genesis", {"expected_genesis": BLOCK}),
        ("wrong_provider", {"expected_profile_fingerprint": "0"*64}),
        ("higher_floor", {"required_min_context_slot": 102}),
        ("stale_observation", {"now_utc": utc(NOW+31)}), ("future_clock", {"now_utc": utc(NOW-1)}),
    ):
        kwargs = {"expected_wallet": WALLET, "expected_genesis": GENESIS,
                  "expected_profile_fingerprint": PROFILE.fingerprint, "required_min_context_slot": 100, "now_utc": utc()}
        kwargs.update(override)
        check(f"consumer_rechecks_{name}", not ledger_account_evidence(empty, **kwargs).account_facts_usable)
    lower_request_obs, _ = run(Scenario(), request=WalletEvidenceRequest(WALLET, GENESIS, 1))
    later_floor = ledger_account_evidence(lower_request_obs, expected_wallet=WALLET, expected_genesis=GENESIS,
                                          expected_profile_fingerprint=PROFILE.fingerprint, required_min_context_slot=101, now_utc=utc())
    check("consumer_floor_uses_proven_context_not_original_request_minimum", later_floor.account_facts_usable
          and lower_request_obs.request.min_context_slot == 1 and later_floor.assessment.context_slot == 101)
    check("forged_assessment_rejected_at_consumer_contract", raises(PublicRpcError, lambda: LedgerAccountEvidence(
        absent_native_port.observation, positive.assessment, True, ())))

    for name, raw in (("duplicate_key", b'{"jsonrpc":"2.0","id":1,"result":"x","result":"y"}'),
                      ("invalid_UTF8", b"\xff"), ("NaN", b'{"result":NaN}'),
                      ("Infinity", b'{"result":Infinity}'), ("float_overflow", b'{"result":1e999}'),
                      ("truncated_json", b'{"jsonrpc":'), ("array_envelope", b"[]")):
        protocol_error(f"protocol_{name}", lambda p, e, raw=raw: httpx.Response(200, content=raw, headers={"Content-Type": "application/json"}))
    for name, transform in (
        ("boolean_id", lambda p, e: {**e, "id": True}), ("string_id", lambda p, e: {**e, "id": "1"}),
        ("wrong_id", lambda p, e: {**e, "id": 999}), ("wrong_version", lambda p, e: {**e, "jsonrpc": "1.0"}),
        ("missing_result", lambda p, e: {"jsonrpc": "2.0", "id": p["id"]}),
        ("error_body", lambda p, e: {**e, "error": {"message": "DO_NOT_RETAIN"}}),
        ("invalid_hash", lambda p, e: {**e, "result": "invalid"}),
        ("HTTP_failure", lambda p, e: httpx.Response(429, text="DO_NOT_RETAIN")),
        ("content_type", lambda p, e: httpx.Response(200, text=json.dumps(e))),
        ("compression", lambda p, e: httpx.Response(200, json=e, headers={"Content-Encoding": "identity-unsupported"})),
    ):
        protocol_error(f"protocol_{name}", transform)
    for field, value in (("lamports", True), ("lamports", 1.0), ("lamports", -1), ("lamports", 1 << 64),
                         ("rentEpoch", -1), ("executable", 1), ("owner", "invalid"), ("data", ["!", "base64"]), ("space", 999)):
        def transform(p, e, field=field, value=value):
            e["result"]["value"][0][field] = value
            return e
        protocol_error(f"account_{field}_{str(value)[:24]}", transform, method="accounts")
    optional_float = Scenario()
    optional_float.accounts[WALLET]["ignoredUiAmount"] = 1.25
    _, optional_float_port = run(optional_float)
    check("ignored_optional_finite_float_is_harmless", optional_float_port.account_facts_usable)
    count_mismatch = lambda p, e: {**e, "result": {"context": {"slot": 101}, "value": []}}
    protocol_error("account_count_mismatch", count_mismatch, method="accounts")
    protocol_error("unknown_context_slot", lambda p, e: {**e, "result": {"context": {}, "value": [None]}}, method="accounts")
    for name, modify in (("duplicate_inventory", lambda s: s.inventory[TOKEN_PROGRAM_ID].append(TOKEN)),
                         ("wrong_program_inventory", lambda s: s.accounts[TOKEN].update(owner=SYSTEM_PROGRAM_ID))):
        duplicate = Scenario().add_token()
        modify(duplicate)
        _, port = run(duplicate)
        check(name, port.assessment.inventory_coverage == "INCOMPLETE" and not port.account_facts_usable)

    limited = replace(PROFILE, max_requests=2)
    budget_case = Scenario()
    _, budget_port = run(budget_case, profile=limited)
    check("request_budget_bounded", len(budget_case.calls) == 2 and not budget_port.account_facts_usable)
    protocol_error("response_byte_budget", lambda p, e: e, method="accounts", profile=replace(PROFILE, max_response_bytes=100))
    oversized_account = Scenario().add_token()
    _, oversized_port = run(oversized_account, profile=replace(PROFILE, max_account_bytes=82))
    check("account_byte_budget", not oversized_port.account_facts_usable)
    count_case = Scenario().add_token().add_token(token=key(30), mint=MINT)
    _, count_port = run(count_case, profile=replace(PROFILE, max_inventory_accounts_per_program=1))
    check("inventory_count_budget", not count_port.account_facts_usable)
    _, total_port = run(Scenario(), profile=replace(PROFILE, max_total_response_bytes=500))
    check("total_response_byte_budget", not total_port.account_facts_usable)
    with PublicReadOnlyRpc(ENDPOINT, PROFILE, transport=httpx.MockTransport(Scenario().handle)) as rpc:
        check("arbitrary_method_and_mutation_not_reachable", raises(PublicRpcError, lambda: rpc._post("sendTransaction", []))
              and not any(hasattr(rpc, name) for name in ("send_transaction", "sign", "simulate_transaction", "call", "request_airdrop")))
        check("request_public_key_validation", raises(PublicRpcError, lambda: rpc.get_multiple_accounts(("invalid",), min_context_slot=100)))
        check("non_token_program_denied_before_request", raises(PublicRpcError,
              lambda: rpc.get_token_accounts_by_owner(WALLET, SYSTEM_PROGRAM_ID, min_context_slot=100)))
        rpc._started -= PROFILE.observation_timeout_seconds + 1
        check("observation_deadline_bounded", raises(PublicRpcError, rpc.get_genesis_hash))
    def timeout_transport(request):
        raise httpx.ReadTimeout("DO_NOT_RETAIN", request=request)
    with PublicReadOnlyRpc(ENDPOINT, PROFILE, transport=httpx.MockTransport(timeout_transport)) as rpc:
        observed_timeout = WalletEvidenceAdapter(rpc, WalletEvidenceRequest(WALLET, GENESIS, 100), clock=lambda: utc()).observe()
    check("timeout_sanitized_and_unresolved", observed_timeout.failures[0].code == "RPC_TRANSPORT_FAILURE"
          and "DO_NOT_RETAIN" not in repr(observed_timeout))
    print(json.dumps({"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "check_count": len(CHECKS),
                      "checks": CHECKS, "failed": [name for name, value in CHECKS.items() if not value],
                      "transport": "HTTPX_MOCK_ONLY", "real_network_requests": 0}, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
