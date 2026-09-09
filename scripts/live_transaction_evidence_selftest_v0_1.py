"""Synthetic public-wire/RPC evidence fixtures; no keypairs, signing or network."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import sys
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from pathlib import Path

import httpx
from solders.hash import Hash
from solders.instruction import CompiledInstruction
from solders.message import Message, MessageHeader, MessageV0, MessageAddressTableLookup, to_bytes_versioned
from solders.pubkey import Pubkey
from solders.signature import Signature

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from live.public_rpc_v0_1 import PublicReadOnlyRpc, PublicRpcProfile, PublicRpcError, FinalizedBlockAnchor, READ_ONLY_METHODS, TOKEN_PROGRAMS
from live.transaction_evidence_v0_1 import TransactionRequest, TransactionEvidenceAdapter, ledger_transaction_evidence, runtime_transaction_order
from live.transaction_coverage_v0_1 import CoverageRequest, CoverageLimits, CanonicalCoverageAdapter, ledger_canonical_coverage, SCOPE

NOW = 1788912120
PROFILE = PublicRpcProfile("SYNTHETIC-PROVIDER", "1")
ENDPOINT = "https://fixture.invalid/private/DO_NOT_RETAIN?api-key=DO_NOT_RETAIN"
CHECKS = {}


def key(n):
    return str(Pubkey.from_bytes(bytes([n]) * 32))


def bh(n):
    return str(Hash.from_bytes(bytes([n]) * 32))


def sig(n):
    return str(Signature.from_bytes(bytes([n]) * 64))


GENESIS, RECENT = bh(200), bh(201)
WALLET, ACCOUNT, MINT, TABLE = (key(n) for n in range(1, 5))
PROGRAM = TOKEN_PROGRAMS[0]


def utc(seconds=NOW):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def raises(kind, call):
    try:
        call()
    except kind:
        return True
    return False


def transaction(n=1, *, slot=10, v0=False, failed=False):
    accounts = [Pubkey.from_string(WALLET), Pubkey.from_string(PROGRAM)]
    ix = CompiledInstruction(1, b"\x01", bytes([0, 2, 3] if v0 else [0, 2]))
    if v0:
        message = MessageV0(MessageHeader(1, 0, 1), accounts, Hash.from_string(RECENT), [ix],
            [MessageAddressTableLookup(Pubkey.from_string(TABLE), bytes([0]), bytes([1]))])
    else:
        message = Message.new_with_compiled_instructions(1, 0, 1, accounts + [Pubkey.from_string(ACCOUNT)],
                                                        Hash.from_string(RECENT), [ix])
    # An inert public signature byte vector is serialized directly. No signer or
    # keypair is created or invoked; these fixtures cannot authorize a real tx.
    wire = b"\x01" + bytes([n]) * 64 + to_bytes_versioned(message)
    count = 4 if v0 else 3
    token = {"accountIndex": 2, "mint": MINT, "owner": WALLET, "programId": PROGRAM,
             "uiTokenAmount": {"amount": "700", "decimals": 6, "uiAmount": 0.0007, "uiAmountString": "DO_NOT_RETAIN"}}
    error = {"InstructionError": [0, {"Custom": 6000}]} if failed else None
    return {"slot": slot, "blockTime": NOW - 10000, "version": 0 if v0 else "legacy",
        "transaction": [base64.b64encode(wire).decode(), "base64"], "ignoredDiagnostic": "DO_NOT_RETAIN",
        "meta": {"err": error, "fee": 5000, "preBalances": [1000000] + [2039280] * (count-1),
            "postBalances": [995000] + [2039280] * (count-1), "preTokenBalances": [token],
            "postTokenBalances": [copy.deepcopy(token)],
            "loadedAddresses": {"writable": [ACCOUNT] if v0 else [], "readonly": [MINT] if v0 else []},
            "innerInstructions": [{"index": 0, "instructions": [{"programIdIndex": 1, "accounts": [2],
                "data": "1", "stackHeight": 2}]}], "logMessages": ["DO_NOT_RETAIN"], "returnData": {"data": "DO_NOT_RETAIN"}}}


def block(slot, parent, height, signatures=(), *, time=NOW-10):
    return {"blockhash": bh(slot), "previousBlockhash": bh(parent), "parentSlot": parent,
            "blockHeight": height, "blockTime": time, "signatures": list(signatures)}


class Scenario:
    def __init__(self, n=1, *, slot=10, v0=False, failed=False):
        self.signature = sig(n)
        self.tx = transaction(n, slot=slot, v0=v0, failed=failed)
        self.status = {"slot": slot, "confirmations": None, "confirmationStatus": "finalized", "err": self.tx["meta"]["err"]}
        self.status_context = 105
        self.status_api = "3.1.0"
        self.blocks = {slot: block(slot, slot-1, slot, [self.signature], time=NOW-10000),
                       100: block(100, 99, 90)}
        self.initial_slot = self.root_slot = 100
        self.genesis = GENESIS
        self.end_genesis = None
        self.genesis_calls = self.slot_calls = 0
        self.calls = []
        self.transform = None

    def handle(self, request):
        payload = json.loads(request.content)
        self.calls.append(payload)
        method, params = payload["method"], payload["params"]
        if method == "getGenesisHash":
            result = self.genesis if not self.genesis_calls or self.end_genesis is None else self.end_genesis
            self.genesis_calls += 1
        elif method == "getSlot":
            result = self.initial_slot if not self.slot_calls else self.root_slot
            self.slot_calls += 1
        elif method == "getSignatureStatuses":
            context = {"slot": self.status_context}
            if self.status_api is not None:
                context["apiVersion"] = self.status_api
            result = {"context": context, "value": [self.status]}
        elif method == "getTransaction":
            result = self.tx
        elif method == "getBlock":
            result = copy.deepcopy(self.blocks.get(params[0]))
            if result is not None and params[1]["transactionDetails"] == "none":
                result.pop("signatures", None)
        else:
            raise AssertionError("unallowed RPC reached fixture")
        envelope = {"jsonrpc": "2.0", "id": payload["id"], "result": copy.deepcopy(result)}
        if self.transform:
            envelope = self.transform(payload, envelope)
            if isinstance(envelope, httpx.Response):
                return envelope
        return httpx.Response(200, json=envelope)


def tx_port(observation, **kwargs):
    return ledger_transaction_evidence(observation, expected_genesis=kwargs.pop("expected_genesis", GENESIS),
        expected_signature=kwargs.pop("expected_signature", observation.request.signature),
        expected_profile_fingerprint=kwargs.pop("expected_profile_fingerprint", observation.profile.fingerprint),
        now_utc=kwargs.pop("now_utc", utc()), **kwargs)


def run_tx(scenario=None, *, profile=PROFILE, min_root=100):
    scenario = scenario or Scenario()
    request = TransactionRequest(GENESIS, scenario.signature, min_root)
    with PublicReadOnlyRpc(ENDPOINT, profile, transport=httpx.MockTransport(scenario.handle)) as rpc:
        adapter = TransactionEvidenceAdapter(rpc, request, clock=lambda: utc())
        observation = adapter.observe()
        check("transaction_attempt_cannot_reset", raises(PublicRpcError, adapter.observe))
    return observation, tx_port(observation)


def deny_tx(name, change, *, profile=PROFILE):
    scenario = Scenario()
    change(scenario)
    observation, port = run_tx(scenario, profile=profile)
    check(name, port.disposition != "SUPPORTED_FINALIZED_OBSERVATION")
    check("all_transaction_failures_sanitized", "DO_NOT_RETAIN" not in repr(observation))
    return observation, port


def coverage_scenario():
    scenario = Scenario()
    scenario.blocks = {90: block(90, 89, 86, [sig(10)]), 92: block(92, 90, 87, [sig(11)]),
                       95: block(95, 92, 88, [sig(12), sig(1)]), 100: block(100, 95, 89, [sig(13)])}
    return scenario


def coverage_request(*, profile=PROFILE, signature=None, upper=None, last_valid=88):
    lower = FinalizedBlockAnchor(90, bh(90), bh(89), 89, 86, NOW-10, profile.fingerprint)
    return CoverageRequest(GENESIS, signature or sig(1), RECENT, last_valid, lower, upper)


def coverage_port(observation, **kwargs):
    return ledger_canonical_coverage(observation, expected_request=kwargs.pop("expected_request", observation.request),
        expected_profile_fingerprint=kwargs.pop("expected_profile_fingerprint", observation.profile.fingerprint),
        now_utc=kwargs.pop("now_utc", utc()), **kwargs)


def run_coverage(scenario=None, *, request=None, profile=PROFILE, limits=CoverageLimits()):
    scenario = scenario or coverage_scenario()
    request = request or coverage_request(profile=profile)
    with PublicReadOnlyRpc(ENDPOINT, profile, transport=httpx.MockTransport(scenario.handle)) as rpc:
        adapter = CanonicalCoverageAdapter(rpc, request, limits=limits, clock=lambda: utc())
        observation = adapter.observe()
        check("coverage_attempt_cannot_reset", raises(PublicRpcError, adapter.observe))
    return observation, coverage_port(observation)


def deny_coverage(name, change, **kwargs):
    scenario = coverage_scenario()
    change(scenario)
    observation, port = run_coverage(scenario, **kwargs)
    check(name, port.disposition != "COMPLETE_REQUESTED_INTERVAL")
    check("all_coverage_failures_sanitized", "DO_NOT_RETAIN" not in repr(observation))
    return observation, port


def order(left, right, **kwargs):
    return runtime_transaction_order(left, right, expected_genesis=GENESIS,
        expected_profile_fingerprint=PROFILE.fingerprint, now_utc=kwargs.pop("now_utc", utc()), **kwargs)


def main():
    scenario = Scenario()
    observed, port = run_tx(scenario)
    fact = observed.transaction
    check("legacy_adapter_to_ledger_supported", port.disposition == "SUPPORTED_FINALIZED_OBSERVATION")
    check("historical_landing_not_subject_to_root_freshness", fact.block_time == NOW-10000)
    check("exact_public_wire_and_message", fact.wire_sha256 == hashlib.sha256(fact.wire_bytes).hexdigest()
          and fact.message_sha256 == hashlib.sha256(fact.message_bytes).hexdigest() and fact.recent_blockhash == RECENT)
    check("native_balances_fee_raw_no_economic_attribution", fact.pre_lamports[0] == 1000000 and fact.post_lamports[0] == 995000 and fact.fee_lamports == 5000)
    check("raw_token_integer_identity_preserved", fact.pre_tokens[0].amount == 700 and fact.pre_tokens[0].mint == MINT and fact.pre_tokens[0].owner == WALLET and fact.pre_tokens[0].program == PROGRAM and fact.pre_tokens[0].decimals == 6)
    check("outer_and_recorded_inner_indexes", fact.outer_instructions[0].account_indexes == (0, 2) and fact.inner_instructions[0].data == b"\x00" and fact.inner_recorded_outer_indexes == (0,))
    check("metadata_display_logs_endpoint_not_retained", "DO_NOT_RETAIN" not in repr(observed) and ENDPOINT not in repr(observed))
    check("status_context_is_not_finalized_root", observed.status.context_slot == 105 and observed.root.slot == 100 and observed.status.context_semantics == "RPC_STATUS_CONTEXT_NOT_FINALIZED_ROOT")
    check("closed_exact_status_request", next(c for c in scenario.calls if c["method"] == "getSignatureStatuses")["params"] == [[sig(1)], {"searchTransactionHistory": True}])
    check("closed_exact_transaction_request", next(c for c in scenario.calls if c["method"] == "getTransaction")["params"][1] == {"commitment": "finalized", "encoding": "base64", "maxSupportedTransactionVersion": 0})
    check("closed_complete_block_request", next(c for c in scenario.calls if c["method"] == "getBlock")["params"][1] == {"commitment": "finalized", "transactionDetails": "signatures", "rewards": False, "maxSupportedTransactionVersion": 0})
    check("immutable_transaction_observation", raises(FrozenInstanceError, lambda: setattr(observed, "transaction", None)))
    check("immutable_nested_balances", type(fact.pre_lamports) is tuple)
    check("mutable_nested_contract_rejected", raises(PublicRpcError, lambda: replace(fact, pre_lamports=list(fact.pre_lamports))))
    check("typed_message_identity_revalidated", raises(PublicRpcError, lambda: replace(fact, recent_blockhash=bh(202))))
    check("typed_outer_indexes_revalidated", raises(PublicRpcError, lambda: replace(fact, outer_instructions=(replace(fact.outer_instructions[0], program_id_index=2),))))
    check("typed_wire_digest_revalidated", raises(PublicRpcError, lambda: replace(fact, wire_sha256="0"*64)))
    check("consumer_freshness_rechecks_retained_observation", tx_port(observed, now_utc=utc(NOW+31)).disposition == "UNKNOWN")
    check("consumer_future_observation_denied", tx_port(observed, now_utc=utc(NOW-1)).disposition == "UNKNOWN")
    check("consumer_chain_binding", tx_port(observed, expected_genesis=bh(202)).disposition != "SUPPORTED_FINALIZED_OBSERVATION")
    check("consumer_exact_signature_binding", tx_port(observed, expected_signature=sig(2)).disposition == "CONTRADICTORY")
    check("consumer_provider_binding", tx_port(observed, expected_profile_fingerprint="0"*64).disposition == "UNKNOWN")
    check("consumer_persisted_message_binding", tx_port(observed, expected_message_sha256="0"*64).disposition == "CONTRADICTORY")
    v0, vp = run_tx(Scenario(v0=True))
    check("v0_wire_and_loaded_lookup_identities", vp.disposition == "SUPPORTED_FINALIZED_OBSERVATION" and v0.transaction.loaded_writable == (ACCOUNT,) and v0.transaction.loaded_readonly == (MINT,) and v0.transaction.lookups[0].table == TABLE and v0.transaction.outer_instructions[0].account_indexes == (0, 2, 3))
    failed, fp = run_tx(Scenario(failed=True))
    check("failed_transaction_fee_metadata_preserved", fp.disposition == "SUPPORTED_FINALIZED_OBSERVATION" and not failed.transaction.outcome.succeeded and failed.transaction.fee_lamports == 5000 and failed.transaction.outcome == failed.status.outcome)
    check("failed_error_only_structural_fingerprint", failed.transaction.outcome.error_fingerprint is not None and "InstructionError" not in repr(failed))
    s = Scenario(); s.status = None
    null_status, np = run_tx(s)
    check("null_status_does_not_erase_positive_finalized_transaction", np.disposition == "SUPPORTED_FINALIZED_OBSERVATION" and null_status.status.slot is None)
    s = Scenario(); s.status.update(confirmationStatus="confirmed", confirmations=2)
    check("earlier_confirmed_status_can_progress_to_finalized_tx", run_tx(s)[1].disposition == "SUPPORTED_FINALIZED_OBSERVATION")
    s = Scenario(); s.status_api = None
    check("optional_node_api_version_unreported", run_tx(s)[0].status.api_version == "UNREPORTED")
    s = Scenario(); s.tx["meta"]["status"] = {"Ok": None}; s.status["status"] = {"Ok": None}
    check("optional_deprecated_status_consistent", run_tx(s)[1].disposition == "SUPPORTED_FINALIZED_OBSERVATION")
    s = Scenario(); s.tx["blockTime"] = None; s.blocks[10]["blockTime"] = None
    check("historical_block_time_optional", run_tx(s)[1].disposition == "SUPPORTED_FINALIZED_OBSERVATION")
    for field in ("err", "fee", "preBalances", "postBalances", "preTokenBalances", "postTokenBalances", "loadedAddresses", "innerInstructions"):
        deny_tx("required_metadata_"+field, lambda s, field=field: s.tx["meta"].pop(field))
    deny_tx("null_transaction_unknown", lambda s: setattr(s, "tx", None))
    deny_tx("null_metadata_unknown", lambda s: s.tx.update(meta=None))
    deny_tx("null_inner_recording_unknown", lambda s: s.tx["meta"].update(innerInstructions=None))
    deny_tx("null_token_recording_unknown", lambda s: s.tx["meta"].update(preTokenBalances=None))
    deny_tx("partial_native_balance_count", lambda s: s.tx["meta"].update(preBalances=[1]))
    deny_tx("native_float_not_integer_economics", lambda s: s.tx["meta"]["preBalances"].__setitem__(0, 1.0))
    deny_tx("fee_bool_denied", lambda s: s.tx["meta"].update(fee=True))
    deny_tx("fee_u64_overflow_denied", lambda s: s.tx["meta"].update(fee=1 << 64))
    for field in ("owner", "programId", "mint"):
        deny_tx("token_identity_missing_"+field, lambda s, field=field: s.tx["meta"]["preTokenBalances"][0].pop(field))
    deny_tx("token_program_unknown", lambda s: s.tx["meta"]["preTokenBalances"][0].update(programId=key(9)))
    deny_tx("token_owner_invalid", lambda s: s.tx["meta"]["preTokenBalances"][0].update(owner="DO_NOT_RETAIN"))
    for amount in ("00", "1.0", "-1", "18446744073709551616"):
        deny_tx("raw_token_amount_rejects_"+amount, lambda s, amount=amount: s.tx["meta"]["preTokenBalances"][0]["uiTokenAmount"].update(amount=amount))
    deny_tx("token_decimal_float_denied", lambda s: s.tx["meta"]["preTokenBalances"][0]["uiTokenAmount"].update(decimals=6.0))
    deny_tx("token_duplicate_account_index", lambda s: s.tx["meta"]["preTokenBalances"].append(copy.deepcopy(s.tx["meta"]["preTokenBalances"][0])))
    deny_tx("token_index_outside_message", lambda s: s.tx["meta"]["preTokenBalances"][0].update(accountIndex=3))
    deny_tx("unexpected_loaded_addresses_legacy", lambda s: s.tx["meta"]["loadedAddresses"].update(writable=[ACCOUNT]))
    s = Scenario(v0=True); s.tx["meta"]["loadedAddresses"]["readonly"] = []
    check("v0_missing_loaded_address_unknown", run_tx(s)[1].disposition != "SUPPORTED_FINALIZED_OBSERVATION")
    s = Scenario(v0=True); s.tx["meta"]["loadedAddresses"]["writable"] = [WALLET]
    check("v0_duplicate_loaded_static_identity_denied", run_tx(s)[1].disposition != "SUPPORTED_FINALIZED_OBSERVATION")
    deny_tx("inner_program_index_invalid", lambda s: s.tx["meta"]["innerInstructions"][0]["instructions"][0].update(programIdIndex=9))
    deny_tx("inner_account_index_invalid", lambda s: s.tx["meta"]["innerInstructions"][0]["instructions"][0].update(accounts=[9]))
    deny_tx("inner_outer_index_invalid", lambda s: s.tx["meta"]["innerInstructions"][0].update(index=9))
    deny_tx("inner_duplicate_recording_group", lambda s: s.tx["meta"]["innerInstructions"].append(copy.deepcopy(s.tx["meta"]["innerInstructions"][0])))
    deny_tx("inner_data_invalid_base58", lambda s: s.tx["meta"]["innerInstructions"][0]["instructions"][0].update(data="0"))
    deny_tx("inner_instruction_budget", lambda s: s.tx["meta"]["innerInstructions"][0]["instructions"].append(copy.deepcopy(s.tx["meta"]["innerInstructions"][0]["instructions"][0])), profile=replace(PROFILE, max_inner_instructions=1))
    deny_tx("instruction_data_budget", lambda s: s.tx["meta"]["innerInstructions"][0]["instructions"][0].update(data="111"), profile=replace(PROFILE, max_instruction_data_bytes=1))
    deny_tx("wire_trailing_bytes_rejected", lambda s: s.tx["transaction"].__setitem__(0, base64.b64encode(base64.b64decode(s.tx["transaction"][0])+b"\0").decode()))
    deny_tx("wire_signature_not_requested", lambda s: s.tx.update(transaction=transaction(2)["transaction"]))
    deny_tx("wire_base64_noncanonical", lambda s: s.tx["transaction"].__setitem__(0, s.tx["transaction"][0]+"="))
    deny_tx("wire_version_mismatch", lambda s: s.tx.update(version=0))
    deny_tx("unsupported_transaction_version", lambda s: s.tx.update(version=1))
    deny_tx("wire_size_budget", lambda s: None, profile=replace(PROFILE, max_transaction_wire_bytes=50))
    _, contradiction = deny_tx("status_tx_slot_contradiction", lambda s: s.status.update(slot=9))
    check("slot_contradiction_classified", contradiction.disposition == "CONTRADICTORY")
    deny_tx("status_tx_outcome_contradiction", lambda s: s.status.update(err="DO_NOT_RETAIN"))
    deny_tx("meta_optional_status_contradiction", lambda s: s.tx["meta"].update(status={"Err": "DO_NOT_RETAIN"}))
    deny_tx("status_optional_status_contradiction", lambda s: s.status.update(status={"Err": "DO_NOT_RETAIN"}))
    deny_tx("status_context_before_reported_tx", lambda s: setattr(s, "status_context", 9))
    deny_tx("status_finalization_confirmations_inconsistent", lambda s: s.status.update(confirmations=2))
    deny_tx("status_unknown_confirmation", lambda s: s.status.update(confirmationStatus="DO_NOT_RETAIN"))
    deny_tx("membership_primary_missing", lambda s: s.blocks[10].update(signatures=[]))
    deny_tx("membership_pruned_unknown", lambda s: s.blocks.pop(10))
    deny_tx("membership_time_contradiction", lambda s: s.blocks[10].update(blockTime=NOW-9999))
    deny_tx("root_stale", lambda s: s.blocks[100].update(blockTime=NOW-121))
    deny_tx("root_future", lambda s: s.blocks[100].update(blockTime=NOW+1))
    deny_tx("root_time_unknown", lambda s: s.blocks[100].update(blockTime=None))
    deny_tx("transaction_future_time", lambda s: (s.tx.update(blockTime=NOW+1), s.blocks[10].update(blockTime=NOW+1)))
    deny_tx("transaction_height_after_later_root", lambda s: s.blocks[10].update(blockHeight=90))
    deny_tx("transaction_impossible_height_slot_delta", lambda s: (s.blocks[10].update(blockHeight=1), s.blocks[100].update(blockHeight=92)))
    deny_tx("block_height_exceeds_slot", lambda s: s.blocks[10].update(blockHeight=11))
    deny_tx("root_height_exceeds_slot", lambda s: s.blocks[100].update(blockHeight=101))
    s = Scenario(slot=100); s.blocks[100] = block(100, 99, 90, [sig(1)], time=NOW-10); s.tx["blockTime"] = NOW-10
    check("same_slot_membership_root_consistent", run_tx(s)[1].disposition == "SUPPORTED_FINALIZED_OBSERVATION")
    for field, bad in (("blockhash", bh(99)), ("previousBlockhash", bh(98)), ("parentSlot", 98), ("blockHeight", 91)):
        s = Scenario(slot=100); s.blocks[100] = block(100, 99, 90, [sig(1)], time=NOW-10); s.tx["blockTime"] = NOW-10
        def alter(payload, envelope, field=field, bad=bad):
            if payload["method"] == "getBlock" and payload["params"][1]["transactionDetails"] == "none":
                envelope["result"][field] = bad
            return envelope
        s.transform = alter
        check("same_slot_root_contradiction_"+field, run_tx(s)[1].disposition == "CONTRADICTORY")
    deny_tx("start_genesis_wrong", lambda s: setattr(s, "genesis", bh(202)))
    deny_tx("end_genesis_changed", lambda s: setattr(s, "end_genesis", bh(202)))
    deny_tx("transaction_request_budget_exhaustion", lambda s: None, profile=replace(PROFILE, max_requests=2))
    deny_tx("transaction_response_byte_bound", lambda s: None, profile=replace(PROFILE, max_response_bytes=100))
    def rpc_error(payload, envelope):
        if payload["method"] == "getTransaction":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "error": {"code": -32009, "message": "DO_NOT_RETAIN"}})
        return envelope
    deny_tx("rpc_pruning_error_unknown_sanitized", lambda s: setattr(s, "transform", rpc_error))
    s = Scenario(); s.tx = None; s.status = None
    check("null_status_and_tx_never_prove_nonlanding", run_tx(s)[1].disposition == "UNKNOWN")

    scenario = coverage_scenario()
    coverage, cp = run_coverage(scenario)
    check("complete_parent_link_interval_with_skipped_slots", cp.disposition == "COMPLETE_REQUESTED_INTERVAL" and tuple(b.slot for b in coverage.blocks_descending) == (100, 95, 92, 90))
    check("coverage_walk_only_produced_parent_slots", [c["params"][0] for c in scenario.calls if c["method"] == "getBlock" and c["params"][1]["transactionDetails"] == "signatures"] == [100, 95, 92, 90])
    check("signature_occurrence_exact_block_index", [(x.slot, x.transaction_index) for x in cp.signature_occurrences] == [(95, 1)])
    check("coverage_original_identity_and_validity_facts", coverage.request.recent_blockhash == RECENT and coverage.request.last_valid_block_height == 88 and cp.chosen_upper_block_height == 89 and cp.fresh_root_block_height == 89)
    absent, ap = run_coverage(request=coverage_request(signature=sig(99)))
    check("absence_fact_only_complete_requested_interval", ap.disposition == "COMPLETE_REQUESTED_INTERVAL" and ap.signature_occurrences == () and ap.scope == SCOPE and "NON_LANDED" not in repr(ap))
    historical, hp = run_coverage(request=coverage_request(upper=92))
    check("historical_complete_interval_has_explicit_missing_validity_tail", hp.disposition == "COMPLETE_REQUESTED_INTERVAL" and hp.chosen_upper_block_height < historical.request.last_valid_block_height < hp.fresh_root_block_height and hp.scope == SCOPE)
    early, ep = run_coverage(request=coverage_request(last_valid=102))
    check("root_before_last_valid_preserved_only_as_fact", ep.disposition == "COMPLETE_REQUESTED_INTERVAL" and ep.fresh_root_block_height < early.request.last_valid_block_height and not hasattr(ep, "retry_allowed"))
    s = coverage_scenario(); s.blocks[92].pop("blockTime"); s.blocks[90]["blockTime"] = None
    check("intermediate_and_lower_times_not_hash_link_requirements", run_coverage(s)[1].disposition == "COMPLETE_REQUESTED_INTERVAL")
    partial, pp = deny_coverage("missing_intermediate_block_insufficient", lambda s: s.blocks.pop(92))
    check("partial_blocks_and_positive_occurrence_survive", tuple(b.slot for b in partial.blocks_descending) == (100, 95) and [(x.slot, x.transaction_index) for x in pp.signature_occurrences] == [(95, 1)])
    for field, bad in (("previousBlockhash", bh(91)), ("blockHeight", 90), ("parentSlot", 90)):
        deny_coverage("broken_canonical_link_"+field, lambda s, field=field, bad=bad: s.blocks[95].update({field: bad}))
    deny_coverage("lower_anchor_hash_changed", lambda s: s.blocks[90].update(blockhash=bh(91)))
    deny_coverage("lower_anchor_core_changed", lambda s: s.blocks[90].update(previousBlockhash=bh(88)))
    deny_coverage("parent_skips_original_lower", lambda s: s.blocks[92].update(parentSlot=89, previousBlockhash=bh(89)))
    deny_coverage("parent_not_decreasing", lambda s: s.blocks[95].update(parentSlot=95))
    deny_coverage("missing_complete_signature_list", lambda s: s.blocks[92].pop("signatures"))
    deny_coverage("null_complete_signature_list", lambda s: s.blocks[92].update(signatures=None))
    deny_coverage("pagination_extension_not_complete_standard_list", lambda s: s.blocks[92].update(nextPage="DO_NOT_RETAIN"))
    deny_coverage("duplicate_signature_within_block", lambda s: s.blocks[95]["signatures"].append(sig(1)))
    _, dup = deny_coverage("duplicate_signature_across_blocks", lambda s: s.blocks[92]["signatures"].append(sig(1)))
    check("duplicate_cross_block_claim_contradictory", dup.disposition == "CONTRADICTORY" and len(dup.signature_occurrences) == 2)
    limited, lp = deny_coverage("coverage_block_budget", lambda s: None, limits=CoverageLimits(max_blocks=2))
    check("block_budget_preserves_partial_exact_evidence", len(limited.blocks_descending) == 2 and len(lp.signature_occurrences) == 1)
    limited, _ = deny_coverage("coverage_slot_span_budget", lambda s: None, limits=CoverageLimits(max_slot_span=5))
    check("slot_span_limit_checked_before_walk", limited.blocks_descending == ())
    deny_coverage("coverage_request_budget", lambda s: None, profile=replace(PROFILE, max_requests=4))
    deny_coverage("coverage_signature_count_budget", lambda s: None, profile=replace(PROFILE, max_block_signatures=1))
    deny_coverage("coverage_total_byte_budget", lambda s: None, profile=replace(PROFILE, max_total_response_bytes=500))
    deny_coverage("coverage_upper_not_finalized", lambda s: None, request=coverage_request(upper=101))
    deny_coverage("coverage_stale_root", lambda s: s.blocks[100].update(blockTime=NOW-121))
    deny_coverage("coverage_future_root", lambda s: s.blocks[100].update(blockTime=NOW+1))
    deny_coverage("coverage_impossible_root_height_delta", lambda s: s.blocks[100].update(blockHeight=96), request=coverage_request(upper=92))
    deny_coverage("coverage_genesis_changed", lambda s: setattr(s, "end_genesis", bh(202)))
    check("coverage_consumer_freshness", coverage_port(coverage, now_utc=utc(NOW+31)).disposition == "INSUFFICIENT_COVERAGE")
    check("coverage_consumer_provider_binding", coverage_port(coverage, expected_profile_fingerprint="0"*64).disposition == "INSUFFICIENT_COVERAGE")
    for field, value in (("recent_blockhash", bh(202)), ("last_valid_block_height", 103), ("signature", sig(99)), ("upper_slot", 95)):
        changed = replace(coverage.request, **{field: value})
        check("consumer_original_coverage_binding_"+field, coverage_port(coverage, expected_request=changed).disposition != "COMPLETE_REQUESTED_INTERVAL")
    check("immutable_coverage_blocks", raises(PublicRpcError, lambda: replace(coverage, blocks_descending=list(coverage.blocks_descending))))
    check("immutable_coverage_occurrences", type(cp.signature_occurrences) is tuple and raises(FrozenInstanceError, lambda: setattr(cp.signature_occurrences[0], "slot", 0)))
    check("coverage_tamper_reduced_independently", coverage_port(replace(coverage, blocks_descending=coverage.blocks_descending[:-1])).disposition != "COMPLETE_REQUESTED_INTERVAL")

    left_s, right_s = Scenario(1), Scenario(2)
    left_s.blocks[10]["signatures"] = right_s.blocks[10]["signatures"] = [sig(1), sig(2)]
    left, _ = run_tx(left_s); right, _ = run_tx(right_s)
    check("same_slot_exact_transaction_order_before", order(left, right).relation == "BEFORE")
    check("same_slot_exact_transaction_order_after", order(right, left).relation == "AFTER")
    check("same_transaction_requires_event_order", order(left, left).relation == "UNKNOWN" and order(left, left).reasons == ("EVENT_ORDER_REQUIRED_WITHIN_SAME_TRANSACTION",))
    later, _ = run_tx(Scenario(3, slot=11))
    check("different_canonical_slot_order", order(left, later).relation == "BEFORE" and order(later, left).relation == "AFTER")
    check("order_consumer_freshness", order(left, right, now_utc=utc(NOW+31)).relation == "UNKNOWN")
    altered = replace(right, membership_block=replace(right.membership_block, signatures=(sig(2), sig(1))))
    check("same_slot_conflicting_order_unknown", order(left, altered).relation == "UNKNOWN")
    for field, bad in (("blockhash", bh(101)), ("previousBlockhash", bh(98)), ("parentSlot", 98), ("blockHeight", 91)):
        s = Scenario(3, slot=11); s.blocks[100][field] = bad
        conflict, conflict_port = run_tx(s)
        check("paired_observations_conflicting_root_"+field, conflict_port.disposition == "SUPPORTED_FINALIZED_OBSERVATION"
              and order(left, conflict).relation == "UNKNOWN" and "KNOWN_FINALIZED_ANCHOR_CONTRADICTION" in order(left, conflict).reasons)
    s = Scenario(3, slot=11); s.initial_slot = s.root_slot = 99
    s.blocks[99] = block(99, 98, 89); s.blocks[99]["blockhash"] = bh(98)
    conflict, conflict_port = run_tx(s, min_root=99)
    check("paired_known_parent_hash_conflict", conflict_port.disposition == "SUPPORTED_FINALIZED_OBSERVATION" and order(left, conflict).relation == "UNKNOWN")
    s = Scenario(3, slot=11); s.initial_slot = s.root_slot = 99
    s.blocks[99] = block(99, 98, 88)
    conflict, conflict_port = run_tx(s, min_root=99)
    check("paired_impossible_root_height_slot_delta", conflict_port.disposition == "SUPPORTED_FINALIZED_OBSERVATION" and order(left, conflict).relation == "UNKNOWN")
    check("transaction_order_has_no_event_eligibility", order(left, later).scope == "CANONICAL_TRANSACTION_ORDER_ONLY" and not hasattr(order(left, later), "eligible"))

    check("public_method_allowlist_no_mutations", set(READ_ONLY_METHODS) == {"getGenesisHash", "getSlot", "getBlock", "getMultipleAccounts", "getTokenAccountsByOwner", "getSignatureStatuses", "getTransaction"})
    check("no_public_generic_send_sign_or_simulation", not any(hasattr(PublicReadOnlyRpc, name) for name in ("call", "request", "send", "send_transaction", "sign", "simulate_transaction")))
    check("unsupported_trust_profile_rejected", raises(PublicRpcError, lambda: replace(PROFILE, trust_profile="UNKNOWN")))
    check("invalid_primary_signature_before_transport", raises(PublicRpcError, lambda: TransactionRequest(GENESIS, "DO_NOT_RETAIN", 100)))
    print(json.dumps({"checks": CHECKS, "count": len(CHECKS), "all_true": all(CHECKS.values())}, sort_keys=True))


if __name__ == "__main__":
    main()
