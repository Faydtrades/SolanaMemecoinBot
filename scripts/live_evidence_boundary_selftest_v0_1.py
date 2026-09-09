"""Composed Evidence adapter-to-port facts; synthetic SQLite and mocked RPC only.

Real Authority, Ledger and Runtime consumers are not built by this test. No
position, reservation, baseline, retry decision or execution state is created.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import FrozenInstanceError
from pathlib import Path

import httpx

import live_source_health_selftest_v0_1 as source
import live_wallet_evidence_selftest_v0_1 as wallet
import live_transaction_evidence_selftest_v0_1 as transaction
from live.evidence_store_v0_1 import SourceEvidenceStore
from live.source_health_v0_1 import CollectorSourceAdapter, SourceProfile, ZERO_DIGEST, source_consumer_evidence
from live.public_rpc_v0_1 import PublicReadOnlyRpc, PublicRpcError, READ_ONLY_METHODS
from live.wallet_evidence_v0_1 import ExpectedTokenAccount, WalletEvidenceAdapter, WalletEvidenceRequest, ledger_account_evidence
from live.transaction_coverage_v0_1 import SCOPE


CHECKS = {}


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def main():
    observations, ports, rpc_calls = [], [], []
    with tempfile.TemporaryDirectory(prefix="meme_live_boundary_") as directory:
        temporary = Path(directory)
        path, journal_path = temporary / "collector.sqlite3", temporary / "evidence.sqlite3"
        binding, profile = source.fixture(path), SourceProfile()
        good = source.observe(path, binding, now=10)
        with SourceEvidenceStore(journal_path, binding, profile) as journal:
            journal.append(good, expected_previous_digest=ZERO_DIGEST)
            good_port = source_consumer_evidence(journal.latest(), expected_source_identity=binding.source_identity,
                required_cut_utc=source.at(9), now_utc=source.at(10))
            check("SQLite_to_journal_to_Authority_Runtime_port_healthy", good_port.observed_prefix_supported
                  and good_port.evidence_digest == good.content_digest)
            missing = CollectorSourceAdapter(temporary / "missing.sqlite3").observe(binding, profile,
                observed_at_utc=source.at(11), requested_cut_utc=source.at(9), previous=good)
            journal.append(missing, expected_previous_digest=good.content_digest)
            missing_port = source_consumer_evidence(journal.latest(), expected_source_identity=binding.source_identity,
                required_cut_utc=source.at(9), now_utc=source.at(11))
            check("actual_source_read_failure_denies_and_retains_original_progress", not missing_port.observed_prefix_supported
                  and missing.progress == good.progress and "SOURCE_UNAVAILABLE_OR_SCHEMA_MISSING" in missing_port.reasons)
        with SourceEvidenceStore(journal_path, binding, profile) as journal:
            restored = source.observe(path, binding, previous=journal.latest(), now=12)
            journal.append(restored, expected_previous_digest=missing.content_digest)
            check("restored_same_lineage_facts_recover_read_failure_after_reopen", restored.disposition == "HEALTHY"
                  and restored.binding == binding and restored.progress.cursors == good.progress.cursors)
            stale = source.observe(path, binding, previous=restored, now=42)
            journal.append(stale, expected_previous_digest=restored.content_digest)
            still_stale = source.observe(path, binding, previous=stale, now=43)
            check("reread_without_new_receipt_cannot_recover_staleness", still_stale.disposition == "STALE")
            source.change(path, "INSERT INTO websocket_observations VALUES (?,?,?,?,?)", ("fresh", 104, source.at(44), 1, ""))
            recovered = source.observe(path, binding, previous=stale, now=45, cut=43)
            journal.append(recovered, expected_previous_digest=stale.content_digest)
            recovered_port = source_consumer_evidence(journal.latest(), expected_source_identity=binding.source_identity,
                required_cut_utc=source.at(43), now_utc=source.at(45))
            check("positive_same_lineage_receipt_restores_source_port", recovered_port.observed_prefix_supported
                  and recovered.binding == binding and recovered.covered_from_utc == binding.coverage_start_utc)
            source.change(path, "INSERT INTO gap_jobs_v034 VALUES (?,?,?,?,?,?,?,?,?)",
                ("later-gap", source.at(46), source.at(47), "DONE", 104, 105, 1, 0, "DO_NOT_RETAIN"))
            source.change(path, "INSERT INTO websocket_observations VALUES (?,?,?,?,?)", ("later", 105, source.at(48), 1, ""))
            source_hash = hashlib.sha256(path.read_bytes()).hexdigest()
            unresolved = source.observe(path, binding, previous=recovered, now=49, cut=43)
            journal.append(unresolved, expected_previous_digest=recovered.content_digest)
            captured = journal.latest_record()
        with SourceEvidenceStore(journal_path, binding, profile) as journal:
            retained = source.observe(path, binding, previous=journal.latest(), now=50, cut=43)
            journal.append(retained, expected_previous_digest=unresolved.content_digest)
            captured_port = source_consumer_evidence(journal.read_record(captured[0])[1],
                expected_source_identity=binding.source_identity, required_cut_utc=source.at(43), now_utc=source.at(50))
            check("known_later_gap_denies_older_cut_after_append_reopen_and_capture", not captured_port.observed_prefix_supported
                  and "UNRESOLVED_RECORDED_GAP" in captured_port.reasons and retained.disposition == "GAP"
                  and journal.read_record(captured[0]) == captured and journal.count() == 7)
        check("source_adapter_and_journal_do_not_mutate_source_bytes", hashlib.sha256(path.read_bytes()).hexdigest() == source_hash)
        with closing(sqlite3.connect(journal_path)) as conn, conn:
            check("journal_history_is_append_only_with_no_economic_tables", all(source.raises(sqlite3.IntegrityError,
                lambda sql=sql: conn.execute(sql)) for sql in ("DELETE FROM source_evidence WHERE seq=1",
                    "UPDATE source_evidence SET payload_json='{}' WHERE seq=1"))
                and {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                == {"source_domain", "source_evidence"})
            check("reopened_source_journal_integrity_ok", conn.execute("PRAGMA integrity_check").fetchone() == ("ok",))
        observations.extend((good, missing, restored, stale, recovered, unresolved, retained))
        ports.extend((good_port, missing_port, recovered_port, captured_port))

    # Account and transaction fixtures share public wallet, genesis, provider
    # profile and a consistent finalized root. These are factual ports only.
    account_ports = {}
    request = WalletEvidenceRequest(transaction.WALLET, transaction.GENESIS, 100,
        (ExpectedTokenAccount(transaction.ACCOUNT, transaction.MINT, transaction.PROGRAM),))
    for case in ("complete", "incomplete", "unsupported", "contradictory", "different_contexts", "native_absent", "native_zero", "invalid_anchor_height"):
        scenario = wallet.Scenario().add_token(token=transaction.ACCOUNT, mint=transaction.MINT)
        scenario.add_token(token=wallet.TOKEN22, mint=wallet.MINT22, program=wallet.TOKEN_2022_PROGRAM_ID,
            data=wallet.token_data(wallet.MINT22, tail=b"unsupported" if case == "unsupported" else b""))
        scenario.genesis = transaction.GENESIS
        scenario.context = scenario.multiple_context = scenario.upper_slot = 100
        anchor = transaction.block(100, 9 if case == "invalid_anchor_height" else 99, 90)

        def anchor_response(payload, envelope, anchor=anchor):
            if payload["method"] == "getBlock":
                envelope["result"] = {key: value for key, value in anchor.items() if key != "signatures"}
            return envelope

        scenario.transform = anchor_response
        if case == "incomplete":
            scenario.program_failure = wallet.TOKEN_2022_PROGRAM_ID
        elif case == "contradictory":
            scenario.explicit_override[transaction.ACCOUNT] = None
        elif case == "different_contexts":
            scenario.program_context[wallet.TOKEN_2022_PROGRAM_ID] = 101
            scenario.upper_slot = 102
        elif case == "native_absent":
            del scenario.accounts[transaction.WALLET]
        elif case == "native_zero":
            scenario.accounts[transaction.WALLET]["lamports"] = 0
        with PublicReadOnlyRpc(wallet.ENDPOINT, transaction.PROFILE, transport=httpx.MockTransport(scenario.handle)) as rpc:
            observed = WalletEvidenceAdapter(rpc, request, clock=transaction.utc).observe()
        port = ledger_account_evidence(observed, expected_wallet=transaction.WALLET, expected_genesis=transaction.GENESIS,
            expected_profile_fingerprint=transaction.PROFILE.fingerprint, required_min_context_slot=100, now_utc=transaction.utc())
        account_ports[case] = port
        observations.append(observed)
        ports.append(port)
        rpc_calls.extend(payload for payload, _ in scenario.calls)
    complete = account_ports["complete"]
    check("actual_account_RPC_to_Ledger_complete_supported_coherent_facts", complete.account_facts_usable
          and complete.assessment.inventory_coverage == "COMPLETE" and complete.assessment.coherent_context == "COHERENT"
          and complete.assessment.supported_shape == "SUPPORTED" and len(complete.assessment.tokens) == 2)
    check("missing_program_is_incomplete_and_unusable", account_ports["incomplete"].assessment.inventory_coverage == "INCOMPLETE"
          and not account_ports["incomplete"].account_facts_usable)
    check("complete_inventory_with_unsupported_shape_remains_unusable", account_ports["unsupported"].assessment.inventory_coverage == "COMPLETE"
          and account_ports["unsupported"].assessment.supported_shape == "UNSUPPORTED" and not account_ports["unsupported"].account_facts_usable)
    check("same_context_absence_inventory_conflict_is_contradictory", account_ports["contradictory"].assessment.inventory_coverage == "CONTRADICTORY"
          and not account_ports["contradictory"].account_facts_usable)
    check("complete_inventory_does_not_merge_differing_account_cuts", account_ports["different_contexts"].assessment.inventory_coverage == "COMPLETE"
          and account_ports["different_contexts"].assessment.coherent_context == "INCOHERENT"
          and not account_ports["different_contexts"].account_facts_usable)
    check("native_absent_and_present_zero_remain_distinct_facts", account_ports["native_absent"].assessment.native_account_presence == "ABSENT"
          and not account_ports["native_absent"].account_facts_usable and account_ports["native_zero"].account_facts_usable
          and account_ports["native_zero"].observation.explicit_read.accounts[0].lamports == 0)
    check("impossible_standalone_account_anchor_height_denies", not account_ports["invalid_anchor_height"].account_facts_usable
          and account_ports["invalid_anchor_height"].observation.anchor is None)

    transaction_ports = {}
    for case in ("legacy_finalized", "v0_finalized", "unknown", "contradictory"):
        scenario = transaction.Scenario(v0=case == "v0_finalized")
        if case == "unknown":
            scenario.tx = scenario.status = None
        elif case == "contradictory":
            scenario.status["slot"] += 1
        observed, port = transaction.run_tx(scenario)
        transaction_ports[case] = port
        observations.append(observed)
        ports.append(port)
        rpc_calls.extend(scenario.calls)
    landed = transaction_ports["legacy_finalized"].observation
    check("actual_exact_transaction_RPC_to_Ledger_supported_legacy_and_v0", all(
        transaction_ports[case].disposition == "SUPPORTED_FINALIZED_OBSERVATION" for case in ("legacy_finalized", "v0_finalized"))
        and landed.transaction.fee_lamports == 5000 and landed.transaction.primary_signature == landed.request.signature)
    check("null_exact_lookup_is_unknown_not_nonlanding", transaction_ports["unknown"].disposition == "UNKNOWN")
    check("conflicting_exact_transaction_facts_are_contradictory", transaction_ports["contradictory"].disposition == "CONTRADICTORY")
    check("account_and_transaction_ports_retain_same_public_domain", complete.observation.request.wallet == transaction.WALLET
          and complete.observation.request.genesis_hash == landed.request.genesis_hash
          and complete.observation.profile == landed.profile and complete.observation.anchor == landed.root)
    same_transaction = transaction.order(landed, landed)
    check("same_transaction_order_requires_future_event_proof", same_transaction.relation == "UNKNOWN"
          and same_transaction.reasons == ("EVENT_ORDER_REQUIRED_WITHIN_SAME_TRANSACTION",))

    coverage_scenario = transaction.coverage_scenario()
    coverage_request = transaction.coverage_request(signature=transaction.sig(99), upper=92, last_valid=88)
    interval, interval_port = transaction.run_coverage(coverage_scenario, request=coverage_request)
    check("complete_chosen_interval_preserves_missing_validity_tail", interval_port.disposition == "COMPLETE_REQUESTED_INTERVAL"
          and interval_port.scope == SCOPE and interval_port.chosen_upper_block_height == 87
          and interval_port.chosen_upper_block_height < interval.request.last_valid_block_height < interval_port.fresh_root_block_height
          and not interval_port.signature_occurrences and interval.request == coverage_request)
    pruned_scenario = transaction.coverage_scenario()
    del pruned_scenario.blocks[92]
    pruned, pruned_port = transaction.run_coverage(pruned_scenario)
    check("missing_canonical_link_is_insufficient_even_with_positive_occurrence", pruned_port.disposition == "INSUFFICIENT_COVERAGE"
          and len(pruned_port.signature_occurrences) == 1)
    observations.extend((interval, pruned))
    ports.extend((interval_port, pruned_port, same_transaction))
    rpc_calls.extend(coverage_scenario.calls + pruned_scenario.calls)

    # Directly conflicting retained root/parent facts must deny both Ledger
    # ports; a parent's skipped interval and produced height are not unknown.
    for case, parent, height, previous_hash in (
        ("direct_parent_hash", 95, 89, transaction.bh(55)),
        ("direct_parent_height", 95, 90, transaction.bh(95)),
        ("skips_known_produced_block", 94, 89, transaction.bh(94)),
        ("intermediate_parent_needs_two_heights", 96, 89, transaction.bh(96)),
        ("skipped_slots_cannot_add_height", 96, 91, transaction.bh(96)),
    ):
        root = transaction.block(100, parent, height)
        root["previousBlockhash"] = previous_hash
        tx_scenario = transaction.Scenario(slot=95)
        tx_scenario.blocks[95] = transaction.block(95, 92, 88, [tx_scenario.signature], time=None)
        tx_scenario.tx["blockTime"] = None
        tx_scenario.blocks[100] = root
        observed, port = transaction.run_tx(tx_scenario)
        coverage_scenario = transaction.coverage_scenario()
        coverage_scenario.blocks[100] = root
        coverage, coverage_port = transaction.run_coverage(coverage_scenario, request=transaction.coverage_request(upper=95))
        check(f"known_anchor_{case}_denies_both_Ledger_ports", port.disposition == "CONTRADICTORY"
              and "TRANSACTION_BLOCK_AND_FINALIZED_ROOT_CONTRADICTION" in port.reasons
              and coverage_port.disposition == "CONTRADICTORY" and "UPPER_AND_CURRENT_ROOT_CONTRADICTION" in coverage_port.reasons)
        observations.extend((observed, coverage))
        ports.extend((port, coverage_port))

    # The Runtime comparison must also catch conflicts appearing only when two
    # individually supported transaction observations are consumed together.
    for case, slot, parent, height, root_height in (
        ("direct_parent_hash", 98, 95, 89, 91),
        ("skips_known_produced_block", 96, 94, 89, 91),
        ("intermediate_parent_needs_two_heights", 98, 96, 89, 91),
        ("skipped_slots_cannot_add_height", 98, 96, 91, 93),
    ):
        left_scenario, right_scenario = transaction.Scenario(slot=95), transaction.Scenario(2, slot=slot)
        left_scenario.blocks[95] = transaction.block(95, 92, 88, [left_scenario.signature], time=None)
        right_scenario.blocks[slot] = transaction.block(slot, parent, height, [right_scenario.signature], time=None)
        if case == "direct_parent_hash":
            right_scenario.blocks[slot]["previousBlockhash"] = transaction.bh(55)
        for scenario in (left_scenario, right_scenario):
            scenario.tx["blockTime"] = None
            scenario.blocks[100] = transaction.block(100, 99, root_height)
        left, left_port = transaction.run_tx(left_scenario)
        right, right_port = transaction.run_tx(right_scenario)
        order = transaction.order(left, right)
        check(f"cross_observation_{case}_makes_Runtime_order_unknown", left_port.disposition == right_port.disposition
              == "SUPPORTED_FINALIZED_OBSERVATION" and order.relation == "UNKNOWN"
              and order.reasons == ("KNOWN_FINALIZED_ANCHOR_CONTRADICTION",))
        observations.extend((left, right))
        ports.append(order)
    # Positive skipped-slot parent proof with absent historical times still
    # supports exact facts and canonical transaction ordering.
    left_scenario, right_scenario = transaction.Scenario(slot=95), transaction.Scenario(2, slot=98)
    left_scenario.blocks[95] = transaction.block(95, 92, 88, [left_scenario.signature], time=None)
    right_scenario.blocks[98] = transaction.block(98, 95, 89, [right_scenario.signature], time=None)
    for scenario in (left_scenario, right_scenario):
        scenario.tx["blockTime"] = None
        scenario.blocks[100] = transaction.block(100, 98, 90)
    left, left_port = transaction.run_tx(left_scenario)
    right, right_port = transaction.run_tx(right_scenario)
    check("consistent_skipped_slot_parent_with_optional_historical_time_orders", left_port.disposition == right_port.disposition
          == "SUPPORTED_FINALIZED_OBSERVATION" and transaction.order(left, right).relation == "BEFORE"
          and transaction.order(right, left).relation == "AFTER")

    for case in ("root", "membership"):
        scenario = transaction.Scenario()
        slot = 100 if case == "root" else 10
        scenario.blocks[slot] = transaction.block(slot, 1, 10, [scenario.signature], time=transaction.NOW-10000)
        observed, port = transaction.run_tx(scenario)
        check(f"impossible_standalone_{case}_height_denies_transaction_port", port.disposition != "SUPPORTED_FINALIZED_OBSERVATION"
              and (observed.root is None if case == "root" else observed.membership_block is None))
    genesis_scenario = transaction.Scenario()
    genesis_scenario.blocks[0] = transaction.block(0, 0, 0)
    with PublicReadOnlyRpc(transaction.ENDPOINT, transaction.PROFILE, transport=httpx.MockTransport(genesis_scenario.handle)) as rpc:
        anchor = rpc.get_finalized_block_anchor(0)
        block = rpc.get_finalized_signature_block(0)
    check("zero_height_genesis_remains_supported", anchor.block_height == block.block_height == 0)
    for case, slot, parent, height in (("genesis_parent", 0, 1, 0), ("non_genesis_zero_height", 10, 9, 0)):
        scenario = transaction.Scenario()
        scenario.blocks[slot] = transaction.block(slot, parent, height)
        with PublicReadOnlyRpc(transaction.ENDPOINT, transaction.PROFILE, transport=httpx.MockTransport(scenario.handle)) as rpc:
            check(f"invalid_{case}_denied_by_both_actual_block_reads", source.raises(PublicRpcError,
                  lambda: rpc.get_finalized_block_anchor(slot)) and source.raises(PublicRpcError,
                  lambda: rpc.get_finalized_signature_block(slot)))

    check("Evidence_and_downstream_ports_are_immutable_and_sanitized", "DO_NOT_RETAIN" not in repr((observations, ports))
          and all(source.raises(FrozenInstanceError, lambda item=item: setattr(item, "schema", "changed"))
                  for item in (good, complete.observation, landed))
          and source.raises(FrozenInstanceError, lambda: setattr(landed.transaction, "fee_lamports", 0)))
    expected_methods = {"getGenesisHash", "getSlot", "getBlock", "getMultipleAccounts", "getTokenAccountsByOwner", "getSignatureStatuses", "getTransaction"}
    public_rpc_methods = {name for name in dir(PublicReadOnlyRpc) if not name.startswith("_") and callable(getattr(PublicReadOnlyRpc, name))}
    check("composed_RPC_uses_only_seven_wire_reads_and_no_mutation_capability", set(READ_ONLY_METHODS)
          == {call["method"] for call in rpc_calls} == expected_methods and public_rpc_methods == {
              "close", "get_genesis_hash", "get_finalized_slot", "get_finalized_block_anchor", "get_multiple_accounts",
              "get_token_accounts_by_owner", "get_signature_status", "get_finalized_transaction", "get_finalized_signature_block"})
    print(json.dumps({"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "check_count": len(CHECKS), "checks": CHECKS,
        "failed": [name for name, result in CHECKS.items() if not result], "transport": "HTTPX_MOCK_ONLY",
        "data": "TEMPORARY_SYNTHETIC_SQLITE_ONLY", "real_network_requests": 0,
        "consumer_scope": "EXISTING_EVIDENCE_PORTS_ONLY_REAL_AUTHORITY_LEDGER_RUNTIME_PENDING"}, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
