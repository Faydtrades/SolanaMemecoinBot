"""L3 deterministic MockTransport -> Ledger finality receipts. No signing/send."""
from __future__ import annotations

import base64
import copy
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import httpx
from solders.message import Message, to_bytes_versioned
from solders.hash import Hash
from solders.instruction import CompiledInstruction
from solders.pubkey import Pubkey
from solders.system_program import advance_nonce_account
from solders.transaction import VersionedTransaction

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from phase5.shadow_unsigned_plan_simulation_v0_1 import BlockhashLeaseV01
from live.ledger_repository_v0_1 import LedgerRepository, LedgerConflict, LedgerJournalError, _DDL
from live.ledger_evidence_codec_v0_1 import LedgerEvidenceCodecError
from live.ledger_domain_v0_1 import LedgerContractError
from live.ledger_chain_codec_v0_1 import chain_observation_from_json, chain_observation_to_json
from live.public_rpc_v0_1 import PublicReadOnlyRpc
from live.transaction_evidence_v0_1 import TransactionRequest, TransactionEvidenceAdapter
from live.transaction_coverage_v0_1 import CoverageRequest, CoverageLimits, CanonicalCoverageAdapter
from live_ledger_actions_selftest_v0_1 import candidate, action_for, preparation, advance, cancel
from live_ledger_baseline_selftest_v0_1 import domain, observe, ingest
from live_wallet_evidence_selftest_v0_1 import GENESIS, BLOCK, PARENT, NOW, PROFILE, ENDPOINT, WALLET, TOKEN, utc
from live_transaction_evidence_selftest_v0_1 import Scenario, block, sig, bh, RECENT

CHECKS = {}


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


def scenario(*, failed=False, absent=False, v0=False):
    value = Scenario(49, slot=108, failed=failed, v0=v0)
    value.genesis = GENESIS
    value.tx["blockTime"] = NOW+5
    value.initial_slot = value.root_slot = value.status_context = 114
    value.blocks = {
        101: {"blockhash": BLOCK, "previousBlockhash": PARENT, "parentSlot": 100,
              "blockHeight": 90, "blockTime": NOW-20, "signatures": []},
        104: block(104, 101, 91, time=NOW+1),
        108: block(108, 104, 92, [] if absent else [sig(49)], time=NOW+5),
        110: block(110, 108, 93, time=NOW+7),
        114: block(114, 110, 94, time=NOW+8),
    }
    value.blocks[104]["previousBlockhash"] = BLOCK
    return value


def prepared(action=None, *, v0=False, validity_profile="RECENT_BLOCKHASH", nonce=False):
    action = action or action_for(candidate())
    wire = base64.b64decode(scenario(v0=v0).tx["transaction"][0])
    message = to_bytes_versioned(VersionedTransaction.from_bytes(wire).message)
    if nonce:
        ix = advance_nonce_account({"nonce_pubkey": Pubkey.from_string(TOKEN), "authorized_pubkey": Pubkey.from_string(WALLET)})
        keys = [Pubkey.from_string(WALLET), ix.accounts[0].pubkey, ix.accounts[1].pubkey, ix.program_id]
        message = to_bytes_versioned(Message.new_with_compiled_instructions(1, 0, 2, keys, Hash.from_string(RECENT),
            [CompiledInstruction(3, ix.data, bytes((1, 2, 0)))]))
    return replace(preparation(action), message_hex=message.hex(),
        lease=BlockhashLeaseV01("EXTERNAL_PLAN_REFERENCE", RECENT, 102, 93, "confirmed", NOW*1_000_000),
        validity_profile=validity_profile)


def setup(path, *, v0=False, validity_profile="RECENT_BLOCKHASH", nonce=False):
    repo = LedgerRepository.initialize(path, domain())
    ingest(repo, observe())
    item = candidate()
    repo.receive_candidate(item, fence=repo.write_fence())
    action = action_for(item)
    repo.stage_action(action, fence=repo.write_fence())
    prep = prepared(action, v0=v0, validity_profile=validity_profile, nonce=nonce)
    repo.prepare_attempt(prep, fence=repo.write_fence())
    advance(repo, prep, "EXACT_SIMULATED", at=NOW+2)
    advance(repo, prep, "AUTHORIZED", at=NOW+3)
    advance(repo, prep, "SIGNED_DURABLE", at=NOW+4)
    return repo, prep


def transaction_observation(value=None, *, at=NOW+10, request=None, profile=PROFILE):
    value = value or scenario()
    request = request or TransactionRequest(GENESIS, sig(49), 101)
    with PublicReadOnlyRpc(ENDPOINT, profile, transport=httpx.MockTransport(value.handle)) as rpc:
        return TransactionEvidenceAdapter(rpc, request, clock=lambda: utc(at)).observe()


def coverage_observation(value=None, *, at=NOW+10, request=None, profile=PROFILE, limits=CoverageLimits()):
    value = value or scenario(absent=True)
    prep = prepared()
    request = request or CoverageRequest(GENESIS, sig(49), RECENT, 93, prep.finalized_lower_anchor)
    with PublicReadOnlyRpc(ENDPOINT, profile, transport=httpx.MockTransport(value.handle)) as rpc:
        return CanonicalCoverageAdapter(rpc, request, limits=limits, clock=lambda: utc(at)).observe()


def receive(repo, prep, observation=None, *, key="chain", at=NOW+10, fence=None):
    return repo.ingest_chain_observation(prep.attempt_id, observation or transaction_observation(), ingestion_key=key,
        evaluated_at_utc=utc(at), fence=repo.write_fence() if fence is None else fence)


def child(mode, path):
    if mode == "--compete":
        try:
            with LedgerRepository.reopen(path, domain()):
                return 99
        except LedgerConflict:
            return 93
    with LedgerRepository.reopen(path, domain()) as repo:
        original = repo._commit

        def crash():
            if mode == "--before":
                os._exit(91)
            original()
            os._exit(92)

        repo._commit = crash
        receive(repo, prepared())
    return 99


def run_child(mode, path):
    return subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), mode, str(path)],
                          cwd=ROOT, capture_output=True, text=True, timeout=30)


def main():
    with tempfile.TemporaryDirectory(prefix="live-ledger-finality-") as temporary:
        directory = Path(temporary)
        path = directory / "positive.sqlite3"
        repo, prep = setup(path)
        original_observation = transaction_observation()
        with repo:
            original_fence = repo.write_fence()
            receipt = receive(repo, prep, original_observation)
            state = receipt.decision.resulting_state
            check("actual_Transaction_adapter_original_Evidence_establishes_finality", state.disposition == "FINALIZED_SUCCESS_UNAPPLIED"
                  and state.positive_finality == "FINALIZED_SUCCESS" and receipt.observation == original_observation)
            check("finality_is_no_economic_application_release_or_retry", not state.economically_applied
                  and not state.may_retry and not state.may_release_economic_lane and not state.has_real_authority_grant
                  and repo.mutation_lane()["held"] and repo.attempt(prep.attempt_id).chain_finality == state.disposition)
            check("full_original_wire_lease_lower_and_floor_bound", receipt.observation.transaction.wire_bytes == repo._signed_lineage(prep.attempt_id)[0]
                  and receipt.decision.expected_request == TransactionRequest(GENESIS, sig(49), 101)
                  and receipt.decision.required_min_finalized_root_slot == 101)
            check("exact_chain_retry_converges_despite_stale_revision", receive(repo, prep, original_observation, fence=original_fence) == receipt
                  and repo.audit()["chain_receipt_count"] == 1)
            check("same_original_evidence_new_key_rejected", raises(LedgerConflict,
                  lambda: receive(repo, prep, original_observation, key="other")))
            check("same_key_changed_original_evaluation_rejected", raises(LedgerConflict,
                  lambda: receive(repo, prep, original_observation, at=NOW+11)))
            changed = scenario(failed=True)
            check("same_key_changed_original_content_rejected", raises(LedgerConflict,
                  lambda: receive(repo, prep, transaction_observation(changed))))
            check("finalized_attempt_cannot_resume_external_stage_or_local_cancel", raises(LedgerConflict,
                  lambda: advance(repo, prep, "SEND_CLAIMED", at=NOW+11)) and raises(LedgerConflict, lambda: cancel(repo, prep, at=NOW+11)))
            null = scenario(); null.tx = null.status = None
            unknown = receive(repo, prep, transaction_observation(null, at=NOW+11), key="unknown", at=NOW+11)
            check("later_unknown_retains_original_positive_proof", unknown.decision.observation_disposition == "UNKNOWN"
                  and unknown.decision.resulting_state.positive_finality == "FINALIZED_SUCCESS"
                  and unknown.decision.resulting_state.proof_evidence_digest == original_observation.content_digest)
            stale = receive(repo, prep, transaction_observation(at=NOW+12), key="stale", at=NOW+60)
            check("new_stale_Evidence_cannot_erase_positive_finality", stale.decision.observation_disposition == "UNKNOWN"
                  and stale.decision.resulting_state.positive_finality == "FINALIZED_SUCCESS"
                  and "OBSERVATION_STALE_OR_FUTURE" in stale.decision.reasons)
            positive_digest = content_fingerprint(receipt.decision.to_record())
            check("competing_process_cannot_mutate_chain_journal", run_child("--compete", path).returncode == 93)
        with LedgerRepository.reopen(path, domain()) as repo:
            check("historical_receipts_rederive_original_time_after_reopen", repo.chain_receipt("chain") == receipt
                  and receive(repo, prep, original_observation) == receipt and repo.audit()["chain_receipt_count"] == 3)
            check("old_writer_generation_cannot_even_retry_chain_ingestion", raises(LedgerConflict,
                  lambda: receive(repo, prep, original_observation, fence=original_fence)))
            altered = scenario(); altered.tx["meta"]["fee"] += 1
            conflict = receive(repo, prep, transaction_observation(altered, at=NOW+13), key="metadata", at=NOW+61)
            check("changed_retained_exact_metadata_quarantines_without_erasing_positive", conflict.decision.resulting_state.quarantined
                  and conflict.decision.resulting_state.positive_finality == "FINALIZED_SUCCESS")
        with LedgerRepository.reopen(path, domain()) as repo:
            check("quarantine_positive_proof_and_lane_reconstruct_together", repo.attempt_finality(prep.attempt_id).quarantined
                  and repo.attempt_finality(prep.attempt_id).positive_finality == "FINALIZED_SUCCESS" and repo.mutation_lane()["held"])

        for name, value, expected in (
            ("failed", scenario(failed=True), "FINALIZED_FAILURE_UNAPPLIED"),
            ("null-status", scenario(), "FINALIZED_SUCCESS_UNAPPLIED"),
            ("v0", scenario(v0=True), "FINALIZED_SUCCESS_UNAPPLIED"),
        ):
            if name == "null-status":
                value.status = None
            local, p = setup(directory / f"{name}.sqlite3", v0=name == "v0")
            with local:
                result = receive(local, p, transaction_observation(value))
                check(f"{name}_exact_metadata_finality", result.decision.resulting_state.disposition == expected
                      and result.observation.transaction.fee_lamports == 5000 and local.audit()["chain_receipt_count"] == 1)
                if name == "v0":
                    check("v0_loaded_accounts_inner_instructions_exact_codec", chain_observation_from_json(chain_observation_to_json(result.observation)) == result.observation
                          and bool(result.observation.transaction.lookups) and bool(result.observation.transaction.inner_instructions))

        local, p = setup(directory / "provisional.sqlite3")
        with local:
            value = scenario(); value.tx = None; value.status.update(confirmationStatus="confirmed", confirmations=2)
            first = receive(local, p, transaction_observation(value))
            check("confirmed_without_exact_metadata_is_provisional_and_held", first.decision.resulting_state.disposition == "CONFIRMED_PROVISIONAL"
                  and first.decision.resulting_state.positive_finality is None and local.mutation_lane()["held"])
            value = scenario(); value.tx = value.status = None
            second = receive(local, p, transaction_observation(value, at=NOW+11), key="missing", at=NOW+11)
            check("provisional_to_unknown_preserves_historical_flag", second.decision.resulting_state.disposition == "UNKNOWN"
                  and second.decision.resulting_state.provisional_seen and local.audit()["chain_receipt_count"] == 2)
            proved = receive(local, p, coverage_observation(at=NOW+12), key="expired", at=NOW+12)
            check("ordinary_confirmed_fork_can_be_canonically_absent", proved.decision.resulting_state.disposition == "PROVEN_NON_LANDED")

        deny_cases = []
        value = scenario(); value.tx = value.status = None
        deny_cases.append(("null-status-and-tx", transaction_observation(value), "UNKNOWN"))
        value = scenario(); value.tx = None
        deny_cases.append(("finalized-status-only", transaction_observation(value), "UNKNOWN"))
        value = scenario(); value.blocks.pop(108)
        deny_cases.append(("pruned-membership", transaction_observation(value), "UNKNOWN"))
        value = scenario(); value.tx["meta"].pop("innerInstructions")
        deny_cases.append(("incomplete-exact-metadata", transaction_observation(value), "UNKNOWN"))
        value = scenario(); value.genesis = bh(200)
        deny_cases.append(("foreign-genesis", transaction_observation(value), "QUARANTINED"))
        deny_cases.append(("foreign-profile", transaction_observation(profile=replace(PROFILE, provider_version="2")), "QUARANTINED"))
        deny_cases.append(("wrong-request-signature", transaction_observation(request=TransactionRequest(GENESIS, sig(50), 101)), "QUARANTINED"))
        deny_cases.append(("request-floor-changed", transaction_observation(request=TransactionRequest(GENESIS, sig(49), 100)), "QUARANTINED"))
        value = scenario(); value.tx["transaction"][0] = base64.b64encode(
            base64.b64decode(value.tx["transaction"][0]).replace(bytes([201])*32, bytes([202])*32)).decode()
        deny_cases.append(("changed-message-or-wire", transaction_observation(value), "QUARANTINED"))
        value = scenario(); value.blocks[108]["blockHeight"] = 93
        deny_cases.append(("impossible-finalized-anchor-height", transaction_observation(value), "QUARANTINED"))
        value = scenario(); value.tx["slot"] = value.status["slot"] = 101; value.tx["blockTime"] = NOW-20; value.blocks[101]["signatures"] = [sig(49)]
        deny_cases.append(("transaction-already-in-custody-cut", transaction_observation(value), "QUARANTINED"))
        value = scenario(); value.tx["slot"] = value.status["slot"] = 114; value.tx["blockTime"] = NOW+8
        value.blocks[108]["signatures"] = []; value.blocks[114]["signatures"] = [sig(49)]
        deny_cases.append(("recent-hash-transaction-after-original-expiry", transaction_observation(value), "QUARANTINED"))
        value = scenario(); value.tx = None; value.status["slot"] = 114; value.root_slot = value.initial_slot = 110
        deny_cases.append(("finalized-status-beyond-later-root", transaction_observation(value), "QUARANTINED"))
        value = scenario(); value.tx = value.status = None; value.blocks[100] = block(100, 99, 89); value.root_slot = value.initial_slot = 100
        deny_cases.append(("missing-tx-root-below-original-floor", transaction_observation(value), "UNKNOWN"))
        value = scenario(); value.tx = value.status = None; value.blocks[100] = block(100, 99, 89); value.root_slot = value.initial_slot = 100
        lower = transaction_observation(value, request=TransactionRequest(GENESIS, sig(49), 100))
        deny_cases.append(("hostile-qualified-root-below-floor", replace(lower, request=TransactionRequest(GENESIS, sig(49), 101)), "QUARANTINED"))
        def timeout(request):
            raise httpx.ReadTimeout("DO_NOT_RETAIN")
        value = scenario(); value.handle = timeout
        deny_cases.append(("timeout", transaction_observation(value), "UNKNOWN"))
        for name, observation, expected in deny_cases:
            local, p = setup(directory / f"deny-{name}.sqlite3")
            with local:
                result = receive(local, p, observation)
                check(f"{name}_durable_{expected}", result.decision.resulting_state.disposition == expected
                      and result.decision.resulting_state.positive_finality is None and local.mutation_lane()["held"]
                      and "DO_NOT_RETAIN" not in repr(result) and local.audit()["chain_receipt_count"] == 1)

        for history in ("finalized-status", "finalized-status-slot", "exact-metadata", "status-versus-exact"):
            history_path = directory / f"history-{history}.sqlite3"
            local, p = setup(history_path)
            with local:
                first = scenario()
                if history in ("finalized-status", "finalized-status-slot", "status-versus-exact"):
                    first.tx = None
                else:
                    first.status = None; first.blocks.pop(108)
                receive(local, p, transaction_observation(first))
            with LedgerRepository.reopen(history_path, domain()) as local:
                second = scenario(failed=history != "exact-metadata")
                if history in ("finalized-status", "finalized-status-slot"):
                    second.tx = None
                if history == "finalized-status-slot":
                    second.status["slot"] = 110
                if history == "exact-metadata":
                    second.tx["meta"]["fee"] = 6000
                result = receive(local, p, transaction_observation(second, at=NOW+11), key="later", at=NOW+11)
                check(f"retained_incomplete_{history}_claim_conflict_quarantined", result.decision.resulting_state.quarantined
                      and local.audit()["chain_receipt_count"] == 2)

        enrichment_path = directory / "enrichment.sqlite3"
        local, p = setup(enrichment_path)
        with local:
            missing = scenario(); missing.tx["blockTime"] = None; missing.blocks[108]["blockTime"] = None
            missing.tx["meta"]["innerInstructions"][0]["instructions"][0]["stackHeight"] = None
            original = receive(local, p, transaction_observation(missing))
            enriched = receive(local, p, transaction_observation(at=NOW+11), key="enriched", at=NOW+11)
            state = enriched.decision.resulting_state
            check("nullable_metadata_enrichment_preserves_original_positive_proof", not state.quarantined
                  and state.positive_finality == "FINALIZED_SUCCESS"
                  and state.proof_evidence_digest == original.observation.content_digest
                  and state.exact_transaction_digest == original.decision.resulting_state.exact_transaction_digest
                  and state.latest_compatible_transaction_evidence_digest == enriched.observation.content_digest)
            missing = scenario(); missing.tx["blockTime"] = None; missing.blocks[108]["blockTime"] = None
            missing.tx["meta"]["innerInstructions"][0]["instructions"][0]["stackHeight"] = None
            less = receive(local, p, transaction_observation(missing, at=NOW+12), key="less", at=NOW+12)
            check("later_nullable_unknown_does_not_downgrade_selected_exact_metadata", not less.decision.resulting_state.quarantined
                  and less.decision.resulting_state.latest_compatible_transaction_evidence_digest == enriched.observation.content_digest)
        with LedgerRepository.reopen(enrichment_path, domain()) as local:
            check("compatible_enrichment_replays_original_and_selected_observations", local.attempt_finality(p.attempt_id) == less.decision.resulting_state
                  and local.chain_receipt("enriched") == enriched and local.audit()["chain_receipt_count"] == 3)
        for nullable in ("block-time", "stack-height"):
            local, p = setup(directory / f"known-conflict-{nullable}.sqlite3")
            with local:
                receive(local, p)
                different = scenario()
                if nullable == "block-time":
                    different.tx["blockTime"] = different.blocks[108]["blockTime"] = NOW+6
                else:
                    different.tx["meta"]["innerInstructions"][0]["instructions"][0]["stackHeight"] = 3
                conflict = receive(local, p, transaction_observation(different, at=NOW+11), key="different", at=NOW+11)
                check(f"different_known_{nullable}_quarantines", conflict.decision.resulting_state.quarantined
                      and conflict.decision.resulting_state.positive_finality == "FINALIZED_SUCCESS")

        local, p = setup(directory / "u64.sqlite3")
        with local:
            maximum = scenario()
            maximum.tx["meta"]["fee"] = (1 << 64)-1
            maximum.tx["meta"]["preBalances"][0] = (1 << 64)-1
            maximum.tx["meta"]["preTokenBalances"][0]["uiTokenAmount"]["amount"] = str((1 << 64)-1)
            receipt = receive(local, p, transaction_observation(maximum))
            restored = local.chain_receipt("chain")
            check("complete_u64_chain_money_and_aggregate_preserved_exactly", restored == receipt
                  and restored.observation.transaction.fee_lamports == (1 << 64)-1
                  and restored.observation.transaction.pre_tokens[0].amount == (1 << 64)-1
                  and sum(restored.observation.transaction.pre_lamports) > (1 << 64)-1
                  and local.audit()["chain_receipt_count"] == 1)

        local, p = setup(directory / "initial-stale.sqlite3")
        with local:
            stale = receive(local, p, at=NOW+60)
            check("stale_NEW_exact_transaction_is_durable_unknown_not_positive", stale.decision.resulting_state.disposition == "UNKNOWN"
                  and stale.decision.resulting_state.positive_finality is None and local.mutation_lane()["held"])

        import live.ledger_repository_v0_1 as storage_module
        local, p = setup(directory / "incremental-chain.sqlite3")
        with local:
            receive(local, p)
            original_decode = storage_module.chain_observation_from_json
            calls = []
            def count_decode(payload):
                calls.append(payload)
                return original_decode(payload)
            storage_module.chain_observation_from_json = count_decode
            try:
                value = scenario(); value.tx = value.status = None
                receive(local, p, transaction_observation(value, at=NOW+11), key="new", at=NOW+11)
                check("new_chain_append_decodes_only_new_original_Evidence", len(calls) == 1)
            finally:
                storage_module.chain_observation_from_json = original_decode
            check("full_audit_still_rederives_all_original_chain_receipts", local.audit()["chain_receipt_count"] == 2)

        nonlanding_path = directory / "nonlanding.sqlite3"
        local, p = setup(nonlanding_path)
        with local:
            result = receive(local, p, coverage_observation())
            check("complete_exact_validity_interval_and_fresh_expired_root_proves_nonlanding", result.decision.resulting_state.disposition == "PROVEN_NON_LANDED"
                  and result.observation.request.original_lower_anchor == p.finalized_lower_anchor
                  and local.mutation_lane()["held"] and not result.decision.resulting_state.may_retry)
            nonlanding_digest = content_fingerprint(result.decision.to_record())
        with LedgerRepository.reopen(nonlanding_path, domain()) as local:
            check("nonlanding_proof_replays_without_economic_release", local.attempt_finality(p.attempt_id).positive_finality == "PROVEN_NON_LANDED"
                  and local.mutation_lane()["held"])
            landed = receive(local, p, transaction_observation(at=NOW+11), key="contradiction", at=NOW+11)
            check("positive_landing_after_absence_proof_is_quarantined", landed.decision.resulting_state.quarantined
                  and landed.decision.resulting_state.positive_finality == "PROVEN_NON_LANDED")

        coverage_cases = []
        request = CoverageRequest(GENESIS, sig(49), RECENT, 93, prepared().finalized_lower_anchor)
        coverage_cases.append(("missing-tail", coverage_observation(request=replace(request, upper_slot=108)), "UNKNOWN"))
        value = scenario(absent=True); value.root_slot = value.initial_slot = 110
        coverage_cases.append(("root-at-expiry", coverage_observation(value), "UNKNOWN"))
        coverage_cases.append(("positive-occurrence-no-outcome", coverage_observation(scenario()), "UNKNOWN"))
        value = scenario(absent=True); value.blocks.pop(104)
        coverage_cases.append(("missing-block", coverage_observation(value), "UNKNOWN"))
        coverage_cases.append(("coverage-budget-exhausted", coverage_observation(limits=CoverageLimits(max_blocks=2)), "UNKNOWN"))
        coverage_cases.append(("wrong-original-recent-hash", coverage_observation(request=replace(request, recent_blockhash=bh(99))), "QUARANTINED"))
        coverage_cases.append(("wrong-original-last-valid", coverage_observation(request=replace(request, last_valid_block_height=92)), "QUARANTINED"))
        coverage_cases.append(("wrong-original-lower", coverage_observation(request=replace(request, original_lower_anchor=replace(request.original_lower_anchor, blockhash=bh(99)))), "QUARANTINED"))
        for name, observation, expected in coverage_cases:
            local, p = setup(directory / f"coverage-{name}.sqlite3")
            with local:
                result = receive(local, p, observation)
                check(f"{name}_cannot_prove_nonlanding", result.decision.resulting_state.disposition == expected
                      and result.decision.resulting_state.positive_finality is None and local.audit()["chain_receipt_count"] == 1)
        for profile in ("DURABLE_NONCE", "UNKNOWN"):
            local, p = setup(directory / f"validity-{profile}.sqlite3", validity_profile=profile)
            with local:
                result = receive(local, p, coverage_observation())
                check(f"{profile}_validity_never_receives_recent_hash_expiry", result.decision.resulting_state.disposition == "UNKNOWN"
                      and "EXPIRY_REQUIRES_SUPPORTED_RECENT_BLOCKHASH_PROFILE" in result.decision.reasons)
        local, p = setup(directory / "decoded-nonce.sqlite3", nonce=True)
        with local:
            result = receive(local, p, coverage_observation())
            check("canonical_nonce_instruction_defeats_false_recent_hash_profile", p.validity_profile == "RECENT_BLOCKHASH"
                  and VersionedTransaction.from_bytes(local._signed_lineage(p.attempt_id)[0]).uses_durable_nonce()
                  and result.decision.resulting_state.disposition == "UNKNOWN" and local.mutation_lane()["held"])
        local, p = setup(directory / "exact-expiry-upper.sqlite3")
        with local:
            result = receive(local, p, coverage_observation(request=replace(request, upper_slot=110)))
            check("chosen_upper_exact_last_valid_with_later_root_is_sufficient", result.decision.resulting_state.disposition == "PROVEN_NON_LANDED"
                  and result.observation.blocks_descending[0].block_height == p.lease.last_valid_block_height)

        history_path = directory / "coverage-history.sqlite3"
        local, p = setup(history_path)
        with local:
            partial = scenario(); partial.blocks.pop(104)
            first = receive(local, p, coverage_observation(partial))
            check("partial_coverage_retains_positive_exact_signature_occurrence", first.decision.resulting_state.canonical_occurrence_seen
                  and first.decision.resulting_state.disposition == "UNKNOWN")
        with LedgerRepository.reopen(history_path, domain()) as local:
            later = receive(local, p, coverage_observation(at=NOW+11), key="later", at=NOW+11)
            check("later_absence_cannot_erase_retained_canonical_signature_vector", later.decision.resulting_state.quarantined
                  and later.decision.resulting_state.positive_finality is None
                  and "RETAINED_CANONICAL_SIGNATURE_VECTOR_CONFLICT" in later.decision.reasons and local.audit()["chain_receipt_count"] == 2)

        for observation in (transaction_observation(), transaction_observation(scenario(v0=True)), coverage_observation()):
            original = chain_observation_to_json(observation)
            kind = "coverage" if hasattr(observation, "blocks_descending") else observation.transaction.version
            check(f"finite_{kind}_codec_exact_roundtrip", chain_observation_from_json(original) == observation)
            for field, value in (("codec_version", "foreign"), ("evidence_model_id", "foreign"), ("kind", "foreign"), ("evidence_digest", "0"*64)):
                row = json.loads(original); row[field] = value
                check(f"finite_{kind}_codec_rejects_{field}", raises(LedgerEvidenceCodecError,
                      lambda row=row: chain_observation_from_json(canonical_json(row))))
            row = json.loads(original); row["observation"]["foreign"] = "DO_NOT_RETAIN"
            check(f"finite_{kind}_codec_rejects_extra_fields", raises(LedgerEvidenceCodecError, lambda: chain_observation_from_json(canonical_json(row))))
            check(f"finite_{kind}_codec_rejects_noncanonical_JSON", raises(LedgerEvidenceCodecError, lambda: chain_observation_from_json(original+" ")))
            check(f"finite_{kind}_codec_rejects_duplicate_keys", raises(LedgerEvidenceCodecError, lambda: chain_observation_from_json('{"kind":"x",'+original[1:])))
        raw = json.loads(chain_observation_to_json(transaction_observation()))
        for name, change in (("bad-base64", lambda r: r["observation"]["transaction"].update(wire_bytes="DO_NOT_RETAIN")),
                             ("float-money", lambda r: r["observation"]["transaction"].update(fee_lamports=1.0)),
                             ("bool-money", lambda r: r["observation"]["transaction"].update(fee_lamports=True)),
                             ("wrong-schema", lambda r: r["observation"].update(schema="foreign"))):
            row = copy.deepcopy(raw); change(row)
            check(f"chain_codec_hostile_{name}", raises(LedgerEvidenceCodecError, lambda row=row: chain_observation_from_json(canonical_json(row))))

        import live.ledger_chain_codec_v0_1 as codec_module
        local, p = setup(directory / "oversize.sqlite3")
        with local:
            fence = local.write_fence()
            bound = codec_module.MAX_CHAIN_PAYLOAD_BYTES
            codec_module.MAX_CHAIN_PAYLOAD_BYTES = 100
            try:
                check("oversize_original_Evidence_fails_without_state_or_lane_change", raises(LedgerEvidenceCodecError,
                      lambda: receive(local, p)) and local.write_fence() == fence and local.mutation_lane()["held"])
            finally:
                codec_module.MAX_CHAIN_PAYLOAD_BYTES = bound
            check("chain_budget_supports_declared_maximum_RPC_total", bound > 128*1024*1024)

        crash_results = {}
        for cut in ("before", "after"):
            crash_path = directory / f"crash-{cut}.sqlite3"
            local, p = setup(crash_path); local.close()
            result = run_child("--"+cut, crash_path)
            check(f"chain_{cut}_commit_child_reached_cut", result.returncode == (91 if cut == "before" else 92)
                  and not result.stdout and not result.stderr)
            with LedgerRepository.reopen(crash_path, domain()) as local:
                count = local.audit()["chain_receipt_count"]
                check(f"chain_{cut}_commit_original_and_receipt_atomic", count == (0 if cut == "before" else 1))
                receipt = receive(local, p)
                check(f"chain_{cut}_commit_exact_retry_converges", local.audit()["chain_receipt_count"] == 1
                      and local.mutation_lane()["held"])
                crash_results[cut] = {"child_exit": result.returncode, "recovered_count": count,
                    "decision_digest": content_fingerprint(receipt.decision.to_record()), "evidence_digest": receipt.observation.content_digest}
        check("both_commit_crashes_rederive_same_exact_original_decision", crash_results["before"]["decision_digest"] == crash_results["after"]["decision_digest"])

        local, p = setup(directory / "lost-acknowledgement.sqlite3")
        with local:
            original_commit = local._commit
            def lost_acknowledgement():
                original_commit()
                raise RuntimeError("TEST_ONLY_LOST_ACKNOWLEDGEMENT")
            local._commit = lost_acknowledgement
            check("successful_commit_with_lost_acknowledgement_reports_uncertainty", raises(LedgerJournalError, lambda: receive(local, p)))
            local._commit = original_commit
            check("lost_acknowledgement_keeps_finality_cache_with_trusted_head", local.attempt_finality(p.attempt_id).positive_finality == "FINALIZED_SUCCESS")
            check("exact_retry_after_lost_acknowledgement_converges", receive(local, p) == local.chain_receipt("chain")
                  and local.audit()["chain_receipt_count"] == 1)
            value = scenario(); value.tx = value.status = None
            result = receive(local, p, transaction_observation(value, at=NOW+11), key="later", at=NOW+11)
            check("append_after_lost_acknowledgement_cannot_erase_positive_proof", result.decision.resulting_state.positive_finality == "FINALIZED_SUCCESS"
                  and local.audit()["chain_receipt_count"] == 2)

        tamper_path = directory / "tamper.sqlite3"
        local, p = setup(tamper_path)
        with local:
            receive(local, p)
            for table in ("ledger_chain_observations", "ledger_chain_receipts"):
                check(f"{table}_append_only", raises(sqlite3.IntegrityError, lambda table=table: local._conn.execute(f"DELETE FROM {table}")))
        with closing(sqlite3.connect(tamper_path, isolation_level=None)) as other:
            other.execute("DROP TRIGGER ledger_chain_receipts_no_update")
            row = json.loads(other.execute("SELECT payload_json FROM ledger_chain_receipts").fetchone()[0])
            row["decision"]["resulting_state"]["may_retry"] = True
            other.execute("UPDATE ledger_chain_receipts SET payload_json=?", (canonical_json(row),))
            other.execute(_DDL["ledger_chain_receipts_no_update"])
        check("reopen_rejects_fabricated_finality_authority_receipt", raises(LedgerContractError,
              lambda: LedgerRepository.reopen(tamper_path, domain())))
    check("all_temporary_journals_and_child_artifacts_cleaned", not directory.exists())
    print(json.dumps({"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "check_count": len(CHECKS), "checks": CHECKS,
        "failed": [name for name, ok in CHECKS.items() if not ok], "positive_decision_digest": positive_digest,
        "nonlanding_decision_digest": nonlanding_digest, "crash_cuts": crash_results,
        "database_scope": "TEMPORARY_DIRECTORY_ONLY", "real_network_requests": 0,
        "private_key_or_signing_operations": 0}, sort_keys=True, indent=2))


if __name__ == "__main__":
    if len(sys.argv) == 3:
        raise SystemExit(child(sys.argv[1], Path(sys.argv[2])))
    main()
