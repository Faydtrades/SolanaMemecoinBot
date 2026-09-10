"""L5 original MockTransport Evidence -> atomic actual custody. Temp DBs only."""
from __future__ import annotations

import base64
import copy
import json
import os
import sqlite3
import struct
import subprocess
import sys
import tempfile
from dataclasses import FrozenInstanceError, asdict, replace
from pathlib import Path
from contextlib import closing

from solders.hash import Hash
from solders.instruction import CompiledInstruction
from solders.message import Message, to_bytes_versioned

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from phase5.shadow_unsigned_plan_simulation_v0_1 import BlockhashLeaseV01
from live.ledger_actions_v0_1 import AttemptPreparation, PendingAction, position_identity
from live.ledger_repository_v0_1 import LedgerRepository, LedgerConflict, LedgerJournalError, _support_json, _support_from_json
from live.ledger_settlement_v0_1 import WalletSupportInput, PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID, TOKEN_PROGRAM_ID, WSOL_MINT, SYSTEM_PROGRAM_ID
from live.wallet_evidence_v0_1 import WalletEvidenceRequest, ExpectedTokenAccount
from live.transaction_evidence_v0_1 import TransactionRequest, _b58data
from live.transaction_coverage_v0_1 import CoverageRequest
from live_ledger_actions_selftest_v0_1 import candidate, stage
from live_ledger_baseline_selftest_v0_1 import observe
from live_ledger_finality_selftest_v0_1 import scenario, transaction_observation, coverage_observation
from live_wallet_evidence_selftest_v0_1 import Scenario as WalletScenario, account, token_data, mint_data, GENESIS, PROFILE, NOW, utc, key
from live_transaction_evidence_selftest_v0_1 import block, sig, bh
import live_ledger_settlement_selftest_v0_1 as sf

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


def apply(repo, prep, support, *, chain="transaction", key="apply", at=NOW+20, fence=None):
    return repo.apply_settlement(prep.attempt_id, chain_receipt_key=chain, support=support, ingestion_key=key,
        recorded_at_utc=utc(at), fence=repo.write_fence() if fence is None else fence)


def composed_support(repo, fixture, *, root=None, at=NOW+14):
    """Actual public post-account fixture, including unchanged owned accounts."""
    state = repo._custody
    value = WalletScenario()
    value.accounts = {sf.WALLET: account(SYSTEM_PROGRAM_ID, lamports=fixture.scenario.tx["meta"]["postBalances"][0])}
    expected = {item.pubkey: (item.mint, item.program) for item in state.accounts}
    expected.update({fixture.base: (fixture.mint, fixture.token_program), fixture.quote: (WSOL_MINT, TOKEN_PROGRAM_ID)})
    # Only identities are read from Ledger. Economic carry-forward comes from
    # independently retained prior public RPC/transaction fixture facts.
    rows = dict(getattr(fixture, "owned_before", {}))
    metadata = fixture.scenario.tx["meta"]
    for key_ in (fixture.base, fixture.quote):
        rows.pop(key_, None)
    for row in metadata["postTokenBalances"]:
        if row["owner"] == sf.WALLET:
            key_ = fixture.keys[row["accountIndex"]]
            rows[key_] = (row["mint"], row["programId"], int(row["uiTokenAmount"]["amount"]),
                          metadata["postBalances"][row["accountIndex"]], sf.R if row["mint"] == WSOL_MINT else None)
    fixture.owned_after = dict(rows)
    for pubkey, (mint, program, units, lamports, reserve) in rows.items():
        value.add_token(token=pubkey, mint=mint, program=program,
            data=token_data(mint, sf.WALLET, units, reserve=reserve), mint_bytes=mint_data(9 if mint == WSOL_MINT else 6), lamports=lamports)
    for mint, program in expected.values():
        value.accounts[mint] = account(program, mint_data(9 if mint == WSOL_MINT else 6))
    if root is None:
        slot, height, parent, previous = 116, 95, 114, bh(114)
    else:
        slot, height, parent, previous = root.slot+2, root.block_height+1, root.slot, root.blockhash
    value.context = value.multiple_context = value.initial_slot = value.upper_slot = slot
    def transform(payload, envelope):
        if payload["method"] == "getBlock":
            envelope["result"].update(blockhash=bh(slot), previousBlockhash=previous, parentSlot=parent,
                                      blockHeight=height, blockTime=at-2)
        return envelope
    value.transform = transform
    floor = fixture.scenario.tx["slot"]
    request = WalletEvidenceRequest(sf.WALLET, GENESIS, floor,
        tuple(ExpectedTokenAccount(pubkey, mint, program) for pubkey, (mint, program) in sorted(expected.items())))
    observation = observe(value, request=request, at=at)
    return WalletSupportInput(observation, utc(at), floor), value, request


def first(path, *, venue="pump", failed=False, mutate=None, landing=True):
    fixture = sf.Fixture(venue, failed=failed)
    fixture.mint = sf.MINT
    if mutate is not None:
        mutate(fixture)
    tx, status = fixture.scenario.tx, fixture.scenario.status
    if not landing:
        fixture.scenario.tx = fixture.scenario.status = None
    repo, binding, action, prep, chain = fixture.setup(path)
    fixture.scenario.tx, fixture.scenario.status = tx, status
    support, value, request = composed_support(repo, fixture)
    return repo, binding, action, prep, chain, support, fixture


def next_step(repo, previous_fixture, lower, *, number, venue="swap", side="SELL", position_id=None,
              mint=sf.MINT, quantity=None, keep_wsol=False, failed=False, ingest_finality=True, existing_action=None, mutate=None, protective_handoff=None, action_ordinal=1):
    """One external public-message fixture from independent prior RPC balances."""
    before = dict(previous_fixture.owned_after)
    quote_key = previous_fixture.quote
    old_globals = sf.MINT, sf.plans.fx.MINT
    original_pool = sf.plans.fx.pool_data
    try:
        sf.MINT = sf.plans.fx.MINT = mint
        sf.plans.fx.pool_data = lambda **kwargs: original_pool(**{"base_mint": mint, **kwargs})
        fixture = sf.Fixture(venue, side, retained=quote_key in before, failed=failed)
    finally:
        sf.MINT, sf.plans.fx.MINT = old_globals
        sf.plans.fx.pool_data = original_pool
    fixture.mint, fixture.owned_before = mint, before
    meta = fixture.scenario.tx["meta"]
    deltas = [post-pre for pre, post in zip(meta["preBalances"], meta["postBalances"])]
    compiled = list(fixture.message.instructions)
    if side == "BUY" and fixture.base in before:
        # Canonical idempotent ATA call finds the prior independently observed
        # account. It funds no account and emits no create/init CPI group.
        for group in fixture.groups:
            ix = compiled[group["index"]]
            if (fixture.keys[ix.program_id_index] == sf.ASSOCIATED_TOKEN_PROGRAM_ID
                    and fixture.keys[ix.accounts[1]] == fixture.base):
                group["instructions"] = []
        deltas[0] += sf.R
        deltas[fixture.index[fixture.base]] -= sf.R
    quantity = fixture.units if quantity is None else quantity
    if side == "SELL":
        fixture.units = quantity
        fixture.actual_base = quantity
        outer = compiled[fixture.venue_index]
        compiled[fixture.venue_index] = CompiledInstruction(outer.program_id_index,
            outer.data[:8]+struct.pack("<Q", quantity)+outer.data[16:], outer.accounts)
        if not failed:
            for row in fixture.groups[-1]["instructions"]:
                data = _b58data(row["data"], 65536)
                if len(data) == 10 and data[0] == 12 and row["accounts"][0] == fixture.index[fixture.base]:
                    row["data"] = sf.b58(data[:1]+struct.pack("<Q", quantity)+data[9:])
                if data[:16] == sf.ANCHOR_EVENT_TAG+sf.PUMP_TRADE_EVENT_TAG:
                    row["data"] = sf.b58(data[:56]+struct.pack("<Q", quantity)+data[64:])
            pool_post = next(row for row in meta["postTokenBalances"] if row["accountIndex"] == fixture.index[fixture.pool_base])
            pool_pre = next(row for row in meta["preTokenBalances"] if row["accountIndex"] == fixture.index[fixture.pool_base])
            pool_post["uiTokenAmount"]["amount"] = str(int(pool_pre["uiTokenAmount"]["amount"])+quantity)
    if keep_wsol:
        assert side == "SELL" and fixture.quote not in before and not failed
        compiled = [ix for ix in compiled if not (fixture.keys[ix.program_id_index] == TOKEN_PROGRAM_ID and ix.data == b"\x09")]
        proceeds = fixture.principal-fixture.venue_fees
        deltas[0] -= sf.R+proceeds
        deltas[fixture.index[fixture.quote]] += sf.R+proceeds
        meta["postTokenBalances"].append(fixture.token_row(fixture.quote, WSOL_MINT, sf.WALLET, proceeds, TOKEN_PROGRAM_ID))
    for key_, asset in ((fixture.base, mint), (fixture.quote, WSOL_MINT)):
        index = fixture.index[key_]
        prior = before.get(key_)
        old_pre = next((row for row in meta["preTokenBalances"] if row["accountIndex"] == index), None)
        old_post = next((row for row in meta["postTokenBalances"] if row["accountIndex"] == index), None)
        if asset == mint:
            delta = 0 if failed else fixture.actual_base if side == "BUY" else -quantity
        else:
            delta = (0 if old_post is None else int(old_post["uiTokenAmount"]["amount"]))-(0 if old_pre is None else int(old_pre["uiTokenAmount"]["amount"]))
        meta["preTokenBalances"] = [row for row in meta["preTokenBalances"] if row["accountIndex"] != index]
        meta["postTokenBalances"] = [row for row in meta["postTokenBalances"] if row["accountIndex"] != index]
        prior_units, prior_lamports = (0, 0) if prior is None else (prior[2], prior[3])
        if prior is not None:
            meta["preTokenBalances"].append(fixture.token_row(key_, asset, sf.WALLET, prior_units, fixture.token_program if asset == mint else TOKEN_PROGRAM_ID))
        if old_post is not None:
            meta["postTokenBalances"].append(fixture.token_row(key_, asset, sf.WALLET, prior_units+delta, fixture.token_program if asset == mint else TOKEN_PROGRAM_ID))
        meta["preBalances"][index] = prior_lamports
        meta["postBalances"][index] = prior_lamports+deltas[index]
    meta["preBalances"][0] = previous_fixture.scenario.tx["meta"]["postBalances"][0]
    meta["postBalances"][0] = meta["preBalances"][0]+deltas[0]
    recent = bh(140+number)
    header = fixture.message.header
    message = Message.new_with_compiled_instructions(header.num_required_signatures, header.num_readonly_signed_accounts,
        header.num_readonly_unsigned_accounts, fixture.message.account_keys, Hash.from_string(recent), compiled)
    message_bytes = to_bytes_versioned(message)
    fixture.message, fixture.message_bytes = message, message_bytes
    fixture.wire = b"\x01"+bytes([number])*64+message_bytes
    at = lower.block_time+10
    tx_slot, root_slot = lower.slot+8, lower.slot+14
    value = scenario(failed=failed)
    value.signature = sig(number)
    value.initial_slot = value.root_slot = value.status_context = root_slot
    value.status.update(slot=tx_slot)
    value.blocks = {lower.slot: {"blockhash": lower.blockhash, "previousBlockhash": lower.previous_blockhash,
        "parentSlot": lower.parent_slot, "blockHeight": lower.block_height, "blockTime": lower.block_time, "signatures": []},
        lower.slot+4: block(lower.slot+4, lower.slot, lower.block_height+1, time=at+1),
        tx_slot: block(tx_slot, lower.slot+4, lower.block_height+2, [sig(number)], time=at+5),
        lower.slot+10: block(lower.slot+10, tx_slot, lower.block_height+3, time=at+6),
        root_slot: block(root_slot, lower.slot+10, lower.block_height+4, time=at+8)}
    value.blocks[lower.slot+4]["previousBlockhash"] = lower.blockhash
    value.tx = {"slot": tx_slot, "blockTime": at+5, "version": "legacy", "transaction": [base64.b64encode(fixture.wire).decode(), "base64"], "meta": meta}
    fixture.scenario = value
    if mutate is not None:
        mutate(fixture)
    if existing_action is not None:
        action = existing_action
        ordinal = repo._conn.execute("SELECT MAX(ordinal) FROM ledger_attempts WHERE action_id=?", (action.action_id,)).fetchone()[0]+1
    else:
        if side == "BUY":
            item = candidate(mint, suffix=str(number))
            root = repo.receive_candidate(item, fence=repo.write_fence())
            position_id = position_identity(repo.domain, root, mint)
        else:
            position = repo.position_history(position_id)
            root = position.root_id
            item = repo.candidate(root)
        action = PendingAction(root, item.content_digest, side, mint, fixture.token_program, position_id, fixture.units,
            "EXTERNAL_TERMS", content_fingerprint("terms"), "EXTERNAL_POLICY",
            content_fingerprint("policy") if protective_handoff is None else protective_handoff.selected_policy_digest,
            "FINAL-A" if protective_handoff is None else protective_handoff.selected_track,
            None if side == "BUY" else "reduction-"+str(number) if protective_handoff is None else protective_handoff.obligation_id, action_ordinal, (NOW+10000)*1_000_000 if side == "BUY" else None,
            content_fingerprint("deadline") if side == "BUY" else None)
        repo.stage_action(action, fence=repo.write_fence())
        ordinal = 1
    prep = AttemptPreparation(action.action_id, action.content_digest, ordinal, message_bytes.hex(), content_fingerprint(message_bytes.hex()),
        content_fingerprint("message-policy"), BlockhashLeaseV01("EXTERNAL_PUBLIC_PLAN_"+str(number), recent, lower.slot+1,
            lower.block_height+3, "confirmed", at*1_000_000), lower, utc(at+1), "EXTERNAL_PREPARATION", content_fingerprint("preparation"))
    repo.prepare_attempt(prep, fence=repo.write_fence())
    for target, offset in (("EXACT_SIMULATED", 2), ("AUTHORIZED", 3), ("SIGNED_DURABLE", 4)):
        repo.record_external_attempt_stage(stage(prep, target, at=at+offset, signature_byte=number), idempotency_key=target,
            expected_attempt_revision=repo.attempt(prep.attempt_id).revision, fence=repo.write_fence())
    observation = transaction_observation(value, at=at+10, request=TransactionRequest(GENESIS, sig(number), lower.slot))
    receipt = None
    if ingest_finality:
        receipt = repo.ingest_chain_observation(prep.attempt_id, observation, ingestion_key="chain-"+str(number),
            evaluated_at_utc=utc(at+10), fence=repo.write_fence())
    support, wallet, request = composed_support(repo, fixture, root=observation.root, at=at+14)
    return fixture, action, prep, receipt, support, at


def comparison_cases(directory):
    for name in ("match", "deposit", "withdrawal", "token", "locked", "missing", "incomplete", "coverage", "stale", "pending"):
        path = directory / ("comparison-"+name+".sqlite3")
        repo, binding, action, prep, chain, support, fixture = first(path)
        with repo:
            applied = None if name == "pending" else apply(repo, prep, support)
            evidence, value, request = composed_support(repo, fixture)
            if name in ("deposit", "withdrawal", "pending"):
                value.accounts[sf.WALLET]["lamports"] += -111 if name == "withdrawal" else 111
            elif name == "token":
                value.add_token(token=key(23), mint=key(24), data=token_data(key(24), sf.WALLET, 9))
            elif name == "locked":
                value.accounts[fixture.base]["lamports"] += 99
            elif name == "missing":
                value.accounts.pop(fixture.base)
                value.inventory[TOKEN_PROGRAM_ID].remove(fixture.base)
            elif name == "incomplete":
                value.program_failure = sf.TOKEN_2022_PROGRAM_ID
            elif name == "coverage":
                request = replace(request, expected_accounts=())
            observation = observe(value, request=request, at=NOW+14)
            evidence = WalletSupportInput(observation, utc(NOW+100 if name == "stale" else NOW+14), request.min_context_slot)
            old = repo.consumer_snapshot()
            fence = repo.write_fence()
            receipt = repo.compare_wallet_observation(evidence, ingestion_key="compare", fence=fence)
            comparison = receipt.comparison
            expected = ("MATCHED_AT_ORIGINAL_CUT" if name == "match" else "PENDING_EFFECTS_UNKNOWN" if name == "pending" else
                        "INCOMPLETE_OR_UNSUPPORTED" if name in ("incomplete", "coverage", "stale") else "CUSTODY_DIFFERENCES_QUARANTINED")
            print("comparison-"+name, comparison.disposition, comparison.reasons, flush=True)
            check(name+"_original_comparison_classification", comparison.disposition == expected)
            check(name+"_comparison_never_adopts_balances", repo.consumer_snapshot()["funding"].native_lamports == old["funding"].native_lamports
                  and repo.consumer_snapshot()["positions"] == old["positions"])
            check(name+"_common_cut_and_pending_binding", comparison.common_revision == fence.revision and comparison.common_digest == fence.last_receipt_digest
                  and comparison.pending_attempts == ((prep.attempt_id,) if name == "pending" else ()) and not comparison.proves_external_transfer)
            check(name+"_original_support_retrievable_immutable", repo.wallet_support(repo.consumer_snapshot()["latest_comparison"].input_digest) == evidence
                  and not repo.consumer_snapshot()["latest_comparison"].authority_current)
            check(name+"_comparison_duplicate", repo.compare_wallet_observation(evidence, ingestion_key="compare", fence=fence) == receipt)
            check(name+"_comparison_conflicting_key", raises(LedgerConflict, lambda: repo.compare_wallet_observation(
                replace(evidence, evaluated_at_utc=utc(NOW+101)), ingestion_key="compare", fence=repo.write_fence())))
            check(name+"_quarantine_only_positive_contradiction", bool(repo.consumer_snapshot()["funding"].quarantine_reasons)
                  is (expected == "CUSTODY_DIFFERENCES_QUARANTINED"))
            if applied is not None:
                check(name+"_historical_application_does_not_expire_or_undo", apply(repo, prep, support) == applied
                      and repo.attempt(prep.attempt_id).economically_applied)
            digest = repo.audit()["custody_digest"]
        with LedgerRepository.reopen(path, binding) as repo:
            check(name+"_original_evaluation_replay", repo.audit()["custody_digest"] == digest and repo.comparison_receipt("compare") == receipt
                  and repo.wallet_support(evidence.digest) == evidence)


def history_cases(directory):
    path = directory / "nullable-enrichment.sqlite3"
    def nullable(fixture):
        fixture.groups[0]["instructions"][-1]["stackHeight"] = None
    repo, binding, action, prep, chain, support, fixture = first(path, mutate=nullable)
    with repo:
        denied = apply(repo, prep, support)
        check("incomplete_metadata_whole_unapplied", denied.decision.disposition == "UNAPPLIED" and repo.audit()["posting_count"] == 0 and repo.mutation_lane()["held"])
        fixture.groups[0]["instructions"][-1]["stackHeight"] = 2
        observed = transaction_observation(fixture.scenario, at=NOW+15)
        enriched = repo.ingest_chain_observation(prep.attempt_id, observed, ingestion_key="enriched", evaluated_at_utc=utc(NOW+15), fence=repo.write_fence())
        accepted = apply(repo, prep, support, chain="enriched", key="enriched-application", at=NOW+21)
        check("compatible_nullable_enrichment_can_resolve", accepted.decision.disposition == "FINALIZED_SUCCESS_APPLIED" and repo.chain_receipt("transaction") == chain)
        count = repo.audit()["posting_count"]
        duplicate = apply(repo, prep, support, chain="enriched", key="enriched-again", at=NOW+22)
        check("enriched_cannot_apply_twice", duplicate.decision.disposition == "ALREADY_RESOLVED" and repo.audit()["posting_count"] == count)
        digest = repo.audit()["custody_digest"]
    with LedgerRepository.reopen(path, binding) as repo:
        check("enrichment_original_proof_replay", repo.audit()["custody_digest"] == digest and repo.application_receipt("enriched-application") == accepted)
    path = directory / "late-chain.sqlite3"
    repo, binding, action, prep, chain, support, fixture = first(path)
    with repo:
        applied = apply(repo, prep, support)
        _, sell, ps, _, _, at = next_step(repo, fixture, support.observation.anchor, number=64, position_id=action.position_id,
            quantity=fixture.actual_base//2, ingest_finality=False)
        pending_lane = repo.mutation_lane()
        amounts = repo.consumer_snapshot()["funding"].native_lamports
        unknown = copy.deepcopy(fixture.scenario)
        unknown.tx = unknown.status = None
        incoming = transaction_observation(unknown, at=NOW+20)
        result = repo.ingest_chain_observation(prep.attempt_id, incoming, ingestion_key="late-unknown", evaluated_at_utc=utc(NOW+20), fence=repo.write_fence())
        check("late_unknown_retains_application_and_other_lane", repo.attempt(prep.attempt_id).economically_applied
              and not repo.attempt(prep.attempt_id).custody_quarantined and repo.mutation_lane() == pending_lane
              and result.decision.resulting_state.positive_finality == "FINALIZED_SUCCESS")
        bad = copy.deepcopy(fixture.scenario)
        bad.tx["meta"]["fee"] += 1
        bad.tx["meta"]["postBalances"][0] -= 1
        incoming = transaction_observation(bad, at=NOW+21)
        revision = repo.write_fence().revision
        result = repo.ingest_chain_observation(prep.attempt_id, incoming, ingestion_key="late-conflict", evaluated_at_utc=utc(NOW+21), fence=repo.write_fence())
        check("late_positive_conflict_quarantines_current_custody", repo.attempt(prep.attempt_id).custody_quarantined and repo.write_fence().revision == revision+1)
        check("late_conflict_never_undoes_or_steals_lane", repo.consumer_snapshot()["funding"].native_lamports == amounts
              and repo.mutation_lane() == pending_lane and repo.attempt(ps.attempt_id).lane_held)
        check("historical_application_exact_retry_under_quarantine", apply(repo, prep, support) == applied and repo.chain_receipt("transaction") == chain)
        digest = repo.audit()["custody_digest"]
    with LedgerRepository.reopen(path, binding) as repo:
        check("late_conflict_other_lane_reconstructs", repo.audit()["custody_digest"] == digest and repo.mutation_lane() == pending_lane
              and repo.attempt(prep.attempt_id).current_disposition == "CUSTODY_QUARANTINED")


def resolution_cases(directory):
    for kind in ("nonlanding", "failed", "success"):
        path = directory / (kind+"-resolution.sqlite3")
        repo, binding, action, prep, chain, support, fixture = first(path, failed=kind == "failed", landing=kind != "nonlanding")
        with repo:
            if kind == "nonlanding":
                refused = apply(repo, prep, None, key="unknown-resolution")
                check("unknown_cannot_release_without_positive_proof", refused.decision.disposition == "UNAPPLIED" and repo.mutation_lane()["held"])
                value = scenario(absent=True)
                value.tx = value.status = None
                observation = coverage_observation(value, at=NOW+12, request=CoverageRequest(GENESIS, sig(49),
                    prep.lease.blockhash, prep.lease.last_valid_block_height, prep.finalized_lower_anchor))
                proof = repo.ingest_chain_observation(prep.attempt_id, observation, ingestion_key="coverage", evaluated_at_utc=utc(NOW+12), fence=repo.write_fence())
                check("original_canonical_nonlanding_still_held_before_disposition", proof.decision.resulting_state.positive_finality == "PROVEN_NON_LANDED"
                      and repo.mutation_lane()["held"])
                receipt = apply(repo, prep, None, chain="coverage", key="resolved")
                check("nonlanding_atomic_resolution_no_money_or_retry_grant", receipt.decision.disposition == "PROVEN_NON_LANDED_RESOLVED"
                      and repo.audit()["posting_count"] == 0 and repo.consumer_snapshot()["funding"].native_lamports == sf.FUNDING
                      and not repo.mutation_lane()["held"] and not receipt.decision.may_retry)
                # Independently known opening custody: the previous hypothetical
                # transaction was proven absent and therefore supplies no balances.
                fixture.owned_after = {}
                fixture.scenario.tx["meta"]["postBalances"][0] = sf.FUNDING
            else:
                receipt = apply(repo, prep, support)
            if kind == "success":
                check("successful_buy_cannot_reacquire_on_new_ordinal", raises(LedgerConflict, lambda: next_step(repo, fixture,
                    support.observation.anchor, number=65, side="BUY", existing_action=action)))
            else:
                newer, same_action, ps, cs, ss, at = next_step(repo, fixture, support.observation.anchor, number=65,
                    side="BUY", existing_action=action)
                check(kind+"_new_external_message_keeps_exact_action_root", same_action == action and ps.ordinal == 2
                      and repo.attempt(ps.attempt_id).preparation.action_id == action.action_id and repo.mutation_lane()["held"])
                receipt2 = apply(repo, ps, ss, chain="chain-65", key="second-attempt", at=at+20)
                check(kind+"_later_actual_buy_one_acquisition", receipt2.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                      and len(repo.consumer_snapshot()["positions"]) == 1
                      and repo.consumer_snapshot()["funding"].network_fees_paid_lamports == (2 if kind == "failed" else 1)*sf.FEE)
            digest = repo.audit()["custody_digest"]
        with LedgerRepository.reopen(path, binding) as repo:
            check(kind+"_resolved_original_history_replays", repo.audit()["custody_digest"] == digest)
    path = directory / "late-root-versus-wallet.sqlite3"
    repo, binding, action, prep, chain, support, fixture = first(path)
    with repo:
        applied = apply(repo, prep, support)
        matched = repo.compare_wallet_observation(support, ingestion_key="compare", fence=repo.write_fence())
        value = copy.deepcopy(fixture.scenario)
        value.root_slot = value.initial_slot = value.status_context = 116
        value.blocks[116] = block(116, 114, 95, time=NOW+12)
        value.blocks[116]["blockhash"] = bh(200)
        incoming = transaction_observation(value, at=NOW+16)
        late = repo.ingest_chain_observation(prep.attempt_id, incoming, ingestion_key="late-wallet-root-conflict", evaluated_at_utc=utc(NOW+16), fence=repo.write_fence())
        check("late_chain_root_compares_recognized_wallet_anchor", not late.decision.resulting_state.quarantined
              and "CHAIN_ANCHOR_AND_CURRENT_CUSTODY_CONTRADICTION" in repo.consumer_snapshot()["funding"].quarantine_reasons)
        check("late_root_conflict_preserves_actual_application", repo.application_receipt("apply") == applied
              and repo.consumer_snapshot()["funding"].native_lamports == fixture.scenario.tx["meta"]["postBalances"][0])
        digest = repo.audit()["custody_digest"]
    with LedgerRepository.reopen(path, binding) as repo:
        check("late_chain_wallet_anchor_replay", repo.audit()["custody_digest"] == digest)


def precondition_cases(directory):
    for kind in ("native", "prefunded-absent", "missing-inner", "stale-support"):
        def mutate(fixture):
            meta = fixture.scenario.tx["meta"]
            if kind == "native":
                meta["preBalances"][0] += 1
                meta["postBalances"][0] += 1
            elif kind == "prefunded-absent":
                meta["preBalances"][fixture.index[fixture.base]] = 99
                meta["postBalances"][fixture.index[fixture.base]] = 99
            elif kind == "missing-inner":
                meta["innerInstructions"] = []
        path = directory / ("precondition-"+kind+".sqlite3")
        repo, binding, action, prep, chain, support, fixture = first(path, failed=kind == "prefunded-absent", mutate=mutate)
        with repo:
            if kind == "stale-support":
                support = replace(support, evaluated_at_utc=utc(NOW+100))
            prior = repo.consumer_snapshot()
            receipt = apply(repo, prep, support, at=NOW+101)
            check(kind+"_unsafe_application_wholly_withheld", receipt.decision.disposition in ("UNAPPLIED", "QUARANTINED")
                  and receipt.decision.proposal is None and repo.audit()["posting_count"] == 0 and repo.mutation_lane()["held"])
            check(kind+"_no_invented_custody", repo.consumer_snapshot()["funding"].native_lamports == prior["funding"].native_lamports
                  and repo.consumer_snapshot()["accounts"] == prior["accounts"] and not repo.consumer_snapshot()["positions"])
            if kind == "prefunded-absent":
                check("prefunded_absence_is_explicit_precondition_conflict", "CUSTODY_UNATTRIBUTED_PREEXISTING_ACCOUNT" in receipt.decision.reasons)
            digest = repo.audit()["custody_digest"]
        with LedgerRepository.reopen(path, binding) as repo:
            check(kind+"_unapplied_replay", repo.audit()["custody_digest"] == digest and repo.application_receipt("apply") == receipt)
    path = directory / "exact-u64.sqlite3"
    original_funding = sf.FUNDING
    try:
        sf.FUNDING = (1 << 64)-1
        repo, binding, action, prep, chain, support, fixture = first(path)
    finally:
        sf.FUNDING = original_funding
    with repo:
        receipt = apply(repo, prep, support)
        check("exact_u64_opening_and_actual_application", receipt.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
              and repo.consumer_snapshot()["funding"].known_initial_native_lamports == (1 << 64)-1
              and repo.consumer_snapshot()["funding"].native_lamports == fixture.scenario.tx["meta"]["postBalances"][0] > (1 << 63))
        check("exact_u64_projection_serialization_is_integer", str((1 << 64)-1) in repo._conn.execute("SELECT payload_json FROM ledger_funding_projection").fetchone()[0]
              and type(repo.consumer_snapshot()["funding"].native_lamports) is int)
        digest = repo.audit()["custody_digest"]
    with LedgerRepository.reopen(path, binding) as repo:
        check("exact_u64_full_reconstruction", repo.audit()["custody_digest"] == digest and repo.application_receipt("apply") == receipt)


def anchor_cases(directory):
    for source in ("application", "comparison"):
        path = directory / (source+"-preparation-anchor.sqlite3")
        repo, binding, action, prep, chain, support, fixture = first(path)
        with repo:
            apply(repo, prep, support)
            if source == "comparison":
                repo.compare_wallet_observation(support, ingestion_key="compare", fence=repo.write_fence())
            for number, lower in ((66, replace(support.observation.anchor, blockhash=bh(206))),
                                  (67, replace(support.observation.anchor, slot=117, blockhash=bh(117), parent_slot=116,
                                               previous_blockhash=bh(207), block_height=96, block_time=NOW+13))):
                check(source+str(number)+"_conflicting_lower_anchor_denied_before_commit", raises(LedgerConflict,
                    lambda: next_step(repo, fixture, lower, number=number, position_id=action.position_id, quantity=1000))
                    and not repo.mutation_lane()["held"] and repo.audit()["attempt_count"] == 1)
            digest = repo.audit()["custody_digest"]
        with LedgerRepository.reopen(path, binding) as repo:
            check(source+"_denied_preparation_replay", repo.audit()["custody_digest"] == digest and repo.audit()["attempt_count"] == 1)
    for order in ("wallet-first", "chain-first"):
        for incomplete in (False, True):
            name = order+("-incomplete" if incomplete else "-usable")
            path = directory / (name+".sqlite3")
            repo, binding, action, prep, chain, support, fixture = first(path, landing=False)
            with repo:
                evidence, wallet, request = composed_support(repo, fixture)
                if incomplete:
                    wallet.program_failure = sf.TOKEN_2022_PROGRAM_ID
                evidence = WalletSupportInput(observe(wallet, request=request, at=NOW+14), utc(NOW+14), 108)
                value = copy.deepcopy(fixture.scenario)
                value.tx = value.status = None
                value.root_slot = value.initial_slot = value.status_context = 116
                value.blocks[116] = block(116, 114, 95, time=NOW+12)
                value.blocks[116]["blockhash"] = bh(201)
                incoming = transaction_observation(value, at=NOW+16)
                def wallet_append():
                    return repo.compare_wallet_observation(evidence, ingestion_key="compare", fence=repo.write_fence())
                def chain_append():
                    return repo.ingest_chain_observation(prep.attempt_id, incoming, ingestion_key="new-root", evaluated_at_utc=utc(NOW+16), fence=repo.write_fence())
                if order == "wallet-first":
                    comparison = wallet_append()
                    check(name+"_unknown_balances_retain_positive_anchor", any(anchor.slot == 116 for anchor in repo._custody.qualified_wallet_anchors)
                          and repo._custody.anchor.slot == 101 and not repo._custody.quarantined)
                    chain_append()
                else:
                    chain_append()
                    comparison = wallet_append()
                check(name+"_symmetric_anchor_contradiction", repo._custody.quarantined and repo.mutation_lane()["held"]
                      and repo.consumer_snapshot()["funding"].native_lamports == sf.FUNDING and repo.audit()["posting_count"] == 0)
                check(name+"_anchor_never_upgrades_account_usability", not comparison.comparison.authority_current
                      and (not incomplete or comparison.comparison.disposition == "INCOMPLETE_OR_UNSUPPORTED"))
                digest = repo.audit()["custody_digest"]
            with LedgerRepository.reopen(path, binding) as repo:
                check(name+"_retained_original_anchor_replays", repo.audit()["custody_digest"] == digest)
    for source in ("applied-support", "unapplied-support"):
        path = directory / (source+"-anchor.sqlite3")
        def mutate(fixture):
            if source == "unapplied-support":
                fixture.groups[0]["instructions"][-1]["stackHeight"] = None
        repo, binding, action, prep, chain, support, fixture = first(path, mutate=mutate)
        with repo:
            receipt = apply(repo, prep, support)
            check(source+"_retains_support_anchor_beyond_custody", any(anchor.slot == 116 for anchor in repo._custody.qualified_wallet_anchors)
                  and repo._custody.anchor.slot < 116)
            value = copy.deepcopy(fixture.scenario)
            value.root_slot = value.initial_slot = value.status_context = 116
            value.blocks[116] = block(116, 114, 95, time=NOW+12)
            value.blocks[116]["blockhash"] = bh(202)
            incoming = transaction_observation(value, at=NOW+16)
            repo.ingest_chain_observation(prep.attempt_id, incoming, ingestion_key="support-conflict", evaluated_at_utc=utc(NOW+16), fence=repo.write_fence())
            check(source+"_positive_support_fact_not_forgotten", repo._custody.quarantined and repo.application_receipt("apply") == receipt)
            digest = repo.audit()["custody_digest"]
        with LedgerRepository.reopen(path, binding) as repo:
            check(source+"_support_anchor_reconstructs", repo.audit()["custody_digest"] == digest)
    path = directory / "unqualified-wallet-anchor.sqlite3"
    repo, binding, action, prep, chain, support, fixture = first(path)
    with repo:
        apply(repo, prep, support)
        repo.compare_wallet_observation(support, ingestion_key="matched", fence=repo.write_fence())
        original = repo._custody.qualified_wallet_anchors
        for source in ("profile", "genesis"):
            _, wallet, request = composed_support(repo, fixture)
            if source == "genesis":
                wallet.end_genesis = bh(203)
            transform = wallet.transform
            def change(payload, envelope):
                envelope = transform(payload, envelope)
                if payload["method"] == "getBlock":
                    envelope["result"]["blockhash"] = bh(204)
                return envelope
            wallet.transform = change
            profile = replace(PROFILE, provider_id="UNEXPECTED_PROVIDER") if source == "profile" else PROFILE
            evidence = WalletSupportInput(observe(wallet, request=request, at=NOW+14, profile=profile), utc(NOW+14), 108)
            receipt = repo.compare_wallet_observation(evidence, ingestion_key="unqualified-"+source, fence=repo.write_fence())
            check(source+"_cannot_seed_canonical_wallet_claim", repo._custody.qualified_wallet_anchors == original and not repo._custody.quarantined
                  and receipt.comparison.disposition == "INCOMPLETE_OR_UNSUPPORTED")
        repo.audit()


def child(mode, path):
    binding = sf.LedgerDomain(GENESIS, sf.WALLET, PROFILE.fingerprint, sf.FUNDING, minimum_context_slot=100)
    if mode == "--compete":
        try:
            with LedgerRepository.reopen(path, binding):
                return 99
        except LedgerConflict:
            return 93
    with LedgerRepository.reopen(path, binding) as repo:
        fixture = sf.Fixture()
        fixture.mint = sf.MINT
        support, _, _ = composed_support(repo, fixture)
        prep = repo.attempt(repo._conn.execute("SELECT attempt_id FROM ledger_attempts").fetchone()[0]).preparation
        original = repo._commit
        def crash():
            if mode.endswith("before"):
                os._exit(91)
            original()
            os._exit(92)
        repo._commit = crash
        if "compare" in mode:
            repo.compare_wallet_observation(support, ingestion_key="compare", fence=repo.write_fence())
        else:
            apply(repo, prep, support)
    return 99


def run_child(mode, path):
    return subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), mode, str(path)],
                          cwd=ROOT, capture_output=True, text=True, timeout=30)


def fault_cases(directory):
    results = {}
    for operation in ("apply", "compare"):
        for cut in ("before", "after"):
            name = operation+"-"+cut
            path = directory / (name+".sqlite3")
            repo, binding, action, prep, chain, support, fixture = first(path)
            with repo:
                if operation == "compare":
                    apply(repo, prep, support)
                starting = repo.audit()
            completed = run_child("--"+name, path)
            check(name+"_process_dies_at_common_commit", completed.returncode == (91 if cut == "before" else 92) and not completed.stdout and not completed.stderr)
            with LedgerRepository.reopen(path, binding) as repo:
                count_name = "application_receipt_count" if operation == "apply" else "comparison_receipt_count"
                check(name+"_whole_commit_or_nothing", repo.audit()[count_name] == starting[count_name]+int(cut == "after"))
                if operation == "apply":
                    check(name+"_postings_position_lane_atomic", repo.audit()["posting_count"] == (0 if cut == "before" else 9)
                          and bool(repo.consumer_snapshot()["positions"]) is (cut == "after") and repo.mutation_lane()["held"] is (cut == "before"))
                    receipt = apply(repo, prep, support)
                    check(name+"_retry_actual_projection", repo.consumer_snapshot()["funding"].native_lamports == fixture.scenario.tx["meta"]["postBalances"][0]
                          and repo.audit()["application_receipt_count"] == 1 and not repo.mutation_lane()["held"])
                else:
                    receipt = repo.compare_wallet_observation(support, ingestion_key="compare", fence=repo.write_fence())
                    check(name+"_comparison_retry_exact_cut", receipt.comparison.disposition == "MATCHED_AT_ORIGINAL_CUT"
                          and repo.wallet_support(receipt.support.digest) == support)
                results[name] = {"child_exit": completed.returncode, "receipt_digest": receipt.content_digest,
                    "custody_digest": repo.audit()["custody_digest"]}
        check(operation+"_both_crash_cuts_same_immutable_receipt", results[operation+"-before"]["receipt_digest"] == results[operation+"-after"]["receipt_digest"])
    for cut in ("before-publication", "after-publication"):
        path = directory / (cut+".sqlite3")
        repo, binding, action, prep, chain, support, fixture = first(path)
        with repo:
            original_connection, original_commit = repo._conn, repo._commit
            class LostCommitAcknowledgement:
                def __getattr__(self, name):
                    return getattr(original_connection, name)
                def execute(self, command, *args):
                    result = original_connection.execute(command, *args)
                    if command == "COMMIT":
                        raise RuntimeError("inert fault")
                    return result
            def after_publication():
                original_commit()
                raise RuntimeError("inert fault")
            if cut == "before-publication":
                repo._conn = LostCommitAcknowledgement()
            else:
                repo._commit = after_publication
            check(cut+"_lost_ack_reports_uncertainty", raises(LedgerJournalError, lambda: apply(repo, prep, support)))
            repo._conn, repo._commit = original_connection, original_commit
            if cut == "before-publication":
                check("unpublished_projection_never_served_as_trusted", raises(LedgerConflict, repo.consumer_snapshot)
                      and raises(LedgerConflict, lambda: apply(repo, prep, support)))
            else:
                receipt = apply(repo, prep, support)
                check("published_projection_exact_retry_converges", receipt.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                      and repo.audit()["application_receipt_count"] == 1 and repo.consumer_snapshot()["funding"].native_lamports == fixture.scenario.tx["meta"]["postBalances"][0])
        with LedgerRepository.reopen(path, binding) as repo:
            check(cut+"_original_atomic_application_recovered", apply(repo, prep, support).decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                  and repo.audit()["application_receipt_count"] == 1 and not repo.mutation_lane()["held"])
    return results


def storage_cases(directory):
    path = directory / "baseline-cache-rollback.sqlite3"
    binding = sf.LedgerDomain(GENESIS, sf.WALLET, PROFILE.fingerprint, sf.FUNDING, minimum_context_slot=100)
    with LedgerRepository.initialize(path, binding) as repo:
        value = WalletScenario()
        value.accounts = {sf.WALLET: account(SYSTEM_PROGRAM_ID, lamports=sf.FUNDING)}
        request = WalletEvidenceRequest(sf.WALLET, GENESIS, 100)
        original = repo._commit
        def interrupted():
            raise RuntimeError("inert before-commit fault")
        repo._commit = interrupted
        check("opening_rollback_reports_uncertainty", raises(LedgerJournalError, lambda: sf.ingest(repo, observe(value, request=request))))
        repo._commit = original
        value = WalletScenario()
        value.genesis = bh(205)
        denied = sf.ingest(repo, observe(value, request=request), key="unresolved")
        check("rolled_back_baseline_cannot_poison_later_custody_cache", denied.decision.disposition != "ESTABLISHED"
              and repo.baseline() is None and repo.consumer_snapshot()["funding"].native_lamports is None and repo.audit()["revision"] == 1)
    path = directory / "outside-projection.sqlite3"
    repo, binding, action, prep, chain, support, fixture = first(path)
    with repo:
        applied = apply(repo, prep, support)
        previous = repo.audit()["custody_digest"]
        competing = run_child("--compete", path)
        check("competing_process_cannot_mutate_custody", competing.returncode == 93 and repo.audit()["custody_digest"] == previous)
        old_fence = repo.write_fence()
        with closing(sqlite3.connect(path, isolation_level=None)) as external:
            original = external.execute("SELECT content_digest FROM ledger_funding_projection").fetchone()[0]
            external.execute("BEGIN IMMEDIATE")
            external.execute("UPDATE ledger_funding_projection SET content_digest=?", ("0"*64,))
            external.execute("UPDATE ledger_funding_projection SET content_digest=?", (original,))
            external.execute("COMMIT")
        check("outside_noop_projection_commit_invalidates_consumer", raises(LedgerConflict, repo.consumer_snapshot)
              and raises(LedgerConflict, lambda: repo.position_history(action.position_id))
              and raises(LedgerConflict, lambda: repo.wallet_support(support.digest))
              and raises(LedgerConflict, lambda: repo.application_receipt("apply")))
        check("outside_noop_commit_cannot_be_blessed_by_application", raises(LedgerConflict, lambda: apply(repo, prep, support)))
        check("outside_noop_commit_cannot_be_blessed_by_comparison", raises(LedgerConflict, lambda: repo.compare_wallet_observation(support, ingestion_key="compare", fence=repo.write_fence())))
    with LedgerRepository.reopen(path, binding) as repo:
        check("explicit_reopen_reverifies_unchanged_projection", repo.audit()["custody_digest"] == previous and repo.application_receipt("apply") == applied)
        check("stale_writer_generation_cannot_apply_even_duplicate", raises(LedgerConflict, lambda: apply(repo, prep, support, fence=old_fence)))
        old_revision = repo.write_fence()
        repo.compare_wallet_observation(support, ingestion_key="compare", fence=repo.write_fence())
        check("common_custody_revision_fences_new_application", raises(LedgerConflict, lambda: apply(repo, prep, support, key="new", at=NOW+21, fence=old_revision)))
        for table in ("ledger_custody_inputs", "ledger_application_receipts", "ledger_wallet_comparisons", "ledger_economic_postings"):
            check(table+"_append_only", raises(sqlite3.DatabaseError, lambda table=table: repo._conn.execute("DELETE FROM "+table)))
        raw = json.loads(_support_json(support))
        for name, changed in (("version", {**raw, "version": "unknown"}), ("extra", {**raw, "authority": True}),
                              ("digest", {**raw, "input_digest": "0"*64}), ("floor", {**raw, "required_min_context_slot": True})):
            check("support_codec_hostile_"+name, raises(Exception, lambda changed=changed: _support_from_json(canonical_json(changed))))
    for table in ("ledger_funding_projection", "ledger_account_projection", "ledger_position_projection", "ledger_resolution_projection"):
        path = directory / (table+"-tamper.sqlite3")
        repo, binding, action, prep, chain, support, fixture = first(path)
        with repo:
            apply(repo, prep, support)
        with closing(sqlite3.connect(path, isolation_level=None)) as external:
            payload = json.loads(external.execute("SELECT payload_json FROM "+table+" LIMIT 1").fetchone()[0])
            # Even coherently rehashed projection rows cannot replace originals.
            field = {"ledger_funding_projection": "native_lamports", "ledger_account_projection": "units",
                     "ledger_position_projection": "remaining_units", "ledger_resolution_projection": "application_sequence"}[table]
            payload[field] += 1
            external.execute("UPDATE "+table+" SET payload_json=?,content_digest=? WHERE rowid=(SELECT rowid FROM "+table+" LIMIT 1)",
                             (canonical_json(payload), content_fingerprint(payload)))
        check(table+"_tamper_rejected_by_full_original_replay", raises(LedgerJournalError, lambda: LedgerRepository.reopen(path, binding)))
    path = directory / "wrong-binding.sqlite3"
    repo, binding, action, prep, chain, support, fixture = first(path)
    repo.close()
    check("wrong_economic_domain_cannot_reopen", raises(LedgerJournalError, lambda: LedgerRepository.reopen(path, replace(binding, wallet=key(27)))))
    with closing(sqlite3.connect(path, isolation_level=None)) as external:
        external.execute("PRAGMA user_version=3")
    check("old_schema_explicitly_rejected_no_migration", raises(LedgerJournalError, lambda: LedgerRepository.reopen(path, binding)))


def main():
    with tempfile.TemporaryDirectory(prefix="live-ledger-custody-") as temporary:
        directory = Path(temporary)
        digests = {}
        for name, venue, failed in (("pump-buy", "pump", False), ("swap-buy", "swap", False), ("failed-fee", "pump", True)):
            path = directory / (name+".sqlite3")
            repo, binding, action, prep, chain, support, fixture = first(path, venue=venue, failed=failed)
            with repo:
                old_fence = repo.write_fence()
                receipt = apply(repo, prep, support)
                print(name, receipt.decision.disposition, receipt.decision.reasons, flush=True)
                expected = "FINALIZED_FAILURE_APPLIED" if failed else "FINALIZED_SUCCESS_APPLIED"
                check(name+"_whole_original_application", receipt.decision.disposition == expected)
                snapshot = repo.consumer_snapshot()
                check(name+"_actual_native_not_double_fee", snapshot["funding"].native_lamports == fixture.scenario.tx["meta"]["postBalances"][0]
                      and snapshot["funding"].network_fees_paid_lamports == sf.FEE)
                check(name+"_own_lane_released_without_authority", not repo.mutation_lane()["held"]
                      and repo.attempt(prep.attempt_id).economically_applied and not receipt.decision.may_retry)
                check(name+"_original_finality_receipt_unchanged", repo.chain_receipt("transaction") == chain
                      and chain.decision.resulting_state.disposition.endswith("UNAPPLIED"))
                check(name+"_exact_retry_converges", apply(repo, prep, support, fence=old_fence) == receipt)
                check(name+"_changed_same_key_rejected", raises(LedgerConflict, lambda: apply(repo, prep, support, at=NOW+21)))
                second = apply(repo, prep, support, key="second", at=NOW+21)
                check(name+"_new_key_no_second_application", second.decision.disposition == "ALREADY_RESOLVED"
                      and repo.audit()["posting_count"] == len(receipt.decision.proposal.components))
                check(name+"_typed_immutable_funding", raises(FrozenInstanceError, lambda: setattr(snapshot["funding"], "native_lamports", 0)))
                check(name+"_consumer_is_explicitly_not_current_authority", snapshot["authority_current"] is False and snapshot["comparison_is_historical"])
                if not failed:
                    position = snapshot["positions"][0]
                    check(name+"_owned_protection_pending", position.remaining_units == fixture.actual_base
                          and position.status == "OWNED_PROTECTION_PENDING" and not position.usable and not position.has_protective_handoff)
                    check(name+"_typed_immutable_position", raises(FrozenInstanceError, lambda: setattr(position, "remaining_units", 0)))
                    quote = next(item for item in snapshot["accounts"] if item.pubkey == fixture.quote)
                    check(name+"_explicit_absent_WSOL_tombstone", quote.presence == "ABSENT" and quote.observed_lamports == quote.units == 0)
                else:
                    check(name+"_no_invented_position", not snapshot["positions"])
                digests[name] = receipt.content_digest
                custody_digest = repo.audit()["custody_digest"]
            with LedgerRepository.reopen(path, binding) as repo:
                check(name+"_full_original_replay", repo.application_receipt("apply") == receipt
                      and repo.audit()["custody_digest"] == custody_digest)
                check(name+"_reopen_exact_original_retry", apply(repo, prep, support) == receipt)
        for venue, retained in (("pump", False), ("swap", False), ("swap", True)):
            label = venue+("-retained" if retained else "-recycled")
            path = directory / (label+".sqlite3")
            repo, binding, action, prep, chain, support, fixture = first(path)
            with repo:
                apply(repo, prep, support)
                initial_units = fixture.actual_base
                partial = min(10_000_000_000, initial_units//2)
                next_fixture, reduction, attempt, proof, evidence, at = next_step(repo, fixture, support.observation.anchor,
                    number=50, venue=venue, position_id=action.position_id, quantity=partial, keep_wsol=retained)
                receipt = apply(repo, attempt, evidence, chain="chain-50", key="sell-50", at=at+20)
                print(label, receipt.decision.disposition, receipt.decision.reasons, flush=True)
                check(label+"_partial_actual_reduction", receipt.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                      and repo.position_history(action.position_id).remaining_units == initial_units-partial)
                snapshot = repo.consumer_snapshot()
                check(label+"_native_metadata_after_reduction", snapshot["funding"].native_lamports == next_fixture.scenario.tx["meta"]["postBalances"][0])
                quote = next(row for row in snapshot["accounts"] if row.pubkey == fixture.quote)
                check(label+"_quote_not_double_counted", (quote.presence == "PRESENT" and quote.units == next_fixture.principal-next_fixture.venue_fees
                    and quote.observed_locked_lamports == sf.R and quote.native_rent_reserve == sf.R) if retained else quote.presence == "ABSENT")
                final_fixture, final_action, final_attempt, proof, final_support, at = next_step(repo, next_fixture, evidence.observation.anchor,
                    number=51, venue=venue, position_id=action.position_id, quantity=initial_units-partial)
                last = apply(repo, final_attempt, final_support, chain="chain-51", key="sell-51", at=at+20)
                print(label+" full", last.decision.disposition, last.decision.reasons, flush=True)
                position = repo.position_history(action.position_id)
                check(label+"_full_actual_sale_no_retirement", last.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                      and position.remaining_units == 0 and position.sold_units == initial_units and position.status == "FLAT_PENDING_RECONCILIATION")
                check(label+"_fees_once_each_transaction", repo.consumer_snapshot()["funding"].network_fees_paid_lamports == 3*sf.FEE)
                digest = repo.audit()["custody_digest"]
            with LedgerRepository.reopen(path, binding) as repo:
                check(label+"_sequential_replay", repo.audit()["custody_digest"] == digest and repo.application_receipt("sell-51") == last)
            digests[label] = last.content_digest
        for different_mint in (False, True):
            label = "two-mints" if different_mint else "shared-account"
            path = directory / (label+".sqlite3")
            repo, binding, action, prep, chain, support, fixture = first(path)
            with repo:
                apply(repo, prep, support)
                second, action2, prep2, chain2, support2, at = next_step(repo, fixture, support.observation.anchor,
                    number=60, side="BUY", mint=key(22) if different_mint else sf.MINT)
                receipt2 = apply(repo, prep2, support2, chain="chain-60", key="buy-60", at=at+20)
                print(label, receipt2.decision.disposition, receipt2.decision.reasons, flush=True)
                check(label+"_distinct_owned_roots", receipt2.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                      and action.position_id != action2.position_id and len(repo.consumer_snapshot()["positions"]) == 2)
                expected_units = second.actual_base if different_mint else fixture.actual_base+second.actual_base
                check(label+"_account_is_actual_aggregate", next(row for row in repo.consumer_snapshot()["accounts"] if row.pubkey == second.base).units == expected_units)
                reduction, sell, ps, cs, ss, at = next_step(repo, second, support2.observation.anchor, number=61,
                    position_id=action.position_id, quantity=fixture.actual_base)
                sold = apply(repo, ps, ss, chain="chain-61", key="sell-61", at=at+20)
                check(label+"_only_bound_position_reduced", sold.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                      and repo.position_history(action.position_id).remaining_units == 0
                      and repo.position_history(action2.position_id).remaining_units == second.actual_base)
                check(label+"_no_singleton_consumer", len(repo.consumer_snapshot(max_positions=2)["positions"]) == 2
                      and raises(LedgerJournalError, lambda: repo.consumer_snapshot(max_positions=1)))
                digest = repo.audit()["custody_digest"]
            with LedgerRepository.reopen(path, binding) as repo:
                check(label+"_reconstruction", repo.audit()["custody_digest"] == digest)
            digests[label] = sold.content_digest
        for failed in (False, True):
            label = "oversell-failed" if failed else "oversell-success"
            path = directory / (label+".sqlite3")
            repo, binding, action, prep, chain, support, fixture = first(path)
            with repo:
                apply(repo, prep, support)
                second, action2, prep2, _, support2, at = next_step(repo, fixture, support.observation.anchor, number=62, side="BUY")
                apply(repo, prep2, support2, chain="chain-62", key="buy-62", at=at+20)
                reduction, sell, ps, cs, ss, at = next_step(repo, second, support2.observation.anchor, number=63,
                    position_id=action.position_id, quantity=fixture.actual_base+1, failed=failed)
                before_snapshot = repo.consumer_snapshot()
                sold = apply(repo, ps, ss, chain="chain-63", key="sell-63", at=at+20)
                print(label, sold.decision.disposition, sold.decision.reasons, flush=True)
                check(label+"_fill_precondition_only_when_actual_fill", sold.decision.disposition == ("FINALIZED_FAILURE_APPLIED" if failed else "QUARANTINED"))
                check(label+"_owned_units_unchanged", repo.consumer_snapshot()["positions"] == before_snapshot["positions"])
                check(label+"_exact_fee_or_whole_no_postings", repo.consumer_snapshot()["funding"].network_fees_paid_lamports == (3 if failed else 2)*sf.FEE)
                check(label+"_lane_release_only_applied", repo.mutation_lane()["held"] is (not failed))
                digest = repo.audit()["custody_digest"]
            with LedgerRepository.reopen(path, binding) as repo:
                check(label+"_reconstruction", repo.audit()["custody_digest"] == digest)
        comparison_cases(directory)
        history_cases(directory)
        resolution_cases(directory)
        precondition_cases(directory)
        anchor_cases(directory)
        crash_results = fault_cases(directory)
        storage_cases(directory)
    print(canonical_json({"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "check_count": len(CHECKS),
        "failed": [name for name, value in CHECKS.items() if not value], "application_digests": digests, "crash_cuts": crash_results}))
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 3:
        raise SystemExit(child(sys.argv[1], Path(sys.argv[2])))
    raise SystemExit(main())
