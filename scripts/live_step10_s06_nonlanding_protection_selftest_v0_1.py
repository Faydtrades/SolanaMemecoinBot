"""Step10 bounded owned-Runtime public nonlanding and protective replacement."""
from __future__ import annotations
import base64
import copy
import hashlib
import json
import sys
import tempfile
import time
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch
import live_step10_s01_s02_selftest_v0_1 as prior
import live_step10_s06_buy_recovery_selftest_v0_1 as buy_helpers
from live.transaction_coverage_v0_1 import CoverageRequest, ledger_canonical_coverage
import live_ledger_finality_selftest_v0_1 as lf
from solders.message import to_bytes_versioned

ops, c2, a3, sf, NOW = prior.ops, prior.c2, prior.a3, prior.sf, prior.NOW
CHECKS, EVIDENCE = {}, {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def send(f, key, action, at, number, *, failed=False, unknown=False):
    with prior.external_mint(action.mint), prior.retained_accumulator_rpc(f, action):
        result, reads, sends = c2.execution(f, key, action, at=at, number=number,
            outcome='TRANSPORT_UNRESOLVED' if unknown else 'ACKNOWLEDGED')
        check(f.name+'_'+str(number)+'_actual_guarded_send', result.work == 'SUBMISSION_OBSERVED' and len(sends.requests) == 1)
        envelope, actual, scenario, pre = c2.original_chain(f, result, at, failed=failed)
    wire = base64.b64decode(envelope.signed_wire_base64)
    simulation = next(r for r in reads.requests if r['method'] == 'simulateTransaction')
    check(f.name+'_'+str(number)+'_exact_simulated_signed_sent',
        to_bytes_versioned(c2.VersionedTransaction.from_bytes(base64.b64decode(simulation['params'][0])).message)
        == bytes.fromhex(envelope.preparation.message_hex)
        and sends.requests[0]['params'][0] == envelope.signed_wire_base64
        and all(c2.VersionedTransaction.from_bytes(wire).verify_with_results()))
    if unknown:
        scenario.tx = scenario.status = None
        for block in scenario.blocks.values():
            block['signatures'] = []
    return result, envelope, actual, scenario, pre


def coverage_scenario(envelope, known, at, *, incomplete=False):
    """Extend immutable external canonical headers; absence is public data only."""
    scenario = copy.deepcopy(known)
    scenario.tx = scenario.status = None
    root_slot = scenario.root_slot
    root = scenario.blocks[root_slot]
    final_height = envelope.preparation.lease.last_valid_block_height+1
    count = final_height-root['blockHeight']
    last_slot, last_hash = root_slot, root['blockhash']
    for offset, height in enumerate(range(root['blockHeight']+1, final_height+1), 1):
        slot = root_slot+offset
        scenario.blocks[slot] = {'blockhash':sf.bh(slot), 'previousBlockhash':last_hash,
            'parentSlot':last_slot, 'blockHeight':height,
            'blockTime':root['blockTime']+offset*(at-1-root['blockTime'])//count, 'signatures':[]}
        last_slot, last_hash = slot, sf.bh(slot)
    scenario.initial_slot = scenario.root_slot = scenario.status_context = last_slot
    scenario.slot_calls = scenario.genesis_calls = 0
    if incomplete:
        del scenario.blocks[last_slot-3]
    return scenario


def economics(f):
    state = f.repo.consumer_snapshot()
    return {name:state[name] for name in ('funding','positions','reservations','accounts')}


def denied_replacement(f, sent, label):
    before = f.repo.audit()
    try:
        f.repo.prepare_attempt(replace(sent[1].preparation, ordinal=sent[1].preparation.ordinal+1),
            fence=f.repo.write_fence())
    except (ValueError, RuntimeError) as exc:
        denial = str(exc)
    else:
        raise AssertionError('held replacement unexpectedly allowed')
    check(f.name+'_'+label+'_replacement_denied_without_mutation',
        denial == 'LEDGER_WALLET_MUTATION_LANE_HELD' and f.repo.audit() == before)
    return denial


def nonlanding(f, sent, at):
    result, envelope, actual, scenario, pre = sent
    before = economics(f)
    unknown, unknown_calls = c2.reconcile(f, scenario, at-8)
    check(f.name+'_public_missing_transaction_UNKNOWN', unknown.reason == 'UNKNOWN'
        and f.repo.attempt(result.attempt_id).lane_held and economics(f) == before)
    full = coverage_scenario(envelope, scenario, at-1)
    incomplete = copy.deepcopy(full)
    del incomplete.blocks[full.root_slot-3]
    partial, partial_calls = c2.reconcile(f, incomplete, at)
    partial_receipt = f.repo.chain_receipt(partial.receipt_key)
    check(f.name+'_incomplete_public_certificate_holds', partial.reason == 'UNKNOWN'
        and partial_receipt.decision.evidence_port_disposition == 'INSUFFICIENT_COVERAGE'
        and f.repo.attempt(result.attempt_id).lane_held and economics(f) == before)
    partial_denial = denied_replacement(f, sent, 'incomplete')
    pending = f.step(at)
    check(f.name+'_incomplete_original_truth_priority', pending.work == 'NEED_RECONCILIATION'
        and pending.attempt_id == result.attempt_id)
    complete, complete_calls = c2.reconcile(f, full, at+1)
    chain = f.repo.chain_receipt(complete.receipt_key)
    check(f.name+'_complete_public_certificate_reaches_Runtime', complete.reason == 'PROVEN_NON_LANDED'
        and chain.decision.evidence_port_disposition == 'COMPLETE_REQUESTED_INTERVAL'
        and chain.observation.request == CoverageRequest(f.domain.genesis_hash, envelope.primary_signature,
            envelope.preparation.lease.blockhash,envelope.preparation.lease.last_valid_block_height,
            envelope.preparation.finalized_lower_anchor)
        and chain.observation.root.block_height > envelope.preparation.lease.last_valid_block_height)
    check(f.name+'_complete_certificate_still_encumbered_before_application',
        f.repo.attempt(result.attempt_id).lane_held and economics(f) == before
        and f.repo.reservation(f.buy.root_id).status == 'RESERVED')
    full_denial = denied_replacement(f, sent, 'complete_unapplied')
    check(f.name+'_public_read_bounds_preserved', len(chain.observation.blocks_descending) <= 256
        and chain.observation.root.slot-envelope.preparation.finalized_lower_anchor.slot <= 2048
        and any(r['method'] == 'getSignatureStatuses' for r in complete_calls)
        and any(r['method'] == 'getTransaction' for r in complete_calls)
        and sum(r['method'] == 'getBlock' and r['params'][1]['transactionDetails'] == 'signatures'
            for r in complete_calls) == len(chain.observation.blocks_descending))
    # An independently read contradictory external fixture is classified without
    # mutating this live obligation; accepted Ledger quarantine semantics remain
    # authoritative. This is evidence-port classification, not Runtime recovery.
    conflicting = copy.deepcopy(full)
    conflicting.blocks[full.root_slot-3]['previousBlockhash'] = sf.bh(240)
    observation = lf.coverage_observation(conflicting, at=at+1, request=chain.observation.request)
    contradiction = ledger_canonical_coverage(observation, expected_request=chain.observation.request,
        expected_profile_fingerprint=f.domain.expected_profile_fingerprint, now_utc=a3.utc(at+1))
    check(f.name+'_contradictory_public_parent_chain_classified', contradiction.disposition == 'CONTRADICTORY'
        and 'CANONICAL_PARENT_HASH_OR_HEIGHT_CONTRADICTION' in contradiction.reasons)
    proof_stamp = prior.stamp(f,'positive public proof still encumbered')
    applied = f.step(at+2)
    receipt = f.repo.application_receipt(applied.receipt_key)
    check(f.name+'_actual_Runtime_nonlanding_application_no_invented_fee',
        applied.work == 'APPLIED' and applied.reason == 'PROVEN_NON_LANDED_RESOLVED'
        and not f.repo.attempt(result.attempt_id).lane_held
        and f.repo.consumer_snapshot()['funding'] == before['funding']
        and f.repo.consumer_snapshot()['positions'] == before['positions']
        and f.repo.reservation(f.buy.root_id).status == 'RESERVED')
    audit = f.repo.audit()
    replay = f.repo.apply_settlement(result.attempt_id,chain_receipt_key=chain.ingestion_key,support=None,
        ingestion_key=applied.receipt_key,recorded_at_utc=receipt.recorded_at_utc,fence=f.repo.write_fence())
    check(f.name+'_nonlanding_application_replay_exactly_once', replay == receipt and f.repo.audit() == audit)
    # Unchanged wallet facts are now observed under the actual public proof root.
    f.lower = chain.observation.root
    return receipt, {'unknown':asdict(unknown),'incomplete':asdict(partial),'complete':asdict(complete),
        'unknown_rpc':unknown_calls,'incomplete_rpc':partial_calls,'complete_rpc':complete_calls,
        'incomplete_decision':asdict(partial_receipt.decision),'complete_decision':asdict(chain.decision),
        'public_canonical_blocks':full.blocks,'coverage_limits':asdict(chain.observation.limits),
        'coverage_block_reads':len(chain.observation.blocks_descending),
        'contradiction_evidence_port_only':asdict(contradiction),
        'denials':[partial_denial,full_denial],'before_application':proof_stamp,
        'application':receipt.to_record(),'after_application':prior.stamp(f,'actual nonlanding resolution')}


def unchanged_authority(f):
    check(f.name+'_original_policy_and_grant_never_changed', all(
        f.repo.authority_snapshot()[name] == f.original_authority[name]
        for name in ('policy','armed_entry_grant','entry_stop_command','hard_stop_command')))


def run_buy(directory):
    name = 's06-owned-public-nonlanding-BUY'
    key = c2.Keypair()
    with patch.object(sf,'WALLET',str(key.pubkey())),patch.object(sf.plans,'ACTOR',str(key.pubkey())):
        original_observe, original_domain = a3.observe, a3.domain
        account = a3.derive_associated_token_address(sf.WALLET,sf.MINT,sf.TOKEN_PROGRAM_ID)
        expected = (a3.ExpectedTokenAccount(account,sf.MINT,sf.TOKEN_PROGRAM_ID),)
        opening = True
        def known_opening_wallet(scenario=None, *, request=None, **kwargs):
            nonlocal opening
            if opening:
                opening = False
                request = replace(request,expected_accounts=expected)
                scenario.accounts[sf.MINT] = a3.account(sf.TOKEN_PROGRAM_ID,a3.mint_data(6))
            return original_observe(scenario,request=request,**kwargs)
        def known_opening_domain(**kwargs):
            return original_domain(**kwargs,expected_empty_token_accounts=expected)
        with patch.object(a3,'observe',known_opening_wallet), patch.object(a3,'domain',known_opening_domain):
            f = prior.prepare(directory,name)
        try:
            check(name+'_actual_opening_wallet_known_absence',len(f.repo.baseline().decision.known_token_accounts) == 1
                and f.repo.baseline().decision.known_token_accounts[0].presence == 'ABSENT')
            initial = prior.stamp(f,'actual owned canonical admission')
            sent = send(f,key,f.buy,NOW+6,191,unknown=True)
            record = {'configuration':prior.EVIDENCE[name],'initial':initial,'action':asdict(f.buy)}
            EVIDENCE[name] = record
            applied, proof = nonlanding(f,sent,NOW+20)
            record.update(proof=proof,attempt=buy_helpers.public_attempt(sent,applied))
            missing = f.step(NOW+23)
            stale = f.step(NOW+23,retirement_wallet=prior.wallet(f,NOW+22,mint=f.buy.mint))
            check(name+'_positive_expiry_missing_stale_wallet_retains_reservation',
                f.buy.claimed_entry_deadline_us < (NOW+23)*1000000 and missing.work == 'NEED_RETIREMENT'
                and stale.work == 'RETIREMENT_WITHHELD' and f.repo.reservation(f.buy.root_id).status == 'RESERVED')
            incomplete = f.step(NOW+24,retirement_wallet=prior.continuation.wallet(f,NOW+24,incomplete=True))
            check(name+'_retirement_requires_complete_known_account_coverage',
                incomplete.work == 'RETIREMENT_WITHHELD'
                and 'COMPARISON_EXPLICIT_KNOWN_ACCOUNT_COVERAGE_MISSING'
                    in f.repo.consumer_snapshot()['latest_comparison'].reasons
                and f.repo.reservation(f.buy.root_id).status == 'RESERVED')
            support = prior.wallet(f,NOW+25,mint=f.buy.mint)
            retired = f.step(NOW+25,retirement_wallet=support)
            receipt = f.repo.port_receipt(retired.receipt_key)
            check(name+'_actual_nonlanding_retirement_without_fee_or_acquisition',
                retired.work == 'RETIREMENT_RETIRED' and receipt.retirement.reason == 'PROVEN_NON_LANDED'
                and f.repo.consumer_snapshot()['funding'].native_lamports == sf.FUNDING
                and f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports == 0
                and not f.repo.consumer_snapshot()['reservations'] and not f.repo.consumer_snapshot()['positions']
                and f.repo.audit()['attempt_count'] == 1 and f.runtime.position_binding is None)
            audit = f.repo.audit()
            replay = f.repo.retire(receipt.retirement,support,ingestion_key=retired.receipt_key,fence=f.repo.write_fence())
            check(name+'_retirement_replay_no_mutation',replay == receipt and f.repo.audit() == audit)
            denied = f.repo.admit_authority_entry(a3.EntryRequest(f.buy.root_id,'PUMP',sf.TOKEN_PROGRAM_ID,f.policy.selected_track),
                a3.clock(f.repo,NOW+26),f.source,prior.wallet(f,NOW+26,mint=f.buy.mint),
                command_id=name+'-old-root',fence=f.repo.write_fence())
            check(name+'_expired_original_root_cannot_reacquire',not denied.accepted
                and f.repo.audit()['attempt_count'] == 1 and not f.repo.consumer_snapshot()['reservations'])
            unchanged_authority(f)
            record.update(missing=asdict(missing),stale=asdict(stale),incomplete=asdict(incomplete),
                retirement=asdict(receipt.retirement),old_root_denial=denied.risk.reasons,
                final=prior.stamp(f,'retired original nonlanded BUY'),audit=f.repo.audit())
        finally:
            ops.close(f)


def run_sell(directory):
    name = 's06-owned-protective-replacements'
    key = c2.Keypair()
    with patch.object(sf,'WALLET',str(key.pubkey())),patch.object(sf.plans,'ACTOR',str(key.pubkey())):
        f = prior.prepare(directory,name)
        try:
            record = {'configuration':prior.EVIDENCE[name],'initial':prior.stamp(f,'actual owned canonical admission'),
                'buy_action':asdict(f.buy)}
            EVIDENCE[name] = record
            bought,buy_evidence = prior.transact(f,key,f.buy,NOW+6,192)
            binding = f.runtime.position_binding
            monitored = f.step(NOW+14)
            staged = f.step(NOW+16)
            action = f.repo.action(staged.action_id)
            original = c2.protective_outcome(f.repo,binding)
            quantity = f.repo.position_history(f.buy.position_id).remaining_units
            check(name+'_actual_acquisition_controller_due_full_SELL',
                bought.reason == 'FINALIZED_SUCCESS_APPLIED' and monitored.work == 'ENTRY_HELD'
                and staged.work == 'PROTECTIVE_ACTION_STAGED' and action.side == 'SELL'
                and action.input_units == quantity and action.root_id == f.buy.root_id
                and action.obligation_id == original.obligation.obligation_id)
            first = send(f,key,action,NOW+20,193,failed=True)
            fees_before = f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports
            failed,_ = buy_helpers.apply(f,first,NOW+28)
            outcome = c2.protective_outcome(f.repo,binding)
            check(name+'_actual_failed_fee_preserves_all_remaining_protection',
                failed.decision.disposition == 'FINALIZED_FAILURE_APPLIED'
                and failed.decision.proposal.transaction_fee_lamports == sf.FEE
                and failed.decision.proposal.base_units_delta == 0
                and f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports == fees_before+sf.FEE
                and f.repo.position_history(f.buy.position_id).remaining_units == quantity
                and outcome.obligation.remaining_units == quantity and outcome.obligation.handoff == original.obligation.handoff
                and outcome.state == 'REPLACE_ATTEMPT' and outcome.action == action and outcome.next_attempt_ordinal == 2)
            second = send(f,key,action,NOW+32,194,unknown=True)
            check(name+'_fresh_failed_replacement_same_action_original_obligation',
                second[1].preparation.ordinal == 2 and second[1].preparation.action_id == action.action_id
                and second[1].primary_signature != first[1].primary_signature
                and second[1].preparation.message_sha256 != first[1].preparation.message_sha256)
            resolved,proof = nonlanding(f,second,NOW+46)
            replacement = c2.protective_outcome(f.repo,binding)
            check(name+'_applied_nonlanding_preserves_original_obligation_and_units',
                replacement.state == 'REPLACE_ATTEMPT' and replacement.next_attempt_ordinal == 3
                and replacement.action == action and replacement.obligation.handoff == original.obligation.handoff
                and replacement.obligation.remaining_units == quantity
                and f.repo.position_history(f.buy.position_id).remaining_units == quantity)
            third = send(f,key,action,NOW+52,195)
            check(name+'_second_fresh_replacement_original_action_ordinal_three',
                third[1].preparation.ordinal == 3 and third[1].preparation.action_id == action.action_id
                and len({r[1].primary_signature for r in (first,second,third)}) == 3
                and len({r[1].preparation.message_sha256 for r in (first,second,third)}) == 3)
            stages = [buy_helpers.stages(f,s[0].attempt_id) for s in (first,second,third)]
            check(name+'_every_replacement_fresh_SIGN_SEND_consumption',
                all([r.original.request.stage for r in group] == ['SIGN','SEND'] for group in stages)
                and all(r.consumed for group in stages for r in group)
                and len({r.content_digest for group in stages for r in group}) == 6)
            succeeded,_ = buy_helpers.apply(f,third,NOW+60)
            satisfied = c2.protective_outcome(f.repo,binding)
            check(name+'_actual_success_only_reduces_original_full_units',
                succeeded.decision.disposition == 'FINALIZED_SUCCESS_APPLIED'
                and succeeded.decision.proposal.base_units_delta == -quantity
                and satisfied.state == 'SATISFIED' and satisfied.obligation.remaining_units == 0
                and satisfied.obligation.sold_units == quantity and satisfied.obligation.handoff == original.obligation.handoff
                and f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports == 3*sf.FEE
                and f.repo.reservation(f.buy.root_id).status == 'RESERVED')
            needed = f.step(NOW+61)
            support = prior.continuation.wallet(f,NOW+62)
            retired = f.step(NOW+62,retirement_wallet=support)
            receipt = f.repo.port_receipt(retired.receipt_key)
            check(name+'_actual_full_retirement_after_success',needed.work == 'NEED_RETIREMENT'
                and retired.work == 'RETIREMENT_RETIRED' and receipt.retirement.terminal_attempt_id == third[0].attempt_id
                and not f.repo.consumer_snapshot()['positions'] and not f.repo.consumer_snapshot()['reservations']
                and f.repo.position_history(f.buy.position_id).acquired_units == f.repo.position_history(f.buy.position_id).sold_units == quantity
                and f.repo.audit()['attempt_count'] == 4 and f.repo.audit()['action_count'] == 2)
            audit = f.repo.audit()
            replay = f.repo.retire(receipt.retirement,support,ingestion_key=retired.receipt_key,fence=f.repo.write_fence())
            check(name+'_retirement_replay_no_mutation',replay == receipt and f.repo.audit() == audit)
            unchanged_authority(f)
            record.update(acquisition=buy_evidence,binding=asdict(binding),sell_action=asdict(action),
                original_protection=asdict(original),first_failed=buy_helpers.public_attempt(first,failed),
                nonlanded=buy_helpers.public_attempt(second,resolved),proof=proof,
                successful=buy_helpers.public_attempt(third,succeeded),
                authority_stages=[[{'stage':r.original.request.stage,'digest':r.content_digest,
                    'command_id':r.original.request.command_id,'consumed':r.consumed} for r in group] for group in stages],
                retirement=asdict(receipt.retirement),final_position=asdict(f.repo.position_history(f.buy.position_id)),
                final=prior.stamp(f,'retired original fully protected position'),audit=f.repo.audit())
        finally:
            ops.close(f)


def main():
    started = time.perf_counter()
    try:
        with tempfile.TemporaryDirectory(prefix='step10-s06-public-nonlanding-') as tmp:
            run_buy(Path(tmp))
            run_sell(Path(tmp))
    finally:
        paths = {Path(__file__), c2.ROOT/'src/live/runtime_composition_v0_1.py',
            c2.ROOT/'src/live/execution_reconciliation_v0_1.py',
            c2.ROOT/'docs/live/MEME_LIVE_PRODUCTION_CLOSURE_ARCHITECTURE_V2.md'}
        paths.update(Path(m.__file__).resolve() for m in tuple(sys.modules.values())
            if getattr(m,'__file__',None) and Path(m.__file__).suffix == '.py'
            and Path(m.__file__).resolve().is_relative_to(c2.ROOT))
        print(c2.canonical_json({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW',
            'scope':'S06_BOUNDED_PUBLIC_NONLANDING_PROTECTIVE_REPLACEMENT',
            'fixture_version':'STEP10_S06_PUBLIC_NONLANDING_V01','qualification':'SYNTHETIC_ENGINEERING_ONLY',
            'checks':CHECKS,'reused_helper_checks':prior.CHECKS,'application_helper_checks':buy_helpers.CHECKS,'evidence':EVIDENCE,
            'limitations':['Synthetic engineering only; no production key, network, or real capital',
                'Full remaining units after failure; partial-reduction E02 excluded',
                'Contradictory coverage checked at existing Evidence port only; actual Runtime quarantine references prior accepted faults'],
            'duration_seconds':time.perf_counter()-started,
            'artifact_hashes':{str(p.relative_to(c2.ROOT)).replace('\\','/'):prior.lf_sha256(p) for p in sorted(paths)}}))


if __name__ == '__main__':
    main()
