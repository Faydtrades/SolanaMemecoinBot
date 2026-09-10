"""B4 one actual Step-7 position lifecycle, public synthetic boundaries only."""
from __future__ import annotations

import copy
import json
import sys
import tempfile
from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import patch

import httpx
from solders.keypair import Keypair

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_execution_composition_selftest_v0_1 as q4
import live_ledger_finality_selftest_v0_1 as lf
from live.position_controller_v0_1 import bind_actual_position
from live.exit_observation_v0_1 import commit_exit_evaluation
from live.protective_obligation_v0_1 import ensure_protective_obligation, stage_protective_sell
from live.protective_outcome_v0_1 import protective_outcome
from live.transaction_coverage_v0_1 import CoverageRequest
from phase5.shadow_domain_v0_1 import content_fingerprint

q3, q2, q1, a4, a3, a5, sf = q4.q3, q4.q2, q4.q1, q4.a4, q4.a3, q4.a5, q4.sf
NOW, utc, control, send = q4.NOW, a3.utc, q4.control, q4.send
CHECKS = []


def check(name, value):
    if not value:
        raise AssertionError(name)
    CHECKS.append(name)
    print('OK', name)


def reject(name, call, expected=None):
    try:
        call()
    except (ValueError, RuntimeError, FrozenInstanceError) as exc:
        check(name, expected is None or expected in str(exc))
    else:
        check(name, False)


class UnknownObserved(Exception):
    pass


def execute(f, key, action, public, lower, *, at, number, failed=False, unknown=False, stale=None, prior=None, probe_stop=False):
    """Reuse q4.execute unchanged; adapt only fixture heights and ordinal input.

    All Q1 production, exact preparation, A4b, guarded Q2, mock-HTTP Q3 and
    public Q4 are the accepted actual implementations. Missing chain data is
    external fixture input; the Runtime projection supplies the exact fence.
    """
    captured = {}
    original_evidence, original_produce = a4.evidence, q1.produce
    original_prepare, original_finality = q1.prepare_exact_message, q4.read_finality
    original_sign = q2.invoke
    original_consume = control.consume

    def evidence(*args, **kwargs):
        kwargs.update(last_valid_height=lower.block_height+30, validity_height=lower.block_height+1)
        return original_evidence(*args, **kwargs)

    class PublicBoundary(q1.PublicTransport):
        def __call__(self, request):
            response = super().__call__(request)
            value, method = response.json(), json.loads(request.content)['method']
            if method == 'getLatestBlockhash':
                value['result']['value']['lastValidBlockHeight'] = lower.block_height+30
            elif method == 'getBlockHeight':
                value['result'] = lower.block_height+1
            return httpx.Response(200, json=value)

    def produce(*args, **kwargs):
        if action.side == 'SELL':
            captured['eligibility'] = protective_outcome(f.repo, f.binding) if stale is None else stale
            check(str(number)+'_Runtime_attempt_eligible', captured['eligibility'].mutation_eligible
                and captured['eligibility'].action == action)
        before = f.repo.write_fence()
        value = original_produce(*args, **kwargs)
        check(str(number)+'_fresh_Q1_reads_do_not_change_cut', f.repo.write_fence() == before)
        captured['production'] = value[0]
        return value

    def prepare(repo, production, *, fence, ordinal=1):
        if action.side == 'SELL':
            outcome = captured['eligibility']
            fence, ordinal = outcome.fence, outcome.next_attempt_ordinal
        result = original_prepare(repo, production, fence=fence, ordinal=ordinal)
        captured['prep'] = result.preparation
        return result

    def finality(f, envelope, scenario, **kwargs):
        if unknown:
            scenario = copy.deepcopy(scenario)
            scenario.tx = scenario.status = None
            for block in scenario.blocks.values():
                block['signatures'] = []
            captured['public_unknown'] = scenario
            captured['unknown'] = original_finality(f, envelope, scenario, **kwargs)
            raise UnknownObserved
        return original_finality(f, envelope, scenario, **kwargs)

    def sign(f, key, production, delivery, at):
        if prior is not None:
            reject(str(number)+'_old_SIGN_delivery_cannot_authorize_replacement',
                lambda:original_sign(f,key,production,prior['sign_delivery'],at))
        captured['sign_delivery'] = delivery
        return original_sign(f,key,production,delivery,at)

    def consume(f, value, request, *, at):
        if probe_stop and request.stage == 'SIGN':
            a3.control(f.repo,'HARD_STOP','b4-prepared-stop',at=at)
            denied = original_consume(f,value,control.request(f,captured['prep'],'prepared-stop-sign'),at=at)
            check('actual_A4b_fresh_SIGN_denied_only_by_hard_stop', type(denied) is not control.FreshStageConsumption
                and denied.decision.reasons == ('GLOBAL_HARD_STOP_LATCHED',)
                and f.repo.attempt(captured['prep'].attempt_id).primary_signature is None)
            # Explicit external Authority control fixture; Runtime does not
            # create release barriers or claim operational recovery readiness.
            a3.control(f.repo,'RELEASE_STOP','b4-prepared-release',at=at,target='b4-prepared-stop',
                barrier=content_fingerprint('external-prepared-test-barrier'))
        delivery = original_consume(f,value,request,at=at)
        if probe_stop and request.stage == 'SIGN':
            check('same_prepared_attempt_A4b_SIGN_eligible_after_external_release', type(delivery) is control.FreshStageConsumption)
        return delivery

    with patch.object(a4, 'evidence', evidence), patch.object(q1, 'PublicTransport', PublicBoundary), \
            patch.object(q1, 'produce', produce), patch.object(q1, 'prepare_exact_message', prepare), \
            patch.object(q4, 'read_finality', finality), patch.object(q2, 'invoke', sign), \
            patch.object(control, 'consume', consume):
        try:
            captured['result'] = q4.execute(f, key, action, public, lower, route='pump', at=at, number=number,
                failed=failed, outcome='TRANSPORT_UNRESOLVED' if unknown else 'ACKNOWLEDGED')
        except UnknownObserved:
            if not unknown:
                raise
    if 'prep' in captured:
        captured['envelope'] = send.load_signed_envelope(f.repo, captured['prep'].attempt_id)
    return captured


def apply(f, run, *, at, key):
    values = run['result']
    return f.repo.apply_settlement(values[1].attempt_id, chain_receipt_key=values[2].ingestion_key,
        support=values[3], ingestion_key=key, recorded_at_utc=utc(at), fence=f.repo.write_fence())


def nonlanding(f, envelope, known, *, at, key, partial=False):
    # Same accepted q3.nonlanding public coverage assembler, with block times
    # based on this later lifecycle's actual lower anchor. No injected verdict.
    prep = envelope.preparation
    lower = prep.finalized_lower_anchor
    scenario = copy.deepcopy(known)
    scenario.tx = scenario.status = None
    # Preserve the already sampled UNKNOWN root/header and every intermediate
    # public block. Evidence extension cannot rewrite an earlier chain anchor.
    last_slot = scenario.root_slot
    original_root = scenario.blocks[last_slot]
    last_hash = original_root['blockhash']
    final_height = prep.lease.last_valid_block_height+1
    count = final_height-original_root['blockHeight']
    start_slot = last_slot
    for offset, height in enumerate(range(original_root['blockHeight']+1, final_height+1), 1):
        slot = start_slot+offset
        scenario.blocks[slot] = {'blockhash':lf.bh(slot), 'previousBlockhash':last_hash, 'parentSlot':last_slot,
            'blockHeight':height, 'blockTime':original_root['blockTime']+offset*4//count, 'signatures':[]}
        last_slot, last_hash = slot, lf.bh(slot)
    scenario.initial_slot = scenario.root_slot = last_slot
    request = CoverageRequest(f.domain.genesis_hash, envelope.primary_signature, prep.lease.blockhash,
        prep.lease.last_valid_block_height, lower, last_slot-3 if partial else None)
    observed = lf.coverage_observation(scenario, at=at, request=request)
    receipt = f.repo.ingest_chain_observation(prep.attempt_id, observed, ingestion_key=key,
        evaluated_at_utc=utc(at), fence=f.repo.write_fence())
    return receipt, observed.root


def state(f):
    return protective_outcome(f.repo, f.binding)


def main():
    with tempfile.TemporaryDirectory(prefix='live-outcome-b4-') as temp:
        key = Keypair()  # Ephemeral synthetic signer, never serialized or logged.
        with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
            f, _, _, buy = a4.fixture(Path(temp), 'b4-lifecycle')
            try:
                public = {f.domain.wallet:a5.PublicAccount(a5.RpcAccountV01(f.domain.wallet,sf.SYSTEM_PROGRAM_ID,b''),sf.FUNDING,False,0),
                    sf.MINT:a5.PublicAccount(sf.plans.fx.mint_account(),1000000000,False,0),
                    sf.WSOL_MINT:a5.PublicAccount(a5.RpcAccountV01(sf.WSOL_MINT,sf.TOKEN_PROGRAM_ID,a3.mint_data(9)),1000000000,False,0)}
                acquired = execute(f,key,buy,public,a3.observe().anchor,at=NOW+9,number=71)
                applied_buy = apply(f,acquired,at=NOW+20,key='actual-buy')
                check('actual_Step7_BUY_applied_before_Runtime', applied_buy.decision.disposition == 'FINALIZED_SUCCESS_APPLIED'
                    and not f.repo.position_history(buy.position_id).usable)
                f.binding = bind_actual_position(f.repo,position_id=buy.position_id,root_id=buy.root_id,mint=buy.mint,
                    selected_track=buy.selected_exit_track,policy_digest=buy.policy_digest,
                    acquisition_application_key='actual-buy').binding
                evaluation = commit_exit_evaluation(f.repo,f.binding,command_id='original-fallback',timer_fence_utc=utc(NOW+21))
                initial = ensure_protective_obligation(f.repo,f.binding,recorded_at_utc=utc(NOW+21))
                check('late_original_fallback_creates_actual_obligation', evaluation.payload['state']['trigger_kind'] == 'FALLBACK'
                    and initial.handoff.fallback_anchor_us == f.binding.candidate.generated_at_us
                    and initial.handoff.fallback_fire_boundary_us == f.binding.fallback_fire_boundary_us
                    and state(f).state == 'STAGE_ACTION')
                quantity = initial.acquired_units
                partial = stage_protective_sell(f.repo,f.binding,max_units=quantity//3)
                public, lower = acquired['result'][4:6]
                q3.source_gap(f)
                stale = state(f)
                a3.control(f.repo,'STOP_ENTRY','b4-entry-stop',at=NOW+22)
                reject('stale_Runtime_fence_rejected_with_fresh_Q1_context',
                    lambda:execute(f,key,partial,public,lower,at=NOW+29,number=72,stale=stale),
                    'LEDGER_REVISION_CAS_FAILED')
                check('stale_eligibility_created_no_attempt', not f.repo.protective_position_context(
                    buy.position_id,f.binding.binding_id)['lane']['held'])
                partial_run = execute(f,key,partial,public,lower,at=NOW+29,number=73)
                check('successful_finality_unapplied_keeps_obligation_held', state(f).state == 'HELD'
                    and state(f).obligation.remaining_units == quantity and state(f).next_attempt_ordinal is None)
                apply(f,partial_run,at=NOW+40,key='partial-success')
                residual = state(f)
                check('partial_actual_application_releases_only_fulfilled_claim', residual.state == 'STAGE_ACTION'
                    and residual.mutation_eligible and residual.obligation.remaining_units == quantity-partial.input_units
                    and residual.obligation.obligation_id == initial.obligation_id)
                f.reopen()
                check('partial_reopen_preserves_binding_units_and_obligation', state(f).obligation == residual.obligation)
                action = stage_protective_sell(f.repo,f.binding)
                check('fresh_residual_action_uses_actual_units_and_next_action_ordinal', action.input_units == quantity-partial.input_units
                    and action.ordinal == 2 and action.action_id != partial.action_id
                    and action.obligation_id == partial.obligation_id)
                public, lower = partial_run['result'][4:6]
                failed_run = execute(f,key,action,public,lower,at=NOW+49,number=74,failed=True)
                check('failed_finality_without_fee_application_is_held', state(f).state == 'HELD' and not state(f).mutation_eligible)
                before_fee = f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports
                failure = apply(f,failed_run,at=NOW+60,key='failed-sell-fee')
                replacement = state(f)
                check('actual_failed_fee_keeps_residual_due', failure.decision.disposition == 'FINALIZED_FAILURE_APPLIED'
                    and f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports == before_fee+7777
                    and replacement.obligation.remaining_units == action.input_units)
                check('applied_failure_allows_same_action_next_attempt', replacement.state == 'REPLACE_ATTEMPT'
                    and replacement.mutation_eligible and replacement.action == action and replacement.next_attempt_ordinal == 2
                    and stage_protective_sell(f.repo,f.binding) == action)
                f.reopen()
                check('failed_resolution_reopen_preserves_replacement_eligibility', state(f).state == 'REPLACE_ATTEMPT'
                    and state(f).action == action and state(f).next_attempt_ordinal == 2)
                a3.control(f.repo,'HARD_STOP','b4-hard-stop',at=NOW+61)
                check('hard_stop_suppresses_otherwise_eligible_replacement', state(f).state == 'REPLACE_ATTEMPT'
                    and state(f).reason == 'GLOBAL_HARD_STOP' and not state(f).mutation_eligible
                    and ensure_protective_obligation(f.repo,f.binding,recorded_at_utc=utc(NOW+61)).handoff == initial.handoff)
                # External control fixture, not a Runtime release/recovery port.
                a3.control(f.repo,'RELEASE_STOP','b4-release-hard',at=NOW+62,target='b4-hard-stop',barrier=content_fingerprint('external-test-barrier'))
                public, lower = failed_run['result'][4:6]
                unknown_run = execute(f,key,action,public,lower,at=NOW+69,number=75,unknown=True,prior=failed_run)
                held = state(f)
                check('UNKNOWN_is_held_without_timeout_retry', held.state == 'HELD' and not held.mutation_eligible
                    and held.next_attempt_ordinal is None and held.action == action
                    and unknown_run['prep'].ordinal == 2 and unknown_run['envelope'].primary_signature != failed_run['envelope'].primary_signature)
                check('source_gap_ENTRY_stop_never_erases_protection', held.obligation.handoff == initial.handoff
                    and held.obligation.evaluation == evaluation and held.obligation.remaining_units == action.input_units)
                f.reopen()
                check('UNKNOWN_reopen_retains_original_signature_and_claim', state(f).state == 'HELD'
                    and send.load_signed_envelope(f.repo,unknown_run['prep'].attempt_id) == unknown_run['envelope']
                    and stage_protective_sell(f.repo,f.binding) == action)
                partial_proof, _ = nonlanding(f,unknown_run['envelope'],unknown_run['public_unknown'],at=NOW+80,key='incomplete-proof',partial=True)
                check('elapsed_lease_and_incomplete_coverage_stay_UNKNOWN', partial_proof.decision.resulting_state.positive_finality is None
                    and state(f).state == 'HELD')
                proof, root = nonlanding(f,unknown_run['envelope'],unknown_run['public_unknown'],at=NOW+81,key='complete-proof')
                check('positive_nonlanding_proof_alone_does_not_release_claim', proof.decision.resulting_state.positive_finality == 'PROVEN_NON_LANDED'
                    and state(f).state == 'HELD' and not state(f).mutation_eligible
                    and state(f).obligation.remaining_units == action.input_units)
                f.reopen()
                check('proof_only_reopen_remains_held', state(f).state == 'HELD' and state(f).next_attempt_ordinal is None)
                resolved = f.repo.apply_settlement(unknown_run['prep'].attempt_id,chain_receipt_key='complete-proof',support=None,
                    ingestion_key='nonlanding-resolved',recorded_at_utc=utc(NOW+82),fence=f.repo.write_fence())
                check('Ledger_application_alone_qualifies_lawful_replacement', resolved.decision.disposition == 'PROVEN_NON_LANDED_RESOLVED'
                    and state(f).state == 'REPLACE_ATTEMPT' and state(f).next_attempt_ordinal == 3
                    and state(f).action == action and state(f).obligation.obligation_id == initial.obligation_id)
                f.reopen()
                check('resolved_nonlanding_reopen_keeps_same_action_and_ordinal', state(f).action == action and state(f).next_attempt_ordinal == 3)
                final_run = execute(f,key,action,public,root,at=NOW+85,number=76,prior=unknown_run,probe_stop=True)
                check('replacement_uses_fresh_exact_message_and_Authority', final_run['prep'].ordinal == 3
                    and final_run['production'].message_hex != unknown_run['production'].message_hex
                    and final_run['envelope'].primary_signature != unknown_run['envelope'].primary_signature
                    and final_run['production'].original.evidence.wallet.observation.anchor == root)
                check('final_success_response_and_finality_are_not_satisfaction', state(f).state == 'HELD'
                    and state(f).obligation.remaining_units == action.input_units)
                actual_final = apply(f,final_run,at=NOW+96,key='actual-final-zero')
                satisfied = state(f)
                check('SATISFIED_requires_actual_applied_successful_zero', actual_final.decision.disposition == 'FINALIZED_SUCCESS_APPLIED'
                    and satisfied.state == 'SATISFIED' and satisfied.obligation.remaining_units == 0
                    and satisfied.obligation.sold_units == quantity and not satisfied.obligation.claiming_actions
                    and not satisfied.mutation_eligible and satisfied.next_attempt_ordinal is None)
                check('satisfaction_retains_original_obligation_and_reservation', satisfied.obligation.obligation_id == initial.obligation_id
                    and satisfied.obligation.handoff == initial.handoff and f.repo.reservation(buy.root_id).status == 'RESERVED')
                reject('satisfied_position_cannot_create_another_sell',lambda:stage_protective_sell(f.repo,f.binding))
                reject('outcome_snapshot_is_immutable',lambda:setattr(satisfied,'state','REPLACE_ATTEMPT'))
                f.reopen()
                check('actual_zero_and_same_satisfaction_reconstruct_after_reopen', state(f).state == 'SATISFIED'
                    and state(f).obligation == satisfied.obligation)
                check('common_journal_replay_integrity', bool(f.repo.audit()))
            finally:
                f.close()
    check('accepted_Execution_checks_all_true', bool(q4.CHECKS) and all(q4.CHECKS.values()))
    print('B4_CHECKS',len(CHECKS),'EXECUTION_CHECKS',len(q4.CHECKS))
    print('RESULT: PASS')


if __name__ == '__main__':
    main()
