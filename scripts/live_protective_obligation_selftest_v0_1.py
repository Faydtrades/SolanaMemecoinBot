"""B3 actual Ledger BUY/B2 -> original protection -> actual-unit SELL fixtures."""
from __future__ import annotations

import json
import sys
import tempfile
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from phase5.shadow_domain_v0_1 import content_fingerprint
from live.protective_obligation_v0_1 import (
    ensure_protective_obligation, protective_obligation_snapshot, stage_protective_sell,
)
from live.exit_observation_v0_1 import commit_exit_evaluation
from live.ledger_actions_v0_1 import PendingAction
from live.ledger_repository_v0_1 import LedgerRepository
from live_wallet_evidence_selftest_v0_1 import NOW, utc
import live_exit_observation_selftest_v0_1 as b2
import live_ledger_custody_selftest_v0_1 as cf
import live_authority_controls_selftest_v0_1 as controls
import live_ledger_ports_selftest_v0_1 as ports

CHECKS=[]


def check(name,value):
    if not value:
        raise AssertionError(name)
    CHECKS.append(name)
    print('OK',name)


def reject(name,call):
    try:
        call()
    except (ValueError,RuntimeError,FrozenInstanceError):
        check(name,True)
    else:
        check(name,False)


def protect(f,at=NOW+16):
    return ensure_protective_obligation(f.repo,f.binding,recorded_at_utc=utc(at))


def snapshot(f):
    return protective_obligation_snapshot(f.repo,f.binding)


def sell(f,**kwargs):
    return stage_protective_sell(f.repo,f.binding,**kwargs)


def reduce_actual(f,action,number=50):
    # Reuse the accepted public exact-message/metadata/finality fixture while
    # supplying B3's already durable action instead of its external terms.
    def original_action(root,candidate_digest,side,mint,program,position,units,*args,**kwargs):
        assert (root,candidate_digest,side,mint,program,position,units) == (
            action.root_id,action.candidate_digest,action.side,action.mint,action.token_program,action.position_id,action.input_units)
        return action
    with patch.object(cf,'PendingAction',original_action):
        fixture,actual_action,prep,chain,support,at=cf.next_step(f.repo,f.market,f.support.observation.anchor,
            number=number,quantity=action.input_units,position_id=action.position_id,
            protective_handoff=snapshot(f).handoff,action_ordinal=action.ordinal)
    assert actual_action == action
    applied=cf.apply(f.repo,prep,support,chain='chain-'+str(number),key='partial-'+str(number),at=at+20)
    return fixture,prep,support,applied,at+20


def main():
    with tempfile.TemporaryDirectory(prefix='live-protection-b3-') as temp:
        directory=Path(temp)
        f=b2.Fixture(directory,'monitoring')
        try:
            reject('B2_evaluation_required',lambda:protect(f,NOW+14))
            monitoring=f.evaluate(at=NOW+14)
            initial=protect(f,NOW+14)
            check('original_monitoring_handoff',initial.state == 'MONITORING' and initial.handoff.obligation_state == 'MONITORING')
            check('atomic_usable_actual_position_and_protection',f.repo.position_history(f.action.position_id).usable
                and f.repo.protection(f.action.position_id).handoff == initial.handoff)
            check('original_acquisition_and_fallback_retained',initial.handoff.acquisition_signature == f.binding.acquisition_signature
                and initial.handoff.acquired_units == f.market.actual_base
                and initial.handoff.fallback_anchor_us == f.binding.candidate.generated_at_us
                and initial.handoff.fallback_fire_boundary_us == f.binding.fallback_fire_boundary_us)
            reject('monitoring_cannot_stage_sell',lambda:sell(f))
            # Source is deliberately unavailable; B2 still owns the original
            # timer and B3 reads only its committed decision.
            f.conn.close()
            due=f.evaluate('due',NOW+16)
            current=protect(f,NOW+16)
            check('later_due_preserves_original_monitoring_handoff',current.state == 'DUE' and current.handoff == initial.handoff
                and current.handoff.obligation_state == 'MONITORING' and current.evaluation == due)
            action=sell(f)
            check('immutable_full_actual_unit_sell',action.side == 'SELL' and action.input_units == f.market.actual_base
                and action.input_units != f.action.input_units and action.obligation_id == current.obligation_id
                and action.selected_exit_track == f.binding.spec.track_id and action.policy_digest == f.binding.policy_digest)
            check('one_staged_claim_no_duplicate_allocation',sell(f) == action and snapshot(f).encumbered_units == action.input_units
                and snapshot(f).unencumbered_units == 0 and len(snapshot(f).claiming_actions) == 1)
            reject('pending_sell_amount_cannot_change',lambda:sell(f,max_units=1))
            reject('pending_action_is_immutable',lambda:setattr(action,'input_units',1))
            f.reopen_ledger()
            restored=snapshot(f)
            check('reopen_same_obligation_and_original_trigger',restored.obligation_id == initial.obligation_id
                and restored.handoff == initial.handoff and restored.evaluation == due)
            check('reopen_same_immutable_sell_decision',sell(f) == action and restored.remaining_units == f.market.actual_base)
            check('original_monitoring_eval_also_retained',f.repo.exit_record('evaluate') == monitoring)
        finally:
            f.close()

        f=b2.Fixture(directory,'market')
        try:
            seq=f.append(15)
            f.capture(proofs={seq:f.pair()})
            market=f.evaluate()
            initial=protect(f,NOW+14)
            action=sell(f)
            check('actual_A2_B2_market_trigger_binds_handoff',initial.handoff.trigger_kind == 'MARKET'
                and initial.handoff.trigger_evidence_digest == market.content_digest
                and action.external_decision_ref.endswith(market.content_digest))
            require_cut=f.repo.consumer_snapshot()['consumer_cut']
            altered=replace(f.binding,fallback_deadline_us=f.binding.fallback_deadline_us+1)
            reject('caller_modified_binding_cannot_create_due',lambda:stage_protective_sell(f.repo,altered))
            check('malformed_binding_denial_does_not_mutate',f.repo.consumer_snapshot()['consumer_cut'] == require_cut)
        finally:
            f.close()

        f=b2.Fixture(directory,'late','SENS-C')
        try:
            due=f.evaluate(at=NOW+20)
            original=protect(f,NOW+20)
            check('late_acquisition_activates_already_due_original_timer',original.state == 'DUE'
                and original.handoff.trigger_at_us == f.binding.fallback_fire_boundary_us
                and original.handoff.trigger_at_us < int(NOW+14)*1_000_000)
            check('late_fallback_can_produce_actual_sell',sell(f).input_units == original.remaining_units)
        finally:
            f.close()

        f=b2.Fixture(directory,'partial')
        try:
            f.evaluate(at=NOW+16)
            original=protect(f)
            ceiling=original.remaining_units//3
            first=sell(f,max_units=ceiling)
            check('explicit_partial_ceiling_uses_actual_available',first.input_units == ceiling
                and snapshot(f).unencumbered_units == original.remaining_units-ceiling)
            check('pending_partial_action_replays_instead_of_second_claim',sell(f) == first)
            _,_,_,applied,at=reduce_actual(f,first)
            current=snapshot(f)
            check('partial_actual_settlement_changes_remaining',applied.decision.disposition == 'FINALIZED_SUCCESS_APPLIED'
                and current.sold_units == ceiling and current.remaining_units == original.remaining_units-ceiling)
            check('positive_applied_reduction_releases_only_its_claim',current.encumbered_units == 0
                and current.unencumbered_units == current.remaining_units and current.obligation_id == original.obligation_id)
            second=sell(f)
            check('residual_action_never_reuses_original_buy_quantity',second.input_units == current.remaining_units
                and second.input_units != original.acquired_units and second.ordinal == 2
                and second.obligation_id == first.obligation_id and second.action_id != first.action_id)
            f.reopen_ledger()
            check('residual_obligation_reopen_exact',snapshot(f).remaining_units == current.remaining_units
                and snapshot(f).handoff == original.handoff and sell(f) == second)
        finally:
            f.close()

        f=b2.Fixture(directory,'stops')
        try:
            f.evaluate(at=NOW+16)
            controls.control(f.repo,'STOP_ENTRY','entry-stop',at=NOW+16)
            initial=protect(f)
            f.conn.close()
            check('entry_stop_and_source_loss_preserve_obligation',snapshot(f).obligation_id == initial.obligation_id)
            check('entry_stop_does_not_deny_protective_action',sell(f).input_units == initial.remaining_units)
            controls.control(f.repo,'HARD_STOP','hard-stop',at=NOW+17)
            before=f.repo.consumer_snapshot()['consumer_cut']
            reject('hard_stop_denies_sell_decision_even_idempotent_call',lambda:sell(f))
            check('hard_stop_preserves_durable_protection',protect(f,NOW+20).handoff == initial.handoff
                and f.repo.consumer_snapshot()['consumer_cut'] == before)
        finally:
            f.close()

        f=b2.Fixture(directory,'hard-before-activation')
        try:
            f.evaluate(at=NOW+16)
            controls.control(f.repo,'HARD_STOP','hard',at=NOW+16)
            check('hard_stop_allows_obligation_maintenance',protect(f).state == 'DUE')
            reject('hard_stop_denies_new_action',lambda:sell(f))
            check('hard_stop_created_no_sell',not [a for _,a in f.repo.protective_position_context(f.action.position_id,f.binding.binding_id)['actions'] if a.side == 'SELL'])
        finally:
            f.close()

        f=b2.Fixture(directory,'cut-race')
        try:
            f.evaluate(at=NOW+16)
            protect(f)
            stage=f.repo.stage_action
            def raced(action,*,fence):
                controls.control(f.repo,'HARD_STOP','concurrent-hard-stop',at=NOW+16)
                return stage(action,fence=fence)
            with patch.object(f.repo,'stage_action',raced):
                reject('captured_fence_rejects_concurrent_hard_stop',lambda:sell(f))
            check('race_did_not_allocate_action',not snapshot(f).claiming_actions)
        finally:
            f.close()

        f=b2.Fixture(directory,'competing-action')
        try:
            f.evaluate(at=NOW+16)
            original=protect(f)
            external=PendingAction(f.action.root_id,f.binding.candidate.content_digest,'SELL',f.binding.mint,
                f.action.token_program,f.action.position_id,original.remaining_units,'EXTERNAL_ACCEPTED_TERMS',content_fingerprint('external'),
                f.binding.policy_ref,f.binding.policy_digest,f.binding.spec.track_id,original.obligation_id,1)
            f.repo.stage_action(external,fence=f.repo.write_fence())
            check('other_existing_action_is_actual_unit_claim',snapshot(f).encumbered_units == original.remaining_units)
            reject('cannot_double_allocate_external_staged_claim',lambda:sell(f))
        finally:
            f.close()

        f=b2.Fixture(directory,'atomic-handoff')
        try:
            f.evaluate(at=NOW+16)
            before=f.repo.consumer_snapshot()['consumer_cut']
            with patch.object(f.repo,'_append_commit',side_effect=RuntimeError('synthetic atomic interruption')):
                reject('handoff_atomic_failure',lambda:protect(f))
            check('failed_handoff_leaves_units_unusable_and_no_obligation',f.repo.consumer_snapshot()['consumer_cut'] == before
                and f.repo.protection(f.action.position_id) is None and not f.repo.position_history(f.action.position_id).usable)
            original=protect(f)
            f.reopen_ledger()
            check('retried_handoff_has_one_stable_identity',snapshot(f).handoff == original.handoff)
        finally:
            f.close()

        f=b2.Fixture(directory,'external-due-not-runtime')
        try:
            f.evaluate(at=NOW+14)
            fake=ports.handoff(f.repo,f.action,f.prep,f.chain,f.support,due=True)
            f.repo.record_protective_handoff(fake,ingestion_key='external-fixture',fence=f.repo.write_fence())
            reject('external_due_handoff_is_not_authentic_B3_trigger',lambda:sell(f))
        finally:
            f.close()

        f=b2.Fixture(directory,'cancelled-then-applied')
        try:
            f.evaluate(at=NOW+16)
            original=protect(f)
            action=sell(f,max_units=original.remaining_units//3)
            with patch.object(f.repo,'record_external_attempt_stage',side_effect=RuntimeError('fixture stops after preparation')):
                reject('fixture_halts_before_any_signed_stage',lambda:reduce_actual(f,action,number=50))
            attempts=f.repo.protective_position_context(f.action.position_id,f.binding.binding_id)['attempts']
            prepared=next(a for a in attempts if a.preparation.action_id == action.action_id)
            cancelled=f.repo.cancel_attempt_locally(prepared.preparation.attempt_id,
                recorded_at_utc=prepared.preparation.prepared_at_utc,idempotency_key='accepted-local-cancel',
                expected_attempt_revision=prepared.revision,fence=f.repo.write_fence())
            check('cancelled_unsigned_alone_does_not_release_B3_action',cancelled.recorded_stage == 'CANCELLED_UNSIGNED'
                and snapshot(f).encumbered_units == action.input_units and sell(f) == action)
            # The existing accepted Ledger path supplies a later authoritative
            # successful outcome. B3 only reads this history; it schedules no
            # retry and produces no permission for the replacement attempt.
            _,_,prep,_,support,at=cf.next_step(f.repo,f.market,f.support.observation.anchor,
                number=51,quantity=action.input_units,position_id=action.position_id,
                protective_handoff=original.handoff,existing_action=action)
            applied=cf.apply(f.repo,prep,support,chain='chain-51',key='accepted-partial',at=at+20)
            current=snapshot(f)
            check('cancelled_predecessor_then_actual_applied_reduction_releases_claim',applied.decision.disposition == 'FINALIZED_SUCCESS_APPLIED'
                and current.encumbered_units == 0 and current.remaining_units == original.remaining_units-action.input_units)
            f.reopen_ledger()
            check('cancelled_predecessor_positive_history_reopens',snapshot(f).remaining_units == current.remaining_units
                and snapshot(f).encumbered_units == 0 and snapshot(f).obligation_id == original.obligation_id)
        finally:
            f.close()

    print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(CHECKS),
        'check_digest':content_fingerprint(CHECKS),'all_checks':True},sort_keys=True))


if __name__ == '__main__':
    main()
