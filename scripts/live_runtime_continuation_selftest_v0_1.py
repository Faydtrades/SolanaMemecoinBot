"""C3 actual first lifecycle, Ledger retirement, fresh canonical second admission.

Only isolated SQLite stores, ephemeral signer and deterministic external RPC facts.
No invented economic/candidate records, rearming or Runtime reconstruction.
"""
from __future__ import annotations
import ast
import tempfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import live_runtime_composition_selftest_v0_1 as c2
from live.authority_admission_v0_1 import EntryRequest
from live.candidate_handoff_v0_1 import CandidateHandoffV01
from live.ledger_repository_v0_1 import LedgerRepository
from live.ledger_actions_v0_1 import utc_microseconds

a3, a5, sf, hf, NOW = c2.a3, c2.a5, c2.sf, c2.hf, c2.NOW
CHECKS = {}
EVIDENCE = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def wallet(f, at, *, mint=None, incomplete=False):
    snapshot = f.repo.consumer_snapshot()
    expected = {a.pubkey:(a.mint,a.program) for a in snapshot['accounts']}
    public = dict(f.public)
    if mint is not None:
        base = a3.derive_associated_token_address(f.domain.wallet,mint,sf.TOKEN_PROGRAM_ID)
        expected[base] = (mint,sf.TOKEN_PROGRAM_ID)
        public[mint] = a5.PublicAccount(a5.RpcAccountV01(mint,sf.TOKEN_PROGRAM_ID,a3.mint_data(6)),1000000000,False,0)
    if incomplete:
        expected = {}
    request = a3.WalletEvidenceRequest(f.domain.wallet,f.domain.genesis_hash,snapshot['required_wallet_context_slot'],
        tuple(a3.ExpectedTokenAccount(k,m,p) for k,(m,p) in sorted(expected.items())))
    observation = a3.observe(a5.wallet_scenario(public,f.lower),request=request,at=at)
    return a3.WalletSupportInput(observation,a3.utc(at),request.min_context_slot)


def held_retirement(clock, support):
    """Retirement compares the support's evaluation stamp with the step's own
    clock sample (Ledger retirement comparison input; Runtime unacquired
    retirement). A ticking fixture clock cannot be predicted from outside the
    step, so sample it once, hold that sample for the whole step, and stamp the
    support with it, as production-owned retirement steps do with a fixed sample.
    """
    sample = clock()
    return (lambda: sample), replace(support, evaluated_at_utc=sample.utc_upper_utc)


def reduce_first(f, key):
    c2.acquisition(f,key)
    staged = f.step(NOW+16)
    check('actual_due_protective_SELL_staged',staged.work=='PROTECTIVE_ACTION_STAGED')
    sell = f.repo.action(staged.action_id)
    submitted,_,sends = c2.execution(f,key,sell,at=NOW+20,number=91)
    check('actual_original_SELL_submitted',submitted.work=='SUBMISSION_OBSERVED' and len(sends.requests)==1)
    _,actual,scenario,pre = c2.original_chain(f,submitted,NOW+20)
    reconciled,_ = c2.reconcile(f,scenario,NOW+26)
    support = c2.application_support(f,actual,f.repo.chain_receipt(reconciled.receipt_key),pre,NOW+28)
    native_before = f.repo.consumer_snapshot()['funding'].native_lamports
    applied = f.step(NOW+28,application_wallet=support)
    native_delta = f.repo.application_receipt(applied.receipt_key).decision.proposal.native_wallet_delta
    EVIDENCE.update(native_before_SELL_application=native_before,actual_SELL_native_delta=native_delta,
        native_after_SELL_application=f.repo.consumer_snapshot()['funding'].native_lamports)
    check('actual_SELL_application_recycles_native_funding',native_delta>0
        and EVIDENCE['native_after_SELL_application']==native_before+native_delta
        and EVIDENCE['native_after_SELL_application']==f.public[f.domain.wallet].lamports)
    binding = f.runtime.position_binding
    check('actual_full_reduction_application_precedes_retirement',applied.reason=='FINALIZED_SUCCESS_APPLIED'
        and c2.protective_outcome(f.repo,binding).state=='SATISFIED'
        and f.repo.reservation(f.buy.root_id).status=='RESERVED')
    return applied, binding


def continuation(directory):
    key = c2.Keypair()
    with patch.object(sf,'WALLET',str(key.pubkey())),patch.object(sf.plans,'ACTOR',str(key.pubkey())):
        f = c2.Fixture(directory,'c3-continuation')
        try:
            applied,binding = reduce_first(f,key)
            original_item, original_buy = f.item, f.buy
            original_authority = f.repo.authority_snapshot()
            original_acceptance = f.repo.authority_acceptance(f.buy.root_id)
            before = f.repo.audit()
            missing = f.step(NOW+29)
            check('missing_retirement_support_holds_current_capacity',missing.work=='NEED_RETIREMENT'
                and f.repo.audit()==before and f.repo.consumer_snapshot()['reservations'])
            support = wallet(f,NOW+29)
            stale = replace(support,required_min_context_slot=100)
            check('pre_settlement_wallet_floor_rejected',c2.fails(lambda:f.step(NOW+29,retirement_wallet=stale)))
            check('rejected_retirement_does_not_release_or_clear_binding',f.repo.audit()==before
                and f.runtime.position_binding==binding and f.repo.consumer_snapshot()['positions'])
            incomplete = f.step(NOW+29,retirement_wallet=wallet(f,NOW+29,incomplete=True))
            check('incomplete_wallet_withholds_retirement',incomplete.work=='RETIREMENT_WITHHELD'
                and f.repo.reservation(f.buy.root_id).status=='RESERVED' and f.runtime.position_binding==binding)
            retired = f.step(NOW+30,retirement_wallet=wallet(f,NOW+30))
            receipt = f.repo.port_receipt(retired.receipt_key)
            current = f.repo.consumer_snapshot()
            check('actual_Ledger_retirement_releases_only_current_capacity',retired.work=='RETIREMENT_RETIRED'
                and receipt.retirement_disposition=='RETIRED' and not current['positions'] and not current['reservations']
                and not current['pending_attempts'] and f.runtime.position_binding is None
                and f.repo.position_history(original_buy.position_id).status=='RETIRED'
                and f.repo.position_history(original_buy.position_id).sold_units==f.repo.position_history(original_buy.position_id).acquired_units)
            check('retirement_is_bound_to_actual_terminal_application',receipt.retirement.terminal_attempt_id==applied.attempt_id
                and receipt.retirement.position_id==binding.position_id
                and receipt.sequence>f.repo.application_receipt(applied.receipt_key).sequence)
            repeated = f.repo.retire(receipt.retirement,wallet(f,NOW+30),ingestion_key=retired.receipt_key,fence=f.repo.write_fence())
            check('exact_retirement_replay_is_idempotent',repeated==receipt)
            # New source rows describe a distinct mint whose original signal is
            # generated after retirement. Existing rows/candidates stay untouched.
            fresh_rows = [(n+32,hf.MINT_C,kind,price) for n,_,kind,price in hf.FIRST if n not in (5,7)]
            f.append(fresh_rows)
            for _ in range(128):
                candidate = f.runtime.step(clock=lambda:a3.clock(f.repo,NOW+36),source_cut_utc=a3.utc(NOW+32))
                if candidate.work=='NEED_ENTRY_FACTS':
                    break
            check('fresh_distinct_canonical_candidate_after_retirement',candidate.work=='NEED_ENTRY_FACTS'
                and candidate.root_id!=original_buy.root_id)
            second = f.repo.candidate(candidate.root_id)
            check('candidate2_original_producer_identity_and_signal_time',second.mint!=original_buy.mint
                and second.generated_at_us==(NOW+32)*1000000
                and second.trade_root(f.domain)==candidate.root_id
                and second.producer_lineage_id==original_item.producer_lineage_id)
            admitted = f.runtime.step(clock=lambda:a3.clock(f.repo,NOW+36),source_cut_utc=a3.utc(NOW+32),
                entry=c2.EntryFacts('PUMP',sf.TOKEN_PROGRAM_ID,wallet(f,NOW+36,mint=second.mint)))
            admission = f.repo.authority_admission_receipt(admitted.receipt_key)
            check('candidate2_actual_Authority_admission',admitted.work=='ENTRY_ADMITTED' and admission.accepted)
            action2 = f.repo.action(admitted.action_id)
            check('candidate2_new_action_preserves_original_deadline',action2.side=='BUY' and action2.action_id!=original_buy.action_id
                and action2.root_id==second.trade_root(f.domain) and action2.candidate_digest==second.content_digest
                and action2.claimed_entry_deadline_us==admission.eligibility.decision.binding.deadline_us
                and action2.claimed_entry_deadline_us==second.generated_at_us+15000000
                and receipt.sequence<admission.sequence)
            check('current_source_genuinely_covers_candidate2',f.source.latest_record()[1].disposition=='HEALTHY'
                and utc_microseconds(f.source.latest_record()[1].covered_through_utc)==(NOW+32)*1000000)
            check('same_original_policy_and_recurring_grant_without_rearming',all(f.repo.authority_snapshot()[name]==original_authority[name]
                    for name in ('policy','armed_entry_grant','entry_stop_command','hard_stop_command'))
                and admission.consumed_grant_id==original_acceptance.consumed_grant_id)
            check('candidate2_uses_actual_retired_wallet_funding',f.repo.consumer_snapshot()['funding'].native_lamports
                ==current['funding'].native_lamports==EVIDENCE['native_after_SELL_application']==admission.risk.native_lamports
                and admission.risk.known_wsol_units==0 and admission.risk.outstanding_native_lamports==0)
            EVIDENCE.update(retirement_sequence=receipt.sequence,second_admission_sequence=admission.sequence,
                second_signal_us=second.generated_at_us,second_original_deadline_us=action2.claimed_entry_deadline_us,
                second_admission_native_lamports=admission.risk.native_lamports)
            check('history_is_not_current_capacity',len(f.repo.consumer_snapshot()['reservations'])==1
                and f.repo.consumer_snapshot()['reservations'][0].root_id==action2.root_id
                and f.repo.audit()['action_count']==3 and f.repo.audit()['attempt_count']==2
                and f.repo.candidate(original_buy.root_id)==original_item and f.repo.action(original_buy.action_id)==original_buy)
            # Ledger-only reopen audits durable economics; no fresh Runtime is
            # constructed and no execution continuation/recovery is claimed.
            prior = hf.economic_audit(f.repo)
            f.repo.close()
            f.repo = LedgerRepository.reopen(f.path,f.domain)
            replay = CandidateHandoffV01(f.producer,f.repo,f.binding,database_identity=hf.DATABASE_ID)
            roots = []
            for _ in range(128):
                page = replay.deliver_page(limit=32)
                roots.extend(page.candidate_roots)
                if not page.scanned_rows:break
            check('duplicate_delivery_after_Ledger_reopen_keeps_two_canonical_roots',set(roots)=={original_buy.root_id,action2.root_id}
                and hf.economic_audit(f.repo)==prior and f.repo.authority_acceptance(original_buy.root_id)==original_acceptance
                and f.repo.port_receipt(retired.receipt_key)==receipt and f.repo.action(action2.action_id)==action2)
            for name,item in (('first',original_item),('second',second)):
                denied = f.repo.admit_authority_entry(EntryRequest(item.trade_root(f.domain),'PUMP',sf.TOKEN_PROGRAM_ID,f.policy.selected_track),
                    a3.clock(f.repo,NOW+36),f.source,wallet(f,NOW+36,mint=item.mint),
                    command_id='c3-duplicate-'+name,fence=f.repo.write_fence())
                check(name+'_duplicate_cannot_create_another_economic_action',not denied.accepted and f.repo.audit()['action_count']==3)
            check('final_Ledger_integrity',bool(f.repo.audit()))
        finally:
            f.close()


def expired_original(directory):
    f = c2.Fixture(directory,'c3-expired-original')
    try:
        f.candidate_ready()
        f.configure()
        original = f.item
        for at in (NOW+20,NOW+21):
            if at==NOW+20:
                result = f.step(at,entry=c2.EntryFacts('PUMP',sf.TOKEN_PROGRAM_ID,a3.wallet(f,at=at,scenario=f.scenario)))
                denied = f.repo.authority_admission_receipt(result.receipt_key)
                check('Runtime_denies_original_expired_queued_candidate',result.work=='ENTRY_DENIED')
            else:
                denied = f.repo.admit_authority_entry(EntryRequest(f.root,'PUMP',sf.TOKEN_PROGRAM_ID,f.policy.selected_track),
                    a3.clock(f.repo,at),f.source,a3.wallet(f,at=at,scenario=f.scenario),
                    command_id='expired-redelivery',fence=f.repo.write_fence())
            check('expired_original_never_renewed_'+str(at-NOW),not denied.accepted
                and denied.eligibility.decision.disposition=='EXPIRED'
                and denied.eligibility.decision.binding.deadline_us==original.generated_at_us+15000000
                and f.repo.candidate(f.root)==original and f.repo.audit()['action_count']==0)
    finally:
        f.close()


def structural():
    tree = ast.parse((c2.ROOT/'src/live/runtime_composition_v0_1.py').read_text(encoding='utf-8-sig'))
    calls = {n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    check('retirement_uses_existing_Ledger_semantic_owner','retire' in calls
        and not calls & {'initialize','reopen','finish_dry','record_retirement','receive_candidate','commit'})


def main():
    with tempfile.TemporaryDirectory(prefix='live-runtime-c3-') as tmp:
        continuation(Path(tmp))
        expired_original(Path(tmp))
        structural()
    print(c2.canonical_json({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(CHECKS),
        'reused_C2_checks':len(c2.CHECKS),'all_checks':all(CHECKS.values()) and all(c2.CHECKS.values()),'evidence':EVIDENCE}))


if __name__=='__main__':main()
