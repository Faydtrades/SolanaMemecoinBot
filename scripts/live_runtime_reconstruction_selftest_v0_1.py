"""C4 cold owner factories + actual Runtime steps, isolated deterministic I/O."""
from __future__ import annotations

import ast
import copy
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import live_runtime_composition_selftest_v0_1 as c2
import live_runtime_continuation_selftest_v0_1 as c3
import live_runtime_dry_selftest_v0_1 as c1
from live.runtime_reconstruction_v0_1 import reopen_live
from live.runtime_dry_reconstruction_v0_1 import reopen_dry
from live.continuous_producer_v0_2 import ContinuationProfileV02
from live.exit_observation_v0_1 import commit_exit_evaluation
from live.protective_obligation_v0_1 import ensure_protective_obligation, stage_protective_sell

a3, sf, hf, NOW = c2.a3, c2.sf, c2.hf, c2.NOW
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def cold_args(f):
    return dict(producer_path=f.ppath,
        market_source=hf.ContinuousMarketSourceV02(f.raw, start_after_p1_rowid=1, database_identity=hf.DATABASE_ID),
        producer_profile=ContinuationProfileV02(), source_path=f.directory/(f.name+'-evidence.sqlite'),
        source_binding=f.binding, source_profile=c2.SourceProfile(), database_identity=hf.DATABASE_ID,
        batch_rows=32, page_rows=1, queued_roots=2)


def close_handles(f):
    if hasattr(f.runtime, 'close'):
        f.runtime.close()
    else:
        f.close()


def cold(f, *, damage=None, **overrides):
    # Only external fixture configuration survives. No old Runtime pointer or
    # owner object is supplied to the actual cold factory.
    close_handles(f)
    if damage is not None:
        damage()
    args = cold_args(f)
    args.update(overrides)
    f.runtime = reopen_live(f.path, f.domain, **args)
    f.repo, f.producer, f.handoff, f.source = f.runtime.ledger, f.runtime.producer, f.runtime.handoff, f.runtime.source
    f.conn = f.runtime._producer_conn
    return f.runtime.reconstruction_facts()


def pending_and_consumed(directory):
    f = c2.Fixture(directory, 'c4-empty')
    try:
        facts = cold(f)
        check('cold_no_position_from_original_baseline',facts.source_state=='SOURCE_RECONSTRUCTED'
            and facts.position_id is None and facts.entry_action_id is None and not facts.grants_permission
            and facts.funding.native_lamports==f.domain.known_native_wallet_lamports)
        first = f.candidate_ready()
        original = f.repo.candidate(first.root_id)
        facts = cold(f)
        check('cold_pending_handoff_original_root_queued',facts.queued_roots==(first.root_id,)
            and f.repo.candidate(first.root_id)==original)
        check('cold_pending_actual_step_requests_original_entry',f.step().work=='NEED_ENTRY_FACTS')
        f.configure()
        a3.control(f.repo,'STOP_ENTRY','c4-deny',at=NOW+4)
        denied = f.step(entry=c2.EntryFacts('PUMP',sf.TOKEN_PROGRAM_ID,a3.wallet(f,scenario=f.scenario)))
        check('actual_denial_consumes_candidate',denied.work=='ENTRY_DENIED')
        facts = cold(f)
        check('cold_denied_root_not_requeued',facts.consumed_roots_seen==1 and not facts.queued_roots
            and f.step().work=='SOURCE_ADVANCED' and f.repo.audit()['action_count']==0)
        with closing(sqlite3.connect(f.raw)) as raw, raw:
            raw.execute('INSERT INTO gap_jobs_v034 VALUES(?,?,?,?,?,?,?,?,?)',
                ('c4-retained-gap',a3.utc(NOW),a3.utc(NOW+1),'PENDING',None,None,0,0,None))
        f.step()
        retained = f.source.latest_record()[1]
        facts = cold(f)
        check('cold_source_reconstruction_retains_historical_GAP_not_permission',facts.source_state=='SOURCE_RECONSTRUCTED'
            and facts.retained_source_disposition==retained.disposition=='GAP'
            and facts.retained_source_digest==retained.content_digest
            and facts.retained_source_observed_at_utc==retained.snapshot.observed_at_utc and not facts.grants_permission)
    finally:
        close_handles(f)

    f = c2.Fixture(directory,'c4-pending-ACK')
    try:
        f.produce()
        class Interrupted(BaseException):
            pass
        def interrupt(point):
            if point=='after_ledger_commit':
                raise Interrupted()
        f.handoff.failure_injector = interrupt
        try:
            for _ in range(128):
                f.handoff.deliver_page(limit=1)
        except Interrupted:
            pass
        else:
            raise AssertionError('actual inbox-before-ACK cut not reached')
        before = hf.economic_audit(f.repo)
        facts = cold(f)
        result = f.step()
        check('cold_pending_original_inbox_before_ACK_replays_actual_handoff',len(facts.queued_roots)==1
            and result.work=='NEED_ENTRY_FACTS' and result.root_id==facts.queued_roots[0]
            and hf.economic_audit(f.repo)==before
            and f.conn.execute(f'SELECT count(*) FROM {hf.TABLES[5]} WHERE delivered=1').fetchone()[0]==1)
    finally:
        close_handles(f)


def admitted_and_sign_cut(directory):
    key = c2.Keypair()
    with patch.object(sf,'WALLET',str(key.pubkey())), patch.object(sf.plans,'ACTOR',str(key.pubkey())):
        f = c2.Fixture(directory,'c4-sign-cut')
        try:
            f.admit()
            original = f.buy
            facts = cold(f)
            held = f.step(NOW+5)
            check('cold_admitted_original_action_awaits_execution',facts.entry_action_id==original.action_id
                and held.work=='NEED_EXECUTION' and held.action_id==original.action_id)
            class Interrupted(BaseException):
                pass
            with patch.object(c2.AutonomousLocalSigner,'sign_exact',side_effect=Interrupted):
                try:
                    c2.execution(f,key,original,at=NOW+6,number=101)
                except Interrupted:
                    pass
                else:
                    raise AssertionError('original SIGN cut not reached')
            attempt = f.repo._root_attempts(original.root_id)[0]
            facts = cold(f)
            result = f.step(NOW+60)
            check('cold_consumed_SIGN_keeps_original_unsigned_attempt',facts.pending_attempt_id==attempt.preparation.attempt_id
                and result.work=='HELD' and result.attempt_id==facts.pending_attempt_id
                and f.repo.audit()['attempt_count']==1 and f.repo.attempt(result.attempt_id).primary_signature is None)
        finally:
            close_handles(f)


def lifecycle(directory):
    key = c2.Keypair()
    with patch.object(sf,'WALLET',str(key.pubkey())), patch.object(sf.plans,'ACTOR',str(key.pubkey())):
        f = c2.Fixture(directory,'c4-lifecycle')
        try:
            actual = c2.acquisition(f,key)
            original_binding = f.runtime.position_binding
            facts = cold(f)
            check('cold_acquired_original_B1_without_process_pointer',f.runtime.position_binding==original_binding
                and facts.remaining_units==actual.actual_base and facts.protective_state is None)
            check('cold_acquired_actual_step_establishes_monitoring',f.step(NOW+14).work=='ENTRY_HELD'
                and c2.protective_outcome(f.repo,f.runtime.position_binding).state=='MONITORING')
            evaluation = commit_exit_evaluation(f.repo,f.runtime.position_binding,
                command_id='c4-original-due',timer_fence_utc=a3.utc(NOW+16))
            obligation = ensure_protective_obligation(f.repo,f.runtime.position_binding,recorded_at_utc=a3.utc(NOW+16))
            # Accepted B3 sizing port establishes an actual partial reduction.
            partial = stage_protective_sell(f.repo,f.runtime.position_binding,max_units=actual.actual_base//3)
            facts = cold(f)
            held = f.step(NOW+17)
            check('cold_due_original_B2_B3_action_consumed',facts.exit_cut_digest==evaluation.content_digest
                and facts.obligation_id==obligation.obligation_id and held.work=='NEED_EXECUTION'
                and held.action_id==partial.action_id)
            submitted,_,_ = c2.execution(f,key,partial,at=NOW+20,number=102)
            _,sold,scenario,pre = c2.original_chain(f,submitted,NOW+20)
            reconciled,_ = c2.reconcile(f,scenario,NOW+26)
            chain = f.repo.chain_receipt(reconciled.receipt_key)
            support = c2.application_support(f,sold,chain,pre,NOW+28)
            facts = cold(f)
            awaiting = f.step(NOW+28)
            check('cold_positive_chain_uses_original_application_path_without_rpc',facts.chain_receipt_key==chain.ingestion_key
                and awaiting.work=='NEED_APPLICATION' and awaiting.attempt_id==submitted.attempt_id)
            applied = f.step(NOW+28,application_wallet=support)
            check('cold_actual_partial_application',applied.reason=='FINALIZED_SUCCESS_APPLIED')
            facts = cold(f)
            before = f.repo.audit()['action_count']
            residual = f.step(NOW+29)
            remaining = actual.actual_base-partial.input_units
            action = f.repo.action(residual.action_id)
            check('cold_partial_residual_actual_step_stages_only_actual_remaining',facts.remaining_units==remaining
                and facts.protective_state=='STAGE_ACTION' and residual.work=='PROTECTIVE_ACTION_STAGED'
                and action.input_units==remaining and action.ordinal==2
                and action.obligation_id==partial.obligation_id and f.repo.audit()['action_count']==before+1)
            # This new action is the accepted B3 residual decision after cold,
            # not a replacement of any unresolved original action or attempt.
            submitted,_,_ = c2.execution(f,key,action,at=NOW+32,number=103,outcome='TRANSPORT_UNRESOLVED')
            _,sold,scenario,pre = c2.original_chain(f,submitted,NOW+32)
            unknown = copy.deepcopy(scenario)
            unknown.tx = unknown.status = None
            reconciled,_ = c2.reconcile(f,unknown,NOW+38)
            signature = f.repo.attempt(submitted.attempt_id).primary_signature
            facts = cold(f)
            audit = f.repo.audit()
            held = f.step(NOW+39)
            check('cold_UNKNOWN_time_and_absence_never_resolve_or_replace',facts.pending_attempt_id==submitted.attempt_id
                and held.work=='NEED_RECONCILIATION' and held.attempt_id==submitted.attempt_id
                and f.repo.audit()==audit and f.repo.attempt(submitted.attempt_id).primary_signature==signature
                and f.repo.position_history(action.position_id).remaining_units==remaining)
            # Resume with real positive original public evidence (no timeout truth).
            reconciled,_ = c2.reconcile(f,scenario,NOW+39)
            support = c2.application_support(f,sold,f.repo.chain_receipt(reconciled.receipt_key),pre,NOW+40)
            applied = f.step(NOW+40,application_wallet=support)
            facts = cold(f)
            held = f.step(NOW+41)
            check('cold_SATISFIED_retains_capacity_until_actual_retirement',facts.protective_state=='SATISFIED'
                and facts.remaining_units==0 and held.work=='NEED_RETIREMENT'
                and f.repo.consumer_snapshot()['reservations'])
            retired = f.step(NOW+42,retirement_wallet=c3.wallet(f,NOW+42))
            check('cold_actual_retirement_releases_capacity',retired.work=='RETIREMENT_RETIRED')
            original_root = f.buy.root_id
            facts = cold(f)
            check('cold_retired_root_is_history_not_capacity_or_queue',facts.position_id is None
                and facts.entry_action_id is None and facts.pending_attempt_id is None
                and original_root not in facts.queued_roots and facts.consumed_roots_seen==1)
            fresh_rows = [(n+48,hf.MINT_C,kind,price) for n,_,kind,price in hf.FIRST if n not in (5,7)]
            f.append(fresh_rows)
            for _ in range(128):
                result = f.runtime.step(clock=lambda:a3.clock(f.repo,NOW+52),source_cut_utc=a3.utc(NOW+48))
                if result.work=='NEED_ENTRY_FACTS':
                    break
            candidate = f.repo.candidate(result.root_id)
            admitted = f.runtime.step(clock=lambda:a3.clock(f.repo,NOW+52),source_cut_utc=a3.utc(NOW+48),
                entry=c2.EntryFacts('PUMP',sf.TOKEN_PROGRAM_ID,c3.wallet(f,NOW+52,mint=candidate.mint)))
            check('cold_retired_then_actual_new_canonical_candidate_admission',admitted.work=='ENTRY_ADMITTED'
                and admitted.root_id!=original_root and candidate.producer_lineage_id==facts.producer_lineage_id
                and f.repo.action(admitted.action_id).claimed_entry_deadline_us==candidate.generated_at_us+15000000)
        finally:
            close_handles(f)


def unhealthy_and_corrupt(directory):
    key = c2.Keypair()
    with patch.object(sf,'WALLET',str(key.pubkey())), patch.object(sf.plans,'ACTOR',str(key.pubkey())):
        f = c2.Fixture(directory,'c4-source-corrupt')
        try:
            c2.acquisition(f,key)
            f.step(NOW+14)
            original = f.runtime.position_binding
            def corrupt_checkpoint():
                with closing(sqlite3.connect(f.ppath)) as conn, conn:
                    conn.execute("UPDATE live_producer_checkpoint_v0_1 SET manifest_digest=?", ('0'*64,))
            facts = cold(f,damage=corrupt_checkpoint)
            result = f.step(NOW+16)
            check('actual_failed_A2_reopen_preserves_Ledger_protection_lane',facts.source_state=='SOURCE_RECONSTRUCTION_UNAVAILABLE'
                and f.producer is None and f.runtime.position_binding==original
                and facts.protective_state=='MONITORING' and result.work=='PROTECTIVE_ACTION_STAGED')
            check('failed_A2_cold_actual_protective_execution_waits_original_action',f.step(NOW+17).work=='NEED_EXECUTION'
                and f.repo.audit()['action_count']==2)
        finally:
            close_handles(f)
    f = c2.Fixture(directory,'c4-missing-source')
    try:
        f.admit()
        missing = directory/'never-created-source.sqlite'
        facts = cold(f,source_path=missing)
        result = f.step(NOW+6)
        check('missing_source_history_not_initialized_and_ENTRY_blocked',facts.source_state=='SOURCE_RECONSTRUCTION_UNAVAILABLE'
            and not missing.exists() and result.work=='SOURCE_HELD' and result.action_id==f.buy.action_id
            and f.repo.audit()['attempt_count']==0)
    finally:
        close_handles(f)
    f = c2.Fixture(directory,'c4-economic-corrupt')
    args = cold_args(f)
    close_handles(f)
    with closing(sqlite3.connect(f.path)) as conn, conn:
        conn.execute("UPDATE ledger_funding_projection SET content_digest=?", ('0'*64,))
    check('economic_corruption_fails_closed_without_Runtime',c2.fails(lambda:reopen_live(f.path,f.domain,**args)))


def active_replay_priority(directory):
    key = c2.Keypair()
    with patch.object(sf,'WALLET',str(key.pubkey())), patch.object(sf.plans,'ACTOR',str(key.pubkey())):
        f = c2.Fixture(directory,'c4-active-replay-priority')
        try:
            c2.acquisition(f,key)
            f.append(hf.NEXT)
            f.produce()
            with f.producer._transaction():
                rows = f.producer.producer_events(limit=256)
                candidates = [f.handoff._candidate(row) for row in rows if row['event_type']=='CandidateEvaluationEvent']
                original = next(candidate for candidate in candidates if candidate is not None and candidate.mint==hf.MINT_C)
            pending_root = original.trade_root(f.domain)
            calls = []
            deliver = hf.CandidateHandoffV01.deliver_page
            def observed_delivery(handoff, **kwargs):
                page = deliver(handoff, **kwargs)
                calls.append(page)
                return page
            # Spy on the real A3 port; no semantic callback or replacement result.
            with patch.object(hf.CandidateHandoffV01,'deliver_page',observed_delivery):
                facts = cold(f)
                check('cold_active_economics_defers_all_initial_candidate_delivery',not calls
                    and facts.initial_handoff_replay_deferred and not facts.queued_roots
                    and f.handoff._after==-1 and f.repo.candidate(pending_root) is None
                    and f.producer.producer_events(limit=256)==rows)
                staged = f.step(NOW+16)
                check('cold_due_dispatch_precedes_original_pending_candidate_replay',staged.work=='PROTECTIVE_ACTION_STAGED'
                    and not calls and f.handoff._after==-1)
                sell = f.repo.action(staged.action_id)
                submitted,_,_ = c2.execution(f,key,sell,at=NOW+20,number=104)
                _,sold,scenario,pre = c2.original_chain(f,submitted,NOW+20)
                reconciled,_ = c2.reconcile(f,scenario,NOW+26)
                support = c2.application_support(f,sold,f.repo.chain_receipt(reconciled.receipt_key),pre,NOW+28)
                applied = f.step(NOW+28,application_wallet=support)
                retired = f.step(NOW+29,retirement_wallet=c3.wallet(f,NOW+29))
                check('cold_truth_application_retirement_keep_priority_over_replay',not calls
                    and applied.reason=='FINALIZED_SUCCESS_APPLIED' and retired.work=='RETIREMENT_RETIRED')
                for _ in range(128):
                    result = f.step(NOW+30)
                    if result.work=='NEED_ENTRY_FACTS':
                        break
                check('deferred_original_candidate_replayed_by_later_bounded_steps',result.work=='NEED_ENTRY_FACTS'
                    and result.root_id==pending_root and f.repo.candidate(pending_root)==original
                    and calls and all(page.scanned_rows<=1 for page in calls)
                    and calls[0].after_sequence==-1)
        finally:
            close_handles(f)


def dry(directory):
    f,_,_,action,seed = c1.fixture(directory,'c4-dry-terminal')
    try:
        terminal,_ = c1.run(f,action,seed)
        before = hf.economic_audit(f.repo)
        f.repo.close()
        runtime = reopen_dry(f.path,f.domain,action.action_id)
        f.repo = runtime.ledger
        facts = runtime.reconstruction_facts()
        repeated = runtime.step(recorded_at_utc=a3.utc(NOW+60))
        check('cold_DRY_terminal_original_identity_and_capacity_release',facts.action_id==action.action_id
            and facts.root_id==action.root_id and facts.disposition=='NON_SUBMITTED'
            and not facts.tentative_capacity_held and not facts.grants_permission
            and repeated==terminal and hf.economic_audit(f.repo)==before)
    finally:
        f.close()

    for target, reason in (('prepare_attempt','DRY_INTERRUPTED_NO_SIGNED_GRAPH'),
                           ('record_external_attempt_stage','DRY_CAPABILITY')):
        f,_,_,action,seed = c1.fixture(directory,'c4-dry-cut-'+target)
        try:
            original = getattr(f.repo,target)
            def interrupt(*args, **kwargs):
                original(*args, **kwargs)
                raise c1.Interrupted()
            try:
                with patch.object(f.repo,target,side_effect=interrupt):
                    c1.run(f,action,seed)
            except c1.Interrupted:
                pass
            else:
                raise AssertionError('actual DRY durable cut not reached')
            preparation = f.repo._root_attempts(action.root_id)[-1].preparation
            f.repo.close()
            runtime = reopen_dry(f.path,f.domain,action.action_id)
            f.repo = runtime.ledger
            facts = runtime.reconstruction_facts()
            terminal = runtime.step(recorded_at_utc=a3.utc(NOW+20))
            check('cold_DRY_'+target+'_original_interruption_consumer',facts.attempt_id==preparation.attempt_id
                and terminal.dry_terminal.attempt_id==preparation.attempt_id and terminal.dry_terminal.reason==reason
                and f.repo.attempt(preparation.attempt_id).preparation==preparation
                and not f.repo.consumer_snapshot()['reservations'] and f.repo.audit()['attempt_count']==1)
        finally:
            f.close()
    f,_,_,action,seed = c1.fixture(directory,'c4-dry-interrupted')
    try:
        f.repo.close()
        runtime = reopen_dry(f.path,f.domain,action.action_id)
        f.repo = runtime.ledger
        facts = runtime.reconstruction_facts()
        terminal = runtime.step(recorded_at_utc=seed.clock.utc_upper_utc)
        check('cold_DRY_interruption_uses_existing_C1_terminal_contract',facts.tentative_capacity_held
            and facts.attempt_id is None and terminal.kind=='DRY_NON_SUBMITTED'
            and terminal.dry_terminal.reason=='DRY_INTERRUPTED_BEFORE_PREPARATION'
            and not f.repo.consumer_snapshot()['reservations'] and f.repo.audit()['attempt_count']==0)
    finally:
        f.close()


def structural():
    live = ast.parse((c2.ROOT/'src/live/runtime_reconstruction_v0_1.py').read_text())
    calls = {n.func.attr for n in ast.walk(live) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    check('cold_no_install_reset_cursor_or_operational_mutation',not calls & {'initialize','kill','terminate','from_seed','load_keypair','commit'})
    for path in ('runtime_reconstruction_v0_1.py','runtime_dry_reconstruction_v0_1.py'):
        tree = ast.parse((c2.ROOT/'src/live'/path).read_text())
        imports = {n.module for n in ast.walk(tree) if isinstance(n,ast.ImportFrom)}
        check(path+'_no_fixture_or_injected_restore_semantics',not any('selftest' in (name or '') for name in imports))
    tree = ast.parse((c2.ROOT/'src/live/runtime_dry_reconstruction_v0_1.py').read_text())
    imports = {n.module for n in ast.walk(tree) if isinstance(n,ast.ImportFrom)}
    check('cold_DRY_graph_separate_from_LIVE_sign_send',not imports & {'runtime_composition_v0_1',
        'runtime_reconstruction_v0_1','execution_signer_v0_1','execution_send_v0_1','execution_reconciliation_v0_1'})


def main():
    with tempfile.TemporaryDirectory(prefix='live-runtime-c4-') as tmp:
        directory = Path(tmp)
        pending_and_consumed(directory)
        admitted_and_sign_cut(directory)
        lifecycle(directory)
        active_replay_priority(directory)
        unhealthy_and_corrupt(directory)
        dry(directory)
        structural()
    print(c2.canonical_json({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(CHECKS),
        'reused_C1_checks':len(c1.CHECKS),'reused_C2_checks':len(c2.CHECKS),
        'all_checks':all(CHECKS.values()) and all(c1.CHECKS.values()) and all(c2.CHECKS.values())}))


if __name__=='__main__':
    main()
