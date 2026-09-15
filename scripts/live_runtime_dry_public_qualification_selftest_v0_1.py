"""Focused public qualification contracts; fake transports and isolated stores.

These checks are engineering fixtures, never actual-environment qualification.
No Step10/11 campaign is run and no canonical source/store is opened.
"""
from __future__ import annotations
import copy
import json
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack, closing
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import httpx

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_runtime_dry_profile_selftest_v0_1 as fixtures
from live.runtime_dry_public_facts_v0_1 import ReviewedPublicFacts
from live.runtime_dry_public_qualification_v0_1 import initialize_isolated, _terminal_evidence, validate_evidence
from live.runtime_dry_v0_1 import DryQualificationInterruption
from live.authority_controls_v0_1 import ControlCommand
from live.ledger_repository_v0_1 import LedgerRepository
from phase5.shadow_domain_v0_1 import content_fingerprint

CHECKS={}


def check(name, value):
    CHECKS[name]=bool(value)
    if not value: raise AssertionError(name)


def reference(path, record):
    import hashlib
    path.write_text(json.dumps(record,sort_keys=True))
    return {'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}


def main():
    import live_runtime_dry_public_qualification_v0_1 as cli
    # Real disposable child handles, no Runtime/source/network. A stalled
    # resolver analogue cannot survive the qualifier's exact-child watchdog.
    command=[sys.executable,'-B','-c','import time,sys;time.sleep(float(sys.argv[1]))']
    launched=time.perf_counter_ns();child=subprocess.Popen(command+['10'])
    result=cli._wait_owned_child(child,timeout_us=100000,termination_us=2000000,launched_ns=launched)
    check('qualifier_exact_stalled_child_terminated',result['deadline_expired']
        and result['termination_confirmed'] and result['exit_code']!=0)
    launched=time.perf_counter_ns();child=subprocess.Popen(command+['0'])
    result=cli._wait_owned_child(child,timeout_us=5000000,termination_us=2000000,launched_ns=launched)
    check('qualifier_normal_child_confirmed',not result['deadline_expired']
        and result['termination_confirmed'] and result['exit_code']==0)
    signals=[]
    unconfirmed=SimpleNamespace(returncode=None,wait=lambda **kw:None,poll=lambda:None,terminate=lambda:signals.append('exact'))
    result=cli._wait_owned_child(unconfirmed,timeout_us=1,termination_us=1,launched_ns=time.perf_counter_ns())
    check('qualifier_unconfirmed_termination_remains_denied',result['deadline_expired']
        and not result['termination_confirmed'] and signals==['exact'])
    late=SimpleNamespace(returncode=0,wait=lambda **kw:None,poll=lambda:0)
    with patch.object(cli.time,'perf_counter_ns',side_effect=[2000,2000,2000]):
        result=cli._wait_owned_child(late,timeout_us=1,termination_us=1,launched_ns=0)
    check('qualifier_late_exit_zero_is_not_success',result['deadline_expired'] and result['termination_confirmed'])
    with tempfile.TemporaryDirectory(prefix='fix2-qualifier-publication-') as directory:
        staged=Path(directory)/'candidate.json';final=Path(directory)/'final.json'
        staged.write_bytes(b'TEST_ONLY_PROCESS_PUBLICATION_NOT_QUALIFICATION')
        success={'deadline_expired':False,'termination_confirmed':True,'exit_code':0}
        for name,change in [('late',{'deadline_expired':True}),('unconfirmed',{'termination_confirmed':False}),('failed',{'exit_code':2})]:
            try:cli._publish_candidate(staged,final,dict(success,**change))
            except ValueError:denied=True
            else:denied=False
            check('qualifier_'+name+'_candidate_remains_nonfinal',denied and staged.exists() and not final.exists())
        cli._publish_candidate(staged,final,success)
        check('qualifier_only_confirmed_timely_candidate_published',not staged.exists()
            and final.read_bytes()==b'TEST_ONLY_PROCESS_PUBLICATION_NOT_QUALIFICATION')
        staged.write_bytes(b'DO_NOT_OVERWRITE')
        try:cli._publish_candidate(staged,final,success)
        except ValueError:denied=True
        else:denied=False
        check('qualifier_final_output_never_overwritten',denied and staged.read_bytes()==b'DO_NOT_OVERWRITE'
            and final.read_bytes()==b'TEST_ONLY_PROCESS_PUBLICATION_NOT_QUALIFICATION')
    guards={'supervisor_startup_us':2000000,'supervisor_progress_us':3000000,'supervisor_termination_us':4000000}
    sample=SimpleNamespace(record={'inputs':{'resource_envelope':{},'driver':{'max_steps':316,
        'public_rpc_profile':{'observation_timeout_seconds':180}}}})
    resource={'derivation':{'measured_guards':guards,'qualification':{'max_startups':2,'max_runtime_steps':316}}}
    with patch.object(cli,'read_reference',return_value=resource):
        observed,budget=cli._watchdog_budget(sample)
        check('qualifier_watchdog_derived_from_complete_work',observed==guards and budget['timeout_us']==1136000000
            and budget['termination_us']==4000000)
        sample.record['inputs']['driver']['max_steps']=317
        try:cli._watchdog_budget(sample)
        except ValueError as exc:reason=str(exc)
        else:reason=''
        check('qualifier_work_bound_plus_one_denied',reason=='PUBLIC_QUALIFICATION_WORK_BOUND_CONFLICT')
    owned=fixtures.owned
    refs=fixtures.references()
    with tempfile.TemporaryDirectory(prefix='fix2-public-dry-') as tmp, fixtures.accepted_public_fixture(refs) as accepted, ExitStack() as stack:
        directory=Path(tmp)
        stack.enter_context(patch.object(httpx.HTTPTransport,'handle_request',side_effect=AssertionError('NETWORK_FORBIDDEN')))
        from live.execution_signer_v0_1 import AutonomousLocalSigner
        from live.execution_send_v0_1 import SolanaSendTransport
        stack.enter_context(patch.object(AutonomousLocalSigner,'__init__',side_effect=AssertionError('SIGNER_FORBIDDEN')))
        stack.enter_context(patch.object(SolanaSendTransport,'__init__',side_effect=AssertionError('SEND_FORBIDDEN')))
        f=owned.fixture(directory,'public-contract')
        try:
            inputs=fixtures.inputs_for(f,refs,accepted)
            check('legacy_synthetic_amendment_unchanged',fixtures.profile_api.build_profile(inputs).record[
                'inputs']['monitor']['resource_limits']['HISTORY_BYTES']==16384)
            from live.continuous_producer_v0_2 import STORAGE_FINGERPRINT
            producer_profile=accepted['constructor_configuration']['continuation_profile']
            storage_amendment={'schema':'MEME_LIVE_DRY_STORAGE_QUALIFICATION_AMENDMENT_V1',
                'scope':'DETERMINISTIC_QUALIFICATION_ONLY','status':'IMPLEMENTED_PENDING_PROJECT_REVIEW',
                'project_acceptance_claimed':False,'production_capacity_claimed':False,
                'accepted_profile':inputs['accepted_profile'],'accepted_extension':inputs['accepted_extension'],
                'producer_storage_fingerprint':STORAGE_FINGERPRINT,'producer_profile':producer_profile,
                'proposed_guard_limits':{'HISTORY_BYTES':producer_profile['history_bytes'],'TOMBSTONE_ROWS':2}}
            inputs['guard_amendment']=reference(directory/'synthetic-storage-amendment.json',storage_amendment)
            inputs['monitor']['resource_limits']['HISTORY_BYTES']=producer_profile['history_bytes']
            profile=fixtures.profile_api.build_profile(inputs)
            check('synthetic_history_uses_bound_architectural_capacity',profile.record['inputs']['monitor'][
                'resource_limits']['HISTORY_BYTES']==producer_profile['history_bytes'])
            for name,field,value in (
                ('wrong_storage','producer_storage_fingerprint','0'*64),
                ('wrong_profile','producer_profile',dict(producer_profile,history_bytes=producer_profile['history_bytes']+1)),
                ('wrong_accepted_profile','accepted_profile',dict(inputs['accepted_profile'],sha256='0'*64)),
                ('production_claim','production_capacity_claimed',True),
                ('scope_relabel','scope','PUBLIC_ENVIRONMENT_REVIEWED'),
                ('observed_history_ratchet','proposed_guard_limits',{'HISTORY_BYTES':23294,'TOMBSTONE_ROWS':2}),
                ('above_architectural_cap','proposed_guard_limits',{'HISTORY_BYTES':producer_profile['history_bytes']+1,'TOMBSTONE_ROWS':2}),
                ('unrelated_limit','proposed_guard_limits',dict(storage_amendment['proposed_guard_limits'],HOST_RSS_BYTES=1))):
                invalid=copy.deepcopy(inputs)
                invalid['guard_amendment']=reference(directory/(name+'.json'),dict(storage_amendment,**{field:value}))
                check('synthetic_storage_amendment_denies_'+name,owned.fails(lambda:fixtures.profile_api.build_profile(invalid)))
            changed=copy.deepcopy(inputs)
            changed['qualification_substitutions'].update(scope='PUBLIC_ENVIRONMENT_REVIEWED',
                synthetic_baseline_and_rpc=False,synthetic_source_binding=False)
            check('public_profile_rejects_synthetic_source',owned.fails(lambda:fixtures.profile_api.build_profile(changed)))
            changed['source_start']['read_path']=changed['source_start']['database_identity']
            changed['qualification_substitutions']['source_read_path']=changed['source_start']['read_path']
            changed['guard_amendment']=None
            changed['monitor']['resource_limits']=fixtures.profile_api.read_reference(refs['accepted_extension'])['limits']
            public_config=fixtures.profile_api.read_reference({'path':accepted['target']['public_configuration_path'],
                'sha256':accepted['target']['public_configuration_sha256']})
            changed['driver']['endpoint']=public_config['public_rpc']['url']
            public_profile=fixtures.profile_api.build_profile(changed)
            check('public_profile_explicit_constructor_only',public_profile.record['inputs']['guard_amendment'] is None
                and public_profile.record['store_initialization'] is False)
            wrong_endpoint=copy.deepcopy(changed);wrong_endpoint['driver']['endpoint']='https://other-provider.invalid'
            check('public_endpoint_substitution_denied',owned.fails(lambda:fixtures.profile_api.build_profile(wrong_endpoint)))
            wrong_guard=copy.deepcopy(changed);wrong_guard['guard_amendment']=inputs['guard_amendment']
            check('public_synthetic_guard_amendment_denied',owned.fails(lambda:fixtures.profile_api.build_profile(wrong_guard)))
            # External decisions below are disposable codec fixtures, not approvals.
            limits = dict(changed['monitor']['resource_limits'], TOMBSTONE_ROWS=2, HISTORY_BYTES=16384)
            measure = {'schema':'MEME_LIVE_PUBLIC_DRY_CAPACITY_OBSERVATION_V1',
                'scope':'ACTUAL_PUBLIC_READONLY_DRY_CAPACITY','accepted_extension':refs['accepted_extension'],
                'signer_send_broadcast':False,'canonical_live_store_mutation':False,
                'maxima':{'TOMBSTONE_ROWS':2,'HISTORY_BYTES':12000}}
            review = {'schema':'MEME_LIVE_PUBLIC_DRY_GUARD_REVIEW_V1','authority':'CHATGPT_PROJECT_REVIEW',
                'decision':'APPROVED_FOR_PUBLIC_DRY','review_reference':'DISPOSABLE_CODEC_FIXTURE_ONLY',
                'accepted_profile':refs['accepted_profile'],'accepted_extension':refs['accepted_extension'],
                'guard_limits':limits,'measurement':reference(directory/'capacity.json',measure)}
            amended = copy.deepcopy(changed)
            amended['guard_amendment'] = reference(directory/'guard.json',review)
            amended['monitor']['resource_limits'] = limits
            check('explicit_reviewed_public_guard_codec',fixtures.profile_api.build_profile(amended).record[
                'inputs']['monitor']['resource_limits']['TOMBSTONE_ROWS']==2)
            review['decision']='PROPOSED_NOT_APPROVED'
            amended['guard_amendment']=reference(directory/'guard.json',review)
            check('proposed_public_guard_denied',owned.fails(lambda:fixtures.profile_api.build_profile(amended)))
            review['decision']='APPROVED_FOR_PUBLIC_DRY';review['guard_limits']=dict(limits,HOST_RSS_BYTES=1)
            amended['guard_amendment']=reference(directory/'guard.json',review)
            check('unrelated_public_guard_change_denied',owned.fails(lambda:fixtures.profile_api.build_profile(amended)))
            from live.runtime_dry_public_qualification_v0_1 import validate_metric_evidence
            public_policy=public_profile.configuration()['degradation_config'].policy
            check('empty_resource_evidence_denied',owned.fails(lambda:validate_metric_evidence({}, {}, public_policy)))
            metrics={item.metric:item.limit for item in public_policy.resource_limits}
            validate_metric_evidence(metrics,metrics,public_policy)
            check('complete_resource_evidence_checked',True)
            sparse=dict(metrics);sparse.pop('TOMBSTONE_ROWS')
            check('omitted_tombstone_evidence_denied',owned.fails(lambda:validate_metric_evidence(sparse,sparse,public_policy)))
            empty=dict(metrics,OLDEST_UNCONSUMED_AGE_US=None)
            check('unknown_backlog_age_denied',owned.fails(lambda:validate_metric_evidence(empty,empty,public_policy)))
            validate_metric_evidence(empty,empty,public_policy,empty_backlog_observations=1)
            check('original_positive_empty_backlog_has_no_age',True)
            changed=copy.deepcopy(inputs)
            changed['driver']['endpoint']='https://public.example/private-token'
            check('endpoint_path_credentials_never_persisted',owned.fails(lambda:fixtures.profile_api.build_profile(changed)))

            # Bootstrap invokes original schema/domain/policy owners on NEW stores.
            fresh=copy.deepcopy(inputs)
            fresh['store_paths']={key:str(directory/'fresh'/('fresh-'+key+'.sqlite')) for key in inputs['store_paths']}
            fresh_profile=fixtures.profile_api.build_profile(fresh)
            state=f.repo.authority_snapshot()
            policy,grant=state['policy'],state['armed_entry_grant']
            commands=(ControlCommand('public-test-install',f.domain.economic_domain_id,'INSTALL_POLICY',policy.approval,policy=policy),
                ControlCommand('public-test-arm',f.domain.economic_domain_id,'ARM',grant.approval,grant=grant))
            source=f.source.latest_record()[1]
            fake=SimpleNamespace(wallet=lambda:None,source=lambda:source,clock=lambda:owned.a3.clock(f.repo,owned.NOW+4))
            initialize_isolated(fresh_profile,commands,fake)
            with closing(LedgerRepository.reopen(fresh['store_paths']['ledger'],f.domain)) as repo:
                check('bootstrap_exact_original_baseline',repo.baseline().observation==f.repo.baseline().observation)
                check('bootstrap_no_candidates_or_effects',repo.audit()['candidate_count']==repo.audit()['posting_count']==0)
            check('bootstrap_existing_stores_denied',owned.fails(lambda:initialize_isolated(fresh_profile,commands,fake)))

            # Actual wallet adapter through a fake HTTP boundary; scope explicit.
            config=profile.configuration()
            from live.runtime_public_clock_v0_1 import NtpProvider, REVIEW_SCHEMA, TRUST
            clock_provider=NtpProvider('FAKE_PUBLIC_CLOCK','test.invalid','192.0.2.1',123,0,1000,1000000,30000000,TRUST,4)
            clock_review={'schema':REVIEW_SCHEMA,'status':'APPROVED_FOR_PUBLIC_DRY','origin':'HUMAN_EXTERNAL',
                'provider':asdict(clock_provider),'clock_policy':dict(provider_id=clock_provider.provider_id,
                    provider_fingerprint=clock_provider.fingerprint,maximum_uncertainty_us=1000000,
                    maximum_drift_ppm=0,maximum_checkpoint_elapsed_us=30000000),
                'host_identity_digest':config['degradation_config'].host_identity_digest,'review_reference':'FAKE_TEST_ONLY'}
            settings={'clock_profile':reference(directory/'clock.json',clock_review),
                'protective_timing':reference(directory/'timing.json',{}),'venue':'PUMP','token_program':owned.sf.TOKEN_PROGRAM_ID,
                'minimum_context_slot':100,'source_lag_us':0}
            def clock_observer(provider):
                return {'provider_id':provider.provider_id,'provider_fingerprint':provider.fingerprint,
                    'monotonic_ns':1000000000,'round_trip_us':0,'utc_lower_utc':owned.a3.utc(owned.NOW+4),
                    'utc_upper_utc':owned.a3.utc(owned.NOW+4)}
            provider=ReviewedPublicFacts(profile,settings,test_transport=httpx.MockTransport(lambda _:httpx.Response(503)),
                test_clock_observer=clock_observer)
            check('fake_transport_never_public_scope',provider.scope=='TEST_ONLY_FAKE_EXTERNAL_TRANSPORT')
            with patch('live.runtime_public_clock_v0_1.time.perf_counter_ns',return_value=1000001000):
                sample=provider.clock()
                check('reviewed_clock_exact_provider',sample.provider_fingerprint==clock_provider.fingerprint)
                check('wallet_unavailable_fails_closed',owned.fails(provider.wallet))
                check('clock_provider_substitution_denied',owned.fails(lambda:provider.clock(f.started)))
                f.scenario.slot_calls=f.scenario.genesis_calls=0
                provider.transport=httpx.MockTransport(f.scenario.handle)
                wallet=provider.wallet()
                check('actual_wallet_adapter_fake_transport_explicit',wallet.observation.request.wallet==f.domain.wallet
                    and provider.observations[-1]['scope']=='TEST_ONLY_FAKE_EXTERNAL_TRANSPORT')
                owned.a3.wallet(f,scenario=f.scenario)
                f.scenario.slot_calls=f.scenario.genesis_calls=0
                support=provider.wallet(f.started)
                check('original_candidate_wallet_accounts_observed',len(support.observation.request.expected_accounts)==2)
            with patch('live.runtime_public_clock_v0_1.time.perf_counter_ns',return_value=100000000000):
                check('stale_calibration_denied',owned.fails(provider.clock))

            # New explicit interruption uses real DRY execution and real journal
            # durability, with only its public HTTP observations fabricated.
            config=profile.configuration()
            fixtures.DegradationStore.initialize(config['degradation_config'].path,f.domain,config['degradation_config'].policy,now_us=0)
            fixtures.adopt(f,profile)
            with patch.object(owned.fixtures,'host',side_effect=lambda fixture,at=owned.NOW+4:fixtures.host(f,at)):
                action=owned.admit(f)
                seed=owned.seed_for(f,action,owned.NOW+9)
                transport=owned.external.DryPublicTransport(seed)
                original=owned.EXECUTION_RPC_INIT
                from dataclasses import replace
                def create(self,*args,**kwargs):
                    kwargs['now_us']=transport.now
                    original(self,*args,**kwargs)
                now=lambda:owned.dry.utc_microseconds(seed.clock.utc_upper_utc) if len(transport.requests)>=9 else seed.evidence.simulation.leases[0].observed_at_us
                facts=fixtures.DryPublicFacts(lambda:seed.clock,None,seed.evidence.wallet,seed.evidence.quote_policy,
                    seed.evidence.plan_policy,fixtures.ComputeBudget(250000,1000),now,lambda:fixtures.host(f,owned.NOW+9),owned.a3.utc(owned.NOW+1))
                driver=replace(profile.driver(lambda _:facts,public_transport=httpx.MockTransport(transport)),
                    qualification_interrupt_after_simulation=True)
                with patch.object(owned.ExecutionReadOnlyRpc,'__init__',create):
                    try:
                        with driver(f.started) as values: f.runtime.step(**values)
                    except DryQualificationInterruption: pass
                    else: raise AssertionError('missing explicit interruption')
                check('explicit_hook_after_durable_exact_simulation',len(transport.requests)==10
                    and f.repo._root_attempts(action.root_id)[0].recorded_stage=='EXACT_SIMULATED'
                    and f.repo.inbox_disposition(action.root_id)!='NON_SUBMITTED')
                fixtures.adopt(f,profile)
                check('cold_owner_recovers_interrupted_dry',f.runtime._dry_recovery)
                # Exercise the original public callbacks on original pending
                # recovery. Resource failure must become UNKNOWN in the monitor.
                with patch.object(provider,'clock',return_value=seed.clock), \
                        patch.object(provider,'resources',side_effect=OSError('ISOLATED_RESOURCE_UNAVAILABLE')):
                    recoveryfacts=provider(f.started)
                    result=profile.drive(f.started,lambda _:recoveryfacts,public_transport=httpx.MockTransport(owned.no_public_read))[0]
                check('cold_terminal_no_public_reexecution',result.work=='NON_SUBMITTED')
                host_metrics=dict(f.runtime._operations_degradation.last_metrics)
                check('resource_failure_unknown_during_original_pending_recovery',all(host_metrics.get(name) is None
                    for name in ('HOST_RSS_BYTES','HOST_DISK_RESERVE_BYTES','STARTUP_US','PROTECTIVE_STEP_US')))
                evidence=_terminal_evidence(f.repo,action.root_id,transport.requests[-1]['params'])
                check('terminal_evidence_exact_original_hash',evidence['simulation_input_digest']==f.repo._simulation_input_digest(evidence['attempt_id']))
                wrong_params=copy.deepcopy(transport.requests[-1]['params']);wrong_params[1]['sigVerify']=True
                check('simulation_request_substitution_denied',owned.fails(lambda:_terminal_evidence(f.repo,action.root_id,wrong_params)))
                f.append(owned.rt.hf.NEXT)
                for _ in range(128):
                    next_step=owned.step(f,owned.NOW+18)
                    if next_step.work=='NEED_ENTRY_FACTS': break
                check('second_candidate_original_producer_only',next_step.work=='NEED_ENTRY_FACTS' and next_step.root_id!=action.root_id)
                f.root,f.item=next_step.root_id,f.repo.candidate(next_step.root_id)
                support=owned.a3.wallet(f,scenario=f.scenario,at=owned.NOW+18)
                admitted=owned.step(f,owned.NOW+18,entry=owned.runtime.EntryFacts('PUMP',owned.sf.TOKEN_PROGRAM_ID,support))
                check('second_original_authority_admission',admitted.work=='ENTRY_ADMITTED')
                second=f.repo.action(admitted.action_id)
                result,second_boundary=fixtures.execute(f,profile,second,owned.NOW+23)
                check('second_exact_simulation_original_terminal',result.work=='NON_SUBMITTED'
                    and _terminal_evidence(f.repo,second.root_id,second_boundary.requests[-1]['params'])['root_id']==second.root_id)
                measured=dict(f.runtime._operations_degradation.last_metrics)
                from live.operations_degradation_v0_1 import resource_condition
                public_guards=public_profile.configuration()['degradation_config'].policy
                check('unamended_public_guard_rejects_two_tombstone_fixture',measured['TOMBSTONE_ROWS']==2
                    and resource_condition(public_guards,'TOMBSTONE_ROWS',measured['TOMBSTONE_ROWS'],
                        configuration_digest=public_guards.reviewed_configuration_digest)=='RESOURCE_EXCEEDED')
                forged={'schema':'MEME_LIVE_PUBLIC_DRY_QUALIFICATION_V1','scope':'TEST_ONLY_FAKE_EXTERNAL_TRANSPORT'}
                forged['content_digest']=content_fingerprint(forged)
                check('fake_evidence_cannot_claim_public_environment',owned.fails(lambda:validate_evidence(forged,profile)))
            owned.absence(f,(action.root_id,second.root_id))
        finally:
            owned.fixtures.close(f)
    print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','scope':'TEST_ONLY_FAKE_EXTERNAL_TRANSPORT',
        'checks':CHECKS,'T010_executed':False},sort_keys=True))


if __name__=='__main__': main()
