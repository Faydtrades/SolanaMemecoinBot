"""IR-C2 actual supported DRY profile; isolated synthetic public observations."""
from __future__ import annotations
import copy
import json
import sys
import tempfile
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch
import httpx

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_runtime_owned_dry_selftest_v0_1 as owned
from live import runtime_dry_profile_v0_1 as profile_api
from live.ledger_domain_v0_1 import LedgerDomain
from live.ledger_evidence_codec_v0_1 import wallet_observation_to_json
from live.public_rpc_v0_1 import PublicRpcProfile
from live.operations_degradation_v0_1 import DegradationStore
from live.operations_degradation_monitor_v0_1 import HostMetric, HostObservations
from live.runtime_dry_public_driver_v0_1 import DryPublicFacts
from live.execution_message_v0_1 import ComputeBudget
from phase5.shadow_domain_v0_1 import content_fingerprint

CHECKS={}


def check(name,value):
    CHECKS[name]=bool(value)
    if not value:raise AssertionError(name)


def references():
    # Only exact interface references from the immutable accepted index.
    index=json.loads((ROOT/'docs/live/MEME_LIVE_STEP11A_PROFILE_REVIEW_V1.json').read_text())
    def ref(suffix):return next(r for r in index['artifacts'] if r['path'].endswith(suffix))
    return dict(accepted_profile=ref('m52-profile-02\\profile.json'),
        accepted_extension=ref('profile-extension.json'),accepted_monitor=ref('canonical-monitor-configuration-candidate.json'))


@contextmanager
def accepted_public_fixture(refs):
    accepted=profile_api.read_reference(refs['accepted_profile'])
    target=accepted['target']
    rpc=PublicRpcProfile(**accepted['constructor_configuration']['public_rpc_profile'])
    a3,sf=owned.a3,owned.sf
    original_observe=a3.observe
    def observe(scenario=None, **kwargs):
        if scenario is not None:scenario.genesis=target['domain']['genesis_hash']
        kwargs['profile']=rpc
        return original_observe(scenario,**kwargs)
    def domain(**kwargs):
        return LedgerDomain(**dict(target['domain'], mode='DRY', expected_empty_token_accounts=(),
            known_native_wallet_lamports=sf.FUNDING))
    original_init=owned.rt.hf.LiveContinuousProducerV02.__init__
    producer_profile=profile_api.ContinuationProfileV02(**accepted['constructor_configuration']['continuation_profile'])
    def producer_init(self,*args,**kwargs):
        kwargs.setdefault('profile',producer_profile)
        original_init(self,*args,**kwargs)
    original_args=owned.fixtures.c4.cold_args
    def cold_args(f):
        args=original_args(f)
        args.update(producer_profile=producer_profile,**accepted['constructor_configuration']['dispatch'])
        return args
    with ExitStack() as stack:
        stack.enter_context(patch.object(owned.rt.hf.LiveContinuousProducerV02,'__init__',producer_init))
        stack.enter_context(patch.object(owned.fixtures.c4,'cold_args',cold_args))
        for module,key,value in ((a3,'domain',domain),(a3,'observe',observe),(a3,'PROFILE',rpc),
            (a3,'GENESIS',target['domain']['genesis_hash']),(sf,'WALLET',target['domain']['wallet']),
            (sf.plans,'ACTOR',target['domain']['wallet']),
            (owned.rt.hf,'DATABASE_ID',accepted['source_configuration']['database_identity'])):
            stack.enter_context(patch.object(module,key,value))
        yield accepted


def inputs_for(f, refs, accepted):
    monitor=profile_api.read_reference(refs['accepted_monitor'])['monitor']
    original=accepted['constructor_configuration']
    amendment_ref={'path':r'C:\Users\Mari1\AppData\Local\Temp\meme-live-step11c-closure-430a9b0-20260912\irc2-dry-guard-amendment.json',
        'sha256':'81284158078f36c151a0b7bcda38257306eea0ad6f4ca6989539922d346235e4'}
    amendment=profile_api.read_reference(amendment_ref)
    return dict(refs,guard_amendment=amendment_ref,baseline=dict(observation_json=wallet_observation_to_json(f.repo.baseline().observation),
        known_native_wallet_lamports=f.domain.known_native_wallet_lamports,
        evaluated_at_utc=f.repo.baseline().decision.evaluated_at_utc,
        required_min_context_slot=f.repo.baseline().decision.required_min_context_slot,
        review_reference='SYNTHETIC_PUBLIC_BASELINE_QUALIFICATION_NOT_ACTUAL_BALANCE'),
        source_start=dict(semantics=profile_api.SOURCE_START,binding=asdict(f.binding),profile=asdict(f.source.profile),
            start_after_p1_rowid=1,database_identity=accepted['source_configuration']['database_identity'],
            read_path=str(f.raw),review_reference='SYNTHETIC_RETAINED_ANCHOR_QUALIFICATION_NOT_ACTUAL_START'),
        store_paths=dict(ledger=str(f.path),producer=str(f.ppath),operations=str(f.operations_path),
            evidence_store=str(f.directory/(f.name+'-evidence.sqlite')),monitor=str(f.directory/(f.name+'-qualified-monitor.sqlite'))),
        producer_schema_digest=f.expected.producer_schema_digest,evidence_schema_digest=f.expected.evidence_schema_digest,
        monitor=dict(resource_limits=dict({r['metric']:r['limit'] for r in monitor['policy']['resource_limits']},**amendment['proposed_guard_limits']),
            persistent_unknown_alert_us=monitor['policy']['persistent_unknown_alert_us'],
            recovery_evidence_max_age_us=monitor['policy']['recovery_evidence_max_age_us'],
            resource_max_age_us=monitor['resource_max_age_us'],protective_qualification_digest=monitor['protective_qualification_digest']),
        driver=dict(endpoint='https://invalid.local',public_rpc_profile=original['public_rpc_profile'],
            facts_provider=profile_api.FACTS_PROVIDER,max_steps=1,quote_policy=dict(slippage_bps=100),
            plan_policy=dict(message_format='LEGACY',setup_policy='SDK_COMPAT_IDEMPOTENT_V01',
                recipient_selection='HASH_BOUND_INDEX_V01',volume_tracking=True,commitment='confirmed'),
            compute=dict(units=250000,micro_lamports=1000),source_cut_semantics='EXPLICIT_CURRENT_PUBLIC_UTC_CUT'),
        qualification_substitutions=dict(scope='DETERMINISTIC_QUALIFICATION_ONLY',synthetic_baseline_and_rpc=True,
            synthetic_source_binding=True,source_read_path=str(f.raw)))


def adopt(f, profile):
    owned.fixtures.close(f)
    state=f.operations.snapshot()
    f.started=profile.start(process_identity=f.name+'-supported',now_us=state['last_control_us']+1,
        replace_generation=state['generation'])
    f.runtime=f.started.runtime
    f.repo,f.producer,f.handoff,f.source=f.runtime.ledger,f.runtime.producer,f.runtime.handoff,f.runtime.source
    f.conn=f.runtime._producer_conn
    f.startup_monitor_config=f.runtime._operations_degradation.configuration
    return f.started


def host(f, at):
    c=f.startup_monitor_config
    metrics=tuple(HostMetric(name,1073741824+1 if name=='HOST_DISK_RESERVE_BYTES' else 100,at*1000000,
        c.protective_qualification_digest if name=='PROTECTIVE_STEP_US' else '9'*64)
        for name in ('HOST_RSS_BYTES','HOST_DISK_RESERVE_BYTES','STARTUP_US','PROTECTIVE_STEP_US'))
    return HostObservations(c.host_identity_digest,c.policy.reviewed_configuration_digest,
        content_fingerprint(asdict(f.runtime._ownership.fence)),metrics)


def execute(f,profile,action,at, *, interrupted=False):
    seed=owned.seed_for(f,action,at)
    boundary=owned.external.DryPublicTransport(seed)
    f.profile_last_boundary=boundary
    def now():
        return owned.dry.utc_microseconds(seed.clock.utc_upper_utc) if len(boundary.requests)>=9 else seed.evidence.simulation.leases[0].observed_at_us
    facts=DryPublicFacts(lambda:seed.clock,None,seed.evidence.wallet,seed.evidence.quote_policy,seed.evidence.plan_policy,
        ComputeBudget(250000,1000),now,lambda:host(f,at),owned.a3.utc(owned.NOW+15 if at>=owned.NOW+18 else owned.NOW+1))
    with patch.object(owned.ExecutionReadOnlyRpc,'__init__',autospec=True) as construct:
        def create(self,*args,**kwargs):
            kwargs['now_us']=boundary.now
            owned.EXECUTION_RPC_INIT(self,*args,**kwargs)
        construct.side_effect=create
        if interrupted:
            original=f.repo.finish_dry
            def fail(*args,**kwargs):raise owned.Interrupted('before terminal')
            with patch.object(f.repo,'finish_dry',side_effect=fail):
                profile.drive(f.started,lambda _:facts,public_transport=httpx.MockTransport(boundary))
        else:
            result=profile.drive(f.started,lambda _:facts,public_transport=httpx.MockTransport(boundary))[0]
            return result,boundary


def qualification(directory):
    refs=references()
    with accepted_public_fixture(refs) as accepted, ExitStack() as traps:
        from live.execution_signer_v0_1 import AutonomousLocalSigner
        from live.execution_send_v0_1 import SolanaSendTransport
        for target,name in ((AutonomousLocalSigner,'__init__'),(AutonomousLocalSigner,'sign_exact'),
            (SolanaSendTransport,'__init__'),(httpx.HTTPTransport,'handle_request')):
            traps.enter_context(patch.object(target,name,side_effect=AssertionError('profile mutation/network forbidden')))
        f=owned.fixture(directory,'supported-profile')
        try:
            inputs=inputs_for(f,refs,accepted)
            from live.acceptance_dossier_v0_1 import canonical_bytes,sha256
            from live.continuous_producer_v0_2 import STORAGE_FINGERPRINT
            producer_profile=accepted['constructor_configuration']['continuation_profile']
            # The historical 16384-byte snapshot no longer covers corrected
            # lossless history. Only this synthetic fixture uses the existing
            # architectural ceiling, never observed usage or a public claim.
            amendment={'schema':'MEME_LIVE_DRY_STORAGE_QUALIFICATION_AMENDMENT_V1',
                'scope':'DETERMINISTIC_QUALIFICATION_ONLY','status':'IMPLEMENTED_PENDING_PROJECT_REVIEW',
                'project_acceptance_claimed':False,'production_capacity_claimed':False,
                'accepted_profile':refs['accepted_profile'],'accepted_extension':refs['accepted_extension'],
                'producer_storage_fingerprint':STORAGE_FINGERPRINT,'producer_profile':producer_profile,
                'proposed_guard_limits':{'HISTORY_BYTES':producer_profile['history_bytes'],'TOMBSTONE_ROWS':2}}
            amendment_path=directory/'supported-profile-storage-qualification-amendment.json'
            amendment_path.write_bytes(canonical_bytes(amendment))
            inputs['guard_amendment']={'path':str(amendment_path),'sha256':sha256(amendment_path.read_bytes())}
            inputs['monitor']['resource_limits'].update(amendment['proposed_guard_limits'])
            profile=profile_api.build_profile(inputs)
            check('profile_original_public_identity',profile.record['domain']['wallet']==accepted['target']['domain']['wallet']
                and profile.record['domain']['genesis_hash']==accepted['target']['domain']['genesis_hash']
                and profile.record['domain']['expected_profile_fingerprint']==accepted['target']['domain']['expected_profile_fingerprint']
                and inputs['source_start']['database_identity']==accepted['source_configuration']['database_identity'])
            check('synthetic_observations_explicit',profile.record['inputs']['qualification_substitutions']['scope']=='DETERMINISTIC_QUALIFICATION_ONLY')
            check('original_baseline_adjudication',profile.record['baseline_decision']['disposition']=='ESTABLISHED')
            check('profile_roundtrip',profile_api.load_profile(profile.record).record==profile.record)
            config=profile.configuration()
            DegradationStore.initialize(config['degradation_config'].path,f.domain,config['degradation_config'].policy,now_us=0)
            adopt(f,profile)
            observed_maxima={}
            from live.operations_degradation_monitor_v0_1 import OperationsMonitor
            actual_observe=OperationsMonitor.observe
            def observe(monitor,*args,**kwargs):
                value=actual_observe(monitor,*args,**kwargs)
                if monitor.configuration and monitor.configuration.policy.content_digest==config['degradation_config'].policy.content_digest:
                    for key,amount in monitor.last_metrics:
                        if amount is not None:observed_maxima[key]=max(observed_maxima.get(key,amount),amount)
                return value
            traps.enter_context(patch.object(OperationsMonitor,'observe',observe))
            check('exact_supported_startup',asdict(f.started.audit.identity)==asdict(config['expected_identity'])
                and f.runtime.capability=='NO_BROADCAST' and f.started.audit.reconstruction.source_state=='SOURCE_RECONSTRUCTED')
            with patch.object(owned.fixtures,'host',side_effect=lambda fixture,at=owned.NOW+4:host(f,at)):
                action=owned.admit(f)
                result,boundary=execute(f,profile,action,owned.NOW+9)
                check('qualified_actual_exact_dry_terminal',result.work=='NON_SUBMITTED' and result.reason=='DRY_CAPABILITY'
                    and len(boundary.requests)==10)
                adopt(f,profile)
                check('qualified_cold_terminal_reopen',f.runtime._entry_action_id is None
                    and f.repo.inbox_disposition(action.root_id)=='NON_SUBMITTED')
                f.append(owned.rt.hf.NEXT)
                for _ in range(128):
                    value=owned.step(f,owned.NOW+18)
                    if value.work=='NEED_ENTRY_FACTS':break
                check('same_profile_next_original_candidate',value.work=='NEED_ENTRY_FACTS' and value.root_id!=action.root_id)
                f.root,f.item=value.root_id,f.repo.candidate(value.root_id)
                support=owned.a3.wallet(f,scenario=f.scenario,at=owned.NOW+18)
                value=owned.step(f,owned.NOW+18,entry=owned.runtime.EntryFacts('PUMP',owned.sf.TOKEN_PROGRAM_ID,support))
                check('same_profile_second_original_admission',value.work=='ENTRY_ADMITTED')
                second=f.repo.action(value.action_id)
                try:execute(f,profile,second,owned.NOW+23,interrupted=True)
                except owned.Interrupted:pass
                else:raise AssertionError('missing profile interruption')
                check('same_profile_prepared_simulation_interruption',len(f.profile_last_boundary.requests)==10
                    and f.repo._root_attempts(second.root_id)[0].recorded_stage=='EXACT_SIMULATED')
                adopt(f,profile)
                seed=owned.seed_for(f,second,owned.NOW+24)
                facts=DryPublicFacts(lambda:seed.clock,None,None,seed.evidence.quote_policy,seed.evidence.plan_policy,
                    ComputeBudget(250000,1000),lambda:owned.dry.utc_microseconds(seed.clock.utc_upper_utc),
                    lambda:host(f,owned.NOW+24),owned.a3.utc(owned.NOW+15))
                recovered=profile.drive(f.started,lambda _:facts,public_transport=httpx.MockTransport(owned.no_public_read))[0]
                check('same_profile_cold_original_non_submitted_recovery',recovered.work=='NON_SUBMITTED'
                    and f.repo.inbox_disposition(second.root_id)=='NON_SUBMITTED'
                    and len(f.repo._root_attempts(second.root_id))==1)
            owned.absence(f,(action.root_id,second.root_id))
            check('driver_changed_compute_denied',owned.fails(lambda:profile.drive(f.started,
                lambda _:replace(facts,compute=ComputeBudget(250001,1000)),
                public_transport=httpx.MockTransport(owned.no_public_read))))
            changed_inputs=copy.deepcopy(inputs)
            observation=profile_api.wallet_observation_from_json(changed_inputs['baseline']['observation_json'])
            observation=replace(observation,started_at_utc=owned.a3.utc(owned.NOW+1),observed_at_utc=owned.a3.utc(owned.NOW+1))
            changed_inputs['baseline'].update(observation_json=wallet_observation_to_json(observation),evaluated_at_utc=owned.a3.utc(owned.NOW+1))
            changed_profile=profile_api.build_profile(changed_inputs)
            owned.fixtures.close(f)
            state=f.operations.snapshot()
            check('startup_different_durable_baseline_denied',owned.fails(lambda:changed_profile.start(
                process_identity='wrong-original-baseline',now_us=state['last_control_us']+1,replace_generation=state['generation'])))
            adopt(f,profile)
            # Config/observation changes cannot reuse this exact profile identity.
            for name,mutate in (
                ('stale-code',lambda r:r['startup_identity'].update(runtime_code_digest='0'*64)),
                ('wrong-domain',lambda r:r['domain'].update(mode='LIVE')),
                ('wrong-path',lambda r:r['inputs']['store_paths'].update(ledger=str(directory/'other.sqlite'))),
                ('monitor-alias',lambda r:r['inputs']['store_paths'].update(monitor=r['inputs']['store_paths']['ledger'])),
                ('wrong-driver-limit',lambda r:r['inputs']['driver'].update(max_steps=2)),
                ('wrong-source-start',lambda r:r['inputs']['source_start'].update(start_after_p1_rowid=2)),
                ('wrong-guard',lambda r:r['inputs']['monitor']['resource_limits'].update(STARTUP_US=1)),
                ('unknown-baseline',lambda r:r['inputs']['baseline'].update(known_native_wallet_lamports=None)),
                ('wrong-baseline-observation',lambda r:r['inputs']['baseline'].update(known_native_wallet_lamports=1)),
                ('wrong-provider',lambda r:r['inputs']['driver']['public_rpc_profile'].update(provider_id='OTHER')),
                ('hidden-substitution',lambda r:r['inputs']['qualification_substitutions'].update(scope='PUBLIC_ENVIRONMENT_REVIEWED'))):
                changed=copy.deepcopy(profile.record);mutate(changed)
                check(name,owned.fails(lambda:profile_api.load_profile(changed)))
            exported=profile.record
            (directory/'supported-profile.json').write_text(json.dumps(exported,sort_keys=True,indent=2)+'\n')
            from live.operations_degradation_v0_1 import resource_condition
            policy=f.startup_monitor_config.policy
            check('amended_guards_fail_closed_above_bound',all(resource_condition(policy,key,limit+1,
                configuration_digest=policy.reviewed_configuration_digest)=='RESOURCE_EXCEEDED'
                for key,limit in (('HISTORY_BYTES',inputs['monitor']['resource_limits']['HISTORY_BYTES']),
                    ('TOMBSTONE_ROWS',2))))
            check('no_capital_or_permission',not exported['grants_permission'] and not exported['capital_authority'])
            from live.acceptance_dossier_v0_1 import source_freeze
            result={'source_content_digest':source_freeze(ROOT)['content_digest'],'schema':'MEME_LIVE_DRY_PROFILE_QUALIFICATION_V1','checks':dict(CHECKS),
                'guard_amendment':inputs['guard_amendment'],'profile_content_digest':exported['content_digest'],'startup_identity':exported['startup_identity'],
                'domain':exported['domain'],'source_identity':exported['source_identity'],
                'qualification_substitutions':exported['inputs']['qualification_substitutions'],
                'same_root':'live.runtime_reconstruction_v0_1.ColdRuntimeV01','simulation_count':2,'cold_interruption_recovery_count':1,
                'original_source_mapping':dict(source_binding_identity=f.binding.source_identity,market_source_identity=f.producer.market_source.source_identity,producer_lineage=f.producer.source_identity),
                'signer_send_broadcast':False,'canonical_store_access':False,'T010_executed':False,
                'observed_monitor_maxima':observed_maxima,
                'host_metrics_scope':'Synthetic external resource comparator facts; no physical performance or capacity claim.',
                'scope':'Exact supported constructor/startup/driver/reopen with synthetic public observations; actual environment remains M58.'}
            (directory/'profile-qualification.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
            return result
        finally:owned.fixtures.close(f)


def main():
    if len(sys.argv)>1:
        directory=Path(sys.argv[1]).resolve();directory.mkdir(parents=True,exist_ok=True)
        result=qualification(directory)
    else:
        with tempfile.TemporaryDirectory(prefix='step11c-dry-profile-') as tmp:result=qualification(Path(tmp))
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__':main()
