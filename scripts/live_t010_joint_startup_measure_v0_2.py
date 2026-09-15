"""V2: original host startup hooks and joint mock RPC allocation measurement.

Isolated retained synthetic stores only. No public requests, real host launch,
numeric policy selection, production codec changes or RSS quota are performed.
"""
from __future__ import annotations
import argparse
import ctypes
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from contextlib import ExitStack, closing
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
EVIDENCE=Path(r'C:\Users\Mari1\AppData\Local\Temp\meme-live-fix2-evidence-20260913')


def native_memory():
    class Counters(ctypes.Structure):
        _fields_=[('cb',ctypes.c_ulong),('PageFaultCount',ctypes.c_ulong),
            *[(name,ctypes.c_size_t) for name in ('PeakWorkingSetSize','WorkingSetSize','QuotaPeakPagedPoolUsage',
                'QuotaPagedPoolUsage','QuotaPeakNonPagedPoolUsage','QuotaNonPagedPoolUsage',
                'PagefileUsage','PeakPagefileUsage','PrivateUsage')]]
    api=ctypes.WinDLL('kernel32',use_last_error=True)
    api.GetCurrentProcess.restype=ctypes.c_void_p
    psapi=ctypes.WinDLL('psapi',use_last_error=True)
    psapi.GetProcessMemoryInfo.argtypes=[ctypes.c_void_p,ctypes.c_void_p,ctypes.c_ulong]
    value=Counters();value.cb=ctypes.sizeof(value)
    if not psapi.GetProcessMemoryInfo(api.GetCurrentProcess(),ctypes.byref(value),value.cb):
        raise ctypes.WinError(ctypes.get_last_error())
    return {name:int(getattr(value,name)) for name,_ in value._fields_ if name!='cb'}


def native_limits():
    from ctypes import wintypes as w
    from live.operations_windows_host_v0_1 import kernel,_Limits
    api=kernel();minimum=ctypes.c_size_t();maximum=ctypes.c_size_t();flags=w.DWORD()
    api.GetProcessWorkingSetSizeEx.argtypes=[w.HANDLE,ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(ctypes.c_size_t),ctypes.POINTER(w.DWORD)]
    assert api.GetProcessWorkingSetSizeEx(api.GetCurrentProcess(),ctypes.byref(minimum),ctypes.byref(maximum),ctypes.byref(flags))
    member=w.BOOL();api.IsProcessInJob.argtypes=[w.HANDLE,w.HANDLE,ctypes.POINTER(w.BOOL)]
    assert api.IsProcessInJob(api.GetCurrentProcess(),None,ctypes.byref(member))
    result=dict(working_set_minimum=minimum.value,working_set_maximum=maximum.value,
        working_set_flags=flags.value,hard_working_set_maximum_enabled=bool(flags.value&4),job_member=bool(member.value))
    if member.value:
        limits=_Limits()
        assert api.QueryInformationJobObject(None,9,ctypes.byref(limits),ctypes.sizeof(limits),None)
        result.update(current_job_flags=limits.BasicLimitInformation.LimitFlags,
            current_job_process_memory_limit=limits.ProcessMemoryLimit,current_job_memory_limit=limits.JobMemoryLimit)
    result['audit_scope']='current process and assigned job; inaccessible ancestor constraints are not inferred'
    return result


def sha(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def write(path,record):
    path.write_text(json.dumps(record,sort_keys=True,indent=2)+'\n',encoding='utf-8')


def build_configuration(record):
    from live.operations_startup_v0_1 import StartupIdentity,configured_identity,dry_monitor_fingerprint,schema_fingerprint
    from live.ledger_domain_v0_1 import LedgerDomain
    from live.source_health_v0_1 import SourceBinding,CursorWitness,SourceProfile
    from live.continuous_producer_v0_2 import ContinuationProfileV02
    from live.operations_ownership_v0_1 import RestartProfile
    from live.operations_degradation_v0_1 import DegradationPolicy,ResourceLimit
    from live.operations_degradation_monitor_v0_1 import MonitorConfiguration
    from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02
    d=dict(record['domain']);d['expected_empty_token_accounts']=tuple(d['expected_empty_token_accounts'])
    domain=LedgerDomain(**d)
    b=dict(record['source_binding']);b['anchors']=tuple(CursorWitness(**v) for v in b['anchors'])
    m=dict(record['monitor']);p=dict(m.pop('policy'));p['resource_limits']=tuple(ResourceLimit(**v) for v in p['resource_limits'])
    monitor=MonitorConfiguration(**m,policy=DegradationPolicy(**p))
    paths=record['paths']
    args=dict(operations_path=paths['operations'],ledger_path=paths['ledger'],domain=domain,
        producer_path=paths['producer'],source_path=paths['source'],source_binding=SourceBinding(**b),
        source_profile=SourceProfile(**record['source_profile']),producer_profile=ContinuationProfileV02(**record['producer_profile']),
        market_source=ContinuousMarketSourceV02(paths['raw'],start_after_p1_rowid=record['source_start_after'],database_identity=record['database_identity']),
        database_identity=record['database_identity'],batch_rows=32,page_rows=32,queued_roots=64,
        dry_live_paths=tuple(record['live_paths']))
    expected=configured_identity(**args,producer_schema_digest=schema_fingerprint(paths['producer']),
        evidence_schema_digest=schema_fingerprint(paths['source']),restart_profile=RestartProfile(100,1000000,0),
        dry_monitor_digest=dry_monitor_fingerprint(monitor),expected_baseline_observation_digest=record['baseline_digest'])
    args.update(expected_identity=expected,degradation_config=monitor,
        expected_baseline_observation_digest=record['baseline_digest'])
    return args


def prepare_monitor_copy(original,config,origin,root):
    """New fixture metadata, exact original generated incident receipt history.

    Absolute-path seals cannot be transplanted. Recreate this synthetic journal
    through its original API, never rewrite an old policy/header or bypass its
    constructor binding check. Original economic/source/producer copies remain
    byte-preserved until their normal measured owned startup.
    """
    from types import SimpleNamespace
    from live.operations_degradation_v0_1 import DegradationStore
    import live_t010_joint_state_v0_1 as builder
    prior=build_configuration(original)
    args=build_configuration(config)
    monitor=args['degradation_config']
    monitor=replace(monitor,policy=replace(monitor.policy,
        reviewed_configuration_digest=monitor.binding_for(args['expected_identity'])),
        runtime_code_digest=args['expected_identity'].runtime_code_digest)
    config['monitor']=asdict(monitor)
    final=build_configuration(config)
    assert monitor.policy.reviewed_configuration_digest==monitor.binding_for(final['expected_identity'])
    def journal_rows(path):
        with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True)) as conn:
            return dict(receipts=conn.execute('SELECT digest FROM receipts ORDER BY digest').fetchall(),
                conditions=conn.execute('SELECT * FROM conditions ORDER BY condition,subject_digest').fetchall())
    expected=journal_rows(original['paths']['monitor'])
    construction=json.loads((origin/'construction.json').read_text())
    count=construction['incidents']['pairs']
    assert len(expected['receipts'])==count*8
    journal=DegradationStore.initialize(monitor.path,args['domain'],monitor.policy,
        now_us=construction['incidents']['last_at_us']-construction['incidents']['record_calls']-1)
    fixture=SimpleNamespace(runtime=SimpleNamespace(_operations_degradation=SimpleNamespace(
        store=journal,configuration=prior['degradation_config'])),domain=args['domain'],
        binding=args['source_binding'],source=SimpleNamespace(profile=args['source_profile']))
    builder.incident_history(fixture,count)
    actual=journal_rows(monitor.path)
    assert actual==expected,'ORIGINAL_GENERATED_INCIDENT_HISTORY_DIFFERENCE'
    write(root/'monitor-reconstruction.json',dict(scope='NEW_ISOLATED_PATH_BOUND_FIXTURE_METADATA_ORIGINAL_GENERATED_HISTORY',
        original_path=original['paths']['monitor'],new_path=monitor.path,
        original_policy=asdict(prior['degradation_config'].policy),new_policy=asdict(monitor.policy),
        original_receipt_count=len(expected['receipts']),original_condition_count=len(expected['conditions']),
        receipt_and_terminal_condition_rows_exactly_equal=True,
        canonical_history_sha256=hashlib.sha256(json.dumps(expected,sort_keys=True).encode()).hexdigest(),
        outside_measured_child_startup=True))


def child(config_path,result_path):
    target_begin=time.perf_counter_ns()
    from live import operations_startup_v0_1 as startup
    from live import runtime_reconstruction_v0_1 as cold
    from live.ledger_repository_v0_1 import LedgerRepository
    from live.operations_ownership_v0_1 import OperationsStore
    from live.operations_degradation_monitor_v0_1 import OperationsMonitor
    from live.authority_controls_v0_1 import TrustedClockSample,utc_from_us
    from live.ledger_domain_v0_1 import ZERO_DIGEST
    from phase5.shadow_domain_v0_1 import content_fingerprint
    from live import t010_public_host_v0_1 as host
    from live.runtime_dry_profile_v0_1 import SupportedDryProfile
    from live.runtime_dry_public_facts_v0_1 import ReviewedPublicFacts
    from live.runtime_dry_public_driver_v0_1 import DryPublicFacts
    from live.execution_message_v0_1 import ComputeBudget
    from phase5.shadow_venue_route_quote_v0_1 import QuotePolicyV01,TOKEN_PROGRAM_ID
    from phase5.shadow_unsigned_plan_simulation_v0_1 import TransactionPlanPolicyV01
    import live_wallet_evidence_selftest_v0_1 as wallet_fixture
    import httpx
    record=json.loads(config_path.read_text())
    limits=native_limits()
    assert not limits['hard_working_set_maximum_enabled'] and not limits.get('current_job_flags',0)&0x300,limits
    events=[]
    def timed(label,call):
        def invoke(*a,**kw):
            start=time.perf_counter_ns()
            try:return call(*a,**kw)
            finally:events.append(dict(label=label,start_ns=start,end_ns=time.perf_counter_ns()))
        return invoke
    start=None
    try:
        with ExitStack() as stack:
            for obj,name,label in ((startup,'configured_identity','configuration_and_code_identity'),
                (OperationsStore,'acquire','operations_acquisition'),
                (LedgerRepository,'_verify_history','ledger_full_history_replay'),
                (cold.ColdRuntimeV01,'_restore_economics','economic_reconstruction'),
                (startup,'_source_preflight','source_integrity_preflight'),
                (cold.LiveContinuousProducerV02,'__init__','producer_reconstruction'),
                (cold.SourceEvidenceStore,'__init__','source_history_reconstruction'),
                (OperationsMonitor,'__init__','monitor_initialization')):
                stack.enter_context(patch.object(obj,name,timed(label,getattr(obj,name))))
            args=build_configuration(record)
            store=OperationsStore(args['operations_path'],args['domain'])
            previous=store.snapshot()
            start=startup.start_runtime(**args,process_identity='isolated-joint-measure:'+str(os.getpid()),
                now_us=previous['last_control_us']+1,replace_generation=previous['generation'])
            assert start.runtime._operations_degradation.store is not None,'ORIGINAL_MONITOR_COLD_REPLAY_NOT_REACHED'
            from types import SimpleNamespace
            wrong_identity=replace(start.audit.identity,configuration_digest='f'*64)
            negative=OperationsMonitor(SimpleNamespace(audit=replace(start.audit,identity=wrong_identity)),
                args['degradation_config'],source_binding=args['source_binding'],source_profile=args['source_profile'],
                producer_profile=args['producer_profile'])
            assert negative.store is None and negative.snapshot().unavailable_code is not None
            original_started=time.perf_counter_ns()
            # Explicit fixture profile seam: original typed constructor already
            # checked its exact identity above; no reviewed public profile is
            # manufactured for historical synthetic stores.
            fixture_record={'inputs':{'driver':dict(endpoint='https://fixture.invalid',
                public_rpc_profile=asdict(wallet_fixture.PROFILE),quote_policy={'slippage_bps':100},
                plan_policy=dict(message_format='LEGACY',setup_policy='SDK_COMPAT_IDEMPOTENT_V01',
                    recipient_selection='HASH_BOUND_INDEX_V01',volume_tracking=True,commitment='confirmed'),
                compute=dict(units=250000,micro_lamports=1000))}}
            profile=SupportedDryProfile(json.dumps(fixture_record))
            stack.enter_context(patch.object(SupportedDryProfile,'configuration',lambda self:args))
            approved_clock=ROOT/'docs/live/MEME_LIVE_T010_APPROVED_CLOCK_PROFILE_V1.json'
            clock_record=json.loads(approved_clock.read_text())
            clock_record['host_identity_digest']=args['degradation_config'].host_identity_digest
            clock_record['review_reference']='TEST_ONLY_CONSTRUCTOR_HOST_BINDING_NOT_AN_APPROVAL'
            clock_path=config_path.parent/'test-only-clock-constructor.json';write(clock_path,clock_record)
            settings=dict(clock_profile={'path':str(clock_path),'sha256':sha(clock_path)},
                protective_timing={'path':str(config_path),'sha256':sha(config_path)},venue='PUMP',
                token_program=TOKEN_PROGRAM_ID,minimum_context_slot=args['domain'].minimum_context_slot,source_lag_us=0)
            def forbidden(*a,**k):raise AssertionError('PUBLIC_NETWORK_FORBIDDEN')
            transport=httpx.MockTransport(forbidden)
            stack.enter_context(patch.object(httpx.HTTPTransport,'handle_request',side_effect=forbidden))
            stack.enter_context(patch.object(host,'ReviewedPublicFacts',lambda p,s:ReviewedPublicFacts(p,s,
                test_transport=transport,test_clock_observer=forbidden)))
            repo=start.runtime.ledger
            terminals=[]
            for disposition in repo._custody.dry_dispositions[:2]:
                receipt=repo._port_at_sequence(disposition.sequence)
                terminals.append(dict(root_id=disposition.input.root_id,terminal_digest=receipt.content_digest,
                    attempt_id=disposition.input.attempt_id,simulation_input_digest=disposition.input.simulation_input_digest))
            assert len(terminals)==2
            authority=start.audit.authority
            inputs=host.PublicHostInputs(profile,settings,authority.policy.content_digest,
                authority.grant.grant_id,content_fingerprint(asdict(authority.grant)),tuple(terminals))
            hook_start=time.perf_counter_ns()
            report=inputs.runtime_started(start,(hook_start-target_begin)//1000)
            hook_end=time.perf_counter_ns()
            events.append(dict(label='original_public_host_runtime_started',start_ns=hook_start,end_ns=hook_end))
            assert report['pending_action'] and len(report['terminal_roots'])==1
            assert inputs.facts.scope=='TEST_ONLY_FAKE_EXTERNAL_TRANSPORT'
            clock_before=inputs.facts.public_clock
            inputs.facts.begin_cold_reopen()
            clock_after=inputs.facts.public_clock
            assert clock_after.epoch!=clock_before.epoch and clock_after.policy==clock_before.policy
            assert clock_after.provider==clock_before.provider and clock_after.reviewed_reference==clock_before.reviewed_reference
            inputs.facts.startup_us=(time.perf_counter_ns()-target_begin)//1000
            ready=time.perf_counter_ns()
            fence=asdict(start.audit.owner_fence)
            print(json.dumps(dict(message='STARTED',pid=os.getpid(),target_begin_ns=target_begin,
                ready_ns=ready,fence=fence,memory=native_memory())),flush=True)
            assert sys.stdin.readline().strip()=='STEP'
            step_begin=time.perf_counter_ns()
            before_pending=start.runtime.ledger.consumer_snapshot()['reservations']
            before_attempts=tuple(start.runtime.ledger._conn.execute('SELECT attempt_id FROM ledger_attempts ORDER BY attempt_id'))
            before_source=(start.runtime.source.count(),start.runtime.source.latest().content_digest,
                start.runtime.producer.durable_p1_rowid)
            now=record['now']*1000000
            calls=0
            def clock():
                nonlocal calls
                calls+=1
                previous=start.runtime.ledger._authority.last_qualified_clock
                policy=start.runtime.ledger._authority.policy.clock
                stamp=now+calls
                if previous is None:
                    epoch='joint-initial';mono=stamp*1000;prior=ZERO_DIGEST
                else:
                    epoch=previous.epoch_id;prior=previous.content_digest
                    from live.ledger_actions_v0_1 import utc_microseconds
                    mono=previous.monotonic_ns+(stamp-utc_microseconds(previous.utc_upper_utc))*1000
                return TrustedClockSample(policy.provider_id,policy.provider_fingerprint,epoch,mono,
                    utc_from_us(stamp),utc_from_us(stamp),content_fingerprint(('joint-clock',stamp)),prior,'QUALIFIED',None)
            driver_settings=fixture_record['inputs']['driver']
            recovery_facts=DryPublicFacts(clock,None,None,QuotePolicyV01(**driver_settings['quote_policy']),
                TransactionPlanPolicyV01(**driver_settings['plan_policy']),ComputeBudget(**driver_settings['compute']),
                lambda:now,None,start.runtime.source.latest().snapshot.requested_cut_utc)
            # Only the external historical facts boundary is synthetic. The
            # original BoundPublicFacts/driver/capability checks are executed.
            lock=sqlite3.connect(args['degradation_config'].path,isolation_level=None)
            lock.execute('BEGIN EXCLUSIVE')
            contention={}
            try:
                with patch.object(ReviewedPublicFacts,'__call__',lambda self,started:recovery_facts):
                    with inputs(start) as dry_inputs:work=start.runtime.step(**dry_inputs)
                step_end=time.perf_counter_ns()
                first_step_memory=native_memory()
                monitor=start.runtime._operations_degradation
                blocked_begin=time.perf_counter_ns()
                with monitor.store.contention_window():
                    before_allowance=monitor.store._busy_seconds()
                    blocked=monitor.snapshot()
                    after_allowance=monitor.store._busy_seconds()
                    blocked_again=monitor.snapshot()
                contention=dict(elapsed_us=(time.perf_counter_ns()-blocked_begin)//1000,
                    allowance_seconds=before_allowance,remaining_seconds=after_allowance,
                    unavailable=blocked.unavailable_code,second_unavailable=blocked_again.unavailable_code)
                assert blocked.unavailable_code is not None and blocked_again.unavailable_code is not None
            finally:
                lock.rollback();lock.close()
            assert work.work in ('NON_SUBMITTED','OPERATIONS_ENTRY_HELD','SOURCE_HELD','ENTRY_HELD','ENTRY_DENIED','NEED_ENTRY_FACTS'),asdict(work)
            if before_pending:
                assert work.work=='NON_SUBMITTED' and not start.runtime.ledger.consumer_snapshot()['reservations']
                assert before_attempts==tuple(start.runtime.ledger._conn.execute('SELECT attempt_id FROM ledger_attempts ORDER BY attempt_id'))
                assert before_source==(start.runtime.source.count(),start.runtime.source.latest().content_digest,
                    start.runtime.producer.durable_p1_rowid)
            completed_report=inputs.runtime_completed(start,work)
            assert completed_report['pending_action'] is False and len(completed_report['terminal_roots'])==2
            import importlib.util
            rpc_path=EVIDENCE/'public_response_resource_v2.py'
            spec=importlib.util.spec_from_file_location('retained_rpc_fixture',rpc_path)
            rpc_fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(rpc_fixture)
            rpc_results=[rpc_fixture.exercise(case) for case in ('inventory_supported','inventory_account_max',
                'discovered_mint_max','response_exact','response_plus_one','total_response_exact','total_response_plus_one')]
            semantic=dict(reconstruction=asdict(start.runtime.reconstruction_facts()),work=asdict(work),
                audit=start.runtime.ledger.audit(),control=store.snapshot())
            memory=native_memory()
            finish=time.perf_counter_ns()
        # Exclusive buckets cover the whole target boundary; nested intervals
        # use the innermost named original call, never sum overlapping timings.
        boundaries=sorted({target_begin,ready,step_begin,step_end,finish,*[v[k] for v in events for k in ('start_ns','end_ns')]})
        exclusive={}
        for lo,hi in zip(boundaries,boundaries[1:]):
            active=[v for v in events if v['start_ns']<=lo and v['end_ns']>=hi]
            label=min(active,key=lambda v:v['end_ns']-v['start_ns'])['label'] if active else 'other_startup_or_step_and_test_audit'
            exclusive[label]=exclusive.get(label,0)+hi-lo
        write(result_path,dict(scope='ISOLATED_ORIGINAL_OWNED_DRY_STARTUP_NOT_T010_HOST_LAUNCH',
            pid=os.getpid(),target_begin_ns=target_begin,ready_ns=ready,step_begin_ns=step_begin,step_end_ns=step_end,
            target_finish_ns=finish,target_startup_us=(ready-target_begin)//1000,first_step_us=(step_end-step_begin)//1000,
            memory=memory,events=events,exclusive_ns=exclusive,semantic=semantic,
            first_step_memory=first_step_memory,contention=contention,rpc_overlap=rpc_results,
            startup_hooks=dict(original_started_ns=original_started,report=report,completed_report=completed_report,
                facts_scope=inputs.facts.scope,clock_epoch_changed=True,clock_provider_policy_unchanged=True,
                clock_scope='constructor/interface only; no packet sample or historical Ledger provider substitution',
                profile_scope='TEST_ONLY configuration seam; public profile codec/preflight qualification not claimed',
                runtime_before_start_rss_quota='NOT_CALLED_UNCONSTRAINED_MEASUREMENT_NO_NUMERIC_GUARD_SELECTED'),
            public_inputs='none; original pending DRY recovery with deterministic qualified fixture clock',
            recovery_checks=dict(original_pending=bool(before_pending),same_attempts_and_no_source_work=bool(before_pending)),
                quota='UNCONSTRAINED_NO_NEW_GUARD_SELECTED',native_limits=limits,full_physical_coverage_claimed=False,
                monitor_checks=dict(original_cold_replay_available=True,wrong_identity_denied=True)))
        print(json.dumps(dict(message='FINISHED',result=str(result_path))),flush=True)
    finally:
        if start is not None:start.close()


def measure(origin,output,repetitions):
    from live.operations_ownership_v0_1 import OperationsStore
    output.mkdir(parents=True,exist_ok=False)
    base=json.loads((origin/'configuration.json').read_text())
    original_hashes={v:sha(Path(v)) for v in base['paths'].values()}
    results=[]
    for n in range(repetitions):
        root=output/('run-'+str(n+1));root.mkdir()
        config=json.loads(json.dumps(base))
        for key,path in base['paths'].items():
            destination=root/Path(path).name
            if key!='monitor':
                with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True)) as source:
                    with closing(sqlite3.connect(destination)) as target:source.backup(target)
            config['paths'][key]=str(destination)
        config['monitor']['path']=config['paths']['monitor']
        config['live_paths']=[str(root/Path(p).name) for p in base['live_paths']]
        prepare_monitor_copy(base,config,origin,root)
        config_path=root/'configuration.json';write(config_path,config)
        result_path=root/'result.json'
        begin=time.perf_counter_ns()
        process=subprocess.Popen([sys.executable,'-B',str(Path(__file__).resolve()),'child','--config',str(config_path),'--result',str(result_path)],
            cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=(root/'stderr.txt').open('w'),text=True)
        try:
            line=process.stdout.readline();arrived=time.perf_counter_ns()
            (root/'stdout.txt').write_text(line)
            message=json.loads(line)
            assert message['message']=='STARTED' and message['pid']==process.pid
            args=build_configuration(config)
            control=OperationsStore(config['paths']['operations'],args['domain'],readonly=True).snapshot()
            assert all(control[k]==v for k,v in message['fence'].items())
            validated=time.perf_counter_ns()
            process.stdin.write('STEP\n');process.stdin.flush()
            process.stdin.close()
            remaining=process.stdout.read()
            with (root/'stdout.txt').open('a') as stream:stream.write(remaining)
            code=process.wait();assert code==0,(code,str(root/'stderr.txt'))
            value=json.loads(result_path.read_text())
            value['parent_boundary']=dict(spawn_begin_ns=begin,started_arrived_ns=arrived,
                started_validated_ns=validated,spawn_to_started_us=(arrived-begin)//1000,
                spawn_to_validated_us=(validated-begin)//1000,
                spawn_to_first_legal_unit_us=(value['step_end_ns']-begin)//1000,
                bootstrap_to_target_us=(value['target_begin_ns']-begin)//1000)
            write(result_path,value)
            results.append(dict(path=str(result_path),sha256=sha(result_path),**value['parent_boundary'],memory=value['memory']))
            print(json.dumps(results[-1]),flush=True)
        finally:
            if process.poll() is None:process.terminate();process.wait()
    assert all(sha(Path(p))==v for p,v in original_hashes.items())
    write(output/'manifest.json',dict(scope='REPEATED_FRESH_PROCESS_UNCONSTRAINED_JOINT_CONSTRUCTOR_AND_RECOVERY',
        origin=str(origin),origin_construction_sha256=sha(origin/'construction.json'),
        original_files_unchanged=original_hashes,repetitions=results,
        cache_state='OS file cache uncontrolled; copies prepared immediately before each fresh interpreter; no cache eviction or collector changes',
        limitations=['does not yet include reviewed T010 facts/profile/preflight hooks','synthetic baseline/clock only',
            'no new public-clock epoch behavior claim','post-start parent identity reconstruction included and separately timed',
            'no blocked-reader physical maximum transaction proof','test-only profile configuration and recovery facts seam',
            'original runtime_started policy/grant/terminal/facts/driver/report hooks included; no actual request or detached host'],
        source_files={str(p.relative_to(ROOT)):sha(p) for p in (Path(__file__),ROOT/'scripts/live_t010_joint_state_v0_1.py')},
        rpc_fixture={'path':str(EVIDENCE/'public_response_resource_v2.py'),'sha256':sha(EVIDENCE/'public_response_resource_v2.py')},
        guards_selected=False,public_qualification_claimed=False))


if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='command',required=True)
    c=s.add_parser('child');c.add_argument('--config',type=Path,required=True);c.add_argument('--result',type=Path,required=True)
    m=s.add_parser('measure');m.add_argument('--origin',type=Path,required=True);m.add_argument('--output',type=Path,required=True)
    m.add_argument('--repetitions',type=int,default=3)
    a=p.parse_args()
    if a.command=='child':child(a.config,a.result)
    else:measure(a.origin,a.output,a.repetitions)
