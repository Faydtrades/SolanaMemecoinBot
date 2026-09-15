"""Fresh-process full original DRY startup and first-recovery measurement.

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
            work=start.runtime.step(clock=clock,source_cut_utc=start.runtime.source.latest().snapshot.requested_cut_utc)
            step_end=time.perf_counter_ns()
            assert work.work in ('NON_SUBMITTED','OPERATIONS_ENTRY_HELD','SOURCE_HELD','ENTRY_HELD','ENTRY_DENIED','NEED_ENTRY_FACTS'),asdict(work)
            if before_pending:
                assert work.work=='NON_SUBMITTED' and not start.runtime.ledger.consumer_snapshot()['reservations']
                assert before_attempts==tuple(start.runtime.ledger._conn.execute('SELECT attempt_id FROM ledger_attempts ORDER BY attempt_id'))
                assert before_source==(start.runtime.source.count(),start.runtime.source.latest().content_digest,
                    start.runtime.producer.durable_p1_rowid)
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
            public_inputs='none; original pending DRY recovery with deterministic qualified fixture clock',
            recovery_checks=dict(original_pending=bool(before_pending),same_attempts_and_no_source_work=bool(before_pending)),
            quota='UNCONSTRAINED_NO_NEW_GUARD_SELECTED',native_limits=limits,full_physical_coverage_claimed=False))
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
            with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True)) as source:
                with closing(sqlite3.connect(destination)) as target:source.backup(target)
            config['paths'][key]=str(destination)
        config['monitor']['path']=config['paths']['monitor']
        config['live_paths']=[str(root/Path(p).name) for p in base['live_paths']]
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
            'no blocked-reader physical maximum transaction proof'],
        source_files={str(p.relative_to(ROOT)):sha(p) for p in (Path(__file__),ROOT/'scripts/live_t010_joint_state_v0_1.py')},
        guards_selected=False,public_qualification_claimed=False))


if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='command',required=True)
    c=s.add_parser('child');c.add_argument('--config',type=Path,required=True);c.add_argument('--result',type=Path,required=True)
    m=s.add_parser('measure');m.add_argument('--origin',type=Path,required=True);m.add_argument('--output',type=Path,required=True)
    m.add_argument('--repetitions',type=int,default=3)
    a=p.parse_args()
    if a.command=='child':child(a.config,a.result)
    else:measure(a.origin,a.output,a.repetitions)
