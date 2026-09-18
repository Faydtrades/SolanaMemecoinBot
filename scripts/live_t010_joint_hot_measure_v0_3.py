"""D-volume original host measurements over isolated synthetic retained stores.

Calibration is unconstrained; distinct contained runs enforce externally
derived guards. No public requests, real host launch or policy selection.
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


def physical_process_facts(runtime, operations, monitor):
    """Read original owned connection defaults; never assign a PRAGMA."""
    from decimal import getcontext
    def observed(conn):
        return {'sqlite_source_id':conn.execute('SELECT sqlite_source_id()').fetchone()[0],
            **{key:conn.execute('PRAGMA '+key).fetchone()[0]
                for key in ('threads','cache_size','temp_store','journal_mode','page_size','auto_vacuum')}}
    connections={name:observed(conn) for name,conn in
        (('ledger',runtime.ledger._conn),('source',runtime.source._conn),('producer',runtime.producer.conn))}
    for name,owner in (('operations',operations),('monitor',monitor)):
        with owner._connection() as conn:connections[name]=observed(conn)
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    api_name='GetTempPathW'  # Exact pinned SQLite Win32 VFS call.
    function=getattr(kernel,api_name)
    function.argtypes=[ctypes.c_uint32,ctypes.c_wchar_p];function.restype=ctypes.c_uint32
    buffer=ctypes.create_unicode_buffer(32768)
    size=function(len(buffer),buffer)
    if not 0<size<len(buffer):raise ctypes.WinError(ctypes.get_last_error())
    return dict(connections=connections,int_max_str_digits=sys.get_int_max_str_digits(),
        decimal_precision=getcontext().prec,temp_path_api=api_name,temp_path=buffer.value,
        scope='read-only owned connection and process settings; no temporary allocation bound inferred')


def process_instance():
    from ctypes import wintypes as w
    from phase5.shadow_domain_v0_1 import content_fingerprint
    api=ctypes.WinDLL('kernel32',use_last_error=True)
    api.GetCurrentProcess.restype=w.HANDLE
    api.GetProcessTimes.argtypes=[w.HANDLE]+[ctypes.POINTER(w.FILETIME)]*4
    values=[w.FILETIME() for _ in range(4)]
    if not api.GetProcessTimes(api.GetCurrentProcess(),*[ctypes.byref(v) for v in values]):
        raise ctypes.WinError(ctypes.get_last_error())
    value={'pid':os.getpid(),'creation_filetime':(values[0].dwHighDateTime<<32)|values[0].dwLowDateTime}
    return dict(value,digest=content_fingerprint(value))


def write(path,record):
    path.write_text(json.dumps(record,sort_keys=True,indent=2)+'\n',encoding='utf-8')


def generate_c2_codec_fixture(historical,output,certificate):
    """Create synthetic codec input data, never empirical qualification evidence.

    Production validators run unchanged. Only the enclosing TEST_ONLY fixture
    owns these invented process records; real measurements must use other IDs.
    """
    from live.runtime_dry_profile_v0_1 import read_reference
    from live.t010_resource_envelope_v0_1 import derive,bound_inputs,OBSERVATION_POLICY,OBSERVATION_METRICS
    from live.t010_resource_measurement_v0_1 import runtime_code_digest,logical_digest,SCENARIOS,SCENARIO_METRICS
    from live.t010_resource_environment_v0_1 import native_identity
    from phase5.shadow_domain_v0_1 import content_fingerprint
    historical=historical.resolve();output=output.resolve()
    source_ref={'path':str(historical),'sha256':sha(historical)}
    source=read_reference(source_ref)
    assert source['scope']=='TEST_ONLY_PROFILE_CODEC_COST_NOT_QUALIFICATION'
    assert source['synthetic_process_records'] is True
    assert output.name.startswith('codec-cost-current-') and not output.exists(),'NEW_TEST_ONLY_CODEC_DIRECTORY_REQUIRED'
    inputs=json.loads(json.dumps(source['inputs']))
    old_envelope=read_reference(inputs['resource_envelope'])
    old_physical=read_reference(old_envelope['physical_evidence'])
    environment=read_reference(old_physical['environment'])
    certificate=certificate.resolve()
    certificate_ref={'path':str(certificate),'sha256':sha(certificate)}
    from live.t010_physical_certificate_v0_1 import CERTIFICATE_SHA256,validate_model
    assert certificate_ref['sha256']==CERTIFICATE_SHA256,'CURRENT_PINNED_CODEC_CERTIFICATE_REQUIRED'
    assert environment['native']==native_identity(),'T010_RESOURCE_NATIVE_BINDING_CHANGED'
    runtime=runtime_code_digest();native=content_fingerprint(environment['native'])
    monitor=read_reference(inputs['accepted_monitor'])['monitor']
    inputs['monitor'].update(resource_max_age_us=monitor['resource_max_age_us'],
        recovery_evidence_max_age_us=monitor['policy']['recovery_evidence_max_age_us'],
        protective_qualification_digest=monitor['protective_qualification_digest'])
    calculated=derive(inputs,read_reference(inputs['accepted_profile']),read_reference(inputs['accepted_extension']))
    for key in OBSERVATION_METRICS:calculated['resource_limits'].pop(key,None)
    inputs['monitor']['resource_limits']=calculated['resource_limits']
    binding=bound_inputs(inputs);derivation_digest=content_fingerprint(calculated)
    validate_model(dict(scope='ALL_GROWABLE_STORES_PHYSICAL_UPPER_BOUND',
        derivation_digest=derivation_digest,logical_digest=logical_digest(calculated),certificate=certificate_ref),
        calculated,environment)
    output.mkdir(parents=True)
    generated=[]
    def emit(name,value):
        path=output/('TEST_ONLY_'+name+'.json')
        assert not path.exists(),'CODEC_FIXTURE_OVERWRITE_DENIED'
        write(path,value);ref={'path':str(path),'sha256':sha(path)};generated.append(ref)
        return ref
    series={key:[] for key in OBSERVATION_METRICS};scenarios={};identities=[]
    for scenario in sorted(SCENARIOS):
        scenarios[scenario]=[]
        for repetition in range(3):
            synthetic_instance={'pid':len(identities)+1,'creation_filetime':int(content_fingerprint(
                {'scope':'CODEC_ONLY_NOT_A_REAL_PROCESS','output':str(output),'scenario':scenario,'repetition':repetition})[:15],16)+1}
            identity=content_fingerprint(synthetic_instance)
            synthetic_instance['digest']=identity
            identities.append(identity)
            # Deliberately invented, independently generated values. No old
            # observation or process identity is re-labelled with current code.
            metrics={key:(1000000000 if key in ('HOST_RSS_BYTES','child_private_bytes') else 10000000)+repetition
                for key in SCENARIO_METRICS[scenario]}
            original=emit(scenario+'_'+str(repetition)+'_original',dict(
                scope='TEST_ONLY_CODEC_SYNTHETIC_PROCESS_NOT_EMPIRICAL',synthetic_process_records=True,
                process_instance=synthetic_instance,runtime_code_digest=runtime,native_digest=native,
                measurement_harness_sha256=sha(Path(__file__)),prepared_codec_cost_binding=source_ref,
                effective_measurement_codec_cost_binding=source_ref,
                codec_cost=[dict(reference=source_ref,start_ns=1,end_ns=2,rebuilt_content_digest='0'*64)],
                step_end_ns=(10000010+repetition)*1000,
                parent_boundary=dict(spawn_begin_ns=0,started_arrived_ns=1,
                    started_validated_ns=metrics['STARTUP_US']*1000,spawn_to_validated_us=metrics['STARTUP_US'],
                    spawn_to_first_legal_unit_us=10000010+repetition),first_step_us=10000020+repetition))
            normalized=emit(scenario+'_'+str(repetition),dict(
                schema='MEME_LIVE_T010_NORMALIZED_PROCESS_MEASUREMENT_V1',scenario=scenario,
                scope='TEST_ONLY_CODEC_SYNTHETIC_PROCESS_NOT_EMPIRICAL',synthetic_process_records=True,
                runtime_code_digest=runtime,native_digest=native,exit_code=0,fresh_process=True,
                process_instance_digest=identity,process_instance=synthetic_instance,
                metrics=metrics,original_result=original,unresolved_constraints=[],codec_cost_call_count=1,
                measurement_provenance=dict(schema='MEME_LIVE_EMPIRICAL_PROCESS_PROVENANCE_V1',
                    kind='CODEC_STRUCTURE_ONLY',original_result=original,process_instance_digest=identity,
                    measurement_harness_sha256=sha(Path(__file__)),prepared_codec_cost_binding=source_ref,
                    effective_measurement_codec_cost_binding=source_ref)))
            scenarios[scenario].append(normalized)
            for key in OBSERVATION_METRICS:
                series[key].append(10000010+repetition if key=='parent_first_legal_unit_us' else
                    10000020+repetition if key=='first_runtime_step_us' else metrics[key])
    observations=emit('observations',dict(schema='MEME_LIVE_T010_RESOURCE_OBSERVATIONS_V1',
        runtime_code_digest=runtime,native_digest=native,scenarios=scenarios,series=series))
    boundary=emit('observed_boundary',dict(schema='MEME_LIVE_T010_OBSERVED_RESOURCE_BOUNDARY_V1',
        scope='ORIGINAL_MAXIMUM_STATE_BOUNDARY_SCENARIOS',derivation_digest=derivation_digest,
        runtime_code_digest=runtime,measurement=observations,scenarios=scenarios,unresolved_constraints=[]))
    environment_ref=emit('environment',environment)
    model=emit('model',dict(scope='ALL_GROWABLE_STORES_PHYSICAL_UPPER_BOUND',
        derivation_digest=derivation_digest,logical_digest=logical_digest(calculated),certificate=certificate_ref))
    physical=emit('physical',dict(schema='MEME_LIVE_T010_MAXIMUM_STATE_RESOURCE_VALIDATION_V1',
        scope='ISOLATED_ORIGINAL_BOUNDARY_VALIDATION',bound_inputs_digest=binding,
        derivation_digest=derivation_digest,coverage=calculated['physical_validation_required'],
        physical_size_model=model,boundary_evidence=boundary,host_identity_digest=old_physical['host_identity_digest'],
        runtime_code_digest=runtime,maxima={k:max(v) for k,v in series.items()},
        disk_reserve_minimum_bytes=calculated['resource_limits']['HOST_DISK_RESERVE_BYTES'],
        environment=environment_ref,unresolved_constraints=[]))
    inputs['resource_envelope']=emit('envelope',dict(schema='MEME_LIVE_T010_RESOURCE_ENVELOPE_V1',
        scope='PUBLIC_ENVIRONMENT_REVIEWED',bound_inputs_digest=binding,derivation=calculated,
        observation_policy=OBSERVATION_POLICY,observation_evidence=observations,physical_evidence=physical,
        authorization_reference='TEST_ONLY_CODEC_COST_NOT_QUALIFICATION_AUTHORIZATION',project_acceptance_claimed=False))
    codec=emit('codec_inputs',dict(scope='TEST_ONLY_PROFILE_CODEC_COST_NOT_QUALIFICATION',inputs=inputs,
        synthetic_process_records=True,stores_initialized=False,qualification_evidence=False,
        empirical_resource_observation=False,final_observation_cohort_eligible=False))
    provenance=dict(scope='TEST_ONLY_CODEC_COST_NOT_QUALIFICATION',generator_script=str(Path(__file__).resolve()),
        generator_script_sha256=sha(Path(__file__)),starting_historical_codec_input=source_ref,
        runtime_code_digest=runtime,native_digest=native,generated_json=generated,
        current_certificate=certificate_ref,synthetic_process_identities=identities,qualification_evidence=False,empirical_resource_observation=False,
        final_observation_cohort_eligible=False,historical_artifacts_modified=False,codec_input=codec)
    write(output/'fixture-provenance.json',provenance)
    assert sha(historical)==source_ref['sha256'],'HISTORICAL_CODEC_INPUT_CHANGED'
    return codec


def validate_retained_codec_binding(value,effective):
    """Only completed calls in the actual original result prove codec coverage."""
    calls=value['codec_cost']
    assert type(calls) is list and (len(calls)>=1 if effective is not None else not calls),'RETAINED_CODEC_CALL_COUNT_CHANGED'
    for call in calls:
        assert call['reference']==effective,'RETAINED_CODEC_INPUT_CHANGED'
        assert type(call['start_ns']) is int and type(call['end_ns']) is int and 0<=call['start_ns']<=call['end_ns'],'RETAINED_CODEC_CALL_INCOMPLETE'
        assert type(call['rebuilt_content_digest']) is str and len(call['rebuilt_content_digest'])==64,'RETAINED_CODEC_CALL_INCOMPLETE'


def validate_codec_cost_input(reference):
    """The child and cheap preflight share this full unmodified codec boundary."""
    from live.runtime_dry_profile_v0_1 import read_reference,construct_codec_cost_graph
    began=time.perf_counter_ns()
    value=read_reference(reference)
    assert value['scope']=='TEST_ONLY_PROFILE_CODEC_COST_NOT_QUALIFICATION'
    assert value.get('synthetic_process_records') is True,'EXPLICIT_CODEC_COST_FIXTURE_REQUIRED'
    # Current native binding is checked by physical validation inside _construct.
    rebuilt=construct_codec_cost_graph(value['inputs'])
    return dict(start_ns=began,end_ns=time.perf_counter_ns(),reference=reference,
        rebuilt_content_digest=rebuilt['content_digest'],memory=native_memory())


def collect_c2_observations(manifests,output):
    """Only completed original harness sequences may supply empirical inputs.

    Codec fixtures are deliberately accepted by shape validators for costing,
    but cannot enter through this collection boundary. No result is relabelled.
    """
    from live.runtime_dry_profile_v0_1 import read_reference
    from live.t010_resource_measurement_v0_1 import SCENARIOS,runtime_code_digest
    from live.t010_resource_environment_v0_1 import native_identity
    from live.t010_resource_envelope_v0_1 import OBSERVATION_METRICS,validate_observations
    from phase5.shadow_domain_v0_1 import content_fingerprint
    assert len(manifests)==4 and not output.exists(),'FOUR_NEW_EMPIRICAL_SEQUENCES_REQUIRED'
    runtime=runtime_code_digest();native=content_fingerprint(native_identity())
    scenarios={};instances=set();originals={};used=[];cohort_codec=None
    provenance_path=output.with_name(output.name+'.provenance.json')
    assert not provenance_path.exists(),'EMPIRICAL_PROVENANCE_EXISTS'
    for path in manifests:
        path=path.resolve();manifest=json.loads(path.read_text())
        assert manifest.get('scope')=='REPEATED_FRESH_PROCESS_UNCONSTRAINED_JOINT_CONSTRUCTOR_AND_RECOVERY','EMPIRICAL_SEQUENCE_REQUIRED'
        assert manifest.get('measurement_harness_sha256')==sha(Path(__file__)),'EMPIRICAL_HARNESS_CHANGED'
        assert len(manifest['repetitions'])==3,'EMPIRICAL_THREE_REPETITIONS_REQUIRED'
        binding=manifest['effective_measurement_codec_cost_binding']
        assert binding is not None and (cohort_codec is None or binding==cohort_codec),'EMPIRICAL_CODEC_BINDING_CHANGED'
        read_reference(binding);cohort_codec=binding
        scenario=None;references=[]
        for row in manifest['repetitions']:
            original_ref={'path':row['path'],'sha256':row['sha256']}
            value=read_reference(original_ref);normalized_path=Path(row['path']).parent/'normalized.json'
            ref={'path':str(normalized_path),'sha256':row['normalized_sha256']};normalized=read_reference(ref)
            assert normalized.get('scope')=='SYNTHETIC_EXTERNAL_FACTS_ORIGINAL_CONSTRUCTORS_AND_CODEC_COST_NOT_PUBLIC_QUALIFICATION','CODEC_SYNTHETIC_OBSERVATION_DENIED'
            assert not normalized.get('synthetic_process_records') and not value.get('synthetic_process_records'),'CODEC_SYNTHETIC_OBSERVATION_DENIED'
            assert value.get('measurement_harness_sha256')==manifest['measurement_harness_sha256'],'EMPIRICAL_HARNESS_CHANGED'
            assert normalized['original_result']==original_ref and normalized['unresolved_constraints']==[],'EMPIRICAL_ORIGINAL_BINDING_REQUIRED'
            assert normalized['runtime_code_digest']==value['runtime_code_digest']==runtime and normalized['native_digest']==value['native_digest']==native,'EMPIRICAL_CURRENT_IDENTITY_REQUIRED'
            assert normalized['exit_code']==0 and normalized['fresh_process'] is True,'EMPIRICAL_SUCCESS_REQUIRED'
            pi=value['process_instance'];identity=content_fingerprint({'pid':pi['pid'],'creation_filetime':pi['creation_filetime']})
            assert type(pi['pid']) is int and pi['pid']>0 and type(pi['creation_filetime']) is int and pi['creation_filetime']>0,'EMPIRICAL_NATIVE_PROCESS_REQUIRED'
            assert pi['digest']==normalized['process_instance_digest']==identity and identity not in instances,'EMPIRICAL_DISTINCT_PROCESS_REQUIRED'
            instances.add(identity)
            assert value['effective_measurement_codec_cost_binding']==manifest['effective_measurement_codec_cost_binding'],'EMPIRICAL_CODEC_BINDING_CHANGED'
            validate_retained_codec_binding(value,manifest['effective_measurement_codec_cost_binding'])
            closed=json.loads((normalized_path.parent/'closed-state-manifest.json').read_text())
            assert closed['files'] and set(closed['physical'])=={'ledger','source','producer','operations','monitor'},'EMPIRICAL_CLOSED_STATE_REQUIRED'
            assert all(x['pragmas']['integrity_check']=='ok' for x in closed['physical'].values()),'EMPIRICAL_CLOSED_STATE_REQUIRED'
            assert normalized['scenario'] in SCENARIOS and (scenario is None or scenario==normalized['scenario']),'EMPIRICAL_SCENARIO_CONFLICT'
            scenario=normalized['scenario'];references.append(ref);originals[identity]=value
        assert scenario not in scenarios,'EMPIRICAL_SCENARIO_REPEATED'
        scenarios[scenario]=references;used.append({'path':str(path),'sha256':sha(path)})
    assert set(scenarios)==set(SCENARIOS),'EMPIRICAL_COMPLETE_SCENARIOS_REQUIRED'
    series={k:[] for k in OBSERVATION_METRICS}
    for scenario in sorted(SCENARIOS):
        for ref in scenarios[scenario]:
            value=read_reference(ref);original=originals[value['process_instance_digest']]
            for key in series:
                series[key].append(original['parent_boundary']['spawn_to_first_legal_unit_us'] if key=='parent_first_legal_unit_us' else original['first_step_us'] if key=='first_runtime_step_us' else value['metrics'][key])
    record=dict(schema='MEME_LIVE_T010_RESOURCE_OBSERVATIONS_V1',runtime_code_digest=runtime,native_digest=native,scenarios=scenarios,series=series)
    pending=output.with_name(output.name+'.pending')
    assert not pending.exists(),'EMPIRICAL_OUTPUT_ALREADY_PENDING'
    write(pending,record)
    validate_observations({'path':str(pending),'sha256':sha(pending)})
    assert not output.exists(),'EMPIRICAL_OUTPUT_EXISTS'
    pending.rename(output)
    write(provenance_path,dict(scope='ORIGINAL_EMPIRICAL_COLLECTION_NOT_PUBLIC_QUALIFICATION',manifests=used,measurement_harness_sha256=sha(Path(__file__)),observations={'path':str(output),'sha256':sha(output)},synthetic_codec_records_eligible=False))
    return {'path':str(output),'sha256':sha(output)}


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


def before_start_io(config_path,guards=None):
    """Original before-start quota-receipt path; calibration omits enforcement.

    The minimal test descriptor is only consumed by this original method.
    Actual profile validation is separately executed in full in the child.
    """
    from types import SimpleNamespace
    from live.t010_public_host_v0_1 import PublicHostInputs
    from live import t010_resource_envelope_v0_1 as resource
    envelope=config_path.parent/'TEST_ONLY-before-start-envelope.json'
    write(envelope,{'derivation':{'measured_guards':guards}})
    original=json.loads(config_path.read_text())
    prior_rss=next(v['limit'] for v in original['monitor']['policy']['resource_limits'] if v['metric']=='HOST_RSS_BYTES')
    profile=SimpleNamespace(record={'inputs':{'monitor':{'resource_limits':{
        'HOST_RSS_BYTES':guards['HOST_RSS_BYTES'] if guards else prior_rss}},
        'resource_envelope':{'path':str(envelope),'sha256':sha(envelope)}}})
    descriptor=SimpleNamespace(profile=profile,resource_path=str(config_path.parent/'resource_work.json'))
    began=time.perf_counter_ns()
    with ExitStack() as stack:
        if guards is None:
            stack.enter_context(patch.object(resource,'enforce_current_process_rss',lambda limit:{
                'scope':'UNCONSTRAINED_CALIBRATION_NO_RSS_LIMIT_INSTALLED','prior_policy_argument_only':limit}))
        PublicHostInputs.runtime_before_start(descriptor)
    path=config_path.parent/'child_rss_quota.json'
    return {'elapsed_us':(time.perf_counter_ns()-began)//1000,'receipt':{'path':str(path),'sha256':sha(path)},
        'original_method_called':True,'enforcement':'ORIGINAL_NATIVE_HELPERS' if guards else 'EXPLICITLY_OMITTED_FOR_CALIBRATION'}


def child(config_path,result_path,codec_cost_inputs=None,guards_path=None,startup_only=False):
    target_begin=time.perf_counter_ns()
    quota=None
    guards=None if guards_path is None else json.loads(guards_path.read_text())
    if guards is not None:
        from live.t010_resource_envelope_v0_1 import enforce_current_process_rss,enforce_current_process_private_bytes,enforce_host_tree_private_bytes
        from live.operations_windows_host_v0_1 import WindowsHostBoundary
        from phase5.shadow_domain_v0_1 import content_fingerprint
        containment=WindowsHostBoundary(content_fingerprint(('ISOLATED_STRUCTURAL_MEASUREMENT',process_instance()))).enter()
        quota={'rss':enforce_current_process_rss(guards['HOST_RSS_BYTES']),
            'tree':enforce_host_tree_private_bytes(containment,guards),
            'private':enforce_current_process_private_bytes(guards['child_private_bytes']),
            'guards':guards}
    before_start=before_start_io(config_path,guards)
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
    if codec_cost_inputs is not None:record['codec_cost_inputs']={'path':str(codec_cost_inputs),'sha256':sha(codec_cost_inputs)}
    instance=process_instance()
    limits=native_limits()
    if guards is None:
        assert not limits['hard_working_set_maximum_enabled'] and not limits.get('current_job_flags',0)&0x300,limits
    events=[]
    codec_cost=[]
    def validate_profile_cost():
        reference=record.get('codec_cost_inputs')
        if reference is None:return
        codec_cost.append(validate_codec_cost_input(reference))
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
            validate_profile_cost()
            args=build_configuration(record)
            store=OperationsStore(args['operations_path'],args['domain'])
            previous=store.snapshot()
            start=startup.start_runtime(**args,process_identity='isolated-joint-measure:'+str(os.getpid()),
                now_us=previous['last_control_us']+1,replace_generation=previous['generation'])
            if start.runtime._operations_degradation.store is None:
                monitor=args['degradation_config']
                failure=dict(error='ORIGINAL_MONITOR_COLD_REPLAY_NOT_REACHED',
                    unavailable_code=start.runtime._operations_degradation.snapshot().unavailable_code,
                    monitor_path=monitor.path,
                    retained_configuration_binding=monitor.policy.reviewed_configuration_digest,
                    required_configuration_binding=monitor.binding_for(start.audit.identity))
                from live.operations_degradation_v0_1 import DegradationStore
                try:
                    DegradationStore(monitor.path,args['domain'],monitor.policy).snapshot()
                except Exception as exc:
                    failure['store_error']=type(exc).__name__+': '+str(exc)
                raise AssertionError(json.dumps(failure,sort_keys=True))
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
                public_rpc_profile=record.get('rpc_profile',asdict(wallet_fixture.PROFILE)),quote_policy={'slippage_bps':100},
                plan_policy=dict(message_format='LEGACY',setup_policy='SDK_COMPAT_IDEMPOTENT_V01',
                    recipient_selection='HASH_BOUND_INDEX_V01',volume_tracking=True,commitment='confirmed'),
                compute=dict(units=250000,micro_lamports=1000))}}
            fixture_record['startup_identity']=asdict(start.audit.identity)
            from live.t010_resource_envelope_v0_1 import derive
            from live_t010_resource_envelope_selftest_v0_1 import fixture as resource_fixture
            envelope_path=config_path.parent/'test-only-resource-derivation.json'
            write(envelope_path,{'derivation':derive(*resource_fixture())})
            fixture_record['inputs']['resource_envelope']={'path':str(envelope_path),'sha256':sha(envelope_path)}
            profile=SupportedDryProfile(json.dumps(fixture_record))
            def historical_configuration(self):
                validate_profile_cost()
                return args
            stack.enter_context(patch.object(SupportedDryProfile,'configuration',historical_configuration))
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
                authority.grant.grant_id,content_fingerprint(asdict(authority.grant)),tuple(terminals),
                resource_path=record['resource_snapshot'])
            hook_start=time.perf_counter_ns()
            report=inputs.runtime_started(start,(hook_start-target_begin)//1000)
            hook_end=time.perf_counter_ns()
            events.append(dict(label='original_public_host_runtime_started',start_ns=hook_start,end_ns=hook_end))
            assert len(report['terminal_roots'])==1
            assert inputs.facts.scope=='TEST_ONLY_FAKE_EXTERNAL_TRANSPORT'
            reported_startup_us=inputs.facts.startup_us
            assert reported_startup_us >= (hook_start-target_begin)//1000
            # Exercise the explicit reopen interface on a separate test probe;
            # reopening the just-started live facts object intentionally clears
            # its startup metric and would invalidate this measured boundary.
            clock_probe=ReviewedPublicFacts(profile,settings,test_transport=transport,test_clock_observer=forbidden)
            clock_before=clock_probe.public_clock
            clock_probe.begin_cold_reopen()
            clock_after=clock_probe.public_clock
            assert clock_after.epoch!=clock_before.epoch and clock_after.policy==clock_before.policy
            assert clock_after.provider==clock_before.provider and clock_after.reviewed_reference==clock_before.reviewed_reference
            assert inputs.facts.startup_us==reported_startup_us
            # Test-only colocated original parent controls: finite reservations
            # and exact child reports, no actual detached host or supervisor.
            budget_begin=time.perf_counter_ns()
            session={'work_budget':derive(*resource_fixture())['work_budget'],
                'resource_envelope':fixture_record['inputs']['resource_envelope']}
            budget=host.WorkBudget(config_path.parent,session,create=True)
            assert budget.before_host_entry() and budget.before_start()
            budget.after_start(report)
            events.append(dict(label='test_colocated_parent_work_budget_setup',start_ns=budget_begin,end_ns=time.perf_counter_ns()))
            ready=time.perf_counter_ns()
            fence=asdict(start.audit.owner_fence)
            print(json.dumps(dict(message='STARTED',pid=os.getpid(),target_begin_ns=target_begin,
                ready_ns=ready,fence=fence,memory=native_memory())),flush=True)
            if startup_only:
                write(result_path,dict(scope='COMMITTED_MEASUREMENT_PREFIX_STARTUP_ONLY',
                    process_instance=instance,target_startup_us=(ready-target_begin)//1000,
                    memory=native_memory(),native_limits=limits,events=events,
                    monitor_cold_replay_available=True,completed_builder_target=False,
                    resource_qualification_claimed=False))
                return
            assert sys.stdin.readline().strip()=='STEP'
            step_begin=time.perf_counter_ns()
            before_pending=start.runtime.ledger.consumer_snapshot()['reservations']
            before_attempts=tuple(start.runtime.ledger._conn.execute('SELECT attempt_id FROM ledger_attempts ORDER BY attempt_id'))
            before_source=(start.runtime.source.count(),start.runtime.source.latest().content_digest,
                start.runtime.producer.durable_p1_rowid)
            now=record['now']*1000000
            calls=0
            clock_captures={};age_evidence={'host':[],'recovery':[]}
            def clock():
                nonlocal calls
                calls+=1
                previous=start.runtime.ledger._authority.last_qualified_clock
                policy=start.runtime.ledger._authority.policy.clock
                stamp=now+(time.perf_counter_ns()-step_begin)//1000+calls
                clock_captures[stamp]=time.perf_counter_ns()
                if previous is None:
                    epoch='joint-initial';mono=stamp*1000;prior=ZERO_DIGEST
                else:
                    epoch=previous.epoch_id;prior=previous.content_digest
                    from live.ledger_actions_v0_1 import utc_microseconds
                    mono=previous.monotonic_ns+(stamp-utc_microseconds(previous.utc_upper_utc))*1000
                return TrustedClockSample(policy.provider_id,policy.provider_fingerprint,epoch,mono,
                    utc_from_us(stamp),utc_from_us(stamp),content_fingerprint(('joint-clock',stamp)),prior,'QUALIFIED',None)
            from live.operations_degradation_v0_1 import DegradationStore
            original_record=DegradationStore.record
            def measured_record(owner,evidence=None,**kw):
                entered=time.perf_counter_ns()
                try:return original_record(owner,evidence,**kw)
                finally:
                    if evidence is not None and evidence.state=='RECOVERED':
                        captured=clock_captures.get(evidence.observed_us)
                        age_evidence['recovery'].append(dict(observed_us=evidence.observed_us,
                            original_ingestion_age_us=kw['now_us']-evidence.observed_us,
                            capture_to_return_us=None if captured is None else (time.perf_counter_ns()-captured)//1000,
                            record_call_us=(time.perf_counter_ns()-entered)//1000))
            stack.enter_context(patch.object(DegradationStore,'record',measured_record))
            driver_settings=fixture_record['inputs']['driver']
            recovery_facts=DryPublicFacts(clock,None,None,QuotePolicyV01(**driver_settings['quote_policy']),
                TransactionPlanPolicyV01(**driver_settings['plan_policy']),ComputeBudget(**driver_settings['compute']),
                lambda:now,None,start.runtime.source.latest().snapshot.requested_cut_utc)
            # Only the external historical facts boundary is synthetic. The
            # original BoundPublicFacts/driver/capability checks are executed.
            lock=sqlite3.connect(args['degradation_config'].path,isolation_level=None)
            lock.execute('BEGIN EXCLUSIVE')
            contention={}
            checkpoint_reader=None;checkpoint_recovery={}
            try:
                if before_pending:
                    checkpoint_reader=sqlite3.connect(Path(args['ledger_path']).as_uri()+'?mode=ro',uri=True,isolation_level=None)
                    checkpoint_reader.execute('BEGIN')
                    pinned=checkpoint_reader.execute('SELECT * FROM ledger_head').fetchall()
                    checkpoint_began=time.perf_counter_ns()
                    assert not inputs.resource_gate._checkpoint_boundary(start)
                    checkpoint_recovery=dict(checkpoint_failure_us=(time.perf_counter_ns()-checkpoint_began)//1000,
                        held_reason=inputs.resource_gate.snapshot()['held_reason'])
                parent_dispatch=budget.before_step()
                admitted_step=inputs.runtime_before_step(start)
                assert parent_dispatch==admitted_step
                if admitted_step:
                    assert before_pending,'HOT_FIXTURE_UNEXPECTED_NEW_ENTRY_SOURCE_PERMISSION'
                    with patch.object(ReviewedPublicFacts,'__call__',lambda self,started:recovery_facts):
                        with inputs(start) as dry_inputs:work=start.runtime.step(**dry_inputs)
                    work_record=asdict(work)
                else:
                    assert not before_pending
                    work_record=dict(work='RESOURCE_HELD',reason=inputs.resource_gate.snapshot()['held_reason'],
                        runtime_step_called=False)
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
            assert work_record['work'] in ('NON_SUBMITTED','RESOURCE_HELD'),work_record
            if before_pending:
                assert work.work=='NON_SUBMITTED' and not start.runtime.ledger.consumer_snapshot()['reservations']
                assert before_attempts==tuple(start.runtime.ledger._conn.execute('SELECT attempt_id FROM ledger_attempts ORDER BY attempt_id'))
                assert before_source==(start.runtime.source.count(),start.runtime.source.latest().content_digest,
                    start.runtime.producer.durable_p1_rowid)
            else:
                assert before_attempts==tuple(start.runtime.ledger._conn.execute('SELECT attempt_id FROM ledger_attempts ORDER BY attempt_id'))
                assert before_source==(start.runtime.source.count(),start.runtime.source.latest().content_digest,
                    start.runtime.producer.durable_p1_rowid)
            completed_report=inputs.runtime_completed(start,work) if admitted_step else inputs._work_report(start)
            if admitted_step:budget.after_step(completed_report)
            if checkpoint_reader is not None:
                assert checkpoint_reader.execute('SELECT * FROM ledger_head').fetchall()==pinned
                checkpoint_reader.rollback();checkpoint_reader.close();checkpoint_reader=None
                assert not budget.before_step() and not budget.before_start()
                checkpoint_recovery.update(original_pending_recovered=True,reader_snapshot_preserved=True,
                    original_parent_dispatch=parent_dispatch,ordinary_work_not_rearmed=True,final_budget=budget.snapshot())
            assert completed_report['pending_action'] is False and len(completed_report['terminal_roots'])==(2 if before_pending else 1)
            import importlib.util
            sys.path.insert(0,str(EVIDENCE))
            rpc_path=EVIDENCE/'public_response_resource_v3.py'
            spec=importlib.util.spec_from_file_location('retained_rpc_fixture',rpc_path)
            rpc_fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(rpc_fixture)
            rpc_results=[rpc_fixture.exercise(case) for case in rpc_fixture.CASES]
            # Actual bounded record shapes through the original JSON writer,
            # conservatively colocated with retained child heap. No filler or
            # invented16MiB record is claimed reachable by the host schema.
            host_record_begin=time.perf_counter_ns()
            host_status={'session_digest':content_fingerprint(session),'state':budget.observation_state,
                'observed_utc_us':now,'completed_steps':budget.snapshot()['runtime_steps_completed'],
                'generation':start.audit.owner_fence.generation,'host_launch_id':'SYNTHETIC_MEASUREMENT',
                'supervisor_pid':os.getpid(),'detached_from_launcher_job':False,
                'alerts':host._jsonable(start.runtime._operations_degradation.snapshot()),'work_budget':budget.snapshot()}
            status_path=config_path.parent/'test-only-host-status.json'
            host.write_record(status_path,host_status)
            assert host.read_record(status_path)==json.loads(json.dumps(host_status))
            assert not status_path.with_name(status_path.name+'.tmp').exists()
            host_record_overlap=dict(scope='original bounded status/work-budget records; test-only parent/child heap colocation',
                status_bytes=status_path.stat().st_size,work_budget_bytes=budget.path.stat().st_size,
                elapsed_us=(time.perf_counter_ns()-host_record_begin)//1000,memory=native_memory())
            def monitor_counts():
                with closing(sqlite3.connect(Path(args['degradation_config'].path).as_uri()+'?mode=ro',uri=True)) as conn:
                    return {name:conn.execute('SELECT count(*) FROM '+name).fetchone()[0] for name in ('conditions','receipts')}
            # Original public resource sampler with a synthetic historical
            # clock and this measured original pending/hold duration. This is
            # test input provenance, never a reviewed public timing approval.
            protective_path=config_path.parent/'test-only-measured-protective-timing.json'
            write(protective_path,dict(schema='MEME_LIVE_REVIEWED_PROTECTIVE_TIMING_V1',
                host_identity_digest=monitor.configuration.host_identity_digest,
                runtime_code_digest=monitor.configuration.runtime_code_digest,
                qualification_digest=monitor.configuration.protective_qualification_digest,
                configuration_digest=start.audit.identity.configuration_digest,
                protective_step_us=(step_end-step_begin)//1000,
                scope='SYNTHETIC_EXTERNAL_FACTS_MEASURED_CURRENT_PROCESS_NOT_PUBLIC_APPROVAL'))
            inputs.facts.settings['protective_timing']={'path':str(protective_path),'sha256':sha(protective_path)}
            sample_begin=time.perf_counter_ns()
            with patch.object(ReviewedPublicFacts,'clock',lambda self,started=None:clock()):
                host_sample=inputs.facts.resources(start)
            sample_end=time.perf_counter_ns();steady_sample=clock()
            from live.ledger_actions_v0_1 import utc_microseconds
            original_host_metrics=monitor._host_metrics
            def measured_host(resources,stamp,owner):
                value=original_host_metrics(resources,stamp,owner)
                age_evidence['host'].append(dict(original_sample_age_us=max(stamp-v.observed_us for v in host_sample.metrics),
                    capture_to_consumption_us=(time.perf_counter_ns()-sample_begin)//1000,
                    original_metrics=value[0]))
                return value
            stack.enter_context(patch.object(monitor,'_host_metrics',measured_host))
            steady_before=monitor_counts();steady_begin=time.perf_counter_ns()
            start.runtime._operations_degradation.observe(steady_sample,entry=None,resources=lambda:host_sample,
                source_cut_utc=start.runtime.source.latest().snapshot.requested_cut_utc)
            steady_end=time.perf_counter_ns()
            steady_monitor=dict(scope='one extra unlocked original observation AFTER full constructor superset; post-count is NOT an allowed end-of-budget history ceiling',
                before_counts=steady_before,after_counts=monitor_counts(),elapsed_us=(steady_end-steady_begin)//1000,memory=native_memory())
            steady_monitor['original_observation_unavailable']=monitor.last.unavailable_code
            if not age_evidence['host']:
                # A capacity-held whole observation may fail before its host
                # port. Measure the unchanged typed host validator separately;
                # this never authorizes Runtime work or clears a condition.
                measured_host(lambda:host_sample,utc_microseconds(clock().utc_upper_utc),
                    content_fingerprint(asdict(start.runtime._ownership.fence)))
                age_evidence['host'][-1]['scope']='separate original host-port validation after whole observation held'
            from live.operations_degradation_monitor_v0_1 import _producer_cut
            producer_metrics,producer_manifest,_=_producer_cut(start.runtime.producer)
            fence=next(v.rowid for v in start.runtime.source.latest().snapshot.cursors if v.table=='pump_events')
            with closing(sqlite3.connect(Path(start.runtime.producer.market_source.db_path).as_uri()+'?mode=ro',uri=True)) as raw:
                assert raw.execute('SELECT max(rowid) FROM pump_events').fetchone()[0]==fence
                backlog_count,oldest=raw.execute('SELECT count(*),min(decoded_at_utc) FROM pump_events WHERE rowid>? AND rowid<=?',
                    (producer_manifest['cursor'],fence)).fetchone()
            oldest_age=None if backlog_count==0 else utc_microseconds(steady_sample.utc_upper_utc)-utc_microseconds(oldest)
            assert oldest_age is None or oldest_age>=0
            workload=dict(producer_metrics=producer_metrics,backlog_count=backlog_count,oldest_timestamp=oldest,
                oldest_unconsumed_age_us=oldest_age,source_fence=fence,producer_cursor=producer_manifest['cursor'])
            events.append(dict(label='unlocked_monitor_observation_stress_superset',start_ns=steady_begin,end_ns=steady_end))
            process_physical=physical_process_facts(start.runtime,store,monitor.store)
            from live.t010_resource_environment_v0_1 import native_identity,owned_connection_facts
            from live.t010_resource_measurement_v0_1 import runtime_code_digest
            native=native_identity()
            try:owned_native=owned_connection_facts(start.runtime)
            except Exception as exc:
                diagnostic={key:[list(r) for r in conn.execute('PRAGMA database_list')]
                    for key,conn in (('ledger',start.runtime.ledger._conn),('source',start.runtime.source._conn),
                        ('producer',start.runtime.producer.conn))}
                for key,owner in (('operations',store),('monitor',monitor.store)):
                    with owner._connection() as conn:diagnostic[key]=[list(r) for r in conn.execute('PRAGMA database_list')]
                write(result_path.with_suffix('.native-failure.json'),dict(error=type(exc).__name__,reason=str(exc),
                    databases=diagnostic,native=native,scope='original owned connection diagnostic; no settings changed'))
                raise
            semantic=dict(reconstruction=asdict(start.runtime.reconstruction_facts()),work=work_record,
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
            checkpoint_pending_recovery=checkpoint_recovery,host_record_overlap=host_record_overlap,
            steady_monitor=steady_monitor,process_physical=process_physical,
            process_instance=instance,runtime_code_digest=runtime_code_digest(),native=native,
            native_digest=content_fingerprint(native),owned_native=owned_native,workload=workload,
            age_evidence=age_evidence,
            codec_cost=codec_cost,
            before_start=before_start,
            startup_hooks=dict(original_started_ns=original_started,report=report,completed_report=completed_report,
                facts_scope=inputs.facts.scope,reported_original_full_hook_startup_us=reported_startup_us,
                clock_epoch_changed=True,clock_provider_policy_unchanged=True,
                clock_scope='constructor/interface only; no packet sample or historical Ledger provider substitution',
                profile_scope='TEST_ONLY configuration seam; public profile codec/preflight qualification not claimed',
                runtime_before_start_rss_quota=quota or 'NOT_CALLED_UNCONSTRAINED_MEASUREMENT_NO_NUMERIC_GUARD_SELECTED'),
            public_inputs='none; original pending DRY recovery with deterministic qualified fixture clock',
            recovery_checks=dict(original_pending=bool(before_pending),same_attempts_and_no_source_work=bool(before_pending)),
            original_resource_gate=dict(admitted_step=admitted_step,snapshot=inputs.resource_gate.snapshot(),
                held_without_runtime_step=not admitted_step),
                quota=quota or 'UNCONSTRAINED_NO_NEW_GUARD_SELECTED',native_limits=limits,full_physical_coverage_claimed=False,
                monitor_checks=dict(original_cold_replay_available=True,wrong_identity_denied=True)))
        print(json.dumps(dict(message='FINISHED',result=str(result_path))),flush=True)
    finally:
        if 'checkpoint_reader' in locals() and checkpoint_reader is not None:
            checkpoint_reader.rollback();checkpoint_reader.close()
        if start is not None:start.close()


def measure(origin,output,repetitions,monitor_observations=2193,prepare_only=False,monitor_staging_root=None,codec_cost_inputs=None,*,resume=False):
    from live.operations_ownership_v0_1 import OperationsStore
    if resume:
        assert prepare_only, 'RESUME_REQUIRES_PREPARE_ONLY'
        assert output.is_dir() and (output/'active').is_dir(), 'RESUME_EXISTING_OUTPUT_REQUIRED'
    else:output.mkdir(parents=True,exist_ok=False)
    base=json.loads((origin/'configuration.json').read_text())
    original_hashes={v:sha(Path(v)) for v in base['paths'].values()}
    results=[];root=output/'active'
    if not resume:root.mkdir()
    config=json.loads(json.dumps(base))
    for key,path in base['paths'].items():
        destination=root/Path(path).name
        if resume:
            if key!='monitor':assert destination.is_file(), 'RESUME_EXISTING_STORE_REQUIRED'
        elif key!='monitor':
            with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True)) as source:
                with closing(sqlite3.connect(destination)) as target:source.backup(target)
        config['paths'][key]=str(destination)
    config['monitor']['path']=config['paths']['monitor']
    config['live_paths']=[str(root/Path(p).name) for p in base['live_paths']]
    resource_copy=root/'resource_work.json'
    if resume:
        assert resource_copy.is_file(), 'RESUME_EXISTING_RESOURCE_INPUT_REQUIRED'
        assert sha(resource_copy)==sha(origin/'host-resources.json'), 'RESUME_RESOURCE_INPUT_CHANGED'
    else:shutil.copyfile(origin/'host-resources.json',resource_copy)
    config['resource_snapshot']=str(resource_copy)
    if codec_cost_inputs is not None:config['codec_cost_inputs']={'path':str(codec_cost_inputs),'sha256':sha(codec_cost_inputs)}
    from live_t010_monitor_full_state_v0_1 import prepare
    monitor_evidence=prepare(base,config,origin,root,monitor_observations,staging_root=monitor_staging_root,resume=resume)
    config_path=root/'configuration.json';write(config_path,config)
    # Initialize/rebind once through original APIs. Repetitions restore these
    # exact closed bytes to the SAME isolated paths; no policy/header rewrite
    # or original validation bypass, and no repeated live-session reset claim.
    prepared=output/'prepared-start';prepared.mkdir(exist_ok=resume)
    if resume:
        assert all(p.is_file() and (root/p.name).is_file() for p in prepared.iterdir()), 'RESUME_PREPARED_FILES_CHANGED'
    for path in root.iterdir():
        assert path.is_file() and path.resolve().parent==root.resolve()
        shutil.copy2(path,prepared/path.name)
    prepared_hashes={p.name:sha(p) for p in prepared.iterdir()}
    write(output/'prepared-start-manifest.json',{'files':prepared_hashes,'monitor':monitor_evidence,
        'origin':str(origin),'original_files_unchanged':original_hashes,
        'scope':'immutable closed measurement fixture bytes; same-path restoration between independent fresh processes'})
    if prepare_only:
        print(json.dumps({'prepared':str(output),'manifest_sha256':sha(output/'prepared-start-manifest.json')}),flush=True)
        return
    measure_prepared(output,repetitions)


def retained_prepared_repetitions(output,proof_path,repetitions):
    """Read completed parent stdout records; never promote an unfinished child."""
    results=[];instances=set()
    for line in proof_path.read_text(encoding='utf-8-sig').splitlines():
        if not line.strip():continue
        row=json.loads(line)
        number=len(results)+1;path=Path(row['path']);run_root=path.parent
        assert number<=repetitions,'TOO_MANY_RETAINED_REPETITIONS'
        assert path.name=='result.json' and run_root.resolve().parent==output.resolve(),'RETAINED_RESULT_PATH_CHANGED'
        assert (run_root.name=='run-'+str(number) or
            run_root.name.startswith('run-'+str(number)+'-continuation-')),'RETAINED_REPETITION_ORDER_CHANGED'
        assert sha(path)==row['sha256'] and sha(run_root/'normalized.json')==row['normalized_sha256'],'RETAINED_RESULT_CHANGED'
        value=json.loads(path.read_text());normalized=json.loads((run_root/'normalized.json').read_text())
        assert normalized['original_result']=={'path':str(path),'sha256':row['sha256']},'RETAINED_RESULT_BINDING_CHANGED'
        assert normalized['schema']=='MEME_LIVE_T010_NORMALIZED_PROCESS_MEASUREMENT_V1'
        assert normalized['exit_code']==0 and normalized['fresh_process'] is True,'RETAINED_CHILD_NOT_SUCCESSFUL'
        assert all(row[k]==v for k,v in value['parent_boundary'].items()) and row['memory']==value['memory']
        assert normalized['process_instance_digest']==value['process_instance']['digest']
        assert normalized['native_digest']==value['native_digest'] and normalized['runtime_code_digest']==value['runtime_code_digest']
        assert normalized['process_instance_digest'] not in instances,'RETAINED_PROCESS_REUSED'
        instances.add(normalized['process_instance_digest'])
        # This metadata is written only after the child succeeds and archival finishes.
        closed=json.loads((run_root/'closed-state-manifest.json').read_text())
        assert closed['files'] and set(closed['physical'])=={'ledger','source','producer','operations','monitor'}
        assert all(v['pragmas']['integrity_check']=='ok' for v in closed['physical'].values())
        results.append(row)
    return results


def prepared_repetition_directory(output,number,continuing):
    """Preserve interrupted output in place; continuation always gets a new directory."""
    run_root=output/('run-'+str(number))
    if continuing:
        candidates=[run_root,*output.glob(run_root.name+'-continuation-*')]
        assert not any((p/'closed-state-manifest.json').exists() for p in candidates),'COMPLETED_REPETITION_REQUIRES_RETAINED_PROOF'
        if run_root.exists():
            import tempfile
            return Path(tempfile.mkdtemp(prefix=run_root.name+'-continuation-',dir=output))
    run_root.mkdir()
    return run_root


def measure_prepared(output,repetitions,codec_cost_inputs=None,guards_path=None,*,continue_from=None):
    from live.operations_ownership_v0_1 import OperationsStore
    if codec_cost_inputs is not None:codec_cost_inputs=codec_cost_inputs.resolve()
    retained=json.loads((output/'prepared-start-manifest.json').read_text())
    origin=Path(retained['origin']);original_hashes=retained['original_files_unchanged']
    monitor_evidence=retained['monitor'];prepared_hashes=retained['files']
    root=output/'active';prepared=output/'prepared-start'
    assert prepared_hashes=={p.name:sha(p) for p in prepared.iterdir()}
    config_path=root/'configuration.json';config=json.loads((prepared/'configuration.json').read_text())
    prepared_codec=config.get('codec_cost_inputs')
    effective_codec=({'path':str(codec_cost_inputs.resolve()),'sha256':sha(codec_cost_inputs)}
        if codec_cost_inputs is not None else prepared_codec)
    results=[]
    if continue_from is not None:
        assert guards_path is None,'CONTINUATION_REQUIRES_UNCONSTRAINED_MEASUREMENT'
        assert repetitions>0 and not (output/'manifest.json').exists(),'COMPLETED_SEQUENCE_MUST_NOT_BE_OVERWRITTEN'
        results=retained_prepared_repetitions(output,continue_from,repetitions)
        assert len(results)<repetitions,'NO_MISSING_REPETITIONS'
        assert all(Path(p).resolve().parent==root.resolve() for p in config['paths'].values()),'PREPARED_ACTIVE_PATH_CHANGED'
        for row in results:
            normalized=json.loads((Path(row['path']).parent/'normalized.json').read_text())
            assert 'verified_quota' not in normalized,'CONTINUATION_REQUIRES_UNCONSTRAINED_SAMPLES'
            assert normalized['construction']=={'path':str(origin/'construction.json'),'sha256':sha(origin/'construction.json')},'RETAINED_CONSTRUCTION_CHANGED'
            validate_retained_codec_binding(json.loads(Path(row['path']).read_text()),effective_codec)
            from live.t010_resource_envelope_v0_1 import validate_empirical_process_result
            validate_empirical_process_result(normalized,json.loads(Path(row['path']).read_text()),
                runtime=normalized['runtime_code_digest'],native=normalized['native_digest'])
    retained_count=len(results)
    for n in range(retained_count,repetitions):
        run_root=prepared_repetition_directory(output,n+1,continue_from is not None)
        if n or continue_from is not None:
            for path in root.iterdir():
                assert path.is_file() and path.resolve().parent==root.resolve()
                path.unlink()  # Only archived, closed files in this new isolated active directory.
            for path in prepared.iterdir():shutil.copy2(path,root/path.name)
        assert prepared_hashes=={p.name:sha(p) for p in root.iterdir()}
        result_path=run_root/'result.json'
        begin=time.perf_counter_ns()
        command=[sys.executable,'-B',str(Path(__file__).resolve()),'child','--config',str(config_path),'--result',str(result_path)]
        if guards_path is not None:command+=['--guards',str(guards_path)]
        if codec_cost_inputs is not None:command.extend(['--codec-cost-inputs',str(codec_cost_inputs)])
        process=subprocess.Popen(command,
            cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=(run_root/'stderr.txt').open('w'),text=True)
        try:
            line=process.stdout.readline();arrived=time.perf_counter_ns()
            (run_root/'stdout.txt').write_text(line)
            message=json.loads(line)
            assert message['message']=='STARTED' and message['pid']==process.pid
            args=build_configuration(config)
            control=OperationsStore(config['paths']['operations'],args['domain'],readonly=True).snapshot()
            assert all(control[k]==v for k,v in message['fence'].items())
            validated=time.perf_counter_ns()
            process.stdin.write('STEP\n');process.stdin.flush()
            process.stdin.close()
            remaining=process.stdout.read()
            with (run_root/'stdout.txt').open('a') as stream:stream.write(remaining)
            code=process.wait();assert code==0,(code,str(run_root/'stderr.txt'))
            value=json.loads(result_path.read_text())
            validate_retained_codec_binding(value,effective_codec)
            value['prepared_codec_cost_binding']=prepared_codec
            value['effective_measurement_codec_cost_binding']=effective_codec
            value['measurement_harness_sha256']=sha(Path(__file__))
            value['parent_boundary']=dict(spawn_begin_ns=begin,started_arrived_ns=arrived,
                started_validated_ns=validated,spawn_to_started_us=(arrived-begin)//1000,
                spawn_to_validated_us=(validated-begin)//1000,
                spawn_to_first_legal_unit_us=(value['step_end_ns']-begin)//1000,
                bootstrap_to_target_us=(value['target_begin_ns']-begin)//1000)
            write(result_path,value)
            construction=json.loads((origin/'construction.json').read_text())
            scenario={'protocol_pending':'protocol98_pending','four_hot_hold':'four_hot73_hold',
                'hottest_pending':'hottest512_pending','active64_hold':'active64_hold'}[
                    construction['disjoint_mode']]
            normalized=normalize_process_result(value,scenario,result_path)
            if guards_path is not None:
                from phase5.shadow_domain_v0_1 import content_fingerprint
                guards=json.loads(guards_path.read_text())
                assert all(v is None or v<=guards[k] for k,v in normalized['metrics'].items()),'CONTAINED_STRUCTURAL_GUARD_EXCEEDED'
                assert value['quota']['guards']==guards and value['quota']['tree']['tree_private_bytes']==guards['child_private_bytes']+guards['supervisor_private_bytes']
                normalized.update(limit_binding_digest=content_fingerprint(guards),
                    verified_quota={'rss_bytes':guards['HOST_RSS_BYTES'],'child_private_bytes':guards['child_private_bytes'],
                        'tree_private_bytes':guards['child_private_bytes']+guards['supervisor_private_bytes']})
            normalized['construction']={'path':str(origin/'construction.json'),'sha256':sha(origin/'construction.json')}
            if (construction['roots'],construction['source_records'])!={'protocol98_pending':(98,627),
                    'four_hot73_hold':(73,627),'hottest512_pending':(65,627),'active64_hold':(64,626)}[scenario]:
                normalized['unresolved_constraints'].append('small harness fixture; not the full named structural scenario')
            write(run_root/'normalized.json',normalized)
            archived=run_root/'closed-state';archived.mkdir()
            for path in root.iterdir():
                assert path.is_file() and path.resolve().parent==root.resolve()
                shutil.copy2(path,archived/path.name)
            physical={}
            for key in ('ledger','source','producer','operations','monitor'):
                path=Path(config['paths'][key])
                with path.open('rb') as header_stream:header=header_stream.read(21)
                with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as conn:
                    physical[key]={'sha256':sha(path),'bytes':path.stat().st_size,
                        'sqlite_source_id':conn.execute('SELECT sqlite_source_id()').fetchone()[0],
                        'header_reserved_bytes':header[20],
                        'pragmas':{p:conn.execute('PRAGMA '+p).fetchone()[0] for p in ('journal_mode','page_size','auto_vacuum','encoding','page_count','freelist_count','integrity_check')}}
            write(run_root/'closed-state-manifest.json',{'files':{p.name:sha(p) for p in archived.iterdir()},'physical':physical})
            results.append(dict(path=str(result_path),sha256=sha(result_path),normalized_sha256=sha(run_root/'normalized.json'),
                **value['parent_boundary'],memory=value['memory']))
            print(json.dumps(results[-1]),flush=True)
        finally:
            if process.poll() is None:process.terminate();process.wait()
    assert all(sha(Path(p))==v for p,v in original_hashes.items())
    if continue_from is not None:
        assert retained_prepared_repetitions(output,continue_from,repetitions)==results[:retained_count],'RETAINED_RESULTS_CHANGED_DURING_CONTINUATION'
        normalized_results=[json.loads((Path(r['path']).parent/'normalized.json').read_text()) for r in results]
        assert len({v['process_instance_digest'] for v in normalized_results})==repetitions,'CONTINUATION_PROCESS_REUSED'
        assert len({(v['scenario'],v['native_digest'],v['runtime_code_digest']) for v in normalized_results})==1,'CONTINUATION_MEASUREMENT_BINDING_CHANGED'
        assert not (output/'manifest.json').exists(),'COMPLETED_SEQUENCE_MUST_NOT_BE_OVERWRITTEN'
    write(output/'manifest.json',dict(scope='REPEATED_FRESH_PROCESS_UNCONSTRAINED_JOINT_CONSTRUCTOR_AND_RECOVERY',
        prepared_codec_cost_binding=prepared_codec,effective_measurement_codec_cost_binding=effective_codec,
        measurement_harness_sha256=sha(Path(__file__)),
        origin=str(origin),origin_construction_sha256=sha(origin/'construction.json'),
        original_files_unchanged=original_hashes,repetitions=results,
        full_monitor_fixture=monitor_evidence,prepared_start_sha256=sha(output/'prepared-start-manifest.json'),
        stable_path_restoration=True,
        cache_state='OS file cache uncontrolled; copies prepared immediately before each fresh interpreter; no cache eviction or collector changes',
        limitations=['does not yet include reviewed T010 facts/profile/preflight hooks','synthetic baseline/clock only',
            'no new public-clock epoch behavior claim','post-start parent identity reconstruction included and separately timed',
            'no blocked-reader physical maximum transaction proof','test-only profile configuration and recovery facts seam',
            'original runtime_started policy/grant/terminal/facts/driver/report hooks included; no actual request or detached host'],
        source_files={str(p.relative_to(ROOT)):sha(p) for p in (Path(__file__),ROOT/'scripts/live_t010_joint_state_v0_3.py',ROOT/'scripts/live_t010_monitor_full_state_v0_1.py')},
        rpc_fixture={'path':str(EVIDENCE/'public_response_resource_v3.py'),'sha256':sha(EVIDENCE/'public_response_resource_v3.py')},
        guards_selected=False,public_qualification_claimed=False))


def measure_preserved(witness,output):
    """One real startup on a byte-preserved disposable committed prefix.

    No builder/preparation API is called. Absolute-path identity checks remain
    authoritative: a relocated seal failure is reported, never repaired.
    """
    began=time.monotonic()
    witness=witness.resolve(strict=True);output=output.resolve()
    inventory=witness.parent/'owner-stop-builder-checkpoint-v1'
    handoff=json.loads((inventory/'handoff.json').read_text())
    matches=[v for v in handoff['families'] if Path(v['stage_database']).resolve().parent==witness]
    assert len(matches)==1,'EXACT_PRESERVED_WITNESS_REQUIRED'
    selected=matches[0]
    def reference(ref):
        path=Path(ref['path']);assert sha(path)==ref['sha256'],'CHECKPOINT_REFERENCE_CHANGED'
        return json.loads(path.read_text())
    checkpoint=reference(selected['monitor_checkpoint'])
    reuse_manifest=json.loads((inventory/'reuse-verification-manifest.json').read_text())
    reuse_path=inventory/(selected['family']+'-reuse-verification.json')
    refs=[r for r in reuse_manifest['results'] if Path(r['path']).resolve()==reuse_path.resolve()]
    assert len(refs)==1,'CHECKPOINT_REUSE_REFERENCE_REQUIRED'
    reuse=reference(refs[0]);config=reference(reuse['reconstructed_configuration'])
    assert checkpoint['confirmed_exited'] and selected['exact_native_exit_confirmed']
    assert Path(checkpoint['database']).resolve()==Path(reuse['staged_monitor']).resolve()
    paths={k:Path(v).resolve(strict=True) for k,v in config['paths'].items() if k!='monitor'}
    paths['monitor']=Path(checkpoint['database']).resolve(strict=True)
    assert set(paths)=={'ledger','producer','operations','source','monitor','raw'}
    active=Path(selected['active_path']).resolve(strict=True)
    assert all(p.parent==active for k,p in paths.items() if k!='monitor')
    assert not output.exists(),'NEW_DISPOSABLE_OUTPUT_REQUIRED'
    assert all(not output.is_relative_to(p) and not p.is_relative_to(output)
        for p in (witness,active,inventory,ROOT)), 'DISPOSABLE_OUTPUT_MUST_BE_SEPARATE'
    def facts():
        result={}
        for key,path in paths.items():
            files={}
            for suffix in ('','-wal','-shm','-journal'):
                part=Path(str(path)+suffix)
                if part.exists():
                    stat=part.stat()
                    files[suffix]=dict(bytes=stat.st_size,sha256=sha(part),device=stat.st_dev,inode=stat.st_ino)
            assert all(files.get(s,{}).get('bytes',0)==0 for s in ('-wal','-journal')), 'CLOSED_CHECKPOINT_REQUIRED'
            # Empty WAL was checked; immutable reads cannot create/change SHM.
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)) as conn:
                conn.execute('PRAGMA query_only=ON')
                tables=[r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
                counts={t:conn.execute('SELECT count(*) FROM "'+t.replace('"','""')+'"').fetchone()[0] for t in tables}
                state=dict(path=str(path),files=files,table_counts=counts)
                if key=='monitor':
                    stream=hashlib.sha256()
                    for digest, in conn.execute('SELECT digest FROM receipts ORDER BY rowid'):
                        stream.update((digest+'\n').encode())
                    state.update(receipt_stream_sha256=stream.hexdigest(),metadata=conn.execute('SELECT * FROM metadata').fetchall())
                result[key]=state
        return result
    before=facts()
    assert before['monitor']['files']['']['sha256']==checkpoint['database_sha256_after_unlock']
    assert before['monitor']['table_counts']==checkpoint['table_counts']
    assert before['monitor']['receipt_stream_sha256']==checkpoint['receipt_insertion_stream_sha256']
    count=before['monitor']['table_counts']['receipts']
    assert count==reuse['current_receipts'] and 0<count<reuse['target_receipts']
    output.mkdir(parents=True);root=output/'active';root.mkdir()
    record=dict(scope='COMMITTED_MEASUREMENT_PREFIX',completed_builder_target=False,
        witness=str(witness),family=selected['family'],committed_receipts=count,
        original_builder_target= reuse['target_receipts'],before=before,
        checkpoint=selected['monitor_checkpoint'],disposable=str(root),
        resource_qualification_claimed=False)
    write(output/'input-manifest.json',record)
    try:
        for key,path in paths.items():
            destination=root/path.name;shutil.copyfile(path,destination)
            assert sha(destination)==before[key]['files']['']['sha256'],'COPY_BYTES_CHANGED'
            config['paths'][key]=str(destination)
        config['monitor']['path']=config['paths']['monitor']
        config['live_paths']=[str(root/Path(p).name) for p in config['live_paths']]
        # Bind generated configuration to disposable paths, as in the existing
        # copy helper. Never rewrite the copied journal's persisted policy seal.
        copied=build_configuration(config);monitor=copied['degradation_config']
        monitor=replace(monitor,policy=replace(monitor.policy,
            reviewed_configuration_digest=monitor.binding_for(copied['expected_identity'])),
            runtime_code_digest=copied['expected_identity'].runtime_code_digest)
        config['monitor']=asdict(monitor)
        resource=root/'resource_work.json';shutil.copyfile(active/'resource_work.json',resource)
        config['resource_snapshot']=str(resource)
        write(root/'configuration.json',config)
        command=[sys.executable,'-B',str(Path(__file__).resolve()),'child',
            '--config',str(root/'configuration.json'),'--result',str(output/'startup.json'),'--startup-only']
        record['command']=command;started=time.monotonic()
        with (output/'stdout.txt').open('w') as stdout,(output/'stderr.txt').open('w') as stderr:
            try:
                run=subprocess.run(command,cwd=ROOT,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,
                    timeout=max(1,540-(time.monotonic()-began)))
                record['exit_code']=run.returncode
            except subprocess.TimeoutExpired:
                record.update(exit_code=None,blocker='WHOLE_PROCESS_BOUNDARY_TIMEOUT')
        record['invocation_seconds']=time.monotonic()-started
        record['boundary_reached']=record['exit_code']==0 and (output/'startup.json').is_file()
    finally:
        record['after']=facts();record['original_unchanged']=record['after']==before
        record['elapsed_seconds']=time.monotonic()-began
        write(output/'smoke.json',record)
        assert record['original_unchanged'],'ORIGINAL_WITNESS_CHANGED'
    print(json.dumps({k:record[k] for k in ('scope','boundary_reached','exit_code','original_unchanged','elapsed_seconds')}),flush=True)
    assert record['boundary_reached'],'COMMITTED_PREFIX_STARTUP_BLOCKED_SEE_STDERR'


def normalize_process_result(value,scenario,result_path):
    from live.t010_resource_measurement_v0_1 import process_metrics
    recovery=value['age_evidence']['recovery'];host=value['age_evidence']['host']
    assert recovery and host,'ORIGINAL_AGE_MEASUREMENT_REQUIRED'
    assert all(v['capture_to_return_us'] is not None for v in recovery),'RECOVERY_CLOCK_CAPTURE_REQUIRED'
    metrics={'STARTUP_US':value['parent_boundary']['spawn_to_validated_us'],
        'HOST_RSS_BYTES':value['memory']['PeakWorkingSetSize'],
        'PROTECTIVE_STEP_US':max(value['first_step_us'],value['steady_monitor']['elapsed_us']),
        'OLDEST_UNCONSUMED_AGE_US':value['workload']['oldest_unconsumed_age_us'],
        'resource_max_age_us':max(max(v['original_sample_age_us'],v['capture_to_consumption_us']) for v in host),
        'recovery_evidence_max_age_us':max(max(v['original_ingestion_age_us'],v['capture_to_return_us']) for v in recovery),
        'child_private_bytes':value['memory']['PeakPagefileUsage']}
    result={'schema':'MEME_LIVE_T010_NORMALIZED_PROCESS_MEASUREMENT_V1','scenario':scenario,
        'runtime_code_digest':value['runtime_code_digest'],'native_digest':value['native_digest'],
        'process_instance_digest':value['process_instance']['digest'],'process_instance':value['process_instance'],
        'exit_code':0,'fresh_process':True,'metrics':metrics,
        'empty_backlog_verified':value['workload']['backlog_count']==0,
        'original_result':{'path':str(result_path),'sha256':sha(result_path)},
        'workload':value['workload'],'codec_cost_call_count':len(value['codec_cost']),
        'scope':'SYNTHETIC_EXTERNAL_FACTS_ORIGINAL_CONSTRUCTORS_AND_CODEC_COST_NOT_PUBLIC_QUALIFICATION',
        'metric_semantics':{'STARTUP_US':'whole parent spawn through validated original STARTED including colocated full codec calls',
            'HOST_RSS_BYTES':'native whole-process peak; includes retained heap, original public response fixtures and conservative extra audit',
            'PROTECTIVE_STEP_US':'maximum original pending/hold unit and unlocked original monitor observation',
            'resource_max_age_us':'maximum original typed age and actual capture through unchanged host-port consumption',
            'recovery_evidence_max_age_us':'maximum original ingestion age and captured original clock through recovery-record return',
            'child_private_bytes':'native process peak private commit',
            'OLDEST_UNCONSUMED_AGE_US':'original same source-fence query; None only positive empty count'},
        'unresolved_constraints':[] if value['codec_cost'] else ['full original codec cost not exercised']}
    result['measurement_provenance']=dict(schema='MEME_LIVE_EMPIRICAL_PROCESS_PROVENANCE_V1',
        kind='ORIGINAL_FRESH_PROCESS_MEASUREMENT',original_result=result['original_result'],
        process_instance_digest=result['process_instance_digest'],measurement_harness_sha256=value.get('measurement_harness_sha256'),
        prepared_codec_cost_binding=value.get('prepared_codec_cost_binding'),
        effective_measurement_codec_cost_binding=value.get('effective_measurement_codec_cost_binding'))
    process_metrics(result,scenario)
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='command',required=True)
    f=s.add_parser('generate-c2-codec-fixture');f.add_argument('--historical',type=Path,required=True)
    f.add_argument('--output',type=Path,required=True);f.add_argument('--certificate',type=Path,required=True)
    v=s.add_parser('codec-preflight');v.add_argument('--codec-cost-inputs',type=Path,required=True)
    k=s.add_parser('collect-c2-observations');k.add_argument('--manifest',type=Path,action='append',required=True);k.add_argument('--output',type=Path,required=True)
    c=s.add_parser('child');c.add_argument('--config',type=Path,required=True);c.add_argument('--result',type=Path,required=True)
    c.add_argument('--codec-cost-inputs',type=Path)
    c.add_argument('--guards',type=Path)
    c.add_argument('--startup-only',action='store_true')
    w=s.add_parser('measure-preserved');w.add_argument('--witness',type=Path,required=True)
    w.add_argument('--output',type=Path,required=True)
    m=s.add_parser('measure');m.add_argument('--origin',type=Path,required=True);m.add_argument('--output',type=Path,required=True)
    m.add_argument('--repetitions',type=int,default=3)
    m.add_argument('--monitor-observations',type=int,choices=(3,2190,2193),default=2193)
    m.add_argument('--prepare-only',action='store_true')
    m.add_argument('--resume',action='store_true',help='continue the original committed monitor in place; requires --prepare-only and original arguments')
    m.add_argument('--monitor-staging-root',type=Path)
    m.add_argument('--codec-cost-inputs',type=Path)
    r=s.add_parser('measure-prepared');r.add_argument('--output',type=Path,required=True)
    r.add_argument('--repetitions',type=int,default=3)
    r.add_argument('--codec-cost-inputs',type=Path)
    r.add_argument('--guards',type=Path)
    r.add_argument('--continue-from',type=Path,help='retain completed parent stdout JSON records and run only missing repetitions; interrupted directories are preserved and new attempt paths are used')
    u=s.add_parser('supervisor');u.add_argument('--config',type=Path,required=True);u.add_argument('--output',type=Path,required=True)
    u.add_argument('--codec-cost-inputs',type=Path,required=True);u.add_argument('--protective-us',type=int,required=True)
    u.add_argument('--guards',type=Path)
    u.add_argument('--blocked-cleanup',action='store_true')
    u.add_argument('--launch-ns',type=int)
    a=p.parse_args()
    if a.command=='generate-c2-codec-fixture':print(json.dumps(generate_c2_codec_fixture(a.historical,a.output,a.certificate)))
    elif a.command=='codec-preflight':print(json.dumps(dict(scope='CODEC_PREFLIGHT_ONLY_NOT_MEASUREMENT_NOT_QUALIFICATION',call=validate_codec_cost_input({'path':str(a.codec_cost_inputs.resolve()),'sha256':sha(a.codec_cost_inputs)}))))
    elif a.command=='collect-c2-observations':print(json.dumps(collect_c2_observations(a.manifest,a.output)))
    elif a.command=='child':child(a.config,a.result,a.codec_cost_inputs,a.guards,a.startup_only)
    elif a.command=='measure-preserved':measure_preserved(a.witness,a.output)
    elif a.command=='measure-prepared':measure_prepared(a.output,a.repetitions,a.codec_cost_inputs,a.guards,continue_from=a.continue_from)
    elif a.command=='supervisor':
        sys.path.insert(0,str(EVIDENCE))
        from resource_supervisor_measure_v1 import run
        run(a.config,a.output,a.codec_cost_inputs,a.protective_us,a.guards,a.blocked_cleanup,a.launch_ns)
    else:measure(a.origin,a.output,a.repetitions,a.monitor_observations,a.prepare_only,a.monitor_staging_root,a.codec_cost_inputs,resume=a.resume)
