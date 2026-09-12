"""Construct an inactive M52 public qualification profile; no runtime invocation.
Protected source reads are bounded immutable-mainfile qualification facts only.
"""
import argparse
import hashlib
import json
import sqlite3
import sys
from contextlib import closing
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live import source_health_v0_1 as source
from live import operations_startup_v0_1 as startup
from live.operations_ownership_v0_1 import RestartProfile
from live.continuous_producer_v0_2 import ContinuationProfileV02
from live.ledger_domain_v0_1 import LedgerDomain
from live.public_rpc_v0_1 import PublicRpcProfile
from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02
from live.operations_degradation_v0_1 import ResourceLimit, DegradationPolicy, REQUIRED_METRICS
from live.operations_degradation_monitor_v0_1 import MonitorConfiguration, monitor_configuration_digest
from phase5.shadow_domain_v0_1 import content_fingerprint


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def write(path,value):
    Path(path).write_text(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n',encoding='utf-8')


def file_state(path):
    value=path.stat()
    return dict(device=value.st_dev,file_id=value.st_ino,size=value.st_size,mtime_ns=value.st_mtime_ns)


def freeze_source(raw,control_anchor_rowid=None):
    before=file_state(raw)
    sidecars={suffix:file_state(Path(str(raw)+suffix)) if Path(str(raw)+suffix).exists() else None for suffix in ('-wal','-shm')}
    counts={}
    with closing(sqlite3.connect(raw.as_uri()+'?mode=ro&immutable=1',uri=True)) as conn:
        budget=[0]
        def progress():
            budget[0]+=1
            return int(budget[0]>250)
        conn.set_progress_handler(progress,1000)
        assert control_anchor_rowid is None or control_anchor_rowid > 0
        rows=(conn.execute('SELECT '+source._SELECT['collector_events']+' FROM collector_events ORDER BY id DESC LIMIT 256').fetchall() if control_anchor_rowid is None else conn.execute('SELECT '+source._SELECT['collector_events']+' FROM collector_events WHERE id=?',(control_anchor_rowid,)).fetchall())
        counts['collector_events']=len(rows)
        starts=[]
        for row in rows:
            if row[1]=='COLLECTOR_V0_3_START':
                try:
                    starts.append(source._fact('collector_events',row))
                except (ValueError,TypeError):
                    continue
        assert starts,'No supported collector start in explicit bounded selection'
        control=starts[0]
        selected={}
        for table,field in (('pump_events','inserted_at_utc'),('websocket_observations','at_utc')):
            rows=conn.execute('SELECT '+source._SELECT[table]+' FROM '+table+' ORDER BY rowid DESC LIMIT 1024').fetchall()
            counts[table]=len(rows)
            for row in rows:
                try:
                    fact=source._fact(table,row)
                except (ValueError,TypeError):
                    continue
                if getattr(fact,field)>=control.at_utc:
                    selected[table]=fact
                    break
            assert table in selected,'No compatible supported '+table+' anchor in bounded1024-row tail'
    assert before==file_state(raw),'Mainfile changed during immutable qualification read; no snapshot claim'
    after_sidecars={suffix:file_state(Path(str(raw)+suffix)) if Path(str(raw)+suffix).exists() else None for suffix in ('-wal','-shm')}
    assert sidecars==after_sidecars,'Sidecar state changed during immutable qualification read; stop'
    facts=(selected['pump_events'],control,selected['websocket_observations'])
    witnesses=tuple(source.witness(table,fact) for table,fact in zip(source.TABLES[:3],facts))
    frozen_digest=content_fingerprint(dict(path=str(raw),witnesses=tuple(asdict(x) for x in witnesses)))
    binding=source.SourceBinding('M52:QUALIFICATION:'+frozen_digest[:24],control.at_utc,witnesses)
    return binding,dict(binding=asdict(binding),source_identity=binding.source_identity,normalizer='Original source_health_v0_1._fact/witness',selected_public_facts=[asdict(x) for x in facts],selection=('Newest supported COLLECTOR_V0_3_START within256 latest controls' if control_anchor_rowid is None else 'Explicit qualification-only COLLECTOR_V0_3_START primary key '+str(control_anchor_rowid))+'; newest compatible positive pump/receipt within1024 rows each, timestamp >= chosen control start. Qualification-only cutoff is selected pump rowid.',rows_read=counts,sqlite_opcode_budget=250000,main_file=before,sidecars_before_after=sidecars,mainfile_and_sidecar_stat_stable=True,observed_utc=datetime.now(timezone.utc).isoformat(),scope='Frozen immutable SQLite main-file qualification witnesses only. WAL excluded, no gap/continuity/current health proof, no future LIVE start selection, no existing producer lineage asserted.')


def run(manifest_path,measurement_path,host_path,out,control_anchor_rowid=None,record_unresolved_source=False):
    target=read(manifest_path)
    measurement=read(measurement_path)
    host=read(host_path)
    assert sha(manifest_path)==measurement['target_public_configuration']['sha256']
    assert sha(measurement_path)=='651fa506e3cc8f02cf89408e9608d18a3d7ac7f4f90c7086a5003a96e496783a'
    assert sha(host_path)=='0ccdaf122183dfbe144267d59f9335cb424341038cd3d8c6923743023c8771dc'
    assert out.resolve().parent==Path(target['paths']['evidence_root']).resolve()/'step11a-3630b2'
    assert not out.exists()
    raw=Path(target['paths']['market_source']).resolve(strict=True)
    if record_unresolved_source:
        binding=None
        frozen=dict(binding=None,source_identity=None,status='SUPPORTED_COLLECTOR_START_WITNESS_NOT_FOUND_IN_AUTHORIZED_SELECTIONS',attempts=[dict(selection='newest256 control rows',exit=1,elapsed_seconds=1.643262),dict(selection='collector primary key id1',exit=1,elapsed_seconds=1.4815699)],pump_receipt_rows_read=0,source_metadata_reference=dict(path=str(measurement_path),sha256=sha(measurement_path)),scope='No new source read by this profile construction. Two prior bounded selections found no supported start; no broader scan, no invented anchors/lineage/current health/live start.')
    else:
        binding,frozen=freeze_source(raw,control_anchor_rowid)
    out.mkdir()
    write(out/'frozen-source.json',frozen)
    m=measurement['measurement']
    shape=m['after']
    usage=shape['producer_profile_usage']
    producer_profile=ContinuationProfileV02(**m['fixture_profile'])
    source_profile=source.SourceProfile(max_rows_per_table=10000,freshness_seconds=30)
    rpc_profile=PublicRpcProfile(provider_id='SOLANA_PUBLIC_MAINNET_BETA',provider_version='JSONRPC2')
    domain=LedgerDomain(genesis_hash=target['genesis_hash'],wallet=target['wallet_public_key'],expected_profile_fingerprint=rpc_profile.fingerprint,known_native_wallet_lamports=None,expected_empty_token_accounts=(),minimum_context_slot=0,mode=target['mode'])
    market=None if binding is None else ContinuousMarketSourceV02(raw,start_after_p1_rowid=binding.anchors[0].rowid,database_identity=raw.as_posix())
    restart=RestartProfile(**m['current_startup_identity']['restart_profile'])
    identity=None if binding is None else startup.configured_identity(domain,operations_path=target['paths']['operations'],ledger_path=target['paths']['ledger'],producer_path=target['paths']['producer'],market_source=market,producer_profile=producer_profile,source_path=target['paths']['evidence_store'],source_binding=binding,source_profile=source_profile,database_identity=market.database_identity,producer_schema_digest=m['current_startup_identity']['producer_schema_digest'],evidence_schema_digest=m['current_startup_identity']['evidence_schema_digest'],restart_profile=restart,batch_rows=32,page_rows=32,queued_roots=64)
    prior_boundary=read(Path(measurement['accepted_fixture']['root'])/'sample-3-before.json')
    no_t0=len(prior_boundary['no_t0_mints'])
    thresholds={
      'AGGREGATE_FEATURE_BYTES':(248125,'MEASURED_WORKLOAD_BOUNDARY','Exact observed aggregate retained event payload; exceeds triggers ENTRY hold; not capacity for all shapes.'),
      'HOTTEST_FEATURE_BYTES':(166275,'MEASURED_WORKLOAD_BOUNDARY','Exact observed hottest retained event payload.'),
      'UNFINISHED_MINTS':(33,'MEASURED_WORKLOAD_BOUNDARY','Exact observed unfinished-mint count.'),
      'NO_T0_MINTS':(no_t0,'REUSED_MEASURED_WORKLOAD_BOUNDARY','Exact accepted unchanged retained fixture no-t0 count.'),
      'IDENTITY_ROWS':(34,'MEASURED_WORKLOAD_BOUNDARY','Exact observed original immutable launch identities.'),
      'TOMBSTONE_ROWS':(1,'MEASURED_WORKLOAD_BOUNDARY','Exact observed original immutable tombstones; no unbounded accumulation claim.'),
      'PRODUCER_PENDING_ROWS':(2346,'MEASURED_WORKLOAD_BOUNDARY','Exact observed retained pending rows.'),
      'LEDGER_TAIL_ROWS':(20,'MEASURED_WORKLOAD_BOUNDARY','Exact observed full replay history, no indefinite history guarantee.'),
      'LEDGER_TAIL_PAYLOAD_BYTES':(10292,'MEASURED_WORKLOAD_BOUNDARY','Exact observed full replay tail UTF8 payload bytes; indexed economic payloads excluded.'),
      'HOST_RSS_BYTES':(100663296,'CALIBRATED_POLICY_CEILING','96MiB: next32MiB band above observed77942784B lifetime peak; margin for B monitor; not measured96MiB capacity.'),
      'HOST_DISK_RESERVE_BYTES':(1073741824,'POLICY_RESERVE_FLOOR','1GiB reserve floor, over276 times observed3879720B workload allocation lengths. Available D-volume observed1.914TB; operation at1GiB not measured.'),
      'STARTUP_US':(4000000,'CALIBRATED_POLICY_CEILING','4s budget versus original measured3.1476025s in-process startup; B must validate monitor overhead; excludes process spawn.'),
      'PROTECTIVE_STEP_US':(100000,'CALIBRATED_POLICY_CEILING','100ms budget versus measured39.2799ms original NEED_EXECUTION protective unit; no execution/send latency claim; B overhead pending.'),
      'OLDEST_UNCONSUMED_AGE_US':(30000000,'POLICY_FRESHNESS_CEILING','30s matches configured SourceProfile freshness. Empty backlog remains inapplicable/unknown; source freshness unqualified until B evidence.')}
    assert set(thresholds)==REQUIRED_METRICS
    intervals=dict(resource_max_age_us=dict(value=1000000,role='POLICY_FRESHNESS',rationale='1s RSS/disk observation age; existing C2 freshness port, B boundary qualification pending.'),recovery_evidence_max_age_us=dict(value=5000000,role='POLICY_RECOVERY_FRESHNESS',rationale='5s positive-restoration evidence age, chosen finite local recovery window; B pending.'),persistent_unknown_alert_us=dict(value=60000000,role='POLICY_ALERT_DELAY',rationale='60s persistent UNKNOWN escalation; UNKNOWN retained immediately and never cleared by timeout; B pending.'))
    uncovered=['Retained/hottest event counts382/256: not individual C2 metrics; unchanged constructor caps1024/512 exceed measured shape.','Retained serialization410065B, pending bytes2210889B, history bytes9086B and checkpoint2239B: not individual C2 metrics; unchanged constructor byte caps are higher and unqualified.','Per-record1MiB and delivery-page256 constructor caps not exercised to exhaustion. Batch32 has prior exact-source C1 evidence; current protective reopen defers handoff replay.','C2 STEP/STARTUP consume supplied timing evidence; do not preempt an in-flight over-budget operation.','No claim of continuous feed throughput, growth beyond20 ledger commits or all possible shapes within thresholds.']
    profile=dict(schema='MEME_LIVE_M52_QUALIFICATION_PROFILE_V1',implementation_status='IMPLEMENTED_PENDING_PROJECT_REVIEW',activation=False,entry_permission=False,live_trading_start_selected=False,scope='Target-bound finite operating workload qualification; synthetic retained economic fixture. Not real-wallet holdings or current healthy LIVE deployment.',target=dict(public_configuration_path=str(manifest_path),public_configuration_sha256=sha(manifest_path),paths=target['paths'],network=target['network'],domain=domain.to_record(),economic_domain_id=domain.economic_domain_id,domain_binding_digest=domain.binding_digest,current_code=target['code'],runtime_code_digest=m['current_startup_identity']['runtime_code_digest']),host=dict(observation_path=str(host_path),sha256=sha(host_path),identity_digest=host['current_host_fingerprint'],identity=host['current_host']),source_configuration=dict(frozen_source_path=str(out/'frozen-source.json'),frozen_source_sha256=sha(out/'frozen-source.json'),binding=None if binding is None else asdict(binding),profile=asdict(source_profile),database_identity=raw.as_posix(),market_source_identity=None if market is None else market.source_identity,qualification_start_after_p1_rowid=None if market is None else market.start_after_p1_rowid,activation_start_after_p1_rowid=None,scope=('SourceBinding unresolved after bounded selection; no fabricated identity.' if binding is None else 'Explicit normalized actual-source main-file qualification witnesses and newly named qualification lineage.')+' No current source-health claim, no existing producer lineage assertion, and no future LIVE trading start.'),constructor_configuration=dict(continuation_profile=asdict(producer_profile),scope='Exact immutable retained C1 profile required by checkpoint equality. Configured caps are not qualified capacity.',restart_profile=asdict(restart),dispatch=dict(batch_rows=32,page_rows=32,queued_roots=64),public_rpc_profile=asdict(rpc_profile),public_rpc_profile_scope='Configured public provider identity/finite API caps only; no new RPC requests or trust/capacity qualification.',opening_domain_scope='known native balance UNKNOWN; no token emptiness or holdings inferred, no wallet observation or Ledger initialization performed.',schema_fingerprint_provenance='Exact accepted original producer/Evidence schema fingerprints from measurement input; canonical stores absent and not inspected.'),canonical_startup_identity=None if identity is None else asdict(identity),canonical_startup_identity_missing_reason=None if identity is not None else 'SourceBinding constructor requires a supported collector-start plus compatible pump/receipt witnesses. Path/schema and two unsuccessful bounded selections cannot provide them. No fabricated identity supplied.',observed_supported_workload=dict(retained_events=382,hottest_events=256,unfinished_mints=33,no_t0_mints=no_t0,aggregate_feature_payload_bytes=248125,hottest_feature_payload_bytes=166275,retained_serialized_bytes=410065,pending_rows=2346,pending_serialized_bytes=2210889,identity_rows=34,tombstone_rows=1,history_bytes=9086,checkpoint_bytes=2239,ledger_tail_rows=20,ledger_tail_payload_bytes=10292),measurement=dict(path=str(measurement_path),sha256=sha(measurement_path),startup_seconds=m['startup_timing']['operation_wall_seconds']['value'],protective_seconds=m['protective_timing']['operation_wall_seconds']['value'],protective_result=m['work']['work'],scope='Original owned in-process startup and no-execution-port protective work, same source/host D-volume, copied synthetic economics; monitor config absent.'),thresholds={k:dict(value=v[0],role=v[1],rationale=v[2],direction='minimum' if k=='HOST_DISK_RESERVE_BYTES' else 'maximum',boundary_validation='PENDING_M53') for k,v in sorted(thresholds.items())},intervals=intervals,uncovered_enforcement_claims=uncovered,M53_required=['Consume this exact profile content digest and preserve domain separation for isolated fixture stores.','Bind C2 monitor policy to actual qualification StartupIdentity/paths and this profile digest; canonical target MonitorConfiguration is provided separately.','Validate measured workload boundary restrictions and calibrated policy threshold crossings, freshness, restored-fact recovery and same-host monitor/protective timing without repeating accepted semantics.'],safety=dict(canonical_store_initialization=False,source_writes=False,signing=False,sending=False,network_requests=False))
    digest=content_fingerprint(profile)
    profile['content_digest']=digest
    alert_path=str(Path(target['paths']['deployment_root'])/'data'/'operations_alerts.sqlite3')
    consumed_identity=identity
    monitor_scope='CANONICAL_TARGET_CONFIGURATION_CANDIDATE'
    if consumed_identity is None:
        measured_identity=dict(m['current_startup_identity'])
        measured_identity['restart_profile']=RestartProfile(**measured_identity['restart_profile'])
        measured_identity['contracts']=tuple(tuple(x) for x in measured_identity['contracts'])
        consumed_identity=startup.StartupIdentity(**measured_identity)
        alert_path=str(out/'fixture-example-alerts.sqlite3')
        monitor_scope='TYPED_FIXTURE_EXAMPLE_ONLY_REBIND_TO_B_COPY_PATHS_NO_CANONICAL_TARGET_IDENTITY'
    reviewed=monitor_configuration_digest(consumed_identity,path=alert_path,host_identity_digest=host['current_host_fingerprint'],resource_max_age_us=intervals['resource_max_age_us']['value'],protective_qualification_digest=digest)
    policy=DegradationPolicy(reviewed,intervals['persistent_unknown_alert_us']['value'],intervals['recovery_evidence_max_age_us']['value'],tuple(ResourceLimit(k,v[0]) for k,v in sorted(thresholds.items())))
    monitor=MonitorConfiguration(alert_path,policy,host['current_host_fingerprint'],consumed_identity.runtime_code_digest,intervals['resource_max_age_us']['value'],digest)
    assert monitor.binding_for(consumed_identity)==policy.reviewed_configuration_digest
    assert all(not Path(target['paths'][k]).exists() for k in ('operations','ledger','producer','evidence_store'))
    assert not Path(alert_path).exists()
    write(out/'profile.json',profile)
    write(out/'monitor-configuration-candidate.json',dict(profile_content_digest=digest,scope=monitor_scope,monitor=asdict(monitor),policy_content_digest=policy.content_digest,activation=False,validation_state='TYPED_CONSTRUCTOR_BINDING_ONLY_M53_BOUNDARY_AND_OVERHEAD_PENDING',profile_digest_scope='Static evidence/config fingerprint; no live or fixture monitor was installed by this constructor check.'))
    write(out/'constructor-validation.json',dict(exit=0,profile_content_digest=digest,profile_sha256=sha(out/'profile.json'),profile_builder_sha256=sha(Path(__file__)),canonical_source_binding_constructed=binding is not None,canonical_startup_identity_constructed=identity is not None,typed_objects=['SourceProfile','LedgerDomain','PublicRpcProfile','ContinuationProfileV02','RestartProfile','StartupIdentity','ResourceLimit14metrics','DegradationPolicy','MonitorConfiguration'],monitor_binding_exact=True,source_reads_this_construction=frozen.get('rows_read',{}),previous_failed_source_selections=frozen.get('attempts',[]),canonical_store_opens=0,runtime_starts=0,workloads_rerun=0,source_metadata_only_measurement_reused=True))
    print(json.dumps(dict(profile=str(out/'profile.json'),content_digest=digest,sha256=sha(out/'profile.json'),frozen_source_rows=frozen.get('rows_read',{}),source_identity=None if binding is None else binding.source_identity,canonical_startup_configuration_digest=None if identity is None else identity.configuration_digest,no_t0_mints=no_t0,uncovered_enforcement_claims=uncovered),sort_keys=True))

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--public-manifest',type=Path,required=True)
    p.add_argument('--measurement',type=Path,required=True)
    p.add_argument('--host-observation',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--control-anchor-rowid',type=int)
    p.add_argument('--record-unresolved-source',action='store_true')
    a=p.parse_args()
    run(a.public_manifest,a.measurement,a.host_observation,a.output,a.control_anchor_rowid,a.record_unresolved_source)
