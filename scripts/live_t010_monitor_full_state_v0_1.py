"""Original-API monitor constructor superset; synthetic evidence, no public claim."""
from __future__ import annotations
import hashlib,json,shutil,sqlite3,time
from contextlib import closing
from dataclasses import asdict,replace
from pathlib import Path
import live_t010_joint_hot_measure_v0_2 as copies
from live.operations_degradation_v0_1 import ConditionEvidence,CONDITIONS,DegradationStore,METRICS
from live.operations_degradation_monitor_v0_1 import _subject
from phase5.shadow_domain_v0_1 import content_fingerprint

DERIVATION=Path(r'D:\Tradingbot\rescued_evidence\meme-live-fix2-evidence-20260913\monitor-full-envelope-derivation-v1.json')
DERIVATION_SHA='4edcd7083f4e6cc117cd2c893edbd39f97ac18f03b236344f9b1004063d7014e'

def prepare(original,config,origin,root,observations=2193,staging_root=None,max_records=None,*,resume=False):
    assert copies.sha(DERIVATION)==DERIVATION_SHA
    assert observations in (3,2190,2193)
    assert max_records in (None,1000)
    assert not resume or max_records is None, 'RESUME_REQUIRES_ORIGINAL_FULL_TARGET'
    args=copies.build_configuration(config)
    monitor=args['degradation_config']
    monitor=replace(monitor,policy=replace(monitor.policy,reviewed_configuration_digest=monitor.binding_for(args['expected_identity'])),
        runtime_code_digest=args['expected_identity'].runtime_code_digest)
    config['monitor']=asdict(monitor)
    args=copies.build_configuration(config)
    assert monitor.binding_for(args['expected_identity'])==monitor.policy.reviewed_configuration_digest
    with closing(sqlite3.connect(Path(original['paths']['ledger']).as_uri()+'?mode=ro',uri=True)) as conn:
        attempts=[row[0] for row in conn.execute('SELECT attempt_id FROM ledger_attempts ORDER BY attempt_id')]
    assert len(attempts) in (3,4) and len(set(attempts))==len(attempts)
    domain=args['domain'];binding=args['source_binding'];source_profile=args['source_profile']
    control=_subject(domain,'OPERATIONS_CONTROL',domain.binding_digest)
    source=_subject(domain,'SOURCE',(binding.source_identity,source_profile.fingerprint))
    producer=_subject(domain,'PRODUCER',(binding.source_identity,asdict(args['producer_profile'])))
    custody=_subject(domain,'CUSTODY',domain.binding_digest)
    coverage=_subject(domain,'PROFILE_COVERAGE',monitor.policy.content_digest)
    humans=[(code,control) for code in ('OWNER_FENCE_UNPROVEN','OPERATOR_STOPPED','RESTART_EXHAUSTED','SUPERVISOR_HELD')]
    humans += [('SOURCE_IDENTITY_OR_HISTORY_BROKEN',source),('PRODUCER_INTEGRITY_UNAVAILABLE',producer),
        ('CUSTODY_TRUTH_UNAVAILABLE',custody)]
    humans += [('UNSENT_ATTEMPT_RECOVERY_REQUIRED',attempt) for attempt in attempts]
    expected=20*(4*observations//3)+2*observations+len(humans)+2
    stamp=config['now']*1000000-expected-1000
    stage_path=Path(monitor.path) if staging_root is None else Path(staging_root)/Path(monitor.path).name
    counter=0;states={};expected_stream=hashlib.sha256();begin=time.perf_counter_ns()
    log_root=root if staging_root is None else Path(staging_root)
    progress=log_root/'monitor-construction-progress.jsonl'
    elapsed_before_us=0
    if resume:
        assert stage_path.is_file() and progress.is_file(), 'RESUME_EXISTING_CHECKPOINT_REQUIRED'
        # The original constructor/snapshot must accept the original identity.
        # Never initialize, repair, or rebind an existing store.
        store=DegradationStore(stage_path,domain,monitor.policy)
        retained=store.snapshot()
        states={(r.condition,r.subject_digest):r for r in retained.conditions}
        checkpoints={}
        for line in progress.read_text(encoding='utf-8').splitlines():
            row=json.loads(line)
            assert row['records']>max(checkpoints,default=0), 'RESUME_PROGRESS_ORDER_CHANGED'
            checkpoints[row['records']]=row['digest_stream_sha256']
            elapsed_before_us=row['elapsed_us']
        # One ordered prefix read restores the accumulator and actual committed
        # count, including the committed tail beyond the last progress entry.
        with closing(sqlite3.connect(stage_path.as_uri()+'?mode=ro',uri=True)) as conn:
            for counter,(digest,) in enumerate(conn.execute('SELECT digest FROM receipts ORDER BY rowid'),1):
                expected_stream.update((digest+'\n').encode())
                if counter in checkpoints:
                    assert expected_stream.hexdigest()==checkpoints[counter], 'RESUME_PROGRESS_DIGEST_CHANGED'
        assert max(checkpoints,default=0)<=counter<=expected, 'RESUME_COMMITTED_COUNT_OUT_OF_RANGE'
        assert (not states and counter==0) or max(r.observed_us for r in states.values())==stamp+counter, \
            'RESUME_ORIGINAL_TIME_OR_TARGET_CHANGED'
        stamp+=counter
    else:
        if staging_root is not None:Path(staging_root).mkdir(parents=True,exist_ok=False)
        store=DegradationStore.initialize(stage_path,domain,monitor.policy,now_us=stamp)
        begin=time.perf_counter_ns()
    resumed_from=counter
    with progress.open('a' if resume else 'x',encoding='utf-8') as progress_file:
        def retain(code,subject,state='ACTIVE'):
            nonlocal counter,stamp,states
            counter+=1;stamp+=1
            old=states.get((code,subject))
            witness=content_fingerprint(('SYNTHETIC_EXTERNAL_FACTS_MONITOR_V1',observations,counter,code,subject,state))
            evidence=ConditionEvidence(code,subject,stamp,witness,state,
                'OWNER_CONDITION_OBSERVED' if state=='ACTIVE' else CONDITIONS[code][1],
                None if state=='ACTIVE' else old.active_digest)
            facts=store.record(evidence,now_us=stamp,coalesce_active=True)
            states={(r.condition,r.subject_digest):r for r in facts.conditions}
            expected_stream.update((evidence.content_digest+'\n').encode())
            if counter%1000==0:
                row=dict(records=counter,elapsed_us=elapsed_before_us+(time.perf_counter_ns()-begin)//1000,
                    digest_stream_sha256=expected_stream.hexdigest())
                progress_file.write(json.dumps(row)+'\n');progress_file.flush()
                print(json.dumps({'monitor_prefix':row}),flush=True)
        def operations():
            for code,subject in humans:yield code,subject,'ACTIVE'
            for code,subject in (('CLOCK_UNPROVEN',control),('SOURCE_TRUTH_UNAVAILABLE',source)):
                for n in range(observations):yield code,subject,'ACTIVE' if n%2==0 else 'RECOVERED'
            for metric in sorted(METRICS):
                subject=_subject(domain,'RESOURCE',(monitor.policy.content_digest,metric))
                for _ in range(observations//3):
                    yield 'PROFILE_UNRESOLVED',subject,'ACTIVE'
                    yield 'RESOURCE_EXCEEDED',subject,'ACTIVE'
                    yield 'PROFILE_UNRESOLVED',subject,'RECOVERED'
                    yield 'RESOURCE_EXCEEDED',subject,'RECOVERED'
            yield 'PROFILE_UNRESOLVED',coverage,'ACTIVE'
            yield 'PROFILE_UNRESOLVED',coverage,'RECOVERED'
        for ordinal,(code,subject,state) in enumerate(operations(),1):
            if ordinal<=resumed_from:continue
            retain(code,subject,state)
            if counter==max_records:break
    assert counter==(expected if max_records is None else max_records)
    final=store.snapshot()
    if max_records is None:assert len(final.conditions)==50+len(attempts)
    actual_stream=hashlib.sha256()
    with closing(sqlite3.connect(stage_path.as_uri()+'?mode=ro',uri=True)) as conn:
        for row in conn.execute('SELECT digest FROM receipts ORDER BY rowid'):
            actual_stream.update((row[0]+'\n').encode())
        count=conn.execute('SELECT count(*) FROM receipts').fetchone()[0]
        integrity=conn.execute('PRAGMA integrity_check').fetchone()[0]
    assert count==counter and actual_stream.hexdigest()==expected_stream.hexdigest() and integrity=='ok'
    final_copy_verified=False
    if staging_root is not None and max_records is None:
        if not (resume and Path(monitor.path).exists()):
            assert not Path(monitor.path).exists()
            shutil.copy2(stage_path,monitor.path)
        assert copies.sha(stage_path)==copies.sha(Path(monitor.path))
        assert DegradationStore(monitor.path,domain,monitor.policy).snapshot()==final
        final_copy_verified=True
    record=dict(schema='MEME_LIVE_T010_MONITOR_ORIGINAL_API_CONSTRUCTOR_SUPERSET_V1',
        scope='SYNTHETIC_EXTERNAL_FACTS_CONSTRUCTOR_SUPERSET_NOT_A_REACHABLE_PUBLIC_CAMPAIGN',
        derivation={'path':str(DERIVATION),'sha256':DERIVATION_SHA},observations=observations,
        original_attempt_ids=attempts,receipts=count,conditions=len(final.conditions),
        original_record_calls=counter,receipt_insertion_stream_sha256=actual_stream.hexdigest(),
        full_construction=max_records is None,target_receipts=expected,
        staging_path=str(stage_path),final_monitor_path=monitor.path,final_copy_verified=final_copy_verified,
        final_conditions=[asdict(v) for v in final.conditions],construction_us=(time.perf_counter_ns()-begin)//1000,
        monitor_policy=asdict(monitor.policy),original_constructor_available=True,
        original_write_and_full_read_checks_preserved=True,sqlite_integrity=integrity,
        no_receipt_hash_fabrication=True,profile_or_history_migration=False,
        qualification_claimed=False)
    if resume:
        record.update(resumed_from_receipts=resumed_from,construction_us_scope='this continuation invocation')
    copies.write(log_root/'monitor-reconstruction.json',record)
    if max_records is None and log_root!=root:
        copies.write(root/'monitor-reconstruction.json',record)
        shutil.copy2(progress,root/progress.name)
    return record
