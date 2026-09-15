"""V2 disjoint original-API joint hot states; never a public campaign.

Only a new caller-selected temporary directory is written. Actual retained
source facts provide historical shape; their time translation and new fixture
identities are explicit. No production collector, wallet or network is opened.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import sqlite3
import sys
import time
from contextlib import ExitStack, closing
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_runtime_owned_dry_selftest_v0_1 as owned
from live import source_health_v0_1 as sh
from live.continuous_producer_v0_2 import ContinuationProfileV02
from live.operations_degradation_v0_1 import ConditionEvidence, CONDITIONS
from live.operations_degradation_monitor_v0_1 import _producer_cut, _subject
from live.t010_resource_envelope_v0_1 import derive, ResourceGate
from live_t010_resource_envelope_selftest_v0_1 import fixture as resource_fixture
from phase5.shadow_domain_v0_1 import content_fingerprint
from solders.pubkey import Pubkey


def write(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2)+'\n', encoding='utf-8')


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def shaped_raw(path, actual, original):
    """Reconstruct retained historical control/gap shape in disposable raw SQL."""
    binding = original(path)
    delta = datetime.fromisoformat(binding.coverage_start_utc)-datetime.fromisoformat(actual.binding.coverage_start_utc)
    def shift(value):
        return '' if not value else (datetime.fromisoformat(value)+delta).isoformat(timespec='microseconds')
    with sqlite3.connect(path) as c:
        c.execute('DELETE FROM collector_events')  # New synthetic raw fixture only.
        for n, value in enumerate(actual.snapshot.controls, 1):
            details = dict(gap_started_at_utc=shift(value.gap_start_utc), gap_ended_at_utc=shift(value.gap_end_utc))
            c.execute('INSERT INTO collector_events VALUES(?,?,?,?)',
                (n,value.event_type,shift(value.at_utc),json.dumps(details)))
        for value in actual.snapshot.gaps:
            c.execute('INSERT INTO gap_jobs_v034 VALUES(?,?,?,?,?,?,?,?,?)',
                (value.gap_key,shift(value.start_utc),shift(value.end_utc),value.status,
                 value.before_slot,value.after_slot,value.confirmed_blocks_checked,value.events_recovered,None))
        anchors = tuple(sh.witness(t,sh._fact(t,c.execute('SELECT '+sh._SELECT[t]+' FROM '+t+' WHERE rowid=1').fetchone()))
            for t in sh.TABLES[:3])
    return replace(binding, lineage='T010:SYNTHETIC:ACTUAL_HISTORICAL_SHAPE', anchors=anchors)


def append_candidate(f, ordinal):
    mint = str(Pubkey.from_bytes(hashlib.sha256(('joint-mint-'+str(ordinal)).encode()).digest()))
    # Original winning first-pullback pattern; no candidate or inbox fabrication.
    with sqlite3.connect(f.raw) as c:
        last=c.execute('SELECT max(rowid) FROM pump_events').fetchone()[0]
        for index,(n,_,kind,price) in enumerate(owned.rt.hf.FIRST,1):
            rowid=last+index;offset=n+14*ordinal
            owned.rt.hf.raw_fixture.insert_row(c,rowid,mint,kind,price_units=price,observed_offset=offset)
            c.execute('UPDATE pump_events SET inserted_at_utc=decoded_at_utc WHERE rowid=?',(rowid,))
            c.execute('INSERT INTO websocket_observations VALUES(?,?,?,?,?)',
                ('sig-'+str(rowid),5000000+rowid,owned.rt.hf.at(offset+2),1,'{}'))


def append_hot_batch(f, batch, mints, events, offset):
    plan=f.hot_plan
    indexes=plan[batch]
    with sqlite3.connect(f.raw) as c:
        last=c.execute('SELECT max(rowid) FROM pump_events').fetchone()[0]
        for i,(mint_number,event) in enumerate(indexes):
            mint=str(Pubkey.from_bytes(hashlib.sha256(('joint-hot-'+str(mint_number)).encode()).digest()))
            rowid=last+i+1
            owned.rt.hf.raw_fixture.insert_row(c,rowid,mint,'LAUNCH' if event==0 else 'BUY',observed_offset=offset)
            c.execute('UPDATE pump_events SET inserted_at_utc=decoded_at_utc WHERE rowid=?',(rowid,))
            c.execute('INSERT INTO websocket_observations VALUES(?,?,?,?,?)',
                ('sig-'+str(rowid),5000000+rowid,owned.rt.hf.at(offset+2),1,'{}'))


def hot_plan(mode):
    if mode=='hottest_pending':
        counts=[512]+[17]*7+[16]*24  # 1015 retained + nine final winning events.
    elif mode=='active64_hold':
        counts=[512]+[16]*15+[15]*16  # 992 in first32 active; then32 new launches.
    else:
        counts=[256]*4
    sequence=[(mint,0) for mint in range(len(counts))]
    sequence += [(mint,event) for mint,count in enumerate(counts) for event in range(1,count)]
    chunks=[sequence[i:i+32] for i in range(0,len(sequence),32)]
    if mode=='active64_hold':chunks.append([(mint,0) for mint in range(32,64)])
    if mode=='four_hot_hold':
        tail=chunks.pop();chunks.extend((tail[:16],tail[16:]))
    return counts,chunks


def add_source_rows(f, ordinal, at, gaps):
    with sqlite3.connect(f.raw) as c:
        c.execute('INSERT INTO websocket_observations VALUES(?,?,?,?,?)',
            ('joint-source-'+str(ordinal),6000000+ordinal,owned.a3.utc(at),1,'{}'))
        for n in range(gaps):
            c.execute('INSERT INTO gap_jobs_v034 VALUES(?,?,?,?,?,?,?,?,?)',
                (content_fingerprint(('joint-gap',ordinal,n)),owned.rt.hf.at(-101),owned.rt.hf.at(-100),
                 'PENDING',440000001,440000002,0,0,None))


def source_unit(f, ordinal, at, gaps, *, candidate=False):
    started_ns=time.perf_counter_ns()
    gate=getattr(f,'active_gate',None)
    assert gate is None or gate.before_step(f.started), gate.snapshot() if gate else None
    add_source_rows(f,ordinal,at,gaps)
    timer = f.producer.prepare_clock_tick(datetime.fromisoformat(owned.a3.utc(at)),
        timer_id=content_fingerprint(('joint-original-timer-unit',ordinal)))
    result = f.producer.process_next_batch(batch_size=32 if gate is None else f.runtime._resource_batch_rows,max_raw_bytes=1024*1024)
    f.producer.execute_prepared_timer(timer)
    prior = f.source.latest()
    def reserve(counts):
        path=f.directory/'capture-reservations.json'
        state=json.loads(path.read_text()) if path.exists() else dict(captures=0,rows={t:0 for t in sh.TABLES})
        assert all(state['rows'][t]+counts[t] <= 20000 for t in sh.TABLES)
        state['captures']+=1
        for t in sh.TABLES:state['rows'][t]+=counts[t]
        write(path,state)  # Recorded before original capture reads payload rows.
        return True
    verdict = sh.CollectorSourceAdapter(f.raw).observe(f.binding,f.source.profile,
        observed_at_utc=owned.a3.utc(at),requested_cut_utc=owned.a3.utc(at),previous=prior,
        max_raw_bytes=1024*1024,reserve_rows=reserve if gate is None else gate.reserve_capture)
    size = len(sh.canonical_json(verdict).encode())
    assert verdict.disposition == 'HEALTHY', verdict.reasons
    assert gate is None or gate.reserve_source_verdict(verdict)
    assert size <= 1024*1024, ('source-record-cap',ordinal,size)
    f.source.append(verdict,expected_previous_digest=prior.content_digest)
    roots=[]
    while True:
        page=f.handoff.deliver_page(limit=32)
        roots.extend(page.candidate_roots)
        if not page.scanned_rows:break
    f.last_source_unit_us=(time.perf_counter_ns()-started_ns)//1000
    return verdict, roots, result.raw_rows_fetched, size


def decide(f, root, ordinal, at, admit):
    started_ns=time.perf_counter_ns()
    f.root,f.item=root,f.repo.candidate(root)
    # New coherent synthetic finalized block per observation. Never reuse a
    # historical block timestamp as current wallet evidence for later roots.
    slot=102+ordinal
    f.scenario.context=f.scenario.multiple_context=slot
    f.scenario.upper_slot=slot+1
    f.scenario.block_time=at-20
    def block(payload,envelope):
        if payload['method']=='getBlock':
            envelope['result'].update(blockhash=owned.sf.bh(10+ordinal),
                previousBlockhash=f.repo.baseline().observation.anchor.blockhash if ordinal==0 else owned.sf.bh(9+ordinal),
                blockHeight=91+ordinal,blockTime=at-20)
        return envelope
    f.scenario.transform=block
    a3=owned.a3
    expected={v.pubkey:(v.mint,v.program) for v in f.repo.consumer_snapshot()['accounts']}
    expected.update({a3.derive_associated_token_address(f.domain.wallet,f.item.mint,owned.sf.TOKEN_PROGRAM_ID):
        (f.item.mint,owned.sf.TOKEN_PROGRAM_ID),a3.derive_associated_token_address(f.domain.wallet,owned.sf.WSOL_MINT,
        owned.sf.TOKEN_PROGRAM_ID):(owned.sf.WSOL_MINT,owned.sf.TOKEN_PROGRAM_ID)})
    for mint,program in expected.values():
        f.scenario.accounts.setdefault(mint,a3.account(program,a3.mint_data(9 if mint==owned.sf.WSOL_MINT else 6)))
    request=a3.WalletEvidenceRequest(f.domain.wallet,f.domain.genesis_hash,slot,
        tuple(a3.ExpectedTokenAccount(key,mint,program) for key,(mint,program) in sorted(expected.items())))
    f.scenario.initial_slot=slot;f.scenario.slot_calls=f.scenario.genesis_calls=0
    support=a3.WalletSupportInput(a3.observe(f.scenario,request=request,at=at),a3.utc(at),slot)
    receipt=f.repo.admit_authority_entry(owned.a3.EntryRequest(root,'PUMP' if admit else 'PUMPSWAP',
        owned.sf.TOKEN_PROGRAM_ID,f.policy.selected_track),owned.a3.clock(f.repo,at),f.source,support,
        command_id='joint-decision-'+str(ordinal),fence=f.repo.write_fence())
    assert receipt.accepted == admit, receipt.risk.reasons
    f.last_decision_us=(time.perf_counter_ns()-started_ns)//1000
    if root in f.runtime._queue:f.runtime._queue.remove(root)
    if not admit:return None
    action=f.repo.action(receipt.action.action_id) if hasattr(receipt,'action') else f.repo.reservation(root).admission.action
    f.runtime._entry_action_id=action.action_id
    return action


def incident_history(f, pairs):
    """Original journal API only; no bulk SQL receipt fabrication."""
    monitor=f.runtime._operations_degradation
    subjects=[('RESOURCE_EXCEEDED',_subject(f.domain,'RESOURCE',(monitor.configuration.policy.content_digest,k)))
        for k in ('HOST_DISK_RESERVE_BYTES','OLDEST_UNCONSUMED_AGE_US')]
    subjects += [('CLOCK_UNPROVEN',_subject(f.domain,'OPERATIONS_CONTROL',f.domain.binding_digest)),
        ('SOURCE_TRUTH_UNAVAILABLE',_subject(f.domain,'SOURCE',(f.binding.source_identity,f.source.profile.fingerprint)))]
    at=(owned.NOW+2000)*1000000
    count=0
    for episode in range(pairs):
        for code,subject in subjects:
            at+=2
            active=ConditionEvidence(code,subject,at,content_fingerprint(('joint-active',subject,episode)))
            monitor.store.record(active,now_us=at)
            recovery=ConditionEvidence(code,subject,at+1,content_fingerprint(('joint-recovered',subject,episode)),
                'RECOVERED',CONDITIONS[code][1],active.content_digest)
            monitor.store.record(recovery,now_us=at+1)
            count+=2
        if episode and episode%100==0:print(json.dumps({'incident_pairs':episode}),flush=True)
    return dict(record_calls=count,subjects=len(subjects),pairs=pairs,last_at_us=at+1)


def configuration(f, producer_profile):
    args=owned.fixtures.c4.cold_args(f)
    args.update(producer_profile=producer_profile,batch_rows=32,page_rows=32,queued_roots=64,
        degradation_config=f.startup_monitor_config,dry_live_paths=f.live_paths,
        expected_baseline_observation_digest=f.repo.baseline().observation.content_digest)
    return dict(domain=asdict(f.domain),source_binding=asdict(f.binding),source_profile=asdict(f.source.profile),
        producer_profile=asdict(producer_profile),database_identity=args['database_identity'],
        expected_identity=asdict(f.expected),monitor=asdict(f.startup_monitor_config),
        live_paths=list(f.live_paths),source_start_after=1,now=owned.NOW+2001,
        baseline_digest=f.repo.baseline().observation.content_digest,
        paths=dict(ledger=str(f.path),producer=str(f.ppath),source=str(args['source_path']),
            raw=str(f.raw),operations=str(f.operations_path),monitor=str(f.startup_monitor_config.path)))


def build(output, source, roots=65, views=627, incident_pairs=951, hot_mints=32, hot_events=32, late_startups=99, gated=True, mode='hottest_pending'):
    assert 4 <= roots <= 128 and views >= roots+3 and views <= 627
    assert roots*2+hot_mints<=256 and hot_mints*hot_events<=1024 and hot_events<=512
    assert hot_mints*hot_events%32==0 and 1<=late_startups<=99
    output.mkdir(parents=True,exist_ok=False)
    actual=sh.verdict_from_json(json.dumps(json.loads(source.read_text())['initial_verdict']))
    profile=ContinuationProfileV02(**resource_fixture()[1]['constructor_configuration']['continuation_profile'])
    old_raw,old_init,old_args,old_policy,old_arm=(owned.rt.hf.build_raw,
        owned.rt.hf.LiveContinuousProducerV02.__init__,owned.fixtures.c4.cold_args,owned.a3.a1.policy,owned.a3.arm)
    from live.evidence_store_v0_1 import SourceEvidenceStore
    source_init=SourceEvidenceStore.__init__
    def initial_source(self,path,binding,profile):
        fresh=not Path(path).exists()
        source_init(self,path,binding,profile)
        if fresh:
            value=sh.CollectorSourceAdapter(output/'joint-raw.sqlite').observe(binding,profile,
                observed_at_utc=owned.a3.utc(owned.NOW+4),requested_cut_utc=binding.coverage_start_utc,
                **(dict(max_raw_bytes=1024*1024,reserve_rows=active[0].reserve_capture) if active[0] else {}))
            assert value.disposition=='HEALTHY',value.reasons
            assert active[0] is None or active[0].reserve_source_verdict(value)
            self.append(value,expected_previous_digest=sh.ZERO_DIGEST)
    def init(self,*a,**kw):kw.setdefault('profile',profile);old_init(self,*a,**kw)
    def args(f):return dict(old_args(f),producer_profile=profile,batch_rows=32,page_rows=32,queued_roots=64)
    def policy(*a,**kw):return replace(old_policy(*a,**kw),allowed_venues=('PUMP',),entry_valid_through_utc=owned.a3.utc(owned.NOW+7200))
    def arm(repo,p,**kw):
        # Explicit synthetic bounded control fixture, not host self-issuance.
        from live.authority_controls_v0_1 import ArmingGrant
        grant=ArmingGrant(kw.get('name','grant1'),p.content_digest,'DRY',None,owned.a3.utc(owned.NOW),
            owned.a3.utc(owned.NOW+7200),owned.a3.a1.operator())
        return owned.a3.control(repo,'ARM','arm-'+grant.grant_id,grant=grant)
    begin=time.perf_counter_ns()
    spec=derive(*resource_fixture())
    gates={segment:ResourceGate(output/(segment+'-resources.json'),{'derivation':spec},segment,create=True)
        for segment in ('qualification','host')} if gated else {}
    active=[gates.get('qualification')]
    original_source_page=owned.runtime.RuntimeCompositionV01._source_page
    def gated_source_page(runtime,*a,**kw):
        assert active[0].before_step(SimpleNamespace(runtime=runtime)),active[0].snapshot()
        return original_source_page(runtime,*a,**kw)
    with ExitStack() as stack:
        stack.enter_context(patch.object(owned.rt.hf,'build_raw',lambda p:shaped_raw(p,actual,old_raw)))
        stack.enter_context(patch.object(SourceEvidenceStore,'__init__',initial_source))
        stack.enter_context(patch.object(owned.rt.hf,'FIRST',tuple(r for r in owned.rt.hf.FIRST if r[1]==owned.rt.hf.MINT_A)))
        stack.enter_context(patch.object(owned.rt.hf.LiveContinuousProducerV02,'__init__',init))
        stack.enter_context(patch.object(owned.fixtures.c4,'cold_args',args))
        stack.enter_context(patch.object(owned.a3.a1,'policy',policy))
        stack.enter_context(patch.object(owned.a3,'arm',arm))
        if gated:stack.enter_context(patch.object(owned.runtime.RuntimeCompositionV01,'_source_page',gated_source_page))
        f=owned.fixture(output,'joint')
        try:
            hot_counts,f.hot_plan=hot_plan(mode)
            for _ in range(late_startups-1):owned.restart(f)
            f.active_gate=active[0]
            initial_count=f.source.count()
            log=[];terminal_roots=[];pending=None;raw_total=9
            # Source growth precedes later candidate decisions. Largest growth
            # prefix is deliberately finite; plateau is still evolving by receipt.
            additions=(1000,500,250,125,62,31,15,7,3,1,*([100]*8),50)
            hot_units=len(f.hot_plan)
            idle=views-initial_count-roots-hot_units
            ordinal=0
            before_qualification=min(idle,313-(initial_count-1)-2)
            for i in range(before_qualification):
                at=owned.NOW+4
                verdict,_,raw,size=source_unit(f,ordinal,at,additions[i] if i<len(additions) else 0)
                raw_total+=raw;log.append(dict(unit=ordinal,kind='SOURCE_ONLY',source_digest=verdict.content_digest,bytes=size))
                ordinal+=1
                if ordinal%50==0:print(json.dumps({'source_units':ordinal,'bytes':size}),flush=True)
            for n in range(roots):
                if n==2:
                    active[0]=gates.get('host');f.active_gate=active[0]
                    for i in range(before_qualification,idle):
                        at=owned.NOW+23
                        verdict,_,raw,size=source_unit(f,ordinal,at,additions[i] if i<len(additions) else 0)
                        raw_total+=raw;log.append(dict(unit=ordinal,kind='SOURCE_ONLY',source_digest=verdict.content_digest,bytes=size))
                        ordinal+=1
                        if ordinal%50==0:print(json.dumps({'source_units':ordinal,'bytes':size}),flush=True)
                if n==roots-1 and mode=='hottest_pending':
                    for batch in range(hot_units):
                        append_hot_batch(f,batch,hot_mints,hot_events,14*n+2)
                        verdict,new,raw,size=source_unit(f,ordinal,owned.NOW+4+14*n,0)
                        assert not new,('hot-fixture-produced-candidate',new)
                        raw_total+=raw;log.append(dict(unit=ordinal,kind='HOT_ACTIVE_SOURCE',source_digest=verdict.content_digest,bytes=size,
                            source_unit_us=f.last_source_unit_us))
                        ordinal+=1
                if n:append_candidate(f,n)
                at=owned.NOW+4+14*n
                verdict,new,raw,size=source_unit(f,ordinal,at,1,candidate=True)
                root=f.root if n==0 else new[0]
                assert n==0 or len(new)==1,('candidate-count',n,len(new))
                admit=n in (0,1,roots-2) or (n==roots-1 and mode=='hottest_pending')
                action=decide(f,root,n,at,admit)
                if action is not None:
                    try:
                        original_facts=owned.DryPublicFacts
                        original_evidence=owned.external.a4.evidence
                        def current_cut(*a,**kw):
                            return replace(original_facts(*a,**kw),source_cut_utc=verdict.snapshot.requested_cut_utc)
                        def current_evidence(*a,**kw):
                            kw.update(context_slot=f.scenario.context,wallet_floor=f.scenario.context,
                                last_valid_height=200+n,validity_height=100+n)
                            return original_evidence(*a,**kw)
                        with patch.object(owned,'DryPublicFacts',current_cut),patch.object(owned.external.a4,'evidence',current_evidence):
                            result,_=owned.execute(f,action,at=at+5,point='before-terminal' if n==roots-1 and mode=='hottest_pending' else None)
                        assert result.work=='NON_SUBMITTED',asdict(result)
                    except owned.Interrupted:pending=action.root_id
                    else:terminal_roots.append(action.root_id)
                raw_total+=raw
                log.append(dict(unit=ordinal,kind='ADMISSION' if admit else 'VENUE_DENIAL',root=root,
                    source_digest=verdict.content_digest,bytes=size,source_unit_us=f.last_source_unit_us,
                    decision_us=f.last_decision_us))
                ordinal+=1
                print(json.dumps({'decisions':n+1,'source_records':f.source.count(),'admitted':admit}),flush=True)
            if mode!='hottest_pending':
                for batch in range(hot_units):
                    append_hot_batch(f,batch,hot_mints,hot_events,14*roots+2)
                    verdict,new,raw,size=source_unit(f,ordinal,owned.NOW+4+14*roots,0)
                    assert not new,('hot-fixture-produced-candidate',new)
                    raw_total+=raw;log.append(dict(unit=ordinal,kind='HOT_ACTIVE_SOURCE',source_digest=verdict.content_digest,bytes=size,
                        source_unit_us=f.last_source_unit_us))
                    ordinal+=1
            assert f.source.count()==views,(f.source.count(),views)
            metrics,_,checkpoint=_producer_cut(f.producer)
            expected_events=1015 if mode=='hottest_pending' else 1024
            expected_active=32 if mode=='hottest_pending' else 64 if mode=='active64_hold' else 4
            assert metrics['RETAINED_EVENTS']==expected_events and metrics['UNFINISHED_MINTS']==expected_active,metrics
            final_gate=None
            if mode!='hottest_pending':
                assert not f.active_gate.before_step(f.started),f.active_gate.snapshot()
                final_gate=f.active_gate.snapshot()['held_reason']
                assert final_gate==('JOINT_IDENTITY_RETIREMENT_CAPACITY' if mode=='active64_hold' else 'SOURCE_WINDOW_EXHAUSTED'),final_gate
            incidents=incident_history(f,incident_pairs)
            config=configuration(f,profile)
            custody=f.repo.consumer_snapshot()
            assert not custody['positions'] and not custody['protections'] and len(custody['reservations'])==int(mode=='hottest_pending')
            write(output/'configuration.json',config)
            write(output/'construction.json',dict(schema='MEME_LIVE_T010_JOINT_SYNTHETIC_STATE_V1',
                scope='ORIGINAL_API_STRESS_SUPERSET_WITH_REBASED_ACTUAL_SOURCE_SHAPE_NOT_GATED_T010_OR_PUBLIC_QUALIFICATION',
                initial_source_reference=dict(path=str(source),sha256=digest(source)),
                transformations=['actual historical controls/gaps shifted by exact coverage-origin delta',
                    'synthetic producer-winning raw events and explicit two-hour DRY control fixture',
                    'unique original-API timer IDs and one new retained historical gap per candidate decision',
                    'original API source/producer/ledger/incident writes; no historical DB merging'],
                roots=roots,source_records=views,source_units=ordinal,initial_source_records=initial_count,
                disjoint_mode=mode,hot_event_counts=hot_counts,final_gate_hold=final_gate,
                hot_mints=hot_mints,hot_events_per_mint=hot_events,owned_startups=late_startups,
                original_resource_gate_exercised=gated,resource_segments={k:v.snapshot() for k,v in gates.items()},
                raw_pump_rows=raw_total,terminal_roots=terminal_roots,pending_root=pending,
                producer_metrics=metrics,checkpoint_digest=checkpoint,incidents=incidents,units=log,
                construction_us=(time.perf_counter_ns()-begin)//1000,
                known_coverage_limits=['segment timing/restart budget schedule not yet replayed by whole supervisor',
                    'conservative ResourceGate batch reservation may stop sequential candidate schedule before128; dataset is a stress superset',
                    'synthetic incident evidence supplied to original API, not actual physical pressure',
                    'full constructor supported; final profile/public qualification not claimed'],
                public_qualification_claimed=False,full_physical_coverage_claimed=False))
        except BaseException as exc:
            write(output/'configuration.json',configuration(f,profile))
            write(output/'construction.json',dict(schema='MEME_LIVE_T010_JOINT_SYNTHETIC_STATE_V1',
                scope='ORIGINAL_API_STRESS_SUPERSET_STOPPED_AT_ORIGINAL_BOUNDARY',
                failure=dict(type=type(exc).__name__,reason=str(exc)),source_records=f.source.count(),
                planned_roots=roots,planned_views=views,completed_units=locals().get('log',[]),
                disjoint_mode=mode,resource_segments={k:v.snapshot() for k,v in gates.items()},
                producer_metrics=_producer_cut(f.producer)[0],
                original_retained_source_digest=f.source.latest().content_digest,
                initial_source_reference=dict(path=str(source),sha256=digest(source)),
                public_qualification_claimed=False,full_physical_coverage_claimed=False))
            raise
        finally:owned.fixtures.close(f)
    files={}
    for path in output.glob('*.sqlite'):
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as c:
            files[path.name]=dict(sha256=digest(path),bytes=path.stat().st_size,
                pragmas={k:c.execute('PRAGMA '+k).fetchone()[0] for k in ('journal_mode','page_size','page_count','freelist_count','integrity_check')},
                tables={r[0]:c.execute('SELECT count(*) FROM "'+r[0]+'"').fetchone()[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")})
    write(output/'files.json',files)
    print(json.dumps({'output':str(output),'construction_sha256':digest(output/'construction.json')}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--source',type=Path,required=True)
    p.add_argument('--mode',choices=('hottest_pending','active64_hold','four_hot_hold'),required=True)
    p.add_argument('--roots',type=int);p.add_argument('--views',type=int)
    p.add_argument('--incident-pairs',type=int,default=951)
    p.add_argument('--hot-mints',type=int,default=0);p.add_argument('--hot-events',type=int,default=0)
    p.add_argument('--late-startups',type=int,default=99);p.add_argument('--gated',action='store_true');a=p.parse_args()
    roots=a.roots or (65 if a.mode=='hottest_pending' else 64)
    views=a.views or (626 if a.mode=='active64_hold' else 627)
    mints,events=(4,256) if a.mode=='four_hot_hold' else (32,32)
    build(a.output,a.source,roots,views,a.incident_pairs,mints,events,a.late_startups,True,a.mode)
