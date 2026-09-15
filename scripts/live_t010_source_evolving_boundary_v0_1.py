"""Isolated evolving original source history at the explicit T010 record cap."""
import argparse
import hashlib
import json
import sys
import time
from dataclasses import replace
from datetime import datetime,timedelta
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live import source_health_v0_1 as source
from live.evidence_store_v0_1 import SourceEvidenceStore
from live.t010_resource_envelope_v0_1 import derive,ResourceGate,enforce_current_process_rss
from live.runtime_dry_public_facts_v0_1 import windows_working_set_bytes
from live_t010_resource_envelope_selftest_v0_1 import fixture
from live_t010_resource_boundary_measure_v0_1 import physical

def main(path,output):
    output.mkdir(exist_ok=False)
    initial=source.verdict_from_json(json.dumps(json.loads(path.read_text())['initial_verdict']))
    inputs,accepted,extension=fixture();spec=derive(inputs,accepted,extension);envelope={'derivation':spec}
    quota=enforce_current_process_rss(spec['resource_limits']['HOST_RSS_BYTES'])
    gates={segment:ResourceGate(output/(segment+'.json'),envelope,segment,create=True)
        for segment in ('qualification','host')}
    gate=gates['qualification']
    counts=dict(zip(source.TABLES,map(len,(initial.snapshot.pump_rows,initial.snapshot.controls,
        initial.snapshot.receipts,initial.snapshot.gaps))))
    assert gate.reserve_capture(counts) and gate.reserve_source_verdict(initial)
    db=output/'source.sqlite';store=SourceEvidenceStore(db,initial.binding,initial.profile)
    store.append(initial,expected_previous_digest=source.ZERO_DIGEST)
    previous=initial;retained=1;max_record=0;max_progress=0;refused=None
    old_gap=initial.progress.gaps[-1];assert old_gap.end_utc < initial.binding.coverage_start_utc
    append_start=time.perf_counter_ns()
    for ordinal in range(1,spec['cumulative']['source_observations']):
        gate=gates['qualification' if ordinal<=spec['host']['max_source_units'] else 'host']
        count=5
        assert gate.reserve_capture(dict(zip(source.TABLES,(0,0,1,count))))
        stamp=(datetime.fromisoformat(initial.snapshot.observed_at_utc)+timedelta(microseconds=ordinal)).isoformat(timespec='microseconds')
        gaps=tuple(replace(old_gap,rowid=previous.progress.cursors[3].rowid+n+1,
            gap_key=hashlib.sha256(('evolving:'+str(ordinal)+':'+str(n)).encode()).hexdigest()) for n in range(count))
        receipt=source.ReceiptFact(previous.progress.cursors[2].rowid+1,'capacity:'+str(ordinal),
            initial.snapshot.receipts[-1].slot,stamp,1)
        cursors=(*previous.progress.cursors[:2],source.witness(source.TABLES[2],receipt),source.witness(source.TABLES[3],gaps[-1]))
        snapshot=source.SourceSnapshot(initial.binding.source_identity,initial.profile.fingerprint,stamp,stamp,
            previous.content_digest,receipts=(receipt,),gaps=gaps,cursors=cursors)
        verdict=source.evaluate_source(initial.binding,initial.profile,snapshot,previous=previous)
        assert verdict.disposition=='HEALTHY'
        if not gate.reserve_source_verdict(verdict):
            refused={'ordinal':ordinal,'metadata':gate.snapshot()['capacity_stop']};break
        store.append(verdict,expected_previous_digest=previous.content_digest)
        previous=verdict;retained+=1
        max_record=max(max_record,len(source.canonical_json(verdict).encode()))
        max_progress=max(max_progress,len(source.canonical_json(verdict.progress).encode()))
        if ordinal%100==0:print(json.dumps({'appended':retained,'record_bytes':max_record}),flush=True)
    append_us=(time.perf_counter_ns()-append_start)//1000
    logical=store._conn.execute('SELECT count(*),sum(length(CAST(payload_json AS BLOB))) FROM source_evidence').fetchone()
    store.close();before=hashlib.sha256(db.read_bytes()).hexdigest()
    start=time.perf_counter_ns();store=SourceEvidenceStore(db,initial.binding,initial.profile)
    cold_us=(time.perf_counter_ns()-start)//1000
    assert store.count()==retained and store.latest()==previous
    sql={name:store._conn.execute('PRAGMA '+name).fetchone()[0] for name in ('page_size','page_count','freelist_count','integrity_check')}
    result={'scope':'ISOLATED_ORIGINAL_EVOLVING_SOURCE_BOUNDARY_NOT_PUBLIC_QUALIFICATION',
        'initial_source':{'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()},
        'quota':quota,'retained_records':retained,'logical_payload_bytes':logical[1],
        'maximum_record_bytes':max_record,'maximum_progress_bytes':max_progress,
        'source_record_capacity':spec['source_record_bytes'],'capacity_refusal':refused,
        'append_us':append_us,'cold_original_source_us':cold_us,
        'original_startup_cap_us':spec['resource_limits']['STARTUP_US'],
        'source_alone_within_startup_cap':cold_us<=spec['resource_limits']['STARTUP_US'],
        'rss_after_bytes':windows_working_set_bytes(),'sqlite':sql,'files':physical(db),
        'source_sqlite_sha256_before_reopen':before,'public_qualification_claimed':False,
        'full_physical_coverage_claimed':False}
    store.close();assert hashlib.sha256(db.read_bytes()).hexdigest()==before
    (output/'measurement.json').write_text(json.dumps(result,sort_keys=True,indent=2));print(json.dumps(result))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();main(args.source,args.output)
