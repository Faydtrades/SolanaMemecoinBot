"""Isolated cumulative original source journal using an actual retained shape.

Later empty cuts are synthetic stress, never public freshness/qualification.
The actual source database and retained input evidence remain read-only.
"""
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
from live.source_health_v0_1 import verdict_from_json,SourceSnapshot,evaluate_source,ZERO_DIGEST,canonical_json
from live.evidence_store_v0_1 import SourceEvidenceStore
from live.t010_resource_envelope_v0_1 import enforce_current_process_rss
from live.runtime_dry_public_facts_v0_1 import windows_working_set_bytes
from live_t010_resource_boundary_measure_v0_1 import physical

def main(source,output,observations):
    output.mkdir(parents=True,exist_ok=False)
    original=verdict_from_json(json.dumps(json.loads(source.read_text())['initial_verdict']))
    quota=enforce_current_process_rss(96*1024*1024)
    path=output/'source-evidence.sqlite'
    store=SourceEvidenceStore(path,original.binding,original.profile)
    store.append(original,expected_previous_digest=ZERO_DIGEST)
    previous=original
    begin=time.perf_counter_ns()
    for ordinal in range(1,observations):
        at=(datetime.fromisoformat(original.snapshot.observed_at_utc)+timedelta(microseconds=ordinal)).isoformat(timespec='microseconds')
        snap=SourceSnapshot(original.binding.source_identity,original.profile.fingerprint,at,
            original.snapshot.requested_cut_utc,previous.content_digest,cursors=previous.progress.cursors)
        current=evaluate_source(original.binding,original.profile,snap,previous=previous)
        assert current.disposition=='HEALTHY'
        store.append(current,expected_previous_digest=previous.content_digest)
        previous=current
    append_us=(time.perf_counter_ns()-begin)//1000
    rows,bytes_=store._conn.execute('SELECT count(*),sum(length(CAST(payload_json AS BLOB))) FROM source_evidence').fetchone()
    store.close()
    begin=time.perf_counter_ns();store=SourceEvidenceStore(path,original.binding,original.profile)
    cold_us=(time.perf_counter_ns()-begin)//1000
    sql={name:store._conn.execute('PRAGMA '+name).fetchone()[0]
        for name in ('page_size','page_count','freelist_count','journal_mode','integrity_check')}
    store.close()
    result={'schema':'MEME_LIVE_T010_SOURCE_HISTORY_BOUNDARY_V1',
        'scope':'ISOLATED_ORIGINAL_ACTUAL_START_SHAPE_SYNTHETIC_LATER_CUTS',
        'source':{'path':str(source),'sha256':hashlib.sha256(source.read_bytes()).hexdigest()},
        'observations':rows,'payload_bytes':bytes_,'initial_progress_bytes':len(canonical_json(original.progress).encode()),
        'append_us':append_us,'cold_original_source_replay_us':cold_us,
        'sqlite':sql,'files':physical(path),'rss_after_bytes':windows_working_set_bytes(),
        'verified_rss_quota':quota,'source_files':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (ROOT/'src/live/source_health_v0_1.py',ROOT/'src/live/evidence_store_v0_1.py')},
        'public_qualification_claimed':False,'full_physical_coverage_claimed':False}
    (output/'measurement.json').write_text(json.dumps(result,indent=2,sort_keys=True))
    print(json.dumps(result))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--source',required=True,type=Path);p.add_argument('--output',required=True,type=Path)
    p.add_argument('--observations',type=int,default=627);a=p.parse_args();main(a.source,a.output,a.observations)
