"""Retained incident-history boundary against original owned DRY Runtime.

Only synthetic disposable stores. Historical receipt digests are constructed
from the original typed active/recovery evidence pairs and bulk-loaded for
physical boundary coverage, not represented as an executed public campaign.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import sqlite3
import sys
import time
from dataclasses import asdict
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_runtime_owned_dry_selftest_v0_1 as owned
from live_t010_resource_envelope_selftest_v0_1 import fixture
from live_t010_resource_boundary_measure_v0_1 import physical
from live.t010_resource_envelope_v0_1 import derive,enforce_current_process_rss
from live.operations_degradation_v0_1 import ConditionEvidence,ConditionState,CONDITIONS
from live.runtime_dry_public_facts_v0_1 import windows_working_set_bytes
from phase5.shadow_domain_v0_1 import content_fingerprint


def main(output, operational=False):
    output.mkdir(parents=True,exist_ok=False)
    spec=derive(*fixture())
    quota=enforce_current_process_rss(spec['resource_limits']['HOST_RSS_BYTES'])
    f=owned.fixture(output,'boundary')
    try:
        # At most one state transition per resource/clock/source condition per
        # monitor observation. Alternate original active/recovery pairs; retain
        # every digest, and leave a recovered last state that the original
        # journal validates on cold opening and all subsequent reads.
        observations=spec['cumulative']['runtime_monitor_observations']
        subjects=[('RESOURCE_EXCEEDED',content_fingerprint(('BOUNDARY',m)))
            for m in sorted(spec['resource_limits'])]
        subjects += [('CLOCK_UNPROVEN',content_fingerprint('BOUNDARY_CLOCK')),
            ('SOURCE_TRUTH_UNAVAILABLE',content_fingerprint('BOUNDARY_SOURCE'))]
        if operational:
            # T010 can exhaust durable producer state only once. The mutable
            # environmental quantities below can alternate while all immutable
            # producer counts remain within their original limits.
            from live.operations_degradation_monitor_v0_1 import _subject
            m=f.runtime._operations_degradation
            subjects=[('RESOURCE_EXCEEDED',_subject(f.domain,'RESOURCE',
                (m.configuration.policy.content_digest,metric))) for metric in
                ('HOST_DISK_RESERVE_BYTES','OLDEST_UNCONSUMED_AGE_US')]
            subjects += [('CLOCK_UNPROVEN',_subject(f.domain,'OPERATIONS_CONTROL',f.domain.binding_digest)),
                ('SOURCE_TRUTH_UNAVAILABLE',_subject(f.domain,'SOURCE',
                    (m.source_binding.source_identity,m.source_profile.fingerprint)))]
            observations=3*(spec['cumulative']['source_units']+2*spec['cumulative']['admitted_roots'])
            action=owned.admit(f)
            try:owned.execute(f,action,point='before-terminal')
            except owned.Interrupted:pass
            assert f.repo.attempt(f.repo._root_attempts(action.root_id)[0].preparation.attempt_id).recorded_stage=='EXACT_SIMULATED'
        pairs=(observations+1)//2
        monitor=f.runtime._operations_degradation
        store=monitor.store
        before=owned.NOW*1000000-2*pairs-10
        last=before
        with sqlite3.connect(store.path) as conn:
            conn.execute('BEGIN IMMEDIATE')
            for code,subject in subjects:
                for episode in range(pairs):
                    at=before+2*episode+1
                    active=ConditionEvidence(code,subject,at,content_fingerprint((code,subject,at)))
                    recovered=ConditionEvidence(code,subject,at+1,content_fingerprint((code,subject,at+1)),
                        'RECOVERED',CONDITIONS[code][1],active.content_digest)
                    conn.executemany('INSERT INTO receipts VALUES (?)',
                        ((active.content_digest,),(recovered.content_digest,)))
                row=ConditionState(code,subject,pairs,'RECOVERED',at,at+1,at+1,
                    recovered.evidence_digest,active.content_digest,1,at,at+1,recovered.reason)
                conn.execute('INSERT INTO conditions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',tuple(asdict(row).values()))
                last=max(last,at+1)
            conn.execute('UPDATE metadata SET last_us=?',(last,))
        begin=time.perf_counter_ns();store.snapshot();snapshot_us=(time.perf_counter_ns()-begin)//1000
        begin=time.perf_counter_ns();owned.restart(f);startup_us=(time.perf_counter_ns()-begin)//1000
        begin=time.perf_counter_ns();result=owned.step(f,owned.NOW+10 if operational else owned.NOW+4);step_us=(time.perf_counter_ns()-begin)//1000
        stores={}
        for path in output.glob('*.sqlite'):
            if path.name.endswith('-raw.sqlite'):continue
            with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True) as conn:
                stores[path.name]={'files':physical(path),'sqlite':{name:conn.execute('PRAGMA '+name).fetchone()[0]
                    for name in ('page_size','page_count','freelist_count','journal_mode','integrity_check')},
                    'tables':{row[0]:conn.execute('SELECT count(*) FROM "'+row[0]+'"').fetchone()[0]
                        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}}
        evidence={'schema':'MEME_LIVE_T010_JOURNAL_BOUNDARY_MEASUREMENT_V1',
            'scope':'SYNTHETIC_TYPED_JOURNAL_BOUNDARY_ORIGINAL_OWNED_DRY_RUNTIME',
            'condition_scope':'RECURRING_ENVIRONMENTAL_ONLY' if operational else 'CONSERVATIVE_ALL_METRICS',
            'derivation_digest':content_fingerprint(spec),'observations':observations,
            'receipt_count':pairs*2*len(subjects),'condition_count':len(subjects),
            'snapshot_us':snapshot_us,'original_owned_cold_startup_us':startup_us,
            'original_monitored_dry_step_us':step_us,'result_work':result.work,
            'rss_after_bytes':windows_working_set_bytes(),'verified_rss_quota':quota,
            'stores':stores,'source_files':{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
                for name in ('src/live/operations_degradation_v0_1.py',
                    'src/live/operations_degradation_monitor_v0_1.py','src/live/runtime_composition_v0_1.py')},
            'public_qualification_claimed':False,'full_physical_coverage_claimed':False}
        (output/'measurement.json').write_text(json.dumps(evidence,sort_keys=True,indent=2))
        print(json.dumps({k:v for k,v in evidence.items() if k not in ('stores','source_files')}))
    finally:owned.fixtures.close(f)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--operational',action='store_true')
    args=parser.parse_args();main(args.output,args.operational)
