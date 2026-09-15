"""Isolated original-producer boundary measurements; never public qualification.

Run each scenario in its own fresh interpreter. Fixtures are generated only in
the requested new output directory. No collector or live database is opened.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import sqlite3
import sys
import threading
import time
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from contextlib import closing

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live_t010_resource_envelope_selftest_v0_1 import fixture
from live_continuous_producer_selftest_v0_2 import BASE, empty_source, insert_row, open_producer
from live.continuous_producer_v0_2 import ContinuationProfileV02
from live.operations_degradation_monitor_v0_1 import _producer_cut
from live.runtime_dry_public_facts_v0_1 import windows_working_set_bytes


def physical(path):
    result={}
    for suffix in ('','-wal','-shm','-journal'):
        try:
            result[suffix]=Path(str(path)+suffix).stat().st_size
        except FileNotFoundError:
            result[suffix]=0  # SQLite may unlink a companion between samples.
    return result


def reopen_measure(output, origin, hard_rss):
    """Read-only copies of retained synthetic boundaries; no fixture regeneration."""
    output.mkdir(parents=True,exist_ok=False)
    original=json.loads((origin/'measurement.json').read_text())
    assert original['scope']=='SYNTHETIC_ORIGINAL_PRODUCER_BOUNDARY_ONLY'
    for name in ('raw.sqlite','producer.sqlite'):
        with closing(sqlite3.connect((origin/name).as_uri()+'?mode=ro',uri=True)) as source:
            with closing(sqlite3.connect(output/name)) as destination:source.backup(destination)
    quota=None
    if hard_rss:
        from live.t010_resource_envelope_v0_1 import enforce_current_process_rss
        quota=enforce_current_process_rss(fixture()[2]['limits']['HOST_RSS_BYTES'])
    path=output/'producer.sqlite';profile=ContinuationProfileV02(**original['producer_profile'])
    before=time.perf_counter_ns()
    conn,producer=open_producer(path,output/'raw.sqlite',profile=profile)
    cold_us=(time.perf_counter_ns()-before)//1000
    metrics,manifest,digest=_producer_cut(producer)
    sql={name:conn.execute('PRAGMA '+name).fetchone()[0]
        for name in ('page_size','page_count','freelist_count','journal_mode','integrity_check')}
    conn.close()
    result={'schema':'MEME_LIVE_T010_ISOLATED_PRODUCER_COLD_REPLAY_MEASUREMENT_V1',
        'scope':'SYNTHETIC_ORIGINAL_PRODUCER_BOUNDARY_ONLY','origin':str(origin),
        'origin_sha256':hashlib.sha256((origin/'measurement.json').read_bytes()).hexdigest(),
        'source_files':{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
            ('src/live/continuous_producer_v0_1.py','src/live/continuous_producer_v0_2.py')},
        'scenario':original['scenario'],'producer_profile':asdict(profile),'final_metrics':metrics,
        'cold_producer_startup_us':cold_us,'rss_after_bytes':windows_working_set_bytes(),
        'verified_rss_quota':quota,'sqlite':sql,'files':physical(path),'checkpoint_digest':digest,
        'original_metrics_equal':metrics==original['final_metrics'],
        'public_qualification_claimed':False,'full_runtime_coverage_claimed':False}
    (output/'measurement.json').write_text(json.dumps(result,sort_keys=True,indent=2))
    print(json.dumps(result,sort_keys=True))


def measure(output,scenario,hard_rss):
    output.mkdir(parents=True,exist_ok=False)
    quota=None
    if hard_rss:
        from live.t010_resource_envelope_v0_1 import enforce_current_process_rss
        quota=enforce_current_process_rss(fixture()[2]['limits']['HOST_RSS_BYTES'])
    raw,path=output/'raw.sqlite',output/'producer.sqlite'
    empty_source(raw)
    p=ContinuationProfileV02(**fixture()[1]['constructor_configuration']['continuation_profile'])
    if scenario=='aggregate':
        mints,events=32,32
    elif scenario=='four-hot':
        mints,events=4,256
    elif scenario=='two-hot':
        mints,events=2,256
    elif scenario=='hottest':
        mints,events=1,512
    elif scenario=='active':
        mints,events=64,1
    elif scenario in ('raw-bytes','raw-bytes-bounded'):
        mints,events=1,32
    else:
        raise ValueError('scenario')
    with sqlite3.connect(raw) as conn:
        row=0
        for event in range(events):
            for mint in range(mints):
                row+=1
                insert_row(conn,row,('M'+str(mint)+'Z').ljust(44,'1'),'LAUNCH' if event==0 else 'BUY',observed_offset=1)
        if scenario in ('raw-bytes','raw-bytes-bounded'):
            # Each row is within original1MiB raw-record cap. Its invalid source
            # label is rejected by original normalization, after bounded capture.
            conn.execute('UPDATE pump_events SET source_decoded_file=?',('X'*(p.record_bytes-1024),))
    peaks={'rss_bytes':windows_working_set_bytes(),'files':physical(path)}
    stop=threading.Event()
    def sample():
        while not stop.wait(.002):
            peaks['rss_bytes']=max(peaks['rss_bytes'],windows_working_set_bytes())
            for suffix,size in physical(path).items():
                peaks['files'][suffix]=max(peaks['files'][suffix],size)
    sampler=threading.Thread(target=sample,daemon=True);sampler.start()
    conn=producer=None
    records=[];failure=None
    begin=time.perf_counter_ns()
    try:
        conn,producer=open_producer(path,raw,profile=p)
        initial_startup=(time.perf_counter_ns()-begin)//1000
        while producer.durable_p1_rowid<row:
            before=time.perf_counter_ns()
            try:
                result=producer.process_next_batch(batch_size=32,
                    max_raw_bytes=p.record_bytes if scenario=='raw-bytes-bounded' else None)
            except (ValueError,RuntimeError,sqlite3.DatabaseError) as exc:
                failure=type(exc).__name__+':'+str(exc)
                break
            metrics,manifest,_=_producer_cut(producer)
            records.append({'cursor':producer.durable_p1_rowid,'metrics':metrics,
                'producer_step_us':(time.perf_counter_ns()-before)//1000,
                'rss_bytes':windows_working_set_bytes()})
            if not result.raw_rows_fetched:
                break
        if failure is None:
            conn.close();conn=None
            before=time.perf_counter_ns()
            conn,producer=open_producer(path,raw,profile=p)
            cold_us=(time.perf_counter_ns()-before)//1000
            final=_producer_cut(producer)[0]
            sqlite_state={name:conn.execute('PRAGMA '+name).fetchone()[0]
                for name in ('page_size','page_count','freelist_count','journal_mode','integrity_check')}
        else:
            cold_us=None;final=records[-1]['metrics'] if records else {};sqlite_state={}
    finally:
        if conn is not None:conn.close()
        stop.set();sampler.join()
    source_files=('src/live/t010_resource_envelope_v0_1.py','src/live/continuous_producer_v0_2.py',
        'src/live/operations_degradation_monitor_v0_1.py')
    result={'schema':'MEME_LIVE_T010_ISOLATED_PRODUCER_BOUNDARY_MEASUREMENT_V1',
        'scope':'SYNTHETIC_ORIGINAL_PRODUCER_BOUNDARY_ONLY','scenario':scenario,
        'source_files':{file:hashlib.sha256((ROOT/file).read_bytes()).hexdigest() for file in source_files},
        'producer_profile':asdict(p),'source_rows':row,'observations':records,'final_metrics':final,
        'verified_rss_quota':quota,
        'initial_producer_startup_us':initial_startup,'cold_producer_startup_us':cold_us,
        'sqlite':sqlite_state,'peaks':peaks,'failure':failure,
        'public_qualification_claimed':False,'full_runtime_coverage_claimed':False}
    (output/'measurement.json').write_text(json.dumps(result,sort_keys=True,indent=2))
    print(json.dumps({key:value for key,value in result.items() if key not in ('observations','source_files')},sort_keys=True))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--scenario',required=True,choices=('aggregate','hottest','four-hot','two-hot','active','raw-bytes','raw-bytes-bounded'))
    parser.add_argument('--hard-rss',action='store_true')
    parser.add_argument('--reopen-from',type=Path)
    args=parser.parse_args()
    if args.reopen_from:reopen_measure(args.output,args.reopen_from,args.hard_rss)
    else:measure(args.output,args.scenario,args.hard_rss)
