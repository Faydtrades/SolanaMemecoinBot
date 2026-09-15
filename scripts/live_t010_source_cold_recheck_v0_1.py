"""Cold replay only, on an exact isolated copy of retained source evidence."""
import argparse
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live.source_health_v0_1 import verdict_from_json
from live.evidence_store_v0_1 import SourceEvidenceStore
from live.t010_resource_envelope_v0_1 import enforce_current_process_rss
from live.runtime_dry_public_facts_v0_1 import windows_working_set_bytes

def sha(path):
    digest=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):digest.update(chunk)
    return digest.hexdigest()

def main(db,initial,output,profile=False):
    output.mkdir(exist_ok=False)
    before=sha(db);target=output/'source.sqlite';shutil.copyfile(db,target)
    assert sha(target)==before
    original=verdict_from_json(json.dumps(json.loads(initial.read_text())['initial_verdict']))
    quota=enforce_current_process_rss(96*1024*1024)
    profiler=None
    if profile:
        import cProfile
        profiler=cProfile.Profile();profiler.enable()
    started=time.perf_counter_ns();store=SourceEvidenceStore(target,original.binding,original.profile)
    cold_us=(time.perf_counter_ns()-started)//1000
    if profiler is not None:
        import pstats
        profiler.disable();profiler.dump_stats(str(output/'profile.pstats'))
        with (output/'profile.txt').open('w') as stream:
            pstats.Stats(profiler,stream=stream).sort_stats('cumulative').print_stats(40)
    result={'scope':'ISOLATED_EXACT_SOURCE_DATABASE_COLD_RECHECK',
        'original_database':{'path':str(db),'sha256':before},'copied_database':str(target),
        'cold_original_source_us':cold_us,'instrumented':profile,'original_startup_cap_us':4000000,
        'source_alone_within_startup_cap':cold_us<=4000000,'quota':quota,
        'rows':store.count(),'rss_after_bytes':windows_working_set_bytes(),
        'sqlite_integrity':store._conn.execute('PRAGMA integrity_check').fetchone()[0],
        'source_files':{name:sha(ROOT/name) for name in
            ('src/live/source_health_v0_1.py','src/live/evidence_store_v0_1.py')},
        'public_qualification_claimed':False,'full_physical_coverage_claimed':False}
    store.close();assert sha(target)==before==sha(db)
    result['original_and_copy_unchanged']=True
    (output/'measurement.json').write_text(json.dumps(result,sort_keys=True,indent=2));print(json.dumps(result))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--database',type=Path,required=True)
    p.add_argument('--initial',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--profile',action='store_true')
    a=p.parse_args();main(a.database,a.initial,a.output,a.profile)
