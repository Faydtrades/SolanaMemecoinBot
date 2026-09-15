"""Full original Ledger cold replay on an exact isolated retained-history copy."""
import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
import time
from dataclasses import fields,is_dataclass
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live.ledger_repository_v0_1 import LedgerRepository
from live.ledger_domain_v0_1 import LedgerDomain
from live.wallet_evidence_v0_1 import ExpectedTokenAccount
from live.t010_resource_envelope_v0_1 import enforce_current_process_rss
from live.runtime_dry_public_facts_v0_1 import windows_working_set_bytes
from live_t010_ledger_cold_integration_selftest_v0_1 import sha,reference_module

def encode(value):
    def default(item):
        if is_dataclass(item):return {f.name:getattr(item,f.name) for f in fields(item)}
        if type(item) is bytes:return {'original_bytes_hex':item.hex()}
        raise TypeError(type(item).__name__)
    return json.dumps(value,default=default,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)

def main(database,output,before=None,profile=False):
    output.mkdir(exist_ok=False)
    original_hash=sha(database);target=output/'ledger.sqlite';shutil.copyfile(database,target)
    assert sha(target)==original_hash
    with sqlite3.connect(target.as_uri()+'?mode=ro',uri=True) as conn:
        raw=json.loads(conn.execute('SELECT payload_json FROM ledger_domain').fetchone()[0])
    raw['expected_empty_token_accounts']=tuple(ExpectedTokenAccount(**v) for v in raw['expected_empty_token_accounts'])
    domain=LedgerDomain(**raw)
    cls=LedgerRepository if before is None else reference_module(before).LedgerRepository
    # Instrumentation only counts calls while delegating every original check.
    class Counted(cls):
        replay_count=0
        def _verify_history(self):
            self.replay_count+=1
            return super()._verify_history()
    quota=enforce_current_process_rss(96*1024*1024)
    profiler=None
    if profile:
        import cProfile
        profiler=cProfile.Profile();profiler.enable()
    start=time.perf_counter_ns();repo=Counted.reopen(target,domain)
    cold_us=(time.perf_counter_ns()-start)//1000
    if profiler is not None:
        import pstats
        profiler.disable();profiler.dump_stats(str(output/'profile.pstats'))
        with (output/'profile.txt').open('w') as stream:
            pstats.Stats(profiler,stream=stream).sort_stats('cumulative').print_stats(45)
    rss=windows_working_set_bytes()
    assert repo.replay_count==2
    semantic={name:hashlib.sha256(encode(getattr(repo,name)).encode()).hexdigest()
        for name in ('_chain_facts','_custody','_applications','_comparisons','_authority')}
    semantic['consumer_snapshot']=hashlib.sha256(encode(repo.consumer_snapshot()).encode()).hexdigest()
    semantic['authority_snapshot']=hashlib.sha256(encode(repo.authority_snapshot()).encode()).hexdigest()
    rows={}
    for table, in repo._conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall():
        h=hashlib.sha256();count=0
        for row in repo._conn.execute('SELECT * FROM "'+table+'" ORDER BY rowid'):
            h.update((encode(row)+'\n').encode());count+=1
        rows[table]={'rows':count,'sha256':h.hexdigest()}
    integrity=repo._conn.execute('PRAGMA integrity_check').fetchone()[0]
    record={'scope':'EXACT_RETAINED_ORIGINAL_API_DENIAL_HISTORY_NOT_PUBLIC_CAMPAIGN',
        'mode':'INTEGRATED' if before is None else 'PRE_PATCH_REFERENCE',
        'original':{'path':str(database),'sha256':original_hash},'copy':str(target),
        'cold_us':cold_us,'instrumented':profile,'quota':quota,'rss_after_open_bytes':rss,
        'original_startup_limit_us':4000000,'ledger_alone_within_startup_limit':cold_us<=4000000,
        'full_replay_count':repo.replay_count,'semantic_state':semantic,'all_tables':rows,
        'integrity':integrity,'source_hashes':{name:sha(ROOT/name) for name in
            ('src/live/source_health_v0_1.py','src/live/ledger_repository_v0_1.py')},
        'reference':None if before is None else {'path':str(before),'sha256':sha(before)},
        'public_qualification_claimed':False,'full_resource_coverage_claimed':False}
    repo.close();assert sha(database)==original_hash
    record['original_unchanged']=True
    (output/'measurement.json').write_text(json.dumps(record,sort_keys=True,indent=2))
    print(json.dumps({k:v for k,v in record.items() if k not in ('all_tables','semantic_state')}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--database',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--before',type=Path)
    p.add_argument('--profile',action='store_true')
    a=p.parse_args();main(a.database.resolve(),a.output.resolve(),a.before,a.profile)
