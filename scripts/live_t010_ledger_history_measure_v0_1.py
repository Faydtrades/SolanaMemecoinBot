"""Isolated original Ledger denial history with retained source-shaped evidence.

This is an original API capacity probe with deliberately mismatched source
binding, not a public qualification or a reachable Runtime campaign claim.
"""
import argparse
import hashlib
import json
import sys
import time
from dataclasses import replace
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_runtime_owned_dry_selftest_v0_1 as owned
from live.source_health_v0_1 import verdict_from_json,ZERO_DIGEST
from live.evidence_store_v0_1 import SourceEvidenceStore
from live.t010_resource_envelope_v0_1 import enforce_current_process_rss
from live.ledger_repository_v0_1 import LedgerRepository
from live.ledger_actions_v0_1 import FirstPullbackStrategyV02
from live_t010_resource_boundary_measure_v0_1 import physical
from live.runtime_dry_public_facts_v0_1 import windows_working_set_bytes
from solders.pubkey import Pubkey

def main(source,output,roots):
    output.mkdir(parents=True,exist_ok=False)
    actual=verdict_from_json(json.dumps(json.loads(source.read_text())['initial_verdict']))
    quota=enforce_current_process_rss(96*1024*1024)
    evidence=SourceEvidenceStore(output/'capacity-source.sqlite',actual.binding,actual.profile)
    evidence.append(actual,expected_previous_digest=ZERO_DIGEST)
    f=owned.fixture(output,'ledger')
    base=f.item;stable=FirstPullbackStrategyV02._stable_id
    begin=time.perf_counter_ns()
    try:
        for ordinal in range(roots):
            if ordinal:
                mint=str(Pubkey.from_bytes(hashlib.sha256(('capacity-'+str(ordinal)).encode()).digest()))
                started=base.run_started_at_us+ordinal
                run=stable('fp1run',mint,started,base.strategy_version,base.parameter_set_id)
                event=base.source_event_key+':capacity'+str(ordinal)
                signal=stable('fp1sig',run,event,base.parameter_set_id,base.strategy_version)
                item=replace(base,mint=mint,run_started_at_us=started,candidate_run_id=run,
                    source_event_key=event,candidate_signal_id=signal)
                root=f.repo.receive_candidate(item,fence=f.repo.write_fence())
                f.root,f.item=root,item
            support=owned.a3.wallet(f)
            receipt=f.repo.admit_authority_entry(owned.a3.EntryRequest(f.root,'PUMP',owned.sf.TOKEN_PROGRAM_ID,
                f.policy.selected_track),owned.a3.clock(f.repo),evidence,support,
                command_id='capacity-'+str(ordinal),fence=f.repo.write_fence())
            assert not receipt.accepted
        append_us=(time.perf_counter_ns()-begin)//1000
        domain,path=f.domain,f.path
        owned.fixtures.close(f)
        begin=time.perf_counter_ns();repo=LedgerRepository.reopen(path,domain)
        cold_us=(time.perf_counter_ns()-begin)//1000
        tables={row[0]:repo._conn.execute('SELECT count(*) FROM "'+row[0]+'"').fetchone()[0]
            for row in repo._conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        sql={name:repo._conn.execute('PRAGMA '+name).fetchone()[0]
            for name in ('page_size','page_count','freelist_count','journal_mode','integrity_check')}
        result={'scope':'ISOLATED_ORIGINAL_LEDGER_API_DENIAL_CAPACITY_NOT_RUNTIME_CAMPAIGN',
            'source':{'path':str(source),'sha256':hashlib.sha256(source.read_bytes()).hexdigest()},
            'roots':roots,'append_us':append_us,'cold_original_ledger_us':cold_us,
            'tables':tables,'sqlite':sql,'files':physical(path),'quota':quota,
            'rss_after_bytes':windows_working_set_bytes(),'public_qualification_claimed':False,
            'full_physical_coverage_claimed':False}
        repo.close();(output/'measurement.json').write_text(json.dumps(result,sort_keys=True,indent=2))
        print(json.dumps({k:v for k,v in result.items() if k!='tables'}))
    finally:
        evidence.close()
        if f.started is not None:owned.fixtures.close(f)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--source',required=True,type=Path);p.add_argument('--output',required=True,type=Path)
    p.add_argument('--roots',type=int,default=128);a=p.parse_args();main(a.source,a.output,a.roots)
