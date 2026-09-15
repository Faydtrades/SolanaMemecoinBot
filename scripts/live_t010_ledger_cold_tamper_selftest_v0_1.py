"""Fail-closed cold reopen differential on isolated corrupted history copies."""
import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live.ledger_repository_v0_1 import LedgerRepository
from live.ledger_domain_v0_1 import LedgerDomain
from live.wallet_evidence_v0_1 import ExpectedTokenAccount
from live_t010_ledger_cold_integration_selftest_v0_1 import reference_module,result,sha

def main(database,before,output):
    output.mkdir(exist_ok=False);original_hash=sha(database)
    reference=reference_module(before).LedgerRepository
    checks={};results=[]
    for kind in ('head_revision','admission_link','noncanonical_payload','source_bool'):
        path=output/(kind+'.sqlite');shutil.copyfile(database,path)
        assert sha(path)==original_hash
        with sqlite3.connect(path) as conn:
            raw=json.loads(conn.execute('SELECT payload_json FROM ledger_domain').fetchone()[0])
            raw['expected_empty_token_accounts']=tuple(ExpectedTokenAccount(**v) for v in raw['expected_empty_token_accounts'])
            domain=LedgerDomain(**raw)
            # Corruption fixture only: retain and restore the exact original
            # immutable-history trigger SQL within the same local transaction.
            conn.execute('BEGIN IMMEDIATE')
            table=('ledger_authority_records' if kind=='noncanonical_payload' else
                'ledger_head' if kind=='head_revision' else 'ledger_authority_admissions')
            triggers=conn.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",(table,)).fetchall()
            for name,sql in triggers:conn.execute('DROP TRIGGER "'+name.replace('"','""')+'"')
            if kind=='head_revision':conn.execute('UPDATE ledger_head SET revision=revision+1')
            elif kind=='admission_link':
                conn.execute("UPDATE ledger_authority_admissions SET root_id=? WHERE rowid=(SELECT min(rowid) FROM ledger_authority_admissions)",('0'*64,))
            elif kind=='noncanonical_payload':
                conn.execute("UPDATE ledger_authority_records SET payload_json=payload_json||' ' WHERE rowid=(SELECT min(rowid) FROM ledger_authority_records)")
            else:
                rowid,payload=conn.execute('SELECT rowid,payload_json FROM ledger_authority_admissions ORDER BY rowid LIMIT 1 OFFSET 1').fetchone()
                value=json.loads(payload)
                def mutate(node):
                    if type(node) is dict:
                        if 'events_recovered' in node:node['events_recovered']=True;return True
                        return any(mutate(part) for part in node.values())
                    if type(node) is list:return any(mutate(part) for part in node)
                    return False
                assert mutate(value)
                conn.execute('UPDATE ledger_authority_admissions SET payload_json=? WHERE rowid=?',
                    (json.dumps(value,sort_keys=True,separators=(',',':')),rowid))
            for name,sql in triggers:conn.execute(sql)
        conn.close()
        corrupt_hash=sha(path)
        assert corrupt_hash!=original_hash
        wal=Path(str(path)+'-wal')
        wal_bytes=wal.stat().st_size if wal.exists() else 0
        checks[kind+'_fixture_wal_closed']=wal_bytes==0
        def reopen(cls):
            repo=cls.reopen(path,domain)
            try:return 'UNEXPECTED_OPEN'
            finally:repo.close()
        old=result(lambda:reopen(reference));after_reference=sha(path)
        new=result(lambda:reopen(LedgerRepository));after_integrated=sha(path)
        checks[kind+'_same_rejection']=old==new and old[0]=='ERROR'
        checks[kind+'_no_generation_or_history_write']=corrupt_hash==after_reference==after_integrated
        results.append({'case':kind,'reference':old,'integrated':new,'corrupt_database_sha256':corrupt_hash,
            'after_reference_sha256':after_reference,'after_integrated_sha256':after_integrated,
            'fixture_wal_bytes_after_close':wal_bytes})
        record={'scope':'ISOLATED_COLD_TAMPER_DIFFERENTIAL','checks':len(checks),'all_checks':all(checks.values()),
            'results':results,'tests':checks,'original':{'path':str(database),'sha256':original_hash},
            'original_unchanged':sha(database)==original_hash,'public_qualification_claimed':False}
        (output/'result.json').write_text(json.dumps(record,sort_keys=True,indent=2))
        assert record['all_checks'] and record['original_unchanged']
    print(json.dumps(record))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--database',required=True,type=Path)
    p.add_argument('--before',required=True,type=Path);p.add_argument('--output',required=True,type=Path)
    a=p.parse_args();main(a.database.resolve(),a.before.resolve(),a.output.resolve())
