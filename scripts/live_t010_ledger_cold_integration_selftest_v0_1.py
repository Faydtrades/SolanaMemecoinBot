"""Read-only differential of the explicitly authorized two cold row readers."""
import argparse
import copy
import hashlib
import importlib.machinery
import importlib.util
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live.ledger_repository_v0_1 import LedgerRepository
from live.source_health_v0_1 import _SourceReplayDecoder
from phase5.shadow_domain_v0_1 import canonical_json

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for part in iter(lambda:stream.read(1048576),b''):h.update(part)
    return h.hexdigest()

def reference_module(path):
    name='live._authorized_cold_reference'
    loader=importlib.machinery.SourceFileLoader(name,str(path))
    spec=importlib.util.spec_from_loader(name,loader)
    module=importlib.util.module_from_spec(spec);sys.modules[name]=module
    loader.exec_module(module);return module

def result(call):
    try:return ('VALUE',call())
    except Exception as exc:return ('ERROR',type(exc).__name__,str(exc))

def main(database,before,output):
    assert not output.exists()
    original_hash=sha(database);reference=reference_module(before)
    original=object.__new__(reference.LedgerRepository)
    current=object.__new__(LedgerRepository);checks={};counts={}
    def check(name,value):checks[name]=bool(value);assert value,name
    @dataclass(frozen=True)
    class Record:
        value:object
    decoder=_SourceReplayDecoder(canonical_fragments=True)
    for n,item in enumerate(('雪 / café','quote" slash\\ newline\n NUL\0',0,-1,2**64-1,
            10**1024,None,True,False,(1,'x',None),{'k':(1,2),'nested':{'u':'雪'}},
            float('nan'),float('inf'),b'bytes',{'set'},(Record('nested'),))):
        value=Record(item)
        # Error classes are the original codec contract; unsupported values
        # are never accepted merely because their exception wording differs.
        left=result(lambda:canonical_json(asdict(value)))
        right=result(lambda:decoder.canonical_record(value))
        check('canonical_adversarial_'+str(n),left[:2]==right[:2])
    with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True) as conn:
        conn.execute('PRAGMA query_only=ON');conn.execute('BEGIN')
        for table,method in (('ledger_authority_records','_authority_receipt_row'),
                ('ledger_authority_admissions','_read_authority_admission_row')):
            count=0;sample=None;decoder=_SourceReplayDecoder(canonical_fragments=True)
            for row in conn.execute('SELECT * FROM '+table+' ORDER BY commit_seq'):
                old=getattr(original,method)(row)
                new=getattr(current,method)(row,_source_decoder=decoder)
                assert old==new
                encoded=decoder.canonical_record(new)
                assert encoded==canonical_json(asdict(old))==row[-1]
                assert hashlib.sha256(encoded.encode()).hexdigest()==old.content_digest==new.content_digest==row[-2]
                count+=1
                if '"events_recovered"' in row[-1]:sample=row
            counts[table]=count;check(table+'_all_bytes_digests_values',count>0)
            if sample is None:continue
            variants=[]
            for offset,replacement in ((0,sample[0]+1),(-2,'0'*64),(-1,sample[-1]+' ')):
                row=list(sample);row[offset]=replacement;variants.append(tuple(row))
            raw=json.loads(sample[-1])
            def numeric_fact(value):
                if isinstance(value,dict):
                    if 'events_recovered' in value:return value
                    for part in value.values():
                        found=numeric_fact(part)
                        if found is not None:return found
                if isinstance(value,list):
                    for part in value:
                        found=numeric_fact(part)
                        if found is not None:return found
            for replacement in (True,1.0,-1,'雪'):
                changed=copy.deepcopy(raw);numeric_fact(changed)['events_recovered']=replacement
                variants.append(tuple(sample[:-1])+(json.dumps(changed,sort_keys=True,separators=(',',':')),))
            for n,row in enumerate(variants):
                decoder=_SourceReplayDecoder(canonical_fragments=True)
                getattr(current,method)(sample,_source_decoder=decoder)
                old=result(lambda:getattr(original,method)(row))
                new=result(lambda:getattr(current,method)(row,_source_decoder=decoder))
                check(table+'_tamper_'+str(n),old==new and old[0]=='ERROR')
    check('retained_database_unchanged',sha(database)==original_hash)
    record={'scope':'AUTHORIZED_LEDGER_COLD_READ_DIFFERENTIAL_NOT_PUBLIC_CAMPAIGN',
        'checks':len(checks),'all_checks':all(checks.values()),'tables':counts,
        'database':{'path':str(database),'sha256':original_hash},
        'reference':{'path':str(before),'sha256':sha(before)},
        'source_hashes':{name:sha(ROOT/name) for name in
            ('src/live/source_health_v0_1.py','src/live/ledger_repository_v0_1.py')},
        'public_qualification_claimed':False,'tests':checks}
    output.write_text(json.dumps(record,indent=2,sort_keys=True));print(json.dumps(record))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--database',type=Path,required=True)
    p.add_argument('--before',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();main(a.database.resolve(),a.before.resolve(),a.output.resolve())
