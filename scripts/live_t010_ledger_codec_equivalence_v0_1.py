"""Read-only original Ledger receipt byte-equivalence evidence; no mutations."""
import argparse
import hashlib
import json
import sqlite3
import sys
from dataclasses import asdict,dataclass
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from live.authority_controls_v0_1 import authority_receipt_from_record
from live.authority_admission_v0_1 import admission_receipt_from_record
from live.source_health_v0_1 import canonical_json as immutable_json
from phase5.shadow_domain_v0_1 import canonical_json

def adversarial():
    @dataclass(frozen=True)
    class Record:
        value:object
    cases=['Unicode snow: 雪 / café','quote" slash\\ newline\n NUL\0',0,-1,2**64-1,
        10**1024,None,True,False,(1,'x',None),{'k':(1,2),'nested':{'u':'雪'}},
        float('nan'),float('inf'),b'bytes',{'set'},(Record('nested'),)]
    count=0
    for item in cases:
        value=Record(item)
        def result(call):
            try:return ('VALUE',call())
            except Exception as exc:return ('ERROR',type(exc).__name__)
        assert result(lambda:canonical_json(asdict(value)))==result(lambda:immutable_json(value))
        count+=1
    return count

def main(path,output):
    assert not output.exists()
    before=hashlib.sha256(path.read_bytes()).hexdigest()
    checked=[]
    with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True) as conn:
        conn.execute('PRAGMA query_only=ON');conn.execute('BEGIN')
        for table,reader in (('ledger_authority_records',authority_receipt_from_record),
                ('ledger_authority_admissions',admission_receipt_from_record)):
            count=0;total=0
            for sequence,retained,payload in conn.execute('SELECT commit_seq,receipt_digest,payload_json FROM '+table+' ORDER BY commit_seq'):
                value=reader(json.loads(payload));original=canonical_json(asdict(value));candidate=immutable_json(value)
                assert original==candidate
                assert hashlib.sha256(original.encode()).hexdigest()==retained==value.content_digest
                total+=len(original.encode());count+=1
            checked.append({'table':table,'rows':count,'original_canonical_bytes':total})
    assert hashlib.sha256(path.read_bytes()).hexdigest()==before
    result={'scope':'READ_ONLY_RETAINED_ORIGINAL_RECEIPT_BYTE_EQUIVALENCE_NOT_IMPLEMENTATION_APPROVAL',
        'ledger':{'path':str(path),'sha256':before},'tables':checked,'all_original_bytes_and_digests_equal':True,
        'source_files':{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
            ('src/live/source_health_v0_1.py','src/live/authority_controls_v0_1.py','src/live/authority_admission_v0_1.py',
             'src/live/ledger_repository_v0_1.py','src/phase5/shadow_domain_v0_1.py')},
        'adversarial_cases':adversarial(),'code_or_ledger_changed':False,'public_qualification_claimed':False}
    output.write_text(json.dumps(result,indent=2,sort_keys=True));print(json.dumps(result))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--ledger',required=True,type=Path);p.add_argument('--output',required=True,type=Path)
    a=p.parse_args();main(a.ledger.resolve(),a.output.resolve())
