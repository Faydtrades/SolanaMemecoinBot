"""Read-only prototype proof; does not alter any original Ledger codec."""
import argparse
import hashlib
import json
import sqlite3
import sys
import time
from dataclasses import asdict, fields, is_dataclass
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live.source_health_v0_1 import _SourceReplayDecoder
from live.authority_controls_v0_1 import authority_receipt_from_record
from live.authority_admission_v0_1 import admission_receipt_from_record
from phase5.shadow_domain_v0_1 import canonical_json
from live_t010_ledger_codec_equivalence_v0_1 import adversarial

def prototype(value, fragments):
    cached=fragments.get(id(value))
    if cached is not None and cached[0] is value:
        return cached[1]
    if is_dataclass(value) and not isinstance(value,type):
        return '{'+','.join(canonical_json(field.name)+':'+prototype(getattr(value,field.name),fragments)
            for field in sorted(fields(value),key=lambda field:field.name))+'}'
    if type(value) in (list,tuple):
        return '['+','.join(prototype(item,fragments) for item in value)+']'
    if type(value) is dict and all(type(key) is str for key in value):
        return '{'+','.join(canonical_json(key)+':'+prototype(value[key],fragments) for key in sorted(value))+'}'
    return canonical_json(value)

def main(path,output):
    assert not output.exists()
    before=hashlib.sha256(path.read_bytes()).hexdigest()
    decoder=_SourceReplayDecoder();counts=[];old_ns=new_ns=0
    with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True) as conn:
        conn.execute('PRAGMA query_only=ON');conn.execute('BEGIN')
        for table,reader in (('ledger_authority_records',authority_receipt_from_record),
                ('ledger_authority_admissions',admission_receipt_from_record)):
            count=0
            for digest,payload in conn.execute('SELECT receipt_digest,payload_json FROM '+table):
                value=reader(json.loads(payload),_source_decoder=decoder)
                fragments={id(part[1]):(part[1],canonical_json(part[2])) for part in decoder.parts.values()}
                start=time.perf_counter_ns();original=canonical_json(asdict(value));old_ns+=time.perf_counter_ns()-start
                start=time.perf_counter_ns();candidate=prototype(value,fragments);new_ns+=time.perf_counter_ns()-start
                assert candidate==original==payload
                assert hashlib.sha256(candidate.encode()).hexdigest()==digest
                count+=1
            counts.append({'table':table,'count':count})
    assert hashlib.sha256(path.read_bytes()).hexdigest()==before
    result={'scope':'READ_ONLY_PROTOTYPE_CANONICAL_ARRAY_FRAGMENT_EQUIVALENCE',
        'ledger':{'path':str(path),'sha256':before},'tables':counts,
        'original_serialization_us':old_ns//1000,'fragment_serialization_us':new_ns//1000,
        'all_original_payload_bytes_and_hashes_equal':True,'prior_adversarial_cases':adversarial(),
        'writes_or_original_codec_changes':False,'implementation_approval_claimed':False}
    output.write_text(json.dumps(result,indent=2,sort_keys=True));print(json.dumps(result))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--ledger',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    main(args.ledger.resolve(),args.output.resolve())
