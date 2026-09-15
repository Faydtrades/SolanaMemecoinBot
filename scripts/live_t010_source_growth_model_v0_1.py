"""Closed conservative source JSON bounds; not a physical/timing qualification."""
import argparse
import ast
import hashlib
import json
import re
import sys
from dataclasses import asdict, replace
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live import source_health_v0_1 as source
from live_t010_resource_envelope_selftest_v0_1 import fixture
from live.t010_resource_envelope_v0_1 import derive

def main(path,output):
    assert not output.exists()
    value=source.verdict_from_json(json.dumps(json.loads(path.read_text())['initial_verdict']))
    inputs,accepted,extension=fixture();spec=derive(inputs,accepted,extension)
    size=lambda value:len(source.canonical_json(value).encode())
    high=2**63-1;at='9999-12-31T23:59:59.999999+00:00'
    facts={
        'pump_events':source.PumpFact(high,'x'*256,'x'*256,high,max(source.SOURCE_MARKERS,key=len),at),
        'collector_events':max((source.ControlFact(high,'x'*256,at),
            source.ControlFact(high,'DATA_GAP_CLOSED',at,at,at)),key=size),
        'websocket_observations':source.ReceiptFact(high,'x'*256,high,at,1),
        'gap_jobs_v034':source.GapFact(high,'f'*64,at,at,'x'*256,high,high,high,high)}
    per_row={table:size(fact) for table,fact in facts.items()}
    # Every original reason comes from a finite local literal or the fixed
    # integrity set. Include all uppercase literals, a conservative superset.
    tree=ast.parse((ROOT/'src/live/source_health_v0_1.py').read_text())
    reasons=sorted({node.value for node in ast.walk(tree) if isinstance(node,ast.Constant)
        and isinstance(node.value,str) and re.fullmatch('[A-Z][A-Z0-9_]{0,255}',node.value)})
    empty=asdict(value)
    for group,names in (('snapshot',('pump_rows','controls','receipts','gaps')),
            ('progress',('boundaries','gaps','control_gaps'))):
        for name in names:empty[group][name]=[]
    # Exact binding/scalar shape, with maximal fixed-length UTCs and reason set.
    for group in ('snapshot','progress'):
        for key,part in empty[group].items():
            if key.endswith('_utc'):empty[group][key]=at
        for cursor in empty[group]['cursors']:cursor['rowid']=high
    empty['progress']['connection_state']='UNOBSERVED'
    empty['snapshot']['problems']=reasons;empty['progress']['integrity_reasons']=sorted(source.INTEGRITY_REASONS)
    empty['reasons']=reasons;empty['disposition']='REGRESSION'
    empty['covered_from_utc']=empty['covered_through_utc']=at
    scaffold=len(json.dumps(empty,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode())
    # Per-table initial capture and later qualification captures share 10000;
    # the host contributes a separate 10000. Initial history is still charged
    # below as well, conservatively, rather than assuming it was absent.
    counts={table:spec['qualification']['max_source_rows']+spec['host']['max_source_rows'] for table in source.TABLES}
    boundary_max=max(size(source.BoundaryFact(at,reason)) for reason in reasons)
    old_progress=sum(sum(size(item)+1 for item in getattr(value.progress,name))
        for name in ('boundaries','gaps','control_gaps'))
    progress=old_progress+counts['collector_events']*max(boundary_max+1,per_row['collector_events']+1)+counts['gap_jobs_v034']*(per_row['gap_jobs_v034']+1)
    snapshot=sum(spec['qualification']['max_source_rows']*(length+1) for length in per_row.values())
    observations=spec['cumulative']['source_observations']
    history=size(value)+observations*(scaffold+progress)+sum(counts[table]*(length+1) for table,length in per_row.items())
    result={'scope':'CONSERVATIVE_ORIGINAL_SOURCE_JSON_COUNT_BOUND_NOT_PHYSICAL_QUALIFICATION',
        'source':{'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()},
        'source_code_sha256':hashlib.sha256((ROOT/'src/live/source_health_v0_1.py').read_bytes()).hexdigest(),
        'source_initial_verdict_bytes':size(value),'source_initial_progress_bytes':size(value.progress),
        'capture_raw_bytes_per_unit':spec['source_unit_raw_bytes'],'captured_rows_total_per_table':counts,
        'typed_fact_canonical_max_bytes':per_row,'source_observations':observations,
        'control_boundary_or_control_gap_joint_max':counts['collector_events'],
        'uncapped_one_verdict_canonical_upper_bytes':scaffold+progress+snapshot,
        'uncapped_all_history_canonical_upper_bytes':history,
        'explicit_t010_source_record_allocation_bytes':spec['source_record_bytes'],
        'guarded_all_history_canonical_upper_bytes':spec['cumulative']['source_history_payload_bytes'],
        'conditions':['SQLite INTEGER values are signed 64-bit; malformed numeric/text facts are rejected by original constructors.',
            'Gap keys use the original adapter SHA256 encoding; UTCs normalize to 32 ASCII bytes.',
            'Each control adds at most one boundary or one control gap; both maxima are not summed.',
            'Counts are conservative independent maxima; raw-byte guards reduce jointly reachable combinations.',
            'The first non-HEALTHY cut is retained then durably held, including crash before callback.',
            'New gap/control-gap rows entirely before coverage origin may remain HEALTHY and still grow arrays.'],
        'unresolved':['Fresh source shape binding to retained physical capacity evidence.',
            'Evolving healthy historical arrays require boundary measurement; repeated-empty-cut evidence alone is insufficient.',
            'SQLite main/WAL/SHM/freelist and maximum transaction/recovery reservation.',
            'Joint retained allocation, hard RSS paging, four-reader public buffers and startup/protective timing.'],
        'public_qualification_claimed':False,'physical_upper_bound_claimed':False}
    output.write_text(json.dumps(result,indent=2,sort_keys=True));print(json.dumps(result))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();main(args.source,args.output)
