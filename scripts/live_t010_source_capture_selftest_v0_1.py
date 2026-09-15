"""Original read-only source adapter with finite T010 capture reservations."""
import hashlib
import json
import sys
import tempfile
import copy
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_source_health_selftest_v0_1 as fixtures
from live import source_health_v0_1 as source
from live import t010_resource_envelope_v0_1 as resources
from live_t010_resource_envelope_selftest_v0_1 import fixture

checks={}
def check(name,value):
    checks[name]=bool(value);assert value,name

def main():
    with tempfile.TemporaryDirectory(prefix='t010-source-capture-') as tmp:
        root=Path(tmp);path=root/'raw.sqlite';binding=fixtures.fixture(path)
        adapter=source.CollectorSourceAdapter(path);profile=source.SourceProfile()
        args={'observed_at_utc':fixtures.at(12),'requested_cut_utc':fixtures.at(9)}
        old=adapter.capture(binding,profile,**args)
        before=hashlib.sha256(path.read_bytes()).hexdigest()
        reservations=[];parsed=[];original=source._fact
        def reserve(counts):
            check('reservation_precedes_any_payload_parse',not parsed)
            reservations.append(counts);return True
        def parse(table,row):
            parsed.append(table);return original(table,row)
        with patch.object(source,'_fact',side_effect=parse):
            current=adapter.capture(binding,profile,**args,max_raw_bytes=1024*1024,reserve_rows=reserve)
        check('bounded_capture_exact_original_snapshot',current==old)
        check('every_table_reserved',reservations==[dict(zip(source.TABLES,(2,3,2,0)))])
        check('source_readonly_hash_unchanged',hashlib.sha256(path.read_bytes()).hexdigest()==before)
        with patch.object(source,'_fact',side_effect=AssertionError('payload must not materialize')):
            denied=adapter.capture(binding,profile,**args,max_raw_bytes=1,reserve_rows=lambda _:True)
        check('raw_bytes_rejected_before_materialization',denied.problems==('SOURCE_CAPTURE_RAW_BYTE_BUDGET_EXHAUSTED',))
        with patch.object(source,'_fact',side_effect=AssertionError('payload must not materialize')):
            denied=adapter.capture(binding,profile,**args,max_raw_bytes=1024*1024,reserve_rows=lambda _:False)
        check('cumulative_refusal_before_materialization',denied.problems==('SOURCE_CAPTURE_CUMULATIVE_ROW_BUDGET_EXHAUSTED',))
        inputs,accepted,extension=fixture();envelope={'derivation':resources.derive(inputs,accepted,extension)}
        verdict=source.evaluate_source(binding,profile,current)
        encoded=len(source.canonical_json(verdict).encode())
        progress_size=len(source.canonical_json(verdict.progress).encode())
        bounded=copy.deepcopy(envelope);bounded['derivation']['source_record_bytes']=encoded
        record_gate=resources.ResourceGate(root/'exact-record.json',bounded,'qualification',create=True)
        check('exact_source_record_byte_boundary',record_gate.reserve_source_verdict(verdict)
            and record_gate.snapshot()['source_payload_bytes_reserved']==encoded)
        check('source_record_crash_reservation_not_refunded',resources.ResourceGate(record_gate.path,bounded,'qualification')
            .snapshot()['source_records_reserved']==1)
        bounded=copy.deepcopy(envelope);bounded['derivation']['source_record_bytes']=encoded-1
        record_gate=resources.ResourceGate(root/'oversized-record.json',bounded,'qualification',create=True)
        check('whole_verdict_one_byte_over_denied',not record_gate.reserve_source_verdict(verdict)
            and record_gate.snapshot()['source_records_reserved']==0)
        stop=record_gate.snapshot()['capacity_stop']
        check('capacity_metadata_is_not_truncated_verdict',stop['is_source_verdict'] is False
            and stop['verdict_bytes_at_least']>=encoded and stop['observed_disposition']==verdict.disposition
            and stop['previous_verdict_digest']==verdict.snapshot.previous_verdict_digest)
        bounded=copy.deepcopy(envelope);bounded['derivation']['source_record_bytes']=progress_size-1
        record_gate=resources.ResourceGate(root/'oversized-progress.json',bounded,'qualification',create=True)
        original_json=source.canonical_json
        def no_full_json(value):
            assert type(value) not in (source.SourceVerdict,source.SourceProgress)
            return original_json(value)
        with patch.object(source,'canonical_json',side_effect=no_full_json):
            check('incremental_progress_guard_never_materializes_full_json',not record_gate.reserve_source_verdict(verdict))
        stop=record_gate.snapshot()['capacity_stop']
        check('progress_denial_does_not_encode_whole_verdict',stop['progress_bytes_at_least']>progress_size-1
            and stop['verdict_bytes_at_least'] is None)
        for table in source.TABLES:
            gate=resources.ResourceGate(root/(table+'.json'),envelope,'host',create=True)
            counts={name:10000 if name==table else 0 for name in source.TABLES}
            check(table+'_exact_total_allowed',gate.reserve_capture(counts))
            reopened=resources.ResourceGate(gate.path,envelope,'host')
            check(table+'_crash_does_not_refund',reopened.snapshot()['capture_rows_reserved']==counts)
            extra={name:int(name==table) for name in source.TABLES}
            check(table+'_one_extra_durably_held',not reopened.reserve_capture(extra)
                and resources.ResourceGate(gate.path,envelope,'host').snapshot()['held_reason']=='SOURCE_CAPTURE_WINDOW_EXHAUSTED')
            check(table+'_refusal_does_not_partially_charge',reopened.snapshot()['capture_rows_reserved']==counts)
        # Oversized fields may be malformed too. Length inspection must reject
        # them before decoding, including repeated original anchor witnesses.
        fixtures.change(path,'UPDATE collector_events SET details_json=? WHERE id=1',('x'*(1024*1024+1),))
        with patch.object(source,'_fact',side_effect=AssertionError('oversized raw field parsed')):
            denied=adapter.capture(binding,profile,**args,max_raw_bytes=1024*1024,reserve_rows=lambda _:True)
        check('oversized_original_anchor_pre_read_guard',denied.problems==('SOURCE_CAPTURE_RAW_BYTE_BUDGET_EXHAUSTED',))
        # No guard is silently installed on the accepted non-T010 API.
        check('original_default_capture_unchanged',adapter.capture(binding,profile,**args)==old)
    print(json.dumps({'scope':'ISOLATED_SYNTHETIC_READONLY_SOURCE','checks':len(checks),
        'failed':[key for key,value in checks.items() if not value]}))

if __name__=='__main__':main()
