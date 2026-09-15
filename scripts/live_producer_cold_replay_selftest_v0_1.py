"""LIVE-only lossless cold rehydration versus original full snapshot replay."""
from __future__ import annotations

import json
import sys
import tempfile
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live_continuous_producer_selftest_v0_2 import (create_fixture,copy_fixture_rows,open_producer,
    copy_db,outputs,active,TABLES,accepted,drain)
from live.continuous_producer_v0_1 import LiveContinuousProducerV01,ProducerConflict
from live.continuous_producer_v0_2 import LiveContinuousProducerV02

CHECKS={}


def check(name,value):
    CHECKS[name]=bool(value)
    if not value:raise AssertionError(name)


def original_replay(producer):
    engine=accepted.DenominationAwareFeatureEngineV01()
    for row in producer.conn.execute('SELECT mint,event_json FROM paper_fp_binding_feature_events_v0_1 '
            'ORDER BY mint,production_p1_rowid'):
        event=accepted._quote_event_from_json(row['event_json'])
        if event.base.mint!=row['mint'] or event.base.mint not in producer.market_source._launch_by_mint:
            raise ProducerConflict('retained feature/launch registry conflict')
        engine.process(event)
    return engine


class OriginalReplayProducer(LiveContinuousProducerV02):
    def _rebuild_engine(self):
        engine=original_replay(self)
        self.metrics['replayed_events']+=sum(len(v.known_events) for v in engine._tokens.values())
        return engine


def result_or_error(call):
    try:return ('VALUE',call())
    except Exception as exc:return (type(exc).__name__,str(exc))


def fake(rows,launches):
    return SimpleNamespace(conn=SimpleNamespace(execute=lambda sql:rows),
        market_source=SimpleNamespace(_launch_by_mint=launches))


def main():
    with tempfile.TemporaryDirectory(prefix='live-cold-lossless-') as tmp:
        root=Path(tmp);template=root/'template.sqlite';raw=root/'raw.sqlite'
        create_fixture(template);copy_fixture_rows(template,raw,through_rowid=13)
        conn,producer=open_producer(root/'optimized.sqlite',raw)
        drain(producer,3)
        rows=[dict(row) for row in conn.execute('SELECT mint,event_json FROM '+TABLES[2]+' ORDER BY mint,production_p1_rowid')]
        old=original_replay(producer)
        check('all_original_accumulator_fields_equal',accepted._primitive(old._tokens)==accepted._primitive(producer._rebuild_engine()._tokens))
        check('normal_engine_type_restored',type(producer._rebuild_engine()) is accepted.DenominationAwareFeatureEngineV01)
        check('normal_method_restored',producer._rebuild_engine()._build_state.__func__ is accepted.DenominationAwareFeatureEngineV01._build_state)
        snapshot=producer.checkpoint_manifest();conn.close()
        copy_db(root/'optimized.sqlite',root/'original.sqlite')
        conn,producer=open_producer(root/'optimized.sqlite',raw)
        original_conn,baseline=open_producer(root/'original.sqlite',raw,cls=OriginalReplayProducer)
        check('cold_checkpoint_bytes_unchanged',producer.checkpoint_manifest()==baseline.checkpoint_manifest()==snapshot)
        check('cold_complete_accumulators_equal',active(producer)==active(baseline))
        check('cold_metrics_preserved',producer.metrics['replayed_events']==baseline.metrics['replayed_events']==len(rows))
        # Normal processing resumes full snapshot construction on both engines.
        last=accepted._quote_event_from_json(rows[-1]['event_json'])
        next_event=replace(last,base=replace(last.base,ingest_seq=last.base.ingest_seq+1000,
            observed_at_us=last.base.observed_at_us+1000000,event_key=last.base.event_key+'-next'))
        optimized=producer._rebuild_engine();full=original_replay(producer)
        check('next_original_market_state_equal',optimized.process(next_event)==full.process(next_event))
        check('next_original_accumulator_equal',accepted._primitive(optimized._tokens)==accepted._primitive(full._tokens))
        copy_fixture_rows(template,raw,through_rowid=14)
        producer.process_next_batch(batch_size=3);baseline.process_next_batch(batch_size=3)
        check('next_candidate_and_audit_bytes_equal',outputs(producer)==outputs(baseline))
        check('next_exact_checkpoint_equal',producer.checkpoint_manifest()==baseline.checkpoint_manifest())
        check('next_retirement_and_history_equal',producer.last_profile_usage==baseline.last_profile_usage)
        conn.close();original_conn.close()
        conn,producer=open_producer(root/'optimized.sqlite',raw)
        original_conn,baseline=open_producer(root/'original.sqlite',raw,cls=OriginalReplayProducer)
        check('second_reopen_exact_state_equal',active(producer)==active(baseline)
            and producer.checkpoint_manifest()==baseline.checkpoint_manifest())
        conn.close();original_conn.close()
        empty=fake([],{});check('empty_original_engine_equal',not LiveContinuousProducerV01._rebuild_engine(empty)._tokens)
        for name,invalid,launches in (
            ('duplicate_order',[rows[0],rows[0]],{rows[0]['mint']:1}),
            ('reverse_order',list(reversed([r for r in rows if r['mint']==rows[0]['mint']])),{rows[0]['mint']:1}),
            ('missing_launch',[rows[0]],{}),
            ('wrong_mint',[dict(rows[0],mint='WRONG')],{'WRONG':1}),
            ('invalid_codec',[dict(rows[0],event_json='{}')],{rows[0]['mint']:1})):
            stub=fake(invalid,launches)
            expected=result_or_error(lambda:original_replay(stub))
            actual=result_or_error(lambda:LiveContinuousProducerV01._rebuild_engine(stub))
            check('original_error_'+name,expected==actual and actual[0]!='VALUE')
    print(json.dumps({'scope':'ISOLATED_ORIGINAL_DIFFERENTIAL','count':len(CHECKS),'checks':CHECKS},sort_keys=True))


if __name__=='__main__':main()
