"""Original pending-state checkpoint/parent-dispatch recovery validation; no public I/O."""
import argparse,json,shutil,sqlite3,sys,time
from contextlib import closing
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_t010_joint_hot_measure_v0_2 as copies
from live.operations_startup_v0_1 import start_runtime
from live.operations_ownership_v0_1 import OperationsStore
from live.t010_public_host_v0_1 import WorkBudget,PublicHostInputs
from types import SimpleNamespace
from dataclasses import asdict
from live.authority_controls_v0_1 import TrustedClockSample,utc_from_us
from live.ledger_actions_v0_1 import utc_microseconds
from phase5.shadow_domain_v0_1 import content_fingerprint
from live.t010_resource_envelope_v0_1 import ResourceGate,derive
from live_t010_resource_envelope_selftest_v0_1 import fixture

def run(origin,output):
    output.mkdir(parents=True,exist_ok=False)
    original=json.loads((origin/'configuration.json').read_text());config=json.loads(json.dumps(original))
    before={str(p):copies.sha(p) for p in origin.glob('*.sqlite')}
    for key,path in original['paths'].items():
        destination=output/Path(path).name
        if key!='monitor':
            with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True)) as source:
                with closing(sqlite3.connect(destination)) as target:source.backup(target)
        config['paths'][key]=str(destination)
    config['monitor']['path']=config['paths']['monitor']
    config['live_paths']=[str(output/Path(p).name) for p in original['live_paths']]
    copies.prepare_monitor_copy(original,config,origin,output)
    copies.write(output/'configuration.json',config)
    args=copies.build_configuration(config)
    control=OperationsStore(args['operations_path'],args['domain']).snapshot()
    started=start_runtime(**args,process_identity='isolated-checkpoint-pending-diagnostic',
        now_us=control['last_control_us']+1,replace_generation=control['generation'])
    runtime=started.runtime
    try:
        assert runtime._operations_degradation.store is not None
        reservations=runtime.ledger.consumer_snapshot()['reservations']
        assert len(reservations)==1
        spec=derive(*fixture());envelope=output/'envelope.json'
        copies.write(envelope,{'derivation':spec})
        shutil.copyfile(origin/'host-resources.json',output/'resource_work.json')
        gate=ResourceGate(output/'resource_work.json',{'derivation':spec},'host')
        session={'work_budget':spec['work_budget'],'resource_envelope':{'path':str(envelope),'sha256':copies.sha(envelope)}}
        budget=WorkBudget(output,session,create=True)
        assert budget.before_start()
        excluded=tuple({'root_id':item.input.root_id} for item in runtime.ledger._custody.dry_dispositions[:2])
        reporter=SimpleNamespace(qualified_terminals=excluded)
        report=lambda:PublicHostInputs._work_report(reporter,started)
        budget.after_start(report())
        before_source=(runtime.source.count(),runtime.source.latest().content_digest,runtime.producer.durable_p1_rowid)
        attempts=list(runtime.ledger._conn.execute('SELECT attempt_id FROM ledger_attempts ORDER BY attempt_id'))
        reader=sqlite3.connect(Path(args['ledger_path']).as_uri()+'?mode=ro',uri=True,isolation_level=None)
        try:
            reader.execute('BEGIN');reader.execute('SELECT * FROM ledger_head').fetchall()
            began=time.perf_counter_ns();boundary=gate._checkpoint_boundary(started)
            elapsed=(time.perf_counter_ns()-began)//1000
            assert not boundary and gate.snapshot()['held_reason'] is not None
            snapshot=budget.snapshot()
            parent_restart=budget.before_start()
            assert parent_restart
            budget.after_start(report())
            parent_step=budget.before_step()
            child_priority=gate.before_step(started)
            assert parent_step and child_priority
            reader_before=reader.execute('SELECT * FROM ledger_head').fetchall()
            calls=0
            def clock():
                nonlocal calls
                calls+=1
                previous=runtime.ledger._authority.last_qualified_clock
                assert previous is not None
                policy=runtime.ledger._authority.policy.clock
                stamp=max(config['now']*1000000,utc_microseconds(previous.utc_upper_utc))+calls
                mono=previous.monotonic_ns+(stamp-utc_microseconds(previous.utc_upper_utc))*1000
                return TrustedClockSample(policy.provider_id,policy.provider_fingerprint,previous.epoch_id,mono,
                    utc_from_us(stamp),utc_from_us(stamp),content_fingerprint(('checkpoint-recovery-fixture',stamp)),
                    previous.content_digest,'QUALIFIED',None)
            work=runtime.step(clock=clock,operations_resources=None,source_cut_utc=runtime.source.latest().snapshot.requested_cut_utc)
            assert work.work=='NON_SUBMITTED',work
            gate.after_step(work,started)
            budget.after_step(report())
            assert not runtime.ledger.consumer_snapshot()['reservations']
            assert reader.execute('SELECT * FROM ledger_head').fetchall()==reader_before
            assert not budget.before_start() and not budget.before_step()
            assert gate.snapshot()['held_reason'] is not None
            assert before_source==(runtime.source.count(),runtime.source.latest().content_digest,runtime.producer.durable_p1_rowid)
            assert attempts==list(runtime.ledger._conn.execute('SELECT attempt_id FROM ledger_attempts ORDER BY attempt_id'))
            record=dict(scope='SYNTHETIC_EXTERNAL_FACTS_ORIGINAL_PENDING_ACTION_HOST_RECOVERY_VALIDATION',
                pending_action_count=len(reservations),attempt_ids=attempts,original_source=before_source,
                checkpoint_boundary=boundary,checkpoint_elapsed_us=elapsed,resource_state=gate.snapshot(),
                parent_before_step=parent_step,parent_before_restart=parent_restart,child_resource_gate_priority=child_priority,
                original_budget=snapshot,budget_unchanged=budget.snapshot()==snapshot,
                contradiction=False,work=asdict(work),final_budget=budget.snapshot(),
                reader_snapshot_preserved=True,ordinary_work_not_rearmed=True,
                no_runtime_step_executed=False,original_pending_recovery_proved=True,
                original_files_unchanged=before,production_edits=False,public_qualification_claimed=False)
            copies.write(output/'result.json',record)
        finally:reader.rollback();reader.close()
    finally:runtime.close()
    assert before=={p:copies.sha(Path(p)) for p in before}
    print(json.dumps({'result':str(output/'result.json'),'sha256':copies.sha(output/'result.json')}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--origin',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();run(a.origin,a.output)
