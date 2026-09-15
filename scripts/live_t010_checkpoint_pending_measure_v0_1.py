"""Original pending-state checkpoint/parent-dispatch diagnostic; no public I/O."""
import argparse,json,shutil,sqlite3,sys,time
from contextlib import closing
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_t010_joint_hot_measure_v0_2 as copies
from live.operations_startup_v0_1 import start_runtime
from live.operations_ownership_v0_1 import OperationsStore
from live.t010_public_host_v0_1 import WorkBudget
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
        budget.after_start({'terminal_roots':[],'pending_action':True})
        before_source=(runtime.source.count(),runtime.source.latest().content_digest,runtime.producer.durable_p1_rowid)
        attempts=list(runtime.ledger._conn.execute('SELECT attempt_id FROM ledger_attempts ORDER BY attempt_id'))
        reader=sqlite3.connect(Path(args['ledger_path']).as_uri()+'?mode=ro',uri=True,isolation_level=None)
        try:
            reader.execute('BEGIN');reader.execute('SELECT * FROM ledger_head').fetchall()
            began=time.perf_counter_ns();boundary=gate._checkpoint_boundary(started)
            elapsed=(time.perf_counter_ns()-began)//1000
            assert not boundary and gate.snapshot()['held_reason'] is not None
            snapshot=budget.snapshot()
            parent_step=budget.before_step();parent_restart=budget.before_start()
            child_priority=gate.before_step(started)
            assert budget.snapshot()==snapshot
            assert before_source==(runtime.source.count(),runtime.source.latest().content_digest,runtime.producer.durable_p1_rowid)
            assert attempts==list(runtime.ledger._conn.execute('SELECT attempt_id FROM ledger_attempts ORDER BY attempt_id'))
            record=dict(scope='SYNTHETIC_EXTERNAL_FACTS_ORIGINAL_PENDING_ACTION_HOST_DISPATCH_DIAGNOSTIC',
                pending_action_count=len(reservations),attempt_ids=attempts,original_source=before_source,
                checkpoint_boundary=boundary,checkpoint_elapsed_us=elapsed,resource_state=gate.snapshot(),
                parent_before_step=parent_step,parent_before_restart=parent_restart,child_resource_gate_priority=child_priority,
                original_budget=snapshot,budget_unchanged=budget.snapshot()==snapshot,
                contradiction=child_priority and not parent_step and not parent_restart,
                no_runtime_step_executed=True,original_pending_recovery_proved=False,
                original_files_unchanged=before,production_edits=False,public_qualification_claimed=False)
            copies.write(output/'result.json',record)
        finally:reader.rollback();reader.close()
    finally:runtime.close()
    assert before=={p:copies.sha(Path(p)) for p in before}
    print(json.dumps({'result':str(output/'result.json'),'sha256':copies.sha(output/'result.json')}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--origin',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();run(a.origin,a.output)
