"""Original gate/SQLite and whole-hook timing; isolated synthetic owner facts."""
import json,sqlite3,sys,tempfile
from contextlib import ExitStack,nullcontext
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace as N
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from live import t010_public_host_v0_1 as host
from live.t010_resource_envelope_v0_1 import ResourceGate,derive
from live_t010_resource_envelope_selftest_v0_1 import fixture
from phase5.shadow_domain_v0_1 import content_fingerprint
CHECKS={}
def check(name,v):CHECKS[name]=bool(v);assert v,name

def checkpoints(directory,cleanup):
    inputs,accepted,extension=fixture();envelope={'derivation':derive(inputs,accepted,extension)}
    connections=[]
    for name in ('ledger','source','producer'):
        c=sqlite3.connect(directory/(name+'.sqlite'));cleanup.callback(c.close)
        c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA wal_autocheckpoint=0')
        c.execute('CREATE TABLE history(id INTEGER PRIMARY KEY, value TEXT)')
        c.execute("INSERT INTO history VALUES(1,'ORIGINAL')");c.commit();connections.append(c)
    runtime=N(capability='NO_BROADCAST',ledger=N(_conn=connections[0],_custody=N(reservations=[])),
        source=N(_conn=connections[1],latest=lambda:N(disposition='HEALTHY')),
        producer=N(conn=connections[2]),_dry_recovery=False)
    started=N(runtime=runtime)
    metrics={'IDENTITY_ROWS':0,'TOMBSTONE_ROWS':0,'UNFINISHED_MINTS':0,'HISTORY_BYTES':0,
        'CHECKPOINT_BYTES':1,'PRODUCER_PENDING_ROWS':0,'PRODUCER_PENDING_BYTES':0}
    gate=ResourceGate(directory/'gate.json',envelope,'host',create=True)
    with patch('live.operations_degradation_monitor_v0_1._producer_cut',return_value=(metrics,{},'0'*64)):
        check('original_restart_then_source_reservation',gate.before_step(started) and gate.snapshot()['source_rows_reserved']==32)
        # A reader arriving after the successful restart sees original main
        # pages. The next write is legal, but its after-boundary must hold.
        reader=sqlite3.connect(directory/'source.sqlite');cleanup.callback(reader.close)
        reader.execute('BEGIN');original=reader.execute('SELECT * FROM history').fetchall()
        connections[1].execute("INSERT INTO history VALUES(2,'NEXT_ORIGINAL_UNIT')");connections[1].commit()
        gate.after_step(N(work='SOURCE_PAGE_CONSUMED'),started)
        check('after_boundary_pinned_reader_latches_hold',gate.snapshot()['held_reason']=='OWNED_SQLITE_CHECKPOINT_INCOMPLETE')
        reopened=ResourceGate(gate.path,envelope,'host')
        check('cold_gate_reopen_cannot_renew_source',not reopened.before_step(started) and reopened.snapshot()['source_rows_reserved']==32)
        runtime.ledger._custody.reservations=[N(retired_sequence=None)]
        check('held_source_preserves_admitted_recovery_priority',reopened.before_step(started) and runtime._resource_batch_rows==0 and reopened.snapshot()['source_rows_reserved']==32)
        check('reader_not_interrupted_or_rewritten',reader.in_transaction and reader.execute('SELECT * FROM history').fetchall()==original)
        reader.close()
        runtime.ledger._custody.reservations=[]
        reopened.after_step(N(work='NON_SUBMITTED'),started)
        check('completed_recovery_does_not_rearm_held_source',not reopened.before_step(started))
        check('original_wal_integrity_preserved',all(c.execute('PRAGMA integrity_check').fetchone()==('ok',) for c in connections))

def timing(directory):
    @dataclass(frozen=True)
    class Grant:grant_id:str='fixture-grant'
    ticks=[0];events=[]
    def advance(event,amount):events.append(event);ticks[0]+=amount
    grant=Grant();state=N(policy=N(content_digest='p'),grant=grant)
    class Audit:
        @property
        def authority(self):advance('policy_and_grant',2);return state
    class Ledger:
        _custody=N(dry_dispositions=[],reservations=[])
        def reservation(self,root):advance('qualified_reservation',3);return N(retired_sequence=1)
        def _port_at_sequence(self,seq):advance('qualified_receipt',5);return N(content_digest='t')
        def _simulation_input_digest(self,attempt):advance('qualified_simulation',7);return 's'
        def _trusted_read(self):advance('original_work_report',11);return nullcontext()
    started=N(audit=Audit(),runtime=N(ledger=Ledger()))
    def facts(profile,settings):advance('public_facts_constructor',13);return N(startup_us=None)
    def driver(value):advance('original_driver_constructor',17);return object()
    original,accepted,extension=fixture();envelope={'derivation':derive(original,accepted,extension)}
    path=directory/'timing-gate.json';ResourceGate(path,envelope,'host',create=True)
    profile=N(driver=driver,record={'inputs':{'resource_envelope':{'path':'EXPLICIT_TEST_REFERENCE'}}})
    inputs=host.PublicHostInputs(profile,{},'p',grant.grant_id,content_fingerprint({'grant_id':grant.grant_id}),
        tuple({'root_id':str(i),'terminal_digest':'t','attempt_id':str(i),'simulation_input_digest':'s'} for i in range(2)),str(path))
    def gate(*args,**kwargs):advance('original_resource_gate_constructor',19);return ResourceGate(*args,**kwargs)
    with patch.object(host,'ReviewedPublicFacts',side_effect=facts),patch.object(host.time,'perf_counter_ns',side_effect=lambda:ticks[0]*1000), \
            patch.object(host,'read_reference',return_value=envelope),patch('live.t010_resource_envelope_v0_1.ResourceGate',side_effect=gate):
        report=inputs.runtime_started(started,100)
    check('startup_includes_checks_constructors_and_work_report',inputs.facts.startup_us==100+2+2*(3+5+7)+13+17+19+11)
    check('startup_preserves_original_terminal_validation_order',events==['policy_and_grant','qualified_reservation','qualified_receipt','qualified_simulation','qualified_reservation','qualified_receipt','qualified_simulation','public_facts_constructor','original_driver_constructor','original_resource_gate_constructor','original_work_report'])
    check('startup_report_unchanged',report=={'terminal_roots':[],'pending_action':False})

def main():
    with tempfile.TemporaryDirectory(prefix='fix2-boundary-integration-') as folder,ExitStack() as cleanup:
        checkpoints(Path(folder),cleanup)
        timing(Path(folder))
    print(json.dumps({'scope':'ORIGINAL_BOUNDARIES_SYNTHETIC_OWNER_FACTS','checks':CHECKS,'count':len(CHECKS),'T010':'NOT_STARTED'},sort_keys=True))
if __name__=='__main__':main()
