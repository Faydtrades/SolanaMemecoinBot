"""B3.4 bounded original retirement/source/stop recovery, disposable I/O only."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from contextlib import closing
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from types import SimpleNamespace

import live_operations_sell_recovery_selftest_v0_1 as b33

b32, b23, b21, b1, rt, NOW = b33.b32, b33.b23, b33.b21, b33.b1, b33.rt, b33.NOW
write, durable = b33.write, b33.durable
ROOT = Path(__file__).resolve().parents[1]
CASES = ('zero-residual-retirement', 'exact-tail-checkpoint-replacement', 'regressed-source-tail',
    'corrupt-checkpoint-digest', 'corrupt-source-evidence', 'missing-producer',
    'durable-hard-stop', 'durable-operator-stop', 'stale-generation')
FAILED_SOURCE = CASES[2:6]
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def facts(started, root):
    # Position binding legitimately disappears only on original retirement.
    value = b23.facts(started, root)
    repo, binding = started.runtime.ledger, started.runtime.position_binding
    value.update(startup_audit=asdict(started.audit), authority=asdict(repo._authority),
        producer_checkpoint=None if started.runtime.producer is None else
            list(started.runtime.producer.conn.execute('SELECT * FROM live_producer_checkpoint_v0_1').fetchone()),
        binding=None if binding is None else asdict(binding),
        exit_records=[] if binding is None else [asdict(r) for r in repo.exit_records(binding.binding_id)],
        protection=None if binding is None else asdict(rt.protective_outcome(repo,binding)))
    return value


def sample(owner, resources, label):
    repo = owner.started.runtime.ledger
    before = durable(repo.database_path)
    barrier = b32.readiness.evaluate(owner.started,clock=lambda:rt.a3.clock(repo,owner.now),entry=owner.entry)
    record = dict(pid=os.getpid(),label=label,at=owner.now,barrier=asdict(barrier),
        read_unchanged=before == durable(repo.database_path),facts=facts(owner.started,owner.root),
        inputs=dict(entry=True,retirement_wallet=None if 'retirement_wallet' not in resources
            else asdict(resources['retirement_wallet'])))
    write(owner.directory/('readiness-'+label+'.json'),record)
    return record


@dataclass
class Inputs(b33.Inputs):
    def __call__(self, started):
        if not hasattr(self,'initialized'):
            self.now,self.phase,self.send_calls = NOW+16,0,0
            self.requests = []
            self.observe_calls()
            resources = b21.Inputs.__call__(self,started)
            self.f = SimpleNamespace(repo=started.runtime.ledger,runtime=started.runtime,
                domain=started.runtime.ledger.domain,public=self.public,lower=self.lower,name=self.case)
            if not self.replacement:
                if self.case == 'zero-residual-retirement':
                    self.now = NOW+20
                    return resources
                repo,binding = self.f.repo,started.runtime.position_binding
                b32.composition.commit_exit_evaluation(repo,binding,command_id='b34-original-monitoring',
                    timer_fence_utc=rt.a3.utc(NOW+14))
                b32.composition.ensure_protective_obligation(repo,binding,recorded_at_utc=rt.a3.utc(NOW+14))
                if self.case == 'durable-hard-stop':
                    rt.a3.control(repo,'HARD_STOP','b34-original-hard-stop',at=NOW+15)
                self.pause()
            write(self.directory/'recovered.json',facts(started,self.root))
        elif self.replacement:
            self.phase += 1

        repo = self.f.repo
        if not self.replacement:
            self.attempt_id = repo._root_attempts(self.root)[1].preparation.attempt_id
            self.envelope = b23.sender.load_signed_envelope(repo,self.attempt_id)
            attempt = repo.attempt(self.attempt_id)
            resources = dict(clock=self.clock,source_cut_utc=rt.a3.utc(NOW+1),entry=self.entry)
            if repo.attempt_finality(self.attempt_id).positive_finality is None:
                self.now = NOW+26
                resources['truth_rpc'] = self.truth_rpc()
            elif attempt.lane_held:
                self.now = NOW+28
                resources['application_wallet'] = self.support(self.now)
            else:
                self.pause()
            self.clock_calls = 0
            return resources

        labels = ('missing','incomplete','current','terminal') if self.case == 'zero-residual-retirement' else (
            'due','missing','terminal')
        label = labels[self.phase]
        self.now = NOW+(30+self.phase if self.case == 'zero-residual-retirement' else 16+self.phase)
        self.clock_calls = 0
        resources = dict(clock=self.clock,source_cut_utc=rt.a3.utc(NOW+1),entry=self.entry)
        if self.case == 'zero-residual-retirement' and label in ('incomplete','current'):
            self.attempt_id = repo._root_attempts(self.root)[1].preparation.attempt_id
            self.envelope = b23.sender.load_signed_envelope(repo,self.attempt_id)
            if not hasattr(self,'restored_public'):
                # Retain the original block identity/time; only observation time advances.
                self.support(NOW+28)
                self.restored_public = True
            resources['retirement_wallet'] = b1.c4.c3.wallet(self.f,self.now,incomplete=label == 'incomplete')
        sample(self,resources,label)
        if label == 'terminal':
            write(self.directory/'finished.json',dict(facts=facts(started,self.root),send_calls=self.send_calls))
            b1.wait_file(self.directory/'finish')
        elif self.case in ('durable-operator-stop','stale-generation') and label == 'due':
            write(self.directory/'control-ready.json',dict(pid=os.getpid(),facts=facts(started,self.root)))
            b1.wait_file(self.directory/'control-release')
            sample(self,resources,'after-control')
            # Actual retained fresh child's original Runtime gate, no owner substitution.
            result = started.runtime.step(**resources)
            sample(self,resources,'after-held-step')
            write(self.directory/'control-result.json',dict(work=result.work,facts=facts(started,self.root)))
            b1.wait_file(self.directory/'finish')
        return resources

    def pause(self):
        value = facts(self.started,self.root)
        producer = self.started.runtime.producer
        write(self.directory/'reached.json',dict(pid=os.getpid(),at=self.now,
            boundary='original-owner-return-before-next-Runtime-step',in_transaction=self.f.repo._conn.in_transaction,
            facts=value,durable=value['durable'],producer_checkpoint=None if producer is None else
                list(producer.conn.execute('SELECT * FROM live_producer_checkpoint_v0_1').fetchone())))
        b1.wait_file(self.directory/'release')


def setup(directory,case):
    if case == 'zero-residual-retirement':
        place,_,unused,_ = b23.setup(directory,(case,'SELL','b34-only',False,None))
        old = unused._external_inputs
        inputs = Inputs(**{f.name:getattr(old,f.name) for f in fields(b23.Inputs)})
    else:
        place,unused,_ = b32.setup(directory,case)
        old = unused._external_inputs
        inputs = Inputs(**{f.name:getattr(old,f.name) for f in fields(b32.Inputs)})
    sup = b1.supervisor.OperationsSupervisor(unused._configuration,inputs,unused.health,
        expected_generation=unused._generation)
    return place,sup,b1.Driver(sup,now=unused._generation+1)


def source_facts(config):
    result = {}
    for name,path in (('tail',config['market_source'].db_path),('producer',config['producer_path']),
            ('evidence',config['source_path'])):
        path = Path(path)
        record = dict(path=str(path),exists=path.exists())
        if path.exists():
            record['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            try:
                with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as conn:
                    record['dump_sha256'] = hashlib.sha256('\n'.join(conn.iterdump()).encode()).hexdigest()
                    if name == 'producer':
                        record['checkpoint'] = [list(row) for row in conn.execute('SELECT * FROM live_producer_checkpoint_v0_1')]
                    if name == 'tail':
                        record['latest_p1_rowid'] = config['market_source'].latest_p1_rowid()
            except sqlite3.DatabaseError as exc:
                record['error'] = str(exc)
        result[name] = record
    result['market_identity'] = config['market_source'].source_identity
    result['binding'] = asdict(config['source_binding'])
    return result


def replace_sources(place,config,case):
    before = source_facts(config)
    paths = dict(tail=Path(config['market_source'].db_path),producer=Path(config['producer_path']),
        evidence=Path(config['source_path']))
    # Called only after the exact disposable child has died. Never touches economic stores.
    for path in paths.values():
        assert place.resolve() in path.resolve().parents
    if case == 'exact-tail-checkpoint-replacement':
        for name in ('tail','producer'):
            path = paths[name]
            backup = place/(name+'-exact-backup.sqlite')
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as src, closing(sqlite3.connect(backup)) as dst:
                src.backup(dst)
            backup.replace(path)
    elif case == 'regressed-source-tail':
        with closing(sqlite3.connect(paths['tail'])) as conn,conn:
            # Original synthetic collector fixture table only; preserve schema and identity.
            conn.execute('DELETE FROM pump_events')
    elif case == 'corrupt-checkpoint-digest':
        with closing(sqlite3.connect(paths['producer'])) as conn,conn:
            conn.execute('UPDATE live_producer_checkpoint_v0_1 SET manifest_digest=?',('0'*64,))
    elif case == 'corrupt-source-evidence':
        paths['evidence'].write_bytes(b'B3.4 disposable corrupt source evidence')
    elif case == 'missing-producer':
        paths['producer'].rename(place/'producer-removed-original.sqlite')
    result = dict(before=before,after=source_facts(config),boundary='confirmed-dead-disposable-child')
    write(place/'source-replacement.json',result)
    return result


def run_case(directory,case):
    place,sup,driver = setup(directory,case)
    takeover = None
    try:
        driver.until(lambda result:(place/'reached.json').exists())
        reached = json.loads((place/'reached.json').read_text())
        old_pid,old_fence,budget = sup.process.pid,sup.fence,sup.store.snapshot()
        driver.mono = sup._deadline
        check(case+':actual-watchdog-loss',driver.tick().state == 'TERMINATING')
        sup.process.join(timeout=5)
        check(case+':confirmed-death-before-source-replacement',sup.process.exitcode is not None)
        dead_exit = sup.process.exitcode
        replacement = replace_sources(place,sup._configuration,case)
        driver.now += 1
        works,prior = [],sup.completed_steps
        controlled = case in ('durable-operator-stop','stale-generation')
        def finished(result):
            nonlocal prior
            if sup.completed_steps > prior:
                works.append(sup.last_work)
                prior = sup.completed_steps
                write(place/'runtime-works.json',works)
            return (place/('control-ready.json' if controlled else 'finished.json')).exists()
        driver.until(finished)
        recovered = json.loads((place/'recovered.json').read_text())
        recovered_budget = sup.store.snapshot()
        control = None
        if controlled:
            if case == 'durable-operator-stop':
                # Store API commits stop without signalling; retained child observes the real fence.
                sup.store.operator_stop(now_us=driver.now+1)
            else:
                takeover = sup.store.acquire('b34-explicit-host-takeover',now_us=driver.now+1,
                    replace_generation=sup.fence.generation)
            driver.now += 1
            (place/'control-release').write_text('observe original current owner gate')
            b1.wait_file(place/'control-result.json')
            control = json.loads((place/'control-result.json').read_text())
            control['supervisor'] = asdict(driver.tick())
            control['operations'] = sup.store.snapshot()
            if case == 'durable-operator-stop':
                other = b1.supervisor.OperationsSupervisor(sup._configuration,sup._external_inputs,sup.health,
                    expected_generation=control['operations']['generation'])
                control['reopened_supervisor'] = asdict(other.poll(now_us=driver.now+100,monotonic_us=0))
                control['no_new_child'] = other.process is None
            else:
                try:
                    b1.supervisor.OperationsSupervisor(sup._configuration,sup._external_inputs,sup.health,
                        expected_generation=sup.fence.generation)
                except ValueError as exc:
                    control['reopen_denial'] = str(exc)
                    control['no_new_child'] = True  # rejected constructor; no poll or spawn occurred
                else:
                    raise AssertionError('stale supervised generation unexpectedly accepted')
            write(place/'control-evidence.json',control)
            ending = dict(facts=control['facts'],send_calls=0)
        else:
            ending = json.loads((place/'finished.json').read_text())
        original,final = reached['facts'],ending['facts']
        reads = [json.loads(p.read_text()) for p in sorted(place.glob('readiness-*.json'))]
        by_label = {r['label']:r for r in reads}
        check(case+':fresh-original-A1-A2-cold-owner',recovered['runtime'] == 'ColdRuntimeV01'
            and recovered['fence']['generation'] == old_fence.generation+1 and sup.process.pid != old_pid
            and recovered['fence']['process_identity'] != old_fence.process_identity
            and recovered['startup_audit']['owner_fence'] == recovered['fence'])
        check(case+':one-budgeted-replacement',recovered_budget['attempts'] == budget['attempts']+1
            and recovered_budget['budget_id'] == budget['budget_id'])
        check(case+':exact-economic-cut-restored',not reached['in_transaction']
            and recovered['durable']['economic_sha256'] == original['durable']['economic_sha256'])
        check(case+':original-full-rows-restored',all(original[k] == recovered[k] for k in
            ('binding','exit_records','position','applications','custody','authority','reservations','attempts','envelopes','stages','message_receipts')))
        check(case+':history-does-not-grant-permission',json.loads((place/'historical-replays.json').read_text())['unchanged']
            and not recovered['audit_permission'] and not recovered['reconstruction']['grants_permission'])
        check(case+':current-A3-pure-no-capability',all(r['read_unchanged'] and not r['barrier']['grants_permission']
            and all(not r['barrier'][k][p] for k in ('entry','protective') for p in ('grants_permission','may_sign','may_send')) for r in reads))
        check(case+':current-common-cut-fence',all(r['barrier']['owner_fence'] == recovered['fence']
            and r['barrier']['authority_digest'] == r['facts']['authority_digest']
            and r['barrier']['reconstruction']['consumer_cut'] == r['facts']['reconstruction']['consumer_cut']
            for r in reads if r['label'] not in ('after-control','after-held-step')))
        check(case+':current-entry-always-denied',all(r['inputs']['entry'] and not r['barrier']['entry']['ready'] for r in reads))
        check(case+':no-duplicate-BUY-SELL-or-application',
            [{k:v for k,v in a.items() if k != 'current_inbox_disposition'} for a in final['attempts']]
            == [{k:v for k,v in a.items() if k != 'current_inbox_disposition'} for a in original['attempts']]
            and final['applications'] == original['applications'] and final['envelopes'] == original['envelopes']
            and final['stages'] == original['stages'] and final['message_receipts'] == original['message_receipts']
            and ending['send_calls'] == 0)
        check(case+':only-original-retirement-disposition-change',all(a['current_inbox_disposition'] ==
            ('RETIRED' if case == 'zero-residual-retirement' else original['attempts'][i]['current_inbox_disposition'])
            for i,a in enumerate(final['attempts'])))
        check(case+':sqlite-economic-integrity',all(r['facts']['durable']['integrity'] == [['ok']]
            and not r['facts']['durable']['foreign_keys'] for r in reads))
        if case == 'zero-residual-retirement':
            check(case+':original-zero-residual-capacity-held',original['position']['remaining_units'] == 0
                and len(original['reservations']) == 1 and original['protection']['state'] == 'SATISFIED')
            check(case+':missing-incomplete-support-never-releases',all(by_label[k]['facts']['reservations'] == original['reservations']
                and by_label[k]['facts']['binding'] == original['binding'] for k in ('missing','incomplete','current')))
            check(case+':only-current-original-retirement-releases',works == ['NEED_RETIREMENT','RETIREMENT_WITHHELD','RETIREMENT_RETIRED']
                and not final['reservations'] and final['binding'] is None and final['position']['remaining_units'] == 0
                and final['reconstruction']['position_id'] is None)
            check(case+':later-entry-still-needs-current-prerequisites',not by_label['terminal']['barrier']['entry']['ready'])
        else:
            check(case+':exact-open-exposure-and-obligation',all(r['facts']['position'] == original['position']
                and r['facts']['reservations'] == original['reservations'] and r['facts']['binding'] == original['binding']
                and r['facts']['custody'] == original['custody']
                and r['facts']['protection']['obligation']['handoff'] == original['protection']['obligation']['handoff']
                and r['facts']['exit_records'][:len(original['exit_records'])] == original['exit_records'] for r in reads))
            if not controlled:
                check(case+':due-stays-due',all(r['barrier']['protective']['due'] for r in reads))
                check(case+':original-safe-protection-continuation',works ==
                    (['ENTRY_HELD','ENTRY_HELD'] if case == 'durable-hard-stop' else ['PROTECTIVE_ACTION_STAGED','NEED_EXECUTION']))
            if case in FAILED_SOURCE:
                check(case+':source-fails-conservatively',recovered['reconstruction']['source_state'] == 'SOURCE_RECONSTRUCTION_UNAVAILABLE'
                    and all('CURRENT_RECONSTRUCTED_SOURCE_REQUIRED' in r['barrier']['entry']['reasons'] for r in reads))
                expected = 'SOURCE_PREFLIGHT_VERIFIED' if case in ('regressed-source-tail','corrupt-checkpoint-digest') else 'SOURCE_PREFLIGHT_FAILED'
                check(case+':exact-startup-source-failure-boundary',recovered['startup_audit']['source_audit'] == expected)
                check(case+':source-failure-never-grants-protection-permission',all(not r['barrier']['protective']['ready'] for r in reads))
                if case == 'missing-producer':
                    check(case+':missing-store-never-initialized',not Path(sup._configuration['producer_path']).exists())
                if case == 'regressed-source-tail':
                    check(case+':exact-source-cursor-regression',replacement['after']['tail']['latest_p1_rowid']
                        < original['reconstruction']['producer_cursor'] <= replacement['before']['tail']['latest_p1_rowid'])
            elif case == 'exact-tail-checkpoint-replacement':
                prior_manifest = json.loads(reached['producer_checkpoint'][2])
                current_manifest = json.loads(recovered['producer_checkpoint'][2])
                handoff_manifest = dict(prior_manifest,generation=prior_manifest['generation']+1,
                    predecessor=reached['producer_checkpoint'][3])
                handoff_digest = b32.composition.content_fingerprint(handoff_manifest)
                check(case+':exact-durable-source-replacement',all(replacement['before'][k]['dump_sha256']
                    == replacement['after'][k]['dump_sha256'] for k in ('tail','producer','evidence')))
                check(case+':same-original-lineage-checkpoint-cursor-source',recovered['reconstruction']['source_state'] == 'SOURCE_RECONSTRUCTED'
                    and all(recovered['reconstruction'][k] == original['reconstruction'][k] for k in
                    ('producer_lineage_id','producer_cursor','retained_source_digest'))
                    and replacement['before']['producer']['checkpoint'] == [reached['producer_checkpoint']])
                # Original handoff constructor and cold checkpoint read each use
                # producer._transaction(), hence two legitimate publications.
                check(case+':original-republish-preserves-exact-checkpoint-truth',
                    {k:v for k,v in prior_manifest.items() if k not in ('generation','predecessor')}
                    == {k:v for k,v in current_manifest.items() if k not in ('generation','predecessor')}
                    and current_manifest['generation'] == prior_manifest['generation']+2
                    and current_manifest['predecessor'] == recovered['reconstruction']['producer_checkpoint_digest'] == handoff_digest
                    and b32.composition.content_fingerprint(current_manifest) == recovered['producer_checkpoint'][3])
            elif case == 'durable-hard-stop':
                check(case+':original-hard-stop-retained-and-current',original['authority']['hard_stop_command'] == 'b34-original-hard-stop'
                    and final['authority'] == original['authority'] and all('GLOBAL_HARD_STOP_LATCHED' in r['barrier']['protective']['reasons']
                    and 'GLOBAL_HARD_STOP_LATCHED' in r['barrier']['entry']['reasons'] and not r['barrier']['protective']['ready'] for r in reads))
                check(case+':hard-stop-no-protective-action-created',all(r['facts']['protection']['action'] is None
                    and not r['facts']['protection']['mutation_eligible'] for r in reads))
            elif controlled:
                check(case+':actual-stale-or-stopped-child-held',control['work'] == 'OPERATIONS_HELD'
                    and final['durable']['economic_sha256'] == recovered['durable']['economic_sha256'])
                check(case+':current-owner-failure-no-historical-resurrection',all(by_label[k]['barrier']['owner_fence'] is None
                    and by_label[k]['barrier']['reconstruction'] is None
                    and not by_label[k]['barrier']['protective']['ready'] for k in ('after-control','after-held-step')))
                expected = 'OPERATOR_STOPPED' if case == 'durable-operator-stop' else 'HELD_OWNER_CHANGED'
                check(case+':durable-control-prevents-replacement',control['supervisor']['state'] == expected
                    and control['no_new_child'] and (control['reopened_supervisor']['state'] == expected
                    if case == 'durable-operator-stop' else control['reopen_denial'] == 'OPERATIONS_SUPERVISOR_GENERATION_CONFLICT'))
        record = dict(case=case,reached=reached,old_pid=old_pid,old_fence=asdict(old_fence),dead_exit=dead_exit,
            budget_before=budget,budget_recovered=recovered_budget,budget_after=sup.store.snapshot(),
            source_replacement=replacement,recovered=recovered,readiness=reads,works=works,ending=ending,control=control,
            checks={k:v for k,v in CHECKS.items() if k.startswith(case+':')})
        write(place/'result.json',record)
        print(json.dumps(dict(case=case,works=works,checks=len(record['checks']))),flush=True)
        return record
    finally:
        if takeover is not None:
            takeover.close()
        b1.cleanup(sup)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence-dir',type=Path,required=True)
    directory = parser.parse_args().evidence_dir.resolve()
    if directory == ROOT or ROOT in directory.parents:
        raise ValueError('external disposable evidence directory required')
    directory.mkdir(parents=True,exist_ok=False)
    records = [run_case(directory,case) for case in CASES]
    sources = {str(Path(m.__file__).resolve().relative_to(ROOT)).replace('\\','/'):
        hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest() for m in tuple(sys.modules.values())
        if getattr(m,'__file__',None) and ROOT in Path(m.__file__).resolve().parents and Path(m.__file__).suffix == '.py'}
    write(directory/'campaign.json',dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW',cases=records,
        checks=CHECKS,source_sha256=sources))
    print(json.dumps(dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW',cases=len(records),checks=len(CHECKS),
        all_checks=all(CHECKS.values()))),flush=True)


if __name__ == '__main__':
    main()
