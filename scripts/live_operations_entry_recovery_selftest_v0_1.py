"""B3.1 fresh entry recovery consumers over accepted B1/B2 fixtures.

Four representative cuts, current original A3 reads, original Runtime work.
Disposable children/stores, ephemeral synthetic keys and mocked external I/O.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import asdict, dataclass, fields
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_settlement_fault_selftest_v0_1 as b24
from live import operations_readiness_v0_1 as readiness

b23, b21, b1, rt, NOW = b24.b23, b24.b21, b24.b1, b24.rt, b24.NOW
b22 = b23.b22
write, durable = b23.write, b21.durable
CHECKS = {}
CASES = ('no-position', 'reserved', 'possible-send', 'finalized-buy')


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def facts(started):
    repo = started.runtime.ledger
    value = b21.owned_facts(started)
    if value['attempts'] or repo._applications:
        root = (value['attempts'][0]['preparation']['action_id'] if value['attempts'] else None)
        action_id = root or repo.attempt(next(iter(repo._applications.values())).attempt_id).preparation.action_id
        root = repo.action(action_id).root_id
        value = b23.facts(started, root)
    value['admissions'] = [asdict(repo.authority_admission_receipt(key)) for (key,) in repo._conn.execute(
        'SELECT command_id FROM ledger_authority_admissions ORDER BY commit_seq')]
    value['startup_audit'] = asdict(started.audit)
    return value


def sample(owner, resources, label):
    """An actual A3 current read, never a replacement mutation capability."""
    repo = owner.started.runtime.ledger
    before = durable(repo.database_path)
    barrier = readiness.evaluate(owner.started, clock=lambda: rt.a3.clock(repo, owner.now),
        entry=resources.get('entry'))
    after = durable(repo.database_path)
    record = dict(pid=os.getpid(), label=label, at=owner.now, barrier=asdict(barrier),
        read_unchanged=before == after, facts=facts(owner.started),
        inputs=dict(entry='entry' in resources, execution='execution' in resources,
            truth_rpc='truth_rpc' in resources, application_wallet='application_wallet' in resources))
    write(owner.directory/('readiness-'+label+'.json'), record)
    return record


@dataclass
class EntryInputs(b21.Inputs):
    case: str = ''
    clock = b22.Inputs.clock

    def __call__(self, started):
        if not hasattr(self, 'initialized'):
            self.now = NOW+(6 if self.case == 'possible-send' else 4)
            self.phase = 0
            resources = super().__call__(started)
            if not self.replacement:
                return resources
            write(self.directory/'recovered.json', facts(started))
        else:
            self.phase += 1
            resources = dict(clock=self.clock, source_cut_utc=rt.a3.utc(NOW+1))
            if self.case == 'possible-send':
                resources['execution'] = self.ports
        resources['entry'] = self.entry
        terminal = 4 if self.case == 'possible-send' else 2
        if self.phase == terminal:
            sample(self, resources, 'terminal')
            write(self.directory/'finished.json', dict(facts=facts(started)))
            b1.wait_file(self.directory/'finish')
        if self.case == 'possible-send':
            self.now = NOW+(6, 3600, 3601, 7200)[self.phase]
            if self.phase == 2:
                repo = started.runtime.ledger
                envelope = b22.sender.load_signed_envelope(repo, started.audit.pending_attempt.preparation.attempt_id)
                self.history = b22.MissingHistory(envelope, repo.domain, self.now)
                resources['truth_rpc'] = rt.PublicReadOnlyRpc('https://invalid.local', rt.a3.PROFILE,
                    transport=rt.httpx.MockTransport(self.history))
            if self.phase == 3:
                write(self.directory/'history-requests.json', self.history.requests)
        sample(self, resources, str(self.phase))
        return resources


@dataclass
class FinalizedInputs(b24.Inputs):
    def __call__(self, started):
        resources = super().__call__(started)
        if self.replacement:
            resources['entry'] = self.entry
            if self.phase == 0:
                write(self.directory/'recovered.json', facts(started))
            sample(self, resources, str(self.phase))
        return resources

    def finish(self):
        sample(self, dict(entry=self.entry), 'terminal')
        write(self.directory/'finished.json', dict(facts=facts(self.started), send_calls=self.send_calls))
        b1.wait_file(self.directory/'finish')


def setup(directory, case):
    if case == 'finalized-buy':
        place, fixture, unused, _ = b24.setup(directory, (case, 'buy-application', 'before'))
        original = unused._external_inputs
        inputs = FinalizedInputs(**{f.name:getattr(original, f.name) for f in fields(b24.Inputs)})
    else:
        target, side = ('send-claim', 'after') if case == 'possible-send' else ('admission',
            'before' if case == 'no-position' else 'after')
        place, fixture, _, unused, _, _ = b21.setup(directory, case, target, side)
        original = unused._external_inputs
        inputs = EntryInputs(**{f.name:getattr(original, f.name) for f in fields(b21.Inputs)}, case=case)
    sup = b1.supervisor.OperationsSupervisor(unused._configuration, inputs, unused.health,
        expected_generation=unused._generation)
    return place, sup, b1.Driver(sup, now=(unused._generation or 0)+1)


def run_case(directory, case):
    place, sup, driver = setup(directory, case)
    try:
        driver.until(lambda result:(place/'reached.json').exists())
        reached = json.loads((place/'reached.json').read_text())
        old_pid, old_fence, budget = sup.process.pid, sup.fence, sup.store.snapshot()
        driver.mono = sup._deadline
        check(case+':original-watchdog-loss', driver.tick().state == 'TERMINATING')
        sup.process.join(timeout=5)
        check(case+':death-confirmed-before-replacement', sup.process.exitcode is not None)
        dead_exit = sup.process.exitcode
        driver.now += 1
        works, prior = [], sup.completed_steps
        def finished(result):
            nonlocal prior
            if sup.completed_steps > prior:
                works.append(sup.last_work)
                prior = sup.completed_steps
            return (place/'finished.json').exists()
        driver.until(finished)
        recovered = json.loads((place/'recovered.json').read_text())
        ending = json.loads((place/'finished.json').read_text())
        final = ending['facts']
        reads = [json.loads(p.read_text()) for p in sorted(place.glob('readiness-*.json'))]
        check(case+':fresh-A1-A2-runtime', recovered['runtime'] == 'ColdRuntimeV01'
            and recovered['fence']['generation'] == old_fence.generation+1
            and recovered['fence']['process_identity'] != old_fence.process_identity and sup.process.pid != old_pid)
        check(case+':original-budget-one-replacement', sup.store.snapshot()['attempts'] == budget['attempts']+1
            and sup.store.snapshot()['budget_id'] == budget['budget_id'])
        check(case+':durable-reconstruction', recovered['durable']['economic_sha256'] == reached['durable']['economic_sha256'])
        replay = json.loads((place/'historical-replays.json').read_text())
        check(case+':history-not-permission', not recovered['audit_permission']
            and not recovered['reconstruction']['grants_permission'] and replay['unchanged'])
        check(case+':read-before-every-replacement-step', len(reads) == len(works)+1
            and all(r['inputs']['entry'] for r in reads))
        check(case+':current-original-common-cut', all(r['barrier']['owner_fence'] == recovered['fence']
            and r['barrier']['reconstruction']['consumer_cut'] == r['facts']['reconstruction']['consumer_cut']
            and r['barrier']['authority_digest'] == r['facts']['authority_digest'] for r in reads))
        check(case+':readiness-pure-no-permission', all(r['read_unchanged'] and not r['barrier']['grants_permission']
            and all(not r['barrier'][k][p] for k in ('entry','protective')
                for p in ('grants_permission','may_sign','may_send')) for r in reads))
        check(case+':sqlite-integrity', all(r['facts']['durable']['integrity'] == [['ok']]
            and not r['facts']['durable']['foreign_keys'] for r in reads))
        check(case+':no-unexpected-send', not (place/'unexpected-send.json').exists())
        expected_works = {'no-position':['ENTRY_ADMITTED','NEED_EXECUTION'],
            'reserved':['NEED_EXECUTION','NEED_EXECUTION'],
            'possible-send':['NEED_RECONCILIATION','NEED_RECONCILIATION','RECONCILED','NEED_RECONCILIATION'],
            'finalized-buy':['NEED_APPLICATION','APPLIED','ENTRY_HELD']}[case]
        check(case+':original-continuation', works == expected_works)
        if case == 'no-position':
            check(case+':pending-canonical-candidate-ready', not recovered['positions'] and not recovered['reservations']
                and bool(recovered['reconstruction']['queued_roots']) and reads[0]['barrier']['entry']['ready']
                and reads[0]['barrier']['protective']['state'] == 'NOT_REQUIRED')
            check(case+':exactly-one-original-admission', not recovered['admissions'] and len(final['admissions']) == 1
                and len(final['reservations']) == 1 and not final['attempts'])
        else:
            check(case+':entry-always-held', all(not r['barrier']['entry']['ready'] for r in reads))
            check(case+':same-original-admission', final['admissions'] == recovered['admissions']
                and len(final['admissions']) == 1)
        if case in ('no-position','reserved'):
            check(case+':no-execution-or-fabricated-position', not final['attempts'] and not final['positions']
                and final['reconstruction']['obligation_id'] is None)
        if case == 'reserved':
            check(case+':retained-reservation-action', recovered['reservations'] == final['reservations']
                and recovered['reconstruction']['entry_action_id'] == final['reconstruction']['entry_action_id']
                and recovered['durable']['economic_sha256'] == final['durable']['economic_sha256'])
        if case == 'possible-send':
            check(case+':exact-signed-claim-retained', len(final['attempts']) == 1
                and final['envelopes'] == recovered['envelopes'] and final['stages'] == recovered['stages']
                and sum(s['external_reference'] == b22.sender._CLAIM for s in final['stages']) == 1)
            check(case+':unknown-no-time-release', final['finalities'][0]['positive_finality'] is None
                and final['attempts'][0]['lane_held'] and not final['applications']
                and final['reservations'] == recovered['reservations'] and final['custody'] == recovered['custody']
                and reads[-1]['at'] == NOW+7200)
            check(case+':original-current-history', len(final['chain_receipts']) == 1
                and not recovered['chain_receipts'] and final['finalities'] != recovered['finalities'])
        if case == 'finalized-buy':
            check(case+':retained-finality-awaits-wallet', len(recovered['chain_receipts']) == 1
                and recovered['finalities'][0]['positive_finality'] is not None and not recovered['applications']
                and recovered['attempts'][0]['lane_held'] and not reads[0]['inputs']['application_wallet']
                and reads[1]['inputs']['application_wallet'] and not reads[1]['facts']['applications'])
            check(case+':exactly-one-original-application', len(final['applications']) == 1
                and not final['attempts'][0]['lane_held'] and final['attempts'][0]['economically_applied']
                and len(final['attempts']) == 1 and recovered['envelopes'] == final['envelopes']
                and recovered['message_receipts'] == final['message_receipts'])
            application = final['applications'][0]
            check(case+':original-position-exact-units', final['position']['remaining_units']
                == application['decision']['proposal']['base_units_delta'] > 0
                and final['reconstruction']['position_id'] is not None
                and reads[-1]['barrier']['protective']['state'] == 'MONITORING'
                and 'EXISTING_EXPOSURE_REQUIRES_PROTECTION_AND_RETIREMENT_FIRST' in reads[-1]['barrier']['entry']['reasons'])
            historical = json.loads(next(place.glob('application-replay-*.json')).read_text())
            check(case+':application-replay-pure', historical['equal'] and historical['unchanged'])
            check(case+':one-original-send-no-replacement', ending['send_calls'] == 0
                and len(list(place.glob('sign-*.json'))) == 1 and len(list(place.glob('send-*.json'))) == 1)
        record = dict(case=case, reached=reached, old_pid=old_pid, old_fence=asdict(old_fence),
            dead_exit=dead_exit, budget_before=budget, budget_after=sup.store.snapshot(),
            recovered=recovered, works=works, readiness=reads, ending=ending,
            checks={k:v for k,v in CHECKS.items() if k.startswith(case+':')})
        write(place/'result.json', record)
        print(json.dumps(dict(case=case, works=works, checks=len(record['checks']))), flush=True)
        return record
    finally:
        b1.cleanup(sup)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence-dir', type=Path, required=True)
    directory = parser.parse_args().evidence_dir.resolve()
    if directory == ROOT or ROOT in directory.parents:
        raise ValueError('external disposable evidence directory required')
    directory.mkdir(parents=True, exist_ok=False)
    records = [run_case(directory, case) for case in CASES]
    sources = {str(Path(m.__file__).resolve().relative_to(ROOT)).replace('\\','/'):
        hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest() for m in tuple(sys.modules.values())
        if getattr(m, '__file__', None) and ROOT in Path(m.__file__).resolve().parents
        and Path(m.__file__).suffix == '.py'}
    write(directory/'campaign.json', dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW',
        cases=records, checks=CHECKS, source_sha256=sources))
    print(json.dumps(dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW', cases=len(records),
        checks=len(CHECKS), all_checks=all(CHECKS.values()))), flush=True)


if __name__ == '__main__':
    main()
