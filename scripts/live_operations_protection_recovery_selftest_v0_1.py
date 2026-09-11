"""B3.2 three fresh protection consumers; disposable stores/children/mock I/O.

Original APIs create every acquisition, evaluation, obligation and action.
Current A3 reads classify evidence; original Runtime independently does work.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_finality_fault_selftest_v0_1 as b23
import live_operations_readiness_selftest_v0_1 as a3test
from live import operations_readiness_v0_1 as readiness

b21, b1, rt, NOW = b23.b21, b23.b1, b23.rt, b23.NOW
composition, write, durable = b21.composition, b23.write, b21.durable
CHECKS = {}
CASES = ('acquired-open', 'obligation-undispatched', 'fallback-already-due')


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def facts(started, root):
    value = b23.facts(started, root)
    repo, binding = started.runtime.ledger, started.runtime.position_binding
    value['startup_audit'] = asdict(started.audit)
    value['binding'] = asdict(binding)
    value['deadline'] = dict(fallback_deadline_us=binding.fallback_deadline_us,
        fallback_fire_boundary_us=binding.fallback_fire_boundary_us)
    value['exit_records'] = [asdict(r) for r in repo.exit_records(binding.binding_id)]
    value['protection'] = (None if value['reconstruction']['protective_state'] is None
        else asdict(rt.protective_outcome(repo, binding)))
    value['admissions'] = [asdict(repo.authority_admission_receipt(key)) for (key,) in repo._conn.execute(
        'SELECT command_id FROM ledger_authority_admissions ORDER BY commit_seq')]
    return value


def sample(owner, resources, label, reduction=None):
    repo = owner.started.runtime.ledger
    before = durable(repo.database_path)
    barrier = readiness.evaluate(owner.started, clock=lambda: rt.a3.clock(repo, owner.now),
        entry=resources['entry'], reduction=reduction)
    record = dict(pid=os.getpid(), label=label, at=owner.now, barrier=asdict(barrier),
        read_unchanged=before == durable(repo.database_path), facts=facts(owner.started, owner.root),
        inputs=dict(entry=True, reduction=None if reduction is None else asdict(reduction),
            execution='execution' in resources))
    write(owner.directory/('readiness-'+label+'.json'), record)
    return record


@dataclass
class Inputs(b21.Inputs):
    case: str = ''
    public: object = None
    lower: object = None
    root: str = ''

    clock = b23.b22.Inputs.clock

    def install_cuts(self):
        # The loss is a consumer cut between returned original owner APIs and
        # the next supervised Runtime step, with no open economic transaction.
        pass

    def __call__(self, started):
        if not hasattr(self, 'initialized'):
            self.now, self.phase, self.send_calls = NOW+14, 0, 0
            b21.Inputs.__call__(self, started)
            self.f = SimpleNamespace(repo=started.runtime.ledger, runtime=started.runtime,
                domain=started.runtime.ledger.domain, public=self.public, lower=self.lower, name=self.case)
            if not self.replacement:
                if self.case != 'acquired-open':
                    at = NOW+(16 if self.case == 'obligation-undispatched' else 14)
                    binding = started.runtime.position_binding
                    evaluation = composition.commit_exit_evaluation(self.f.repo, binding,
                        command_id='b32-original-'+self.case, timer_fence_utc=rt.a3.utc(at))
                    obligation = composition.ensure_protective_obligation(self.f.repo, binding,
                        recorded_at_utc=rt.a3.utc(at))
                    write(self.directory/'original-protection-input.json', dict(at=at,
                        evaluation=asdict(evaluation), obligation=asdict(obligation)))
                    self.now = NOW+16
                state = facts(started, self.root)
                write(self.directory/'reached.json', dict(pid=os.getpid(), at=self.now,
                    boundary='original-owner-APIs-returned-before-next-Runtime-step',
                    in_transaction=self.f.repo._conn.in_transaction, facts=state, durable=state['durable']))
                b1.wait_file(self.directory/'release')
            write(self.directory/'recovered.json', facts(started, self.root))
        else:
            self.phase += 1
        # Open acquisition gets one original monitoring step before its due
        # sample. Both retained-obligation cases are already due at first read.
        labels = (('monitoring',) if self.case == 'acquired-open' else ()) + (
            'due', 'missing', 'current-no-ports', 'stale', 'current-dispatch', 'terminal')
        label = labels[self.phase]
        self.now = NOW+dict(monitoring=14, due=16, missing=17,
            **{'current-no-ports':20, 'stale':100, 'current-dispatch':101, 'terminal':102})[label]
        self.clock_calls = 0
        resources = dict(clock=self.clock, source_cut_utc=rt.a3.utc(NOW+1), entry=self.entry)
        reduction = None
        if label in ('current-no-ports', 'current-dispatch'):
            action = rt.protective_outcome(self.f.repo, started.runtime.position_binding).action
            proof = a3test.public_reduction(self.f, action, self.now, 181 if label == 'current-no-ports' else 182)
            self.proof = proof
            reduction = proof.evidence
            write(self.directory/('public-reduction-'+label+'.json'), asdict(proof))
        elif label == 'stale':
            reduction = self.proof.evidence
        if label == 'current-dispatch':
            # Fresh independently produced Q1 evidence uses Runtime's own +1us
            # SELL intent; the A3 classification is never supplied as a grant.
            seed = b23.seed_for(self.f, action, self.now, 183)
            read = b21.ReadBoundary(seed)
            rpc = rt.ExecutionReadOnlyRpc('https://invalid.local', rt.a3.PROFILE,
                now_us=read.now, transport=rt.httpx.MockTransport(read))
            transport = b23.sender.SolanaSendTransport('https://invalid.local', rt.a3.PROFILE,
                transport=rt.httpx.MockTransport(self.external_send))
            resources['execution'] = rt.ExecutionPorts(rpc, seed.evidence.wallet,
                seed.evidence.quote_policy, seed.evidence.plan_policy, rt.ComputeBudget(250000,1000),
                rt.AutonomousLocalSigner(rt.Keypair.from_bytes(self.key_bytes)), transport,
                lambda: seed.evidence.simulation.leases[0].observed_at_us)
        sample(self, resources, label, reduction)
        if label == 'terminal':
            write(self.directory/'finished.json', dict(facts=facts(started,self.root), send_calls=self.send_calls))
            b1.wait_file(self.directory/'finish')
        return resources

    def external_send(self, request):
        self.send_calls += 1
        payload = json.loads(request.content)
        pending = self.f.repo.consumer_snapshot()['pending_attempts'][0]
        envelope = b23.sender.load_signed_envelope(self.f.repo, pending)
        assert self.replacement and self.send_calls == 1
        assert payload['method'] == 'sendTransaction' and payload['params'][0] == envelope.signed_wire_base64
        assert all(rt.VersionedTransaction.from_bytes(base64.b64decode(payload['params'][0])).verify_with_results())
        write(self.directory/'send.json', dict(pid=os.getpid(), request=payload, attempt_id=pending,
            signature=envelope.primary_signature, authoritative_chain_truth=False))
        return rt.httpx.Response(200,json={'jsonrpc':'2.0','id':payload['id'],'result':envelope.primary_signature})


def setup(directory, case):
    place, f, config, unused, _, _ = b21.setup(directory, case, 'b32-protection', 'after')
    old = unused._external_inputs
    key = rt.Keypair.from_bytes(old.key_bytes)
    with patch.object(rt.sf, 'WALLET', str(key.pubkey())), patch.object(rt.sf.plans, 'ACTOR', str(key.pubkey())):
        b21.a2.restart(f)
        buy = f.repo.action(f.runtime.reconstruction_facts().entry_action_id)
        submitted, _, sends = rt.execution(f, key, buy, at=NOW+6, number=171)
        _, actual, scenario, pre = rt.original_chain(f, submitted, NOW+6)
        reconciled, _ = rt.reconcile(f, scenario, NOW+12)
        support = rt.application_support(f, actual, f.repo.chain_receipt(reconciled.receipt_key), pre, NOW+14)
        check(case+':original-acquisition', f.step(NOW+14, application_wallet=support).work == 'APPLIED'
            and len(sends.requests) == 1)
        write(place/'acquisition.json', dict(facts=facts(f.started, f.root),
            actual_base=actual.actual_base, send_requests=sends.requests))
        generation = f.operations.snapshot()['generation']
        old.initial_generation, old.seed = generation+1, None
        inputs = Inputs(**{field.name:getattr(old,field.name) for field in fields(b21.Inputs)},
            case=case, public=f.public, lower=f.lower, root=f.root)
        b21.a2.close(f)
    sup = b1.supervisor.OperationsSupervisor(config, inputs, unused.health, expected_generation=generation)
    return place, sup, b1.Driver(sup,now=generation+1)


def run_case(directory, case):
    place, sup, driver = setup(directory,case)
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
        final, original = ending['facts'], reached['facts']
        reads = sorted((json.loads(p.read_text()) for p in place.glob('readiness-*.json')), key=lambda r:r['at'])
        by_label = {r['label']:r for r in reads}
        check(case+':fresh-A1-A2-cold-Runtime', recovered['runtime'] == 'ColdRuntimeV01'
            and recovered['fence']['generation'] == old_fence.generation+1 and sup.process.pid != old_pid
            and recovered['fence']['process_identity'] != old_fence.process_identity
            and recovered['startup_audit']['owner_fence'] == recovered['fence'])
        check(case+':original-budget-one-replacement', sup.store.snapshot()['attempts'] == budget['attempts']+1
            and sup.store.snapshot()['budget_id'] == budget['budget_id'])
        check(case+':exact-durable-reconstruction', recovered['durable']['economic_sha256'] == reached['durable']['economic_sha256'])
        check(case+':original-cut-restored', all(recovered[k] == original[k]
            for k in ('binding','deadline','exit_records','position','applications','custody')))
        if original['protection'] is not None:
            before_protection, after_protection = original['protection'], recovered['protection']
            check(case+':same-protection-current-writer-generation',
                {k:v for k,v in before_protection.items() if k != 'fence'}
                == {k:v for k,v in after_protection.items() if k != 'fence'}
                and all(before_protection['fence'][k] == after_protection['fence'][k]
                    for k in ('economic_domain_id','last_receipt_digest','revision'))
                and after_protection['fence']['generation'] == before_protection['fence']['generation']+1
                and after_protection['fence']['generation_digest'] != before_protection['fence']['generation_digest'])
        check(case+':retained-exact-binding-deadline-history', all(r['facts'][k] == original[k]
            for r in reads for k in ('binding','deadline','applications','reservations','admissions')))
        # Original handoff legitimately changes precisely these four position
        # attributes, plus custody's positions/protections. Original execution
        # also retains its pending-effects wallet comparison, checked below.
        handoff_fields = {'has_protective_handoff','protective_binding_id','status','usable'}
        check(case+':retained-exact-economic-exposure', all(
            {k:v for k,v in r['facts']['position'].items() if k not in handoff_fields}
            == {k:v for k,v in original['position'].items() if k not in handoff_fields}
            and {k:v for k,v in r['facts']['custody'].items() if k not in ('positions','protections','latest_comparison')}
            == {k:v for k,v in original['custody'].items() if k not in ('positions','protections','latest_comparison')}
            for r in reads))
        check(case+':only-original-handoff-bookkeeping', all(
            r['facts']['position'] == final['position']
            and r['facts']['custody']['positions'] == final['custody']['positions']
            and r['facts']['custody']['protections'] == final['custody']['protections']
            for r in reads if r['facts']['protection'] is not None)
            and final['position']['has_protective_handoff'] and final['position']['usable']
            and final['position']['status'] == 'OWNED_PROTECTED'
            and len(final['custody']['protections']) == 1)
        check(case+':original-knowledge-record-prefix', all(r['facts']['exit_records'][:len(original['exit_records'])]
            == original['exit_records'] for r in reads))
        history = json.loads((place/'historical-replays.json').read_text())
        check(case+':history-not-permission', history['unchanged'] and not recovered['audit_permission']
            and not recovered['reconstruction']['grants_permission'])
        check(case+':current-read-before-every-step', len(reads) == len(works)+1
            and all(r['barrier']['owner_fence'] == recovered['fence']
                and r['barrier']['reconstruction']['consumer_cut'] == r['facts']['reconstruction']['consumer_cut']
                and r['barrier']['authority_digest'] == r['facts']['authority_digest'] for r in reads))
        check(case+':pure-readiness-no-capability', all(r['read_unchanged'] and not r['barrier']['grants_permission']
            and all(not r['barrier'][k][p] for k in ('entry','protective')
                for p in ('grants_permission','may_sign','may_send')) for r in reads))
        check(case+':entry-always-held-with-facts', all(r['inputs']['entry'] and not r['barrier']['entry']['ready'] for r in reads))
        check(case+':due-stays-due', all(r['barrier']['protective']['due'] for r in reads if r['label'] != 'monitoring')
            and by_label['due']['barrier']['protective']['state'] == 'DUE_STAGING_REQUIRED')
        due_evaluation = by_label['missing']['facts']['protection']['obligation']['evaluation']
        due_payload = json.loads(due_evaluation['payload_json'])
        check(case+':original-fallback-evaluation-exact-trigger', due_payload['state']['state'] == 'DUE'
            and due_payload['state']['trigger_at_us'] == original['deadline']['fallback_fire_boundary_us']
            and final['protection']['obligation']['evaluation'] == due_evaluation)
        check(case+':missing-current-evidence-held', not by_label['missing']['barrier']['protective']['ready']
            and 'CURRENT_ORIGINAL_REDUCTION_EVIDENCE_REQUIRED' in by_label['missing']['barrier']['protective']['reasons'])
        check(case+':stale-evidence-held', not by_label['stale']['barrier']['protective']['ready']
            and by_label['stale']['inputs']['reduction'] == by_label['current-no-ports']['inputs']['reduction'])
        check(case+':actual-current-public-PROTECTIVE_READY', all(by_label[k]['barrier']['protective']['ready']
            for k in ('current-no-ports','current-dispatch'))
            and not by_label['current-no-ports']['inputs']['execution'] and by_label['current-dispatch']['inputs']['execution'])
        check(case+':independent-original-continuation', works ==
            (['ENTRY_HELD'] if case == 'acquired-open' else []) +
            ['PROTECTIVE_ACTION_STAGED','NEED_EXECUTION','NEED_EXECUTION','NEED_EXECUTION','SUBMISSION_OBSERVED'])
        check(case+':one-original-BUY-one-protective-SELL', len(recovered['attempts']) == 1
            and len(final['attempts']) == 2 and final['attempts'][0] == recovered['attempts'][0]
            and len(final['applications']) == 1 and ending['send_calls'] == 1)
        sell = final['attempts'][-1]
        comparison = final['custody']['latest_comparison']
        check(case+':only-original-pending-wallet-comparison', all(r['facts']['custody']['latest_comparison']
            == original['custody']['latest_comparison'] for r in reads if r['label'] != 'terminal')
            and comparison['disposition'] == 'PENDING_EFFECTS_UNKNOWN' and not comparison['differences']
            and not comparison['reasons'] and not comparison['proves_external_transfer']
            and comparison['pending_attempts'] == [sell['preparation']['attempt_id']])
        check(case+':original-protective-action-and-units', sell['preparation']['action_id']
            == by_label['current-dispatch']['barrier']['protective']['action_id']
            == by_label['missing']['barrier']['protective']['action_id']
            and final['protection']['action']['input_units'] == original['position']['remaining_units'] > 0)
        check(case+':original-obligation-retained', all(r['facts']['protection']['obligation']['handoff']
            == final['protection']['obligation']['handoff'] for r in reads if r['facts']['protection'] is not None))
        check(case+':one-fresh-original-SIGN-SEND', len(final['message_receipts']) == len(recovered['message_receipts'])+2
            and final['message_receipts'][:len(recovered['message_receipts'])] == recovered['message_receipts']
            and [m['stage'] for m in final['message_receipts'][-2:]] == ['SIGN','SEND']
            and all(m['consumed'] for m in final['message_receipts'][-2:]))
        check(case+':terminal-UNKNOWN-awaits-original-truth', sell['lane_held'] and not sell['economically_applied']
            and final['finalities'][-1]['positive_finality'] is None
            and by_label['terminal']['barrier']['protective']['state'] == 'TRUTH_REQUIRED'
            and not by_label['terminal']['barrier']['protective']['ready'])
        check(case+':sqlite-integrity', all(r['facts']['durable']['integrity'] == [['ok']]
            and not r['facts']['durable']['foreign_keys'] for r in reads))
        if case == 'acquired-open':
            check(case+':unprotected-acquisition-monitored-originally', original['protection'] is None
                and by_label['monitoring']['barrier']['protective']['state'] == 'MONITORING')
        else:
            expected = 'DUE' if case == 'obligation-undispatched' else 'MONITORING'
            check(case+':original-obligation-state-no-dispatch', original['protection']['obligation']['state'] == expected
                and original['protection']['action'] is None and len(original['attempts']) == 1)
            check(case+':loss-already-beyond-original-fallback', reached['at']*1000000 >= original['deadline']['fallback_fire_boundary_us'])
        record = dict(case=case,reached=reached,old_pid=old_pid,old_fence=asdict(old_fence),dead_exit=dead_exit,
            budget_before=budget,budget_after=sup.store.snapshot(),recovered=recovered,works=works,
            readiness=reads,ending=ending,checks={k:v for k,v in CHECKS.items() if k.startswith(case+':')})
        write(place/'result.json',record)
        print(json.dumps(dict(case=case,works=works,checks=len(record['checks']))),flush=True)
        return record
    finally:
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
        if getattr(m,'__file__',None) and ROOT in Path(m.__file__).resolve().parents
        and Path(m.__file__).suffix == '.py'}
    write(directory/'campaign.json',dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW',
        cases=records,checks=CHECKS,source_sha256=sources))
    print(json.dumps(dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW',cases=len(records),
        checks=len(CHECKS),all_checks=all(CHECKS.values()))),flush=True)


if __name__ == '__main__':
    main()
