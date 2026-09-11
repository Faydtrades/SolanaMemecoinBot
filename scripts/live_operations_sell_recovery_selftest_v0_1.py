"""B3.3 four fresh SELL recovery consumers; disposable children/mock I/O only."""
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

import live_operations_protection_recovery_selftest_v0_1 as b32

b23, b21, b1, rt, NOW = b32.b23, b32.b21, b32.b1, b32.rt, b32.NOW
write, durable, facts = b32.write, b32.durable, b32.facts
ROOT = Path(__file__).resolve().parents[1]
CASES = ('sell-claimed-unknown', 'failed-sell-fee', 'partial-sell-residual', 'finalized-sell-unapplied')
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


@dataclass
class Inputs(b23.Inputs):
    def install_cuts(self):
        # Loss between returned original Runtime steps; no open transaction.
        pass

    def __call__(self, started):
        if not hasattr(self, 'initialized'):
            self.now, self.phase, self.send_calls = NOW+20, 0, 0
            self.requests = []
            self.observe_calls()
            resources = b21.Inputs.__call__(self, started)
            self.f = SimpleNamespace(repo=started.runtime.ledger, runtime=started.runtime,
                domain=started.runtime.ledger.domain, public=self.public, lower=self.lower, name=self.case)
            if not self.replacement:
                return resources
            write(self.directory/'recovered.json', facts(started, self.root))
        else:
            self.phase += 1
        repo = self.f.repo
        self.attempt_id = repo._root_attempts(self.root)[1].preparation.attempt_id
        self.envelope = b23.sender.load_signed_envelope(repo, self.attempt_id)
        attempt = repo.attempt(self.attempt_id)
        finality = repo.attempt_finality(self.attempt_id)
        resources = dict(clock=self.clock, source_cut_utc=rt.a3.utc(NOW+1), entry=self.entry)
        if not self.replacement:
            if not (self.directory/'before-truth.json').exists():
                write(self.directory/'before-truth.json', facts(started,self.root))
            if self.case == 'sell-claimed-unknown':
                self.pause()
            if finality.positive_finality is None:
                self.now = NOW+26
                resources['truth_rpc'] = self.truth_rpc()
            elif self.case == 'finalized-sell-unapplied':
                self.pause()
            elif attempt.lane_held:
                self.now = NOW+28
                resources['application_wallet'] = self.support(self.now)
            else:
                self.pause()
            self.clock_calls = 0
            return resources

        labels = {
            'sell-claimed-unknown': ('unknown-late', 'unknown-later', 'terminal'),
            'finalized-sell-unapplied': ('missing-application', 'current-application', 'terminal'),
            'failed-sell-fee': ('missing', 'current-dispatch', 'terminal'),
            'partial-sell-residual': ('stage-residual', 'missing', 'current-dispatch', 'terminal'),
        }[self.case]
        label = labels[self.phase]
        self.now = NOW+(3600*(self.phase+1) if self.case == 'sell-claimed-unknown' else 30+self.phase*2)
        self.clock_calls = 0
        reduction = None
        if self.case != 'sell-claimed-unknown' and attempt.economically_applied:
            if not hasattr(self, 'restored_public'):
                self.support()
                self.restored_public = True
            if not hasattr(self, 'replayed'):
                self.replayed = True
                application = next(a for a in b23.applications(repo,self.root) if a.attempt_id == self.attempt_id)
                before = durable(repo.database_path)
                replay = repo.apply_settlement(self.attempt_id, chain_receipt_key=application.chain_receipt_key,
                    support=application.support, ingestion_key=application.ingestion_key,
                    recorded_at_utc=application.recorded_at_utc, fence=repo.write_fence())
                write(self.directory/'application-replay.json', dict(equal=replay == application,
                    unchanged=before == durable(repo.database_path), application=asdict(application)))
        if label == 'current-application':
            resources['application_wallet'] = self.support(self.now)
        if label == 'current-dispatch':
            action = rt.protective_outcome(repo,started.runtime.position_binding).action
            proof = b32.a3test.public_reduction(self.f,action,self.now,191)
            reduction = proof.evidence
            write(self.directory/'public-reduction.json',asdict(proof))
            seed = b23.seed_for(self.f,action,self.now,192)
            read = b21.ReadBoundary(seed)
            rpc = rt.ExecutionReadOnlyRpc('https://invalid.local',rt.a3.PROFILE,
                now_us=read.now,transport=rt.httpx.MockTransport(read))
            transport = b23.sender.SolanaSendTransport('https://invalid.local',rt.a3.PROFILE,
                transport=rt.httpx.MockTransport(self.external_send))
            resources['execution'] = rt.ExecutionPorts(rpc,seed.evidence.wallet,seed.evidence.quote_policy,
                seed.evidence.plan_policy,rt.ComputeBudget(250000,1000),
                rt.AutonomousLocalSigner(rt.Keypair.from_bytes(self.key_bytes)),transport,
                lambda:seed.evidence.simulation.leases[0].observed_at_us)
        b32.sample(self,resources,label,reduction)
        if label == 'terminal':
            write(self.directory/'finished.json',dict(facts=facts(started,self.root),send_calls=self.send_calls))
            b1.wait_file(self.directory/'finish')
        return resources

    def support(self, at=NOW+28):
        self.build_chain()
        repo = self.f.repo
        applied = next((a for a in b23.applications(repo,self.root) if a.attempt_id == self.attempt_id),None)
        chain = repo.chain_receipt(applied.chain_receipt_key if applied is not None
            else self.started.runtime.reconstruction_facts().chain_receipt_key)
        return rt.application_support(self.f,self.actual,chain,self.pre,at)

    def pause(self):
        state = facts(self.started,self.root)
        write(self.directory/'reached.json',dict(pid=os.getpid(),at=self.now,
            boundary='original-Runtime-return-before-next-step',in_transaction=self.f.repo._conn.in_transaction,
            facts=state,durable=state['durable']))
        b1.wait_file(self.directory/'release')

    def external_send(self, request):
        self.send_calls += 1
        payload = json.loads(request.content)
        pending = self.f.repo.consumer_snapshot()['pending_attempts'][0]
        envelope = b23.sender.load_signed_envelope(self.f.repo,pending)
        assert self.send_calls == 1
        assert not self.replacement or self.case in ('failed-sell-fee','partial-sell-residual')
        assert payload['method'] == 'sendTransaction' and payload['params'][0] == envelope.signed_wire_base64
        assert all(rt.VersionedTransaction.from_bytes(base64.b64decode(payload['params'][0])).verify_with_results())
        write(self.directory/('send-'+str(os.getpid())+'.json'),dict(pid=os.getpid(),request=payload,
            attempt_id=pending,signature=envelope.primary_signature,authoritative_chain_truth=False))
        return rt.httpx.Response(200,json={'jsonrpc':'2.0','id':payload['id'],'result':envelope.primary_signature})

    forbidden_send = external_send


def setup(directory,case):
    original = b32.composition.stage_protective_sell
    def partial(repo,binding):
        return original(repo,binding,max_units=repo.position_history(binding.position_id).remaining_units//3)
    with patch.object(b32.composition,'stage_protective_sell',partial if case == 'partial-sell-residual' else original):
        place, f, unused, _ = b23.setup(directory,(case,'SELL','b33-only',case == 'failed-sell-fee',None))
    old = unused._external_inputs
    inputs = Inputs(**{field.name:getattr(old,field.name) for field in fields(b23.Inputs)})
    sup = b1.supervisor.OperationsSupervisor(unused._configuration,inputs,unused.health,
        expected_generation=unused._generation)
    return place,sup,b1.Driver(sup,now=unused._generation+1)


def run_case(directory,case):
    place,sup,driver = setup(directory,case)
    try:
        driver.until(lambda result:(place/'reached.json').exists())
        reached = json.loads((place/'reached.json').read_text())
        before_truth = json.loads((place/'before-truth.json').read_text())
        old_pid,old_fence,budget = sup.process.pid,sup.fence,sup.store.snapshot()
        driver.mono = sup._deadline
        check(case+':original-watchdog-loss',driver.tick().state == 'TERMINATING')
        sup.process.join(timeout=5)
        check(case+':death-before-replacement',sup.process.exitcode is not None)
        dead_exit = sup.process.exitcode
        driver.now += 1
        works,prior = [],sup.completed_steps
        def finished(result):
            nonlocal prior
            if sup.completed_steps > prior:
                works.append(sup.last_work)
                prior = sup.completed_steps
            return (place/'finished.json').exists()
        driver.until(finished)
        recovered = json.loads((place/'recovered.json').read_text())
        ending = json.loads((place/'finished.json').read_text())
        original,final = reached['facts'],ending['facts']
        reads = sorted((json.loads(p.read_text()) for p in place.glob('readiness-*.json')),key=lambda r:r['at'])
        labels = {r['label']:r for r in reads}
        check(case+':fresh-A1-A2-cold-Runtime',recovered['runtime'] == 'ColdRuntimeV01'
            and recovered['fence']['generation'] == old_fence.generation+1 and sup.process.pid != old_pid
            and recovered['fence']['process_identity'] != old_fence.process_identity
            and recovered['startup_audit']['owner_fence'] == recovered['fence'])
        check(case+':one-original-budget-replacement',sup.store.snapshot()['attempts'] == budget['attempts']+1
            and sup.store.snapshot()['budget_id'] == budget['budget_id'])
        check(case+':full-economic-cut-restored',original['durable']['economic_sha256'] == recovered['durable']['economic_sha256']
            and not reached['in_transaction'])
        check(case+':original-identity-and-economic-records',all(original[k] == recovered[k] for k in
            ('binding','deadline','exit_records','position','applications','custody','attempts','envelopes','stages','message_receipts')))
        history = json.loads((place/'historical-replays.json').read_text())
        check(case+':history-is-not-permission',history['unchanged'] and not recovered['audit_permission']
            and not recovered['reconstruction']['grants_permission'])
        check(case+':every-current-read-pure-no-permission',len(reads) == len(works)+1
            and all(r['read_unchanged'] and not r['barrier']['grants_permission']
                and r['barrier']['owner_fence'] == recovered['fence']
                and r['barrier']['authority_digest'] == r['facts']['authority_digest']
                and r['barrier']['reconstruction']['consumer_cut'] == r['facts']['reconstruction']['consumer_cut']
                and all(not r['barrier'][k][p] for k in ('entry','protective') for p in
                    ('grants_permission','may_sign','may_send')) for r in reads))
        check(case+':current-entry-always-held',all(r['inputs']['entry'] and not r['barrier']['entry']['ready'] for r in reads))
        check(case+':binding-obligation-knowledge-retained',all(r['facts']['binding'] == original['binding']
            and r['facts']['deadline'] == original['deadline']
            and r['facts']['exit_records'][:len(original['exit_records'])] == original['exit_records']
            and r['facts']['protection']['obligation']['handoff'] == original['protection']['obligation']['handoff']
            and r['facts']['reconstruction']['obligation_id'] == original['reconstruction']['obligation_id']
            and r['facts']['reservations'] == original['reservations'] for r in reads))
        check(case+':due-remains-due',all(r['barrier']['protective']['due'] for r in reads))
        check(case+':original-signed-history-prefix',final['envelopes'][:2] == original['envelopes']
            and final['message_receipts'][:len(original['message_receipts'])] == original['message_receipts']
            and len(list(place.glob('send-*.json'))) == 1+ending['send_calls'])
        continuation = case in ('failed-sell-fee','partial-sell-residual')
        check(case+':bounded-SELL-count',len(original['attempts']) == 2
            and len(final['attempts']) == 2+int(continuation) and ending['send_calls'] == int(continuation))
        if case == 'sell-claimed-unknown':
            check(case+':UNKNOWN-retained-without-timer-release',all(r['facts']['durable']['economic_sha256']
                == original['durable']['economic_sha256'] and r['facts']['attempts'][1]['lane_held']
                and r['facts']['finalities'][1]['positive_finality'] is None
                and r['barrier']['protective']['state'] == 'TRUTH_REQUIRED'
                and not r['barrier']['protective']['ready'] for r in reads)
                and reads[-1]['at'] >= NOW+7200)
            check(case+':original-claim-preserved',original['attempts'][1]['recorded_stage'] == 'UNKNOWN'
                and any(s['external_reference'] == b23.sender._CLAIM for s in original['stages']))
        else:
            application = [a for a in final['applications'] if a['attempt_id'] == original['attempts'][1]['preparation']['attempt_id']]
            check(case+':one-SELL-application',len(application) == 1 and len(final['applications']) == 2)
            proposal = application[0]['decision']['proposal']
            actual = json.loads(next(place.glob('actual-*.json')).read_text())
            check(case+':exact-fee-native-and-residual',final['custody']['network_fees_paid_lamports']
                == before_truth['custody']['network_fees_paid_lamports']+actual['transaction_fee']
                and proposal['transaction_fee_lamports'] == actual['transaction_fee']
                and final['custody']['native_lamports'] == before_truth['custody']['native_lamports']+proposal['native_wallet_delta']
                and proposal['native_wallet_delta'] == actual['post_native']-actual['pre_native']
                and final['position']['remaining_units'] == before_truth['position']['remaining_units']+proposal['base_units_delta'])
            replay = json.loads((place/'application-replay.json').read_text())
            check(case+':exact-once-replay',replay['equal'] and replay['unchanged'])
            check(case+':exact-venue-fees-once',proposal['venue_fee_units'] == (0 if case == 'failed-sell-fee' else actual['venue_fees'])
                and sum(final['custody'][k]-before_truth['custody'][k] for k in
                    ('venue_fees_paid_sol_lamports','venue_fees_paid_wsol_units')) == proposal['venue_fee_units'])
            if continuation:
                check(case+':missing-resources-hold-protection',not labels['missing']['barrier']['protective']['ready']
                    and 'CURRENT_ORIGINAL_REDUCTION_EVIDENCE_REQUIRED' in labels['missing']['barrier']['protective']['reasons'])
                check(case+':current-original-ready-and-fresh-authority',labels['current-dispatch']['barrier']['protective']['ready']
                    and [m['stage'] for m in final['message_receipts'][len(original['message_receipts']):]] == ['SIGN','SEND']
                    and all(m['consumed'] for m in final['message_receipts'][-2:]))
                check(case+':actual-residual-action',final['protection']['action']['input_units'] == final['position']['remaining_units'] > 0
                    and final['attempts'][-1]['preparation']['action_id'] == labels['current-dispatch']['barrier']['protective']['action_id'])
                old_attempt,new_attempt = original['attempts'][1],final['attempts'][2]
                check(case+':original-root-bound-new-attempt',final['protection']['action']['root_id'] == original['binding']['root_id']
                    and new_attempt['primary_signature'] != old_attempt['primary_signature']
                    and new_attempt['preparation']['attempt_id'] != old_attempt['preparation']['attempt_id']
                    and new_attempt['preparation']['ordinal'] == (2 if case == 'failed-sell-fee' else 1)
                    and (new_attempt['preparation']['action_id'] == old_attempt['preparation']['action_id'])
                        == (case == 'failed-sell-fee'))
                check(case+':continuation-pending-truth-only',final['attempts'][-1]['lane_held']
                    and not final['attempts'][-1]['economically_applied'] and final['finalities'][-1]['positive_finality'] is None
                    and labels['terminal']['barrier']['protective']['state'] == 'TRUTH_REQUIRED'
                    and not labels['terminal']['barrier']['protective']['ready'])
                if case == 'failed-sell-fee':
                    check(case+':failed-fee-with-no-sold-units',proposal['base_units_delta'] == 0
                        and proposal['venue_fee_units'] == 0 and application[0]['decision']['disposition'] == 'FINALIZED_FAILURE_APPLIED')
                else:
                    check(case+':exact-one-third-partial',-proposal['base_units_delta'] == before_truth['position']['remaining_units']//3
                        and application[0]['decision']['disposition'] == 'FINALIZED_SUCCESS_APPLIED')
            else:
                check(case+':retained-positive-truth-still-encumbered',original['finalities'][1]['positive_finality'] is not None
                    and original['attempts'][1]['lane_held'] and len(original['applications']) == 1
                    and not labels['missing-application']['barrier']['protective']['ready']
                    and not labels['current-application']['barrier']['protective']['ready'])
                check(case+':zero-residual-awaits-original-retirement',final['position']['remaining_units'] == 0
                    and len(final['reservations']) == 1 and labels['terminal']['barrier']['protective']['state'] == 'SATISFIED_AWAITING_RETIREMENT')
        expected = {'sell-claimed-unknown':['NEED_RECONCILIATION','NEED_RECONCILIATION'],
            'failed-sell-fee':['NEED_EXECUTION','SUBMISSION_OBSERVED'],
            'partial-sell-residual':['PROTECTIVE_ACTION_STAGED','NEED_EXECUTION','SUBMISSION_OBSERVED'],
            'finalized-sell-unapplied':['NEED_APPLICATION','APPLIED']}[case]
        check(case+':exact-original-continuation',works == expected)
        check(case+':sqlite-integrity',all(r['facts']['durable']['integrity'] == [['ok']]
            and not r['facts']['durable']['foreign_keys'] for r in reads))
        record = dict(case=case,before_truth=before_truth,reached=reached,old_pid=old_pid,old_fence=asdict(old_fence),
            dead_exit=dead_exit,budget_before=budget,budget_after=sup.store.snapshot(),recovered=recovered,
            works=works,readiness=reads,ending=ending,checks={k:v for k,v in CHECKS.items() if k.startswith(case+':')})
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
        if getattr(m,'__file__',None) and ROOT in Path(m.__file__).resolve().parents and Path(m.__file__).suffix == '.py'}
    write(directory/'campaign.json',dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW',cases=records,
        checks=CHECKS,source_sha256=sources))
    print(json.dumps(dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW',cases=len(records),
        checks=len(CHECKS),all_checks=all(CHECKS.values()))),flush=True)


if __name__ == '__main__':
    main()
