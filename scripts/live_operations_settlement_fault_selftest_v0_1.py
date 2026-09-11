"""B2.4 supervised original settlement/retirement commit cuts, mock I/O only.

Before/after surround original _commit (pending transaction / returned commit
and cache publication). No crash inside commit or assigned economic truth.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass, fields, replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import live_operations_finality_fault_selftest_v0_1 as b23
import live_runtime_continuation_selftest_v0_1 as c3

b21, b1, rt, NOW = b23.b21, b23.b1, b23.rt, b23.NOW
Ledger, durable, write, facts = b23.Ledger, b23.durable, b23.write, b23.facts
ROOT = Path(__file__).resolve().parents[1]
CHECKS = {}
CASES = tuple((kind+'-'+edge, kind, edge) for kind in
    ('buy-application', 'failed-sell-application', 'partial-sell-application', 'retirement')
    for edge in ('before', 'after'))


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


@dataclass
class Inputs(b23.Inputs):
    kind: str = ''
    edge: str = ''
    raw: Path | None = None

    def install_cuts(self):
        self.cut_enabled = True
        original_commit = Ledger._commit
        def commit(repo):
            if self.active == self.target and self.cut_enabled:
                write(self.directory/'commit-before.json', durable(repo.database_path))
                self.pause_commit('before')
            result = original_commit(repo)
            if self.active == self.target and self.cut_enabled:
                self.pause_commit('after')
            return result
        patch.object(Ledger, '_commit', commit).start()
        for name in ('apply_settlement', 'retire'):
            self.wrap_operation(name)

    def wrap_operation(self, name):
        original = getattr(Ledger, name)
        def operation(repo, *args, **kwargs):
            previous = self.active
            self.active = name
            if name == self.target and self.cut_enabled:
                caller = sys._getframe(1).f_code.co_name
                self.runtime_caller = caller
                write(self.directory/'operation-entry.json', facts(self.started, self.root))
                write(self.directory/'operation-input.json', dict(operation=name,
                    input=asdict(args[0]) if name == 'retire' else args[0],
                    support=asdict(args[1] if name == 'retire' else kwargs['support']),
                    ingestion_key=kwargs['ingestion_key']))
            try:
                return original(repo, *args, **kwargs)
            finally:
                self.active = previous
        patch.object(Ledger, name, operation).start()

    def pause_commit(self, edge):
        if edge != self.edge:
            return
        repo = self.started.runtime.ledger
        write(self.directory/'reached.json', dict(operation=self.target, edge=edge,
            runtime_caller=self.runtime_caller, pid=os.getpid(),
            fence=asdict(self.started.audit.owner_fence), in_transaction=repo._conn.in_transaction,
            durable=durable(repo.database_path)))
        b1.wait_file(self.directory/'release')

    def __call__(self, started):
        if not hasattr(self, 'initialized'):
            self.now = NOW+(6 if self.trade_side == 'BUY' else 20)
            self.phase = 0
            self.send_calls, self.requests = 0, []
            self.observe_calls()
            resources = b21.Inputs.__call__(self, started)
            self.f = SimpleNamespace(repo=started.runtime.ledger, runtime=started.runtime,
                domain=started.runtime.ledger.domain, public=self.public, lower=self.lower, raw=self.raw)
            if not self.replacement:
                return resources
            write(self.directory/'reconstructed-settlement.json', facts(started, self.root))
        else:
            self.phase += 1
        repo = self.f.repo
        self.clock_calls = 0
        self.attempt_id = repo._root_attempts(self.root)[-1].preparation.attempt_id
        self.envelope = b23.sender.load_signed_envelope(repo, self.attempt_id)
        state = None
        if not hasattr(self, 'retirement_replayed'):
            state = facts(started, self.root)
            write(self.directory/('state-'+str(os.getpid())+'-'+str(self.phase)+'.json'), state)
        resources = dict(clock=self.clock, source_cut_utc=rt.a3.utc(NOW+1))
        attempt = repo.attempt(self.attempt_id)
        finality = repo.attempt_finality(self.attempt_id)
        self.now = NOW+(14 if self.trade_side == 'BUY' else 28)
        if finality.positive_finality is None:
            self.now = NOW+(12 if self.trade_side == 'BUY' else 26)
            write(self.directory/'before-truth.json', state)
            resources['truth_rpc'] = self.truth_rpc()
        elif attempt.lane_held:
            # Retained finality is necessary but cannot apply without actual
            # current wallet evidence. Let the real next step demonstrate it.
            if not hasattr(self, 'missing_application'):
                self.missing_application = True
                write(self.directory/('finalized-before-application-'+str(os.getpid())+'.json'), state)
                return resources
            self.build_chain()
            chain = repo.chain_receipt(started.runtime.reconstruction_facts().chain_receipt_key)
            resources['application_wallet'] = rt.application_support(self.f, self.actual, chain, self.pre, self.now)
        else:
            self.restore_public_after_application()
            if not hasattr(self, 'application_replayed'):
                self.application_replayed = True
                application = repo.application_receipt(next(a.ingestion_key for a in b23.applications(repo, self.root)
                    if a.attempt_id == self.attempt_id))
                before = durable(repo.database_path)
                replay = repo.apply_settlement(self.attempt_id, chain_receipt_key=application.chain_receipt_key,
                    support=application.support, ingestion_key=application.ingestion_key,
                    recorded_at_utc=application.recorded_at_utc, fence=repo.write_fence())
                write(self.directory/('application-replay-'+str(os.getpid())+'.json'),
                    dict(equal=replay == application, unchanged=before == durable(repo.database_path),
                        application=asdict(application)))
                if self.kind != 'retirement':
                    return resources
            elif self.kind != 'retirement':
                self.finish()
            if repo.reservation(self.root).status != 'RETIRED':
                if not hasattr(self, 'retirement_checked'):
                    self.retirement_checked = True
                    self.retirement_prerequisites()
                self.now, self.clock_calls = NOW+30, 0
                self.cut_enabled = True
                resources['retirement_wallet'] = c3.wallet(self.f, self.now)
            elif not hasattr(self, 'retirement_replayed'):
                self.retirement_replayed = True
                receipt = repo._port_at_sequence(repo.reservation(self.root).retired_sequence)
                before = durable(repo.database_path)
                replay = repo.retire(receipt.retirement, c3.wallet(self.f, NOW+30),
                    ingestion_key=receipt.ingestion_key, fence=repo.write_fence())
                write(self.directory/'retirement-replay.json', dict(equal=replay == receipt,
                    unchanged=before == durable(repo.database_path), receipt=asdict(receipt), facts=facts(started, self.root)))
                # Only disposable raw source fixture rows; original producer,
                # handoff and Runtime determine candidate identity and priority.
                rt.hf.Fixture.append(self.f, [(n+32,rt.hf.MINT_C,k,p)
                    for n,_,k,p in rt.hf.FIRST if n not in (5,7)])
                self.later_steps = 0
                self.now = NOW+36
                resources['source_cut_utc'] = rt.a3.utc(NOW+32)
            else:
                self.now = NOW+36
                resources['source_cut_utc'] = rt.a3.utc(NOW+32)
                self.later_steps += 1
                if hasattr(self, 'later_supplied'):
                    receipts = [asdict(repo.authority_admission_receipt(key)) for (key,) in repo._conn.execute(
                        'SELECT command_id FROM ledger_authority_admissions ORDER BY commit_seq')]
                    write(self.directory/'later-admission-receipts.json', receipts)
                    self.finish()
                if self.later_steps > 128:
                    raise AssertionError('fresh canonical candidate unavailable')
                roots = started.runtime.queued_roots
                if roots:
                    self.later_supplied = True
                    item = repo.candidate(roots[0])
                    write(self.directory/'later-candidate.json', dict(root=roots[0], candidate=asdict(item),
                        facts=facts(started, self.root), source=asdict(started.runtime.source.latest_record()[1])))
                    resources['entry'] = rt.EntryFacts('PUMP', rt.sf.TOKEN_PROGRAM_ID,
                        c3.wallet(self.f, self.now, mint=item.mint))
        return resources

    def restore_public_after_application(self):
        if hasattr(self, 'restored_public'):
            return
        self.restored_public = True
        self.build_chain()
        application = next(a for a in b23.applications(self.f.repo, self.root) if a.attempt_id == self.attempt_id)
        rt.application_support(self.f, self.actual, self.f.repo.chain_receipt(application.chain_receipt_key),
            self.pre, NOW+(14 if self.trade_side == 'BUY' else 28))

    def retirement_prerequisites(self):
        self.cut_enabled = False
        repo = self.f.repo
        clock = lambda: rt.a3.clock(repo, NOW+29)
        before = facts(self.started, self.root)
        missing = self.started.runtime.step(clock=clock, source_cut_utc=rt.a3.utc(NOW+1))
        after_missing = facts(self.started, self.root)
        stale = replace(c3.wallet(self.f, NOW+29), required_min_context_slot=100)
        try:
            self.started.runtime.step(clock=clock, retirement_wallet=stale, source_cut_utc=rt.a3.utc(NOW+1))
        except (ValueError, RuntimeError) as exc:
            stale_reason = str(exc)
        else:
            raise AssertionError('stale retirement support accepted')
        after_stale = facts(self.started, self.root)
        withheld = self.started.runtime.step(clock=clock,
            retirement_wallet=c3.wallet(self.f, NOW+29, incomplete=True), source_cut_utc=rt.a3.utc(NOW+1))
        write(self.directory/('retirement-prerequisites-'+str(os.getpid())+'.json'), dict(before=before,
            missing_work=missing.work, after_missing=after_missing, stale_reason=stale_reason,
            after_stale=after_stale, incomplete_work=withheld.work, after=facts(self.started, self.root)))


def setup(directory, case):
    name, kind, edge = case
    side = 'BUY' if kind == 'buy-application' else 'SELL'
    original_stage = b23.b22.composition.stage_protective_sell
    def stage(repo, binding):
        return original_stage(repo, binding, max_units=repo.position_history(binding.position_id).remaining_units//3)
    with patch.object(b23.b22.composition, 'stage_protective_sell', stage if kind == 'partial-sell-application' else original_stage):
        place, f, unused, _ = b23.setup(directory,
            (name, side, 'b24-only', kind == 'failed-sell-application', None))
    old = unused._external_inputs
    inputs = Inputs(**{field.name:getattr(old,field.name) for field in fields(b23.Inputs)},
        kind=kind, edge=edge, raw=f.raw)
    inputs.target = 'retire' if kind == 'retirement' else 'apply_settlement'
    sup = b1.supervisor.OperationsSupervisor(unused._configuration, inputs, unused.health,
        expected_generation=unused._generation)
    return place, f, sup, b1.Driver(sup, now=unused._generation+1)


def run_case(directory, case):
    name, kind, edge = case
    place, f, sup, driver = setup(directory, case)
    try:
        driver.until(lambda result:(place/'reached.json').exists())
        reached = json.loads((place/'reached.json').read_text())
        entry = json.loads((place/'operation-entry.json').read_text())
        before_truth = json.loads((place/'before-truth.json').read_text())
        old_pid, old_fence, budget = sup.process.pid, sup.fence, sup.store.snapshot()
        driver.mono = sup._deadline
        check(name+':watchdog-terminates', driver.tick().state == 'TERMINATING')
        sup.process.join(timeout=5)
        check(name+':death-confirmed', sup.process.exitcode is not None)
        driver.now += 1
        works, prior = [], sup.completed_steps
        def finished(result):
            nonlocal prior
            if sup.completed_steps > prior:
                works.append(sup.last_work)
                prior = sup.completed_steps
            return (place/'finished.json').exists()
        # Original Driver retains its per-progress timeout. A bounded source
        # page continuation can require several steps after retirement.
        for _ in range(140):
            completed = sup.completed_steps
            driver.until(lambda result: finished(result) or sup.completed_steps > completed)
            if (place/'finished.json').exists():
                break
        else:
            raise AssertionError('bounded replacement continuation exhausted')
        recovered = json.loads((place/'reconstructed-settlement.json').read_text())
        ending = json.loads((place/'finished.json').read_text())
        final = ending['facts']
        attempt_id = before_truth['attempts'][-1]['preparation']['attempt_id']
        check(name+':actual-Runtime-Ledger-cut', reached['runtime_caller'] == ('_retire' if kind == 'retirement' else '_truth')
            and reached['operation'] == ('retire' if kind == 'retirement' else 'apply_settlement')
            and reached['edge'] == edge and reached['in_transaction'] == (edge == 'before'))
        check(name+':fresh-owned-original-runtime', recovered['runtime'] == 'ColdRuntimeV01'
            and recovered['fence']['generation'] == old_fence.generation+1 and sup.process.pid != old_pid
            and recovered['fence']['process_identity'] != old_fence.process_identity)
        check(name+':same-budget-one-replacement', sup.store.snapshot()['attempts'] == budget['attempts']+1
            and sup.store.snapshot()['budget_id'] == budget['budget_id'])
        check(name+':exact-committed-reconstruction', reached['durable']['economic_sha256']
            == recovered['durable']['economic_sha256'])
        check(name+':atomic-commit-view', (reached['durable']['economic_sha256']
            == json.loads((place/'commit-before.json').read_text())['economic_sha256']) == (edge == 'before'))
        history = json.loads((place/'historical-replays.json').read_text())
        check(name+':no-reconstructed-permission', not recovered['audit_permission']
            and not recovered['reconstruction']['grants_permission'] and history['unchanged'])
        check(name+':original-attempt-message-Authority-history', all(final[key] == before_truth[key]
            for key in ('envelopes','stages','message_receipts')) and len(final['attempts']) == len(before_truth['attempts']))
        check(name+':one-original-send-no-replacement-send', ending['send_calls'] == 0
            and len(list(place.glob('sign-*.json'))) == 1 and len(list(place.glob('send-*.json'))) == 1)
        application = [a for a in final['applications'] if a['attempt_id'] == attempt_id]
        check(name+':one-original-application', len(application) == 1)
        proposal = application[0]['decision']['proposal']
        actual = json.loads(next(place.glob('actual-*.json')).read_text())
        check(name+':actual-fee-and-native-delta-once', final['custody']['network_fees_paid_lamports']
            == before_truth['custody']['network_fees_paid_lamports']+actual['transaction_fee']
            and final['custody']['native_lamports'] == before_truth['custody']['native_lamports']+proposal['native_wallet_delta']
            and proposal['native_wallet_delta'] == actual['post_native']-actual['pre_native']
            and proposal['transaction_fee_lamports'] == actual['transaction_fee'])
        check(name+':actual-base-and-venue-fees', proposal['base_units_delta']
            == (0 if kind == 'failed-sell-application' else actual['actual_base']*(1 if kind == 'buy-application' else -1))
            and proposal['venue_fee_units'] == (0 if kind == 'failed-sell-application' else actual['venue_fees'])
            and application[0]['decision']['disposition'] == ('FINALIZED_FAILURE_APPLIED'
                if kind == 'failed-sell-application' else 'FINALIZED_SUCCESS_APPLIED')
            and sum(final['custody'][key]-before_truth['custody'][key] for key in
                ('venue_fees_paid_sol_lamports','venue_fees_paid_wsol_units')) == proposal['venue_fee_units'])
        replays = list(place.glob('application-replay-*.json'))
        check(name+':application-replay-present', bool(replays))
        for index,path in enumerate(replays):
            replay = json.loads(path.read_text())
            check(name+':exact-application-replay-'+str(index), replay['equal'] and replay['unchanged'])
        finalized = [json.loads(p.read_text()) for p in place.glob('finalized-before-application-*.json')]
        check(name+':positive-truth-alone-retains-lane-capacity', bool(finalized) and all(
            s['attempts'][-1]['lane_held'] and not s['attempts'][-1]['economically_applied']
            and s['finalities'][-1]['positive_finality'] is not None and len(s['reservations']) == 1 for s in finalized))
        if kind != 'retirement':
            check(name+':application-atomic-postings-and-lane', recovered['attempts'][-1]['economically_applied'] == (edge == 'after')
                and recovered['attempts'][-1]['lane_held'] == (edge == 'before')
                and len(recovered['applications']) == len(before_truth['applications'])+int(edge == 'after'))
            expected = 'ENTRY_HELD' if kind == 'buy-application' else ('NEED_EXECUTION' if kind == 'failed-sell-application' else 'PROTECTIVE_ACTION_STAGED')
            check(name+':actual-recovery-work', works == (['NEED_APPLICATION','APPLIED',expected] if edge == 'before' else [expected]))
            check(name+':no-early-capacity-release', len(final['reservations']) == 1
                and final['reservations'][0]['reservation_id'] == entry['reservations'][0]['reservation_id'])
            if kind != 'buy-application':
                expected_units = before_truth['position']['remaining_units']+proposal['base_units_delta']
                check(name+':exact-original-residual', final['position']['remaining_units'] == expected_units and expected_units > 0)
                check(name+':original-protection-survives', final['custody']['protections'][0]['handoff']
                    == before_truth['custody']['protections'][0]['handoff']
                    and final['reconstruction']['obligation_id'] == before_truth['reconstruction']['obligation_id']
                    and recovered['reconstruction']['binding_id'] == before_truth['reconstruction']['binding_id']
                    and recovered['reconstruction']['exit_cut_digest'] == entry['reconstruction']['exit_cut_digest'])
                if kind == 'partial-sell-application':
                    check(name+':partial-original-input', -proposal['base_units_delta'] == before_truth['position']['remaining_units']//3)
        else:
            check(name+':retirement-atomic-capacity', len(recovered['reservations']) == int(edge == 'before')
                and recovered['position']['status'] == ('RETIRED' if edge == 'after' else entry['position']['status']))
            replay = json.loads((place/'retirement-replay.json').read_text())
            receipt = replay['receipt']
            check(name+':exact-retirement-replay', replay['equal'] and replay['unchanged'])
            check(name+':actual-terminal-proof-before-release', receipt['retirement_disposition'] == 'RETIRED'
                and receipt['retirement']['terminal_attempt_id'] == attempt_id
                and receipt['sequence'] > application[0]['sequence'] and final['position']['remaining_units'] == 0
                and not final['reservations'] and not final['positions'])
            prerequisites = list(place.glob('retirement-prerequisites-*.json'))
            check(name+':retirement-prerequisites-present', bool(prerequisites))
            for index,path in enumerate(prerequisites):
                p = json.loads(path.read_text())
                check(name+':original-retirement-prerequisites-'+str(index), p['missing_work'] == 'NEED_RETIREMENT'
                    and 'BEHIND_RESULTING_SETTLEMENT_CUT' in p['stale_reason'] and p['incomplete_work'] == 'RETIREMENT_WITHHELD'
                    and all(len(p[s]['reservations']) == 1 for s in ('before','after_missing','after_stale','after'))
                    and p['after_missing']['durable']['economic_sha256'] == p['after_stale']['durable']['economic_sha256'])
            candidate = json.loads((place/'later-candidate.json').read_text())
            admission = json.loads((place/'later-admission-receipts.json').read_text())[-1]
            write(place/'later-admission.json', admission)
            check(name+':fresh-canonical-candidate-after-retirement', candidate['root'] != f.root
                and candidate['candidate']['generated_at_us'] == (NOW+32)*1000000
                and candidate['source']['disposition'] == 'HEALTHY')
            check(name+':capacity-is-not-current-Authority', works[-1] == 'ENTRY_DENIED'
                and not final['reservations'] and len(final['applications']) == len(before_truth['applications'])+1)
            check(name+':actual-current-grant-denial-receipt', admission['request']['root_id'] == candidate['root']
                and set(admission['risk']['reasons']) == {'ENTRY_ELIGIBILITY_NOT_PROVEN',
                    'GRANT_POLICY_OR_ROOT_CONFLICT','ONE_TIME_ENTRY_GRANT_ALREADY_CONSUMED'}
                and admission['risk']['current_wallet_comparison_digest'] is not None
                and admission['risk']['occupied_roots'] == [] and admission['risk']['outstanding_native_lamports'] == 0
                and admission['eligibility']['original']['source']['disposition'] == 'HEALTHY'
                and admission['sequence'] > receipt['sequence'] and not admission['grants_message_permission'])
        states = [recovered,final]+[json.loads(p.read_text()) for p in place.glob('state-*.json')]
        check(name+':all-observed-durable-integrity', all(s['durable']['integrity'] == [['ok']]
            and not s['durable']['foreign_keys'] for s in states))
        record = dict(case=case, before_truth=before_truth, operation_entry=entry, reached=reached,
            reconstructed=recovered, final=final, runtime_work=works,
            invariants={k:v for k,v in CHECKS.items() if k.startswith(name+':')})
        write(place/'result.json', record)
        print(json.dumps(dict(case=name, runtime_work=works, checks=len(record['invariants']))), flush=True)
        return record
    finally:
        b1.cleanup(sup)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence-dir', required=True, type=Path)
    parser.add_argument('--case', choices=[c[0] for c in CASES])
    args = parser.parse_args()
    directory = args.evidence_dir.resolve()
    if directory == ROOT or ROOT in directory.parents:
        raise ValueError('external disposable evidence directory required')
    directory.mkdir(parents=True, exist_ok=False)
    records = [run_case(directory,c) for c in CASES if args.case is None or args.case == c[0]]
    write(directory/'cut-manifest.json', dict(version='B2.4', cases=records, checks=CHECKS))
    print(json.dumps(dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW', cases=len(records),
        checks=len(CHECKS), all_checks=all(CHECKS.values()))), flush=True)


if __name__ == '__main__':
    main()
