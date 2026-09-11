"""B2.3 original supervised finality recovery; disposable mocked external I/O.

Coverage is explicit original bounded Evidence ingestion, not transaction RPC
absence inference. Wrappers pause original API boundaries, never assign owners'
economic state, finality decisions, Runtime pointers or permissions.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from dataclasses import asdict, dataclass, fields, replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_send_fault_selftest_v0_1 as b22
from live import execution_reconciliation_v0_1 as reconciliation
from live import execution_signer_v0_1 as signer
from live.wallet_evidence_v0_1 import ledger_account_evidence

b21, b1, rt, NOW = b22.b21, b22.b1, b22.rt, b22.NOW
Ledger, sender = b21.Ledger, b22.sender
durable = b21.durable
CHECKS = {}
CASES = (
    ('buy-observation', 'BUY', 'observation-return', False, None),
    ('sell-lookup', 'SELL', 'authoritative-lookup-return', False, None),
    ('sell-failed-retained', 'SELL', 'truth-retained', True, None),
    ('sell-complete-coverage', 'SELL', 'coverage-retained', False, 'complete'),
    ('sell-incomplete-coverage', 'SELL', 'coverage-retained', False, 'incomplete'),
)


def write(path, value):
    def public_bytes(item):
        if isinstance(item, bytes):
            return {'public_bytes_hex': item.hex()}
        raise TypeError(type(item).__name__)
    temporary = path.with_suffix(path.suffix+'.writing')
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2, default=public_bytes), encoding='utf-8')
    temporary.replace(path)


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def facts(started, root):
    value = b21.owned_facts(started)
    repo = started.runtime.ledger
    attempts = repo._root_attempts(root)
    value['attempts'] = [dict(asdict(a), preparation=dict(asdict(a.preparation),
        attempt_id=a.preparation.attempt_id)) for a in attempts]
    value['stages'] = [s.to_record() for a in attempts for s in repo.attempt_stage_inputs(a.preparation.attempt_id)]
    value['envelopes'] = [asdict(sender.load_signed_envelope(repo, a.preparation.attempt_id))
        for a in attempts if a.primary_signature is not None]
    value['finalities'] = [asdict(repo.attempt_finality(a.preparation.attempt_id)) for a in attempts]
    value['chain_receipts'] = [asdict(repo.chain_receipt(key)) for (key,) in repo._conn.execute(
        'SELECT ingestion_key FROM ledger_chain_receipts ORDER BY commit_seq')]
    value['applications'] = [asdict(a) for a in applications(repo, root)]
    action = repo.action(attempts[0].preparation.action_id)
    position = repo.position_history(action.position_id)
    value['position'] = None if position is None else asdict(position)
    value['custody'] = asdict(repo._custody)
    return value


def applications(repo, root):
    ids = {a.preparation.attempt_id for a in repo._root_attempts(root)}
    return sorted((a for a in repo._applications.values() if a.attempt_id in ids), key=lambda a: a.sequence)


def seed_for(f, action, at, number):
    intent = rt.q1._intent(rt.q1.capture_message_context(f.repo, action.action_id), at*1000000+1)
    return rt.a4.evidence(f, action, rt.a5.wallet_scenario(f.public, f.lower), at=at,
        context_slot=f.lower.slot, intent=intent, wallet_floor=f.lower.slot,
        recent_blockhash=rt.sf.bh(number), last_valid_height=f.lower.block_height+30,
        validity_height=f.lower.block_height+1, original_accounts=f.public)[0]


@dataclass
class Inputs(b21.Inputs):
    case: str = ''
    boundary: str = ''
    failed: bool = False
    coverage: str | None = None
    public: object = None
    lower: object = None
    root: str = ''
    trade_side: str = ''

    clock = b22.Inputs.clock

    def __call__(self, started):
        if not hasattr(self, 'initialized'):
            self.now = NOW+(6 if self.trade_side == 'BUY' else 20)
            self.phase = 0
            self.send_calls = 0
            self.requests = []
            self.observations = []
            self.observe_calls()
            resources = b21.Inputs.__call__(self, started)
            self.f = SimpleNamespace(repo=started.runtime.ledger, runtime=started.runtime,
                domain=started.runtime.ledger.domain, public=self.public, lower=self.lower)
            if not self.replacement:
                return resources
            self.attempt_id = started.audit.pending_attempt.preparation.attempt_id
            self.envelope = sender.load_signed_envelope(self.f.repo, self.attempt_id)
            write(self.directory/'reconstructed-finality.json', facts(started, self.root))
        else:
            self.phase += 1
        repo = self.f.repo
        resources = dict(clock=self.clock, source_cut_utc=rt.a3.utc(NOW+1))
        if not self.replacement:
            self.attempt_id = repo.consumer_snapshot()['pending_attempts'][0]
            self.envelope = sender.load_signed_envelope(repo, self.attempt_id)
            write(self.directory/'before-truth.json', facts(started, self.root))
            if self.coverage:
                self.now = NOW+81
                receipt, _ = rt.q3.nonlanding(self.f, self.envelope,
                    partial=self.coverage == 'incomplete', at=self.now, key='b23-coverage')
                raise AssertionError('coverage retained cut did not pause: '+receipt.ingestion_key)
            self.now = NOW+(12 if self.trade_side == 'BUY' else 26)
            resources['truth_rpc'] = self.truth_rpc()
            return resources

        state = facts(started, self.root)
        write(self.directory/('continuation-'+str(self.phase)+'.json'), state)
        original = repo.attempt(self.attempt_id)
        finality = repo.attempt_finality(self.attempt_id)
        if self.coverage == 'incomplete':
            if self.phase >= 2:
                self.finish()
            self.now = NOW+(3600 if self.phase == 0 else 7200)
            # Actual current original wallet evidence is available, but Runtime
            # cannot substitute unchanged balances for authoritative finality.
            root = repo.chain_receipt('b23-coverage').observation.root
            anchor = replace(root, slot=root.slot+2+self.phase, blockhash=rt.sf.bh(root.slot+2+self.phase),
                previous_blockhash=root.blockhash, parent_slot=root.slot,
                block_height=root.block_height+1, block_time=self.now-1)
            snap = repo.consumer_snapshot()
            request = rt.a3.WalletEvidenceRequest(repo.domain.wallet, repo.domain.genesis_hash,
                snap['required_wallet_context_slot'], tuple(rt.a3.ExpectedTokenAccount(a.pubkey, a.mint, a.program)
                    for a in snap['accounts']))
            support = rt.a3.WalletSupportInput(rt.a3.observe(rt.a5.wallet_scenario(self.public, anchor),
                request=request, at=self.now), rt.a3.utc(self.now), request.min_context_slot)
            port = ledger_account_evidence(support.observation, expected_wallet=repo.domain.wallet,
                expected_genesis=repo.domain.genesis_hash, expected_profile_fingerprint=repo.domain.expected_profile_fingerprint,
                required_min_context_slot=request.min_context_slot, now_utc=rt.a3.utc(self.now))
            assert port.account_facts_usable
            resources['application_wallet'] = support
            write(self.directory/('balance-support-'+str(self.phase)+'.json'), dict(support=asdict(support), port=asdict(port)))
        elif finality.positive_finality is None:
            self.now = NOW+(12 if self.trade_side == 'BUY' else 26)
            resources['truth_rpc'] = self.truth_rpc()
        elif original.lane_held:
            self.now = NOW+(82 if self.coverage else (14 if self.trade_side == 'BUY' else 28))
            if not self.coverage:
                self.build_chain()
                chain = repo.chain_receipt(started.runtime.reconstruction_facts().chain_receipt_key)
                resources['application_wallet'] = rt.application_support(self.f, self.actual, chain, self.pre, self.now)
        elif not hasattr(self, 'replayed'):
            self.replayed = True
            application = next(a for a in applications(repo, self.root) if a.attempt_id == self.attempt_id)
            before = durable(repo.database_path)
            replay = repo.apply_settlement(self.attempt_id, chain_receipt_key=application.chain_receipt_key,
                support=application.support, ingestion_key=application.ingestion_key,
                recorded_at_utc=application.recorded_at_utc, fence=repo.write_fence())
            write(self.directory/'application-replay.json', dict(equal=replay == application,
                unchanged=before == durable(repo.database_path), application=asdict(application)))
            if self.coverage == 'complete':
                # Original Runtime selects REPLACE_ATTEMPT and consumes current
                # Authority itself. Only fresh public wallet/venue/lease facts
                # are supplied; no direct preparation or permission injection.
                chain = repo.chain_receipt(application.chain_receipt_key)
                root = chain.observation.root
                self.f.lower = replace(root, slot=root.slot+1, blockhash=rt.sf.bh(root.slot+1),
                    previous_blockhash=root.blockhash, parent_slot=root.slot,
                    block_height=root.block_height+1, block_time=NOW+80)
                self.now = NOW+85
                # Match the existing rt.execution fixture's per-step clock:
                # Runtime samples at +0us and Q1's fresh SELL intent at +1us.
                self.clock_calls = 0
                action = repo.action(original.preparation.action_id)
                self.seed = seed_for(self.f, action, self.now, 231)
                self.read = b21.ReadBoundary(self.seed)
                rpc = rt.ExecutionReadOnlyRpc('https://invalid.local', rt.a3.PROFILE,
                    now_us=self.read.now, transport=rt.httpx.MockTransport(self.read))
                transport = sender.SolanaSendTransport('https://invalid.local', rt.a3.PROFILE,
                    transport=rt.httpx.MockTransport(self.external_send))
                resources['execution'] = rt.ExecutionPorts(rpc, self.seed.evidence.wallet,
                    self.seed.evidence.quote_policy, self.seed.evidence.plan_policy,
                    rt.ComputeBudget(250000, 1000), signer.AutonomousLocalSigner(rt.Keypair.from_bytes(self.key_bytes)),
                    transport, lambda: self.seed.evidence.simulation.leases[0].observed_at_us)
            else:
                self.now = NOW+(14 if self.trade_side == 'BUY' else 28)
        else:
            self.finish()
        return resources

    def build_chain(self):
        if hasattr(self, 'actual'):
            return
        at = NOW+(6 if self.trade_side == 'BUY' else 20)
        _, self.actual, self.scenario, self.pre = rt.original_chain(self.f,
            SimpleNamespace(attempt_id=self.attempt_id), at, failed=self.failed)
        write(self.directory/('actual-'+str(os.getpid())+'.json'), dict(failed=self.failed,
            actual_base=self.actual.actual_base, transaction_fee=self.scenario.tx['meta']['fee'],
            venue_fees=self.actual.venue_fees, pre_native=self.actual.pre_native[rt.sf.WALLET],
            post_native=self.actual.post_native[rt.sf.WALLET]))

    def truth_rpc(self):
        self.build_chain()
        def external(request):
            payload = json.loads(request.content)
            self.requests.append(payload)
            response = self.scenario.handle(request)
            write(self.directory/('rpc-'+str(os.getpid())+'.json'), self.requests)
            if not self.replacement and payload['method'] == 'getTransaction' and self.boundary == 'authoritative-lookup-return':
                self.cut(self.boundary)
            return response
        return rt.PublicReadOnlyRpc('https://invalid.local', rt.a3.PROFILE, transport=rt.httpx.MockTransport(external))

    def observe_calls(self):
        original_produce = b22.composition.produce_exact_message
        def produce(*args, **kwargs):
            result = original_produce(*args, **kwargs)
            write(self.directory/('production-'+str(os.getpid())+'.json'), asdict(result.validation))
            return result
        patch.object(b22.composition, 'produce_exact_message', produce).start()
        original = signer.AutonomousLocalSigner.sign_exact
        def sign(*args, **kwargs):
            result = original(*args, **kwargs)
            write(self.directory/('sign-'+result.preparation.attempt_id+'.json'), dict(pid=os.getpid(),
                attempt_id=result.preparation.attempt_id, signature=result.primary_signature,
                authority=asdict(result.authority_receipt)))
            return result
        patch.object(signer.AutonomousLocalSigner, 'sign_exact', sign).start()

    def install_cuts(self):
        original_handoff = b22.composition.observe_attempt_finality
        def handoff(*args, **kwargs):
            self.in_runtime_truth = True
            try:
                return original_handoff(*args, **kwargs)
            finally:
                self.in_runtime_truth = False
        patch.object(b22.composition, 'observe_attempt_finality', handoff).start()
        original_observe = reconciliation.TransactionEvidenceAdapter.observe
        def observe(adapter):
            result = original_observe(adapter)
            if getattr(self, 'in_runtime_truth', False):
                write(self.directory/'observation-return.json', asdict(result))
                if self.boundary == 'observation-return':
                    self.cut(self.boundary)
            return result
        patch.object(reconciliation.TransactionEvidenceAdapter, 'observe', observe).start()
        original_ingest = Ledger.ingest_chain_observation
        def ingest(repo, *args, **kwargs):
            result = original_ingest(repo, *args, **kwargs)
            write(self.directory/'retained-truth.json', asdict(result))
            if self.boundary in ('truth-retained', 'coverage-retained'):
                self.cut(self.boundary)
            return result
        patch.object(Ledger, 'ingest_chain_observation', ingest).start()

    def cut(self, boundary):
        write(self.directory/'reached.json', dict(boundary=boundary, pid=os.getpid(),
            actual_runtime_truth_handoff=getattr(self, 'in_runtime_truth', False),
            in_transaction=self.f.repo._conn.in_transaction, facts=facts(self.started, self.root),
            durable=durable(self.f.repo.database_path)))
        b1.wait_file(self.directory/'release')

    def external_send(self, request):
        self.send_calls += 1
        payload = json.loads(request.content)
        pending = self.started.runtime.ledger.consumer_snapshot()['pending_attempts'][0]
        envelope = sender.load_signed_envelope(self.started.runtime.ledger, pending)
        assert payload['method'] == 'sendTransaction' and payload['params'][0] == envelope.signed_wire_base64
        assert all(rt.VersionedTransaction.from_bytes(base64.b64decode(payload['params'][0])).verify_with_results())
        assert self.send_calls == 1 and (not self.replacement or self.coverage == 'complete')
        write(self.directory/('send-'+str(os.getpid())+'.json'), dict(request=payload, attempt_id=pending,
            signature=envelope.primary_signature, authoritative_chain_truth=False))
        return rt.httpx.Response(200, json={'jsonrpc':'2.0', 'id':payload['id'], 'result':envelope.primary_signature})

    forbidden_send = external_send

    def finish(self):
        write(self.directory/'finished.json', dict(facts=facts(self.started, self.root), phase=self.phase,
            send_calls=self.send_calls, clock_at=self.now))
        b1.wait_file(self.directory/'finish')


def setup(directory, case):
    name, side, boundary, failed, coverage = case
    place, f, config, unused, _, _ = b21.setup(directory, name, 'b23-finality', 'after')
    old = unused._external_inputs
    with patch.object(rt.sf, 'WALLET', str(rt.Keypair.from_bytes(old.key_bytes).pubkey())), \
            patch.object(rt.sf.plans, 'ACTOR', str(rt.Keypair.from_bytes(old.key_bytes).pubkey())):
        if side == 'SELL':
            b21.a2.restart(f)
            buy = f.repo.action(f.runtime.reconstruction_facts().entry_action_id)
            submitted, _, _ = rt.execution(f, rt.Keypair.from_bytes(old.key_bytes), buy, at=NOW+6, number=171)
            _, actual, scenario, pre = rt.original_chain(f, submitted, NOW+6)
            reconciled, _ = rt.reconcile(f, scenario, NOW+12)
            support = rt.application_support(f, actual, f.repo.chain_receipt(reconciled.receipt_key), pre, NOW+14)
            assert f.step(NOW+14, application_wallet=support).work == 'APPLIED'
            staged = f.step(NOW+16)
            assert staged.work == 'PROTECTIVE_ACTION_STAGED'
            old.seed = seed_for(f, f.repo.action(staged.action_id), NOW+20, 172)
            generation = f.operations.snapshot()['generation']
            old.initial_generation = generation+1
            b21.a2.close(f)
        else:
            generation = unused._generation
        inputs = Inputs(**{field.name: getattr(old, field.name) for field in fields(b21.Inputs)},
            case=name, boundary=boundary, failed=failed, coverage=coverage, public=f.public,
            lower=f.lower, root=f.root, trade_side=side)
    sup = b1.supervisor.OperationsSupervisor(config, inputs, unused.health, expected_generation=generation)
    return place, f, sup, b1.Driver(sup, now=generation+1)


def run_case(directory, case):
    name, side, boundary, failed, coverage = case
    place, f, sup, driver = setup(directory, case)
    try:
        driver.until(lambda result: (place/'reached.json').exists())
        reached = json.loads((place/'reached.json').read_text())
        before = json.loads((place/'before-truth.json').read_text())
        old_fence = sup.fence
        control = sup.store.snapshot()
        driver.mono = sup._deadline
        check(name+':watchdog-terminates', driver.tick().state == 'TERMINATING')
        sup.process.join(timeout=5)
        check(name+':death-before-replacement', sup.process.exitcode is not None)
        driver.now += 1
        works = []
        prior_steps = sup.completed_steps
        def finished(result):
            nonlocal prior_steps
            if sup.completed_steps > prior_steps:
                works.append(sup.last_work)
                prior_steps = sup.completed_steps
            return (place/'finished.json').exists()
        driver.until(finished)
        reconstructed = json.loads((place/'reconstructed-finality.json').read_text())
        ending = json.loads((place/'finished.json').read_text())
        final = ending['facts']
        replay_history = json.loads((place/'historical-replays.json').read_text())
        original = reconstructed['attempts'][-1]
        attempt_id = original['preparation']['attempt_id']
        check(name+':boundary', reached['boundary'] == boundary and not reached['in_transaction'])
        check(name+':exact-committed-reconstruction', reached['durable']['economic_sha256']
            == reconstructed['durable']['economic_sha256'])
        check(name+':new-original-owned-runtime', reconstructed['runtime'] == 'ColdRuntimeV01'
            and reconstructed['fence']['generation'] == old_fence.generation+1
            and reconstructed['fence']['process_identity'] != old_fence.process_identity)
        check(name+':budget-not-reset', sup.store.snapshot()['attempts'] == control['attempts']+1
            and sup.store.snapshot()['budget_id'] == control['budget_id'])
        check(name+':historical-no-permission', replay_history['unchanged']
            and not reconstructed['audit_permission'] and not reconstructed['reconstruction']['grants_permission'])
        check(name+':original-history-identity', final['envelopes'][:len(before['envelopes'])] == before['envelopes']
            and final['stages'][:len(before['stages'])] == before['stages']
            and final['message_receipts'][:len(before['message_receipts'])] == before['message_receipts'])
        check(name+':original-reservation', len(final['reservations']) == 1
            and final['reservations'][0]['reservation_id'] == before['reservations'][0]['reservation_id'])
        check(name+':original-fenced-history-retained', reconstructed['envelopes'] == before['envelopes']
            and reconstructed['stages'] == before['stages'] and reconstructed['message_receipts'] == before['message_receipts'])
        if coverage:
            retained = json.loads((place/'retained-truth.json').read_text())
            request = retained['decision']['expected_request']
            check(name+':original-supported-proof-binding', request['signature'] == original['primary_signature']
                and request['recent_blockhash'] == original['preparation']['lease']['blockhash']
                and request['last_valid_block_height'] == original['preparation']['lease']['last_valid_block_height']
                and retained['observation']['root']['block_height'] > request['last_valid_block_height']
                and retained == reconstructed['chain_receipts'][-1])
        else:
            check(name+':cut-is-runtime-Q4', reached['actual_runtime_truth_handoff'])
            retained = boundary == 'truth-retained'
            check(name+':cut-durable-finality', reconstructed['finalities'][-1]['evidence_revision'] == int(retained)
                and len(reconstructed['chain_receipts']) == len(before['chain_receipts'])+int(retained)
                and len(final['chain_receipts']) == len(before['chain_receipts'])+1)
            expected_work = (['APPLIED', 'NEED_EXECUTION'] if failed else
                ['RECONCILED', 'APPLIED', 'ENTRY_HELD' if side == 'BUY' else 'NEED_RETIREMENT'])
            check(name+':original-lifecycle-resumes', works == expected_work)
        states = [reconstructed, final]
        states.extend(json.loads(path.read_text()) for path in sorted(place.glob('continuation-*.json')))
        for index, state in enumerate(states):
            check(name+':integrity-'+str(index), state['durable']['integrity'] == [['ok']]
                and not state['durable']['foreign_keys'])
        if coverage == 'incomplete':
            check(name+':incomplete-original-UNKNOWN', original['lane_held']
                and reconstructed['finalities'][-1]['positive_finality'] is None
                and reconstructed['finalities'][-1]['disposition'] == 'UNKNOWN')
            check(name+':balance-time-no-release', all(state['attempts'][-1] == original for state in states)
                and all(state['custody'] == reconstructed['custody'] for state in states)
                and works == ['NEED_RECONCILIATION', 'NEED_RECONCILIATION'] and ending['clock_at'] == NOW+7200)
            check(name+':no-replacement', ending['send_calls'] == 0 and len(list(place.glob('sign-*.json'))) == 1)
            check(name+':incomplete-tail-not-proof', request['upper_slot'] is not None
                and request['upper_slot'] < retained['observation']['root']['slot']
                and not reconstructed['finalities'][-1]['may_release_economic_lane']
                and not reconstructed['finalities'][-1]['may_retry']
                and len(list(place.glob('balance-support-*.json'))) == 2)
        else:
            replay = json.loads((place/'application-replay.json').read_text())
            applications = [a for a in final['applications'] if a['attempt_id'] == attempt_id]
            check(name+':exactly-one-original-application', len(applications) == 1 and replay['equal'] and replay['unchanged'])
            check(name+':retained-before-application', original['lane_held'] and not original['economically_applied']
                and len(before['applications']) == len(reconstructed['applications']))
            if coverage:
                replacement = final['attempts'][-1]
                check(name+':COMPLETE-original-resolution', reconstructed['finalities'][-1]['positive_finality'] == 'PROVEN_NON_LANDED'
                    and applications[0]['decision']['disposition'] == 'PROVEN_NON_LANDED_RESOLVED'
                    and not applications[0]['decision']['may_retry'] and request['upper_slot'] is None
                    and retained['decision']['evidence_port_disposition'] == 'COMPLETE_REQUESTED_INTERVAL'
                    and not reconstructed['finalities'][-1]['may_release_economic_lane'])
                check(name+':original-runtime-lawful-replacement', works == ['APPLIED', 'SUBMISSION_OBSERVED']
                    and len(final['attempts']) == len(before['attempts'])+1
                    and replacement['preparation']['ordinal'] == 2
                    and replacement['preparation']['action_id'] == original['preparation']['action_id']
                    and replacement['primary_signature'] != original['primary_signature'])
                new_messages = final['message_receipts'][len(before['message_receipts']):]
                check(name+':fresh-current-Authority', [m['stage'] for m in new_messages] == ['SIGN', 'SEND']
                    and all(m['consumed'] for m in new_messages) and ending['send_calls'] == 1
                    and len(list(place.glob('sign-*.json'))) == 2)
                check(name+':nonlanding-preserves-original-economics', all(before['custody'][key] == final['custody'][key]
                    for key in ('native_lamports', 'network_fees_paid_lamports', 'venue_fees_paid_sol_lamports',
                        'accounts', 'positions', 'protections', 'reservations')))
            else:
                actual = json.loads(next(place.glob('actual-*.json')).read_text())
                proposal = applications[0]['decision']['proposal']
                expected = 'FINALIZED_FAILURE_APPLIED' if failed else 'FINALIZED_SUCCESS_APPLIED'
                check(name+':original-positive-application', applications[0]['decision']['disposition'] == expected
                    and proposal['transaction_fee_lamports'] == actual['transaction_fee']
                    and proposal['base_units_delta'] == (0 if failed else actual['actual_base']*(1 if side == 'BUY' else -1)))
                check(name+':actual-native-delta-once', final['custody']['native_lamports']
                    == before['custody']['native_lamports']+proposal['native_wallet_delta']
                    and proposal['native_wallet_delta'] == actual['post_native']-actual['pre_native']
                    and final['custody']['network_fees_paid_lamports']
                        == before['custody']['network_fees_paid_lamports']+actual['transaction_fee']
                    and proposal['venue_fee_units'] == (0 if failed else actual['venue_fees']))
                check(name+':no-second-send', ending['send_calls'] == 0 and len(list(place.glob('sign-*.json'))) == 1)
                if side == 'SELL':
                    check(name+':residual-and-obligation', final['custody']['positions'][0]['remaining_units']
                        == before['custody']['positions'][0]['remaining_units']+proposal['base_units_delta']
                        and final['custody']['protections'][0]['handoff'] == before['custody']['protections'][0]['handoff'])
        record = dict(case=case, before=before, reached=reached, reconstructed=reconstructed,
            continuation=states[2:], final=final, runtime_work=works,
            invariants={k:v for k,v in CHECKS.items() if k.startswith(name+':')})
        write(place/'result.json', record)
        print(json.dumps(dict(case=name, runtime_work=works, checks=len(record['invariants']))), flush=True)
        return record
    finally:
        b1.cleanup(sup)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--case', choices=[case[0] for case in CASES])
    args = parser.parse_args()
    directory = args.evidence_dir.resolve()
    if directory == ROOT or ROOT in directory.parents:
        raise ValueError('external disposable evidence directory required')
    directory.mkdir(parents=True, exist_ok=False)
    records = [run_case(directory, case) for case in CASES if args.case is None or case[0] == args.case]
    write(directory/'cut-manifest.json', dict(version='B2.3', cases=records, checks=CHECKS))
    print(json.dumps(dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW', cases=len(records),
        checks=len(CHECKS), all_checks=all(CHECKS.values()))), flush=True)


if __name__ == '__main__':
    main()
