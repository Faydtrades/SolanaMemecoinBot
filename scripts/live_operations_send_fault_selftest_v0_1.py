"""B2.2 actual supervised send ambiguity; disposable/mock external I/O only.

Fake processing/response records are external test witnesses, never chain
evidence. Original Runtime, Execution, Evidence and Ledger own every decision.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_presend_fault_selftest_v0_1 as b21
from live import runtime_composition_v0_1 as composition
from live import execution_send_v0_1 as sender
from live import execution_signer_v0_1 as signer

b1, rt, NOW = b21.b1, b21.rt, b21.NOW
write, durable = b21.write, b21.durable
CHECKS = {}
CASES = (
    ('ack-loss', 'TRANSPORT_UNRESOLVED', 'original-send-exact-return', True),
    ('timeout', 'TRANSPORT_UNRESOLVED', 'original-send-exact-return', False),
    ('rpc-error', 'RPC_ERROR', 'original-send-exact-return', False),
    ('processing-interrupted', None, 'fake-processing-before-response', True),
    ('response-interrupted', None, 'original-send-claimed-return', True),
    ('ack-durable', 'ACKNOWLEDGED', 'original-send-exact-return', True),
)


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def facts(started):
    original = b21.owned_facts(started)
    repo = started.runtime.ledger
    attempt_id = original['reconstruction']['pending_attempt_id']
    envelope = sender.load_signed_envelope(repo, attempt_id)
    original['envelope'] = dict(attempt_id=attempt_id,
        root_id=repo.action(envelope.preparation.action_id).root_id,
        action_id=envelope.preparation.action_id, message_sha256=envelope.preparation.message_sha256,
        primary_signature=envelope.primary_signature, signed_wire_digest=envelope.signed_wire_digest,
        signed_wire_base64=envelope.signed_wire_base64, envelope_digest=envelope.content_digest)
    original['send_observations'] = [asdict(x) for x in sender.load_send_observations(repo, attempt_id)]
    original['finality'] = asdict(repo.attempt_finality(attempt_id))
    key = original['reconstruction']['chain_receipt_key']
    original['chain_receipt'] = None if key is None else asdict(repo.chain_receipt(key))
    return original


class LostAckStream(rt.httpx.SyncByteStream):
    def __iter__(self):
        # The fake service produced an ACK, but the local consumer never gets
        # a complete response. Original HTTP streaming classifies the failure.
        yield b'{"jsonrpc":"2.0","id":1,"result":"'
        raise rt.httpx.ReadError('SYNTHETIC_ACK_STREAM_LOST')


class MissingHistory:
    """Bound original signature, null history and expired-root public facts.

    No transaction metadata, canonical interval coverage or positive finality
    is supplied. The original Evidence adapter and Ledger must adjudicate it.
    """
    def __init__(self, envelope, domain, at):
        self.envelope, self.domain, self.at = envelope, domain, at
        lower = envelope.preparation.finalized_lower_anchor
        self.root_slot = lower.slot+100
        self.root_height = lower.block_height+100
        self.requests = []

    def __call__(self, request):
        value = json.loads(request.content)
        self.requests.append(value)
        method, params = value['method'], value['params']
        if method == 'getGenesisHash':
            result = self.domain.genesis_hash
        elif method == 'getSignatureStatuses':
            assert params == [[self.envelope.primary_signature], {'searchTransactionHistory': True}]
            result = {'context': {'slot': self.root_slot, 'apiVersion': '3.1.0'}, 'value': [None]}
        elif method == 'getTransaction':
            assert params[0] == self.envelope.primary_signature
            result = None
        elif method == 'getSlot':
            result = self.root_slot
        elif method == 'getBlock':
            assert params[0] == self.root_slot and params[1]['transactionDetails'] == 'none'
            result = dict(blockhash=rt.sf.bh(self.root_slot), previousBlockhash=rt.sf.bh(self.root_slot-1),
                parentSlot=self.root_slot-1, blockHeight=self.root_height, blockTime=self.at-1)
        else:
            raise AssertionError('B2.2 unexpected historical RPC: '+method)
        return rt.httpx.Response(200, json={'jsonrpc': '2.0', 'id': value['id'], 'result': result})


@dataclass
class Inputs(b21.Inputs):
    case: str = ''

    def __call__(self, started):
        if not hasattr(self, 'initialized'):
            self.now = NOW+6
            self.phase = 0
            self.send_calls = 0
            self.observe_sign_calls()
            resources = super().__call__(started)
            if self.replacement:
                write(self.directory/'reconstructed-send.json', facts(started))
            return resources
        if not self.replacement:
            raise AssertionError('original send must pause before later Runtime progression')
        self.phase += 1
        resources = dict(clock=self.clock, execution=self.ports, source_cut_utc=rt.a3.utc(NOW+1))
        if self.phase == 1:
            write(self.directory/'after-initial-step.json', facts(started))
            self.now = NOW+3600
        elif self.phase == 2:
            write(self.directory/'after-elapsed-before-history.json', facts(started))
            self.now = NOW+3601
            repo = started.runtime.ledger
            attempt_id = started.audit.pending_attempt.preparation.attempt_id
            envelope = sender.load_signed_envelope(repo, attempt_id)
            self.history = MissingHistory(envelope, repo.domain, self.now)
            self.truth_rpc = rt.PublicReadOnlyRpc('https://invalid.local', rt.a3.PROFILE,
                transport=rt.httpx.MockTransport(self.history))
            resources['truth_rpc'] = self.truth_rpc
        elif self.phase == 3:
            write(self.directory/'after-missing-history.json', facts(started))
            write(self.directory/'history-requests.json', self.history.requests)
            self.now = NOW+7200
        elif self.phase == 4:
            write(self.directory/'after-later-time.json', facts(started))
            write(self.directory/'replacement-resources.json', dict(send_calls=self.send_calls,
                exact_message_read_calls=len(self.read.requests), clock_at=self.now,
                initial_clock_at=NOW+6, fake_processing_is_chain_truth=False))
            b1.wait_file(self.directory/'finish')
        else:
            raise AssertionError('B2.2 finite replacement steps exhausted')
        return resources

    def clock(self):
        value = rt.a3.clock(self.started.runtime.ledger, self.now)
        self.clock_calls += 1
        shift = self.clock_calls-1
        stamp = (datetime.fromisoformat(value.utc_upper_utc)+timedelta(microseconds=shift)).isoformat(timespec='microseconds')
        return replace(value, utc_lower_utc=stamp, utc_upper_utc=stamp,
            monotonic_ns=value.monotonic_ns+1000*shift)

    def observe_sign_calls(self):
        original = signer.AutonomousLocalSigner.sign_exact
        owner = self
        def sign(*args, **kwargs):
            path = owner.directory/('sign-call-'+str(os.getpid())+'.json')
            if path.exists():
                raise AssertionError('B2.2 attempted a second signing call')
            write(path, {'pid': os.getpid(), 'synthetic_key': True})
            return original(*args, **kwargs)
        patch.object(signer.AutonomousLocalSigner, 'sign_exact', sign).start()

    def install_cuts(self):
        owner = self
        original_claimed = sender.SolanaSendTransport._send_claimed
        def claimed(transport, ticket):
            result = original_claimed(transport, ticket)
            write(owner.directory/'transport-return.json', dict(outcome=result[0], acknowledged_signature=result[1],
                authoritative_chain_truth=False))
            if owner.case == 'response-interrupted':
                owner.cut('original-send-claimed-return')
            return result
        patch.object(sender.SolanaSendTransport, '_send_claimed', claimed).start()
        original_exact = composition.send_exact
        def exact(*args, **kwargs):
            result = original_exact(*args, **kwargs)
            write(owner.directory/'original-local-return.json', asdict(result))
            owner.cut('original-send-exact-return')
            return result
        patch.object(composition, 'send_exact', exact).start()

    def cut(self, boundary):
        repo = self.started.runtime.ledger
        write(self.directory/'reached.json', dict(case=self.case, boundary=boundary, pid=os.getpid(),
            owner=asdict(self.started.audit.owner_fence), in_transaction=repo._conn.in_transaction,
            durable=durable(repo.database_path)))
        b1.wait_file(self.directory/'release')

    def forbidden_send(self, request):
        # Reuse the published concrete port builder, replacing only its fake
        # external handler. This method name is inherited fixture plumbing.
        self.send_calls += 1
        if self.replacement or self.send_calls != 1:
            write(self.directory/'unexpected-send.json', {'calls': self.send_calls, 'replacement': self.replacement})
            raise AssertionError('B2.2 repeated external send')
        payload = json.loads(request.content)
        repo = self.started.runtime.ledger
        attempt_id = repo.consumer_snapshot()['pending_attempts'][0]
        envelope = sender.load_signed_envelope(repo, attempt_id)
        transaction = rt.VersionedTransaction.from_bytes(base64.b64decode(payload['params'][0]))
        assert payload['method'] == 'sendTransaction' and payload['params'][0] == envelope.signed_wire_base64
        assert all(transaction.verify_with_results())
        assert payload['params'][1] == dict(encoding='base64', skipPreflight=False, preflightCommitment='confirmed',
            maxRetries=0, minContextSlot=envelope.preparation.lease.context_slot)
        assert repo.attempt(attempt_id).recorded_stage == 'UNKNOWN'
        processing = self.case not in ('timeout', 'rpc-error')
        write(self.directory/'external-request.json', dict(case=self.case, pid=os.getpid(), request=payload,
            attempt_id=attempt_id, root_id=repo.action(envelope.preparation.action_id).root_id,
            primary_signature=envelope.primary_signature, signed_wire_digest=envelope.signed_wire_digest,
            fake_processing='ACCEPTED_BY_FAKE_SERVICE' if processing else 'UNDETERMINED',
            authoritative_chain_truth=False, durable_before_response=durable(repo.database_path)))
        if self.case == 'processing-interrupted':
            self.cut('fake-processing-before-response')
        if self.case == 'timeout':
            raise rt.httpx.ReadTimeout('SYNTHETIC_RESPONSE_TIMEOUT')
        if self.case == 'rpc-error':
            response = {'jsonrpc': '2.0', 'id': payload['id'], 'error': {'code': -32000, 'message': 'SYNTHETIC_RPC_FAILURE'}}
        else:
            response = {'jsonrpc': '2.0', 'id': payload['id'], 'result': envelope.primary_signature}
        write(self.directory/'external-response.json', dict(response=response, authoritative_chain_truth=False,
            delivery='ACK_STREAM_LOST' if self.case == 'ack-loss' else 'FAKE_RESPONSE_RETURNED'))
        if self.case == 'ack-loss':
            return rt.httpx.Response(200, headers={'content-type': 'application/json'}, stream=LostAckStream())
        return rt.httpx.Response(200, json=response)


def run_case(directory, case):
    name, expected_outcome, boundary, processed = case
    place, fixture, config, unused, _, _ = b21.setup(directory, name, 'b22-send', 'after')
    original_inputs = unused._external_inputs
    inputs = Inputs(**{f.name: getattr(original_inputs, f.name) for f in fields(b21.Inputs)}, case=name)
    sup = b1.supervisor.OperationsSupervisor(config, inputs, unused.health, expected_generation=unused._generation)
    driver = b1.Driver(sup, now=unused._generation+1)
    try:
        driver.until(lambda result: (place/'reached.json').exists())
        reached = json.loads((place/'reached.json').read_text(encoding='utf-8'))
        external = json.loads((place/'external-request.json').read_text(encoding='utf-8'))
        old_fence = sup.fence
        control = sup.store.snapshot()
        driver.mono = sup._deadline
        check(name+':watchdog-terminates-retained-child', driver.tick().state == 'TERMINATING')
        sup.process.join(timeout=5)
        check(name+':death-before-replacement', sup.process.exitcode is not None)
        driver.now += 1
        works = []
        prior_steps = 0
        def finished(result):
            nonlocal prior_steps
            if sup.completed_steps != prior_steps:
                works.append(sup.last_work)
                prior_steps = sup.completed_steps
            return (place/'after-later-time.json').exists()
        driver.until(finished)
        names = ('reconstructed-send', 'after-initial-step', 'after-elapsed-before-history',
            'after-missing-history', 'after-later-time')
        states = {key: json.loads((place/(key+'.json')).read_text(encoding='utf-8')) for key in names}
        recovered = states['reconstructed-send']
        resources = json.loads((place/'replacement-resources.json').read_text(encoding='utf-8'))
        history = json.loads((place/'history-requests.json').read_text(encoding='utf-8'))
        replay = json.loads((place/'historical-replays.json').read_text(encoding='utf-8'))
        check(name+':actual-boundary', reached['boundary'] == boundary)
        check(name+':fresh-owned-startup', recovered['runtime'] == 'ColdRuntimeV01'
            and recovered['fence']['generation'] == old_fence.generation+1
            and recovered['fence']['process_identity'] != old_fence.process_identity)
        check(name+':original-budget-no-reset', sup.store.snapshot()['attempts'] == control['attempts']+1
            and sup.store.snapshot()['budget_id'] == control['budget_id'])
        check(name+':exact-committed-reconstruction', reached['durable']['economic_sha256']
            == recovered['durable']['economic_sha256'])
        observations = recovered['send_observations']
        check(name+':original-local-outcome', len(observations) == (0 if expected_outcome is None else 1)
            and (not observations or observations[0]['outcome'] == expected_outcome))
        check(name+':local-response-not-finality', recovered['finality']['positive_finality'] is None
            and recovered['finality']['evidence_revision'] == 0
            and all(o['finality'] == 'UNKNOWN' and o['possible_send'] and not o['may_retry'] for o in observations))
        check(name+':fake-processing-separate', not external['authoritative_chain_truth']
            and (external['fake_processing'] == 'ACCEPTED_BY_FAKE_SERVICE') == processed)
        check(name+':original-external-wire-identity', external['attempt_id'] == recovered['envelope']['attempt_id']
            and external['root_id'] == recovered['envelope']['root_id']
            and external['primary_signature'] == recovered['envelope']['primary_signature']
            and external['signed_wire_digest'] == recovered['envelope']['signed_wire_digest']
            and external['request']['params'][0] == recovered['envelope']['signed_wire_base64'])
        check(name+':original-runtime-priority', works == ['NEED_RECONCILIATION', 'NEED_RECONCILIATION', 'RECONCILED', 'NEED_RECONCILIATION'])
        check(name+':no-second-signature-or-send', len(tuple(place.glob('sign-call-*.json'))) == 1
            and resources['send_calls'] == 0 and resources['exact_message_read_calls'] == 0
            and not (place/'unexpected-send.json').exists())
        check(name+':historical-authority-not-permission', replay['unchanged']
            and len(replay['outcomes']) == 2 and all(x['type'] == 'MessageStageReceipt' for x in replay['outcomes']))
        for phase, state in states.items():
            check(name+':'+phase+':identity-and-encumbrance', len(state['attempts']) == 1
                and state['envelope'] == recovered['envelope']
                and state['attempts'][0]['preparation'] == recovered['attempts'][0]['preparation']
                and state['reservations'] == recovered['reservations'] and len(state['reservations']) == 1
                and state['grants'] == recovered['grants'] and state['stages'] == recovered['stages']
                and state['message_receipts'] == recovered['message_receipts'] and not state['positions'])
            check(name+':'+phase+':no-release-or-permission', state['attempts'][0]['lane_held']
                and state['attempts'][0]['recorded_stage'] == 'UNKNOWN'
                and not state['attempts'][0]['economically_applied'] and not state['attempts'][0]['may_send']
                and state['finality']['positive_finality'] is None
                and not state['finality']['may_release_economic_lane'] and not state['finality']['may_retry']
                and not state['audit_permission'] and not state['reconstruction']['grants_permission'])
            check(name+':'+phase+':integrity', state['durable']['integrity'] == [['ok']]
                and not state['durable']['foreign_keys'])
        before_history = states['after-elapsed-before-history']
        incomplete = states['after-missing-history']
        later = states['after-later-time']
        check(name+':elapsed-alone-no-change', recovered['durable']['economic_sha256']
            == before_history['durable']['economic_sha256'] and incomplete['durable']['economic_sha256']
            == later['durable']['economic_sha256'] and resources['clock_at'] == NOW+7200)
        receipt = incomplete['chain_receipt']
        observation = receipt['observation']
        check(name+':incomplete-history-original-UNKNOWN', incomplete['finality']['disposition'] == 'UNKNOWN'
            and incomplete['finality']['evidence_revision'] == 1
            and receipt['decision']['observation_disposition'] == 'UNKNOWN'
            and observation['transaction'] is None and observation['membership_block'] is None
            and observation['status']['slot'] is None and observation['status']['search_transaction_history'])
        check(name+':expiry-is-not-nonlanding', observation['root']['block_height']
            > recovered['attempts'][0]['preparation']['lease']['last_valid_block_height']
            and not observation['failures'] and later['finality'] == incomplete['finality'])
        check(name+':history-queries-original-signature', [q['method'] for q in history]
            == ['getGenesisHash', 'getSignatureStatuses', 'getTransaction', 'getSlot', 'getBlock', 'getGenesisHash']
            and history[1]['params'] == [[external['primary_signature']], {'searchTransactionHistory': True}]
            and history[2]['params'][0] == external['primary_signature'])
        claims = [s for s in recovered['stages'] if s['external_reference'] == sender._CLAIM]
        claim = json.loads(claims[0]['external_record_json'])
        check(name+':one-original-possible-send-claim', len(claims) == 1 and claim['possible_send']
            and claim['finality'] == 'UNKNOWN' and claim['primary_signature'] == external['primary_signature'])
        record = dict(case=name, boundary=boundary, expected_local_outcome=expected_outcome,
            fake_external=external, reached=reached, states=states, history_requests=history,
            replacement_resources=resources, runtime_works=works,
            checks={key: value for key, value in CHECKS.items() if key.startswith(name+':')})
        write(place/'result.json', record)
        print(json.dumps(dict(case=name, local_outcome=expected_outcome, finality=later['finality']['disposition'],
            checks=len(record['checks']))), flush=True)
        return record
    finally:
        b1.cleanup(sup)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence-dir', type=Path, required=True)
    args = parser.parse_args()
    directory = args.evidence_dir.resolve()
    if directory == ROOT or ROOT in directory.parents:
        raise ValueError('external disposable evidence directory required')
    directory.mkdir(parents=True, exist_ok=False)
    records = [run_case(directory, case) for case in CASES]
    write(directory/'outcome-cut-manifest.json', dict(version='B2.2', cases=records, checks=CHECKS,
        scope='Send/ACK ambiguity and incomplete-history UNKNOWN only; no positive finality/coverage/settlement.'))
    print(json.dumps(dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW', cases=len(records),
        checks=len(CHECKS), all_checks=all(CHECKS.values()))), flush=True)


if __name__ == '__main__':
    main()
