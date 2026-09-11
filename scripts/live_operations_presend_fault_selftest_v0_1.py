"""B2.1 original supervised Runtime pre-send cuts; disposable/mock I/O only.

No economic state is assigned. Test wrappers observe actual owner API/COMMIT
boundaries, then pause the retained real child for interruption or A1 fencing.
The evidence directory must be external to this repository.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from contextlib import closing
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_supervisor_selftest_v0_1 as b1
from live import runtime_composition_v0_1 as composition
from live import execution_send_v0_1 as sender
from live import execution_signer_v0_1 as signer
from live.authority_message_control_v0_1 import FreshStageConsumption, MessageStageReceipt

a2, rt, NOW = b1.a2, b1.rt, b1.NOW
Ledger = rt.a3.LedgerRepository
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def write(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2), encoding='utf-8')


def durable(path):
    """Independent read-only committed view, including while writer is paused.

    Original reopen legitimately changes only these two generation/head tables.
    All other table rows and schema are included in economic_sha256.
    """
    with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro', uri=True)) as conn:
        lines = tuple(conn.iterdump())
        economic = '\n'.join(line for line in lines if not line.startswith((
            'INSERT INTO "ledger_writer_generations"', 'INSERT INTO "ledger_head"')))
        tables = {}
        for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
            if name.startswith('sqlite_'):
                continue
            rows = conn.execute('SELECT * FROM "'+name+'"').fetchall()
            encoded = json.dumps(sorted(rows, key=repr), sort_keys=True)
            tables[name] = dict(count=len(rows), sha256=hashlib.sha256(encoded.encode()).hexdigest())
        return dict(economic_sha256=hashlib.sha256(economic.encode()).hexdigest(), tables=tables,
            integrity=conn.execute('PRAGMA integrity_check').fetchall(),
            foreign_keys=conn.execute('PRAGMA foreign_key_check').fetchall())


def owned_facts(started):
    repo = started.runtime.ledger
    snap = repo.consumer_snapshot()
    attempts = tuple(repo.attempt(key) for key in snap['pending_attempts'])
    messages = tuple(repo.authority_message_receipt(row[0]) for row in repo._conn.execute(
        'SELECT command_id FROM ledger_authority_message_stages ORDER BY commit_seq'))
    stages = tuple(s for a in attempts for s in repo.attempt_stage_inputs(a.preparation.attempt_id))
    statuses = tuple(repo.authority_grant_status(g.issuance.grant_id) for g in started.audit.relevant_grants)
    grants = tuple({**g, 'issuance': asdict(g['issuance'])} for g in statuses)
    return dict(runtime=type(started.runtime).__name__, fence=asdict(started.audit.owner_fence),
        audit_permission=started.audit.grants_permission,
        reconstruction=asdict(started.runtime.reconstruction_facts()),
        authority_digest=repo.audit()['authority_digest'], custody_digest=repo.audit()['custody_digest'],
        reservations=[dict(asdict(x), root_id=x.root_id, reservation_id=x.reservation_id)
            for x in snap['reservations']], positions=[asdict(x) for x in snap['positions']],
        attempts=[asdict(x) for x in attempts], grants=grants,
        message_receipts=[dict(command_id=m.original.request.command_id, stage=m.original.request.stage,
            consumed=m.consumed, digest=m.content_digest) for m in messages],
        stages=[s.to_record() for s in stages], durable=durable(repo.database_path))


# Each before/after pair surrounds the actual _commit API while its original
# caller owns the pending transaction. "after" is return from original _commit,
# including SQL COMMIT and original trusted-cache publication; no interior claim.
CUTS = tuple((name+'-'+side, name, side) for name in (
    'admission', 'preparation', 'sign-consumption', 'save-exact-simulated',
    'save-authorized', 'save-signed-durable', 'send-consumption', 'send-claim',
    'unknown-marker') for side in ('before', 'after')) + (
    ('production-before', 'production', 'before'),
    ('production-after', 'production', 'after'),
    ('sign-final-clock', 'sign-final-clock', 'before'),
    ('signed-envelope-return', 'signed-envelope-return', 'after'),
    ('send-final-clock', 'send-final-clock', 'before'))


def expectation(target, side):
    """Exact current-item committed stage, SIGN/SEND slots, claim count."""
    states = {
        'admission': ((None, 0, 0, 0), (None, 0, 0, 0)),
        'production': ((None, 0, 0, 0), (None, 0, 0, 0)),
        'preparation': ((None, 0, 0, 0), ('PREPARED', 0, 0, 0)),
        'sign-consumption': (('PREPARED', 0, 0, 0), ('PREPARED', 1, 0, 0)),
        'save-exact-simulated': (('PREPARED', 1, 0, 0), ('EXACT_SIMULATED', 1, 0, 0)),
        'save-authorized': (('EXACT_SIMULATED', 1, 0, 0), ('AUTHORIZED', 1, 0, 0)),
        'save-signed-durable': (('AUTHORIZED', 1, 0, 0), ('SIGNED_DURABLE', 1, 0, 0)),
        'send-consumption': (('SIGNED_DURABLE', 1, 0, 0), ('SIGNED_DURABLE', 1, 1, 0)),
        'send-claim': (('SIGNED_DURABLE', 1, 1, 0), ('SEND_CLAIMED', 1, 1, 1)),
        'unknown-marker': (('SEND_CLAIMED', 1, 1, 1), ('UNKNOWN', 1, 1, 1)),
        'sign-final-clock': (('PREPARED', 1, 0, 0),)*2,
        'signed-envelope-return': (('PREPARED', 1, 0, 0),)*2,
        'send-final-clock': (('UNKNOWN', 1, 1, 1),)*2,
    }
    stage, sign, send, claims = states[target][side == 'after']
    return dict(stage=stage, sign_slots=sign, send_slots=send, claims=claims,
        reservations=0 if target == 'admission' and side == 'before' else 1,
        signature_persisted=stage in ('SIGNED_DURABLE', 'SEND_CLAIMED', 'UNKNOWN'))


def verify_expected(cut_id, facts, expected):
    attempts = facts['attempts']
    stage = None if not attempts else attempts[0]['recorded_stage']
    messages = facts['message_receipts']
    claims = [s for s in facts['stages'] if s['external_reference'] == sender._CLAIM]
    observed = dict(stage=stage,
        sign_slots=sum(m['consumed'] and m['stage'] == 'SIGN' for m in messages),
        send_slots=sum(m['consumed'] and m['stage'] == 'SEND' for m in messages),
        claims=len(claims), reservations=len(facts['reservations']),
        signature_persisted=bool(attempts and attempts[0]['primary_signature'] is not None))
    check(cut_id+':exact-durable-stage-slots-claims', observed == expected)
    grant = facts['grants'][0]
    root = None if not facts['reservations'] else facts['reservations'][0]['root_id']
    check(cut_id+':original-one-shot-consumption', len(facts['grants']) == 1
        and grant['issuance']['scope'] == 'ENTRY_ONCE' and not grant['grants_message_permission']
        and grant['consumed_root'] == root
        and (grant['consumed_by_command'] is None) == (root is None))
    if claims:
        record = json.loads(claims[0]['external_record_json'])
        check(cut_id+':claim-retains-possible-send', record['possible_send'] and record['finality'] == 'UNKNOWN'
            and record['primary_signature'] == attempts[0]['primary_signature'])
    if attempts and not expected['signature_persisted']:
        check(cut_id+':no-invented-volatile-signature', attempts[0]['primary_signature'] is None
            and attempts[0]['signed_wire_digest'] is None)
    return observed


class ReadBoundary(rt.q1.PublicTransport):
    def __call__(self, request):
        value = super().__call__(request).json()
        method = json.loads(request.content)['method']
        if method == 'getLatestBlockhash':
            value['result']['value']['lastValidBlockHeight'] = self.seed.evidence.wallet.observation.anchor.block_height+30
        elif method == 'getBlockHeight':
            value['result'] = self.seed.evidence.wallet.observation.anchor.block_height+1
        return rt.httpx.Response(200, json=value)


@dataclass
class Inputs:
    directory: Path
    target: str
    side: str
    initial_generation: int
    entry: object
    seed: object
    key_bytes: bytes
    stale: bool = False

    def __call__(self, started):
        # These attributes are child-local resources, never serialized owners.
        if not hasattr(self, 'initialized'):
            self.initialized = True
            wallet = str(rt.Keypair.from_bytes(self.key_bytes).pubkey())
            patch.object(rt.sf, 'WALLET', wallet).start()
            patch.object(rt.sf.plans, 'ACTOR', wallet).start()
            self.started = started
            self.calls = 0
            self.clock_calls = 0
            self.last_calls = 0
            self.active = None
            self.witness = {}
            self.read = self.ports = None
            self.replacement = started.audit.owner_fence.generation > self.initial_generation
            if self.replacement:
                write(self.directory/'reconstructed.json', owned_facts(started))
                self.historical_replays()
            else:
                self.install_cuts()
        elif self.replacement:
            write(self.directory/'next-step-state.json', owned_facts(started))
            b1.wait_file(self.directory/'finish')
        self.calls += 1
        resources = dict(clock=self.clock, source_cut_utc=rt.a3.utc(NOW+1))
        if self.target == 'admission':
            resources['entry'] = self.entry
        elif self.seed is not None:
            if self.ports is None:
                self.read = ReadBoundary(self.seed)
                # Parent fixture fixes the exact block-height values in seed.
                self.read.seed = self.seed
                rpc = rt.ExecutionReadOnlyRpc('https://invalid.local', rt.a3.PROFILE,
                    now_us=self.read.now, transport=rt.httpx.MockTransport(self.read))
                transport = sender.SolanaSendTransport('https://invalid.local', rt.a3.PROFILE,
                    transport=rt.httpx.MockTransport(self.forbidden_send))
                key = rt.Keypair.from_bytes(self.key_bytes)
                self.ports = rt.ExecutionPorts(rpc, self.seed.evidence.wallet,
                    self.seed.evidence.quote_policy, self.seed.evidence.plan_policy,
                    rt.ComputeBudget(250000, 1000), signer.AutonomousLocalSigner(key), transport,
                    lambda: self.seed.evidence.simulation.leases[0].observed_at_us)
            # Without a prepared attempt, original next work is NEED_EXECUTION;
            # prepared cases receive real concrete ports to prove lane priority.
            if not self.replacement or started.audit.pending_attempt is not None:
                resources['execution'] = self.ports
        return resources

    def forbidden_send(self, request):
        write(self.directory/'unexpected-send.json', {'method': json.loads(request.content)['method']})
        raise AssertionError('B2.1 must stop before external send')

    def clock(self):
        value = rt.a3.clock(self.started.runtime.ledger, NOW+4 if self.target == 'admission' else NOW+6)
        self.clock_calls += 1
        caller = sys._getframe(1).f_code.co_name
        if caller == '_last_call':
            self.last_calls += 1
        if not self.replacement:
            if self.target == 'sign-final-clock' and caller == 'sign_exact':
                self.pause('sign-final-clock', 'before')
            if self.target == 'send-final-clock' and caller == '_last_call' and self.last_calls == 2:
                self.pause('send-final-clock', 'before')
        stamp = (datetime.fromisoformat(value.utc_upper_utc)+timedelta(microseconds=self.clock_calls-1)).isoformat(timespec='microseconds')
        return replace(value, utc_lower_utc=stamp, utc_upper_utc=stamp,
            monotonic_ns=value.monotonic_ns+1000*(self.clock_calls-1))

    def pause(self, operation, side):
        if (operation, side) != (self.target, self.side):
            return
        repo = self.started.runtime.ledger
        write(self.directory/'reached.json', dict(operation=operation, side=side, pid=os.getpid(),
            owner=asdict(self.started.audit.owner_fence), in_transaction=repo._conn.in_transaction,
            durable=durable(repo.database_path), witness=self.witness))
        b1.wait_file(self.directory/'release')

    def historical_replays(self):
        repo = self.started.runtime.ledger
        before = durable(repo.database_path)
        outcomes = []
        for (key,) in repo._conn.execute('SELECT command_id FROM ledger_authority_message_stages ORDER BY commit_seq').fetchall():
            receipt = repo.authority_message_receipt(key)
            original = receipt.original
            replay = repo.consume_authority_message_stage(original.request, original.validation.clock,
                self.started.runtime.source, original.validation.evidence, fence=repo.write_fence())
            if type(replay) is not MessageStageReceipt or replay != receipt or isinstance(replay, FreshStageConsumption):
                raise AssertionError('historical lookup minted fresh Authority')
            outcomes.append(dict(command_id=key, stage=original.request.stage, type=type(replay).__name__))
        write(self.directory/'historical-replays.json', dict(outcomes=outcomes,
            unchanged=before == durable(repo.database_path)))

    def install_cuts(self):
        # Patches live only in this disposable child and all delegate to original
        # APIs. No Runtime pointer, Ledger projection, Authority or OMS assignment.
        owner = self
        original_commit = Ledger._commit

        def commit(repo):
            operation = owner.active
            if operation == owner.target:
                write(owner.directory/'boundary-before.json', durable(repo.database_path))
                owner.pause(operation, 'before')
            result = original_commit(repo)
            if operation == owner.target:
                owner.pause(operation, 'after')
            return result
        patch.object(Ledger, '_commit', commit).start()

        def surround(name, identify):
            original = getattr(Ledger, name)
            def wrapped(repo, *args, **kwargs):
                previous = owner.active
                owner.active = identify(*args, **kwargs)
                if name == 'prepare_attempt':
                    owner.witness.update(attempt_id=args[0].attempt_id, action_id=args[0].action_id,
                        message_sha256=args[0].message_sha256)
                try:
                    return original(repo, *args, **kwargs)
                finally:
                    owner.active = previous
            patch.object(Ledger, name, wrapped).start()
        surround('admit_authority_entry', lambda *a, **k: 'admission')
        surround('prepare_attempt', lambda *a, **k: 'preparation')
        surround('consume_authority_message_stage', lambda request, *a, **k: request.stage.lower()+'-consumption')
        def stage_name(stage, *args, **kwargs):
            if stage.external_reference == sender._CLAIM:
                return 'send-claim'
            if stage.external_reference == sender._UNCERTAIN:
                return 'unknown-marker'
            return 'save-'+stage.target_stage.lower().replace('_', '-')
        surround('record_external_attempt_stage', stage_name)
        original_produce = composition.produce_exact_message
        def produce(*args, **kwargs):
            owner.pause('production', 'before')
            result = original_produce(*args, **kwargs)
            owner.witness['production_digest'] = result.content_digest
            owner.witness['message_sha256'] = result.original.evidence.simulation.envelopes[0].message_sha256
            owner.pause('production', 'after')
            return result
        patch.object(composition, 'produce_exact_message', produce).start()
        original_sign = signer.AutonomousLocalSigner.sign_exact
        def sign(*args, **kwargs):
            try:
                result = original_sign(*args, **kwargs)
            except (RuntimeError, ValueError) as exc:
                if owner.stale and owner.target == 'sign-final-clock':
                    owner.stale_denial(exc)
                raise
            owner.witness.update(primary_signature=result.primary_signature,
                signed_wire_digest=result.signed_wire_digest, envelope_digest=result.content_digest)
            owner.pause('signed-envelope-return', 'after')
            return result
        patch.object(signer.AutonomousLocalSigner, 'sign_exact', sign).start()
        if self.stale:
            original_send = composition.send_exact
            def send(*args, **kwargs):
                try:
                    return original_send(*args, **kwargs)
                except (RuntimeError, ValueError) as exc:
                    owner.stale_denial(exc)
                    raise
            patch.object(composition, 'send_exact', send).start()

    def stale_denial(self, exc):
        # The actual signer/sender released its guards before this observation.
        # A second original Runtime step must reject the retained stale owner.
        if not (self.directory/'reached.json').exists():
            raise AssertionError('stale fixture failed before its intended cut') from exc
        held = self.started.runtime.step(clock=self.clock, source_cut_utc=rt.a3.utc(NOW+1))
        write(self.directory/'stale-result.json', dict(error_type=type(exc).__name__,
            next_work=held.work, facts=owned_facts(self.started),
            durable=durable(self.started.runtime.ledger.database_path)))
        b1.wait_file(self.directory/'finish')


def setup(directory, cut_id, target, side, *, stale=False):
    place = directory/cut_id
    place.mkdir()
    key = rt.Keypair()
    with patch.object(rt.sf, 'WALLET', str(key.pubkey())), patch.object(rt.sf.plans, 'ACTOR', str(key.pubkey())):
        f = a2.installed(rt.Fixture(place, 'fixture'))
        f.candidate_ready()
        with patch.object(rt.a3, 'arm', partial(rt.a3.arm, scope='ENTRY_ONCE', root=f.root)):
            f.configure()
        support = rt.a3.wallet(f, scenario=f.scenario)
        seed = None
        if target != 'admission':
            # Preparation fixture itself uses original owned startup/admission.
            a2.restart(f)
            admitted = f.step(entry=rt.EntryFacts('PUMP', rt.sf.TOKEN_PROGRAM_ID, support))
            check(cut_id+':fixture-owned-admission', admitted.work == 'ENTRY_ADMITTED')
            action = f.repo.action(admitted.action_id)
            intent = rt.q1._intent(rt.q1.capture_message_context(f.repo, action.action_id), (NOW+6)*1000000+1)
            seed, _ = rt.a4.evidence(f, action, rt.a5.wallet_scenario(f.public, f.lower), at=NOW+6,
                context_slot=f.lower.slot, intent=intent, wallet_floor=f.lower.slot,
                recent_blockhash=rt.sf.bh(171), last_valid_height=f.lower.block_height+30,
                validity_height=f.lower.block_height+1, original_accounts=f.public)
            rt.q1.install(f, seed)
        config = dict(operations_path=f.operations_path, ledger_path=f.path, domain=f.domain,
            expected_identity=f.expected, **a2.c4.cold_args(f))
        generation = f.operations.snapshot()['generation']
        initial = durable(f.path)
        a2.close(f)
        inputs = Inputs(place, target, side, generation+1,
            rt.EntryFacts('PUMP', rt.sf.TOKEN_PROGRAM_ID, support), seed, bytes(key), stale)
    sup = b1.supervisor.OperationsSupervisor(config, inputs,
        b1.supervisor.HealthProfile(20000000, 20000000, 1000000),
        expected_generation=generation if generation else None)
    return place, f, config, sup, b1.Driver(sup, now=generation+1), initial


def run_cut(directory, cut):
    cut_id, target, side = cut
    place, f, config, sup, driver, initial = setup(directory, cut_id, target, side)
    try:
        driver.until(lambda result: (place/'reached.json').exists())
        reached = json.loads((place/'reached.json').read_text())
        first_pid, first_fence = sup.process.pid, sup.fence
        control = sup.store.snapshot()
        # Invoke the production watchdog timeout. It signals only its retained
        # exact child, proves its death, then original A1 budgets replacement.
        driver.mono = sup._deadline
        check(cut_id+':watchdog-termination', driver.tick().state == 'TERMINATING')
        sup.process.join(timeout=5)
        check(cut_id+':original-child-confirmed-dead', sup.process.exitcode is not None)
        driver.now += 1
        driver.until(lambda result: (place/'next-step-state.json').exists())
        recovered = json.loads((place/'reconstructed.json').read_text())
        after = json.loads((place/'next-step-state.json').read_text())
        replays = json.loads((place/'historical-replays.json').read_text())
        next_work = sup.last_work
        check(cut_id+':fresh-owned-replacement', recovered['runtime'] == 'ColdRuntimeV01'
            and recovered['fence']['generation'] == first_fence.generation+1
            and recovered['fence']['process_identity'] != first_fence.process_identity
            and sup.process.pid != first_pid and sup.completed_steps == 1)
        check(cut_id+':budget-no-reset', sup.store.snapshot()['attempts'] == control['attempts']+1
            and sup.store.snapshot()['budget_id'] == control['budget_id'])
        check(cut_id+':exact-economic-reconstruction', reached['durable']['economic_sha256']
            == recovered['durable']['economic_sha256'])
        check(cut_id+':original-historical-no-permission', not recovered['audit_permission']
            and not recovered['reconstruction']['grants_permission'] and replays['unchanged'])
        check(cut_id+':no-external-send', not (place/'unexpected-send.json').exists())
        check(cut_id+':integrity', recovered['durable']['integrity'] == [['ok']]
            and not recovered['durable']['foreign_keys'])
        attempts, reservations = recovered['attempts'], recovered['reservations']
        expected = expectation(target, side)
        observed = verify_expected(cut_id, recovered, expected)
        check(cut_id+':no-fabricated-position-obligation', not recovered['positions']
            and recovered['reconstruction']['obligation_id'] is None)
        if target == 'admission' and side == 'before':
            check(cut_id+':atomic-rollback', not attempts and not reservations
                and len(after['reservations']) == 1 and next_work == 'ENTRY_ADMITTED')
        else:
            check(cut_id+':original-encumbrance', len(reservations) == 1
                and reservations == after['reservations'])
            if attempts:
                attempt = attempts[0]
                check(cut_id+':same-attempt-root-lane', len(attempts) == 1 and attempt['lane_held']
                    and attempts == after['attempts'])
                signed = attempt['primary_signature'] is not None
                check(cut_id+':original-next-work', next_work == ('NEED_RECONCILIATION' if signed else 'HELD'))
                witness = reached['witness']
                check(cut_id+':exact-preparation-identity', recovered['reconstruction']['pending_attempt_id']
                    == witness['attempt_id'] and attempt['preparation']['action_id'] == witness['action_id']
                    and hashlib.sha256(bytes.fromhex(attempt['preparation']['message_hex'])).hexdigest()
                    == witness['message_sha256'])
                if signed:
                    check(cut_id+':exact-durable-signature', attempt['primary_signature'] == witness['primary_signature']
                        and attempt['signed_wire_digest'] == witness['signed_wire_digest'])
                check(cut_id+':no-second-economic-or-authority-path', recovered['durable']['economic_sha256']
                    == after['durable']['economic_sha256'])
            else:
                check(cut_id+':action-awaits-execution', next_work == 'NEED_EXECUTION' and not after['attempts'])
        record = dict(cut_id=cut_id, boundary=target, side=side, reached=reached,
            expected=expected, observed=observed,
            durable_before=json.loads((place/'boundary-before.json').read_text()) if (place/'boundary-before.json').exists() else initial,
            restart_work=next_work, recovered=recovered, after_next_work=after, historical_replays=replays,
            invariants={k: v for k, v in CHECKS.items() if k.startswith(cut_id+':')})
        write(place/'result.json', record)
        print(json.dumps(dict(cut=cut_id, restart=next_work, checks=len(record['invariants']))), flush=True)
        return record
    finally:
        b1.cleanup(sup)


def stale_cut(directory, stage):
    cut_id, target = 'stale-'+stage, stage+'-final-clock'
    place, f, config, sup, driver, _ = setup(directory, cut_id, target, 'before', stale=True)
    replacement = None
    try:
        driver.until(lambda result: (place/'reached.json').exists())
        reached = json.loads((place/'reached.json').read_text())
        old = sup.fence
        replacement = sup.store.acquire('b21-explicit-replacement', now_us=driver.now+1,
            replace_generation=old.generation)
        (place/'release').write_text('release stale original callback', encoding='utf-8')
        b1.wait_file(place/'stale-result.json')
        result = json.loads((place/'stale-result.json').read_text())
        expected = expectation(target, 'before')
        observed = verify_expected(cut_id, result['facts'], expected)
        check(cut_id+':stale-original-runtime-held', result['next_work'] == 'OPERATIONS_HELD'
            and replacement.fence.generation == old.generation+1)
        check(cut_id+':no-stale-economic-mutation', reached['durable']['economic_sha256']
            == result['durable']['economic_sha256'])
        check(cut_id+':no-stale-external-send', not (place/'unexpected-send.json').exists())
        record = dict(cut_id=cut_id, boundary=target, side='before', reached=reached,
            expected=expected, observed=observed,
            replacement_fence=asdict(replacement.fence), outcome=result,
            invariants={k: v for k, v in CHECKS.items() if k.startswith(cut_id+':')})
        write(place/'result.json', record)
        print(json.dumps(dict(cut=cut_id, restart=result['next_work'], checks=len(record['invariants']))), flush=True)
        return record
    finally:
        if replacement is not None:
            replacement.close()
        b1.cleanup(sup)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence-dir', type=Path, required=True)
    args = parser.parse_args()
    directory = args.evidence_dir.resolve()
    if directory == ROOT or ROOT in directory.parents:
        raise ValueError('external disposable evidence directory required')
    directory.mkdir(parents=True, exist_ok=False)
    records = [run_cut(directory, cut) for cut in CUTS]
    records.extend(stale_cut(directory, stage) for stage in ('sign', 'send'))
    write(directory/'cut-manifest.json', dict(version='B2.1', cuts=records, checks=CHECKS,
        scope='Pre-send only; no external dispatch/finality/settlement. Original owners only.'))
    print(json.dumps(dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW', cuts=len(records),
        checks=len(CHECKS), all_checks=all(CHECKS.values()))), flush=True)


if __name__ == '__main__':
    main()
