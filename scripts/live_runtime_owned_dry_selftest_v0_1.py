"""IR-C1 same-root owned DRY qualification; synthetic public inputs only."""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import time
from contextlib import ExitStack
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_startup_selftest_v0_1 as fixtures
import live_runtime_dry_selftest_v0_1 as external
from live import operations_startup_v0_1 as startup
from live import runtime_composition_v0_1 as runtime
from live import runtime_dry_v0_1 as dry
from live.operations_ownership_v0_1 import OperationsStore, RestartProfile
from live.operations_readiness_v0_1 import evaluate
from live.runtime_dry_public_driver_v0_1 import DryPublicFacts, DryPublicInputDriver, drive_steps
from live.execution_message_v0_1 import ComputeBudget
from live.execution_readonly_v0_1 import ExecutionReadOnlyRpc

rt, a3, sf, NOW = fixtures.rt, fixtures.a3, fixtures.sf, fixtures.NOW
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def fails(call):
    try:
        call()
    except (ValueError, RuntimeError, TypeError, sqlite3.DatabaseError, OSError):
        return True
    return False


class Interrupted(BaseException):
    pass


def fixture(directory, name):
    domain = a3.domain
    with patch.object(a3, 'domain', side_effect=lambda **kw: domain(**dict(kw, mode='DRY'))):
        f = rt.Fixture(directory, name)
    f.candidate_ready()
    arm = a3.arm
    with patch.object(a3, 'arm', side_effect=lambda *a, **kw: arm(*a, **dict(kw, scope='DRY'))):
        f.configure()
    f.operations_path = directory/(name+'-operations.sqlite')
    f.operations = OperationsStore.initialize(f.operations_path, f.domain, RestartProfile(100, 1000000, 0), now_us=0)
    f.live_paths = tuple(str(directory/(name+'-excluded-live-'+part+'.sqlite'))
        for part in ('ledger','producer','evidence','operations','monitor'))
    f.expected = startup.configured_identity(f.domain, operations_path=f.operations_path,
        ledger_path=f.path, **fixtures.c4.cold_args(f),
        producer_schema_digest=startup.schema_fingerprint(f.ppath),
        evidence_schema_digest=startup.schema_fingerprint(directory/(name+'-evidence.sqlite')),
        restart_profile=RestartProfile(100, 1000000, 0), dry_live_paths=f.live_paths, dry_monitor_digest="0"*64)
    f.expected = replace(f.expected, dry_monitor_digest=startup.dry_monitor_fingerprint(fixtures.healthy_monitor_config(f)))
    f.started = None
    restart(f)
    return f


def restart(f, **overrides):
    fixtures.close(f)
    args = fixtures.c4.cold_args(f)
    args.update(dry_live_paths=f.live_paths, degradation_config=fixtures.healthy_monitor_config(f))
    args.update(overrides)
    state = f.operations.snapshot()
    f.started = startup.start_dry(f.operations_path, f.path, f.domain,
        process_identity=f.name+'-child', now_us=state['last_control_us']+1,
        replace_generation=state['generation'] if state['generation'] else None,
        expected_identity=args.pop('expected_identity', f.expected), **args)
    f.runtime = f.started.runtime
    f.repo, f.producer, f.handoff, f.source = f.runtime.ledger, f.runtime.producer, f.runtime.handoff, f.runtime.source
    f.conn = f.runtime._producer_conn
    return f.started.audit


def step(f, at=NOW+4, **kwargs):
    calls = []
    def clock():
        calls.append(None)
        value = a3.clock(f.repo, at)
        stamp = (datetime.fromisoformat(value.utc_upper_utc)+timedelta(microseconds=len(calls)-1)).isoformat(timespec='microseconds')
        return replace(value, utc_lower_utc=stamp, utc_upper_utc=stamp,
            monotonic_ns=value.monotonic_ns+1000*(len(calls)-1))
    return f.runtime.step(clock=clock, source_cut_utc=a3.utc((NOW+15 if at >= NOW+18 else NOW+1)),
        operations_resources=lambda: fixtures.host(f, at), **kwargs)


def admit(f):
    support = a3.wallet(f, scenario=f.scenario)
    result = step(f, entry=runtime.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID, support))
    check(f.name+'_original_authority_admitted', result.work == 'ENTRY_ADMITTED')
    f.buy = f.repo.action(result.action_id)
    return f.buy


def seed_for(f, action, at=NOW+9):
    with patch.object(sf.plans.fx, 'MINT', action.mint):
        seed, _ = external.a4.evidence(f, action, f.scenario, at=at)
    return seed


def execute(f, action, *, point=None, at=NOW+9):
    seed = seed_for(f, action, at)
    boundary = external.DryPublicTransport(seed)
    def now():
        return dry.utc_microseconds(seed.clock.utc_upper_utc) if len(boundary.requests) >= 9 else seed.evidence.simulation.leases[0].observed_at_us
    facts = DryPublicFacts(lambda: seed.clock, None, seed.evidence.wallet,
        seed.evidence.quote_policy, seed.evidence.plan_policy, ComputeBudget(250000,1000),
        now, lambda: fixtures.host(f, at), a3.utc((NOW+15 if at >= NOW+18 else NOW+1)))
    driver = DryPublicInputDriver('https://invalid.local', a3.PROFILE, lambda _: facts,
        httpx.MockTransport(boundary))
    # Separate response observation clock matches the existing accepted fixture.
    with patch.object(ExecutionReadOnlyRpc, '__init__', autospec=True) as construct:
        original = EXECUTION_RPC_INIT
        def create(self, *args, **kwargs):
            kwargs['now_us'] = boundary.now
            original(self, *args, **kwargs)
        construct.side_effect = create
        with ExitStack() as stack:
            if point:
                method = 'finish_dry' if point in ('before-terminal','after-terminal') else 'prepare_attempt'
                actual = getattr(f.repo, method)
                def interrupt(*args, **kwargs):
                    if point != 'before-terminal':
                        actual(*args, **kwargs)
                    raise Interrupted(point)
                stack.enter_context(patch.object(f.repo, method, side_effect=interrupt))
            result = drive_steps(f.started, driver, max_steps=1)[0]
    return result, boundary


EXECUTION_RPC_INIT = ExecutionReadOnlyRpc.__init__


def absence(f, roots):
    audit, snap = f.repo.audit(), f.repo.consumer_snapshot()
    check(f.name+'_no_effects', audit['posting_count'] == audit['application_receipt_count'] == 0
        and not snap['positions'] and not snap['protections']
        and snap['funding'].native_lamports == f.domain.known_native_wallet_lamports
        and snap['funding'].network_fees_paid_lamports == 0)
    check(f.name+'_only_unsigned_graph', all(a.primary_signature is None and a.signed_wire_digest is None
        and a.recorded_stage in ('PREPARED','EXACT_SIMULATED') and not a.economically_applied
        for root in roots for a in f.repo._root_attempts(root)))


def positive(directory):
    f = fixture(directory, 'continuous')
    try:
        check('same_original_root', type(f.runtime) is fixtures.ColdRuntimeV01
            and isinstance(f.runtime, runtime.RuntimeCompositionV01) and f.runtime.capability == 'NO_BROADCAST'
            and f.started.audit.identity.capability == startup.DRY_CAPABILITY)
        action = admit(f)
        result, boundary = execute(f, action)
        check('original_dry_exact_simulation_terminal', result.work == 'NON_SUBMITTED'
            and len(boundary.requests) == 10 and boundary.requests[-1]['method'] == 'simulateTransaction'
            and result.reason == 'DRY_CAPABILITY')
        check('durable_terminal_capacity_release', f.repo.inbox_disposition(action.root_id) == 'NON_SUBMITTED'
            and not f.repo.consumer_snapshot()['reservations'] and not f.repo.mutation_lane()['held'])
        restart(f)
        check('cold_terminal_not_requeued', action.root_id not in f.runtime.queued_roots
            and f.runtime._entry_action_id is None)
        # Original continuous source contributes a different canonical candidate.
        f.append(rt.hf.NEXT)
        next_result = None
        for _ in range(128):
            next_result = step(f, NOW+18)
            if next_result.work == 'NEED_ENTRY_FACTS':
                break
        check('next_original_continuous_candidate', next_result.work == 'NEED_ENTRY_FACTS'
            and next_result.root_id != action.root_id)
        f.root, f.item = next_result.root_id, f.repo.candidate(next_result.root_id)
        support = a3.wallet(f, scenario=f.scenario, at=NOW+18)
        result = step(f, NOW+18, entry=runtime.EntryFacts('PUMP',sf.TOKEN_PROGRAM_ID,support))
        check('next_original_authority_admission', result.work == 'ENTRY_ADMITTED')
        second = f.repo.action(result.action_id)
        result, boundary = execute(f, second, at=NOW+23)
        check('second_continuous_exact_simulation_terminal', result.work == 'NON_SUBMITTED'
            and result.reason == 'DRY_CAPABILITY' and len(boundary.requests) == 10)
        absence(f, (action.root_id, second.root_id))
    finally:
        fixtures.close(f)


def interruptions(directory):
    for point in ('before-terminal','after-terminal','after-preparation','after-admission'):
        f = fixture(directory, point)
        try:
            action = admit(f)
            if point != 'after-admission':
                try:
                    execute(f, action, point=point)
                except Interrupted:
                    pass
                else:
                    raise AssertionError(point)
            count = len(f.repo._root_attempts(action.root_id))
            terminal_before = f.repo.inbox_disposition(action.root_id) == 'NON_SUBMITTED'
            restart(f)
            result = step(f, NOW+10)
            check(point+'_cold_original_recovery', (terminal_before and result.work == 'SOURCE_ADVANCED')
                or (not terminal_before and result.work == 'NON_SUBMITTED'))
            check(point+'_no_replacement_attempt', len(f.repo._root_attempts(action.root_id)) == count
                and f.repo.inbox_disposition(action.root_id) == 'NON_SUBMITTED')
            restart(f)
            step(f, NOW+11)
            check(point+'_repeated_reopen_stable', len(f.repo._root_attempts(action.root_id)) == count
                and not f.repo.consumer_snapshot()['reservations'])
            absence(f, (action.root_id,))
            check(point+'_no_live_unsent_incident', not any(
                item.condition == 'UNSENT_ATTEMPT_RECOVERY_REQUIRED'
                for item in f.runtime._operations_degradation.snapshot().conditions))
            if point == 'after-preparation':
                f.append(rt.hf.NEXT)
                for _ in range(128):
                    continued = step(f, NOW+18)
                    if continued.work == 'NEED_ENTRY_FACTS':
                        break
                check('prepared_recovery_next_candidate_unheld', continued.work == 'NEED_ENTRY_FACTS'
                    and not f.runtime._operations_degradation.snapshot().entry_held)
        finally:
            fixtures.close(f)


def controls(directory):
    f = fixture(directory,'controls')
    try:
        check('duplicate_ownership_denied', fails(lambda: f.operations.acquire('duplicate', now_us=2)))
        before = f.repo.write_fence()
        live_ports = object.__new__(runtime.ExecutionPorts)
        for resource in ('execution','truth_rpc','application_wallet','retirement_wallet','exit_proofs','protective_max_units'):
            check('capability_reject_'+resource, fails(lambda: step(f, **{resource:live_ports})))
        check('escalation_no_ledger_effect', f.repo.write_fence() == before)
        f.operations.operator_stop(now_us=3)
        result = step(f)
        check('durable_stop_blocks_existing_root', result.work == 'OPERATIONS_HELD'
            and f.repo.write_fence() == before)
        check('durable_stop_reopen_denied', fails(lambda: restart(f)))
    finally:
        fixtures.close(f)
    for kind in ('stale-code','wrong-path','wrong-domain','live-alias','missing-ledger','missing-source','source-failure-recovery','wrong-monitor-path','wrong-monitor-limit','wrong-dispatch','missing-exclusions','corrupt-recovery'):
        f = fixture(directory,kind)
        try:
            if kind == 'stale-code':
                check(kind, fails(lambda: restart(f, expected_identity=replace(f.expected, runtime_code_digest='0'*64))))
            elif kind == 'wrong-path':
                check(kind, fails(lambda: restart(f, producer_path=directory/'wrong-producer.sqlite')))
            elif kind == 'wrong-domain':
                check(kind, fails(lambda: OperationsStore(f.operations_path, replace(f.domain, mode='LIVE'))))
            elif kind == 'live-alias':
                check(kind, fails(lambda: restart(f, dry_live_paths=(str(f.path), *f.live_paths[1:]))))
            elif kind == 'missing-ledger':
                fixtures.close(f)
                f.path.unlink()
                check(kind, fails(lambda: restart(f)) and not f.path.exists())
            elif kind == 'missing-source':
                fixtures.close(f)
                source_path = directory/(kind+'-evidence.sqlite')
                source_path.unlink()
                audit = restart(f)
                check(kind, audit.reconstruction.source_state == 'SOURCE_RECONSTRUCTION_UNAVAILABLE'
                    and not source_path.exists() and step(f).work == 'SOURCE_HELD')
            elif kind == 'wrong-monitor-path':
                changed = replace(f.startup_monitor_config, path=str(directory/'wrong-alerts.sqlite'))
                check(kind, fails(lambda: restart(f, degradation_config=changed)))
            elif kind == 'wrong-monitor-limit':
                config = f.startup_monitor_config
                limits = config.policy.resource_limits
                changed = replace(config, policy=replace(config.policy,
                    resource_limits=(replace(limits[0], limit=limits[0].limit+1), *limits[1:])))
                check(kind, fails(lambda: restart(f, degradation_config=changed)))
            elif kind == 'wrong-dispatch':
                check(kind, fails(lambda: restart(f, queued_roots=3)))
            elif kind == 'missing-exclusions':
                check(kind, fails(lambda: restart(f, dry_live_paths=None)))
            elif kind == 'corrupt-recovery':
                action = admit(f)
                fixtures.close(f)
                # Isolated corruption of original durable history; no repair.
                with sqlite3.connect(f.path) as conn:
                    conn.execute('UPDATE ledger_head SET last_receipt_digest=?', ('0'*64,))
                check(kind, fails(lambda: restart(f)))
            else:
                action = admit(f)
                fixtures.close(f)
                source_path = directory/(kind+'-evidence.sqlite')
                source_path.unlink()
                audit = restart(f)
                result = step(f, NOW+10)
                check(kind, audit.reconstruction.entry_action_id == action.action_id
                    and result.work == 'NON_SUBMITTED' and not source_path.exists())
                absence(f, (action.root_id,))
        finally:
            fixtures.close(f)


def no_public_read(request):
    raise AssertionError('recovery must not read public RPC')


@dataclass
class SpawnFacts:
    directory: Path
    calls: int = 0

    def __call__(self, started):
        from live.execution_signer_v0_1 import AutonomousLocalSigner
        from live.execution_send_v0_1 import SolanaSendTransport
        # These traps run inside every new child, including after a crash.
        def forbidden(*args, **kwargs):
            raise AssertionError('child mutation capability forbidden')
        AutonomousLocalSigner.__init__ = forbidden
        AutonomousLocalSigner.sign_exact = forbidden
        SolanaSendTransport.__init__ = forbidden
        runtime.send_exact = forbidden
        httpx.HTTPTransport.handle_request = forbidden
        self.calls += 1
        (self.directory/('child-'+str(os.getpid())+'.json')).write_text(json.dumps({
            'generation':started.audit.owner_fence.generation,
            'runtime':type(started.runtime).__name__, 'capability':started.runtime.capability,
            'public_driver':True, 'calls':self.calls}))
        marker = self.directory/'crashed-once'
        if self.calls == 2 and not marker.exists():
            marker.write_text('synthetic child failure after original recovery step')
            os._exit(23)
        sample = a3.clock(started.runtime.ledger, NOW+10)
        config = started.runtime._operations_degradation.configuration
        return DryPublicFacts(lambda:sample, None, None, None, None, ComputeBudget(250000,1000),
            lambda:dry.utc_microseconds(sample.utc_upper_utc),
            lambda:fixtures.host(SimpleNamespace(runtime=started.runtime,startup_monitor_config=config),NOW+10),
            a3.utc(NOW+1))


def supervision(directory):
    import live_operations_supervisor_selftest_v0_1 as public_supervisor
    from live.operations_supervisor_v0_1 import OperationsSupervisor, HealthProfile
    place = directory/'supervision'
    place.mkdir()
    f = fixture(place, 'supervision')
    action = admit(f)
    configuration = dict(operations_path=f.operations_path, ledger_path=f.path, domain=f.domain,
        expected_identity=f.expected, degradation_config=f.startup_monitor_config,
        dry_live_paths=f.live_paths, **fixtures.c4.cold_args(f))
    fixtures.close(f)
    inputs = DryPublicInputDriver('https://invalid.local', a3.PROFILE, SpawnFacts(place),
        httpx.MockTransport(no_public_read))
    state = f.operations.snapshot()
    sup = OperationsSupervisor(configuration, inputs, HealthProfile(20000000,20000000,1000000),
        expected_generation=state['generation'])
    driver = public_supervisor.Driver(sup, now=state['last_control_us']+1)
    try:
        driver.until(lambda fact: fact.completed_steps >= 2)
        witnesses = [json.loads(path.read_text()) for path in place.glob('child-*.json')]
        check('supervisor_fresh_dry_child_restart', len(witnesses) >= 2
            and all(item['runtime'] == 'ColdRuntimeV01' and item['capability'] == 'NO_BROADCAST'
                and item['public_driver'] for item in witnesses)
            and len({item['generation'] for item in witnesses}) == len(witnesses))
        for _ in range(100):
            try:
                stopped = sup.operator_stop(now_us=driver.now+1)
                break
            except sqlite3.OperationalError:
                time.sleep(.01)
        else:
            raise AssertionError('bounded DRY step did not release ownership lock')
        check('supervisor_durable_dry_stop', stopped.state == 'OPERATOR_STOPPED'
            and f.operations.snapshot()['stopped'] == 1)
        public_supervisor.cleanup(sup)
        repo = a3.LedgerRepository.reopen(f.path, f.domain)
        try:
            check('supervisor_original_interruption_recovered', repo.inbox_disposition(action.root_id) == 'NON_SUBMITTED'
                and not repo.consumer_snapshot()['reservations'] and repo.audit()['attempt_count'] == 0)
        finally:
            repo.close()
        check('supervisor_stop_survives_reopen', fails(lambda: startup.start_dry(**configuration,
            process_identity='after-stop', now_us=driver.now+2)))
    finally:
        public_supervisor.cleanup(sup)


def qualification(directory):
    # Any accidental call into existing LIVE execution is a test failure.
    from live.execution_signer_v0_1 import AutonomousLocalSigner
    from live.execution_send_v0_1 import SolanaSendTransport
    with ExitStack() as traps:
        watched = [traps.enter_context(patch.object(httpx.HTTPTransport, 'handle_request', side_effect=AssertionError('network forbidden'))),
            traps.enter_context(patch.object(AutonomousLocalSigner, '__init__', side_effect=AssertionError('signer forbidden'))),
            traps.enter_context(patch.object(AutonomousLocalSigner, 'sign_exact', side_effect=AssertionError('sign forbidden'))),
            traps.enter_context(patch.object(SolanaSendTransport, '__init__', side_effect=AssertionError('send transport forbidden'))),
            traps.enter_context(patch.object(runtime, 'send_exact', side_effect=AssertionError('send forbidden'))),
            traps.enter_context(patch.object(runtime.RuntimeCompositionV01, '_execute', side_effect=AssertionError('LIVE execution forbidden'))),
            traps.enter_context(patch.object(runtime.RuntimeCompositionV01, '_truth', side_effect=AssertionError('LIVE truth forbidden')))]
        positive(directory)
        interruptions(directory)
        controls(directory)
        supervision(directory)
        check('zero_signer_send_or_live_graph_calls', all(item.call_count == 0 for item in watched))
    return dict(CHECKS)


def main():
    with tempfile.TemporaryDirectory(prefix='step11c-owned-dry-') as directory:
        qualification(Path(directory))
    print(json.dumps({'checks':CHECKS, 'count':len(CHECKS), 'all_ok':all(CHECKS.values()),
        'T010_executed':False, 'signing':False, 'send':False, 'broadcast':False}, sort_keys=True))


if __name__ == '__main__':
    main()
