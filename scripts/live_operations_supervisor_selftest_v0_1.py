"""B1 disposable real spawned children; accepted Runtime and fake external I/O."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_startup_selftest_v0_1 as a2
from live import operations_supervisor_v0_1 as supervisor
from live.operations_ownership_v0_1 import OperationsStore, RestartProfile

rt, c4, NOW = a2.rt, a2.c4, a2.NOW
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def digest(path):
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro', uri=True)) as conn:
        return hashlib.sha256('\n'.join(line for line in conn.iterdump()
            if not line.startswith(('INSERT INTO \"ledger_writer_generations\"', 'INSERT INTO \"ledger_head\"'))).encode()).hexdigest()


def wait_file(path):
    end = time.monotonic()+20
    while not path.exists():
        if time.monotonic() >= end:
            raise AssertionError('child fixture timed out: '+path.name)
        time.sleep(.01)


def resources(started):
    calls = []
    def clock():
        value = rt.a3.clock(started.runtime.ledger, NOW+6)
        calls.append(None)
        stamp = (datetime.fromisoformat(value.utc_upper_utc)+timedelta(microseconds=len(calls))).isoformat(timespec='microseconds')
        return replace(value, utc_lower_utc=stamp, utc_upper_utc=stamp,
                       monotonic_ns=value.monotonic_ns+1000*len(calls))
    return dict(clock=clock, source_cut_utc=rt.a3.utc(NOW+1))


@dataclass
class Inputs:
    directory: Path
    behavior: str = 'normal'

    def __call__(self, started):
        path = self.directory/('owner-'+str(os.getpid())+'.json')
        if not path.exists():
            path.write_text(json.dumps({'pid': os.getpid(), 'generation': started.audit.owner_fence.generation,
                'ownership_pid': started.runtime._ownership._pid,
                'runtime': type(started.runtime).__name__, 'permission': started.audit.grants_permission,
                'ledger_digest': digest(started.runtime.ledger.database_path),
                'authority': hashlib.sha256(repr(started.audit.authority).encode()).hexdigest(),
                'entry_action_id': started.audit.reconstruction.entry_action_id,
                'pending_attempt_id': started.audit.reconstruction.pending_attempt_id}))
        if self.behavior == 'hang':
            time.sleep(30)
        elif self.behavior == 'crash':
            os._exit(23)
        elif self.behavior == 'normal-exit':
            raise SystemExit(0)
        return resources(started)


@dataclass
class FinalFenceInputs:
    directory: Path
    stage: str
    public: object
    lower: object
    key_bytes: bytes

    def __call__(self, started):
        done = self.directory/'final-result.json'
        if done.exists():
            return resources(started)
        key = rt.Keypair.from_bytes(self.key_bytes)
        f = SimpleNamespace(runtime=started.runtime, repo=started.runtime.ledger,
            domain=started.runtime.ledger.domain, public=self.public, lower=self.lower)
        calls, last, dispatched = [], [], []
        def clock():
            value = rt.a3.clock(f.repo, NOW+6)
            calls.append(None)
            caller = sys._getframe(1).f_code.co_name
            if caller == '_last_call':
                last.append(None)
            target = caller == 'sign_exact' if self.stage == 'sign' else caller == '_last_call' and len(last) == 2
            if target and not (self.directory/'reached').exists():
                (self.directory/'reached').write_text('final '+self.stage)
                wait_file(self.directory/'release')
            stamp = (datetime.fromisoformat(value.utc_upper_utc)+timedelta(microseconds=len(calls))).isoformat(timespec='microseconds')
            return replace(value, utc_lower_utc=stamp, utc_upper_utc=stamp,
                           monotonic_ns=value.monotonic_ns+1000*len(calls))
        f.step = lambda at, **kwargs: f.runtime.step(clock=clock, source_cut_utc=rt.a3.utc(NOW+1), **kwargs)
        original = rt.q3.Boundary.__call__
        def boundary(instance, request):
            dispatched.append(True)
            return original(instance, request)
        denied = False
        with patch.object(rt.sf, 'WALLET', str(key.pubkey())), patch.object(rt.sf.plans, 'ACTOR', str(key.pubkey())), patch.object(rt.q3.Boundary, '__call__', boundary):
            try:
                rt.execution(f, key, f.repo.action(started.audit.reconstruction.entry_action_id), at=NOW+6, number=171)
            except (RuntimeError, ValueError):
                denied = True
        pending = f.repo.consumer_snapshot()['pending_attempts']
        attempt = f.repo.attempt(pending[0])
        done.write_text(json.dumps({'denied': denied, 'dispatches': len(dispatched),
            'lane_held': attempt.lane_held, 'signed': attempt.primary_signature is not None,
            'stage': attempt.recorded_stage, 'applied': attempt.economically_applied}))
        return resources(started)


class Driver:
    def __init__(self, sup, now=0):
        self.sup, self.now, self.mono = sup, now, 0

    def tick(self):
        self.mono += 1000
        return self.sup.poll(now_us=self.now, monotonic_us=self.mono)

    def until(self, predicate):
        end = time.monotonic()+20
        while time.monotonic() < end:
            result = self.tick()
            if predicate(result):
                return result
            if self.sup._latched:
                raise AssertionError('unexpected hold '+result.state)
            time.sleep(.01)
        raise AssertionError('watchdog fixture timeout')


def fixture(directory, name, *, profile=RestartProfile(4, 100, 10), inputs=None):
    place = directory/name
    place.mkdir()
    f = a2.installed(rt.Fixture(place, name), profile=profile)
    f.candidate_ready()
    config = dict(operations_path=f.operations_path, ledger_path=f.path, domain=f.domain,
        expected_identity=f.expected, **c4.cold_args(f))
    a2.close(f)
    sup = supervisor.OperationsSupervisor(config, inputs or Inputs(place), supervisor.HealthProfile(20000000, 20000000, 1000000))
    return f, config, sup, Driver(sup)


def cleanup(sup):
    # Only the retained exact disposable child handle; never PID lookup/signals.
    if sup.process is not None:
        if sup.process.exitcode is None:
            sup.process.terminate()
        sup.process.join(timeout=5)
        if sup.process.is_alive():
            raise AssertionError('disposable child did not exit')
        sup.process.close()
        sup.process = None
    if sup._channel is not None:
        sup._channel.close()
        sup._channel = None


def running(driver):
    return driver.until(lambda result: result.completed_steps >= 1)


def recovery(directory):
    f, config, sup, driver = fixture(directory, 'recovery')
    original = digest(f.path)
    budget = sup.store.snapshot()['budget_id']
    try:
        running(driver)
        first = sup.fence
        pid = sup.process.pid
        record = json.loads((f.directory/('owner-'+str(pid)+'.json')).read_text())
        check('real_child_original_owned_cold_runtime_and_no_permission', record['pid'] == record['ownership_pid'] == pid
              and record['runtime'] == 'ColdRuntimeV01' and not record['permission'])
        check('actual_runtime_step_progress_only_not_entry_permission', sup.last_work == 'NEED_ENTRY_FACTS'
              and not driver.tick().grants_permission)
        sup.process.terminate()
        sup.process.join(5)
        original = digest(f.path)
        check('confirmed_exit_respects_durable_backoff', driver.tick().state == 'RESTART_BACKOFF'
              and sup.store.snapshot()['attempts'] == 1)
        driver.now = 10
        driver.until(lambda result: sup.fence is not None and sup.fence.generation > first.generation and result.completed_steps >= 2)
        check('real_fresh_process_replacement_new_nonce_pid_generation', sup.process.pid != pid
              and sup.fence.process_identity != first.process_identity and sup.fence.nonce != first.nonce
              and sup.fence.generation == first.generation+1)
        check('same_budget_and_original_economic_store_after_replacement', sup.store.snapshot()['attempts'] == 2
              and sup.store.snapshot()['budget_id'] == budget
              and json.loads((f.directory/('owner-'+str(sup.process.pid)+'.json')).read_text())['ledger_digest'] == original)
        sup.process.terminate()
        sup.process.join(5)
        driver.now = 100
        second = sup.fence.generation
        driver.until(lambda result: sup.fence is not None and sup.fence.generation > second and result.completed_steps >= 3)
        check('original_nonexhausted_window_rollover_retains_budget', sup.store.snapshot()['attempts'] == 1
              and sup.store.snapshot()['window_start_us'] == 100 and sup.store.snapshot()['budget_id'] == budget)
        with closing(sqlite3.connect(f.operations_path, isolation_level=None)) as conn:
            conn.execute('BEGIN IMMEDIATE')
            failed = False
            try:
                sup.operator_stop(now_us=101)
            except sqlite3.OperationalError:
                failed = True
            check('busy_stop_not_claimed_or_signalled', failed and sup._latched is None
                  and sup.process.exitcode is None and not sup.store.snapshot()['stopped'])
            conn.rollback()
        stopped = sup.operator_stop(now_us=101)
        sup.process.join(5)
        check('operator_stop_committed_prevents_replacement', stopped.state == driver.tick().state == 'OPERATOR_STOPPED'
              and sup.store.snapshot()['stopped'] == 1)
        other = supervisor.OperationsSupervisor(config, Inputs(f.directory), sup.health)
        check('reopened_supervisor_cannot_reset_stop', other.poll(now_us=10000, monotonic_us=0).state == 'OPERATOR_STOPPED'
)
    finally:
        cleanup(sup)


def faults(directory):
    for behavior in ('crash', 'normal-exit', 'hang'):
        f, config, sup, driver = fixture(directory, behavior, profile=RestartProfile(1, 100, 0), inputs=Inputs(directory/behavior, behavior))
        try:
            driver.until(lambda result: bool(list(f.directory.glob('owner-*.json'))))
            if behavior == 'hang':
                driver.mono = sup._deadline
                check('lost_progress_starts_exact_child_termination', driver.tick().state == 'TERMINATING')
            if behavior == 'normal-exit':
                result = driver.until(lambda result: result.state == 'HELD_NORMAL_EXIT')
                check('normal_exit_never_auto_replaced', result.state == 'HELD_NORMAL_EXIT' and sup.store.snapshot()['attempts'] == 1)
            else:
                result = driver.until(lambda result: result.state == 'RESTART_EXHAUSTED')
                retained = sup.store.snapshot()
                check(behavior+'_original_exhaustion_latched', result.state == 'RESTART_EXHAUSTED' and retained['exhausted'] == 1
                      and retained['attempts'] == 1 and retained['nonce'] is None)
                fresh = supervisor.OperationsSupervisor(config, Inputs(f.directory), sup.health)
                check(behavior+'_exhaustion_does_not_expire', fresh.poll(now_us=100000, monotonic_us=0).state == 'RESTART_EXHAUSTED'
                      and fresh.store.snapshot() == retained)
        finally:
            cleanup(sup)
    f, config, sup, driver = fixture(directory, 'channel')
    try:
        running(driver)
        sup._channel.close()
        check('live_child_pipe_loss_is_health_loss_not_death', driver.tick().state == 'CHILD_CHANNEL_LOST' and sup._waiting)
        driver.mono = sup._deadline
        check('pipe_loss_has_finite_termination_deadline', driver.tick().state == 'TERMINATING')
    finally:
        cleanup(sup)


def unproven_and_identity(directory):
    f, config, sup, driver = fixture(directory, 'unproven')
    try:
        # Spawn pickle failure occurs before original A1 acquisition.
        sup._external_inputs = lambda started: resources(started)
        check('spawn_failure_holds_without_unmetered_retries', driver.tick().state == 'HELD_LAUNCH_UNPROVEN'
              and driver.tick().state == 'HELD_LAUNCH_UNPROVEN' and sup.store.snapshot()['attempts'] == 0)
    finally:
        cleanup(sup)
    f, config, sup, driver = fixture(directory, 'generation')
    try:
        running(driver)
        other = supervisor.OperationsSupervisor(config, Inputs(f.directory), sup.health)
        check('new_supervisor_does_not_adopt_existing_owner', other.poll(now_us=1, monotonic_us=0).state == 'HELD_EXISTING_OWNER')
        stale = sup.fence
        replacement = sup.store.acquire('explicit-host-takeover', now_us=10, replace_generation=stale.generation)
        check('retained_full_fence_owner_change_holds', driver.sup.poll(now_us=10, monotonic_us=driver.mono+1000).state == 'HELD_OWNER_CHANGED')
        replacement.close()
        cleanup(sup)
        fresh = supervisor.OperationsSupervisor(config, Inputs(f.directory), sup.health, expected_generation=replacement.fence.generation)
        fresh_driver = Driver(fresh, now=20)
        try:
            running(fresh_driver)
            check('explicit_orphan_generation_reacquires_original_budget', fresh.fence.generation == replacement.fence.generation+1
                  and fresh.store.snapshot()['attempts'] == 3)
        finally:
            cleanup(fresh)
    finally:
        cleanup(sup)


def handshake_race(directory):
    f, config, sup, driver = fixture(directory, 'handshake-race')
    try:
        driver.tick()
        stale = dict(sup.store.snapshot())
        # Deterministically recreate the first-read-before-child-acquire race.
        stale.update(generation=0, process_identity=None, nonce=None, attempts=0)
        end = time.monotonic()+20
        while not sup._channel.poll(0):
            if time.monotonic() > end:
                raise AssertionError('handshake timeout')
            time.sleep(.01)
        original = sup.store.snapshot
        with patch.object(sup.store, 'snapshot', side_effect=[stale, original()]):
            check('handshake_refreshes_control_after_child_acquisition_race', driver.tick().state == 'RUNNING' and sup.fence is not None)
        actual = original()
        forged = dict(actual, nonce='f'*32 if actual['nonce'] != 'f'*32 else 'e'*32)
        with patch.object(sup.store, 'snapshot', return_value=forged):
            check('same_generation_and_pid_wrong_nonce_is_held', driver.tick().state == 'HELD_OWNER_CHANGED')
    finally:
        cleanup(sup)


def final_fencing(directory):
    for stage in ('sign', 'send'):
        place = directory/('final-'+stage)
        place.mkdir()
        key = rt.Keypair()
        with patch.object(rt.sf, 'WALLET', str(key.pubkey())), patch.object(rt.sf.plans, 'ACTOR', str(key.pubkey())):
            f = a2.installed(rt.Fixture(place, stage), profile=RestartProfile(4, 100, 0))
            f.admit()
            config = dict(operations_path=f.operations_path, ledger_path=f.path, domain=f.domain,
                expected_identity=f.expected, **c4.cold_args(f))
            inputs = FinalFenceInputs(place, stage, f.public, f.lower, bytes(key))
            a2.close(f)
            sup = supervisor.OperationsSupervisor(config, inputs, supervisor.HealthProfile(20000000, 20000000, 1000000))
            driver = Driver(sup)
            try:
                driver.until(lambda result: (place/'reached').exists())
                fence = sup.fence
                replacement = sup.store.acquire('fence-test-replacement', now_us=1, replace_generation=fence.generation)
                driver.now = 1
                check(stage+'_supervisor_reports_stale_child_held', driver.tick().state == 'HELD_OWNER_CHANGED')
                (place/'release').write_text('release disposable final clock')
                wait_file(place/'final-result.json')
                result = json.loads((place/'final-result.json').read_text())
                check(stage+'_stale_real_child_denied_at_actual_final_mutation', result['denied'] and result['dispatches'] == 0
                      and result['lane_held'] and not result['applied'] and result['signed'] == (stage == 'send'))
                if stage == 'send':
                    check('final_send_retains_original_unknown', result['stage'] == 'UNKNOWN')
                replacement.close()
            finally:
                cleanup(sup)



def _decode_hang(directory):
    time.sleep(30)
    return Inputs(directory)


@dataclass
class DecodeHang:
    directory: Path

    def __call__(self, started):
        return resources(started)

    def __reduce__(self):
        return _decode_hang, (self.directory,)


def retained_authority(directory):
    place = directory/'retained-authority'
    place.mkdir()
    f = a2.installed(rt.Fixture(place, 'retained'), profile=RestartProfile(3, 100, 0))
    f.admit()
    action = f.buy.action_id
    grant = f.repo._authority.grant
    rt.a3.control(f.repo, 'HARD_STOP', 'b1-retained-hard-stop', at=NOW+5)
    authority = f.repo._authority
    check('fixture_original_admission_and_retained_hardstop', grant is not None
          and authority.hard_stop_command == 'b1-retained-hard-stop'
          and f.repo.consumer_snapshot()['reservations'] and f.repo.audit()['attempt_count'] == 0)
    expected_authority = hashlib.sha256(repr(authority).encode()).hexdigest()
    config = dict(operations_path=f.operations_path, ledger_path=f.path, domain=f.domain,
        expected_identity=f.expected, **c4.cold_args(f))
    a2.close(f)
    original = digest(f.path)
    sup = supervisor.OperationsSupervisor(config, Inputs(place, 'hang'), supervisor.HealthProfile(20000000,20000000,1000000))
    driver = Driver(sup)
    try:
        driver.until(lambda result: bool(list(place.glob('owner-*.json'))))
        first = sup.process.pid
        for generation in (1, 2):
            record = json.loads((place/('owner-'+str(sup.process.pid)+'.json')).read_text())
            check('generation_'+str(generation)+'_retains_original_authority_action_reservation_and_no_attempt',
                  record['ledger_digest'] == original and record['authority'] == expected_authority
                  and record['entry_action_id'] == action and record['pending_attempt_id'] is None)
            if generation == 1:
                sup.process.terminate()
                sup.process.join(5)
                driver.now = 1
                driver.until(lambda result: sup.process is not None and sup.process.pid != first
                             and (place/('owner-'+str(sup.process.pid)+'.json')).exists())
        check('replacement_does_not_rearm_retained_grants_or_hardstop', digest(f.path) == original
              and sup.store.snapshot()['attempts'] == 2)
    finally:
        cleanup(sup)


def bounded_failures(directory):
    f, config, sup, driver = fixture(directory, 'preacquire', inputs=DecodeHang(directory/'preacquire'))
    try:
        driver.tick()
        driver.mono = sup._deadline
        check('startup_timeout_signals_only_exact_unverified_child', driver.tick().state == 'TERMINATING')
        sup.process.join(5)
        check('real_preacquisition_loss_holds_without_retry', driver.tick().state == 'HELD_EXIT_WITHOUT_EXACT_ACQUISITION'
              and sup.store.snapshot()['attempts'] == 0 and driver.tick().state == 'HELD_EXIT_WITHOUT_EXACT_ACQUISITION')
    finally:
        cleanup(sup)
    f, config, sup, driver = fixture(directory, 'termination', inputs=Inputs(directory/'termination', 'hang'))
    try:
        driver.until(lambda result: bool(list(f.directory.glob('owner-*.json'))))
        with patch.object(sup.process, 'terminate', return_value=None):
            driver.mono = sup._deadline
            check('termination_request_is_not_confirmed_death', driver.tick().state == 'TERMINATING' and sup.process.is_alive())
            driver.mono = sup._deadline
            check('unconfirmed_termination_latches_no_replacement', driver.tick().state == 'HELD_TERMINATION_UNCONFIRMED'
                  and sup.store.snapshot()['attempts'] == 1 and sup.process.is_alive())
    finally:
        cleanup(sup)
    f, config, sup, driver = fixture(directory, 'startup-denial', profile=RestartProfile(2,100,0))
    sup._configuration['expected_identity'] = replace(config['expected_identity'], runtime_version='wrong-reviewed-version')
    try:
        result = driver.until(lambda result: result.state == 'RESTART_EXHAUSTED')
        check('actual_startup_audit_failure_is_budgeted_until_exhaustion', result.state == 'RESTART_EXHAUSTED'
              and sup.store.snapshot()['attempts'] == 2 and not list(f.directory.glob('owner-*.json')))
    finally:
        cleanup(sup)


def bounded_driver(directory):
    f, config, sup, driver = fixture(directory, 'bounded-driver', profile=RestartProfile(3,100000000,0))
    # The caller cannot silently change the reviewed restart configuration.
    config['ledger_path'] = f.directory/'not-installed.sqlite'
    config['expected_identity'] = replace(config['expected_identity'], runtime_version='caller-edited')
    try:
        result = sup.run(max_polls=1000, interval_seconds=.005)
        check('actual_bounded_watchdog_driver_launches_and_steps_frozen_configuration',
              result.state == 'RUNNING' and result.completed_steps > 0 and not result.grants_permission)
        stopped = sup.operator_stop(now_us=time.time_ns()//1000)
        check('bounded_driver_returns_immediately_when_stopped', sup.run(max_polls=1000).state == stopped.state == 'OPERATOR_STOPPED')
    finally:
        cleanup(sup)


def main():
    with tempfile.TemporaryDirectory(prefix='live-operations-b1-') as tmp:
        directory = Path(tmp)
        recovery(directory)
        faults(directory)
        unproven_and_identity(directory)
        handshake_race(directory)
        final_fencing(directory)
        retained_authority(directory)
        bounded_failures(directory)
        bounded_driver(directory)
    print(json.dumps({'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'checks': len(CHECKS),
        'all_checks': all(CHECKS.values()), 'results': CHECKS, 'reused_fixture_checks': len(rt.CHECKS)}, sort_keys=True))


if __name__ == '__main__':
    main()
