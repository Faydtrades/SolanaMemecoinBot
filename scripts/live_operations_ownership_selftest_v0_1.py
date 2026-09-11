"""A1 actual Runtime/Q2/Q3 ownership; disposable stores and mock I/O only."""
from __future__ import annotations

import json
import pickle
import sqlite3
import subprocess
import sys
import tempfile
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_runtime_composition_selftest_v0_1 as rt
from live.operations_ownership_v0_1 import OperationsStore, RestartProfile
from live.runtime_composition_v0_1 import RuntimeCompositionV01
from solders.keypair import Keypair

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


def enter(owner, domain):
    with owner.mutation_guard(domain):
        return True


def child(store, operation):
    code = """import sys,json,sqlite3
sys.path.insert(0,sys.argv[1])
from live.operations_ownership_v0_1 import OperationsStore
from live.ledger_domain_v0_1 import LedgerDomain
record=json.loads(sys.argv[3])
record["expected_empty_token_accounts"]=tuple(record["expected_empty_token_accounts"])
s=OperationsStore(sys.argv[2],LedgerDomain(**record))
try:
 if sys.argv[4]=='stop': s.operator_stop(now_us=100)
 else: s.acquire('child',now_us=100)
except (RuntimeError,sqlite3.DatabaseError): print('DENIED')
else: print('CONTROL_COMMITTED')
"""
    result = subprocess.run([sys.executable, '-B', '-c', code, str(ROOT/'src'), str(store.path),
        json.dumps(rt.a3.domain().to_record()), operation], capture_output=True, text=True, timeout=20)
    if result.returncode:
        print(result.stderr)
    check('child_exit_'+operation, result.returncode == 0)
    return result.stdout.strip()


def controls(directory):
    domain = rt.a3.domain()
    path = directory/'controls.sqlite'
    store = OperationsStore.initialize(path, domain, RestartProfile(2, 100, 10), now_us=0)
    owner = store.acquire('parent', now_us=0)
    check('current_exact_owner', enter(owner, domain))
    check('competing_process_denied', child(store, 'acquire') == 'DENIED')
    reopened = OperationsStore(path, domain)
    check('reopen_only_facts_no_capability', reopened.snapshot() == store.snapshot()
        and fails(lambda: reopened.acquire('restart', now_us=10)))
    check('memory_capability_not_serializable', fails(lambda: pickle.dumps(owner)))
    with owner.mutation_guard(domain):
        check('cross_process_stop_cannot_commit_through_dispatch', child(store, 'stop') == 'DENIED')
        check('cross_process_safe_read_during_dispatch', reopened.snapshot()['nonce'] == owner.fence.nonce)
    check('backoff_retained', fails(lambda: reopened.acquire('early', now_us=9,
        replace_generation=owner.fence.generation)))
    replacement = reopened.acquire('replacement', now_us=10, replace_generation=owner.fence.generation)
    check('takeover_stale_owner_denied', fails(lambda: enter(owner, domain)) and enter(replacement, domain))
    check('stale_takeover_CAS_denied', fails(lambda: store.acquire('old', now_us=20,
        replace_generation=owner.fence.generation)))
    check('finite_attempts_consumed', store.snapshot()['attempts'] == 2)
    check('exhaustion_attempt_denied', fails(lambda: store.acquire('third', now_us=1000,
        replace_generation=replacement.fence.generation)))
    exhausted = store.snapshot()
    check('exhaustion_latched_past_window_and_revokes_owner', exhausted['exhausted'] == 1
        and exhausted['attempts'] == 2 and fails(lambda: enter(replacement, domain)))
    check('reopen_cannot_clear_exhaustion', fails(lambda: OperationsStore(path, domain).acquire('later', now_us=2000)))
    store.operator_stop(now_us=1001)
    stopped = store.snapshot()
    check('operator_stop_survives_reopen_child', OperationsStore(path, domain).snapshot()['stopped'] == 1
        and child(store, 'acquire') == 'DENIED')
    store.operator_reset(expected_generation=stopped['generation'], now_us=1002)
    reset = store.snapshot()
    check('explicit_operator_boundary_new_budget_no_owner', reset['budget_id'] != exhausted['budget_id']
        and reset['attempts'] == reset['stopped'] == reset['exhausted'] == 0 and reset['nonce'] is None)
    check('operator_reset_replay_denied', fails(lambda: store.operator_reset(
        expected_generation=stopped['generation'], now_us=1002)))
    fresh = store.acquire('fresh', now_us=1002)
    check('old_memory_does_not_revive_after_reset', enter(fresh, domain) and fails(lambda: enter(owner, domain)))
    fresh.close()
    check('closed_capability_denied', fails(lambda: enter(fresh, domain)))
    check('clock_regression_denied', fails(lambda: store.operator_stop(now_us=1001)))
    check('domain_configuration_conflict_denied', fails(lambda: OperationsStore(path,
        replace(domain, minimum_context_slot=domain.minimum_context_slot+1))))
    check('wrong_wallet_guard_denied', fails(lambda: enter(owner, replace(domain, wallet=str(Keypair().pubkey())))))
    other = OperationsStore.initialize(directory/'window.sqlite', domain, RestartProfile(3, 100, 10), now_us=0)
    initial = other.acquire('initial', now_us=0)
    window = other.acquire('window', now_us=100, replace_generation=initial.fence.generation)
    check('nonexhausted_window_rollover_retains_budget', other.snapshot()['attempts'] == 1
        and other.snapshot()['window_start_us'] == 100 and enter(window, domain))
    original_path = other.path
    original_path.rename(directory/'replaced.sqlite')
    OperationsStore.initialize(original_path, domain, RestartProfile(3, 100, 10), now_us=0)
    check('replaced_store_does_not_revive_memory', fails(lambda: enter(window, domain)))
    for field, value in [('attempts', -1), ('exhausted', 2), ('nonce', 'invalid'), ('next_attempt_us', -1)]:
        corrupt = directory/('invalid-'+field+'.sqlite')
        bad = OperationsStore.initialize(corrupt, domain, RestartProfile(3, 100, 10), now_us=0)
        with sqlite3.connect(corrupt) as conn:
            conn.execute('UPDATE operations SET '+field+'=?', (value,))
        check('malformed_'+field+'_fails_closed', fails(bad.snapshot))
    check('missing_store_not_initialized', fails(lambda: OperationsStore(directory/'missing.sqlite', domain)))


def integrated(directory, stage, transition):
    name = stage+'-'+transition
    key = Keypair()
    with patch.object(rt.sf, 'WALLET', str(key.pubkey())), patch.object(rt.sf.plans, 'ACTOR', str(key.pubkey())):
        f = rt.Fixture(directory, name)
        try:
            ops = OperationsStore.initialize(directory/(name+'-ops.sqlite'), f.domain,
                RestartProfile(4, 1000, 0), now_us=0)
            owner = ops.acquire('runtime', now_us=0)
            f.runtime = RuntimeCompositionV01(f.producer, f.handoff, f.repo, f.source, ownership=owner)
            f.admit()
            authority = f.repo._authority
            before = f.repo.consumer_snapshot()
            if stage == 'prestep':
                rt.a3.control(f.repo, 'HARD_STOP', 'existing-authority-hard-stop', at=rt.NOW+4)
                authority = f.repo._authority
                before = f.repo.consumer_snapshot()
                ops.operator_stop(now_us=1)
                held = f.step()
                check(name+'_no_new_work', held.work == 'OPERATIONS_HELD' and f.repo.consumer_snapshot() == before)
                check(name+'_safe_audit', f.repo.audit()['attempt_count'] == 0)
                check(name+'_Authority_unchanged', f.repo._authority == authority)
                ops.operator_reset(expected_generation=ops.snapshot()['generation'], now_us=2)
                ops.acquire('restart', now_us=2)
                check(name+'_reset_preserves_existing_hardstop_policy_grant', f.repo._authority == authority
                    and authority.hard_stop_command is not None and authority.policy is not None)
                return
            calls, final_send_clocks, triggered = [], [], []
            def clock():
                calls.append(None)
                value = rt.a3.clock(f.repo, rt.NOW+6)
                stamp = (datetime.fromisoformat(value.utc_upper_utc)+timedelta(microseconds=len(calls)-1)).isoformat(timespec='microseconds')
                caller = sys._getframe(1).f_code.co_name
                if caller == '_last_call':
                    final_send_clocks.append(None)
                target = caller == 'sign_exact' if stage == 'sign' else caller == '_last_call' and len(final_send_clocks) == 2
                if target and transition != 'normal' and not triggered:
                    triggered.append(True)
                    if transition == 'stop':
                        ops.operator_stop(now_us=1)
                    else:
                        ops.acquire('replacement', now_us=1, replace_generation=owner.fence.generation)
                return replace(value, utc_lower_utc=stamp, utc_upper_utc=stamp,
                    monotonic_ns=value.monotonic_ns+1000*(len(calls)-1))
            def step(at, **kwargs):
                return f.runtime.step(clock=clock, source_cut_utc=rt.a3.utc(rt.NOW+1), **kwargs)
            f.step = step
            dispatched = []
            original_boundary = rt.q3.Boundary.__call__
            def boundary(transport, request):
                dispatched.append(True)
                check(name+'_dispatch_holds_stop_serialization', fails(lambda: ops.operator_stop(now_us=2)))
                return original_boundary(transport, request)
            with patch.object(rt.q3.Boundary, '__call__', boundary):
                if transition == 'normal':
                    result, read, sent = rt.execution(f, key, f.buy, at=rt.NOW+6, number=71)
                    check(name+'_actual_runtime_dispatch', result.work == 'SUBMISSION_OBSERVED' and len(sent.requests) == 1)
                    ops.operator_stop(now_us=2)
                    check(name+'_stop_commits_after_dispatch', ops.snapshot()['stopped'] == 1)
                else:
                    check(name+'_actual_execution_denied', fails(lambda: rt.execution(f, key, f.buy, at=rt.NOW+6, number=71)))
                    attempts = f.repo.consumer_snapshot()['pending_attempts']
                    attempt = f.repo.attempt(attempts[0])
                    check(name+'_final_clock_injection_reached', bool(triggered))
                    check(name+'_zero_transport_calls', not dispatched)
                    check(name+'_original_attempt_held', attempt.lane_held and not attempt.economically_applied)
                    check(name+'_signature_state', (attempt.primary_signature is None) == (stage == 'sign'))
                    if stage == 'send':
                        check(name+'_possible_send_UNKNOWN_preserved', attempt.recorded_stage == 'UNKNOWN')
            # Economic policy, hard-stop and grants are untouched by Operations.
            current = f.repo._authority
            check(name+'_Authority_controls_preserved', current.policy == authority.policy
                and current.hard_stop_command == authority.hard_stop_command and current.grant == authority.grant)
            check(name+'_safe_ledger_audit_after_loss', f.repo.audit()['attempt_count'] == 1)
        finally:
            f.close()


def rebroadcast(directory):
    q3 = rt.q3
    with q3.fixture(directory, 'owned-rebroadcast') as (f, production, envelope, fresh, boundary, transport, at):
        store = OperationsStore.initialize(directory/'rebroadcast-ops.sqlite', f.repo.domain,
            RestartProfile(3, 1000, 0), now_us=0)
        owner = store.acquire('sender', now_us=0)
        q3.invoke(f, fresh, transport, at, ownership=owner)
        repeated = q3.control.consume(f, production.original,
            q3.control.request(f, envelope.preparation, 'rebroadcast', 'REBROADCAST', 1), at=at+1)
        samples = q3.clocks(f, at+2)
        calls = []
        def clock():
            calls.append(None)
            if len(calls) == 2:
                store.acquire('replacement', now_us=1, replace_generation=owner.fence.generation)
            return samples()
        check('rebroadcast_final_fence_loss_denied', fails(lambda: q3.invoke(f, repeated, transport, at+1,
            clock=clock, ownership=owner)))
        check('rebroadcast_no_second_dispatch', len(calls) == 2 and len(boundary.requests) == 1)
        check('rebroadcast_original_signature_unknown_retained',
            f.repo.attempt(envelope.preparation.attempt_id).primary_signature == envelope.primary_signature
            and f.repo.attempt(envelope.preparation.attempt_id).recorded_stage == 'UNKNOWN')


def main():
    with tempfile.TemporaryDirectory(prefix='live-operations-a1-') as tmp:
        directory = Path(tmp)
        controls(directory)
        integrated(directory, 'prestep', 'stop')
        for stage in ('sign', 'send'):
            for transition in ('stop', 'takeover'):
                integrated(directory, stage, transition)
        integrated(directory, 'send', 'normal')
        rebroadcast(directory)
    print(json.dumps({'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'checks': len(CHECKS),
        'all_checks': all(CHECKS.values()), 'check_names': sorted(CHECKS)}, sort_keys=True))


if __name__ == '__main__':
    main()
