"""Isolated T010 host engineering tests. No public network or T010 execution."""
from __future__ import annotations
import ast
from contextlib import ExitStack
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
from live import t010_public_host_v0_1 as host
from live.operations_ownership_v0_1 import OperationsStore, RestartProfile
import live_runtime_dry_selftest_v0_1 as drytest

CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def fails(call):
    try:
        call()
    except (ValueError, RuntimeError, TypeError, OSError, KeyError, sqlite3.DatabaseError):
        return True
    return False


def await_file(path, seconds=15):
    deadline = time.perf_counter()+seconds
    while time.perf_counter() < deadline:
        if path.exists():
            return
        time.sleep(.02)
    raise AssertionError('finite disposable child timed out: '+path.name)


def probe_launcher(directory):
    child = host.detached_spawn([sys.executable, '-B', str(Path(__file__).resolve()),
        '--probe-child', str(directory)], cwd=ROOT)
    (directory/'launched.json').write_text(json.dumps({'pid':child.pid}))
    time.sleep(15)


def probe_child(directory):
    outside = host.detached_context()["remaining_job_limit_flags"] == 0
    # Use the production boundary only around a short fake loop, never Runtime.
    boundary = host.WindowsHostBoundary(host.content_fingerprint(str(directory))).enter()
    (directory/'ready.json').write_text(json.dumps({'outside_launcher_job':outside,
        'pid':os.getpid(), 'boot':host.boot_identity()}))
    end = time.perf_counter()+5
    while time.perf_counter() < end and not (directory/'launcher-dead').exists():
        time.sleep(.02)
    time.sleep(.15)
    (directory/'survived.json').write_text(json.dumps({'survived_launcher':(directory/'launcher-dead').exists()}))
    os._exit(0)


def detachment(directory):
    launched = subprocess.Popen([sys.executable, '-B', str(Path(__file__).resolve()),
        '--probe-launcher', str(directory)], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        await_file(directory/'ready.json')
        record = json.loads((directory/'ready.json').read_text())
        check('detached_child_no_remaining_launcher_job_limits', record['outside_launcher_job'])
        launched.terminate()
        launched.wait(timeout=5)
        (directory/'launcher-dead').write_text('exact launcher handle exited')
        await_file(directory/'survived.json')
        check('detached_fake_loop_survives_launcher_death', json.loads((directory/'survived.json').read_text())['survived_launcher'])
        check('native_boot_identity_stable', host.boot_identity() == host.boot_identity() == record['boot'])
    finally:
        if launched.poll() is None:
            launched.terminate()
            launched.wait(timeout=5)
        if launched.stderr:
            detail = launched.stderr.read().decode(errors='replace')
            if detail:
                print(detail, file=sys.stderr)


def lifetimes():
    utc = 1700000000000000
    start = {'entry_valid_from_utc':host.utc_from_us(utc),
        'entry_valid_through_utc':host.utc_from_us(utc+host.HARD_EXPIRY_US)}
    life = host.Lifetime.begin(start, start, utc_us=utc, qpc_ns=1000000, boot_id='boot')
    check('original_12h_target_14h_limit', life.observation_target_utc_us-utc == host.OBSERVATION_US
        and life.hard_expiry_utc_us-utc == host.HARD_EXPIRY_US)
    restored = host.Lifetime(**json.loads(json.dumps(asdict(life))))
    check('restart_preserves_original_deadlines', restored == life)
    for name, now, qpc, boot, expected in (
        ('within_window', utc+1, 1001000, 'boot', None),
        ('utc_target', utc+host.OBSERVATION_US, 1001000, 'boot', 'OBSERVATION_TARGET_REACHED'),
        ('qpc_target', utc+1, 1000000+host.OBSERVATION_US*1000, 'boot', 'OBSERVATION_TARGET_REACHED'),
        ('utc_hard_expiry', utc+host.HARD_EXPIRY_US, 1001000, 'boot', 'HARD_EXPIRED'),
        ('qpc_hard_expiry', utc+1, 1000000+host.HARD_EXPIRY_US*1000, 'boot', 'HARD_EXPIRED'),
        ('new_boot_denied', utc+1, 1001000, 'new', 'CLOCK_BOOT_DISCONTINUITY'),
        ('qpc_regression_denied', utc+1, 999999, 'boot', 'CLOCK_BOOT_DISCONTINUITY'),
        ('utc_regression_denied', utc-1, 1001000, 'boot', 'UTC_REGRESSION')):
        check(name, life.reason(utc_us=now, qpc_ns=qpc, boot_id=boot) == expected)
    early = replace(life, policy_expiry_utc_us=utc+3)
    check('independent_policy_expiry', early.reason(utc_us=utc+3, qpc_ns=1001000, boot_id='boot') == 'AUTHORITY_WINDOW_EXPIRED')
    early = replace(life, grant_expiry_utc_us=utc+3)
    check('independent_grant_expiry', early.reason(utc_us=utc+3, qpc_ns=1001000, boot_id='boot') == 'AUTHORITY_WINDOW_EXPIRED')
    wide = dict(start, entry_valid_through_utc=host.utc_from_us(utc+host.HARD_EXPIRY_US+1))
    check('unbounded_policy_denied', fails(lambda:host.Lifetime.begin(wide, start, utc_us=utc, qpc_ns=0, boot_id='boot')))
    check('unbounded_grant_denied', fails(lambda:host.Lifetime.begin(start, wide, utc_us=utc, qpc_ns=0, boot_id='boot')))


def request_bindings():
    profile_record = {'inputs':{'qualification_substitutions':{'scope':'PUBLIC_ENVIRONMENT_REVIEWED'}}}
    request = {key:{'path':key, 'sha256':'a'*64} for key in
        ('profile','dossier','package','qualification','facts','handoff','resource_envelope')}
    request.update(schema=host.SCHEMA, health={}, journal_root='unused', execution_review_reference='TEST_ONLY')
    dossier = {'supported_dry':{key:request[key] for key in ('profile','qualification')}}
    for case in ('same','profile','qualification','missing'):
        changed = json.loads(json.dumps(dossier))
        if case == 'missing':
            changed['supported_dry'] = None
        elif case != 'same':
            changed['supported_dry'][case]['sha256'] = 'b'*64
        with ExitStack() as stack:
            stack.enter_context(patch.object(host,'read_reference',side_effect=[profile_record,changed,{'dry_configuration':profile_record},{}]))
            stack.enter_context(patch.object(host,'load_profile',return_value=SimpleNamespace(record=profile_record)))
            stack.enter_context(patch.object(host,'preflight',return_value={'ready':True}))
            validator = stack.enter_context(patch.object(host,'validate_evidence',side_effect=ValueError('EXPECTED_BINDING_GATE_REACHED')))
            try:
                host.validate_request(request, ROOT, utc_us=1)
            except ValueError as exc:
                expected = 'EXPECTED_BINDING_GATE_REACHED' if case == 'same' else 'T010_HOST_EXACT_DOSSIER_QUALIFICATION_REQUIRED'
                check('dossier_exact_reference_'+case, str(exc) == expected and validator.call_count == (1 if case == 'same' else 0))
            else:
                raise AssertionError('binding gate not exercised')


def session_fixture(directory):
    domain = drytest.a3.domain(mode='DRY')
    store = OperationsStore.initialize(directory/'operations.sqlite', domain, RestartProfile(4, 1000000000, 0), now_us=1)
    owner = store.acquire('qualified-original', now_us=2)
    owner.close()
    control = store.snapshot()
    now = time.time_ns()//1000
    session = {'schema':host.STATE_SCHEMA, 'journal_root':str(directory.resolve()),
        'domain':domain.to_record(), 'domain_id':domain.economic_domain_id,
        'operations_path':str(store.path), 'initial_control':control, 'run_id':'test-original',
        'work_budget':{'max_startups':3,'max_runtime_steps':8,'target_dry_terminals':1},
        'lifetime':asdict(host.Lifetime(now, time.perf_counter_ns(), host.boot_identity(),
            now+host.OBSERVATION_US, now+host.HARD_EXPIRY_US, now+host.HARD_EXPIRY_US, now+host.HARD_EXPIRY_US))}
    host.write_record(directory/'session.json', session)
    host.WorkBudget(directory, session, create=True)
    host._owner_record(directory, session, control, launch_id='initial')
    host.write_record(directory/'status.json', {'session_digest':host.content_fingerprint(session),
        'state':'RUNNING', 'observed_utc_us':now, 'completed_steps':3,
        'host_launch_id':'original-host', 'supervisor_pid':123456})
    return session, domain, store


def controls(directory):
    session, domain, store = session_fixture(directory)
    before = store.snapshot()
    file_hash = host.sha(store.path)
    readonly = OperationsStore(store.path, domain, readonly=True)
    check('readonly_snapshot_identical', readonly.snapshot() == before)
    with readonly._connection() as conn:
        check('sqlite_query_only_enforced', conn.execute('PRAGMA query_only').fetchone()[0] == 1)
        check('sqlite_readonly_write_denied', fails(lambda:conn.execute('UPDATE operations SET attempts=0')))
    check('readonly_acquire_denied', fails(lambda:readonly.acquire('not-allowed', now_us=5)))
    check('readonly_stop_denied', fails(lambda:readonly.operator_stop(now_us=5)))
    check('readonly_reset_denied', fails(lambda:readonly.operator_reset(expected_generation=before['generation'], now_us=5)))
    with patch.object(host, 'validate_request', side_effect=AssertionError('status/stop must not preflight')):
        status = host.status(directory)
        check('status_historical_exact_process_identity', status['recorded_supervisor_pid'] == 123456
            and status['recorded_host_launch_id'] == 'original-host' and 'NOT_PROCESS_LIVENESS' in status['health_scope'])
        check('status_no_store_or_file_mutations', host.sha(store.path) == file_hash and store.snapshot() == before)
        check('stale_package_stop_durable', host.stop(directory, now_us=0)['state'] == 'STOP_COMMITTED')
    stopped = store.snapshot()
    check('stop_retains_budget_attempts', stopped['stopped'] and stopped['budget_id'] == before['budget_id']
        and stopped['attempts'] == before['attempts'] and stopped['generation'] == before['generation']+1)
    host.stop(directory, now_us=0)
    check('stop_idempotent_original_generation', store.snapshot() == stopped)
    check('changed_owner_hold', fails(lambda:host._matching_owner(dict(before, generation=before['generation']+1), before)))
    check('reset_budget_hold', fails(lambda:host._matching_owner(dict(before, budget_id='a'*32), before)))
    (directory/'owner.json').write_text('{}')
    check('corrupt_owner_status_denied', fails(lambda:host.status(directory)))
    check('stop_succeeds_despite_corrupt_owner_evidence', host.stop(directory)['state'] == 'STOP_COMMITTED')


def expired_restart(directory):
    session, domain, store = session_fixture(directory)
    session['lifetime']['hard_expiry_utc_us'] = 1
    host.write_record(directory/'session.json', session)
    with patch.object(host, 'validate_request', side_effect=AssertionError('expired run cannot require approvals')):
        result = host.run({'journal_root':str(directory)}, ROOT, launch_id='expired-restart', detached_required=False)
    check('expired_restart_stops_before_stale_preflight', result == 0 and store.snapshot()['stopped'] == 1)
    check('expired_restart_no_new_owner', store.snapshot()['attempts'] == 1)


def reconcile_original(directory):
    for point in ('before-preparation', 'after-preparation', 'after-simulation'):
        f, external, scenario, action, seed = drytest.fixture(directory, 'host-'+point)
        target = 'record_external_attempt_stage' if point == 'after-simulation' else 'prepare_attempt'
        original = getattr(f.repo, target)
        def interrupt(*args, **kwargs):
            if point != 'before-preparation':
                original(*args, **kwargs)
            raise drytest.Interrupted(point)
        try:
            with patch.object(f.repo, target, side_effect=interrupt):
                drytest.run(f, action, seed)
        except drytest.Interrupted:
            pass
        attempts = f.repo._root_attempts(action.root_id)
        lineage = None if not attempts else attempts[0].preparation
        original_authority = f.repo._authority.content_digest
        f.repo.close()
        store = OperationsStore.initialize(directory/(point+'-ops.sqlite'), f.domain, RestartProfile(4,1000000000,0), now_us=1)
        owned = store.acquire('original', now_us=2)
        owned.close()
        initial = store.snapshot()
        store.operator_stop(now_us=3)
        stopped = store.snapshot()
        session = {'operations_path':str(store.path), 'ledger_path':str(f.path), 'initial_control':initial}
        with patch.object(host, 'ReviewedPublicFacts', side_effect=AssertionError('no public reads in reconcile')):
            result = host.reconcile_stopped_ledger(session, f.domain)
        check(point+'_original_terminal_reconciled', result['ledger_retirement_performed'] and not result['pending_original_actions'])
        check(point+'_operations_generation_budget_stop_preserved', store.snapshot() == stopped)
        file_hash = host.sha(f.path)
        again = host.reconcile_stopped_ledger(session, f.domain)
        check(point+'_reconcile_idempotent_no_new_ledger_writer', not again['ledger_retirement_performed'] and host.sha(f.path) == file_hash)
        f.reopen()
        check(point+'_original_policy_grant_authority_preserved', f.repo._authority.content_digest == original_authority)
        if lineage:
            check(point+'_original_attempt_bytes_preserved', f.repo.attempt(lineage.attempt_id).preparation == lineage)
        check(point+'_no_economic_posting_or_broadcast', f.repo.audit()['posting_count'] == 0 and f.repo.audit()['chain_receipt_count'] == 0)
        f.close()


def host_loop(directory):
    session, domain, store = session_fixture(directory)
    resource_path = directory/'test-only-resource-interface.json'
    resource_path.write_text(json.dumps({'derivation': {}}))
    request = {"journal_root":str(directory), "resource_envelope":
        {"path":str(resource_path), "sha256":host.sha(resource_path)}}
    session.update(request_digest=host.content_fingerprint(request),
        interpreter_path=str(Path(sys.executable).resolve()), interpreter_sha256=host.sha(sys.executable),
        policy_digest="a"*64, grant_digest="b"*64, grant_id="original")
    host.write_record(directory/"session.json", session)
    budget_state = host.read_record(directory/"work_budget.json")
    budget_state["session_digest"] = host.content_fingerprint(session)
    host.write_record(directory/"work_budget.json", budget_state)
    host._owner_record(directory, session, store.snapshot(), launch_id="original")
    original_control = store.snapshot()
    seen = {}
    class FakeBoundary:
        def __init__(self, domain_id):
            seen['boundary_domain'] = domain_id
        def enter(self):
            return self
    class FakeSupervisor:
        def __init__(self, configuration, inputs, health, *, expected_generation, work_control):
            seen['expected_generation'] = expected_generation
            seen['input_type'] = type(inputs)
            self.budget = work_control
            self.budget.before_start()
            self.budget.after_start({'terminal_roots':[],'pending_action':False})
            self.owner = store.acquire('fake-child-exact', now_us=original_control['last_control_us']+1,
                replace_generation=expected_generation)
            self.fence = self.owner.fence
            self.process = None
        def poll(self, **kwargs):
            self.budget.before_step()
            self.budget.after_step({'terminal_roots':['c'*64],'pending_action':False})
            return SimpleNamespace(state='OBSERVING_TARGET_REACHED', completed_steps=1, generation=self.fence.generation,
                last_work='FAKE_EXTERNAL_BOUNDARY_ONLY')
        def alert_snapshot(self):
            return host.HealthProfile(1,2,3)  # dataclass serialization boundary
    profile = SimpleNamespace(configuration=lambda:{'operations_path':store.path,'domain':domain})
    with ExitStack() as stack:
        stack.enter_context(patch.object(host, 'WindowsHostBoundary', FakeBoundary))
        stack.enter_context(patch.object(host, 'OperationsSupervisor', FakeSupervisor))
        stack.enter_context(patch.object(host, 'validate_request', return_value=(profile, {}, {'terminals':[]}, {}, host.HealthProfile(100,100,100))))
        stack.enter_context(patch.object(host.Lifetime, 'reason', side_effect=[None,None,None,'OBSERVATION_TARGET_REACHED']))
        result = host.run(request, ROOT, launch_id='restart-loop', detached_required=False)
    check('host_loop_uses_original_generation_CAS', seen['expected_generation'] == original_control['generation'])
    check('host_loop_fixed_public_inputs', seen['input_type'] is host.PublicHostInputs)
    check('host_loop_original_domain_boundary', seen['boundary_domain'] == domain.economic_domain_id)
    check('host_loop_target_durable_stop', result == 0 and store.snapshot()['stopped'] == 1)
    check('host_loop_preserves_original_budget_and_attempt_count', store.snapshot()['budget_id'] == original_control['budget_id']
        and store.snapshot()['attempts'] == original_control['attempts']+1)
    check('host_loop_original_expiry_not_renewed', host.read_record(directory/'session.json')['lifetime'] == session['lifetime'])
    check('host_loop_sanitized_dataclass_health_serializable', host.read_record(directory/'status.json')['durable_stop'])
    check('host_loop_durable_process_identity', host.read_record(directory/'process-restart-loop.json')['supervisor_pid'] == os.getpid())


def child_startup_hook():
    from live import operations_supervisor_v0_1 as supervisor
    measured = []
    class Inputs:
        def runtime_started(self, started, startup_us):
            measured.append(startup_us)
    class Channel:
        def send(self, value):
            measured.append(value[0])
        def recv(self):
            raise EOFError
        def close(self):
            pass
    started = SimpleNamespace(audit=SimpleNamespace(owner_fence='test-fence'), close=lambda:None)
    with patch.object(supervisor, 'start_runtime', return_value=started), patch.object(supervisor, 'perf_counter_ns', side_effect=[1000,251000]):
        supervisor._runtime_child(Channel(), 'launch', {}, Inputs(), 1, None)
    check('actual_child_startup_measured_before_handshake', measured == [250,'STARTED'])


def structure():
    tree = ast.parse((ROOT/'src/live/t010_public_host_v0_1.py').read_text(encoding='utf-8-sig'))
    calls = {n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    check('host_no_initialize_reset_arming_sign_send', not {'initialize','operator_reset','record_authority_control','sign','send_transaction','send_raw_transaction'} & calls)
    check('host_original_supervisor_and_public_facts_only', 'OperationsSupervisor(' in ast.unparse(tree)
        and 'ReviewedPublicFacts(' in ast.unparse(tree) and '.profile.driver(' in ast.unparse(tree))


def main():
    if len(sys.argv)>1:
        directory = Path(sys.argv[2])
        return probe_launcher(directory) if sys.argv[1] == '--probe-launcher' else probe_child(directory)
    lifetimes()
    request_bindings()
    structure()
    child_startup_hook()
    with tempfile.TemporaryDirectory(prefix='t010-host-fake-') as temporary:
        root = Path(temporary)
        for name, call in [('control',controls), ('expiry',expired_restart), ('reconcile',reconcile_original), ('loop',host_loop), ('detach',detachment)]:
            directory = root/name
            directory.mkdir()
            call(directory)
    print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(CHECKS),
        'all_checks':all(CHECKS.values()),'results':CHECKS,'T010_executed':False},sort_keys=True))


if __name__ == '__main__':
    main()
