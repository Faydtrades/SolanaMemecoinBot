"""Finite T010 whole-session work reservations; synthetic local tests only."""
from __future__ import annotations
from contextlib import nullcontext
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
from live import t010_public_host_v0_1 as host
from live import operations_supervisor_v0_1 as sup
from live.runtime_dry_public_facts_v0_1 import windows_working_set_bytes
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def fails(call):
    try:
        call()
    except (ValueError, RuntimeError, TypeError, OSError, KeyError):
        return True
    return False


def make_budget(root, *, starts=2, steps=3, target=1):
    root.mkdir()
    session = {'profile_reference':{'path':'TEST_ONLY','sha256':'a'*64},
        'resource_envelope':{'path':'TEST_ONLY','sha256':'b'*64},
        'work_budget':{'max_startups':starts,'max_runtime_steps':steps,'target_dry_terminals':target}}
    return session, host.WorkBudget(root, session, create=True)


def counter_tests(root):
    session, budget = make_budget(root/'counts', steps=3)
    check('startup_reserved_before_dispatch', budget.before_start() and budget.snapshot()['startups_reserved'] == 1)
    # Reopen means interrupted launch. Its reservation is not returned.
    reopened = host.WorkBudget(root/'counts', session)
    check('lost_launch_does_not_refund', reopened.before_start() and reopened.snapshot()['startups_reserved'] == 2)
    reopened.after_start({'terminal_roots':[],'pending_action':False})
    check('whole_session_startups_never_rollover', not reopened.before_start())
    for n in range(3):
        check('step_reserved_before_dispatch_'+str(n), reopened.before_step()
            and reopened.snapshot()['runtime_steps_reserved'] == n+1)
        reopened.after_step({'terminal_roots':[],'pending_action':False})
    check('total_steps_exhausted_not_renewed', not reopened.before_step()
        and not host.WorkBudget(root/'counts', session).before_start())
    check('read_only_observation_after_exhaustion', reopened.observation_state == 'OBSERVING_WORK_EXHAUSTED')
    check('unsolicited_or_duplicate_completion_denied', fails(lambda:reopened.after_step({'terminal_roots':[],'pending_action':False})))
    changed = dict(session, resource_envelope={'path':'TEST_ONLY','sha256':'c'*64})
    check('replacement_envelope_denied', fails(lambda:host.WorkBudget(root/'counts', changed)))
    check('missing_state_never_initializes', fails(lambda:host.WorkBudget(root/'absent', session)))
    for invalid in ({'max_startups':True,'max_runtime_steps':3,'target_dry_terminals':1},
                    {'max_startups':1,'max_runtime_steps':0,'target_dry_terminals':1},
                    {'max_startups':1,'max_runtime_steps':1,'target_dry_terminals':2}):
        check('invalid_explicit_budget_'+str(len(CHECKS)), fails(lambda:host.validate_work_budget(invalid)))


def recovery_tests(root):
    session, budget = make_budget(root/'recovery', starts=3, steps=4)
    budget.before_start()
    budget.after_start({'terminal_roots':[],'pending_action':False})
    budget.before_step()
    # Process death after reservation, possibly during Runtime work. Same total.
    recovered = host.WorkBudget(root/'recovery', session)
    check('crashed_step_remains_charged', recovered.snapshot()['runtime_steps_reserved'] == 1
        and recovered.snapshot()['pending_step'] == 1)
    recovered.before_start()
    recovered.after_start({'terminal_roots':[],'pending_action':True})
    check('recovery_uses_original_startup_and_step_budget', recovered.snapshot()['startups_reserved'] == 2
        and recovered.snapshot()['unconfirmed_steps'] == 1 and recovered.before_step())
    recovered.after_step({'terminal_roots':['d'*64],'pending_action':False})
    check('original_new_terminal_stops_all_further_work', not recovered.before_step() and not recovered.before_start()
        and recovered.observation_state == 'OBSERVING_TARGET_REACHED')
    check('duplicate_root_not_counted', recovered.snapshot()['terminal_roots'] == ['d'*64])
    check('terminal_disappearance_denied', fails(lambda:recovered.after_start({'terminal_roots':[],'pending_action':False})))
    state = recovered.snapshot()
    host.write_record(recovered.path, dict(state, pending_action=True))
    check('target_with_pending_action_retains_bounded_recovery', recovered.observation_state == 'OBSERVING_HELD_PENDING_ACTION'
        and recovered.before_start())
    host.write_record(recovered.path, dict(state, runtime_steps_reserved=state['runtime_steps_reserved']+1))
    check('corrupt_counter_accounting_denied', fails(recovered.snapshot))


def crash_child(root):
    session = json.loads((root/'fixture-session.json').read_text())
    budget = host.WorkBudget(root, session)
    budget.before_start()
    budget.after_start({'terminal_roots':[],'pending_action':False})
    budget.before_step()
    os._exit(9)


def abrupt_test(root):
    directory = root/'abrupt'
    session, budget = make_budget(directory, starts=2, steps=1)
    (directory/'fixture-session.json').write_text(json.dumps(session))
    result = subprocess.run([sys.executable,'-B',str(Path(__file__).resolve()),'--crash-child',str(directory)],
        cwd=ROOT, timeout=15, capture_output=True)
    check('abrupt_external_child_exit_confirmed', result.returncode == 9)
    reopened = host.WorkBudget(directory, session)
    check('fsynced_step_survives_process_death', reopened.snapshot()['runtime_steps_reserved'] == 1
        and reopened.snapshot()['pending_step'] == 1)
    check('last_unknown_step_prevents_restart_and_new_work', not reopened.before_start()
        and reopened.observation_state == 'OBSERVING_WORK_EXHAUSTED_PENDING_UNPROVEN')


def child_protocol():
    order = []
    class Inputs:
        def runtime_started(self, started, startup_us):
            order.append('startup-report')
            return {'terminal_roots':[],'pending_action':False}
        def __call__(self, started):
            order.append('external-facts')
            return {}
        def runtime_completed(self, started, result):
            order.append('original-work-report')
            return {'terminal_roots':['e'*64],'pending_action':False}
    class Channel:
        messages = iter(('STEP','END'))
        def send(self, value):
            order.append(value)
        def recv(self):
            return next(self.messages)
        def close(self):
            pass
    started = SimpleNamespace(audit=SimpleNamespace(owner_fence='original-fence'),
        runtime=SimpleNamespace(step=lambda **kw:SimpleNamespace(work='DRY_NON_SUBMITTED')), close=lambda:None)
    with patch.object(sup,'start_runtime',return_value=started):
        sup._runtime_child(Channel(),'test',{},Inputs(),1,None)
    check('original_child_report_precedes_parent_next_step', order == ['startup-report',
        ('STARTED',('original-fence',{'terminal_roots':[],'pending_action':False})),
        'external-facts','original-work-report',('PROGRESS',('DRY_NON_SUBMITTED',{'terminal_roots':['e'*64],'pending_action':False}))])


def supervisor_start_gate(root):
    session, budget = make_budget(root/'spawn', starts=1)
    order = []
    class Process:
        pid = 999
        def start(self):
            order.append(('START',budget.snapshot()['startups_reserved']))
    class Endpoint:
        def close(self):
            pass
    instance = object.__new__(sup.OperationsSupervisor)
    instance._configuration = {}
    instance._external_inputs = None
    instance._generation = 0
    instance._work_control = budget
    instance.health = sup.HealthProfile(1,1,1)
    instance._context = SimpleNamespace(Pipe=lambda:(Endpoint(),Endpoint()), Process=lambda **kw:Process())
    instance._facts = lambda state:state
    check('original_supervisor_reserves_before_Process_start', instance._launch(1,1) == 'STARTING'
        and order == [('START',1)])
    check('original_supervisor_exhaustion_no_second_spawn', instance._launch(2,2).startswith('OBSERVING_')
        and order == [('START',1)])
    with patch('live.operations_degradation_monitor_v0_1.observe_supervisor',side_effect=AssertionError('read-only observation')):
        instance.completed_steps = 0
        instance.last_work = None
        instance.process = None
        instance.fence = None
        check('observation_does_not_mutate_monitor', sup.OperationsSupervisor._facts(instance,'OBSERVING_TARGET_REACHED').state == 'OBSERVING_TARGET_REACHED')


def supervisor_step_gate(root):
    session, budget = make_budget(root/'dispatch', starts=2, steps=3)
    budget.before_start()
    budget.after_start({'terminal_roots':[],'pending_action':False})
    budget.before_step()  # original in-flight dispatch
    fence = sup.OwnerFence('a'*64,'b'*64,1,'test-child','c'*32)
    control = dict(domain_id=fence.domain_id,binding_digest=fence.binding_digest,generation=1,
        process_identity=fence.process_identity,nonce=fence.nonce,last_control_us=0,stopped=0,exhausted=0)
    reports = iter([{'terminal_roots':[],'pending_action':False},
        {'terminal_roots':['f'*64],'pending_action':False}])
    sent = []
    class Channel:
        def poll(self, timeout):
            return True
        def recv(self):
            return ('PROGRESS',('ORIGINAL_DRY_WORK',next(reports)))
        def send(self, message):
            sent.append((message,budget.snapshot()['runtime_steps_reserved'],budget.snapshot()['pending_step']))
    instance = object.__new__(sup.OperationsSupervisor)
    instance.store = SimpleNamespace(snapshot=lambda:control)
    instance._configuration = {}
    instance._last_mono = None
    instance._latched = instance._observation_state = None
    instance._failed = False
    instance._waiting = True
    instance._deadline = 100000
    instance._initial_generation = 0
    instance._launch_identity = 'test-child'
    instance.fence = fence
    instance.process = SimpleNamespace(exitcode=None,pid=99)
    instance._channel = Channel()
    instance._work_control = budget
    instance.health = sup.HealthProfile(100,100,100)
    instance.completed_steps = 0
    instance.last_work = None
    instance._facts = lambda state:SimpleNamespace(state=state)
    first = instance.poll(now_us=1,monotonic_us=1)
    check('supervisor_STEP_send_after_durable_reservation', first.state == 'RUNNING' and sent == [('STEP',2,2)])
    second = instance.poll(now_us=2,monotonic_us=2)
    third = instance.poll(now_us=3,monotonic_us=3)
    check('supervisor_target_report_prevents_next_STEP', second.state == third.state == 'OBSERVING_TARGET_REACHED'
        and sent == [('STEP',2,2)] and budget.snapshot()['runtime_steps_reserved'] == 2)


def target_and_binding_tests():
    excluded = SimpleNamespace(root_id='1'*64,simulation_input_digest='a'*64,attempt_id='old')
    unfinished = SimpleNamespace(root_id='2'*64,simulation_input_digest=None,attempt_id=None)
    simulated = SimpleNamespace(root_id='3'*64,simulation_input_digest='b'*64,attempt_id='exact')
    repo = SimpleNamespace(_trusted_read=lambda:nullcontext(),
        _custody=SimpleNamespace(dry_dispositions=[SimpleNamespace(input=item,sequence=n)
            for n,item in enumerate((excluded,unfinished,simulated))],reservations=[]),
        _port_at_sequence=lambda seq:SimpleNamespace(kind='DRY_NON_SUBMITTED',dry_terminal=simulated),
        _simulation_input_digest=lambda attempt:'b'*64)
    started = SimpleNamespace(runtime=SimpleNamespace(ledger=repo))
    inputs = host.PublicHostInputs(None,{},'a'*64,'grant','b'*64,({'root_id':'1'*64},))
    check('target_excludes_qualification_and_unsimulated_retirement', inputs._work_report(started) ==
        {'terminal_roots':['3'*64],'pending_action':False})
    repo._simulation_input_digest = lambda attempt:'c'*64
    check('target_requires_original_simulation_identity', fails(lambda:inputs._work_report(started)))
    profile = SimpleNamespace(configuration=lambda:{},record={'inputs':{}})
    with patch.object(host,'read_reference',side_effect=AssertionError('missing binding cannot read envelope')):
        check('unimplemented_profile_envelope_integration_fails_closed', fails(lambda:host.reviewed_work_budget(profile,{})))


def main():
    if len(sys.argv)>1:
        return crash_child(Path(sys.argv[2]))
    with tempfile.TemporaryDirectory(prefix='t010-work-budget-') as temporary:
        root = Path(temporary)
        counter_tests(root)
        recovery_tests(root)
        abrupt_test(root)
        child_protocol()
        supervisor_start_gate(root)
        supervisor_step_gate(root)
        target_and_binding_tests()
    check('native_windows_rss_without_psutil', type(windows_working_set_bytes()) is int and windows_working_set_bytes()>0)
    print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(CHECKS),
        'all_checks':all(CHECKS.values()),'results':CHECKS,'T010_executed':False},sort_keys=True))


if __name__ == '__main__':
    main()
