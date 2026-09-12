"""M46-only actual supervisor process replacement on byte-copied C1 stores.

Never reruns Step-9/10/11-A measurements. The accepted measurement supplies
public typed identity only. No initialization, setup/signing helper or port.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import ctypes
from ctypes import wintypes as w
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_windows_host_v0_1 as host
import live_operations_deployed_measurement_v0_1 as retained
from live.operations_ownership_v0_1 import OperationsStore, OperationsOwnershipError
from live.operations_startup_v0_1 import configured_identity

PUBLIC = Path(r'D:\Tradingbot\meme_live\config\deployment_public_v1.json')
PUBLIC_SHA = '8d937bb7bc0428d652fca4fd6b53553d72ce49233e9644c3e6807337949be025'
PROFILE = Path(r'D:\Tradingbot\meme_live\evidence\step11a-3630b2\m52-profile-02\profile.json')
PROFILE_SHA = 'fc924b5d572211b82a889b6933e9fb191bea9c9e9e2e82c95d2fd7a89fdc6440'
EXTENSION = PROFILE.parent.parent/'m53-recovery-03/profile-extension.json'
EXTENSION_DIGEST = '1ce618084995c7126a668c7c948784195be3f8afe53901bc9acd95ba10cd3455'
ENTRY = ROOT/'scripts/live_operations_windows_host_v0_1.py'


def check(value, reason):
    if not value:
        raise AssertionError(reason)


def read_control(m):
    c = host.configuration(m)
    return OperationsStore(c['operations_path'], c['domain']).snapshot()


def economics(m):
    path = m['startup']['paths']['ledger']
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro', uri=True)) as db:
        check(db.execute('PRAGMA integrity_check').fetchall() == [('ok',)], 'ledger integrity')
        check(not db.execute('PRAGMA foreign_key_check').fetchall(), 'ledger foreign keys')
        # Original cold acquisition appends writer-generation fencing metadata.
        # Preserve every other row plus the head revision/receipt digest exactly.
        tables = {}
        for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
            if name == 'ledger_writer_generations':
                continue
            if name == 'ledger_head':
                tables[name] = db.execute('SELECT singleton,revision,last_receipt_digest FROM ledger_head').fetchall()
            else:
                tables[name] = db.execute('SELECT * FROM "'+name+'" ORDER BY rowid').fetchall()
        return retained.canonical_digest(tables)


def writer_history(m):
    with closing(sqlite3.connect(Path(m['startup']['paths']['ledger']).as_uri()+'?mode=ro',uri=True)) as db:
        return db.execute('SELECT * FROM ledger_writer_generations ORDER BY generation').fetchall()


def journal_events(m):
    result = []
    root = Path(m['journal_root'])
    for path in sorted(root.glob('*.jsonl')):
        for line in path.read_text().splitlines():
            try:
                result.append(json.loads(line))
            except json.JSONDecodeError:
                pass  # a live append can be incomplete; wait for next observation
    return result


def launch(path, label):
    output = path.parent/(label+'.stdout.txt')
    stream = output.open('x', encoding='utf-8')
    process = subprocess.Popen([sys.executable, '-B', str(ENTRY), '--manifest', str(path),
        '--sha256', host.sha(path)], cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NO_WINDOW)
    stream.close()
    process.m46_manifest = host.read(path)
    process.m46_supervisor_handle = None
    return process, output


def wait_running(process, m):
    deadline = time.monotonic()+35
    while time.monotonic() < deadline:
        check(process.poll() is None, 'host exited before actual startup')
        for event in journal_events(m):
            facts = event.get('facts', {})
            if (facts.get('state') == 'RUNNING' and facts.get('completed_steps', 0) > 0
                    and retain_supervisor(process, event['supervisor_pid'])):
                return event
        time.sleep(0.1)
    raise AssertionError('actual host startup deadline')


def retained_process_handle(pid):
    # Only a child PID emitted by our launched, retained supervisor handle.
    # Open before killing its parent, preventing PID reuse from affecting proof.
    api = ctypes.WinDLL('kernel32', use_last_error=True)
    api.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
    api.OpenProcess.restype = w.HANDLE
    api.WaitForSingleObject.argtypes = [w.HANDLE, w.DWORD]
    api.WaitForSingleObject.restype = w.DWORD
    api.CloseHandle.argtypes = [w.HANDLE]
    api.CloseHandle.restype = w.BOOL
    handle = api.OpenProcess(0x100000, False, pid)  # SYNCHRONIZE only, no kill right
    check(bool(handle), 'retain exact runtime handle')
    return api, handle


def retain_supervisor(process, pid):
    """Venv redirectors are launchers; prove and retain their actual child.

    NtQueryInformationProcess reads parent identity through the opened handle.
    No process name matching or terminating by rediscovered PID is used.
    """
    if process.m46_supervisor_handle is not None:
        return process.m46_supervisor_handle[2] == pid
    api = ctypes.WinDLL('kernel32', use_last_error=True)
    api.OpenProcess.argtypes, api.OpenProcess.restype = [w.DWORD,w.BOOL,w.DWORD],w.HANDLE
    api.CloseHandle.argtypes, api.CloseHandle.restype = [w.HANDLE],w.BOOL
    handle = api.OpenProcess(0x100000 | 0x400 | 1, False, pid)
    if not handle:
        return False
    class BasicInformation(ctypes.Structure):
        _fields_ = [('ExitStatus',ctypes.c_void_p),('Peb',ctypes.c_void_p),
            ('Affinity',ctypes.c_void_p),('BasePriority',ctypes.c_void_p),
            ('UniquePid',ctypes.c_void_p),('ParentPid',ctypes.c_void_p)]
    info = BasicInformation()
    query = ctypes.WinDLL('ntdll').NtQueryInformationProcess
    query.argtypes = [w.HANDLE,w.ULONG,ctypes.c_void_p,w.ULONG,ctypes.c_void_p]
    query.restype = w.LONG
    valid = query(handle,0,ctypes.byref(info),ctypes.sizeof(info),None) == 0
    api.QueryFullProcessImageNameW.argtypes = [w.HANDLE,w.DWORD,w.LPWSTR,ctypes.POINTER(w.DWORD)]
    api.QueryFullProcessImageNameW.restype = w.BOOL
    size = w.DWORD(32768)
    image = ctypes.create_unicode_buffer(size.value)
    valid = valid and bool(api.QueryFullProcessImageNameW(handle,0,image,ctypes.byref(size)))
    api.GetProcessTimes.argtypes = [w.HANDLE,ctypes.POINTER(w.FILETIME),ctypes.POINTER(w.FILETIME),
        ctypes.POINTER(w.FILETIME),ctypes.POINTER(w.FILETIME)]
    api.GetProcessTimes.restype = w.BOOL
    def created(value):
        times = [w.FILETIME() for _ in range(4)]
        check(bool(api.GetProcessTimes(value,*(ctypes.byref(x) for x in times))), 'retained process creation time')
        return times[0].dwHighDateTime*2**32+times[0].dwLowDateTime
    valid = (valid and Path(image.value).resolve() == Path(sys._base_executable).resolve()
        and created(handle) >= created(int(process._handle)))
    if not valid or info.UniquePid != pid or (pid != process.pid and info.ParentPid != process.pid):
        api.CloseHandle(handle)
        return False
    process.m46_supervisor_handle = (api,handle,pid)
    return True


def terminate_owned(process):
    if process.m46_supervisor_handle is None and process.poll() is None:
        for event in journal_events(process.m46_manifest):
            if retain_supervisor(process,event['supervisor_pid']):
                break
    if process.m46_supervisor_handle is not None:
        api,handle,pid = process.m46_supervisor_handle
        api.WaitForSingleObject.argtypes,api.WaitForSingleObject.restype = [w.HANDLE,w.DWORD],w.DWORD
        api.TerminateProcess.argtypes,api.TerminateProcess.restype = [w.HANDLE,w.UINT],w.BOOL
        if api.WaitForSingleObject(handle,0) != 0:
            check(bool(api.TerminateProcess(handle,23)), 'terminate exact owned supervisor handle')
            check(api.WaitForSingleObject(handle,10000) == 0, 'owned supervisor died')
        api.CloseHandle(handle)
        process.m46_supervisor_handle = None
    if process.poll() is None:
        # Terminate the retained redirector only after its actual supervisor.
        process.terminate()
    process.wait(timeout=10)


def prepare(output):
    check(host.sha(PUBLIC) == PUBLIC_SHA and host.sha(PROFILE) == PROFILE_SHA, 'accepted input hashes')
    extension = host.read(EXTENSION)
    check(extension['content_digest'] == EXTENSION_DIGEST and retained.canonical_digest(
        {k:v for k,v in extension.items() if k != 'content_digest'}) == EXTENSION_DIGEST, 'accepted B digest')
    profile = host.read(PROFILE)
    check(host.sha(profile['measurement']['path']) == profile['measurement']['sha256'], 'accepted typed input')
    measurement = host.read(profile['measurement']['path'])['measurement']
    ref = host.read(retained.PROFILE_FILE)['evidence']['C1.3']['fixture_hashes.json']
    check(host.sha(ref['path']) == ref['sha256'], 'retained fixture manifest')
    originals = host.read(ref['path'])
    target = host.read(PUBLIC)
    expected_parent = (Path(target['paths']['evidence_root'])/'step11b-14c1930').resolve(strict=True)
    check(output.parent == expected_parent and not output.exists(), 'fresh named qualification child required')
    check(not any(Path(target['paths'][k]).exists() for k in ('operations','ledger','producer','evidence_store')),
          'canonical LIVE stores must remain absent')
    output.mkdir()
    manifests = {}
    for name in ('restart','stopped','exhausted'):
        root = output/name
        root.mkdir()
        copied = root/'retained-synthetic-c1'
        copied.mkdir()
        for relative, expected in originals.items():
            payload = (retained.C1/relative).read_bytes()
            check(hashlib.sha256(payload).hexdigest() == expected, 'original fixture byte binding')
            (copied/Path(relative).name).write_bytes(payload)
        paths = {key:str(copied/('fixture-'+file+'.sqlite')) for key,file in
            [('operations','operations'),('ledger','ledger'),('producer','producer'),
             ('evidence_store','evidence'),('market_source','raw')]}
        m = dict(schema='MEME_LIVE_WINDOWS_HOST_V1', scope='SYNTHETIC_RETAINED_QUALIFICATION',
            checkout=str(ROOT), python=sys.executable, host=socket.gethostname(), code_files=host.code_inventory(),
            interpreter_binding=host.interpreter_binding(),
            accepted_bindings=[dict(path=str(p),sha256=host.sha(p)) for p in (PUBLIC,PROFILE,EXTENSION)],
            qualification_root=str(root), journal_root=str(root/'host-journal'), run_seconds=120,
            health=dict(startup_us=20000000, progress_us=20000000, termination_us=5000000),
            startup=dict(paths=paths, domain=measurement['fixture_domain'],
                producer_profile=measurement['fixture_profile'], source_binding=measurement['fixture_source_binding'],
                source_profile=measurement['fixture_source_profile'], database_identity='A3:SYNTHETIC:RETAINED:COLLECTOR',
                market_anchor=1, expected_identity=measurement['current_startup_identity']))
        config = host.configuration(m)
        old = config.pop('expected_identity')
        expected = configured_identity(**config, producer_schema_digest=old.producer_schema_digest,
            evidence_schema_digest=old.evidence_schema_digest, restart_profile=old.restart_profile)
        m['startup']['expected_identity'] = asdict(expected)
        store = OperationsStore(config['operations_path'], config['domain'])
        control = store.snapshot()
        now = time.time_ns()//1000
        if name == 'stopped':
            store.operator_stop(now_us=now)
        if name == 'exhausted':
            # Arrange this isolated input solely through original Operations
            # acquire, without resetting/editing control SQL or its profile.
            for _ in range(control['max_attempts']+1):
                control = store.snapshot()
                try:
                    store.acquire('M46:ISOLATED:EXHAUSTION:ARRANGEMENT', now_us=now,
                        replace_generation=control['generation']).close()
                except OperationsOwnershipError as error:
                    check(str(error) == 'OPERATIONS_RESTART_EXHAUSTED', 'original exhaustion arrangement')
                    break
            check(store.snapshot()['exhausted'] == 1, 'exhausted fixture required')
        m['initial_control'] = store.snapshot()
        path = root/'host-manifest.json'
        host.write(path, m)
        manifests[name] = (path,m)
    canonical = json.loads(json.dumps(manifests['restart'][1]))
    canonical['scope'] = 'CANONICAL_LIVE_DENIAL_ONLY'
    canonical['startup']['paths'] = {k:target['paths'][k] for k in canonical['startup']['paths']}
    canonical['startup']['domain'] = profile['target']['domain']
    canonical['startup']['expected_identity'] = profile['canonical_startup_identity']
    host.write(output/'canonical-denial-manifest.json', canonical)
    host.write(output/'preparation.json', dict(manifests={k:dict(path=str(v[0]),sha256=host.sha(v[0]))
        for k,v in manifests.items()}, original_fixture_manifest=ref, limitations=[
        'Retained synthetic economics, fixed synthetic clock, no execution ports or current source-health claim.',
        'Canonical LIVE stores absent; canonical activation is not authorized by this entry.',
        'Task registration and physical reboot are separate owner actions; this script performs neither.',
        'Each host launch is bounded to 120 seconds; no sustained unattended trading qualification.']))
    return manifests, originals


def run(output, terminal_evidence=None):
    manifests, originals = prepare(output)
    checks = {}
    details = {}
    live = []
    try:
        for name in ('stopped','exhausted'):
            path,m = manifests[name]
            before, truth = read_control(m), economics(m)
            tested_m = m
            if terminal_evidence is None:
                for index in range(2):
                    process, stdout = launch(path, 'launch-'+str(index))
                    live.append(process)
                    check(process.wait(timeout=15) == 0, name+' host exit')
            else:
                check(terminal_evidence.parent == output.parent, 'terminal evidence scope')
                tested_m = host.read(terminal_evidence/name/'host-manifest.json')
                check(read_control(tested_m) == tested_m['initial_control'], 'retained terminal control')
                check(economics(tested_m) == truth, 'retained terminal economic truth')
            events = journal_events(tested_m)
            expected_state = 'OPERATOR_STOPPED' if name == 'stopped' else 'RESTART_EXHAUSTED'
            check(sum(e.get('state') == expected_state for e in events) == 2, name+' retained across process launches')
            check(not any(e.get('facts',{}).get('pid') for e in events), name+' child was not created')
            check(read_control(m) == before and economics(m) == truth, name+' durable truth unchanged')
            checks[name+'_two_actual_host_launches_preserve_control_economics_no_child'] = True
            details[name] = dict(control=before, ledger_economic_sha256=truth,
                qualified_host_manifest=str(path if terminal_evidence is None else terminal_evidence/name/'host-manifest.json'),
                terminal_process_evidence_reused=terminal_evidence is not None)
        path,m = manifests['restart']
        original_truth = economics(m)
        original_writers = writer_history(m)
        first, log1 = launch(path, 'first')
        live.append(first)
        first_event = wait_running(first,m)
        first_control, first_truth = read_control(m), economics(m)
        first_writers = writer_history(m)
        duplicate, dup_log = launch(path, 'duplicate')
        live.append(duplicate)
        check(duplicate.wait(timeout=15) == 2 and 'HOST_DUPLICATE_DENIED' in dup_log.read_text(), 'duplicate host denied')
        check(read_control(m) == first_control and economics(m) == first_truth, 'duplicate caused no durable change')
        check(writer_history(m) == first_writers, 'duplicate did not acquire Ledger writer')
        api, runtime_handle = retained_process_handle(first_event['facts']['pid'])
        try:
            terminate_owned(first)
            check(api.WaitForSingleObject(runtime_handle, 10000) == 0, 'job killed old Runtime after actual supervisor death')
        finally:
            api.CloseHandle(runtime_handle)
        second, log2 = launch(path, 'replacement')
        live.append(second)
        second_event = wait_running(second,m)
        second_control, second_truth = read_control(m), economics(m)
        second_writers = writer_history(m)
        check(first_event['supervisor_pid'] != second_event['supervisor_pid']
            and first_event['facts']['pid'] != second_event['facts']['pid'], 'actual new supervisor and Runtime processes')
        check(second_control['generation'] == first_control['generation']+1
            and second_control['nonce'] != first_control['nonce'], 'fresh original owner fence')
        check(second_control['budget_id'] == first_control['budget_id']
            and not second_control['stopped'] and not second_control['exhausted'], 'durable restart budget identity retained')
        check(second_truth == first_truth == original_truth, 'all durable ledger economics preserved')
        check(first_writers[:-1] == original_writers and second_writers[:-1] == first_writers,
              'exact original writer fencing history append per startup')
        api, runtime_handle = retained_process_handle(second_event['facts']['pid'])
        try:
            terminate_owned(second)
            check(api.WaitForSingleObject(runtime_handle,10000) == 0, 'replacement Runtime containment')
        finally:
            api.CloseHandle(runtime_handle)
        checks.update(actual_supervisor_replacement=True, duplicate_launch_denied=True,
            old_supervisor_and_runtime_dead=True, original_startup_new_fence=True,
            restart_budget_identity_retained=True, ledger_economics_unchanged=True,
            original_writer_fencing_history_append_only=True)
        details['replacement'] = dict(first=first_event, replacement=second_event,
            first_control=first_control, second_control=second_control, ledger_economic_sha256=second_truth,
            writer_generations=[len(original_writers),len(first_writers),len(second_writers)],
            restart_accounting_scope='Original wall-clock window/backoff semantics; no host reset API.')
        for label, manifest in [('canonical',output/'canonical-denial-manifest.json')]:
            process, stdout = launch(manifest,label)
            live.append(process)
            check(process.wait(timeout=15) == 2 and 'HOST_CANONICAL_EXISTING_STORES_REQUIRED' in stdout.read_text(),
                  'canonical missing stores fail closed before acquisition')
        checks['canonical_missing_stores_denied_without_initialization'] = True
        # Targeted binding failures use separate manifests, never changing code
        # under a running host or changing the retained approved manifests.
        for label, edit, reason in [
            ('code', lambda x:x['code_files'].update({next(iter(x['code_files'])):'0'*64}), 'HOST_CODE_BINDING_CONFLICT'),
            ('identity', lambda x:x['startup']['expected_identity'].update(configuration_digest='0'*64), 'HOST_REVIEWED_STARTUP_IDENTITY_CONFLICT'),
            ('missing', lambda x:x['startup']['paths'].update(ledger=str(output/'restart/absent.sqlite')), 'HOST_EXISTING_QUALIFICATION_STORES_REQUIRED')]:
            altered = json.loads(json.dumps(m))
            edit(altered)
            altered_path = output/(label+'-denial.json')
            host.write(altered_path,altered)
            process,stdout = launch(altered_path,label)
            live.append(process)
            check(process.wait(timeout=15) == 2 and reason in stdout.read_text(), label+' frozen binding denial')
            checks[label+'_binding_denied'] = True
        check(read_control(m) == second_control and economics(m) == second_truth, 'binding denials did not mutate durable stores')
        check(all(host.sha(retained.C1/relative) == expected for relative,expected in originals.items()), 'original fixtures unchanged')
        target = host.read(PUBLIC)
        check(not any(Path(target['paths'][k]).exists() for k in ('operations','ledger','producer','evidence_store')), 'canonical stores remain absent')
        result = dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW', scope='M46_WINDOWS_SUPERVISOR_PROCESS_DELTA',
            checks=checks, details=details, host=socket.gethostname(), code_files=host.code_inventory(),
            manifests={k:dict(path=str(p),sha256=host.sha(p)) for k,(p,_) in manifests.items()},
            physical_reboot=False, task_registered=False, original_fixture_bytes_unchanged=True,
            canonical_live_stores_created=False, signing=False, sending=False, network=False,
            reboot_prerequisite='Review and install exact hash-pinned BootTrigger task under reviewed identity; actual noninteractive smoke and reboot still required.')
        host.write(output/'result.json',result)
        print(json.dumps(dict(result=str(output/'result.json'),sha256=host.sha(output/'result.json'),checks=checks),sort_keys=True))
    finally:
        for process in live:
            terminate_owned(process)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--terminal-evidence',type=Path)
    args = parser.parse_args()
    run(args.output.resolve(), None if args.terminal_evidence is None else args.terminal_evidence.resolve())
