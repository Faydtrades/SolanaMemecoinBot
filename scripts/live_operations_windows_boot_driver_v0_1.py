"""Fixed M46 native-task receipt driver; retained synthetic qualification only.

The accepted host entry is the only Runtime launch path. This driver captures
its finite outputs; it does not acquire, initialize, reset, adopt or execute.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import time
from uuid import uuid4
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
PRIOR_PACKAGE = Path(r'D:\Tradingbot\meme_live\evidence\step11b-14c1930\boot-preparation-01')
PACKAGE = PRIOR_PACKAGE/'retry-effective-defaults-01'
PINS = PACKAGE/'pins.json'
TASK = 'MEME-LIVE-M46-BOOT-QUALIFICATION'
POWERSHELL = r'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe'
SCHTASKS = r'C:\Windows\System32\schtasks.exe'
NS = {'t':'http://schemas.microsoft.com/windows/2004/02/mit/task'}


def check(value, reason):
    if not value:
        raise RuntimeError(reason)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def utc():
    return datetime.now(timezone.utc).isoformat()


def write(path, value):
    path = Path(path)
    temporary = path.with_name(path.name+'.'+uuid4().hex+'.tmp')
    with temporary.open('x',encoding='utf-8') as stream:
        stream.write(json.dumps(value,sort_keys=True,indent=2)+'\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary,path)


def native_facts():
    command = "$ErrorActionPreference='Stop'; $i=[System.Security.Principal.WindowsIdentity]::GetCurrent(); $t=Get-ScheduledTaskInfo -TaskName '"+TASK+"'; [pscustomobject]@{boot_utc=(Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime().ToString('o');sid=$i.User.Value;user=$i.Name;task_last_run_utc=$t.LastRunTime.ToUniversalTime().ToString('o');session_id=(Get-Process -Id $PID).SessionId}|ConvertTo-Json -Compress"
    result = subprocess.run([POWERSHELL,'-NoProfile','-NonInteractive','-Command',command],
        capture_output=True,text=True,timeout=30,creationflags=subprocess.CREATE_NO_WINDOW)
    check(result.returncode == 0,'BOOT_NATIVE_FACTS_UNAVAILABLE')
    facts = json.loads(result.stdout)
    result = subprocess.run([SCHTASKS,'/Query','/TN','\\'+TASK,'/XML'],capture_output=True,
        text=True,timeout=30,creationflags=subprocess.CREATE_NO_WINDOW)
    check(result.returncode == 0,'BOOT_NATIVE_TASK_UNAVAILABLE')
    return facts,result.stdout


def task_check(xml, pins, *, expected_state='Active', pins_sha=None):
    check(expected_state in ('Active','DisabledRetry'),'BOOT_EXPECTED_TASK_STATE_REQUIRED')
    tree = ET.fromstring(xml)
    for name in ('Triggers','Actions','Principals','Settings'):
        check(len(tree.findall('t:'+name,NS)) == 1,'BOOT_EXACT_CONTAINER_REQUIRED')
    check(len(tree.findall('t:Triggers/*',NS)) == 1
        and tree.find('t:Triggers/t:BootTrigger',NS) is not None,'BOOT_EXACT_TRIGGER_REQUIRED')
    check(len(tree.findall('t:Actions/*',NS)) == 1
        and len(tree.findall('t:Principals/*',NS)) == 1,'BOOT_EXACT_ACTION_PRINCIPAL_REQUIRED')
    expected = {
        't:Triggers/t:BootTrigger/t:Enabled':'true',
        't:Triggers/t:BootTrigger/t:Delay':'PT30S',
        't:Principals/t:Principal/t:UserId':pins['sid'],
        't:Principals/t:Principal/t:LogonType':'S4U',
        't:Principals/t:Principal/t:RunLevel':'LeastPrivilege',
        't:Settings/t:MultipleInstancesPolicy':'IgnoreNew',
        't:Settings/t:ExecutionTimeLimit':'PT10M',
        't:Settings/t:Enabled':'true' if expected_state == 'Active' else 'false',
        't:Settings/t:StartWhenAvailable':'true',
        't:Settings/t:AllowStartOnDemand':'true',
        't:Actions/t:Exec/t:Command':pins['python'],
        't:Actions/t:Exec/t:Arguments':pins['task_arguments_template'].replace('{PINS_SHA256}',pins_sha or sha(PINS)),
        't:Actions/t:Exec/t:WorkingDirectory':pins['checkout']}
    # Task Scheduler schema defaults, not arbitrary missing-node tolerance.
    defaults = {'t:Triggers/t:BootTrigger/t:Enabled':'true',
        't:Principals/t:Principal/t:RunLevel':'LeastPrivilege',
        't:Settings/t:Enabled':'true','t:Settings/t:AllowStartOnDemand':'true'}
    for path,value in expected.items():
        elements = tree.findall(path,NS)
        check(len(elements) <= 1,'BOOT_DUPLICATE_CRITICAL_NODE')
        effective = elements[0].text if elements else defaults.get(path)
        check(effective == value,'BOOT_NATIVE_TASK_BINDING_CONFLICT')
    check(tree.find('t:Settings/t:RestartOnFailure',NS) is None,'BOOT_NO_SCHEDULER_RETRY_REQUIRED')


def validate(pins_path, expected_sha):
    check(Path(pins_path).resolve() == PINS.resolve() and sha(pins_path) == expected_sha,'BOOT_PIN_MANIFEST_CONFLICT')
    pins = read(pins_path)
    check(pins['schema'] == 'MEME_LIVE_M46_BOOT_PINS_V1' and pins['task_name'] == TASK,'BOOT_PIN_SCHEMA_CONFLICT')
    check(pins['checkout'] == str(ROOT) and pins['host'] == socket.gethostname(),'BOOT_DEPLOYMENT_CONFLICT')
    check(Path(pins['python']).resolve() == Path(sys.executable).resolve(),'BOOT_PYTHON_PATH_CONFLICT')
    for item in pins['frozen_files']:
        check(sha(item['path']) == item['sha256'],'BOOT_FROZEN_FILE_CONFLICT')
    check([c['name'] for c in pins['cases']] == ['restart','stopped','exhausted','canonical'], 'BOOT_FIXED_CASES_REQUIRED')
    for case in pins['cases']:
        check(sha(case['manifest']) == case['sha256'],'BOOT_CASE_MANIFEST_CONFLICT')
        manifest = read(case['manifest'])
        if case['name'] != 'canonical':
            require_existing_stores(manifest)
            if case['name'] == 'restart':
                owner = Path(manifest['journal_root'])/'owner.json'
                check(owner.is_file() and read(owner)['manifest_sha256'] == case['sha256'],'BOOT_RETAINED_HOST_PROVENANCE_REQUIRED')
        for relative,expected in manifest['code_files'].items():
            check(sha(ROOT/relative) == expected,'BOOT_ACCEPTED_SOURCE_CONFLICT')
        for item in manifest['accepted_bindings']:
            check(sha(item['path']) == item['sha256'],'BOOT_ACCEPTED_INPUT_CONFLICT')
    target = read(pins['public_configuration'])
    check(not any(Path(target['paths'][k]).exists() for k in ('operations','ledger','producer','evidence_store')),
          'BOOT_CANONICAL_STORES_MUST_REMAIN_ABSENT')
    return pins


def retry_lineage(lineage_path, expected_sha):
    """Historical proof stays hash-bound; old driver pins are not current inputs."""
    check(sha(lineage_path) == expected_sha,'BOOT_RETRY_LINEAGE_HASH_CONFLICT')
    lineage = read(lineage_path)
    check(lineage['schema'] == 'MEME_LIVE_M46_DISABLED_RETRY_V1'
        and lineage['prior_package'] == str(PRIOR_PACKAGE)
        and lineage['retry_package'] == str(PACKAGE),'BOOT_RETRY_SCOPE_CONFLICT')
    for name, expected in lineage['prior_files'].items():
        check(Path(name).name == name and sha(PRIOR_PACKAGE/name) == expected,'BOOT_RETRY_PRIOR_FILE_CONFLICT')
    required = {'pins.json','task.xml','install-smoke-and-reboot.ps1','preparation-result.json',
        'install-request.json','owner-failure-3a0df6528d8d42cb94c2bf7947d04805.json','registered-task-actual.xml'}
    check(set(lineage['prior_files']) == required,'BOOT_RETRY_EXACT_PRIOR_FILES_REQUIRED')
    old = read(PRIOR_PACKAGE/'pins.json')
    request = read(PRIOR_PACKAGE/'install-request.json')
    failure = read(PRIOR_PACKAGE/'owner-failure-3a0df6528d8d42cb94c2bf7947d04805.json')
    preparation = read(PRIOR_PACKAGE/'preparation-result.json')
    check(request['request_id'] == lineage['prior_request_id'] == 'b03b73ffa3914beaa0626a4bea277dc4'
        and request['pins_sha256'] == sha(PRIOR_PACKAGE/'pins.json')
        and request['task_xml_sha256'] == sha(PRIOR_PACKAGE/'task.xml')
        and request['sid'] == old['sid'],'BOOT_RETRY_REQUEST_CONFLICT')
    check(failure['status'] == 'FAILED' and failure['created_this_invocation'] is True
        and failure['reboot_attempted'] is False and failure['exact_created_task_disabled'] is False
        and failure['reason'] == 'OWNER_REGISTERED_TASK_BINDING_CONFLICT_TASK_DISABLE_UNCONFIRMED'
        and datetime.fromisoformat(failure['utc']) >= datetime.fromisoformat(request['requested_utc']),
        'BOOT_RETRY_FAILED_INSTALL_REQUIRED')
    for name in ('pins.json','task.xml','install-smoke-and-reboot.ps1'):
        check(preparation['files'][str(PRIOR_PACKAGE/name)] == sha(PRIOR_PACKAGE/name),'BOOT_RETRY_PREPARATION_CONFLICT')
    check(old['task_name'] == TASK and old['checkout'] == str(ROOT),'BOOT_RETRY_OLD_SCOPE_CONFLICT')
    task_check((PRIOR_PACKAGE/'task.xml').read_text(encoding='utf-8-sig'),old,pins_sha=request['pins_sha256'])
    task_check((PRIOR_PACKAGE/'registered-task-actual.xml').read_text(encoding='utf-16'),old,
        pins_sha=request['pins_sha256'],expected_state='DisabledRetry')
    check(not any((PRIOR_PACKAGE/n).exists() for n in
        ('smoke-result.json','verified-smoke.json','pre-reboot-baseline.json','postboot-result.json')),
        'BOOT_RETRY_NO_PRIOR_SMOKE_REQUIRED')
    historical = next(i['sha256'] for i in old['frozen_files'] if i['path'] == str(Path(__file__).resolve()))
    check(lineage['historical_driver_sha256'] == historical
        and preparation['files'][str(Path(__file__).resolve())] == historical
        and lineage['current_driver_sha256'] == sha(__file__),'BOOT_RETRY_DRIVER_HISTORY_CONFLICT')
    return old


def require_existing_stores(manifest):
    check(all(Path(p).is_file() for p in manifest['startup']['paths'].values()),
          'BOOT_EXISTING_RETAINED_STORES_REQUIRED')


def database(path):
    return sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=5)


def snapshot(manifest):
    paths = manifest['startup']['paths']
    with closing(database(paths['operations'])) as db:
        db.row_factory = sqlite3.Row
        control = dict(db.execute('SELECT * FROM operations').fetchone())
    with closing(database(paths['ledger'])) as db:
        check(db.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
            and not db.execute('PRAGMA foreign_key_check').fetchall(),'BOOT_LEDGER_INTEGRITY_FAILURE')
        tables = {}
        for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
            if name == 'ledger_writer_generations':
                continue
            tables[name] = (db.execute('SELECT singleton,revision,last_receipt_digest FROM ledger_head').fetchall()
                if name == 'ledger_head' else db.execute('SELECT * FROM "'+name+'" ORDER BY rowid').fetchall())
        writers = [list(row) for row in db.execute('SELECT * FROM ledger_writer_generations ORDER BY generation')]
    return dict(control=control,economic_digest=digest(tables),writer_rows=writers)


def journal_inventory(manifest):
    return {str(p):sha(p) for p in Path(manifest['journal_root']).glob('*.jsonl')}


def capture(arguments, output, timeout):
    with Path(output).open('xb') as stream:
        process = subprocess.Popen(arguments,cwd=ROOT,stdout=stream,stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            code = process.wait(timeout=timeout)
        finally:
            stream.flush()
            os.fsync(stream.fileno())
    # On timeout do not adopt/kill an unproven process. Accepted host has its own
    # 120-second bound and job containment; fail the task and prohibit reboot.
    return dict(exit_code=code,launcher_pid=process.pid,stdout=str(output),stdout_sha256=sha(output))


def qualify_case(case,pins,run_root,expected_before):
    m = read(case['manifest'])
    before = None if case['name'] == 'canonical' else snapshot(m)
    if before is not None:
        check(before == expected_before,'BOOT_PRESERVED_STATE_BASELINE_CONFLICT')
    old_journals = {} if before is None else journal_inventory(m)
    receipt = capture([pins['python'],'-B',pins['host_entry'],'--manifest',case['manifest'],
        '--sha256',case['sha256']],run_root/(case['name']+'.stdout.txt'),180)
    if before is None:
        check(receipt['exit_code'] == 2 and 'HOST_CANONICAL_EXISTING_STORES_REQUIRED' in
            Path(receipt['stdout']).read_text(),'BOOT_CANONICAL_DENIAL_REQUIRED')
        return dict(**receipt,case='canonical',missing_canonical_stores_denied=True)
    after = snapshot(m)
    fresh = {p:h for p,h in journal_inventory(m).items() if p not in old_journals}
    check(receipt['exit_code'] == 0 and len(fresh) == 1,'BOOT_NEW_SUCCESSFUL_HOST_RECEIPT_REQUIRED')
    events = [json.loads(line) for p in fresh for line in Path(p).read_text().splitlines()]
    check(all(e['manifest_sha256'] == case['sha256'] and e['host'] == pins['host'] for e in events),
          'BOOT_HOST_RECEIPT_BINDING_CONFLICT')
    check(before['economic_digest'] == after['economic_digest'],'BOOT_ECONOMIC_STATE_CHANGED')
    if case['name'] in ('stopped','exhausted'):
        state = 'OPERATOR_STOPPED' if case['name'] == 'stopped' else 'RESTART_EXHAUSTED'
        check(after == before and any(e.get('state') == state for e in events)
            and not any(e.get('facts',{}).get('pid') for e in events),'BOOT_TERMINAL_STATE_NOT_RETAINED')
    else:
        old,new = before['control'],after['control']
        check(new['generation'] == old['generation']+1 and new['nonce'] != old['nonce']
            and not new['stopped'] and not new['exhausted'],'BOOT_ORIGINAL_FENCE_REQUIRED')
        check(all(new[k] == old[k] for k in ('budget_id','max_attempts','window_us','backoff_us','domain_id','binding_digest')),
              'BOOT_ORIGINAL_BUDGET_BINDING_REQUIRED')
        check(after['writer_rows'][:-1] == before['writer_rows'],'BOOT_ORIGINAL_WRITER_HISTORY_REQUIRED')
        running = [e for e in events if e.get('facts',{}).get('state') == 'RUNNING']
        check(running and len({e['facts']['pid'] for e in running}) == 1
            and any(e.get('event') == 'HOST_FINITE_END' for e in events),'BOOT_ONE_FINITE_OWNED_STARTUP_REQUIRED')
        owner = read(Path(m['journal_root'])/'owner.json')
        check(owner['manifest_sha256'] == case['sha256'] and owner['control'] == new,'BOOT_DURABLE_HOST_PROVENANCE_REQUIRED')
    return dict(**receipt,case=case['name'],before=before,after=after,new_journals=fresh)


def run(pins,pin_sha):
    request = read(PACKAGE/'install-request.json')
    facts,xml = native_facts()
    check(facts['sid'] == pins['sid'],'BOOT_ACCOUNT_CONFLICT')
    task_check(xml,pins)
    started = utc()
    check(datetime.fromisoformat(facts['task_last_run_utc']) >= datetime.fromisoformat(request['requested_utc']),
          'BOOT_FRESH_NATIVE_TASK_RUN_REQUIRED')
    ready_path = PACKAGE/'pre-reboot-baseline.json'
    if facts['boot_utc'] == request['boot_utc']:
        check(not (PACKAGE/'smoke-result.json').exists(),'BOOT_SMOKE_ALREADY_RECORDED')
        phase = 'NATIVE_S4U_SMOKE'
        baselines = {c['name']:c.get('initial_state') for c in pins['cases']}
    else:
        check(ready_path.is_file() and not (PACKAGE/'postboot-result.json').exists(),'BOOT_REVIEWED_BASELINE_REQUIRED')
        ready = read(ready_path)
        check(datetime.fromisoformat(facts['boot_utc']) > datetime.fromisoformat(request['boot_utc']),
              'BOOT_NEW_PHYSICAL_BOOT_ID_REQUIRED')
        check(ready['status'] == 'REBOOT_READY' and ready['pins_sha256'] == pin_sha
            and ready['boot_utc'] == request['boot_utc'],'BOOT_PRE_REBOOT_BINDING_CONFLICT')
        check(ready['native_task_xml_digest'] == hashlib.sha256(xml.encode()).hexdigest(),'BOOT_NATIVE_TASK_CHANGED')
        phase = 'ACTUAL_NEW_BOOT'
        baselines = ready['preserved_states']
    run_root = PACKAGE/'runs'/uuid4().hex
    run_root.mkdir(parents=True)
    result = dict(status='IN_PROGRESS',phase=phase,started_utc=started,boot=facts,
        request_id=request['request_id'],pins_sha256=pin_sha,native_task_xml_digest=hashlib.sha256(xml.encode()).hexdigest(),
        run_root=str(run_root),cases=[],grants_permission=False,canonical_positive_startup=False)
    write(run_root/'result.json',result)
    try:
        for case in pins['cases']:
            result['cases'].append(qualify_case(case,pins,run_root,baselines.get(case['name'])))
            write(run_root/'result.json',result)
        result.update(status='IMPLEMENTED_PENDING_PROJECT_REVIEW',finished_utc=utc())
        write(run_root/'result.json',result)
        target = 'smoke-result.json' if phase == 'NATIVE_S4U_SMOKE' else 'postboot-result.json'
        write(PACKAGE/target,dict(result_path=str(run_root/'result.json'),sha256=sha(run_root/'result.json')))
        return 0
    except Exception as error:
        code = str(error)
        if not code.startswith('BOOT_') or not code.replace('_','').isalnum():
            code = 'BOOT_DRIVER_FAILED_'+type(error).__name__
        result.update(status='FAILED',reason=code,finished_utc=utc())
        write(run_root/'result.json',result)
        raise


def verify_smoke(pins,pin_sha):
    request = read(PACKAGE/'install-request.json')
    pointer = read(PACKAGE/'smoke-result.json')
    check(sha(pointer['result_path']) == pointer['sha256'],'BOOT_SMOKE_RECEIPT_HASH_CONFLICT')
    result = read(pointer['result_path'])
    facts,xml = native_facts()
    task_check(xml,pins)
    check(result['status'] == 'IMPLEMENTED_PENDING_PROJECT_REVIEW'
        and result['phase'] == 'NATIVE_S4U_SMOKE' and result['request_id'] == request['request_id']
        and result['pins_sha256'] == pin_sha and result['boot']['boot_utc'] == facts['boot_utc'] == request['boot_utc']
        and result['boot']['task_last_run_utc'] == facts['task_last_run_utc']
        and datetime.fromisoformat(result['started_utc']) >= datetime.fromisoformat(request['requested_utc']),
        'BOOT_FRESH_SMOKE_BINDING_REQUIRED')
    check(len(result['cases']) == 4,'BOOT_ALL_CASE_RECEIPTS_REQUIRED')
    states = {}
    for case,receipt in zip(pins['cases'],result['cases']):
        check(case['name'] == receipt['case'],'BOOT_CASE_RECEIPT_ORDER_CONFLICT')
        if case['name'] != 'canonical':
            check(snapshot(read(case['manifest'])) == receipt['after'],'BOOT_SMOKE_STATE_DRIFT')
            states[case['name']] = receipt['after']
    verified = dict(status='NATIVE_SMOKE_VERIFIED_PENDING_REBOOT',verified_utc=utc(),
        pins_sha256=pin_sha,request_id=request['request_id'],boot_utc=facts['boot_utc'],
        native_task_xml_digest=hashlib.sha256(xml.encode()).hexdigest(),
        native_task_last_run_utc=facts['task_last_run_utc'],preserved_states=states,smoke_receipt=pointer)
    write(PACKAGE/'verified-smoke.json',verified)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pins',type=Path,required=True)
    parser.add_argument('--sha256',required=True)
    parser.add_argument('--validate-only',action='store_true')
    parser.add_argument('--verify-smoke',action='store_true')
    parser.add_argument('--validate-retry-lineage')
    args = parser.parse_args()
    try:
        pins = validate(args.pins,args.sha256)
        if args.validate_retry_lineage:
            retry_lineage(PACKAGE/'retry-lineage.json',args.validate_retry_lineage)
            print('BOOT_DISABLED_RETRY_LINEAGE_VALIDATED_NO_RUNTIME_LAUNCH')
            return 0
        if args.validate_only:
            print('BOOT_FIXED_INPUTS_VALIDATED_NO_RUNTIME_LAUNCH')
            return 0
        if args.verify_smoke:
            verify_smoke(pins,args.sha256)
            print('BOOT_FRESH_NATIVE_SMOKE_VERIFIED_NO_RUNTIME_LAUNCH')
            return 0
        return run(pins,args.sha256)
    except Exception as error:
        code = str(error)
        if not code.startswith('BOOT_') or not code.replace('_','').isalnum():
            code = 'BOOT_DRIVER_FAILED_'+type(error).__name__
        # Fixed root captures even pre-journal manifest/native-fact denial.
        if PACKAGE.is_dir():
            write(PACKAGE/('failure-'+uuid4().hex+'.json'),dict(status='FAILED',reason=code,utc=utc()))
        print(code,flush=True)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
