"""Offline M46 Task Scheduler artifacts. Never registers, launches Runtime or reboots."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

import live_operations_windows_boot_driver_v0_1 as d

CASE_ROOT = d.PACKAGE.parent/'m46-host-process-03'
RESULT_SHA = 'ecc1fd82b1b8d9d18ed15443a2f69ed9690cbe087428df76758247eac574d34c'
HOST_FILES = {
    'src/live/operations_windows_host_v0_1.py':'6eb7396cebe12a08eb67ca50d1640c86e955c331359852e563a38a73c3608f7d',
    'scripts/live_operations_windows_host_v0_1.py':'889b0e983859b6a8acfd9ca83240b92030d3630810fc9b8714d7ad3238779471',
    'scripts/live_operations_windows_host_qualification_v0_1.py':'0d46d59b817453d7400500f04d865bbc0cec6cedd55204940f8da81c5ef049c4'}
SID = 'S-1-5-21-3858496780-2977153031-2895069985-1001'
USER = r'DESKTOP-SVU1PTT\Mari1'


OWNER_SCRIPT = r'''param([switch]$InstallAndReboot)
$ErrorActionPreference = 'Stop'
$Package = '__PACKAGE__'
$PinsPath = Join-Path $Package 'pins.json'
$PinsSha = '__PINS_SHA__'
$Definition = Join-Path $Package 'task.xml'
$DefinitionSha = '__XML_SHA__'
$TaskName = 'MEME-LIVE-M46-BOOT-QUALIFICATION'
$ExpectedSid = 'S-1-5-21-3858496780-2977153031-2895069985-1001'
$Python = '__PYTHON__'
$Driver = '__DRIVER__'
$created = $false
$rebootAttempted = $false
$folder = $null

function Require($Condition, [string]$Code) {
    if (-not $Condition) { throw $Code }
}
function Sha([string]$Path) { (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }
function SaveJson([string]$Path, $Value) {
    $text = $Value | ConvertTo-Json -Depth 100
    $temporary = $Path + '.' + [guid]::NewGuid().ToString('N') + '.tmp'
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($text + "`n")
    $stream = [IO.FileStream]::new($temporary,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
    try { $stream.Write($bytes,0,$bytes.Length); $stream.Flush($true) } finally { $stream.Dispose() }
    Move-Item -LiteralPath $temporary -Destination $Path -Force
}
function VerifyTask($Registered, $Pins) {
    [xml]$actual = $Registered.Xml
    $ns = [Xml.XmlNamespaceManager]::new($actual.NameTable)
    $ns.AddNamespace('t','http://schemas.microsoft.com/windows/2004/02/mit/task')
    Require ($actual.SelectNodes('/t:Task/t:Triggers/*',$ns).Count -eq 1) 'OWNER_EXACT_BOOT_TRIGGER_REQUIRED'
    Require ($null -ne $actual.SelectSingleNode('/t:Task/t:Triggers/t:BootTrigger',$ns)) 'OWNER_BOOT_TRIGGER_REQUIRED'
    Require ($actual.SelectNodes('/t:Task/t:Actions/*',$ns).Count -eq 1) 'OWNER_EXACT_ACTION_REQUIRED'
    Require ($actual.SelectNodes('/t:Task/t:Principals/*',$ns).Count -eq 1) 'OWNER_EXACT_PRINCIPAL_REQUIRED'
    $expect = @{
        '/t:Task/t:Triggers/t:BootTrigger/t:Enabled' = 'true'
        '/t:Task/t:Triggers/t:BootTrigger/t:Delay' = 'PT30S'
        '/t:Task/t:Principals/t:Principal/t:UserId' = $ExpectedSid
        '/t:Task/t:Principals/t:Principal/t:LogonType' = 'S4U'
        '/t:Task/t:Principals/t:Principal/t:RunLevel' = 'LeastPrivilege'
        '/t:Task/t:Settings/t:MultipleInstancesPolicy' = 'IgnoreNew'
        '/t:Task/t:Settings/t:ExecutionTimeLimit' = 'PT10M'
        '/t:Task/t:Settings/t:Enabled' = 'true'
        '/t:Task/t:Settings/t:StartWhenAvailable' = 'true'
        '/t:Task/t:Settings/t:AllowStartOnDemand' = 'true'
        '/t:Task/t:Actions/t:Exec/t:Command' = $Python
        '/t:Task/t:Actions/t:Exec/t:Arguments' = $Pins.task_arguments_template.Replace('{PINS_SHA256}',$PinsSha)
        '/t:Task/t:Actions/t:Exec/t:WorkingDirectory' = '__CHECKOUT__'
    }
    foreach ($key in $expect.Keys) {
        $node = $actual.SelectSingleNode($key,$ns)
        Require ($null -ne $node -and $node.InnerText -ceq $expect[$key]) 'OWNER_REGISTERED_TASK_BINDING_CONFLICT'
    }
    Require ($null -eq $actual.SelectSingleNode('/t:Task/t:Settings/t:RestartOnFailure',$ns)) 'OWNER_NO_SCHEDULER_RETRY_REQUIRED'
}

try {
    Require ((Sha $PinsPath) -eq $PinsSha -and (Sha $Definition) -eq $DefinitionSha) 'OWNER_PACKAGE_HASH_CONFLICT'
    $pins = Get-Content -LiteralPath $PinsPath -Raw | ConvertFrom-Json
    foreach ($item in $pins.frozen_files) { Require ((Sha $item.path) -eq $item.sha256) 'OWNER_FROZEN_FILE_CONFLICT' }
    $validationOutput = & $Python -B $Driver --pins $PinsPath --sha256 $PinsSha --validate-only 2>&1
    Require ($LASTEXITCODE -eq 0) 'OWNER_FIXED_INPUT_VALIDATION_FAILED'
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    Require ($identity.User.Value -eq $ExpectedSid) 'OWNER_SAME_ACCOUNT_REQUIRED'
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    $elevated = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    if (-not $InstallAndReboot) {
        [pscustomobject]@{status='PREPARED_NOT_REGISTERED';elevated=$elevated;pins_sha256=$PinsSha;task_xml_sha256=$DefinitionSha;validation=$validationOutput}|ConvertTo-Json -Depth 4
        exit 0
    }
    Require $elevated 'OWNER_ELEVATED_SAME_ACCOUNT_REQUIRED'
    Require (-not (Test-Path -LiteralPath (Join-Path $Package 'install-request.json'))) 'OWNER_FRESH_INSTALL_REQUEST_REQUIRED'
    Require (-not (Test-Path -LiteralPath (Join-Path $Package 'smoke-result.json'))) 'OWNER_NO_STALE_SMOKE_RECEIPT_ALLOWED'
    $service = New-Object -ComObject 'Schedule.Service'
    $service.Connect()
    $folder = $service.GetFolder('\')
    $existing = $null
    try { $existing = $folder.GetTask($TaskName) } catch {
        Require ($_.Exception.GetBaseException().HResult -eq -2147024894) 'OWNER_TASK_LOOKUP_FAILED'
    }
    Require ($null -eq $existing) 'OWNER_EXISTING_TASK_REFUSED'
    $boot = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime().ToString('o')
    $request = [ordered]@{request_id=[guid]::NewGuid().ToString('N');requested_utc=[DateTime]::UtcNow.ToString('o');boot_utc=$boot;pins_sha256=$PinsSha;task_xml_sha256=$DefinitionSha;sid=$identity.User.Value}
    SaveJson (Join-Path $Package 'install-request.json') $request
    $xmlText = Get-Content -LiteralPath $Definition -Raw
    # TASK_CREATE=2, TASK_LOGON_S4U=2; no password, no update/overwrite flags.
    $registered = $folder.RegisterTask($TaskName,$xmlText,2,$ExpectedSid,$null,2,$null)
    $created = $true
    VerifyTask $registered $pins
    $native = Get-ScheduledTask -TaskName $TaskName
    Require ($native.Principal.LogonType -eq 'S4U' -and $native.Principal.RunLevel -eq 'Limited') 'OWNER_NATIVE_PRINCIPAL_CONFLICT'
    # Native TaskLastRun may have one-second precision. Start strictly after
    # the durable request second so the fresh-receipt comparison is exact.
    Start-Sleep -Milliseconds 1200
    Start-ScheduledTask -TaskName $TaskName
    $deadline = [DateTime]::UtcNow.AddMinutes(7)
    $pointer = Join-Path $Package 'smoke-result.json'
    do {
        Start-Sleep -Seconds 2
        $info = Get-ScheduledTaskInfo -TaskName $TaskName
        $native = Get-ScheduledTask -TaskName $TaskName
        $fresh = $info.LastRunTime.ToUniversalTime() -ge [DateTime]::Parse($request.requested_utc).ToUniversalTime()
        if ($fresh -and $native.State -ne 'Running' -and $native.State -ne 'Queued') { break }
    } while ([DateTime]::UtcNow -lt $deadline)
    Require ($fresh -and $native.State -ne 'Running' -and $native.State -ne 'Queued') 'OWNER_NATIVE_SMOKE_DEADLINE'
    Require ($info.LastTaskResult -eq 0 -and (Test-Path -LiteralPath $pointer)) 'OWNER_NATIVE_SMOKE_FAILED'
    VerifyTask ($folder.GetTask($TaskName)) $pins
    $verifyOutput = & $Python -B $Driver --pins $PinsPath --sha256 $PinsSha --verify-smoke 2>&1
    Require ($LASTEXITCODE -eq 0) 'OWNER_NATIVE_SMOKE_RECEIPT_INVALID'
    $verified = Get-Content -LiteralPath (Join-Path $Package 'verified-smoke.json') -Raw | ConvertFrom-Json
    Require ($verified.request_id -ceq $request.request_id -and $verified.pins_sha256 -ceq $PinsSha) 'OWNER_STALE_SMOKE_RECEIPT'
    Require ([DateTime]::Parse($verified.native_task_last_run_utc).ToUniversalTime() -eq $info.LastRunTime.ToUniversalTime()) 'OWNER_NATIVE_LAST_RUN_CONFLICT'
    $baseline = [ordered]@{status='REBOOT_READY';recorded_utc=[DateTime]::UtcNow.ToString('o');boot_utc=$boot;pins_sha256=$PinsSha;task_xml_sha256=$DefinitionSha;request_id=$request.request_id;native_task_xml_digest=$verified.native_task_xml_digest;native_task_last_run_utc=$verified.native_task_last_run_utc;preserved_states=$verified.preserved_states;smoke_receipt=$verified.smoke_receipt;canonical_positive_startup=$false}
    SaveJson (Join-Path $Package 'pre-reboot-baseline.json') $baseline
    SaveJson (Join-Path $Package 'owner-action.json') ([ordered]@{status='REBOOT_READY';request_id=$request.request_id;utc=[DateTime]::UtcNow.ToString('o');validation=$validationOutput;smoke_verification=$verifyOutput;last_task_result=$info.LastTaskResult;graceful_reboot_requested=$true})
    # Explicit owner invocation above authorizes graceful reboot; never force.
    $rebootAttempted = $true
    Restart-Computer -Confirm:$false -ErrorAction Stop
} catch {
    $reason = 'OWNER_PREPARATION_FAILED'
    if ($_.Exception.Message -match '^OWNER_[A-Z_]+$') { $reason = $_.Exception.Message }
    $disabled = $false
    if ($created -and $null -ne $folder) {
        try {
            $ours = $folder.GetTask($TaskName)
            VerifyTask $ours $pins
            $ours.Enabled = $false
            $disabled = $true
        } catch { $reason = $reason + '_TASK_DISABLE_UNCONFIRMED' }
    }
    SaveJson (Join-Path $Package ('owner-failure-' + [guid]::NewGuid().ToString('N') + '.json')) ([ordered]@{status='FAILED';reason=$reason;utc=[DateTime]::UtcNow.ToString('o');created_this_invocation=$created;exact_created_task_disabled=$disabled;reboot_attempted=$rebootAttempted})
    Write-Error $reason
    exit 2
}
'''


def prepare(refresh=False):
    if d.PACKAGE.exists():
        d.check(refresh and not any((d.PACKAGE/name).exists() for name in
            ('install-request.json','smoke-result.json','pre-reboot-baseline.json','postboot-result.json')),
            'BOOT_UNACTIVATED_PREPARATION_REQUIRED')
        revision = 1
        while (d.PACKAGE/('superseded-'+str(revision).zfill(2))).exists():
            revision += 1
        previous = d.PACKAGE/('superseded-'+str(revision).zfill(2))
        previous.mkdir()
        for name in ('pins.json','task.xml','install-smoke-and-reboot.ps1','preparation-result.json'):
            (previous/name).write_bytes((d.PACKAGE/name).read_bytes())
    else:
        d.check(not refresh,'BOOT_EXISTING_PREPARATION_REQUIRED_FOR_REFRESH')
    for relative,expected in HOST_FILES.items():
        d.check(d.sha(d.ROOT/relative) == expected,'BOOT_ACCEPTED_HOST_FILES_CHANGED')
    d.check(d.sha(CASE_ROOT/'result.json') == RESULT_SHA,'BOOT_ACCEPTED_RESULT_CHANGED')
    accepted = d.read(CASE_ROOT/'result.json')
    cases = []
    for name in ('restart','stopped','exhausted','canonical'):
        path = CASE_ROOT/('canonical-denial-manifest.json' if name == 'canonical' else name+'/host-manifest.json')
        manifest = d.read(path)
        item = dict(name=name,manifest=str(path),sha256=d.sha(path))
        if name != 'canonical':
            d.check(item['sha256'] == accepted['manifests'][name]['sha256'],'BOOT_ACCEPTED_CASE_CHANGED')
            state = d.snapshot(manifest)
            expected = (accepted['details']['replacement']['second_control'] if name == 'restart' else manifest['initial_control'])
            d.check(state['control'] == expected,'BOOT_ACCEPTED_CONTROL_CHANGED')
            d.check(state['economic_digest'] == accepted['details']['replacement']['ledger_economic_sha256'],
                    'BOOT_ACCEPTED_ECONOMICS_CHANGED')
            item['initial_state'] = state
        cases.append(item)
    entry = d.ROOT/'scripts/live_operations_windows_host_v0_1.py'
    driver = Path(d.__file__).resolve()
    frozen = [dict(path=str(d.ROOT/p),sha256=h) for p,h in HOST_FILES.items()]
    frozen += [dict(path=str(p),sha256=d.sha(p)) for p in (driver,CASE_ROOT/'result.json',Path(sys.executable),Path(sys._base_executable))]
    restart = d.read(cases[0]['manifest'])
    interpreter = restart['interpreter_binding']
    d.check(d.sha(sys.executable) == interpreter['launcher_sha256']
        and d.sha(sys._base_executable) == interpreter['base_sha256']
        and str(Path(sys._base_executable).resolve()) == interpreter['base_path']
        and sys.version == interpreter['version'],'BOOT_ACCEPTED_INTERPRETER_CHANGED')
    frozen += restart['accepted_bindings']
    arguments = '-B "'+str(driver)+'" --pins "'+str(d.PINS)+'" --sha256 {PINS_SHA256}'
    pins = dict(schema='MEME_LIVE_M46_BOOT_PINS_V1',checkout=str(d.ROOT),host='DESKTOP-SVU1PTT',
        user=USER,sid=SID,task_name=d.TASK,python=sys.executable,host_entry=str(entry),
        task_arguments_template=arguments,cases=cases,frozen_files=frozen,
        public_configuration=restart['accepted_bindings'][0]['path'],scope='Retained synthetic M46 qualification only',
        run_bound='One 120-second active host plus stopped/exhausted/canonical denials; no sustained Runtime operation.')
    d.PACKAGE.mkdir(exist_ok=refresh)
    d.write(d.PINS,pins)
    pin_sha = d.sha(d.PINS)
    namespace = d.NS['t']
    ET.register_namespace('',namespace)
    root = ET.Element('{'+namespace+'}Task',version='1.4')
    def add(parent,name,text=None,**attributes):
        node = ET.SubElement(parent,'{'+namespace+'}'+name,attributes)
        node.text = text
        return node
    registration = add(root,'RegistrationInfo')
    add(registration,'Author',USER)
    add(registration,'Description','M46 retained synthetic boot qualification. No canonical LIVE activation or execution ports.')
    triggers = add(root,'Triggers')
    boot = add(triggers,'BootTrigger')
    add(boot,'Enabled','true')
    add(boot,'Delay','PT30S')
    principals = add(root,'Principals')
    principal = add(principals,'Principal',id='QualificationUser')
    add(principal,'UserId',SID)
    add(principal,'LogonType','S4U')
    add(principal,'RunLevel','LeastPrivilege')
    settings = add(root,'Settings')
    for key,value in [('MultipleInstancesPolicy','IgnoreNew'),('DisallowStartIfOnBatteries','false'),
        ('StopIfGoingOnBatteries','false'),('AllowHardTerminate','false'),('StartWhenAvailable','true'),
        ('RunOnlyIfNetworkAvailable','false'),('AllowStartOnDemand','true'),('Enabled','true'),
        ('Hidden','false'),('RunOnlyIfIdle','false'),('WakeToRun','false'),('ExecutionTimeLimit','PT10M'),('Priority','7')]:
        add(settings,key,value)
    actions = add(root,'Actions',Context='QualificationUser')
    execution = add(actions,'Exec')
    add(execution,'Command',sys.executable)
    add(execution,'Arguments',arguments.replace('{PINS_SHA256}',pin_sha))
    add(execution,'WorkingDirectory',str(d.ROOT))
    ET.indent(root)
    # RegisterTask receives a UTF-16 COM BSTR. An UTF-8 declaration in that
    # string causes native SCHED_E_MALFORMEDXML "cannot switch encoding".
    # Omit an encoding claim; the on-disk artifact remains UTF-8/ASCII text.
    xml = '<?xml version="1.0"?>\n'+ET.tostring(root,encoding='unicode')+'\n'
    (d.PACKAGE/'task.xml').write_text(xml,encoding='utf-8')
    d.task_check(xml,pins)
    script = OWNER_SCRIPT
    for key,value in {'PACKAGE':str(d.PACKAGE),'PINS_SHA':pin_sha,'XML_SHA':d.sha(d.PACKAGE/'task.xml'),
        'PYTHON':sys.executable,'DRIVER':str(driver),'CHECKOUT':str(d.ROOT)}.items():
        script = script.replace('__'+key+'__',value)
    owner = d.PACKAGE/'install-smoke-and-reboot.ps1'
    owner.write_text(script,encoding='utf-8')
    d.write(d.PACKAGE/'preparation-result.json',dict(status='PREPARED_NOT_REGISTERED',
        files={str(p):d.sha(p) for p in (d.PINS,d.PACKAGE/'task.xml',owner,driver,Path(__file__).resolve())},
        owner_command='& "'+d.POWERSHELL+'" -NoProfile -ExecutionPolicy Bypass -File "'+str(owner)+'" -InstallAndReboot',
        required_context='Elevated PowerShell under '+USER+'; no credentials supplied or stored.',
        expected_smoke_seconds='Approximately 130–180 seconds; owner deadline 7 minutes, task bound 10 minutes.',
        physical_reboot=False,task_registered=False,canonical_positive_startup=False))
    print(json.dumps(dict(package=str(d.PACKAGE),pins_sha256=pin_sha,task_xml_sha256=d.sha(d.PACKAGE/'task.xml'),owner_sha256=d.sha(owner))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--refresh-unactivated',action='store_true')
    prepare(parser.parse_args().refresh_unactivated)
