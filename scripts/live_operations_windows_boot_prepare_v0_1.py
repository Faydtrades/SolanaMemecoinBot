"""Offline M46 Task Scheduler artifacts. Never registers, launches Runtime or reboots."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

import live_operations_windows_boot_driver_v0_1 as d

CASE_ROOT = d.PRIOR_PACKAGE.parent/'m46-host-process-03'
RESULT_SHA = 'ecc1fd82b1b8d9d18ed15443a2f69ed9690cbe087428df76758247eac574d34c'
HOST_FILES = {
    'src/live/operations_windows_host_v0_1.py':'6eb7396cebe12a08eb67ca50d1640c86e955c331359852e563a38a73c3608f7d',
    'scripts/live_operations_windows_host_v0_1.py':'889b0e983859b6a8acfd9ca83240b92030d3630810fc9b8714d7ad3238779471',
    'scripts/live_operations_windows_host_qualification_v0_1.py':'0d46d59b817453d7400500f04d865bbc0cec6cedd55204940f8da81c5ef049c4'}
SID = 'S-1-5-21-3858496780-2977153031-2895069985-1001'
USER = r'DESKTOP-SVU1PTT\Mari1'


OWNER_SCRIPT = r'''param([switch]$InstallAndReboot, [switch]$RetryDisabledFailedTask, [switch]$ValidateOnly)
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
$RetryLineage = Join-Path $Package 'retry-lineage.json'
$RetryLineageSha = '__LINEAGE_SHA__'
$PriorPackage = '__PRIOR_PACKAGE__'
$created = $false
$updated = $false
$updateAttempted = $false
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
function VerifyTask($Registered, $Pins, [string]$ExpectedState='Active', [string]$BindingSha=$PinsSha) {
    Require ($ExpectedState -cin @('Active','DisabledRetry')) 'OWNER_EXPECTED_TASK_STATE_REQUIRED'
    [xml]$actual = $Registered.Xml
    $ns = [Xml.XmlNamespaceManager]::new($actual.NameTable)
    $ns.AddNamespace('t','http://schemas.microsoft.com/windows/2004/02/mit/task')
    foreach ($name in @('Triggers','Actions','Principals','Settings')) {
        Require ($actual.SelectNodes('/t:Task/t:'+$name,$ns).Count -eq 1) 'OWNER_EXACT_CONTAINER_REQUIRED'
    }
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
        '/t:Task/t:Settings/t:Enabled' = $(if ($ExpectedState -ceq 'Active') {'true'} else {'false'})
        '/t:Task/t:Settings/t:StartWhenAvailable' = 'true'
        '/t:Task/t:Settings/t:AllowStartOnDemand' = 'true'
        '/t:Task/t:Actions/t:Exec/t:Command' = $Pins.python
        '/t:Task/t:Actions/t:Exec/t:Arguments' = $Pins.task_arguments_template.Replace('{PINS_SHA256}',$BindingSha)
        '/t:Task/t:Actions/t:Exec/t:WorkingDirectory' = $Pins.checkout
    }
    # Documented Task Scheduler effective defaults only.
    $defaults = @{
        '/t:Task/t:Triggers/t:BootTrigger/t:Enabled' = 'true'
        '/t:Task/t:Principals/t:Principal/t:RunLevel' = 'LeastPrivilege'
        '/t:Task/t:Settings/t:Enabled' = 'true'
        '/t:Task/t:Settings/t:AllowStartOnDemand' = 'true'
    }
    foreach ($key in $expect.Keys) {
        $nodes = $actual.SelectNodes($key,$ns)
        Require ($nodes.Count -le 1) 'OWNER_DUPLICATE_CRITICAL_NODE'
        $effective = if ($nodes.Count -eq 1) {$nodes[0].InnerText} else {$defaults[$key]}
        Require ($effective -ceq $expect[$key]) 'OWNER_REGISTERED_TASK_BINDING_CONFLICT'
    }
    Require ($null -eq $actual.SelectSingleNode('/t:Task/t:Settings/t:RestartOnFailure',$ns)) 'OWNER_NO_SCHEDULER_RETRY_REQUIRED'
}
function VerifyRetry($Registered) {
    Require ($null -ne $Registered) 'OWNER_RETRY_EXISTING_TASK_REQUIRED'
    $lineageOutput = & $Python -B $Driver --pins $PinsPath --sha256 $PinsSha --validate-retry-lineage $RetryLineageSha 2>&1
    Require ($LASTEXITCODE -eq 0) 'OWNER_RETRY_LINEAGE_INVALID'
    $oldPins = Get-Content -LiteralPath (Join-Path $PriorPackage 'pins.json') -Raw | ConvertFrom-Json
    $oldRequest = Get-Content -LiteralPath (Join-Path $PriorPackage 'install-request.json') -Raw | ConvertFrom-Json
    Require ($Registered.Path -ceq ('\'+$TaskName)) 'OWNER_RETRY_EXACT_TASK_REQUIRED'
    Require (-not $Registered.Enabled -and [int]$Registered.State -eq 1) 'OWNER_RETRY_DISABLED_NOT_RUNNING_REQUIRED'
    Require ($Registered.GetInstances(0).Count -eq 0) 'OWNER_RETRY_NO_RUNNING_INSTANCE_REQUIRED'
    Require ([int]$Registered.LastTaskResult -eq 267011) 'OWNER_RETRY_NEVER_RUN_REQUIRED'
    VerifyTask $Registered $oldPins 'DisabledRetry' $oldRequest.pins_sha256
    Require ([int]$Registered.Definition.Principal.RunLevel -eq 0) 'OWNER_RETRY_NATIVE_LEAST_PRIVILEGE_REQUIRED'
    return $oldRequest
}

try {
    Require (-not ($ValidateOnly -and $InstallAndReboot)) 'OWNER_CONFLICTING_MODE_FLAGS'
    Require ((Sha $PinsPath) -eq $PinsSha -and (Sha $Definition) -eq $DefinitionSha) 'OWNER_PACKAGE_HASH_CONFLICT'
    $pins = Get-Content -LiteralPath $PinsPath -Raw | ConvertFrom-Json
    foreach ($item in $pins.frozen_files) { Require ((Sha $item.path) -eq $item.sha256) 'OWNER_FROZEN_FILE_CONFLICT' }
    $validationOutput = & $Python -B $Driver --pins $PinsPath --sha256 $PinsSha --validate-only 2>&1
    Require ($LASTEXITCODE -eq 0) 'OWNER_FIXED_INPUT_VALIDATION_FAILED'
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    Require ($identity.User.Value -eq $ExpectedSid) 'OWNER_SAME_ACCOUNT_REQUIRED'
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    $elevated = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    if ($RetryDisabledFailedTask) {
        $service = New-Object -ComObject 'Schedule.Service'
        $service.Connect()
        $folder = $service.GetFolder('\')
        $existing = $null
        try { $existing = $folder.GetTask($TaskName) } catch {
            Require ($_.Exception.GetBaseException().HResult -eq -2147024894) 'OWNER_TASK_LOOKUP_FAILED'
        }
        $oldRequest = VerifyRetry $existing
    }
    if (-not $InstallAndReboot) {
        [pscustomobject]@{status='PREPARED_NOT_REGISTERED';elevated=$elevated;pins_sha256=$PinsSha;task_xml_sha256=$DefinitionSha;validation=$validationOutput}|ConvertTo-Json -Depth 4
        exit 0
    }
    Require $elevated 'OWNER_ELEVATED_SAME_ACCOUNT_REQUIRED'
    Require $RetryDisabledFailedTask 'OWNER_EXPLICIT_DISABLED_RETRY_REQUIRED'
    Require (-not (Test-Path -LiteralPath (Join-Path $Package 'install-request.json'))) 'OWNER_FRESH_INSTALL_REQUEST_REQUIRED'
    Require (-not (Test-Path -LiteralPath (Join-Path $Package 'smoke-result.json'))) 'OWNER_NO_STALE_SMOKE_RECEIPT_ALLOWED'
    foreach ($name in @('verified-smoke.json','pre-reboot-baseline.json','postboot-result.json')) {
        Require (-not (Test-Path -LiteralPath (Join-Path $Package $name))) 'OWNER_FRESH_RETRY_EVIDENCE_REQUIRED'
    }
    $service = New-Object -ComObject 'Schedule.Service'
    $service.Connect()
    $folder = $service.GetFolder('\')
    $existing = $null
    try { $existing = $folder.GetTask($TaskName) } catch {
        Require ($_.Exception.GetBaseException().HResult -eq -2147024894) 'OWNER_TASK_LOOKUP_FAILED'
    }
    $oldRequest = VerifyRetry $existing
    $boot = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime().ToString('o')
    $request = [ordered]@{request_id=[guid]::NewGuid().ToString('N');requested_utc=[DateTime]::UtcNow.ToString('o');boot_utc=$boot;pins_sha256=$PinsSha;task_xml_sha256=$DefinitionSha;sid=$identity.User.Value;prior_request_id=$oldRequest.request_id;retry_lineage_sha256=$RetryLineageSha}
    Require ($request.request_id -cne $oldRequest.request_id) 'OWNER_FRESH_RETRY_LINEAGE_REQUIRED'
    SaveJson (Join-Path $Package 'install-request.json') $request
    $xmlText = Get-Content -LiteralPath $Definition -Raw
    # Recheck immediately before TASK_UPDATE=4 of the exact owned disabled task.
    $null = VerifyRetry ($folder.GetTask($TaskName))
    $updateAttempted = $true
    $registered = $folder.RegisterTask($TaskName,$xmlText,4,$ExpectedSid,$null,2,$null)
    $updated = $true
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
    $originalPreserved = $false
    if (($created -or $updateAttempted) -and $null -ne $folder) {
        try {
            $ours = $folder.GetTask($TaskName)
            Require ($ours.Path -ceq ('\'+$TaskName)) 'OWNER_EXACT_CLEANUP_TASK_REQUIRED'
            $cleanupState = if ($ours.Enabled) {'Active'} else {'DisabledRetry'}
            VerifyTask $ours $pins $cleanupState
            if ($ours.Enabled) { $ours.Enabled = $false }
            $ours = $folder.GetTask($TaskName)
            VerifyTask $ours $pins 'DisabledRetry'
            Require (-not $ours.Enabled) 'OWNER_DISABLE_NOT_CONFIRMED'
            $disabled = $true
        } catch {
            try { $null = VerifyRetry ($folder.GetTask($TaskName)); $originalPreserved = $true }
            catch { $reason = $reason + '_TASK_DISABLE_UNCONFIRMED' }
        }
    }
    SaveJson (Join-Path $Package ('owner-failure-' + [guid]::NewGuid().ToString('N') + '.json')) ([ordered]@{status='FAILED';reason=$reason;utc=[DateTime]::UtcNow.ToString('o');created_this_invocation=$created;update_attempted=$updateAttempted;updated_this_invocation=$updated;exact_created_task_disabled=$disabled;original_disabled_task_preserved=$originalPreserved;reboot_attempted=$rebootAttempted;retry_lineage_sha256=$RetryLineageSha})
    Write-Error $reason
    exit 2
}
'''


def prepare_disabled_retry():
    """Reuse retained inputs without rerunning qualification or opening any DB."""
    d.check(not d.PACKAGE.exists(),'BOOT_FRESH_RETRY_PACKAGE_REQUIRED')
    old = d.read(d.PRIOR_PACKAGE/'pins.json')
    d.check(old['checkout'] == str(d.ROOT) and old['python'] == sys.executable,
            'BOOT_RETRY_DEPLOYMENT_CONFLICT')
    driver = Path(d.__file__).resolve()
    preparation = Path(__file__).resolve()
    names = ('pins.json','task.xml','install-smoke-and-reboot.ps1','preparation-result.json',
        'install-request.json','owner-failure-3a0df6528d8d42cb94c2bf7947d04805.json','registered-task-actual.xml')
    lineage = dict(schema='MEME_LIVE_M46_DISABLED_RETRY_V1',prior_package=str(d.PRIOR_PACKAGE),
        retry_package=str(d.PACKAGE),prior_request_id='b03b73ffa3914beaa0626a4bea277dc4',
        prior_files={n:d.sha(d.PRIOR_PACKAGE/n) for n in names},
        historical_driver_sha256=next(i['sha256'] for i in old['frozen_files'] if i['path'] == str(driver)),
        current_driver_sha256=d.sha(driver))
    for relative, expected in HOST_FILES.items():
        d.check(d.sha(d.ROOT/relative) == expected,'BOOT_ACCEPTED_HOST_FILES_CHANGED')
    # Historical driver hash is retained in lineage; only the new manifest uses
    # amended source hashes. No pin refers to a hash of itself or its helper.
    pins = json.loads(json.dumps(old))
    for item in pins['frozen_files']:
        if item['path'] == str(driver):
            item['sha256'] = d.sha(driver)
        else:
            d.check(d.sha(item['path']) == item['sha256'],'BOOT_RETAINED_INPUT_CHANGED')
    pins['task_arguments_template'] = old['task_arguments_template'].replace(
        str(d.PRIOR_PACKAGE/'pins.json'),str(d.PINS))
    d.check(pins['task_arguments_template'] != old['task_arguments_template'],'BOOT_FRESH_PIN_PATH_REQUIRED')
    d.PACKAGE.mkdir()
    lineage_path = d.PACKAGE/'retry-lineage.json'
    d.write(lineage_path,lineage)
    lineage_sha = d.sha(lineage_path)
    d.retry_lineage(lineage_path,lineage_sha)
    pins['frozen_files'] += [dict(path=str(p),sha256=d.sha(p)) for p in (preparation,lineage_path)]
    d.write(d.PINS,pins)
    pin_sha = d.sha(d.PINS)
    tree = ET.fromstring((d.PRIOR_PACKAGE/'task.xml').read_text(encoding='utf-8-sig'))
    tree.find('t:Actions/t:Exec/t:Arguments',d.NS).text = pins['task_arguments_template'].replace('{PINS_SHA256}',pin_sha)
    ET.register_namespace('',d.NS['t'])
    xml = '<?xml version="1.0"?>\n'+ET.tostring(tree,encoding='unicode')+'\n'
    d.task_check(xml,pins,pins_sha=pin_sha)
    (d.PACKAGE/'task.xml').write_text(xml,encoding='utf-8')
    script = OWNER_SCRIPT
    for key,value in {'PACKAGE':str(d.PACKAGE),'PINS_SHA':pin_sha,'XML_SHA':d.sha(d.PACKAGE/'task.xml'),
        'PYTHON':sys.executable,'DRIVER':str(driver),'CHECKOUT':str(d.ROOT),
        'LINEAGE_SHA':lineage_sha,'PRIOR_PACKAGE':str(d.PRIOR_PACKAGE)}.items():
        script = script.replace('__'+key+'__',value)
    owner = d.PACKAGE/'install-smoke-and-reboot.ps1'
    owner.write_text(script,encoding='utf-8')
    d.write(d.PACKAGE/'preparation-result.json',dict(status='PREPARED_NOT_REGISTERED',
        files={str(p):d.sha(p) for p in (d.PINS,d.PACKAGE/'task.xml',owner,driver,preparation,lineage_path)},
        prior_request_id=lineage['prior_request_id'],historical_driver_sha256=lineage['historical_driver_sha256'],
        owner_command='& "'+d.POWERSHELL+'" -NoProfile -ExecutionPolicy Bypass -File "'+str(owner)+'" -InstallAndReboot -RetryDisabledFailedTask',
        validation_only_command='& "'+d.POWERSHELL+'" -NoProfile -ExecutionPolicy Bypass -File "'+str(owner)+'" -ValidateOnly -RetryDisabledFailedTask',
        required_context='Elevated PowerShell under '+USER+'; no credentials supplied or stored.',
        physical_reboot=False,task_registered=False,canonical_positive_startup=False))
    print(json.dumps(dict(package=str(d.PACKAGE),pins_sha256=pin_sha,task_xml_sha256=d.sha(d.PACKAGE/'task.xml'),owner_sha256=d.sha(owner))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--prepare-disabled-retry',action='store_true',required=True)
    parser.parse_args()
    prepare_disabled_retry()
