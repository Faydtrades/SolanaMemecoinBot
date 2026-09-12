"""Targeted offline verifier/retry tests. Native scheduler commands are mocked."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import tempfile
from unittest.mock import patch
import xml.etree.ElementTree as ET

import live_operations_windows_boot_driver_v0_1 as d
import live_operations_windows_boot_prepare_v0_1 as p


def must_fail(call):
    try:
        call()
    except (RuntimeError,KeyError):
        return
    raise AssertionError('expected denial')


def main():
    old = d.read(d.PRIOR_PACKAGE/'pins.json')
    old_sha = d.sha(d.PRIOR_PACKAGE/'pins.json')
    native = (d.PRIOR_PACKAGE/'registered-task-actual.xml').read_text(encoding='utf-16')
    vectors = []
    def vector(name, xml, state='Active', allowed=False):
        vectors.append(dict(name=name,xml=xml,state=state,allowed=allowed))
    vector('retained_disabled',native,'DisabledRetry',True)
    vector('disabled_is_not_active',native)
    active = native.replace('<Enabled>false</Enabled>','<Enabled>true</Enabled>')
    vector('normalized_active',active,allowed=True)
    vector('enabled_not_retry',active,'DisabledRetry')
    vector('settings_enabled_default',active.replace('<Enabled>true</Enabled>',''),allowed=True)
    vector('missing_enabled_not_retry',active.replace('<Enabled>true</Enabled>',''),'DisabledRetry')
    replacements = {
        'Triggers/BootTrigger/Enabled':'false', 'Triggers/BootTrigger/Delay':'PT31S',
        'Principals/Principal/UserId':'S-1-5-18', 'Principals/Principal/LogonType':'InteractiveToken',
        'Principals/Principal/RunLevel':'HighestAvailable', 'Settings/MultipleInstancesPolicy':'Parallel',
        'Settings/ExecutionTimeLimit':'PT0S', 'Settings/Enabled':'false', 'Settings/StartWhenAvailable':'false',
        'Settings/AllowStartOnDemand':'false', 'Actions/Exec/Command':'evil.exe',
        'Actions/Exec/Arguments':'--wrong-pin', 'Actions/Exec/WorkingDirectory':'C:\\other'}
    for path,value in replacements.items():
        tree = ET.fromstring(active)
        parts = path.split('/')
        parent = tree.find('/'.join('t:'+x for x in parts[:-1]),d.NS)
        node = parent.find('t:'+parts[-1],d.NS)
        if node is None:
            node = ET.SubElement(parent,'{'+d.NS['t']+'}'+parts[-1])
        node.text = value
        vector('contrary_'+path,ET.tostring(tree,encoding='unicode'))
        node.text = 'true' if parts[-1] in ('Enabled','AllowStartOnDemand') else (
            'LeastPrivilege' if parts[-1] == 'RunLevel' else tree.find('/'.join('t:'+x for x in parts),d.NS).text)
        parent.append(deepcopy(node))
        vector('duplicate_'+path,ET.tostring(tree,encoding='unicode'))
    for path in ('Settings','Triggers','Actions','Principals','Actions/Exec','Principals/Principal','Triggers/BootTrigger'):
        tree = ET.fromstring(active)
        parts = path.split('/')
        parent = tree if len(parts) == 1 else tree.find('t:'+parts[0],d.NS)
        parent.append(deepcopy(parent.find('t:'+parts[-1],d.NS)))
        vector('duplicate_container_'+path,ET.tostring(tree,encoding='unicode'))
    for path in ('Triggers/BootTrigger/Delay','Settings/MultipleInstancesPolicy','Settings/ExecutionTimeLimit','Settings/StartWhenAvailable'):
        tree = ET.fromstring(active)
        parts = path.split('/')
        parent = tree.find('/'.join('t:'+x for x in parts[:-1]),d.NS)
        parent.remove(parent.find('t:'+parts[-1],d.NS))
        vector('missing_required_'+path,ET.tostring(tree,encoding='unicode'))
    tree = ET.fromstring(active)
    ET.SubElement(tree.find('t:Settings',d.NS),'{'+d.NS['t']+'}RestartOnFailure')
    vector('scheduler_restart',ET.tostring(tree,encoding='unicode'))
    for item in vectors:
        call = lambda: d.task_check(item['xml'],old,expected_state=item['state'],pins_sha=old_sha)
        if item['allowed']:
            call()
        else:
            must_fail(call)

    with tempfile.TemporaryDirectory(prefix='m46-retry-selftest-') as temporary:
        root = Path(temporary)
        # Parse functions only; the native owner body is never evaluated here.
        source = root/'owner-source.ps1'
        source.write_text(p.OWNER_SCRIPT,encoding='utf-8')
        fixture = root/'verifier.json'
        d.write(fixture,dict(pins=old,sha=old_sha,vectors=vectors))
        harness = root/'verify.ps1'
        harness.write_text(r'''param($Source,$Fixture)
$ErrorActionPreference='Stop'
$tokens=$null; $errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($Source,[ref]$tokens,[ref]$errors)
if ($errors.Count) { throw 'parser errors' }
foreach($f in $ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst]},$false)) {
    if($f.Name -in @('Require','VerifyTask')) { . ([scriptblock]::Create($f.Extent.Text)) }
}
$data=Get-Content -Raw -LiteralPath $Fixture|ConvertFrom-Json
$ExpectedSid=$data.pins.sid; $PinsSha=$data.sha
foreach($v in $data.vectors) {
    $denied=$false
    try { VerifyTask ([pscustomobject]@{Xml=$v.xml}) $data.pins $v.state $data.sha } catch { $denied=$true }
    if ($denied -eq $v.allowed) { throw ('verifier mismatch: '+$v.name) }
}
'POWERSHELL_VERIFIER_VECTORS='+$data.vectors.Count
''',encoding='utf-8')
        result = subprocess.run([d.POWERSHELL,'-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(harness),str(source),str(fixture)],capture_output=True,text=True)
        assert result.returncode == 0, result.stdout+result.stderr
        print(result.stdout.strip())

        names = ('pins.json','task.xml','install-smoke-and-reboot.ps1','preparation-result.json',
            'install-request.json','owner-failure-3a0df6528d8d42cb94c2bf7947d04805.json','registered-task-actual.xml')
        lineage = dict(schema='MEME_LIVE_M46_DISABLED_RETRY_V1',prior_package=str(d.PRIOR_PACKAGE),
            retry_package=str(d.PACKAGE),prior_request_id='b03b73ffa3914beaa0626a4bea277dc4',
            prior_files={n:d.sha(d.PRIOR_PACKAGE/n) for n in names},
            historical_driver_sha256=next(i['sha256'] for i in old['frozen_files'] if i['path'] == str(Path(d.__file__).resolve())),
            current_driver_sha256=d.sha(d.__file__))
        line_path=root/'lineage.json'
        d.write(line_path,lineage)
        line_sha=d.sha(line_path)
        d.retry_lineage(line_path,line_sha)
        must_fail(lambda:d.retry_lineage(line_path,'0'*64))
        real_read=d.read
        failures=0
        for name,key,value in [('install-request.json','request_id','unrelated'),
            ('install-request.json','pins_sha256','0'*64),('install-request.json','task_xml_sha256','0'*64),
            ('owner-failure-3a0df6528d8d42cb94c2bf7947d04805.json','created_this_invocation',False),
            ('owner-failure-3a0df6528d8d42cb94c2bf7947d04805.json','reboot_attempted',True),
            ('owner-failure-3a0df6528d8d42cb94c2bf7947d04805.json','reason','OTHER')]:
            def mutated(path):
                data=real_read(path)
                if Path(path).name == name:
                    data[key]=value
                return data
            with patch.object(d,'read',side_effect=mutated):
                must_fail(lambda:d.retry_lineage(line_path,line_sha))
            failures+=1
        print('PYTHON_VERIFIER_VECTORS='+str(len(vectors)))
        print('LINEAGE_POSITIVE_AND_DENIALS='+str(failures+2))
        mock_owner(root,old,native)


def mock_owner(root, old, native):
    """Run the generated owner control flow with every native surface mocked."""
    harness=root/'mock-owner.ps1'
    harness.write_text(MOCK_HARNESS,encoding='utf-8')
    scenarios = ('success','validation_only','missing','unrelated','enabled','running','instances','ran','lineage',
        'recheck_changed','before_update_throw','after_update_throw','disabled_after_update_throw',
        'unrelated_after_update_throw','smoke_failure')
    for scenario in scenarios:
        case=root/scenario
        case.mkdir()
        prior=case/'prior';prior.mkdir()
        package=case/'retry';package.mkdir()
        d.write(prior/'pins.json',old)
        d.write(prior/'install-request.json',dict(request_id='old-request',pins_sha256=d.sha(d.PRIOR_PACKAGE/'pins.json')))
        (prior/'native.xml').write_text(native,encoding='utf-8')
        pins=deepcopy(old)
        pins['frozen_files']=[]
        pins['task_arguments_template']='new driver --pins fresh.json --sha256 {PINS_SHA256}'
        d.write(package/'pins.json',pins)
        tree=ET.fromstring(native)
        tree.find('t:Settings/t:Enabled',d.NS).text='true'
        tree.find('t:Actions/t:Exec/t:Arguments',d.NS).text=pins['task_arguments_template'].replace('{PINS_SHA256}','mock-pin-sha')
        (package/'task.xml').write_text(ET.tostring(tree,encoding='unicode'),encoding='utf-8')
        script=p.OWNER_SCRIPT
        for key,value in dict(PACKAGE=str(package),PINS_SHA='mock-pin-sha',XML_SHA='mock-xml-sha',
            PYTHON='MockDriver',DRIVER='mock-driver',LINEAGE_SHA='mock-lineage-sha',PRIOR_PACKAGE=str(prior)).items():
            script=script.replace('__'+key+'__',value)
        # Only identity/elevation and clock are replaced; all owner decisions,
        # binding checks, update sequencing, durable evidence and cleanup run.
        script=script.replace('[Security.Principal.WindowsIdentity]::GetCurrent()',"([pscustomobject]@{User=[pscustomobject]@{Value=$ExpectedSid}})")
        script=script.replace('[Security.Principal.WindowsPrincipal]::new($identity)',"$null")
        script=script.replace('$principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)','$true')
        script=script.replace('function Sha([string]$Path) { (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }',
            "function Sha([string]$Path) { if ($Path.EndsWith('pins.json')) {'mock-pin-sha'} else {'mock-xml-sha'} }")
        owner=case/'owner.ps1';owner.write_text(script,encoding='utf-8')
        result=subprocess.run([d.POWERSHELL,'-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(harness),str(owner),str(case),scenario],capture_output=True,text=True)
        assert result.returncode == 0, scenario+': '+result.stdout+result.stderr
        report=d.read(case/'mock-report.json')
        events=report['events']
        failures=list(package.glob('owner-failure-*.json'))
        if scenario=='success':
            assert events.count('update')==1 and events.count('start')==1 and events.count('reboot')==1,report
            request=d.read(package/'install-request.json')
            assert request['request_id']!='old-request' and request['prior_request_id']=='old-request'
            assert request['retry_lineage_sha256']=='mock-lineage-sha'
            assert not failures
        elif scenario=='validation_only':
            assert not events and not failures and not (package/'install-request.json').exists(),report
        else:
            assert 'reboot' not in events and len(failures)==1,(scenario,report)
            failure=d.read(failures[0])
            if scenario in ('after_update_throw','disabled_after_update_throw','smoke_failure'):
                assert not report['enabled'] and failure['exact_created_task_disabled'],(scenario,report,failure)
            elif scenario=='before_update_throw':
                assert failure['original_disabled_task_preserved'],failure
            elif scenario=='unrelated_after_update_throw':
                assert report['enabled'] and 'TASK_DISABLE_UNCONFIRMED' in failure['reason'],failure
            else:
                assert 'update' not in events and 'start' not in events,(scenario,report)
        assert d.read(prior/'install-request.json')['request_id']=='old-request'
    print('MOCK_OWNER_SCENARIOS='+str(len(scenarios)))


MOCK_HARNESS = r'''param($Owner,$Case,$Scenario)
$ErrorActionPreference='Stop'
$global:scenario=$Scenario; $global:events=[Collections.Generic.List[string]]::new()
$global:package=Join-Path $Case 'retry'; $global:lookups=0
$global:task=[pscustomobject]@{Path='\MEME-LIVE-M46-BOOT-QUALIFICATION';Xml=(Get-Content -Raw (Join-Path $Case 'prior\native.xml'));Flag=$false;State=1;LastTaskResult=267011;Definition=[pscustomobject]@{Principal=[pscustomobject]@{RunLevel=0}}}
$global:task|Add-Member ScriptProperty Enabled {$this.Flag} {
    param($value) $this.Flag=$value
    [xml]$xml=$this.Xml; $ns=[Xml.XmlNamespaceManager]::new($xml.NameTable);$ns.AddNamespace('t','http://schemas.microsoft.com/windows/2004/02/mit/task')
    $xml.SelectSingleNode('/t:Task/t:Settings/t:Enabled',$ns).InnerText=([string]$value).ToLowerInvariant();$this.Xml=$xml.OuterXml
}
$global:task|Add-Member ScriptMethod GetInstances {param($flags) [pscustomobject]@{Count=$(if($global:scenario -eq 'instances'){1}else{0})}}
if($Scenario -eq 'unrelated'){$global:task.Path='\OTHER'}
if($Scenario -eq 'enabled'){$global:task.Enabled=$true;$global:task.State=3}
if($Scenario -eq 'running'){$global:task.State=4}
if($Scenario -eq 'ran'){$global:task.LastTaskResult=0}
$global:folder=[pscustomobject]@{}
$global:folder|Add-Member ScriptMethod GetTask {
    param($name)
    if($name -cne 'MEME-LIVE-M46-BOOT-QUALIFICATION'){throw 'unexpected task lookup'}
    $global:lookups++
    if($global:scenario -eq 'missing'){return $null}
    if($global:scenario -eq 'recheck_changed' -and $global:lookups -ge 3){$global:task.Enabled=$true;$global:task.State=3}
    return $global:task
}
$global:folder|Add-Member ScriptMethod RegisterTask {
    param($name,$xml,$flags,$sid,$password,$logon,$sddl)
    if($name -cne 'MEME-LIVE-M46-BOOT-QUALIFICATION' -or $flags -ne 4 -or $logon -ne 2 -or $null -ne $password){throw 'unexpected update scope'}
    $global:events.Add('update')
    if($global:scenario -eq 'before_update_throw'){throw 'OWNER_MOCK_UPDATE_FAILURE'}
    $global:task.Xml=$xml;$global:task.Enabled=$true;$global:task.State=3
    if($global:scenario -eq 'disabled_after_update_throw'){$global:task.Enabled=$false;throw 'OWNER_MOCK_UPDATE_FAILURE'}
    if($global:scenario -eq 'unrelated_after_update_throw'){$global:task.Xml=$xml.Replace('new driver','unrelated driver');throw 'OWNER_MOCK_UPDATE_FAILURE'}
    if($global:scenario -eq 'after_update_throw'){throw 'OWNER_MOCK_UPDATE_FAILURE'}
    return $global:task
}
$global:service=[pscustomobject]@{}
$global:service|Add-Member ScriptMethod Connect {}
$global:service|Add-Member ScriptMethod GetFolder {param($name) if($name -cne '\'){throw 'unexpected folder'};return $global:folder}
function New-Object {param($ComObject) if($ComObject -cne 'Schedule.Service'){throw 'unmocked New-Object'};return $global:service}
function Get-CimInstance {param($ClassName) if($ClassName -cne 'Win32_OperatingSystem'){throw 'unmocked CIM'};[pscustomobject]@{LastBootUpTime=[DateTime]'2026-09-06T20:13:46Z'}}
function MockDriver {
    $global:LASTEXITCODE=0
    if($args -contains '--validate-retry-lineage' -and $global:scenario -eq 'lineage'){$global:LASTEXITCODE=2}
    if($args -contains '--verify-smoke'){
        $r=Get-Content -Raw (Join-Path $global:package 'install-request.json')|ConvertFrom-Json
        @{request_id=$r.request_id;pins_sha256=$r.pins_sha256;native_task_last_run_utc=$global:runTime.ToUniversalTime().ToString('o');native_task_xml_digest='mock-native-digest';preserved_states=@{};smoke_receipt=@{}}|ConvertTo-Json -Depth 10|Set-Content (Join-Path $global:package 'verified-smoke.json')
    }
    'mock validation'
}
function Start-Sleep {}
function Start-ScheduledTask {param($TaskName) if($TaskName -cne 'MEME-LIVE-M46-BOOT-QUALIFICATION'){throw 'unexpected start'};$global:events.Add('start');$global:runTime=[DateTime]::UtcNow.AddSeconds(2);'{}'|Set-Content (Join-Path $global:package 'smoke-result.json')}
function Get-ScheduledTask {param($TaskName) [pscustomobject]@{Principal=[pscustomobject]@{LogonType='S4U';RunLevel='Limited'};State='Ready'}}
function Get-ScheduledTaskInfo {param($TaskName) [pscustomobject]@{LastRunTime=$global:runTime;LastTaskResult=$(if($global:scenario -eq 'smoke_failure'){2}else{0})}}
function Restart-Computer {param($Confirm,$ErrorAction) $global:events.Add('reboot')}
try {
    if($Scenario -eq 'validation_only'){& $Owner -ValidateOnly -RetryDisabledFailedTask}
    else {& $Owner -InstallAndReboot -RetryDisabledFailedTask}
} catch { }
@{events=@($global:events.ToArray());enabled=$global:task.Enabled;lookups=$global:lookups}|ConvertTo-Json -Depth 10|Set-Content (Join-Path $Case 'mock-report.json')
'''


if __name__ == '__main__':
    main()
