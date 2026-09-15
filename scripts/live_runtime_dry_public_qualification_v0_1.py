"""Explicit finite read-only public RPC/source qualification in NEW DRY stores.

This is not T010 activation. The request binds a reviewed public profile,
facts and explicit DRY-only controls by absolute path and SHA256. No keys,
signer, send transport, source repair, automatic candidate or grant creation.
"""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from live.acceptance_dossier_v0_1 import read_json, canonical_bytes
from live.runtime_dry_public_qualification_v0_1 import qualify, validate_evidence, qualification_health
from live.runtime_dry_profile_v0_1 import load_profile, read_reference


def _arguments(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--request', type=Path)
    group.add_argument('--verify', type=Path)
    parser.add_argument('--output', type=Path)
    return parser.parse_args(arguments)


def _direct_main(arguments=None, *, announce_success=True):
    args = _arguments(arguments)
    try:
        if args.verify:
            record = read_json(args.verify)
            validate_evidence(record, load_profile(read_reference(record['request']['profile'])))
        else:
            if args.output is None or not args.output.is_absolute() or args.output.exists():
                raise ValueError('NEW_ABSOLUTE_QUALIFICATION_OUTPUT_REQUIRED')
            record = qualify(read_json(args.request))
            validate_evidence(record, load_profile(read_reference(record['request']['profile'])))
            with args.output.open('xb') as stream:
                stream.write(canonical_bytes(record))
        if announce_success:
            print(json.dumps({'content_digest':record['content_digest'], 'scope':record['scope'],
                'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'grants_permission':False, 'T010_executed':False}, sort_keys=True))
        return 0
    except Exception as exc:
        # Never render remote exceptions, request strings or path/URL contents.
        reason = str(exc)
        safe = reason if reason and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in reason) else 'PUBLIC_QUALIFICATION_DENIED'
        print(json.dumps({'result':'DENIED', 'reason':safe, 'grants_permission':False}, sort_keys=True))
        return 2


def _wait_owned_child(child, *, timeout_us, termination_us, launched_ns):
    """Only the process we just created; no PID search, adoption or retry."""
    if any(type(v) is not int or v <= 0 for v in (timeout_us, termination_us)):
        raise ValueError('PUBLIC_QUALIFICATION_FINITE_WATCHDOG_REQUIRED')
    deadline = launched_ns+timeout_us*1000
    try:
        child.wait(timeout=max(0,(deadline-time.perf_counter_ns())/1000000000))
    except subprocess.TimeoutExpired:
        pass
    expired = child.poll() is None or time.perf_counter_ns() > deadline
    if child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=termination_us/1000000)
        except subprocess.TimeoutExpired:
            pass
    confirmed = child.poll() is not None
    result = {'deadline_expired':expired, 'termination_confirmed':confirmed,
        'exit_code':child.returncode, 'elapsed_us':(time.perf_counter_ns()-launched_ns)//1000,
        'timeout_us':timeout_us, 'termination_us':termination_us,'launched_monotonic_ns':launched_ns}
    return result


def _watchdog_budget(profile, request=None):
    inputs = profile.record['inputs']
    envelope = read_reference(inputs['resource_envelope'])
    resource = envelope['derivation']
    from live.t010_resource_envelope_v0_1 import observation_only
    health = qualification_health({} if request is None else request, profile) if observation_only(envelope) else None
    guards = resource['measured_guards'] if health is None else None
    startup = guards['supervisor_startup_us'] if health is None else health.startup_us
    progress = guards['supervisor_progress_us'] if health is None else health.progress_us
    termination = guards['supervisor_termination_us'] if health is None else health.termination_us
    work = resource['qualification']
    # Two original reopens and a finite number of dispatched steps. Initial
    # and final profile/receipt validation each use one complete supplied
    # startup allowance. Initial fresh wallet acquisition separately retains
    # its original observation budget. This is process containment, not an
    # extension of source freshness, clock validity or entry deadlines.
    starts, steps = work['max_startups'], inputs['driver']['max_steps']
    if starts != 2 or steps > work['max_runtime_steps']:
        raise ValueError('PUBLIC_QUALIFICATION_WORK_BOUND_CONFLICT')
    observations_us = inputs['driver']['public_rpc_profile']['observation_timeout_seconds']*1000000
    deadline = ((starts+2)*startup + steps*progress + observations_us)
    if type(deadline) is not int or not 0 < deadline <= 14*60*60*1000000:
        raise ValueError('PUBLIC_QUALIFICATION_WATCHDOG_ENVELOPE_CONFLICT')
    return guards, {'timeout_us':deadline,'termination_us':termination,
        'startups':starts,'maximum_steps':steps,'validation_startup_allowances':2,
        'initial_observation_us':observations_us}


def _publish_candidate(staged, output, process_result):
    if (process_result['deadline_expired'] or not process_result['termination_confirmed']
            or process_result['exit_code'] != 0):
        raise ValueError('PUBLIC_QUALIFICATION_PROCESS_DENIED')
    # The supported platform is Windows: rename refuses an existing target.
    # Failed/late child evidence remains staged and is never the final output.
    if os.name != 'nt' or output.exists():
        raise ValueError('NEW_ABSOLUTE_QUALIFICATION_OUTPUT_REQUIRED')
    os.rename(staged,output)


def main():
    args = _arguments()
    if args.verify:
        return _direct_main()
    child = None
    wait_entered = False
    try:
        if args.output is None or not args.output.is_absolute() or args.output.exists():
            raise ValueError('NEW_ABSOLUTE_QUALIFICATION_OUTPUT_REQUIRED')
        receipt = args.output.with_suffix(args.output.suffix+'.process.json')
        staged = args.output.with_suffix(args.output.suffix+'.candidate.json')
        if receipt.exists() or staged.exists():
            raise ValueError('NEW_QUALIFICATION_PROCESS_RECEIPT_REQUIRED')
        request = read_json(args.request)
        profile = load_profile(read_reference(request['profile']))
        guards, budget = _watchdog_budget(profile, request)
        from phase5.shadow_domain_v0_1 import content_fingerprint
        from live.operations_windows_host_v0_1 import WindowsHostBoundary
        from live.t010_resource_envelope_v0_1 import enforce_current_process_rss, enforce_host_tree_private_bytes
        # Original job API owns only this qualification parent and inherited
        # child. Parent death closes its non-inheritable KILL_ON_JOB_CLOSE
        # handle. No T010 host session, source repair or runtime activation.
        boundary = WindowsHostBoundary(content_fingerprint({'qualification':request,'output':str(args.output)})).enter()
        quota = {'scope':'OBSERVATION_ONLY_NO_NUMERICAL_QUOTA', 'quota_installed':False}
        if guards is not None:
            enforce_current_process_rss(guards['supervisor_rss_bytes'])
            quota = enforce_host_tree_private_bytes(boundary, guards)
        # Popen retains the exact native handle and has no multiprocessing
        # atexit join. Unconfirmed death therefore cannot prevent this parent
        # exiting and closing the original containment job.
        command = [sys.executable,'-B','-c',
            "import runpy,sys; m=runpy.run_path(sys.argv[1]); sys.exit(m['_direct_main'](sys.argv[2:],announce_success=False))",
            str(Path(__file__).resolve()),'--request',str(args.request),'--output',str(staged)]
        launched = time.perf_counter_ns()
        child = subprocess.Popen(command, cwd=ROOT)
        wait_entered = True
        result = _wait_owned_child(child, timeout_us=budget['timeout_us'],termination_us=budget['termination_us'],
            launched_ns=launched)
        result.update(schema='MEME_LIVE_PUBLIC_QUALIFICATION_PROCESS_V1',budget=budget,quota=quota,
            request=request,output=str(args.output),staged_output=str(staged),T010_executed=False,grants_permission=False)
        with receipt.open('xb') as stream:
            stream.write(canonical_bytes(result))
        _publish_candidate(staged,args.output,result)
        print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','qualification_published':True,
            'grants_permission':False,'T010_executed':False},sort_keys=True))
        return 0
    except Exception as exc:
        if child is not None and not wait_entered and child.poll() is None:
            try:
                child.terminate()
                child.wait(timeout=budget['termination_us']/1000000)
            except (OSError,subprocess.TimeoutExpired):
                # No unbounded join on exit. The parent's original job is
                # retained until process death and kills its inherited tree.
                pass
        reason = str(exc)
        safe = reason if reason and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in reason) else 'PUBLIC_QUALIFICATION_PROCESS_DENIED'
        print(json.dumps({'result':'DENIED','reason':safe,'grants_permission':False},sort_keys=True))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
