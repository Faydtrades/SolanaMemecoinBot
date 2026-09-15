"""Isolated derivation and durable resource reservation boundaries; no network."""
from __future__ import annotations

import copy
import json
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
from live import t010_resource_envelope_v0_1 as resources
from live import t010_public_host_v0_1 as host
from live.continuous_producer_v0_2 import ContinuationProfileV02
from live.operations_ownership_v0_1 import RestartProfile

CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def fails(call):
    try:
        call()
    except (ValueError, TypeError, KeyError, OSError):
        return True
    return False


def fixture():
    profile = asdict(ContinuationProfileV02(active_mints=64,hot_mint_events=512,hot_mint_bytes=2*1024*1024,
        retained_events=1024,retained_bytes=8*1024*1024,pending_rows=8192,pending_bytes=16*1024*1024,
        history_rows=256,history_bytes=8*1024*1024,batch_rows=32))
    original = {'constructor_configuration':{'continuation_profile':profile,
        'restart_profile':{'max_attempts':100}, 'dispatch':{'batch_rows':32,'page_rows':32,'queued_roots':64}},
        'source_configuration':{'profile':{'max_rows_per_table':10000,'freshness_seconds':30}}}
    inputs = {'source_start':{'profile':original['source_configuration']['profile']},
        'driver':{'max_steps':316}, 'monitor':{'resource_limits':{},'protective_qualification_digest':'a'*64}}
    extension = {'limits':{'HOST_RSS_BYTES':96*1024*1024,'HOST_DISK_RESERVE_BYTES':1024**3,
        'STARTUP_US':4000000,'PROTECTIVE_STEP_US':2000000,'OLDEST_UNCONSUMED_AGE_US':30000000}}
    return inputs, original, extension


def main():
    inputs, original, extension = fixture()
    value = resources.derive(inputs, original, extension)
    p = original['constructor_configuration']['continuation_profile']
    check('host_source_scope_original10000',value['host']['max_source_rows']==10000)
    check('ceil_sourceunits313',value['host']['max_source_units']==313)
    check('whole_startup100_and_steps415',value['work_budget']=={
        'max_startups':100,'max_runtime_steps':415,'target_dry_terminals':2})
    check('qualification_driver316_included',value['qualification']['max_runtime_steps']==316)
    check('lifetime_12h14h_unchanged',value['lifetime']=={'observation_us':43200000000,'hard_expiry_us':50400000000})
    check('joint_history256_not256plus128',value['joint']['launch_plus_retired_rows']==p['history_rows']==256)
    check('recovery_candidates_not_multiplied',value['joint']['maximum_candidate_roots']==128)
    check('cumulative_ledger_closed_paths',value['resource_limits']['LEDGER_TAIL_ROWS']==3+2+128+128+3*4+1)
    check('closed_payload_per_commit_not_observed_ratchet',value['resource_limits']['LEDGER_TAIL_PAYLOAD_BYTES']==
        value['resource_limits']['LEDGER_TAIL_ROWS']*value['cumulative']['ledger_commit_payload_bytes_per_row'])
    check('producer_pending_original8192',value['resource_limits']['PRODUCER_PENDING_ROWS']==8192)
    check('producer_history_original8MiB',value['resource_limits']['HISTORY_BYTES']==8*1024*1024)
    for key in extension['limits']:
        check('original_physical_or_age_cap_'+key,value['resource_limits'][key]==extension['limits'][key])
    first = resources.bound_inputs(inputs)
    copy_inputs = copy.deepcopy(inputs)
    copy_inputs['resource_envelope']={'path':'TEST','sha256':'0'*64}
    copy_inputs['monitor']['resource_limits']=value['resource_limits']
    check('no_envelope_hash_cycle',resources.bound_inputs(copy_inputs)==first)
    copy_inputs['driver']['max_steps']+=1
    check('work_input_change_invalidates_binding',resources.bound_inputs(copy_inputs)!=first)
    changed=copy.deepcopy(inputs);changed['source_start']['profile']['max_rows_per_table']=10001
    check('original_source_scope_cannot_widen',fails(lambda:resources.derive(changed,original,extension)))
    with tempfile.TemporaryDirectory(prefix='t010-resource-unit-') as tmp:
        directory=Path(tmp)
        envelope={'derivation':value}
        gate=resources.ResourceGate(directory/'resource.json',envelope,'host',create=True)
        metrics={'IDENTITY_ROWS':1,'TOMBSTONE_ROWS':0,'UNFINISHED_MINTS':1,
            'HISTORY_BYTES':100,'CHECKPOINT_BYTES':100,'PRODUCER_PENDING_ROWS':0,'PRODUCER_PENDING_BYTES':0}
        runtime=SimpleNamespace(capability='NO_BROADCAST',ledger=SimpleNamespace(
            _custody=SimpleNamespace(reservations=[])),_dry_recovery=False,producer=object(),
            source=SimpleNamespace(latest=lambda:SimpleNamespace(disposition='HEALTHY')))
        started=SimpleNamespace(runtime=runtime)
        # Pure reservation/cardinality cases use an explicit successful SQLite
        # boundary fixture; the original SQLite integration is tested separately.
        with patch('live.operations_degradation_monitor_v0_1._producer_cut',side_effect=lambda p:(metrics,{},'0'*64)), \
                patch('live.t010_sqlite_boundary_v0_1.checkpoint_owned_stores',return_value=(True,None,{})):
            check('reserved_before_original_source_work',gate.before_step(started)
                and gate.snapshot()['source_rows_reserved']==32 and runtime._resource_batch_rows==32)
            reopened=resources.ResourceGate(gate.path,envelope,'host')
            check('crash_empty_or_failed_page_not_refunded',reopened.snapshot()['source_rows_reserved']==32)
            for _ in range(311):
                assert reopened.before_step(started)
            check('last_source_batch_exact16',reopened.before_step(started) and runtime._resource_batch_rows==16
                and reopened.snapshot()['source_rows_reserved']==10000)
            check('source_scope_exhaustion_durable',not reopened.before_step(started)
                and resources.ResourceGate(gate.path,envelope,'host').snapshot()['held_reason']=='SOURCE_WINDOW_EXHAUSTED')
            other=resources.ResourceGate(directory/'joint.json',envelope,'host',create=True)
            metrics.update(IDENTITY_ROWS=100,TOMBSTONE_ROWS=90,UNFINISHED_MINTS=3)
            check('joint_next_retirement_and_launch_denied_before_reservation',not other.before_step(started)
                and other.snapshot()['source_rows_reserved']==0)
            active=resources.ResourceGate(directory/'active.json',envelope,'host',create=True)
            metrics.update(IDENTITY_ROWS=33,TOMBSTONE_ROWS=0,UNFINISHED_MINTS=33)
            check('active_next_batch_guard',not active.before_step(started))
            # Dormant launches consume one identity row; active/retired mints
            # consume two across L+R+A. These are gate boundary fixtures,
            # not claims that a particular market stream reaches each state.
            for name,launch,retired,unfinished,last,allowed in (
                ('full_joint_exact_with_dormant',100,80,12,False,True),
                ('full_joint_plus_one_with_dormant',101,80,12,False,False),
                ('full_active_exact',64,32,32,False,True),
                ('full_active_plus_one',65,32,33,False,False),
                ('last16_joint_exact',112,100,12,True,True),
                ('last16_joint_plus_one',113,100,12,True,False),
                ('last16_active_exact',80,32,48,True,True),
                ('last16_active_plus_one',81,32,49,True,False)):
                case=resources.ResourceGate(directory/(name+'.json'),envelope,'host',create=True)
                if last:
                    reserved=case.snapshot()
                    reserved.update(source_rows_reserved=9984,source_units_reserved=312)
                    case._write(reserved)
                metrics.update(IDENTITY_ROWS=launch,TOMBSTONE_ROWS=retired,UNFINISHED_MINTS=unfinished)
                before_rows=case.snapshot()['source_rows_reserved']
                check(name,case.before_step(started) is allowed
                    and case.snapshot()['source_rows_reserved']==before_rows+(16 if last else 32)*int(allowed))
            pending=resources.ResourceGate(directory/'pending.json',envelope,'host',create=True)
            runtime.ledger._custody.reservations=[SimpleNamespace(retired_sequence=None)]
            check('original_recovery_has_no_source_reservation',pending.before_step(started)
                and pending.snapshot()['source_rows_reserved']==0 and runtime._resource_batch_rows==0)
            pending.after_step(SimpleNamespace(work='SOURCE_HELD'))
            check('held_ordinary_work_preserves_original_recovery_priority',pending.before_step(started)
                and pending.snapshot()['source_rows_reserved']==0 and runtime._resource_batch_rows==0)
            runtime.ledger._custody.reservations=[]
            check('original_profile_exhaustion_does_not_spin',not pending.before_step(started))
            source_hold=resources.ResourceGate(directory/'source-hold.json',envelope,'host',create=True)
            runtime.source=SimpleNamespace(latest=lambda:SimpleNamespace(disposition='GAP'))
            source_hold.after_step(SimpleNamespace(work='OPERATIONS_ENTRY_HELD'),started)
            check('first_original_failed_source_cut_latches',source_hold.snapshot()['held_reason']=='SOURCE_EVIDENCE_NOT_HEALTHY')
            check('failed_cut_restart_cannot_resume_source',not resources.ResourceGate(source_hold.path,envelope,'host').before_step(started)
                and source_hold.snapshot()['source_rows_reserved']==0)
            crashed=resources.ResourceGate(directory/'source-crash.json',envelope,'host',create=True)
            runtime.ledger._custody.reservations=[]
            check('crash_before_callback_cannot_read_source',not crashed.before_step(started)
                and crashed.snapshot()['held_reason']=='SOURCE_EVIDENCE_NOT_HEALTHY')
        changed=copy.deepcopy(envelope);changed['derivation']['host']['max_source_rows']+=1
        check('changed_envelope_cannot_rearm',fails(lambda:resources.ResourceGate(gate.path,changed,'host')))
        check('missing_reservation_not_recreated',fails(lambda:resources.ResourceGate(directory/'missing.json',envelope,'host')))
        root=directory/'host';root.mkdir()
        budget=host.WorkBudget(root,{'work_budget':{'max_startups':2,'max_runtime_steps':5,'target_dry_terminals':2}},create=True)
        check('wholebudget_start_before_hold',budget.before_start())
        budget.after_start({'terminal_roots':[],'pending_action':False})
        budget.hold_resources()
        check('resource_hold_stops_start_and_step',not budget.before_step() and not budget.before_start())
        check('resource_hold_is_readonly_observing',budget.observation_state=='OBSERVING_RESOURCE_EXHAUSTED')
    print(json.dumps({'scope':'ISOLATED_SYNTHETIC_ENGINEERING','checks':CHECKS,'count':len(CHECKS)},sort_keys=True))


if __name__=='__main__':
    main()
