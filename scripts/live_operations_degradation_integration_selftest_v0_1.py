"""C2 actual production integration; finite synthetic stores and mock I/O only."""
from __future__ import annotations
import argparse
import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_startup_selftest_v0_1 as a2
from live import operations_readiness_v0_1 as readiness
from live.operations_degradation_v0_1 import DegradationPolicy, DegradationStore, ResourceLimit, METRICS
from live.operations_degradation_monitor_v0_1 import (
    MonitorConfiguration, OperationsMonitor, HostObservations, HostMetric, CollectorSourceAdapter, content_fingerprint, monitor_configuration_digest,
)
from live.operations_supervisor_v0_1 import OperationsSupervisor, HealthProfile
from live.operations_ownership_v0_1 import RestartProfile

rt, NOW = a2.rt, a2.NOW
D = lambda n: format(n, '064x')
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


class Fixture(rt.Fixture):
    def __init__(self, directory, name, *, restart_profile=RestartProfile(100,1000000,0)):
        self._clock_upper = None
        self.now, self.host_mode = NOW+4, 'healthy'
        self.host_calls = 0
        super().__init__(directory, name, page=1, queue=2)
        self.candidate_ready()
        self.configure()
        a2.installed(self,profile=restart_profile)
        limits = tuple(ResourceLimit(metric, 10 if metric == 'HOST_DISK_RESERVE_BYTES' else 10**12)
            for metric in sorted(METRICS))
        alert_path=str(directory/(name+'-alerts.sqlite'))
        reviewed=monitor_configuration_digest(self.expected,path=alert_path,host_identity_digest=D(91),
            resource_max_age_us=1000000,protective_qualification_digest=D(92))
        policy = DegradationPolicy(reviewed, 100, 1000000, limits)
        self.monitor_config = MonitorConfiguration(alert_path, policy,
            D(91), self.expected.runtime_code_digest, 1000000, D(92))
        DegradationStore.initialize(self.monitor_config.path, self.domain, policy, now_us=0)

    def start(self, *, configured=True):
        a2.restart(self, degradation_config=self.monitor_config if configured else None)
        self.monitor = self.runtime._operations_degradation

    def host(self):
        self.host_calls += 1
        values = {'HOST_RSS_BYTES': 100, 'HOST_DISK_RESERVE_BYTES': 100, 'STARTUP_US': 100, 'PROTECTIVE_STEP_US': 100}
        if self.host_mode == 'high':
            values['HOST_RSS_BYTES'] = 10**12+1
        if self.host_mode == 'unknown':
            values['HOST_RSS_BYTES'] = None
        observed = self.now*1000000-(2000000 if self.host_mode == 'stale' else 0)
        metrics = tuple(HostMetric(metric, value,
            observed-86400000000 if self.host_mode=='old-timing' and metric in ('STARTUP_US','PROTECTIVE_STEP_US') else observed,
            (D(94) if self.host_mode=='unqualified' else D(92)) if metric=='PROTECTIVE_STEP_US' else D(93))
            for metric, value in values.items())
        return HostObservations(self.monitor_config.host_identity_digest,
            D(999) if self.host_mode == 'unbound' else self.monitor_config.policy.reviewed_configuration_digest,
            content_fingerprint(asdict(self.runtime._ownership.fence)), metrics)

    def clock(self, at=NOW+4):
        # One local positive timeline, independent of retained evidence times.
        sample = rt.a3.clock(self.repo, at)
        lower, upper = map(datetime.fromisoformat, (sample.utc_lower_utc, sample.utc_upper_utc))
        target = upper if self._clock_upper is None else max(upper, self._clock_upper+timedelta(microseconds=1))
        delta = target-upper
        self._clock_upper = target
        return replace(sample, utc_lower_utc=(lower+delta).isoformat(timespec='microseconds'),
            utc_upper_utc=target.isoformat(timespec='microseconds'),
            monotonic_ns=sample.monotonic_ns+(delta.days*86400000000+delta.seconds*1000000+delta.microseconds)*1000)

    def step(self, at=NOW+4, **kwargs):
        self.now = at
        return self.runtime.step(clock=lambda: self.clock(at), source_cut_utc=rt.a3.utc(NOW+1),
            operations_resources=self.host, **kwargs)

    def sample(self, at=None):
        self.now = self.now+1 if at is None else at
        return self.monitor.observe(self.clock(self.now), resources=self.host,
            source_cut_utc=rt.a3.utc(NOW+1))

    def alerts(self, code=None, state='ACTIVE'):
        return tuple(row for row in self.monitor.snapshot().conditions
            if row.state == state and (code is None or row.condition == code))


def close(f):
    a2.close(f)


def clock_order(directory):
    f = Fixture(directory, 'c2-clock-order')
    try:
        f.start()
        retained = f.source.latest_record()
        # Deliberate regression bypasses the positive fixture clock scheduler.
        early = rt.a3.clock(f.repo, NOW+4)
        view = f.monitor.observe(early, resources=f.host, source_cut_utc=rt.a3.utc(NOW+1))
        check('monitor_before_source_knowledge_stays_degraded', view.entry_held
            and f.alerts('SOURCE_TRUTH_UNAVAILABLE')
            and datetime.fromisoformat(early.utc_upper_utc) < datetime.fromisoformat(retained[1].snapshot.observed_at_utc)
            and f.source.latest_record() == retained)
        recovered = f.sample(NOW+4)
        check('later_known_fresh_source_recovers_original_condition', not recovered.entry_held
            and f.alerts('SOURCE_TRUTH_UNAVAILABLE', state='RECOVERED')
            and f.source.latest_record() == retained and not recovered.grants_permission)
        expired = f.sample(NOW+100)
        check('expired_source_cannot_reuse_healthy_recovery', expired.entry_held
            and f.alerts('SOURCE_TRUTH_UNAVAILABLE') and not expired.grants_permission)
    finally:
        close(f)


def preflight(directory):
    f = Fixture(directory,'c2-preflight')
    try:
        f.start()
        before = (dict(f.producer.metrics),dict(f.producer.last_profile_usage),f.producer.conn.total_changes)
        view = f.sample(NOW+4)
        if view.entry_held:
            print(json.dumps({'preflight_alerts':asdict(view),'metrics':f.monitor.last_metrics},sort_keys=True))
        check('configured_actual_owned_monitor_healthy', not view.entry_held)
        check('sampling_does_not_change_producer_counters', before ==
            (dict(f.producer.metrics),dict(f.producer.last_profile_usage),f.producer.conn.total_changes))
        check('positive_empty_backlog_age_inapplicable_not_zero', f.monitor.backlog_age_applicable is False
            and dict(f.monitor.last_metrics)['OLDEST_UNCONSUMED_AGE_US'] is None)
        result=f.step()
        check('actual_runtime_ordinary_waiting_no_incident',result.work=='NEED_ENTRY_FACTS' and not f.alerts())
        f.host_mode='old-timing'
        check('retained_qualified_timing_needs_no_new_protective_action',not f.sample(NOW+5).entry_held
            and f.runtime.position_binding is None)
        f.host_mode='unqualified'
        check('missing_timing_qualification_stays_unknown',f.sample(NOW+6).entry_held and f.alerts('PROFILE_UNRESOLVED'))
        f.host_mode='healthy'
        check('reviewed_timing_reference_recovers_profile',not f.sample(NOW+7).entry_held)
        f.append([(15,'C2-PENDING','LAUNCH',10000)])
        with closing(sqlite3.connect(f.raw)) as raw:
            raw.execute('UPDATE pump_events SET decoded_at_utc=?,inserted_at_utc=? WHERE rowid=15',
                (rt.a3.utc(NOW+7),rt.a3.utc(NOW+7)))
            raw.commit()
        f.sample(NOW+8)
        check('positive_nonempty_backlog_requires_actual_age',f.monitor.backlog_age_applicable is True
            and dict(f.monitor.last_metrics)['OLDEST_UNCONSUMED_AGE_US']==1000000)
        for field,value in (('path',str(directory/'different-alerts.sqlite')),('host_identity_digest',D(95)),
                ('resource_max_age_us',2),('protective_qualification_digest',D(96))):
            changed=OperationsMonitor(f.started,replace(f.monitor_config,**{field:value}),source_binding=f.binding,
                source_profile=f.source.profile,producer_profile=f.producer.profile)
            check('reviewed_monitor_binding_rejects_'+field,changed.store is None and changed.last.entry_held)
        partial_path=str(directory/'partial-profile.sqlite')
        partial_binding=monitor_configuration_digest(f.expected,path=partial_path,host_identity_digest=D(91),
            resource_max_age_us=1000000,protective_qualification_digest=D(92))
        partial_policy=replace(f.monitor_config.policy,reviewed_configuration_digest=partial_binding,resource_limits=())
        DegradationStore.initialize(partial_path,f.domain,partial_policy,now_us=0)
        partial_config=replace(f.monitor_config,path=partial_path,policy=partial_policy)
        partial=OperationsMonitor(f.started,partial_config,source_binding=f.binding,
            source_profile=f.source.profile,producer_profile=f.producer.profile)
        view=partial.observe(f.clock(NOW+9),resources=lambda:replace(f.host(),configuration_digest=partial_binding))
        check('empty_resource_profile_cannot_grant_entry',view.entry_held and
            any(row.condition=='PROFILE_UNRESOLVED' for row in view.conditions))
    finally:
        close(f)


def resources_and_source(directory):
    f=Fixture(directory,'c2-resources')
    try:
        f.start(configured=False)
        check('missing_configuration_holds_actual_runtime',f.step().work=='OPERATIONS_ENTRY_HELD')
        check('missing_configuration_readiness_agrees', 'CURRENT_DEGRADATION_ENTRY_HOLD' in
            readiness.evaluate(f.started,clock=lambda:f.clock(NOW+4)).entry.reasons)
        f.start()
        check('configured_healthy_initial',not f.sample(NOW+5).entry_held)
        f.host_mode='high'
        check('resource_high_actual_runtime_entry_hold',f.step(NOW+6).work=='OPERATIONS_ENTRY_HELD'
            and f.alerts('RESOURCE_EXCEEDED'))
        check('resource_high_actual_readiness_agrees','CURRENT_DEGRADATION_ENTRY_HOLD' in
            readiness.evaluate(f.started,clock=lambda:f.clock(NOW+6),operations_resources=f.host).entry.reasons)
        for index,mode in enumerate(('unknown','stale','unbound'),7):
            f.host_mode=mode
            check('resource_'+mode+'_unresolved',f.sample(NOW+index).entry_held and f.alerts('PROFILE_UNRESOLVED'))
        f.host_mode='healthy'
        check('fresh_bound_resource_recovery',not f.sample(NOW+10).entry_held
            and f.alerts('RESOURCE_EXCEEDED',state='RECOVERED') and f.alerts('PROFILE_UNRESOLVED',state='RECOVERED'))
        with patch.object(CollectorSourceAdapter,'capture',side_effect=OSError('https://private.invalid/TOKEN')):
            check('actual_source_read_loss_durable',f.sample(NOW+11).entry_held and f.alerts('SOURCE_TRUTH_UNAVAILABLE'))
        check('same_lineage_positive_continuity_recovers',not f.sample(NOW+12).entry_held
            and f.alerts('SOURCE_TRUTH_UNAVAILABLE',state='RECOVERED'))
        with closing(sqlite3.connect(f.raw)) as raw:
            original=raw.execute('SELECT signature FROM pump_events WHERE rowid=1').fetchone()[0]
            raw.execute('UPDATE pump_events SET signature=? WHERE rowid=1',('changed-public-anchor',))
            raw.commit()
        check('actual_source_identity_break_durable',f.sample(NOW+14).entry_held
            and f.alerts('SOURCE_IDENTITY_OR_HISTORY_BROKEN'))
        with closing(sqlite3.connect(f.raw)) as raw:
            raw.execute('UPDATE pump_events SET signature=? WHERE rowid=1',(original,)); raw.commit()
        check('restoring_anchor_does_not_auto_clear_integrity',f.sample(NOW+15).entry_held
            and f.alerts('SOURCE_IDENTITY_OR_HISTORY_BROKEN'))
        output=json.dumps(asdict(f.monitor.snapshot()))
        check('alert_output_sanitized','https://' not in output and 'TOKEN' not in output and
            all(row.action and row.alerted_us is not None for row in f.alerts()))
    finally:
        close(f)


def acquisition(f):
    support=rt.a3.wallet(f,scenario=f.scenario)
    admitted=f.step(NOW+4,entry=rt.EntryFacts('PUMP',rt.sf.TOKEN_PROGRAM_ID,support))
    check(f.name+'_actual_admission',admitted.work=='ENTRY_ADMITTED')
    f.buy=f.repo.action(admitted.action_id)
    return admitted


def prepared_buy(directory):
    key=rt.keypair('c2-prepared-buy')
    with patch.object(rt.sf,'WALLET',str(key.pubkey())),patch.object(rt.sf.plans,'ACTOR',str(key.pubkey())):
        f=Fixture(directory,'c2-prepared-buy')
        try:
            f.start(); acquisition(f)
            before=f.repo.consumer_snapshot()
            f.host_mode='high'
            result=f.step(NOW+5)
            check('admitted_buy_held_without_dispatch',result.work=='OPERATIONS_ENTRY_HELD'
                and not f.repo._root_attempts(f.root) and f.repo.consumer_snapshot()['reservations']==before['reservations'])
            f.host_mode='healthy'
            # Inject only the external resource change during actual Q1 simulation.
            original=rt.q1.PublicTransport.__call__
            def resource_loss(transport,request):
                result=original(transport,request)
                if json.loads(request.content)['method']=='simulateTransaction':
                    f.host_mode='high'
                return result
            with patch.object(rt.q1.PublicTransport,'__call__',resource_loss):
                result,read,send=rt.execution(f,key,f.buy,at=NOW+6,number=71)
            attempt=f.repo.attempt(result.attempt_id)
            check('prepared_buy_resource_rechecked_before_sign_send',result.work=='OPERATIONS_ENTRY_HELD'
                and attempt.primary_signature is None and attempt.lane_held and not send.requests)
            check('prepared_buy_capacity_preserved',f.repo.consumer_snapshot()['reservations']==before['reservations'])
            f.host_mode='healthy'
            f.sample(NOW+7)
            later=f.step(NOW+8)
            check('unsigned_resource_recovery_does_not_resume_or_release',later.work=='HELD'
                and later.reason=='UNSIGNED_OR_CONSUMED_ATTEMPT_REQUIRES_LATER_RECOVERY'
                and f.repo.attempt(result.attempt_id).lane_held and not f.alerts('RESOURCE_EXCEEDED')
                and f.alerts('UNSENT_ATTEMPT_RECOVERY_REQUIRED')
                and f.repo.consumer_snapshot()['reservations']==before['reservations'])
        finally:
            close(f)


def signed_unsent(directory):
    from live import runtime_composition_v0_1 as composition
    key=rt.keypair('c2-signed-unsent')
    with patch.object(rt.sf,'WALLET',str(key.pubkey())),patch.object(rt.sf.plans,'ACTOR',str(key.pubkey())):
        f=Fixture(directory,'c2-signed-unsent')
        try:
            f.start(); acquisition(f)
            before=f.repo.consumer_snapshot()
            original=composition.persist_signed_envelope
            def resource_loss(*args,**kwargs):
                result=original(*args,**kwargs)
                f.host_mode='high'
                return result
            with patch.object(composition,'persist_signed_envelope',resource_loss):
                result,read,send=rt.execution(f,key,f.buy,at=NOW+6,number=73)
            attempt=f.repo.attempt(result.attempt_id)
            check('signed_unsent_resource_rechecked_before_send',result.work=='OPERATIONS_ENTRY_HELD'
                and attempt.primary_signature is not None and attempt.recorded_stage=='SIGNED_DURABLE'
                and attempt.lane_held and not send.requests and f.alerts('UNSENT_ATTEMPT_RECOVERY_REQUIRED'))
            f.host_mode='healthy'
            f.sample(NOW+7)
            later=f.step(NOW+8)
            check('signed_unsent_resource_recovery_only_requests_original_truth',later.work=='NEED_RECONCILIATION'
                and f.repo.attempt(result.attempt_id).lane_held and not f.alerts('RESOURCE_EXCEEDED')
                and f.alerts('UNSENT_ATTEMPT_RECOVERY_REQUIRED')
                and f.repo.consumer_snapshot()['reservations']==before['reservations'])
        finally:
            close(f)


def unknown_and_protection(directory):
    key=rt.keypair('c2-unknown')
    with patch.object(rt.sf,'WALLET',str(key.pubkey())),patch.object(rt.sf.plans,'ACTOR',str(key.pubkey())):
        f=Fixture(directory,'c2-unknown')
        try:
            f.start(); acquisition(f)
            result,read,send=rt.execution(f,key,f.buy,at=NOW+6,number=72)
            check('actual_mock_submission_unknown_incident',result.work=='SUBMISSION_OBSERVED'
                and len(send.requests)==1 and f.alerts('SEND_UNKNOWN'))
            original=f.alerts('SEND_UNKNOWN')[0]
            attempt_id=result.attempt_id
            f.sample(NOW+7)
            check('persistent_unknown_alerts_at_explicit_interval',f.alerts('SEND_UNKNOWN')[0].alerted_us is not None)
            f.start()
            check('unknown_reopen_retains_age_and_reservation',f.alerts('SEND_UNKNOWN')[0].first_us==original.first_us
                and f.repo.attempt(attempt_id).lane_held and f.repo.consumer_snapshot()['reservations'])
            check('unknown_original_reconciliation_priority',f.step(NOW+8).work=='NEED_RECONCILIATION'
                and f.repo.attempt(attempt_id).lane_held)
            envelope,actual,scenario,pre=rt.original_chain(f,result,NOW+6)
            reconciled,calls=rt.reconcile(f,scenario,NOW+12)
            check('original_exact_finality_recovers_unknown_alert_only',reconciled.work=='RECONCILED'
                and f.alerts('SEND_UNKNOWN',state='RECOVERED') and f.repo.attempt(attempt_id).lane_held)
            chain=f.repo.chain_receipt(reconciled.receipt_key)
            support=rt.application_support(f,actual,chain,pre,NOW+14)
            applied=f.step(NOW+14,application_wallet=support)
            check('original_application_restores_position',applied.work=='APPLIED' and f.runtime.position_binding is not None)
            f.sample(NOW+15)
            check('ordinary_active_position_not_incident',not f.alerts())
            f.host_mode='high'
            position=f.runtime.position_binding.position_id
            due=f.step(NOW+16)
            check('due_protection_before_entry_while_resource_held',due.work=='PROTECTIVE_ACTION_STAGED'
                and f.alerts('RESOURCE_EXCEEDED') and f.runtime.position_binding.position_id==position)
            check('protective_obligation_preserved',rt.protective_outcome(f.repo,f.runtime.position_binding).obligation.state=='DUE')
            # A damaged separate alert store cannot reset economic owners.
            with closing(sqlite3.connect(f.monitor_config.path)) as conn:
                conn.execute('CREATE TABLE corrupt_extra (x INTEGER)'); conn.commit()
            before=f.repo.consumer_snapshot()
            f.sample(NOW+17)
            check('alert_store_failure_visible_and_held',f.monitor.last.entry_held
                and f.monitor.last.unavailable_code=='OPERATIONS_ALERT_STORE_UNAVAILABLE')
            check('alert_store_failure_keeps_original_protective_work',f.step(NOW+17).work=='NEED_EXECUTION'
                and f.repo.consumer_snapshot()['positions']==before['positions'])
        finally:
            close(f)


def supervisor_alerts(directory):
    f=Fixture(directory,'c2-supervisor',restart_profile=RestartProfile(1,1000000,0))
    try:
        f.start()
        configuration=dict(operations_path=f.operations_path,ledger_path=f.path,domain=f.domain,
            expected_identity=f.expected,degradation_config=f.monitor_config,**a2.c4.cold_args(f))
        sup=OperationsSupervisor(configuration,dict,HealthProfile(100,100,100))
        facts=sup.poll(now_us=(NOW+5)*1000000,monotonic_us=1)
        check('actual_supervisor_existing_owner_alert_without_child',facts.state=='HELD_EXISTING_OWNER'
            and any(row.condition=='OWNER_FENCE_UNPROVEN' for row in sup.alert_snapshot().conditions)
            and sup.process is None)
        stale=f.monitor
        generation=f.operations.snapshot()['generation']
        try:
            f.operations.acquire('synthetic-denied-replacement',now_us=(NOW+5)*1000000,replace_generation=generation)
        except RuntimeError:
            pass
        else:
            raise AssertionError('original restart exhaustion expected')
        exhausted=OperationsSupervisor(configuration,dict,HealthProfile(100,100,100))
        check('actual_supervisor_restart_exhaustion_alert',exhausted.poll(now_us=(NOW+6)*1000000,monotonic_us=2).state=='RESTART_EXHAUSTED'
            and any(row.condition=='RESTART_EXHAUSTED' for row in exhausted.alert_snapshot().conditions))
        stopped=exhausted.operator_stop(now_us=(NOW+7)*1000000)
        check('actual_supervisor_stop_alert_without_child',stopped.state=='OPERATOR_STOPPED'
            and any(row.condition=='OPERATOR_STOPPED' for row in exhausted.alert_snapshot().conditions))
        f.now=NOW+8
        stale.observe(rt.a3.clock(f.repo,NOW+8),resources=f.host)
        check('stale_owner_cannot_recover_control_incidents',all(row.state=='ACTIVE' for row in
            stale.snapshot().conditions if row.condition in ('OWNER_FENCE_UNPROVEN','RESTART_EXHAUSTED','OPERATOR_STOPPED')))
        with patch.object(f.operations,'snapshot',side_effect=OSError('https://private.invalid/TOKEN')):
            from live.operations_degradation_monitor_v0_1 import observe_supervisor
            unavailable=observe_supervisor(configuration,f.operations,stopped,now_us=(NOW+9)*1000000)
            check('unreadable_controls_fail_closed_sanitized',unavailable.entry_held
                and any(row.condition=='SUPERVISOR_HELD' and row.alerted_us is not None for row in unavailable.conditions)
                and 'https://' not in json.dumps(asdict(unavailable)))
    finally:
        close(f)


def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--preflight',action='store_true'); parser.add_argument('--case',choices=('all','resources','buy','unknown','supervisor'),default='all'); args=parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='meme-live-c2-integration-') as temporary:
        directory=Path(temporary)
        if args.preflight or args.case=='all':
            clock_order(directory)
            preflight(directory)
        if not args.preflight:
            for name,run in (('resources',resources_and_source),('buy',prepared_buy),('buy',signed_unsent),('unknown',unknown_and_protection),('supervisor',supervisor_alerts)):
                if args.case in ('all',name):
                    run(directory)
    print(json.dumps({'check_count':len(CHECKS),'checks':CHECKS,'all_checks_true':all(CHECKS.values())},sort_keys=True))


if __name__=='__main__':
    main()
