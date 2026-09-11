"""C2 final narrow status and one real supervised child; synthetic I/O only."""
from __future__ import annotations
import json
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_degradation_integration_selftest_v0_1 as c2
from live.operations_degradation_status_v0_1 import recorded_status
from live.operations_degradation_monitor_v0_1 import HostMetric, HostObservations, content_fingerprint
from live.operations_supervisor_v0_1 import OperationsSupervisor, HealthProfile

CHECKS = {}
def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)

def wait_file(path):
    end = time.monotonic()+15
    while not path.exists():
        if time.monotonic() > end:
            raise AssertionError('fixture rendezvous timeout')
        time.sleep(.005)

@dataclass
class Timeline:
    origin_ns: int
    def now_us(self):
        # A shared synthetic epoch with genuinely independently read elapsed
        # monotonic time. No scheduling-based timestamp adjustment is permitted.
        return (c2.NOW+6)*1000000+(time.monotonic_ns()-self.origin_ns)//1000

@dataclass
class Inputs:
    directory: Path
    timeline: Timeline
    calls: int = 0
    def __call__(self, started):
        self.calls += 1
        if self.calls == 1 and (self.directory/'diagnostic-enabled').exists():
            import re
            def trace(frame,event,arg):
                if event == 'exception' and 'operations_degradation' in frame.f_code.co_filename:
                    kind,value,_ = arg
                    code=str(value)
                    if re.fullmatch('[A-Z][A-Z0-9_]+',code):
                        with (self.directory/'diagnostic-codes.jsonl').open('a') as output:
                            output.write(json.dumps({'function':frame.f_code.co_name,'type':kind.__name__,'code':code})+'\n')
                return trace
            sys.settrace(trace)
        step = self.calls
        # Parent controls only fixture external-resource delivery, not Runtime.
        (self.directory/('ready-'+str(step))).write_text('ready')
        wait_file(self.directory/('run-'+str(step)))
        last = []
        def clock():
            at = self.timeline.now_us()
            base = c2.rt.a3.clock(started.runtime.ledger, c2.NOW+6)
            stamp = datetime.fromtimestamp(at/1000000, timezone.utc).isoformat(timespec='microseconds')
            sample = replace(base, utc_lower_utc=stamp, utc_upper_utc=stamp,
                monotonic_ns=(at-c2.NOW*1000000)*1000,
                observation_record_digest=content_fingerprint(('independent-child-sample', at)))
            last[:] = [at]
            if step == 2 and not (self.directory/'captured').exists():
                (self.directory/'captured').write_text(str(at))
                wait_file(self.directory/'release-captured')
            return sample
        def host():
            config = started.runtime._operations_degradation.configuration
            metrics = tuple(HostMetric(metric,
                10**12+1 if metric == 'HOST_RSS_BYTES' and step in (2,3) else 100,
                last[0], config.protective_qualification_digest if metric == 'PROTECTIVE_STEP_US' else c2.D(93))
                for metric in ('HOST_RSS_BYTES','HOST_DISK_RESERVE_BYTES','STARTUP_US','PROTECTIVE_STEP_US'))
            return HostObservations(config.host_identity_digest, config.policy.reviewed_configuration_digest,
                content_fingerprint(asdict(started.runtime._ownership.fence)), metrics)
        return dict(clock=clock, source_cut_utc=c2.rt.a3.utc(c2.NOW+1), operations_resources=host)

def receipts(path):
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True)) as conn:
        return conn.execute('SELECT count(*) FROM receipts').fetchone()[0]

def ordering():
    from live.operations_degradation_v0_1 import ConditionEvidence, DegradationStore, CONDITIONS
    with tempfile.TemporaryDirectory(prefix='c2-watermark-') as tmp:
        f=c2.Fixture(Path(tmp),'watermark')
        try:
            store=DegradationStore(f.monitor_config.path,f.domain,f.monitor_config.policy)
            store.advance(now_us=1000)  # Independently captured later parent poll.
            initial=ConditionEvidence('SEND_UNKNOWN',c2.D(800),900,c2.D(801))
            first=store.record(initial,now_us=900,coalesce_active=True).conditions[0]
            check('late_active_preserves_actual_first_observation',first.first_us==900 and first.observed_us==900 and first.last_us==900)
            check('parent_watermark_escalates_unknown_without_release',first.alerted_us==1000 and store.snapshot().entry_held)
            count=receipts(f.monitor_config.path)
            store.record(ConditionEvidence('SEND_UNKNOWN',c2.D(800),950,c2.D(802)),now_us=950,coalesce_active=True)
            row=store.snapshot().conditions[0]
            check('coalesce_preserves_receipt_first_seen_latest_actual_sample',receipts(f.monitor_config.path)==count and row.first_us==900 and row.last_us==950 and row.active_digest==first.active_digest)
            def recovery(at):
                return ConditionEvidence(row.condition,row.subject_digest,at,c2.D(803),'RECOVERED',CONDITIONS[row.condition][1],row.active_digest)
            try:
                store.record(recovery(940),now_us=940)
                rejected=False
            except (ValueError,RuntimeError): rejected=True
            check('older_than_latest_active_recovery_rejected',rejected)
            try:
                store.record(recovery(980),now_us=970)
                rejected=False
            except (ValueError,RuntimeError): rejected=True
            check('watermark_does_not_authorize_future_evidence',rejected)
            other=ConditionEvidence('SEND_UNKNOWN',c2.D(810),910,c2.D(811))
            other_row=next(item for item in store.record(other,now_us=910).conditions if item.subject_digest==c2.D(810))
            late=ConditionEvidence(other_row.condition,other_row.subject_digest,980,c2.D(812),'RECOVERED',CONDITIONS[other_row.condition][1],other_row.active_digest)
            resolved=next(item for item in store.record(late,now_us=980).conditions if item.subject_digest==c2.D(810))
            check('fresh_recovery_before_parent_watermark_keeps_real_time',resolved.recovered_us==980 and resolved.observed_us==980)
            store.advance(now_us=1001000)
            try:
                store.record(recovery(980),now_us=980)
                rejected=False
            except (ValueError,RuntimeError): rejected=True
            check('recovery_age_measured_against_ingestion_watermark',rejected)
            # A new genuinely fresh sample is after both latest active and watermark.
            recovered=store.record(recovery(1001001),now_us=1001001).conditions[0]
            check('fresh_recovery_keeps_real_time',recovered.recovered_us==1001001 and recovered.observed_us==1001001)
            try:
                store.record(ConditionEvidence(row.condition,row.subject_digest,960,c2.D(804)),now_us=960,coalesce_active=True)
                rejected=False
            except (ValueError,RuntimeError): rejected=True
            check('late_older_active_cannot_resurrect_recovered_episode',rejected)
            check('reopen_preserves_watermark_episode',DegradationStore(f.monitor_config.path,f.domain,f.monitor_config.policy).snapshot()==store.snapshot())
        finally: c2.close(f)


def contention():
    with tempfile.TemporaryDirectory(prefix='c2-contention-') as tmp:
        f=c2.Fixture(Path(tmp),'contention')
        try:
            f.start()
            with closing(sqlite3.connect(f.monitor_config.path,isolation_level=None)) as conn:
                conn.execute('BEGIN EXCLUSIVE')
                start=time.monotonic()
                result=f.step(c2.NOW+4)
                elapsed=time.monotonic()-start
                check('persistent_alert_lock_holds_entry',result.work=='OPERATIONS_ENTRY_HELD' and f.monitor.last.unavailable_code is not None)
                check('one_contention_allowance_for_whole_runtime_step',.8 <= elapsed < 1.8)
                print(json.dumps({'locked_step_seconds':elapsed}),flush=True)
                conn.rollback()
            check('next_step_resamples_after_lock_release',f.step(c2.NOW+5).work=='NEED_ENTRY_FACTS' and f.monitor.last.unavailable_code is None)
        finally: c2.close(f)


def main():
    with tempfile.TemporaryDirectory(prefix='c2-child-status-') as tmp:
        directory=Path(tmp)
        f=c2.Fixture(directory,'composed')
        if '--diagnostic' in sys.argv: (directory/'diagnostic-enabled').write_text('enabled')
        config=dict(operations_path=f.operations_path,ledger_path=f.path,domain=f.domain,
            expected_identity=f.expected,degradation_config=f.monitor_config,**c2.a2.c4.cold_args(f))
        c2.close(f)
        timeline=Timeline(time.monotonic_ns())
        sup=OperationsSupervisor(config,Inputs(directory,timeline),HealthProfile(20000000,20000000,1000000))
        def poll():
            return sup.poll(now_us=timeline.now_us(),monotonic_us=time.monotonic_ns()//1000)
        def until(predicate):
            end=time.monotonic()+20
            while time.monotonic()<end:
                facts=poll()
                if predicate(facts): return facts
                if facts.state.startswith('HELD'):
                    raise AssertionError(facts.state)
                time.sleep(.01)
            raise AssertionError('bounded child timeout')
        def status():
            return recorded_status(f.monitor_config,f.domain,f.expected)
        try:
            until(lambda facts:(directory/'ready-1').exists())
            (directory/'run-1').write_text('go')
            first=until(lambda facts:facts.completed_steps==1)
            print(json.dumps({'first_work':first.last_work,'first_status':status(),'parent_alerts':asdict(sup.alert_snapshot())}),flush=True)
            check('spawned_original_startup_runtime_waiting',first.last_work=='NEED_ENTRY_FACTS')
            check('ordinary_waiting_no_incident',status()['conditions']==[] and status()['unavailable_code'] is None)
            (directory/'run-2').write_text('go')
            wait_file(directory/'captured')
            captured=int((directory/'captured').read_text())
            poll()
            with closing(sqlite3.connect(f.monitor_config.path)) as conn:
                watermark=conn.execute('SELECT last_us FROM metadata').fetchone()[0]
            check('independent_parent_sample_later_than_captured_child',watermark>captured)
            (directory/'release-captured').write_text('go')
            second=until(lambda facts:facts.completed_steps==2)
            view=status()
            print(json.dumps({'second_work':second.last_work,'second_status':view,'supervisor_alerts':asdict(sup.alert_snapshot())},sort_keys=True),flush=True)
            active=[row for row in view['conditions'] if row['condition']=='RESOURCE_EXCEEDED' and row['state']=='ACTIVE']
            check('child_resource_condition_survives_parent_poll',second.last_work=='OPERATIONS_ENTRY_HELD' and len(active)==1)
            check('resource_status_exact_metric_limit',active[0]['metric']=='HOST_RSS_BYTES' and active[0]['configured_limit']==10**12 and active[0]['limit_direction']=='MAXIMUM')
            before=receipts(f.monitor_config.path)
            (directory/'run-3').write_text('go')
            until(lambda facts:facts.completed_steps==3)
            after=receipts(f.monitor_config.path)
            print(json.dumps({'receipts_before':before,'receipts_after':after}),flush=True)
            check('unchanged_active_no_per_poll_receipt',before==after)
            (directory/'run-4').write_text('go')
            fourth=until(lambda facts:facts.completed_steps==4)
            recovered=status()
            check('positive_child_recovery_visible',fourth.last_work=='NEED_ENTRY_FACTS' and not recovered['entry_held']
                and any(row['state']=='RECOVERED' and row['recovered_us'] is not None for row in recovered['conditions']))
            check('parent_alert_consumer_sees_recovery',sup.alert_snapshot().unavailable_code is None and not sup.alert_snapshot().entry_held)
            check('status_historical_never_permission',recovered['view']=='HISTORICAL_RECORDED_ALERTS' and all(recovered[key] is False for key in ('grants_permission','may_sign','may_send')))
            check('sanitized_status_no_paths_provider_payload',all(value not in json.dumps(recovered) for value in (str(directory),'SYNTHETIC_TRUSTED_CLOCK','https://','sample_digest')))
            invalid=recorded_status(replace(f.monitor_config,path='https://private.invalid/secret'),f.domain,f.expected)
            check('invalid_status_fixed_unavailable_no_input_echo',invalid['unavailable_code']=='OPERATIONS_ALERT_STATUS_UNAVAILABLE' and 'private' not in json.dumps(invalid))
        finally:
            if (directory/'diagnostic-codes.jsonl').exists():
                print((directory/'diagnostic-codes.jsonl').read_text(),flush=True)
            if sup.process is not None:
                if sup.process.exitcode is None: sup.process.terminate()
                sup.process.join(5)
                check('exact_retained_disposable_child_cleanup',not sup.process.is_alive())
                sup.process.close()
            if sup._channel is not None: sup._channel.close()
    print(json.dumps({'checks':CHECKS,'count':len(CHECKS)},sort_keys=True))

if __name__=='__main__':
    if '--contention' in sys.argv:
        contention()
        print(json.dumps({'checks':CHECKS,'count':len(CHECKS)},sort_keys=True))
    elif '--ordering' in sys.argv:
        ordering()
        print(json.dumps({'checks':CHECKS,'count':len(CHECKS)},sort_keys=True))
    else:
        main()
