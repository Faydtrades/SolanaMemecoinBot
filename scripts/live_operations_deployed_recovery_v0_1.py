"""Bounded M53 profile binding; copied retained economics, no execution ports.

Real Windows RSS pressure is at most 64 MiB of touched observer allocation.
Comparator/freshness fault inputs are labeled; no low-disk workload is claimed.
"""
from __future__ import annotations
import argparse
import gc
import json
import shutil
import sys
import time
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_deployed_measurement_v0_1 as a
from live_operations_measurement_v0_1 import _process_memory
from live import operations_degradation_monitor_v0_1 as monitor
from live.operations_degradation_v0_1 import (
    REQUIRED_METRICS, METRICS, ResourceLimit, DegradationPolicy,
    DegradationStore, ConditionEvidence, resource_condition, CONDITIONS)
from live.ledger_actions_v0_1 import utc_microseconds
from live.operations_degradation_status_v0_1 import recorded_status
from phase5.shadow_domain_v0_1 import content_fingerprint as digest

PROFILE = Path(r'D:\Tradingbot\meme_live\evidence\step11a-3630b2\m52-profile-02\profile.json')
PROFILE_SHA = 'fc924b5d572211b82a889b6933e9fb191bea9c9e9e2e82c95d2fd7a89fdc6440'
PROFILE_DIGEST = '09457b947def64b00e924945188c2710dac35ada379ec87b01fedfe73deea18d'
ADDITIONS = {'RETAINED_EVENTS': 'retained_events', 'HOTTEST_EVENTS': 'hottest_events',
    'RETAINED_SERIALIZED_BYTES': 'retained_serialized_bytes',
    'PRODUCER_PENDING_BYTES': 'pending_serialized_bytes',
    'HISTORY_BYTES': 'history_bytes', 'CHECKPOINT_BYTES': 'checkpoint_bytes'}


def timed(operation):
    before = time.perf_counter()
    result = operation()
    return result, time.perf_counter()-before


def run(out, protective_limit_us=None):
    assert a.sha(PROFILE) == PROFILE_SHA
    profile = a.read(PROFILE)
    assert profile['content_digest'] == PROFILE_DIGEST
    assert digest({k:v for k,v in profile.items() if k != 'content_digest'}) == PROFILE_DIGEST
    assert {k:v['value'] for k,v in profile['intervals'].items()}==dict(
        resource_max_age_us=1000000,recovery_evidence_max_age_us=5000000,persistent_unknown_alert_us=60000000)
    measured_path = Path(profile['measurement']['path'])
    assert a.sha(measured_path) == profile['measurement']['sha256']
    measured = a.read(measured_path)
    m = measured['measurement']
    out = out.resolve()
    assert out.parent == PROFILE.parent.parent and not out.exists()
    out.mkdir()
    copied = out/'retained-synthetic-c1'
    copied.mkdir()
    fixture_ref = a.read(a.PROFILE_FILE)['evidence']['C1.3']['fixture_hashes.json']
    assert a.sha(fixture_ref['path']) == measured['accepted_fixture']['manifest_sha256']
    originals = a.read(fixture_ref['path'])
    for relative, expected in originals.items():
        source = a.C1/relative
        payload = source.read_bytes()
        assert a.hashlib.sha256(payload).hexdigest() == expected
        (copied/source.name).write_bytes(payload)
    domain_record = dict(m['fixture_domain'])
    domain_record['expected_empty_token_accounts'] = tuple(domain_record['expected_empty_token_accounts'])
    domain = a.LedgerDomain(**domain_record)
    assert domain.wallet != profile['target']['domain']['wallet']
    binding_record = dict(m['fixture_source_binding'])
    binding_record['anchors'] = tuple(a.CursorWitness(**x) for x in binding_record['anchors'])
    binding = a.SourceBinding(**binding_record)
    source_profile = a.SourceProfile(**m['fixture_source_profile'])
    producer_profile = a.ContinuationProfileV02(**m['fixture_profile'])
    market = a.ContinuousMarketSourceV02(copied/'fixture-raw.sqlite', start_after_p1_rowid=1,
        database_identity='A3:SYNTHETIC:RETAINED:COLLECTOR')
    config = dict(operations_path=copied/'fixture-operations.sqlite', ledger_path=copied/'fixture-ledger.sqlite',
        producer_path=copied/'fixture-producer.sqlite', market_source=market, producer_profile=producer_profile,
        source_path=copied/'fixture-evidence.sqlite', source_binding=binding, source_profile=source_profile,
        database_identity=market.database_identity)
    controls = a.OperationsStore(config['operations_path'], domain)
    control = controls.snapshot()
    identity = a.startup.configured_identity(domain, **config,
        producer_schema_digest=m['current_startup_identity']['producer_schema_digest'],
        evidence_schema_digest=m['current_startup_identity']['evidence_schema_digest'],
        restart_profile=a.startup.ownership.RestartProfile(*(control[k] for k in ('max_attempts','window_us','backoff_us'))))
    limits = {key:item['value'] for key,item in profile['thresholds'].items()}
    assert set(limits) == REQUIRED_METRICS
    limits.update({key:profile['observed_supported_workload'][source] for key,source in ADDITIONS.items()})
    assert set(limits) == METRICS
    amendments = []
    if protective_limit_us is not None:
        assert protective_limit_us==2000000, 'Only the explicitly reviewed B guard amendment is supported'
        prior_b_path=PROFILE.parent.parent/'m53-recovery-02'/'result.json'
        prior_b=a.read(prior_b_path)
        assert prior_b['timings']['pressure_monitored_protective_seconds']==1.4911294000339694
        amendments.append(dict(metric='PROTECTIVE_STEP_US',old=limits['PROTECTIVE_STEP_US'],new=protective_limit_us,
            role='CALIBRATED_POLICY_GUARD',evidence_path=str(prior_b_path),evidence_sha256=a.sha(prior_b_path),
            measured_seconds=prior_b['timings']['pressure_monitored_protective_seconds'],
            rationale='Project-reviewed B amendment: 2s guard above observed1.4911294s full monitored protective unit (~34% margin). No worst-case, capacity or preemption guarantee. A remains immutable.'))
        limits['PROTECTIVE_STEP_US']=protective_limit_us
    extension = dict(schema='MEME_LIVE_M53_PROFILE_EXTENSION_V1', parent_profile_sha256=PROFILE_SHA,
        parent_profile_content_digest=PROFILE_DIGEST, added_measured_limits={k:limits[k] for k in ADDITIONS},
        limits=limits, explicit_policy_amendments=amendments, host_identity_digest=profile['host']['identity_digest'],
        original_constructor_profile=asdict(producer_profile), fixture_startup_identity=asdict(identity),
        canonical_startup_identity=profile['canonical_startup_identity'],
        changed_monitor_code={str(Path(module.__file__).relative_to(ROOT)):a.sha(module.__file__)
            for module in (monitor, monitor.condition_contract)},
        scope='Qualification only. Added limits bind unchanged A measurements; unchanged constructor caps are not capacity. Current monitor code measured here. Real target wallet remains distinct from copied economic domain.',
        activation=False, entry_permission=False)
    extension_digest = digest(extension)
    extension['content_digest'] = extension_digest
    a.write(out/'profile-extension.json', extension)
    def configuration(path, selected=limits, startup_identity=identity):
        reviewed = monitor.monitor_configuration_digest(startup_identity, path=str(path),
            host_identity_digest=profile['host']['identity_digest'], resource_max_age_us=1000000,
            protective_qualification_digest=extension_digest)
        policy = DegradationPolicy(reviewed, 60000000, 5000000,
            tuple(ResourceLimit(k,v) for k,v in sorted(selected.items())))
        return monitor.MonitorConfiguration(str(path), policy, profile['host']['identity_digest'],
            startup_identity.runtime_code_digest, 1000000, extension_digest)
    mc = configuration(out/'alerts.sqlite')
    a.write(out/'monitor-configuration.json', asdict(mc))
    canonical_fields=dict(profile['canonical_startup_identity'])
    canonical_fields['restart_profile']=a.startup.ownership.RestartProfile(**canonical_fields['restart_profile'])
    canonical_fields['contracts']=tuple(tuple(x) for x in canonical_fields['contracts'])
    canonical_identity=a.startup.StartupIdentity(**canonical_fields)
    canonical_path=a.read(PROFILE.parent/'monitor-configuration-candidate.json')['monitor']['path']
    canonical_monitor=configuration(canonical_path,startup_identity=canonical_identity)
    assert canonical_monitor.binding_for(canonical_identity)==canonical_monitor.policy.reviewed_configuration_digest
    a.write(out/'canonical-monitor-configuration-candidate.json',dict(monitor=asdict(canonical_monitor),
        parent_profile_content_digest=PROFILE_DIGEST,extension_content_digest=extension_digest,activation=False,
        scope='Typed constructor binding only to unchanged canonical A identity/paths plus current monitor hashes and explicit B profile extension. No canonical store initialization or runtime activation.'))
    DegradationStore.initialize(mc.path, domain, mc.policy, now_us=0)
    started = None
    result = dict(profile_content_digest=PROFILE_DIGEST, extension_content_digest=extension_digest,
        script_sha256=a.sha(__file__), observed_utc=datetime.now(timezone.utc).isoformat(), checks={}, timings={},
        fault_input_scope='Only comparator/freshness contract cases use external values. RSS and D-volume free bytes are actual point samples. No low-disk workload, economic state fabrication or maximum-capacity claim.')
    checks, timings = result['checks'], result['timings']
    def save():
        a.write(out/'result.json', result)
    try:
        started, timings['monitored_startup_seconds'] = timed(lambda: a.startup.start_live(domain=domain,
            **config, expected_identity=identity, process_identity='M53:ISOLATED:REOPEN',
            now_us=control['last_control_us']+1000001, replace_generation=control['generation'], degradation_config=mc))
        owner_startup_seconds = timings['monitored_startup_seconds']
        runtime = started.runtime
        mon = runtime._operations_degradation
        assert mon.store is not None
        prior = runtime.ledger._authority.last_qualified_clock
        assert prior is not None and prior.reconciliation is None
        sequence = 0
        current = prior
        clock_origin = time.perf_counter()
        host_acquired_perf = None
        sampled_host = None
        def clock():
            nonlocal sequence, current, host_acquired_perf, sampled_host
            sequence += 1
            rss, peak = _process_memory()
            sampled_host = dict(HOST_RSS_BYTES=rss.value, HOST_DISK_RESERVE_BYTES=shutil.disk_usage(out).free,
                peak_rss_bytes=peak.value)
            host_acquired_perf = time.perf_counter()
            delta = 10000000+int((host_acquired_perf-clock_origin)*1000000)
            utc = (datetime.fromisoformat(prior.utc_upper_utc)+timedelta(microseconds=delta)).isoformat()
            current = replace(prior, monotonic_ns=prior.monotonic_ns+delta*1000,
                utc_lower_utc=utc, utc_upper_utc=utc, previous_sample_digest=prior.content_digest)
            return current
        host_samples = []
        protective_us = round(profile['measurement']['protective_seconds']*1000000)
        def resources(*, sample_scope='operations_monitor'):
            values = dict(HOST_RSS_BYTES=sampled_host['HOST_RSS_BYTES'], HOST_DISK_RESERVE_BYTES=sampled_host['HOST_DISK_RESERVE_BYTES'],
                STARTUP_US=round(owner_startup_seconds*1000000), PROTECTIVE_STEP_US=protective_us)
            host_samples.append(dict(values, peak_rss_bytes=sampled_host['peak_rss_bytes'],
                sample_scope=sample_scope,
                actual_acquisition_to_consumption_seconds=time.perf_counter()-host_acquired_perf,
                clock_scope='Synthetic retained economic UTC/monotonic origin plus actual perf_counter elapsed; host facts acquired at clock sample, consumed later without timestamp refresh.'))
            return monitor.HostObservations(mc.host_identity_digest, mc.policy.reviewed_configuration_digest,
                digest(asdict(runtime._ownership.fence)), tuple(monitor.HostMetric(k,v,
                utc_microseconds(current.utc_upper_utc), extension_digest if k=='PROTECTIVE_STEP_US' else digest((k,v,sequence)))
                for k,v in sorted(values.items())))
        before = runtime.reconstruction_facts()
        owner_cache = (dict(runtime.producer.metrics), dict(runtime.producer.last_profile_usage), runtime.producer.conn.total_changes)
        fact_metrics, manifest, checkpoint = monitor._producer_cut(runtime.producer)
        checks['six_exact_measurements'] = all(fact_metrics[k] == limits[k] for k in ADDITIONS)
        checks['observer_no_producer_mutation'] = owner_cache == (dict(runtime.producer.metrics), dict(runtime.producer.last_profile_usage), runtime.producer.conn.total_changes)
        result['exact_producer_metrics'] = fact_metrics
        result['retained_checkpoint'] = dict(generation=manifest['generation'], digest=checkpoint)
        baseline, timings['baseline_observation_seconds'] = timed(lambda: mon.observe(clock(), resources=resources))
        result['baseline_alerts'] = asdict(baseline)
        # Original 14-metric coverage remains usable with no added requirements.
        old_mc = configuration(out/'old14-alerts.sqlite', {k:limits[k] for k in REQUIRED_METRICS})
        DegradationStore.initialize(old_mc.path, domain, old_mc.policy, now_us=0)
        old_mon = monitor.OperationsMonitor(started, old_mc, source_binding=binding,
            source_profile=source_profile, producer_profile=producer_profile)
        base_resources = resources(sample_scope='legacy_external_sample_construction')
        old_host = replace(base_resources, configuration_digest=old_mc.policy.reviewed_configuration_digest)
        old_view = old_mon.observe(clock(), resources=lambda:old_host)
        old_coverage=monitor._subject(domain,'PROFILE_COVERAGE',old_mc.policy.content_digest)
        checks['old14_policy_coverage_unchanged'] = not any(row.condition=='PROFILE_UNRESOLVED' and row.state=='ACTIVE' and row.subject_digest==old_coverage for row in old_view.conditions) and len(old_mon.last_metrics)==14
        # Finite threshold comparison qualification; deliberately external values.
        boundaries = {}
        for metric, limit in sorted(limits.items()):
            values = (limit, limit-1 if metric=='HOST_DISK_RESERVE_BYTES' else limit+1)
            answers = [resource_condition(mc.policy, metric, value,
                configuration_digest=mc.policy.reviewed_configuration_digest) for value in values]
            boundaries[metric] = dict(values=values, conditions=answers)
            assert answers == [None, 'RESOURCE_EXCEEDED']
        result['external_comparator_boundaries'] = boundaries
        checks['all20_exact_boundary_comparisons'] = True
        # Actual modest same-process RSS crossing; touch every byte, cap 64 MiB.
        allocations = []
        while _process_memory()[0].value <= limits['HOST_RSS_BYTES'] and len(allocations)<64:
            allocations.append(bytearray(b'X'*(1024*1024)))
        pressure_rss = _process_memory()[0].value
        assert pressure_rss > limits['HOST_RSS_BYTES']
        result['pressure'] = dict(allocated_bytes=len(allocations)*1024*1024, measured_rss_bytes=pressure_rss)
        pressure, timings['pressure_observation_seconds'] = timed(lambda: mon.observe(clock(), resources=resources))
        rss_subject = monitor._subject(domain, 'RESOURCE', (mc.policy.content_digest, 'HOST_RSS_BYTES'))
        rss_rows = lambda view: [row for row in view.conditions if row.condition=='RESOURCE_EXCEEDED' and row.subject_digest==rss_subject]
        assert rss_rows(pressure)[0].state=='ACTIVE' and pressure.entry_held
        checks['actual_rss_crossing_durable_entry_hold'] = True
        checks['original_runtime_entry_gate_held'] = runtime._entry_degradation(clock, None, resources)
        work, timings['pressure_monitored_protective_seconds'] = timed(lambda: runtime.step(clock=clock, operations_resources=resources))
        result['pressure_alerts'], result['protective_work'] = asdict(mon.snapshot()), asdict(work)
        checks['protective_work_available'] = work.work=='NEED_EXECUTION'
        # Bind actual monitored timing evidence on subsequent observations.
        protective_us = round(timings['pressure_monitored_protective_seconds']*1000000)
        del allocations
        gc.collect()
        restored_rss = _process_memory()[0].value
        result['restored_rss_bytes'] = restored_rss
        # Reopen original owner/stores before submitting any restored facts.
        old_rss = asdict(rss_rows(mon.snapshot())[0])
        started.close()
        started = None
        control = controls.snapshot()
        started, timings['restart_monitored_startup_seconds'] = timed(lambda: a.startup.start_live(domain=domain,
            **config, expected_identity=identity, process_identity='M53:ISOLATED:RESTART',
            now_us=control['last_control_us']+1000001, replace_generation=control['generation'], degradation_config=mc))
        owner_startup_seconds = timings['restart_monitored_startup_seconds']
        runtime, mon = started.runtime, started.runtime._operations_degradation
        restarted = mon.snapshot()
        checks['restart_alone_retains_condition'] = asdict(rss_rows(restarted)[0]) == old_rss
        result['restart_alerts_before_observation'] = asdict(restarted)
        restored, timings['restored_fact_recovery_seconds'] = timed(lambda: mon.observe(clock(), resources=resources))
        result['restored_alerts'] = asdict(restored)
        checks['measured_rss_restored_recovery'] = rss_rows(restored)[0].state=='RECOVERED' and restored_rss <= limits['HOST_RSS_BYTES']
        after = runtime.reconstruction_facts()
        checks['economic_obligations_retained'] = before.remaining_units == after.remaining_units > 0 and before.consumer_cut == after.consumer_cut
        metrics_after, manifest_after, checkpoint_after = monitor._producer_cut(runtime.producer)
        retention_keys = ('state_digest','lineage','source','anchor','cursor','next_event_sequence','launches','profile')
        checks['retained_producer_unchanged'] = all(manifest_after[k]==manifest[k] for k in retention_keys) and metrics_after==fact_metrics
        result['checkpoint_after_restart'] = dict(generation=manifest_after['generation'],digest=checkpoint_after,
            state_digest_unchanged=manifest_after['state_digest']==manifest['state_digest'],
            scope='Original cold reopen publishes a checkpoint generation; retained state/lineage/cursor/profile and measured usage remain exact.')
        result['original_recorded_status_after_recovery'] = recorded_status(mc,domain,identity)
        checks['status_maps_profile_limit'] = any(row['metric']=='HOST_RSS_BYTES' and row['configured_limit']==limits['HOST_RSS_BYTES'] and row['state']=='RECOVERED' for row in result['original_recorded_status_after_recovery']['conditions'])
        # Typed external host freshness edges, exact same current owner/config.
        clock()
        now = utc_microseconds(current.utc_upper_utc)
        observed = resources(sample_scope='external_freshness_predicate_fixture')
        owner_digest = digest(asdict(runtime._ownership.fence))
        fresh = {}
        for age in (1000000,1000001):
            values = replace(observed, metrics=tuple(replace(x, observed_us=now-age) for x in observed.metrics))
            mapped, _ = mon._host_metrics(lambda:values, now, owner_digest)
            fresh[str(age)] = {k:mapped[k] for k in ('HOST_RSS_BYTES','HOST_DISK_RESERVE_BYTES')}
        checks['resource_freshness_1s_edge'] = all(v is not None for v in fresh['1000000'].values()) and all(v is None for v in fresh['1000001'].values())
        result['external_host_freshness_boundaries'] = fresh
        # Source age is qualified from original immutable copied source facts.
        previous_source = runtime.source.latest_record()[1]
        source_record = monitor.CollectorSourceAdapter(market.db_path).observe(binding,source_profile,
            observed_at_utc=current.utc_upper_utc, requested_cut_utc=previous_source.snapshot.requested_cut_utc,
            previous=previous_source)
        receipt_utc = source_record.progress.last_live_receipt_utc
        assert receipt_utc is not None
        source_edges = {}
        for age in (29999999,30000000):
            utc = (datetime.fromisoformat(receipt_utc)+timedelta(microseconds=age)).isoformat()
            verdict = monitor.CollectorSourceAdapter(market.db_path).observe(binding, source_profile,
                observed_at_utc=utc, requested_cut_utc=source_record.snapshot.requested_cut_utc, previous=source_record)
            source_edges[str(age)] = dict(disposition=verdict.disposition, reasons=verdict.reasons)
        checks['source_freshness_30s_edge'] = 'LIVE_RECEIPT_STALE' not in source_edges['29999999']['reasons'] and 'LIVE_RECEIPT_STALE' in source_edges['30000000']['reasons']
        result['source_freshness_from_original_receipt'] = dict(receipt_utc=receipt_utc, observations=source_edges)
        # Resource-only external restoration receipt age; no economic incidents.
        recovery_edges = {}
        for age in (5000000,5000001):
            journal = DegradationStore.initialize(out/('recovery-age-'+str(age)+'.sqlite'), domain, mc.policy, now_us=0)
            subject = digest(('EXTERNAL_RESOURCE_FRESHNESS_BOUNDARY', extension_digest, age))
            active = ConditionEvidence('RESOURCE_EXCEEDED',subject,now,digest(('actual_pressure',pressure_rss)))
            with runtime._ownership.mutation_guard(domain):
                journal.record(active, now_us=now)
                restored_evidence = ConditionEvidence('RESOURCE_EXCEEDED',subject,now+1,digest(('actual_restored',restored_rss)),
                    'RECOVERED',CONDITIONS['RESOURCE_EXCEEDED'][1],active.content_digest)
                try:
                    journal.record(restored_evidence, now_us=now+1+age)
                    recovery_edges[str(age)] = 'RECOVERED'
                except ValueError as exc:
                    recovery_edges[str(age)] = str(exc)
        checks['recovery_evidence_5s_edge'] = recovery_edges=={'5000000':'RECOVERED','5000001':'OPERATIONS_DEGRADATION_POSITIVE_RECOVERY_REQUIRED'}
        result['external_recovery_evidence_age_boundaries'] = recovery_edges
        # Reuse the exact accepted C2 contract incident input, not Ledger state.
        accepted_unknown = ROOT/'scripts/live_operations_degradation_selftest_v0_1.py'
        assert a.sha(accepted_unknown)=='91dc54b19556b16b14afe417e2c4234fc093977eff2c20b85d47e98e3f8aade7'
        unknown_path = out/'accepted-incident-60s.sqlite'
        unknown = DegradationStore.initialize(unknown_path, domain, mc.policy, now_us=0)
        fixed = ConditionEvidence('SEND_UNKNOWN',format(3,'064x'),10,format(4,'064x'))
        with runtime._ownership.mutation_guard(domain):
            initial = unknown.record(fixed,now_us=10)
            early = unknown.advance(now_us=60000009)
            reopened = DegradationStore(unknown_path,domain,mc.policy)
            reopen_view = reopened.snapshot()
            exact = reopened.advance(now_us=60000010)
            later = reopened.advance(now_us=60000011)
        checks['accepted_incident_60s_boundary_and_reopen'] = (initial.entry_held and
            early.conditions[0].alerted==0 and reopen_view==early and
            exact.conditions[0].alerted_us==60000010 and later.entry_held and
            later.conditions[0].first_us==10 and later.conditions[0].state=='ACTIVE')
        result['accepted_incident_policy_binding'] = dict(input=asdict(fixed),
            source=str(accepted_unknown), source_sha256=a.sha(accepted_unknown),
            exact_boundary=asdict(exact), scope='Exact prior C2 contract condition input replayed under this profile policy in isolated incident journal. Not a newly created Ledger UNKNOWN or current wallet custody claim.',
            reused_integration_evidence=dict(path=r'C:\Users\Mari1\AppData\Local\Temp\meme-live-c2-evidence-4dd1e26\c2-3-integration-final.stdout.txt',sha256='992a6d9471830bb3ab562eef728c0d7a0b783c3e4fb0940c20483661a113df20',checks=['actual_mock_submission_unknown_incident','unknown_reopen_retains_age_and_reservation','original_exact_finality_recovers_unknown_alert_only']))
        result['host_samples'] = host_samples
        actual_ages=[item['actual_acquisition_to_consumption_seconds'] for item in host_samples if item['sample_scope']=='operations_monitor']
        result['maximum_actual_host_sample_age_seconds']=max(actual_ages)
        checks['actual_host_sample_age_within_1s']=max(actual_ages)<=1.0
        result['timing_fit'] = dict(startup=timings['monitored_startup_seconds']<=4,
            restart=timings['restart_monitored_startup_seconds']<=4,
            protective=timings['pressure_monitored_protective_seconds']*1000000<=limits['PROTECTIVE_STEP_US'],
            recovery_evidence=timings['restored_fact_recovery_seconds']<=5,
            scope='Observed perf_counter intervals only; no worst-case or preemption guarantee. Protective includes original pre/post monitor observations and no execution ports.')
        result['remaining_delta'] = []
        if not result['timing_fit']['protective']:
            result['remaining_delta'].append('Configured protective guard is exceeded by actual monitored protective work; no automatic relaxation. A remains immutable and original monitor retains this condition.')
        if not result['timing_fit']['startup'] or not result['timing_fit']['restart']:
            result['remaining_delta'].append('A STARTUP_US=4000000 exceeded in this current monitored run; no worst-case/startup guarantee.')
        if not checks['actual_host_sample_age_within_1s']:
            result['remaining_delta'].append('Actual monitor acquisition-to-consumption age exceeded unchanged1s bound; no timestamp refresh or automatic relaxation.')
        if not result['timing_fit']['recovery_evidence']:
            result['remaining_delta'].append('Actual restored-fact observation exceeded5s recovery evidence window.')
        result['status'] = 'IMPLEMENTED_PENDING_PROJECT_REVIEW'
        save()
        assert all(checks.values()), checks
    except BaseException as exc:
        result['error'] = type(exc).__name__+': '+str(exc)
        save()
        raise
    finally:
        if started is not None:
            started.close()
    print(json.dumps(dict(result=str(out/'result.json'), checks=checks, timings=timings,
        timing_fit=result['timing_fit'], extension_content_digest=extension_digest), sort_keys=True))


def audit_existing(run_path, out):
    """Read-only diagnosis of the preserved run03; never rerun its workload."""
    run_path=run_path.resolve(strict=True)
    out=out.resolve()
    assert run_path==PROFILE.parent.parent/'m53-recovery-03'
    assert out.parent==PROFILE.parent.parent and not out.exists()
    raw=a.read(run_path/'result.json')
    extension=a.read(run_path/'profile-extension.json')
    assert raw['profile_content_digest']==PROFILE_DIGEST
    assert raw['extension_content_digest']==extension['content_digest']
    assert digest({k:v for k,v in extension.items() if k!='content_digest'})==extension['content_digest']
    assert {k for k,v in raw['checks'].items() if not v}=={
        'old14_policy_coverage_unchanged','actual_host_sample_age_within_1s'}
    assert raw['error'].startswith('AssertionError:')
    assert all(a.sha(ROOT/path)==expected for path,expected in extension['changed_monitor_code'].items())
    fields=dict(extension['fixture_startup_identity'])
    fields['restart_profile']=a.startup.ownership.RestartProfile(**fields['restart_profile'])
    fields['contracts']=tuple(tuple(x) for x in fields['contracts'])
    identity=a.startup.StartupIdentity(**fields)
    m=a.read(a.read(PROFILE)['measurement']['path'])['measurement']
    domain_fields=dict(m['fixture_domain'])
    domain_fields['expected_empty_token_accounts']=tuple(domain_fields['expected_empty_token_accounts'])
    domain=a.LedgerDomain(**domain_fields)
    old_path=run_path/'old14-alerts.sqlite'
    reviewed=monitor.monitor_configuration_digest(identity,path=str(old_path),
        host_identity_digest=extension['host_identity_digest'],resource_max_age_us=1000000,
        protective_qualification_digest=extension['content_digest'])
    policy=DegradationPolicy(reviewed,60000000,5000000,
        tuple(ResourceLimit(k,extension['limits'][k]) for k in sorted(REQUIRED_METRICS)))
    with a.closing(a.ro(old_path,immutable=True)) as conn:
        assert conn.execute('PRAGMA quick_check').fetchall()==[('ok',)]
        assert conn.execute('SELECT policy_digest FROM metadata').fetchone()[0]==policy.content_digest
        rows=conn.execute('SELECT condition,subject_digest,state FROM conditions ORDER BY condition,subject_digest').fetchall()
    mapping={monitor._subject(domain,'RESOURCE',(policy.content_digest,k)):k for k in METRICS}
    coverage=monitor._subject(domain,'PROFILE_COVERAGE',policy.content_digest)
    mapped=[dict(condition=code,subject_digest=subject,state=state,metric=mapping.get(subject),
        profile_coverage=subject==coverage) for code,subject,state in rows]
    unresolved=[row for row in mapped if row['condition']=='PROFILE_UNRESOLVED' and row['state']=='ACTIVE']
    assert {row['metric'] for row in unresolved}=={'HOST_RSS_BYTES','HOST_DISK_RESERVE_BYTES'}
    assert not any(row['profile_coverage'] for row in unresolved)
    # Exact callsite order in the executed run03 source, not filtering by age.
    callsites=('baseline monitor.observe','legacy_external_sample_construction',
        'pressure monitor.observe','original Runtime ENTRY gate',
        'Runtime.step initial monitor','Runtime.step final monitor',
        'restored-fact monitor.observe','external_freshness_predicate_fixture')
    assert len(raw['host_samples'])==len(callsites)
    indices=(0,2,3,4,5,6)
    ages=[raw['host_samples'][i]['actual_acquisition_to_consumption_seconds'] for i in indices]
    assert max(ages)<=1
    prior_path=PROFILE.parent.parent/'m53-recovery-02'/'result.json'
    assert a.read(prior_path)['checks']['old14_policy_coverage_unchanged'] is True
    assert all(raw['timing_fit'][k] for k in ('startup','restart','protective','recovery_evidence'))
    diagnostics=dict(schema='M53_RUN03_TARGETED_DIAGNOSTIC_V1',status='IMPLEMENTED_PENDING_PROJECT_REVIEW',
        raw_run=dict(path=str(run_path/'result.json'),sha256=a.sha(run_path/'result.json'),exit=1,
            executed_harness_sha256=raw['script_sha256'],error=raw['error']),
        profile_content_digest=PROFILE_DIGEST,extension_content_digest=extension['content_digest'],
        raw_results_unchanged=True,workload_reruns=0,
        old14=dict(journal=str(old_path),policy_digest=policy.content_digest,conditions=mapped,
            finding='Original14 coverage intact; manually reused2.0977s-old host sample correctly created only RSS/disk unresolved subjects. The old assertion incorrectly equated any unresolved host metric with missing coverage.',
            prior_healthy_original14=dict(path=str(prior_path),sha256=a.sha(prior_path),check=True)),
        host_sample_sequence=[dict(index=i,callsite=callsite,
            age_seconds=raw['host_samples'][i]['actual_acquisition_to_consumption_seconds'],
            actual_monitor_callback=i in indices) for i,callsite in enumerate(callsites)],
        maximum_actual_monitor_sample_age_seconds=max(ages),
        original_interval_us=1000000,
        sampling_scope='Samples acquired at each clock call; economic UTC/monotonic advanced with actual perf_counter elapsed. Actual monitor consumption ages are measured independently of frozen read-cut comparisons. This finite observed fit does not preempt calls or promise future freshness.',
        assertion_corrections=dict(coverage='Check exact PROFILE_COVERAGE subject; individual stale metrics remain held.',
            age='Tag callsites at collection; aggregate only actual monitor callbacks. External compatibility/predicate fixture assembly has distinct scope.'),
        changed_harness_sha256=a.sha(__file__),changed_monitor_code=extension['changed_monitor_code'],
        targeted_checks=True,timing_fit=raw['timing_fit'],timings=raw['timings'],
        remaining_delta=[],recommendation='M53_VERIFIED_CANDIDATE',
        qualification_limits=['Finite retained synthetic domain only; canonical LIVE stores remain uninitialized and no ENTRY permission is granted.',
            'Approved B2s protective guard supersedes A provisional100ms only; A and failed100ms evidence remain immutable.',
            'Other numerical guard crossings are explicitly external comparator evidence; physical pressure only exercised bounded RSS, not low disk or every workload shape.',
            'Resource restoration clears only the measured resource incident. Original protective action missing external ports still holds ENTRY.',
            'UNKNOWN60s test binds exact accepted incident input in isolated incident journal; accepted integration owner retention is reused, no new Ledger UNKNOWN.'])
    out.mkdir()
    a.write(out/'diagnostic.json',diagnostics)
    print(json.dumps(dict(diagnostic=str(out/'diagnostic.json'),sha256=a.sha(out/'diagnostic.json'),
        maximum_actual_monitor_sample_age_seconds=max(ages),raw_run_exit=1,targeted_diagnostic_exit=0,
        recommendation=diagnostics['recommendation']),sort_keys=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--protective-limit-us',type=int)
    parser.add_argument('--audit-existing',type=Path)
    args=parser.parse_args()
    if args.audit_existing:
        audit_existing(args.audit_existing,args.output)
    else:
        run(args.output,args.protective_limit_us)
