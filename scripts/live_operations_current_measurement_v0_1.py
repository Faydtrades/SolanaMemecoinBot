"""C3.1 current-source delta: one C1 loaded shape, two owned children.

Measurement wrappers delegate once to original startup/monitor/Runtime methods.
No threshold qualification, producer campaign, network or execution child ports.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_owned_measurement_v0_1 as c1
from live_operations_measurement_v0_1 import time_operation, collect_resources
from live.operations_degradation_v0_1 import DegradationPolicy, DegradationStore, ResourceLimit, METRICS
from live.ledger_actions_v0_1 import utc_microseconds
from live.operations_degradation_monitor_v0_1 import (
    MonitorConfiguration, HostMetric, HostObservations, content_fingerprint,
    monitor_configuration_digest,
)

BASELINE = ROOT/'docs/live/MEME_LIVE_OPERATIONS_C1_ENGINEERING_PROFILE_V0_1.json'
STARTUP = None
ORIGINAL_START = c1.b1.supervisor.start_live


def timed(operation, scope):
    result, record = time_operation(operation, scope=scope)
    return result, {key: asdict(value) for key, value in record.items()}


def measured_start_live(*args, **kwargs):
    global STARTUP
    started, STARTUP = timed(lambda: ORIGINAL_START(*args, **kwargs),
        'exact original start_live call in owned child; includes monitor installation; excludes spawn/import/IPC')
    STARTUP.update(pid=os.getpid(), completed_utc=c1.utc().isoformat())
    original_step = started.runtime.step
    def step_once(**resources):
        result, timing = timed(lambda: original_step(**resources),
            'exact original Runtime.step with installed monitor and nested measurement bookkeeping; excludes boundary collection')
        started.runtime._c3_step_measurement = dict(timing=timing, result=asdict(result))
        return result
    # Install before the supervisor evaluates its bound method: Python resolves
    # runtime.step before calling external_inputs inside that call expression.
    started.runtime.step = step_once
    return started


# Spawn imports this harness before the original supervisor child entry point.
# Only the harness reference is wrapped; the original source file is unchanged.
c1.b1.supervisor.start_live = measured_start_live


class Inputs(c1.Inputs):
    def _collect_and_supply(self, started):
        entered_wall, entered_cpu, entered_utc = time.perf_counter(), time.process_time(), c1.utc()
        if not hasattr(self, 'started'):
            self.started, self.now, self.clock_calls = started, c1.NOW+16, 0
            self.observations, self.host_samples = [], []
            assert STARTUP is not None and STARTUP['pid'] == os.getpid()
            producer = started.runtime.producer
            generation = producer.conn.execute('SELECT generation FROM live_producer_checkpoint_v0_1 WHERE singleton=1').fetchone()[0]
            assert generation == self.producer_generation_before+2
            self.publication = dict(start_utc=self.initiation_utc, end_utc=entered_utc.isoformat(),
                published_generation=generation, provenance='Original owned startup publication; witnessed parent initiation through first callback')
            before = c1.boundary(started, self.directory, witness=self.publication)
            barrier = c1.original.readiness.evaluate(started, clock=lambda: c1.rt.a3.clock(started.runtime.ledger, self.now), entry=self.entry)
            before.update(first_callback_utc=entered_utc.isoformat(), first_callback_perf_counter=entered_wall,
                process_lifetime_cpu_at_first_callback_seconds=entered_cpu, readiness=asdict(barrier), exact_startup=STARTUP)
            c1.atomic_json(self.directory/f'sample-{self.sample_number}-before.json', before)
            runtime = started.runtime
            monitor = runtime._operations_degradation
            assert monitor.store is not None and monitor.configuration.protective_qualification_digest is None
            observe = monitor.observe

            def observe_once(sample, **facts):
                self.host_now = utc_microseconds(sample.utc_upper_utc)
                before_changes = (dict(producer.metrics), dict(producer.last_profile_usage), producer.conn.total_changes)
                journal_before = self.alert_counts(monitor)
                result, timing = timed(lambda: observe(sample, **facts),
                    'original monitor.observe including current source/producer/ledger/host reads and alert writes')
                unchanged = before_changes == (dict(producer.metrics), dict(producer.last_profile_usage), producer.conn.total_changes)
                self.observations.append(dict(timing=timing, alert=asdict(result), metrics=dict(monitor.last_metrics),
                    backlog_age_applicable=monitor.backlog_age_applicable, producer_observer_unchanged=unchanged,
                    journal_before=journal_before, journal_after=self.alert_counts(monitor)))
                assert unchanged
                return result

            monitor.observe = observe_once
            self.step_start_wall, self.step_start_cpu = time.perf_counter(), time.process_time()
            return dict(clock=self.clock, source_cut_utc=c1.rt.a3.utc(c1.NOW+1), entry=self.entry,
                operations_resources=self.host)
        after = c1.boundary(started, self.directory, witness=self.publication)
        after.update(callback_interval_wall_seconds=entered_wall-self.step_start_wall,
            callback_interval_cpu_seconds=entered_cpu-self.step_start_cpu,
            interval_scope='first callback return through second callback entry; Runtime plus IPC/polling; no boundary sampling',
            runtime_timing=started.runtime._c3_step_measurement['timing'],
            runtime_result=started.runtime._c3_step_measurement['result'],
            monitor_observations=self.observations, host_samples=self.host_samples)
        c1.atomic_json(self.directory/f'sample-{self.sample_number}-after.json', after)
        c1.b1.wait_file(self.directory/f'never-release-{self.sample_number}')
        raise AssertionError('C3.1 measured child must not execute a second work unit')

    @staticmethod
    def alert_counts(monitor):
        with closing(sqlite3.connect(Path(monitor.configuration.path).resolve().as_uri()+'?mode=ro', uri=True)) as conn:
            return dict(conditions=conn.execute('SELECT count(*) FROM conditions').fetchone()[0],
                receipts=conn.execute('SELECT count(*) FROM receipts').fetchone()[0],
                metadata_last_us=conn.execute('SELECT last_us FROM metadata WHERE singleton=1').fetchone()[0])

    def host(self):
        config = self.started.runtime._operations_degradation.configuration
        resources = collect_resources(self.directory, c1.database_files(self.directory))
        record = dict(sampled_real_utc=c1.utc().isoformat(), fixture_observed_us=self.host_now,
            clock_scope='real host sample at this callback; typed timestamp mapped to original synthetic fixture clock only',
            resources={key: asdict(value) for key, value in resources.items()}, startup=STARTUP)
        witness = content_fingerprint(record)
        metrics = tuple(HostMetric(name, value, self.host_now, witness) for name, value in (
            ('HOST_RSS_BYTES', resources['process_rss_bytes'].value),
            ('HOST_DISK_RESERVE_BYTES', resources['disk_reserve_bytes'].value),
            ('STARTUP_US', int(STARTUP['operation_wall_seconds']['value']*1000000)),
            ('PROTECTIVE_STEP_US', None)))
        port = HostObservations(config.host_identity_digest, config.policy.reviewed_configuration_digest,
            content_fingerprint(asdict(self.started.runtime._ownership.fence)), metrics)
        self.host_samples.append(dict(evidence=record, port=asdict(port)))
        return port


class TwoChildren(c1.ChildBoundSupervisor):
    def _launch(self, now_us, mono):
        assert self.launch_attempts < 2, 'C3.1 permits only initial startup and one replacement'
        return super()._launch(now_us, mono)


def run(output):
    output = output.resolve()
    assert output.is_relative_to(Path(tempfile.gettempdir()).resolve()) and not output.exists()
    output.mkdir(parents=True)
    baseline_hash = c1.sha256(BASELINE)
    accepted = json.loads(BASELINE.read_text())
    environment = dict(python=sys.version, executable=sys.executable, platform=platform.platform(),
        processor=platform.processor(), cpu_count=os.cpu_count(), sqlite=sqlite3.sqlite_version,
        parent_pid=os.getpid(), argv=sys.argv, perf_counter=vars(time.get_clock_info('perf_counter')),
        host_scope='current engineering workstation/process; no deployment host approval')
    c1.write_json(output/'environment.json', environment)
    original_fixture_restart = c1.b21.a2.restart
    def configured_fixture_restart(fixture, *args, **kwargs):
        # The accepted C2 integration selftest's explicitly synthetic external
        # policy/host contract supplies the legacy C1 acquisition recipe. Actual
        # startup, installed monitor, Authority, Execution and Ledger stay active.
        digest = lambda value: format(value, '064x')
        path = str(fixture.directory/'c3-parent-synthetic-alerts.sqlite')
        reviewed = monitor_configuration_digest(fixture.expected, path=path,
            host_identity_digest=digest(91), resource_max_age_us=1000000,
            protective_qualification_digest=digest(92))
        policy = DegradationPolicy(reviewed, 100, 1000000, tuple(
            ResourceLimit(metric, 10 if metric == 'HOST_DISK_RESERVE_BYTES' else 10**12)
            for metric in sorted(METRICS)))
        config = MonitorConfiguration(path, policy, digest(91), fixture.expected.runtime_code_digest, 1000000, digest(92))
        if not Path(path).exists():
            DegradationStore.initialize(path, fixture.domain, policy, now_us=0)
        result = original_fixture_restart(fixture, *args, degradation_config=config, **kwargs)
        original_step = fixture.runtime.step
        def supplied_step(*, clock, **resources):
            last = []
            def sampled_clock():
                value = clock()
                last[:] = [utc_microseconds(value.utc_upper_utc)]
                return value
            def synthetic_host():
                return HostObservations(config.host_identity_digest, config.policy.reviewed_configuration_digest,
                    content_fingerprint(asdict(fixture.runtime._ownership.fence)), tuple(
                        HostMetric(metric, 100, last[0], digest(92) if metric == 'PROTECTIVE_STEP_US' else digest(93))
                        for metric in ('HOST_RSS_BYTES','HOST_DISK_RESERVE_BYTES','STARTUP_US','PROTECTIVE_STEP_US')))
            return original_step(clock=sampled_clock, operations_resources=synthetic_host, **resources)
        fixture.runtime.step = supplied_step
        c1.atomic_json(output/'parent-synthetic-setup-configuration.json', dict(monitor=asdict(config),
            host_metric_values=100, origin='accepted C2 integration fixture contract; synthetic inputs only, no host/deployment qualification'))
        return result
    with patch.object(c1.b21.a2, 'restart', configured_fixture_restart):
        place, configuration, generation, entry, unused = c1.build_loaded_fixture(output)
    baseline = json.loads((output/'loaded-boundary.json').read_text())
    expected_shape = accepted['recorded_shapes']['C1.3_composed']
    for metric, key in (('active_feature_events','retained_feature_events'), ('hottest_mint_events','hot_events'), ('unfinished_mints','unfinished_mints')):
        assert baseline['metrics'][metric]['value'] == expected_shape[key]
    # No deployment limits are inferred from observed C1 maxima. Empty policy
    # coverage deliberately holds ENTRY while exercising the actual observer.
    identity = configuration['expected_identity']
    host_digest = content_fingerprint(environment)
    alert_path = str(place/'c3-current-alerts.sqlite')
    binding = monitor_configuration_digest(identity, path=alert_path, host_identity_digest=host_digest,
        resource_max_age_us=1000000, protective_qualification_digest=None)
    policy = DegradationPolicy(binding, 100, 1000000, ())
    config = MonitorConfiguration(alert_path, policy, host_digest, identity.runtime_code_digest, 1000000, None)
    DegradationStore.initialize(alert_path, configuration['domain'], policy, now_us=0)
    configuration['degradation_config'] = config
    c1.write_json(output/'configuration.json', dict(monitor=asdict(config), producer_profile=asdict(c1.PROFILE),
        identity=asdict(identity), baseline_profile=dict(path=str(BASELINE), sha256=baseline_hash),
        policy_state='CONFIGURED measurement-only unresolved policy; empty resource limits, no protective qualification',
        configured_timing_scope='C2 fixture alert-delay/recovery-age and resource-age controls only; no performance acceptance limit',
        fixture_preparation_adapter='Parent-only a2.restart supplies accepted C2 synthetic policy/host contracts to actual installed monitor during C1 acquisition recipe. Original production boundaries remain active. Restored before all measured owned startups; parent synthetic qualification is never used as measured host qualification.',
        unknown=['deployment resource limits','protective latency qualification','real-host deployment gate'],
        source_binding=asdict(configuration['source_binding']), source_profile=asdict(configuration['source_profile']),
        loaded_shape=expected_shape, children=2, runtime_steps_per_child=1))
    inputs = Inputs(place, entry)
    supervisor = TwoChildren(configuration, inputs, c1.b1.supervisor.HealthProfile(20000000,20000000,1000000), expected_generation=generation)
    driver = c1.GuardedDriver(supervisor, place, now=generation+1)
    samples, checks = [], []
    try:
        for number in (1, 2):
            inputs.sample_number = number
            inputs.producer_generation_before = baseline['checkpoint']['generation'] if number == 1 else samples[-1]['after']['checkpoint']['generation']
            initiated_wall, initiated_utc = time.perf_counter(), c1.utc()
            inputs.initiation_utc = initiated_utc.isoformat()
            terminated = None
            if number == 2:
                previous = supervisor.process
                previous.terminate()
                previous.join(timeout=5)
                assert not previous.is_alive()
                terminated = dict(pid=previous.pid, exitcode=previous.exitcode,
                    confirmed_dead_wall_seconds=time.perf_counter()-initiated_wall)
                driver.deliberately_terminated.add(previous.pid)
                driver.now += 1
            result = driver.until(lambda fact: fact.completed_steps >= number)
            ended_wall, ended_utc = time.perf_counter(), c1.utc()
            deadline = time.monotonic()+20
            while not (place/f'sample-{number}-after.json').exists():
                failures = list(place.glob('sample-*-failure.json'))
                assert not failures, 'collector failure: '+failures[0].read_text() if failures else ''
                assert time.monotonic() < deadline, 'bounded after-boundary rendezvous timeout'
                time.sleep(.01)
            before = json.loads((place/f'sample-{number}-before.json').read_text())
            after = json.loads((place/f'sample-{number}-after.json').read_text())
            c1.validate_boundaries(baseline, before, after)
            assert result.last_work == ('PROTECTIVE_ACTION_STAGED' if number == 1 else 'NEED_EXECUTION')
            assert len(after['monitor_observations']) == len(after['host_samples']) == 2
            for observation in after['monitor_observations']:
                assert observation['alert']['unavailable_code'] is None
                assert observation['alert']['entry_held'] and not observation['alert']['grants_permission']
                assert any(row['condition'] == 'PROFILE_UNRESOLVED' and row['state'] == 'ACTIVE' for row in observation['alert']['conditions'])
                assert set(observation['metrics']) == METRICS and observation['metrics']['PROTECTIVE_STEP_US'] is None
                assert observation['metrics']['HOST_RSS_BYTES'] > 0 and observation['metrics']['HOST_DISK_RESERVE_BYTES'] > 0
                assert observation['producer_observer_unchanged'] and observation['backlog_age_applicable'] is False
            sample = dict(number=number, kind='initial_owned_startup' if number == 1 else 'actual_replacement',
                initiated_utc=initiated_utc.isoformat(), confirmed_result_utc=ended_utc.isoformat(),
                observed_endpoint_seconds=ended_wall-initiated_wall,
                endpoint_scope='parent initiation to receipt of first protective Runtime result; includes spawn/import/monitor/startup/evidence/IPC/polling and prior exact-child termination for replacement',
                terminated_previous_child=terminated, supervision=asdict(result), before=before, after=after)
            samples.append(sample)
            checks.append(dict(child=number, boundary_identity_protection_source_preserved=True,
                actual_monitor_observations=2, actual_host_samples=2, missing_qualification_entry_held=True))
            c1.atomic_json(output/f'owned-sample-{number}.json', sample)
    finally:
        c1.b1.cleanup(supervisor)
        c1.atomic_json(output/'child-cleanup.json', dict(launch_attempts=supervisor.launch_attempts,
            child_pids=supervisor.child_pids, retained_process_handle_cleared=supervisor.process is None,
            retained_channel_cleared=supervisor._channel is None,
            method='original B1 cleanup of exact retained handle; replaced child was joined and confirmed dead'))
    assert supervisor.launch_attempts == 2 and len(set(supervisor.child_pids)) == 2
    integrity = {}
    for path in sorted(place.glob('*.sqlite')):
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as conn:
            integrity[path.name] = dict(integrity=conn.execute('PRAGMA integrity_check').fetchall(), foreign_keys=conn.execute('PRAGMA foreign_key_check').fetchall())
            assert integrity[path.name] == dict(integrity=[('ok',)], foreign_keys=[])
    c1.write_json(output/'checks.json', dict(children=checks, sqlite=integrity, baseline_immutable=c1.sha256(BASELINE) == baseline_hash))
    assert c1.sha256(BASELINE) == baseline_hash
    paths = {Path(module.__file__).resolve() for module in tuple(sys.modules.values())
        if getattr(module,'__file__',None) and Path(module.__file__).suffix == '.py' and Path(module.__file__).resolve().is_relative_to(ROOT)}
    c1.write_json(output/'source_hashes.json', {str(path.relative_to(ROOT)): c1.sha256(path) for path in sorted(paths)})
    c1.write_json(output/'fixture_hashes.json', {str(path.relative_to(output)): c1.sha256(path) for path in sorted(place.iterdir()) if path.is_file() and path.name in c1.database_files(place)})
    boundaries = [record for sample in samples for record in (sample['before'],sample['after'])]
    summary = dict(version='operations_c3_current_measurement_v0.1', status='IMPLEMENTED_PENDING_PROJECT_REVIEW',
        children=2, protected_runtime_steps=2, monitor_observations=4, host_samples=4,
        observed_endpoint_seconds=[sample['observed_endpoint_seconds'] for sample in samples],
        observed_exact_startup_seconds=[sample['before']['exact_startup']['operation_wall_seconds']['value'] for sample in samples],
        observed_runtime_step_seconds=[sample['after']['runtime_timing']['operation_wall_seconds']['value'] for sample in samples],
        observed_monitor_seconds=[[record['timing']['operation_wall_seconds']['value'] for record in sample['after']['monitor_observations']] for sample in samples],
        observed_child_boundary_resource_maxima={name: max(record['metrics'][name]['value'] for record in boundaries)
            for name in ('process_rss_bytes','process_lifetime_peak_rss_bytes','workload_disk_bytes')},
        min_observed_child_boundary_disk_reserve_bytes=min(record['metrics']['disk_reserve_bytes']['value'] for record in boundaries),
        first_runtime_results=[sample['supervision']['last_work'] for sample in samples],
        current_policy='unresolved; no thresholds or protective qualification inferred',
        limitations='Two current-source samples at one accepted synthetic loaded acquired/due shape on current engineering workstation. Monitor times are nested observed components, not a causal A/B overhead estimate. Includes instrumentation/host collection and normal supervisor alert-store traffic, no deliberate lock contention. Synthetic truth clock does not measure elapsed latency: timings use real perf_counter/process_time. No real-host deployment qualification, healthy-policy latency, sustained rate, configured-maxima capacity, percentile, worst-case or total protection guarantee; no child execution/network ports.')
    c1.write_json(output/'result.json', summary)
    c1.write_json(output/'artifact_hashes.json', {str(path.relative_to(output)): c1.sha256(path) for path in sorted(output.rglob('*.json'))})
    print(json.dumps(summary, sort_keys=True))
    print('C3_1_EVIDENCE='+str(output))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    run(parser.parse_args().output)
