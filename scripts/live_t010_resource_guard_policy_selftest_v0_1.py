"""Small deterministic C2 envelope/readiness tests; no native quotas or workloads."""
import copy
from dataclasses import asdict, replace
from contextlib import ExitStack, redirect_stdout
import io
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
sys.path.insert(0, str(ROOT/'scripts'))
from live import t010_resource_envelope_v0_1 as resource
from live import t010_public_host_v0_1 as host


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='c2-resource-policy-')
        self.root = Path(self.temp.name)
        self.index = 0
        self.profile = json.loads((ROOT/'docs/live/MEME_LIVE_OPERATIONS_C1_ENGINEERING_PROFILE_V0_1.json').read_text())['configured_continuation_profile']['values']
        self.spec = {'resource_limits': {'HISTORY_BYTES': self.profile['history_bytes'],
            'CHECKPOINT_BYTES': self.profile['checkpoint_bytes'],
            'PRODUCER_PENDING_ROWS': self.profile['pending_rows'],
            'PRODUCER_PENDING_BYTES': self.profile['pending_bytes'], 'HOST_DISK_RESERVE_BYTES': 1},
            'producer_profile': self.profile, 'dispatch': {'batch_rows': 32},
            'host': {'max_source_rows': 100, 'max_admitted_roots': 2},
            'qualification': {'max_source_rows': 100, 'max_admitted_roots': 2},
            'source_record_bytes': 1048576, 'source_unit_raw_bytes': 1048576}
        self.envelope = {'derivation': self.spec, 'observation_policy': resource.OBSERVATION_POLICY,
            'observation_evidence': self.ref({'placeholder': True})}
    def tearDown(self):
        self.temp.cleanup()
    def ref(self, value):
        self.index += 1
        path = self.root/(str(self.index)+'.json')
        path.write_text(json.dumps(value, sort_keys=True))
        return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    def startup(self, envelope, limits):
        inputs = host.PublicHostInputs(NS(record={'inputs': {'resource_envelope': self.ref(envelope),
            'monitor': {'resource_limits': limits}}}), {}, 'policy', 'grant', 'digest', (),
            resource_path=str(self.root/'work.json'))
        with (patch.object(resource, 'enforce_current_process_rss', return_value={'maximum_bytes': 123}) as rss,
            patch.object(resource, 'enforce_current_process_private_bytes', return_value={'private_bytes': 456}) as private):
            inputs.runtime_before_start()
            return rss.call_args_list, private.call_args_list, host.read_record(self.root/'child_rss_quota.json')
    def test_legacy_numeric_startup_unchanged(self):
        rss, private, receipt = self.startup({'derivation': {'measured_guards': {'child_private_bytes': 456}}}, {'HOST_RSS_BYTES': 123})
        self.assertEqual(rss[0].args, (123,)); self.assertEqual(private[0].args, (456,))
        self.assertEqual(receipt['quota']['maximum_bytes'], 123)
    def test_observation_startup_installs_no_quota(self):
        rss, private, receipt = self.startup(self.envelope, {})
        self.assertEqual(rss, []); self.assertEqual(private, [])
        self.assertFalse(receipt['quota']['quota_installed'])
    def test_numerical_guard_cannot_silently_downgrade(self):
        for change in ({'measured_guards': {'HOST_RSS_BYTES': 123}}, {'resource_limits': {'HOST_RSS_BYTES': 123}}):
            value = copy.deepcopy(self.envelope); value['derivation'].update(change)
            with self.assertRaisesRegex(ValueError, 'OBSERVATION_ENFORCEMENT_CONFLICT'):
                resource.observation_only(value)
    def test_missing_or_invalid_designation_rejected(self):
        for policy in (None, 'unknown'):
            value = copy.deepcopy(self.envelope); value['observation_policy'] = policy
            with self.assertRaises(ValueError): resource.observation_only(value)
    def observation_fixture(self):
        # Shape of an original fresh-process cohort as the hardened validator
        # admits it: real native identity, the current measurement harness
        # bytes, a native process instance and retained codec bindings. The
        # runtime digest stays a fixture value patched at the validator.
        from live.t010_resource_measurement_v0_1 import SCENARIOS
        from live.t010_resource_environment_v0_1 import native_identity
        native = resource.content_fingerprint(native_identity())
        harness = hashlib.sha256((ROOT/'scripts/live_t010_joint_hot_measure_v0_3.py').read_bytes()).hexdigest()
        codec = self.ref({'scope': 'TEST_ONLY_PROFILE_CODEC_COST_NOT_QUALIFICATION'})
        series = {key: [] for key in resource.OBSERVATION_METRICS}; scenarios = {}
        number = 0
        for scenario in sorted(SCENARIOS):
            scenarios[scenario] = []
            for _ in range(3):
                number += 1
                instance = {'pid': number, 'creation_filetime': number}
                identity = resource.content_fingerprint(instance); instance['digest'] = identity
                raw = {'scope': resource.EMPIRICAL_ORIGINAL_SCOPE, 'process_instance': instance,
                    'runtime_code_digest': 'runtime', 'native_digest': native, 'measurement_harness_sha256': harness,
                    'prepared_codec_cost_binding': codec, 'effective_measurement_codec_cost_binding': codec,
                    'codec_cost': [{'reference': codec, 'start_ns': 1, 'end_ns': 2, 'rebuilt_content_digest': 'c'*64}],
                    'parent_boundary': {'spawn_begin_ns': 0, 'started_arrived_ns': 0, 'started_validated_ns': 0,
                        'spawn_to_validated_us': 0, 'spawn_to_first_legal_unit_us': number},
                    'step_end_ns': number*1000, 'first_step_us': number}
                original_result = self.ref(raw)
                metrics = {key: number for key in resource.OBSERVATION_METRICS if key not in ('parent_first_legal_unit_us', 'first_runtime_step_us')}
                metrics['OLDEST_UNCONSUMED_AGE_US'] = None
                normalized = {'scope': resource.EMPIRICAL_RESULT_SCOPE, 'scenario': scenario, 'empty_backlog_verified': True,
                    'runtime_code_digest': 'runtime', 'native_digest': native, 'exit_code': 0,
                    'fresh_process': True, 'unresolved_constraints': [], 'process_instance_digest': identity,
                    'process_instance': instance, 'original_result': original_result, 'metrics': metrics,
                    'codec_cost_call_count': 1,
                    'measurement_provenance': {'schema': 'MEME_LIVE_EMPIRICAL_PROCESS_PROVENANCE_V1',
                        'kind': 'ORIGINAL_FRESH_PROCESS_MEASUREMENT', 'original_result': original_result,
                        'process_instance_digest': identity, 'measurement_harness_sha256': harness,
                        'prepared_codec_cost_binding': codec, 'effective_measurement_codec_cost_binding': codec}}
                scenarios[scenario].append(self.ref(normalized))
                for key in series: series[key].append(number)
        return {'schema': 'MEME_LIVE_T010_RESOURCE_OBSERVATIONS_V1', 'runtime_code_digest': 'runtime',
            'native_digest': native, 'scenarios': scenarios, 'series': series}
    def test_observations_validated_as_evidence_not_thresholds(self):
        evidence = self.observation_fixture()
        with patch('live.t010_resource_measurement_v0_1.runtime_code_digest', return_value='runtime'):
            self.assertEqual(resource.validate_observations(self.ref(evidence)), evidence)
            evidence['series']['HOST_RSS_BYTES'][0] += 1
            with self.assertRaisesRegex(ValueError, 'OBSERVATION_SERIES_CONFLICT'):
                    resource.validate_observations(self.ref(evidence))
    def gate(self, *, physical=True):
        self.index += 1
        envelope = copy.deepcopy(self.envelope)
        if physical: envelope['physical_evidence'] = self.ref({'environment': self.ref({'volumes': {}, 'requirements': {}})})
        with patch.object(resource.ResourceGate, '_source_tables', return_value=('raw',)):
            gate = resource.ResourceGate(self.root/('gate-'+str(self.index)+'.json'), envelope, 'host', create=True)
        return gate
    def runtime(self, *, pending=False, recovery=False):
        reservations = [NS(retired_sequence=None)] if pending else []
        return NS(runtime=NS(capability='NO_BROADCAST', _dry_recovery=recovery,
            ledger=NS(_custody=NS(reservations=reservations)), producer=object(),
            source=NS(latest=lambda: NS(disposition='HEALTHY'))))
    def metrics(self):
        return {'IDENTITY_ROWS': 0, 'TOMBSTONE_ROWS': 0, 'UNFINISHED_MINTS': 0,
            'HISTORY_BYTES': 0, 'CHECKPOINT_BYTES': 0, 'PRODUCER_PENDING_ROWS': 0, 'PRODUCER_PENDING_BYTES': 0}
    def step(self, gate, started, metrics):
        with patch.object(resource.ResourceGate, '_source_tables', return_value=('raw',)), patch.object(gate, '_checkpoint_boundary', return_value=True), patch('live.t010_resource_environment_v0_1.check_volume_reserves', return_value={}), patch('live.operations_degradation_monitor_v0_1._producer_cut', return_value=(metrics, {}, 'digest')):
            return gate.before_step(started)
    def test_below_capacity_fresh_work_eligible(self):
        gate = self.gate(); started = self.runtime()
        self.assertTrue(self.step(gate, started, self.metrics()))
        self.assertEqual(started.runtime._resource_batch_rows, 32)
    def test_unknown_invalid_required_usage_denies(self):
        for bad in (None, -1, 'unknown', 1.5):
            gate = self.gate(); metrics = self.metrics(); metrics['CHECKPOINT_BYTES'] = bad
            self.assertFalse(self.step(gate, self.runtime(), metrics))
        gate = self.gate(); metrics = self.metrics(); del metrics['HISTORY_BYTES']
        self.assertFalse(self.step(gate, self.runtime(), metrics))
    def test_missing_required_physical_evidence_denies_fresh_work(self):
        self.assertFalse(self.step(self.gate(physical=False), self.runtime(), self.metrics()))
    def test_reached_and_exceeded_existing_caps_deny(self):
        for key in ('HISTORY_BYTES', 'CHECKPOINT_BYTES', 'PRODUCER_PENDING_ROWS', 'PRODUCER_PENDING_BYTES'):
            for extra in (0, 1):
                gate = self.gate(); metrics = self.metrics(); metrics[key] = self.spec['resource_limits'][key]+extra
                self.assertFalse(self.step(gate, self.runtime(), metrics))
    def test_pending_and_recovery_precede_unknown_and_holds(self):
        for pending, recovery in ((True, False), (False, True)):
            gate = self.gate(physical=False); started = self.runtime(pending=pending, recovery=recovery)
            owned = list(started.runtime.ledger._custody.reservations)
            with patch.object(resource.ResourceGate, '_source_tables', return_value=('raw',)): gate.hold('EXHAUSTED')
            self.assertTrue(self.step(gate, started, {}))
            self.assertEqual(started.runtime.ledger._custody.reservations, owned)
            self.assertEqual(started.runtime._resource_batch_rows, 0)
    def test_policy_ceiling_values_unchanged(self):
        policy = json.loads((ROOT/'docs/live/MEME_LIVE_OPERATIONS_C2_RESOURCE_GUARD_POLICY_V0_1.json').read_text())
        self.assertEqual(policy['configured_ceilings']['values'], self.profile)
        self.assertTrue(all(row['enforcement_threshold'] is None for row in policy['observations']))

    def test_envelope_modes_and_exact_monitor_map(self):
        for observing in (False, True):
            calculated = copy.deepcopy(self.spec)
            calculated['resource_limits']['HOST_RSS_BYTES'] = 123
            record = {'schema': resource.SCHEMA, 'scope': resource.SCOPE,
                'derivation': copy.deepcopy(calculated), 'physical_evidence': None,
                'authorization_reference': 'UNIT_FIXTURE', 'project_acceptance_claimed': False}
            if observing:
                record.update(observation_policy=resource.OBSERVATION_POLICY,
                    observation_evidence=self.ref({'fixture': True}))
                del record['derivation']['resource_limits']['HOST_RSS_BYTES']
            inputs = {'accepted_profile': self.ref({}), 'accepted_extension': self.ref({}),
                'qualification_substitutions': {'scope': resource.SCOPE}, 'guard_amendment': None,
                'monitor': {'resource_limits': copy.deepcopy(record['derivation']['resource_limits'])}}
            record['bound_inputs_digest'] = resource.bound_inputs(inputs)
            inputs['resource_envelope'] = self.ref(record)
            with patch.object(resource, 'derive', side_effect=lambda *args: copy.deepcopy(calculated)), patch.object(resource, '_validate_observations', return_value={}):
                self.assertEqual(resource.validate(inputs, require_physical=False), record)
                inputs['monitor']['resource_limits']['HOST_RSS_BYTES'] = 124
                with self.assertRaisesRegex(ValueError, 'T010_RESOURCE_LIMITS_CONFLICT'):
                    resource.validate(inputs, require_physical=False)

    def physical_fixture(self, observing):
        from live.t010_resource_measurement_v0_1 import SCENARIOS
        spec = copy.deepcopy(self.spec); spec['physical_validation_required'] = ['UNIT_FIXTURE']
        guards = {'HOST_RSS_BYTES': 123, 'child_private_bytes': 456, 'supervisor_private_bytes': 789}
        if not observing: spec['measured_guards'] = guards
        digest = resource.content_fingerprint(spec)
        native = {'fixture': 'native'}
        measured = {'runtime_code_digest': 'runtime', 'native_digest': resource.content_fingerprint(native),
            'series': {'HOST_RSS_BYTES': [100]}, 'scenarios': {scenario: [] for scenario in SCENARIOS}}
        measured['measurement_boundary'] = self.ref({'scenarios': measured['scenarios'], 'supervisor_runs': [], 'public_response_runs': []})
        evidence_ref = self.ref(measured)
        record = {'derivation': spec, 'bound_inputs_digest': 'binding'}
        boundary = {'scope': 'ORIGINAL_MAXIMUM_STATE_BOUNDARY_SCENARIOS', 'derivation_digest': digest,
            'runtime_code_digest': 'runtime', 'measurement': evidence_ref, 'unresolved_constraints': []}
        if observing:
            record.update(observation_policy=resource.OBSERVATION_POLICY, observation_evidence=evidence_ref)
            boundary.update(schema='MEME_LIVE_T010_OBSERVED_RESOURCE_BOUNDARY_V1', scenarios=measured['scenarios'])
        else:
            record['measured_guard_evidence'] = evidence_ref
            boundary.update(schema='MEME_LIVE_T010_QUALIFIED_RESOURCE_BOUNDARY_V1', under_limits={})
            for scenario in (*sorted(SCENARIOS), 'supervisor', 'public_response'):
                boundary['under_limits'][scenario] = []
                for _ in range(3):
                    value = {'scenario': scenario, 'exit_code': 0, 'fresh_process': True,
                        'runtime_code_digest': 'runtime', 'native_digest': measured['native_digest'],
                        'limit_binding_digest': resource.content_fingerprint(guards),
                        'verified_quota': {'rss_bytes': 123, 'child_private_bytes': 456, 'tree_private_bytes': 1245},
                        'process_instance_digest': f'{self.index+1:064x}', 'metrics': {'HOST_RSS_BYTES': 100}}
                    boundary['under_limits'][scenario].append(self.ref(value))
            boundary['validation'] = {name: self.ref({'runtime_code_digest': 'runtime', 'exit_code': 0, 'checks': 1})
                for name in ('volume_boundary', 'physical_certificate', 'measurement_guards', 'process_private_limit')}
            boundary['independent_review'] = self.ref({'runtime_code_digest': 'runtime', 'scope': 'FROZEN_T010_RESOURCE_INTERFACES_AND_PROOF', 'findings': []})
        physical = {'schema': 'MEME_LIVE_T010_MAXIMUM_STATE_RESOURCE_VALIDATION_V1',
            'scope': 'ISOLATED_ORIGINAL_BOUNDARY_VALIDATION', 'bound_inputs_digest': 'binding',
            'derivation_digest': digest, 'coverage': ['UNIT_FIXTURE'],
            'physical_size_model': self.ref({'scope': 'ALL_GROWABLE_STORES_PHYSICAL_UPPER_BOUND', 'derivation_digest': digest}),
            'boundary_evidence': self.ref(boundary), 'host_identity_digest': 'host', 'runtime_code_digest': 'runtime',
            'maxima': {'HOST_RSS_BYTES': 100}, 'disk_reserve_minimum_bytes': 1,
            'environment': self.ref({'native': native}), 'unresolved_constraints': []}
        inputs = {'store_paths': {}, 'accepted_extension': self.ref({'host_identity_digest': 'host'})}
        return physical, record, spec, inputs, measured, boundary

    def validate_physical_fixture(self, fixture):
        physical, record, spec, inputs, measured, _ = fixture
        with (patch.object(resource, '_validate_observations', return_value=measured),
            patch('live.t010_resource_measurement_v0_1.runtime_code_digest', return_value='runtime'),
            patch('live.t010_resource_measurement_v0_1.process_metrics', side_effect=lambda value, scenario: value['metrics']),
            patch('live.t010_resource_environment_v0_1.validate_binding'),
            patch('live.t010_resource_environment_v0_1.constructor_input_references', return_value=[]),
            patch('live.t010_physical_certificate_v0_1.validate_model')):
            resource.validate_physical(physical, record, spec, inputs=inputs)

    def test_physical_observations_require_binding_but_not_quota(self):
        fixture = self.physical_fixture(True)
        self.validate_physical_fixture(fixture)
        fixture[0]['maxima']['HOST_RSS_BYTES'] = 101
        with self.assertRaisesRegex(ValueError, 'MEASURED_PHYSICAL_BINDING_CONFLICT'):
            self.validate_physical_fixture(fixture)

    def test_physical_enforced_quota_validation_unchanged(self):
        fixture = self.physical_fixture(False)
        self.validate_physical_fixture(fixture)
        physical, record, spec, inputs, measured, boundary = fixture
        scenario = next(iter(boundary['under_limits']))
        ref = boundary['under_limits'][scenario][0]
        value = json.loads(Path(ref['path']).read_text()); value['verified_quota']['rss_bytes'] = 122
        boundary['under_limits'][scenario][0] = self.ref(value)
        physical['boundary_evidence'] = self.ref(boundary)
        with self.assertRaisesRegex(ValueError, 'CONTAINED_RESULT_CONFLICT'):
            self.validate_physical_fixture(fixture)

    def health_fixture(self, observing=True):
        # Deliberately small test inputs, not defaults or reviewed policy values.
        health = {'startup_us': 2000000, 'progress_us': 3000000, 'termination_us': 4000000}
        envelope = copy.deepcopy(self.envelope) if observing else {'derivation': {}}
        envelope['derivation']['qualification'] = {'max_startups': 2, 'max_runtime_steps': 5}
        if not observing:
            envelope['derivation']['measured_guards'] = dict(
                supervisor_startup_us=health['startup_us'], supervisor_progress_us=health['progress_us'],
                supervisor_termination_us=health['termination_us'], supervisor_rss_bytes=123,
                child_private_bytes=456, supervisor_private_bytes=789)
        profile = NS(record={'inputs': {'resource_envelope': self.ref(envelope),
            'driver': {'max_steps': 5, 'public_rpc_profile': {'observation_timeout_seconds': 1}},
            'monitor': {'resource_limits': {} if observing else {'HOST_RSS_BYTES': 123}},
            'store_paths': {'ledger': str(self.root/'ledger.db')}}})
        request = {'profile': self.ref({}), 'facts': self.ref({})}
        if observing: request.update(health=health, observation_policy=resource.OBSERVATION_POLICY)
        return profile, request, health

    def test_qualifier_observation_and_legacy_startup(self):
        from live import runtime_dry_public_qualification_v0_1 as qualifier
        for observing in (True, False):
            profile, request, _ = self.health_fixture(observing)
            with (patch.object(qualifier, 'validate_request', return_value=(profile, [NS(policy=NS(clock='clock'))])),
                patch.object(qualifier, 'ReviewedPublicFacts', return_value=NS(public_clock=NS(policy='clock'))),
                patch.object(resource, 'ResourceGate'),
                patch.object(qualifier, 'initialize_isolated', side_effect=RuntimeError('INITIALIZATION_BOUNDARY')),
                patch.object(resource, 'enforce_current_process_rss') as rss,
                patch.object(resource, 'enforce_current_process_private_bytes') as private):
                with self.assertRaisesRegex(RuntimeError, 'INITIALIZATION_BOUNDARY'):
                    qualifier.qualify(request)
                if observing:
                    rss.assert_not_called(); private.assert_not_called()
                else:
                    rss.assert_called_once_with(123); private.assert_called_once_with(456)
                    del profile.record['inputs']['monitor']['resource_limits']['HOST_RSS_BYTES']
                    with self.assertRaises(KeyError): qualifier.qualify(request)

    def test_health_contract_and_finite_cli_containment(self):
        import live_runtime_dry_public_qualification_v0_1 as cli
        from live.runtime_dry_public_qualification_v0_1 import qualification_health
        for observing in (True, False):
            profile, request, health = self.health_fixture(observing)
            if observing: self.assertEqual(asdict(qualification_health(request, profile)), health)
            guards, budget = cli._watchdog_budget(profile, request)
            self.assertEqual(budget['timeout_us'], 4*health['startup_us']+5*health['progress_us']+1000000)
            self.assertEqual(budget['termination_us'], health['termination_us'])
            output = self.root/('observed.json' if observing else 'enforced.json')
            result = dict(deadline_expired=False, termination_confirmed=True, exit_code=0)
            with (patch.object(cli, '_arguments', return_value=NS(verify=False, output=output, request=self.root/'request.json')),
                patch.object(cli, 'read_json', return_value=request), patch.object(cli, 'load_profile', return_value=profile),
                patch('live.operations_windows_host_v0_1.WindowsHostBoundary') as boundary,
                patch.object(cli.subprocess, 'Popen') as child,
                patch.object(cli, '_wait_owned_child', return_value=result.copy()) as wait,
                patch.object(cli, '_publish_candidate') as publish,
                patch.object(resource, 'enforce_current_process_rss') as rss,
                patch.object(resource, 'enforce_host_tree_private_bytes', return_value={'quota_installed': True}) as private,
                redirect_stdout(io.StringIO())):
                self.assertEqual(cli.main(), 0)
                boundary.return_value.enter.assert_called_once()
                self.assertIs(wait.call_args.args[0], child.return_value)
                self.assertEqual(wait.call_args.kwargs['timeout_us'], budget['timeout_us'])
                self.assertEqual(wait.call_args.kwargs['termination_us'], health['termination_us'])
                self.assertGreater(wait.call_args.kwargs['launched_ns'], 0)
                publish.assert_called_once()
                if observing:
                    rss.assert_not_called(); private.assert_not_called()
                    receipt = json.loads(output.with_suffix('.json.process.json').read_text())
                    self.assertFalse(receipt['quota']['quota_installed']); self.assertIsNone(guards)
                else:
                    rss.assert_called_once_with(123)
                    private.assert_called_once_with(boundary.return_value.enter.return_value, guards)
        profile, request, _ = self.health_fixture()
        for field in ('startup_us', 'progress_us', 'termination_us'):
            for invalid in (None, 0, -1, True, 86400000001):
                bad = copy.deepcopy(request); bad['health'][field] = invalid
                with self.assertRaises(ValueError): cli._watchdog_budget(profile, bad)
        for field in ('health', 'observation_policy'):
            bad = copy.deepcopy(request); del bad[field]
            with self.assertRaises(ValueError): cli._watchdog_budget(profile, bad)
        candidate = self.root/'late.json'; candidate.write_text('{}')
        final = self.root/'published.json'
        for change in ({'deadline_expired': True}, {'termination_confirmed': False}, {'exit_code': 1}):
            with self.assertRaisesRegex(ValueError, 'PROCESS_DENIED'):
                cli._publish_candidate(candidate, final, dict(result, **change))
            self.assertTrue(candidate.exists()); self.assertFalse(final.exists())

    def monitor_fixture(self, observing=True, omitted=()):
        from live import operations_degradation_monitor_v0_1 as monitor
        from live.operations_degradation_v0_1 import ResourceLimit
        observed = monitor.HOST_METRICS - {'HOST_DISK_RESERVE_BYTES'} if observing else frozenset()
        policy = monitor.DegradationPolicy('a'*64, 100, 100,
            tuple(ResourceLimit(key, 100) for key in sorted(monitor.REQUIRED_METRICS-observed-set(omitted))))
        config = monitor.MonitorConfiguration(str(self.root/'monitor.db'), policy, 'b'*64, 'c'*64, 10, 'd'*64,
            resource.OBSERVATION_POLICY if observing else None)
        metrics = {key: 10 for key in monitor.REQUIRED_METRICS}
        metrics['HOST_DISK_RESERVE_BYTES'] = 100
        for key in observed: metrics[key] = 10000
        return monitor, config, metrics

    def monitor_conditions(self, monitor, config, metrics):
        current = NS(configuration=config, store=object(),
            started=NS(runtime=NS(ledger=NS(domain=NS(economic_domain_id='e'*64)))))
        with patch.object(monitor, '_record') as record:
            monitor.OperationsMonitor._observe_resources(current, metrics, 'f'*64, '1'*64, '2'*64, 1, 100)
        return [call.args[1] for call in record.call_args_list if not call.kwargs.get('healthy', False)]

    def test_monitor_observation_coverage_freshness_and_identity(self):
        from live.operations_startup_v0_1 import dry_monitor_fingerprint
        monitor, config, metrics = self.monitor_fixture()
        self.assertEqual(self.monitor_conditions(monitor, config, metrics), [])
        for metric in monitor.HOST_METRICS-{'HOST_DISK_RESERVE_BYTES'}:
            for invalid in (None, -1, True, 'unknown'):
                self.assertIn('PROFILE_UNRESOLVED', self.monitor_conditions(monitor, config, dict(metrics, **{metric: invalid})))
        _, missing, _ = self.monitor_fixture(omitted=('AGGREGATE_FEATURE_BYTES',))
        self.assertIn('PROFILE_UNRESOLVED', self.monitor_conditions(monitor, missing, metrics))
        _, legacy, legacy_metrics = self.monitor_fixture(False)
        legacy_metrics['HOST_RSS_BYTES'] = 101
        self.assertIn('RESOURCE_EXCEEDED', self.monitor_conditions(monitor, legacy, legacy_metrics))
        self.assertNotEqual(dry_monitor_fingerprint(config), dry_monitor_fingerprint(replace(config, observation_policy=None)))
        old_values = (str(Path(legacy.path).resolve()), legacy.policy.persistent_unknown_alert_us,
            legacy.policy.recovery_evidence_max_age_us, tuple(asdict(limit) for limit in legacy.policy.resource_limits),
            legacy.host_identity_digest, legacy.resource_max_age_us, legacy.protective_qualification_digest)
        self.assertEqual(dry_monitor_fingerprint(legacy), resource.content_fingerprint(old_values))
        with self.assertRaisesRegex(ValueError, 'OBSERVATION_ENFORCEMENT_CONFLICT'):
            replace(legacy, observation_policy=resource.OBSERVATION_POLICY)
        current = NS(configuration=config)
        observation = monitor.HostObservations(config.host_identity_digest, config.policy.reviewed_configuration_digest,
            'e'*64, tuple(monitor.HostMetric(key, 10, 1, 'd'*64) for key in monitor.HOST_METRICS))
        values, _ = monitor.OperationsMonitor._host_metrics(current, lambda: observation, 100, 'e'*64)
        self.assertIsNone(values['HOST_RSS_BYTES']); self.assertIsNone(values['HOST_DISK_RESERVE_BYTES'])
        self.assertEqual(values['STARTUP_US'], 10); self.assertEqual(values['PROTECTIVE_STEP_US'], 10)
        values, _ = monitor.OperationsMonitor._host_metrics(current, lambda: observation, 100, 'f'*64)
        self.assertTrue(all(value is None for value in values.values()))

    def test_qualification_exact_observation_evidence(self):
        from live.runtime_dry_public_qualification_v0_1 import validate_metric_evidence
        _, config, metrics = self.monitor_fixture()
        def validate(values, policy=config.policy, **kwargs):
            validate_metric_evidence(values, values, policy, observation_policy=resource.OBSERVATION_POLICY, **kwargs)
        validate(metrics)
        empty = dict(metrics, OLDEST_UNCONSUMED_AGE_US=None)
        validate(empty, empty_backlog_observations=1)
        for key in metrics:
            absent = dict(metrics); del absent[key]
            with self.assertRaisesRegex(ValueError, 'GUARD_EVIDENCE_CONFLICT'): validate(absent)
        for invalid in (None, -1, True, 2**63):
            with self.assertRaisesRegex(ValueError, 'GUARD_EVIDENCE_CONFLICT'):
                validate(dict(metrics, HOST_RSS_BYTES=invalid))
        with self.assertRaises(ValueError): validate(empty)
        with self.assertRaises(ValueError): validate(dict(metrics, AGGREGATE_FEATURE_BYTES=101))
        _, missing, _ = self.monitor_fixture(omitted=('AGGREGATE_FEATURE_BYTES',))
        with self.assertRaises(ValueError): validate(metrics, missing.policy)
        _, legacy, values = self.monitor_fixture(False)
        validate_metric_evidence(values, values, legacy.policy)
        with self.assertRaises(ValueError):
            validate_metric_evidence(dict(values, HOST_RSS_BYTES=101), values, legacy.policy)

    def test_profile_propagates_explicit_monitor_designation(self):
        import live_runtime_dry_profile_selftest_v0_1 as fixtures
        from live import runtime_dry_profile_v0_1 as profile_api
        refs = fixtures.references()
        with fixtures.accepted_public_fixture(refs) as accepted:
            original = fixtures.owned.fixture(self.root, 'profile-binding')
            try:
                inputs = fixtures.inputs_for(original, refs, accepted)
                inputs['source_start']['read_path'] = inputs['source_start']['database_identity']
                inputs['qualification_substitutions'] = {'scope': resource.SCOPE,
                    'synthetic_baseline_and_rpc': False, 'synthetic_source_binding': False,
                    'source_read_path': inputs['source_start']['read_path']}
                inputs['guard_amendment'] = None
                for metric in ('HOST_RSS_BYTES', 'STARTUP_US', 'PROTECTIVE_STEP_US'):
                    inputs['monitor']['resource_limits'].pop(metric, None)
                envelope = copy.deepcopy(self.envelope)
                envelope['derivation']['resource_limits'] = inputs['monitor']['resource_limits']
                inputs['resource_envelope'] = self.ref(envelope)
                reader = profile_api.read_reference
                target = accepted['target']
                def read_reference(reference):
                    if reference['path'] == target['public_configuration_path']:
                        return {'public_rpc': {'url': inputs['driver']['endpoint']}}
                    return reader(reference)
                with (patch.object(resource, 'validate', return_value=envelope),
                    patch.object(profile_api, 'read_reference', side_effect=read_reference)):
                    profile = profile_api.build_profile(inputs)
                    config = profile.configuration()['degradation_config']
                    self.assertEqual(config.observation_policy, resource.OBSERVATION_POLICY)
                    self.assertEqual(config.resource_max_age_us, inputs['monitor']['resource_max_age_us'])
                    self.assertEqual(config.policy.recovery_evidence_max_age_us, inputs['monitor']['recovery_evidence_max_age_us'])
            finally:
                fixtures.owned.fixtures.close(original)

    def test_first_launch_volume_denies_and_restart_precedes_capacity(self):
        from live.operations_supervisor_v0_1 import HealthProfile
        root = self.root/'host'; root.mkdir()
        envelope = copy.deepcopy(self.envelope)
        envelope['physical_evidence'] = self.ref({'environment': self.ref({'volumes': {}, 'requirements': {}})})
        control = dict(generation=1, nonce='nonce', process_identity='owner', stopped=False, exhausted=False)
        request = {'journal_root': str(root), 'resource_envelope': self.ref(envelope),
            'handoff': self.ref({'stores': [], 'operations_control': control})}
        profile = NS(configuration=lambda: {'domain': NS(economic_domain_id='a'*64), 'operations_path': str(root/'operations.db')})
        evidence = {'startup_audits': [{'owner': control}], 'terminals': []}
        with (patch.object(host, 'validate_request', return_value=(profile, {}, evidence, {}, HealthProfile(1, 1, 1))),
            patch.object(host, 'WindowsHostBoundary'), patch.object(host, 'OperationsStore') as store,
            patch('live.t010_resource_environment_v0_1.check_volume_reserves', side_effect=ValueError('LOW_VOLUME_RESERVE')) as reserve):
            store.return_value.snapshot.return_value = control
            with self.assertRaisesRegex(ValueError, 'LOW_VOLUME_RESERVE'):
                host.run(request, ROOT, launch_id='test', detached_required=False)
            reserve.assert_called_once(); self.assertFalse((root/'session.json').exists())
        # A stopped/expired existing session must take the original recovery path
        # before first-launch checks or current request validation.
        (root/'session.json').write_text('{}')
        with (patch.object(host, '_saved', return_value=({'lifetime': {}}, {})),
            patch.object(host, 'Lifetime') as lifetime, patch.object(host, 'stop') as stop,
            patch.object(host, 'validate_request') as validate,
            patch.object(resource, 'check_initial_capacity') as capacity):
            lifetime.return_value.reason.return_value = 'EXPIRED'
            self.assertEqual(host.run(request, ROOT, launch_id='test', detached_required=False), 0)
            stop.assert_called_once_with(root); validate.assert_not_called(); capacity.assert_not_called()

    def test_misassigned_scenario_evidence_rejected(self):
        evidence = self.observation_fixture()
        scenario = next(iter(evidence['scenarios']))
        value = json.loads(Path(evidence['scenarios'][scenario][0]['path']).read_text())
        value['scenario'] = next(key for key in evidence['scenarios'] if key != scenario)
        evidence['scenarios'][scenario][0] = self.ref(value)
        with patch('live.t010_resource_measurement_v0_1.runtime_code_digest', return_value='runtime'):
            with self.assertRaisesRegex(ValueError, 'OBSERVATION_RESULT_CONFLICT'):
                resource.validate_observations(self.ref(evidence))


def run_compatibility_checks():
    names = ('qualifier_observation_and_legacy_startup', 'health_contract_and_finite_cli_containment',
        'monitor_observation_coverage_freshness_and_identity', 'qualification_exact_observation_evidence',
        'profile_propagates_explicit_monitor_designation',
        'first_launch_volume_denies_and_restart_precedes_capacity', 'misassigned_scenario_evidence_rejected',
        'pending_and_recovery_precede_unknown_and_holds', 'numerical_guard_cannot_silently_downgrade')
    output = io.StringIO()
    result = unittest.TextTestRunner(stream=output).run(unittest.TestSuite(PolicyTests('test_'+name) for name in names))
    if not result.wasSuccessful(): raise AssertionError(output.getvalue())
    return result.testsRun


if __name__ == '__main__': unittest.main(verbosity=2)
