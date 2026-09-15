"""Disposable measurement-record contract tests; never public evidence."""
from __future__ import annotations
import copy
import hashlib
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
from live.t010_resource_measurement_v0_1 import (SCENARIOS, SCENARIO_METRICS, derive_guards, logical_digest, runtime_code_digest)
from live.t010_resource_envelope_v0_1 import derive
from live_t010_resource_envelope_selftest_v0_1 import fixture

CHECKS = {}
def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def fails(call):
    try:
        call()
    except (ValueError, KeyError, TypeError, OSError):
        return True
    return False


def main():
    derivation = derive(*fixture())
    monitor = {'resource_max_age_us': 1000000, 'recovery_evidence_max_age_us': 5000000}
    metrics = {'STARTUP_US': 5000000, 'HOST_RSS_BYTES': 200000000, 'PROTECTIVE_STEP_US': 3000000,
        'OLDEST_UNCONSUMED_AGE_US': 20000000, 'resource_max_age_us': 1500000,
        'recovery_evidence_max_age_us': 3000000, 'supervisor_startup_us': 6000000,
        'supervisor_progress_us': 7000000, 'supervisor_termination_us': 1000000,
        'child_private_bytes': 250000000, 'supervisor_private_bytes': 80000000, 'supervisor_rss_bytes': 90000000}
    code = runtime_code_digest()
    with tempfile.TemporaryDirectory(prefix='t010-measured-guard-contract-') as temporary:
        directory = Path(temporary)
        serial = 0
        def put(value):
            nonlocal serial
            serial += 1
            path = directory/(str(serial)+'.json')
            path.write_text(json.dumps(value, sort_keys=True))
            return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        families = {key: [put({'runtime_code_digest': code, 'native_digest': 'a'*64, 'exit_code': 0,
            'fresh_process': True, 'scenario': key, 'process_instance_digest': hashlib.sha256((key+str(i)).encode()).hexdigest(),
            'metrics': {k: metrics[k]+i for k in SCENARIO_METRICS[key]}}) for i in range(3)]
            for key in (*SCENARIOS, 'supervisor', 'public_response')}
        runs = families['supervisor']
        series = {k: [v+i for family in (*SCENARIOS, 'supervisor', 'public_response')
            if k in SCENARIO_METRICS[family] for i in range(3)] for k, v in metrics.items()}
        boundary = {'schema': 'MEME_LIVE_T010_FULL_PROCESS_MEASUREMENT_BOUNDARY_V1',
            'logical_digest': logical_digest(derivation), 'runtime_code_digest': code, 'native_digest': 'a'*64,
            'series': series, 'unresolved_constraints': [], 'scenarios': {key: families[key] for key in SCENARIOS},
            'supervisor_runs': runs, 'public_response_runs': families['public_response']}
        measured = {'schema': 'MEME_LIVE_T010_MEASURED_RESOURCE_GUARDS_V1',
            'scope': 'T010_DRY_NO_BROADCAST_NON_SUBMITTED', 'logical_digest': logical_digest(derivation),
            'runtime_code_digest': code, 'native_digest': 'a'*64, 'measurement_boundary': put(boundary),
            'series': series, 'units': {k: 4096 if k.endswith(('BYTES', 'bytes')) else 1 for k in metrics},
            'unchanged_safety': {'source_rows': 10000, 'source_freshness_seconds': 30,
                'clock_policy': 'EXACT_OWNER_APPROVED_T010_RECORD', 'entry_deadlines': 'ORIGINAL',
                'lifetime_us': 50400000000, 'reserve_floor': 'ORIGINAL'},
            'limitations': ['TEST_ONLY_DISPOSABLE_RECORDS_NEVER_PUBLIC_EVIDENCE']}
        guards = derive_guards(measured, derivation, monitor)
        check('full_cohort_range_derives_startup', guards['STARTUP_US'] == 5000004)
        check('sufficient_original_backlog_age_unchanged', guards['OLDEST_UNCONSUMED_AGE_US'] == 30000000)
        check('sufficient_original_recovery_age_unchanged', guards['recovery_evidence_max_age_us'] == 5000000)
        check('all_guard_dimensions_derived_together', set(guards) == set(metrics))
        def altered(record=None, document=None):
            value = copy.deepcopy(measured)
            if record:
                record(value)
            if document:
                new = copy.deepcopy(boundary); document(new); value['measurement_boundary'] = put(new)
            return fails(lambda: derive_guards(value, derivation, monitor))
        check('missing_scenario_denied', altered(document=lambda x: x['scenarios'].pop(SCENARIOS[0])))
        check('one_supervisor_repeat_denied', altered(document=lambda x: x.update(supervisor_runs=runs[:1])))
        check('same_process_cannot_count_as_three_runs', altered(document=lambda x: x.update(supervisor_runs=[runs[0]]*3)))
        sparse = json.loads(Path(families[SCENARIOS[0]][0]['path']).read_text())
        sparse['metrics'] = {'STARTUP_US': sparse['metrics']['STARTUP_US']}
        sparse_ref = put(sparse)
        check('structural_family_cannot_omit_memory_and_work_metrics', altered(
            document=lambda x: x['scenarios'][SCENARIOS[0]].__setitem__(0, sparse_ref)))
        check('unresolved_measurement_denied', altered(document=lambda x: x.update(unresolved_constraints=['unfinished'])))
        check('runtime_change_invalidates_calibration', altered(record=lambda x: x.update(runtime_code_digest='b'*64)))
        check('work_change_invalidates_calibration', altered(record=lambda x: x.update(logical_digest='b'*64)))
        check('source_freshness_relaxation_denied', altered(record=lambda x: x['unchanged_safety'].update(source_freshness_seconds=31)))
        check('clock_policy_relaxation_denied', altered(record=lambda x: x['unchanged_safety'].update(clock_policy='OTHER')))
        check('arbitrary_round_number_unit_denied', altered(record=lambda x: x['units'].update(HOST_RSS_BYTES=100000000)))
        check('series_cannot_replace_retained_metrics', altered(record=lambda x: x['series']['STARTUP_US'].__setitem__(0, 1)))
        changed = copy.deepcopy(boundary)
        changed['series']['STARTUP_US'][0] = 1
        wrong = copy.deepcopy(measured); wrong['series'] = changed['series']; wrong['measurement_boundary'] = put(changed)
        check('matching_summary_still_requires_original_runs', fails(lambda: derive_guards(wrong, derivation, monitor)))
    print(json.dumps({'checks': CHECKS, 'count': len(CHECKS), 'public_evidence': False}, sort_keys=True))


if __name__ == '__main__':
    main()
