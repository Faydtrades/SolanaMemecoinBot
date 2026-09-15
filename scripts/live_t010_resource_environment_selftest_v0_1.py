"""Two-volume boundary and pending-recovery checks on disposable local files."""
from __future__ import annotations
import copy
import hashlib
import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
from live import t010_resource_environment_v0_1 as environment
from live import t010_resource_envelope_v0_1 as resources
from live_t010_resource_envelope_selftest_v0_1 import fixture

CHECKS = {}
def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def fails(call):
    try:
        call()
    except (ValueError, OSError, TypeError, KeyError):
        return True
    return False


def main():
    bindings = {role: {'path': role, 'volume_id': identity, 'allocation_unit_bytes': 4096}
        for role, identity in (('owned', 'D'), ('temporary', 'C'))}
    amounts = (100, 100, 30, 10, 3)
    limits = environment.volume_requirements(bindings, *amounts)
    check('separate_volume_peaks_and_floor', limits == {'D': 213, 'C': 40})
    together = copy.deepcopy(bindings); together['temporary']['volume_id'] = 'D'
    check('shared_volume_sums_simultaneous_claims', environment.volume_requirements(together, *amounts) == {'D': 243})
    actual = {item['path']: dict(item, free_bytes=limits[item['volume_id']]) for item in bindings.values()}
    observed = lambda path: actual[path]
    check('exact_reserve_boundary', bool(environment.check_volume_reserves(bindings, limits, observe=observed)))
    for role in bindings:
        actual[role]['free_bytes'] -= 1
        check(role+'_one_byte_below_denied', fails(lambda: environment.check_volume_reserves(bindings, limits, observe=observed)))
        actual[role]['free_bytes'] += 1
        actual[role]['volume_id'] = 'changed'
        check(role+'_volume_substitution_denied', fails(lambda: environment.check_volume_reserves(bindings, limits, observe=observed)))
        actual[role]['volume_id'] = bindings[role]['volume_id']
        actual[role]['allocation_unit_bytes'] = 65536
        check(role+'_allocation_change_denied', fails(lambda: environment.check_volume_reserves(bindings, limits, observe=observed)))
        actual[role]['allocation_unit_bytes'] = 4096
    check('unavailable_volume_denied', fails(lambda: environment.check_volume_reserves(bindings, limits,
        observe=lambda _: (_ for _ in ()).throw(OSError('unavailable')))))
    for index in range(5):
        invalid = list(amounts); invalid[index] = True
        check('boolean_quantity_'+str(index)+'_denied', fails(lambda: environment.volume_requirements(bindings, *invalid)))
    guards = {'HOST_RSS_BYTES': 100, 'supervisor_rss_bytes': 20,
        'child_private_bytes': 200, 'supervisor_private_bytes': 30}
    memory = {'physical_available_bytes': 120, 'commit_available_bytes': 230}
    check('exact_recovery_memory_boundary', bool(environment.check_memory_reserve(guards, observe=lambda: memory)))
    for key in memory:
        memory[key] -= 1
        check(key+'_one_byte_below_denied', fails(lambda: environment.check_memory_reserve(guards, observe=lambda: memory)))
        memory[key] += 1
    with tempfile.TemporaryDirectory(prefix='t010-volume-boundary-') as temporary:
        directory = Path(temporary)
        with closing(sqlite3.connect(directory/'native-main.sqlite')) as connection:
            connection.execute('PRAGMA journal_mode=WAL')
            connection.execute('CREATE TABLE retained(value INTEGER)')
            connection.commit()
            before = environment._connection_facts({'ledger': connection})
            check('original_main_connection_native_facts', bool(before['ledger']['schema_digest']))
            connection.execute('SELECT * FROM temp.sqlite_schema').fetchall()
            check('original_empty_builtin_temp_preserves_facts', environment._connection_facts({'ledger': connection}) == before)
            connection.execute('CREATE TEMP TABLE outside_proof(value INTEGER)')
            check('named_temp_table_denied', fails(lambda: environment._connection_facts({'ledger': connection})))
            connection.execute('DROP TABLE temp.outside_proof')
            connection.execute("ATTACH DATABASE ':memory:' AS outside_proof")
            check('independent_attached_database_denied', fails(lambda: environment._connection_facts({'ledger': connection})))
        immutable = directory/'input.json'
        immutable.write_bytes(b'{"retained":true}')
        reference = {'path': str(immutable), 'sha256': hashlib.sha256(immutable.read_bytes()).hexdigest()}
        with patch.object(environment, 'volume_observation', return_value=actual['owned']):
            inputs = environment.existing_input_files([reference, reference])
            check('existing_input_deduplicated_actual_allocation', len(inputs) == 1
                and inputs[0]['logical_bytes'] == immutable.stat().st_size
                and inputs[0]['allocation_upper_bound_bytes'] == 4096 and inputs[0]['volume_id'] == 'D')
            immutable.write_bytes(b'{"retained":false}')
            check('changed_immutable_input_denied', fails(lambda: environment.existing_input_files([reference])))
        native = {'temporary_directory': str(Path('temporary').resolve())}
        structural = {'schema': 'MEME_LIVE_T010_RESOURCE_ENVIRONMENT_V1', 'native': native,
            'connections': {name: {'path': str(directory/(name+'.db'))} for name in environment.STORE_NAMES},
            'volumes': bindings, 'owned_peak_bytes': 100, 'recovery_bytes': 100, 'temporary_peak_bytes': 30,
            'reserve_floor_bytes': 10, 'immutable_input_bytes': 0, 'immutable_inputs': [],
            'requirements': environment.volume_requirements(bindings, 100, 100, 30, 10, 0)}
        with (patch.object(environment, 'native_identity', return_value=native),
                patch.object(environment, 'volume_observation', return_value=actual['owned']),
                patch.object(environment, 'check_volume_reserves', side_effect=ValueError('low reserve')) as reserve):
            check('structural_binding_allows_reconstruction_on_low_reserve',
                environment.validate_binding(structural, check_reserve=False) == {} and reserve.call_count == 0)
            check('fresh_capacity_still_denies_low_reserve', fails(lambda: environment.validate_binding(structural)))
            fabricated = copy.deepcopy(structural); fabricated['immutable_input_bytes'] = 1
            check('caller_selected_immutable_growth_denied', fails(lambda: environment.validate_binding(fabricated)))
        physical = {'environment': 'environment'}
        references = {'physical': physical, 'environment': structural}
        physical_envelope = {'physical_evidence': 'physical', 'derivation': {'measured_guards': guards}}
        with (patch('live.runtime_dry_profile_v0_1.read_reference', side_effect=lambda key: references[key]),
                patch.object(environment, 'volume_observation', return_value=actual['owned']) as volume):
            check('journal_on_charged_volume_allowed', bool(resources.check_host_journal_volume(physical_envelope, directory)))
            volume.return_value = actual['temporary']
            check('journal_on_uncharged_volume_denied', fails(lambda: resources.check_host_journal_volume(physical_envelope, directory)))
            with patch.object(environment, 'check_volume_reserves', side_effect=ValueError('low reserve')):
                check('initial_capacity_denies_low_disk', fails(lambda: resources.check_initial_capacity(physical_envelope)))
            with (patch.object(environment, 'check_volume_reserves', return_value={}),
                    patch.object(environment, 'check_memory_reserve', side_effect=ValueError('low memory'))):
                check('initial_capacity_denies_low_memory', fails(lambda: resources.check_initial_capacity(physical_envelope)))
        envelope = {'derivation': resources.derive(*fixture())}
        gate = resources.ResourceGate(Path(temporary)/'resource.json', envelope, 'host', create=True)
        gate.environment = {'volumes': bindings, 'requirements': limits}
        runtime = SimpleNamespace(capability='NO_BROADCAST', _dry_recovery=False,
            ledger=SimpleNamespace(_custody=SimpleNamespace(reservations=[])))
        started = SimpleNamespace(runtime=runtime)
        with (patch.object(environment, 'check_volume_reserves', side_effect=ValueError('low C reserve')) as observe,
                patch.object(gate, '_checkpoint_boundary', side_effect=AssertionError('checkpoint before reserve'))):
            check('ordinary_source_held_before_checkpoint', not gate.before_step(started))
            state = gate.snapshot()
            check('no_source_reservation_on_low_disk', state['source_rows_reserved'] == state['source_units_reserved'] == 0)
            check('durable_physical_hold', state['held_reason'] == 'PHYSICAL_VOLUME_RESERVE_UNPROVEN')
            runtime.ledger._custody.reservations.append(SimpleNamespace(retired_sequence=None))
            check('pending_recovery_before_low_disk', gate.before_step(started) and runtime._resource_batch_rows == 0)
            check('pending_does_not_query_or_mutate_disk', observe.call_count == 1)
            runtime.ledger._custody.reservations.clear(); runtime._dry_recovery = True
            check('reconstructed_dry_recovery_priority', gate.before_step(started))
            runtime._dry_recovery = False
            check('ordinary_work_cannot_rearm', not gate.before_step(started))
    print(json.dumps({'checks': CHECKS, 'count': len(CHECKS), 'network': False, 'production_data': False}, sort_keys=True))


if __name__ == '__main__':
    main()
