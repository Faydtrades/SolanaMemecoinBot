"""Exact proof pin and measured-rounding boundaries; no runtime/public campaign."""
from __future__ import annotations
import copy
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
from live.t010_physical_certificate_v0_1 import validate_model, CERTIFICATE_SHA256
from live.t010_resource_measurement_v0_1 import repeated_guard, logical_digest
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
    path = ROOT/'docs/live/MEME_LIVE_T010_PHYSICAL_PROOF_CERTIFICATE_V7.json'
    proof = json.loads(path.read_text())
    reference = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    check('exact_certificate_pin', reference['sha256'] == CERTIFICATE_SHA256)
    derivation = derive(*fixture())
    model = {'scope': 'ALL_GROWABLE_STORES_PHYSICAL_UPPER_BOUND', 'derivation_digest': 'a'*64,
        'logical_digest': logical_digest(derivation), 'certificate': reference}
    environment = {key: proof[key] for key in ('native', 'connections', 'owned_peak_bytes', 'recovery_bytes',
        'temporary_peak_bytes', 'reserve_floor_bytes')}
    check('current_sources_exact_closed_proof', validate_model(model, derivation, environment) == proof)
    for key in ('owned_peak_bytes', 'recovery_bytes', 'temporary_peak_bytes', 'reserve_floor_bytes'):
        wrong = copy.deepcopy(environment); wrong[key] -= 1
        check(key+'_one_byte_reduction_denied', fails(lambda: validate_model(model, derivation, wrong)))
    changed = copy.deepcopy(derivation); changed['work_budget']['max_startups'] += 1
    check('one_additional_startup_invalidates_model', fails(lambda: validate_model(model, changed, environment)))
    changed = copy.deepcopy(derivation); changed['producer_profile']['pending_rows'] += 1
    check('one_additional_pending_row_invalidates_model', fails(lambda: validate_model(model, changed, environment)))
    wrong = copy.deepcopy(environment); wrong['native']['sqlite_dll_sha256'] = 'a'*64
    check('engine_substitution_denied', fails(lambda: validate_model(model, derivation, wrong)))
    wrong = copy.deepcopy(environment); wrong['connections']['producer']['schema_digest'] = 'a'*64
    check('schema_substitution_denied', fails(lambda: validate_model(model, derivation, wrong)))
    wrong = copy.deepcopy(model); wrong['certificate']['sha256'] = 'b'*64
    check('self_declared_alternate_proof_denied', fails(lambda: validate_model(wrong, derivation, environment)))
    check('timing_adds_observed_range', repeated_guard([100, 120, 110], 1) == 140)
    check('no_variation_adds_one_native_tick', repeated_guard([100, 100, 100], 1) == 101)
    check('memory_rounds_up_native_page_only', repeated_guard([4096, 5000, 7000], 4096) == 12288)
    check('one_run_cannot_set_guard', fails(lambda: repeated_guard([100], 1)))
    check('boolean_cannot_set_guard', fails(lambda: repeated_guard([100, 120, True], 1)))
    check('negative_cannot_set_guard', fails(lambda: repeated_guard([100, 120, -1], 1)))
    check('zero_unit_denied', fails(lambda: repeated_guard([100, 120, 130], 0)))
    print(json.dumps({'checks': CHECKS, 'count': len(CHECKS), 'network': False, 'production_data': False}, sort_keys=True))


if __name__ == '__main__':
    main()
