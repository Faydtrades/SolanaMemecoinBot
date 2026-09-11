"""C2.1 durable condition contract only; synthetic disposable stores, no I/O ports."""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from dataclasses import asdict, replace
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from live.operations_degradation_v0_1 import (
    CONDITIONS, ConditionEvidence, DegradationPolicy, DegradationStore, ResourceLimit,
    readiness_conditions, resource_condition, supervision_conditions,
)
from live.ledger_domain_v0_1 import LedgerDomain
from live.operations_readiness_v0_1 import ReadinessBarrier, ReadinessFacts
from live.operations_supervisor_v0_1 import SupervisionFacts
from live.authority_controls_v0_1 import TrustedClockSample

CHECKS = {}
D = lambda n: format(n, '064x')
DOMAIN = LedgerDomain('1'*32, '1'*32, D(1), 0)
POLICY = DegradationPolicy(D(2), 100, 20,
    (ResourceLimit('HOST_RSS_BYTES', 1000), ResourceLimit('HOST_DISK_RESERVE_BYTES', 200)))


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def fails(call, code=None):
    try:
        call()
    except (ValueError, RuntimeError) as exc:
        return code is None or str(exc) == code
    return False


def active(code='SEND_UNKNOWN', subject=3, observed=10, witness=4):
    return ConditionEvidence(code, D(subject), observed, D(witness))


def recovery(row, *, observed, witness=9, **changes):
    return replace(ConditionEvidence(row.condition, row.subject_digest, observed, D(witness),
        'RECOVERED', CONDITIONS[row.condition][1], row.active_digest), **changes)


def transitions(directory):
    path = directory/'conditions.sqlite'
    store = DegradationStore.initialize(path, DOMAIN, POLICY, now_us=0)
    initial = active()
    facts = store.record(initial, now_us=10)
    check('unknown_immediate_hold_delayed_alert', facts.entry_held and
        facts.affected_scopes == ('WALLET_MUTATION',) and not facts.conditions[0].alerted)
    check('facts_never_grant_permission', not facts.grants_permission and not facts.may_sign and not facts.may_send)
    check('exact_record_replay_idempotent', store.record(initial, now_us=11) == facts)
    store.advance(now_us=109)
    reopened = DegradationStore(path, DOMAIN, POLICY)
    check('reopen_retains_first_seen_and_delay', reopened.snapshot() == facts)
    facts = reopened.advance(now_us=110)
    check('explicit_interval_escalates', facts.conditions[0].alerted == 1 and facts.conditions[0].first_us == 10)
    check('time_never_releases_unknown', reopened.advance(now_us=10000).entry_held)
    second = active(observed=10001, witness=5)
    facts = reopened.record(second, now_us=10001)
    row = facts.conditions[0]
    check('fresh_fault_preserves_episode_and_age', row.episode == 1 and row.first_us == 10 and row.alerted == 1)
    stale = recovery(row, observed=10000)
    check('stale_recovery_rejected', fails(lambda: reopened.record(stale, now_us=10001)))
    old_witness = recovery(row, observed=10002, expected_active_digest=initial.content_digest)
    check('superseded_active_witness_rejected', fails(lambda: reopened.record(old_witness, now_us=10002)))
    same_witness = recovery(row, observed=10002, witness=5)
    check('active_evidence_is_not_positive_recovery', fails(lambda: reopened.record(same_witness, now_us=10002)))
    other_subject = recovery(row, observed=10002, subject_digest=D(77))
    check('mismatched_subject_cannot_clear', fails(lambda: reopened.record(other_subject, now_us=10002)))
    expired = recovery(row, observed=10002)
    check('expired_positive_evidence_cannot_clear', fails(lambda: reopened.record(expired, now_us=10023)))
    check('missing_observation_cannot_clear', reopened.advance(now_us=10024).entry_held)
    check('future_recovery_rejected', fails(lambda: reopened.record(recovery(row, observed=10030), now_us=10025)))
    check('rejected_recovery_unchanged', reopened.snapshot().conditions[0] == row)
    accepted = recovery(row, observed=10025)
    recovered = reopened.record(accepted, now_us=10025)
    check('fresh_exact_positive_recovery', not recovered.entry_held and
        recovered.conditions[0].state == 'RECOVERED' and recovered.conditions[0].recovered_us == 10025)
    check('old_active_replay_never_resurrects', reopened.record(initial, now_us=10026) == recovered)
    check('recovery_replay_idempotent', reopened.record(accepted, now_us=10026) == recovered)
    reopened = DegradationStore(path, DOMAIN, POLICY)
    check('recovered_state_durable', reopened.snapshot() == recovered)
    again = reopened.record(active(observed=10027, witness=10), now_us=10027)
    check('new_episode_has_own_alert_delay', again.entry_held and again.conditions[0].episode == 2
        and again.conditions[0].first_us == 10027 and not again.conditions[0].alerted)
    before = again.conditions[0]
    other = reopened.record(active(subject=44, observed=10028, witness=45), now_us=10028)
    check('new_subject_preserves_previous_incident', len(other.conditions) == 2 and before in other.conditions)
    changed_policy = replace(POLICY, persistent_unknown_alert_us=101)
    check('policy_reopen_cannot_restart_age', fails(lambda: DegradationStore(path, DOMAIN, changed_policy)))
    check('domain_binding_reopen_conflict', fails(lambda: DegradationStore(path,
        replace(DOMAIN, expected_profile_fingerprint=D(99)), POLICY)))
    check('older_independent_advance_preserves_snapshot', reopened.advance(now_us=9) == reopened.snapshot())
    with closing(sqlite3.connect(path)) as conn:
        check('sqlite_integrity', conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok')
        check('replay_deduplicates_receipts', conn.execute('SELECT count(*) FROM receipts').fetchone()[0] == 5)
        conn.execute('CREATE TABLE unrelated (payload TEXT)')
        conn.commit()
    check('schema_conflict_rejected', fails(reopened.snapshot))


def families_and_sanitization(directory):
    store = DegradationStore.initialize(directory/'families.sqlite', DOMAIN, POLICY, now_us=0)
    for index, code in enumerate(CONDITIONS, 1):
        row = store.record(active(code, subject=index, observed=index, witness=index+100), now_us=index)
    check('required_families_retained', len(row.conditions) == len(CONDITIONS))
    check('scope_specific_restrictions_present', {'ENTRY', 'ALL_MUTATION', 'ACTION',
        'CUSTODY_AND_FUNDING', 'WALLET_MUTATION'} <= set(row.affected_scopes))
    secret = 'https://provider.invalid/?token=PRIVATE_TEST_PAYLOAD'
    for field, value in (('condition', secret), ('reason', secret), ('subject_digest', secret),
            ('evidence_digest', secret), ('expected_active_digest', secret)):
        check('sanitized_reject_'+field, fails(lambda: replace(active(), **{field: value})))
    check('acknowledgement_not_recovery', fails(lambda: replace(active(),
        state='RECOVERED', reason='ACKNOWLEDGED', expected_active_digest=D(4))))
    check('no_ack_reset_or_owner_api', all(not hasattr(store, name)
        for name in ('acknowledge', 'reset', 'operator_reset', 'operator_stop', 'release')))
    serialized = json.dumps(asdict(row))
    check('only_fixed_codes_and_digests_serialized', secret not in serialized
        and 'https://' not in serialized and 'PRIVATE_TEST_PAYLOAD' not in serialized)
    check('raw_payload_not_persisted', secret.encode() not in store.path.read_bytes())
    receipt_path = directory/'receipt-conflict.sqlite'
    receipt_store = DegradationStore.initialize(receipt_path, DOMAIN, POLICY, now_us=0)
    receipt_store.record(active(), now_us=10)
    with closing(sqlite3.connect(receipt_path)) as conn:
        conn.execute("UPDATE conditions SET evidence_digest=?", (D(999),))
        conn.commit()
    check('state_receipt_binding_tamper_denied', fails(receipt_store.snapshot))
    path = directory/'damaged.sqlite'
    damaged = DegradationStore.initialize(path, DOMAIN, POLICY, now_us=0)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("UPDATE metadata SET last_us=-1")
        conn.commit()
    check('invalid_state_reopen_denied', fails(lambda: DegradationStore(path, DOMAIN, POLICY)))


def policy_and_projections():
    for field, value in (('persistent_unknown_alert_us', 0), ('persistent_unknown_alert_us', True),
            ('recovery_evidence_max_age_us', -1), ('resource_limits', [ResourceLimit('HOST_RSS_BYTES', 1)]),
            ('resource_limits', (ResourceLimit('HOST_RSS_BYTES', 1), ResourceLimit('HOST_RSS_BYTES', 2)))):
        check('policy_invalid_'+field+'_'+str(len(CHECKS)), fails(lambda: replace(POLICY, **{field: value})))
    check('metric_no_invented_limit', fails(lambda: ResourceLimit('UNSUPPORTED', 10)))
    check('metric_no_negative_limit', fails(lambda: ResourceLimit('HOST_RSS_BYTES', -1)))
    calc = lambda metric, value, config=D(2): resource_condition(POLICY, metric, value, configuration_digest=config)
    check('resources_explicit_reviewed_upper_limit', calc('HOST_RSS_BYTES', 1000) is None and
        calc('HOST_RSS_BYTES', 1001) == 'RESOURCE_EXCEEDED')
    check('disk_reserve_is_lower_limit', calc('HOST_DISK_RESERVE_BYTES', 199) == 'RESOURCE_EXCEEDED'
        and calc('HOST_DISK_RESERVE_BYTES', 200) is None)
    check('unsupported_absent_mismatched_resources_unresolved', all(value == 'PROFILE_UNRESOLVED' for value in (
        calc('STARTUP_US', 5), calc('HOST_RSS_BYTES', None), calc('HOST_RSS_BYTES', True),
        calc('HOST_RSS_BYTES', 1, D(55)), calc('UNSUPPORTED', 1))))
    check('empty_policy_has_no_defaults', resource_condition(replace(POLICY, resource_limits=()),
        'HOST_RSS_BYTES', 0, configuration_digest=D(2)) == 'PROFILE_UNRESOLVED')
    sample = TrustedClockSample('test', D(1), 'epoch', 0, '2026-01-01T00:00:00+00:00',
        '2026-01-01T00:00:00+00:00', D(2), D(3), 'QUALIFIED')
    ordinary = ReadinessFacts(sample, None, None,
        ReadinessBarrier('PROTECTIVE_READY', 'MONITORING', False,
            ('ACTUAL_STAGED_REDUCTION_AND_CURRENT_RESOURCES_REQUIRED',)),
        ReadinessBarrier('ENTRY_READY', 'HELD', False, ('CURRENT_CANONICAL_CANDIDATE_REQUIRED',
            'EXISTING_EXPOSURE_REQUIRES_PROTECTION_AND_RETIREMENT_FIRST', 'CURRENT_CAPACITY_OR_MUTATION_LANE_HELD')),
        (), None, None)
    check('ordinary_waiting_and_position_not_incidents', readiness_conditions(ordinary) == ())
    pending = replace(ordinary, protective=replace(ordinary.protective, state='TRUTH_REQUIRED',
        reasons=('ORIGINAL_PENDING_ATTEMPT_REQUIRES_TRUTH',), due=True))
    check('pending_attempt_alone_not_unknown_alert', readiness_conditions(pending) == ())
    broken = replace(ordinary, entry=replace(ordinary.entry, reasons=(
        'CURRENT_PRODUCER_CHECKPOINT_UNAVAILABLE', 'CLOCK_UNKNOWN', 'https://untrusted.invalid')),
        protective=replace(ordinary.protective, state='HELD', due=True))
    check('actual_readiness_projection_sanitized', readiness_conditions(broken) ==
        ('CLOCK_UNPROVEN', 'PRODUCER_INTEGRITY_UNAVAILABLE', 'PROTECTIVE_ACTION_UNAVAILABLE'))
    for state in ('STARTING', 'RUNNING', 'RESTART_BACKOFF', 'TERMINATING'):
        check('ordinary_supervisor_'+state, supervision_conditions(SupervisionFacts(state, None, None, 0, None)) == ())
    for state, code in (('OPERATOR_STOPPED', 'OPERATOR_STOPPED'), ('RESTART_EXHAUSTED', 'RESTART_EXHAUSTED'),
            ('HELD_EXISTING_OWNER', 'OWNER_FENCE_UNPROVEN'), ('HELD_CONTROL_UNREADABLE', 'SUPERVISOR_HELD')):
        check('supervisor_'+state, supervision_conditions(SupervisionFacts(state, None, None, 0, None)) == (code,))


def main():
    with tempfile.TemporaryDirectory(prefix='meme-live-c2-contract-') as temporary:
        directory = Path(temporary)
        transitions(directory)
        families_and_sanitization(directory)
        policy_and_projections()
    print(json.dumps({'version': 'operations_c2_1_contract_selftest_v0.1',
        'checks': CHECKS, 'check_count': len(CHECKS), 'all_checks_true': all(CHECKS.values())}, sort_keys=True))


if __name__ == '__main__':
    main()
