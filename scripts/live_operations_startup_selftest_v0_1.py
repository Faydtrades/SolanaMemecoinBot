"""A2 actual owned cold startup; disposable fixtures and fake external I/O."""
from __future__ import annotations

import copy
import json
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import closing
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_runtime_reconstruction_selftest_v0_1 as c4
from live import operations_startup_v0_1 as startup
from live.operations_ownership_v0_1 import OperationsStore, RestartProfile
from live.runtime_reconstruction_v0_1 import ColdRuntimeV01
from live.operations_degradation_v0_1 import DegradationPolicy, DegradationStore, ResourceLimit, METRICS
from live.operations_degradation_monitor_v0_1 import (
    MonitorConfiguration, HostObservations, HostMetric, content_fingerprint, monitor_configuration_digest,
)

rt, a3, sf, NOW = c4.c2, c4.a3, c4.sf, c4.NOW
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def fails(call):
    try:
        call()
    except (ValueError, RuntimeError, TypeError, sqlite3.DatabaseError, OSError):
        return True
    return False


def installed(f, *, profile=RestartProfile(100, 1000000, 0)):
    f.operations_path = f.directory/(f.name+'-operations.sqlite')
    f.operations = OperationsStore.initialize(f.operations_path, f.domain, profile, now_us=0)
    args = c4.cold_args(f)
    f.expected = startup.configured_identity(f.domain, operations_path=f.operations_path,
        ledger_path=f.path, **args, producer_schema_digest=startup.schema_fingerprint(f.ppath),
        evidence_schema_digest=startup.schema_fingerprint(args['source_path']), restart_profile=profile)
    f.started = None
    return f


def healthy_monitor_config(f):
    if hasattr(f, 'startup_monitor_config'):
        return f.startup_monitor_config
    # Explicit synthetic C2 prerequisites for these healthy owned-runtime fixtures.
    digest = lambda n: format(n, '064x')
    alert_path = str(f.directory/(f.name+'-startup-alerts.sqlite'))
    reviewed = monitor_configuration_digest(f.expected, path=alert_path, host_identity_digest=digest(91),
        resource_max_age_us=1000000, protective_qualification_digest=digest(92))
    limits = tuple(ResourceLimit(metric, 10 if metric == 'HOST_DISK_RESERVE_BYTES' else 10**12)
        for metric in sorted(METRICS))
    policy = DegradationPolicy(reviewed, 100, 1000000, limits)
    f.startup_monitor_config = MonitorConfiguration(alert_path, policy, digest(91),
        f.expected.runtime_code_digest, 1000000, digest(92))
    DegradationStore.initialize(alert_path, f.domain, policy, now_us=0)
    return f.startup_monitor_config


def host(f, at=NOW+4):
    config = f.startup_monitor_config
    metrics = tuple(HostMetric(metric, 100, at*1000000,
        config.protective_qualification_digest if metric == 'PROTECTIVE_STEP_US' else format(93, '064x'))
        for metric in ('HOST_RSS_BYTES', 'HOST_DISK_RESERVE_BYTES', 'STARTUP_US', 'PROTECTIVE_STEP_US'))
    return HostObservations(config.host_identity_digest, config.policy.reviewed_configuration_digest,
        content_fingerprint(asdict(f.runtime._ownership.fence)), metrics)


def close(f):
    if f.started is not None:
        f.started.close()
        f.started = None
    else:
        c4.close_handles(f)


def restart(f, *, damage=None, expected=None, startup_now_us=None, **overrides):
    close(f)
    if damage:
        damage()
    args = c4.cold_args(f)
    if 'degradation_config' not in overrides:
        args['degradation_config'] = healthy_monitor_config(f)
        if getattr(f.step, '__func__', None) is rt.Fixture.step:
            original_step = f.step
            def owned_step(at=NOW+4, **kwargs):
                kwargs.setdefault('operations_resources', lambda: host(f, at))
                return original_step(at, **kwargs)
            f.step = owned_step
    args.update(overrides)
    control = f.operations.snapshot()
    f.started = startup.start_live(f.operations_path, f.path, f.domain,
        process_identity=f.name+'-child', now_us=control['last_control_us']+1 if startup_now_us is None else startup_now_us,
        expected_identity=f.expected if expected is None else expected,
        replace_generation=control['generation'] if control['generation'] else None, **args)
    f.runtime = f.started.runtime
    f.repo, f.producer, f.handoff, f.source = f.runtime.ledger, f.runtime.producer, f.runtime.handoff, f.runtime.source
    f.conn = f.runtime._producer_conn
    return f.started.audit


def sql(path, statement, args=()):
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute(statement, args)


def change_immutable(path, trigger, statement, args):
    # Deliberate isolated identity corruption, with the original schema restored.
    with closing(sqlite3.connect(path)) as conn, conn:
        ddl = conn.execute('SELECT sql FROM sqlite_master WHERE name=?', (trigger,)).fetchone()[0]
        conn.execute('DROP TRIGGER '+trigger)
        conn.execute(statement, args)
        conn.execute(ddl)


def normal(directory):
    f = installed(rt.Fixture(directory, 'a2-normal'))
    try:
        events = []
        acquire = OperationsStore.acquire
        ledger_audit = startup.repository._schema_and_domain
        economics = ColdRuntimeV01._restore_economics
        source_audit = startup._source_preflight
        def acquiring(*args, **kwargs):
            result = acquire(*args, **kwargs)
            events.append('owned')
            return result
        def ledger(*args):
            result = ledger_audit(*args)
            events.append('ledger-schema')
            return result
        def economic(*args):
            result = economics(*args)
            events.append('economics-controllers')
            return result
        def sources(*args):
            events.append('source-audit')
            return source_audit(*args)
        with patch.object(OperationsStore, 'acquire', acquiring), patch.object(startup.repository, '_schema_and_domain', ledger), \
             patch.object(ColdRuntimeV01, '_restore_economics', economic), patch.object(startup, '_source_preflight', sources):
            audit = restart(f)
        facts = audit.reconstruction
        check('startup_actual_order_owner_audit_economics_controllers_source',
            events.index('owned') < events.index('ledger-schema') < events.index('economics-controllers') < events.index('source-audit'))
        check('startup_actual_owned_cold_type_and_fence', type(f.runtime) is ColdRuntimeV01
            and f.runtime._ownership is not None and f.runtime._ownership.fence == audit.owner_fence)
        check('startup_no_position_original_funding_no_permission', facts.position_id is None
            and facts.funding.native_lamports == f.domain.known_native_wallet_lamports
            and not audit.grants_permission and not facts.grants_permission)
        check('startup_exact_identity_and_source_audit', audit.identity == f.expected
            and audit.source_audit == 'SOURCE_PREFLIGHT_VERIFIED' and facts.source_state == 'SOURCE_RECONSTRUCTED')
        root = f.candidate_ready().root_id
        audit = restart(f)
        check('startup_pending_original_candidate_actual_owned_consumer', audit.reconstruction.queued_roots == (root,)
            and f.step().root_id == root and f.runtime._ownership.fence == audit.owner_fence)
        f.configure()
        original = f.repo._authority
        audit = restart(f)
        check('startup_original_armed_policy_grant_restored', audit.authority == original
            and len(audit.relevant_grants) == 1 and audit.relevant_grants[0].issuance == original.grant
            and audit.relevant_grants[0].currently_selected and not audit.relevant_grants[0].grants_message_permission)
        previous = f.runtime._ownership
        replacement = f.operations.acquire('replacement', now_us=100, replace_generation=previous.fence.generation)
        check('startup_root_stale_fence_denies_actual_step', f.step().work == 'OPERATIONS_HELD')
        replacement.close()
        audit = restart(f)
        a3.control(f.repo, 'HARD_STOP', 'a2-hard-stop', at=NOW+5)
        retained = f.repo._authority
        audit = restart(f)
        check('startup_retains_original_hardstop_policy_no_rearming', audit.authority == retained
            and audit.authority.hard_stop_command == 'a2-hard-stop' and audit.authority.grant is None)
        f.operations.operator_stop(now_us=200)
        close(f)
        with patch.object(startup.cold, 'reopen_live', wraps=startup.cold.reopen_live) as factory:
            check('startup_operator_stop_denies_before_cold_factory', fails(lambda: restart(f)) and factory.call_count == 0)
        check('startup_stop_latched_and_authority_unchanged', f.operations.snapshot()['stopped'] == 1)
    finally:
        close(f)


def lifecycle(directory):
    key = rt.Keypair()
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = installed(rt.Fixture(directory, 'a2-lifecycle'))
        try:
            f.admit()
            original = f.buy
            audit = restart(f)
            check('startup_admitted_original_action_and_grant_status', audit.reconstruction.entry_action_id == original.action_id
                and 'ADMITTED_ACTION' in audit.unresolved and f.step(NOW+5).work == 'NEED_EXECUTION'
                and audit.relevant_grants[0] == startup.RetainedGrantFacts(**f.repo.authority_grant_status(
                    f.repo.authority_acceptance(original.root_id).eligibility.decision.grant_id)))
            submitted, _, _ = rt.execution(f, key, original, at=NOW+6, number=111)
            _, actual, scenario, pre = rt.original_chain(f, submitted, NOW+6)
            reconciled, _ = rt.reconcile(f, scenario, NOW+12)
            support = rt.application_support(f, actual, f.repo.chain_receipt(reconciled.receipt_key), pre, NOW+14)
            f.step(NOW+14, application_wallet=support)
            binding = f.runtime.position_binding
            audit = restart(f)
            check('startup_open_original_economics_and_controller', audit.reconstruction.remaining_units == actual.actual_base
                and f.runtime.position_binding == binding and f.runtime._ownership.fence == audit.owner_fence)
            f.step(NOW+14)
            evaluation = c4.commit_exit_evaluation(f.repo, binding, command_id='a2-due', timer_fence_utc=a3.utc(NOW+16))
            obligation = c4.ensure_protective_obligation(f.repo, binding, recorded_at_utc=a3.utc(NOW+16))
            partial = c4.stage_protective_sell(f.repo, binding, max_units=actual.actual_base//3)
            audit = restart(f)
            check('startup_original_due_cut_obligation_before_entry', audit.reconstruction.exit_cut_digest == evaluation.content_digest
                and audit.reconstruction.obligation_id == obligation.obligation_id and f.step(NOW+17).action_id == partial.action_id)
            submitted, _, _ = rt.execution(f, key, partial, at=NOW+20, number=112)
            _, sold, scenario, pre = rt.original_chain(f, submitted, NOW+20)
            reconciled, _ = rt.reconcile(f, scenario, NOW+26)
            chain = f.repo.chain_receipt(reconciled.receipt_key)
            support = rt.application_support(f, sold, chain, pre, NOW+28)
            retained_attempt = f.repo.attempt(submitted.attempt_id)
            audit = restart(f)
            check('startup_original_pending_finality_application_classified', audit.pending_attempt.preparation.attempt_id == submitted.attempt_id
                and audit.pending_attempt == retained_attempt
                and audit.reconstruction.chain_receipt_key == chain.ingestion_key and f.step(NOW+28).work == 'NEED_APPLICATION')
            f.step(NOW+28, application_wallet=support)
            audit = restart(f)
            remaining = actual.actual_base-partial.input_units
            check('startup_partial_residual_uses_original_fact', audit.reconstruction.remaining_units == remaining
                and 'PROTECTION:STAGE_ACTION' in audit.unresolved and audit.pending_attempt is None)
            staged = f.step(NOW+29)
            action = f.repo.action(staged.action_id)
            submitted, _, _ = rt.execution(f, key, action, at=NOW+32, number=113, outcome='TRANSPORT_UNRESOLVED')
            _, sold, scenario, pre = rt.original_chain(f, submitted, NOW+32)
            unknown = copy.deepcopy(scenario)
            unknown.tx = unknown.status = None
            rt.reconcile(f, unknown, NOW+38)
            original_attempt = f.repo.attempt(submitted.attempt_id)
            audit = restart(f)
            check('startup_UNKNOWN_retains_exact_original_attempt_and_capacity', audit.pending_attempt == original_attempt
                and audit.pending_attempt.lane_held and audit.reconstruction.remaining_units == remaining
                and any('UNKNOWN' in value for value in audit.unresolved))
            repeated = restart(f, startup_now_us=900000000)
            check('startup_repeated_elapsed_restart_cannot_resolve_UNKNOWN', repeated.pending_attempt == original_attempt
                and repeated.unresolved == audit.unresolved and f.step(NOW+39).work == 'NEED_RECONCILIATION')
            reconciled, _ = rt.reconcile(f, scenario, NOW+39)
            support = rt.application_support(f, sold, f.repo.chain_receipt(reconciled.receipt_key), pre, NOW+40)
            applied = f.step(NOW+40, application_wallet=support)
            check('startup_UNKNOWN_resolved_only_by_original_positive_evidence_application', applied.reason == 'FINALIZED_SUCCESS_APPLIED')
            audit = restart(f)
            check('startup_satisfied_is_original_fact_not_capacity_release', audit.reconstruction.protective_state == 'SATISFIED'
                and audit.reconstruction.remaining_units == 0 and f.step(NOW+41).work == 'NEED_RETIREMENT')
            grant_id = audit.relevant_grants[0].issuance.grant_id
            a3.control(f.repo, 'REVOKE_GRANT', 'a2-revoke-original', at=NOW+42, target=grant_id)
            audit = restart(f)
            check('startup_retains_original_revoked_admission_grant', audit.relevant_grants[0].revoked
                and audit.relevant_grants[0].issuance.grant_id == grant_id
                and not audit.relevant_grants[0].grants_message_permission)
        finally:
            close(f)


def source_failure(directory):
    key = rt.Keypair()
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = installed(rt.Fixture(directory, 'a2-source-failure'))
        try:
            rt.acquisition(f, key)
            f.step(NOW+14)
            binding = f.runtime.position_binding
            audit = restart(f, damage=lambda: sql(f.ppath,
                'UPDATE live_producer_checkpoint_v0_1 SET manifest_digest=?', ('0'*64,)))
            check('startup_actual_source_reconstruction_failure_preserves_protection',
                audit.source_audit == 'SOURCE_PREFLIGHT_VERIFIED' and 'SOURCE_UNAVAILABLE' in audit.unresolved
                and f.runtime.position_binding == binding and audit.reconstruction.protective_state == 'MONITORING'
                and f.runtime.producer is None and f.runtime.source is None and f.runtime._ownership is not None)
            check('startup_degraded_root_actual_due_protection_still_consumed', f.step(NOW+16).work == 'PROTECTIVE_ACTION_STAGED')
            audit = restart(f, damage=lambda: sql(f.ppath, 'CREATE TABLE incompatible_extra(x)'))
            check('startup_schema_preflight_failure_preserves_original_due_obligation', audit.source_audit == 'SOURCE_PREFLIGHT_FAILED'
                and audit.reconstruction.obligation_id is not None and 'SOURCE_UNAVAILABLE' in audit.unresolved
                and f.step(NOW+17).work == 'NEED_EXECUTION')
            f.operations.operator_stop(now_us=100)
            check('startup_source_failure_does_not_bypass_stop_fence', f.step(NOW+18).work == 'OPERATIONS_HELD')
        finally:
            close(f)


def failures(directory):
    for name, change in (
        ('runtime', lambda e: replace(e, runtime_version='incompatible')),
        ('runtime-code', lambda e: replace(e, runtime_code_digest='0'*64)),
        ('restart-profile', lambda e: replace(e, restart_profile=RestartProfile(99, 1000000, 0))),
        ('capability', lambda e: replace(e, capability='DRY')),
        ('contract', lambda e: replace(e, contracts=e.contracts+(('other', 'version'),))),
        ('config', lambda e: replace(e, configuration_digest='0'*64)),
    ):
        f = installed(rt.Fixture(directory, 'a2-'+name))
        try:
            close(f)
            with patch.object(startup.cold, 'reopen_live', wraps=startup.cold.reopen_live) as factory:
                check('startup_'+name+'_mismatch_before_reconstruction', fails(lambda: restart(f, expected=change(f.expected)))
                    and factory.call_count == 0 and f.operations.snapshot()['attempts'] == 1)
        finally:
            close(f)
    for name in ('missing-ledger', 'corrupt-ledger', 'ledger-schema', 'ledger-domain', 'corrupt-projection',
                 'missing-source', 'source-schema', 'source-domain', 'source-profile', 'producer-domain', 'producer-profile',
                 'missing-producer', 'corrupt-source'):
        f = installed(rt.Fixture(directory, 'a2-'+name))
        try:
            close(f)
            args = c4.cold_args(f)
            if name == 'missing-ledger': f.path.unlink()
            elif name == 'corrupt-ledger': f.path.write_bytes(b'not a SQLite journal')
            elif name == 'ledger-schema': sql(f.path, 'PRAGMA user_version=999')
            elif name == 'ledger-domain':
                change_immutable(f.path, 'ledger_domain_no_update', 'UPDATE ledger_domain SET binding_digest=?', ('0'*64,))
            elif name == 'corrupt-projection': sql(f.path, 'UPDATE ledger_funding_projection SET content_digest=?', ('0'*64,))
            elif name == 'missing-source': args['source_path'].unlink()
            elif name == 'source-schema': sql(args['source_path'], 'DROP TRIGGER source_domain_no_update')
            elif name == 'source-domain':
                change_immutable(args['source_path'], 'source_domain_no_update', 'UPDATE source_domain SET binding_json=?', ('{}',))
            elif name == 'source-profile':
                args['source_profile'] = replace(args['source_profile'], freshness_seconds=31)
                f.expected = replace(f.expected, configuration_digest=startup.configured_identity(f.domain,
                    operations_path=f.operations_path, ledger_path=f.path, **args,
                    producer_schema_digest=f.expected.producer_schema_digest,
                    evidence_schema_digest=f.expected.evidence_schema_digest, restart_profile=f.expected.restart_profile).configuration_digest)
            elif name == 'producer-domain':
                with closing(sqlite3.connect(f.ppath)) as conn:
                    payload = json.loads(conn.execute('SELECT payload FROM live_candidate_handoff_config_v0_1').fetchone()[0])
                payload['economic_domain_id'] = '0'*64
                change_immutable(f.ppath, 'live_candidate_handoff_config_v0_1_no_update',
                    'UPDATE live_candidate_handoff_config_v0_1 SET payload=?', (startup.handoff.accepted._json(payload),))
            elif name == 'producer-profile':
                args['producer_profile'] = replace(args['producer_profile'], active_mints=127)
                f.expected = replace(f.expected, configuration_digest=startup.configured_identity(f.domain,
                    operations_path=f.operations_path, ledger_path=f.path, **args,
                    producer_schema_digest=f.expected.producer_schema_digest,
                    evidence_schema_digest=f.expected.evidence_schema_digest, restart_profile=f.expected.restart_profile).configuration_digest)
            elif name == 'missing-producer': f.ppath.unlink()
            elif name == 'corrupt-source': args['source_path'].write_bytes(b'not a SQLite journal')
            if name in ('missing-ledger', 'corrupt-ledger', 'ledger-schema', 'ledger-domain', 'corrupt-projection'):
                check('startup_'+name+'_no_runtime', fails(lambda: restart(f)))
                if name == 'missing-ledger': check('startup_missing_ledger_never_created', not f.path.exists())
            else:
                audit = restart(f, **args)
                check('startup_'+name+'_explicit_source_hold', audit.reconstruction.source_state == 'SOURCE_RECONSTRUCTION_UNAVAILABLE'
                    and f.runtime.source is None and f.runtime.producer is None and f.runtime._ownership is not None
                    and f.step().work == 'SOURCE_HELD')
                if name == 'missing-source': check('startup_missing_evidence_never_created', not args['source_path'].exists())
                if name == 'missing-producer': check('startup_missing_producer_never_created', not f.ppath.exists())
        finally:
            close(f)
    f = installed(rt.Fixture(directory, 'a2-missing-operations'))
    try:
        close(f)
        f.operations_path.unlink()
        check('startup_missing_operations_never_created', fails(lambda: startup.start_live(f.operations_path, f.path,
            f.domain, process_identity='missing', now_us=1, expected_identity=f.expected, **c4.cold_args(f)))
            and not f.operations_path.exists())
        check('startup_DRY_capability_not_supported', fails(lambda: startup.configured_identity(replace(f.domain, mode='DRY'),
            operations_path=f.operations_path, ledger_path=f.path, **c4.cold_args(f),
            producer_schema_digest=f.expected.producer_schema_digest, evidence_schema_digest=f.expected.evidence_schema_digest,
            restart_profile=f.expected.restart_profile)))
    finally:
        close(f)
    f = installed(rt.Fixture(directory, 'a2-exhaustion'), profile=RestartProfile(1, 10, 0))
    try:
        restart(f)
        with patch.object(startup.cold, 'reopen_live', wraps=startup.cold.reopen_live) as factory:
            check('startup_exhaustion_before_reconstruction_and_latched', fails(lambda: restart(f, startup_now_us=1000))
                and factory.call_count == 0 and f.operations.snapshot()['exhausted'] == 1)
    finally:
        close(f)


def child(directory):
    f = installed(rt.Fixture(directory, 'a2-real-child'))
    try:
        audit = restart(f)
        check('child_actual_process_local_owner', f.runtime._ownership._pid == __import__('os').getpid())
        check('child_actual_audit_and_cold_root', type(f.runtime) is ColdRuntimeV01 and audit.identity == f.expected
            and audit.reconstruction.source_state == 'SOURCE_RECONSTRUCTED' and not audit.grants_permission)
        check('child_owned_actual_runtime_consumer', f.candidate_ready().work == 'NEED_ENTRY_FACTS')
    finally:
        close(f)


def main():
    if len(sys.argv) > 1:
        child(Path(sys.argv[1]))
    else:
        with tempfile.TemporaryDirectory(prefix='live-operations-startup-') as temporary:
            directory = Path(temporary)
            result = subprocess.run([sys.executable, '-B', __file__, str(directory)], capture_output=True,
                text=True, timeout=60)
            print(result.stdout, end='')
            if result.returncode: print(result.stderr)
            check('real_child_start_audit_cold_exit', result.returncode == 0)
            normal(directory)
            lifecycle(directory)
            source_failure(directory)
            failures(directory)
    print(json.dumps({'checks': len(CHECKS), 'results': CHECKS,
        'reused_fixture_checks': len(rt.CHECKS)}, sort_keys=True))


if __name__ == '__main__':
    main()
