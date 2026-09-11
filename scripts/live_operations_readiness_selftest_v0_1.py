"""A3 actual A2-owned Runtime barriers; disposable stores, original fake I/O."""
from __future__ import annotations

import ast
import copy
import hashlib
import sqlite3
from contextlib import closing
import json
import sys
import tempfile
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_operations_startup_selftest_v0_1 as a2
import live_runtime_continuation_selftest_v0_1 as c3
from live import operations_readiness_v0_1 as barriers
from live.execution_message_v0_1 import produce_exact_message

rt, a3, sf, NOW = a2.rt, a2.a3, a2.sf, a2.NOW
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    print(json.dumps({"check": name, "result": bool(value)}), flush=True)
    if not value:
        raise AssertionError(name)


def read(f, at=NOW+4, **kwargs):
    kwargs.setdefault('operations_resources', lambda: a2.host(f, at))
    return barriers.evaluate(f.started, clock=lambda: a3.clock(f.repo, at), **kwargs)


def entry(f, at=NOW+4):
    return rt.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID, a3.wallet(f, at=at, scenario=f.scenario))


def public_reduction(f, action, at, number):
    # Actual production Q1 factory and original public decoders/validation; only
    # transport bytes and clock are controlled fixtures. No prepare/sign/send.
    intent = rt.q1._intent(rt.q1.capture_message_context(f.repo, action.action_id), at*1000000)
    seed, _ = rt.a4.evidence(f, action, rt.a5.wallet_scenario(f.public, f.lower), at=at,
        context_slot=f.lower.slot, intent=intent, wallet_floor=f.lower.slot,
        recent_blockhash=sf.bh(number), last_valid_height=f.lower.block_height+30,
        validity_height=f.lower.block_height+1, original_accounts=f.public)
    class Boundary(rt.q1.PublicTransport):
        def __call__(self, request):
            response = super().__call__(request)
            value, method = response.json(), json.loads(request.content)['method']
            if method == 'getLatestBlockhash':
                value['result']['value']['lastValidBlockHeight'] = f.lower.block_height+30
            elif method == 'getBlockHeight':
                value['result'] = f.lower.block_height+1
            return rt.httpx.Response(200, json=value)
    external = Boundary(seed)
    before = f.repo.write_fence()
    with rt.ExecutionReadOnlyRpc('https://invalid.local', a3.PROFILE, now_us=external.now,
            transport=rt.httpx.MockTransport(external)) as rpc:
        production = produce_exact_message(f.repo, action.action_id, rpc, seed.evidence.wallet,
            seed.evidence.quote_policy, seed.evidence.plan_policy, rt.ComputeBudget(250000, 1000),
            clock=lambda: seed.clock, now_us=lambda: seed.evidence.simulation.leases[0].observed_at_us)
    if production.validation.disposition != 'SUPPORTED_CONTEXT_ONLY':
        print(json.dumps(asdict(production.validation)), flush=True)
    check(f.name+'_readonly_public_production_'+str(number), f.repo.write_fence() == before
        and production.validation.disposition == 'SUPPORTED_CONTEXT_ONLY'
        and len(external.requests) == 10)
    return production.original


def durable_digests(f):
    # Synthetic fixture-only logical SQLite cuts; never production/raw data.
    paths = (f.path, f.ppath, f.raw, f.operations_path, f.directory/(f.name+'-evidence.sqlite'))
    results = []
    for path in paths:
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as conn:
            results.append(hashlib.sha256('\n'.join(conn.iterdump()).encode()).hexdigest())
    return tuple(results)


def normal(directory):
    f = a2.installed(rt.Fixture(directory, 'a3-entry'))
    try:
        audit = a2.restart(f)
        cold = read(f)
        check('healthy_empty_start_neither_shortcut_nor_permission', not cold.entry.ready
            and cold.protective.state == 'NOT_REQUIRED' and not cold.protective.ready
            and cold.reconstruction.source_state == 'SOURCE_RECONSTRUCTED')
        f.candidate_ready()
        f.configure()
        facts = entry(f)
        before = f.repo.write_fence()
        durable_before = durable_digests(f)
        ready = read(f, entry=facts)
        check("readiness_leaves_all_durable_owners_unchanged", durable_digests(f) == durable_before)
        check('actual_current_candidate_source_wallet_policy_grant_entry_ready', ready.entry.ready)
        check('entry_original_root_digest_deadline_no_acceptance', ready.entry.root_id == f.root
            and ready.entry.candidate_digest == f.item.content_digest
            and ready.entry.deadline_us == f.item.generated_at_us+15000000
            and f.repo.authority_acceptance(f.root) is None and f.repo.write_fence() == before)
        check('historical_A2_audit_never_current_Authority', audit.authority.policy is None
            and ready.authority_digest == f.repo._authority.content_digest and audit == f.started.audit)
        check('all_readiness_results_no_message_capabilities', all(not value.grants_permission for value in
            (ready, ready.entry, ready.protective)) and not ready.entry.may_sign and not ready.protective.may_send)
        check('historical_matched_wallet_denied_at_new_clock', not read(f, NOW+5, entry=facts).entry.ready)
        stale_clock = replace(a3.clock(f.repo), status='UNKNOWN')
        check('unknown_clock_denied', not barriers.evaluate(f.started, clock=lambda: stale_clock, entry=facts).entry.ready)
        old_receipt = f.repo.evaluate_authority_entry(f.root, f.policy.selected_track, a3.clock(f.repo), f.source,
            command_id='historical-only', fence=f.repo.write_fence())
        check('original_positive_Authority_receipt_is_context', old_receipt.decision.disposition == 'ELIGIBLE_CONTEXT_ONLY')
        a3.control(f.repo, 'STOP_ENTRY', 'entry-stop', at=NOW+5)
        denied = read(f, NOW+5, entry=entry(f, NOW+5))
        check('stale_positive_Authority_receipt_cannot_defeat_current_stop', not denied.entry.ready
            and 'ENTRY_STOP_LATCHED' in denied.entry.reasons and f.repo.authority_receipt('historical-only') == old_receipt)
        original = f.runtime._ownership
        replacement = f.operations.acquire('replaced', now_us=100, replace_generation=original.fence.generation)
        check('stale_process_current_barrier_and_actual_Runtime_held', not read(f).entry.ready
            and f.step().work == 'OPERATIONS_HELD')
        replacement.close()
        a2.restart(f)
        f.operations.operator_stop(now_us=200)
        check('durable_operator_stop_does_not_reuse_startup_audit', not read(f).entry.ready
            and not read(f).protective.ready and f.step().work == 'OPERATIONS_HELD')
    finally:
        a2.close(f)
    f = a2.installed(rt.Fixture(directory, 'a3-stale-source'))
    try:
        a2.restart(f)
        f.candidate_ready(); f.configure()
        check('fresh_source_positive_baseline', read(f, entry=entry(f)).entry.ready)
        old = f.source.latest_record()
        delayed = read(f, NOW+100, entry=entry(f, NOW+100))
        check('retained_healthy_source_stale_and_original_candidate_expired', not delayed.entry.ready
            and any('STALE' in r or 'EXPIRED' in r for r in delayed.entry.reasons)
            and f.source.latest_record() == old)
        a2.sql(f.ppath, 'UPDATE live_producer_checkpoint_v0_1 SET manifest_digest=?', ('0'*64,))
        denied = read(f, entry=entry(f))
        check('startup_healthy_producer_cannot_hide_current_checkpoint_damage', not denied.entry.ready)
    finally:
        a2.close(f)


def lifecycle(directory):
    key = rt.Keypair()
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = a2.installed(rt.Fixture(directory, 'a3-lifecycle'))
        try:
            a2.restart(f)
            acquired = rt.acquisition(f, key)
            original = f.runtime.position_binding
            a2.restart(f)
            opened = read(f, NOW+14)
            check('cold_actual_open_units_controller_protection_before_entry', opened.reconstruction.remaining_units == acquired.actual_base
                and f.runtime.position_binding == original and not opened.entry.ready
                and opened.protective.state == 'MONITORING' and not opened.protective.ready)
            f.step(NOW+14)
            monitored = read(f, NOW+14)
            check('original_monitoring_state_intact_no_fabricated_dispatch_ready', monitored.protective.state == 'MONITORING'
                and 'ORIGINAL_PROTECTION_RETAINED' in monitored.safe_work and not monitored.protective.ready)
            before_due = f.repo.write_fence()
            due = read(f, NOW+16)
            check('elapsed_original_fallback_due_not_reset_or_auto_staged', due.protective.state == 'DUE_STAGING_REQUIRED'
                and due.protective.due and not due.entry.ready and f.repo.write_fence() == before_due)
            a2.c4.commit_exit_evaluation(f.repo, original, command_id='a3-due', timer_fence_utc=a3.utc(NOW+16))
            a2.c4.ensure_protective_obligation(f.repo, original, recorded_at_utc=a3.utc(NOW+16))
            partial = a2.c4.stage_protective_sell(f.repo, original, max_units=acquired.actual_base//3)
            a2.restart(f)
            check('actual_Runtime_selects_original_partial_before_entry', f.step(NOW+17).action_id == partial.action_id)
            proof = public_reduction(f, partial, NOW+20, 141)
            before = f.repo.write_fence()
            durable_before = durable_digests(f)
            ready = barriers.evaluate(f.started, clock=lambda: proof.clock, reduction=proof.evidence)
            check(f.name+"_protective_readiness_leaves_all_durable_owners_unchanged", durable_digests(f) == durable_before)
            check('current_actual_partial_SELL_protective_ready', ready.protective.ready)
            check('positive_protection_bound_actual_partial_and_due', ready.protective.action_id == partial.action_id
                and ready.protective.due and not ready.entry.ready and f.repo.write_fence() == before)
            submitted, _, sends = rt.execution(f, key, partial, at=NOW+20, number=142)
            check('actual_Runtime_Authority_Execution_independently_dispatch_original', submitted.work == 'SUBMISSION_OBSERVED'
                and len(sends.requests) == 1)
            _, sold, scenario, pre = rt.original_chain(f, submitted, NOW+20)
            reconciled, _ = rt.reconcile(f, scenario, NOW+26)
            support = rt.application_support(f, sold, f.repo.chain_receipt(reconciled.receipt_key), pre, NOW+28)
            before_apply = read(f, NOW+28)
            check('positive_finality_still_application_required_not_ready', not before_apply.protective.ready
                and before_apply.protective.state == 'TRUTH_REQUIRED')
            f.step(NOW+28, application_wallet=support)
            a2.restart(f)
            residual = read(f, NOW+29)
            check('partial_residual_keeps_original_units_due_and_priority', residual.reconstruction.remaining_units == acquired.actual_base-partial.input_units
                and residual.protective.due and residual.protective.state == 'DUE_STAGING_REQUIRED' and not residual.entry.ready)
            staged = f.step(NOW+29)
            remaining_action = f.repo.action(staged.action_id)
            proof = public_reduction(f, remaining_action, NOW+32, 143)
            check('actual_residual_reduction_current_protective_ready', barriers.evaluate(f.started,
                clock=lambda: proof.clock, reduction=proof.evidence).protective.ready)
            submitted, _, _ = rt.execution(f, key, remaining_action, at=NOW+32, number=144, outcome='TRANSPORT_UNRESOLVED')
            _, sold, scenario, pre = rt.original_chain(f, submitted, NOW+32)
            unknown = copy.deepcopy(scenario); unknown.tx = unknown.status = None
            rt.reconcile(f, unknown, NOW+38)
            original_attempt = f.repo.attempt(submitted.attempt_id)
            a2.restart(f)
            before = f.repo.write_fence()
            held = read(f, NOW+39, reduction=proof.evidence)
            check('UNKNOWN_exact_pending_original_due_capacity_held', not held.protective.ready and not held.entry.ready
                and held.protective.state == 'TRUTH_REQUIRED' and held.protective.due
                and f.repo.attempt(submitted.attempt_id) == original_attempt and f.repo.write_fence() == before
                and 'ORIGINAL_RECONCILIATION_OR_APPLICATION_REQUIRED' in held.safe_work)
            check('readiness_hold_does_not_deadlock_accepted_truth_work', f.step(NOW+39).work == 'NEED_RECONCILIATION')
            reconciled, _ = rt.reconcile(f, scenario, NOW+39)
            support = rt.application_support(f, sold, f.repo.chain_receipt(reconciled.receipt_key), pre, NOW+40)
            f.step(NOW+40, application_wallet=support)
            a2.restart(f)
            satisfied = read(f, NOW+41)
            check('zero_actual_units_not_empty_capacity_shortcut', satisfied.reconstruction.remaining_units == 0
                and satisfied.protective.state == 'SATISFIED_AWAITING_RETIREMENT' and not satisfied.entry.ready)
            check('actual_retirement_requires_current_wallet', f.step(NOW+41).work == 'NEED_RETIREMENT')
            retired = f.step(NOW+42, retirement_wallet=c3.wallet(f, NOW+42))
            check('original_retirement_releases_actual_capacity', retired.work == 'RETIREMENT_RETIRED')
            a2.restart(f)
            check('retired_healthy_restart_still_requires_current_candidate_truth', not read(f, NOW+42).entry.ready)
            fresh_rows = [(n+48, rt.hf.MINT_C, kind, price) for n, _, kind, price in rt.hf.FIRST if n not in (5, 7)]
            f.append(fresh_rows)
            for _ in range(128):
                candidate = f.runtime.step(clock=lambda: a3.clock(f.repo, NOW+52), source_cut_utc=a3.utc(NOW+48),
                    operations_resources=lambda: a2.host(f, NOW+52))
                if candidate.work == 'NEED_ENTRY_FACTS': break
            item = f.repo.candidate(candidate.root_id)
            facts = rt.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID, c3.wallet(f, NOW+52, mint=item.mint))
            ready = read(f, NOW+52, entry=facts)
            check('legal_eventual_current_entry_after_truth_retirement_source', ready.entry.ready
                and ready.entry.root_id == candidate.root_id and ready.entry.root_id != original.root_id
                and ready.entry.deadline_us == item.generated_at_us+15000000 and ready.protective.state == 'NOT_REQUIRED')
            admitted = f.runtime.step(clock=lambda: a3.clock(f.repo, NOW+52), source_cut_utc=a3.utc(NOW+48), entry=facts,
                operations_resources=lambda: a2.host(f, NOW+52))
            check('eventual_entry_actual_Authority_independently_admits_no_second_execution', admitted.work == 'ENTRY_ADMITTED'
                and f.repo.authority_acceptance(item.trade_root(f.domain)).accepted and f.repo.audit()['attempt_count'] == 3)
        finally:
            a2.close(f)


def stale_proof(directory):
    # The future-time probe has its own durable monitor; this timeline ends here.
    key = rt.Keypair()
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = a2.installed(rt.Fixture(directory, 'a3-stale-proof'))
        try:
            a2.restart(f)
            acquired = rt.acquisition(f, key)
            f.step(NOW+14)
            original = f.runtime.position_binding
            a2.c4.commit_exit_evaluation(f.repo, original, command_id='stale-due', timer_fence_utc=a3.utc(NOW+16))
            a2.c4.ensure_protective_obligation(f.repo, original, recorded_at_utc=a3.utc(NOW+16))
            partial = a2.c4.stage_protective_sell(f.repo, original, max_units=acquired.actual_base//3)
            proof = public_reduction(f, partial, NOW+20, 145)
            check('fresh_exact_proof_before_isolated_future_probe', read(f, NOW+20, reduction=proof.evidence).protective.ready)
            old = read(f, NOW+100, reduction=proof.evidence)
            check('stale_exact_venue_wallet_clock_evidence_cannot_keep_protective_ready', not old.protective.ready)
        finally:
            a2.close(f)


def source_failure(directory):
    key = rt.Keypair()
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = a2.installed(rt.Fixture(directory, 'a3-source-failure'))
        try:
            a2.restart(f)
            rt.acquisition(f, key)
            f.step(NOW+14)
            original = f.runtime.position_binding
            a2.restart(f, damage=lambda: a2.sql(f.ppath,
                'UPDATE live_producer_checkpoint_v0_1 SET manifest_digest=?', ('0'*64,)))
            held = read(f, NOW+16)
            check('source_ENTRY_failure_preserves_original_due_fallback', held.protective.state == 'DUE_STAGING_REQUIRED'
                and held.protective.due and f.runtime.position_binding == original and not held.entry.ready)
            staged = f.step(NOW+16)
            action = f.repo.action(staged.action_id)
            proof = public_reduction(f, action, NOW+20, 151)
            ready = barriers.evaluate(f.started, clock=lambda: proof.clock, reduction=proof.evidence)
            check('failed_source_intact_reduction_current_protective_ready', ready.protective.ready
                and not ready.entry.ready and f.runtime.producer is None and f.runtime.source is None)
            a3.control(f.repo, 'STOP_ENTRY', 'source-entry-stop', at=NOW+20)
            ready = barriers.evaluate(f.started, clock=lambda: proof.clock, reduction=proof.evidence)
            check('Authority_ENTRY_stop_does_not_destroy_original_reduction', ready.protective.ready and not ready.entry.ready)
            a3.control(f.repo, 'REVOKE_GRANT', 'source-revoked', at=NOW+20,
                target=f.repo.authority_acceptance(original.root_id).eligibility.decision.grant_id)
            denied = barriers.evaluate(f.started, clock=lambda: proof.clock, reduction=proof.evidence)
            check('current_original_grant_revocation_denies_reduction', not denied.protective.ready
                and 'ORIGINAL_SCOPE_GRANT_MISSING_OR_HARD_REVOKED' in denied.protective.reasons)
            a3.control(f.repo, 'HARD_STOP', 'source-hard-stop', at=NOW+20)
            denied = barriers.evaluate(f.started, clock=lambda: proof.clock, reduction=proof.evidence)
            check('hardstop_current_even_with_original_due_truth', not denied.protective.ready
                and not denied.entry.ready and denied.protective.due
                and 'GLOBAL_HARD_STOP_LATCHED' in denied.protective.reasons)
            check('actual_hardstop_never_dispatches_from_historical_ready', f.step(NOW+20).work == 'ENTRY_HELD')
        finally:
            a2.close(f)


def structural():
    tree = ast.parse((ROOT/'src/live/operations_readiness_v0_1.py').read_text(encoding='utf-8-sig'))
    calls = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)}
    check('classification_no_mutation_grant_or_reconstruction_engine', not calls & {
        'admit_authority_entry', 'consume_authority_message_stage', 'evaluate_authority_entry',
        'prepare_attempt', 'record_authority_control', 'initialize', 'reopen', 'step', 'commit', 'sign_exact', 'send_exact'})


def main():
    with tempfile.TemporaryDirectory(prefix='live-operations-readiness-') as temporary:
        directory = Path(temporary)
        normal(directory)
        lifecycle(directory)
        stale_proof(directory)
        source_failure(directory)
        structural()
    print(json.dumps({'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'checks': len(CHECKS),
        'results': CHECKS, 'reused_fixture_checks': len(rt.CHECKS)}, sort_keys=True))


if __name__ == '__main__':
    main()
