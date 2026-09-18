"""Step10 S04/S08 negative/replay composition; synthetic engineering only.

S01/S02's qualified positive retirement result is referenced, never rerun here.
All candidates, positions, reservations, and denials come from actual production
owners. Fault injection uses existing handoff cuts; external chain/host/policy
observations are explicit isolated fixtures. No production network or keys.
"""
from __future__ import annotations

import copy
import argparse
import hashlib
import json
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import live_step10_s01_s02_selftest_v0_1 as prior

ops, c2, a3, sf, hf, NOW = prior.ops, prior.c2, prior.a3, prior.sf, prior.hf, prior.NOW
CHECKS, EVIDENCE = {}, {}
EVIDENCE_ROOT = Path(tempfile.gettempdir())/'meme-live-step10-3fd8bcb-20260911'
QUALIFIED_HELPER_SHA256 = '775a01bcc7b7cc1142704e266c03dc64a377e33003dd785a2f5606822ea33964'


class SyntheticProcessCut(BaseException):
    """In-process interruption of work; not an OS process-kill qualification."""


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def attach_steps(f):
    runtime, fence = f.runtime, f.runtime._ownership.fence
    def step(at=NOW+4, **kwargs):
        # Persistent across calls and reopen; see prior.persistent_clock.
        clock = lambda: prior.persistent_clock(f, at)
        if kwargs.get('retirement_wallet') is not None:
            clock, kwargs['retirement_wallet'] = prior.continuation.held_retirement(clock, kwargs['retirement_wallet'])
        result = f.runtime.step(clock=clock, source_cut_utc=a3.utc(f.source_cut),
            operations_resources=lambda: ops.host(f, at), **kwargs)
        check(f.name+'_owner_monitor_attached', f.runtime is runtime and f.runtime._ownership.fence == fence
            and f.runtime._operations_degradation is not None)
        return result
    f.step = step


def reopen(f, *, expected=None):
    ops.close(f)
    controls = f.operations.snapshot()
    args = ops.c4.cold_args(f)
    args['degradation_config'] = ops.healthy_monitor_config(f)
    f.started = ops.startup.start_live(f.operations_path, f.path, f.domain,
        process_identity=f.name+'-process-'+str(controls['generation']+1),
        now_us=controls['last_control_us']+1, expected_identity=expected or f.expected,
        replace_generation=controls['generation'] or None, **args)
    f.runtime = f.started.runtime
    f.repo, f.producer, f.handoff, f.source = f.runtime.ledger, f.runtime.producer, f.runtime.handoff, f.runtime.source
    f.conn = f.runtime._producer_conn
    attach_steps(f)
    return f.started.audit


def cold(directory, name):
    f = ops.installed(c2.Fixture(directory, name))
    f.source_cut = NOW+1
    try:
        reopen(f)
    except BaseException:
        ops.close(f)
        raise
    return f


def admit_first(f, at=NOW+4):
    f.configure()
    result = f.step(at, entry=c2.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID, prior.wallet(f, at, f.item.mint)))
    if result.work != 'ENTRY_ADMITTED':
        receipt = None if result.receipt_key is None else f.repo.authority_admission_receipt(result.receipt_key)
        raise AssertionError((result, None if receipt is None else receipt.risk.reasons,
            f.source.latest_record()[1].reasons, f.runtime._operations_degradation.snapshot()))
    f.buy = f.repo.action(result.action_id)
    return f.repo.authority_admission_receipt(result.receipt_key)


def identity(f):
    return {'root': f.root, 'mint': f.item.mint, 'candidate_digest': f.item.content_digest,
        'signal_us': f.item.generated_at_us, 'original_deadline_us': f.item.generated_at_us+15000000,
        'producer_lineage': f.item.producer_lineage_id}


def append_competitor(f, at=NOW+14):
    # Explicit zero-lag synthetic collector receipt timing. The old helper's
    # artificial +2s receipt lag would put the final receipt after first expiry.
    # Strategy rows/prices and actual original signal times remain unchanged.
    with closing(sqlite3.connect(f.raw)) as conn, conn:
        for rowid, mint, kind, price in hf.NEXT:
            hf.raw_fixture.insert_row(conn, rowid, mint, kind, price_units=price)
            conn.execute('UPDATE pump_events SET inserted_at_utc=decoded_at_utc WHERE rowid=?', (rowid,))
            conn.execute('INSERT INTO websocket_observations VALUES(?,?,?,?,?)',
                (f'sig-{rowid}', 5000000+rowid, hf.at(rowid), 1, '{}'))
    f.source_cut = NOW+14
    for _ in range(128):
        f.step(at)
        if len(f.runtime.queued_roots) == 2:
            second_root = next(root for root in f.runtime.queued_roots if root != f.root)
            return f.repo.candidate(second_root)
    raise AssertionError('two actual canonical queued candidates required')


def admission(f, item, at, key):
    return f.repo.admit_authority_entry(a3.EntryRequest(item.trade_root(f.domain), 'PUMP', sf.TOKEN_PROGRAM_ID, f.policy.selected_track),
        a3.clock(f.repo, at), f.source, prior.wallet(f, at, item.mint), command_id=key, fence=f.repo.write_fence())


def fresh_source(f, at):
    # New ordinary collector trade; its already-signalled mint cannot reselect.
    rowid = at-NOW+14
    f.append([(rowid, f.item.mint, 'BUY', 10100)])
    f.source_cut = at
    observed = f.runtime._source_health(a3.clock(f.repo, at+4), a3.utc(at), lambda:a3.clock(f.repo, at+4))
    check(f.name+'_actual_refreshed_source', observed.disposition == 'HEALTHY'
        and prior.continuation.utc_microseconds(observed.covered_through_utc) == at*1000000)
    return observed


def replay_cut(directory, cut):
    f = cold(directory, 's04-'+cut)
    try:
        baseline = prior.stamp(f, 'before handoff cut')
        fired = []
        def crash(point):
            if point == cut:
                fired.append(point)
                raise SyntheticProcessCut('SYNTHETIC_HANDOFF_COMMIT_CUT')
        f.handoff.failure_injector = crash
        for _ in range(128):
            try:
                f.step()
            except SyntheticProcessCut:
                break
            if fired:
                break
        check(f.name+'_actual_requested_cut', fired == [cut])
        row = f.conn.execute(f"SELECT * FROM {hf.TABLES[5]} WHERE event_type='CandidateEvaluationEvent' ORDER BY event_sequence LIMIT 1").fetchone()
        original = f.handoff._candidate(dict(row))
        root = original.trade_root(f.domain)
        expected_count = 0 if cut == 'before_ledger_receive' else 1
        check(f.name+'_durable_inbox_ACK_cut', f.repo.audit()['candidate_count'] == expected_count
            and row['delivered'] == (1 if cut == 'after_ack_commit' else 0)
            and f.repo.audit()['action_count'] == 0)
        old_fence = f.runtime._ownership.fence
        audit = reopen(f)
        check(f.name+'_new_owned_process_backlog_original', audit.owner_fence.process_identity != old_fence.process_identity
            and audit.owner_fence.generation > old_fence.generation and not audit.grants_permission)
        for _ in range(128):
            recovered = f.step()
            if recovered.work == 'NEED_ENTRY_FACTS':
                f.root, f.item = recovered.root_id, f.repo.candidate(recovered.root_id)
                break
        else:
            raise AssertionError((f.name, recovered, f.runtime.reconstruction_facts(),
                f.runtime._operations_degradation.snapshot()))
        check(f.name+'_replayed_original_candidate', f.root == root and f.item == original
            and f.repo.audit()['candidate_count'] == 1 and f.runtime.queued_roots == (root,))
        accepted = admit_first(f)
        original_buy = f.buy
        before = f.repo.audit()
        exact = f.repo.admit_authority_entry(accepted.request, accepted.eligibility.original.clock, f.source,
            f.repo.wallet_support(accepted.wallet_support_digest), command_id=accepted.command_id, fence=f.repo.write_fence())
        check(f.name+'_exact_admission_replay_one_amount', exact == accepted and f.repo.audit() == before
            and len(f.repo.consumer_snapshot()['reservations']) == 1)
        # Deliberately corrupted input retains the original canonical key; it is
        # never accepted/used as a candidate or a new economic lineage.
        corrupted = replace(original, source_record_digest='0'*64)
        try:
            f.repo.receive_candidate(corrupted, fence=f.repo.write_fence())
        except (ValueError, RuntimeError) as exc:
            conflict = str(exc)
        else:
            raise AssertionError('immutable canonical content conflict must reject')
        check(f.name+'_immutable_conflict_no_mutation', corrupted.trade_root(f.domain) == root
            and f.repo.audit() == before and f.repo.candidate(root) == original)
        replay = prior.CandidateHandoffV01(f.producer, f.repo, f.binding, database_identity=hf.DATABASE_ID)
        for _ in range(128):
            if not replay.deliver_page(limit=32).scanned_rows:
                break
        check(f.name+'_post_ACK_delivery_no_second_reservation', f.repo.audit() == before
            and f.repo.action(original_buy.action_id) == original_buy)
        record = {'baseline': baseline, 'identity': identity(f), 'accepted_amount': original_buy.input_units,
            'interruption_model': 'IN_PROCESS_PROPAGATING_BASEEXCEPTION_AND_OWNED_RECONSTRUCTION',
            'before_restart_ACK': row['delivered'], 'immutable_conflict': conflict,
            'new_owner': asdict(audit.owner_fence)}
        if cut == 'after_ack_commit':
            # Intentional negative external version selection; no recovery rearm.
            f.policy = replace(f.policy, policy_id='SYNTHETIC_POLICY_REVISION_2', approval=a3.operator(NOW+5))
            a3.control(f.repo, 'INSTALL_POLICY', 's04-policy-revision', policy_value=f.policy)
            denied = admission(f, original, NOW+6, 's04-policy-changed-original')
            check('s04_policy_revision_does_not_rearm_or_resize', not denied.accepted
                and f.repo.authority_snapshot()['armed_entry_grant'] is None
                and f.repo.action(original_buy.action_id) == original_buy
                and f.repo.authority_acceptance(root) == accepted)
            try:
                reopen(f, expected=replace(f.expected, runtime_version='SYNTHETIC_UNSUPPORTED_RUNTIME_VERSION'))
            except (ValueError, RuntimeError) as exc:
                version_denial = str(exc)
            else:
                raise AssertionError('unsupported runtime version must fail startup')
            reopen(f)
            check('s04_version_failure_cannot_reset_original_identity', f.repo.candidate(root) == original
                and f.repo.action(original_buy.action_id) == original_buy
                and f.repo.audit()['action_count'] == 1 and len(f.repo.consumer_snapshot()['reservations']) == 1
                and f.repo.authority_snapshot()['armed_entry_grant'] is None)
            record.update(policy_denial=denied.risk.reasons, unsupported_version_denial=version_denial,
                new_policy_digest=f.policy.content_digest)
        if cut == 'after_ledger_commit':
            original_deadline = original_buy.claimed_entry_deadline_us
            refreshed = fresh_source(f, NOW+20)
            # Fresh original public quote/message graph is read-only. Authority
            # must still classify its original admitted BUY window as elapsed.
            with prior.external_mint(original.mint):
                intent = c2.q1._intent(c2.q1.capture_message_context(f.repo, original_buy.action_id), (NOW+24)*1000000)
                seed, _ = c2.a4.evidence(f, original_buy, c2.a5.wallet_scenario(f.public, f.lower), at=NOW+24,
                    context_slot=f.lower.slot, intent=intent, wallet_floor=f.lower.slot, original_accounts=f.public)
                c2.q1.install(f, seed)
                production, reads = c2.q1.produce(f, original_buy, seed)
            check('s04_fresh_quote_cannot_extend_original_window',
                'MESSAGE_ENTRY_ORIGINAL_WINDOW_UNRESOLVED_OR_ELAPSED' in production.validation.reasons
                and production.original.evidence.venue_read.primary.cut.observed_at_us > original_deadline
                and any(r['method'] == 'simulateTransaction' for r in reads.requests)
                and production.original.context.action == original_buy)
            try:
                c2.q1.prepare_exact_message(f.repo, production, fence=f.repo.write_fence())
            except (ValueError, RuntimeError) as exc:
                execution_denial = str(exc)
            else:
                raise AssertionError('expired final message cannot become an attempt')
            check('s04_expired_fresh_quote_no_attempt_or_new_root', f.repo.audit()['attempt_count'] == 0
                and f.repo.audit()['action_count'] == 1 and f.repo.candidate(root) == original
                and f.repo.action(original_buy.action_id).claimed_entry_deadline_us == original_deadline)
            record.update(refreshed_source_digest=refreshed.content_digest,
                fresh_quote=json.loads(production.original.evidence.supplied_quote_json),
                message_denial=production.validation.reasons, preparation_denial=execution_denial)
        record['final'] = prior.stamp(f, 'final original replay journal')
        record['synthetic_policy'] = asdict(f.policy)
        record['startup_identity'] = asdict(f.expected)
        record['audit'] = f.repo.audit()
        EVIDENCE[f.name] = record
    finally:
        ops.close(f)


def expired_backlog(directory):
    f = cold(directory, 's04-expired-backlog')
    try:
        baseline = prior.stamp(f, 'before backlog')
        f.candidate_ready()
        original = f.item
        f.configure()
        grant = f.repo.authority_snapshot()['armed_entry_grant']
        before = identity(f)
        fresh_source(f, NOW+20)
        reopen(f)
        result = f.step(NOW+24, entry=c2.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID, prior.wallet(f, NOW+24, original.mint)))
        denied = f.repo.authority_admission_receipt(result.receipt_key)
        check('s04_expired_backlog_original_deadline_after_reopen', result.work == 'ENTRY_DENIED'
            and denied.eligibility.decision.disposition == 'EXPIRED'
            and denied.eligibility.decision.binding.deadline_us == original.generated_at_us+15000000
            and f.repo.candidate(f.root) == original and f.repo.audit()['action_count'] == 0
            and not f.repo.consumer_snapshot()['reservations']
            and f.repo.authority_snapshot()['armed_entry_grant'] == grant)
        EVIDENCE[f.name] = {'identity': before, 'denial': denied.risk.reasons, 'audit': f.repo.audit(),
            'baseline': baseline, 'final': prior.stamp(f, 'after expired backlog denial'),
            'synthetic_policy': asdict(f.policy), 'startup_identity': asdict(f.expected)}
    finally:
        ops.close(f)


def competing(directory, *, unknown=False):
    name = 's08-unknown-BUY' if unknown else 's08-occupied-due-exit'
    key = c2.keypair(name)
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = cold(directory, name)
        try:
            baseline = prior.stamp(f, 'before original candidates')
            f.candidate_ready()
            first = f.item
            second = append_competitor(f)
            accepted = admit_first(f, NOW+14)
            original_authority = f.repo.authority_snapshot()
            check(name+'_two_original_independent_mints_before_execution', second.mint != first.mint
                and f.runtime.queued_roots == (second.trade_root(f.domain),)
                and first.generated_at_us+15000000 == (NOW+15)*1000000
                and second.generated_at_us+15000000 == (NOW+29)*1000000)
            if unknown:
                with prior.external_mint(first.mint), prior.retained_accumulator_rpc(f, f.buy):
                    submitted, reads, sends = c2.execution(f, key, f.buy, at=NOW+14, number=151, outcome='TRANSPORT_UNRESOLVED')
                    envelope, actual, scenario, pre = c2.original_chain(f, submitted, NOW+14)
                check(name+'_actual_uncertain_send', submitted.work == 'SUBMISSION_OBSERVED'
                    and submitted.reason == 'TRANSPORT_UNRESOLVED' and len(sends.requests) == 1)
                unresolved = copy.deepcopy(scenario)
                unresolved.tx = unresolved.status = None
                reconciled, calls = c2.reconcile(f, unresolved, NOW+20)
                attempt = f.repo.attempt(submitted.attempt_id)
                check(name+'_actual_null_history_holds_original_lane', reconciled.work == 'RECONCILED'
                    and calls and attempt.lane_held and not f.repo.consumer_snapshot()['positions'])
                result = f.step(NOW+22, entry=c2.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID, prior.wallet(f, NOW+22, second.mint)))
                check(name+'_runtime_reconciliation_priority', result.work == 'NEED_RECONCILIATION'
                    and result.attempt_id == attempt.preparation.attempt_id
                    and f.runtime.queued_roots == (second.trade_root(f.domain),))
                before_binding = None
                economics = {'signature': attempt.primary_signature, 'attempt_id': attempt.preparation.attempt_id,
                    'recorded_stage': attempt.recorded_stage, 'economic_disposition': attempt.economic_disposition}
            else:
                applied, economics = prior.transact(f, key, f.buy, NOW+14, 141)
                before_binding = f.runtime.position_binding
                before_units = f.repo.position_history(f.buy.position_id).remaining_units
                result = f.step(NOW+22, entry=c2.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID, prior.wallet(f, NOW+22, second.mint)))
                sell = f.repo.action(result.action_id)
                check(name+'_runtime_due_exit_before_competing_entry', result.work == 'PROTECTIVE_ACTION_STAGED'
                    and sell.side == 'SELL' and sell.root_id == f.buy.root_id and sell.input_units == before_units
                    and f.runtime.queued_roots == (second.trade_root(f.domain),))
                repeated = f.step(NOW+23, entry=c2.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID, prior.wallet(f, NOW+23, second.mint)))
                check(name+'_runtime_pending_exit_before_competing_entry', repeated.work == 'NEED_EXECUTION'
                    and repeated.action_id == sell.action_id and f.repo.audit()['action_count'] == 2)
            denied = admission(f, second, NOW+24, name+'-independent-cap-decision')
            check(name+'_independently_eligible_candidate_denied_by_cap', not denied.accepted
                and denied.eligibility.decision.disposition == 'ELIGIBLE_CONTEXT_ONLY'
                and 'V1_OCCUPIED_POSITION_OR_UNRESOLVED_ADMISSION' in denied.risk.reasons
                and f.buy.root_id in denied.risk.occupied_roots
                and f.repo.authority_acceptance(second.trade_root(f.domain)) is None)
            if unknown:
                check(name+'_possibly_landing_encumbrance', 'COMPETING_POSSIBLY_LANDING_MUTATION_LANE' in denied.risk.reasons)
            reservation = f.repo.reservation(f.buy.root_id)
            funding = f.repo.consumer_snapshot()['funding']
            old_owner = f.runtime._ownership.fence
            audit = reopen(f)
            resumed = f.step(NOW+25, entry=c2.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID, prior.wallet(f, NOW+25, second.mint)))
            check(name+'_reopen_retains_economics_policy_and_priority', audit.owner_fence.generation > old_owner.generation
                and f.repo.reservation(f.buy.root_id) == reservation and f.repo.consumer_snapshot()['funding'] == funding
                and f.runtime.position_binding == before_binding
                and f.repo.candidate(second.trade_root(f.domain)) == second
                and all(f.repo.authority_snapshot()[field] == original_authority[field]
                    for field in ('policy', 'armed_entry_grant', 'entry_stop_command', 'hard_stop_command'))
                and resumed.work == ('NEED_RECONCILIATION' if unknown else 'NEED_EXECUTION'))
            if unknown:
                check(name+'_same_unknown_signature_after_reopen', f.repo.attempt(attempt.preparation.attempt_id) == attempt
                    and f.repo.audit()['attempt_count'] == 1 and f.repo.audit()['action_count'] == 1)
            else:
                check(name+'_same_controller_units_after_reopen', f.repo.position_history(f.buy.position_id).remaining_units == before_units
                    and resumed.action_id == sell.action_id)
            EVIDENCE[name] = {'first': identity(f), 'second': {'root': second.trade_root(f.domain), 'mint': second.mint,
                'candidate_digest': second.content_digest, 'signal_us': second.generated_at_us,
                'original_deadline_us': denied.eligibility.decision.binding.deadline_us},
                'cap_denial': denied.risk.reasons, 'eligibility': denied.eligibility.decision.disposition,
                'initial_priority': asdict(result), 'reopened_priority': asdict(resumed),
                'economic_evidence': economics, 'audit': f.repo.audit(),
                'gate': asdict(f.runtime._operations_degradation.snapshot()),
                'baseline': baseline, 'final': prior.stamp(f, 'after cap denial and owned reopen'),
                'synthetic_policy': asdict(f.policy), 'startup_identity': asdict(f.expected),
                'synthetic_monitor_configuration': asdict(f.startup_monitor_config),
                'collector_fixture': 'original strategy rows; competitor websocket receipt lag explicitly zero seconds'}
        finally:
            ops.close(f)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--positive-evidence', type=Path, default=EVIDENCE_ROOT/'S01_S02.stdout.json')
    args = parser.parse_args()
    helper = Path(prior.__file__)
    check('qualified_S01_S02_helper_unchanged', prior.lf_sha256(helper) == QUALIFIED_HELPER_SHA256)
    positive_path = args.positive_evidence
    positive = json.loads(positive_path.read_text(encoding='utf-8-sig'))
    check('qualified_positive_retirement_source_binding', positive['artifact_hashes']['scripts/live_step10_s01_s02_selftest_v0_1.py'] == QUALIFIED_HELPER_SHA256
        and all(positive['checks'].values()) and len(positive['evidence']['s01-s02-normal']['lifecycles']) == 2)
    with tempfile.TemporaryDirectory(prefix='step10-s04-s08-') as tmp:
        directory = Path(tmp)
        for cut in ('before_ledger_receive', 'after_ledger_commit', 'before_ack_commit', 'after_ack_commit'):
            replay_cut(directory, cut)
        expired_backlog(directory)
        competing(directory)
        competing(directory, unknown=True)
    print(c2.canonical_json({'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'scope': ['S04', 'S08'],
        'qualification': 'SYNTHETIC_ENGINEERING_ONLY_NOT_HOST_PROFILE_OR_REAL_CAPITAL', 'checks': CHECKS,
        'reused_helper_checks': prior.CHECKS,
        'artifact_sha256': prior.lf_sha256(Path(__file__)),
        'qualified_helper_sha256': QUALIFIED_HELPER_SHA256, 'reused_after_retirement_boundary': {
            'artifact_path': str(positive_path), 'artifact_sha256': hashlib.sha256(positive_path.read_bytes()).hexdigest(),
            'normal_economics': positive['evidence']['s01-s02-normal']['economics']}, 'evidence': EVIDENCE}))


if __name__ == '__main__':
    main()
