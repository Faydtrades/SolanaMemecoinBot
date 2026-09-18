"""S03/S07 owned-composition continuation of accepted Step9 recovery facts.

Only disposable sources/stores, synthetic operator/clock/host inputs, mocked
public RPC and an ephemeral signer are used. In-process propagated cuts below
are consumer interruptions, not new OS process-kill qualification. Accepted
B2.4/B3.2/B3.4 abrupt-loss campaigns and E04 dispatch fencing are reused.
"""
from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

import live_step10_s01_s02_selftest_v0_1 as prior
import live_step10_s04_s08_selftest_v0_1 as owned
import live_step10_s06_buy_recovery_selftest_v0_1 as buy
import live_step10_s06_nonlanding_protection_selftest_v0_1 as public
import live_step10_s09_selftest_v0_1 as venue
from live import operations_readiness_v0_1 as readiness
from live import runtime_composition_v0_1 as composition
from live.authority_message_control_codec_v0_1 import receipt_to_json

ops, c2, a3, sf, NOW = prior.ops, prior.c2, prior.a3, prior.sf, prior.NOW
ROOT = Path(__file__).resolve().parents[1]
CHECKS, EVIDENCE = {}, {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


class ConsumerInterruption(BaseException):
    """Propagate after/before a real owner API, outside its transaction."""


def cut(f, function, work, *, after):
    original = getattr(composition, function)
    reached = []

    def interrupt(*args, **kwargs):
        result = original(*args, **kwargs) if after else None
        reached.append(result)
        raise ConsumerInterruption(function)

    with patch.object(composition, function, interrupt):
        try:
            work()
        except ConsumerInterruption:
            pass
    check(function+'_requested_consumer_cut', len(reached) == 1 and not f.repo._conn.in_transaction)
    return reached[0]


def reopen(f, label):
    state = public.economics(f)
    owner = f.runtime._ownership.fence
    before = prior.stamp(f, label+' before')
    audit = owned.reopen(f)
    check(label+'_new_owned_reconstruction_exact_economics', not audit.grants_permission
        and audit.owner_fence.generation > owner.generation
        and audit.owner_fence.process_identity != owner.process_identity
        and public.economics(f) == state)
    return {'before': before, 'after': prior.stamp(f, label+' after'),
        'old_owner': asdict(owner), 'new_owner': asdict(audit.owner_fence),
        'reconstruction': asdict(f.runtime.reconstruction_facts())}


def original_remaining(f, binding, original):
    current = c2.protective_outcome(f.repo, binding).obligation
    return (current.binding == original.binding and current.handoff == original.handoff
        and current.obligation_id == original.obligation_id and current.remaining_units == original.remaining_units)


def run(directory):
    name = 's03-s07-original-protection'
    key = c2.Keypair()
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = prior.prepare(directory, name)
        try:
            grant_id = f.repo.authority_acceptance(f.buy.root_id).eligibility.decision.grant_id
            original_grant = f.repo.authority_grant(grant_id)
            record = {'configuration': prior.EVIDENCE[name], 'initial': prior.stamp(f, 'admitted original'),
                'baseline_economics': asdict(f.repo.consumer_snapshot()['funding']),
                'candidate': asdict(f.item), 'original_grant': asdict(original_grant),
                'buy_action': asdict(f.buy), 'cuts': []}
            EVIDENCE[name] = record
            sent = public.send(f, key, f.buy, NOW+6, 211)
            result, envelope, actual, scenario, pre = sent
            reconciled, calls = c2.reconcile(f, scenario, NOW+12)
            chain = f.repo.chain_receipt(reconciled.receipt_key)
            check('s03_actual_BUY_finality_before_application', reconciled.reason == 'FINALIZED_SUCCESS_UNAPPLIED'
                and calls and not f.repo.consumer_snapshot()['positions']
                and f.repo.attempt(result.attempt_id).lane_held)
            record['cuts'].append(reopen(f, 'unapplied_acquisition'))
            pending = f.step(NOW+13)
            check('s03_recovered_original_unapplied_BUY', pending.work == 'NEED_APPLICATION'
                and pending.attempt_id == result.attempt_id and f.repo.audit()['attempt_count'] == 1)
            support = c2.application_support(f, actual, chain, pre, NOW+14)
            cut(f, 'bind_actual_position', lambda: f.step(NOW+14, application_wallet=support), after=False)
            applications = [r for r in f.repo._applications.values() if r.attempt_id == result.attempt_id]
            application = applications[-1]
            check('s03_actual_acquisition_committed_before_binding', len(applications) == 1
                and application.decision.disposition == 'FINALIZED_SUCCESS_APPLIED'
                and f.runtime.position_binding is None
                and f.repo.position_history(f.buy.position_id).remaining_units == actual.actual_base)
            record['cuts'].append(reopen(f, 'applied_before_binding'))
            binding = f.runtime.position_binding
            check('s03_reconstruction_binds_original_acquisition', binding is not None
                and binding.acquisition_application_key == application.ingestion_key
                and binding.root_id == f.buy.root_id and binding.position_id == f.buy.position_id)
            audit = f.repo.audit()
            replay = f.repo.apply_settlement(result.attempt_id, chain_receipt_key=chain.ingestion_key,
                support=support, ingestion_key=application.ingestion_key, recorded_at_utc=application.recorded_at_utc,
                fence=f.repo.write_fence())
            check('s03_acquisition_replay_exactly_once', replay == application and f.repo.audit() == audit)
            evaluation = cut(f, 'commit_exit_evaluation', lambda: f.step(NOW+14), after=True)
            knowledge = f.repo.exit_records(binding.binding_id)
            record['cuts'].append(reopen(f, 'after_controller_before_obligation'))
            check('s03_same_binding_and_committed_knowledge_after_controller_cut',
                f.runtime.position_binding == binding and f.repo.exit_records(binding.binding_id) == knowledge)
            obligation = cut(f, 'ensure_protective_obligation', lambda: f.step(NOW+16), after=True)
            original = c2.protective_outcome(f.repo, binding)
            due_records = f.repo.exit_records(binding.binding_id)
            check('s03_original_due_fallback_obligation_before_staging', original.state == 'STAGE_ACTION'
                and original.obligation.remaining_units == actual.actual_base
                and binding.fallback_fire_boundary_us <= (NOW+16)*1000000)
            record['cuts'].append(reopen(f, 'after_due_obligation_before_staging'))
            check('s03_original_due_obligation_and_cut_restored', f.runtime.position_binding == binding
                and f.repo.exit_records(binding.binding_id) == due_records
                and c2.protective_outcome(f.repo, binding).obligation == original.obligation)
            # Damage only this disposable producer checkpoint. Original startup
            # classifies failed source and keeps the acquired Ledger exposure.
            ops.close(f)
            ops.sql(f.ppath, 'UPDATE live_producer_checkpoint_v0_1 SET manifest_digest=?', ('0'*64,))
            owned.reopen(f)
            barrier = readiness.evaluate(f.started, clock=lambda: a3.clock(f.repo, NOW+17),
                operations_resources=lambda: ops.host(f, NOW+17))
            check('s07_exact_source_specific_ENTRY_denial', not barrier.entry.ready
                and 'CURRENT_RECONSTRUCTED_SOURCE_REQUIRED' in barrier.entry.reasons
                and f.runtime.producer is None and f.runtime.source is None)
            check('s07_failed_source_preserves_original_obligation_and_knowledge',
                f.runtime.position_binding == binding and f.repo.exit_records(binding.binding_id) == due_records
                and c2.protective_outcome(f.repo, binding).obligation == original.obligation)
            staged = f.step(NOW+17, entry=c2.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID,
                prior.wallet(f, NOW+17, mint=f.buy.mint)))
            sell = f.repo.action(staged.action_id)
            check('s03_s07_due_original_SELL_before_entry', staged.work == 'PROTECTIVE_ACTION_STAGED'
                and sell.side == 'SELL' and sell.root_id == f.buy.root_id
                and sell.obligation_id == original.obligation.obligation_id and sell.input_units == actual.actual_base
                and f.repo.audit()['action_count'] == 2 and f.repo.audit()['attempt_count'] == 1)
            before_venue = public.economics(f)
            missing, error, reads, sends, signed = venue.execute(f, key, sell, route='swap',
                at=NOW+20, number=212, fault='delayed-pool')
            check('s07_missing_venue_denies_without_erasing_reduction', missing is None
                and error == 'EXECUTION_ROUTE_UNAVAILABLE' and not sends.requests and not signed
                and public.economics(f) == before_venue and f.repo.audit()['attempt_count'] == 1
                and original_remaining(f, binding, original.obligation))
            # Separate explicit stop phase, after source/venue denial. Original
            # E04 SIGN/SEND/REBROADCAST race guards are established inputs.
            stop = a3.control(f.repo, 'HARD_STOP', 's07-explicit-hard-stop', at=NOW+21)
            blocked = readiness.evaluate(f.started, clock=lambda: a3.clock(f.repo, NOW+22),
                operations_resources=lambda: ops.host(f, NOW+22))
            held, held_error, _, stop_sends, stop_signed = venue.execute(f, key, sell,
                route='swap', at=NOW+22, number=213)
            check('s07_exact_hard_stop_blocks_EXIT_before_key_or_transport', not blocked.protective.ready
                and 'GLOBAL_HARD_STOP_LATCHED' in blocked.protective.reasons
                and held_error is None and held.work == 'ENTRY_HELD' and not stop_sends.requests and not stop_signed
                and held.reason == 'PROTECTED_PREPARE_ATTEMPT:GLOBAL_HARD_STOP'
                and public.economics(f) == before_venue and f.repo.audit()['attempt_count'] == 1)
            record['cuts'].append(reopen(f, 'hard_stop_retained'))
            check('s07_hard_stop_survives_owned_reopen', f.repo.authority_snapshot()['hard_stop_command']
                == 's07-explicit-hard-stop' and original_remaining(f, binding, original.obligation))
            # Explicit synthetic operator acknowledgment of the observed hold;
            # this control input grants no message permission or readiness.
            release = a3.control(f.repo, 'RELEASE_STOP', 's07-explicit-release', at=NOW+23,
                target='s07-explicit-hard-stop', barrier=c2.content_fingerprint(asdict(blocked)))
            check('s07_release_uses_original_control_without_rearm', f.repo.authority_snapshot()['hard_stop_command'] is None
                and f.repo.authority_snapshot()['policy'] == f.original_authority['policy']
                and f.repo.authority_snapshot()['armed_entry_grant'] is None
                and f.repo.authority_grant(grant_id) == original_grant)
            sold, sale = venue.transact(f, key, sell, route='swap', at=NOW+24, number=214)
            sell_envelope = c2.q4.send.load_signed_envelope(f.repo, sold.attempt_id)
            stages = buy.stages(f, sold.attempt_id)
            check('s07_release_requires_fresh_exact_SIGN_SEND', [r.original.request.stage for r in stages] == ['SIGN', 'SEND']
                and all(r.consumed for r in stages)
                and sell_envelope.preparation.message_sha256 == sale['message_sha256']
                and sell_envelope.authority_receipt.original.validation.clock.utc_lower_utc >= a3.utc(NOW+24))
            satisfied = c2.protective_outcome(f.repo, binding)
            check('s07_original_obligation_satisfied_by_actual_SELL', satisfied.state == 'SATISFIED'
                and satisfied.obligation.remaining_units == 0 and satisfied.obligation.handoff == original.obligation.handoff)
            retired = f.step(NOW+34, retirement_wallet=prior.wallet(f, NOW+34))
            check('s03_s07_actual_retirement_after_protected_settlement', retired.work == 'RETIREMENT_RETIRED'
                and not f.repo.consumer_snapshot()['positions'] and not f.repo.consumer_snapshot()['reservations']
                and f.repo.audit()['attempt_count'] == 2 and f.repo.audit()['action_count'] == 2
                and f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports == 2*sf.FEE)
            final_barrier = readiness.evaluate(f.started, clock=lambda: a3.clock(f.repo, NOW+35),
                operations_resources=lambda: ops.host(f, NOW+35))
            check('s07_source_ENTRY_denial_persists_after_capacity_released', not final_barrier.entry.ready
                and 'CURRENT_RECONSTRUCTED_SOURCE_REQUIRED' in final_barrier.entry.reasons)
            check('s03_s07_SQLite_integrity', f.repo._conn.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
                and not f.repo._conn.execute('PRAGMA foreign_key_check').fetchall())
            record.update(buy=buy.public_attempt(sent, application), binding=asdict(binding),
                controller=asdict(evaluation), original_obligation=asdict(original.obligation),
                knowledge=[asdict(r) for r in due_records], source_denial=asdict(barrier), sell_action=asdict(sell),
                missing_venue={'reason': error, 'public_responses': reads.public_responses},
                hard_stop=asdict(stop), stopped=asdict(blocked), held=asdict(held), release=asdict(release),
                sale=sale, message_stages=[json.loads(receipt_to_json(r)) for r in stages],
                satisfied=asdict(satisfied), retirement=asdict(retired), final_source_denial=asdict(final_barrier),
                final=prior.stamp(f, 'actual protected retirement'),
                final_economics=asdict(f.repo.consumer_snapshot()['funding']), audit=f.repo.audit())
        finally:
            ops.close(f)


def main():
    with tempfile.TemporaryDirectory(prefix='step10-s03-s07-') as tmp:
        run(Path(tmp))
    paths = [Path(__file__), Path(prior.__file__), Path(owned.__file__), Path(buy.__file__),
        Path(public.__file__), Path(venue.__file__), ROOT/'src/live/runtime_composition_v0_1.py',
        ROOT/'src/live/execution_reconciliation_v0_1.py']
    print(c2.canonical_json({'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'scope': ['S03', 'S07', 'E04-continuation'],
        'qualification': 'SYNTHETIC_ENGINEERING_ONLY_NOT_HOST_OR_REAL_CAPITAL',
        'interruption_model': 'IN_PROCESS_PROPAGATING_BASEEXCEPTION_AND_OWNED_COLD_REOPEN',
        'established_inputs_not_rerun': {
            'B2.4': {'manifest_sha256': 'f26abca6abda2c7cbac77198568497541b4e491dfd48d1017f4efe1e962be296',
                'facts': '8 actual abrupt cuts; exact-once acquisition/application/fees/retirement'},
            'B3.2': {'manifest_sha256': '73ba5862c39c49ad892c1124f1e292c69c997c861ac3b3b986f6b3eb389c443f',
                'facts': '3 cases / 6 children; original exposure/knowledge/deadline; independent dispatch; terminal SELL truth required'},
            'B3.4': {'manifest_sha256': '0bc9c2fd76ad5099da7107cf4541c6573060d193e2ad4379dfa395fa2da6092f',
                'facts': '9 cases / 18 children; source failure retains exposure/protection; hard stop, operator stop, stale-generation controls'},
            'E04': {'source': 'docs/live/MEME_LIVE_OPERATIONS_FOUNDATION_V1.md:29-48',
                'facts': 'Actual SIGN/SEND final-clock stop/takeover guard, zero later dispatch, original REBROADCAST fence loss; dispatched call cannot be recalled'}},
        'artifact_hashes': {p.relative_to(ROOT).as_posix(): prior.lf_sha256(p) for p in paths},
        'checks': CHECKS, 'reused_helper_checks': {m.__name__: m.CHECKS for m in (prior, owned, public, venue)},
        'evidence': EVIDENCE}))


if __name__ == '__main__':
    main()
