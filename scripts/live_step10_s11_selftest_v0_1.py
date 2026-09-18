"""S11/E05 owned open-position producer continuation; synthetic engineering only.

One canonical BUY, external collector rows and public same-slot proofs. Accepted
A2/C1/C3 fault/resource campaigns are inputs, not rerun. Reopens are in-process.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import httpx

import live_step10_s01_s02_selftest_v0_1 as prior
import live_step10_s04_s08_selftest_v0_1 as owned
import live_step10_s06_nonlanding_protection_selftest_v0_1 as public
import live_continuous_producer_selftest_v0_2 as producer_helpers
import live_exit_observation_selftest_v0_1 as order_helpers
from live.exit_observation_v0_1 import EVIDENCE, EVALUATION, commit_exit_evaluation
from live import operations_readiness_v0_1 as readiness
from live.public_rpc_v0_1 import PublicReadOnlyRpc
from live.transaction_evidence_v0_1 import TransactionEvidenceAdapter, TransactionRequest

ops, c2, a3, sf, hf, NOW = prior.ops, prior.c2, prior.a3, prior.sf, prior.hf, prior.NOW
ROOT = Path(__file__).resolve().parents[1]
CHECKS, EVIDENCE_LOG = {}, {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def retained(f):
    return {
        'manifest': f.producer.checkpoint_manifest(),
        'outputs': producer_helpers.outputs(f.producer),
        'active': producer_helpers.active(f.producer),
        'launches': [dict(r) for r in f.conn.execute(
            f'SELECT * FROM {producer_helpers.LAUNCHES} ORDER BY mint')],
        'winner': f.producer._retired(f.item.mint),
        'winner_runs': f.producer.original_runs(f.item.mint),
        'unfinished_runs': f.producer.original_runs(hf.MINT_B),
        'no_t0_runs': f.producer.original_runs(hf.MINT_C),
    }


def append(f, rowid, mint, kind, *, offset, n=None, slot=None):
    # Independent public collector facts. Existing source rows are immutable.
    with closing(sqlite3.connect(f.raw)) as conn, conn:
        hf.raw_fixture.insert_row(conn, rowid, mint, kind, price_units=10100,
            observed_offset=offset+14)
        if n is not None:
            signature = order_helpers.sig(n)
            conn.execute('UPDATE pump_events SET signature=?,slot=?,event_key=? WHERE rowid=?',
                (signature, slot, signature+':0:0123456789abcdef', rowid))
        conn.execute('UPDATE pump_events SET inserted_at_utc=decoded_at_utc WHERE rowid=?', (rowid,))
        row = conn.execute('SELECT signature,slot,decoded_at_utc FROM pump_events WHERE rowid=?', (rowid,)).fetchone()
        conn.execute('INSERT INTO websocket_observations VALUES(?,?,?,?,?)', (*row, 1, '{}'))


def event_sequence(f, n):
    row = f.conn.execute(f'SELECT event_sequence FROM {hf.TABLES[5]} WHERE event_key=?',
        (order_helpers.sig(n)+':0:0123456789abcdef:MARKET',)).fetchone()
    check('original_market_output_'+str(n), row is not None)
    return row[0]


def step(f, at, **kwargs):
    calls = []
    protect, source = f.runtime._protect, f.runtime._source_page

    def protecting(*args):
        calls.append({'lane': 'protection', 'through': f.producer.next_event_sequence-1})
        return protect(*args)

    def sourcing(*args):
        first = f.conn.execute(f"SELECT timer_key,source_watermark_p1_rowid FROM {hf.TABLES[6]} "
            "WHERE status<>'COMPLETE' ORDER BY source_watermark_p1_rowid,timer_key LIMIT 1").fetchone()
        before = f.producer.durable_p1_rowid
        result = source(*args)
        calls.append({'lane': 'source', 'timer': None if first is None else dict(first),
            'before': before, 'after': f.producer.durable_p1_rowid, 'result': result})
        return result

    with patch.object(f.runtime, '_protect', protecting), patch.object(f.runtime, '_source_page', sourcing):
        result = f.step(at, **kwargs)
    check('monitoring_protection_before_source_'+str(len(EVIDENCE_LOG['steps'])),
        [c['lane'] for c in calls] == ['protection', 'source'])
    EVIDENCE_LOG['steps'].append({'at': a3.utc(at), 'result': asdict(result), 'calls': calls})
    return result


def reopen(f, label):
    state, economics = retained(f), public.economics(f)
    binding, knowledge = f.runtime.position_binding, f.repo.exit_records(f.runtime.position_binding.binding_id)
    fence = f.runtime._ownership.fence
    journal = prior.stamp(f, label+' before')
    publications = []
    publish = producer_helpers.LiveContinuousProducerV02._publish_checkpoint

    def published(producer):
        result = publish(producer)
        publications.append(producer.checkpoint_manifest())
        return result

    with patch.object(producer_helpers.LiveContinuousProducerV02, '_publish_checkpoint', published):
        audit = owned.reopen(f)
    replay_metrics = dict(f.producer.metrics)
    restored = retained(f)
    check(label+'_exact_retained_replay', same_retained(restored, state))
    predecessors = [state['manifest'], *publications]
    check(label+'_checkpoint_publication_chain', bool(publications)
        and all(current['generation'] == previous['generation']+1
            and current['predecessor'] == c2.content_fingerprint(previous)
            for previous, current in zip(predecessors, predecessors[1:]))
        and publications[-1] == restored['manifest'])
    check(label+'_same_position_binding_knowledge', public.economics(f) == economics
        and f.runtime.position_binding == binding and f.repo.exit_records(binding.binding_id) == knowledge)
    check(label+'_new_owner_original_source', audit.owner_fence.generation > fence.generation
        and not audit.grants_permission and audit.reconstruction.source_state == 'SOURCE_RECONSTRUCTED'
        and f.runtime._initial_replay_deferred)
    expected_events = sum(len(token['known_events']) for token in state['active'].values())
    check(label+'_complete_active_per_mint_replay', replay_metrics['replayed_events'] == expected_events
        and replay_metrics['retired_bundle_rows_read'] == 0)
    EVIDENCE_LOG['reopens'].append({'label': label, 'before': journal,
        'after': prior.stamp(f, label+' after'), 'retained': state,
        'restored_manifest': restored['manifest'],
        'cold_checkpoint_publications': publications,
        'metrics_at_cold_reopen_before_explicit_history_reads': replay_metrics,
        'reconstruction': asdict(audit.reconstruction)})


def same_retained(current, original):
    # The actual cold factory publishes a validated checkpoint transaction;
    # generation/predecessor advance while all original state bytes stay fixed.
    return (all(current[k] == original[k] for k in original if k != 'manifest')
        and all(current['manifest'][k] == value for k, value in original['manifest'].items()
            if k not in ('generation', 'predecessor'))
        and current['manifest']['generation'] >= original['manifest']['generation'])


def run(directory):
    key = c2.Keypair()
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = prior.prepare(directory, 's11-owned-position')
        try:
            EVIDENCE_LOG.update(configuration=prior.EVIDENCE[f.name], steps=[], reopens=[],
                baseline=prior.stamp(f, 'original admitted'), candidate=asdict(f.item),
                baseline_economics=asdict(f.repo.consumer_snapshot()['funding']))
            result, envelope, actual, scenario, pre = public.send(f, key, f.buy, NOW+6, 231)
            slot = scenario.tx['slot']
            # Public finalized membership declares the real signed acquisition
            # followed by two independent public market signatures in one slot.
            scenario.blocks[slot]['signatures'] = [envelope.primary_signature,
                order_helpers.sig(50), order_helpers.sig(51)]
            reconciled, calls = c2.reconcile(f, scenario, NOW+12)
            check('actual_public_finality_before_application', reconciled.reason == 'FINALIZED_SUCCESS_UNAPPLIED'
                and calls and not f.repo.consumer_snapshot()['positions'])
            chain = f.repo.chain_receipt(reconciled.receipt_key)
            support = c2.application_support(f, actual, chain, pre, NOW+14)
            applied = f.step(NOW+14, application_wallet=support)
            binding = f.runtime.position_binding
            application = f.repo.application_receipt(applied.receipt_key)
            check('actual_owned_BUY_position', applied.reason == 'FINALIZED_SUCCESS_APPLIED'
                and binding.acquisition_application_key == applied.receipt_key
                and f.repo.position_history(f.buy.position_id).remaining_units == actual.actual_base
                and f.repo.audit()['action_count'] == f.repo.audit()['attempt_count'] == 1)
            acquisition_funding = f.repo.consumer_snapshot()['funding']
            step(f, NOW+14)
            economics = public.economics(f)
            check('actual_initial_protective_handoff_before_source_baseline',
                c2.protective_outcome(f.repo, binding).state == 'MONITORING'
                and f.repo.protection(f.buy.position_id).handoff.binding_id == binding.binding_id
                and economics['funding'] == acquisition_funding)
            original = retained(f)
            check('original_winner_tombstone_and_unfinished', original['winner']['reason'] == 'WINNER_SUPPRESSED'
                and f.item.mint not in original['active'] and hf.MINT_B in original['active'])
            append(f, 15, hf.MINT_C, 'LAUNCH', offset=1)
            append(f, 16, f.item.mint, 'BUY', offset=14, n=50, slot=slot)
            for name in ('S11-B', 'S11-A'):
                f.producer.prepare_clock_tick(datetime.fromisoformat(a3.utc(NOW+14)), timer_id=name)
            append(f, 17, hf.MINT_B, 'BUY', offset=14)
            f.producer.prepare_clock_tick(datetime.fromisoformat(a3.utc(NOW+14)), timer_id='S11-C')
            f.source_cut = NOW+14
            step(f, NOW+14)
            first = EVIDENCE_LOG['steps'][-1]['calls'][-1]
            check('minimum_watermark_and_lexical_first', first['timer']['timer_key'] == 'S11-A'
                and first['after'] == 16 and first['timer']['source_watermark_p1_rowid'] == 16)
            seq = event_sequence(f, 50)

            def pair(n, *, unknown=False):
                external = copy.deepcopy(scenario)
                external.signature = order_helpers.sig(n)
                external.tx = order_helpers.transaction(n, slot=slot)
                external.tx['blockTime'] = scenario.blocks[slot]['blockTime']
                external.status['err'] = None
                external.status['slot'] = slot
                external.slot_calls = external.genesis_calls = 0
                if unknown:
                    external.blocks.pop(slot)
                requests = []

                def transport(request):
                    requests.append(json.loads(request.content))
                    return external.handle(request)

                with PublicReadOnlyRpc('https://invalid.local', a3.PROFILE,
                        transport=httpx.MockTransport(transport)) as rpc:
                    right = TransactionEvidenceAdapter(rpc,
                        TransactionRequest(f.domain.genesis_hash, external.signature, chain.observation.root.slot),
                        clock=lambda: a3.utc(NOW+14 if unknown else NOW+15)).observe()
                EVIDENCE_LOG.setdefault('public_proof_requests', []).append(
                    {'signature': external.signature, 'unknown_membership': unknown, 'requests': requests})
                check('public_RPC_order_observation_'+str(n)+'_'+str(unknown),
                    right.transaction is not None and bool(requests)
                    and (right.membership_block is None) == unknown)
                return chain.observation, right

            step(f, NOW+14, exit_proofs={seq: pair(50, unknown=True)})
            check('same_watermark_second_before_later_fence',
                EVIDENCE_LOG['steps'][-1]['calls'][-1]['timer']['timer_key'] == 'S11-B'
                and f.producer.durable_p1_rowid == 16)
            records = f.repo.exit_records(binding.binding_id)
            fixed = [r for r in records if r.kind == EVALUATION][-1]
            unproved = [r for r in records if r.kind == EVIDENCE and r.payload['proofs']][-1]
            check('same_slot_unproved_consumed_once', unproved.payload['proofs'][0]['relation'] == 'UNKNOWN'
                and any(d['event_sequence'] == seq and d['reason'] == 'OBSERVATION_ORDER_UNPROVEN'
                    for d in fixed.payload['decisions']))
            step(f, NOW+14)
            check('later_watermark_after_both_equal_fences',
                EVIDENCE_LOG['steps'][-1]['calls'][-1]['timer']['timer_key'] == 'S11-C'
                and f.producer.durable_p1_rowid == 17)
            after_first = retained(f)
            check('no_t0_original_no_deadline_runs', hf.MINT_C in after_first['active']
                and after_first['active'][hf.MINT_C]['first_tradable_event_at_us'] is None
                and bool(after_first['no_t0_runs'])
                and all(r['deadline_json'] is None for r in after_first['no_t0_runs']))
            check('winner_identity_survives_later_observation', after_first['winner'] == original['winner']
                and after_first['winner_runs'] == original['winner_runs'])
            check('single_original_candidate_after_later_observation',
                sum(r['event_type'] == 'CandidateEvaluationEvent' for r in after_first['outputs']) == 1
                and f.repo.audit()['candidate_count'] == 1)
            check('source_continuation_no_economic_mutation', public.economics(f) == economics)
            reopen(f, 'generation_one')
            # Drain the already retained C-fence output into actual knowledge;
            # late proof enrichment then addresses the original captured row.
            step(f, NOW+14)
            step(f, NOW+15, exit_proofs={seq: pair(50)})
            enriched = [r for r in f.repo.exit_records(binding.binding_id)
                if r.kind == EVIDENCE and r.payload['proofs']][-1]
            replay = commit_exit_evaluation(f.repo, binding, command_id=fixed.command_id,
                timer_fence_utc=fixed.payload['recorded_at_utc'])
            latest = [r for r in f.repo.exit_records(binding.binding_id) if r.kind == EVALUATION][-1]
            check('late_same_slot_proof_new_knowledge', enriched.sequence > fixed.sequence
                and enriched.payload['proofs'][0]['relation'] == 'BEFORE')
            check('late_same_slot_proof_keeps_fixed_cut', replay == fixed and not latest.payload['decisions']
                and latest.payload['state']['state'] == 'MONITORING')
            append(f, 18, f.item.mint, 'BUY', offset=15, n=51, slot=slot)
            f.source_cut = NOW+15
            step(f, NOW+15)
            seq2 = event_sequence(f, 51)
            step(f, NOW+15, exit_proofs={seq2: pair(51)})
            latest = [r for r in f.repo.exit_records(binding.binding_id) if r.kind == EVALUATION][-1]
            check('new_proved_same_slot_observation_reaches_selected_controller',
                any(d['event_sequence'] == seq2 and d['eligible'] for d in latest.payload['decisions'])
                and latest.payload['state']['state'] == 'MONITORING')
            final_source = retained(f)
            check('second_checkpoint_generation_original_no_t0_tombstone_ids',
                final_source['manifest']['generation'] > after_first['manifest']['generation']
                and final_source['winner'] == original['winner']
                and final_source['winner_runs'] == original['winner_runs']
                and final_source['no_t0_runs'] == after_first['no_t0_runs']
                and final_source['outputs'][:len(after_first['outputs'])] == after_first['outputs']
                and f.repo.audit()['candidate_count'] == 1 and public.economics(f) == economics)
            reopen(f, 'generation_two')
            # A changed caller-configured source identity must pass the actual
            # owner startup gate; do not manufacture a new accepted identity.
            before_denial = prior.stamp(f, 'changed identity durable cut')
            knowledge = f.repo.exit_records(binding.binding_id)
            ops.close(f)
            args = ops.c4.cold_args(f)
            args['market_source'] = hf.ContinuousMarketSourceV02(f.raw, start_after_p1_rowid=1,
                database_identity=hf.DATABASE_ID+':CHANGED')
            args['database_identity'] = hf.DATABASE_ID+':CHANGED'
            args['degradation_config'] = ops.healthy_monitor_config(f)
            control = f.operations.snapshot()
            try:
                ops.startup.start_live(f.operations_path, f.path, f.domain,
                    process_identity=f.name+'-changed-source', now_us=control['last_control_us']+1,
                    expected_identity=f.expected, replace_generation=control['generation'], **args)
            except (ValueError, RuntimeError) as exc:
                denial = str(exc)
            else:
                raise AssertionError('changed source unexpectedly acquired runtime')
            owned.reopen(f)
            check('changed_source_identity_denied_without_economic_or_knowledge_mutation',
                denial == 'OPERATIONS_STARTUP_IDENTITY_CONFLICT' and public.economics(f) == economics
                and f.runtime.position_binding == binding and f.repo.exit_records(binding.binding_id) == knowledge
                and prior.stamp(f, 'changed identity durable cut') == before_denial
                and same_retained(retained(f), final_source))
            barrier = readiness.evaluate(f.started, clock=lambda: a3.clock(f.repo, NOW+15),
                operations_resources=lambda: ops.host(f, NOW+15))
            source_sequence = f.producer.next_event_sequence
            staged = f.step(NOW+16)
            sell = f.repo.action(staged.action_id)
            due = [r for r in f.repo.exit_records(binding.binding_id) if r.kind == EVALUATION][-1]
            check('original_due_fallback_protection_before_source_and_no_new_economics',
                staged.work == 'PROTECTIVE_ACTION_STAGED' and sell.side == 'SELL'
                and sell.root_id == f.buy.root_id and sell.input_units == actual.actual_base
                and due.payload['state']['trigger_kind'] == 'FALLBACK'
                and due.payload['state']['trigger_at_us'] == binding.fallback_fire_boundary_us
                and f.producer.next_event_sequence == source_sequence and public.economics(f) == economics
                and f.repo.audit()['candidate_count'] == 1 and f.repo.audit()['attempt_count'] == 1)
            check('isolated_store_integrity', f.repo._conn.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
                and not f.repo._conn.execute('PRAGMA foreign_key_check').fetchall()
                and f.conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                and not f.conn.execute('PRAGMA foreign_key_check').fetchall())
            EVIDENCE_LOG.update(acquisition=application.to_record(), binding=asdict(binding),
                public_acquisition_signature=envelope.primary_signature, public_same_slot=slot,
                acquisition_message={'preparation': envelope.preparation.to_record(),
                    'signed_wire_sha256': hashlib.sha256(base64.b64decode(envelope.signed_wire_base64)).hexdigest(),
                    'exact_simulated_message_sha256': envelope.preparation.message_sha256,
                    'binding_check': f.name+'_231_exact_simulated_signed_sent',
                    'public_finality_digest': chain.observation.content_digest},
                original_producer=original, first_generation=after_first, final_producer=final_source,
                unproved=asdict(unproved), fixed_cut=asdict(fixed), late_enrichment=asdict(enriched),
                proved_new_observation=asdict(latest), source_identity_denial=denial,
                source_identity_attempt={'original': hf.DATABASE_ID, 'rejected': args['database_identity']},
                before_denial=before_denial, readiness_after_original_reopen=asdict(barrier),
                due=asdict(due), protective_sell=asdict(sell),
                final=prior.stamp(f, 'original fallback staged'),
                final_economics=asdict(f.repo.consumer_snapshot()['funding']), audit=f.repo.audit())
        finally:
            ops.close(f)


def main():
    with tempfile.TemporaryDirectory(prefix='step10-s11-') as tmp:
        run(Path(tmp))
    paths = [Path(__file__), Path(prior.__file__), Path(owned.__file__), Path(public.__file__),
        Path(producer_helpers.__file__), Path(order_helpers.__file__),
        ROOT/'src/live/runtime_composition_v0_1.py', ROOT/'src/live/candidate_handoff_v0_1.py',
        ROOT/'src/live/execution_reconciliation_v0_1.py', ROOT/'src/live/continuous_producer_v0_2.py',
        ROOT/'src/live/exit_observation_v0_1.py']
    print(c2.canonical_json({'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'scope': ['S11', 'E05-position-bridge'],
        'qualification': 'SYNTHETIC_ENGINEERING_ONLY_NOT_HOST_OR_REAL_CAPITAL',
        'reconstruction_method': 'COMPLETE_ACTIVE_PER_MINT_REPLAY',
        'interruption_model': 'IN_PROCESS_OWNED_REOPEN',
        'established_inputs_not_rerun': ['Step8A A2 exact differential/no-t0 age/corrupt checkpoint/missing tail guards',
            'Step9 C1/C3 measured active/hottest-mint/aggregate bounds and causal cuts'],
        'artifact_hashes': {p.relative_to(ROOT).as_posix(): prior.lf_sha256(p) for p in paths},
        'checks': CHECKS, 'reused_helper_checks': {m.__name__: m.CHECKS for m in (prior, owned, public)},
        'evidence': EVIDENCE_LOG}))


if __name__ == '__main__':
    main()
