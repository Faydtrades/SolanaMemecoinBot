"""S05: one owned BUY/protection/SELL journey with lost ACK and delayed truth.

Synthetic public I/O, ephemeral signer, isolated stores and controlled clock only.
Accepted Step9 abrupt-cut/fresh-process proofs are referenced, never rerun.
"""
from __future__ import annotations
import base64
import copy
import hashlib
import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch

import live_step10_s01_s02_selftest_v0_1 as prior
from solders.message import to_bytes_versioned

ops, c2, a3, sf, NOW = prior.ops, prior.c2, prior.a3, prior.sf, prior.NOW
CHECKS, EVIDENCE = {}, {}
FIXTURE_VERSION = 'STEP10_S05_SYNTHETIC_ENGINEERING_V01'
HELPER_SHA256 = '775a01bcc7b7cc1142704e266c03dc64a377e33003dd785a2f5606822ea33964'
ACCEPTED = {
    'B2.1': ('meme-live-b21-evidence', 'e5dafe878d1e5c25840a06468da7cdffc43079f219cf76893bb621acc10b4a54'),
    'B2.2': ('meme-live-b22-evidence', '94151bc3c7cc8a90b177f2b80c0d4024e4ebfe09d531739523a733c19c58d68a'),
    'B2.3': ('meme-live-b23-evidence', 'e6aeaf7591842a9bc741274db87907b0141fd7d5f8d910bc7e34914549aa3b47'),
}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def economics(f):
    state = f.repo.consumer_snapshot()
    return {'funding': state['funding'], 'positions': state['positions'],
        'reservations': state['reservations'], 'accounts': state['accounts']}


def late_root(scenario, at):
    # Original transaction remains in its valid original block. A later root
    # strictly exceeds the lease, without pretending to provide full coverage.
    old = scenario.root_slot
    for index in range(27):
        slot = old+1
        scenario.blocks[slot] = dict(blockhash=sf.bh(slot),
            previousBlockhash=scenario.blocks[old]['blockhash'], parentSlot=old,
            blockHeight=scenario.blocks[old]['blockHeight']+1,
            blockTime=at+9, signatures=[])
        old = slot
    scenario.initial_slot = scenario.root_slot = scenario.status_context = old


def delayed(f, key, action, at, number):
    side = action.side
    before = economics(f)
    audit_before = f.repo.audit()
    binding_before = f.runtime.position_binding
    protection_before = f.repo.protection(action.position_id)
    with prior.external_mint(action.mint), prior.retained_accumulator_rpc(f, action):
        submitted, reads, sends = c2.execution(f, key, action, at=at, number=number,
            outcome='TRANSPORT_UNRESOLVED')
        envelope, actual, scenario, pre = c2.original_chain(f, submitted, at)
    attempt_id = submitted.attempt_id
    wire = base64.b64decode(envelope.signed_wire_base64)
    simulations = [r for r in reads.requests if r['method'] == 'simulateTransaction']
    check(side+'_lost_ACK_original_real_send', submitted.work == 'SUBMISSION_OBSERVED'
        and submitted.reason == 'TRANSPORT_UNRESOLVED' and len(sends.requests) == 1
        and sends.requests[0]['params'][0] == envelope.signed_wire_base64
        and all(c2.VersionedTransaction.from_bytes(wire).verify_with_results()))
    check(side+'_same_simulated_signed_sent_message', len(simulations) == 1
        and to_bytes_versioned(c2.VersionedTransaction.from_bytes(base64.b64decode(simulations[0]['params'][0])).message)
            == bytes.fromhex(envelope.preparation.message_hex)
        and to_bytes_versioned(c2.VersionedTransaction.from_bytes(wire).message)
            == bytes.fromhex(envelope.preparation.message_hex))
    check(side+'_send_no_economic_application', economics(f) == before
        and f.repo.audit()['attempt_count'] == audit_before['attempt_count']+1)
    late_root(scenario, at)
    observations = []
    for label, offset in (('null', 10), ('incomplete_finalized_status', 14)):
        unknown = copy.deepcopy(scenario)
        unknown.tx = None
        if label == 'null':
            unknown.status = None
        observed, calls = c2.reconcile(f, unknown, at+offset)
        attempt = f.repo.attempt(attempt_id)
        chain = f.repo.chain_receipt(observed.receipt_key)
        check(side+'_'+label+'_original_UNKNOWN_after_lease_height', observed.work == 'RECONCILED'
            and attempt.chain_finality == 'UNKNOWN' and not attempt.chain_quarantined
            and attempt.recorded_stage == 'UNKNOWN' and attempt.lane_held
            and attempt.primary_signature == envelope.primary_signature
            and not attempt.economically_applied and economics(f) == before
            and chain.observation.root.block_height > envelope.preparation.lease.last_valid_block_height)
        check(side+'_'+label+'_public_history_signature_bound',
            any(r['method'] == 'getSignatureStatuses' and r['params'] == [[envelope.primary_signature],
                {'searchTransactionHistory': True}] for r in calls)
            and any(r['method'] == 'getTransaction' and r['params'][0] == envelope.primary_signature for r in calls))
        snapshot = f.repo.audit()
        try:
            f.repo.prepare_attempt(replace(envelope.preparation, ordinal=2), fence=f.repo.write_fence())
        except (ValueError, RuntimeError) as exc:
            denied = str(exc)
        else:
            raise AssertionError(side+' replacement must be denied')
        check(side+'_'+label+'_replacement_denied_by_unknown_lane',
            denied == 'LEDGER_WALLET_MUTATION_LANE_HELD' and f.repo.audit() == snapshot
            and f.repo.attempt(attempt_id) == attempt)
        next_work = f.step(at+offset+1)
        check(side+'_'+label+'_elapsed_time_retains_reconciliation_priority',
            next_work.work == 'NEED_RECONCILIATION' and next_work.attempt_id == attempt_id
            and economics(f) == before and f.runtime.position_binding == binding_before
            and f.repo.protection(action.position_id) == protection_before
            and len(sends.requests) == 1)
        observations.append({'kind': label, 'clock_utc': a3.utc(at+offset),
            'result': asdict(observed), 'attempt': asdict(attempt), 'root': asdict(chain.observation.root),
            'chain_evidence_digest': chain.observation.content_digest, 'denial': denied,
            'next_work': asdict(next_work), 'journal': prior.stamp(f, label)})
    check(side+'_unknown_reservation_encumbrance', f.repo.reservation(action.root_id).status == 'RESERVED'
        and f.repo.mutation_lane()['active_attempt_id'] == attempt_id
        and len(f.repo.consumer_snapshot()['reservations']) == 1)
    reconciled, calls = c2.reconcile(f, scenario, at+20)
    chain = f.repo.chain_receipt(reconciled.receipt_key)
    check(side+'_later_exact_finalized_success_unapplied',
        reconciled.reason == 'FINALIZED_SUCCESS_UNAPPLIED' and economics(f) == before
        and chain.observation.transaction.wire_bytes == wire
        and chain.observation.transaction.primary_signature == envelope.primary_signature
        and f.repo.attempt(attempt_id).lane_held)
    snapshot = f.repo.audit()
    replay = f.repo.ingest_chain_observation(attempt_id, chain.observation,
        ingestion_key=chain.ingestion_key, evaluated_at_utc=chain.decision.evaluated_at_utc,
        fence=f.repo.write_fence())
    check(side+'_exact_chain_replay_no_duplicate', replay == chain and f.repo.audit() == snapshot)
    support = c2.application_support(f, actual, chain, pre, at+22)
    applied = f.step(at+22, application_wallet=support)
    receipt = f.repo.application_receipt(applied.receipt_key)
    proposal = receipt.decision.proposal
    check(side+'_late_actual_application_once', applied.reason == 'FINALIZED_SUCCESS_APPLIED'
        and proposal.transaction_fee_lamports == sf.FEE and proposal.venue_fee_units == actual.venue_fees
        and proposal.quote_principal_units == actual.principal
        and proposal.base_units_delta == (actual.actual_base if side == 'BUY' else -actual.actual_base)
        and f.repo.consumer_snapshot()['funding'].native_lamports
            == before['funding'].native_lamports+proposal.native_wallet_delta
        and f.repo.consumer_snapshot()['funding'].native_lamports == f.public[f.domain.wallet].lamports
        and not f.repo.attempt(attempt_id).lane_held
        and f.repo.attempt(attempt_id).primary_signature == envelope.primary_signature)
    check(side+'_actual_fee_rent_components_balance',
        sum(c.units for c in proposal.components if c.asset == 'SOL' and c.account == f.domain.wallet) == proposal.native_wallet_delta
        and proposal.locked_account_lamports_delta == sum(c.units for c in proposal.components if c.asset == 'LOCKED_LAMPORTS')
        and proposal.account_funding_lamports == -sum(c.units for c in proposal.components
            if c.kind in ('ACCOUNT_FUNDING', 'VENUE_ACCOUNT_SETUP'))
        and proposal.account_refund_lamports == sum(c.units for c in proposal.components if c.kind == 'ACCOUNT_REFUND'))
    snapshot = f.repo.audit()
    repeated = f.repo.apply_settlement(attempt_id, chain_receipt_key=chain.ingestion_key,
        support=support, ingestion_key=applied.receipt_key, recorded_at_utc=receipt.recorded_at_utc,
        fence=f.repo.write_fence())
    check(side+'_exact_application_replay_no_postings', repeated == receipt and f.repo.audit() == snapshot
        and len(sends.requests) == 1)
    result = {'action': asdict(action), 'attempt_id': attempt_id, 'signature': envelope.primary_signature,
        'message_hex': envelope.preparation.message_hex, 'message_sha256': envelope.preparation.message_sha256,
        'signed_wire_base64': envelope.signed_wire_base64, 'signed_wire_sha256': hashlib.sha256(wire).hexdigest(),
        'lease': asdict(envelope.preparation.lease), 'unknown_observations': observations,
        'exact_final_chain_digest': chain.observation.content_digest, 'application': receipt.to_record(),
        'native_before': before['funding'].native_lamports,
        'native_after': f.repo.consumer_snapshot()['funding'].native_lamports,
        'actual_base_units': actual.actual_base, 'proposal': asdict(proposal),
        'journal': prior.stamp(f, side+' applied exactly once')}
    EVIDENCE[side] = result
    return applied, result


def run(directory):
    key = c2.keypair('s05-delayed-owned')  # Fixed synthetic key; public signature/wire only in evidence.
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = prior.prepare(directory, 's05-delayed-owned')
        try:
            EVIDENCE['configuration'] = prior.EVIDENCE[f.name]
            buy = f.buy
            EVIDENCE['identity'] = {'root': buy.root_id, 'position_id': buy.position_id,
                'candidate_digest': f.item.content_digest, 'source_record_digest': f.item.source_record_digest,
                'producer_lineage': f.item.producer_lineage_id, 'mint': buy.mint,
                'original_deadline_us': buy.claimed_entry_deadline_us, 'selected_track': buy.selected_exit_track}
            applied_buy, buy_result = delayed(f, key, buy, NOW+6, 171)
            binding = f.runtime.position_binding
            EVIDENCE['after_BUY_application'] = {'position': asdict(f.repo.position_history(buy.position_id)),
                'binding': asdict(binding), 'reservation': asdict(f.repo.reservation(buy.root_id)),
                'application_key': applied_buy.receipt_key, 'protection_created': f.repo.protection(buy.position_id) is not None}
            staged = f.step(NOW+30)
            check('BUY_late_truth_original_position_and_single_controller',
                binding.acquisition_application_key == applied_buy.receipt_key
                and binding.spec.track_id == buy.selected_exit_track
                and f.repo.position_history(buy.position_id).remaining_units == buy_result['actual_base_units']
                and f.repo.protection(buy.position_id).handoff.binding_id == binding.binding_id)
            check('BUY_recovery_resumes_original_due_protection', staged.work == 'PROTECTIVE_ACTION_STAGED')
            sell = f.repo.action(staged.action_id)
            EVIDENCE['after_protection_staging'] = {'result': asdict(staged),
                'binding_id': binding.binding_id, 'handoff': asdict(f.repo.protection(buy.position_id).handoff),
                'reservation': asdict(f.repo.reservation(buy.root_id))}
            check('SELL_original_owned_obligation_and_units', sell.side == 'SELL'
                and sell.root_id == buy.root_id and sell.position_id == buy.position_id
                and sell.input_units == buy_result['actual_base_units']
                and sell.obligation_id == f.repo.protection(buy.position_id).handoff.obligation_id)
            applied_sell, sell_result = delayed(f, key, sell, NOW+32, 172)
            check('SELL_recovery_satisfies_original_protection', f.runtime.position_binding == binding
                and c2.protective_outcome(f.repo, binding).state == 'SATISFIED'
                and f.repo.reservation(buy.root_id).status == 'RESERVED'
                and f.step(NOW+57).work == 'NEED_RETIREMENT')
            retired = f.step(NOW+58, retirement_wallet=prior.continuation.wallet(f, NOW+58))
            current = f.repo.consumer_snapshot()
            history = f.repo.position_history(buy.position_id)
            check('original_lifecycle_lawful_retirement', retired.work == 'RETIREMENT_RETIRED'
                and not current['positions'] and not current['reservations'] and not current['pending_attempts']
                and f.runtime.position_binding is None and history.status == 'RETIRED'
                and history.acquired_units == history.sold_units == buy_result['actual_base_units']
                and f.repo.port_receipt(retired.receipt_key).retirement.terminal_attempt_id == applied_sell.attempt_id)
            check('whole_run_two_attempts_no_replacement_no_rearm',
                f.repo.audit()['attempt_count'] == 2 and f.repo.audit()['action_count'] == 2
                and all(f.repo.authority_snapshot()[name] == f.original_authority[name]
                    for name in ('policy', 'armed_entry_grant', 'entry_stop_command', 'hard_stop_command'))
                and current['funding'].native_lamports == sf.FUNDING
                    + buy_result['proposal']['native_wallet_delta']+sell_result['proposal']['native_wallet_delta'])
            EVIDENCE['final'] = {'journal': prior.stamp(f, 'final retired original lineage'),
                'audit': f.repo.audit(), 'position': asdict(history), 'binding_id': binding.binding_id,
                'gate': asdict(f.runtime._operations_degradation.snapshot()),
                'retirement_key': retired.receipt_key, 'retirement': asdict(f.repo.port_receipt(retired.receipt_key).retirement)}
            ops.close(f)
            paths = sorted(directory.glob('*.sqlite'))
            integrity = {}
            for path in paths:
                with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as conn:
                    integrity[path.name] = conn.execute('PRAGMA integrity_check').fetchall()
                    check('sqlite_integrity_'+path.name, integrity[path.name] == [('ok',)]
                        and not conn.execute('PRAGMA foreign_key_check').fetchall())
            EVIDENCE['sqlite_integrity'] = integrity
        finally:
            if f.started is not None:
                ops.close(f)


def main():
    check('qualified_helper_unchanged', prior.lf_sha256(Path(prior.__file__)) == HELPER_SHA256)
    references = {}
    for label, (folder, expected) in ACCEPTED.items():
        path = Path(tempfile.gettempdir())/folder/'FINAL_FROZEN_MANIFEST.json'
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        check(label+'_accepted_manifest_binding', digest == expected)
        references[label] = {'manifest_path': str(path), 'manifest_sha256': digest,
            'use': 'Established abrupt-cut and fresh-process evidence; no rerun or new review'}
    EVIDENCE['accepted_inputs'] = references
    with tempfile.TemporaryDirectory(prefix='step10-s05-') as tmp:
        run(Path(tmp))
    paths = {Path(__file__), Path(prior.__file__),
        c2.ROOT/'docs/live/MEME_LIVE_PRODUCTION_CLOSURE_ARCHITECTURE_V2.md'}
    paths.update(Path(m.__file__).resolve() for m in tuple(sys.modules.values())
        if getattr(m, '__file__', None) and Path(m.__file__).suffix == '.py'
        and Path(m.__file__).resolve().is_relative_to(c2.ROOT))
    print(c2.canonical_json({'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'scope': ['S05'],
        'fixture_version': FIXTURE_VERSION, 'qualification': 'SYNTHETIC_ENGINEERING_ONLY',
        'limitations': ['No new process crash campaign; accepted B2/B3 proofs remain inputs',
            'No real host/profile/provider/capital qualification; synthetic monitoring limits',
            'One Pump FINAL-A full lifecycle; no route or exit-track permutation campaign'],
        'checks': CHECKS, 'reused_helper_checks': prior.CHECKS, 'evidence': EVIDENCE,
        'artifact_hashes': {str(p.relative_to(c2.ROOT)).replace('\\', '/'): prior.lf_sha256(p)
            for p in sorted(paths)}}))


if __name__ == '__main__':
    try:
        main()
    except BaseException:
        print(json.dumps({'checks': CHECKS, 'evidence': EVIDENCE}, default=lambda value: {'bytes_hex': value.hex()} if isinstance(value, bytes) else str(value)), file=sys.stderr)
        raise
