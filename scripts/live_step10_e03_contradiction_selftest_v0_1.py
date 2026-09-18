"""Step10 E03: owned Runtime consumes contradictory canonical public truth.

One isolated admitted BUY; only public RPC/transport responses are synthetic.
No old selftest main, network, production data, key persistence, or recovery grant.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch

import live_step10_s01_s02_selftest_v0_1 as prior
import live_step10_s06_nonlanding_protection_selftest_v0_1 as s06
import live_step10_s04_s08_selftest_v0_1 as restart
from live.ledger_chain_codec_v0_1 import chain_observation_from_json, chain_observation_to_json
from live.ledger_repository_v0_1 import LedgerRepository
from live.transaction_coverage_v0_1 import CanonicalCoverageObservation, CoverageLimits, CoverageRequest
from live.transaction_evidence_v0_1 import TransactionObservation, TransactionRequest

ops, c2, a3, sf, NOW = prior.ops, prior.c2, prior.a3, prior.sf, prior.NOW
CHECKS = {}


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def economics(repo):
    value = repo.consumer_snapshot()
    # Quarantine is an explicit separate control fact, not an economic posting.
    return {"funding": asdict(replace(value['funding'], quarantine_reasons=())),
        **{name: [asdict(item) for item in value[name]] for name in ('positions', 'reservations', 'accounts')}}


def public_receipt(receipt):
    encoded = chain_observation_to_json(receipt.observation)
    check('original_public_observation_codec_roundtrip', chain_observation_from_json(encoded) == receipt.observation)
    return {'receipt': receipt.to_record(), 'observation': json.loads(encoded)}


def held(f, sent, initial, label):
    result, envelope, _, _, _ = sent
    attempt = f.repo.attempt(result.attempt_id)
    snapshot, audit = f.repo.consumer_snapshot(), f.repo.audit()
    check(label+'_unchanged_original_attempt', attempt.preparation == envelope.preparation
        and attempt.primary_signature == envelope.primary_signature
        and attempt.signed_wire_digest == envelope.signed_wire_digest
        and f.repo.action(result.action_id) == f.buy)
    check(label+'_quarantined_verdict', attempt.chain_quarantined
        and attempt.chain_finality == 'QUARANTINED'
        and snapshot['funding'].quarantine_reasons == ('CHAIN_FACTS_CONTRADICTION',))
    check(label+'_original_lane_still_held', attempt.lane_held
        and f.repo.mutation_lane()['held']
        and f.repo.mutation_lane()['active_attempt_id'] == result.attempt_id
        and snapshot['pending_attempts'] == (result.attempt_id,))
    check(label+'_original_capacity_still_reserved', len(snapshot['reservations']) == 1
        and f.repo.reservation(f.buy.root_id).status == 'RESERVED'
        and not snapshot['positions'] and f.runtime.position_binding is None)
    check(label+'_no_economic_application_or_fill', economics(f.repo) == initial
        and audit['posting_count'] == 0 and audit['attempt_count'] == audit['action_count'] == 1
        and f.repo.position_history(f.buy.position_id) is None)
    check(label+'_authority_not_changed', all(f.repo.authority_snapshot()[field] == f.original_authority[field]
        for field in ('policy', 'armed_entry_grant', 'entry_stop_command', 'hard_stop_command')))
    return {'stamp': prior.stamp(f, label), 'attempt_finality': asdict(f.repo.attempt_finality(result.attempt_id)),
        'funding': asdict(snapshot['funding']), 'lane': f.repo.mutation_lane(), 'audit': audit}


def denial(f, envelope):
    before = f.repo.audit()
    try:
        f.repo.prepare_attempt(replace(envelope.preparation, ordinal=envelope.preparation.ordinal+1),
            fence=f.repo.write_fence())
    except (ValueError, RuntimeError) as exc:
        reason = str(exc)
    else:
        raise AssertionError('quarantined replacement unexpectedly permitted')
    check('exact_quarantine_replacement_denial', reason == 'LEDGER_CURRENT_CUSTODY_QUARANTINED'
        and f.repo.audit() == before)
    return reason


def run(directory):
    name = 'e03-owned-canonical-parent-contradiction'
    key = c2.Keypair()  # Ephemeral synthetic key; never serialized.
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = prior.prepare(directory, name)
        try:
            initial = economics(f.repo)
            source = f.source.latest_record()[1]
            evidence = {'configuration': prior.EVIDENCE[name], 'initial': prior.stamp(f, 'owned admission'),
                'economics': initial, 'original_action': asdict(f.buy),
                'source_record_digest': source.content_digest, 'source_binding': asdict(f.binding),
                'candidate': asdict(f.item), 'domain': asdict(f.domain)}
            sent = s06.send(f, key, f.buy, NOW+6, 211, unknown=True)
            result, envelope, actual, unknown, pre = sent
            check('actual_unknown_send_economics_unchanged', result.reason == 'TRANSPORT_UNRESOLVED'
                and economics(f.repo) == initial and f.repo.attempt(result.attempt_id).recorded_stage == 'UNKNOWN')
            evidence['signed_attempt'] = {'result': asdict(result), 'preparation': envelope.preparation.to_record(),
                'signature': envelope.primary_signature, 'signed_wire_sha256': envelope.signed_wire_digest,
                'signed_wire_base64': envelope.signed_wire_base64}
            full = s06.coverage_scenario(envelope, unknown, NOW+13)
            conflict = copy.deepcopy(full)
            conflict.blocks[full.root_slot-3]['previousBlockhash'] = sf.bh(240)
            check('only_one_external_parent_hash_changed', sum(conflict.blocks[slot] != full.blocks[slot]
                for slot in full.blocks) == 1 and conflict.tx is None and conflict.status is None)
            reconciled, calls = c2.reconcile(f, conflict, NOW+13)
            rows = f.repo._conn.execute('SELECT ingestion_key FROM ledger_chain_receipts ORDER BY commit_seq').fetchall()
            receipts = [f.repo.chain_receipt(row[0]) for row in rows]
            check('actual_Runtime_original_read_then_coverage', len(receipts) == 2
                and type(receipts[0].observation) is TransactionObservation
                and type(receipts[1].observation) is CanonicalCoverageObservation
                and receipts[0].decision.resulting_state.disposition == 'UNKNOWN'
                and receipts[0].observation.root.block_height > envelope.preparation.lease.last_valid_block_height
                and reconciled.receipt_key == receipts[1].ingestion_key)
            normal, chain = receipts
            floor = max(f.domain.minimum_context_slot, f.repo.baseline().observation.anchor.slot,
                envelope.preparation.finalized_lower_anchor.slot)
            check('original_normal_and_coverage_identity_bound', normal.observation.request == TransactionRequest(
                f.domain.genesis_hash, envelope.primary_signature, floor)
                and chain.observation.request == CoverageRequest(f.domain.genesis_hash, envelope.primary_signature,
                    envelope.preparation.lease.blockhash, envelope.preparation.lease.last_valid_block_height,
                    envelope.preparation.finalized_lower_anchor)
                and all(r.preparation_digest == envelope.preparation.content_digest
                    and r.signed_wire_digest == envelope.signed_wire_digest
                    and r.observation.profile.fingerprint == f.domain.expected_profile_fingerprint for r in receipts))
            expected_reasons = {'CANONICAL_LINK:CANONICAL_PARENT_HASH_OR_HEIGHT_CONTRADICTION',
                'CANONICAL_PARENT_HASH_OR_HEIGHT_CONTRADICTION', 'EXACT_REQUESTED_INTERVAL_NOT_COVERED',
                'RETAINED_FINALIZED_ANCHOR_CONFLICT'}
            check('exact_Runtime_Ledger_contradiction_verdict', reconciled.work == 'RECONCILED'
                and reconciled.reason == 'QUARANTINED'
                and chain.decision.observation_disposition == 'QUARANTINED'
                and chain.decision.evidence_port_disposition == 'CONTRADICTORY'
                and set(chain.decision.reasons) == expected_reasons
                and chain.decision.resulting_state.positive_finality is None)
            signature_reads = [r for r in calls if r['method'] == 'getBlock'
                and r['params'][1]['transactionDetails'] == 'signatures']
            check('public_read_limits_and_order_preserved', chain.observation.limits == CoverageLimits()
                and chain.observation.profile == a3.PROFILE and len(chain.observation.blocks_descending) == 5
                and len(signature_reads) == 5 and len(calls) == 16
                and [r['method'] for r in calls].index('getTransaction') < calls.index(signature_reads[0])
                and sum(r['method'] == 'getSignatureStatuses' for r in calls) == 1
                and chain.observation.root.slot-floor <= 2048)
            check('contradiction_not_deadline_denial', (NOW+13)*1000000 < f.buy.claimed_entry_deadline_us)
            evidence['contradiction'] = {'normal': public_receipt(normal), 'coverage': public_receipt(chain),
                'rpc_calls': calls, 'external_headers': conflict.blocks,
                'state': held(f, sent, initial, 'contradiction'), 'replacement_denial': denial(f, envelope)}
            check('no_application_requested_by_contradiction', f.repo.audit()['application_receipt_count'] == 0)
            # Later exact positive public data has the original wire and valid membership.
            # Its normal observation does not touch the conflicting intermediate header.
            with prior.external_mint(f.buy.mint):
                _, actual, positive, pre = c2.original_chain(f, result, NOW+6)
            positive.blocks.update({slot: copy.deepcopy(block) for slot, block in full.blocks.items()
                if slot not in positive.blocks})
            positive.initial_slot = positive.root_slot = positive.status_context = full.root_slot
            positive.slot_calls = positive.genesis_calls = 0
            later, later_calls = c2.reconcile(f, positive, NOW+14)
            later_chain = f.repo.chain_receipt(later.receipt_key)
            check('later_original_positive_port_does_not_clear_quarantine', later.reason == 'QUARANTINED'
                and later_chain.decision.evidence_port_disposition == 'SUPPORTED_FINALIZED_OBSERVATION'
                and later_chain.decision.observation_disposition == 'FINALIZED_SUCCESS'
                and later_chain.decision.reasons == ()
                and later_chain.decision.resulting_state.positive_finality == 'FINALIZED_SUCCESS'
                and later_chain.decision.resulting_state.quarantined
                and later_chain.observation.transaction.wire_bytes == base64.b64decode(envelope.signed_wire_base64)
                and later_chain.observation.transaction.message_sha256 == envelope.preparation.message_sha256)
            check('quarantined_positive_does_not_request_coverage', type(later_chain.observation) is TransactionObservation
                and f.repo.audit()['chain_receipt_count'] == 3 and len(later_calls) == 7)
            evidence['later_positive'] = {'result': asdict(later), 'chain': public_receipt(later_chain),
                'rpc_calls': later_calls, 'state': held(f, sent, initial, 'later_positive')}
            support = c2.application_support(f, actual, later_chain, pre, NOW+15)
            applied = f.step(NOW+15, application_wallet=support)
            refusal = f.repo.application_receipt(applied.receipt_key)
            check('actual_Runtime_application_consumer_refuses_quarantine', applied.work == 'APPLIED'
                and applied.reason == 'QUARANTINED' and refusal.decision.disposition == 'QUARANTINED'
                and refusal.decision.reasons == ('CURRENT_CUSTODY_QUARANTINED',)
                and refusal.decision.proposal is None and not refusal.decision.releases_own_lane)
            evidence['application_refusal'] = {'result': asdict(applied), 'receipt': refusal.to_record(),
                'state': held(f, sent, initial, 'application_refusal')}
            # Replay only the exact accepted receipts; never inject a fabricated proof.
            before = f.repo.audit()
            for original in (chain, later_chain):
                replay = f.repo.ingest_chain_observation(result.attempt_id, original.observation,
                    ingestion_key=original.ingestion_key, evaluated_at_utc=original.decision.evaluated_at_utc,
                    fence=f.repo.write_fence())
                check('exact_chain_replay_'+str(original.sequence), replay == original and f.repo.audit() == before)
            replay = f.repo.apply_settlement(result.attempt_id, chain_receipt_key=later_chain.ingestion_key,
                support=support, ingestion_key=refusal.ingestion_key, recorded_at_utc=refusal.recorded_at_utc,
                fence=f.repo.write_fence())
            check('exact_refusal_replay_has_no_mutation', replay == refusal and f.repo.audit() == before)
            durable = held(f, sent, initial, 'before_cold_start')
            try:
                restart.reopen(f)
            except (ValueError, RuntimeError) as exc:
                cold_denial = str(exc)
            else:
                raise AssertionError('cold startup unexpectedly accepted economic quarantine')
            check('actual_owned_cold_start_refuses_unresolved_quarantine', cold_denial == 'RUNTIME_COLD_ECONOMIC_QUARANTINE'
                and f.started is None)
            with LedgerRepository.reopen(f.path, f.domain) as reopened:
                audit = reopened.audit()
                check('cold_refusal_durable_economics_and_receipts_intact', economics(reopened) == initial
                    and reopened.chain_receipt(chain.ingestion_key) == chain
                    and reopened.chain_receipt(later_chain.ingestion_key) == later_chain
                    and reopened.application_receipt(refusal.ingestion_key) == refusal
                    and reopened.attempt_finality(result.attempt_id) == later_chain.decision.resulting_state
                    and reopened.mutation_lane() == durable['lane']
                    and audit['custody_digest'] == durable['audit']['custody_digest']
                    and audit['chain_receipt_count'] == 3 and audit['application_receipt_count'] == 1
                    and audit['posting_count'] == 0 and audit['sqlite_integrity'] == 'ok'
                    and audit['foreign_key_violations'] == 0)
                evidence['cold_refusal'] = {'reason': cold_denial, 'audit': audit,
                    'funding': asdict(reopened.consumer_snapshot()['funding']), 'lane': reopened.mutation_lane()}
            with closing(sqlite3.connect(f.path)) as conn:
                check('explicit_final_sqlite_integrity', conn.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
                    and conn.execute('PRAGMA foreign_key_check').fetchall() == [])
            return evidence
        finally:
            ops.close(f)


def main():
    with tempfile.TemporaryDirectory(prefix='step10-e03-') as tmp:
        evidence = run(Path(tmp))
    paths = [Path(__file__), Path(prior.__file__), Path(s06.__file__), Path(restart.__file__),
        Path(c2.__file__), *(c2.ROOT/'src/live'/name for name in ('runtime_composition_v0_1.py',
        'execution_reconciliation_v0_1.py', 'ledger_finality_v0_1.py', 'ledger_custody_v0_1.py',
        'ledger_repository_v0_1.py', 'transaction_coverage_v0_1.py', 'transaction_evidence_v0_1.py',
        'runtime_reconstruction_v0_1.py', 'operations_startup_v0_1.py'))]
    result = {'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'scope': 'E03_RUNTIME_CANONICAL_PARENT_CONTRADICTION',
        'qualification': 'SYNTHETIC_ENGINEERING_ONLY_NOT_HOST_PROFILE_OR_REAL_CAPITAL',
        'checks': CHECKS, 'fixture_checks': {'admission': prior.CHECKS, 'send': s06.CHECKS},
        'artifact_hashes': {str(p.relative_to(c2.ROOT)).replace('\\', '/'): prior.lf_sha256(p) for p in paths},
        'evidence': evidence}
    encoded = c2.canonical_json(result)
    json.loads(encoded)  # Validate exact JSON; no lossy fallback serialization.
    print(encoded)


if __name__ == '__main__':
    main()
