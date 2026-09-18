"""Step10 E01: actual owned runtime exact-authority boundary qualification.

Synthetic disposable sources, databases, public RPC, clocks and ephemeral keys.
Negative copy interposition is deliberately adversarial, not a supported
production construction path. It delegates the unchanged original signer and
never replaces a validator or grants permission. SELL is acquired and made due
by actual BUY/finality/application/controller transitions. No old suite main.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
from solders.hash import Hash
from solders.instruction import CompiledInstruction
from solders.message import Message
from solders.pubkey import Pubkey

import live_step10_s01_s02_selftest_v0_1 as s1
from live.authority_message_codec_v0_1 import encode_validation_input
from live.authority_message_control_v0_1 import MessageProfileCommand, FreshStageConsumption
from live.authority_message_control_codec_v0_1 import receipt_to_json, receipt_from_json
from live.execution_signer_v0_1 import ExecutionSigningError
from live.ledger_actions_v0_1 import utc_microseconds

c2, ops, a3, sf, NOW = s1.c2, s1.ops, s1.a3, s1.sf, s1.NOW
CHECKS, EVIDENCE = {}, {}
MUTATIONS = ('byte', 'account', 'instruction', 'fee', 'blockhash')


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


@contextmanager
def key_calls():
    calls, previous = [], sys.getprofile()
    def profile(frame, event, arg):
        if event == 'c_call' and getattr(arg, '__name__', '') == 'sign_message':
            calls.append(True)
    sys.setprofile(profile)
    try:
        yield calls
    finally:
        sys.setprofile(previous)


def stamp(f):
    audit = f.repo.audit()
    return {k: audit[k] for k in ('revision', 'last_receipt_digest', 'custody_digest',
        'action_count', 'attempt_count', 'application_count') if k in audit}


def economics(f):
    snap = f.repo.consumer_snapshot()
    return {'funding': asdict(snap['funding']), 'positions': [asdict(p) for p in snap['positions']],
        'applications': f.repo.audit().get('application_count')}


def corrupt(production, label):
    """Corrupt only a copy after the real simulation and real SIGN grant.

    Recompute local envelope/lineage encoding to reach the actual signer evidence
    equality guard. Original read records, durable preparation and approval are
    retained unchanged. Bypassing the production constructor is intentional
    negative interposition, not permission or a supported source replacement.
    """
    original = production.original
    envelope = original.evidence.simulation.envelopes[0]
    raw = bytes.fromhex(envelope.message_hex)
    message = Message.from_bytes(raw)
    keys, instructions = list(message.account_keys), list(message.instructions)
    blockhash = message.recent_blockhash
    identity = {}
    if label == 'account':
        index = len(keys)-1
        data = bytearray(bytes(keys[index]))
        data[-1] ^= 1
        keys[index] = Pubkey.from_bytes(bytes(data))
        identity = {'account_index': index}
    elif label == 'blockhash':
        data = bytearray(bytes(blockhash))
        data[-1] ^= 1
        blockhash = Hash.from_bytes(bytes(data))
        identity = {'field': 'recent_blockhash'}
    elif label == 'instruction':
        index = len(instructions)-1
        ix = instructions[index]
        target = (ix.program_id_index+1) % len(keys)
        instructions[index] = CompiledInstruction(target, ix.data, ix.accounts)
        identity = {'instruction_index': index, 'field': 'program_id_index'}
    else:
        index = 1 if label == 'fee' else len(instructions)-1
        ix = instructions[index]
        data = bytearray(ix.data)
        offset = 1 if label == 'fee' else len(data)-1
        data[offset] ^= 1
        instructions[index] = CompiledInstruction(ix.program_id_index, bytes(data), ix.accounts)
        identity = {'instruction_index': index, 'data_offset': offset,
            'field': 'compute_unit_price' if label == 'fee' else 'instruction_data'}
    header = message.header
    changed = bytes(Message.new_with_compiled_instructions(header.num_required_signatures,
        header.num_readonly_signed_accounts, header.num_readonly_unsigned_accounts,
        keys, blockhash, instructions))
    wire = b'\x01'+bytes(64)+changed
    new_envelope = replace(envelope, message_hex=changed.hex(), message_sha256=hashlib.sha256(changed).hexdigest(),
        transaction_base64=base64.b64encode(wire).decode(), wire_sha256=hashlib.sha256(wire).hexdigest())
    run = original.evidence.simulation
    attempt = replace(run.attempts[0], envelope_id=new_envelope.envelope_id)
    result = replace(run.results[0], envelope_id=new_envelope.envelope_id, attempt_id=attempt.attempt_id)
    run = replace(run, envelopes=(new_envelope,), attempts=(attempt,), results=(result,))
    payload = encode_validation_input(c2.a4.with_run(original, run))
    copied = copy.copy(production)
    object.__setattr__(copied, 'validation_input_json', payload)
    check(label+'_copy_codec_roundtrip', encode_validation_input(copied.original) == payload)
    check(label+'_original_production_untouched', production.message_hex == raw.hex())
    offsets = [i for i, (a, b) in enumerate(zip(raw, changed)) if a != b]
    check(label+'_one_byte_payload_change', len(raw) == len(changed) and len(offsets) == 1)
    return copied, {**identity, 'changed_offsets': offsets, 'corrupted_message_hex': changed.hex(),
        'corrupted_message_sha256': hashlib.sha256(changed).hexdigest(),
        'interposition': 'unsupported copy at concrete signer boundary; original guard unchanged'}


def signing_sample(f, receipt, target_us):
    prior = receipt.original.validation.clock
    prior_us = utc_microseconds(prior.utc_upper_utc)
    when = (datetime.fromisoformat(prior.utc_upper_utc)+timedelta(microseconds=target_us-prior_us)).isoformat(timespec='microseconds')
    return replace(a3.clock(f.repo, NOW+6), utc_lower_utc=when, utc_upper_utc=when,
        monotonic_ns=prior.monotonic_ns+(target_us-prior_us)*1000)


def run_case(directory, side, label):
    name = 'e01-'+side.lower()+'-'+label
    key = c2.Keypair()  # Never serialized.
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = s1.prepare(directory, name)
        try:
            acquisition = None
            action, at = f.buy, NOW+6
            if side == 'SELL':
                applied, acquisition = s1.transact(f, key, f.buy, NOW+6, 71)
                check(name+'_actual_bound_controller', f.step(NOW+14).work == 'ENTRY_HELD'
                    and f.runtime.position_binding.acquisition_application_key == applied.receipt_key)
                due = f.step(NOW+16)
                check(name+'_actual_due_sell', due.work == 'PROTECTIVE_ACTION_STAGED')
                action, at = f.repo.action(due.action_id), NOW+20
                check(name+'_sell_no_entry_deadline', action.claimed_entry_deadline_us is None)
            baseline, before = stamp(f), economics(f)
            original_policy = f.repo.authority_snapshot()['policy'].content_digest
            intent = c2.q1._intent(c2.q1.capture_message_context(f.repo, action.action_id), at*1000000+1)
            seed, _ = c2.a4.evidence(f, action, c2.a5.wallet_scenario(f.public, f.lower), at=at,
                context_slot=f.lower.slot, intent=intent, wallet_floor=f.lower.slot,
                recent_blockhash=sf.bh(91), last_valid_height=f.lower.block_height+30,
                validity_height=f.lower.block_height+1, original_accounts=f.public)
            if f.repo.authority_message_profile(action.policy_digest) is None:
                c2.q1.install(f, seed)
            captured, receipts, signed = {}, [], []
            original_sign = c2.AutonomousLocalSigner.sign_exact
            original_consume = type(f.repo).consume_authority_message_stage

            class ReadBoundary(c2.q1.PublicTransport):
                def __call__(self, request):
                    response = super().__call__(request)
                    payload, value = json.loads(request.content), response.json()
                    if payload['method'] == 'getLatestBlockhash':
                        value['result']['value']['lastValidBlockHeight'] = f.lower.block_height+30
                    elif payload['method'] == 'getBlockHeight':
                        value['result'] = f.lower.block_height+(31 if label == 'lease-height-after'
                            else 30 if label == 'lease-height-at' else 1)
                    return httpx.Response(200, json=value)

            def consume(repo, request, sample, source, evidence, **kwargs):
                if label == 'resource-changed' and request.stage == 'SIGN':
                    public = dict(f.public)
                    public[f.domain.wallet] = replace(public[f.domain.wallet],
                        lamports=public[f.domain.wallet].lamports-1)
                    expected = {a.pubkey: (a.mint, a.program) for a in f.repo.consumer_snapshot()['accounts']}
                    for mint in (action.mint, sf.WSOL_MINT):
                        expected[a3.derive_associated_token_address(f.domain.wallet, mint, sf.TOKEN_PROGRAM_ID)] = (mint, sf.TOKEN_PROGRAM_ID)
                    old_request = a3.WalletEvidenceRequest(f.domain.wallet, f.domain.genesis_hash,
                        f.lower.slot, tuple(a3.ExpectedTokenAccount(k, m, p) for k, (m, p) in sorted(expected.items())))
                    observed = a3.observe(c2.a5.wallet_scenario(public, f.lower), request=old_request, at=at-4)
                    evidence = replace(evidence, wallet=a3.WalletSupportInput(observed, a3.utc(at-4), f.lower.slot))
                result = original_consume(repo, request, sample, source, evidence, **kwargs)
                receipt = result.receipt if type(result) is FreshStageConsumption else result
                encoded = receipt_to_json(receipt)
                check(name+'_receipt_codec', receipt_from_json(encoded) == receipt)
                receipts.append(json.loads(encoded))
                captured['receipt'] = receipt
                return result

            def sign(signer, repository, production, delivery, **kwargs):
                receipt = delivery.receipt
                captured.update(production=production, delivery=delivery, signer=signer,
                    kwargs=dict(kwargs), original_message_hex=production.message_hex,
                    initial_validation=production.validation.disposition,
                    preparation=f.repo.attempt(receipt.original.request.attempt_id).preparation.content_digest)
                if label in MUTATIONS:
                    production, captured['corruption'] = corrupt(production, label)
                elif label == 'signer-mismatch':
                    signer = c2.AutonomousLocalSigner(c2.Keypair())
                elif label == 'approval-reselected':
                    profile = production.original.profile
                    f.repo.record_authority_message_profile(MessageProfileCommand(name, 'SELECT_EXISTING',
                        profile, profile.approval), fence=f.repo.write_fence())
                elif label == 'policy-replaced':
                    replacement = replace(f.policy, policy_id=name)
                    a3.control(f.repo, 'INSTALL_POLICY', name, policy_value=replacement, at=at)
                elif label in ('clock-backward', 'deadline-at', 'deadline-after'):
                    prior = receipt.original.validation.clock
                    prior_us = utc_microseconds(prior.utc_upper_utc)
                    target = (action.claimed_entry_deadline_us+(label == 'deadline-after')
                        if label.startswith('deadline-') else prior_us-1)
                    sample = signing_sample(f, receipt, target)
                    if label == 'clock-backward':
                        sample = replace(sample, monotonic_ns=prior.monotonic_ns+1000)
                    captured['boundary_sample'] = asdict(sample)
                    captured['boundary_validation'] = asdict(c2.a4.validate_message_evidence(
                        replace(receipt.original.validation, clock=sample)))
                    kwargs['clock'] = lambda: sample
                envelope = original_sign(signer, repository, production, delivery, **kwargs)
                signed.append(envelope)
                if label == 'deadline-at':
                    raise ExecutionSigningError('E01_POSITIVE_BOUNDARY_STOP_AFTER_VERIFIED_SIGN')
                return envelope

            reads, sends = ReadBoundary(seed), c2.q3.Boundary(f, 'ACKNOWLEDGED')
            with c2.ExecutionReadOnlyRpc('https://invalid.local', a3.PROFILE, now_us=reads.now,
                    transport=httpx.MockTransport(reads)) as rpc:
                transport = c2.q4.send.SolanaSendTransport('https://invalid.local', a3.PROFILE,
                    transport=httpx.MockTransport(sends))
                try:
                    ports = c2.ExecutionPorts(rpc, seed.evidence.wallet, seed.evidence.quote_policy,
                        seed.evidence.plan_policy, c2.ComputeBudget(250000, 1000),
                        c2.AutonomousLocalSigner(key), transport, lambda: seed.evidence.simulation.leases[0].observed_at_us)
                    with patch.object(c2.AutonomousLocalSigner, 'sign_exact', sign),                             patch.object(type(f.repo), 'consume_authority_message_stage', consume), key_calls() as calls:
                        try:
                            result = f.step(at, execution=ports)
                            denial = None
                        except (ValueError, RuntimeError) as exc:
                            result, denial = None, str(exc)
                finally:
                    transport.close()
            simulations = [r for r in reads.requests if r['method'] == 'simulateTransaction']
            check(name+'_original_simulation_cut', len(simulations) == (0 if label == 'lease-height-after' else 1))
            simulated = (bytes(c2.VersionedTransaction.from_bytes(base64.b64decode(simulations[0]['params'][0])).message)
                if simulations else base64.b64decode(next(r['params'][0] for r in reads.requests if r['method'] == 'getFeeForMessage')))
            positive = label in ('lease-height-at', 'deadline-at')
            check(name+'_no_economic_application', economics(f) == before)
            if positive:
                check(name+'_one_actual_key_call', len(calls) == len(signed) == 1)
                check(name+'_original_exact_signed_message', signed[0].preparation.message_hex == simulated.hex())
                check(name+'_boundary_result', (result is not None and result.work == 'SUBMISSION_OBSERVED'
                    and len(sends.requests) == 1) if label == 'lease-height-at' else
                    denial == 'E01_POSITIVE_BOUNDARY_STOP_AFTER_VERIFIED_SIGN' and not sends.requests)
            else:
                check(name+'_no_key_wire_send', not calls and not signed and not sends.requests)
                check(name+'_explicit_denial', denial is not None or result is not None
                    and result.reason == 'AUTHORITY_SIGN_NOT_FRESHLY_GRANTED')
            expected_denial = ('EXECUTION_ORIGINAL_PRODUCTION_APPROVAL_CONFLICT' if label in MUTATIONS else
                {'signer-mismatch': 'EXECUTION_LOCAL_SIGNER_WALLET_MISMATCH',
                 'clock-backward': 'EXECUTION_CURRENT_CLOCK_CONTINUATION_UNPROVEN',
                 'approval-reselected': 'EXECUTION_CONSUMED_LEDGER_CONTINUATION_CHANGED',
                 'policy-replaced': 'EXECUTION_CONSUMED_LEDGER_CONTINUATION_CHANGED',
                 'lease-height-after': 'EXECUTION_LEASE_UNAVAILABLE_OR_EXPIRED',
                 'deadline-after': 'EXECUTION_MESSAGE_EVIDENCE_STALE_AT_KEY_CALL'}.get(label))
            if expected_denial is not None:
                check(name+'_exact_original_guard_reason', denial == expected_denial)
            if label.startswith('deadline-'):
                boundary = captured['boundary_validation']
                check(name+'_isolated_original_deadline_boundary',
                    boundary['disposition'] == 'SUPPORTED_CONTEXT_ONLY' and not boundary['reasons']
                    if label == 'deadline-at' else boundary['reasons'] ==
                        ('MESSAGE_ENTRY_ORIGINAL_WINDOW_UNRESOLVED_OR_ELAPSED',))
            if label == 'resource-changed':
                receipt = captured['receipt']
                check(name+'_actual_resource_denial', not receipt.consumed and
                    'CURRENT_COMPLETE_UNCHANGED_CUSTODY_COMPARISON_REQUIRED' in receipt.decision.reasons)
                before_replay = stamp(f)
                historical = original_consume(f.repo, receipt.original.request,
                    receipt.original.validation.clock, f.source if side == 'BUY' else None,
                    receipt.original.validation.evidence, fence=f.repo.write_fence())
                check(name+'_denied_receipt_replay_no_permission', historical == receipt
                    and type(historical) is not FreshStageConsumption and stamp(f) == before_replay)
            replay = None
            if 'delivery' in captured:
                check(name+'_initial_real_supported_approval', captured['initial_validation'] == 'SUPPORTED_CONTEXT_ONLY'
                    and captured['delivery'].receipt.consumed
                    and captured['original_message_hex'] == simulated.hex())
                replay_before = stamp(f)
                with key_calls() as replay_calls:
                    try:
                        original_sign(captured['signer'], f.repo, captured['production'],
                            captured['delivery'], **captured['kwargs'])
                    except ExecutionSigningError as exc:
                        replay = str(exc)
                    else:
                        raise AssertionError(name+'_fresh_delivery_replay')
                check(name+'_replay_no_key_no_journal_change', not replay_calls and stamp(f) == replay_before
                    and economics(f) == before)
            attempt = next((f.repo.attempt(a) for a in f.repo.consumer_snapshot()['pending_attempts']
                if f.repo.attempt(a).preparation.action_id == action.action_id), None)
            if not positive and attempt is not None:
                check(name+'_retained_unsigned_lane', attempt.lane_held and attempt.primary_signature is None)
            EVIDENCE[name] = {'side': side, 'case': label, 'baseline': baseline, 'final': stamp(f),
                'source_binding_digest': f.binding.content_digest if hasattr(f.binding, 'content_digest') else c2.content_fingerprint(asdict(f.binding)),
                'source_profile_digest': f.source.profile.fingerprint, 'policy_digest': original_policy,
                'current_policy_digest': f.repo.authority_snapshot()['policy'].content_digest,
                'message_profile_digest': seed.profile.content_digest, 'action_id': action.action_id,
                'action_digest': action.content_digest, 'entry_deadline_us': action.claimed_entry_deadline_us,
                'acquisition': acquisition, 'simulation_count': len(simulations),
                'original_simulated_message_hex': simulated.hex() if simulations else None,
                'constructed_message_hex': simulated.hex(),
                'corruption': captured.get('corruption'), 'boundary_sample': captured.get('boundary_sample'),
                'boundary_validation': captured.get('boundary_validation'), 'denial': denial,
                'runtime_result': None if result is None else asdict(result), 'authority_receipts': receipts,
                'replay_denial': replay, 'key_calls': len(calls), 'send_count': len(sends.requests),
                'signed_public_signatures': [e.primary_signature for e in signed], 'economics_unchanged': True}
            json.dumps(EVIDENCE[name], allow_nan=False)  # Fail serialization in the first case.
            print(name+' verified', file=sys.stderr, flush=True)
        finally:
            ops.close(f)


def main():
    labels = (*MUTATIONS, 'signer-mismatch', 'clock-backward', 'approval-reselected',
        'policy-replaced', 'resource-changed', 'lease-height-at', 'lease-height-after')
    with tempfile.TemporaryDirectory(prefix='step10-e01-') as tmp:
        for side in ('BUY', 'SELL'):
            for label in labels + (('deadline-at', 'deadline-after') if side == 'BUY' else ()):
                if len(sys.argv) == 1 or side+':'+label in sys.argv[1:]:
                    run_case(Path(tmp), side, label)
    paths = [Path(__file__), c2.ROOT/'src/live/runtime_composition_v0_1.py',
        c2.ROOT/'src/live/execution_reconciliation_v0_1.py', c2.ROOT/'src/live/execution_message_v0_1.py',
        c2.ROOT/'src/live/execution_signer_v0_1.py', c2.ROOT/'src/live/authority_message_control_v0_1.py',
        c2.ROOT/'scripts/live_step10_s01_s02_selftest_v0_1.py']
    print(c2.canonical_json({'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'scope': 'Step10 E01 only',
        'qualification': 'SYNTHETIC_ENGINEERING_ONLY', 'checks': CHECKS, 'evidence': EVIDENCE,
        'artifact_hashes': {str(p.relative_to(c2.ROOT)).replace('\\', '/'): s1.lf_sha256(p) for p in paths},
        'accepted_lower_level_facts_not_rerun': 'Steps4-9 generic guard/fault/cut catalogs remain prior evidence; this adds owned runtime transitions.'}))


if __name__ == '__main__':
    main()
