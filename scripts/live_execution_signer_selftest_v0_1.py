"""Q2 real producer/A4b/local crypto; temporary DBs and ephemeral synthetic keys.

Private keys are generated in memory and never serialized, printed or stored.
Old fixture modules supply only original external public facts. Every tested
approval comes from actual A4b after actual Q1 production and preparation.
"""
from __future__ import annotations

import base64
import copy
import pickle
import sqlite3
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from solders.keypair import Keypair
from solders.signature import Signature
from solders.transaction import VersionedTransaction

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
import live_execution_message_selftest_v0_1 as q1
import live.execution_signer_v0_1 as signer
from live.authority_message_control_v0_1 import FreshStageConsumption, MessageProfileCommand
from live.authority_message_control_codec_v0_1 import receipt_from_json, receipt_to_json
from live.authority_message_codec_v0_1 import encode_validation_input
from live.ledger_domain_v0_1 import LedgerContractError
from live.evidence_store_v0_1 import SourceEvidenceStore
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint

a4, a3, sf, control, NOW = q1.a4, q1.a3, q1.sf, q1.control, q1.NOW
CHECKS = {}


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def fails(call):
    try:
        call()
    except (ValueError, TypeError, RuntimeError, AttributeError, LedgerContractError):
        return True
    return False


@contextmanager
def key_calls():
    """Observe the actual solders primitive without substituting a signer."""
    calls = []
    previous = sys.getprofile()
    def profile(frame, event, arg):
        if event == 'c_call' and getattr(arg, '__name__', '') == 'sign_message':
            calls.append(True)
    sys.setprofile(profile)
    try:
        yield calls
    finally:
        sys.setprofile(previous)


@contextmanager
def fixture(directory, name, *, side='BUY', route='pump', transform=None):
    key = Keypair()
    # Alter only ephemeral fixture public wallet constants. No key seed has a
    # file, environment, argument, representation or deterministic persisted form.
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f, settlement, scenario, action = a4.fixture(directory, name, route)
        try:
            at = NOW+9
            if side == 'SELL':
                prep, chain, _ = a3.actual_finality(f, settlement, action)
                support, post, request = a3.cf.composed_support(f.repo, settlement)
                post.accounts[sf.MINT] = a3.account(a4.venue.TOKEN_PROGRAM_ID, sf.plans.fx.mint_account().data)
                post.slot_calls = post.genesis_calls = 0
                support = a3.WalletSupportInput(a3.observe(post, request=request, at=NOW+14), a3.utc(NOW+14), request.min_context_slot)
                handoff = a3.ports.handoff(f.repo, action, prep, chain, support)
                f.repo.apply_settlement_with_ports(prep.attempt_id, chain_receipt_key='transaction', support=support,
                    ingestion_key='protected', recorded_at_utc=a3.utc(NOW+14), handoff=handoff, fence=f.repo.write_fence())
                buy = action
                action = a3.PendingAction(buy.root_id, buy.candidate_digest, 'SELL', buy.mint, buy.token_program,
                    buy.position_id, settlement.actual_base//3, 'EXTERNAL_RUNTIME_REDUCTION', content_fingerprint('runtime'),
                    buy.policy_ref, buy.policy_digest, buy.selected_exit_track, handoff.obligation_id, 1)
                f.repo.stage_action(action, fence=f.repo.write_fence())
                intent = q1._intent(q1.capture_message_context(f.repo, action.action_id), (NOW+24)*1000000)
                seed, _ = a4.evidence(f, action, post, route=route, at=NOW+24, context_slot=116, intent=intent, wallet_floor=116)
                at = NOW+24
            else:
                seed, _ = a4.evidence(f, action, scenario, route=route)
            if transform:
                seed = transform(seed)
            q1.install(f, seed)
            production, _ = q1.produce(f, action, seed)
            stored = q1.prepare_exact_message(f.repo, production, fence=f.repo.write_fence())
            request = control.request(f, stored.preparation, 'sign')
            delivery = control.consume(f, production.original, request, at=at)
            check(name+'_actual_Q1_A4b', type(delivery) is FreshStageConsumption)
            yield f, key, production, delivery, at
        finally:
            f.close()


def invoke(f, key, production, delivery, at, **kwargs):
    return signer.AutonomousLocalSigner(key).sign_exact(f.repo, production, delivery,
        source_store=kwargs.pop('source_store', f.source if production.original.context.action.side == 'BUY' else None),
        clock=kwargs.pop('clock', lambda: a3.clock(f.repo, at+1)), **kwargs)


def healthy(directory):
    for side in ('BUY', 'SELL'):
        for route in ('pump', 'swap'):
            name = side+'_'+route
            with fixture(directory, name, side=side, route=route) as (f, key, production, delivery, at):
                before = f.repo.write_fence()
                source_before = f.source.latest_record()
                with key_calls() as calls:
                    envelope = invoke(f, key, production, delivery, at)
                check(name+'_one_actual_local_key_call', len(calls) == 1)
                wire = base64.b64decode(envelope.signed_wire_base64)
                check(name+'_exact_original_bytes', wire[65:].hex() == production.message_hex)
                check(name+'_cryptographically_verified', all(VersionedTransaction.from_bytes(wire).verify_with_results())
                    and Signature.from_string(envelope.primary_signature).verify(key.pubkey(), wire[65:]))
                check(name+'_actual_units_root', envelope.preparation.action_content_digest == production.original.context.action.content_digest)
                check(name+'_no_persistence_or_source_mutation', f.repo.write_fence() == before and f.source.latest_record() == source_before
                    and f.repo.attempt(envelope.preparation.attempt_id).primary_signature is None)
                check(name+'_immutable_public_output', fails(lambda: setattr(envelope, 'primary_signature', 'changed')) and not envelope.may_send)
                check(name+'_public_roundtrip', replace(envelope) == envelope and len(envelope.content_digest) == 64)
                check(name+'_copy_is_same_delivery', copy.copy(delivery) is delivery and copy.deepcopy(delivery) is delivery)
                check(name+'_delivery_not_serializable', fails(lambda: pickle.dumps(delivery)))
                retry = control.consume(f, production.original, delivery.receipt.original.request,
                    sample=delivery.receipt.original.validation.clock)
                check(name+'_exact_A4b_retry_historical', type(retry) is not FreshStageConsumption and retry == delivery.receipt)
                with key_calls() as calls:
                    for historical in (delivery, copy.copy(delivery), delivery.receipt, retry,
                            receipt_from_json(receipt_to_json(delivery.receipt)), f.repo.authority_message_receipt('sign')):
                        check(name+'_replay_'+str(len(CHECKS)), fails(lambda: invoke(f, key, production, historical, at)))
                check(name+'_replay_no_key_call', not calls)
                other = Keypair()
                wrong = other.sign_message(wire[65:])
                bad_wire = base64.b64encode(b'\x01'+bytes(wrong)+wire[65:]).decode()
                check(name+'_wrong_signer_return_rejected', fails(lambda: replace(envelope,
                    primary_signature=str(wrong), signed_wire_base64=bad_wire)))
                changed = bytes([wire[-1]^1])
                check(name+'_altered_signed_bytes_rejected', fails(lambda: replace(envelope,
                    signed_wire_base64=base64.b64encode(wire[:-1]+changed).decode())))
                check(name+'_wrong_signature_rejected', fails(lambda: replace(envelope, primary_signature=str(Signature.default()))))
                f.repo.audit()


def denials(directory):
    labels = ('wrong-key', 'old-clock-object', 'old-clock-copy', 'clock-unknown', 'clock-epoch', 'clock-regressed',
        'clock-chain', 'clock-error', 'clock-boundary-error', 'deadline', 'hard-stop', 'entry-stop', 'disarm', 'revoke', 'profile-reselected',
        'attempt-advanced', 'source-changed', 'source-missing', 'production-changed', 'message-changed', 'reopen',
        'clock-control-callback', 'clock-source-unlock', 'clock-ledger-unlock', 'clock-reentrant', 'existing-read')
    for label in labels:
        with fixture(directory, label) as (f, key, production, delivery, at):
            receipt = delivery.receipt
            clock = lambda: a3.clock(f.repo, at+1)
            source = f.source
            if label == 'wrong-key': key = Keypair()
            elif label == 'old-clock-object': clock = lambda: receipt.original.validation.clock
            elif label == 'old-clock-copy': clock = lambda: replace(receipt.original.validation.clock)
            elif label == 'clock-unknown': clock = lambda: a3.clock(f.repo, at+1, status='UNKNOWN')
            elif label == 'clock-epoch': clock = lambda: a3.clock(f.repo, at+1, epoch='other-boot')
            elif label == 'clock-regressed': clock = lambda: a3.clock(f.repo, at-1)
            elif label == 'clock-chain': clock = lambda: replace(a3.clock(f.repo, at+1), previous_sample_digest='0'*64)
            elif label == 'clock-error':
                def clock(): raise RuntimeError('PRIVATE_CALLBACK_SENTINEL')
            elif label == 'clock-boundary-error':
                def clock(): raise signer.ExecutionSigningError('PRIVATE_CALLBACK_SENTINEL')
            elif label == 'deadline': clock = lambda: a3.clock(f.repo, NOW+15)
            elif label in ('hard-stop', 'entry-stop', 'disarm', 'revoke'):
                a3.control(f.repo, {'hard-stop':'HARD_STOP', 'entry-stop':'STOP_ENTRY', 'disarm':'DISARM_ENTRY', 'revoke':'REVOKE_GRANT'}[label],
                    label, at=at, **({'target': f.repo.authority_acceptance(f.root).eligibility.decision.grant_id} if label=='revoke' else {}))
            elif label == 'profile-reselected':
                profile = production.original.profile
                f.repo.record_authority_message_profile(MessageProfileCommand('reselect', 'SELECT_EXISTING', profile, profile.approval), fence=f.repo.write_fence())
            elif label == 'attempt-advanced': control.advance(f, f.repo.attempt(receipt.original.request.attempt_id).preparation, 'EXACT_SIMULATED')
            elif label == 'source-changed':
                previous = f.source.latest_record()[1]
                a3.a1.source_fixture.change(f.raw, 'DELETE FROM websocket_observations')
                gap = a3.a1.source_fixture.observe(f.raw, f.source.binding, previous=previous, now=8, cut=5)
                f.source.append(gap, expected_previous_digest=previous.content_digest)
            elif label == 'source-missing': source = None
            elif label == 'production-changed':
                changed = replace(production.original, clock=replace(production.original.clock, observation_record_digest='1'*64))
                production = replace(production, validation_input_json=encode_validation_input(changed))
            elif label == 'message-changed':
                changed, _ = q1.produce(f, production.original.context.action, production.original,
                    compute=q1.ComputeBudget(250000, 900))
                check('message_changed_actual_Q1_bytes', changed.message_hex != production.message_hex)
                production = changed
            elif label == 'reopen': f.reopen()
            elif label == 'clock-control-callback':
                def clock():
                    try: a3.control(f.repo, 'HARD_STOP', 'callback-stop', at=at)
                    except LedgerContractError: pass
                    return a3.clock(f.repo, at+1)
            elif label in ('clock-source-unlock', 'clock-ledger-unlock'):
                def clock():
                    (f.source if label == 'clock-source-unlock' else f.repo)._conn.execute('ROLLBACK')
                    return a3.clock(f.repo, at+1)
            elif label == 'clock-reentrant':
                def clock():
                    invoke(f, key, production, delivery, at)
                    return a3.clock(f.repo, at+1)
            elif label == 'existing-read': f.repo._conn.execute('BEGIN')
            try:
                with key_calls() as calls:
                    try: invoke(f, key, production, delivery, at, clock=clock, source_store=source)
                    except signer.ExecutionSigningError as exc:
                        check(label+'_sanitized_rejection', 'PRIVATE' not in str(exc))
                    else: raise AssertionError(label)
                check(label+'_before_key_call', not calls)
            finally:
                if f.repo._conn.in_transaction: f.repo._conn.execute('ROLLBACK')
                if f.source._conn.in_transaction: f.source._conn.execute('ROLLBACK')
            with key_calls() as calls:
                check(label+'_spent_even_on_failure', fails(lambda: invoke(f, key, production, delivery, at)))
            check(label+'_retry_no_key', not calls)
            check(label+'_retains_lane', f.repo.attempt(receipt.original.request.attempt_id).lane_held)
            f.repo.audit()


def failure_cuts(directory):
    with fixture(directory, 'copied-source-store') as (f, key, production, delivery, at):
        with SourceEvidenceStore(directory/'source-copy.sqlite3', f.source.binding, f.source.profile) as clone:
            original = f.source.latest_record()[1]
            clone.append(original, expected_previous_digest='0'*64)
            a3.a1.source_fixture.change(f.raw, 'DELETE FROM websocket_observations')
            gap = a3.a1.source_fixture.observe(f.raw, f.source.binding, previous=original, now=8, cut=5)
            f.source.append(gap, expected_previous_digest=original.content_digest)
            check('source_copy_retains_same_original_row', clone.latest_record() ==
                (delivery.receipt.original.source.source_sequence, delivery.receipt.original.source.source))
            with key_calls() as calls:
                check('copied_source_store_denied', fails(lambda: invoke(f, key, production, delivery, at, source_store=clone)))
            check('copied_source_store_before_key', not calls)
    with fixture(directory, 'lease-expires-at-call', transform=lambda seed: replace(seed,
            profile=replace(seed.profile, maximum_lease_age_us=3000000))) as (f, key, production, delivery, at):
        with key_calls() as calls:
            check('lease_fresh_at_consume_stale_at_call', fails(lambda: invoke(f, key, production, delivery, at)))
        check('stale_lease_before_key', not calls)
    for surface in ('ledger', 'source'):
        with fixture(directory, 'closed-'+surface) as (f, key, production, delivery, at):
            def clock():
                sample = a3.clock(f.repo, at+1)
                (f.repo if surface == 'ledger' else f.source).close()
                return sample
            with key_calls() as calls:
                check(surface+'_closed_callback_denied', fails(lambda: invoke(f, key, production, delivery, at, clock=clock)))
            check(surface+'_closed_callback_before_key', not calls)
            if surface == 'ledger': f.reopen()
            check(surface+'_closed_callback_ledger_lock_released', not f.repo._conn.in_transaction)
    with fixture(directory, 'begin-validation-failure') as (f, key, production, delivery, at):
        original_begin = f.repo._begin_economic_write
        def begin_then_fail(fence):
            original_begin(fence)
            raise LedgerContractError('SYNTHETIC_AFTER_BEGIN_REJECTION')
        with patch.object(f.repo, '_begin_economic_write', begin_then_fail), key_calls() as calls:
            check('begin_validation_failure_rejected', fails(lambda: invoke(f, key, production, delivery, at)))
        check('begin_validation_failure_before_key', not calls)
        check('begin_validation_failure_releases_owned_lock', not f.repo._conn.in_transaction)
    with fixture(directory, 'lost-return') as (f, key, production, delivery, at):
        with key_calls() as calls, patch.object(signer, 'VerifiedSignedEnvelope', side_effect=RuntimeError('PRIVATE_RETURN_SENTINEL')):
            check('lost_return_sanitized', fails(lambda: invoke(f, key, production, delivery, at)))
        check('lost_return_one_key_call', len(calls) == 1)
        attempt = f.repo.attempt(delivery.receipt.original.request.attempt_id)
        check('lost_return_no_durable_signature', attempt.primary_signature is None)
        cancelled = f.repo.cancel_attempt_locally(attempt.preparation.attempt_id, recorded_at_utc=a3.utc(at+2),
            idempotency_key='cancel-after-lost-return', expected_attempt_revision=attempt.revision, fence=f.repo.write_fence())
        check('lost_return_unknown_not_unsigned', cancelled.recorded_stage == 'UNKNOWN' and cancelled.lane_held)
        f.reopen()
        with key_calls() as calls:
            check('lost_return_reopen_no_redelivery', fails(lambda: invoke(f, key, production, f.repo.authority_message_receipt('sign'), at)))
        check('lost_return_reopen_no_key', not calls)
        f.repo.audit()
    with fixture(directory, 'concurrent-claim') as (_, key, _, delivery, _):
        def claim(_): return not fails(delivery._claim_execution)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = tuple(pool.map(claim, range(2)))
        check('concurrent_delivery_exactly_once', sum(results) == 1)
        facility = signer.AutonomousLocalSigner(key)
        check('signer_not_serializable', fails(lambda: pickle.dumps(facility)))
        check('signer_single_public_operation', [name for name in dir(facility) if not name.startswith('_')] == ['sign_exact'])
        check('no_generic_key_adapter', fails(lambda: signer.AutonomousLocalSigner(lambda raw: raw)))
    with fixture(directory, 'outside-writer-locks') as (f, key, production, delivery, at):
        blocked = []
        def clock():
            for path in (f.path, Path(f.source._conn.execute('PRAGMA database_list').fetchone()[2])):
                conn = sqlite3.connect(path, timeout=0, isolation_level=None)
                try:
                    try: conn.execute('BEGIN IMMEDIATE')
                    except sqlite3.OperationalError: blocked.append(True)
                    else:
                        blocked.append(False)
                        conn.execute('ROLLBACK')
                finally: conn.close()
            return a3.clock(f.repo, at+1)
        invoke(f, key, production, delivery, at, clock=clock)
        check('both_journals_writer_locked_through_callback', blocked == [True, True])
        check('no_lingering_transactions', not f.repo._conn.in_transaction and not f.source._conn.in_transaction)


def main():
    with tempfile.TemporaryDirectory(prefix='live-execution-q2-') as directory:
        directory = Path(directory)
        healthy(directory)
        denials(directory)
        failure_cuts(directory)
    print(canonical_json({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'checks':len(CHECKS), 'all_checks':all(CHECKS.values())}))


if __name__ == '__main__':
    main()
