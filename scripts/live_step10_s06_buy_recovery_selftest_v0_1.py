"""S06 bounded resolved-BUY Runtime correction regression.

Two owned synthetic cases only: failed BUY retry, and expired failed BUY
retirement. Real reducers/ports; no direct retry/retirement orchestration bypass.
"""
from __future__ import annotations
import base64
import copy
import hashlib
import json
import sys
import tempfile
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
import live_step10_s01_s02_selftest_v0_1 as prior
import live_step10_s04_s08_selftest_v0_1 as owned
from solders.message import to_bytes_versioned

ops, c2, a3, sf, NOW = prior.ops, prior.c2, prior.a3, prior.sf, prior.NOW
CHECKS, EVIDENCE = {}, {}
EVIDENCE_ROOT = Path(tempfile.gettempdir())/'meme-live-step10-3fd8bcb-20260911'


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def send(f, key, at, number, *, failed=False):
    with prior.external_mint(f.buy.mint), prior.retained_accumulator_rpc(f, f.buy):
        result, reads, sends = c2.execution(f, key, f.buy, at=at, number=number)
        check(f.name+'_'+str(number)+'_actual_send', result.work == 'SUBMISSION_OBSERVED'
            and len(sends.requests) == 1)
        # Failed external tx/root land at send+1/+3; retained wallet cut is
        # available before the helper's retry wallet observation (retry-4).
        envelope, actual, scenario, pre = c2.original_chain(f, result, at-2 if failed else at, failed=failed)
    wire = base64.b64decode(envelope.signed_wire_base64)
    simulation = next(r for r in reads.requests if r['method'] == 'simulateTransaction')
    check(f.name+'_'+str(number)+'_exact_simulated_signed_sent',
        to_bytes_versioned(c2.VersionedTransaction.from_bytes(base64.b64decode(simulation['params'][0])).message)
            == bytes.fromhex(envelope.preparation.message_hex)
        and sends.requests[0]['params'][0] == envelope.signed_wire_base64
        and all(c2.VersionedTransaction.from_bytes(wire).verify_with_results()))
    return result, envelope, actual, scenario, pre


def apply(f, sent, at, *, negative=False):
    result, envelope, actual, scenario, pre = sent
    native = f.repo.consumer_snapshot()['funding'].native_lamports
    if negative:
        unresolved = copy.deepcopy(scenario)
        unresolved.tx = unresolved.status = None
        observed, _ = c2.reconcile(f, unresolved, at-1)
        pending = f.step(at-1)
        check(f.name+'_UNKNOWN_never_enters_retry_branch', observed.reason == 'UNKNOWN'
            and pending.work == 'NEED_RECONCILIATION' and pending.attempt_id == result.attempt_id
            and f.repo.attempt(result.attempt_id).lane_held and f.repo.audit()['attempt_count'] == 1)
    reconciled, _ = c2.reconcile(f, scenario, at-1)
    chain = f.repo.chain_receipt(reconciled.receipt_key)
    check(f.name+'_'+str(at)+'_exact_public_finality_before_application',
        reconciled.reason in ('FINALIZED_FAILURE_UNAPPLIED', 'FINALIZED_SUCCESS_UNAPPLIED')
        and chain.observation.transaction.wire_bytes == base64.b64decode(envelope.signed_wire_base64)
        and f.repo.consumer_snapshot()['funding'].native_lamports == native)
    if negative:
        wait = f.step(at-1)
        check(f.name+'_positive_proof_without_application_cannot_retry',
            wait.work == 'NEED_APPLICATION' and wait.attempt_id == result.attempt_id
            and f.repo.attempt(result.attempt_id).lane_held and f.repo.audit()['attempt_count'] == 1)
    earlier = None
    if reconciled.reason == 'FINALIZED_FAILURE_UNAPPLIED':
        stale = prior.wallet(f, at-1, mint=f.buy.mint)
        rejected = f.step(at-1, application_wallet=stale)
        earlier = f.repo.application_receipt(rejected.receipt_key)
        check(f.name+'_earlier_stale_application_retained_UNAPPLIED',
            rejected.reason == 'UNAPPLIED' and earlier.decision.disposition == 'UNAPPLIED'
            and f.repo.attempt(result.attempt_id).lane_held
            and f.repo.consumer_snapshot()['funding'].native_lamports == native)
    support = c2.application_support(f, actual, chain, pre, at)
    applied = f.step(at, application_wallet=support)
    receipt = f.repo.application_receipt(applied.receipt_key)
    proposal = receipt.decision.proposal
    if earlier is not None:
        check(f.name+'_later_application_owns_actual_resolution',
            receipt.sequence > earlier.sequence
            and f.repo._custody.resolution(result.attempt_id).application_sequence == receipt.sequence)
    check(f.name+'_'+str(at)+'_actual_accounting',
        proposal.transaction_fee_lamports == sf.FEE
        and f.repo.consumer_snapshot()['funding'].native_lamports == native+proposal.native_wallet_delta
        and f.repo.consumer_snapshot()['funding'].native_lamports == f.public[f.domain.wallet].lamports)
    before = f.repo.audit()
    replay = f.repo.apply_settlement(result.attempt_id, chain_receipt_key=chain.ingestion_key,
        support=support, ingestion_key=applied.receipt_key, recorded_at_utc=receipt.recorded_at_utc,
        fence=f.repo.write_fence())
    check(f.name+'_'+str(at)+'_application_replay_exactly_once', replay == receipt and f.repo.audit() == before)
    return receipt, support


def stages(f, attempt_id):
    with f.repo._trusted_read():
        keys = f.repo._conn.execute('SELECT command_id FROM ledger_authority_message_stages WHERE attempt_id=? ORDER BY commit_seq',
            (attempt_id,)).fetchall()
        return tuple(f.repo.authority_message_receipt(key) for key, in keys)


def public_attempt(sent, application):
    result, envelope, actual, scenario, pre = sent
    return {'attempt_id': result.attempt_id, 'signature': envelope.primary_signature,
        'message_hex': envelope.preparation.message_hex, 'message_sha256': envelope.preparation.message_sha256,
        'signed_wire_base64': envelope.signed_wire_base64,
        'signed_wire_sha256': hashlib.sha256(base64.b64decode(envelope.signed_wire_base64)).hexdigest(),
        'ordinal': envelope.preparation.ordinal, 'application': application.to_record()}


def run(directory, *, expired=False):
    name = 's06-expired-failed-BUY' if expired else 's06-same-root-failed-BUY-retry'
    key = c2.keypair(name)
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = prior.prepare(directory, name)
        try:
            original_action, policy = f.buy, f.policy
            original_grant = f.repo.authority_acceptance(f.buy.root_id).eligibility.decision.grant_id
            check(name+'_original_policy_permits_failure_budget', policy.costs.failed_attempt_count == 2
                and policy.costs.failed_attempt_network_budget_lamports == 20000)
            first = send(f, key, NOW+6, 181, failed=True)
            failed, stale_support = apply(f, first, NOW+11, negative=not expired)
            proposal = failed.decision.proposal
            check(name+'_applied_failure_fee_no_acquisition',
                failed.decision.disposition == 'FINALIZED_FAILURE_APPLIED'
                and proposal.native_wallet_delta == -sf.FEE and proposal.base_units_delta == 0
                and f.repo.consumer_snapshot()['funding'].native_lamports == sf.FUNDING-sf.FEE
                and not f.repo.consumer_snapshot()['positions'] and not f.repo.consumer_snapshot()['pending_attempts']
                and f.repo.reservation(f.buy.root_id).status == 'RESERVED')
            record = {'configuration': prior.EVIDENCE[name], 'action': asdict(original_action),
                'first_failed': public_attempt(first, failed), 'original_grant_id': original_grant,
                'after_failure': prior.stamp(f, 'applied failed BUY')}
            EVIDENCE[name] = record
            if expired:
                center = datetime.fromisoformat(a3.utc(NOW+15))
                sample = replace(a3.clock(f.repo, NOW+15),
                    utc_lower_utc=(center-timedelta(microseconds=1)).isoformat(timespec='microseconds'),
                    utc_upper_utc=(center+timedelta(microseconds=1)).isoformat(timespec='microseconds'))
                ambiguous = f.runtime.step(clock=lambda: sample, source_cut_utc=a3.utc(f.source_cut),
                    operations_resources=lambda: ops.host(f, NOW+15))
                check(name+'_deadline_ambiguity_holds', ambiguous.work == 'HELD'
                    and ambiguous.reason == 'ORIGINAL_DEADLINE_AMBIGUOUS')
                missing = f.step(NOW+16)
                stale = f.step(NOW+16, retirement_wallet=stale_support)
                check(name+'_missing_or_stale_wallet_keeps_capacity', missing.work == 'NEED_RETIREMENT'
                    and stale.work == 'RETIREMENT_WITHHELD' and f.repo.reservation(f.buy.root_id).status == 'RESERVED'
                    and f.repo.audit()['attempt_count'] == 1)
                old_owner = f.runtime._ownership.fence
                audit = owned.reopen(f)
                missing_again = f.step(NOW+17)
                check(name+'_cold_owner_retains_expired_retirement_priority',
                    audit.owner_fence.generation > old_owner.generation and missing_again.work == 'NEED_RETIREMENT'
                    and f.runtime.position_binding is None and f.repo.action(f.buy.action_id) == original_action)
                incomplete = f.step(NOW+18, retirement_wallet=prior.continuation.wallet(f, NOW+18))
                record['incomplete_wallet'] = asdict(incomplete)
                check(name+'_incomplete_absence_coverage_keeps_capacity',
                    incomplete.work == 'RETIREMENT_WITHHELD'
                    and 'COMPARISON_EXPLICIT_KNOWN_ACCOUNT_COVERAGE_MISSING'
                        in f.repo.consumer_snapshot()['latest_comparison'].reasons
                    and f.repo.reservation(f.buy.root_id).status == 'RESERVED')
                support = prior.wallet(f, NOW+19, mint=f.buy.mint)
                retired = f.step(NOW+19, retirement_wallet=support)
                record['retirement_result'] = asdict(retired)
                receipt = f.repo.port_receipt(retired.receipt_key)
                check(name+'_current_wallet_retires_no_acquisition', retired.work == 'RETIREMENT_RETIRED'
                    and receipt.retirement.reason == 'FAILED_NO_ACQUISITION'
                    and receipt.retirement.terminal_attempt_id == first[0].attempt_id
                    and f.repo.reservation(f.buy.root_id).terminal_disposition == 'CLOSED_NO_ACQUISITION'
                    and not f.repo.consumer_snapshot()['reservations'] and f.runtime.position_binding is None)
                before = f.repo.audit()
                exact = f.repo.retire(receipt.retirement, support, ingestion_key=retired.receipt_key,
                    fence=f.repo.write_fence())
                check(name+'_exact_retirement_replay_no_mutation', exact == receipt and f.repo.audit() == before)
                owned.reopen(f)
                repeated = f.step(NOW+20)
                denied = f.repo.admit_authority_entry(a3.EntryRequest(f.buy.root_id, 'PUMP', sf.TOKEN_PROGRAM_ID, policy.selected_track),
                    a3.clock(f.repo, NOW+21), f.source, prior.wallet(f, NOW+21, mint=f.buy.mint),
                    command_id=name+'-old-root', fence=f.repo.write_fence())
                check(name+'_retired_root_cannot_retry_or_reacquire', not denied.accepted
                    and f.repo.audit()['attempt_count'] == f.repo.audit()['action_count'] == 1
                    and not f.repo.consumer_snapshot()['reservations'] and not f.repo.consumer_snapshot()['positions']
                    and repeated.work != 'NEED_EXECUTION')
                record.update(ambiguous=asdict(ambiguous), missing=asdict(missing), stale=asdict(stale),
                    retired=asdict(retired), retirement=asdict(receipt.retirement),
                    old_root_denial=denied.risk.reasons, cold_repeated=asdict(repeated))
            else:
                pending = f.step(NOW+14)
                old_owner = f.runtime._ownership.fence
                audit = owned.reopen(f)
                repeated = f.step(NOW+14)
                check(name+'_same_action_retry_priority_after_owned_reconstruction',
                    pending.work == repeated.work == 'NEED_EXECUTION' and repeated.action_id == original_action.action_id
                    and audit.owner_fence.generation > old_owner.generation and f.repo.audit()['attempt_count'] == 1)
                retry = send(f, key, NOW+14, 182)
                second_stages = stages(f, retry[0].attempt_id)
                first_stages = stages(f, first[0].attempt_id)
                check(name+'_new_ordinal_message_signature_same_original_action',
                    retry[1].preparation.ordinal == 2 and retry[1].preparation.action_id == original_action.action_id
                    and retry[1].preparation.message_sha256 != first[1].preparation.message_sha256
                    and retry[1].primary_signature != first[1].primary_signature
                    and f.repo.action(original_action.action_id) == original_action
                    and (NOW+14)*1000000 < original_action.claimed_entry_deadline_us)
                check(name+'_fresh_SIGN_and_SEND_permission_each_attempt',
                    len(first_stages) == len(second_stages) == 2
                    and [r.original.request.stage for r in second_stages] == ['SIGN','SEND']
                    and all(r.consumed for r in first_stages+second_stages)
                    and not {r.content_digest for r in first_stages}&{r.content_digest for r in second_stages}
                    and all(r.decision.risk.failed_attempt_count == 1
                        and r.decision.risk.remaining_failed_attempt_count == 1
                        and r.decision.risk.failure_allowance_lamports == 20000-sf.FEE for r in second_stages))
                succeeded, _ = apply(f, retry, NOW+22)
                check(name+'_retry_success_keeps_first_actual_fee',
                    succeeded.decision.disposition == 'FINALIZED_SUCCESS_APPLIED'
                    and f.repo.consumer_snapshot()['funding'].native_lamports
                        == sf.FUNDING-sf.FEE+succeeded.decision.proposal.native_wallet_delta
                    and f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports == 2*sf.FEE
                    and f.runtime.position_binding.acquisition_application_key == succeeded.ingestion_key)
                protected = f.step(NOW+24)
                check(name+'_successful_original_root_protected_not_reacquired',
                    protected.work == 'PROTECTIVE_ACTION_STAGED'
                    and f.repo.action(protected.action_id).side == 'SELL'
                    and f.repo.action(protected.action_id).root_id == original_action.root_id
                    and f.repo.position_history(original_action.position_id).remaining_units == retry[2].actual_base
                    and f.repo.audit()['attempt_count'] == 2)
                record.update(retry=public_attempt(retry, succeeded),
                    retry_stages=[{'command_id':r.original.request.command_id, 'stage':r.original.request.stage,
                        'receipt_digest':r.content_digest, 'consumed':r.consumed, 'risk':asdict(r.decision.risk),
                        'clock':asdict(r.original.validation.clock)} for r in second_stages],
                    after_reopen=asdict(repeated), protected=asdict(protected),
                    position=asdict(f.repo.position_history(original_action.position_id)))
            check(name+'_original_policy_and_grant_unchanged', f.repo.authority_snapshot()['policy'] == policy
                and f.repo.authority_acceptance(original_action.root_id).eligibility.decision.grant_id == original_grant
                and all(f.repo.authority_snapshot()[field] == f.original_authority[field]
                    for field in ('policy','armed_entry_grant','entry_stop_command','hard_stop_command')))
            record.update(final=prior.stamp(f, 'final bounded resolved BUY branch'), audit=f.repo.audit(),
                gate=asdict(f.runtime._operations_degradation.snapshot()))
        finally:
            ops.close(f)


def main():
    preserved = {'scripts/live_step10_s01_s02_selftest_v0_1.py':'775a01bcc7b7cc1142704e266c03dc64a377e33003dd785a2f5606822ea33964',
        'scripts/live_step10_s04_s08_selftest_v0_1.py':'112b2a171adbbabb784b189643e2fb52b78be90f542a5421001cb77cd7b74092',
        'scripts/live_step10_s05_selftest_v0_1.py':'722d59de66259cfa15974408f984dafdd7e7ee2bd5045d791f475346dc761afa'}
    for path,digest in preserved.items():
        check('preserved_'+path, prior.lf_sha256(c2.ROOT/path)==digest)
    with tempfile.TemporaryDirectory(prefix='step10-s06-buy-recovery-') as tmp:
        run(Path(tmp))
        run(Path(tmp), expired=True)
    paths={Path(__file__),c2.ROOT/'docs/live/MEME_LIVE_PRODUCTION_CLOSURE_ARCHITECTURE_V2.md'}
    paths.update(Path(m.__file__).resolve() for m in tuple(sys.modules.values())
        if getattr(m,'__file__',None) and Path(m.__file__).suffix=='.py'
        and Path(m.__file__).resolve().is_relative_to(c2.ROOT))
    print(c2.canonical_json({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW',
        'scope':'S06_BOUNDED_RESOLVED_BUY_RUNTIME_CORRECTION', 'qualification':'SYNTHETIC_ENGINEERING_ONLY',
        'fixture_version':'STEP10_S06_BUY_RECOVERY_V01',
        'checks':CHECKS,'reused_helper_checks':prior.CHECKS,'owned_helper_checks':owned.CHECKS,'evidence':EVIDENCE,
        'limitations':['Full S06 failure/nonlanding/residual permutations remain unqualified',
            'No production key/network or real host qualification; synthetic policy and monitor'],
        'historical_probe':{'path':str(EVIDENCE_ROOT/'S06.historical_probe.stdout.json'),
            'sha256':hashlib.sha256((EVIDENCE_ROOT/'S06.historical_probe.stdout.json').read_bytes()).hexdigest()},
        'artifact_hashes':{str(p.relative_to(c2.ROOT)).replace('\\','/'):prior.lf_sha256(p) for p in sorted(paths)}}))


if __name__ == '__main__':
    try:
        main()
    except BaseException:
        print(json.dumps({'checks':CHECKS,'evidence':EVIDENCE},default=lambda value:value.hex() if isinstance(value,bytes) else str(value)),file=sys.stderr)
        raise
