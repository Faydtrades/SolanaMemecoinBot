"""Step10 E02: owned Runtime partial reduction, residual and original cost denial.

Two fresh canonical/admitted synthetic journeys. Only external public I/O and
an ephemeral signer are fixtures. No direct stage/bind/settlement proposal,
permission injection, policy mutation, network or prior suite main is used.
"""
from __future__ import annotations
import argparse
import base64
import hashlib
import json
import sys
import tempfile
import time
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

import live_step10_s01_s02_selftest_v0_1 as prior
import live_step10_s04_s08_selftest_v0_1 as owned
import live_step10_s06_buy_recovery_selftest_v0_1 as receipts
from solders.message import to_bytes_versioned
import live.runtime_composition_v0_1 as runtime_module
from live.authority_message_control_codec_v0_1 import receipt_to_json, receipt_from_json

ops, c2, a3, sf, NOW = prior.ops, prior.c2, prior.a3, prior.sf, prior.NOW
CHECKS, EVIDENCE = {}, {}


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def state(f):
    return c2.protective_outcome(f.repo, f.runtime.position_binding)


def unchanged_policy(f):
    check(f.name+'_original_policy_and_controls_unchanged', all(
        f.repo.authority_snapshot()[name] == f.original_authority[name]
        for name in ('policy', 'armed_entry_grant', 'entry_stop_command', 'hard_stop_command')))


def reduction(f, key, action, at, number, *, failed=False):
    label = f.name+'_'+str(number)
    before, native = state(f), f.repo.consumer_snapshot()['funding'].native_lamports
    with prior.external_mint(action.mint), prior.retained_accumulator_rpc(f, action):
        result, reads, sends = c2.execution(f, key, action, at=at, number=number)
        check(label+'_actual_runtime_guarded_send', result.work == 'SUBMISSION_OBSERVED' and len(sends.requests) == 1)
        envelope, actual, scenario, pre = c2.original_chain(f, result, at, failed=failed)
    wire = base64.b64decode(envelope.signed_wire_base64)
    simulation = [r for r in reads.requests if r['method'] == 'simulateTransaction']
    check(label+'_exact_simulated_signed_sent_message', len(simulation) == 1
        and to_bytes_versioned(c2.VersionedTransaction.from_bytes(base64.b64decode(simulation[0]['params'][0])).message)
            == bytes.fromhex(envelope.preparation.message_hex)
        and sends.requests[0]['params'][0] == envelope.signed_wire_base64
        and all(c2.VersionedTransaction.from_bytes(wire).verify_with_results()))
    meta = scenario.tx['meta']
    base_index = actual.index[actual.base]
    pre_units = int(next(t for t in meta['preTokenBalances'] if t['accountIndex'] == base_index)['uiTokenAmount']['amount'])
    post_units = int(next(t for t in meta['postTokenBalances'] if t['accountIndex'] == base_index)['uiTokenAmount']['amount'])
    expected_reduction = 0 if failed else action.input_units
    check(label+'_exact_instruction_and_public_metadata_no_partial_fill_mismatch',
        actual.units == actual.actual_base == action.input_units
        and pre_units == before.obligation.remaining_units
        and pre_units-post_units == expected_reduction)
    sent_state = state(f)
    check(label+'_send_does_not_reduce_or_release', sent_state.state == 'HELD'
        and sent_state.obligation.remaining_units == before.obligation.remaining_units
        and not sent_state.mutation_eligible and f.repo.reservation(action.root_id).status == 'RESERVED'
        and f.repo.consumer_snapshot()['funding'].native_lamports == native)
    reconciled, calls = c2.reconcile(f, scenario, at+6)
    chain = f.repo.chain_receipt(reconciled.receipt_key)
    expected = 'FINALIZED_FAILURE_UNAPPLIED' if failed else 'FINALIZED_SUCCESS_UNAPPLIED'
    waiting = state(f)
    check(label+'_public_finality_without_application_keeps_all_original_units',
        calls and reconciled.reason == expected and waiting.state == 'HELD'
        and waiting.obligation.remaining_units == before.obligation.remaining_units
        and f.repo.reservation(action.root_id).status == 'RESERVED'
        and f.repo.consumer_snapshot()['funding'].native_lamports == native
        and chain.observation.transaction.wire_bytes == wire)
    support = c2.application_support(f, actual, chain, pre, at+8)
    applied = f.step(at+8, application_wallet=support)
    receipt = f.repo.application_receipt(applied.receipt_key)
    proposal = receipt.decision.proposal
    check(label+'_actual_exact_accounting', receipt.decision.disposition == expected.replace('UNAPPLIED','APPLIED')
        and proposal.base_units_delta == -expected_reduction
        and proposal.transaction_fee_lamports == meta['fee'] == sf.FEE
        and proposal.native_wallet_delta == meta['postBalances'][actual.index[f.domain.wallet]]-meta['preBalances'][actual.index[f.domain.wallet]]
        and f.repo.consumer_snapshot()['funding'].native_lamports == native+proposal.native_wallet_delta == f.public[f.domain.wallet].lamports
        and f.repo.position_history(action.position_id).remaining_units == before.obligation.remaining_units-expected_reduction
        and (not failed or proposal.native_wallet_delta == -sf.FEE))
    if not failed:
        check(label+'_actual_venue_principal_fee_and_component_conservation',
            proposal.venue_fee_units == actual.venue_fees and proposal.quote_principal_units == actual.principal
            and sum(c.units for c in proposal.components if c.asset == 'SOL' and c.account == f.domain.wallet) == proposal.native_wallet_delta
            and proposal.locked_account_lamports_delta == sum(c.units for c in proposal.components if c.asset == 'LOCKED_LAMPORTS'))
    audit, stable = f.repo.audit(), f.repo.position_history(action.position_id)
    replay = f.repo.apply_settlement(result.attempt_id, chain_receipt_key=chain.ingestion_key,
        support=support, ingestion_key=applied.receipt_key, recorded_at_utc=receipt.recorded_at_utc,
        fence=f.repo.write_fence())
    check(label+'_duplicate_application_never_reduces_or_charges_again',
        replay == receipt and f.repo.audit() == audit and f.repo.position_history(action.position_id) == stable)
    stages = receipts.stages(f, result.attempt_id)
    check(label+'_fresh_original_SIGN_and_SEND_permissions',
        [r.original.request.stage for r in stages] == ['SIGN','SEND'] and all(r.consumed for r in stages)
        and all(r.original.validation.context.action == action for r in stages))
    evidence = receipts.public_attempt((result, envelope, actual, scenario, pre), receipt)
    evidence.update(action=asdict(action), public_base_before=pre_units, public_base_after=post_units,
        native_before=native, native_after=f.public[f.domain.wallet].lamports,
        authority_stages=[{'stage':r.original.request.stage,'digest':r.content_digest,
            'consumed':r.consumed,'risk':asdict(r.decision.risk)} for r in stages],
        chain_evidence_digest=chain.observation.content_digest)
    return result, receipt, evidence


def run(directory, *, exhaust=False):
    name = 'e02-partial-residual-cost-exhaustion' if exhaust else 'e02-partial-residual-retirement'
    key = c2.Keypair()  # Ephemeral external synthetic signer; secret never serialized.
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = prior.prepare(directory, name)
        try:
            record = {'configuration':prior.EVIDENCE[name], 'before_acquisition':prior.stamp(f,'admitted original root')}
            EVIDENCE[name] = record
            applied_buy, buy_evidence = prior.transact(f, key, f.buy, NOW+6, 71)
            quantity, binding = buy_evidence['actual_base_units'], f.runtime.position_binding
            monitored = f.step(NOW+14)
            check(name+'_actual_owned_acquisition_and_selected_controller', monitored.work == 'ENTRY_HELD'
                and state(f).state == 'MONITORING' and binding.spec.track_id == f.buy.selected_exit_track == 'FINAL-A'
                and binding.acquisition_application_key == applied_buy.receipt_key)
            partial_units = quantity//3
            staged = f.step(NOW+16, protective_max_units=partial_units)
            partial = f.repo.action(staged.action_id)
            original = state(f).obligation
            check(name+'_actual_runtime_ceiling_stages_exact_partial_due_action', staged.work == 'PROTECTIVE_ACTION_STAGED'
                and 0 < partial.input_units == partial_units < quantity
                and original.state == 'DUE' and original.remaining_units == quantity
                and original.obligation_id == partial.obligation_id and partial.ordinal == 1)
            different_ceiling = f.step(NOW+17, protective_max_units=quantity)
            check(name+'_later_ceiling_cannot_resize_existing_staged_action', different_ceiling.work == 'NEED_EXECUTION'
                and different_ceiling.action_id == partial.action_id and f.repo.action(partial.action_id) == partial
                and state(f).action == partial and f.repo.audit()['action_count'] == 2)
            _, partial_receipt, partial_evidence = reduction(f,key,partial,NOW+20,72)
            after_partial = state(f)
            check(name+'_only_actual_partial_released_residual_remains_due', after_partial.state == 'STAGE_ACTION'
                and after_partial.mutation_eligible and after_partial.obligation.remaining_units == quantity-partial_units
                and after_partial.obligation.sold_units == partial_units
                and after_partial.obligation.handoff == original.handoff
                and after_partial.obligation.obligation_id == original.obligation_id
                and f.repo.reservation(f.buy.root_id).status == 'RESERVED'
                and len(f.repo.consumer_snapshot()['positions']) == 1)
            before_reopen, old_owner = prior.stamp(f,'actual partial applied'), f.runtime._ownership.fence
            cold = owned.reopen(f)
            restored = state(f)
            check(name+'_cold_owned_reopen_reconstructs_original_residual_and_obligation',
                cold.owner_fence.generation > old_owner.generation
                and f.runtime.position_binding == binding and restored.obligation == after_partial.obligation
                and restored.state == 'STAGE_ACTION')
            if exhaust:
                residual_staged = f.step(NOW+30)
            else:
                # Retain the legacy two-argument call boundary; delegate all
                # staging to the original implementation without changing units.
                original_stage = runtime_module.stage_protective_sell
                legacy_calls = []
                def legacy_stage(repo, selected_binding):
                    legacy_calls.append((repo is f.repo, selected_binding is f.runtime.position_binding))
                    return original_stage(repo, selected_binding)
                with patch.object(runtime_module, 'stage_protective_sell', legacy_stage):
                    residual_staged = f.step(NOW+30)
                check(name+'_default_staging_preserves_legacy_two_argument_call', legacy_calls == [(True, True)])
                record['default_call_compatibility'] = {'legacy_two_argument_calls':len(legacy_calls),
                    'original_staging_delegated':True,'explicit_partial_ceiling_preceded_default_residual':True}
            residual = f.repo.action(residual_staged.action_id)
            check(name+'_default_runtime_stages_exact_distinct_full_residual',
                residual_staged.work == 'PROTECTIVE_ACTION_STAGED' and residual.input_units == quantity-partial_units
                and residual.ordinal == 2 and residual.action_id != partial.action_id
                and all(getattr(residual, field) == getattr(partial, field) for field in
                    ('root_id','mint','position_id','selected_exit_track','obligation_id','policy_ref','policy_digest','candidate_digest'))
                and residual.external_decision_ref == partial.external_decision_ref
                and f.repo.position_history(f.buy.position_id).remaining_units == residual.input_units)
            record.update(buy=buy_evidence,binding=asdict(binding),original_obligation=asdict(original),
                partial=partial_evidence,after_partial=asdict(after_partial),before_reopen=before_reopen,
                cold_audit=asdict(cold),residual_action=asdict(residual))
            if exhaust:
                check(name+'_original_failure_limits_two_and_20000',
                    f.policy.costs.failed_attempt_count == 2 and f.policy.costs.failed_attempt_network_budget_lamports == 20000)
                failures = []
                for n, at in enumerate((NOW+36,NOW+50),1):
                    _, failed, proof = reduction(f,key,residual,at,72+n,failed=True)
                    current = state(f)
                    check(name+'_'+str(n)+'_actual_failed_residual_fee_keeps_original_obligation',
                        current.state == 'REPLACE_ATTEMPT' and current.action == residual and current.next_attempt_ordinal == n+1
                        and current.obligation.remaining_units == residual.input_units
                        and current.obligation.sold_units == partial_units and current.obligation.handoff == original.handoff
                        and f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports == (2+n)*sf.FEE
                        and f.repo.reservation(f.buy.root_id).status == 'RESERVED')
                    failures.append(proof)
                before = f.repo.consumer_snapshot()
                with prior.external_mint(residual.mint), prior.retained_accumulator_rpc(f,residual):
                    denied, reads, sends = c2.execution(f,key,residual,at=NOW+64,number=75)
                denial = f.repo.authority_message_receipt(denied.receipt_key)
                attempt = f.repo.attempt(denied.attempt_id)
                risk = denial.decision.risk
                denial_stages = receipts.stages(f,denied.attempt_id)
                check(name+'_fresh_original_authority_denies_exhausted_failure_costs_before_sign_send',
                    denied.work == 'HELD' and denied.reason == 'AUTHORITY_SIGN_NOT_FRESHLY_GRANTED'
                    and not denial.consumed and denial.original.request.stage == 'SIGN'
                    and set(denial.decision.reasons) == {'ACTUAL_FAILURE_ATTEMPT_BUDGET_EXHAUSTED','CURRENT_EXACT_FEE_EXCEEDS_REMAINING_FAILURE_ALLOWANCE'}
                    and risk.failed_attempt_count == 2 and risk.remaining_failed_attempt_count == 0
                    and risk.failure_allowance_lamports == 20000-2*sf.FEE
                    and risk.historical_network_paid_lamports == 4*sf.FEE
                    and attempt.primary_signature is None and not sends.requests
                    and len(denial_stages) == 1 and not denial_stages[0].consumed)
                after = f.repo.consumer_snapshot()
                check(name+'_denial_never_releases_reduces_or_charges_residual',
                    all(after[field] == before[field] for field in ('funding','positions','reservations','accounts'))
                    and state(f).obligation.remaining_units == residual.input_units
                    and state(f).obligation.handoff == original.handoff
                    and f.repo.reservation(f.buy.root_id).status == 'RESERVED')
                encoded_denial = receipt_to_json(denial)
                check(name+'_denial_original_canonical_codec_roundtrip',
                    receipt_from_json(encoded_denial) == denial
                    and c2.canonical_json(json.loads(encoded_denial)) == encoded_denial)
                record.update(failures=failures,denial_result=asdict(denied),denial=json.loads(encoded_denial),
                    retained_residual=asdict(state(f)))
            else:
                sent, final_receipt, residual_evidence = reduction(f,key,residual,NOW+36,73)
                satisfied = state(f)
                waiting = f.step(NOW+45)
                check(name+'_zero_actual_residual_still_requires_retirement', satisfied.state == 'SATISFIED'
                    and satisfied.obligation.remaining_units == 0 and satisfied.obligation.sold_units == quantity
                    and satisfied.obligation.handoff == original.handoff and waiting.work == 'NEED_RETIREMENT'
                    and f.repo.reservation(f.buy.root_id).status == 'RESERVED')
                support = prior.continuation.wallet(f,NOW+46)
                retired = f.step(NOW+46,retirement_wallet=support)
                retirement = f.repo.port_receipt(retired.receipt_key)
                current, history = f.repo.consumer_snapshot(), f.repo.position_history(f.buy.position_id)
                check(name+'_actual_full_retirement_after_exact_two_reductions', retired.work == 'RETIREMENT_RETIRED'
                    and history.acquired_units == history.sold_units == quantity and history.remaining_units == 0
                    and history.status == 'RETIRED' and not current['positions'] and not current['reservations']
                    and not current['pending_attempts'] and f.runtime.position_binding is None
                    and retirement.retirement.terminal_attempt_id == sent.attempt_id
                    and f.repo.audit()['action_count'] == f.repo.audit()['attempt_count'] == 3)
                audit = f.repo.audit()
                replay = f.repo.retire(retirement.retirement,support,ingestion_key=retired.receipt_key,fence=f.repo.write_fence())
                check(name+'_retirement_replay_exactly_once', replay == retirement and f.repo.audit() == audit)
                record.update(residual=residual_evidence,retirement=asdict(retirement.retirement),final_position=asdict(history))
            unchanged_policy(f)
            record.update(final=prior.stamp(f,'final E02 journal'),audit=f.repo.audit())
            c2.canonical_json(record)  # Validate retained evidence before temporary stores close.
            check(name+'_sqlite_integrity',f.repo._conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                and f.conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok')
        finally:
            ops.close(f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--default-call-compat', action='store_true',
        help='Run only the positive E02 journey qualifying the legacy default staging call.')
    args = parser.parse_args()
    started = time.perf_counter()
    try:
        with tempfile.TemporaryDirectory(prefix='step10-e02-partial-') as directory:
            run(Path(directory))
            if not args.default_call_compat:
                run(Path(directory),exhaust=True)
    finally:
        paths = {Path(__file__), c2.ROOT/'src/live/runtime_composition_v0_1.py',
            c2.ROOT/'src/live/execution_reconciliation_v0_1.py',
            c2.ROOT/'docs/live/MEME_LIVE_PRODUCTION_CLOSURE_ARCHITECTURE_V2.md'}
        paths.update(Path(m.__file__).resolve() for m in tuple(sys.modules.values())
            if getattr(m,'__file__',None) and Path(m.__file__).suffix == '.py'
            and Path(m.__file__).resolve().is_relative_to(c2.ROOT))
        print(c2.canonical_json({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','scope':'STEP10_E02_DEFAULT_CALL_COMPAT_POSITIVE_ONLY' if args.default_call_compat else 'STEP10_E02_PARTIAL_RESIDUAL_AND_EXHAUSTED_EXIT_COSTS',
            'qualification':'SYNTHETIC_ENGINEERING_ONLY','checks':CHECKS,'helper_checks':prior.CHECKS,'reopen_helper_checks':owned.CHECKS,
            'evidence':EVIDENCE,'duration_seconds':time.perf_counter()-started,
            'limitations':['Synthetic external host/policy/RPC and ephemeral signer; no production deployment or capital qualification',
                'Owned cold reopen is in-process SQLite reconstruction, not full process-loss testing',
                ('Original exhausted-cost qualification is referenced from e02-partial-targeted-03; not rerun'
                 if args.default_call_compat else 'Exhaustion uses two actual failed residual transactions under unchanged original failure allowance; no recovery is added')],
            'artifact_hashes':{str(p.relative_to(c2.ROOT)).replace('\\','/'):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}}))


if __name__ == '__main__':
    main()