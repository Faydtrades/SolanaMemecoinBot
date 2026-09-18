"""Step10 S09 and missing E03 account/venue transitions; synthetic ports only.

Continuous producer, owned Runtime, Ledger/Authority, exact message, ephemeral
signer, finality, application and protection are production components. Only
public RPC/transport, synthetic policy/host inputs and clocks are fixtures.
No accepted suite main, network, persistent private keys or economic injection.
"""
from __future__ import annotations

import base64
import hashlib
import json
import tempfile
from contextlib import ExitStack
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch

import httpx
import live_step10_s01_s02_selftest_v0_1 as s1
from live.wallet_evidence_v0_1 import ledger_account_evidence

c2, ops, a3, sf, NOW = s1.c2, s1.ops, s1.a3, s1.sf, s1.NOW
CHECKS, EVIDENCE = {}, {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def prepare(directory, name, venue='PUMPSWAP', *, admit=True):
    f = ops.installed(c2.Fixture(directory, name))
    try:
        audit = ops.restart(f)
        # Local owned step: the shared c2.Fixture.step resets its clock per call,
        # which regresses behind retained evidence; keep its source cut and host.
        def step(at=NOW+4, **kwargs):
            kwargs.setdefault('operations_resources', lambda: ops.host(f, at))
            return f.runtime.step(clock=lambda: s1.persistent_clock(f, at), source_cut_utc=a3.utc(NOW+1), **kwargs)
        f.step = step
        check(name+'_owned_startup', not audit.grants_permission and f.runtime._operations_degradation is not None)
        f.candidate_ready()
        f.configure()
        check(name+'_original_producer_root', f.item.trade_root(f.domain) == f.root
            and f.item.producer_lineage_id == f.producer.source_identity
            and f.source.latest_record()[1].disposition == 'HEALTHY')
        if admit:
            result = f.step(entry=c2.EntryFacts(venue, sf.TOKEN_PROGRAM_ID, s1.wallet(f, NOW+4, mint=f.item.mint)))
            check(name+'_actual_entry_admission', result.work == 'ENTRY_ADMITTED')
            f.buy = f.repo.action(result.action_id)
        return f
    except BaseException:
        ops.close(f)
        raise


def execute(f, key, action, *, route, at, number, fault=None, wallet_support=None):
    """Adapt only the accepted external route fixture; all consumers are real."""
    public = dict(f.public)
    if route == 'swap':
        curve = c2.a4.venue.derive_bonding_curve_pda(action.mint)
        public[curve] = c2.a5.PublicAccount(c2.a5.RpcAccountV01(curve, sf.PUMP_PROGRAM_ID,
            sf.plans.curve_bytes(complete=True)), 1000000000 if public.get(curve) is None else public[curve].lamports, False, 0)
    intent = c2.q1._intent(c2.q1.capture_message_context(f.repo, action.action_id), at*1000000+1)
    seed, _ = c2.a4.evidence(f, action, c2.a5.wallet_scenario(public, f.lower), route=route,
        at=at, context_slot=f.lower.slot, intent=intent, wallet_floor=f.lower.slot,
        recent_blockhash=sf.bh(number), last_valid_height=f.lower.block_height+30,
        validity_height=f.lower.block_height+1, original_accounts=public)
    if f.repo.authority_message_profile(action.policy_digest) is None:
        c2.q1.install(f, seed)

    class Boundary(c2.q1.PublicTransport):
        def __init__(self, seed):
            super().__init__(seed)
            self.public_responses = []

        def __call__(self, request):
            response = super().__call__(request)
            payload, value = json.loads(request.content), response.json()
            method = payload['method']
            if method == 'getLatestBlockhash':
                value['result']['value']['lastValidBlockHeight'] = f.lower.block_height+30
            elif method == 'getBlockHeight':
                value['result'] = f.lower.block_height+1
            elif fault == 'delayed-pool' and method == 'getMultipleAccounts':
                pool = c2.a4.venue.derive_pumpswap_pool_pda(action.mint)
                for index, account in enumerate(payload['params'][0]):
                    if account == pool:
                        value['result']['value'][index] = None
            elif fault == 'fee-over-cap' and method == 'getFeeForMessage':
                value['result']['value'] = (f.policy.costs.network_total_fee_lamports if action.side == 'BUY'
                    else f.policy.costs.protective_network_fee_lamports)+1
            self.public_responses.append({'method': method, 'result': value['result']})
            return httpx.Response(200, json=value)

    reads, sends, signed = Boundary(seed), c2.q3.Boundary(f, 'ACKNOWLEDGED'), []
    original_sign = c2.AutonomousLocalSigner.sign_exact

    def sign(signer, *args, **kwargs):
        signed.append(action.action_id)
        return original_sign(signer, *args, **kwargs)

    with c2.ExecutionReadOnlyRpc('https://invalid.local', a3.PROFILE, now_us=reads.now,
            transport=httpx.MockTransport(reads)) as rpc:
        transport = c2.q4.send.SolanaSendTransport('https://invalid.local', a3.PROFILE,
            transport=httpx.MockTransport(sends))
        try:
            ports = c2.ExecutionPorts(rpc, wallet_support or seed.evidence.wallet,
                seed.evidence.quote_policy, seed.evidence.plan_policy, c2.ComputeBudget(250000, 1000),
                c2.AutonomousLocalSigner(key), transport, lambda: seed.evidence.simulation.leases[0].observed_at_us)
            try:
                with patch.object(c2.AutonomousLocalSigner, 'sign_exact', sign):
                    result = f.step(at, execution=ports)
                denial = None
            except (ValueError, RuntimeError) as error:
                result, denial = None, str(error)
        finally:
            transport.close()
    return result, denial, reads, sends, signed


def transact(f, key, action, *, route, at, number):
    before = f.repo.consumer_snapshot()
    result, error, reads, sends, signed = execute(f, key, action, route=route, at=at, number=number)
    label = f.name+'_'+action.side
    check(label+'_guarded_exact_send', error is None and result.work == 'SUBMISSION_OBSERVED'
        and len(sends.requests) == len(signed) == 1)
    envelope = c2.q4.send.load_signed_envelope(f.repo, result.attempt_id)
    validation = envelope.authority_receipt.original.validation
    plan = c2.q4._rebuild(validation)[-1]
    inputs = validation.evidence.setup_accounts
    pre = dict(zip(inputs.requested_keys, inputs.accounts))
    wire = base64.b64decode(envelope.signed_wire_base64)
    actual = sf.Fixture(route, action.side, token_program=action.token_program, external_plan=plan,
        external_message_hex=envelope.preparation.message_hex, external_wire=wire, external_pre_accounts=pre)
    actual.mint = action.mint
    scenario = c2.q4.chain_scenario(actual, envelope.preparation, at, envelope.primary_signature)
    simulation = [r for r in reads.requests if r['method'] == 'simulateTransaction']
    check(label+'_same_simulated_signed_sent_message', len(simulation) == 1
        and base64.b64decode(simulation[0]['params'][0])[65:] == wire[65:]
        and sends.requests[0]['params'][0] == envelope.signed_wire_base64
        and all(c2.VersionedTransaction.from_bytes(wire).verify_with_results()))
    check(label+'_send_never_applies_economics', f.repo.consumer_snapshot()['funding'] == before['funding']
        and f.repo.consumer_snapshot()['positions'] == before['positions'])
    reconciled, calls = c2.reconcile(f, scenario, at+6)
    check(label+'_public_finality_unapplied', reconciled.reason == 'FINALIZED_SUCCESS_UNAPPLIED' and calls
        and f.repo.consumer_snapshot()['funding'] == before['funding'])
    chain = f.repo.chain_receipt(reconciled.receipt_key)
    support = c2.application_support(f, actual, chain, pre, at+8)
    applied = f.step(at+8, application_wallet=support)
    receipt = f.repo.application_receipt(applied.receipt_key)
    proposal = receipt.decision.proposal
    check(label+'_actual_application', applied.reason == 'FINALIZED_SUCCESS_APPLIED' and proposal is not None)
    components = proposal.components
    check(label+'_native_and_WSOL_exact_conservation',
        sum(c.units for c in components if c.asset == 'SOL' and c.account == f.domain.wallet) == proposal.native_wallet_delta
        and sum(c.units for c in components if c.asset == 'WSOL') == 0
        and f.repo.consumer_snapshot()['funding'].native_lamports == before['funding'].native_lamports+proposal.native_wallet_delta
        and f.public[f.domain.wallet].lamports == f.repo.consumer_snapshot()['funding'].native_lamports)
    check(label+'_actual_fee_once_and_actual_rent_refund', proposal.transaction_fee_lamports == sf.FEE
        and proposal.venue_fee_units == actual.venue_fees
        and proposal.account_refund_lamports == sf.R
        and sum(c.units for c in components if c.kind == 'ACCOUNT_REFUND' and c.asset == 'SOL') == sf.R
        and pre[actual.quote] is None and f.public[actual.quote] is None
        and actual.scenario.tx['meta']['postBalances'][actual.index[actual.quote]] == 0)
    check(label+'_WSOL_is_not_available_native', all(account.units == 0 for account in f.repo.consumer_snapshot()['accounts']
        if account.mint == sf.WSOL_MINT))
    if action.side == 'BUY':
        check(label+'_actual_wrap_native', any(c.kind == 'WRAP_NATIVE' and c.asset == 'SOL' and c.units == -action.input_units for c in components))
    return applied, {'action_id': action.action_id, 'root_id': action.root_id, 'side': action.side,
        'signature': envelope.primary_signature, 'message_hex': envelope.preparation.message_hex,
        'message_sha256': envelope.preparation.message_sha256, 'signed_wire_sha256': hashlib.sha256(wire).hexdigest(),
        'public_chain_digest': chain.observation.content_digest, 'proposal': asdict(proposal),
        'native_before': before['funding'].native_lamports, 'native_after': f.public[f.domain.wallet].lamports,
        'WSOL_account': actual.quote, 'WSOL_pre': None, 'WSOL_post': None,
        'actual_base_units': actual.actual_base, 'application_key': applied.receipt_key}


def swap_lifecycle(directory, key):
    f = prepare(directory, 's09-swap')
    try:
        bought, buy = transact(f, key, f.buy, route='swap', at=NOW+6, number=101)
        binding = f.runtime.position_binding
        check(f.name+'_actual_selected_controller', binding.acquisition_application_key == bought.receipt_key
            and binding.spec.track_id == f.buy.selected_exit_track)
        staged = f.step(NOW+16)
        check(f.name+'_actual_due_protection', staged.work == 'PROTECTIVE_ACTION_STAGED')
        sell = f.repo.action(staged.action_id)
        check(f.name+'_actual_position_quantity', sell.input_units == buy['actual_base_units'])
        sold, sale = transact(f, key, sell, route='swap', at=NOW+20, number=102)
        retired = f.step(NOW+30, retirement_wallet=s1.continuation.wallet(f, NOW+30))
        check(f.name+'_actual_retirement', retired.work == 'RETIREMENT_RETIRED'
            and not f.repo.consumer_snapshot()['positions'] and not f.repo.consumer_snapshot()['reservations'])
        funding = f.repo.consumer_snapshot()['funding']
        check(f.name+'_recycled_native_exact_fees', funding.native_lamports == sf.FUNDING+buy['proposal']['native_wallet_delta']+sale['proposal']['native_wallet_delta']
            and funding.network_fees_paid_lamports == 2*sf.FEE
            and funding.venue_fees_paid_wsol_units == 2*1000000 and funding.venue_fees_paid_sol_lamports == 0)
        EVIDENCE[f.name] = {'buy': buy, 'sell': sale, 'funding': asdict(funding), 'journal': s1.stamp(f, 'retired')}
    finally:
        ops.close(f)


def admission_negative(directory, kind):
    name = 's09-entry-'+kind
    with ExitStack() as stack:
        if kind == 'insufficient-fees':
            original_domain = a3.domain
            stack.enter_context(patch.object(sf, 'FUNDING', 100000000))
            stack.enter_context(patch.object(a3, 'domain', lambda **kwargs:
                original_domain(**kwargs, known_native_wallet_lamports=100000000)))
        f = prepare(directory, name, admit=False)
        try:
            before = f.repo.consumer_snapshot()
            if kind in ('deposit', 'withdrawal'):
                f.public[f.domain.wallet] = replace(f.public[f.domain.wallet],
                    lamports=f.public[f.domain.wallet].lamports+(1234567 if kind == 'deposit' else -1234567))
            elif kind in ('external-token', 'external-WSOL', 'empty-WSOL'):
                mint = f.item.mint if kind == 'external-token' else sf.WSOL_MINT
                units = 0 if kind == 'empty-WSOL' else 1234567
                account = a3.derive_associated_token_address(f.domain.wallet, mint, sf.TOKEN_PROGRAM_ID)
                f.public[account] = c2.a5.PublicAccount(c2.a5.RpcAccountV01(account, sf.TOKEN_PROGRAM_ID,
                    a3.token_data(mint, f.domain.wallet, units, reserve=sf.R if mint == sf.WSOL_MINT else None)),
                    sf.R+(units if mint == sf.WSOL_MINT else 0), False, 0)
            support = s1.wallet(f, NOW+4, mint=f.item.mint)
            result = f.step(entry=c2.EntryFacts('PUMPSWAP', sf.TOKEN_PROGRAM_ID, support))
            receipt = f.repo.authority_admission_receipt(result.receipt_key)
            comparison = f.repo.comparison_receipt(result.receipt_key).comparison
            current = f.repo.consumer_snapshot()
            reasons = receipt.risk.reasons
            check(name+'_actual_admission_denied', result.work == 'ENTRY_DENIED' and not receipt.accepted
                and not current['positions'] and not current['reservations'] and f.repo.audit()['attempt_count'] == 0)
            check(name+'_external_facts_never_become_PnL_or_funding', current['funding'].native_lamports == before['funding'].native_lamports
                and current['funding'].network_fees_paid_lamports == current['funding'].venue_fees_paid_sol_lamports
                    == current['funding'].venue_fees_paid_wsol_units == 0
                and current['accounts'] == before['accounts'])
            if kind == 'insufficient-fees':
                check(name+'_principal_available_fees_not_available', 'INSUFFICIENT_UNENCUMBERED_NATIVE_SOL' in reasons
                    and receipt.risk.native_lamports >= f.market.units
                    and receipt.risk.required_native_lamports > receipt.risk.available_native_lamports)
            else:
                check(name+'_current_custody_difference_denies', 'CURRENT_COMPLETE_CUSTODY_COMPARISON_REQUIRED' in reasons
                    and comparison.disposition == 'CUSTODY_DIFFERENCES_QUARANTINED')
                expected_difference = ('POSITIVE_NATIVE_DIFFERENCE' if kind == 'deposit' else
                    'NEGATIVE_NATIVE_DIFFERENCE' if kind == 'withdrawal' else 'UNATTRIBUTED_TOKEN_ACCOUNT')
                check(name+'_exact_public_difference', len(comparison.differences) == 1
                    and comparison.differences[0].kind == expected_difference)
            if 'WSOL' in kind:
                check(name+'_retained_WSOL_contract_denied', 'ENTRY_REQUIRES_ABSENT_WSOL_ACCOUNT' in reasons
                    and receipt.risk.native_lamports == before['funding'].native_lamports
                    and receipt.risk.known_wsol_units == 0)
            EVIDENCE[name] = {'risk': asdict(receipt.risk), 'funding': asdict(current['funding']),
                'public_observation_digest': support.observation.content_digest,
                'comparison': asdict(comparison), 'journal': s1.stamp(f, 'denied')}
        finally:
            ops.close(f)


def opening_negative(directory, *, empty):
    name = 's09-opening-'+('empty-WSOL' if empty else 'funded-WSOL')
    value = sf.Fixture().wallet_scenario()
    units = 0 if empty else 1234567
    quote = a3.derive_associated_token_address(sf.WALLET, sf.WSOL_MINT, sf.TOKEN_PROGRAM_ID)
    value.add_token(token=quote, mint=sf.WSOL_MINT, program=sf.TOKEN_PROGRAM_ID,
        data=a3.token_data(sf.WSOL_MINT, sf.WALLET, units, reserve=sf.R),
        mint_bytes=a3.mint_data(9), lamports=sf.R+units)
    # Unsupported starting holdings cannot yield a baseline or start a runtime.
    # Enter at the real public opening port, before producer/runtime ownership.
    repo = a3.LedgerRepository.initialize(directory/(name+'.sqlite'), a3.domain(wallet=sf.WALLET))
    try:
        receipt = a3.ingest(repo, a3.observe(value, request=a3.WalletEvidenceRequest(sf.WALLET, a3.GENESIS, 100)))
        check(name+'_actual_public_opening_quarantined', receipt.decision.disposition == 'QUARANTINED'
            and 'PREEXISTING_WSOL_ACCOUNT_UNSUPPORTED' in receipt.decision.reasons
            and receipt.decision.owned_native_lamports is None and receipt.decision.locked_token_account_lamports is None)
        check(name+'_no_baseline_action_or_fill', repo.baseline() is None
            and repo.audit()['action_count'] == repo.audit()['attempt_count'] == 0)
        EVIDENCE[name] = {'opening_decision': asdict(receipt.decision),
            'observation_digest': receipt.observation.content_digest, 'audit': repo.audit()}
    finally:
        repo.close()


def migration_and_account_negatives(directory, key):
    f = prepare(directory, 'e03-migration', venue='PUMP')
    try:
        # One actual Pump acquisition is needed to test same-position migration.
        acquired, buy = s1.transact(f, key, f.buy, NOW+6, 111)
        buy['message_hex'] = c2.q4.send.load_signed_envelope(f.repo, acquired.attempt_id).preparation.message_hex
        binding = f.runtime.position_binding
        staged = f.step(NOW+16)
        check(f.name+'_actual_original_due_protection', staged.work == 'PROTECTIVE_ACTION_STAGED')
        sell = f.repo.action(staged.action_id)
        position = f.repo.position_history(sell.position_id)
        funding = f.repo.consumer_snapshot()['funding']
        count = f.repo.audit()['attempt_count']
        denials = []
        for index, kind in enumerate(('changed-token-owner', 'changed-token-program', 'delayed-pool', 'fee-over-cap')):
            at = NOW+20+index
            support = None
            if kind.startswith('changed-token-'):
                original_public = f.public
                f.public = dict(f.public)
                account = a3.derive_associated_token_address(f.domain.wallet, sell.mint, sell.token_program)
                current = f.public[account]
                if kind == 'changed-token-owner':
                    data = bytearray(current.account.data)
                    data[32:64] = bytes(c2.a5.Pubkey.from_string(sf.plans.fx.CREATOR))
                    f.public[account] = replace(current, account=replace(current.account, data=bytes(data)))
                else:
                    f.public[account] = replace(current, account=replace(current.account, owner=sf.TOKEN_2022_PROGRAM_ID))
                try:
                    support = s1.wallet(f, at-4, mint=sell.mint)
                finally:
                    f.public = original_public
            result, error, reads, sends, signed = execute(f, key, sell, route='swap', at=at,
                number=121+index, fault=kind, wallet_support=support)
            check(f.name+'_'+kind+'_cannot_sign_send_or_fill', error is not None and result is None
                and not sends.requests and not signed and f.repo.audit()['attempt_count'] == count
                and f.repo.position_history(sell.position_id) == position
                and f.repo.consumer_snapshot()['funding'] == funding)
            expected = ('EXECUTION_WALLET_ORIGINAL_FACTS_UNAVAILABLE' if kind.startswith('changed-token-') else
                'EXECUTION_ROUTE_UNAVAILABLE' if kind == 'delayed-pool' else 'EXECUTION_MESSAGE_CONTEXT_UNSUPPORTED')
            check(f.name+'_'+kind+'_exact_deny', error == expected)
            assessment = None if support is None else ledger_account_evidence(support.observation,
                expected_wallet=f.domain.wallet, expected_genesis=f.domain.genesis_hash,
                expected_profile_fingerprint=f.domain.expected_profile_fingerprint,
                required_min_context_slot=support.required_min_context_slot, now_utc=a3.utc(at)).assessment
            if assessment is not None:
                exact = 'TOKEN_AUTHORITY_MISMATCH' if kind == 'changed-token-owner' else 'EXPECTED_ACCOUNT_MINT_OR_PROGRAM_MISMATCH'
                check(f.name+'_'+kind+'_exact_public_shape_reason', exact in assessment.reasons)
            if kind == 'delayed-pool':
                primary = next(item['result'] for item in reads.public_responses if item['method'] == 'getMultipleAccounts')
                check(f.name+'_completed_curve_pool_actually_absent', primary['value'][1] is None
                    and primary['value'][0]['data'][0] == base64.b64encode(sf.plans.curve_bytes(complete=True)).decode())
            denials.append({'kind': kind, 'reason': error, 'public_read_methods': [r['method'] for r in reads.requests],
                'wallet_observation_digest': None if support is None else support.observation.content_digest,
                'public_account_shape_reasons': None if assessment is None else assessment.reasons,
                'public_response_digest': c2.content_fingerprint(reads.public_responses),
                'fee_cap': f.policy.costs.protective_network_fee_lamports if kind == 'fee-over-cap' else None,
                'observed_fee': next((r['result']['value'] for r in reads.public_responses if r['method'] == 'getFeeForMessage'), None)})
        # Later healthy public pool facts select PumpSwap for the same action.
        applied, sold = transact(f, key, sell, route='swap', at=NOW+25, number=131)
        check(f.name+'_same_acquired_position_migrated_reduction', sell.root_id == f.buy.root_id
            and sold['actual_base_units'] == position.remaining_units and f.runtime.position_binding == binding
            and c2.protective_outcome(f.repo, binding).state == 'SATISFIED')
        retired = f.step(NOW+35, retirement_wallet=s1.continuation.wallet(f, NOW+35))
        check(f.name+'_healthy_delayed_venue_allows_retirement', retired.work == 'RETIREMENT_RETIRED'
            and f.repo.audit()['attempt_count'] == count+1)
        EVIDENCE[f.name] = {'pump_acquisition': buy, 'denials': denials, 'migrated_sell': sold,
            'binding_id': binding.binding_id, 'journal': s1.stamp(f, 'migrated and retired')}
    finally:
        ops.close(f)


def main():
    key = c2.Keypair()  # Disposable synthetic key, never serialized.
    with tempfile.TemporaryDirectory(prefix='step10-s09-') as tmp, patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        swap_lifecycle(Path(tmp), key)
        for kind in ('deposit', 'withdrawal', 'external-token', 'external-WSOL', 'empty-WSOL', 'insufficient-fees'):
            admission_negative(Path(tmp), kind)
        opening_negative(Path(tmp), empty=True)
        opening_negative(Path(tmp), empty=False)
        migration_and_account_negatives(Path(tmp), key)
    paths = [Path(__file__), Path(s1.__file__), Path(c2.__file__), Path(ops.__file__), Path(c2.a4.__file__), Path(sf.__file__)]
    paths.extend(c2.ROOT/'src/live'/name for name in ('runtime_composition_v0_1.py', 'execution_reconciliation_v0_1.py',
        'ledger_repository_v0_1.py', 'ledger_custody_v0_1.py', 'ledger_settlement_v0_1.py', 'authority_admission_v0_1.py',
        'authority_message_evidence_v0_1.py', 'execution_message_v0_1.py', 'wallet_evidence_v0_1.py'))
    hashes = {str(p.relative_to(c2.ROOT)).replace('\\', '/'): s1.lf_sha256(p) for p in paths}
    print(c2.canonical_json({'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'scope': ['S09', 'E03_ACCOUNT_AND_VENUE_TRANSITIONS_ONLY'],
        'qualification': 'SYNTHETIC_ENGINEERING_ONLY', 'checks': {**CHECKS, **s1.CHECKS},
        'artifact_hashes': hashes, 'evidence': EVIDENCE}))


if __name__ == '__main__':
    main()
