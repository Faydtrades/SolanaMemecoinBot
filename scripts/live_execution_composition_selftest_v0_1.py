"""Q4 actual Execution -> public Evidence -> Ledger composition.

Only public chain/HTTP/time and ephemeral in-memory private keys are external
fixtures. A5 supplies explicit unbuilt Runtime handoff, reduction decision and
retirement inputs; its execution helper is replaced by production Q1/Q2/Q3/Q4.
No fabricated approval, execution output, finality or economic projection.
"""
from __future__ import annotations

import base64
import copy
import json
import sys
import tempfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import httpx
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
import live_authority_composition_selftest_v0_1 as a5
import live_execution_send_selftest_v0_1 as q3
from live.execution_reconciliation_v0_1 import observe_attempt_finality, ExecutionReconciliationError
from live.authority_message_evidence_v0_1 import _rebuild
from live.public_rpc_v0_1 import PublicReadOnlyRpc
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint

q2, q1, a3, control, NOW = q3.q2, q3.q1, q3.a3, q3.control, q3.NOW
a4, sf, send = q2.a4, q2.sf, q3.send
CHECKS, RECORDS = {}, []


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def read_finality(f, envelope, scenario, *, label, at, fault=None, fence=None):
    calls = []
    def external(request):
        original = json.loads(request.content)
        calls.append(original)
        if fault == 'timeout':
            raise httpx.ReadTimeout('PRIVATE_EXTERNAL_TRANSPORT')
        result = scenario.handle(request)
        if fault == 'concurrent' and len(calls) == 1:
            a3.control(f.repo, 'STOP_ENTRY', label+'-stop', at=at)
        return result
    with PublicReadOnlyRpc('https://private.invalid/PRIVATE_ENDPOINT', a3.PROFILE,
            transport=httpx.MockTransport(external)) as rpc:
        receipt = observe_attempt_finality(f.repo, envelope.preparation.attempt_id, rpc,
            ingestion_key=label, evaluated_at_utc=a3.utc(at), clock=lambda: a3.utc(at),
            fence=f.repo.write_fence() if fence is None else fence)
    check(label+'_only_original_signature_requested', all(
        r['params'][0] == envelope.primary_signature if r['method'] == 'getTransaction' else
        r['params'][0] == [envelope.primary_signature]
        for r in calls if r['method'] in ('getTransaction', 'getSignatureStatuses')))
    check(label+'_request_floor_derived_from_original_anchors', receipt.observation.request.min_finalized_root_slot == max(
        f.domain.minimum_context_slot, f.repo.baseline().observation.anchor.slot,
        envelope.preparation.finalized_lower_anchor.slot))
    check(label+'_original_observation_and_decision_durable', f.repo.chain_receipt(label) == receipt
        and receipt.signed_wire_digest == envelope.signed_wire_digest)
    return receipt


def chain_scenario(actual, prep, at, signature):
    # Reuse only the accepted fixture's public block/metadata construction.
    # Its preliminary observation is discarded; Q4 makes the qualified read.
    a5.original_chain(actual, prep, at, signature)
    scenario = actual.scenario
    scenario.status['err'] = copy.deepcopy(scenario.tx['meta']['err'])
    return scenario


def execute(f, key, action, accounts, lower, *, route, at, number, intent=None,
            outcome='ACKNOWLEDGED', failed=False, missing_first=False, stop_sign=False):
    label = f.name+'-'+str(number)
    originals = dict(accounts)
    if route == 'swap':
        address = a4.venue.derive_bonding_curve_pda(action.mint)
        originals[address] = a5.PublicAccount(a5.RpcAccountV01(address, sf.PUMP_PROGRAM_ID,
            sf.plans.curve_bytes(complete=True)),
            1000000000 if originals.get(address) is None else originals[address].lamports, False, 0)
    scenario = a5.wallet_scenario(originals, lower)
    # Q1 owns the immutable LIVE intent adapter; fixture public recipient reads
    # must use that same intent identity (including the fresh SELL decision cut).
    intent = q1._intent(q1.capture_message_context(f.repo, action.action_id), at*1000000)
    seed, _ = a4.evidence(f, action, scenario, route=route, at=at, context_slot=lower.slot,
        intent=intent, wallet_floor=lower.slot, recent_blockhash=sf.bh(number),
        last_valid_height=200, validity_height=100, original_accounts=originals)
    if f.repo.authority_message_profile(action.policy_digest) is None:
        q1.install(f, seed)
    production, public_read = q1.produce(f, action, seed)
    stored = q1.prepare_exact_message(f.repo, production, fence=f.repo.write_fence())
    prep = stored.preparation
    if stop_sign:
        a3.control(f.repo, 'HARD_STOP', label+'-hard-stop', at=at)
    sign = control.consume(f, production.original, control.request(f, prep, label+'-SIGN'), at=at)
    if stop_sign:
        check(label+'_source_gap_never_bypasses_SELL_hard_stop', type(sign) is not control.FreshStageConsumption
            and 'GLOBAL_HARD_STOP_LATCHED' in sign.decision.reasons and f.repo.attempt(prep.attempt_id).primary_signature is None
            and f.repo.protection(action.position_id).obligation_state == 'DUE'
            and f.repo.position_history(action.position_id).remaining_units >= action.input_units)
        return
    check(label+'_actual_Q1_current_SIGN', type(sign) is control.FreshStageConsumption)
    envelope = q2.invoke(f, key, production, sign, at)
    check(label+'_actual_Q2_Q3_durable_envelope', send.persist_signed_envelope(f.repo, envelope,
        fence=f.repo.write_fence()) == envelope)
    fresh = control.consume(f, production.original, control.request(f, prep, label+'-SEND', 'SEND'), at=at+2)
    check(label+'_actual_current_SEND', type(fresh) is control.FreshStageConsumption)
    boundary = q3.Boundary(f, outcome)
    transport = send.SolanaSendTransport('https://private.invalid/SYNTHETIC', a3.PROFILE,
        transport=httpx.MockTransport(boundary))
    try:
        observation = q3.invoke(f, fresh, transport, at+2)
    finally:
        transport.close()
    simulated = base64.b64decode(next(r['params'][0] for r in public_read.requests if r['method'] == 'simulateTransaction'))
    wire = base64.b64decode(envelope.signed_wire_base64)
    check(label+'_same_actual_message_simulation_signature_transport', simulated[:65] == b'\x01'+bytes(64)
        and simulated[65:] == wire[65:] == bytes.fromhex(production.message_hex)
        and all(VersionedTransaction.from_bytes(wire).verify_with_results())
        and boundary.requests[0]['params'][0] == envelope.signed_wire_base64)
    check(label+'_send_observation_is_not_settlement', observation.finality == 'UNKNOWN'
        and not observation.may_retry and f.repo.attempt(prep.attempt_id).lane_held
        and f.repo.chain_receipt(label+'-chain') is None)
    original = production.original
    plan = _rebuild(original)[-1]
    inputs = original.evidence.setup_accounts
    # Finalized metadata includes all balances, independently of Q1's economic
    # setup-data query. Retain the fixture's original public program accounts.
    pre = dict(public_read.setup)
    pre.update(zip(inputs.requested_keys, inputs.accounts))
    actual = sf.Fixture(route, action.side, failed=failed, token_program=action.token_program,
        external_plan=plan, external_message_hex=prep.message_hex, external_wire=wire, external_pre_accounts=pre)
    actual.mint = action.mint
    public_chain = chain_scenario(actual, prep, at, envelope.primary_signature)
    if missing_first:
        absent = copy.deepcopy(public_chain)
        absent.tx = None
        absent.status = None
        before = f.repo.consumer_snapshot()
        unknown = read_finality(f, envelope, absent, label=label+'-missing', at=at+6)
        check(label+'_missing_after_lost_response_keeps_actual_units_and_lane',
            unknown.decision.resulting_state.positive_finality is None
            and f.repo.attempt(prep.attempt_id).lane_held
            and f.repo.consumer_snapshot()['positions'] == before['positions']
            and not send.load_send_observations(f.repo, prep.attempt_id)[0].may_retry)
        f.reopen()
        check(label+'_restart_retains_unknown_original_signature', send.load_signed_envelope(f.repo, prep.attempt_id) == envelope)
    chain = read_finality(f, envelope, public_chain, label=label+'-chain', at=at+6)
    check(label+'_Ledger_finality_same_exact_wire', chain.observation.transaction.wire_bytes == wire
        and chain.observation.transaction.message_sha256 == prep.message_sha256
        and chain.decision.resulting_state.disposition == ('FINALIZED_FAILURE_UNAPPLIED' if failed else 'FINALIZED_SUCCESS_UNAPPLIED')
        and f.repo.attempt(prep.attempt_id).lane_held)
    after = a5.post_accounts({**originals, **pre}, actual)
    root = chain.observation.root
    anchor = replace(root, slot=root.slot+2, blockhash=sf.bh(root.slot+2), previous_blockhash=root.blockhash,
        parent_slot=root.slot, block_height=root.block_height+1, block_time=at+7)
    expected = {item.pubkey:(item.mint,item.program) for item in f.repo.consumer_snapshot()['accounts']}
    expected.update({actual.base:(action.mint,action.token_program), actual.quote:(sf.WSOL_MINT,sf.TOKEN_PROGRAM_ID)})
    request = a3.WalletEvidenceRequest(f.domain.wallet, f.domain.genesis_hash, chain.observation.transaction.slot,
        tuple(a3.ExpectedTokenAccount(k,m,p) for k,(m,p) in sorted(expected.items())))
    support = a3.WalletSupportInput(a3.observe(a5.wallet_scenario(after, anchor), request=request, at=at+8),
        a3.utc(at+8), request.min_context_slot)
    proposal = sf.attribute_settlement(f.domain, action, f.repo.attempt(prep.attempt_id), chain, support)
    check(label+'_actual_Ledger_settlement_attribution', proposal.proposal is not None)
    RECORDS.append({'side':action.side,'production':production.content_digest,'signed_envelope':envelope.content_digest,
        'send_observation':observation.content_digest,'chain_receipt':chain.content_digest,'input_units':action.input_units})
    return actual, prep, chain, support, after, anchor, proposal.proposal, sign.receipt, fresh.receipt


def cycle(directory, name, *, gap=False):
    key = Keypair()
    def production_path(f, action, accounts, lower, **kwargs):
        return execute(f, key, action, accounts, lower, **kwargs,
            outcome='TRANSPORT_UNRESOLVED', missing_first=True)
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())), \
            patch.object(a5, 'execute_public', production_path):
        result = a5.cycle(directory, name, gap=gap)
    CHECKS.update(a5.CHECKS)
    return result


def failure_cycles(directory):
    for case in ('failed-buy', 'failed-sell', 'gap-hard-stop'):
        key = Keypair()
        with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
            f, _, _, buy = a4.fixture(directory, case)
            try:
                public = {f.domain.wallet:a5.PublicAccount(a5.RpcAccountV01(f.domain.wallet,sf.SYSTEM_PROGRAM_ID,b''),sf.FUNDING,False,0),
                    sf.MINT:a5.PublicAccount(sf.plans.fx.mint_account(),1000000000,False,0),
                    sf.WSOL_MINT:a5.PublicAccount(a5.RpcAccountV01(sf.WSOL_MINT,sf.TOKEN_PROGRAM_ID,a3.mint_data(9)),1000000000,False,0)}
                values = execute(f,key,buy,public,a3.observe().anchor,route='pump',at=NOW+9,number=71,failed=case=='failed-buy')
                actual, prep, chain, support, public, lower = values[:6]
                if case == 'failed-buy':
                    applied = f.repo.apply_settlement(prep.attempt_id, chain_receipt_key=case+'-71-chain', support=support,
                        ingestion_key='failed-fee', recorded_at_utc=a3.utc(NOW+20), fence=f.repo.write_fence())
                    check(case+'_actual_failed_fee_without_position', applied.decision.disposition == 'FINALIZED_FAILURE_APPLIED'
                        and not f.repo.consumer_snapshot()['positions']
                        and f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports == 7777)
                else:
                    handoff = a3.ports.handoff(f.repo,buy,prep,chain,support,due=True)
                    f.repo.apply_settlement_with_ports(prep.attempt_id,chain_receipt_key=case+'-71-chain',support=support,
                        ingestion_key='acquire-protect',recorded_at_utc=a3.utc(NOW+20),handoff=handoff,fence=f.repo.write_fence())
                    quantity = f.repo.position_history(buy.position_id).remaining_units
                    if case == 'gap-hard-stop':
                        q3.source_gap(f)
                    sell,intent = a5.exit_action(f,buy,handoff,quantity,1,NOW+29)
                    before = f.repo.position_history(buy.position_id)
                    values = execute(f,key,sell,public,lower,route='swap',at=NOW+29,number=72,
                        intent=intent,failed=case=='failed-sell',stop_sign=case=='gap-hard-stop')
                    if case == 'failed-sell':
                        actual,prep,chain,support,public,lower = values[:6]
                        applied = f.repo.apply_settlement(prep.attempt_id,chain_receipt_key=case+'-72-chain',support=support,
                            ingestion_key='failed-sell-fee',recorded_at_utc=a3.utc(NOW+37),fence=f.repo.write_fence())
                        check(case+'_actual_failed_fee_preserves_units_and_due_protection', applied.decision.disposition == 'FINALIZED_FAILURE_APPLIED'
                            and f.repo.position_history(buy.position_id).remaining_units == before.remaining_units
                            and f.repo.protection(buy.position_id).obligation_state == 'DUE'
                            and f.repo.reservation(buy.root_id).status == 'RESERVED'
                            and f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports == 15554)
                f.reopen()
                check(case+'_original_history_replays', bool(f.repo.audit()))
            finally:
                f.close()


def handoff_guards(directory):
    original_produce = q1.produce
    chain_accounts = {}
    def capture_public_accounts(*args, **kwargs):
        result, public_read = original_produce(*args, **kwargs)
        chain_accounts.clear()
        chain_accounts.update(public_read.setup)
        return result, public_read
    for case in ('timeout','membership-contradiction','wrong-wire','wrong-genesis','concurrent','wrong-profile','stale-fence','clock-private'):
        with patch.object(q1,'produce',capture_public_accounts), q3.fixture(directory,case) as (f,production,envelope,fresh,boundary,transport,at):
            # Deliberately no send claim: public reconciliation is still lawful
            # for already signed durable output after an interrupted dispatch.
            prep = envelope.preparation
            inputs = production.original.evidence.setup_accounts
            actual = sf.Fixture(external_plan=_rebuild(production.original)[-1],external_message_hex=prep.message_hex,
                external_wire=base64.b64decode(envelope.signed_wire_base64),external_pre_accounts=dict(chain_accounts))
            scenario = chain_scenario(actual,prep,at,envelope.primary_signature)
            if case == 'membership-contradiction': scenario.blocks[scenario.tx['slot']]['signatures'] = []
            elif case == 'wrong-wire': scenario.tx['transaction'] = a5.chain_fixture.scenario().tx['transaction']
            elif case == 'wrong-genesis': scenario.genesis = sf.bh(231)
            if case in ('wrong-profile','stale-fence','clock-private'):
                calls = []
                fence = f.repo.write_fence()
                profile = replace(a3.PROFILE, max_requests=a3.PROFILE.max_requests-1) if case == 'wrong-profile' else a3.PROFILE
                if case == 'stale-fence': a3.control(f.repo,'STOP_ENTRY',case+'-stop',at=at)
                def sample():
                    if case == 'clock-private':
                        raise ExecutionReconciliationError('PRIVATE_CLOCK_FAILURE')
                    return a3.utc(at+6)
                with PublicReadOnlyRpc('https://private.invalid',profile,transport=httpx.MockTransport(lambda r:calls.append(r))) as rpc:
                    before = f.repo.write_fence()
                    try:
                        observe_attempt_finality(f.repo,prep.attempt_id,rpc,ingestion_key=case,
                            evaluated_at_utc=a3.utc(at+6),clock=sample,fence=fence)
                    except ExecutionReconciliationError as exc:
                        check(case+'_denied_before_any_read_or_write', not calls and f.repo.write_fence() == before
                            and 'PRIVATE' not in str(exc))
                    else:
                        raise AssertionError(case)
            elif case == 'concurrent':
                check(case+'_changed_fence_rejects_ingestion', q3.fails(lambda:read_finality(f,envelope,scenario,label=case,at=at+6,fault=case))
                    and f.repo.chain_receipt(case) is None)
            else:
                receipt = read_finality(f,envelope,scenario,label=case,at=at+6,fault=case)
                state = receipt.decision.resulting_state
                check(case+'_no_false_finality_or_lane_release', state.positive_finality is None and f.repo.attempt(prep.attempt_id).lane_held)
                if case in ('membership-contradiction','wrong-wire'):
                    check(case+'_Ledger_quarantines_original_conflict', state.quarantined)
                check(case+'_original_send_observation_not_required', send.load_send_observations(f.repo,prep.attempt_id) == ())
                f.reopen()
                check(case+'_durable_replay', f.repo.chain_receipt(case) == receipt)


def main():
    with tempfile.TemporaryDirectory(prefix='execution-q4-') as tmp:
        try:
            results = {name:cycle(Path(tmp), name, gap=gap) for name,gap in (('healthy',False), ('source-gap',True))}
            failure_cycles(Path(tmp))
            handoff_guards(Path(tmp))
        finally:
            for fixture in a3.a1.FIXTURES:
                fixture.close()
    check('temporary_databases_removed', not Path(tmp).exists())
    print(canonical_json({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'checks':len(CHECKS),
        'all_true':all(CHECKS.values()), 'check_digest':content_fingerprint(CHECKS), 'results':results,
        'producer_durable_consumer':RECORDS, 'actual_network_requests':0, 'actual_chain_broadcasts':0,
        'runtime_fixtures':['handoff','due obligation','SELL decision','retirement input']}))


if __name__ == '__main__':
    main()
