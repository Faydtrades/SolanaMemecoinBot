"""Step10 S01/S02 only: owned deterministic engineering qualification.

Actual production reducers/ports, isolated SQLite stores, ephemeral synthetic
signer, and scripted public RPC/transport facts. Policy approval and host/profile
observations are explicitly synthetic engineering inputs, not real deployment
qualification. No accepted selftest main, network, private configuration or keys
are read. Main scenario has two lifecycles; ENTRY_ONCE variant has one lifecycle
then a second acquisition denial after retirement.
"""
from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import httpx
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import live_operations_startup_selftest_v0_1 as ops
import live_runtime_continuation_selftest_v0_1 as continuation
from live.authority_admission_v0_1 import EntryRequest
from live.candidate_handoff_v0_1 import CandidateHandoffV01
from phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001
from phase4.paper_cost_model_v0_2 import PaperCostModelV02
from phase4.paper_entry_router_v0_1 import EntryMarketObservation, EntryRouteState
from phase4.paper_entry_router_v0_2 import PaperEntryRouterV02

c2, a3, sf, NOW = ops.rt, ops.a3, ops.sf, ops.NOW
hf = c2.hf
CHECKS, EVIDENCE = {}, {}
FIXTURE_VERSION = 'STEP10_S01_S02_SYNTHETIC_ENGINEERING_V01'


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def stamp(f, label):
    audit = f.repo.audit()
    return {'label': label, 'journal_digest': audit['last_receipt_digest'],
        'custody_digest': audit['custody_digest'], 'revision': audit['revision']}


def wallet(f, at, mint=None):
    if mint is None:
        return continuation.wallet(f, at)
    expected = {a.pubkey: (a.mint, a.program) for a in f.repo.consumer_snapshot()['accounts']}
    for token_mint in (mint, sf.WSOL_MINT):
        expected[a3.derive_associated_token_address(f.domain.wallet, token_mint, sf.TOKEN_PROGRAM_ID)] = (token_mint, sf.TOKEN_PROGRAM_ID)
    public = dict(f.public)
    with external_mint(mint):
        public.setdefault(mint, c2.a5.PublicAccount(sf.plans.fx.mint_account(), 1000000000, False, 0))
    f.public.setdefault(mint, public[mint])
    request = a3.WalletEvidenceRequest(f.domain.wallet, f.domain.genesis_hash, f.lower.slot,
        tuple(a3.ExpectedTokenAccount(k, m, p) for k, (m, p) in sorted(expected.items())))
    return a3.WalletSupportInput(a3.observe(c2.a5.wallet_scenario(public, f.lower), request=request, at=at), a3.utc(at), request.min_context_slot)


@contextmanager
def external_mint(mint):
    """Only legacy external-fixture account factories have module-level mint IDs."""
    with patch.object(sf, 'MINT', mint), patch.object(sf.plans.fx, 'MINT', mint):
        yield


@contextmanager
def retained_accumulator_rpc(f, action):
    """External RPC facts retain the program account created by the first BUY.

    The inherited seed assumes initial account absence for every BUY. Change
    only its public account/simulation responses for this sequential consumer;
    actual production message construction and finality metadata then consume
    those responses. No approval, simulation classification or posting is set.
    """
    volume = sf.derive_user_volume_accumulator(sf.PUMP_PROGRAM_ID, f.domain.wallet)
    existing = f.public.get(volume)
    original_transport = c2.q1.PublicTransport

    class PublicContinuity(original_transport):
        def __init__(self, seed, **kwargs):
            super().__init__(seed, **kwargs)
            if existing is not None:
                self.setup[volume] = existing

        def __call__(self, request):
            response = super().__call__(request)
            payload = json.loads(request.content)
            if existing is not None and action.side == 'BUY' and payload['method'] == 'simulateTransaction':
                result = response.json()
                keys = self.seed.evidence.setup_accounts.requested_keys
                removed = 0
                for group in result['result']['value']['innerInstructions']:
                    kept = []
                    for instruction in group['instructions']:
                        raw = a3.cf._b58data(instruction['data'], 65536)
                        accounts = tuple(keys[index] for index in instruction['accounts'])
                        create = (keys[instruction['programIdIndex']] == sf.SYSTEM_PROGRAM_ID
                            and len(raw) == 52 and raw[:4] == bytes(4)
                            and accounts == (f.domain.wallet, volume))
                        if create:
                            removed += 1
                        else:
                            kept.append(instruction)
                    group['instructions'] = kept
                check(f.name+'_retained_accumulator_simulation_has_no_recreation', removed == 1)
                return httpx.Response(200, json=result)
            return response

    with patch.object(c2.q1, 'PublicTransport', PublicContinuity):
        yield existing


def next_candidate(f, *, shift=32):
    f.append([(n+shift, hf.MINT_C, kind, price)
        for n, _, kind, price in hf.FIRST if n not in (5, 7)])
    f.source_cut = NOW+shift
    for _ in range(128):
        result = f.step(NOW+shift+4)
        if result.work == 'NEED_ENTRY_FACTS':
            f.root, f.item = result.root_id, f.repo.candidate(result.root_id)
            return result
    raise AssertionError('bounded distinct original candidate delivery')


def paper_rejection(f):
    # Deserialize the exact original producer event, not a made-up candidate.
    row = f.conn.execute(f"SELECT event_type,event_json FROM {hf.TABLES[5]} "
        "WHERE event_type='CandidateEvaluationEvent' ORDER BY event_sequence LIMIT 1").fetchone()
    from phase4.paper_continuous_firstpullback_binding_v0_1 import _event_from_payload
    event = _event_from_payload(row['event_type'], json.loads(row['event_json']))
    candidate = event.candidate
    check(f.name+'_paper_same_original_candidate', candidate.candidate_id == f.item.candidate_signal_id)
    with sqlite3.connect(':memory:') as conn:
        router = PaperEntryRouterV02(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
        routed = router.route_candidate(candidate)
        observation = EntryMarketObservation(candidate.mint, routed.route.execution_ready_at,
            candidate.signal_ingest_seq+1, candidate.price_identity,
            candidate.price_numerator_raw*2, candidate.price_denominator_raw,
            source_event_key='STEP10:SYNTHETIC:PAPER:ADVERSE:OBSERVATION')
        rejected = router.on_market_observation(candidate=candidate, observation=observation, price_impact_bps=0)
        check(f.name+'_actual_PAPER_rejection_without_position', rejected.route.state is EntryRouteState.REJECTED
            and conn.execute('SELECT COUNT(*) FROM paper_positions').fetchone()[0] == 0)
        return {'candidate_id': candidate.candidate_id, 'route_state': rejected.route.state.value,
            'reason': rejected.route.state_reason, 'model_fingerprint': router.cost_model.baseline.fingerprint,
            'observation_fixture': 'independent synthetic adverse PAPER market observation'}


def prepare(directory, name, *, once=False):
    f = ops.installed(c2.Fixture(directory, name))
    try:
        return prepare_owned(f, name, once=once)
    except BaseException:
        ops.close(f)
        raise


def persistent_clock(f, at):
    # Monotonic fixture sample that persists across step calls and reopen (the
    # same pattern as A07/A08): production retains evidence stamped by the last
    # sample, so a per-call reset to `at` would regress the clock behind it.
    sample = a3.clock(f.repo, at)
    upper = datetime.fromisoformat(sample.utc_upper_utc)
    previous = getattr(f, '_clock_upper', None)
    target = upper if previous is None else max(upper, previous+timedelta(microseconds=1))
    f._clock_upper = target
    delta = target-upper
    when = target.isoformat(timespec='microseconds')
    return replace(sample, utc_lower_utc=when, utc_upper_utc=when,
        monotonic_ns=sample.monotonic_ns+(delta.days*86400000000+delta.seconds*1000000+delta.microseconds)*1000)


def lf_sha256(path):
    # Git-LF content hash (CRLF checkout invariant), as acceptance_dossier._lf.
    return hashlib.sha256(path.read_bytes().replace(b'\r\n', b'\n')).hexdigest()


def prepare_owned(f, name, *, once):
    audit = ops.restart(f)
    owned_runtime = f.runtime
    # Keep original monotonic clock sampling, with current source cut.
    def step(at=NOW+4, **kwargs):
        clock = lambda: persistent_clock(f, at)
        if kwargs.get('retirement_wallet') is not None:
            clock, kwargs['retirement_wallet'] = continuation.held_retirement(clock, kwargs['retirement_wallet'])
        result = f.runtime.step(clock=clock, source_cut_utc=a3.utc(f.source_cut),
            operations_resources=lambda: ops.host(f, at), **kwargs)
        check(f.name+'_monitor_and_owner_remain_attached', f.runtime._operations_degradation is not None
            and f.runtime is owned_runtime and f.runtime._ownership.fence == audit.owner_fence)
        current = f.repo.consumer_snapshot()
        check(f.name+'_single_position_and_reservation_cap', len(current['positions']) <= 1
            and len(current['reservations']) <= 1)
        return result
    f.source_cut, f.step = NOW+1, step
    f.candidate_ready()
    cold = f.repo.evaluate_authority_entry(f.root, 'FINAL-A', a3.clock(f.repo, NOW+4), f.source,
        command_id=name+'-cold-unarmed', fence=f.repo.write_fence())
    check(name+'_cold_actual_authority_denial', not audit.grants_permission
        and 'ENTRY_UNARMED' in cold.decision.reasons and not f.repo.consumer_snapshot()['reservations'])
    baseline = stamp(f, 'cold original baseline')
    paper = paper_rejection(f)
    # Reuse original bounded synthetic accepted policy; arm only once.
    if once:
        original_arm = a3.arm
        with patch.object(a3, 'arm', lambda repo, policy: original_arm(repo, policy, scope='ENTRY_ONCE', root=f.root)):
            f.configure()
    else:
        f.configure()
    f.original_authority = f.repo.authority_snapshot()
    entry = f.step(entry=c2.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID, wallet(f, NOW+4, mint=f.item.mint)))
    if entry.work != 'ENTRY_ADMITTED':
        receipt = f.repo.authority_admission_receipt(entry.receipt_key)
        raise AssertionError((entry, receipt.eligibility.decision.reasons, receipt.risk.reasons))
    check(name+'_first_actual_admission', entry.work == 'ENTRY_ADMITTED')
    f.buy = f.repo.action(entry.action_id)
    EVIDENCE[name] = {'fixture': FIXTURE_VERSION, 'baseline': baseline, 'cold_reasons': cold.decision.reasons,
        'paper': paper, 'policy': asdict(f.policy), 'startup_identity': asdict(f.expected),
        'monitor_configuration': asdict(f.startup_monitor_config), 'lifecycles': [],
        'gate_at_first_admission': asdict(f.runtime._operations_degradation.snapshot())}
    return f


def transact(f, key, action, at, number):
    native_before = f.repo.consumer_snapshot()['funding'].native_lamports
    retained_other_accounts = {a.pubkey: f.public[a.pubkey]
        for a in f.repo.consumer_snapshot()['accounts'] if a.mint != action.mint and f.public.get(a.pubkey) is not None}
    units_before = 0 if action.side == 'BUY' else f.repo.position_history(action.position_id).remaining_units
    with external_mint(action.mint), retained_accumulator_rpc(f, action) as prior_accumulator:
        submitted, reads, sends = c2.execution(f, key, action, at=at, number=number)
        if submitted.work != 'SUBMISSION_OBSERVED' or len(sends.requests) != 1:
            raise AssertionError({'check': f.name+'_'+str(number)+'_actual_guarded_send',
                'runtime_step': asdict(submitted), 'send_request_count': len(sends.requests)})
        check(f.name+'_'+str(number)+'_actual_guarded_send', submitted.work == 'SUBMISSION_OBSERVED'
            and len(sends.requests) == 1)
        envelope, actual, scenario, pre = c2.original_chain(f, submitted, at)
    volume = sf.derive_user_volume_accumulator(sf.PUMP_PROGRAM_ID, f.domain.wallet)
    if prior_accumulator is not None:
        check(f.name+'_'+str(number)+'_retained_accumulator_original_plan_metadata',
            pre[volume] == prior_accumulator
            and dict(zip(envelope.authority_receipt.original.validation.evidence.setup_accounts.requested_keys,
                envelope.authority_receipt.original.validation.evidence.setup_accounts.accounts))[volume] == prior_accumulator
            and actual.scenario.tx['meta']['preBalances'][actual.index[volume]] == prior_accumulator.lamports
            and actual.scenario.tx['meta']['postBalances'][actual.index[volume]] == prior_accumulator.lamports)
    if prior_accumulator is not None and action.side == 'BUY':
        public_wallet = envelope.authority_receipt.original.validation.evidence.wallet.observation.explicit_read
        read_accounts = dict(zip(public_wallet.requested_keys, public_wallet.accounts))
        check(f.name+'_second_BUY_retains_prior_wallet_funded_ATA_evidence', bool(retained_other_accounts)
            and all(read_accounts.get(pubkey) == account for pubkey, account in retained_other_accounts.items()))
    simulations = [r for r in reads.requests if r['method'] == 'simulateTransaction']
    wire = base64.b64decode(envelope.signed_wire_base64)
    from solders.message import to_bytes_versioned
    check(f.name+'_'+str(number)+'_exact_simulated_signed_sent_bytes', len(simulations) == 1
        and to_bytes_versioned(c2.VersionedTransaction.from_bytes(base64.b64decode(simulations[0]['params'][0])).message)
            == to_bytes_versioned(c2.VersionedTransaction.from_bytes(wire).message)
        and sends.requests[0]['params'][0] == envelope.signed_wire_base64
        and all(c2.VersionedTransaction.from_bytes(wire).verify_with_results()))
    check(f.name+'_'+str(number)+'_send_never_changes_economics',
        f.repo.consumer_snapshot()['funding'].native_lamports == native_before
        and (not f.repo.consumer_snapshot()['positions'] if action.side == 'BUY'
             else f.repo.position_history(action.position_id).remaining_units == units_before))
    reconciled, calls = c2.reconcile(f, scenario, at+6)
    check(f.name+'_'+str(number)+'_public_finality_not_yet_applied', reconciled.reason == 'FINALIZED_SUCCESS_UNAPPLIED'
        and calls and f.repo.consumer_snapshot()['funding'].native_lamports == native_before)
    chain = f.repo.chain_receipt(reconciled.receipt_key)
    support = c2.application_support(f, actual, chain, pre, at+8)
    applied = f.step(at+8, application_wallet=support)
    receipt = f.repo.application_receipt(applied.receipt_key)
    proposal = receipt.decision.proposal
    check(f.name+'_'+str(number)+'_actual_application', applied.reason == 'FINALIZED_SUCCESS_APPLIED'
        and proposal.transaction_fee_lamports == sf.FEE
        and proposal.venue_fee_units == actual.venue_fees
        and proposal.quote_principal_units == actual.principal
        and proposal.base_units_delta == (actual.actual_base if action.side == 'BUY' else -actual.actual_base)
        and proposal.native_wallet_delta == actual.scenario.tx['meta']['postBalances'][actual.index[f.domain.wallet]]
            - actual.scenario.tx['meta']['preBalances'][actual.index[f.domain.wallet]]
        and f.repo.consumer_snapshot()['funding'].native_lamports == native_before+proposal.native_wallet_delta
        and f.repo.consumer_snapshot()['funding'].native_lamports == f.public[f.domain.wallet].lamports)
    components = proposal.components
    check(f.name+'_'+str(number)+'_fees_rent_exact_native_balance',
        sum(c.units for c in components if c.asset == 'SOL' and c.account == f.domain.wallet) == proposal.native_wallet_delta
        and proposal.locked_account_lamports_delta == sum(c.units for c in components if c.asset == 'LOCKED_LAMPORTS')
        and proposal.account_funding_lamports == -sum(c.units for c in components
            if c.kind in ('ACCOUNT_FUNDING', 'VENUE_ACCOUNT_SETUP'))
        and proposal.account_refund_lamports == sum(c.units for c in components if c.kind == 'ACCOUNT_REFUND'))
    before_replay = f.repo.audit()
    repeated = f.repo.apply_settlement(submitted.attempt_id, chain_receipt_key=chain.ingestion_key,
        support=support, ingestion_key=applied.receipt_key, recorded_at_utc=receipt.recorded_at_utc,
        fence=f.repo.write_fence())
    check(f.name+'_'+str(number)+'_exact_application_replay_no_postings', repeated == receipt
        and f.repo.audit() == before_replay)
    if prior_accumulator is not None:
        check(f.name+'_'+str(number)+'_retained_accumulator_no_second_setup_cost',
            f.public[volume] == prior_accumulator
            and not any(c.kind == 'VENUE_ACCOUNT_SETUP' and c.counterparty == volume for c in components))
    if retained_other_accounts:
        check(f.name+'_'+str(number)+'_other_wallet_funded_accounts_unchanged_after_settlement',
            all(f.public.get(pubkey) == account for pubkey, account in retained_other_accounts.items()))
    return applied, {'side': action.side, 'action_id': action.action_id, 'attempt_id': submitted.attempt_id,
        'signature': envelope.primary_signature, 'message_sha256': envelope.preparation.message_sha256,
        'signed_wire_sha256': hashlib.sha256(wire).hexdigest(), 'native_before': native_before,
        'native_after': f.public[f.domain.wallet].lamports, 'proposal': asdict(proposal),
        'actual_base_units': actual.actual_base, 'application_sequence': receipt.sequence,
        'chain_evidence_digest': chain.observation.content_digest,
        'operations_gate': asdict(f.runtime._operations_degradation.snapshot()),
        'accumulator': {'pubkey': volume, 'retained_before': prior_accumulator is not None,
            'lamports_before': 0 if prior_accumulator is None else prior_accumulator.lamports,
            'lamports_after': f.public[volume].lamports},
        'retained_other_wallet_accounts': {pubkey: {'lamports': account.lamports,
            'account_digest': c2.content_fingerprint({'pubkey': pubkey, 'owner': account.account.owner,
                'data_hex': account.account.data.hex(), 'lamports': account.lamports,
                'executable': account.executable, 'rent_epoch': account.rent_epoch})}
            for pubkey, account in retained_other_accounts.items()}}


def lifecycle(f, key, *, shift=0, number=71):
    buy = f.buy
    item = f.repo.candidate(buy.root_id)
    admitted = f.repo.authority_acceptance(buy.root_id)
    applied_buy, buy_evidence = transact(f, key, buy, NOW+shift+6, number)
    binding = f.runtime.position_binding
    monitored = f.step(NOW+shift+14)
    check(f.name+'_'+str(number)+'_actual_single_selected_controller', monitored.work == 'ENTRY_HELD'
        and binding.spec.track_id == buy.selected_exit_track
        and binding.acquisition_application_key == applied_buy.receipt_key
        and c2.protective_outcome(f.repo, binding).state == 'MONITORING'
        and f.repo.protection(buy.position_id).handoff.binding_id == binding.binding_id)
    position_before_sell = f.repo.position_history(buy.position_id)
    staged = f.step(NOW+shift+16)
    check(f.name+'_'+str(number)+'_actual_due_protection', staged.work == 'PROTECTIVE_ACTION_STAGED')
    sell = f.repo.action(staged.action_id)
    check(f.name+'_'+str(number)+'_actual_units_same_root_sell', sell.root_id == buy.root_id
        and sell.input_units == buy_evidence['actual_base_units']
        and f.repo.position_history(buy.position_id) == position_before_sell)
    applied_sell, sell_evidence = transact(f, key, sell, NOW+shift+20, number+1)
    check(f.name+'_'+str(number)+'_no_capacity_release_before_retirement',
        f.repo.reservation(buy.root_id).status == 'RESERVED'
        and c2.protective_outcome(f.repo, binding).state == 'SATISFIED'
        and f.step(NOW+shift+29).work == 'NEED_RETIREMENT')
    retired = f.step(NOW+shift+30, retirement_wallet=continuation.wallet(f, NOW+shift+30))
    receipt = f.repo.port_receipt(retired.receipt_key)
    current = f.repo.consumer_snapshot()
    history = f.repo.position_history(buy.position_id)
    check(f.name+'_'+str(number)+'_lawful_actual_retirement', retired.work == 'RETIREMENT_RETIRED'
        and not current['positions'] and not current['reservations'] and not current['pending_attempts']
        and f.runtime.position_binding is None and history.status == 'RETIRED'
        and history.acquired_units == history.sold_units == buy_evidence['actual_base_units']
        and receipt.retirement.terminal_attempt_id == applied_sell.attempt_id)
    before = stamp(f, 'before exact retirement replay')
    replay = f.repo.retire(receipt.retirement, continuation.wallet(f, NOW+shift+30),
        ingestion_key=retired.receipt_key, fence=f.repo.write_fence())
    check(f.name+'_'+str(number)+'_exact_retirement_idempotence', replay == receipt
        and before == stamp(f, 'before exact retirement replay'))
    check(f.name+'_'+str(number)+'_original_policy_never_rearmed', all(
        f.repo.authority_snapshot()[name] == f.original_authority[name]
        for name in ('policy', 'armed_entry_grant', 'entry_stop_command', 'hard_stop_command')))
    result = {'root': buy.root_id, 'mint': item.mint, 'candidate_digest': item.content_digest,
        'producer_lineage': item.producer_lineage_id, 'source_record_digest': item.source_record_digest,
        'signal_us': item.generated_at_us, 'deadline_us': buy.claimed_entry_deadline_us,
        'live_input_lamports': buy.input_units, 'admission_native_lamports': admitted.risk.native_lamports,
        'grant_id': admitted.consumed_grant_id, 'binding_id': binding.binding_id,
        'selected_track': buy.selected_exit_track, 'buy': buy_evidence, 'sell': sell_evidence,
        'retirement_sequence': receipt.sequence, 'retired_history': asdict(history)}
    EVIDENCE[f.name]['lifecycles'].append(result)
    return result


def run(directory, *, once=False):
    name = 's02-once' if once else 's01-s02-normal'
    key = c2.keypair(name)  # Fixed synthetic signing key; never serialized/logged.
    with patch.object(sf, 'WALLET', str(key.pubkey())), patch.object(sf.plans, 'ACTOR', str(key.pubkey())):
        f = prepare(directory, name, once=once)
        try:
            first = lifecycle(f, key)
            retired_native = f.repo.consumer_snapshot()['funding'].native_lamports
            next_candidate(f)
            second = f.item
            check(name+'_fresh_original_second_mint_root_deadline', second.mint != first['mint']
                and f.root != first['root'] and second.generated_at_us == (NOW+32)*1000000
                and second.producer_lineage_id == first['producer_lineage']
                and f.source.latest_record()[1].disposition == 'HEALTHY'
                and continuation.utc_microseconds(f.source.latest_record()[1].covered_through_utc) == (NOW+32)*1000000)
            admitted = f.step(NOW+36, entry=c2.EntryFacts('PUMP', sf.TOKEN_PROGRAM_ID,
                wallet(f, NOW+36, mint=second.mint)))
            decision = f.repo.authority_admission_receipt(admitted.receipt_key)
            if once:
                check(name+'_retired_capacity_cannot_rearm_once', admitted.work == 'ENTRY_DENIED'
                    and not decision.accepted and not f.repo.consumer_snapshot()['reservations']
                    and not f.repo.consumer_snapshot()['positions']
                    and 'ONE_TIME_ENTRY_GRANT_ALREADY_CONSUMED' in decision.risk.reasons
                    and set(decision.risk.reasons) == {'ENTRY_ELIGIBILITY_NOT_PROVEN',
                        'GRANT_POLICY_OR_ROOT_CONFLICT', 'ONE_TIME_ENTRY_GRANT_ALREADY_CONSUMED'}
                    and f.repo.authority_grant_status(first['grant_id'])['consumed_root'] == first['root']
                    and f.repo.audit()['action_count'] == 2 and f.repo.audit()['attempt_count'] == 2)
                EVIDENCE[name]['second_denial'] = {'eligibility': decision.eligibility.decision.reasons,
                    'risk': decision.risk.reasons, 'root': f.root, 'native_lamports': retired_native}
            else:
                check(name+'_second_admission_uses_returned_native', admitted.work == 'ENTRY_ADMITTED'
                    and decision.risk.native_lamports == retired_native
                    and decision.risk.known_wsol_units == 0 and decision.risk.outstanding_native_lamports == 0
                    and decision.consumed_grant_id == first['grant_id'])
                f.buy = f.repo.action(admitted.action_id)
                check(name+'_distinct_immutable_deadline', f.buy.claimed_entry_deadline_us == second.generated_at_us+15000000
                    and f.buy.claimed_entry_deadline_us != first['deadline_us'])
                second_result = lifecycle(f, key, shift=32, number=81)
                check(name+'_two_fully_retired_separate_positions', first['binding_id'] != second_result['binding_id']
                    and first['retired_history']['position_id'] != second_result['retired_history']['position_id']
                    and f.repo.audit()['action_count'] == 4 and f.repo.audit()['attempt_count'] == 4)
            before = f.repo.audit()
            replay = CandidateHandoffV01(f.producer, f.repo, f.binding, database_identity=hf.DATABASE_ID)
            roots = []
            for _ in range(128):
                page = replay.deliver_page(limit=32)
                roots.extend(page.candidate_roots)
                if not page.scanned_rows:
                    break
            check(name+'_original_outbox_replay_suppressed', set(roots) == {first['root'], second.trade_root(f.domain)}
                and f.repo.audit() == before)
            # Existing Authority contract independently suppresses the old root.
            denied = f.repo.admit_authority_entry(EntryRequest(first['root'], 'PUMP', sf.TOKEN_PROGRAM_ID, f.policy.selected_track),
                a3.clock(f.repo, NOW+64), f.source, wallet(f, NOW+64, mint=first['mint']),
                command_id=name+'-old-root-replay', fence=f.repo.write_fence())
            check(name+'_old_candidate_never_reacquired', not denied.accepted
                and f.repo.audit()['action_count'] == before['action_count'])
            EVIDENCE[name]['old_root_denial'] = {'eligibility': denied.eligibility.decision.reasons, 'risk': denied.risk.reasons}
            EVIDENCE[name]['final'] = stamp(f, 'final replayed journal')
            EVIDENCE[name]['audit'] = f.repo.audit()
            trades = EVIDENCE[name]['lifecycles']
            proposals = [trade[side]['proposal'] for trade in trades for side in ('buy', 'sell')]
            final_native = f.repo.consumer_snapshot()['funding'].native_lamports
            check(name+'_whole_run_native_conservation', final_native == sf.FUNDING
                + sum(p['native_wallet_delta'] for p in proposals)
                and sum(p['transaction_fee_lamports'] for p in proposals) == 2*len(trades)*sf.FEE)
            EVIDENCE[name]['economics'] = {'initial_native_lamports': sf.FUNDING,
                'final_native_lamports': final_native,
                'actual_network_fees_lamports': sum(p['transaction_fee_lamports'] for p in proposals),
                'actual_venue_fees_lamports': sum(p['venue_fee_units'] for p in proposals),
                'actual_account_funding_lamports': sum(p['account_funding_lamports'] for p in proposals),
                'actual_account_refund_lamports': sum(p['account_refund_lamports'] for p in proposals),
                'current_positions': len(f.repo.consumer_snapshot()['positions']),
                'current_reservations': len(f.repo.consumer_snapshot()['reservations'])}
        finally:
            ops.close(f)


def main():
    with c2.fixed_producer_clock(), tempfile.TemporaryDirectory(prefix='step10-s01-s02-') as tmp:
        run(Path(tmp))
        run(Path(tmp), once=True)
    fixture_modules = (ops, continuation, c2, c2.q4, c2.q3, c2.q1, c2.a4, c2.a3, c2.a5, sf, hf, sf.plans, sf.plans.fx)
    paths = {Path(__file__), *(c2.ROOT/'src/live').glob('*.py'), *(Path(m.__file__) for m in fixture_modules)}
    hashes = {str(p.relative_to(c2.ROOT)).replace('\\', '/'): lf_sha256(p) for p in paths}
    print(c2.canonical_json({'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'scope': ['S01', 'S02'],
        'fixture_version': FIXTURE_VERSION, 'qualification': 'SYNTHETIC_ENGINEERING_ONLY_NOT_HOST_PROFILE_OR_REAL_CAPITAL',
        'lifecycle_counts': {'normal': 2, 'one_time_variant': 1}, 'checks': CHECKS,
        'artifact_hashes': hashes, 'evidence': EVIDENCE}))


if __name__ == '__main__':
    main()
