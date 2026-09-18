"""C2 actual producer-to-protection composition, isolated public/mock I/O only."""
from __future__ import annotations
import ast
import base64
import copy
import hashlib
import json
import sqlite3
import sys
import tempfile
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import datetime,timedelta
from pathlib import Path
from unittest.mock import patch
import httpx
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
import live_execution_composition_selftest_v0_1 as q4
import live_candidate_handoff_selftest_v0_1 as hf
from live.runtime_composition_v0_1 import RuntimeCompositionV01, EntryFacts, ExecutionPorts
from live.execution_readonly_v0_1 import ExecutionReadOnlyRpc
from live.execution_signer_v0_1 import AutonomousLocalSigner
from live.execution_message_v0_1 import ComputeBudget
from live.ledger_actions_v0_1 import decode_message
from live.public_rpc_v0_1 import PublicReadOnlyRpc
from live.protective_outcome_v0_1 import protective_outcome
from live.evidence_store_v0_1 import SourceEvidenceStore
from live.source_health_v0_1 import SourceProfile
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint

q3, q1, a4, a3, a5, sf, NOW = q4.q3, q4.q1, q4.a4, q4.a3, q4.a5, q4.sf, q4.NOW
CHECKS = {}

def check(name, value):
    CHECKS[name] = bool(value)
    if not value: raise AssertionError(name)

def fails(call):
    try: call()
    except (ValueError, RuntimeError, TypeError): return True
    return False

def keypair(name):
    # Fixed fixture wallet: the actor fingerprint feeds the hashed fee-recipient
    # choice, so a random key made lifecycle outcomes vary per run.
    return Keypair.from_seed(hashlib.sha256(name.encode()).digest())

@contextmanager
def fixed_producer_clock():
    # Cold startup builds the producer without wall_clock, so datetime.now()
    # P1 stamps reach exit-evidence digests, the B3 SELL intent and the hashed
    # fee-recipient choice. Same fixed clock as hf.Fixture.open_producer;
    # exact production type retained (A07 fixed_clock_producer pattern).
    import live.runtime_reconstruction_v0_1 as reconstruction
    production = reconstruction.LiveContinuousProducerV02
    def fixed(conn, market_source, **kwargs):
        return production(conn, market_source, wall_clock=lambda: hf.BASE, **kwargs)
    with patch.object(reconstruction, 'LiveContinuousProducerV02', fixed):
        yield


class Fixture(hf.Fixture):
    def __init__(self, directory, name, *, batch=32, page=32, queue=64):
        self.directory, self.name = directory, name
        self.raw, self.path, self.ppath = (directory/(name+s) for s in ('-raw.sqlite','-ledger.sqlite','-producer.sqlite'))
        self.binding = hf.build_raw(self.raw)
        self.market = sf.Fixture()
        self.market.mint = sf.MINT
        self.scenario = self.market.wallet_scenario()
        self.domain = a3.domain(wallet=sf.WALLET)
        self.repo = a3.LedgerRepository.initialize(self.path, self.domain)
        a3.ingest(self.repo, a3.observe(self.scenario, request=a3.WalletEvidenceRequest(sf.WALLET,a3.GENESIS,100)))
        self.source = SourceEvidenceStore(directory/(name+'-evidence.sqlite'), self.binding, SourceProfile())
        self.append([(n,sf.MINT if mint==hf.MINT_A else mint,kind,price) for n,mint,kind,price in hf.FIRST])
        self.open_producer(initialize=True)
        self.runtime = RuntimeCompositionV01(self.producer,self.handoff,self.repo,self.source,
            batch_rows=batch,page_rows=page,queued_roots=queue)
        self.public = {self.domain.wallet:a5.PublicAccount(a5.RpcAccountV01(self.domain.wallet,sf.SYSTEM_PROGRAM_ID,b''),sf.FUNDING,False,0),
            sf.MINT:a5.PublicAccount(sf.plans.fx.mint_account(),1000000000,False,0),
            sf.WSOL_MINT:a5.PublicAccount(a5.RpcAccountV01(sf.WSOL_MINT,sf.TOKEN_PROGRAM_ID,a3.mint_data(9)),1000000000,False,0)}
        self.lower = self.repo.baseline().observation.anchor

    def step(self, at=NOW+4, **kwargs):
        calls = []
        def clock():
            calls.append(None)
            value = a3.clock(self.repo,at)
            stamp = (datetime.fromisoformat(value.utc_upper_utc)+timedelta(microseconds=len(calls)-1)).isoformat(timespec='microseconds')
            return replace(value,utc_lower_utc=stamp,utc_upper_utc=stamp,monotonic_ns=value.monotonic_ns+1000*(len(calls)-1))
        return self.runtime.step(clock=clock,source_cut_utc=a3.utc(NOW+1),**kwargs)

    def candidate_ready(self):
        for _ in range(128):
            result = self.step()
            if result.work == 'NEED_ENTRY_FACTS':
                self.root, self.item = result.root_id, self.repo.candidate(result.root_id)
                return result
        raise AssertionError('bounded original candidate not delivered')

    def configure(self):
        legacy = a3.a1.policy(self.item,binding=self.domain)
        self.policy = a3.policy_v02(replace(legacy,size=replace(legacy.size,
            fixed_quote_lamports=self.market.units,ceiling_quote_lamports=self.market.units,
            max_trade_notional_lamports=self.market.units,max_global_exposure_lamports=3*self.market.units,
            max_mint_exposure_lamports=self.market.units),costs=replace(legacy.costs,
            venue_fee_within_quote_cap_lamports=2000000,setup_outflow_lamports=10000000,
            refundable_account_lock_lamports=10000000,protective_setup_lamports=10000000,
            protective_refundable_lock_lamports=10000000)))
        a3.control(self.repo,'INSTALL_POLICY','install',policy_value=self.policy)
        a3.arm(self.repo,self.policy)

    def admit(self):
        self.candidate_ready()
        self.configure()
        support=a3.wallet(self,scenario=self.scenario)
        result=self.step(entry=EntryFacts('PUMP',sf.TOKEN_PROGRAM_ID,support))
        check(self.name+'_actual_root_admission',result.work=='ENTRY_ADMITTED')
        self.buy=self.repo.action(result.action_id)
        check(self.name+'_actual_candidate_identity',self.buy.candidate_digest==self.item.content_digest
            and self.item.producer_lineage_id==self.producer.source_identity and self.item.trade_root(self.domain)==self.buy.root_id)
        check(self.name+'_actual_source_observed_by_root',self.source.latest_record()[1].disposition=='HEALTHY'
            and self.repo.authority_acceptance(self.root).accepted)
        return result


def recipient_accounts(seed,side,originals):
    """Public accounts for every verified fee recipient the production plan may select.

    Production picks fee/buyback recipients by hash over quote/actor/policy
    fingerprints, so the seed's own selection binds nothing. Each candidate is
    shaped exactly as the seed shaped its selection: a plain system account, and
    a WSOL ATA that is absent (pump) or an empty token account (pumpswap SELL).
    Retained public facts win, as in the seed.
    """
    e,venue,plans,fx=seed.evidence,a4.venue,a4.plans,sf.plans.fx
    raw={key:value.account for batch in (e.venue_read.primary,e.venue_read.dependent)
        for key,value in zip(batch.requested_keys,batch.accounts) if value is not None}
    cut=e.venue_read.primary.cut;mint=e.intent.mint
    curve=venue.build_pump_state(e.intent,*(raw[key] for key in (venue.derive_bonding_curve_pda(mint),mint,
        venue.derive_pump_global_pda(),venue.derive_pump_fee_config_pda())),
        slot_min=cut.context_slot,slot_max=cut.context_slot,observed_at_us=cut.observed_at_us)
    pool=None
    if raw.get(venue.derive_pumpswap_pool_pda(mint)) is not None:
        decoded=venue.decode_pumpswap_pool(raw[venue.derive_pumpswap_pool_pda(mint)],mint)
        pool=venue.build_pumpswap_state(e.intent,*(raw[key] for key in (venue.derive_pumpswap_pool_pda(mint),
            decoded.pool_base_token_account,decoded.pool_quote_token_account,mint,
            venue.derive_pumpswap_global_pda(),venue.derive_pumpswap_fee_config_pda())),
            slot_min=cut.context_slot,slot_max=cut.context_slot,observed_at_us=cut.observed_at_us)
    route=venue.decide_route(e.intent,curve,pool)
    pump=route.selected_state_id==curve.state_id
    state=curve if pump else pool
    recipients=plans.decode_verified_recipient_evidence(state,raw[venue.derive_pump_global_pda() if pump else venue.derive_pumpswap_global_pda()])
    accounts={}
    for key in recipients.normal_fee_recipients+recipients.reserved_fee_recipients+recipients.buyback_fee_recipients:
        ata=plans.derive_associated_token_address(key,sf.WSOL_MINT,sf.TOKEN_PROGRAM_ID)
        accounts[key]=originals.get(key) or a4.original_public(fx.account(key,sf.SYSTEM_PROGRAM_ID,b''))
        accounts[ata]=originals.get(ata) or (None if pump or side=='BUY' else
            a4.original_public(fx.account(ata,sf.TOKEN_PROGRAM_ID,a3.token_data(sf.WSOL_MINT,key,0,reserve=sf.R)),sf.R))
    return accounts


def recipient_agnostic_cpi(seed,recipients,transaction_base64,groups):
    """Seed CPI rows re-indexed onto the exact simulated message.

    Only a fee-recipient substitution (and its ATA) is translated; any other
    difference leaves the seed rows untouched so production still rejects it.
    """
    if type(groups) is not list:return groups
    message=decode_message(base64.b64decode(transaction_base64)[65:].hex())
    seed_message=decode_message(seed.evidence.simulation.envelopes[0].message_hex)
    keys=tuple(map(str,message.account_keys));seed_keys=tuple(map(str,seed_message.account_keys))
    if keys==seed_keys or len(message.instructions)!=len(seed_message.instructions):return groups
    alias={}
    for s,p in zip(seed_message.instructions,message.instructions):
        if len(s.accounts)!=len(p.accounts):return groups
        for a,b in zip((s.program_id_index,*s.accounts),(p.program_id_index,*p.accounts)):
            if alias.setdefault(seed_keys[a],keys[b])!=keys[b]:return groups
    changed={a for a,b in alias.items() if a!=b}
    if not(changed<=set(recipients) and {alias[a] for a in changed}<=set(recipients)):return groups
    index={key:i for i,key in enumerate(keys)}
    remap=lambda i:index[alias.get(seed_keys[i],seed_keys[i])]
    return [{**group,'instructions':[{**row,'programIdIndex':remap(row['programIdIndex']),
        'accounts':[remap(i) for i in row['accounts']]} for row in group['instructions']]} for group in groups]


def execution(f,key,action,*,at,number,outcome='ACKNOWLEDGED',gap_during_read=False,stop_during_read=False):
    intent=q1._intent(q1.capture_message_context(f.repo,action.action_id),at*1000000+1)
    seed,_=a4.evidence(f,action,a5.wallet_scenario(f.public,f.lower),at=at,context_slot=f.lower.slot,
        intent=intent,wallet_floor=f.lower.slot,recent_blockhash=sf.bh(number),
        last_valid_height=f.lower.block_height+30,validity_height=f.lower.block_height+1,original_accounts=f.public)
    if f.repo.authority_message_profile(action.policy_digest) is None: q1.install(f,seed)
    class ReadBoundary(q1.PublicTransport):
        def __init__(self,seed,**kwargs):
            super().__init__(seed,**kwargs)
            # Recipient-agnostic: the seed's hash-selected recipients stay
            # authoritative; every other verified recipient is answered the same way.
            self.recipients=recipient_accounts(seed,action.side,f.public)
            self.setup={**self.recipients,**self.setup}
        def __call__(self,request):
            response=super().__call__(request)
            value=response.json();payload=json.loads(request.content);method=payload['method']
            if method=='getLatestBlockhash':value['result']['value']['lastValidBlockHeight']=f.lower.block_height+30
            elif method=='getBlockHeight':value['result']=f.lower.block_height+1
            if method=='simulateTransaction' and stop_during_read:
                a3.control(f.repo,'HARD_STOP','during-execution-stop',at=at)
            if method=='simulateTransaction' and value.get('result'):
                value['result']['value']['innerInstructions']=recipient_agnostic_cpi(self.seed,self.recipients,
                    payload['params'][0],value['result']['value']['innerInstructions'])
            return httpx.Response(200,json=value)
    read=ReadBoundary(seed)
    send=q3.Boundary(f,outcome)
    with ExecutionReadOnlyRpc('https://invalid.local',a3.PROFILE,now_us=read.now,transport=httpx.MockTransport(read)) as rpc:
        transport=q4.send.SolanaSendTransport('https://invalid.local',a3.PROFILE,transport=httpx.MockTransport(send))
        try:
            ports=ExecutionPorts(rpc,seed.evidence.wallet,seed.evidence.quote_policy,seed.evidence.plan_policy,
                ComputeBudget(250000,1000),AutonomousLocalSigner(key),transport,
                lambda:seed.evidence.simulation.leases[0].observed_at_us)
            result=f.step(at,execution=ports)
        finally:transport.close()
    # Independent declared chain facts include read-only program balances;
    # Q1's setup query only needs complete economic account data.
    f._fixture_chain_accounts=dict(read.setup)
    return result,read,send


def original_chain(f,result,at,*,failed=False):
    envelope=q4.send.load_signed_envelope(f.repo,result.attempt_id)
    original=envelope.authority_receipt.original.validation
    plan=q4._rebuild(original)[-1]
    action=f.repo.action(envelope.preparation.action_id)
    inputs=original.evidence.setup_accounts
    pre=dict(f._fixture_chain_accounts)
    pre.update(zip(inputs.requested_keys,inputs.accounts))
    # Exactly the accounts of the message production actually built; declared
    # chain facts for recipients it did not select are not part of this chain.
    pre={key:pre[key] for key in map(str,decode_message(envelope.preparation.message_hex).account_keys) if key in pre}
    actual=sf.Fixture('pump',action.side,failed=failed,token_program=action.token_program,
        external_plan=plan,external_message_hex=envelope.preparation.message_hex,
        external_wire=base64.b64decode(envelope.signed_wire_base64),external_pre_accounts=pre)
    actual.mint=action.mint
    scenario=q4.chain_scenario(actual,envelope.preparation,at,envelope.primary_signature)
    return envelope,actual,scenario,pre


def reconcile(f,scenario,at):
    calls=[]
    def external(request):
        calls.append(json.loads(request.content))
        return scenario.handle(request)
    with PublicReadOnlyRpc('https://invalid.local',a3.PROFILE,transport=httpx.MockTransport(external)) as rpc:
        result=f.step(at,truth_rpc=rpc)
    return result,calls


def application_support(f,actual,chain,pre,at):
    after=a5.post_accounts({**f.public,**pre},actual)
    root=chain.observation.root
    anchor=replace(root,slot=root.slot+2,blockhash=sf.bh(root.slot+2),previous_blockhash=root.blockhash,
        parent_slot=root.slot,block_height=root.block_height+1,block_time=at-1)
    expected={item.pubkey:(item.mint,item.program) for item in f.repo.consumer_snapshot()['accounts']}
    expected.update({actual.base:(actual.mint,actual.token_program),actual.quote:(sf.WSOL_MINT,sf.TOKEN_PROGRAM_ID)})
    request=a3.WalletEvidenceRequest(f.domain.wallet,f.domain.genesis_hash,chain.observation.transaction.slot,
        tuple(a3.ExpectedTokenAccount(k,m,p) for k,(m,p) in sorted(expected.items())))
    support=a3.WalletSupportInput(a3.observe(a5.wallet_scenario(after,anchor),request=request,at=at),a3.utc(at),request.min_context_slot)
    f.public,f.lower=after,anchor
    return support


def acquisition(f,key):
    f.admit()
    result,read,send=execution(f,key,f.buy,at=NOW+6,number=71)
    check(f.name+'_actual_Q1_Q2_Q3_root',result.work=='SUBMISSION_OBSERVED' and len(send.requests)==1
        and len([r for r in read.requests if r['method']=='simulateTransaction'])==1)
    envelope,actual,scenario,pre=original_chain(f,result,NOW+6)
    simulated=base64.b64decode(read.requests[-1]['params'][0]);signed=base64.b64decode(envelope.signed_wire_base64)
    check(f.name+'_same_exact_simulated_signed_sent_message',simulated[65:]==signed[65:]
        and send.requests[0]['params'][0]==envelope.signed_wire_base64
        and all(VersionedTransaction.from_bytes(signed).verify_with_results()))
    reconciled,calls=reconcile(f,scenario,NOW+12)
    check(f.name+'_actual_Q4_finality',reconciled.work=='RECONCILED' and reconciled.reason=='FINALIZED_SUCCESS_UNAPPLIED'
        and calls and not f.repo.consumer_snapshot()['positions'])
    chain=f.repo.chain_receipt(reconciled.receipt_key)
    support=application_support(f,actual,chain,pre,NOW+14)
    applied=f.step(NOW+14,application_wallet=support)
    check(f.name+'_actual_application_and_B1',applied.work=='APPLIED' and applied.reason=='FINALIZED_SUCCESS_APPLIED'
        and f.runtime.position_binding.acquisition_application_key==applied.receipt_key
        and f.repo.position_history(f.buy.position_id).remaining_units==actual.actual_base)
    return actual


def lifecycle(directory):
    key=keypair('actual-composition')
    with patch.object(sf,'WALLET',str(key.pubkey())),patch.object(sf.plans,'ACTOR',str(key.pubkey())):
        f=Fixture(directory,'actual-composition',page=1,queue=1)
        try:
            actual=acquisition(f,key)
            monitored=f.step(NOW+14)
            check('actual_B2_B3_monitoring_before_entry',monitored.work=='ENTRY_HELD'
                and f.repo.protection(f.buy.position_id) is not None
                and protective_outcome(f.repo,f.runtime.position_binding).state=='MONITORING')
            # Distinct later canonical candidate is produced by the same root.
            f.append(hf.NEXT)
            for _ in range(128):
                f.step(NOW+14)
                if f.runtime.queued_roots:break
            check('distinct_candidate_retained_while_position_occupied',len(f.runtime.queued_roots)==1
                and f.runtime.queued_roots[0]!=f.buy.root_id
                and f.repo.candidate(f.runtime.queued_roots[0]).mint!=f.buy.mint
                and f.repo.authority_acceptance(f.runtime.queued_roots[0]) is None)
            pending=f.runtime.queued_roots
            f.append([(29,sf.MINT,'BUY',10200)])
            first_cut=f.producer.durable_p1_rowid
            f.step(NOW+15)
            check('full_candidate_queue_does_not_starve_active_market_source',f.producer.durable_p1_rowid==29
                and f.producer.durable_p1_rowid>first_cut and f.runtime.queued_roots==pending)
            f.step(NOW+15)
            stable=f.repo.exit_records(f.runtime.position_binding.binding_id)
            f.step(NOW+15)
            check('identical_monitoring_knowledge_no_new_exit_journal',f.repo.exit_records(f.runtime.position_binding.binding_id)==stable)
            producer_cut=dict(f.producer.metrics)
            f.conn.close()  # External source-store outage; Ledger/Evidence remain intact.
            due=f.step(NOW+16)
            check('source_failure_due_protection_precedes_waiting_candidate',due.work=='PROTECTIVE_ACTION_STAGED'
                and f.runtime.queued_roots==pending and f.producer.metrics==producer_cut)
            sell=f.repo.action(due.action_id)
            check('root_sell_uses_actual_original_position_units',sell.side=='SELL' and sell.input_units==actual.actual_base
                and sell.root_id==f.buy.root_id and sell.obligation_id==f.repo.protection(f.buy.position_id).handoff.obligation_id)
            result,read,send=execution(f,key,sell,at=NOW+20,number=72,outcome='TRANSPORT_UNRESOLVED')
            check('protective_actual_execution_with_original_queue',result.work=='SUBMISSION_OBSERVED'
                and result.reason=='TRANSPORT_UNRESOLVED' and len(send.requests)==1 and f.runtime.queued_roots==pending)
            envelope,actual_sell,scenario,pre=original_chain(f,result,NOW+20)
            unknown=copy.deepcopy(scenario);unknown.tx=unknown.status=None
            before=f.repo.position_history(f.buy.position_id)
            records=f.repo.exit_records(f.runtime.position_binding.binding_id)
            observed,calls=reconcile(f,unknown,NOW+26)
            check('held_due_does_not_starve_actual_reconciliation',observed.work=='RECONCILED' and calls
                and f.repo.attempt(result.attempt_id).lane_held and f.repo.position_history(f.buy.position_id)==before
                and f.repo.exit_records(f.runtime.position_binding.binding_id)==records)
            held=f.step(NOW+26)
            check('UNKNOWN_no_retry_no_second_acquisition',held.work=='NEED_RECONCILIATION'
                and f.repo.audit()['attempt_count']==2 and f.runtime.queued_roots==pending
                and f.repo.authority_acceptance(pending[0]) is None)
            a3.control(f.repo,'HARD_STOP','held-hard-stop',at=NOW+26)
            original_signature=f.repo.attempt(result.attempt_id).primary_signature
            known,calls=reconcile(f,scenario,NOW+27)
            check('hard_stop_and_held_protection_allow_truth_without_new_signature',known.work=='RECONCILED' and calls
                and f.repo.attempt(result.attempt_id).primary_signature==original_signature and f.repo.audit()['attempt_count']==2)
            chain=f.repo.chain_receipt(known.receipt_key)
            support=application_support(f,actual_sell,chain,pre,NOW+28)
            applied=f.step(NOW+28,application_wallet=support)
            check('actual_final_reduction_application',applied.work=='APPLIED' and applied.reason=='FINALIZED_SUCCESS_APPLIED')
            satisfied=f.step(NOW+29)
            check('actual_SATISFIED_keeps_reservation_without_retirement_support',protective_outcome(f.repo,f.runtime.position_binding).state=='SATISFIED'
                and satisfied.work=='NEED_RETIREMENT' and f.repo.reservation(f.buy.root_id).status=='RESERVED'
                and f.runtime.queued_roots==pending and f.repo.audit()['attempt_count']==2)
            check('full_root_Ledger_integrity',bool(f.repo.audit()))
        finally:f.close()



def admission_and_identity_negatives(directory):
    key=keypair('same-clock-gap')
    with patch.object(sf,'WALLET',str(key.pubkey())),patch.object(sf.plans,'ACTOR',str(key.pubkey())):
        f=Fixture(directory,'same-clock-gap')
        other=Fixture(directory,'other-handles')
        try:
            check('wrong_producer_handle_rejected',fails(lambda:RuntimeCompositionV01(other.producer,f.handoff,f.repo,f.source)))
            check('wrong_Ledger_handle_rejected',fails(lambda:RuntimeCompositionV01(f.producer,f.handoff,other.repo,f.source)))
            from live.source_health_v0_1 import SourceBinding
            wrong=replace(f.binding,lineage='OTHER:SYNTHETIC:LINEAGE')
            with SourceEvidenceStore(directory/'wrong-source.sqlite',wrong,SourceProfile()) as store:
                check('wrong_source_binding_rejected',fails(lambda:RuntimeCompositionV01(f.producer,f.handoff,f.repo,store)))
            check('invalid_dispatch_bound_rejected',fails(lambda:RuntimeCompositionV01(f.producer,f.handoff,f.repo,f.source,batch_rows=0)))
            check('generic_execution_semantics_rejected',fails(lambda:ExecutionPorts(object(),None,None,None,None,object(),object(),lambda:0)))
            f.admit()
            old=f.source.latest_record()
            with closing(sqlite3.connect(f.raw)) as raw,raw:
                raw.execute('INSERT INTO gap_jobs_v034 VALUES(?,?,?,?,?,?,?,?,?)',
                    ('late-discovered-original-gap',a3.utc(NOW),a3.utc(NOW+1),'PENDING',None,None,0,0,None))
            with q4.q2.key_calls() as signing:
                result,reads,sends=execution(f,key,f.buy,at=NOW+4,number=81)
            check('same_clock_new_gap_is_actually_reobserved',f.source.latest_record()!=old
                and f.source.latest_record()[1].disposition=='GAP'
                and f.source.latest_record()[1].snapshot.observed_at_utc==old[1].snapshot.observed_at_utc)
            check('actual_A4b_gap_denies_BUY_SIGN_without_erasing_reservation',result.work=='HELD'
                and result.reason=='AUTHORITY_SIGN_NOT_FRESHLY_GRANTED' and not signing and not sends.requests
                and f.repo.reservation(f.buy.root_id).status=='RESERVED'
                and f.repo.attempt(result.attempt_id).primary_signature is None)
            check('unsigned_held_never_reprepares_or_admits_second',f.step(NOW+5).work=='HELD'
                and f.repo.audit()['attempt_count']==1)
        finally:f.close();other.close()
    f=Fixture(directory,'actual-entry-denial')
    try:
        f.candidate_ready();f.configure()
        a3.control(f.repo,'STOP_ENTRY','external-entry-stop',at=NOW+4)
        result=f.step(entry=EntryFacts('PUMP',sf.TOKEN_PROGRAM_ID,a3.wallet(f,scenario=f.scenario)))
        receipt=f.repo.authority_admission_receipt(result.receipt_key)
        check('real_Authority_denial_distinct_from_scheduler_hold',result.work=='ENTRY_DENIED' and not receipt.accepted
            and f.repo.audit()['action_count']==0 and not f.repo.consumer_snapshot()['reservations'])
    finally:f.close()


def timer_fences(directory):
    f=Fixture(directory,'producer-timer',batch=2,page=2,queue=2)
    try:
        timer=f.producer.prepare_clock_tick(datetime.fromisoformat(a3.utc(NOW+4)))
        f.append([(15,hf.MINT_B,'BUY',10500)])
        cut=f.producer.metrics['source_rows_read']
        f.step(NOW+4)
        check('prepared_timer_drains_only_bounded_source_batch',0<f.producer.metrics['source_rows_read']-cut<=2
            and f.producer._prepared_timer(timer)['status']!='COMPLETE')
        for _ in range(32):
            f.step(NOW+4)
            if f.producer._prepared_timer(timer)['status']=='COMPLETE':break
        check('original_minimum_watermark_timer_completes_without_t0_replay',f.producer._prepared_timer(timer)['status']=='COMPLETE'
            and f.producer.durable_p1_rowid==14)
        f.step(NOW+5)
        check('source_beyond_original_timer_fence_continues',f.producer.durable_p1_rowid==15)
    finally:f.close()


def market_and_interrupted_evaluation(directory):
    import live_exit_observation_selftest_v0_1 as b2
    import live.runtime_composition_v0_1 as module
    key=keypair('market-root')
    with patch.object(sf,'WALLET',str(key.pubkey())),patch.object(sf.plans,'ACTOR',str(key.pubkey())):
        f=Fixture(directory,'market-root')
        try:
            acquisition(f,key)
            class Interrupted(BaseException):pass
            with patch.object(module,'commit_exit_evaluation',side_effect=Interrupted):
                try:f.step(NOW+14)
                except Interrupted:pass
                else:raise AssertionError('evaluation cut not reached')
            records=f.repo.exit_records(f.runtime.position_binding.binding_id)
            check('actual_capture_survives_before_evaluation_cut',records and all(r.kind==module.EVIDENCE for r in records))
            result=f.step(NOW+14)
            check('evidence_only_history_reaches_actual_B2_B3',result.work=='ENTRY_HELD'
                and protective_outcome(f.repo,f.runtime.position_binding).state=='MONITORING')
            sig=b2.sig(50)
            chain=f.repo.chain_receipt(f.runtime.position_binding.acquisition_chain_receipt_key)
            left=chain.observation
            market_slot=left.root.parent_slot
            with closing(sqlite3.connect(f.raw)) as raw,raw:
                hf.raw_fixture.insert_row(raw,15,sf.MINT,'BUY',price_units=12000,observed_offset=28)
                raw.execute('UPDATE pump_events SET signature=?,slot=?,event_key=?,inserted_at_utc=decoded_at_utc WHERE rowid=15',
                    (sig,market_slot,sig+':0:0123456789abcdef'))
                raw.execute('INSERT INTO websocket_observations VALUES(?,?,?,?,?)',(sig,market_slot,a3.utc(NOW+14),1,'{}'))
            f.step(NOW+14)  # Real root advances raw producer after nonactionable monitoring.
            seq=f.conn.execute(f'SELECT event_sequence FROM {hf.TABLES[5]} WHERE event_key=?',(sig+':0:0123456789abcdef:MARKET',)).fetchone()[0]
            chain=f.repo.chain_receipt(f.runtime.position_binding.acquisition_chain_receipt_key)
            left=chain.observation
            block={'blockhash':left.root.previous_blockhash,'previousBlockhash':left.membership_block.blockhash,'parentSlot':left.transaction.slot,
                'blockHeight':left.root.block_height-1,'blockTime':NOW+10,'signatures':[sig]}
            tx=b2.transaction(50,slot=market_slot);tx['blockTime']=block['blockTime']
            right=replace(left,request=replace(left.request,signature=sig),
                status=replace(left.status,signature=sig,slot=market_slot,outcome=b2.decode_transaction_result(sig,tx,a3.PROFILE).outcome),
                transaction=b2.decode_transaction_result(sig,tx,a3.PROFILE),membership_block=b2.decode_signature_block(market_slot,block,a3.PROFILE))
            triggered=f.step(NOW+14,exit_proofs={seq:(left,right)})
            state=protective_outcome(f.repo,f.runtime.position_binding)
            check('actual_root_market_evidence_reaches_B2_trigger_before_fallback',triggered.work=='PROTECTIVE_ACTION_STAGED'
                and state.obligation.evaluation.payload['state']['trigger_kind']=='MARKET'
                and state.obligation.evaluation.payload['state']['trigger_reason']=='TAKE_PROFIT'
                and state.obligation.evaluation.payload['timer_fence_us']<f.runtime.position_binding.fallback_fire_boundary_us)
            check('market_trigger_retains_original_binding_deadline',state.obligation.handoff.fallback_anchor_us==f.item.generated_at_us
                and f.runtime.position_binding.fallback_deadline_us==f.item.generated_at_us+15000000)
        finally:f.close()


def structural():
    path=ROOT/'src/live/runtime_composition_v0_1.py'
    tree=ast.parse(path.read_text(encoding='utf-8-sig'))
    imports={n.module for n in ast.walk(tree) if isinstance(n,ast.ImportFrom)}
    check('root_uses_production_consumers_only',not any('selftest' in (name or '') for name in imports))
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    check('only_Ledger_retirement_no_Operations_mutation','retire' in calls and not calls & {'retire_with_ports','record_retirement','initialize','reopen','terminate','kill','load_keypair','from_seed'})
    methods=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='RuntimeCompositionV01')
    step=next(n for n in methods.body if isinstance(n,ast.FunctionDef) and n.name=='step')
    check('no_injected_semantic_callbacks',not {arg.arg for arg in (*step.args.args,*step.args.kwonlyargs)}
        & {'admit','execute','settle','protect','verdict','dry','mode'})


def main():
    with tempfile.TemporaryDirectory(prefix='live-runtime-c2-') as tmp:
        directory=Path(tmp)
        lifecycle(directory)
        admission_and_identity_negatives(directory)
        timer_fences(directory)
        market_and_interrupted_evaluation(directory)
        structural()
    print(canonical_json({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(CHECKS),'all_checks':all(CHECKS.values())}))

if __name__=='__main__':main()
