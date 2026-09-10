"""A5 exact public-message composition; temporary fixtures, no real signer/send.

Every acquisition/reduction follows its own true A4b SIGN/SEND consumption.
Runtime handoff/reduction/retirement inputs remain explicit external fixtures.
"""
from __future__ import annotations
import base64
import copy
import hashlib
import json
import struct
import sys
import tempfile
from dataclasses import replace, asdict
from pathlib import Path
from solders.pubkey import Pubkey
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
import live_authority_message_control_selftest_v0_1 as control
import live_ledger_finality_selftest_v0_1 as chain_fixture
from live.authority_message_control_v0_1 import FreshStageConsumption, MessageStageReceipt, MessageProfileCommand
from live.ledger_actions_v0_1 import AttemptStageInput, PendingAction
from live.public_rpc_v0_1 import PublicAccount
from phase5.shadow_venue_route_quote_v0_1 import RpcAccountV01
from phase5.shadow_domain_v0_1 import content_fingerprint, canonical_json
import phase5.shadow_repository_v0_1 as shadow_repository

a4,a3,sf,NOW=control.a4,control.a3,control.sf,control.NOW
CHECKS={}


def check(name,condition):
    CHECKS[name]=bool(condition)
    if not condition:raise AssertionError(name)


def wallet_scenario(accounts,anchor):
    value=a3.Scenario();value.accounts={}
    for key,public in accounts.items():
        if public is None:continue
        raw=public.account
        value.accounts[key]=a3.account(raw.owner,raw.data,public.lamports,executable=public.executable,rentEpoch=public.rent_epoch)
        if raw.owner in (sf.TOKEN_PROGRAM_ID,sf.TOKEN_2022_PROGRAM_ID) and len(raw.data)>=165 and str(Pubkey.from_bytes(raw.data[32:64]))==sf.WALLET:
            value.inventory[raw.owner].append(key)
    value.context=value.multiple_context=value.initial_slot=value.upper_slot=anchor.slot
    def transform(payload,envelope):
        if payload['method']=='getBlock':
            envelope['result'].update(blockhash=anchor.blockhash,previousBlockhash=anchor.previous_blockhash,
                parentSlot=anchor.parent_slot,blockHeight=anchor.block_height,blockTime=anchor.block_time)
        return envelope
    value.transform=transform
    return value


def post_accounts(previous,fixture):
    """Independent next public facts from the exact prior RPC metadata only."""
    result=dict(previous);meta=fixture.scenario.tx['meta']
    tokens={fixture.keys[row['accountIndex']]:row for row in meta['postTokenBalances']}
    allocations={}
    for group in fixture.groups:
        for ix in group['instructions']:
            raw=a3.cf._b58data(ix['data'],65536)
            if fixture.keys[ix['programIdIndex']]==sf.SYSTEM_PROGRAM_ID and len(raw)==52 and raw[:4]==bytes(4):
                allocations[fixture.keys[ix['accounts'][1]]]=(str(Pubkey.from_bytes(raw[20:52])),int.from_bytes(raw[12:20],'little'))
    for index,key in enumerate(fixture.keys):
        lamports=meta['postBalances'][index]
        if key in tokens:
            row=tokens[key];units=int(row['uiTokenAmount']['amount']);reserve=sf.R if row['mint']==sf.WSOL_MINT else None
            raw=a3.token_data(row['mint'],row['owner'],units,reserve=reserve)
            result[key]=PublicAccount(RpcAccountV01(key,row['programId'],raw),lamports,False,0)
        elif key in allocations and lamports:
            owner,space=allocations[key]
            result[key]=PublicAccount(RpcAccountV01(key,owner,bytes(space)),lamports,False,0)
        elif previous.get(key) is not None:
            result[key]=replace(previous[key],lamports=lamports)
        else:
            assert lamports==0
            result[key]=None
    return result


def stage(f,prep,target,at,wire=None):
    attempt=f.repo.attempt(prep.attempt_id)
    original=AttemptStageInput(prep.attempt_id,target,prep.action_content_digest,prep.message_sha256,prep.lease.fingerprint,
        prep.message_policy_digest,a3.utc(at),'EXTERNAL_PUBLIC_'+target,content_fingerprint({'stage':target,'wire':None if wire is None else hashlib.sha256(wire).hexdigest()}),
        None if wire is None else base64.b64encode(wire).decode())
    return f.repo.record_external_attempt_stage(original,idempotency_key=prep.attempt_id+target,
        expected_attempt_revision=attempt.revision,fence=f.repo.write_fence())


def exit_action(f,buy,handoff,quantity,ordinal,at):
    action=PendingAction(buy.root_id,buy.candidate_digest,'SELL',buy.mint,buy.token_program,buy.position_id,quantity,
        'EXTERNAL_RUNTIME_REDUCTION',content_fingerprint({'obligation':handoff.obligation_id,'quantity':quantity,'ordinal':ordinal}),
        buy.policy_ref,buy.policy_digest,buy.selected_exit_track,handoff.obligation_id,ordinal)
    f.repo.stage_action(action,fence=f.repo.write_fence())
    context=a4.capture_message_context(f.repo,action.action_id);item=context.candidate
    intent=a4.ExecutionIntentV01(item.candidate_signal_id,item.candidate_run_id,item.strategy_evaluation_id,item.strategy_version,
        item.parameter_set_id,item.producer_run_id,item.source_event_key,item.signal_ingest_seq,(at-6)*1000000,item.mint,
        a4.IntentRole.EXIT,a4.IntentSide.SELL,'MEME_BASE_UNITS',quantity,action.position_id,a4._entry_intent(context).intent_id,
        action.external_decision_ref,action.selected_exit_track,handoff.binding_id)
    return action,intent


def original_chain(fixture,prep,at,signature):
    lower=prep.finalized_lower_anchor
    value=chain_fixture.scenario();value.signature=signature
    tx_slot,root_slot=lower.slot+8,lower.slot+14
    value.initial_slot=value.root_slot=value.status_context=root_slot
    value.status.update(slot=tx_slot)
    value.blocks={lower.slot:{'blockhash':lower.blockhash,'previousBlockhash':lower.previous_blockhash,'parentSlot':lower.parent_slot,
        'blockHeight':lower.block_height,'blockTime':lower.block_time,'signatures':[]},
        lower.slot+4:chain_fixture.block(lower.slot+4,lower.slot,lower.block_height+1,time=at+1),
        tx_slot:chain_fixture.block(tx_slot,lower.slot+4,lower.block_height+2,[signature],time=at+3),
        lower.slot+10:chain_fixture.block(lower.slot+10,tx_slot,lower.block_height+3,time=at+4),
        root_slot:chain_fixture.block(root_slot,lower.slot+10,lower.block_height+4,time=at+5)}
    value.blocks[lower.slot+4]['previousBlockhash']=lower.blockhash
    value.tx=copy.deepcopy(fixture.scenario.tx)
    value.tx.update(slot=tx_slot,blockTime=at+3)
    fixture.scenario=value
    return chain_fixture.transaction_observation(value,at=at+6,
        request=chain_fixture.TransactionRequest(a3.GENESIS,signature,lower.slot))


def execute_public(f,action,accounts,lower,*,route,at,number,intent=None):
    label=f.name+'-'+str(number)
    # The venue migration itself is an explicit later public read fact.
    originals=dict(accounts)
    if route=='swap':
        key=a4.venue.derive_bonding_curve_pda(action.mint)
        originals[key]=PublicAccount(RpcAccountV01(key,sf.PUMP_PROGRAM_ID,sf.plans.curve_bytes(complete=True)),
            1000000000 if originals.get(key) is None else originals[key].lamports,False,0)
    scenario=wallet_scenario(originals,lower)
    value,plan=a4.evidence(f,action,scenario,route=route,at=at,context_slot=lower.slot,intent=intent,wallet_floor=lower.slot,
        recent_blockhash=sf.bh(number),last_valid_height=lower.block_height+20,validity_height=lower.block_height+1,
        original_accounts=originals)
    context_only=a4.validate_message_evidence(value)
    if context_only.disposition!='SUPPORTED_CONTEXT_ONLY':print(label,'CONTEXT',context_only.reasons)
    check(label+'_real_A4a_exact_context',context_only.disposition=='SUPPORTED_CONTEXT_ONLY')
    if f.repo.authority_message_profile(action.policy_digest) is None:
        f.repo.record_authority_message_profile(MessageProfileCommand('profile','INSTALL_AND_SELECT',value.profile,value.profile.approval),fence=f.repo.write_fence())
    prep=control.prepare(f,action,value,plan,at=at-1,lower=lower)
    original_intent=canonical_json(asdict(value.evidence.intent))
    sign=control.consume(f,value,control.request(f,prep,label+'-SIGN'),at=at)
    if type(sign) is not FreshStageConsumption:print(label,'SIGN',sign.decision.reasons)
    check(label+'_true_current_SIGN',type(sign) is FreshStageConsumption)
    wire=b'\x01'+bytes([number])*64+bytes.fromhex(prep.message_hex)
    signature=chain_fixture.sig(number)
    for target in ('EXACT_SIMULATED','AUTHORIZED'):stage(f,prep,target,at+1)
    stage(f,prep,'SIGNED_DURABLE',at+1,wire)
    send=control.consume(f,value,control.request(f,prep,label+'-SEND','SEND'),at=at+2)
    if type(send) is not FreshStageConsumption:print(label,'SEND',send.decision.reasons)
    check(label+'_true_current_SEND_with_real_SIGN_predecessor',type(send) is FreshStageConsumption
        and send.receipt.decision.prior_sign_digest==sign.receipt.content_digest)
    stage(f,prep,'SEND_CLAIMED',at+2)
    check(label+'_same_original_action_root_amount_policy_grant',send.receipt.original.validation.context.action==action
        and send.receipt.original.validation.context.acceptance==f.repo.authority_acceptance(action.root_id)
        and prep.action_content_digest==action.content_digest)
    check(label+'_same_exact_message_lease_and_public_signature',send.receipt.original.request.primary_signature==signature
        and prep.lease==value.evidence.simulation.leases[-1] and prep.message_hex==value.evidence.simulation.envelopes[-1].message_hex
        and bytes.fromhex(prep.message_hex)==base64.b64decode(value.evidence.fee.request_message_base64))
    before=f.repo.write_fence()
    retry=f.repo.consume_authority_message_stage(send.receipt.original.request,send.receipt.original.validation.clock,
        f.source if action.side=='BUY' else None,send.receipt.original.validation.evidence,fence=before)
    check(label+'_lookup_and_retry_are_historical_not_fresh',type(retry) is MessageStageReceipt and retry==send.receipt
        and f.repo.authority_message_receipt(label+'-SEND')==send.receipt and f.repo.write_fence()==before)
    inputs=value.evidence.setup_accounts
    pre=dict(zip(inputs.requested_keys,inputs.accounts))
    actual=sf.Fixture(route,action.side,token_program=action.token_program,external_plan=plan,external_message_hex=prep.message_hex,
        external_wire=wire,external_pre_accounts=pre)
    actual.mint=action.mint
    check(label+'_fixture_indices_and_recipients_from_supplied_message',actual.message_bytes==bytes.fromhex(prep.message_hex)
        and actual.wire==wire and actual.plan is plan and actual.plan_tuple is None
        and actual.units==action.input_units and tuple(pre)==actual.keys)
    check(label+'_prior_public_account_facts_carried_exactly',all(pre[key]==accounts[key] for key in pre if key in accounts
        and key!=a4.venue.derive_bonding_curve_pda(action.mint)))
    check(label+'_pre_metadata_equals_original_public_reads',all(actual.scenario.tx['meta']['preBalances'][i]==(0 if pre[key] is None else pre[key].lamports)
        for i,key in enumerate(actual.keys)))
    obs=original_chain(actual,prep,at,signature)
    check(label+'_actual_adapter_same_full_wire_message_signature',obs.transaction is not None and obs.transaction.wire_bytes==wire
        and obs.transaction.message_bytes==bytes.fromhex(prep.message_hex) and obs.transaction.primary_signature==signature)
    chain=f.repo.ingest_chain_observation(prep.attempt_id,obs,ingestion_key=label+'-chain',evaluated_at_utc=a3.utc(at+6),fence=f.repo.write_fence())
    check(label+'_Ledger_finality_from_consumed_wire',chain.decision.resulting_state.disposition=='FINALIZED_SUCCESS_UNAPPLIED'
        and f.repo.attempt(prep.attempt_id).lane_held)
    after=post_accounts({**originals,**pre},actual)
    # Wallet account support is a later finalized original read, never fill truth.
    root=obs.root
    anchor=replace(root,slot=root.slot+2,blockhash=sf.bh(root.slot+2),previous_blockhash=root.blockhash,
        parent_slot=root.slot,block_height=root.block_height+1,block_time=at+7)
    expected={item.pubkey:(item.mint,item.program) for item in f.repo.consumer_snapshot()['accounts']}
    expected.update({actual.base:(action.mint,action.token_program),actual.quote:(sf.WSOL_MINT,sf.TOKEN_PROGRAM_ID)})
    request=a3.WalletEvidenceRequest(f.domain.wallet,f.domain.genesis_hash,obs.transaction.slot,
        tuple(a3.ExpectedTokenAccount(key,mint,pid) for key,(mint,pid) in sorted(expected.items())))
    support=a3.WalletSupportInput(a3.observe(wallet_scenario(after,anchor),request=request,at=at+8),a3.utc(at+8),request.min_context_slot)
    proposal=sf.attribute_settlement(f.domain,action,f.repo.attempt(prep.attempt_id),chain,support)
    if proposal.proposal is None:print(label,'ATTRIBUTION',proposal.disposition,proposal.reasons)
    check(label+'_whole_actual_attribution',proposal.proposal is not None)
    check(label+'_actual_metadata_fee_not_priority_price',actual.scenario.tx['meta']['fee']==7777
        and actual.message.instructions[1].data==b'\x03'+struct.pack('<Q',1000))
    check(label+'_no_mutation_of_original_LIVE_intent',canonical_json(asdict(value.evidence.intent))==original_intent)
    return actual,prep,chain,support,after,anchor,proposal.proposal,sign.receipt,send.receipt


def cycle(directory,name,*,gap=False):
    f,unused,initial,buy=a4.fixture(directory,name)
    accepted=f.repo.authority_acceptance(buy.root_id);grant=accepted.eligibility.decision.grant_id
    original_action=buy.content_digest
    entry_number=91 if gap else 81
    initial_grant=f.repo.authority_grant(grant)
    shadow_intent_before=canonical_json(asdict(unused.plan_tuple[0]))
    public={f.domain.wallet:PublicAccount(RpcAccountV01(f.domain.wallet,sf.SYSTEM_PROGRAM_ID,b''),sf.FUNDING,False,0),
        sf.MINT:PublicAccount(sf.plans.fx.mint_account(),1000000000,False,0),
        sf.WSOL_MINT:PublicAccount(RpcAccountV01(sf.WSOL_MINT,sf.TOKEN_PROGRAM_ID,a3.mint_data(9)),1000000000,False,0)}
    lower=a3.observe().anchor
    acquired,prep,chain,support,public,lower,proposal,sign,send=execute_public(f,buy,public,lower,route='pump',at=NOW+9,number=entry_number)
    handoff=a3.ports.handoff(f.repo,buy,prep,chain,support,due=True)
    group=f.repo.apply_settlement_with_ports(prep.attempt_id,chain_receipt_key=name+'-'+str(entry_number)+'-chain',support=support,
        ingestion_key='acquire-protect',recorded_at_utc=a3.utc(NOW+20),handoff=handoff,fence=f.repo.write_fence())
    check(name+'_acquisition_and_due_protection_same_real_commit',group.application_disposition=='FINALIZED_SUCCESS_APPLIED'
        and f.repo.protection(buy.position_id).obligation_state=='DUE'
        and f.repo.protection(buy.position_id).sequence==group.sequence and f.repo.position_history(buy.position_id).remaining_units==acquired.actual_base)
    check(name+'_actual_output_not_quote_or_authorized_native',acquired.actual_base!=buy.input_units
        and acquired.actual_base!=acquired.minimum and proposal.base_units_delta==acquired.actual_base)
    check(name+'_single_actual_fee_and_native_projection',f.repo.consumer_snapshot()['funding'].native_lamports==public[sf.WALLET].lamports
        and f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports==7777)
    frozen=f.repo.write_fence()
    duplicate=f.repo.apply_settlement_with_ports(prep.attempt_id,chain_receipt_key=name+'-'+str(entry_number)+'-chain',support=support,
        ingestion_key='acquire-protect',recorded_at_utc=a3.utc(NOW+20),handoff=handoff,fence=frozen)
    check(name+'_acquisition_retry_never_second_applies',duplicate==group and f.repo.write_fence()==frozen)
    f.reopen()
    check(name+'_reopen_does_not_recreate_consumption_delivery',f.repo.authority_message_receipt(name+'-'+str(entry_number)+'-SEND')==send
        and type(f.repo.authority_message_receipt(name+'-'+str(entry_number)+'-SEND')) is MessageStageReceipt)
    if gap:
        a3.later_candidate(f,a3.cf.key(23),'gap-entry',NOW+21)
        a3.a1.source_fixture.add_gap(f.raw,'DONE')
        previous=f.source.latest_record()[1]
        record=a3.a1.source_fixture.observe(f.raw,f.source.binding,previous=previous,now=34,cut=32)
        f.source.append(record,expected_previous_digest=previous.content_digest)
        request=replace(support.observation.request,min_context_slot=lower.slot,
            expected_accounts=(*support.observation.request.expected_accounts,a3.ExpectedTokenAccount(a3.derive_associated_token_address(sf.WALLET,f.item.mint,sf.TOKEN_PROGRAM_ID),f.item.mint,sf.TOKEN_PROGRAM_ID)))
        public[f.item.mint]=PublicAccount(RpcAccountV01(f.item.mint,sf.TOKEN_PROGRAM_ID,a3.mint_data(6)),1000000000,False,0)
        current=a3.WalletSupportInput(a3.observe(wallet_scenario(public,lower),request=request,at=NOW+27),a3.utc(NOW+27),lower.slot)
        denied=a3.admit(f,'gap-entry',at=NOW+27,support=current)
        check(name+'_actual_recorded_gap_ENTRY_denial',not denied.accepted and record.disposition=='GAP'
            and 'CURRENT_SOURCE_NOT_USABLE' in denied.eligibility.decision.reasons and f.repo.protection(buy.position_id).obligation_state=='DUE')
    records=[send.content_digest];messages=[prep.message_sha256];signatures=[send.original.request.primary_signature]
    for ordinal,at,number in ((1,NOW+29,entry_number+1),(2,NOW+49,entry_number+2)):
        before_units=acquired.actual_base if ordinal==1 else acquired.actual_base-partial
        quantity=acquired.actual_base//3 if ordinal==1 else before_units
        if ordinal==1:partial=quantity
        sell,intent=exit_action(f,buy,handoff,quantity,ordinal,at)
        actual,p,c,s,public,lower,proposed,sign,send=execute_public(f,sell,public,lower,route='swap',at=at,number=number,intent=intent)
        check(name+str(ordinal)+'_pending_reduction_holds_capacity',f.repo.reservation(buy.root_id).status=='RESERVED'
            and f.repo.attempt(p.attempt_id).lane_held and f.repo.position_history(buy.position_id).remaining_units==before_units)
        if ordinal==1:
            applied=f.repo.apply_settlement(p.attempt_id,chain_receipt_key=name+'-'+str(number)+'-chain',support=s,
                ingestion_key='partial',recorded_at_utc=a3.utc(at+8),fence=f.repo.write_fence())
            check(name+'_partial_actual_residual_keeps_due_obligation',applied.decision.disposition=='FINALIZED_SUCCESS_APPLIED'
                and f.repo.position_history(buy.position_id).remaining_units==before_units-quantity
                and f.repo.protection(buy.position_id).obligation_state=='DUE' and f.repo.reservation(buy.root_id).status=='RESERVED')
        else:
            retirement,comparison=a3.ports.retirement(f.repo,sell,p,s,at=at+8)
            closed=f.repo.apply_settlement_with_ports(p.attempt_id,chain_receipt_key=name+'-'+str(number)+'-chain',support=s,
                ingestion_key='full-retire',recorded_at_utc=a3.utc(at+8),retirement=retirement,retirement_support=comparison,fence=f.repo.write_fence())
            if closed.retirement_disposition!='RETIRED':print('RETIRE',closed)
            check(name+'_full_actual_settlement_and_retirement_atomic',closed.application_disposition=='FINALIZED_SUCCESS_APPLIED'
                and closed.retirement_disposition=='RETIRED' and f.repo.position_history(buy.position_id).remaining_units==0
                and not f.repo.consumer_snapshot()['reservations'])
        check(name+str(ordinal)+'_native_accounting_matches_prior_public_chain',f.repo.consumer_snapshot()['funding'].native_lamports==public[sf.WALLET].lamports
            and f.repo.consumer_snapshot()['funding'].network_fees_paid_lamports==(ordinal+1)*7777)
        check(name+str(ordinal)+'_original_root_amount_grant_and_protection_retained',f.repo.action(buy.action_id).content_digest==original_action
            and f.repo.authority_acceptance(buy.root_id)==accepted and f.repo.protection(buy.position_id).handoff==handoff)
        records.append(send.content_digest);messages.append(p.message_sha256);signatures.append(send.original.request.primary_signature)
        f.reopen();check(name+str(ordinal)+'_historical_stage_replay',f.repo.authority_message_receipt(name+'-'+str(number)+'-SEND')==send)
        if ordinal==1 and not gap:
            a3.later_candidate(f,a3.cf.key(25),'occupied-after-partial',NOW+38)
            public[f.item.mint]=PublicAccount(RpcAccountV01(f.item.mint,sf.TOKEN_PROGRAM_ID,a3.mint_data(6)),1000000000,False,0)
            expected={item.pubkey:(item.mint,item.program) for item in f.repo.consumer_snapshot()['accounts']}
            expected[a3.derive_associated_token_address(sf.WALLET,f.item.mint,sf.TOKEN_PROGRAM_ID)]=(f.item.mint,sf.TOKEN_PROGRAM_ID)
            request=a3.WalletEvidenceRequest(sf.WALLET,a3.GENESIS,lower.slot,
                tuple(a3.ExpectedTokenAccount(key,mint,pid) for key,(mint,pid) in sorted(expected.items())))
            current=a3.WalletSupportInput(a3.observe(wallet_scenario(public,lower),request=request,at=NOW+42),a3.utc(NOW+42),lower.slot)
            occupied=a3.admit(f,'occupied-after-partial',at=NOW+42,support=current)
            check(name+'_healthy_current_ENTRY_still_denied_before_retirement',not occupied.accepted
                and occupied.eligibility.decision.disposition=='ELIGIBLE_CONTEXT_ONLY'
                and 'V1_OCCUPIED_POSITION_OR_UNRESOLVED_ADMISSION' in occupied.risk.reasons)
            check(name+'_partial_exit_preserves_reserved_future_cost',occupied.risk.root_encumbrances[0].protective_lamports==
                f.policy.costs.protective_network_fee_lamports+f.policy.costs.protective_setup_lamports+f.policy.costs.protective_refundable_lock_lamports)
    check(name+'_distinct_exact_messages_and_inert_signatures',len(set(messages))==len(set(signatures))==3)
    check(name+'_original_Shadow_intent_unchanged',canonical_json(asdict(unused.plan_tuple[0]))==shadow_intent_before)
    check(name+'_no_rearming_or_scope_transfer',f.repo.authority_grant(grant)==initial_grant
        and f.repo.authority_snapshot()['armed_entry_grant']==initial_grant)
    a3.later_candidate(f,a3.cf.key(24),'next-after-retirement',NOW+60)
    public[f.item.mint]=PublicAccount(RpcAccountV01(f.item.mint,sf.TOKEN_PROGRAM_ID,a3.mint_data(6)),1000000000,False,0)
    expected={item.pubkey:(item.mint,item.program) for item in f.repo.consumer_snapshot()['accounts']}
    expected.update({a3.derive_associated_token_address(sf.WALLET,f.item.mint,sf.TOKEN_PROGRAM_ID):(f.item.mint,sf.TOKEN_PROGRAM_ID)})
    req=a3.WalletEvidenceRequest(sf.WALLET,a3.GENESIS,lower.slot,tuple(a3.ExpectedTokenAccount(key,mint,pid) for key,(mint,pid) in sorted(expected.items())))
    current=a3.WalletSupportInput(a3.observe(wallet_scenario(public,lower),request=req,at=NOW+64),a3.utc(NOW+64),lower.slot)
    next_receipt=a3.admit(f,'next-after-retirement',at=NOW+64,support=current)
    check(name+'_recorded_gap_never_silently_repaired' if gap else name+'_same_recurring_grant_next_distinct_mint',
        (not next_receipt.accepted and 'CURRENT_SOURCE_NOT_USABLE' in next_receipt.eligibility.decision.reasons) if gap else
        (next_receipt.accepted and next_receipt.eligibility.decision.grant_id==grant and f.item.mint!=buy.mint
         and f.item.winner_binding_digest!=f.repo.candidate(buy.root_id).winner_binding_digest))
    check(name+'_retired_history_and_paid_costs_survive_next_decision',f.repo.authority_acceptance(buy.root_id)==accepted
        and next_receipt.risk.historical_network_paid_lamports==3*7777 and next_receipt.risk.outstanding_native_lamports==0)
    f.reopen();check(name+'_complete_original_journal_rebuild',f.repo.authority_admission_receipt('next-after-retirement')==next_receipt)
    result={'stages':records,'message_digests':messages,'public_signatures':signatures,'custody':f.repo.audit()['custody_digest'],'next_admission':next_receipt.content_digest,
        'acquired_units':acquired.actual_base,'partial_sold_units':partial,'final_native_lamports':public[sf.WALLET].lamports}
    f.close();return result


def main():
    original=shadow_repository.ShadowRepositoryV01.__init__
    def forbidden(*args,**kwargs):raise AssertionError('composition may not access Shadow economic repository')
    files=(Path(a4.plans.__file__),Path(a4.venue.__file__),Path(shadow_repository.__file__))
    hashes={str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    shadow_repository.ShadowRepositoryV01.__init__=forbidden
    try:
        with tempfile.TemporaryDirectory(prefix='authority-composition-') as tmp:
            try:results={name:cycle(Path(tmp),name,gap=gap) for name,gap in (('healthy',False),('recorded-gap',True))}
            finally:
                for f in a3.a1.FIXTURES:f.close()
        check('Phase5_pure_reuse_source_files_unchanged',hashes=={str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in files})
        check('all_temporary_fixture_artifacts_removed',not Path(tmp).exists())
    finally:shadow_repository.ShadowRepositoryV01.__init__=original
    print(canonical_json({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(CHECKS),'check_digest':content_fingerprint(CHECKS),
        'all_true':all(CHECKS.values()),'results':results,'real_network_or_signing_or_send_operations':0}))


if __name__=='__main__':main()
