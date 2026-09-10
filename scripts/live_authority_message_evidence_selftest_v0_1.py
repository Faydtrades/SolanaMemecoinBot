"""A4a deterministic external-producer fixtures; no signer/provider/runtime.

Every root is actually admitted through Source/Wallet adapters and Ledger A3.
Only this fixture boundary constructs public messages with zero placeholders.
"""
from __future__ import annotations
import base64
import copy
import hashlib
import json
import struct
import sys
import tempfile
import sqlite3
from contextlib import ExitStack, closing
from dataclasses import asdict, replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"src"))
import live_authority_admission_selftest_v0_1 as a3
import live_ledger_settlement_selftest_v0_1 as sf
from solders.instruction import Instruction, AccountMeta
from solders.message import Message, to_bytes_versioned
from solders.pubkey import Pubkey
from solders.hash import Hash
from solders.compute_budget import ID as COMPUTE_BUDGET_ID
from live.authority_message_evidence_v0_1 import (
    MessageValidationProfile, PublicReadCut, OriginalAccountBatch, OriginalVenueRead,
    FeeForMessageRead, SimulationProvenance, ExternalMessageEvidence, MessageValidationInput,
    capture_message_context, validate_message_evidence, _entry_intent, _validate,
)
from live.authority_message_codec_v0_1 import encode_validation_input, decode_validation_input, simulation_run_digest
from live.public_rpc_v0_1 import PublicAccount
from live.ledger_domain_v0_1 import LedgerContractError
from live.ledger_settlement_v0_1 import PUMP_BUY_ACCOUNTS, PUMP_SELL_ACCOUNTS, PUMPSWAP_BUY_ACCOUNTS, PUMPSWAP_SELL_ACCOUNTS
from phase5 import shadow_unsigned_plan_simulation_v0_1 as plans
from phase5 import shadow_venue_route_quote_v0_1 as venue
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint, IntentSide, IntentRole, ExecutionIntentV01

NOW, CHECKS = a3.NOW, {}
R = sf.R


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def raises(call):
    try:
        call()
    except (ValueError, TypeError):
        return True
    return False


def original_public(raw, lamports=1000000000):
    return PublicAccount(raw, lamports, False, 0)


def fixture(directory, name, route="pump", program=venue.TOKEN_PROGRAM_ID, allowed_venues=("PUMP","PUMPSWAP")):
    f, settlement, scenario = a3.actual_fixture(directory, name, venue=route, program=program)
    # Explicit synthetic policy values, installed BEFORE the real A3 admission.
    a3.install(f, name+"-costs", allowed_venues=allowed_venues, costs={"setup_outflow_lamports":10000000,"refundable_account_lock_lamports":10000000,
        "protective_setup_lamports":10000000,"protective_refundable_lock_lamports":10000000})
    scenario.accounts[sf.MINT] = a3.account(program, sf.plans.fx.mint_account(program).data)
    admission = a3.admit(f, name+"-admit", venue="PUMP" if route=="pump" else "PUMPSWAP",
                         support=a3.wallet(f, scenario=scenario, program=program), program=program)
    check(name+"_actual_A3_admission", admission.accepted)
    action = f.repo.reservation(f.root).admission.action
    return f, settlement, scenario, action


def evidence(f, action, scenario, *, route="pump", at=NOW+9, context_slot=101, intent=None):
    context = capture_message_context(f.repo, action.action_id)
    intent = intent or _entry_intent(context)
    program = action.token_program
    fx, raw = sf.plans.fx, {}
    def remember(value):
        raw[value.pubkey] = value
        return value
    curve = remember(fx.account(venue.derive_bonding_curve_pda(action.mint), venue.PUMP_PROGRAM_ID,
                                sf.plans.curve_bytes(complete=route=="swap")))
    mint = remember(fx.mint_account(program))
    pump_global = remember(fx.account(venue.derive_pump_global_pda(),venue.PUMP_PROGRAM_ID,sf.plans.pump_global_full()))
    pump_fee = remember(fx.fee_account(venue.PUMP_PROGRAM_ID))
    curve_state = venue.build_pump_state(intent,curve,mint,pump_global,pump_fee,slot_min=context_slot,
                                         slot_max=context_slot,observed_at_us=(at-5)*1000000)
    pool, pool_state = None,None
    if route=="swap":
        pool = remember(fx.account(venue.derive_pumpswap_pool_pda(action.mint),venue.PUMPSWAP_PROGRAM_ID,sf.plans.pool_bytes()))
        base_vault = remember(fx.token_account(fx.BASE_VAULT,action.mint,500000000000,program))
        quote_vault = remember(fx.account(fx.QUOTE_VAULT,venue.TOKEN_PROGRAM_ID,
            a3.token_data(venue.WSOL_MINT,pool.pubkey,20000000000,reserve=R)))
        global_ = remember(fx.account(venue.derive_pumpswap_global_pda(),venue.PUMPSWAP_PROGRAM_ID,sf.plans.pumpswap_global_full()))
        fee_ = remember(fx.fee_account(venue.PUMPSWAP_PROGRAM_ID))
        pool_state = venue.build_pumpswap_state(intent,pool,base_vault,quote_vault,mint,global_,fee_,slot_min=context_slot,
            slot_max=context_slot,observed_at_us=(at-5)*1000000)
    route_decision = venue.decide_route(intent,curve_state,pool_state)
    state = curve_state if route=="pump" else pool_state
    qp = venue.QuotePolicyV01(100)
    quote = venue.create_executable_quote(intent,state,route_decision,qp)
    actor = plans.PublicShadowActorV01(f.domain.wallet)
    support = a3.wallet(f,scenario=scenario,at=at-4,program=program)
    explicit = dict(zip(support.observation.explicit_read.requested_keys,support.observation.explicit_read.accounts))
    base = plans.derive_associated_token_address(f.domain.wallet,action.mint,program)
    wsol = plans.derive_associated_token_address(f.domain.wallet,venue.WSOL_MINT,venue.TOKEN_PROGRAM_ID)
    account_snapshot = plans.build_actor_account_snapshot(state,actor,base_account=None if explicit[base] is None else explicit[base].account,
        quote_account=None if explicit[wsol] is None else explicit[wsol].account,
        context_slot=context_slot,observed_at_us=(at-4)*1000000)
    recipients = plans.decode_verified_recipient_evidence(state,pump_global if route=="pump" else global_)
    pp = plans.TransactionPlanPolicyV01("LEGACY","SDK_COMPAT_IDEMPOTENT_V01","HASH_BOUND_INDEX_V01",True,"confirmed")
    plan = plans.build_unsigned_transaction_plan(intent,state,route_decision,quote,actor,pp,recipients,account_snapshot)
    primary_keys = (curve.pubkey,venue.derive_pumpswap_pool_pda(action.mint))
    cut = PublicReadCut(f.domain.genesis_hash,f.domain.expected_profile_fingerprint,context_slot,(at-5)*1000000,"confirmed")
    primary = OriginalAccountBatch(cut,primary_keys,(original_public(curve),None if pool is None else original_public(pool)))
    dependencies = tuple(sorted(set(raw)-set(primary_keys)))
    dep = OriginalAccountBatch(cut,dependencies,tuple(original_public(raw[key],20000000000+R if key==fx.QUOTE_VAULT else 1000000000) for key in dependencies))
    reads = OriginalVenueRead(primary,dep,primary)
    # External fixture serialization only. No private keys or real signatures.
    instructions = [Instruction(COMPUTE_BUDGET_ID,b"\x02"+struct.pack("<I",250000),[]),
        Instruction(COMPUTE_BUDGET_ID,b"\x03"+struct.pack("<Q",1000),[])]+[ix.materialize() for ix in plan.instructions]
    lease = plans.BlockhashLeaseV01(plan.plan_id,sf.RECENT,context_slot,200,"confirmed",(at-3)*1000000)
    message = Message.new_with_blockhash(instructions,Pubkey.from_string(f.domain.wallet),Hash.from_string(lease.blockhash))
    message_bytes = to_bytes_versioned(message)
    keys = tuple(map(str,message.account_keys)); indexes = {key:i for i,key in enumerate(keys)}
    config = plans.SimulationRequestConfigV01(context_slot)
    wire = b"\x01"+bytes(64)+message_bytes
    envelope = plans.SimulationEnvelopeV01(plan.plan_id,plan.fingerprint,lease.lease_id,lease.fingerprint,config.fingerprint,
        canonical_json(config.payload()),message_bytes.hex(),base64.b64encode(wire).decode(),1,1,
        hashlib.sha256(message_bytes).hexdigest(),hashlib.sha256(wire).hexdigest())
    validity = plans.BlockhashValidityEvidenceV01(lease.lease_id,100,context_slot,True,context_slot,(at-2)*1000000)
    attempt = plans.SimulationAttemptV01(plan.plan_id,lease.lease_id,envelope.envelope_id,config.fingerprint,validity.fingerprint,0,(at-2)*1000000)
    def inner(pid, accounts, data, stack=2):
        return {"programIdIndex":indexes[pid],"accounts":[indexes[key] for key in accounts],"data":sf.b58(data),"stackHeight":stack}
    def ata_rows(key,owner,mint,pid,stack=2):
        return [inner(pid,(mint,),b"\x15\x07\x00",stack),
            inner(plans.SYSTEM_PROGRAM_ID,(f.domain.wallet,key),struct.pack("<IQQ",0,R,165)+bytes(Pubkey.from_string(pid)),stack),
            inner(pid,(key,),b"\x16",stack),inner(pid,(key,mint),b"\x12"+bytes(Pubkey.from_string(owner)),stack)]
    pre = {key:original_public(raw.get(key,fx.account(key,plans.SYSTEM_PROGRAM_ID,b""))) for key in keys}
    pre.update((key,explicit[key]) for key in (f.domain.wallet,base,wsol))
    groups=[]
    selected_program = venue.PUMP_PROGRAM_ID if route=="pump" else venue.PUMPSWAP_PROGRAM_ID
    for oi,ix in enumerate(instructions):
        pid, accounts = str(ix.program_id),tuple(str(meta.pubkey) for meta in ix.accounts)
        if pid==plans.ASSOCIATED_TOKEN_PROGRAM_ID:
            groups.append({"index":oi,"instructions":[] if pre[accounts[1]] is not None else ata_rows(accounts[1],accounts[2],accounts[3],accounts[5])})
        elif pid==selected_program:
            schema = (PUMP_BUY_ACCOUNTS if action.side=="BUY" else PUMP_SELL_ACCOUNTS) if route=="pump" else (
                PUMPSWAP_BUY_ACCOUNTS if action.side=="BUY" else PUMPSWAP_SELL_ACCOUNTS)
            roles={name:key for (name,_,_),key in zip(schema,accounts)}
            pool_key=roles["bonding_curve"] if route=="pump" else roles["pool"]
            pool_base=roles["associated_base_bonding_curve"] if route=="pump" else roles["pool_base_token_account"]
            pre[pool_base]=original_public(fx.account(pool_base,program,a3.token_data(action.mint,pool_key,500000000000)),R)
            if route=="pump":
                for name in ("associated_quote_bonding_curve","associated_quote_fee_recipient","associated_quote_buyback_fee_recipient",
                             "associated_creator_vault","associated_user_volume_accumulator"):
                    if name in roles:
                        pre[roles[name]]=None
            else:
                pre[roles["pool_quote_token_account"]]=original_public(quote_vault,20000000000+R)
                for key,owner in ((roles["protocol_fee_recipient_token_account"],roles["protocol_fee_recipient"]),
                                  (roles["coin_creator_vault_ata"],roles["coin_creator_vault_authority"]),(accounts[-1],accounts[-2])):
                    pre[key]=original_public(fx.account(key,venue.TOKEN_PROGRAM_ID,a3.token_data(venue.WSOL_MINT,owner,0,reserve=R)),R)
            rows=[]
            if action.side=="BUY":
                volume=roles["user_volume_accumulator"];pre[volume]=None
                rows.append(inner(plans.SYSTEM_PROGRAM_ID,(f.domain.wallet,volume),struct.pack("<IQQ",0,1844400,137 if route=="pump" else 256)+bytes(Pubkey.from_string(pid))))
            if route=="swap" and action.side=="BUY":
                for key,owner in ((roles["protocol_fee_recipient_token_account"],roles["protocol_fee_recipient"]),
                                  (roles["coin_creator_vault_ata"],roles["coin_creator_vault_authority"])):
                    pre[key]=None
                    rows.append(inner(plans.ASSOCIATED_TOKEN_PROGRAM_ID,(f.domain.wallet,key,owner,venue.WSOL_MINT,plans.SYSTEM_PROGRAM_ID,venue.TOKEN_PROGRAM_ID),b"\x01"))
                    rows.extend(ata_rows(key,owner,venue.WSOL_MINT,venue.TOKEN_PROGRAM_ID,3))
            source,target,owner = (pool_base,base,pool_key) if action.side=="BUY" else (base,pool_base,f.domain.wallet)
            units = quote.minimum_output+17 if action.side=="BUY" else action.input_units
            rows.append(inner(program,(source,action.mint,target,owner),b"\x0c"+struct.pack("<QB",units,6)))
            if action.side=="BUY":
                if route=="pump":
                    rows.append(inner(plans.SYSTEM_PROGRAM_ID,(f.domain.wallet,pool_key),struct.pack("<IQ",2,95000000)))
                    rows.append(inner(plans.SYSTEM_PROGRAM_ID,(f.domain.wallet,roles["fee_recipient"]),struct.pack("<IQ",2,1000000)))
                else:
                    rows.append(inner(venue.TOKEN_PROGRAM_ID,(wsol,venue.WSOL_MINT,roles["pool_quote_token_account"],f.domain.wallet),
                        b"\x0c"+struct.pack("<QB",96000000,9)))
            else:
                if route=="swap":
                    rows.append(inner(venue.TOKEN_PROGRAM_ID,(roles["pool_quote_token_account"],venue.WSOL_MINT,wsol,pool_key),
                        b"\x0c"+struct.pack("<QB",quote.minimum_output+17,9)))
            rows.append(inner(pid,(roles["event_authority"],),sf.ANCHOR_EVENT_TAG+bytes(16)))
            groups.append({"index":oi,"instructions":rows})
    response=plans.SimulationRpcResponseV01(context_slot,{"err":None,"logs":[],"unitsConsumed":200000,
        "loadedAccountsDataSize":30000,"innerInstructions":groups,"returnData":None,"accounts":None})
    result=plans.classify_simulation_response(plan,attempt,lease,envelope,config,response,observed_at_us=(at-1)*1000000)
    run=plans.SimulationRunEvidenceV01((lease,),(validity,),(envelope,),(attempt,),(result,))
    provenance=SimulationProvenance(f.domain.genesis_hash,f.domain.expected_profile_fingerprint,"FIXTURE_EXECUTION_PUBLIC_READ",
        content_fingerprint({"run":simulation_run_digest(run)}),simulation_run_digest(run),(at-3)*1000000,(at-1)*1000000)
    fee_cut=replace(cut,observed_at_us=(at-1)*1000000)
    fee=FeeForMessageRead(fee_cut,base64.b64encode(message_bytes).decode(),context_slot,"OBSERVED",7777)
    setup=OriginalAccountBatch(replace(cut,observed_at_us=(at-3)*1000000),keys,tuple(pre[key] for key in keys))
    external=ExternalMessageEvidence(intent,reads,support,qp,pp,*(canonical_json(v.payload()) for v in (state,route_decision,quote,plan)),run,provenance,fee,setup)
    profile=MessageValidationProfile("SYNTHETIC_MESSAGE_PROFILE",f.domain.economic_domain_id,context.original_policy.content_digest,
        f.domain.expected_profile_fingerprint,a3.operator(),*(20000000 for _ in range(7)),100,250000,1000)
    return MessageValidationInput(context,profile,a3.clock(f.repo,at),external),plan


def with_run(value, run):
    return replace(value,evidence=replace(value.evidence,simulation=run,
        simulation_provenance=replace(value.evidence.simulation_provenance,run_digest=simulation_run_digest(run))))


def with_result(value, **fields):
    run=value.evidence.simulation
    return with_run(value,replace(run,results=(*run.results[:-1],replace(run.results[-1],**fields))))


def with_trace(value, transform):
    groups=json.loads(value.evidence.simulation.results[-1].inner_instructions_json)
    transform(groups)
    return with_result(value,inner_instructions_json=canonical_json(groups))


def with_pre(value,key,transform):
    setup=value.evidence.setup_accounts
    accounts=list(setup.accounts);index=setup.requested_keys.index(key)
    accounts[index]=transform(accounts[index])
    return replace(value,evidence=replace(value.evidence,setup_accounts=replace(setup,accounts=tuple(accounts))))


def with_message(value,message):
    raw=to_bytes_versioned(message);wire=b"\x01"+bytes(64)+raw
    run=value.evidence.simulation;old=run.envelopes[-1]
    envelope=replace(old,message_hex=raw.hex(),message_sha256=hashlib.sha256(raw).hexdigest(),
        transaction_base64=base64.b64encode(wire).decode(),wire_sha256=hashlib.sha256(wire).hexdigest())
    attempt=replace(run.attempts[-1],envelope_id=envelope.envelope_id)
    result=replace(run.results[-1],envelope_id=envelope.envelope_id,attempt_id=attempt.attempt_id)
    value=with_run(value,replace(run,envelopes=(envelope,),attempts=(attempt,),results=(result,)))
    return replace(value,evidence=replace(value.evidence,fee=replace(value.evidence.fee,request_message_base64=base64.b64encode(raw).decode())))


def deny(name,value,unknown=False):
    result=validate_message_evidence(value)
    check(name,result.disposition=="UNKNOWN" if unknown else result.disposition!="SUPPORTED_CONTEXT_ONLY")
    check(name+"_no_cost_or_permission",result.costs is None and not result.grants_message_permission and not result.may_sign and not result.may_send)


def hostile(value,plan,label):
    e=value.evidence
    for name,changed in (
        ("run_wrong_chain",replace(value,evidence=replace(e,simulation_provenance=replace(e.simulation_provenance,genesis_hash=sf.bh(199))))),
        ("run_wrong_profile",replace(value,evidence=replace(e,simulation_provenance=replace(e.simulation_provenance,profile_fingerprint=content_fingerprint("wrong"))))),
        ("run_wrong_original_digest",replace(value,evidence=replace(e,simulation_provenance=replace(e.simulation_provenance,run_digest=content_fingerprint("wrong"))))),
        ("fee_null",replace(value,evidence=replace(e,fee=replace(e.fee,outcome="NULL",fee_lamports=None)))),
        ("fee_missing",replace(value,evidence=replace(e,fee=None))),
        ("fee_old",replace(value,evidence=replace(e,fee=replace(e.fee,cut=replace(e.fee.cut,observed_at_us=(NOW-40)*1000000))))),
        ("fee_context_regressed",replace(value,evidence=replace(e,fee=replace(e.fee,cut=replace(e.fee.cut,context_slot=100))))),
        ("fee_far_context",replace(value,evidence=replace(e,fee=replace(e.fee,cut=replace(e.fee.cut,context_slot=999))))),
        ("fee_less_than_priority",replace(value,evidence=replace(e,fee=replace(e.fee,fee_lamports=249)))),
        ("setup_missing",replace(value,evidence=replace(e,setup_accounts=None))),
        ("setup_old",replace(value,evidence=replace(e,setup_accounts=replace(e.setup_accounts,cut=replace(e.setup_accounts.cut,observed_at_us=(NOW-40)*1000000))))),
        ("result_error_despite_success",with_result(value,error_json=canonical_json({"InstructionError":[0,"InvalidArgument"]}))),
        ("result_missing_metrics",with_result(value,units_consumed=None)),
        ("result_contradictory_loaded_metric",with_result(value,loaded_accounts_data_size=0)),
        ("result_missing_context",with_result(value,outcome=plans.SimulationOutcome.RPC_FAILURE,context_slot=None)),
        ("result_old",with_result(value,observed_at_us=(NOW-40)*1000000)),
        ("trace_null",with_result(value,inner_instructions_json="null")),
        ("trace_empty",with_result(value,inner_instructions_json="[]")),
        ("clock_unknown",replace(value,clock=replace(value.clock,status="UNKNOWN"))),
        ("clock_straddles_original_deadline",replace(value,clock=replace(value.clock,utc_lower_utc=a3.utc(NOW+15),utc_upper_utc=a3.utc(NOW+16)))),
    ):
        deny(label+"_"+name,changed)
    for name,changed in (
        ("fee_above_cap",replace(value,evidence=replace(e,fee=replace(e.fee,fee_lamports=10001)))),
        ("units_above_message_limit",with_result(value,units_consumed=250001)),
        ("profile_wrong_policy",replace(value,profile=replace(value.profile,policy_digest=content_fingerprint("wrong")))),
        ("intent_wrong_amount",replace(value,evidence=replace(e,intent=replace(e.intent,input_amount_base_units=e.intent.input_amount_base_units+1)))),
        ("intent_wrong_source",replace(value,evidence=replace(e,intent=replace(e.intent,source_run_id="unrelated")))),
        ("original_action_wrong_deadline_binding",replace(value,context=replace(value.context,action=replace(value.context.action,deadline_binding_digest=content_fingerprint("wrong"))))),
    ):
        deny(label+"_"+name,changed)
    run=e.simulation
    validity=replace(run.validity[-1],block_height=run.leases[-1].last_valid_block_height+1)
    attempt=replace(run.attempts[-1],validity_fingerprint=validity.fingerprint)
    result=replace(run.results[-1],attempt_id=attempt.attempt_id)
    deny(label+"_original_recent_hash_expired",with_run(value,replace(run,validity=(validity,),attempts=(attempt,),results=(result,))),True)
    forged=replace(plan,instructions=(replace(plan.instructions[0],data_hex="00"),*plan.instructions[1:]))
    check(label+"_P5_replace_retains_factory_token",forged._factory_token is plan._factory_token)
    deny(label+"_forged_factory_plan_denied",replace(value,evidence=replace(e,supplied_plan_json=canonical_json(forged.payload()))))
    raw=json.loads(e.supplied_state_json)
    raw["observed_at_us"]+=1
    deny(label+"_claimed_decoded_state_not_original_bytes",replace(value,evidence=replace(e,supplied_state_json=canonical_json(raw))))
    program=next(ix for ix in plan.instructions if ix.program_id in (venue.PUMP_PROGRAM_ID,venue.PUMPSWAP_PROGRAM_ID))
    original_ix=[Instruction(COMPUTE_BUDGET_ID,b"\x02"+struct.pack("<I",250000),[]),
                 Instruction(COMPUTE_BUDGET_ID,b"\x03"+struct.pack("<Q",1000),[])]+[ix.materialize() for ix in plan.instructions]
    def message(instructions,hash_=sf.RECENT,payer=sf.WALLET):
        return Message.new_with_blockhash(instructions,Pubkey.from_string(payer),Hash.from_string(hash_))
    cases={
        "extra_transfer":original_ix+[Instruction(Pubkey.from_string(plans.SYSTEM_PROGRAM_ID),struct.pack("<IQ",2,1),
            [AccountMeta(Pubkey.from_string(sf.WALLET),True,True),AccountMeta(Pubkey.from_string(sf.plans.fx.pk(189)),False,True)])],
        "duplicate_compute":original_ix[:1]+original_ix,
        "unknown_compute":[Instruction(COMPUTE_BUDGET_ID,b"\x01"+struct.pack("<I",1000),[])]+original_ix[1:],
        "excess_price":[original_ix[0],Instruction(COMPUTE_BUDGET_ID,b"\x03"+struct.pack("<Q",1001),[])]+original_ix[2:],
        "missing_compute":original_ix[1:],
        "reordered_instructions":original_ix[:2]+list(reversed(original_ix[2:])),
    }
    for name,instructions in cases.items():
        deny(label+"_"+name,with_message(value,message(instructions)))
    deny(label+"_wrong_blockhash",with_message(value,message(original_ix,hash_=sf.bh(191))))
    # Alter effective privileges while preserving ordered instruction keys/data.
    ix=original_ix[-1] if str(original_ix[-1].program_id)==program.program_id else next(ix for ix in original_ix if str(ix.program_id)==program.program_id)
    accounts=list(ix.accounts)
    readonly=next(i for i,m in enumerate(accounts) if not m.is_writable and not m.is_signer)
    accounts[readonly]=AccountMeta(accounts[readonly].pubkey,False,True)
    altered=list(original_ix);altered[original_ix.index(ix)]=Instruction(ix.program_id,ix.data,accounts)
    deny(label+"_escalated_effective_writable",with_message(value,message(altered)))
    altered=list(original_ix);altered[original_ix.index(ix)]=Instruction(ix.program_id,ix.data,list(reversed(ix.accounts)))
    deny(label+"_account_order_changed",with_message(value,message(altered)))
    # A known volume role cannot become a zero-cost assumption when child trace
    # is absent even though the RPC says success.
    volume=plans.derive_user_volume_accumulator(program.program_id,sf.WALLET)
    vi=e.setup_accounts.requested_keys.index(volume)
    system=e.setup_accounts.requested_keys.index(plans.SYSTEM_PROGRAM_ID)
    def omit_volume(groups):
        for group in groups:
            group["instructions"]=[ix for ix in group["instructions"] if not (ix["programIdIndex"]==system and vi in ix["accounts"])]
    deny(label+"_missing_volume_create",with_trace(value,omit_volume),True)
    raw=encode_validation_input(value)
    for name,transform in (
        ("version",lambda r:r.update(codec_version="unknown")),
        ("extra",lambda r:r.update(dynamic_type="evil")),
        ("bool_amount",lambda r:r["evidence"]["intent"].update(input_amount_base_units=True)),
        ("positive_permission",lambda r:r["context"]["acceptance"].update(grants_message_permission=True)),
        ("unknown_profile_version",lambda r:r["profile"].update(version="unknown")),
    ):
        row=json.loads(raw);transform(row)
        check(label+"_codec_"+name,raises(lambda:decode_validation_input(canonical_json(row))))
    check(label+"_duplicate_json_key",raises(lambda:decode_validation_input(raw[:-1]+',"codec_version":"duplicate"}')))
    check(label+"_float_amount",raises(lambda:decode_validation_input(raw.replace('"fee_lamports":7777','"fee_lamports":7777.0'))))
    check(label+"_fixed_permission_flag",raises(lambda:replace(validate_message_evidence(value),may_send=True)))
    check(label+"_integer_bool_profile_rejected",raises(lambda:replace(value.profile,maximum_fee_age_us=True)))
    for outcome in ("MISSING","PRUNED","ERROR"):
        deny(label+"_fee_"+outcome,replace(value,evidence=replace(e,fee=replace(e.fee,outcome=outcome,fee_lamports=None))),True)
    subset=replace(e.setup_accounts,requested_keys=e.setup_accounts.requested_keys[:-1],accounts=e.setup_accounts.accounts[:-1])
    deny(label+"_missing_setup_account_read",replace(value,evidence=replace(e,setup_accounts=subset)),True)
    for name,payload in (("zero_signature_count",b"\x00"+bytes.fromhex(e.simulation.envelopes[-1].message_hex)),
                         ("nonzero_signature",b"\x01"+bytes([1])*64+bytes.fromhex(e.simulation.envelopes[-1].message_hex))):
        check(label+"_"+name,raises(lambda payload=payload:replace(e.simulation.envelopes[-1],
            transaction_base64=base64.b64encode(payload).decode(),wire_sha256=hashlib.sha256(payload).hexdigest())))
    for name,size in (("trailing_wire_bytes",1),("oversized_full_wire",1233-len(base64.b64decode(e.simulation.envelopes[-1].transaction_base64)))):
        wire=base64.b64decode(e.simulation.envelopes[-1].transaction_base64)+bytes(size)
        try:
            envelope=replace(e.simulation.envelopes[-1],transaction_base64=base64.b64encode(wire).decode(),wire_sha256=hashlib.sha256(wire).hexdigest())
        except ValueError:
            check(label+"_"+name+"_rejected_at_external_constructor",True)
        else:
            attempt=replace(e.simulation.attempts[-1],envelope_id=envelope.envelope_id)
            result=replace(e.simulation.results[-1],envelope_id=envelope.envelope_id,attempt_id=attempt.attempt_id)
            deny(label+"_"+name,with_run(value,replace(e.simulation,envelopes=(envelope,),attempts=(attempt,),results=(result,))))
    raw_input=json.loads(encode_validation_input(value))
    raw_input["evidence"]["simulation"]["envelopes"][0]["request_config"]["replaceRecentBlockhash"]=True
    check(label+"_replace_hash_simulation_rejected",raises(lambda:decode_validation_input(canonical_json(raw_input))))
    raw_input=json.loads(encode_validation_input(value))
    raw_input["evidence"]["simulation"]["envelopes"][0]["request_config"]["sigVerify"]=True
    check(label+"_sigverify_simulation_rejected",raises(lambda:decode_validation_input(canonical_json(raw_input))))
    for number in (True,1.5,-1,1<<64):
        check(label+"_fee_exact_integer_"+str(number),raises(lambda number=number:replace(e.fee,fee_lamports=number)))
    max_fee=replace(e.fee,fee_lamports=(1<<64)-1)
    extreme=replace(value,evidence=replace(e,fee=max_fee))
    check(label+"_u64_codec_exact",decode_validation_input(encode_validation_input(extreme)).evidence.fee.fee_lamports==(1<<64)-1)
    deny(label+"_u64_above_approved_fee_cap",extreme)
    # Positive RPC success and large post-trade refunds cannot fund upfront.
    low=7777+value.context.action.input_units-1
    wallet=e.wallet.observation
    er=wallet.explicit_read
    accounts=tuple(replace(account,lamports=low) if key==sf.WALLET else account for key,account in zip(er.requested_keys,er.accounts))
    low_wallet=replace(e.wallet,observation=replace(wallet,explicit_read=replace(er,accounts=accounts)))
    low_value=with_pre(replace(value,evidence=replace(e,wallet=low_wallet)),sf.WALLET,lambda account:replace(account,lamports=low))
    deny(label+"_refund_cannot_fund_upfront",low_value,True)
    if label=="swap":
        ata_pid=e.setup_accounts.requested_keys.index(plans.ASSOCIATED_TOKEN_PROGRAM_ID)
        def omit_nested(groups):
            for group in groups:
                group["instructions"]=[ix for ix in group["instructions"] if ix["stackHeight"]!=3]
        deny(label+"_missing_all_nested_children",with_trace(value,omit_nested),True)
        def omit_invocation(groups):
            for group in groups:
                group["instructions"]=[ix for ix in group["instructions"] if ix["programIdIndex"]!=ata_pid]
        deny(label+"_orphan_nested_children",with_trace(value,omit_invocation))
        quote_vault=sf.plans.fx.QUOTE_VAULT
        for name,data in (("tail",lambda data:data+b"unsupported"),("frozen",lambda data:data[:108]+b"\x02"+data[109:])):
            changed=with_pre(value,quote_vault,lambda old:replace(old,account=replace(old.account,data=data(old.account.data))))
            deny(label+"_pool_"+name,changed,True)


def actual_positions(directory,cleanup):
    f,settlement,scenario,buy=fixture(directory,"position")
    cleanup.callback(f.close)
    prep,chain,_=a3.actual_finality(f,settlement,buy)
    support,post,request=a3.cf.composed_support(f.repo,settlement)
    post.accounts[sf.MINT]=a3.account(venue.TOKEN_PROGRAM_ID,sf.plans.fx.mint_account().data)
    post.slot_calls=post.genesis_calls=0
    support=a3.WalletSupportInput(a3.observe(post,request=request,at=NOW+14),a3.utc(NOW+14),request.min_context_slot)
    handoff=a3.ports.handoff(f.repo,buy,prep,chain,support)
    applied=f.repo.apply_settlement_with_ports(prep.attempt_id,chain_receipt_key="transaction",support=support,
        ingestion_key="applied-protected",recorded_at_utc=a3.utc(NOW+14),handoff=handoff,fence=f.repo.write_fence())
    check("SELL_fixture_actual_L3_L4_L6_acquisition",applied.application_disposition=="FINALIZED_SUCCESS_APPLIED"
        and f.repo.position_history(buy.position_id).remaining_units==settlement.actual_base)
    # The prior transaction is the existing external Execution fixture. A5's
    # continuous proof will carry A4's own exact bytes through settlement.
    quantity=settlement.actual_base//3
    sell=a3.PendingAction(buy.root_id,buy.candidate_digest,"SELL",buy.mint,buy.token_program,buy.position_id,quantity,
        "FIXTURE_RUNTIME_REDUCTION",content_fingerprint("runtime-reduction"),buy.policy_ref,buy.policy_digest,
        buy.selected_exit_track,handoff.obligation_id,1)
    f.repo.stage_action(sell,fence=f.repo.write_fence())
    c=capture_message_context(f.repo,sell.action_id);item=c.candidate
    intent=ExecutionIntentV01(item.candidate_signal_id,item.candidate_run_id,item.strategy_evaluation_id,item.strategy_version,
        item.parameter_set_id,item.producer_run_id,item.source_event_key,item.signal_ingest_seq,(NOW+18)*1000000,item.mint,
        IntentRole.EXIT,IntentSide.SELL,"MEME_BASE_UNITS",quantity,sell.position_id,_entry_intent(c).intent_id,
        sell.external_decision_ref,sell.selected_exit_track,handoff.binding_id)
    results=[]
    for route in ("pump","swap"):
        value,plan=evidence(f,sell,post,route=route,at=NOW+24,context_slot=116,intent=intent)
        result=validate_message_evidence(value)
        if result.disposition!="SUPPORTED_CONTEXT_ONLY":
            _validate(value)
        check("actual_position_"+route+"_SELL_context",result.disposition=="SUPPORTED_CONTEXT_ONLY")
        check("actual_position_"+route+"_SELL_no_proceeds_upfront",result.costs.quote_cap_including_venue_fees==0
            and result.costs.gross_upfront_native_lamports==7777+(R if route=="swap" else 0))
        check("actual_position_"+route+"_original_entry_deadline_not_exit_expiry",value.clock.utc_lower_utc>a3.utc(NOW+15))
        check("actual_position_"+route+"_original_position_codec",decode_validation_input(encode_validation_input(value))==value)
        wrong=replace(value,context=replace(value.context,action=replace(sell,obligation_id="unrelated")))
        deny("actual_position_"+route+"_wrong_obligation",wrong)
        deny("actual_position_"+route+"_wrong_parent",replace(value,evidence=replace(value.evidence,intent=replace(intent,parent_entry_intent_id="unrelated"))))
        results.append(result.content_digest)
        if route=="swap":
            check("migration_keeps_original_root_amount",value.context.acceptance.request.venue=="PUMP"
                and value.context.action.action_id==sell.action_id and value.evidence.intent.input_amount_base_units==quantity)
            # A new ENTRY policy/disarm does not erase original protection.
            a3.install(f,"unrelated-entry",allowed_venues=("PUMP",))
            a3.control(f.repo,"DISARM_ENTRY","entry-disarm",at=NOW+24)
            later=replace(value,context=capture_message_context(f.repo,sell.action_id))
            check("ENTRY_replacement_disarm_preserves_original_protective_context",
                  validate_message_evidence(later).disposition=="SUPPORTED_CONTEXT_ONLY")
            f.reopen()
            check("protective_context_after_reopen",validate_message_evidence(decode_validation_input(encode_validation_input(later)))==validate_message_evidence(later))
    f.close()
    f,_,scenario,buy=fixture(directory,"route-forbidden","pump",allowed_venues=("PUMP",))
    cleanup.callback(f.close)
    value,_=evidence(f,buy,scenario,route="swap")
    deny("original_allowed_venue_blocks_unsupported_refresh",value)
    f.close()
    # BUY route hint also cannot re-root or resize a refreshed compatible route.
    f,_,scenario,buy=fixture(directory,"buy-migration","pump")
    cleanup.callback(f.close)
    value,_=evidence(f,buy,scenario,route="swap")
    check("BUY_migration_same_admitted_root_amount",validate_message_evidence(value).disposition=="SUPPORTED_CONTEXT_ONLY"
        and value.context.acceptance.request.venue=="PUMP" and value.context.action==buy)
    f.close()
    # Exact canonical170 is independently supported by the actual Wallet path.
    f,_,scenario,buy=fixture(directory,"token22","swap",venue.TOKEN_2022_PROGRAM_ID)
    cleanup.callback(f.close)
    value,_=evidence(f,buy,scenario,route="swap")
    result=validate_message_evidence(value)
    if result.disposition!="SUPPORTED_CONTEXT_ONLY":
        _validate(value)
    check("canonical170_actual_A3_existing_account_context",result.disposition=="SUPPORTED_CONTEXT_ONLY")
    check("canonical170_no_fresh_base_creation",result.costs.gross_wallet_account_funding_lamports==R)
    absent=copy.deepcopy(scenario)
    absent.accounts.pop(buy.position_id,None)
    base=plans.derive_associated_token_address(sf.WALLET,buy.mint,buy.token_program)
    absent.accounts.pop(base);absent.inventory[buy.token_program].remove(base)
    fresh=a3.wallet(f,scenario=absent,program=buy.token_program,at=NOW+5)
    deny("fresh_token22_creation_explicitly_unresolved",replace(value,evidence=replace(value.evidence,wallet=fresh)),True)
    legacy=replace(value.evidence.wallet,observation=replace(value.evidence.wallet.observation,schema="live_wallet_account_evidence_v0.1"))
    deny("old_wallet_version_never_reinterprets_canonical170",replace(value,evidence=replace(value.evidence,wallet=legacy)),True)
    f.reopen();check("canonical170_original_codec_replay",validate_message_evidence(decode_validation_input(encode_validation_input(value)))==result)
    f.close()
    return results


def main():
    digests=[]
    with tempfile.TemporaryDirectory(prefix="authority-message-") as temp, ExitStack() as cleanup:
        directory=Path(temp)
        for route in ("pump","swap"):
            f,settlement,scenario,action=fixture(directory,route,route)
            cleanup.callback(f.close)
            value,plan=evidence(f,action,scenario,route=route)
            check(route+"_finite_codec_roundtrip",decode_validation_input(encode_validation_input(value))==value)
            result=validate_message_evidence(value)
            if result.disposition!="SUPPORTED_CONTEXT_ONLY":
                _validate(value)
            check(route+"_actual_admission_external_exact_message_context",result.disposition=="SUPPORTED_CONTEXT_ONLY")
            check(route+"_never_permission",not result.grants_message_permission and not result.may_sign and not result.may_send)
            check(route+"_gross_costs_no_refund_credit",result.costs.gross_upfront_native_lamports==7777+100000000+1844400+(R if route=="pump" else 4*R))
            check(route+"_SOL_and_WSOL_cost_units_distinct",result.costs.venue_quote_asset==("SOL" if route=="pump" else "WSOL")
                and result.costs.gross_wsol_quote_outflow_units==(0 if route=="pump" else 96000000))
            digests.append(result.content_digest)
            f.reopen()
            check(route+"_reopen_original_input_replay",validate_message_evidence(decode_validation_input(encode_validation_input(value)))==result)
            check(route+"_trusted_recapture_same_economics",capture_message_context(f.repo,action.action_id)==value.context)
            hostile(value,plan,route)
            broken=copy.deepcopy(scenario);broken.program_failure=venue.TOKEN_PROGRAM_ID
            observed=a3.wallet(f,scenario=broken,at=NOW+5)
            deny(route+"_actual_Wallet_adapter_incomplete",replace(value,evidence=replace(value.evidence,wallet=observed)),True)
            old_audit=f.repo.audit()
            deny(route+"_new_evaluation_does_not_reuse_aged_context",replace(value,clock=a3.clock(f.repo,NOW+100)),True)
            check(route+"_validator_no_journal_mutation",f.repo.audit()==old_audit)
            with closing(sqlite3.connect(f.path)) as outside:
                outside.execute("PRAGMA user_version=8")
            check(route+"_guarded_capture_rejects_unverified_outside_commit",raises(lambda:capture_message_context(f.repo,action.action_id)))
            f.reopen()
            check(route+"_explicit_reopen_reverifies_unchanged_history",capture_message_context(f.repo,action.action_id)==value.context)
            f.close()
        digests.extend(actual_positions(directory,cleanup))
    print(canonical_json({"status":"IMPLEMENTED_PENDING_PROJECT_REVIEW","checks":len(CHECKS),"check_digest":content_fingerprint(CHECKS),"result_digests":digests}))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
