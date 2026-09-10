"""Q1 production producer qualification; public deterministic HTTP, temporary DBs.

Old fixture code supplies only external chain/account/CPI facts. No supplied
route, plan, message, validation or permission enters the production producer.
"""
from __future__ import annotations
import base64
import copy
import json
import sys
import tempfile
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
import httpx
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
import live_authority_message_control_selftest_v0_1 as control
from live.execution_message_v0_1 import ComputeBudget, produce_exact_message, prepare_exact_message, ExactMessageProduction, _intent
from live.execution_readonly_v0_1 import ExecutionReadOnlyRpc
from live.authority_message_control_v0_1 import MessageProfileCommand, FreshStageConsumption
from live.authority_message_evidence_v0_1 import capture_message_context
from live.public_rpc_v0_1 import PublicRpcError
from live.ledger_domain_v0_1 import LedgerContractError
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from live.authority_message_codec_v0_1 import encode_validation_input

a4,a3,sf,NOW = control.a4,control.a3,control.sf,control.NOW
CHECKS={}
def check(name, value):
    CHECKS[name]=bool(value)
    if not value: raise AssertionError(name)

def fails(fn):
    try: fn()
    except (ValueError, TypeError, RuntimeError): return True
    return False

class PublicTransport:
    def __init__(self, seed, *, fault=None):
        self.seed, self.fault, self.requests = seed, fault, []
        e=seed.evidence
        self.venue={key:value for batch in (e.venue_read.primary,e.venue_read.dependent)
            for key,value in zip(batch.requested_keys,batch.accounts)}
        self.setup=dict(zip(e.setup_accounts.requested_keys,e.setup_accounts.accounts))
        self.at=seed.evidence.venue_read.verification.cut.observed_at_us
        self.account_calls=0
    def now(self): return self.at
    def __call__(self, request):
        payload=json.loads(request.content);method=payload['method'];params=payload['params']
        self.requests.append(payload)
        e=self.seed.evidence;run=e.simulation;slot=run.leases[0].context_slot
        if method=='getGenesisHash': result=a3.GENESIS
        elif method=='getMultipleAccounts':
            self.account_calls+=1
            source=self.venue if self.account_calls<=3 else self.setup
            result={'context':{'slot':slot},'value':[]}
            for key in params[0]:
                value=source.get(key)
                result['value'].append(None if value is None else a3.account(value.account.owner,value.account.data,
                    value.lamports,executable=value.executable,rentEpoch=value.rent_epoch))
            if self.fault=='changed-venue' and self.account_calls==3:
                result['value'][0]['lamports']+=1
        elif method=='getLatestBlockhash':
            self.at=run.leases[0].observed_at_us
            result={'context':{'slot':slot},'value':{'blockhash':run.leases[0].blockhash,'lastValidBlockHeight':200}}
        elif method=='getBlockHeight': result=100
        elif method=='isBlockhashValid': result={'context':{'slot':slot},'value':self.fault!='expired'}
        elif method=='getFeeForMessage':
            result={'context':{'slot':slot},'value':None if self.fault=='null-fee' else 7777}
        elif method=='simulateTransaction':
            r=run.results[0]
            result={'context':{'slot':slot},'value':{'err':None,'logs':[],
                'unitsConsumed':200000,'loadedAccountsDataSize':30000,
                'innerInstructions':json.loads(r.inner_instructions_json),'returnData':None,'accounts':None}}
            if self.fault=='missing-cpi': result['value']['innerInstructions']=None
            elif self.fault=='compute-exceeded': result['value']['unitsConsumed']=250001
            elif self.fault=='simulation-failed': result['value']['err']={'InstructionError':[2,'InvalidArgument']}
        else: raise AssertionError(method)
        if self.fault=='private-error':
            return httpx.Response(200,json={'jsonrpc':'2.0','id':payload['id'],'error':{'message':'SECRET_SENTINEL'}})
        if self.fault=='wrong-genesis' and method=='getGenesisHash': result=sf.bh(221)
        if self.fault=='context-regression' and method=='getLatestBlockhash': result['context']['slot']=0
        return httpx.Response(200,json={'jsonrpc':'2.0','id':payload['id'],'result':result})


def install(f,seed):
    f.repo.record_authority_message_profile(MessageProfileCommand('profile','INSTALL_AND_SELECT',seed.profile,seed.profile.approval),fence=f.repo.write_fence())


def produce(f,action,seed,*,fault=None,compute=None):
    boundary=PublicTransport(seed,fault=fault)
    at=seed.clock.utc_upper_utc
    # Same bounded public temporal cut. No network access: MockTransport only.
    with ExecutionReadOnlyRpc('https://private.invalid/SECRET_ENDPOINT',a3.PROFILE,now_us=boundary.now,
        transport=httpx.MockTransport(boundary)) as rpc:
        value=produce_exact_message(f.repo,action.action_id,rpc,seed.evidence.wallet,seed.evidence.quote_policy,
            seed.evidence.plan_policy,compute or ComputeBudget(250000,1000),
            clock=lambda:seed.clock,now_us=lambda:seed.evidence.simulation.leases[0].observed_at_us)
    return value,boundary


def qualify(f,action,seed,label):
    install(f,seed)
    before=f.repo.write_fence()
    value,boundary=produce(f,action,seed)
    check(label+'_actual_A4a',value.validation.disposition=='SUPPORTED_CONTEXT_ONLY')
    original=value.original
    check(label+'_no_mutation_before_preparation',f.repo.write_fence()==before)
    check(label+'_original_LIVE_amount',original.context.action==action and original.evidence.intent.input_amount_base_units==action.input_units)
    wire=base64.b64decode(next(r['params'][0] for r in boundary.requests if r['method']=='simulateTransaction'))
    check(label+'_same_exact_zero_signature_message',wire[:65]==b'\x01'+bytes(64) and wire[65:].hex()==value.message_hex)
    check(label+'_no_replacement_or_retry',len([r for r in boundary.requests if r['method']=='simulateTransaction'])==1
        and len([r for r in boundary.requests if r['method']=='getLatestBlockhash'])==1
        and next(r['params'][1]['replaceRecentBlockhash'] for r in boundary.requests if r['method']=='simulateTransaction') is False)
    check(label+'_exact_fee',base64.b64decode(next(r['params'][0] for r in boundary.requests if r['method']=='getFeeForMessage'))==wire[65:])
    check(label+'_private_endpoint_excluded','SECRET_ENDPOINT' not in str(value))
    check(label+'_read_capture',len(value.original_read_records)==len(boundary.requests)==10)
    check(label+'_immutable_original_roundtrip',ExactMessageProduction(value.validation_input_json,value.original_read_records)==value)
    stored=prepare_exact_message(f.repo,value,fence=f.repo.write_fence())
    before_retry=f.repo.write_fence()
    check(label+'_idempotent_preparation',prepare_exact_message(f.repo,value,fence=before_retry)==stored
        and f.repo.write_fence()==before_retry)
    prep=stored.preparation
    check(label+'_actual_Ledger_exact_preparation',stored.recorded_stage=='PREPARED' and prep.message_hex==value.message_hex
        and prep.external_preparation_digest==value.content_digest and prep.action_content_digest==action.content_digest)
    delivery=control.consume(f,value.original,control.request(f,prep,label+'SIGN'),at=NOW+9 if action.side=='BUY' else NOW+24)
    check(label+'_actual_current_A4b_SIGN',type(delivery) is FreshStageConsumption)
    check(label+'_context_is_not_signing',not value.validation.may_sign and f.repo.attempt(prep.attempt_id).primary_signature is None)
    return value,prep


def main():
    with tempfile.TemporaryDirectory(prefix='live-execution-q1-') as tmp, ExitStack() as cleanup:
        tmp=Path(tmp)
        for route in ('pump','swap'):
            f,_,scenario,action=a4.fixture(tmp,'buy-'+route,route);cleanup.callback(f.close)
            seed,_=a4.evidence(f,action,scenario,route=route)
            qualify(f,action,seed,'BUY_'+route)
        for route in ('pump','swap'):
            f,settlement,scenario,buy=a4.fixture(tmp,'sell-'+route);cleanup.callback(f.close)
            prep,chain,_=a3.actual_finality(f,settlement,buy)
            support,post,request=a3.cf.composed_support(f.repo,settlement)
            post.accounts[sf.MINT]=a3.account(a4.venue.TOKEN_PROGRAM_ID,sf.plans.fx.mint_account().data)
            post.slot_calls=post.genesis_calls=0
            support=a3.WalletSupportInput(a3.observe(post,request=request,at=NOW+14),a3.utc(NOW+14),request.min_context_slot)
            handoff=a3.ports.handoff(f.repo,buy,prep,chain,support)
            f.repo.apply_settlement_with_ports(prep.attempt_id,chain_receipt_key='transaction',support=support,
                ingestion_key='protected',recorded_at_utc=a3.utc(NOW+14),handoff=handoff,fence=f.repo.write_fence())
            quantity=settlement.actual_base//3
            sell=a3.PendingAction(buy.root_id,buy.candidate_digest,'SELL',buy.mint,buy.token_program,buy.position_id,quantity,
                'EXTERNAL_RUNTIME_REDUCTION',content_fingerprint('runtime'),buy.policy_ref,buy.policy_digest,buy.selected_exit_track,handoff.obligation_id,1)
            f.repo.stage_action(sell,fence=f.repo.write_fence())
            intent=_intent(capture_message_context(f.repo,sell.action_id),(NOW+24)*1000000)
            seed,_=a4.evidence(f,sell,post,route=route,at=NOW+24,context_slot=116,intent=intent,wallet_floor=116)
            qualify(f,sell,seed,'SELL_'+route)
        f,_,scenario,action=a4.fixture(tmp,'hostile');cleanup.callback(f.close)
        seed,_=a4.evidence(f,action,scenario);install(f,seed)
        for fault in ('null-fee','missing-cpi','compute-exceeded','simulation-failed'):
            value,_=produce(f,action,seed,fault=fault)
            check(fault+'_unsupported',value.validation.disposition!='SUPPORTED_CONTEXT_ONLY')
            before=f.repo.write_fence()
            check(fault+'_no_preparation',fails(lambda:prepare_exact_message(f.repo,value,fence=before)) and f.repo.write_fence()==before)
        for fault in ('changed-venue','expired','wrong-genesis','private-error','context-regression'):
            before=f.repo.write_fence()
            try: produce(f,action,seed,fault=fault)
            except (ValueError,RuntimeError) as exc:
                check(fault+'_failclosed','SECRET' not in str(exc) and f.repo.write_fence()==before)
            else: raise AssertionError(fault)
        check('compute_bound_before_read',fails(lambda:produce(f,action,seed,compute=ComputeBudget(250001,1000))))
        value,_=produce(f,action,seed)
        check('read_provenance_tamper',fails(lambda:replace(value,original_read_records=value.original_read_records[:-1])))
        message=a4.Message.from_bytes(bytes.fromhex(value.message_hex))
        h=message.header
        from solders.instruction import CompiledInstruction
        def altered(*,keys=None,instructions=None,blockhash=None):
            changed=a4.Message.new_with_compiled_instructions(h.num_required_signatures,h.num_readonly_signed_accounts,
                h.num_readonly_unsigned_accounts,keys or message.account_keys,blockhash or message.recent_blockhash,
                instructions or message.instructions)
            return a4.with_message(value.original,changed)
        instructions=list(message.instructions)
        fee_ix=instructions[1]
        changed_fee=list(instructions)
        changed_fee[1]=CompiledInstruction(fee_ix.program_id_index,b"\x03"+(999).to_bytes(8,'little'),fee_ix.accounts)
        changed_order=list(instructions);changed_order[-1],changed_order[-2]=changed_order[-2],changed_order[-1]
        changed_accounts=list(message.account_keys);changed_accounts[-1],changed_accounts[-2]=changed_accounts[-2],changed_accounts[-1]
        changed_amount=list(instructions)
        trade=changed_amount[-1]
        changed_amount[-1]=CompiledInstruction(trade.program_id_index,bytes(trade.data)[:-1]+bytes([trade.data[-1]^1]),trade.accounts)
        e=value.original.evidence
        mutations={
            'fee-freshness':replace(value.original,evidence=replace(e,fee=replace(e.fee,
                cut=replace(e.fee.cut,observed_at_us=e.fee.cut.observed_at_us+1)))),
            'simulation-freshness':a4.with_result(value.original,observed_at_us=e.simulation.results[0].observed_at_us+1),
            'priority-fee':altered(instructions=changed_fee),
            'instruction-order':altered(instructions=changed_order),
            'account-order':altered(keys=changed_accounts),
            'instruction-data':altered(instructions=changed_amount),
            'blockhash':altered(blockhash=a4.Hash.from_string(sf.bh(240))),
            'exact-total-fee':replace(value.original,evidence=replace(e,fee=replace(e.fee,fee_lamports=7778))),
        }
        for label,changed in mutations.items():
            check(label+'_original_capture_binding',fails(lambda:replace(value,validation_input_json=encode_validation_input(changed))))
        before=f.repo.write_fence()
        with ExecutionReadOnlyRpc('https://invalid.local',a3.PROFILE,now_us=lambda:0,
                transport=httpx.MockTransport(lambda _: (_ for _ in ()).throw(AssertionError('external call')))) as rpc:
            check('arbitrary_rpc_rejected',fails(lambda:rpc._post('sendTransaction',[])))
            check('unrelated_inherited_read_rejected',fails(lambda:rpc.get_finalized_slot(min_context_slot=100)))
            wire=bytearray(base64.b64decode(e.simulation.envelopes[0].transaction_base64));wire[1]=1
            config=a4.plans.SimulationRequestConfigV01(101).rpc_payload()
            check('nonzero_signature_rejected_before_transport',fails(lambda:rpc.simulate_transaction(base64.b64encode(wire).decode(),config=config)))
            config['replaceRecentBlockhash']=True
            check('replacement_shortcut_rejected_before_transport',fails(lambda:rpc.simulate_transaction(e.simulation.envelopes[0].transaction_base64,config=config)))
        check('denial_suite_no_ledger_write',f.repo.write_fence()==before)
        a3.control(f.repo,'DISARM_ENTRY','disarm',at=NOW+9)
        check('changed_current_context_blocks_prepare',fails(lambda:prepare_exact_message(f.repo,value,fence=f.repo.write_fence())))
    print(canonical_json({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(CHECKS),'all_checks':all(CHECKS.values())}))

if __name__=='__main__': main()
