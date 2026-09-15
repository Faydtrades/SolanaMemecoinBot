"""Bounded concurrent wallet acquisition; deterministic external fixtures only."""
from pathlib import Path
import sys,json,threading
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(ROOT/'scripts'))
import live_wallet_evidence_selftest_v0_1 as f
from live.wallet_evidence_v0_1 import WalletEvidenceAdapter,WalletEvidenceRequest,ExpectedTokenAccount,assess_wallet_accounts,ledger_account_evidence
from live.public_rpc_v0_1 import PublicReadOnlyRpc,PublicRpcError
from live.ledger_evidence_codec_v0_1 import wallet_observation_to_json
import httpx
checks={}
def check(name,value):
 checks[name]=bool(value)
 if not value:raise AssertionError(name)
def observe(scenario,cohort=True,request=None,profile=f.PROFILE,handler=None):
 with PublicReadOnlyRpc(f.ENDPOINT,profile,transport=httpx.MockTransport(handler or scenario.handle)) as rpc:
  adapter=WalletEvidenceAdapter(rpc,request or WalletEvidenceRequest(f.WALLET,f.GENESIS,100),clock=f.utc,concurrent_cohort=cohort)
  result=adapter.observe()
  check('single_observation_no_budget_renewal',f.raises(PublicRpcError,adapter.observe))
  return result,rpc._requests,rpc._response_bytes
s=f.Scenario();a,_,_=observe(s,False);b,_,_=observe(f.Scenario())
check('historical_sequential_and_cohort_exact_facts',wallet_observation_to_json(a)==wallet_observation_to_json(b))
barrier=threading.Barrier(2);lock=threading.Lock();active=0;peak=0
s=f.Scenario();s.accounts[f.MINT]=f.account(f.TOKEN_PROGRAM_ID,f.mint_data());request=WalletEvidenceRequest(f.WALLET,f.GENESIS,100,(ExpectedTokenAccount(f.TOKEN,f.MINT,f.TOKEN_PROGRAM_ID),))
def paired(request):
 global active,peak
 method=json.loads(request.content)['method']
 if method in ('getTokenAccountsByOwner','getMultipleAccounts'):
  with lock:active+=1;peak=max(peak,active)
  barrier.wait(timeout=5)
  try:return s.handle(request)
  finally:
   with lock:active-=1
 return s.handle(request)
b,count,_=observe(s,request=request,handler=paired)
check('each_dependent_pair_really_overlaps',peak==2)
check('cohort_complete_and_coherent',not assess_wallet_accounts(b).reasons)
check('exact_request_count_no_retry',count==9)
check('known_mint_read_once',len(b.mint_reads)==1)
s=f.Scenario();s.context=s.multiple_context=s.upper_slot=105
s.accounts[f.MINT]=f.account(f.TOKEN_PROGRAM_ID,f.mint_data())
def inventory_floor(req):
 payload=json.loads(req.content)
 if payload['method']=='getMultipleAccounts':
  check('dependent_reads_follow_both_inventories',sum(p['method']=='getTokenAccountsByOwner' for p,_ in s.calls)==2)
  check('dependent_floor_is_observed_inventory_slot',payload['params'][1]['minContextSlot']==105)
 if payload['method']=='getSlot' and s.slot_calls:
  check('upper_floor_covers_all_actual_contexts',payload['params'][0]['minContextSlot']==105)
 return s.handle(req)
b,count,_=observe(s,request=request,handler=inventory_floor)
check('inventory_floor_coherent_original_facts',not assess_wallet_accounts(b).reasons and b.initial_finalized_slot==100 and b.explicit_read.context.slot==105)
check('inventory_floor_does_not_change_original_request',b.request.min_context_slot==100 and count==9)
s=f.Scenario();s.context=105;s.multiple_context=104;s.upper_slot=105
bad,_,_=observe(s)
check('provider_below_observed_floor_is_retained_failure',any(x.operation=='EXPLICIT_ACCOUNTS' for x in bad.failures) and bool(assess_wallet_accounts(bad).reasons))
for name,mutate in [('different_slot',lambda s:setattr(s,'multiple_context',102)),('inventory_failure',lambda s:setattr(s,'program_failure',f.TOKEN_PROGRAM_ID)),('wrong_end_genesis',lambda s:setattr(s,'end_genesis',f.PARENT))]:
 s=f.Scenario();mutate(s);bad,_,_=observe(s)
 check(name+'_remains_denied',bool(assess_wallet_accounts(bad).reasons))
s=f.Scenario();bad,count,_=observe(s,profile=replace(f.PROFILE,max_requests=3))
check('request_cap_never_exceeded',count==3 and len(s.calls)==3)
check('request_cap_failure_retained',any(x.code=='RPC_REQUEST_BUDGET_EXHAUSTED' for x in bad.failures))
s=f.Scenario();bad,count,used=observe(s,profile=replace(f.PROFILE,max_total_response_bytes=500))
check('aggregate_response_cap_denies',any(x.code=='RPC_RESPONSE_BUDGET_EXHAUSTED' for x in bad.failures))
ids=[p['id'] for p,_ in s.calls];check('concurrent_request_ids_unique',len(ids)==len(set(ids)))
s=f.Scenario();s.program_failure=f.TOKEN_PROGRAM_ID;bad,count,_=observe(s)
check('failed_cohort_no_repeat_inventory',sum(p['method']=='getTokenAccountsByOwner' for p,_ in s.calls)==2)
# Discovered mint is still read and checked; no omission merely because absent from expected keys.
s=f.Scenario();s.accounts[f.TOKEN]=f.account(f.TOKEN_PROGRAM_ID,f.token_data());s.accounts[f.MINT]=f.account(f.TOKEN_PROGRAM_ID,f.mint_data());s.inventory[f.TOKEN_PROGRAM_ID]=[f.TOKEN]
b,_,_=observe(s);check('new_inventory_mint_observed',b.mint_reads[0].requested_keys==(f.MINT,))
check('new_inventory_mint_original_checks',not assess_wallet_accounts(b).reasons)
with PublicReadOnlyRpc(f.ENDPOINT,f.PROFILE,transport=httpx.MockTransport(f.Scenario().handle)) as rpc:
 check('explicit_bool_mode',f.raises(PublicRpcError,lambda:WalletEvidenceAdapter(rpc,WalletEvidenceRequest(f.WALLET,f.GENESIS,100),concurrent_cohort=1)))
print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(checks),'all_checks':all(checks.values()),'results':checks,'network':False,'production_stores':False},sort_keys=True))
