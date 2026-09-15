"""Original post-venue wallet observation; isolated deterministic tests."""
from pathlib import Path
import sys,tempfile,json
from dataclasses import replace
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_execution_message_selftest_v0_1 as q
import live_runtime_dry_selftest_v0_1 as d
from live.execution_message_v0_1 import produce_exact_message,ComputeBudget
from live.execution_readonly_v0_1 import ExecutionReadOnlyRpc
from live.wallet_evidence_v0_1 import ledger_account_evidence
import httpx
checks={}
def check(name,value):
 checks[name]=bool(value)
 if not value:raise AssertionError(name)
def older(support):
 o=support.observation;slot=o.explicit_read.context.slot-1
 return replace(support,observation=replace(o,anchor=replace(o.anchor,slot=slot,parent_slot=slot-1),finalized_upper_slot=max(o.finalized_upper_slot,slot),inventories=tuple(replace(r,context=replace(r.context,slot=slot)) for r in o.inventories),explicit_read=replace(o.explicit_read,context=replace(o.explicit_read.context,slot=slot)),mint_reads=tuple(replace(r,context=replace(r.context,slot=slot)) for r in o.mint_reads)))
with tempfile.TemporaryDirectory(prefix='wallet-after-venue-') as tmp:
 for mode in ('valid','below_floor','wrong_type','unavailable'):
  f,_,scenario,action=q.a4.fixture(Path(tmp),'live-'+mode,'pump')
  try:
   seed,_=q.a4.evidence(f,action,scenario,route='pump');q.install(f,seed);boundary=q.PublicTransport(seed);seen=[];stale=older(seed.evidence.wallet)
   def observe(floor):
    seen.append(floor);check(mode+'_after_three_venue_reads',boundary.account_calls==3)
    if mode=='unavailable':raise ValueError('PUBLIC_CURRENT_WALLET_SUPPORT_UNAVAILABLE')
    if mode=='wrong_type':return object()
    return stale if mode=='below_floor' else seed.evidence.wallet
   with ExecutionReadOnlyRpc('https://fixture.invalid',q.a3.PROFILE,now_us=boundary.now,transport=httpx.MockTransport(boundary)) as rpc:
    def produce():return produce_exact_message(f.repo,action.action_id,rpc,stale,seed.evidence.quote_policy,seed.evidence.plan_policy,ComputeBudget(250000,1000),clock=lambda:seed.clock,now_us=lambda:seed.evidence.simulation.leases[0].observed_at_us,wallet_after_venue=observe)
    if mode=='valid':
     result=produce();check('fresh_observation_original_A4_supported',result.validation.disposition=='SUPPORTED_CONTEXT_ONLY');check('fresh_original_observation_retained',result.original.evidence.wallet==seed.evidence.wallet)
    else:check(mode+'_denied',q.fails(produce))
   check(mode+'_one_observation_no_retry',len(seen)==1 and seen[0]==seed.evidence.venue_read.verification.cut.context_slot)
   check(mode+'_no_preparation_or_economic_write',f.repo.audit()['attempt_count']==0 and f.repo.audit()['posting_count']==0)
  finally:f.close()
 f,_,scenario,action,seed=d.fixture(Path(tmp),'dry-valid')
 try:
  boundary=d.DryPublicTransport(seed);seen=[]
  def observe(floor):seen.append(floor);return seed.evidence.wallet
  with ExecutionReadOnlyRpc('https://fixture.invalid',q.a3.PROFILE,now_us=boundary.now,transport=httpx.MockTransport(boundary)) as rpc:
   result=d.dry.run_dry(f.repo,action.action_id,rpc,older(seed.evidence.wallet),seed.evidence.quote_policy,seed.evidence.plan_policy,ComputeBudget(250000,1000),clock=lambda:seed.clock,now_us=lambda:d.dry.utc_microseconds(seed.clock.utc_upper_utc) if len(boundary.requests)>=9 else seed.evidence.simulation.leases[0].observed_at_us,wallet_after_venue=observe)
  check('dry_original_exact_simulation_terminal',result.kind=='DRY_NON_SUBMITTED' and f.repo.audit()['attempt_count']==1)
  check('dry_original_simulation_stage_recorded',any(json.loads(row[0])['to_stage']=='EXACT_SIMULATED' for row in f.repo._conn.execute('SELECT payload_json FROM ledger_attempt_stages')))
  check('dry_observed_once',len(seen)==1)
  check('dry_no_signatures_no_positions',all(a.primary_signature is None for a in f.repo._root_attempts(action.root_id)) and not f.repo.consumer_snapshot()['positions'])
 finally:f.close()
print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(checks),'all_checks':all(checks.values()),'results':checks,'network':False,'production_stores':False},sort_keys=True))
