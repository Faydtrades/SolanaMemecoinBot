"""Current normal Pump Token2022 lifecycle; fake public transports, isolated stores only."""
from pathlib import Path
from contextlib import closing
from dataclasses import replace
from unittest.mock import patch
import sys, json, struct, base64, tempfile, copy, subprocess, os
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_ledger_settlement_selftest_v0_1 as sf
import live_ledger_custody_selftest_v0_1 as cf
from live_pump_token2022_evidence_selftest_v0_1 import mint_bytes
from live.wallet_evidence_v0_1 import SCHEMA, LEGACY_SCHEMA, IMMUTABLE_OWNER_SCHEMA
from live.ledger_settlement_v0_1 import attribute_settlement, _pump_trade_event, GET_FEES_WITH_QUOTE_MINT_TAG
from live.transaction_evidence_v0_1 import _b58data
from solders.pubkey import Pubkey
CHECKS={}
TAIL=bytes.fromhex('0207000000')
ORIGINAL_FIXTURE=sf.Fixture
ORIGINAL_OBSERVE=sf.observe

def check(name,value):
 CHECKS[name]=bool(value)
 if not value:raise AssertionError(name)

def observe(value,**kwargs):
 # Only the explicitly synthetic public-response fixture is transformed.
 for key,item in tuple(value.accounts.items()):
  if item is None or item['owner']!=sf.TOKEN_2022_PROGRAM_ID:continue
  raw=base64.b64decode(item['data'][0])
  if len(raw)==82:raw=mint_bytes(key)
  elif len(raw)==165:raw+=TAIL
  item['data']=[base64.b64encode(raw).decode(),'base64'];item['space']=len(raw)
 return ORIGINAL_OBSERVE(value,**kwargs)

class Fixture(ORIGINAL_FIXTURE):
 def __init__(self,*args,**kwargs):
  kwargs.setdefault('token_program',sf.TOKEN_2022_PROGRAM_ID)
  super().__init__(*args,**kwargs)
  self.mint=sf.MINT
  for group in self.groups:
   for row in group['instructions']:
    data=_b58data(row['data'],65536)
    if data[:4]==bytes(4) and len(data)==52 and str(Pubkey.from_bytes(data[20:]))==sf.TOKEN_2022_PROGRAM_ID:
     row['data']=sf.b58(data[:12]+struct.pack('<Q',170)+data[20:])
    if data[:16]==sf.ANCHOR_EVENT_TAG+sf.PUMP_TRADE_EVENT_TAG:
     row['data']=sf.b58(self.trade_event(fee=800000)+bytes(16))
    if data[:8]==sf.GET_FEES_TAG:
     row['data']=sf.b58(GET_FEES_WITH_QUOTE_MINT_TAG+b'\x01'+bytes(16)+bytes(32))

def evaluate(directory,name,mutate=None,support_change=None):
 fixture=Fixture()
 if mutate:mutate(fixture)
 repo,binding,action,prep,chain=fixture.setup(directory/(name+'.sqlite3'))
 with repo:
  support,value,request=cf.composed_support(repo,fixture)
  if support_change:support=support_change(support)
  result=attribute_settlement(binding,action,repo.attempt(prep.attempt_id),chain,support)
  receipt=cf.apply(repo,prep,support)
  if mutate or support_change:
   check(name+'_no_proposal',result.proposal is None)
   check(name+'_atomic_no_postings',receipt.decision.disposition in ('UNAPPLIED','QUARANTINED') and repo.audit()['posting_count']==0 and not repo.consumer_snapshot()['positions'])
  else:
   check(name+'_actual_finalized_proposal',result.disposition=='PROPOSED')
  digest=repo.audit()['custody_digest']
 with sf.LedgerRepository.reopen(directory/(name+'.sqlite3'),binding) as repo:
  check(name+'_cold_replay',repo.audit()['custody_digest']==digest and repo.application_receipt('apply')==receipt)
 return result

def creation_row(f):return f.groups[0]['instructions'][1]
def event_row(f):return next(row for row in f.groups[-1]['instructions'] if _b58data(row['data'],65536)[:16]==sf.ANCHOR_EVENT_TAG+sf.PUMP_TRADE_EVENT_TAG)

def bad_create(offset,data):
 def mutate(f):
  row=creation_row(f);raw=_b58data(row['data'],65536);row['data']=sf.b58(raw[:offset]+data+raw[offset+len(data):])
 return mutate

def lifecycle(directory):
 path=directory/'lifecycle.sqlite3'
 repo,binding,action,prep,chain,support,fixture=cf.first(path)
 with repo:
  receipt=cf.apply(repo,prep,support)
  check('fresh_buy_atomic_applied',receipt.decision.disposition=='FINALIZED_SUCCESS_APPLIED')
  proposal=receipt.decision.proposal
  check('fresh_exact_finalized_units_not_quote',proposal.base_units_delta==fixture.actual_base and fixture.actual_base!=fixture.plan_tuple[3].expected_base_amount)
  check('fresh_exact_funding_lock_fee',proposal.locked_account_lamports_delta==sf.R and proposal.account_funding_lamports==sf.R+1844400 and proposal.transaction_fee_lamports==sf.FEE)
  check('fresh_native_account_and_position',repo.consumer_snapshot()['accounts'][0].observed_lamports==sf.R and repo.position_history(action.position_id).remaining_units==fixture.actual_base)
  digest=repo.audit()['custody_digest']
 with sf.LedgerRepository.reopen(path,binding) as repo:
  check('fresh_cold_exact_position_and_funding',repo.audit()['custody_digest']==digest and repo.application_receipt('apply')==receipt)
  fs,act,pr,ch,sp,at=cf.next_step(repo,fixture,support.observation.anchor,number=61,venue='pump',side='SELL',position_id=action.position_id,quantity=fixture.actual_base)
  sold=cf.apply(repo,pr,sp,chain='chain-61',key='sell',at=at+20)
  check('sell_actual_settled_units',sold.decision.disposition=='FINALIZED_SUCCESS_APPLIED' and sold.decision.proposal.base_units_delta==-fixture.actual_base)
  check('sell_preserves_account_lock',sold.decision.proposal.locked_account_lamports_delta==0 and repo.consumer_snapshot()['accounts'][0].observed_lamports==sf.R)
  check('sell_zero_actual_remaining',repo.position_history(action.position_id).remaining_units==0)
  digest=repo.audit()['custody_digest']
 with sf.LedgerRepository.reopen(path,binding) as repo:check('sell_cold_reconstruction',repo.audit()['custody_digest']==digest)




def existing_and_topups(directory):
 path=directory/'existing-ata.sqlite3'
 repo,binding,action,prep,chain,support,fixture=cf.first(path)
 with repo:
  cf.apply(repo,prep,support)
  with patch.object(sf,'Fixture',lambda *a,**kw:Fixture(*a,**dict(kw,volume_setup=False))):
   fs,act,pr,ch,sp,at=cf.next_step(repo,fixture,support.observation.anchor,number=62,venue='pump',side='BUY')
  result=cf.apply(repo,pr,sp,chain='chain-62',key='existing-buy',at=at+20)
  check('existing_Token2022_ATA_current_buy_applied',result.decision.disposition=='FINALIZED_SUCCESS_APPLIED')
  check('existing_ATA_no_fresh_rent_witness_or_funding',result.decision.proposal.account_funding_lamports==0 and result.decision.proposal.locked_account_lamports_delta==0)
  digest=repo.audit()['custody_digest']
 with sf.LedgerRepository.reopen(path,binding) as repo:check('existing_ATA_current_buy_cold_replay',repo.audit()['custody_digest']==digest)
 for role in ('bonding_curve','creator_vault','fee_recipient'):
  def topup(f,role=role):
   target=f.roles[role];meta=f.scenario.tx['meta'];meta['postBalances'][0]-=111;meta['postBalances'][f.index[target]]+=111
   f.groups[-1]['instructions'].insert(-2,f.inner(sf.SYSTEM_PROGRAM_ID,(sf.WALLET,target),struct.pack('<IQ',2,111)))
  evaluate(directory,'current_topup_'+role,topup)
  if role in ('bonding_curve','creator_vault'):
   path=directory/('existing-topup-'+role+'.sqlite3')
   repo,binding,action,prep,chain,support,fixture=cf.first(path)
   with repo:
    cf.apply(repo,prep,support)
    with patch.object(sf,'Fixture',lambda *a,**kw:Fixture(*a,**dict(kw,volume_setup=False))):
     fs,act,pr,ch,sp,at=cf.next_step(repo,fixture,support.observation.anchor,number=63,venue='pump',side='BUY',mutate=topup)
    before=repo.audit()['posting_count'];result=cf.apply(repo,pr,sp,chain='chain-63',key='existing-topup',at=at+20)
    check('existing_'+role+'_topup_wholly_unapplied',result.decision.proposal is None and repo.audit()['posting_count']==before)

 def quote_conflict(f):event_row(f).update(data=sf.b58(f.trade_event(fee=800000,quote_amount=f.principal+1)+bytes(16)))
 evaluate(directory,'current_event_quote_conflict',quote_conflict)
 def wrong_fee(f):event_row(f).update(data=sf.b58(f.trade_event(fee=800001)+bytes(16)))
 evaluate(directory,'current_event_aggregate_fee_conflict',wrong_fee)
 for label,data in [('truncated_fee_query',GET_FEES_WITH_QUOTE_MINT_TAG+b'\x01'+bytes(47)),
                    ('non_native_fee_query',GET_FEES_WITH_QUOTE_MINT_TAG+b'\x01'+bytes(16)+bytes(Pubkey.from_string(sf.MINT)))]:
  def change(f,data=data):f.groups[-1]['instructions'][-1]['data']=sf.b58(data)
  evaluate(directory,label,change)
 def post_base_only(support):
  def change(value):
   if value is not None and value.account.owner==sf.TOKEN_2022_PROGRAM_ID and len(value.account.data)==170:
    return replace(value,account=replace(value.account,data=value.account.data[:165]))
   return value
  obs=support.observation
  return replace(support,observation=replace(obs,explicit_read=replace(obs.explicit_read,accounts=tuple(map(change,obs.explicit_read.accounts))),
   inventories=tuple(replace(read,accounts=tuple(map(change,read.accounts))) for read in obs.inventories)))
 evaluate(directory,'fresh_post_account_without_ImmutableOwner',support_change=post_base_only)


def retained_public_event():
 # Other public trader, finalized slot446737359; exact original RPC bytes:
 # transaction-0.json SHA256 dfa86162c04557d4469a9b19782297f335858ad01a19373e373390600ade77b6.
 # This is parser/attribution evidence, never a bot fill or permission.
 raw=bytes.fromhex('e445a52e51cb9a1dbddb7fd34ee661ee08ed57841b5391e744840797af7e49dc06f6639e42c7307508e378fcbddc7f4fe5c6b400000000008922693d5b00000000f02dc3bc0bebb1e6605d5c47624610d24fc1a103ae53380bdb2dec9b888c437e69c1a66a0000000002a3134307000000e3b6558ca6aa030002f7ef4600000000e31e434015ac02004ac2f8d0dd5cbc97e3289c197cb5062a54f3d956b9ce6e5115f96567aa5cb3e65f00000000000000a7b7010000000000e6c75f992baadd86de87bd6424d415520a075e2687ce941ea882b8b1f285eb6a1e00000000000000d78a0000000000000000000000000000000000000000000000000000000000000000000000000000000400000073656c6c00000000000000000000000000000000008813000000000000d3db0000000000000100000019e4fb2b77e9ad0c0d4222ab405d76653d5ce58f06e9dacdcc2a4612d58feb1010270000000000000000000000000000000000000000000000000000000000000000e5c6b4000000000002a313430700000002f7ef460000000000000000000000000000000000000000')
 event=_pump_trade_event(raw)
 check('actual_public_current_event_normal_shareholder',event['current_layout'] and event['shareholders']==(('2k5hrzuykwyTbUe8L7UriYAQhr5hijNLgBvEB4B9pP5y',10000),))
 check('actual_public_SOL_event_principal_matches_curve_debit',event['sol_amount']==event['quote_amount']==1203396175-1191548778==11847397)
 check('actual_public_aggregate_protocol_buyback_split',event['fee']-event['buyback_fee']==4831254672771-4831254616495==56276 and event['buyback_fee']==316688651-316632376==56275 and event['creator_fee']==1280194-1244651==35543)

def failed_prefixes(directory):
 for count in (1,2,3,4):
  fixture=Fixture(failed=True)
  whole=Fixture()
  fixture.scenario.tx['meta']['innerInstructions']=[{'index':whole.groups[0]['index'],'instructions':whole.groups[0]['instructions'][:count]}]
  repo,binding,action,prep,chain=fixture.setup(directory/('failed-prefix-'+str(count)+'.sqlite3'))
  with repo:
   support,_,_=cf.composed_support(repo,fixture)
   result=cf.apply(repo,prep,support)
   proposal=result.decision.proposal
   check('failed_prefix_'+str(count)+'_fee_only_atomic',result.decision.disposition=='FINALIZED_FAILURE_APPLIED'
    and proposal.transaction_fee_lamports==sf.FEE and proposal.account_funding_lamports==proposal.locked_account_lamports_delta==proposal.base_units_delta==0
    and all(item.units==0 and item.observed_lamports==0 for item in repo.consumer_snapshot()['accounts'])
    and not repo.consumer_snapshot()['positions'])

def authority(directory):
 import live_authority_message_evidence_selftest_v0_1 as am
 from phase5.shadow_venue_route_quote_v0_1 import RpcAccountV01
 # Start from an empty baseline, then request the current Token2022 program.
 f,_,scenario=am.a3.actual_fixture(directory,'fresh-message',program=sf.TOKEN_PROGRAM_ID)
 with closing(f):
  am.a3.install(f,'fresh-message-costs',costs={"setup_outflow_lamports":10000000,"refundable_account_lock_lamports":10000000,
   "protective_setup_lamports":10000000,"protective_refundable_lock_lamports":10000000})
  data=mint_bytes(sf.MINT);supply=int.from_bytes(sf.plans.fx.mint_account(sf.TOKEN_2022_PROGRAM_ID).data[36:44],'little')-1
  data=data[:36]+supply.to_bytes(8,'little')+data[44:]
  raw=RpcAccountV01(sf.MINT,sf.TOKEN_2022_PROGRAM_ID,data)
  scenario.accounts[sf.MINT]=am.a3.account(raw.owner,raw.data)
  admission=am.a3.admit(f,'fresh-message-admit',support=am.a3.wallet(f,scenario=scenario,program=raw.owner),program=raw.owner)
  check('fresh_actual_A3_admission',admission.accepted)
  action=f.repo.reservation(f.root).admission.action
  from live.pump_current_state_v0_1 import build_current_pump_state
  # Only this current-metadata synthetic producer uses the versioned factory.
  with patch.object(am.venue,'build_pump_state',build_current_pump_state):
   value,plan=am.evidence(f,action,scenario,original_accounts={sf.MINT:am.original_public(raw)})
  def correct_space(groups):
   for group in groups:
    for row in group['instructions']:
     data=_b58data(row['data'],65536)
     if len(data)==52 and data[:4]==bytes(4) and data[20:]==bytes(Pubkey.from_string(raw.owner)):
      row['data']=sf.b58(data[:12]+struct.pack('<Q',170)+data[20:])
  value=am.with_trace(value,correct_space)
  keys=value.evidence.setup_accounts.requested_keys
  def current_event(groups):
   planned=next(ix for ix in plan.instructions if ix.program_id==sf.PUMP_PROGRAM_ID)
   roles={name:meta.pubkey for (name,_,_),meta in zip(am.PUMP_BUY_ACCOUNTS,planned.accounts)}
   principal=fees=creator_fee=buyback_fee=units=0
   for group in groups:
    for row in group['instructions']:
     data=_b58data(row['data'],65536)
     if keys[row['programIdIndex']]==sf.SYSTEM_PROGRAM_ID and data[:4]==bytes((2,0,0,0)):
      target=keys[row['accounts'][1]];amount=int.from_bytes(data[4:12],'little')
      if target==roles['bonding_curve']:principal+=amount
      elif target==roles['fee_recipient']:fees+=amount
      elif target==roles['creator_vault']:creator_fee+=amount
      elif target==roles['buyback_fee_recipient']:buyback_fee+=amount
     if data[:1]==b'\x0c' and keys[row['accounts'][2]]==roles['associated_base_user']:units=int.from_bytes(data[1:9],'little')
   event_fixture=Fixture();event_fixture.principal=principal;event_fixture.actual_base=units;event_fixture.roles=roles
   for group in groups:
    for row in group['instructions']:
     data=_b58data(row['data'],65536)
     if keys[row['programIdIndex']]==sf.PUMP_PROGRAM_ID and data[:8]==sf.ANCHOR_EVENT_TAG:
      row['data']=sf.b58(event_fixture.trade_event(fee=fees+buyback_fee,creator_fee=creator_fee,buyback_fee=buyback_fee)+bytes(16))
  value=am.with_trace(value,current_event)
  result=am.validate_message_evidence(value)
  if result.disposition!='SUPPORTED_CONTEXT_ONLY':print('fresh message',result)
  check('fresh_exact_A4_zero_signature_simulation',result.disposition=='SUPPORTED_CONTEXT_ONLY')
  for field,start in (('fee_recipient',137),('creator',185)):
   def wrong_event_identity(groups,start=start):
    for group in groups:
     for row in group['instructions']:
      data=_b58data(row['data'],65536)
      if keys[row['programIdIndex']]==sf.PUMP_PROGRAM_ID and data[:16]==sf.ANCHOR_EVENT_TAG+sf.PUMP_TRADE_EVENT_TAG:
       row['data']=sf.b58(data[:start]+bytes(Pubkey.new_unique())+data[start+32:])
   check('current_A4_'+field+'_role_conflict_denied',am.validate_message_evidence(am.with_trace(value,wrong_event_identity)).disposition!='SUPPORTED_CONTEXT_ONLY')
  from live.authority_message_evidence_v0_1 import _rebuild
  state,_,_,_=_rebuild(value)
  check('current_A4_burned_supply_original_bytes',state.current_mint_supply==supply and state.decoded.token_total_supply==supply+1 and state.current_mint_account==raw)
  import live_execution_message_selftest_v0_1 as em
  from live.execution_message_v0_1 import _route_plan
  boundary=em.PublicTransport(value)
  with em.ExecutionReadOnlyRpc('https://test.local',em.a3.PROFILE,now_us=boundary.now,transport=em.httpx.MockTransport(boundary)) as rpc:
   rpc.bind_genesis(f.domain.genesis_hash)
   _,produced,_,_,_,_=_route_plan(value.context,value.evidence.intent,rpc,value.evidence.wallet,value.evidence.quote_policy,value.evidence.plan_policy,value.clock)
  check('current_Execution_and_A4_same_supply_payload',produced.payload()==state.payload())
  from live_simulation_public_cpi_selftest_v0_1 import parsed_fixture
  parsed=am.with_trace(value,lambda groups:parsed_fixture(groups,keys))
  check('current_public_parsed_CPI_A4_context',am.validate_message_evidence(parsed).disposition=='SUPPORTED_CONTEXT_ONLY')
  from live.authority_message_codec_v0_1 import encode_validation_input,decode_validation_input
  check('parsed_original_response_cold_codec',decode_validation_input(encode_validation_input(parsed))==parsed)
  em.install(f,parsed)
  exact,_=em.produce(f,action,parsed)
  check('current_original_Execution_parsed_CPI_and_economic_setup',exact.validation.disposition=='SUPPORTED_CONTEXT_ONLY'
   and len(exact.original.evidence.setup_accounts.requested_keys)<len(keys)
   and exact.original.evidence.simulation.results[0].inner_instructions_json==parsed.evidence.simulation.results[0].inner_instructions_json)
  bad_setup=exact.original.evidence.setup_accounts
  bad_setup=replace(bad_setup,requested_keys=bad_setup.requested_keys[1:],accounts=bad_setup.accounts[1:])
  missing=replace(exact.original,evidence=replace(exact.original.evidence,setup_accounts=bad_setup))
  check('missing_economic_setup_account_denied',am.validate_message_evidence(missing).disposition!='SUPPORTED_CONTEXT_ONLY')
  check('fresh_exact_A4_funding',result.costs.gross_wallet_account_funding_lamports==sf.R and any(row[2:]==(170,raw.owner) for row in result.costs.created_accounts))
  def omit(groups):
   for group in groups:
    group['instructions'][:]=[row for row in group['instructions'] if _b58data(row['data'],65536)!=b'\x16']
  check('fresh_A4_missing_immutable_denied',am.validate_message_evidence(am.with_trace(value,omit)).disposition!='SUPPORTED_CONTEXT_ONLY')
  def simulated_topup(groups):
   for group in groups:
    for row in group['instructions']:
     data=_b58data(row['data'],65536)
     if keys[row['programIdIndex']]==sf.SYSTEM_PROGRAM_ID and data[:4]==bytes((2,0,0,0)):
      row['data']=sf.b58(data[:4]+struct.pack('<Q',int.from_bytes(data[4:12],'little')+1));return
  check('current_A4_unclassified_topup_denied',am.validate_message_evidence(am.with_trace(value,simulated_topup)).disposition!='SUPPORTED_CONTEXT_ONLY')
  legacy=replace(value,evidence=replace(value.evidence,wallet=replace(value.evidence.wallet,observation=replace(value.evidence.wallet.observation,schema=IMMUTABLE_OWNER_SCHEMA))))
  check('fresh_A4_historical_mint_verdict_preserved',am.validate_message_evidence(legacy).disposition!='SUPPORTED_CONTEXT_ONLY')


def crash_child(edge,path):
 binding=sf.LedgerDomain(sf.GENESIS,sf.WALLET,sf.PROFILE.fingerprint,sf.FUNDING,minimum_context_slot=100)
 with sf.LedgerRepository.reopen(path,binding) as repo:
  fixture=Fixture();support,_,_=cf.composed_support(repo,fixture)
  prep=repo.attempt(repo._conn.execute('SELECT attempt_id FROM ledger_attempts').fetchone()[0]).preparation
  commit=repo._commit
  def crash():
   if edge=='before':os._exit(91)
   commit();os._exit(92)
  repo._commit=crash
  cf.apply(repo,prep,support)


def crash_cases(directory):
 results=[]
 for edge in ('before','after'):
  path=directory/('crash-'+edge+'.sqlite3')
  repo,binding,action,prep,chain,support,fixture=cf.first(path)
  repo.close()
  child=subprocess.run([sys.executable,'-B',str(Path(__file__).resolve()),'--crash',edge,str(path)],cwd=ROOT,capture_output=True,text=True,timeout=30)
  check(edge+'_process_cut',child.returncode==(91 if edge=='before' else 92) and not child.stderr)
  with sf.LedgerRepository.reopen(path,binding) as repo:
   check(edge+'_funding_account_position_atomic',bool(repo.consumer_snapshot()['positions'])==(edge=='after') and (repo.audit()['posting_count']>0)==(edge=='after'))
   result=cf.apply(repo,prep,support)
   check(edge+'_retry_exact_once',result.decision.disposition=='FINALIZED_SUCCESS_APPLIED' and repo.audit()['application_receipt_count']==1)
   results.append(result.content_digest)
 check('both_creation_application_crashcuts_same_receipt',results[0]==results[1])

def run():
 with tempfile.TemporaryDirectory(prefix='pump-token2022-lifecycle-') as temporary,patch.object(sf,'Fixture',Fixture),patch.object(sf,'observe',observe),patch.object(cf,'observe',observe):
  directory=Path(temporary)
  lifecycle(directory)
  authority(directory)
  crash_cases(directory)
  failed_prefixes(directory)
  existing_and_topups(directory)
  retained_public_event()
  for name,mutate in [
   ('wrong_space',bad_create(12,struct.pack('<Q',165))),
   ('wrong_program',bad_create(20,bytes(Pubkey.from_string(sf.TOKEN_PROGRAM_ID)))),
   ('wrong_funding',bad_create(4,struct.pack('<Q',sf.R+1))),
   ('missing_size',lambda f:f.groups[0]['instructions'].pop(0)),
   ('missing_immutable',lambda f:f.groups[0]['instructions'].pop(2)),
   ('missing_init',lambda f:f.groups[0]['instructions'].pop()),
   ('wrong_size_extensions',lambda f:f.groups[0]['instructions'][0].update(data=sf.b58(b'\x15'))),
   ('reordered_init',lambda f:f.groups[0]['instructions'].__setitem__(slice(2,4),f.groups[0]['instructions'][2:4][::-1])),
   ('holder_fee',lambda f:event_row(f).update(data=sf.b58(f.trade_event()+struct.pack('<QQ',1,0)))),
   ('unknown_event_suffix',lambda f:event_row(f).update(data=sf.b58(f.trade_event()+bytes(17)))),
  ]:evaluate(directory,name,mutate)
  for schema in (LEGACY_SCHEMA,IMMUTABLE_OWNER_SCHEMA):
   evaluate(directory,schema,support_change=lambda s,schema=schema:replace(s,observation=replace(s.observation,schema=schema)))
  # The old and current exact event layouts remain distinct accepted shapes.
  f=Fixture();check('old_event_layout',not _pump_trade_event(f.trade_event())['current_layout'])
  check('current_normal_zero_holder_layout',_pump_trade_event(f.trade_event()+bytes(16))['current_layout'])
 print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':CHECKS,'check_count':len(CHECKS),'network':False,'signing':False,'production_stores':False},sort_keys=True))
if __name__=='__main__':
 if len(sys.argv)>1 and sys.argv[1]=='--crash':
  with patch.object(sf,'Fixture',Fixture),patch.object(sf,'observe',observe),patch.object(cf,'observe',observe):crash_child(sys.argv[2],Path(sys.argv[3]))
 else:run()
