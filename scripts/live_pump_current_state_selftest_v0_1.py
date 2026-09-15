"""Finite synthetic state cases and exact retained CPI arithmetic; no network."""
from pathlib import Path
from dataclasses import replace
import json,sys,subprocess,types
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from phase5 import shadow_venue_route_quote_v0_1 as v
import phase5_shadow_venue_route_quote_selftest_v0_1 as f
from live.pump_current_state_v0_1 import *
from live.wallet_evidence_v0_1 import SCHEMA, IMMUTABLE_OWNER_SCHEMA, LEGACY_SCHEMA
from live_pump_token2022_evidence_selftest_v0_1 import mint_bytes
CHECKS={}
def check(name,value):
 CHECKS[name]=bool(value);assert value,name
def denied(name,call):
 try:call()
 except (ValueError,TypeError):check(name,True)
 else:check(name,False)
def mint(supply=999999966857460):
 raw=mint_bytes(f.MINT);return v.RpcAccountV01(f.MINT,v.TOKEN_2022_PROGRAM_ID,raw[:36]+supply.to_bytes(8,'little')+raw[44:])
def run():
 intent=f.intent();kwargs=dict(slot_min=f.SLOT,slot_max=f.SLOT,observed_at_us=f.BASE_US+10)
 curve=f.account(v.derive_bonding_curve_pda(f.MINT),v.PUMP_PROGRAM_ID,
  f.curve_data(virtual_token=1031665353528410,virtual_quote=31201978855,
  real_token=751765353528410,supply=1000000000000000))
 global_raw=f.account(v.derive_pump_global_pda(),v.PUMP_PROGRAM_ID,f.pump_global_data());fee=f.fee_account(v.PUMP_PROGRAM_ID)
 args=(intent,curve,mint(),global_raw,fee);state=build_current_pump_state(*args,**kwargs)
 check('raw_initial_supply_retained',state.decoded.token_total_supply==1000000000000000)
 check('raw_current_supply_retained',state.current_mint_supply==999999966857460 and state.current_mint_account==args[2])
 check('current_profile_hash_bound',state.payload()['schema_version']==STATE_SCHEMA and state.payload()['fee_supply_basis']=='CURRENT_RAW_MINT_SUPPLY' and state.fingerprint==v.content_fingerprint(state.payload()))
 check('actual_public_CPI_exact_marketcap_basis',v._selected_fees(state,amount=100000000)[1]==30244281941)
 check('initial_supply_does_not_match_CPI',state.decoded.virtual_quote_reserves*state.decoded.token_total_supply//state.decoded.virtual_token_reserves==30244282943)
 for supply,label in [(0,'zero'),(1000000000000001,'increase'),(700000000000000,'below_real_reserves')]:
  denied(label,lambda supply=supply:build_current_pump_state(intent,curve,mint(supply),global_raw,fee,**kwargs))
 for offset,data,label in [(0,b'\x01\0\0\0','mint_authority'),(46,b'\x01\0\0\0','freeze_authority'),(44,b'\x09','decimals'),(170,b'\x01','metadata_pointer_authority')]:
  bad=replace(args[2],data=args[2].data[:offset]+data+args[2].data[offset+len(data):]);denied(label,lambda bad=bad:build_current_pump_state(intent,curve,bad,global_raw,fee,**kwargs))
 denied('wrong_mint_identity',lambda:validate_current_pump_supply(curve,replace(args[2],pubkey=f.CREATOR),f.MINT))
 denied('wrong_curve_owner',lambda:build_current_pump_state(intent,replace(curve,owner=v.TOKEN_PROGRAM_ID),args[2],global_raw,fee,**kwargs))
 denied('wrong_fee_owner',lambda:build_current_pump_state(intent,curve,args[2],global_raw,replace(fee,owner=v.TOKEN_PROGRAM_ID),**kwargs))
 denied('slot_mismatch',lambda:build_current_pump_state(*args,**kwargs,account_slots=(f.SLOT+1,)*4))
 denied('mint_raw_evidence_substitution',lambda:replace(state,current_mint_account=mint(999999966857459)))
 denied('historical_builder_burn_difference_still_denied',lambda:v.build_pump_state(*args,**kwargs))
 for schema in (LEGACY_SCHEMA,IMMUTABLE_OWNER_SCHEMA):check('historical_dispatch_'+schema,pump_state_builder(schema,args[2]) is v.build_pump_state)
 check('current_extended_dispatch',pump_state_builder(SCHEMA,args[2]) is build_current_pump_state)
 legacy=replace(args[2],owner=v.TOKEN_PROGRAM_ID,data=args[2].data[:82])
 legacy_state=build_current_pump_state(intent,curve,legacy,global_raw,fee,**kwargs)
 check('current_legacy_burn_uses_exact_raw_supply',legacy_state.current_mint_supply==state.current_mint_supply
  and v._selected_fees(legacy_state,amount=100000000)==v._selected_fees(state,amount=100000000)
  and legacy_state.payload()['current_mint_profile']=='PUMP_SOL_LEGACY_SPL_BASE_V1')
 check('current_legacy_dispatch',pump_state_builder(SCHEMA,legacy,curve) is build_current_pump_state)
 check('current_legacy_unchanged_supply_original_factory',pump_state_builder(SCHEMA,
  replace(legacy,data=legacy.data[:36]+state.decoded.token_total_supply.to_bytes(8,'little')+legacy.data[44:]),curve) is v.build_pump_state)
 denied('legacy_extension_not_accepted',lambda:build_current_pump_state(intent,curve,replace(legacy,data=legacy.data+b'\0'),global_raw,fee,**kwargs))
 for offset,label in ((0,'mint'),(46,'freeze')):
  bad=replace(legacy,data=legacy.data[:offset]+b'\x01\0\0\0'+legacy.data[offset+4:])
  denied('legacy_'+label+'_authority_denied',lambda bad=bad:build_current_pump_state(intent,curve,bad,global_raw,fee,**kwargs))
 for schema in (LEGACY_SCHEMA,IMMUTABLE_OWNER_SCHEMA):
  check('historical_legacy_dispatch_'+schema,pump_state_builder(schema,legacy) is v.build_pump_state)
 check('current_base82_dispatch',pump_state_builder(SCHEMA,f.mint_account(v.TOKEN_2022_PROGRAM_ID)) is v.build_pump_state)
 # Execute the immutable checkpoint module only to compare the affected default
 # state/route/quote outputs; this runs no historical campaign or filesystem work.
 old=types.ModuleType('phase5._current_state_historical_reference');old.__package__='phase5';sys.modules[old.__name__]=old
 source=subprocess.check_output(['git','show','2b57b8642d5dc09ac01a7f9de09d7f42bc803fb2:src/phase5/shadow_venue_route_quote_v0_1.py'],cwd=ROOT,text=True)
 exec(compile(source,'historical_reference','exec'),old.__dict__)
 for program in (v.TOKEN_PROGRAM_ID,v.TOKEN_2022_PROGRAM_ID):
  raw_curve=f.account(v.derive_bonding_curve_pda(f.MINT),v.PUMP_PROGRAM_ID,f.curve_data());raw_mint=f.mint_account(program)
  current=v.build_pump_state(intent,raw_curve,raw_mint,global_raw,fee,**kwargs);prior=old.build_pump_state(intent,raw_curve,raw_mint,global_raw,fee,**kwargs)
  check('historical_state_payload_'+program,current.payload()==prior.payload() and current.fingerprint==prior.fingerprint)
  route=v.decide_route(intent,current,None);old_route=old.decide_route(intent,prior,None)
  check('historical_route_'+program,route.payload()==old_route.payload())
  check('historical_quote_'+program,v.create_executable_quote(intent,current,route,v.QuotePolicyV01(100)).payload()==old.create_executable_quote(intent,prior,old_route,old.QuotePolicyV01(100)).payload())
 print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':CHECKS,'check_count':len(CHECKS),'network':False},sort_keys=True))
if __name__=='__main__':run()
