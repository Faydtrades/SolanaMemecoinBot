"""Finite metadata-only mint contract and unchanged historical verdicts. Offline."""
from pathlib import Path
from dataclasses import replace, asdict
import sys,json,base64,struct
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live.pump_token2022_profile_v0_1 import parse_pump_token2022_mint
from live.wallet_evidence_v0_1 import SCHEMA,IMMUTABLE_OWNER_SCHEMA,LEGACY_SCHEMA,WalletEvidenceRequest,ExpectedTokenAccount,assess_wallet_accounts
from live.ledger_evidence_codec_v0_1 import wallet_observation_from_json,wallet_observation_to_json
from live_ledger_baseline_selftest_v0_1 import observe
import live_wallet_evidence_selftest_v0_1 as f
from phase5.shadow_venue_route_quote_v0_1 import RpcAccountV01,TOKEN_2022_PROGRAM_ID,TOKEN_PROGRAM_ID
from solders.pubkey import Pubkey
CHECKS={}
def check(name,value):
 CHECKS[name]=bool(value);assert value,name

def mint_bytes(mint,name='normal',symbol='N',uri='https://example.invalid/metadata',extras=()):
 def string(s):
  b=s.encode();return struct.pack('<I',len(b))+b
 key=bytes(Pubkey.from_string(mint));base=bytearray(82);base[36:44]=(1000000000000000).to_bytes(8,'little');base[44:46]=bytes((6,1))
 pointer=bytes(32)+key;metadata=bytes(32)+key+string(name)+string(symbol)+string(uri)+struct.pack('<I',len(extras))+b''.join(string(k)+string(v) for k,v in extras)
 return bytes(base)+bytes(83)+bytes((1,))+struct.pack('<HH',18,len(pointer))+pointer+struct.pack('<HH',19,len(metadata))+metadata

def run():
 witness=json.loads((ROOT/'docs/live/MEME_LIVE_FIX2_PUMP_MINT_WITNESS_V1.json').read_text());a=witness['account']
 raw=base64.b64decode(a['data_base64']);account=RpcAccountV01(a['pubkey'],a['owner'],raw)
 parsed=parse_pump_token2022_mint(account)
 check('actual_witness_exact_metadata_pair',parsed.extensions==(18,19) and parsed.mint==a['pubkey'] and len(raw)==409)
 check('actual_witness_metadata_relationship',parsed.metadata_mint==parsed.metadata_pointer==a['pubkey'] and parsed.decimals==6)
 for n in (0,1,10,117,1000):
  data=mint_bytes(f.MINT22,name='x'*n,extras=(('field','value'),))
  check('variable_length_'+str(n),parse_pump_token2022_mint(RpcAccountV01(f.MINT22,TOKEN_2022_PROGRAM_ID,data)).name=='x'*n)
 data=mint_bytes(f.MINT22);key=bytes(Pubkey.from_string(f.MINT22))
 changes={'truncated':data[:-1],'header_truncated':data[:165],'nonzero_padding':data[:82]+b'X'+data[83:],
 'wrong_account_type':data[:165]+b'\x02'+data[166:],'unknown_extension':data+struct.pack('<HH',65535,0),
 'transfer_fee':data+struct.pack('<HH',1,108)+bytes(108),'transfer_hook':data+struct.pack('<HH',14,64)+bytes(64),
 'permanent_delegate':data+struct.pack('<HH',12,32)+bytes(32),'nontransferable':data+struct.pack('<HH',9,0),
 'default_account_state':data+struct.pack('<HH',6,1)+b'\x01','duplicate_tlv':data+data[166:234],
 'trailing_zero':data+b'\0','pointer_mutable':data[:170]+key+data[202:],
 'wrong_pointer':data[:202]+bytes(32)+data[234:],'wrong_metadata_mint':data[:270]+bytes(32)+data[302:],
 'mutable_metadata':data[:238]+key+data[270:],'wrong_decimals':data[:44]+b'\x09'+data[45:],
 'mint_authority':b'\x01\x00\x00\x00'+key+data[36:],'freeze_authority':data[:46]+b'\x01\x00\x00\x00'+key+data[82:],
 'uninitialized':data[:45]+b'\0'+data[46:],'oversized_string':data[:302]+b'\xff'*4+data[306:],
 'duplicate_metadata_key':mint_bytes(f.MINT22,extras=(('x','1'),('x','2')))}
 for name,value in changes.items():
  try:parse_pump_token2022_mint(RpcAccountV01(f.MINT22,TOKEN_2022_PROGRAM_ID,value));denied=False
  except ValueError:denied=True
  check('denied_'+name,denied)
 for name,account in [('wrong_program',RpcAccountV01(f.MINT22,TOKEN_PROGRAM_ID,data)),('wrong_mint',RpcAccountV01(f.MINT,TOKEN_2022_PROGRAM_ID,data))]:
  try:parse_pump_token2022_mint(account);denied=False
  except ValueError:denied=True
  check(name,denied)
 request=WalletEvidenceRequest(f.WALLET,f.GENESIS,100,(ExpectedTokenAccount(f.TOKEN22,f.MINT22,TOKEN_2022_PROGRAM_ID),))
 s=f.Scenario().add_token(token=f.TOKEN22,mint=f.MINT22,program=TOKEN_2022_PROGRAM_ID,mint_bytes=data,data=f.token_data(f.MINT22,tail=bytes.fromhex('0207000000')))
 obs=observe(s,request=request);check('current_original_adapter_supported',obs.schema==SCHEMA and assess_wallet_accounts(obs).supported_shape=='SUPPORTED')
 check('codec_roundtrip',wallet_observation_from_json(wallet_observation_to_json(obs))==obs)
 for schema in (LEGACY_SCHEMA,IMMUTABLE_OWNER_SCHEMA):
  old=replace(obs,schema=schema);assessment=assess_wallet_accounts(wallet_observation_from_json(wallet_observation_to_json(old)))
  check('historical_'+schema,assessment.supported_shape=='UNSUPPORTED' and 'UNSUPPORTED_MINT_EXTENSIONS_OR_LENGTH' in assessment.reasons and old.content_digest!=obs.content_digest)
 print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':CHECKS,'check_count':len(CHECKS),'actual_witness_classification':asdict(parsed),'network':False,'T010':False},sort_keys=True))
if __name__=='__main__':run()
