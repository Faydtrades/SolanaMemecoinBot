"""Focused public parsed-CPI and complete economic setup coverage checks."""
from pathlib import Path
import copy
import json
import struct
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from solders.message import Message
from solders.pubkey import Pubkey
from live.simulation_public_cpi_v0_1 import semantic_instruction
from live.authority_message_evidence_v0_1 import _simulation_inner,economic_setup_keys
from live.transaction_evidence_v0_1 import _b58data
from phase5 import shadow_venue_route_quote_v0_1 as venue
CHECKS={}

def check(name,value):
 CHECKS[name]=bool(value)
 if not value:raise AssertionError(name)

def denied(fn):
 try:fn()
 except ValueError:return True
 return False

def parsed_fixture(groups,keys):
 """Encode the independently constructed test CPI facts like public RPC."""
 for group in groups:
  for row in group['instructions']:
   pid=keys[row['programIdIndex']];accounts=tuple(keys[i] for i in row['accounts']);data=_b58data(row['data'],65536)
   replacement={'programId':pid,'stackHeight':row['stackHeight']}
   kind=info=None
   if pid==venue.SYSTEM_PROGRAM_ID:
    replacement['program']='system'
    if data[:4]==bytes(4):
     kind='createAccount';info=dict(source=accounts[0],newAccount=accounts[1],lamports=int.from_bytes(data[4:12],'little'),space=int.from_bytes(data[12:20],'little'),owner=str(Pubkey.from_bytes(data[20:])))
    elif data[:4]==struct.pack('<I',2):
     kind='transfer';info=dict(source=accounts[0],destination=accounts[1],lamports=int.from_bytes(data[4:],'little'))
   elif pid in (venue.TOKEN_PROGRAM_ID,venue.TOKEN_2022_PROGRAM_ID):
    replacement['program']='spl-token'
    if data[:1]==b'\x15':
     kind='getAccountDataSize';info={'mint':accounts[0]}
     if data[1:]:
      assert data[1:]==b'\x07\x00';info['extensionTypes']=['immutableOwner']
    elif data==b'\x16':kind='initializeImmutableOwner';info={'account':accounts[0]}
    elif data[:1]==b'\x12':kind='initializeAccount3';info=dict(account=accounts[0],mint=accounts[1],owner=str(Pubkey.from_bytes(data[1:])))
    elif data[:1]==b'\x0c':
     units=int.from_bytes(data[1:9],'little');decimals=data[9];text=str(units).zfill(decimals+1)
     ui=(text[:-decimals]+'.'+text[-decimals:]).rstrip('0').rstrip('.') if decimals else text
     kind='transferChecked';info=dict(source=accounts[0],mint=accounts[1],destination=accounts[2],authority=accounts[3],tokenAmount=dict(amount=str(units),decimals=decimals,uiAmount=float(ui),uiAmountString=ui))
    elif data[:1]==b'\x03':kind='transfer';info=dict(source=accounts[0],destination=accounts[1],authority=accounts[2],amount=str(int.from_bytes(data[1:],'little')))
    elif data==b'\x11':kind='syncNative';info={'account':accounts[0]}
    elif data==b'\x09':kind='closeAccount';info=dict(account=accounts[0],destination=accounts[1],owner=accounts[2])
   if kind is not None:replacement['parsed']={'type':kind,'info':info}
   else:
    replacement.pop('program',None);replacement.update(accounts=list(accounts),data=row['data'])
   row.clear();row.update(replacement)

def main():
 witness=json.loads((ROOT/'docs/live/MEME_LIVE_FIX2_PUBLIC_BUY_CPI_WITNESS_V1.json').read_text())
 message=Message.from_bytes(bytes.fromhex(witness['message_hex']));keys=tuple(map(str,message.account_keys))
 groups=json.loads(witness['inner_instructions_json'])
 rows,indexes=_simulation_inner(witness['inner_instructions_json'],len(keys),len(message.instructions),public_keys=keys)
 check('actual_public_parsed_and_partial_CPI',len(rows)==12 and indexes==(2,3))
 creates=[ix for ix in rows if keys[ix.program_id_index]==venue.SYSTEM_PROGRAM_ID and ix.data[:4]==bytes(4)]
 check('actual_two_account_creations',[(int.from_bytes(ix.data[4:12],'little'),int.from_bytes(ix.data[12:20],'little')) for ix in creates]==[(1513840,170),(1346200,137)])
 transfers=[int.from_bytes(ix.data[1:9],'little') for ix in rows if keys[ix.program_id_index]==venue.TOKEN_2022_PROGRAM_ID and ix.data[:1]==b'\x0c']
 check('actual_simulated_units_exact_integer',transfers==[3285876950728])
 check('historical_compiled_only_verdict_unchanged',denied(lambda:_simulation_inner(witness['inner_instructions_json'],len(keys),len(message.instructions))))
 for label,mutate in (
  ('extension',lambda r:r['parsed']['info'].update(extensionTypes=['transferHook'])),
  ('wrong_program',lambda r:r.update(programId=venue.SYSTEM_PROGRAM_ID)),
  ('unknown_type',lambda r:r['parsed'].update(type='transferFee')),
  ('extra_info',lambda r:r['parsed']['info'].update(signers=[])),
  ('missing_mint',lambda r:r['parsed']['info'].pop('mint')),
  ('unknown_key',lambda r:r['parsed']['info'].update(mint=str(Pubkey.new_unique()))),
 ):
  row=copy.deepcopy(groups[0]['instructions'][0]);mutate(row)
  check(label+'_denied',denied(lambda:semantic_instruction(row,keys)))
 transfer=copy.deepcopy(groups[1]['instructions'][2]);assert transfer['parsed']['type']=='transferChecked'
 for amount in ('-1','01','18446744073709551616',1,True):
  bad=copy.deepcopy(transfer);bad['parsed']['info']['tokenAmount']['amount']=amount
  check('invalid_amount_'+str(amount),denied(lambda:semantic_instruction(bad,keys)))
 # Decorative floating UI values never override exact integer amount.
 changed=copy.deepcopy(transfer);changed['parsed']['info']['tokenAmount']['uiAmount']=0.0
 check('UI_float_not_economic_truth',semantic_instruction(changed,keys)==semantic_instruction(transfer,keys))
 header=(message.header.num_required_signatures,message.header.num_readonly_signed_accounts,message.header.num_readonly_unsigned_accounts)
 selected=economic_setup_keys(keys,header)
 check('all_writable_and_signer_keys_retained',all(key in selected for i,key in enumerate(keys) if i<header[0] or i<len(keys)-header[2]))
 check('legacy_program_code_omitted',venue.TOKEN_PROGRAM_ID not in selected)
 check('unknown_readonly_key_retained',economic_setup_keys(('unknown',venue.TOKEN_PROGRAM_ID),(0,0,2))==('unknown',))
 check('program_writable_or_signer_never_omitted',economic_setup_keys((venue.TOKEN_PROGRAM_ID,),(1,0,0))==(venue.TOKEN_PROGRAM_ID,))
 print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(CHECKS),'all_checks':all(CHECKS.values()),'network':False}))

if __name__=='__main__':main()
