"""Differential complete receipt validation; isolated SQLite, no caching."""
import json
import sqlite3
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from live.operations_degradation_v0_1 import digest,_validate_receipt_digests

checks={}
def outcome(call):
    try:call();return None
    except ValueError as exc:return str(exc)
def check(name,value):
    checks[name]=bool(value)
    assert value,name
with sqlite3.connect(':memory:') as conn:
    conn.execute('CREATE TABLE receipts(digest TEXT PRIMARY KEY NOT NULL)')
    def original():
        for row in conn.execute('SELECT digest FROM receipts'):digest(row[0])
    values=['a'*64,'0'*64,'f'*64,'A'*64,'g'+'1'*63,'1'*63,'1'*65,
        '\0'+'1'*63,'1'*63+'\0','1'*64+'\0junk','é'*64,'é'*32,
        '１'*64,b'a'*64,b'\xff'*64,123,-1,1.5,'1'*1000000]
    for index,value in enumerate(values):
        conn.execute('DELETE FROM receipts');conn.execute('INSERT INTO receipts VALUES (?)',(value,))
        check('same_original_result_'+str(index),outcome(original)==outcome(lambda:_validate_receipt_digests(conn)))
    conn.execute('DELETE FROM receipts')
    check('empty',outcome(lambda:_validate_receipt_digests(conn)) is None)
    conn.executemany('INSERT INTO receipts(rowid,digest) VALUES (?,?)',
        ((i,format(i+2**63,'064x')) for i in range(-4200,4200)))
    conn.execute('INSERT INTO receipts(rowid,digest) VALUES (?,?)',(-2**63,'e'*64))
    conn.execute('INSERT INTO receipts(rowid,digest) VALUES (?,?)',(2**63-1,'f'*64))
    check('all_signed_rowids_and_multiple_blocks',outcome(original)==outcome(lambda:_validate_receipt_digests(conn)) is None)
    for rowid in (-2**63,-4096,0,4096,2**63-1):
        old=conn.execute('SELECT digest FROM receipts WHERE rowid=?',(rowid,)).fetchone()[0]
        conn.execute('UPDATE receipts SET digest=? WHERE rowid=?',('BAD'+str(rowid),rowid))
        check('same_connection_tamper_'+str(rowid),outcome(original)==outcome(lambda:_validate_receipt_digests(conn))
            =='OPERATIONS_DEGRADATION_DIGEST_REQUIRED')
        conn.execute('UPDATE receipts SET digest=? WHERE rowid=?',(old,rowid))
    check('restored_without_cache',outcome(lambda:_validate_receipt_digests(conn)) is None)
print(json.dumps({'checks':len(checks),'failed':[key for key,value in checks.items() if not value]}))
