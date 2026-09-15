"""Real SQLite pinned-reader/work-boundary cases; isolated disposable stores."""
import hashlib, json, sqlite3, sys, tempfile
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from live.t010_sqlite_boundary_v0_1 import checkpoint_owned_stores
CHECKS={}
def check(name, value):
    CHECKS[name]=bool(value);assert value,name
def semantics(conn):
    rows=[(identity,payload.hex()) for identity,payload in conn.execute('SELECT * FROM history ORDER BY id')]
    return hashlib.sha256(json.dumps(rows,separators=(',',':')).encode()).hexdigest()
def main():
    with tempfile.TemporaryDirectory(prefix='t010-sqlite-boundary-') as tmp, ExitStack() as cleanup:
        paths=[Path(tmp)/f'{n}.sqlite' for n in ('ledger','source','producer')]
        connections=[]
        for path in paths:
            c=sqlite3.connect(path);cleanup.callback(c.close);c.execute('PRAGMA journal_mode=WAL');c.execute('CREATE TABLE history(id INTEGER PRIMARY KEY,payload BLOB)')
            c.execute('INSERT INTO history VALUES(1,?)',(b'a'*8192,));c.commit();connections.append(c)
            c.execute('PRAGMA busy_timeout=173')
        runtime=SimpleNamespace(capability='NO_BROADCAST',ledger=SimpleNamespace(_conn=connections[0]),source=SimpleNamespace(_conn=connections[1]),producer=SimpleNamespace(conn=connections[2]))
        original=[semantics(c) for c in connections]
        check('all_original_wal_checkpoints_complete',checkpoint_owned_stores(runtime)[0])
        check('canonical_rows_unchanged',original==[semantics(c) for c in connections])
        check('success_preserves_original_busy_timeouts',all(c.execute('PRAGMA busy_timeout').fetchone()==(173,) for c in connections))
        reader=sqlite3.connect(paths[1]);cleanup.callback(reader.close);reader.execute('BEGIN');reader.execute('SELECT * FROM history').fetchall()
        c=connections[1];c.execute('INSERT INTO history VALUES(2,?)',(b'b'*8192,));c.commit()
        ok,reason,rows=checkpoint_owned_stores(runtime)
        check('reader_pinned_one_new_commit_denies',not ok and reason=='OWNED_SQLITE_CHECKPOINT_INCOMPLETE' and rows['source']['frames']>rows['source']['checkpointed'])
        check('pinned_reader_not_interrupted',reader.in_transaction and len(reader.execute('SELECT * FROM history').fetchall())==1)
        check('busy_denial_restores_original_timeout',c.execute('PRAGMA busy_timeout').fetchone()==(173,))
        reader.rollback();reader.close()
        check('reader_release_original_checkpoint_succeeds',checkpoint_owned_stores(runtime)[0])
        c.execute('UPDATE history SET payload=? WHERE id=2',(b'd'*8192,));c.commit()
        tail=sqlite3.connect(paths[1]);cleanup.callback(tail.close)
        tail.execute('BEGIN');tail_rows=tail.execute('SELECT * FROM history').fetchall()
        passive=c.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
        check('tail_reader_can_allow_complete_passive_backfill',passive[0]==0 and passive[1]==passive[2])
        ok,reason,rows=checkpoint_owned_stores(runtime)
        check('complete_passive_is_not_accepted_as_restart',not ok and reason=='OWNED_SQLITE_CHECKPOINT_INCOMPLETE' and rows['source']['busy']==1)
        check('tail_reader_remains_unchanged',tail.in_transaction and tail.execute('SELECT * FROM history').fetchall()==tail_rows)
        tail.close()
        check('tail_release_allows_wal_reuse_boundary',checkpoint_owned_stores(runtime)[0])
        c.execute('BEGIN IMMEDIATE');c.execute('INSERT INTO history VALUES(3,?)',(b'c',))
        ok,reason,_=checkpoint_owned_stores(runtime)
        check('active_transaction_denied_without_rollback',not ok and reason=='OWNED_SQLITE_CHECKPOINT_UNAVAILABLE' and c.in_transaction and c.execute('SELECT count(*) FROM history').fetchone()[0]==3)
        check('exception_restores_original_timeout',c.execute('PRAGMA busy_timeout').fetchone()==(173,))
        c.rollback()
        check('caller_rollback_restores_original_semantics',c.execute('SELECT count(*) FROM history').fetchone()[0]==2)
        other=sqlite3.connect(':memory:');cleanup.callback(other.close)
        check('wrong_journal_mode_denied',not checkpoint_owned_stores(SimpleNamespace(capability='NO_BROADCAST',ledger=SimpleNamespace(_conn=other),source=runtime.source,producer=runtime.producer))[0])
        wrong_page=sqlite3.connect(Path(tmp)/'wrong-page.sqlite');cleanup.callback(wrong_page.close)
        wrong_page.execute('PRAGMA page_size=8192');wrong_page.execute('PRAGMA journal_mode=WAL')
        wrong_page.execute('CREATE TABLE history(id INTEGER PRIMARY KEY,payload BLOB)')
        check('unqualified_page_geometry_denied',checkpoint_owned_stores(SimpleNamespace(capability='NO_BROADCAST',ledger=SimpleNamespace(_conn=wrong_page),source=runtime.source,producer=runtime.producer))[1]=='OWNED_SQLITE_PHYSICAL_MODEL_CONFLICT')
        with patch('live.t010_sqlite_boundary_v0_1.sqlite3.sqlite_version','UNREVIEWED'):
            check('unqualified_engine_denied',checkpoint_owned_stores(runtime)[1]=='OWNED_SQLITE_QUALIFIED_ENGINE_REQUIRED')
        for conn in connections:
            check('integrity_'+str(len(CHECKS)),conn.execute('PRAGMA integrity_check').fetchone()[0]=='ok');conn.close()
    print(json.dumps({'scope':'ISOLATED_ORIGINAL_SQLITE_BOUNDARY','checks':CHECKS,'count':len(CHECKS),'T010':'NOT_STARTED'},sort_keys=True))
if __name__=='__main__':main()
