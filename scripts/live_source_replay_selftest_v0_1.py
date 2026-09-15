"""Differential immutable source codec/replay; local synthetic SQLite only."""
import copy
import hashlib
import json
import sqlite3
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_source_health_selftest_v0_1 as f
from live import source_health_v0_1 as source
from live.evidence_store_v0_1 import SourceEvidenceStore,SourceJournalConflict
checks={}
def check(name,value):
    checks[name]=bool(value);assert value,name
def old_json(value):
    return json.dumps(asdict(value),sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)
def outcome(call):
    try:return ('VALUE',call())
    except Exception as exc:return ('ERROR',type(exc).__name__,str(exc))
def old_replay(store):
    previous=None;sequence=0
    for seq,pred,content,payload in store._conn.execute('SELECT seq,previous_digest,content_digest,payload_json FROM source_evidence ORDER BY seq'):
        sequence+=1;current=source.verdict_from_json(payload)
        expected=source.ZERO_DIGEST if previous is None else previous.content_digest
        if (seq!=sequence or pred!=expected or current.content_digest!=content
            or current.binding!=store.binding or current.profile!=store.profile
            or current.snapshot.previous_verdict_digest!=expected
            or source.evaluate_source(store.binding,store.profile,current.snapshot,previous=previous)!=current):
            raise SourceJournalConflict('source journal evidence chain invalid')
        previous=current
for index,(left,right,expected) in enumerate((
    ({'nested':[{'n':1}]},{'nested':[{'n':True}]},False),
    ({'nested':[{'n':1}]},{'nested':[{'n':1.0}]},False),
    ({'nested':[{'n':1}]},{'nested':[{'n':1}]},True),
    ({'nested':('x',)},{'nested':['x']},False),
    ({'a':None},{'b':None},False),
    ({'u':'雪'},{'u':'雪'},True),
    ({'u':'雪'},{'u':'snow'},False),
    ({'n':None},{'n':False},False))):
    check('deep_raw_equality_'+str(index),source._same_source_raw(left,right) is expected)
with tempfile.TemporaryDirectory(prefix='source-replay-differential-') as directory:
    root=Path(directory);raw=root/'raw.sqlite';binding=f.fixture(raw)
    for n in range(8):f.change(raw,'INSERT INTO gap_jobs_v034 VALUES(?,?,?,?,?,?,?,?,?)',
        ('old-gap-'+str(n),f.at(-3),f.at(-2),'CLOSED',1,2,1,1,''))
    first=f.observe(raw,binding)
    second=f.observe(raw,binding,previous=first,now=13)
    f.change(raw,'INSERT INTO pump_events VALUES (?,?,?,?,?)',('event3','sig3',103,f.LIVE,f.at(11)))
    third=f.observe(raw,binding,previous=second,now=14)
    decoder=source._SourceReplayDecoder(allow_append_prefix=True)
    prior=None
    for index,value in enumerate((first,second,third)):
        old=old_json(value);read,digest=decoder.decode(old)
        check('canonical_bytes_'+str(index),source.canonical_json(value)==old)
        check('original_decoded_values_and_hash_'+str(index),read==source.verdict_from_json(old)==value
            and digest==hashlib.sha256(old.encode()).hexdigest()==value.content_digest)
        if prior is not None:
            check('unchanged_gap_array_reused_'+str(index),read.progress.gaps is prior.progress.gaps)
            check('same_original_transition_'+str(index),source.evaluate_source(binding,value.profile,value.snapshot,previous=prior)==
                source._evaluate_source(binding,value.profile,value.snapshot,previous=prior,previous_digest=prior.content_digest))
        prior=read
    base=json.loads(old_json(second))
    variants=[]
    for path,replacement in ((('progress','gaps',0,'status'),'CHANGED'),(('progress','gaps',0,'rowid'),-1),
        (('snapshot','observed_at_utc'),'invalid'),(('progress','gaps',0,'end_utc'),f.at(-4)),
        (('snapshot','previous_verdict_digest'),'z'*64),(('progress','connection_state'),'LOST')):
        changed=copy.deepcopy(base);cursor=changed
        for key in path[:-1]:cursor=cursor[key]
        cursor[path[-1]]=replacement;variants.append(changed)
    normalized=copy.deepcopy(base);normalized['snapshot']['observed_at_utc']=normalized['snapshot']['observed_at_utc'].replace('+00:00','Z')
    variants.append(normalized)
    for index,value in enumerate(variants):
        payload=json.dumps(value,indent=3)
        expected=outcome(lambda:source.verdict_from_json(payload))
        actual=outcome(lambda:decoder.decode(payload)[0])
        check('changed_or_malformed_array_'+str(index),actual==expected)
        if actual[0]=='VALUE':check('normalized_hash_'+str(index),decoder.decode(payload)[1]==expected[1].content_digest)
    store=SourceEvidenceStore(root/'evidence.sqlite',binding,first.profile)
    previous=source.ZERO_DIGEST
    for item in (first,second,third):store.append(item,expected_previous_digest=previous);previous=item.content_digest
    check('original_and_optimized_complete_history',outcome(lambda:old_replay(store))==outcome(store._verify_history)==('VALUE',None))
    store._conn.execute('DROP TRIGGER source_evidence_no_update')
    payload=json.loads(source.canonical_json(second));payload['progress']['gaps'][0]['events_recovered']+=1
    changed=source.verdict_from_json(json.dumps(payload))
    store._conn.execute('UPDATE source_evidence SET content_digest=?,payload_json=? WHERE seq=2',
        (changed.content_digest,source.canonical_json(changed)))
    check('same_connection_changed_transition_detected',outcome(lambda:old_replay(store))==outcome(store._verify_history)
        ==('ERROR','SourceJournalConflict','source journal evidence chain invalid'))
    store._conn.execute('UPDATE source_evidence SET content_digest=?,payload_json=? WHERE seq=2',
        (second.content_digest,source.canonical_json(second)))
    check('restored_scan_not_cached',outcome(store._verify_history)==('VALUE',None))
    store.close();store=SourceEvidenceStore(root/'evidence.sqlite',binding,first.profile)
    check('cold_original_latest_and_count',store.count()==3 and store.latest()==third);store.close()
    f.change(raw,'INSERT INTO gap_jobs_v034 VALUES(?,?,?,?,?,?,?,?,?)',
        ('new-historical-gap',f.at(-3),f.at(-2),'CLOSED',1,2,1,1,''))
    fourth=f.observe(raw,binding,previous=third,now=15)
    prefix=source._SourceReplayDecoder(allow_append_prefix=True)
    old,_=prefix.decode(old_json(third));new,current_hash=prefix.decode(old_json(fourth))
    check('appended_gap_original_value_and_hash',new==fourth and current_hash==fourth.content_digest)
    check('complete_equal_old_prefix_reuses_facts',all(left is right for left,right in zip(old.progress.gaps,new.progress.gaps)))
    ordinary=source._SourceReplayDecoder();old,_=ordinary.decode(old_json(third));new,_=ordinary.decode(old_json(fourth))
    check('ledger_default_decoder_does_not_enable_prefix_path',all(left is not right for left,right in zip(old.progress.gaps,new.progress.gaps)))
    variants=[];original_fourth=json.loads(old_json(fourth))
    for action in ('middle','first','reordered','deleted','unicode','bad_date'):
        changed=copy.deepcopy(original_fourth);gaps=changed['progress']['gaps']
        if action=='middle':gaps[3]['events_recovered']+=1
        elif action=='first':gaps[0]['status']='CHANGED'
        elif action=='reordered':gaps[1],gaps[2]=gaps[2],gaps[1]
        elif action=='deleted':del gaps[2]
        elif action=='unicode':gaps[2]['gap_key']='old 雪 \\ "'
        else:gaps[2]['end_utc']='invalid-old-date'
        decoder=source._SourceReplayDecoder(allow_append_prefix=True);decoder.decode(old_json(third))
        payload=json.dumps(changed,ensure_ascii=False)
        expected=outcome(lambda:source.verdict_from_json(payload));actual=outcome(lambda:decoder.decode(payload)[0])
        check('exact_prefix_adversarial_'+action,actual==expected)
        if actual[0]=='VALUE':check('exact_prefix_adversarial_hash_'+action,decoder.decode(payload)[1]==actual[1].content_digest)
    for enabled in (False,True):
        for appended in (False,True):
            for corrupt in (True,1.0):
                decoder=source._SourceReplayDecoder(allow_append_prefix=enabled)
                decoder.decode(old_json(third))
                changed=json.loads(old_json(fourth if appended else third))
                changed['progress']['gaps'][0]['events_recovered']=corrupt
                payload=json.dumps(changed)
                original=outcome(lambda:source.verdict_from_json(payload))
                actual=outcome(lambda:decoder.decode(payload))
                check('numeric_type_tamper_'+str(enabled)+'_'+str(appended)+'_'+type(corrupt).__name__,
                    actual==original and actual[0]=='ERROR')
    with SourceEvidenceStore(root/'evidence.sqlite',binding,first.profile) as store:
        store._conn.execute('DROP TRIGGER source_evidence_no_update')
        for corrupt in (True,1.0):
            changed=json.loads(old_json(second));changed['progress']['gaps'][0]['events_recovered']=corrupt
            store._conn.execute('UPDATE source_evidence SET payload_json=? WHERE seq=2',(json.dumps(changed),))
            expected=outcome(lambda:old_replay(store));actual=outcome(store._verify_history)
            check('unchanged_digest_cannot_hide_numeric_payload_tamper_'+type(corrupt).__name__,
                actual==expected and actual[0]=='ERROR')
            store._conn.execute('UPDATE source_evidence SET payload_json=? WHERE seq=2',(old_json(second),))
print(json.dumps({'checks':len(checks),'failed':[key for key,value in checks.items() if not value]}))
