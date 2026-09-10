"""A3 retained collector -> actual A2 -> Ledger -> actual Authority admission.

Only disposable stores and deterministic public external wallet/clock fixtures.
No fabricated candidate, position, settlement, signing or transport invocation.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]
from live.candidate_handoff_v0_1 import CandidateHandoffV01, CONFIG
from live.continuous_producer_v0_2 import LiveContinuousProducerV02, ProducerConflict, TABLES, RETIRED
from live.continuous_producer_v0_1 import connect_producer_db
from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02
from live.source_health_v0_1 import SourceBinding, SourceProfile, CursorWitness, CollectorSourceAdapter, _SELECT, _fact, witness, ZERO_DIGEST
from live.evidence_store_v0_1 import SourceEvidenceStore
from live.ledger_repository_v0_1 import LedgerRepository
from live.ledger_domain_v0_1 import LedgerContractError
import phase4_continuous_firstpullback_binding_selftest_v0_1 as raw_fixture
import live_authority_admission_selftest_v0_1 as admission
import live_authority_controls_selftest_v0_1 as controls
from live_ledger_baseline_selftest_v0_1 import domain, observe, ingest
from live_wallet_evidence_selftest_v0_1 import NOW, utc
from live_ledger_custody_selftest_v0_1 import key

BASE = datetime.fromtimestamp(NOW - 14, timezone.utc)
raw_fixture.BASE = BASE
DATABASE_ID = "A3:SYNTHETIC:RETAINED:COLLECTOR"
MINT_A, MINT_B, MINT_C = key(11), key(12), key(13)
CHECKS = []


def check(name, value):
    if not value:
        raise AssertionError(name)
    CHECKS.append(name)
    print("OK", name)


def reject(name, call):
    try:
        call()
    except (ProducerConflict, LedgerContractError, sqlite3.DatabaseError, RuntimeError):
        check(name, True)
    else:
        check(name, False)


def at(offset):
    return (BASE + timedelta(seconds=offset)).isoformat(timespec="microseconds")


def economic_audit(repo):
    value=repo.audit()
    value.pop("generation")  # Reopen intentionally advances the write fence.
    return value


def build_raw(path):
    with closing(sqlite3.connect(path)) as c, c:
        c.execute(raw_fixture.SCHEMA_SQL)
        c.execute("ALTER TABLE pump_events ADD COLUMN inserted_at_utc TEXT")
        c.executescript("""
        CREATE TABLE collector_events (id INTEGER PRIMARY KEY,event_type TEXT,happened_at_utc TEXT,details_json TEXT);
        CREATE TABLE websocket_observations (signature TEXT PRIMARY KEY,slot INTEGER,received_at_utc TEXT,executed INTEGER,raw_notification_json TEXT);
        CREATE TABLE gap_jobs_v034 (gap_key TEXT PRIMARY KEY,gap_started_at_utc TEXT,gap_ended_at_utc TEXT,status TEXT,before_slot INTEGER,after_slot INTEGER,confirmed_blocks_checked INTEGER,events_recovered INTEGER,last_error TEXT);
        """)
        c.executemany("INSERT INTO collector_events VALUES(?,?,?,?)", (
            (1,"COLLECTOR_V0_3_START",at(-2),"{}"),
            (2,"WS_CONNECTED",at(-1),"{}"), (3,"PUMP_SUBSCRIPTION_ACTIVE",at(0),"{}")))
        raw_fixture.insert_row(c,1,key(14),"BUY")
        c.execute("UPDATE pump_events SET inserted_at_utc=decoded_at_utc")
        c.execute("INSERT INTO websocket_observations VALUES(?,?,?,?,?)",("sig-1",5000001,at(1),1,"{}"))
    with closing(sqlite3.connect(path)) as c:
        anchors = tuple(witness(t,_fact(t,c.execute(f"SELECT {_SELECT[t]} FROM {t} WHERE rowid=1").fetchone()))
                        for t in ("pump_events","collector_events","websocket_observations"))
    return SourceBinding("A3:COLLECTOR:LINEAGE",at(1),anchors)


FIRST = ((4,MINT_A,"LAUNCH",10000),(5,MINT_B,"LAUNCH",10000),
         (6,MINT_A,"BUY",10000),(7,MINT_B,"BUY",10000),
         (8,MINT_A,"BUY",10600),(9,MINT_A,"BUY",10600),(10,MINT_A,"SELL",9400),
         (11,MINT_A,"SELL",9400),(12,MINT_A,"BUY",9700),(13,MINT_A,"BUY",9900),(14,MINT_A,"BUY",10100))
NEXT = tuple((n+14,MINT_C,kind,price) for n,_,kind,price in FIRST if n not in (5,7))


class Fixture:
    def __init__(self, directory, name, *, through=14):
        self.directory, self.name = directory, name
        self.raw, self.path, self.ppath = (directory/(name+s) for s in ("-raw.sqlite","-ledger.sqlite","-producer.sqlite"))
        self.binding = build_raw(self.raw)
        self.domain = domain()
        self.repo = LedgerRepository.initialize(self.path,self.domain)
        ingest(self.repo,observe())
        self.source = SourceEvidenceStore(directory/(name+"-evidence.sqlite"),self.binding,SourceProfile())
        self.append([r for r in FIRST if r[0] <= through])
        self.open_producer(initialize=True)

    def append(self, rows):
        with closing(sqlite3.connect(self.raw)) as c, c:
            for rowid,mint,kind,price in rows:
                raw_fixture.insert_row(c,rowid,mint,kind,price_units=price)
                c.execute("UPDATE pump_events SET inserted_at_utc=decoded_at_utc WHERE rowid=?",(rowid,))
                c.execute("INSERT INTO websocket_observations VALUES(?,?,?,?,?)",(f"sig-{rowid}",5000000+rowid,at(rowid+2),1,"{}"))

    def open_producer(self, *, initialize=False):
        self.conn = connect_producer_db(self.ppath)
        self.producer = LiveContinuousProducerV02(self.conn,ContinuousMarketSourceV02(self.raw,
            start_after_p1_rowid=1,database_identity=DATABASE_ID),wall_clock=lambda:BASE)
        self.handoff = CandidateHandoffV01(self.producer,self.repo,self.binding,database_identity=DATABASE_ID,initialize=initialize)

    def reopen(self):
        self.conn.close()
        self.repo.close()
        self.repo = LedgerRepository.reopen(self.path,self.domain)
        self.open_producer()

    def produce(self):
        while self.producer.process_next_batch(batch_size=4).raw_rows_fetched:
            pass

    def deliver(self, limit=4):
        roots = []
        while True:
            page = self.handoff.deliver_page(limit=limit)
            roots.extend(page.candidate_roots)
            if page.scanned_rows == 0:
                return roots

    def evidence(self, *, offset=18, cut=15):
        previous = self.source.latest_record()
        prior = None if previous is None else previous[1]
        value = CollectorSourceAdapter(self.raw).observe(self.binding,self.source.profile,
            observed_at_utc=at(offset),requested_cut_utc=at(cut),previous=prior)
        self.source.append(value,expected_previous_digest=ZERO_DIGEST if prior is None else prior.content_digest)
        return value

    def configure(self, root):
        self.root, self.item = root, self.repo.candidate(root)
        self.policy = admission.policy_v02(controls.policy(self.item,binding=self.domain))
        controls.control(self.repo,"INSTALL_POLICY","install",policy_value=self.policy)
        controls.arm(self.repo,self.policy)

    def close(self):
        self.conn.close()
        self.repo.close()
        self.source.close()


def transition(directory):
    f = Fixture(directory,"transition",through=13)
    try:
        f.produce()
        check("restart before candidate has no injected inbox", f.deliver()==[])
        lineage = f.producer.source_identity
        f.reopen()
        f.append([FIRST[-1]])
        f.produce()
        original = next(r for r in f.producer.producer_events() if r["event_type"]=="CandidateEvaluationEvent")
        check("actual winner retired with unfinished mint retained", f.conn.execute(f"SELECT COUNT(*) FROM {RETIRED}").fetchone()[0]==1
              and MINT_B in f.producer._engine._tokens)
        roots = f.deliver()
        check("one actual canonical candidate reaches Ledger",len(roots)==1)
        item = f.repo.candidate(roots[0])
        check("original envelope signal time lineage and digest preserved",item.candidate_signal_id==json.loads(original["event_json"])["candidate"]["candidate_id"]
              and item.generated_at_us==NOW*1000000 and item.producer_lineage_id==lineage
              and item.source_record_digest==f.conn.execute(f"SELECT content_fingerprint FROM {TABLES[1]} WHERE input_key='P1:14'").fetchone()[0])
        check("market audit and inputs never acknowledged",f.conn.execute(f"SELECT COUNT(*) FROM {TABLES[5]} WHERE event_type<>'CandidateEvaluationEvent' AND delivered=1").fetchone()[0]==0
              and f.conn.execute(f"SELECT COUNT(*) FROM {TABLES[4]} WHERE delivered=1").fetchone()[0]==0)
        check("real source health supports candidate",f.evidence().disposition=="HEALTHY")
        f.configure(roots[0])
        receipt = admission.admit(f,"candidate-entry")
        print("A3_ADMISSION",receipt.accepted,receipt.eligibility.decision.reasons)
        check("actual Authority admission accepts producer candidate",receipt.accepted)
        binding = f.repo.authority_entry_binding(f.root)
        check("Authority deadline uses original candidate",binding.deadline_us==item.generated_at_us+15000000)
        before = economic_audit(f.repo)
        f.reopen()
        check("restart after ACK exactly repeats original inbox",f.deliver()==roots and f.repo.candidate(roots[0])==item)
        check("duplicate delivery no economic mutation",economic_audit(f.repo)==before)
        duplicate = admission.admit(f,"duplicate-entry")
        check("actual consumer denies second admission without extra reservation",not duplicate.accepted
              and len(f.repo.consumer_snapshot()["reservations"])==1 and f.repo.audit()["action_count"]==1)
        f.append(NEXT)
        f.produce()
        later = f.deliver()
        check("next canonical candidate from retained source distinct root",len(later)==1 and later[0]!=roots[0])
        check("same producer lineage with original next time",f.repo.candidate(later[0]).producer_lineage_id==lineage
              and f.repo.candidate(later[0]).generated_at_us==(NOW+14)*1000000)
        f.root,f.item=later[0],f.repo.candidate(later[0])
        check("current source advances for next candidate",f.evidence(offset=32,cut=29).disposition=="HEALTHY")
        competing=admission.admit(f,"next-entry",at=NOW+18)
        check("next actual candidate reaches actual admission and occupied cap denies",not competing.accepted
              and competing.eligibility.decision.disposition=="ELIGIBLE_CONTEXT_ONLY"
              and len(f.repo.consumer_snapshot()["reservations"])==1 and f.repo.audit()["action_count"]==1)
        f.append([(29,MINT_A,"BUY",10300)])
        f.produce()
        check("retired winner never emits another logical candidate",f.deliver()==[])
        print("A3_CANDIDATES",item.candidate_signal_id,f.repo.candidate(later[0]).candidate_signal_id)
        print("A3_ROOTS",*roots,*later)
        print("A3_ORIGINAL_DEADLINE_US",binding.deadline_us)
        check("temp stores SQLite integrity",all(c.execute("PRAGMA integrity_check").fetchone()[0]=="ok" for c in (f.conn,)))
        check("admission boundary creates no attempt or position",f.repo.audit()["attempt_count"]==0
              and len(f.repo.consumer_snapshot()["positions"])==0)
    finally:
        f.close()


def crash_cases(directory):
    for cut in ("after_capture","before_ledger_receive","after_ledger_commit","before_ack_commit","after_ack_commit"):
        f = Fixture(directory,"crash-"+cut)
        try:
            f.produce()
            def crash(stage):
                if stage==cut:
                    raise RuntimeError("synthetic crash")
            f.handoff.failure_injector=crash
            reject(cut+" injected",lambda:f.handoff.deliver_page())
            ack = f.conn.execute(f"SELECT delivered FROM {TABLES[5]} WHERE event_type='CandidateEvaluationEvent'").fetchone()[0]
            check(cut+" ACK commit boundary",ack==int(cut=="after_ack_commit"))
            f.reopen()
            roots=f.deliver()
            check(cut+" reopen delivers one original candidate",len(roots)==1 and f.repo.candidate(roots[0]).generated_at_us==NOW*1000000)
            before=economic_audit(f.repo)
            f.reopen()
            check(cut+" replay deduplicates committed receipt",f.deliver()==roots and economic_audit(f.repo)==before)
        finally:
            f.close()


def denials(directory):
    for kind in ("stale","gap","interruption","witness"):
        f=Fixture(directory,"deny-"+kind)
        try:
            f.produce()
            roots=f.deliver()
            item=f.repo.candidate(roots[0])
            f.evidence()
            f.configure(roots[0])
            if kind=="gap":
                with closing(sqlite3.connect(f.raw)) as c,c:
                    c.execute("INSERT INTO gap_jobs_v034 VALUES(?,?,?,?,?,?,?,?,?)",("gap",at(8),at(9),"PENDING",1,2,0,0,""))
            if kind=="interruption":
                with closing(sqlite3.connect(f.raw)) as c,c:
                    c.execute("INSERT INTO collector_events VALUES(?,?,?,?)",(4,"WS_DISCONNECTED",at(17),"{}"))
            if kind=="witness":
                with closing(sqlite3.connect(f.raw)) as c,c:
                    c.execute("UPDATE pump_events SET signature='changed' WHERE rowid=1")
            f.evidence(offset=19,cut=15)
            when=NOW+20 if kind=="stale" else NOW+5
            result=admission.admit(f,"deny",at=when)
            print("A3_DENIAL",kind,result.eligibility.decision.disposition,result.eligibility.decision.reasons)
            check(kind+" actual Authority denies ENTRY",not result.accepted)
            check(kind+" original deadline remains unchanged",result.eligibility.decision.binding.deadline_us==item.generated_at_us+15000000)
            if kind=="witness":
                reject("source anchor mutation prevents handoff reopen",lambda:CandidateHandoffV01(f.producer,f.repo,f.binding,database_identity=DATABASE_ID))
        finally:
            f.close()
    f=Fixture(directory,"late-handoff")
    try:
        f.produce()
        f.reopen()  # Original candidate remains pending across producer restart.
        f.producer.wall_clock=lambda:BASE+timedelta(seconds=36)
        roots=f.deliver()
        f.configure(roots[0])
        with closing(sqlite3.connect(f.raw)) as c,c:
            c.execute("INSERT INTO websocket_observations VALUES(?,?,?,?,?)",("fresh-tail",5000035,at(35),0,"{}"))
        check("backlog has currently healthy source",f.evidence(offset=36,cut=34).disposition=="HEALTHY")
        late=admission.admit(f,"late-first-admission",at=NOW+22)
        check("first admission after delayed handoff still expires original candidate",not late.accepted
              and late.eligibility.decision.disposition=="EXPIRED"
              and late.eligibility.decision.binding.deadline_us==(NOW+15)*1000000
              and len(f.repo.consumer_snapshot()["reservations"])==0)
    finally:
        f.close()


def conflicts(directory):
    f=Fixture(directory,"conflicts")
    try:
        f.produce()
        roots=f.deliver()
        other=LedgerRepository.initialize(directory/"other-domain.sqlite",domain(wallet=key(20)))
        try:
            reject("different economic destination rejected",lambda:CandidateHandoffV01(f.producer,other,f.binding,database_identity=DATABASE_ID))
        finally:
            other.close()
        f.reopen()
        empty=LedgerRepository.initialize(directory/"empty-replacement.sqlite",f.domain)
        try:
            h=CandidateHandoffV01(f.producer,empty,f.binding,database_identity=DATABASE_ID)
            reject("same-domain empty replacement cannot trust producer ACK",lambda:h.deliver_page())
        finally:
            empty.close()
        f.reopen()
        reject("explicit database identity mismatch",lambda:CandidateHandoffV01(f.producer,f.repo,f.binding,database_identity="different"))
        reject("SourceBinding rebinding rejected",lambda:CandidateHandoffV01(f.producer,f.repo,replace(f.binding,lineage="other"),database_identity=DATABASE_ID))
        f.reopen()
        second=connect_producer_db(f.ppath)
        try:
            p2=LiveContinuousProducerV02(second,ContinuousMarketSourceV02(f.raw,start_after_p1_rowid=1,database_identity=DATABASE_ID))
            p2.checkpoint()
            reject("stale generation capture denied",lambda:f.handoff.deliver_page())
        finally:
            second.close()
        f.reopen()
        f.conn.execute(f"UPDATE {TABLES[5]} SET event_json='{{}}' WHERE event_type='CandidateEvaluationEvent'")
        f.conn.commit()
        reject("altered original provenance denied before Ledger",lambda:f.handoff.deliver_page())
    finally:
        f.close()


def bounded_anchor(directory):
    f=Fixture(directory,"oversized-anchor")
    try:
        with closing(sqlite3.connect(f.raw)) as c,c:
            c.execute("UPDATE pump_events SET signature=? WHERE rowid=1",("x"*(f.producer.profile.record_bytes+1),))
        materialized=[]
        original_open=CollectorSourceAdapter._open
        def guarded_open(adapter):
            c=original_open(adapter)
            def row_factory(cursor,row):
                if any(type(v) is str and len(v)>f.producer.profile.record_bytes for v in row):
                    materialized.append(True)
                return row
            c.row_factory=row_factory
            return c
        with patch.object(CollectorSourceAdapter,"_open",guarded_open):
            reject("oversized source anchor preflight denies",lambda:f.handoff.deliver_page())
        check("oversized anchor rejected before Python materialization",materialized==[])
    finally:
        f.close()


def missing_binding(directory):
    for acknowledged in (False,True):
        f=Fixture(directory,"missing-binding-"+str(acknowledged))
        try:
            f.produce()
            if acknowledged:
                f.deliver()
            else:
                def lose_ack(stage):
                    if stage=="after_ledger_commit":
                        raise RuntimeError("lost ACK")
                f.handoff.failure_injector=lose_ack
                reject("pending ACK has actual committed Ledger candidate",lambda:f.handoff.deliver_page())
            check("missing-binding fixture has durable candidate "+str(acknowledged),f.repo.audit()["candidate_count"]==1)
            f.conn.execute(f"DROP TABLE {CONFIG}")
            f.conn.commit()
            reject("missing binding reopen rejects "+str(acknowledged),lambda:CandidateHandoffV01(f.producer,f.repo,f.binding,database_identity=DATABASE_ID))
            if acknowledged:
                f.conn.close()
                f.conn=connect_producer_db(f.ppath)
                f.producer=LiveContinuousProducerV02(f.conn,ContinuousMarketSourceV02(f.raw,start_after_p1_rowid=1,database_identity=DATABASE_ID))
                reject("explicit initialization cannot erase ACK destination provenance",lambda:CandidateHandoffV01(f.producer,f.repo,f.binding,database_identity=DATABASE_ID,initialize=True))
        finally:
            f.close()


def concurrent_capture(directory):
    f=Fixture(directory,"concurrent-capture")
    try:
        f.produce()
        def publish(stage):
            if stage=="after_capture":
                with closing(connect_producer_db(f.ppath)) as c:
                    p=LiveContinuousProducerV02(c,ContinuousMarketSourceV02(f.raw,start_after_p1_rowid=1,database_identity=DATABASE_ID))
                    p.checkpoint()
        f.handoff.failure_injector=publish
        reject("concurrent generation after snapshot cannot acknowledge stale owner",lambda:f.handoff.deliver_page())
        check("concurrent capture keeps candidate pending",f.conn.execute(f"SELECT delivered FROM {TABLES[5]} WHERE event_type='CandidateEvaluationEvent'").fetchone()[0]==0)
        f.reopen()
        check("concurrent capture retry deduplicates original durable candidate",len(f.deliver())==1 and f.repo.audit()["candidate_count"]==1)
        reject("delivery page above finite profile rejected",lambda:f.handoff.deliver_page(limit=257))
    finally:
        f.close()


def abrupt_processes(directory):
    for cut in ("ledger-before","ledger-after","ack-before","ack-after"):
        f=Fixture(directory,"abrupt-"+cut)
        try:
            f.produce()
            f.conn.close()
            f.repo.close()
            result=subprocess.run([sys.executable,"-B",str(Path(__file__).resolve()),"--crash",str(f.raw),str(f.ppath),str(f.path),cut],
                cwd=ROOT,capture_output=True,text=True,timeout=30)
            check(cut+" abrupt child reaches exact cut",result.returncode==81)
            f.reopen()
            check(cut+" durable Ledger commit state",f.repo.audit()["candidate_count"]==int(cut!="ledger-before"))
            ack=f.conn.execute(f"SELECT delivered FROM {TABLES[5]} WHERE event_type='CandidateEvaluationEvent'").fetchone()[0]
            check(cut+" durable producer commit state",ack==int(cut=="ack-after"))
            roots=f.deliver()
            check(cut+" fresh process recovery yields one logical candidate",len(roots)==1 and f.repo.audit()["candidate_count"]==1
                  and f.repo.candidate(roots[0]).generated_at_us==NOW*1000000)
        finally:
            f.close()


def crash_child():
    raw,ppath,path,cut=sys.argv[2:]
    c=connect_producer_db(Path(ppath))
    config=json.loads(c.execute(f"SELECT payload FROM {CONFIG}").fetchone()[0])
    value=config["source_binding"]
    value["anchors"]=tuple(CursorWitness(**w) for w in value["anchors"])
    binding=SourceBinding(**value)
    repo=LedgerRepository.reopen(Path(path),domain())
    p=LiveContinuousProducerV02(c,ContinuousMarketSourceV02(Path(raw),start_after_p1_rowid=1,database_identity=DATABASE_ID))
    h=CandidateHandoffV01(p,repo,binding,database_identity=DATABASE_ID)
    connection=repo._conn
    class ExitConnection:
        def __getattr__(self,name):
            return getattr(connection,name)
        def execute(self,sql,*args):
            if sql=="COMMIT" and cut=="ledger-before":
                os._exit(81)
            result=connection.execute(sql,*args)
            if sql=="COMMIT" and cut=="ledger-after":
                os._exit(81)
            return result
    repo._conn=ExitConnection()
    def stop(stage):
        if (cut,stage) in (("ack-before","before_ack_commit"),("ack-after","after_ack_commit")):
            os._exit(81)
    h.failure_injector=stop
    h.deliver_page()
    raise AssertionError("unreached abrupt cut")


def main():
    with tempfile.TemporaryDirectory(prefix="meme-live-a3-") as tmp:
        directory=Path(tmp)
        transition(directory)
        crash_cases(directory)
        denials(directory)
        conflicts(directory)
        bounded_anchor(directory)
        missing_binding(directory)
        concurrent_capture(directory)
        abrupt_processes(directory)
    print("A3_CHECKS",len(CHECKS))
    return 0


if __name__=="__main__":
    if len(sys.argv)>1 and sys.argv[1]=="--crash":
        crash_child()
    raise SystemExit(main())
