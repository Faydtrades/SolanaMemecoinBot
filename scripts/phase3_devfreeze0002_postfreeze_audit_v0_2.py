from __future__ import annotations

import csv,hashlib,json,sqlite3
from datetime import timedelta
from pathlib import Path
from typing import Any
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase2.phase1_readonly_adapter_v0_1 import BOT_TRUTH_SOURCE_PREFIXES
from phase3.collector_coverage_v0_2 import CollectorCoverageSnapshotV02,parse_utc

def sha(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):h.update(c)
    return h.hexdigest()

def source_sql():
    return "("+" OR ".join("source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES)+")",tuple(p+"%" for p in BOT_TRUTH_SOURCE_PREFIXES)

def main():
    freeze=PROJECT_ROOT/"data/research/phase3/dev_freezes/DEV-FREEZE-0002"
    db=freeze/"phase3_development_dataset_v0_2.sqlite3"
    mp=freeze/"phase3_development_dataset_manifest_v0_2.json"
    cp=freeze/"collector_coverage_DEV-FREEZE-0002_v0_2.json"
    tc=freeze/"phase3_development_tokens_v0_2.csv"
    selection=PROJECT_ROOT/"data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json"
    for p in (db,mp,cp,tc,selection):
        if not p.exists():raise FileNotFoundError(p)

    m=json.loads(mp.read_text(encoding="utf-8"));d=m["dataset"]
    if d["freeze_id"]!="DEV-FREEZE-0002" or d["design_id"]!="DEV-FREEZE-0002-DESIGN-002":
        raise RuntimeError("freeze/design binding mismatch")
    if d["future_activity_filter"] is not False or d["future_outcome_filter"] is not False:
        raise RuntimeError("forbidden future filters")
    if d["strategy_signal_used_for_cohort_selection"] is not False:
        raise RuntimeError("strategy signal used for cohort selection")
    if sha(db)!=d["snapshot_sqlite_sha256"] or sha(tc)!=d["development_token_csv_sha256"]:
        raise RuntimeError("immutable file hash mismatch")
    if sha(selection)!=m["entry_binding"]["EXP-0005_selection_sha256"]:
        raise RuntimeError("entry selection SHA mismatch")
    if m["entry_binding"]["A_B_C_D_parameters_fixed"] is not True:
        raise RuntimeError("entry parameters not fixed")

    r=d["session_range_utc"];start=parse_utc(r["start"]);end=parse_utc(r["end"])
    cov=CollectorCoverageSnapshotV02.load(cp)
    cov.validate_binding(freeze_id="DEV-FREEZE-0002",range_start=start,range_end=end)
    active=[x for x in cov.payload.get("active_intervals",[]) if not bool(x.get("uncertain_close"))]
    gaps=list(cov.payload.get("explicit_gaps",[]))
    if not active:raise RuntimeError("coverage has no closed active intervals")
    if any(bool(x.get("uncertain_close")) for x in cov.payload.get("active_intervals",[])):
        raise RuntimeError("coverage contains uncertain interval")

    intervals=[(parse_utc(x["start_utc"]),parse_utc(x["end_utc"])) for x in active]
    uri=f"file:{db.resolve().as_posix()}?mode=ro"
    conn=sqlite3.connect(uri,uri=True);conn.row_factory=sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        if conn.execute("PRAGMA quick_check").fetchone()[0]!="ok":
            raise RuntimeError("snapshot quick_check failed")
        row_count=int(conn.execute("SELECT COUNT(*) FROM pump_events").fetchone()[0])
        token_count=int(conn.execute("SELECT COUNT(*) FROM freeze_tokens").fetchone()[0])
        if row_count!=int(d["frozen_pump_event_rows"]) or token_count!=int(d["eligible_token_count"]):
            raise RuntimeError("freeze cardinality mismatch")
        where,args=source_sql()
        failures=[]
        for r0 in conn.execute("SELECT mint,t0_observed_at_utc FROM freeze_tokens ORDER BY mint"):
            mint=str(r0["mint"]);t0=parse_utc(str(r0["t0_observed_at_utc"]))
            tail=t0+timedelta(minutes=10)
            if not any(a<=t0 and tail<=b for a,b in intervals):
                failures.append((mint,"10M_NOT_INSIDE_CLEAN_INTERVAL"));continue
            first=conn.execute(f"""
                SELECT decoded_at_utc,source_decoded_file
                FROM pump_events
                WHERE mint=? AND event_type IN ('BUY','SELL')
                  AND event_key IS NOT NULL AND signature IS NOT NULL
                  AND pump_timestamp IS NOT NULL AND decoded_at_utc IS NOT NULL
                  AND slot IS NOT NULL AND {where}
                ORDER BY decoded_at_utc,rowid LIMIT 1
            """,(mint,*args)).fetchone()
            if first is None or parse_utc(str(first["decoded_at_utc"]))!=t0:
                failures.append((mint,"T0_MISMATCH"))
            elif str(first["source_decoded_file"]).startswith("GAP_RECONCILIATION_"):
                failures.append((mint,"T0_GAP_RECOVERY"))
        if failures:
            raise RuntimeError(f"{len(failures)} token eligibility audit failures; first={failures[:3]}")
    finally:conn.close()

    with tc.open("r",encoding="utf-8",newline="") as f:
        csv_rows=list(csv.DictReader(f))
    if len(csv_rows)!=token_count:raise RuntimeError("token CSV cardinality mismatch")

    report={
      "schema_version":"P3-DEVFREEZE-POSTAUDIT-0.2","result":"PASS",
      "freeze_id":"DEV-FREEZE-0002","eligible_token_count":token_count,
      "frozen_pump_event_rows":row_count,"closed_active_intervals":len(active),
      "explicit_gaps_recorded":len(gaps),
      "checks":{
        "sqlite_quick_check":True,"immutable_hashes":True,"entry_binding":True,
        "session_coverage_bound":True,"gaps_recorded_not_ignored":True,
        "every_token_t0_to_t0_plus_10m_inside_one_clean_interval":True,
        "clean_global_t0_identity":True,"future_filters_absent":True,
      },
      "production_db_touched":False,"profitability_claim_allowed":False,
    }
    out=freeze/"DEV-FREEZE-0002_postfreeze_audit_v0_2.json"
    out.write_text(json.dumps(report,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print("DEV-FREEZE-0002 POST-FREEZE AUDIT v0.2")
    print("="*70)
    print(f"Eligible tokens             : {token_count}")
    print(f"Frozen BOT_TRUTH rows       : {row_count}")
    print(f"Closed active intervals     : {len(active)}")
    print(f"Explicit gaps recorded      : {len(gaps)}")
    print("Every t0->t0+10m clean      : PASS")
    print("Entry A/B/C/D binding       : PASS")
    print("Gaps ignored                : NO")
    print("Production DB touched       : NO")
    print(f"Audit                       : {out}")
    print("="*70)
    print("RESULT: PASS")
if __name__=="__main__":main()
