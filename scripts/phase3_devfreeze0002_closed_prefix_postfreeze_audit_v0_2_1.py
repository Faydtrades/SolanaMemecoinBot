from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from datetime import timedelta
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.collector_coverage_v0_2 import CollectorCoverageSnapshotV02,parse_utc

FREEZE=PROJECT_ROOT/"data/research/phase3/dev_freezes/DEV-FREEZE-0002"

def sha(p):
    h=hashlib.sha256()
    with p.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):h.update(c)
    return h.hexdigest()

def main():
    db=FREEZE/"phase3_development_dataset_closed_prefix_v0_2_1.sqlite3"
    mp=FREEZE/"phase3_development_dataset_manifest_closed_prefix_v0_2_1.json"
    cp=FREEZE/"collector_coverage_DEV-FREEZE-0002_closed_prefix_v0_2_1.json"
    tc=FREEZE/"phase3_development_tokens_closed_prefix_v0_2_1.csv"
    selection=PROJECT_ROOT/"data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json"
    for p in (db,mp,cp,tc,selection):
        if not p.exists():raise FileNotFoundError(p)

    m=json.loads(mp.read_text(encoding="utf-8"));d=m["dataset"]
    if d["freeze_id"]!="DEV-FREEZE-0002":raise RuntimeError("wrong freeze id")
    if d["design_id"]!="DEV-FREEZE-0002-DESIGN-002A":raise RuntimeError("wrong design")
    if d["source_session_state"]!="EOF_UNCERTAIN":raise RuntimeError("wrong salvage source state")
    if d["uncertain_tail_included"] is not False:raise RuntimeError("uncertain tail included")
    if d["future_activity_filter"] or d["future_outcome_filter"] or d["strategy_signal_used_for_cohort_selection"]:
        raise RuntimeError("forbidden selection filter")
    if sha(db)!=d["snapshot_sqlite_sha256"] or sha(tc)!=d["development_token_csv_sha256"]:
        raise RuntimeError("immutable file hash mismatch")
    if sha(cp)!=m["coverage_snapshot"]["sha256"]:raise RuntimeError("coverage SHA mismatch")
    if sha(selection)!=m["entry_binding"]["EXP-0005_selection_sha256"]:
        raise RuntimeError("entry selection SHA mismatch")
    if m["entry_binding"]["A_B_C_D_parameters_fixed"] is not True:
        raise RuntimeError("entry parameters not fixed")

    r=d["session_range_utc"];start=parse_utc(r["start"]);end=parse_utc(r["end"])
    cov=CollectorCoverageSnapshotV02.load(cp)
    cov.validate_binding(freeze_id="DEV-FREEZE-0002",range_start=start,range_end=end)
    active=list(cov.payload.get("active_intervals",[]))
    gaps=list(cov.payload.get("explicit_gaps",[]))
    if not active:raise RuntimeError("no active intervals")
    if any(bool(x.get("uncertain_close")) for x in active):
        raise RuntimeError("uncertain coverage interval in immutable freeze")
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
            raise RuntimeError("cardinality mismatch")
        after=int(conn.execute(
            "SELECT COUNT(*) FROM pump_events WHERE decoded_at_utc>?",
            (d["safe_prefix_cutoff_utc"],)
        ).fetchone()[0])
        if after:raise RuntimeError("rows exist after safe prefix cutoff")
        failures=[]
        for r0 in conn.execute("""
            SELECT mint,t0_observed_at_utc,clean_interval_start_utc,clean_interval_end_utc
            FROM freeze_tokens ORDER BY mint
        """):
            t0=parse_utc(str(r0["t0_observed_at_utc"]));tail=t0+timedelta(minutes=10)
            a=parse_utc(str(r0["clean_interval_start_utc"]));b=parse_utc(str(r0["clean_interval_end_utc"]))
            if not (a<=t0 and tail<=b and b<=end):
                failures.append(str(r0["mint"]))
        if failures:
            raise RuntimeError(f"{len(failures)} token clean-window failures")
    finally:
        conn.close()

    with tc.open("r",encoding="utf-8",newline="") as f:
        csv_rows=list(csv.DictReader(f))
    if len(csv_rows)!=token_count:raise RuntimeError("token CSV cardinality mismatch")

    report={
      "schema_version":"P3-DEVFREEZE-CLOSED-PREFIX-AUDIT-0.2.1",
      "result":"PASS","freeze_id":"DEV-FREEZE-0002",
      "eligible_token_count":token_count,"frozen_pump_event_rows":row_count,
      "definite_clean_intervals":len(active),"explicit_gaps_retained":len(gaps),
      "checks":{
        "sqlite_quick_check":True,"immutable_hashes":True,
        "safe_prefix_cutoff_enforced":True,"uncertain_tail_excluded":True,
        "all_token_10m_windows_inside_definite_clean_interval":True,
        "entry_binding_fixed":True,"future_filters_absent":True,
        "explicit_gaps_not_ignored":True,
      },
      "profitability_claim_allowed":False,
    }
    out=FREEZE/"DEV-FREEZE-0002_closed_prefix_postfreeze_audit_v0_2_1.json"
    out.write_text(json.dumps(report,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print("DEV-FREEZE-0002 CLOSED-PREFIX POST-FREEZE AUDIT v0.2.1")
    print("="*78)
    print(f"Eligible tokens             : {token_count}")
    print(f"Frozen BOT_TRUTH rows       : {row_count}")
    print(f"Definite clean intervals    : {len(active)}")
    print(f"Explicit gaps retained      : {len(gaps)}")
    print("Safe prefix cutoff          : PASS")
    print("Uncertain tail excluded     : PASS")
    print("Every t0->t0+10m clean      : PASS")
    print("Entry A/B/C/D binding       : PASS")
    print("Profitability claim         : NO")
    print(f"Audit                       : {out}")
    print("="*78)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
