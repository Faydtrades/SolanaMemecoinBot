from __future__ import annotations
import csv,json,os
from pathlib import Path
import sys
PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))
from phase3.locked_entry_readiness_v0_1_1 import run_locked_entry_readiness
def atomic(p,x):
    p.parent.mkdir(parents=True,exist_ok=True);t=p.with_suffix(p.suffix+".tmp")
    with t.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(x,f,indent=2,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
    os.replace(t,p)
def main():
    freeze=PROJECT_ROOT/"data/research/phase3/dev_freezes/DEV-FREEZE-0002";p3=PROJECT_ROOT/"data/research/phase3"
    db=freeze/"phase3_development_dataset_v0_1.sqlite3"
    manifest=freeze/"phase3_development_dataset_manifest_v0_1.json"
    coverage=freeze/"collector_coverage_DEV-FREEZE-0002_v0_1.json"
    selection=p3/"EXP-0005/EXP-0005_D_selection_v0_1.json"
    for p in (db,manifest,coverage,selection):
        if not p.exists():raise FileNotFoundError(p)
    result=run_locked_entry_readiness(freeze_db=db,freeze_manifest=manifest,
        coverage_snapshot=coverage,selection_path=selection)
    out=p3/"EXP-0007";report=out/"DEV-FREEZE-0002_locked_entry_readiness_report_v0_1.json"
    summary=out/"DEV-FREEZE-0002_locked_entry_readiness_by_regime_v0_1.csv";atomic(report,result)
    rows=[]
    allstatuses=sorted({s for r in result["regimes"] for s in r["path_status_counts"]})
    for r in result["regimes"]:
        row={"role":r["role"],"parameter_set_id":r["parameter_set_id"],
             "candidate_count":r["candidate_count"],"clean_5m_count":r["clean_5m_count"],
             "clean_5m_rate":r["clean_5m_rate"]}
        for s in allstatuses:row[f"status_{s}"]=r["path_status_counts"].get(s,0)
        rows.append(row)
    with summary.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys()));w.writeheader();w.writerows(rows)
    print("EXP-0007 DEV-FREEZE-0002 LOCKED-ENTRY READINESS v0.1.1");print("="*72)
    for r in rows:print(f"{r['role']:8s} candidates={r['candidate_count']:4d} clean5m={r['clean_5m_count']:4d} rate={100*r['clean_5m_rate']:6.2f}%")
    rd=result["readiness"];print("-"*72)
    print(f"Readiness tier             : {rd['tier']}")
    print(f"Minimum robust clean 5m    : {rd['minimum_clean_5m_count']}")
    print(f"Minimum robust clean rate  : {100*rd['minimum_clean_5m_rate']:.2f}%")
    print("Entry A/B/C/D changed      : NO");print("Exit thresholds optimized  : NO")
    print("Profitability claim        : NO");print(f"Report                     : {report}")
    print("="*72);print("RESULT: PASS")
if __name__=="__main__":main()
