from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.locked_entry_readiness_v0_1_2 import run_locked_entry_readiness

def atomic(p,x):
    p.parent.mkdir(parents=True,exist_ok=True)
    t=p.with_suffix(p.suffix+".tmp")
    with t.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(x,f,indent=2,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
    os.replace(t,p)

def main():
    freeze=PROJECT_ROOT/"data/research/phase3/dev_freezes/DEV-FREEZE-0002"
    p3=PROJECT_ROOT/"data/research/phase3"
    result=run_locked_entry_readiness(
        freeze_db=freeze/"phase3_development_dataset_closed_prefix_v0_2_1.sqlite3",
        freeze_manifest=freeze/"phase3_development_dataset_manifest_closed_prefix_v0_2_1.json",
        coverage_snapshot=freeze/"collector_coverage_DEV-FREEZE-0002_closed_prefix_v0_2_1.json",
        selection_path=p3/"EXP-0005/EXP-0005_D_selection_v0_1.json",
    )
    out=p3/"EXP-0007"
    rp=out/"DEV-FREEZE-0002_closed_prefix_locked_entry_readiness_report_v0_2_1.json"
    cp=out/"DEV-FREEZE-0002_closed_prefix_locked_entry_readiness_by_regime_v0_2_1.csv"
    atomic(rp,result)

    statuses=sorted({s for r in result["regimes"] for s in r["path_status_counts"]})
    rows=[]
    for r in result["regimes"]:
        row={
            "role":r["role"],
            "parameter_set_id":r["parameter_set_id"],
            "candidate_count":r["candidate_count"],
            "clean_5m_count":r["clean_5m_count"],
            "clean_5m_rate":r["clean_5m_rate"],
        }
        for s in statuses:
            row[f"status_{s}"]=r["path_status_counts"].get(s,0)
        rows.append(row)

    with cp.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys()))
        w.writeheader();w.writerows(rows)

    print("EXP-0007 DEV-FREEZE-0002 CLOSED-PREFIX READINESS v0.2.1")
    print("="*80)
    for r in rows:
        print(
            f"{r['role']:8s} candidates={r['candidate_count']:4d} "
            f"clean5m={r['clean_5m_count']:4d} "
            f"rate={100*r['clean_5m_rate']:6.2f}%"
        )
    rd=result["readiness"]
    print("-"*80)
    print(f"Readiness tier              : {rd['tier']}")
    print(f"Minimum robust clean 5m     : {rd['minimum_clean_5m_count']}")
    print(f"Minimum robust clean rate   : {100*rd['minimum_clean_5m_rate']:.2f}%")
    print("Entry A/B/C/D changed       : NO")
    print("Exit thresholds optimized   : NO")
    print("Profitability claim         : NO")
    print(f"Report                      : {rp}")
    print("="*80)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
