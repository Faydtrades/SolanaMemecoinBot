from __future__ import annotations

import csv, json, os, time
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e5_small_interaction_design_v0_1 import DESIGN, validate_design
from phase3.e5_small_interaction_engine_v0_1 import evaluate_e5

def atomic(path,payload):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+".tmp")
    with tmp.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(payload,f,indent=2,sort_keys=True)
        f.write("\n");f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)

def main():
    validate_design(DESIGN)
    root=PROJECT_ROOT
    freeze=root/"data/research/phase3/dev_freezes/DEV-FREEZE-0002"
    exp7=root/"data/research/phase3/EXP-0007"
    exp11=root/"data/research/phase3/EXP-0011"

    design_path=exp11/"EXP-0011_E5_small_interaction_design_v0_1.json"
    if not design_path.exists():
        raise FileNotFoundError("E5 design must be initialized first")

    locked=json.loads(design_path.read_text(encoding="utf-8"))
    for k,v in DESIGN.items():
        if locked.get(k)!=v:
            raise RuntimeError(f"locked E5 design differs at key {k}")

    started=time.monotonic()
    result=evaluate_e5(
        freeze_db=freeze/"phase3_development_dataset_closed_prefix_v0_2_1.sqlite3",
        manifest_path=freeze/"phase3_development_dataset_manifest_closed_prefix_v0_2_1.json",
        coverage_path=freeze/"collector_coverage_DEV-FREEZE-0002_closed_prefix_v0_2_1.json",
        selection_path=root/"data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json",
        readiness_path=exp7/"DEV-FREEZE-0002_closed_prefix_locked_entry_readiness_report_v0_2_1.json",
    )

    rp=exp11/"EXP-0011_E5_small_interaction_results_v0_1.json"
    cp=exp11/"EXP-0011_E5_small_interaction_by_regime_v0_1.csv"
    atomic(rp,result)

    reason_keys=sorted({
        k for r in result["results"]
        for k in r["exit_reason_counts"]
    })
    rows=[]
    for r in result["results"]:
        s=r["exit_bps"]
        row={
            "role":r["role"],
            "variant_id":r["variant_id"],
            "variant_role":r["variant_role"],
            "fallback_ms":r["fallback_ms"],
            "tp_bps":r["tp_bps"],
            "trail_activation_bps":r["trail_activation_bps"],
            "trail_giveback_bps":r["trail_giveback_bps"],
            "hard_stop_bps":r["hard_stop_bps"],
            "clean_path_count":r["clean_path_count"],
            "scored_count":r["scored_count"],
            "unscored_count":r["unscored_count"],
            "p10_exit_bps":s["p10"],
            "p25_exit_bps":s["p25"],
            "p50_exit_bps":s["p50"],
            "p75_exit_bps":s["p75"],
            "p90_exit_bps":s["p90"],
            "mean_exit_bps":s["mean"],
            "positive_rate":s["positive_rate"],
            "negative_rate":s["negative_rate"],
            "exit_delay_p50_ms":r["exit_delay_p50_ms"],
            "activation_delay_p50_ms":r["activation_delay_p50_ms"],
            "fallback_mark_age_p50_ms":r["fallback_mark_age_p50_ms"],
        }
        for reason in reason_keys:
            row[f"exit_{reason}"]=r["exit_reason_counts"].get(reason,0)
        rows.append(row)

    with cp.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys()))
        w.writeheader();w.writerows(rows)

    print()
    print("EXP-0011 E5 SMALL INTERACTION RESEARCH v0.1")
    print("="*94)
    print(f"Elapsed                     : {(time.monotonic()-started)/60:.2f} min")
    print(f"Variants x regimes          : {len(rows)}")
    print("Variants                    : 9 hand-enumerated")
    print("Cartesian search            : NO")
    print("Entry A/B/C/D changed       : NO")
    print("Gross reference only        : YES")
    print("Profitability claim         : NO")
    print(f"Results                     : {rp}")
    print(f"CSV                         : {cp}")
    print("="*94)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
