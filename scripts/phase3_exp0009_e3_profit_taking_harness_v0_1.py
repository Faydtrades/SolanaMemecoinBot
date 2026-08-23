from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e3_profit_taking_design_v0_1 import DESIGN, validate_design
from phase3.e3_profit_taking_engine_v0_1 import evaluate_e3

def atomic(path,payload):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+".tmp")
    with tmp.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(payload,f,indent=2,sort_keys=True)
        f.write("\n"); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path)

def main():
    validate_design(DESIGN)
    root=PROJECT_ROOT
    freeze=root/"data/research/phase3/dev_freezes/DEV-FREEZE-0002"
    exp7=root/"data/research/phase3/EXP-0007"
    exp9=root/"data/research/phase3/EXP-0009"

    design_path=exp9/"EXP-0009_E3_profit_taking_design_v0_1.json"
    if not design_path.exists():
        raise FileNotFoundError("E3 design must be initialized first")
    if json.loads(design_path.read_text(encoding="utf-8")) != DESIGN:
        raise RuntimeError("locked E3 design differs from code")

    started=time.monotonic()
    result=evaluate_e3(
        freeze_db=freeze/"phase3_development_dataset_closed_prefix_v0_2_1.sqlite3",
        manifest_path=freeze/"phase3_development_dataset_manifest_closed_prefix_v0_2_1.json",
        coverage_path=freeze/"collector_coverage_DEV-FREEZE-0002_closed_prefix_v0_2_1.json",
        selection_path=root/"data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json",
        readiness_path=exp7/"DEV-FREEZE-0002_closed_prefix_locked_entry_readiness_report_v0_2_1.json",
    )

    rp=exp9/"EXP-0009_E3_profit_taking_results_v0_1.json"
    cp=exp9/"EXP-0009_E3_profit_taking_by_regime_v0_1.csv"
    atomic(rp,result)

    rows=[]
    for r in result["results"]:
        s=r["exit_bps"]
        rows.append({
            "role":r["role"],
            "family":r["family"],
            "variant_id":r["variant_id"],
            "tp_threshold_bps":r["tp_threshold_bps"],
            "fallback_time_stop_ms":r["fallback_time_stop_ms"],
            "fallback_role":r["fallback_role"],
            "clean_path_count":r["clean_path_count"],
            "scored_count":r["scored_count"],
            "unscored_count":r["unscored_count"],
            "tp_hit_count":r["tp_hit_count"],
            "tp_hit_rate_clean_paths":r["tp_hit_rate_clean_paths"],
            "tp_hit_rate_scored":r["tp_hit_rate_scored"],
            "fallback_exit_count":r["fallback_exit_count"],
            "p10_exit_bps":s["p10"],
            "p25_exit_bps":s["p25"],
            "p50_exit_bps":s["p50"],
            "p75_exit_bps":s["p75"],
            "p90_exit_bps":s["p90"],
            "mean_exit_bps":s["mean"],
            "positive_rate":s["positive_rate"],
            "negative_rate":s["negative_rate"],
            "exit_delay_p50_ms":r["exit_delay_p50_ms"],
            "tp_hit_delay_p50_ms":r["tp_hit_delay_p50_ms"],
            "fallback_mark_age_p50_ms":r["fallback_mark_age_p50_ms"],
        })

    with cp.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)

    print()
    print("EXP-0009 E3 PROFIT-TAKING RESEARCH v0.1")
    print("="*90)
    print(f"Elapsed                     : {(time.monotonic()-started)/60:.2f} min")
    print(f"Variants x regimes          : {len(rows)}")
    print("TP grid                     : +3,+5,+10,+15,+20,+30 %")
    print("Primary fallback            : 15s")
    print("Sensitivity fallback        : 5s")
    print("Hard stop                   : NOT USED")
    print("Trailing                    : NOT USED")
    print("Partial/multi-TP            : NOT USED")
    print("Entry A/B/C/D changed       : NO")
    print("Gross reference only        : YES")
    print("Profitability claim         : NO")
    print(f"Results                     : {rp}")
    print(f"CSV                         : {cp}")
    print("="*90)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
