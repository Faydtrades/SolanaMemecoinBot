from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e4_trailing_design_v0_1 import DESIGN, validate_design
from phase3.e4_trailing_engine_v0_1 import evaluate_e4

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
    exp10=root/"data/research/phase3/EXP-0010"

    design_path=exp10/"EXP-0010_E4_trailing_design_v0_1.json"
    e3_selection=exp9/"EXP-0009_E3_profit_taking_selection_v0_1.json"
    if not design_path.exists():
        raise FileNotFoundError("E4 design must be initialized first")
    if not e3_selection.exists():
        raise FileNotFoundError("E3 selection missing")

    locked=json.loads(design_path.read_text(encoding="utf-8"))
    for k,v in DESIGN.items():
        if locked.get(k)!=v:
            raise RuntimeError(f"locked E4 design differs at key {k}")

    started=time.monotonic()
    result=evaluate_e4(
        freeze_db=freeze/"phase3_development_dataset_closed_prefix_v0_2_1.sqlite3",
        manifest_path=freeze/"phase3_development_dataset_manifest_closed_prefix_v0_2_1.json",
        coverage_path=freeze/"collector_coverage_DEV-FREEZE-0002_closed_prefix_v0_2_1.json",
        selection_path=root/"data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json",
        readiness_path=exp7/"DEV-FREEZE-0002_closed_prefix_locked_entry_readiness_report_v0_2_1.json",
    )

    rp=exp10/"EXP-0010_E4_trailing_results_v0_1.json"
    cp=exp10/"EXP-0010_E4_trailing_by_regime_v0_1.csv"
    atomic(rp,result)

    rows=[]
    for r in result["results"]:
        s=r["exit_bps"]
        rows.append({
            "role":r["role"],
            "family":r["family"],
            "variant_id":r["variant_id"],
            "activation_threshold_bps":r["activation_threshold_bps"],
            "giveback_bps":r["giveback_bps"],
            "fallback_time_stop_ms":r["fallback_time_stop_ms"],
            "clean_path_count":r["clean_path_count"],
            "scored_count":r["scored_count"],
            "unscored_count":r["unscored_count"],
            "activation_count":r["activation_count"],
            "activation_rate_clean_paths":r["activation_rate_clean_paths"],
            "trail_exit_count":r["trail_exit_count"],
            "trail_exit_rate_clean_paths":r["trail_exit_rate_clean_paths"],
            "trail_exit_rate_activated":r["trail_exit_rate_activated"],
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
            "activation_delay_p50_ms":r["activation_delay_p50_ms"],
            "fallback_mark_age_p50_ms":r["fallback_mark_age_p50_ms"],
            "peak_at_trail_exit_p50_bps":r["peak_at_trail_exit_p50_bps"],
        })

    with cp.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)

    print()
    print("EXP-0010 E4 TRAILING RESEARCH v0.1")
    print("="*92)
    print(f"Elapsed                     : {(time.monotonic()-started)/60:.2f} min")
    print(f"Variants x regimes          : {len(rows)}")
    print("Activation grid             : +5,+10,+15 %")
    print("Giveback grid               : 3,5,10 %")
    print("Fallback                    : 15s")
    print("E3 fixed TP used            : NO")
    print("Hard stop                   : NOT USED")
    print("T5s sensitivity             : NOT USED")
    print("Entry A/B/C/D changed       : NO")
    print("Gross reference only        : YES")
    print("Profitability claim         : NO")
    print(f"Results                     : {rp}")
    print(f"CSV                         : {cp}")
    print("="*92)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
