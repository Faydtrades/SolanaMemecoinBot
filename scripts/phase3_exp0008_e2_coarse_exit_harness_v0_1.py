from __future__ import annotations

import csv,json,os,time
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e2_coarse_exit_design_v0_1 import DESIGN,validate_design
from phase3.e2_coarse_exit_engine_v0_1 import evaluate_e2

def atomic(p,x):
    p.parent.mkdir(parents=True,exist_ok=True);t=p.with_suffix(p.suffix+".tmp")
    with t.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(x,f,indent=2,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
    os.replace(t,p)

def main():
    validate_design(DESIGN)
    root=PROJECT_ROOT;freeze=root/"data/research/phase3/dev_freezes/DEV-FREEZE-0002"
    exp7=root/"data/research/phase3/EXP-0007";exp8=root/"data/research/phase3/EXP-0008"
    design_path=exp8/"EXP-0008_E2_coarse_exit_design_v0_1.json"
    if not design_path.exists():raise FileNotFoundError("Lock E2 design first")
    if json.loads(design_path.read_text(encoding="utf-8"))!=DESIGN:
        raise RuntimeError("locked E2 design differs from code")

    t0=time.monotonic()
    result=evaluate_e2(
        freeze_db=freeze/"phase3_development_dataset_closed_prefix_v0_2_1.sqlite3",
        manifest_path=freeze/"phase3_development_dataset_manifest_closed_prefix_v0_2_1.json",
        coverage_path=freeze/"collector_coverage_DEV-FREEZE-0002_closed_prefix_v0_2_1.json",
        selection_path=root/"data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json",
        readiness_path=exp7/"DEV-FREEZE-0002_closed_prefix_locked_entry_readiness_report_v0_2_1.json",
    )
    rp=exp8/"EXP-0008_E2_coarse_exit_results_v0_1.json"
    cp=exp8/"EXP-0008_E2_coarse_exit_by_regime_v0_1.csv";atomic(rp,result)

    rows=[]
    for r in result["results"]:
        s=r["exit_bps"]
        rows.append({
            "role":r["role"],"family":r["family"],"variant_id":r["variant_id"],
            "threshold_bps":r["threshold_bps"],"time_stop_ms":r["time_stop_ms"],
            "clean_path_count":r["clean_path_count"],"scored_count":r["scored_count"],
            "unscored_count":r["unscored_count"],"stop_hit_count":r["stop_hit_count"],
            "stop_hit_rate":r["stop_hit_rate"],
            "p10_exit_bps":s["p10"],"p25_exit_bps":s["p25"],"p50_exit_bps":s["p50"],
            "p75_exit_bps":s["p75"],"p90_exit_bps":s["p90"],"mean_exit_bps":s["mean"],
            "positive_rate":s["positive_rate"],"negative_rate":s["negative_rate"],
            "exit_delay_p50_ms":r["exit_delay_p50_ms"],"mark_age_p50_ms":r["mark_age_p50_ms"],
        })
    with cp.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys()));w.writeheader();w.writerows(rows)

    print()
    print("EXP-0008 E2 COARSE EXIT RESEARCH v0.1")
    print("="*90)
    print(f"Elapsed                     : {(time.monotonic()-t0)/60:.2f} min")
    print(f"Variants x regimes          : {len(rows)}")
    print("Hard-stop grid              : -10,-20,-30,-40,-50,-60 %")
    print("Time-stop grid              : 5s,15s,30s,60s,2m,5m")
    print("Families combined           : NO")
    print("TP/trailing tested          : NO")
    print("Entry A/B/C/D changed       : NO")
    print("Gross reference only        : YES")
    print("Profitability claim         : NO")
    print(f"Results                     : {rp}")
    print(f"CSV                         : {cp}")
    print("="*90)
    print("RESULT: PASS")

if __name__=="__main__":main()
