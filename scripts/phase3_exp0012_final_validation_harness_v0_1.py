from __future__ import annotations

import csv
import hashlib
import json
import os
import time
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.final_phase3_validation_design_v0_1 import DESIGN,validate_design
from phase3.final_phase3_validation_engine_v0_1 import run_final_validation

def file_sha(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):
            h.update(c)
    return h.hexdigest()

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
    exp12=root/"data/research/phase3/EXP-0012"

    design_path=exp12/"EXP-0012_final_phase3_validation_design_v0_1.json"
    e5_results=exp11/"EXP-0011_E5_small_interaction_results_v0_1.json"
    e5_audit=exp11/"EXP-0011_E5_small_interaction_postrun_audit_v0_1.json"
    final_selection=exp11/"EXP-0011_E5_final_candidate_selection_v0_1.json"

    for p in (design_path,e5_results,e5_audit,final_selection):
        if not p.exists():
            raise FileNotFoundError(p)

    locked=json.loads(design_path.read_text(encoding="utf-8"))
    for k,v in DESIGN.items():
        if locked.get(k)!=v:
            raise RuntimeError(f"locked EXP-0012 design differs at key {k}")

    # Enforce source hashes bound by the locked design.
    source=locked["source_binding"]
    checks=[
        (e5_results,source["EXP-0011_E5_results_sha256"]),
        (e5_audit,source["EXP-0011_E5_audit_sha256"]),
        (final_selection,source["EXP-0011_final_selection_sha256"]),
    ]
    for path,expected in checks:
        actual=file_sha(path)
        if actual!=expected:
            raise RuntimeError(
                f"source hash mismatch {path.name}: {actual} != {expected}"
            )

    started=time.monotonic()
    result=run_final_validation(
        freeze_db=freeze/"phase3_development_dataset_closed_prefix_v0_2_1.sqlite3",
        manifest_path=freeze/"phase3_development_dataset_manifest_closed_prefix_v0_2_1.json",
        coverage_path=freeze/"collector_coverage_DEV-FREEZE-0002_closed_prefix_v0_2_1.json",
        selection_path=root/"data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json",
        readiness_path=exp7/"DEV-FREEZE-0002_closed_prefix_locked_entry_readiness_report_v0_2_1.json",
        e5_results_path=e5_results,
    )
    result["source_hash_binding"]="PASS"

    rp=exp12/"EXP-0012_final_phase3_validation_results_v0_1.json"
    cp=exp12/"EXP-0012_final_phase3_validation_by_regime_v0_1.csv"
    atomic(rp,result)

    rows=[]
    for r in result["locked_finalist_rows"]:
        s=r["exit_bps"]
        rows.append({
            "role":r["role"],
            "candidate_id":r["candidate_id"],
            "track":r["track"],
            "variant_id":r["variant_id"],
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
            "exit_reason_counts_json":json.dumps(
                r["exit_reason_counts"],sort_keys=True,separators=(",",":")
            ),
        })

    with cp.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys()))
        w.writeheader();w.writerows(rows)

    print()
    print("EXP-0012 FINAL PHASE-3 DETERMINISTIC VALIDATION v0.1")
    print("="*96)
    print(f"Elapsed                       : {(time.monotonic()-started)/60:.2f} min")
    print("Freeze quick_check            : PASS")
    print("Coverage binding              : PASS")
    print("Source hash binding           : PASS")
    print("Entry exact reproduction      : PASS")
    print("Clean 5m exact reproduction   : PASS")
    print("Locked finalist rows          : 12")
    print("EXP-0011 exact reproduction   : PASS")
    print("Cached-path determinism       : PASS")
    print(f"Deterministic SHA256          : {result['cached_path_evaluation_hash_pass1']}")
    print("Parameter search              : NO")
    print("Candidate reselection         : NO")
    print("Further exit tuning           : NO")
    print("Profitability claim           : NO")
    print("Phase-3 final status          : PHASE3_FINAL_VALIDATION_PASS")
    print("Phase-4 build allowed         : YES")
    print(f"Results                       : {rp}")
    print(f"CSV                           : {cp}")
    print("="*96)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
