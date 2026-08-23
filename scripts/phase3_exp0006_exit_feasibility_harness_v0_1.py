from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT/"src"))

from phase3.exit_research_design_v0_1 import (
    HORIZONS, ROBUST_ROLES, build_design, validate_selection,
)

SOURCE_OUTCOMES_SHA256 = "5f5c0b7b4acd770bd08cb97b1be642bf291c39cd13ffc3397b28ce99c82aee98"

def file_sha(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):
            h.update(c)
    return h.hexdigest()

def percentile_nearest_rank(values: list[int], p: int) -> int | None:
    if not values:
        return None
    s=sorted(values)
    rank=max(1, math.ceil(p/100*len(s)))
    return s[rank-1]

def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys()))
        w.writeheader();w.writerows(rows)

def require(cond: bool, msg: str) -> None:
    if not cond:
        print(f"[FAIL] {msg}")
        print("RESULT: FAIL")
        raise SystemExit(1)

def verify_registry(path: Path) -> dict[str, Any]:
    r=json.loads(path.read_text(encoding="utf-8"))
    core=dict(r); stored=core.pop("deterministic_registry_sha256",None)
    payload=json.dumps(core,sort_keys=True,separators=(",",":"),ensure_ascii=True).encode("utf-8")
    require(stored==hashlib.sha256(payload).hexdigest(),"registry deterministic hash invalid")
    require(r.get("next_experiment_id")=="EXP-0006","registry next ID must be EXP-0006")
    exp5=[x for x in r.get("experiments",[]) if isinstance(x,dict) and x.get("experiment_id")=="EXP-0005"]
    require(len(exp5)==1 and exp5[0].get("status")=="COMPLETE","EXP-0005 not registered COMPLETE")
    return exp5[0]

def main():
    ap=argparse.ArgumentParser()
    freeze=PROJECT_ROOT/"data/research/phase2_research_freeze_v0_1"
    p3=PROJECT_ROOT/"data/research/phase3"
    ap.add_argument("--registry",default=str(freeze/"phase2_experiment_registry_v0_1.json"))
    ap.add_argument("--selection",default=str(p3/"EXP-0005/EXP-0005_D_selection_v0_1.json"))
    ap.add_argument("--candidate-outcomes",default=str(p3/"EXP-0005/EXP-0005_D_reclaim_runaway_candidate_outcomes_v0_1.csv"))
    ap.add_argument("--out-dir",default=str(p3/"EXP-0006"))
    args=ap.parse_args()
    registry=Path(args.registry).resolve()
    selection_path=Path(args.selection).resolve()
    outcomes_path=Path(args.candidate_outcomes).resolve()
    out=Path(args.out_dir).resolve()

    print("PHASE 3 / EXP-0006 — EXIT PATH FEASIBILITY v0.1")
    print("="*70)

    exp5_entry=verify_registry(registry)
    require(selection_path.exists(),"EXP-0005 selection missing")
    require(outcomes_path.exists(),"EXP-0005 candidate outcomes missing")
    require(file_sha(outcomes_path)==SOURCE_OUTCOMES_SHA256,"source outcome CSV hash mismatch")

    selection=json.loads(selection_path.read_text(encoding="utf-8"))
    validate_selection(selection)
    selection_sha=file_sha(selection_path)
    require(exp5_entry.get("selection_sha256")==selection_sha,"registry selection binding mismatch")

    ids={str(x["parameter_set_id"]):str(x["role"]) for x in selection["carry_forward"]}
    rows_by_id={pid:[] for pid in ids}
    with outcomes_path.open("r",encoding="utf-8",newline="") as f:
        for row in csv.DictReader(f):
            pid=row["parameter_set_id"]
            if pid in rows_by_id:
                rows_by_id[pid].append(row)

    require(all(rows_by_id[pid] for pid in ids),"one or more selected regimes have no candidate rows")

    by_regime=[]
    for pid,role in ids.items():
        rows=rows_by_id[pid]
        outrow={"role":role,"parameter_set_id":pid,"candidate_count":len(rows)}
        for h in HORIZONS:
            status_col=f"{h}_status"; age_col=f"{h}_mark_age_ms"
            statuses={}
            for r in rows:
                statuses[r[status_col]]=statuses.get(r[status_col],0)+1
            obs=[r for r in rows if r[status_col]=="OBSERVABLE"]
            ages=[int(float(r[age_col])) for r in obs if r.get(age_col) not in ("",None)]
            outrow[f"{h}_observable_n"]=len(obs)
            outrow[f"{h}_observable_pct"]=round(100*len(obs)/len(rows),2)
            outrow[f"{h}_mark_age_p50_ms"]=percentile_nearest_rank(ages,50)
            outrow[f"{h}_mark_age_p90_ms"]=percentile_nearest_rank(ages,90)
            outrow[f"{h}_mark_age_max_ms"]=max(ages) if ages else None
            for status in (
                "COVERAGE_GAP","COVERAGE_UNKNOWN","NO_FUTURE_PRICE",
                "MARKET_CONTINUITY_UNKNOWN","PRICE_IDENTITY_MISMATCH"
            ):
                outrow[f"{h}_{status.lower()}_n"]=statuses.get(status,0)
        outrow["complete_5m_extrema_n"]=sum(
            str(r.get("extrema_complete_5m","")).lower()=="true" for r in rows
        )
        by_regime.append(outrow)

    by_role={r["role"]:r for r in by_regime}
    robust_5m=[int(by_role[role]["5m_observable_n"]) for role in ROBUST_ROLES]
    robust_2m=[int(by_role[role]["2m_observable_n"]) for role in ROBUST_ROLES]
    robust_60s=[int(by_role[role]["60s_observable_n"]) for role in ROBUST_ROLES]
    robust_30s=[int(by_role[role]["30s_observable_n"]) for role in ROBUST_ROLES]

    # This is an observability gate, not a statistical sample-size threshold.
    # A 5m exit claim cannot be cleanly evaluated when every robust regime has
    # zero clean 5m observations.
    five_min_supported = all(n > 0 for n in robust_5m)
    mainline_grid_allowed = five_min_supported

    design=build_design(selection_sha)
    report={
        "schema_version":"P3-EXP0006-EXIT-FEASIBILITY-0.1",
        "experiment_id":"EXP-0006",
        "role":"EXIT_RESEARCH_DESIGN_AND_PATH_FEASIBILITY",
        "status":"COMPLETE_PENDING_REVIEW",
        "source":{
            "entry_selection_path":"data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json",
            "entry_selection_sha256":selection_sha,
            "candidate_outcomes_path":"data/research/phase3/EXP-0005/EXP-0005_D_reclaim_runaway_candidate_outcomes_v0_1.csv",
            "candidate_outcomes_sha256":SOURCE_OUTCOMES_SHA256,
            "dataset_freeze_id":"DEV-FREEZE-0001",
            "dataset_role":"DEVELOPMENT_DISCOVERY",
            "untouched_oos":False,
        },
        "design":design,
        "observability":{
            "by_regime":by_regime,
            "robust_clean_counts":{
                "30s":robust_30s,
                "60s":robust_60s,
                "2m":robust_2m,
                "5m":robust_5m,
            },
            "all_robust_regimes_have_clean_5m":five_min_supported,
        },
        "feasibility_conclusion":{
            "five_minute_dependent_exit_research_supported_on_DEV_FREEZE_0001":five_min_supported,
            "mainline_exit_parameter_grid_allowed_now":mainline_grid_allowed,
            "reason_if_blocked":(
                None if mainline_grid_allowed else
                "All three robust full-entry regimes have zero clean observable 5m outcomes on DEV-FREEZE-0001. "
                "Long-horizon exit optimization on this freeze would therefore be unsupported."
            ),
            "shorter_horizon_evidence_role":(
                "DESCRIPTIVE_FEASIBILITY_ONLY; do not optimize exits from these sparse counts in EXP-0006."
            ),
            "recommended_next_action":(
                "Build ExitPathReplay foundation/selftests and obtain a larger, continuously observed "
                "development dataset before mainline exit-rule optimization."
                if not mainline_grid_allowed else
                "Proceed to ExitPathReplay foundation before any threshold grid."
            ),
        },
        "performance_claims":{
            "exit_thresholds_tested":False,
            "stop_loss_selected":False,
            "take_profit_selected":False,
            "trailing_selected":False,
            "time_stop_selected":False,
            "fees_modeled":False,
            "slippage_modeled":False,
            "latency_modeled":False,
            "realistic_net_pnl_available":False,
            "profitability_claim_allowed":False,
        },
    }

    out.mkdir(parents=True,exist_ok=True)
    by_regime_path=out/"EXP-0006_exit_feasibility_by_regime_v0_1.csv"
    report_path=out/"EXP-0006_exit_feasibility_report_v0_1.json"
    design_path=out/"EXP-0006_exit_research_design_v0_1.json"
    manifest_path=out/"EXP-0006_manifest_v0_1.json"
    write_csv(by_regime_path,by_regime)
    design_path.write_text(json.dumps(design,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    report_path.write_text(json.dumps(report,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    manifest={
        "manifest_schema_version":"EXPM-0.1-P3EXT",
        "experiment_id":"EXP-0006",
        "status":"RUN_COMPLETE_PENDING_POSTRUN_AUDIT_AND_REVIEW",
        "role":"EXIT_RESEARCH_DESIGN_AND_PATH_FEASIBILITY",
        "dataset_freeze_id":"DEV-FREEZE-0001",
        "dataset_role":"DEVELOPMENT_DISCOVERY",
        "exit_optimization_performed":False,
        "artifacts":{
            by_regime_path.name:file_sha(by_regime_path),
            design_path.name:file_sha(design_path),
            report_path.name:file_sha(report_path),
        },
        "decision":"REVIEW_OBSERVABILITY_BEFORE_BUILDING_EXIT_THRESHOLD_SEARCH",
    }
    manifest_path.write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    print("Selected full-entry regimes       : 4")
    print(f"Robust clean 30s counts           : {robust_30s}")
    print(f"Robust clean 60s counts           : {robust_60s}")
    print(f"Robust clean 2m counts            : {robust_2m}")
    print(f"Robust clean 5m counts            : {robust_5m}")
    print(f"5m exit research supported now    : {'YES' if five_min_supported else 'NO'}")
    print(f"Main exit parameter grid now      : {'ALLOWED' if mainline_grid_allowed else 'BLOCKED'}")
    print("Exit thresholds tested            : NO")
    print("Profitability claim               : NO")
    print(f"Report                            : {report_path}")
    print("="*70)
    print("RESULT: PASS")

if __name__=="__main__":main()
