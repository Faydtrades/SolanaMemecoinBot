from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

EXPECTED_SNAPSHOT_SHA256 = "44e3457b81a16d4e5e2670e24d758756a147867e1d81d627622157ad4bd5813f"

EXPECTED_ARTIFACTS = {
    "EXP-0006_exit_feasibility_by_regime_v0_1.csv":
        "42f62d93f31445786cc8772db05d7a13c1c3ce4f12470037e0020a99eaf6a6a6",
    "EXP-0006_exit_research_design_v0_1.json":
        "2f4900294a430c7831f9d3d19cc9dc422ca97861ae102bbc993b27c182d7006a",
    "EXP-0006_exit_feasibility_report_v0_1.json":
        "306d8778167227b0193c7140a2595f673a7d22a29f6bc09f0086154d988bb105",
    "EXP-0006_manifest_v0_1.json":
        "a7f0b5d9457cb37515386c4d3e4ee4eef9032a6916c7446cebeed4da6c242344",
    "EXP-0006_exit_feasibility_postrun_audit_v0_1.json":
        "f9fd00ce85c681a945ea2783cba58060e5786cd5d9a8da14738c21eb81127609",
}

def canonical_json(v: Any) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=True)

def stable_sha(v: Any) -> str:
    return hashlib.sha256(canonical_json(v).encode("utf-8")).hexdigest()

def file_sha(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def load_json(path: Path) -> dict[str, Any]:
    obj=json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj,dict):
        raise RuntimeError(f"{path} is not a JSON object")
    return obj

def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp=path.with_suffix(path.suffix+".tmp")
    with tmp.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(payload,f,indent=2,sort_keys=True,ensure_ascii=True)
        f.write("\n"); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path)

def require(cond: bool, msg: str) -> None:
    if not cond:
        print(f"[FAIL] {msg}")
        print("RESULT: FAIL")
        raise SystemExit(1)

def main() -> None:
    root=Path(__file__).resolve().parents[1]
    freeze=root/"data/research/phase2_research_freeze_v0_1"
    exp6=root/"data/research/phase3/EXP-0006"
    phase3=root/"data/research/phase3"
    registry_path=freeze/"phase2_experiment_registry_v0_1.json"
    freeze_db=freeze/"phase2_development_dataset_v0_1.sqlite3"

    print("EXP-0006 EXIT FEASIBILITY BOOKKEEPING v0.1")
    print("="*70)

    require(registry_path.exists(),"canonical registry missing")
    require(freeze_db.exists(),"DEV-FREEZE-0001 SQLite missing")
    require(file_sha(freeze_db)==EXPECTED_SNAPSHOT_SHA256,"freeze SQLite hash mismatch")

    for name,expected in EXPECTED_ARTIFACTS.items():
        p=exp6/name
        require(p.exists(),f"missing reviewed EXP-0006 artifact: {name}")
        require(file_sha(p)==expected,f"artifact hash mismatch: {name}")

    audit=load_json(exp6/"EXP-0006_exit_feasibility_postrun_audit_v0_1.json")
    require(audit.get("result")=="PASS","EXP-0006 postrun audit did not PASS")
    for key in (
        "exact_four_entry_regimes",
        "design_locked_before_exit_results",
        "no_exit_thresholds_tested",
        "no_profitability_claim",
        "five_minute_observability_gate_consistent",
    ):
        require(audit.get("checks",{}).get(key) is True,f"audit check failed: {key}")

    report=load_json(exp6/"EXP-0006_exit_feasibility_report_v0_1.json")
    clean=report["observability"]["robust_clean_counts"]
    require(clean["30s"]==[5,8,7],"unexpected robust clean 30s counts")
    require(clean["60s"]==[4,5,5],"unexpected robust clean 60s counts")
    require(clean["2m"]==[2,3,3],"unexpected robust clean 2m counts")
    require(clean["5m"]==[0,0,0],"unexpected robust clean 5m counts")
    conclusion=report["feasibility_conclusion"]
    require(conclusion["mainline_exit_parameter_grid_allowed_now"] is False,
            "mainline exit grid must be blocked")
    require(conclusion["five_minute_dependent_exit_research_supported_on_DEV_FREEZE_0001"] is False,
            "5m exit research must be unsupported on DEV-FREEZE-0001")
    claims=report["performance_claims"]
    require(all(claims[k] is False for k in (
        "exit_thresholds_tested","stop_loss_selected","take_profit_selected",
        "trailing_selected","time_stop_selected","fees_modeled","slippage_modeled",
        "latency_modeled","realistic_net_pnl_available","profitability_claim_allowed"
    )),"EXP-0006 performance claims invalid")

    decision={
        "schema_version":"P3-EXP0006-DECISION-0.1",
        "experiment_id":"EXP-0006",
        "status":"COMPLETE",
        "role":"EXIT_RESEARCH_DESIGN_AND_PATH_FEASIBILITY",
        "dataset":{
            "freeze_id":"DEV-FREEZE-0001",
            "role":"DEVELOPMENT_DISCOVERY",
            "untouched_oos":False,
        },
        "locked_result":{
            "robust_clean_30s":[5,8,7],
            "robust_clean_60s":[4,5,5],
            "robust_clean_2m":[2,3,3],
            "robust_clean_5m":[0,0,0],
            "mainline_exit_parameter_grid_allowed_on_DEV_FREEZE_0001":False,
        },
        "interpretation":(
            "DEV-FREEZE-0001 is adequate for the completed staged entry research but "
            "does not provide clean 5m post-entry paths for any robust full-entry regime. "
            "Do not optimize SL/TP/trailing/time-stop thresholds on this freeze."
        ),
        "next_step":{
            "experiment_id":"EXP-0007",
            "role":"EXIT_PATH_REPLAY_FOUNDATION_AND_CONTINUOUS_DEV_READINESS",
            "entry_parameters_remain_fixed":True,
            "exit_threshold_optimization_allowed":False,
        },
        "profitability_claim_allowed":False,
    }
    decision_path=exp6/"EXP-0006_locked_decision_v0_1.json"
    if decision_path.exists():
        require(load_json(decision_path)==decision,"existing EXP-0006 decision differs")
    else:
        write_json_atomic(decision_path,decision)
    decision_sha=file_sha(decision_path)

    registry=load_json(registry_path)
    core=dict(registry)
    stored=core.pop("deterministic_registry_sha256",None)
    require(stored==stable_sha(core),"registry deterministic hash invalid")
    exps=registry.get("experiments")
    require(isinstance(exps,list),"registry experiments invalid")

    existing=[x for x in exps if isinstance(x,dict) and x.get("experiment_id")=="EXP-0006"]
    entry={
        "experiment_id":"EXP-0006",
        "phase":"PHASE_3_EXIT_RESEARCH",
        "role":"EXIT_RESEARCH_DESIGN_AND_PATH_FEASIBILITY",
        "status":"COMPLETE",
        "dataset_freeze_id":"DEV-FREEZE-0001",
        "dataset_role":"DEVELOPMENT_DISCOVERY",
        "untouched_oos":False,
        "report_path":"data/research/phase3/EXP-0006/EXP-0006_exit_feasibility_report_v0_1.json",
        "report_sha256":EXPECTED_ARTIFACTS["EXP-0006_exit_feasibility_report_v0_1.json"],
        "manifest_path":"data/research/phase3/EXP-0006/EXP-0006_manifest_v0_1.json",
        "manifest_sha256":EXPECTED_ARTIFACTS["EXP-0006_manifest_v0_1.json"],
        "postrun_audit_path":"data/research/phase3/EXP-0006/EXP-0006_exit_feasibility_postrun_audit_v0_1.json",
        "postrun_audit_sha256":EXPECTED_ARTIFACTS["EXP-0006_exit_feasibility_postrun_audit_v0_1.json"],
        "locked_decision_path":"data/research/phase3/EXP-0006/EXP-0006_locked_decision_v0_1.json",
        "locked_decision_sha256":decision_sha,
        "summary":{
            "full_entry_regimes":4,
            "robust_clean_5m_counts":[0,0,0],
            "mainline_exit_grid_blocked":True,
            "exit_thresholds_tested":False,
            "realistic_net_pnl_available":False,
            "profitability_claim_allowed":False,
        },
    }

    if existing:
        require(len(existing)==1,"EXP-0006 appears more than once")
        require(existing[0]==entry,"existing EXP-0006 registry entry differs")
        require(registry.get("next_experiment_id")=="EXP-0007",
                "EXP-0006 registered but next ID is not EXP-0007")
        action="ALREADY_REGISTERED"
    else:
        require(registry.get("next_experiment_id")=="EXP-0006",
                "registry next ID must be EXP-0006 before registration")
        require(any(isinstance(x,dict) and x.get("experiment_id")=="EXP-0005"
                    and x.get("status")=="COMPLETE" for x in exps),
                "EXP-0005 predecessor missing")

        hist=phase3/"registry_history"; hist.mkdir(parents=True,exist_ok=True)
        backup=hist/"phase2_experiment_registry_v0_1_pre_EXP-0006.json"
        if not backup.exists():
            shutil.copy2(registry_path,backup)

        updated_core={
            "registry_schema_version":registry["registry_schema_version"],
            "freeze_id":registry["freeze_id"],
            "dataset_manifest_sha256":registry["dataset_manifest_sha256"],
            "locked_search_decision_id":registry["locked_search_decision_id"],
            "next_experiment_id":"EXP-0007",
            "experiments":exps+[entry],
        }
        updated=dict(updated_core)
        updated["deterministic_registry_sha256"]=stable_sha(updated_core)
        write_json_atomic(registry_path,updated)
        registry=load_json(registry_path)
        action="REGISTERED"

    final_core=dict(registry)
    final_hash=final_core.pop("deterministic_registry_sha256",None)
    require(final_hash==stable_sha(final_core),"final registry hash invalid")
    require(registry.get("next_experiment_id")=="EXP-0007","next ID not EXP-0007")
    require(file_sha(freeze_db)==EXPECTED_SNAPSHOT_SHA256,"DEV-FREEZE-0001 changed")

    print(f"Action                       : {action}")
    print("EXP-0006 status              : COMPLETE")
    print("Robust clean 5m paths        : [0, 0, 0]")
    print("Mainline exit grid           : BLOCKED ON DEV-FREEZE-0001")
    print("Registry next experiment ID  : EXP-0007")
    print(f"Locked decision SHA256       : {decision_sha}")
    print(f"Registry deterministic SHA256: {final_hash}")
    print("DEV-FREEZE-0001 SQLite       : UNCHANGED")
    print("Production DB touched        : NO")
    print("Exit thresholds tested       : NO")
    print("Profitability claim          : NO")
    print("="*70)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
