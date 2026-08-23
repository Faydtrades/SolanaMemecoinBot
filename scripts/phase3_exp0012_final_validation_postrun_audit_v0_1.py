from __future__ import annotations

import hashlib
import json
from pathlib import Path

def sha(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):
            h.update(c)
    return h.hexdigest()

def req(cond,msg):
    if not cond:
        print(f"[FAIL] {msg}")
        print("RESULT: FAIL")
        raise SystemExit(1)

def main():
    root=Path(__file__).resolve().parents[1]
    exp=root/"data/research/phase3/EXP-0012"

    dp=exp/"EXP-0012_final_phase3_validation_design_v0_1.json"
    rp=exp/"EXP-0012_final_phase3_validation_results_v0_1.json"
    cp=exp/"EXP-0012_final_phase3_validation_by_regime_v0_1.csv"

    for p in (dp,rp,cp):
        req(p.exists(),f"missing {p.name}")

    design=json.loads(dp.read_text(encoding="utf-8"))
    result=json.loads(rp.read_text(encoding="utf-8"))

    req(design["status"]=="LOCKED_BEFORE_FINAL_VALIDATION_RUN",
        "design was not prelocked")
    req(result["phase3_final_validation_status"]=="PHASE3_FINAL_VALIDATION_PASS",
        "final validation status not PASS")
    req(result["phase4_build_allowed"] is True,
        "Phase 4 build is not allowed")
    req(result["source_hash_binding"]=="PASS",
        "source hash binding failed")
    req(result["source_validation"]["sqlite_quick_check"]=="PASS",
        "SQLite quick_check failed")
    req(result["source_validation"]["coverage_binding"]=="PASS",
        "coverage binding failed")
    req(result["entry_candidate_counts"]==design["exact_expected_entry_counts"],
        "entry candidate counts differ from design")
    req(result["clean_5m_counts"]==design["exact_expected_clean_5m_counts"],
        "clean 5m counts differ from design")
    req(result["EXP_0011_exact_reproduction"]=="PASS",
        "EXP-0011 exact reproduction failed")
    req(result["cached_path_determinism"]=="PASS",
        "cached-path determinism failed")
    req(result["cached_path_evaluation_hash_pass1"]==
        result["cached_path_evaluation_hash_pass2"],
        "double evaluation hashes differ")
    req(len(result["locked_finalist_rows"])==12,
        "expected 12 finalist/regime rows")
    req(result["parameter_search_performed"] is False,
        "parameter search occurred")
    req(result["candidate_reselection_performed"] is False,
        "candidate reselection occurred")
    req(result["further_exit_tuning_performed"] is False,
        "further exit tuning occurred")
    req(result["profitability_claim_allowed"] is False,
        "profitability claim incorrectly allowed")

    report={
        "schema_version":"P3-FINAL-VALIDATION-AUDIT-0.1",
        "result":"PASS",
        "phase3_status":"COMPLETE_PENDING_CHECKPOINT_WRITE",
        "phase4_build_allowed":True,
        "checks":{
            "design_prelocked":True,
            "source_hash_binding":True,
            "sqlite_quick_check":True,
            "coverage_binding":True,
            "entry_exact_reproduction":True,
            "clean_5m_exact_reproduction":True,
            "EXP_0011_exact_reproduction":True,
            "double_cached_path_determinism":True,
            "exact_12_finalist_rows":True,
            "no_parameter_search":True,
            "no_candidate_reselection":True,
            "no_further_exit_tuning":True,
            "no_profitability_claim":True,
        },
        "deterministic_sha256":result["cached_path_evaluation_hash_pass1"],
        "hashes":{
            dp.name:sha(dp),
            rp.name:sha(rp),
            cp.name:sha(cp),
        },
    }

    out=exp/"EXP-0012_final_phase3_validation_postrun_audit_v0_1.json"
    out.write_text(
        json.dumps(report,indent=2,sort_keys=True)+"\n",
        encoding="utf-8",
    )

    print("EXP-0012 FINAL PHASE-3 VALIDATION POSTRUN AUDIT v0.1")
    print("="*94)
    print("Design prelocked               : PASS")
    print("Source hash binding            : PASS")
    print("Freeze / coverage validation   : PASS")
    print("Entry exact reproduction       : PASS")
    print("Clean 5m exact reproduction    : PASS")
    print("EXP-0011 exact reproduction    : PASS")
    print("Double cached-path determinism : PASS")
    print("Parameter search               : NO")
    print("Candidate reselection          : NO")
    print("Further exit tuning            : NO")
    print("Profitability claim            : NO")
    print("Phase-3 status                 : COMPLETE_PENDING_CHECKPOINT_WRITE")
    print("Phase-4 build allowed          : YES")
    print(f"Audit                           : {out}")
    print("="*94)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
