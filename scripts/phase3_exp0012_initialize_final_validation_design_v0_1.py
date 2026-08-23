from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.final_phase3_validation_design_v0_1 import DESIGN,validate_design

def sha(path: Path) -> str:
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
    exp11=PROJECT_ROOT/"data/research/phase3/EXP-0011"
    e5_results=exp11/"EXP-0011_E5_small_interaction_results_v0_1.json"
    e5_audit=exp11/"EXP-0011_E5_small_interaction_postrun_audit_v0_1.json"
    final_sel=exp11/"EXP-0011_E5_final_candidate_selection_v0_1.json"

    for p in (e5_results,e5_audit,final_sel):
        if not p.exists():
            raise FileNotFoundError(p)

    audit=json.loads(e5_audit.read_text(encoding="utf-8"))
    sel=json.loads(final_sel.read_text(encoding="utf-8"))
    if audit.get("result")!="PASS":
        raise RuntimeError("EXP-0011 postrun audit not PASS")
    if [x["variant_id"] for x in sel["final_mainline_candidates"]] != [
        "E3_TP10_T15","E4_ACT10_GB03_T15"
    ]:
        raise RuntimeError("EXP-0011 mainline finalist set mismatch")
    if sel["sensitivity_only"]["variant_id"]!="SENS_TP20_T5":
        raise RuntimeError("EXP-0011 sensitivity finalist mismatch")
    if sel["guardrails"]["no_further_exit_parameter_tuning_before_final_validation"] is not True:
        raise RuntimeError("EXP-0011 did not lock no-further-tuning guard")

    payload=dict(DESIGN)
    payload["source_binding"]={
        "EXP-0011_E5_results_file":e5_results.name,
        "EXP-0011_E5_results_sha256":sha(e5_results),
        "EXP-0011_E5_audit_file":e5_audit.name,
        "EXP-0011_E5_audit_sha256":sha(e5_audit),
        "EXP-0011_final_selection_file":final_sel.name,
        "EXP-0011_final_selection_sha256":sha(final_sel),
    }

    out=PROJECT_ROOT/"data/research/phase3/EXP-0012/EXP-0012_final_phase3_validation_design_v0_1.json"
    if out.exists():
        existing=json.loads(out.read_text(encoding="utf-8"))
        if existing!=payload:
            raise RuntimeError("existing EXP-0012 design differs; refusing overwrite")
        action="ALREADY_LOCKED_IDENTICALLY"
    else:
        atomic(out,payload)
        action="LOCKED"

    print("EXP-0012 FINAL PHASE-3 VALIDATION DESIGN INITIALIZER v0.1")
    print("="*92)
    print(f"Action                         : {action}")
    print("FINAL-A                        : E3 TP10 + T15s")
    print("FINAL-B                        : E4 ACT10 + GB3 + T15s")
    print("SENS-C                         : TP20 + T5s (sensitivity only)")
    print("Parameter search               : NO")
    print("Candidate reselection          : NO")
    print("Further DEV-freeze tuning      : NO")
    print("Independent end-to-end replay  : YES")
    print("Exact E5 reproduction required : YES")
    print("Double cached-path eval         : YES")
    print("Phase 4 gated on PASS           : YES")
    print("Profitability claim             : NO")
    print(f"Output                         : {out}")
    print("="*92)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
