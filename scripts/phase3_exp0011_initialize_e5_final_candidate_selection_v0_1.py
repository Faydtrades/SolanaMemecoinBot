from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e5_final_candidate_selection_v0_1 import SELECTION,validate_selection

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

def aggregate(rows):
    scored=sum(int(r["scored_count"]) for r in rows)
    clean=sum(int(r["clean_path_count"]) for r in rows)
    wmean=sum(float(r["exit_bps"]["mean"])*int(r["scored_count"]) for r in rows)/scored
    wpos=sum(float(r["exit_bps"]["positive_rate"])*int(r["scored_count"]) for r in rows)/scored
    return {
        "robust_rows":len(rows),
        "clean_path_count_sum":clean,
        "scored_count_sum":scored,
        "scored_coverage":scored/clean,
        "weighted_mean_exit_bps":wmean,
        "weighted_positive_rate":wpos,
        "role_p10_exit_bps":[int(r["exit_bps"]["p10"]) for r in rows],
        "role_p25_exit_bps":[int(r["exit_bps"]["p25"]) for r in rows],
        "role_p50_exit_bps":[int(r["exit_bps"]["p50"]) for r in rows],
    }

def main():
    validate_selection(SELECTION)
    exp=PROJECT_ROOT/"data/research/phase3/EXP-0011"
    results_path=exp/"EXP-0011_E5_small_interaction_results_v0_1.json"
    audit_path=exp/"EXP-0011_E5_small_interaction_postrun_audit_v0_1.json"
    design_path=exp/"EXP-0011_E5_small_interaction_design_v0_1.json"

    for p in (results_path,audit_path,design_path):
        if not p.exists():
            raise FileNotFoundError(p)

    results=json.loads(results_path.read_text(encoding="utf-8"))
    audit=json.loads(audit_path.read_text(encoding="utf-8"))
    if audit.get("result")!="PASS":
        raise RuntimeError("E5 postrun audit not PASS")
    if audit["hashes"].get(results_path.name)!=sha(results_path):
        raise RuntimeError("E5 result hash mismatch vs audit")
    if audit["hashes"].get(design_path.name)!=sha(design_path):
        raise RuntimeError("E5 design hash mismatch vs audit")

    robust=[r for r in results["results"] if str(r["role"]).startswith("ROBUST_")]
    by_id={}
    for r in robust:
        by_id.setdefault(r["variant_id"],[]).append(r)

    required=[
        "REF_T15","E3_TP10_T15","E4_ACT10_GB03_T15",
        "TP10_ACT05_GB03_T15","TP15_ACT10_GB03_T15",
        "TP10_SL10_T15","ACT10_GB03_SL10_T15",
        "TP10_ACT05_GB03_SL10_T15","SENS_TP20_T5"
    ]
    for vid in required:
        if len(by_id.get(vid,[]))!=3:
            raise RuntimeError(f"{vid}: expected exactly 3 robust rows")

    evidence={vid:aggregate(by_id[vid]) for vid in required}
    payload=dict(SELECTION)
    payload["source_binding"]={
        "E5_results_file":results_path.name,
        "E5_results_sha256":sha(results_path),
        "E5_design_file":design_path.name,
        "E5_design_sha256":sha(design_path),
        "E5_postrun_audit_file":audit_path.name,
        "E5_postrun_audit_sha256":sha(audit_path),
    }
    payload["evidence"]=evidence

    out=exp/"EXP-0011_E5_final_candidate_selection_v0_1.json"
    if out.exists():
        existing=json.loads(out.read_text(encoding="utf-8"))
        if existing!=payload:
            raise RuntimeError("existing E5 final selection differs; refusing overwrite")
        action="ALREADY_LOCKED_IDENTICALLY"
    else:
        atomic(out,payload)
        action="LOCKED"

    a=evidence["E3_TP10_T15"]
    b=evidence["E4_ACT10_GB03_T15"]
    c=evidence["SENS_TP20_T5"]

    print("EXP-0011 E5 FINAL CANDIDATE SELECTION INITIALIZER v0.1")
    print("="*90)
    print(f"Action                         : {action}")
    print("FINAL-A                        : E3 TP10 + T15s")
    print("FINAL-B                        : E4 ACT10 + GB3 + T15s")
    print("SENS-C                         : TP20 + T5s (sensitivity only)")
    print("E5 interaction promoted        : NO")
    print("SL10 promoted                  : NO")
    print("Single DEV-data winner         : NO")
    print("Further DEV-freeze exit tuning : NO")
    print("Entry A/B/C/D changed          : NO")
    print("Profitability claim            : NO")
    print("-"*90)
    print(f"FINAL-A coverage               : {100*a['scored_coverage']:.2f}%")
    print(f"FINAL-A weighted mean          : {a['weighted_mean_exit_bps']:.2f} bps GROSS REF")
    print(f"FINAL-A weighted positive      : {100*a['weighted_positive_rate']:.2f}%")
    print(f"FINAL-A robust p50             : {a['role_p50_exit_bps']}")
    print(f"FINAL-B coverage               : {100*b['scored_coverage']:.2f}%")
    print(f"FINAL-B weighted mean          : {b['weighted_mean_exit_bps']:.2f} bps GROSS REF")
    print(f"FINAL-B weighted positive      : {100*b['weighted_positive_rate']:.2f}%")
    print(f"FINAL-B robust p50             : {b['role_p50_exit_bps']}")
    print(f"SENS-C coverage                : {100*c['scored_coverage']:.2f}%")
    print(f"SENS-C weighted mean           : {c['weighted_mean_exit_bps']:.2f} bps GROSS REF")
    print(f"SENS-C weighted positive       : {100*c['weighted_positive_rate']:.2f}%")
    print(f"Output                         : {out}")
    print("="*90)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
