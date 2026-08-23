from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e4_trailing_selection_v0_1 import SELECTION,validate_selection

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
    activated=sum(int(r["activation_count"]) for r in rows)
    trail=sum(int(r["trail_exit_count"]) for r in rows)
    mean=sum(float(r["exit_bps"]["mean"])*int(r["scored_count"]) for r in rows)/scored
    positive=sum(float(r["exit_bps"]["positive_rate"])*int(r["scored_count"]) for r in rows)/scored
    return {
        "robust_rows":len(rows),
        "clean_path_count_sum":clean,
        "scored_count_sum":scored,
        "scored_coverage":scored/clean,
        "activation_count_sum":activated,
        "activation_rate_clean_paths":activated/clean,
        "trail_exit_count_sum":trail,
        "trail_exit_rate_clean_paths":trail/clean,
        "trail_exit_rate_activated":trail/activated if activated else None,
        "weighted_mean_exit_bps":mean,
        "weighted_positive_rate":positive,
        "role_p50_exit_bps":[int(r["exit_bps"]["p50"]) for r in rows],
        "role_p10_exit_bps":[int(r["exit_bps"]["p10"]) for r in rows],
    }

def main():
    validate_selection(SELECTION)
    exp10=PROJECT_ROOT/"data/research/phase3/EXP-0010"
    exp9=PROJECT_ROOT/"data/research/phase3/EXP-0009"
    results_path=exp10/"EXP-0010_E4_trailing_results_v0_1.json"
    audit_path=exp10/"EXP-0010_E4_trailing_postrun_audit_v0_1.json"
    design_path=exp10/"EXP-0010_E4_trailing_design_v0_1.json"
    e3_selection_path=exp9/"EXP-0009_E3_profit_taking_selection_v0_1.json"

    for p in (results_path,audit_path,design_path,e3_selection_path):
        if not p.exists():
            raise FileNotFoundError(p)

    results=json.loads(results_path.read_text(encoding="utf-8"))
    audit=json.loads(audit_path.read_text(encoding="utf-8"))
    if audit.get("result")!="PASS":
        raise RuntimeError("E4 audit not PASS")
    if results.get("experiment_id")!="EXP-0010":
        raise RuntimeError("wrong E4 results experiment id")
    if audit["hashes"].get(results_path.name)!=sha(results_path):
        raise RuntimeError("E4 results hash mismatch vs audit")
    if audit["hashes"].get(design_path.name)!=sha(design_path):
        raise RuntimeError("E4 design hash mismatch vs audit")

    e3=json.loads(e3_selection_path.read_text(encoding="utf-8"))
    if e3["mainline"]["primary_anchor"]["variant_id"]!="TP10_T15s":
        raise RuntimeError("upstream E3 primary anchor changed")

    robust=[r for r in results["results"] if str(r["role"]).startswith("ROBUST_")]
    by_id={}
    for r in robust:
        by_id.setdefault(r["variant_id"],[]).append(r)

    required=["ACT05_GB03","ACT10_GB03","ACT10_GB05","ACT05_GB05"]
    for vid in required:
        if len(by_id.get(vid,[]))!=3:
            raise RuntimeError(f"{vid}: expected exactly 3 robust rows")

    evidence={vid:aggregate(by_id[vid]) for vid in required}

    payload=dict(SELECTION)
    payload["source_binding"]={
        "results_file":results_path.name,
        "results_sha256":sha(results_path),
        "design_file":design_path.name,
        "design_sha256":sha(design_path),
        "postrun_audit_file":audit_path.name,
        "postrun_audit_sha256":sha(audit_path),
        "EXP-0009_selection_file":e3_selection_path.name,
        "EXP-0009_selection_sha256":sha(e3_selection_path),
    }
    payload["evidence"]=evidence

    out=exp10/"EXP-0010_E4_trailing_selection_v0_1.json"
    if out.exists():
        existing=json.loads(out.read_text(encoding="utf-8"))
        if existing!=payload:
            raise RuntimeError("existing E4 selection differs; refusing overwrite")
        action="ALREADY_LOCKED_IDENTICALLY"
    else:
        atomic(out,payload)
        action="LOCKED"

    e=evidence["ACT10_GB03"]
    print("EXP-0010 E4 TRAILING SELECTION INITIALIZER v0.1")
    print("="*84)
    print(f"Action                       : {action}")
    print("Primary trailing anchor      : ACT10 + GB3 + T15s")
    print("Lower activation neighbor    : ACT5 + GB3 + T15s")
    print("Wider giveback neighbor      : ACT10 + GB5 + T15s")
    print("Strong plateau context       : ACT5-10 / GB3-5")
    print("E3 TP10/T15s replaced        : NO")
    print("Hard stop promoted           : NO")
    print("Entry A/B/C/D changed        : NO")
    print("Profitability claim          : NO")
    print(f"ACT10/GB3 scored coverage    : {100*e['scored_coverage']:.2f}%")
    print(f"ACT10/GB3 activation rate    : {100*e['activation_rate_clean_paths']:.2f}%")
    print(f"ACT10/GB3 trail-exit rate    : {100*e['trail_exit_rate_clean_paths']:.2f}%")
    print(f"ACT10/GB3 weighted mean      : {e['weighted_mean_exit_bps']:.2f} bps GROSS REF")
    print(f"ACT10/GB3 weighted positive  : {100*e['weighted_positive_rate']:.2f}%")
    print(f"Output                       : {out}")
    print("="*84)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
