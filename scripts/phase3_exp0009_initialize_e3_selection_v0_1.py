from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e3_profit_taking_selection_v0_1 import SELECTION, validate_selection

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
        f.write("\n"); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path)

def main():
    validate_selection(SELECTION)
    exp=PROJECT_ROOT/"data/research/phase3/EXP-0009"
    results_path=exp/"EXP-0009_E3_profit_taking_results_v0_1.json"
    audit_path=exp/"EXP-0009_E3_profit_taking_postrun_audit_v0_1.json"
    design_path=exp/"EXP-0009_E3_profit_taking_design_v0_1.json"
    for p in (results_path,audit_path,design_path):
        if not p.exists():
            raise FileNotFoundError(p)

    results=json.loads(results_path.read_text(encoding="utf-8"))
    audit=json.loads(audit_path.read_text(encoding="utf-8"))
    if audit.get("result")!="PASS":
        raise RuntimeError("E3 postrun audit not PASS")
    if results.get("experiment_id")!="EXP-0009":
        raise RuntimeError("wrong E3 results experiment id")
    if audit["hashes"].get(results_path.name)!=sha(results_path):
        raise RuntimeError("E3 results hash does not match audit")
    if audit["hashes"].get(design_path.name)!=sha(design_path):
        raise RuntimeError("E3 design hash does not match audit")

    robust=[r for r in results["results"] if str(r["role"]).startswith("ROBUST_")]
    by_id={}
    for r in robust:
        by_id.setdefault(r["variant_id"],[]).append(r)

    required=[
        "TP05_T15s","TP10_T15s","TP15_T15s","TP20_T5s",
        "TP20_T15s","TP30_T15s","TP30_T5s","TP03_T15s"
    ]
    for vid in required:
        if len(by_id.get(vid,[]))!=3:
            raise RuntimeError(f"{vid}: expected exactly 3 robust rows")

    # Evidence summary is computed, not hand-copied.
    evidence={}
    for vid in required:
        rs=by_id[vid]
        scored=sum(int(r["scored_count"]) for r in rs)
        clean=sum(int(r["clean_path_count"]) for r in rs)
        hits=sum(int(r["tp_hit_count"]) for r in rs)
        mean=sum(float(r["exit_bps"]["mean"])*int(r["scored_count"]) for r in rs)/scored
        pos=sum(float(r["exit_bps"]["positive_rate"])*int(r["scored_count"]) for r in rs)/scored
        evidence[vid]={
            "robust_rows":3,
            "clean_path_count_sum":clean,
            "scored_count_sum":scored,
            "scored_coverage":scored/clean,
            "tp_hit_count_sum":hits,
            "tp_hit_rate_clean_paths":hits/clean,
            "weighted_mean_exit_bps":mean,
            "weighted_positive_rate":pos,
            "role_p50_exit_bps":[int(r["exit_bps"]["p50"]) for r in rs],
            "role_p10_exit_bps":[int(r["exit_bps"]["p10"]) for r in rs],
        }

    payload=dict(SELECTION)
    payload["source_binding"]={
        "results_file":results_path.name,
        "results_sha256":sha(results_path),
        "design_file":design_path.name,
        "design_sha256":sha(design_path),
        "postrun_audit_file":audit_path.name,
        "postrun_audit_sha256":sha(audit_path),
    }
    payload["evidence"]=evidence

    out=exp/"EXP-0009_E3_profit_taking_selection_v0_1.json"
    if out.exists():
        existing=json.loads(out.read_text(encoding="utf-8"))
        if existing!=payload:
            raise RuntimeError("existing E3 selection differs; refusing overwrite")
        action="ALREADY_LOCKED_IDENTICALLY"
    else:
        atomic(out,payload)
        action="LOCKED"

    print("EXP-0009 E3 PROFIT-TAKING SELECTION INITIALIZER v0.1")
    print("="*82)
    print(f"Action                       : {action}")
    print("Primary mainline anchor      : TP10 + T15s")
    print("Mainline neighbors           : TP5 + T15s / TP15 + T15s")
    print("Sensitivity only             : TP20 + T5s")
    print("High TP hypotheses           : DEFERRED_NOT_REJECTED")
    print("Hard stop promoted           : NO")
    print("Entry A/B/C/D changed        : NO")
    print("Profitability claim          : NO")
    e=evidence["TP10_T15s"]
    print(f"TP10/T15s scored coverage    : {100*e['scored_coverage']:.2f}%")
    print(f"TP10/T15s TP hit rate        : {100*e['tp_hit_rate_clean_paths']:.2f}%")
    print(f"TP10/T15s weighted mean      : {e['weighted_mean_exit_bps']:.2f} bps GROSS REF")
    print(f"TP10/T15s weighted positive  : {100*e['weighted_positive_rate']:.2f}%")
    print(f"Output                       : {out}")
    print("="*82)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
