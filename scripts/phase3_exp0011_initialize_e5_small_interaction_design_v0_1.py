from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e5_small_interaction_design_v0_1 import DESIGN, validate_design

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
    validate_design(DESIGN)

    exp9=PROJECT_ROOT/"data/research/phase3/EXP-0009"
    exp10=PROJECT_ROOT/"data/research/phase3/EXP-0010"
    exp8=PROJECT_ROOT/"data/research/phase3/EXP-0008"

    e3=exp9/"EXP-0009_E3_profit_taking_selection_v0_1.json"
    e4=exp10/"EXP-0010_E4_trailing_selection_v0_1.json"
    e2_results=exp8/"EXP-0008_E2_coarse_exit_results_v0_1.json"
    e2_audit=exp8/"EXP-0008_E2_coarse_exit_postrun_audit_v0_1.json"

    for p in (e3,e4,e2_results,e2_audit):
        if not p.exists():
            raise FileNotFoundError(p)

    e3p=json.loads(e3.read_text(encoding="utf-8"))
    e4p=json.loads(e4.read_text(encoding="utf-8"))
    e2a=json.loads(e2_audit.read_text(encoding="utf-8"))

    if e3p["mainline"]["primary_anchor"]["variant_id"]!="TP10_T15s":
        raise RuntimeError("E3 primary anchor changed")
    if e4p["mainline"]["primary_anchor"]["variant_id"]!="ACT10_GB03":
        raise RuntimeError("E4 primary anchor changed")
    if e2a.get("result")!="PASS":
        raise RuntimeError("E2 audit not PASS")

    payload=dict(DESIGN)
    payload["source_binding"]={
        "EXP-0009_selection_file":e3.name,
        "EXP-0009_selection_sha256":sha(e3),
        "EXP-0010_selection_file":e4.name,
        "EXP-0010_selection_sha256":sha(e4),
        "EXP-0008_results_file":e2_results.name,
        "EXP-0008_results_sha256":sha(e2_results),
        "EXP-0008_audit_file":e2_audit.name,
        "EXP-0008_audit_sha256":sha(e2_audit),
    }

    out=PROJECT_ROOT/"data/research/phase3/EXP-0011/EXP-0011_E5_small_interaction_design_v0_1.json"
    if out.exists():
        existing=json.loads(out.read_text(encoding="utf-8"))
        if existing!=payload:
            raise RuntimeError("existing E5 design differs; refusing overwrite")
        action="ALREADY_LOCKED_IDENTICALLY"
    else:
        atomic(out,payload)
        action="LOCKED"

    print("EXP-0011 E5 SMALL INTERACTION DESIGN INITIALIZER v0.1")
    print("="*88)
    print(f"Action                       : {action}")
    print("Variants                     : 9 hand-enumerated")
    print("Cartesian search             : NO")
    print("E3 primary reference         : TP10 + T15s")
    print("E4 primary reference         : ACT10 + GB3 + T15s")
    print("TP/trailing interactions     : 2")
    print("SL10 interactions            : 3")
    print("T5 sensitivity reference     : TP20 + T5s")
    print("Entry A/B/C/D changed        : NO")
    print("Profitability claim          : NO")
    print(f"Bound E3 selection SHA256    : {payload['source_binding']['EXP-0009_selection_sha256']}")
    print(f"Bound E4 selection SHA256    : {payload['source_binding']['EXP-0010_selection_sha256']}")
    print(f"Output                       : {out}")
    print("="*88)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
