from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e4_trailing_design_v0_1 import DESIGN, validate_design

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

    exp9=PROJECT_ROOT/"data/research/phase3/EXP-0009"
    sel=exp9/"EXP-0009_E3_profit_taking_selection_v0_1.json"
    if not sel.exists():
        raise FileNotFoundError("E3 selection must be locked before E4 design")
    e3=json.loads(sel.read_text(encoding="utf-8"))
    if e3.get("status")!="POSTRUN_RESEARCH_CARRY_FORWARD":
        raise RuntimeError("E3 selection status mismatch")
    if e3["mainline"]["primary_anchor"]["variant_id"]!="TP10_T15s":
        raise RuntimeError("E3 primary anchor mismatch")
    if e3["sensitivity_only"]["variant_id"]!="TP20_T5s":
        raise RuntimeError("E3 sensitivity mismatch")

    payload=dict(DESIGN)
    payload["source_binding"]={
        "EXP-0009_selection_file":sel.name,
        "EXP-0009_selection_sha256":sha(sel),
    }

    out=PROJECT_ROOT/"data/research/phase3/EXP-0010/EXP-0010_E4_trailing_design_v0_1.json"
    if out.exists():
        existing=json.loads(out.read_text(encoding="utf-8"))
        if existing!=payload:
            raise RuntimeError("existing E4 design differs; refusing overwrite")
        action="ALREADY_LOCKED_IDENTICALLY"
    else:
        atomic(out,payload)
        action="LOCKED"

    print("EXP-0010 E4 TRAILING DESIGN INITIALIZER v0.1")
    print("="*82)
    print(f"Action                       : {action}")
    print("Activation grid              : +5,+10,+15 %")
    print("Trailing giveback grid       : 3,5,10 %")
    print("Fallback                     : 15s")
    print("Variants                     : 9")
    print("E3 TP mixed in               : NO")
    print("Hard stop                    : NOT USED")
    print("T5s sensitivity              : DEFERRED TO E5")
    print("Entry A/B/C/D changed        : NO")
    print("Profitability claim          : NO")
    print(f"Bound E3 selection SHA256    : {payload['source_binding']['EXP-0009_selection_sha256']}")
    print(f"Output                       : {out}")
    print("="*82)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
