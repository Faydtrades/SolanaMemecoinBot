from __future__ import annotations
import json, os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.devfreeze0002_design_v0_2 import DESIGN, validate_design

def write_atomic(path,payload):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+".tmp")
    with tmp.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(payload,f,indent=2,sort_keys=True)
        f.write("\n"); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path)

def main():
    validate_design(DESIGN)
    out=PROJECT_ROOT/"data/research/phase3/EXP-0007/DEV-FREEZE-0002_design_v0_2.json"
    if out.exists():
        existing=json.loads(out.read_text(encoding="utf-8"))
        if existing != DESIGN:
            raise RuntimeError("existing v0.2 design differs; refusing overwrite")
        print("Design already initialized identically.")
    else:
        write_atomic(out,DESIGN)

    print("DEV-FREEZE-0002 COVERAGE-AMENDED DESIGN INITIALIZER v0.2")
    print("="*72)
    print(f"Design id                   : {DESIGN['design_id']}")
    print(f"Supersedes                  : {DESIGN['supersedes_design_id']}")
    print("Reason                      : brief WS gaps; preserve unrelated clean windows")
    print("Token clean-window guard    : t0 -> t0+10m inside ONE clean interval")
    print("Gaps ignored                : NO")
    print("Entry A/B/C/D               : FIXED")
    print("Outcome/PnL used to amend   : NO")
    print(f"Output                      : {out}")
    print("="*72)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
