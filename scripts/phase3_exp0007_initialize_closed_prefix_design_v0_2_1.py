from __future__ import annotations
import json,os
from pathlib import Path
import sys
PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))
from phase3.devfreeze0002_closed_prefix_design_v0_2_1 import DESIGN,validate_design

def atomic(p,x):
    p.parent.mkdir(parents=True,exist_ok=True)
    t=p.with_suffix(p.suffix+".tmp")
    with t.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(x,f,indent=2,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
    os.replace(t,p)

def main():
    validate_design(DESIGN)
    out=PROJECT_ROOT/"data/research/phase3/EXP-0007/DEV-FREEZE-0002_closed_prefix_design_v0_2_1.json"
    if out.exists():
        existing=json.loads(out.read_text(encoding="utf-8"))
        if existing!=DESIGN:
            raise RuntimeError("existing design differs; refusing overwrite")
        print("Design already initialized identically.")
    else:
        atomic(out,DESIGN)
    print("DEV-FREEZE-0002 CLOSED-PREFIX DESIGN INITIALIZER v0.2.1")
    print("="*74)
    print(f"Design id                   : {DESIGN['design_id']}")
    print(f"Supersedes                  : {DESIGN['supersedes_design_id']}")
    print("Cutoff rule                 : last definitely closed coverage point")
    print("Uncertain tail              : DISCARDED")
    print("Explicit gaps               : RETAINED / NOT IGNORED")
    print("Forward outcomes used       : NO")
    print("Entry A/B/C/D               : FIXED")
    print(f"Output                      : {out}")
    print("="*74)
    print("RESULT: PASS")
if __name__=="__main__":
    main()
