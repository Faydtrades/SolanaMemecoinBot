from __future__ import annotations
import json,os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e2_coarse_exit_design_v0_1 import DESIGN,validate_design

def atomic(p,x):
    p.parent.mkdir(parents=True,exist_ok=True)
    t=p.with_suffix(p.suffix+".tmp")
    with t.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(x,f,indent=2,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
    os.replace(t,p)

def main():
    validate_design(DESIGN)
    out=PROJECT_ROOT/"data/research/phase3/EXP-0008/EXP-0008_E2_coarse_exit_design_v0_1.json"
    if out.exists():
        existing=json.loads(out.read_text(encoding="utf-8"))
        if existing!=DESIGN:
            raise RuntimeError("existing E2 design differs; refusing overwrite")
        action="ALREADY_LOCKED_IDENTICALLY"
    else:
        atomic(out,DESIGN);action="LOCKED"
    print("EXP-0008 E2 COARSE EXIT DESIGN INITIALIZER v0.1")
    print("="*76)
    print(f"Action                      : {action}")
    print("Hard stops                  : -10,-20,-30,-40,-50,-60 %")
    print("Time stops                  : 5s,15s,30s,60s,2m,5m")
    print("Families combined           : NO")
    print("TP/trailing tested          : NO")
    print("Entry A/B/C/D changed       : NO")
    print("Profitability claim         : NO")
    print(f"Output                      : {out}")
    print("="*76)
    print("RESULT: PASS")
if __name__=="__main__":main()
