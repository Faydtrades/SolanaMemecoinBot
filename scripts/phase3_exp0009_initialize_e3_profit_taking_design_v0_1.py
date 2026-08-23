from __future__ import annotations
import json, os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e3_profit_taking_design_v0_1 import DESIGN, validate_design

def atomic(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp=path.with_suffix(path.suffix+".tmp")
    with tmp.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(payload,f,indent=2,sort_keys=True)
        f.write("\n"); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path)

def main():
    validate_design(DESIGN)
    out=PROJECT_ROOT/"data/research/phase3/EXP-0009/EXP-0009_E3_profit_taking_design_v0_1.json"
    if out.exists():
        existing=json.loads(out.read_text(encoding="utf-8"))
        if existing != DESIGN:
            raise RuntimeError("existing E3 design differs; refusing overwrite")
        action="ALREADY_LOCKED_IDENTICALLY"
    else:
        atomic(out,DESIGN)
        action="LOCKED"

    print("EXP-0009 E3 PROFIT-TAKING DESIGN INITIALIZER v0.1")
    print("="*78)
    print(f"Action                       : {action}")
    print("TP grid                      : +3,+5,+10,+15,+20,+30 %")
    print("Primary fallback             : 15s")
    print("Sensitivity fallback         : 5s")
    print("Hard stop                    : NOT USED")
    print("Trailing                     : NOT USED")
    print("Partial/multi-TP             : NOT USED")
    print("Entry A/B/C/D changed        : NO")
    print("Profitability claim          : NO")
    print(f"Output                       : {out}")
    print("="*78)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
