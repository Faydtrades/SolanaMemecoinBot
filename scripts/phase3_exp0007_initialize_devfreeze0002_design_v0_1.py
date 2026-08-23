from __future__ import annotations
import json, hashlib, os
from pathlib import Path
import sys
PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.devfreeze0002_design_v0_1 import DESIGN, validate_design

def canonical(x): return json.dumps(x,sort_keys=True,separators=(",",":"),ensure_ascii=True)
def write_atomic(p,payload):
    p.parent.mkdir(parents=True,exist_ok=True)
    tmp=p.with_suffix(p.suffix+".tmp")
    with tmp.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(payload,f,indent=2,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
    os.replace(tmp,p)

def main():
    validate_design(DESIGN)
    core=dict(DESIGN)
    payload=dict(core)
    payload["deterministic_design_sha256"]=hashlib.sha256(canonical(core).encode()).hexdigest()
    p=PROJECT_ROOT/"data/research/phase3/EXP-0007/DEV-FREEZE-0002_design_v0_1.json"
    if p.exists():
        old=json.loads(p.read_text(encoding="utf-8"))
        if old!=payload: raise RuntimeError("existing DEV-FREEZE-0002 design differs")
    else:
        write_atomic(p,payload)
    print("DEV-FREEZE-0002 DESIGN INITIALIZER v0.1")
    print("="*60)
    print("Design status              : LOCKED BEFORE NEW DATA RESULTS")
    print("Required tail after t0     : 10 minutes")
    print("Entry A/B/C/D              : FIXED")
    print("Future/outcome filter      : NO")
    print("Immutable active-session freeze: FORBIDDEN")
    print(f"Output                     : {p}")
    print("="*60)
    print("RESULT: PASS")
if __name__=="__main__":main()
