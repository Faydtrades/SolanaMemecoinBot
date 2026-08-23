from __future__ import annotations
import hashlib,json,os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.continuous_dev_readiness_v0_1 import POLICY
from phase3.exit_path_replay_v0_1 import SCHEMA_VERSION as EXIT_PATH_SCHEMA

def canonical(v):
    return json.dumps(v,sort_keys=True,separators=(",",":"),ensure_ascii=True)

def stable(v):
    return hashlib.sha256(canonical(v).encode("utf-8")).hexdigest()

def write_atomic(p,payload):
    p.parent.mkdir(parents=True,exist_ok=True)
    tmp=p.with_suffix(p.suffix+".tmp")
    with tmp.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(payload,f,indent=2,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
    os.replace(tmp,p)

def main():
    out=PROJECT_ROOT/"data/research/phase3/EXP-0007"
    design={
        "schema_version":"P3-EXP0007-FOUNDATION-DESIGN-0.1",
        "experiment_id":"EXP-0007",
        "status":"FOUNDATION_INITIALIZED_NOT_COMPLETE",
        "role":"EXIT_PATH_REPLAY_FOUNDATION_AND_CONTINUOUS_DEV_READINESS",
        "source_experiment":"EXP-0006",
        "exit_path_replay_schema":EXIT_PATH_SCHEMA,
        "entry_regimes":"locked EXP-0005 full-entry control + ROBUST_1/2/3",
        "mainline_exit_threshold_optimization_allowed":False,
        "objectives":[
            "deterministic causal post-entry path construction",
            "first observed passage semantics without interpolation",
            "explicit gap/unknown/GAP_RECOVERY/continuity handling",
            "evidence-based readiness gate for a fresh continuous development freeze",
        ],
        "non_objectives":[
            "choosing SL","choosing TP","choosing trailing stop","choosing time stop",
            "fees/slippage/latency","realistic net PnL","profitability claim",
        ],
        "next_operational_step":(
            "Keep the production collector running continuously. Use the readiness policy "
            "to decide when a fresh immutable development freeze is worth creating; duration "
            "alone is not the gate."
        ),
    }
    design["deterministic_design_sha256"]=stable(design)
    out.mkdir(parents=True,exist_ok=True)
    dp=out/"EXP-0007_exit_path_foundation_design_v0_1.json"
    pp=out/"EXP-0007_continuous_dev_readiness_policy_v0_1.json"

    if dp.exists():
        old=json.loads(dp.read_text(encoding="utf-8"))
        if old!=design:raise RuntimeError("existing EXP-0007 design differs")
    else:write_atomic(dp,design)

    if pp.exists():
        old=json.loads(pp.read_text(encoding="utf-8"))
        if old!=POLICY:raise RuntimeError("existing readiness policy differs")
    else:write_atomic(pp,POLICY)

    print("EXP-0007 FOUNDATION INITIALIZER v0.1")
    print("="*62)
    print("ExitPathReplay foundation : INITIALIZED")
    print("Readiness policy          : LOCKED BEFORE NEW DATA")
    print("E1 gate                   : >=30 clean 5m / robust regime AND >=80% clean rate")
    print("E2 gate                   : >=100 clean 5m / robust regime AND >=90% clean rate")
    print("Fixed duration gate       : NO")
    print("Exit threshold tuning     : BLOCKED")
    print(f"Output                    : {out}")
    print("="*62)
    print("RESULT: PASS")

if __name__=="__main__":main()
