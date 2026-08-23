from __future__ import annotations
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT/"src"))

from phase3.exit_research_design_v0_1 import build_design

def check(cond, label):
    if not cond:
        raise AssertionError(label)
    print(f"[PASS] {label}")

def main():
    print("PHASE 3 / EXP-0006 EXIT RESEARCH DESIGN SELFTEST v0.1")
    print("="*66)
    d=build_design("0"*64)
    check(d["design_id"]=="PHASE-3-EXIT-DESIGN-001","design ID locked")
    check(d["status"]=="LOCKED_BEFORE_EXIT_OPTIMIZATION_RESULTS","design locked before exit results")
    check(len(d["staged_exit_plan"])==6,"E0-E5 staged exit plan present")
    check(d["staged_exit_plan"][0]["exit_threshold_optimization"] is False,"E0 has no exit tuning")
    check(d["staged_exit_plan"][1]["exit_threshold_optimization"] is False,"E1 foundation has no exit tuning")
    forbidden=" ".join(d["what_EXP0006_must_not_do"]).lower()
    check("stop-loss" in forbidden and "take-profit" in forbidden,"EXP-0006 cannot choose SL/TP")
    principles=" ".join(d["locked_principles"]).lower()
    check("-30%" in principles,"manual -30% stop remains hypothesis only")
    check("fees" in principles and "slippage" in principles and "latency" in principles,
          "execution friction remains later")
    print("="*66)
    print("RESULT: PASS")
if __name__=="__main__":main()
