from __future__ import annotations
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.continuous_dev_readiness_v0_1 import (
    POLICY,ReadinessTier,RegimeReadiness,evaluate
)

def rows(count,rate):
    return {
        r:RegimeReadiness(r,count,count,rate)
        for r in ("ROBUST_1","ROBUST_2","ROBUST_3")
    }

def check(cond,label):
    if not cond:raise AssertionError(label)
    print(f"[PASS] {label}")

def main():
    print("PHASE 3 / EXP-0007 CONTINUOUS DEV READINESS SELFTEST v0.1")
    print("="*70)
    check(POLICY["duration_policy"]["fixed_wall_clock_duration_required"] is False,
          "readiness is evidence-based, not an arbitrary fixed duration")
    check(POLICY["locked_before_new_dataset_results"] is True,
          "readiness gates locked before new data results")
    check(POLICY["E1_path_characterization_gate"]["exit_threshold_optimization_allowed"] is False,
          "E1 does not permit exit threshold tuning")
    check(POLICY["E2_coarse_exit_search_gate"]["exit_threshold_optimization_allowed"] is True,
          "E2 may permit only coarse pre-locked exit-family research")

    r=evaluate(rows(29,0.95))
    check(r.tier==ReadinessTier.NOT_READY,"29 clean paths/regime is below E1 gate")
    r=evaluate(rows(30,0.79))
    check(r.tier==ReadinessTier.NOT_READY,"clean-rate gate cannot be bypassed")
    r=evaluate(rows(30,0.80))
    check(r.tier==ReadinessTier.E1_PATH_CHARACTERIZATION_READY,
          "30 clean paths and 80% rate reaches E1")
    r=evaluate(rows(99,0.99))
    check(r.tier==ReadinessTier.E1_PATH_CHARACTERIZATION_READY,
          "99 paths does not prematurely unlock E2")
    r=evaluate(rows(100,0.90))
    check(r.tier==ReadinessTier.E2_COARSE_EXIT_SEARCH_READY,
          "100 clean paths and 90% rate reaches E2")
    check(evaluate({"ROBUST_1":RegimeReadiness("ROBUST_1",100,100,1.0)}).tier
          ==ReadinessTier.NOT_READY,"all three robust regimes are required")
    print("="*70)
    print("RESULT: PASS")

if __name__=="__main__":main()
