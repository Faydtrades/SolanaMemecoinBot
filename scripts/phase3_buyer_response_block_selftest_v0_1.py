from __future__ import annotations

from pathlib import Path
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT/"src"))

from phase3.buyer_response_block_research_v0_1 import (
    FIXED_D_RECLAIM, FIXED_D_RUNAWAY,
    generate_buyer_response_combinations,
    parameter_set_for_buyer_response,
    validate_locked_search,
    validate_selection,
)

LOCKED = {
    "decision_id":"PHASE-2-PARAM-SEARCH-001",
    "strategy_version_target":"v1.1",
    "full_cartesian_recommended":False,
    "naive_full_cartesian_count":13230000,
    "block_combination_counts":{
        "A_IMPULSE":175,"B_PULLBACK":21,"C_BUYER_RESPONSE_3S":150,"D_RECLAIM_RUNAWAY":24
    },
    "buyer_response":{
        "window_id":["3s"],
        "min_rebound_bps":[200,500,800,2300,7300,20700],
        "min_buys":[1,2,3,4,8],
        "min_net_flow_reserve_ppm":[0,6000,30000,93000,185000],
    },
}
SELECTION = {
    "experiment_id":"EXP-0003",
    "status":"LOCKED_FOR_EXP0004_BLOCK_C",
    "carry_forward":[
        {"role":"CONTROL","parameter_set_id":"FP1-EXP0003-B-005-CONTROL-R500-T2-U1-PD1000-9000",
         "min_return_bps":500,"min_trades_since_t0":2,"min_unique_buyers_since_t0":1,
         "min_depth_bps":1000,"max_depth_bps":9000},
        {"role":"ROBUST_1","parameter_set_id":"FP1-EXP0003-B-030-ROBUST_1-R1500-T5-U9-PD2500-6500",
         "min_return_bps":1500,"min_trades_since_t0":5,"min_unique_buyers_since_t0":9,
         "min_depth_bps":2500,"max_depth_bps":6500},
        {"role":"ROBUST_2","parameter_set_id":"FP1-EXP0003-B-051-ROBUST_2-R2000-T3-U5-PD2500-6500",
         "min_return_bps":2000,"min_trades_since_t0":3,"min_unique_buyers_since_t0":5,
         "min_depth_bps":2500,"max_depth_bps":6500},
        {"role":"ROBUST_3","parameter_set_id":"FP1-EXP0003-B-072-ROBUST_3-R5000-T3-U2-PD2500-6500",
         "min_return_bps":5000,"min_trades_since_t0":3,"min_unique_buyers_since_t0":2,
         "min_depth_bps":2500,"max_depth_bps":6500},
    ],
}

def check(cond,label):
    if not cond: raise AssertionError(label)
    print(f"[PASS] {label}")

def main():
    print("PHASE 3 / EXP-0004 BLOCK C SELFTEST v0.1")
    print("="*58)
    validate_locked_search(LOCKED)
    validate_selection(SELECTION)
    combos=generate_buyer_response_combinations(LOCKED,SELECTION)
    check(len(combos)==600,"4 A+B configurations x 150 C combinations = 600")
    check(len({c.parameter_set_id for c in combos})==600,"all parameter_set_id values unique")
    check({c.ab_role for c in combos}=={"CONTROL","ROBUST_1","ROBUST_2","ROBUST_3"},"all four A+B roles represented")
    check({c.min_rebound_bps for c in combos}=={200,500,800,2300,7300,20700},"rebound grid unchanged")
    check({c.min_buys for c in combos}=={1,2,3,4,8},"buy-count grid unchanged")
    check({c.min_net_flow_reserve_ppm for c in combos}=={0,6000,30000,93000,185000},"flow grid unchanged")
    for c in combos:
        p=parameter_set_for_buyer_response(c)
        if p.reclaim_parameters!=FIXED_D_RECLAIM: raise AssertionError(f"D reclaim varied {c.index}")
        if p.runaway_entry_parameters!=FIXED_D_RUNAWAY: raise AssertionError(f"D runaway varied {c.index}")
        if p.buyer_response_parameters["window_id"]!="3s": raise AssertionError("3s window changed")
    check(True,"D fixed for all 600 combinations")
    check(LOCKED["full_cartesian_recommended"] is False,"13.23m Cartesian remains forbidden")
    print("="*58)
    print("RESULT: PASS")
if __name__=="__main__":main()
