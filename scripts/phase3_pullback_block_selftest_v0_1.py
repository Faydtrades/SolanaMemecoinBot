from __future__ import annotations

from pathlib import Path
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT/"src"))

from phase3.pullback_block_research_v0_1 import (
    FIXED_C, FIXED_D_RECLAIM, FIXED_D_RUNAWAY,
    generate_pullback_combinations, parameter_set_for_pullback,
    valid_pullback_pairs, validate_locked_search, validate_selection,
)

LOCKED = {
    "decision_id":"PHASE-2-PARAM-SEARCH-001",
    "strategy_version_target":"v1.1",
    "full_cartesian_recommended":False,
    "naive_full_cartesian_count":13230000,
    "max_entry_age_ms":[300000],
    "block_combination_counts":{"A_IMPULSE":175,"B_PULLBACK":21,"C_BUYER_RESPONSE_3S":150,"D_RECLAIM_RUNAWAY":24},
    "pullback":{
        "min_depth_bps":[1000,2500,3000,4000,6000],
        "max_depth_bps":[3500,5500,6000,6500,9000],
        "valid_min_max_pair_count":21,
    },
    "buyer_response":{
        "window_id":["3s"],
        "min_rebound_bps":[200,500,800,2300,7300,20700],
        "min_buys":[1,2,3,4,8],
        "min_net_flow_reserve_ppm":[0,6000,30000,93000,185000],
    },
}
SELECTION = {
    "experiment_id":"EXP-0002",
    "status":"LOCKED_FOR_EXP0003_BLOCK_B",
    "carry_forward":[
        {"role":"CONTROL","parameter_set_id":"FP1-EXP0002-A-001-R500-T2-B1","min_return_bps":500,"min_trades_since_t0":2,"min_unique_buyers_since_t0":1},
        {"role":"ROBUST_1","parameter_set_id":"FP1-EXP0002-A-040-R1500-T5-B9","min_return_bps":1500,"min_trades_since_t0":5,"min_unique_buyers_since_t0":9},
        {"role":"ROBUST_2","parameter_set_id":"FP1-EXP0002-A-059-R2000-T3-B5","min_return_bps":2000,"min_trades_since_t0":3,"min_unique_buyers_since_t0":5},
        {"role":"ROBUST_3","parameter_set_id":"FP1-EXP0002-A-107-R5000-T3-B2","min_return_bps":5000,"min_trades_since_t0":3,"min_unique_buyers_since_t0":2},
        {"role":"ROBUST_4","parameter_set_id":"FP1-EXP0002-A-115-R5000-T5-B9","min_return_bps":5000,"min_trades_since_t0":5,"min_unique_buyers_since_t0":9},
    ],
}

def check(cond, label):
    if not cond:
        raise AssertionError(label)
    print(f"[PASS] {label}")

def main():
    print("PHASE 3 / EXP-0003 BLOCK B SELFTEST v0.1")
    print("="*58)
    validate_locked_search(LOCKED)
    validate_selection(SELECTION)
    pairs = valid_pullback_pairs(LOCKED)
    check(len(pairs)==21, "exactly 21 valid locked pullback pairs")
    check(all(a<b for a,b in pairs), "all pullback min/max pairs satisfy min<max")
    combos = generate_pullback_combinations(LOCKED, SELECTION)
    check(len(combos)==105, "5 A configurations x 21 B pairs = 105")
    check(len({c.parameter_set_id for c in combos})==105, "all parameter_set_id values unique")
    check({c.a_role for c in combos}=={"CONTROL","ROBUST_1","ROBUST_2","ROBUST_3","ROBUST_4"}, "all five A roles represented")
    for c in combos:
        p=parameter_set_for_pullback(c)
        check(p.buyer_response_parameters==FIXED_C, f"C fixed for combo {c.index}")
        check(p.reclaim_parameters==FIXED_D_RECLAIM, f"D reclaim fixed for combo {c.index}")
        check(p.runaway_entry_parameters==FIXED_D_RUNAWAY, f"D runaway fixed for combo {c.index}")
    check(LOCKED["full_cartesian_recommended"] is False, "13.23m Cartesian remains forbidden")
    check(LOCKED["max_entry_age_ms"]==[300000], "5m entry window fixed")
    print("="*58)
    print("RESULT: PASS")

if __name__=="__main__": main()
