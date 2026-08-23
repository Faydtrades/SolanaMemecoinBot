from __future__ import annotations
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT/"src"))

from phase3.reclaim_runaway_block_research_v0_1 import (
    generate_d_combinations, parameter_set_for_d, valid_d_pairs,
    validate_locked_search, validate_selection,
)

LOCKED = {
    "decision_id":"PHASE-2-PARAM-SEARCH-001",
    "strategy_version_target":"v1.1",
    "full_cartesian_recommended":False,
    "naive_full_cartesian_count":13230000,
    "max_entry_age_ms":[300000],
    "block_combination_counts":{
        "A_IMPULSE":175,"B_PULLBACK":21,"C_BUYER_RESPONSE_3S":150,"D_RECLAIM_RUNAWAY":24
    },
    "reclaim":{"min_extension_from_response_bps":[200,500,700,800,2500,7100,19500]},
    "runaway":{
        "max_extension_from_response_bps":[2000,2500,3000,7000,10000],
        "valid_reclaim_runaway_pair_count":24,
    },
}
SELECTION = {
    "experiment_id":"EXP-0004",
    "status":"LOCKED_FOR_EXP0005_BLOCK_D",
    "carry_forward":[
        {"role":"CONTROL","parameter_set_id":"FP1-EXP0004-C-001-CONTROL-R500-T2-U1-PD1000-9000-RB200-BU1-FL0",
         "min_return_bps":500,"min_trades_since_t0":2,"min_unique_buyers_since_t0":1,
         "min_depth_bps":1000,"max_depth_bps":9000,"min_rebound_bps":200,"min_buys":1,
         "min_net_flow_reserve_ppm":0},
        {"role":"ROBUST_1","parameter_set_id":"FP1-EXP0004-C-232-ROBUST_1-R1500-T5-U9-PD2500-6500-RB2300-BU2-FL6000",
         "min_return_bps":1500,"min_trades_since_t0":5,"min_unique_buyers_since_t0":9,
         "min_depth_bps":2500,"max_depth_bps":6500,"min_rebound_bps":2300,"min_buys":2,
         "min_net_flow_reserve_ppm":6000},
        {"role":"ROBUST_2","parameter_set_id":"FP1-EXP0004-C-382-ROBUST_2-R2000-T3-U5-PD2500-6500-RB2300-BU2-FL6000",
         "min_return_bps":2000,"min_trades_since_t0":3,"min_unique_buyers_since_t0":5,
         "min_depth_bps":2500,"max_depth_bps":6500,"min_rebound_bps":2300,"min_buys":2,
         "min_net_flow_reserve_ppm":6000},
        {"role":"ROBUST_3","parameter_set_id":"FP1-EXP0004-C-532-ROBUST_3-R5000-T3-U2-PD2500-6500-RB2300-BU2-FL6000",
         "min_return_bps":5000,"min_trades_since_t0":3,"min_unique_buyers_since_t0":2,
         "min_depth_bps":2500,"max_depth_bps":6500,"min_rebound_bps":2300,"min_buys":2,
         "min_net_flow_reserve_ppm":6000},
    ],
}

def check(cond,label):
    if not cond: raise AssertionError(label)
    print(f"[PASS] {label}")

def main():
    print("PHASE 3 / EXP-0005 BLOCK D SELFTEST v0.1")
    print("="*60)
    validate_locked_search(LOCKED)
    validate_selection(SELECTION)
    pairs=valid_d_pairs(LOCKED)
    check(len(pairs)==24,"exactly 24 valid reclaim/runaway pairs")
    check(all(a<b for a,b in pairs),"every reclaim threshold is below runaway cap")
    combos=generate_d_combinations(LOCKED,SELECTION)
    check(len(combos)==96,"4 A+B+C configurations x 24 D pairs = 96")
    check(len({c.parameter_set_id for c in combos})==96,"all parameter_set_id values unique")
    check({c.abc_role for c in combos}=={"CONTROL","ROBUST_1","ROBUST_2","ROBUST_3"},
          "all four A+B+C roles represented")
    for c in combos:
        p=parameter_set_for_d(c)
        if p.impulse_parameters["min_return_bps"]!=c.min_return_bps:raise AssertionError("A varied")
        if p.pullback_parameters["min_depth_bps"]!=c.min_depth_bps:raise AssertionError("B varied")
        if p.buyer_response_parameters["min_rebound_bps"]!=c.min_rebound_bps:raise AssertionError("C varied")
    check(True,"A/B/C fixed within each carry-forward regime; only D varies")
    check(LOCKED["full_cartesian_recommended"] is False,"13.23m Cartesian remains forbidden")
    print("="*60)
    print("RESULT: PASS")
if __name__=="__main__":main()
