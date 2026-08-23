from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase3.impulse_block_research_v0_1 import (
    FOUNDATION_DOWNSTREAM,
    PARETO_METRICS,
    generate_impulse_combinations,
    parameter_set_for_impulse,
    pareto_frontier,
    validate_locked_search,
    validate_parameter_set_against_locked_search,
)


LOCKED = {
    "decision_id": "PHASE-2-PARAM-SEARCH-001",
    "status": "LOCKED_FOR_FIRST_RESEARCH_CYCLE",
    "strategy_version_target": "v1.1",
    "full_cartesian_recommended": False,
    "naive_full_cartesian_count": 13230000,
    "max_entry_age_ms": [300000],
    "block_combination_counts": {
        "A_IMPULSE": 175,
        "B_PULLBACK": 21,
        "C_BUYER_RESPONSE_3S": 150,
        "D_RECLAIM_RUNAWAY": 24,
    },
    "impulse": {
        "min_return_bps": [500, 1500, 2000, 3000, 5000, 5500, 12500],
        "min_trades_since_t0": [2, 3, 5, 7, 24],
        "min_unique_buyers_since_t0": [1, 2, 3, 5, 9],
    },
    "pullback": {
        "min_depth_bps": [1000, 2500, 3000, 4000, 6000],
        "max_depth_bps": [3500, 5500, 6000, 6500, 9000],
        "valid_min_max_pair_count": 21,
    },
    "buyer_response": {
        "window_id": ["3s"],
        "min_rebound_bps": [200, 500, 800, 2300, 7300, 20700],
        "min_buys": [1, 2, 3, 4, 8],
        "min_net_flow_reserve_ppm": [0, 6000, 30000, 93000, 185000],
    },
    "reclaim": {"min_extension_from_response_bps": [200, 500, 700, 800, 2500, 7100, 19500]},
    "runaway": {
        "max_extension_from_response_bps": [2000, 2500, 3000, 7000, 10000],
        "valid_reclaim_runaway_pair_count": 24,
    },
}


def check(cond, label):
    if not cond:
        raise AssertionError(label)
    print(f"[PASS] {label}")


def main():
    print("PHASE 3 / EXP-0002 BLOCK A SELFTEST v0.1")
    print("=" * 56)

    validate_locked_search(LOCKED)
    check(True, "locked PHASE-2-PARAM-SEARCH-001 mechanics accepted")

    combos = generate_impulse_combinations(LOCKED)
    check(len(combos) == 175, "exactly 175 A_IMPULSE combinations")
    check(len({c.parameter_set_id for c in combos}) == 175, "all parameter_set_id values unique")

    expected_returns = {500, 1500, 2000, 3000, 5000, 5500, 12500}
    expected_trades = {2, 3, 5, 7, 24}
    expected_buyers = {1, 2, 3, 5, 9}
    check({c.min_return_bps for c in combos} == expected_returns, "return grid unchanged")
    check({c.min_trades_since_t0 for c in combos} == expected_trades, "trade-count grid unchanged")
    check({c.min_unique_buyers_since_t0 for c in combos} == expected_buyers, "buyer grid unchanged")

    for c in combos:
        p = parameter_set_for_impulse(c)
        validate_parameter_set_against_locked_search(p, LOCKED)
        if p.pullback_parameters != FOUNDATION_DOWNSTREAM["pullback"]:
            raise AssertionError("Block B varied")
        if p.buyer_response_parameters != FOUNDATION_DOWNSTREAM["buyer_response"]:
            raise AssertionError("Block C varied")
        if p.reclaim_parameters != FOUNDATION_DOWNSTREAM["reclaim"]:
            raise AssertionError("Block D reclaim varied")
        if p.runaway_entry_parameters != FOUNDATION_DOWNSTREAM["runaway"]:
            raise AssertionError("Block D runaway varied")
    check(True, "B/C/D remain fixed foundation anchors for all 175 runs")

    check(LOCKED["full_cartesian_recommended"] is False, "13.23m full Cartesian remains forbidden")
    check(LOCKED["max_entry_age_ms"] == [300000], "5m entry window fixed")
    check(LOCKED["buyer_response"]["window_id"] == ["3s"], "3s buyer-response window fixed")

    rows = [
        {
            "parameter_set_id": "A",
            "candidate_count": 100,
            "h15_observable_n": 60,
            "h15_p50_bps": 100,
            "h30_observable_n": 30,
            "h30_p50_bps": 50,
        },
        {
            "parameter_set_id": "B",
            "candidate_count": 90,
            "h15_observable_n": 55,
            "h15_p50_bps": 200,
            "h30_observable_n": 28,
            "h30_p50_bps": 100,
        },
        {
            "parameter_set_id": "C",
            "candidate_count": 80,
            "h15_observable_n": 40,
            "h15_p50_bps": 50,
            "h30_observable_n": 20,
            "h30_p50_bps": 25,
        },
        {
            "parameter_set_id": "SPARSE",
            "candidate_count": 1,
            "h15_observable_n": 1,
            "h15_p50_bps": 9999,
            "h30_observable_n": 0,
            "h30_p50_bps": None,
        },
    ]
    frontier = pareto_frontier(rows)
    ids = [r["parameter_set_id"] for r in frontier]
    check(ids == ["A", "B"], "Pareto frontier deterministic and dominated row removed")
    check("SPARSE" not in ids, "no 30s-clean-data row cannot enter diagnostic frontier")
    check(PARETO_METRICS == (
        "candidate_count", "h15_observable_n", "h15_p50_bps",
        "h30_observable_n", "h30_p50_bps",
    ), "frontier metric contract locked")

    print("=" * 56)
    print("RESULT: PASS")


if __name__ == "__main__":
    main()
