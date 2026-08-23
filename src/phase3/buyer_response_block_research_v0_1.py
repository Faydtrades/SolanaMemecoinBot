from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import Mapping, Sequence

from phase2.models_v0_1 import FirstPullbackParameterSet

SCHEMA_VERSION = "P3-CRESP-0.1"
EXPERIMENT_ID = "EXP-0004"
BLOCK_ID = "C_BUYER_RESPONSE_3S"

FIXED_D_RECLAIM = {"min_extension_from_response_bps": 200}
FIXED_D_RUNAWAY = {"max_extension_from_response_bps": 10000}

@dataclass(frozen=True, slots=True)
class BuyerResponseCombination:
    index: int
    parameter_set_id: str
    ab_role: str
    ab_source_parameter_set_id: str
    min_return_bps: int
    min_trades_since_t0: int
    min_unique_buyers_since_t0: int
    min_depth_bps: int
    max_depth_bps: int
    min_rebound_bps: int
    min_buys: int
    min_net_flow_reserve_ppm: int

def validate_locked_search(locked: Mapping[str, object]) -> None:
    if locked.get("decision_id") != "PHASE-2-PARAM-SEARCH-001":
        raise ValueError("unexpected search decision id")
    if locked.get("strategy_version_target") != "v1.1":
        raise ValueError("unexpected strategy version")
    if locked.get("full_cartesian_recommended") is not False:
        raise ValueError("full Cartesian must remain forbidden")
    if int(locked.get("naive_full_cartesian_count", -1)) != 13_230_000:
        raise ValueError("naive Cartesian count changed")
    blocks = locked.get("block_combination_counts")
    if not isinstance(blocks, Mapping) or int(blocks.get("C_BUYER_RESPONSE_3S", -1)) != 150:
        raise ValueError("Block C must contain exactly 150 combinations")
    buyer = locked.get("buyer_response")
    if not isinstance(buyer, Mapping):
        raise ValueError("buyer_response block missing")
    if buyer.get("window_id") != ["3s"]:
        raise ValueError("3s buyer-response window changed")
    count = (
        len(buyer["min_rebound_bps"])
        * len(buyer["min_buys"])
        * len(buyer["min_net_flow_reserve_ppm"])
    )
    if count != 150:
        raise ValueError(f"expected 150 buyer-response combinations, got {count}")

def validate_selection(selection: Mapping[str, object]) -> list[Mapping[str, object]]:
    if selection.get("experiment_id") != "EXP-0003":
        raise ValueError("selection experiment mismatch")
    if selection.get("status") != "LOCKED_FOR_EXP0004_BLOCK_C":
        raise ValueError("selection is not locked for Block C")
    rows = selection.get("carry_forward")
    if not isinstance(rows, list) or len(rows) != 4:
        raise ValueError("exactly four A+B configurations required")
    roles = [str(x.get("role")) for x in rows if isinstance(x, Mapping)]
    if roles != ["CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3"]:
        raise ValueError("unexpected A+B role order")
    return rows

def generate_buyer_response_combinations(
    locked: Mapping[str, object],
    selection: Mapping[str, object],
) -> list[BuyerResponseCombination]:
    validate_locked_search(locked)
    rows = validate_selection(selection)
    buyer = locked["buyer_response"]
    c_grid = list(product(
        [int(x) for x in buyer["min_rebound_bps"]],
        [int(x) for x in buyer["min_buys"]],
        [int(x) for x in buyer["min_net_flow_reserve_ppm"]],
    ))
    if len(c_grid) != 150:
        raise ValueError("unexpected C grid size")

    out: list[BuyerResponseCombination] = []
    idx = 0
    for ab in rows:
        for rebound, buys, flow in c_grid:
            idx += 1
            role = str(ab["role"])
            out.append(BuyerResponseCombination(
                index=idx,
                parameter_set_id=(
                    f"FP1-EXP0004-C-{idx:03d}-{role}"
                    f"-R{int(ab['min_return_bps'])}-T{int(ab['min_trades_since_t0'])}"
                    f"-U{int(ab['min_unique_buyers_since_t0'])}"
                    f"-PD{int(ab['min_depth_bps'])}-{int(ab['max_depth_bps'])}"
                    f"-RB{rebound}-BU{buys}-FL{flow}"
                ),
                ab_role=role,
                ab_source_parameter_set_id=str(ab["parameter_set_id"]),
                min_return_bps=int(ab["min_return_bps"]),
                min_trades_since_t0=int(ab["min_trades_since_t0"]),
                min_unique_buyers_since_t0=int(ab["min_unique_buyers_since_t0"]),
                min_depth_bps=int(ab["min_depth_bps"]),
                max_depth_bps=int(ab["max_depth_bps"]),
                min_rebound_bps=rebound,
                min_buys=buys,
                min_net_flow_reserve_ppm=flow,
            ))
    if len(out) != 600:
        raise ValueError(f"expected 600 staged A+B+C combinations, got {len(out)}")
    return out

def parameter_set_for_buyer_response(combo: BuyerResponseCombination) -> FirstPullbackParameterSet:
    return FirstPullbackParameterSet(
        parameter_set_id=combo.parameter_set_id,
        strategy_version="v1.1",
        max_entry_age_ms=300_000,
        impulse_parameters={
            "min_return_bps": combo.min_return_bps,
            "min_trades_since_t0": combo.min_trades_since_t0,
            "min_unique_buyers_since_t0": combo.min_unique_buyers_since_t0,
        },
        pullback_parameters={
            "min_depth_bps": combo.min_depth_bps,
            "max_depth_bps": combo.max_depth_bps,
        },
        buyer_response_parameters={
            "window_id": "3s",
            "min_rebound_bps": combo.min_rebound_bps,
            "min_buys": combo.min_buys,
            "min_net_flow_reserve_ppm": combo.min_net_flow_reserve_ppm,
        },
        reclaim_parameters=dict(FIXED_D_RECLAIM),
        runaway_entry_parameters=dict(FIXED_D_RUNAWAY),
    )

PARETO_METRICS = (
    "candidate_count",
    "h15_observable_n",
    "h15_p50_bps",
    "h30_observable_n",
    "h30_p50_bps",
)

def pareto_frontier(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    eligible = [
        r for r in rows
        if int(r.get("h15_observable_n", 0) or 0) > 0
        and int(r.get("h30_observable_n", 0) or 0) > 0
        and r.get("h15_p50_bps") is not None
        and r.get("h30_p50_bps") is not None
    ]
    def dominates(a, b):
        av = [int(a[m]) for m in PARETO_METRICS]
        bv = [int(b[m]) for m in PARETO_METRICS]
        return all(x >= y for x, y in zip(av, bv)) and any(x > y for x, y in zip(av, bv))
    frontier = []
    for row in eligible:
        if not any(dominates(other, row) for other in eligible if other is not row):
            frontier.append(dict(row))
    frontier.sort(key=lambda r: (
        -int(r["candidate_count"]),
        -int(r["h15_observable_n"]),
        -int(r["h15_p50_bps"]),
        -int(r["h30_observable_n"]),
        -int(r["h30_p50_bps"]),
        str(r["parameter_set_id"]),
    ))
    return frontier
