from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from phase2.models_v0_1 import FirstPullbackParameterSet

SCHEMA_VERSION = "P3-BPULL-0.1"
EXPERIMENT_ID = "EXP-0003"
BLOCK_ID = "B_PULLBACK"

FIXED_C = {
    "window_id": "3s",
    "min_rebound_bps": 200,
    "min_buys": 1,
    "min_net_flow_reserve_ppm": 0,
}
FIXED_D_RECLAIM = {"min_extension_from_response_bps": 200}
FIXED_D_RUNAWAY = {"max_extension_from_response_bps": 10000}

@dataclass(frozen=True, slots=True)
class PullbackCombination:
    index: int
    parameter_set_id: str
    a_role: str
    a_source_parameter_set_id: str
    min_return_bps: int
    min_trades_since_t0: int
    min_unique_buyers_since_t0: int
    min_depth_bps: int
    max_depth_bps: int

def valid_pullback_pairs(locked: Mapping[str, object]) -> list[tuple[int, int]]:
    pullback = locked.get("pullback")
    if not isinstance(pullback, Mapping):
        raise ValueError("pullback block missing")
    mins = [int(x) for x in pullback["min_depth_bps"]]
    maxs = [int(x) for x in pullback["max_depth_bps"]]
    pairs = [(a, b) for a in mins for b in maxs if a < b]
    if len(pairs) != 21:
        raise ValueError(f"expected 21 valid pullback pairs, got {len(pairs)}")
    return pairs

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
    if not isinstance(blocks, Mapping) or int(blocks.get("B_PULLBACK", -1)) != 21:
        raise ValueError("B_PULLBACK must have 21 valid pairs")
    if locked.get("max_entry_age_ms") != [300000]:
        raise ValueError("5m entry window changed")
    buyer = locked.get("buyer_response")
    if not isinstance(buyer, Mapping) or buyer.get("window_id") != ["3s"]:
        raise ValueError("3s response window changed")
    valid_pullback_pairs(locked)

def validate_selection(selection: Mapping[str, object]) -> list[Mapping[str, object]]:
    if selection.get("experiment_id") != "EXP-0002":
        raise ValueError("selection experiment mismatch")
    if selection.get("status") != "LOCKED_FOR_EXP0003_BLOCK_B":
        raise ValueError("selection is not locked for Block B")
    rows = selection.get("carry_forward")
    if not isinstance(rows, list) or len(rows) != 5:
        raise ValueError("exactly five A configurations required")
    roles = [str(x.get("role")) for x in rows if isinstance(x, Mapping)]
    if roles != ["CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3", "ROBUST_4"]:
        raise ValueError("unexpected A shortlist roles/order")
    return rows

def generate_pullback_combinations(
    locked: Mapping[str, object],
    selection: Mapping[str, object],
) -> list[PullbackCombination]:
    validate_locked_search(locked)
    rows = validate_selection(selection)
    pairs = valid_pullback_pairs(locked)
    combos: list[PullbackCombination] = []
    idx = 0
    for a in rows:
        for min_depth, max_depth in pairs:
            idx += 1
            role = str(a["role"])
            ret = int(a["min_return_bps"])
            trades = int(a["min_trades_since_t0"])
            buyers = int(a["min_unique_buyers_since_t0"])
            combos.append(
                PullbackCombination(
                    index=idx,
                    parameter_set_id=(
                        f"FP1-EXP0003-B-{idx:03d}-{role}-R{ret}-T{trades}-U{buyers}"
                        f"-PD{min_depth}-{max_depth}"
                    ),
                    a_role=role,
                    a_source_parameter_set_id=str(a["parameter_set_id"]),
                    min_return_bps=ret,
                    min_trades_since_t0=trades,
                    min_unique_buyers_since_t0=buyers,
                    min_depth_bps=min_depth,
                    max_depth_bps=max_depth,
                )
            )
    if len(combos) != 105:
        raise ValueError(f"expected 105 A+B combinations, got {len(combos)}")
    return combos

def parameter_set_for_pullback(combo: PullbackCombination) -> FirstPullbackParameterSet:
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
        buyer_response_parameters=dict(FIXED_C),
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
