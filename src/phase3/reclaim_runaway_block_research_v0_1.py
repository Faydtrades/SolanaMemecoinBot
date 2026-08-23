from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from phase2.models_v0_1 import FirstPullbackParameterSet

SCHEMA_VERSION = "P3-DRECLAIM-0.1"
EXPERIMENT_ID = "EXP-0005"
BLOCK_ID = "D_RECLAIM_RUNAWAY"

@dataclass(frozen=True, slots=True)
class ReclaimRunawayCombination:
    index: int
    parameter_set_id: str
    abc_role: str
    abc_source_parameter_set_id: str
    min_return_bps: int
    min_trades_since_t0: int
    min_unique_buyers_since_t0: int
    min_depth_bps: int
    max_depth_bps: int
    min_rebound_bps: int
    min_buys: int
    min_net_flow_reserve_ppm: int
    min_extension_from_response_bps: int
    max_extension_from_response_bps: int

def valid_d_pairs(locked: Mapping[str, object]) -> list[tuple[int, int]]:
    reclaim = locked.get("reclaim")
    runaway = locked.get("runaway")
    if not isinstance(reclaim, Mapping) or not isinstance(runaway, Mapping):
        raise ValueError("reclaim/runaway blocks missing")
    mins = [int(x) for x in reclaim["min_extension_from_response_bps"]]
    maxs = [int(x) for x in runaway["max_extension_from_response_bps"]]
    pairs = [(a, b) for a in mins for b in maxs if a < b]
    if len(pairs) != 24:
        raise ValueError(f"expected 24 valid D pairs, got {len(pairs)}")
    if int(runaway.get("valid_reclaim_runaway_pair_count", -1)) != 24:
        raise ValueError("locked D pair count changed")
    return pairs

def validate_locked_search(locked: Mapping[str, object]) -> None:
    if locked.get("decision_id") != "PHASE-2-PARAM-SEARCH-001":
        raise ValueError("unexpected search decision")
    if locked.get("strategy_version_target") != "v1.1":
        raise ValueError("unexpected strategy version")
    if locked.get("full_cartesian_recommended") is not False:
        raise ValueError("full Cartesian must remain forbidden")
    if int(locked.get("naive_full_cartesian_count", -1)) != 13_230_000:
        raise ValueError("naive Cartesian count changed")
    blocks = locked.get("block_combination_counts")
    if not isinstance(blocks, Mapping) or int(blocks.get("D_RECLAIM_RUNAWAY", -1)) != 24:
        raise ValueError("Block D must contain exactly 24 pairs")
    if locked.get("max_entry_age_ms") != [300000]:
        raise ValueError("5m entry window changed")
    valid_d_pairs(locked)

def validate_selection(selection: Mapping[str, object]) -> list[Mapping[str, object]]:
    if selection.get("experiment_id") != "EXP-0004":
        raise ValueError("selection experiment mismatch")
    if selection.get("status") != "LOCKED_FOR_EXP0005_BLOCK_D":
        raise ValueError("selection is not locked for Block D")
    rows = selection.get("carry_forward")
    if not isinstance(rows, list) or len(rows) != 4:
        raise ValueError("exactly four A+B+C configs required")
    roles = [str(x.get("role")) for x in rows if isinstance(x, Mapping)]
    if roles != ["CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3"]:
        raise ValueError("unexpected A+B+C role order")
    return rows

def generate_d_combinations(
    locked: Mapping[str, object],
    selection: Mapping[str, object],
) -> list[ReclaimRunawayCombination]:
    validate_locked_search(locked)
    rows = validate_selection(selection)
    pairs = valid_d_pairs(locked)
    out: list[ReclaimRunawayCombination] = []
    idx = 0
    for abc in rows:
        for reclaim, runaway in pairs:
            idx += 1
            role = str(abc["role"])
            out.append(ReclaimRunawayCombination(
                index=idx,
                parameter_set_id=(
                    f"FP1-EXP0005-D-{idx:03d}-{role}"
                    f"-R{int(abc['min_return_bps'])}-T{int(abc['min_trades_since_t0'])}"
                    f"-U{int(abc['min_unique_buyers_since_t0'])}"
                    f"-PD{int(abc['min_depth_bps'])}-{int(abc['max_depth_bps'])}"
                    f"-RB{int(abc['min_rebound_bps'])}-BU{int(abc['min_buys'])}"
                    f"-FL{int(abc['min_net_flow_reserve_ppm'])}"
                    f"-RC{reclaim}-RW{runaway}"
                ),
                abc_role=role,
                abc_source_parameter_set_id=str(abc["parameter_set_id"]),
                min_return_bps=int(abc["min_return_bps"]),
                min_trades_since_t0=int(abc["min_trades_since_t0"]),
                min_unique_buyers_since_t0=int(abc["min_unique_buyers_since_t0"]),
                min_depth_bps=int(abc["min_depth_bps"]),
                max_depth_bps=int(abc["max_depth_bps"]),
                min_rebound_bps=int(abc["min_rebound_bps"]),
                min_buys=int(abc["min_buys"]),
                min_net_flow_reserve_ppm=int(abc["min_net_flow_reserve_ppm"]),
                min_extension_from_response_bps=reclaim,
                max_extension_from_response_bps=runaway,
            ))
    if len(out) != 96:
        raise ValueError(f"expected 96 staged combinations, got {len(out)}")
    return out

def parameter_set_for_d(combo: ReclaimRunawayCombination) -> FirstPullbackParameterSet:
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
        reclaim_parameters={
            "min_extension_from_response_bps": combo.min_extension_from_response_bps,
        },
        runaway_entry_parameters={
            "max_extension_from_response_bps": combo.max_extension_from_response_bps,
        },
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
