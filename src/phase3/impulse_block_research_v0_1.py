from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import Iterable, Mapping, Sequence

from phase2.models_v0_1 import FirstPullbackParameterSet


SCHEMA_VERSION = "P3-AIMP-0.1"
EXPERIMENT_ID = "EXP-0002"
BLOCK_ID = "A_IMPULSE"

FOUNDATION_DOWNSTREAM = {
    "pullback": {"min_depth_bps": 1000, "max_depth_bps": 9000},
    "buyer_response": {
        "window_id": "3s",
        "min_rebound_bps": 200,
        "min_buys": 1,
        "min_net_flow_reserve_ppm": 0,
    },
    "reclaim": {"min_extension_from_response_bps": 200},
    "runaway": {"max_extension_from_response_bps": 10000},
}


@dataclass(frozen=True, slots=True)
class ImpulseCombination:
    index: int
    parameter_set_id: str
    min_return_bps: int
    min_trades_since_t0: int
    min_unique_buyers_since_t0: int


def validate_locked_search(locked: Mapping[str, object]) -> None:
    if locked.get("decision_id") != "PHASE-2-PARAM-SEARCH-001":
        raise ValueError("unexpected locked search decision id")
    if locked.get("strategy_version_target") != "v1.1":
        raise ValueError("unexpected strategy version target")
    if locked.get("full_cartesian_recommended") is not False:
        raise ValueError("full Cartesian must remain explicitly not recommended")
    if int(locked.get("naive_full_cartesian_count", -1)) != 13_230_000:
        raise ValueError("unexpected naive full Cartesian count")

    blocks = locked.get("block_combination_counts")
    if not isinstance(blocks, Mapping) or int(blocks.get("A_IMPULSE", -1)) != 175:
        raise ValueError("A_IMPULSE block must contain exactly 175 combinations")

    if locked.get("max_entry_age_ms") != [300000]:
        raise ValueError("5m First Pullback entry window changed")

    buyer = locked.get("buyer_response")
    if not isinstance(buyer, Mapping) or buyer.get("window_id") != ["3s"]:
        raise ValueError("3s buyer-response window changed")


def generate_impulse_combinations(
    locked: Mapping[str, object],
) -> list[ImpulseCombination]:
    validate_locked_search(locked)
    impulse = locked.get("impulse")
    if not isinstance(impulse, Mapping):
        raise ValueError("locked impulse block missing")

    returns = [int(x) for x in impulse["min_return_bps"]]
    trades = [int(x) for x in impulse["min_trades_since_t0"]]
    buyers = [int(x) for x in impulse["min_unique_buyers_since_t0"]]

    combos: list[ImpulseCombination] = []
    for idx, (ret, trd, byr) in enumerate(product(returns, trades, buyers), start=1):
        combos.append(
            ImpulseCombination(
                index=idx,
                parameter_set_id=f"FP1-EXP0002-A-{idx:03d}-R{ret}-T{trd}-B{byr}",
                min_return_bps=ret,
                min_trades_since_t0=trd,
                min_unique_buyers_since_t0=byr,
            )
        )

    if len(combos) != 175:
        raise ValueError(f"expected 175 A combinations, got {len(combos)}")
    return combos


def parameter_set_for_impulse(combo: ImpulseCombination) -> FirstPullbackParameterSet:
    return FirstPullbackParameterSet(
        parameter_set_id=combo.parameter_set_id,
        strategy_version="v1.1",
        max_entry_age_ms=300_000,
        impulse_parameters={
            "min_return_bps": combo.min_return_bps,
            "min_trades_since_t0": combo.min_trades_since_t0,
            "min_unique_buyers_since_t0": combo.min_unique_buyers_since_t0,
        },
        pullback_parameters=dict(FOUNDATION_DOWNSTREAM["pullback"]),
        buyer_response_parameters=dict(FOUNDATION_DOWNSTREAM["buyer_response"]),
        reclaim_parameters=dict(FOUNDATION_DOWNSTREAM["reclaim"]),
        runaway_entry_parameters=dict(FOUNDATION_DOWNSTREAM["runaway"]),
    )


def validate_parameter_set_against_locked_search(
    params: FirstPullbackParameterSet,
    locked: Mapping[str, object],
) -> None:
    validate_locked_search(locked)
    impulse = locked["impulse"]
    pullback = locked["pullback"]
    buyer = locked["buyer_response"]
    reclaim = locked["reclaim"]
    runaway = locked["runaway"]

    checks = (
        (params.impulse_parameters["min_return_bps"], impulse["min_return_bps"], "min_return_bps"),
        (params.impulse_parameters["min_trades_since_t0"], impulse["min_trades_since_t0"], "min_trades_since_t0"),
        (params.impulse_parameters["min_unique_buyers_since_t0"], impulse["min_unique_buyers_since_t0"], "min_unique_buyers_since_t0"),
        (params.pullback_parameters["min_depth_bps"], pullback["min_depth_bps"], "min_depth_bps"),
        (params.pullback_parameters["max_depth_bps"], pullback["max_depth_bps"], "max_depth_bps"),
        (params.buyer_response_parameters["window_id"], buyer["window_id"], "window_id"),
        (params.buyer_response_parameters["min_rebound_bps"], buyer["min_rebound_bps"], "min_rebound_bps"),
        (params.buyer_response_parameters["min_buys"], buyer["min_buys"], "min_buys"),
        (params.buyer_response_parameters["min_net_flow_reserve_ppm"], buyer["min_net_flow_reserve_ppm"], "min_net_flow_reserve_ppm"),
        (params.reclaim_parameters["min_extension_from_response_bps"], reclaim["min_extension_from_response_bps"], "reclaim"),
        (params.runaway_entry_parameters["max_extension_from_response_bps"], runaway["max_extension_from_response_bps"], "runaway"),
    )
    for value, allowed, name in checks:
        if value not in allowed:
            raise ValueError(f"{name}={value!r} is outside PHASE-2-PARAM-SEARCH-001")

    # EXP-0002 isolates A. B/C/D must remain exactly at foundation anchors.
    if params.pullback_parameters != FOUNDATION_DOWNSTREAM["pullback"]:
        raise ValueError("EXP-0002 may not vary Block B")
    if params.buyer_response_parameters != FOUNDATION_DOWNSTREAM["buyer_response"]:
        raise ValueError("EXP-0002 may not vary Block C")
    if params.reclaim_parameters != FOUNDATION_DOWNSTREAM["reclaim"]:
        raise ValueError("EXP-0002 may not vary reclaim component of Block D")
    if params.runaway_entry_parameters != FOUNDATION_DOWNSTREAM["runaway"]:
        raise ValueError("EXP-0002 may not vary runaway component of Block D")


PARETO_METRICS = (
    "candidate_count",
    "h15_observable_n",
    "h15_p50_bps",
    "h30_observable_n",
    "h30_p50_bps",
)


def pareto_frontier(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Return a deterministic diagnostic frontier; it does NOT select a winner.

    A row is frontier-eligible only when both 15s and 30s have at least one
    clean observable return. All five PARETO_METRICS are maximized. A row is
    dominated when another eligible row is >= on every metric and > on at
    least one metric.

    Sample counts are explicit Pareto dimensions so a tiny high-return sample
    cannot silently outrank a well-supported configuration.
    """
    eligible: list[Mapping[str, object]] = []
    for row in rows:
        if int(row.get("h15_observable_n", 0) or 0) <= 0:
            continue
        if int(row.get("h30_observable_n", 0) or 0) <= 0:
            continue
        if row.get("h15_p50_bps") is None or row.get("h30_p50_bps") is None:
            continue
        eligible.append(row)

    def dominates(a: Mapping[str, object], b: Mapping[str, object]) -> bool:
        av = [int(a[m]) for m in PARETO_METRICS]
        bv = [int(b[m]) for m in PARETO_METRICS]
        return all(x >= y for x, y in zip(av, bv)) and any(x > y for x, y in zip(av, bv))

    frontier: list[dict[str, object]] = []
    for row in eligible:
        if not any(dominates(other, row) for other in eligible if other is not row):
            frontier.append(dict(row))

    frontier.sort(
        key=lambda r: (
            -int(r["candidate_count"]),
            -int(r["h15_observable_n"]),
            -int(r["h15_p50_bps"]),
            -int(r["h30_observable_n"]),
            -int(r["h30_p50_bps"]),
            str(r["parameter_set_id"]),
        )
    )
    return frontier
