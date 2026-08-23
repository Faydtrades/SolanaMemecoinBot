from __future__ import annotations

from dataclasses import dataclass, asdict
from decimal import Decimal, ROUND_HALF_EVEN
from hashlib import sha256
import json
import math
from typing import Iterable, Mapping, Sequence

SCHEMA_VERSION = "P2PSD-0.1"

# These are deliberately permissive CHARACTERIZATION anchors, not strategy rules.
# They exist only so later-stage feature opportunity distributions can be observed.
BROAD_IMPULSE_RETURN_BPS = 500       # +5%
BROAD_IMPULSE_MIN_TRADES = 2
BROAD_IMPULSE_MIN_BUYERS = 1
BROAD_PULLBACK_MIN_BPS = 1000        # 10%
BROAD_PULLBACK_MAX_BPS = 9000        # 90%
BROAD_PROTO_RESPONSE_REBOUND_BPS = 100
BROAD_PROTO_RESPONSE_MIN_BUYS = 1

QUANTILES = (0.25, 0.50, 0.75, 0.90, 0.95)


def canonical_json(value: object) -> str:
    def conv(v: object):
        if isinstance(v, Decimal):
            return str(v)
        if hasattr(v, "__dataclass_fields__"):
            return {k: conv(getattr(v, k)) for k in v.__dataclass_fields__}
        if isinstance(v, Mapping):
            return {str(k): conv(x) for k, x in v.items()}
        if isinstance(v, (list, tuple, set)):
            return [conv(x) for x in v]
        return v
    return json.dumps(conv(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_sha256(value: object) -> str:
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


def nearest_rank(values: Sequence[int], q: float) -> int | None:
    if not values:
        return None
    if not 0 < q <= 1:
        raise ValueError("q must be in (0, 1]")
    ordered = sorted(int(v) for v in values)
    rank = max(1, math.ceil(q * len(ordered)))
    return ordered[rank - 1]


def quantile_summary(values: Iterable[int | None]) -> dict[str, int | None]:
    clean = [int(v) for v in values if v is not None]
    out: dict[str, int | None] = {"n": len(clean)}
    for q in QUANTILES:
        out[f"p{int(q * 100)}"] = nearest_rank(clean, q)
    out["min"] = min(clean) if clean else None
    out["max"] = max(clean) if clean else None
    return out


def _round_nearest(value: int, step: int) -> int:
    if step <= 0:
        raise ValueError("step must be > 0")
    return int((Decimal(value) / Decimal(step)).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN)) * step


def _thin(values: list[int], max_items: int, must_keep: set[int]) -> list[int]:
    values = sorted(set(values))
    if len(values) <= max_items:
        return values

    keep = sorted(v for v in values if v in must_keep)
    remaining_slots = max(0, max_items - len(keep))
    others = [v for v in values if v not in must_keep]
    if remaining_slots <= 0:
        return sorted(keep)[:max_items]
    if len(others) <= remaining_slots:
        return sorted(set(keep + others))

    if remaining_slots == 1:
        sampled = [others[len(others) // 2]]
    else:
        sampled = []
        for i in range(remaining_slots):
            idx = round(i * (len(others) - 1) / (remaining_slots - 1))
            sampled.append(others[idx])
    return sorted(set(keep + sampled))


def propose_grid(
    values: Sequence[int],
    *,
    quantiles: Sequence[float],
    step: int,
    anchors: Sequence[int] = (),
    minimum: int | None = None,
    maximum: int | None = None,
    max_items: int = 8,
) -> list[int]:
    clean = [int(v) for v in values]
    candidates: list[int] = []
    for q in quantiles:
        v = nearest_rank(clean, q)
        if v is not None:
            candidates.append(_round_nearest(v, step))
    candidates.extend(int(v) for v in anchors)

    if minimum is not None:
        candidates = [max(minimum, v) for v in candidates]
    if maximum is not None:
        candidates = [min(maximum, v) for v in candidates]

    candidates = sorted(set(candidates))
    must_keep = set(int(v) for v in anchors if (minimum is None or v >= minimum) and (maximum is None or v <= maximum))
    return _thin(candidates, max_items, must_keep)


def propose_count_grid(
    values: Sequence[int],
    *,
    quantiles: Sequence[float],
    anchors: Sequence[int],
    minimum: int = 1,
    max_items: int = 8,
) -> list[int]:
    clean = [int(v) for v in values]
    candidates = []
    for q in quantiles:
        v = nearest_rank(clean, q)
        if v is not None:
            candidates.append(max(minimum, v))
    candidates.extend(max(minimum, int(v)) for v in anchors)
    return _thin(sorted(set(candidates)), max_items, set(anchors))


def bps(current: Decimal | None, reference: Decimal | None) -> int | None:
    if current is None or reference is None or reference == 0:
        return None
    raw = ((current / reference) - Decimal(1)) * Decimal(10_000)
    return int(raw.quantize(Decimal("1"), rounding=ROUND_HALF_EVEN))


@dataclass(slots=True)
class TokenOpportunityMetrics:
    mint: str
    price_identity: str

    max_return_bps: int | None = None
    max_trades_since_t0: int = 0
    max_unique_buyers_since_t0: int = 0
    max_drawdown_depth_bps: int = 0
    max_3s_buys: int = 0
    max_3s_net_flow_reserve_ppm: int | None = None

    broad_impulse_reached: bool = False
    broad_impulse_age_ms: int | None = None
    broad_impulse_return_bps: int | None = None
    broad_impulse_trades: int | None = None
    broad_impulse_unique_buyers: int | None = None

    broad_pullback_reached: bool = False
    broad_pullback_age_ms: int | None = None
    broad_pullback_depth_at_activation_bps: int | None = None
    max_pullback_depth_after_broad_impulse_bps: int | None = None

    broad_proto_response_reached: bool = False
    broad_proto_response_age_ms: int | None = None
    max_rebound_after_pullback_bps: int | None = None
    max_3s_buys_after_pullback: int | None = None
    max_3s_net_flow_reserve_ppm_after_pullback: int | None = None

    max_reclaim_extension_after_proto_response_bps: int | None = None

    data_valid_states: int = 0
    data_invalid_states: int = 0


def characterize_states(mint: str, states: Sequence[object]) -> TokenOpportunityMetrics:
    if not states:
        return TokenOpportunityMetrics(mint=mint, price_identity="UNAVAILABLE")

    identity_label = getattr(states[-1].market.price_identity, "label", "UNAVAILABLE")
    out = TokenOpportunityMetrics(mint=mint, price_identity=identity_label)

    impulse_high: Decimal | None = None
    pullback_low: Decimal | None = None
    response_ref: Decimal | None = None

    for state in states:
        if state.data_quality.state_valid:
            out.data_valid_states += 1
        else:
            out.data_invalid_states += 1

        price = state.market.current_price_proxy
        ret = state.price_structure.return_from_t0_bps
        dd = state.price_structure.drawdown_from_high_bps
        since = state.windows["since_t0"]
        w3 = state.windows["3s"]

        if ret is not None:
            out.max_return_bps = ret if out.max_return_bps is None else max(out.max_return_bps, ret)
        out.max_trades_since_t0 = max(out.max_trades_since_t0, since.trade_count)
        out.max_unique_buyers_since_t0 = max(out.max_unique_buyers_since_t0, since.unique_buyers)
        depth = -dd if dd is not None and dd < 0 else 0
        out.max_drawdown_depth_bps = max(out.max_drawdown_depth_bps, depth)
        out.max_3s_buys = max(out.max_3s_buys, w3.buys)
        if out.max_3s_net_flow_reserve_ppm is None:
            out.max_3s_net_flow_reserve_ppm = w3.net_flow_reserve_ppm
        else:
            out.max_3s_net_flow_reserve_ppm = max(out.max_3s_net_flow_reserve_ppm, w3.net_flow_reserve_ppm)

        if price is None or not state.data_quality.state_valid:
            continue

        if not out.broad_impulse_reached:
            if (
                ret is not None
                and ret >= BROAD_IMPULSE_RETURN_BPS
                and since.trade_count >= BROAD_IMPULSE_MIN_TRADES
                and since.unique_buyers >= BROAD_IMPULSE_MIN_BUYERS
            ):
                out.broad_impulse_reached = True
                out.broad_impulse_age_ms = state.identity.age_ms
                out.broad_impulse_return_bps = ret
                out.broad_impulse_trades = since.trade_count
                out.broad_impulse_unique_buyers = since.unique_buyers
                impulse_high = price
            continue

        if impulse_high is None or price > impulse_high:
            impulse_high = price

        structural_depth = bps(price, impulse_high)
        structural_depth = -structural_depth if structural_depth is not None and structural_depth < 0 else 0
        if out.max_pullback_depth_after_broad_impulse_bps is None:
            out.max_pullback_depth_after_broad_impulse_bps = structural_depth
        else:
            out.max_pullback_depth_after_broad_impulse_bps = max(
                out.max_pullback_depth_after_broad_impulse_bps, structural_depth
            )

        if structural_depth >= BROAD_PULLBACK_MAX_BPS and not out.broad_pullback_reached:
            # Extremely deep collapse before a broad first pullback is observed:
            # keep the depth distribution, but do not invent a response lifecycle.
            continue

        if not out.broad_pullback_reached:
            if structural_depth >= BROAD_PULLBACK_MIN_BPS:
                out.broad_pullback_reached = True
                out.broad_pullback_age_ms = state.identity.age_ms
                out.broad_pullback_depth_at_activation_bps = structural_depth
                pullback_low = price
            continue

        if pullback_low is None or price < pullback_low:
            pullback_low = price
            response_ref = None

        rebound = bps(price, pullback_low)
        rebound = 0 if rebound is None else max(0, rebound)
        out.max_rebound_after_pullback_bps = (
            rebound if out.max_rebound_after_pullback_bps is None
            else max(out.max_rebound_after_pullback_bps, rebound)
        )
        out.max_3s_buys_after_pullback = (
            w3.buys if out.max_3s_buys_after_pullback is None
            else max(out.max_3s_buys_after_pullback, w3.buys)
        )
        out.max_3s_net_flow_reserve_ppm_after_pullback = (
            w3.net_flow_reserve_ppm
            if out.max_3s_net_flow_reserve_ppm_after_pullback is None
            else max(out.max_3s_net_flow_reserve_ppm_after_pullback, w3.net_flow_reserve_ppm)
        )

        if response_ref is None and (
            rebound >= BROAD_PROTO_RESPONSE_REBOUND_BPS
            and w3.buys >= BROAD_PROTO_RESPONSE_MIN_BUYS
        ):
            out.broad_proto_response_reached = True
            out.broad_proto_response_age_ms = state.identity.age_ms
            response_ref = price
            continue

        if response_ref is not None:
            extension = bps(price, response_ref)
            if extension is not None:
                extension = max(0, extension)
                out.max_reclaim_extension_after_proto_response_bps = (
                    extension
                    if out.max_reclaim_extension_after_proto_response_bps is None
                    else max(out.max_reclaim_extension_after_proto_response_bps, extension)
                )

    return out


def empirical_distributions(rows: Sequence[TokenOpportunityMetrics]) -> dict[str, dict[str, int | None]]:
    return {
        "max_return_bps": quantile_summary(r.max_return_bps for r in rows),
        "max_trades_since_t0": quantile_summary(r.max_trades_since_t0 for r in rows),
        "max_unique_buyers_since_t0": quantile_summary(r.max_unique_buyers_since_t0 for r in rows),
        "max_drawdown_depth_bps": quantile_summary(r.max_drawdown_depth_bps for r in rows),
        "max_3s_buys": quantile_summary(r.max_3s_buys for r in rows),
        "max_3s_net_flow_reserve_ppm": quantile_summary(r.max_3s_net_flow_reserve_ppm for r in rows),
        "broad_impulse_age_ms": quantile_summary(r.broad_impulse_age_ms for r in rows if r.broad_impulse_reached),
        "broad_impulse_return_bps": quantile_summary(r.broad_impulse_return_bps for r in rows if r.broad_impulse_reached),
        "broad_impulse_trades": quantile_summary(r.broad_impulse_trades for r in rows if r.broad_impulse_reached),
        "broad_impulse_unique_buyers": quantile_summary(r.broad_impulse_unique_buyers for r in rows if r.broad_impulse_reached),
        "pullback_depth_activation_bps": quantile_summary(r.broad_pullback_depth_at_activation_bps for r in rows if r.broad_pullback_reached),
        "max_pullback_depth_after_impulse_bps": quantile_summary(r.max_pullback_depth_after_broad_impulse_bps for r in rows if r.broad_impulse_reached),
        "max_rebound_after_pullback_bps": quantile_summary(r.max_rebound_after_pullback_bps for r in rows if r.broad_pullback_reached),
        "max_3s_buys_after_pullback": quantile_summary(r.max_3s_buys_after_pullback for r in rows if r.broad_pullback_reached),
        "max_3s_net_flow_ppm_after_pullback": quantile_summary(r.max_3s_net_flow_reserve_ppm_after_pullback for r in rows if r.broad_pullback_reached),
        "max_reclaim_extension_bps": quantile_summary(r.max_reclaim_extension_after_proto_response_bps for r in rows if r.broad_proto_response_reached),
    }


def build_proposed_search_space(rows: Sequence[TokenOpportunityMetrics]) -> dict[str, object]:
    # Search-space proposal is based only on market-feature support. No outcome/PnL
    # label is consumed. Existing LOOSE/MID/STRICT probe values are included only as
    # pre-existing characterization anchors, not because they performed well.
    positive_returns = [r.max_return_bps for r in rows if r.max_return_bps is not None and r.max_return_bps > 0]
    trades = [r.max_trades_since_t0 for r in rows]
    buyers = [r.max_unique_buyers_since_t0 for r in rows]
    pullbacks = [
        r.max_pullback_depth_after_broad_impulse_bps
        for r in rows
        if r.max_pullback_depth_after_broad_impulse_bps is not None
    ]
    rebounds = [
        r.max_rebound_after_pullback_bps
        for r in rows
        if r.max_rebound_after_pullback_bps is not None
    ]
    response_buys = [
        r.max_3s_buys_after_pullback
        for r in rows
        if r.max_3s_buys_after_pullback is not None
    ]
    response_flow_positive = [
        r.max_3s_net_flow_reserve_ppm_after_pullback
        for r in rows
        if r.max_3s_net_flow_reserve_ppm_after_pullback is not None
        and r.max_3s_net_flow_reserve_ppm_after_pullback > 0
    ]
    reclaim_ext = [
        r.max_reclaim_extension_after_proto_response_bps
        for r in rows
        if r.max_reclaim_extension_after_proto_response_bps is not None
    ]

    impulse_return = propose_grid(
        positive_returns,
        quantiles=(0.25, 0.50, 0.75, 0.90),
        step=500,
        anchors=(1500, 3000, 5000),
        minimum=500,
        max_items=8,
    )
    impulse_trades = propose_count_grid(
        trades, quantiles=(0.50, 0.75, 0.90), anchors=(2, 3, 5), minimum=2, max_items=7
    )
    impulse_buyers = propose_count_grid(
        buyers, quantiles=(0.50, 0.75, 0.90), anchors=(1, 2, 3, 5), minimum=1, max_items=7
    )

    min_depth = propose_grid(
        pullbacks,
        quantiles=(0.25, 0.50, 0.75),
        step=500,
        anchors=(2500, 3000, 4000),
        minimum=1000,
        maximum=7000,
        max_items=8,
    )
    max_depth = propose_grid(
        pullbacks,
        quantiles=(0.50, 0.75, 0.90, 0.95),
        step=500,
        anchors=(5500, 6000, 6500),
        minimum=3500,
        maximum=9000,
        max_items=8,
    )

    rebound = propose_grid(
        rebounds,
        quantiles=(0.25, 0.50, 0.75, 0.90),
        step=100,
        anchors=(200, 500, 800),
        minimum=100,
        max_items=8,
    )
    min_buys = propose_count_grid(
        response_buys, quantiles=(0.25, 0.50, 0.75, 0.90), anchors=(1, 2, 3), minimum=1, max_items=7
    )

    # PPM is dimensionless. Use a 1,000 ppm (0.1% reserve) rounding unit for a
    # readable initial grid. 0 remains the neutral "positive-or-flat net flow" gate.
    flow = propose_grid(
        response_flow_positive,
        quantiles=(0.25, 0.50, 0.75, 0.90),
        step=1000,
        anchors=(0,),
        minimum=0,
        max_items=8,
    )

    reclaim = propose_grid(
        reclaim_ext,
        quantiles=(0.25, 0.50, 0.75, 0.90),
        step=100,
        anchors=(200, 500, 800),
        minimum=100,
        max_items=8,
    )
    runaway = propose_grid(
        reclaim_ext,
        quantiles=(0.50, 0.75, 0.90, 0.95),
        step=500,
        anchors=(2000, 2500, 3000),
        minimum=1000,
        maximum=10000,
        max_items=8,
    )

    valid_pullback_pairs = [(a, b) for a in min_depth for b in max_depth if a < b]
    valid_reclaim_pairs = [(a, b) for a in reclaim for b in runaway if a < b]

    block_counts = {
        "A_IMPULSE": len(impulse_return) * len(impulse_trades) * len(impulse_buyers),
        "B_PULLBACK": len(valid_pullback_pairs),
        "C_BUYER_RESPONSE_3S": len(rebound) * len(min_buys) * len(flow),
        "D_RECLAIM_RUNAWAY": len(valid_reclaim_pairs),
    }
    full_cartesian = math.prod(block_counts.values()) if all(block_counts.values()) else 0

    return {
        "status": "PROPOSAL_NOT_LOCKED",
        "strategy_version_target": "v1.1",
        "max_entry_age_ms": [300_000],
        "impulse": {
            "min_return_bps": impulse_return,
            "min_trades_since_t0": impulse_trades,
            "min_unique_buyers_since_t0": impulse_buyers,
        },
        "pullback": {
            "min_depth_bps": min_depth,
            "max_depth_bps": max_depth,
            "valid_min_max_pair_count": len(valid_pullback_pairs),
        },
        "buyer_response": {
            "window_id": ["3s"],
            "min_rebound_bps": rebound,
            "min_buys": min_buys,
            "min_net_flow_reserve_ppm": flow,
        },
        "reclaim": {
            "min_extension_from_response_bps": reclaim,
        },
        "runaway": {
            "max_extension_from_response_bps": runaway,
            "valid_reclaim_runaway_pair_count": len(valid_reclaim_pairs),
        },
        "search_blocks": {
            "A_IMPULSE": ["min_return_bps", "min_trades_since_t0", "min_unique_buyers_since_t0"],
            "B_PULLBACK": ["min_depth_bps", "max_depth_bps"],
            "C_BUYER_RESPONSE_3S": ["min_rebound_bps", "min_buys", "min_net_flow_reserve_ppm"],
            "D_RECLAIM_RUNAWAY": ["min_extension_from_response_bps", "max_extension_from_response_bps"],
        },
        "block_combination_counts": block_counts,
        "naive_full_cartesian_count": full_cartesian,
        "full_cartesian_recommended": False,
        "design_note": (
            "Use staged/block experiments later; do not evaluate the naive full Cartesian "
            "product. These values are feature-support ranges only and were not selected "
            "using future returns, PnL, winrate, or expectancy."
        ),
    }


def validate_search_space(space: Mapping[str, object]) -> None:
    if space.get("status") != "PROPOSAL_NOT_LOCKED":
        raise ValueError("Search space must remain proposal-only until explicitly locked")
    if space.get("strategy_version_target") != "v1.1":
        raise ValueError("Expected target strategy v1.1")

    imp = space["impulse"]
    pb = space["pullback"]
    resp = space["buyer_response"]
    rec = space["reclaim"]
    run = space["runaway"]

    for name, values in (
        ("impulse.min_return_bps", imp["min_return_bps"]),
        ("impulse.min_trades_since_t0", imp["min_trades_since_t0"]),
        ("impulse.min_unique_buyers_since_t0", imp["min_unique_buyers_since_t0"]),
        ("pullback.min_depth_bps", pb["min_depth_bps"]),
        ("pullback.max_depth_bps", pb["max_depth_bps"]),
        ("buyer_response.min_rebound_bps", resp["min_rebound_bps"]),
        ("buyer_response.min_buys", resp["min_buys"]),
        ("buyer_response.min_net_flow_reserve_ppm", resp["min_net_flow_reserve_ppm"]),
        ("reclaim.min_extension_from_response_bps", rec["min_extension_from_response_bps"]),
        ("runaway.max_extension_from_response_bps", run["max_extension_from_response_bps"]),
    ):
        if not values or list(values) != sorted(set(values)):
            raise ValueError(f"{name} must be non-empty sorted unique values")

    if not any(a < b for a in pb["min_depth_bps"] for b in pb["max_depth_bps"]):
        raise ValueError("No valid pullback min/max pairs")
    if not any(
        a < b
        for a in rec["min_extension_from_response_bps"]
        for b in run["max_extension_from_response_bps"]
    ):
        raise ValueError("No valid reclaim/runaway pairs")
    if resp["window_id"] != ["3s"]:
        raise ValueError("Initial parameter-search design keeps response window fixed at 3s")
    if space.get("full_cartesian_recommended") is not False:
        raise ValueError("Naive full Cartesian search must not be recommended")
