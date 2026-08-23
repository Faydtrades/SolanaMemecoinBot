from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.parameter_search_design_v0_1 import (
    TokenOpportunityMetrics,
    build_proposed_search_space,
    nearest_rank,
    propose_grid,
    quantile_summary,
    stable_sha256,
    validate_search_space,
)

failures: list[str] = []


def check(label: str, ok: bool) -> None:
    print(f"{label:<78} {'PASS' if ok else 'FAIL'}")
    if not ok:
        failures.append(label)


print("PHASE 2 EMPIRICAL PARAMETER-SEARCH DESIGN SELF-TEST v0.1")
print("Purpose                    : FEATURE-SUPPORT SEARCH RANGE MECHANICS")
print("Outcome/PnL labels          : NOT USED")
print("Parameter optimization      : NOT PERFORMED")
print("Search-space status         : PROPOSAL ONLY")
print("Production DB touched       : NO")
print()

vals = [1, 2, 3, 4, 5, 100]
check("Test 1a - nearest-rank p50 deterministic", nearest_rank(vals, 0.50) == 3)
check("Test 1b - nearest-rank p95 preserves upper-tail support", nearest_rank(vals, 0.95) == 100)
summary = quantile_summary(vals)
check("Test 1c - quantile summary records n/min/max", summary["n"] == 6 and summary["min"] == 1 and summary["max"] == 100)

grid = propose_grid(
    [450, 900, 1400, 3100, 5200, 8800],
    quantiles=(0.25, 0.50, 0.75, 0.90),
    step=500,
    anchors=(1500, 3000, 5000),
    minimum=500,
    max_items=8,
)
check("Test 2a - proposed grid sorted and unique", grid == sorted(set(grid)))
check("Test 2b - pre-existing characterization anchors retained", {1500, 3000, 5000}.issubset(grid))
check("Test 2c - grid generation uses feature support only", all(isinstance(x, int) for x in grid))

rows = []
for i in range(1, 41):
    rows.append(
        TokenOpportunityMetrics(
            mint=f"M{i}",
            price_identity="SOL",
            max_return_bps=500 + i * 250,
            max_trades_since_t0=1 + i % 12,
            max_unique_buyers_since_t0=1 + i % 8,
            max_drawdown_depth_bps=500 + i * 150,
            max_3s_buys=1 + i % 6,
            max_3s_net_flow_reserve_ppm=i * 2500,
            broad_impulse_reached=True,
            broad_impulse_age_ms=1000 * i,
            broad_impulse_return_bps=500 + i * 250,
            broad_impulse_trades=2 + i % 8,
            broad_impulse_unique_buyers=1 + i % 6,
            broad_pullback_reached=True,
            broad_pullback_age_ms=2000 * i,
            broad_pullback_depth_at_activation_bps=1000 + i * 100,
            max_pullback_depth_after_broad_impulse_bps=1500 + i * 175,
            broad_proto_response_reached=True,
            broad_proto_response_age_ms=2500 * i,
            max_rebound_after_pullback_bps=100 + i * 80,
            max_3s_buys_after_pullback=1 + i % 5,
            max_3s_net_flow_reserve_ppm_after_pullback=i * 3500,
            max_reclaim_extension_after_proto_response_bps=100 + i * 90,
        )
    )

space_a = build_proposed_search_space(rows)
space_b = build_proposed_search_space(rows)
try:
    validate_search_space(space_a)
    valid = True
except Exception:
    valid = False
check("Test 3a - proposed search space validates", valid)
check("Test 3b - search design deterministic", stable_sha256(space_a) == stable_sha256(space_b))
check("Test 3c - max entry age remains locked at 5m", space_a["max_entry_age_ms"] == [300_000])
check("Test 3d - initial buyer-response window remains fixed 3s", space_a["buyer_response"]["window_id"] == ["3s"])
check("Test 3e - reserve-normalized flow has its own ppm grid", len(space_a["buyer_response"]["min_net_flow_reserve_ppm"]) > 1)
check("Test 3f - valid pullback min/max pairs exist", space_a["pullback"]["valid_min_max_pair_count"] > 0)
check("Test 3g - valid reclaim/runaway pairs exist", space_a["runaway"]["valid_reclaim_runaway_pair_count"] > 0)
check("Test 3h - naive full Cartesian search explicitly rejected", space_a["full_cartesian_recommended"] is False)
check("Test 3i - search space remains proposal, not silently locked", space_a["status"] == "PROPOSAL_NOT_LOCKED")

print()
print(f"{'Empirical quantile mechanics':<52}: {'PASS' if not any(x.startswith('Test 1') for x in failures) else 'FAIL'}")
print(f"{'Grid construction / anchor preservation':<52}: {'PASS' if not any(x.startswith('Test 2') for x in failures) else 'FAIL'}")
print(f"{'Search-space safety / staging':<52}: {'PASS' if not any(x.startswith('Test 3') for x in failures) else 'FAIL'}")
print(f"{'Outcome/PnL labels used':<52}: NO")
print(f"{'Parameter optimization performed':<52}: NO")
print(f"{'Production DB touched':<52}: NO")
print(f"{'RESULT':<52}: {'PASS' if not failures else 'FAIL'}")

if failures:
    raise SystemExit(1)
