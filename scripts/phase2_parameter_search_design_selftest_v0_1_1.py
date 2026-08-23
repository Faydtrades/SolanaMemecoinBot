from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.parameter_search_design_v0_1_1 import (
    TokenOpportunityMetrics,
    build_proposed_search_space,
    price_identity_family,
    stable_sha256,
    validate_search_space,
)
from src.phase2.quote_aware_price_v0_1 import PriceIdentity, PricePathKind

failures: list[str] = []


def check(label: str, ok: bool) -> None:
    print(f"{label:<82} {'PASS' if ok else 'FAIL'}")
    if not ok:
        failures.append(label)


print("PHASE 2 EMPIRICAL PARAMETER-SEARCH DESIGN REGRESSION SELF-TEST v0.1.1")
print("Purpose                    : PRICE-IDENTITY ACCOUNTING + SEARCH-SPACE NON-REGRESSION")
print("Outcome/PnL labels          : NOT USED")
print("Parameter optimization      : NOT PERFORMED")
print("Search-space status         : PROPOSAL ONLY")
print("Production DB touched       : NO")
print()

sol_label = PriceIdentity(PricePathKind.SOL, None).label
quote_label = PriceIdentity(PricePathKind.QUOTE, "QuoteMint").label

check("Test 1a - production SOL identity label is SOL_NATIVE", sol_label == "SOL_NATIVE")
check("Test 1b - SOL_NATIVE maps to SOL research family", price_identity_family(sol_label) == "SOL")
check("Test 1c - QUOTE:<mint> maps to QUOTE research family", price_identity_family(quote_label) == "QUOTE")
check("Test 1d - unknown identity does not silently count as SOL/QUOTE", price_identity_family("UNAVAILABLE") == "OTHER")

labels = [sol_label] * 3792 + [quote_label] * 109
sol_n = sum(price_identity_family(x) == "SOL" for x in labels)
quote_n = sum(price_identity_family(x) == "QUOTE" for x in labels)
check("Test 2a - 3792 SOL + 109 QUOTE identities account for 3901 tokens", sol_n == 3792 and quote_n == 109 and sol_n + quote_n == 3901)

rows = []
for i in range(1, 41):
    identity = sol_label if i % 5 else quote_label
    rows.append(
        TokenOpportunityMetrics(
            mint=f"M{i}",
            price_identity=identity,
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

check("Test 3a - corrected identity accounting does not alter search-space mechanics", valid)
check("Test 3b - search-space calculation remains deterministic", stable_sha256(space_a) == stable_sha256(space_b))
check("Test 3c - search space remains proposal-only", space_a["status"] == "PROPOSAL_NOT_LOCKED")
check("Test 3d - no naive full Cartesian search is recommended", space_a["full_cartesian_recommended"] is False)

print()
print(f"{'Production price-identity accounting':<55}: {'PASS' if not any(x.startswith('Test 1') or x.startswith('Test 2') for x in failures) else 'FAIL'}")
print(f"{'Parameter-search design non-regression':<55}: {'PASS' if not any(x.startswith('Test 3') for x in failures) else 'FAIL'}")
print(f"{'Outcome/PnL labels used':<55}: NO")
print(f"{'Parameter optimization performed':<55}: NO")
print(f"{'Production DB touched':<55}: NO")
print(f"{'RESULT':<55}: {'PASS' if not failures else 'FAIL'}")

if failures:
    raise SystemExit(1)
