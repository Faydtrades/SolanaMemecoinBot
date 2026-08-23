from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase4.paper_cost_model_v0_2 import (
    CostSide,
    FillDecision,
    PaperCostBaseline,
    PaperCostModelV02,
    RationalPrice,
)
from phase4.runtime_exit_price_impact_v0_1 import (
    INVENTORY_POLICY,
    MECHANICS,
    MODEL_FINGERPRINT,
    MODEL_ID,
    QUOTE_POLICY,
    ROUTER_ROUNDING,
    RuntimeExitPriceImpactV01,
)


def main() -> int:
    print("=" * 116)
    print("PHASE 4.4C - SOL_NATIVE EXIT PRICE-IMPACT PROVIDER SELF-TEST v0.1")
    print("=" * 116)
    print(f"Project root                       : {PROJECT_ROOT}")
    print("Production DB opened               : NO")
    print("Collector / network / RPC          : NO")
    print("Wallet / signing / live orders     : NO")
    print("Paper position mutation            : NO")
    print("Exit fill / PnL mutation           : NO")
    print("Parameter tuning / reselection     : NO")
    print()

    Provider = RuntimeExitPriceImpactV01
    checks: dict[str, bool] = {}

    # 0.10 SOL at 1000 lamports/token_raw -> exactly 100,000 token_raw.
    # Current reserve 9.9m -> q/(y+q) = 100k/10m = exactly 100 bps.
    exact = Provider.for_position(
        price_identity="SOL_NATIVE",
        filled_size_lamports=100_000_000,
        entry_price_numerator_raw=1_000,
        entry_price_denominator_raw=1,
        current_virtual_token_reserve_raw=9_900_000,
    )
    checks["exact_token_inventory_reconstruction"] = (
        exact.available
        and exact.derived_token_input_raw == 100_000
        and exact.inventory_remainder_numerator == 0
    )
    checks["exact_100bps_exit_impact"] = (
        exact.exact_impact_bps_numerator == 1_000_000_000
        and exact.exact_impact_bps_denominator == 10_000_000
        and exact.router_price_impact_bps == 100
    )

    # Floor policy: 100*3/7 = 42 raw tokens plus 6/7 remainder.
    floor_case = Provider.for_position(
        price_identity="SOL_NATIVE",
        filled_size_lamports=100,
        entry_price_numerator_raw=7,
        entry_price_denominator_raw=3,
        current_virtual_token_reserve_raw=100_000,
    )
    checks["inventory_floor_never_oversells"] = (
        floor_case.available
        and floor_case.derived_token_input_raw == 42
        and floor_case.inventory_remainder_numerator == 6
        and floor_case.inventory_remainder_denominator == 7
        and Fraction(floor_case.derived_token_input_raw, 1)
            <= Fraction(100 * 3, 7)
    )

    # Fractional impact must round UP for the integer-bps router input.
    fractional = Provider.for_position(
        price_identity="SOL_NATIVE",
        filled_size_lamports=100_000_000,
        entry_price_numerator_raw=1_000,
        entry_price_denominator_raw=1,
        current_virtual_token_reserve_raw=10_000_000,
    )
    # 100000 / 10100000 * 10000 = 99.0099... -> 100 bps.
    checks["ceil_fractional_exit_impact"] = (
        fractional.available
        and fractional.router_price_impact_bps == 100
    )

    # More position inventory cannot reduce impact at fixed liquidity.
    sizes = [10_000_000, 50_000_000, 100_000_000, 250_000_000]
    size_impacts = [
        Provider.for_position(
            price_identity="SOL_NATIVE",
            filled_size_lamports=size,
            entry_price_numerator_raw=1_000,
            entry_price_denominator_raw=1,
            current_virtual_token_reserve_raw=20_000_000,
        ).router_price_impact_bps
        for size in sizes
    ]
    checks["impact_monotonic_with_position_size"] = size_impacts == sorted(size_impacts)

    # More token liquidity cannot increase impact for the same position.
    reserves = [2_000_000, 10_000_000, 50_000_000, 100_000_000]
    reserve_impacts = [
        Provider.for_position(
            price_identity="SOL_NATIVE",
            filled_size_lamports=100_000_000,
            entry_price_numerator_raw=1_000,
            entry_price_denominator_raw=1,
            current_virtual_token_reserve_raw=reserve,
        ).router_price_impact_bps
        for reserve in reserves
    ]
    checks["impact_monotonic_with_liquidity"] = (
        reserve_impacts == sorted(reserve_impacts, reverse=True)
    )

    # Determinism.
    again = Provider.for_position(
        price_identity="SOL_NATIVE",
        filled_size_lamports=100_000_000,
        entry_price_numerator_raw=1_000,
        entry_price_denominator_raw=1,
        current_virtual_token_reserve_raw=10_000_000,
    )
    checks["deterministic_repeat"] = fractional == again

    # Fail closed when causal reserve is unavailable.
    no_reserve = Provider.for_position(
        price_identity="SOL_NATIVE",
        filled_size_lamports=100_000_000,
        entry_price_numerator_raw=1_000,
        entry_price_denominator_raw=1,
        current_virtual_token_reserve_raw=None,
    )
    checks["missing_token_reserve_unavailable"] = (
        not no_reserve.available
        and no_reserve.router_price_impact_bps is None
        and no_reserve.reason_code == "TOKEN_RESERVE_UNAVAILABLE"
    )

    # QUOTE path remains unavailable; no hidden SOL->quote inventory conversion.
    quote = Provider.for_position(
        price_identity="QUOTE:USDC_TEST",
        filled_size_lamports=100_000_000,
        entry_price_numerator_raw=1_000,
        entry_price_denominator_raw=1,
        current_virtual_token_reserve_raw=10_000_000,
    )
    checks["quote_inventory_unavailable"] = (
        not quote.available
        and quote.router_price_impact_bps is None
        and quote.reason_code == "QUOTE_EXIT_INVENTORY_UNAVAILABLE"
    )

    # Guards.
    invalid_size = False
    try:
        Provider.for_position(
            price_identity="SOL_NATIVE",
            filled_size_lamports=0,
            entry_price_numerator_raw=1,
            entry_price_denominator_raw=1,
            current_virtual_token_reserve_raw=1,
        )
    except ValueError:
        invalid_size = True
    checks["invalid_position_size_guard"] = invalid_size

    invalid_price = False
    try:
        Provider.for_position(
            price_identity="SOL_NATIVE",
            filled_size_lamports=1,
            entry_price_numerator_raw=0,
            entry_price_denominator_raw=1,
            current_virtual_token_reserve_raw=1,
        )
    except ValueError:
        invalid_price = True
    checks["invalid_entry_price_guard"] = invalid_price

    invalid_reserve = False
    try:
        Provider.for_position(
            price_identity="SOL_NATIVE",
            filled_size_lamports=1_000,
            entry_price_numerator_raw=1,
            entry_price_denominator_raw=1,
            current_virtual_token_reserve_raw=0,
        )
    except ValueError:
        invalid_reserve = True
    checks["invalid_token_reserve_guard"] = invalid_reserve

    checks["model_id_locked"] = MODEL_ID == "P4-RUNTIME-EXIT-PRICE-IMPACT-0001"
    checks["fingerprint_present"] = isinstance(MODEL_FINGERPRINT, str) and len(MODEL_FINGERPRINT) == 64

    # Compatibility with the exact Phase-4 cost model exit semantics.
    baseline = PaperCostBaseline(
        assumption_set_id="SELFTEST-EXIT-IMPACT-COMPAT",
        scope="SELFTEST_ONLY",
        reference_position_lamports=100_000_000,
        default_venue_fee_bps=125,
        base_fee_lamports_per_signature=5_000,
        signatures_per_tx=1,
        priority_fee_fallback_lamports_per_tx=100_000,
        builder_tip_lamports_per_tx=1_000_000,
        interface_fee_bps=0,
        entry_slippage_cap_bps=1_500,
        exit_slippage_cap_bps=2_000,
        entry_latency_ms=500,
        exit_latency_ms=500,
        priority_fee_policy="SELFTEST",
        price_impact_policy="RUNTIME",
        latency_calibration_status="SELFTEST",
    )
    cost_model = PaperCostModelV02(baseline)
    t0 = datetime(2026, 8, 23, 12, 0, 0, tzinfo=timezone.utc)
    market_at_ready = RationalPrice("SOL_NATIVE", 1_000, 1)
    fill_eval = cost_model.evaluate_fill(
        side=CostSide.EXIT,
        signal_reference_price=market_at_ready,
        market_price_at_ready=market_at_ready,
        price_impact_bps=exact.router_price_impact_bps,
        signal_observed_at=t0,
        market_observed_at=t0 + timedelta(milliseconds=500),
    )
    checks["cost_model_exit_side_compatibility"] = (
        fill_eval.decision is FillDecision.FILLED
        and fill_eval.simulated_execution_price.fraction == Fraction(990, 1)
        and fill_eval.price_impact_bps == 100
    )

    rejected = cost_model.evaluate_fill(
        side=CostSide.EXIT,
        signal_reference_price=market_at_ready,
        market_price_at_ready=market_at_ready,
        price_impact_bps=2_500,
        signal_observed_at=t0,
        market_observed_at=t0 + timedelta(milliseconds=500),
    )
    checks["cost_model_exit_slippage_rejection_works"] = (
        rejected.decision is FillDecision.REJECTED_SLIPPAGE
        and rejected.rejection_reason == "SLIPPAGE_CAP_EXCEEDED"
    )

    print("-" * 116)
    print("VALIDATION")
    print("-" * 116)
    for label, ok in checks.items():
        print(f"{label:<52}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values())
    print()
    print(f"Model ID                             : {MODEL_ID}")
    print(f"Model fingerprint                    : {MODEL_FINGERPRINT}")
    print(f"Mechanics                            : {MECHANICS}")
    print(f"Inventory policy                     : {INVENTORY_POLICY}")
    print(f"Router rounding                      : {ROUTER_ROUNDING}")
    print(f"Quote policy                         : {QUOTE_POLICY}")
    print(f"Exact example token input            : {exact.derived_token_input_raw} raw")
    print(f"Exact example impact                 : {exact.router_price_impact_bps} bps")
    print("Fixed exit price-impact haircut      : NO")
    print("Fee subtraction inside curve impact : NO")
    print("Production DB touched                : NO")
    print("Live market action                   : NO")
    print("Wallet/signing/live orders           : NO")
    print("Parameter tuning/reselection         : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    if all_ok:
        print(
            "NEXT: bind EXIT_PENDING positions to first causal post-ready SOL_NATIVE "
            "observation and simulated exit-fill execution; no PnL/accounting yet."
        )
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
