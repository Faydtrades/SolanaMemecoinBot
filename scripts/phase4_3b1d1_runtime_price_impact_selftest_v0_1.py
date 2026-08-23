from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "src" / "phase4" / "runtime_price_impact_v0_1.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "runtime_price_impact_v0_1_selftest_module",
        MODULE_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load runtime_price_impact_v0_1.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    print("=" * 106)
    print("PHASE 4.3B1D1 - RUNTIME PRICE-IMPACT PROVIDER SELF-TEST v0.1")
    print("=" * 106)
    print(f"Project root                 : {PROJECT_ROOT}")
    print(f"Module                       : {MODULE_PATH}")
    print("Production DB opened         : NO")
    print("Collector / network / RPC    : NO")
    print("Paper orders / fills         : NO")
    print("Wallet / live orders         : NO")
    print("Parameter tuning/reselection : NO")
    print()

    mod = load_module()
    Provider = mod.RuntimePriceImpactV01

    checks = {}

    # Exact 100 bps case: q=0.1 SOL, x=10 SOL.
    exact = Provider.for_entry(
        price_identity="SOL_NATIVE",
        requested_size_lamports=100_000_000,
        virtual_sol_reserve_lamports=10_000_000_000,
    )
    checks["exact_100bps"] = (
        exact.available
        and exact.exact_impact_bps_numerator == 1_000_000_000_000
        and exact.exact_impact_bps_denominator == 10_000_000_000
        and exact.router_price_impact_bps == 100
    )

    # Fractional 23.14... bps should conservatively route as 24 bps.
    fractional = Provider.for_entry(
        price_identity="SOL_NATIVE",
        requested_size_lamports=100_000_000,
        virtual_sol_reserve_lamports=43_215_000_000,
    )
    checks["ceil_fractional_bps"] = (
        fractional.available
        and fractional.router_price_impact_bps == 24
    )

    # Increasing requested size must never reduce impact.
    sizes = [10_000_000, 50_000_000, 100_000_000, 250_000_000]
    impacts = [
        Provider.for_entry(
            price_identity="SOL_NATIVE",
            requested_size_lamports=size,
            virtual_sol_reserve_lamports=20_000_000_000,
        ).router_price_impact_bps
        for size in sizes
    ]
    checks["monotonic_with_size"] = impacts == sorted(impacts)

    # Increasing liquidity must never increase impact.
    reserves = [
        2_000_000_000,
        10_000_000_000,
        50_000_000_000,
        100_000_000_000,
    ]
    reserve_impacts = [
        Provider.for_entry(
            price_identity="SOL_NATIVE",
            requested_size_lamports=100_000_000,
            virtual_sol_reserve_lamports=reserve,
        ).router_price_impact_bps
        for reserve in reserves
    ]
    checks["monotonic_with_liquidity"] = (
        reserve_impacts == sorted(reserve_impacts, reverse=True)
    )

    # Determinism.
    again = Provider.for_entry(
        price_identity="SOL_NATIVE",
        requested_size_lamports=100_000_000,
        virtual_sol_reserve_lamports=43_215_000_000,
    )
    checks["deterministic_repeat"] = fractional == again

    # Missing SOL reserve must not fabricate impact.
    missing_sol = Provider.for_entry(
        price_identity="SOL_NATIVE",
        requested_size_lamports=100_000_000,
        virtual_sol_reserve_lamports=None,
    )
    checks["missing_sol_unavailable"] = (
        not missing_sol.available
        and missing_sol.router_price_impact_bps is None
        and missing_sol.reason_code == "SOL_RESERVE_UNAVAILABLE"
    )

    # Quote path must remain unavailable without a causal conversion.
    quote = Provider.for_entry(
        price_identity="QUOTE:USDC_TEST",
        requested_size_lamports=100_000_000,
        virtual_sol_reserve_lamports=None,
        virtual_quote_reserve_raw=25_000_000_000,
        causal_quote_input_raw=None,
    )
    checks["quote_no_conversion_unavailable"] = (
        not quote.available
        and quote.router_price_impact_bps is None
        and quote.reason_code == "QUOTE_INPUT_CONVERSION_UNAVAILABLE"
    )

    # If a future caller supplies a causal quote input, math remains the same
    # constant-product fraction; this test does NOT create the conversion.
    quote_causal = Provider.for_entry(
        price_identity="QUOTE:USDC_TEST",
        requested_size_lamports=100_000_000,
        virtual_sol_reserve_lamports=None,
        virtual_quote_reserve_raw=25_000_000_000,
        causal_quote_input_raw=250_000_000,
    )
    checks["quote_causal_math"] = (
        quote_causal.available
        and quote_causal.router_price_impact_bps == 100
    )

    # Invalid zero/negative reserve and size must fail closed.
    invalid_size_ok = False
    try:
        Provider.for_entry(
            price_identity="SOL_NATIVE",
            requested_size_lamports=0,
            virtual_sol_reserve_lamports=1,
        )
    except ValueError:
        invalid_size_ok = True
    checks["invalid_size_guard"] = invalid_size_ok

    invalid_reserve_ok = False
    try:
        Provider.for_entry(
            price_identity="SOL_NATIVE",
            requested_size_lamports=1,
            virtual_sol_reserve_lamports=0,
        )
    except ValueError:
        invalid_reserve_ok = True
    checks["invalid_reserve_guard"] = invalid_reserve_ok

    # Fingerprint is deterministic and non-empty.
    checks["model_id_locked"] = mod.MODEL_ID == "P4-RUNTIME-PRICE-IMPACT-0001"
    checks["fingerprint_present"] = (
        isinstance(mod.MODEL_FINGERPRINT, str)
        and len(mod.MODEL_FINGERPRINT) == 64
    )

    print("-" * 106)
    print("VALIDATION")
    print("-" * 106)
    for label, ok in checks.items():
        print(f"{label:<44}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values())
    print()
    print(f"Model ID                     : {mod.MODEL_ID}")
    print(f"Model fingerprint            : {mod.MODEL_FINGERPRINT}")
    print(f"Mechanics                    : {mod.MECHANICS}")
    print(f"Router rounding              : {mod.ROUTER_ROUNDING}")
    print(f"Quote policy                 : {mod.QUOTE_POLICY}")
    print(f"Example 0.10 SOL / 43.215 SOL: exact={fractional.exact_impact_bps_numerator}/{fractional.exact_impact_bps_denominator} bps -> router={fractional.router_price_impact_bps} bps")
    print("Fixed price-impact haircut   : NO")
    print("Fee subtraction inside impact: NO")
    print("Production DB touched        : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    if all_ok:
        print(
            "NEXT: Phase 4.3B1D2 causal SOL_NATIVE entry-fill smoke using "
            "first post-ready live market observation and this exact provider."
        )
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
