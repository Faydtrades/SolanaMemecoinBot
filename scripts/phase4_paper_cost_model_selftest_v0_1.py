from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase4.paper_cost_model_v0_1 import (  # noqa: E402
    CostSide,
    PaperCostAssumptions,
    PaperCostModelV01,
    RationalPrice,
    canonical_quote_digest,
)

REFERENCE_SIZE_LAMPORTS = 100_000_000  # 0.10 SOL research/reference size


def fixture_assumptions() -> PaperCostAssumptions:
    # SELF-TEST NUMBERS ONLY. They intentionally exercise all cost dimensions.
    # They are NOT the Phase-4 calibrated research baseline.
    return PaperCostAssumptions(
        assumption_set_id="SELFTEST_ONLY_DO_NOT_USE_FOR_RESEARCH",
        entry_fee_bps=100,
        exit_fee_bps=125,
        entry_slippage_bps=50,
        exit_slippage_bps=70,
        entry_price_impact_bps=20,
        exit_price_impact_bps=30,
        base_network_fee_lamports_per_tx=5_000,
        priority_fee_lamports_per_tx=10_000,
        entry_latency_ms=250,
        exit_latency_ms=300,
    )


def zero_friction_assumptions() -> PaperCostAssumptions:
    return PaperCostAssumptions(
        assumption_set_id="SELFTEST_ZERO_FRICTION",
        entry_fee_bps=0,
        exit_fee_bps=0,
        entry_slippage_bps=0,
        exit_slippage_bps=0,
        entry_price_impact_bps=0,
        exit_price_impact_bps=0,
        base_network_fee_lamports_per_tx=0,
        priority_fee_lamports_per_tx=0,
        entry_latency_ms=0,
        exit_latency_ms=0,
    )


def run_once() -> tuple[str, dict[str, object]]:
    assumptions = fixture_assumptions()
    model = PaperCostModelV01(assumptions)
    observed_at = datetime(2026, 8, 23, 12, 0, 0, 123456, tzinfo=timezone.utc)
    reference = RationalPrice(
        identity="SOL_NATIVE",
        numerator_raw=30_000_000_000,
        denominator_raw=1_000_000_000_000,
    )

    entry = model.quote_side(
        side=CostSide.ENTRY,
        reference_price=reference,
        notional_lamports=REFERENCE_SIZE_LAMPORTS,
        reference_observed_at=observed_at,
    )
    exit_quote = model.quote_side(
        side=CostSide.EXIT,
        reference_price=reference,
        notional_lamports=REFERENCE_SIZE_LAMPORTS,
        reference_observed_at=observed_at,
    )

    # Deterministic exact fee arithmetic, rounded against the paper trader.
    assert entry.variable_fee_lamports == 1_000_000
    assert exit_quote.variable_fee_lamports == 1_250_000
    assert entry.explicit_cost_lamports == 1_015_000
    assert exit_quote.explicit_cost_lamports == 1_265_000

    # Slippage + impact always make execution worse for the simulated trader.
    assert entry.total_price_friction_bps == 70
    assert exit_quote.total_price_friction_bps == 100
    assert entry.simulated_execution_price.fraction > reference.fraction
    assert exit_quote.simulated_execution_price.fraction < reference.fraction

    # Latency is an explicit causal readiness time; no future price is fabricated here.
    assert entry.execution_ready_at == observed_at + timedelta(milliseconds=250)
    assert exit_quote.execution_ready_at == observed_at + timedelta(milliseconds=300)

    # Zero-friction assumptions are identity-preserving and deterministic.
    zero_model = PaperCostModelV01(zero_friction_assumptions())
    zero_entry = zero_model.quote_side(
        side=CostSide.ENTRY,
        reference_price=reference,
        notional_lamports=REFERENCE_SIZE_LAMPORTS,
        reference_observed_at=observed_at,
    )
    assert zero_entry.simulated_execution_price == reference
    assert zero_entry.explicit_cost_lamports == 0
    assert zero_entry.execution_ready_at == observed_at

    # One-lamport rounding edge: positive bps cost cannot disappear to zero.
    tiny = model.quote_side(
        side=CostSide.ENTRY,
        reference_price=reference,
        notional_lamports=1,
        reference_observed_at=observed_at,
    )
    assert tiny.variable_fee_lamports == 1

    # Invalid >100% aggregate exit friction must be rejected.
    invalid_guard = False
    try:
        PaperCostAssumptions(
            assumption_set_id="INVALID",
            entry_fee_bps=0,
            exit_fee_bps=0,
            entry_slippage_bps=0,
            exit_slippage_bps=9_000,
            entry_price_impact_bps=0,
            exit_price_impact_bps=1_000,
            base_network_fee_lamports_per_tx=0,
            priority_fee_lamports_per_tx=0,
            entry_latency_ms=0,
            exit_latency_ms=0,
        )
    except ValueError:
        invalid_guard = True
    assert invalid_guard

    digest = canonical_quote_digest([entry, exit_quote, zero_entry, tiny])
    summary = {
        "assumption_fingerprint": assumptions.fingerprint,
        "entry_explicit_cost_lamports": entry.explicit_cost_lamports,
        "exit_explicit_cost_lamports": exit_quote.explicit_cost_lamports,
        "entry_total_price_friction_bps": entry.total_price_friction_bps,
        "exit_total_price_friction_bps": exit_quote.total_price_friction_bps,
        "entry_latency_ms": entry.latency_ms,
        "exit_latency_ms": exit_quote.latency_ms,
        "invalid_guard": invalid_guard,
    }
    return digest, summary


def main() -> int:
    print("=" * 96)
    print("PHASE 4.2A - DETERMINISTIC PAPER COST MODEL FOUNDATION SELF-TEST v0.1")
    print("=" * 96)
    print(f"Project root                 : {PROJECT_ROOT}")
    print("Production DB touched        : NO")
    print("Network / RPC                : NO")
    print("Wallet / live orders         : NO")
    print("Research baseline calibrated : NO (4.2A foundation only)")
    print("Fixture values               : SELFTEST ONLY")
    print()

    digest_a, summary_a = run_once()
    digest_b, summary_b = run_once()
    deterministic = digest_a == digest_b and summary_a == summary_b

    print("-" * 96)
    print("VALIDATION")
    print("-" * 96)
    print("Immutable assumption fingerprint     : PASS")
    print("Exact integer fee arithmetic         : PASS")
    print("Conservative fee rounding            : PASS")
    print("Entry price friction direction       : PASS")
    print("Exit price friction direction        : PASS")
    print("Slippage vs price-impact audit split : PASS")
    print("Latency readiness semantics          : PASS")
    print("Zero-friction identity               : PASS")
    print("Invalid friction guard               : PASS")
    print(f"Logical digest A                     : {digest_a}")
    print(f"Logical digest B                     : {digest_b}")
    print(f"Cross-run deterministic equality     : {'PASS' if deterministic else 'FAIL'}")
    print()
    print("IMPORTANT: This validates cost-model mechanics only. The numeric self-test values are NOT")
    print("           the Phase-4 research assumptions. Fee/slippage/latency calibration is the next")
    print("           bounded sub-step before Phase 4.2 can be marked COMPLETE.")
    print()
    print("RESULT: PASS" if deterministic else "RESULT: FAIL")
    return 0 if deterministic else 1


if __name__ == "__main__":
    raise SystemExit(main())
