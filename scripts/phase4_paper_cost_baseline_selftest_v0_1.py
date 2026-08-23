from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase4.paper_cost_baseline_v0_1 import (  # noqa: E402
    MANUAL_TERMINAL_REFERENCE_2026_08_23,
    P4_COST_BASELINE_0001,
)
from src.phase4.paper_cost_model_v0_2 import (  # noqa: E402
    CostSide,
    FillDecision,
    PaperCostModelV02,
    PriorityFeeSource,
    RationalPrice,
    canonical_digest,
)


T0 = datetime(2026, 8, 23, 12, 0, 0, 123456, tzinfo=timezone.utc)


def price(numerator: int, denominator: int = 100) -> RationalPrice:
    return RationalPrice("SOL_NATIVE", numerator, denominator)


def run_once() -> tuple[str, dict[str, object]]:
    baseline = P4_COST_BASELINE_0001
    model = PaperCostModelV02(baseline)
    size = baseline.reference_position_lamports

    # 1) Calibrated explicit-cost baseline at the locked 0.10 SOL reference size.
    entry_cost = model.explicit_costs(side=CostSide.ENTRY, notional_lamports=size)
    exit_cost = model.explicit_costs(side=CostSide.EXIT, notional_lamports=size)

    assert entry_cost.venue_fee_bps == 125
    assert entry_cost.venue_fee_lamports == 1_250_000
    assert entry_cost.interface_fee_lamports == 0
    assert entry_cost.base_network_fee_lamports == 5_000
    assert entry_cost.priority_fee_lamports == 100_000
    assert entry_cost.priority_fee_source is PriorityFeeSource.LOCKED_FALLBACK
    assert entry_cost.builder_tip_lamports == 1_000_000
    assert entry_cost.total_explicit_cost_lamports == 2_355_000
    assert exit_cost.total_explicit_cost_lamports == 2_355_000

    # 2) Live priority observation overrides the fallback without changing the baseline.
    live_priority = model.explicit_costs(
        side=CostSide.ENTRY,
        notional_lamports=size,
        observed_priority_fee_lamports=250_000,
    )
    assert live_priority.priority_fee_source is PriorityFeeSource.LIVE_OBSERVED
    assert live_priority.priority_fee_lamports == 250_000
    assert live_priority.total_explicit_cost_lamports == 2_505_000

    # 3) Venue fee can be supplied by runtime context (e.g. later PumpSwap tier),
    #    without silently rewriting the baseline default.
    lower_venue = model.explicit_costs(
        side=CostSide.EXIT,
        notional_lamports=size,
        venue_fee_bps=100,
    )
    assert lower_venue.venue_fee_lamports == 1_000_000
    assert baseline.default_venue_fee_bps == 125

    # 4) Slippage cap is a rejection limit, not a fixed execution haircut.
    #    Entry: market +5%, impact +1% => execution 1.0605 => 605 bps adverse, FILLED.
    entry_fill = model.evaluate_fill(
        side=CostSide.ENTRY,
        signal_reference_price=price(100),
        market_price_at_ready=price(105),
        price_impact_bps=100,
        signal_observed_at=T0,
        market_observed_at=T0 + timedelta(milliseconds=500),
    )
    assert entry_fill.execution_ready_at == T0 + timedelta(milliseconds=500)
    assert entry_fill.signed_market_move_bps == 500
    assert entry_fill.adverse_slippage_bps == 605
    assert entry_fill.slippage_cap_bps == 1_500
    assert entry_fill.decision is FillDecision.FILLED

    # Entry: +15% market move plus +1% impact => 16.15% adverse => rejected.
    entry_reject = model.evaluate_fill(
        side=CostSide.ENTRY,
        signal_reference_price=price(100),
        market_price_at_ready=price(115),
        price_impact_bps=100,
        signal_observed_at=T0,
        market_observed_at=T0 + timedelta(milliseconds=700),
    )
    assert entry_reject.adverse_slippage_bps == 1_615
    assert entry_reject.decision is FillDecision.REJECTED_SLIPPAGE
    assert entry_reject.rejection_reason == "SLIPPAGE_CAP_EXCEEDED"

    # Exit: market -15%, impact -1% => execution 0.8415 => 15.85% adverse, FILLED.
    exit_fill = model.evaluate_fill(
        side=CostSide.EXIT,
        signal_reference_price=price(100),
        market_price_at_ready=price(85),
        price_impact_bps=100,
        signal_observed_at=T0,
        market_observed_at=T0 + timedelta(milliseconds=500),
    )
    assert exit_fill.signed_market_move_bps == -1_500
    assert exit_fill.adverse_slippage_bps == 1_585
    assert exit_fill.slippage_cap_bps == 2_000
    assert exit_fill.decision is FillDecision.FILLED

    # Exit: market -20%, impact -1% => execution 0.792 => 20.8% adverse => rejected.
    exit_reject = model.evaluate_fill(
        side=CostSide.EXIT,
        signal_reference_price=price(100),
        market_price_at_ready=price(80),
        price_impact_bps=100,
        signal_observed_at=T0,
        market_observed_at=T0 + timedelta(milliseconds=900),
    )
    assert exit_reject.adverse_slippage_bps == 2_080
    assert exit_reject.decision is FillDecision.REJECTED_SLIPPAGE

    # 5) Favorable market movement is not converted into an invented adverse haircut.
    favorable_entry = model.evaluate_fill(
        side=CostSide.ENTRY,
        signal_reference_price=price(100),
        market_price_at_ready=price(95),
        price_impact_bps=100,
        signal_observed_at=T0,
        market_observed_at=T0 + timedelta(milliseconds=500),
    )
    assert favorable_entry.signed_market_move_bps == -500
    assert favorable_entry.adverse_slippage_bps == 0
    assert favorable_entry.simulated_execution_price.fraction == Fraction(9595, 10000)
    assert favorable_entry.decision is FillDecision.FILLED

    # 6) No look-ahead shortcut: the market observation used for fill must be at/after ready_at.
    early_guard = False
    try:
        model.evaluate_fill(
            side=CostSide.ENTRY,
            signal_reference_price=price(100),
            market_price_at_ready=price(101),
            price_impact_bps=0,
            signal_observed_at=T0,
            market_observed_at=T0 + timedelta(milliseconds=499),
        )
    except ValueError:
        early_guard = True
    assert early_guard

    # 7) Manual Terminal settings are preserved as audit-only reference and do not
    #    silently become the bot's slippage caps.
    manual = MANUAL_TERMINAL_REFERENCE_2026_08_23
    assert manual.priority_fee_lamports_per_tx == 100_000
    assert manual.tip_lamports_per_tx == 1_000_000
    assert manual.buy_slippage_cap_bps == 2_000
    assert manual.sell_slippage_cap_bps == 5_000
    assert manual.mev_mode == "OFF"
    assert baseline.entry_slippage_cap_bps == 1_500
    assert baseline.exit_slippage_cap_bps == 2_000
    assert baseline.interface_fee_bps == 0

    digest = canonical_digest(
        baseline=baseline,
        costs=[entry_cost, exit_cost, live_priority, lower_venue],
        fills=[entry_fill, entry_reject, exit_fill, exit_reject, favorable_entry],
    )
    summary = {
        "baseline_fingerprint": baseline.fingerprint,
        "reference_size_lamports": size,
        "entry_explicit_cost_lamports_fallback": entry_cost.total_explicit_cost_lamports,
        "exit_explicit_cost_lamports_fallback": exit_cost.total_explicit_cost_lamports,
        "entry_slippage_cap_bps": baseline.entry_slippage_cap_bps,
        "exit_slippage_cap_bps": baseline.exit_slippage_cap_bps,
        "entry_latency_ms": baseline.entry_latency_ms,
        "exit_latency_ms": baseline.exit_latency_ms,
        "early_observation_guard": early_guard,
    }
    return digest, summary


def main() -> int:
    print("=" * 100)
    print("PHASE 4.2B - RESEARCH COST BASELINE CALIBRATION SELF-TEST v0.1")
    print("=" * 100)
    print(f"Project root                    : {PROJECT_ROOT}")
    print("Production DB touched           : NO")
    print("Network / RPC                   : NO")
    print("Wallet / live orders            : NO")
    print("DEV-FREEZE-0002 used/tuned      : NO")
    print(f"Baseline id                     : {P4_COST_BASELINE_0001.assumption_set_id}")
    print(f"Baseline fingerprint            : {P4_COST_BASELINE_0001.fingerprint}")
    print()
    print("LOCKED EX-ANTE PAPER BASELINE")
    print(f"Reference position              : {P4_COST_BASELINE_0001.reference_position_lamports / 1_000_000_000:.2f} SOL")
    print(f"Default Pump venue fee          : {P4_COST_BASELINE_0001.default_venue_fee_bps / 100:.2f}%")
    print(f"Solana base fee                 : {P4_COST_BASELINE_0001.base_fee_lamports_per_signature} lamports/signature")
    print(f"Priority fallback               : {P4_COST_BASELINE_0001.priority_fee_fallback_lamports_per_tx / 1_000_000_000:.4f} SOL")
    print(f"Builder tip                     : {P4_COST_BASELINE_0001.builder_tip_lamports_per_tx / 1_000_000_000:.3f} SOL")
    print(f"Bot interface fee               : {P4_COST_BASELINE_0001.interface_fee_bps / 100:.2f}%")
    print(f"Buy slippage cap                : {P4_COST_BASELINE_0001.entry_slippage_cap_bps / 100:.0f}% (cap only; not haircut)")
    print(f"Sell slippage cap               : {P4_COST_BASELINE_0001.exit_slippage_cap_bps / 100:.0f}% (cap only; not haircut)")
    print(f"Entry / exit latency            : {P4_COST_BASELINE_0001.entry_latency_ms} / {P4_COST_BASELINE_0001.exit_latency_ms} ms")
    print("Price impact                    : RUNTIME market-state input; no fixed invented haircut")
    print()

    digest_a, summary_a = run_once()
    digest_b, summary_b = run_once()
    deterministic = digest_a == digest_b and summary_a == summary_b

    print("-" * 100)
    print("VALIDATION")
    print("-" * 100)
    print("Versioned immutable baseline              : PASS")
    print("Official/default venue fee arithmetic     : PASS")
    print("Base / priority / tip audit separation    : PASS")
    print("Live priority overrides fallback          : PASS")
    print("Runtime venue-fee override                : PASS")
    print("Slippage cap != fixed PnL haircut         : PASS")
    print("Entry slippage rejection                  : PASS")
    print("Exit slippage rejection                   : PASS")
    print("Favorable movement preserved              : PASS")
    print("500ms causal ready-time guard             : PASS")
    print("Manual Terminal reference kept separate   : PASS")
    print("Direct-bot interface fee = 0              : PASS")
    print(f"Logical digest A                           : {digest_a}")
    print(f"Logical digest B                           : {digest_b}")
    print(f"Cross-run deterministic equality           : {'PASS' if deterministic else 'FAIL'}")
    print()
    print("IMPORTANT: 500ms latency is an EX-ANTE paper assumption, not measured live truth.")
    print("           Later Phase-4 measurement may justify a NEW versioned baseline; do not")
    print("           silently retune this baseline from PnL outcomes.")
    print()
    print("RESULT: PASS" if deterministic else "RESULT: FAIL")
    return 0 if deterministic else 1


if __name__ == "__main__":
    raise SystemExit(main())
