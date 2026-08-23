from __future__ import annotations

from .paper_cost_model_v0_2 import ManualTerminalReference, PaperCostBaseline


# Phase 4.2B ex-ante paper baseline.
#
# This baseline is intentionally locked BEFORE continuous Phase-4 paper results.
# It is not derived by tuning DEV-FREEZE-0002 or by selecting values that improve PnL.
# If later live/shadow measurements show that an assumption is unrealistic, create a
# new versioned baseline with evidence; do not silently edit this object.
P4_COST_BASELINE_0001 = PaperCostBaseline(
    assumption_set_id="P4-COST-BASELINE-0001",
    scope="NEW_PUMP_SOL_USDC_EARLY_LIFECYCLE_PAPER",
    reference_position_lamports=100_000_000,  # 0.10 SOL locked research size
    default_venue_fee_bps=125,                # 1.25% Pump bonding-curve / lowest canonical tier
    base_fee_lamports_per_signature=5_000,    # current Solana base fee per signature
    signatures_per_tx=1,
    priority_fee_fallback_lamports_per_tx=100_000,  # 0.0001 SOL; user's manual reference fallback
    builder_tip_lamports_per_tx=1_000_000,          # 0.001 SOL; conservative fast-route paper overhead
    interface_fee_bps=0,                     # direct bot baseline; no Terminal/Padre UI fee
    entry_slippage_cap_bps=1_500,             # 15% rejection cap; NOT a fixed slippage haircut
    exit_slippage_cap_bps=2_000,              # 20% rejection cap; NOT a fixed slippage haircut
    entry_latency_ms=500,                     # ex-ante conservative paper baseline, not measured truth
    exit_latency_ms=500,
    priority_fee_policy="LIVE_OBSERVED_IF_AVAILABLE_ELSE_LOCKED_FALLBACK",
    price_impact_policy="RUNTIME_MARKET_STATE_REQUIRED_NO_FIXED_HAIRCUT",
    latency_calibration_status="EX_ANTE_LOCKED_UNTIL_PHASE4_MEASUREMENT",
)


# Audit-only manual reference captured from the user's Terminal/Padre preset screenshots.
# It must remain separate from the bot baseline.
MANUAL_TERMINAL_REFERENCE_2026_08_23 = ManualTerminalReference(
    reference_id="MANUAL-TERMINAL-2026-08-23",
    priority_fee_lamports_per_tx=100_000,   # 0.0001 SOL
    tip_lamports_per_tx=1_000_000,          # 0.001 SOL
    buy_slippage_cap_bps=2_000,             # 20%
    sell_slippage_cap_bps=5_000,            # 50%
    mev_mode="OFF",
)
