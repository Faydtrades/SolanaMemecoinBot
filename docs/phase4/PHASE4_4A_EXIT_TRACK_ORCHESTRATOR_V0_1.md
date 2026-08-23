# Phase 4.4A — Deterministic Persistent Exit-Track Orchestrator v0.1

This bounded foundation implements **exit-intent decisions only** for the locked Phase-4 tracks:

- `FINAL-A` → `E3_TP10_T15`: +10% fixed TP, 15s fallback.
- `FINAL-B` → `E4_ACT10_GB03_T15`: +10% trail activation, 3% giveback, 15s fallback.
- `SENS-C` → `SENS_TP20_T5`: +20% fixed TP, 5s fallback, sensitivity-only.

No hard stop is added. No parameter is tuned or reselected.

## Preserved research semantics

Threshold return math uses the exact Phase-3 `OutcomeReplayV01` rational-price basis-point calculation and half-away-from-zero rounding. Rule thresholds and fallback clocks remain anchored to the causal Phase-3 CandidateSignal reference/time rather than silently being re-tuned around the simulated entry fill.

Phase 4 adds a separate causality guard: no exit rule may react to a market observation at or before the actual paper entry-fill observation.

`GAP_RECOVERY`, cross-mint observations and price-identity mismatches cannot trigger exits or update the fresh rule mark.

Exact fallback deadline remains market-observation eligible; fallback fires one microsecond later. This mirrors the project's established inclusive-boundary / +1us deterministic clock convention.

## Trailing rule

After activation, the engine retains the highest **observed** rule return. It exits on the first observed point at least 300 bps below that observed peak. There is no interpolation.

## Persistence

The engine persists:
- causal position/track binding,
- fallback deadline,
- trailing activation,
- observed peak,
- last processed observation identity,
- last fresh same-identity mark,
- terminal exit intent.

This is necessary so a restart cannot forget an already-activated trailing state or peak.

## Explicit non-goals in 4.4A

This module does **not**:
- mutate `paper_positions`,
- mark lifecycle `EXIT_PENDING`,
- simulate an exit fill,
- calculate fees/slippage/price impact for the exit,
- calculate PnL/expectancy/PF/drawdown,
- access production market data,
- access RPC/network,
- use a wallet or submit a real order.

Those are subsequent bounded integration steps.
