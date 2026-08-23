# Phase 4.5C — Controlled Live Runtime Observability Binding v0.1

## Purpose

Bind the already validated `P4-RUNTIME-OBSERVABILITY-0001` persistence model to the controlled live Phase-4 entry/exit runtime.

The live smoke preserves the existing Phase-4.4E execution path and adds:

- every actual FirstPullback strategy evaluation -> persisted strategy evaluation audit;
- genuine terminal no-candidate runs -> explicit `SKIP` decisions;
- accepted paper entry -> explicit `TRADE` decision;
- entry slippage rejection -> explicit execution `SKIP` decision;
- unsupported QUOTE candidate -> explicit `QUOTE_INPUT_CONVERSION_UNAVAILABLE` skip;
- causal SOL_NATIVE market marks for every open/EXIT_PENDING paper position;
- per-track MTM equity stream with no FINAL-A/B/SENS-C cross-track summation;
- live MAE/MFE and mark-to-market max drawdown;
- final per-track equity reconciliation to deterministic completed-trade accounting.

## Causality

Position marks use only real observed non-gap SOL_NATIVE events. The entry-fill event itself may be the first mark because the position exists at that exact execution observation. Later marks are persisted in production ingest order.

For every market event, position marks are persisted before exit decisions; per-track equity is persisted after any exit executions from that same event. Therefore a closing event contributes to MAE/MFE while the equity mark reflects the realized CLOSED state.

## Strategy audit

The runtime records exact strategy evaluations as they occur. It records terminal SKIP only when the run actually finishes without a CandidateSignal or when an explicit execution boundary rejects/does not support an already-emitted candidate. It does not invent retrospective skip reasons.

A bounded live smoke may legitimately observe zero natural expired SKIPs; the local self-test separately validates the real SKIP persistence path.

## Safety

- production market DB sidecar access remains query-only/read-only;
- collector v0.3.4 performs only its existing validated market-data writes;
- paper state and observability are stored only in the isolated fresh paper DB;
- no wallet, signing or live orders;
- no entry/exit parameter tuning or reselection;
- no cross-track portfolio PnL aggregation.

## Validation sequence

Run the local binding self-test first:

```powershell
python scripts\phase4_5c_live_observability_binding_selftest_v0_1.py
```

Only if that returns `RESULT: PASS`, run the controlled live smoke:

```powershell
python scripts\phase4_5c_live_observability_smoke_v0_1.py
```

The live smoke requires the user's actual Phase-2 source, locked EXP-0005 selection, production collector and live market DB, so final live validation cannot be reproduced in the isolated local fixture environment.
