# Phase 4.3B1C — Live CandidateSignal → Simulated Entry Routing Smoke v0.1

## Purpose

This is the first live strategy-routing boundary after Phase 4.3B1B passed.

It uses:

- validated `live_pump_collector_v0_3_4.py`;
- only mints whose LAUNCH is observed after the smoke start cursor;
- `Phase1QuoteAwareAdapterV01`;
- `DenominationAwareFeatureEngineV01`;
- exact locked FirstPullback v1.1 entry roles:
  - CONTROL
  - ROBUST_1
  - ROBUST_2
  - ROBUST_3
- exact 5-minute BOT_TRUTH entry clock;
- `P4-COST-BASELINE-0001`;
- `PaperEntryRouterV01`;
- the three locked Phase-4 paper tracks.

The Phase-3 locked entry rows are not searched or reselected. Their exact
parameter IDs and thresholds are asserted against the locked EXP-0005 selection.

## Important boundary

This smoke validates:

`live market -> CandidateSignal -> deterministic paper route -> 3 ENTRY_PENDING paper orders`

It intentionally does **not** fabricate an entry fill.

The router's fill API requires a causal post-signal market observation and a
runtime price-impact input. The Phase-4 cost baseline explicitly forbids an
invented fixed price-impact haircut, so fill integration remains the next
bounded step.

## Safety

- production SQLite: sidecar opens query-only/read-only;
- collector writes normal market data as already validated;
- paper orders are written only to a new isolated paper SQLite DB;
- no wallet;
- no signing;
- no live orders;
- no parameter tuning/reselection.

## Runtime

The default hard limit is 600 seconds (10 minutes), but the test stops as soon
as the first routable live CandidateSignal is deterministically routed.

No manual Ctrl+C is required.

Run:

```powershell
python scripts\phase4_3b1c_live_candidate_route_smoke_v0_1.py
```

A window with no CandidateSignal is `RESULT: CHECK`, not a failed strategy.
Re-run the same locked test; do not alter parameters to force a signal.
