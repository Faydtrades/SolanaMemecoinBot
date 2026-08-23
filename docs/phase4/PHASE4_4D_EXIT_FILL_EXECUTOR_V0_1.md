# Phase 4.4D — Causal EXIT_PENDING → CLOSED Simulated Exit-Fill Executor v0.1

## Scope

This step implements the persistent paper-execution boundary after the locked
Phase-4 exit orchestrator has produced an immutable ExitIntent and the lifecycle
position is EXIT_PENDING.

It validates:

- locked 500ms EXIT latency from ExitIntent.requested_at;
- first qualifying post-ready same-mint / same-identity / non-gap observation;
- P4-RUNTIME-EXIT-PRICE-IMPACT-0001;
- PaperCostModelV02 EXIT slippage semantics;
- lifecycle EXIT_PENDING -> CLOSED only on an accepted simulated fill;
- persisted slippage-rejection attempts with later causal retry;
- restart/replay determinism;
- staggered sibling tracks remain independent;
- fallback reference uses a real persisted market/reference price, never a
  fabricated fallback fill price.

## Fallback execution reference

For TAKE_PROFIT/TRAIL, the exact trigger market price is the execution-slippage
reference.

For FALLBACK:
1. use the persisted last-fresh real market mark when available;
2. if there was no post-entry fresh mark, use the real persisted CandidateSignal
   rule-reference price.

The fallback timer itself never fabricates a price.

## Execution economics

On an accepted fill the executor persists:

- derived token input;
- runtime exit price impact;
- simulated rational exit execution price;
- floor gross SOL proceeds;
- locked explicit EXIT costs.

It intentionally does **not** calculate PnL, expectancy, PF or drawdown yet.

## Restart safety

Accepted fills are first persisted as `FILL_SELECTED`, then applied to
`PaperLifecycleStore.close_position()`. On restart, any staged fill is recovered
deterministically. Exact observation replay is idempotent.

## Local validation before delivery

The exact packaged implementation/self-test was run locally before delivery:

`RESULT: PASS`

## Run

From the project root with `(.venv)` active:

```powershell
python scripts\phase4_4d_exit_fill_executor_selftest_v0_1.py
```

No collector, production DB, RPC, wallet or live order is used by this self-test.
