# Phase 4.5B — Causal MTM / MAE-MFE / Strategy Decision Persistence v0.1

## Purpose

Phase 4.5A proved deterministic completed-trade accounting but correctly left
three locked requirements unavailable from fills alone:

- mark-to-market max drawdown;
- MAE / MFE;
- full strategy trade/skip audit.

Phase 4.5B adds the persistent observability substrate needed for those metrics
before continuous paper trading.

## Model

`P4-RUNTIME-OBSERVABILITY-0001`

Fingerprint:

`0c16799f2587957ea8b73aaa919d5213f15426a306cd44d58b10d4e08637ee43`

## Persisted streams

The module adds four deterministic/idempotent tables to the paper runtime DB:

1. `paper_strategy_evaluations`
   - exact run/role/ingest keyed evaluations;
   - reason code;
   - passed/failed filters;
   - CandidateSignal identity when emitted;
   - canonical audit snapshot.

2. `paper_terminal_trade_decisions`
   - explicit terminal `TRADE` or `SKIP` decision;
   - exact decision reason / skip reason;
   - optional CandidateSignal / paper-entry lineage.

3. `paper_position_mtm_marks`
   - causal non-gap market marks per persisted paper position;
   - current market-price return vs simulated entry execution price;
   - exit curve impact using `P4-RUNTIME-EXIT-PRICE-IMPACT-0001`;
   - estimated current liquidation price/proceeds;
   - locked explicit estimated exit costs;
   - net MTM PnL.

4. `paper_track_equity_marks`
   - independent FINAL-A / FINAL-B / SENS-C equity-PnL streams;
   - realized closed PnL plus all currently open-position net MTM values;
   - enough information for causal peak-to-trough MTM max drawdown.

## Locked metric semantics introduced here

### MAE / MFE

For the long paper position:

`market_return = current causal market price / simulated entry execution price - 1`

MAE is the minimum persisted causal market return.
MFE is the maximum persisted causal market return.

### MTM PnL

The runtime MTM policy is conservative net immediate liquidation value at the
causal mark:

`net_mtm_pnl = estimated_gross_liquidation - entry_principal - entry_explicit_cost - estimated_exit_explicit_cost`

The estimated liquidation price includes current SOL_NATIVE curve impact from
the locked exit-impact provider. Exit latency is not applied to a mark-to-market
mark, and the exit slippage cap is not used as a mark rejection gate.

### Max drawdown

Each locked exit track has its own PnL-equity stream. Drawdown is calculated
peak-to-trough with a zero starting PnL anchor. Alternative exit tracks remain
strictly separate; cross-track portfolio summation is prohibited.

## Causality / determinism

- GAP_RECOVERY marks are excluded.
- pre-entry, cross-mint and price-identity mismatches are excluded.
- old ingest-sequence marks cannot rewind position state.
- exact replays are idempotent.
- same deterministic key with changed content raises a determinism conflict.
- restart reproduces the same canonical report digest.

## Local validation before delivery

The exact implementation was run locally against a byte-copy of the exact
Phase-4.4E execution fixture:

- foundation self-test: `RESULT: PASS`
- exact-schema integration smoke: `RESULT: PASS`

No production DB, collector, network, wallet, live orders, or parameter tuning
was used.

## Commands

Run the two already-prepared commands in order. The second should only run if
the first exits 0:

```powershell
python scripts\phase4_5b_runtime_observability_selftest_v0_1.py
if ($LASTEXITCODE -eq 0) {
    python scripts\phase4_5b_fixture_integration_smoke_v0_1.py
}
```

After both PASS, the next bounded step is live binding of this persistence to
the controlled Phase-4 runtime before the continuous paper runner is enabled.
