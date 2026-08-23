# Phase 4.3B1D2 — Causal SOL_NATIVE Paper Entry Execution Smoke v0.1

## Goal

Validate the first full Phase-4 paper-entry execution path:

```text
live market
→ locked FirstPullback v1.1 CandidateSignal
→ shared ENTRY_PENDING route
→ locked 500ms latency
→ first valid post-ready live observation
→ runtime SOL price impact
→ cost/slippage decision
→ FILLED or REJECTED
```

The smoke continues unchanged after a valid slippage rejection until an actual
accepted SOL_NATIVE fill occurs or the hard runtime limit is reached.

## Causality rules

The fill observation must be:

- same mint as the CandidateSignal;
- same `SOL_NATIVE` price identity;
- non-gap-recovery;
- strictly later in ingest sequence than the signal event;
- observed at or after `execution_ready_at`;
- the first qualifying observation the runner encounters.

No later observation is cherry-picked if the first qualifying observation is
unfavorable.

## Price impact

Uses the already self-tested:

`P4-RUNTIME-PRICE-IMPACT-0001`

For SOL_NATIVE:

```text
impact_bps = ceil(
    requested_size_lamports * 10_000
    / current_virtual_sol_reserve_lamports
)
```

The reserve input comes from the exact post-ready market observation, which is
the market state immediately before the hypothetical paper order.

Fees stay separate in `PaperCostModelV02`.

## Quote policy

QUOTE CandidateSignals are audited but are not eligible for this B1D2 fill.
The system still refuses to invent a SOL→quote conversion.

## Safety

- production DB is sidecar query-only/read-only;
- validated collector writes only normal market data;
- all paper orders/fills live in a fresh isolated paper SQLite DB;
- no wallet;
- no signing;
- no transaction broadcast;
- no live orders;
- no parameter tuning/reselection.

## Runtime

Default hard limit: 15 minutes.

The collector is started/stopped automatically.

Run:

```powershell
python scripts\phase4_3b1d2_causal_sol_entry_fill_smoke_v0_1.py
```

A timeout with no SOL_NATIVE candidate or no accepted fill is `RESULT: CHECK`,
not permission to loosen strategy or slippage parameters.
