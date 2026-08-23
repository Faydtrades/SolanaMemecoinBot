# Phase 4.5A — Deterministic Paper Trade Accounting Foundation v0.1

## Scope

This bounded step derives deterministic accounting from persisted Phase-4
entry/exit fills without mutating execution state.

Per completed `signal_key × track_id` lifecycle it calculates:

- gross execution PnL;
- entry explicit costs;
- exit explicit costs;
- realistic net PnL;
- net return in bps / percent;
- holding time;
- WIN / LOSS / BREAKEVEN;
- entry/exit price-impact and adverse-slippage audit.

Per locked exit track it calculates:

- completed trades;
- wins / losses / breakevens;
- total net PnL;
- net expectancy;
- average net return;
- winrate;
- average winner / loser;
- net Profit Factor;
- realized closed-equity drawdown diagnostic;
- average / median holding time;
- explicit execution costs.

## Critical accounting rule

FINAL-A, FINAL-B and SENS-C are alternative paper exit variants evaluated from
the same CandidateSignal. They MUST NOT be summed into one portfolio PnL.

Each track is reported independently.

## Net PnL definition

For one completed track:

```text
net_pnl =
    gross_exit_proceeds
    - filled_entry_principal
    - entry_explicit_cost
    - exit_explicit_cost
```

Entry and exit price impact / latency are already embedded in the simulated
execution prices, so they are not charged again as a separate haircut.

## What 4.5A deliberately does NOT claim

The locked project metric is **mark-to-market max drawdown**.

A completed-fill database alone cannot reconstruct a causal MTM equity curve,
MAE or MFE. Therefore 4.5A explicitly reports those as unavailable rather than
silently substituting closed-trade drawdown.

Likewise, the paper execution DB can audit execution rejections, but it does
not contain the full strategy token trade/skip decision stream. Strategy skip
audit remains explicitly unavailable until that stream is persisted by the
continuous runtime.

## Local validation before delivery

The exact implementation was run against the packaged Phase-4.4E fixture and
produced:

`RESULT: PASS`

The packaged read-only postrun accounting smoke was also run against the same
fixture and produced:

`RESULT: PASS`

## First command

```powershell
python scripts\phase4_5a_trade_accounting_selftest_v0_1.py
```

After that passes, the same package already contains the read-only script for
the latest real Phase-4.4E paper DB:

```powershell
python scripts\phase4_5a_live_postrun_accounting_smoke_v0_1.py
```
