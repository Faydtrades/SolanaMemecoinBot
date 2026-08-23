# Phase 4.4E — Controlled Live Causal Entry → Parallel Exit-Fill Smoke v0.1

## Purpose

This is the first controlled live smoke that carries one fresh real-time
SOL_NATIVE paper trade through the complete execution boundary:

```text
live Pump market
→ locked FirstPullback v1.1 CandidateSignal
→ causal 500ms paper entry execution
→ 3 persisted OPEN positions
→ locked FINAL-A / FINAL-B / SENS-C exit orchestration
→ lifecycle EXIT_PENDING
→ causal 500ms exit execution
→ runtime SOL_NATIVE exit price impact
→ PaperCostModelV02 exit slippage/cost decision
→ CLOSED
```

The smoke stops only when all three locked tracks are CLOSED, or when a bounded
timeout is reached.

## Locked strategy / exit scope

No parameter search or reselection is allowed.

- FINAL-A: `E3_TP10_T15`
- FINAL-B: `E4_ACT10_GB03_T15`
- SENS-C: `SENS_TP20_T5` (sensitivity only)

Locked exit-spec fingerprint:

`0bde5378f4ff2b63c6a96ad64140840ef8b2770c3be772a7aa71f8bdeb070129`

## Execution models

Entry:
- P4-COST-BASELINE-0001
- 500ms entry latency
- 15% entry slippage rejection cap
- P4-RUNTIME-PRICE-IMPACT-0001

Exit:
- same P4-COST-BASELINE-0001
- 500ms exit latency
- 20% exit slippage rejection cap
- P4-RUNTIME-EXIT-PRICE-IMPACT-0001

No fixed price-impact haircut is used.

A rejected exit attempt remains audited and EXIT_PENDING; later causal
observations may retry the same immutable exit intent.

## Causality

Only mints with a LAUNCH observed after the smoke start cursor are eligible.

The entry fill uses the first qualifying same-mint SOL_NATIVE observation at or
after entry execution-ready time.

Each exit track receives observations in production ingest order. Once a
locked ExitIntent is persisted, the exit executor uses the first qualifying
post-ready observation; it does not cherry-pick a better later price.

Fallback intents are still anchored to the exact locked Phase-3 signal clock.
The fallback timer itself never fabricates a fill price.

## Safety

- production DB sidecar: query-only/read-only;
- validated v0.3.4 collector: normal market-data collection only;
- all simulated orders/positions/fills: fresh isolated paper SQLite;
- wallet: none;
- signing: none;
- live order: none;
- PnL/accounting: intentionally not performed in this step.

## Runtime

Default:
- up to 900s to acquire one accepted SOL_NATIVE entry;
- then up to 60s to close all three exit tracks.

The collector starts/stops automatically. No Ctrl+C is required.

Run:

```powershell
python scripts\phase4_4e_live_entry_exit_smoke_v0_1.py
```

`RESULT: CHECK` because of no signal, no accepted entry, or incomplete causal
exit fill is observational. Do not loosen parameters or invent prices.

## Local validation before delivery

The packaged Phase-4 entry/lifecycle/exit integration was run on an isolated
fixture before delivery:

```text
RESULT: PASS
```

The fixture validated:
- one FILLED entry with three positions;
- SENS-C 5s fallback close;
- FINAL-A +10% TP close after SENS-C is already CLOSED;
- FINAL-B +10% activation, peak, exact 300bps giveback and trail close;
- all three independent CLOSED states;
- persisted FILLED exit routes;
- terminal replay canonical-digest stability;
- SQLite quick_check.

The actual live Phase-2/collector path still requires final validation in the
user's project, because the local test environment does not contain the user's
production SQLite or Solana collector feed.
