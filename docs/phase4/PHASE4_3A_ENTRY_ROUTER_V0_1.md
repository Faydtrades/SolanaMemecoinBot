# Phase 4.3A — CandidateSignal -> Simulated Entry Routing Foundation v0.1

Scope is deliberately bounded.

This sub-step connects the already-locked Phase-2 `CandidateSignal` boundary to
Phase-4 paper execution mechanics without introducing a wallet, live order,
network dependency, exit logic, PnL accounting, or new parameter search.

## Locked behavior

- CandidateSignal remains a strategy output, not an order.
- One candidate is routed to the three Phase-4 tracks:
  - FINAL-A / `E3_TP10_T15`
  - FINAL-B / `E4_ACT10_GB03_T15`
  - SENS-C / `SENS_TP20_T5`
- All three tracks use one shared entry decision so FINAL-A and FINAL-B are
  paired on the same candidate/fill whenever possible.
- `P4-COST-BASELINE-0001` supplies the entry latency/cost/slippage rules.
- A market observation earlier than `execution_ready_at` cannot fill.
- GAP_RECOVERY cannot masquerade as fresh entry data.
- Cross-mint or price-identity mismatches cannot drive a fill.
- Entry slippage cap rejection applies consistently to all three tracks.
- Pending routes and resulting lifecycle state survive SQLite close/reopen.
- Replays are idempotent; conflicting terminal replay is detected.

## Explicit non-goals

Phase 4.3A does NOT:

- run the live collector/strategy engine,
- create CandidateSignals from live market traffic,
- call RPC/network services,
- use a wallet or broadcast orders,
- implement FINAL-A/B/SENS-C exits,
- compute strategy PnL/PF/drawdown/expectancy,
- tune DEV-FREEZE-0002.

The next bounded sub-step after a local PASS is Phase 4.3B: actual live
CandidateSignal source integration and a controlled live-smoke test, still with
no wallet and no real orders.
