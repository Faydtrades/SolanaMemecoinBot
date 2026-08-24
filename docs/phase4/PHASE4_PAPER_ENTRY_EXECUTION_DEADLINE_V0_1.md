# Phase 4 Paper Entry Execution Deadline v0.1

Status: `IMPLEMENTED_PENDING_PROJECT_REVIEW`

Model: `P4-PAPER-ENTRY-EXECUTION-DEADLINE-0001`

Fingerprint:
`6e2ae931d7270012dc025ef9a509ec30a240a850ea0d1c2666560a5e5506c9a7`

## Failure corrected

The accepted multi-hour v0.1 harness exposed a causal execution defect during
its first bounded production attempt. A genuine CandidateSignal at
`2026-08-24T20:28:07.275108Z` was not filled until approximately 36.512
seconds later, at `2026-08-24T20:28:43.787124Z`. The old shared entry route
opened FINAL-A, FINAL-B, and SENS-C even though all signal-anchored fallback
deadlines were already past. Exit handling then attempted an effective
transition before the positions' OPEN transition.

`PaperLifecycleStore` rejected that transition with `InvalidTransition:
effective_at precedes prior position transition`. That rejection was correct:
the lifecycle must never rewrite causal history to accommodate an impossible
timestamp ordering. The failed v0.1 run remains FAIL evidence and its runtime
SQLite, JSON, and logs are not modified by this task.

## Locked architecture

Fallback and threshold semantics remain the accepted Phase-3 definitions and
remain anchored to `candidate.signal_observed_at`. Moving a fallback to entry
fill, or clamping it forward to `fill_at`, would silently extend a locked exit
window and constitute a research-semantic change.

Entry viability is instead derived from each locked exit clock:

| Track | Last eligible entry fill | Expiry instant |
|---|---:|---:|
| FINAL-A | signal + 15,000,000us | signal + 15,000,001us |
| FINAL-B | signal + 15,000,000us | signal + 15,000,001us |
| SENS-C | signal + 5,000,000us | signal + 5,000,001us |

The deadline is inclusive. A fill exactly at +5s or +15s is eligible; the
corresponding deadline becomes expired exactly one microsecond later. This is
execution viability derived from the locked exit contract, not a strategy
parameter and not a tunable value.

The policy fingerprint binds the model/schema identity, all three integer
microsecond windows, inclusive-boundary semantics, the +1us expiry rule, and
locked exit-spec fingerprint
`0bde5378f4ff2b63c6a96ad64140840ef8b2770c3be772a7aa71f8bdeb070129`.

## Route and lifecycle semantics

- CandidateSignal remains a strategy output and remains visible in strategy
  audit even when execution later expires.
- Each candidate still creates one durable order per locked track.
- Existing 500ms entry latency, mint, gap-recovery, price-identity, impact,
  and slippage checks remain in force.
- `ENTRY_EXECUTION_DEADLINE_EXPIRED` is an execution rejection. It never uses
  the slippage reason and never rewrites a strategy evaluation as a skip.
- An expired order is never passed to `fill_order`, creates no position,
  incurs no entry principal or explicit entry cost, and contributes no MTM,
  MAE/MFE, exit, completed-trade, or PnL row.
- SENS-C expires independently. A fill after +5s and through +15s opens only
  FINAL-A and FINAL-B; the shared route is FILLED because execution occurred.
- If all tracks expire, the route is REJECTED with
  `ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED`.
- If SENS-C has expired and A/B later fail the locked slippage check, SENS-C
  retains its deadline reason, A/B retain the real slippage reason, and the
  no-fill route is REJECTED.
- FINAL-A, FINAL-B, and SENS-C remain alternative tracks and are never summed.
  SENS-C remains `SENSITIVITY ONLY`.

## Clock, watermark, and restart behavior

Silent markets are handled by the accepted ClockTick path. A late clock writes
the canonical deadline +1us timestamp, not its later wall-clock timestamp, and
does not fabricate a market point, price, or fill. The source-watermarked
prepared-timer fence is unchanged: all production rows through the captured
watermark must drain before the timer event can expire an order. Exact-boundary
source observations therefore retain precedence and eligibility.

`paper_entry_execution_deadlines_v0_1` is a narrow companion audit table. It
persists route/order/candidate/track lineage, signal time, last eligible fill,
canonical expiry, terminal outcome/reason, market/fill provenance, and the
route-level explicit entry cost needed for deterministic terminal recovery. A
singleton metadata row binds the policy ID and fingerprint. Its composite
route/track identity prevents duplicate audit rows.

Restart reconstructs pending, filled, and rejected tracks independently. A
restart with SENS-C expired and A/B pending can still accept an A/B fill through
+15s; SENS-C cannot resurrect. A restart after all-track expiry can never fill
that candidate. If a process stops after the final terminal track write but
before the shared-route write, reopen validates the lifecycle order/position
state and heals only the uniquely implied route outcome. Conflicting terminal
state or provenance fails closed rather than being reclassified.

## Entry and exit boundary ordering

Price observations before actual OPEN do not enter exit state. A track filled
exactly at its fallback boundary can OPEN at T. A later-ingest observation at
the same T can create a locked exit intent at T without timestamp regression,
while exit execution remains WAITING until the accepted T+500ms latency and a
causal post-ready market observation.

## Bounded-run end

The final prepared timer uses the actual frozen duration instant. Deadlines at
or before that instant are processed; deadlines after it remain auditable as
ENTRY_PENDING. The runner never advances a synthetic clock beyond the bound,
fabricates an entry/exit, or forces an otherwise causal open position closed.

## Scope and claims

This task used isolated SQLite fixtures only. It performed no production/live
validation, accessed no production database, used no network/RPC, started no
collector, and implemented no wallet, signing, or live order path. It made no
profitability claim and performed no parameter tuning or reselection.
