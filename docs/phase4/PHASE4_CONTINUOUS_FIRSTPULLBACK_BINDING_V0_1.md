# Phase 4 Continuous FirstPullback Binding Foundation v0.1

## Status

BOUNDED ISOLATED FIRSTPULLBACK BINDING:
IMPLEMENTED_PENDING_PROJECT_REVIEW

PRODUCTION FIRSTPULLBACK PAPER TRADING:
NOT YET VALIDATED

UNBOUNDED CONTINUOUS LIVE PAPER:
NOT YET VALIDATED

## Binding identity

- Model: `P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0001`
- Fingerprint: `318e15b104c691821f5715b090c934e5b1cb3d9a8357fe0930d8825ef66d0daa`
- Accepted market source: `P4-CONTINUOUS-MARKET-SOURCE-0001`
- Market-source fingerprint: `242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb`
- Gap compatibility fingerprint: `aabb765f81a29b2dd119607cdea92a2949dd3acec10aa204d246e6aa7a28a78c`
- Accepted continuous runner: `P4-CONTINUOUS-PAPER-RUNNER-0001`
- Runner fingerprint: `8dcab9d17f2ec71f4920a199b5745dae92dc3464a466860593231424e0a24dea`

The fingerprint also binds `DenominationAwareFeatureEngineV01`
(`MS-DAF-0.1`), `FirstPullbackStrategyV02` (`v1.1`),
`FirstPullbackStrategyClockV02` (`SCLOCK-0.2`), the locked selection SHA-256,
the complete parameter values, causal ordering, full evaluation-audit contract,
cursor/outbox contract, and source-watermarked timer preparation contract.
It additionally binds the rule that every incomplete prepared timer establishes
a production-consumption fence until every timer at the earliest watermark is
complete.

## Exact strategy contract

Roles are evaluated in this exact order:

1. `CONTROL` — `FP1-EXP0005-D-005-CONTROL-R500-T2-U1-PD1000-9000-RB200-BU1-FL0-RC200-RW10000`
2. `ROBUST_1` — `FP1-EXP0005-D-026-ROBUST_1-R1500-T5-U9-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500`
3. `ROBUST_2` — `FP1-EXP0005-D-050-ROBUST_2-R2000-T3-U5-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500`
4. `ROBUST_3` — `FP1-EXP0005-D-074-ROBUST_3-R5000-T3-U2-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500`

The first fresh `SOL_NATIVE` candidate in role order wins that mint lifecycle.
Later roles are not evaluated on the winning observation, matching the accepted
Phase-4.5C arbitration. Other mints remain independent. The binding translates
the Phase-2 `ENTRY_LONG` candidate into the runner's locked
`FIRST_PULLBACK` evaluation classification; it does not alter the Phase-2
candidate object or strategy state machine.

## Market-source and exact event hydration

The accepted market-source projection deliberately contains the fields needed
for paper price observations, but not all wallet and raw-flow fields used by the
denomination-aware feature engine. For every accepted record, the binding makes
one bounded `mode=ro`, `query_only=ON` lookup by exact `pump_events.rowid`, then
passes that row through `Phase1GapSourceCompatibilityV01`. It asserts that the
resulting ingest identity, event identity, mint, type, timestamps, price path,
reserves, source, and gap provenance exactly match the accepted market-source
record before strategy processing. No alternative normalizer is introduced and
neither Phase 2 nor the accepted source adapter is modified.

## Production cursor and deterministic skips

The binding owns `last_durable_p1_rowid`; the market source remains stateless.
Each fetched row becomes a versioned binding input. A normalized record is
durable only after its complete semantic outbox has been accepted by the
continuous runner. A deterministic source skip has an empty outbox and can be
consumed directly. This allows a skip-only fetched prefix to advance without
being fetched forever.

Cursor advancement is prefix-only. An unfinished row prevents advancement to
later rows. Fetching, normalization, and strategy-state persistence alone do not
advance the production cursor.

An incomplete prepared timer also bounds this prefix. The binding derives the
minimum watermark across all `PREPARED` and `PLANNED` timers directly from paper
SQLite on every production-processing call. It may consume rows only through
that watermark. A requested batch size cannot bypass the bound; any source rows
read above it remain entirely unprocessed and are fetched again after release.
No synthetic skip or cursor advancement is created for a fenced row.

## Full strategy-evaluation audit

Every actual `StrategyEvaluation` returned by a market-row evaluation or an
explicit strategy-clock expiry is represented in the accepted
`paper_strategy_evaluations` table. This includes no-transition waits,
intermediate transitions, candidates, and terminal `INVALIDATED`, `REJECT`, or
`EXPIRED` evaluations. The accepted run/role/mint/version/parameter identity,
ingest identity, evaluation time, current state, transition flag, reason,
passed/failed filters, signal/candidate identity, and audit snapshot are retained.

A binding-owned evaluation-audit ledger is created atomically with persisted
strategy state and the semantic runner outbox. The ledger contains the exact
accepted audit payload and fingerprint. Delivery calls the accepted
`PaperRuntimeObservabilityV01.record_strategy_evaluation()` boundary. If a crash
occurs after that store commits but before the ledger acknowledges delivery,
restart replays the durable audit payload—not the historical strategy input—and
the accepted store verifies the same deterministic evaluation identity/content.

Terminal TRADE/SKIP decisions remain separate records. Candidate and terminal
runner events can idempotently revisit the already-recorded evaluation, while
ordinary nonterminal evaluation history remains fully auditable without creating
terminal decisions.

## Event sequence and replay

`paper_fp_binding_outbox_v0_1` assigns a strictly increasing binding-local
integer to every runner item. The immutable item binds its sequence, event key,
event type, payload, and SHA-256 fingerprint. Multiple actions from one
production row therefore cannot collide, and explicit clock events fit between
market rows without borrowing a production rowid.

The runner may commit before the binding marks an outbox item delivered. On
restart, the exact item is replayed at the same runner cursor; the accepted
runner returns `ALREADY_COMMITTED` only when the content fingerprint matches.
The binding then completes the outbox and production cursor. Arbitrary older
cursor replays remain prohibited.

## Same-row causal order

For one normalized row, semantic strategy terminal actions are sequenced first
in locked role order. The `MarketObservationEvent` is sequenced last. Thus a
candidate route exists before that row's observation reaches the runner, while
the runner's strict `ingest_seq > signal_ingest_seq` rule prevents a candidate
from filling on its own signal row. Existing open positions still receive the
observation after strategy processing. This locks the Phase-4.5C entry-before-
later-market-observation causal behavior while supporting multiple paper
signals.

## Restart state

The paper SQLite database stores:

- binding, market-source, runner, fingerprint, anchor, and timestamp identity;
- last durable production rowid and next binding event sequence;
- exact accepted normalized feature inputs;
- every per-mint/per-role mutable `StrategyRunState` and clock deadline;
- the winning candidate identity; and
- immutable evaluation-ledger, input/outbox, prepared-timer, and delivery state.

Active timer fences are reconstructed from those prepared-timer rows rather
than in-memory state. If restart finds an incomplete timer whose watermark is
already below the durable production cursor, causal order has been violated and
initialization fails closed.

On restart, the accepted feature engine is reconstructed by replaying only the
persisted normalized feature inputs in production row order. Mutable strategy
runs and deadlines are restored from their exact persisted fields, not
re-evaluated. Paper order, position, exit, mark, decision, and accounting state
continues to be reconstructed by the accepted continuous runner.

## Source-watermarked strategy and exit clocks

Timer creation is a two-step prepare/execute contract. Preparation asks the
accepted read-only market source for its current maximum production `p1_rowid`
and freezes that binding-owned watermark with the timer identity, clock type,
timezone-aware timestamp, `drive_exit_clock` meaning, binding fingerprint, and
content fingerprint. The caller cannot supply the watermark.

Execution remains `PENDING` and performs no strategy mutation or runner clock
delivery while `last_durable_p1_rowid` is below the frozen watermark. Once every
source row through that watermark has completed its evaluation ledger and runner
outbox, the timer may execute. The exact production order is:

```text
capture watermark N
-> source may receive N+1...
-> binding consumes only through N
-> all timers at fence N complete
-> binding may consume N+1...
```

Rows arriving after the frozen watermark therefore do not delay timer execution,
but they cannot overtake it. This is enforced inside `process_next_batch()` for
all batch sizes rather than delegated to caller behavior.

When multiple timers exist, the earliest incomplete watermark is authoritative.
Every timer at that watermark must become `COMPLETE` before the next production
row is admitted. A later prepared timer then becomes the next fence. `PLANNED`
timers—including a timer interrupted after partial audit or runner delivery—
continue to fence production through restart. Only `COMPLETE` timers release
their fence.

Strategy expiry retains the accepted exact boundary: `t0 + 300000ms` remains
eligible and expiry occurs at the first microsecond after it. Clock evaluations
use a deterministic timer-specific audit-ingest namespace because the accepted
observability identity is `(run, role, ingest_seq)` and a clock can evaluate
without a new market ingest sequence.

The exit clock remains a separate runner `ClockTickEvent`. A single prepared
timestamp may drive strategy items first and one exit item second, or a separately
prepared exit-only tick may be used. Prepared watermark/timestamp/type/drive
meaning survives restart, conflicting replays fail closed, and replay never
resamples either source watermark or wall clock. No background thread is created.

## Gap semantics

Only a fresh non-gap launch after the explicit session anchor can establish
eligibility. `GAP_RECONCILIATION_V0_3_4` is accepted only through the exact
compatibility alias and remains `GAP_RECOVERY`. A gap row is retained by the
accepted feature engine for causal/audit history, but is excluded by that engine
from fresh short-window flow and produces an invalid triggering strategy state.
It may still reach paper lifecycle observation with `is_gap_recovery=true`;
the runner excludes it from fresh MTM/equity marks. Unknown future gap labels
remain unsupported.

## Isolated validation and performance limitation

The deterministic self-test uses only temporary source and paper SQLite files.
It covers two overlapping candidates, a separate timer-expired mint, all three
locked exit siblings, injected restart boundaries, gap handling, accounting,
SQLite integrity, large-batch strategy and exit fence enforcement, same- and
later-watermark timer ordering, `PLANNED` delivery failure, fail-closed historical
cursor corruption, and identical complete-run digests. The failure/restart run
made 28 exact reconstruction queries across injected restarts. Each newly
planned normalized record still requires exactly one bounded hydration query;
already-durable audit/outbox retries do not rehydrate or re-evaluate it.

The accepted market source's `_eligible_launches_through()` growing launch-prefix
rescan remains unchanged. The per-record exact hydration query is an additional
known bounded cost. Both must be evaluated before an unbounded live paper run;
neither was optimized by semantic shortcut in this task.
