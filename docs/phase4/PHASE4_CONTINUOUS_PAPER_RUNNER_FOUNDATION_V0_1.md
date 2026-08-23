# Phase 4 Continuous Paper Runner Foundation v0.1

## Scope

This foundation provides a paper-only, restart-safe orchestration core over the
accepted Phase-4.5C execution and observability stack. It does not start a
collector, access RPC/WebSocket services, open the production database, or run
an unbounded live session.

LIVE CONTINUOUS PAPER RUN: **NOT YET VALIDATED**

## Runner identity

- Model ID: `P4-CONTINUOUS-PAPER-RUNNER-0001`
- Schema: `phase4_continuous_paper_runner_v0.1`
- Engine: `ContinuousPaperRunnerV01`
- Spec fingerprint:
  `8dcab9d17f2ec71f4920a199b5745dae92dc3464a466860593231424e0a24dea`

The fingerprint canonically binds:

- FirstPullback v1.1 and the immutable EXP-0005-D locked selection digest;
- locked strategy role order `CONTROL`, `ROBUST_1`, `ROBUST_2`, `ROBUST_3`;
- the exact accepted parameter-set ID bound to each locked strategy role;
- `P4-COST-BASELINE-0001` and its fingerprint;
- reference size, entry/exit latency, and entry/exit slippage caps;
- entry impact model and fingerprint;
- locked exit specification and exit-impact fingerprint;
- observability model/fingerprint and live observability binding;
- `FINAL-A`, `FINAL-B`, and `SENS-C` as separate alternative tracks;
- at-least-once, monotonically increasing sparse cursor delivery with cursor
  commit after durable processing and fail-closed cursor regression;
- explicit timezone-aware clock ticks with replay-stable semantic time.

Runtime construction fails closed if any imported locked Phase-4 contract has
drifted. Restart also fails closed if the persisted model, fingerprint, or
source identity differs.

## Persistent schema

`paper_continuous_runtime_state_v0_1` contains one bound runtime row:

- runner model ID;
- runner spec fingerprint;
- source identity;
- last successfully committed source cursor;
- last source event key and content fingerprint;
- creation and update timestamps.

`paper_continuous_signal_contexts_v0_1` persists the strategy evaluation,
candidate, entry-route, role, parameter-set, and source lineage needed to
finish terminal TRADE/SKIP audit after restart.

All orders, positions, exit tracks, trailing peaks, fallback deadlines, exit
intents, execution attempts, fills, MTM marks, equity marks, and accounting
facts remain owned by the already accepted Phase-4 tables.

## Cursor and crash-safety contract

Source items are processed at least once. The runner dispatches one item
through the existing idempotent stores and advances its source cursor only
after dispatch returns successfully. An exception before cursor commit is
propagated and leaves the cursor at the last completed item.

The source item content fingerprint is computed and validated exactly once
before durable dispatch. That same value is persisted with signal context and
reused for the cursor commit, so processing cannot observe two different
fingerprints for one source item.

On retry, existing deterministic IDs and unique constraints replay the durable
strategy evaluation, entry route, three orders/positions, exit state, attempts,
marks, and decisions without duplication. The runner also heals the narrow
entry-route-to-terminal-audit boundary if a process stops between those writes.
A replay of the already committed cursor is a no-op and conflicting content at
that cursor fails closed. New cursors must be delivered monotonically in
ascending order. Sparse increasing values are allowed for sources such as
database rowids. A lower cursor fails closed because the minimal state cannot
prove that an arbitrary historical item was processed.

## Source fingerprint canonicalization

Source-item fingerprints recursively canonicalize dataclasses, enums,
timestamps, mappings, and sequences. Finite `Decimal` values use their exact
base-10 `str(value)` text, preserving trailing zeroes without a float
round-trip. `Decimal` NaN and positive/negative infinity fail closed with
`ValueError`.

Before either an accepted candidate or a terminal skip is handed to
`PaperRuntimeObservabilityV01.record_strategy_evaluation`, its audit snapshot
is recursively normalized by the same primitive conversion. Finite Decimal
values therefore reach JSON persistence as exact base-10 strings, including
trailing zeroes. A non-finite Decimal fails before an evaluation row or cursor
commit is written.

## Restart contract

SQLite is the lifecycle authority. Construction queries persisted positions,
rebinds every signal to all three exit tracks, recovers staged exit fills via
the accepted executor, and rebinds execution routes for `EXIT_PENDING` and
`CLOSED` positions. It does not reopen closed positions or recreate filled
entries.

Mixed sibling state is supported. A signal can have a closed `FINAL-A`, an
`EXIT_PENDING` `SENS-C`, and an open/trailing `FINAL-B`; closed siblings remain
terminal and cannot block live siblings.

## Multi-signal handling

There is no new concurrency or maximum-position limit. Candidate events create
independent persistent signal lineages. Each accepted signal fans out through
the existing entry router to exactly `FINAL-A`, `FINAL-B`, and `SENS-C`.
Market observations are routed by mint to every active signal and each signal
retains independent entry, exit, trailing, fallback, MTM, and accounting state.

## Event-source and strategy boundary

The core consumes an injected `ContinuousEventSource`; it does not require a
live source. Candidate and terminal-skip items carry the accepted observability
audit input produced by the FirstPullback pipeline.

`run()` returns a lazy iterator and yields one `ProcessResult` per consumed
source item. It does not materialize or retain the source or result history, so
an indefinite source can be processed with bounded runner memory.

The later production read-only adapter must preserve the Phase-4.5C semantics:
evaluate roles in locked order and emit only the first eligible candidate for a
mint/source observation. The runner does not arbitrate candidates or create a
new winner among the four roles.

## Market-time and wall-clock boundary

Market events retain their source observation timestamp and ingest sequence.
Those values drive causal entry eligibility, exit progression, fills, and
marks. Clock ticks are separate events and require an explicit timezone-aware
`now` timestamp at construction. That timestamp is part of the source-item
fingerprint and is the only semantic time used to process or replay the tick;
the runner never resamples its injectable wall clock for tick semantics. A
fallback clock can persist an exit intent, but only a later causal market
observation can produce an exit fill. A future source adapter must stamp each
clock tick once when constructing the event.

## Protected locks

The implementation does not change costs, latency, slippage caps, impact
models, exit definitions, strategy parameters, research freezes, or candidate
selection. It never sums the three alternative exit tracks into one portfolio.
No wallet, signing, or live-order path exists.

## Deterministic validation

`scripts/phase4_continuous_paper_runner_selftest_v0_1.py` uses temporary SQLite
databases and synthetic source items. It proves:

- two overlapping accepted signals for two mints;
- exactly three locked tracks per signal and one explicit terminal skip;
- independent market and fallback exit progress;
- restart with `OPEN` and `EXIT_PENDING` positions;
- no duplicate orders, positions, intents, fills, or marks after retry;
- cursor retention after an injected pre-commit failure;
- exact-once durable completion on retry;
- closed siblings do not block remaining siblings;
- conflicting runtime fingerprints fail closed;
- per-track MTM/MAE/MFE and final realized accounting reconciliation;
- prohibited cross-track aggregation;
- SQLite `quick_check=ok`;
- identical report and runner digests across repeated runs.
- nested Decimal fingerprint stability, trailing-zero preservation, and
  non-finite fail-closed behavior;
- lazy one-item-at-a-time source consumption without result accumulation;
- exact last-cursor replay, conflicting replay rejection, unseen lower-cursor
  rejection, and sparse increasing cursor acceptance;
- exact Decimal JSON persistence for accepted candidates and terminal skips,
  plus atomic rejection of non-finite values;
- mandatory aware clock timestamps and stable crash/retry behavior even when
  the injected wall clock advances between attempts.

The accepted Phase-4.5A, 4.5B, and 4.5C deterministic regressions are also
required for review of this foundation.

## Explicitly not validated

- a production read-only source adapter;
- live collector integration;
- RPC or WebSocket behavior;
- a continuous live paper session;
- operational concurrency limits or backpressure policy;
- runtime supervision, deployment, or dashboards;
- wallet, signing, or live execution.
