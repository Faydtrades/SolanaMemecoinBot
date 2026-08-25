# Phase 4 Production Fence-Drain Throughput v0.1

## Status

`IMPLEMENTED_PENDING_PROJECT_REVIEW`

Binding model: `P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0005`

Binding fingerprint:
`f66acb5926f3dd77d68c086d74e1bc07b43fad4307071c1ec8c3b7d1111660b7`

Multi-hour model: `P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0005`

Multi-hour fingerprint:
`7e439eb90ca0d44467501ae15008ecc4c733582dc14a04afae9a94ac9ab9f3ca`

## Failed V0.4 production evidence

The preserved interrupted V0.4 run had a final frozen watermark of `635181`
and advanced only 125 durable P1 rowids in 60 seconds during final drain,
approximately 2.08 rowids/second. At that rate its remaining 6,470 rowids
would have required approximately another 51.8 minutes.

This failed run remains immutable evidence. It was not resumed or modified by
T016.

## Measured root cause

T016 opened the production source read-only and profiled a copy of the
preserved paper database. No production or preserved runtime database was
written.

On the copied state, one accepted V0.4 batch advanced from durable cursor
`628850` to `628975`:

- raw rows: 125;
- normalized rows: 44;
- deterministic skips: 81;
- elapsed: 8.9082245 seconds;
- throughput: 14.03198 raw rows/second;
- SQLite connection-context finalization: 7.573 seconds, 85.0% of elapsed;
- source fetch: 0.0347168 seconds, 0.4% of elapsed;
- hydration: 0.4313847 seconds, 4.8% of elapsed; and
- strategy planning: 1.0786758 seconds, 12.1% of elapsed.

Component timers overlap, so their percentages must not be summed. The
dominant measured operation was repeated FULL-synchronous SQLite transaction
finalization, not `Path.resolve()`, source connection creation, hydration, or
the source query shape. V0.4 committed multiple independently durable scopes
for every raw source row: input planning, runner work, outbox delivery, and
cursor completion. The live collector and paper database shared the same
storage environment, which explains why the real concurrent run was slower
than an isolated copy and why the skip-heavy T015 fixture missed the class of
failure.

## Minimal fix

V0.5 retains WAL and `synchronous=FULL`, but makes all paper writes for one
fetched source batch one SQLite transaction. Existing inner Phase-4 commit
scopes are deferred until the source-batch boundary.

On success, semantic work and the final batch cursor become durable together.
On any exception or commit failure, the entire batch rolls back to the prior
durable cursor. The failed binding instance becomes terminal; a fresh reopen
reconstructs from SQLite and deterministically replays the rolled-back rows.

This changes commit frequency only. It does not skip, aggregate, reorder, or
reinterpret source records or strategy work.

## Deterministic production-shaped regression

The regression uses 250 raw rows with the accepted T015 production mix, real
normalization, hydration, strategy evaluation/audit, semantic outbox delivery,
runner work, cursor persistence, and an exact fixed-watermark timer. It adds a
deterministic 25ms durable-commit delay to reproduce the measured storage-bound
failure class without a live run.

Latest acceptance-run evidence:

- V0.4: 700 physical commit boundaries, 19.1036638 seconds, 13.08649 raw
  rows/second;
- V0.5: one source-batch commit and six total boundaries including timer
  finalization, 0.4715909 seconds, 530.12049 raw rows/second;
- locked minimum: 15 raw rows/second;
- final durable cursor: exactly 250;
- frozen watermark: exactly 250;
- rows 251 through 260: not admitted before timer completion;
- V0.4/V0.5 semantic rows: byte-equivalent after excluding version and wall
  clock metadata;
- SQLite `quick_check`: `ok`; and
- exact reopen database SHA-256: unchanged.

The matrix also covers a mid-batch semantic failure, a physical commit failure,
a second-batch failure after a first durable batch, terminal-instance rejection,
whole-batch rollback, restart replay, timer idempotency, duplicate protection,
ordered source identity, and exact release of rows above the completed
watermark.

## Preserved invariants

- production source remains fresh SQLite `mode=ro` plus `query_only`;
- V0.4 fixed-watermark and same-watermark timer ordering is inherited
  unchanged;
- rows above the frozen watermark remain fenced;
- no future market data enters semantic state;
- durable cursor, semantic outbox, evaluations, and runner effects commit
  atomically per source batch;
- restart and replay remain deterministic and idempotent;
- `FINAL-A`, `FINAL-B`, and `SENS-C` remain separate alternatives;
- entry deadlines, exit clocks, 500ms latencies, costs, impact, trade size,
  parameters, and finalist selections remain unchanged; and
- execution remains paper-only.

## Validation boundary

The failed V0.4 production run and its artifacts remain evidence only. T016
performed bounded read-only profiling plus isolated deterministic tests. It did
not start a collector, access RPC/network, run a wallet, place an order, resume
the failed session, or start another one-hour/four-hour production session.

V0.5 requires separate ChatGPT project review before a new production
validation run.

NO NEW PRODUCTION/LIVE RUN PERFORMED.
