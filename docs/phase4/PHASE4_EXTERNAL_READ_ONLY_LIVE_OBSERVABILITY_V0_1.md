# Phase-4 External Read-Only Live Observability Contract v0.1

## Status and scope

`MEME-P4-T017` implements a producer-owned, externally read-only contract for
the accepted continuous FirstPullback paper runtime. It does not provide a
dashboard, a control API, an execution API, or a trading input.

Implementation status is `IMPLEMENTED_PENDING_PROJECT_REVIEW`. Project review
remains the acceptance authority.

## Identities

- Contract model: `P4-EXTERNAL-LIVE-OBSERVABILITY-0001`
- Contract version: `phase4_external_live_observability_v0.1`
- Contract fingerprint:
  `058135c958bbddff241d50cfa50ac5e6b9fe85655f4beabe2b9c033aed9267ac`
- Passive integration model:
  `P4-CONTINUOUS-FIRSTPULLBACK-EXTERNAL-OBSERVABILITY-0001`
- Passive integration fingerprint:
  `f9d782a26a278779cd97c1b405555817fd7169ab6fb1729135d5226ca52bdfbd`
- Bound accepted runtime model:
  `P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0005`
- Bound accepted runtime fingerprint:
  `0308899e0a2306c921603f3beff4a957e01832ce180a397b88841ba498fab5e4`

T019 refreshes only the passive integration's locked V0.5 runtime identity for
the corrected source-health timestamp semantics. The external contract model,
schema, fingerprint, publication behavior, isolation, and all trading semantics
remain unchanged.

## Representation and fixed locator

The producer publishes a dedicated SQLite run registry. Its repository-local
default locator is fixed by the contract integration:

```text
data\paper\multihour\phase4_external_live_observability_v0_1.sqlite3
```

The path is runtime data and remains ignored by Git. A consumer is configured
with this one documented registry locator; it does not scan directories or
select a file by recency. The registry may contain several complete run rows,
each of which binds lifecycle and source metadata atomically under one
`run_id`.

The producer creates the schema, uses WAL, uses `synchronous=FULL`, and commits
each start, heartbeat, or terminal transition as one SQLite transaction. The
external reader opens the existing registry with SQLite URI `mode=ro`, enables
`PRAGMA query_only=ON`, validates the contract metadata and exact schema, and
never invokes schema creation or migration.

## Run identity and fields

Every producer invocation receives a UUID4 `run_id`. A restarted or new run
receives a new ID. The existing accepted runtime session identity is retained
separately as `runtime_session_identity`.

Each row contains:

- contract model, version, and fingerprint;
- accepted runtime model, fingerprint, and session identity;
- absolute paper-runtime SQLite path;
- producer PID as diagnostic evidence only;
- `started_at_utc`, explicit runtime state, and optional `ended_at_utc`;
- heartbeat sequence, heartbeat UTC, producer TTL, and valid-until UTC;
- source model, version, fingerprint, and source identity;
- exact absolute source SQLite path and database identity;
- source anchor, durable source cursor, and current/frozen watermark;
- stable source row and normalized-ingest identities;
- price representation version; and
- atomic publication UTC.

PID or process existence is never sufficient liveness evidence.

## RUNNING validity

The producer heartbeat interval is 1 second and the producer TTL is 5 seconds.
These values are part of the producer contract; a consumer must not invent a
stale threshold.

A row is positively active only when both conditions hold:

```text
runtime_state == RUNNING
AND
heartbeat_at_utc <= observer_now_utc <= heartbeat_valid_until_utc
```

The exact valid-until instant is valid. The first instant after it is stale.
A SQLite file, paper database, process, cursor, timer, or old RUNNING string by
itself never proves current activity.

Heartbeat publication is cadence-bounded rather than performed for each
market row. A timer-preparation boundary forces publication of its frozen
watermark. Other source probes and batch/timer boundaries publish only when
the one-second cadence is due.

## Terminal, crash, and publication-failure semantics

States are:

- `RUNNING`
- `COMPLETED`
- `FAILED`
- `ABORTED`

Normal completion publishes `COMPLETED`. A classified unsuccessful return
publishes `FAILED`. An escaping interrupt/system exit publishes `ABORTED` when
the producer can still publish. Terminal rows contain `ended_at_utc`, close
their liveness interval, and cannot return to RUNNING.

If the producer crashes before a terminal update, its RUNNING row remains as
historical evidence but becomes inactive after its producer-defined
valid-until time. If observability publication itself fails, the passive hook
records the diagnostic in its returned summary, disables further publication,
and does not alter trading flow. The previous proof then expires. This can
produce a conservative false-negative observer state, never a permanent false
RUNNING or an execution-control decision.

## Restart and multiple-run semantics

All lifecycle and source fields for a run reside in the same row. An observer
cannot combine an old lifecycle record with a new source binding.

A new invocation inserts a new `run_id`; it does not mutate or resurrect a
prior terminal/stale row. If exactly one RUNNING row has valid producer
liveness, the view is `ONE_ACTIVE_RUN`. If two or more rows are simultaneously
RUNNING and valid, the view is
`AMBIGUOUS_MULTIPLE_ACTIVE_RUNS`. The contract does not invent leader election
for an observer.

## Exact source binding

The integration obtains the source path, database identity, source identity,
model/version/fingerprint, and anchor from the actual
`ContinuousMarketSourceV02` instance passed into the accepted V0.5 binding.
It never chooses the newest database, scans a nearby directory, copies a
source, or derives a filename.

The stable raw row identity is:

```text
pump_events.rowid
```

The accepted normalized identity relationship is:

```text
phase2.ingest_seq == pump_events.rowid
```

The price representation version is published as `QAP-0.1/P1QAA-0.1` so a
future consumer does not need to infer rational-price representation. The
contract does not create OHLC, interpolate events, or guess entries, exits,
targets, stops, or trailing levels.

## External read-only consumer procedure

An external consumer performs these bounded steps:

1. Open the fixed registry locator with SQLite URI `mode=ro`.
2. Set and verify `PRAGMA query_only=ON`.
3. Validate the metadata row, exact contract fingerprint, and schema.
4. Read complete run rows and apply the producer validity interval exactly.
5. Require one active run, or explicitly report none/ambiguity.
6. Use that same row's `source_sqlite_path`; do not perform source discovery.
7. Open that exact source with URI `mode=ro` and `query_only=ON`.
8. Read bounded `pump_events` rows by ascending `rowid`, no earlier than the
   run anchor and no later than the published source watermark.

The reference reader in
`src/phase4/paper_external_live_observability_v0_1.py` performs these steps
without importing strategy, entry, exit, wallet, network, collector, or
execution-control code.

## Source cursor and watermark semantics

`durable_source_cursor_p1_rowid` is the accepted binding's durably committed
source position. It cannot move backward. `source_watermark_p1_rowid` cannot
precede that cursor.

`CURRENT` means the latest source position observed at publication.
`FROZEN` means the accepted prepared-timer fence is active, including the
graceful duration-bound watermark. Cursor, watermark, kind, heartbeat, and
sequence update in one registry transaction.

## Non-interference and security boundary

The wrapper delegates to the frozen V0.5 run path. Publication is downstream
and passive. Its return value is never read by strategy, candidate selection,
entry/exit routing, position sizing, costs, accounting, timers, source
ordering, cursors, or execution. Publication failure cannot stop or change the
paper run.

The contract has an explicit allow-list of fields and rejects secret-bearing
field names. It does not publish seed phrases, private/signing keys, wallet or
RPC secrets, API secrets, authentication tokens, or credentials. Source and
paper locators are metadata only; observers receive no write or control path.

Phase 4 remains paper only. No wallet, signing, blockchain transaction, or live
order capability is added.

## Performance contract

Publication is independent of individual market rows. The primary focused
performance gate runs two 1,000-row production-shaped accepted V0.5 binding
fixtures with observability disabled and enabled. It requires exact semantic
digest equality, at least 15 enabled raw rows/second, and no more than a 25%
throughput reduction. A separate cadence bound processes 20,000 cursor
observations over 20 simulated seconds. It requires no more than 21 start plus
heartbeat publications, one terminal publication, and less than 5% of the
simulated live-time budget. Exact machine measurements are captured in the
T017 review evidence.

At accepted V0.5 collector load, the registry therefore adds approximately one
small FULL-synchronous transaction per second, plus one start and one terminal
transaction. It does not add writes to the source or paper databases and does
not recreate a per-row persistence bottleneck.

## Deterministic validation

The focused self-test covers current and expired RUNNING proofs, exact TTL
boundary order, crash/stale behavior, all terminal states, terminal
irreversibility, new run identities, old/new coexistence, multiple active-run
ambiguity, concurrent reads during publication, corrupt publication fail-
closed behavior, Windows source paths with spaces, wrong nearby/newest source
databases, query-only source reads, source non-mutation, cursor/watermark
validation-before-update, deterministic replay, no-secret fields, no control
path, and performance.

The same 250-row deterministic V0.5 fixture is executed with observability
disabled and enabled. Canonical paper-runtime digest and semantic counts must
match exactly. Observability-specific registry metadata and wall-clock fields
are the only allowed differences.

No production database, network/RPC, collector, live session, wallet, or live
order is used by these tests.
