# Continuous FirstPullback Multi-Hour Run v0.4

## Status

`IMPLEMENTED_PENDING_PROJECT_REVIEW`

Model: `P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0004`

Fingerprint:
`ce690a8fa864516fcb6fe7c834e4a4342655a9c01c80f522dc0ba9ba3673e0ad`

Bound binding: `P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0004`

Binding fingerprint:
`e2c35ce63fe419ac4a934913dd26249d72411884cb115e0701f601b276b0f7d6`

Bound source: accepted `P4-CONTINUOUS-MARKET-SOURCE-0002`

## Scheduler integration

V0.4 replaces the inherited clock-cycle and graceful-end drain callbacks with
the versioned binding-owned active drain. Normal source processing remains the
accepted path. During a fence drain, the harness updates existing raw,
normalized, skip, semantic-event, source-fetch, and latency counters for every
batch and invokes the accepted T014 evidence-based watchdog at progress
boundaries.

The final duration watermark is prepared once, remains immutable, drains
through exactly that watermark, executes all required timers, verifies zero
pending outbox work, and requires the final durable cursor to equal the frozen
watermark.

Legacy `timer_fence_wait_seconds` now represents passive waiting and is zero
for readable backlog. Active CPU/SQLite work is reported as
`fence_drain_seconds`; source-unavailable waiting is separate.

## Deterministic workload evidence

The 5,000-row ready-backlog regression required:

- exact durable completion through row 5,000;
- 20 drain batches at batch size 250;
- zero deliberate poll sleeps;
- no row 5,001 semantic effect before the timer;
- timer completion after row 5,000;
- normal source processing resuming through row 5,010; and
- exact restart/reopen digest.

The 20,000-row workload used 1,000 fresh mints, 5,000 normalized rows, 15,000
deterministic skips, recurring timers, source growth beyond each fixed
watermark, strategy/audit persistence, semantic outbox delivery, durable
cursors, and real timer effects.

Canonical runtime result:

- raw throughput: 131.81 rows/second;
- normalized throughput: 32.95 rows/second;
- fence-drain throughput: 132.01 rows/second;
- timer count: 5;
- maximum cursor lag after a timer: 4,000 rowids;
- cursor-lag sequence: 4,000, 4,000, 4,000, 4,000, 0;
- maximum fence drain: 47.54 seconds;
- maximum runner probe interval: 0.05 seconds;
- passive fence wait: 0 seconds;
- source-unavailable wait: 0 seconds.

The same-runtime 500-row profile improved from 160.53 raw rows/second under
V0.3 to 224.79 under V0.4, a 1.40x ratio. The absolute V0.4 result exceeds the
required 15 rows/second by more than seven times, so the environment-exception
5x ratio alternative was not needed.

## Validation status

This is isolated synthetic evidence only. V0.4 has not been run against the
production database, collector, network, or a four-hour window. It makes no
profitability claim.

NO PRODUCTION/LIVE VALIDATION PERFORMED.
