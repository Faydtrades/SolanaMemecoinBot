# Phase 4 Timer-Fence Active Drain v0.1

## Status

`IMPLEMENTED_PENDING_PROJECT_REVIEW`

Binding model: `P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0004`

Binding fingerprint:
`e2c35ce63fe419ac4a934913dd26249d72411884cb115e0701f601b276b0f7d6`

## Production evidence motivating the correction

The accepted V0.3 bounded run requested 14,400 seconds but stopped after
1,421.637045 seconds with correctly classified
`RUNNER_SOURCE_POLL_STARVED`. Production remained healthy: its maximum actual
gap and final staleness were 26.012124 seconds, while the runner probe interval
reached 950.25 seconds. The run attributed 1,367.2016976 of 1,421.637045
seconds to timer-fence waiting. V0.3 integrity and restart checks passed.

The accepted source optimization was healthy: one launch rebuild, 25 normal
monotonic fetches without rebuild, and no catch-up or rewind reconstruction.
Market Source V0.2 is therefore retained unchanged.

## Controlled root-cause profile

Inspection and isolated profiling showed that accepted V0.3 already called a
synchronous `_drain_through` loop. The metric called timer-fence wait mostly
measured active normalization, strategy/audit, outbox, and cursor work; it was
not a market-poll sleep. The loop nevertheless behaved as one monolithic
scheduler call and did not refresh T014 continuity telemetry while advancing.

On the controlled 500-row mixed fixture, semantic evaluation/outbox delivery
was materially more expensive than hydration. Repeated FULL-sync evaluation
commits were the largest correctable persistence component. V0.4 retains the
same evaluation IDs, payload fingerprints, audit rows, and replay checks while
delivering all evaluations for one source input in one SQLite transaction.

## Active fixed-watermark drain

For durable cursor `C` and the minimum active timer watermark `W`:

1. retain the persisted immutable `W`;
2. actively process source batches until the durable cursor reaches `W`;
3. perform a bounded source-health probe before and after each batch;
4. execute every incomplete timer at `W` in timer-key order;
5. only then release row `W+1` or move to a later timer watermark.

Ready rows cause no deliberate sleep. If a previously captured row is
temporarily unavailable, only that source-availability condition can invoke
the bounded poll callback, and its time is reported separately. A batch that
does not durably advance fails closed instead of busy-looping.

Market Source V0.2 remains sufficient. Its accepted binding-side fence filters
all persisted semantic work to rows `<=W`. Continuous production may advance
beyond `W`, but the target does not move and no `W+1` input, feature event,
audit, outbox item, or runner effect is admitted before timers at `W` complete.

## Crash and restart

The isolated matrix interrupts after source fetch, normalization, semantic
state persistence, runner/outbox work before cursor commit, final cursor `W`
before timer execution, and timer execution. Each restart converges to:

- cursor exactly through `W`;
- timer `COMPLETE` only after `W` is durable;
- zero pending outbox work;
- no duplicate input identities;
- SQLite `quick_check = ok`; and
- an idempotent canonical digest on a second reopen.

## Instrumentation

V0.4 reports normal and fence source fetch counts/time, drain counts/batches,
raw and normalized drain rows, active drain duration, passive wait, genuine
source-unavailable wait, hydration, strategy evaluation/persistence, semantic
audit/outbox delivery, cursor commits, timer prepare/execute, attributed
SQLite transaction time, maximum fence distance, maximum drain duration, and
the T014 maximum runner-probe interval.

## Safety boundary

No FirstPullback parameters, finalists, entry deadlines, fallback clocks,
500ms latencies, costs, impacts, trade size, accounting, PnL observability,
source normalization, or collector behavior changed. This remains paper-only.

NO PRODUCTION/LIVE VALIDATION PERFORMED.
