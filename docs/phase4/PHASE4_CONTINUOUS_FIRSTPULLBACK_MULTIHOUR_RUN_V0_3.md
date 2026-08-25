# Phase 4 Continuous FirstPullback Multi-Hour Run v0.3

## Status

`IMPLEMENTED_PENDING_PROJECT_REVIEW`

Multi-hour model: `P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0003`

Multi-hour fingerprint:
`6fb400b77606cd1ef3fce32037c95f4ea1cd7299a71ee6c902e43eb2114917bb`

Binding model: `P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0003`

Binding fingerprint:
`45521225ccbe6c3b98a48b7e92523deea16d6ff9b91fdd37a415a1a45958babf`

Bound market source: `P4-CONTINUOUS-MARKET-SOURCE-0002`

Market-source fingerprint:
`9cb094f52bf4b4fe28dc4828b1d9a52a3350cde664c7da5a489fa84e9a4085a9`

## Failed V0.2 evidence and root cause

The immutable failed V0.2 bounded-run artifacts reported a 396.578-second
`PRODUCTION_SOURCE_STALLED` interval at row 602267. Direct read-only
postmortem evidence showed row 602267 at
`2026-08-25T02:04:57.237604+00:00` and row 602268 at
`2026-08-25T02:05:00.753528+00:00`, an actual gap of about 3.516 seconds;
production continued through at least row 604051 and WebSocket observations
continued. The old classification was therefore a false source-stall result.

The V0.2 watchdog measured time since the runner last observed a rowid
increase. That conflated actual production inactivity with a runner unable to
reach its probe while processing. The same run reported repeated launch
reconstruction and heavy timer-fence time. Its database integrity, restart,
cursor, timer, deadline, duplicate-identity, and collector-cleanup checks were
healthy. The old artifacts remain unchanged.

## Evidence-based continuity contract

The V0.3 bounded session establishes its own UTC continuity baseline. An old
anchor row cannot make a new session fail immediately. Each probe reads only
new production rows and evaluates `inserted_at_utc` gaps:

- prior session activity to the first new row;
- between new rows; and
- the last actual activity to probe time.

An actual gap greater than or equal to 300.0 seconds is terminal
`PRODUCTION_SOURCE_STALLED`, even when a late row proves that data later
resumed. The exact boundary is fail-closed: 299.999 seconds is healthy;
300.000 and 300.001 seconds fail.

## Runner starvation

Monotonic time between continuity probes is measured separately. An interval
greater than or equal to 300.0 seconds is
`RUNNER_SOURCE_POLL_STARVED`. Production process liveness, timer activity, and
paper cursor movement do not substitute for either evidence stream.

If both conditions exist, deterministic precedence is:

1. `PRODUCTION_SOURCE_STALLED` when database evidence proves an actual source
   gap;
2. otherwise `RUNNER_SOURCE_POLL_STARVED` when the runner probe interval is
   too long.

Both evidence sets are retained regardless of the chosen stop reason. The
accepted fail-closed shutdown, durable-state preservation, integrity audit,
restart probe, and owned-child-only cleanup paths remain in use.

## Regression evidence

The isolated nightly-shape regression delays the runner probe for 396.578
seconds while committing 112 rows about 3.516 seconds apart. It requires:

- maximum actual source gap: 3.516 seconds;
- actual source stalled: false;
- runner source-poll starved: true; and
- stop reason: `RUNNER_SOURCE_POLL_STARVED`.

A separate recovered-outage regression commits its first new row at 301
seconds and requires `PRODUCTION_SOURCE_STALLED`; late recovery cannot erase
the actual outage.

## Preserved contracts

The V0.3 binding is the smallest versioned compatibility layer needed to bind
Market Source V0.2. It retains Continuous Paper Runner V0.2 and the accepted
PREPARE -> frozen source watermark -> drain through watermark -> execute timer
ordering. T012 entry execution deadlines, CandidateSignal-anchored fallbacks,
500ms entry/exit latency, `FINAL-A`, `FINAL-B`, `SENS-C`, costs, impact,
accounting, observability, and non-aggregation are unchanged.

V0.3 has not run against production. The four-hour V0.3 paper run has not
been executed. No profitability claim is made.

NO PRODUCTION/LIVE VALIDATION PERFORMED.
