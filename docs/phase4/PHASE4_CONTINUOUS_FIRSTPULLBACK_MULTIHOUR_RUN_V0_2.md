# Continuous FirstPullback Multi-Hour Runner v0.2

Status: `IMPLEMENTED_PENDING_PROJECT_REVIEW`

Model: `P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0002`

Fingerprint:
`9002ed30ecd74722af5a37efec437143a17c4865f94ae37a60863fbc2320a7d7`

## Purpose

v0.2 is the corrected bounded multi-hour paper harness for the track-safe entry
execution policy. It retains the accepted v0.1 duration, source-continuity,
read-only source, collector-ownership, timer-fence, frozen-end-watermark,
artifact-finalization, restart, accounting, and non-aggregation contracts.
The accepted v0.1 harness and self-test remain byte-stable.

The v0.2 fingerprint binds:

- the accepted v0.1 multi-hour harness SHA-256
  `948db8f791f21242a49c85f3b6f8c70c1736db36ffa6c40367fd97a80fff2ddf`;
- versioned FirstPullback binding fingerprint
  `c2f90d610ab774831af6a52b4a24e948b4bf0727d69457fee63311b5a159feca`;
- versioned continuous-runner fingerprint
  `66725275f7e01e510b369df08281be9769d057113896f60f2ac652c2efe072fb`;
- entry-deadline model `P4-PAPER-ENTRY-EXECUTION-DEADLINE-0001` and fingerprint
  `6e2ae931d7270012dc025ef9a509ec30a240a850ea0d1c2666560a5e5506c9a7`;
- locked exit-spec fingerprint
  `0bde5378f4ff2b63c6a96ad64140840ef8b2770c3be772a7aa71f8bdeb070129`.

## Active versioned path

The harness selects versioned counterparts without changing accepted semantic
history:

- `PaperEntryRouterV02` for independent entry eligibility and audit;
- `PaperExitLifecycleBridgeV03` for the non-empty subset of tracks that
  actually opened;
- `ContinuousPaperRunnerV02` for entry deadline clock handling, partial-track
  restart, and deadline terminal audit healing; and
- `ContinuousFirstPullbackBindingV02` for the accepted source/outbox/timer
  transport bound to the v0.2 runner.

The accepted V01 source adapter, Phase-2 strategy, Phase-3 finalists, cost
baseline, entry/exit impact, exit orchestrator, accounting, observability,
collector, and production-smoke harness remain unchanged.

The postrun reopen probe now requires both canonical runtime reconstruction and
byte-exact SQLite SHA-256 stability. A same-size file mutation cannot produce a
false restart PASS. Integrity auditing also starts from every entry route, so a
route missing its entire deadline companion ledger fails closed.

## Runtime diagnostics

The JSON paper summary adds a separate `entry_execution_deadlines` section:

- total execution expiry events;
- execution expiry count by track;
- candidates with mixed filled/rejected tracks;
- candidates for which all tracks execution-expired;
- pending entry tracks at the bounded end;
- filled track count by track; and
- raw per-track deadline state counts.

Existing track accounting remains separate. FINAL-A and FINAL-B are alternative
primary tracks, SENS-C remains `SENSITIVITY ONLY`, and cross-track portfolio PnL
summation remains prohibited.

## Corrected production regression

The deterministic integrated regression reproduces an observation 36.512s
after CandidateSignal. v0.2 expires all three orders at their canonical times,
creates zero positions and zero entry costs, records a terminal execution SKIP
audit without changing the CandidateSignal, and accepts a later ClockTick to
prove the runner continues. No backwards lifecycle transition occurs.

## Graceful bounded end

The duration-boundary branch still prepares one timer and treats that timer's
captured production watermark as the authoritative end watermark. It drains
only through the frozen watermark and executes the timer at the actual boundary
instant. A deadline later than that instant remains pending; a deadline due by
that instant expires. There is no clock fast-forward, future observation,
invented fill, or forced position closure.

## Validation state

The policy and harness have deterministic isolated self-tests, including the
accepted v0.1 harness regression suite. The failed first v0.1 production attempt
remains FAIL evidence. v0.2 has not been run against production and is not
accepted or checkpointed.

MULTI-HOUR V0.2 PRODUCTION PAPER RUN:
NOT YET EXECUTED / NOT YET VALIDATED / NOT YET ACCEPTED

UNBOUNDED CONTINUOUS LIVE PAPER:
NOT YET VALIDATED / NOT YET ACCEPTED

No profitability claim is made. No entry/exit parameter tuning, research
reselection, wallet/signing/live-order implementation, production database
access, network/RPC access, or collector execution occurred in this task.
