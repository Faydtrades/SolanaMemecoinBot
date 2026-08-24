# Bounded Multi-Hour FirstPullback Paper Harness v0.1

## Status and scope

This dedicated run-control layer is implemented pending ChatGPT project
review. It composes the checkpointed T009 source, binding, runner, timer,
accounting, observability, audit, restart, and artifact helpers without
modifying the accepted production-smoke harness.

The harness is paper-only. It creates no wallet, signature, blockchain
transaction, real order, or live-capital exposure. It performs no strategy,
entry, exit, cost, latency, impact, or research tuning.

THE FOUR-HOUR PRODUCTION RUN HAS NOT OCCURRED.

UNBOUNDED CONTINUOUS LIVE PAPER:
NOT YET VALIDATED / NOT YET ACCEPTED

## Identity

- Model: `P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0001`
- Fingerprint: `e7cfb5049d93728391decf633a245585b49d7c4ea8e256bca8c98b53ccb5da47`
- Pre-correction fingerprint: `b211c563d3b66de6a947d76f5630a6798a6c85629a5d7d678967553526d4a5a1`
  / `PRE-ACCEPTANCE / SUPERSEDED / NOT CHECKPOINTED`
- Accepted T009 fingerprint: `0ad1cd2eff06399b93736d452859bab4ea081ee84d8ac287435e4e4b35c6311b`
- Accepted T009 file SHA-256: `9c6a786d56137aa0d1f99c24d8878e60ab4a5846e8ec91b8ec1fabd056b5d34d`
- Binding: `318e15b104c691821f5715b090c934e5b1cb3d9a8357fe0930d8825ef66d0daa`
- Market source: `242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb`
- Gap compatibility: `aabb765f81a29b2dd119607cdea92a2949dd3acec10aa204d246e6aa7a28a78c`
- Continuous runner: `8dcab9d17f2ec71f4920a199b5745dae92dc3464a466860593231424e0a24dea`
- FirstPullback selection SHA-256: `408657c1d6dc39435b61e01b368dac69796481864b7462efd150a2d4e1b0daa1`
- Cost fingerprint: `9652aeffc792eb48d62348360c6ae804f660e43329195d1f169219b80ccab67c`

The model fingerprint binds accepted components, duration range/default,
absence of candidate early stop, polling/clock cadence, unlimited managed
collector policy, source/anchor policy, graceful end, artifact policy, metric
families, the exact 300-second production-source continuity watchdog,
alternative exit identities, and cross-track summation prohibition.
The requested duration, resolved database identity, and exact anchor are also
bound into a unique runtime session identity.

H1-C2 corrects the implementation ordering to enforce the already serialized
300-second contract before any newly observed row can mutate watchdog state.
Because `MULTIHOUR_SPEC` did not change, the model fingerprint remains
`e7cfb5049d93728391decf633a245585b49d7c4ea8e256bca8c98b53ccb5da47`.

H1-C3 corrects the normal duration-boundary implementation to make the
persisted prepared timer—not a separate earlier source query—the sole frozen
end-watermark authority. This is also an implementation correction to the
already serialized freeze/drain/execute contract, so the fingerprint remains
unchanged.

## Duration and full-session policy

- Default: 14,400 seconds (four hours)
- Minimum: 3,600 seconds
- Maximum: 21,600 seconds
- CLI: `--duration-seconds`
- Candidate-based early stop: disabled

Naturally occurring candidates and completed trades do not stop the session.
The loop ends only at the requested monotonic duration or on a fail-closed
runtime/integrity error. Multiple candidates remain independent. Every
accepted candidate uses the unchanged alternatives:

- `FINAL-A` / `E3_TP10_T15`
- `FINAL-B` / `E4_ACT10_GB03_T15`
- `SENS-C` / `SENS_TP20_T5` / `SENSITIVITY ONLY`

The alternatives are never aggregated into one portfolio.

## Production source and runtime artifacts

At the reviewed session start, the accepted source opens
`data/db/tradingbot.sqlite3` with SQLite URI `mode=ro` and
`PRAGMA query_only=ON`. A bounded latest-rowid query freezes the exact anchor
before any managed collector starts. Only post-anchor launches can activate a
mint. The harness does not perform a production `quick_check`, full count, or
historical full scan.

Production rowid advancement is monitored independently of the binding
cursor. `SOURCE_STALL_FAIL_SECONDS` is exactly 300.0. Only an increase in the
latest production `p1_rowid` resets the monotonic source-activity clock.
Collector process liveness, prepared timers, clock execution, and binding
cursor advancement cannot reset it. The same watchdog applies to existing and
managed collector modes.

The JSON `source_continuity` object records the threshold, number of observed
advances, maximum source-stall duration, final source staleness, continuity
status, and last observed production rowid. A non-advancing source at or above
300 seconds stops processing immediately with `PRODUCTION_SOURCE_STALLED`,
even when that observation includes a newly advanced rowid. An advance at
299.999 seconds may reset the clock; an advance at exactly 300.0 seconds may
not update the last rowid, activity time, or advancement count. Once terminal,
the watchdog cannot recover or restore continuity.
Source continuity and final staleness below the threshold are mandatory for
`PASS_MULTI_HOUR`; early live evidence followed by long silence cannot pass.

Every invocation creates a new timestamped paper database, JSON summary, and
optional owned-collector log under `data/paper/multihour/`. It never reuses a
T009 smoke database. The tracked root `.gitignore` owns the exact
`data/paper/multihour/` rule. Runtime execution creates no nested `.gitignore`
or other Git metadata.

## Collector lifecycle

The operator first checks for one independently verified healthy v0.3.4
collector. `--use-existing-collector` reuses it and grants this harness no
authority to stop it.

When no healthy collector exists, `--launch-managed-collector` starts the
accepted v0.3.4 `run()` implementation in an owned child process group with:

```text
max_pump_events = None
drain_seconds = 10
```

This is the accepted unlimited/time-independent mode, not the T009 500-event
target. At the session end the parent sends an interrupt to only its owned
process group. The child converts Windows `SIGBREAK` to `KeyboardInterrupt`,
allowing the accepted collector's `finally` block to stop the listener and
drain confirmation/deep/gap queues. The parent waits up to 180 seconds, then
may terminate or kill only that owned child as a recorded failing fallback.
The child's absence is verified through its process handle.

Only the exact accepted post-cleanup `KeyError('legacy_v0_2_jobs_ignored')` is
handled. No broader collector exception is suppressed, and the collector file
is unchanged.

## Timer, watermark, and graceful end

Polling remains 0.5 seconds, clock preparation remains 1.0 second, and batch
size remains 250. All clock work uses T009's accepted binding helpers:

```text
prepare immutable clock and capture watermark
-> drain only through frozen watermark
-> execute prepared timer
-> continue production
```

At the duration bound, the harness freezes one final source watermark, drains
through it under the accepted timer fence, executes the prepared final timer,
requires an empty semantic outbox, persists the exact durable cursor, and
closes paper SQLite. Duration handling captures one boundary UTC instant and
immediately prepares the final timer with it. The watermark persisted by that
timer is authoritative; there is no separate pre-read source watermark and no
second latest-row query that can redefine the boundary. The same instant is
recorded in the prepared timer, emitted `ClockTickEvent`, and JSON
`duration_boundary_utc` field.

Rows written after final timer preparation remain beyond its active fence.
They are not drained, admitted, counted in bounded production evidence, or
included in the restart digest. Production may legitimately have a later
current rowid than the session's final durable cursor. It never fabricates a
market observation or fill. A position with no causal exit may remain `OPEN`;
open state is recorded rather than converted into a synthetic close.

On `PRODUCTION_SOURCE_STALLED`, the result remains `FAIL`; when the binding is
still usable and has no pending outbox or timer fence, the harness preserves
the already durable cursor and closes it cleanly. It deliberately does not
re-read a final live watermark or prepare a new timer: either could admit the
late row that triggered the terminal continuity failure. It never waits for
the original duration after a confirmed stall and never invents data or fills.

## Evidence and postrun audit

The JSON records source counts, evaluated mints, role evaluations, every
candidate identity, transitions, terminal skips, accepted/rejected entries,
position state, per-track trade counts, exit reasons, gross/net PnL,
wins/losses, profit factor where meaningful, MAE/MFE, per-track equity and
drawdown, timers, fence waits, cursor lag, reconstruction/hydration queries,
SQLite lock/busy errors, conflicts/retries, and mean/p95 latency.
The `source_continuity` object is a separate production-source health signal;
binding cursor lag remains a distinct performance measurement.

Postrun inspection opens only the isolated paper database for
`PRAGMA quick_check`, duplicate identities, cursor/outbox/timer consistency,
accounting, UTC timestamps, and cross-track prohibition. One exact-identity
restart probe reconstructs persisted state without starting a new live
session.

`PASS_MULTI_HOUR` additionally requires the requested duration, live source
advance, a genuine fresh launch, actual evaluations, timer execution, graceful
shutdown, paper integrity, restart success, and continuity valid through the
duration boundary. Otherwise the result is `FAIL`. Candidate completion is
not an acceptance prerequisite and open positions are allowed.

## Commands for a future reviewed production task

These commands are documented only; they were not run in T011-H1.

```text
.codex_venv\Scripts\python.exe scripts\phase4_continuous_firstpullback_multihour_run_v0_1.py --duration-seconds 14400 --use-existing-collector
```

or, only when no healthy collector exists:

```text
.codex_venv\Scripts\python.exe scripts\phase4_continuous_firstpullback_multihour_run_v0_1.py --duration-seconds 14400 --launch-managed-collector
```

The production entry point requires a clean Git checkout and revalidates every
locked identity plus the byte-exact T009 harness before opening production.

## Isolated self-test

```text
.codex_venv\Scripts\python.exe scripts\phase4_continuous_firstpullback_multihour_run_selftest_v0_1.py
```

The A-X gate plus C1 and C2 continuity scenarios use temporary SQLite fixtures
and simulated child processes only. They open no production database,
collector, network, RPC, or WebSocket. Coverage includes exact identity,
duration, full-session/multi-candidate control, every 300-second continuity
boundary, late-advance rejection before mutation, terminal non-recovery,
process-liveness independence, timer/cursor independence, reset behavior,
mandatory PASS continuity, rejected-late-row durable-state preservation, exact
post-failure restart digest, prepared-timer-only boundary authority, stale
pre-read race removal, one and multiple post-freeze rows, exact final clock
timestamp, bounded production evidence, deterministic boundary-race replay,
root ignore policy, causal finalization, open-position policy, per-track
labels, child ownership, unlimited mode, JSON, quick check, repeated
deterministic digests, and T009 byte immutability.
