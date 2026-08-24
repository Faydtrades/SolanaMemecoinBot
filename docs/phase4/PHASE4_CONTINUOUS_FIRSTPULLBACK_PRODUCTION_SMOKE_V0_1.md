# Controlled Production FirstPullback Paper Smoke v0.1

## Status and scope

This harness performs one bounded production-data validation of the accepted
continuous FirstPullback paper stack. It never creates a wallet, signature,
blockchain transaction, real order, or live-capital exposure. The run evaluates
causal/runtime correctness and does not establish profitability.

UNBOUNDED CONTINUOUS LIVE PAPER:
NOT YET VALIDATED / NOT YET ACCEPTED

## Identity

- Model: `P4-CONTINUOUS-FIRSTPULLBACK-PRODUCTION-SMOKE-0001`
- Fingerprint: `0ad1cd2eff06399b93736d452859bab4ea081ee84d8ac287435e4e4b35c6311b`
- Binding: `318e15b104c691821f5715b090c934e5b1cb3d9a8357fe0930d8825ef66d0daa`
- Market source: `242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb`
- Gap compatibility: `aabb765f81a29b2dd119607cdea92a2949dd3acec10aa204d246e6aa7a28a78c`
- Continuous runner: `8dcab9d17f2ec71f4920a199b5745dae92dc3464a466860593231424e0a24dea`
- FirstPullback selection SHA-256: `408657c1d6dc39435b61e01b368dac69796481864b7462efd150a2d4e1b0daa1`
- Cost baseline: `P4-COST-BASELINE-0001`
- Cost fingerprint: `9652aeffc792eb48d62348360c6ae804f660e43329195d1f169219b80ccab67c`

The fingerprint binds the exact role order, `FINAL-A`, `FINAL-B`, and `SENS-C`
exit identities, source-anchor/database-identity policies, polling cadence,
clock cadence, runtime bounds, batch size, collector supervision policy, and
paper-only scope.

## Source and anchor

The production database is `data/db/tradingbot.sqlite3`. The accepted market
source opens it with SQLite URI `mode=ro`, sets `PRAGMA query_only=ON`, and uses
bounded rowid operations. The harness does not run production `quick_check`, a
full-table count, or a historical full scan.

Before starting a managed collector, the harness reads the latest production
`p1_rowid` and freezes it as `start_after_p1_rowid`. The resolved absolute
database path and exact anchor form the source identity. Only later rows belong
to the smoke; pre-anchor launches cannot activate a mint.

## Live and timer policy

- Minimum runtime: 300 seconds
- Hard maximum runtime: 1,200 seconds
- Market polling: 500 ms
- Strategy/exit clock preparation: 1 second
- Market batch limit: 250 rows
- Collector queue drain: 10 seconds
- Collector cleanup allowance: 120 seconds, covering accepted worker sleeps
  and bounded RPC waits
- Managed collector target: 500 newly inserted Pump events

Every clock cycle uses the accepted order:

```text
prepare immutable UTC clock and capture production watermark
-> drain binding only through the captured watermark
-> execute the prepared strategy/exit clock
-> admit later production rows
```

The accepted `PREPARED`/`PLANNED` production fence remains authoritative. Early
stop is allowed only after five minutes when at least one genuine candidate has
all three alternative paper tracks closed and reconciled.

## Collector supervision

The workflow first inspects running process command lines outside the harness.
It passes `--use-existing-collector` only for an independently verified healthy
v0.3.4 process. Otherwise it uses `--launch-managed-collector`.

The managed path launches the accepted v0.3.4 `run()` implementation in a child
process with its accepted bounded target set to 500 new Pump events. Reaching
that target invokes the collector's own listener stop and queue-drain path. The
paper harness may continue its bounded timers after this target is complete.
The child also has a unique parent-owned stop sentinel for low-throughput cases
where the target is not reached before the smoke stops. A cleanup timeout is
recorded as failure and a process-specific forced fallback prevents an orphan.
The workflow never signals or terminates an unrelated process.

Accepted collector v0.3.4 contains a post-cleanup summary-label typo for
`legacy_v0_2_jobs_ignored`. The managed wrapper catches only that exact `KeyError`
after listener/worker cleanup; all other collector errors remain failures. The
collector source is not changed.

## Graceful stop and audit

At stop, the harness freezes an end watermark, prepares a final clock bound to
that exact value, drains only through it, executes the timer, verifies no binding
outbox remains, and closes the paper connection. It does not invent a future
market observation to close a position.

Postrun checks open only the isolated paper database for `PRAGMA quick_check`,
identity/anchor/cursor validation, duplicate audits, outbox/audit delivery,
timer completion, UTC timestamps, accounting, observability, and cross-track
summation prohibition. One exact binding/source/anchor restart probe must
reconstruct the same state without duplicate effects.

## Classification

- `PASS_FULL_TRADE`: real data, candidate, all three paper tracks closed, and
  accounting/integrity/restart passed.
- `PASS_PIPELINE_NO_CANDIDATE`: genuine post-anchor data, fresh launch, strategy
  evaluations, and integrity/restart passed without a natural candidate.
- `FAIL`: a correctness, integrity, conflict, or runtime contract failed.
- `FAIL_INSUFFICIENT_LIVE_EVIDENCE`: the hard bound ended without the minimum
  fresh production/evaluation/timer evidence.

`FINAL-A`, `FINAL-B`, and `SENS-C` are reported separately and are never summed
as a single portfolio.

## Production smoke evidence pending project review

The controlled run beginning `2026-08-24T13:53:52.299461+00:00` ended after
`301.577536` seconds with `PASS_FULL_TRADE`. The production cursor advanced
from the frozen anchor `576100` through `576600`: 500 raw rows produced 76
normalized records, 424 deterministic skips, three fresh launches, 225
persisted role evaluations, and one CONTROL candidate. All three alternative
paper tracks closed, accounting reconciled, `PRAGMA quick_check` returned
`ok`, and the exact-identity restart probe passed.

The smoke-managed collector reached its accepted 500-event bound, completed
its own drain path, exited with code 0, and left no orphan process. Its log
ended with the documented v0.3.4 post-cleanup summary-label defect only. No
production source, accepted binding, runner, strategy, research, cost, or exit
contract was changed.

Runtime artifacts:

- Paper DB: `data/paper/live_smoke/phase4_firstpullback_production_smoke_20260824T135352_299461Z.sqlite3`
- Paper DB SHA-256: `9353f633fec770a11ba0d601865f424a9a19c1e320e9380c7a4c0b2a25b8ce89`
- JSON: `data/paper/live_smoke/phase4_firstpullback_production_smoke_20260824T135352_299461Z.json`
- JSON SHA-256: `3256ac0d8d60208ebfa00527594550ac11e031bf985e3ad716c7de1201d9a81a`
- Collector log SHA-256: `3a07d1e4cbc94e8fbfb37a82b5db6111b844d22be3fb7ea34505814b428bfae0`

This evidence is pending ChatGPT project review. It is runtime-validation
evidence, not a profitability claim or acceptance of unbounded operation.

## Artifacts and commands

Each production invocation creates a new timestamped paper SQLite database,
JSON summary, and managed-collector log under ignored
`data/paper/live_smoke/`. It never reuses an earlier paper database.

Local isolated harness gate:

```text
\.codex_venv\Scripts\python.exe scripts\phase4_continuous_firstpullback_production_smoke_selftest_v0_1.py
```

Managed-collector production run:

```text
\.codex_venv\Scripts\python.exe scripts\phase4_continuous_firstpullback_production_smoke_v0_1.py --launch-managed-collector
```

## Known performance limitations

The accepted market source rescans the growing post-anchor launch prefix in
`_eligible_launches_through()`. Binding hydration performs one exact read-only
row query per normalized production record. The smoke measures these costs and
does not redesign or optimize them.
