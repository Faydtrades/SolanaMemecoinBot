# Frozen OOS Collection Lifecycle v0.1

## Status and scope

Model: `P6-FROZEN-OOS-COLLECTION-LIFECYCLE-0001`

Schema: `phase6_frozen_oos_collection_lifecycle_v0.1`

Model fingerprint: `bd2188b6c57ffafbefe93c9b051146c0c9517bfcaac202f5e659c2aa1260b1bf`

This component is operational orchestration around the accepted Phase-4 continuous FirstPullback source, binding, and paper runner. It does not define a strategy, alter execution economics, evaluate OOS outcomes, or expose an outcome metric during collection. It is paper-only and introduces no wallet, signing, custody, send, or broadcast capability.

The real 72-hour run was not started by MEME-P6-T002.

## Architecture

One OOS run owns one external directory under `D:\Tradingbot\runtime_oos\<run_id>`. Its durable artifacts are:

- `run_manifest.json`: immutable, digest-enveloped scientific identity;
- `lifecycle_state.json`: atomic, digest-enveloped operational state;
- `segment_NNNN.log`: one log per runtime segment;
- `stop.request`: transient graceful-stop request;
- `paper_oos.sqlite3`: the same restart-safe Phase-4 v0.5 paper database for every segment;
- `frozen_source.sqlite3` and `frozen_paper.sqlite3`: consistent immutable final snapshots;
- `frozen_handoff_manifest.json`: immutable, digest-enveloped P6-T001 handoff.

The immutable run manifest binds the run ID, repository commit, accepted P6 protocol and policy set, source path/identity/scope/anchor, paper path/identity, exact 72-hour minimum boundary, no-peek policy, and accepted Phase-4 file and model fingerprints. Resume revalidates every binding before a new segment is registered.

Each process is identified by both PID and operating-system process creation token. A stale PID or reused PID cannot impersonate the prior worker. Abrupt termination closes the stale journal segment on the next status/resume operation; the following segment reopens the same paper database and the accepted binding resumes from `last_durable_p1_rowid`.

All lifecycle state writes use a write-flush-fsync-replace sequence. Manifest and handoff payload hashes are verified on every read. Cursors, watermarks, counts, coverage, boundaries, extension history, segment ordering, and terminal states are checked for backward or contradictory transitions.

## Exact PowerShell commands

Use the reviewed repository checkpoint and its Python interpreter. These examples use the currently available Codex interpreter; a project-approved interpreter may be substituted without changing the lifecycle identity.

```powershell
$python = 'C:\Users\Mari1\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$cli = 'D:\Tradingbot\solana_memecoin_bot_phase1_v0_1\scripts\phase6_frozen_oos_collection_lifecycle_v0_1.py'

& $python $cli start --source-db 'D:\Tradingbot\solana_memecoin_bot_phase1_v0_1\data\db\tradingbot.sqlite3' --runtime-root 'D:\Tradingbot\runtime_oos'
& $python $cli status --run-id '<run_id>' --runtime-root 'D:\Tradingbot\runtime_oos'
& $python $cli stop --run-id '<run_id>' --runtime-root 'D:\Tradingbot\runtime_oos'
& $python $cli resume --run-id '<run_id>' --runtime-root 'D:\Tradingbot\runtime_oos'
& $python $cli finalize --run-id '<run_id>' --runtime-root 'D:\Tradingbot\runtime_oos'
```

An allowed extension is explicit and exactly 24 hours:

```powershell
& $python $cli extend --run-id '<run_id>' --runtime-root 'D:\Tradingbot\runtime_oos' --hours 24 --reason H1_SAMPLE_INSUFFICIENT
```

The other allowed reason is `TECHNICAL_COMPLETENESS`. An extension is accepted only after the current boundary, in 24-hour increments, and never beyond 168 total hours. `H1_SAMPLE_INSUFFICIENT` additionally requires fewer than 150 H1-eligible filled entries. There is no performance-based extension API.

`start` requires a clean checkout, captures the current repository commit and source anchor, rejects an existing unfinished run, and launches one detached segment. The lifecycle assumes an independently managed healthy accepted v0.3.4 collector and never starts a duplicate collector. `stop` creates only an external stop request; the owned worker drains the accepted timer fence, commits durable work, closes SQLite, and records the segment end. `resume` removes only a stale external stop request after proving no matching process remains.

## Coverage and frozen boundary

Wall-clock passage is not coverage. Operational progress records a durable source cursor and watermark, accepted Phase-4 paper counts, H1 sample count, coverage-through timestamp, and unresolved v0.3.4 gap jobs. The source is always opened with SQLite URI `mode=ro` and `PRAGMA query_only=ON`.

Before the target boundary the worker uses accepted Phase-4 v0.5 atomic source batches and prepared timer fences. After the target boundary, a restarted worker may process recovered source rows but does not advance the strategy/exit clock beyond the frozen target. Finalization is blocked until the current source gap ledger contains no unresolved interval intersecting the frozen window and source evidence reaches the exact target.

P6-T001 uses the half-open timestamp window `start_at_utc <= signal_observed_at < end_at_utc`; exact-end and post-end CandidateSignals are excluded even when later catch-up advances the physical source cursor. Finalization takes consistent SQLite snapshots so an independent collector cannot mutate the frozen handoff afterward.

An unrecoverable interval is durably terminal as `UNRECOVERABLE_SOURCE_GAP` / `OOS_COVERAGE_INCOMPLETE`. Identity drift, protocol drift, repository/runtime drift, corrupt state, premature finalization, and active writer conflicts all fail closed.

## No-peek status contract

Status is a fixed allowlist containing only run identity, time boundary/progress, lifecycle and process health, segment/restart counts, source cursor/watermark/counts, CandidateSignal count, H1-eligible sample count, source coverage, database paths/integrity, last durable progress, and operational error state.

The collection module neither imports nor calls the P6-T001 evaluation entry points. Outcome calculations and promotion decisions are unavailable until a separate user action invokes P6-T001 against a `FROZEN_READY_FOR_EVALUATION` handoff.

## Finalization

Finalization requires all of the following:

- target wall-clock boundary reached;
- no matching lifecycle worker active;
- live source gap ledger rechecked and complete through the target;
- durable cursor at the recorded watermark;
- no pending durable work;
- exact source, paper, protocol, policy, repository, and runtime identity;
- source and paper `PRAGMA quick_check = ok`.

The handoff records the exact time window, source and paper snapshot paths and hashes, start/end cursor/watermark, coverage proof, duration, segment-history digest, lifecycle/protocol/policy fingerprints, H1 sample count, extension history, finalization timestamp, and a page-layout-independent logical outcome digest. It explicitly records `evaluation_performed: false`.

Snapshot creation is retry-safe. Without an immutable handoff, leftover snapshots from an interrupted attempt are replaced from the newly revalidated final state and partial temporary snapshots are discarded. Once the immutable handoff exists it is reused unchanged, and a crash after writing that handoff but before updating lifecycle state is recovered idempotently.

## Deterministic test evidence

The focused synthetic matrix covers the 35 required cases plus twelve adversarial and worker-integration cases. It produced 47/47 PASS. Equivalent uninterrupted and stop/resume fixtures produced the same frozen logical outcome digest:

`0f8127fa6d763079303fdeaff4dfb92a16a9ec4bbbe4b8fd9e9caaec915ab1f3`

The synthetic lifecycle used temporary external directories and left the real repository status byte-for-byte unchanged. It did not access the production database, start a collector, use the network, run P6-T001 evaluation, or start the real OOS window.
