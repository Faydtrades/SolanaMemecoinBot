# Repository Governance

## Role split

### ChatGPT project

The ChatGPT project is the technical project lead, architecture guard, task
definer, acceptance authority, and reviewer.

### Codex

Codex is the implementation and testing agent. It may inspect the repository,
implement explicitly bounded tasks, run authorized tests, and inspect diffs. It
must not choose project direction independently.

## Standard workflow

```text
ONE TASK
-> IMPLEMENT
-> TEST
-> REVIEW
-> PASS
-> CHECKPOINT
-> NEXT TASK
```

Codex must not declare a task or phase `COMPLETE`, `PASS`, `ACCEPTED`, or
`CHECKPOINTED`. ChatGPT project review owns those decisions. The normal Codex
implementation status is:

```text
IMPLEMENTED_PENDING_PROJECT_REVIEW
```

## One bounded task at a time

- Do not expand the assigned scope.
- Do not perform unrelated refactoring.
- Do not clean up or rewrite validated paths unless the task explicitly
  authorizes it.
- Stop and report a conflict when the requested work would violate a lock or
  protected-data rule.

## Start-state verification

For normal future tasks, inspect before editing:

```text
git status
git rev-parse HEAD
git branch --show-current
```

The observed checkpoint and working-tree state must match the task's stated
expectations. Preserve unrelated user changes.

## Locked research

Treat validated freezes, manifests, hashes, finalist selections, parameter
sets, validation artifacts, source bindings, and experiment identities as
immutable unless a future explicit task authorizes a change. This includes:

- `DEV-FREEZE-0001`
- `DEV-FREEZE-0002`
- `EXP-0012` Phase-3 final validation
- `FINAL-A`
- `FINAL-B`
- `SENS-C`

No new parameter search, candidate reselection, or exit tuning may occur
without explicit ChatGPT project authorization.

## Protected runtime and data

Production collector baseline:

```text
scripts\live_pump_collector_v0_3_4.py
```

Production database:

```text
data\db\tradingbot.sqlite3
```

Production, raw, and live runtime data must never be modified by ordinary
tests. Prefer temporary databases, isolated copied datasets, synthetic
fixtures, and deterministic self-tests.

## Live-process safety

- Do not stop, restart, signal, or change a collector or paper runtime unless
  explicitly instructed.
- Do not modify code underneath an active runtime unless the task is explicitly
  designed for that operation.
- Do not start network, collector, live-smoke, or long-running activity merely
  to validate an unrelated change.

## Phase-4 safety

- No wallet, signing, or live-order implementation is currently authorized.
- Phase 4 remains paper trading.
- `FINAL-A`, `FINAL-B`, and `SENS-C` are alternative exit tracks and must not be
  summed as one portfolio.
- Preserve UTC, causality, deterministic replay, restart safety, idempotence,
  source/hash bindings, and applicable SQLite integrity checks.
- Do not silently alter locked costs, latency, impact models, slippage caps,
  strategy parameters, or exit definitions.

## Testing

- Run the exact relevant tests authorized by the task before reporting an
  implementation ready for review.
- Report concrete commands, exit status, and relevant validation evidence; do
  not report only "tests passed."
- Prefer existing standalone self-tests over inventing a new framework unless
  the task specifically requires one.
- Tests must not touch production databases, raw collector data, or live
  runtime state.

## Git rules

For normal implementation tasks:

- leave changes uncommitted;
- do not stage files;
- inspect the diff and source-control surface;
- report the exact changed files;
- await ChatGPT project review.

Only a separate, approved checkpoint task may stage and commit accepted work.
Do not configure or change a remote without explicit authorization.

## Secrets

Never expose, print, log, persist, or commit secret values. Do not place wallet
seed material, private keys, signing credentials, API tokens, or private RPC
credentials in this repository.
