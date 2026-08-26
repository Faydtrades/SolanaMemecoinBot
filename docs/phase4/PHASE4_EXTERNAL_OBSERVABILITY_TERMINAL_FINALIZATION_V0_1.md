# Phase-4 External Observability Terminal Finalization Correction v0.1

## Status

`MEME-P4-T021`: `IMPLEMENTED_PENDING_PROJECT_REVIEW`

No production database, network/RPC, collector, live session, wallet, signing,
order, or multi-hour validation was used by this task. The accepted formal
six-hour V0.5 paper result and all trading semantics remain frozen.

## Root cause and bounded correction

The T017 wrapper closed and cleared its sole heartbeat publisher after any
publication error. Terminal publication then required that same publisher and
the live paper binding. The accepted six-hour process completed after runtime
teardown, leaving run `95aebf92-3353-4404-bb03-82e776218740` safely inactive
by TTL but persistently `RUNNING` with `ended_at_utc = NULL`. The wrapper also
added its outcome only to the returned in-memory summary while `main()`
discarded that summary.

T021 adds a fresh producer-owned finalizer to the unchanged T017 registry
contract. It opens the exact registry, validates metadata and exact schema,
selects only a canonical exact `run_id`, reserves the writer with
`BEGIN IMMEDIATE`, revalidates the complete RUNNING record, and atomically
writes the terminal state, time, cursor, watermark, watermark kind, heartbeat,
publication time, and sequence. TTL expiry does not prohibit terminalization.
A terminal row cannot transition again.

For a normally returned V0.5 run, the completed summary is authoritative for
`final_durable_p1_rowid`, `end_watermark_p1_rowid`, and frozen/current
watermark kind. Neither the closed binding nor the original publisher is used.
After the registry attempt, the wrapper adds `external_observability` to the
same V0.5 JSON using a same-directory exclusive temporary file, flush,
`fsync`, and atomic replace. Every publication or JSON error remains passive:
the V0.5 return code, result class, paper database, digest, and accounting are
not changed.

## Identity decision

- Contract model: `P4-EXTERNAL-LIVE-OBSERVABILITY-0001`
- Contract version: `phase4_external_live_observability_v0.1`
- Contract fingerprint (unchanged):
  `058135c958bbddff241d50cfa50ac5e6b9fe85655f4beabe2b9c033aed9267ac`
- Passive integration model:
  `P4-CONTINUOUS-FIRSTPULLBACK-EXTERNAL-OBSERVABILITY-0001`
- Refreshed passive integration fingerprint:
  `6ac56972dd192e58f1c042768303fecb7a7e5d20d146ee3b7271190459d8fda4`

The schema, consumer fields, liveness interpretation, and query-only reader
did not change, so the external contract identity remains fixed. The producer
wrapper behavior changed materially, so its integration fingerprint changed.

## Exact changed files

1. `PROJECT_STATE.md`
2. `docs/phase4/PHASE4_EXTERNAL_READ_ONLY_LIVE_OBSERVABILITY_V0_1.md`
3. `docs/phase4/PHASE4_EXTERNAL_OBSERVABILITY_TERMINAL_FINALIZATION_V0_1.md`
4. `scripts/phase4_continuous_firstpullback_external_observability_run_v0_1.py`
5. `scripts/phase4_external_live_observability_selftest_v0_1.py`
6. `scripts/phase4_external_observability_terminal_finalization_selftest_v0_1.py`
7. `src/phase4/paper_external_live_observability_v0_1.py`

No trading-critical implementation, accepted V0.5 harness, production
collector, source database, or runtime artifact was changed.

## Focused validation evidence

All commands used `.codex_venv\Scripts\python.exe` with
`PYTHONDONTWRITEBYTECODE=1`.

| Gate | Result | Evidence |
|---|---|---|
| T021 terminal-finalization self-test | PASS | 14/14; semantic digest `13f6fec1f11463e223d56367e2deb894d9823402293a059f0b603e5e05ded460`; normal finalizer `0.300318s`; busy retry `0.057484s` |
| T017 focused observability suite | PASS | 28/28; 2,000 rows; baseline `4.810214s`; enabled `4.908624s`; delta `0.098410s` / `2.045844%`; `407.446` enabled rows/s |
| T019 focused suite | PASS | 13/13; exact `704294` to `704295` regression; 100,000-row performance `0.148713s` / `672437.98` rows/s |
| V0.5 synthetic multi-hour self-test | PASS | model identities, duration/timer policy, V0.5 binding, atomic batch summary, and accepted-global restoration |
| Continuous Market Source V0.2 | PASS | all A-W bounded checks, including semantic equivalence and restart digest |
| Continuous FirstPullback Binding v0.1 | PASS | all A-BG checks; canonical digest `04641f83147b005d8822b7e3df500d6cfd581b2151243cdb921b4c4d09f5e549` |
| Changed Python AST parse | PASS | four changed/new Python files parsed with no bytecode writes |

The T021 synthetic wrapper writes a real isolated registry and V0.5-shaped JSON.
It proves the persisted JSON keeps every base field semantically identical and
adds an exact section containing run ID, intended/actual terminal state,
terminal outcome, final cursor/watermark, publication errors and metrics,
publisher/heartbeat status, registry path, model identities, JSON outcome/path,
and `passive_only = true`. Injected finalizer failure leaves the successful
trading classification unchanged, leaves a conservatively stale registry row,
and persists the exact failure reason. Injected atomic-replace failure leaves
the prior JSON byte-identical and surfaces the error in memory/CLI evidence.

## Protected-file hashes

Every path below is byte-identical to `HEAD` and retains its SHA-256:

| Protected path | SHA-256 |
|---|---|
| `src/phase4/paper_continuous_firstpullback_binding_v0_5.py` | `806f11af051edd8b2d8d0f07d5bae0129856f16fd01bbe435ca4612e813586ee` |
| `src/phase4/paper_continuous_market_source_v0_2.py` | `4a04043d86523018b9933b6a174e120999bc41d25edd35ed10a97118e793ed85` |
| `src/phase4/paper_continuous_runner_v0_2.py` | `27a42d3ca7e4cdaf878d1a707b43c2ddeb42bafa4c28d06b9cb5aa3f2e5116c3` |
| `src/phase4/paper_entry_execution_deadline_v0_1.py` | `b68c752b3a59ffca78fd363a632fba59f6a0e7aeaea7d9d1fb266cf0d0bbcf9f` |
| `src/phase4/paper_entry_router_v0_2.py` | `eac9a71f363fc4c62954f6a3eb1a39dc68fe12cc4bee6cc83de8500cdc6b3c8c` |
| `src/phase4/paper_exit_lifecycle_bridge_v0_3.py` | `aa7c576001eea67aed1573a9715913d8c481cc79bd152c6c78293b0364099e56` |
| `src/phase4/runtime_price_impact_v0_1.py` | `a063ff0bf10a3cba51f029f43687a4714fc5cfd2575964fcacd4c291073446d9` |
| `src/phase4/runtime_exit_price_impact_v0_1.py` | `1194f49027234ef865de2d66e156009078742b468ae0b58af5eb99d827d1a5f9` |
| `src/phase4/paper_cost_baseline_v0_1.py` | `41f0f9a7ef44f9f28a0a9802ad49c01c56ddd54082cc49db360ca5d8ef0bb57c` |
| `src/phase4/paper_cost_model_v0_2.py` | `f06dee50a33eb6d894d1f98aced01a1d1f69b680e8a0a6b2d712230b9a9e8d0b` |
| `src/phase4/paper_trade_accounting_v0_1.py` | `2530ede969c6bb532021113c23e54493ca4dac5a299d3e9383d08e417dc61445` |
| `scripts/live_pump_collector_v0_3_4.py` | `cf44a5af308f4e95d116585717aa4b9da80e1918a4ba20a4ed7f469eaee35aa1` |

## Pre-delivery adversarial review

The final implementation was reviewed line-by-line around publisher disable,
runtime return/exception classification, global restoration, independent
terminalization, JSON replacement, and CLI reporting. The review exercised:

- original publisher already closed and binding already torn down;
- TTL expired by ten seconds and finalization after delayed drain;
- heartbeat failure followed by successful fresh finalization;
- SQLite busy/locked release and bounded retry;
- duplicate/conflicting terminal attempts and heartbeat resurrection;
- wrong/missing exact run ID with two stale historical rows;
- multiple active rows and unchanged explicit ambiguity;
- final cursor regression, watermark-before-cursor, and backward lifecycle
  time rejection before mutation;
- FAILED and ABORTED terminal paths, including interrupt re-raise;
- registry reopen, exact final position, and read-only/query-only consumer;
- malformed JSON, atomic replace failure, and absence of partial temporary
  evidence;
- semantic digest, result-class, accounting, and protected-file
  non-interference; and
- bounded finalizer cost and unchanged cadence policy.

One additional implementation bug was found and fixed during this review. The
first version read and validated the RUNNING row before it had reserved the
SQLite writer, permitting a concurrent heartbeat to advance between validation
and update. The final version starts `BEGIN IMMEDIATE` before the row read, so
validation and transition occur under one write reservation. WAL mode is now
verified rather than changed before contract validation. The SQLite-busy test
was rerun after this correction. One test-fixture mismatch was also corrected:
the enabled digest fixture now uses the exact same frozen identity, timestamp,
and timer ID as its disabled baseline.

No remaining acceptance blocker was found.

## Remaining bounded risks

The registry and JSON artifact are separate files and cannot be committed in
one atomic transaction. A process crash after registry terminalization but
before JSON replacement can leave the correct terminal registry with an older
JSON lacking the wrapper section. Atomic replacement prevents partial JSON
from being accepted. A registry lock exceeding the five-second producer busy
timeout leaves the row conservatively stale and records the error in JSON when
that artifact remains writable. A JSON write failure is reportable through the
returned summary/CLI but, by definition, cannot create durable JSON evidence.
Neither limitation can mark a run active, mutate trading state, or convert a
trading PASS to FAIL.

## Final governance statement

No production/live validation was performed. No task file is staged or
committed. Expected baseline HEAD remains
`b0b158ce2355280eed0ad28ccb2616da0a2212d3` on `master`.
