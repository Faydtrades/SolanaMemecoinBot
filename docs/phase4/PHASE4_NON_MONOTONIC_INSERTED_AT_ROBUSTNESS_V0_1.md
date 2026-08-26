# Phase-4 Non-Monotonic `inserted_at_utc` Robustness v0.1

## Status

`MEME-P4-T019`: `IMPLEMENTED_PENDING_PROJECT_REVIEW`

No production/live validation was performed. The known production rows were
inspected through SQLite `mode=ro` with `query_only=1` only.

## Failure and root cause

The failed V0.5 run encountered increasing P1 rowids `704294`, `704295` with
ordinary live WebSocket timestamps `2026-08-26T00:52:19.677782+00:00` and
`2026-08-26T00:52:19.519529+00:00`. The second timestamp regressed by
`0.158253` seconds.

The market source already fetched continuity evidence in ascending P1 SQLite
rowid order. The inherited V0.3 watchdog then incorrectly treated
`inserted_at_utc` as another strict ordering key and raised `BindingConflict`.
Repository-wide inspection found that single strict timestamp-order rejection;
the source reader validates and returns timestamps but does not order by them.

## Corrected contract

Canonical durable order remains strictly increasing P1 SQLite rowid. A rowid
rewind, duplicate/non-increasing health row, missing bounded interval, or
interval that does not reach the reported latest row still fails closed.

`inserted_at_utc` remains required, parseable, timezone-aware UTC metadata. A
future timestamp relative to its health probe remains invalid. For health only,
effective activity is now:

```text
max(previous effective activity, session start, current inserted_at_utc)
```

This accepts legitimate timestamp regressions without moving effective source
activity backward or masking a genuine interval/final-staleness failure. It
does not replace rowid ordering with slot, event time, decoded time, or wall
clock.

## Identities

- V0.5 model: `P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0005`
- corrected V0.5 fingerprint:
  `0308899e0a2306c921603f3beff4a957e01832ce180a397b88841ba498fab5e4`
- binding model/fingerprint unchanged:
  `P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0005` /
  `f66acb5926f3dd77d68c086d74e1bc07b43fad4307071c1ec8c3b7d1111660b7`
- T017 contract fingerprint unchanged:
  `058135c958bbddff241d50cfa50ac5e6b9fe85655f4beabe2b9c033aed9267ac`
- refreshed passive T017 integration fingerprint:
  `f9d782a26a278779cd97c1b405555817fd7169ab6fb1729135d5226ca52bdfbd`

The T017 integration refresh is limited to its locked corrected V0.5 file hash
and runtime fingerprint. Its producer/consumer schema, liveness semantics,
control-plane isolation, failure policy, and trading-digest equivalence are
unchanged. A 1,000-row direct-versus-observed probe produced the identical
paper-runtime digest
`d217b5715ec3e2de6529c2af428fae873a71e3930b7e7d4482bcc66440555bdb`
on both paths, with cursor `1000`, 250 normalized rows, a committed timer, and
zero pending outbox work.

## Focused deterministic evidence

`scripts/phase4_non_monotonic_inserted_at_selftest_v0_1.py` covers:

- the exact `704294` to `704295` timestamp regression;
- a 30-day regression while preserving rowid order;
- equal and normally increasing timestamps;
- rowid rewind, duplicate, and out-of-order failure;
- malformed, timezone-naive, and future timestamp failure;
- regression exactly across two continuity batches;
- two independent paper replays with identical durable cursor, paper digest,
  and watchdog digest;
- health activity that cannot move backward or fabricate a stall;
- the `299.999`/`300.000` second staleness boundary and terminal failure replay;
- multiple regressions plus legitimate rowid gaps; and
- a 100,000-row alternating-timestamp linear throughput probe.

Latest focused acceptance run: `13/13`, `RESULT: PASS`. The measured 100,000-row
watchdog probe exceeded `900,000` rows/second on the Codex test runtime, above
the deterministic `20,000` rows/second guard. The replay cursor was `40`, with
paper digest
`2f7fc6ecaf8c388561539ee7c073463cf6ca2820fe0a5814978179bada1a3bd4`
and watchdog digest
`c3d010d56e468e8e7751350dd1a311d609a5636bd92c8b4cae0c873a95f9d407`.

The read-only production proof returned `query_only=1`, the two expected live
WebSocket rows, final accepted health rowid `704295`, effective activity equal
to the maximum timestamp at row `704294`, and `actual_source_stalled=False` at
the original causal probe time.

## Regression gates

All commands used `.codex_venv\Scripts\python.exe` with
`PYTHONDONTWRITEBYTECODE=1` and exited `0` with `RESULT: PASS`:

- `scripts/phase4_non_monotonic_inserted_at_selftest_v0_1.py` (`13/13`);
- `scripts/phase4_continuous_firstpullback_multihour_run_selftest_v0_5.py`;
- `scripts/phase4_production_fence_drain_throughput_selftest_v0_1.py`;
- `scripts/phase4_continuous_market_source_selftest_v0_2.py`;
- `scripts/phase4_continuous_firstpullback_multihour_run_selftest_v0_3.py`;
- `scripts/phase4_continuous_firstpullback_multihour_run_selftest_v0_4.py`;
- `scripts/phase4_timer_fence_throughput_selftest_v0_1.py`;
- `scripts/phase4_continuous_firstpullback_binding_selftest_v0_1.py`;
- `scripts/phase4_late_entry_execution_deadline_selftest_v0_1.py`; and
- `scripts/phase4_external_live_observability_selftest_v0_1.py` (`28/28`).

The production-shaped V0.5 gate retained exact V0.4 semantic-row equivalence,
cursor/watermark `250`/`250`, whole-batch rollback/restart determinism, SQLite
integrity, and measured `617.249` raw rows/second against the locked `15`
rows/second minimum. The 20,000-row timer-fence gate measured `117.312` raw
rows/second and returned cursor lag to zero.

## Preserved semantics

No strategy, signal, candidate, entry deadline, entry/exit execution, latency,
cost, impact, sizing, accounting, portfolio, source database access, timer,
anchor, durable cursor, watermark, replay, source identity, collector, or
observability control-plane behavior was changed. `FINAL-A`, `FINAL-B`, and
`SENS-C` remain separate alternative tracks.

Pre/post SHA-256 comparison was exact for every protected implementation below:

| Protected path | Unchanged SHA-256 |
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
| `src/phase4/paper_external_live_observability_v0_1.py` | `5d0cf1f11f116e0a0e2c39417a917ee93e0e96d4addb61784c228ff938ed424a` |
| `scripts/live_pump_collector_v0_3_4.py` | `cf44a5af308f4e95d116585717aa4b9da80e1918a4ba20a4ed7f469eaee35aa1` |

## Pre-delivery adversarial review

The final state transitions and classification branches were reviewed
line-by-line against the inherited V0.3 watchdog. The review exercised multiple
regressions in one batch, exact cross-batch ordering, a much older late value,
equal timestamps, rowid gaps, rowid rewind/duplicate/out-of-order evidence,
malformed/naive/future timestamps, validation-before-publication, exact
`299.999`/`300.000` staleness, terminal failure replay, two independent replays,
exact-state reopen, T017 wrapping/restoration, and frequent-regression linear
performance.

No additional implementation defect was found. The review added stronger
state-atomic assertions for malformed, timezone-naive, future, duplicate, and
out-of-order evidence, plus exact staleness-boundary, terminal-irreversibility,
and exact-reopen coverage. The initial test assertion was corrected to compare
watchdog state rather than read-only source probe counters; that was a test-only
issue, not a runtime change.

Remaining known risk: a sustained producer-clock regression that never reaches
a new maximum for at least 300 seconds remains a fail-closed source-health
condition. This is intentional preservation of accepted staleness policy, not
timestamp ordering. No multi-hour production validation has yet been performed
on the corrected candidate.

## Validation boundary

Validation is isolated and deterministic except for the explicitly bounded
read-only proof of rows `704294`-`704295`. No collector, RPC/network access,
wallet, signing, live order, new paper session, or multi-hour run was started.
No parameter tuning or finalist reselection occurred. No profitability claim is
made.

ChatGPT project review remains the acceptance authority.
