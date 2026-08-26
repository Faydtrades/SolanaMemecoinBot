# Project State

## Project

Solana Memecoin Trading Bot

Canonical root: `D:\Tradingbot\solana_memecoin_bot_phase1_v0_1`

## Governance

ChatGPT project review is the acceptance authority. Codex is the implementation
and testing agent.

## Current accepted phase state

- Phase 0: COMPLETE
- Phase 1: COMPLETE
- Phase 2: COMPLETE
- Phase 3: COMPLETE
- Phase 4: through Phase 4.5C COMPLETE / PASS / ACCEPTED

No acceptance beyond Phase 4.5C is recorded here.

## Latest accepted live evidence

- Validation: Phase 4.5C v0.3 controlled live observability smoke
- Artifact: `data\paper\live_smoke\phase4_5c_live_observability_20260823T183559Z.json`
- Result: `PASS`
- Observability report digest:
  `4099b5055ad4b59ba56f3df2912bac868609e3381dda9e467a10071226cad803`

## Current production collector

`scripts\live_pump_collector_v0_3_4.py`

## Locked Phase-4 contracts

| Contract | Locked value |
|---|---|
| Cost baseline | `P4-COST-BASELINE-0001` |
| Cost fingerprint | `9652aeffc792eb48d62348360c6ae804f660e43329195d1f169219b80ccab67c` |
| Reference paper size | `100000000` lamports = `0.10 SOL` |
| Entry latency | `500ms` |
| Exit latency | `500ms` |
| Entry slippage rejection cap | `1500bps` |
| Exit slippage rejection cap | `2000bps` |
| Entry impact | `P4-RUNTIME-PRICE-IMPACT-0001` |
| Entry-impact fingerprint | `30d8880aa58d56f70f3baf235f4973a80c94b59724a5ee286b67f60be924d9e1` |
| Exit-spec fingerprint | `0bde5378f4ff2b63c6a96ad64140840ef8b2770c3be772a7aa71f8bdeb070129` |
| Exit impact | `P4-RUNTIME-EXIT-PRICE-IMPACT-0001` |
| Exit-impact fingerprint | `e1f1fd1c786cc679cf54f45c16b5d9a0c6aff37ed4132ac63c8abc5506aea59d` |
| Observability | `P4-RUNTIME-OBSERVABILITY-0001` |
| Observability fingerprint | `0c16799f2587957ea8b73aaa919d5213f15426a306cd44d58b10d4e08637ee43` |
| Live observability binding | `P4-LIVE-OBSERVABILITY-BINDING-0002` |

## Locked exit tracks

- `FINAL-A` / `E3_TP10_T15`: TP +10%, 15s fallback.
- `FINAL-B` / `E4_ACT10_GB03_T15`: activation +10%, giveback 3%, 15s
  fallback.
- `SENS-C` / `SENS_TP20_T5`: TP +20%, 5s fallback, SENSITIVITY ONLY.

These are alternative exit tracks and must not be summed as one portfolio.

## Research locks

- `DEV-FREEZE-0001` is immutable.
- `DEV-FREEZE-0002` is immutable.
- No further parameter search, candidate reselection, or exit tuning is
  authorized on the locked Phase-3 development baseline.
- Phase-3 cached-path deterministic SHA-256:
  `cabb205e255d7085a168ce5a97aac4231579bff1f0eace1f39112487a9cb757c`

## Latest accepted technical foundation

Continuous Paper Runner Foundation v0.1:

PASS / ACCEPTED

Continuous Paper Runner model:
`P4-CONTINUOUS-PAPER-RUNNER-0001`

Runner fingerprint:
`8dcab9d17f2ec71f4920a199b5745dae92dc3464a466860593231424e0a24dea`

Canonical runner digest:
`54b6a15f55635cc78145f147b9148d5ec12a7a47de380ba25e2c54392299752d`

Observability regression digest:
`88b6023737010f8e50395bcd1002af14a4f1c83e53347d23598facb80b91cde7`

The foundation uses the already validated:

- live CandidateSignal pipeline;
- causal entry fills;
- `FINAL-A` / `FINAL-B` / `SENS-C` exit lifecycle;
- realistic costs;
- runtime price impact;
- trade accounting;
- live MTM;
- MAE/MFE;
- per-track equity/drawdown;
- trade/skip persistence.

No entry or exit tuning is authorized.

LIVE CONTINUOUS PAPER RUN:
NOT YET VALIDATED / NOT YET ACCEPTED

No entry/exit tuning or reselection is authorized.

## Current technical state

Production Read-Only Market Source Adapter Foundation v0.1:

PASS / ACCEPTED

Model:
`P4-CONTINUOUS-MARKET-SOURCE-0001`

Fingerprint:
`47cdd010c76c3530f0d3d181e6545d9e50ce40508005a37eda850efe290dceda`

Self-test source identity:
`7b390d2cd1aa57cc1d3001875337c1072c0292ad9500e3078ed1a65fa9c4215f`

Canonical source digest:
`1302846d698410a49edcf65bc7134749f1a6508425db23f230aeb2299d70a84e`

The accepted market-source task remains unchanged. The bounded FirstPullback
binding foundation below is implemented pending project review; production and
continuous live paper remain unvalidated and unaccepted.

FIRSTPULLBACK LIVE BINDING:
BOUNDED FOUNDATION IMPLEMENTED_PENDING_PROJECT_REVIEW

LIVE CONTINUOUS PAPER RUN:
NOT YET VALIDATED / NOT YET ACCEPTED

No entry/exit tuning or reselection is authorized.

## Known follow-up requirements

1. A Phase-4-local exact compatibility layer for
   `GAP_RECONCILIATION_V0_3_4` is implemented pending project review. The
   accepted Phase-2 adapter remains unchanged.
2. `_eligible_launches_through()` rescans the growing post-anchor launch prefix
   during repeated polling. This accepted foundation limitation must be
   considered before long-running continuous live operation.

## Current compatibility task

Collector v0.3.4 Gap-Reconciliation Compatibility

PASS / ACCEPTED

Compatibility model:
`P4-PHASE1-GAP-SOURCE-COMPAT-0001`

Compatibility fingerprint:
`aabb765f81a29b2dd119607cdea92a2949dd3acec10aa204d246e6aa7a28a78c`

Exact alias:
`GAP_RECONCILIATION_V0_3_4` -> `GAP_RECONCILIATION_V0_3_3` ->
`IngestionSource.GAP_RECOVERY`

Post-compatibility market-source model:
`P4-CONTINUOUS-MARKET-SOURCE-0001`

Post-compatibility market-source fingerprint:
`242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb`

Compatibility self-test source identity:
`e86d96ec4a832844c6cf7d16bc367f1a799bb5eff4395c49eaefd98416cf1bc2`

Compatibility canonical digest:
`5f93f561bbef004f3acf8c42d5a38fea6fc731b7ce44318f89bd83379fcee028`

Market-source self-test source identity:
`0c65453be772a88bd015aaa2088e0095e66694348f60a749a724d1bc4a39a734`

Market-source canonical digest:
`1a574d84cc2cfa037daceda8af6f4e799ff09217fdb0caddc763873409a8a39d`

Accepted Phase-2 adapter:
UNCHANGED

Collector:
UNCHANGED

## Current FirstPullback continuous binding task

FirstPullback to Continuous Paper Runner Binding Foundation v0.1:

PASS / ACCEPTED

Binding model:
`P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0001`

Binding fingerprint:
`318e15b104c691821f5715b090c934e5b1cb3d9a8357fe0930d8825ef66d0daa`

Bound market-source fingerprint:
`242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb`

Bound gap-compatibility fingerprint:
`aabb765f81a29b2dd119607cdea92a2949dd3acec10aa204d246e6aa7a28a78c`

Bound continuous-runner fingerprint:
`8dcab9d17f2ec71f4920a199b5745dae92dc3464a466860593231424e0a24dea`

Locked EXP-0005 selection SHA-256:
`408657c1d6dc39435b61e01b368dac69796481864b7462efd150a2d4e1b0daa1`

Reported canonical binding digest:
`04641f83147b005d8822b7e3df500d6cfd581b2151243cdb921b4c4d09f5e549`

Reported timer-fence digest:
`12f92e1d1b64e0097f6b1f4fa466bc766e35833ecf0a65464e32e57b0fd47724`

Full strategy-evaluation audit contract:
EVERY ACTUAL MARKET-ROW / STRATEGY-CLOCK EVALUATION IS DURABLY RECORDED
EXACTLY ONCE THROUGH THE ACCEPTED `paper_strategy_evaluations` STORE

Source-watermarked timer contract:
PREPARE WITH BINDING-CAPTURED PRODUCTION `p1_rowid` WATERMARK; DRAIN THROUGH
THE FROZEN WATERMARK BEFORE STRATEGY OR EXIT CLOCK EFFECTS

Prepared-timer production-consumption fence:
THE EARLIEST `PREPARED` / `PLANNED` TIMER WATERMARK CAPS PRODUCTION PROCESSING;
ALL TIMERS AT THAT FENCE MUST COMPLETE BEFORE ANY LATER ROW IS CONSUMED

Accepted binding semantics:

- full FirstPullback strategy-evaluation audit exactly once;
- durable production cursor owned by the binding;
- deterministic skip-only advancement;
- replay-stable semantic outbox;
- immutable prepared strategy and exit timers;
- binding-captured production watermark;
- `PREPARED` / `PLANNED` timers fence production consumption;
- rows above the earliest timer fence cannot be processed;
- every timer at the same fence must be `COMPLETE` before release;
- restart restores the persisted active timer fence; and
- historical durable cursor greater than an incomplete timer watermark fails closed.

Known performance limitations:

- `_eligible_launches_through()` rescans the growing post-anchor launch prefix.
- Binding hydration performs one exact read-only row query per normalized
  production record.

Phase-2:
UNCHANGED

Collector:
UNCHANGED

Entry/exit parameters:
UNCHANGED / LOCKED

Production FirstPullback paper trading:
CONTROLLED BOUNDED SMOKE PASS / ACCEPTED

PRODUCTION FIRSTPULLBACK PAPER SMOKE:
PASS / ACCEPTED

Continuous live paper:
NOT YET VALIDATED / NOT YET ACCEPTED

No new phase acceptance is claimed.

FIRSTPULLBACK LIVE BINDING:
BOUNDED FOUNDATION PASS / ACCEPTED

CONTINUOUS LIVE PAPER:
NOT YET VALIDATED / NOT YET ACCEPTED

No strategy or exit tuning is authorized.

## Current controlled production FirstPullback paper smoke

MEME-P4-T009:
PASS / ACCEPTED / CHECKPOINTED

MEME-P4-T010:
PASS / ACCEPTED / CHECKPOINTED

Smoke model:
`P4-CONTINUOUS-FIRSTPULLBACK-PRODUCTION-SMOKE-0001`

Smoke fingerprint:
`0ad1cd2eff06399b93736d452859bab4ea081ee84d8ac287435e4e4b35c6311b`

Run start UTC:
`2026-08-24T13:53:52.299461+00:00`

Run end UTC:
`2026-08-24T13:58:53.876997+00:00`

Session anchor / final durable production cursor:
`576100` / `576600`

Result class:
`PASS_FULL_TRADE`

Accepted production evidence:

- 500 raw production rows
- 76 normalized records
- 424 deterministic skips
- 3 fresh launches
- 225 strategy evaluations
- 1 genuine `CONTROL` candidate
- 3 alternative paper tracks `CLOSED`
- 0 conflicts
- 0 retries
- 0 SQLite busy/locked errors

Paper database:
`data\paper\live_smoke\phase4_firstpullback_production_smoke_20260824T135352_299461Z.sqlite3`

Paper database SHA-256:
`9353f633fec770a11ba0d601865f424a9a19c1e320e9380c7a4c0b2a25b8ce89`

JSON summary:
`data\paper\live_smoke\phase4_firstpullback_production_smoke_20260824T135352_299461Z.json`

JSON summary SHA-256:
`3256ac0d8d60208ebfa00527594550ac11e031bf985e3ad716c7de1201d9a81a`

Collector log SHA-256:
`3a07d1e4cbc94e8fbfb37a82b5db6111b844d22be3fb7ea34505814b428bfae0`

This is a five-minute bounded causal/runtime smoke only. It makes no
profitability claim and does not validate or accept unbounded continuous live
paper operation.

UNBOUNDED CONTINUOUS LIVE PAPER:
NOT YET VALIDATED / NOT YET ACCEPTED

## Current bounded multi-hour harness task

MEME-P4-T011:
BLOCKED_MULTI_HOUR_HARNESS_REQUIRED

This was the expected safe stop because the accepted T009 harness has a
20-minute maximum, candidate-based early stop, and a 500-event managed
collector target. It was not a failure of the accepted runtime.

MEME-P4-T011-H1:
PASS / ACCEPTED / CHECKPOINTED

MEME-P4-T011-H1-C1:
PASS / ACCEPTED / CHECKPOINTED

MEME-P4-T011-H1-C2:
PASS / ACCEPTED / CHECKPOINTED

MEME-P4-T011-H1-C3:
PASS / ACCEPTED / CHECKPOINTED

Multi-hour model:
`P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0001`

Multi-hour fingerprint:
`e7cfb5049d93728391decf633a245585b49d7c4ea8e256bca8c98b53ccb5da47`

Pre-correction fingerprint:
`b211c563d3b66de6a947d76f5630a6798a6c85629a5d7d678967553526d4a5a1`

Pre-correction fingerprint status:
PRE-ACCEPTANCE / SUPERSEDED / NOT CHECKPOINTED

Run-control contract:

- default duration: 14,400 seconds (four hours)
- accepted range: 3,600 through 21,600 seconds
- requested duration bound into exact runtime session identity
- no candidate-based early stop
- multiple natural candidates and trades are allowed through the full
  requested duration
- `FINAL-A`, `FINAL-B`, and `SENS-C` remain separate alternatives
- `SENS-C` remains `SENSITIVITY ONLY`

Production-source continuity contract:

- `SOURCE_STALL_FAIL_SECONDS = 300.0` monotonic seconds
- only latest production `p1_rowid` advancement resets source activity
- collector process liveness alone is insufficient
- timers and binding cursor movement do not reset source activity
- both existing and managed collector modes use the same watchdog
- `PRODUCTION_SOURCE_STALLED` stops processing fail-closed before duration
- `PASS_MULTI_HOUR` requires continuity through the duration boundary
- maximum stall and final source staleness must remain below 300 seconds
- the `>=300` threshold is enforced before a newly advanced row can mutate
  watchdog state
- a late row cannot increment advance evidence, reset the activity clock, or
  restore terminal continuity
- source-stall shutdown preserves the already durable cursor without
  re-reading and admitting a rejected late row
- a terminal source stall cannot recover

Runtime artifact ignore policy:

- tracked root `.gitignore` owns `data/paper/multihour/`
- runtime creates no nested `.gitignore` or other Git metadata

Duration-boundary contract:

- the duration branch captures one boundary UTC instant and immediately
  prepares the final timer
- the prepared timer's persisted source watermark is the sole authoritative
  end watermark
- no separate source pre-read is compared with or redefines that watermark
- final draining remains fenced through the prepared watermark
- later production rows are not admitted and do not alter bounded evidence or
  restart state
- the final durable cursor must equal the prepared watermark and pending
  semantic outbox work must be zero
- postrun integrity and exact-state restart checks are required

Managed collector lifecycle:

- accepted v0.3.4 unlimited mode (`max_pump_events=None`)
- harness-owned child process group and PID only
- graceful owned-process interrupt and accepted queue drain
- 180-second cleanup allowance before owned-child-only forced fallback
- exact known post-cleanup summary-label `KeyError` handling only
- child absence verified through the owned process handle

This task implemented and isolated-self-tested the harness only. The four-hour
production run has not occurred. No profitability claim is made.

FOUR-HOUR PRODUCTION PAPER RUN:
NOT YET EXECUTED

UNBOUNDED CONTINUOUS LIVE PAPER:
NOT YET VALIDATED / NOT YET ACCEPTED

## Current track-safe late-entry correction task

MEME-P4-T012:
PASS / ACCEPTED / CHECKPOINTED

FAILED BOUNDED V0.1 LIVE RUN:
FAIL / RETAINED AS REGRESSION EVIDENCE

Reason: `InvalidTransition` caused by an approximately 36.512-second late
entry after the signal-anchored exit deadlines. Its relevant causal ordering
was:

- CandidateSignal: `2026-08-24T20:28:07.275108Z`
- late paper entry fill/open: `2026-08-24T20:28:43.787124Z`
- delay: approximately 36.512 seconds
- failure: lifecycle correctly rejected an exit transition whose effective
  time preceded the position OPEN transition

The failed v0.1 SQLite/JSON/log artifacts remain unchanged.

Corrected V0.2 behavior: the late entry becomes deterministic execution expiry
rather than a backward lifecycle transition.

Entry execution deadline model:
`P4-PAPER-ENTRY-EXECUTION-DEADLINE-0001`

Entry execution deadline fingerprint:
`6e2ae931d7270012dc025ef9a509ec30a240a850ea0d1c2666560a5e5506c9a7`

Locked entry viability derived from the CandidateSignal-anchored exit contract:

- FINAL-A: last eligible entry fill = CandidateSignal +15s inclusive;
  expiry = +15s +1us
- FINAL-B: last eligible entry fill = CandidateSignal +15s inclusive;
  expiry = +15s +1us
- SENS-C: last eligible entry fill = CandidateSignal +5s inclusive;
  expiry = +5s +1us; SENSITIVITY ONLY

Execution expiry reason:
`ENTRY_EXECUTION_DEADLINE_EXPIRED`

All-track route reason:
`ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED`

Continuous runner v0.2 model:
`P4-CONTINUOUS-PAPER-RUNNER-0002`

Continuous runner v0.2 fingerprint:
`66725275f7e01e510b369df08281be9769d057113896f60f2ac652c2efe072fb`

Continuous FirstPullback binding v0.2 model:
`P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0002`

Continuous FirstPullback binding v0.2 fingerprint:
`c2f90d610ab774831af6a52b4a24e948b4bf0727d69457fee63311b5a159feca`

Corrected multi-hour model:
`P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0002`

Corrected multi-hour fingerprint:
`9002ed30ecd74722af5a37efec437143a17c4865f94ae37a60863fbc2320a7d7`

The locked exit fallback clocks remain anchored to CandidateSignal and were
not moved to entry fill. The late-entry deadline is execution viability, not
strategy tuning. Tracks expire independently, partial-track candidate
execution is supported, silent-market expiry is deterministic, deadline state
is restart/replay safe, and exact-boundary fills remain eligible. The runtime
fabricates no observations or fills; execution-expired tracks receive no entry
cost; cross-track PnL aggregation remains prohibited; and accepted V01
implementations remain immutable. Phase-2 strategy semantics, Phase-3
finalists/exits, costs, impact, accounting, observability, source, and collector
behavior remain unchanged. There was no parameter tuning or reselection.

FOUR-HOUR V0.2 PRODUCTION PAPER RUN:
NOT YET EXECUTED

UNBOUNDED CONTINUOUS LIVE PAPER:
NOT YET VALIDATED / NOT YET ACCEPTED

No profitability claim is made. No production/live validation was performed
for MEME-P4-T012.

## Current incremental source and continuity correction task

MEME-P4-T014:
PASS / ACCEPTED / CHECKPOINTED

Market Source V0.2 model:
`P4-CONTINUOUS-MARKET-SOURCE-0002`

Market Source V0.2 fingerprint:
`9cb094f52bf4b4fe28dc4828b1d9a52a3350cde664c7da5a489fa84e9a4085a9`

Continuous FirstPullback Binding V0.3 model:
`P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0003`

Continuous FirstPullback Binding V0.3 fingerprint:
`45521225ccbe6c3b98a48b7e92523deea16d6ff9b91fdd37a415a1a45958babf`

Corrected Multi-Hour V0.3 model:
`P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0003`

Corrected Multi-Hour V0.3 fingerprint:
`6fb400b77606cd1ef3fce32037c95f4ea1cd7299a71ee6c902e43eb2114917bb`

V0.2 FAILED BOUNDED RUN:
FAIL / RETAINED IMMUTABLE FAILURE EVIDENCE

Reported reason:
`PRODUCTION_SOURCE_STALLED`

Postmortem:
FALSE SOURCE-STALL CLASSIFICATION

Production evidence:
row 602267 to row 602268 advanced in approximately 3.516 seconds while
production and WebSocket observations continued. The actual operational issue
was runner/source-probe starvation, not production-source inactivity. All
V0.2 paper integrity and exact-state restart checks remained healthy. The old
runtime artifacts are unchanged.

V0.3 separates incremental `inserted_at_utc` source evidence from monotonic
runner probe intervals. Both use the locked `>=300.0s` fail-closed boundary;
actual `PRODUCTION_SOURCE_STALLED` evidence has precedence over
`RUNNER_SOURCE_POLL_STARVED`. Market Source V0.2 replaces repeated growing
launch-prefix reconstruction with a non-authoritative, failure-atomic,
rebuildable in-memory cache. The durable binding cursor remains authoritative.

T012 late-entry fix:
REMAINS ACCEPTED / CHECKPOINTED

V0.3 multi-hour:
NOT YET LIVE VALIDATED

FOUR-HOUR V0.3 PRODUCTION PAPER RUN:
NOT YET EXECUTED

UNBOUNDED CONTINUOUS LIVE PAPER:
NOT YET VALIDATED / NOT YET ACCEPTED

No strategy, entry/exit, cost, latency, impact, trade-size, accounting,
observability, finalist, or collector behavior was changed. No parameter
tuning or reselection occurred. No profitability claim is made.

## Current timer-fence throughput hardening task

MEME-P4-T015:
PASS / ACCEPTED / CHECKPOINTED

Accepted predecessor:
MEME-P4-T014 PASS / ACCEPTED / CHECKPOINTED

V0.3 FAILED BOUNDED RUN:

- requested: 14,400 seconds
- actual: 1,421.637045 seconds
- stop: `RUNNER_SOURCE_POLL_STARVED`
- production source: HEALTHY
- maximum actual source gap: 26.012124 seconds
- maximum runner probe interval: 950.25 seconds
- timer-fence interval: 1,367.2016976 seconds
- V0.3 integrity/restart: PASS

The accepted Market Source V0.2 launch cache remained healthy and is retained
unchanged. Controlled profiling proved that V0.3's timer-fence metric mostly
contained active per-row strategy/audit/outbox/cursor processing inside one
monolithic drain call. V0.4 makes the fixed-watermark drain binding-owned,
probes accepted T014 health evidence at batch progress boundaries, reports
active work separately from passive/source-unavailable waits, and batches
semantic-equivalent evaluation delivery per source input.

Continuous FirstPullback Binding V0.4 model:
`P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0004`

Binding V0.4 fingerprint:
`e2c35ce63fe419ac4a934913dd26249d72411884cb115e0701f601b276b0f7d6`

Continuous FirstPullback Multi-Hour Run V0.4 model:
`P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0004`

Multi-Hour V0.4 fingerprint:
`ce690a8fa864516fcb6fe7c834e4a4342655a9c01c80f522dc0ba9ba3673e0ad`

Isolated 20,000-row synthetic throughput:

- 131.81 raw rows/second
- 32.95 normalized rows/second
- five fixed-watermark timers
- zero passive fence wait
- maximum cursor lag returned to zero
- no runner or production-source starvation

V0.4:
NOT YET LIVE VALIDATED

FOUR-HOUR V0.4 PRODUCTION PAPER RUN:
NOT YET EXECUTED

UNBOUNDED CONTINUOUS LIVE PAPER:
NOT YET VALIDATED / NOT YET ACCEPTED

No strategy, deadline, entry/exit, cost, latency, impact, trade-size,
accounting, observability-PnL, source, finalist, or collector semantics were
changed. No parameter tuning or reselection occurred. No profitability claim
is made.

## Current production fence-drain throughput correction task

MEME-P4-T016:
IMPLEMENTED_PENDING_PROJECT_REVIEW

V0.4 FAILED ONE-HOUR PRODUCTION PAPER RUN:
FAIL / MANUALLY INTERRUPTED / RETAINED AS IMMUTABLE EVIDENCE

Observed V0.4 final drain:

- previous complete timer watermark: `625704`
- frozen final watermark: `635181`
- durable cursor sample A: `628586`
- durable cursor after 60 seconds: `628711`
- measured live drain: approximately 2.08 P1 rowids/second
- projected remaining drain: approximately 51.8 minutes

Read-only profiling of a copied preserved runtime slice identified repeated
FULL-synchronous SQLite transaction finalization as the dominant production-
shaped hot path. The 125-row copied batch took 8.9082245 seconds at 14.03198
raw rows/second; SQLite connection-context finalization consumed 7.573 seconds
(85.0%), while source fetch consumed 0.0347168 seconds and hydration consumed
0.4313847 seconds.

Continuous FirstPullback Binding V0.5 model:
`P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0005`

Binding V0.5 fingerprint:
`f66acb5926f3dd77d68c086d74e1bc07b43fad4307071c1ec8c3b7d1111660b7`

Continuous FirstPullback Multi-Hour Run V0.5 model:
`P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0005`

Multi-Hour V0.5 fingerprint:
`7e439eb90ca0d44467501ae15008ecc4c733582dc14a04afae9a94ac9ab9f3ca`

V0.5 retains WAL plus `synchronous=FULL` and commits each fetched source batch
as one atomic paper transaction. A failed batch rolls back its semantic work
and cursor together to the prior durable position; the failed instance becomes
terminal and restart deterministically replays from SQLite.

Deterministic production-shaped regression:

- 250 raw source rows
- 700 V0.4 physical commit boundaries
- 13.08649 V0.4 raw rows/second under deterministic commit latency
- one V0.5 source-batch commit
- six V0.5 total boundaries including timer finalization
- 530.12049 V0.5 raw rows/second
- locked minimum: 15 raw rows/second
- exact frozen watermark / final durable cursor: `250` / `250`
- no lost, duplicated, reordered, or pre-timer `W+1` rows
- semantic rows equivalent to accepted V0.4
- exact reopen bytes unchanged
- first-/later-batch rollback, restart, and commit-failure replay: PASS
- SQLite quick check: `ok`

V0.5:
NOT YET LIVE VALIDATED

FOUR-HOUR V0.5 PRODUCTION PAPER RUN:
NOT YET EXECUTED

UNBOUNDED CONTINUOUS LIVE PAPER:
NOT YET VALIDATED / NOT YET ACCEPTED

No strategy, deadline, entry/exit, cost, latency, impact, trade-size,
accounting, observability-PnL, source, finalist, or collector semantics were
changed. No parameter tuning or reselection occurred. No profitability claim
is made. No new production/live run was performed by T016.

## Current external read-only live observability task

MEME-P4-T017:
PASS / ACCEPTED / CHECKPOINTED

External live observability contract model:
`P4-EXTERNAL-LIVE-OBSERVABILITY-0001`

External live observability contract version:
`phase4_external_live_observability_v0.1`

External live observability contract fingerprint:
`058135c958bbddff241d50cfa50ac5e6b9fe85655f4beabe2b9c033aed9267ac`

Passive V0.5 integration model:
`P4-CONTINUOUS-FIRSTPULLBACK-EXTERNAL-OBSERVABILITY-0001`

Passive V0.5 integration fingerprint:
`d4f1d5d3277ffdd156a1da323c12f58c9522349431552d3c435de8d179a9a5f4`

The producer-owned SQLite registry publishes one complete atomic row per
unique run ID. RUNNING is valid only while the explicit producer heartbeat
interval remains valid; stale RUNNING rows are inactive. The same row binds
the actual read-only source SQLite locator, source identity, anchor, durable
cursor, current/frozen watermark, stable P1 rowid/ingest identity, and price
representation version.

The external consumer is read-only/query-only and receives no execution or
control path. Publication is cadence-bounded and passive; a publication fault
does not change accepted V0.5 trading behavior and the prior liveness proof
expires conservatively.

The deterministic T017 gate proves exact enabled/disabled V0.5 paper-runtime
digest equivalence, restart/replay consistency, terminal irreversibility,
source non-mutation, no-secret fields, concurrent atomic reads, explicit
multiple-active ambiguity, and bounded publication overhead.

EXTERNAL READ-ONLY LIVE OBSERVABILITY CONTRACT V0.1:
IMPLEMENTED_PENDING_PROJECT_REVIEW

V0.5 TRADING / STRATEGY / ACCOUNTING SEMANTICS:
UNCHANGED

NO PRODUCTION/LIVE VALIDATION PERFORMED

## Current non-monotonic source-activity robustness task

MEME-P4-T019:
IMPLEMENTED_PENDING_PROJECT_REVIEW

The formal V0.5 plus T017 run failed closed after `95.225099` seconds because
the inherited continuity watchdog treated `inserted_at_utc` as a strict order
key. Read-only evidence proved increasing P1 rowids `704294` and `704295` had
ordinary live WebSocket timestamps that regressed by `0.158253` seconds.

Corrected V0.5 multi-hour model:
`P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0005`

Corrected V0.5 fingerprint:
`0308899e0a2306c921603f3beff4a957e01832ce180a397b88841ba498fab5e4`

Canonical durable source ordering remains strictly increasing P1 SQLite
rowid. `inserted_at_utc` remains validated UTC-aware metadata, while source
health uses the maximum observed activity timestamp so a newer row cannot move
health time backward.

External observability contract fingerprint remains:
`058135c958bbddff241d50cfa50ac5e6b9fe85655f4beabe2b9c033aed9267ac`

Candidate refreshed passive integration fingerprint:
`f9d782a26a278779cd97c1b405555817fd7169ab6fb1729135d5226ca52bdfbd`

No strategy, entry/exit, cost, latency, impact, accounting, portfolio, source
database access, collector, or T017 control-plane semantics changed. No
production/live validation was performed by T019; only the known two rows were
reopened read-only for bounded reproduction evidence.

## Current external observability terminal-finalization correction

MEME-P4-T021:
IMPLEMENTED_PENDING_PROJECT_REVIEW

The project-accepted formal six-hour V0.5 paper run on canonical commit
`b0b158ce2355280eed0ad28ccb2616da0a2212d3` remains:

PHASE-4 TRADING RUNTIME:
PASS / ACCEPTED / FROZEN

FORMAL SIX-HOUR V0.5 PAPER RUN:
PASS / `PASS_MULTI_HOUR`

- requested duration: `21600` seconds
- actual duration: `21643.959842` seconds
- anchor: `704295`
- final durable cursor / watermark: `766687 / 766687`
- raw source rows: `62392`
- candidates: `81`
- graceful shutdown: `true`
- errors: `[]`
- integrity / source continuity: `true / true`
- conflicts / retries / SQLite busy: `0 / 0 / 0`

The run's external-observability row
`95aebf92-3353-4404-bb03-82e776218740` expired safely by TTL but remained
persisted as `RUNNING` with `ended_at_utc = NULL`. T021 adds a fresh,
producer-owned exact-run-ID terminal finalizer that is independent of the
closed heartbeat publisher and paper binding. Returned V0.5 summary positions
are authoritative, and final wrapper evidence is atomically added to the same
V0.5 JSON artifact.

External live observability contract fingerprint remains:
`058135c958bbddff241d50cfa50ac5e6b9fe85655f4beabe2b9c033aed9267ac`

Refreshed passive integration fingerprint:
`6ac56972dd192e58f1c042768303fecb7a7e5d20d146ee3b7271190459d8fda4`

The registry and JSON are separate durability domains; a process crash after
the registry terminal commit but before JSON replacement can leave terminal
registry evidence with the older JSON. This cannot create active liveness or
change the trading result. No trading, source-ordering, strategy, entry/exit,
cost, impact, accounting, collector, wallet, or execution semantics changed.

NO PRODUCTION/LIVE VALIDATION PERFORMED BY T021

## Codex takeover state

- MEME-TAKEOVER-001: PASS / ACCEPTED
- MEME-TAKEOVER-002: PASS / ACCEPTED
- MEME-TAKEOVER-003: PASS / ACCEPTED
- MEME-TAKEOVER-004: PASS / ACCEPTED / CHECKPOINTED
- MEME-TAKEOVER-005: PASS / ACCEPTED / CHECKPOINTED
- MEME-TAKEOVER-006: PASS / ACCEPTED / CHECKPOINTED
- MEME-TAKEOVER-007: PASS / ACCEPTED / CHECKPOINTED
- MEME-TAKEOVER-008: PASS / ACCEPTED / CHECKPOINTED
- CODEX TAKEOVER: COMPLETE
- MEME-P4-T001: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T001-C1: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T001-C2: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T002: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T003: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T003-C1: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T004: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T005: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T006: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T007: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T007-C1: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T007-C2: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T008: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T009: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T010: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T011: BLOCKED_MULTI_HOUR_HARNESS_REQUIRED
- MEME-P4-T011-H1: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T011-H1-C1: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T011-H1-C2: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T011-H1-C3: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T012: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T014: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T015: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T016: IMPLEMENTED_PENDING_PROJECT_REVIEW
- MEME-P4-T017: PASS / ACCEPTED / CHECKPOINTED
- MEME-P4-T019: IMPLEMENTED_PENDING_PROJECT_REVIEW
- MEME-P4-T021: IMPLEMENTED_PENDING_PROJECT_REVIEW

## Canonical Codex test runtime

`.codex_venv\Scripts\python.exe`

This is the default Codex test interpreter for future bounded tasks unless a
future task explicitly changes it.

## Previous canonical technical baseline

- Commit: `321675b53d678e4734c99a3905ba10dd5862f764`
- Checkpoint message: `checkpoint: establish memecoin canonical baseline through phase4.5c`
