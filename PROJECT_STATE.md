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
CHECKPOINT_PENDING

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
- MEME-P4-T010: CHECKPOINT_PENDING

## Canonical Codex test runtime

`.codex_venv\Scripts\python.exe`

This is the default Codex test interpreter for future bounded tasks unless a
future task explicitly changes it.

## Previous canonical technical baseline

- Commit: `321675b53d678e4734c99a3905ba10dd5862f764`
- Checkpoint message: `checkpoint: establish memecoin canonical baseline through phase4.5c`
