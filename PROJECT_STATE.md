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

## Current technical task

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

Next technical area:
production read-only continuous source adapter / live binding foundation

No entry/exit tuning or reselection is authorized.

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
- MEME-P4-T002: CHECKPOINT_PENDING

## Canonical Codex test runtime

`.codex_venv\Scripts\python.exe`

This is the default Codex test interpreter for future bounded tasks unless a
future task explicitly changes it.

## Previous canonical technical baseline

- Commit: `321675b53d678e4734c99a3905ba10dd5862f764`
- Checkpoint message: `checkpoint: establish memecoin canonical baseline through phase4.5c`
