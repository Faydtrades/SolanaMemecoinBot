TRADINGBOT — EXP-0007 QUEUE-DRAIN RECOVERY v0.1

WHY
---
The long collector shutdown was interrupted during drain. Read-only diagnostic
showed a very large persistent backlog:
- confirmation_jobs: ~309k active
- deep_enrichment_jobs: ~2.8k active
- gap_jobs: 0

This tool deliberately DOES NOT start the WebSocket listener and DOES NOT
create a new collector session. It imports the existing production
live_pump_collector_v0_3_3.py workers and lets only the persistent
confirmation/deep/gap workers run.

It is NOT read-only:
- confirmation statuses are resolved in the existing DB
- deep launch enrichment is completed
- restart-safe queue states are updated
- if a gap job existed, the normal v0.3.3 gap worker could recover its events

It does NOT alter strategy parameters or run research/outcome logic.

RUN
---
From:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Command:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_queue_drain_recovery_v0_1.py --confirm-drain --timeout-seconds 7200

IMPORTANT:
- Do not start the normal live collector at the same time.
- Do not press Ctrl+C just because it takes a while.
- Progress is printed every 30 seconds.
- If RESULT: PASS, all three persistent queues are zero.
- If RESULT: CHECK, send the final queue counts before doing anything else.
- Do NOT build DEV-FREEZE-0002 until the queue state and uncertain session tail
  have both been handled.
