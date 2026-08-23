TRADINGBOT PHASE 3.1 PATCH v0.1.1

Extract this ZIP directly into the existing project root:

D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Allow Windows to merge src\ and scripts\.

This patch DOES NOT replace or delete existing Phase-2 files.
It adds:
- SCLOCK-0.2 compatibility for FirstPullback v1.1 / V02
- OutcomeReplay v0.1.1
- collector coverage snapshot support
- Phase2->Phase3 candidate/outcome adapter
- selftests
- coverage exporter
- real-data EXP-0001 harness

Recommended sequence after extraction:

1)
python scripts\phase3_outcome_replay_selftest_v0_1_1.py

2)
python scripts\phase3_strategy_clock_v0_2_selftest.py

3) Stop the live collector before this step, then:
python scripts\phase3_collector_coverage_export_v0_1.py

This writes:
data\research\phase3\collector_coverage_DEV-FREEZE-0001_v0_1.json

4) Do NOT run EXP-0001 until the coverage export reports RESULT: PASS.
Then the real-data harness is:
python scripts\phase3_realdata_outcome_harness_v0_1.py

The real-data harness does not optimize exits and does not model fees,
slippage, latency, execution failures, or realistic-net PnL.
