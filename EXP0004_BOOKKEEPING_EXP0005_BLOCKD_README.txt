TRADINGBOT — EXP-0004 BOOKKEEPING + EXP-0005 BLOCK D v0.1

Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Run with (.venv) active, in this exact order:

1) Canonicalize EXP-0004 and create the separate deferred-hypothesis ledger:
python scripts\phase3_exp0004_bookkeeping_v0_1.py

Expected:
EXP-0004 COMPLETE
4 carry-forward configs
2 deferred hypotheses retained
next experiment ID EXP-0005
DEV-FREEZE unchanged
RESULT: PASS

2) Validate EXP-0005 design:
python scripts\phase3_reclaim_runaway_block_selftest_v0_1.py

Expected:
4 A+B+C x 24 valid D pairs = 96
RESULT: PASS

3) Run Block D:
python scripts\phase3_exp0005_reclaim_runaway_harness_v0_1.py

Block D varies only:
- reclaim min_extension_from_response_bps:
  200, 500, 700, 800, 2500, 7100, 19500
- runaway max_extension_from_response_bps:
  2000, 2500, 3000, 7000, 10000
Only valid reclaim < runaway pairs are used: 24.
4 A+B+C regimes x 24 = 96 staged combinations.

Checkpoint/resume is enabled after every completed combination.
No exits, PnL, fees, slippage or latency are modeled.

The sparse high-stringency Block-C hypotheses are preserved separately in:
data\research\phase3\EXP-0004\EXP-0004_C_deferred_hypotheses_v0_1.json
They do NOT enter the main EXP-0005 search.

4) After harness RESULT: PASS:
python scripts\phase3_exp0005_postrun_audit_v0_1.py

5) If audit PASS:
python scripts\phase3_exp0005_package_results_v0_1.py

Upload:
EXP-0005_D_results_for_review_v0_1.zip

Pre-analysis method locked before Block-D results:
- integrity first
- clean 15s/30s primary
- 5m MFE/MAE supporting only
- sample support + outcome quality together
- cross-ABC and neighboring-D stability preferred
- overlapping cohorts are not independent evidence
- no highest-median winner
- deferred Block-C hypotheses stay separate
- produce a small robust full-entry shortlist after D
