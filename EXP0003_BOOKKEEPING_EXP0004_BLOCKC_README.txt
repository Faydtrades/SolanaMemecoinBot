TRADINGBOT — EXP-0003 BOOKKEEPING + EXP-0004 BLOCK C v0.1

Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Run in this exact order with (.venv) active:

1)
python scripts\phase3_exp0003_bookkeeping_v0_1.py

Expected:
EXP-0003 COMPLETE
next experiment ID = EXP-0004
RESULT: PASS

2)
python scripts\phase3_buyer_response_block_selftest_v0_1.py

Expected:
4 A+B x 150 C = 600
RESULT: PASS

3)
python scripts\phase3_exp0004_buyer_response_harness_v0_1.py

This is a LONG run. At roughly prior single-combination timings it may take
many hours, potentially overnight. Checkpoint/resume is enabled after every
completed combination from the start. If interrupted, rerun the same command.

Block C varies only the locked 3s buyer-response parameters:
- min_rebound_bps: 200, 500, 800, 2300, 7300, 20700
- min_buys: 1, 2, 3, 4, 8
- min_net_flow_reserve_ppm: 0, 6000, 30000, 93000, 185000

4 carry-forward A+B regimes x 150 = 600 combinations.
D reclaim/runaway stays fixed.
No exit optimization, no PnL/friction model, no OOS claim.

After harness RESULT: PASS:

4)
python scripts\phase3_exp0004_postrun_audit_v0_1.py

5) If audit PASS:
python scripts\phase3_exp0004_package_results_v0_1.py

Upload:
EXP-0004_C_results_for_review_v0_1.zip

Pre-analysis methodology was locked BEFORE Block-C results:
- clean 15s/30s are primary
- 5m MFE/MAE supporting/descriptive only
- sample support counts together with outcome quality
- prefer stability across multiple A+B regimes and neighboring C settings
- do not pool overlapping cohorts as independent evidence
- do not select by highest median alone
- create a small robust A+B+C shortlist for Block D
