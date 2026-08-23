TRADINGBOT — EXP-0002 BOOKKEEPING + EXP-0003 BLOCK B v0.1

Install:
Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

No existing versioned strategy files are overwritten.

Run in this exact order with (.venv) active:

1) Bookkeep completed EXP-0002 and advance registry:
python scripts\phase3_exp0002_bookkeeping_v0_1.py

Expected: RESULT: PASS and next experiment ID = EXP-0003.

2) Validate Block-B design:
python scripts\phase3_pullback_block_selftest_v0_1.py

Expected: RESULT: PASS.

3) Run EXP-0003 Block B:
python scripts\phase3_exp0003_pullback_harness_v0_1.py

Design:
5 locked A configurations x 21 valid B pullback pairs = 105 combinations.
C/D remain fixed. Checkpoint/resume is enabled from the first combination.
No exit optimization, PnL/friction model or profitability claim.

4) After harness RESULT: PASS:
python scripts\phase3_exp0003_postrun_audit_v0_1.py

5) If audit PASS:
python scripts\phase3_exp0003_package_results_v0_1.py

Upload:
EXP-0003_B_results_for_review_v0_1.zip

Pre-analysis method was locked before Block-B results:
- clean 15s/30s are primary
- 5m MFE/MAE supporting only
- sample support matters
- prefer stability across multiple A regimes and neighboring B ranges
- do not pool overlapping A cohorts as independent evidence
- no highest-median winner selection
- produce a small robust A+B shortlist for Block C
