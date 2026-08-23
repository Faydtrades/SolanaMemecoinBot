TRADINGBOT — EXP-0007 LOCKED-ENTRY READINESS FIX v0.1.1

Reason:
v0.1 attempted to serialize CandidateSignal.generated_at, but CandidateSignal
stores generated_at_us. CandidateExtraction already carries the canonical UTC
timestamp as reference.signal_at. v0.1.1 uses c.reference.signal_at.isoformat().

This is a reporting/runtime integration bug only. It does not change strategy
parameters, CandidateSignal generation, collector data, or entry logic.

Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Run in the separate PowerShell window while the unlimited collector keeps running:

1)
.\.venv\Scripts\python.exe scripts\phase3_exp0007_locked_entry_readiness_selftest_v0_1_1.py

2)
.\.venv\Scripts\python.exe scripts\phase3_exp0007_devfreeze0001_readiness_regression_v0_1_1.py

Expected regression:
CONTROL candidates 230
ROBUST_1 candidates 19
ROBUST_2 candidates 22
ROBUST_3 candidates 21
robust clean 5m [0,0,0]
readiness NOT_READY
RESULT: PASS

Do NOT run the DEV-FREEZE-0002 readiness harness yet. The corrected future
script is included as phase3_exp0007_devfreeze0002_readiness_harness_v0_1_1.py
for use only after DEV-FREEZE-0002 exists.
