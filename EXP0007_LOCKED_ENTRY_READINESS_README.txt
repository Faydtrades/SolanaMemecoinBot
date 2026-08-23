TRADINGBOT — EXP-0007 LOCKED-ENTRY READINESS v0.1

Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

WHILE THE UNLIMITED COLLECTOR CONTINUES IN ITS OWN POWERSHELL WINDOW:

Run in the separate PowerShell window:

1)
.\.venv\Scripts\python.exe scripts\phase3_exp0007_locked_entry_readiness_selftest_v0_1.py

2)
.\.venv\Scripts\python.exe scripts\phase3_exp0007_devfreeze0001_readiness_regression_v0_1.py

The second command can take a few minutes. It replays the four locked
full-entry regimes on the old immutable DEV-FREEZE-0001.

Expected regression:
CONTROL candidates 230
ROBUST_1 candidates 19
ROBUST_2 candidates 22
ROBUST_3 candidates 21
robust clean 5m [0,0,0]
readiness NOT_READY
RESULT: PASS

DO NOT RUN YET:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_devfreeze0002_readiness_harness_v0_1.py

That command is for AFTER DEV-FREEZE-0002 is immutably created.

INTEGRATION FIX INCLUDED:
collector_coverage_v0_1 is bound to OutcomeReplay v0.1.1 enum/provider classes,
while ExitPathReplay v0.1 is bound to OutcomeReplay v0.1.3. This package adds
collector_coverage_v0_2 with the same P3COV-0.1 JSON schema but v0.1.3
CoverageStatus/IntervalCoverageProvider classes. This prevents enum-identity
mismatches in exit-path readiness.

No production DB writes.
No entry parameter changes.
No exit tuning.
