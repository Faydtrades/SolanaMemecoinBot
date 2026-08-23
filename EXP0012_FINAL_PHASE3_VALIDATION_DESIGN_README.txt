TRADINGBOT — EXP-0012 FINAL PHASE-3 VALIDATION DESIGN v0.1

Purpose
-------
Lock the last Phase-3 validation step before any final validation output is run.

Locked finalists:
- FINAL-A: TP +10% with 15s fallback
- FINAL-B: trailing activation +10%, giveback 3%, 15s fallback
- SENS-C: TP +20% with 5s fallback, sensitivity-only

EXP-0012 is NOT another research search.
It may not:
- tune exit parameters
- promote/demote a candidate
- reopen SL10
- reopen E5 interactions
- declare live profitability

The final validation run must:
1. reopen DEV-FREEZE-0002 read-only and quick_check it
2. validate coverage/source bindings
3. independently replay locked entry
4. reproduce exact candidate counts
5. reproduce exact clean 5m counts
6. evaluate only FINAL-A/FINAL-B/SENS-C
7. reproduce the already-audited E5 rows exactly
8. evaluate exits twice over cached clean paths and require identical deterministic hashes
9. produce PHASE3_FINAL_VALIDATION_PASS or FAIL

Only PASS permits the Phase-4 Real-Time Paper Trading build to begin.

RUN NOW
-------
1)
.\.venv\Scripts\python.exe scripts\phase3_exp0012_final_validation_design_selftest_v0_1.py

2) If PASS:
.\.venv\Scripts\python.exe scripts\phase3_exp0012_initialize_final_validation_design_v0_1.py

Then STOP and send the outputs. The next package will contain the actual final
validation harness/audit.
