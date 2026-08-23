TRADINGBOT — EXP-0012 FINAL PHASE-3 VALIDATION RUN v0.1

Precondition
------------
EXP-0012 final-validation design v0.1 is already LOCKED.

Run
---
1) Engine selftest:
.\.venv\Scripts\python.exe scripts\phase3_exp0012_final_validation_engine_selftest_v0_1.py

2) If PASS, final deterministic validation:
.\.venv\Scripts\python.exe scripts\phase3_exp0012_final_validation_harness_v0_1.py

3) If RESULT: PASS, audit:
.\.venv\Scripts\python.exe scripts\phase3_exp0012_final_validation_postrun_audit_v0_1.py

Then STOP and send the harness + audit outputs.

Expected characteristics
------------------------
This can take roughly the same order as E5 because it independently replays the
locked entry pipeline and rebuilds clean paths.

It performs NO search and NO tuning.

PASS requires:
- immutable freeze quick_check
- coverage/source hash binding
- exact entry counts
- exact clean 5m counts
- exact reproduction of FINAL-A / FINAL-B / SENS-C rows from audited EXP-0011
- two identical cached-path exit evaluations
- no parameter search, no candidate reselection, no further exit tuning

Only after PASS + audit should the Phase-3 COMPLETE checkpoint be written and the
Phase-4 Real-Time Paper Trading build begin.
