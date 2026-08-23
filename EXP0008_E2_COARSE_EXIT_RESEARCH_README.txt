TRADINGBOT — EXP-0008 E2 COARSE EXIT RESEARCH v0.1

PRECONDITION
------------
EXP-0007 / DEV-FREEZE-0002:
- post-freeze audit PASS
- readiness E2_COARSE_EXIT_SEARCH_READY
- E1 path characterization PASS + audit PASS

LOCK THIS BEFORE RUNNING E2 RESULTS
-----------------------------------
The E2 design uses the E1 characterization only to choose a broad coarse range,
before any E2 exit results are observed.

Hard-stop family:
-10%, -20%, -30%, -40%, -50%, -60%
First observed threshold crossing, no interpolation.
If no stop is hit, research terminal mark is latest fresh same-identity mark
at or before 5m.

Time-stop family:
5s, 15s, 30s, 60s, 2m, 5m
These are the already-established OutcomeReplay horizons.
Research mark is latest fresh same-identity observation at/before the horizon;
mark age is exposed, with no hidden freshness threshold.

The families are NOT crossed with one another in E2.
No TP, no trailing, no fees/slippage/latency and no profitability claim.

RUN
---
1)
.\.venv\Scripts\python.exe scripts\phase3_exp0008_e2_coarse_exit_design_selftest_v0_1.py

2)
.\.venv\Scripts\python.exe scripts\phase3_exp0008_initialize_e2_coarse_exit_design_v0_1.py

STOP and confirm design initialization before running the E2 outcome harness if
you want a human checkpoint.

Then:
3)
.\.venv\Scripts\python.exe scripts\phase3_exp0008_e2_coarse_exit_harness_v0_1.py

4)
.\.venv\Scripts\python.exe scripts\phase3_exp0008_e2_coarse_exit_postrun_audit_v0_1.py

Send the E2 CSV/results package for review before selecting any shortlist or
moving to E3 profit-taking.
