TRADINGBOT — EXP-0007 CLOSED-PREFIX E1 PATH CHARACTERIZATION v0.2.1

PRECONDITION
------------
DEV-FREEZE-0002 closed-prefix readiness v0.2.1 completed with RESULT: PASS:
- CONTROL 1053 candidates / 1036 clean 5m / 98.39%
- ROBUST_1 206 / 203 / 98.54%
- ROBUST_2 232 / 229 / 98.71%
- ROBUST_3 218 / 211 / 96.79%
- readiness = E2_COARSE_EXIT_SEARCH_READY

PURPOSE
-------
Run the locked E1 descriptive path-characterization stage before E2 coarse
exit research.

E1 describes only CLEAN 5m paths:
- fresh observation count distribution
- future observation count distribution
- peak-bps distribution
- trough-bps distribution

E1 DOES NOT:
- select stop-loss thresholds
- select take-profit thresholds
- select trailing thresholds
- select time stops
- change entry A/B/C/D
- claim profitability

RUN
---
In the normal tool PowerShell window:

1)
.\.venv\Scripts\python.exe scripts\phase3_exp0007_closed_prefix_e1_path_characterization_v0_2_1.py

2) If PASS:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_closed_prefix_e1_path_characterization_audit_v0_2_1.py

Then STOP and send both outputs for review before E2 coarse exit research.
