TRADINGBOT — EXP-0010 E4 TRAILING RESEARCH v0.1

PRECONDITION
------------
EXP-0010 E4 trailing design v0.1 is already locked before results.

LOCKED E4
---------
Activation thresholds:
+5%, +10%, +15%

Giveback distances:
3%, 5%, 10%

Fallback:
15s

Observed-point semantics only:
- no interpolation
- first observed activation
- observed running peak
- trail = running peak - fixed giveback
- first observed trail crossing
- fallback to latest fresh same-identity mark <=15s

Factor isolation:
- NO E3 fixed TP
- NO hard stop
- NO partial/multi-TP
- NO T5s sensitivity
- NO fees/slippage/latency
- NO profitability claim

RUN
---
1) Engine semantics selftest:
.\.venv\Scripts\python.exe scripts\phase3_exp0010_e4_trailing_engine_selftest_v0_1.py

2) If PASS, run E4:
.\.venv\Scripts\python.exe scripts\phase3_exp0010_e4_trailing_harness_v0_1.py

3) If RESULT: PASS, audit:
.\.venv\Scripts\python.exe scripts\phase3_exp0010_e4_trailing_postrun_audit_v0_1.py

Then STOP and upload:
- EXP-0010_E4_trailing_results_v0_1.json
- EXP-0010_E4_trailing_by_regime_v0_1.csv
- EXP-0010_E4_trailing_postrun_audit_v0_1.json

Do not move to E5 interaction research until EXP-0010 results are reviewed.
