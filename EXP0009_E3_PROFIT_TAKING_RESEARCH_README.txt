TRADINGBOT — EXP-0009 E3 PROFIT-TAKING RESEARCH v0.1

PRECONDITION
------------
EXP-0009 E3 design v0.1 has already been initialized/locked before results.

LOCKED E3
---------
TP thresholds:
+3%, +5%, +10%, +15%, +20%, +30%

Fallbacks:
- PRIMARY: 15s
- SENSITIVITY: 5s

Semantics:
- first observed fresh same-identity TP crossing at/before fallback horizon
- no interpolation
- if no TP hit, use latest fresh same-identity observation at/before fallback
- expose fallback mark age
- no hard stop
- no trailing
- no partial/multiple TP
- no fees/slippage/latency
- no profitability claim

RUN
---
1) Engine semantics selftest:
.\.venv\Scripts\python.exe scripts\phase3_exp0009_e3_profit_taking_engine_selftest_v0_1.py

2) If PASS, run E3:
.\.venv\Scripts\python.exe scripts\phase3_exp0009_e3_profit_taking_harness_v0_1.py

3) If RESULT: PASS, audit:
.\.venv\Scripts\python.exe scripts\phase3_exp0009_e3_profit_taking_postrun_audit_v0_1.py

Then STOP and send:
- EXP-0009_E3_profit_taking_results_v0_1.json
- EXP-0009_E3_profit_taking_by_regime_v0_1.csv
- EXP-0009_E3_profit_taking_postrun_audit_v0_1.json

Do not move to E4 trailing until EXP-0009 results are reviewed.
