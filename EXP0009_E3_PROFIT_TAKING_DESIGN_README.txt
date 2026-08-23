TRADINGBOT — EXP-0009 E3 PROFIT-TAKING DESIGN v0.1

WHY THIS GRID
-------------
EXP-0008 E2 showed:
- T5s = strongest aggressive early gross-reference result, but more unscored paths
- T15s = primary robust anchor with near-complete scoring
- T30s ≈ edge boundary
- no hard-stop mainline winner

Before any E3 outcomes are inspected, lock:
TP thresholds: +3%, +5%, +10%, +15%, +20%, +30%
Fallbacks:
- PRIMARY: 15s
- SENSITIVITY: 5s

E3 isolates a single TP threshold plus the pre-carried time-stop fallback.
No hard stop, no trailing, no partial TP, no multi-TP, no break-even logic.

RUN NOW
-------
Extract into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

In the normal tool PowerShell:

1)
.\.venv\Scripts\python.exe scripts\phase3_exp0009_e3_profit_taking_design_selftest_v0_1.py

2)
.\.venv\Scripts\python.exe scripts\phase3_exp0009_initialize_e3_profit_taking_design_v0_1.py

Then STOP and send the outputs.

Do not run E3 outcome research until this design is locked and reviewed.
