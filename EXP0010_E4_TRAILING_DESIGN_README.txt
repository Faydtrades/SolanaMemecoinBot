TRADINGBOT — EXP-0010 E4 TRAILING DESIGN v0.1

LOCKED CONTEXT
--------------
EXP-0009 E3 selection is already locked:
- primary mainline anchor: TP10 + T15s
- mainline neighbors: TP5 + T15s / TP15 + T15s
- sensitivity-only: TP20 + T5s
- no hard-stop winner

E4 intentionally does NOT combine trailing with those TP rules yet.

E4 TRAILING GRID
----------------
Activation:
+5%, +10%, +15%

Giveback:
3%, 5%, 10%

Fallback:
15s

This creates 9 trailing variants x 4 regimes = 36 result rows when E4 is run.

Semantics:
- first observed activation; no interpolation
- running observed peak after activation
- trail = peak minus fixed giveback
- first observed crossing of trail; no interpolation
- if no activation/trail hit, fall back to latest fresh mark <=15s

Not included:
- fixed TP
- hard stop
- partial/multi-TP
- breakeven logic
- T5s sensitivity
- fees/slippage/latency
- profitability claim

RUN NOW
-------
1)
.\.venv\Scripts\python.exe scripts\phase3_exp0010_e4_trailing_design_selftest_v0_1.py

2) If PASS:
.\.venv\Scripts\python.exe scripts\phase3_exp0010_initialize_e4_trailing_design_v0_1.py

Then STOP and send the outputs before any E4 outcome run.
