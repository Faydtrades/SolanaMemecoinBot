TRADINGBOT — DEV-FREEZE-0002 COVERAGE AMENDMENT v0.2

WHY THIS EXISTS
---------------
The v0.1 freeze design required one globally gap-free collector segment.
A read-only diagnostic on the active session showed ~99.75% active coverage,
with many brief (typically ~2-3 second) WS reconnect gaps. Keeping v0.1 would
discard unrelated clean data.

v0.2 does NOT ignore gaps and does NOT lower E1/E2 gates.

Instead:
- the selected unit is one collector session;
- explicit gaps remain recorded;
- a token is eligible only when its GLOBAL t0 and the ENTIRE [t0,t0+10m]
  interval lie inside ONE clean active interval;
- therefore a gap touching a token's required 10m window excludes that token;
- a gap elsewhere in the session does not exclude unrelated tokens.

This amendment was made from COVERAGE METADATA ONLY, before looking at any
new strategy outcomes, exit outcomes, or PnL.

SAFE TO RUN WHILE COLLECTOR CONTINUES
-------------------------------------
Extract into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Run, in the separate PowerShell:

1)
.\.venv\Scripts\python.exe scripts\phase3_exp0007_devfreeze0002_coverage_amendment_selftest_v0_2.py

2)
.\.venv\Scripts\python.exe scripts\phase3_exp0007_initialize_devfreeze0002_design_v0_2.py

3)
.\.venv\Scripts\python.exe scripts\phase3_devfreeze0002_candidate_preview_v0_2.py

DO NOT BUILD DEV-FREEZE-0002 YET.
DO NOT RUN EXIT RESEARCH.
DO NOT STOP THE COLLECTOR FOR THESE COMMANDS.

The v0.1 design file should remain as historical provenance. v0.2 supersedes it;
do not delete or overwrite v0.1.
