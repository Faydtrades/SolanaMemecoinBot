TRADINGBOT — PHASE 3 LIVE COVERAGE DIAGNOSTIC v0.1

Extract into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Safe to run while the unlimited collector is still running.
It opens the production DB READ-ONLY and writes nothing.

Run:
.\.venv\Scripts\python.exe scripts\phase3_live_coverage_diagnostic_v0_1.py

Purpose:
- distinguish historical gaps from gaps in the current collector session
- show current session duration, active intervals and explicit gap count
- show total/median/P90/max gap duration
- show WS/control-event counts and interval close reasons
- show longest/current clean interval

Do NOT stop the collector just to run this.
