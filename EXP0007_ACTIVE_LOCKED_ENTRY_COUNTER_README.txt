TRADINGBOT — EXP-0007 ACTIVE LOCKED-ENTRY CANDIDATE COUNTER v0.1

Purpose
-------
Count the exact CONTROL / ROBUST_1 / ROBUST_2 / ROBUST_3 CandidateSignals
present in the ACTIVE collector session at one stable SQLite read snapshot.

This is deliberately ENTRY-ONLY:
- no forward returns
- no ExitPathReplay
- no MFE/MAE
- no PnL
- no exit threshold research

Cohort semantics are the locked DEV-FREEZE-0002 design v0.2:
a token is included only when its global t0 and entire [t0,t0+10m] window
fit inside one clean active collector interval.

Safe while collector is running
-------------------------------
The production DB is opened READ-ONLY. A single read transaction provides a
stable snapshot while the WAL writer continues.

Run from:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Command:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_active_locked_entry_candidate_count_v0_1.py

It may take a few minutes because it causally replays the locked entry strategy
for thousands of eligible tokens.

Important
---------
The candidate counts are exact for that snapshot, but they are NOT yet the
E1/E2 "clean 5m path" counts. We intentionally do not inspect forward outcomes
while the live development session is still active.
