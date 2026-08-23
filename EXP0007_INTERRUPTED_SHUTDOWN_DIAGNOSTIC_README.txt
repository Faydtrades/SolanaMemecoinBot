TRADINGBOT — EXP-0007 INTERRUPTED SHUTDOWN DIAGNOSTIC v0.1

Safe to run after the interrupted collector drain.
READ-ONLY: it does not modify the production DB.

Run:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_interrupted_shutdown_diagnostic_v0_1.py

It reports:
- whether the latest collector session is still uncertain
- definite vs uncertain clean intervals
- last definitely closed coverage point
- non-terminal confirmation/deep/gap queue jobs

Do not build DEV-FREEZE-0002 until this diagnostic has been reviewed.
