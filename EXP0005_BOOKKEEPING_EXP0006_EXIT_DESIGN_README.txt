TRADINGBOT — EXP-0005 BOOKKEEPING + EXP-0006 EXIT DESIGN / FEASIBILITY v0.1

Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Run with (.venv) active in this exact order:

1) Lock/bookkeep EXP-0005 and preserve the sparse D hypothesis:
python scripts\phase3_exp0005_bookkeeping_v0_1.py

Expected:
EXP-0005 COMPLETE
4 full-entry carry-forward regimes
1 deferred D hypothesis retained
next experiment ID EXP-0006
RESULT: PASS

2) Validate the exit-research methodology BEFORE any exit results:
python scripts\phase3_exp0006_exit_design_selftest_v0_1.py

Expected:
RESULT: PASS

3) Run the fast feasibility audit:
python scripts\phase3_exp0006_exit_feasibility_harness_v0_1.py

This does NOT test SL, TP, trailing stop, time stop or PnL.
It only asks whether the existing DEV-FREEZE-0001 provides enough clean
post-entry observability for the locked full-entry regimes to support exit research.

4) Audit:
python scripts\phase3_exp0006_exit_feasibility_postrun_audit_v0_1.py

5) Package:
python scripts\phase3_exp0006_package_results_v0_1.py

Upload:
EXP-0006_exit_feasibility_results_for_review_v0_1.zip

Important design:
- A/B/C/D entry parameters are now fixed for mainline exit research.
- Exact SL/TP/trailing/time-stop thresholds remain intentionally UNLOCKED.
- The historical manual -30% stop is not silently adopted.
- Missing future data is never treated as flat/no-trigger/profit/loss.
- Deferred high-stringency entry hypotheses stay separate.
- Exit parameter optimization is blocked until the path/observability foundation supports it.
