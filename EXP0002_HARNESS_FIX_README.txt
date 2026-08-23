TRADINGBOT — EXP-0002 HARNESS CORRECTION v0.1.1

Why
---
The first EXP-0002 harness stopped at combination 151 because its terminal
accounting omitted FirstPullbackState.REJECT. REJECT is a legitimate v1.1
terminal state (runaway/too-late rejection), so the strategy/data were not
the problem.

The exact failing combination was regression-tested against DEV-FREEZE-0001:
R=12500, T=2, B=1
3901 runs = 3835 expired + 11 invalidated + 1 rejected + 54 candidate + 0 trade.

What changes
------------
- phase2_outcome_adapter_v0_1_2.py exposes rejected_count and trade_count.
- harness v0.1.1 includes REJECT in terminal lifecycle accounting.
- every completed combination is atomically checkpointed.
- rerunning v0.1.1 resumes completed v0.1.1 combinations after a crash.
- official result artifacts are versioned v0_1_1.
- corrected post-run audit also validates REJECT accounting.

What does NOT change
--------------------
- FirstPullback v1.1 strategy logic
- PHASE-2-PARAM-SEARCH-001
- DEV-FREEZE-0001
- A/B/C/D research design
- pre-analysis methodology
- outcome semantics
- no exit/PnL/friction optimization

Install
-------
Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

If Windows asks about src\phase3\__init__.py, skip it.

Run FIRST:
python scripts\phase3_exp0002_reject_regression_v0_1.py

Expected: RESULT: PASS.

Then run:
python scripts\phase3_exp0002_impulse_harness_v0_1_1.py

IMPORTANT: the failed v0.1 run did not checkpoint its first 150 calculations,
so those must be recomputed once. From v0.1.1 onward each completed combination
is saved and can be resumed.

After harness RESULT: PASS:
python scripts\phase3_exp0002_postrun_audit_v0_1_1.py

If audit PASS:
python scripts\phase3_exp0002_package_results_v0_1_1.py

Upload:
EXP-0002_A_results_for_review_v0_1_1.zip
