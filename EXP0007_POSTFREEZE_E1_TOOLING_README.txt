TRADINGBOT — EXP-0007 POST-FREEZE + E1 TOOLING v0.1

Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

YOU MAY RUN NOW, WHILE THE UNLIMITED COLLECTOR KEEPS RUNNING:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_postfreeze_e1_tooling_selftest_v0_1.py

DO NOT RUN THE OTHER SCRIPTS YET.

LATER, after the collector has been stopped cleanly and DEV-FREEZE-0002 has
been immutably created:

1) Audit the new freeze:
.\.venv\Scripts\python.exe scripts\phase3_devfreeze0002_postfreeze_audit_v0_1.py

2) Run corrected locked-entry readiness from the prior v0.1.1 package:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_devfreeze0002_readiness_harness_v0_1_1.py

3) Review readiness tier.

If NOT_READY:
- do not run E1
- collect more data and create a later fresh development freeze.

If E1_PATH_CHARACTERIZATION_READY or E2_COARSE_EXIT_SEARCH_READY:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_e1_path_characterization_v0_1.py
.\.venv\Scripts\python.exe scripts\phase3_exp0007_e1_path_characterization_audit_v0_1.py

4) Package:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_package_devfreeze0002_results_v0_1.py

E1 is descriptive only:
- clean 5m path point-count distribution
- future-observation count distribution
- peak-bps distribution
- trough-bps distribution

It does NOT select SL, TP, trailing, time-stop, alter A/B/C/D, model friction,
or claim profitability.
