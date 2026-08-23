TRADINGBOT — EXP-0007 DEV-FREEZE-0002 CLOSE/FREEZE/READINESS v0.2

WHY
---
The active-session candidate count has exceeded 100 in every robust regime.
The remaining unknown is CLEAN 5m path rate/count.

This package aligns final preview, immutable freeze, audit and readiness with
DEV-FREEZE-0002 design v0.2 (one closed session, explicit gaps retained, each
eligible token requires full t0->t0+10m inside one clean active interval).

IMPORTANT
---------
Do NOT use the old v0.1 immutable builder or old v0.1 post-freeze audit for
DEV-FREEZE-0002. They assumed one globally gap-free segment.

RUN NOW WHILE COLLECTOR IS STILL ACTIVE
---------------------------------------
Only:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_devfreeze0002_v02_pipeline_selftest_v0_1.py

THEN, WHEN YOU ARE READY TO CLOSE THIS COLLECTION
-------------------------------------------------
1) In collector window press Ctrl+C ONCE.
2) Wait until collector finishes draining and prints RESULT: PASS / returns prompt.
3) In separate PowerShell run:
.\.venv\Scripts\python.exe scripts\phase3_devfreeze0002_candidate_preview_v0_2_1.py

Proceed only if:
Latest provisional close = NO
DB changed during preview = NO
Immutable freeze now = ALLOWED

4) Build immutable freeze:
.\.venv\Scripts\python.exe scripts\phase3_build_devfreeze0002_v0_2.py --confirm-create

5) Audit:
.\.venv\Scripts\python.exe scripts\phase3_devfreeze0002_postfreeze_audit_v0_2.py

6) Readiness:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_devfreeze0002_readiness_harness_v0_1_2.py

STOP THERE and review readiness output before E1/E2 work.

If readiness is E1_PATH_CHARACTERIZATION_READY or E2_COARSE_EXIT_SEARCH_READY,
the prepared E1 wrapper is:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_e1_path_characterization_v0_2.py

No outcome/PnL was used to decide to stop the active collection. Entry A/B/C/D
remain fixed. The active candidate counter itself inspected no forward outcomes.
