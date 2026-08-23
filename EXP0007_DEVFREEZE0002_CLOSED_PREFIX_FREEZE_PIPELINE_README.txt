TRADINGBOT — DEV-FREEZE-0002 CLOSED-PREFIX FREEZE PIPELINE v0.2.1

PRECONDITION
------------
Closed-prefix preview v0.2.1 already passed with:
- safe prefix duration ~22.84h
- 70 definite clean intervals
- 1 discarded uncertain interval
- 69 explicit gaps retained
- 8,649 projected eligible tokens
- ROBUST_1/2/3 entry candidates 206 / 232 / 218
- no forward outcomes / exit paths / PnL inspected
- DB unchanged during preview
- Prefix freeze candidate = YES

IMPORTANT
---------
Do NOT restart the collector.
Do NOT restart queue-drain recovery.
Do NOT use the older v0.1/v0.2 whole-session builders.

RUN NEXT
--------
1) Create immutable closed-prefix DEV-FREEZE-0002:
.\.venv\Scripts\python.exe scripts\phase3_build_devfreeze0002_closed_prefix_v0_2_1.py --confirm-create

2) If builder RESULT: PASS, audit:
.\.venv\Scripts\python.exe scripts\phase3_devfreeze0002_closed_prefix_postfreeze_audit_v0_2_1.py

3) If audit RESULT: PASS, run exact clean-5m readiness:
.\.venv\Scripts\python.exe scripts\phase3_exp0007_devfreeze0002_closed_prefix_readiness_v0_2_1.py

Then STOP and send the full readiness output for review.

The readiness command is the FIRST step that intentionally inspects forward
5m exit-path cleanliness on DEV-FREEZE-0002. It still does not tune exits or
claim profitability.
