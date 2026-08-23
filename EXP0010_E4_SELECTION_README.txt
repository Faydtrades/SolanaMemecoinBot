TRADINGBOT — EXP-0010 E4 TRAILING SELECTION v0.1

Purpose
-------
Lock the E4 postrun trailing carry-forward before E5 interaction design.

Mainline trailing carry-forward:
- PRIMARY: activation +10%, giveback 3%, fallback 15s
- lower-activation neighbor: activation +5%, giveback 3%, fallback 15s
- wider-giveback neighbor: activation +10%, giveback 5%, fallback 15s

Supporting plateau:
- ACT05/GB05 remains supporting evidence
- strongest coarse region is activation +5 to +10% / giveback 3 to 5%

Important
---------
E4 does NOT replace the independently locked E3 primary TP10/T15s.
The E4 family has higher gross-reference mean but generally lower median than
TP10/T15s, consistent with more runner/tail capture. E5 must test only a small
prelocked interaction shortlist.

No hard stop, no T5s interaction, no profitability claim is added here.

Run
---
1)
.\.venv\Scripts\python.exe scripts\phase3_exp0010_e4_selection_selftest_v0_1.py

2) If PASS:
.\.venv\Scripts\python.exe scripts\phase3_exp0010_initialize_e4_selection_v0_1.py

Then STOP and review before EXP-0011 / E5 interaction design.
