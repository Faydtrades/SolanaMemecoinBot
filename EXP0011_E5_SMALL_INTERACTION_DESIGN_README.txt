TRADINGBOT — EXP-0011 E5 SMALL INTERACTION DESIGN v0.1

Purpose
-------
Lock a tiny, explicit E5 interaction shortlist before seeing any E5 outcomes.

Nine variants
-------------
1. REF_T15
2. E3 TP10/T15 reference
3. E4 ACT10/GB3/T15 reference
4. TP10 + ACT5/GB3 + T15
5. TP15 + ACT10/GB3 + T15
6. TP10 + SL10 + T15
7. ACT10/GB3 + SL10 + T15
8. TP10 + ACT5/GB3 + SL10 + T15
9. TP20/T5 sensitivity reference

This is intentionally NOT a Cartesian search.

Why SL10 appears
----------------
EXP-0008 did not promote any hard stop, but SL10 was retained only as a
risk-control interaction hypothesis. E5 is the bounded place to test whether it
improves downside once paired with the surviving E3/E4 exit families.

Decision rule
-------------
Complexity must earn its place. If an interaction does not materially improve
cross-regime downside/central tendency versus the simpler E3/E4 references,
the simpler rule wins.

No fees/slippage/latency or profitability claim yet.

RUN NOW
-------
1)
.\.venv\Scripts\python.exe scripts\phase3_exp0011_e5_small_interaction_design_selftest_v0_1.py

2) If PASS:
.\.venv\Scripts\python.exe scripts\phase3_exp0011_initialize_e5_small_interaction_design_v0_1.py

Then STOP and send the outputs before any E5 outcome run.
