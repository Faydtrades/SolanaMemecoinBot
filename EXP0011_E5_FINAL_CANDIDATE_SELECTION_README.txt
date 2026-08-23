TRADINGBOT — EXP-0011 E5 FINAL CANDIDATE SELECTION v0.1

Conclusion from E5
------------------
No interaction earns enough complexity to replace the simpler locked references.

Advance to final Phase-3 validation:
- FINAL-A: E3 TP +10% with 15s fallback
- FINAL-B: E4 activation +10%, giveback 3%, 15s fallback
- SENS-C: TP +20% with 5s fallback, sensitivity-only
- REF_T15 remains benchmark only

Do NOT promote:
- TP+trail E5 interactions
- any SL10 interaction
- the three-component interaction

Why keep two mainline finalists?
--------------------------------
On development/discovery data there is no clean dominance:
- FINAL-A has the stronger median/central tendency and is simpler.
- FINAL-B has the higher gross-reference mean, consistent with more runner capture.
Selecting one solely on DEV-FREEZE-0002 would be another layer of development-data
winner selection. Both should advance to deterministic final validation and then
fresh real-time paper evaluation where practical.

No additional exit tuning on DEV-FREEZE-0002 is allowed before final validation.

RUN NOW
-------
1)
.\.venv\Scripts\python.exe scripts\phase3_exp0011_e5_final_candidate_selection_selftest_v0_1.py

2) If PASS:
.\.venv\Scripts\python.exe scripts\phase3_exp0011_initialize_e5_final_candidate_selection_v0_1.py

Then STOP and send output. Next: EXP-0012 final Phase-3 validation design.
