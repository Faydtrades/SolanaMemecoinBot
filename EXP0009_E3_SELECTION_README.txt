TRADINGBOT — EXP-0009 E3 PROFIT-TAKING SELECTION v0.1

Purpose
-------
Lock the postrun research carry-forward from the already-audited EXP-0009
results before designing E4 trailing.

Mainline T15s neighborhood:
- lower neighbor: TP +5%
- PRIMARY anchor: TP +10%
- upper neighbor: TP +15%

Sensitivity-only:
- TP +20% with 5s fallback

Deferred, not rejected:
- TP20/T15s
- TP30/T15s
- TP30/T5s
- TP03/T15s

Why TP10/T15s is the anchor
---------------------------
It is not simply the highest mean. Across all three robust regimes it combines:
- strong medians
- improved positive rate vs pure T15s
- materially improved lower tail vs pure T15s
- roughly half of clean paths reaching TP
- 98%+ T15s scoring coverage

The higher TP20/TP30 variants have higher gross means but lower hit rates and
more tail-dependent distributions, so they are not promoted to mainline.

Run
---
1)
.\.venv\Scripts\python.exe scripts\phase3_exp0009_e3_selection_selftest_v0_1.py

2) If PASS:
.\.venv\Scripts\python.exe scripts\phase3_exp0009_initialize_e3_selection_v0_1.py

Then STOP and review before locking EXP-0010 / E4 trailing design.
