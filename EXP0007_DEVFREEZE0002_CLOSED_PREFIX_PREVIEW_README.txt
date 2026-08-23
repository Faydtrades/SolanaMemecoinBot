TRADINGBOT — DEV-FREEZE-0002 CLOSED-PREFIX PREVIEW v0.2.1

Context
-------
The collector was interrupted during drain. The source session is EOF_UNCERTAIN.
A read-only diagnostic found a maximal definitely closed prefix ending before
the uncertain tail.

This amendment is coverage-driven. We had seen locked ENTRY candidate counts,
but no forward returns, exit paths, MFE/MAE or PnL.

The background recovery was subsequently stopped. Confirmation/deep/gap queues
may remain; they do not gate this preview. Explicit collector gaps remain hard
non-observable coverage boundaries, and the entire uncertain tail is discarded.

RUN NOW
-------
Extract into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

In the separate PowerShell:

1)
.\.venv\Scripts\python.exe scripts\phase3_exp0007_devfreeze0002_closed_prefix_selftest_v0_2_1.py

2)
.\.venv\Scripts\python.exe scripts\phase3_exp0007_initialize_closed_prefix_design_v0_2_1.py

3)
.\.venv\Scripts\python.exe scripts\phase3_exp0007_devfreeze0002_closed_prefix_preview_v0_2_1.py

Then STOP and send the preview output for review.

Do not restart the collector.
Do not restart queue recovery.
Do not build DEV-FREEZE-0002 yet.
