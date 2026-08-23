TRADINGBOT — EXP-0007 / DEV-FREEZE-0002 TOOLING v0.1

Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

WHILE THE UNLIMITED COLLECTOR IS RUNNING:

1)
python scripts\phase3_exp0007_devfreeze0002_design_selftest_v0_1.py

2)
python scripts\phase3_exp0007_initialize_devfreeze0002_design_v0_1.py

3) Safe read-only preview:
python scripts\phase3_devfreeze0002_candidate_preview_v0_1.py

The preview may say:
Immutable freeze now : BLOCKED
That is EXPECTED while the current collector session has a provisional/uncertain close.

DO NOT run the immutable builder while the collector is active.

LATER, only when ChatGPT tells you the data window is ready:
- stop the collector normally with Ctrl+C
- wait for RESULT: PASS
- regenerate the preview
- confirm Latest provisional close = NO
- then run:
python scripts\phase3_build_devfreeze0002_v0_1.py --confirm-create

The builder:
- refuses overwrite
- refuses a provisional active segment
- requires global first observed BOT_TRUTH trade inside the clean segment
- excludes a GAP_RECOVERY first trade from clean cohort seeding
- requires t0 + 10 minutes <= segment end
  (5m entry eligibility + 5m post-entry path)
- applies no future-activity or outcome selection filter
- records the locked EXP-0005 entry-selection hash
- opens production market data read-only

No SL/TP/trailing/time-stop research is performed by this package.
