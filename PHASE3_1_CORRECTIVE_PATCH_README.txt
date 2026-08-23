TRADINGBOT PHASE 3.1 CORRECTIVE PATCH v0.1.3

This supersedes the earlier v0.1.2 MFE-only patch.

Review of EXP-0001 v0.1 found two semantic issues to correct before Phase 3.1
is marked complete:

1) MFE/MAE excursion baseline
   Candidate/reference price is 0 bps.
   Therefore valid MFE cannot be negative and valid MAE cannot be positive.

2) Pump market-continuity boundary
   A post-signal Pump TradeEvent with real_token_reserve_raw <= 0 indicates
   the Pump curve has no remaining real token reserve. The current frozen
   dataset does not provide migrated-venue/PumpSwap continuation, so horizons
   strictly after that boundary must not be labeled clean OBSERVABLE merely
   by carrying the final Pump price forward.

The patch preserves all previous files and adds new versioned files:
- src\phase3\outcome_replay_v0_1_3.py
- src\phase3\phase2_outcome_adapter_v0_1_1.py
- scripts\phase3_outcome_replay_selftest_v0_1_3.py
- scripts\phase3_realdata_outcome_harness_v0_1_2.py

Install:
Extract directly into
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Then run:
python scripts\phase3_outcome_replay_selftest_v0_1_3.py

If PASS:
python scripts\phase3_realdata_outcome_harness_v0_1_2.py

The corrected harness writes new versioned EXP-0001 artifacts and leaves the
old v0.1 outputs intact for audit/history.
