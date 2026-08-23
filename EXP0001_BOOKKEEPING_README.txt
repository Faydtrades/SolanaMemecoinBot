TRADINGBOT — EXP-0001 BOOKKEEPING v0.1

Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Then, from project root with (.venv) active, run:

python scripts\phase3_exp0001_bookkeeping_v0_1.py

This registers the corrected completed EXP-0001, advances the canonical
experiment registry to next_experiment_id = EXP-0002, creates a pre-update
registry backup under data\research\phase3\registry_history\, and verifies
that DEV-FREEZE-0001 SQLite is unchanged.

Expected final line:
RESULT: PASS
