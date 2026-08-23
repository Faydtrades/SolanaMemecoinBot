TRADINGBOT — EXP-0002 / BLOCK A IMPULSE v0.1

Purpose
-------
Run the first staged parameter block after Phase 3.1.

EXP-0002 varies ONLY:
- min_return_bps
- min_trades_since_t0
- min_unique_buyers_since_t0

It evaluates exactly the 175 locked A_IMPULSE combinations from
PHASE-2-PARAM-SEARCH-001.

B/C/D are held fixed at the permissive foundation anchors used by EXP-0001
to isolate Block A:
B pullback: 1000 / 9000 bps
C response: 3s, rebound 200 bps, 1 buy, flow 0 ppm
D reclaim/runaway: 200 / 10000 bps

This is NOT:
- the 13.23m full Cartesian search
- exit optimization
- TP/SL tuning
- a PnL test
- realistic-net validation
- OOS validation

The harness outputs all 175 results plus a deterministic DIAGNOSTIC Pareto
frontier. It deliberately does not select a single winner.

Install
-------
Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Run selftest first:
python scripts\phase3_impulse_block_selftest_v0_1.py

If RESULT: PASS, run:
python scripts\phase3_exp0002_impulse_harness_v0_1.py

The real-data harness prints progress every 10 combinations. It can take
noticeably longer than EXP-0001 because the complete 3,901-token strategy
replay is repeated for all 175 Block-A parameter combinations.

Outputs:
data\research\phase3\EXP-0002\
