TRADINGBOT — EXP-0006 BOOKKEEPING + EXP-0007 EXITPATH/READINESS v0.1

Extract directly into:
D:\Tradingbot\solana_memecoin_bot_phase1_v0_1

Run with (.venv) active:

1) Lock/bookkeep EXP-0006:
python scripts\phase3_exp0006_bookkeeping_v0_1.py

2) Validate ExitPathReplay foundation:
python scripts\phase3_exp0007_exit_path_replay_selftest_v0_1.py

3) Validate the continuous-development readiness policy:
python scripts\phase3_exp0007_continuous_dev_readiness_selftest_v0_1.py

4) Initialize canonical EXP-0007 design/policy artifacts:
python scripts\phase3_exp0007_initialize_foundation_v0_1.py

All four should end RESULT: PASS.

OPTIONAL — while the production collector is running, inspect current coverage:
python scripts\phase3_continuous_collection_scout_v0_1.py

The scout is read-only and DOES NOT create a freeze. Its 6h/12h/24h milestones
are informational only.

LOCKED READINESS POLICY:
- No fixed wall-clock duration alone makes a dataset ready.
- E1 path characterization:
  >=30 clean 5m paths in EACH robust regime
  AND >=80% clean 5m rate in EACH robust regime.
  Exit threshold tuning remains forbidden.
- E2 coarse exit research:
  >=100 clean 5m paths in EACH robust regime
  AND >=90% clean 5m rate in EACH robust regime.
  Even then, exit ranges must be locked before results.
- CONTROL does not gate readiness.
- A/B/C/D entry parameters stay fixed.
- Deferred high-stringency entry hypotheses remain separate.
- No SL/TP/trailing/time-stop choice, friction model, realistic net PnL or
  profitability claim occurs in EXP-0007 foundation.

OPERATIONAL NEXT STEP AFTER PASS:
Keep the production collector running continuously. Do not stop just because a
clock milestone is reached. When the collection looks substantial, use the scout
and then return to ChatGPT; the next artifact should select/freeze a fresh,
well-covered development window and replay the locked entry regimes against it.
