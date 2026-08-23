# Phase 4.3B1C0 — Exact Candidate / Router Contract Capture v0.1

Phase 4.3B1B has passed live.

Before enabling live FirstPullback strategy evaluation and simulated entry routing,
this bounded capture records the exact local runtime contracts needed by B1C:

- StrategyRun / StrategyEvaluation / CandidateSignal;
- FirstPullbackParameterSet and nested strategy dataclasses;
- FirstPullbackStrategyV02 methods;
- StrategyClockV01 methods;
- paper_entry_router_v0_1 classes/functions/methods;
- paper_lifecycle_v0_1 classes/functions/methods;
- exact locked CONTROL / ROBUST_1 / ROBUST_2 / ROBUST_3 role objects found in
  the Phase-3 selection artifacts.

It does not open production SQLite, start the collector, use network/RPC, create
paper orders, or tune/reselect parameters.

Run:

```powershell
python scripts\phase4_3b1c_exact_candidate_router_contract_v0_1.py
```

Return the full output. It should be much smaller than prior broad probes and is
the final contract capture before the actual B1C live runner.
