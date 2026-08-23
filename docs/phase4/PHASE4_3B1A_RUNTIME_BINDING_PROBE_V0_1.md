# Phase 4.3B1A — Exact Runtime Binding Probe v0.1

This is a deliberately small bridge between the successful 4.3B0 preflight and
the actual live CandidateSignal → paper-entry smoke.

Why it exists:
- 4.3B0 proved that the active Phase-2/3 files and all four locked entry roles exist.
- The actual live runner must construct the exact local StrategyRun and nested
  FirstPullbackParameterSet objects.
- Those constructor details should be read from the installed code, not guessed.

This probe:
- imports the active Phase-2 modules;
- prints exact Python signatures;
- prints dataclass fields/types/defaults;
- reads only the last 30 Pump rows from production SQLite in query-only mode;
- runs those rows through the validated Phase1ReadonlyAdapterV01;
- runs the resulting events through FeatureEngineV02;
- finds the relevant Phase-3 JSON artifacts and prints role-bearing parameter objects.

It does NOT:
- start the collector;
- use network/RPC;
- change production DB;
- create paper orders;
- use a wallet;
- sign/broadcast transactions;
- tune/reselect CONTROL / ROBUST_1 / ROBUST_2 / ROBUST_3.

Run with collector stopped:

```powershell
python scripts\phase4_live_runtime_binding_probe_v0_1.py
```

Expected duration: seconds.
