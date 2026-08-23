# Phase 4.3B1C — Live CandidateSignal → Simulated Entry Routing Smoke v0.2

## Correction from v0.1

The first live B1C run exposed a runtime binding error:

- locked Phase-3 entry parameters are FirstPullback `v1.1`;
- B1C v0.1 incorrectly imported `strategy_clock_v0_1`;
- that clock delegates parameter validation to `FirstPullbackStrategyV01`
  and therefore rejects `strategy_version="v1.1"`.

v0.2 changes only the strategy-clock binding:

- module: `strategy_clock_v0_2`
- class: `FirstPullbackStrategyClockV02`

This is the existing compatibility clock already used by the project's
FirstPullback v1.1 compatibility self-test.

v0.2 also adds a preflight guard before collector startup so the runner will
fail immediately if the exact locked v1.1 parameter sets and clock contract
are incompatible.

No entry parameters, strategy thresholds, cost baseline, paper tracks,
collector logic, wallet logic, or routing semantics are changed.

Run:

```powershell
python scripts\phase4_3b1c_live_candidate_route_smoke_v0_2.py
```

The runner auto-stops. Do not use Ctrl+C.
