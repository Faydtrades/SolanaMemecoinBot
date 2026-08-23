# Phase 4.3B1B — v0.3.4 Controlled Live Source Sidecar Smoke v0.1

This re-runs the previously blocked Phase 4.3B1B live source validation using the
new `live_pump_collector_v0_3_4.py`.

The script controls the collector automatically. No separate PowerShell collector
window and no manual Ctrl+C are needed.

Live path under test:

`Pump live stream`
→ `live_pump_collector_v0_3_4.py`
→ `production SQLite`
→ read-only Phase-4 sidecar
→ `Phase1ReadonlyAdapterV01`
→ `FeatureEngineV02`

Only mints whose LAUNCH is observed after the smoke start cursor are admitted to
the causal sidecar set.

This step still does NOT run:
- strategy evaluation;
- CandidateSignal;
- paper orders;
- wallet/signing/live orders;
- parameter search/reselection.

Run:

```powershell
python scripts\phase4_3b1b_v034_live_source_smoke_v0_1.py
```

The collector is started and stopped automatically.
