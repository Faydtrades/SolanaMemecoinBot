# Phase 4.3B1B — Live Source Sidecar Smoke v0.1

This is the first test in Phase 4 that reads **new rows while the Phase-1 collector
is running live**.

It is deliberately narrower than CandidateSignal routing.

## What it validates

`live Pump events → production SQLite → Phase1ReadonlyAdapterV01 → FeatureEngineV02`

Only mints whose `LAUNCH` is observed after the sidecar's start cursor are admitted
to this smoke. This avoids pretending that a pre-existing token's first event seen
by the sidecar is its true `t0`.

## What it does NOT do

- no strategy parameter evaluation;
- no CandidateSignal;
- no paper order;
- no exit logic;
- no wallet/signing/live transaction;
- no parameter search/reselection;
- no writes to the production DB;
- no RPC/network call from the sidecar.

The collector runs separately and remains the validated Phase-1
`live_pump_collector_v0_3_3.py`.

## Run

1. Start the Phase-1 collector in a separate PowerShell window.
2. Keep this sidecar in the project venv and run:

```powershell
python scripts\phase4_live_source_sidecar_smoke_v0_1.py
```

Defaults:
- 120 seconds maximum;
- 250 usable normalized events maximum;
- stops on whichever limit happens first.

A JSON audit is written under:

`data\paper\smoke\`

After this smoke exits, stop the collector with **Ctrl+C once** and wait for its normal
SUMMARY/drain.

`RESULT: PASS` is required before CandidateSignal + paper-entry routing is enabled.
