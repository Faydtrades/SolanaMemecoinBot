# Phase 4.5C v0.3 — Exit-schema initialization ordering hotfix

## Diagnosis from live v0.2

The live v0.2 run again completed the actual trading path:

- accepted causal SOL_NATIVE entry;
- FINAL-A / FINAL-B / SENS-C all CLOSED;
- live MTM marks persisted;
- per-track MTM equity persisted;
- final equity reconciled to realized net PnL;
- live strategy decision audit persisted;
- SQLite quick_check passed.

The single failed validation was `Observability runtime errors`.

The exact error was:

```text
paper_exit_execution_routes missing required columns: [...]
```

## Root cause

On a fresh live paper DB, immediately after the entry fill, the runner did this
in the wrong order:

1. persist the first position MTM/equity mark;
2. only afterward construct `PaperExitFillExecutorV01`.

`record_track_equity_for_market()` calls deterministic completed-trade
accounting. That accounting validates the exact exit-execution schema even when
there are currently zero completed exits.

Therefore the first equity call occurred before
`PaperExitFillExecutorV01` had created `paper_exit_execution_routes` and
`paper_exit_execution_attempts`.

This was a runtime initialization-order bug, not a strategy, accounting,
collector, market-data or paper-fill error.

## v0.3 correction

The controlled live runner now performs:

```text
entry FILLED
→ construct exit lifecycle bridge
→ construct PaperExitFillExecutorV01
→ validate exact accounting source schema
→ bind the three locked exit tracks
→ persist first post-entry MTM/equity observation
```

No strategy parameters, exits, fees, latency, price-impact model, observability
model or Decimal binding are changed.

The live binding remains the already validated:

`P4-LIVE-OBSERVABILITY-BINDING-0002`

## Pre-delivery validation

The exact regression self-test reproduces the live v0.2 failure on a fresh
fixture with the exit-execution tables deliberately absent, then verifies that
the v0.3 initialization ordering creates the exact required schema before the
first equity mark.

Local results before delivery:

```text
ENTRY/EXIT-SCHEMA ORDER REGRESSION: PASS
4.5C Decimal-safe binding regression: PASS
4.5B observability regression: PASS
4.5B exact-schema integration regression: PASS
v0.3 runner compile/import contract: PASS
runner ordering source contract: PASS
```

The actual collector/Phase-2 live path still requires user-project validation.

## Run

No other collector should be running.

```powershell
python scripts\phase4_5c_entry_equity_schema_order_selftest_v0_1.py

if ($LASTEXITCODE -eq 0) {
    python scripts\phase4_5c_live_observability_smoke_v0_3.py
}
```
