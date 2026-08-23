# Phase 4.5C v0.2 — Decimal-safe live observability binding hotfix

## Diagnosis from the first live 4.5C run

The live run completed the actual paper-trading path successfully:

- accepted causal SOL_NATIVE entry;
- all FINAL-A / FINAL-B / SENS-C positions CLOSED;
- causal exit fills;
- live MTM marks persisted;
- per-track MTM equity persisted;
- final MTM equity reconciled to realized net PnL;
- live terminal decision audit persisted;
- SQLite quick_check passed.

The only failed validation was `Observability runtime errors`.

The captured error was:

```text
TypeError: Object of type Decimal is not JSON serializable
```

The source was the live Phase-2 `evaluation.audit_snapshot`. The binding's
canonical primitive conversion handled dataclasses, enums, datetimes,
containers, etc. but did not handle `decimal.Decimal`.

## v0.2 correction

`paper_live_observability_binding_v0_2.py`:

- versions the binding as `P4-LIVE-OBSERVABILITY-BINDING-0002`;
- converts finite Decimal values to their exact base-10 string form before
  canonical JSON persistence;
- rejects non-finite Decimal audit values fail-closed;
- leaves the locked observability model, entry/exit strategies, costs,
  price-impact models, and all trading parameters unchanged.

Decimal values are stored as exact strings to avoid any float round-trip or
precision change in the audit trail.

## Pre-delivery validation

The exact packaged v0.2 binding self-test was executed locally and returned:

`RESULT: PASS`

The regression self-test explicitly includes nested Decimal values and verifies
their exact serialized JSON representation.

The existing Phase 4.5B observability self-test and exact-schema fixture
integration smoke were also re-run after the change and both returned:

`RESULT: PASS`

The v0.2 live runner compiles against the same locked contracts. The real
collector/Phase-2 market path necessarily requires user-project live
validation.

## Run

```powershell
python scripts\phase4_5c_live_observability_binding_selftest_v0_2.py

if ($LASTEXITCODE -eq 0) {
    python scripts\phase4_5c_live_observability_smoke_v0_2.py
}
```
