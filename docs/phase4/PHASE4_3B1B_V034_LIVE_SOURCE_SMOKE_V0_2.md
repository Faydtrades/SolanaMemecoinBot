# Phase 4.3B1B — v0.3.4 Live Source Smoke v0.2

v0.1 incorrectly treated `Phase1ReadonlyAdapterV01.normalize_rows(...)`
as an iterable of per-row result objects.

The actual adapter contract is:

`events, skipped = normalize_rows(rows)`

v0.2 fixes only this Phase-4 sidecar integration bug.

No collector, adapter, FeatureEngine, strategy, parameter set, or production DB
schema is modified.

Run:

```powershell
python scripts\phase4_3b1b_v034_live_source_smoke_v0_2.py
```

Collector lifecycle remains automatic.
