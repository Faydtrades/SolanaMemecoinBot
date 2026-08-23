# Phase 4.3B1B — v0.3.4 Live Source Smoke v0.3

v0.2 fixed the adapter return contract but exposed the exact remaining binding error:

`NORMALIZATION_ERROR:IndexError:No item with that key`

`Phase1ReadonlyAdapterV01` uses Phase-1 SQLite rowid as deterministic `ingest_seq`
and its own cohort SQL exposes it as:

`rowid AS p1_rowid`

The v0.2 sidecar instead exposed:

`rowid AS _paper_source_rowid`

v0.3 fixes only this Phase-4 sidecar row-shape contract.

No collector, production schema, adapter, FeatureEngine, strategy or parameter
set is modified.

Run:

```powershell
python scripts\phase4_3b1b_v034_live_source_smoke_v0_3.py
```

Collector lifecycle is automatic.
