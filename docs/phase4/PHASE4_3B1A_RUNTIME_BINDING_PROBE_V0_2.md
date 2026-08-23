# Phase 4.3B1A — Exact Runtime Binding Probe v0.2

v0.1 had an import-path bug: it attempted to import `src.phase2.*`.

The project uses `src` as the source root, so v0.2 explicitly adds `<project>\src`
to `sys.path` and imports `phase2.*`.

No strategy logic, parameters, database contents, or Phase-4 baselines are changed.

Run with the collector stopped:

```powershell
python scripts\phase4_live_runtime_binding_probe_v0_2.py
```

The probe remains read-only and uses no network, wallet, signing, paper order,
live order, parameter search, or candidate reselection.
