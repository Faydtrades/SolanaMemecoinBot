# Phase 4.3B1A — Exact Runtime Binding Probe v0.3

v0.3 fixes the runtime import ambiguity seen in v0.2.

Instead of importing a generic `phase2` package, it creates an isolated namespace
whose `__path__` points directly to:

`<project>\src\phase2`

Every imported module is then verified by exact `__file__` path.

This prevents any installed or unrelated `phase2` package from shadowing the
tradingbot's local Phase-2 modules.

The probe remains read-only:
- no RPC/network;
- no wallet/signing/live order;
- no paper order;
- no parameter search or candidate reselection;
- production SQLite is opened query-only.

Run with collector stopped:

```powershell
python scripts\phase4_live_runtime_binding_probe_v0_3.py
```
