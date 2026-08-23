# Phase 4.3B1B — Adapter Boundary Probe v0.1

The v0.3.4 live source smoke proved:

- collector produced new live Pump rows;
- new LAUNCH rows were observed;
- causally new mints were identified;
- `Phase1ReadonlyAdapterV01` produced zero usable normalized events.

This probe reopens the exact latest smoke rowid window read-only and reproduces
the adapter decision offline.

It reports:
- skip-reason distribution;
- representative rejected source rows;
- adapter source windows around skip/source/schema guards.

It does not:
- start collector;
- use network/RPC;
- modify production SQLite;
- evaluate strategy;
- create paper/live orders;
- change parameters.

Run:

```powershell
python scripts\phase4_3b1b_adapter_boundary_probe_v0_1.py
```
