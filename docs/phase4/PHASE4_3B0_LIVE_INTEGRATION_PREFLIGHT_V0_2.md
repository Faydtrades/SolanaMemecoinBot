# Phase 4.3B0 — Live CandidateSignal Integration Preflight v0.2

This replaces v0.1 of the preflight.

## Why v0.2 exists

v0.1 used `PRAGMA quick_check` against the large production SQLite database.
That check can be expensive and can remain inside SQLite native code long enough
that Ctrl+C does not immediately interrupt it.

The preflight does not need a full database integrity scan because prior phases
already validated database integrity. Its purpose is only to bind Phase 4.3B to
the exact local Phase-2/3 interfaces.

## v0.2 changes

v0.2 deliberately removes:

- `PRAGMA quick_check`
- `COUNT(*)` over `pump_events`
- any other deliberate full-table scan

It keeps:

- query-only/read-only DB open
- sqlite schema inspection
- required table checks
- required column checks
- a fast `ORDER BY rowid DESC LIMIT 1` tail lookup
- DB size/mtime unchanged check
- active baseline file checks
- local Phase-2 API AST inspection
- discovery of Phase-3 artifacts containing:
  - CONTROL
  - ROBUST_1
  - ROBUST_2
  - ROBUST_3
- no parameter search
- no candidate reselection
- no wallet
- no live orders
- no network/RPC

## Run

Keep the collector stopped for this preflight.

With the project venv active:

```powershell
python scripts\phase4_live_candidate_integration_preflight_v0_2.py
```

Expected duration: normally seconds, not minutes.

`RESULT: PASS` means the exact local interfaces/artifacts needed for Phase 4.3B1
were discovered.

`RESULT: CHECK` means return the output and do not guess around the missing binding.
