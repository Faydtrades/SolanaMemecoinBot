# Phase 4.3B1D2 — Lifecycle API Replay Probe v0.1

This probe follows the first postrun audit.

The paper DB itself proved:
- three filled orders;
- three open positions;
- exact FINAL-A / FINAL-B / SENS-C membership;
- correct one-to-one order→position linkage.

The remaining question is why the original live runner's
`position_tracks_match_lock` check failed.

This probe opens the same paper DB query-only/read-only and calls the actual
`PaperLifecycleStore.list_open_positions()` API twice. It compares:

- runtime API track order;
- runtime API track value types;
- raw DB row order;
- `LOCKED_PHASE4_TRACKS`;
- repeat determinism.

It also prints the source of `list_open_positions()`.

No live market run is performed.
