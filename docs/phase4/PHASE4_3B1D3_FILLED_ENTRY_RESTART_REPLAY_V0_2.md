# Phase 4.3B1D3 — FILLED Entry Restart / Replay Persistence v0.2

## Correction from v0.1

v0.1 failed inside its own snapshot query:

```text
sqlite3.OperationalError: no such column: candidate_id
```

The reason is a probe schema mistake:

- `EntryRoute` contains `candidate_id`;
- `PaperOrder` contains `signal_key`, not `candidate_id`.

The exact Phase-4 runtime contract already captured this distinction.

v0.2 fixes the probe only.

## v0.2 snapshot contract

- route state is read through the public `PaperEntryRouterV01.list_routes()` API;
- order state reads only actual `PaperOrder` columns:
  - paper_order_id
  - signal_key
  - mint
  - track_id
  - state
  - requested_size_lamports
  - filled_size_lamports
  - simulated entry price
- positions are read through `PaperLifecycleStore.list_open_positions()`.

Ordering of positions remains explicitly non-semantic. Semantic snapshots are
sorted before restart/replay comparison.

## Validation

The probe still uses an exact isolated copy of the completed B1D2 paper DB and
tests:

1. FILLED route survives restart;
2. three FILLED orders survive restart;
3. three OPEN positions survive restart;
4. exact locked FINAL-A / FINAL-B / SENS-C membership;
5. exact CandidateSignal replay is idempotent;
6. exact fill-observation replay is idempotent;
7. canonical router digest is unchanged;
8. a third restart reproduces the same semantic state;
9. original B1D2 DB SHA-256 remains unchanged.

Run:

```powershell
python scripts\phase4_3b1d3_filled_entry_restart_replay_v0_2.py
```

No collector or live market run is required.
