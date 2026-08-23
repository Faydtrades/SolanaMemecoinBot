# Phase 4.4B1 — Partial-Closure Exit Lifecycle Bridge Hardening v0.1

## Why this bounded step exists

Phase 4.4B v0.1 correctly bound the three persisted Phase-4 positions while
they were OPEN / EXIT_PENDING.

Before the first real simulated exit fill, one additional lifecycle condition
must be supported: the locked tracks can close at different times.

For example:

- FINAL-A can close at +10%;
- SENS-C can close at +20% or its 5s fallback;
- FINAL-B may still be OPEN and trailing afterward.

The v0.1 bridge used `list_open_positions()` as its rebinding source. That API
intentionally excludes CLOSED positions, so after one sibling track closes the
bridge could no longer reconstruct all three locked tracks.

## v0.2 bridge behavior

`paper_exit_lifecycle_bridge_v0_2.py`:

- reconstructs all three persisted positions by `signal_key`, including CLOSED;
- preserves semantic FINAL-A / FINAL-B / SENS-C ordering;
- treats a CLOSED sibling with its immutable persisted ExitIntent as terminal
  replay rather than trying to reopen or mutate it;
- continues evaluating still-OPEN or EXIT_PENDING sibling tracks;
- leaves all strategy thresholds, fallback clocks, exit variants and Phase-3
  locks unchanged.

## Local validation

The exact package self-test was executed before delivery and produced:

`RESULT: PASS`

It validates both staggered market exits and staggered fallback exits, restart,
terminal replay, canonical digest stability and SQLite integrity.

## Run

From the project root with `(.venv)` active:

```powershell
python scripts\phase4_4b1_partial_close_bridge_selftest_v0_1.py
```

No collector, RPC, wallet, live order, PnL/accounting or production DB is used.
