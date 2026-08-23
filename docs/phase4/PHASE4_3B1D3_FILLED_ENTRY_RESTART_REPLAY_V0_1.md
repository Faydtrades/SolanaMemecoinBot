# Phase 4.3B1D3 — FILLED Entry Restart / Replay Persistence v0.1

## Purpose

Phase 4.3B1D2 produced the first causal live SOL_NATIVE paper entry:

- one FILLED shared entry route;
- three FILLED paper orders;
- three OPEN paper positions;
- FINAL-A / FINAL-B / SENS-C exact membership.

The apparent B1D2 failure was proven to be an order-only validator false
negative: `list_open_positions()` sorts by `open_at, paper_position_id`, not by
the semantic locked-track tuple.

B1D3 now validates restart safety and exact replay idempotence of that already
FILLED entry before live exit orchestration begins.

## Method

No new live market run is performed.

The test:

1. locates the latest B1D2 paper DB and matching audit;
2. SHA-256 hashes the original;
3. creates a byte-identical isolated copy under `data/selftest`;
4. reopens the copy through the real Phase-4 router/lifecycle;
5. verifies FILLED route/orders and OPEN positions survive restart;
6. restarts again;
7. exact-replays the original CandidateSignal;
8. exact-replays the original causal entry observation with the same locked
   runtime price-impact input;
9. verifies the canonical router digest does not change;
10. restarts a third time and verifies state still matches;
11. proves the original B1D2 DB SHA-256 is unchanged.

Position list ordering is intentionally treated as non-semantic; exact track
membership and one-to-one order linkage are the invariant.

## Run

```powershell
python scripts\phase4_3b1d3_filled_entry_restart_replay_v0_1.py
```

No collector, RPC, wallet, live order, parameter tuning, or original paper DB
write is used.
