# Phase 4.3B0 — Live CandidateSignal Integration Preflight v0.1

This is a bounded preflight before the actual Phase 4.3B live smoke.

It deliberately does **not**:
- run the collector,
- use network/RPC,
- create a paper order,
- open a wallet,
- sign/broadcast a transaction,
- change Phase-3 entry parameters,
- select a winner between CONTROL / ROBUST_1 / ROBUST_2 / ROBUST_3.

It checks:
1. the active Phase-1 / Phase-2 / Phase-4 baseline files are present;
2. the production SQLite DB can be opened query-only and has the required live-source fields;
3. the DB is unchanged during the read-only probe;
4. the local Phase-2 source API shape, using AST only;
5. the exact local Phase-3 artifacts that contain the four locked entry roles.

Why this sub-step exists:
Phase 4.3A validated the generic routing contract. The next live runner must bind to the
**actual local Phase-2 APIs and exact locked Phase-3 entry definitions**, not to guessed
function names or to the ROBUST_2 fixture used only by the 4.3A self-test.

Run with the collector stopped for this preflight so the DB unchanged check is meaningful:

```powershell
python scripts\phase4_live_candidate_integration_preflight_v0_1.py
```

`RESULT: PASS` means we have enough exact local binding information to build 4.3B1.
`RESULT: CHECK` means return the output; do not improvise or tune around the missing binding.
