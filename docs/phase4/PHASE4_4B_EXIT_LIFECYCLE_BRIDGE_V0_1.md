# Phase 4.4B — Exit Lifecycle Bridge v0.1

This bounded step connects the already validated persistent exit-intent engine
from Phase 4.4A to the existing paper position lifecycle.

It binds a persisted FILLED shared entry route plus the matching three
OPEN/EXIT_PENDING paper positions into exact `ExitPositionRef` objects for:

- FINAL-A / E3_TP10_T15
- FINAL-B / E4_ACT10_GB03_T15
- SENS-C / SENS_TP20_T5

When `PaperExitOrchestratorV01` emits an `ExitIntent`, the bridge performs only:

`OPEN -> EXIT_PENDING`

using `PaperLifecycleStore.request_exit()` at the exact deterministic
`ExitIntent.requested_at` timestamp.

It intentionally does **not**:

- close positions;
- invent an exit market price;
- calculate exit price impact;
- calculate PnL;
- use network/RPC;
- access a wallet;
- tune/reselect exit rules.

The local self-test creates real isolated entry-router/lifecycle state using the
locked Phase-4 cost baseline, then validates market TP/trailing intents,
fallback intents, lifecycle persistence, replay idempotence, restart safety and
SQLite integrity.

Run:

```powershell
python scripts\phase4_4b_exit_lifecycle_bridge_selftest_v0_1.py
```
