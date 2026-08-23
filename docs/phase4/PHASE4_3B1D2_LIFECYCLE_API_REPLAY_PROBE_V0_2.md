# Phase 4.3B1D2 — Lifecycle API Replay Probe v0.2

v0.1 attempted to construct `PaperLifecycleStore` against a SQLite connection
opened with `mode=ro`.

That cannot work because `PaperLifecycleStore.__init__()` calls
`create_schema(self.conn)`, which executes schema DDL even when the schema
already exists.

This is a probe-design error, not evidence of a B1D2 lifecycle problem.

v0.2:

1. finds the most recent completed B1D2 paper database;
2. computes its SHA-256;
3. copies it byte-for-byte to `data/selftest`;
4. verifies the copy hash equals the original;
5. runs the real `PaperLifecycleStore` API against the isolated writable copy;
6. calls `list_open_positions()` twice;
7. compares membership/order/types against `LOCKED_PHASE4_TRACKS`;
8. verifies the original paper DB SHA-256 is unchanged afterward.

No collector, network, wallet, live order, or original paper DB write is used.

Run:

```powershell
python scripts\phase4_3b1d2_lifecycle_api_replay_probe_v0_2.py
```
