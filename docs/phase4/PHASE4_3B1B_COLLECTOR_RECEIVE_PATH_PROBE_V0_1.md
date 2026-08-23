# Phase 4.3B1B — Collector Receive-Path Probe v0.1

The direct diagnostic proved all three external layers work:

- HTTP RPC: PASS
- WebSocket `slotSubscribe`: PASS
- Pump `logsSubscribe`: PASS

Therefore this probe inspects the exact local production collector
`live_pump_collector_v0_3_3.py` without executing or modifying it.

It extracts:
- WebSocket/RPC/Pump constants;
- subscription payload source lines;
- receive-loop source lines;
- timeout/watchdog lines;
- relevant functions and line ranges;
- structural contract checks against the known-good direct test.

It does not:
- start the collector;
- open production SQLite;
- use network/RPC;
- create paper/live orders;
- change any Phase-3 or Phase-4 parameters.

Run:

```powershell
python scripts\phase4_collector_receive_path_probe_v0_1.py
```

Return the full output (or screenshots covering the source-hit/context sections).
