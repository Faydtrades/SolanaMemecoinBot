# Phase 4.3B1B — Controlled Collector Live Diagnostic v0.1

The previous live sidecar smoke saw zero new `pump_events` during 120 seconds.
Before changing any strategy or Phase-4 routing code, this diagnostic isolates the
Phase-1 live collector path.

It runs the existing, unchanged production baseline:

`live_pump_collector_v0_3_3.py`

under a temporary wrapper for 90 seconds by default.

The wrapper:
- snapshots the production DB tail before start;
- starts the collector as a child process;
- captures collector stdout;
- automatically stops the child after the time limit;
- prefers Windows `CTRL_BREAK_EVENT` in a dedicated process group;
- falls back to terminate/kill only if needed;
- snapshots the DB tail afterward;
- reports whether new `pump_events` actually reached SQLite;
- saves an audit JSON and captured collector log under `data\paper\smoke`.

It does NOT:
- change the collector baseline;
- run a strategy;
- create paper orders;
- use a wallet or sign/broadcast a transaction;
- tune/reselect parameters.

Run with no other collector instance running:

```powershell
python scripts\phase4_controlled_collector_live_diagnostic_v0_1.py
```

Do not manually stop it. The wrapper stops it automatically.
