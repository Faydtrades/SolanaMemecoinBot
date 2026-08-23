# Phase 4.3B1B — Controlled Collector Live Diagnostic v0.2

v0.1 could hang because the wrapper called `readline()` on the collector's stdout.
If the child produced no newline, the wrapper could not return to its timer loop.

v0.2 fixes this by:
- redirecting collector stdout/stderr directly to a log file;
- running the child with unbuffered Python;
- never reading child stdout while the child is running;
- polling child status and production DB every 250 ms;
- printing a heartbeat every 5 seconds;
- stopping automatically after 90 seconds;
- attempting CTRL_BREAK first, then terminate/kill fallback.

The production collector baseline itself is unchanged.

Run only after any old collector/wrapper processes have been stopped:

```powershell
python scripts\phase4_controlled_collector_live_diagnostic_v0_2.py
```

Do not manually stop it. It should show `[HEALTH]` lines within seconds.
