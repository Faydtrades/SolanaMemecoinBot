# Phase 4.3B1B — Collector Startup DB Probe v0.1

This probe tests the current leading hypothesis without changing the production DB.

The production collector performs this sequence before opening the WebSocket:

`connect_db -> ensure_schema -> recover_jobs -> COLLECTOR_V0_3_START -> connect WS`

`recover_jobs()` contains historical full-dataset backfill queries. As the production
DB grows, those queries can become too expensive to run synchronously in the
live listener before its first asynchronous WebSocket operation.

This probe:
- opens production SQLite strictly read-only/query-only;
- prints recent `collector_events`;
- prints current queue-status counts;
- runs read-only SELECT equivalents of the two historical `recover_jobs()` backfills;
- shows SQLite query plans;
- aborts either query after 10 seconds via SQLite's progress handler.

It does not start the collector, use network/RPC, or modify any data.

Run:

```powershell
python scripts\phase4_collector_startup_db_probe_v0_1.py
```
