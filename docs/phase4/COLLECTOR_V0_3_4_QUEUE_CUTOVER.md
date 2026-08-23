# Collector v0.3.4 — Queue Generation Cutover

Phase 4.3B1B identified a scaling regression in the Phase-1 v0.3.3 startup path.

v0.3.3 performs historical queue backfills before opening its WebSocket, and the
pre-v0.3.4 queue generation has grown large enough to delay/starve fresh live work.

v0.3.4 keeps the validated market/decode/persistence design but isolates active
background queues:

- confirmation_jobs_v034
- deep_enrichment_jobs_v034
- gap_jobs_v034

The old queue tables are preserved untouched.

v0.3.4 recover_jobs() only resumes genuinely interrupted v0.3.4 jobs.
It does not rescan historical pump_events or transactions at live startup.

Build:
python scripts\build_live_pump_collector_v0_3_4.py

Self-test:
python scripts\live_pump_collector_v0_3_4_selftest.py

Controlled live smoke, only after both above PASS:
python scripts\live_pump_collector_v0_3_4_controlled_smoke.py
