# Phase 4.3B1D0 — Runtime Price-Impact Mechanics Probe v0.2

v0.1 could appear frozen before producing progress because it ran a full
`PRAGMA quick_check` on the grown production database before printing the next
line.

v0.2 fixes only probe execution behavior:

- production SQLite remains query-only/read-only;
- full `PRAGMA quick_check` is skipped by design because earlier Phase-4
  validation already established DB integrity;
- the trade scan is hard-bounded to the latest 100,000 raw `pump_events`
  rowids;
- at most 50,000 eligible BUY/SELL rows are loaded;
- explicit `[STEP 1/3]`, `[STEP 2/3]`, `[STEP 3/3]` progress is printed;
- query elapsed time and exact rowid window are reported.

The reserve-mechanics calculations and safety boundary are unchanged.

Run:

```powershell
python scripts\phase4_3b1d0_runtime_price_impact_probe_v0_2.py
```

No collector is required.
