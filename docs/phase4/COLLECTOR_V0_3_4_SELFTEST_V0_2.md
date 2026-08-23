# Collector v0.3.4 Queue Cutover Self-Test v0.2

v0.1 contained one false-negative validator:

`recover_no_pump_events_scan`

It searched the entire source text of `recover_jobs()` for the token
`pump_events`. The v0.3.4 function intentionally contains a comment explaining
that historical `pump_events` are not rescanned, so the comment itself caused
the check to fail.

v0.2 fixes only the validator. It parses the function with Python AST and checks
actual string literals / SQL content. Comments are ignored.

The generated `live_pump_collector_v0_3_4.py` is not modified by this package.

Run:

```powershell
python scripts\live_pump_collector_v0_3_4_selftest_v0_2.py
```

Only after RESULT: PASS should the controlled live smoke be run.
