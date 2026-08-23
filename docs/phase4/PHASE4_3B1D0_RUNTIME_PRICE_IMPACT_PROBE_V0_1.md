# Phase 4.3B1D0 — Runtime Price-Impact Mechanics Probe v0.1

## Why this step exists

Phase 4.3B1C successfully reached a real live `CandidateSignal`, routed that
candidate into one shared paper entry route, and created the three locked paper
orders in `ENTRY_PENDING`.

The router intentionally did **not** fabricate a fill. Its fill API requires
an explicit runtime `price_impact_bps`.

`P4-COST-BASELINE-0001` deliberately does not contain an invented fixed
price-impact haircut. Therefore this bounded step checks the actual Pump trade
and virtual-reserve mechanics before a runtime price-impact model is allowed
to affect paper fills.

## Scope

This probe:

- opens the production SQLite database query-only/read-only;
- does not start the collector;
- does not use RPC/network;
- does not create paper orders or fills;
- does not access a wallet;
- does not tune or reselect strategy parameters;
- inspects a bounded recent slice of real BUY/SELL Pump events;
- compares reported trade amounts with observed virtual-reserve transitions;
- tests whether event reserves behave like post-trade constant-product state;
- compares gross reported amount against an alternate fee-subtracted
  reconstruction rather than assuming fee semantics;
- reports a **diagnostic-only** 0.10 SOL curve-impact hypothesis for SOL_NATIVE;
- deliberately refuses to fabricate a 0.10-SOL-equivalent quote amount for
  QUOTE paths.

## Important

A `RESULT: PASS` means the diagnostic ran correctly. The separate
`MECHANICS_VERDICT` is the empirical result that determines the next build
step.

No formula printed by this probe is considered locked merely because the
script ran.

## Run

From the project root with `(.venv)` active:

```powershell
python scripts\phase4_3b1d0_runtime_price_impact_probe_v0_1.py
```

Copy the complete output back into the ChatGPT project.
