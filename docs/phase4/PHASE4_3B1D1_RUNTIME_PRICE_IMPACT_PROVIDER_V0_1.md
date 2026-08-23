# Phase 4.3B1D1 — Runtime Price-Impact Provider v0.1

## Locked scope

This step versions the runtime curve-impact mechanic supported by the
Phase 4.3B1D0 real-data probe. It does **not** create a paper fill.

For `SOL_NATIVE` entry:

```text
q = requested SOL curve input
x = current virtual SOL reserve before our simulated order

exact impact bps = q * 10,000 / x
router integer bps = ceil(exact impact bps)
```

The formula comes directly from the constant-product average-price relationship:

```text
average_execution_price / pre_trade_spot_price - 1 = q / x
```

The ceiling rule is an explicit conservative router rounding policy so
fractional impact is never understated.

## Fees remain separate

Phase 4.3B1D0 showed gross reported SOL input reconstructed observed reserve
mechanics better than subtracting the event fee. This provider therefore does
not subtract venue, priority, builder-tip, or base fees from the curve input.

Those costs remain the responsibility of `PaperCostModelV02`.

## Quote policy

The Phase-4 reference position is 0.10 SOL. A `QUOTE:<mint>` curve requires a
quote-raw input amount, not a SOL amount.

Until there is a causal SOL→quote conversion available at the relevant
observation, the provider returns:

```text
QUOTE_INPUT_CONVERSION_UNAVAILABLE
```

It never invents an exchange rate.

## Run self-test

```powershell
python scripts\phase4_3b1d1_runtime_price_impact_selftest_v0_1.py
```

No production DB, collector, network, wallet, orders, or fills are used.
