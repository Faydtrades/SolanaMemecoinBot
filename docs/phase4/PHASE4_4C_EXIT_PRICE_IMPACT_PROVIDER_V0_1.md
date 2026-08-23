# Phase 4.4C — SOL_NATIVE Exit Price-Impact Provider v0.1

## Purpose

Phase 4.4B proved deterministic binding from the three persisted Phase-4 paper
positions into lifecycle `EXIT_PENDING`.  The next required execution input is
runtime exit curve impact.

This step versions only that mechanic.  It does **not** close a position or
calculate PnL.

## Persisted token inventory

`PaperPosition v0.1` persists:

- filled SOL notional (`filled_size_lamports`);
- exact rational simulated entry execution price.

For `SOL_NATIVE`, price is lamports per token-raw unit, so token inventory is
reconstructed as:

```text
exact tokens = filled_size_lamports * entry_price_denominator
               / entry_price_numerator
```

The provider floors this to integer token-raw units.  Any fractional raw-token
remainder is retained for audit.  Flooring ensures the paper executor never
sells more token inventory than the persisted entry supports.

## Exit constant-product impact

Before the hypothetical sell:

```text
y = current virtual token reserve
q = reconstructed token input
```

Under `x*y=k`, average sell execution price relative to pre-trade spot is:

```text
average / spot = y / (y + q)
```

Therefore adverse curve impact is:

```text
impact_bps = q * 10,000 / (y + q)
```

The existing Phase-4 cost model accepts integer bps.  The provider rounds UP so
fractional adverse impact is never understated.

## Important boundaries

- fees remain separate in `PaperCostModelV02`;
- no fixed exit-impact haircut is introduced;
- no QUOTE inventory or SOL→quote conversion is invented;
- no production DB, collector, wallet, order, fill, or PnL path is used.

## Run

```powershell
python scripts\phase4_4c_exit_price_impact_selftest_v0_1.py
```

Proceed only on `RESULT: PASS`.
