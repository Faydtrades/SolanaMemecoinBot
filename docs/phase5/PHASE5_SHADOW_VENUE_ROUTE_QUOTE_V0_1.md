# Phase-5 Shadow Venue State + Route + Executable Quote v0.1

## Status and boundary

`MEME-P5-T002` is `PASS / ACCEPTED / CHECKPOINTED`.

This isolated layer converts an accepted `ExecutionIntentV01` into immutable,
auditable public-chain account evidence, a deterministic route decision, and
an immutable executable quote. It does not construct instructions or
transactions and has no recent-blockhash, simulation, key-material, wallet,
submission, or broadcast capability. It is not connected to the active
Phase-4 runtime.

## Contract identity

- Model: `P5-SHADOW-VENUE-ROUTE-QUOTE-0001`
- Schema: `phase5_shadow_venue_route_quote_v0.1`
- Venue-state schema: `phase5_immutable_venue_state_v0.1`
- Route schema: `phase5_immutable_route_v0.1`
- Quote schema: `phase5_immutable_executable_quote_v0.1`
- Policy schema: `phase5_quote_policy_v0.1`
- Model fingerprint:
  `3b0219e1b30bceb7c77bde4674ada8069a1cbf8aaf8669844dc03b09c53d59d0`

The fingerprint is SHA-256 over canonical sorted compact JSON describing the
supported venues, route outcomes, context-slot ordering, dynamic fees,
integer quote sources, canonical-pool proof, immutable quote binding,
capability allow-list, and T003 handoff.

The accepted T001 model and fingerprint remain unchanged:

```text
P5-SHADOW-DOMAIN-CAPABILITY-FIREWALL-0001
506312a81b6d8acc724cb27d6d450086bc3fc4986dcdea4831b998a3a8ec91e3
```

## Official protocol sources used

Protocol verification was performed on 2026-08-27 without live RPC access.
The implementation uses:

- official Pump public IDLs on `pump-fun/pump-public-docs` `main`:
  [`pump.json`](https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump.json),
  [`pump_amm.json`](https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump_amm.json),
  and [`pump_fees.json`](https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump_fees.json);
- official dynamic-fee reference:
  [`FEE_PROGRAM_README.md`](https://github.com/pump-fun/pump-public-docs/blob/main/docs/FEE_PROGRAM_README.md);
- official Pump program state/migration reference:
  [`PUMP_PROGRAM_README.md`](https://github.com/pump-fun/pump-public-docs/blob/main/docs/PUMP_PROGRAM_README.md);
- official PumpSwap state/effective-reserve reference:
  [`PUMP_SWAP_README.md`](https://github.com/pump-fun/pump-public-docs/blob/main/docs/PUMP_SWAP_README.md);
- official `@pump-fun/pump-sdk@1.36.0` source files
  `src/bondingCurve.ts`, `src/fees.ts`, and `src/pda.ts`; and
- official `@pump-fun/pump-swap-sdk@1.19.0` source files
  `src/sdk/buy.ts`, `sell.ts`, `fees.ts`, `util.ts`, and `pda.ts`.

The review manifest records the package-integrity values for the exact SDK
source files. No network result is persisted as executable state, and no live
account values are claimed.

Canonical program identities are:

```text
Pump       6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
PumpSwap   pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA
Pump Fees  pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ
WSOL       So11111111111111111111111111111111111111112
```

## Read-only RPC and causal snapshots

`ReadOnlyRpcV01` contains one injectable public-state method:
`get_multiple_accounts(pubkeys, min_context_slot=...)`. There is deliberately
no concrete network client in T002. Tests use a deterministic fake.

Dependent reads are assembled as:

1. read route-defining primary accounts;
2. derive/read dependent accounts at `minContextSlot >= primary slot`;
3. re-read primary accounts at `minContextSlot >= dependent slot`;
4. compare pubkey, owner, and SHA-256 for every primary account; and
5. retry a bounded number of times, then fail closed if the race persists.

Every immutable state records an honest `slot_min`/`slot_max`, per-account
context slots, local `observed_at_us`, pubkey, owner, data length, and data
SHA-256. A multi-slot state cannot be constructed without explicit
per-account slots matching the declared span. Local time is forensic metadata
only and never supersedes slot ordering.

## Account decoding and validation

All economic fields are Python integers. Borsh booleans accept only `0` or
`1`; truncated current appended layouts fail closed. Extra trailing account
capacity is tolerated because both programs support account extension.

### Pump bonding curve

The decoder verifies the Anchor discriminator, Pump owner, PDA derived from
`["bonding-curve", mint]`, mint supply, token-program owner, SOL/WSOL quote
identity, global PDA, fee-config PDA and bump, and the full current appended
curve layout:

- virtual token and quote reserves;
- real token and quote reserves;
- token total supply;
- `complete`;
- creator;
- mayhem and cashback flags; and
- quote mint.

An active curve requires positive virtual reserves and real token reserves.
A completed curve is recognized only when `complete == true` and
`real_token_reserves == 0`. Any disagreement between those fields is invalid.

### Canonical PumpSwap pool

The pool decoder verifies the Anchor discriminator, PumpSwap owner, stored
PDA bump, index `0`, base mint, WSOL quote mint, and the exact canonical
relationships:

```text
creator = Pump PDA ["pool-authority", base_mint]
pool = PumpSwap PDA ["pool", u16_le(0), creator, base_mint, WSOL]
```

Canonicality uses `Pool.creator`, not `Pool.coin_creator`. Both vault pubkeys
must equal the stored pool fields; their SPL account mints and authorities
must match the pool; the WSOL vault must use legacy SPL Token. The base mint
and vault may use legacy SPL Token or Token-2022. Current appended fields
include `coin_creator`, mayhem/cashback flags, and signed `i128`
`virtual_quote_reserves`.

Quote math uses:

```text
effective_quote_reserve = raw_quote_vault_amount + virtual_quote_reserves
```

Zero raw reserves, non-positive effective reserves, malformed vaults,
disabled buy/sell flags, or broken identity relationships fail closed.

## Dynamic fees

The relevant Pump/PumpSwap global account and Pump Fees `FeeConfig` account
are decoded and fingerprinted. `FeeConfig` contains flat fees, ascending
market-cap tiers, and stable tiers. SOL-paired canonical routes select from
the dynamic market-cap tiers; missing, malformed, unsorted, duplicated, or
wrong-owner fee evidence fails closed.

Market cap is integer floor division:

```text
Pump:     virtual_quote_reserve * token_supply / virtual_token_reserve
PumpSwap: effective_quote_reserve * base_mint_supply / raw_base_reserve
```

Below the first threshold selects the first tier. At and above a threshold,
the last tier whose threshold is `<= market_cap` is selected. Every fee
component is `ceil(amount * bps / 10_000)`. Pump applies protocol and creator
fees; its tier LP component is not a bonding-curve charge. PumpSwap preserves
LP, protocol, and creator components. Creator fee is zero when the relevant
on-chain creator field is the default pubkey.

## Route table

| Verified state | Outcome |
|---|---|
| active Pump curve, no canonical pool | `ROUTE_PUMP_BONDING_CURVE` |
| completed Pump curve, canonical pool absent | `NO_ROUTE_MIGRATION_PENDING` |
| completed Pump curve, verified canonical pool | `ROUTE_PUMPSWAP_CANONICAL` |
| active Pump curve plus canonical pool | `NO_ROUTE_AMBIGUOUS` |
| broken intent/state lineage | `NO_ROUTE_INVALID_STATE` |
| no supported verified Pump state | `NO_ROUTE_UNSUPPORTED` |

Arbitrary PumpSwap pools never become routes. A completed curve does not imply
that permissionless/idempotent migration has already created the pool.

## Executable quote arithmetic

Quotes bind the intent ID/fingerprint, exact venue state ID/fingerprint,
route ID/fingerprint, policy fingerprint, fee-config fingerprint, context-slot
span, selected fee tier, and all resulting integer economics. A changed
timestamp, slot, account fingerprint, reserve, flag, or fee tier produces a
new state and quote; quotes are never refreshed in place.

The only v0.1 policy field is integer `slippage_bps` (`0..5000`). BUY intent
input is explicitly `SOL_LAMPORTS`; SELL input is `MEME_BASE_UNITS`.

### BUY / exact quote budget

Pump mirrors official SDK 1.36.0 exact-quote behavior:

```text
spendable = floor((budget - 1) * 10000 / (10000 + applicable_fee_bps))
base_out  = floor(spendable * virtual_base /
                  (virtual_quote + spendable))
```

PumpSwap mirrors official SDK 1.19.0 `buyQuoteInput` behavior. It performs the
same spendable-fee reconciliation, then uses `spendable - 1` in its constant-
product input:

```text
base_out = floor(raw_base * (spendable - 1) /
                 (effective_quote + spendable - 1))
```

The minimum base output is floor-rounded by policy. Pump real-token exhaustion
and PumpSwap depletion fail closed rather than silently fabricating liquidity.

### SELL / exact base input

Both venues use:

```text
raw_quote = floor(input_base * quote_reserve /
                  (base_reserve + input_base))
net_quote = raw_quote - ceil(each independent fee component)
min_quote = floor(net_quote * (10000 - slippage_bps) / 10000)
```

Pump checks real quote reserves. PumpSwap checks the real vault can cover
`raw_quote - lp_fee`; its LP fee remains in the pool. Reserve deltas and an
integer PPM impact metric are bound into the quote. No float or mark-to-market
approximation appears in the production math.

## Independent reference vectors

The self-test contains four immutable, hand-derived worksheet vectors rather
than comparing the implementation with a second Python copy of itself. The
inputs use a 1% slippage policy and the documented integer equations above.

| Venue/side | Exact reference result |
|---|---|
| Pump BUY, 100,000,000 quote | spendable `98,522,166`; protocol `985,222`; creator `492,611`; base `3,273,322,372`; minimum `3,240,589,148` |
| Pump SELL, 10,000,000,000 base | raw `297,029,702`; protocol `2,970,298`; creator `1,485,149`; net `292,574,255`; minimum `289,648,512` |
| PumpSwap BUY, 100,000,000 quote | spendable `98,667,981`; LP `148,002`; protocol `789,344`; creator `394,672`; base `2,424,433,385`; minimum `2,400,189,051` |
| PumpSwap SELL, 10,000,000,000 base | raw `397,058,823`; LP `595,589`; protocol `3,176,471`; creator `1,588,236`; net `391,698,527`; minimum `387,781,541` |

The PumpSwap vectors use raw quote reserve `20,000,000,000` plus non-zero
virtual quote reserve `250,000,000`. A raw-only calculation is asserted to
produce a different result. These constants catch floor/ceiling and `-1`
direction regressions.

## Persistence and lifecycle integration

T002 reuses the T001 database path under `data/shadow` but creates only
separate versioned tables:

- `shadow_t002_venue_states`;
- `shadow_t002_routes`; and
- `shadow_t002_quotes`.

Rows foreign-key to the immutable T001 intent. WAL, `synchronous=FULL`, foreign
keys, append-only triggers, exact replay, conflict rejection, `quick_check`,
full identity/fingerprint audit, and reopen digest are tested. Paths under
`data/paper` and `data/db` fail before creation.

Integration ordering is intentionally recoverable without rewriting T001:

```text
CREATED
-> ELIGIBILITY_CHECKED       persist immutable venue state
-> ROUTE_BOUND               persist executable route
-> QUOTE_BOUND               persist executable quote
```

The public T001 transition occurs before the matching T002 route/quote row.
If the process stops between those operations, replay completes the missing
append-only row using the fingerprint already carried in transition evidence.
A T002 row can therefore never exist before its prerequisite T001 state.
Fail-closed route evidence is admitted only after a T001 terminal negative
state. T002 never progresses beyond `QUOTE_BOUND`.

## Focused evidence and adversarial review

- T002 offline self-test: `100/100 PASS`;
- T001 recursive firewall/regression: `43/43 PASS`;
- T001 fingerprint: unchanged;
- T002 repository quick check: `ok`;
- T002 foreign keys: enabled and clean;
- protected active Phase-4 hashes: exact; and
- network/RPC, production DB, active paper DB, observability registry,
  collector, and live runtime: not accessed.

The pre-delivery review found and fixed account-slot overstatement, missing
vault-authority validation, missing pool/fee PDA-bump validation, incomplete
reopen identity auditing, constructor paths that could admit incomplete
evidence or malformed quote shapes, and incomplete multi-state route lineage.
Regression tests cover each correction.

## T003 handoff

T003 may consume one immutable executable quote and determine whether its
exact state binding remains usable while producing an unsigned account/
instruction plan and offline simulation evidence. T003 must not mutate a T002
quote. Recent blockhash, transaction serialization, simulation, signing,
submission, retry-after-send, and live lifecycle semantics are absent here.
## Project-review C1 hardening

Project review added fail-closed persisted-quote provenance validation.

The repository now verifies persisted executable quotes against exact:
- intent fingerprint and economic amount
- venue
- slot span
- route outcome
- fee-config fingerprint

Adversarial regression coverage added:
- G07B_FORGED_INTENT_FINGERPRINT_REJECTED
- G07C_FORGED_INTENT_AMOUNT_REJECTED
- G07D_FORGED_VENUE_REJECTED
- G07E_FORGED_SLOT_SPAN_REJECTED
- G07F_FORGED_FEE_CONFIG_FINGERPRINT_REJECTED

Final focused validation:
- T002: 100/100 PASS
- T001 regression: 43/43 PASS
