# Phase 5 Shadow Unsigned Plan + Non-Broadcast Simulation v0.1

## Status and boundary

`MEME-P5-T003` is PASS / ACCEPTED / CHECKPOINTED. The contract is:

- model: `P5-SHADOW-UNSIGNED-PLAN-SIMULATION-0001`
- model fingerprint: `a7b619bb3e85a6a2310969437080c125c821ffaeb8ae8abd40de9b08cca84a2a`
- stable plan schema: `phase5_unsigned_transaction_plan_v0.1`

This remains Shadow-only. It can construct public-account instruction plans and
serialize transactions containing only default, all-zero signature
placeholders for `simulateTransaction` with signature verification disabled.
It has no private material, signing object, signing method, generic RPC escape
hatch, transaction-submission method, or live-enable switch.

The semantic boundary is strict:

```text
executable quote != unsigned plan != simulation result != live fill
```

A successful result means only that the unsigned planned transaction was
evaluated successfully by simulation in the recorded chain context. It is not
a transaction signature, confirmation, fill, or guarantee of later execution.

## Official reference provenance

The instruction contracts were verified on 2026-08-28 against the current
official Pump repositories and Solana RPC documentation:

- Pump public docs and IDLs: <https://github.com/pump-fun/pump-public-docs>
- Pump BUY v2 docs: <https://github.com/pump-fun/pump-public-docs/blob/main/docs/instructions/BUY.md>
- PumpSwap docs: <https://github.com/pump-fun/pump-public-docs/blob/main/docs/PUMP_SWAP_README.md>
- Pump/PumpSwap trailing-account upgrade:
  <https://github.com/pump-fun/pump-public-docs/blob/main/docs/BREAKING_FEE_RECIPIENT.md>
- Pump IDL: <https://raw.githubusercontent.com/pump-fun/pump-public-docs/main/idl/pump.json>
- PumpSwap IDL: <https://raw.githubusercontent.com/pump-fun/pump-public-docs/main/idl/pump_amm.json>
- `@pump-fun/pump-sdk` reference version: `1.36.0`
- `@pump-fun/pump-swap-sdk` reference version: `1.19.0`
- Solana `simulateTransaction`: <https://solana.com/docs/rpc/http/simulatetransaction>
- Solana `getLatestBlockhash`: <https://solana.com/docs/rpc/http/getlatestblockhash>
- Solana `getBlockHeight`: <https://solana.com/docs/rpc/http/getblockheight>
- Solana `isBlockhashValid`: <https://solana.com/docs/rpc/http/isblockhashvalid>

Reference IDL SHA-256 values used by the independent fixture review:

- Pump: `b90bc471327f671449271d5d1d42354d1fae6f5a06502f5834459a3108138e49`
- PumpSwap: `6b5c7ec4e5ef9742fa99dc57b0d75b1031b379bba02a7e1b3c5a4cad68d77e56`

No SDK was installed or vendored. Official package tarballs were inspected
read-only outside the repository. Acceptance tests make no network/RPC calls.

## Instruction contracts

Each contract fingerprint binds program ID, instruction name, discriminator,
ordered fixed-account schema with writable/authority-required flags, argument
schema, and—where applicable—the exact upgraded remaining-account contract.

### Pump BUY

- instruction: `buy_exact_quote_in_v2`
- discriminator: `c2ab1c46684d5b2f`
- arguments: `spendable_quote_in: u64`, `min_tokens_out: u64`
- T002 translation: `(quote.input_amount, quote.minimum_output)`
- contract fingerprint: `925990a04e2e2d3c82a260557b7c8f24a98f9804e7de6671b7b417addcb7c1c8`
- ordered accounts: global, base mint, quote mint, base token program, quote
  token program, associated token program, fee recipient, fee-recipient quote
  ATA, buyback recipient, buyback-recipient quote ATA, bonding curve,
  curve base ATA, curve quote ATA, user, user base ATA, user quote ATA,
  creator vault, creator-vault quote ATA, sharing config, global volume
  accumulator, user volume accumulator, user-volume quote ATA, fee config,
  fee program, system program, event authority, program.

### Pump SELL

- instruction: `sell_v2`
- discriminator: `5df6823ce7e940b2`
- arguments: `amount: u64`, `min_sol_output: u64`
- T002 translation: `(quote.input_amount, quote.minimum_output)`
- contract fingerprint: `affe70abf949247b888febaa456bf25822b36d90348b506192806a07dac195c4`
- ordered accounts: the Pump BUY order above without global volume accumulator.

### PumpSwap BUY

- instruction: `buy_exact_quote_in`
- discriminator: `c62e1552b4d9e870`
- arguments: `spendable_quote_in: u64`, `min_base_amount_out: u64`,
  `track_volume: OptionBool`
- T002 translation: `(quote.input_amount, quote.minimum_output, policy flag)`.
  The first field is the immutable full quote-asset input budget. The accepted
  T002 `spendable_quote_input` remains the fee-deducted worksheet component;
  because the official instruction deducts fees from `spendable_quote_in`, it
  is not passed as a second, reduced budget.
- contract fingerprint: `c1ee7da925bb29dcecf8bcfa2985bf7c65083e5b5ca0c7743ec1f944bc26da43`
- 23 ordered base accounts: pool, user, global config, base mint, quote mint,
  user base ATA, user quote ATA, pool base account, pool quote account,
  protocol recipient, protocol-recipient quote ATA, base token program, quote
  token program, system program, associated token program, event authority,
  program, coin-creator vault ATA, coin-creator vault authority, global volume
  accumulator, user volume accumulator, fee config, fee program.
- upgraded remaining order, independent of `coin_creator`: non-cashback uses
  pool-v2, buyback recipient, buyback-recipient quote ATA; cashback uses
  user-volume quote ATA, pool-v2, buyback recipient, buyback-recipient quote
  ATA.

### PumpSwap SELL

- instruction: `sell`
- discriminator: `33e685a4017f83ad`
- arguments: `base_amount_in: u64`, `min_quote_amount_out: u64`
- T002 translation: `(quote.input_amount, quote.minimum_output)`
- contract fingerprint: `33f9086933c5945be1fc739375f71c8598bb47c178089d0a19411fb99c230cf4`
- 21 ordered base accounts: the PumpSwap BUY base order without global and user
  volume accumulators.
- upgraded remaining order, independent of `coin_creator`: non-cashback uses
  pool-v2, buyback recipient, buyback-recipient quote ATA; cashback uses
  user-volume quote ATA, user-volume accumulator PDA, pool-v2, buyback
  recipient, buyback-recipient quote ATA.

For both PumpSwap directions, pool-v2 is always the exact readonly,
non-authority PDA derived with `["pool-v2", base_mint]` under the PumpSwap
program. It appears exactly once, including when the decoded pool
`coin_creator` is the default/system public key. The builder validates the
whole trailing sequence and fails closed on missing, duplicate, reordered, or
mis-flagged pool-v2 metadata. This C2 rule follows the upgraded protocol
contract; `coin_creator` population is not used as a remaining-account
presence predicate. The current official IDL independently exposes dedicated
`InvalidPoolV2`, `MissingCashbackAccounts`, and invalid cashback-accumulator
errors, supporting distinct validation of these trailing account groups.

The T003 builder does not recompute venue economics. Pump/PumpSwap integer
outputs, fees, slippage constraints, and virtual-reserve effects remain bound
to the immutable accepted T002 quote.

## Actor, recipients, accounts, and plan identity

`PublicShadowActorV01` contains one public key and derived schema/fingerprint
fields only. Test actors are deterministic fixture pubkeys; no real user wallet
is requested or embedded.

Current Pump/PumpSwap global account bytes must match the exact T002 account
evidence before recipient sets can be decoded. Normal, reserved/mayhem, and
buyback sets each contain eight unique non-default public keys. Selection is a
deterministic hash-bound index over immutable quote, actor, policy, and purpose
fingerprints. No randomness is used.

Actor ATA existence is established from a coherent read-only account snapshot
at or above the selected venue-state slot. Pubkey, owner program, mint,
authority, and data hash are verified. The snapshot and recipient decoder have
factory guards so unverified callers cannot construct accepted evidence
directly.

`UnsignedTransactionPlanV01` is frozen and factory-built. Its canonical payload
embeds the exact public actor, structural policy, actor-account snapshot, and
recipient evidence, in addition to T001/T002 IDs and fingerprints, token
programs, derived public accounts, instruction order, exact meta flags, and
instruction bytes. Its ID changes for any structural/economic change. It
contains no blockhash, signature, simulation response, or mutable transaction.

Setup/cleanup follows the verified SDK shape. Missing destination ATAs use the
idempotent associated-token instruction where needed. PumpSwap BUY funds and
synchronizes the actor WSOL ATA. A PumpSwap plan closes that WSOL ATA only when
the prerequisite actor-account snapshot proved it absent and the same plan
created it. A pre-existing actor WSOL ATA is never created or closed by the
plan: BUY only transfers/synchronizes its new budget before
`buy_exact_quote_in`, while SELL writes proceeds into it without cleanup.
Changing this snapshot evidence changes the plan identity. SELL requires a
verified base-token account. Legacy SPL Token and Token-2022 base mints are
explicit; WSOL quote accounts use legacy SPL Token.

## Blockhash lease and serialization

`BlockhashLeaseV01` is separate from the stable plan and records blockhash,
response context slot, last valid block height, commitment, observation time,
fingerprint, and immutable lease ID. Acquisition requires:

```text
getLatestBlockhash.minContextSlot >= plan prerequisite slot
```

Before simulation, T003 records `getBlockHeight` and `isBlockhashValid`
evidence using:

```text
required minContextSlot = max(plan prerequisite slot, blockhash context slot)
```

`block_height == last_valid_block_height` remains valid if the RPC validity
response is true. `block_height > last_valid_block_height`, or a false validity
response, is expired. One deterministic refresh is allowed. A refresh appends
a new lease, envelope, attempt, and result while preserving the same plan ID
and all historical expiry evidence. A second expiry is terminal.

For a given plan and lease, `SimulationEnvelopeV01` deterministically records
message bytes, wire bytes, digests, exact request config, required authority
count, and zero-placeholder count. The serialized transaction is parsed again;
its message must equal the recorded message and every 64-byte signature slot
must be all zero. There is no API that can replace those placeholders with a
valid signature.

## Simulation request and results

The fixed v0.1 request is:

```json
{
  "encoding": "base64",
  "sigVerify": false,
  "replaceRecentBlockhash": false,
  "commitment": "confirmed",
  "minContextSlot": "exact causal maximum described above",
  "innerInstructions": true
}
```

The narrow protocol exposes only `get_latest_blockhash`, `get_block_height`,
`is_blockhash_valid`, and `simulate_transaction`. It has no arbitrary method
parameter.

`ShadowSimulationResultV01` binds plan, attempt, lease, envelope, config,
context, canonical error, logs, compute units, return data, inner instructions,
loaded-account data size, returned accounts, reason, observation time, and a
deterministic evidence ID/fingerprint. Outcomes are:

- `SIMULATION_SUCCESS`: returned response, non-regressing context, `err == null`.
- `PROGRAM_REJECTED`: returned response with program/transaction error.
- `BLOCKHASH_EXPIRED`: invalid before simulation; not venue rejection.
- `RPC_FAILURE`: simulation transport/operational exception.
- `MALFORMED_RESPONSE`: returned evidence violates the response contract.
- `CONTEXT_REGRESSION`: returned context is below exact `minContextSlot`.

## Persistence and lifecycle

T003 adds separate versioned append-only tables alongside accepted T001/T002
tables in the isolated `data/shadow` database. It persists canonical plans,
ordered instructions, leases, validity evidence, envelopes with exact request
configs, attempts, and results. New plans require an exact persisted T002 quote
and `QUOTE_BOUND`; new attempts require `PLAN_BUILT`. Plan/lease/config/result
lineage is checked again at persistence. Exact replay is idempotent; identity,
attempt-index, or attempt-result conflicts fail closed. WAL, synchronous FULL,
foreign keys, immutable triggers, canonical audit, digest, reopen audit, and
`quick_check` are exercised offline.

Lifecycle mapping uses the accepted T001 matrix:

- plan persisted: `QUOTE_BOUND -> PLAN_BUILT`
- returned simulation evidence: `PLAN_BUILT -> SIMULATED`
- success: `SIMULATED -> COMPLETED`
- returned rejection/malformed/regression: `SIMULATED -> FAILED`
- refresh-exhausted expiry: `PLAN_BUILT -> EXPIRED`
- operational failure without returned context: `PLAN_BUILT -> FAILED`

Terminal state remains terminal; historical evidence is never rewritten.

## Verification and T004 handoff

The deterministic self-test is
`scripts/phase5_shadow_unsigned_plan_simulation_selftest_v0_1.py`. It uses only
synthetic T001/T002 fixtures, static official-reference vectors, a narrow fake
RPC, and a temporary isolated Shadow database. It does not access production,
paper, collector, observability, wallet, network, or active T022 artifacts.
The final C2 matrix is `116/116 PASS`, including all eight PumpSwap
side/cashback/coin-creator-presence quadrants and explicit rejection of
missing, duplicated, reordered, or mis-flagged pool-v2 layouts.

The exact next handoff is a separately reviewed task for a short live,
read-only Shadow simulation smoke after the Phase-4 acceptance audit. That task
must supply a dedicated public actor address and RPC endpoint, retain zero-only
signature placeholders, and must not add signing or submission capability.
