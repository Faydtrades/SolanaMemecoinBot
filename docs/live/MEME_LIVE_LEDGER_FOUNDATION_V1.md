# MEME-LIVE Step 5 — Ledger Foundation v1

Status: IMPLEMENTED_PENDING_PROJECT_REVIEW. L1–L6 are implemented and locally qualified; production coverage remains partial at the explicit Evidence/attribution limits below, and full system-transition acceptance remains pending real consumers. Project/package acceptance remains with the ChatGPT project. No Authority/Step-6 work is authorized here.

Authoritative starting checkpoint: `5258537c7de98de4883f594375f24c90de57250b`, clean local/index/remote `live/meme-production-readiness` in `C:\Users\Mari1\AppData\Local\Temp\meme-live-audit-e6b9a4b`. Remote master remains `e6b9a4beca06d8a08ecfe900356b80228e7667b3`. The active checkout `D:\Tradingbot\solana_memecoin_bot_phase1_v0_1` is protected and is not an implementation/test target.

The Step-4 project decision supplied by the Step-5 request is recorded with its attachment provenance in [owner/lane status](MEME_LIVE_OWNER_ACCEPTANCE_AND_LANE_STATUS.md#project-decision-supplied-with-the-step-5-request): `PROJECT_ACCEPTED_IMPLEMENTATION`, `LOCAL_PASS`, `SYSTEM_TRANSITION_PASS_PENDING_REAL_CONSUMERS`. Full Evidence package acceptance remains pending real Ledger/Authority/Runtime transitions. The accepted Evidence checkpoint is reused; no Step-4 review or suite rerun is performed merely to record that decision.

## Bounded worker split — recorded before Ledger source edits

CHIEF and consequential Ledger work use Astra Ultra because identity, replay, finality, attribution and transaction atomicity materially affect correctness. One consequential worker runs at a time. A worker receives one bounded task, supplies focused verification and stops for CHIEF review; ordinary corrections stay in that worker context. CHIEF owns documentation and normal authorized LIVE checkpoints. No task self-authorizes package acceptance.

| Task | Smallest coherent implementation boundary | Required consumer-facing output |
|---|---|---|
| L1 — Original Evidence and opening baseline | LIVE domain/config binding, explicit immutable Wallet Evidence codec, dedicated journal and baseline ingestion/adjudication/replay | One exact original finalized clean-wallet baseline receipt or durable unresolved/quarantined reasons; no action, position or fabricated funds |
| L2 — Economic identity and durable progression | Canonical candidate inbox/root uniqueness, position/mint-scoped action/attempt and append-only storage transitions; exclusive/fenced write contract | Stable roots/actions/attempt/message/signature lineage, monotone versions and immutable dispositions for future policy/Execution consumers; no admission or send policy |
| L3 — Finality adjudication | Exact transaction/status and bounded canonical coverage ingestion against original attempt/validity binding | Provisional/finalized-success/finalized-failure/UNKNOWN/proven-non-landing or positively evidenced non-submission; no retry/new-message authority |
| L4 — Actual complete-transaction attribution | Pure validation of authoritative finalized account/transaction movements into native/token/WSOL/fee/rent components | Complete balanced transaction posting proposal, or explicit unapplied/quarantined attribution; no quote/PAPER/Shadow substitution or partial economic application |
| L5 — Atomic application and reconstruction | Exactly-once settlement group, position/funding projections, actual-account comparison and external-adjustment quarantine | Rebuildable remaining inventory and distinct native/WSOL/token/fee/rent facts; settlement plus projections commit together, no risk/headroom decisions |
| L6 — Atomic future-consumer ports and final qualification | Concrete admission/reservation/inbox and acquisition/obligation/retirement/DRY storage boundaries; composed replay/crash review | Atomic storage contracts and deterministic port fixtures with future policies/controllers still unbuilt; final real Evidence-to-Ledger transition evidence |

L1 necessarily introduces the minimal dedicated store used by the later tasks. The later tasks extend concrete facts in that store; they do not create a general event/ledger framework. This split separates pure attribution from economic application so neither one worker nor one unreviewed reducer owns the entire package.

## Contracts and invariants carried across tasks

- A fixed LIVE custody domain binds chain genesis, dedicated public wallet, schema/version and immutable configuration/provenance. Economic identity remains chain/wallet/LIVE plus canonical candidate/root/action identity; changed policy, profile, process, restart or schema metadata cannot create a replacement economic root. A missing expected journal is not an invitation to create a new epoch.
- Dedicated SQLite persistence uses WAL, FULL durability, foreign keys, immutable evidence/state records, exact-content idempotency, unique economic keys and revision/fence checks. The storage boundary must exclude concurrent writers; it must not invent heartbeat takeover or Operations arming authority. Conflicting records are rejected or durably quarantined rather than overwritten.
- Accepted Evidence types/ports are the only public-chain truth input. Persist original sanitized observations and the bound evaluation time/floor/profile used for decisions. Replay verifies the original durable decision; today's clock cannot invalidate an already-applied historical baseline or settlement. New capacity still requires current account reconciliation evidence.
- V1 opening shape is configured known native SOL and no unsupported/unattributed token/WSOL positions. Empty supported non-WSOL accounts may remain explicit account/rent facts; pre-existing WSOL is outside the initial native-recycling shape. No real key or wallet provisioning is required for deterministic implementation.
- Monetary and raw token quantities use exact integers. SQLite storage must not truncate u64 values or lifetime totals through signed-64-bit columns, floats or inverse price arithmetic. Owned native wallet SOL, token-account lamports/rent, WSOL units, base tokens, fees and encumbrances remain distinct.
- Exact primary signature/message and original finalized lower anchor/recent-blockhash/last-valid height remain bound before any possible-send disposition. A null/timeout/incomplete/pruned/contradictory response is never terminal. Complete requested coverage must actually cover the required original validity window; a later root alone cannot fill a missing tail.
- Finalized failed transactions can post supported actual fees but cannot invent inventory. All required attribution must validate before one atomic settlement group is applied. Acquisition/reduction/residual identities are position/mint scoped; a singleton-position schema is forbidden.
- Future Authority/Runtime decisions are explicit transaction inputs, not decisions made by Ledger. Acquisition usability and a durable protective handoff must share the economic commit; later reservation/retirement contracts cannot release UNKNOWN or fabricate a fill for DRY/NON_SUBMITTED. Real policies, exit logic and process orchestration remain outside this package.

## Direct reuse and qualification plan

Reuse pure canonical/fingerprint helpers and accepted append-only/idempotent/CAS repository patterns from `src/phase5/shadow_domain_v0_1.py` and `shadow_repository_v0_1.py`; do not reuse their economic records as LIVE truth. Keep the G001-only `SourceEvidenceStore` separate. Consume `WalletObservation` / `ledger_account_evidence` and exact transaction/coverage Evidence decoders and ports directly; any necessary serialization is an explicit persistence codec for those concrete types, not another RPC layer.

Each task gets focused identity, malformed-evidence, duplicate and relevant crash/reopen tests using temporary databases and deterministic accepted Evidence fixture builders. Actual ingestion/reduction/persistence paths must construct baseline, dispositions, postings and projections; tests cannot inject precomputed economic state to claim the transition into it.

At the final Ledger freeze, run the full package-specific composed verification plus one broad non-research regression covering all accepted Phase-5 standalone selftests and the full LIVE Evidence/Ledger selftest surface. Confirm the existing Phase-5 tests remain isolated/mocked before invoking them. Canonical Git-byte exports may be used for existing raw-hash-bound Windows compatibility tests, preserving and recording exact source identity; do not change locks or accepted implementations to satisfy tests. No Phase-6 research, real-chain call, long soak, collector/live smoke or active runtime DB is involved.

Matrix review targets M13–M18 and M32/M33, plus factual Ledger portions of M12/M36/M42/M43/M48/M55. Rows are promoted only for their real proven transitions. Future Authority/Runtime/Execution/Operations consumers cannot be represented by fake production implementations to obtain acceptance.

## L1 implementation evidence

L1 is `IMPLEMENTED_PENDING_PROJECT_REVIEW`. CHIEF inspected the domain, explicit original-Evidence codec, repository and focused selftest, then independently reproduced the worker's result. The checkpoint containing this section records L1 plus the supplied Step-4 decision; later Ledger capabilities remain in progress.

`ledger_domain_v0_1.py` binds the economic identity to genesis/wallet/LIVE while pinning schema, RPC profile, known native funding and supported empty-account configuration separately. `ledger_evidence_codec_v0_1.py` preserves only the accepted immutable WalletObservation, its Evidence model/version and content digest through explicit canonical serialization. `ledger_repository_v0_1.py` stores the original observation and re-derived baseline receipt atomically in a dedicated WAL/FULL/FK journal with immutable history, content uniqueness, generation/revision fencing and an OS-exclusive writer guard. Initialize never resets/adopts an existing store; reopen never creates a missing journal. These are Ledger storage guarantees, not Operations arming or a heartbeat takeover mechanism.

An established baseline requires coherent, complete, supported finalized account facts matching known funding and configured identities. Missing/insufficient facts remain `UNRESOLVED`; unsupported holdings or contradictory bindings remain `QUARANTINED`, with reasons and original evidence retained. Explicit absence differs from unknown and present zero. Nonzero/unattributed token holdings and all pre-existing WSOL accounts are rejected by the opening profile. Known empty non-WSOL accounts retain observed locked lamports separately; no minimum-rent or spending-authority claim is made. Historical receipt replay uses its original evaluation time and floor; a newly stale observation cannot replace established funds.

From the isolated checkout, both worker and CHIEF ran:

```powershell
& 'C:\Users\Mari1\AppData\Local\Temp\meme-live-evidence-venv\Scripts\python.exe' -B scripts/live_ledger_baseline_selftest_v0_1.py
```

Exit `0`, **126 checks**, `failed=[]`. This includes real MockTransport-backed Wallet Evidence adapter -> original Ledger ingestion -> durable baseline -> reopen, exact u64 native values and aggregate locked lamports beyond u64, preserved denials, conflicting identity/content, stale/new observations, damaged/wrong stores and exclusion of a competing OS process. All fixture databases and subprocess files were cleaned. No real RPC call occurred.

Abrupt subprocess exits immediately before/after the economic commit returned `71`/`72`; reopen recovered zero/one observation-receipt pairs respectively. Both retries converged to exactly one baseline decision digest `57b6bf3a764d1a3b0deaf66fa76753f11e6cb46d8089cce51c0fc5e67ae88aee`, from original Evidence digest `12758ff1e38c7994d0a07da0b076af182d786a80c32b5363b91a6c49b826e695`. An additional CHIEF smoke reproduced identical receipts across three writer generations. `git diff --check` exited `0`.

This supplies the actual Evidence -> Ledger opening-baseline boundary relevant to M13/M14. Project review and the remaining finality, settlement, funding and future policy/runtime transitions remain pending; no matrix row or package is self-accepted here. No shared Evidence/P4/P5 source was changed, so broader regression is deferred to the final Ledger freeze as planned.

NEXT: L2 — economic identity and durable action/attempt progression. Signing/send/broadcast/mutating RPC and real-capital authority remain OFF.

## L2 implementation evidence

L1 was checkpointed at `12cebb838652b66550ae73b396381b6b2f448ad8`. L2 is `IMPLEMENTED_PENDING_PROJECT_REVIEW`; CHIEF reviewed its concrete action contracts, journal integration and tests and independently reproduced the final focused results before the authorized checkpoint.

`ledger_actions_v0_1.py` preserves original candidate/run/signal/source/winner and exact reference-price provenance, reusing the accepted canonical run/signal identity formula without executing strategy logic. BUY roots cannot change with amount, policy, restart or schema metadata. SELL terms remain position/mint/obligation scoped. Immutable pending action terms and external simulation/authorization claims explicitly confer no real admission or send authority. The inbox accepts original candidate provenance without requiring a future Authority deadline; a claimed ENTRY deadline belongs to pending action terms.

Preparation durably binds the exact public message, plan/policy digests, original blockhash lease and finalized lower anchor before any possible exposure. Canonical signed-wire storage retains public bytes/signature lineage only; it contains no signing operation. A wallet mutation lane prevents overlapping unresolved attempts across roots. Transport repeats retain the same message/signature. Timeout, restart, signed cancellation and UNKNOWN cannot clear it. Positive local unsigned cancellation is limited to an intact writer generation and stages with no signed bytes; caller-supplied finality/non-landing assertions are rejected. Finality adjudication belongs to L3.

Storage/schema v2 adds six concrete journal fact kinds with immutable history, exact-content uniqueness, common revision/digest CAS and reconstruction of signature/lane state. Economic domain identity is unchanged. Pre-L2 fixture stores require their original explicit binding and are not silently migrated. The L1 digest above describes its schema-v1 checkpoint; the schema-v2 baseline replay digest is `4c43f8ec46a782cebd39bd24be11cda1f139251053f4d35f07fccf6faf30b6a9`. Every public mutation checks trusted state after `BEGIN IMMEDIATE`; trusted head/data/schema versions are captured before commit. Outside commits, including structurally valid same-value metadata commits, require explicit reopen and verification. Ordinary economic appends verify their relevant facts without re-decoding the entire original Wallet Evidence history. No-commit insertion helpers preserve the later atomic admission/reservation/inbox seam without implementing those policies.

Worker and CHIEF each ran from the isolated checkout:

```powershell
& 'C:\Users\Mari1\AppData\Local\Temp\meme-live-evidence-venv\Scripts\python.exe' -B scripts/live_ledger_actions_selftest_v0_1.py
& 'C:\Users\Mari1\AppData\Local\Temp\meme-live-evidence-venv\Scripts\python.exe' -B scripts/live_ledger_baseline_selftest_v0_1.py
```

Both commands exited `0`: **98 action checks**, **126 baseline checks**, `failed=[]`. Tests cover canonical duplicates/conflicts, two mints/position identities, original message/lease/anchor binding, held UNKNOWN, unsigned cancellation limits, terminal inbox tombstones, fencing, interleaved original Wallet Evidence, and external-write races around begin/commit. Action and attempt subprocess cuts exit `81` before commit and `82` after commit; reopen recovers zero/one facts respectively and exact retry converges to one. Replayed UNKNOWN commit digest: `e156c7e5fa38024eaf3c65b05a0bba5c2fbfb6329d16a71a015ba0d77e1d2752`. All database/network inputs are temporary deterministic fixtures; signing operations and real network requests are zero. `git diff --check` exits `0`.

No Evidence/P4/P5/P6 source was changed. Actual upstream Runtime/Authority/Execution consumers are still unbuilt; no lifecycle row or package is self-accepted. Broader regression remains scheduled for final Ledger freeze.

NEXT: L3 — original transaction/coverage Evidence and authoritative finality/non-landing reconciliation, without settlement or retry policy.

## L3 implementation evidence

L2 was checkpointed at `1933457be4cbe9fb7a14e8ae6c6098eba4639c49`. L3 is `IMPLEMENTED_PENDING_PROJECT_REVIEW`. The finite `ledger_chain_codec_v0_1.py` preserves original accepted transaction/coverage observations, model/version, exact public bytes and digest. Its explicit 160 MiB bound accommodates the accepted RPC profile's 128 MiB maximum response budget; oversize input fails without truncating evidence or changing held state. Storage/schema v3 adds original chain observations and derived receipts as one concrete common-journal transaction. Economic identity is unchanged; earlier fixture schemas are not silently migrated.

`ledger_finality_v0_1.py` reuses the accepted transaction/coverage ports with the stored attempt's genesis/profile/signature/full wire, original lease, finalized lower anchor and baseline cut. Actual confirmed status is provisional. Null, timeout, pruned/incomplete metadata and finalized status without a qualified exact transaction remain `UNKNOWN`, retaining positive finalized claims separately. Complete supported exact transactions establish `FINALIZED_SUCCESS_UNAPPLIED` or `FINALIZED_FAILURE_UNAPPLIED`. Later missing/stale observations do not erase the original positive proof. Contradictory finalized status/outcome/metadata, retained anchors or canonical signature vectors quarantine. Optional block-time/inner-stack facts may enrich from unknown to known; conflicting known values fail closed. A later compatible supported transaction reference is available for settlement while the first proof remains immutable.

`PROVEN_NON_LANDED` requires the original recent-blockhash profile, full canonical coverage through its last-valid height, a fresh finalized root strictly beyond expiry, and no exact-signature occurrence in present or retained finalized claims. A complete shorter requested interval, missing/pruned block, budget exhaustion or positive occurrence without an exact outcome cannot prove nonlanding. The installed read-only `VersionedTransaction.uses_durable_nonce()` check prevents nonce-shaped messages receiving recent-hash expiration proof. A finalized recent-hash transaction beyond its original last-valid height is contradictory. These results do not release the economic lane, apply balances or grant replacement-message authority.

Original observations and receipts reconstruct at their original evaluation time. Derived chain facts publish before the committed head is trusted in memory, so lost acknowledgements cannot pair a new trusted head with old finality. Full reopen audits receipt derivation, generation, common-chain linkage and original evidence. Ordinary writes retain the L2 outside-write guard.

Worker and CHIEF independently ran the following from the isolated checkout with the pinned interpreter, all exit `0`, `failed=[]`:

| Command suffix after `python.exe -B` | Checks |
|---|---:|
| `scripts/live_ledger_finality_selftest_v0_1.py` | 113 |
| `scripts/live_ledger_actions_selftest_v0_1.py` | 98 |
| `scripts/live_ledger_baseline_selftest_v0_1.py` | 126 |

The new suite uses actual MockTransport Transaction/Coverage adapters -> original Ledger ingestion -> durable decisions -> reopen. It covers legacy/v0 bytes, finalized/provisional/unknown/nonlanding distinctions, conflicting historical claims, nullable enrichment, original validity bounds, nonce rejection, competing processes, malformed codecs, tampered receipts and lost acknowledgement. Subprocess cuts exit `91` before commit and `92` after commit, recovering zero/one receipt respectively; exact retry converges to one. Positive decision digest: `47d0cd779840de093ebeb7d211be97e9e6ff64444a09fe314a3b9f4a4afb72a0`; original transaction Evidence digest: `876f32fe59c73453f25bea7d2be0816e535690be49812404f4eb9542e9eb31aa`; nonlanding decision digest: `ca606c5cdb818a8df6fd1ac009a3fab4de33bb9df083601b45e6d7d9d5b9ecc6`. Current schema-v3 baseline replay digest is `9a6ae1a862df585cd0a437e7f0f49adbcb1fd6279d35da1633dec7b51d8024d8`; earlier sections retain their historical checkpoint digests.

This establishes the available real Evidence -> Ledger finality/coverage boundary for M13/M15/M33. The actual Execution producer of the original lease/preparation and later economic/Runtime consumers remain unbuilt; deterministic external producer claims confer no authority. No lifecycle row or package is self-accepted. No Evidence/P4/P5/P6 source, signing, RPC mutation or real-capital capability changed. `git diff --check` exited `0`; final broad regression remains deferred to the complete Ledger freeze.

### Account-shape compatibility question discovered while preparing L4

A concrete existing M13 compatibility limitation was reproduced, without source changes: the accepted planner's Token-2022 associated-account creation path produces an `ImmutableOwner` extension, while accepted Wallet Evidence currently supports only exact 165-byte token-account layouts. The [official extension guide](https://www.solana-program.com/docs/token-2022/extensions#immutable-owner) and [associated-account processor](https://github.com/solana-program/associated-token-account/blob/main/program/src/processor.rs) confirm that standard creation adds this extension. The actual Wallet Evidence adapter -> Ledger port probe returned COMPLETE/COHERENT/SUPPORTED for a plain account and COMPLETE/COHERENT/UNSUPPORTED (`UNSUPPORTED_TOKEN_EXTENSIONS_OR_LENGTH`) for the canonical 170-byte ImmutableOwner-only shape. Both probes used deterministic public fixtures, with zero network calls or source changes.

Step-5 section 3 requires owner input before implementation in another package. A scope question is pending: authorize a bounded Evidence correction for this one extension, or retain and report the limitation. Evidence remains unchanged pending an explicit answer; unaffected Ledger work continues. This is an existing account-evidence compatibility requirement, not a new lifecycle subsystem or permission to support arbitrary extensions.

NEXT: L4 — whole actual settlement attribution, producing a complete posting proposal or an unapplied/quarantined result; no economic application yet.

## L4 implementation evidence

L3 was checkpointed at `dca989a920a9d64df25a229031b3bfdafbd00007`. L4 is `IMPLEMENTED_PENDING_PROJECT_REVIEW`. The pure `ledger_settlement_v0_1.py` consumes original stored action/attempt/finality evidence and an immutable original Wallet support input. It re-derives the accepted Evidence ports at their recorded evaluation times, binds exact public wire/message/signature/account identities and reconciles complete native and raw-token movements. It returns one complete immutable proposal or no proposal (`UNAPPLIED` / `QUARANTINED`). It does not apply custody, release the wallet lane or grant inventory usability; those require the next atomic storage boundary.

Supported profiles cover Pump BUY/SELL, PumpSwap native recycling BUY/SELL, retained WSOL proceeds, known first-use account setup and finalized fee-only failure. Network fees come from exact metadata, without inventing a priority/base split. Native SOL, base units, WSOL, observed wallet-account funding/refunds/locked lamports, externally owned fee-account funding and program-owned setup outflow remain distinct. Wallet support supplies account ownership and supported shape, never substitute fill quantities. Missing pre/post token facts require a positively proved create/close lifecycle; quote output and inverse arithmetic never supply actual units. Unknown flows prevent the whole proposal, including its otherwise known fee component.

The finite instruction profile reuses accepted Phase-5 account layouts, PDA identities and read-only decoders. The pinned [Pump IDL](https://github.com/pump-fun/pump-public-docs/blob/9c82f61cb711b044a17f770ab8ce9f9bdf78f333/idl/pump.json) and [PumpSwap IDL](https://github.com/pump-fun/pump-public-docs/blob/9c82f61cb711b044a17f770ab8ce9f9bdf78f333/idl/pump_amm.json) have exactly the already accepted Phase-5 hashes. Known Anchor event envelopes, the exact read-only get_fees shape and ComputeBudget limit/price encodings are bounded explicitly. A narrow Pump TradeEvent decoder cross-checks actual base units and exact protocol/creator/buyback fee classification; opaque event quote fields do not determine economic amounts.

Native Pump BUY requires a same-transaction fresh canonical 165-byte ATA funding witness and original curve/creator balances at least that observed funding. The [documented target account sizes](https://github.com/pump-fun/pump-public-docs/blob/9c82f61cb711b044a17f770ab8ce9f9bdf78f333/docs/instructions/BUY.md), canonical ATA creation and [monotone rent minimum by data length](https://github.com/solana-labs/solana/blob/v1.18.26/sdk/program/src/rent.rs) establish a relative upper bound excluding the documented smaller-account top-ups. This is not a numeric rent estimate or a claim that observed funding equals measured minimum rent. Without that witness, BUY remains wholly unapplied. SELL requires no wallet-to-venue/fee System debit and reconciles positive proceeds, exact event fees and the negative curve movement. Input-cap slack alone never proves fee/principal classification.

Unsupported Token-2022 extensions remain denied by unchanged Evidence. Cashback/shareholder distributions, prefunded allocate/assign, unsupported setup/resize, unknown instruction shapes and missing native top-up exclusion witnesses remain unapplied. These are explicit limits of the supported attribution proof, not fabricated economic outcomes. The separately pending Evidence scope question above remains unanswered.

Worker and CHIEF independently ran from the isolated checkout, both commands exit `0`:

```powershell
& 'C:\Users\Mari1\AppData\Local\Temp\meme-live-evidence-venv\Scripts\python.exe' -B scripts/live_ledger_settlement_selftest_v0_1.py
& 'C:\Users\Mari1\AppData\Local\Temp\meme-live-evidence-venv\Scripts\python.exe' -B scripts/live_ledger_finality_selftest_v0_1.py
```

Results: **256 settlement checks**, **113 finality checks**, all true. Actual MockTransport adapters feed original durable Ledger finality and then attribution; no precomputed Ledger finality is injected. Tests include exact u64 values, changed actual output, first-use nested fee ATA/volume setup, missing coverage, unknown flows, recipient top-up inside input slack, absent/insufficient rent-bound witnesses, metadata contradictions, fee-only failure, retained WSOL, nullable enrichment and identical proposals after reopen. Pump BUY proposal digest: `ebedb4f2e4b12935b37442b9c863eb11b2b6190eb9eba9a8d99f6e6d86b3588c`; PumpSwap BUY: `0ae9a7b4942bdfc3e4decf7da7a40e483701971bdf9267d161ce28484b907a19`; retained WSOL SELL: `35e0f29b90f480d7ec23181000f7ff7bde7dc91fddbdcb99822a9a1bd1103f7c`.

Only the new Ledger reducer/selftest and this evidence document changed. No Evidence/P4/P5/P6 implementation, signer/send, mutation RPC, real-chain request or runtime action was added. The M16 attribution boundary has concrete proof; actual settlement application and position/funding output still belong to L5. No lifecycle row or package is self-accepted. Broader regression remains scheduled for the final Ledger freeze.

NEXT: L5 — atomic whole settlement application, real position/funding reconstruction and original wallet comparison/quarantine.

## L5 implementation evidence

L4 was checkpointed at `c3ff5366a8c517da961ffcb42174d46b7f2f141f`. L5 is `IMPLEMENTED_PENDING_PROJECT_REVIEW`. Storage/schema v4 adds original Wallet support inputs, immutable application/comparison receipts and balanced postings, with rebuildable native/account/position/resolution projections. The repository calls the actual L4 reducer with original stored action/attempt/chain evidence and original support; callers cannot inject a settlement proposal or precomputed custody state.

A complete successful or fee-only failed transaction applies all supported effects, its actual position/funding projection and its own lane disposition in one SQL transaction. Repeated receipt keys converge; another key cannot settle the same attempt/signature again. Original positive nonlanding can be explicitly resolved without economic postings or retry authority. UNKNOWN, incomplete attribution, divergent prior balances, unsupported account identity or position-specific oversell cannot partially post fees or release custody. A failed attempted oversell may still post complete evidenced fee-only effects. Positive acquisitions remain `OWNED_PROTECTION_PENDING` and unusable until L6 supplies the durable external protective handoff. A full reduction is `FLAT_PENDING_RECONCILIATION`, not capacity retirement.

Native SOL, retained WSOL units, WSOL native reserve, observed account locked lamports, venue/network fees and external setup outflow remain separate exact integers. Accounts may aggregate several independently identified positions; a SELL cannot consume another position's units. Explicit create/close/absence proof remains necessary, including a durable absent WSOL tombstone after transient wrapping. Original wallet observations are compared to one named Ledger cut. Positive/negative/unattributed differences are retained and quarantined, never imported as capital or fabricated external-transfer/PnL classifications. A pending attempt leaves differences `PENDING_EFFECTS_UNKNOWN`; incomplete, stale or unsupported account coverage never overwrites owned balances.

Qualified finalized block identity survives independently of incomplete account coverage or pending balance effects. Original baseline, comparison and application-support anchor claims are cross-checked with retained L3 chain claims in both arrival orders and with new attempt preparations. Wrong domain/profile claims cannot seed this history. A later contradiction quarantines current custody while preserving immutable applied receipts and another attempt's held lane. This is bounded reconciliation of retained original Evidence, without a new RPC/indexing layer.

Consumer views expose current common revision/digest, funding, accounts, position facts and current unresolved attempt, explicitly without current Authority permission. Historical receipts and positions remain addressable. Reads reject unverified outside commits; lost acknowledgement cannot pair a trusted new head with old in-memory projections. Replay re-derives original decisions, postings and projection at their recorded evaluation time, so receipt age does not invalidate applied effects.

Worker and CHIEF independently ran the following from the isolated checkout with `C:\Users\Mari1\AppData\Local\Temp\meme-live-evidence-venv\Scripts\python.exe -B`. Each command exited `0`, with all **872 checks** true:

| Script | Checks |
|---|---:|
| `scripts/live_ledger_custody_selftest_v0_1.py` | 279 |
| `scripts/live_ledger_settlement_selftest_v0_1.py` | 256 |
| `scripts/live_ledger_finality_selftest_v0_1.py` | 113 |
| `scripts/live_ledger_actions_selftest_v0_1.py` | 98 |
| `scripts/live_ledger_baseline_selftest_v0_1.py` | 126 |

The custody suite covers actual Pump/PumpSwap acquisition and partial/full reductions, native recycling/retained WSOL, two mints and multiple roots sharing an account, exact u64 movement, failed fee-only application, nullable evidence enrichment, duplicate application, unknown/nonlanding distinction, wallet mismatch/pending/coverage states, historical and late contradictory claims, and preparation/replay parity. Tampered original inputs/postings/projections, stale fences, competing processes, outside commits and lost commit acknowledgements fail closed.

Application and comparison subprocess cuts exit `91` before commit and `92` after commit, recovering zero/all effects and converging after retry. Application receipt digest: `5b4e54ec1deeb59135a11c851f4ab1b421808a38841bd4ad39b0e18314053076`; custody digest: `0b7084c1ae18996dc8359e561f7c0b8bb461128d3e04b2dc49925b1666fa7acb`. Comparison receipt digest: `732b23fd5ecd25999cb5465d18ba18891ecbc77d36455ea8be14d3d214d34b8e`; custody digest: `bd764187ffcb66a42bb89a3cff2b0850024d6e0bc6e700e58e1cc445a5a10228`. All test databases are temporary; real RPC and signing operations are zero. `git diff --check` exits `0`.

The actual composed path is MockTransport Wallet/Transaction/Coverage adapters -> accepted Evidence ports -> original Ledger baseline/action/attempt/finality -> whole attribution -> atomic application -> actual scoped position/funding -> original wallet comparison -> durable reopen. No Ledger economic oracle is injected. M16–M18/M32/M33 now have additional real Ledger evidence; actual Authority/Runtime/Execution/Operations consumers and project review remain pending. No lifecycle row or package is self-accepted. No Evidence/P4/P5/P6 source or signer/send/mutation/real-capital capability changed. The separate Token-2022 Evidence scope question remains pending. Broader regression remains scheduled for the final Ledger freeze.

NEXT: L6 — concrete atomic admission/reservation/inbox, protective handoff, retirement and DRY/NON_SUBMITTED storage ports; no policy or runtime orchestration.

## L6 implementation evidence

L5 was checkpointed at `1eba4e95ac3557cc23196c21a9677f7ff9e670c8`. L6 is `IMPLEMENTED_PENDING_PROJECT_REVIEW`. Storage/schema v5 adds one immutable concrete consumer-group table, with a closed set of admission, protection, retirement, DRY termination and settlement-plus-port receipts. An admission may contain its same-sequence original ACTION child; a settlement group contains its exact application child. Replay verifies the complete named child set and one common commit, re-derives each transition against its chronological parent cut and checks the resulting projection. No generic unit-of-work, callback transaction framework or policy subsystem was added.

`ledger_ports_v0_1.py` accepts external immutable admission terms, explicit native reservation components and original decision provenance. `LedgerRepository.admit` persists these with the candidate's ACCEPTED disposition atomically. It does not calculate risk, headroom, allocation, maximum concurrent positions or current Authority permission. Admitted BUY terms cannot change; terminal roots cannot acquire new actions/attempts. Later SELL terms must retain the actual position, candidate, track/policy and original protective binding.

A protective handoff contains the actual acquisition signature/proposal and original chain receipt, full original candidate, selected track/policy, fallback deadline and deadline-plus-one-microsecond fire boundary, external MONITORING or DUE decision, trigger, knowledge time and immutable decision provenance. Ledger binds and stores the supplied Runtime decision; it does not evaluate exits or schedule protection. Acquired inventory remains unusable until that handoff is durable. The grouped path commits complete actual settlement, position/funding and handoff together; the standalone handoff makes previously protection-pending inventory usable in the same commit as its binding. Residual reductions preserve the original obligation. Current quarantine masks usability without rewriting historical receipts.

Retirement accepts an external intent and original Wallet support, with structural root/position/attempt/common-cut validation. Positive full reduction, fully applied failed costs, proven nonlanding or intact-generation unsigned cancellation may support retirement only with matched current account evidence and no residual/pending/unapplied obligation. Missing, contradictory or older account evidence yields a durable `WITHHELD` receipt retaining the original input and reservation. Complete economic settlement may commit atomically with withheld retirement; incomplete economic attribution still posts nothing and retains its lane. Retained WSOL and account funding survive lawful position retirement as distinct account facts. A historical retirement receipt never grants current capacity.

Wallet comparison exposes its parent journal revision/digest separately from its target custody digest and effects-through sequence, including the post-settlement child inside a shared group. Its recognized context floor includes usable account cuts and qualified finalized Wallet anchors retained even when inventory coverage failed. An older complete response cannot erase that newer context. Consumer snapshots expose the current required wallet context; original comparisons remain historical evidence requiring fresh external validation.

The fixed DRY domain supports NON_SUBMITTED terminal storage with atomic tentative-reservation release. An interruption before preparation records actual absence; later cases retain exact original preparation/simulation and any positively cancelled unsigned predecessors. DRY cannot enter signed/send stages, chain-finality adjudication, economic settlement or actual inventory. LIVE uncertainty cannot be reclassified through this DRY contract. No DRY runtime/controller was implemented.

Worker focused qualification ran all six Ledger scripts with the pinned interpreter and `-B`, each exit `0`: **372 ports + 279 custody + 256 settlement + 113 finality + 98 actions + 126 baseline = 1,244 checks**. CHIEF reviewed the concrete contracts, transaction/replay paths, current-read guards, final fixture changes and all nine frozen file hashes, then independently reproduced those results within the final regression below.

The ports suite uses actual deterministic Wallet/Transaction/Coverage adapters through original Ledger baseline/action/attempt/finality, complete settlement and the real atomic port methods. External decisions are explicit consumer fixtures, never injected economic state. Tests cover native Pump/PumpSwap BUY -> partial/full SELL, MONITORING/DUE acquisition handoff, residual protection, complete fee-only settlement with withheld retirement, stale/incomplete/contradictory or regressing wallet cuts, positive nonlanding/unsigned cancellation, held lost-generation/signed uncertainty, retained WSOL, fixed DRY modes, and late contradictions after retirement. Historical receipts and applied money remain intact while current custody quarantines and conveys no capacity grant.

Actual subprocess cuts after ACTION/application/group/common inserts and before/after COMMIT recover zero/all linked facts and converge after exact retry. Lost acknowledgements before/after cache publication, competing writers, stale fences, outside commits and restored-trigger content tampering are covered. L6 check digest: `5b787a1687ae5ce46680e4cd73be683048fdcfd7fef5eb160b29a5a7dfa4bdbd`. Original receipt digests across crash cuts:

| Group | Receipt SHA256 |
|---|---|
| admission | `e606cc200d4d119ddf459e81fa842911c4710e96974c72a96e6d9d761e6a1c15` |
| protection | `45bc86339fb0f7f59e8e517a0b42c5e987368f611a68145e5acae54fd8e396f9` |
| retirement | `123f6fd2181b34778184420dcb304a1e3fe922b1dac489d8d74c18b26027db42` |
| withheld | `9b50aa816dc30ff3fea331cc5330aed6c6ef8c1cad253395eb0af92fb21c5778` |
| dry | `5751b26eaaa53298538b55683a4b6366078b28e47420bc9ab8b3f7eb2db29d7f` |

No Evidence/P4/P5/P6 source changed. Schema v5/SQLite user-version 5 is explicit; older fixture stores require their original version and are not silently migrated. All six bounded Ledger tasks are implemented pending project review. Real Authority policy/reservation calculations, Runtime evaluation/later obligation transitions/DRY orchestration, Execution producers and Operations startup remain unbuilt.

## Final Ledger freeze — one broad regression

CHIEF ran the authorized final broad regression once after L6 source freeze on 2026-09-10 UTC. All **20 standalone suites exited 0**, with **2,277 checks**: 668 Phase-5, 365 Evidence and 1,244 Ledger. Each script's own final result/check map was verified, including legacy JSON-plus-`RESULT: PASS` output. No implementation change or test rerun was required by this broad regression.

Exact command for each table entry:

```powershell
& 'C:\Users\Mari1\AppData\Local\Temp\meme-live-evidence-venv\Scripts\python.exe' -B scripts/<script-below>
```

LIVE commands ran in `C:\Users\Mari1\AppData\Local\Temp\meme-live-audit-e6b9a4b`. Phase-5 commands ran in `C:\Users\Mari1\AppData\Local\Temp\meme-live-canonical-tests-t11edhd2`, an isolated canonical Git-byte export for accepted raw-hash checks on Windows. Before the run, all **356 non-LIVE Python files** in that export matched current HEAD Git blobs exactly; ordered relative-path/SHA256 manifest digest `9f1f5fa9ca5fe1ec7baeb88dc57a4d4ac31c4ef9f2e09fd5867472c5360dfa38`. Phase-4/5 source, compatibility scripts and hash locks were unchanged. Suites were sequential within each checkout; the two isolated groups ran concurrently.

Logs and consolidated verification are in `C:\Users\Mari1\AppData\Local\Temp\meme-live-ledger-final-regression-20260910-0213`. `verified-regression.json` SHA256: `cc71b47ee6c06c07b0a96a6b20a1bf2cdea44d9f568936af1275dd3faeb60539`. The following per-command output hashes bind the reported checks to their captured logs.

| Script in scripts/ | Checks | Exit | Output SHA256 |
|---|---:|---:|---|
| `phase5_shadow_venue_route_quote_selftest_v0_1.py` | 100 | 0 | `27b920525740114b4050f9e8e6bea7cbd22956e37ee8b5cb2adc7dec9d6c3295` |
| `phase5_shadow_unsigned_plan_simulation_selftest_v0_1.py` | 116 | 0 | `c82718ad1cac7ac49abb91a73bee487698e6b70e5ef0565ba9d4c75a07603e08` |
| `phase5_shadow_readonly_rpc_selftest_v0_1.py` | 59 | 0 | `ab59f30ded1c8d337cfc975acb995286f89c107248fd080c7cd1f5afc7c625ee` |
| `phase5_shadow_lifecycle_bridge_selftest_v0_1.py` | 40 | 0 | `dc68dde5072b5c60177caedd5bf81ccfec5ad0ca8d4705b93fe024b296c02d7d` |
| `phase5_shadow_instruction_persistence_selftest_v0_2.py` | 36 | 0 | `434a1ae0b204f3d43fd37ea9791188eaf1af4e266d11cf64be72e7c73f69bb32` |
| `phase5_shadow_domain_capability_firewall_selftest_v0_1.py` | 43 | 0 | `856fa406a1ffc81732731ea427c4febef55f879f83f3359b97703acb6917a6b3` |
| `phase5_shadow_continuous_source_bridge_selftest_v0_1.py` | 32 | 0 | `0c94ce5c51d93c1d1aabdbc6f1bbc20f933b3a069922c1157f0865faf5e39552` |
| `phase5_shadow_continuous_execution_selftest_v0_1.py` | 182 | 0 | `cce9c66ccfb08aa859f6e3bb905a420a232830c65faed1576c522ede36ca80a0` |
| `phase5_multirun_source_scope_selftest_v0_1.py` | 44 | 0 | `f70d8f5f4ae959ee0131660877f54b219d9a1532d5af866116e8c8a4352caa3f` |
| `phase5_late_arriving_exit_source_selftest_v0_1.py` | 16 | 0 | `593fabc7526ee2708dcf4877d24bc85faeaca577a5bd83b1478d585866745973` |
| `live_source_health_selftest_v0_1.py` | 67 | 0 | `da3c6014ac9181ef516a2305082a67cbeff3f6b975b9446a2a4d88c309bb8238` |
| `live_wallet_evidence_selftest_v0_1.py` | 88 | 0 | `f115c110ee6c86bb71414c0a678869955fc3802b000cd1d947432863e834a135` |
| `live_transaction_evidence_selftest_v0_1.py` | 170 | 0 | `aa0d5fa27a20da4cc32b4a7166833508a4c33c7285595cf2fd4f170480582509` |
| `live_evidence_boundary_selftest_v0_1.py` | 40 | 0 | `8466afed691f27c144a9aadedcec1b1a168aa3d7ef31af9c55aebd6c56bb3029` |
| `live_ledger_baseline_selftest_v0_1.py` | 126 | 0 | `042a0e28e6a4b74031276da542cc618f6ed55f7ffd7de0426ad52c84c3533166` |
| `live_ledger_actions_selftest_v0_1.py` | 98 | 0 | `b6d2c8761a92ae9167c4f700e7b025f14540fef0f64d596f1df7781ecc86929d` |
| `live_ledger_finality_selftest_v0_1.py` | 113 | 0 | `fa24a9b55301c004e1dee687c741184c594ed8e0e81e03e8789fae606673f2bf` |
| `live_ledger_settlement_selftest_v0_1.py` | 256 | 0 | `45d36ca9d8f538f275b9571be346c1e046629d77921798fe5ee81b6b335e36bc` |
| `live_ledger_custody_selftest_v0_1.py` | 279 | 0 | `1d6e9e2dcd75d2a6614b49fdd8917e9105cb5001e6cbe0ce609ec1a947a86f7e` |
| `live_ledger_ports_selftest_v0_1.py` | 372 | 0 | `55012eeaca94e4a17204c53048fa9bfeaedf14e4e35d8258baeb8f95432d5b22` |

The available system transition is actual MockTransport Wallet/Transaction/Coverage adapters -> immutable accepted Evidence -> actual Ledger original ingestion/reducers/store -> durable baseline/finality/whole settlement -> scoped position/funding and consumer-port outputs -> durable-only reconstruction. Complete versus incomplete account coverage, qualified finalized evidence versus unknown/contradictory claims, applied versus unapplied economics, and retired versus withheld reservations remain distinguishable. G001 regression also preserves healthy versus unresolved continuity at its existing Evidence ports/fixtures; its real Authority/Runtime consumers remain unbuilt. These are composed implementation proofs for project review, without real network or fabricated economic projections.

## Matrix disposition, scope limits and next task

No row was promoted to VERIFIED by Codex. M13–M18/M32/M33 now link actual Ledger consumer evidence and may advance within their proven scope after project review. In particular, M14's narrow opening-baseline consumer is built; M13's Evidence-to-Ledger portion is built, while Runtime chain-order use and the existing Token-2022 account compatibility issue remain open. M15/M16/M32/M33 still need real Execution producer composition; M17/M18 still need Runtime/Authority consumer composition.

M12/M36/M42/M43/M48/M55 remain `OWNED_NOT_BUILT`, with the exact remaining consumers named in the matrix: Authority admission/reservation and capacity; Runtime protective evaluation/later obligation transitions, residual handling, reconstruction and DRY continuation; Execution SELL preparation; Operations startup. Ledger storage ports and fixture proof do not substitute for those implementations. The matrix remains 61 rows: 6 VERIFIED, 49 OWNED_NOT_BUILT, 6 HUMAN_EXTERNAL, 0 BLOCKED.

The outstanding Token-2022 ImmutableOwner Evidence question remains unanswered. Evidence has not been changed under Ledger authorization. Canonical extended accounts remain explicitly unsupported; unknown/pruned/incomplete chain evidence remains UNKNOWN, incomplete attribution remains wholly unapplied, and unresolved retirement remains WITHHELD. The finite Pump/PumpSwap settlement profiles and native BUY witness limits documented under L4 still apply. These are production-coverage limitations, so no full Ledger/Evidence package or system-transition PASS is claimed.

The implementation exposed no new required lifecycle subsystem or downstream transition beyond the accepted matrix. Original protective knowledge/fire facts, group comparison targets/context floors, retirement withholding and early interrupted DRY refine existing M36/M18/M43/M55 contracts. The ImmutableOwner issue is an existing M13 compatibility gap.

Only Ledger implementation/selftests and minimal owner/status/evidence/matrix documentation changed during Step 5. Locked roadmaps and architecture, accepted Evidence and Phase-4/5/6 source remain unchanged. No Authority/Execution/Runtime/Operations implementation, signer/private-key support, signing/send/broadcast, mutation RPC or real-capital capability was added. Tests used temporary databases and deterministic public fixtures; the protected runtime checkout, production database and processes were not modified.

NEXT: ChatGPT project review of L1–L6 and the composed proof, plus an explicit scope decision on the narrow Evidence ImmutableOwner correction. If authorized, close that compatibility issue with a separate bounded Evidence task and affected Ledger qualification. Authority/Step 6 has not begun.
