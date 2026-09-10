# MEME-LIVE Step 5 — Ledger Foundation v1

Status: IN_PROGRESS under the supplied Step-5 owner instruction. Codex reports implementation evidence; project/package acceptance remains with the ChatGPT project. No Authority/Step-6 work is authorized here.

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
