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
