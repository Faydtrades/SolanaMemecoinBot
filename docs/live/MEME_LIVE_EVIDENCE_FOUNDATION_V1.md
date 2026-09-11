# MEME-LIVE Step 4 — Evidence Foundation v1

Status: **IMPLEMENTED_PENDING_PROJECT_REVIEW**. The bounded G001/T001 implementation and final adapter-to-consumer-port/fixture verification results are recorded below. Formal package acceptance and lane advancement remain with the ChatGPT project under `AGENTS.md`. Ledger/T002/T003, risk, signing/send, orchestration, Operations and real-capital authority remain outside this task.

Starting checkpoint: `1d0c27ae26425835aecf3dcd5cca440128ad0448`, clean local and remote `live/meme-production-readiness`. The literal protected path `D:\Tradingbot\solana\_memecoin\_bot\_phase1\_v0\_1` is absent on this machine; the previously identified active checkout `D:\Tradingbot\solana_memecoin_bot_phase1_v0_1` remains protected, clean master at `5d5cb0426fa423fd9528fb37d86e80b1333a8aa9`.

## Bounded tasks and consumer contracts (before source edits)

| Task | Deliverable | Known consumer boundary |
|---|---|---|
| E1 / G001 | Immutable source binding, controls/gap/cursor/coverage/liveness observations and durable source-evidence history, using read-only collector fixtures | Authority consumes coverage/deny reasons; Runtime consumes original lineage, cut and recovery evidence; no economic state or protective obligation is changed |
| E2 / T001 accounts | Closed read-only RPC surface, configured public wallet/genesis/provider profile, native SOL and SPL/Token-2022 enumeration/account evidence | Ledger opening/account-evidence port distinguishes complete inventory coverage, absent accounts, stale/unsupported/contradictory facts and differing context cuts; no spendable balance or position is created |
| E3 / T001 transaction evidence | Exact-signature status/transaction metadata, bounded finalized coverage and transaction-order evidence | Ledger receives public facts without finality adjudication/retry policy; Runtime receives proven transaction order or explicit insufficient ordering evidence |
| E4 / boundary qualification | Production adapters through immutable sanitized contracts into real downstream-facing ports or consumer fixtures | Distinguish healthy/unresolved continuity, complete/incomplete account coverage and supported-finalized/unknown/conflicting evidence without fabricating economic state |

One consequential worker runs at a time. Difficult source identity/coverage and transaction-proof work uses Astra Ultra; ordinary bounded adapters use Astra Extra High when additional Ultra reasoning has no material benefit. Each implementation task gets focused LOCAL evidence and CHIEF review before checkpoint. Final SYSTEM TRANSITION evidence is limited to available consumer ports/fixtures; unbuilt real Ledger/Authority/Runtime consumers remain named as pending integration.

## G001 operational coverage profile

Preserve the accepted collector and Phase-4/5 semantics. Bind explicit expected source lineage/model/anchor plus durable cursor witnesses; database file growth/mtime is not identity. Read collector tables in a bounded consistent read-only snapshot. Missing schema/identity/witnesses, cursor regression, invalid/future clock evidence, read truncation/failure, stale/lost subscription and recorded gaps deny new-exposure use.

The new operational profile may certify only a **receipt-bounded observed LIVE prefix**: the requested source cut is inside a durably observed active subscription, extends no later than a corroborating live receipt, and has ordered control/row witnesses. Current health additionally requires no known unresolved gap or uncertain session boundary since the original coverage start, including known facts after the requested cut. This is an explicit collector-trust assumption that its durable controls record connection loss and its committed rows report the observed stream; it is not a proof of chain-wide event completeness. It does not relabel the accepted research coverage export's open-EOF intervals as COMPLETE. A receipt without subscription/control continuity is insufficient. An unobserved tail stays UNKNOWN.

Every explicit gap overrides optimistic liveness. In particular collector recovery `DONE`, `DONE_NO_BOUNDARY` or `DONE_NO_INTERIOR` is not a completeness certificate: confirmed interior scans, skipped/missing blocks and unrecovered boundaries cannot prove whole-interval recovery. Re-reading the same lineage with positive coverage/liveness can recover a transient stale/read-unknown verdict; an unresolved recorded gap cannot be erased by a new receipt, process, source anchor or evidence epoch. Evidence journals preserve previous verdicts and cuts. Producer checkpoint/input completeness is a separate future Runtime obligation and remains necessary for admission.

## T001 evidence limits

The wallet is public configuration, with deterministic fixtures until a later authorized real binding. Read-only methods are explicitly enumerated outside Shadow's narrower allowlist. Reuse public-key validation, content fingerprints, account-byte types and accepted decoders where they fit; do not promote Shadow expected inventory or omit lamports/rent facts needed by Ledger.

Evidence binds chain identity, public wallet/accounts/signature, sanitized provider-profile identity, commitment/context, observation UTC and immutable content. No endpoint credential/error body, private key, seed, signing API or arbitrary RPC caller is exposed in evidence. Missing accounts are explicitly absent at a context, never silently zero-funded; null signature/transaction results never imply non-landing. Account enumeration coverage and coherent account-state cuts are separate claims. Unsupported Token-2022 shapes/extensions, incomplete metadata, contradictory responses and stale/unknown contexts remain explicit.

Per-attempt history work is bounded by a configured slot/request/response budget. A standard RPC's null lookup or pruned block cannot certify non-landing. Return UNKNOWN/INSUFFICIENT_COVERAGE unless exact coverage requirements and finalized boundary evidence are present under the declared proof profile. The future Ledger owns adjudication and retry. Chain order is relative to exact finalized transactions; same-transaction event order remains unresolved without event-level proof. No chain indexer or general wallet manager is added.

### Required bounded RPC profile

The reviewed implementation profile trusts a configured Solana JSON-RPC service to report the selected genesis and complete standard method results honestly. It does not claim cryptographic verification or Byzantine-provider consensus. Profile identity and limits are public configuration; endpoint URLs and credentials are transport-only. Actual deployment binding and no-broadcast environment qualification remain later authorized work.

Account observations preserve native lamports and full public account bytes, separate absent-at-context from zero, enumerate both supported token programs, and retain mint/program/owner and native-reserve/delegate/frozen/close-authority evidence. Reuse the existing base decoders; their permissive minimum-length checks alone do not validate extensions. V1 unsupported account/mint extensions remain explicitly unsupported. Full enumeration under the declared RPC profile, structural support and a coherent finalized account cut are separate claims. A differing read context must not silently become one wallet baseline.

The bounded canonical-interval profile starts from a stored finalized lower block anchor and walks parent links from an observed finalized upper block back to that exact anchor. Validate block hashes, parent slots and block-height progression; linked parents account for skipped slots. Every produced block in the interval must supply its complete ordered primary transaction-signature list within explicit budgets. Null/pruned/error/missing lists, broken ancestry, unbound lower anchor or exhausted budgets produce insufficient or contradictory coverage. Preserve whether the exact primary signature occurs, the root height, recent-blockhash/last-valid-height request binding and interval bounds as facts. Evidence never emits retry permission or an economic non-landing disposition; Ledger must verify the attempt's previously persisted pre-send bounds and adjudicate the result.

Exact finalized transaction evidence retains the requested primary signature, message/account identities (including loaded addresses), supported version, pre/post native and token balances, actual fee, execution outcome and recorded outer/inner instructions. Missing required metadata is unresolved; errors/log strings are excluded from sanitized contracts. Chain-order evidence binds exact signatures to the canonical block's ordered list. Different transactions can be ordered; two observations inside one transaction still require event-level proof from the future Runtime adapter.

Protocol references: [account enumeration](https://solana.com/docs/rpc/http/gettokenaccountsbyowner), [account fields](https://solana.com/docs/rpc/http/getaccountinfo), [transaction metadata](https://solana.com/docs/rpc/http/gettransaction), [signature search scope](https://solana.com/docs/rpc/http/getsignaturestatuses), [finalized block fields](https://solana.com/docs/rpc/http/getblock), and [base SPL account layout](https://github.com/solana-program/token/blob/main/interface/src/state.rs). Only documentation was read; no public-chain query was run for planning.

## Validation and execution boundaries

Tests use synthetic temporary SQLite sources/evidence stores and mocked HTTP transport through the actual RPC adapter. No active production DB or process is opened or manipulated. A separate temporary Python environment holds the repository's pinned dependencies. Focused existing compatibility tests are used for reused surfaces; broad regression is required only if accepted shared implementations change or focused evidence reveals cross-surface risk.

### Focused compatibility evidence

Interpreter: `C:\Users\Mari1\AppData\Local\Temp\meme-live-evidence-venv\Scripts\python.exe`; repository-pinned requirements installed in that temporary environment. Commands use `-B` and synthetic temporary fixtures.

| Command (after the interpreter) | Result |
|---|---|
| `-B scripts/phase4_continuous_market_source_selftest_v0_2.py` | Exit 0; 14 source/read-only/restart checks |
| `-B scripts/phase4_non_monotonic_inserted_at_selftest_v0_1.py` | Exit 0; 13/13, including 100,000 synthetic rows and deterministic replay |
| `-B scripts/phase5_shadow_readonly_rpc_selftest_v0_1.py` | Exit 0; 59/59 strict RPC/protocol/capability checks |
| `-B scripts/phase5_shadow_domain_capability_firewall_selftest_v0_1.py` | Exit 0; 43/43, temporary SQLite integrity `ok`, protected source hashes intact |

The timestamp self-test initially exited 1 before running its cases because this Windows checkout expanded a locked file to CRLF. The canonical Git blob SHA-256 is exactly the accepted `9c6a786d56137aa0d1f99c24d8878e60ab4a5846e8ec91b8ec1fabd056b5d34d`; its working file differs only by CRLF expansion. Hash-bound compatibility tests were therefore run from `C:\Users\Mari1\AppData\Local\Temp\meme-live-canonical-tests-t11edhd2`, a temporary export whose copied tracked source/script bytes were each verified against their Git blob identity. No lock, accepted source or test was modified. The LIVE implementation tests run in the authoritative isolated checkout.

## E1 / G001 implementation and review

Delivered `src/live/source_health_v0_1.py`, `src/live/evidence_store_v0_1.py` and `scripts/live_source_health_selftest_v0_1.py`. The actual collector adapter feeds immutable `SourceVerdict` and `SourceConsumerEvidence` contracts; the dedicated source-only journal supplies atomic append/predecessor checks, content digests, append-only rows, deterministic reopening, an atomic `(sequence, verdict)` read and exact sequence reads. It refuses collector databases as output. Neither the adapter nor the port creates or changes any economic state.

The source binding pins lineage, accepted source model/fingerprint, original coverage start and pump/control/receipt anchor witnesses. Each bounded SQLite read validates original anchors and saved cursor witnesses. All gaps and interrupted/uncertain boundaries remain explicit. Source dispositions are `HEALTHY`, `UNKNOWN`, `GAP`, `STALE`, `LOST` and `REGRESSION`, with machine-readable reasons. A consumer checks expected identity, exact requested cut and receipt age again at use time. A detected identity/cursor-integrity break remains latched in the journal even if the source later looks restored; this package has no reset/re-anchor or reconciliation bypass. Same-lineage valid evidence can recover transient read unavailability or staleness.

Supported read budget is explicit: default 10,000 rows per table per observation, configurable within 1–100,000 when establishing the fixed profile; exhausting it denies without committing a partial cursor. Receipt freshness is an explicit operational profile value (default 30 seconds, bounded 1–3,600), not a change to strategy clocks or locked research. Bounded-prefix continuity trusts append-only collector facts; it is not a whole-history database integrity certificate. Producer checkpoint/input completeness and journal lifetime/resource qualification remain future Runtime/Operations obligations already identified in the architecture.

Focused command: `-B scripts/live_source_health_selftest_v0_1.py`, using the interpreter above in the authoritative isolated checkout. Final worker and CHIEF review runs both exited 0 with **54/54** checks. Cases exercise actual adapter-to-consumer healthy/unknown/gap/stale/lost/regression transitions, every collector gap-recovery status, required ordered connection controls, future/decreasing timestamps, partial-read cursor retention, unsupported markers, unchanged source fixture hashes, immutable serialization, append/reopen/idempotence, competing-writer predecessor rejection, exact captured sequences and SQLite integrity `ok`. The first worker test run exposed temporary fixture handles remaining open on Windows; explicit fixture connection closure corrected that test cleanup issue before the successful runs.

CHIEF review found and corrected integrity-failure recovery and atomic evidence-sequence capture before checkpoint. The accepted boundary is the real read-only SQLite adapter through the immutable source contract into the Authority/Runtime consumer port and fixture. Real Authority ENTRY gating (M07) and Runtime reconstruction/input verification (M05/M47) are not built here. M02 therefore retains `OWNED_NOT_BUILT` for that real consumer integration, while its Evidence implementation/proof is recorded explicitly. Existing positions and protective obligations are outside this contract's state.

E1 checkpoint: `7930fa872bece3979cb820143aaf32c29ecda0d9`, normally pushed and verified equal to remote LIVE HEAD before E2 began.

## E2 / T001 wallet and account implementation and review

Delivered `src/live/public_rpc_v0_1.py`, `src/live/wallet_evidence_v0_1.py` and `scripts/live_wallet_evidence_selftest_v0_1.py`. At this checkpoint the closed RPC surface contains only `getGenesisHash`, finalized `getSlot`, finalized metadata-only `getBlock`, `getMultipleAccounts` and `getTokenAccountsByOwner` for SPL/Token-2022. E3 may add only its reviewed transaction/coverage reads. Existing Phase-5 RPC and decoders remain unchanged.

`WalletEvidenceRequest` binds the dedicated public key, expected genesis, minimum context and up to 32 explicitly expected token accounts with their mint/program identities. Immutable `WalletObservation` retains both genesis observations, start/end finalized slot bounds, an exact selected block anchor, account bytes/lamports/rent epoch/executable flags, both program inventories, expected account absences, mint reads, UTC and sanitized failures. Existing Phase-5 base account/mint decoders are reused with additional exact-layout/COption validation. Token units, native reserves, delegates, frozen state, mint/freeze authority and close authority remain public facts; no baseline, attribution, spendability or arming decision is made.

The `LedgerAccountEvidence` port re-derives its assessment and checks wallet/genesis/provider-profile identity, actual proven context floor, observation age and finalized-block age at use time. Inventory coverage is `COMPLETE`, `INCOMPLETE` or `CONTRADICTORY`; context is `COHERENT`, `INCOHERENT` or `UNKNOWN`; account-shape support is `SUPPORTED`, `UNSUPPORTED` or `UNKNOWN`; native-account presence is `PRESENT`, `ABSENT` or `UNKNOWN`. All reasons remain explicit and any unresolved reason makes the port unusable for a complete account-facts claim. Complete enumeration does not authorize a wallet baseline or classify pre-existing tokens.

V1 supports exact 165-byte token-account and 82-byte mint bases for both token programs. Extensions/trailing layouts, frozen/delegated token accounts, external close authority, unknown owner/program, inconsistent native reserve, or mint/account binding conflicts remain unsupported or contradictory. Missing token-program responses, required mint/account reads, absent native accounts, null/pruned block anchors, unknown/future/stale times, mismatching genesis/context and exhausted budgets remain unresolved. Null accounts are preserved distinctly from present zero-lamport accounts. WSOL units and rent reserve remain distinct from native wallet SOL.

Transport limits are explicit profile configuration, enforced per bounded observation: response/account sizes, inventory counts, request count, total response bytes and time. No retry/reset or generic RPC-call API exists. The endpoint stays transport-only; error bodies, logs and ignored provider metadata are omitted from evidence. Optional omitted node `apiVersion` is recorded as `UNREPORTED` ([upstream context type](https://github.com/anza-xyz/agave/blob/master/rpc-client-types/src/response.rs)); finite optional JSON display numbers are ignored while all actual integer fields reject float/bool and JSON rejects nonfinite numbers and duplicate keys.

Focused command: `-B scripts/live_wallet_evidence_selftest_v0_1.py`, using the same temporary interpreter in the authoritative isolated checkout. Final worker and CHIEF runs both exited 0, **88/88** checks. Actual `PublicReadOnlyRpc` -> `WalletEvidenceAdapter` -> immutable `WalletObservation` -> `ledger_account_evidence` cases distinguish complete/incomplete coverage, differing versus coherent finalized cuts, absent versus present-zero accounts, base SPL/Token-2022 and unsupported extensions, missing mints, ownership/program conflicts, stale/future/pruned evidence, immutable/sanitized output and all bounded transport failures. HTTP uses `httpx.MockTransport` only; zero real network requests. Known source files are unchanged, so the prior focused Phase-5 compatibility evidence remains applicable.

CHIEF review corrected optional RPC context/display-number handling, bracketing genesis observations, and the consumer floor check before checkpoint. The available transition boundary is exercised; actual Ledger opening-baseline/reconciliation consumers M14–M18 remain unbuilt. Exact transaction/coverage/order evidence is the next bounded task.

E2 checkpoint: `ccd304723592865440371f730cba19a6edd7bb66`, normally pushed and verified equal to remote LIVE HEAD before E3 began.

### Source correction identified at the package boundary

During E3, CHIEF's composed source fixture identified an E4 correction: requested cut 9, a known recorded gap at 10–11 and a new receipt at 12 allowed the older observed prefix. E4 removes the upper-cut clipping from both evaluation and in-memory healthy validation. Known recorded/control gaps and uncertainty since the original origin now deny current source health even after the requested cut. The original-origin boundary remains fixed; genuinely older facts outside it are not relabeled. The final boundary proof below records durable reopening and consumer behavior. No real source/admission integration is claimed by these fixtures.

## E3 / T001 transaction and bounded coverage implementation and review

The bounded implementation adds `transaction_evidence_v0_1.py` and `transaction_coverage_v0_1.py`, with only two additional wire methods (`getSignatureStatuses`, `getTransaction`) and the signatures-only form of the existing finalized `getBlock`. The final RPC producer identity is `LIVE-PUBLIC-SOLANA-RPC-0002`; its fingerprint includes the explicit observation/transaction/coverage budgets. Evidence created under the earlier E2 profile is not silently promoted to this profile.

Exact transaction facts preserve canonical legacy/v0 wire and message bytes/digests, the primary and full signature list, static/loaded account keys and lookup identities, recorded compiled outer/inner instructions, pre/post native/token facts, fee and sanitized outcome fingerprint. An earlier confirmed status may precede a later finalized transaction observation. The signature-status RPC context is explicitly not a finalized root. Required metadata omissions or unsupported versions remain unknown, and conflicting exact identity/outcome or finalized block claims remain contradictory. These are public observations; no signature is created or cryptographically authenticated by this adapter.

The coverage port labels a successful parent walk `COMPLETE_REQUESTED_INTERVAL` with scope `EXACT_LOWER_THROUGH_CHOSEN_UPPER_NOT_WHOLE_VALIDITY`. It separately retains the chosen upper height, current finalized root height, original lower anchor, last-valid-height/recent-blockhash binding and exact signature occurrences. A complete older interval can still omit part of the validity window. Future Ledger must compare those facts with its original persisted attempt and required window; this package emits no non-landing verdict or retry permission. Missing/pruned blocks, incomplete signature lists, exhausted limits or broken parent/hash/height evidence prevent the complete-interval claim.

Runtime's port reports canonical transaction `BEFORE`, `AFTER` or `UNKNOWN`, using exact finalized metadata and the block's ordered primary-signature list. Same-transaction event order remains `UNKNOWN` and requires the future Runtime event decoder. Historical transaction/intermediate block times may be old or absent; observation and current-root freshness are checked separately.

Default canonical coverage is bounded to 256 produced blocks and 2,048 slots, with hard configuration ceilings of 500 blocks and 8,192 slots. The RPC observation defaults to 320 requests, 64 MiB total and 180 seconds, with independent per-response (1 MiB), signature-count (8,192/block), wire (1,232 bytes), recorded-inner-instruction (1,024) and instruction-data (16 KiB) limits. Budget exhaustion preserves partial evidence and denies complete coverage. Each adapter instance permits one observation; there is no implicit retry/reset. Production provider capacity and retained-history sufficiency remain unqualified until later authorized deployment work.

Focused command: `-B scripts/live_transaction_evidence_selftest_v0_1.py` in the isolated checkout with the temporary interpreter above. Final worker and independent CHIEF runs both exited 0, **170/170** named checks. The directly affected wallet compatibility command `-B scripts/live_wallet_evidence_selftest_v0_1.py` again exited 0, **88/88**. All HTTP used `httpx.MockTransport`; no real chain request, keypair or signing operation occurred. `git diff --check` exited 0.

Fixtures exercise legacy/v0 canonical bytes and loaded addresses, actual success/failure/fee facts, exact status/metadata agreement, omitted versus contradictory metadata, null status with separately positive finalized metadata, root/time/profile/identity checks, read budgets, pruned blocks, skipped-slot parent walks, original lower-anchor equality, partial positive occurrences, complete older intervals missing the validity tail, and canonical transaction versus unresolved same-transaction event order. Competing finalized roots/parent facts must deny order even when the two observations individually look supported. No economic state is fabricated by these ports.

CHIEF review corrected optional status/error consistency, same-slot root/membership conflicts and cross-observation order conflicts. Produced-block height relationships follow the [Agave parent-bank increment](https://github.com/anza-xyz/agave/blob/master/runtime/src/bank.rs); all coverage fixtures use physically consistent slot/height values. These refinements serve the already assigned Evidence-to-Ledger M13/M33 and Evidence-to-Runtime M35 requirements. They introduce no new economic transition or authority. Real consumers remain unbuilt.

E3 checkpoint: `d28ea330f2f80a29f6adf3bc6dadd0b91a0f8d75`, normally pushed and independently read back from the remote LIVE branch before E4 began.

## E4 / consumer-boundary verification evidence

A separate sequential Astra Ultra reviewer implemented the final source correction and composed fixtures. CHIEF independently inspected the diff and reran all directly affected Evidence suites at the final source revision; every check returned true. Review demonstrated retained finalized parent/hash/height contradictions that the individual Ledger transaction/coverage ports did not reject. One small pure pair-consistency check now serves those ports and Runtime order, using only retained public facts and no additional RPC or history scope. It rejects conflicting same-slot facts, wrong explicit parent hashes/heights, a claimed parent interval that skips a known produced block, and impossible height increments across known skipped slots. Both typed block constructors enforce genesis/non-genesis parent and produced-height bounds. Missing historical times remain allowed; current-root freshness remains required.

### Focused local and composed transition results

Run in `C:\Users\Mari1\AppData\Local\Temp\meme-live-audit-e6b9a4b`, using `C:\Users\Mari1\AppData\Local\Temp\meme-live-evidence-venv\Scripts\python.exe`:

| Exact arguments after the interpreter | Final worker / independent CHIEF result |
|---|---|
| `-B scripts/live_source_health_selftest_v0_1.py` | Exit 0; 67/67 |
| `-B scripts/live_wallet_evidence_selftest_v0_1.py` | Exit 0; 88/88 |
| `-B scripts/live_transaction_evidence_selftest_v0_1.py` | Exit 0; 170/170 |
| `-B scripts/live_evidence_boundary_selftest_v0_1.py` | Exit 0; 40/40; composed system-transition evidence at the available boundary |

The composed suite uses the actual production adapter classes with synthetic temporary SQLite and `httpx.MockTransport`, then consumes their immutable sanitized outputs through the real Evidence ports. It does not substitute pre-approved port results. Account and transaction observations share the same configured public wallet/genesis/provider and consistent finalized root. No real RPC endpoint, real wallet binding or economic state is required by these fixtures.

| Actual transition exercised | Established distinction |
|---|---|
| Collector SQLite -> source verdict -> append-only journal -> captured/reopened Authority/Runtime evidence port | Healthy versus read-unknown/stale/gap; reread alone cannot recover staleness; restored same-lineage facts/new receipt can recover appropriate transient failures; a later known gap still denies an older cut after reopen; source bytes unchanged and journal integrity `ok` |
| Public RPC account reads -> wallet observation -> Ledger account port | Complete versus incomplete enumeration; complete but unsupported Token-2022 shape; coherent versus differing account cuts; same-context contradictions; absent native account versus present zero; impossible finalized anchor remains unresolved |
| Public RPC exact status/wire/metadata -> transaction observation -> Ledger transaction port | Supported finalized legacy/v0 facts versus null/unknown and contradictory identity/outcome/parent facts; fee remains a public fact without settlement attribution |
| Public RPC canonical parent walk -> bounded coverage observation -> Ledger coverage port | Complete chosen interval can still miss the validity tail; pruned link means insufficient coverage even when a positive signature occurrence is retained; contradictory retained root/parent facts deny completeness |
| Two exact transaction observations -> Runtime order port | Consistent canonical BEFORE/AFTER versus contradictory known anchors or unresolved same-transaction event order; no price eligibility or execution decision |

The composed test also checks immutable/sanitized outputs, append-only source-only tables and all seven allowed public RPC wire reads. No generic mutation/sign/send API is exposed. Focused Phase-4/5 compatibility evidence above remains applicable: shared code and accepted decoder/source/Shadow RPC contracts were not changed. Broader regression was therefore unnecessary.

The remaining real consumer obligations are already assigned by Architecture v2:

| Evidence port | Future real consumer and required use |
|---|---|
| `source_consumer_evidence` plus captured journal sequence | Authority M07 checks the original source identity/cut and current evidence before ENTRY; Runtime M05/M47 separately verifies producer reconstruction/checkpoint/input completeness. Source health cannot discharge positions or protective obligations. |
| `ledger_account_evidence` | Ledger M14–M18 establishes the opening baseline, attributes holdings/adjustments and derives settled/spendable facts only from supported coherent evidence; enumeration alone is not attribution or funding authority. |
| `ledger_transaction_evidence` | Ledger M15–M17/M33 binds the original exact attempt/signature/message and performs finality/actual settlement adjudication with idempotent durable economic state. A public failed outcome and fee remain facts until that consumer exists. |
| `ledger_canonical_coverage` | Ledger M33 checks its original pre-send lower anchor, blockhash and last-valid height, and verifies that the complete requested interval covers the necessary validity window. Missing occurrences, timeouts and null lookups do not independently prove non-landing. |
| `runtime_transaction_order` | Runtime M35 applies exact acquisition/event identity and fixed knowledge-cut rules. Transaction order alone cannot establish event order inside a transaction, price eligibility or a protective trigger. |

### Remaining boundaries and source-control surface

M02 and M13 retain `OWNED_NOT_BUILT` because their actual Authority/Ledger/Runtime consumers listed above do not yet exist. Only their Evidence-owned factual proof descriptions are updated. The supplied results cover the expressly permitted adapter/port/fixture boundary; they do not mark the real economic/runtime lifecycle verified or authorize deployment, a soak or capital. Formal package classification remains pending project review.

No new required downstream transition was discovered. The source older-cut correction and finalized parent-consistency corrections address defects inside already assigned M02/M07 and M13/M33/M35 responsibilities. Same-transaction event identity/order, original pre-send validity binding, coherent opening-baseline attribution and producer reconstruction remain the named existing future obligations.

Unresolved source states are `UNKNOWN`, `GAP`, `STALE`, `LOST` and `REGRESSION`, with durable reasons. Wallet inventory is `INCOMPLETE` or `CONTRADICTORY` when coverage cannot be used; context may be `INCOHERENT`/`UNKNOWN`, shape `UNSUPPORTED`/`UNKNOWN`, and native presence `ABSENT`/`UNKNOWN`. Exact transaction evidence is `UNKNOWN` or `CONTRADICTORY`; canonical coverage is `INSUFFICIENT_COVERAGE` or `CONTRADICTORY`; order may be `UNKNOWN`. Limits, missing/pruned metadata, stale/future facts, unsupported versions/layouts and conflicting identities never silently become zero, finality or non-landing. Even `COMPLETE_REQUESTED_INTERVAL` has only its explicit interval scope; future Ledger must establish the full required validity coverage and adjudicate.

The package changed-file surface is limited to:

| Area | Files |
|---|---|
| Evidence implementation | `src/live/__init__.py`, `src/live/source_health_v0_1.py`, `src/live/evidence_store_v0_1.py`, `src/live/public_rpc_v0_1.py`, `src/live/wallet_evidence_v0_1.py`, `src/live/transaction_evidence_v0_1.py`, `src/live/transaction_coverage_v0_1.py` |
| Focused fixtures | `scripts/live_source_health_selftest_v0_1.py`, `scripts/live_wallet_evidence_selftest_v0_1.py`, `scripts/live_transaction_evidence_selftest_v0_1.py`, `scripts/live_evidence_boundary_selftest_v0_1.py` |
| Delivery/control status | This Evidence record; `docs/live/MEME_LIVE_OWNER_ACCEPTANCE_AND_LANE_STATUS.md`; status-only line in `docs/live/MEME_LIVE_PRODUCTION_CLOSURE_ARCHITECTURE_V2.md`; Evidence-owned M02/M13 descriptions in `docs/live/MEME_LIVE_PRODUCTION_LIFECYCLE_MATRIX_V2.md` |

The isolated LIVE branch is the implementation lane; master and the active runtime checkout are not updated. Collector/source/Phase-4/5 implementations, locked plans/roadmap, model policy, strategy/research artifacts and production data/processes remain unchanged. All signing/send/broadcast/real-capital authority remains OFF. Real public-wallet/provider configuration and deployment/history-capacity qualification remain later HUMAN_EXTERNAL/authorized work and did not block implementation.

NEXT: ChatGPT project review of this package and its recorded transition evidence. Following that review and separate authorization, the recommended engineering package is locked Step 5 / Ledger T002/T003, beginning with one bounded durable opening-baseline/original-evidence binding contract. No Step-5 implementation was begun.

## Step 6 / A0 — canonical ImmutableOwner prerequisite

The supplied Step-6 instruction explicitly authorizes this narrow correction before Authority consumes account shape. Step-5 L1–L6 and their available Evidence -> Ledger transition evidence were project-accepted within the documented supported profile; see [owner/lane status](MEME_LIVE_OWNER_ACCEPTANCE_AND_LANE_STATUS.md#project-decision-and-bounded-authority-work-supplied-with-step-6). A0 implementation evidence is recorded here for project review.

Wallet observations now explicitly default to `live_wallet_account_evidence_v0.2`. This version accepts a Token-2022 token account only when its standard 165-byte base is followed by exactly `02 07 00 00 00`: account type Account, ImmutableOwner extension, zero-length value. The [official Token-2022 definition](https://github.com/solana-program/token-2022/tree/aa84ca89f26127a8f881c46484974b534fefb6f6/interface/src/extension) is pinned at that revision, with the two source hashes recorded beside the implementation. No generic TLV parser or additional extension support was added.

All original base amount/native/delegate/frozen/close-authority/program/mint/wallet checks remain. Truncation, wrong type/length/endianness, duplicate or unrelated extensions, padding, conflicting same-context account reads, incomplete enumeration and incoherent cuts remain unresolved or unsupported. Legacy SPL cannot claim this extension. Exact integer amounts, including u64 maximum, and original sanitized public bytes remain preserved.

A finite legacy `v0.1` branch retains the exact earlier 165-byte-only interpretation. Original schemas and digests already belong to the Ledger codec, so no Ledger production edit, schema migration or receipt reinterpretation was required. Tests reject a schema relabel with an unchanged original digest and prove that a historical unsupported baseline/support receipt remains unchanged after new evidence is accepted and after reopen.

Worker (Astra Ultra) and CHIEF independently ran from the isolated checkout using `C:\Users\Mari1\AppData\Local\Temp\meme-live-evidence-venv\Scripts\python.exe -B`:

| Script | Checks | Exit |
|---|---:|---:|
| `scripts/live_wallet_immutable_owner_selftest_v0_1.py` | 71 | 0 |
| `scripts/live_wallet_evidence_selftest_v0_1.py` | 88 | 0 |
| `scripts/live_ledger_baseline_selftest_v0_1.py` | 126 | 0 |
| `scripts/live_ledger_settlement_selftest_v0_1.py` | 256 | 0 |
| `scripts/live_ledger_custody_selftest_v0_1.py` | 279 | 0 |

All **820 checks** succeeded. `git diff --check` exited 0. CHIEF logs are in `C:\Users\Mari1\AppData\Local\Temp\meme-live-authority-a0-chief-20260910`. A0 check digest: `98667e2c798acd4a6b64186b7662a053903891b8060c3e0a3db0d87cfbb66c9d`.

Actual MockTransport Wallet/Transaction adapters -> original Evidence -> Ledger empty canonical170 baseline -> actual PumpSwap BUY into that existing ATA -> complete application -> actual scoped token/native/fee/account-lamport facts -> durable reopen is proved. Actual acquired units deliberately differ from quoted expected units. Hostile or original-v0.1 support produces no postings/position and retains the held attempt. Canonical application receipt `fe42ae7ba4a51e975714ae45b754b52722e5d102629e97478d74aab148189476`; custody digest `0cab49812889ce7d04b67b2e153680b90c4e6521951967a71f0287f145e5b3fe`; original support `9d5407be5b075bc8b66a1b87629b2fe0b8784934bd771a92905c2504db6fc06e`.

The accepted L4 profile still excludes fresh Token-2022 ATA creation (`ACCOUNT_CREATE_SHAPE_UNSUPPORTED` / `ATA_EXTENSION_INITIALIZATION_UNSUPPORTED`); A0 changes account evidence, not that transaction lifecycle. The Authority package must deny plans outside the supported attribution profile. This is an explicit existing-profile limitation, not permission to expand Ledger or support arbitrary extensions.

Changed implementation/selftests: `src/live/wallet_evidence_v0_1.py`, new `scripts/live_wallet_immutable_owner_selftest_v0_1.py`, and the affected old-version denial fixture in `scripts/live_ledger_settlement_selftest_v0_1.py`. No real RPC test, private material, signing/send, production data/process or protected-checkout change. The broader Step-5 regression was not repeated. No lifecycle row or full package PASS is self-authorized. NEXT: A1 Authority policy/stop/clock state under the already supplied Step-6 authorization.

## Remaining Step-9 gates - accepted-evidence disposition

Under the owner's remaining-Step-9 authorization from accepted C3 revision 98cd5952cab9e7e35b3ab5151eb41a8dbd77f557, CHIEF reconciles M13 against its exact existing consumer contract. **M13 is VERIFIED within its existing supported profile.** The two consumers described as pending in the historical M13 record already have accepted evidence; no new implementation or qualification campaign is required.

- Account evidence into actual Ledger/Authority A3/A4b and A5 consumers was already recorded in M13.
- [Execution Q4](MEME_LIVE_EXECUTION_FOUNDATION_V1.md#q4--original-public-finality-handoff-and-composed-buysell) documents the actual public Evidence adapter from the original signed envelope/anchors into Ledger ingestion, including null/UNKNOWN across reopen, later exact finality, contradictory identity and current-fence negatives. The accepted Execution publication is 9b9f210320bf43ecff37007bf11f3e63995b6a15, production freeze 14d193c86ec77bb4c183fc4dfbd3cb08f82cc4c9. Existing composition evidence: 212 checks, exit 0; q4-chief.log SHA256 0d23b1c230013abe402419ac8d33501b791c2c4120637d233c452ca924208fe6.
- Accepted Runtime B2 consumes runtime_transaction_order into actual exit observations and immutable knowledge cuts; M35 is already VERIFIED. Strict/same-slot completeness, unproven/same-transaction UNKNOWN, contradiction, late enrichment and reopen are evidenced. B2 checkpoint 536ccd04f84b4d09ca6a5c9363a0c36ca21deed1; existing focused/independent 102-check result exited 0, log SHA256 6a0bb9c0f29cbaffa999c2388121a43c1fef573e0b7e9a043897cd7c3d4feddb. Owner-accepted B5 freeze 35aa742ac7ac4911046e60ac09881adb6893c7f3, publication d2839809bf000904f76c75159ec42c3aee021af4; see [Runtime B5](MEME_LIVE_RUNTIME_FOUNDATION_V1.md#step-8b-b5--frozen-bounded-transition-review-and-handoff).

These are existing accepted results, not newly run checks. Historical Step-4 pending-consumer statements retain their original checkpoint scope. The classification change closes stale M13 bookkeeping; it does not claim full Evidence-package S04-S07/S09/S11-S12/E01/E03/E05 acceptance, the Step-10 full integrated dossier, or real deployment qualification.

Coverage/context/support remain distinct. Null is never zero/non-landing; coverage proves only the requested interval; canonical170 support does not reinterpret original v0.1 records; same-transaction event order remains UNKNOWN; fresh Token-2022 ATA attribution remains unsupported. Actual public wallet/provider binding, capacity and retained-history sufficiency remain later authorized deployment/environment qualification. No implementation was identified as missing merely because those gates remain.

One fresh compact Astra High worker supplied this read-only consumer mapping; CHIEF checked the exact accepted descriptions. No source inspection, tests, old evidence regeneration, public RPC request, key loading or mutation occurred. Current counts after M13 only: **44 VERIFIED / 11 OWNED_NOT_BUILT / 6 HUMAN_EXTERNAL / 0 BLOCKED; 61 total**.
