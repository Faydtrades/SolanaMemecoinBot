# MEME-LIVE Step 4 — Evidence Foundation v1

Status: IN_PROGRESS. Owner-authorized scope: G001 + T001 read-only Evidence implementation, focused tests, CHIEF review and normal LIVE-branch checkpoints. Ledger/T002/T003, risk, signing/send, orchestration, Operations and real-capital authority remain outside this task.

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

The new operational profile may certify only a **receipt-bounded observed LIVE prefix**: the requested source cut is inside a durably observed active subscription, extends no later than a corroborating live receipt, and has ordered control/row witnesses with no unresolved intersecting gap or uncertain session boundary. This is an explicit collector-trust assumption that its durable controls record connection loss and its committed rows report the observed stream; it is not a proof of chain-wide event completeness. It does not relabel the accepted research coverage export's open-EOF intervals as COMPLETE. A receipt without subscription/control continuity is insufficient. An unobserved tail stays UNKNOWN.

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

NEXT: E2 / T001 public wallet and account evidence.
