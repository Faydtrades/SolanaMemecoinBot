# MEME-LIVE Execution Foundation v1

Status: **AUTHORIZED_IN_PROGRESS**. Owner instruction and exact starting revision are recorded in [Owner acceptance and lane status](MEME_LIVE_OWNER_ACCEPTANCE_AND_LANE_STATUS.md#step-7--bounded-execution-authorization). Governing scope: [Architecture v2, Execution](MEME_LIVE_PRODUCTION_CLOSURE_ARCHITECTURE_V2.md#45-execution--exact-envelope-signing-and-submission).

## Bounded queue and consumer contracts

Execute one item at a time: implementation, focused tests, short CHIEF review, local validation record, checkpoint and normal push before the next item. No project/package PASS is implied by a local item result.

| Item | Producer -> contract -> consumer | Boundary |
|---|---|---|
| Q1 | Actual accepted Ledger/Authority economics -> reused route/quote/plan, exact final bytes and original exact simulation evidence -> Authority message validation | Read-only public evidence; immutable amount/root, accounts, instruction order, fee/compute policy and lease; no signer/send |
| Q2 | Current exact Authority consumption -> guarded isolated signing -> verified exact signature/envelope | Synthetic signer qualification; no arbitrary-message public signing path or production key access |
| Q3 | Verified signed envelope -> durable envelope/send claim -> narrow transport and immutable observations | Claim before call; lost response UNKNOWN; same-signature rebroadcast; replacement requires actual Ledger proof and fresh required permission |
| Q4 | Actual BUY/SELL Execution components with deterministic external ports -> Ledger finality/reconciliation handoff | Same safety chain for both sides; no quote-derived settlement or injected preapproved Execution result |
| Q5 | Frozen component evidence -> short SYSTEM TRANSITION review and affected matrix disposition -> project-review handoff | One justified final regression for changed message/signature/send semantics; Runtime-dependent transitions remain pending |

The implementation reuses accepted `src/phase5` primitives while preserving its non-signing/non-broadcast capability boundary. Ledger retains finality, custody and settlement authority. Execution consumes actual accepted BUY economics and supplied actual-unit SELL actions; it does not create Runtime controllers, scheduling, producer loops or Operations. Real wallet/configuration binding and capital authorization are not supplied by this package.

## Validation and delivery boundary

Focused standalone suites run against disposable stores and deterministic public RPC/simulation/signer/transport boundaries. No production database or private material is used. The Step-6 27-suite / 3,603-check regression remains authoritative for its frozen checkpoint. Architecture section 4.5 requires one relevant final broad regression when exact-message serialization, signer guards and send/retry semantics have changed; no broad regression is run between queue items.

No matrix classification is promoted by queue creation. Item results, exact evidence and limitations are appended only after actual qualification. Final project/package acceptance belongs to project review. Step 8 / Runtime and Operations remain not authorized.

## Q1 — Exact message and simulation

**LOCAL PASS**, following bounded CHIEF review and an independent focused run. Worker: **Astra High**. Parent checkpoint: `0c764c049ad3acd71f0a978bb9524fae45abfd30`; the Q1 checkpoint is the commit containing this record. Overall Step 7 remains in progress and pending eventual project review.

`src/live/execution_message_v0_1.py` consumes the actual accepted Ledger/Authority action and builds a separate LIVE intent through accepted Phase-5 route, integer quote and plan factories. Explicit ComputeBudget limit/price precede the plan instructions. A single recent-hash lease, canonical one-wallet legacy message (maximum 1,232 transaction bytes), zero-signature simulation and exact total-fee read retain the same message. The immutable transfer carries canonical original evidence and captured public requests/results; account bytes, fee, lease/validity, simulation and freshness timestamps are bound back to the capture. Actual A4a validation gates durable Ledger preparation. An identical preparation retry returns the same stored identity without conferring permission.

`src/live/execution_readonly_v0_1.py` reuses Evidence account parsing/budgets and accepted Phase-5 typed results. Its closed wire methods are genesis, account batch, recent blockhash, height, validity, simulation and exact-message fee reads. Endpoint/header/error-body material is excluded from returned evidence. Protocol/transport/lease failures raise finite sanitized errors without returning preparation; null fee or unsupported simulation remains actual A4a UNKNOWN/DENIED and cannot prepare. No automatic refresh/retry or signing/send exists in Q1.

The focused fixture drives actual production producer -> A4a -> Ledger PREPARED -> current A4b SIGN consumption for BUY and actual-unit SELL on both Pump and PumpSwap. It rejects changed accounts/instructions/fees/blockhash/timestamps, changed custody context, unavailable CPI, null fee, failed simulation, expired lease, contradictory provider facts and arbitrary RPC methods. The CHIEF-found freshness-capture gap was corrected within Q1 and qualified by timestamp-tamper cases. SELL's initial fixture denial correctly identified incomplete Ledger account coverage; complete original wallet evidence resolves it without an Authority change.

Commands use `C:\Users\Mari1\AppData\Local\Temp\meme-live-evidence-venv\Scripts\python.exe -B`:

| Script | Checks | Exit |
|---|---:|---:|
| `scripts/live_execution_message_selftest_v0_1.py` | 81/81, worker and independent CHIEF run | 0 |
| `scripts/live_authority_message_evidence_selftest_v0_1.py` | 298/298 | 0 |
| `scripts/live_authority_message_control_selftest_v0_1.py` | 362/362 | 0 |
| `scripts/phase5_shadow_unsigned_plan_simulation_selftest_v0_1.py` | 116/116, canonical export | 0 |

Logs: `C:\Users\Mari1\AppData\Local\Temp\meme-live-q1-validation` (`q1-final.log`, `q1-chief.log`, `a4a-captured-output.log`, `a4b-captured-output.log`, `phase5-canonical.log`). Independent Q1 output SHA256: `85e7b4ef6cbf7697954b1da1a2fbad5d8d804d97aec1ac062ca45f5e83f9500c`. The ordinary checkout Phase-5 run reported 115/116 solely on the known protected-file line-ending hash check; the successful run used `C:\Users\Mari1\AppData\Local\Temp\meme-live-canonical-tests-t11edhd2`, whose existing 365 Python files were verified against current Git blobs. No locked files were altered. `git diff --check` exited 0; no broad regression was run.

Q2 must consume the exact prepared message and fresh current A4b SIGN delivery, then enforce its final call boundary. Q1 context/preparation is never permission. Preparation is limited to first-attempt ordinal 1; proof-gated replacement belongs to Q3. Existing supported account/venue restrictions remain unchanged. M20/M24/M26/M27/M39 receive Q1 producer evidence, but classifications remain unchanged until the package disposition review. No new downstream transition was discovered.

## Q2 — Guarded autonomous local signer

**LOCAL PASS**, following bounded CHIEF review and an independent focused run. Worker: **Astra Extra High**, selected for the concrete one-use Authority delivery, journal continuation and final-call message/source/clock semantics. Parent checkpoint: `3210f6ebfb1e147cfe6819c78c449c1b90bcfd5e`; the Q2 checkpoint is the commit containing this record. No project/package acceptance is asserted.

`src/live/execution_signer_v0_1.py` exposes one guarded signing operation for an isolated in-memory solders key. It accepts the exact Q1 production object, stored preparation and newly consumed A4b SIGN delivery. It checks root/action/wallet/message/lease/profile identity, current writer generation and common cut, original source-store identity, intact attempt/lane and a fresh chained trusted-clock sample. Existing Ledger and source-journal write locks protect the finite local final-call interval; no network operation occurs under them and no cross-database commit guarantee is claimed. The returned immutable public envelope cryptographically verifies the signature against the exact message and wallet and carries no send permission. There is no key loading/serialization, arbitrary-message signing method, Phantom dependency or send transport.

The only Authority/Ledger integration change is ephemeral delivery custody: thread-safe one-time spending, copy/deepcopy retaining the same spent object, serialization rejection and the original source-store reference passed by the existing Ledger factory. Durable codecs, schemas and Authority decisions are unchanged. A failed or lost return spends the delivery and leaves durable SIGN consumption and possible-signing uncertainty intact. Local cancellation then yields UNKNOWN with the lane held; reopen/history cannot re-create key-call permission. Q3 owns durable envelope persistence and send claims.

Review corrected two direct boundary gaps: rollback after a failed post-BEGIN validation, and rejection of a copied same-row source journal in place of the actual source store. Focused qualification also covers actual BUY/SELL on Pump/PumpSwap, wrong signer/signature/message, stale or revoked controls/profile, interrupted or regressed clock/source/fence, callback and concurrent replay, lost return, no lingering locks and sanitized external callback errors. Synthetic ephemeral keys exist only in the disposable test process; no production key, real mainnet signature or broadcast was used.

Commands use the same isolated Python interpreter recorded for Q1:

| Script | Checks | Exit |
|---|---:|---:|
| `scripts/live_execution_signer_selftest_v0_1.py` | 275/275, worker and independent CHIEF run | 0 |
| `scripts/live_execution_message_selftest_v0_1.py` | 81/81 | 0 |
| `scripts/live_authority_message_control_selftest_v0_1.py` | 362/362 | 0 |

Logs: `C:\Users\Mari1\AppData\Local\Temp\meme-live-q2-validation` (`q2-final.log`, `q2-chief.log`, `q1-compatibility.log`, `a4b-compatibility.log`). Independent Q2 output SHA256: `164fcaa1dd302b1006f57b23bfcc9c09394abaed66aca97e425747583a2ea01e`. A4b check digest: `023bd75b64ba9b4846985e5e3fc398d9cbd3c106e8d9034eccd71953bc8d4327`. Staged scope/whitespace checks accompany the checkpoint; no broad regression was run. M28/M29 receive actual current Authority -> synthetic key -> verified public envelope evidence, with durable envelope/send consumption still assigned to Q3. No new downstream transition was discovered; Runtime/Operations remain unimplemented.
