# Memecoin LIVE Production Readiness — Locked Roadmap

Status: **LOCKED ROADMAP / NOT YET AUTHORIZED FOR IMPLEMENTATION**

Repository: `Faydtrades/SolanaMemecoinBot`

Current remote master at roadmap lock: `5d5cb0426fa423fd9528fb37d86e80b1333a8aa9` (`MEME-P6-T002B: fix Windows worker ownership`).

Latest accepted production/shadow foundation: Phase 5 `COMPLETE / PASS / ACCEPTED / CHECKPOINTED`, canonical completion commit `6f18e0531593a679204548bf31d142becb19a0df` with final accepted Phase-5 correction `2c75f98ad13732f369a67c322d831ffb0913ff0e`.

## Activation condition

Do not begin this lane until the BTC/SOL production-readiness lane is finished or the human owner explicitly changes priority. Starting any task below requires a separate explicit human authorization.

When activated, create a dedicated production-readiness branch from the then-current clean repository checkpoint. Phase 5 accepted contracts are the technical dependency baseline. Phase 6 research files/history may remain present in Git history but are **not** a dependency for production readiness and must not be modified by this lane unless an exact production blocker is proven and separately authorized.

## Finish line

The memecoin bot is production-infrastructure complete when it can, without manual babysitting:

- continuously consume the accepted Pump/FirstPullback source;
- reject new exposure when source continuity or chain truth is uncertain;
- use the accepted Pump/PumpSwap route, quote and transaction-plan contracts;
- maintain durable local live transaction/order state;
- reconcile that state to Solana wallet, token-account, signature and transaction truth;
- enforce live risk limits and durable kill switches;
- sign only an exact approved transaction message;
- submit only through a narrowly bounded Solana mutation adapter;
- survive crashes, network/RPC loss, delayed confirmation and reboot without duplicate trades;
- run unattended with heartbeat, alarms, bounded restart/watchdog behavior;
- pass a real-mainnet no-broadcast soak;
- finally pass a separately human-authorized micro-capital BUY/SELL smoke.

**Profitability is explicitly not part of this finish line.** Strategy/OOS research remains separate. The production system may be declared infrastructure-ready while no strategy is approved for meaningful capital.

## Frozen reuse rule

Do **not** rebuild the accepted Phase-5 stack. Reuse it as immutable upstream execution evidence where possible:

`CandidateSignal / ExecutionIntent -> accepted Pump/PumpSwap venue state -> accepted executable quote -> accepted unsigned transaction plan -> LIVE authority/signing/send/reconciliation`

The lane must not rewrite Pump/PumpSwap decoding, quote arithmetic, route selection, instruction ordering, ATA/WSOL setup or the accepted zero-signature simulation contracts merely to make them "live". Any required delta must be minimal, explicit and reviewable.

Phase-5 capability firewall remains valid for `src/phase5`; signing/send capability belongs in a separate LIVE package/layer rather than weakening the Shadow firewall.

## Locked task sequence

### MEME-LIVE-G000 — Production Lane Split
Create the dedicated LIVE production-readiness lane and minimal control plane. Preserve Phase-5 locks and make Phase 6 explicitly out of scope. No execution capability is added here.

### MEME-LIVE-G001 — Collector / Source Health Gate
Turn existing collector/gap/coverage truth into an explicit operational guard. Stale source, unresolved gap, collector loss, cursor regression or unknown continuity must deny new ENTRY exposure. Recovery requires explicit proof of restored continuity. Avoid collector redesign unless a concrete defect requires it.

### MEME-LIVE-T001 — Solana Read-Only Wallet + Chain Truth
Bind one dedicated bot wallet by public key only. Add narrowly bounded read-only truth for SOL balance, relevant SPL/Token-2022 accounts, transaction/signature status and required chain evidence. No private key, signing or submission.

### MEME-LIVE-T002 — Durable Live Transaction OMS
Create a dedicated LIVE durable store/journal, separate from Phase-5 Shadow persistence. Preserve stable logical intent/transaction-attempt/signature identities and append-only audit semantics. Crash/reopen and conflicting replay must fail closed. No network mutation.

### MEME-LIVE-T003 — On-Chain Reconciliation Engine
Reconcile local LIVE state against authoritative wallet/token-account/signature/transaction truth. Distinguish confirmed/finalized/failed/unknown without inference. Unknown or contradictory state blocks fresh mutation.

### MEME-LIVE-T004 — Live Risk Envelope + Kill Switch
Implement explicit production limits: trade/notional cap, global and mint exposure, slippage/impact guard, priority-fee cap, failure budget, source-health guard, durable reservations/exposure lifecycle and global kill. Default state is unarmed/fail-closed. No operational capital policy is invented from research results.

### MEME-LIVE-T005 — Signer + Exact Live Transaction Envelope
Introduce the dedicated hot-wallet signing boundary. Secrets remain local and never enter Git, prompts, logs or evidence. The signer may sign only an exact T002/T004-approved message derived from accepted Phase-5 plan contracts. Add bounded ComputeBudget/priority-fee policy as an explicit live envelope and simulate the exact final message before signing. **No send yet.**

### MEME-LIVE-T006 — Solana Send + Confirmation Adapter
Add the only mutating Solana boundary: submit an already-approved signed payload and observe confirmation/finality. No generic RPC mutation escape hatch. Unknown send outcome must preserve the exact original signed message/signature and re-observe that identity; no blind creation of a new blockhash/signature.

Critical invariant: a fresh blockhash/new signature is allowed only after authoritative proof that the previous signed transaction cannot still land. A timeout is not proof of failure.

### MEME-LIVE-T007 — Live Runtime Orchestration
Connect the accepted upstream intent seam through source-health, risk, quote/plan, OMS, signer, send and reconciliation. Exact replay must never create a second logical trade. For real capital, exactly one approved exit policy/track may control one live position; FINAL-A/FINAL-B/SENS-C remain alternative research/shadow tracks and must never all execute against one real position.

### MEME-LIVE-T008 — Crash / Network / RPC Recovery Acceptance
Prove recovery at every durable cut: before/after sign, before/after send, lost send ACK, timeout/socket/429/5xx, delayed confirmation, conflicting RPC views, expired blockhash, reboot, chain-read loss and partial local progress. No duplicate trade, fabricated finality or silent fresh signature is permitted.

### MEME-LIVE-T009 — Unattended Operations
Standalone process, singleton ownership, heartbeat, sanitized logs/alarms, durable bounded restart budget/backoff, graceful operator stop, Windows autostart/watchdog and safe reconstruction. Dashboard must not be required for survival.

### MEME-LIVE-T010 — Real Mainnet No-Broadcast Soak
Run the production stack for multiple hours against the real collector, real mainnet RPC and real wallet **public state** with final plan build/simulation but signing/send physically disabled. Include controlled process restart and network/RPC interruption. Prove no local state drift and clean recovery.

### MEME-LIVE-T011 — Controlled Micro-Capital Smoke
Separate explicit human gate. Use very small capital for one controlled real BUY and one controlled SELL/exit through the complete stack. Verify local OMS <-> chain signature/transaction <-> wallet/token-account reconciliation, kill behavior and restart safety. Only after this may the infrastructure be called production-ready.

## CHIEF + worker operating method

Use the same efficient pattern established in the BTC/SOL lane:

- one CHIEF plus exactly one consequential production worker per task;
- one bounded task at a time;
- CHIEF reuses verified context and reads only the new task/state delta plus directly affected files/interfaces;
- no fresh bootstrap or broad reread of Phase 0-6 for every task;
- no architecture ceremony or new framework unless a concrete blocker proves it necessary;
- diff-first review;
- targeted/self-test development loops first;
- broad/full regression only when an accepted production surface is actually changed or a targeted failure exposes cross-surface risk;
- narrow corrections stay with the same worker/context;
- no repeated full test ritual after test-only or documentation-only changes;
- human wall-clock soaks do not consume Codex time while the clock runs;
- every implementation checkpoint must be committed and pushed so remote HEAD is authoritative;
- ChatGPT project review remains acceptance authority; worker status alone is never owner acceptance.

Quality remains more important than quota, but wasted context, duplicated reads and duplicated broad tests are explicitly prohibited.

## Safety invariants

Until the relevant task and human gate explicitly authorizes otherwise:

- no live signing;
- no transaction submission/broadcast;
- no wallet seed/private key in repository, prompt/chat, command line, logs or evidence;
- no arbitrary/generic mutating RPC interface;
- no strategy tuning, candidate reselection or Phase-6 outcome peeking in this lane;
- unknown source/chain/local state fails closed;
- no automatic transition from no-broadcast soak to real-money smoke;
- no real capital increase without separate human authorization.

## Research separation

Phase 6 frozen OOS research and future strategy discovery continue independently. The failed/non-salvageable OOS collection attempt does not block this production-readiness lane. Its source-gap lesson is represented only by MEME-LIVE-G001 operational health gating.

A strategy may later be swapped into the finished runtime through the accepted intent seam after separate research acceptance. Production-readiness tasks must not use profitability as an acceptance criterion.

## Planning estimate at lock time

Expected active CHIEF/worker engineering from activation to production-ready infrastructure: approximately **10-16 hours**, with a planning midpoint around **13 active hours**, excluding wall-clock soak time.

Expected Codex weekly quota consumption under the delta-scoped workflow: approximately **40-60% of one weekly quota**, with ~50% as the planning midpoint. A clean run may be lower; genuine signing/send/finality defects may push materially higher. These are planning estimates, not acceptance criteria.

## Resume instruction

When BTC/SOL is complete and the owner wants to start this lane, begin at `MEME-LIVE-G000`. Do not regenerate this roadmap from scratch. First verify current remote HEAD, clean state, Phase-5 accepted identities and whether any subsequent master changes materially affect the locked assumptions. Then authorize exactly G000 and continue task-by-task.
