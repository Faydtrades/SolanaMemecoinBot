# MEME-LIVE Production Closure Workflow v2 — LOCKED OWNER WORKFLOW

Status: **LOCKED OWNER WORKFLOW / IMPLEMENTATION NOT AUTHORIZED**

This document locks the engineering method and acceptance principles for the future MEME-LIVE production-readiness lane. It does not authorize G000, G001, T001-T011, signing, sending, broadcasting, mainnet mutation, or any real-capital action.

The workflow is based on the pre-implementation completeness audit and intentionally adopts the lifecycle-first closure method used for BTC/SOL, with Solana/memecoin-specific contract boundaries added where required.

## 1. Architecture before implementation

No MEME-LIVE production code is to be implemented from the old task list as-is.

First produce and independently review a corrected **MEME-LIVE Production Closure Architecture** / roadmap amendment that owns all audit findings and the complete production lifecycle.

The architecture must trace at minimum:

`COLLECTOR -> SOURCE HEALTH -> STRATEGY/CANDIDATE -> LIVE ADMISSION -> LIVE-SIZED IMMUTABLE INTENT -> CHAIN TRUTH -> ACTUAL SETTLEMENT/POSITION -> RISK/FUNDING -> VENUE/QUOTE/PLAN -> OMS/AUTHORITY -> FINAL MESSAGE -> SIGN -> SEND -> FINALITY -> REAL POSITION -> SELECTED EXIT -> RECONCILIATION -> NEXT TRADE -> CRASH/REBOOT -> SAFE UNATTENDED CONTINUATION`

No unexplained transition may remain.

## 2. Existing accepted core is reused, not casually rebuilt

Accepted Phase-4/Phase-5 components remain the preferred foundation unless concrete evidence proves a required change.

In particular, do not rebuild or weaken accepted Pump/PumpSwap decoding, route selection, integer quote arithmetic, unsigned plan construction, zero-signature simulation primitives, deterministic candidate identity, or the Phase-5 capability firewall merely to make them live.

Signing/send capability remains in a separate LIVE layer. `src/phase5` remains non-signing/non-broadcast.

Existing PAPER/Shadow objects must not be silently promoted where their semantics differ from LIVE truth.

## 3. Project-specific LIVE boundary rules

The corrected architecture must explicitly own these memecoin/Solana-specific seams before implementation:

- canonical candidate -> fresh/eligible LIVE admission;
- immutable PAPER/Shadow amount -> separately persisted approved LIVE amount without mutating accepted Shadow evidence;
- finalized chain evidence -> exactly-once actual settlement, cash attribution and real-position projection;
- native SOL / WSOL / token / fee / rent funding and protected EXIT headroom;
- actual real position -> exactly one selected locked exit controller;
- protective EXIT obligation survives source/producer interruption when a safe reduction remains possible;
- stable producer/source identity and bounded reconstructable state across long unattended operation;
- no-broadcast mode has explicit durable disposition and never fabricates fills;
- final message, signature and send attempt identities remain durable across blockhash/RPC/network uncertainty; no blind replacement transaction is allowed.

## 4. Bounded worker tasks remain the implementation unit

Continue using CHIEF + one consequential worker for implementation.

Worker tasks should remain bounded, reviewable and dependency-aware. Do not solve the audit findings as one giant implementation task.

However, a bounded worker task passing its own contract is only a **LOCAL PASS**. It is not sufficient for package/area completion.

## 5. Two acceptance levels are mandatory

Every meaningful implementation area must eventually obtain both:

### LOCAL PASS
The bounded implementation satisfies its own contract, focused tests and direct safety invariants.

### SYSTEM TRANSITION PASS
The state/API/evidence produced by that work is proven compatible with the next required production transitions in the locked lifecycle.

A component may not be treated as production-complete solely because local tests pass.

## 6. Permanent Production Lifecycle Matrix

Maintain one authoritative lifecycle matrix throughout the lane.

Every necessary production transition must always be classified as one of:

- `VERIFIED`
- `OWNED_NOT_BUILT`
- `HUMAN_EXTERNAL`
- `BLOCKED`

No required transition may be omitted because it is outside the current worker's local scope.

## 7. No unowned limitations

Review language such as:

- `not implemented`
- `default deny`
- `future provider`
- `out of scope`
- `not integrated`
- `immutable`
- `paper-only`
- `shadow-only`

is acceptable only when the limitation is either:

1. intentionally human/external, or
2. explicitly owned by a concrete later work package/task in the lifecycle matrix.

If neither is true, the current package cannot receive final SYSTEM TRANSITION PASS.

## 8. CHIEF has system-level checkpoint responsibility

CHIEF continues to perform diff/safety review, but package checkpoints must additionally answer:

> Which later required production transitions consume the state/API/evidence produced here, and does the current design actually support them without bypass, fabricated truth or incompatible identity?

CHIEF must actively block package acceptance if a required downstream transition is unowned or impossible under the current contract.

## 9. Work packages are lifecycle-coherent, tasks remain bounded

After the corrected architecture is reviewed, implementation is grouped into a small number of coherent lifecycle work packages rather than eight isolated audit-gap patches or twelve isolated roadmap tasks.

The final package boundaries are set by the corrected architecture, not invented ad hoc during coding.

Likely dependency families are:

- admission / identity / actual settlement contracts;
- public truth / source health;
- durable LIVE state / reconciliation;
- risk / funding / sizing;
- exact message / signer / send;
- real position / exit / producer / dry-mode runtime;
- recovery / unattended operations;
- external no-broadcast and micro-capital gates.

A work package may contain several bounded worker tasks.

## 10. Integrated deterministic acceptance before external gates

Before T010 no-broadcast soak or T011 real-capital smoke, run a full integrated deterministic/fake-stack acceptance of the corrected production composition.

It must exercise representative complete transitions including:

`cold deny -> source/truth -> LIVE admission -> approved size -> risk/funding -> quote/plan -> final message -> simulated send/finality/settlement -> real-position projection -> selected exit -> reconciliation -> next trade -> interruption/restart -> safe continuation`

It must also cover relevant unknown/partial/failure branches without using mainnet mutation.

No test helper may bypass the production boundaries being accepted.

## 11. Normal unattended runtime must exist before real-money closure

T011 may be the last real mutation gate only after the normal continuous production runtime is already implemented and deterministically accepted.

A one-shot BUY/SELL harness is not sufficient evidence of the stated finish line.

The system must already support later trades, multiple mints/positions within the declared supported policy, open-position restart, producer/source continuation, protective exits and safe reboot/reconstruction.

## 12. Testing strategy

Use targeted/focused tests during bounded implementation and correction loops.

Run broad/full regression on meaningful frozen work-package boundaries or when evidence shows cross-surface risk, not mechanically after every micro-fix.

Package SYSTEM TRANSITION PASS requires appropriate integrated scenario evidence in addition to module tests.

Quality and production correctness take priority over quota conservation. Avoid duplicated reads/tests that add no evidence.

## 13. Human and real-capital gates remain separate

Audit approval, architecture approval, implementation PASS and real-capital permission are separate states.

No signing/send/broadcast or real-capital action is authorized by this workflow.

T011 requires a separate explicit human authorization after all predecessor engineering, deterministic acceptance and T010 evidence are reviewed.

Capital increases after micro-smoke remain separate human decisions.

## 14. Definition of production-readiness completion

The MEME-LIVE lane is not complete until the corrected architecture/work packages, recovery/operations composition, deterministic integrated acceptance, corrected T010 and separately authorized T011 all pass and final project review confirms:

- no statically visible required production transition remains unowned;
- no unresolved local/chain state is treated as known;
- real position/accounting truth is reconstructable;
- protective exits and later legitimate trades remain possible under the declared supported policy;
- crash/reboot/network uncertainty does not create duplicate trades;
- unattended operation does not depend on the dashboard or manual babysitting.

Profitability remains outside this production-engineering finish line.

## 15. Next authorized planning step

The next planning step is the **MEME-LIVE Production Closure Architecture / roadmap correction review**, based on the pre-implementation audit.

No production implementation is authorized merely by locking this workflow.
