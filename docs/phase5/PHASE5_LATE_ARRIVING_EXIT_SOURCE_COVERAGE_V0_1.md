# Phase 5 Late-Arriving Exit Source Coverage v0.1

Task: `MEME-P5-T004D1`

Status: `IMPLEMENTED_PENDING_PROJECT_REVIEW`

## Root cause

T004B v0.2 advanced its durable exit-source cursor by
`(requested_at, exit_intent_id)`. Those fields are causal/economic metadata;
they are not an insertion sequence. When a later SQLite insert shared the
same `requested_at` but had a lexicographically smaller ID—or carried an older
causal timestamp—the row sorted behind the durable cursor and was skipped
permanently.

## Corrected contract

- Model: `P5-SHADOW-LIFECYCLE-BRIDGE-0003`
- Model fingerprint:
  `a14d1a7b2d959ebcc75583f830f8d444313b694f947a2bebdfe2a59b27b0e7f0`
- Schema: `phase5_shadow_lifecycle_bridge_v0.3`
- New evidence schema: `phase5_shadow_exit_source_evidence_v0.3`
- Append-progress schema: `phase5_shadow_exit_source_progress_v0.1`
- C2 fingerprint:
  `c587e05e772b3f9e7de3ed9afaf7e8d83056a029aec5c5b19bffe71ba5c8e694`

Source discovery now orders and advances exclusively by the bound Phase-4
SQLite `paper_exit_intents.rowid`. `requested_at` remains unchanged as the
Shadow `ExecutionIntent` decision timestamp. Each processed row gets an
immutable append-progress record binding the source ID, physical rowid, full
normalized source row, and exact evidence ID. Before every poll, the processed
prefix is re-read through the read-only Phase-4 connection and checked against
that binding.

## v0.2 migration and reconciliation

Only the exact accepted v0.2 schema/model fingerprint is migratable. Unknown
metadata fails closed. Migration adds the rowid cursor and immutable progress
ledger, records the v0.3 schema metadata, and starts the append cursor at zero.
The bridge then deterministically replays the exact bound source from its
beginning. Exact legacy v0.2 evidence is recognized byte-for-byte and linked
to progress without being updated or duplicated. Missing rows receive v0.3
evidence. Evidence persistence remains idempotent across a crash before the
atomic progress/cursor transaction.

The accepted C2 database binding is retained exactly for existing databases.
New databases use the new C2 fingerprint. Existing comparisons therefore do
not acquire duplicate observations solely because T004B discovery changed.

## Real acceptance-copy reconciliation

The original paper and Shadow databases were not modified. A copy of the
accepted Shadow database was migrated and drained against the original paper
database opened `mode=ro` plus `query_only`.

- Paper exit intents: `389`
- T004B evidence: `385 -> 389`
- Immutable legacy evidence: `385/385` unchanged
- Append-progress rows: `0 -> 389`
- Shadow execution intents: `322 -> 325`
- ENTRY intents: `198 -> 198`
- Expected inventories: `59 -> 59`
- Recovered source exits: four FINAL-B rows
- New Shadow EXIT intents: three
- Contract-backed no-position evidence: one, because its parent ENTRY was
  already terminal `FAILED`
- Duplicate evidence, exit decisions, or lineage: zero
- Nonterminal work after bounded drain: zero
- SQLite `quick_check`: `ok`
- Foreign-key violations: zero

The pre-existing terminal `P5EI-ec300ae2da0a29b1c6921143728b56a3`
comparison gap was filled by the unchanged C2 comparison scan. The three new
Shadow EXIT intents were terminalized by one bounded read-only RPC/simulation
cycle on the copy. No signer, transaction submission, or broadcast capability
was present or used.

## Preserved semantics

T004A is byte-identical to the checkpoint. There remains one shared ENTRY per
physical candidate. FINAL-A, FINAL-B, and SENS-C remain independent full
hypothetical exit tracks. Phase-4 remains independent and read-only from
Phase-5. No strategy, route, quote, plan, cost, signing, submission, or
broadcast behavior changed.

No commit was created. ChatGPT project review remains the acceptance authority.
