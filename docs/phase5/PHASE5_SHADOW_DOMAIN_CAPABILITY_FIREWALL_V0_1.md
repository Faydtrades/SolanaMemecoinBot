# Phase-5 Shadow Domain + Capability Firewall v0.1

## Status and purpose

`MEME-P5-T001` is `PASS / ACCEPTED / CHECKPOINTED`.

This contract establishes an isolated, sidecar-capable domain and persistence
foundation for future shadow execution. It does not integrate with the active
Phase-4 paper runtime and has no venue, quote, instruction, wallet, or live
execution implementation.

## Contract identity

- Model: `P5-SHADOW-DOMAIN-CAPABILITY-FIREWALL-0001`
- Schema: `phase5_shadow_domain_capability_firewall_v0.1`
- Intent schema: `phase5_execution_intent_v0.1`
- State machine: `phase5_shadow_state_machine_v0.1`
- Model fingerprint:
  `506312a81b6d8acc724cb27d6d450086bc3fc4986dcdea4831b998a3a8ec91e3`

The fingerprint is SHA-256 over canonical, sorted, compact JSON describing the
identity source, entry cardinality, integer economics, lifecycle states,
persistence ownership, and structural capability allow-list.

## ExecutionIntent v0.1

`ExecutionIntentV01` is a frozen, slotted record containing only immutable
economic decision and lineage data. Its canonical JSON, full content
fingerprint, and `P5EI-...` identity are deterministic. Economic amounts,
source ingest sequence, and UTC epoch decision time in microseconds are strict
integers; booleans and floating amounts are rejected.

The intent retains the accepted Phase-2/4 identities rather than inventing
parallel identity:

- `CandidateSignal.signal_id` as `candidate_signal_id`;
- candidate strategy run and evaluation identity;
- strategy version and parameter-set identity;
- source run, event key, and ingest sequence where available;
- mint, role, side, input asset, and integer base-unit amount; and
- for EXIT, the Phase-4 position plus parent-entry, exit-decision, track, and
  lifecycle identities.

ENTRY is always BUY. EXIT is always SELL. Route, quote, blockhash, plan,
simulation, and result data are excluded from the immutable intent core.

### One shared ENTRY

An ENTRY intent has no alternative exit-track field. Its identity is shared by
`FINAL-A`, `FINAL-B`, and `SENS-C`, and the repository has a partial unique
index on the canonical candidate identity for ENTRY rows. Exact replay returns
the same intent. A second ENTRY for the same candidate with different immutable
content fails closed.

EXIT intents may share a position but must have independent deterministic exit
decision and lifecycle lineage, supporting partial or multiple closures. The
repository requires the referenced parent ENTRY to exist and match canonical
candidate and mint lineage; the SQLite schema also enforces that parent with a
self-referential foreign key.

## State machine

Normal progression is strictly:

```text
CREATED
-> ELIGIBILITY_CHECKED
-> ROUTE_BOUND
-> QUOTE_BOUND
-> PLAN_BUILT
-> SIMULATED
-> COMPLETED
```

`REJECTED`, `EXPIRED`, and `FAILED` are terminal negative outcomes reachable
only from the stages declared by the central transition matrix. `FAILED` is
available from every nonterminal operational stage. No state represents a
live transaction lifecycle.

Forward skipping, stale timestamps, completion before simulation, terminal
resurrection, and conflicting idempotency replays fail closed. Exact replay of
an already persisted transition remains idempotent even after later states.
History is append-only, sequenced, content-fingerprinted, and sufficient to
reconstruct and audit the cached current state.

## Capability firewall

The production `src/phase5` package imports only standard-library and local
Phase-5 modules. Its public API exposes immutable intents, lifecycle types, and
the isolated repository. It contains no Phase-4 runtime dependency and no raw
database-connection API.

The executable firewall scans production AST imports, identifiers, attributes,
functions, classes, and identifier-like strings. It rejects key-material,
signing, submission, broadcast, wallet-mutation, and capability-enable API
shapes. It also verifies that forbidden transaction lifecycle states are
absent and that no public configuration field can enable those capabilities.
Injected prohibited examples are proven to fail the scanner.

The scanner deliberately allows public-state reads and an unsigned
`simulate_transaction` call shape so T002-T004 can add read-only venue and
simulation behavior without weakening the firewall.

## Persistence ownership

The default database is:

```text
data\shadow\phase5_shadow_execution_v0_1.sqlite3
```

Paths under `data\paper` and `data\db` fail validation before filesystem
creation. Tests use a temporary directory only. The shadow repository owns a
separate WAL, `synchronous=FULL`, foreign-key-enabled SQLite schema containing:

- immutable execution intents;
- cached state-machine position;
- append-only transition history; and
- exact model/schema metadata.

SQLite triggers prohibit intent updates/deletes and history updates/deletes.
Registration and transition changes use `BEGIN IMMEDIATE`; current state and
history update atomically. Restart/reopen, `PRAGMA quick_check`, foreign-key
checks, canonical digest, idempotency, and conflict behavior are deterministic.

## Focused evidence

The isolated self-test reports `43/43` checks and `RESULT: PASS`:

- sample intent ID:
  `P5EI-6be446401cf5cac9dda35904b6b50549`;
- sample intent fingerprint:
  `cf018029cae50f1ac56d179bc5acef05fef1dcf8aea71043692bd8b4e2d58ab1`;
- deterministic repository digest:
  `96330938547136171c91affe270ec50a960c95b1a4f87e71cf6cd24ba140a3d5`;
- SQLite quick check: `ok`;
- foreign keys: enabled, enforced, and no violations; and
- all four protected active Phase-4 file hashes: exact.

No production database, active observability registry, collector, network/RPC,
runtime, dependency, wallet, or environment was accessed or changed.

## Pre-delivery adversarial review

The final intent validation, identity construction, replay ordering, SQL
transactions, schema constraints, transition reconstruction, package exports,
and firewall scanner were reviewed line-by-line. The review found and fixed:

- missing repository validation that an EXIT parent ENTRY exists and matches
  candidate and mint lineage;
- a public-constructor path that could bypass the safe repository factory;
- acceptance of a manually corrupted persisted JSON object containing fields
  outside the canonical intent record; and
- runtime mutability of the central transition matrix.

Regression coverage now proves parent-lineage rejection, constructor/path
isolation, absence of a public raw connection, canonical restart integrity,
immutable matrix behavior, and injected firewall violations. No remaining
T001 acceptance blocker was found.

## Explicitly out of scope

T001 implements no venue discovery/routing, migration detection, executable
quote, instruction construction, blockhash acquisition, RPC simulation,
balance lookup, continuous sidecar, paper/shadow comparison, strategy change,
exit change, or candidate change. It contains no kill switch or live retry
system because there is no live capability to control.

## T002 extension rule

T002 may add immutable public venue-state evidence and deterministic route
selection downstream of `ExecutionIntentV01`. It must not add ephemeral route
or chain fields to the intent identity, bypass the repository's one-shared-
ENTRY invariant, expose a mutable raw persistence handle, or weaken the AST/API
firewall. Read-only RPC dependencies must be narrowly imported and remain
incapable of key-material handling or transaction submission.
